/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.storage

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ShuffleBlockInfo
import org.apache.spark.util.Utils

/**
 * Collects and manages statistics for block migration during executor decommissioning.
 * Provides thread-safe tracking of migration progress, timing, and metrics.
 */
private[storage] class MigrationStatisticsCollector extends Logging {

  // Shuffle migration counters
  private val _numMigratedShuffles = new AtomicInteger(0)
  private val _migratedShufflesSize = new AtomicLong(0)
  private val _totalShufflesSize = new AtomicLong(0)
  private val _deletedShuffles = new AtomicInteger(0)
  private val _failedShuffles = new AtomicInteger(0)

  // Track which shuffles are being migrated vs completed
  private val _migratingShuffles = mutable.HashSet[ShuffleBlockInfo]()

  /**
   * Record that a shuffle block was successfully migrated
   */
  def recordShuffleMigrated(sizeBytes: Long): Unit = {
    _numMigratedShuffles.incrementAndGet()
    _migratedShufflesSize.addAndGet(sizeBytes)
    logDebug(s"Recorded shuffle migration: ${Utils.bytesToString(sizeBytes)}")
  }

  /**
   * Record that a shuffle block was deleted during migration
   */
  def recordShuffleDeleted(): Unit = {
    _deletedShuffles.incrementAndGet()
    logDebug("Recorded shuffle block deletion")
  }

  /**
   * Record that a shuffle block failed migration after exceeding max retries
   */
  def recordShuffleFailed(): Unit = {
    _failedShuffles.incrementAndGet()
    logDebug("Recorded shuffle block failure after exceeding max retries")
  }

  /**
   * Add newly discovered shuffle blocks to track
   */
  def addShufflesToTrack(shuffles: Seq[ShuffleBlockInfo], totalSize: Long): Unit = {
    _migratingShuffles ++= shuffles
    _totalShufflesSize.addAndGet(totalSize)
    logDebug(s"Added ${shuffles.size} shuffle blocks " +
      s"(${Utils.bytesToString(totalSize)}) to tracking")
  }

  /**
   * Get current migration statistics
   */
  def buildMigrationStat(): MigrationStat = {
    val totalBlocks = _migratingShuffles.size
    val migratedBlocks = _numMigratedShuffles.get()
    val deletedBlocks = _deletedShuffles.get()
    val failedBlocks = _failedShuffles.get()
    val blocksLeft = totalBlocks - migratedBlocks - deletedBlocks - failedBlocks

    MigrationStat(
      numBlocksLeft = blocksLeft,
      totalMigratedSize = _migratedShufflesSize.get(),
      numMigratedBlock = migratedBlocks,
      totalBlocks = totalBlocks,
      totalSize = _totalShufflesSize.get(),
      deletedBlocks = deletedBlocks,
      failedBlocks = failedBlocks
    )
  }

  /**
   * Get the number of remaining shuffle blocks to migrate
   */
  def getRemainingShuffleCount: Int = {
    _migratingShuffles.size - _numMigratedShuffles.get() - _deletedShuffles.get() -
      _failedShuffles.get()
  }

  /**
   * Check if there are more shuffles to migrate
   */
  def hasShufflesToMigrate: Boolean = {
    _migratingShuffles.size > _numMigratedShuffles.get() + _deletedShuffles.get() +
      _failedShuffles.get()
  }

  /**
   * Get current progress as a percentage (0.0 to 100.0)
   */
  def getMigrationProgressPercent: Double = {
    val totalBlocks = _migratingShuffles.size
    if (totalBlocks > 0) {
      val completedBlocks = _numMigratedShuffles.get() + _deletedShuffles.get() +
        _failedShuffles.get()
      (completedBlocks * 100.0) / totalBlocks
    } else {
      100.0 // No blocks to migrate means 100% complete
    }
  }

  /**
   * Generate a formatted progress summary
   */
  def getProgressSummary: String = {
    val stats = buildMigrationStat()
    val progressPct = getMigrationProgressPercent

    f"${stats.numMigratedBlock}/${stats.totalBlocks} blocks ($progressPct%.1f%%), " +
      f"${Utils.bytesToString(stats.totalMigratedSize)}/" +
      f"${Utils.bytesToString(stats.totalSize)}"
  }

  /**
   * Generate a detailed migration summary with timing information
   */
  def getDetailedSummary(elapsedTimeMs: Long): String = {
    val stats = buildMigrationStat()
    val progressPct = getMigrationProgressPercent
    val elapsedSeconds = elapsedTimeMs / 1000.0

    val baseInfo = f"${stats.numMigratedBlock}/${stats.totalBlocks} blocks " +
      f"($progressPct%.1f%%), " +
      f"${Utils.bytesToString(stats.totalMigratedSize)}/" +
      f"${Utils.bytesToString(stats.totalSize)}"

    val timingInfo = if (elapsedSeconds > 0 && stats.numMigratedBlock > 0) {
      val blocksPerSec = stats.numMigratedBlock.toDouble / elapsedSeconds
      val bytesPerSec = stats.totalMigratedSize.toDouble / elapsedSeconds
      f", rate: $blocksPerSec%.1f blocks/s, ${Utils.bytesToString(bytesPerSec.toLong)}/s"
    } else {
      ""
    }

    val deletionInfo = if (stats.deletedBlocks > 0) {
      s", ${stats.deletedBlocks} deleted"
    } else {
      ""
    }

    val failureInfo = if (stats.failedBlocks > 0) {
      s", ${stats.failedBlocks} failed"
    } else {
      ""
    }

    s"$baseInfo$timingInfo$deletionInfo$failureInfo"
  }

  /**
   * Log a progress update if significant progress has been made
   */
  def logProgressIfNeeded(lastLoggedCount: Int, progressThreshold: Int = 10): Int = {
    val currentCount = _numMigratedShuffles.get()
    if (currentCount - lastLoggedCount >= progressThreshold) {
      val progressSummary = getProgressSummary
      logInfo(s"Migration progress: $progressSummary")
      currentCount
    } else {
      lastLoggedCount
    }
  }

  /**
   * Reset all statistics (useful for testing)
   */
  def reset(): Unit = {
    _numMigratedShuffles.set(0)
    _migratedShufflesSize.set(0)
    _totalShufflesSize.set(0)
    _deletedShuffles.set(0)
    _failedShuffles.set(0)
    _migratingShuffles.clear()
    logDebug("Migration statistics reset")
  }

  /**
   * Get raw statistics for testing and monitoring
   */
  def getStatsSummary: String = {
    s"Migrated: ${_numMigratedShuffles.get()}, " +
      s"Size: ${Utils.bytesToString(_migratedShufflesSize.get())}, " +
      s"Total: ${_migratingShuffles.size}, " +
      s"Deleted: ${_deletedShuffles.get()}, " +
      s"Failed: ${_failedShuffles.get()}"
  }

  // Accessors for backward compatibility
  def numMigratedShuffles: Int = _numMigratedShuffles.get()
  def migratedShufflesSize: Long = _migratedShufflesSize.get()
  def totalShufflesSize: Long = _totalShufflesSize.get()
  def deletedShuffles: Int = _deletedShuffles.get()
  def failedShuffles: Int = _failedShuffles.get()
  def migratingShufflesSize: Int = _migratingShuffles.size
}
