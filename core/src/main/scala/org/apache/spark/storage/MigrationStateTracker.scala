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

import org.apache.spark.internal.Logging

/**
 * Tracks the state of block migration during executor decommissioning.
 * Centralizes volatile state variables and provides thread-safe state management.
 */
private[storage] class MigrationStateTracker extends Logging {

  // Migration timing tracking
  @volatile private var _lastRDDMigrationTime: Long = 0
  @volatile private var _lastShuffleMigrationTime: Long = 0
  @volatile private var _migrationStartTime: Long = 0
  @volatile private var _lastProgressLogTime: Long = 0
  @volatile private var _lastMigrationActivity: Long = 0

  // Block availability tracking
  @volatile private var _rddBlocksLeft: Boolean = true
  @volatile private var _shuffleBlocksLeft: Boolean = true

  // Migration completion state
  @volatile private var _stopped: Boolean = false
  @volatile private var _stoppedRDD: Boolean = false
  @volatile private var _stoppedShuffle: Boolean = false

  /**
   * Initialize migration tracking with current timestamp
   */
  def initializeMigration(): Unit = {
    val currentTime = System.currentTimeMillis()
    _migrationStartTime = currentTime
    _lastProgressLogTime = currentTime
    logInfo("Migration state tracker initialized")
  }

  /**
   * Mark RDD migration round as completed
   */
  def markRDDMigrationCompleted(hasBlocksLeft: Boolean): Unit = {
    _lastRDDMigrationTime = System.nanoTime()
    _rddBlocksLeft = hasBlocksLeft
  }

  /**
   * Mark shuffle migration round as completed
   */
  def markShuffleMigrationCompleted(hasBlocksLeft: Boolean): Unit = {
    _lastShuffleMigrationTime = System.nanoTime()
    _shuffleBlocksLeft = hasBlocksLeft
  }

  /**
   * Update last migration activity timestamp
   */
  def recordMigrationActivity(): Unit = {
    _lastMigrationActivity = System.currentTimeMillis()
  }

  /**
   * Stop all migration activities
   */
  def stopAll(): Unit = {
    if (!_stopped) {
      logInfo("Stopping all migration activities")
      _stopped = true
    }
  }

  /**
   * Stop RDD migration
   */
  def stopRDD(reason: String = ""): Unit = {
    if (!_stoppedRDD) {
      logInfo(s"Stopping RDD migration${if (reason.nonEmpty) s": $reason" else ""}")
      _stoppedRDD = true
    }
  }

  /**
   * Stop shuffle migration
   */
  def stopShuffle(reason: String = ""): Unit = {
    if (!_stoppedShuffle) {
      logInfo(s"Stopping shuffle migration${if (reason.nonEmpty) s": $reason" else ""}")
      _stoppedShuffle = true
    }
  }

  // Getters for current state
  def lastRDDMigrationTime: Long = _lastRDDMigrationTime
  def lastShuffleMigrationTime: Long = _lastShuffleMigrationTime
  def migrationStartTime: Long = _migrationStartTime
  def lastProgressLogTime: Long = _lastProgressLogTime
  def lastMigrationActivity: Long = _lastMigrationActivity

  def rddBlocksLeft: Boolean = _rddBlocksLeft
  def shuffleBlocksLeft: Boolean = _shuffleBlocksLeft

  def stopped: Boolean = _stopped
  def stoppedRDD: Boolean = _stoppedRDD
  def stoppedShuffle: Boolean = _stoppedShuffle

  /**
   * Check if migration is complete (all activities stopped or no blocks left)
   */
  def isMigrationComplete: Boolean = {
    _stopped || (_stoppedRDD && _stoppedShuffle)
  }

  /**
   * Check if all blocks have been successfully migrated
   */
  def areAllBlocksMigrated: Boolean = {
    (!_shuffleBlocksLeft || _stoppedShuffle) && (!_rddBlocksLeft || _stoppedRDD)
  }

  /**
   * Check if RDD migration should continue
   */
  def shouldContinueRDDMigration: Boolean = {
    !_stopped && !_stoppedRDD
  }

  /**
   * Check if shuffle migration should continue
   */
  def shouldContinueShuffleMigration: Boolean = {
    !_stopped && !_stoppedShuffle
  }

  /**
   * Get the last migration time considering both RDD and shuffle migrations
   */
  def getLastMigrationTime: Long = {
    if (isMigrationComplete) {
      // If stopped, return current time to indicate no more migrations expected
      System.nanoTime()
    } else if (!_stoppedRDD && !_stoppedShuffle) {
      // Both running, return the minimum (oldest) time
      Math.min(_lastRDDMigrationTime, _lastShuffleMigrationTime)
    } else if (!_stoppedShuffle) {
      // Only shuffle running
      _lastShuffleMigrationTime
    } else {
      // Only RDD running
      _lastRDDMigrationTime
    }
  }

  /**
   * Update progress log timestamp
   */
  def updateProgressLogTime(): Unit = {
    _lastProgressLogTime = System.currentTimeMillis()
  }

  /**
   * Get current state summary for logging
   */
  def getStateSummary: String = {
    s"Migration state: stopped=${_stopped}, stoppedRDD=${_stoppedRDD}, " +
      s"stoppedShuffle=${_stoppedShuffle}, rddBlocksLeft=${_rddBlocksLeft}, " +
      s"shuffleBlocksLeft=${_shuffleBlocksLeft}"
  }
}
