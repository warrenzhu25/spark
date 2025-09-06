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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.shuffle.ShuffleBlockInfo
import org.apache.spark.util.Utils

/**
 * Class to handle block manager decommissioning retries.
 * It creates a Thread to retry migrating all RDD cache and Shuffle blocks
 */
private[storage] class BlockManagerDecommissioner(
    conf: SparkConf,
    bm: BlockManager) extends Logging {

  private val fallbackStorage = FallbackStorage.getFallbackStorage(conf)
  private val maxReplicationFailuresForDecommission =
    conf.get(config.STORAGE_DECOMMISSION_MAX_REPLICATION_FAILURE_PER_BLOCK)

  // Migration strategies
  private val rddMigrationStrategy = new RDDMigrationStrategy()

  // Thread management
  private val threadManager = new MigrationThreadManager(conf)

  // State tracking
  private val stateTracker = new MigrationStateTracker()

  // Statistics collection
  private val statisticsCollector = new MigrationStatisticsCollector()
  private val errorHandler = new MigrationErrorHandler(maxReplicationFailuresForDecommission)

  // Backwards compatibility accessors for testing
  private[storage] def lastRDDMigrationTime: Long = stateTracker.lastRDDMigrationTime
  private[storage] def lastShuffleMigrationTime: Long = stateTracker.lastShuffleMigrationTime
  private[storage] def rddBlocksLeft: Boolean = stateTracker.rddBlocksLeft
  private[storage] def shuffleBlocksLeft: Boolean = stateTracker.shuffleBlocksLeft

  /**
   * This runnable consumes any shuffle blocks in the queue for migration. This part of a
   * producer/consumer where the main migration loop updates the queue of blocks to be migrated
   * periodically. On migration failure, the current thread will reinsert the block for another
   * thread to consume. Each thread migrates blocks to a different particular executor to avoid
   * distribute the blocks as quickly as possible without overwhelming any particular executor.
   *
   * There is no preference for which peer a given block is migrated to.
   * This is notable different than the RDD cache block migration (further down in this file)
   * which uses the existing priority mechanism for determining where to replicate blocks to.
   * Generally speaking cache blocks are less impactful as they normally represent narrow
   * transformations and we normally have less cache present than shuffle data.
   *
   * The producer/consumer model is chosen for shuffle block migration to maximize
   * the chance of migrating all shuffle blocks before the executor is forced to exit.
   */
  // Shuffle migration workers are now handled by ShuffleMigrationWorker class

  // Shuffles which are either in queue for migrations or migrated
  private[storage] val migratingShuffles = mutable.HashSet[ShuffleBlockInfo]()

  // Backwards compatibility accessors for statistics (for testing)
  private[storage] def numMigratedShuffles: AtomicInteger =
    new AtomicInteger(statisticsCollector.numMigratedShuffles)
  private[storage] def migratedShufflesSize: AtomicLong =
    new AtomicLong(statisticsCollector.migratedShufflesSize)
  private[storage] def totalShufflesSize: AtomicLong =
    new AtomicLong(statisticsCollector.totalShufflesSize)
  private[storage] def deletedShuffles: AtomicInteger =
    new AtomicInteger(statisticsCollector.deletedShuffles)
  private[storage] def failedShuffles: AtomicInteger =
    new AtomicInteger(statisticsCollector.failedShuffles)

  // Timing and progress tracking for enhanced logging
  // Migration timing is now managed by stateTracker
  private def migrationStartTime: Long = stateTracker.migrationStartTime
  private def lastProgressLogTime: Long = stateTracker.lastProgressLogTime
  private def lastMigrationActivity: Long = stateTracker.lastMigrationActivity

  // Shuffles which are queued for migration & number of retries so far.
  // Visible in storage for testing.
  private[storage] val shufflesToMigrate =
    new java.util.concurrent.ConcurrentLinkedQueue[(ShuffleBlockInfo, Int)]()

  // Migration state is now managed by stateTracker
  // Initialize disabled migrations as already stopped
  if (!conf.get(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED)) {
    stateTracker.stopRDD("RDD migration disabled in configuration")
  }
  if (!conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)) {
    stateTracker.stopShuffle("Shuffle migration disabled in configuration")
  }

  // Backwards compatibility accessors
  private def stopped: Boolean = stateTracker.stopped
  private[storage] def stoppedRDD: Boolean = stateTracker.stoppedRDD
  private def stoppedShuffle: Boolean = stateTracker.stoppedShuffle

  private val migrationPeers =
    mutable.HashMap[BlockManagerId, ShuffleMigrationWorker]()

  // Initialize thread manager if RDD migration is enabled
  if (conf.get(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED)) {
    threadManager.initialize()
  }

  private val rddBlockMigrationRunnable = new RDDMigrationWorker(
    bm, conf, rddMigrationStrategy, stateTracker, errorHandler, fallbackStorage)

  // Shuffle migration will use thread manager's executors

  private val shuffleBlockMigrationRefreshRunnable = new ShuffleRefreshWorker(
    conf, stateTracker, errorHandler, () => refreshMigratableShuffleBlocks(),
    () => logProgressIfNeeded())

  // Shuffle migration pool is managed by thread manager

  /**
   * Tries to migrate all shuffle blocks that are registered with the shuffle service locally.
   * Note: this does not delete the shuffle files in-case there is an in-progress fetch
   * but rather shadows them.
   * Requires an Indexed based shuffle resolver.
   * Note: if called in testing please call stopMigratingShuffleBlocks to avoid thread leakage.
   * Returns true if we are not done migrating shuffle blocks.
   */
  private[storage] def refreshMigratableShuffleBlocks(): Boolean = {
    // Update the queue of shuffles to be migrated
    logInfo("Start refreshing migratable shuffle blocks")
    val localShuffles = bm.migratableResolver.getStoredShuffles().toSet
    val newShufflesToMigrate = (localShuffles.diff(migratingShuffles)).toSeq
      .sortBy(b => (b.shuffleId, b.mapId))
    shufflesToMigrate.addAll(newShufflesToMigrate.map(x => (x, 0)).asJava)
    migratingShuffles ++= newShufflesToMigrate
    // Track total size of newly discovered shuffle blocks
    val totalNewSize = newShufflesToMigrate.map { shuffleBlockInfo =>
      try {
        val blocks = bm.migratableResolver.getMigrationBlocks(shuffleBlockInfo)
        val blockSize = blocks.map(_._2.size()).sum
        logDebug(s"Added ${Utils.bytesToString(blockSize)} for $shuffleBlockInfo to total size")
        blockSize
      } catch {
        case e: Exception =>
          logWarning(s"Failed to get size for shuffle block $shuffleBlockInfo: ${e.getMessage}")
          // Continue processing other blocks even if one fails
          0L
      }
    }.sum

    statisticsCollector.addShufflesToTrack(newShufflesToMigrate, totalNewSize)
    val remainedShuffles = statisticsCollector.getRemainingShuffleCount
    logInfo(s"${newShufflesToMigrate.size} of ${localShuffles.size} local shuffles " +
      s"are added. In total, $remainedShuffles shuffles are remained.")

    // Update the threads doing migrations
    val livePeerSet = bm.getPeers(false).toSet
    val currentPeerSet = migrationPeers.keys.toSet
    val deadPeers = currentPeerSet.diff(livePeerSet)
    // Randomize the orders of the peers to avoid hotspot nodes.
    val newPeers = Utils.randomize(livePeerSet.diff(currentPeerSet))
    migrationPeers ++= newPeers.map { peer =>
      logDebug(s"Starting thread to migrate shuffle blocks to ${peer}")
      val worker = new ShuffleMigrationWorker(peer, bm, shufflesToMigrate, fallbackStorage,
        statisticsCollector, stateTracker, errorHandler, maxReplicationFailuresForDecommission)
      if (conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)) {
        threadManager.shuffleMigrationPool.submit(worker)
      }
      (peer, worker)
    }
    // A peer may have entered a decommissioning state, don't transfer any new blocks
    deadPeers.foreach(migrationPeers.get(_).foreach(_.keepRunning = false))
    // If we don't have anyone to migrate to give up
    if (!migrationPeers.values.exists(_.keepRunning)) {
      logWarning("No available peers to receive Shuffle blocks, stop migration.")
      stateTracker.stopShuffle("No available peers")
    }
    // If we found any new shuffles to migrate or otherwise have not migrated everything.
    newShufflesToMigrate.nonEmpty || statisticsCollector.hasShufflesToMigrate
  }

  /**
   * Stop migrating shuffle blocks.
   */
  private[storage] def stopMigratingShuffleBlocks(): Unit = {
    if (conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)) {
      logInfo("Stopping migrating shuffle blocks.")
      // Stop as gracefully as possible.
      migrationPeers.values.foreach(_.keepRunning = false)
    }
  }

  /**
   * Tries to migrate all cached RDD blocks from this BlockManager to peer BlockManagers
   * Visible for testing
   * Returns true if we have not migrated all of our RDD blocks.
   */
  private[storage] def decommissionRddCacheBlocks(): Boolean = {
    rddMigrationStrategy.migrateBlocks(bm, conf)
  }


  def start(): Unit = {
    logInfo("Starting BlockManager decommissioning")

    // Log configuration details
    logInfo(s"Decommissioning configuration: " +
      s"maxReplicationFailures=$maxReplicationFailuresForDecommission, " +
      s"rddBlocksEnabled=${conf.get(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED)}, " +
      s"shuffleBlocksEnabled=${conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)}")

    // Log available peers
    val peers = bm.getPeers(false)
    if (peers.nonEmpty) {
      logInfo(s"Found ${peers.size} available peers for migration: ${peers.mkString(", ")}")
    } else {
      logWarning("No peers available for block migration - decommissioning may not complete")
    }

    // Initialize timing tracking
    stateTracker.initializeMigration()

    logInfo("Starting block migration threads")
    if (conf.get(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED)) {
      threadManager.rddBlockMigrationExecutor.submit(rddBlockMigrationRunnable)
    }
    if (conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)) {
      threadManager.shuffleBlockMigrationRefreshExecutor.submit(
        shuffleBlockMigrationRefreshRunnable)
    }
  }

  def stop(): Unit = {
    if (stateTracker.stopped) {
      return
    } else {
      logInfo("Initiating BlockManager decommissioning shutdown")
      stateTracker.stopAll()
    }
    try {
      threadManager.shutdown()
    } catch {
      case NonFatal(e) =>
        errorHandler.handleThreadError(e, "shutdown of migration thread manager")
    }
    try {
      stopMigratingShuffleBlocks()
    } catch {
      case NonFatal(e) =>
        errorHandler.handleThreadError(e, "shutdown shuffle block migration thread")
    }
    logMigrationSummary()
    logInfo("Stopped block migration")
  }

  /*
   *  Returns the last migration time and a boolean for if all blocks have been migrated.
   *  The last migration time is calculated to be the minimum of the last migration of any
   *  running migration (and if there are now current running migrations it is set to current).
   *  This provides a timeStamp which, if there have been no tasks running since that time
   *  we can know that all potential blocks that can be have been migrated off.
   */
  private[storage] def lastMigrationInfo(): MigrationInfo = {
    val shuffleMigrationStat = buildShuffleStat()
    if (stateTracker.isMigrationComplete) {
      // Since we don't have anything left to migrate ever (since we don't restart once
      // stopped), return that we're done with a validity timestamp that doesn't expire.
      MigrationInfo(Long.MaxValue, true, shuffleMigrationStat)
    } else {
      val lastMigrationTime = stateTracker.getLastMigrationTime
      val blocksMigrated = stateTracker.areAllBlocksMigrated
      MigrationInfo(lastMigrationTime, blocksMigrated, shuffleMigrationStat)
    }
  }

  private def buildShuffleStat(): MigrationStat = {
    statisticsCollector.buildMigrationStat()
  }

  /**
   * Log periodic progress updates to provide visibility into migration status
   */
  private def logProgressIfNeeded(): Unit = {
    val currentTime = System.currentTimeMillis()
    // Log progress every 30 seconds
    if (currentTime - lastProgressLogTime > 30000) {
      val stats = buildShuffleStat()
      val elapsedSeconds = (currentTime - migrationStartTime) / 1000

      if (stats.totalBlocks > 0) {
        val elapsedTimeMs = currentTime - migrationStartTime
        val detailedSummary = statisticsCollector.getDetailedSummary(elapsedTimeMs)
        logInfo(s"Migration progress after ${elapsedSeconds}s: $detailedSummary")
      } else {
        logInfo(s"Migration progress after ${elapsedSeconds}s: No shuffle blocks to migrate")
      }

      // Check for stalled migration
      checkForStalledMigration(currentTime)

      stateTracker.updateProgressLogTime()
    }
  }

  /**
   * Detect and log warnings for stalled migration
   */
  private def checkForStalledMigration(currentTime: Long): Unit = {
    val stallThreshold = 120000L // 2 minutes
    if (lastMigrationActivity > 0 &&
        currentTime - lastMigrationActivity > stallThreshold &&
        stateTracker.shouldContinueShuffleMigration) {
      val stats = buildShuffleStat()
      if (stats.numBlocksLeft > 0) {
        val stalledSeconds = (currentTime - lastMigrationActivity) / 1000
        logWarning(s"Migration appears stalled: no activity for ${stalledSeconds}s. " +
          s"${stats.numBlocksLeft} blocks remaining. " +
          s"Possible causes: slow/overloaded peers, network issues, or resource constraints. " +
          s"Consider checking peer health and network connectivity.")
      }
    }
  }

  /**
   * Log comprehensive migration summary with timing and statistics
   */
  private def logMigrationSummary(): Unit = {
    val currentTime = System.currentTimeMillis()
    val totalDuration = if (migrationStartTime > 0) {
      (currentTime - migrationStartTime) / 1000
    } else 0L

    val stats = buildShuffleStat()

    logInfo("=== BlockManager Decommissioning Summary ===")
    logInfo(s"Total duration: ${totalDuration}s")

    val blockProgressPct = if (stats.totalBlocks > 0) {
      (stats.numMigratedBlock * 100.0) / stats.totalBlocks
    } else 0.0
    logInfo(f"Shuffle blocks: ${stats.numMigratedBlock}/${stats.totalBlocks} " +
      f"migrated ($blockProgressPct%.1f%%)")

    val sizeProgressPct = if (stats.totalSize > 0) {
      (stats.totalMigratedSize * 100.0) / stats.totalSize
    } else 0.0
    logInfo(s"Data migrated: ${Utils.bytesToString(stats.totalMigratedSize)}/" +
      f"${Utils.bytesToString(stats.totalSize)} ($sizeProgressPct%.1f%%)")
    logInfo(s"Blocks deleted during migration: ${stats.deletedBlocks}")
    logInfo(s"Blocks failed after max retries: ${stats.failedBlocks}")
    logInfo(s"Blocks remaining: ${stats.numBlocksLeft}")

    if (totalDuration > 0 && stats.numMigratedBlock > 0) {
      val avgBlocksPerSec = stats.numMigratedBlock.toDouble / totalDuration
      val avgBytesPerSec = stats.totalMigratedSize.toDouble / totalDuration
      logInfo(f"Average migration rate: $avgBlocksPerSec%.2f blocks/sec, " +
        f"${Utils.bytesToString(avgBytesPerSec.toLong)}/sec")
    }

    val migrationStatus = if (stats.numBlocksLeft == 0) {
      "COMPLETED"
    } else if (stateTracker.stopped) {
      "STOPPED_INCOMPLETE"
    } else {
      "IN_PROGRESS"
    }
    logInfo(s"Migration status: $migrationStatus")
    logInfo("=== End Decommissioning Summary ===")
  }
}

/**
 * Migration information containing the current state of block migration during decommissioning.
 *
 * @param lastMigrationTime timestamp of the last migration activity, used to determine if
 *                         migration is complete when no tasks are running
 * @param allBlocksMigrated whether all blocks have been successfully migrated
 * @param shuffleMigrationStat detailed statistics about shuffle block migration progress
 */
private[spark] case class MigrationInfo(lastMigrationTime: Long,
    allBlocksMigrated: Boolean,
    shuffleMigrationStat: MigrationStat
)

/**
 * Statistics tracking the progress of shuffle block migration during decommissioning.
 *
 * This provides comprehensive metrics about the migration process, including the current
 * state, overall progress, and efficiency indicators.
 *
 * @param numBlocksLeft number of shuffle blocks remaining to be migrated
 * @param totalMigratedSize total size in bytes of all successfully migrated shuffle blocks
 * @param numMigratedBlock total number of shuffle blocks that have been successfully migrated
 * @param totalBlocks total number of shuffle blocks discovered during the migration process
 * @param totalSize total size in bytes of all shuffle blocks (migrated + remaining)
 * @param deletedBlocks number of shuffle blocks that were deleted during migration
 * @param failedBlocks number of shuffle blocks that failed migration after exceeding max retries
 */
private[spark] case class MigrationStat(numBlocksLeft: Int,
    totalMigratedSize: Long,
    numMigratedBlock: Int,
    totalBlocks: Int,
    totalSize: Long,
    deletedBlocks: Int,
    failedBlocks: Int)
