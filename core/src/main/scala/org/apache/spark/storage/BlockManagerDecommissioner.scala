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

import java.io.IOException
import java.util.concurrent.TimeoutException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.MapOutputTrackerMaster
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.shuffle.ShuffleBlockInfo
import org.apache.spark.storage.BlockManagerMessages.ReplicateBlock
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Class to handle block manager decommissioning retries.
 * It creates a Thread to retry migrating all RDD cache and Shuffle blocks
 */
private[storage] class BlockManagerDecommissioner(
    conf: SparkConf,
    bm: BlockManager,
    mapOutputTracker: MapOutputTrackerMaster = null) extends Logging {

  private val fallbackStorage = FallbackStorage.getFallbackStorage(conf)
  private val maxReplicationFailuresForDecommission =
    conf.get(config.STORAGE_DECOMMISSION_MAX_REPLICATION_FAILURE_PER_BLOCK)
  private val enableSizeBasedTimeouts =
    conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_UPLOAD_TIMEOUT_ENABLED)
  private val timeoutMbPerSec =
    conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_UPLOAD_TIMEOUT_MB_PER_SEC)

  // Hardcoded thresholds (no longer configurable)
  private val minUploadThroughputBytesPerSec = 100L * 1024 * 1024 // 100 MB/sec

  // Used for tracking if our migrations are complete. Readable for testing
  @volatile private[storage] var lastRDDMigrationTime: Long = 0
  @volatile private[storage] var lastShuffleMigrationTime: Long = 0
  @volatile private[storage] var rddBlocksLeft: Boolean = true
  @volatile private[storage] var shuffleBlocksLeft: Boolean = true

  // Track upload statistics per executor for shuffle migration
  private[storage] case class UploadStats(
    var totalBytes: Long = 0L,
    var totalTimeMs: Long = 0L,
    var firstUploadTime: Long = 0L,
    var timeoutCount: Int = 0) {

    def addUpload(bytes: Long, durationMs: Long): Unit = {
      if (firstUploadTime == 0L) {
        firstUploadTime = System.currentTimeMillis()
      }
      totalBytes += bytes
      totalTimeMs += durationMs
    }

    def addTimeout(): Unit = {
      timeoutCount += 1
    }

    def currentThroughputBytesPerSec: Double = {
      if (totalTimeMs == 0L) 0.0 else (totalBytes * 1000.0) / totalTimeMs
    }

    def isTooSlow(minThroughput: Long): Boolean = {
      totalTimeMs > 0 && currentThroughputBytesPerSec < minThroughput
    }

    def hasTooManyTimeouts(maxTimeouts: Int = 3): Boolean = {
      timeoutCount >= maxTimeouts
    }
  }

  private[storage] val uploadStats =
    mutable.Map[BlockManagerId, UploadStats]()

  /**
   * Calculate upload timeout based on block size and expected throughput.
   * Uses simple MB/sec rate with automatic scaling and minimum timeout.
   */
  private[storage] def calculateUploadTimeout(blockSizeBytes: Long): Duration = {
    val blockSizeMB = blockSizeBytes.toDouble / (1024 * 1024)
    val expectedSeconds = blockSizeMB / timeoutMbPerSec
    val timeoutWithBuffer = (expectedSeconds * 1.5).toLong // 50% buffer for network variance
    val finalTimeoutSeconds = Math.max(30, timeoutWithBuffer) // Minimum 30 seconds
    Duration(finalTimeoutSeconds, TimeUnit.SECONDS)
  }

  /**
   * Upload a block with optional size-based timeout.
   * When size-based timeouts are enabled, throws TimeoutException if upload exceeds timeout.
   * When disabled, waits indefinitely (original behavior).
   */
  private def uploadBlockSyncWithTimeout(
      hostname: String,
      port: Int,
      execId: String,
      blockId: BlockId,
      buffer: org.apache.spark.network.buffer.ManagedBuffer,
      level: StorageLevel,
      blockSizeBytes: Long): Unit = {
    val future = bm.blockTransferService.uploadBlock(
      hostname, port, execId, blockId, buffer, level, null)

    if (enableSizeBasedTimeouts) {
      val timeout = calculateUploadTimeout(blockSizeBytes)
      ThreadUtils.awaitResult(future, timeout)
    } else {
      // Original behavior: wait indefinitely
      ThreadUtils.awaitResult(future, Duration.Inf)
    }
  }

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
  private class ShuffleMigrationRunnable(peer: BlockManagerId) extends Runnable {
    @volatile var keepRunning = true

    private def isThroughputTooSlow(): Boolean = {
      uploadStats.get(peer) match {
        case Some(stats) => stats.isTooSlow(minUploadThroughputBytesPerSec)
        case None => false
      }
    }

    private def hasTooManyTimeouts(): Boolean = {
      if (enableSizeBasedTimeouts) {
        uploadStats.get(peer) match {
          case Some(stats) => stats.hasTooManyTimeouts()
          case None => false
        }
      } else {
        false // Never consider timeouts when feature is disabled
      }
    }

    private def recordUpload(bytes: Long, durationMs: Long): Unit = {
      val stats = uploadStats.getOrElseUpdate(peer, UploadStats())
      stats.addUpload(bytes, durationMs)
    }

    private def recordTimeout(): Unit = {
      if (enableSizeBasedTimeouts) {
        val stats = uploadStats.getOrElseUpdate(peer, UploadStats())
        stats.addTimeout()
      }
    }

    private def allowRetry(shuffleBlock: ShuffleBlockInfo, failureNum: Int): Boolean = {
      if (failureNum < maxReplicationFailuresForDecommission) {
        logInfo(s"Add $shuffleBlock back to migration queue for " +
          s"retry ($failureNum / $maxReplicationFailuresForDecommission)")
        // The block needs to retry so we should not mark it as finished
        shufflesToMigrate.add((shuffleBlock, failureNum))
      } else {
        logWarning(s"Give up migrating $shuffleBlock since it's been " +
          s"failed for $maxReplicationFailuresForDecommission times")
        false
      }
    }

    private def nextShuffleBlockToMigrate(): (ShuffleBlockInfo, Int) = {
      while (!Thread.currentThread().isInterrupted) {
        Option(shufflesToMigrate.poll()) match {
          case Some(head) => return head
          // Nothing to do right now, but maybe a transfer will fail or a new block
          // will finish being committed.
          case None => Thread.sleep(1000)
        }
      }
      throw SparkCoreErrors.interruptedError()
    }

    override def run(): Unit = {
      logInfo(s"Starting shuffle block migration thread for $peer")
      // Once a block fails to transfer to an executor stop trying to transfer more blocks
      while (keepRunning) {
        try {
          val (shuffleBlockInfo, retryCount) = nextShuffleBlockToMigrate()
          val blocks = bm.migratableResolver.getMigrationBlocks(shuffleBlockInfo)
          // We only migrate a shuffle block when both index file and data file exist.
          if (blocks.isEmpty) {
            logInfo(s"Ignore deleted shuffle block $shuffleBlockInfo")
          } else if (isThroughputTooSlow()) {
            val stats = uploadStats(peer)
            logInfo(s"Upload throughput too slow for $peer " +
              s"(${Utils.bytesToString(stats.currentThroughputBytesPerSec.toLong)}/sec < " +
              s"${Utils.bytesToString(minUploadThroughputBytesPerSec)}/sec), stopping migration")
            keepRunning = false
            // Re-add the block to queue for other threads to pick up
            if (!allowRetry(shuffleBlockInfo, retryCount)) {
              numMigratedShuffles.incrementAndGet()
            }
          } else if (hasTooManyTimeouts()) {
            val stats = uploadStats(peer)
            logInfo(s"Too many upload timeouts for $peer " +
              s"(${stats.timeoutCount} timeouts), stopping migration")
            keepRunning = false
            // Re-add the block to queue for other threads to pick up
            if (!allowRetry(shuffleBlockInfo, retryCount)) {
              numMigratedShuffles.incrementAndGet()
            }
          } else {
            logInfo(s"Got migration sub-blocks $blocks. Trying to migrate $shuffleBlockInfo " +
              s"to $peer ($retryCount / $maxReplicationFailuresForDecommission)")
            // Migrate the components of the blocks.
            try {
              val totalBytes = blocks.map(b => b._2.size()).sum
              val startTime = System.currentTimeMillis()
              if (fallbackStorage.isDefined && peer == FallbackStorage.FALLBACK_BLOCK_MANAGER_ID) {
                fallbackStorage.foreach(_.copy(shuffleBlockInfo, bm))
              } else {
                blocks.foreach { case (blockId, buffer) =>
                  logDebug(s"Migrating sub-block ${blockId}")
                  uploadBlockSyncWithTimeout(
                    peer.host,
                    peer.port,
                    peer.executorId,
                    blockId,
                    buffer,
                    StorageLevel.DISK_ONLY,
                    buffer.size())
                  logDebug(s"Migrated sub-block $blockId")
                }
              }
              val durationMs = System.currentTimeMillis() - startTime
              recordUpload(totalBytes, durationMs)
              val stats = uploadStats(peer)
              logInfo(s"Migrated $shuffleBlockInfo (" +
                s"size: ${Utils.bytesToString(totalBytes)}) to $peer " +
                s"in ${durationMs} ms. " +
                s"Throughput: " +
                s"${Utils.bytesToString(stats.currentThroughputBytesPerSec.toLong)}/sec")
            } catch {
              case e: TimeoutException =>
                recordTimeout()
                val totalBytes = blocks.map(b => b._2.size()).sum
                val timeout = calculateUploadTimeout(totalBytes)
                logWarning(s"Upload timeout for $shuffleBlockInfo to $peer " +
                  s"(${Utils.bytesToString(totalBytes)} in ${timeout.toMillis}ms), " +
                  s"total timeouts: ${uploadStats(peer).timeoutCount}")
                keepRunning = false
              case e @ ( _ : IOException | _ : SparkException) =>
                // If a block got deleted before netty opened the file handle, then trying to
                // load the blocks now will fail. This is most likely to occur if we start
                // migrating blocks and then the shuffle TTL cleaner kicks in. However this
                // could also happen with manually managed shuffles or a GC event on the
                // driver a no longer referenced RDD with shuffle files.
                if (bm.migratableResolver.getMigrationBlocks(shuffleBlockInfo).size < blocks.size) {
                  logWarning(s"Skipping block $shuffleBlockInfo, block deleted.")
                } else if (fallbackStorage.isDefined
                    // Confirm peer is not the fallback BM ID because fallbackStorage would already
                    // have been used in the try-block above so there's no point trying again
                    && peer != FallbackStorage.FALLBACK_BLOCK_MANAGER_ID) {
                  fallbackStorage.foreach(_.copy(shuffleBlockInfo, bm))
                } else {
                  logError(s"Error occurred during migrating $shuffleBlockInfo", e)
                  keepRunning = false
                }
              case e: Exception =>
                logError(s"Error occurred during migrating $shuffleBlockInfo", e)
                keepRunning = false
            }
          }
          if (keepRunning) {
            numMigratedShuffles.incrementAndGet()
          } else {
            logWarning(s"Stop migrating shuffle blocks to $peer")
            // Do not mark the block as migrated if it still needs retry
            if (!allowRetry(shuffleBlockInfo, retryCount + 1)) {
              numMigratedShuffles.incrementAndGet()
            }
          }
        } catch {
          case _: InterruptedException =>
            logInfo(s"Stop shuffle block migration${if (keepRunning) " unexpectedly"}.")
            keepRunning = false
          case NonFatal(e) =>
            keepRunning = false
            logError("Error occurred during shuffle blocks migration.", e)
        }
      }
    }
  }

  // Shuffles which are either in queue for migrations or migrated
  private[storage] val migratingShuffles = mutable.HashSet[ShuffleBlockInfo]()

  // Shuffles which have migrated. This used to know when we are "done", being done can change
  // if a new shuffle file is created by a running task.
  private[storage] val numMigratedShuffles = new AtomicInteger(0)

  // Shuffles which are queued for migration & number of retries so far.
  // Visible in storage for testing.
  private[storage] val shufflesToMigrate =
    new java.util.concurrent.ConcurrentLinkedQueue[(ShuffleBlockInfo, Int)]()

  // Set if we encounter an error attempting to migrate and stop.
  @volatile private var stopped = false
  @volatile private[storage] var stoppedRDD =
    !conf.get(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED)
  @volatile private var stoppedShuffle =
    !conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)

  private val migrationPeers =
    mutable.HashMap[BlockManagerId, ShuffleMigrationRunnable]()

  private val rddBlockMigrationExecutor =
    if (conf.get(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED)) {
      Some(ThreadUtils.newDaemonSingleThreadExecutor("block-manager-decommission-rdd"))
    } else None

  private val rddBlockMigrationRunnable = new Runnable {
    val sleepInterval = conf.get(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL)

    override def run(): Unit = {
      logInfo("Attempting to migrate all RDD blocks")
      while (!stopped && !stoppedRDD) {
        // Validate if we have peers to migrate to. Otherwise, give up migration.
        if (!bm.getPeers(false).exists(_ != FallbackStorage.FALLBACK_BLOCK_MANAGER_ID)) {
          logWarning("No available peers to receive RDD blocks, stop migration.")
          stoppedRDD = true
        } else {
          try {
            val startTime = System.nanoTime()
            logInfo("Attempting to migrate all cached RDD blocks")
            rddBlocksLeft = decommissionRddCacheBlocks()
            lastRDDMigrationTime = startTime
            logInfo(s"Finished current round RDD blocks migration, " +
              s"waiting for ${sleepInterval}ms before the next round migration.")
            Thread.sleep(sleepInterval)
          } catch {
            case _: InterruptedException =>
              logInfo(s"Stop RDD blocks migration${if (!stopped && !stoppedRDD) " unexpectedly"}.")
              stoppedRDD = true
            case NonFatal(e) =>
              logError("Error occurred during RDD blocks migration.", e)
              stoppedRDD = true
          }
        }
      }
    }
  }

  private val shuffleBlockMigrationRefreshExecutor =
    if (conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)) {
      Some(ThreadUtils.newDaemonSingleThreadExecutor("block-manager-decommission-shuffle"))
    } else None

  private val shuffleBlockMigrationRefreshRunnable = new Runnable {
    val sleepInterval = conf.get(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL)

    override def run(): Unit = {
      logInfo("Attempting to migrate all shuffle blocks")
      while (!stopped && !stoppedShuffle) {
        try {
          val startTime = System.nanoTime()
          shuffleBlocksLeft = refreshMigratableShuffleBlocks()
          lastShuffleMigrationTime = startTime
          logInfo(s"Finished current round refreshing migratable shuffle blocks, " +
            s"waiting for ${sleepInterval}ms before the next round refreshing.")
          Thread.sleep(sleepInterval)
        } catch {
          case _: InterruptedException if stopped =>
            logInfo("Stop refreshing migratable shuffle blocks.")
          case NonFatal(e) =>
            logError("Error occurred during shuffle blocks migration.", e)
            stoppedShuffle = true
        }
      }
    }
  }

  private val shuffleMigrationPool =
    if (conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)) {
      Some(ThreadUtils.newDaemonCachedThreadPool("migrate-shuffles",
        conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_MAX_THREADS)))
    } else None

  /**
   * Calculate total shuffle data size per executor by querying MapOutputTracker.
   * This helps identify executors with low data (likely future decommission candidates)
   * vs high data (overloaded) to make better migration targeting decisions.
   */
  private def calculateExecutorShuffleLoads(): Map[String, Long] = {
    val tracker = Option(mapOutputTracker).orElse {
      Option(SparkEnv.get).map(_.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster])
    }

    if (tracker.isEmpty) {
      logDebug("No MapOutputTracker available, cannot calculate executor loads")
      return Map.empty[String, Long]
    }

    val loads = mutable.Map[String, Long]()

    tracker.get.shuffleStatuses.values.foreach {
      shuffleStatus =>
      shuffleStatus.withMapStatuses { statuses =>
        statuses.filter(_ != null).foreach { status =>
          val executorId = status.location.executorId
          // Sum all partition sizes for this map output
          var totalSize = 0L
          var partId = 0
          try {
            // Keep summing until we get an exception (reached the end)
            while (true) {
              val size = status.getSizeForBlock(partId)
              if (size > 0) totalSize += size
              partId += 1
            }
          } catch {
            case _: Exception => // Expected when we've gone past the last partition
          }
          loads(executorId) = loads.getOrElse(executorId, 0L) + totalSize
        }
      }
    }

    val result = loads.toMap
    if (result.nonEmpty) {
      logDebug(s"Calculated shuffle loads for ${result.size} executors: " +
        result.toSeq.sortBy(_._2).map { case (exec, load) =>
          s"$exec=${Utils.bytesToString(load)}"
        }.mkString(", "))
    }
    result
  }

  /**
   * Select peers for shuffle migration based on their current shuffle load.
   * Avoids executors with very low data (likely future decommission candidates)
   * and very high data (already overloaded). Targets middle-range executors.
   */
  private def selectPeersByLoad(availablePeers: Set[BlockManagerId]): Seq[BlockManagerId] = {
    if (availablePeers.isEmpty) return Seq.empty

    val executorLoads = calculateExecutorShuffleLoads()

    // If no load data available, fall back to random selection
    if (executorLoads.isEmpty) {
      logInfo("No executor load data available, falling back to random peer selection")
      return Utils.randomize(availablePeers)
    }

    // Sort peers by their current shuffle load (low to high)
    val sortedByLoad = availablePeers.toSeq.sortBy(peer =>
      executorLoads.getOrElse(peer.executorId, 0L))

    val numPeers = sortedByLoad.length
    if (numPeers <= 2) {
      // Too few peers, use all available
      sortedByLoad
    } else {
      // Skip lowest 20% (future decom candidates) and highest 20% (overloaded)
      val skipLow = (numPeers * 0.2).toInt
      val skipHigh = (numPeers * 0.2).toInt
      val middleRange = sortedByLoad.drop(skipLow).dropRight(skipHigh)

      if (middleRange.nonEmpty) {
        val totalLoad = executorLoads.values.sum
        val avgLoad = if (executorLoads.nonEmpty) totalLoad / executorLoads.size else 0L
        logInfo(s"Selected ${middleRange.length} peers from middle load range " +
          s"(skipped ${skipLow} low-load and ${skipHigh} high-load executors). " +
          s"Avg load: ${Utils.bytesToString(avgLoad)}")
        logDebug(s"Selected peers: ${middleRange.map(_.executorId).mkString(", ")}")
        middleRange
      } else {
        // Fallback if middle range is empty
        logWarning("Middle load range empty, falling back to all available peers")
        sortedByLoad
      }
    }
  }

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
    val remainedShuffles = migratingShuffles.size - numMigratedShuffles.get()
    logInfo(s"${newShufflesToMigrate.size} of ${localShuffles.size} local shuffles " +
      s"are added. In total, $remainedShuffles shuffles are remained.")

    // Update the threads doing migrations
    val livePeerSet = bm.getPeers(false).toSet
    val currentPeerSet = migrationPeers.keys.toSet
    val deadPeers = currentPeerSet.diff(livePeerSet)
    // Select peers based on load balance to avoid future decommission candidates
    // and overloaded executors. Fall back to random selection if load data unavailable.
    val availablePeers = livePeerSet.diff(currentPeerSet)
    val newPeers = if (mapOutputTracker != null) {
      selectPeersByLoad(availablePeers)
    } else {
      // Exact original behavior when no MapOutputTracker
      Utils.randomize(availablePeers)
    }
    migrationPeers ++= newPeers.map { peer =>
      logDebug(s"Starting thread to migrate shuffle blocks to ${peer}")
      val runnable = new ShuffleMigrationRunnable(peer)
      shuffleMigrationPool.foreach(_.submit(runnable))
      (peer, runnable)
    }
    // A peer may have entered a decommissioning state, don't transfer any new blocks
    deadPeers.foreach(migrationPeers.get(_).foreach(_.keepRunning = false))
    // If we don't have anyone to migrate to give up
    if (!migrationPeers.values.exists(_.keepRunning)) {
      logWarning("No available peers to receive Shuffle blocks, stop migration.")
      stoppedShuffle = true
    }
    // If we found any new shuffles to migrate or otherwise have not migrated everything.
    newShufflesToMigrate.nonEmpty || migratingShuffles.size > numMigratedShuffles.get()
  }

  /**
   * Stop migrating shuffle blocks.
   */
  private[storage] def stopMigratingShuffleBlocks(): Unit = {
    shuffleMigrationPool.foreach { threadPool =>
      logInfo("Stopping migrating shuffle blocks.")
      // Stop as gracefully as possible.
      migrationPeers.values.foreach(_.keepRunning = false)
      threadPool.shutdownNow()
    }
  }

  /**
   * Tries to migrate all cached RDD blocks from this BlockManager to peer BlockManagers
   * Visible for testing
   * Returns true if we have not migrated all of our RDD blocks.
   */
  private[storage] def decommissionRddCacheBlocks(): Boolean = {
    val replicateBlocksInfo = bm.getMigratableRDDBlocks()
    // Refresh peers and validate we have somewhere to move blocks.

    if (replicateBlocksInfo.nonEmpty) {
      logInfo(s"Need to replicate ${replicateBlocksInfo.size} RDD blocks " +
        "for block manager decommissioning")
    } else {
      logWarning(s"Asked to decommission RDD cache blocks, but no blocks to migrate")
      return false
    }

    // TODO: We can sort these blocks based on some policy (LRU/blockSize etc)
    //   so that we end up prioritize them over each other
    val blocksFailedReplication = replicateBlocksInfo.map { replicateBlock =>
        val replicatedSuccessfully = migrateBlock(replicateBlock)
        (replicateBlock.blockId, replicatedSuccessfully)
    }.filterNot(_._2).map(_._1)
    if (blocksFailedReplication.nonEmpty) {
      logWarning("Blocks failed replication in cache decommissioning " +
        s"process: ${blocksFailedReplication.mkString(",")}")
      return true
    }
    false
  }

  private def migrateBlock(blockToReplicate: ReplicateBlock): Boolean = {
    val replicatedSuccessfully = bm.replicateBlock(
      blockToReplicate.blockId,
      blockToReplicate.replicas.toSet,
      blockToReplicate.maxReplicas,
      maxReplicationFailures = Some(maxReplicationFailuresForDecommission))
    if (replicatedSuccessfully) {
      logInfo(s"Block ${blockToReplicate.blockId} migrated successfully, Removing block now")
      bm.removeBlock(blockToReplicate.blockId)
      logInfo(s"Block ${blockToReplicate.blockId} removed")
    } else {
      logWarning(s"Failed to migrate block ${blockToReplicate.blockId}")
    }
    replicatedSuccessfully
  }

  def start(): Unit = {
    logInfo("Starting block migration")
    logInfo(s"Configuration: RDD migration=" +
      s"${conf.get(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED)}, " +
      s"shuffle migration=${conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)}, " +
      s"size-based timeouts=$enableSizeBasedTimeouts")
    logInfo(s"Migration settings: max failures per block=$maxReplicationFailuresForDecommission, " +
      s"min throughput=${Utils.bytesToString(minUploadThroughputBytesPerSec)}/sec, " +
      s"timeout rate=${timeoutMbPerSec}MB/sec")
    if (fallbackStorage.isDefined) {
      logInfo("Fallback storage is enabled for failed migrations")
    }
    if (mapOutputTracker != null) {
      logInfo("Load-balanced peer selection enabled for shuffle migration")
    }

    rddBlockMigrationExecutor.foreach(_.submit(rddBlockMigrationRunnable))
    shuffleBlockMigrationRefreshExecutor.foreach(_.submit(shuffleBlockMigrationRefreshRunnable))
  }

  def stop(): Unit = {
    if (stopped) {
      return
    } else {
      stopped = true
    }
    try {
      rddBlockMigrationExecutor.foreach(_.shutdownNow())
    } catch {
      case NonFatal(e) =>
        logError(s"Error during shutdown RDD block migration thread", e)
    }
    try {
      shuffleBlockMigrationRefreshExecutor.foreach(_.shutdownNow())
    } catch {
      case NonFatal(e) =>
        logError(s"Error during shutdown shuffle block refreshing thread", e)
    }
    try {
      stopMigratingShuffleBlocks()
    } catch {
      case NonFatal(e) =>
        logError(s"Error during shutdown shuffle block migration thread", e)
    }
    logInfo("Stopped block migration")
  }

  /*
   *  Returns the last migration time and a boolean for if all blocks have been migrated.
   *  The last migration time is calculated to be the minimum of the last migration of any
   *  running migration (and if there are now current running migrations it is set to current).
   *  This provides a timeStamp which, if there have been no tasks running since that time
   *  we can know that all potential blocks that can be have been migrated off.
   */
  private[storage] def lastMigrationInfo(): (Long, Boolean) = {
    if (stopped || (stoppedRDD && stoppedShuffle)) {
      // Since we don't have anything left to migrate ever (since we don't restart once
      // stopped), return that we're done with a validity timestamp that doesn't expire.
      (Long.MaxValue, true)
    } else {
      // Chose the min of the active times. See the function description for more information.
      val lastMigrationTime = if (!stoppedRDD && !stoppedShuffle) {
        Math.min(lastRDDMigrationTime, lastShuffleMigrationTime)
      } else if (!stoppedShuffle) {
        lastShuffleMigrationTime
      } else {
        lastRDDMigrationTime
      }

      // Technically we could have blocks left if we encountered an error, but those blocks will
      // never be migrated, so we don't care about them.
      val blocksMigrated = (!shuffleBlocksLeft || stoppedShuffle) && (!rddBlocksLeft || stoppedRDD)
      (lastMigrationTime, blocksMigrated)
    }
  }
}
