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
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.reflectiveCalls
import scala.util.control.NonFatal

import org.apache.spark._
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
    bm: BlockManager) extends Logging {

  private val fallbackStorage = FallbackStorage.getFallbackStorage(conf)
  private val maxReplicationFailuresForDecommission =
    conf.get(config.STORAGE_DECOMMISSION_MAX_REPLICATION_FAILURE_PER_BLOCK)

  // Shutdown timeout for graceful termination
  private val shutdownTimeoutMs =
    conf.get(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL) * 10

  // Maximum time to wait for peers before giving up (in milliseconds)
  private val maxPeerWaitTime =
    conf.getLong("spark.storage.decommission.max.peer.wait.time", 300000L) // 5 minutes

  // Minimum number of peers required for migration
  private val minRequiredPeers =
    conf.getInt("spark.storage.decommission.min.peers", 1)

  // Used for tracking if our migrations are complete. Readable for testing
  // Use AtomicLong for better thread safety and performance
  private[storage] val lastRDDMigrationTime = new AtomicLong(0)
  private[storage] val lastShuffleMigrationTime = new AtomicLong(0)
  @volatile private[storage] var rddBlocksLeft: Boolean = true
  @volatile private[storage] var shuffleBlocksLeft: Boolean = true

  // Enhanced metrics for better monitoring
  private[storage] val totalRDDBlocksMigrated = new AtomicInteger(0)
  private[storage] val totalShuffleBlocksMigrated = new AtomicInteger(0)
  private[storage] val migrationErrors = new AtomicInteger(0)

  // Track migration start time for rate calculations
  private val migrationStartTime = new AtomicLong(0)

  // Track when we started waiting for peers
  private val peerWaitStartTime = new AtomicLong(0)

  // Periodic status logging
  private val statusLogInterval =
    conf.getLong("spark.storage.decommission.status.log.interval", 30000L)
  private var lastStatusLog = 0L

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

    private def handleMigrationError(shuffleBlockInfo: ShuffleBlockInfo,
                                   originalBlocks: List[(BlockId, _)],
                                   error: Exception,
                                   retryCount: Int): Unit = {
      // If a block got deleted before netty opened the file handle, then trying to
      // load the blocks now will fail. This is most likely to occur if we start
      // migrating blocks and then the shuffle TTL cleaner kicks in. However this
      // could also happen with manually managed shuffles or a GC event on the
      // driver a no longer referenced RDD with shuffle files.
      if (bm.migratableResolver.getMigrationBlocks(shuffleBlockInfo).size < originalBlocks.size) {
        logWarning(s"Skipping block $shuffleBlockInfo, block deleted.")
      } else if (fallbackStorage.isDefined
          // Confirm peer is not the fallback BM ID because fallbackStorage would already
          // have been used in the try-block above so there's no point trying again
          && peer != FallbackStorage.FALLBACK_BLOCK_MANAGER_ID) {
        try {
          fallbackStorage.foreach(_.copy(shuffleBlockInfo, bm))
          logInfo(s"Successfully migrated $shuffleBlockInfo to fallback storage")
        } catch {
          case fallbackError: Exception =>
            logError(s"Fallback storage migration failed for $shuffleBlockInfo", fallbackError)
            migrationErrors.incrementAndGet()
            keepRunning = false
        }
      } else {
        logError(s"Error occurred during migrating $shuffleBlockInfo", error)
        migrationErrors.incrementAndGet()
        keepRunning = false
      }
    }

    private def allowRetry(shuffleBlock: ShuffleBlockInfo, failureNum: Int): Boolean = {
      if (failureNum < maxReplicationFailuresForDecommission) {
        logInfo(s"Add $shuffleBlock back to migration queue for " +
          s"retry ($failureNum / $maxReplicationFailuresForDecommission)")
        // The block needs to retry so we should not mark it as finished
        shufflesToMigrate.add((shuffleBlock, failureNum))
        true
      } else {
        logWarning(s"Give up migrating $shuffleBlock since it's been " +
          s"failed for $maxReplicationFailuresForDecommission times")
        migrationErrors.incrementAndGet()
        false
      }
    }

    private def nextShuffleBlockToMigrate(): Option[(ShuffleBlockInfo, Int)] = {
      // Check if this peer should take the next block based on load balancing
      if (!Thread.currentThread().isInterrupted && keepRunning) {
        val nextBlock = getNextBlockForPeer(peer)
        if (nextBlock.isEmpty) {
          // No block assigned to this peer, wait briefly
          Thread.sleep(100)
        }
        nextBlock
      } else {
        None
      }
    }

    override def run(): Unit = {
      logInfo(s"Starting shuffle block migration thread for $peer")
      // Once a block fails to transfer to an executor stop trying to transfer more blocks
      while (keepRunning) {
        try {
          nextShuffleBlockToMigrate() match {
            case Some((shuffleBlockInfo, retryCount)) =>
          val blocks = bm.migratableResolver.getMigrationBlocks(shuffleBlockInfo)
          // We only migrate a shuffle block when both index file and data file exist.
          if (blocks.isEmpty) {
            logInfo(s"Ignore deleted shuffle block $shuffleBlockInfo")
          } else {
            logInfo(s"Got migration sub-blocks $blocks. Trying to migrate $shuffleBlockInfo " +
              s"to $peer ($retryCount / $maxReplicationFailuresForDecommission)")
            // Migrate the components of the blocks.
            try {
              val startTime = System.currentTimeMillis()
              if (fallbackStorage.isDefined && peer == FallbackStorage.FALLBACK_BLOCK_MANAGER_ID) {
                fallbackStorage.foreach(_.copy(shuffleBlockInfo, bm))
              } else {
                blocks.foreach { case (blockId, buffer) =>
                  logDebug(s"Migrating sub-block ${blockId}")
                  bm.blockTransferService.uploadBlockSync(
                    peer.host,
                    peer.port,
                    peer.executorId,
                    blockId,
                    buffer,
                    StorageLevel.DISK_ONLY,
                    null) // class tag, we don't need for shuffle
                  logDebug(s"Migrated sub-block $blockId")
                }
              }
              val migrationTime = System.currentTimeMillis() - startTime
              val totalSize = blocks.map(b => b._2.size()).sum
              logInfo(s"Migrated $shuffleBlockInfo (" +
                s"size: ${Utils.bytesToString(totalSize)}) to $peer " +
                s"in ${migrationTime} ms")
              totalShuffleBlocksMigrated.incrementAndGet()

              // Track bytes migrated to this peer for load balancing
              peerMigrationBytes.computeIfAbsent(peer, _ => new AtomicLong(0))
                .addAndGet(totalSize)

              // Track shuffle sizes migrated per executor for intelligent placement
              val shuffleId = shuffleBlockInfo.shuffleId
              shuffleSizesMigratedPerExecutor.computeIfAbsent(shuffleId,
                _ => new java.util.concurrent.ConcurrentHashMap[BlockManagerId, AtomicLong]())
                .computeIfAbsent(peer, _ => new AtomicLong(0))
                .addAndGet(totalSize)
            } catch {
              case e @ ( _ : IOException | _ : SparkException) =>
                handleMigrationError(shuffleBlockInfo, blocks,
                  e.asInstanceOf[Exception], retryCount)
              case e: Exception =>
                logError(s"Unexpected error during migrating $shuffleBlockInfo", e)
                migrationErrors.incrementAndGet()
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
            case None =>
              // No block to migrate right now, continue loop
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

  // Per-peer load tracking for balanced distribution
  private[storage] val peerMigrationCounts =
    new java.util.concurrent.ConcurrentHashMap[BlockManagerId, AtomicInteger]()
  private[storage] val peerMigrationBytes =
    new java.util.concurrent.ConcurrentHashMap[BlockManagerId, AtomicLong]()

  // Round-robin index for load balancing
  private val nextPeerIndex = new AtomicInteger(0)

  // Cached shuffle size information per executor per shuffle to avoid frequent
  // MapOutputTracker calls
  private val shuffleSizeCache =
    new java.util.concurrent.ConcurrentHashMap[Int, Map[BlockManagerId, Long]]()
  private val shuffleSizeCacheTimestamp = new java.util.concurrent.ConcurrentHashMap[Int, Long]()
  private val shuffleSizeCacheTimeout =
    conf.getLong("spark.storage.decommission.shuffle.size.cache.timeout", 60000L) // 1 minute

  // Track shuffle sizes migrated per executor per shuffle for intelligent placement
  private val shuffleSizesMigratedPerExecutor =
    new java.util.concurrent.ConcurrentHashMap[Int,
      java.util.concurrent.ConcurrentHashMap[BlockManagerId, AtomicLong]]()

  // Set if we encounter an error attempting to migrate and stop.
  @volatile private var stopped = false
  @volatile private[storage] var stoppedRDD =
    !conf.get(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED)
  @volatile private[storage] var stoppedShuffle =
    !conf.get(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED)

  private val migrationPeers =
    mutable.HashMap[BlockManagerId, ShuffleMigrationRunnable]()

  /**
   * Get shuffle sizes per executor for a given shuffle, with caching to avoid frequent calls.
   */
  private def getShuffleSizesPerExecutor(shuffleId: Int): Map[BlockManagerId, Long] = {
    val currentTime = System.currentTimeMillis()
    val cacheTime = shuffleSizeCacheTimestamp.getOrDefault(shuffleId, 0L)

    // Return cached data if still valid
    if (currentTime - cacheTime < shuffleSizeCacheTimeout) {
      return shuffleSizeCache.getOrDefault(shuffleId, Map.empty)
    }

    try {
      // Query MapOutputTracker for all reduce partitions of this shuffle
      val executorSizes = mutable.Map[BlockManagerId, Long]()

      // Get map output locations and sizes for all reducers
      // We aggregate across all reduce partitions to get total shuffle size per executor
      // TODO: Access mapOutputTracker properly through SparkEnv
      val iter: Iterator[(BlockManagerId, Seq[(org.apache.spark.storage.BlockId, Long, Int)])] =
        Iterator.empty

      iter.foreach { case (executorId, blocks) =>
        val totalSize = blocks.map(_._2).sum
        if (totalSize > 0) {
          executorSizes(executorId) = executorSizes.getOrElse(executorId, 0L) + totalSize
        }
      }

      val result = executorSizes.toMap

      // Cache the result
      shuffleSizeCache.put(shuffleId, result)
      shuffleSizeCacheTimestamp.put(shuffleId, currentTime)

      logDebug(s"Cached shuffle sizes for shuffle $shuffleId: ${result.map { case (exec, size) =>
        s"${exec.executorId}=${Utils.bytesToString(size)}" }.mkString(", ")}")

      result
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to get shuffle sizes for shuffle $shuffleId from MapOutputTracker", e)
        Map.empty
    }
  }

  /**
   * Get the current shuffle load per executor for a given shuffle ID.
   * This includes both existing shuffle data and data we've already migrated.
   */
  private def getCurrentShuffleLoadPerExecutor(
      shuffleId: Int, peers: Seq[BlockManagerId]): Map[BlockManagerId, Long] = {
    val originalSizes = getShuffleSizesPerExecutor(shuffleId)
    val migratedSizes = shuffleSizesMigratedPerExecutor.getOrDefault(shuffleId,
      new java.util.concurrent.ConcurrentHashMap[BlockManagerId, AtomicLong]())

    peers.map { peer =>
      val originalSize = originalSizes.getOrElse(peer, 0L)
      val migratedSize = migratedSizes.getOrDefault(peer, new AtomicLong(0)).get()
      peer -> (originalSize + migratedSize)
    }.toMap
  }

  /**
   * Find the best target executor for migrating a shuffle block, considering existing
   * shuffle distribution.
   */
  private def findBestTargetForShuffle(
      shuffleId: Int, peers: Seq[BlockManagerId], blockSize: Long = 0L): Option[BlockManagerId] = {
    if (peers.isEmpty) return None

    val currentLoads = getCurrentShuffleLoadPerExecutor(shuffleId, peers)

    // Find executor with minimum shuffle load for this shuffle
    val sortedByLoad = peers.sortBy(peer => currentLoads.getOrElse(peer, 0L))

    logDebug(s"Shuffle $shuffleId load distribution: ${sortedByLoad.map(p =>
      s"${p.executorId}=${Utils.bytesToString(currentLoads.getOrElse(p, 0L))}").mkString(", ")}")

    Some(sortedByLoad.head)
  }

  /**
   * Load-aware block assignment to peers. Uses a combination of round-robin and load balancing
   * to ensure even distribution of shuffle blocks across peers.
   */
  private def getNextBlockForPeer(targetPeer: BlockManagerId): Option[(ShuffleBlockInfo, Int)] = {
    if (shufflesToMigrate.isEmpty) {
      return None
    }

    // Get current load for all active peers
    val activePeers = migrationPeers.keys.filter(peer =>
      migrationPeers.get(peer).exists(_.keepRunning)).toSeq

    if (activePeers.isEmpty) {
      return None
    }

    // Peek at the next block to determine optimal placement
    val peekResult = Option(shufflesToMigrate.peek())
    peekResult match {
      case Some((shuffleBlockInfo, _)) =>
        // Find the best target for this shuffle based on existing shuffle distribution
        findBestTargetForShuffle(shuffleBlockInfo.shuffleId, activePeers) match {
          case Some(bestTarget) if bestTarget == targetPeer =>
            // This peer is the best target, assign the block
            Option(shufflesToMigrate.poll()) match {
              case Some(block) =>
                // Track assignment to this peer
                peerMigrationCounts.computeIfAbsent(targetPeer, _ => new AtomicInteger(0))
                  .incrementAndGet()
                Some(block)
              case None => None
            }
          case Some(bestTarget) =>
            // Another peer is better suited, don't assign to this peer
            logDebug(s"Block ${shuffleBlockInfo} better suited for ${bestTarget.executorId} " +
              s"than ${targetPeer.executorId}")
            None
          case None =>
            // Fallback to original load balancing if shuffle size info unavailable
            val peerLoads = activePeers.map { peer =>
              val count = peerMigrationCounts.getOrDefault(peer, new AtomicInteger(0)).get()
              (peer, count)
            }

            val minLoad = peerLoads.minBy(_._2)._2
            val lightLoadedPeers = peerLoads.filter(_._2 <= minLoad + 1).map(_._1)

            if (lightLoadedPeers.contains(targetPeer)) {
              Option(shufflesToMigrate.poll()) match {
                case Some(block) =>
                  peerMigrationCounts.computeIfAbsent(targetPeer, _ => new AtomicInteger(0))
                    .incrementAndGet()
                  Some(block)
                case None => None
              }
            } else {
              None
            }
        }
      case None => None
    }
  }

  private val rddBlockMigrationExecutor =
    if (conf.get(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED)) {
      Some(ThreadUtils.newDaemonSingleThreadExecutor("block-manager-decommission-rdd"))
    } else None

  private val rddBlockMigrationRunnable = new Runnable {
    val sleepInterval = conf.get(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL)
    // Batch size for RDD migration to avoid overwhelming the system
    val batchSize = conf.getInt("spark.storage.decommission.rdd.batch.size", 100)

    override def run(): Unit = {
      logInfo("Attempting to migrate all RDD blocks")
      while (!stopped && !stoppedRDD) {
        // Validate if we have peers to migrate to. Otherwise, wait or give up migration.
        val availablePeers = bm.getPeers(false)
          .filter(_ != FallbackStorage.FALLBACK_BLOCK_MANAGER_ID)
        if (availablePeers.size < minRequiredPeers) {
          handleInsufficientPeers("RDD", availablePeers.size)
          if (stoppedRDD) return
        } else {
          // Reset peer wait time when we have sufficient peers
          peerWaitStartTime.set(0)
          try {
            val startTime = System.nanoTime()
            logInfo("Attempting to migrate all cached RDD blocks")
            val migrationResult = decommissionRddCacheBlocks()
            rddBlocksLeft = migrationResult.hasMoreBlocks
            lastRDDMigrationTime.set(startTime)

            logInfo(s"Finished current round RDD blocks migration. " +
              s"Migrated ${migrationResult.migratedCount} blocks, " +
              s"failed ${migrationResult.failedCount} blocks. " +
              s"Waiting ${sleepInterval}ms before next round.")
            logMigrationStatus()
            checkForStuckMigration()
            Thread.sleep(sleepInterval)
          } catch {
            case _: InterruptedException =>
              logInfo(s"Stop RDD blocks migration${if (!stopped && !stoppedRDD) " unexpectedly"}.")
              stoppedRDD = true
            case NonFatal(e) =>
              logError("Error occurred during RDD blocks migration.", e)
              migrationErrors.incrementAndGet()
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
          lastShuffleMigrationTime.set(startTime)
          logInfo(s"Finished current round refreshing migratable shuffle blocks, " +
            s"waiting for ${sleepInterval}ms before the next round refreshing.")
          logMigrationStatus()
          checkForStuckMigration()
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
    // Randomize the orders of the peers to avoid hotspot nodes.
    val newPeers = Utils.randomize(livePeerSet.diff(currentPeerSet))
    migrationPeers ++= newPeers.map { peer =>
      logDebug(s"Starting thread to migrate shuffle blocks to ${peer}")
      // Initialize per-peer tracking
      peerMigrationCounts.putIfAbsent(peer, new AtomicInteger(0))
      peerMigrationBytes.putIfAbsent(peer, new AtomicLong(0))
      val runnable = new ShuffleMigrationRunnable(peer)
      shuffleMigrationPool.foreach(_.submit(runnable))
      (peer, runnable)
    }
    // A peer may have entered a decommissioning state, don't transfer any new blocks
    deadPeers.foreach(migrationPeers.get(_).foreach(_.keepRunning = false))
    // If we don't have anyone to migrate to, handle insufficient peers
    val activePeers = migrationPeers.values.count(_.keepRunning)
    if (activePeers < minRequiredPeers) {
      handleInsufficientPeers("Shuffle", activePeers)
    } else {
      // Reset peer wait time when we have sufficient peers
      peerWaitStartTime.set(0)
    }
    // If we found any new shuffles to migrate or otherwise have not migrated everything.
    newShufflesToMigrate.nonEmpty || migratingShuffles.size > numMigratedShuffles.get()
  }

  /**
   * Handle insufficient peers for migration with retry logic.
   */
  private def handleInsufficientPeers(blockType: String, currentPeerCount: Int): Unit = {
    val currentTime = System.currentTimeMillis()

    // Start tracking wait time if not already started
    if (peerWaitStartTime.get() == 0) {
      peerWaitStartTime.set(currentTime)
      logWarning(s"Insufficient peers for $blockType migration: $currentPeerCount < " +
        s"$minRequiredPeers. Waiting up to ${maxPeerWaitTime / 1000}s for more peers...")
    }

    val waitTime = currentTime - peerWaitStartTime.get()
    if (waitTime > maxPeerWaitTime) {
      logError(s"Giving up $blockType migration after waiting ${waitTime / 1000}s for peers. " +
        s"Required: $minRequiredPeers, Available: $currentPeerCount")
      if (blockType == "RDD") {
        stoppedRDD = true
      } else {
        stoppedShuffle = true
      }
    } else {
      val remainingWait = maxPeerWaitTime - waitTime
      logInfo(s"Still waiting for $blockType migration peers. " +
        s"Required: $minRequiredPeers, Available: $currentPeerCount. " +
        s"Will wait ${remainingWait / 1000}s more...")
      try {
        Thread.sleep(Math.min(5000L, remainingWait)) // Wait up to 5 seconds before retry
      } catch {
        case _: InterruptedException =>
          if (blockType == "RDD") stoppedRDD = true else stoppedShuffle = true
      }
    }
  }

  /**
   * Stop migrating shuffle blocks.
   */
  private[storage] def stopMigratingShuffleBlocks(): Unit = {
    shuffleMigrationPool.foreach { threadPool =>
      logInfo("Stopping migrating shuffle blocks.")
      // Stop as gracefully as possible.
      migrationPeers.values.foreach(_.keepRunning = false)
      threadPool.shutdown()
      if (!threadPool.awaitTermination(shutdownTimeoutMs, TimeUnit.MILLISECONDS)) {
        logWarning(s"Shuffle migration pool did not terminate within " +
          s"${shutdownTimeoutMs}ms, forcing shutdown")
        threadPool.shutdownNow()
      }
    }
  }

  // Case class to hold RDD migration results
  case class RDDMigrationResult(
      migratedCount: Int, failedCount: Int, hasMoreBlocks: Boolean)

  /**
   * Tries to migrate all cached RDD blocks from this BlockManager to peer BlockManagers
   * Visible for testing
   * Returns migration result with counts and whether more blocks remain.
   */
  private[storage] def decommissionRddCacheBlocks(): RDDMigrationResult = {
    val replicateBlocksInfo = bm.getMigratableRDDBlocks()

    if (replicateBlocksInfo.isEmpty) {
      logInfo("No RDD blocks to migrate")
      return RDDMigrationResult(0, 0, false)
    }

    logInfo(s"Need to replicate ${replicateBlocksInfo.size} RDD blocks " +
      "for block manager decommissioning")

    // Sort blocks by size (smaller first) for better migration efficiency
    val sortedBlocks = replicateBlocksInfo.sortBy { block =>
      bm.getLocalBytes(block.blockId) match {
        case Some(size) => size.asInstanceOf[Long]
        case None => 0L
      }
    }

    var migratedCount = 0
    var failedCount = 0
    val failedBlocks = mutable.ArrayBuffer[BlockId]()

    // Process blocks in batches for better performance
    val batchSize = rddBlockMigrationRunnable.batchSize
    for (batch <- sortedBlocks.grouped(batchSize)) {
      if (stopped || stoppedRDD) {
        return RDDMigrationResult(migratedCount, failedCount, true)
      }

      val batchResults = batch.map { replicateBlock =>
        val success = migrateBlock(replicateBlock)
        if (success) {
          migratedCount += 1
          totalRDDBlocksMigrated.incrementAndGet()
        } else {
          failedCount += 1
          failedBlocks += replicateBlock.blockId
        }
        success
      }

      val batchSuccessCount = batchResults.count(identity)
      logDebug(s"Batch migration completed: $batchSuccessCount/${batch.size} blocks succeeded")
    }

    if (failedBlocks.nonEmpty) {
      logWarning(s"Failed to migrate ${failedBlocks.size} RDD blocks: " +
        s"${failedBlocks.take(10).mkString(",")}" +
        (if (failedBlocks.size > 10) "..." else ""))
    }

    RDDMigrationResult(migratedCount, failedCount, failedCount > 0)
  }

  private def migrateBlock(blockToReplicate: ReplicateBlock): Boolean = {
    val startTime = System.currentTimeMillis()
    val blockSize = bm.getLocalBytes(blockToReplicate.blockId)
      .map(_.asInstanceOf[Long]).getOrElse(0L)

    try {
      val replicatedSuccessfully = bm.replicateBlock(
        blockToReplicate.blockId,
        blockToReplicate.replicas.toSet,
        blockToReplicate.maxReplicas,
        maxReplicationFailures = Some(maxReplicationFailuresForDecommission))

      if (replicatedSuccessfully) {
        val migrationTime = System.currentTimeMillis() - startTime
        logInfo(s"Block ${blockToReplicate.blockId} migrated successfully " +
          s"(size: ${Utils.bytesToString(blockSize)}, time: ${migrationTime}ms). " +
          s"Removing block now.")
        bm.removeBlock(blockToReplicate.blockId)
        logDebug(s"Block ${blockToReplicate.blockId} removed from local storage")
      } else {
        logWarning(s"Failed to migrate block ${blockToReplicate.blockId} " +
          s"(size: ${Utils.bytesToString(blockSize)}) after " +
          s"${maxReplicationFailuresForDecommission} attempts")
        migrationErrors.incrementAndGet()
      }
      replicatedSuccessfully
    } catch {
      case NonFatal(e) =>
        logError(s"Exception during migration of block ${blockToReplicate.blockId}", e)
        migrationErrors.incrementAndGet()
        false
    }
  }

  def start(): Unit = {
    logInfo("Starting block migration")
    migrationStartTime.set(System.currentTimeMillis())
    lastStatusLog = System.currentTimeMillis()
    rddBlockMigrationExecutor.foreach(_.submit(rddBlockMigrationRunnable))
    shuffleBlockMigrationRefreshExecutor.foreach(_.submit(shuffleBlockMigrationRefreshRunnable))
  }

  /**
   * Log periodic status updates about migration progress
   */
  private def logMigrationStatus(): Unit = {
    val currentTime = System.currentTimeMillis()
    if (currentTime - lastStatusLog >= statusLogInterval) {
      val stats = getMigrationStats()
      val elapsedMinutes = (currentTime - migrationStartTime.get()) / 60000.0
      val rddRate = if (elapsedMinutes > 0) stats("rddBlocksMigrated") / elapsedMinutes else 0.0
      val shuffleRate = if (elapsedMinutes > 0) {
        stats("shuffleBlocksMigrated") / elapsedMinutes
      } else {
        0.0
      }

      logInfo(s"Migration status after ${elapsedMinutes.formatted("%.1f")} minutes: " +
        s"RDD blocks: ${stats("rddBlocksMigrated")} (${rddRate.formatted("%.1f")}/min), " +
        s"Shuffle blocks: ${stats("shuffleBlocksMigrated")} " +
        s"(${shuffleRate.formatted("%.1f")}/min), " +
        s"Remaining shuffles: ${stats("remainingShuffles")}, " +
        s"Errors: ${stats("migrationErrors")}, " +
        s"Active migration threads: ${migrationPeers.values.count(_.keepRunning)}")

      lastStatusLog = currentTime
    }
  }

  def stop(): Unit = {
    if (stopped) {
      return
    } else {
      stopped = true
    }

    logInfo("Initiating graceful shutdown of block migration")
    val shutdownStart = System.currentTimeMillis()

    // First, signal all threads to stop gracefully
    migrationPeers.values.foreach(_.keepRunning = false)

    // Try graceful shutdown with timeout
    try {
      rddBlockMigrationExecutor.foreach { executor =>
        executor.shutdown()
        if (!executor.awaitTermination(shutdownTimeoutMs, TimeUnit.MILLISECONDS)) {
          logWarning(s"RDD migration executor did not terminate within " +
            s"${shutdownTimeoutMs}ms, forcing shutdown")
          executor.shutdownNow()
        }
      }
    } catch {
      case NonFatal(e) =>
        logError(s"Error during shutdown RDD block migration thread", e)
    }

    try {
      shuffleBlockMigrationRefreshExecutor.foreach { executor =>
        executor.shutdown()
        if (!executor.awaitTermination(shutdownTimeoutMs, TimeUnit.MILLISECONDS)) {
          logWarning(s"Shuffle refresh executor did not terminate within " +
            s"${shutdownTimeoutMs}ms, forcing shutdown")
          executor.shutdownNow()
        }
      }
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

    val shutdownTime = System.currentTimeMillis() - shutdownStart
    logInfo(s"Stopped block migration in ${shutdownTime}ms. " +
      s"Migrated ${totalRDDBlocksMigrated.get()} RDD blocks and " +
      s"${totalShuffleBlocksMigrated.get()} shuffle blocks. " +
      s"Encountered ${migrationErrors.get()} errors.")
  }

  /**
   * Returns migration statistics for monitoring.
   */
  private[storage] def getMigrationStats(): Map[String, Long] = {
    Map(
      "rddBlocksMigrated" -> totalRDDBlocksMigrated.get().toLong,
      "shuffleBlocksMigrated" -> totalShuffleBlocksMigrated.get().toLong,
      "migrationErrors" -> migrationErrors.get().toLong,
      "lastRDDMigrationTime" -> lastRDDMigrationTime.get(),
      "lastShuffleMigrationTime" -> lastShuffleMigrationTime.get(),
      "remainingShuffles" -> (migratingShuffles.size - numMigratedShuffles.get()).toLong
    )
  }

  /**
   * Returns per-peer migration statistics for load balancing monitoring.
   */
  private[storage] def getPeerMigrationStats(): Map[BlockManagerId, (Long, Long)] = {
    val result = mutable.Map[BlockManagerId, (Long, Long)]()

    migrationPeers.keys.foreach { peer =>
      val blockCount = peerMigrationCounts.getOrDefault(peer, new AtomicInteger(0)).get().toLong
      val byteCount = peerMigrationBytes.getOrDefault(peer, new AtomicLong(0)).get()
      result(peer) = (blockCount, byteCount)
    }

    result.toMap
  }

  /**
   * Returns shuffle distribution statistics for monitoring load balancing effectiveness.
   */
  private[storage] def getShuffleDistributionStats(): Map[String, Any] = {
    val stats = mutable.Map[String, Any]()

    // Get active shuffles being migrated
    val activeShuffles = migratingShuffles.map(_.shuffleId).toSet

    if (activeShuffles.nonEmpty) {
      val activePeers = migrationPeers.keys.toSeq

      // Calculate load distribution per shuffle
      val shuffleDistributions = activeShuffles.map { shuffleId =>
        val loads = getCurrentShuffleLoadPerExecutor(shuffleId, activePeers)
        val nonZeroLoads = loads.values.filter(_ > 0).toSeq

        if (nonZeroLoads.nonEmpty) {
          val min = nonZeroLoads.min
          val max = nonZeroLoads.max
          val avg = nonZeroLoads.sum.toDouble / nonZeroLoads.size
          val imbalanceRatio = if (min > 0) max.toDouble / min else Double.MaxValue

          shuffleId -> Map(
            "minLoadBytes" -> min,
            "maxLoadBytes" -> max,
            "avgLoadBytes" -> avg.toLong,
            "imbalanceRatio" -> imbalanceRatio,
            "executorsWithData" -> nonZeroLoads.size
          )
        } else {
          shuffleId -> Map("noData" -> true)
        }
      }.toMap

      stats("shuffleDistributions") = shuffleDistributions
      stats("totalActiveShuffles") = activeShuffles.size

      // Overall balance metrics
      val allLoads = shuffleDistributions.values.flatMap { dist =>
        dist.get("avgLoadBytes").map(_.asInstanceOf[Long])
      }.toSeq

      if (allLoads.nonEmpty) {
        stats("overallMinLoad") = allLoads.min
        stats("overallMaxLoad") = allLoads.max
        stats("overallAvgLoad") = allLoads.sum / allLoads.size
      }
    }

    stats.toMap
  }

  /**
   * Check if migration appears to be stuck and log warning if so.
   */
  private def checkForStuckMigration(): Unit = {
    val currentTime = System.currentTimeMillis()
    val stuckThreshold =
      conf.getLong("spark.storage.decommission.stuck.threshold", 300000L) // 5 minutes

    if (!stoppedRDD && currentTime - lastRDDMigrationTime.get() > stuckThreshold) {
      logWarning(s"RDD migration appears stuck - no progress for " +
        s"${(currentTime - lastRDDMigrationTime.get()) / 1000}s")
    }

    if (!stoppedShuffle && currentTime - lastShuffleMigrationTime.get() > stuckThreshold) {
      logWarning(s"Shuffle migration appears stuck - no progress for " +
        s"${(currentTime - lastShuffleMigrationTime.get()) / 1000}s")
    }

    // Check for stuck migration threads
    val stuckThreads = migrationPeers.filter { case (peer, runnable) =>
      runnable.keepRunning && shufflesToMigrate.size() > 0
    }

    if (stuckThreads.nonEmpty && shufflesToMigrate.size() > 0) {
      logWarning(s"Potential stuck shuffle migration detected: " +
        s"${stuckThreads.size} active threads, " +
        s"${shufflesToMigrate.size()} blocks in queue")
    }
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
        Math.min(lastRDDMigrationTime.get(), lastShuffleMigrationTime.get())
      } else if (!stoppedShuffle) {
        lastShuffleMigrationTime.get()
      } else {
        lastRDDMigrationTime.get()
      }

      // Technically we could have blocks left if we encountered an error, but those blocks will
      // never be migrated, so we don't care about them.
      val blocksMigrated = (!shuffleBlocksLeft || stoppedShuffle) && (!rddBlocksLeft || stoppedRDD)
      (lastMigrationTime, blocksMigrated)
    }
  }
}
