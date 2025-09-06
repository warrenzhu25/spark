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

import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ShuffleBlockInfo
import org.apache.spark.util.Utils

/**
 * Worker that migrates shuffle blocks from one peer to another during decommissioning.
 * Each worker is dedicated to migrating blocks to a specific peer to balance load distribution.
 */
private[storage] class ShuffleMigrationWorker(
    peer: BlockManagerId,
    blockManager: BlockManager,
    shufflesToMigrate: ConcurrentLinkedQueue[(ShuffleBlockInfo, Int)],
    fallbackStorage: Option[FallbackStorage],
    statisticsCollector: MigrationStatisticsCollector,
    stateTracker: MigrationStateTracker,
    errorHandler: MigrationErrorHandler,
    maxReplicationFailures: Int) extends Runnable with Logging {

  @volatile var keepRunning = true

  /**
   * Attempt to add a block back to the migration queue for retry
   */
  private def allowRetry(shuffleBlock: ShuffleBlockInfo, failureNum: Int): Boolean = {
    val shouldRetry = errorHandler.shouldRetryBlock(failureNum)
    if (shouldRetry) {
      logInfo(s"Add $shuffleBlock back to migration queue for " +
        s"retry ($failureNum / $maxReplicationFailures)")
      // The block needs to retry so we should not mark it as finished
      shufflesToMigrate.add((shuffleBlock, failureNum))
    }
    errorHandler.logRetryDecision(shuffleBlock, failureNum, shouldRetry)
    shouldRetry
  }

  /**
   * Get the next shuffle block to migrate, blocking until one is available
   */
  private def nextShuffleBlockToMigrate(): (ShuffleBlockInfo, Int) = {
    while (!Thread.currentThread().isInterrupted) {
      Option(shufflesToMigrate.poll()) match {
        case Some(head) => return head
        // Nothing to do right now, but maybe a transfer will fail or a new block
        // will finish being committed.
        case None => Thread.sleep(10)
      }
    }
    throw SparkCoreErrors.interruptedError()
  }

  /**
   * Migrate a single shuffle block to the peer
   */
  private def migrateShuffleBlock(shuffleBlockInfo: ShuffleBlockInfo): Unit = {
    val blocks = blockManager.migratableResolver.getMigrationBlocks(shuffleBlockInfo)
    val startTime = System.currentTimeMillis()

    if (fallbackStorage.isDefined && peer == FallbackStorage.FALLBACK_BLOCK_MANAGER_ID) {
      fallbackStorage.foreach(_.copy(shuffleBlockInfo, blockManager))
    } else {
      blocks.foreach { case (blockId, buffer) =>
        logDebug(s"Migrating sub-block ${blockId}")
        blockManager.blockTransferService.uploadBlockSync(
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

    val totalSize = blocks.map(b => b._2.size()).sum
    statisticsCollector.recordShuffleMigrated(totalSize)
    logInfo(s"Migrated $shuffleBlockInfo (" +
      s"size: ${Utils.bytesToString(totalSize)}) to $peer " +
      s"in ${System.currentTimeMillis() - startTime} ms")
    stateTracker.recordMigrationActivity()
  }

  /**
   * Handle exceptions during block migration with fallback strategies
   */
  private def handleMigrationException(
      exception: Exception,
      shuffleBlockInfo: ShuffleBlockInfo,
      retryCount: Int,
      originalBlocks: Seq[(BlockId, _)]): Boolean = {

    val (migrationError, action) = errorHandler.handleMigrationException(
      exception, shuffleBlockInfo, peer.toString, retryCount)

    migrationError match {
      case _: errorHandler.FileNotFoundError =>
        // File not found means the block was already deleted, so we can consider it
        // successfully migrated and count it as such
        statisticsCollector.recordShuffleMigrated(0) // No size since file is gone
        logInfo(s"Shuffle block $shuffleBlockInfo was deleted, marking as migrated")
        stateTracker.recordMigrationActivity()
        true // Continue running

      case _: errorHandler.RetryableError =>
        // If a block got deleted before netty opened the file handle, then trying to
        // load the blocks now will fail. This is most likely to occur if we start
        // migrating blocks and then the shuffle TTL cleaner kicks in. However this
        // could also happen with manually managed shuffles or a GC event on the
        // driver a no longer referenced RDD with shuffle files.
        val migrationBlocks = blockManager.migratableResolver.getMigrationBlocks(shuffleBlockInfo)
        if (migrationBlocks.size < originalBlocks.size) {
          logWarning(s"Skipping block $shuffleBlockInfo, block deleted.")
          statisticsCollector.recordShuffleDeleted()
          true // Continue running
        } else if (fallbackStorage.isDefined
            // Confirm peer is not the fallback BM ID because fallbackStorage would
            // have been used in the try-block above so there's no point trying again
            && peer != FallbackStorage.FALLBACK_BLOCK_MANAGER_ID) {
          fallbackStorage.foreach(_.copy(shuffleBlockInfo, blockManager))
          true // Continue running
        } else {
          action != errorHandler.AbortMigration
        }
      case errorHandler.InterruptionError | _: errorHandler.FatalError =>
        false // Stop running
    }
  }

  override def run(): Unit = {
    logInfo(s"Starting shuffle block migration thread for $peer")
    // Once a block fails to transfer to an executor stop trying to transfer more blocks
    while (keepRunning) {
      try {
        val (shuffleBlockInfo, retryCount) = nextShuffleBlockToMigrate()
        val blocks = blockManager.migratableResolver.getMigrationBlocks(shuffleBlockInfo)

        // We only migrate a shuffle block when both index file and data file exist.
        if (blocks.isEmpty) {
          logInfo(s"Ignore deleted shuffle block $shuffleBlockInfo")
          statisticsCollector.recordShuffleDeleted()
        } else {
          logInfo(s"Got migration sub-blocks $blocks. Trying to migrate $shuffleBlockInfo " +
            s"to $peer ($retryCount / $maxReplicationFailures)")
          // Migrate the components of the blocks.
          try {
            migrateShuffleBlock(shuffleBlockInfo)
          } catch {
            case e: Exception =>
              val (migrationError, action) = errorHandler.handleMigrationException(
                e, shuffleBlockInfo, peer.toString, retryCount + 1)

              action match {
                case errorHandler.SkipMigration =>
                  // Handle file not found - treat as successfully migrated
                  statisticsCollector.recordShuffleMigrated(0) // No size since file is gone
                  logInfo(s"Shuffle block $shuffleBlockInfo was deleted, marking as migrated")
                  stateTracker.recordMigrationActivity()
                  // Continue to next block
                case errorHandler.RetryMigration =>
                  allowRetry(shuffleBlockInfo, retryCount + 1)
                case errorHandler.StopAllMigration =>
                  keepRunning = false
                case errorHandler.AbortMigration =>
                  // Don't retry this block but continue with others - max retries exceeded
                  statisticsCollector.recordShuffleFailed()
                  logWarning(s"Aborting migration of $shuffleBlockInfo after exceeding max " +
                    s"retries due to: ${e.getMessage}")
              }
          }
        }

        if (!keepRunning) {
          logWarning(s"Stop migrating shuffle blocks to $peer")
        }
      } catch {
        case e: Exception =>
          val action = errorHandler.handleGeneralMigrationError(e, "shuffle blocks migration")
          action match {
            case errorHandler.StopAllMigration =>
              logInfo(s"Stop shuffle block migration${if (keepRunning) " unexpectedly"}.")
              keepRunning = false
            case _ =>
              keepRunning = false
          }
      }
    }
  }
}
