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

package org.apache.spark.scheduler

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.mutable

import org.apache.spark.{MapOutputTrackerMaster, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.storage.{BlockId, BlockManagerId, BlockManagerMaster, ShuffleBlockId}
import org.apache.spark.storage.ShuffleRebalanceMessages._
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Manager for coordinating shuffle data rebalancing between executors to balance
 * shuffle data distribution. This helps address skewed shuffle scenarios where
 * some executors hold significantly more shuffle data than others.
 */
private[spark] class ShuffleRebalanceManager(
    conf: SparkConf,
    mapOutputTracker: MapOutputTrackerMaster,
    blockManagerMaster: BlockManagerMaster) extends Logging {

  // Configuration for shuffle rebalancing feature
  private val shuffleRebalanceEnabled = conf.get(SHUFFLE_REBALANCE_ENABLED)
  private val shuffleRebalanceThreshold = conf.get(SHUFFLE_REBALANCE_THRESHOLD)
  private val shuffleRebalanceMinSizeMB = conf.get(SHUFFLE_REBALANCE_MIN_SIZE_MB)
  private val shuffleRebalanceCheckIntervalMs = conf.get(SHUFFLE_REBALANCE_CHECK_INTERVAL_MS)
  private val shuffleRebalanceMaxConcurrent = conf.get(SHUFFLE_REBALANCE_MAX_CONCURRENT)

  // Multi-location configuration
  private val enableMultiLocation = conf.get(SHUFFLE_REBALANCE_ENABLE_MULTI_LOCATION)
  private val maxLocationsPerBlock = conf.get(SHUFFLE_REBALANCE_MAX_LOCATIONS_PER_BLOCK)

  // Track shuffle sizes per executor
  private val executorShuffleSizes = new ConcurrentHashMap[String, Long]()

  // Track ongoing shuffle rebalancings
  private val ongoingMoves = new ConcurrentHashMap[String, ShuffleRebalanceOperation]()

  // Thread pool for shuffle rebalancing operations
  private val shuffleRebalanceThreadPool = ThreadUtils.newDaemonFixedThreadPool(
    shuffleRebalanceMaxConcurrent, "shuffle-rebalance-executor")

  /**
   * Check if shuffle rebalancing is needed for a given stage and initiate rebalancing if necessary.
   * This is called during stage execution when some tasks have completed.
   */
  def checkAndInitiateShuffleRebalance(stage: ShuffleMapStage, completedTasks: Int): Unit = {
    if (!shuffleRebalanceEnabled) return

    val completionRatio = completedTasks.toDouble / stage.numTasks
    if (completionRatio < 0.25 || completionRatio >= 1.0) return

    val shuffleId = stage.shuffleDep.shuffleId
    val executorSizes = getExecutorShuffleSizes(shuffleId)

    if (isShuffleRebalanceNeeded(executorSizes)) {
      val rebalanceOperations = planShuffleRebalancing(shuffleId, executorSizes)
      rebalanceOperations.foreach(executeShuffleRebalance)
    }
  }

  /**
   * Get shuffle sizes per executor for a given shuffle.
   */
  private def getExecutorShuffleSizes(shuffleId: Int): Map[String, Long] = {
    // Calculate total shuffle size per executor
    val executorSizes = mutable.Map[String, Long]()

    val shuffleStatus = mapOutputTracker.shuffleStatuses(shuffleId)
    shuffleStatus.withMapStatuses { statuses =>
      statuses.filter(_ != null).foreach { status =>
        val executorId = status.location.executorId
        // Sum up all partition sizes for this map output
        // Use iteration approach to determine number of partitions
        var totalSize = 0L
        var partitionId = 0
        try {
          // Keep summing until we get an exception (reached the end)
          while (true) {
            val size = status.getSizeForBlock(partitionId)
            if (size > 0) totalSize += size
            partitionId += 1
          }
        } catch {
          case _: Exception => // Expected when we've gone past the last partition
        }
        executorSizes(executorId) = executorSizes.getOrElse(executorId, 0L) + totalSize
      }
    }

    executorSizes.toMap
  }

  /**
   * Determine if shuffle rebalancing is needed based on size distribution.
   */
  private def isShuffleRebalanceNeeded(executorSizes: Map[String, Long]): Boolean = {
    if (executorSizes.size < 2) return false

    val sizes = executorSizes.values.toSeq
    val avgSize = sizes.sum.toDouble / sizes.length
    val maxSize = sizes.max
    val minSize = sizes.min

    // Check if imbalance exceeds threshold
    val imbalanceRatio = maxSize.toDouble / avgSize
    val sizeDifferenceMB = (maxSize - minSize) / (1024 * 1024)

    imbalanceRatio > shuffleRebalanceThreshold && sizeDifferenceMB > shuffleRebalanceMinSizeMB
  }

  /**
   * Plan shuffle rebalancing operations to balance data distribution.
   */
  private def planShuffleRebalancing(
      shuffleId: Int,
      executorSizes: Map[String, Long]): Seq[ShuffleRebalanceOperation] = {

    val sorted = executorSizes.toSeq.sortBy(_._2)
    val avgSize = executorSizes.values.sum.toDouble / executorSizes.size

    val operations = mutable.ArrayBuffer[ShuffleRebalanceOperation]()

    // Identify source (over-loaded) and target (under-loaded) executors
    val sources = sorted.filter(_._2 > avgSize * shuffleRebalanceThreshold).reverse
    val targets = sorted.filter(_._2 < avgSize / shuffleRebalanceThreshold)

    var sourceIdx = 0
    var targetIdx = 0

    while (sourceIdx < sources.length && targetIdx < targets.length) {
      val (sourceExec, sourceSize) = sources(sourceIdx)
      val (targetExec, targetSize) = targets(targetIdx)

      val moveSize = math.min(
        sourceSize - avgSize.toLong,
        avgSize.toLong - targetSize
      )

      if (moveSize > shuffleRebalanceMinSizeMB * 1024 * 1024) {
        // Find specific shuffle blocks to move
        val blocksToMove = selectBlocksToMove(shuffleId, sourceExec, moveSize)

        if (blocksToMove.nonEmpty) {
          operations += ShuffleRebalanceOperation(
            shuffleId = shuffleId,
            sourceExecutor = sourceExec,
            targetExecutor = targetExec,
            blocks = blocksToMove,
            totalSize = blocksToMove.map(_._2).sum
          )
        }
      }

      // Move to next target if current one is filled enough
      if (targetSize + moveSize >= avgSize * 0.9) {
        targetIdx += 1
      } else {
        sourceIdx += 1
      }
    }

    operations.toSeq
  }

  /**
   * Select specific shuffle blocks to move from source executor.
   */
  private def selectBlocksToMove(
      shuffleId: Int,
      sourceExecutor: String,
      targetSize: Long): Seq[(BlockId, Long)] = {

    val blocks = mutable.ArrayBuffer[(BlockId, Long)]()
    var currentSize = 0L

    mapOutputTracker.shuffleStatuses(shuffleId).withMapStatuses { statuses =>
      val shuffledBlocks = statuses.zipWithIndex
        .filter { case (status, _) =>
          status != null && status.location.executorId == sourceExecutor
        }
        .flatMap { case (status, mapIndex) =>
          // Build list of blocks for this map output
          val blocks = mutable.ArrayBuffer[(BlockId, Long)]()
          var partitionId = 0
          try {
            // Keep adding blocks until we get an exception (reached the end)
            while (true) {
              val blockSize = status.getSizeForBlock(partitionId)
              if (blockSize > 0) {
                val blockId = ShuffleBlockId(shuffleId, status.mapId, partitionId)
                blocks += ((blockId, blockSize))
              }
              partitionId += 1
            }
          } catch {
            case _: Exception => // Expected when we've gone past the last partition
          }
          blocks.toSeq
        }
        .filter(_._2 > 0)
        .sortBy(_._2) // Start with smaller blocks for more granular balancing

      for ((blockId, blockSize) <- shuffledBlocks if currentSize < targetSize) {
        blocks += ((blockId, blockSize))
        currentSize += blockSize
      }
    }

    blocks.toSeq
  }

  /**
   * Execute a shuffle rebalancing operation.
   */
  private def executeShuffleRebalance(operation: ShuffleRebalanceOperation): Unit = {
    val moveKey = s"${operation.shuffleId}-${operation.sourceExecutor}-${operation.targetExecutor}"

    if (ongoingMoves.containsKey(moveKey)) {
      logWarning(s"Shuffle move already in progress: $moveKey")
      return
    }

    ongoingMoves.put(moveKey, operation)

    shuffleRebalanceThreadPool.submit(new Runnable {
      override def run(): Unit = {
        try {
          logInfo(s"Starting shuffle rebalancing: ${operation.blocks.length} blocks " +
            s"(${Utils.bytesToString(operation.totalSize)}) from " +
            s"${operation.sourceExecutor} to ${operation.targetExecutor}")

          // Send message to source executor to transfer blocks
          sendShuffleBlockTransferMessage(operation)

          logInfo(s"Completed shuffle rebalancing: $moveKey")

        } catch {
          case e: Exception =>
            logError(s"Failed shuffle rebalancing: $moveKey", e)
        } finally {
          ongoingMoves.remove(moveKey)
        }
      }
    })
  }

  /**
   * Send message to source executor to transfer shuffle blocks.
   */
  private def sendShuffleBlockTransferMessage(operation: ShuffleRebalanceOperation): Unit = {
    val targetBlockManagerId = getBlockManagerId(operation.targetExecutor)

    val operationId = java.util.UUID.randomUUID().toString
    val message = SendShuffleBlocks(
      targetExecutor = targetBlockManagerId,
      blocks = operation.blocks,
      operationId = operationId,
      priority = 1
    )

    // Get the source executor's endpoint and send the message
    blockManagerMaster.getExecutorEndpointRef(operation.sourceExecutor) match {
      case Some(endpointRef) =>
        logInfo(s"Sending shuffle rebalance message to executor ${operation.sourceExecutor} " +
          s"to transfer ${operation.blocks.length} blocks to ${operation.targetExecutor}")
        endpointRef.send(message)

      case None =>
        logError(s"Could not find executor endpoint for ${operation.sourceExecutor}")
        throw new RuntimeException(s"Executor ${operation.sourceExecutor} not found")
    }
  }

  /**
   * Get BlockManagerId for a given executor.
   */
  private def getBlockManagerId(executorId: String): BlockManagerId = {
    // This would typically come from the BlockManagerMaster
    // For now, we'll create a placeholder
    BlockManagerId(executorId, "localhost", 7337)
  }

  /**
   * Get statistics about current shuffle distribution.
   */
  def getShuffleDistributionStats(shuffleId: Int): ShuffleDistributionStats = {
    val executorSizes = getExecutorShuffleSizes(shuffleId)
    val sizes = executorSizes.values.toSeq

    if (sizes.nonEmpty) {
      val total = sizes.sum
      val avg = total.toDouble / sizes.length
      val max = sizes.max
      val min = sizes.min
      val stdDev = math.sqrt(sizes.map(s => math.pow(s - avg, 2)).sum / sizes.length)

      ShuffleDistributionStats(
        totalSize = total,
        averageSize = avg.toLong,
        maxSize = max,
        minSize = min,
        standardDeviation = stdDev,
        imbalanceRatio = max.toDouble / avg,
        executorCount = sizes.length
      )
    } else {
      ShuffleDistributionStats(0, 0, 0, 0, 0, 0, 0)
    }
  }




  def stop(): Unit = {
    shuffleRebalanceThreadPool.shutdown()
    try {
      if (!shuffleRebalanceThreadPool.awaitTermination(10, TimeUnit.SECONDS)) {
        shuffleRebalanceThreadPool.shutdownNow()
      }
    } catch {
      case _: InterruptedException =>
        shuffleRebalanceThreadPool.shutdownNow()
    }
  }
}

/**
 * Represents a shuffle rebalancing operation.
 */
private[spark] case class ShuffleRebalanceOperation(
    shuffleId: Int,
    sourceExecutor: String,
    targetExecutor: String,
    blocks: Seq[(BlockId, Long)],
    totalSize: Long)

/**
 * Statistics about shuffle data distribution across executors.
 */
private[spark] case class ShuffleDistributionStats(
    totalSize: Long,
    averageSize: Long,
    maxSize: Long,
    minSize: Long,
    standardDeviation: Double,
    imbalanceRatio: Double,
    executorCount: Int)
