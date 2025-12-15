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
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.spark.{MapOutputTrackerMaster, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.metrics.source.Source
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
  private val fetchWaitThresholdMs = conf.get(SHUFFLE_REBALANCE_FETCH_WAIT_THRESHOLD_MS)
  private val minCompletionRatio = conf.get(SHUFFLE_REBALANCE_MIN_COMPLETION_RATIO)
  private val maxCompletionRatio = conf.get(SHUFFLE_REBALANCE_MAX_COMPLETION_RATIO)

  // Multi-location configuration
  private val enableMultiLocation = conf.get(SHUFFLE_REBALANCE_ENABLE_MULTI_LOCATION)
  private val maxLocationsPerBlock = conf.get(SHUFFLE_REBALANCE_MAX_LOCATIONS_PER_BLOCK)

  // Metrics
  private val _rebalanceBytesMoved = new AtomicLong(0)
  private val _rebalanceOpsCount = new AtomicLong(0)
  private val _rebalanceErrors = new AtomicLong(0)

  def rebalanceBytesMoved: Long = _rebalanceBytesMoved.get()
  def rebalanceOpsCount: Long = _rebalanceOpsCount.get()
  def rebalanceErrors: Long = _rebalanceErrors.get()

  val metricsSource: Source = new ShuffleRebalanceManagerSource(this)

  // Track shuffle sizes per executor
  private val executorShuffleSizes = new ConcurrentHashMap[String, Long]()

  // Track ongoing shuffle rebalancings
  private val ongoingMoves = new ConcurrentHashMap[String, ShuffleRebalanceOperation]()

  // Track which shuffle attempts have already logged fetch wait gating to avoid log spam
  private val loggedFetchWaitGating = new ConcurrentHashMap[Int, java.lang.Boolean]()

  // Thread pool for shuffle rebalancing operations
  private val shuffleRebalanceThreadPool = ThreadUtils.newDaemonFixedThreadPool(
    shuffleRebalanceMaxConcurrent, "shuffle-rebalance-executor")

  // Thread pool for monitoring rebalance batches
  private val rebalanceMonitorThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "shuffle-rebalance-monitor")

  /**
   * Check if shuffle rebalancing is needed for a given stage and initiate rebalancing if necessary.
   * This is called during stage execution when some tasks have completed.
   */
  def checkAndInitiateShuffleRebalance(stage: ShuffleMapStage, completedTasks: Int): Unit = {
    if (!shuffleRebalanceEnabled) {
      logDebug(s"Shuffle rebalance disabled for shuffle ${stage.shuffleDep.shuffleId}. " +
        s"Set spark.shuffle.rebalance.enabled=true to enable.")
      return
    }

    if (fetchWaitThresholdMs > 0 && completedTasks > 0) {
      val stageInfo = stage.latestInfo
      val taskMetrics = stageInfo.taskMetrics
      if (taskMetrics != null) {
        val totalFetchWaitMs = taskMetrics.shuffleReadMetrics.fetchWaitTime
        val avgFetchWaitMs = totalFetchWaitMs / completedTasks

        if (avgFetchWaitMs >= fetchWaitThresholdMs) {
          val shuffleId = stage.shuffleDep.shuffleId
          val attemptKey = shuffleId * 1000 + stageInfo.attemptNumber()
          if (loggedFetchWaitGating.putIfAbsent(attemptKey, true) == null) {
            val message =
              s"Skipping shuffle rebalance for shuffle $shuffleId " +
                s"(stage ${stage.id}, attempt ${stageInfo.attemptNumber()}) due to high fetch " +
                s"wait: avgFetchWaitMs=$avgFetchWaitMs >= threshold=$fetchWaitThresholdMs"
            logInfo(message)
          }
          return
        }
      }
    }

    val completionRatio = completedTasks.toDouble / stage.numTasks
    if (completionRatio < minCompletionRatio || completionRatio >= maxCompletionRatio) {
      logDebug(s"Shuffle rebalance skipped for shuffle ${stage.shuffleDep.shuffleId}: " +
        s"completion ratio ${f"$completionRatio%.2f"} " +
        s"($completedTasks/${stage.numTasks} tasks) outside valid range " +
        s"[$minCompletionRatio, $maxCompletionRatio).")
      return
    }

    logDebug(s"Checking shuffle rebalance for shuffle ${stage.shuffleDep.shuffleId}: " +
      s"$completedTasks/${stage.numTasks} tasks completed " +
      s"(${f"${completionRatio * 100}%.1f"}%)")

    val shuffleId = stage.shuffleDep.shuffleId
    val numPartitions = stage.shuffleDep.partitioner.numPartitions
    val executorSizes = getExecutorShuffleSizes(shuffleId, numPartitions)

    logDebug(s"Executor shuffle distribution for shuffle $shuffleId: " +
      executorSizes.toSeq.sortBy(_._2).reverse
        .map { case (exec, size) => s"$exec=${Utils.bytesToString(size)}" }
        .mkString(", "))

    if (isShuffleRebalanceNeeded(executorSizes, shuffleId)) {
      val rebalanceOperations = planShuffleRebalancing(shuffleId, executorSizes, numPartitions)

      if (rebalanceOperations.isEmpty) {
        logInfo(s"No rebalance operations planned for shuffle $shuffleId. " +
          s"Possible reasons: all moves < ${shuffleRebalanceMinSizeMB}MB threshold, " +
          s"no suitable blocks to move, or all executors within acceptable range.")
      } else {
        val currentStats = computeDistributionStats(executorSizes)

        // Calculate projected sizes after rebalancing
        val projectedSizes = mutable.Map(executorSizes.toSeq: _*)
        rebalanceOperations.foreach { op =>
          projectedSizes(op.sourceExecutor) -= op.totalSize
          projectedSizes(op.targetExecutor) += op.totalSize
        }
        val projectedStats = computeDistributionStats(projectedSizes.toMap)

        // Calculate skew reduction percentage
        val skewReduction = 100.0 * (currentStats.imbalanceRatio - projectedStats.imbalanceRatio) /
          currentStats.imbalanceRatio

        logInfo(s"Initiating shuffle rebalance for shuffle $shuffleId. " +
          s"Baseline skew: ${f"${currentStats.imbalanceRatio}%.2f"}, " +
          s"Projected skew after rebalance: ${f"${projectedStats.imbalanceRatio}%.2f"} " +
          s"(${f"$skewReduction%.1f"}% reduction). " +
          s"Moving ${Utils.bytesToString(rebalanceOperations.map(_.totalSize).sum)} " +
          s"in ${rebalanceOperations.size} operations.")

        // Execute rebalancing operations and track their futures
        val futures = rebalanceOperations.map(executeShuffleRebalance).flatMap(_.toSeq)

        // Monitor completion asynchronously
        if (futures.nonEmpty) {
          // Capture baseline for comparison in final summary
          val baselineSkew = currentStats.imbalanceRatio

          rebalanceMonitorThreadPool.submit(new Runnable {
            override def run(): Unit = {
              try {
                // Wait for all operations to complete
                futures.foreach(_.get())

                // Check final status
                val finalStats = getShuffleDistributionStats(shuffleId, numPartitions)
                val multiLocStats = getMultiLocationStats(shuffleId)

                // Calculate overall improvement
                val totalSkewReduction = 100.0 * (baselineSkew - finalStats.imbalanceRatio) /
                  baselineSkew

                val multiLocInfo = if (enableMultiLocation && multiLocStats.totalBlocks > 0) {
                  val pctMultiLoc = 100.0 * multiLocStats.blocksWithMultiLocation /
                    multiLocStats.totalBlocks
                  s" Multi-location enabled: ${multiLocStats.blocksWithMultiLocation}/" +
                    s"${multiLocStats.totalBlocks} blocks (${f"$pctMultiLoc%.1f"}%) " +
                    s"with avg ${f"${multiLocStats.averageLocationsPerBlock}%.2f"} " +
                    s"locations/block for improved fetch load balancing."
                } else {
                  ""
                }

                logInfo(s"Finished shuffle rebalance for shuffle $shuffleId. " +
                  s"Final skew: ${f"${finalStats.imbalanceRatio}%.2f"} " +
                  s"(${f"$totalSkewReduction%.1f"}% improvement from baseline " +
                  s"${f"$baselineSkew%.2f"}).$multiLocInfo")
              } catch {
                case e: Exception =>
                  logWarning(s"Error monitoring rebalance for shuffle $shuffleId", e)
              }
            }
          })
        }
      }
    }
  }

  /**
   * Get shuffle sizes per executor for a given shuffle.
   */
  private def getExecutorShuffleSizes(shuffleId: Int, numPartitions: Int): Map[String, Long] = {
    // Calculate total shuffle size per executor
    val executorSizes = mutable.Map[String, Long]()

    val shuffleStatus = mapOutputTracker.shuffleStatuses(shuffleId)
    shuffleStatus.withMapStatuses { statuses =>
      statuses.filter(_ != null).foreach { status =>
        val executorId = status.location.executorId
        // Sum up all partition sizes for this map output
        var totalSize = 0L
        (0 until numPartitions).foreach { partitionId =>
          val size = status.getSizeForBlock(partitionId)
          if (size > 0) totalSize += size
        }
        executorSizes(executorId) = executorSizes.getOrElse(executorId, 0L) + totalSize
      }
    }

    executorSizes.toMap
  }

  /**
   * Determine if shuffle rebalancing is needed based on size distribution.
   */
  private def isShuffleRebalanceNeeded(
      executorSizes: Map[String, Long],
      shuffleId: Int): Boolean = {
    if (executorSizes.size < 2) {
      logDebug(s"Shuffle rebalance not needed for shuffle $shuffleId: " +
        s"only ${executorSizes.size} executor(s)")
      return false
    }

    val sizes = executorSizes.values.toSeq
    val avgSize = sizes.sum.toDouble / sizes.length
    val maxSize = sizes.max
    val minSize = sizes.min

    // Check if imbalance exceeds threshold
    val imbalanceRatio = maxSize.toDouble / avgSize
    val sizeDifferenceMB = (maxSize - minSize) / (1024 * 1024)

    val isNeeded =
      imbalanceRatio > shuffleRebalanceThreshold && sizeDifferenceMB > shuffleRebalanceMinSizeMB

    if (!isNeeded) {
      val failedChecks = mutable.ArrayBuffer[String]()
      if (imbalanceRatio <= shuffleRebalanceThreshold) {
        failedChecks += s"imbalance ratio ${f"$imbalanceRatio%.2f"} <= " +
          s"threshold $shuffleRebalanceThreshold"
      }
      if (sizeDifferenceMB <= shuffleRebalanceMinSizeMB) {
        failedChecks += s"size difference ${sizeDifferenceMB}MB <= " +
          s"min ${shuffleRebalanceMinSizeMB}MB"
      }
      logInfo(s"Shuffle rebalance not needed for shuffle $shuffleId: " +
        s"${failedChecks.mkString(", ")}. " +
        s"Distribution: ${executorSizes.size} executors, " +
        s"avg=${Utils.bytesToString(avgSize.toLong)}, " +
        s"max=${Utils.bytesToString(maxSize)}, min=${Utils.bytesToString(minSize)}")
    } else {
      logInfo(s"Shuffle rebalance needed for shuffle $shuffleId: " +
        s"imbalance ratio ${f"$imbalanceRatio%.2f"} > threshold $shuffleRebalanceThreshold, " +
        s"size difference ${sizeDifferenceMB}MB > min ${shuffleRebalanceMinSizeMB}MB. " +
        s"Distribution: ${executorSizes.size} executors, " +
        s"avg=${Utils.bytesToString(avgSize.toLong)}, " +
        s"max=${Utils.bytesToString(maxSize)}, min=${Utils.bytesToString(minSize)}")
    }

    isNeeded
  }

  /**
   * Plan shuffle rebalancing operations to balance data distribution.
   *
   * Uses a one-to-many greedy algorithm: each source (largest first) distributes
   * to multiple targets (smallest first) until balanced. This minimizes the maximum
   * executor size and achieves optimal balance.
   */
  private def planShuffleRebalancing(
      shuffleId: Int,
      executorSizes: Map[String, Long],
      numPartitions: Int): Seq[ShuffleRebalanceOperation] = {

    val sorted = executorSizes.toSeq.sortBy(_._2)
    val avgSize = executorSizes.values.sum.toDouble / executorSizes.size
    val operations = mutable.ArrayBuffer[ShuffleRebalanceOperation]()

    val numExecutors = sorted.length
    if (numExecutors < 2) return operations.toSeq

    // Sources: All executors above average (largest first)
    // Targets: All executors below average (smallest first)
    val sources = sorted.filter(_._2 > avgSize).reverse
    val targets = sorted.filter(_._2 < avgSize)

    if (sources.isEmpty || targets.isEmpty) {
      return operations.toSeq
    }

    // Track remaining excess/deficit for each executor
    val sourceRemaining = mutable.Map(sources.map { case (exec, size) =>
      exec -> (size - avgSize.toLong)
    }: _*)
    val targetRemaining = mutable.Map(targets.map { case (exec, size) =>
      exec -> (avgSize.toLong - size)
    }: _*)

    var targetIdx = 0

    // Process each source, distributing to multiple targets (one-to-many)
    for ((sourceExec, _) <- sources if sourceRemaining(sourceExec) > 0) {

      // Keep moving from this source to multiple targets until balanced
      while (targetIdx < targets.length && sourceRemaining(sourceExec) > 0) {
        val (targetExec, _) = targets(targetIdx)
        val targetCapacity = targetRemaining(targetExec)

        if (targetCapacity <= 0) {
          targetIdx += 1
        } else {
          // Move as much as possible: limited by source excess or target capacity
          val moveSize = math.min(sourceRemaining(sourceExec), targetCapacity)

          if (moveSize > shuffleRebalanceMinSizeMB * 1024 * 1024) {
            val blocksToMove = selectBlocksToMove(shuffleId, sourceExec, moveSize, numPartitions)

            if (blocksToMove.nonEmpty) {
              val actualMoveSize = blocksToMove.map(_._2).sum

              operations += ShuffleRebalanceOperation(
                shuffleId = shuffleId,
                sourceExecutor = sourceExec,
                targetExecutor = targetExec,
                blocks = blocksToMove,
                totalSize = actualMoveSize
              )

              // Update remaining capacity
              sourceRemaining(sourceExec) -= actualMoveSize
              targetRemaining(targetExec) -= actualMoveSize
            }
          }

          // Move to next target if this one is full or move was too small
          if (targetRemaining(targetExec) <= shuffleRebalanceMinSizeMB * 1024 * 1024) {
            targetIdx += 1
          }
        }
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
      targetSize: Long,
      numPartitions: Int): Seq[(BlockId, Long)] = {

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
          (0 until numPartitions).foreach { partitionId =>
            val blockSize = status.getSizeForBlock(partitionId)
            if (blockSize > 0) {
              val blockId = ShuffleBlockId(shuffleId, status.mapId, partitionId)
              blocks += ((blockId, blockSize))
            }
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
  private def executeShuffleRebalance(
      operation: ShuffleRebalanceOperation): Option[java.util.concurrent.Future[_]] = {
    val moveKey = s"${operation.shuffleId}-${operation.sourceExecutor}-${operation.targetExecutor}"

    if (ongoingMoves.containsKey(moveKey)) {
      logWarning(s"Shuffle move already in progress: $moveKey")
      return None
    }

    ongoingMoves.put(moveKey, operation)

    val future = shuffleRebalanceThreadPool.submit(new Runnable {
      override def run(): Unit = {
        try {
          logInfo(s"Starting shuffle rebalancing: ${operation.blocks.length} blocks " +
            s"(${Utils.bytesToString(operation.totalSize)}) from " +
            s"${operation.sourceExecutor} to ${operation.targetExecutor}")

          // Send message to source executor to transfer blocks
          sendShuffleBlockTransferMessage(operation)

          _rebalanceOpsCount.incrementAndGet()
          _rebalanceBytesMoved.addAndGet(operation.totalSize)

          logInfo(s"Completed shuffle rebalancing: $moveKey")

        } catch {
          case e: Exception =>
            _rebalanceErrors.incrementAndGet()
            logError(s"Failed shuffle rebalancing: $moveKey", e)
        } finally {
          ongoingMoves.remove(moveKey)
        }
      }
    })
    Some(future)
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
  def getShuffleDistributionStats(shuffleId: Int, numPartitions: Int): ShuffleDistributionStats = {
    val executorSizes = getExecutorShuffleSizes(shuffleId, numPartitions)
    computeDistributionStats(executorSizes)
  }

  private def computeDistributionStats(
      executorSizes: Map[String, Long]): ShuffleDistributionStats = {
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

  /**
   * Get multi-location statistics for a shuffle.
   */
  private def getMultiLocationStats(shuffleId: Int): MultiLocationStats = {
    val shuffleStatus = mapOutputTracker.shuffleStatuses(shuffleId)
    var totalBlocks = 0
    var blocksWithMultiLocation = 0
    var totalLocations = 0
    var maxLocations = 0

    shuffleStatus.withMapStatuses { statuses =>
      statuses.filter(_ != null).foreach { status =>
        totalBlocks += 1
        val locationCount = status.locations.size
        totalLocations += locationCount
        if (locationCount > 1) {
          blocksWithMultiLocation += 1
        }
        if (locationCount > maxLocations) {
          maxLocations = locationCount
        }
      }
    }

    MultiLocationStats(
      totalBlocks = totalBlocks,
      blocksWithMultiLocation = blocksWithMultiLocation,
      averageLocationsPerBlock = if (totalBlocks > 0) {
        totalLocations.toDouble / totalBlocks
      } else 0.0,
      maxLocationsPerBlock = maxLocations
    )
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

/**
 * Statistics about multi-location distribution for shuffle blocks.
 */
private[spark] case class MultiLocationStats(
    totalBlocks: Int,
    blocksWithMultiLocation: Int,
    averageLocationsPerBlock: Double,
    maxLocationsPerBlock: Int)
