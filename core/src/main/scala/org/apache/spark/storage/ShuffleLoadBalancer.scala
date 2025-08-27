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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockManagerMessages._

/**
 * Enum for load shedding actions to take when executors are overloaded.
 */
private[spark] object LoadSheddingAction extends Enumeration {
  val THROTTLE_CONNECTIONS, REDUCE_REQUEST_RATE, REJECT_NEW_REQUESTS = Value
}

/**
 * Tracks the current load state of an executor for shuffle operations.
 * Includes both client-side and server-side metrics for comprehensive load assessment.
 */
private[spark] case class ExecutorLoadState(
    executorId: String,
    // Client-side metrics
    bytesInFlight: Long,
    activeConnections: Int,
    networkCapacity: Long,
    avgResponseTime: Long,
    avgWaitingTime: Long,
    avgNetworkTime: Long,
    queueDepth: Int,
    // Server-side metrics
    serverRequestsReceived: Long,
    serverRequestsCompleted: Long,
    serverRequestsFailed: Long,
    serverBytesServed: Long,
    serverAvgProcessingTime: Double,
    serverAvgDiskReadTime: Double,
    serverQueueDepth: Int,
    lastUpdateTime: Long) {

  /**
   * Calculate a simple load score for this executor.
   * Score ranges from 0.0 (idle) to 1.0+ (overloaded).
   * Goal: Keep all executors busy but not overloaded.
   */
  def loadScore: Double = {
    // Simple capacity utilization (primary factor)
    val capacityUsed = if (networkCapacity > 0) {
      bytesInFlight.toDouble / networkCapacity
    } else {
      0.0
    }

    // Simple server health check (secondary factor)
    val serverHealth = if (serverRequestsReceived > 0) {
      val failureRate = serverRequestsFailed.toDouble / serverRequestsReceived
      val isSlowProcessing = serverAvgProcessingTime > 200.0 // 200ms threshold
      val isSlowDisk = serverAvgDiskReadTime > 100.0 // 100ms threshold

      if (failureRate >= 0.1 || isSlowProcessing || isSlowDisk) 0.5 else 0.0
    } else {
      0.0
    }

    // Simple formula: capacity + server penalty
    capacityUsed + serverHealth
  }

  /**
   * Check if this executor is overloaded based on configurable thresholds.
   */
  def isOverloaded(conf: SparkConf): Boolean = {
    val loadThreshold = conf.getDouble("spark.shuffle.loadbalancer.overloadThreshold", 0.8)
    loadScore > loadThreshold
  }

  /**
   * Check if the server-side is experiencing high failure rates.
   */
  def hasServerFailures(conf: SparkConf): Boolean = {
    val failureThreshold = conf.getDouble("spark.shuffle.loadbalancer.serverFailureThreshold", 0.1)
    if (serverRequestsReceived > 0) {
      (serverRequestsFailed.toDouble / serverRequestsReceived) > failureThreshold
    } else {
      false
    }
  }

  /**
   * Check if the server-side is experiencing disk bottlenecks.
   */
  def hasDiskBottleneck(conf: SparkConf): Boolean = {
    val diskThreshold = conf.getDouble(
      "spark.shuffle.loadbalancer.diskReadThreshold", 100.0) // 100ms
    serverAvgDiskReadTime > diskThreshold
  }

  /**
   * Check if the server-side is experiencing processing bottlenecks.
   */
  def hasProcessingBottleneck(conf: SparkConf): Boolean = {
    val processingThreshold = conf.getDouble(
      "spark.shuffle.loadbalancer.processingThreshold", 200.0) // 200ms
    serverAvgProcessingTime > processingThreshold
  }

  /**
   * Simple classification: Is this executor ready for more work?
   */
  def canAcceptMoreWork(): Boolean = {
    loadScore < 0.8
  }

  /**
   * Is this executor completely idle?
   */
  def isIdle: Boolean = {
    loadScore < 0.1
  }

  /**
   * Calculate the recommended maximum request size for this executor.
   */
  def recommendedRequestSize(baseRequestSize: Long, conf: SparkConf): Long = {
    val minRequestSize = conf.getLong(
      "spark.shuffle.loadbalancer.minRequestSize", 1024 * 1024) // 1MB
    val reductionFactor = Math.max(0.1, 1.0 - loadScore)
    Math.max(minRequestSize, (baseRequestSize * reductionFactor).toLong)
  }
}

/**
 * Manages shuffle load balancing across executors in the cluster.
 * Tracks executor load metrics and provides intelligent fetch scheduling recommendations.
 */
private[spark] class ShuffleLoadBalancer(conf: SparkConf) extends Logging {

  // Thread-safe map to store executor load states
  private val executorLoadStates = new ConcurrentHashMap[String, ExecutorLoadState]()

  // Historical data for predictive load balancing
  private val executorHistory = new ConcurrentHashMap[String, ExecutorHistoryTracker]()

  // Configuration parameters
  private val loadUpdateTimeoutMs = conf.getLong(
    "spark.shuffle.loadbalancer.updateTimeoutMs", 30000)
  private val maxPreferredSources = conf.getInt("spark.shuffle.loadbalancer.maxPreferredSources", 3)
  private val historyWindowSize = conf.getInt("spark.shuffle.loadbalancer.historyWindowSize", 20)
  private val predictionEnabled = conf.getBoolean(
    "spark.shuffle.loadbalancer.predictionEnabled", true)

  /**
   * Update the load state for an executor based on received metrics.
   */
  def updateExecutorLoad(metrics: ShuffleLoadMetrics): Unit = {
    val loadState = ExecutorLoadState(
      executorId = metrics.executorId,
      // Client-side metrics
      bytesInFlight = metrics.bytesInFlight,
      activeConnections = metrics.activeConnections,
      networkCapacity = metrics.networkCapacity,
      avgResponseTime = metrics.avgResponseTime,
      avgWaitingTime = metrics.avgWaitingTime,
      avgNetworkTime = metrics.avgNetworkTime,
      queueDepth = metrics.queueDepth,
      // Server-side metrics
      serverRequestsReceived = metrics.serverRequestsReceived,
      serverRequestsCompleted = metrics.serverRequestsCompleted,
      serverRequestsFailed = metrics.serverRequestsFailed,
      serverBytesServed = metrics.serverBytesServed,
      serverAvgProcessingTime = metrics.serverAvgProcessingTime,
      serverAvgDiskReadTime = metrics.serverAvgDiskReadTime,
      serverQueueDepth = metrics.serverQueueDepth,
      lastUpdateTime = metrics.timestamp
    )

    executorLoadStates.put(metrics.executorId, loadState)

    // Update historical data for predictive analysis
    if (predictionEnabled) {
      val history = executorHistory.computeIfAbsent(metrics.executorId,
        _ => new ExecutorHistoryTracker(historyWindowSize))
      history.addDataPoint(loadState)
    }

    logDebug(s"Updated load state for executor ${metrics.executorId}: " +
      s"load score = ${loadState.loadScore}")
  }

  /**
   * Get the current load state for an executor.
   */
  def getExecutorLoadState(executorId: String): Option[ExecutorLoadState] = {
    Option(executorLoadStates.get(executorId)).filter { state =>
      System.currentTimeMillis() - state.lastUpdateTime < loadUpdateTimeoutMs
    }
  }

  /**
   * Get all executor load states that are not stale.
   */
  def getAllExecutorLoadStates: Map[String, ExecutorLoadState] = {
    val currentTime = System.currentTimeMillis()
    executorLoadStates.asScala.toMap.filter { case (_, state) =>
      currentTime - state.lastUpdateTime < loadUpdateTimeoutMs
    }
  }

  /**
   * Simple load balancing strategy: Keep all executors busy but not overloaded.
   */
  def calculateOptimalFetchStrategy(targetExecutor: String): Option[ShuffleFetchDirective] = {
    getExecutorLoadState(targetExecutor).map { targetState =>
      val preferredSources = getBestAvailableSources(targetExecutor)
      val baseRequestSize = conf.getLong("spark.reducer.maxSizeInFlight", 48) * 1024 * 1024 / 5

      // Simple throttling: slow down if overloaded
      val throttleDelay = if (targetState.canAcceptMoreWork()) 0 else 50

      // Simple priority: prefer idle executors, avoid overloaded ones
      val priority = if (!targetState.canAcceptMoreWork()) {
        3 // Lower priority for overloaded
      } else if (targetState.isIdle) {
        1 // Highest priority for idle
      } else {
        2 // Normal priority for busy but not overloaded
      }

      ShuffleFetchDirective(
        targetExecutor = targetExecutor,
        preferredSources = preferredSources,
        maxRequestSize = baseRequestSize,
        throttleDelay = throttleDelay,
        priority = priority
      )
    }
  }

  /**
   * Get best available sources: prefer idle executors, then busy but not overloaded.
   */
  def getBestAvailableSources(targetExecutor: String): Seq[String] = {
    val availableExecutors = getAllExecutorLoadStates
      .filter { case (executorId, _) => executorId != targetExecutor }
      .filter { case (_, loadState) => loadState.canAcceptMoreWork() }
      .toSeq

    // Sort by load: idle first, then by increasing load
    val sortedByLoad = availableExecutors.sortBy { case (_, loadState) => loadState.loadScore }

    sortedByLoad.take(maxPreferredSources).map(_._1)
  }

  /**
   * Get preferred source executors for a target executor, ordered by their load scores.
   */
  def getPreferredSourceExecutors(targetExecutor: String): Seq[String] = {
    getAllExecutorLoadStates
      .filterKeys(_ != targetExecutor) // Exclude the target executor itself
      .toSeq
      .sortBy(_._2.loadScore) // Sort by load score (ascending, so least loaded first)
      .take(maxPreferredSources)
      .map(_._1)
  }

  /**
   * Determine if an executor should defer new fetch requests.
   */
  def shouldDeferRequests(executorId: String): Boolean = {
    getExecutorLoadState(executorId).exists(_.isOverloaded(conf))
  }

  /**
   * Generate dynamic configuration updates based on current cluster load.
   */
  def generateConfigUpdates(): Seq[(String, ShuffleConfigUpdate)] = {
    val allStates = getAllExecutorLoadStates
    if (allStates.isEmpty) return Seq.empty

    val avgLoadScore = allStates.values.map(_.loadScore).sum / allStates.size
    val baseMaxBytesInFlight = conf.getLong("spark.reducer.maxSizeInFlight", 48) * 1024 * 1024

    allStates.map { case (executorId, state) =>
      val adjustedMaxBytesInFlight = if (state.loadScore > avgLoadScore * 1.5) {
        // Reduce for overloaded executors
        (baseMaxBytesInFlight * 0.7).toLong
      } else if (state.loadScore < avgLoadScore * 0.5) {
        // Increase for underloaded executors
        (baseMaxBytesInFlight * 1.3).toLong
      } else {
        baseMaxBytesInFlight
      }

      val adjustedMaxBlocksInFlight = if (state.isOverloaded(conf)) 3 else 5
      val adjustedTargetRequestSize = state.recommendedRequestSize(baseMaxBytesInFlight / 5, conf)

      executorId -> ShuffleConfigUpdate(
        maxBytesInFlight = adjustedMaxBytesInFlight,
        maxBlocksInFlightPerAddress = adjustedMaxBlocksInFlight,
        targetRequestSize = adjustedTargetRequestSize
      )
    }.toSeq
  }

  /**
   * Remove an executor from load tracking (called when executor is removed).
   */
  def removeExecutor(executorId: String): Unit = {
    executorLoadStates.remove(executorId)
    logDebug(s"Removed executor $executorId from load balancer")
  }

  /**
   * Get cluster-wide load statistics for monitoring.
   */
  def getClusterLoadStats: Map[String, Double] = {
    val allStates = getAllExecutorLoadStates.values
    if (allStates.isEmpty) {
      Map("avgLoadScore" -> 0.0, "maxLoadScore" -> 0.0, "activeExecutors" -> 0.0)
    } else {
      val loadScores = allStates.map(_.loadScore).toSeq
      Map(
        "avgLoadScore" -> loadScores.sum / loadScores.size,
        "maxLoadScore" -> loadScores.max,
        "minLoadScore" -> loadScores.min,
        "activeExecutors" -> allStates.size.toDouble
      )
    }
  }

  /**
   * Predict future load for an executor based on historical patterns.
   */
  def predictExecutorLoad(executorId: String, forecastWindowMs: Long): Option[Double] = {
    if (!predictionEnabled) return None

    executorHistory.asScala.get(executorId).flatMap { history =>
      history.predictLoad(forecastWindowMs)
    }
  }

  /**
   * Get executors likely to become hotspots in the near future.
   */
  def getPredictedHotspots(forecastWindowMs: Long = 60000): Seq[String] = {
    if (!predictionEnabled) return Seq.empty

    executorHistory.asScala.flatMap { case (executorId, history) =>
      history.predictLoad(forecastWindowMs).filter(_ > 0.7).map(_ => executorId)
    }.toSeq
  }

  /**
   * Simple load shedding: just identify overloaded executors.
   */
  def generateLoadSheddingRecommendations(): Map[String, LoadSheddingAction.Value] = {
    getAllExecutorLoadStates.flatMap { case (executorId, loadState) =>
      if (!loadState.canAcceptMoreWork()) {
        Some(executorId -> LoadSheddingAction.THROTTLE_CONNECTIONS)
      } else {
        None
      }
    }
  }

  /**
   * Simple check: avoid executor if it can't accept more work.
   */
  def shouldAvoidExecutor(executorId: String): Boolean = {
    getExecutorLoadState(executorId) match {
      case Some(loadState) => !loadState.canAcceptMoreWork()
      case None => false
    }
  }

  /**
   * Simple throttling: reduce rate if executor is overloaded.
   */
  def getRequestThrottlingFactor(executorId: String): Double = {
    getExecutorLoadState(executorId) match {
      case Some(loadState) =>
        if (loadState.canAcceptMoreWork()) 1.0 else 0.5
      case None => 1.0
    }
  }

  /**
   * Tracks historical load data for an executor to enable predictive load balancing.
   */
  private class ExecutorHistoryTracker(maxWindowSize: Int) {
    private val dataPoints = scala.collection.mutable.Queue[TimestampedLoadData]()

    def addDataPoint(loadState: ExecutorLoadState): Unit = {
      synchronized {
        val dataPoint = TimestampedLoadData(
          timestamp = loadState.lastUpdateTime,
          loadScore = loadState.loadScore,
          avgResponseTime = loadState.avgResponseTime,
          bytesInFlight = loadState.bytesInFlight,
          activeConnections = loadState.activeConnections
        )

        dataPoints.enqueue(dataPoint)

        // Maintain sliding window
        while (dataPoints.size > maxWindowSize) {
          dataPoints.dequeue()
        }
      }
    }

    def predictLoad(forecastWindowMs: Long): Option[Double] = {
      synchronized {
        if (dataPoints.size < 3) return None // Need minimum data for prediction

        val currentTime = System.currentTimeMillis()
        val recentPoints = dataPoints.filter(_.timestamp > currentTime - forecastWindowMs * 2)

        if (recentPoints.size < 2) return None

        // Simple linear trend prediction
        val timeDeltas = recentPoints.zip(recentPoints.tail).map { case (prev, curr) =>
          curr.timestamp - prev.timestamp
        }
        val loadDeltas = recentPoints.zip(recentPoints.tail).map { case (prev, curr) =>
          curr.loadScore - prev.loadScore
        }

        if (timeDeltas.nonEmpty && timeDeltas.sum > 0) {
          val avgLoadChangeRate = loadDeltas.sum / timeDeltas.sum
          val currentLoad = recentPoints.last.loadScore
          val predictedLoad = currentLoad + (avgLoadChangeRate * forecastWindowMs)

          Some(math.max(0.0, math.min(1.0, predictedLoad))) // Clamp to [0, 1]
        } else {
          Some(recentPoints.last.loadScore) // No change prediction
        }
      }
    }

    def getTrend: LoadTrend.Value = {
      synchronized {
        if (dataPoints.size < 3) return LoadTrend.STABLE

        val recent = dataPoints.takeRight(5)
        val loadChanges = recent.zip(recent.tail).map { case (prev, curr) =>
          curr.loadScore - prev.loadScore
        }

        val avgChange = loadChanges.sum / loadChanges.size
        if (avgChange > 0.05) LoadTrend.INCREASING
        else if (avgChange < -0.05) LoadTrend.DECREASING
        else LoadTrend.STABLE
      }
    }
  }

  private case class TimestampedLoadData(
      timestamp: Long,
      loadScore: Double,
      avgResponseTime: Long,
      bytesInFlight: Long,
      activeConnections: Int)

  private object LoadTrend extends Enumeration {
    type LoadTrend = Value
    val INCREASING, DECREASING, STABLE = Value
  }
}
