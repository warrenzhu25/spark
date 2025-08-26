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
 * Tracks the current load state of an executor for shuffle operations.
 */
private[spark] case class ExecutorLoadState(
    executorId: String,
    bytesInFlight: Long,
    activeConnections: Int,
    networkCapacity: Long,
    avgResponseTime: Long,
    avgWaitingTime: Long,
    avgNetworkTime: Long,
    queueDepth: Int,
    lastUpdateTime: Long) {

  /**
   * Calculate a load score for this executor. Higher scores indicate higher load.
   * Score ranges from 0.0 (no load) to 1.0+ (overloaded).
   */
  def loadScore: Double = {
    val capacityUtilization = if (networkCapacity > 0) {
      bytesInFlight.toDouble / networkCapacity
    } else {
      0.0
    }
    val connectionPressure = activeConnections.toDouble / 10.0 // Assume 10 as baseline
    val queuePressure = queueDepth.toDouble / 5.0 // Assume 5 as baseline
    val responsePressure = Math.max(0.0, (avgResponseTime - 100.0) / 500.0) // 100ms baseline
    val waitingPressure = Math.max(0.0, (avgWaitingTime - 50.0) / 200.0) // 50ms baseline, 200ms max
    val networkPressure = Math.max(0.0, (avgNetworkTime - 50.0) / 300.0) // 50ms baseline, 300ms max

    (capacityUtilization + connectionPressure + queuePressure + responsePressure +
      waitingPressure + networkPressure) / 6.0
  }

  /**
   * Check if this executor is overloaded based on configurable thresholds.
   */
  def isOverloaded(conf: SparkConf): Boolean = {
    val loadThreshold = conf.getDouble("spark.shuffle.loadbalancer.overloadThreshold", 0.8)
    loadScore > loadThreshold
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
      bytesInFlight = metrics.bytesInFlight,
      activeConnections = metrics.activeConnections,
      networkCapacity = metrics.networkCapacity,
      avgResponseTime = metrics.avgResponseTime,
      avgWaitingTime = metrics.avgWaitingTime,
      avgNetworkTime = metrics.avgNetworkTime,
      queueDepth = metrics.queueDepth,
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
   * Calculate optimal fetch strategy for a target executor.
   */
  def calculateOptimalFetchStrategy(targetExecutor: String): Option[ShuffleFetchDirective] = {
    getExecutorLoadState(targetExecutor).map { targetState =>
      val preferredSources = getPreferredSourceExecutors(targetExecutor)
      val recommendedRequestSize = targetState.recommendedRequestSize(
        conf.getLong("spark.reducer.maxSizeInFlight", 48) * 1024 * 1024 / 5, conf)
      val throttleDelay = if (targetState.isOverloaded(conf)) 10 else 0
      val priority = if (targetState.loadScore < 0.5) {
        1
      } else if (targetState.loadScore < 0.8) {
        2
      } else {
        3
      }

      ShuffleFetchDirective(
        targetExecutor = targetExecutor,
        preferredSources = preferredSources,
        maxRequestSize = recommendedRequestSize,
        throttleDelay = throttleDelay,
        priority = priority
      )
    }
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
