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

import java.util.concurrent.{ConcurrentHashMap, ScheduledExecutorService, ScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.ThreadUtils

/**
 * Tracks shuffle fetch performance metrics for an individual request.
 */
private[spark] case class ShuffleFetchMetrics(
    requestId: String,
    sourceExecutor: String,
    startTime: Long,
    var queuedTime: Option[Long] = None,
    var networkStartTime: Option[Long] = None,
    var endTime: Option[Long] = None,
    var bytesTransferred: Long = 0L,
    var failed: Boolean = false) {

  def totalDuration: Long = endTime.map(_ - startTime).getOrElse(
    System.currentTimeMillis() - startTime)
  def waitingTime: Long = networkStartTime.map(_ - startTime).getOrElse(0L)
  def networkTime: Long = (networkStartTime, endTime) match {
    case (Some(start), Some(end)) => end - start
    case (Some(start), None) => System.currentTimeMillis() - start
    case _ => 0L
  }
  def queueingTime: Long = queuedTime.map(_ - startTime).getOrElse(0L)
  def isCompleted: Boolean = endTime.isDefined
  def isNetworkActive: Boolean = networkStartTime.isDefined && endTime.isEmpty
}

/**
 * Collects and tracks shuffle load metrics on an executor for load balancing.
 * This class is thread-safe and designed to handle high-frequency updates.
 */
private[spark] class ExecutorShuffleLoadCollector(
    executorId: String,
    conf: SparkConf) extends Logging {

  // Atomic counters for thread-safe metric tracking
  private val bytesInFlight = new AtomicLong(0L)
  private val activeConnections = new AtomicInteger(0)
  private val queueDepth = new AtomicInteger(0)
  private val totalRequests = new AtomicLong(0L)
  private val totalBytes = new AtomicLong(0L)
  private val totalDuration = new AtomicLong(0L)
  private val totalWaitingTime = new AtomicLong(0L)
  private val totalNetworkTime = new AtomicLong(0L)

  // Track individual fetch requests for detailed metrics
  private val activeFetches = new ConcurrentHashMap[String, ShuffleFetchMetrics]()

  // Configuration
  private val reportingInterval = conf.getLong(
    "spark.shuffle.loadbalancer.reportingIntervalMs", 5000L)
  private val maxConcurrentFetches = conf.getInt(
    "spark.shuffle.loadbalancer.maxConcurrentFetches", 100)
  private val networkCapacityBytes = conf.getLong(
    "spark.shuffle.loadbalancer.networkCapacityBytes", 100L * 1024L * 1024L)

  // Scheduled executor for periodic reporting
  private val reportingExecutor: ScheduledExecutorService =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("shuffle-load-reporter")
  private var reportingTask: Option[ScheduledFuture[_]] = None

  // Callback for sending metrics to driver
  private var metricsReporter: Option[ShuffleLoadMetrics => Unit] = None

  /**
   * Start collecting and reporting shuffle load metrics.
   */
  def start(reporter: ShuffleLoadMetrics => Unit): Unit = {
    metricsReporter = Some(reporter)

    val task = reportingExecutor.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          try {
            reportMetrics()
          } catch {
            case e: Exception =>
              logWarning("Failed to report shuffle load metrics", e)
          }
        }
      },
      reportingInterval,
      reportingInterval,
      TimeUnit.MILLISECONDS
    )

    reportingTask = Some(task)
    logInfo(s"Started shuffle load collector for executor $executorId with " +
      s"${reportingInterval}ms reporting interval")
  }

  /**
   * Stop collecting and reporting metrics.
   */
  def stop(): Unit = {
    reportingTask.foreach(_.cancel(false))
    reportingTask = None
    reportingExecutor.shutdown()
    logInfo(s"Stopped shuffle load collector for executor $executorId")
  }

  /**
   * Record the start of a shuffle fetch request.
   */
  def recordFetchStart(requestId: String, sourceExecutor: String, requestSize: Long): Unit = {
    val metrics = ShuffleFetchMetrics(requestId, sourceExecutor, System.currentTimeMillis())
    activeFetches.put(requestId, metrics)

    bytesInFlight.addAndGet(requestSize)
    activeConnections.incrementAndGet()
    queueDepth.incrementAndGet()

    logDebug(s"Started fetch request $requestId from $sourceExecutor, size: $requestSize bytes")
  }

  /**
   * Record the completion of a shuffle fetch request.
   */
  def recordFetchCompletion(requestId: String, bytesTransferred: Long, success: Boolean): Unit = {
    Option(activeFetches.remove(requestId)).foreach { metrics =>
      val endTime = System.currentTimeMillis()
      val duration = endTime - metrics.startTime

      metrics.endTime = Some(endTime)
      metrics.bytesTransferred = bytesTransferred
      metrics.failed = !success

      // Update global counters
      bytesInFlight.addAndGet(-bytesTransferred)
      activeConnections.decrementAndGet()
      queueDepth.decrementAndGet()
      totalRequests.incrementAndGet()
      totalBytes.addAndGet(bytesTransferred)
      totalDuration.addAndGet(duration)
      totalWaitingTime.addAndGet(metrics.waitingTime)
      totalNetworkTime.addAndGet(metrics.networkTime)

      logDebug(s"Completed fetch request $requestId: $bytesTransferred bytes in ${duration}ms, " +
        s"success: $success")
    }
  }

  /**
   * Record when a fetch request is queued (waiting to be processed).
   */
  def recordFetchQueued(): Unit = {
    queueDepth.incrementAndGet()
  }

  /**
   * Record when a queued fetch request starts processing.
   */
  def recordFetchDequeued(): Unit = {
    queueDepth.decrementAndGet()
  }

  /**
   * Record when a fetch request moves from queued to active network state.
   */
  def recordFetchNetworkStart(requestId: String): Unit = {
    Option(activeFetches.get(requestId)).foreach { metrics =>
      metrics.networkStartTime = Some(System.currentTimeMillis())
      logDebug(s"Network transfer started for request $requestId after " +
        s"${metrics.waitingTime}ms waiting time")
    }
  }

  /**
   * Record when a fetch request is initially queued (before network transfer).
   */
  def recordFetchQueued(requestId: String): Unit = {
    Option(activeFetches.get(requestId)).foreach { metrics =>
      metrics.queuedTime = Some(System.currentTimeMillis())
      queueDepth.incrementAndGet()
      logDebug(s"Request $requestId queued after ${metrics.queueingTime}ms")
    }
  }

  /**
   * Get current load metrics snapshot.
   */
  def getCurrentMetrics: ShuffleLoadMetrics = {
    val requests = totalRequests.get()
    val avgResponseTime = if (requests > 0) totalDuration.get() / requests else 0L
    val avgWaitingTime = if (requests > 0) totalWaitingTime.get() / requests else 0L
    val avgNetworkTime = if (requests > 0) totalNetworkTime.get() / requests else 0L

    ShuffleLoadMetrics(
      executorId = executorId,
      bytesInFlight = bytesInFlight.get(),
      activeConnections = activeConnections.get(),
      networkCapacity = networkCapacityBytes,
      avgResponseTime = avgResponseTime,
      avgWaitingTime = avgWaitingTime,
      avgNetworkTime = avgNetworkTime,
      queueDepth = queueDepth.get(),
      timestamp = System.currentTimeMillis()
    )
  }

  /**
   * Report current metrics to the driver.
   */
  private def reportMetrics(): Unit = {
    metricsReporter.foreach { reporter =>
      val metrics = getCurrentMetrics
      reporter(metrics)
      logDebug(s"Reported metrics for executor $executorId: " +
        s"bytesInFlight=${metrics.bytesInFlight}, " +
        s"activeConnections=${metrics.activeConnections}, " +
        s"queueDepth=${metrics.queueDepth}, " +
        s"avgResponseTime=${metrics.avgResponseTime}ms " +
        s"(waiting=${metrics.avgWaitingTime}ms, network=${metrics.avgNetworkTime}ms)")
    }
  }

  /**
   * Check if the executor is currently overloaded.
   */
  def isOverloaded: Boolean = {
    val currentMetrics = getCurrentMetrics
    val loadThreshold = conf.getDouble("spark.shuffle.loadbalancer.overloadThreshold", 0.8)

    val capacityUtilization = if (networkCapacityBytes > 0) {
      currentMetrics.bytesInFlight.toDouble / networkCapacityBytes
    } else {
      0.0
    }

    val connectionPressure = currentMetrics.activeConnections.toDouble / maxConcurrentFetches
    val queuePressure = currentMetrics.queueDepth.toDouble / 10.0

    val loadScore = (capacityUtilization + connectionPressure + queuePressure) / 3.0
    loadScore > loadThreshold
  }

  /**
   * Get statistics about completed fetch requests for the last reporting period.
   */
  def getCompletedFetchStats: Map[String, Any] = {
    val completedFetches = activeFetches.asScala.values.filter(_.isCompleted).toSeq

    if (completedFetches.isEmpty) {
      Map(
        "count" -> 0,
        "totalBytes" -> 0L,
        "avgDuration" -> 0L,
        "successRate" -> 1.0
      )
    } else {
      val successCount = completedFetches.count(!_.failed)
      val totalBytes = completedFetches.map(_.bytesTransferred).sum
      val avgDuration = completedFetches.map(_.totalDuration).sum / completedFetches.size
      val successRate = successCount.toDouble / completedFetches.size

      Map(
        "count" -> completedFetches.size,
        "totalBytes" -> totalBytes,
        "avgDuration" -> avgDuration,
        "successRate" -> successRate
      )
    }
  }

  /**
   * Reset metrics (useful for testing).
   */
  private[storage] def reset(): Unit = {
    bytesInFlight.set(0L)
    activeConnections.set(0)
    queueDepth.set(0)
    totalRequests.set(0L)
    totalBytes.set(0L)
    totalDuration.set(0L)
    totalWaitingTime.set(0L)
    totalNetworkTime.set(0L)
    activeFetches.clear()
  }
}
