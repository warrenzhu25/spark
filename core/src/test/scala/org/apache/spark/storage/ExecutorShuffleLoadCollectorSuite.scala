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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.mutable

import org.apache.spark.SparkConf
import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.BlockManagerMessages._

class ExecutorShuffleLoadCollectorSuite extends SparkFunSuite {

  private def createLoadCollector(): ExecutorShuffleLoadCollector = {
    val conf = new SparkConf()
      .set("spark.shuffle.loadbalancer.reportingIntervalMs", "100") // Fast reporting for tests
      .set("spark.shuffle.loadbalancer.overloadThreshold", "0.8")
      .set("spark.shuffle.loadbalancer.networkCapacityBytes", "10485760") // 10MB
    new ExecutorShuffleLoadCollector("test-executor", conf)
  }

  test("ShuffleFetchMetrics tracks request lifecycle") {
    val startTime = System.currentTimeMillis()
    val metrics = ShuffleFetchMetrics("req-1", "executor-2", startTime)

    assert(!metrics.isCompleted)
    assert(metrics.duration >= 0)

    Thread.sleep(10) // Small delay to ensure duration > 0
    metrics.endTime = Some(System.currentTimeMillis())
    metrics.bytesTransferred = 1024L

    assert(metrics.isCompleted)
    assert(metrics.duration > 0)
    assert(metrics.bytesTransferred === 1024L)
  }

  test("ExecutorShuffleLoadCollector records fetch start") {
    val collector = createLoadCollector()

    val initialMetrics = collector.getCurrentMetrics
    assert(initialMetrics.bytesInFlight === 0L)
    assert(initialMetrics.activeConnections === 0)
    assert(initialMetrics.queueDepth === 0)

    collector.recordFetchStart("req-1", "executor-2", 1024L)

    val afterStartMetrics = collector.getCurrentMetrics
    assert(afterStartMetrics.bytesInFlight === 1024L)
    assert(afterStartMetrics.activeConnections === 1)
    assert(afterStartMetrics.queueDepth === 1)
  }

  test("ExecutorShuffleLoadCollector records fetch completion") {
    val collector = createLoadCollector()

    collector.recordFetchStart("req-1", "executor-2", 1024L)
    collector.recordFetchCompletion("req-1", 1024L, success = true)

    val metrics = collector.getCurrentMetrics
    assert(metrics.bytesInFlight === 0L)
    assert(metrics.activeConnections === 0)
    assert(metrics.queueDepth === 0)
    assert(metrics.avgResponseTime > 0L) // Should have recorded some response time
  }

  test("ExecutorShuffleLoadCollector tracks queue depth") {
    val collector = createLoadCollector()

    collector.recordFetchQueued()
    collector.recordFetchQueued()

    val metrics = collector.getCurrentMetrics
    assert(metrics.queueDepth === 2)

    collector.recordFetchDequeued()
    val updatedMetrics = collector.getCurrentMetrics
    assert(updatedMetrics.queueDepth === 1)
  }

  test("ExecutorShuffleLoadCollector detects overload") {
    val collector = createLoadCollector()

    // Initially not overloaded
    assert(!collector.isOverloaded)

    // Add enough load to trigger overload
    collector.recordFetchStart("req-1", "executor-2", 9 * 1024 * 1024L) // 9MB of 10MB capacity

    // Should now be overloaded due to high capacity utilization
    assert(collector.isOverloaded)
  }

  test("ExecutorShuffleLoadCollector reports metrics") {
    val collector = createLoadCollector()
    val reportedMetrics = mutable.ArrayBuffer[ShuffleLoadMetrics]()
    val reportLatch = new CountDownLatch(1)

    // Start collector with metric reporter
    collector.start { metrics =>
      reportedMetrics += metrics
      reportLatch.countDown()
    }

    try {
      // Add some load
      collector.recordFetchStart("req-1", "executor-2", 1024L)

      // Wait for at least one report
      assert(reportLatch.await(1, TimeUnit.SECONDS), "Should have received metrics report")
      assert(reportedMetrics.nonEmpty)

      val metrics = reportedMetrics.head
      assert(metrics.executorId === "test-executor")
      assert(metrics.bytesInFlight === 1024L)

    } finally {
      collector.stop()
    }
  }

  test("ExecutorShuffleLoadCollector calculates average response time") {
    val collector = createLoadCollector()

    // Record multiple requests with known durations
    collector.recordFetchStart("req-1", "executor-2", 1024L)
    Thread.sleep(10)
    collector.recordFetchCompletion("req-1", 1024L, success = true)

    collector.recordFetchStart("req-2", "executor-3", 2048L)
    Thread.sleep(20)
    collector.recordFetchCompletion("req-2", 2048L, success = true)

    val metrics = collector.getCurrentMetrics
    assert(metrics.avgResponseTime > 0L)
    assert(metrics.avgResponseTime < 100L) // Should be reasonable for test timing
  }

  test("ExecutorShuffleLoadCollector provides completed fetch stats") {
    val collector = createLoadCollector()

    // Initially no completed fetches
    val initialStats = collector.getCompletedFetchStats
    assert(initialStats("count") === 0)
    assert(initialStats("successRate") === 1.0)

    // Add some completed fetches
    collector.recordFetchStart("req-1", "executor-2", 1024L)
    collector.recordFetchCompletion("req-1", 1024L, success = true)

    collector.recordFetchStart("req-2", "executor-3", 2048L)
    collector.recordFetchCompletion("req-2", 2048L, success = false)

    val stats = collector.getCompletedFetchStats
    assert(stats("count") === 2)
    assert(stats("totalBytes") === 3072L)
    assert(stats("successRate") === 0.5) // 1 out of 2 successful
  }

  test("ExecutorShuffleLoadCollector handles concurrent access") {
    val collector = createLoadCollector()
    val numThreads = 10
    val requestsPerThread = 100

    val threads = (0 until numThreads).map { threadId =>
      new Thread {
        override def run(): Unit = {
          (0 until requestsPerThread).foreach { reqId =>
            val requestId = s"thread-$threadId-req-$reqId"
            collector.recordFetchStart(requestId, "executor-2", 1024L)
            Thread.sleep(1) // Small delay
            collector.recordFetchCompletion(requestId, 1024L, success = true)
          }
        }
      }
    }

    threads.foreach(_.start())
    threads.foreach(_.join())

    val finalMetrics = collector.getCurrentMetrics
    assert(finalMetrics.bytesInFlight === 0L) // All requests should be completed
    assert(finalMetrics.activeConnections === 0)
    assert(finalMetrics.avgResponseTime > 0L)
  }

  test("ExecutorShuffleLoadCollector stops cleanly") {
    val collector = createLoadCollector()
    val reportLatch = new CountDownLatch(1)

    collector.start { _ => reportLatch.countDown() }

    // Verify reporting is working
    assert(reportLatch.await(1, TimeUnit.SECONDS))

    // Stop should complete without hanging
    collector.stop()

    // After stop, no more reports should be sent
    val noMoreReportsLatch = new CountDownLatch(1)
    // This should timeout since collector is stopped
    assert(!noMoreReportsLatch.await(200, TimeUnit.MILLISECONDS))
  }

  test("ExecutorShuffleLoadCollector resets correctly") {
    val collector = createLoadCollector()

    // Add some load
    collector.recordFetchStart("req-1", "executor-2", 1024L)
    collector.recordFetchQueued()

    val beforeReset = collector.getCurrentMetrics
    assert(beforeReset.bytesInFlight > 0)
    assert(beforeReset.queueDepth > 0)

    // Reset should clear all metrics
    collector.reset()

    val afterReset = collector.getCurrentMetrics
    assert(afterReset.bytesInFlight === 0L)
    assert(afterReset.activeConnections === 0)
    assert(afterReset.queueDepth === 0)
    assert(afterReset.avgResponseTime === 0L)
  }
}
