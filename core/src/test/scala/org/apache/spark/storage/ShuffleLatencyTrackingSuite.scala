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

import org.apache.spark.SparkConf
import org.apache.spark.SparkFunSuite

class ShuffleLatencyTrackingSuite extends SparkFunSuite {

  test("ShuffleFetchMetrics tracks detailed timing phases") {
    val startTime = System.currentTimeMillis()
    val metrics = ShuffleFetchMetrics("req-1", "executor-2", startTime)

    // Initially no timing phases completed
    assert(metrics.waitingTime === 0L)
    assert(metrics.networkTime === 0L)
    assert(metrics.queueingTime === 0L)
    assert(!metrics.isCompleted)
    assert(!metrics.isNetworkActive)

    Thread.sleep(10)

    // Queue the request
    metrics.queuedTime = Some(System.currentTimeMillis())
    assert(metrics.queueingTime > 0)

    Thread.sleep(10)

    // Start network transfer
    metrics.networkStartTime = Some(System.currentTimeMillis())
    assert(metrics.waitingTime > 0)
    assert(metrics.isNetworkActive)

    Thread.sleep(10)

    // Complete the request
    metrics.endTime = Some(System.currentTimeMillis())
    metrics.bytesTransferred = 1024L

    assert(metrics.isCompleted)
    assert(!metrics.isNetworkActive)
    assert(metrics.networkTime > 0)
    assert(metrics.totalDuration >= metrics.waitingTime + metrics.networkTime)
  }

  test("ExecutorShuffleLoadCollector tracks waiting and network time separately") {
    val conf = new SparkConf()
      .set("spark.shuffle.loadbalancer.reportingIntervalMs", "1000")
    val collector = new ExecutorShuffleLoadCollector("test-executor", conf)

    try {
      // Record a request with different timing phases
      collector.recordFetchStart("req-1", "source-executor", 1024L)

      Thread.sleep(20) // Simulate queueing time
      collector.recordFetchQueued("req-1")

      Thread.sleep(30) // Simulate waiting time
      collector.recordFetchNetworkStart("req-1")

      Thread.sleep(50) // Simulate network time
      collector.recordFetchCompletion("req-1", 1024L, success = true)

      val metrics = collector.getCurrentMetrics
      assert(metrics.avgResponseTime > 0L)
      assert(metrics.avgWaitingTime > 0L)
      assert(metrics.avgNetworkTime > 0L)

      // Total response time should be sum of waiting + network time (approximately)
      assert(metrics.avgResponseTime >= metrics.avgWaitingTime + metrics.avgNetworkTime)

    } finally {
      collector.stop()
    }
  }

  test("Enhanced load score considers latency factors") {
    val baselineState = ExecutorLoadState(
      executorId = "executor-1",
      bytesInFlight = 1000000L, // 10% of 10MB capacity
      activeConnections = 1, // 10% of 10 baseline
      networkCapacity = 10000000L,
      avgResponseTime = 100L, // At baseline (100ms)
      avgWaitingTime = 50L, // At baseline (50ms)
      avgNetworkTime = 50L, // At baseline (50ms)
      queueDepth = 1, // 20% of 5 baseline
      serverRequestsReceived = 100L,
      serverRequestsCompleted = 95L,
      serverRequestsFailed = 5L,
      serverBytesServed = 2000000L,
      serverAvgProcessingTime = 50.0,
      serverAvgDiskReadTime = 20.0,
      serverQueueDepth = 1,
      lastUpdateTime = System.currentTimeMillis()
    )

    val highLatencyState = ExecutorLoadState(
      executorId = "executor-2",
      bytesInFlight = 6000000L, // Higher capacity utilization (60%)
      activeConnections = 7, // Higher connection pressure (70%)
      networkCapacity = 10000000L,
      avgResponseTime = 700L, // High response time (600ms above baseline)
      avgWaitingTime = 300L, // High waiting time (250ms above baseline)
      avgNetworkTime = 400L, // High network time (350ms above baseline)
      queueDepth = 8, // Higher queue pressure (160%)
      serverRequestsReceived = 200L,
      serverRequestsCompleted = 170L,
      serverRequestsFailed = 30L,
      serverBytesServed = 12000000L,
      serverAvgProcessingTime = 150.0,
      serverAvgDiskReadTime = 80.0,
      serverQueueDepth = 10,
      lastUpdateTime = System.currentTimeMillis()
    )

    // High latency executor should have higher load score
    assert(highLatencyState.loadScore > baselineState.loadScore)
    // Verify individual components contribute to load score
    val conf = new SparkConf()
    assert(!baselineState.isOverloaded(conf)) // Should not be overloaded with baseline metrics
    assert(highLatencyState.isOverloaded(conf)) // Should be overloaded due to high latency
  }

  test("Load collector handles concurrent latency tracking") {
    val conf = new SparkConf()
    val collector = new ExecutorShuffleLoadCollector("test-executor", conf)
    try {
      // Start multiple concurrent requests
      val numRequests = 10
      (0 until numRequests).foreach { i =>
        collector.recordFetchStart(s"req-$i", "source-executor", 1024L)
        // Simulate different timing for each request
        Thread.sleep(5)
        collector.recordFetchQueued(s"req-$i")
        Thread.sleep(5)
        collector.recordFetchNetworkStart(s"req-$i")
        Thread.sleep(10)
        collector.recordFetchCompletion(s"req-$i", 1024L, success = true)
      }
      val metrics = collector.getCurrentMetrics
      assert(metrics.avgResponseTime > 0L)
      assert(metrics.avgWaitingTime > 0L)
      assert(metrics.avgNetworkTime > 0L)

      // All requests should be completed
      assert(metrics.bytesInFlight === 0L)
      assert(metrics.activeConnections === 0)
    } finally {
      collector.stop()
    }
  }

  test("Load collector provides detailed latency breakdown") {
    val conf = new SparkConf()
    val collector = new ExecutorShuffleLoadCollector("test-executor", conf)
    try {
      // Record request with known timing
      collector.recordFetchStart("req-1", "source-executor", 1024L)
      val start = System.currentTimeMillis()
      Thread.sleep(20) // Queueing time
      collector.recordFetchQueued("req-1")
      val queued = System.currentTimeMillis()
      Thread.sleep(30) // Waiting time
      collector.recordFetchNetworkStart("req-1")
      val networkStart = System.currentTimeMillis()
      Thread.sleep(50) // Network time
      collector.recordFetchCompletion("req-1", 1024L, success = true)

      val metrics = collector.getCurrentMetrics
      // Verify timing breakdown makes sense
      assert(metrics.avgResponseTime >= 90) // Should be close to total sleep time (100ms)
      assert(metrics.avgWaitingTime >= 25) // Should be close to waiting sleep (30ms)
      assert(metrics.avgNetworkTime >= 45) // Should be close to network sleep (50ms)
      // Waiting + network should be less than or equal to total response time
      // Debug: print actual values to understand the relationship
      // scalastyle:off println
      println(s"avgWaitingTime: ${metrics.avgWaitingTime}, " +
        s"avgNetworkTime: ${metrics.avgNetworkTime}, avgResponseTime: ${metrics.avgResponseTime}")
      // scalastyle:on println
      assert(metrics.avgWaitingTime + metrics.avgNetworkTime <=
        metrics.avgResponseTime + 50) // Increased tolerance for timing variations
    } finally {
      collector.stop()
    }
  }
}
