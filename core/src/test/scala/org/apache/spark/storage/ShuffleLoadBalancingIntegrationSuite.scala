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

import scala.collection.mutable

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.storage.BlockManagerMessages._

class ShuffleLoadBalancingIntegrationSuite extends SparkFunSuite {

  test("ShuffleLoadBalancer and ExecutorShuffleLoadCollector integration") {
    val conf = new SparkConf()
      .set("spark.shuffle.loadbalancer.overloadThreshold", "0.6")
      .set("spark.shuffle.loadbalancer.reportingIntervalMs", "100")
    val loadBalancer = new ShuffleLoadBalancer(conf)
    val collector = new ExecutorShuffleLoadCollector("test-executor", conf)
    try {
      // Test basic integration
      val reportedMetrics = mutable.ArrayBuffer[ShuffleLoadMetrics]()
      collector.start { metrics =>
        reportedMetrics += metrics
        loadBalancer.updateExecutorLoad(metrics)
      }
      // Add some load to the collector
      collector.recordFetchStart("req-1", "source-executor", 1024L)
      // Wait for metrics to be reported
      Thread.sleep(200)
      // Verify metrics were reported and processed by load balancer
      assert(reportedMetrics.nonEmpty)
      val loadState = loadBalancer.getExecutorLoadState("test-executor")
      assert(loadState.isDefined)
      assert(loadState.get.bytesInFlight > 0)
    } finally {
      collector.stop()
    }
  }

  test("Load balancer generates appropriate directives based on collector metrics") {
    val conf = new SparkConf()
      .set("spark.shuffle.loadbalancer.overloadThreshold", "0.5")
    val loadBalancer = new ShuffleLoadBalancer(conf)
    // Add a heavily loaded executor
    val heavyLoadMetrics = ShuffleLoadMetrics(
      executorId = "heavy-executor",
      bytesInFlight = 8 * 1024 * 1024L, // 8MB
      activeConnections = 10,
      networkCapacity = 10 * 1024 * 1024L, // 10MB capacity
      avgResponseTime = 500L,
      avgWaitingTime = 200L,
      avgNetworkTime = 300L,
      queueDepth = 8,
      timestamp = System.currentTimeMillis()
    )
    // Add a lightly loaded executor
    val lightLoadMetrics = ShuffleLoadMetrics(
      executorId = "light-executor",
      bytesInFlight = 1 * 1024 * 1024L, // 1MB
      activeConnections = 2,
      networkCapacity = 10 * 1024 * 1024L, // 10MB capacity
      avgResponseTime = 50L,
      avgWaitingTime = 20L,
      avgNetworkTime = 30L,
      queueDepth = 1,
      timestamp = System.currentTimeMillis()
    )
    loadBalancer.updateExecutorLoad(heavyLoadMetrics)
    loadBalancer.updateExecutorLoad(lightLoadMetrics)
    // Get strategy for heavy executor
    val heavyStrategy = loadBalancer.calculateOptimalFetchStrategy("heavy-executor")
    assert(heavyStrategy.isDefined)
    val directive = heavyStrategy.get
    assert(directive.targetExecutor === "heavy-executor")
    assert(directive.preferredSources.contains("light-executor")) // Should prefer light executor
    assert(directive.throttleDelay > 0) // Should throttle heavy executor
    assert(directive.priority === 3) // High priority due to high load
    // Light executor should get better treatment
    val lightStrategy = loadBalancer.calculateOptimalFetchStrategy("light-executor")
    assert(lightStrategy.isDefined)
    assert(lightStrategy.get.throttleDelay === 0) // No throttling for light executor
  }

  test("Cluster load statistics reflect current state") {
    val conf = new SparkConf()
    val loadBalancer = new ShuffleLoadBalancer(conf)
    // Initially empty cluster
    val emptyStats = loadBalancer.getClusterLoadStats
    assert(emptyStats("activeExecutors") === 0.0)
    // Add executors with different loads
    loadBalancer.updateExecutorLoad(ShuffleLoadMetrics(
      "executor-1", 1000000L, 5, 10000000L, 100L, 40L, 60L, 2, System.currentTimeMillis()))
    loadBalancer.updateExecutorLoad(ShuffleLoadMetrics(
      "executor-2", 5000000L, 8, 10000000L, 200L, 100L, 100L, 5, System.currentTimeMillis()))
    val stats = loadBalancer.getClusterLoadStats
    assert(stats("activeExecutors") === 2.0)
    assert(stats("avgLoadScore") > 0.0)
    assert(stats("maxLoadScore") >= stats("minLoadScore"))
  }

  test("Configuration updates adapt to cluster load") {
    val conf = new SparkConf()
      .set("spark.reducer.maxSizeInFlight", "48") // 48MB base
    val loadBalancer = new ShuffleLoadBalancer(conf)
    // Add executors with varying loads
    val lightLoad = ShuffleLoadMetrics("light", 1000000L, 2, 10000000L, 50L, 20L, 30L, 1,
      System.currentTimeMillis())
    val heavyLoad = ShuffleLoadMetrics("heavy", 8000000L, 8, 10000000L, 400L, 150L, 250L, 8,
      System.currentTimeMillis())
    loadBalancer.updateExecutorLoad(lightLoad)
    loadBalancer.updateExecutorLoad(heavyLoad)
    val configUpdates = loadBalancer.generateConfigUpdates()
    assert(configUpdates.length === 2)
    val lightConfig = configUpdates.find(_._1 == "light").get._2
    val heavyConfig = configUpdates.find(_._1 == "heavy").get._2
    // Heavy executor should get reduced limits
    assert(heavyConfig.maxBlocksInFlightPerAddress === 3)
    // Light executor should get normal/increased limits
    assert(lightConfig.maxBlocksInFlightPerAddress === 5)
  }

  test("ExecutorShuffleLoadCollector detects and reports overload") {
    val conf = new SparkConf()
      .set("spark.shuffle.loadbalancer.overloadThreshold", "0.5")
      .set("spark.shuffle.loadbalancer.networkCapacityBytes", "1048576") // 1MB
    val collector = new ExecutorShuffleLoadCollector("test-executor", conf)
    // Initially not overloaded
    assert(!collector.isOverloaded)
    // Add load close to capacity
    collector.recordFetchStart("req-1", "source-1", 600000L) // 600KB
    assert(collector.isOverloaded) // Should be overloaded (60% > 50% threshold)
    // Complete the request - should reduce load
    collector.recordFetchCompletion("req-1", 600000L, success = true)
    assert(!collector.isOverloaded) // Should no longer be overloaded
  }

  test("Load balancing respects executor removal") {
    val conf = new SparkConf()
    val loadBalancer = new ShuffleLoadBalancer(conf)
    // Add executor
    val metrics = ShuffleLoadMetrics("executor-1", 1000L, 1, 1000000L, 100L, 40L, 60L, 1,
      System.currentTimeMillis())
    loadBalancer.updateExecutorLoad(metrics)
    assert(loadBalancer.getExecutorLoadState("executor-1").isDefined)
    // Remove executor
    loadBalancer.removeExecutor("executor-1")
    assert(loadBalancer.getExecutorLoadState("executor-1").isEmpty)
    // Cluster stats should reflect removal
    val stats = loadBalancer.getClusterLoadStats
    assert(stats("activeExecutors") === 0.0)
  }
}
