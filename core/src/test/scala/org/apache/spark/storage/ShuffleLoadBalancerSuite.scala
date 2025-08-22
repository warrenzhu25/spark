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
import org.apache.spark.storage.BlockManagerMessages._

class ShuffleLoadBalancerSuite extends SparkFunSuite {

  private def createLoadBalancer(): ShuffleLoadBalancer = {
    val conf = new SparkConf()
      .set("spark.shuffle.loadbalancer.overloadThreshold", "0.8")
      .set("spark.shuffle.loadbalancer.minRequestSize", "1048576") // 1MB
      .set("spark.shuffle.loadbalancer.maxPreferredSources", "3")
    new ShuffleLoadBalancer(conf)
  }

  private def createLoadMetrics(
      executorId: String,
      bytesInFlight: Long = 1024L,
      activeConnections: Int = 5,
      networkCapacity: Long = 1000000L,
      avgResponseTime: Long = 100L,
      avgWaitingTime: Long = 50L,
      avgNetworkTime: Long = 50L,
      queueDepth: Int = 3): ShuffleLoadMetrics = {
    ShuffleLoadMetrics(
      executorId = executorId,
      bytesInFlight = bytesInFlight,
      activeConnections = activeConnections,
      networkCapacity = networkCapacity,
      avgResponseTime = avgResponseTime,
      avgWaitingTime = avgWaitingTime,
      avgNetworkTime = avgNetworkTime,
      queueDepth = queueDepth,
      timestamp = System.currentTimeMillis()
    )
  }

  test("ExecutorLoadState load score calculation") {
    val loadState = ExecutorLoadState(
      executorId = "executor-1",
      bytesInFlight = 500000L, // 50% of network capacity
      activeConnections = 5, // 50% of baseline (10)
      networkCapacity = 1000000L,
      avgResponseTime = 350L, // 250ms above baseline (100ms)
      queueDepth = 5, // 100% of baseline (5)
      lastUpdateTime = System.currentTimeMillis()
    )

    // Load score should be average of: 0.5 + 0.5 + 1.0 + 0.5 = 2.5 / 4 = 0.625
    assert(math.abs(loadState.loadScore - 0.625) < 0.01)
  }

  test("ExecutorLoadState overload detection") {
    val conf = new SparkConf().set("spark.shuffle.loadbalancer.overloadThreshold", "0.7")

    val overloadedState = ExecutorLoadState(
      executorId = "executor-1",
      bytesInFlight = 800000L, // 80% of capacity
      activeConnections = 8,
      networkCapacity = 1000000L,
      avgResponseTime = 400L,
      queueDepth = 8,
      lastUpdateTime = System.currentTimeMillis()
    )

    val normalState = ExecutorLoadState(
      executorId = "executor-2",
      bytesInFlight = 200000L, // 20% of capacity
      activeConnections = 2,
      networkCapacity = 1000000L,
      avgResponseTime = 50L,
      queueDepth = 1,
      lastUpdateTime = System.currentTimeMillis()
    )

    assert(overloadedState.isOverloaded(conf))
    assert(!normalState.isOverloaded(conf))
  }

  test("ShuffleLoadBalancer updates executor load") {
    val loadBalancer = createLoadBalancer()
    val metrics = createLoadMetrics("executor-1")

    loadBalancer.updateExecutorLoad(metrics)

    val loadState = loadBalancer.getExecutorLoadState("executor-1")
    assert(loadState.isDefined)
    assert(loadState.get.executorId === "executor-1")
    assert(loadState.get.bytesInFlight === 1024L)
  }

  test("ShuffleLoadBalancer calculates optimal fetch strategy") {
    val loadBalancer = createLoadBalancer()

    // Add multiple executors with different load levels
    loadBalancer.updateExecutorLoad(
      createLoadMetrics("executor-1", bytesInFlight = 100000L)) // low load
    loadBalancer.updateExecutorLoad(
      createLoadMetrics("executor-2", bytesInFlight = 500000L)) // medium load
    loadBalancer.updateExecutorLoad(
      createLoadMetrics("executor-3", bytesInFlight = 900000L)) // high load

    val strategy = loadBalancer.calculateOptimalFetchStrategy("executor-3")
    assert(strategy.isDefined)

    val directive = strategy.get
    assert(directive.targetExecutor === "executor-3")
    assert(directive.preferredSources.contains("executor-1")) // Should prefer low-load executor
    assert(directive.throttleDelay > 0) // Should throttle high-load executor
    assert(directive.priority === 3) // High priority due to high load
  }

  test("ShuffleLoadBalancer gets preferred source executors") {
    val loadBalancer = createLoadBalancer()

    // Add executors with different load levels
    loadBalancer.updateExecutorLoad(
      createLoadMetrics("executor-1", bytesInFlight = 100000L)) // lowest load
    loadBalancer.updateExecutorLoad(
      createLoadMetrics("executor-2", bytesInFlight = 300000L)) // medium load
    loadBalancer.updateExecutorLoad(
      createLoadMetrics("executor-3", bytesInFlight = 800000L)) // highest load
    loadBalancer.updateExecutorLoad(
      createLoadMetrics("executor-4", bytesInFlight = 200000L)) // low load

    val preferredSources = loadBalancer.getPreferredSourceExecutors("executor-3")

    // Should return executors ordered by load score (lowest first), excluding target executor
    assert(preferredSources.length === 3) // maxPreferredSources = 3
    assert(preferredSources.head === "executor-1") // Lowest load first
    assert(preferredSources.contains("executor-4")) // Second lowest load
    assert(preferredSources.contains("executor-2")) // Medium load
    assert(!preferredSources.contains("executor-3")) // Should not include target executor
  }

  test("ShuffleLoadBalancer removes executor") {
    val loadBalancer = createLoadBalancer()
    val metrics = createLoadMetrics("executor-1")

    loadBalancer.updateExecutorLoad(metrics)
    assert(loadBalancer.getExecutorLoadState("executor-1").isDefined)

    loadBalancer.removeExecutor("executor-1")
    assert(loadBalancer.getExecutorLoadState("executor-1").isEmpty)
  }

  test("ShuffleLoadBalancer generates cluster load stats") {
    val loadBalancer = createLoadBalancer()

    // Add executors with different load levels
    loadBalancer.updateExecutorLoad(createLoadMetrics("executor-1", bytesInFlight = 100000L))
    loadBalancer.updateExecutorLoad(createLoadMetrics("executor-2", bytesInFlight = 500000L))
    loadBalancer.updateExecutorLoad(createLoadMetrics("executor-3", bytesInFlight = 900000L))

    val stats = loadBalancer.getClusterLoadStats
    assert(stats("activeExecutors") === 3.0)
    assert(stats("avgLoadScore") > 0.0)
    assert(stats("maxLoadScore") > stats("minLoadScore"))
  }

  test("ShuffleLoadBalancer handles stale load data") {
    val loadBalancer = createLoadBalancer()

    // Create metrics with old timestamp
    val staleMetrics = ShuffleLoadMetrics(
      executorId = "executor-1",
      bytesInFlight = 1024L,
      activeConnections = 5,
      networkCapacity = 1000000L,
      avgResponseTime = 100L,
      queueDepth = 3,
      timestamp = System.currentTimeMillis() - 60000L // 1 minute ago
    )

    loadBalancer.updateExecutorLoad(staleMetrics)

    // Should filter out stale data (older than 30 seconds by default)
    val loadState = loadBalancer.getExecutorLoadState("executor-1")
    assert(loadState.isEmpty)
  }

  test("ShuffleLoadBalancer generates config updates") {
    val loadBalancer = createLoadBalancer()

    // Add executors with different load levels
    loadBalancer.updateExecutorLoad(
      createLoadMetrics("executor-1", bytesInFlight = 100000L)) // low load
    loadBalancer.updateExecutorLoad(
      createLoadMetrics("executor-2", bytesInFlight = 800000L)) // high load

    val configUpdates = loadBalancer.generateConfigUpdates()
    assert(configUpdates.length === 2)

    val (lowLoadExecutor, lowLoadConfig) = configUpdates.find(_._1 == "executor-1").get
    val (highLoadExecutor, highLoadConfig) = configUpdates.find(_._1 == "executor-2").get

    // High load executor should get reduced limits
    assert(highLoadConfig.maxBlocksInFlightPerAddress === 3)
    // Low load executor should get normal/increased limits
    assert(lowLoadConfig.maxBlocksInFlightPerAddress === 5)
  }
}
