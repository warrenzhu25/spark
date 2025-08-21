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

import org.apache.spark.{Heartbeat, SparkFunSuite}
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.storage.BlockManagerMessages._

class ShuffleLoadBalancingMessagesSuite extends SparkFunSuite {

  test("ShuffleLoadMetrics message creation") {
    val metrics = ShuffleLoadMetrics(
      executorId = "executor-1",
      bytesInFlight = 1024L,
      activeConnections = 5,
      networkCapacity = 1000000L,
      avgResponseTime = 100L,
      queueDepth = 3,
      timestamp = System.currentTimeMillis()
    )

    assert(metrics.executorId === "executor-1")
    assert(metrics.bytesInFlight === 1024L)
    assert(metrics.activeConnections === 5)
    assert(metrics.networkCapacity === 1000000L)
    assert(metrics.avgResponseTime === 100L)
    assert(metrics.queueDepth === 3)
  }

  test("ShuffleFetchCompleted message creation") {
    val completed = ShuffleFetchCompleted(
      executorId = "executor-1",
      bytesTransferred = 2048L,
      duration = 500L,
      sourceExecutors = Set("executor-2", "executor-3")
    )

    assert(completed.executorId === "executor-1")
    assert(completed.bytesTransferred === 2048L)
    assert(completed.duration === 500L)
    assert(completed.sourceExecutors === Set("executor-2", "executor-3"))
  }

  test("ShuffleFetchDirective message creation") {
    val directive = ShuffleFetchDirective(
      targetExecutor = "executor-1",
      preferredSources = Seq("executor-2", "executor-3"),
      maxRequestSize = 1048576L,
      throttleDelay = 10,
      priority = 1
    )

    assert(directive.targetExecutor === "executor-1")
    assert(directive.preferredSources === Seq("executor-2", "executor-3"))
    assert(directive.maxRequestSize === 1048576L)
    assert(directive.throttleDelay === 10)
    assert(directive.priority === 1)
  }

  test("ShuffleConfigUpdate message creation") {
    val configUpdate = ShuffleConfigUpdate(
      maxBytesInFlight = 48 * 1024 * 1024L,
      maxBlocksInFlightPerAddress = 5,
      targetRequestSize = 1024 * 1024L
    )

    assert(configUpdate.maxBytesInFlight === 48 * 1024 * 1024L)
    assert(configUpdate.maxBlocksInFlightPerAddress === 5)
    assert(configUpdate.targetRequestSize === 1024 * 1024L)
  }

  test("Heartbeat with shuffle load metrics") {
    val shuffleMetrics = ShuffleLoadMetrics(
      executorId = "executor-1",
      bytesInFlight = 1024L,
      activeConnections = 5,
      networkCapacity = 1000000L,
      avgResponseTime = 100L,
      queueDepth = 3,
      timestamp = System.currentTimeMillis()
    )

    val blockManagerId = BlockManagerId("executor-1", "localhost", 7077)
    val heartbeat = Heartbeat(
      executorId = "executor-1",
      accumUpdates = Array.empty,
      blockManagerId = blockManagerId,
      executorUpdates = Map.empty,
      shuffleLoadMetrics = Some(shuffleMetrics)
    )

    assert(heartbeat.executorId === "executor-1")
    assert(heartbeat.shuffleLoadMetrics.isDefined)
    assert(heartbeat.shuffleLoadMetrics.get.bytesInFlight === 1024L)
  }

  test("Heartbeat without shuffle load metrics") {
    val blockManagerId = BlockManagerId("executor-1", "localhost", 7077)
    val heartbeat = Heartbeat(
      executorId = "executor-1",
      accumUpdates = Array.empty,
      blockManagerId = blockManagerId,
      executorUpdates = Map.empty
    )

    assert(heartbeat.executorId === "executor-1")
    assert(heartbeat.shuffleLoadMetrics.isEmpty)
  }
}