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

import org.apache.spark.internal.Logging

/**
 * Registry for server-side metrics providers to integrate with shuffle load balancing.
 * This bridges the gap between network layer (ChunkFetchRequestHandler) and
 * storage layer (ExecutorShuffleLoadCollector).
 */
private[spark] object ServerMetricsRegistry extends Logging {

  // Map from executor ID to server metrics provider function
  private val metricsProviders =
    new ConcurrentHashMap[String, () => (Long, Long, Long, Long, Double, Double, Int)]()

  // Map from executor ID to load collector for integration
  private val loadCollectors = new ConcurrentHashMap[String, ExecutorShuffleLoadCollector]()

  /**
   * Register a server metrics provider for an executor.
   */
  def registerServerMetricsProvider(
      executorId: String,
      provider: () => (Long, Long, Long, Long, Double, Double, Int)): Unit = {
    metricsProviders.put(executorId, provider)
    logInfo(s"Registered server metrics provider for executor $executorId")

    // If load collector is already registered, link them
    Option(loadCollectors.get(executorId)).foreach(_.setServerMetricsProvider(provider))
  }

  /**
   * Register a load collector for an executor.
   */
  def registerLoadCollector(executorId: String, collector: ExecutorShuffleLoadCollector): Unit = {
    loadCollectors.put(executorId, collector)
    logInfo(s"Registered load collector for executor $executorId")

    // If server metrics provider is already registered, link them
    Option(metricsProviders.get(executorId)).foreach(collector.setServerMetricsProvider)
  }

  /**
   * Unregister metrics for an executor (cleanup).
   */
  def unregisterExecutor(executorId: String): Unit = {
    metricsProviders.remove(executorId)
    loadCollectors.remove(executorId)
    logInfo(s"Unregistered metrics for executor $executorId")
  }

  /**
   * Get current registered executors (for testing/debugging).
   */
  def getRegisteredExecutors: Set[String] = {
    (metricsProviders.keySet().asScala ++ loadCollectors.keySet().asScala).toSet
  }

  /**
   * Clear all registrations (for testing).
   */
  private[storage] def clear(): Unit = {
    metricsProviders.clear()
    loadCollectors.clear()
  }
}