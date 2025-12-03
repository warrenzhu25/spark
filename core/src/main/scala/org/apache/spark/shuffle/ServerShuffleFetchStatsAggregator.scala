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

package org.apache.spark.shuffle

import scala.collection.mutable

/**
 * Aggregates server-side shuffle fetch statistics from executors and maintains
 * top-K slowest executors for reporting after stage completion.
 *
 * Thread-safe for concurrent heartbeat updates.
 *
 * @param topK Number of slowest executors to track
 */
private[spark] class ServerShuffleFetchStatsAggregator(topK: Int = 3) {

  private val statsMap = new mutable.HashMap[String, ServerShuffleFetchAggregate]()

  /**
   * Merge incoming stats from an executor heartbeat.
   * Aggregates with existing stats for the same executor.
   */
  def mergeStats(stats: ServerShuffleFetchStats): Unit = synchronized {
    val aggregate = ServerShuffleFetchAggregate.fromStats(stats)
    statsMap.get(stats.executorId) match {
      case Some(existing) => statsMap(stats.executorId) = existing.merge(aggregate)
      case None => statsMap(stats.executorId) = aggregate
    }
  }

  /**
   * Get top-K executors with highest average shuffle fetch latency.
   * Returns executors sorted by total latency (descending).
   */
  def getTopKSlowestExecutors: Seq[ServerShuffleFetchAggregate] = synchronized {
    statsMap.values
      .filter(_.totalRequests > 0)
      .toSeq
      .sortBy(-_.totalLatencySum)
      .take(topK)
  }

  /**
   * Get all executor statistics.
   */
  def getAllStats: Map[String, ServerShuffleFetchAggregate] = synchronized {
    statsMap.toMap
  }

  /**
   * Reset all accumulated statistics.
   * Called after stage completion to prepare for next stage.
   */
  def reset(): Unit = synchronized {
    statsMap.clear()
  }

  /**
   * Remove metrics associated with an executor that has been lost.
   */
  def removeExecutor(executorId: String): Unit = synchronized {
    statsMap.remove(executorId)
  }

  /**
   * Get statistics for a specific executor.
   */
  def getStatsForExecutor(executorId: String): Option[ServerShuffleFetchAggregate] =
    synchronized {
      statsMap.get(executorId)
    }
}
