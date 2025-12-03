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

/**
 * Server-side shuffle fetch statistics sent from executor to driver via heartbeat.
 * Contains aggregate metrics about shuffle fetch request processing on the server side.
 *
 * @param executorId Executor ID serving shuffle fetch requests
 * @param requestCount Total number of chunk fetch requests processed
 * @param totalLatencySum Sum of total processing latency (ms)
 * @param totalLatencyMax Maximum total processing latency (ms)
 * @param diskReadLatencySum Sum of disk I/O latency (ms)
 * @param diskReadLatencyMax Maximum disk I/O latency (ms)
 * @param sendLatencySum Sum of network send latency (ms)
 * @param sendLatencyMax Maximum network send latency (ms)
 * @param queueWaitTimeSum Sum of queue wait time (ms)
 * @param queueWaitTimeMax Maximum queue wait time (ms)
 * @param maxQueueLength Peak number of requests waiting in queue
 * @param avgQueueLength Average number of requests waiting in queue
 */
case class ServerShuffleFetchStats(
    executorId: String,
    requestCount: Long,
    totalLatencySum: Long,
    totalLatencyMax: Long,
    diskReadLatencySum: Long,
    diskReadLatencyMax: Long,
    sendLatencySum: Long,
    sendLatencyMax: Long,
    queueWaitTimeSum: Long,
    queueWaitTimeMax: Long,
    maxQueueLength: Int,
    avgQueueLength: Double)

/**
 * Aggregated server-side shuffle fetch statistics with derived metrics.
 * Used for reporting top-K slow executors after stage completion.
 */
case class ServerShuffleFetchAggregate(
    executorId: String,
    totalRequests: Long,
    totalLatencySum: Long,
    totalLatencyMax: Long,
    diskReadLatencySum: Long,
    diskReadLatencyMax: Long,
    sendLatencySum: Long,
    sendLatencyMax: Long,
    queueWaitTimeSum: Long,
    queueWaitTimeMax: Long,
    maxQueueLength: Int,
    avgQueueLength: Double) {

  def avgLatency: Double =
    if (totalRequests > 0) totalLatencySum.toDouble / totalRequests else 0.0

  def avgDiskReadLatency: Double =
    if (totalRequests > 0) diskReadLatencySum.toDouble / totalRequests else 0.0

  def avgSendLatency: Double =
    if (totalRequests > 0) sendLatencySum.toDouble / totalRequests else 0.0

  def avgQueueWaitTime: Double =
    if (totalRequests > 0) queueWaitTimeSum.toDouble / totalRequests else 0.0

  def diskReadPercent: Double =
    if (totalLatencySum > 0) (diskReadLatencySum.toDouble / totalLatencySum) * 100 else 0.0

  def sendPercent: Double =
    if (totalLatencySum > 0) (sendLatencySum.toDouble / totalLatencySum) * 100 else 0.0

  def queueWaitPercent: Double =
    if (totalLatencySum > 0) (queueWaitTimeSum.toDouble / totalLatencySum) * 100 else 0.0

  /** Validation: queue wait time ~= queue length * avg processing time */
  def expectedQueueWaitTime: Double = avgQueueLength * avgLatency

  /** Merge with another aggregate for the same executor */
  def merge(other: ServerShuffleFetchAggregate): ServerShuffleFetchAggregate = {
    require(executorId == other.executorId, "Cannot merge stats from different executors")
    ServerShuffleFetchAggregate(
      executorId = executorId,
      totalRequests = totalRequests + other.totalRequests,
      totalLatencySum = totalLatencySum + other.totalLatencySum,
      totalLatencyMax = math.max(totalLatencyMax, other.totalLatencyMax),
      diskReadLatencySum = diskReadLatencySum + other.diskReadLatencySum,
      diskReadLatencyMax = math.max(diskReadLatencyMax, other.diskReadLatencyMax),
      sendLatencySum = sendLatencySum + other.sendLatencySum,
      sendLatencyMax = math.max(sendLatencyMax, other.sendLatencyMax),
      queueWaitTimeSum = queueWaitTimeSum + other.queueWaitTimeSum,
      queueWaitTimeMax = math.max(queueWaitTimeMax, other.queueWaitTimeMax),
      maxQueueLength = math.max(maxQueueLength, other.maxQueueLength),
      avgQueueLength = {
        val totalCount = totalRequests + other.totalRequests
        if (totalCount > 0) {
          (avgQueueLength * totalRequests + other.avgQueueLength * other.totalRequests) /
            totalCount
        } else {
          0.0
        }
      }
    )
  }
}

object ServerShuffleFetchAggregate {
  /** Create aggregate from a single stats snapshot */
  def fromStats(stats: ServerShuffleFetchStats): ServerShuffleFetchAggregate = {
    ServerShuffleFetchAggregate(
      executorId = stats.executorId,
      totalRequests = stats.requestCount,
      totalLatencySum = stats.totalLatencySum,
      totalLatencyMax = stats.totalLatencyMax,
      diskReadLatencySum = stats.diskReadLatencySum,
      diskReadLatencyMax = stats.diskReadLatencyMax,
      sendLatencySum = stats.sendLatencySum,
      sendLatencyMax = stats.sendLatencyMax,
      queueWaitTimeSum = stats.queueWaitTimeSum,
      queueWaitTimeMax = stats.queueWaitTimeMax,
      maxQueueLength = stats.maxQueueLength,
      avgQueueLength = stats.avgQueueLength
    )
  }
}
