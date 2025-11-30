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

package org.apache.spark.scheduler

import org.apache.spark.util.Utils

/**
 * Message providing more detail when an executor is being decommissioned.
 * @param message Human readable reason for why the decommissioning is happening.
 * @param workerHost When workerHost is defined, it means the host (aka the `node` or `worker`
 *                in other places) has been decommissioned too. Used to infer if the
 *                shuffle data might be lost even if the external shuffle service is enabled.
 * @param reason Optional structured reason code (e.g. idle timeout, shuffle timeout, manual).
 * @param details Optional structured parameters relevant to the reason (e.g. idle duration).
 * @param timestamp Epoch millis when the decision to decommission was made.
 */
private[spark]
case class ExecutorDecommissionReason(
    message: String,
    workerHost: Option[String] = None,
    reason: Option[String] = None,
    details: Map[String, String] = Map.empty,
    timestamp: Long = System.currentTimeMillis())

private[spark] object ExecutorDecommissionReason {
  val IDLE_TIMEOUT_REASON = "idle_timeout"
  val SHUFFLE_TIMEOUT_REASON = "shuffle_timeout"
  val STORAGE_TIMEOUT_REASON = "storage_timeout"

  private def fmt(reason: String,
      idleMs: Long,
      timeoutMs: Long,
      rpId: Int,
      shuffles: Int,
      cachedBlocks: Int,
      timeoutConfigSuffix: Option[String] = None): String = {
    val idleStr = Utils.msDurationToString(idleMs)
    val timeoutStr = Utils.msDurationToString(timeoutMs)
    val configPart = timeoutConfigSuffix
      .map(k => s", $k=$timeoutStr")
      .getOrElse("")
    val timeoutLabel = timeoutConfigSuffix.map(_ => "").getOrElse(s", timeout=$timeoutStr")
    s"$reason: idle=$idleStr$timeoutLabel, rp=$rpId, shuffles=$shuffles, cachedBlocks=$cachedBlocks$configPart"
  }

  def idleTimeout(
      idleMs: Long,
      timeoutMs: Long,
      resourceProfileId: Int,
      shuffleCount: Int,
      cachedBlocksCount: Int,
      hasShuffleData: Boolean,
      hasCachedBlocks: Boolean,
      timeoutConfigSuffix: Option[String] = None): ExecutorDecommissionReason = {
    val msg = fmt("Idle timeout", idleMs, timeoutMs, resourceProfileId, shuffleCount,
      cachedBlocksCount, timeoutConfigSuffix)
    ExecutorDecommissionReason(
      message = msg,
      workerHost = None,
      reason = Some(IDLE_TIMEOUT_REASON),
      details = Map(
        "idleDurationMs" -> idleMs.toString,
        "timeoutMs" -> timeoutMs.toString,
        "resourceProfileId" -> resourceProfileId.toString,
        "shuffleCount" -> shuffleCount.toString,
        "cachedBlocksCount" -> cachedBlocksCount.toString,
        "hasShuffleData" -> hasShuffleData.toString,
        "hasCachedBlocks" -> hasCachedBlocks.toString) ++
        timeoutConfigSuffix.map(suffix => Map("timeoutConfig" -> s"$suffix=$timeoutStr")).getOrElse(Map.empty))
  }

  def shuffleTimeout(
      idleMs: Long,
      timeoutMs: Long,
      resourceProfileId: Int,
      shuffleIds: Int,
      cachedBlocksCount: Int,
      timeoutConfigSuffix: Option[String] = None): ExecutorDecommissionReason = {
    val msg = fmt("Shuffle timeout", idleMs, timeoutMs, resourceProfileId, shuffleIds,
      cachedBlocksCount, timeoutConfigSuffix)
    ExecutorDecommissionReason(
      message = msg,
      workerHost = None,
      reason = Some(SHUFFLE_TIMEOUT_REASON),
      details = Map(
        "idleDurationMs" -> idleMs.toString,
        "timeoutMs" -> timeoutMs.toString,
        "resourceProfileId" -> resourceProfileId.toString,
        "shuffleIds" -> shuffleIds.toString,
        "cachedBlocksCount" -> cachedBlocksCount.toString) ++
        timeoutConfigSuffix.map(suffix => Map("timeoutConfig" -> s"$suffix=$timeoutStr")).getOrElse(Map.empty))
  }

  def storageTimeout(
      idleMs: Long,
      timeoutMs: Long,
      resourceProfileId: Int,
      cachedBlocksCount: Int,
      timeoutConfigSuffix: Option[String] = None): ExecutorDecommissionReason = {
    val msg = fmt("Storage timeout", idleMs, timeoutMs, resourceProfileId, shuffles = 0,
      cachedBlocks = cachedBlocksCount, timeoutConfigSuffix)
    ExecutorDecommissionReason(
      message = msg,
      workerHost = None,
      reason = Some(STORAGE_TIMEOUT_REASON),
      details = Map(
        "idleDurationMs" -> idleMs.toString,
        "timeoutMs" -> timeoutMs.toString,
        "resourceProfileId" -> resourceProfileId.toString,
        "cachedBlocksCount" -> cachedBlocksCount.toString,
        "hasCachedBlocks" -> "true") ++
        timeoutConfigSuffix.map(suffix => Map("timeoutConfig" -> s"$suffix=$timeoutStr")).getOrElse(Map.empty))
  }
}

/**
 * State related to decommissioning that is kept by the TaskSchedulerImpl. This state is derived
 * from the info message above but it is kept distinct to allow the state to evolve independently
 * from the message.
 */
private[scheduler] case class ExecutorDecommissionState(
    // Timestamp the decommissioning commenced as per the Driver's clock,
    // to estimate when the executor might eventually be lost if EXECUTOR_DECOMMISSION_KILL_INTERVAL
    // is configured.
    startTime: Long,
    workerHost: Option[String] = None)
