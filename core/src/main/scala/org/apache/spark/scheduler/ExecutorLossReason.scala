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

import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.util.Utils

/**
 * Migration information for decommissioning summary.
 */
private[spark]
case class MigrationInfo(
    blocksMigrated: Int,
    blocksTotal: Int,
    bytesToMigrate: Long,
    bytesMigrated: Long,
    blockType: String) {

  override def toString: String = {
    val blockProgress = if (blocksTotal > 0) {
      s"$blocksMigrated/$blocksTotal"
    } else "0/0"
    val bytesProgress = s"${Utils.bytesToString(bytesMigrated)}/" +
      s"${Utils.bytesToString(bytesToMigrate)}"
    s"$blockType blocks: $blockProgress ($bytesProgress)"
  }
}

/**
 * Comprehensive decommissioning summary with timing and migration details.
 * Includes all detailed statistics AND the 2 fields from lastMigrationInfo().
 */
private[spark]
case class DecommissionSummary(
    decommissionTime: Long,
    migrationTime: Long,
    taskWaitingTime: Long,
    shuffleMigrationInfo: MigrationInfo,
    rddMigrationInfo: MigrationInfo,
    // Fields from lastMigrationInfo() for compatibility
    lastMigrationTimestamp: Long,        // First field from lastMigrationInfo()
    migrationComplete: Boolean           // Second field from lastMigrationInfo()
) {

  override def toString: String = {
    val decommissionTimeStr = f"${decommissionTime / 1000.0}%.2f"
    val migrationTimeStr = f"${migrationTime / 1000.0}%.2f"
    val taskWaitingTimeStr = f"${taskWaitingTime / 1000.0}%.2f"
    s"Decommission completed in ${decommissionTimeStr}s " +
      s"(task waiting: ${taskWaitingTimeStr}s, migration: ${migrationTimeStr}s). " +
      s"Migration: ${shuffleMigrationInfo}, ${rddMigrationInfo}"
  }
}

/**
 * Represents an explanation for an executor or whole process failing or exiting.
 */
private[spark]
class ExecutorLossReason(val message: String) extends Serializable {
  override def toString: String = message
}

private[spark]
case class ExecutorExited(exitCode: Int, exitCausedByApp: Boolean, reason: String)
  extends ExecutorLossReason(reason)

private[spark] object ExecutorExited {
  def apply(exitCode: Int, exitCausedByApp: Boolean): ExecutorExited = {
    ExecutorExited(
      exitCode,
      exitCausedByApp,
      ExecutorExitCode.explainExitCode(exitCode))
  }
}

private[spark] object ExecutorLossMessage {
  val decommissionFinished = "Finished decommissioning"
}

private[spark] object ExecutorKilled extends ExecutorLossReason("Executor killed by driver.")

/**
 * A loss reason that means we don't yet know why the executor exited.
 *
 * This is used by the task scheduler to remove state associated with the executor, but
 * not yet fail any tasks that were running in the executor before the real loss reason
 * is known.
 */
private [spark] object LossReasonPending extends ExecutorLossReason("Pending loss reason.")

/**
 * @param _message human readable loss reason
 * @param workerHost it's defined when the host is confirmed lost too (i.e. including
 *                   shuffle service)
 * @param causedByApp whether the loss of the executor is the fault of the running app.
 *                    (assumed true by default unless known explicitly otherwise)
 */
private[spark]
case class ExecutorProcessLost(
    _message: String = "Executor Process Lost",
    workerHost: Option[String] = None,
    causedByApp: Boolean = true)
  extends ExecutorLossReason(_message)

/**
 * A loss reason that means the executor is marked for decommissioning.
 *
 * This is used by the task scheduler to remove state associated with the executor, but
 * not yet fail any tasks that were running in the executor before the executor is "fully" lost.
 * If you update this code make sure to re-run the K8s integration tests.
 *
 * @param workerHost it is defined when the worker is decommissioned too
 * @param reason detailed decommission message
 * @param migrationCompleted indicates if all shuffle and RDD blocks were successfully migrated
 * @param summary optional detailed migration summary for completed decommissions
 */
private [spark] case class ExecutorDecommission(
    workerHost: Option[String] = None,
    reason: String = "",
    migrationCompleted: Boolean = false,
    summary: Option[DecommissionSummary] = None)
  extends ExecutorLossReason(
    ExecutorDecommission.formatMessage(reason, migrationCompleted, summary))

private[spark] object ExecutorDecommission {
  val msgPrefix = "Executor decommission: "

  private def formatMessage(
      reason: String,
      migrationCompleted: Boolean,
      summary: Option[DecommissionSummary]): String = {
    val status = if (migrationCompleted) "completed" else "incomplete"
    val baseMsg = s"$msgPrefix$status: $reason"
    summary.map(s => s"$baseMsg. $s").getOrElse(baseMsg)
  }
}
