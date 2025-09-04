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
 */
private [spark] case class ExecutorDecommission(
    workerHost: Option[String] = None,
    reason: String = "")
  extends ExecutorLossReason(ExecutorDecommission.msgPrefix + reason)

private[spark] object ExecutorDecommission {
  val msgPrefix = "Executor decommission: "
}

/**
 * A loss reason that means the executor has finished decommissioning and exited gracefully.
 *
 * This is distinct from ExecutorDecommission which indicates an executor is in the process
 * of being decommissioned. ExecutorDecommissionFinished indicates the decommissioning
 * process has completed successfully.
 *
 * @param workerHost it is defined when the worker was decommissioned too
 * @param reason basic decommission completion message
 * @param summary optional comprehensive decommission summary with timing and migration details
 */
private [spark] case class ExecutorDecommissionFinished(
    workerHost: Option[String] = None,
    reason: String = ExecutorLossMessage.decommissionFinished,
    summary: Option[DecommissionSummary] = None)
  extends ExecutorLossReason(
    ExecutorDecommissionFinished.msgPrefix +
    summary.map(_.toDetailedMessage).getOrElse(reason))

private[spark] object ExecutorDecommissionFinished {
  val msgPrefix = "Executor decommission finished: "

  def apply(reason: String): ExecutorDecommissionFinished =
    ExecutorDecommissionFinished(None, reason, None)

  def apply(summary: DecommissionSummary): ExecutorDecommissionFinished =
    ExecutorDecommissionFinished(summary.workerHost, summary.message, Some(summary))
}
