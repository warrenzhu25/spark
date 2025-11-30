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

import org.apache.spark.storage.MigrationInfo
import org.apache.spark.util.Utils

/**
 * Comprehensive summary of executor decommissioning including migration details and timing.
 *
 * @param message Human readable reason for the decommissioning
 * @param workerHost When defined, indicates the worker/host was decommissioned too.
 *                   Used to infer if shuffle data might be lost even with external shuffle service.
 * @param startTime Timestamp when decommissioning began (epoch millis)
 * @param endTime Optional timestamp when decommissioning completed (epoch millis)
 * @param migrationInfo Optional detailed migration statistics from BlockManagerDecommissioner
 * @param reason Optional structured reason code for the decommission.
 * @param details Optional structured parameters associated with the reason.
 */
private[spark]
case class DecommissionSummary(
    message: String,
    workerHost: Option[String] = None,
    startTime: Long = System.currentTimeMillis(),
    endTime: Option[Long] = None,
    migrationInfo: Option[MigrationInfo] = None,
    reason: Option[String] = None,
    details: Map[String, String] = Map.empty
) {
  /**
   * Duration of decommissioning in milliseconds, if completed
   */
  def duration: Option[Long] = endTime.map(_ - startTime)

  /**
   * Whether decommissioning has completed
   */
  def isComplete: Boolean = endTime.isDefined

  /**
   * Create a detailed message including timing and migration information
   */
  def toDetailedMessage: String = {
    val baseMsg = message
    val timing = duration.map(d => f" (${d/1000.0}%.1fs)").getOrElse("")
    val migration = migrationInfo.map(createMigrationSummary)
      .map(s => s" - Migration: $s").getOrElse("")
    s"$baseMsg$timing$migration"
  }

  /**
   * Create a concise migration summary from MigrationInfo
   */
  private def createMigrationSummary(info: MigrationInfo): String = {
    val stats = info.shuffleMigrationStat
    f"${stats.numMigratedBlock}/${stats.totalBlocks} blocks " +
    f"(${Utils.bytesToString(stats.totalMigratedSize)}/" +
    f"${Utils.bytesToString(stats.totalSize)}), ${stats.deletedBlocks} deleted"
  }

  /**
   * Mark decommissioning as completed with optional migration info
   */
  def markCompleted(migrationInfo: Option[MigrationInfo] = None): DecommissionSummary = {
    this.copy(
      endTime = Some(System.currentTimeMillis()),
      migrationInfo = migrationInfo.orElse(this.migrationInfo)
    )
  }

  /**
   * Convert to basic ExecutorDecommissionInfo for backward compatibility
   */
  def toExecutorDecommissionInfo: ExecutorDecommissionInfo =
    ExecutorDecommissionInfo(message, workerHost, reason, details, startTime)
}

/**
 * Companion object for creating common DecommissionSummary instances
 */
private[spark] object DecommissionSummary {
  def create(message: String, workerHost: Option[String] = None): DecommissionSummary = {
    DecommissionSummary(message, workerHost)
  }

  def createCompleted(message: String,
                     startTime: Long,
                     migrationInfo: Option[MigrationInfo] = None,
                     workerHost: Option[String] = None,
                     reason: Option[String] = None,
                     details: Map[String, String] = Map.empty): DecommissionSummary = {
    DecommissionSummary(
      message = message,
      workerHost = workerHost,
      startTime = startTime,
      endTime = Some(System.currentTimeMillis()),
      migrationInfo = migrationInfo,
      reason = reason,
      details = details
    )
  }

  /**
   * Create from existing ExecutorDecommissionInfo
   */
  def fromExecutorDecommissionInfo(info: ExecutorDecommissionInfo): DecommissionSummary =
    DecommissionSummary(
      message = info.message,
      workerHost = info.workerHost,
      startTime = info.timestamp,
      reason = info.reason,
      details = info.details)
}
