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

import scala.concurrent.duration._

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.{MigrationInfo, MigrationStat}

class ExecutorLossMessageSuite extends SparkFunSuite {

  test("ExecutorLossMessage contains standard decommission message") {
    assert(ExecutorLossMessage.decommissionFinished === "Finished decommissioning")
  }

  test("ExecutorLossMessage decommission message is non-empty") {
    assert(ExecutorLossMessage.decommissionFinished.nonEmpty)
    assert(ExecutorLossMessage.decommissionFinished.length > 0)
  }

  test("ExecutorLossMessage can be concatenated with additional info") {
    val additionalInfo = " - 5 blocks migrated"
    val fullMessage = ExecutorLossMessage.decommissionFinished + additionalInfo

    assert(fullMessage === "Finished decommissioning - 5 blocks migrated")
    assert(fullMessage.startsWith(ExecutorLossMessage.decommissionFinished))
  }

  test("ExecutorLossMessage format is consistent") {
    val message = ExecutorLossMessage.decommissionFinished

    // Verify standard message format
    assert(message.startsWith("Finished"))
    assert(message.contains("decommissioning"))
    assert(!message.endsWith(".")) // Should not end with punctuation for concatenation
  }

  test("ExecutorDecommissionFinished creation and message formatting") {
    val migrationStat = MigrationStat(
      numBlocksLeft = 2,
      totalMigratedSize = 1024000L,
      numMigratedBlock = 10
    )
    val migrationInfo = MigrationInfo(
      lastMigrationTime = System.currentTimeMillis(),
      allBlocksMigrated = true,
      shuffleMigrationStat = migrationStat
    )
    val summary = DecommissionSummary(
      decommissionTime = 5000.milliseconds,
      migrationTime = 3000.milliseconds,
      taskWaitingTime = 1000.milliseconds,
      migrationInfo = migrationInfo
    )

    val lossReason = ExecutorDecommissionFinished(summary)

    assert(lossReason.message.startsWith(ExecutorLossMessage.decommissionFinished))
    assert(lossReason.message.contains("5,000 ms"))
    assert(lossReason.message.contains("3,000 ms"))
    assert(lossReason.message.contains("1,000 ms"))
    assert(lossReason.message.contains("10 blocks"))
    assert(lossReason.message.contains("1024000"))
    assert(lossReason.message.contains("2 blocks not migrated"))
  }

  test("ExecutorDecommissionFinished toString consistency") {
    val migrationStat = MigrationStat(0, 2048000L, 5)
    val migrationInfo = MigrationInfo(1000L, true, migrationStat)
    val summary = DecommissionSummary(
      decommissionTime = 2000.milliseconds,
      migrationTime = 1500.milliseconds,
      taskWaitingTime = 500.milliseconds,
      migrationInfo = migrationInfo
    )

    val lossReason = ExecutorDecommissionFinished(summary)

    // toString should be the same as message
    assert(lossReason.toString === lossReason.message)
    assert(lossReason.message.contains(": Decommission finished"))
  }

  test("ExecutorDecommissionFinished with no migrated blocks") {
    val migrationStat = MigrationStat(0, 0L, 0)
    val migrationInfo = MigrationInfo(1000L, true, migrationStat)
    val summary = DecommissionSummary(
      decommissionTime = 1000.milliseconds,
      migrationTime = 500.milliseconds,
      taskWaitingTime = 200.milliseconds,
      migrationInfo = migrationInfo
    )

    val lossReason = ExecutorDecommissionFinished(summary)

    assert(lossReason.message.contains("0 blocks of size 0.0 B migrated"))
    assert(lossReason.message.contains("0 blocks not migrated"))
  }

  test("ExecutorDecommissionFinished extends ExecutorLossReason") {
    val migrationStat = MigrationStat(1, 1024L, 3)
    val migrationInfo = MigrationInfo(1000L, false, migrationStat)
    val summary = DecommissionSummary(
      decommissionTime = 1000.milliseconds,
      migrationTime = 800.milliseconds,
      taskWaitingTime = 200.milliseconds,
      migrationInfo = migrationInfo
    )

    val lossReason = ExecutorDecommissionFinished(summary)

    // Should be an instance of ExecutorLossReason
    assert(lossReason.isInstanceOf[ExecutorLossReason])

    // Should have access to base class properties
    assert(lossReason.message.nonEmpty)
  }

  test("ExecutorDecommission vs ExecutorDecommissionFinished distinction") {
    // ExecutorDecommission is used when driver is removing an executor for decommissioning
    val executorDecommission = ExecutorDecommission(Some("worker1"), "Scaling down")
    assert(executorDecommission.message.startsWith("Executor decommission: "))
    assert(executorDecommission.message.contains("Scaling down"))
    assert(executorDecommission.workerHost === Some("worker1"))

    // ExecutorDecommissionFinished is used when executor reports completion of decommissioning
    val migrationStat = MigrationStat(0, 1024000L, 5)
    val migrationInfo = MigrationInfo(1000L, true, migrationStat)
    val summary = DecommissionSummary(
      decommissionTime = 3000.milliseconds,
      migrationTime = 2000.milliseconds,
      taskWaitingTime = 500.milliseconds,
      migrationInfo = migrationInfo
    )
    val executorDecommissionFinished = ExecutorDecommissionFinished(summary)

    assert(executorDecommissionFinished.message.startsWith("Finished decommissioning"))
    assert(executorDecommissionFinished.message.contains("3,000 ms"))
    assert(executorDecommissionFinished.message.contains("5 blocks"))

    // These should be different types and have different message content
    assert(executorDecommission.getClass != executorDecommissionFinished.getClass)
    assert(executorDecommission.message != executorDecommissionFinished.message)
  }

  test("ExecutorDecommission message prefix consistency") {
    val decommission1 = ExecutorDecommission(None, "Test reason")
    val decommission2 = ExecutorDecommission(Some("host"), "Another reason")

    assert(decommission1.message.startsWith(ExecutorDecommission.msgPrefix))
    assert(decommission2.message.startsWith(ExecutorDecommission.msgPrefix))
    assert(decommission1.message.contains("Test reason"))
    assert(decommission2.message.contains("Another reason"))
  }
}
