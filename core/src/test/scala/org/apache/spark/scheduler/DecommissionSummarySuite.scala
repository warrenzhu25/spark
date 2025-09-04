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

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.{MigrationInfo, MigrationStat}

class DecommissionSummarySuite extends SparkFunSuite {

  test("DecommissionSummary basic functionality") {
    val summary = DecommissionSummary.create("Test decommission", Some("host1"))

    assert(summary.message === "Test decommission")
    assert(summary.workerHost === Some("host1"))
    assert(summary.endTime.isEmpty)
    assert(!summary.isComplete)
    assert(summary.duration.isEmpty)
  }

  test("DecommissionSummary completion tracking") {
    val startTime = System.currentTimeMillis()
    val summary = DecommissionSummary("Test message", None, startTime)

    assert(!summary.isComplete)
    assert(summary.duration.isEmpty)

    Thread.sleep(10) // Ensure some time passes
    val completedSummary = summary.markCompleted()

    assert(completedSummary.isComplete)
    assert(completedSummary.duration.isDefined)
    assert(completedSummary.duration.get > 0)
    assert(completedSummary.endTime.get > startTime)
  }

  test("DecommissionSummary with migration info") {
    val migrationStat = MigrationStat(
      numBlocksLeft = 5,
      totalMigratedSize = 1024L,
      numMigratedBlock = 15,
      totalBlocks = 20,
      totalSize = 2048L,
      deletedBlocks = 3
    )
    val migrationInfo = MigrationInfo(System.nanoTime(), allBlocksMigrated = true, migrationStat)

    val summary = DecommissionSummary.create("Migration test")
    val completedSummary = summary.markCompleted(Some(migrationInfo))

    assert(completedSummary.migrationInfo.isDefined)
    assert(completedSummary.migrationInfo.get === migrationInfo)
  }

  test("DecommissionSummary detailed message without migration") {
    val startTime = System.currentTimeMillis()
    val summary = DecommissionSummary("Test message", None, startTime)

    Thread.sleep(100) // Ensure at least 100ms passes
    val completedSummary = summary.markCompleted()

    val detailedMessage = completedSummary.toDetailedMessage
    assert(detailedMessage.startsWith("Test message"))
    assert(detailedMessage.contains("0.1")) // Should show duration in seconds
    assert(!detailedMessage.contains("Migration:"))
  }

  test("DecommissionSummary detailed message with migration") {
    val migrationStat = MigrationStat(
      numBlocksLeft = 5,
      totalMigratedSize = 1536L, // 1.5KB
      numMigratedBlock = 15,
      totalBlocks = 20,
      totalSize = 2048L, // 2KB
      deletedBlocks = 3
    )
    val migrationInfo = MigrationInfo(System.nanoTime(), allBlocksMigrated = true, migrationStat)

    val startTime = System.currentTimeMillis()
    val summary = DecommissionSummary("Migration test", None, startTime)

    Thread.sleep(50)
    val completedSummary = summary.markCompleted(Some(migrationInfo))

    val detailedMessage = completedSummary.toDetailedMessage
    assert(detailedMessage.startsWith("Migration test"))
    assert(detailedMessage.contains("Migration:"))
    assert(detailedMessage.contains("15/20 blocks"))
    assert(detailedMessage.contains("1536.0 B/2.0 KiB"))
    assert(detailedMessage.contains("3 deleted"))
  }

  test("DecommissionSummary conversion to ExecutorDecommissionInfo") {
    val summary = DecommissionSummary.create("Test conversion", Some("worker1"))
    val execInfo = summary.toExecutorDecommissionInfo

    assert(execInfo.message === "Test conversion")
    assert(execInfo.workerHost === Some("worker1"))
  }

  test("DecommissionSummary.fromExecutorDecommissionInfo") {
    val execInfo = ExecutorDecommissionInfo("Original message", Some("host2"))
    val summary = DecommissionSummary.fromExecutorDecommissionInfo(execInfo)

    assert(summary.message === "Original message")
    assert(summary.workerHost === Some("host2"))
    assert(summary.migrationInfo.isEmpty)
    assert(!summary.isComplete)
  }

  test("DecommissionSummary.createCompleted") {
    val startTime = System.currentTimeMillis() - 1000 // 1 second ago
    val migrationStat = MigrationStat(0, 512L, 10, 10, 512L, 0)
    val migrationInfo = MigrationInfo(System.nanoTime(), allBlocksMigrated = true, migrationStat)

    val summary = DecommissionSummary.createCompleted(
      "Completed test",
      startTime,
      Some(migrationInfo),
      Some("testhost")
    )

    assert(summary.isComplete)
    assert(summary.startTime === startTime)
    assert(summary.migrationInfo.isDefined)
    assert(summary.workerHost === Some("testhost"))
    assert(summary.duration.isDefined)
    assert(summary.duration.get >= 1000) // At least 1 second
  }

  test("DecommissionSummary preserves existing migration info when marking completed") {
    val originalMigrationStat = MigrationStat(0, 256L, 5, 5, 256L, 0)
    val originalMigrationInfo = MigrationInfo(System.nanoTime(),
      allBlocksMigrated = true, originalMigrationStat)

    val summary = DecommissionSummary.create("Test")
      .copy(migrationInfo = Some(originalMigrationInfo))
    val completedSummary = summary.markCompleted() // No new migration info provided

    assert(completedSummary.migrationInfo.isDefined)
    assert(completedSummary.migrationInfo.get === originalMigrationInfo)
  }

  test("DecommissionSummary replaces migration info when marking completed with new info") {
    val originalMigrationStat = MigrationStat(0, 256L, 5, 5, 256L, 0)
    val originalMigrationInfo = MigrationInfo(System.nanoTime(),
      allBlocksMigrated = true, originalMigrationStat)

    val newMigrationStat = MigrationStat(0, 1024L, 20, 20, 1024L, 2)
    val newMigrationInfo = MigrationInfo(System.nanoTime(),
      allBlocksMigrated = true, newMigrationStat)

    val summary = DecommissionSummary.create("Test")
      .copy(migrationInfo = Some(originalMigrationInfo))
    val completedSummary = summary.markCompleted(Some(newMigrationInfo))

    assert(completedSummary.migrationInfo.isDefined)
    assert(completedSummary.migrationInfo.get === newMigrationInfo)
    assert(completedSummary.migrationInfo.get !== originalMigrationInfo)
  }

  test("ExecutorDecommission loss reason with DecommissionSummary") {
    val migrationStat = MigrationStat(2, 800L, 8, 10, 1000L, 1)
    val migrationInfo = MigrationInfo(System.nanoTime(), allBlocksMigrated = true, migrationStat)

    val summary = DecommissionSummary.create("Loss reason test", Some("losshost"))
      .markCompleted(Some(migrationInfo))

    val lossReason = ExecutorDecommission(Some("losshost"), "fallback", Some(summary))

    // The message should use the detailed summary message
    val expectedMessage = summary.toDetailedMessage
    assert(lossReason.message === expectedMessage)

    // Verify the detailed message contains expected elements
    assert(lossReason.message.contains("Loss reason test"))
    assert(lossReason.message.contains("Migration:"))
    assert(lossReason.message.contains("8/10 blocks"))
  }

  test("ExecutorDecommission loss reason without DecommissionSummary " +
    "falls back to prefix + reason") {
    val lossReason = ExecutorDecommission(Some("fallbackhost"), "fallback reason", None)

    assert(lossReason.message === "Executor decommission: fallback reason")
  }
}
