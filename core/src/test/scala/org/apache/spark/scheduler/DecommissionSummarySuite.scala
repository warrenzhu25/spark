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

import scala.concurrent.duration.Duration

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.{MigrationInfo, MigrationStat}

class DecommissionSummarySuite extends SparkFunSuite {

  test("DecommissionSummary creation with all parameters") {
    val migrationStat = MigrationStat(2, 1024L, 10)
    val migrationInfo = MigrationInfo(1000L, true, migrationStat)
    val summary = DecommissionSummary(
      decommissionTime = Duration.fromNanos(1000000000L), // 1 second
      migrationTime = Duration.fromNanos(500000000L), // 0.5 seconds
      taskWaitingTime = Duration.fromNanos(100000000L), // 0.1 seconds
      migrationInfo = migrationInfo
    )

    assert(summary.decommissionTime.toMillis === 1000)
    assert(summary.migrationTime.toMillis === 500)
    assert(summary.taskWaitingTime.toMillis === 100)
    assert(summary.migrationInfo === migrationInfo)
  }

  test("DecommissionSummary toString formatting") {
    val migrationStat = MigrationStat(2, 1024L, 10)
    val migrationInfo = MigrationInfo(1000L, true, migrationStat)
    val summary = DecommissionSummary(
      decommissionTime = Duration.fromNanos(1000000000L),
      migrationTime = Duration.fromNanos(500000000L),
      taskWaitingTime = Duration.fromNanos(100000000L),
      migrationInfo = migrationInfo
    )

    val expectedString = "Decommission finished in 1,000 ms. " +
      "Migration finished in 500 ms " +
      "(including 100 ms running task waiting time). " +
      "10 blocks of size 1024.0 B migrated, " +
      "2 blocks not migrated."

    assert(summary.toString === expectedString)
  }

  test("DecommissionSummary toString with zero times") {
    val migrationStat = MigrationStat(0, 0L, 0)
    val migrationInfo = MigrationInfo(0L, true, migrationStat)
    val summary = DecommissionSummary(
      decommissionTime = Duration.fromNanos(0L),
      migrationTime = Duration.fromNanos(0L),
      taskWaitingTime = Duration.fromNanos(0L),
      migrationInfo = migrationInfo
    )

    val result = summary.toString
    assert(result.contains("0 ms"))
    assert(result.contains("0 blocks of size 0.0 B migrated"))
    assert(result.contains("0 blocks not migrated"))
  }

  test("DecommissionSummary toString with large values") {
    val migrationStat = MigrationStat(5, 2048000000L, 100) // 2GB migrated
    val migrationInfo = MigrationInfo(5000L, false, migrationStat)
    val summary = DecommissionSummary(
      decommissionTime = Duration.fromNanos(10000000000L), // 10 seconds
      migrationTime = Duration.fromNanos(8000000000L), // 8 seconds
      taskWaitingTime = Duration.fromNanos(1000000000L), // 1 second
      migrationInfo = migrationInfo
    )

    val result = summary.toString
    assert(result.contains("10,000 ms"))
    assert(result.contains("8,000 ms"))
    assert(result.contains("1,000 ms"))
    assert(result.contains("100 blocks"))
    assert(result.contains("2.0 GB"))
    assert(result.contains("5 blocks not migrated"))
  }

  test("DecommissionSummary case class equality") {
    val migrationStat1 = MigrationStat(1, 500L, 5)
    val migrationStat2 = MigrationStat(1, 500L, 5)
    val migrationInfo1 = MigrationInfo(1000L, true, migrationStat1)
    val migrationInfo2 = MigrationInfo(1000L, true, migrationStat2)

    val summary1 = DecommissionSummary(
      Duration.fromNanos(1000000000L),
      Duration.fromNanos(500000000L),
      Duration.fromNanos(100000000L),
      migrationInfo1
    )
    val summary2 = DecommissionSummary(
      Duration.fromNanos(1000000000L),
      Duration.fromNanos(500000000L),
      Duration.fromNanos(100000000L),
      migrationInfo2
    )

    assert(summary1 === summary2)
  }

  test("DecommissionSummary with partial migration") {
    val migrationStat = MigrationStat(10, 1000000L, 5) // 5 migrated, 10 remaining
    val migrationInfo = MigrationInfo(2000L, false, migrationStat)
    val summary = DecommissionSummary(
      decommissionTime = Duration.fromNanos(5000000000L),
      migrationTime = Duration.fromNanos(3000000000L),
      taskWaitingTime = Duration.fromNanos(500000000L),
      migrationInfo = migrationInfo
    )

    val result = summary.toString
    assert(result.contains("5 blocks of size 976.6 KB migrated"))
    assert(result.contains("10 blocks not migrated"))
    assert(!migrationInfo.allBlocksMigrated)
  }

  test("DecommissionSummary serialization") {
    val migrationStat = MigrationStat(1, 1024L, 3)
    val migrationInfo = MigrationInfo(1500L, true, migrationStat)
    val summary = DecommissionSummary(
      Duration.fromNanos(2000000000L),
      Duration.fromNanos(1500000000L),
      Duration.fromNanos(200000000L),
      migrationInfo
    )

    // Test that the case class can be serialized (basic test)
    assert(summary.productArity === 4)
    assert(summary.productElement(0) === Duration.fromNanos(2000000000L))
    assert(summary.productElement(1) === Duration.fromNanos(1500000000L))
    assert(summary.productElement(2) === Duration.fromNanos(200000000L))
    assert(summary.productElement(3) === migrationInfo)
  }
}
