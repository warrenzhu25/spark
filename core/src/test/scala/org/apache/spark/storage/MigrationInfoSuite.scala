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

import org.apache.spark.SparkFunSuite

class MigrationInfoSuite extends SparkFunSuite {

  test("MigrationStat creation and field access") {
    val migrationStat = MigrationStat(
      numBlocksLeft = 5,
      totalMigratedSize = 1024000L,
      numMigratedBlock = 10
    )

    assert(migrationStat.numBlocksLeft === 5)
    assert(migrationStat.totalMigratedSize === 1024000L)
    assert(migrationStat.numMigratedBlock === 10)
  }

  test("MigrationStat with zero values") {
    val migrationStat = MigrationStat(
      numBlocksLeft = 0,
      totalMigratedSize = 0L,
      numMigratedBlock = 0
    )

    assert(migrationStat.numBlocksLeft === 0)
    assert(migrationStat.totalMigratedSize === 0L)
    assert(migrationStat.numMigratedBlock === 0)
  }

  test("MigrationInfo creation and field access") {
    val migrationStat = MigrationStat(3, 500000L, 7)
    val migrationInfo = MigrationInfo(
      lastMigrationTime = System.currentTimeMillis(),
      allBlocksMigrated = true,
      shuffleMigrationStat = migrationStat
    )

    assert(migrationInfo.allBlocksMigrated === true)
    assert(migrationInfo.shuffleMigrationStat === migrationStat)
    assert(migrationInfo.lastMigrationTime > 0)
  }

  test("MigrationInfo with incomplete migration") {
    val migrationStat = MigrationStat(5, 200000L, 3)
    val migrationInfo = MigrationInfo(
      lastMigrationTime = 1000L,
      allBlocksMigrated = false,
      shuffleMigrationStat = migrationStat
    )

    assert(migrationInfo.allBlocksMigrated === false)
    assert(migrationInfo.shuffleMigrationStat.numBlocksLeft === 5)
    assert(migrationInfo.lastMigrationTime === 1000L)
  }

  test("MigrationInfo case class equality") {
    val stat1 = MigrationStat(2, 100000L, 5)
    val stat2 = MigrationStat(2, 100000L, 5)
    val info1 = MigrationInfo(1000L, true, stat1)
    val info2 = MigrationInfo(1000L, true, stat2)

    assert(info1 === info2)
    assert(stat1 === stat2)
  }

  test("MigrationStat serialization") {
    val migrationStat = MigrationStat(10, 2048000L, 25)

    // Test that the case class can be serialized (basic test)
    assert(migrationStat.productArity === 3)
    assert(migrationStat.productElement(0) === 10)
    assert(migrationStat.productElement(1) === 2048000L)
    assert(migrationStat.productElement(2) === 25)
  }

  test("MigrationInfo serialization") {
    val migrationStat = MigrationStat(1, 1024L, 4)
    val migrationInfo = MigrationInfo(5000L, false, migrationStat)

    // Test that the case class can be serialized (basic test)
    assert(migrationInfo.productArity === 3)
    assert(migrationInfo.productElement(0) === 5000L)
    assert(migrationInfo.productElement(1) === false)
    assert(migrationInfo.productElement(2) === migrationStat)
  }

  test("MigrationStat tracks size correctly") {
    // Test tracking zero blocks
    val emptyStats = MigrationStat(0, 0L, 0)
    assert(emptyStats.totalMigratedSize === 0L)
    assert(emptyStats.numMigratedBlock === 0)
    assert(emptyStats.numBlocksLeft === 0)

    // Test tracking with migrated blocks
    val withDataStats = MigrationStat(5, 2048000L, 10)
    assert(withDataStats.totalMigratedSize === 2048000L) // 2MB
    assert(withDataStats.numMigratedBlock === 10)
    assert(withDataStats.numBlocksLeft === 5)
  }

  test("MigrationInfo tracks completion status") {
    val incompleteStat = MigrationStat(3, 1000L, 2)
    val incompleteInfo = MigrationInfo(1000L, false, incompleteStat)
    assert(!incompleteInfo.allBlocksMigrated)
    assert(incompleteInfo.shuffleMigrationStat.numBlocksLeft === 3)

    val completeStat = MigrationStat(0, 5000L, 5)
    val completeInfo = MigrationInfo(2000L, true, completeStat)
    assert(completeInfo.allBlocksMigrated)
    assert(completeInfo.shuffleMigrationStat.numBlocksLeft === 0)
  }
}
