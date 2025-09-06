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
import org.apache.spark.shuffle.ShuffleBlockInfo

class MigrationStatisticsCollectorSuite extends SparkFunSuite {

  test("MigrationStatisticsCollector initial state") {
    val collector = new MigrationStatisticsCollector()

    assert(collector.numMigratedShuffles === 0)
    assert(collector.migratedShufflesSize === 0L)
    assert(collector.totalShufflesSize === 0L)
    assert(collector.deletedShuffles === 0)
    assert(collector.migratingShufflesSize === 0)
    assert(collector.getMigrationProgressPercent === 100.0) // No blocks means 100% complete
    assert(!collector.hasShufflesToMigrate)
    assert(collector.getRemainingShuffleCount === 0)
  }

  test("MigrationStatisticsCollector record shuffle migration") {
    val collector = new MigrationStatisticsCollector()

    collector.recordShuffleMigrated(1024L)
    collector.recordShuffleMigrated(2048L)

    assert(collector.numMigratedShuffles === 2)
    assert(collector.migratedShufflesSize === 3072L)
  }

  test("MigrationStatisticsCollector record shuffle deletion") {
    val collector = new MigrationStatisticsCollector()

    collector.recordShuffleDeleted()
    collector.recordShuffleDeleted()

    assert(collector.deletedShuffles === 2)
  }

  test("MigrationStatisticsCollector add shuffles to track") {
    val collector = new MigrationStatisticsCollector()
    val shuffles = Seq(
      ShuffleBlockInfo(1, 0),
      ShuffleBlockInfo(1, 1),
      ShuffleBlockInfo(2, 0)
    )

    collector.addShufflesToTrack(shuffles, 5120L)

    assert(collector.migratingShufflesSize === 3)
    assert(collector.totalShufflesSize === 5120L)
    assert(collector.hasShufflesToMigrate)
    assert(collector.getRemainingShuffleCount === 3)
  }

  test("MigrationStatisticsCollector progress calculation") {
    val collector = new MigrationStatisticsCollector()
    val shuffles = Seq(
      ShuffleBlockInfo(1, 0),
      ShuffleBlockInfo(1, 1),
      ShuffleBlockInfo(2, 0),
      ShuffleBlockInfo(2, 1)
    )

    collector.addShufflesToTrack(shuffles, 8192L)
    assert(collector.getMigrationProgressPercent === 0.0)

    collector.recordShuffleMigrated(1024L)
    assert(collector.getMigrationProgressPercent === 25.0)

    collector.recordShuffleMigrated(2048L)
    assert(collector.getMigrationProgressPercent === 50.0)

    collector.recordShuffleMigrated(1536L)
    collector.recordShuffleMigrated(512L)
    assert(collector.getMigrationProgressPercent === 100.0)
    assert(!collector.hasShufflesToMigrate)
    assert(collector.getRemainingShuffleCount === 0)
  }

  test("MigrationStatisticsCollector build migration stat") {
    val collector = new MigrationStatisticsCollector()
    val shuffles = Seq(
      ShuffleBlockInfo(1, 0),
      ShuffleBlockInfo(1, 1),
      ShuffleBlockInfo(2, 0)
    )

    collector.addShufflesToTrack(shuffles, 6144L)
    collector.recordShuffleMigrated(1024L)
    collector.recordShuffleMigrated(2048L)
    collector.recordShuffleDeleted()

    val stats = collector.buildMigrationStat()
    assert(stats.numBlocksLeft === 1)
    assert(stats.totalMigratedSize === 3072L)
    assert(stats.numMigratedBlock === 2)
    assert(stats.totalBlocks === 3)
    assert(stats.totalSize === 6144L)
    assert(stats.deletedBlocks === 1)
  }

  test("MigrationStatisticsCollector progress summary") {
    val collector = new MigrationStatisticsCollector()
    val shuffles = Seq(
      ShuffleBlockInfo(1, 0),
      ShuffleBlockInfo(1, 1)
    )

    collector.addShufflesToTrack(shuffles, 2048L)
    collector.recordShuffleMigrated(1024L)

    val summary = collector.getProgressSummary
    assert(summary.contains("1/2 blocks"))
    assert(summary.contains("50.0%"))
    assert(summary.contains("1024.0 B/2.0 KiB"))
  }

  test("MigrationStatisticsCollector detailed summary") {
    val collector = new MigrationStatisticsCollector()
    val shuffles = Seq(
      ShuffleBlockInfo(1, 0),
      ShuffleBlockInfo(1, 1)
    )

    collector.addShufflesToTrack(shuffles, 2048L)
    collector.recordShuffleMigrated(1024L)
    collector.recordShuffleDeleted()

    val detailedSummary = collector.getDetailedSummary(2000L) // 2 seconds
    assert(detailedSummary.contains("1/2 blocks"))
    assert(detailedSummary.contains("100.0%")) // 1 migrated + 1 deleted = 2/2 = 100%
    assert(detailedSummary.contains("rate: "))
    assert(detailedSummary.contains("blocks/s"))
    assert(detailedSummary.contains("1 deleted"))
  }

  test("MigrationStatisticsCollector detailed summary without timing") {
    val collector = new MigrationStatisticsCollector()
    val shuffles = Seq(ShuffleBlockInfo(1, 0))

    collector.addShufflesToTrack(shuffles, 1024L)
    collector.recordShuffleMigrated(1024L)

    val detailedSummary = collector.getDetailedSummary(0L) // No elapsed time
    assert(detailedSummary.contains("1/1 blocks"))
    assert(detailedSummary.contains("100.0%"))
    assert(!detailedSummary.contains("rate: "))
  }

  test("MigrationStatisticsCollector log progress tracking") {
    val collector = new MigrationStatisticsCollector()
    val shuffles = Seq(
      ShuffleBlockInfo(1, 0),
      ShuffleBlockInfo(1, 1),
      ShuffleBlockInfo(1, 2),
      ShuffleBlockInfo(1, 3)
    )

    collector.addShufflesToTrack(shuffles, 4096L)

    // Should not log initially
    var lastLogged = collector.logProgressIfNeeded(0, progressThreshold = 2)
    assert(lastLogged === 0)

    // Migrate one block - still below threshold
    collector.recordShuffleMigrated(1024L)
    lastLogged = collector.logProgressIfNeeded(lastLogged, progressThreshold = 2)
    assert(lastLogged === 0)

    // Migrate second block - should trigger log
    collector.recordShuffleMigrated(1024L)
    lastLogged = collector.logProgressIfNeeded(lastLogged, progressThreshold = 2)
    assert(lastLogged === 2)

    // Migrate third block - should not trigger log yet (only 1 more since last log)
    collector.recordShuffleMigrated(1024L)
    lastLogged = collector.logProgressIfNeeded(lastLogged, progressThreshold = 2)
    assert(lastLogged === 2)  // Still 2 because threshold not reached

    // Migrate fourth block - should trigger log again (2 more since last log)
    collector.recordShuffleMigrated(1024L)
    lastLogged = collector.logProgressIfNeeded(lastLogged, progressThreshold = 2)
    assert(lastLogged === 4)
  }

  test("MigrationStatisticsCollector reset functionality") {
    val collector = new MigrationStatisticsCollector()
    val shuffles = Seq(ShuffleBlockInfo(1, 0))

    collector.addShufflesToTrack(shuffles, 1024L)
    collector.recordShuffleMigrated(1024L)
    collector.recordShuffleDeleted()

    assert(collector.numMigratedShuffles === 1)
    assert(collector.migratedShufflesSize === 1024L)
    assert(collector.deletedShuffles === 1)

    collector.reset()

    assert(collector.numMigratedShuffles === 0)
    assert(collector.migratedShufflesSize === 0L)
    assert(collector.totalShufflesSize === 0L)
    assert(collector.deletedShuffles === 0)
    assert(collector.migratingShufflesSize === 0)
  }

  test("MigrationStatisticsCollector stats summary") {
    val collector = new MigrationStatisticsCollector()
    val shuffles = Seq(ShuffleBlockInfo(1, 0), ShuffleBlockInfo(1, 1))

    collector.addShufflesToTrack(shuffles, 2048L)
    collector.recordShuffleMigrated(1024L)
    collector.recordShuffleDeleted()

    val summary = collector.getStatsSummary
    assert(summary.contains("Migrated: 1"))
    assert(summary.contains("Size: 1024.0 B"))
    assert(summary.contains("Total: 2"))
    assert(summary.contains("Deleted: 1"))
  }

  test("MigrationStatisticsCollector thread safety") {
    val collector = new MigrationStatisticsCollector()
    val shuffles = (0 until 100).map(i => ShuffleBlockInfo(1, i))

    collector.addShufflesToTrack(shuffles, 100 * 1024L)

    // Simulate concurrent updates
    val threads = (0 until 10).map { _ =>
      new Thread(() => {
        for (_ <- 0 until 10) {
          collector.recordShuffleMigrated(1024L)
          if (math.random() < 0.1) {
            collector.recordShuffleDeleted()
          }
        }
      })
    }

    threads.foreach(_.start())
    threads.foreach(_.join())

    // Verify final counts are consistent
    assert(collector.numMigratedShuffles === 100)
    assert(collector.migratedShufflesSize === 100 * 1024L)
    assert(collector.deletedShuffles >= 0) // Could be 0 due to randomness
  }
}
