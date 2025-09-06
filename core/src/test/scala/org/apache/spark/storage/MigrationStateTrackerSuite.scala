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

class MigrationStateTrackerSuite extends SparkFunSuite {

  test("MigrationStateTracker initial state") {
    val tracker = new MigrationStateTracker()

    assert(tracker.rddBlocksLeft)
    assert(tracker.shuffleBlocksLeft)
    assert(!tracker.stopped)
    assert(!tracker.stoppedRDD)
    assert(!tracker.stoppedShuffle)
    assert(!tracker.isMigrationComplete)
    assert(!tracker.areAllBlocksMigrated)
    assert(tracker.shouldContinueRDDMigration)
    assert(tracker.shouldContinueShuffleMigration)
  }

  test("MigrationStateTracker initialization") {
    val tracker = new MigrationStateTracker()
    val beforeInit = System.currentTimeMillis()

    tracker.initializeMigration()

    assert(tracker.migrationStartTime >= beforeInit)
    assert(tracker.lastProgressLogTime >= beforeInit)
  }

  test("MigrationStateTracker RDD migration tracking") {
    val tracker = new MigrationStateTracker()
    val beforeMigration = System.nanoTime()

    tracker.markRDDMigrationCompleted(hasBlocksLeft = false)

    assert(tracker.lastRDDMigrationTime >= beforeMigration)
    assert(!tracker.rddBlocksLeft)
  }

  test("MigrationStateTracker shuffle migration tracking") {
    val tracker = new MigrationStateTracker()
    val beforeMigration = System.nanoTime()

    tracker.markShuffleMigrationCompleted(hasBlocksLeft = true)

    assert(tracker.lastShuffleMigrationTime >= beforeMigration)
    assert(tracker.shuffleBlocksLeft)
  }

  test("MigrationStateTracker stop operations") {
    val tracker = new MigrationStateTracker()

    assert(tracker.shouldContinueRDDMigration)
    tracker.stopRDD("test reason")
    assert(!tracker.shouldContinueRDDMigration)
    assert(tracker.stoppedRDD)

    assert(tracker.shouldContinueShuffleMigration)
    tracker.stopShuffle()
    assert(!tracker.shouldContinueShuffleMigration)
    assert(tracker.stoppedShuffle)

    assert(tracker.isMigrationComplete)
  }

  test("MigrationStateTracker stop all") {
    val tracker = new MigrationStateTracker()

    tracker.stopAll()

    assert(tracker.stopped)
    assert(!tracker.shouldContinueRDDMigration)
    assert(!tracker.shouldContinueShuffleMigration)
    assert(tracker.isMigrationComplete)
  }

  test("MigrationStateTracker blocks migrated logic") {
    val tracker = new MigrationStateTracker()

    // Initially has blocks left, not migrated
    assert(!tracker.areAllBlocksMigrated)

    // Mark RDD blocks as migrated
    tracker.markRDDMigrationCompleted(hasBlocksLeft = false)
    assert(!tracker.areAllBlocksMigrated) // Still have shuffle blocks

    // Mark shuffle blocks as migrated
    tracker.markShuffleMigrationCompleted(hasBlocksLeft = false)
    assert(tracker.areAllBlocksMigrated) // Now all migrated
  }

  test("MigrationStateTracker blocks migrated with stopped migrations") {
    val tracker = new MigrationStateTracker()

    // Stop RDD migration, shuffle still has blocks
    tracker.stopRDD()
    tracker.markShuffleMigrationCompleted(hasBlocksLeft = false)
    assert(tracker.areAllBlocksMigrated) // RDD stopped counts as migrated

    val tracker2 = new MigrationStateTracker()
    // Stop shuffle migration, RDD still has blocks
    tracker2.stopShuffle()
    tracker2.markRDDMigrationCompleted(hasBlocksLeft = false)
    assert(tracker2.areAllBlocksMigrated) // Shuffle stopped counts as migrated
  }

  test("MigrationStateTracker last migration time") {
    val tracker = new MigrationStateTracker()

    val rddTime = System.nanoTime()
    tracker.markRDDMigrationCompleted(hasBlocksLeft = false)

    Thread.sleep(10) // Ensure different timestamps

    val shuffleTime = System.nanoTime()
    tracker.markShuffleMigrationCompleted(hasBlocksLeft = false)

    // Should return the minimum (oldest) time when both are running
    val lastTime = tracker.getLastMigrationTime
    assert(lastTime == tracker.lastRDDMigrationTime) // RDD was earlier

    // When stopped, should return current time
    val beforeStop = System.nanoTime()
    tracker.stopAll()
    val stoppedTime = tracker.getLastMigrationTime
    assert(stoppedTime >= beforeStop)
  }

  test("MigrationStateTracker activity tracking") {
    val tracker = new MigrationStateTracker()
    val beforeActivity = System.currentTimeMillis()

    tracker.recordMigrationActivity()

    assert(tracker.lastMigrationActivity >= beforeActivity)
  }

  test("MigrationStateTracker progress log time") {
    val tracker = new MigrationStateTracker()
    val beforeUpdate = System.currentTimeMillis()

    tracker.updateProgressLogTime()

    assert(tracker.lastProgressLogTime >= beforeUpdate)
  }

  test("MigrationStateTracker state summary") {
    val tracker = new MigrationStateTracker()

    val summary = tracker.getStateSummary
    assert(summary.contains("stopped=false"))
    assert(summary.contains("stoppedRDD=false"))
    assert(summary.contains("stoppedShuffle=false"))
    assert(summary.contains("rddBlocksLeft=true"))
    assert(summary.contains("shuffleBlocksLeft=true"))

    tracker.stopRDD()
    val updatedSummary = tracker.getStateSummary
    assert(updatedSummary.contains("stoppedRDD=true"))
  }
}
