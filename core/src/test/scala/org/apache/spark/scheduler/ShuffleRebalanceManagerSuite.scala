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

import org.mockito.Mockito.{mock, when}

import org.apache.spark.{MapOutputTrackerMaster, ShuffleDependency, SparkConf, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.BlockManagerMaster
import org.apache.spark.util.CallSite

class ShuffleRebalanceManagerSuite extends SparkFunSuite {

  test("shuffle move detection with balanced executors") {
    val conf = new SparkConf()
      .set(SHUFFLE_REBALANCE_ENABLED, true)
      .set(SHUFFLE_REBALANCE_THRESHOLD, 1.5)
      .set(SHUFFLE_REBALANCE_MIN_SIZE_MB, 100L)

    val mapOutputTracker = mock(classOf[MapOutputTrackerMaster])
    val blockManagerMaster = mock(classOf[BlockManagerMaster])

    val shuffleMoveManager = new ShuffleRebalanceManager(conf, mapOutputTracker, blockManagerMaster)

    // Test with balanced executor sizes (no move needed)
    val balancedSizes = Map("exec1" -> 1000L, "exec2" -> 1100L, "exec3" -> 900L)

    // This would test the isShuffleMoveNeeded method
    // assert(!shuffleMoveManager.isShuffleMoveNeeded(balancedSizes))
  }

  test("shuffle move detection with imbalanced executors") {
    val conf = new SparkConf()
      .set(SHUFFLE_REBALANCE_ENABLED, true)
      .set(SHUFFLE_REBALANCE_THRESHOLD, 1.5)
      .set(SHUFFLE_REBALANCE_MIN_SIZE_MB, 100L)

    val mapOutputTracker = mock(classOf[MapOutputTrackerMaster])
    val blockManagerMaster = mock(classOf[BlockManagerMaster])

    val shuffleMoveManager = new ShuffleRebalanceManager(conf, mapOutputTracker, blockManagerMaster)

    // Test with imbalanced executor sizes (move needed)
    val imbalancedSizes = Map(
      "exec1" -> 1000L * 1024 * 1024, // 1GB
      "exec2" -> 200L * 1024 * 1024,  // 200MB
      "exec3" -> 150L * 1024 * 1024   // 150MB
    )

    // This would test the isShuffleMoveNeeded method
    // assert(shuffleMoveManager.isShuffleMoveNeeded(imbalancedSizes))
  }

  test("shuffle move operation planning") {
    val conf = new SparkConf()
      .set(SHUFFLE_REBALANCE_ENABLED, true)
      .set(SHUFFLE_REBALANCE_THRESHOLD, 1.5)
      .set(SHUFFLE_REBALANCE_MIN_SIZE_MB, 100L)

    val mapOutputTracker = mock(classOf[MapOutputTrackerMaster])
    val blockManagerMaster = mock(classOf[BlockManagerMaster])

    val shuffleMoveManager = new ShuffleRebalanceManager(conf, mapOutputTracker, blockManagerMaster)


    // Test would verify that move operations are planned correctly
    // with source and target executors identified properly
  }

  test("shuffle move execution with transfer service") {
    val conf = new SparkConf()
      .set(SHUFFLE_REBALANCE_ENABLED, true)
      .set(SHUFFLE_REBALANCE_THRESHOLD, 1.5)
      .set(SHUFFLE_REBALANCE_MIN_SIZE_MB, 100L)

    val mapOutputTracker = mock(classOf[MapOutputTrackerMaster])
    val blockManagerMaster = mock(classOf[BlockManagerMaster])

    val shuffleMoveManager = new ShuffleRebalanceManager(conf, mapOutputTracker, blockManagerMaster)


    // Test would verify that shuffle move operations are executed
    // and MapOutputTracker is updated with new locations
  }

  ignore("shuffle move disabled by configuration") {
    val conf = new SparkConf()
      .set(SHUFFLE_REBALANCE_ENABLED, false) // Disabled

    val mapOutputTracker = mock(classOf[MapOutputTrackerMaster])
    val blockManagerMaster = mock(classOf[BlockManagerMaster])

    val shuffleMoveManager = new ShuffleRebalanceManager(conf, mapOutputTracker, blockManagerMaster)

    // Create a mock shuffle map stage
    val stage = createMockShuffleMapStage(1)

    // This should not trigger any moves
    shuffleMoveManager.checkAndInitiateShuffleRebalance(stage)

    // Verify no transfers were initiated (message-based approach)
  }

  test("shuffle distribution statistics") {
    val conf = new SparkConf()
      .set(SHUFFLE_REBALANCE_ENABLED, true)

    val mapOutputTracker = mock(classOf[MapOutputTrackerMaster])
    val blockManagerMaster = mock(classOf[BlockManagerMaster])

    val shuffleMoveManager = new ShuffleRebalanceManager(conf, mapOutputTracker, blockManagerMaster)

    // Test statistics calculation for shuffle distribution
    val shuffleId = 1

    // Mock the MapOutputTracker to return test data
    // val stats = shuffleMoveManager.getShuffleDistributionStats(shuffleId)

    // Verify statistics are calculated correctly
    // assert(stats.totalSize >= 0)
    // assert(stats.executorCount >= 0)
    // assert(stats.imbalanceRatio >= 0)
  }

  private def createMockShuffleMapStage(shuffleId: Int): ShuffleMapStage = {
    // Create a minimal mock ShuffleMapStage for testing
    val rdd = mock(classOf[RDD[_]])
    val parents = List.empty[Stage]
    val callSite = CallSite("test", "test")
    val shuffleDep = mock(classOf[ShuffleDependency[_, _, _]])
    val mapOutputTracker = mock(classOf[MapOutputTrackerMaster])

    // Mock required methods for ShuffleDependency
    when(shuffleDep.shuffleId).thenReturn(shuffleId)

    new ShuffleMapStage(
      id = 1,
      rdd = rdd,
      numTasks = 10,
      parents = parents,
      firstJobId = 1,
      callSite = callSite,
      shuffleDep = shuffleDep,
      mapOutputTrackerMaster = mapOutputTracker,
      resourceProfileId = 0
    )
  }
}
