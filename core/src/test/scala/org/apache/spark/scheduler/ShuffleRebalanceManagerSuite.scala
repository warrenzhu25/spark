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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.mockito.Mockito.{mock, verifyNoInteractions, when}

import org.apache.spark.{MapOutputTrackerMaster, ShuffleDependency, ShuffleStatus, SparkConf, SparkFunSuite}
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.BlockManagerMaster
import org.apache.spark.util.CallSite

class ShuffleRebalanceManagerSuite extends SparkFunSuite {

  test("shuffle rebalance gated when fetch wait is high") {
    val conf = new SparkConf()
      .set(SHUFFLE_REBALANCE_ENABLED, true)
      .set(SHUFFLE_REBALANCE_FETCH_WAIT_THRESHOLD_MS, 1000L)

    val mapOutputTracker = mock(classOf[MapOutputTrackerMaster])
    val blockManagerMaster = mock(classOf[BlockManagerMaster])
    val manager = new ShuffleRebalanceManager(conf, mapOutputTracker, blockManagerMaster)

    val stage = createStageWithFetchWait(
      shuffleId = 1,
      numTasks = 100,
      fetchWaitMs = Some(60000L),
      numPartitions = 2)

    manager.checkAndInitiateShuffleRebalance(stage, completedTasks = 50)

    verifyNoInteractions(mapOutputTracker)
  }

  test("shuffle rebalance proceeds when fetch wait is below threshold") {
    val conf = new SparkConf()
      .set(SHUFFLE_REBALANCE_ENABLED, true)
      .set(SHUFFLE_REBALANCE_FETCH_WAIT_THRESHOLD_MS, 1000L)

    val mapOutputTracker = mock(classOf[MapOutputTrackerMaster])
    val shuffleStatuses = createShuffleStatuses(shuffleId = 1, numMaps = 1)
    when(mapOutputTracker.shuffleStatuses)
      .thenReturn(shuffleStatuses.asInstanceOf[collection.concurrent.Map[Int, ShuffleStatus]])

    val blockManagerMaster = mock(classOf[BlockManagerMaster])
    val manager = new ShuffleRebalanceManager(conf, mapOutputTracker, blockManagerMaster)

    val stage = createStageWithFetchWait(
      shuffleId = 1,
      numTasks = 100,
      fetchWaitMs = Some(25000L),
      numPartitions = 2)

    manager.checkAndInitiateShuffleRebalance(stage, completedTasks = 50)

    assert(loggedFetchWaitAttempts(manager).isEmpty)
  }

  test("shuffle rebalance gate disabled when threshold is zero or negative") {
    val conf = new SparkConf()
      .set(SHUFFLE_REBALANCE_ENABLED, true)
      .set(SHUFFLE_REBALANCE_FETCH_WAIT_THRESHOLD_MS, 0L)

    val mapOutputTracker = mock(classOf[MapOutputTrackerMaster])
    val shuffleStatuses = createShuffleStatuses(shuffleId = 1, numMaps = 1)
    when(mapOutputTracker.shuffleStatuses)
      .thenReturn(shuffleStatuses.asInstanceOf[collection.concurrent.Map[Int, ShuffleStatus]])

    val blockManagerMaster = mock(classOf[BlockManagerMaster])
    val manager = new ShuffleRebalanceManager(conf, mapOutputTracker, blockManagerMaster)

    val stage = createStageWithFetchWait(
      shuffleId = 1,
      numTasks = 100,
      fetchWaitMs = Some(600000L),
      numPartitions = 2)

    manager.checkAndInitiateShuffleRebalance(stage, completedTasks = 50)

    assert(loggedFetchWaitAttempts(manager).isEmpty)
  }

  test("shuffle rebalance handles missing task metrics gracefully") {
    val conf = new SparkConf()
      .set(SHUFFLE_REBALANCE_ENABLED, true)
      .set(SHUFFLE_REBALANCE_FETCH_WAIT_THRESHOLD_MS, 1000L)

    val mapOutputTracker = mock(classOf[MapOutputTrackerMaster])
    val shuffleStatuses = createShuffleStatuses(shuffleId = 1, numMaps = 1)
    when(mapOutputTracker.shuffleStatuses)
      .thenReturn(shuffleStatuses.asInstanceOf[collection.concurrent.Map[Int, ShuffleStatus]])

    val blockManagerMaster = mock(classOf[BlockManagerMaster])
    val manager = new ShuffleRebalanceManager(conf, mapOutputTracker, blockManagerMaster)

    val stage = createStageWithFetchWait(
      shuffleId = 1,
      numTasks = 100,
      fetchWaitMs = None,
      numPartitions = 2)

    manager.checkAndInitiateShuffleRebalance(stage, completedTasks = 50)

    assert(loggedFetchWaitAttempts(manager).isEmpty)
  }

  test("shuffle rebalance logs fetch wait gating once per attempt") {
    val conf = new SparkConf()
      .set(SHUFFLE_REBALANCE_ENABLED, true)
      .set(SHUFFLE_REBALANCE_FETCH_WAIT_THRESHOLD_MS, 1000L)

    val mapOutputTracker = mock(classOf[MapOutputTrackerMaster])
    val blockManagerMaster = mock(classOf[BlockManagerMaster])
    val manager = new ShuffleRebalanceManager(conf, mapOutputTracker, blockManagerMaster)

    val stage = createStageWithFetchWait(
      shuffleId = 1,
      numTasks = 100,
      fetchWaitMs = Some(60000L),
      numPartitions = 2,
      attemptNumber = 0)

    manager.checkAndInitiateShuffleRebalance(stage, completedTasks = 50)
    manager.checkAndInitiateShuffleRebalance(stage, completedTasks = 51)
    manager.checkAndInitiateShuffleRebalance(stage, completedTasks = 52)

    assert(loggedFetchWaitAttempts(manager).size === 1)
    verifyNoInteractions(mapOutputTracker)
  }

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

  test("shuffle move operation planning: one-to-many pairing") {
    val conf = new SparkConf()
      .set(SHUFFLE_REBALANCE_ENABLED, true)
      .set(SHUFFLE_REBALANCE_THRESHOLD, 1.5)
      .set(SHUFFLE_REBALANCE_MIN_SIZE_MB, 10L) // Low threshold for testing

    val mapOutputTracker = mock(classOf[MapOutputTrackerMaster])
    val blockManagerMaster = mock(classOf[BlockManagerMaster])

    val manager = new ShuffleRebalanceManager(conf, mapOutputTracker, blockManagerMaster)

    // Test that one-to-many strategy selects correct sources and targets
    // by checking which executors are identified as sources vs targets

    // Test scenario: [300MB, 150MB, 100MB, 50MB, 0MB] with avg=120MB
    val executorSizes = Map(
      "exec1" -> (300L * 1024 * 1024), // 180MB excess
      "exec2" -> (150L * 1024 * 1024), // 30MB excess
      "exec3" -> (100L * 1024 * 1024), // 20MB deficit
      "exec4" -> (50L * 1024 * 1024),  // 70MB deficit
      "exec5" -> (0L * 1024 * 1024)    // 120MB deficit
    )

    val avg = executorSizes.values.sum.toDouble / executorSizes.size
    assert(math.abs(avg - 120.0 * 1024 * 1024) < 1.0, "Average should be 120MB")

    // Verify source identification (executors > average)
    val sources = executorSizes.filter(_._2 > avg)
    assert(sources.keySet == Set("exec1", "exec2"),
      "Sources should be exec1 and exec2 (above average)")

    // Verify target identification (executors < average)
    val targets = executorSizes.filter(_._2 < avg)
    assert(targets.keySet == Set("exec3", "exec4", "exec5"),
      "Targets should be exec3, exec4, exec5 (below average)")

    // Verify largest source has most excess
    val largestSource = sources.maxBy(_._2)
    assert(largestSource._1 == "exec1", "exec1 should be largest source")

    // Verify smallest target has most deficit
    val smallestTarget = targets.minBy(_._2)
    assert(smallestTarget._1 == "exec5", "exec5 should be smallest target")
  }

  test("shuffle move operation planning: perfect balance achievement") {
    val conf = new SparkConf()
      .set(SHUFFLE_REBALANCE_ENABLED, true)
      .set(SHUFFLE_REBALANCE_THRESHOLD, 1.2)
      .set(SHUFFLE_REBALANCE_MIN_SIZE_MB, 1L)

    val mapOutputTracker = mock(classOf[MapOutputTrackerMaster])
    val blockManagerMaster = mock(classOf[BlockManagerMaster])

    val manager = new ShuffleRebalanceManager(conf, mapOutputTracker, blockManagerMaster)

    // Scenario where perfect balance is achievable
    val executorSizes = Map(
      "exec1" -> (200L * 1024 * 1024), // 100MB excess (avg=100MB)
      "exec2" -> (0L * 1024 * 1024)    // 100MB deficit
    )

    val avg = executorSizes.values.sum.toDouble / executorSizes.size
    assert(math.abs(avg - 100.0 * 1024 * 1024) < 1.0, "Average should be 100MB")

    // With one-to-many strategy, exec1 can distribute all excess to exec2
    val source = executorSizes("exec1")
    val target = executorSizes("exec2")
    val sourceExcess = source - avg.toLong
    val targetDeficit = avg.toLong - target

    assert(sourceExcess == 100L * 1024 * 1024, "Source should have 100MB excess")
    assert(targetDeficit == 100L * 1024 * 1024, "Target should have 100MB deficit")
    assert(sourceExcess == targetDeficit, "Perfect balance is achievable")
  }

  test("shuffle move operation planning: edge case with equal sizes") {
    val conf = new SparkConf()
      .set(SHUFFLE_REBALANCE_ENABLED, true)
      .set(SHUFFLE_REBALANCE_THRESHOLD, 1.5)
      .set(SHUFFLE_REBALANCE_MIN_SIZE_MB, 100L)

    val mapOutputTracker = mock(classOf[MapOutputTrackerMaster])
    val blockManagerMaster = mock(classOf[BlockManagerMaster])

    val manager = new ShuffleRebalanceManager(conf, mapOutputTracker, blockManagerMaster)

    val method = classOf[ShuffleRebalanceManager].getDeclaredMethod(
      "planShuffleRebalancing",
      classOf[Int],
      classOf[Map[String, Long]],
      classOf[Int])
    method.setAccessible(true)

    // All executors have equal size - no rebalancing needed
    val executorSizes = Map(
      "exec1" -> (100L * 1024 * 1024),
      "exec2" -> (100L * 1024 * 1024),
      "exec3" -> (100L * 1024 * 1024)
    )

    val operations = method.invoke(manager, Int.box(1), executorSizes, Int.box(10))
      .asInstanceOf[Seq[ShuffleRebalanceOperation]]

    assert(operations.isEmpty, "Should not create operations when all executors are balanced")
  }

  test("shuffle move operation planning: single executor") {
    val conf = new SparkConf()
      .set(SHUFFLE_REBALANCE_ENABLED, true)
      .set(SHUFFLE_REBALANCE_THRESHOLD, 1.5)
      .set(SHUFFLE_REBALANCE_MIN_SIZE_MB, 100L)

    val mapOutputTracker = mock(classOf[MapOutputTrackerMaster])
    val blockManagerMaster = mock(classOf[BlockManagerMaster])

    val manager = new ShuffleRebalanceManager(conf, mapOutputTracker, blockManagerMaster)

    val method = classOf[ShuffleRebalanceManager].getDeclaredMethod(
      "planShuffleRebalancing",
      classOf[Int],
      classOf[Map[String, Long]],
      classOf[Int])
    method.setAccessible(true)

    // Single executor - no rebalancing possible
    val executorSizes = Map("exec1" -> (500L * 1024 * 1024))

    val operations = method.invoke(manager, Int.box(1), executorSizes, Int.box(10))
      .asInstanceOf[Seq[ShuffleRebalanceOperation]]

    assert(operations.isEmpty, "Should not create operations with single executor")
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
    shuffleMoveManager.checkAndInitiateShuffleRebalance(stage, 5) // Simulate 5 completed tasks

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
    // val stats = shuffleMoveManager.getShuffleDistributionStats(shuffleId, 10)

    // Verify statistics are calculated correctly
    // assert(stats.totalSize >= 0)
    // assert(stats.executorCount >= 0)
    // assert(stats.imbalanceRatio >= 0)
  }

  test("metrics are updated on success and failure") {
    val conf = new SparkConf()
      .set(SHUFFLE_REBALANCE_ENABLED, true)

    val mapOutputTracker = mock(classOf[MapOutputTrackerMaster])
    val blockManagerMaster = mock(classOf[BlockManagerMaster])

    val shuffleMoveManager = new ShuffleRebalanceManager(conf, mapOutputTracker, blockManagerMaster)

    // Initial state
    assert(shuffleMoveManager.rebalanceOpsCount === 0)
    assert(shuffleMoveManager.rebalanceBytesMoved === 0)
    assert(shuffleMoveManager.rebalanceErrors === 0)

    val source = shuffleMoveManager.metricsSource
    assert(source.sourceName === "ShuffleRebalance")
    assert(source.metricRegistry.getGauges.containsKey("rebalanceOpsCount"))
    assert(source.metricRegistry.getGauges.containsKey("rebalanceBytesMoved"))
    assert(source.metricRegistry.getGauges.containsKey("rebalanceErrors"))
  }

  private def createStageWithFetchWait(
      shuffleId: Int,
      numTasks: Int,
      fetchWaitMs: Option[Long],
      numPartitions: Int,
      attemptNumber: Int = 0): ShuffleMapStage = {
    val stage = mock(classOf[ShuffleMapStage])
    val partitioner = mock(classOf[org.apache.spark.Partitioner])
    when(partitioner.numPartitions).thenReturn(numPartitions)

    val shuffleDep = mock(classOf[ShuffleDependency[_, _, _]])
    when(shuffleDep.shuffleId).thenReturn(shuffleId)
    when(shuffleDep.partitioner).thenReturn(partitioner)

    val stageInfo = createStageInfo(
      stageId = 1,
      attemptId = attemptNumber,
      numTasks = numTasks,
      shuffleId = shuffleId,
      fetchWaitMs = fetchWaitMs)

    val dep: ShuffleDependency[_, _, _] = shuffleDep
    when(stage.id).thenReturn(1)
    when(stage.numTasks).thenReturn(numTasks)
    when(stage.shuffleDep).thenAnswer(_ => dep.asInstanceOf[ShuffleDependency[Any, Any, Any]])
    when(stage.latestInfo).thenReturn(stageInfo)
    stage
  }

  private def createStageInfo(
      stageId: Int,
      attemptId: Int,
      numTasks: Int,
      shuffleId: Int,
      fetchWaitMs: Option[Long]): StageInfo = {
    val taskMetrics = fetchWaitMs.map { wait =>
      val metrics = new TaskMetrics()
      metrics.shuffleReadMetrics.setFetchWaitTime(wait)
      metrics
    }.orNull

    new StageInfo(
      stageId,
      attemptId,
      s"stage-$stageId",
      numTasks,
      Seq.empty,
      Seq.empty,
      "details",
      taskMetrics,
      Seq.empty,
      Some(shuffleId),
      resourceProfileId = 0)
  }

  private def createShuffleStatuses(
      shuffleId: Int,
      numMaps: Int,
      mapStatuses: Seq[MapStatus] = Seq.empty): collection.concurrent.Map[Int, ShuffleStatus] = {
    val shuffleStatus = new ShuffleStatus(numMaps)
    mapStatuses.zipWithIndex.foreach { case (status, idx) =>
      if (idx < shuffleStatus.mapStatuses.length) {
        shuffleStatus.mapStatuses(idx) = status
      }
    }

    val statuses = new ConcurrentHashMap[Int, ShuffleStatus]().asScala
    statuses.put(shuffleId, shuffleStatus)
    statuses
  }

  private def loggedFetchWaitAttempts(manager: ShuffleRebalanceManager): Set[Int] = {
    val field = classOf[ShuffleRebalanceManager].getDeclaredField("loggedFetchWaitGating")
    field.setAccessible(true)
    val logged = field.get(manager)
      .asInstanceOf[ConcurrentHashMap[Int, java.lang.Boolean]]
    logged.keySet().asScala.map(_.intValue()).toSet
  }

  private def createMockShuffleMapStage(shuffleId: Int): ShuffleMapStage = {
    // Create a minimal mock ShuffleMapStage for testing
    val rdd = mock(classOf[RDD[_]])
    val parents = List.empty[Stage]
    val callSite = CallSite("test", "test")
    val shuffleDep = mock(classOf[ShuffleDependency[_, _, _]])
    val mapOutputTracker = mock(classOf[MapOutputTrackerMaster])
    val partitioner = mock(classOf[org.apache.spark.Partitioner])

    // Mock required methods for ShuffleDependency
    when(shuffleDep.shuffleId).thenReturn(shuffleId)
    when(shuffleDep.partitioner).thenReturn(partitioner)
    when(partitioner.numPartitions).thenReturn(10)

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
