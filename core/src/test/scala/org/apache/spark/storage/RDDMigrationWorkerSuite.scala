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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}

import org.apache.spark.{SparkConf, SparkFunSuite}

class RDDMigrationWorkerSuite extends SparkFunSuite {

  test("RDDMigrationWorker basic construction") {
    val conf = new SparkConf()
    val mockBlockManager = mock(classOf[BlockManager])
    val mockMigrationStrategy = mock(classOf[MigrationStrategy])
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)
    val fallbackStorage = None

    val worker = new RDDMigrationWorker(
      mockBlockManager,
      conf,
      mockMigrationStrategy,
      stateTracker,
      errorHandler,
      fallbackStorage)

    assert(worker != null)
  }

  test("RDDMigrationWorker hasAvailablePeers with peers") {
    val conf = new SparkConf()
    val mockBlockManager = mock(classOf[BlockManager])
    val mockMigrationStrategy = mock(classOf[MigrationStrategy])
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)
    val fallbackStorage = None

    val peers = Array(
      BlockManagerId("executor1", "host1", 1234),
      BlockManagerId("executor2", "host2", 1234))
    when(mockBlockManager.getPeers(false)).thenReturn(peers)

    val worker = new RDDMigrationWorker(
      mockBlockManager,
      conf,
      mockMigrationStrategy,
      stateTracker,
      errorHandler,
      fallbackStorage)

    // Use reflection to test private method
    val hasAvailablePeersMethod = worker.getClass.getDeclaredMethod("hasAvailablePeers")
    hasAvailablePeersMethod.setAccessible(true)
    val result = hasAvailablePeersMethod.invoke(worker).asInstanceOf[Boolean]

    assert(result)
  }

  test("RDDMigrationWorker hasAvailablePeers without peers") {
    val conf = new SparkConf()
    val mockBlockManager = mock(classOf[BlockManager])
    val mockMigrationStrategy = mock(classOf[MigrationStrategy])
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)
    val fallbackStorage = None

    when(mockBlockManager.getPeers(false)).thenReturn(Array.empty[BlockManagerId])

    val worker = new RDDMigrationWorker(
      mockBlockManager,
      conf,
      mockMigrationStrategy,
      stateTracker,
      errorHandler,
      fallbackStorage)

    // Use reflection to test private method
    val hasAvailablePeersMethod = worker.getClass.getDeclaredMethod("hasAvailablePeers")
    hasAvailablePeersMethod.setAccessible(true)
    val result = hasAvailablePeersMethod.invoke(worker).asInstanceOf[Boolean]

    assert(!result)
  }

  test("RDDMigrationWorker hasAvailablePeers with only fallback peer") {
    val conf = new SparkConf()
    val mockBlockManager = mock(classOf[BlockManager])
    val mockMigrationStrategy = mock(classOf[MigrationStrategy])
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)
    val fallbackStorage = None

    val peers = Array(FallbackStorage.FALLBACK_BLOCK_MANAGER_ID)
    when(mockBlockManager.getPeers(false)).thenReturn(peers)

    val worker = new RDDMigrationWorker(
      mockBlockManager,
      conf,
      mockMigrationStrategy,
      stateTracker,
      errorHandler,
      fallbackStorage)

    // Use reflection to test private method
    val hasAvailablePeersMethod = worker.getClass.getDeclaredMethod("hasAvailablePeers")
    hasAvailablePeersMethod.setAccessible(true)
    val result = hasAvailablePeersMethod.invoke(worker).asInstanceOf[Boolean]

    assert(!result)
  }

  test("RDDMigrationWorker performMigrationRound successful") {
    val conf = new SparkConf()
    val mockBlockManager = mock(classOf[BlockManager])
    val mockMigrationStrategy = mock(classOf[MigrationStrategy])
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)
    val fallbackStorage = None

    when(mockMigrationStrategy.migrateBlocks(any[BlockManager], any[SparkConf]))
      .thenReturn(false) // No blocks left

    val worker = new RDDMigrationWorker(
      mockBlockManager,
      conf,
      mockMigrationStrategy,
      stateTracker,
      errorHandler,
      fallbackStorage)

    // Use reflection to test private method
    val performMigrationRoundMethod = worker.getClass.getDeclaredMethod("performMigrationRound")
    performMigrationRoundMethod.setAccessible(true)
    val result = performMigrationRoundMethod.invoke(worker).asInstanceOf[Boolean]

    assert(!result) // Should return false when no blocks left
  }

  test("RDDMigrationWorker performMigrationRound with blocks remaining") {
    val conf = new SparkConf()
    val mockBlockManager = mock(classOf[BlockManager])
    val mockMigrationStrategy = mock(classOf[MigrationStrategy])
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)
    val fallbackStorage = None

    when(mockMigrationStrategy.migrateBlocks(any[BlockManager], any[SparkConf]))
      .thenReturn(true) // Blocks still remaining

    val worker = new RDDMigrationWorker(
      mockBlockManager,
      conf,
      mockMigrationStrategy,
      stateTracker,
      errorHandler,
      fallbackStorage)

    // Use reflection to test private method
    val performMigrationRoundMethod = worker.getClass.getDeclaredMethod("performMigrationRound")
    performMigrationRoundMethod.setAccessible(true)
    val result = performMigrationRoundMethod.invoke(worker).asInstanceOf[Boolean]

    assert(result) // Should return true when blocks remaining
  }

  test("RDDMigrationWorker with fallback storage") {
    val conf = new SparkConf()
    val mockBlockManager = mock(classOf[BlockManager])
    val mockMigrationStrategy = mock(classOf[MigrationStrategy])
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)
    val mockFallbackStorage = mock(classOf[FallbackStorage])
    val fallbackStorage = Some(mockFallbackStorage)

    val worker = new RDDMigrationWorker(
      mockBlockManager,
      conf,
      mockMigrationStrategy,
      stateTracker,
      errorHandler,
      fallbackStorage)

    assert(worker != null)
  }

  test("RDDMigrationWorker state tracker integration") {
    val conf = new SparkConf()
    val mockBlockManager = mock(classOf[BlockManager])
    val mockMigrationStrategy = mock(classOf[MigrationStrategy])
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)
    val fallbackStorage = None

    // Initially should continue
    assert(stateTracker.shouldContinueRDDMigration)

    val worker = new RDDMigrationWorker(
      mockBlockManager,
      conf,
      mockMigrationStrategy,
      stateTracker,
      errorHandler,
      fallbackStorage)

    // Stop migration
    stateTracker.stopRDD("Test")
    assert(!stateTracker.shouldContinueRDDMigration)
  }
}
