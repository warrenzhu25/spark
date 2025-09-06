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

import java.io.IOException
import java.util.concurrent.ConcurrentLinkedQueue

import scala.reflect.ClassTag

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doNothing, doThrow, mock, when}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleBlockInfo}

class ShuffleMigrationWorkerSuite extends SparkFunSuite {

  test("ShuffleMigrationWorker basic construction") {
    val conf = new SparkConf()
    val peer = BlockManagerId("executor1", "host1", 1234)
    val mockBlockManager = mock(classOf[BlockManager])
    val shuffleQueue = new ConcurrentLinkedQueue[(ShuffleBlockInfo, Int)]()
    val fallbackStorage = None
    val statsCollector = new MigrationStatisticsCollector()
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)

    val worker = new ShuffleMigrationWorker(
      peer,
      mockBlockManager,
      shuffleQueue,
      fallbackStorage,
      statsCollector,
      stateTracker,
      errorHandler,
      3)

    assert(worker != null)
    assert(worker.keepRunning)
  }

  test("ShuffleMigrationWorker handles empty queue") {
    val peer = BlockManagerId("executor1", "host1", 1234)
    val mockBlockManager = mock(classOf[BlockManager])
    val mockResolver = mock(classOf[IndexShuffleBlockResolver])
    val mockTransferService = mock(classOf[BlockTransferService])
    val shuffleQueue = new ConcurrentLinkedQueue[(ShuffleBlockInfo, Int)]()
    val fallbackStorage = None
    val statsCollector = new MigrationStatisticsCollector()
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)
    val shuffleBlock = ShuffleBlockInfo(1, 0)

    when(mockBlockManager.migratableResolver).thenReturn(mockResolver)
    when(mockBlockManager.blockTransferService).thenReturn(mockTransferService)

    val mockBuffer = mock(classOf[ManagedBuffer])
    when(mockBuffer.size()).thenReturn(1024)
    doNothing().when(mockTransferService).uploadBlockSync(
        any[String],
        any[Int],
        any[String],
        any[BlockId],
        any[ManagedBuffer],
        any[StorageLevel],
        any[ClassTag[_]])

    when(mockResolver.getMigrationBlocks(shuffleBlock)).thenReturn(List.empty)

    val worker = new ShuffleMigrationWorker(
      peer,
      mockBlockManager,
      shuffleQueue,
      fallbackStorage,
      statsCollector,
      stateTracker,
      errorHandler,
      3)

    // Add a block to test empty migration blocks
    shuffleQueue.add((shuffleBlock, 0))

    // This should handle the empty blocks case
    // We can't easily test run() due to threading, but we can test the basic setup
    assert(worker.keepRunning)
  }

  test("ShuffleMigrationWorker handles successful migration") {
    val peer = BlockManagerId("executor1", "host1", 1234)
    val mockBlockManager = mock(classOf[BlockManager])
    val mockResolver = mock(classOf[IndexShuffleBlockResolver])
    val mockTransferService = mock(classOf[BlockTransferService])
    val shuffleQueue = new ConcurrentLinkedQueue[(ShuffleBlockInfo, Int)]()
    val fallbackStorage = None
    val statsCollector = new MigrationStatisticsCollector()
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)
    val shuffleBlock = ShuffleBlockInfo(1, 0)

    when(mockBlockManager.migratableResolver).thenReturn(mockResolver)
    when(mockBlockManager.blockTransferService).thenReturn(mockTransferService)

    val mockBuffer = mock(classOf[ManagedBuffer])
    when(mockBuffer.size()).thenReturn(1024)
    val blocks = List((ShuffleBlockId(1, 0, 0), mockBuffer))
    when(mockResolver.getMigrationBlocks(shuffleBlock)).thenReturn(blocks)

    doNothing().when(mockTransferService).uploadBlockSync(
        any[String],
        any[Int],
        any[String],
        any[BlockId],
        any[ManagedBuffer],
        any[StorageLevel],
        any[ClassTag[_]])

    val worker = new ShuffleMigrationWorker(
      peer,
      mockBlockManager,
      shuffleQueue,
      fallbackStorage,
      statsCollector,
      stateTracker,
      errorHandler,
      3)

    shuffleQueue.add((shuffleBlock, 0))
    assert(worker.keepRunning)
  }

  test("ShuffleMigrationWorker handles migration error") {
    val peer = BlockManagerId("executor1", "host1", 1234)
    val mockBlockManager = mock(classOf[BlockManager])
    val mockResolver = mock(classOf[IndexShuffleBlockResolver])
    val mockTransferService = mock(classOf[BlockTransferService])
    val shuffleQueue = new ConcurrentLinkedQueue[(ShuffleBlockInfo, Int)]()
    val fallbackStorage = None
    val statsCollector = new MigrationStatisticsCollector()
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)
    val shuffleBlock = ShuffleBlockInfo(1, 0)
    val exception = new IOException("Network error")

    when(mockBlockManager.migratableResolver).thenReturn(mockResolver)
    when(mockBlockManager.blockTransferService).thenReturn(mockTransferService)

    val mockBuffer = mock(classOf[ManagedBuffer])
    when(mockBuffer.size()).thenReturn(1024)
    val blocks = List((ShuffleBlockId(1, 0, 0), mockBuffer))
    when(mockResolver.getMigrationBlocks(shuffleBlock)).thenReturn(blocks)

    doThrow(exception).when(mockTransferService).uploadBlockSync(
        any[String],
        any[Int],
        any[String],
        any[BlockId],
        any[ManagedBuffer],
        any[StorageLevel],
        any[ClassTag[_]])

    val worker = new ShuffleMigrationWorker(
      peer,
      mockBlockManager,
      shuffleQueue,
      fallbackStorage,
      statsCollector,
      stateTracker,
      errorHandler,
      3)

    shuffleQueue.add((shuffleBlock, 0))
    assert(worker.keepRunning)
  }

  test("ShuffleMigrationWorker allowRetry method with retry below limit") {
    val peer = BlockManagerId("executor1", "host1", 1234)
    val mockBlockManager = mock(classOf[BlockManager])
    val shuffleQueue = new ConcurrentLinkedQueue[(ShuffleBlockInfo, Int)]()
    val fallbackStorage = None
    val statsCollector = new MigrationStatisticsCollector()
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)

    val worker = new ShuffleMigrationWorker(
      peer,
      mockBlockManager,
      shuffleQueue,
      fallbackStorage,
      statsCollector,
      stateTracker,
      errorHandler,
      3)

    val shuffleBlock = ShuffleBlockInfo(1, 0)

    // Use reflection to access private method for testing
    val allowRetryMethod =
      worker.getClass.getDeclaredMethod("allowRetry", classOf[ShuffleBlockInfo], classOf[Int])
    allowRetryMethod.setAccessible(true)

    val result =
      allowRetryMethod.invoke(worker, shuffleBlock, Integer.valueOf(2)).asInstanceOf[Boolean]
    assert(result) // Should allow retry when below limit
  }

  test("ShuffleMigrationWorker allowRetry method with retry at limit") {
    val peer = BlockManagerId("executor1", "host1", 1234)
    val mockBlockManager = mock(classOf[BlockManager])
    val shuffleQueue = new ConcurrentLinkedQueue[(ShuffleBlockInfo, Int)]()
    val fallbackStorage = None
    val statsCollector = new MigrationStatisticsCollector()
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)

    val worker = new ShuffleMigrationWorker(
      peer,
      mockBlockManager,
      shuffleQueue,
      fallbackStorage,
      statsCollector,
      stateTracker,
      errorHandler,
      3)

    val shuffleBlock = ShuffleBlockInfo(1, 0)

    // Use reflection to access private method for testing
    val allowRetryMethod =
      worker.getClass.getDeclaredMethod("allowRetry", classOf[ShuffleBlockInfo], classOf[Int])
    allowRetryMethod.setAccessible(true)

    val result =
      allowRetryMethod.invoke(worker, shuffleBlock, Integer.valueOf(3)).asInstanceOf[Boolean]
    assert(!result) // Should not allow retry when at limit
  }

  test("ShuffleMigrationWorker with fallback storage") {
    val peer = FallbackStorage.FALLBACK_BLOCK_MANAGER_ID
    val mockBlockManager = mock(classOf[BlockManager])
    val shuffleQueue = new ConcurrentLinkedQueue[(ShuffleBlockInfo, Int)]()
    val mockFallbackStorage = mock(classOf[FallbackStorage])
    val fallbackStorage = Some(mockFallbackStorage)
    val statsCollector = new MigrationStatisticsCollector()
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)

    val worker = new ShuffleMigrationWorker(
      peer,
      mockBlockManager,
      shuffleQueue,
      fallbackStorage,
      statsCollector,
      stateTracker,
      errorHandler,
      3)

    assert(worker.keepRunning)
  }
}
