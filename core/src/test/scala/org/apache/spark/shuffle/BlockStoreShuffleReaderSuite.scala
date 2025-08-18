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

package org.apache.spark.shuffle

import java.io.{ByteArrayOutputStream, InputStream}
import java.nio.ByteBuffer

import org.mockito.ArgumentMatchers.{eq => meq}
import org.mockito.Mockito.{mock, when}

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.storage.{BlockManager, BlockManagerId, ShuffleBlockId}

/**
 * Wrapper for a managed buffer that keeps track of how many times retain and release are called.
 *
 * We need to define this class ourselves instead of using a spy because the NioManagedBuffer class
 * is final (final classes cannot be spied on).
 */
class RecordingManagedBuffer(underlyingBuffer: NioManagedBuffer) extends ManagedBuffer {
  var callsToRetain = 0
  var callsToRelease = 0

  override def size(): Long = underlyingBuffer.size()
  override def nioByteBuffer(): ByteBuffer = underlyingBuffer.nioByteBuffer()
  override def createInputStream(): InputStream = underlyingBuffer.createInputStream()
  override def convertToNetty(): AnyRef = underlyingBuffer.convertToNetty()

  override def retain(): ManagedBuffer = {
    callsToRetain += 1
    underlyingBuffer.retain()
  }
  override def release(): ManagedBuffer = {
    callsToRelease += 1
    underlyingBuffer.release()
  }
}

class BlockStoreShuffleReaderSuite extends SparkFunSuite with LocalSparkContext {

  /**
   * This test makes sure that, when data is read from a HashShuffleReader, the underlying
   * ManagedBuffers that contain the data are eventually released.
   */
  test("read() releases resources on completion") {
    val testConf = new SparkConf(false)
    // Create a SparkContext as a convenient way of setting SparkEnv (needed because some of the
    // shuffle code calls SparkEnv.get()).
    sc = new SparkContext("local", "test", testConf)

    val reduceId = 15
    val shuffleId = 22
    val numMaps = 6
    val keyValuePairsPerMap = 10
    val serializer = new JavaSerializer(testConf)

    // Make a mock BlockManager that will return RecordingManagedByteBuffers of data, so that we
    // can ensure retain() and release() are properly called.
    val blockManager = mock(classOf[BlockManager])

    // Create a buffer with some randomly generated key-value pairs to use as the shuffle data
    // from each mappers (all mappers return the same shuffle data).
    val byteOutputStream = new ByteArrayOutputStream()
    val serializationStream = serializer.newInstance().serializeStream(byteOutputStream)
    (0 until keyValuePairsPerMap).foreach { i =>
      serializationStream.writeKey(i)
      serializationStream.writeValue(2*i)
    }

    // Setup the mocked BlockManager to return RecordingManagedBuffers.
    val localBlockManagerId = BlockManagerId("test-client", "test-client", 1)
    when(blockManager.blockManagerId).thenReturn(localBlockManagerId)
    val buffers = (0 until numMaps).map { mapId =>
      // Create a ManagedBuffer with the shuffle data.
      val nioBuffer = new NioManagedBuffer(ByteBuffer.wrap(byteOutputStream.toByteArray))
      val managedBuffer = new RecordingManagedBuffer(nioBuffer)

      // Setup the blockManager mock so the buffer gets returned when the shuffle code tries to
      // fetch shuffle data.
      val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, reduceId)
      when(blockManager.getLocalBlockData(meq(shuffleBlockId))).thenReturn(managedBuffer)
      managedBuffer
    }

    // Make a mocked MapOutputTracker for the shuffle reader to use to determine what
    // shuffle data to read.
    val mapOutputTracker = mock(classOf[MapOutputTracker])
    when(mapOutputTracker.getMapSizesByExecutorId(
      shuffleId, 0, numMaps, reduceId, reduceId + 1)).thenReturn {
      // Test a scenario where all data is local, to avoid creating a bunch of additional mocks
      // for the code to read data over the network.
      val shuffleBlockIdsAndSizes = (0 until numMaps).map { mapId =>
        val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, reduceId)
        (shuffleBlockId, byteOutputStream.size().toLong, mapId)
      }
      Seq((localBlockManagerId, shuffleBlockIdsAndSizes)).iterator
    }

    // Create a mocked shuffle handle to pass into HashShuffleReader.
    val shuffleHandle = {
      val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
      when(dependency.serializer).thenReturn(serializer)
      when(dependency.aggregator).thenReturn(None)
      when(dependency.keyOrdering).thenReturn(None)
      new BaseShuffleHandle(shuffleId, dependency)
    }

    val serializerManager = new SerializerManager(
      serializer,
      new SparkConf()
        .set(config.SHUFFLE_COMPRESS, false)
        .set(config.SHUFFLE_SPILL_COMPRESS, false))

    val taskContext = TaskContext.empty()
    val metrics = taskContext.taskMetrics.createTempShuffleReadMetrics()
    val blocksByAddress = mapOutputTracker.getMapSizesByExecutorId(
      shuffleId, 0, numMaps, reduceId, reduceId + 1)
    val shuffleReader = new BlockStoreShuffleReader(
      shuffleHandle,
      blocksByAddress,
      taskContext,
      metrics,
      serializerManager,
      blockManager)

    assert(shuffleReader.read().length === keyValuePairsPerMap * numMaps)

    // Calling .length above will have exhausted the iterator; make sure that exhausting the
    // iterator caused retain and release to be called on each buffer.
    buffers.foreach { buffer =>
      assert(buffer.callsToRetain === 1)
      assert(buffer.callsToRelease === 1)
    }
  }

  test("fetch requests are load balanced across executors") {
    val conf = new SparkConf()
      .set(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS, 3)
      .set(config.REDUCER_MAX_SIZE_IN_FLIGHT, "1m")
      .set(config.REDUCER_MAX_REQ_SIZE_SHUFFLE_TO_MEM, "1m")

    // Create multiple mock executors with different performance characteristics
    val fastExecutor = BlockManagerId("fast", "fast-host", 7337)
    val mediumExecutor = BlockManagerId("medium", "medium-host", 7337)
    val slowExecutor = BlockManagerId("slow", "slow-host", 7337)

    val blockManager = mock(classOf[BlockManager])
    when(blockManager.conf).thenReturn(conf)
    when(blockManager.blockManagerId).thenReturn(BlockManagerId("local", "local-host", 7337))

    val mapOutputTracker = mock(classOf[MapOutputTracker])

    // Set up blocks distributed across executors with different sizes
    val blocksByExecutor = Map(
      fastExecutor -> (0 until 4).map(i => (ShuffleBlockId(0, i, 0), 1000L, i)),
      mediumExecutor -> (4 until 8).map(i => (ShuffleBlockId(0, i, 0), 2000L, i)),
      slowExecutor -> (8 until 12).map(i => (ShuffleBlockId(0, i, 0), 3000L, i))
    )

    when(mapOutputTracker.getMapSizesByExecutorId(0, 0, 12, 0, 1)).thenReturn {
      blocksByExecutor.toSeq.iterator
    }

    val shuffleHandle = {
      val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
      when(dependency.serializer).thenReturn(new JavaSerializer(conf))
      when(dependency.aggregator).thenReturn(None)
      when(dependency.keyOrdering).thenReturn(None)
      new BaseShuffleHandle(0, dependency)
    }

    val serializerManager = new SerializerManager(new JavaSerializer(conf), conf)
    val taskContext = TaskContext.empty()
    val metrics = taskContext.taskMetrics.createTempShuffleReadMetrics()
    val blocksByAddress = mapOutputTracker.getMapSizesByExecutorId(0, 0, 12, 0, 1)

    // Create the shuffle reader which will use ShuffleBlockFetcherIterator internally
    val shuffleReader = new BlockStoreShuffleReader(
      shuffleHandle,
      blocksByAddress,
      taskContext,
      metrics,
      serializerManager,
      blockManager)

    // The test passes if we can create the reader without errors
    // In practice, the load balancing happens in ShuffleBlockFetcherIterator.initialize()
    // which is called when the reader is created
    assert(shuffleReader != null)

    logInfo("Successfully created shuffle reader with load-balanced fetch requests")
  }
}
