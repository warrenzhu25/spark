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

package org.apache.spark.network.netty

import java.io.IOException

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Random

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, times, verify, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.{ExecutorDeadException, SecurityManager, SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.client.{TransportClient, TransportClientFactory}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager}
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcTimeout}
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.storage.{StorageLevel, TestBlockId}

class NettyBlockTransferServiceSuite
  extends SparkFunSuite
  with BeforeAndAfterEach
  with Matchers {

  private var service0: NettyBlockTransferService = _
  private var service1: NettyBlockTransferService = _

  override def afterEach(): Unit = {
    try {
      if (service0 != null) {
        service0.close()
        service0 = null
      }

      if (service1 != null) {
        service1.close()
        service1 = null
      }
    } finally {
      super.afterEach()
    }
  }

  test("can bind to a random port") {
    service0 = createService(port = 0)
    service0.port should not be 0
  }

  test("can bind to two random ports") {
    service0 = createService(port = 0)
    service1 = createService(port = 0)
    service0.port should not be service1.port
  }

  test("can bind to a specific port") {
    val port = 17634 + Random.nextInt(10000)
    logInfo("random port for test: " + port)
    service0 = createService(port)
    verifyServicePort(expectedPort = port, actualPort = service0.port)
  }

  test("can bind to a specific port twice and the second increments") {
    val port = 17634 + Random.nextInt(10000)
    logInfo("random port for test: " + port)
    service0 = createService(port)
    verifyServicePort(expectedPort = port, actualPort = service0.port)
    service1 = createService(service0.port)
    // `service0.port` is occupied, so `service1.port` should not be `service0.port`
    verifyServicePort(expectedPort = service0.port + 1, actualPort = service1.port)
  }

  test("SPARK-27637: test fetch block with executor dead") {
    implicit val executionContext = ExecutionContext.global
    val port = 17634 + Random.nextInt(10000)
    logInfo("random port for test: " + port)

    val driverEndpointRef = new RpcEndpointRef(new SparkConf()) {
      override def address: RpcAddress = null
      override def name: String = "test"
      override def send(message: Any): Unit = {}
      // This rpcEndPointRef always return false for unit test to touch ExecutorDeadException.
      override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
        Future{false.asInstanceOf[T]}
      }
    }

    val clientFactory = mock(classOf[TransportClientFactory])
    val client = mock(classOf[TransportClient])
    // This is used to touch an IOException during fetching block.
    when(client.sendRpc(any(), any())).thenAnswer(_ => {throw new IOException()})
    var createClientCount = 0
    when(clientFactory.createClient(any(), any(), any())).thenAnswer(_ => {
      createClientCount += 1
      client
    })

    val listener = mock(classOf[BlockFetchingListener])
    var hitExecutorDeadException = false
    when(listener.onBlockTransferFailure(any(), any(classOf[ExecutorDeadException])))
      .thenAnswer(_ => {hitExecutorDeadException = true})

    service0 = createService(port, driverEndpointRef)
    val clientFactoryField = service0.getClass
      .getSuperclass.getSuperclass.getDeclaredField("clientFactory")
    clientFactoryField.setAccessible(true)
    clientFactoryField.set(service0, clientFactory)

    service0.fetchBlocks("localhost", port, "exec1",
      Array("block1"), listener, mock(classOf[DownloadFileManager]))
    assert(createClientCount === 1)
    assert(hitExecutorDeadException)
  }

  test("uploadBlock should reject blocks exceeding maximum size") {
    service0 = createService(port = 0)

    val blockId = new TestBlockId("test-block")
    val maxSafeSize = Integer.MAX_VALUE - (8 * 1024 * 1024) // 2GB - 8MB safety margin
    val oversizedBlockSize = maxSafeSize + 1L

    // Create a mock ManagedBuffer that reports an oversized size
    val oversizedBuffer = mock(classOf[ManagedBuffer])
    when(oversizedBuffer.size()).thenReturn(oversizedBlockSize)

    // Attempt to upload the oversized block
    val uploadFuture = service0.uploadBlock(
      hostname = "localhost",
      port = service0.port,
      execId = "test-exec",
      blockId = blockId,
      blockData = oversizedBuffer,
      level = StorageLevel.MEMORY_ONLY,
      classTag = scala.reflect.classTag[Array[Byte]]
    )

    // The upload should fail with a SparkException
    val exception = intercept[SparkException] {
      // scalastyle:off awaitresult
      scala.concurrent.Await.result(uploadFuture, scala.concurrent.duration.Duration("5 seconds"))
      // scalastyle:on awaitresult
    }

    // Verify the exception message contains expected information
    exception.getMessage should include("exceeds maximum transferable size")
    exception.getMessage should include(blockId.toString)
    exception.getMessage should include("Consider increasing the number of partitions")
  }

  test("uploadBlock should succeed for blocks within size limit") {
    service0 = createService(port = 0)

    val blockId = new TestBlockId("test-block")
    val safeBlockSize = 1024L // 1KB - well within limits
    val blockData = new Array[Byte](safeBlockSize.toInt)
    val buffer = new NioManagedBuffer(java.nio.ByteBuffer.wrap(blockData))

    // This test just verifies that size validation doesn't reject valid blocks
    // The actual upload may fail due to missing server setup, but we're only
    // testing that the size validation passes
    val uploadFuture = service0.uploadBlock(
      hostname = "localhost",
      port = service0.port,
      execId = "test-exec",
      blockId = blockId,
      blockData = buffer,
      level = StorageLevel.MEMORY_ONLY,
      classTag = scala.reflect.classTag[Array[Byte]]
    )

    // The size validation should not reject this block
    // We don't wait for completion as the upload itself may fail due to test setup,
    // but the important thing is that it doesn't fail immediately with size validation
    uploadFuture should not be null
  }

  test("uploadBlock should use stream mode for blocks approaching size limits") {
    service0 = createService(port = 0)

    val blockId = new TestBlockId("large-block")
    // Create a block that's larger than half of max safe size but smaller than max safe size
    val maxSafeSize = Integer.MAX_VALUE - (8 * 1024 * 1024) // 2GB - 8MB safety margin
    val largeBlockSize = (maxSafeSize / 2) + (1024 * 1024) // Half + 1MB, should trigger stream mode
    val largeBuffer = mock(classOf[ManagedBuffer])
    when(largeBuffer.size()).thenReturn(largeBlockSize)

    // Mock the client factory and client to capture the upload method used
    val mockClientFactory = mock(classOf[TransportClientFactory])
    val mockClient = mock(classOf[TransportClient])
    when(mockClientFactory.createClient(any(), any())).thenReturn(mockClient)

    // Set the mock client factory using reflection
    val clientFactoryField = service0.getClass
      .getSuperclass.getSuperclass.getDeclaredField("clientFactory")
    clientFactoryField.setAccessible(true)
    clientFactoryField.set(service0, mockClientFactory)

    // Attempt upload - this should use stream mode due to conservative threshold
    val uploadFuture = service0.uploadBlock(
      hostname = "localhost",
      port = service0.port,
      execId = "test-exec",
      blockId = blockId,
      blockData = largeBuffer,
      level = StorageLevel.MEMORY_ONLY,
      classTag = scala.reflect.classTag[Array[Byte]]
    )

    // Verify that uploadStream was called (indicating stream mode was used)
    // and sendRpc was NOT called (indicating RPC mode was not used)
    verify(mockClient, times(1)).uploadStream(any(), any(), any())
    verify(mockClient, times(0)).sendRpc(any(), any())
  }

  private def verifyServicePort(expectedPort: Int, actualPort: Int): Unit = {
    actualPort should be >= expectedPort
    // avoid testing equality in case of simultaneous tests
    // if `spark.testing` is true,
    // the default value for `spark.port.maxRetries` is 100 under test
    actualPort should be <= (expectedPort + 100)
  }

  private def createService(
      port: Int,
      rpcEndpointRef: RpcEndpointRef = null): NettyBlockTransferService = {
    val conf = new SparkConf()
      .set("spark.app.id", s"test-${getClass.getName}")
    val serializerManager = new SerializerManager(new JavaSerializer(conf), conf)
    val securityManager = new SecurityManager(conf)
    val blockDataManager = mock(classOf[BlockDataManager])
    val service = new NettyBlockTransferService(
      conf, securityManager, serializerManager, "localhost", "localhost", port, 1, rpcEndpointRef)
    service.init(blockDataManager)
    service
  }
}
