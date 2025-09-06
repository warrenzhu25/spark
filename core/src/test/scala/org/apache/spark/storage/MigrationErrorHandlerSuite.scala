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

import java.io.{FileNotFoundException, IOException}

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.shuffle.ShuffleBlockInfo

class MigrationErrorHandlerSuite extends SparkFunSuite {

  test("MigrationErrorHandler handle IOException with retry") {
    val handler = new MigrationErrorHandler(maxReplicationFailures = 3)
    val shuffleBlock = ShuffleBlockInfo(1, 0)
    val peer = "peer1"
    val retryCount = 1

    val ioException = new IOException("Network error")
    val (error, action) =
      handler.handleMigrationException(ioException, shuffleBlock, peer, retryCount)

    assert(error.isInstanceOf[handler.RetryableError])
    assert(action == handler.RetryMigration)
  }

  test("MigrationErrorHandler handle SparkException with retry") {
    val handler = new MigrationErrorHandler(maxReplicationFailures = 2)
    val shuffleBlock = ShuffleBlockInfo(2, 1)
    val peer = "peer2"
    val retryCount = 0

    val sparkException = new SparkException("Block not found")
    val (error, action) =
      handler.handleMigrationException(sparkException, shuffleBlock, peer, retryCount)

    assert(error.isInstanceOf[handler.RetryableError])
    assert(action == handler.RetryMigration)
  }

  test("MigrationErrorHandler exceed max retries") {
    val handler = new MigrationErrorHandler(maxReplicationFailures = 2)
    val shuffleBlock = ShuffleBlockInfo(1, 0)
    val peer = "peer1"
    val retryCount = 2 // At the limit

    val ioException = new IOException("Persistent error")
    val (error, action) =
      handler.handleMigrationException(ioException, shuffleBlock, peer, retryCount)

    assert(error.isInstanceOf[handler.RetryableError])
    assert(action == handler.AbortMigration)
  }

  test("MigrationErrorHandler handle InterruptedException") {
    val handler = new MigrationErrorHandler(maxReplicationFailures = 3)
    val shuffleBlock = ShuffleBlockInfo(1, 0)
    val peer = "peer1"
    val retryCount = 0

    val interruptedException = new InterruptedException("Thread interrupted")
    val (error, action) =
      handler.handleMigrationException(interruptedException, shuffleBlock, peer, retryCount)

    assert(error == handler.InterruptionError)
    assert(action == handler.StopAllMigration)
  }

  test("MigrationErrorHandler handle unexpected exception") {
    val handler = new MigrationErrorHandler(maxReplicationFailures = 3)
    val shuffleBlock = ShuffleBlockInfo(1, 0)
    val peer = "peer1"
    val retryCount = 1

    val runtimeException = new RuntimeException("Unexpected error")
    val (error, action) =
      handler.handleMigrationException(runtimeException, shuffleBlock, peer, retryCount)

    assert(error.isInstanceOf[handler.FatalError])
    assert(action == handler.AbortMigration)
  }

  test("MigrationErrorHandler shouldRetryBlock logic") {
    val handler = new MigrationErrorHandler(maxReplicationFailures = 3)

    // Should retry when below limit
    assert(handler.shouldRetryBlock(0))
    assert(handler.shouldRetryBlock(1))
    assert(handler.shouldRetryBlock(2))

    // Should not retry when at or above limit
    assert(!handler.shouldRetryBlock(3))
    assert(!handler.shouldRetryBlock(4))
  }

  test("MigrationErrorHandler handleGeneralMigrationError") {
    val handler = new MigrationErrorHandler(maxReplicationFailures = 3)
    val exception = new RuntimeException("General error")
    val context = "test context"

    val action = handler.handleGeneralMigrationError(exception, context)
    assert(action == handler.AbortMigration)
  }

  test("MigrationErrorHandler logRetryDecision") {
    val handler = new MigrationErrorHandler(maxReplicationFailures = 3)
    val shuffleBlock = ShuffleBlockInfo(1, 0)

    // Should not throw exception
    handler.logRetryDecision(shuffleBlock, 1, true)
    handler.logRetryDecision(shuffleBlock, 3, false)
  }

  test("MigrationErrorHandler retry count boundary conditions") {
    val handler = new MigrationErrorHandler(maxReplicationFailures = 5)
    val shuffleBlock = ShuffleBlockInfo(1, 0)
    val peer = "peer1"

    // Test with max retries = 5: should retry for counts 0,1,2,3,4 and abort at 5
    val ioException = new IOException("Network error")
    val (_, action4) = handler.handleMigrationException(ioException, shuffleBlock, peer, 4)
    assert(action4 == handler.RetryMigration)
    val (_, action5) = handler.handleMigrationException(ioException, shuffleBlock, peer, 5)
    assert(action5 == handler.AbortMigration)
  }

  test("MigrationErrorHandler handle direct FileNotFoundException") {
    val handler = new MigrationErrorHandler(maxReplicationFailures = 3)
    val shuffleBlock = ShuffleBlockInfo(1, 0)
    val peer = "peer1"
    val retryCount = 1

    val fileNotFoundException = new FileNotFoundException("File not found")
    val (error, action) =
      handler.handleMigrationException(fileNotFoundException, shuffleBlock, peer, retryCount)

    assert(error.isInstanceOf[handler.FileNotFoundError])
    assert(action == handler.SkipMigration)
  }

  test("MigrationErrorHandler handle wrapped FileNotFoundException in IOException") {
    val handler = new MigrationErrorHandler(maxReplicationFailures = 3)
    val shuffleBlock = ShuffleBlockInfo(1, 0)
    val peer = "peer1"
    val retryCount = 1

    // Test scenario from SPARK-40168 where FileNotFoundException is wrapped in IOException
    val wrappedException = new IOException("boop", new FileNotFoundException("file not found"))
    val (error, action) =
      handler.handleMigrationException(wrappedException, shuffleBlock, peer, retryCount)

    assert(error.isInstanceOf[handler.FileNotFoundError])
    assert(action == handler.SkipMigration)
  }

  test("MigrationErrorHandler handle wrapped FileNotFoundException in SparkException") {
    val handler = new MigrationErrorHandler(maxReplicationFailures = 3)
    val shuffleBlock = ShuffleBlockInfo(1, 0)
    val peer = "peer1"
    val retryCount = 1

    val wrappedException = new SparkException("Task failed",
      new FileNotFoundException("file not found"))
    val (error, action) =
      handler.handleMigrationException(wrappedException, shuffleBlock, peer, retryCount)

    assert(error.isInstanceOf[handler.FileNotFoundError])
    assert(action == handler.SkipMigration)
  }

  test("MigrationErrorHandler handle deeply nested FileNotFoundException") {
    val handler = new MigrationErrorHandler(maxReplicationFailures = 3)
    val shuffleBlock = ShuffleBlockInfo(1, 0)
    val peer = "peer1"
    val retryCount = 1

    // Test deeply nested exception chain
    val deeplyNested = new SparkException("outer",
      new IOException("middle", new FileNotFoundException("deeply nested file not found")))
    val (error, action) =
      handler.handleMigrationException(deeplyNested, shuffleBlock, peer, retryCount)

    assert(error.isInstanceOf[handler.FileNotFoundError])
    assert(action == handler.SkipMigration)
  }

  test("MigrationErrorHandler handle IOException without FileNotFoundException cause") {
    val handler = new MigrationErrorHandler(maxReplicationFailures = 3)
    val shuffleBlock = ShuffleBlockInfo(1, 0)
    val peer = "peer1"
    val retryCount = 1

    // Regular IOException without FileNotFoundException should be retryable
    val regularIOException = new IOException("Network timeout")
    val (error, action) =
      handler.handleMigrationException(regularIOException, shuffleBlock, peer, retryCount)

    assert(error.isInstanceOf[handler.RetryableError])
    assert(action == handler.RetryMigration)
  }

  test("MigrationErrorHandler FileNotFoundException ignores retry count") {
    val handler = new MigrationErrorHandler(maxReplicationFailures = 2)
    val shuffleBlock = ShuffleBlockInfo(1, 0)
    val peer = "peer1"

    // FileNotFoundException should always result in SkipMigration regardless of retry count
    val fileNotFoundException = new FileNotFoundException("File deleted")
    val (error1, action1) =
      handler.handleMigrationException(fileNotFoundException, shuffleBlock, peer, 0)
    assert(error1.isInstanceOf[handler.FileNotFoundError])
    assert(action1 == handler.SkipMigration)

    val (error2, action2) =
      handler.handleMigrationException(fileNotFoundException, shuffleBlock, peer, 2)
    assert(error2.isInstanceOf[handler.FileNotFoundError])
    assert(action2 == handler.SkipMigration)

    val (error3, action3) =
      handler.handleMigrationException(fileNotFoundException, shuffleBlock, peer, 10)
    assert(error3.isInstanceOf[handler.FileNotFoundError])
    assert(action3 == handler.SkipMigration)
  }

  test("MigrationErrorHandler circular exception chain handling") {
    val handler = new MigrationErrorHandler(maxReplicationFailures = 3)
    val shuffleBlock = ShuffleBlockInfo(1, 0)
    val peer = "peer1"
    val retryCount = 1

    // Create a circular reference to test infinite loop protection
    val ioException = new IOException("Circular reference test")
    val sparkException = new SparkException("Wrapper", ioException)
    // Note: We can't actually create true circular references in constructor,
    // but we can test that non-FileNotFoundException chains are handled correctly
    val (error, action) =
      handler.handleMigrationException(sparkException, shuffleBlock, peer, retryCount)

    assert(error.isInstanceOf[handler.RetryableError])
    assert(action == handler.RetryMigration)
  }
}
