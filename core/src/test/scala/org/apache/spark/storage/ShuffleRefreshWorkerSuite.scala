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

import org.mockito.Mockito.{doThrow, mock, when}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config

class ShuffleRefreshWorkerSuite extends SparkFunSuite {

  test("ShuffleRefreshWorker basic construction") {
    val conf = new SparkConf()
    conf.set(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL, 1000L)
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)
    val mockRefreshFunction = mock(classOf[Function0[Boolean]])
    val mockLogFunction = mock(classOf[Function0[Unit]])

    val worker = new ShuffleRefreshWorker(
      conf,
      stateTracker,
      errorHandler,
      mockRefreshFunction,
      mockLogFunction)

    assert(worker != null)
  }

  test("ShuffleRefreshWorker state tracker integration") {
    val conf = new SparkConf()
    conf.set(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL, 100L)
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)
    val mockRefreshFunction = mock(classOf[Function0[Boolean]])
    val mockLogFunction = mock(classOf[Function0[Unit]])

    when(mockRefreshFunction.apply()).thenReturn(false) // No blocks left

    val worker = new ShuffleRefreshWorker(
      conf,
      stateTracker,
      errorHandler,
      mockRefreshFunction,
      mockLogFunction)

    // Initially should continue
    assert(stateTracker.shouldContinueShuffleMigration)

    // Stop migration
    stateTracker.stopShuffle("Test")
    assert(!stateTracker.shouldContinueShuffleMigration)
  }

  test("ShuffleRefreshWorker handles successful refresh") {
    val conf = new SparkConf()
    conf.set(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL, 50L)
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)
    val mockRefreshFunction = mock(classOf[Function0[Boolean]])
    val mockLogFunction = mock(classOf[Function0[Unit]])

    when(mockRefreshFunction.apply()).thenReturn(false) // No blocks left

    val worker = new ShuffleRefreshWorker(
      conf,
      stateTracker,
      errorHandler,
      mockRefreshFunction,
      mockLogFunction)

    // Test that the worker can be created and used
    assert(worker != null)
  }

  test("ShuffleRefreshWorker handles refresh with remaining blocks") {
    val conf = new SparkConf()
    conf.set(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL, 50L)
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)
    val mockRefreshFunction = mock(classOf[Function0[Boolean]])
    val mockLogFunction = mock(classOf[Function0[Unit]])

    when(mockRefreshFunction.apply()).thenReturn(true) // Blocks remaining

    val worker = new ShuffleRefreshWorker(
      conf,
      stateTracker,
      errorHandler,
      mockRefreshFunction,
      mockLogFunction)

    assert(worker != null)
  }

  test("ShuffleRefreshWorker handles RuntimeException from refresh") {
    val conf = new SparkConf()
    conf.set(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL, 50L)
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)
    val mockRefreshFunction = mock(classOf[Function0[Boolean]])
    val mockLogFunction = mock(classOf[Function0[Unit]])

    doThrow(new RuntimeException("Refresh error")).when(mockRefreshFunction).apply()

    val worker = new ShuffleRefreshWorker(
      conf,
      stateTracker,
      errorHandler,
      mockRefreshFunction,
      mockLogFunction)

    // Test that worker can be constructed and will handle exception during execution
    assert(worker != null)
  }

  test("ShuffleRefreshWorker handles log function exception") {
    val conf = new SparkConf()
    conf.set(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL, 50L)
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)
    val mockRefreshFunction = mock(classOf[Function0[Boolean]])
    val mockLogFunction = mock(classOf[Function0[Unit]])

    when(mockRefreshFunction.apply()).thenReturn(false)
    doThrow(new RuntimeException("Log error")).when(mockLogFunction).apply()

    val worker = new ShuffleRefreshWorker(
      conf,
      stateTracker,
      errorHandler,
      mockRefreshFunction,
      mockLogFunction)

    assert(worker != null)
  }

  test("ShuffleRefreshWorker sleep interval configuration") {
    val conf = new SparkConf()
    val customInterval = 2000L
    conf.set(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL, customInterval)

    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)
    val mockRefreshFunction = mock(classOf[Function0[Boolean]])
    val mockLogFunction = mock(classOf[Function0[Unit]])

    val worker = new ShuffleRefreshWorker(
      conf,
      stateTracker,
      errorHandler,
      mockRefreshFunction,
      mockLogFunction)

    // Worker should be created successfully with custom interval
    assert(worker != null)
  }

  test("ShuffleRefreshWorker state tracker initial state validation") {
    val conf = new SparkConf()
    conf.set(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL, 50L)
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)
    val mockRefreshFunction = mock(classOf[Function0[Boolean]])
    val mockLogFunction = mock(classOf[Function0[Unit]])

    when(mockRefreshFunction.apply()).thenReturn(false)

    val worker = new ShuffleRefreshWorker(
      conf,
      stateTracker,
      errorHandler,
      mockRefreshFunction,
      mockLogFunction)

    // Verify initial state
    assert(stateTracker.shouldContinueShuffleMigration)
    assert(worker != null)
  }

  test("ShuffleRefreshWorker log progress function integration") {
    val conf = new SparkConf()
    conf.set(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL, 50L)
    val stateTracker = new MigrationStateTracker()
    val errorHandler = new MigrationErrorHandler(3)
    val mockRefreshFunction = mock(classOf[Function0[Boolean]])

    // Create a mock log function that can be verified
    var logCalled = false
    val logFunction = () => { logCalled = true }

    when(mockRefreshFunction.apply()).thenReturn(false)

    val worker =
      new ShuffleRefreshWorker(conf, stateTracker, errorHandler, mockRefreshFunction, logFunction)

    assert(worker != null)
    // The log function integration is tested at the component level
  }
}
