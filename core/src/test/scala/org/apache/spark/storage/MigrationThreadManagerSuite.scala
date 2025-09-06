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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config

class MigrationThreadManagerSuite extends SparkFunSuite {

  test("MigrationThreadManager initialization") {
    val conf = new SparkConf()
    val manager = new MigrationThreadManager(conf)

    manager.initialize()

    assert(manager.rddBlockMigrationExecutor != null)
    assert(manager.shuffleBlockMigrationRefreshExecutor != null)
    assert(manager.shuffleMigrationPool != null)
    assert(!manager.isShutdown)

    manager.shutdown()
  }

  test("MigrationThreadManager shutdown") {
    val conf = new SparkConf()
    val manager = new MigrationThreadManager(conf)

    manager.initialize()
    assert(!manager.isShutdown)

    manager.shutdown()
    assert(manager.isShutdown)
  }

  test("MigrationThreadManager requires initialization before use") {
    val conf = new SparkConf()
    val manager = new MigrationThreadManager(conf)

    intercept[IllegalArgumentException] {
      manager.rddBlockMigrationExecutor
    }

    intercept[IllegalArgumentException] {
      manager.shuffleBlockMigrationRefreshExecutor
    }

    intercept[IllegalArgumentException] {
      manager.shuffleMigrationPool
    }
  }

  test("MigrationThreadManager thread pool configuration") {
    val conf = new SparkConf()
      .set(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL, 500L)

    val manager = new MigrationThreadManager(conf)
    manager.initialize()

    val rddExecutor = manager.rddBlockMigrationExecutor
    assert(rddExecutor.getCorePoolSize >= 1)

    manager.shutdown()
  }

  test("MigrationThreadManager status reporting") {
    val conf = new SparkConf()
    val manager = new MigrationThreadManager(conf)

    val uninitializedStatus = manager.getThreadPoolStatus
    assert(uninitializedStatus.contains("not initialized"))

    manager.initialize()
    val initializedStatus = manager.getThreadPoolStatus
    assert(initializedStatus.contains("RDD:"))
    assert(initializedStatus.contains("Shuffle refresh:"))
    assert(initializedStatus.contains("Shuffle migration:"))

    manager.shutdown()
    val shutdownStatus = manager.getThreadPoolStatus
    assert(shutdownStatus.contains("shutdown"))
  }

  test("MigrationThreadManager handles multiple shutdown calls") {
    val conf = new SparkConf()
    val manager = new MigrationThreadManager(conf)

    manager.initialize()
    manager.shutdown()
    assert(manager.isShutdown)

    // Should not throw exception
    manager.shutdown()
    assert(manager.isShutdown)
  }

  test("MigrationThreadManager custom parallelism configuration") {
    val conf = new SparkConf()
      .set("spark.storage.decommission.shuffleBlocks.migrationParallelism", "4")
      .set(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL, 200L)

    val manager = new MigrationThreadManager(conf)
    manager.initialize()

    val rddExecutor = manager.rddBlockMigrationExecutor
    assert(rddExecutor.getCorePoolSize >= 1)

    manager.shutdown()
  }
}
