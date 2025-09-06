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

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config
import org.apache.spark.shuffle.IndexShuffleBlockResolver
import org.apache.spark.storage.BlockManagerMessages.ReplicateBlock

class MigrationStrategySuite extends SparkFunSuite with Matchers {

  test("RDDMigrationStrategy should have correct strategy name") {
    val strategy = new RDDMigrationStrategy()
    strategy.strategyName should be("RDD Cache Blocks")
  }

  test("ShuffleMigrationStrategy should have correct strategy name") {
    val strategy = new ShuffleMigrationStrategy()
    strategy.strategyName should be("Shuffle Blocks")
  }

  test("RDDMigrationStrategy should return false when no blocks to migrate") {
    val strategy = new RDDMigrationStrategy()
    val mockBlockManager = mock(classOf[BlockManager])
    val conf = new SparkConf()

    when(mockBlockManager.getMigratableRDDBlocks()).thenReturn(Seq.empty)

    val result = strategy.migrateBlocks(mockBlockManager, conf)
    result should be(false)
  }

  test("ShuffleMigrationStrategy should return false when no blocks to migrate") {
    val strategy = new ShuffleMigrationStrategy()
    val mockBlockManager = mock(classOf[BlockManager])
    val mockResolver = mock(classOf[IndexShuffleBlockResolver])
    val conf = new SparkConf()

    when(mockBlockManager.migratableResolver).thenReturn(mockResolver)
    when(mockResolver.getStoredShuffles()).thenReturn(Seq.empty)

    val result = strategy.migrateBlocks(mockBlockManager, conf)
    result should be(false)
  }

  test("RDDMigrationStrategy should return true when blocks fail to migrate") {
    val strategy = new RDDMigrationStrategy()
    val mockBlockManager = mock(classOf[BlockManager])
    val conf = new SparkConf()
      .set(config.STORAGE_DECOMMISSION_MAX_REPLICATION_FAILURE_PER_BLOCK, 3)

    val blockId = TestBlockId("test")
    val replicateBlock = ReplicateBlock(blockId, Seq.empty, 2)

    when(mockBlockManager.getMigratableRDDBlocks()).thenReturn(Seq(replicateBlock))
    when(mockBlockManager.replicateBlock(any(), any(), any(), any())).thenReturn(false)

    val result = strategy.migrateBlocks(mockBlockManager, conf)
    result should be(true)
  }

  test("RDDMigrationStrategy should return false when all blocks migrate successfully") {
    val strategy = new RDDMigrationStrategy()
    val mockBlockManager = mock(classOf[BlockManager])
    val conf = new SparkConf()
      .set(config.STORAGE_DECOMMISSION_MAX_REPLICATION_FAILURE_PER_BLOCK, 3)

    val blockId = TestBlockId("test")
    val replicateBlock = ReplicateBlock(blockId, Seq.empty, 2)

    when(mockBlockManager.getMigratableRDDBlocks()).thenReturn(Seq(replicateBlock))
    when(mockBlockManager.replicateBlock(any(), any(), any(), any())).thenReturn(true)

    val result = strategy.migrateBlocks(mockBlockManager, conf)
    result should be(false)

    // Verify that removeBlock was called after successful replication
    verify(mockBlockManager).removeBlock(blockId)
  }
}
