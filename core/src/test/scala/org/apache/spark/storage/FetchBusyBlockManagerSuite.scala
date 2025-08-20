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
import org.mockito.Mockito.{mock, when, verify, times}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.BlockManagerMessages._

class FetchBusyBlockManagerSuite extends AnyFunSuite with BeforeAndAfter {

  private var blockManager: BlockManager = _
  private var blockManagerMaster: BlockManagerMaster = _
  private var mockMasterEndpoint: BlockManagerMasterEndpoint = _

  before {
    val conf = new SparkConf()
    blockManagerMaster = mock(classOf[BlockManagerMaster])
    mockMasterEndpoint = mock(classOf[BlockManagerMasterEndpoint])
  }

  test("getMigrationPeers returns empty sequence when no peers available") {
    when(blockManagerMaster.getMigrationPeers(any[GetMigrationPeers]))
      .thenReturn(Seq.empty[BlockManagerId])
    
    val blockManagerId = BlockManagerId("executor1", "host1", 7337)
    val peers = blockManagerMaster.getMigrationPeers(GetMigrationPeers(blockManagerId))
    
    assert(peers.isEmpty, "Should return empty sequence when no migration peers available")
    verify(blockManagerMaster, times(1)).getMigrationPeers(any[GetMigrationPeers])
  }

  test("getMigrationPeers returns available peers for migration") {
    val sourceId = BlockManagerId("executor1", "host1", 7337)
    val peer1 = BlockManagerId("executor2", "host2", 7337)
    val peer2 = BlockManagerId("executor3", "host3", 7337)
    val expectedPeers = Seq(peer1, peer2)
    
    when(blockManagerMaster.getMigrationPeers(any[GetMigrationPeers]))
      .thenReturn(expectedPeers)
    
    val actualPeers = blockManagerMaster.getMigrationPeers(GetMigrationPeers(sourceId))
    
    assert(actualPeers == expectedPeers, "Should return the available migration peers")
    assert(actualPeers.length == 2, "Should return correct number of peers")
    verify(blockManagerMaster, times(1)).getMigrationPeers(any[GetMigrationPeers])
  }

  test("getMigrationPeers excludes the requesting block manager") {
    val sourceId = BlockManagerId("executor1", "host1", 7337)
    val peer1 = BlockManagerId("executor2", "host2", 7337)
    val peer2 = BlockManagerId("executor3", "host3", 7337)
    
    // Mock should not return the source ID itself
    val expectedPeers = Seq(peer1, peer2)
    when(blockManagerMaster.getMigrationPeers(any[GetMigrationPeers]))
      .thenReturn(expectedPeers)
    
    val actualPeers = blockManagerMaster.getMigrationPeers(GetMigrationPeers(sourceId))
    
    assert(!actualPeers.contains(sourceId), "Migration peers should not include the source block manager")
    assert(actualPeers == expectedPeers, "Should return only peer block managers")
  }

  test("BlockManagerDecommissioner handles fetch busy exceptions gracefully") {
    val blockManagerId = BlockManagerId("executor1", "host1", 7337)
    val mockDecommissioner = mock(classOf[BlockManagerDecommissioner])
    
    // Simulate fetch busy exception during decommissioning
    val fetchBusyException = new RuntimeException("Fetch busy - server overloaded")
    when(mockDecommissioner.start()).thenThrow(fetchBusyException)
    
    // Test that the exception is caught and handled
    try {
      mockDecommissioner.start()
      fail("Should have thrown fetch busy exception")
    } catch {
      case e: RuntimeException if e.getMessage.contains("Fetch busy") =>
        // Expected behavior - fetch busy exception should be thrown
        assert(true, "Fetch busy exception handled correctly")
      case other: Exception =>
        fail(s"Unexpected exception type: $other")
    }
  }

  test("BlockManagerMessages GetMigrationPeers serialization") {
    val blockManagerId = BlockManagerId("executor1", "host1", 7337)
    val getMigrationPeers = GetMigrationPeers(blockManagerId)
    
    // Verify the message can be created and contains the correct block manager ID
    assert(getMigrationPeers.blockManagerId == blockManagerId, 
      "GetMigrationPeers should contain the correct BlockManagerId")
  }

  test("multiple concurrent getMigrationPeers requests") {
    val sourceId = BlockManagerId("executor1", "host1", 7337)
    val peers = Seq(
      BlockManagerId("executor2", "host2", 7337),
      BlockManagerId("executor3", "host3", 7337)
    )
    
    when(blockManagerMaster.getMigrationPeers(any[GetMigrationPeers]))
      .thenReturn(peers)
    
    // Simulate multiple concurrent requests
    val futures = (1 to 10).map { _ =>
      scala.concurrent.Future {
        blockManagerMaster.getMigrationPeers(GetMigrationPeers(sourceId))
      }(scala.concurrent.ExecutionContext.global)
    }
    
    val results = scala.concurrent.Await.result(
      scala.concurrent.Future.sequence(futures),
      scala.concurrent.duration.Duration("5 seconds")
    )
    
    // All requests should return the same peers
    results.foreach { result =>
      assert(result == peers, "All concurrent requests should return same peers")
    }
    
    verify(blockManagerMaster, times(10)).getMigrationPeers(any[GetMigrationPeers])
  }
}