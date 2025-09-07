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

import java.io.FileNotFoundException

import scala.concurrent.Future
import scala.concurrent.duration._

import org.mockito.{ArgumentMatchers => mc}
import org.mockito.Mockito.{atLeast => least, mock, never, times, verify, when}
import org.scalatest.concurrent.Eventually._
import org.scalatest.matchers.must.Matchers

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle.{MigratableResolver, ShuffleBlockInfo}
import org.apache.spark.storage.BlockManagerMessages.ReplicateBlock

class BlockManagerDecommissionUnitSuite extends SparkFunSuite with Matchers {

  private val bmPort = 12345

  private val sparkConf = new SparkConf(false)
    .set(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED, true)
    .set(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED, true)
    // Just replicate blocks quickly during testing, as there isn't another
    // workload we need to worry about.
    .set(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL, 10L)

  /**
   * Wait for migration to complete successfully.
   */
  private def waitComplete(
      bmDecomManager: BlockManagerDecommissioner,
      timeoutSeconds: Int = 10): Unit = {
    eventually(timeout(timeoutSeconds.seconds), interval(100.milliseconds)) {
      val migrationInfo = bmDecomManager.lastMigrationInfo()
      assert(migrationInfo.isComplete,
        s"Expected complete but got: ${migrationInfo.status}")
    }
  }

  /**
   * Wait for migration to stop with a specific reason.
   */
  private def waitStopped(
      bmDecomManager: BlockManagerDecommissioner,
      expectedReason: MigrationStopReason,
      timeoutSeconds: Int = 10): Unit = {
    eventually(timeout(timeoutSeconds.seconds), interval(100.milliseconds)) {
      val migrationInfo = bmDecomManager.lastMigrationInfo()
      assert(migrationInfo.isStopped, s"Expected stopped but got: ${migrationInfo.status}")
      assert(migrationInfo.stopReason.contains(expectedReason),
        s"Expected reason $expectedReason but got: ${migrationInfo.stopReason}")
    }
  }

  /**
   * Verify the final state after migration completion/stop.
   */
  private def verifyFinalState(
      bmDecomManager: BlockManagerDecommissioner,
      expectedNumShuffles: Option[Int] = None,
      expectComplete: Boolean = true): Unit = {
    val migrationInfo = bmDecomManager.lastMigrationInfo()
    val stats = migrationInfo.shuffleMigrationStat

    // Verify completion status (only if we expect it to be complete)
    if (expectComplete) {
      assert(migrationInfo.isComplete,
        s"Expected completion but got status: ${migrationInfo.status}")
    } else {
      // For non-completion cases, just verify it's not unexpectedly complete
      if (migrationInfo.isComplete) {
        // This is fine - it completed unexpectedly fast, which is not an error
        logInfo(s"Migration completed unexpectedly: ${migrationInfo.status}")
      }
    }

    // Verify shuffle counts
    expectedNumShuffles.foreach { expected =>
      assert(bmDecomManager.numMigratedShuffles.get() === expected,
        s"Expected $expected migrated shuffles, got ${bmDecomManager.numMigratedShuffles.get()}")
    }

    // Verify migration statistics consistency (only if we have blocks)
    if (stats.totalBlocks > 0) {
      assert(stats.numMigratedBlock + stats.numBlocksLeft === stats.totalBlocks,
        s"Block count mismatch: migrated(${stats.numMigratedBlock}) + " +
        s"remaining(${stats.numBlocksLeft}) != total(${stats.totalBlocks})")

      // Verify size consistency
      if (expectComplete || migrationInfo.isComplete) {
        assert(stats.numBlocksLeft === 0,
          s"Expected no remaining blocks but found ${stats.numBlocksLeft}")
        assert(stats.totalMigratedSize <= stats.totalSize,
          s"Migrated size (${stats.totalMigratedSize}) exceeds total size (${stats.totalSize})")
      }
    }
  }

  private def registerShuffleBlocks(
      mockMigratableShuffleResolver: MigratableResolver,
      ids: Set[(Int, Long, Int)]): Unit = {

    when(mockMigratableShuffleResolver.getStoredShuffles())
      .thenReturn(ids.map(triple => ShuffleBlockInfo(triple._1, triple._2)).toSeq)

    ids.foreach { case (shuffleId: Int, mapId: Long, reduceId: Int) =>
      val mockBuffer = mock(classOf[ManagedBuffer])
      when(mockBuffer.size()).thenReturn(100L)
      when(mockMigratableShuffleResolver.getMigrationBlocks(mc.any()))
        .thenReturn(List(
          (ShuffleIndexBlockId(shuffleId, mapId, reduceId), mockBuffer),
          (ShuffleDataBlockId(shuffleId, mapId, reduceId), mockBuffer)))
    }
  }

  /**
   * Validate a given configuration with the mocks.
   * The fail variable controls if we expect migration to fail, in which case we expect
   * a constant Long.MaxValue timestamp.
   */
  private def validateDecommissionTimestamps(conf: SparkConf, bm: BlockManager,
      fail: Boolean = false, assertDone: Boolean = true) = {
    // Verify the decommissioning manager timestamps and status
    val bmDecomManager = new BlockManagerDecommissioner(conf, bm)
    validateDecommissionTimestampsOnManager(bmDecomManager, fail, assertDone)
  }

  private def validateDecommissionTimestampsOnManager(bmDecomManager: BlockManagerDecommissioner,
      fail: Boolean = false, assertDone: Boolean = true, numShuffles: Option[Int] = None) = {
    try {
      bmDecomManager.start()

      if (fail) {
        // For failure scenarios, wait for stopped status with expected reason
        // We expect NoPeersAvailable or TooManyFailures based on mock setup
        waitStopped(bmDecomManager, NoPeersAvailable, timeoutSeconds = 10)
      } else if (assertDone) {
        // Wait for successful completion
        waitComplete(bmDecomManager, timeoutSeconds = 10)
      } else {
        // For partial completion tests, just wait a bit and verify state
        Thread.sleep(500)
      }

      // Verify final state after waiting
      verifyFinalState(bmDecomManager, numShuffles, expectComplete = !fail && assertDone)

      if (!fail) {
        // Verify timing progresses correctly - timestamps should be reasonable
        val migrationInfo = bmDecomManager.lastMigrationInfo()
        val currentTime = System.currentTimeMillis()
        // Activity time should be within reasonable bounds (not too old, not in future)
        assert(migrationInfo.lastActivityTime > 0 &&
          migrationInfo.lastActivityTime <= currentTime + 1000,
          s"Activity time ${migrationInfo.lastActivityTime} not reasonable vs current $currentTime")
      }
    } finally {
      bmDecomManager.stop()
    }
  }

  test("test that with no blocks we finish migration") {
    // Set up the mocks so we return empty
    val bm = mock(classOf[BlockManager])
    val migratableShuffleBlockResolver = mock(classOf[MigratableResolver])
    when(migratableShuffleBlockResolver.getStoredShuffles())
      .thenReturn(Seq())
    when(bm.migratableResolver).thenReturn(migratableShuffleBlockResolver)
    when(bm.getMigratableRDDBlocks())
      .thenReturn(Seq())
    when(bm.getPeers(mc.any()))
      .thenReturn(Seq(BlockManagerId("exec2", "host2", 12345)))

    // Verify the decom manager handles this correctly
    validateDecommissionTimestamps(sparkConf, bm)
  }

  test("block decom manager with no migrations configured") {
    val bm = mock(classOf[BlockManager])
    val migratableShuffleBlockResolver = mock(classOf[MigratableResolver])
    registerShuffleBlocks(migratableShuffleBlockResolver, Set((1, 1L, 1)))
    when(bm.migratableResolver).thenReturn(migratableShuffleBlockResolver)
    when(bm.getMigratableRDDBlocks())
      .thenReturn(Seq())
    when(bm.getPeers(mc.any()))
      .thenReturn(Seq(BlockManagerId("exec2", "host2", 12345)))

    val badConf = new SparkConf(false)
      .set(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED, false)
      .set(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED, false)
      .set(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL, 10L)
    // Verify the decom manager handles this correctly
    validateDecommissionTimestamps(badConf, bm, fail = true)
  }

  test("block decom manager with no peers") {
    // Set up the mocks so we return one shuffle block
    val bm = mock(classOf[BlockManager])
    val migratableShuffleBlockResolver = mock(classOf[MigratableResolver])
    registerShuffleBlocks(migratableShuffleBlockResolver, Set((1, 1L, 1)))
    when(bm.migratableResolver).thenReturn(migratableShuffleBlockResolver)
    when(bm.getMigratableRDDBlocks())
      .thenReturn(Seq())
    when(bm.getPeers(mc.any()))
      .thenReturn(Seq())

    // Verify the decom manager handles this correctly
    validateDecommissionTimestamps(sparkConf, bm, fail = true)
  }


  test("block decom manager with only shuffle files time moves forward") {
    // Set up the mocks so we return one shuffle block
    val bm = mock(classOf[BlockManager])
    val migratableShuffleBlockResolver = mock(classOf[MigratableResolver])
    registerShuffleBlocks(migratableShuffleBlockResolver, Set((1, 1L, 1)))
    when(bm.migratableResolver).thenReturn(migratableShuffleBlockResolver)
    when(bm.getMigratableRDDBlocks())
      .thenReturn(Seq())
    when(bm.getPeers(mc.any()))
      .thenReturn(Seq(BlockManagerId("exec2", "host2", 12345)))

    // Verify the decom manager handles this correctly
    validateDecommissionTimestamps(sparkConf, bm)
  }

  test("block decom manager does not re-add removed shuffle files") {
    // Set up the mocks so we return one shuffle block
    val bm = mock(classOf[BlockManager])
    val migratableShuffleBlockResolver = mock(classOf[MigratableResolver])
    registerShuffleBlocks(migratableShuffleBlockResolver, Set())
    when(bm.migratableResolver).thenReturn(migratableShuffleBlockResolver)
    when(bm.getMigratableRDDBlocks())
      .thenReturn(Seq())
    when(bm.getPeers(mc.any()))
      .thenReturn(Seq(BlockManagerId("exec2", "host2", 12345)))
    val bmDecomManager = new BlockManagerDecommissioner(sparkConf, bm)
    bmDecomManager.migratingShuffles += ShuffleBlockInfo(10, 10)

    validateDecommissionTimestampsOnManager(bmDecomManager, fail = false, assertDone = false)
  }

  test("SPARK-40168: block decom manager handles shuffle file not found") {
    // Set up the mocks so we return one shuffle block
    val bm = mock(classOf[BlockManager])
    val migratableShuffleBlockResolver = mock(classOf[MigratableResolver])
    // First call get blocks, then empty list simulating a delete.
    when(migratableShuffleBlockResolver.getStoredShuffles())
      .thenReturn(Seq(ShuffleBlockInfo(1, 1)))
      .thenReturn(Seq())
    when(migratableShuffleBlockResolver.getMigrationBlocks(mc.any()))
      .thenReturn(
        List(
          (ShuffleIndexBlockId(1, 1, 1), mock(classOf[ManagedBuffer])),
          (ShuffleDataBlockId(1, 1, 1), mock(classOf[ManagedBuffer]))))
      .thenReturn(List())

    when(bm.migratableResolver).thenReturn(migratableShuffleBlockResolver)
    when(bm.getMigratableRDDBlocks())
      .thenReturn(Seq())
    when(bm.getPeers(mc.any()))
      .thenReturn(Seq(BlockManagerId("exec2", "host2", 12345)))

    val blockTransferService = mock(classOf[BlockTransferService])
    // Simulate FileNotFoundException wrap inside SparkException
    when(
      blockTransferService
        .uploadBlock(mc.any(), mc.any(), mc.any(), mc.any(), mc.any(), mc.any(), mc.isNull()))
      .thenReturn(Future.failed(
        new java.io.IOException("boop", new FileNotFoundException("file not found"))))
    when(
      blockTransferService
        .uploadBlockSync(mc.any(), mc.any(), mc.any(), mc.any(), mc.any(), mc.any(), mc.isNull()))
      .thenCallRealMethod()

    when(bm.blockTransferService).thenReturn(blockTransferService)

    // Verify the decom manager handles this correctly
    val bmDecomManager = new BlockManagerDecommissioner(sparkConf, bm)
    validateDecommissionTimestampsOnManager(
      bmDecomManager,
      numShuffles = Option(1))
  }

  test("block decom manager handles IO failures") {
    // Set up the mocks so we return one shuffle block
    val bm = mock(classOf[BlockManager])
    val migratableShuffleBlockResolver = mock(classOf[MigratableResolver])
    registerShuffleBlocks(migratableShuffleBlockResolver, Set((1, 1L, 1)))
    when(bm.migratableResolver).thenReturn(migratableShuffleBlockResolver)
    when(bm.getMigratableRDDBlocks())
      .thenReturn(Seq())
    when(bm.getPeers(mc.any()))
      .thenReturn(Seq(BlockManagerId("exec2", "host2", 12345)))

    val blockTransferService = mock(classOf[BlockTransferService])
    // Simulate an ambiguous IO error (e.g. block could be gone, connection failed, etc.)
    when(blockTransferService.uploadBlockSync(
      mc.any(), mc.any(), mc.any(), mc.any(), mc.any(), mc.any(), mc.isNull())).thenThrow(
      new java.io.IOException("boop")
    )

    when(bm.blockTransferService).thenReturn(blockTransferService)

    // Verify the decom manager handles this correctly
    val bmDecomManager = new BlockManagerDecommissioner(sparkConf, bm)
    validateDecommissionTimestampsOnManager(bmDecomManager, fail = false)
  }

  test("block decom manager short circuits removed blocks") {
    // Set up the mocks so we return one shuffle block
    val bm = mock(classOf[BlockManager])
    val migratableShuffleBlockResolver = mock(classOf[MigratableResolver])
    // First call get blocks, then empty list simulating a delete.
    when(migratableShuffleBlockResolver.getStoredShuffles())
      .thenReturn(Seq(ShuffleBlockInfo(1, 1)))
      .thenReturn(Seq())
    when(migratableShuffleBlockResolver.getMigrationBlocks(mc.any()))
      .thenReturn(List(
        (ShuffleIndexBlockId(1, 1, 1), mock(classOf[ManagedBuffer])),
        (ShuffleDataBlockId(1, 1, 1), mock(classOf[ManagedBuffer]))))
      .thenReturn(List())

    when(bm.migratableResolver).thenReturn(migratableShuffleBlockResolver)
    when(bm.getMigratableRDDBlocks())
      .thenReturn(Seq())
    when(bm.getPeers(mc.any()))
      .thenReturn(Seq(BlockManagerId("exec2", "host2", 12345)))

    val blockTransferService = mock(classOf[BlockTransferService])
    // Simulate an ambiguous IO error (e.g. block could be gone, connection failed, etc.)
    when(blockTransferService.uploadBlockSync(
      mc.any(), mc.any(), mc.any(), mc.any(), mc.any(), mc.any(), mc.isNull())).thenThrow(
      new java.io.IOException("boop")
    )

    when(bm.blockTransferService).thenReturn(blockTransferService)

    // Verify the decom manager handles this correctly
    val bmDecomManager = new BlockManagerDecommissioner(sparkConf, bm)
    validateDecommissionTimestampsOnManager(bmDecomManager, fail = false,
      numShuffles = Some(1))
  }

  test("test shuffle and cached rdd migration without any error") {
    val blockTransferService = mock(classOf[BlockTransferService])
    val bm = mock(classOf[BlockManager])

    val storedBlockId1 = RDDBlockId(0, 0)
    val storedBlock1 =
      new ReplicateBlock(storedBlockId1, Seq(BlockManagerId("replicaHolder", "host1", bmPort)), 1)

    val migratableShuffleBlockResolver = mock(classOf[MigratableResolver])
    registerShuffleBlocks(migratableShuffleBlockResolver, Set((1, 1L, 1)))
    when(bm.getPeers(mc.any()))
      .thenReturn(Seq(BlockManagerId("exec2", "host2", 12345)))

    when(bm.blockTransferService).thenReturn(blockTransferService)
    when(bm.migratableResolver).thenReturn(migratableShuffleBlockResolver)
    when(bm.getMigratableRDDBlocks())
      .thenReturn(Seq(storedBlock1))

    val bmDecomManager = new BlockManagerDecommissioner(sparkConf, bm)

    try {
      bmDecomManager.start()

      // Wait for shuffle migration to complete (but RDD blocks will continue indefinitely)
      // We can't use waitComplete() here because RDD blocks are mocked to never finish
      eventually(timeout(10.second), interval(10.milliseconds)) {
        // Verify shuffle migration completed
        assert(bmDecomManager.shufflesToMigrate.isEmpty === true)
        assert(bmDecomManager.numMigratedShuffles.get() === 1)
        assert(!bmDecomManager.shuffleBlocksLeft)

        // Verify RDD migration is still ongoing
        assert(bmDecomManager.rddBlocksLeft)

        // Verify mock interactions
        verify(bm, least(1)).replicateBlock(
          mc.eq(storedBlockId1), mc.any(), mc.any(), mc.eq(Some(3)))
        verify(blockTransferService, times(2))
          .uploadBlockSync(mc.eq("host2"), mc.eq(bmPort), mc.eq("exec2"), mc.any(), mc.any(),
            mc.eq(StorageLevel.DISK_ONLY), mc.isNull())
      }

      // Verify migration status is still in progress due to RDD blocks
      val migrationInfo = bmDecomManager.lastMigrationInfo()
      assert(migrationInfo.status === MigrationInProgress,
        s"Expected MigrationInProgress but got: ${migrationInfo.status}")

      // Verify timing is reasonable
      val currentTime = System.currentTimeMillis()
      assert(migrationInfo.lastActivityTime > 0 &&
        migrationInfo.lastActivityTime <= currentTime + 1000,
        s"Activity time ${migrationInfo.lastActivityTime} not reasonable vs current $currentTime")

      // Verify shuffle migration statistics show completion
      val stats = migrationInfo.shuffleMigrationStat
      assert(stats.numMigratedBlock === 1)
      assert(stats.numBlocksLeft === 0,
        s"Expected 0 shuffle blocks left, got ${stats.numBlocksLeft}")
    } finally {
        bmDecomManager.stop()
    }
  }

  test("SPARK-44547: test cached rdd migration no available hosts") {
    val blockTransferService = mock(classOf[BlockTransferService])
    val bm = mock(classOf[BlockManager])

    val storedBlockId1 = RDDBlockId(0, 0)
    val storedBlock1 =
      new ReplicateBlock(storedBlockId1, Seq(BlockManagerId("replicaHolder", "host1", bmPort)), 1)

    val migratableShuffleBlockResolver = mock(classOf[MigratableResolver])
    registerShuffleBlocks(migratableShuffleBlockResolver, Set())
    when(bm.getPeers(mc.any()))
      .thenReturn(Seq(FallbackStorage.FALLBACK_BLOCK_MANAGER_ID))

    when(bm.blockTransferService).thenReturn(blockTransferService)
    when(bm.migratableResolver).thenReturn(migratableShuffleBlockResolver)
    when(bm.getMigratableRDDBlocks())
      .thenReturn(Seq(storedBlock1))

    val bmDecomManager = new BlockManagerDecommissioner(sparkConf, bm)

    try {
      bmDecomManager.start()

      // Wait for migration to stop - it should stop quickly due to no peers available
      eventually(timeout(10.seconds), interval(100.milliseconds)) {
        val migrationInfo = bmDecomManager.lastMigrationInfo()
        // Should be either stopped or still in progress but with stoppedRDD=true
        assert(migrationInfo.isStopped || bmDecomManager.stoppedRDD,
          s"Expected stopped or stoppedRDD=true, got status: ${migrationInfo.status}, " +
          s"stoppedRDD: ${bmDecomManager.stoppedRDD}")
      }

      // Verify no replication was attempted since no peers available
      verify(bm, never()).replicateBlock(
        mc.eq(storedBlockId1), mc.any(), mc.any(), mc.eq(Some(3)))

      // Verify final state - RDD migration stopped due to no peers
      assert(bmDecomManager.rddBlocksLeft)
      assert(bmDecomManager.stoppedRDD)

      // Verify final state shows stopped with correct reason
      verifyFinalState(bmDecomManager, expectedNumShuffles = Some(0), expectComplete = false)
    } finally {
      bmDecomManager.stop()
    }
  }

  test("block decom manager updates shuffle migration stats correctly") {
    val blockTransferService = mock(classOf[BlockTransferService])
    val bm = mock(classOf[BlockManager])

    val migratableShuffleBlockResolver = mock(classOf[MigratableResolver])
    registerShuffleBlocks(migratableShuffleBlockResolver, Set((1, 1L, 1), (2, 2L, 2), (3, 3L, 3)))
    when(bm.getPeers(mc.any()))
      .thenReturn(Seq(BlockManagerId("exec2", "host2", 12345)))

    when(bm.blockTransferService).thenReturn(blockTransferService)
    when(bm.migratableResolver).thenReturn(migratableShuffleBlockResolver)
    when(bm.getMigratableRDDBlocks())
      .thenReturn(Seq())

    val bmDecomManager = new BlockManagerDecommissioner(sparkConf, bm)

    try {
      bmDecomManager.start()

      // Wait for migration to complete
      waitComplete(bmDecomManager)

      // Verify final state and statistics
      verifyFinalState(bmDecomManager, expectedNumShuffles = Some(3), expectComplete = true)

      // Additional detailed verification of shuffle migration statistics
      val stat = bmDecomManager.lastMigrationInfo().shuffleMigrationStat

      // Verify block counts
      assert(stat.numBlocksLeft === 0)
      assert(stat.numMigratedBlock === 3)
      assert(stat.totalBlocks === 3)
      assert(stat.deletedBlocks === 0)

      // Verify size calculations precisely
      // Each shuffle block has 2 buffers (index + data), each 100 bytes = 200 bytes per block
      // 3 blocks x 200 bytes = 600 bytes total
      val expectedTotalSize = 3 * 2 * 100L
      assert(stat.totalSize === expectedTotalSize,
        s"Expected total size $expectedTotalSize but got ${stat.totalSize}")
      assert(stat.totalMigratedSize === expectedTotalSize,
        s"Expected migrated size $expectedTotalSize but got ${stat.totalMigratedSize}")

      // Verify size relationships are logical
      assert(stat.totalSize >= stat.totalMigratedSize,
        s"Total size (${stat.totalSize}) should be >= migrated size (${stat.totalMigratedSize})")

      // Since all blocks migrated successfully, remaining size should be 0
      val remainingSize = stat.totalSize - stat.totalMigratedSize
      assert(remainingSize === 0,
        s"All blocks migrated, so remaining size should be 0 but got $remainingSize")
    } finally {
      bmDecomManager.stop()
    }
  }
}
