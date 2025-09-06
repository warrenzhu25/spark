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

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
 * Strategy interface for migrating blocks during executor decommissioning.
 * Provides a pluggable architecture for different types of block migration.
 */
private[storage] trait MigrationStrategy extends Logging {
  /**
   * Attempt to migrate blocks of this strategy's type.
   * @param blockManager the block manager instance
   * @param conf Spark configuration
   * @return true if there are more blocks left to migrate, false if migration is complete
   */
  def migrateBlocks(blockManager: BlockManager, conf: SparkConf): Boolean

  /**
   * Get a human-readable name for this migration strategy
   */
  def strategyName: String
}

/**
 * Migration strategy for RDD cache blocks.
 * Handles replication-based migration using the existing block replication infrastructure.
 */
private[storage] class RDDMigrationStrategy extends MigrationStrategy {
  override def strategyName: String = "RDD Cache Blocks"

  override def migrateBlocks(blockManager: BlockManager, conf: SparkConf): Boolean = {
    val maxReplicationFailures = conf.get(
      org.apache.spark.internal.config.STORAGE_DECOMMISSION_MAX_REPLICATION_FAILURE_PER_BLOCK)

    val replicateBlocksInfo = blockManager.getMigratableRDDBlocks()

    if (replicateBlocksInfo.nonEmpty) {
      logInfo(s"Need to replicate ${replicateBlocksInfo.size} RDD blocks " +
        "for block manager decommissioning")
    } else {
      logInfo("No RDD cache blocks to migrate")
      return false
    }

    // TODO: We can sort these blocks based on some policy (LRU/blockSize etc)
    //   so that we end up prioritize them over each other
    val blocksFailedReplication = replicateBlocksInfo.map { replicateBlock =>
      val replicatedSuccessfully = migrateRDDBlock(blockManager, replicateBlock,
        maxReplicationFailures)
      (replicateBlock.blockId, replicatedSuccessfully)
    }.filterNot(_._2).map(_._1)

    if (blocksFailedReplication.nonEmpty) {
      logWarning("Blocks failed replication in cache decommissioning " +
        s"process: ${blocksFailedReplication.mkString(",")}")
      return true
    }
    false
  }

  private def migrateRDDBlock(blockManager: BlockManager,
                              blockToReplicate: BlockManagerMessages.ReplicateBlock,
                              maxReplicationFailures: Int): Boolean = {
    val replicatedSuccessfully = blockManager.replicateBlock(
      blockToReplicate.blockId,
      blockToReplicate.replicas.toSet,
      blockToReplicate.maxReplicas,
      maxReplicationFailures = Some(maxReplicationFailures))

    if (replicatedSuccessfully) {
      logInfo(s"Block ${blockToReplicate.blockId} migrated successfully, Removing block now")
      blockManager.removeBlock(blockToReplicate.blockId)
      logInfo(s"Block ${blockToReplicate.blockId} removed")
    } else {
      logWarning(s"Failed to migrate block ${blockToReplicate.blockId}")
    }
    replicatedSuccessfully
  }
}

/**
 * Migration strategy for shuffle blocks.
 * Handles peer-to-peer migration using the producer/consumer model.
 */
private[storage] class ShuffleMigrationStrategy extends MigrationStrategy {
  override def strategyName: String = "Shuffle Blocks"

  override def migrateBlocks(blockManager: BlockManager, conf: SparkConf): Boolean = {
    // For now, this is a placeholder that maintains the existing shuffle migration logic
    // The actual shuffle migration logic will be refactored in later commits
    // to use this strategy pattern more effectively

    val localShuffles = blockManager.migratableResolver.getStoredShuffles()
    if (localShuffles.nonEmpty) {
      logInfo(s"Found ${localShuffles.size} shuffle blocks for migration")
      return true // Indicate more work needed
    } else {
      logInfo("No shuffle blocks to migrate")
      return false
    }
  }
}
