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
import org.apache.spark.internal.{config, Logging}

/**
 * Worker that continuously migrates RDD blocks during decommissioning.
 * Runs in a loop until migration is complete or stopped.
 */
private[storage] class RDDMigrationWorker(
    blockManager: BlockManager,
    conf: SparkConf,
    migrationStrategy: MigrationStrategy,
    stateTracker: MigrationStateTracker,
    errorHandler: MigrationErrorHandler,
    fallbackStorage: Option[FallbackStorage]) extends Runnable with Logging {

  private val sleepInterval = conf.get(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL)

  /**
   * Check if there are available peers for migration
   */
  private def hasAvailablePeers: Boolean = {
    blockManager.getPeers(false).exists(_ != FallbackStorage.FALLBACK_BLOCK_MANAGER_ID)
  }

  /**
   * Perform one round of RDD block migration
   */
  private def performMigrationRound(): Boolean = {
    val startTime = System.nanoTime()
    logInfo("Attempting to migrate all cached RDD blocks")
    val hasBlocksLeft = migrationStrategy.migrateBlocks(blockManager, conf)
    stateTracker.markRDDMigrationCompleted(hasBlocksLeft)
    logInfo(s"Finished current round RDD blocks migration, " +
      s"waiting for ${sleepInterval}ms before the next round migration.")
    hasBlocksLeft
  }

  override def run(): Unit = {
    logInfo("Attempting to migrate all RDD blocks")
    while (stateTracker.shouldContinueRDDMigration) {
      // Validate if we have peers to migrate to. Otherwise, give up migration.
      if (!hasAvailablePeers) {
        logWarning("No available peers to receive RDD blocks, stop migration.")
        stateTracker.stopRDD("No available peers")
      } else {
        try {
          performMigrationRound()
          Thread.sleep(sleepInterval)
        } catch {
          case e: Exception =>
            val action = errorHandler.handleGeneralMigrationError(e, "RDD blocks migration")
            action match {
              case errorHandler.StopAllMigration =>
                logInfo(s"Stop RDD blocks migration${
                  if (stateTracker.shouldContinueRDDMigration) " unexpectedly" else ""}.")
                stateTracker.stopRDD("Interrupted")
              case _ =>
                stateTracker.stopRDD("Error occurred")
            }
        }
      }
    }
  }
}
