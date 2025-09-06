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
 * Worker that periodically refreshes and discovers new shuffle blocks for migration.
 * Coordinates with the main decommissioner to update the migration queue.
 */
private[storage] class ShuffleRefreshWorker(
    conf: SparkConf,
    stateTracker: MigrationStateTracker,
    errorHandler: MigrationErrorHandler,
    refreshCallback: () => Boolean,
    progressCallback: () => Unit) extends Runnable with Logging {

  private val sleepInterval = conf.get(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL)

  /**
   * Perform one round of shuffle block discovery and refresh
   */
  private def performRefreshRound(): Unit = {
    val startTime = System.nanoTime()
    val hasBlocksLeft = refreshCallback()
    stateTracker.markShuffleMigrationCompleted(hasBlocksLeft)
    logInfo(s"Finished current round refreshing migratable shuffle blocks, " +
      s"waiting for ${sleepInterval}ms before the next round refreshing.")
    progressCallback()
  }

  override def run(): Unit = {
    logInfo("Attempting to migrate all shuffle blocks")
    while (stateTracker.shouldContinueShuffleMigration) {
      try {
        performRefreshRound()
        Thread.sleep(sleepInterval)
      } catch {
        case e: Exception =>
          val action = errorHandler.handleGeneralMigrationError(e,
            "shuffle block migration thread")
          action match {
            case errorHandler.StopAllMigration =>
              if (stateTracker.stopped) {
                logInfo("Shuffle block migration thread interrupted - stopping migration")
              }
            case _ =>
              stateTracker.stopShuffle("Fatal error")
          }
      }
    }
  }
}
