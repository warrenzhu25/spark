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

import java.util.concurrent.{ExecutorService, ThreadPoolExecutor, TimeUnit}

import org.apache.spark.SparkConf
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.util.ThreadUtils

/**
 * Manages thread pools for block migration during executor decommissioning.
 * Centralizes thread creation, lifecycle management, and shutdown logic.
 */
private[storage] class MigrationThreadManager(conf: SparkConf) extends Logging {

  private val replicationParallelism = math.max(1,
    conf.get(config.STORAGE_DECOMMISSION_REPLICATION_REATTEMPT_INTERVAL).toInt / 100)

  private val shuffleMigrationParallelism = math.max(1,
    conf.getInt("spark.storage.decommission.shuffleBlocks.migrationParallelism",
      replicationParallelism))

  // Thread pools for migration operations
  @volatile private var _rddBlockMigrationExecutor: ThreadPoolExecutor = _
  @volatile private var _shuffleBlockMigrationRefreshExecutor: ExecutorService = _
  @volatile private var _shuffleMigrationPool: ExecutorService = _

  /**
   * Initialize all thread pools
   */
  def initialize(): Unit = {
    logInfo("Initializing migration thread pools")

    _rddBlockMigrationExecutor = ThreadUtils.newDaemonFixedThreadPool(
      replicationParallelism, "block-migration-rdd").asInstanceOf[ThreadPoolExecutor]

    _shuffleBlockMigrationRefreshExecutor = ThreadUtils.newDaemonSingleThreadExecutor(
      "block-migration-refresh-shuffle")

    _shuffleMigrationPool = ThreadUtils.newDaemonFixedThreadPool(
      shuffleMigrationParallelism, "block-migration-shuffle")

    logInfo(s"Created thread pools: RDD migration ($replicationParallelism threads), " +
      s"shuffle refresh (1 thread), shuffle migration ($shuffleMigrationParallelism threads)")
  }

  /**
   * Get the RDD block migration executor
   */
  def rddBlockMigrationExecutor: ThreadPoolExecutor = {
    require(_rddBlockMigrationExecutor != null, "Thread pools not initialized")
    _rddBlockMigrationExecutor
  }

  /**
   * Get the shuffle block migration refresh executor
   */
  def shuffleBlockMigrationRefreshExecutor: ExecutorService = {
    require(_shuffleBlockMigrationRefreshExecutor != null, "Thread pools not initialized")
    _shuffleBlockMigrationRefreshExecutor
  }

  /**
   * Get the shuffle migration pool
   */
  def shuffleMigrationPool: ExecutorService = {
    require(_shuffleMigrationPool != null, "Thread pools not initialized")
    _shuffleMigrationPool
  }

  /**
   * Shutdown all thread pools gracefully
   */
  def shutdown(): Unit = {
    logInfo("Shutting down migration thread pools")

    val executors = Seq(
      ("RDD migration", _rddBlockMigrationExecutor),
      ("Shuffle refresh", _shuffleBlockMigrationRefreshExecutor),
      ("Shuffle migration", _shuffleMigrationPool)
    ).filter(_._2 != null)

    executors.foreach { case (name, executor) =>
      try {
        logInfo(s"Shutting down $name executor")
        executor.shutdown()
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
          logWarning(s"$name executor did not shutdown gracefully, forcing shutdown")
          executor.shutdownNow()
        }
      } catch {
        case e: Exception =>
          logError(s"Error shutting down $name executor", e)
      }
    }
  }

  /**
   * Check if all thread pools are shutdown
   */
  def isShutdown: Boolean = {
    val executors = Seq(_rddBlockMigrationExecutor, _shuffleBlockMigrationRefreshExecutor,
      _shuffleMigrationPool).filter(_ != null)
    executors.forall(_.isShutdown)
  }

  /**
   * Get current thread pool status for monitoring
   */
  def getThreadPoolStatus: String = {
    val rddStatus = Option(_rddBlockMigrationExecutor).map { exec =>
      s"RDD: ${exec.getActiveCount}/${exec.getCorePoolSize} active"
    }.getOrElse("RDD: not initialized")

    val shuffleRefreshStatus = Option(_shuffleBlockMigrationRefreshExecutor).map { exec =>
      if (exec.isShutdown) "Shuffle refresh: shutdown" else "Shuffle refresh: active"
    }.getOrElse("Shuffle refresh: not initialized")

    val shuffleMigrationStatus = Option(_shuffleMigrationPool).map { exec =>
      if (exec.isShutdown) "Shuffle migration: shutdown" else "Shuffle migration: active"
    }.getOrElse("Shuffle migration: not initialized")

    s"Thread pools - $rddStatus, $shuffleRefreshStatus, $shuffleMigrationStatus"
  }
}
