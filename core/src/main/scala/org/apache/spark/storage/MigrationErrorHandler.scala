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

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ShuffleBlockInfo

/**
 * Centralized error handling and retry logic for block migration operations.
 * Provides standardized error classification, logging, and retry decisions.
 */
private[storage] class MigrationErrorHandler(
    maxReplicationFailures: Int) extends Logging {

  /**
   * Error classification for migration failures
   */
  sealed trait MigrationError
  case class RetryableError(exception: Exception, shouldRetry: Boolean) extends MigrationError
  case class FatalError(exception: Exception) extends MigrationError
  case class FileNotFoundError(exception: FileNotFoundException) extends MigrationError
  case object InterruptionError extends MigrationError

  /**
   * Action to take after handling an error
   */
  sealed trait ErrorAction
  case object RetryMigration extends ErrorAction
  case object AbortMigration extends ErrorAction
  case object StopAllMigration extends ErrorAction
  case object SkipMigration extends ErrorAction

  /**
   * Classify and handle exceptions that occur during migration
   */
  def handleMigrationException(
      exception: Exception,
      shuffleBlockInfo: ShuffleBlockInfo,
      peer: String,
      retryCount: Int): (MigrationError, ErrorAction) = {

    val migrationError = classifyError(exception)
    val action = determineAction(migrationError, retryCount)

    logMigrationError(migrationError, shuffleBlockInfo, peer, retryCount, exception)

    (migrationError, action)
  }

  /**
   * Classify the type of error that occurred
   */
  private def classifyError(exception: Exception): MigrationError = {
    exception match {
      case _: InterruptedException => InterruptionError
      case e: FileNotFoundException => FileNotFoundError(e)
      case e @ (_: IOException | _: SparkException) =>
        // Check if it's a wrapped FileNotFoundException
        if (hasFileNotFoundException(e)) {
          e.getCause match {
            case fnf: FileNotFoundException => FileNotFoundError(fnf)
            case _ => FileNotFoundError(new FileNotFoundException(e.getMessage))
          }
        } else {
          RetryableError(e, shouldRetry = true)
        }
      case e: Exception =>
        FatalError(e)
    }
  }

  /**
   * Check if an exception has FileNotFoundException as a cause
   */
  private def hasFileNotFoundException(exception: Exception): Boolean = {
    var cause = exception.getCause
    while (cause != null) {
      if (cause.isInstanceOf[FileNotFoundException]) {
        return true
      }
      cause = cause.getCause
    }
    false
  }

  /**
   * Determine what action to take based on error type and retry count
   */
  private def determineAction(error: MigrationError, retryCount: Int): ErrorAction = {
    error match {
      case InterruptionError => StopAllMigration
      case _: FileNotFoundError => SkipMigration
      case RetryableError(_, shouldRetry) if shouldRetry && retryCount < maxReplicationFailures =>
        RetryMigration
      case RetryableError(_, _) => AbortMigration
      case FatalError(_) => AbortMigration
    }
  }

  /**
   * Log migration errors with appropriate level and context
   */
  private def logMigrationError(
      error: MigrationError,
      shuffleBlockInfo: ShuffleBlockInfo,
      peer: String,
      retryCount: Int,
      exception: Exception): Unit = {

    error match {
      case InterruptionError =>
        logInfo("Migration interrupted - stopping all migration activities")

      case FileNotFoundError(e) =>
        logInfo(s"Shuffle block $shuffleBlockInfo file not found during migration to $peer - " +
          s"block was likely already deleted. Treating as successfully migrated. " +
          s"Error: ${e.getMessage}")

      case RetryableError(e, _) =>
        logError(s"Error occurred during migrating $shuffleBlockInfo to $peer " +
          s"(retry $retryCount/$maxReplicationFailures). " +
          s"Error: ${e.getClass.getSimpleName}: ${e.getMessage}", e)

      case FatalError(e) =>
        logError(s"Unexpected error during migration of $shuffleBlockInfo to $peer " +
          s"(retry $retryCount/$maxReplicationFailures). " +
          s"Error: ${e.getClass.getSimpleName}: ${e.getMessage}", e)
    }
  }

  /**
   * Handle general migration errors (for RDD blocks, etc.)
   */
  def handleGeneralMigrationError(
      exception: Exception,
      context: String): ErrorAction = {

    exception match {
      case null =>
        logError(s"Null exception occurred during $context.")
        AbortMigration
      case _: InterruptedException => StopAllMigration
      case e: Exception =>
        logError(s"Error occurred during $context.", e)
        AbortMigration
    }
  }

  /**
   * Handle thread startup/shutdown errors
   */
  def handleThreadError(exception: Throwable, context: String): Unit = {
    logError(s"Error during $context", exception)
  }

  /**
   * Check if a block should be retried based on failure count
   */
  def shouldRetryBlock(failureCount: Int): Boolean = {
    if (failureCount < maxReplicationFailures) {
      logDebug(s"Block will be retried ($failureCount / $maxReplicationFailures)")
      true
    } else {
      logWarning(s"Block migration failed for $maxReplicationFailures times - giving up")
      false
    }
  }

  /**
   * Log retry decision for a block
   */
  def logRetryDecision(
      shuffleBlockInfo: ShuffleBlockInfo, failureCount: Int, willRetry: Boolean): Unit = {
    if (willRetry) {
      logDebug(s"Will retry migration of $shuffleBlockInfo " +
        s"(retry $failureCount / $maxReplicationFailures)")
    } else {
      logWarning(s"Giving up on migration of $shuffleBlockInfo after " +
        s"$failureCount failures (max: $maxReplicationFailures)")
    }
  }

  /**
   * Get a summary of error handling configuration
   */
  def getConfigurationSummary: String = {
    s"Migration error handler: maxRetries=$maxReplicationFailures"
  }
}
