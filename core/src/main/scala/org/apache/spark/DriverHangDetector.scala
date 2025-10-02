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

package org.apache.spark

import java.lang.management.ManagementFactory
import java.util.concurrent.{ScheduledFuture, TimeUnit}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Detects driver hangs by periodically taking thread dumps of non-daemon threads
 * and comparing them. If consecutive thread dumps are identical, it indicates a hang.
 *
 * @param sc The SparkContext to monitor
 */
private[spark] class DriverHangDetector(sc: SparkContext) extends Logging {

  private val checkIntervalMs = sc.conf.get(DRIVER_HANG_DETECTION_INTERVAL)
  private val threshold = sc.conf.get(DRIVER_HANG_DETECTION_THRESHOLD)

  // Store recent thread dumps for comparison (circular buffer)
  private val threadDumpHistory = mutable.Queue[Map[Long, String]]()

  private val eventLoopThread =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-hang-detector")

  private var checkTask: ScheduledFuture[_] = null

  def start(): Unit = {
    logInfo(s"Starting driver hang detector with interval ${checkIntervalMs}ms " +
      s"and threshold $threshold")
    checkTask = eventLoopThread.scheduleAtFixedRate(
      () => Utils.tryLogNonFatalError { checkForHang() },
      checkIntervalMs,
      checkIntervalMs,
      TimeUnit.MILLISECONDS)
  }

  def stop(): Unit = {
    if (checkTask != null) {
      checkTask.cancel(true)
    }
    eventLoopThread.shutdownNow()
  }

  private def checkForHang(): Unit = {
    val currentDump = getNonDaemonThreadDump()

    // Add to history
    threadDumpHistory.enqueue(currentDump)

    // Keep only the last 'threshold' dumps
    while (threadDumpHistory.size > threshold) {
      threadDumpHistory.dequeue()
    }

    // Check if we have enough dumps to compare
    if (threadDumpHistory.size == threshold) {
      // Check if all dumps are identical
      if (areAllDumpsIdentical()) {
        handleHangDetected(currentDump)
      }
    }
  }

  /**
   * Get thread dumps for non-daemon threads only.
   * Returns a map of thread ID to stack trace string.
   */
  private def getNonDaemonThreadDump(): Map[Long, String] = {
    val threadMXBean = ManagementFactory.getThreadMXBean
    val allThreads = threadMXBean.dumpAllThreads(true, true).filter(_ != null)

    allThreads
      .filterNot(_.getThreadName.contains("driver-hang-detector"))
      .filter { threadInfo =>
        // Get the actual Thread object to check if it's a daemon
        val thread = Thread.getAllStackTraces.keySet().asScala
          .find(_.getId == threadInfo.getThreadId)
        thread.exists(!_.isDaemon)
      }
      .map { threadInfo =>
        val stackTrace = threadInfo.getStackTrace.map(_.toString).mkString("\n  ")
        val state = threadInfo.getThreadState
        val threadDump = s"Thread: ${threadInfo.getThreadName} (ID: ${threadInfo.getThreadId})\n" +
          s"State: $state\n" +
          s"Stack:\n  $stackTrace"
        threadInfo.getThreadId -> threadDump
      }
      .toMap
  }

  /**
   * Check if all thread dumps in history are identical.
   */
  private def areAllDumpsIdentical(): Boolean = {
    if (threadDumpHistory.isEmpty) {
      return false
    }

    val first = threadDumpHistory.head
    threadDumpHistory.tail.forall { dump =>
      // Dumps are identical if they have the same threads with same stack traces
      dump.keySet == first.keySet && dump.keySet.forall { threadId =>
        dump(threadId) == first(threadId)
      }
    }
  }

  /**
   * Handle detected hang: log thread dump and stop SparkContext.
   */
  private def handleHangDetected(threadDump: Map[Long, String]): Unit = {
    val dumpString = threadDump.values.mkString("\n\n")
    logError(
      s"""
         |================================================================================
         |DRIVER HANG DETECTED!
         |================================================================================
         |The driver has been hung for at least ${checkIntervalMs * threshold}ms.
         |${threadDump.size} non-daemon thread(s) have identical stack traces across
         |$threshold consecutive thread dumps.
         |
         |Thread Dump:
         |$dumpString
         |
         |Stopping SparkContext...
         |================================================================================
       """.stripMargin)

    // Stop the SparkContext to prevent indefinite hang
    try {
      sc.stop()
    } catch {
      case e: Exception =>
        logError("Error stopping SparkContext after hang detection", e)
    }
  }
}
