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
package org.apache.spark.streaming.ui

import java.io.{BufferedOutputStream, FileOutputStream, PrintWriter}
import java.net.URI
import java.util.Locale

import org.apache.hadoop.fs.{ FileSystem, FSDataOutputStream, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.Utils

/**
 * Only when spark.eventLog.enabled & spark.streamingLog.enabled
 * will init and start StreamingPersistListener
 */
private[spark] class StreamingPersistListener(ssc: StreamingContext)
  extends SparkListener with StreamingListener with Logging{

  private val hadoopConf = SparkHadoopUtil.
    get.newConfiguration(ssc.sparkContext.conf)
  private val logBaseDir = ssc.sparkContext.getEventLogDir()
  private val fileSystem : FileSystem = Utils.getHadoopFileSystem(logBaseDir, hadoopConf)
  private val outputBufferSize = ssc.sparkContext.conf.get(EVENT_LOG_OUTPUT_BUFFER_SIZE).toInt
  private val LOG_FILE_PERMISSIONS = new FsPermission(Integer.parseInt("770", 8).toShort)

  private[spark] var logFilePath = getLogFilePath(ssc.sparkContext, logBaseDir)
  private var writer: Option[PrintWriter] = None
  private var hadoopDataStream: Option[FSDataOutputStream] = None

  private val keepLatestCompletedNum = 10
  private var countCompletedNum = 0

  def start(): Unit = {
    if (!fileSystem.getFileStatus(new Path(logBaseDir)).isDirectory) {
      throw new IllegalArgumentException(s"Log directory $logBaseDir is not a directory.")
    }

    val path = new Path(logFilePath)
    val uri = path.toUri
    val defaultFs = FileSystem.getDefaultUri(hadoopConf).getScheme
    val isDefaultLocal = defaultFs == null || defaultFs == "file"

    // Should always persist the latest listener
    // Overwrite is needed
    if (fileSystem.delete(path, true)) {
      logInfo(s"Spark Streaming listener log $path deleted")
    }

    val dstream =
      if ((isDefaultLocal && uri.getScheme == null) || uri.getScheme == "file") {
        new FileOutputStream(uri.getPath)
      } else {
        // create listener log file
        hadoopDataStream = Some(fileSystem.create(path))
        hadoopDataStream.get
      }

    try {
      val bstream = new BufferedOutputStream(dstream, outputBufferSize)

      fileSystem.setPermission(path, LOG_FILE_PERMISSIONS)
      writer = Some(new PrintWriter(bstream))
    } catch {
      case ex: Exception =>
        dstream.close()
    }
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    countCompletedNum += 1
    logListener(true)
  }

  private def logListener(flushLogger: Boolean = false): Unit = {
    val needOverWrite = countCompletedNum >= keepLatestCompletedNum
    try {
      // need to do overwrite for keep a small size streaming log file
      if (needOverWrite) {
        log.debug("Need to overwrite streaming log file")
        countCompletedNum = 0
        start()
      }
      val listenerJson = ListenerJsonProtocol
        .streamingJobProgressListenerToJson(ssc.progressListener)
      // scalastyle:off println
      writer.foreach(_.println(compact(render(listenerJson)).concat("*")))
      log.debug(s"StreamingPersistListener start to log: ${compact(render(listenerJson))}")
      // scalastyle:on println
      if (flushLogger) {
        writer.foreach(_.flush())
        hadoopDataStream.foreach(_.hflush())
      }
    } catch {
      case e: Throwable =>
        logInfo(s"logListener error: ${e}")
    }
  }

  def stop(): Unit = {
    val target = new Path(logFilePath)
    try {
      writer.foreach(_.close())
      fileSystem.setTimes(target, System.currentTimeMillis(), -1)
    } catch {
      case e: Exception => logDebug(s"failed to set time of $target", e)
    }
  }

  def getLogFilePath(
      sc: SparkContext,
      logBaseDir: URI): String = {
    val base = new Path(logBaseDir).toString.stripSuffix("/") + "/" + "streaming_" +
      sanitize(sc.applicationId)
    if (sc.applicationAttemptId.isDefined) {
      base + "_" + sanitize(sc.applicationAttemptId.get)
    } else {
      base
    }
  }

  private def sanitize(str: String): String = {
    str.replaceAll("[ :/]", "-").replaceAll("[.${}'\"]", "_").toLowerCase(Locale.ROOT)
  }

}
