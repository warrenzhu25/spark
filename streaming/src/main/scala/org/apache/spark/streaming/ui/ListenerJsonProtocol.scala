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

import scala.collection.mutable
import scala.collection.mutable.Queue

import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL._

import org.apache.spark.streaming.Time
import org.apache.spark.streaming.scheduler.{ReceiverInfo, StreamInputInfo}
import org.apache.spark.streaming.ui.StreamingJobProgressListener.OutputOpId

private[ui] object ListenerJsonProtocol {
  private implicit val format = DefaultFormats

  private var batchesWithJobsNum = -1

  def streamingJobProgressListenerToJson(listener: StreamingJobProgressListener): JValue = {
    val completedBatchData = JArray(addOpIdJobIdPairs(listener.retainedCompletedBatches, listener)
      .map(batchUIDataToJson).toList)
    val waitingBatchData = JArray(addOpIdJobIdPairs(listener.waitingBatches, listener)
      .map(batchUIDataToJson).toList)
    val runningBatchData = JArray(addOpIdJobIdPairs(listener.runningBatches, listener)
      .map(batchUIDataToJson).toList)
    val streams = listener.streamIds.map(id => (id, listener.streamName(id)))
    val receiverInfos = listener.getReceiverInfos
    val startTime = listener.startTime
    val batchDuration = listener.batchDuration

    ("Listener" -> "StreamingBatches") ~
    ("totalCompletedBatchesNum" -> listener.numTotalCompletedBatches) ~
    ("totalReceivedRecordsNum" -> listener.numTotalReceivedRecords) ~
    ("totalProcessedRecordsNum" -> listener.numTotalProcessedRecords) ~
    ("batchDuration" -> batchDuration) ~
    ("startTime" -> startTime) ~
    ("streams" -> JArray(streams.map(streamsToJson).toList)) ~
    ("completedBatches" -> completedBatchData) ~
    ("waitingBatches" -> waitingBatchData) ~
    ("runningBatches" -> runningBatchData) ~
    ("receiverInfos" -> receiverMapToJson(receiverInfos))
  }

  def streamsToJson(stream: (Int, Option[String])): JValue = {
    ("streamId" -> stream._1 ) ~
    ("name" -> stream._2)
  }

  def batchUIDataToJson(batchUIData: BatchUIData): JValue = {
    ("batchTime" -> batchUIData.batchTime.toString) ~
    ("submissionTime" -> batchUIData.submissionTime.toString) ~
    ("processingEndTime" -> batchUIData.processingEndTime.getOrElse(-1).toString) ~
    ("processingStartTime" -> batchUIData.processingStartTime.getOrElse(-1).toString) ~
    ("outputOperations" -> outputOpDataToJson(batchUIData.outputOperations)) ~
    ("outputOpIdSparkJobIdPairs" -> JArray(batchUIData.outputOpIdSparkJobIdPairs
      .map(outputOpIdAndSparkJobIdToJson).toList)) ~
    ("streamIdToInputInfo" -> inputInfoMapToJson(batchUIData.streamIdToInputInfo))
  }

  def addOpIdJobIdPairs(batches: Seq[BatchUIData], listener: StreamingJobProgressListener):
  Seq[BatchUIData] = {
    val orderBatches = batches.sortBy(_.batchTime)(Ordering[Time].reverse)
    val batchWithJobIds = new Queue[BatchUIData]
    // each micro batch has fixed job number
    // only need to calculate once
    if (batchesWithJobsNum < 0) {
      for (batch <- orderBatches) {
        batchWithJobIds.enqueue(listener.getBatchUIData(batch.batchTime).getOrElse(batch))
      }
      setBatchesWithJobsNum(batchWithJobIds.front, listener.batchUIDataLimitNum)
    } else {
      var oversize = false
      for (batch <- orderBatches) {
        if (oversize) {
          batchWithJobIds.enqueue(batch)
        } else {
          batchWithJobIds.enqueue(listener.getBatchUIData(batch.batchTime).getOrElse(batch))
          oversize = batchWithJobIds.size > batchesWithJobsNum
        }
      }
    }
    batchWithJobIds.toIndexedSeq
  }

  def setBatchesWithJobsNum(batchUIData: BatchUIData, batchUIDataLimitNum: Int): Unit = {
    val jobsNum = batchUIData.outputOpIdSparkJobIdPairs.size
    batchesWithJobsNum = batchUIDataLimitNum / jobsNum
  }

  def outputOperationUIDataToJson(outputOperationUIData: OutputOperationUIData): JValue = {
    ("outputOpId" -> outputOperationUIData.id) ~
    ("name" -> outputOperationUIData.name) ~
    ("description" -> outputOperationUIData.description) ~
    ("startTime" -> outputOperationUIData.startTime.getOrElse(-1).toString) ~
    ("endTime" -> outputOperationUIData.endTime.getOrElse(-1).toString) ~
    ("failureReason" -> outputOperationUIData.failureReason)
  }

  def outputOpDataToJson(m: mutable.Map[OutputOpId, OutputOperationUIData]): JValue = {
    val jsonFields = m.map {
      case(k, v) => JField(k.toString, outputOperationUIDataToJson(v))
    }
    JObject(jsonFields.toList)
  }

  def receiverInfoToJson(receiverInfo: ReceiverInfo): JValue = {
    ("streamId" -> receiverInfo.streamId) ~
    ("name" -> receiverInfo.name) ~
    ("active" -> receiverInfo.active) ~
    ("location" -> receiverInfo.location) ~
    ("executorId" -> receiverInfo.executorId) ~
    ("lastErrorMessage" -> receiverInfo.lastErrorMessage) ~
    ("lastError" -> receiverInfo.lastError) ~
    ("lastErrorTime" -> receiverInfo.lastErrorTime.toString)
  }

  def outputOpIdAndSparkJobIdToJson(output: OutputOpIdAndSparkJobId): JValue = {
    ("outputOpId" -> output.outputOpId) ~
    ("sparkJobId" -> output.sparkJobId)
  }

  def receiverMapToJson(m: mutable.Map[Int, ReceiverInfo]): JValue = {
    val jsonFields = m.map {
      case (k, v) => JField(k.toString, receiverInfoToJson(v))
    }
    JObject(jsonFields.toList)
  }

  def inputInfoMapToJson(m: Map[Int, StreamInputInfo]): JValue = {
    val jsonFields = m.map {
      case (k, v) => JField(k.toString, streamInputInfoToJson(v))
    }
    JObject(jsonFields.toList)
  }

  def streamInputInfoToJson(info: StreamInputInfo): JValue = {
    ("inputStreamId" -> info.inputStreamId) ~
    ("numRecords" -> info.numRecords) ~
    ("metaData" -> metadataToJson(info.metadata))
  }

  def metadataToJson(m: Map[String, Any]): JValue = {
    val jsonFields = m.map {
      case (k, v) => JField(k, v.toString)
    }
    JObject(jsonFields.toList)
  }
}


