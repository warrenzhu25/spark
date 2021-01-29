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
package org.apache.spark.status.insight.heuristics

import org.apache.spark.status.api.v1.FailureReason

object FailureDiagnosis {

  lazy val rulesByType = Seq(
    TaskRetryRule,
    HeartbeatTimeoutRule,
    MetadataMissRule,
    ConnectionFailureRule,
    MemoryExceededRule)
    .groupBy(_.failureType)

  def analysis(failureReason: FailureReason): Option[DiagnosisResult] = {
    val shortFailureType = failureReason.failureType.split("\\.").last
    rulesByType.getOrElse(shortFailureType, Seq.empty)
      .flatMap(_.apply(failureReason))
      .headOption
  }

}

case class DiagnosisResult(
  desc: String,
  suggestedConfigs: Map[String, String] = Map.empty,
  isRootCause: Option[Boolean] = None) {
  def rootCause: String = {
    isRootCause match {
      case Some(true) => "Yes"
      case Some(false) => "No"
      case _ => "Unknown"
    }
  }

  def configs: Option[String] = {
    if (suggestedConfigs.nonEmpty) {
      Some(suggestedConfigs
        .map(_.productIterator.mkString("="))
        .mkString(","))
    } else {
      None
    }
  }
}

abstract class DiagnosisRule (val failureType: String) {

  def apply(failureReason: FailureReason): Option[DiagnosisResult]
}

object TaskRetryRule extends DiagnosisRule("ContinuousTaskRetryException") {
  private val suggestedConfigs = Map(
    "spark.task.maxFailures" -> "1"
  )

  val result =
    """
      |Most likely caused by other task failures. Please check others.
      |""".stripMargin

  override def apply(failureReason: FailureReason): Option[DiagnosisResult] =
    Some(DiagnosisResult(
      result,
      suggestedConfigs,
      Some(false)
    ))
}

object HeartbeatTimeoutRule extends DiagnosisRule("ExecutorLostFailure") {
  private val pattern = raw"""Executor heartbeat timed out after (\d+) ms*""".r
  private val result =
    """
      |Most executor heartbeat timeout are caused by long gc pause.
      |You can reduce gc pause or increase network timeout.
      |""".stripMargin
  private val suggestedConfigs = Map(
    "spark.network.timeout" -> "240s",
    "spark.executor.extraJavaOptions" -> "-XX:ParallelGCThreads={spark.executor.cores}"
  )

  override def apply(failureReason: FailureReason): Option[DiagnosisResult] =
    failureReason.message match {
      case pattern(_*) => Some(DiagnosisResult(result, suggestedConfigs, Some(true)))
      case _ => None
    }

}
object MetadataMissRule extends DiagnosisRule("MetadataFetchFailedException") {
  private val pattern = raw"""Missing an output location for shuffle (\d+)*""".r
  private val result =
    """
      |Most likely caused by executor lost due to memory issue.
      |Please check other ExecutorLost failure to identify root cause.
      |""".stripMargin
  override def apply(failureReason: FailureReason): Option[DiagnosisResult] =
    failureReason.message match {
      case pattern(_*) => Some(DiagnosisResult(result, Map.empty, Some(false)))
      case _ => None
    }
}
object ConnectionFailureRule extends DiagnosisRule("IOException") {
  private val pattern = raw"^Failed to connect to *".r
  private val suggestedConfigs = Map(
    "spark.shuffle.io.connectionTimeout" -> "240s",
    "spark.shuffle.io.maxRetries" -> "10"
  )

  private val result =
    """
      |Most likely caused by executor lost due to machine or network issue.
      |Please check log of this executor to identify root cause.
      |""".stripMargin

  override def apply(failureReason: FailureReason): Option[DiagnosisResult] =
    failureReason.message match {
      case pattern(_*) => Some(DiagnosisResult(result, suggestedConfigs, Some(false)))
      case _ => None
    }
}
object MemoryExceededRule extends DiagnosisRule("ExecutorLostFailure") {
  private val pattern = raw"^Container killed by YARN for exceeding (physical )?memory limits.*".r
  private val result =
    """
      |Caused by too few memoryOverhead of executor allocated.
      |Please increase spark.executor.memoryOverhead.
      |""".stripMargin
  private val suggestedConfigs = Map(
    "spark.shuffle.io.connectionTimeout" -> "240s",
    "spark.shuffle.io.maxRetries" -> "10"
  )

  override def apply(failureReason: FailureReason): Option[DiagnosisResult] =
    failureReason.message match {
      case pattern(_*) => Some(DiagnosisResult(result, suggestedConfigs, Some(true)))
      case _ => None
    }
}

object OOMErrorRule extends DiagnosisRule("ExecutorLostFailure") {
  private val pattern = raw"Container from a bad node: container_* on host: *. Exit status: 1.*".r
  val result =
    """
      |Most likely caused by OutOfMemoryError. You can confirm this from stdout.
      |You will see some like 'java.lang.OutOfMemoryError: Java heap space'.
      |Please enable heap dump using suggested configs to investigate this
      |""".stripMargin
    ""
  val suggestedConfigs = Map(
    "spark.executor.extraJavaOptions" -> "-XX:+HeapDumpOnOutOfMemoryError"
  )

  override def apply(failureReason: FailureReason): Option[DiagnosisResult] =
    failureReason.message match {
      case pattern(_*) => Some(DiagnosisResult(result, suggestedConfigs, Some(true)))
      case _ => None
    }
}