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
package org.apache.spark.insight.heuristics

import org.apache.spark.status.api.v1.FailureReason

object FailureDiagnosis {

  lazy val rulesByType = Seq(
    TaskRetryRule,
    HeartbeatTimeoutRule,
    MetadataMissRule,
    ConnectionFailureRule,
    MemoryExceededRule,
    OOMErrorRule
  )
    .groupBy(_.failureType)

  def analysis(failureReason: FailureReason): Option[DiagnosisResult] = {
    val shortFailureType = failureReason.failureType.split("\\.").last
    rulesByType.getOrElse(shortFailureType, Seq.empty)
      .flatMap(_.apply(failureReason))
      .headOption
  }

}

case class DiagnosisResult(
  diagnosisType: String,
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
  def name(): String = getClass.getSimpleName.dropRight(5)
  def isRootCause: Option[Boolean] = None
  def suggestedConfigs: Map[String, String] = Map.empty
  def result: String
  def matched(failureReason: FailureReason): Boolean
  def apply(failureReason: FailureReason): Option[DiagnosisResult] =
    if (matched(failureReason: FailureReason)) {
      Some(DiagnosisResult(
        name(),
        result,
        suggestedConfigs,
        isRootCause))
    } else {
      None
    }
}

object TaskRetryRule extends DiagnosisRule("ContinuousTaskRetryException") {

  override val suggestedConfigs = Map(
    "spark.task.maxFailures" -> "1"
  )

  override val result =
    """
      |Most likely caused by other task failures. Please check others.
      |""".stripMargin

  override def isRootCause: Option[Boolean] = Option(false)

  override def matched(failureReason: FailureReason): Boolean = true
}

object HeartbeatTimeoutRule extends DiagnosisRule("ExecutorLostFailure") {
  private val pattern = raw"""Executor heartbeat timed out after (\d+) ms*""".r
  override val result =
    """
      |Most executor heartbeat timeout are caused by long gc pause.
      |You can reduce gc pause or increase network timeout.
      |""".stripMargin
  override val suggestedConfigs = Map(
    "spark.network.timeout" -> "240s",
    "spark.executor.extraJavaOptions" -> "-XX:ParallelGCThreads={spark.executor.cores}"
  )
  override def isRootCause: Option[Boolean] = Option(true)

  override def matched(failureReason: FailureReason): Boolean =
    failureReason.message match {
      case pattern(_*) => true
      case _ => false
  }

}
object MetadataMissRule extends DiagnosisRule("MetadataFetchFailedException") {
  private val pattern = raw"""Missing an output location for shuffle (\d+)*""".r
  override val result =
    """
      |Most likely caused by executor lost due to memory issue.
      |Please check other ExecutorLost failure to identify root cause.
      |""".stripMargin

  override def isRootCause: Option[Boolean] = Option(false)

  override def matched(failureReason: FailureReason): Boolean =
    failureReason.message match {
      case pattern(_*) => true
      case _ => false
    }
}
object ConnectionFailureRule extends DiagnosisRule("IOException") {
  private val pattern = raw"^Failed to connect to .*".r
  override val suggestedConfigs = Map(
    "spark.shuffle.io.connectionTimeout" -> "240s",
    "spark.shuffle.io.maxRetries" -> "10"
  )

  override val result =
    """
      |Most likely caused by executor lost due to machine or network issue.
      |Please check log of this executor to identify root cause.
      |""".stripMargin
  override def isRootCause: Option[Boolean] = Option(false)

  override def matched(failureReason: FailureReason): Boolean =
    failureReason.message match {
      case pattern(_*) => true
      case _ => false
    }

}
object MemoryExceededRule extends DiagnosisRule("ExecutorLostFailure") {
  private val pattern = raw"^Container killed by YARN for exceeding (physical )?memory limits.*".r
  override val result =
    """
      |Caused by too few memoryOverhead of executor allocated.
      |Please increase spark.executor.memoryOverhead.
      |""".stripMargin
  override val suggestedConfigs = Map(
    "spark.executor.memoryOverhead" -> "0.2 * {spark.executor.memory}"
  )

  override def isRootCause: Option[Boolean] = Option(true)

  override def matched(failureReason: FailureReason): Boolean =
    failureReason.message match {
      case pattern(_*) => true
      case _ => false
    }
}

object OOMErrorRule extends DiagnosisRule("ExecutorLostFailure") {
  private val pattern = raw"^Container from a bad node: container_.* on host: .* Exit status: 1.*".r
  override val result =
    """
      |Most likely caused by OutOfMemoryError. You can confirm this from stdout.
      |You will see some like 'java.lang.OutOfMemoryError: Java heap space'.
      |Please enable heap dump using suggested configs to investigate this
      |""".stripMargin
  override val suggestedConfigs = Map(
    "spark.executor.extraJavaOptions" -> "-XX:+HeapDumpOnOutOfMemoryError"
  )

  override def isRootCause: Option[Boolean] = Option(true)

  override def matched(failureReason: FailureReason): Boolean =
    failureReason.message match {
      case pattern(_*) => true
      case _ => false
    }
}