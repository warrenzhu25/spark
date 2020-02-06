/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.spark.heuristics

import org.apache.spark.metrics.{ExecutorMetricType, OffHeapUnifiedMemory, OnHeapUnifiedMemory}
import org.apache.spark.status.api.v1.ExecutorSummary
import org.apache.spark.status.insight.SparkApplicationData
import org.apache.spark.status.insight.analysis.{MemoryFormatUtils, Severity, SeverityThresholds}
import org.apache.spark.status.insight.heuristics.{AnalysisResult, Heuristic, HeuristicResult, SimpleResult}
import org.apache.spark.status.insight.util.Utils

import scala.collection.JavaConverters


/**
  * A heuristic based on peak unified memory for the spark executors
  *
  * This heuristic reports the fraction of memory used/ memory allocated and if the fraction can be reduced. Also, it checks for the skew in peak unified memory and reports if the skew is too much.
  */
class UnifiedMemoryHeuristic extends Heuristic {

  import UnifiedMemoryHeuristic._
  import JavaConverters._

  val peakUnifiedMemoryThresholdString: String = PEAK_UNIFIED_MEMORY_THRESHOLD_KEY
  val sparkExecutorMemoryThreshold: String = DEFAULT_SPARK_EXECUTOR_MEMORY_THRESHOLD

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)

    val resultDetails = Seq(
      SimpleResult("Unified Memory Space Allocated", MemoryFormatUtils.bytesToString(evaluator.maxMemory)),
      SimpleResult("Mean peak unified memory", MemoryFormatUtils.bytesToString(evaluator.meanUnifiedMemory)),
      SimpleResult(MAX_PEAK_UNIFIED_MEMORY_HEURISTIC_NAME, MemoryFormatUtils.bytesToString(evaluator.maxUnifiedMemory)),
      SimpleResult("spark.executor.memory", MemoryFormatUtils.bytesToString(evaluator.sparkExecutorMemory)),
      SimpleResult("spark.memory.fraction", evaluator.sparkMemoryFraction.toString)
    )

    HeuristicResult(
      name,
      resultDetails
    )
  }
}

object UnifiedMemoryHeuristic {

  val EXECUTION_MEMORY = "executionMemory"
  val STORAGE_MEMORY = "storageMemory"
  val SPARK_EXECUTOR_MEMORY_KEY = "spark.executor.memory"
  val SPARK_MEMORY_FRACTION_KEY = "spark.memory.fraction"
  val PEAK_UNIFIED_MEMORY_THRESHOLD_KEY = "peak_unified_memory_threshold"
  val SPARK_EXECUTOR_MEMORY_THRESHOLD_KEY = "spark_executor_memory_threshold"
  val DEFAULT_SPARK_EXECUTOR_MEMORY_THRESHOLD = "2G"
  val UNIFIED_MEMORY_ALLOCATED_THRESHOLD = "256M"
  val SPARK_MEMORY_FRACTION_THRESHOLD : Double = 0.05
  val MAX_PEAK_UNIFIED_MEMORY_HEURISTIC_NAME="Max peak unified memory";

  class Evaluator(unifiedMemoryHeuristic: UnifiedMemoryHeuristic, data: SparkApplicationData) {
    lazy val appConfigurationProperties: Map[String, String] =
      data.appConf

    lazy val DEFAULT_PEAK_UNIFIED_MEMORY_THRESHOLD: SeverityThresholds = SeverityThresholds(low = 0.7 * maxMemory, moderate = 0.6 * maxMemory, severe = 0.4 * maxMemory, critical = 0.2 * maxMemory, ascending = false)

    lazy val executorSummaries: Seq[ExecutorSummary] = data.executorSummaries
    if (executorSummaries == null) {
      throw new Exception("Executors Summary is null.")
    }

    val executorList: Seq[ExecutorSummary] = executorSummaries.filterNot(_.id.equals("driver"))
    if (executorList.isEmpty) {
      throw new Exception("No executor information available.")
    }

    //allocated memory for the unified region
    val maxMemory: Long = executorList.head.maxMemory

    val PEAK_UNIFIED_MEMORY_THRESHOLDS: SeverityThresholds = if (unifiedMemoryHeuristic.peakUnifiedMemoryThresholdString == null) {
      DEFAULT_PEAK_UNIFIED_MEMORY_THRESHOLD
    } else {
      SeverityThresholds.parse(unifiedMemoryHeuristic.peakUnifiedMemoryThresholdString.split(",").map(_.toDouble * maxMemory).toString, ascending = false).getOrElse(DEFAULT_PEAK_UNIFIED_MEMORY_THRESHOLD)
    }

    val sparkExecutorMemory: Long = (appConfigurationProperties.get(SPARK_EXECUTOR_MEMORY_KEY).map(MemoryFormatUtils.stringToBytes)).getOrElse(0L)

    lazy val sparkMemoryFraction: Double = appConfigurationProperties.getOrElse(SPARK_MEMORY_FRACTION_KEY, "0.6").toDouble

    lazy val meanUnifiedMemory: Long = executorList.map(getPeakUnifiedMemory).sum / executorList.size

    lazy val maxUnifiedMemory: Long = executorList.map(getPeakUnifiedMemory).max

    //If sparkMemoryFraction or total Unified Memory allocated is less than their respective thresholds then won't consider for severity
    lazy val severity: Severity = if (sparkMemoryFraction > SPARK_MEMORY_FRACTION_THRESHOLD && maxMemory > MemoryFormatUtils.stringToBytes(UNIFIED_MEMORY_ALLOCATED_THRESHOLD)) {
      if (sparkExecutorMemory <= MemoryFormatUtils.stringToBytes(unifiedMemoryHeuristic.sparkExecutorMemoryThreshold)) {
        Severity.NONE
      } else {
        PEAK_UNIFIED_MEMORY_THRESHOLDS.severityOf(maxUnifiedMemory)
      }
    } else {
      Severity.NONE
    }

    val executorCount = executorList.size
    lazy val score = Utils.getHeuristicScore(severity, executorCount)

    def getPeakUnifiedMemory(executorSummary: ExecutorSummary): Long = {
      getMetricValue(executorSummary, OnHeapUnifiedMemory) + getMetricValue(executorSummary, OffHeapUnifiedMemory)
    }

    def getMetricValue(executorSummary: ExecutorSummary, executorMetricType: ExecutorMetricType) : Long = {
      executorSummary.peakMemoryMetrics.map(_.getMetricValue(executorMetricType)).getOrElse(0)
    }
  }
}
