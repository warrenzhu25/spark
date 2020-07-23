/*
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

package org.apache.spark.status.insight.heuristics

import org.apache.spark.status.api.v1.ExecutorSummary
import org.apache.spark.status.insight.SparkApplicationData
import org.apache.spark.status.insight.analysis.{MemoryFormatUtils, Severity, SeverityThresholds}
import org.apache.spark.status.insight.util.Utils
import org.apache.spark.ui.UIUtils

import scala.collection.JavaConverters
import scala.xml.Node

/**
  * A heuristic based on peak JVM used memory for the spark executors
  *
  */
object JvmUsedMemoryHeuristic extends Heuristic {
  val JVM_USED_MEMORY = "jvmUsedMemory"
  val SPARK_EXECUTOR_MEMORY = "spark.executor.memory"
  val SPARK_EXECUTOR_MEMORY_THRESHOLD_KEY = "spark_executor_memory_threshold"

  // 300 * FileUtils.ONE_MB (300 * 1024 * 1024)
  val reservedMemory: Long = 314572800
  val BUFFER_FRACTION: Double = 0.2
  val MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_THRESHOLD_KEY = "executor_peak_jvm_memory_threshold"
  val MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_HEURISTIC_NAME = "Max executor peak JVM used memory"

  lazy val DEFAULT_SPARK_EXECUTOR_MEMORY_THRESHOLD = "2G"

  lazy val sparkExecutorMemoryThreshold: String = DEFAULT_SPARK_EXECUTOR_MEMORY_THRESHOLD

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(data)

    var resultDetails = Seq(
      new SingleValue(MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_HEURISTIC_NAME, MemoryFormatUtils.bytesToString(evaluator.maxExecutorPeakJvmUsedMemory)),
      new SingleValue("spark.executor.memory", MemoryFormatUtils.bytesToString(evaluator.sparkExecutorMemory))
    )

    if (evaluator.severity != Severity.NONE) {
      resultDetails = resultDetails :+ new SingleValue("Executor Memory", "The allocated memory for the executor (in " + SPARK_EXECUTOR_MEMORY + ") is much more than the peak JVM used memory by executors.")
      resultDetails = resultDetails :+ new SingleValue("Suggested spark.executor.memory", MemoryFormatUtils.roundOffMemoryStringToNextInteger((MemoryFormatUtils.bytesToString(((1 + BUFFER_FRACTION) * evaluator.maxExecutorPeakJvmUsedMemory).toLong))))
    }

    new JvmUsedMemoryHeuristicResult(resultDetails)
  }

  class Evaluator(data: SparkApplicationData) {
    lazy val appConfigurationProperties: Map[String, String] =
      data.appConf

    if (data.executorSummaries == null) {
      throw new Exception("Executor Summary is Null.")
    }

    lazy val executorSummaries: Seq[ExecutorSummary] = data.executorSummaries
    val executorList: Seq[ExecutorSummary] = executorSummaries.filterNot(_.id.equals("driver"))
    val sparkExecutorMemory: Long = (appConfigurationProperties.get(SPARK_EXECUTOR_MEMORY).map(MemoryFormatUtils.stringToBytes)).getOrElse(0L)
    lazy val maxExecutorPeakJvmUsedMemory: Long = 0L

    lazy val DEFAULT_MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_THRESHOLDS =
      SeverityThresholds(low = 1.25 * (maxExecutorPeakJvmUsedMemory + reservedMemory), moderate = 1.5 * (maxExecutorPeakJvmUsedMemory + reservedMemory), severe = 2 * (maxExecutorPeakJvmUsedMemory + reservedMemory), critical = 3 * (maxExecutorPeakJvmUsedMemory + reservedMemory), ascending = true)

    val MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_THRESHOLDS: SeverityThresholds = DEFAULT_MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_THRESHOLDS

    lazy val severity = if (sparkExecutorMemory <= MemoryFormatUtils.stringToBytes(sparkExecutorMemoryThreshold)) {
      Severity.NONE
    } else {
      MAX_EXECUTOR_PEAK_JVM_USED_MEMORY_THRESHOLDS.severityOf(sparkExecutorMemory)
    }

    val executorCount = executorList.size
    lazy val score = Utils.getHeuristicScore(severity, executorCount)
  }
}

class JvmUsedMemoryHeuristicResult(results: Seq[AnalysisResult])
  extends HeuristicResult("Jvm Memory Insights", results) {
  override def toTable: Seq[Node] =
    UIUtils.listingTable(insightHeader, insightRow, results.map(_.toTuple())
      , fixedWidth = true)

  private def insightHeader = Seq("Name", "Value")

  private def insightRow(data: (String, String, String, String)) =
    <tr>
      <td>{data._1}</td>
      <td>{data._2}</td>
    </tr>
}
