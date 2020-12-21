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

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.executor.ExecutorMetricsDistributions
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.status.api.v1.StageData
import org.apache.spark.status.api.v1.TaskMetricDistributions
import org.apache.spark.status.insight.SparkApplicationData
import org.apache.spark.status.insight.heuristics.ConfigurationHeuristicsConstants._
import org.apache.spark.util.Utils._

/**
 * A heuristic based on executor peak memory metrics
 */
object StageSkewHeuristic extends Heuristic {
  val SKEW_THRESHOLD = 0.2

  val taskMetrics: Map[String, (TaskMetricDistributions) => IndexedSeq[Double]] = Map(
    "executorRunTime" -> (d => d.executorCpuTime)
  )

  override def analysis(data: SparkApplicationData): Seq[AnalysisResult] = {
    data.stageDataWithSummaries.flatMap(m => analysis(data.appConf, m))
  }

  override def name: String = "Stage Skew Summary"

  def analysis(conf: Map[String, String],
               stageData: StageData): Option[AnalysisResult] = {
    val metricDistributions = stageData.taskSummary.get
    val records =
      taskMetrics
        .mapValues(f => f.apply(metricDistributions))
        .filter{ case (_, d) => d(0) > d(1) * SKEW_THRESHOLD}
        .map { case (name, metrics) =>
          new StageSkewRecord(
            name = name,
            value = "",
            distributions = metrics.map(v => metricToString(name, v)))
        }
        .toSeq

    if (records.nonEmpty) {
      Some(AnalysisResult(records))
    } else {
      None
    }
  }

  def metricToString(name: String, value: Double): String = {
    if (name.contains("Time")) {
      timeStringAsSeconds(value.toLong + "ms") + "s"
    } else if (name.contains("Size")) {
      bytesToString(value.toLong)
    } else {
      value.toLong.toString
    }
  }
}

class StageSkewRecord(name: String,
                      value: String,
                      distributions: IndexedSeq[String],
                      description: String = "",
                      suggested: String = "",
                      severity: Severity = Severity.Normal)
  extends AnalysisRecord(name, value, description, suggested, severity) {

  override val header: Seq[String] =
    Seq("Name", "Value", "Usage (Median / Max)", "Suggested", "Description", "Severity")

  override def toHTML(request: HttpServletRequest): Seq[Node] = {
    <tr>
      <td>{name.split("(?=[A-Z])")}</td>
      <td>{value}</td>
      <td>{distributions(0)} / {distributions(1)}</td>
      <td>{description}</td>
      <td>{suggested}</td>
      <td>
        <span data-toggle="tooltip" title={severity.getTooltip}>
          {severity}
        </span>
      </td>
    </tr>
  }
}


