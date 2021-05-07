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

package org.apache.spark.insight.heuristics

import scala.xml.Node

import org.apache.spark.status.api.v1.StageData
import org.apache.spark.status.api.v1.TaskMetricDistributions
import org.apache.spark.insight.SparkApplicationData
import org.apache.spark.util.Utils._

/**
 * A heuristic based on executor peak memory metrics
 */
object StageSkewHeuristic extends Heuristic {
  val SKEW_THRESHOLD = 0.3

  val taskMetrics: Map[String, TaskMetricDistributions => IndexedSeq[Double]] = Map(
    "duration" -> (d => d.duration),
    "executorRunTime" -> (d => d.executorRunTime),
    "executorCpuTime"-> (d => d.executorCpuTime),
    "jvmGcTime"-> (d => d.jvmGcTime),
    "peakExecutionMemory"-> (d => d.peakExecutionMemory),
    "memoryBytesSpilled"-> (d => d.memoryBytesSpilled),
    "diskBytesSpilled"-> (d => d.diskBytesSpilled),
    "bytesRead"-> (d => d.inputMetrics.bytesRead),
    "recordsRead"-> (d => d.inputMetrics.recordsRead),
    "bytesWritten"-> (d => d.outputMetrics.bytesWritten),
    "recordsWritten"-> (d => d.outputMetrics.recordsWritten),
    "shuffleReadBytes"-> (d => d.shuffleReadMetrics.readBytes),
    "shuffleReadRecords"-> (d => d.shuffleReadMetrics.readRecords),
    "shuffleRemoteBlocksFetched"-> (d => d.shuffleReadMetrics.remoteBlocksFetched),
    "shuffleLocalBlocksFetched"-> (d => d.shuffleReadMetrics.localBlocksFetched),
    "shuffleFetchWaitTime"-> (d => d.shuffleReadMetrics.fetchWaitTime),
    "shuffleRemoteBytesRead"-> (d => d.shuffleReadMetrics.remoteBytesRead),
    "shuffleRemoteBytesReadToDisk"-> (d => d.shuffleReadMetrics.remoteBytesReadToDisk),
    "shuffleTotalBlocksFetched"-> (d => d.shuffleReadMetrics.totalBlocksFetched),
    "shuffleWriteBytes"-> (d => d.shuffleWriteMetrics.writeBytes),
    "shuffleWriteRecords"-> (d => d.shuffleWriteMetrics.writeRecords),
    "shuffleWriteTime"-> (d => d.shuffleWriteMetrics.writeTime))

  override def analysis(data: SparkApplicationData): Seq[AnalysisResult] = {
    data.stageDataWithSummaries.flatMap(m => analysis(data.appConf, m))
  }

  override def name: String = "Stage Skew Summary"

  def analysis(conf: Map[String, String],
               stageData: StageData): Option[AnalysisResult] = {
    val metricDistributions = stageData.taskMetricsDistributions.get
    val records =
      taskMetrics
        .mapValues(f => f.apply(metricDistributions))
        .filter{ case (_, d) => d(2) < d(4) * SKEW_THRESHOLD}
        .map { case (name, metrics) =>
          new StageSkewRecord(
            name = name.capitalize,
            value = "",
            distributions = metrics.map(v => metricToString(name, v)))
        }
        .toSeq

    if (records.nonEmpty && records.head.name.equals("Duration")) {
      Some(AnalysisResult(records, stageNameLink(stageData, _: String)))
    } else {
      None
    }
  }

  def metricToString(name: String, value: Double): String = {
    if (name.contains("Time")) {
      timeStringAsSeconds(value.toLong + "ms") + "s"
    } else if (name.contains("Size") || name.contains("Bytes")) {
      bytesToString(value.toLong)
    } else {
      value.toLong.toString
    }
  }

  private def stageNameLink(s: StageData, basePathUri: String): Seq[Node] = {
    val nameLinkUri = s"$basePathUri/stages/stage/?id=${s.stageId}&attempt=${s.attemptId}"
    <a href={nameLinkUri} class="name-link">
      {s.description.get} {s.numCompleteTasks}/{s.numTasks} tasks
    </a>
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

  override def toHTML(basePathUri: String): Seq[Node] = {
    <tr>
      <td>{name.split("(?=[A-Z])")}</td>
      <td>{value}</td>
      <td>{distributions(2)} / {distributions(4)}</td>
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


