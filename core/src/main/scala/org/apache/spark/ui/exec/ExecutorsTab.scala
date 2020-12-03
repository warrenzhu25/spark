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

package org.apache.spark.ui.exec

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.internal.config.UI._
import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.ExecutorPeakMetricsDistributions
import org.apache.spark.ui.{SparkUI, SparkUITab, UIUtils, WebUIPage}

private[ui] class ExecutorsTab(parent: SparkUI, store: AppStatusStore)
  extends SparkUITab(parent, "executors") {

  init()

  private def init(): Unit = {
    val threadDumpEnabled =
      parent.sc.isDefined && parent.conf.get(UI_THREAD_DUMPS_ENABLED)

    attachPage(new ExecutorsPage(this, store, threadDumpEnabled))
    if (threadDumpEnabled) {
      attachPage(new ExecutorThreadDumpPage(this, parent.sc))
    }
  }

}

private[ui] class ExecutorsPage(
    parent: SparkUITab,
    store: AppStatusStore,
    threadDumpEnabled: Boolean)
  extends WebUIPage("") {

  private val quantiles = Array(0, 0.25, 0.5, 0.75, 1.0)

  def render(request: HttpServletRequest): Seq[Node] = {
    val content =
      {
        <div id="active-executors"></div> ++
          <script src={UIUtils.prependBaseUri(request, "/static/utils.js")}></script> ++
          <script src={UIUtils.prependBaseUri(request, "/static/executorspage.js")}></script> ++
          <script>setThreadDumpEnabled({threadDumpEnabled})</script> ++
          <div style="display: none" id="exec-peak-summary">
            {buildExecutorPeakMemorySummaryTable()}
          </div>
      }

    UIUtils.headerSparkPage(request, "Executors", content, parent, useDataTables = true)
  }

  def buildExecutorPeakMemorySummaryTable(): Seq[Node] = {
    val executorPeakMemorySummaryData = store.executorMetricSummary(false, quantiles)
    if (executorPeakMemorySummaryData.nonEmpty) {
      <span class="collapse-aggregated-peakMetricsSummaries collapse-table"
            onClick="collapseTable('collapse-aggregated-peakMetricsSummaries',
            'aggregated-peakMetricsSummaries')">
        <h5>
          <span class="collapse-table-arrow arrow-closed"></span>
          <a>Peak Metrics Summary</a>
        </h5>
      </span>
        <div class="aggregated-peakMetricsSummaries collapsible-table collapsed">
          {executorPeakMemorySummaryTable(executorPeakMemorySummaryData.get)}
        </div>
    } else Seq.empty
  }

  def executorPeakMemorySummaryTable(distributions: ExecutorPeakMetricsDistributions): Seq[Node] = {
    val propertyHeader = Seq("Name", "Min", "25th percentile", "Median", "75th percentile", "Max")
    val metricsDistributions = ExecutorMetricType.metricToOffset.map { case (metric, _) =>
      (metric, distributions.getMetricDistribution(metric))
    }.toSeq
    UIUtils.listingTable(propertyHeader, executorPeakMemorySummaryRow,
      metricsDistributions)
  }

  def executorPeakMemorySummaryRow(tuple: (String, IndexedSeq[Double])): Seq[Node] = {
    <tr>
      <td>{tuple._1}</td>
      {tuple._2.map(q => <td>{q}</td>)}
    </tr>
  }
}
