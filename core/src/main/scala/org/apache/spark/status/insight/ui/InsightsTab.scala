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

package org.apache.spark.status.insight.ui

import javax.servlet.http.HttpServletRequest
import org.apache.spark.JobExecutionStatus
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.StageStatus
import org.apache.spark.status.insight.SparkApplicationData
import org.apache.spark.status.insight.heuristics.{AnalysisResult, ConfigurationHeuristic, ConfigurationParametersHeuristic, DriverHeuristic, ExecutorGcHeuristic, ExecutorsHeuristic, HeuristicResult, JobsHeuristic, JvmUsedMemoryHeuristic, StagesHeuristic}
import org.apache.spark.ui.{SparkUI, SparkUITab, UIUtils, WebUIPage}
import org.apache.spark.util.Utils

import scala.xml.Node
import scala.collection.JavaConverters._

class InsightsTab(parent: SparkUI, store: AppStatusStore) extends SparkUITab(parent, "insights") {

  init()

  private def init(): Unit = {
    attachPage(new InsightsPage(this, parent.appId, store))
  }

}

private[ui] class InsightsPage(
    parent: SparkUITab,
    appId: String,
    store: AppStatusStore)
  extends WebUIPage("") {

  private val heuristic = Seq(
    ConfigurationHeuristic,
    ConfigurationParametersHeuristic,
    ExecutorGcHeuristic,
    ExecutorsHeuristic,
    DriverHeuristic,
    JvmUsedMemoryHeuristic,
    JobsHeuristic,
    StagesHeuristic
  )

  def render(request: HttpServletRequest): Seq[Node] = {
    val content =
      <span>
        {insightsTable}
      </span>

    UIUtils.headerSparkPage(request, "Insights", content, parent, useDataTables = true)
  }

  private def insightsTable() = {
    heuristic.map(_.apply(appData())).map(r =>
    <span class="collapse-aggregated-classpathEntries collapse-table"
          onClick="collapseTable('collapse-aggregated-classpathEntries',
            'aggregated-classpathEntries')">
      <h4>
        <span class="collapse-table-arrow arrow-open"></span>
        <a>{r.name}</a>
      </h4>
    </span>
      <div class="aggregated-classpathEntries collapsible-table">
        {r.toTable}
      </div>
    )
  }

  private def appData(): SparkApplicationData = {
    val applicationInfo = store.applicationInfo()
    val appConfig = store.environmentInfo().sparkProperties.map(a => a._1 -> a._2).toMap
    val jobData = store.jobsList(List.empty[JobExecutionStatus].asJava)
    val stageData = store.stageList(List.empty[StageStatus].asJava)
    val executorSummary = store.executorList(false)

    SparkApplicationData(appId, appConfig, applicationInfo, jobData, stageData, executorSummary)
  }
}
