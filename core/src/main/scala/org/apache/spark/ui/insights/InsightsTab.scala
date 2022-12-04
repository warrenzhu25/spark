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

package org.apache.spark.ui.insights

import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters._
import scala.xml.Node

import org.apache.spark.JobExecutionStatus
import org.apache.spark.insight.SparkApplicationData
import org.apache.spark.insight.heuristics._
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.StageStatus
import org.apache.spark.ui.SparkUI
import org.apache.spark.ui.SparkUITab
import org.apache.spark.ui.UIUtils
import org.apache.spark.ui.WebUIPage
import org.apache.spark.util.kvstore.KVStore

class InsightsTab(
    parent: SparkUI,
    val store: AppStatusStore,
    listing: Option[KVStore] = None)
  extends SparkUITab(parent, "insights") {

  init()

  private def init(): Unit = {
    attachPage(new InsightsPage(this, parent.appId, store))
    attachPage(new FailureSummaryPage(this, parent.appId, store))
//    if (listing.nonEmpty) {
//      attachPage(new AppTrendPage(this, parent.appId, store, listing.get))
//    }
  }
}

private[ui] class InsightsPage(
    parent: SparkUITab,
    appId: String,
    store: AppStatusStore)
  extends WebUIPage("") {

  private val heuristic = Seq(
    ConfigurationHeuristic,
    PeakMemoryUsageHeuristic,
    StageSkewHeuristic
  )

  def render(request: HttpServletRequest): Seq[Node] = {
    val basePathUri = UIUtils.prependBaseUri(request, parent.basePath)
    val content =
      <span>
        {insightsTable(basePathUri)}
      </span>

    UIUtils.headerSparkPage(request, "Insights", content, parent, useDataTables = true)
  }

  private def insightsTable(basePathUri: String) = {
    heuristic.flatMap(_.apply(appData())).map(r => r.toHTML(basePathUri))
  }

  def appData(): SparkApplicationData = {

    SparkApplicationData(
      store,
      appId,
      appConf = store.environmentInfo().sparkProperties.map(a => a._1 -> a._2).toMap,
      appInfo = store.applicationInfo(),
      jobData = store.jobsList(List.empty[JobExecutionStatus].asJava),
      stageData = store.stageList(List.empty[StageStatus].asJava),
      executorSummaries = store.executorList(false),
      executorMetricsDistributions = store.executorMetricSummary(false, Array(0.5, 1.0)))
  }
}
