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
import scala.xml.NodeSeq

import org.apache.spark.JobExecutionStatus

import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.StageData
import org.apache.spark.status.api.v1.StageStatus
import org.apache.spark.ui.insights.InsightUIUtils.failureSummaryTable
import org.apache.spark.ui.UIUtils
import org.apache.spark.ui.WebUIPage
import org.apache.spark.ui.jobs.AllJobsPage.jobsTable
import org.apache.spark.ui.jobs.AllStagesPage.stageTag
import org.apache.spark.ui.jobs.AllStagesPage.statusName
import org.apache.spark.ui.jobs.AllStagesPage.table
import org.apache.spark.ui.jobs.StageTableBase

private[ui] class FailureSummaryPage(
    parent: InsightsTab,
    appId: String,
    store: AppStatusStore)
  extends WebUIPage("") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val content =
      <span>
        {failedJobs(request)}
        {failedStages(request)}
        {failureSummary(request)}
      </span>

    UIUtils.headerSparkPage(request, "Failure Diagnosis", content, parent, useDataTables = true)
  }

  private def failedJobs(request: HttpServletRequest): NodeSeq = {
    val status = JobExecutionStatus.FAILED
    val failedJobs = parent.store.jobsList(Seq(status).asJava)
    if (failedJobs.isEmpty) {
      Seq.empty
    } else {
      val table =
        jobsTable(store, parent.basePath, request, "failed", "failedJob",
          failedJobs, killEnabled = false)
      failedJobsTable(table, failedJobs.size)
    }
  }

  private def failedStages(request: HttpServletRequest): NodeSeq = {
    val status = StageStatus.FAILED
    val failedStages = parent.store.stageList(Seq(status).asJava)
    val appSummary = parent.store.appSummary()
    val stagesTable =
      new StageTableBase(parent.store, request, failedStages, statusName(status), stageTag(status),
        parent.basePath, "stages", false, false, true)

    table(appSummary, status, stagesTable, failedStages.size)
  }

  private def failureSummary(request: HttpServletRequest) = {
    val failedStages: Seq[StageData] = store
      .stageList(List.empty[StageStatus].asJava)
      .filter(_.status.equals(StageStatus.FAILED))

    {if (failedStages.nonEmpty) {
      <span class="collapse-aggregated-failureSummaries collapse-table"
            onClick="collapseTable('collapse-aggregated-failureSummaries',
            'aggregated-exceptionSummaries')">
        <h4>
          <span class="collapse-table-arrow arrow-open"></span>
          <a>Stage Failure Summary</a>
        </h4>
      </span> ++
        failedStages.map(s =>
          <h5>
            {stageNameLink(request, s)}
          </h5>
            <div class="aggregated-exceptionSummaries collapsible-table">
              {failureSummaryTable(store.failureSummary(s.stageId, s.attemptId))}
            </div>
        )
    } else Seq.empty}
  }

  private def failedJobsTable(table: NodeSeq, size: Int) = {
    <span id ="failedJobs" class="collapse-aggregated-failedJobs collapse-table"
          onClick="collapseTable('collapse-aggregated-failedJobs','aggregated-failedJobs')">
      <h4>
        <span class="collapse-table-arrow arrow-open"></span>
        <a>Failed Jobs ({size})</a>
      </h4>
    </span> ++
      <div class="aggregated-failedJobs collapsible-table">
        {table}
      </div>
  }

  private def stageNameLink(request: HttpServletRequest, s: StageData) = {
    val basePathUri = UIUtils.prependBaseUri(request, parent.basePath)
    val nameLinkUri = s"$basePathUri/stages/stage/?id=${s.stageId}&attempt=${s.attemptId}"
    <a href={nameLinkUri} class="name-link">
      Stage {s.stageId} has {s.numFailedTasks} failed of {s.numTasks} total tasks
    </a>
  }
}
