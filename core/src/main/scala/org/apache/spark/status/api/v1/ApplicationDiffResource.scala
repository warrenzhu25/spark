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
package org.apache.spark.status.api.v1

import javax.ws.rs._
import javax.ws.rs.core.MediaType

import org.apache.spark.JobExecutionStatus
import org.apache.spark.ui.SparkUI

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class ApplicationDiffResource extends ApiRequestContext {

  @GET
  def diff(
      @QueryParam("appId1") appId1: String,
      @QueryParam("attemptId1") attemptId1: String,
      @QueryParam("appId2") appId2: String,
      @QueryParam("attemptId2") attemptId2: String): ApplicationDiffInfo = {

    if (appId1 == null || appId1.isEmpty) {
      throw new BadParameterException("appId1 is required")
    }
    if (appId2 == null || appId2.isEmpty) {
      throw new BadParameterException("appId2 is required")
    }

    val (app1Summary, app1Metrics) = getAppMetrics(appId1, Option(attemptId1))
    val (app2Summary, app2Metrics) = getAppMetrics(appId2, Option(attemptId2))

    val jobMetrics = new JobMetricsComparison(
      app1Metrics.jobMetrics,
      app2Metrics.jobMetrics,
      diffJobMetrics(app1Metrics.jobMetrics, app2Metrics.jobMetrics))

    val stageMetrics = new StageMetricsComparison(
      app1Metrics.stageMetrics,
      app2Metrics.stageMetrics,
      diffStageMetrics(app1Metrics.stageMetrics, app2Metrics.stageMetrics))

    val taskMetrics = new TaskMetricsComparison(
      app1Metrics.taskMetrics,
      app2Metrics.taskMetrics,
      diffTaskMetrics(app1Metrics.taskMetrics, app2Metrics.taskMetrics))

    val executorMetrics = new ExecutorMetricsComparison(
      app1Metrics.executorMetrics,
      app2Metrics.executorMetrics,
      diffExecutorMetrics(app1Metrics.executorMetrics, app2Metrics.executorMetrics))

    val durationMetrics = new DurationMetricsComparison(
      app1Metrics.durationMetrics,
      app2Metrics.durationMetrics,
      diffDurationMetrics(app1Metrics.durationMetrics, app2Metrics.durationMetrics))

    new ApplicationDiffInfo(
      app1Summary,
      app2Summary,
      jobMetrics,
      stageMetrics,
      taskMetrics,
      executorMetrics,
      durationMetrics)
  }

  private case class AppMetrics(
      jobMetrics: JobMetricsValues,
      stageMetrics: StageMetricsValues,
      taskMetrics: TaskMetricsValues,
      executorMetrics: ExecutorMetricsValues,
      durationMetrics: DurationMetricsValues)

  private def getAppMetrics(
      appId: String,
      attemptId: Option[String]): (ApplicationSummary, AppMetrics) = {
    try {
      uiRoot.withSparkUI(appId, attemptId) { ui =>
        val user = httpRequest.getRemoteUser()
        if (!ui.securityManager.checkUIViewPermissions(user)) {
          throw new ForbiddenException(raw"""user "$user" is not authorized""")
        }
        (buildAppSummary(ui, appId, attemptId), buildAppMetrics(ui))
      }
    } catch {
      case _: NoSuchElementException =>
        val appKey = attemptId.map(appId + "/" + _).getOrElse(appId)
        throw new NotFoundException(s"no such app: $appKey")
    }
  }

  private def buildAppSummary(
      ui: SparkUI,
      appId: String,
      attemptId: Option[String]): ApplicationSummary = {
    val appInfo = ui.store.applicationInfo()
    new ApplicationSummary(appId, appInfo.name, attemptId)
  }

  private def buildAppMetrics(ui: SparkUI): AppMetrics = {
    val jobMetrics = buildJobMetrics(ui)
    val stageMetrics = buildStageMetrics(ui)
    val taskMetrics = buildTaskMetrics(ui)
    val executorMetrics = buildExecutorMetrics(ui)
    val durationMetrics = buildDurationMetrics(ui)

    AppMetrics(jobMetrics, stageMetrics, taskMetrics, executorMetrics, durationMetrics)
  }

  private def buildJobMetrics(ui: SparkUI): JobMetricsValues = {
    val jobs = ui.store.jobsList(null)
    val numJobs = jobs.size
    val succeededJobs = jobs.count(_.status == JobExecutionStatus.SUCCEEDED)
    val failedJobs = jobs.count(_.status == JobExecutionStatus.FAILED)
    new JobMetricsValues(numJobs, succeededJobs, failedJobs)
  }

  private def buildStageMetrics(ui: SparkUI): StageMetricsValues = {
    val stages = ui.store.stageList(null)
    val numStages = stages.size
    val completedStages = stages.count(_.status == StageStatus.COMPLETE)
    val failedStages = stages.count(_.status == StageStatus.FAILED)
    val skippedStages = stages.count(_.status == StageStatus.SKIPPED)
    new StageMetricsValues(numStages, completedStages, failedStages, skippedStages)
  }

  private def buildTaskMetrics(ui: SparkUI): TaskMetricsValues = {
    val stages = ui.store.stageList(null)
    var totalTasks = 0L
    var completedTasks = 0L
    var failedTasks = 0L
    var killedTasks = 0L
    var skippedTasks = 0L

    stages.foreach { stage =>
      totalTasks += stage.numTasks
      completedTasks += stage.numCompleteTasks
      failedTasks += stage.numFailedTasks
      killedTasks += stage.numKilledTasks
      if (stage.status == StageStatus.SKIPPED) {
        skippedTasks += stage.numTasks
      }
    }

    new TaskMetricsValues(totalTasks, completedTasks, failedTasks, killedTasks, skippedTasks)
  }

  private def buildExecutorMetrics(ui: SparkUI): ExecutorMetricsValues = {
    val executors = ui.store.executorList(false)
    var totalExecutorRunTime = 0L
    var totalJvmGcTime = 0L
    var totalInputBytes = 0L
    var totalShuffleReadBytes = 0L
    var totalShuffleWriteBytes = 0L

    executors.foreach { executor =>
      totalExecutorRunTime += executor.totalDuration
      totalJvmGcTime += executor.totalGCTime
      totalInputBytes += executor.totalInputBytes
      totalShuffleReadBytes += executor.totalShuffleRead
      totalShuffleWriteBytes += executor.totalShuffleWrite
    }

    new ExecutorMetricsValues(
      totalExecutorRunTime,
      totalJvmGcTime,
      totalInputBytes,
      totalShuffleReadBytes,
      totalShuffleWriteBytes)
  }

  private def buildDurationMetrics(ui: SparkUI): DurationMetricsValues = {
    val appInfo = ui.store.applicationInfo()
    val duration = appInfo.attempts.headOption.map(_.duration).getOrElse(0L)
    new DurationMetricsValues(duration)
  }

  private def diffJobMetrics(m1: JobMetricsValues, m2: JobMetricsValues): JobMetricsValues = {
    new JobMetricsValues(
      m2.numJobs - m1.numJobs,
      m2.succeededJobs - m1.succeededJobs,
      m2.failedJobs - m1.failedJobs)
  }

  private def diffStageMetrics(
      m1: StageMetricsValues,
      m2: StageMetricsValues): StageMetricsValues = {
    new StageMetricsValues(
      m2.numStages - m1.numStages,
      m2.completedStages - m1.completedStages,
      m2.failedStages - m1.failedStages,
      m2.skippedStages - m1.skippedStages)
  }

  private def diffTaskMetrics(m1: TaskMetricsValues, m2: TaskMetricsValues): TaskMetricsValues = {
    new TaskMetricsValues(
      m2.totalTasks - m1.totalTasks,
      m2.completedTasks - m1.completedTasks,
      m2.failedTasks - m1.failedTasks,
      m2.killedTasks - m1.killedTasks,
      m2.skippedTasks - m1.skippedTasks)
  }

  private def diffExecutorMetrics(
      m1: ExecutorMetricsValues,
      m2: ExecutorMetricsValues): ExecutorMetricsValues = {
    new ExecutorMetricsValues(
      m2.totalExecutorRunTime - m1.totalExecutorRunTime,
      m2.totalJvmGcTime - m1.totalJvmGcTime,
      m2.totalInputBytes - m1.totalInputBytes,
      m2.totalShuffleReadBytes - m1.totalShuffleReadBytes,
      m2.totalShuffleWriteBytes - m1.totalShuffleWriteBytes)
  }

  private def diffDurationMetrics(
      m1: DurationMetricsValues,
      m2: DurationMetricsValues): DurationMetricsValues = {
    new DurationMetricsValues(m2.durationMs - m1.durationMs)
  }
}
