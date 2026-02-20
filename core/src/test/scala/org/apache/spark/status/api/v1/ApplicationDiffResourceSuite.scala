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

import org.apache.spark.SparkFunSuite

/**
 * Unit tests for ApplicationDiffResource diff API data classes.
 * Integration tests are covered by HistoryServerSuite.
 */
class ApplicationDiffResourceSuite extends SparkFunSuite {

  test("JobMetricsValues holds correct values") {
    val metrics = new JobMetricsValues(10, 8, 2)
    assert(metrics.numJobs === 10)
    assert(metrics.succeededJobs === 8)
    assert(metrics.failedJobs === 2)
  }

  test("StageMetricsValues holds correct values") {
    val metrics = new StageMetricsValues(20, 15, 3, 2)
    assert(metrics.numStages === 20)
    assert(metrics.completedStages === 15)
    assert(metrics.failedStages === 3)
    assert(metrics.skippedStages === 2)
  }

  test("TaskMetricsValues holds correct values") {
    val metrics = new TaskMetricsValues(100L, 90L, 5L, 3L, 2L)
    assert(metrics.totalTasks === 100L)
    assert(metrics.completedTasks === 90L)
    assert(metrics.failedTasks === 5L)
    assert(metrics.killedTasks === 3L)
    assert(metrics.skippedTasks === 2L)
  }

  test("ExecutorMetricsValues holds correct values") {
    val metrics = new ExecutorMetricsValues(1000L, 100L, 5000L, 3000L, 2000L)
    assert(metrics.totalExecutorRunTime === 1000L)
    assert(metrics.totalJvmGcTime === 100L)
    assert(metrics.totalInputBytes === 5000L)
    assert(metrics.totalShuffleReadBytes === 3000L)
    assert(metrics.totalShuffleWriteBytes === 2000L)
  }

  test("DurationMetricsValues holds correct values") {
    val metrics = new DurationMetricsValues(60000L)
    assert(metrics.durationMs === 60000L)
  }

  test("ApplicationSummary holds correct values") {
    val summary = new ApplicationSummary("app-123", "TestApp", Some("1"))
    assert(summary.id === "app-123")
    assert(summary.name === "TestApp")
    assert(summary.attemptId === Some("1"))
  }

  test("JobMetricsComparison calculates diff correctly") {
    val app1 = new JobMetricsValues(10, 8, 2)
    val app2 = new JobMetricsValues(15, 12, 3)
    val diff = new JobMetricsValues(5, 4, 1)
    val comparison = new JobMetricsComparison(app1, app2, diff)

    assert(comparison.app1.numJobs === 10)
    assert(comparison.app2.numJobs === 15)
    assert(comparison.diff.numJobs === 5)
    assert(comparison.diff.succeededJobs === 4)
    assert(comparison.diff.failedJobs === 1)
  }

  test("StageMetricsComparison calculates diff correctly") {
    val app1 = new StageMetricsValues(10, 8, 1, 1)
    val app2 = new StageMetricsValues(15, 12, 2, 1)
    val diff = new StageMetricsValues(5, 4, 1, 0)
    val comparison = new StageMetricsComparison(app1, app2, diff)

    assert(comparison.app1.numStages === 10)
    assert(comparison.app2.numStages === 15)
    assert(comparison.diff.numStages === 5)
    assert(comparison.diff.completedStages === 4)
    assert(comparison.diff.failedStages === 1)
    assert(comparison.diff.skippedStages === 0)
  }

  test("TaskMetricsComparison calculates diff correctly") {
    val app1 = new TaskMetricsValues(100L, 90L, 5L, 3L, 2L)
    val app2 = new TaskMetricsValues(150L, 140L, 6L, 2L, 2L)
    val diff = new TaskMetricsValues(50L, 50L, 1L, -1L, 0L)
    val comparison = new TaskMetricsComparison(app1, app2, diff)

    assert(comparison.app1.totalTasks === 100L)
    assert(comparison.app2.totalTasks === 150L)
    assert(comparison.diff.totalTasks === 50L)
    assert(comparison.diff.completedTasks === 50L)
    assert(comparison.diff.failedTasks === 1L)
    assert(comparison.diff.killedTasks === -1L)
    assert(comparison.diff.skippedTasks === 0L)
  }

  test("ExecutorMetricsComparison calculates diff correctly") {
    val app1 = new ExecutorMetricsValues(1000L, 100L, 5000L, 3000L, 2000L)
    val app2 = new ExecutorMetricsValues(1500L, 150L, 7000L, 4000L, 3000L)
    val diff = new ExecutorMetricsValues(500L, 50L, 2000L, 1000L, 1000L)
    val comparison = new ExecutorMetricsComparison(app1, app2, diff)

    assert(comparison.app1.totalExecutorRunTime === 1000L)
    assert(comparison.app2.totalExecutorRunTime === 1500L)
    assert(comparison.diff.totalExecutorRunTime === 500L)
    assert(comparison.diff.totalJvmGcTime === 50L)
    assert(comparison.diff.totalInputBytes === 2000L)
    assert(comparison.diff.totalShuffleReadBytes === 1000L)
    assert(comparison.diff.totalShuffleWriteBytes === 1000L)
  }

  test("DurationMetricsComparison calculates diff correctly") {
    val app1 = new DurationMetricsValues(60000L)
    val app2 = new DurationMetricsValues(90000L)
    val diff = new DurationMetricsValues(30000L)
    val comparison = new DurationMetricsComparison(app1, app2, diff)

    assert(comparison.app1.durationMs === 60000L)
    assert(comparison.app2.durationMs === 90000L)
    assert(comparison.diff.durationMs === 30000L)
  }

  test("ApplicationDiffInfo contains all comparison data") {
    val app1Summary = new ApplicationSummary("app-1", "App1", Some("1"))
    val app2Summary = new ApplicationSummary("app-2", "App2", Some("1"))

    val jobMetrics = new JobMetricsComparison(
      new JobMetricsValues(10, 8, 2),
      new JobMetricsValues(15, 12, 3),
      new JobMetricsValues(5, 4, 1))

    val stageMetrics = new StageMetricsComparison(
      new StageMetricsValues(10, 8, 1, 1),
      new StageMetricsValues(15, 12, 2, 1),
      new StageMetricsValues(5, 4, 1, 0))

    val taskMetrics = new TaskMetricsComparison(
      new TaskMetricsValues(100L, 90L, 5L, 3L, 2L),
      new TaskMetricsValues(150L, 140L, 6L, 2L, 2L),
      new TaskMetricsValues(50L, 50L, 1L, -1L, 0L))

    val executorMetrics = new ExecutorMetricsComparison(
      new ExecutorMetricsValues(1000L, 100L, 5000L, 3000L, 2000L),
      new ExecutorMetricsValues(1500L, 150L, 7000L, 4000L, 3000L),
      new ExecutorMetricsValues(500L, 50L, 2000L, 1000L, 1000L))

    val durationMetrics = new DurationMetricsComparison(
      new DurationMetricsValues(60000L),
      new DurationMetricsValues(90000L),
      new DurationMetricsValues(30000L))

    val diffInfo = new ApplicationDiffInfo(
      app1Summary,
      app2Summary,
      jobMetrics,
      stageMetrics,
      taskMetrics,
      executorMetrics,
      durationMetrics)

    assert(diffInfo.app1.id === "app-1")
    assert(diffInfo.app2.id === "app-2")
    assert(diffInfo.jobMetrics.diff.numJobs === 5)
    assert(diffInfo.stageMetrics.diff.numStages === 5)
    assert(diffInfo.taskMetrics.diff.totalTasks === 50L)
    assert(diffInfo.executorMetrics.diff.totalExecutorRunTime === 500L)
    assert(diffInfo.durationMetrics.diff.durationMs === 30000L)
  }

  test("negative diff values when app2 has lower metrics than app1") {
    val app1 = new JobMetricsValues(15, 12, 3)
    val app2 = new JobMetricsValues(10, 8, 2)
    val diff = new JobMetricsValues(-5, -4, -1)
    val comparison = new JobMetricsComparison(app1, app2, diff)

    assert(comparison.diff.numJobs === -5)
    assert(comparison.diff.succeededJobs === -4)
    assert(comparison.diff.failedJobs === -1)
  }
}
