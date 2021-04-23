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

package org.apache.spark.status.insight

import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1.{ApplicationInfo, ExecutorSummary, JobData, StageData}
import org.apache.spark.status.api.v1.ExecutorPeakMetricsDistributions


case class SparkApplicationData(
  store: AppStatusStore,
  appId: String,
  appConf: Map[String, String],
  appInfo: ApplicationInfo,
  jobData: Seq[JobData],
  stageData: Seq[StageData],
  executorSummaries: Seq[ExecutorSummary],
  executorMetricsDistributions: Option[ExecutorPeakMetricsDistributions],
  stagesWithFailedTasks: Seq[StageData] = Seq.empty) {

  private val DEFAULT_QUANTILES = Seq(0, 0.25, 0.5, 0.75, 1).toArray

  lazy val stageDataWithSummaries: Seq[StageData] = {
    stageData
      .sortBy(_.executorRunTime)
      .reverse
      .take(30)
      .map(s => store.newStageData(s, withSummaries = true, unsortedQuantiles = DEFAULT_QUANTILES))
  }
}


