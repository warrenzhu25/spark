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

import javax.ws.rs.core.MediaType
import javax.ws.rs.{GET, Path, Produces}
import org.apache.spark.JobExecutionStatus
import org.apache.spark.status.insight.SparkApplicationData
import org.apache.spark.status.insight.heuristics.{AnalysisResult, ConfigurationHeuristic, ConfigurationParametersHeuristic, DriverHeuristic, ExecutorGcHeuristic, ExecutorsHeuristic, HeuristicResult, JobsHeuristic, StagesHeuristic, StagesWithFailedTasksHeuristic}

import scala.collection.JavaConverters._

@Produces(Array(MediaType.APPLICATION_JSON))
private[v1] class AppInsightResource extends BaseAppResource {

  private val heuristic = Seq(
    ConfigurationHeuristic,
    ExecutorGcHeuristic,
    ExecutorsHeuristic,
    JobsHeuristic,
    StagesHeuristic,
    ConfigurationParametersHeuristic,
    DriverHeuristic,
    StagesWithFailedTasksHeuristic
  )

  @GET
  @Path("insights")
  def insights(): Seq[HeuristicResult] = {
    heuristic.map(_.apply(appData()))
  }

  private def appData(): SparkApplicationData = {
    val applicationInfo = uiRoot.getApplicationInfo(appId).get
    val appConfig = withUI(_.store.environmentInfo()).sparkProperties.map(a => a._1 -> a._2).toMap
    val jobData = withUI(_.store.jobsList(List.empty[JobExecutionStatus].asJava))
    val stageData = withUI(_.store.stageList(List.empty[StageStatus].asJava))
    val executorSummary = withUI(_.store.executorList(false))

    SparkApplicationData(appId, appConfig, applicationInfo, jobData, stageData, executorSummary)
  }
}
