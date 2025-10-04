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

package org.apache.spark.scheduler

import java.security.MessageDigest

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}

/**
 * A stable signature for identifying similar stages across different application runs.
 *
 * Stage IDs change between runs, so we use a combination of structural characteristics
 * to match stages. This includes SQL execution plan fingerprints (for SQL workloads),
 * RDD operation chains (for RDD API workloads), and stage dependencies.
 *
 * @param sqlPlanHash Hash of the SQL physical execution plan structure, if this stage
 *                    is part of a SQL execution
 * @param sqlPlanOperators Sequence of SQL operators in execution order
 *                         (e.g., ["Scan", "Filter", "HashJoin", "Exchange"])
 * @param rddOperationChain Sequence of RDD operation names in the lineage
 *                          (e.g., ["MapPartitionsRDD", "ShuffledRDD"])
 * @param rddChainHash Hash of the RDD operation chain
 * @param stageName Name of the stage
 * @param hasShuffleDependency Whether this stage has shuffle dependencies
 * @param shuffleDepId Optional shuffle dependency ID
 * @param numPartitions Number of partitions/tasks in the stage
 * @param parentStageCount Number of parent stages
 * @param callSitePattern Optional pattern extracted from call site (less stable)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class StageSignature(
    @JsonProperty sqlPlanHash: Option[String] = None,
    @JsonProperty sqlPlanOperators: Option[Seq[String]] = None,
    @JsonProperty rddOperationChain: Seq[String] = Seq.empty,
    @JsonProperty rddChainHash: String,
    @JsonProperty stageName: String,
    @JsonProperty hasShuffleDependency: Boolean,
    @JsonProperty shuffleDepId: Option[Int] = None,
    @JsonProperty numPartitions: Int,
    @JsonProperty parentStageCount: Int,
    @JsonProperty callSitePattern: Option[String] = None) {

  /**
   * Generate a stable hash from this signature for exact matching.
   * Uses the most stable components (SQL plan, RDD chain, shuffle info).
   */
  def toHash: String = {
    val components = Seq(
      sqlPlanHash.getOrElse(""),
      rddChainHash,
      hasShuffleDependency.toString,
      numPartitions.toString
    )
    hashComponents(components)
  }

  private def hashComponents(components: Seq[String]): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    components.foreach(c => digest.update(c.getBytes("UTF-8")))
    digest.digest().map("%02x".format(_)).mkString
  }
}

/**
 * Statistical metrics profile collected from historical stage executions.
 *
 * Contains aggregated metrics (average, percentiles) for resource planning.
 *
 * @param avgNumTasks Average number of tasks
 * @param minNumTasks Minimum observed task count
 * @param maxNumTasks Maximum observed task count
 * @param p50NumTasks Median task count
 * @param p95NumTasks 95th percentile task count
 * @param avgShuffleReadBytes Average shuffle read bytes
 * @param p95ShuffleReadBytes 95th percentile shuffle read bytes
 * @param avgShuffleWriteBytes Average shuffle write bytes
 * @param p95ShuffleWriteBytes 95th percentile shuffle write bytes
 * @param avgPeakMemoryPerTask Average peak memory per task (bytes)
 * @param p95PeakMemoryPerTask 95th percentile peak memory per task
 * @param avgTaskDuration Average task duration (milliseconds)
 * @param p95TaskDuration 95th percentile task duration
 * @param avgStageDuration Average stage duration (milliseconds)
 * @param recommendedExecutors Recommended number of executors
 * @param recommendedCoresPerExecutor Recommended cores per executor
 * @param recommendedMemoryPerExecutor Recommended memory per executor (bytes)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class StageMetricsProfile(
    @JsonProperty avgNumTasks: Double,
    @JsonProperty minNumTasks: Int,
    @JsonProperty maxNumTasks: Int,
    @JsonProperty p50NumTasks: Int,
    @JsonProperty p95NumTasks: Int,
    @JsonProperty avgShuffleReadBytes: Long,
    @JsonProperty p95ShuffleReadBytes: Long,
    @JsonProperty avgShuffleWriteBytes: Long,
    @JsonProperty p95ShuffleWriteBytes: Long,
    @JsonProperty avgPeakMemoryPerTask: Long,
    @JsonProperty p95PeakMemoryPerTask: Long,
    @JsonProperty avgTaskDuration: Long,
    @JsonProperty p95TaskDuration: Long,
    @JsonProperty avgStageDuration: Long,
    @JsonProperty recommendedExecutors: Int,
    @JsonProperty recommendedCoresPerExecutor: Int,
    @JsonProperty recommendedMemoryPerExecutor: Long)

/**
 * Historical performance profile for a stage pattern.
 *
 * Represents aggregated data from multiple observations of stages with the same signature.
 *
 * @param signature The stage signature
 * @param signatureHash Hash of the signature for efficient lookup
 * @param metrics Aggregated performance metrics
 * @param observationCount Number of times this pattern was observed
 * @param firstSeen Timestamp of first observation (milliseconds since epoch)
 * @param lastSeen Timestamp of last observation (milliseconds since epoch)
 * @param sourceAppIds Application IDs where this profile was collected from
 * @param profileVersion Version number for schema evolution
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class StageProfile(
    @JsonProperty signature: StageSignature,
    @JsonProperty signatureHash: String,
    @JsonProperty metrics: StageMetricsProfile,
    @JsonProperty observationCount: Int,
    @JsonProperty firstSeen: Long,
    @JsonProperty lastSeen: Long,
    @JsonProperty sourceAppIds: Seq[String] = Seq.empty,
    @JsonProperty profileVersion: Int = 1)

/**
 * Container for a collection of stage profiles, typically loaded from a JSON file.
 *
 * @param version Format version of the profile collection
 * @param applicationName Optional application name these profiles were collected from
 * @param collectedFrom List of application IDs these profiles came from
 * @param collectionTimestamp ISO-8601 timestamp when profiles were collected
 * @param profiles Sequence of stage profiles
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class StageProfileCollection(
    @JsonProperty version: String = "1.0",
    @JsonProperty applicationName: Option[String] = None,
    @JsonProperty collectedFrom: Seq[String] = Seq.empty,
    @JsonProperty collectionTimestamp: Option[String] = None,
    @JsonProperty profiles: Seq[StageProfile])
