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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.storage.RDDInfo

class StageMatcherSuite extends SparkFunSuite {

  private def createConf(
      confidenceThreshold: Double = 0.75,
      useSQLPlan: Boolean = true,
      useRDDChain: Boolean = true): SparkConf = {
    new SparkConf()
      .set(DYN_ALLOCATION_STAGE_MATCHING_CONFIDENCE_THRESHOLD, confidenceThreshold)
      .set(DYN_ALLOCATION_STAGE_MATCHING_USE_SQL_PLAN, useSQLPlan)
      .set(DYN_ALLOCATION_STAGE_MATCHING_USE_RDD_CHAIN, useRDDChain)
  }

  private def createStageInfo(
      stageId: Int,
      name: String,
      numTasks: Int,
      rddNames: Seq[String],
      hasShuffleDep: Boolean = false,
      parentIds: Seq[Int] = Seq.empty): StageInfo = {
    val rddInfos = rddNames.map { rddName =>
      new RDDInfo(
        id = 0,
        name = rddName,
        numPartitions = numTasks,
        storageLevel = null,
        isBarrier = false,
        parentIds = Seq.empty
      )
    }
    new StageInfo(
      stageId = stageId,
      attemptId = 0,
      name = name,
      numTasks = numTasks,
      rddInfos = rddInfos,
      parentIds = parentIds,
      details = "test details",
      taskMetrics = null,
      shuffleDepId = if (hasShuffleDep) Some(1) else None,
      resourceProfileId = 0
    )
  }

  private def createTestMetrics(): StageMetricsProfile = {
    StageMetricsProfile(
      avgNumTasks = 100.0,
      minNumTasks = 100,
      maxNumTasks = 100,
      p50NumTasks = 100,
      p95NumTasks = 100,
      avgShuffleReadBytes = 1000000L,
      p95ShuffleReadBytes = 1200000L,
      avgShuffleWriteBytes = 2000000L,
      p95ShuffleWriteBytes = 2400000L,
      avgPeakMemoryPerTask = 500000000L,
      p95PeakMemoryPerTask = 600000000L,
      avgTaskDuration = 5000L,
      p95TaskDuration = 7000L,
      avgStageDuration = 15000L,
      recommendedExecutors = 5,
      recommendedCoresPerExecutor = 4,
      recommendedMemoryPerExecutor = 4294967296L
    )
  }

  test("extract signature from StageInfo") {
    val conf = createConf()
    val store = new StageProfileStore(conf)
    val matcher = new StageMatcher(store, conf)

    val stageInfo = createStageInfo(
      stageId = 1,
      name = "map stage",
      numTasks = 100,
      rddNames = Seq("MapPartitionsRDD", "ShuffledRDD"),
      hasShuffleDep = true,
      parentIds = Seq(0)
    )

    val signature = matcher.extractSignature(stageInfo)

    assert(signature.stageName === "map stage")
    assert(signature.numPartitions === 100)
    assert(signature.hasShuffleDependency === true)
    assert(signature.parentStageCount === 1)
    assert(signature.rddOperationChain === Seq("MapPartitionsRDD", "ShuffledRDD"))
    assert(signature.rddChainHash.nonEmpty)
  }

  test("extract SQL plan operators") {
    val conf = createConf()
    val store = new StageProfileStore(conf)
    val matcher = new StageMatcher(store, conf)

    val stageInfo = createStageInfo(1, "sql stage", 100, Seq("MapPartitionsRDD"))
    val sqlPlan =
      """== Physical Plan ==
        |*(2) HashAggregate(keys=[id#0], functions=[sum(value#1)])
        |+- Exchange hashpartitioning(id#0, 200)
        |   +- *(1) HashAggregate(keys=[id#0], functions=[partial_sum(value#1)])
        |      +- *(1) Filter (value#1 > 10)
        |         +- *(1) Scan parquet default.table[]
        |""".stripMargin

    val signature = matcher.extractSignature(stageInfo, Some(sqlPlan))

    assert(signature.sqlPlanHash.isDefined)
    assert(signature.sqlPlanOperators.isDefined)
    val ops = signature.sqlPlanOperators.get
    assert(ops.contains("Scan"))
    assert(ops.contains("Filter"))
    assert(ops.contains("HashAggregate"))
    assert(ops.contains("Exchange"))
  }

  test("exact match by signature hash") {
    val conf = createConf()
    val store = new StageProfileStore(conf)
    val matcher = new StageMatcher(store, conf)

    val stageInfo = createStageInfo(1, "test stage", 100, Seq("MapPartitionsRDD"))
    val signature = matcher.extractSignature(stageInfo)
    val hash = signature.toHash

    val profile = StageProfile(
      signature = signature,
      signatureHash = hash,
      metrics = createTestMetrics(),
      observationCount = 10,
      firstSeen = 1000000000L,
      lastSeen = 2000000000L
    )

    store.updateProfile(profile)

    val result = matcher.matchStage(stageInfo)

    assert(result.isDefined)
    val (matchedProfile, confidence) = result.get
    assert(matchedProfile.signatureHash === hash)
    assert(confidence === 1.0)
  }

  test("no match when store is empty") {
    val conf = createConf()
    val store = new StageProfileStore(conf)
    val matcher = new StageMatcher(store, conf)

    val stageInfo = createStageInfo(1, "test stage", 100, Seq("MapPartitionsRDD"))
    val result = matcher.matchStage(stageInfo)

    assert(result.isEmpty)
  }

  test("fuzzy match with high similarity") {
    val conf = createConf(confidenceThreshold = 0.7)
    val store = new StageProfileStore(conf)
    val matcher = new StageMatcher(store, conf)

    // Create similar but not identical stages
    val originalSignature = StageSignature(
      rddOperationChain = Seq("MapPartitionsRDD", "ShuffledRDD"),
      rddChainHash = matcher.hashString("MapPartitionsRDD->ShuffledRDD"),
      stageName = "aggregation",
      hasShuffleDependency = true,
      numPartitions = 100,
      parentStageCount = 1
    )

    val profile = StageProfile(
      signature = originalSignature,
      signatureHash = originalSignature.toHash,
      metrics = createTestMetrics(),
      observationCount = 10,
      firstSeen = 1000000000L,
      lastSeen = 2000000000L
    )
    store.updateProfile(profile)

    // Similar stage with slightly different partition count
    val stageInfo = createStageInfo(
      stageId = 2,
      name = "aggregation",
      numTasks = 95,
      rddNames = Seq("MapPartitionsRDD", "ShuffledRDD"),
      hasShuffleDep = true,
      parentIds = Seq(1)
    )

    val result = matcher.matchStage(stageInfo)

    assert(result.isDefined)
    val (_, confidence) = result.get
    assert(confidence >= 0.7)
    assert(confidence < 1.0)
  }

  test("no fuzzy match when similarity is below threshold") {
    val conf = createConf(confidenceThreshold = 0.9)
    val store = new StageProfileStore(conf)
    val matcher = new StageMatcher(store, conf)

    val originalSignature = StageSignature(
      rddOperationChain = Seq("MapPartitionsRDD", "ShuffledRDD"),
      rddChainHash = matcher.hashString("MapPartitionsRDD->ShuffledRDD"),
      stageName = "aggregation",
      hasShuffleDependency = true,
      numPartitions = 100,
      parentStageCount = 1
    )

    val profile = StageProfile(
      signature = originalSignature,
      signatureHash = originalSignature.toHash,
      metrics = createTestMetrics(),
      observationCount = 10,
      firstSeen = 1000000000L,
      lastSeen = 2000000000L
    )
    store.updateProfile(profile)

    // Very different stage
    val stageInfo = createStageInfo(
      stageId = 2,
      name = "different stage",
      numTasks = 200,
      rddNames = Seq("CoalescedRDD"),
      hasShuffleDep = false,
      parentIds = Seq.empty
    )

    val result = matcher.matchStage(stageInfo)

    assert(result.isEmpty)
  }

  test("calculate similarity - identical signatures") {
    val conf = createConf()
    val store = new StageProfileStore(conf)
    val matcher = new StageMatcher(store, conf)

    val sig1 = StageSignature(
      rddOperationChain = Seq("MapPartitionsRDD", "ShuffledRDD"),
      rddChainHash = "hash1",
      stageName = "test",
      hasShuffleDependency = true,
      numPartitions = 100,
      parentStageCount = 1
    )

    val sig2 = sig1.copy()

    val similarity = matcher.calculateSimilarity(sig1, sig2)
    assert(similarity === 1.0)
  }

  test("calculate similarity - different RDD chains") {
    val conf = createConf()
    val store = new StageProfileStore(conf)
    val matcher = new StageMatcher(store, conf)

    val sig1 = StageSignature(
      rddOperationChain = Seq("MapPartitionsRDD", "ShuffledRDD"),
      rddChainHash = "hash1",
      stageName = "test",
      hasShuffleDependency = true,
      numPartitions = 100,
      parentStageCount = 1
    )

    val sig2 = sig1.copy(
      rddOperationChain = Seq("CoalescedRDD"),
      rddChainHash = "hash2"
    )

    val similarity = matcher.calculateSimilarity(sig1, sig2)
    assert(similarity < 1.0)
    assert(similarity > 0.0)
  }

  test("calculate similarity - with SQL plan match") {
    val conf = createConf()
    val store = new StageProfileStore(conf)
    val matcher = new StageMatcher(store, conf)

    val sig1 = StageSignature(
      sqlPlanHash = Some("sqlHash123"),
      sqlPlanOperators = Some(Seq("Scan", "Filter", "HashJoin")),
      rddOperationChain = Seq("MapPartitionsRDD"),
      rddChainHash = "hash1",
      stageName = "test",
      hasShuffleDependency = true,
      numPartitions = 100,
      parentStageCount = 1
    )

    val sig2 = sig1.copy()

    val similarity = matcher.calculateSimilarity(sig1, sig2)
    assert(similarity === 1.0)
  }

  test("calculate similarity - different SQL plans") {
    val conf = createConf()
    val store = new StageProfileStore(conf)
    val matcher = new StageMatcher(store, conf)

    val sig1 = StageSignature(
      sqlPlanHash = Some("sqlHash1"),
      sqlPlanOperators = Some(Seq("Scan", "Filter", "HashJoin")),
      rddOperationChain = Seq("MapPartitionsRDD"),
      rddChainHash = "hash1",
      stageName = "test",
      hasShuffleDependency = true,
      numPartitions = 100,
      parentStageCount = 1
    )

    val sig2 = sig1.copy(
      sqlPlanHash = Some("sqlHash2"),
      sqlPlanOperators = Some(Seq("Scan", "Project"))
    )

    val similarity = matcher.calculateSimilarity(sig1, sig2)
    assert(similarity < 1.0)
  }

  test("calculate similarity - partition count variance") {
    val conf = createConf()
    val store = new StageProfileStore(conf)
    val matcher = new StageMatcher(store, conf)

    val sig1 = StageSignature(
      rddOperationChain = Seq("MapPartitionsRDD"),
      rddChainHash = "hash1",
      stageName = "test",
      hasShuffleDependency = true,
      numPartitions = 100,
      parentStageCount = 1
    )

    // 85% of original partition count (should get partial credit)
    val sig2 = sig1.copy(numPartitions = 85)

    val similarity = matcher.calculateSimilarity(sig1, sig2)
    assert(similarity > 0.8)
    assert(similarity < 1.0)
  }

  test("hash string produces consistent results") {
    val conf = createConf()
    val store = new StageProfileStore(conf)
    val matcher = new StageMatcher(store, conf)

    val input = "test string"
    val hash1 = matcher.hashString(input)
    val hash2 = matcher.hashString(input)

    assert(hash1 === hash2)
    assert(hash1.length === 64) // SHA-256 produces 64 hex chars
  }

  test("hash string produces different results for different inputs") {
    val conf = createConf()
    val store = new StageProfileStore(conf)
    val matcher = new StageMatcher(store, conf)

    val hash1 = matcher.hashString("input1")
    val hash2 = matcher.hashString("input2")

    assert(hash1 !== hash2)
  }

  test("signature toHash is stable") {
    val sig1 = StageSignature(
      sqlPlanHash = Some("sqlHash"),
      rddOperationChain = Seq("MapPartitionsRDD"),
      rddChainHash = "rddHash",
      stageName = "test",
      hasShuffleDependency = true,
      numPartitions = 100,
      parentStageCount = 1
    )

    val sig2 = sig1.copy()

    assert(sig1.toHash === sig2.toHash)
  }

  test("match prefers exact match over fuzzy match") {
    val conf = createConf(confidenceThreshold = 0.5)
    val store = new StageProfileStore(conf)
    val matcher = new StageMatcher(store, conf)

    val exactSignature = StageSignature(
      rddOperationChain = Seq("MapPartitionsRDD", "ShuffledRDD"),
      rddChainHash = matcher.hashString("MapPartitionsRDD->ShuffledRDD"),
      stageName = "exact",
      hasShuffleDependency = true,
      numPartitions = 100,
      parentStageCount = 1
    )

    val similarSignature = exactSignature.copy(
      stageName = "similar",
      numPartitions = 95
    )

    val exactProfile = StageProfile(
      signature = exactSignature,
      signatureHash = exactSignature.toHash,
      metrics = createTestMetrics(),
      observationCount = 10,
      firstSeen = 1000000000L,
      lastSeen = 2000000000L
    )

    val similarProfile = StageProfile(
      signature = similarSignature,
      signatureHash = similarSignature.toHash,
      metrics = createTestMetrics(),
      observationCount = 5,
      firstSeen = 1000000000L,
      lastSeen = 2000000000L
    )

    store.updateProfile(exactProfile)
    store.updateProfile(similarProfile)

    val stageInfo = createStageInfo(
      stageId = 1,
      name = "exact",
      numTasks = 100,
      rddNames = Seq("MapPartitionsRDD", "ShuffledRDD"),
      hasShuffleDep = true,
      parentIds = Seq(0)
    )

    val result = matcher.matchStage(stageInfo)

    assert(result.isDefined)
    val (profile, confidence) = result.get
    assert(confidence === 1.0)
    assert(profile.signatureHash === exactSignature.toHash)
  }

  test("disable SQL plan matching") {
    val conf = createConf(useSQLPlan = false)
    val store = new StageProfileStore(conf)
    val matcher = new StageMatcher(store, conf)

    val stageInfo = createStageInfo(1, "test", 100, Seq("MapPartitionsRDD"))
    val sqlPlan = "Scan parquet"

    val signature = matcher.extractSignature(stageInfo, Some(sqlPlan))

    assert(signature.sqlPlanHash.isEmpty)
    assert(signature.sqlPlanOperators.isEmpty)
  }

  test("disable RDD chain matching") {
    val conf = createConf(useRDDChain = false)
    val store = new StageProfileStore(conf)
    val matcher = new StageMatcher(store, conf)

    val sig1 = StageSignature(
      rddOperationChain = Seq("MapPartitionsRDD"),
      rddChainHash = "hash1",
      stageName = "test",
      hasShuffleDependency = false,
      numPartitions = 100,
      parentStageCount = 0
    )

    val sig2 = sig1.copy(
      rddOperationChain = Seq("DifferentRDD"),
      rddChainHash = "hash2"
    )

    // When RDD chain is disabled, similarity should still be > 0 based on other factors
    val similarity = matcher.calculateSimilarity(sig1, sig2)
    assert(similarity > 0.0)
  }
}
