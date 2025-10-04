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

import java.io.File

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.util.Utils

class StageProfileStoreSuite extends SparkFunSuite {

  private var tempDir: File = _
  private var conf: SparkConf = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    tempDir = Utils.createTempDir()
    conf = new SparkConf()
  }

  override def afterEach(): Unit = {
    if (tempDir != null) {
      Utils.deleteRecursively(tempDir)
    }
    super.afterEach()
  }

  test("create empty store") {
    val store = new StageProfileStore(conf)
    assert(store.size === 0)
    assert(store.getAllProfiles().isEmpty)
  }

  test("add and retrieve profile") {
    val store = new StageProfileStore(conf)
    val profile = createTestProfile("sig1")

    store.updateProfile(profile)

    assert(store.size === 1)
    assert(store.getProfile("sig1").isDefined)
    assert(store.getProfile("sig1").get === profile)
    assert(store.getProfile("nonexistent").isEmpty)
  }

  test("update existing profile") {
    val store = new StageProfileStore(conf)
    val profile1 = createTestProfile("sig1", observationCount = 5)
    val profile2 = createTestProfile("sig1", observationCount = 10)

    store.updateProfile(profile1)
    assert(store.getProfile("sig1").get.observationCount === 5)

    store.updateProfile(profile2)
    assert(store.size === 1)
    assert(store.getProfile("sig1").get.observationCount === 10)
  }

  test("get all profiles") {
    val store = new StageProfileStore(conf)
    val profile1 = createTestProfile("sig1")
    val profile2 = createTestProfile("sig2")
    val profile3 = createTestProfile("sig3")

    store.updateProfile(profile1)
    store.updateProfile(profile2)
    store.updateProfile(profile3)

    val allProfiles = store.getAllProfiles()
    assert(allProfiles.size === 3)
    assert(allProfiles.map(_.signatureHash).toSet === Set("sig1", "sig2", "sig3"))
  }

  test("clear store") {
    val store = new StageProfileStore(conf)
    store.updateProfile(createTestProfile("sig1"))
    store.updateProfile(createTestProfile("sig2"))
    assert(store.size === 2)

    store.clear()
    assert(store.size === 0)
    assert(store.getAllProfiles().isEmpty)
  }

  test("export to JSON") {
    val store = new StageProfileStore(conf)
    val profile = createTestProfile("sig1")
    store.updateProfile(profile)

    val json = store.exportToJson(Some("TestApp"), Seq("app-001"))

    assert(json.contains("\"version\""))
    assert(json.contains("\"applicationName\""))
    assert(json.contains("TestApp"))
    assert(json.contains("\"signatureHash\""))
    assert(json.contains("sig1"))
  }

  test("load from JSON string") {
    val json =
      """{
        |  "version": "1.0",
        |  "profiles": [
        |    {
        |      "signatureHash": "hash1",
        |      "signature": {
        |        "rddOperationChain": ["MapPartitionsRDD", "ShuffledRDD"],
        |        "rddChainHash": "rddHash1",
        |        "stageName": "test stage",
        |        "hasShuffleDependency": true,
        |        "numPartitions": 100,
        |        "parentStageCount": 1
        |      },
        |      "metrics": {
        |        "avgNumTasks": 100.0,
        |        "minNumTasks": 100,
        |        "maxNumTasks": 100,
        |        "p50NumTasks": 100,
        |        "p95NumTasks": 100,
        |        "avgShuffleReadBytes": 1000000,
        |        "p95ShuffleReadBytes": 1200000,
        |        "avgShuffleWriteBytes": 2000000,
        |        "p95ShuffleWriteBytes": 2400000,
        |        "avgPeakMemoryPerTask": 500000000,
        |        "p95PeakMemoryPerTask": 600000000,
        |        "avgTaskDuration": 5000,
        |        "p95TaskDuration": 7000,
        |        "avgStageDuration": 15000,
        |        "recommendedExecutors": 5,
        |        "recommendedCoresPerExecutor": 4,
        |        "recommendedMemoryPerExecutor": 4294967296
        |      },
        |      "observationCount": 10,
        |      "firstSeen": 1000000000,
        |      "lastSeen": 2000000000,
        |      "profileVersion": 1
        |    }
        |  ]
        |}""".stripMargin

    val store = new StageProfileStore(conf)
    val profiles = store.loadFromJson(json)

    assert(profiles.size === 1)
    assert(store.size === 1)

    val profile = store.getProfile("hash1").get
    assert(profile.signatureHash === "hash1")
    assert(profile.signature.stageName === "test stage")
    assert(profile.metrics.avgNumTasks === 100.0)
    assert(profile.metrics.recommendedExecutors === 5)
    assert(profile.observationCount === 10)
  }

  test("save and load from file") {
    val store1 = new StageProfileStore(conf)
    val profile1 = createTestProfile("sig1")
    val profile2 = createTestProfile("sig2")

    store1.updateProfile(profile1)
    store1.updateProfile(profile2)

    val filePath = new File(tempDir, "profiles.json").getAbsolutePath
    store1.saveToFile(filePath, Some("TestApp"), Seq("app-001"))

    // Load into new store
    val store2 = new StageProfileStore(conf)
    store2.loadFromFile(filePath)

    assert(store2.size === 2)
    assert(store2.getProfile("sig1").isDefined)
    assert(store2.getProfile("sig2").isDefined)
  }

  test("save and load with file:// prefix") {
    val store1 = new StageProfileStore(conf)
    store1.updateProfile(createTestProfile("sig1"))

    val filePath = "file://" + new File(tempDir, "profiles2.json").getAbsolutePath
    store1.saveToFile(filePath)

    val store2 = new StageProfileStore(conf)
    store2.loadFromFile(filePath)

    assert(store2.size === 1)
    assert(store2.getProfile("sig1").isDefined)
  }

  test("handle invalid JSON") {
    val store = new StageProfileStore(conf)
    val invalidJson = "{ invalid json }"

    intercept[Exception] {
      store.loadFromJson(invalidJson)
    }
  }

  test("handle nonexistent file") {
    val store = new StageProfileStore(conf)
    val nonexistentPath = new File(tempDir, "nonexistent.json").getAbsolutePath

    intercept[Exception] {
      store.loadFromFile(nonexistentPath)
    }
  }

  test("JSON round-trip preserves data") {
    val store1 = new StageProfileStore(conf)
    val profile = StageProfile(
      signature = StageSignature(
        sqlPlanHash = Some("sqlHash123"),
        sqlPlanOperators = Some(Seq("Scan", "Filter", "HashJoin")),
        rddOperationChain = Seq("MapPartitionsRDD", "ShuffledRDD"),
        rddChainHash = "rddHash123",
        stageName = "aggregation stage",
        hasShuffleDependency = true,
        shuffleDepId = Some(42),
        numPartitions = 200,
        parentStageCount = 2,
        callSitePattern = Some("org.example.MyApp.process")
      ),
      signatureHash = "fullHash123",
      metrics = StageMetricsProfile(
        avgNumTasks = 200.5,
        minNumTasks = 150,
        maxNumTasks = 250,
        p50NumTasks = 200,
        p95NumTasks = 240,
        avgShuffleReadBytes = 10737418240L,
        p95ShuffleReadBytes = 12884901888L,
        avgShuffleWriteBytes = 5368709120L,
        p95ShuffleWriteBytes = 6442450944L,
        avgPeakMemoryPerTask = 536870912L,
        p95PeakMemoryPerTask = 644245094L,
        avgTaskDuration = 5000L,
        p95TaskDuration = 7000L,
        avgStageDuration = 15000L,
        recommendedExecutors = 10,
        recommendedCoresPerExecutor = 4,
        recommendedMemoryPerExecutor = 4294967296L
      ),
      observationCount = 25,
      firstSeen = 1704268800000L,
      lastSeen = 1704355200000L,
      sourceAppIds = Seq("app-001", "app-002"),
      profileVersion = 1
    )

    store1.updateProfile(profile)
    val json = store1.exportToJson()

    val store2 = new StageProfileStore(conf)
    store2.loadFromJson(json)

    val loaded = store2.getProfile("fullHash123").get

    assert(loaded.signatureHash === profile.signatureHash)
    assert(loaded.signature.sqlPlanHash === profile.signature.sqlPlanHash)
    assert(loaded.signature.sqlPlanOperators === profile.signature.sqlPlanOperators)
    assert(loaded.signature.rddOperationChain === profile.signature.rddOperationChain)
    assert(loaded.signature.stageName === profile.signature.stageName)
    assert(loaded.signature.hasShuffleDependency === profile.signature.hasShuffleDependency)
    assert(loaded.signature.shuffleDepId === profile.signature.shuffleDepId)
    assert(loaded.signature.numPartitions === profile.signature.numPartitions)
    assert(loaded.signature.callSitePattern === profile.signature.callSitePattern)
    assert(loaded.metrics.avgNumTasks === profile.metrics.avgNumTasks)
    assert(loaded.metrics.recommendedExecutors === profile.metrics.recommendedExecutors)
    assert(loaded.observationCount === profile.observationCount)
  }

  /** Helper method to create a test profile */
  private def createTestProfile(
      hash: String,
      observationCount: Int = 1): StageProfile = {
    StageProfile(
      signature = StageSignature(
        rddOperationChain = Seq("MapPartitionsRDD"),
        rddChainHash = "rddHash",
        stageName = "test stage",
        hasShuffleDependency = false,
        numPartitions = 10,
        parentStageCount = 0
      ),
      signatureHash = hash,
      metrics = StageMetricsProfile(
        avgNumTasks = 10.0,
        minNumTasks = 10,
        maxNumTasks = 10,
        p50NumTasks = 10,
        p95NumTasks = 10,
        avgShuffleReadBytes = 0L,
        p95ShuffleReadBytes = 0L,
        avgShuffleWriteBytes = 0L,
        p95ShuffleWriteBytes = 0L,
        avgPeakMemoryPerTask = 100000000L,
        p95PeakMemoryPerTask = 120000000L,
        avgTaskDuration = 1000L,
        p95TaskDuration = 1500L,
        avgStageDuration = 5000L,
        recommendedExecutors = 2,
        recommendedCoresPerExecutor = 2,
        recommendedMemoryPerExecutor = 2147483648L
      ),
      observationCount = observationCount,
      firstSeen = 1000000000L,
      lastSeen = 2000000000L
    )
  }
}
