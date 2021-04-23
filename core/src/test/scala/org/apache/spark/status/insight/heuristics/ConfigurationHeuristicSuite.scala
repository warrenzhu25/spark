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
package org.apache.spark.status.insight.heuristics

import org.apache.spark.SparkFunSuite
import org.apache.spark.status.insight.heuristics.ConfigurationHeuristic._
import org.apache.spark.status.insight.heuristics.ConfigurationHeuristicsConstants._

class ConfigurationHeuristicSuite extends SparkFunSuite {

  test("CompressedOops not suggested") {
    val sparkConf = Map(SPARK_EXECUTOR_OPTIONS -> CompressedOopsEvaluator.ENABLE_COMPRESSED_OOPS)
    val result = CompressedOopsEvaluator.evaluate(sparkConf)
    assert(result.isEmpty)
  }

  test("CompressedOops suggested") {
    val sparkConf = Map[String, String]()
    val result = CompressedOopsEvaluator.evaluate(sparkConf)
    assert(result.nonEmpty)
    assert(result.head.isInstanceOf[AnalysisRecord])
    val singleValue = result.head.asInstanceOf[AnalysisRecord]
    assert(singleValue.name == SPARK_EXECUTOR_OPTIONS)
    assert(singleValue.value == "")
    assert(singleValue.suggested == CompressedOopsEvaluator.ENABLE_COMPRESSED_OOPS)
  }

  test("ParallelGCThreads suggested") {
    val sparkConf = Map[String, String]()
    val result = ParallelGcThreadEvaluator.evaluate(sparkConf)
    assert(result.nonEmpty)
    assert(result.head.isInstanceOf[AnalysisRecord])
    val singleValue = result.head.asInstanceOf[AnalysisRecord]
    assert(singleValue.name == SPARK_EXECUTOR_OPTIONS)
    assert(singleValue.value == "")
    assert(singleValue.suggested == ParallelGcThreadEvaluator.getParallelGCThreadsOption(1))
  }

  test("ParallelGCThreads not suggested") {
    val sparkConf = Map(
      SPARK_EXECUTOR_OPTIONS -> ParallelGcThreadEvaluator.getParallelGCThreadsOption(1))
    val result = ParallelGcThreadEvaluator.evaluate(sparkConf)
    assert(result.isEmpty)
  }

  test("ShuffleService suggested") {
    val sparkConf = Map(SPARK_DYNAMIC_ALLOCATION_ENABLED -> "true")
    val result = ShuffleServiceEvaluator.evaluate(sparkConf)
    assert(result.nonEmpty)
    assert(result.head.isInstanceOf[AnalysisRecord])
    val singleValue = result.head.asInstanceOf[AnalysisRecord]
    assert(singleValue.name == SPARK_SHUFFLE_SERVICE_ENABLED)
    assert(singleValue.value == "false")
    assert(singleValue.suggested == "true")
  }

  test("Executor cores suggested") {
    val sparkConf = Map[String, String]()
    val result = ExecutorCoreEvaluator.evaluate(sparkConf)
    assert(result.nonEmpty)
    assert(result.head.isInstanceOf[AnalysisRecord])
    val singleValue = result.head.asInstanceOf[AnalysisRecord]
    assert(singleValue.name == SPARK_EXECUTOR_CORES)
    assert(singleValue.value == "1")
    assert(singleValue.suggested == ExecutorCoreEvaluator.SUGGESTED_EXECUTOR_CORES)
  }

  test("Executor memory overhead suggested") {
    val sparkConf = Map(SPARK_OFF_HEAP_ENABLED -> "true",
      SPARK_OFF_HEAP_SIZE -> "1g")
    val result = ExecutorMemoryOverheadEvaluator.evaluate(sparkConf)
    assert(result.nonEmpty)
    assert(result.head.isInstanceOf[AnalysisRecord])
    val singleValue = result.head.asInstanceOf[AnalysisRecord]
    assert(singleValue.name == SPARK_EXECUTOR_MEMORY_OVERHEAD)
    assert(singleValue.value == "384m")
    assert(singleValue.suggested == "1408m")
  }
}
