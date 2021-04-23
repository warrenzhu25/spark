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

import org.apache.spark.status.insight.SparkApplicationData
import org.apache.spark.status.insight.heuristics.ConfigurationHeuristicsConstants._
import org.apache.spark.util.Utils

/**
 * A heuristic based on an app's known configuration
 *
 */
object ConfigurationHeuristic extends Heuristic {

  val THRESHOLD_MIN_EXECUTORS: Int = 1
  val THRESHOLD_MAX_EXECUTORS: Int = 2000

  val evaluators = Seq(
    KyroSerializerEvaluator,
    ShuffleServiceEvaluator,
    ExecutorCoreEvaluator,
    ExecutorMemoryOverheadEvaluator,
    CompressedOopsEvaluator,
    ParallelGcThreadEvaluator
  )

  object KyroSerializerEvaluator extends ConfigEvaluator {
    override def evaluate(appConf: Map[String, String]): Seq[AnalysisRecord] = {
      val sparkSerializer = getProperty(appConf, SPARK_SERIALIZER)
      if(sparkSerializer.isEmpty) {
        Seq(AnalysisRecord(
          SPARK_SERIALIZER,
          "",
          "Kryo is significantly faster and more compact." +
            "You need to add 'conf.registerKryoClasses(Array(classOf[MyClass1])'",
          "org.apache.spark.serializer.KryoSerializer",
          Severity.LOW
        ))
      } else {
        Seq.empty
      }
    }
  }

  object ShuffleServiceEvaluator extends ConfigEvaluator {
    override def evaluate(appConf: Map[String, String]): Seq[AnalysisRecord] = {
      val dynamicAllocationEnabled = getProperty(appConf, SPARK_DYNAMIC_ALLOCATION_ENABLED)
        .getOrElse("false").toBoolean
      val shuffleServiceEnabled = getProperty(appConf, SPARK_SHUFFLE_SERVICE_ENABLED)
        .getOrElse("false").toBoolean
      if(dynamicAllocationEnabled && !shuffleServiceEnabled) {
        Seq(AnalysisRecord(
          SPARK_SHUFFLE_SERVICE_ENABLED,
          "false",
          "Spark shuffle service is not enabled when dynamic allocation is enabled.",
          "true"))
      } else {
        Seq.empty
      }
    }
  }

  object ExecutorCoreEvaluator extends ConfigEvaluator {
    private val MIN_EXECUTOR_CORES = 2
    private val MAX_EXECUTOR_CORES = 5
    val SUGGESTED_EXECUTOR_CORES = "4"
    override def evaluate(appConf: Map[String, String]): Seq[AnalysisRecord] = {
      val executorCores = getProperty(appConf, SPARK_EXECUTOR_CORES)
        .map(_.toInt)
        .getOrElse(SPARK_EXECUTOR_CORES_DEFAULT)
      if(executorCores < MIN_EXECUTOR_CORES || executorCores >= MAX_EXECUTOR_CORES) {
        Seq(AnalysisRecord(
          SPARK_EXECUTOR_CORES,
          executorCores.toString,
          "Spark executor cores should be between 2 and 5. " +
            "Too small cores will have higher overhead, " +
            "and too many cores lead to poor performance due to HDFS concurrent read issues.",
          SUGGESTED_EXECUTOR_CORES))
      } else {
        Seq.empty
      }
    }
  }

  object CompressedOopsEvaluator extends ConfigEvaluator {
    val COMPRESSED_OOPS_THRESHOLD = 32
    val ENABLE_COMPRESSED_OOPS = "-XX:+UseCompressedOops"
    override def evaluate(appConf: Map[String, String]): Seq[AnalysisRecord] = {
      val executorMemory = getProperty(appConf, SPARK_EXECUTOR_MEMORY)
        .getOrElse(SPARK_EXECUTOR_MEMORY_DEFAULT)
      val executorJavaOptions = getProperty(appConf, SPARK_EXECUTOR_OPTIONS).getOrElse("")
      if(Utils.byteStringAsGb(executorMemory) < COMPRESSED_OOPS_THRESHOLD &&
        !executorJavaOptions.contains(ENABLE_COMPRESSED_OOPS)) {
        Seq(AnalysisRecord(
          SPARK_EXECUTOR_OPTIONS,
          "",
          "When executor memory is smaller than 32g, " +
            "use this option to make pointers be four bytes instead of eight.",
          ENABLE_COMPRESSED_OOPS))
      } else {
        Seq.empty
      }
    }
  }

  object ParallelGcThreadEvaluator extends ConfigEvaluator {
    val PARALLEL_GC_THREADS = "-XX:ParallelGCThreads="
    override def evaluate(appConf: Map[String, String]): Seq[AnalysisRecord] = {
      val executorCore = getProperty(appConf, SPARK_EXECUTOR_CORES)
        .map(_.toInt)
        .getOrElse(SPARK_EXECUTOR_CORES_DEFAULT)
      val parallelGCThreads = getProperty(appConf, SPARK_EXECUTOR_OPTIONS)
        .flatMap(_.split("""\s+""").find(s => s.startsWith(PARALLEL_GC_THREADS)))
        .map(s => s.substring(PARALLEL_GC_THREADS.length))

      if(parallelGCThreads.isEmpty || parallelGCThreads.get.toInt != executorCore) {
        Seq(AnalysisRecord(
          SPARK_EXECUTOR_OPTIONS,
          parallelGCThreads.getOrElse(""),
          "ParallelGCThreads should be equal to executor cores. " +
            "Otherwise, it will be calculated based on physical cores, " +
            "which is too large, causing many context switches ",
          getParallelGCThreadsOption(executorCore),
          Severity.Critical
        ))
      } else {
        Seq.empty
      }
    }

    def getParallelGCThreadsOption(threads: Int): String = {
      s"$PARALLEL_GC_THREADS$threads"
    }
  }

  object ExecutorMemoryOverheadEvaluator extends ConfigEvaluator {
    override def evaluate(appConf: Map[String, String]): Seq[AnalysisRecord] = {
      val offheapEnabled = getProperty(appConf, SPARK_OFF_HEAP_ENABLED)
        .getOrElse("false")
        .toBoolean
      val offHeapSize = Utils.byteStringAsBytes(getProperty(appConf, SPARK_OFF_HEAP_SIZE)
        .getOrElse("0"))

      val memory = getProperty(appConf, SPARK_EXECUTOR_MEMORY)
        .getOrElse(SPARK_EXECUTOR_MEMORY_DEFAULT)
      val memoryOverhead = getProperty(appConf, SPARK_EXECUTOR_MEMORY_OVERHEAD)
        .getOrElse(calculateOverhead(memory) + "b")

      val targetOverhead = offHeapSize + calculateOverhead(memory)
      if(offheapEnabled && (targetOverhead > Utils.byteStringAsBytes(memoryOverhead))) {
        Seq(AnalysisRecord(
          SPARK_EXECUTOR_MEMORY_OVERHEAD,
          Utils.byteStringAsMb(memoryOverhead) + "m",
          "When executor memory off heap is enabled, " +
            "memory overhead value should add off heap size up.",
          Utils.byteStringAsMb(targetOverhead + "b") + "m"))
      } else {
        Seq.empty
      }
    }

    def calculateOverhead(memory: String): Long = {
      Math.max(SPARK_MEMORY_OVERHEAD_MIN_DEFAULT, Utils.byteStringAsBytes(memory) / 10)
    }
  }

  override def analysis(data: SparkApplicationData): Seq[AnalysisResult] = {

    Seq(AnalysisResult(evaluators
      .flatMap(_.evaluate(data.appConf))
      .sortBy(r => r.severity.getValue)(Ordering.Int.reverse)))
  }

  override val name = "Config Insights"

}

trait ConfigEvaluator {
  def evaluate(appConf: Map[String, String]): Seq[AnalysisRecord]
  protected def getProperty(appConf: Map[String, String], key: String): Option[String] =
    appConf.get(key)
}
