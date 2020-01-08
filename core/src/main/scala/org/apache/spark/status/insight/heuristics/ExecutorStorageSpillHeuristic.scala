/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.spark.status.insight.heuristics

import org.apache.spark.status.api.v1.ExecutorSummary
import org.apache.spark.status.insight.SparkApplicationData
import org.apache.spark.status.insight.analysis.{MemoryFormatUtils, Severity}
import org.apache.spark.status.insight.util.Utils

import scala.collection.JavaConverters


/**
  * A heuristic based on memory spilled.
  *
  */
class ExecutorStorageSpillHeuristic()
  extends Heuristic {

  import ExecutorStorageSpillHeuristic._
  import JavaConverters._

  val spillFractionOfExecutorsThreshold: Double = DEFAULT_SPILL_FRACTION_OF_EXECUTORS_THRESHOLD

  val spillMaxMemoryThreshold: Double = DEFAULT_SPILL_MAX_MEMORY_THRESHOLD

  val sparkExecutorCoresThreshold : Int = DEFAULT_SPARK_EXECUTOR_CORES_THRESHOLD

  val sparkExecutorMemoryThreshold : String = DEFAULT_SPARK_EXECUTOR_MEMORY_THRESHOLD

  override def apply(data: SparkApplicationData): HeuristicResult = {
    val evaluator = new Evaluator(this, data)
    var resultDetails = Seq(
      new HeuristicResultDetails("Total memory spilled", MemoryFormatUtils.bytesToString(evaluator.totalMemorySpilled)),
      new HeuristicResultDetails("Max memory spilled", MemoryFormatUtils.bytesToString(evaluator.maxMemorySpilled)),
      new HeuristicResultDetails("Mean memory spilled", MemoryFormatUtils.bytesToString(evaluator.meanMemorySpilled)),
      new HeuristicResultDetails("Fraction of executors having non zero bytes spilled", evaluator.fractionOfExecutorsHavingBytesSpilled.toString)
    )

    if(evaluator.severity != Severity.NONE){
      resultDetails :+ new HeuristicResultDetails("Note", "Your execution memory is being spilled. Kindly look into it.")
      if(evaluator.sparkExecutorCores >= sparkExecutorCoresThreshold && evaluator.sparkExecutorMemory >= MemoryFormatUtils.stringToBytes(sparkExecutorMemoryThreshold)) {
        resultDetails :+ new HeuristicResultDetails("Recommendation", "You can try decreasing the number of cores to reduce the number of concurrently running tasks.")
      } else if (evaluator.sparkExecutorMemory <= MemoryFormatUtils.stringToBytes(sparkExecutorMemoryThreshold)) {
        resultDetails :+ new HeuristicResultDetails("Recommendation", "You can try increasing the executor memory to reduce spill.")
      }
    }

    HeuristicResult(
      name,
      resultDetails
    )
  }
}

object ExecutorStorageSpillHeuristic {
  val SPARK_EXECUTOR_MEMORY = "spark.executor.memory"
  val SPARK_EXECUTOR_CORES = "spark.executor.cores"
  val SPILL_FRACTION_OF_EXECUTORS_THRESHOLD_KEY = "spill_fraction_of_executors_threshold"
  val SPILL_MAX_MEMORY_THRESHOLD_KEY = "spill_max_memory_threshold"
  val SPARK_EXECUTOR_CORES_THRESHOLD_KEY = "spark_executor_cores_threshold"
  val SPARK_EXECUTOR_MEMORY_THRESHOLD_KEY = "spark_executor_memory_threshold"
  val DEFAULT_SPILL_FRACTION_OF_EXECUTORS_THRESHOLD : Double = 0.2
  val DEFAULT_SPILL_MAX_MEMORY_THRESHOLD : Double = 0.05
  val DEFAULT_SPARK_EXECUTOR_CORES_THRESHOLD : Int = 4
  val DEFAULT_SPARK_EXECUTOR_MEMORY_THRESHOLD : String  ="10GB"

  class Evaluator(executorStorageSpillHeuristic: ExecutorStorageSpillHeuristic, data: SparkApplicationData) {
    lazy val executorAndDriverSummaries: Seq[ExecutorSummary] = data.executorSummaries
    if (executorAndDriverSummaries == null) {
      throw new Exception("Executors Summary is null.")
    }
    lazy val executorSummaries: Seq[ExecutorSummary] = executorAndDriverSummaries.filterNot(_.id.equals("driver"))
    if (executorSummaries.isEmpty) {
      throw new Exception("No executor information available.")
    }
    lazy val appConfigurationProperties: Map[String, String] =
      data.appConf
    val maxTasks: Int = executorSummaries.head.maxTasks
    val maxMemorySpilled: Long = executorSummaries.map(_.totalMemoryBytesSpilled).max
    val meanMemorySpilled = executorSummaries.map(_.totalMemoryBytesSpilled).sum / executorSummaries.size
    lazy val totalTasks = Integer.max(executorSummaries.map(_.totalTasks).sum, 1)
    val totalMemorySpilledPerTask = totalMemorySpilled/totalTasks
    lazy val totalMemorySpilled = executorSummaries.map(_.totalMemoryBytesSpilled).sum
    val fractionOfExecutorsHavingBytesSpilled: Double = executorSummaries.count(_.totalMemoryBytesSpilled > 0).toDouble / executorSummaries.size.toDouble
    val severity: Severity = {
      if (fractionOfExecutorsHavingBytesSpilled != 0) {
        if (fractionOfExecutorsHavingBytesSpilled < executorStorageSpillHeuristic.spillFractionOfExecutorsThreshold
          && totalMemorySpilledPerTask < executorStorageSpillHeuristic.spillMaxMemoryThreshold * (sparkExecutorMemory/maxTasks)) {
          Severity.LOW
        } else if (fractionOfExecutorsHavingBytesSpilled < executorStorageSpillHeuristic.spillFractionOfExecutorsThreshold
          && totalMemorySpilledPerTask < executorStorageSpillHeuristic.spillMaxMemoryThreshold * (sparkExecutorMemory/maxTasks)) {
          Severity.MODERATE
        } else if (fractionOfExecutorsHavingBytesSpilled >= executorStorageSpillHeuristic.spillFractionOfExecutorsThreshold
          && totalMemorySpilledPerTask < executorStorageSpillHeuristic.spillMaxMemoryThreshold * (sparkExecutorMemory/maxTasks)) {
          Severity.SEVERE
        } else if (fractionOfExecutorsHavingBytesSpilled >= executorStorageSpillHeuristic.spillFractionOfExecutorsThreshold
          && totalMemorySpilledPerTask >= executorStorageSpillHeuristic.spillMaxMemoryThreshold * (sparkExecutorMemory/maxTasks)) {
          Severity.CRITICAL
        } else Severity.NONE
      }
      else Severity.NONE
    }

    val executorCount = executorSummaries.size
    lazy val score = Utils.getHeuristicScore(severity, executorCount)


    lazy val sparkExecutorMemory: Long = (appConfigurationProperties.get(SPARK_EXECUTOR_MEMORY).map(MemoryFormatUtils.stringToBytes)).getOrElse(0)
    lazy val sparkExecutorCores: Int = (appConfigurationProperties.get(SPARK_EXECUTOR_CORES).map(_.toInt)).getOrElse(0)
  }
}

