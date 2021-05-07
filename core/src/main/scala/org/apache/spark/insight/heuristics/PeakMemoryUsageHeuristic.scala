/*
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

package org.apache.spark.insight.heuristics

import scala.xml.Node

import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.status.api.v1.ExecutorPeakMetricsDistributions
import org.apache.spark.insight.SparkApplicationData
import org.apache.spark.insight.heuristics.ConfigurationHeuristicsConstants._
import org.apache.spark.util.Utils._

/**
 * A heuristic based on executor peak memory metrics
 */
object PeakMemoryUsageHeuristic extends Heuristic {

  override def analysis(data: SparkApplicationData): Seq[AnalysisResult] = {
    data.executorMetricsDistributions.flatMap(m => analysis(data.appConf, m)).toSeq
  }

  def analysis(conf : Map[String, String],
               executorMetrics: ExecutorPeakMetricsDistributions): Option[AnalysisResult] = {
    val results =
      ExecutorMetricType.metricToOffset
        .filter(m => m._1.contains("Memory"))
        .filter(m => !m._1.contains("Process"))
        .map { case (metric, _) =>
          val usage = executorMetrics
            .getMetricDistribution(metric)
            .map(_.toLong)
            .map(bytesToString)

          new UsageRecord(
            name = metric,
            value = bytesToString(Resources.get(metric).calculate(conf)),
            usage = usage)
        }
        .toSeq

    if (results.nonEmpty) {
      Some(AnalysisResult(results))
    } else {
      None
    }
  }

  override def name: String = "Memory Usage Insights"
}

class UsageRecord(name: String,
                  value: String,
                  usage: IndexedSeq[String],
                  description: String = "",
                  suggested: String = "",
                  severity: Severity = Severity.Normal)
  extends AnalysisRecord(name, value, description, suggested, severity) {

  override val header: Seq[String] =
    Seq("Name", "Value", "Usage (Median / Max)", "Suggested", "Description", "Severity")

  override def toHTML(basePathUri: String): Seq[Node] = {
    <tr>
      <td>{name.split("(?=[A-Z])")}</td>
      <td>{value}</td>
      <td>{usage(0)} / {usage(1)}</td>
      <td>{description}</td>
      <td>{suggested}</td>
      <td>
        <span data-toggle="tooltip" title={severity.getTooltip}>
          {severity}
        </span>
      </td>
    </tr>
  }
}

sealed trait Resources {
  def calculate(conf: Map[String, String]): Long
}

object Resources {
  case object JVMHeapMemory extends Resources {
    override def calculate(conf: Map[String, String]): Long =
      byteStringAsBytes(conf.getOrElse(SPARK_EXECUTOR_MEMORY, SPARK_EXECUTOR_MEMORY_DEFAULT))
  }

  case object OnHeapUnifiedMemory extends Resources {
    override def calculate(conf: Map[String, String]): Long = {
      val heapMemory = JVMHeapMemory.calculate(conf)
      val memoryFraction = conf
        .getOrElse(SPARK_MEMORY_FRACTION, SPARK_MEMORY_FRACTION_DEFAULT)
        .toDouble
      ((heapMemory - SPARK_RESERVED_MEMORY) * memoryFraction).toLong
    }
  }

  case object OnHeapStorageMemory extends Resources {
    override def calculate(conf: Map[String, String]): Long = {
      val heapUnifiedMemory = OnHeapUnifiedMemory.calculate(conf)
      val storageFraction = conf
        .getOrElse(SPARK_MEMORY_STORAGE_FRACTION, SPARK_MEMORY_STORAGE_FRACTION_DEFAULT)
        .toDouble
      (heapUnifiedMemory * storageFraction).toLong
    }
  }

  case object OnHeapExecutionMemory extends Resources {
    override def calculate(conf: Map[String, String]): Long = {
      OnHeapUnifiedMemory.calculate(conf) - OnHeapStorageMemory.calculate(conf)
    }
  }

  case object JVMOffHeapMemory extends Resources {
    override def calculate(conf: Map[String, String]): Long = {
      val heapMemory = JVMHeapMemory.calculate(conf)
      Math.max(SPARK_MEMORY_OVERHEAD_MIN_DEFAULT,
        (heapMemory * SPARK_MEMORY_OVERHEAD_PCT_DEFAULT).toLong)
    }
  }

  case object OffHeapUnifiedMemory extends Resources {
    override def calculate(conf: Map[String, String]): Long = {
      val heapMemory = offHeapSize(conf)
      val memoryFraction = conf
        .getOrElse(SPARK_MEMORY_FRACTION, SPARK_MEMORY_FRACTION_DEFAULT)
        .toDouble
      (heapMemory * memoryFraction).toLong
    }
  }

  case object OffHeapStorageMemory extends Resources {
    override def calculate(conf: Map[String, String]): Long = {
      val heapUnifiedMemory = OffHeapUnifiedMemory.calculate(conf)
      val storageFraction = conf
        .getOrElse(SPARK_MEMORY_STORAGE_FRACTION, SPARK_MEMORY_STORAGE_FRACTION_DEFAULT)
        .toDouble
      (heapUnifiedMemory * storageFraction).toLong
    }
  }

  case object OffHeapExecutionMemory extends Resources {
    override def calculate(conf: Map[String, String]): Long = {
      OffHeapUnifiedMemory.calculate(conf) - OffHeapStorageMemory.calculate(conf)
    }
  }

  case object UnDefined extends Resources {
    override def calculate(conf: Map[String, String]): Long = 0L
  }

  def get(metricType: String): Resources = {
    values.getOrElse(metricType, UnDefined)
  }

  def offHeapSize(conf: Map[String, String]): Long = {
    val offheapEnabled = conf.getOrElse(SPARK_OFF_HEAP_ENABLED, "false")
      .toBoolean
    val offHeapSize = byteStringAsBytes(conf.getOrElse(SPARK_OFF_HEAP_SIZE, "0"))
    if (offheapEnabled) offHeapSize else 0L
  }

  private val values = Map(
    "JVMHeapMemory" -> JVMHeapMemory,
    "JVMOffHeapMemory" -> JVMOffHeapMemory,
    "OnHeapExecutionMemory" -> OnHeapExecutionMemory,
    "OffHeapExecutionMemory" -> OffHeapExecutionMemory,
    "OnHeapStorageMemory" -> OnHeapStorageMemory,
    "OffHeapStorageMemory" -> OffHeapStorageMemory,
    "OnHeapUnifiedMemory" -> OnHeapUnifiedMemory,
    "OffHeapUnifiedMemory" -> OffHeapUnifiedMemory)
}
