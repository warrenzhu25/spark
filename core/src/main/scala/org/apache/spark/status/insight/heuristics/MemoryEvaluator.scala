package org.apache.spark.status.insight.heuristics

import org.apache.spark.metrics.ExecutorMetricType
import org.apache.spark.status.api.v1.ExecutorSummary
import org.apache.spark.status.insight.SparkApplicationData
import org.apache.spark.status.insight.analysis.MemoryFormatUtils._
import org.apache.spark.status.insight.heuristics.MemoryEvaluator._

class MemoryEvaluator (data: SparkApplicationData) {
  lazy val appConf: Map[String, String] = data.appConf

  lazy val executorMemory: Long = stringToBytes(appConf.getOrElse(SPARK_EXECUTOR_MEMORY, SPARK_EXECUTOR_MEMORY_DEFAULT))

  lazy val driverMemory: Long = stringToBytes(appConf.getOrElse(SPARK_DRIVER_MEMORY, SPARK_DRIVER_MEMORY_DEFAULT))

  lazy val executorMemoryOverhead: Long = {
    val defaultMemoryOverhead = Math.max(executorMemory / 10, stringToBytes("384m"))
    stringToBytes(appConf.getOrElse(SPARK_EXECUTOR_MEMORY_OVERHEAD, defaultMemoryOverhead.toString))
  }

  lazy val executorExecutionMemory: Long = {
    (unifiedMemory * (1 - storageFraction)).toLong
  }

  lazy val executorStorageMemory: Long = {
    (unifiedMemory * storageFraction).toLong
  }

  lazy val unifiedMemory: Long = (executorMemory * memoryFraction).toLong

  lazy val memoryFraction: Double = appConf.getOrElse(SPARK_MEMORY_FRACTION, SPARK_MEMORY_FRACTION_DEFAULT).toDouble

  lazy val storageFraction: Double = appConf.getOrElse(SPARK_MEMORY_STORAGE_FRACTION, SPARK_MEMORY_STORAGE_FRACTION).toDouble

  lazy val executorCores: Int = appConf.getOrElse(SPARK_EXECUTOR_CORES, SPARK_EXECUTOR_CORES_DEFAULT).toInt

  lazy val driverCores: Int = appConf
    .getOrElse(SPARK_DRIVER_CORES, SPARK_DRIVER_CORES_DEFAULT).toInt

  lazy val sparkSqlShufflePartitions: Int = appConf.getOrElse(SPARK_SQL_SHUFFLE_PARTITIONS, SPARK_SQL_SHUFFLE_PARTITIONS_DEFAULT).toInt

  lazy val executorInstances: Option[Int] = appConf
    .get(SPARK_EXECUTOR_INSTANCES).map(_.toInt)

  lazy val driverMemoryOverhead: Option[Long] = appConf
    .get(SPARK_DRIVER_MEMORY_OVERHEAD).map(stringToBytes)
  
  def getMetricStat(executorMetricType: ExecutorMetricType): (Long, Long) = {
    val executorMetrics = data.executorSummaries.map(e => getMetricValue(e, executorMetricType))
    (executorMetrics.sum / data.executorSummaries.size, executorMetrics.max)
  }

  def getMetricValue(executorSummary: ExecutorSummary, executorMetricType: ExecutorMetricType): Long = {
    executorSummary.peakMemoryMetrics.map(_.getMetricValue(executorMetricType)).getOrElse(0)
  }
}

object MemoryEvaluator {

  // Spark configuration parameters
  val SPARK_EXECUTOR_MEMORY = "spark.executor.memory"
  val SPARK_DRIVER_MEMORY = "spark.driver.memory"
  val SPARK_EXECUTOR_MEMORY_OVERHEAD = "spark.executor.memoryOverhead"
  val SPARK_DRIVER_MEMORY_OVERHEAD = "spark.driver.memoryOverhead"
  val SPARK_EXECUTOR_CORES = "spark.executor.cores"
  val SPARK_DRIVER_CORES = "spark.driver.cores"
  val SPARK_EXECUTOR_INSTANCES = "spark.executor.instances"
  val SPARK_SQL_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions"
  val SPARK_MEMORY_FRACTION = "spark.memory.fraction"
  val SPARK_MEMORY_STORAGE_FRACTION = "spark.memory.storageFraction"

  // Spark default configuration values
  val SPARK_EXECUTOR_MEMORY_DEFAULT = "1g"
  val SPARK_DRIVER_MEMORY_DEFAULT = "1g"
  val SPARK_EXECUTOR_CORES_DEFAULT = "1"
  val SPARK_DRIVER_CORES_DEFAULT = "1"
  val SPARK_SQL_SHUFFLE_PARTITIONS_DEFAULT = "200"
  val SPARK_MEMORY_FRACTION_DEFAULT = "0.6"
  val SPARK_MEMORY_STORAGE_FRACTION_DEFAULT = "0.5"

}
