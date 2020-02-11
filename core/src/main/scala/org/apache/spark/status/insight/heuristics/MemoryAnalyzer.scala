package org.apache.spark.status.insight.heuristics

import org.apache.spark.metrics._
import org.apache.spark.status.insight.SparkApplicationData
import org.apache.spark.status.insight.analysis.MemoryFormatUtils
import org.apache.spark.status.insight.heuristics.MemoryEvaluator._

private [heuristics] trait MemoryAnalyzer {
  def analysis(data: SparkApplicationData): HeuristicResult
}

abstract class PeakMemoryAnalyzer(val executorMetricType: ExecutorMetricType)
  extends MemoryAnalyzer {

  def getCurrentConfigs(conf: Map[String, String]) : Seq[(String, String)] = {
    defaultConfigs.map({case (k, v) => (k, conf.getOrElse(k, s"(Default): $v"))}).toSeq
  }

  def analysis(data: SparkApplicationData): HeuristicResult = {
    val memoryEvaluator = new MemoryEvaluator(data)
    val (mean, max) = memoryEvaluator.getMetricStat(executorMetricType)

    val results = Seq(
      (s"$executorMetricType Allocated", getAllocated(memoryEvaluator)),
      (s"Mean/Max Peak $executorMetricType" , s"$mean/$max")
    )

    results ++ getCurrentConfigs(memoryEvaluator.appConf)
    results ++ getSuggested(memoryEvaluator)

    HeuristicResult(
      s"$executorMetricType Heuristic",
      results.map({ case (a, b) => SimpleResult(a, b)})
    )
  }

  def getSuggested(evaluator: MemoryEvaluator): Seq[(String, String)]

  def getAllocated(memoryEvaluator: MemoryEvaluator): String

  val defaultConfigs: Map[String, String]

  val toSize = MemoryFormatUtils.bytesToString _
}

object PeakUnifiedHeapMemoryAnalyzer extends PeakMemoryAnalyzer(OnHeapUnifiedMemory) {

  override val defaultConfigs: Map[String, String] = Map(
    SPARK_EXECUTOR_MEMORY -> SPARK_EXECUTOR_MEMORY_DEFAULT,
    SPARK_MEMORY_FRACTION -> SPARK_MEMORY_FRACTION_DEFAULT)

  override def getAllocated(memoryEvaluator: MemoryEvaluator): String = {
    toSize(memoryEvaluator.unifiedMemory)
  }

  override def getSuggested(evaluator: MemoryEvaluator): Seq[(String, String)] = {
    val executorMemory = evaluator.executorMemory
    val (max, _) = evaluator.getMetricStat(executorMetricType)
    val suggestedMemoryFraction = max.toDouble * 10 / executorMemory / 10 + 0.1
    Seq((SPARK_MEMORY_FRACTION, suggestedMemoryFraction.toString))
  }
}

object PeakHeapMemoryAnalyzer extends PeakMemoryAnalyzer(JVMHeapMemory) {

  override val defaultConfigs: Map[String, String] = Map(SPARK_EXECUTOR_MEMORY -> SPARK_EXECUTOR_MEMORY_DEFAULT)

  override def getAllocated(memoryEvaluator: MemoryEvaluator): String = {
    toSize(memoryEvaluator.executorMemory)
  }

  override def getSuggested(evaluator: MemoryEvaluator): Seq[(String, String)] = {
    val (max, _) = evaluator.getMetricStat(executorMetricType)
    Seq((SPARK_MEMORY_FRACTION, toSize(max)))
  }
}

object PeakOffHeapMemoryAnalyzer extends PeakMemoryAnalyzer(JVMOffHeapMemory) {

  override val defaultConfigs: Map[String, String] = Map(SPARK_EXECUTOR_MEMORY_OVERHEAD -> SPARK_EXECUTOR_MEMORY_DEFAULT)

  override def getAllocated(memoryEvaluator: MemoryEvaluator): String = {
    toSize(memoryEvaluator.executorMemoryOverhead)
  }

  override def getSuggested(evaluator: MemoryEvaluator): Seq[(String, String)] = {
    val (max, _) = evaluator.getMetricStat(executorMetricType)
    Seq((SPARK_EXECUTOR_MEMORY_OVERHEAD, toSize(max)))
  }
}

object PeakExecutionHeapMemoryAnalyzer extends PeakMemoryAnalyzer(OnHeapExecutionMemory) {

  override val defaultConfigs: Map[String, String] = Map(
    SPARK_EXECUTOR_MEMORY -> SPARK_EXECUTOR_MEMORY_DEFAULT,
    SPARK_MEMORY_FRACTION -> SPARK_MEMORY_FRACTION_DEFAULT,
    SPARK_MEMORY_STORAGE_FRACTION -> SPARK_MEMORY_STORAGE_FRACTION_DEFAULT)

  override def getAllocated(memoryEvaluator: MemoryEvaluator): String = {
    toSize(memoryEvaluator.executorExecutionMemory)
  }

  override def getSuggested(evaluator: MemoryEvaluator): Seq[(String, String)] = {
    val (max, _) = evaluator.getMetricStat(executorMetricType)
    val suggestedStorageFraction = max.toDouble * 10 / evaluator.unifiedMemory / 10 + 0.1
    Seq((SPARK_MEMORY_STORAGE_FRACTION, (1 - suggestedStorageFraction).toString))
  }
}

object PeakStorageHeapMemoryAnalyzer extends PeakMemoryAnalyzer(OnHeapStorageMemory) {

  override val defaultConfigs: Map[String, String] = Map(
    SPARK_EXECUTOR_MEMORY -> SPARK_EXECUTOR_MEMORY_DEFAULT,
    SPARK_MEMORY_FRACTION -> SPARK_MEMORY_FRACTION_DEFAULT,
    SPARK_MEMORY_STORAGE_FRACTION -> SPARK_MEMORY_STORAGE_FRACTION_DEFAULT)

  override def getAllocated(memoryEvaluator: MemoryEvaluator): String = {
    toSize(memoryEvaluator.executorStorageMemory)
  }

  override def getSuggested(evaluator: MemoryEvaluator): Seq[(String, String)] = {
    val (max, _) = evaluator.getMetricStat(executorMetricType)
    val suggestedStorageFraction = max.toDouble * 10 / evaluator.unifiedMemory / 10 + 0.1
    Seq((SPARK_MEMORY_STORAGE_FRACTION, suggestedStorageFraction.toString))
  }
}
