package org.apache.spark.status.insight.heuristics

import org.apache.spark.status.insight.SparkApplicationData

trait SparkEvaluator {
  def evaluate(sparkAppData: SparkApplicationData): Seq[AnalysisResult]
  protected def getProperty(sparkAppData: SparkApplicationData, key: String): Option[String] =
    sparkAppData.appConf.get(key)
}
