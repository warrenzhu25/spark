package org.apache.spark.status.insight.heuristics

import org.apache.spark.status.insight.SparkAppData

trait Evaluator {
  def evaluate(sparkAppData: SparkAppData): Seq[HeuristicRecord]
  protected def getProperty(sparkAppData: SparkAppData, key: String): Option[String] =
    sparkAppData.appConf.get(key)
}
