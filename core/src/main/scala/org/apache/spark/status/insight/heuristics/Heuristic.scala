package org.apache.spark.status.insight.heuristics

import org.apache.spark.status.insight.SparkApplicationData

trait Heuristic {
  def apply(data: SparkApplicationData): HeuristicResult = {
    HeuristicResult(name, evaluators.flatMap(e => e.evaluate(data)))
  }

  val evaluators: Seq[SparkEvaluator] = Seq.empty

  val name: String = this.getClass.getSimpleName
}
