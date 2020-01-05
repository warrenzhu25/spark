package org.apache.spark.status.insight.heuristics

import org.apache.spark.status.insight.SparkAppData

trait Heuristic {
  def apply(data: SparkAppData): HeuristicResult = {
    HeuristicResult(name, evaluators.flatMap(e => e.evaluate(data)))
  }

  val evaluators: Seq[Evaluator]

  val name: String = this.getClass.getSimpleName
}
