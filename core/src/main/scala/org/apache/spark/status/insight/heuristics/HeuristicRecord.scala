package org.apache.spark.status.insight.heuristics

case class HeuristicRecord(name: String,
                           value: String = "Default",
                           suggestedValue: String = "",
                           description: String = "")

case class HeuristicResult(name: String,
                           heuristicRecords: Seq[HeuristicRecord])
