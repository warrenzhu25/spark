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

package org.apache.spark.shuffle

/**
 * Aggregate statistics for shuffle fetch wait time from a remote executor.
 * Separates aggregate metrics (totals, counts) from distribution data following
 * Spark's standard metrics pattern.
 *
 * @param remoteExecutorId executor that served the shuffle data
 * @param totalWaitMs total wait time (ms) observed while fetching from that executor
 * @param count number of fetches contributing to this summary
 */
private[spark] case class ShuffleFetchWaitAggregate(
    remoteExecutorId: String,
    totalWaitMs: Long,
    count: Int) extends Serializable {
  require(count >= 0, s"count must be non-negative but was $count")
  require(totalWaitMs >= 0, s"totalWaitMs must be non-negative but was $totalWaitMs")
}

/**
 * Distribution of shuffle fetch wait times from a remote executor.
 * Follows Spark's standard distribution pattern with quantiles and corresponding values,
 * consistent with TaskMetricDistributions.
 *
 * @param remoteExecutorId executor that served the shuffle data
 * @param quantiles quantile points (e.g., [0, 0.25, 0.5, 0.75, 1.0])
 * @param values wait time values (ms) at each quantile point
 */
private[spark] case class ShuffleFetchWaitDistribution(
    remoteExecutorId: String,
    quantiles: IndexedSeq[Double],
    values: IndexedSeq[Long]) extends Serializable {
  require(quantiles.length == values.length,
    s"quantiles and values must have same length: ${quantiles.length} != ${values.length}")
  require(quantiles.nonEmpty, "quantiles cannot be empty")
  require(values.forall(_ >= 0), s"all values must be non-negative but found: $values")
}

/**
 * Complete shuffle fetch wait statistics combining aggregates and distribution.
 * This is the primary API for per-executor wait time analysis, following Spark's
 * standard metric distribution pattern.
 *
 * @param aggregate aggregate statistics (totals, counts)
 * @param distribution distribution of wait times across quantiles
 */
private[spark] case class ShuffleFetchWaitStat(
    aggregate: ShuffleFetchWaitAggregate,
    distribution: ShuffleFetchWaitDistribution) extends Serializable {
  require(aggregate.remoteExecutorId == distribution.remoteExecutorId,
    "aggregate and distribution must be for same executor")

  def remoteExecutorId: String = aggregate.remoteExecutorId
}

/**
 * Container for the top-K remote executors contributing to shuffle fetch wait time.
 * Uses the standard distribution format consistent with other Spark metrics.
 */
private[spark] case class ExecutorShuffleFetchWaitStats(
    stats: Seq[ShuffleFetchWaitStat]) extends Serializable

private[spark] object ExecutorShuffleFetchWaitStats {
  val EMPTY: ExecutorShuffleFetchWaitStats = ExecutorShuffleFetchWaitStats(Seq.empty)

  /**
   * Standard quantile points for shuffle fetch wait distributions.
   * Matches Spark's TaskMetricDistributions standard: min, 25th, median, 75th, max.
   */
  val STANDARD_QUANTILES: IndexedSeq[Double] = IndexedSeq(0.0, 0.25, 0.5, 0.75, 1.0)
}
