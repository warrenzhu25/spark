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

import org.apache.spark.SparkFunSuite

class ShuffleFetchWaitStatsAggregatorSuite extends SparkFunSuite {

  private def makeStat(execId: String, total: Long, count: Int,
      min: Long, p25: Long, p50: Long, p75: Long, max: Long): ShuffleFetchWaitStat = {
    val agg = ShuffleFetchWaitAggregate(execId, total, count)
    val dist = ShuffleFetchWaitDistribution(
      execId, ExecutorShuffleFetchWaitStats.STANDARD_QUANTILES,
      IndexedSeq(min, p25, p50, p75, max))
    ShuffleFetchWaitStat(agg, dist)
  }

  test("sorts by total wait and caps to top K") {
    val aggregator = new ShuffleFetchWaitStatsAggregator
    aggregator.update(Seq(
      makeStat("exec-1", 100L, 2, 0L, 1L, 2L, 3L, 5L),
      makeStat("exec-2", 50L, 3, 0L, 2L, 3L, 4L, 6L),
      makeStat("exec-3", 200L, 1, 5L, 10L, 15L, 20L, 30L)))

    val top = aggregator.topStats(2)
    assert(top.map(_.remoteExecutorId) === Seq("exec-3", "exec-1"))
    assert(top.head.aggregate.totalWaitMs === 200L)
  }

  test("merges distributions across updates") {
    val aggregator = new ShuffleFetchWaitStatsAggregator
    aggregator.update(Seq(makeStat("exec-1", 100L, 4, 0L, 1L, 2L, 3L, 5L)))
    aggregator.update(Seq(makeStat("exec-1", 50L, 6, 0L, 2L, 3L, 5L, 8L)))

    val merged = aggregator.topStats(1).head
    assert(merged.aggregate.totalWaitMs === 150L)
    assert(merged.aggregate.count === 10)
    assert(merged.distribution.values.last === 8L)  // max
    // Weighted bucket reconstruction should land in mid-to-high latency buckets
    val values = merged.distribution.values
    assert(values(2) >= 2L)  // median should be >= 2
    assert(values(3) >= 3L)  // p75 should be >= 3
  }
}
