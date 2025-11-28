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

import scala.collection.mutable

/**
 * Aggregates [[ShuffleFetchWaitStat]] entries coming from multiple tasks or executors and produces
 * top-K summaries keyed by remote executor.
 *
 * The quantiles are reconstructed from incoming summaries using weighted quantile buckets. This
 * is approximate, but keeps the payload small while still surfacing distribution shape.
 * Uses Spark's standard quantiles: min, 25th, median, 75th, max.
 */
private[spark] class ShuffleFetchWaitStatsAggregator {
  private val lock = new Object
  private val byExecutor = mutable.HashMap.empty[String, MutableWaitSummary]

  def update(stats: Seq[ShuffleFetchWaitStat]): Unit = lock.synchronized {
    stats.foreach { stat =>
      val summary = byExecutor.getOrElseUpdate(stat.remoteExecutorId, new MutableWaitSummary)
      summary.merge(stat)
    }
  }

  def topStats(k: Int): Seq[ShuffleFetchWaitStat] = lock.synchronized {
    byExecutor.toSeq
      .sortBy { case (_, summary) => -summary.totalWait }
      .take(k)
      .map { case (execId, summary) => summary.toStat(execId) }
  }

  def isEmpty: Boolean = lock.synchronized { byExecutor.isEmpty }
}

private class MutableWaitSummary {
  private var totalCount: Long = 0L
  private var totalWaitMs: Long = 0L
  private var minWaitMs: Long = Long.MaxValue
  private var maxWaitMs: Long = 0L
  private val weightedBuckets = mutable.TreeMap.empty[Long, Long]

  def merge(stat: ShuffleFetchWaitStat): Unit = {
    if (stat.aggregate.count > 0) {
      totalCount += stat.aggregate.count
      totalWaitMs += stat.aggregate.totalWaitMs
      addWeightedSamples(stat)
    }
  }

  private def addWeightedSamples(stat: ShuffleFetchWaitStat): Unit = {
    val count = math.max(1L, stat.aggregate.count.toLong)
    val weights = quantileWeights(count)
    val values = stat.distribution.values

    // Update min and max from incoming distribution
    if (values.nonEmpty) {
      minWaitMs = math.min(minWaitMs, values.head)
      maxWaitMs = math.max(maxWaitMs, values.last)
    }

    // Add weighted samples for all quantile points
    values.zip(weights).foreach { case (value, weight) =>
      val existing = weightedBuckets.getOrElse(value, 0L)
      weightedBuckets.update(value, existing + weight)
    }
  }

  private def quantileWeights(count: Long): Seq[Long] = {
    // Standard quantiles: [0.0, 0.25, 0.5, 0.75, 1.0]
    // Distribute count proportionally: min(1%), p25(24%), median(50%), p75(24%), max(1%)
    val wMin = math.max(1L, count / 100)
    val wP25 = math.max(1L, (count * 24) / 100)
    val wMedian = math.max(1L, (count * 50) / 100)
    val wP75 = math.max(1L, (count * 24) / 100)
    val assigned = wMin + wP25 + wMedian + wP75
    val wMax = math.max(1L, count - assigned)
    Seq(wMin, wP25, wMedian, wP75, wMax)
  }

  private def quantile(fraction: Double): Long = {
    if (weightedBuckets.isEmpty) {
      0L
    } else {
      val totalWeight = weightedBuckets.values.sum
      val target = math.max(1L, math.ceil(totalWeight * fraction).toLong)
      var seen = 0L
      var valueAtQuantile = weightedBuckets.lastKey
      val iter = weightedBuckets.iterator
      while (iter.hasNext && seen < target) {
        val (value, weight) = iter.next()
        seen += weight
        valueAtQuantile = value
      }
      valueAtQuantile
    }
  }

  def toStat(execId: String): ShuffleFetchWaitStat = {
    val aggregate = ShuffleFetchWaitAggregate(
      execId,
      totalWaitMs,
      math.min(totalCount, Int.MaxValue).toInt)

    val distribution = ShuffleFetchWaitDistribution(
      execId,
      ExecutorShuffleFetchWaitStats.STANDARD_QUANTILES,
      IndexedSeq(
        if (minWaitMs == Long.MaxValue) 0L else minWaitMs,  // min
        quantile(0.25),                                       // p25
        quantile(0.5),                                        // median
        quantile(0.75),                                       // p75
        maxWaitMs))                                           // max

    ShuffleFetchWaitStat(aggregate, distribution)
  }

  def totalWait: Long = totalWaitMs
}
