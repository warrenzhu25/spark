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

package org.apache.spark.scheduler

import org.apache.spark.SparkFunSuite

class StageMetricsProfileSuite extends SparkFunSuite {

  test("calculate executors for CPU-bound stage") {
    val metrics = StageMetricsProfile(
      avgNumTasks = 200.0,
      minNumTasks = 200,
      maxNumTasks = 200,
      p50NumTasks = 200,
      p95NumTasks = 200,
      avgShuffleReadBytes = 100 * 1024 * 1024, // 100MB
      p95ShuffleReadBytes = 120 * 1024 * 1024,
      avgShuffleWriteBytes = 50 * 1024 * 1024,
      p95ShuffleWriteBytes = 60 * 1024 * 1024,
      avgPeakMemoryPerTask = 100 * 1024 * 1024,
      p95PeakMemoryPerTask = 120 * 1024 * 1024,
      avgTaskDuration = 5000, // 5 seconds - fast
      p95TaskDuration = 7000,
      avgStageDuration = 15000,
      recommendedExecutors = 0,
      recommendedCoresPerExecutor = 4,
      recommendedMemoryPerExecutor = 4 * 1024 * 1024 * 1024L
    )

    // 200 tasks / (4 cores * 2 tasks/core) = 25 executors
    // No duration multiplier (5s < 30s target)
    // No shuffle multiplier (150MB total < 1GB threshold per task)
    val recommended = StageMetricsProfile.calculateRecommendedExecutors(
      metrics,
      coresPerExecutor = 4,
      tasksPerCore = 2,
      targetTaskDurationMs = 30000,
      shuffleThresholdBytes = 1024 * 1024 * 1024,
      maxExecutors = 100
    )

    assert(recommended === 25)
  }

  test("calculate executors for shuffle-intensive stage") {
    val metrics = StageMetricsProfile(
      avgNumTasks = 100.0,
      minNumTasks = 100,
      maxNumTasks = 100,
      p50NumTasks = 100,
      p95NumTasks = 100,
      avgShuffleReadBytes = 100L * 1024 * 1024 * 1024, // 100GB read
      p95ShuffleReadBytes = 120L * 1024 * 1024 * 1024,
      avgShuffleWriteBytes = 100L * 1024 * 1024 * 1024, // 100GB write
      p95ShuffleWriteBytes = 120L * 1024 * 1024 * 1024,
      avgPeakMemoryPerTask = 500 * 1024 * 1024,
      p95PeakMemoryPerTask = 600 * 1024 * 1024,
      avgTaskDuration = 45000, // 45 seconds
      p95TaskDuration = 60000,
      avgStageDuration = 120000,
      recommendedExecutors = 0,
      recommendedCoresPerExecutor = 4,
      recommendedMemoryPerExecutor = 4 * 1024 * 1024 * 1024L
    )

    // Parallelism: 100 / 8 = 13 executors
    // Duration multiplier: min(45000/30000, 3.0) = 1.5
    // Shuffle: 200GB / 100 tasks = 2GB per task
    //   Multiplier: 1 + min((2GB-1GB)/1GB * 0.2, 0.5) = 1.2
    // Total: 13 * 1.5 * 1.2 = 23.4 -> 23 executors
    val recommended = StageMetricsProfile.calculateRecommendedExecutors(
      metrics,
      coresPerExecutor = 4,
      tasksPerCore = 2,
      targetTaskDurationMs = 30000,
      shuffleThresholdBytes = 1024 * 1024 * 1024,
      maxExecutors = 100
    )

    assert(recommended === 23)
  }

  test("calculate executors for long-running tasks") {
    val metrics = StageMetricsProfile(
      avgNumTasks = 50.0,
      minNumTasks = 50,
      maxNumTasks = 50,
      p50NumTasks = 50,
      p95NumTasks = 50,
      avgShuffleReadBytes = 500 * 1024 * 1024, // 500MB
      p95ShuffleReadBytes = 600 * 1024 * 1024,
      avgShuffleWriteBytes = 0,
      p95ShuffleWriteBytes = 0,
      avgPeakMemoryPerTask = 200 * 1024 * 1024,
      p95PeakMemoryPerTask = 250 * 1024 * 1024,
      avgTaskDuration = 120000, // 2 minutes - very slow
      p95TaskDuration = 150000,
      avgStageDuration = 300000,
      recommendedExecutors = 0,
      recommendedCoresPerExecutor = 4,
      recommendedMemoryPerExecutor = 4 * 1024 * 1024 * 1024L
    )

    // Parallelism: 50 / 8 = 7 executors
    // Duration multiplier: min(120000/30000, 3.0) = 3.0 (capped)
    // No shuffle multiplier
    // Total: 7 * 3.0 = 21 executors
    val recommended = StageMetricsProfile.calculateRecommendedExecutors(
      metrics,
      coresPerExecutor = 4,
      tasksPerCore = 2,
      targetTaskDurationMs = 30000,
      shuffleThresholdBytes = 1024 * 1024 * 1024,
      maxExecutors = 100
    )

    assert(recommended === 21)
  }

  test("calculate executors respects max executors limit") {
    val metrics = StageMetricsProfile(
      avgNumTasks = 1000.0,
      minNumTasks = 1000,
      maxNumTasks = 1000,
      p50NumTasks = 1000,
      p95NumTasks = 1000,
      avgShuffleReadBytes = 500L * 1024 * 1024 * 1024, // 500GB
      p95ShuffleReadBytes = 600L * 1024 * 1024 * 1024,
      avgShuffleWriteBytes = 500L * 1024 * 1024 * 1024,
      p95ShuffleWriteBytes = 600L * 1024 * 1024 * 1024,
      avgPeakMemoryPerTask = 1024 * 1024 * 1024,
      p95PeakMemoryPerTask = 1200 * 1024 * 1024,
      avgTaskDuration = 180000, // 3 minutes
      p95TaskDuration = 200000,
      avgStageDuration = 600000,
      recommendedExecutors = 0,
      recommendedCoresPerExecutor = 4,
      recommendedMemoryPerExecutor = 4 * 1024 * 1024 * 1024L
    )

    // Would calculate very high, but should be capped at maxExecutors
    val recommended = StageMetricsProfile.calculateRecommendedExecutors(
      metrics,
      coresPerExecutor = 4,
      tasksPerCore = 2,
      targetTaskDurationMs = 30000,
      shuffleThresholdBytes = 1024 * 1024 * 1024,
      maxExecutors = 50 // Cap at 50
    )

    assert(recommended === 50)
  }

  test("calculate executors with zero tasks returns minimum") {
    val metrics = StageMetricsProfile(
      avgNumTasks = 0.0,
      minNumTasks = 0,
      maxNumTasks = 0,
      p50NumTasks = 0,
      p95NumTasks = 0,
      avgShuffleReadBytes = 0,
      p95ShuffleReadBytes = 0,
      avgShuffleWriteBytes = 0,
      p95ShuffleWriteBytes = 0,
      avgPeakMemoryPerTask = 0,
      p95PeakMemoryPerTask = 0,
      avgTaskDuration = 0,
      p95TaskDuration = 0,
      avgStageDuration = 0,
      recommendedExecutors = 0,
      recommendedCoresPerExecutor = 4,
      recommendedMemoryPerExecutor = 4 * 1024 * 1024 * 1024L
    )

    val recommended = StageMetricsProfile.calculateRecommendedExecutors(
      metrics,
      coresPerExecutor = 4,
      tasksPerCore = 2,
      targetTaskDurationMs = 30000,
      shuffleThresholdBytes = 1024 * 1024 * 1024,
      maxExecutors = 100
    )

    assert(recommended === 1) // Minimum
  }

  test("calculate executors with custom tasks per core") {
    val metrics = StageMetricsProfile(
      avgNumTasks = 200.0,
      minNumTasks = 200,
      maxNumTasks = 200,
      p50NumTasks = 200,
      p95NumTasks = 200,
      avgShuffleReadBytes = 0,
      p95ShuffleReadBytes = 0,
      avgShuffleWriteBytes = 0,
      p95ShuffleWriteBytes = 0,
      avgPeakMemoryPerTask = 100 * 1024 * 1024,
      p95PeakMemoryPerTask = 120 * 1024 * 1024,
      avgTaskDuration = 10000,
      p95TaskDuration = 15000,
      avgStageDuration = 30000,
      recommendedExecutors = 0,
      recommendedCoresPerExecutor = 4,
      recommendedMemoryPerExecutor = 4 * 1024 * 1024 * 1024L
    )

    // With tasksPerCore=4: 200 / (4 * 4) = 13 executors
    val recommended = StageMetricsProfile.calculateRecommendedExecutors(
      metrics,
      coresPerExecutor = 4,
      tasksPerCore = 4, // More aggressive
      targetTaskDurationMs = 30000,
      shuffleThresholdBytes = 1024 * 1024 * 1024,
      maxExecutors = 100
    )

    assert(recommended === 13)
  }

  test("calculate executors handles very high shuffle multiplier cap") {
    val metrics = StageMetricsProfile(
      avgNumTasks = 10.0,
      minNumTasks = 10,
      maxNumTasks = 10,
      p50NumTasks = 10,
      p95NumTasks = 10,
      avgShuffleReadBytes = 50L * 1024 * 1024 * 1024, // 50GB
      p95ShuffleReadBytes = 60L * 1024 * 1024 * 1024,
      avgShuffleWriteBytes = 50L * 1024 * 1024 * 1024, // 50GB
      p95ShuffleWriteBytes = 60L * 1024 * 1024 * 1024,
      avgPeakMemoryPerTask = 1024 * 1024 * 1024,
      p95PeakMemoryPerTask = 1200 * 1024 * 1024,
      avgTaskDuration = 20000,
      p95TaskDuration = 25000,
      avgStageDuration = 60000,
      recommendedExecutors = 0,
      recommendedCoresPerExecutor = 4,
      recommendedMemoryPerExecutor = 4 * 1024 * 1024 * 1024L
    )

    // 100GB / 10 tasks = 10GB per task
    // Shuffle multiplier should be capped at 1.5 (not 1 + 10*0.2 = 3.0)
    val recommended = StageMetricsProfile.calculateRecommendedExecutors(
      metrics,
      coresPerExecutor = 4,
      tasksPerCore = 2,
      targetTaskDurationMs = 30000,
      shuffleThresholdBytes = 1024 * 1024 * 1024,
      maxExecutors = 100
    )

    // Parallelism: 10/8 = 2 executors
    // Shuffle: 1.5x (capped)
    // Total: 2 * 1.5 = 3 executors
    assert(recommended === 3)
  }
}
