---
layout: global
title: Adaptive Query Execution (AQE) Deep Dive
displayTitle: Adaptive Query Execution (AQE) Deep Dive
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

Adaptive Query Execution (AQE) is an optimization technique in Spark SQL that makes use of the runtime statistics to choose the most efficient query execution plan.

* Table of contents
{:toc}

## AQE Overview

### Purpose
The primary purpose of AQE is to solve the problem of static query planning. Traditional query optimizers generate an execution plan before the query runs, basing decisions on estimated statistics. These estimates can be inaccurate due to complex data transformations, stale statistics, or unpredictable data distributions. AQE allows Spark to inspect the actual data characteristics at runtime (specifically, after shuffle stages) and re-optimize the remaining query plan dynamically.

### Benefits
*   **Robustness:** Handles unpredictable data volume and distribution better than static planning.
*   **Performance:** Reduces query latency by choosing better join strategies and reducing the amount of data shuffled.
*   **Simplicity:** Simplifies tuning. Users typically need less manual configuration (e.g., tuning `spark.sql.shuffle.partitions`) because AQE can adjust these parameters automatically.

## Core Components

AQE primarily relies on three major optimization features:

### Dynamic Join Selection
Spark can change the join strategy at runtime. For example, a join initially planned as a **Sort-Merge Join** might be converted to a **Broadcast Hash Join** if one of the participating datasets turns out to be small enough (smaller than the broadcast threshold) after the shuffle. This avoids the expensive sorting phase of the sort-merge join. It can also convert to a **Shuffled Hash Join** to optimize performance when Sort-Merge Join overhead is high.

### Dynamic Shuffle Partitions (Coalescing)
One of the most difficult parameters to tune in Spark is `spark.sql.shuffle.partitions`.
*   **Problem:** If set too small, tasks run out of memory (OOM). If set too large, it creates many small tasks, causing scheduling overhead.
*   **AQE Solution:** Users can set a relatively large number of initial partitions. AQE will then look at the actual size of the data in each partition after the shuffle. If adjacent partitions are small, AQE will coalesce (merge) them into a single partition. This ensures that tasks process a reasonable amount of data, balancing parallelism and task overhead.

### Skew Join Optimization
Data skew occurs when keys are not evenly distributed, causing some tasks to take much longer than others (stragglers).
*   **Problem:** A single slow task can delay the entire stage.
*   **AQE Solution:** AQE detects skewed partitions automatically using runtime statistics. It splits the large skewed partitions into smaller sub-partitions and replicates the corresponding partition from the other side of the join. This allows the skewed data to be processed by multiple tasks in parallel, significantly speeding up the join.

## Execution Flow

### AQE Disabled
1.  **Analysis & Logical Optimization:** Spark parses the SQL and creates a logical plan.
2.  **Physical Planning:** Spark generates one or more physical plans and selects the best one based on cost model and static statistics.
3.  **Execution:** The selected physical plan is executed from start to finish. If statistics were wrong, the plan might be suboptimal, but it cannot be changed.

### AQE Enabled
1.  **Initial Planning:** Similar to the disabled case, Spark creates an initial physical plan.
2.  **Execution of Stages:** Spark executes the plan stage-by-stage.
3.  **Materialization & Stats Collection:** As shuffle map stages finish, Spark collects accurate runtime statistics (e.g., actual row counts, partition sizes) from the map outputs.
4.  **Re-optimization:** Before scheduling the next stage (e.g., the reduce stage), AQE interrupts the flow. It checks the statistics against the active plan. If optimization opportunities (like a smaller join side or skewed partitions) are found, it generates a new, optimized physical plan for the remaining query fragments.
5.  **Continue Execution:** The query execution resumes with the new, optimized plan. This process repeats for subsequent stages.

## Configuration

AQE is enabled by default in Spark 3.2.0 and later.

### Enabling/Disabling
*   `spark.sql.adaptive.enabled`: Set to `true` (default) or `false` to enable/disable the entire AQE framework.

### Key Parameters
*   **Coalescing Partitions:**
    *   `spark.sql.adaptive.coalescePartitions.enabled`: (Default: `true`) Enables dynamic partition coalescing.
    *   `spark.sql.adaptive.advisoryPartitionSizeInBytes`: (Default: `64MB`) The target size for coalesced partitions.
    *   `spark.sql.adaptive.coalescePartitions.initialPartitionNum`: Initial number of partitions before coalescing (defaults to `spark.sql.shuffle.partitions`).
*   **Join Optimization:**
    *   `spark.sql.adaptive.autoBroadcastJoinThreshold`: (Default: uses `spark.sql.autoBroadcastJoinThreshold`) Configures the threshold for switching to broadcast join at runtime.
    *   `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold`: Configures threshold to prefer Shuffled Hash Join over Sort Merge Join.
*   **Skew Join:**
    *   `spark.sql.adaptive.skewJoin.enabled`: (Default: `true`) Enables automatic skew handling.
    *   `spark.sql.adaptive.skewJoin.skewedPartitionFactor`: (Default: `5`) A partition is skewed if it is N times larger than the median partition size.
    *   `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes`: (Default: `256MB`) Minimum size for a partition to be considered skewed.

## Monitoring & Debugging

### Metrics
When AQE applies an optimization, specific metrics are often added to the query plan nodes.
*   **Num Coalesced Partitions:** Indicates how many shuffle partitions were merged.
*   **Num Split Partitions:** Indicates how many skewed partitions were split.

### UI Analysis
In the Spark SQL UI (SQL Tab):
1.  **"AdaptiveSparkPlan" Node:** You will see an `AdaptiveSparkPlan` node wrapping the query plan.
2.  **"Final Plan" vs. "Initial Plan":** The UI often shows the plan changing. You might see a `CustomShuffleReader` node, which indicates that AQE has intervened (e.g., for coalescing or skew handling).
    *   **Coalescing:** Look for `CustomShuffleReader coalesced`.
    *   **Skew Join:** Look for `SortMergeJoin (isSkew=true)` or similar indicators in the details of the join node.
3.  **Runtime Statistics:** The details of plan nodes (like `Exchange` or `QueryStage`) will show `isRuntime=true` for statistics, confirming they are real values observed during execution.

## Use Cases

### When to Use (Recommended)
*   **General ETL/SQL Workloads:** AQE is beneficial for almost all batch data processing where data volume can vary or is not perfectly known in advance.
*   **Complex Joins:** Queries involving multiple joins where intermediate result sizes are hard to predict.
*   **Skewed Data:** Workloads known to have uneven data distribution (e.g., null keys or popular items).
*   **Star Schema Joins:** Fact-to-dimension table joins often benefit from dynamic broadcast join conversion.

### When Not to Use
*   **Streaming Workloads:** AQE is primarily designed for batch queries. While some concepts apply to Structured Streaming, the stateful nature and continuous execution of streaming make full AQE less applicable or supported differently.
*   **Extremely Low Latency Queries:** The overhead of re-optimization (pausing execution, planning again) might introduce a small latency penalty that is undesirable for sub-second, interactive dashboard queries (though usually negligible compared to the gains).
*   **Predictable Static Workloads:** If you have a highly tuned, static pipeline where data sizes never change and you have manually optimized every partition count and join type perfectly, AQE might not add value (though it rarely hurts).
