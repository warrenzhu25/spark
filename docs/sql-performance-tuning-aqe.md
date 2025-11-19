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

Adaptive Query Execution (AQE) is an optimization technique in Spark SQL that makes use of the runtime statistics to choose the most efficient query execution plan. By inspecting the data *during* query execution (specifically, as map tasks complete and write shuffle files), Spark can correct initial estimation errors and re-optimize the remaining logical plan.

* Table of contents
{:toc}

## AQE Overview

### Purpose: The Problem with Static Planning
Traditional query optimizers in distributed systems perform "static planning." They generate a complete physical execution plan before a single task runs. This approach relies heavily on:
1.  **Table Statistics:** Row counts, column histograms, and min/max values stored in the metastore (e.g., Hive Metastore).
2.  **Estimation Logic:** Heuristics to estimate the output size of operators (e.g., "Filtering by `date > '2023-01-01'` will reduce rows by 50%").

However, these static estimates are often wildly inaccurate because:
*   Statistics may be stale, missing, or only available at the table level (not for intermediate transformation results).
*   User-defined functions (UDFs) are "black boxes" to the optimizer, making output size prediction impossible.
*   Data distribution can be non-uniform (skewed), which averages and histograms fail to capture effectively.

**AQE solves this by breaking the query into "Query Stages".** The execution stops at materialization points (shuffles), collects precise runtime statistics from the completed stage, and then re-optimizes the subsequent stages.

### Benefits
*   **Robustness:** Queries are less likely to fail due to Out-Of-Memory (OOM) errors caused by under-provisioned partition counts or skewed data.
*   **Performance:** Reduces query latency by choosing better join strategies (e.g., avoiding sorts) and minimizing network/disk I/O (e.g., reading less data).
*   **Simplicity:** Drastically reduces the need for manual tuning of `spark.sql.shuffle.partitions` and join hints.

## Core Components

AQE primarily relies on three major optimization features:

### 1. Dynamic Join Selection
Spark supports different join strategies (Broadcast Hash Join, Shuffle Hash Join, Sort Merge Join). Static planning picks one based on estimated size.
*   **The Optimization:** If the actual size of one join side is found to be smaller than the broadcast threshold (default 10MB) after a shuffle stage, AQE can dynamically switch the plan from a **Sort-Merge Join (SMJ)** to a **Broadcast Hash Join (BHJ)**.
*   **Why it helps:**
    *   **Eliminates Sorting:** SMJ requires sorting both sides of the join, which is CPU and memory-intensive. BHJ builds a hash map, which is generally faster.
    *   **Local Shuffle Reader:** When converted to BHJ, Spark can use a `CustomShuffleReader` (specifically a local shuffle reader) to read the map outputs locally on the executors if the data was already shuffled, minimizing network traffic.
*   **Fallback:** If the data is still too large for broadcast but small enough to fit in memory partitions, AQE might convert SMJ to a **Shuffled Hash Join**, which avoids sorting but still requires shuffling.

### 2. Dynamic Shuffle Partitions (Coalescing)
Tuning `spark.sql.shuffle.partitions` (default 200) is a classic Spark pain point.
*   **Small Partition Problem:** If set too high (e.g., 2000 for 1GB of data), you get thousands of tiny files and tasks. The overhead of scheduling tasks and opening/closing files dominates execution time.
*   **Large Partition Problem:** If set too low (e.g., 10 for 100GB of data), tasks become huge. They spill to disk (slow) or crash with OOM errors.

*   **AQE Solution (Coalescing):**
    1.  Users set a **high initial shuffle partition count** (e.g., `spark.sql.shuffle.partitions=2000` or even higher based on cluster size).
    2.  During the map stage, tasks write data to these many partitions.
    3.  AQE inspects the file sizes. If it finds adjacent small partitions (e.g., partition 0 is 1MB, partition 1 is 2MB), it **coalesces** them into a single task (e.g., Task A processes partitions 0-4 to reach a target size of 64MB).
    4.  **Result:** The reducer stage runs with the optimal number of tasks for the *actual* data volume.

### 3. Skew Join Optimization
Data skew happens when one key (e.g., `NULL` or a popular `user_id`) has significantly more data than others.
*   **The Symptom:** 99% of tasks finish in seconds, but 1% (the "stragglers") take hours. The entire stage waits for these stragglers.
*   **AQE Solution:**
    1.  AQE detects partitions that are significantly larger than the median size (controlled by skew factors).
    2.  **Split:** The large partition on the skewed side is split into $N$ smaller sub-partitions.
    3.  **Replicate:** The matching partition on the other join side is replicated $N$ times.
    4.  Spark launches $N$ tasks to handle the skewed key in parallel, eliminating the bottleneck.

## Execution Flow: Detailed Walkthrough

### Standard Execution (AQE Disabled)
1.  **Analysis:** SQL is parsed.
2.  **Logical Plan:** Optimization rules apply (filter pushdown, etc.).
3.  **Physical Plan:** Spark estimates costs and picks the "best" plan (e.g., SortMergeJoin).
4.  **Execution:**
    *   Map Stage runs.
    *   Shuffle Write.
    *   Reduce Stage runs (using the pre-determined 200 partitions).
    *   *If estimations were wrong, the Reduce stage might crash or crawl.*

### Adaptive Execution (AQE Enabled)
1.  **Initial Plan:** Spark creates a plan with "Query Stages" separated by `Exchange` (shuffle) nodes.
2.  **Execute Stage 1 (Map):** Spark runs the independent stages (leaves of the query tree).
3.  **Materialization Point:** When the Map stage finishes, it writes shuffle files. **Execution Pauses.**
4.  **Stats Collection:** The `MapOutputTracker` now holds the *exact* size of every partition and the total data size.
5.  **Re-Optimization (The "Loop"):**
    *   AQE rules inspect the stats.
    *   **Rule 1 (Join):** "Is table A actually 5MB? Yes -> Switch SMJ to Broadcast."
    *   **Rule 2 (Partitions):** "Are partitions 0-50 tiny? Yes -> Merge them into one task."
    *   **Rule 3 (Skew):** "Is partition 100 huge? Yes -> Split it."
6.  **New Plan Generation:** A new physical plan is generated for the next stage.
7.  **Execute Stage 2 (Reduce):** The optimized reduce stage runs.
8.  This repeats for every shuffle boundary in the query.

## Configuration Parameters

AQE is enabled by default in Spark 3.2.0+.

### Enabling/Disabling
*   `spark.sql.adaptive.enabled` (Default: `true`)

### Tuning Coalescing
*   `spark.sql.adaptive.coalescePartitions.enabled` (Default: `true`)
*   `spark.sql.adaptive.advisoryPartitionSizeInBytes` (Default: `64MB`):
    *   The target size for a task. Lower this if tasks are still running out of memory. Increase this if you want fewer tasks (e.g., for writing fewer output files).
*   `spark.sql.adaptive.coalescePartitions.minPartitionSize` (Default: `1MB`):
    *   Prevents coalescing partitions if they are already smaller than this, maintaining a minimum level of parallelism.
*   `spark.sql.adaptive.coalescePartitions.initialPartitionNum`:
    *   If not set, uses `spark.sql.shuffle.partitions`. **Crucial:** If this initial value is too low, AQE *cannot* create more partitions. It can only reduce them. Always err on the side of setting this higher (e.g., 1000-4000) when using AQE.

### Tuning Join Optimization
*   `spark.sql.adaptive.autoBroadcastJoinThreshold`:
    *   AQE uses the standard broadcast threshold. However, you can set a specific one for AQE via `spark.sql.adaptive.autoBroadcastJoinThreshold` if you want different behavior dynamically (though usually not needed).
*   `spark.sql.adaptive.localShuffleReader.enabled` (Default: `true`):
    *   Optimizes coalesced or broadcast joins by avoiding network fetch when possible.

### Tuning Skew Join
*   `spark.sql.adaptive.skewJoin.enabled` (Default: `true`)
*   `spark.sql.adaptive.skewJoin.skewedPartitionFactor` (Default: `5`):
    *   A partition must be 5x larger than the median partition size to be considered skewed.
*   `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` (Default: `256MB`):
    *   A partition must *also* be larger than 256MB raw size to be optimized. This prevents optimizing "skew" in tiny datasets where it doesn't matter.

## Monitoring & Debugging

### Identifying AQE in Spark UI
1.  **SQL Tab:** Click on a query description.
2.  **Plan Visualization:**
    *   Look for the `AdaptiveSparkPlan` node at the top of the tree.
    *   **Coalesced Partitions:** You will see a `CustomShuffleReader`. Hover over it to see metrics like "coalesced from 2000 to 50 partitions".
    *   **Skew Join:** Look for the **SortMergeJoin** node. If skew optimization fired, the node name often changes to `SortMergeJoin(isSkew=true)`, or you will see multiple `Exchange` nodes feeding into it representing the split/replicated data.
    *   **Join Strategy Change:** If you see a `BroadcastHashJoin` where you expected a `SortMergeJoin` (and there are `Exchange` nodes above it), AQE likely performed a runtime conversion.

### Explanation
Running `df.explain()` often shows the *initial* plan. To see the *final* executed plan with AQE applied, you usually need to look at the UI after execution, or use `df.explain(mode="cost")` which might provide hints, but the runtime plan is best observed in the event logs or UI.

## Use Cases and Limitations

### Ideal Scenarios
*   **Shared Clusters:** Where default configurations must work for both "small" and "big" queries. AQE normalizes the performance.
*   **Complex ETL:** Chains of transformations where intermediate data sizes vary wildly (e.g., filter -> group by -> join).
*   **Fact-Dim Joins:** Especially where the "Dimension" table is filtered at runtime (e.g., `WHERE date = 'today'`) making it small enough to broadcast, even if the full table is large.

### Limitations
*   **Requires Shuffle:** AQE only activates at shuffle boundaries. It cannot optimize a query that is purely map-only (e.g., a simple `FILTER` and `SELECT` with no `JOIN` or `GROUP BY`).
*   **Initial Partition Count:** As mentioned, AQE can reduce partitions but cannot increase them beyond the `initialPartitionNum`. Users must still set a sufficiently high "ceiling" for parallelism.
*   **Cache Interaction:** In older Spark versions, caching could interfere with AQE because the cache acts as a static barrier. In modern Spark (3.2+), this is largely resolved, but be aware that reading from a cached DataFrame might lock in the partitioning scheme of the cache.