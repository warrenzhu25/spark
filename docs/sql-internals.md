---
layout: global
title: Spark SQL Internals Deep Dive
displayTitle: Spark SQL Internals Deep Dive
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

Spark SQL is not just a SQL engine; it is a sophisticated compiler that translates high-level queries (SQL or DataFrame/Dataset API) into highly optimized executable code (RDDs). This guide explores the internal architecture of Spark SQL, focusing on the **Catalyst Optimizer** and the **Tungsten Execution Engine**.

* Table of contents
{:toc}

## Architecture Overview

The execution of a Spark SQL query follows a multi-stage pipeline, transforming a user's code through several tree-based representations before finally executing as tasks on the cluster.

**The Pipeline:**
1.  **Unresolved Logical Plan:** Parsed SQL or API calls.
2.  **Analyzed Logical Plan:** resolved against the Catalog.
3.  **Optimized Logical Plan:** Transformed by rule-based optimizations.
4.  **Physical Plan:** Selected based on cost models and strategies.
5.  **Executed RDDs:** Java bytecode (generated or interpreted) running on Executors.

## The Catalyst Optimizer

Catalyst is the extensible query optimizer at the core of Spark SQL. It is implemented in Scala and uses functional programming constructs heavily.

### Core Abstractions
*   **TreeNode:** The base class for all nodes in the query plan (e.g., `Filter`, `Project`, `Join`). Trees are immutable. Modifications (optimizations) produce new trees.
*   **Rules:** Functions that transform a `TreeNode` into another `TreeNode`. Rules are grouped into batches and executed by a `RuleExecutor`.

### Phase 1: Analysis
When you write `spark.sql("SELECT * FROM my_table")`, Spark initially doesn't know if `my_table` exists or what columns it has. The result is an **Unresolved Logical Plan**.
*   **The Analyzer:** Uses the **SessionCatalog** and **FunctionRegistry** to look up table names, column names, and data types.
*   **Result:** A **Resolved Logical Plan**. If a table or column doesn't exist, an `AnalysisException` is thrown here.

### Phase 2: Logical Optimization
This phase applies standard compiler optimizations to the logical plan. These are primarily **Rule-Based Optimizations (RBO)**.
*   **Predicate Pushdown:** Pushes filter conditions as close to the source as possible to reduce data volume early.
*   **Column Pruning:** Removes columns that are not required by the final output.
*   **Constant Folding:** Pre-calculates expressions (e.g., `1 + 1` becomes `2`).
*   **Boolean Simplification:** Simplifies logic (e.g., `NOT (A OR B)` becomes `NOT A AND NOT B`).
*   **Null Propagation:** Infers if an expression will be null.

### Phase 3: Physical Planning
The **SparkPlanner** takes the Optimized Logical Plan and generates one or more **Physical Plans**.
*   **Strategies:** It maps logical nodes to physical operators.
    *   *Logical Join* -> *BroadcastHashJoin*, *SortMergeJoin*, or *ShuffleHashJoin*.
    *   *Logical Aggregate* -> *HashAggregate* or *SortAggregate*.
*   **Cost-Based Optimization (CBO):** If multiple physical strategies are valid (e.g., Broadcast vs. SortMerge), Spark uses a cost model (based on table statistics like row count and size) to pick the cheapest one.

## Tungsten Execution Engine

Once the physical plan is selected, the **Tungsten** engine is responsible for executing it efficiently. Tungsten focuses on optimizing CPU and memory efficiency.

### Memory Management
Java objects have high overhead (headers, pointers) and heavily tax the Garbage Collector (GC). Tungsten manages memory explicitly to avoid this.
*   **UnsafeRow:** An internal binary row format. Data is stored as raw bytes (off-heap or on-heap) rather than as Java objects.
    *   This is much more compact than Java objects.
    *   It allows accurate memory accounting.
    *   It avoids GC pauses because the data is not visible to the JVM's GC scanner.
*   **Page-Based Memory:** Memory is managed in pages (similar to an OS), allowing Spark to spill to disk gracefully when memory is exhausted.

### Code Generation (Whole-Stage CodeGen)
Traditional database engines use the "Volcano Iterator Model," where each operator calls `next()` on its child. This introduces virtual function call overhead for every row.
*   **Whole-Stage Code Generation (WSCG):** Spark collapses a chain of operators (e.g., `Scan` -> `Filter` -> `Project`) into a **single Java function**.
    *   It generates raw Java source code strings at runtime.
    *   It compiles this code using **Janino** (a fast, lightweight Java compiler).
    *   **Benefits:** Removing virtual function calls, keeping data in CPU L1/L2 cache, and enabling compiler optimizations like loop unrolling.

### Vectorized Execution
For file formats that support columnar storage (Parquet, ORC), Spark uses **Vectorized Execution**.
*   Instead of processing one row at a time, it processes a **batch** of rows (e.g., 4096 rows) for a single column at once.
*   This leverages **SIMD (Single Instruction, Multiple Data)** instructions in modern CPUs, allowing operations to be applied to multiple data points in a single CPU cycle.

## Query Execution Lifecycle: A Walkthrough

Let's trace `df = spark.table("users").filter("age > 21").select("name")`:

1.  **API Call:** The user calls the DataFrame API. Spark constructs an `UnresolvedRelation` ("users") linked to a `Filter` and a `Project`.
2.  **Analysis:** The Analyzer looks up "users" in the catalog, verifies "age" and "name" columns exist, and resolves their IDs.
3.  **Optimization:** The Optimizer sees a `Filter` on "age". It checks if the data source (e.g., Parquet) supports filter pushdown. It might push this filter directly into the `FileScan` node.
4.  **Physical Planning:**
    *   The `Filter` and `Project` are mapped to physical counterparts.
    *   The `Scan` is mapped to a `FileSourceScanExec`.
5.  **Preparation for Execution:**
    *   Spark inserts `EnsureRequirements` rules. If a join required a shuffle, exchange nodes are added here.
    *   **CollapseCodegenStages:** The planner identifies that `Scan`, `Filter`, and `Project` can be fused. It inserts a `WholeStageCodegenExec` node wrapping them.
6.  **Execution:**
    *   The DAGScheduler breaks the plan into Stages.
    *   Tasks are sent to Executors.
    *   On the Executor, the generated Java code runs, scanning the file, applying the filter in a tight loop, and returning the result rows.

## Complex Query Walkthrough: Join & Aggregation

Let's examine a more complex query involving a join and an aggregation to see how Spark handles shuffling and distributed state.

**Query:**
```sql
SELECT d.dept_name, AVG(e.salary)
FROM employees e
JOIN departments d ON e.dept_id = d.id
GROUP BY d.dept_name
```

### 1. Logical Plan & Optimization
*   **Analysis:** Resolves `employees` (e) and `departments` (d). Checks that `salary`, `dept_id`, `id`, and `dept_name` exist.
*   **Optimization:**
    *   **Column Pruning:** Spark sees that only `e.salary`, `e.dept_id`, `d.id`, and `d.dept_name` are needed. Other columns (like `e.address` or `d.manager`) are removed from the scan.
    *   **Predicate Pushdown:** If there were `WHERE` clauses, they would be pushed to the source.

### 2. Physical Planning (Strategy Selection)
Spark decides how to physically execute the Join and Aggregate.
*   **Join Strategy:**
    *   If `departments` is very small (< 10MB), Spark chooses **BroadcastHashJoin**.
    *   If both are large, Spark chooses **SortMergeJoin** (or **ShuffledHashJoin** if enabled/applicable). Let's assume **SortMergeJoin** for this example.
*   **Aggregate Strategy:**
    *   Spark generally chooses **HashAggregate** (using an in-memory hash map).

### 3. Execution Prep (The "EnsureRequirements" Rule)
SortMergeJoin requires that data on both sides be:
1.  **Partitioned** by the join key (`e.dept_id` and `d.id`).
2.  **Sorted** by the join key.

Since the raw files are likely not distributed this way, Spark inserts **Exchange** (Shuffle) and **Sort** operators.

### 4. Final Execution Plan & Stages
The physical plan is broken into **Stages** at Shuffle boundaries.

**Stage 1: Scan & Shuffle Write (Employees)**
*   **Scan:** Read `employees` file.
*   **Filter/Project:** Extract `salary`, `dept_id`.
*   **Shuffle Write:** Hash the rows by `dept_id` and write to local disk, divided into partition buckets.

**Stage 2: Scan & Shuffle Write (Departments)**
*   **Scan:** Read `departments` file.
*   **Filter/Project:** Extract `id`, `dept_name`.
*   **Shuffle Write:** Hash the rows by `id` and write to local disk.

**Stage 3: Shuffle Read, Join, & Partial Aggregation**
*   **Shuffle Read:** Read the relevant partitions from Stage 1 and Stage 2.
*   **Sort:** Sort the employee rows by `dept_id` and department rows by `id`.
*   **SortMergeJoin:** Merge the two sorted streams. Matches produce rows: `(dept_name, salary)`.
*   **Partial Aggregation:**
    *   Instead of sending all raw `(dept_name, salary)` rows to the next stage, Spark pre-aggregates.
    *   It computes a running `SUM(salary)` and `COUNT(salary)` for each `dept_name` seen in this task.
*   **Shuffle Write:** Hash the partial results by `dept_name` and write to disk.

**Stage 4: Final Aggregation**
*   **Shuffle Read:** Read partial sums/counts from Stage 3.
*   **Final Aggregation:** Merge the partial sums/counts for each `dept_name`.
    *   `Total Sum = sum(partial_sums)`
    *   `Total Count = sum(partial_counts)`
    *   `Average = Total Sum / Total Count`
*   **Result:** Return final rows to the driver or write to output.

### Key Optimizations in this Flow
*   **Partial Aggregation:** drastically reduces data sent over the network between Stage 3 and 4.
*   **Whole-Stage Codegen:** In Stage 3, the Sort -> Join -> Project -> Partial Aggregate chain is likely compiled into a single while-loop in Java, avoiding object creation for intermediate rows.