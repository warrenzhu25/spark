---
title: "Adaptive Input Partitioning Design Document"
subtitle: "Dynamic Partition Size Adjustment for Apache Spark"
author: "Adaptive Input Partitioning Feature Team"
date: "2025-10-27"
version: "1.0"
tags: ["Apache Spark", "AQE", "Performance", "Optimization"]
---

# Adaptive Input Partitioning Design Document

**Dynamic Partition Size Adjustment for Apache Spark**

---

## Table of Contents

1. [Overview](#1-overview)
   - [Problem Statement](#problem-statement)
   - [Goal](#goal)
   - [Example Scenario](#example-scenario)

2. [Current Implementation Analysis](#2-current-implementation-analysis)
   - [File Partition Assignment Flow](#21-file-partition-assignment-flow)
   - [Adaptive Query Execution Infrastructure](#22-adaptive-query-execution-aqe-infrastructure)

3. [Proposed Solution](#3-proposed-solution)
   - [Architecture](#31-architecture)
   - [Detailed Design](#32-detailed-design)
   - [Configuration Parameters](#33-configuration-parameters)
   - [Metrics](#34-metrics)

4. [Use Cases and Examples](#4-use-cases-and-examples)
   - [Highly Compressed Parquet Files](#use-case-1-highly-compressed-parquet-files)
   - [Selective Filters](#use-case-2-selective-filters)
   - [Iterative ML Workloads](#use-case-3-iterative-ml-workloads)

5. [Limitations and Edge Cases](#5-limitations-and-edge-cases)
   - [When It Doesn't Help](#51-when-it-doesnt-help)
   - [Risk Factors](#52-risk-factors)
   - [Edge Cases](#53-edge-cases)

6. [Implementation Phases](#6-implementation-phases)

7. [Testing Strategy](#7-testing-strategy)
   - [Unit Tests](#71-unit-tests)
   - [Integration Tests](#72-integration-tests)
   - [Performance Benchmarks](#73-performance-benchmarks)

8. [Alternatives Considered](#8-alternatives-considered)

9. [Future Enhancements](#9-future-enhancements)

10. [Open Questions](#10-open-questions)

11. [References](#11-references)

---

<div style="page-break-after: always;"></div>

## 1. Overview

### Problem Statement

Currently, `spark.sql.files.maxPartitionBytes` is a **static configuration** (default 128MB) that determines how much data each input partition reads from files. This static approach has three major limitations:

1. **Compression Ratio Unknown**
   When reading compressed files (Parquet, ORC), we don't know the output size until after reading

2. **Filter Selectivity Unknown**
   When applying filters, we don't know how much data will survive filtering

3. **Suboptimal Shuffle Sizes**
   Input partitions of 128MB might produce shuffle partitions of 10MB (too small, overhead) or 500MB (too large, memory pressure)

> **Key Insight**: Static partition sizing leads to inefficient resource utilization because we can't predict the compression ratio or filter selectivity at planning time.

---

### Goal

**Dynamically adjust input partition sizes** based on observed compression ratios and filter selectivity from earlier query stages to achieve optimal shuffle partition sizes.

**Target**: Produce shuffle partitions of optimal size (default 64MB) by learning from runtime statistics.

---

### Example Scenario

**Current Behavior (Static):**
```
Task 1: Read 128MB → Apply filter → Write 10MB shuffle (12.8:1 ratio)
Task 2: Read 128MB → Apply filter → Write 12MB shuffle
Task 3: Read 128MB → Apply filter → Write 9MB shuffle

Result: 100 shuffle partitions × 10-12MB each = High overhead
```

**Desired Behavior (Adaptive):**
```
Stage 1: Tasks read 128MB → Learn median ratio is ~12:1
Stage 2: Adjust input to 1280MB to target ~100MB shuffle output

Result: 16 shuffle partitions × 100MB each = Optimal size
```

**Benefit**: Fewer tasks, less overhead, better resource utilization.

---

<div style="page-break-after: always;"></div>

## 2. Current Implementation Analysis

### 2.1 File Partition Assignment Flow

The current file partitioning process has four steps:

#### Step 1: Calculate maxSplitBytes

**Location**: `FilePartition.scala:118`

```scala
def maxSplitBytes(
    sparkSession: SparkSession,
    calculateTotalBytes: => Long): Long = {

  val defaultMaxSplitBytes =
    sparkSession.sessionState.conf.filesMaxPartitionBytes  // 128MB
  val openCostInBytes =
    sparkSession.sessionState.conf.filesOpenCostInBytes    // 4MB
  val minPartitionNum =
    sparkSession.sessionState.conf.filesMinPartitionNum
      .getOrElse(sparkSession.leafNodeDefaultParallelism)

  val totalBytes = calculateTotalBytes
  val bytesPerCore = totalBytes / minPartitionNum

  Math.min(defaultMaxSplitBytes,
           Math.max(openCostInBytes, bytesPerCore))
}
```

**Key Point**: Calculated **at planning time** using static configuration.

---

#### Step 2: Split Files

**Location**: `PartitionedFileUtil.scala:27`

For splitable files (Parquet, ORC), create multiple `PartitionedFile` chunks:

```scala
def splitFiles(
    file: FileStatusWithMetadata,
    maxSplitBytes: Long,
    partitionValues: InternalRow): Seq[PartitionedFile] = {

  if (isSplitable) {
    (0L until file.getLen by maxSplitBytes).map { offset =>
      val size = Math.min(maxSplitBytes, file.getLen - offset)
      PartitionedFile(partitionValues, filePath, offset, size)
    }
  } else {
    Seq(PartitionedFile(partitionValues, filePath, 0, file.getLen))
  }
}
```

**Result**: Large files split into chunks of `maxSplitBytes` each.

---

#### Step 3: Pack into Partitions

**Location**: `FilePartition.scala:58`

Uses **"Next Fit Decreasing"** algorithm to group file chunks:

```scala
private def getFilePartitions(
    partitionedFiles: Seq[PartitionedFile],
    maxSplitBytes: Long,
    openCostInBytes: Long): Seq[FilePartition] = {

  partitionedFiles.foreach { file =>
    if (currentSize + file.length > maxSplitBytes) {
      closePartition()  // Start new partition
    }
    currentSize += file.length + openCostInBytes
    currentFiles += file
  }

  partitions.toSeq
}
```

**Result**: Multiple `PartitionedFile` objects grouped into `FilePartition` tasks.

---

#### Step 4: Task Reads Assigned Files

**Location**: `FileScanRDD.scala:111`

Each task receives a `FilePartition` containing specific byte ranges to read:

```scala
override def compute(
    split: RDDPartition,
    context: TaskContext): Iterator[InternalRow] = {

  val files = split.asInstanceOf[FilePartition].files.iterator

  files.map { partitionedFile =>
    // Each PartitionedFile specifies:
    //   - filePath: which file
    //   - start: byte offset
    //   - length: bytes to read
    readFunction(partitionedFile)
  }
}
```

> **Critical Observation**: `maxSplitBytes` is calculated **at planning time** before any data is read. We cannot use runtime statistics with the current architecture without modifications.

---

<div style="page-break-after: always;"></div>

### 2.2 Adaptive Query Execution (AQE) Infrastructure

Spark already has a sophisticated AQE framework that collects runtime statistics and re-optimizes queries.

#### Shuffle Statistics Collection

**Location**: `ShuffleQueryStageExec.scala`

```scala
case class ShuffleQueryStageExec(...) extends QueryStageExec {
  var mapStats: Option[MapOutputStatistics] = None

  override def doMaterialize(): Future[MapOutputStatistics] = {
    sparkContext.submitMapStage(shuffleDependency)
  }
}
```

**MapOutputStatistics** contains per-partition shuffle sizes:

```scala
class MapOutputStatistics(
    val shuffleId: Int,
    val bytesByPartitionId: Array[Long])  // ← Size of each partition
```

---

#### Existing AQE Optimizations

AQE currently supports four major optimizations based on `MapOutputStatistics`:

1. **CoalesceShufflePartitions**
   Combines small partitions based on `bytesByPartitionId`

2. **OptimizeSkewedJoin**
   Splits large partitions to handle data skew

3. **DynamicJoinSelection**
   Converts between join types (SMJ ↔ BHJ) based on partition sizes

4. **OptimizeShuffleWithLocalRead**
   Enables local reads instead of network shuffle

---

#### AQE Re-optimization Loop

**Location**: `AdaptiveSparkPlanExec.scala:356`

After each stage completes, AQE re-optimizes the remaining plan:

```scala
private def reOptimize(logicalPlan: LogicalPlan): SparkPlan = {
  logicalPlan.invalidateStatsCache()
  val optimized = optimizer.execute(logicalPlan)
  // optimizer contains all AQE rules including our new one
  planner.plan(optimized).next()
}
```

> **Integration Opportunity**: We can add a new AQE rule that adjusts file scan partition sizes based on completed shuffle statistics.

---

<div style="page-break-after: always;"></div>

## 3. Proposed Solution

### 3.1 Architecture

#### High-Level Design

**Add new AQE rule**: `AdaptiveInputPartitioning`

**Execution Flow**:

1. Stage 1 completes → Shuffle statistics collected
2. AQE re-optimization triggered
3. `AdaptiveInputPartitioning` rule examines shuffle stats
4. Calculate compression/selectivity ratio
5. Adjust `maxSplitBytes` for subsequent file scans
6. Stage 2 executes with optimized partition sizes

---

#### Flow Diagram

```
┌──────────────────────────────────────────────────────────┐
│ Stage 1: File Scan → Filter → Shuffle                   │
│   Input:  128MB partitions × 100 tasks = 12.8GB         │
│   Output: 10MB shuffle partitions × 100 = 1GB           │
│   Ratio:  12.8:1 compression/filtering                  │
└───────────────────────┬──────────────────────────────────┘
                        │ Statistics collected
                        ▼
┌──────────────────────────────────────────────────────────┐
│ AQE Re-optimization Loop                                │
│   • AdaptiveInputPartitioning rule executes             │
│   • Observes: median shuffle partition = 10MB           │
│   • Target: 64MB shuffle partitions (advisory size)     │
│   • Calculates: 128MB × (64MB/10MB) ≈ 819MB            │
└───────────────────────┬──────────────────────────────────┘
                        │ Plan adjusted
                        ▼
┌──────────────────────────────────────────────────────────┐
│ Stage 2: File Scan (with adjusted partitioning)         │
│   Input:  819MB partitions × 16 tasks = 12.8GB          │
│   Output: 64MB shuffle partitions × 16 (optimal!)       │
└──────────────────────────────────────────────────────────┘
```

---

#### When It Runs

- **After** a shuffle stage completes
- **Before** planning subsequent file scan stages
- **Within** the AQE re-optimization loop
- **Only if** enabled via configuration

---

#### What It Does

1. **Examines** completed shuffle stages and collects `MapOutputStatistics`
2. **Calculates** compression/selectivity ratio: `inputBytes / shuffleOutputBytes`
3. **Computes** adjusted `maxSplitBytes` to target optimal shuffle size
4. **Injects** adjusted value into file scan planning

---

<div style="page-break-after: always;"></div>

### 3.2 Detailed Design

The implementation consists of four main components:

#### Component 1: Statistics Tracker

**Purpose**: Track input/output bytes for each stage to calculate compression ratio

**Location**: `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveInputStatisticsTracker.scala`

```scala
case class StageCompressionStats(
    stageId: Int,
    inputBytesRead: Long,
    shuffleBytesWritten: Long,
    partitionSizes: Array[Long]) {

  def compressionRatio: Double = {
    if (shuffleBytesWritten > 0) {
      inputBytesRead.toDouble / shuffleBytesWritten.toDouble
    } else {
      1.0
    }
  }

  def medianShufflePartitionSize: Long = {
    if (partitionSizes.nonEmpty) {
      Utils.median(partitionSizes, false).toLong
    } else {
      0L
    }
  }
}
```

**Key Methods**:

- `recordStageStats()`: Store statistics when stage completes
- `getRecentCompressionRatio()`: Calculate ratio from recent N stages
- `medianShufflePartitionSize`: Use median (more robust than mean)

---

**Statistics Tracker Class**:

```scala
class AdaptiveInputStatisticsTracker {
  private val stageStats =
    mutable.HashMap[Int, StageCompressionStats]()

  def recordStageStats(
      stageId: Int,
      inputMetrics: InputMetrics,
      mapStats: MapOutputStatistics): Unit = {
    val stats = StageCompressionStats(
      stageId = stageId,
      inputBytesRead = inputMetrics.bytesRead,
      shuffleBytesWritten = mapStats.bytesByPartitionId.sum,
      partitionSizes = mapStats.bytesByPartitionId
    )
    stageStats(stageId) = stats
  }

  def getRecentCompressionRatio(
      lookbackStages: Int = 3): Option[Double] = {
    val recentStats =
      stageStats.values.toSeq.takeRight(lookbackStages)

    if (recentStats.nonEmpty) {
      val totalInput = recentStats.map(_.inputBytesRead).sum
      val totalOutput = recentStats.map(_.shuffleBytesWritten).sum

      if (totalOutput > 0) {
        Some(totalInput.toDouble / totalOutput.toDouble)
      } else {
        None
      }
    } else {
      None
    }
  }
}
```

> **Design Choice**: Use rolling window of recent stages (default 3) to handle changing data characteristics.

---

<div style="page-break-after: always;"></div>

#### Component 2: Adaptive Partitioning Rule

**Location**: `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveInputPartitioning.scala`

This is the core AQE rule that adjusts partition sizes.

```scala
case class AdaptiveInputPartitioning(
    session: SparkSession,
    statsTracker: AdaptiveInputStatisticsTracker)
  extends Rule[SparkPlan] {

  private def conf = session.sessionState.conf

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.adaptiveInputPartitioningEnabled) {
      return plan
    }

    plan.transformUp {
      case scan: FileSourceScanExec =>
        adjustFileScan(scan)
      case scan: BatchScanExec
        if scan.scan.isInstanceOf[FileScan] =>
        adjustBatchScan(scan)
    }
  }

  // ... implementation details follow
}
```

---

**Adjustment Logic**:

```scala
private def adjustFileScan(
    scan: FileSourceScanExec): FileSourceScanExec = {

  statsTracker.getRecentCompressionRatio() match {
    case Some(ratio)
      if ratio > conf.adaptiveInputPartitioningMinRatio =>

      val currentMaxSplit = scan.maxSplitBytes
      val adjustedMaxSplit =
        computeAdjustedMaxSplit(currentMaxSplit, ratio)

      if (adjustedMaxSplit != currentMaxSplit) {
        logInfo(
          s"Adjusting maxSplitBytes: " +
          s"$currentMaxSplit → $adjustedMaxSplit " +
          s"(ratio: $ratio)")

        scan.copy(
          maxSplitBytesOverride = Some(adjustedMaxSplit))
      } else {
        scan
      }

    case _ =>
      scan  // No adjustment
  }
}
```

---

**Calculation with Safety Bounds**:

```scala
private def computeAdjustedMaxSplit(
    currentMaxSplit: Long,
    compressionRatio: Double): Long = {

  val targetShuffleSize =
    conf.advisoryPartitionSizeInBytes  // 64MB
  val currentShuffleSize =
    currentMaxSplit / compressionRatio

  if (currentShuffleSize > 0) {
    val scaleFactor = targetShuffleSize / currentShuffleSize
    val adjustedMaxSplit =
      (currentMaxSplit * scaleFactor).toLong

    // Apply safety bounds
    val minAllowed = conf.adaptiveInputPartitioningMinBytes
    val maxAllowed = conf.adaptiveInputPartitioningMaxBytes
    val maxScaleFactor =
      conf.adaptiveInputPartitioningMaxScaleFactor

    adjustedMaxSplit
      .max(minAllowed)        // Min: 64MB
      .min(maxAllowed)        // Max: 1GB
      .min((currentMaxSplit * maxScaleFactor).toLong)  // Max 10x
  } else {
    currentMaxSplit
  }
}
```

> **Safety Bounds**: Critical to prevent extreme adjustments that could cause OOM or excessive parallelism.

---

<div style="page-break-after: always;"></div>

#### Component 3: FileSourceScanExec Modification

**Location**: `sql/core/src/main/scala/org/apache/spark/sql/execution/DataSourceScanExec.scala`

Add optional override parameter for adaptive partitioning:

```scala
case class FileSourceScanExec(
    @transient relation: HadoopFsRelation,
    output: Seq[Attribute],
    requiredSchema: StructType,
    partitionFilters: Seq[Expression],
    optionalBucketSet: Option[BitSet],
    optionalNumCoalescedBuckets: Option[Int],
    dataFilters: Seq[Expression],
    tableIdentifier: Option[TableIdentifier],
    disableBucketedScan: Boolean = false,
    maxSplitBytesOverride: Option[Long] = None  // ← New parameter
) extends DataSourceScanExec {

  // Modify maxSplitBytes calculation
  private lazy val maxSplitBytes: Long = {
    maxSplitBytesOverride.getOrElse {
      FilePartition.maxSplitBytes(sparkSession, selectedPartitions)
    }
  }

  // Rest of implementation unchanged
}
```

**Key Changes**:
- Add `maxSplitBytesOverride` parameter (default `None`)
- Use override if present, otherwise use default calculation
- Backward compatible (no changes to existing behavior)

---

<div style="page-break-after: always;"></div>

#### Component 4: Integration with AQE

**Location**: `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveSparkPlanExec.scala`

Integrate statistics tracker into AQE execution:

```scala
class AdaptiveSparkPlanExec(...) {

  // Create statistics tracker
  private val inputStatsTracker =
    new AdaptiveInputStatisticsTracker()

  // Record statistics when stage completes
  private def onStageSuccess(stage: QueryStageExec): Unit = {
    stage match {
      case shuffleStage: ShuffleQueryStageExec
        if shuffleStage.mapStats.isDefined =>

        val mapStats = shuffleStage.mapStats.get
        val inputMetrics =
          shuffleStage.plan.metrics.get("inputMetrics")

        inputStatsTracker.recordStageStats(
          stageId = shuffleStage.id,
          inputMetrics = inputMetrics,
          mapStats = mapStats
        )

      case _ => // Non-shuffle stages
    }
  }

  // Pass tracker to optimizer
  private def optimizer: AQEOptimizer = {
    new AQEOptimizer(conf, inputStatsTracker)
  }
}
```

---

**Register Rule in AQEOptimizer**:

```scala
class AQEOptimizer(
    conf: SQLConf,
    statsTracker: AdaptiveInputStatisticsTracker)
  extends RuleExecutor[LogicalPlan] {

  override def batches: Seq[Batch] = Seq(
    Batch("Adaptive Input Partitioning", Once,
      AdaptiveInputPartitioning(sparkSession, statsTracker)),

    Batch("Propagate Empty Relations", Once,
      // ... existing rules ...
    ),

    // ... other batches ...
  )
}
```

> **Integration Point**: The new rule runs as the first AQE optimization, before existing rules.

---

<div style="page-break-after: always;"></div>

### 3.3 Configuration Parameters

**Location**: `sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala`

#### Enable/Disable Feature

```scala
val ADAPTIVE_INPUT_PARTITIONING_ENABLED =
  buildConf("spark.sql.adaptive.inputPartitioning.enabled")
    .doc(
      "When true, adaptively adjust file input partition " +
      "sizes based on observed compression ratios and " +
      "filter selectivity from prior shuffle stages.")
    .version("4.1.0")
    .booleanConf
    .createWithDefault(false)
```

**Default**: `false` (opt-in for safety)

---

#### Minimum Compression Ratio

```scala
val ADAPTIVE_INPUT_PARTITIONING_MIN_RATIO =
  buildConf(
    "spark.sql.adaptive.inputPartitioning.minCompressionRatio")
    .doc(
      "Minimum input/output compression ratio required " +
      "to trigger adaptive adjustments. Values below " +
      "this are considered noise.")
    .version("4.1.0")
    .doubleConf
    .checkValue(v => v >= 1.0, "Must be >= 1.0")
    .createWithDefault(1.5)
```

**Default**: `1.5` (50% compression minimum)

**Rationale**: Small ratios (close to 1.0) don't justify adjustment overhead.

---

#### Safety Bounds

```scala
val ADAPTIVE_INPUT_PARTITIONING_MIN_BYTES =
  buildConf(
    "spark.sql.adaptive.inputPartitioning.minPartitionBytes")
    .doc(
      "Minimum allowed input partition size in bytes " +
      "when adaptively adjusting.")
    .version("4.1.0")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("64MB")

val ADAPTIVE_INPUT_PARTITIONING_MAX_BYTES =
  buildConf(
    "spark.sql.adaptive.inputPartitioning.maxPartitionBytes")
    .doc(
      "Maximum allowed input partition size in bytes " +
      "when adaptively adjusting.")
    .version("4.1.0")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("1GB")
```

**Bounds**: 64MB ≤ partition size ≤ 1GB

**Rationale**: Prevent too-small (overhead) or too-large (OOM) partitions.

---

#### Maximum Scale Factor

```scala
val ADAPTIVE_INPUT_PARTITIONING_MAX_SCALE_FACTOR =
  buildConf(
    "spark.sql.adaptive.inputPartitioning.maxScaleFactor")
    .doc(
      "Maximum factor by which input partition size " +
      "can be scaled up or down.")
    .version("4.1.0")
    .doubleConf
    .checkValue(v => v >= 1.0, "Must be >= 1.0")
    .createWithDefault(10.0)
```

**Default**: `10.0` (max 10x adjustment)

**Rationale**: Prevents extreme adjustments from outlier ratios.

---

#### Lookback Window

```scala
val ADAPTIVE_INPUT_PARTITIONING_LOOKBACK_STAGES =
  buildConf(
    "spark.sql.adaptive.inputPartitioning.lookbackStages")
    .doc(
      "Number of recent shuffle stages to consider " +
      "when calculating average compression ratio.")
    .version("4.1.0")
    .intConf
    .checkValue(v => v > 0, "Must be positive")
    .createWithDefault(3)
```

**Default**: `3` stages

**Rationale**: Recent stages more representative than all history.

---

#### Configuration Summary Table

| Configuration | Default | Range | Purpose |
|--------------|---------|-------|---------|
| `enabled` | `false` | true/false | Enable feature |
| `minCompressionRatio` | `1.5` | ≥ 1.0 | Minimum ratio to trigger |
| `minPartitionBytes` | `64MB` | > 0 | Lower bound |
| `maxPartitionBytes` | `1GB` | > 0 | Upper bound |
| `maxScaleFactor` | `10.0` | ≥ 1.0 | Max adjustment |
| `lookbackStages` | `3` | > 0 | Rolling window size |

---

<div style="page-break-after: always;"></div>

### 3.4 Metrics

Add new metrics to track effectiveness in `AdaptiveSparkPlanExec`:

```scala
private val adaptiveInputMetrics = Map(
  "numInputPartitionAdjustments" ->
    SQLMetrics.createMetric(sparkContext,
      "number of input partition adjustments"),

  "avgCompressionRatio" ->
    SQLMetrics.createAverageMetric(sparkContext,
      "average compression ratio"),

  "inputPartitionSizeBefore" ->
    SQLMetrics.createSizeMetric(sparkContext,
      "input partition size before adjustment"),

  "inputPartitionSizeAfter" ->
    SQLMetrics.createSizeMetric(sparkContext,
      "input partition size after adjustment")
)
```

**Metrics Visible In**:
- Spark UI (SQL tab)
- Query execution plan
- Application history

**Use Cases**:
- Verify optimizations are applied
- Debug unexpected behavior
- Measure effectiveness

---

<div style="page-break-after: always;"></div>

## 4. Use Cases and Examples

### Use Case 1: Highly Compressed Parquet Files

**Scenario**: Reading Snappy-compressed Parquet files with 10:1 compression ratio

```sql
-- Table: events (Parquet, Snappy compressed)
-- Compression: 10:1 ratio
SELECT user_id, COUNT(*)
FROM events
WHERE event_type = 'click'
GROUP BY user_id
```

**Stage 1: Initial Scan + Shuffle**
- Input: 128MB compressed × 100 partitions = 12.8GB compressed
- After decompression: 1.28TB uncompressed
- After filter (5% selectivity): 64GB
- Shuffle output: 64GB / 100 partitions = ~640MB per partition

**Problem**: Shuffle partitions too large (640MB >> 64MB target)

**Stage 2: With Adaptive Input**
- Learned ratio: 1.28TB / 64GB = 20:1
- Target shuffle size: 64MB
- Adjusted input: 128MB × (64MB / 640MB) = 12.8MB compressed
- Result: 12.8MB → 128MB uncompressed → 6.4MB after filter ≈ 64MB (optimal!)

**Benefit**: 10x fewer tasks, optimal partition sizes, better performance.

---

### Use Case 2: Selective Filters

**Scenario**: Highly selective filter eliminates 95% of data

```sql
SELECT *
FROM large_table
WHERE country = 'US'  -- Only 5% of data
  AND year = 2024
```

**Stage 1: Initial Scan**
- Input: 128MB × 1000 partitions = 128GB
- Filter selectivity: 5%
- Shuffle output: 6.4GB / 1000 = ~6.4MB per partition

**Problem**: Many small shuffle partitions (6.4MB << 64MB target)

**Stage 2: With Adaptive Input**
- Learned ratio: 128GB / 6.4GB = 20:1
- Adjusted input: 128MB × 10 = 1.28GB
- Result: 1.28GB → 64MB after filter (optimal!)

**Benefit**: 10x fewer partitions, reduced scheduling overhead.

---

<div style="page-break-after: always;"></div>

### Use Case 3: Iterative ML Workloads

**Scenario**: K-means clustering with multiple iterations

```python
# K-means with 10 iterations
for i in range(10):
    df = spark.read.parquet("/data/features")

    # First iteration: learns compression ratio
    # Iterations 2-10: use adjusted partition sizes
    centroids = df.groupBy(...).agg(...)
    distances = df.crossJoin(centroids).select(...)
```

**Behavior**:
- **Iteration 1**: Uses default 128MB partitions, learns ratio
- **Iteration 2+**: Automatically adjusts to optimal size
- **No manual tuning required**

**Benefit**: Self-optimizing workloads, better developer experience.

---

<div style="page-break-after: always;"></div>

## 5. Limitations and Edge Cases

### 5.1 When It Doesn't Help

#### 1. Single-Stage Queries

```sql
-- No optimization possible
SELECT * FROM parquet_table LIMIT 100
```

**Reason**: Only one stage, no prior shuffle statistics available.

**Workaround**: Feature gracefully degrades to default behavior.

---

#### 2. Heterogeneous Data

Files with vastly different compression ratios:
- `part-001.parquet`: 10:1 compression
- `part-002.parquet`: 2:1 compression

**Mitigation**: Use **median** instead of **mean** for ratio calculation.

**Future Enhancement**: Per-file ratio tracking.

---

#### 3. First Query on New Table

**Reason**: No historical statistics for brand new tables.

**Mitigation**:
- Use default partitioning for first query
- Learn and apply to subsequent stages within same query
- Optional: Store learned ratios in table properties

---

### 5.2 Risk Factors

#### 1. Memory Pressure

**Risk**: Larger input partitions require more memory

**Mitigation**:
- Enforce `maxPartitionBytes = 1GB` upper bound
- Monitor executor memory usage
- Add memory-aware adjustment in future

**Example**:
```
Before: 128MB input → 256MB in memory
After:  1GB input → 2GB in memory (8x increase!)
```

Solution: Configure executor memory appropriately.

---

#### 2. Over-Correction from Outliers

**Risk**: Single stage with extreme ratio skews all adjustments

**Mitigation**:
- Use **median** partition size (robust to outliers)
- Require **minimum sample size** (3 stages)
- Apply **max scale factor** (10x limit)
- Use **rolling window** (recent 3 stages only)

**Example**:
```
Stage 1: 1000:1 ratio (outlier)
Stage 2: 5:1 ratio (normal)
Stage 3: 6:1 ratio (normal)

Median: 6:1 (outlier ignored) ✓
Mean:   337:1 (skewed by outlier) ✗
```

---

#### 3. Non-Uniform Distribution

**Risk**: Some partitions compress well, others don't

**Example**:
```
Partition 1: Text data → 10:1 compression
Partition 2: Binary data → 1.1:1 compression
```

**Mitigation**:
- Per-partition ratio calculation (future)
- Skew detection similar to `OptimizeSkewedJoin`
- Fall back to default if coefficient of variation is high

---

#### 4. Changing Filter Selectivity

**Risk**: Filter selectivity varies across stages

**Example**:
```
Stage 1: WHERE country = 'US' → 5% selectivity
Stage 2: WHERE country = 'CN' → 20% selectivity
```

**Mitigation**:
- Use rolling window (default 3 stages)
- Decay older observations
- Re-adjust if ratio changes significantly

---

<div style="page-break-after: always;"></div>

### 5.3 Edge Cases

#### Empty Shuffle Partitions

```scala
// Handle all-zero partition sizes
if (shuffleBytesWritten == 0) {
  return currentMaxSplit  // No adjustment
}
```

**Scenario**: Filter eliminates all data
**Action**: Skip adjustment

---

#### Very Small Files

```scala
// Don't adjust if total file size < adjusted split
if (totalFileSize < adjustedMaxSplit) {
  return currentMaxSplit  // Keep original
}
```

**Scenario**: Small files (10MB total), adjusted split (1GB)
**Action**: Use original partitioning

---

#### Broadcast Joins

```scala
// Skip stages without shuffle
if (!stage.hasShuffle) {
  return currentMaxSplit  // No statistics available
}
```

**Scenario**: Broadcast join produces no shuffle statistics
**Action**: No adjustment possible

---

#### Zero Input Bytes

```scala
// Handle cached/materialized data with no input
if (inputBytesRead == 0) {
  return 1.0  // Default ratio
}
```

**Scenario**: Reading from cached DataFrame
**Action**: Use default ratio (1.0)

---

<div style="page-break-after: always;"></div>

## 6. Implementation Phases

### Phase 1: Foundation (Week 1)

**Tasks**:
- [ ] Add 6 configuration parameters to `SQLConf.scala`
- [ ] Create `AdaptiveInputStatisticsTracker.scala`
- [ ] Implement statistics tracking logic
- [ ] Add unit tests for tracker
- [ ] Run scalastyle, fix violations
- [ ] First commit: Configuration + tracker

**Deliverable**: Statistics tracking infrastructure

---

### Phase 2: Core Logic (Week 2)

**Tasks**:
- [ ] Create `AdaptiveInputPartitioning.scala` rule
- [ ] Implement adjustment calculation logic
- [ ] Add `maxSplitBytesOverride` to `FileSourceScanExec`
- [ ] Unit tests for ratio calculation
- [ ] Unit tests for safety bounds
- [ ] Edge case handling tests
- [ ] Run scalastyle, fix violations
- [ ] Second commit: Core partitioning logic

**Deliverable**: Adjustment logic with safety bounds

---

### Phase 3: AQE Integration (Week 3)

**Tasks**:
- [ ] Integrate tracker into `AdaptiveSparkPlanExec`
- [ ] Modify `AQEOptimizer` to include new rule
- [ ] Add metrics collection
- [ ] Hook up `onStageSuccess` callback
- [ ] Integration tests with multi-stage queries
- [ ] Verify metrics appear in Spark UI
- [ ] Run scalastyle, fix violations
- [ ] Third commit: AQE integration

**Deliverable**: Full AQE integration

---

### Phase 4: Testing & Validation (Week 4)

**Tasks**:
- [ ] End-to-end tests with Parquet files
- [ ] End-to-end tests with ORC files
- [ ] Tests with compressed data (Snappy, LZO, Gzip)
- [ ] Tests with selective filters
- [ ] TPC-DS benchmark queries (Q3, Q7, Q42)
- [ ] Memory pressure tests
- [ ] Edge case validation
- [ ] Documentation: configuration guide
- [ ] Documentation: migration guide
- [ ] Fourth commit: Tests + docs

**Deliverable**: Comprehensive test coverage

---

### Phase 5: Tuning & Refinement (Week 5)

**Tasks**:
- [ ] Analyze TPC-DS benchmark results
- [ ] Tune default configuration values
- [ ] Add additional safety checks if needed
- [ ] Performance profiling
- [ ] Reduce overhead in hot paths
- [ ] Code review with team
- [ ] Address review feedback
- [ ] Final commit: Tuning + refinements

**Deliverable**: Production-ready implementation

---

<div style="page-break-after: always;"></div>

## 7. Testing Strategy

### 7.1 Unit Tests

**File**: `AdaptiveInputPartitioningSuite.scala`

#### Test 1: Calculate Compression Ratio

```scala
test("calculate compression ratio from shuffle statistics") {
  val tracker = new AdaptiveInputStatisticsTracker()
  val mapStats = new MapOutputStatistics(0,
    Array(10L, 12L, 11L))

  tracker.recordStageStats(0,
    inputBytesRead = 1000L,
    mapStats)

  assert(tracker.getRecentCompressionRatio() ===
    Some(1000.0 / 33.0))
}
```

---

#### Test 2: Adjust Based on Ratio

```scala
test("adjust maxSplitBytes based on compression ratio") {
  withSQLConf(
    SQLConf.ADAPTIVE_INPUT_PARTITIONING_ENABLED.key -> "true",
    SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "64MB") {

    // Setup query with known 10:1 ratio
    // Verify adjustment: 128MB → 1.28GB
  }
}
```

---

#### Test 3: Respect Maximum Scale Factor

```scala
test("respect maximum scale factor") {
  withSQLConf(
    SQLConf.ADAPTIVE_INPUT_PARTITIONING_MAX_SCALE_FACTOR.key
      -> "5.0") {

    // Compression ratio: 100:1 (extreme)
    // Max adjustment: 128MB × 5 = 640MB (capped)
  }
}
```

---

#### Test 4: Safety Bounds

```scala
test("respect min and max partition bytes") {
  withSQLConf(
    SQLConf.ADAPTIVE_INPUT_PARTITIONING_MIN_BYTES.key -> "32MB",
    SQLConf.ADAPTIVE_INPUT_PARTITIONING_MAX_BYTES.key -> "512MB") {

    // Test lower bound: ratio would suggest 10MB → use 32MB
    // Test upper bound: ratio would suggest 2GB → use 512MB
  }
}
```

---

#### Test 5: Minimum Ratio Threshold

```scala
test("skip adjustment when compression ratio too low") {
  withSQLConf(
    SQLConf.ADAPTIVE_INPUT_PARTITIONING_MIN_RATIO.key -> "2.0") {

    // Ratio: 1.5:1 (below threshold)
    // Expected: No adjustment
  }
}
```

---

#### Test 6: Empty Shuffle Partitions

```scala
test("handle empty shuffle partitions") {
  val tracker = new AdaptiveInputStatisticsTracker()
  val mapStats = new MapOutputStatistics(0,
    Array(0L, 0L, 0L))  // All empty

  tracker.recordStageStats(0,
    inputBytesRead = 1000L,
    mapStats)

  // Should return None (no valid ratio)
  assert(tracker.getRecentCompressionRatio() === None)
}
```

---

<div style="page-break-after: always;"></div>

### 7.2 Integration Tests

#### Test 1: Multi-Stage Query

```scala
test("adaptive input partitioning with multi-stage query") {
  withSQLConf(
    SQLConf.ADAPTIVE_INPUT_PARTITIONING_ENABLED.key -> "true") {

    withTempPath { path =>
      // Create compressed Parquet with known ratio
      spark.range(10000000)
        .selectExpr("id", "id % 100 as group")
        .write
        .option("compression", "snappy")
        .parquet(path.getAbsolutePath)

      // Multi-stage query
      val df = spark.read.parquet(path.getAbsolutePath)
        .filter("group = 1")  // High selectivity
        .groupBy("group")
        .count()

      val result = df.collect()

      // Verify adjustment occurred
      val metrics = df.queryExecution.executedPlan.metrics
      assert(metrics("numInputPartitionAdjustments").value > 0)

      // Verify compression ratio tracked
      assert(metrics("avgCompressionRatio").value > 1.0)
    }
  }
}
```

---

#### Test 2: Iterative Workload

```scala
test("adaptive adjustment across multiple scans") {
  withSQLConf(
    SQLConf.ADAPTIVE_INPUT_PARTITIONING_ENABLED.key -> "true") {

    withTempPath { path =>
      // Create test data
      spark.range(1000000).write.parquet(path.getAbsolutePath)

      // Simulate iterative ML workload
      var prevPartitionSize = 0L

      for (i <- 1 to 5) {
        val df = spark.read.parquet(path.getAbsolutePath)
          .filter(s"id % 100 < $i")
          .groupBy("id")
          .count()

        df.collect()

        val currentSize =
          df.queryExecution.executedPlan.metrics(
            "inputPartitionSizeAfter").value

        if (i > 1) {
          // Should stabilize after first iteration
          assert(currentSize > 0)
        }

        prevPartitionSize = currentSize
      }
    }
  }
}
```

---

<div style="page-break-after: always;"></div>

### 7.3 Performance Benchmarks

#### TPC-DS Queries

Test with queries known to benefit from adaptive partitioning:

**Query 3**: Highly selective filters
```sql
SELECT dt.d_year, item.i_brand_id, item.i_brand,
       SUM(ss_ext_sales_price) sum_agg
FROM date_dim dt, store_sales, item
WHERE dt.d_date_sk = store_sales.ss_sold_date_sk
  AND store_sales.ss_item_sk = item.i_item_sk
  AND item.i_manufact_id = 128
  AND dt.d_moy = 11
GROUP BY dt.d_year, item.i_brand, item.i_brand_id
ORDER BY dt.d_year, sum_agg DESC, item.i_brand_id
LIMIT 100
```

**Expected Improvement**: 15-30% faster (fewer small partitions)

---

**Query 7**: Multi-stage aggregations
```sql
SELECT i_item_id,
       AVG(ss_quantity) agg1,
       AVG(ss_list_price) agg2,
       AVG(ss_coupon_amt) agg3,
       AVG(ss_sales_price) agg4
FROM store_sales, customer_demographics, date_dim, item, promotion
WHERE ss_sold_date_sk = d_date_sk
  AND ss_item_sk = i_item_sk
  AND ss_cdemo_sk = cd_demo_sk
  AND ss_promo_sk = p_promo_sk
  AND cd_gender = 'M'
  AND cd_marital_status = 'S'
  AND cd_education_status = 'College'
  AND (p_channel_email = 'N' OR p_channel_event = 'N')
  AND d_year = 2000
GROUP BY i_item_id
ORDER BY i_item_id
LIMIT 100
```

**Expected Improvement**: 10-25% faster (optimal partition sizes)

---

#### Metrics to Track

| Metric | Baseline | With Adaptive | Target |
|--------|----------|---------------|--------|
| Query Time | 100s | ? | <85s (15% improvement) |
| # of Tasks | 1000 | ? | <500 (fewer small tasks) |
| Avg Task Time | 5s | ? | 8-10s (optimal size) |
| Shuffle Bytes | 10GB | 10GB | Same (no data change) |
| Memory Usage | 50GB | ? | <60GB (acceptable) |

---

#### Test Methodology

1. **Baseline**: Run with feature disabled
2. **Adaptive**: Run with feature enabled
3. **Compare**: Statistical significance (t-test, p<0.05)
4. **Repeat**: 5 runs each, take median
5. **Report**: Mean ± std dev, p-value

---

<div style="page-break-after: always;"></div>

## 8. Alternatives Considered

### Alternative 1: Static Table Statistics

**Approach**: Store compression ratios in table metadata (Hive metastore, Glue catalog)

```sql
ALTER TABLE events SET TBLPROPERTIES (
  'spark.stats.avgCompressionRatio' = '10.5',
  'spark.stats.lastUpdated' = '2025-10-27'
);
```

**Pros**:
- Available at planning time (no runtime overhead)
- Works for first query on table
- Simple implementation

**Cons**:
- Requires statistics collection job
- May become stale as data changes
- Doesn't handle query-specific filters
- Requires metastore schema changes

**Decision**: ❌ **Rejected**
Runtime adaptation is more accurate for query-specific selectivity and filter predicates.

---

### Alternative 2: Sample-Based Estimation

**Approach**: Read small sample (1%) of data first to estimate compression ratio

```scala
// Read 1% sample
val sample = spark.read.parquet(path)
  .sample(withReplacement = false, fraction = 0.01)

val estimatedRatio =
  sample.inputBytes / sample.outputBytes

// Use estimated ratio for full scan
val adjusted = adjustPartitionSize(estimatedRatio)
```

**Pros**:
- Works for single-stage queries
- Available before first stage
- No metastore changes needed

**Cons**:
- Additional I/O overhead (1-5%)
- Sampling may not be representative
- Doesn't account for filter selectivity
- Adds query latency

**Decision**: ❌ **Rejected for initial version**
Could be future enhancement, but AQE-based approach is simpler and more accurate for multi-stage queries.

---

### Alternative 3: Per-File Metadata

**Approach**: Store compression ratio per file in catalog or sidecar files

```json
{
  "file": "s3://bucket/table/part-001.parquet",
  "compressedSize": 128000000,
  "uncompressedSize": 1280000000,
  "compressionRatio": 10.0,
  "recordCount": 1000000
}
```

**Pros**:
- Most granular approach
- Handles heterogeneous data well
- Accurate per-file estimates

**Cons**:
- High metadata overhead (millions of files)
- Requires background statistics collection
- Doesn't account for filters
- Complex metadata management

**Decision**: ❌ **Rejected**
Too complex for initial implementation. Per-partition adjustment could be future work.

---

### Why AQE-Based Approach Won

✅ **Advantages**:
1. **Zero configuration**: Works automatically
2. **Query-specific**: Accounts for actual filter selectivity
3. **Self-correcting**: Adapts if data characteristics change
4. **No I/O overhead**: Uses existing shuffle statistics
5. **Proven framework**: Builds on mature AQE infrastructure

✅ **Selected for implementation**

---

<div style="page-break-after: always;"></div>

## 9. Future Enhancements

### 9.1 Persistent Learning

Store learned compression ratios in catalog for future queries:

```sql
-- Automatic statistics collection
ANALYZE TABLE events
  COMPUTE STATISTICS FOR COMPRESSION;

-- View statistics
DESCRIBE EXTENDED events;
-- Output:
-- Table Properties:
--   spark.stats.avgCompressionRatio=10.5
--   spark.stats.compressionMethod=snappy
--   spark.stats.lastUpdated=2025-10-27
```

**Benefits**:
- First query on table can use historical ratios
- Cross-session optimization
- Reduced "learning" overhead

**Implementation**: Phase 2 feature

---

### 9.2 Per-Partition Adjustment

Instead of uniform adjustment, vary partition size based on skew:

```scala
case class AdaptiveFilePartition(
    index: Int,
    files: Array[PartitionedFile],
    adjustedMaxSplit: Long,  // ← Per-partition value
    estimatedCompressionRatio: Double)

// Different sizes for different partitions
Partition 0: 128MB (low compression)
Partition 1: 1GB (high compression)
Partition 2: 512MB (medium compression)
```

**Benefits**:
- Handles heterogeneous data better
- More consistent output partition sizes
- Better load balancing

**Implementation**: Phase 3 feature

---

### 9.3 ML-Based Prediction

Use machine learning to predict optimal partition sizes:

```scala
class PartitionSizePredictor {
  def predict(
      tableId: String,
      filterPredicates: Seq[Expression],
      fileFormat: FileFormat,
      compressionCodec: String,
      historicalStats: Seq[QueryStats]): Long = {

    // Features:
    // - Table ID (categorical)
    // - Filter selectivity estimate
    // - File format
    // - Compression codec
    // - Historical avg ratio

    // Model: Gradient Boosting Regressor
    // Trained on query history
  }
}
```

**Benefits**:
- Even better predictions
- Learns complex patterns
- Accounts for multiple factors

**Challenges**:
- Requires training data
- Model deployment complexity
- Explainability concerns

**Implementation**: Research project (Phase 4+)

---

### 9.4 Cross-Query Optimization

Share statistics across queries in same session:

```scala
// Session-level cache
class AdaptiveInputStatsCache {
  private val cache =
    ConcurrentHashMap[TableIdentifier, CompressionStats]()

  def getOrUpdate(
      table: TableIdentifier,
      compute: => CompressionStats): CompressionStats = {
    cache.computeIfAbsent(table, _ => compute)
  }
}

// Usage
sparkSession.sharedState.adaptiveInputStatsCache
  .getOrUpdate(tableId, computeStats())
```

**Benefits**:
- First query in session benefits
- Reduced learning overhead
- Better interactive experience

**Implementation**: Phase 2 feature

---

### 9.5 Memory-Aware Adjustment

Adjust partition sizes based on available executor memory:

```scala
def computeMemoryAwareMaxSplit(
    desiredMaxSplit: Long,
    executorMemory: Long,
    memoryOverhead: Double): Long = {

  val availableMemory = executorMemory * (1 - memoryOverhead)
  val maxSafePartition = (availableMemory * 0.5).toLong

  Math.min(desiredMaxSplit, maxSafePartition)
}
```

**Benefits**:
- Prevents OOM errors
- Better memory utilization
- Safer in production

**Implementation**: Phase 2 feature

---

<div style="page-break-after: always;"></div>

## 10. Open Questions

### Question 1: Non-Shuffle Operations

**Q**: Should we adjust input partitioning for non-shuffle operations (e.g., broadcast joins)?

**Considerations**:
- Broadcast joins don't produce shuffle statistics
- Could track input/output ratios differently
- May not be as beneficial

**Decision**: ❌ **No for initial version**
Focus on shuffle-heavy workloads where benefit is clearest.

---

### Question 2: Skewed Compression Ratios

**Q**: How to handle skewed compression ratios across partitions?

**Example**:
```
Partition 1: Text → 10:1 compression
Partition 2: JSON → 5:1 compression
Partition 3: Binary → 1.1:1 compression
```

**Options**:
1. Use median (current approach)
2. Per-partition adjustment (future)
3. Fall back if coefficient of variation > threshold

**Decision**: ✅ **Use median for now**
Add coefficient of variation check in Phase 2.

---

### Question 3: Per-Table vs Per-Query

**Q**: Should adjustment be per-table or per-query?

**Per-Table**:
- Store in catalog
- Reuse across queries
- May be stale

**Per-Query**:
- Fresh statistics
- Query-specific (filters!)
- No persistent storage

**Decision**: ✅ **Per-query for initial version**
Add per-table statistics as future enhancement.

---

### Question 4: Mid-Query Changes

**Q**: What if compression ratio changes significantly mid-query?

**Example**:
```
Stage 1: Ratio = 5:1 → Adjust to 640MB
Stage 2: Ratio = 15:1 → Should re-adjust?
```

**Options**:
1. Keep adjustment (stable)
2. Re-adjust if > 2x change (adaptive)
3. Use exponential moving average (smooth)

**Decision**: ✅ **Use rolling window (3 stages)**
Naturally adapts to changes without explicit re-adjustment logic.

---

### Question 5: Default Enabled?

**Q**: Should feature be enabled by default in production?

**Pros**:
- Users benefit automatically
- Better out-of-box performance

**Cons**:
- Risk of regressions
- Unexpected memory usage
- Hard to debug initially

**Decision**: ❌ **Disabled by default (opt-in)**
Enable by default after 2-3 releases with proven stability.

---

<div style="page-break-after: always;"></div>

## 11. References

### Existing Code

**Core File Partitioning**:
- `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FilePartition.scala`
- `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/PartitionedFileUtil.scala`
- `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileScanRDD.scala`

**AQE Framework**:
- `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveSparkPlanExec.scala`
- `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AQEOptimizer.scala`
- `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/QueryStageExec.scala`

**Existing AQE Rules**:
- `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/CoalesceShufflePartitions.scala`
- `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/OptimizeSkewedJoin.scala`
- `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/DynamicJoinSelection.scala`

**Statistics**:
- `core/src/main/scala/org/apache/spark/MapOutputStatistics.scala`
- `sql/core/src/main/scala/org/apache/spark/sql/execution/metric/SQLMetrics.scala`

---

### Related JIRAs

- **SPARK-XXXXX**: Adaptive Input Partitioning (to be created)
- **SPARK-31412**: Adaptive Query Execution framework
- **SPARK-29544**: Optimize skewed join with AQE
- **SPARK-30291**: Coalesce shuffle partitions based on stats

---

### Related Configurations

**Existing Configurations**:
- `spark.sql.files.maxPartitionBytes` - Default: 128MB
- `spark.sql.adaptive.advisoryPartitionSizeInBytes` - Default: 64MB
- `spark.sql.adaptive.coalescePartitions.minPartitionSize` - Default: 1MB
- `spark.sql.adaptive.enabled` - Default: true (since Spark 3.2)

**New Configurations** (this feature):
- `spark.sql.adaptive.inputPartitioning.enabled` - Default: false
- `spark.sql.adaptive.inputPartitioning.minCompressionRatio` - Default: 1.5
- `spark.sql.adaptive.inputPartitioning.minPartitionBytes` - Default: 64MB
- `spark.sql.adaptive.inputPartitioning.maxPartitionBytes` - Default: 1GB
- `spark.sql.adaptive.inputPartitioning.maxScaleFactor` - Default: 10.0
- `spark.sql.adaptive.inputPartitioning.lookbackStages` - Default: 3

---

### External References

**Academic Papers**:
- "Adaptive Query Processing" - Deshpande et al., 2007
- "Eddies: Continuously Adaptive Query Processing" - Avnur & Hellerstein, 2000

**Industry Blog Posts**:
- "Adaptive Query Execution in Spark 3.0" - Databricks Blog
- "Understanding Spark Partition Sizing" - Netflix Tech Blog

**Documentation**:
- Apache Spark SQL Performance Tuning Guide
- AQE Design Document (internal)

---

<div style="page-break-after: always;"></div>

## Appendix A: Configuration Quick Reference

### Enable Feature

```python
spark.conf.set(
  "spark.sql.adaptive.inputPartitioning.enabled",
  "true")
```

---

### Full Configuration Example

```python
# Enable adaptive input partitioning
spark.conf.set(
  "spark.sql.adaptive.inputPartitioning.enabled", "true")

# Set target shuffle partition size (64MB default)
spark.conf.set(
  "spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")

# Minimum compression ratio to trigger adjustment
spark.conf.set(
  "spark.sql.adaptive.inputPartitioning.minCompressionRatio", "2.0")

# Partition size bounds
spark.conf.set(
  "spark.sql.adaptive.inputPartitioning.minPartitionBytes", "32MB")
spark.conf.set(
  "spark.sql.adaptive.inputPartitioning.maxPartitionBytes", "512MB")

# Maximum adjustment factor
spark.conf.set(
  "spark.sql.adaptive.inputPartitioning.maxScaleFactor", "5.0")

# Rolling window size
spark.conf.set(
  "spark.sql.adaptive.inputPartitioning.lookbackStages", "5")
```

---

### Verify Feature is Working

```python
# Run query
df = spark.read.parquet("s3://data/events") \
  .filter("event_type = 'click'") \
  .groupBy("user_id") \
  .count()

result = df.collect()

# Check metrics
plan = df.queryExecution.executedPlan
metrics = plan.metrics

print(f"Adjustments: {metrics['numInputPartitionAdjustments'].value}")
print(f"Avg Ratio: {metrics['avgCompressionRatio'].value}")
print(f"Size Before: {metrics['inputPartitionSizeBefore'].value}")
print(f"Size After: {metrics['inputPartitionSizeAfter'].value}")
```

---

<div style="page-break-after: always;"></div>

## Appendix B: Conversion to Kindle Format

### Using Pandoc

```bash
# Install pandoc
brew install pandoc  # macOS
sudo apt-get install pandoc  # Linux

# Convert to EPUB
pandoc adaptive-input-partitioning-design-kindle.md \
  -o adaptive-input-partitioning.epub \
  --toc \
  --toc-depth=3 \
  --metadata title="Adaptive Input Partitioning" \
  --metadata author="Spark Team"

# Convert EPUB to MOBI using Kindle Previewer or Calibre
```

---

### Using Calibre

```bash
# Install Calibre
brew install --cask calibre  # macOS

# Convert to MOBI
ebook-convert \
  adaptive-input-partitioning-design-kindle.md \
  adaptive-input-partitioning.mobi \
  --title "Adaptive Input Partitioning" \
  --authors "Spark Team" \
  --book-producer "Apache Spark" \
  --language en \
  --enable-heuristics
```

---

### Send to Kindle Email

1. Convert to MOBI or EPUB format (above)
2. Email to your Kindle email address (find in Amazon account)
3. Subject: "Convert" (for EPUB files)
4. Attachment: The .mobi or .epub file

**Your Kindle Email**: `yourname@kindle.com`
**Send From**: Approved email (add in Amazon Kindle settings)

---

### HTML Version

For better formatting, use HTML:

```bash
# Convert to HTML
pandoc adaptive-input-partitioning-design-kindle.md \
  -o adaptive-input-partitioning.html \
  --toc \
  --standalone \
  --css=kindle.css
```

Then use Calibre to convert HTML → MOBI with better control over styling.

---

**End of Document**

---

**Document Version**: 1.0 (Kindle Edition)
**Last Updated**: 2025-10-27
**Format**: Markdown (Kindle-optimized)
**Author**: Adaptive Input Partitioning Feature Team
**License**: Apache License 2.0
