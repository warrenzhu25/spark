# Adaptive Input Partitioning Design Document

## 1. Overview

### Problem Statement
Currently, `spark.sql.files.maxPartitionBytes` is a static configuration (default 128MB) that determines how much data each input partition reads from files. This static approach has limitations:

1. **Compression Ratio Unknown**: When reading compressed files (Parquet, ORC), we don't know the output size until after reading
2. **Filter Selectivity Unknown**: When applying filters, we don't know how much data will survive filtering
3. **Suboptimal Shuffle Sizes**: Input partitions of 128MB might produce shuffle partitions of 10MB (too small, overhead) or 500MB (too large, memory pressure)

### Goal
Dynamically adjust input partition sizes based on observed compression ratios and filter selectivity from earlier query stages to achieve optimal shuffle partition sizes.

### Example Scenario
```
Current Behavior:
  Task 1: Read 128MB input → Apply filter → Write 10MB shuffle (12.8:1 ratio)
  Task 2: Read 128MB input → Apply filter → Write 12MB shuffle
  Task 3: Read 128MB input → Apply filter → Write 9MB shuffle
  Result: Many small shuffle partitions (10-12MB), high overhead

Desired Behavior with Adaptive Input:
  Stage 1: Tasks read 128MB → Learn median ratio is ~12:1
  Stage 2: Adjust input to 1280MB (128MB * 10) to target ~100MB shuffle output
  Result: Optimal shuffle partition sizes
```

## 2. Current Implementation Analysis

### 2.1 File Partition Assignment Flow

**Step 1: Calculate maxSplitBytes** (`FilePartition.scala:118`)
```scala
def maxSplitBytes(sparkSession: SparkSession, calculateTotalBytes: => Long): Long = {
  val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes  // 128MB
  val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes        // 4MB
  val minPartitionNum = sparkSession.sessionState.conf.filesMinPartitionNum
    .getOrElse(sparkSession.leafNodeDefaultParallelism)
  val totalBytes = calculateTotalBytes
  val bytesPerCore = totalBytes / minPartitionNum

  Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
}
```

**Step 2: Split Files** (`PartitionedFileUtil.scala:27`)
```scala
def splitFiles(
    file: FileStatusWithMetadata,
    filePath: Path,
    isSplitable: Boolean,
    maxSplitBytes: Long,  // ← From step 1
    partitionValues: InternalRow): Seq[PartitionedFile] = {

  if (isSplitable) {
    // Create multiple PartitionedFile chunks
    (0L until file.getLen by maxSplitBytes).map { offset =>
      val size = Math.min(maxSplitBytes, file.getLen - offset)
      PartitionedFile(partitionValues, filePath, offset, size, locations)
    }
  } else {
    Seq(PartitionedFile(partitionValues, filePath, 0, file.getLen, locations))
  }
}
```

**Step 3: Pack into Partitions** (`FilePartition.scala:58`)
```scala
private def getFilePartitions(
    partitionedFiles: Seq[PartitionedFile],
    maxSplitBytes: Long,
    openCostInBytes: Long): Seq[FilePartition] = {

  val partitions = new ArrayBuffer[FilePartition]
  val currentFiles = new ArrayBuffer[PartitionedFile]
  var currentSize = 0L

  // "Next Fit Decreasing" algorithm
  partitionedFiles.foreach { file =>
    if (currentSize + file.length > maxSplitBytes) {
      closePartition()  // Create new partition
    }
    currentSize += file.length + openCostInBytes
    currentFiles += file
  }

  partitions.toSeq
}
```

**Step 4: Task Reads Assigned Files** (`FileScanRDD.scala:111`)
```scala
override def compute(split: RDDPartition, context: TaskContext): Iterator[InternalRow] = {
  val files = split.asInstanceOf[FilePartition].files.iterator

  files.map { partitionedFile =>
    // Each PartitionedFile specifies:
    //   - filePath: which file
    //   - start: byte offset to start reading
    //   - length: number of bytes to read
    readFunction(partitionedFile)
  }
}
```

**Key Observation**: `maxSplitBytes` is calculated **at planning time** before any data is read. We cannot use runtime statistics with the current architecture.

### 2.2 Adaptive Query Execution (AQE) Infrastructure

AQE already supports runtime optimizations based on shuffle statistics:

**Shuffle Statistics Collection** (`ShuffleQueryStageExec.scala`)
```scala
case class ShuffleQueryStageExec(...) extends QueryStageExec {
  var mapStats: Option[MapOutputStatistics] = None

  override def doMaterialize(): Future[MapOutputStatistics] = {
    sparkContext.submitMapStage(shuffleDependency)
  }
}
```

**MapOutputStatistics Structure** (`MapOutputStatistics.scala`)
```scala
class MapOutputStatistics(
    val shuffleId: Int,
    val bytesByPartitionId: Array[Long])  // ← Size of each shuffle partition
```

**Existing AQE Optimizations**:
1. `CoalesceShufflePartitions` - Combines small partitions based on `bytesByPartitionId`
2. `OptimizeSkewedJoin` - Splits large partitions based on `bytesByPartitionId`
3. `DynamicJoinSelection` - Switches join strategies based on partition sizes
4. `OptimizeShuffleWithLocalRead` - Enables local reads based on partition layout

**Integration Point**: After a shuffle stage completes, AQE re-optimizes the remaining plan:

```scala
// In AdaptiveSparkPlanExec.scala:356
private def reOptimize(logicalPlan: LogicalPlan): SparkPlan = {
  logicalPlan.invalidateStatsCache()
  val optimized = optimizer.execute(logicalPlan)
  // optimizer contains all AQE rules including our new one
  planner.plan(optimized).next()
}
```

## 3. Proposed Solution

### 3.1 Architecture

**Add new AQE rule**: `AdaptiveInputPartitioning`

**When it runs**:
- After a shuffle stage completes
- Before planning subsequent file scan stages
- Within the AQE re-optimization loop

**What it does**:
1. Examines completed shuffle stages and collects `MapOutputStatistics`
2. Calculates compression/selectivity ratio: `inputBytes / shuffleOutputBytes`
3. Computes adjusted `maxSplitBytes` to target optimal shuffle size
4. Injects adjusted value into file scan planning

**Flow Diagram**:
```
┌─────────────────────────────────────────────────────────────────┐
│ Stage 1: File Scan → Filter → Shuffle                          │
│   Input: 128MB partitions × 100 tasks = 12.8GB                 │
│   Output: 10MB shuffle partitions × 100 = 1GB                  │
│   Ratio: 12.8:1 compression/filtering                          │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│ AQE Re-optimization (AdaptiveInputPartitioning rule)           │
│   Observes: median shuffle partition = 10MB                    │
│   Target: 64MB shuffle partitions (ADVISORY_PARTITION_SIZE)    │
│   Calculates: new maxSplitBytes = 128MB × (64MB/10MB) ≈ 819MB  │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│ Stage 2: File Scan (with adjusted partitioning)                │
│   Input: 819MB partitions × 16 tasks = 12.8GB (same total)     │
│   Output: 64MB shuffle partitions × 16 (optimal size)          │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Detailed Design

#### Component 1: Statistics Tracker

**Purpose**: Track input/output bytes for each stage to calculate compression ratio

**Location**: New class in `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/`

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

class AdaptiveInputStatisticsTracker {
  private val stageStats = mutable.HashMap[Int, StageCompressionStats]()

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

  def getRecentCompressionRatio(lookbackStages: Int = 3): Option[Double] = {
    val recentStats = stageStats.values.toSeq.takeRight(lookbackStages)
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

#### Component 2: Adaptive Partitioning Rule

**Location**: `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveInputPartitioning.scala`

```scala
case class AdaptiveInputPartitioning(
    session: SparkSession,
    statsTracker: AdaptiveInputStatisticsTracker) extends Rule[SparkPlan] {

  private def conf = session.sessionState.conf

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.adaptiveInputPartitioningEnabled) {
      return plan
    }

    plan.transformUp {
      case scan: FileSourceScanExec =>
        adjustFileScan(scan)
      case scan: BatchScanExec if scan.scan.isInstanceOf[FileScan] =>
        adjustBatchScan(scan)
    }
  }

  private def adjustFileScan(scan: FileSourceScanExec): FileSourceScanExec = {
    statsTracker.getRecentCompressionRatio() match {
      case Some(ratio) if ratio > conf.adaptiveInputPartitioningMinRatio =>
        val currentMaxSplit = scan.maxSplitBytes
        val adjustedMaxSplit = computeAdjustedMaxSplit(currentMaxSplit, ratio)

        if (adjustedMaxSplit != currentMaxSplit) {
          logInfo(s"Adjusting maxSplitBytes from $currentMaxSplit to $adjustedMaxSplit " +
            s"based on compression ratio $ratio")
          scan.copy(maxSplitBytesOverride = Some(adjustedMaxSplit))
        } else {
          scan
        }
      case _ =>
        scan
    }
  }

  private def computeAdjustedMaxSplit(
      currentMaxSplit: Long,
      compressionRatio: Double): Long = {

    val targetShuffleSize = conf.advisoryPartitionSizeInBytes
    val currentShuffleSize = currentMaxSplit / compressionRatio

    if (currentShuffleSize > 0) {
      val scaleFactor = targetShuffleSize / currentShuffleSize
      val adjustedMaxSplit = (currentMaxSplit * scaleFactor).toLong

      // Apply safety bounds
      val minAllowed = conf.adaptiveInputPartitioningMinBytes
      val maxAllowed = conf.adaptiveInputPartitioningMaxBytes
      val maxScaleFactor = conf.adaptiveInputPartitioningMaxScaleFactor

      val boundedAdjustment = adjustedMaxSplit
        .max(minAllowed)
        .min(maxAllowed)
        .min((currentMaxSplit * maxScaleFactor).toLong)

      boundedAdjustment
    } else {
      currentMaxSplit
    }
  }
}
```

#### Component 3: FileSourceScanExec Modification

**Location**: Modify `sql/core/src/main/scala/org/apache/spark/sql/execution/DataSourceScanExec.scala`

```scala
case class FileSourceScanExec(
    // ... existing parameters ...
    maxSplitBytesOverride: Option[Long] = None  // ← New parameter
) extends DataSourceScanExec {

  // Modify the maxSplitBytes calculation
  private lazy val maxSplitBytes: Long = {
    maxSplitBytesOverride.getOrElse {
      FilePartition.maxSplitBytes(sparkSession, selectedPartitions)
    }
  }

  // Rest of implementation remains the same
}
```

#### Component 4: Integration with AQE

**Location**: Modify `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveSparkPlanExec.scala`

```scala
class AdaptiveSparkPlanExec(...) {

  private val inputStatsTracker = new AdaptiveInputStatisticsTracker()

  // When stage completes, record statistics
  private def onStageSuccess(stage: QueryStageExec): Unit = {
    stage match {
      case shuffleStage: ShuffleQueryStageExec if shuffleStage.mapStats.isDefined =>
        val mapStats = shuffleStage.mapStats.get
        val inputMetrics = shuffleStage.plan.metrics.get("inputMetrics")

        inputStatsTracker.recordStageStats(
          stageId = shuffleStage.id,
          inputMetrics = inputMetrics,
          mapStats = mapStats
        )
      case _ => // non-shuffle stages
    }
  }

  // Pass tracker to AQE optimizer
  private def optimizer: AQEOptimizer = {
    new AQEOptimizer(
      conf,
      inputStatsTracker  // ← Pass tracker
    )
  }
}

// Modify AQEOptimizer to include the new rule
class AQEOptimizer(
    conf: SQLConf,
    statsTracker: AdaptiveInputStatisticsTracker) extends RuleExecutor[LogicalPlan] {

  override def batches: Seq[Batch] = Seq(
    Batch("Adaptive Input Partitioning", Once,
      AdaptiveInputPartitioning(sparkSession, statsTracker)),  // ← New rule
    Batch("Propagate Empty Relations", Once,
      // ... existing rules ...
    )
  )
}
```

### 3.3 Configuration Parameters

**Location**: Add to `sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala`

```scala
// Enable/disable adaptive input partitioning
val ADAPTIVE_INPUT_PARTITIONING_ENABLED =
  buildConf("spark.sql.adaptive.inputPartitioning.enabled")
    .doc("When true, adaptively adjust file input partition sizes based on " +
      "observed compression ratios and filter selectivity from prior shuffle stages.")
    .version("4.1.0")
    .booleanConf
    .createWithDefault(false)

// Target shuffle partition size (reuse existing)
// spark.sql.adaptive.advisoryPartitionSizeInBytes (default: 64MB)

// Minimum compression ratio to trigger adjustment
val ADAPTIVE_INPUT_PARTITIONING_MIN_RATIO =
  buildConf("spark.sql.adaptive.inputPartitioning.minCompressionRatio")
    .doc("Minimum input/output compression ratio required to trigger adaptive " +
      "input partitioning adjustments. Values below this are considered noise.")
    .version("4.1.0")
    .doubleConf
    .checkValue(v => v >= 1.0, "Must be >= 1.0")
    .createWithDefault(1.5)

// Safety bounds for adjusted partition size
val ADAPTIVE_INPUT_PARTITIONING_MIN_BYTES =
  buildConf("spark.sql.adaptive.inputPartitioning.minPartitionBytes")
    .doc("Minimum allowed input partition size in bytes when adaptively adjusting.")
    .version("4.1.0")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("64MB")

val ADAPTIVE_INPUT_PARTITIONING_MAX_BYTES =
  buildConf("spark.sql.adaptive.inputPartitioning.maxPartitionBytes")
    .doc("Maximum allowed input partition size in bytes when adaptively adjusting.")
    .version("4.1.0")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("1GB")

// Maximum scale factor to prevent extreme adjustments
val ADAPTIVE_INPUT_PARTITIONING_MAX_SCALE_FACTOR =
  buildConf("spark.sql.adaptive.inputPartitioning.maxScaleFactor")
    .doc("Maximum factor by which input partition size can be scaled up or down.")
    .version("4.1.0")
    .doubleConf
    .checkValue(v => v >= 1.0, "Must be >= 1.0")
    .createWithDefault(10.0)

// Number of recent stages to consider for ratio calculation
val ADAPTIVE_INPUT_PARTITIONING_LOOKBACK_STAGES =
  buildConf("spark.sql.adaptive.inputPartitioning.lookbackStages")
    .doc("Number of recent shuffle stages to consider when calculating " +
      "average compression ratio.")
    .version("4.1.0")
    .intConf
    .checkValue(v => v > 0, "Must be positive")
    .createWithDefault(3)
```

### 3.4 Metrics

Add new metrics to track effectiveness:

```scala
// In AdaptiveSparkPlanExec
private val adaptiveInputMetrics = Map(
  "numInputPartitionAdjustments" -> SQLMetrics.createMetric(sparkContext, "number of input partition adjustments"),
  "avgCompressionRatio" -> SQLMetrics.createAverageMetric(sparkContext, "average compression ratio"),
  "inputPartitionSizeBefore" -> SQLMetrics.createSizeMetric(sparkContext, "input partition size before adjustment"),
  "inputPartitionSizeAfter" -> SQLMetrics.createSizeMetric(sparkContext, "input partition size after adjustment")
)
```

## 4. Use Cases and Examples

### Use Case 1: Highly Compressed Parquet Files

**Scenario**: Reading compressed Parquet files with high compression ratio

```sql
-- Table: events (Parquet, Snappy compressed, 10:1 ratio)
SELECT user_id, COUNT(*)
FROM events
WHERE event_type = 'click'
GROUP BY user_id

Stage 1: Scan + Filter + Shuffle
  - Input: 128MB partitions → After decompression: 1.28GB
  - Filter selectivity: 5% → Output: 64MB
  - Shuffle output: 64MB (but many small partitions)

Stage 2: With Adaptive Input
  - Learned ratio: 1.28GB / 64MB = 20:1
  - Adjusted input: 128MB × (64MB target / 3.2MB actual) ≈ 2.5GB
  - Result: Optimal shuffle partition sizes
```

### Use Case 2: Selective Filters

**Scenario**: Filter eliminates 95% of data

```sql
SELECT *
FROM large_table
WHERE country = 'US'  -- Only 5% of data

Stage 1: Initial scan
  - Input: 128MB → Filter → Output: 6.4MB shuffle
  - Ratio: 20:1

Stage 2: Subsequent scan with adaptive partitioning
  - Adjusted input: 128MB × 10 = 1.28GB
  - Output: 64MB shuffle (optimal)
```

### Use Case 3: Iterative ML Workloads

**Scenario**: Multiple passes over same dataset

```python
# K-means clustering with multiple iterations
for i in range(10):
    df = spark.read.parquet("/data/features")
    centroids = df.groupBy(...).agg(...)  # First iteration learns ratio
    # Subsequent iterations use adjusted partition sizes
```

## 5. Limitations and Edge Cases

### 5.1 When It Doesn't Help

1. **Single-stage queries**: No prior shuffle to learn from
   ```sql
   SELECT * FROM parquet_table LIMIT 100
   -- No optimization possible, only one stage
   ```

2. **Heterogeneous data**: Compression ratio varies significantly across files
   - Solution: Use percentile-based estimates (median, P75) instead of average
   - Add coefficient of variation check

3. **First query on new table**: No historical statistics
   - Solution: Use default maxPartitionBytes for first query
   - Optionally store table-level statistics in catalog

### 5.2 Risk Factors

1. **Memory Pressure**: Larger input partitions require more memory
   - Mitigation: Enforce `ADAPTIVE_INPUT_PARTITIONING_MAX_BYTES` (default 1GB)
   - Monitor executor memory metrics

2. **Over-Correction**: Extreme adjustments from outlier ratios
   - Mitigation: Use median instead of mean for ratio calculation
   - Enforce `MAX_SCALE_FACTOR` (default 10x)
   - Require minimum sample size (>= 3 stages)

3. **Non-Uniform Data Distribution**: Some partitions highly compressed, others not
   - Mitigation: Calculate ratio per-partition and use percentiles
   - Consider skew detection similar to `OptimizeSkewedJoin`

4. **Filter Selectivity Changes**: Filter selectivity varies across stages
   - Mitigation: Use rolling window of recent stages
   - Decay older observations

### 5.3 Edge Cases

**Empty Shuffle Partitions**:
```scala
// Handle case where shuffle outputs are empty
if (shuffleBytesWritten == 0) {
  return currentMaxSplit  // Don't adjust
}
```

**Very Small Files**:
```scala
// Don't adjust if files are smaller than adjusted split size
if (totalFileSize < adjustedMaxSplit) {
  return currentMaxSplit  // Keep original
}
```

**Broadcast Joins**: Stages with broadcast don't produce shuffle statistics
```scala
// Skip adjustment for broadcast-only stages
if (!stage.hasShuffle) {
  return currentMaxSplit
}
```

## 6. Implementation Phases

### Phase 1: Foundation (Week 1)
- [ ] Add configuration parameters to SQLConf
- [ ] Create AdaptiveInputStatisticsTracker class
- [ ] Add unit tests for statistics tracking
- [ ] Run scalastyle and fix violations

### Phase 2: Core Logic (Week 2)
- [ ] Implement AdaptiveInputPartitioning rule
- [ ] Add maxSplitBytesOverride to FileSourceScanExec
- [ ] Modify FilePartition.maxSplitBytes to accept override
- [ ] Unit tests for adjustment calculations
- [ ] Edge case handling tests

### Phase 3: AQE Integration (Week 3)
- [ ] Integrate statistics tracker into AdaptiveSparkPlanExec
- [ ] Register rule in AQEOptimizer
- [ ] Add metrics collection
- [ ] Integration tests with multi-stage queries

### Phase 4: Testing & Validation (Week 4)
- [ ] End-to-end tests with Parquet/ORC files
- [ ] Performance benchmarks (TPC-DS queries)
- [ ] Memory pressure tests
- [ ] Edge case validation (empty partitions, small files, etc.)
- [ ] Documentation updates

### Phase 5: Tuning & Refinement (Week 5)
- [ ] Analyze benchmark results
- [ ] Tune default configuration values
- [ ] Add additional safety checks if needed
- [ ] Performance optimization
- [ ] Code review and refinements

## 7. Testing Strategy

### 7.1 Unit Tests

**File**: `sql/core/src/test/scala/org/apache/spark/sql/execution/adaptive/AdaptiveInputPartitioningSuite.scala`

```scala
class AdaptiveInputPartitioningSuite extends QueryTest with SharedSparkSession {

  test("calculate compression ratio from shuffle statistics") {
    val tracker = new AdaptiveInputStatisticsTracker()
    val mapStats = new MapOutputStatistics(0, Array(10L, 12L, 11L))
    tracker.recordStageStats(0, inputBytesRead = 1000L, mapStats)

    assert(tracker.getRecentCompressionRatio() === Some(1000.0 / 33.0))
  }

  test("adjust maxSplitBytes based on compression ratio") {
    withSQLConf(
      SQLConf.ADAPTIVE_INPUT_PARTITIONING_ENABLED.key -> "true",
      SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES.key -> "64MB") {

      // Setup: Create query with known compression ratio
      // Verify: Input partition size adjusted correctly
    }
  }

  test("respect maximum scale factor") {
    // Test that adjustments don't exceed MAX_SCALE_FACTOR
  }

  test("respect min and max partition bytes") {
    // Test safety bounds
  }

  test("skip adjustment when compression ratio is too low") {
    // Test MIN_RATIO threshold
  }

  test("handle empty shuffle partitions") {
    // Edge case: all shuffle partitions are 0 bytes
  }
}
```

### 7.2 Integration Tests

```scala
test("adaptive input partitioning with multi-stage query") {
  withSQLConf(SQLConf.ADAPTIVE_INPUT_PARTITIONING_ENABLED.key -> "true") {
    withTempPath { path =>
      // Create compressed Parquet file
      spark.range(10000000)
        .selectExpr("id", "id % 100 as group")
        .write.parquet(path.getAbsolutePath)

      // Query with multiple stages
      val df = spark.read.parquet(path.getAbsolutePath)
        .filter("group = 1")  // High selectivity
        .groupBy("group")
        .count()

      val result = df.collect()

      // Verify metrics show adjustment occurred
      val metrics = df.queryExecution.executedPlan.metrics
      assert(metrics("numInputPartitionAdjustments").value > 0)
    }
  }
}
```

### 7.3 Performance Benchmarks

Use TPC-DS queries to measure impact:
- Query 3: Highly selective filters
- Query 7: Multi-stage aggregations
- Query 42: Compressed data reads

Metrics to track:
- Total query execution time
- Number of tasks
- Average task duration
- Shuffle read/write bytes
- Memory consumption

## 8. Alternatives Considered

### Alternative 1: Static Table Statistics
Store compression ratios in table metadata.

**Pros**: Available at planning time, no runtime overhead
**Cons**: Requires statistics collection, may be stale, doesn't handle query-specific filters

**Decision**: Rejected. Runtime adaptation is more accurate for query-specific selectivity.

### Alternative 2: Sample-Based Estimation
Read small sample of data first to estimate ratio.

**Pros**: Works for single-stage queries
**Cons**: Additional I/O overhead, sampling may not be representative

**Decision**: Could be future enhancement, but AQE-based approach is simpler for multi-stage queries.

### Alternative 3: Per-File Metadata
Store compression ratio per file in catalog.

**Pros**: More granular than table-level
**Cons**: High metadata overhead, doesn't account for filters

**Decision**: Rejected. Too complex for initial implementation.

## 9. Future Enhancements

### 9.1 Persistent Learning
Store learned compression ratios in catalog:
```sql
ALTER TABLE events SET TBLPROPERTIES (
  'spark.adaptive.avgCompressionRatio' = '10.5',
  'spark.adaptive.lastUpdated' = '2025-10-27'
);
```

### 9.2 Per-Partition Adjustment
Instead of uniform adjustment, adjust per partition based on skew:
```scala
// Different maxSplitBytes for different file groups
case class AdaptiveFilePartition(
    index: Int,
    files: Array[PartitionedFile],
    adjustedMaxSplit: Long  // Per-partition value
)
```

### 9.3 ML-Based Prediction
Use historical query patterns to predict optimal partition sizes:
```scala
class PartitionSizePredictor {
  def predict(
      tableId: String,
      filterPredicates: Seq[Expression],
      historicalStats: Seq[QueryStats]): Long
}
```

### 9.4 Cross-Query Optimization
Share statistics across queries in same session:
```scala
// Session-level statistics cache
sparkSession.sharedState.adaptiveInputStatsCache
  .getOrElseUpdate(tableId, computeStats())
```

## 10. Open Questions

1. **Should we adjust input partitioning for non-shuffle operations?**
   - E.g., file scans followed by broadcast joins
   - Decision: No for initial version, focus on shuffle-heavy workloads

2. **How to handle skewed compression ratios?**
   - Some partitions compress 10:1, others 2:1
   - Decision: Use median instead of mean, add skew detection in future

3. **Should adjustment be per-table or per-query?**
   - Decision: Per-query for initial version, per-table statistics in future

4. **What if compression ratio changes mid-query?**
   - E.g., different files have different characteristics
   - Decision: Use rolling window, re-adjust if ratio changes significantly

## 11. References

### Existing Code
- `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FilePartition.scala`
- `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveSparkPlanExec.scala`
- `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/CoalesceShufflePartitions.scala`
- `sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/OptimizeSkewedJoin.scala`

### Related JIRAs
- SPARK-XXXXX: Adaptive Input Partitioning (to be created)

### Related Configurations
- `spark.sql.files.maxPartitionBytes` (128MB)
- `spark.sql.adaptive.advisoryPartitionSizeInBytes` (64MB)
- `spark.sql.adaptive.coalescePartitions.minPartitionSize` (1MB)

---

**Document Version**: 1.0
**Last Updated**: 2025-10-27
**Author**: Adaptive Input Partitioning Feature Team
