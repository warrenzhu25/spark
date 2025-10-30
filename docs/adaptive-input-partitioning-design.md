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
│ Validation: Check Shuffle Partition Sizes                      │
│   Median: 10MB > 1MB threshold → OK (forward optimization)     │
│   OR                                                            │
│   Median: 0.5MB < 1MB threshold → Re-execution candidate       │
└──────────────┬────────────────────────┬─────────────────────────┘
               │                        │
     (≥ 1MB)   │                        │ (< 1MB + safety checks pass)
               ▼                        ▼
┌──────────────────────┐  ┌─────────────────────────────────────┐
│ Forward Optimization │  │ Re-execute Stage 1                  │
│ (Original Flow)      │  │   with larger input partitions      │
└──────────┬───────────┘  └──────────┬──────────────────────────┘
           │                         │
           │                         ▼
           │              ┌─────────────────────────────────────┐
           │              │ Stage 1 Re-executed:                │
           │              │   Input: 8GB partitions × 2 tasks   │
           │              │   Output: 64MB shuffle partitions   │
           │              └──────────┬──────────────────────────┘
           │                         │
           └─────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│ AQE Re-optimization (AdaptiveInputPartitioning rule)           │
│   Observes: median shuffle partition = 10MB (or 64MB if re-run)│
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

// Re-execution configuration parameters
val ADAPTIVE_INPUT_PARTITIONING_MIN_SHUFFLE_PARTITION_BYTES =
  buildConf("spark.sql.adaptive.inputPartitioning.minShufflePartitionBytes")
    .doc("Minimum acceptable median shuffle partition size in bytes. If a stage " +
      "produces shuffle partitions below this threshold, the stage may be " +
      "re-executed with larger input partitions to achieve better shuffle sizes. " +
      "Set to 0 to disable re-execution based on partition size.")
    .version("4.1.0")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("1MB")

val ADAPTIVE_INPUT_PARTITIONING_MAX_RE_EXECUTIONS =
  buildConf("spark.sql.adaptive.inputPartitioning.maxReExecutions")
    .doc("Maximum number of times a stage can be re-executed due to suboptimal " +
      "shuffle partition sizes. This prevents infinite re-execution loops.")
    .version("4.1.0")
    .intConf
    .checkValue(v => v >= 0, "Must be non-negative")
    .createWithDefault(2)

val ADAPTIVE_INPUT_PARTITIONING_MIN_IMPROVEMENT_FACTOR =
  buildConf("spark.sql.adaptive.inputPartitioning.minImprovementFactor")
    .doc("Minimum improvement factor required to trigger stage re-execution. " +
      "Only re-execute if the adjusted partition size would be at least this " +
      "many times larger than the current size. This prevents re-execution " +
      "for marginal improvements.")
    .version("4.1.0")
    .doubleConf
    .checkValue(v => v >= 1.0, "Must be >= 1.0")
    .createWithDefault(2.0)

val ADAPTIVE_INPUT_PARTITIONING_RE_EXECUTION_COST_BENEFIT_ENABLED =
  buildConf("spark.sql.adaptive.inputPartitioning.reExecutionCostBenefitEnabled")
    .doc("When true, perform cost-benefit analysis before re-executing a stage. " +
      "Only re-execute if the expected benefit (based on remaining query " +
      "complexity and data volume) outweighs the re-execution overhead.")
    .version("4.1.0")
    .booleanConf
    .createWithDefault(true)
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

### 3.5 Shuffle Partition Size Validation and Re-execution

#### 3.5.1 Problem Statement

The forward-looking adjustment approach described in Section 3.1 optimizes subsequent
stages based on learned compression ratios. However, this leaves the first stage with
suboptimal shuffle partition sizes when:

1. **Initial Stage Produces Undersized Partitions**: First file scan may produce shuffle
   partitions much smaller than the target size (e.g., 0.5MB instead of 64MB)
2. **Single-Stage Queries**: Queries with only one shuffle stage never benefit from
   adaptive adjustment
3. **Query Performance Impact**: Undersized shuffle partitions create excessive task
   overhead and reduce parallelism efficiency

#### 3.5.2 Solution: Stage Re-execution

When a completed stage produces shuffle partitions below the configurable threshold
`minShufflePartitionBytes` (default 1MB), the stage can be re-executed with
larger input partition sizes to achieve better shuffle output sizes.

**Key Design Principles**:
- Re-execution is optional and controlled by safety mechanisms
- Only re-execute when expected benefit outweighs cost
- Limit re-execution attempts to prevent infinite loops
- Use learned compression ratios to calculate optimal input sizes

#### 3.5.3 Detection Logic

After a shuffle stage completes, validate the shuffle partition sizes:

```scala
case class ShufflePartitionValidator {
  def shouldReExecute(
      stage: ShuffleQueryStageExec,
      reExecutionCount: Int,
      conf: SQLConf): Boolean = {

    // Extract shuffle partition sizes
    val partitionSizes = stage.mapStats.get.bytesByPartitionId
    val medianSize = Utils.median(partitionSizes, false).toLong

    // Check if below threshold
    val minThreshold = conf.adaptiveInputPartitioningMinShufflePartitionBytes
    val isUnderSized = minThreshold > 0 && medianSize < minThreshold

    if (!isUnderSized) {
      return false
    }

    // Apply safety checks (detailed in Section 3.5.4)
    val maxReExecutions = conf.adaptiveInputPartitioningMaxReExecutions
    if (reExecutionCount >= maxReExecutions) {
      logInfo(s"Skipping re-execution: max attempts ($maxReExecutions) reached")
      return false
    }

    val improvementFactor = calculateImprovementFactor(stage)
    val minImprovement = conf.adaptiveInputPartitioningMinImprovementFactor
    if (improvementFactor < minImprovement) {
      logInfo(s"Skipping re-execution: improvement factor $improvementFactor " +
        s"< minimum $minImprovement")
      return false
    }

    if (conf.adaptiveInputPartitioningReExecutionCostBenefitEnabled) {
      val costBenefitRatio = calculateCostBenefitRatio(stage)
      if (costBenefitRatio < 1.0) {
        logInfo(s"Skipping re-execution: cost-benefit ratio $costBenefitRatio < 1.0")
        return false
      }
    }

    true
  }
}
```

#### 3.5.4 Re-execution Criteria

**Three-Layer Safety Mechanism** (detailed in Section 3.5.5):

1. **Maximum Attempts Check**:
   ```scala
   if (reExecutionCount >= conf.maxReExecutions) {
     // Skip re-execution
   }
   ```
   Default: 2 re-execution attempts per stage

2. **Minimum Improvement Check**:
   ```scala
   val currentMedianSize = calculateMedianPartitionSize(stage)
   val projectedMedianSize = calculateProjectedSize(stage, adjustedMaxSplit)
   val improvementFactor = projectedMedianSize / currentMedianSize

   if (improvementFactor < conf.minImprovementFactor) {
     // Skip re-execution (marginal benefit)
   }
   ```
   Default: 2.0x improvement required

3. **Cost-Benefit Analysis**:
   ```scala
   val reExecutionCost = estimateReExecutionCost(stage)
   val downstreamBenefit = estimateDownstreamBenefit(stage, remainingPlan)
   val costBenefitRatio = downstreamBenefit / reExecutionCost

   if (costBenefitRatio < 1.0) {
     // Skip re-execution (cost exceeds benefit)
   }
   ```

#### 3.5.5 Safety Mechanisms

##### Safety Mechanism 1: Maximum Re-execution Attempts

**Purpose**: Prevent infinite re-execution loops

**Implementation**:
```scala
class StageReExecutionTracker {
  private val reExecutionCounts = mutable.HashMap[Int, Int]()

  def recordReExecution(stageId: Int): Unit = {
    reExecutionCounts(stageId) = reExecutionCounts.getOrElse(stageId, 0) + 1
  }

  def getReExecutionCount(stageId: Int): Int = {
    reExecutionCounts.getOrElse(stageId, 0)
  }

  def canReExecute(stageId: Int, maxAllowed: Int): Boolean = {
    getReExecutionCount(stageId) < maxAllowed
  }
}
```

**Configuration**: `spark.sql.adaptive.inputPartitioning.maxReExecutions` (default: 2)

**Example**:
- Attempt 1: 128MB input → 0.5MB shuffle (too small)
- Attempt 2: 8GB input → 32MB shuffle (too small)
- Attempt 3: Blocked by max attempts (accept 32MB partitions)

##### Safety Mechanism 2: Minimum Improvement Threshold

**Purpose**: Avoid re-execution for marginal improvements

**Calculation**:
```scala
def calculateImprovementFactor(
    stage: ShuffleQueryStageExec,
    adjustedMaxSplit: Long): Double = {

  val currentMedian = medianShufflePartitionSize(stage)
  val compressionRatio = calculateCompressionRatio(stage)
  val projectedMedian = (adjustedMaxSplit / compressionRatio).toLong

  if (currentMedian > 0) {
    projectedMedian.toDouble / currentMedian.toDouble
  } else {
    1.0
  }
}
```

**Configuration**: `spark.sql.adaptive.inputPartitioning.minImprovementFactor` (default: 2.0)

**Example**:
- Current: 8MB median shuffle partition
- Projected: 24MB median (3x improvement)
- Decision: Re-execute (3x > 2x threshold)

**Example (skipped)**:
- Current: 32MB median shuffle partition
- Projected: 48MB median (1.5x improvement)
- Decision: Skip re-execution (1.5x < 2x threshold)

##### Safety Mechanism 3: Cost-Benefit Analysis

**Purpose**: Re-execute only when expected benefit justifies the cost

**Cost Estimation**:
```scala
def estimateReExecutionCost(stage: ShuffleQueryStageExec): Double = {
  val taskDuration = stage.metrics("taskDuration").value
  val numTasks = stage.metrics("numTasks").value
  val totalCost = taskDuration * numTasks

  // Account for stage scheduling overhead
  val schedulingOverhead = 5000.0  // ~5 seconds in milliseconds
  totalCost + schedulingOverhead
}
```

**Benefit Estimation**:
```scala
def estimateDownstreamBenefit(
    stage: ShuffleQueryStageExec,
    remainingPlan: SparkPlan): Double = {

  // Count remaining stages that will read this shuffle
  val dependentStages = countDependentStages(stage, remainingPlan)

  // Estimate task overhead reduction
  val currentMedian = medianShufflePartitionSize(stage)
  val targetSize = conf.advisoryPartitionSizeInBytes
  val taskOverheadPerPartition = 50.0  // ~50ms per task

  val currentNumPartitions = stage.mapStats.get.bytesByPartitionId.length
  val projectedNumPartitions = (stage.mapStats.get.bytesByPartitionId.sum /
    targetSize).toInt.max(1)
  val partitionReduction = currentNumPartitions - projectedNumPartitions

  // Benefit = overhead saved × dependent stages
  partitionReduction * taskOverheadPerPartition * dependentStages
}
```

**Decision Logic**:
```scala
if (costBenefitRatio >= 1.0) {
  // Benefit >= Cost: Re-execute
  logInfo(s"Re-executing stage: benefit=$downstreamBenefit, cost=$reExecutionCost")
  reExecuteStage(stage, adjustedMaxSplit)
} else {
  // Cost > Benefit: Skip re-execution
  logInfo(s"Skipping re-execution: benefit=$downstreamBenefit < cost=$reExecutionCost")
}
```

**Configuration**: `spark.sql.adaptive.inputPartitioning.reExecutionCostBenefitEnabled`
(default: true)

**Example Scenarios**:

*Scenario 1: Multi-stage query (re-execute)*
- Re-execution cost: 10 seconds
- Downstream stages: 5 stages will read this shuffle
- Overhead savings: 3 seconds per stage × 5 = 15 seconds
- Cost-benefit ratio: 15 / 10 = 1.5
- Decision: Re-execute

*Scenario 2: Final stage (skip re-execution)*
- Re-execution cost: 10 seconds
- Downstream stages: 0 (this is the final shuffle)
- Benefit: 0 seconds
- Cost-benefit ratio: 0 / 10 = 0
- Decision: Skip re-execution

#### 3.5.6 Re-execution Flow

**High-Level Flow**:
```
┌─────────────────────────────────────────────────────────────────┐
│ Stage N Completes                                               │
│   Shuffle output: 100 partitions × 0.5MB = 50MB total          │
└───────────────────┬─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────┐
│ Validation: Check median partition size                         │
│   Median: 0.5MB < 1MB threshold → UNDERSIZED                   │
└───────────────────┬─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────┐
│ Safety Check 1: Maximum Attempts                                │
│   Current attempts: 0 < 2 max → PASS                           │
└───────────────────┬─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────┐
│ Safety Check 2: Minimum Improvement                             │
│   Calculate: 128MB × (64MB / 0.5MB) = 16GB input               │
│   Projected median: 64MB / compression_ratio ≈ 6MB             │
│   Improvement: 6MB / 0.5MB = 12x > 2x threshold → PASS        │
└───────────────────┬─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────┐
│ Safety Check 3: Cost-Benefit Analysis                           │
│   Cost: 5 seconds re-execution                                  │
│   Benefit: 8 seconds saved across 3 downstream stages           │
│   Ratio: 8 / 5 = 1.6 > 1.0 → PASS                             │
└───────────────────┬─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────┐
│ Re-execute Stage N with Adjusted Input                          │
│   New input: 16GB partitions                                    │
│   New output: 100 partitions × 6MB = 600MB (better!)           │
└───────────────────┬─────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────┐
│ Continue Query Execution                                        │
│   Downstream stages benefit from larger shuffle partitions      │
└─────────────────────────────────────────────────────────────────┘
```

#### 3.5.7 Integration with Adaptive Partitioning Rule

**Modify AdaptiveInputPartitioning to support re-execution**:

```scala
case class AdaptiveInputPartitioning(
    session: SparkSession,
    statsTracker: AdaptiveInputStatisticsTracker,
    reExecutionTracker: StageReExecutionTracker) extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.adaptiveInputPartitioningEnabled) {
      return plan
    }

    // Check if any completed stages need re-execution
    val stagesToReExecute = identifyStagesToReExecute(plan)

    if (stagesToReExecute.nonEmpty) {
      stagesToReExecute.foreach { stage =>
        val adjustedMaxSplit = computeAdjustedMaxSplit(stage)
        reExecuteStageWithAdjustment(stage, adjustedMaxSplit)
        reExecutionTracker.recordReExecution(stage.id)
      }
    }

    // Apply forward-looking adjustments to upcoming file scans
    plan.transformUp {
      case scan: FileSourceScanExec => adjustFileScan(scan)
      case scan: BatchScanExec if scan.scan.isInstanceOf[FileScan] =>
        adjustBatchScan(scan)
    }
  }

  private def identifyStagesToReExecute(
      plan: SparkPlan): Seq[ShuffleQueryStageExec] = {

    plan.collect {
      case stage: ShuffleQueryStageExec
          if stage.isMaterialized && shouldReExecute(stage) =>
        stage
    }
  }

  private def shouldReExecute(stage: ShuffleQueryStageExec): Boolean = {
    val validator = new ShufflePartitionValidator()
    val reExecutionCount = reExecutionTracker.getReExecutionCount(stage.id)
    validator.shouldReExecute(stage, reExecutionCount, conf)
  }
}
```

#### 3.5.8 Metrics for Re-execution

Add metrics to track re-execution effectiveness:

```scala
// In AdaptiveSparkPlanExec metrics
private val reExecutionMetrics = Map(
  "numStageReExecutions" -> SQLMetrics.createMetric(
    sparkContext, "number of stage re-executions"),
  "reExecutionTimeMs" -> SQLMetrics.createTimingMetric(
    sparkContext, "time spent in re-execution"),
  "avgPartitionSizeBeforeReExecution" -> SQLMetrics.createSizeMetric(
    sparkContext, "average partition size before re-execution"),
  "avgPartitionSizeAfterReExecution" -> SQLMetrics.createSizeMetric(
    sparkContext, "average partition size after re-execution"),
  "reExecutionCostBenefitRatio" -> SQLMetrics.createAverageMetric(
    sparkContext, "average cost-benefit ratio for re-executions")
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

### Use Case 4: Stage Re-execution with Undersized Shuffle Partitions

**Scenario**: First stage produces extremely small shuffle partitions that trigger re-execution

```sql
-- Query on highly selective filtered data
SELECT product_id, SUM(revenue) as total_revenue
FROM sales_data
WHERE region = 'APAC' AND year = 2024  -- Very selective: 0.1% of data
GROUP BY product_id
HAVING total_revenue > 100000

Configuration:
  spark.sql.adaptive.inputPartitioning.enabled = true
  spark.sql.adaptive.inputPartitioning.minShufflePartitionBytes = 1MB
  spark.sql.adaptive.inputPartitioning.maxReExecutions = 2
  spark.sql.adaptive.inputPartitioning.minImprovementFactor = 2.0
```

**Execution Timeline**:

```
Stage 1 - Initial Execution (Attempt 1):
├─ File Scan: 200 tasks × 128MB = 25.6GB input
├─ Filter: 0.1% selectivity → 25.6MB data remains
├─ Shuffle Write: 200 partitions × 0.128MB = 25.6MB total
└─ Median Shuffle Partition: 0.128MB

Validation Phase:
├─ Check: 0.128MB < 1MB threshold → UNDERSIZED ✓
├─ Safety Check 1: Re-execution count (0) < max (2) → PASS ✓
├─ Safety Check 2: Calculate improvement factor
│   - Current median: 0.128MB
│   - Compression ratio: 25.6GB / 25.6MB = 1000:1
│   - Adjusted input: 128MB × (64MB / 0.128MB) = 64GB
│   - Projected median: 64GB / 1000 = 64MB
│   - Improvement: 64MB / 0.128MB = 500x > 2.0x → PASS ✓
├─ Safety Check 3: Cost-benefit analysis
│   - Re-execution cost: ~8 seconds (stage duration)
│   - Downstream stages: 1 (final aggregation)
│   - Current partitions: 200 (excessive overhead)
│   - Projected partitions: 1 (optimal)
│   - Task overhead saved: 199 tasks × 50ms = 9.95 seconds
│   - Cost-benefit ratio: 9.95 / 8 = 1.24 > 1.0 → PASS ✓
└─ Decision: RE-EXECUTE with adjusted input partitions

Stage 1 - Re-execution (Attempt 2):
├─ File Scan: 1 task × 25.6GB = 25.6GB input (same total, different split)
├─ Filter: 0.1% selectivity → 25.6MB data remains
├─ Shuffle Write: 1 partition × 25.6MB = 25.6MB total
└─ Median Shuffle Partition: 25.6MB

Validation Phase (Attempt 2):
├─ Check: 25.6MB > 1MB threshold → OK (still not optimal, but acceptable)
├─ Safety Check 1: Re-execution count (1) < max (2) → Could re-execute again
├─ Safety Check 2: Calculate improvement factor
│   - Current median: 25.6MB
│   - Projected median: 64MB
│   - Improvement: 64MB / 25.6MB = 2.5x > 2.0x → PASS ✓
├─ Safety Check 3: Cost-benefit analysis
│   - Re-execution cost: ~8 seconds
│   - Task overhead saved: 0 tasks (already 1 partition)
│   - Cost-benefit ratio: 0 / 8 = 0 < 1.0 → FAIL ✗
└─ Decision: ACCEPT current partitions (cost-benefit check prevents wasteful re-execution)

Stage 2 - Final Aggregation:
├─ Reads: 1 partition × 25.6MB = 25.6MB
├─ Aggregate: GROUP BY product_id, HAVING filter
└─ Result: Optimal performance (single partition, no task overhead)
```

**Performance Impact**:
- **Without Re-execution**: 200 tiny tasks (0.128MB each) with ~10 seconds overhead
- **With Re-execution**: 1 optimal task (25.6MB) + 8 seconds re-execution cost
- **Net Benefit**: ~2 seconds faster overall query execution

**Key Insights**:
1. First attempt produced 1000:1 compression ratio (extreme selectivity)
2. Re-execution reduced task count from 200 → 1
3. Cost-benefit analysis prevented a second re-execution (diminishing returns)
4. Safety mechanisms balanced optimization benefit vs. re-execution overhead

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

5. **Infinite Re-execution Loops**: Stage repeatedly re-executed without improvement
   - Mitigation: Enforce `MAX_RE_EXECUTIONS` limit (default 2 attempts)
   - Track re-execution count per stage
   - Prevent re-execution if improvement factor < threshold

6. **Excessive Re-execution Overhead**: Re-execution cost exceeds performance benefit
   - Mitigation: Enable `RE_EXECUTION_COST_BENEFIT_ENABLED` (default true)
   - Calculate cost-benefit ratio before re-executing
   - Only re-execute if benefit > cost (ratio > 1.0)
   - Skip re-execution for final stages (no downstream benefit)

7. **Re-execution Thrashing**: Rapidly oscillating between different partition sizes
   - Mitigation: Require `MIN_IMPROVEMENT_FACTOR` (default 2.0x)
   - Only re-execute for significant improvements
   - Use stable compression ratio estimates (median over multiple attempts)

8. **Resource Contention**: Re-execution competes with other query stages for resources
   - Mitigation: Re-execution uses same resource allocation as original stage
   - No additional executor resources required
   - Stage scheduling overhead (~5 seconds) included in cost estimation

9. **Query Timeout Risk**: Re-execution may cause queries to exceed timeout limits
   - Mitigation: Cost-benefit analysis accounts for query complexity
   - Skip re-execution if remaining query is simple
   - Consider disabling for time-sensitive queries via configuration

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

### 7.4 Re-execution Performance Analysis

**Purpose**: Measure the overhead and benefit of stage re-execution

#### 7.4.1 Re-execution Overhead Metrics

Track these metrics specifically for re-executed stages:

```scala
// Re-execution overhead tracking
case class ReExecutionMetrics(
  numReExecutions: Int,
  totalReExecutionTimeMs: Long,
  avgReExecutionTimeMs: Double,
  maxReExecutionTimeMs: Long,
  reExecutionRatio: Double  // re-execution time / total query time
)
```

**Key Metrics**:
1. **Re-execution Frequency**: How often stages are re-executed
   - Target: < 10% of all stages for typical workloads
   - Alert if > 25% (may indicate misconfiguration)

2. **Re-execution Time Overhead**: Time spent re-executing stages
   - Measure: Re-execution duration vs. original stage duration
   - Target: Re-execution time < 20% of total query time
   - Alert if > 50% (re-execution may be counterproductive)

3. **Partition Size Improvement**: Shuffle partition size before/after re-execution
   - Measure: Median partition size improvement ratio
   - Target: ≥ 2x improvement (matches MIN_IMPROVEMENT_FACTOR)
   - Alert if < 1.5x (minimal benefit)

4. **Task Count Reduction**: Number of tasks before/after re-execution
   - Measure: Task count reduction ratio
   - Target: ≥ 50% reduction for undersized partitions
   - Alert if < 25% (marginal benefit)

#### 7.4.2 Cost-Benefit Validation

Validate that cost-benefit analysis is accurate:

```scala
test("cost-benefit analysis accuracy") {
  withSQLConf(RE_EXECUTION_COST_BENEFIT_ENABLED.key -> "true") {
    val query = // query with multiple stages

    // Measure actual benefit
    val metricsWithReExecution = runQuery(query)
    val metricsWithoutReExecution = runQuery(query,
      RE_EXECUTION_ENABLED.key -> "false")

    val actualBenefit = metricsWithoutReExecution.totalTime -
      metricsWithReExecution.totalTime
    val reExecutionCost = metricsWithReExecution.reExecutionTime

    val actualRatio = actualBenefit / reExecutionCost

    // Predicted ratio should be within 2x of actual
    assert(predictedRatio >= actualRatio * 0.5)
    assert(predictedRatio <= actualRatio * 2.0)
  }
}
```

**Validation Criteria**:
- Predicted cost-benefit ratio within 2x of actual ratio
- False positive rate (re-execute when shouldn't) < 10%
- False negative rate (don't re-execute when should) < 15%

#### 7.4.3 Re-execution Performance Scenarios

**Scenario 1: Extreme Selectivity (Best Case)**
- Input: 100GB table, 0.01% selectivity → 10MB shuffle
- Without re-execution: 1000 tasks × 10KB = high overhead
- With re-execution: 1 task × 10MB = optimal
- Expected benefit: 30-50% query speedup

**Scenario 2: Moderate Selectivity (Common Case)**
- Input: 100GB table, 10% selectivity → 10GB shuffle
- Without re-execution: 1000 tasks × 10MB = moderate overhead
- With re-execution: 160 tasks × 64MB = better
- Expected benefit: 10-20% query speedup

**Scenario 3: Minimal Selectivity (Edge Case)**
- Input: 100GB table, 90% selectivity → 90GB shuffle
- Without re-execution: 1000 tasks × 90MB = acceptable
- With re-execution: Skipped (cost-benefit ratio < 1.0)
- Expected overhead: 0% (re-execution correctly skipped)

**Scenario 4: Single-Stage Query (Worst Case)**
- Input: 100GB table, final aggregation
- Without re-execution: Completes in 1 stage
- With re-execution: 2x stage execution time
- Expected overhead: Cost-benefit should prevent re-execution

#### 7.4.4 Benchmark Configuration Matrix

Test re-execution across different configurations:

| Configuration | minShufflePartitionBytes | maxReExecutions | minImprovementFactor | costBenefitEnabled |
|---------------|--------------------------|-----------------|----------------------|--------------------|
| Conservative  | 512KB                    | 1               | 3.0                  | true               |
| Moderate      | 1MB (default)            | 2 (default)     | 2.0 (default)        | true (default)     |
| Aggressive    | 4MB                      | 3               | 1.5                  | false              |
| Disabled      | 0                        | 0               | -                    | false              |

**Expected Results**:
- **Conservative**: Lowest re-execution rate, safest for production
- **Moderate**: Balanced re-execution vs. benefit
- **Aggressive**: Highest re-execution rate, may cause overhead
- **Disabled**: Baseline for comparison

#### 7.4.5 Performance Regression Tests

Add automated regression tests:

```scala
test("re-execution should not regress query performance") {
  val queries = Seq(tpcdsQuery3, tpcdsQuery7, tpcdsQuery42)

  queries.foreach { query =>
    // Baseline: without re-execution
    val baselineTime = benchmark(query,
      RE_EXECUTION_ENABLED.key -> "false")

    // With re-execution
    val reExecutionTime = benchmark(query,
      RE_EXECUTION_ENABLED.key -> "true")

    // Re-execution should not make queries slower
    // Allow 5% variance for noise
    assert(reExecutionTime <= baselineTime * 1.05,
      s"Query regressed: $query took ${reExecutionTime}ms vs " +
      s"baseline ${baselineTime}ms")
  }
}
```

#### 7.4.6 Memory Impact Analysis

Monitor memory consumption during re-execution:

```scala
case class MemoryMetrics(
  peakExecutorMemory: Long,
  avgExecutorMemory: Long,
  spilledBytes: Long,
  oomErrors: Int
)
```

**Validation**:
- Re-execution should not cause OOM errors
- Peak memory should stay within executor limits
- Spill bytes should not increase significantly
- If memory pressure detected, verify `MAX_BYTES` limit is enforced

**Target Metrics**:
- OOM error rate: 0%
- Memory increase: < 20% vs. baseline
- Spill increase: < 10% vs. baseline

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
