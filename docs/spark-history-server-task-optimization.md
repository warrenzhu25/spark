# Spark History Server: Large Task Event Log Optimization

## Problem Statement

When Spark applications have millions of tasks, the History Server takes excessive time to process event logs due to:
1. Each task write updates 40+ KVStore indices
2. Individual task writes (no batching)
3. All tasks stored regardless of importance

## Solution Overview

Three complementary optimizations:

| Optimization | Impact | When Applied |
|--------------|--------|--------------|
| **1. Batched Task Writes** | Reduce KVStore overhead | During replay |
| **2. Reduced Index Set** | Faster writes, less memory | During replay |
| **3. Quantile-Based Filtering** | Keep only important tasks | After replay |

**Key Insight**: Compute and cache quantiles **once at stage completion** instead of relying on indices for on-demand calculation. This enables the reduced index set while preserving quantile functionality.

---

## Optimization 1: Batched Task Writes

### Design

Buffer task writes during history replay and flush in batches.

### Configuration

```scala
val TASK_WRITE_BATCH_SIZE = ConfigBuilder("spark.history.task.writeBatchSize")
  .doc("Batch size for task writes during replay. Default 1000.")
  .version("4.0.0")
  .intConf
  .createWithDefault(1000)
```

### Implementation

**File:** `core/src/main/scala/org/apache/spark/status/AppStatusListener.scala`

```scala
// Add at class level
private val taskWriteBuffer = new ArrayBuffer[Any]()  // TaskDataWrapper or Light
private val taskWriteBatchSize = conf.get(TASK_WRITE_BATCH_SIZE)

// In onTaskEnd (non-live mode)
taskWriteBuffer += taskWrapper
if (taskWriteBuffer.size >= taskWriteBatchSize) {
  flushTaskBuffer()
}

private def flushTaskBuffer(): Unit = {
  taskWriteBuffer.foreach(kvstore.write)
  taskWriteBuffer.clear()
}

// Flush on: onStageCompleted, onApplicationEnd, close
```

---

## Optimization 2: Reduced Index Set + Pre-computed Quantiles

### Design

Use `TaskDataWrapperLight` with ~10 essential indices. To compensate for removed indices used in quantile calculation, **compute quantiles once at stage completion** and cache them.

### Why This Works

Current flow (index-dependent):
```
UI requests quantiles → scanTasks() iterates via DURATION index → expensive
```

New flow (pre-computed):
```
Stage completes → compute all quantiles once → save to CachedQuantile → UI reads cache
```

### Essential Indices (10 vs 40+)

| Index | Purpose |
|-------|---------|
| `STAGE` (parent) | Parent relationship |
| `TASK_INDEX` | Sort by partition |
| `LAUNCH_TIME` | Sort by start time |
| `DURATION` | Sort by duration (keep for basic sorting) |
| `STATUS` | Filter by status |
| `EXECUTOR` | Filter by executor |
| `COMPLETION_TIME` | Cleanup ordering |

**Removed indices** (used only for quantile calculation - now pre-computed):
- `SHUFFLE_READ_FETCH_WAIT_TIME`, `SHUFFLE_WRITE_TIME`, `MEM_SPILL`, `DISK_SPILL`
- `GC_TIME`, `EXEC_RUN_TIME`, `DESER_TIME`, etc. (30+ indices)

### Configuration

```scala
val USE_LIGHT_TASK_STORAGE = ConfigBuilder("spark.history.task.useLightStorage")
  .doc("Use reduced index set with pre-computed quantiles. Default true.")
  .version("4.0.0")
  .booleanConf
  .createWithDefault(true)
```

### Implementation

#### 2.1 TaskDataWrapperLight

**File:** `core/src/main/scala/org/apache/spark/status/storeTypes.scala`

```scala
private[spark] class TaskDataWrapperLight(
    // Indexed fields (~10)
    @KVIndexParam(parent = TaskIndexNames.STAGE) val taskId: JLong,
    @KVIndexParam(value = TaskIndexNames.TASK_INDEX, parent = TaskIndexNames.STAGE)
    val index: Int,
    @KVIndexParam(value = TaskIndexNames.LAUNCH_TIME, parent = TaskIndexNames.STAGE)
    val launchTime: Long,
    @KVIndexParam(value = TaskIndexNames.DURATION, parent = TaskIndexNames.STAGE)
    val duration: Long,
    @KVIndexParam(value = TaskIndexNames.STATUS, parent = TaskIndexNames.STAGE)
    val status: String,
    @KVIndexParam(value = TaskIndexNames.EXECUTOR, parent = TaskIndexNames.STAGE)
    val executorId: String,
    @KVIndexParam(value = TaskIndexNames.COMPLETION_TIME, parent = TaskIndexNames.STAGE)
    val completionTime: Long,

    // Non-indexed fields (for storage and API conversion)
    val attempt: Int,
    val partitionId: Int,
    val host: String,
    val taskLocality: String,
    val speculative: Boolean,
    val errorMessage: Option[String],
    val stageId: Int,
    val stageAttemptId: Int,

    // Metrics (not indexed, but stored for quantile computation)
    val executorRunTime: Long,
    val executorCpuTime: Long,
    val jvmGcTime: Long,
    val memoryBytesSpilled: Long,
    val diskBytesSpilled: Long,
    val shuffleFetchWaitTime: Long,
    val shuffleWriteTime: Long,
    val shuffleBytesWritten: Long,
    val shuffleBytesRead: Long,
    val inputBytesRead: Long,
    val outputBytesWritten: Long
    // ... other metrics as non-indexed fields
)
```

#### 2.2 Pre-compute Quantiles at Stage Completion

**File:** `core/src/main/scala/org/apache/spark/status/AppStatusListener.scala`

```scala
override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
  // ... existing code ...

  // For history replay with light storage, pre-compute quantiles
  if (!live && useLightStorage) {
    computeAndCacheQuantiles(stage.info.stageId, stage.info.attemptNumber)
  }
}

/**
 * Compute quantiles by iterating all tasks once, sort in memory, cache results.
 * This replaces index-based scanning when using light storage.
 */
private def computeAndCacheQuantiles(stageId: Int, stageAttemptId: Int): Unit = {
  val stageKey = Array(stageId, stageAttemptId)
  val tasks = KVUtils.viewToSeq(kvstore.view(classOf[TaskDataWrapperLight]).parent(stageKey))

  if (tasks.isEmpty) return

  val count = tasks.size
  val quantilesToCache = Seq(0.05, 0.25, 0.50, 0.75, 0.95)

  // Extract and sort metrics
  def computeQuantile(values: Seq[Long], q: Double): Double = {
    val sorted = values.sorted
    val idx = math.min((q * count).toInt, count - 1)
    sorted(idx).toDouble
  }

  // Collect metric arrays
  val durations = tasks.map(_.duration)
  val gcTimes = tasks.map(_.jvmGcTime)
  val memSpills = tasks.map(_.memoryBytesSpilled)
  val diskSpills = tasks.map(_.diskBytesSpilled)
  val shuffleFetchWaits = tasks.map(_.shuffleFetchWaitTime)
  val shuffleWriteTimes = tasks.map(_.shuffleWriteTime)
  // ... other metrics

  quantilesToCache.foreach { q =>
    val cached = new CachedQuantile(
      stageId = stageId,
      stageAttemptId = stageAttemptId,
      quantile = f"$q%.2f",
      taskCount = count,
      duration = computeQuantile(durations, q),
      jvmGcTime = computeQuantile(gcTimes, q),
      memoryBytesSpilled = computeQuantile(memSpills, q),
      diskBytesSpilled = computeQuantile(diskSpills, q),
      shuffleFetchWaitTime = computeQuantile(shuffleFetchWaits, q),
      shuffleWriteTime = computeQuantile(shuffleWriteTimes, q),
      // ... other fields
    )
    kvstore.write(cached)
  }

  logDebug(s"Pre-computed quantiles for stage $stageId.$stageAttemptId ($count tasks)")
}
```

#### 2.3 Modify AppStatusStore to Use Cached Quantiles

**File:** `core/src/main/scala/org/apache/spark/status/AppStatusStore.scala`

```scala
def taskSummary(...): Option[v1.TaskMetricDistributions] = {
  // ... existing cache lookup code ...

  // If using light storage and quantiles not cached, they should have been
  // pre-computed. Return what's available.
  if (useLightStorage) {
    // Try to return cached quantiles
    val cached = getCachedQuantiles(stageId, stageAttemptId, quantiles)
    if (cached.nonEmpty) {
      return Some(buildDistributionsFromCache(cached))
    }
    // Light storage: can't fall back to index scanning
    logWarning(s"Quantiles not pre-computed for stage $stageId.$stageAttemptId")
    return None
  }

  // Full storage: fall back to index-based scanning (existing behavior)
  // ... existing scanTasks code ...
}
```

---

## Optimization 3: Quantile-Based Task Filtering

### Design

After replay, filter tasks keeping only important ones based on pre-computed quantiles.

### Configuration

```scala
val TASK_FILTER_THRESHOLD = ConfigBuilder("spark.history.task.filterThreshold")
  .doc("Filter non-important tasks when stage exceeds this count. 0 to disable.")
  .version("4.0.0")
  .intConf
  .createWithDefault(100000)

val TASK_IMPORTANCE_QUANTILE = ConfigBuilder("spark.history.task.importanceQuantile")
  .doc("Keep tasks above this quantile. 0.5 = above median.")
  .version("4.0.0")
  .doubleConf
  .createWithDefault(0.5)
```

### Implementation

**File:** `core/src/main/scala/org/apache/spark/status/TaskImportanceFilter.scala`

```scala
private[spark] class TaskImportanceFilter(store: KVStore, quantile: Double) {

  // Key metrics for importance (use pre-computed quantiles as thresholds)
  private val keyMetrics = Seq("duration", "shuffleFetchWaitTime", "shuffleWriteTime",
    "memoryBytesSpilled", "diskBytesSpilled", "jvmGcTime")

  def filterStage(stageId: Int, stageAttemptId: Int): Int = {
    val stageKey = Array(stageId, stageAttemptId)
    val quantileKey = f"$quantile%.2f"

    // Get pre-computed quantile thresholds
    val cachedQuantile = store.read(classOf[CachedQuantile],
      Array(stageId, stageAttemptId, quantileKey))

    val thresholds = Map(
      "duration" -> cachedQuantile.duration,
      "shuffleFetchWaitTime" -> cachedQuantile.shuffleFetchWaitTime,
      "shuffleWriteTime" -> cachedQuantile.shuffleWriteTime,
      "memoryBytesSpilled" -> cachedQuantile.memoryBytesSpilled,
      "diskBytesSpilled" -> cachedQuantile.diskBytesSpilled,
      "jvmGcTime" -> cachedQuantile.jvmGcTime
    )

    // Iterate tasks (uses primary key, no special index needed)
    var deletedCount = 0
    store.view(classOf[TaskDataWrapperLight])
      .parent(stageKey)
      .iterator()
      .asScala
      .filterNot(isImportant(_, thresholds))
      .foreach { task =>
        store.delete(classOf[TaskDataWrapperLight], task.taskId)
        deletedCount += 1
      }

    deletedCount
  }

  private def isImportant(task: TaskDataWrapperLight, thresholds: Map[String, Double]): Boolean = {
    // Always keep failed/killed
    if (task.status == "FAILED" || task.status == "KILLED") return true

    // Keep if above threshold for ANY key metric
    task.duration > thresholds("duration") ||
      task.shuffleFetchWaitTime > thresholds("shuffleFetchWaitTime") ||
      task.shuffleWriteTime > thresholds("shuffleWriteTime") ||
      task.memoryBytesSpilled > thresholds("memoryBytesSpilled") ||
      task.diskBytesSpilled > thresholds("diskBytesSpilled") ||
      task.jvmGcTime > thresholds("jvmGcTime")
  }
}
```

---

## Monitoring & Metrics

**New file:** `core/src/main/scala/org/apache/spark/deploy/history/HistoryServerMetrics.scala`

```scala
private[history] class HistoryServerMetrics(prefix: String) extends Source {
  override val sourceName = "HistoryServer"
  override val metricRegistry = new MetricRegistry

  // Parse timing
  val eventLogParseTimer = new Timer()
  val quantileComputeTimer = new Timer()

  // Task stats
  val tasksProcessed = new Counter()
  val tasksFiltered = new Counter()
  val stagesProcessed = new Counter()

  // Current app gauges
  private val currentParseTimeMs = new AtomicLong(0)
  private val currentTasksTotal = new AtomicLong(0)
  private val currentTasksKept = new AtomicLong(0)

  def init(): Unit = {
    metricRegistry.register(s"$prefix.parse.timer", eventLogParseTimer)
    metricRegistry.register(s"$prefix.quantile.timer", quantileComputeTimer)
    metricRegistry.register(s"$prefix.tasks.processed", tasksProcessed)
    metricRegistry.register(s"$prefix.tasks.filtered", tasksFiltered)
  }
}
```

---

## Configuration Summary

```properties
# Optimization 1: Batched writes
spark.history.task.writeBatchSize = 1000

# Optimization 2: Light storage with pre-computed quantiles
spark.history.task.useLightStorage = true

# Optimization 3: Quantile-based filtering
spark.history.task.filterThreshold = 100000
spark.history.task.importanceQuantile = 0.5
```

---

## Files to Modify/Create

| File | Action | Description |
|------|--------|-------------|
| `internal/config/History.scala` | Modify | Add 4 config options |
| `status/AppStatusListener.scala` | Modify | Batching, pre-compute quantiles |
| `status/storeTypes.scala` | Modify | Add TaskDataWrapperLight |
| `status/LiveEntity.scala` | Modify | Support light wrapper |
| `status/AppStatusStore.scala` | Modify | Use cached quantiles for light storage |
| `status/TaskImportanceFilter.scala` | **Create** | Quantile-based filtering |
| `deploy/history/FsHistoryProvider.scala` | Modify | Post-replay filtering |
| `deploy/history/HistoryServerMetrics.scala` | **Create** | Metrics |

---

## Data Flow Summary

```
Event Log Replay:
  1. Parse events → buffer tasks → batch write TaskDataWrapperLight
  2. On stage completion → load all tasks → compute quantiles → save CachedQuantile
  3. After replay complete → for large stages → filter using cached quantile thresholds

UI Request (taskSummary):
  - Read pre-computed CachedQuantile directly (no index scan needed)
```

---

## Testing & Verification

### Unit Tests

1. **AppStatusListenerSuite**
   - Test batch flushing
   - Test quantile pre-computation at stage completion

2. **TaskDataWrapperLightSuite**
   - Test toApi() conversion
   - Verify indices work for basic sorting

3. **TaskImportanceFilterSuite**
   - Test failed tasks always kept
   - Test quantile-based filtering

### Integration Test

```bash
# Start with all optimizations
./sbin/start-history-server.sh \
  --conf spark.history.task.writeBatchSize=1000 \
  --conf spark.history.task.useLightStorage=true \
  --conf spark.history.task.filterThreshold=100000

# Expected logs:
# Pre-computed quantiles for stage 0.0 (500000 tasks)
# Stage 0.0: filtered 250000 of 500000 tasks
# Parsed app-xxx in 45000ms
```

### Performance Targets

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Parse time (1M tasks) | 120s | 40s | 66% |
| Index updates per task | 40+ | 7 | 82% |
| Memory usage | 8GB | 3GB | 62% |
| Stored tasks | 1M | 500K | 50% |
