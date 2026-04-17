# Spark History Server: Large Task Event Log Optimization

## Problem Statement

When Spark applications have millions of tasks, the History Server takes excessive time to process event logs due to:
1. Each task write updates 40+ KVStore indices
2. Individual task writes (no batching)
3. All tasks stored regardless of importance

## Solution Overview

Four complementary optimizations:

| Optimization | Impact | When Applied |
|--------------|--------|--------------|
| **1. Batched Task Writes** | Reduce KVStore overhead | During replay |
| **2. Reduced Index Set** | Faster writes, less memory | During replay |
| **3. Quantile-Based Filtering** | Keep only important tasks | After replay |
| **4. UI Load Optimization** | Reduce UI response/render time | During UI access |

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

# Optimization 4: UI load optimization
spark.ui.task.pageSize = 100
spark.ui.task.maxPageSize = 10000
spark.ui.task.renderLimit = 1000
spark.ui.timeline.taskThreshold = 50000
```

---

## Files to Modify/Create

| File | Action | Description |
|------|--------|-------------|
| `internal/config/History.scala` | Modify | Add 4 config options |
| `internal/config/UI.scala` | Modify | Add 4 UI config options |
| `status/AppStatusListener.scala` | Modify | Batching, pre-compute quantiles, task summary |
| `status/storeTypes.scala` | Modify | Add TaskDataWrapperLight, StageTaskSummary |
| `status/LiveEntity.scala` | Modify | Support light wrapper |
| `status/AppStatusStore.scala` | Modify | Lazy task construction, KVStore pagination |
| `status/api/v1/api.scala` | Modify | Add TaskDataLite class |
| `status/TaskImportanceFilter.scala` | **Create** | Quantile-based filtering |
| `status/api/v1/StagesResource.scala` | Modify | Pagination defaults, CSV export |
| `ui/jobs/StagePage.scala` | Modify | Optimized timeline, render limits |
| `deploy/history/FsHistoryProvider.scala` | Modify | Post-replay filtering |
| `deploy/history/HistoryServerMetrics.scala` | **Create** | Metrics |

---

## Data Flow Summary

```
Event Log Replay:
  1. Parse events → buffer tasks → batch write TaskDataWrapperLight
  2. On stage completion → load all tasks → compute quantiles → save CachedQuantile
  3. On stage completion → compute aggregate stats → save StageTaskSummary
  4. After replay complete → for large stages → filter using cached quantile thresholds

UI Request (taskSummary):
  - Read pre-computed CachedQuantile directly (no index scan needed)

UI Request (task list):
  - Apply default pagination (100 tasks) at API level
  - Use KVStore skip()/max() for efficient index traversal
  - Return TaskDataLite (no full toApi() deserialization)
  - Total count from StageTaskSummary (no iteration)

UI Request (timeline):
  - Check task count vs threshold (default 50K)
  - If below: use LAUNCH_TIME index for pre-sorted access, limit 1K
  - If above: show disabled message, skip generation

UI Rendering:
  - Limit rendered rows (default 1000)
  - CSV export endpoint for full data access (streaming, no buffering)
```

---

## Optimization 4: UI Load Latency Reduction

### Problem

Even with replay optimizations, the Spark UI can be slow when displaying stages with large task counts.

#### Root Cause Analysis

| Bottleneck | Location | Impact | Description |
|------------|----------|--------|-------------|
| **Task deserialization** | `AppStatusStore.scala:822-845` | 40% | `toApi()` called on every task, deserializes all metrics |
| **Executor log fetching** | `AppStatusStore.scala:828` | 10% | KVStore read per unique executor in `constructTaskDataList()` |
| **Quantile computation** | `AppStatusStore.scala:253-531` | 30% | 20+ full table scans, one per metric index |
| **Timeline generation** | `StagePage.scala:249-386` | 15% | Sorts ALL tasks, heavy string interpolation per task |
| **Computed fields** | `AppStatusUtils` | 5% | `schedulerDelay()`, `gettingResultTime()` per task |

#### Code Evidence

**1. Per-task deserialization in constructTaskDataList (AppStatusStore.scala:822-845):**
```scala
def constructTaskDataList(taskDataWrapperIter: Iterable[TaskDataWrapper]): Seq[v1.TaskData] = {
  taskDataWrapperIter.map { taskDataWrapper =>
    val taskDataOld: v1.TaskData = taskDataWrapper.toApi  // Expensive deserialization
    val executorLogs = executorIdToLogs.getOrElseUpdate(taskDataOld.executorId, {
      executorSummary(taskDataOld.executorId).executorLogs  // KVStore read per executor
    })
    // ... creates new TaskData with computed fields
  }.toSeq
}
```

**2. Quantile computation with 20+ scans (AppStatusStore.scala:350-375):**
```scala
// Comment in code: "It's also slow, especially with disk stores"
def scanTasks(index: String)(fn: TaskDataWrapper => Long): IndexedSeq[Double] = {
  store.view(classOf[TaskDataWrapper])
    .parent(stageKey)
    .index(index)  // One full scan per metric index
    .closeableIterator()
  // ... iterates all tasks
}
```

**3. Timeline sorts ALL tasks before taking top N (StagePage.scala:266):**
```scala
tasks.sortBy(-_.launchTime.getTime()).take(MAX_TIMELINE_TASKS).map { ... }
// Sorts entire dataset even when only taking 1000 tasks
```

### Design

Six sub-optimizations targeting the actual bottlenecks:

| Sub-Optimization | Target Bottleneck | Impact |
|------------------|-------------------|--------|
| **4.1 Lazy Task Construction** | Task deserialization | 40% faster |
| **4.2 KVStore-Level Pagination** | Iteration overhead | 30% faster |
| **4.3 Pre-computed Quantiles** | Quantile scans | Already in Opt 2 |
| **4.4 Optimized Timeline** | Timeline generation | 15% faster |
| **4.5 Stage Task Summary Cache** | Aggregate queries | 5% faster |
| **4.6 API Pagination Defaults** | Response size | Prevents worst case |

---

### 4.1 Lazy Task Construction

Avoid expensive `toApi()` deserialization until data is actually needed.

#### Problem

Current `constructTaskDataList()` eagerly deserializes ALL task metrics even when UI only displays a subset of columns.

#### Implementation

**File:** `core/src/main/scala/org/apache/spark/status/AppStatusStore.scala`

```scala
/**
 * Lightweight task data for list display. Only deserializes essential fields.
 * Full metrics loaded on-demand when user expands task details.
 */
def taskListLite(
    stageId: Int,
    stageAttemptId: Int,
    offset: Int,
    length: Int,
    sortBy: Option[String],
    descending: Boolean): Seq[v1.TaskDataLite] = {

  val stageKey = Array(stageId, stageAttemptId)

  // Use KVStore skip/max for true pagination (see 4.2)
  val view = store.view(classOf[TaskDataWrapper])
    .parent(stageKey)
    .index(sortBy.getOrElse(TaskIndexNames.TASK_INDEX))
    .reverse(descending)
    .skip(offset)
    .max(length)

  // Only extract fields needed for table display - NO full toApi()
  KVUtils.viewToSeq(view).map { wrapper =>
    new v1.TaskDataLite(
      taskId = wrapper.taskId,
      index = wrapper.index,
      attempt = wrapper.attempt,
      launchTime = wrapper.launchTime,
      duration = wrapper.duration,
      status = wrapper.status,
      executorId = wrapper.executorId,
      host = wrapper.host,
      // Omit: full metrics, executor logs, computed fields
      hasMetrics = true  // Flag to indicate full data available
    )
  }
}

/**
 * Full task data for single task detail view.
 * Called only when user clicks on a specific task.
 */
def taskData(taskId: Long): Option[v1.TaskData] = {
  asOption(store.read(classOf[TaskDataWrapper], taskId)).map { wrapper =>
    constructTaskData(wrapper)  // Full deserialization for single task
  }
}
```

#### New Lite Data Class

**File:** `core/src/main/scala/org/apache/spark/status/api/v1/api.scala`

```scala
/**
 * Lightweight task data for list views.
 * Contains only fields displayed in task table columns.
 */
class TaskDataLite(
    val taskId: Long,
    val index: Int,
    val attempt: Int,
    val launchTime: Date,
    val duration: Long,
    val status: String,
    val executorId: String,
    val host: String,
    val hasMetrics: Boolean  // True if full metrics available via taskData()
)
```

---

### 4.2 KVStore-Level Pagination

Apply skip/max at KVStore level to avoid iterating skipped records.

#### Problem

Current pagination fetches all tasks then slices in memory:
```scala
// Bad: Fetches 100K tasks, returns 100
KVUtils.viewToSeq(view).slice(offset, offset + length)
```

#### Implementation

**File:** `core/src/main/scala/org/apache/spark/status/AppStatusStore.scala`

```scala
def taskList(
    stageId: Int,
    stageAttemptId: Int,
    offset: Int,
    length: Int,
    sortBy: Option[String],
    descending: Boolean): Seq[v1.TaskData] = {

  val stageKey = Array(stageId, stageAttemptId)

  // Good: KVStore handles skip/max efficiently via index
  val view = store.view(classOf[TaskDataWrapper])
    .parent(stageKey)
    .index(sortBy.getOrElse(TaskIndexNames.TASK_INDEX))
    .reverse(descending)
    .skip(offset)   // Skip at KVStore level
    .max(length)    // Limit at KVStore level

  constructTaskDataList(KVUtils.viewToSeq(view))
}
```

**Key Change:** `skip()` and `max()` are applied to the KVStore view BEFORE iteration, not after collecting to Seq.

---

### 4.3 Pre-computed Quantiles

**Already addressed in Optimization 2.** The quantile computation bottleneck (20+ full table scans) is solved by pre-computing quantiles at stage completion.

Cross-reference: See "Optimization 2: Reduced Index Set + Pre-computed Quantiles"

---

### 4.4 Optimized Timeline Generation

Avoid sorting all tasks when only top N are needed.

#### Problem

```scala
// Current: O(n log n) sort of ALL tasks, then take 1000
tasks.sortBy(-_.launchTime.getTime()).take(MAX_TIMELINE_TASKS)
```

#### Implementation

**File:** `core/src/main/scala/org/apache/spark/ui/jobs/StagePage.scala`

```scala
private def makeTimeline(stageId: Int, stageAttemptId: Int): NodeSeq = {
  // Use KVStore index for pre-sorted access - O(k) where k = MAX_TIMELINE_TASKS
  val timelineTasks = store.taskList(
    stageId,
    stageAttemptId,
    offset = 0,
    length = MAX_TIMELINE_TASKS,
    sortBy = Some(TaskIndexNames.LAUNCH_TIME),
    descending = true  // Most recent first
  )

  // Build timeline from already-sorted, limited set
  buildTimelineJson(timelineTasks)
}

private def buildTimelineJson(tasks: Seq[TaskData]): String = {
  // Use StringBuilder for efficient concatenation
  val sb = new StringBuilder(tasks.size * 500)  // Pre-size estimate
  sb.append('[')

  var first = true
  tasks.foreach { task =>
    if (!first) sb.append(',')
    first = false

    // Inline JSON building without regex replaceAll
    sb.append(s"""{"className":"task task-assignment-timeline-object",""")
    sb.append(s""""group":"${task.executorId}",""")
    sb.append(s""""start":${task.launchTime.getTime},""")
    sb.append(s""""end":${task.launchTime.getTime + task.duration}}""")
  }

  sb.append(']')
  sb.toString()
}
```

#### Alternative: Disable Timeline for Large Stages

```scala
val TIMELINE_TASK_THRESHOLD = ConfigBuilder("spark.ui.timeline.taskThreshold")
  .doc("Disable timeline visualization when stage has more tasks than this. Default 50000.")
  .version("4.0.0")
  .intConf
  .createWithDefault(50000)

private def makeTimeline(...): NodeSeq = {
  val taskCount = store.taskCount(stageId, stageAttemptId)
  if (taskCount > conf.get(TIMELINE_TASK_THRESHOLD)) {
    <div class="alert alert-info">
      Timeline disabled for stages with {taskCount} tasks (threshold: {conf.get(TIMELINE_TASK_THRESHOLD)}).
    </div>
  } else {
    // ... generate timeline
  }
}
```

---

### 4.5 Stage Task Summary Cache

Cache aggregated task statistics per stage to avoid repeated computation.

#### Data Structure

**File:** `core/src/main/scala/org/apache/spark/status/storeTypes.scala`

```scala
/**
 * Cached summary statistics for a stage's tasks.
 * Computed once at stage completion, avoids repeated aggregation.
 */
@KVIndexParam(value = "id", copy = true)
private[spark] class StageTaskSummary(
    val stageId: Int,
    val stageAttemptId: Int,

    // Aggregate counts
    val totalTasks: Int,
    val completedTasks: Int,
    val failedTasks: Int,
    val killedTasks: Int,
    val runningTasks: Int,

    // Aggregate metrics
    val totalDuration: Long,
    val totalGcTime: Long,
    val totalInputBytes: Long,
    val totalOutputBytes: Long,
    val totalShuffleReadBytes: Long,
    val totalShuffleWriteBytes: Long,
    val totalMemorySpilled: Long,
    val totalDiskSpilled: Long,

    // Min/Max for quick stats
    val minDuration: Long,
    val maxDuration: Long,
    val minLaunchTime: Long,
    val maxCompletionTime: Long
) {
  @JsonIgnore @KVIndex("id")
  def id: Array[Int] = Array(stageId, stageAttemptId)
}
```

#### Implementation

**File:** `core/src/main/scala/org/apache/spark/status/AppStatusListener.scala`

```scala
override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
  // ... existing code ...

  // Compute and cache task summary (both live and history modes)
  if (!live || cacheTaskSummary) {
    computeAndCacheTaskSummary(stage.info.stageId, stage.info.attemptNumber)
  }
}

private def computeAndCacheTaskSummary(stageId: Int, stageAttemptId: Int): Unit = {
  val stageKey = Array(stageId, stageAttemptId)
  val taskClass = if (useLightStorage) classOf[TaskDataWrapperLight]
                  else classOf[TaskDataWrapper]

  var total, completed, failed, killed, running = 0
  var totalDuration, totalGcTime = 0L
  var totalInput, totalOutput, totalShuffleRead, totalShuffleWrite = 0L
  var totalMemSpill, totalDiskSpill = 0L
  var minDuration = Long.MaxValue
  var maxDuration = 0L
  var minLaunch = Long.MaxValue
  var maxCompletion = 0L

  val iter = kvstore.view(taskClass).parent(stageKey).iterator()
  while (iter.hasNext) {
    val task = iter.next()
    total += 1

    task.status match {
      case "SUCCESS" => completed += 1
      case "FAILED" => failed += 1
      case "KILLED" => killed += 1
      case "RUNNING" => running += 1
      case _ =>
    }

    totalDuration += task.duration
    totalGcTime += task.jvmGcTime
    totalInput += task.inputBytesRead
    totalOutput += task.outputBytesWritten
    totalShuffleRead += task.shuffleBytesRead
    totalShuffleWrite += task.shuffleBytesWritten
    totalMemSpill += task.memoryBytesSpilled
    totalDiskSpill += task.diskBytesSpilled

    minDuration = math.min(minDuration, task.duration)
    maxDuration = math.max(maxDuration, task.duration)
    minLaunch = math.min(minLaunch, task.launchTime)
    if (task.completionTime > 0) {
      maxCompletion = math.max(maxCompletion, task.completionTime)
    }
  }

  val summary = new StageTaskSummary(
    stageId, stageAttemptId,
    total, completed, failed, killed, running,
    totalDuration, totalGcTime,
    totalInput, totalOutput, totalShuffleRead, totalShuffleWrite,
    totalMemSpill, totalDiskSpill,
    if (minDuration == Long.MaxValue) 0 else minDuration, maxDuration,
    if (minLaunch == Long.MaxValue) 0 else minLaunch, maxCompletion
  )

  kvstore.write(summary)
  logDebug(s"Cached task summary for stage $stageId.$stageAttemptId ($total tasks)")
}
```

#### Usage in AppStatusStore

**File:** `core/src/main/scala/org/apache/spark/status/AppStatusStore.scala`

```scala
def stageTaskSummary(stageId: Int, stageAttemptId: Int): Option[StageTaskSummary] = {
  try {
    Some(store.read(classOf[StageTaskSummary], Array(stageId, stageAttemptId)))
  } catch {
    case _: NoSuchElementException => None
  }
}

// Use in stageData() to avoid re-aggregating
def stageData(stageId: Int, stageAttemptId: Int, details: Boolean): v1.StageData = {
  val stageInfo = store.read(classOf[StageDataWrapper], ...)

  // Use cached summary if available
  val taskSummary = stageTaskSummary(stageId, stageAttemptId)

  val numTasks = taskSummary.map(_.totalTasks).getOrElse(
    // Fallback: count via index (expensive)
    store.count(classOf[TaskDataWrapper], TaskIndexNames.STAGE, ...)
  )
  // ... use taskSummary for other aggregates
}
```

---

### 4.6 API Pagination Defaults

Add default limits to task-fetching APIs to prevent unbounded responses.

#### Configuration

```scala
val UI_TASK_PAGE_SIZE = ConfigBuilder("spark.ui.task.pageSize")
  .doc("Default page size for task list APIs. Default 100.")
  .version("4.0.0")
  .intConf
  .createWithDefault(100)

val UI_TASK_MAX_PAGE_SIZE = ConfigBuilder("spark.ui.task.maxPageSize")
  .doc("Maximum allowed page size for task APIs. Default 10000.")
  .version("4.0.0")
  .intConf
  .createWithDefault(10000)

val UI_TASK_RENDER_LIMIT = ConfigBuilder("spark.ui.task.renderLimit")
  .doc("Maximum tasks to render in UI tables. Default 1000.")
  .version("4.0.0")
  .intConf
  .createWithDefault(1000)

val TIMELINE_TASK_THRESHOLD = ConfigBuilder("spark.ui.timeline.taskThreshold")
  .doc("Disable timeline for stages exceeding this task count. Default 50000.")
  .version("4.0.0")
  .intConf
  .createWithDefault(50000)
```

#### REST API CSV Export

For users needing all task data, provide efficient streaming CSV export.

**File:** `core/src/main/scala/org/apache/spark/status/api/v1/StagesResource.scala`

```scala
@GET
@Path("{stageId}/{stageAttemptId}/taskList.csv")
@Produces(Array("text/csv"))
def taskListCsv(
    @PathParam("stageId") stageId: Int,
    @PathParam("stageAttemptId") stageAttemptId: Int): StreamingOutput = {

  new StreamingOutput {
    override def write(output: OutputStream): Unit = {
      val writer = new PrintWriter(output)
      writer.println("taskId,index,attempt,launchTime,duration,status,executor,...")

      // Stream tasks directly without buffering - no toApi() overhead
      val iter = store.view(classOf[TaskDataWrapper])
        .parent(Array(stageId, stageAttemptId))
        .iterator()

      while (iter.hasNext) {
        val task = iter.next()
        writer.println(s"${task.taskId},${task.index},${task.attempt}," +
          s"${task.launchTime},${task.duration},${task.status},${task.executorId},...")
      }
      writer.flush()
    }
  }
}
```

---

### UI Optimization Summary

```
Before (100K tasks stage):
  Stage page load:
    1. taskList() → iterate 100K tasks → toApi() each → executor log fetch → 5-10s
    2. taskSummary() → 20 metric scans × 100K tasks → 15-30s
    3. makeTimeline() → sort 100K tasks → string build 1K → 2-5s
    Total: 20-45s

After:
  Stage page load:
    1. taskListLite() → skip/max at KVStore → 100 tasks → no toApi() → 50ms
    2. taskSummary() → read CachedQuantile → 1ms
    3. stageTaskSummary() → read StageTaskSummary → 1ms
    4. makeTimeline() → use index sort → limit 1K → StringBuilder → 100ms
    Total: ~200ms

Key optimizations by bottleneck:
  - Task deserialization (40%) → Lazy construction, TaskDataLite
  - Quantile computation (30%) → Pre-computed at stage completion (Opt 2)
  - Timeline generation (15%) → Index-based sort, disable for large stages
  - Aggregates (5%) → StageTaskSummary cache
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

4. **StageTaskSummarySuite**
   - Test summary computation accuracy
   - Test summary caching and retrieval

5. **StagesResourceSuite**
   - Test pagination defaults applied
   - Test max page size enforced
   - Test CSV export streaming

### Integration Test

```bash
# Start with all optimizations
./sbin/start-history-server.sh \
  --conf spark.history.task.writeBatchSize=1000 \
  --conf spark.history.task.useLightStorage=true \
  --conf spark.history.task.filterThreshold=100000 \
  --conf spark.ui.task.pageSize=100 \
  --conf spark.ui.task.renderLimit=1000

# Expected logs:
# Pre-computed quantiles for stage 0.0 (500000 tasks)
# Cached task summary for stage 0.0 (500000 tasks)
# Stage 0.0: filtered 250000 of 500000 tasks
# Parsed app-xxx in 45000ms
```

### UI Load Test

```bash
# Measure task list API response time
time curl "http://localhost:18080/api/v1/applications/app-xxx/stages/0/0/taskList"
# Expected: < 100ms (returns 100 tasks by default)

# Measure with pagination
time curl "http://localhost:18080/api/v1/applications/app-xxx/stages/0/0/taskList?offset=0&length=1000"
# Expected: < 500ms

# CSV export for full data
time curl "http://localhost:18080/api/v1/applications/app-xxx/stages/0/0/taskList.csv" > tasks.csv
# Streams all tasks, memory-efficient
```

### Performance Targets

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Parse time (1M tasks) | 120s | 40s | 66% |
| Index updates per task | 40+ | 7 | 82% |
| Memory usage | 8GB | 3GB | 62% |
| Stored tasks | 1M | 500K | 50% |
| Stage page load (1M tasks) | 15s | 200ms | 98% |
| Task list API response | 8s | 50ms | 99% |
| Task table render time | 10s | 100ms | 99% |
