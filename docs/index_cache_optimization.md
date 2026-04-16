# Index and Cache Optimization for Spark History Server

## Problem Statement

Current implementation writes 58+ indexes per task but only 3 are actively used for queries:

1. **Unnecessary index overhead**: 55+ indexes are written but never queried for sorting
2. **Quantile recalculation**: Quantiles recomputed on every first UI request instead of served from cache
3. **Write bottleneck**: Index updates slow down event log replay

## Current State Analysis

### TaskDataWrapper Index Usage

| Usage | Indexes | Count |
|-------|---------|-------|
| **Actually used** | STAGE (taskId), EXEC_RUN_TIME, COMPLETION_TIME | 3 |
| **Quantile scan only** | All metric fields (scanned, not indexed sort) | ~38 |
| **Never used** | Shuffle push, input/output, LOCALITY, etc. | ~55+ |

### Quantile Calculation

| Aspect | Current | Issue |
|--------|---------|-------|
| **Computation** | On first UI request | Slow first load |
| **Caching** | Lazily after first compute | Not during replay |
| **Storage** | CachedQuantile exists | Never triggered |

## Optimization 1: Reduce Index Writes

### Design

Keep only essential indexes, remove unused ones during write:

```scala
// storeTypes.scala - TaskDataWrapper
class TaskDataWrapper {
  // ESSENTIAL: For task lookup and parent relationship
  @Indexed
  val taskId: Long

  // ESSENTIAL: For UI runtime sort
  @Indexed
  val executorRunTime: Long

  // ESSENTIAL: For task cleanup and retention
  @Indexed
  val completionTime: Long

  // REMOVE @Indexed from unused fields:
  // - TASK_INDEX, ATTEMPT, TASK_PARTITION_ID
  // - LAUNCH_TIME, DURATION, EXECUTOR, HOST
  // - STATUS, LOCALITY, DESER_TIME, etc.
  // - All shuffle push merged indexes
  // - All input/output metric indexes

  // RETAIN as regular fields (for quantile scan):
  val executorDeserializeTime: Long
  val executorCpuTime: Long
  val jvmGcTime: Long
  val inputBytesRead: Long
  // ... all metrics (but not @Indexed)
}
```

### Implementation

1. **Remove @Indexed annotations** from unused fields in TaskDataWrapper
2. **Keep fields** as regular fields for quantile scanning
3. **Quantile calculation** uses scanTasks() which iterates all tasks anyway (no index needed)

### Config Option

```scala
object HistoryServerConf {
  val MINIMAL_INDEX_MODE = "spark.history.ui.minimalIndex.enabled"
  val DEFAULT_MINIMAL_INDEX = true
}
```

### Risk Assessment

| Risk | Level | Mitigation |
|------|-------|------------|
| UI sort broken | Low | Keep EXEC_RUN_TIME for runtime sort |
| Quantile broken | Low | Uses scan not index |
| Old apps broken | None | Read path unchanged |

## Optimization 2: Pre-compute Quantiles

### Design

Compute and cache quantiles during replay when stage completes:

```scala
// AppStatusListener.scala
class AppStatusListener(...) {

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    // ... existing code ...

    // NEW: Pre-compute quantiles for completed stage
    Option(liveStages.get((event.stageInfo.stageId, event.stageInfo.attemptNumber)))
      .foreach { stage =>
        if (stage.status == StageStatus.COMPLETE) {
          computeAndCacheQuantiles(stage.id, stage.attemptNumber)
        }
      }
  }

  private def computeAndCacheQuantiles(stageId: Int, attemptId: Int): Unit = {
    val QUANTILES = Seq(0.05, 0.10, 0.15, 0.20, 0.25, 0.30,
                     0.35, 0.40, 0.45, 0.50, 0.55, 0.60,
                     0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95)

    val stageKey = Array(stageId, attemptId)
    val tasks = scanTasks(stageId, attemptId, None)

    QUANTILES.foreach { q =>
      val cached = computeQuantileMetrics(tasks, q)
      store.write(cached)  // Write to KVStore, cached for UI
    }
  }
}
```

### Alternative: Post-replay in FsHistoryProvider

```scala
// FsHistoryProvider.scala
private[spark] def rebuildAppStore(...): Unit = {
  // ... existing replay ...

  // NEW: Post-process to compute quantiles
  postProcessQuantiles(store)
}

private def postProcessQuantiles(store: KVStore): Unit = {
  val completedStages = store.view(classOf[StageDataWrapper])
    .index(StageIndexNames.STATUS)
    .first(StageStatus.COMPLETED)
    .last(StageStatus.COMPLETED)
    .iterator()
    .asScala

  completedStages.foreach { stage =>
    computeAndCacheQuantiles(stage.stageId, stage.attemptId)
  }
}
```

### CachedQuantile Storage

Existing CachedQuantile class handles storage:

```scala
// storeTypes.scala - already exists
class CachedQuantile(
    stageId: Int,
    stageAttemptId: Int,
    quantile: Double,
    taskCount: Long,
    metrics: QuantileMetrics
) extends Serializable

class QuantileMetrics(
    duration: Array[Long],        // 14 executor metrics
    inputBytesRead: Array[Long],   // 2 input metrics
    outputBytesWritten: Array[Long], // 2 output metrics
    shuffleReadMetrics: Array[Long], // 18 shuffle read metrics
    shuffleWriteMetrics: Array[Long] // 3 shuffle write metrics
) extends Serializable
```

## Optimization 3: Cache-Aside Pattern for Quantiles

### Read Path

When UI requests task summary:

```scala
// AppStatusStore.scala - taskSummary()
def taskSummary(
    stageId: Int,
    stageAttemptId: Int,
    quantiles: Seq[Double]
): Option[TaskQuantileSummary] = {
  val stageKey = Array(stageId, stageAttemptId)

  // Try cache first
  val cached = quantiles.flatMap { q =>
    store.read(classOf[CachedQuantile], Array(stageId, stageAttemptId, quantileString(q)))
  }.toSeq

  if (cached.size == quantiles.size) {
    // All cached - return immediately
    Some(buildSummary(cached))
  } else {
    // Cache miss - compute on demand
    // (existing logic)
    computeAndCacheQuantiles(stageId, stageAttemptId)
    retryCacheRead()
  }
}
```

### Cache Invalidation

Invalidate cache when task count changes:

```scala
// AppStatusListener.scala
override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
  // Update task data

  // Invalidate quantile cache for this stage
  val stageKey = Array(event.stageId, event.stageAttemptId)
  invalidateQuantileCache(stageKey)
}
```

## Combined Flow

### During Replay

```
TaskEnd Event → AppStatusListener → update TaskDataWrapper (3 indexes)
                                    ↓
Stage Complete → computeAndCacheQuantiles() → write CachedQuantile (19 entries)
                                    ↓
UI Request → taskSummary() → CachedQuantile read → instant response
```

### Performance Comparison

| Aspect | Before | After |
|--------|--------|-------|
| Index writes per task | 58 | 3-5 |
| Index write time | ~60% replay | ~10% |
| First UI quantile | Recompute | Cache hit |
| Index storage | 58 × tasks | 3 × tasks |

## Configuration

```scala
object HistoryServerConf {
  // Index optimization
  val MINIMAL_INDEX_ENABLED = "spark.history.ui.minimalIndex.enabled"
  val DEFAULT_MINIMAL_INDEX = true

  // Quantile pre-computation
  val PRECOMPUTE_QUANTILES_ENABLED = "spark.history.precomputeQuantiles.enabled"
  val DEFAULT_PRECOMPUTE_QUANTILES = true

  // Quantile calculation metrics
  val QUANTILE_METRICS = "spark.history.ui.quantileMetrics"
  val DEFAULT_QUANTILE_METRICS = "executorRunTime,duration,shuffleRead,shuffleWrite"
}
```

## Backward Compatibility

### Old Event Logs

When loading old apps with full indexes:
- Read path unchanged (indexes used if present)
- Quantiles recomputed on first access (cache miss)

### New Event Logs

With minimal indexes:
- Write: 3-5 indexes
- Quantiles: Pre-computed during replay

### Config Switch

```scala
val useMinimalIndex = conf.get(MINIMAL_INDEX_ENABLED)
if (useMinimalIndex) {
  // Write minimal indexes
} else {
  // Write all 58 indexes (current behavior)
}
```

## Files to Modify

| File | Changes |
|------|---------|
| `core/.../status/storeTypes.scala` | Remove @Indexed from 55+ fields |
| `core/.../status/AppStatusListener.scala` | Add onStageCompleted quantile compute |
| `core/.../status/AppStatusStore.scala` | Cache-Aside read path |
| `core/.../deploy/history/FsHistoryProvider.scala` | Post-process hook |

## Testing Plan

### Unit Tests
- TaskDataWrapperIndexTest: Verify index count
- QuantilePrecomputeTest: Verify computed values match

### Integration Tests  
- LargeAppReplaySuite: 100k+ tasks replay time
- QuantileUICacheTest: First UI load time

## Estimated Performance Gain

| Metric | Before | After |
|--------|--------|-------|
| Replay time (100k tasks) | 100s | 40s |
| Index write overhead | 60% | 10% |
| First UI quantile load | 5s | 10ms |