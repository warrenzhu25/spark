# Batch Write Optimization for Spark History Server

## Problem Statement

When processing large event logs with many tasks, the Spark History Server suffers from slow replay times:

1. **Write bottleneck**: Each task triggers 4-6 synchronous writes to KVStore with 58+ index updates
2. **Sequential processing**: No parallelism in event parsing or writing
3. **Lazy computation**: Quantiles calculated on first UI request, not during replay

## Current Architecture

### Event Flow During Replay

```
EventLogFileReader
       ↓
ReplayListenerBus.parseEvents()  [single-threaded]
       ↓
AppStatusListener.onTaskEnd()    [per-event callback]
       ↓
ElementTrackingStore.write()       [synchronous write]
       ↓
KVStore (LevelDB/RocksDB)       [58+ index updates per write]
```

### Current Write Pattern

```scala
// Per task event: 4-6 individual writes
override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
  // Write 1: TaskDataWrapper with 58 indexes
  update(task, now, last = true)

  // Write 2: StageDataWrapper
  update(stage, now, last = true)

  // Write 3: JobDataWrapper
  update(job, now)

  // Write 4: ExecutorStageSummaryWrapper
  update(esummary, now)

  // Write 5: ExecutorSummaryWrapper
  update(exec, now)
}
```

## Batch Write Design

### Core Idea

Accumulate writes in a buffer and flush in batches instead of per-event synchronous writes.

### Architecture with Batch Write

```
EventLogFileReader
       ↓
ReplayListenerBus.parseEvents()
       ↓
AppStatusListener.onTaskEnd()    [buffer to queue]
       ↓
BatchWriteManager               [background flusher]
       ↓
KVStore.writeBatch()            [batched index updates]
```

### Implementation Components

#### 1. BatchWriteBuffer

In-memory buffer that accumulates writes until batch size threshold.

```scala
class BatchWriteBuffer(
    maxBatchSize: Int = 1000,
    maxFlushIntervalMs: Long = 100
) {
  private val buffer = new java.util.ArrayList[Any](maxBatchSize)
  private val flushTimer = new ScheduledThreadPoolExecutor(1)

  def add(value: Any): Unit = synchronized {
    buffer.add(value)
    if (buffer.size >= maxBatchSize) {
      flushAsync()
    }
  }

  def flushAsync(): Unit = {
    flushTimer.submit(() => flush())
  }

  def flush(): Unit = synchronized {
    if (buffer.isEmpty) return
    val batch = new java.util.ArrayList[Any](buffer)
    buffer.clear()
    store.writeBatch(batch)
  }
}
```

#### 2. BatchWriteManager

Manages multiple buffers and coordinates flushing.

```scala
class BatchWriteManager(store: KVStore, conf: SparkConf) {
  private val buffers = Map[Class[_], BatchWriteBuffer](
    classOf[TaskDataWrapper] -> new BatchWriteBuffer(...),
    classOf[StageDataWrapper] -> new BatchWriteBuffer(...),
    classOf[JobDataWrapper] -> new BatchWriteBuffer(...)
  )

  def write[T](value: T): Unit = {
    val buffer = buffers.get(value.getClass)
    buffer.foreach(_.add(value))
  }

  def flushAll(): Unit = {
    buffers.values.foreach(_.flush())
  }
}
```

#### 3. KVStore Batch Write Support

Extend KVStore interface for batch operations.

```scala
trait KVStore {
  def write(value: Any): Unit
  def writeBatch(values: java.util.Collection[_]): Unit  // NEW
  def update(value: Any): Unit = write(value)
  def updateBatch(values: java.util.Collection[_]): Unit = writeBatch(values)
}
```

For LevelDB/RocksDB backends, batch write uses native atomic batch:

```scala
// LevelDB implementation
override def writeBatch(values: java.util.Collection[_]): Unit = {
  val batch = new WriteBatch()
  values.forEach { value =>
    batch.put(key(value), serialize(value))
  }
  db.write(batch)  // Single I/O operation
}
```

#### 4. Integration with AppStatusListener

Modify existing listener to use batch writes:

```scala
class AppStatusListener(
    store: KVStore,
    conf: SparkConf,
    live: Boolean
) {
  // Batch write manager
  private val batchManager = new BatchWriteManager(store, conf)

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    val task = processTaskEnd(event)
    // Use batch write instead of direct write
    batchManager.write(task)
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    // Flush all pending writes before stage completes
    batchManager.flushAll()
    // ... rest of existing logic
  }

  // Ensure cleanup on replay completion
  def close(): Unit = {
    batchManager.flushAll()
  }
}
```

#### 5. Flushing Strategies

Multiple strategies to ensure data consistency:

| Strategy | When | Use Case |
|----------|------|----------|
| **Size-based** | Buffer reaches maxBatchSize | Normal operation |
| **Time-based** | maxFlushIntervalMs elapsed | Prevent staleness |
| **Event-based** | Stage/Job completion events | Data consistency |
| **Shutdown** | Listener close | Ensure all written |

## Quantile Pre-Computation

### Design

Compute and cache quantiles during replay when stage completes, not on first UI request.

### Implementation

```scala
class QuantileComputer(store: KVStore) {
  private val STANDARD_QUANTILES = Seq(
    0.05, 0.10, 0.15, 0.20, 0.25, 0.30,
    0.35, 0.40, 0.45, 0.50, 0.55, 0.60,
    0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95
  )

  def computeAndCache(stageId: Int, stageAttemptId: Int): Unit = {
    val taskCount = store.view(classOf[TaskDataWrapper])
      .parent(stageKey(stageId, stageAttemptId))
      .iterator()
      .asInstanceOf[CloseableIterator[TaskDataWrapper]]
      .asScala
      .toSeq
      .size

    STANDARD_QUANTILES.foreach { q =>
      val metrics = computeQuantilesForAllMetrics(taskData, q)
      val cached = new CachedQuantile(
        stageId,
        stageAttemptId,
        q,
        taskCount,
        metrics
      )
      store.write(cached)
    }
  }

  private def computeQuantilesForAllMetrics(
      tasks: Seq[TaskDataWrapper],
      quantile: Double): Array[Long] = {
    // Compute 39 metrics for given quantile
    // executorRunTime, shuffleReadBytes, etc.
  }
}
```

### Integration Point

In FsHistoryProvider after replay completes:

```scala
// In FsHistoryProvider.scala
private[spark] def rebuildAppStore(...): Unit = {
  // ... existing replay logic ...

  // After replay, compute quantiles for all completed stages
  val stages = store.view(classOf[StageDataWrapper])
    .index(StageIndexNames.COMPLETED_TIME)
    .iterator()
    .asScala

  stages.foreach { stage =>
    quantileComputer.computeAndCache(stage.stageId, stage.attemptId)
  }
}
```

## Combined Optimization

### Flow After Optimization

```
EventLogFileReader
       ↓
ReplayListenerBus.parseEvents()   [parallel parsing - separate effort]
       ↓
AppStatusListener.onTaskEnd()   [buffer to batch]
       ↓
BatchWriteManager               [batch aggregation]
       ↓ (async flush)
KVStore.writeBatch()            [single I/O per batch]
       ↓
Stage Completed? → computeAndCacheQuantiles() → write CachedQuantile
```

### Performance Comparison

| Aspect | Before | After (with batch) |
|--------|--------|-------------------|
| Writes per task | 4-6 | 1 (batched) |
| Index updates per task | 58 | 58 (but batched) |
| I/O operations | O(tasks × 4) | O(tasks / batchSize) |
| Quantile first UI load | Recompute | Cache hit |
| Memory overhead | Low | Batch buffer |

## Configuration

```scala
object BatchWriteConfig {
  val BATCH_SIZE = "spark.history.store.batchSize"
  val FLUSH_INTERVAL_MS = "spark.history.store.flushIntervalMs"
  val ENABLED = "spark.history.store.batchWrite.enabled"
  val PRECOMPUTE_QUANTILES = "spark.history.precomputeQuantiles.enabled"

  // Defaults
  val DEFAULT_BATCH_SIZE = 1000
  val DEFAULT_FLUSH_INTERVAL_MS = 100
}
```

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Data loss on crash | High | Flush on stage completion events |
| Memory pressure | Medium | Bounded batch size |
| Ordering issues | Low | Event order preserved within batch |
| Backward compatibility | Low | Configurable, defaults to disabled |

## Testing Plan

### Unit Tests

1. **BatchWriteBufferTest**: Verify batch accumulation and flushing
2. **BatchWriteManagerTest**: Verify multiple buffer types
3. **QuantileComputerTest**: Verify pre-computed values match on-demand

### Integration Tests

1. **FsHistoryProviderBatchWriteSuite**: Large app replay with batch writes
2. **EndToEndQuantileTest**: Verify cache hit after replay

## Files to Modify

| File | Changes |
|------|---------|
| `core/src/main/scala/org/apache/spark/status/buffer/BatchWriteBuffer.scala` | NEW |
| `core/src/main/scala/org/apache/spark/status/buffer/BatchWriteManager.scala` | NEW |
| `core/src/main/scala/org/apache/spark/util/collection/KVStore.scala` | Add writeBatch |
| `core/src/main/scala/org/apache/spark/deploy/history/FsHistoryProvider.scala` | Integrate batch write |
| `core/src/main/scala/org/apache/spark/status/AppStatusListener.scala` | Use batch write |
| `core/.../status/QuantileComputer.scala` | NEW |