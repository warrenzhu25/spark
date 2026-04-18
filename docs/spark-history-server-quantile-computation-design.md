# Spark History Server: Replay-Time Quantile Computation Design

## Summary

The current Spark History Server quantile path is expensive because quantiles are computed on demand by
scanning persisted task rows through many metric-specific KVStore indices. This is slow for large event
logs and forces task storage to retain a wide set of secondary indices that exist mainly to support
quantile calculation.

This design moves quantile computation into history replay:

1. Keep a per-stage in-memory quantile accumulator while replaying task end events.
2. Update the accumulator as each eligible task is parsed.
3. Finalize quantiles once when the stage completes.
4. Persist only `CachedQuantile` rows to KVStore.
5. Remove metric-specific task indices that are only needed for on-demand quantile scans.

This keeps the query path fast, reduces KVStore write amplification, and avoids re-reading task rows to
compute stage summaries.

## Current Problem

Today, `AppStatusStore.taskSummary()` computes quantiles by:

1. Counting valid tasks by scanning one metric index.
2. Looking up cached quantiles if available.
3. Falling back to scanning multiple metric-specific indices if any requested quantile is missing.
4. Writing `CachedQuantile` after the expensive computation has already happened.

This has several drawbacks:

- The first quantile request for a stage is expensive.
- Disk-backed stores pay repeated iterator and deserialization costs.
- Task rows need many secondary indices purely for summary computations.
- Query latency depends on the number of retained tasks.

## Proposed Design

### Core Approach

Quantiles should be computed during event log replay instead of at UI query time.

For each stage attempt:

1. Create an in-memory accumulator when the first eligible task completes.
2. Update the accumulator from each successful task end event.
3. When `SparkListenerStageCompleted` arrives, finalize the quantiles.
4. Persist the results as `CachedQuantile`.
5. Drop the accumulator from memory.

The UI and API continue to read `CachedQuantile` through `AppStatusStore.taskSummary()`, but the costly
fallback scan becomes unnecessary in the optimized path.

### Scope

This design applies to History Server replay (`!live`) first.

It does not change the existing live UI retention and cleanup model. Live mode currently deletes old
tasks when retention thresholds are exceeded, so replay-only is the safer and simpler first target.

## Architecture

### New Replay-Time Accumulator

Add a per-stage in-memory quantile accumulator in `AppStatusListener`, keyed by:

```scala
(stageId: Int, stageAttemptId: Int)
```

Suggested shape:

```scala
private val replayStageQuantiles =
  new ConcurrentHashMap[(Int, Int), StageQuantileAccumulator]()
```

Introduce an internal accumulator contract:

```scala
private[spark] trait StageQuantileAccumulator {
  def add(task: TaskDataWrapper): Unit
  def taskCount: Int
  def snapshot(quantiles: Array[Double]): QuantileSnapshot
}
```

`QuantileSnapshot` should hold all values needed to materialize `CachedQuantile` rows for the requested
quantiles.

### Event Flow

#### `onTaskEnd`

During replay, after task metrics have been finalized:

- Build or access the accumulator for `(stageId, stageAttemptId)`.
- Add the task's metric values if the task is eligible for quantile computation.

Eligibility must match current summary semantics:

- Include successful tasks with valid metrics.
- Exclude failed, killed, and resubmitted tasks.
- Preserve the current behavior where non-successful task metrics do not contribute to summary
  quantiles.

#### `onStageCompleted`

When the stage completes:

- Read the accumulator for the stage attempt.
- Compute the cached quantiles once.
- Write `CachedQuantile` rows to KVStore.
- Remove the accumulator from memory.

This guarantees stable quantiles for completed history stages without any later full task scan.

## Exact vs Approximate Modes

### Recommended v1: Exact In-Memory Accumulation

For a first implementation, use exact quantiles with primitive in-memory arrays.

Each accumulator stores one growable primitive `long` buffer per metric. On stage completion:

1. Trim the buffers to arrays.
2. Sort each array once.
3. Read quantile positions directly.
4. Materialize `CachedQuantile`.

This keeps behavior equivalent to the current exact scan-based computation while removing the expensive
store re-scan.

Recommended internal utility:

```scala
private class LongVector(initialCapacity: Int = 1024) {
  private var data = new Array[Long](initialCapacity)
  private var size = 0

  def add(v: Long): Unit = { ... }
  def size: Int = size
  def toTrimmedArray(): Array[Long] = { ... }
}
```

This avoids boxing and reduces memory overhead compared with `ArrayBuffer[Long]`.

### Future Option: Approximate Quantiles with Sketches

For very large stages, exact per-task value retention may consume too much memory. A future extension can
replace per-metric arrays with streaming quantile sketches such as:

- KLL
- Greenwald-Khanna
- t-digest-like structures where appropriate

Benefits:

- Bounded memory per metric
- One-pass update during replay
- No need to retain every task value

Tradeoff:

- Quantiles become approximate instead of exact

This is a strong option for very large stages, but exact mode is simpler and lower-risk for an initial
implementation.

## Metrics Covered

The replay-time accumulator should cover the same values currently written into `CachedQuantile`, so the
external behavior of `taskSummary()` remains unchanged.

Metrics to accumulate:

- `duration`
- `executorDeserializeTime`
- `executorDeserializeCpuTime`
- `executorRunTime`
- `executorCpuTime`
- `resultSize`
- `jvmGcTime`
- `resultSerializationTime`
- `gettingResultTime`
- `schedulerDelay`
- `peakExecutionMemory`
- `memoryBytesSpilled`
- `diskBytesSpilled`
- input bytes and records
- output bytes and records
- shuffle read bytes, records, blocks, fetch wait, remote read bytes, read-to-disk bytes
- shuffle remote request duration
- shuffle push read metrics
- shuffle write bytes, records, and time

Derived values should be computed once when the task is added to the accumulator, not recomputed later.

Examples:

- `shuffleReadBytes = shuffleLocalBytesRead + shuffleRemoteBytesRead`
- `shuffleTotalBlocksFetched = shuffleLocalBlocksFetched + shuffleRemoteBlocksFetched`
- `gettingResultTime` from task timing fields
- `schedulerDelay` from the same utility used by current API conversion

## Memory Management

### Risk

Exact accumulation can consume substantial memory for large stages because the accumulator holds per-task
values until stage completion.

This is still usually better than:

- writing many task indices to KVStore, and
- scanning/deserializing those tasks again later

But it needs a guardrail.

### Guardrail

Add a configuration limit for exact tracking:

```scala
spark.history.task.quantiles.maxTrackedTasksPerStage
```

If a stage exceeds the configured threshold, the implementation should choose one explicit behavior:

- stop exact accumulation and fall back to legacy quantile computation, or
- switch to approximate sketch mode if enabled

The chosen behavior must be fixed and documented. The preferred long-term choice is to switch to
approximate mode when available.

## KVStore Changes

### What Gets Persisted

Task rows are still stored for task table rendering and task-level UI/API access.

Quantiles are persisted as `CachedQuantile`, as they are today.

### What Can Be Removed

Once quantile computation no longer depends on store-time scans, metric-specific secondary indices on
`TaskDataWrapper` can be removed from the light-storage path.

These indices are high-value removal candidates because they exist mainly to support quantile scans:

- `EXEC_RUN_TIME`
- `DESER_TIME`
- `DESER_CPU_TIME`
- `EXEC_CPU_TIME`
- `RESULT_SIZE`
- `GC_TIME`
- `SER_TIME`
- `GETTING_RESULT_TIME`
- `SCHEDULER_DELAY`
- `PEAK_MEM`
- `MEM_SPILL`
- `DISK_SPILL`
- input/output metric indices
- shuffle read/write metric indices
- shuffle push metric indices

The light-storage path should keep only the indices needed for UI behavior and cleanup, such as:

- `STAGE`
- `TASK_INDEX`
- `LAUNCH_TIME`
- `STATUS`
- `EXECUTOR`
- `COMPLETION_TIME`
- optionally `DURATION` if runtime sorting remains indexed

## Query Path Changes

`AppStatusStore.taskSummary()` should change behavior as follows:

1. Read cached quantiles first.
2. If all requested quantiles are cached, return them immediately.
3. If replay-time quantile computation is enabled, do not perform metric-index scans in light-storage
   mode.
4. Keep the old scan-based path only as a compatibility fallback for legacy/full-storage mode.

This keeps query-time logic simple and makes the performance improvement predictable.

## Configuration

Suggested new configs:

```scala
spark.history.task.quantiles.computeDuringReplay = true
spark.history.task.quantiles.mode = exact
spark.history.task.quantiles.maxTrackedTasksPerStage = 200000
```

Future extension:

```scala
spark.history.task.quantiles.mode = approx
```

Semantics:

- `computeDuringReplay=true`
  - Quantiles are generated in replay and read from cache later.
- `mode=exact`
  - Retain all needed values in memory for exact final quantiles.
- `mode=approx`
  - Use bounded-memory sketches.
- `maxTrackedTasksPerStage`
  - Guardrail for exact accumulation.

## Why This Is Better Than Scanning Tasks at Stage Completion

A weaker version of this idea would still store every task, then do one full pass over persisted task
rows at stage completion. That is better than on-demand UI scans, but it still pays for:

- KVStore iteration
- task deserialization
- reading stage data back from disk

Replay-time accumulation is better because it uses the task data while it is already in memory during
event processing. The work becomes:

1. parse task event once
2. update accumulator once
3. write cached quantiles once

instead of:

1. parse task event
2. write task row
3. reopen task row
4. deserialize task row
5. scan again to compute quantiles

## DuckDB Evaluation

### Question

Would replacing KVStore with DuckDB improve performance?

### Answer

Not as the first optimization for this problem.

The dominant issues here are:

- write amplification from many task secondary indices
- expensive store scans for quantile computation

Replay-time quantile accumulation removes the second problem entirely and allows removal of many
metric-specific indices, which significantly reduces the first problem.

DuckDB is strong for analytical SQL over columnar batch data, but Spark's current History Server storage
API is object-oriented and index-driven:

- `write(Object)`
- `read(Class, key)`
- `view(type).index(name).parent(key)`
- point reads and paginated sorted iteration

DuckDB is not a drop-in replacement for those semantics. Replacing KVStore with DuckDB would require:

- a new physical schema for all persisted UI entities
- a new query layer for sorted pagination and parent-child traversal
- a migration strategy for current store behavior
- reworking serialization and type/index metadata
- validating concurrency and lifecycle behavior

That is a storage-engine rewrite, not a targeted optimization.

### Recommendation

Do not replace KVStore with DuckDB as part of quantile optimization.

If further storage work is needed after replay-time quantiles and reduced indices are implemented, compare
existing KVStore backends and measure the remaining bottlenecks first. Only consider DuckDB as part of a
larger History Server storage redesign.

## Testing

Add coverage for the following:

- replay-time quantiles for a completed stage match the current exact scan-based output
- failed, killed, and resubmitted tasks do not contribute to quantiles
- cached quantiles are available immediately after stage completion during replay
- light storage works without the removed metric indices
- `taskSummary()` does not fall back to expensive scans when replay-time quantiles are enabled
- exact-mode guardrail behavior when tracked task count exceeds the configured threshold
- approximate mode, if added later, stays within acceptable error bounds

## Recommendation

Implement replay-time quantile accumulation in History Server replay, persist only `CachedQuantile`, and
remove metric-specific task indices from the light-storage path.

For v1:

- use exact in-memory accumulation with primitive arrays
- keep it replay-only
- add a per-stage memory guardrail
- preserve the existing `CachedQuantile` read path

For later scaling:

- add approximate quantile sketches for very large stages

This delivers most of the performance benefit with much less complexity than replacing the storage engine.
