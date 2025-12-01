# Shuffle Skew Detection by Shuffle Write Size

## Overview
Replace shuffle skew detection based on finished task counts with a signal derived from shuffle write size. Executors will be excluded when they produce a disproportionate amount of shuffle data, which better reflects real skew than simply finishing more tasks.

## Motivation
- Task-count heuristic misses skew when a few huge map outputs dominate bytes.
- Executors with many tiny tasks are falsely flagged while large writers slip through.
- Shuffle fetch stalls are driven by data volume, not task cardinality.

## Proposed Behavior
For shuffle map stages, track cumulative shuffle write bytes per executor. An executor is considered skewed when its total bytes written exceeds `max(avgBytesPerExecutor, minBytesThreshold) * shuffleSkewRatio`, capped by existing executor limits. The scheduler then excludes these executors from new task offers in the same stage.

## Design Details
### Data collection
- In `TaskSetManager.handleSuccessfulTask`, when `isShuffleMapTasks` is true, extract shuffle write metrics from the task result:
  - Prefer `TaskMetrics.shuffleWriteMetrics.bytesWritten`.
  - Fallback to `InternalAccumulator.SHUFFLE_WRITE_METRICS_PREFIX` accumulators if metrics are missing.
- Maintain `shuffleWriteBytesByExecutorId: Map[String, Long]` alongside the existing `finishedTasksByExecutorId`.
- Ignore tasks with missing metrics (e.g., 0-byte map outputs) to avoid counting speculative attempts that emit no shuffle data.

### Skew computation
- Inputs:
  - `totalExecutors`: passed from `TaskSchedulerImpl`.
  - `shuffleWriteBytesByExecutorId`: bytes aggregated per executor for this task set.
- Derived values:
  - `executorsWithBytes = shuffleWriteBytesByExecutorId.size`.
  - `avgBytesPerExecutor = if (executorsWithBytes > 0) totalBytes / executorsWithBytes else 0`.
  - `threshold = max(avgBytesPerExecutor, shuffleSkewMinShuffleWriteBytes) * shuffleSkewRatio`.
  - `maxSkewedNum = min(ceil(totalExecutors * shuffleSkewMaxExecutorsRatio), shuffleSkewMaxExecutorsNum)`.
- Selection:
  - Filter executors with `bytesWritten >= threshold`.
  - Sort by `bytesWritten` desc and take up to `maxSkewedNum`.
  - Log the threshold and selected executors at debug level for observability.

### State management
- Reset `shuffleWriteBytesByExecutorId` per task set (existing lifecycle).
- Remove entries for lost executors in `executorLost`/`resourceOffer` cleanup to keep averages accurate.
- Do not decrement bytes on re-run of a finished partition; only the first successful attempt counts (matching existing task success semantics).

### Configuration
- New: `spark.scheduler.shuffleSkew.minShuffleWriteBytes` (default: 256m).
  - Minimum bytes an executor must have produced before being considered for skew exclusion.
  - Replaces `spark.scheduler.shuffleSkew.minFinishedTasks` in the skew calculation.
- Existing configs remain:
  - `spark.scheduler.shuffleSkew.ratio`
  - `spark.scheduler.shuffleSkew.maxExecutorsNumber`
  - `spark.scheduler.shuffleSkew.maxExecutorsRatio`
- Deprecate `minFinishedTasks` for skew detection; keep it for backward compatibility until removal, but ignore it when `minShuffleWriteBytes` is set.

## Compatibility and Fallbacks
- If no shuffle write metrics are available for any executor, skip skew filtering for that stage to avoid false positives.
- Behavior for non-shuffle stages is unchanged (skew filtering disabled).
- Preserve executor caps to avoid excessive exclusions during small stages.

## Validation Plan
- Unit tests in `TaskSetManagerSuite`:
  - Verify bytes accumulation per executor from shuffle write metrics.
  - Threshold calculation: avg vs. `minShuffleWriteBytes`, ratio application, and cap enforcement.
  - Missing-metrics path: no skewed executors when bytes are absent.
  - Executor loss: averages recomputed without lost executors.
- Integration checks in `TaskSchedulerImplSuite` to ensure `totalExecutors` flows through and filtering respects caps.
- Config tests for new `minShuffleWriteBytes` default, parsing, and deprecation of `minFinishedTasks`.

## Migration Notes
- Users tuning skew via task counts should migrate to byte-based tuning:
  - Lower `minShuffleWriteBytes` for aggressive skew detection in small shuffles.
  - Raise `minShuffleWriteBytes` to suppress exclusions on lightweight stages.
- Document the behavioral shift in the core migration guide and configs reference.
