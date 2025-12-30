# Shuffle Skew Filtering Metrics and Logging

## Overview

This feature provides visibility into the effectiveness of Spark's shuffle skew executor filtering by logging detailed metrics when a stage completes. The logging shows how excluding skewed executors improves task distribution and reduces maximum executor load.

## Background

Shuffle skew occurs when some executors process significantly more shuffle output data than others, leading to:
- Long task execution times on overloaded executors
- Inefficient cluster utilization
- Delayed job completion

Spark's shuffle skew filtering feature (controlled by `spark.scheduler.excludeShuffleSkewExecutors`) addresses this by excluding skewed executors from subsequent task scheduling, allowing tasks to be distributed more evenly across the remaining executors.

## Feature Description

When shuffle skew filtering is enabled and a stage completes, Spark logs a comprehensive summary showing:

1. **Filtered Executors**: Which executors were excluded and how many times
2. **Baseline Distribution**: Maximum tasks per executor before filtering
3. **Ideal Distribution**: Expected even distribution across all executors
4. **Actual Distribution**: Maximum tasks per executor after filtering
5. **Improvement Metrics**: Reduction in max executor load (tasks and percentage)

## Configuration

### Enable Shuffle Skew Filtering

```properties
# Enable the feature (default: false)
spark.scheduler.excludeShuffleSkewExecutors=true

# Skew detection threshold - executor is skewed if tasks >= average * ratio (default: 1.5)
spark.scheduler.shuffleSkew.ratio=1.5

# Minimum finished tasks before considering skew (default: 10)
spark.scheduler.shuffleSkew.minFinishedTasks=10

# Maximum number of executors to exclude (absolute cap, default: 5)
spark.scheduler.shuffleSkew.maxExecutorsNumber=5

# Maximum percentage of executors to exclude (default: 0.05 = 5%)
spark.scheduler.shuffleSkew.maxExecutorsRatio=0.05
```

### Metrics Logging

Metrics logging is **automatically enabled** when shuffle skew filtering is enabled. The summary is logged at **INFO level** when a stage completes, only if filtering actually occurred.

No additional configuration is required.

## Log Output Format

### Log Level
`INFO` - Logged via `TaskSetManager`

### Log Message Structure

```
INFO TaskSetManager: Shuffle skew filtering summary for stage <stageId>:
  Filtered executors: [<execId>(<tasks> tasks, excluded <count> times), ...]
  | Before filtering: max <maxTasks> tasks/executor (ideal: <ideal> across <total> executors)
  | After filtering: max <actual> tasks/executor
  | Improvement: reduced max load by <reduced> tasks (<percent>%)
```

### Example: Effective Filtering

```
INFO TaskSetManager: Shuffle skew filtering summary for stage 2: Filtered executors: [exec-3(145 tasks, excluded 12 times), exec-7(98 tasks, excluded 8 times)] | Before filtering: max 145 tasks/executor (ideal: 50 across 10 executors) | After filtering: max 62 tasks/executor | Improvement: reduced max load by 83 tasks (57%)
```

**Interpretation:**
- Stage 2 had significant skew
- 2 executors (exec-3, exec-7) were repeatedly filtered
- Before filtering: One executor had 145 tasks (the skewed executor)
- Ideal distribution: 50 tasks per executor (500 tasks / 10 executors)
- After filtering: Max reduced to 62 tasks per executor
- **57% reduction** in max executor load

### Example: Minimal Impact

```
INFO TaskSetManager: Shuffle skew filtering summary for stage 5: Filtered executors: [exec-2(78 tasks, excluded 3 times)] | Before filtering: max 78 tasks/executor (ideal: 64 across 8 executors) | After filtering: max 71 tasks/executor | Improvement: reduced max load by 7 tasks (9%)
```

**Interpretation:**
- Stage 5 had minor skew
- 1 executor (exec-2) was filtered a few times
- Only 9% improvement, indicating skew was less severe

## Use Cases

### 1. Validating Skew Detection

Verify that Spark correctly identifies skewed executors in your workload:
- Check if the right executors are being filtered
- Confirm the task count matches expectations
- Validate the skew threshold configuration

### 2. Measuring Performance Impact

Quantify the benefit of shuffle skew filtering:
- High improvement percentage (>30%) indicates effective filtering
- Low improvement (<10%) suggests tuning may be needed
- Compare stage completion times with/without filtering

### 3. Tuning Configuration

Use metrics to optimize shuffle skew filtering parameters:
- If too many executors are filtered, increase `shuffleSkew.ratio`
- If skew persists, decrease `shuffleSkew.ratio` to be more aggressive
- Adjust `maxExecutorsNumber` or `maxExecutorsRatio` based on cluster size

### 4. Debugging Data Skew

Investigate root causes of data skew:
- Identify which stages have severe skew
- Correlate with specific operations (groupBy, join, etc.)
- Track skew across multiple stages to find the source

## Implementation Details

### Baseline Capture

Metrics are captured when shuffle skew filtering **first occurs** during a stage:

1. **Trigger**: First time `filterShuffleSkewExecutors()` detects skewed executors
2. **Metrics Captured**:
   - `baselineMaxTasksPerExecutor`: Maximum tasks on any executor at that moment
   - `baselineTotalExecutors`: Total number of available executors
3. **One-time**: Baseline is captured only once per stage to reflect initial skew

### Improvement Calculation

At stage completion:

```scala
// Actual max tasks after filtering
actualMaxTasks = max(finishedTasksByExecutorId.values)

// Ideal distribution if tasks were evenly distributed
idealMaxTasks = ceil(tasksSuccessful / baselineTotalExecutors)

// Improvement metrics
tasksReduced = baselineMaxTasksPerExecutor - actualMaxTasks
improvementPercent = (tasksReduced / baselineMaxTasksPerExecutor) * 100
```

### Filtered Executors Summary

Executors are sorted by exclusion count (descending) to highlight the most frequently filtered:

```scala
filteredSummary = filteredSkewExecutors
  .sortBy(-exclusionCount)
  .map { (execId, count) =>
    tasks = finishedTasksByExecutorId.getOrElse(execId, 0)
    s"$execId($tasks tasks, excluded $count times)"
  }
```

### Logging Condition

Summary is logged only when:
1. Stage completes successfully (`isZombie && runningTasks == 0`)
2. Filtering occurred (`filteredSkewExecutors.nonEmpty`)
3. Baseline was captured (`firstFilteringDone`)

This ensures the log only appears when shuffle skew filtering was active and had data to report.

## Interpretation Guide

### High Improvement (>40%)

Indicates severe skew that was effectively mitigated:
- **Action**: Monitor if skew persists across stages
- **Consideration**: Investigate data partitioning strategy
- **Performance**: Significant benefit from filtering

### Moderate Improvement (20-40%)

Indicates noticeable skew with good filtering effectiveness:
- **Action**: Current configuration is working well
- **Consideration**: Skew may be acceptable for the workload
- **Performance**: Measurable benefit from filtering

### Low Improvement (<20%)

Indicates minor skew or ineffective filtering:
- **Action**: Review skew threshold (`shuffleSkew.ratio`)
- **Consideration**: Skew may not be the bottleneck
- **Performance**: Limited benefit from filtering

### Zero or Negative Improvement

Indicates filtering may not have helped or baseline was captured late:
- **Action**: Check if baseline capture timing is correct
- **Consideration**: Data distribution may have changed during stage
- **Performance**: Re-evaluate if filtering is beneficial

## Metrics Breakdown

| Metric | Description | Interpretation |
|--------|-------------|----------------|
| **Filtered executors** | Executors excluded during scheduling | More executors = more severe skew |
| **Excluded N times** | How often each executor was filtered | Higher count = persistent skew |
| **Before filtering: max** | Maximum tasks on any executor initially | Baseline for comparison |
| **Ideal** | Even distribution target | Perfect distribution benchmark |
| **After filtering: max** | Maximum tasks after filtering | Actual outcome achieved |
| **Reduced max load by** | Absolute task reduction | Improvement in tasks |
| **Improvement %** | Percentage reduction | Effectiveness measure |

## Future Enhancements

### Shuffle Write Bytes Tracking

A planned enhancement will add shuffle write size metrics:

```
INFO TaskSetManager: Shuffle skew filtering summary for stage 2:
  ...
  | Shuffle writes: max 2.5GB → 800MB (68% reduction)
```

This will provide data skew measurement in addition to task count skew, offering a more complete picture of the improvement.

## Related Configuration

### Log Level Configuration

To see the metrics, ensure your log level is set to INFO or lower for TaskSetManager:

```properties
# In log4j2.properties
logger.tasksetmanager.name = org.apache.spark.scheduler.TaskSetManager
logger.tasksetmanager.level = info
```

### Disable Metrics Logging

To disable metrics logging while keeping shuffle skew filtering enabled, set the log level to WARN:

```properties
logger.tasksetmanager.level = warn
```

Note: This will also suppress other TaskSetManager INFO logs.

## References

- **Configuration**: See `spark.scheduler.*` in Spark Configuration Guide
- **Implementation**: `TaskSetManager.scala` (lines 610-652)
- **Related Features**: Health Tracker, Executor Exclusion, Delay Scheduling

## Changelog

### Version 1.0 (Initial Implementation)

- Added baseline metrics capture on first filtering
- Added comprehensive summary logging at stage completion
- Metrics include: filtered executors, task distribution, improvement percentage
- Automatically enabled when shuffle skew filtering is active
