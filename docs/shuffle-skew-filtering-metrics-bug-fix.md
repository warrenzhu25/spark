# Bug Fix Plan: Shuffle Skew Filtering Metrics Calculation

## Bug Summary

The shuffle skew filtering metrics in commit `177a7ba2015` contain a critical bug in how improvement is calculated. The current implementation compares baseline to actual max across ALL executors, which doesn't properly measure filtering effectiveness.

### Location
- **File**: `core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala`
- **Method**: `logShuffleSkewFilteringSummary()` (lines 626-664)
- **Lines to modify**: 628-647

## The Problem

### Current Implementation

```scala
// Line 628-632: Calculate actual max
val actualMaxTasks = if (finishedTasksByExecutorId.nonEmpty) {
  finishedTasksByExecutorId.values.max  // Includes ALL executors
} else {
  0
}

// Line 641-647: Calculate improvement
val tasksReduced = baselineMaxTasksPerExecutor - actualMaxTasks
val improvementPercent = if (baselineMaxTasksPerExecutor > 0) {
  (tasksReduced.toDouble / baselineMaxTasksPerExecutor * 100).toInt
} else {
  0
}
```

### Why This Is Wrong

The current calculation compares:
- **Baseline**: Max tasks when filtering started (e.g., 100)
- **Actual**: Max tasks across ALL executors at stage end (e.g., 104) ← includes filtered executors!
- **Result**: Shows negative improvement (-4%) because filtered executor grew slightly

**The fundamental issue**: Filtered executors can GROW after being filtered (completing already-running tasks), making it appear filtering made things worse!

## The Correct Approach

### What Should Be Measured

**Filtering effectiveness** should be measured by: *"How many tasks did we prevent from being assigned to the skewed executor?"*

This requires comparing:
1. **Baseline**: Tasks on filtered executor when filtering started
2. **Hypothetical**: Tasks the executor WOULD have reached without filtering
3. **Actual**: Tasks the executor ACTUALLY reached with filtering
4. **Improvement**: Prevented tasks as % of potential growth

### Formula

```
hypothetical_max = baseline + (stage_finish_time - filtering_start_time) / avg_task_duration
prevented_tasks = hypothetical_max - actual_max_on_filtered_executor
potential_growth = hypothetical_max - baseline
improvement = prevented_tasks / potential_growth = (hypothetical - actual) / (hypothetical - baseline)
```

### Concrete Example

**Scenario: Cluster scaling with skew**
- Start: 2 executors, each with 100 tasks
- Cluster scales: 10 executors total (2 old + 8 new)
- Filtering detects skew when old executors have 100 tasks → baseline = 100
- Stage runs for 4 more minutes, avg task time = 1 minute
- Without filtering: old executor would get ~10 more tasks → 110 total
- With filtering: old executor only completes running tasks → 104 total

**Current buggy output:**
```
Before filtering: max 100 tasks/executor
After filtering: max 104 tasks/executor
Improvement: reduced max load by -4 tasks (-4%)  ← WRONG! Negative!
```

**Correct output with fix:**
```
Before filtering: max 100 tasks/executor
Hypothetical without filtering: 110 tasks/executor
After filtering: max 104 tasks/executor
Improvement: prevented 6 out of 10 potential new tasks (60%)
```

## Fix Implementation

### Required Data Collection

We need to track:
1. **Baseline captured** (already done): `baselineMaxTasksPerExecutor`
2. **Filtering start time** (NEW): When filtering first occurred
3. **Filtered executor IDs** (already tracked): `filteredSkewExecutors`
4. **Stage finish time**: When `maybeFinishTaskSet()` is called
5. **Average task duration**: From successful tasks

### Code Changes

**Step 1: Add field to track filtering start time**

Location: Line ~134 (with other baseline fields)

```scala
private var baselineMaxTasksPerExecutor: Int = 0
private var baselineTotalExecutors: Int = 0
private var firstFilteringDone: Boolean = false
private var filteringStartTime: Long = 0  // NEW: Track when filtering started
```

**Step 2: Capture filtering start time**

Location: Line ~1418 in `filterShuffleSkewExecutors()`

```scala
if (!firstFilteringDone && skewedExecutors.nonEmpty) {
  firstFilteringDone = true
  filteringStartTime = clock.getTimeMillis()  // NEW: Capture timestamp
  baselineTotalExecutors = shuffledOffers.length
  baselineMaxTasksPerExecutor = if (finishedTasksByExecutorId.nonEmpty) {
    finishedTasksByExecutorId.values.max
  } else {
    0
  }
}
```

**Step 3: Rewrite improvement calculation**

Location: Lines 626-664 in `logShuffleSkewFilteringSummary()`

```scala
private def logShuffleSkewFilteringSummary(): Unit = {
  // Get the most heavily filtered executor (highest count in filteredSkewExecutors)
  val primaryFilteredExec = filteredSkewExecutors.maxBy(_._2)._1

  // Actual max on the filtered executor
  val actualMaxOnFiltered = finishedTasksByExecutorId.getOrElse(primaryFilteredExec, 0)

  // Calculate average task duration from successful tasks
  val avgTaskDuration = if (taskInfos.nonEmpty && tasksSuccessful > 0) {
    val totalDuration = taskInfos.filter(_._2.successful).map { case (_, info) =>
      info.finishTime - info.launchTime
    }.sum
    totalDuration.toDouble / tasksSuccessful
  } else {
    1.0  // Fallback to prevent division by zero
  }

  // Calculate hypothetical max without filtering
  val stageFinishTime = clock.getTimeMillis()
  val timeAfterFiltering = stageFinishTime - filteringStartTime
  val potentialNewTasks = math.ceil(timeAfterFiltering / avgTaskDuration).toInt
  val hypotheticalMax = baselineMaxTasksPerExecutor + potentialNewTasks

  // Calculate improvement
  val potentialGrowth = hypotheticalMax - baselineMaxTasksPerExecutor
  val actualGrowth = actualMaxOnFiltered - baselineMaxTasksPerExecutor
  val preventedTasks = potentialGrowth - actualGrowth

  val improvementPercent = if (potentialGrowth > 0) {
    (preventedTasks.toDouble / potentialGrowth * 100).toInt
  } else {
    0
  }

  // Calculate ideal distribution (unchanged)
  val idealMaxTasks = if (baselineTotalExecutors > 0) {
    math.ceil(tasksSuccessful.toDouble / baselineTotalExecutors).toInt
  } else {
    0
  }

  // Max among non-filtered executors (for comparison)
  val maxNonFiltered = if (finishedTasksByExecutorId.nonEmpty) {
    finishedTasksByExecutorId
      .filterNot { case (execId, _) => filteredSkewExecutors.contains(execId) }
      .values.maxOption.getOrElse(0)
  } else {
    0
  }

  // Format filtered executors with details
  val filteredSummary = filteredSkewExecutors.toSeq
    .sortBy(-_._2)
    .map { case (execId, count) =>
      val tasks = finishedTasksByExecutorId.getOrElse(execId, 0)
      s"$execId($tasks tasks, excluded $count times)"
    }
    .mkString(", ")

  logInfo(s"Shuffle skew filtering summary for stage $stageId: " +
    s"Filtered executors: [$filteredSummary] | " +
    s"Baseline: $baselineMaxTasksPerExecutor tasks/executor | " +
    s"Hypothetical without filtering: $hypotheticalMax tasks/executor | " +
    s"Actual on filtered executor: $actualMaxOnFiltered tasks/executor | " +
    s"Max on non-filtered executors: $maxNonFiltered tasks/executor | " +
    s"Improvement: prevented $preventedTasks out of $potentialGrowth potential tasks ($improvementPercent%)")
}
```

## Critical Files to Modify

- `core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala`
  - Line ~134: Add `filteringStartTime` field
  - Line ~1420: Capture filtering start time in `filterShuffleSkewExecutors()`
  - Lines 626-664: Rewrite `logShuffleSkewFilteringSummary()` method

## Edge Cases to Handle

1. **avgTaskDuration = 0**: Use fallback value (1.0) to prevent division by zero
2. **potentialGrowth = 0**: Return 0% improvement (filtering started right before stage end)
3. **No tasks completed yet**: Use fallback avg duration
4. **Multiple filtered executors**: Focus on the primary one (most filtered)
5. **Negative actual growth**: Possible if executor completes fewer tasks than expected - clamp to 0

## Verification Plan

### 1. Code Review
- Verify time calculation: `(stageFinishTime - filteringStartTime) / avgTaskDuration`
- Confirm improvement formula: `(hypotheticalMax - actualMax) / (hypotheticalMax - baseline)`
- Check edge case handling

### 2. Unit Tests (if they exist)
- Test with mock data: baseline=100, hypothetical=110, actual=104 → expect 60%
- Test edge cases: zero growth, negative growth, no filtered executors

### 3. Integration Testing
- Run Spark job with cluster scaling and shuffle skew
- Verify log output shows realistic improvement (e.g., 50-70%)
- Compare hypothetical vs actual to validate formula

### 4. Validation Scenarios

**Scenario 1: Effective filtering**
- Baseline: 100, Hypothetical: 150, Actual: 105
- Expected improvement: (150-105)/(150-100) = 45/50 = 90%

**Scenario 2: Minimal filtering effect**
- Baseline: 100, Hypothetical: 110, Actual: 108
- Expected improvement: (110-108)/(110-100) = 2/10 = 20%

**Scenario 3: No time for filtering**
- Baseline: 100, Hypothetical: 100, Actual: 100
- Expected improvement: 0% (stage finished immediately)

## Alternative Considered: Distribution-Based Metric

An alternative approach (initially proposed) would measure:
- **Improvement = (baseline - maxNonFiltered) / baseline**
- Example: (100 - 60) / 100 = 40%

This measures: *"How much better is the distribution among active executors?"*

**Why we're not using this:**
- Doesn't account for filtered executor growth
- Doesn't measure prevention effectiveness
- Doesn't consider time dimension

The counterfactual approach (hypothetical vs actual) better captures filtering effectiveness.

## Documentation Updates

The documentation in `docs/shuffle-skew-filtering-metrics.md` should be updated to reflect:
1. New improvement calculation methodology
2. Hypothetical max calculation
3. Updated example outputs with hypothetical values
4. Explanation of prevention effectiveness metric

## Impact Assessment

### Severity: High
- **Current**: Metrics show negative improvement, misleading users
- **Impact**: Users may disable an effective feature
- **Visibility**: Makes it impossible to validate filtering benefits

### Risk of Fix: Medium
- **Complexity**: Requires time tracking and average duration calculation
- **Dependencies**: Uses `clock.getTimeMillis()` and `taskInfos`
- **Edge cases**: Need robust handling of timing edge cases

### Benefits
- **Accurate metrics**: Shows true prevention effectiveness
- **Better visibility**: Users can validate filtering is working
- **Debugging**: Helps identify if skew persists despite filtering

## Summary

This fix transforms the metrics from a simple baseline-to-actual comparison into a **counterfactual prevention metric** that shows how many tasks were prevented from being assigned to skewed executors. This requires:

1. Tracking when filtering starts
2. Calculating average task duration
3. Estimating what would have happened without filtering
4. Comparing hypothetical vs actual to measure prevented tasks

The result is a meaningful percentage that shows: *"We prevented X% of potential new task assignments to the skewed executor"* - a true measure of filtering effectiveness.
