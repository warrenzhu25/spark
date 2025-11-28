# Simplify Shuffle Skew Detection: Remove Ratio-Based Logic

## Overview

Transform Apache Spark's shuffle skew detection from a complex ratio-based approach to a simple top-K selection mechanism. This removes the `shuffleSkewRatio` configuration and associated average calculation logic, making the system more predictable and easier to understand.

### Current Behavior
- Calculate average: `max(tasksSuccessful / totalExecutors, minFinishedTasks)`
- Apply ratio: `threshold = average * shuffleSkewRatio`
- Filter: executors where `taskCount >= threshold`
- Cap results: `min(maxExecutorsNum, ceil(totalExecutors * maxExecutorsRatio))`

### New Behavior
- Filter: executors where `taskCount >= minFinishedTasks`
- Sort by task count (descending)
- Take top K: `min(maxExecutorsNum, ceil(totalExecutors * maxExecutorsRatio))`

## Rationale

**Benefits:**
1. **Simpler logic**: No complex ratio calculations or average computations
2. **Predictable**: Always excludes exactly K executors (if eligible)
3. **Fewer edge cases**: Eliminates issues with minimum threshold application
4. **Same control**: Maintains both absolute and relative caps for limiting exclusions

**Trade-offs:**
- **Breaking change**: Removes `spark.scheduler.shuffleSkew.ratio` configuration
- **Behavioral change**: May exclude different executors in some scenarios
- **Less adaptive**: Doesn't adjust threshold based on overall distribution

## Implementation Plan

### Phase 1: Configuration Changes (30 min)

#### File: `core/src/main/scala/org/apache/spark/internal/config/package.scala`

**Remove (lines 2118-2124):**
```scala
val SHUFFLE_SKEW_RATIO =
  ConfigBuilder("spark.scheduler.shuffleSkew.ratio")
    .doc("How many times larger than average finished shuffle map task number on executor " +
      "to be considered as skewed.")
    .version("4.0.0")
    .doubleConf
    .createWithDefault(1.5)
```

**Update documentation for remaining configs:**
- Update `SCHEDULER_EXCLUDE_SHUFFLE_SKEW_EXECUTORS` doc to: "Exclude the top K shuffle map skewed executors when scheduling, where K is determined by maxExecutorsNumber and maxExecutorsRatio caps."
- Update `SHUFFLE_SKEW_MIN_FINISHED_TASKS` doc to: "Minimum number of finished shuffle map tasks on one executor before it can be considered for exclusion."
- Update `SHUFFLE_SKEW_MAX_EXECUTORS_NUM` doc to: "Maximum number of top-task executors to exclude when scheduling."
- Update `SHUFFLE_SKEW_MAX_EXECUTORS_RATIO` doc to: "Maximum ratio of total executors to exclude when scheduling (top executors by task count)."

### Phase 2: Core Logic Changes (1 hour)

#### File: `core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala`

**Step 1: Remove shuffleSkewRatio field (line 117)**

Remove:
```scala
private val shuffleSkewRatio = conf.get(SHUFFLE_SKEW_RATIO)
```

**Step 2: Delete getAverageTaskNum method (lines 1340-1346)**

Remove entire method:
```scala
private def getAverageTaskNum(totalExecutors: Int) = {
  if (tasksSuccessful > 0 && totalExecutors > 0) {
    math.max(tasksSuccessful / totalExecutors, shuffleSkewMinFinishedTasks)
  } else {
    shuffleSkewMinFinishedTasks
  }
}
```

**Step 3: Rewrite getSkewedExecutors method (lines 1307-1327)**

Replace with:
```scala
def getSkewedExecutors(totalExecutors: Int): Set[String] = {
  if (!excludeShuffleSkewExecutors || totalExecutors <= 0) {
    return Set.empty
  }

  // Calculate maximum number of executors to exclude
  val maxSkewedNum = math.min(
    math.ceil(totalExecutors * shuffleSkewMaxExecutorsRatio).toInt,
    shuffleSkewMaxExecutorsNum)

  // Filter executors meeting minimum threshold, sort by task count, take top K
  val skewedExecutors = finishedTasksByExecutorId.filter { case (_, numOutputs) =>
    numOutputs >= shuffleSkewMinFinishedTasks
  }.toSeq
    .sortBy(_._2)(Ordering.Int.reverse)
    .take(maxSkewedNum)

  if (skewedExecutors.nonEmpty) {
    logDebug(s"Top skewed executors for stage $stageId: $skewedExecutors")
  }
  skewedExecutors.map(_._1).toSet
}
```

**Key changes:**
- Remove `averageTaskNum` calculation
- Filter only by minimum threshold (`>= shuffleSkewMinFinishedTasks`)
- Take top K by task count where K is the cap calculation
- Update log message to reflect new semantics

### Phase 3: Test Updates (3-4 hours)

#### File: `core/src/test/scala/org/apache/spark/scheduler/TaskSetManagerSuite.scala`

**Step 1: Update test helper (lines 1984-2004)**

Modify `testExcludeShuffleSkewSetup` signature:
```scala
private def testExcludeShuffleSkewSetup(
    numTasks: Int,
    enableShuffleSkewOpt: Option[Boolean] = None,
    minFinishedTasksOpt: Option[Int] = None,
    maxExecutorsNumOpt: Option[Int] = None): TaskSetManager = {
  // Remove ratioOpt parameter

  val conf = new SparkConf()
  enableShuffleSkewOpt.foreach { v =>
    conf.set(config.SCHEDULER_EXCLUDE_SHUFFLE_SKEW_EXECUTORS, v)
  }
  minFinishedTasksOpt.foreach { v =>
    conf.set(config.SHUFFLE_SKEW_MIN_FINISHED_TASKS, v)
  }
  maxExecutorsNumOpt.foreach { v =>
    conf.set(config.SHUFFLE_SKEW_MAX_EXECUTORS_NUM, v)
  }
  // Remove ratio configuration

  // ... rest of method unchanged
}
```

**Step 2: Update all test calls (31 tests)**

Find and replace pattern:
```scala
// Old:
testExcludeShuffleSkewSetup(numTasks, Some(true), Some(2), Some(5), Some(1.5))

// New:
testExcludeShuffleSkewSetup(numTasks, Some(true), Some(2), Some(5))
```

**Step 3: Rewrite ratio-specific tests (3 tests)**

These tests need logic updates:

1. **"getSkewedExecutors only return executors greater than ratio"** (lines 2381-2411)
   - Rename to: "getSkewedExecutors returns top K executors by task count"
   - Update assertions to verify top-K selection instead of ratio threshold
   - Example: After 10 tasks distributed as exec1=5, exec2=3, exec3=2, with K=2, expect Set("exec1", "exec2")

2. **"getSkewedExecutors respects skew ratio threshold"** (if exists)
   - Rename to: "getSkewedExecutors filters by minimum threshold"
   - Test that only executors with >= minFinishedTasks are considered

3. **"getSkewedExecutors with varying ratios"** (if exists)
   - Remove or replace with: "getSkewedExecutors with varying minimum thresholds"

**Step 4: Remove obsolete tests (3 tests to delete)**

Remove these ratio-specific tests entirely:
- Any test with "ratio" in the name that can't be adapted
- Tests specifically validating ratio calculation accuracy
- Tests for ratio edge cases (e.g., ratio=1.0, ratio=2.0)

**Step 5: Add new validation tests (2 tests)**

Add tests to verify new behavior:

```scala
test("getSkewedExecutors selects top K by task count") {
  val manager = testExcludeShuffleSkewSetup(
    numTasks = 30,
    Some(true),
    Some(5),  // minFinishedTasks
    Some(2))  // maxExecutorsNum

  val directTaskResult = createTaskResult(0)

  // Distribute tasks: exec1=15, exec2=10, exec3=5, exec4=0
  for (i <- 0 until 30) {
    val execId = if (i < 15) "exec1"
                 else if (i < 25) "exec2"
                 else "exec3"
    val task = manager.resourceOffer(execId, s"host$execId", NO_PREF)._1.get
    manager.handleSuccessfulTask(task.taskId, directTaskResult)
  }

  // Should select top 2: exec1 and exec2
  assert(manager.getSkewedExecutors(4) === Set("exec1", "exec2"))
}

test("getSkewedExecutors respects minimum threshold in top-K selection") {
  val manager = testExcludeShuffleSkewSetup(
    numTasks = 20,
    Some(true),
    Some(10),  // minFinishedTasks
    Some(3))   // maxExecutorsNum

  val directTaskResult = createTaskResult(0)

  // Distribute: exec1=12, exec2=6, exec3=2
  for (i <- 0 until 20) {
    val execId = if (i < 12) "exec1" else if (i < 18) "exec2" else "exec3"
    val task = manager.resourceOffer(execId, s"host$execId", NO_PREF)._1.get
    manager.handleSuccessfulTask(task.taskId, directTaskResult)
  }

  // Only exec1 meets minimum threshold, so only it is returned
  assert(manager.getSkewedExecutors(3) === Set("exec1"))
}
```

### Phase 4: Integration Verification (30 min)

#### File: `core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala`

**No changes needed**, but verify:
- Line 400: `filterShuffleSkewExecutors` call passes `totalExecutors` correctly
- Lines 461-465: Wrapper method signature remains compatible
- Integration logic in `resourceOffers` still works with simplified `getSkewedExecutors`

### Phase 5: Documentation Updates (30 min)

#### File: `docs/core-migration-guide.md` (or similar)

Add migration notice:

```markdown
### Shuffle Skew Detection Simplification

- **SPARK-XXXXX**: The `spark.scheduler.shuffleSkew.ratio` configuration has been removed.
- **Migration**: The shuffle skew detection now uses a simple top-K approach instead of ratio-based filtering.
  - Previously: Executors were excluded if their task count exceeded `average * ratio`
  - Now: The top K executors by task count are excluded (where K is determined by the existing cap configurations)
  - If you were relying on the ratio parameter to tune skew detection sensitivity, adjust `spark.scheduler.shuffleSkew.minFinishedTasks` instead:
    - Lower values (e.g., 5) make detection more aggressive
    - Higher values (e.g., 20) make detection more conservative
  - The caps `spark.scheduler.shuffleSkew.maxExecutorsNumber` and `spark.scheduler.shuffleSkew.maxExecutorsRatio` continue to work as before
```

### Phase 6: Validation (1-2 hours)

**Pre-commit checklist:**
1. Run scalastyle: `./build/sbt "core/scalastyle"`
2. Run affected tests: `./build/sbt "core/testOnly *TaskSetManagerSuite"`
3. Run scheduler tests: `./build/sbt "core/testOnly *TaskSchedulerImplSuite"`
4. Verify no compilation errors: `./build/sbt compile`
5. Check for any remaining references to `SHUFFLE_SKEW_RATIO` or `shuffleSkewRatio`

**Post-commit validation:**
1. Run full core test suite
2. Manual testing with varying configurations
3. Verify log messages make sense

## Files to Modify

### Critical Files (must edit):
1. **core/src/main/scala/org/apache/spark/internal/config/package.scala** - Remove SHUFFLE_SKEW_RATIO config
2. **core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala** - Remove ratio field, delete getAverageTaskNum, rewrite getSkewedExecutors
3. **core/src/test/scala/org/apache/spark/scheduler/TaskSetManagerSuite.scala** - Update helper, modify 31 test calls, rewrite 3 tests, remove 3 tests, add 2 new tests

### Documentation Files (should update):
4. **docs/core-migration-guide.md** - Add breaking change notice

### Reference Files (verify only):
5. **core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala** - No changes needed but verify integration still works

## Testing Strategy

### Unit Tests to Update (37 total)

**Category 1: Helper method calls (31 tests)**
- Simply remove the ratio parameter from all `testExcludeShuffleSkewSetup` calls
- No logic changes needed, just signature update

**Category 2: Ratio-based tests (3 tests to rewrite)**
- "getSkewedExecutors only return executors greater than ratio" → test top-K selection
- Update assertions to verify ranking instead of threshold
- Ensure minimum threshold is still validated

**Category 3: Obsolete tests (3 tests to remove)**
- Any test specifically validating ratio calculation
- Tests for ratio edge cases that no longer apply

**Category 4: New tests (2 tests to add)**
- Test top-K selection with various distributions
- Test minimum threshold filtering in top-K context

### Integration Tests
- Existing TaskSchedulerImplSuite tests should pass without changes
- The "Scheduler passes total executor count to skew detection" test verifies correct parameter passing

## Risk Assessment

### Breaking Changes
- **HIGH**: Removal of `spark.scheduler.shuffleSkew.ratio` configuration
  - Impact: Users with custom ratio values will see different behavior
  - Mitigation: Clear migration guide, default behavior still reasonable

### Behavioral Changes
- **MEDIUM**: Different executors may be excluded compared to ratio-based approach
  - Impact: Job performance characteristics may change
  - Mitigation: New behavior is more predictable and easier to reason about

### Implementation Risks
- **LOW**: Test updates are mechanical but numerous (37 tests)
  - Mitigation: Systematic approach, verify each category separately

## Estimated Effort

- Configuration changes: 30 minutes
- Core logic rewrite: 1 hour
- Test updates: 3-4 hours (37 tests)
- Documentation: 30 minutes
- Validation: 1-2 hours
- **Total: 7-9 hours**

## Success Criteria

1. All tests pass (core/testOnly *TaskSetManagerSuite and *TaskSchedulerImplSuite)
2. Scalastyle clean (0 errors, 0 warnings)
3. No compilation errors or warnings
4. No references to shuffleSkewRatio or SHUFFLE_SKEW_RATIO remain
5. Migration guide clearly explains the breaking change
6. New behavior is simpler and more predictable than old behavior
