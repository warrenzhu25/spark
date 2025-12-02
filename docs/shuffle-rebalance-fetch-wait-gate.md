# Implementation Plan: Gate Shuffle Rebalance When Fetch Wait Is High

## Overview

Add a gate to prevent shuffle rebalancing when average fetch wait time is high, indicating the system is already under network pressure.

## Rationale

When shuffle fetch wait time is high, it signals that:
- The network or remote executors are saturated
- Tasks are blocked waiting for shuffle data
- Adding more network traffic (from rebalancing) would worsen the situation

By gating rebalancing during high fetch wait periods, we:
- Avoid self-inflicted performance degradation
- Prevent adding load when the system is already stressed
- Allow the system to drain existing work before redistributing

## Implementation

### 1. Configuration Parameter

**File**: `core/src/main/scala/org/apache/spark/internal/config/package.scala`

**Location**: Add after existing shuffle rebalance configs (around line 789)

```scala
val SHUFFLE_REBALANCE_FETCH_WAIT_THRESHOLD_MS =
  ConfigBuilder("spark.shuffle.rebalance.fetchWaitThresholdMs")
    .doc("Maximum average fetch wait time (in milliseconds) per task before disabling " +
      "shuffle rebalancing. When the average fetch wait time exceeds this threshold, " +
      "rebalancing is skipped to avoid adding network load during high contention. " +
      "Set to 0 or negative to disable this check (default behavior).")
    .version("4.0.0")
    .longConf
    .createWithDefault(0L)
```

### 2. ShuffleRebalanceManager Changes

**File**: `core/src/main/scala/org/apache/spark/scheduler/ShuffleRebalanceManager.scala`

#### 2a. Add Configuration Field

Add to constructor or as class field (around line 60):

```scala
private val fetchWaitThresholdMs = conf.get(SHUFFLE_REBALANCE_FETCH_WAIT_THRESHOLD_MS)
```

#### 2b. Add Tracking for Logged Stages

Add as class field to prevent log spam:

```scala
// Track which shuffle attempts we've already logged about fetch wait gating
private val loggedFetchWaitGating = new ConcurrentHashMap[Int, java.lang.Boolean]()
```

#### 2c. Add Fetch Wait Gate Logic

**Location**: In `checkAndInitiateShuffleRebalance()` method, after feature-enabled check (around line 87)

Insert this logic:

```scala
def checkAndInitiateShuffleRebalance(stage: ShuffleMapStage, completedTasks: Int): Unit = {
  if (!shuffleRebalanceEnabled) return

  // NEW: Gate on high fetch wait time
  if (fetchWaitThresholdMs > 0 && completedTasks > 0) {
    val stageInfo = stage.latestInfo
    if (stageInfo.taskMetrics != null) {
      val totalFetchWaitMs = stageInfo.taskMetrics.shuffleReadMetrics.fetchWaitTime
      val avgFetchWaitMs = totalFetchWaitMs / completedTasks

      if (avgFetchWaitMs >= fetchWaitThresholdMs) {
        // Log once per shuffle attempt
        val shuffleId = stage.shuffleDep.shuffleId
        val attemptKey = shuffleId * 1000 + stageInfo.attemptNumber()
        if (loggedFetchWaitGating.putIfAbsent(attemptKey, true) == null) {
          logInfo(s"Skipping shuffle rebalance for shuffle $shuffleId " +
            s"(stage ${stage.id}, attempt ${stageInfo.attemptNumber()}) due to high fetch wait: " +
            s"avgFetchWaitMs=$avgFetchWaitMs >= threshold=$fetchWaitThresholdMs")
        }
        return
      }
    }
  }

  val completionRatio = completedTasks.toDouble / stage.numTasks
  if (completionRatio < 0.25 || completionRatio >= 1.0) return

  // ... rest of existing logic
}
```

### 3. Test Cases

**File**: `core/src/test/scala/org/apache/spark/scheduler/ShuffleRebalanceManagerSuite.scala`

Add new test cases:

```scala
test("shuffle rebalance gated when fetch wait is high") {
  val conf = new SparkConf()
    .set(SHUFFLE_REBALANCE_ENABLED, true)
    .set(SHUFFLE_REBALANCE_FETCH_WAIT_THRESHOLD_MS, 1000L) // 1 second threshold

  val manager = new ShuffleRebalanceManager(conf, mockMapOutputTracker, mockBlockManagerMaster)

  // Create stage with high average fetch wait time
  val stage = createMockStage(
    shuffleId = 1,
    numTasks = 100,
    completedTasks = 50,
    totalFetchWaitMs = 60000L  // 60 seconds total = 1200ms avg per task
  )

  // Should skip rebalancing due to high fetch wait
  manager.checkAndInitiateShuffleRebalance(stage, 50)

  // Verify no rebalance operations were planned
  verify(mockMapOutputTracker, never()).getMapSizesByExecutorId(anyInt(), anyInt())
}

test("shuffle rebalance proceeds when fetch wait is below threshold") {
  val conf = new SparkConf()
    .set(SHUFFLE_REBALANCE_ENABLED, true)
    .set(SHUFFLE_REBALANCE_FETCH_WAIT_THRESHOLD_MS, 1000L)

  val manager = new ShuffleRebalanceManager(conf, mockMapOutputTracker, mockBlockManagerMaster)

  // Create stage with low average fetch wait time
  val stage = createMockStage(
    shuffleId = 1,
    numTasks = 100,
    completedTasks = 50,
    totalFetchWaitMs = 25000L  // 25 seconds total = 500ms avg per task
  )

  // Should proceed with rebalancing check
  manager.checkAndInitiateShuffleRebalance(stage, 50)

  // Verify rebalance check was performed
  verify(mockMapOutputTracker).getMapSizesByExecutorId(eq(1), anyInt())
}

test("shuffle rebalance gate disabled when threshold is 0") {
  val conf = new SparkConf()
    .set(SHUFFLE_REBALANCE_ENABLED, true)
    .set(SHUFFLE_REBALANCE_FETCH_WAIT_THRESHOLD_MS, 0L) // Disabled

  val manager = new ShuffleRebalanceManager(conf, mockMapOutputTracker, mockBlockManagerMaster)

  // Create stage with very high fetch wait
  val stage = createMockStage(
    shuffleId = 1,
    numTasks = 100,
    completedTasks = 50,
    totalFetchWaitMs = 600000L  // 10 minutes total
  )

  // Should still proceed since gate is disabled
  manager.checkAndInitiateShuffleRebalance(stage, 50)

  verify(mockMapOutputTracker).getMapSizesByExecutorId(eq(1), anyInt())
}

test("shuffle rebalance handles missing task metrics gracefully") {
  val conf = new SparkConf()
    .set(SHUFFLE_REBALANCE_ENABLED, true)
    .set(SHUFFLE_REBALANCE_FETCH_WAIT_THRESHOLD_MS, 1000L)

  val manager = new ShuffleRebalanceManager(conf, mockMapOutputTracker, mockBlockManagerMaster)

  // Create stage with null task metrics
  val stage = createMockStageWithNullMetrics(shuffleId = 1, numTasks = 100, completedTasks = 50)

  // Should skip the fetch wait check and proceed to other gates
  manager.checkAndInitiateShuffleRebalance(stage, 50)

  // Should not throw exception
  verify(mockMapOutputTracker).getMapSizesByExecutorId(eq(1), anyInt())
}

test("shuffle rebalance logs fetch wait gating once per attempt") {
  val conf = new SparkConf()
    .set(SHUFFLE_REBALANCE_ENABLED, true)
    .set(SHUFFLE_REBALANCE_FETCH_WAIT_THRESHOLD_MS, 1000L)

  val manager = new ShuffleRebalanceManager(conf, mockMapOutputTracker, mockBlockManagerMaster)

  // Create stage with high fetch wait
  val stage = createMockStage(
    shuffleId = 1,
    numTasks = 100,
    completedTasks = 50,
    totalFetchWaitMs = 60000L,
    attemptNumber = 0
  )

  // Call multiple times - should only log once
  manager.checkAndInitiateShuffleRebalance(stage, 50)
  manager.checkAndInitiateShuffleRebalance(stage, 51)
  manager.checkAndInitiateShuffleRebalance(stage, 52)

  // Verify logging occurred only once (implementation-specific verification)
}
```

### 4. Edge Cases Handled

1. **Threshold disabled** (≤ 0): Skip the check entirely, preserve existing behavior
2. **No completed tasks**: Skip check since `completedTasks = 0` would cause division by zero
3. **Null task metrics**: Skip check gracefully, proceed to other gates
4. **Multiple stage attempts**: Use `shuffleId * 1000 + attemptNumber` as unique key for logging
5. **Zero fetch wait**: Passes the check (0 < threshold)

### 5. Observability

**Logging Strategy**:
- Log at INFO level when rebalancing is skipped due to high fetch wait
- Include shuffle ID, stage ID, attempt number, average fetch wait, and threshold
- Use `ConcurrentHashMap` to track logged attempts and avoid spam
- Clear format: `"Skipping shuffle rebalance for shuffle X (stage Y, attempt Z) due to high fetch wait: avgFetchWaitMs=A >= threshold=B"`

**Metrics** (optional future enhancement):
- Could add `rebalanceSkippedDueToFetchWait` counter to `ShuffleRebalanceManagerSource`

## Files to Modify

1. **core/src/main/scala/org/apache/spark/internal/config/package.scala**
   - Add `SHUFFLE_REBALANCE_FETCH_WAIT_THRESHOLD_MS` configuration

2. **core/src/main/scala/org/apache/spark/scheduler/ShuffleRebalanceManager.scala**
   - Add `fetchWaitThresholdMs` field
   - Add `loggedFetchWaitGating` tracking map
   - Add fetch wait gate logic in `checkAndInitiateShuffleRebalance()`

3. **core/src/test/scala/org/apache/spark/scheduler/ShuffleRebalanceManagerSuite.scala**
   - Add 5 new test cases covering various scenarios

## Testing Strategy

1. **Unit tests**: Verify gate logic with mocked stage info and metrics
2. **Integration tests**: Verify gate works end-to-end with real task metrics
3. **Negative tests**: Verify threshold ≤ 0 disables the gate
4. **Edge case tests**: Null metrics, zero tasks, multiple attempts

## Migration Path

- Default threshold is 0 (disabled), so existing deployments are unaffected
- Users can opt-in by setting `spark.shuffle.rebalance.fetchWaitThresholdMs > 0`
- Recommended starting value: 500-1000ms based on workload characteristics
- Can be tuned based on observed fetch wait patterns in production

## Implementation Order

1. Add configuration parameter
2. Add fetch wait gate logic
3. Add unit tests
4. Manual testing with various threshold values
5. Documentation update (if applicable)

## Risks and Considerations

**Low Risk**:
- Additive change, doesn't modify existing logic
- Default behavior unchanged (threshold = 0)
- Early return pattern matches existing gates
- No performance impact when disabled

**Considerations**:
- Threshold value is workload-dependent; users need to tune based on their environment
- Very low thresholds might prevent beneficial rebalancing
- Very high thresholds effectively disable the gate
- Consider documenting recommended threshold ranges

## Success Criteria

- All unit tests pass
- No regression in existing shuffle rebalance tests
- Gate correctly prevents rebalancing when fetch wait exceeds threshold
- Gate correctly allows rebalancing when fetch wait is below threshold
- Gate is disabled when threshold ≤ 0
- Logging occurs once per shuffle attempt without spam
