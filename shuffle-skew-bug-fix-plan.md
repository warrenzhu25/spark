# Bug Fix Plan: Shuffle Skew Executor Implementation

## Executive Summary

Found **3 critical bugs** and **2 logic errors** in commit 11e5783fb4b that implements shuffle skew executor filtering. The most severe bugs cause incorrect executor selection, wrong task placement, and resource assignment errors when executors are filtered out for being skewed.

### Context
The commit adds functionality to detect and exclude "shuffle skewed" executors (executors that have processed significantly more shuffle data than average) from receiving new tasks. This aims to reduce long shuffle fetch waits caused by uneven workload distribution.

The implementation made a fundamental change from array-based indexing to map-based resource tracking, but introduced critical bugs where filtered indices don't align with original indices.

## Critical Bugs Identified

### Bug #1: Index Mismatch in Resource Profile Check (CRITICAL)
**Location:** `TaskSchedulerImpl.scala:408`

**Root Cause Analysis:**
The code filters executors but then uses the wrong array index for resource profile checking.

**Problematic Code:**
```scala
// Line 399: Create filtered list (some executors removed)
val filteredOffers = filterShuffleSkewExecutors(taskSet, shuffledOffers)

// Line 400: Loop over FILTERED offers
for (i <- filteredOffers.indices) {
  val execId = filteredOffers(i).executorId    // Uses filteredOffers
  val host = filteredOffers(i).host            // Uses filteredOffers

  // Line 407-408: BUG - Uses shuffledOffers with filteredOffers index!
  if (sc.resourceProfileManager
    .canBeScheduled(taskSetRpID, shuffledOffers(i).resourceProfileId)) {
```

**Why This is Wrong:**
When executors are filtered out, the two arrays have different contents at the same index position.

**Concrete Example:**
```
Initial shuffledOffers:
  Index 0: exec0 (resourceProfileId=1)
  Index 1: exec1 (resourceProfileId=1)  ← SKEWED, will be filtered out
  Index 2: exec2 (resourceProfileId=2)

After filtering → filteredOffers:
  Index 0: exec0 (resourceProfileId=1)
  Index 1: exec2 (resourceProfileId=2)  ← Index 1 now refers to exec2!

When i=1:
  - We're processing filteredOffers(1) = exec2 (rpId=2)
  - But checking shuffledOffers(1) = exec1 (rpId=1)
  - We check exec1's resource profile for exec2's task!
  - Result: Wrong compatibility check
```

**Actual Impact:**
1. **Wrong executor selection:** Tasks may be scheduled on executors with incompatible resource profiles
2. **Missed opportunities:** Compatible executors may be skipped due to wrong profile check
3. **Potential crashes:** ArrayIndexOutOfBoundsException if filteredOffers is longer than shuffledOffers (shouldn't happen but defensive)

**Fix:**
```scala
// Line 408: Use filteredOffers instead of shuffledOffers
.canBeScheduled(taskSetRpID, filteredOffers(i).resourceProfileId))
```

**Verification:**
After fix, the same index `i` consistently refers to the same executor in filteredOffers throughout the loop.

---

### Bug #2: Wrong Index for Tasks Array (CRITICAL)
**Location:** `TaskSchedulerImpl.scala:420, 426, 748-749`

**Root Cause Analysis:**
The `tasks` array is created and indexed based on `shuffledOffers`, but the code uses indices from `filteredOffers` to access it. This causes tasks to be placed in wrong positions, leading to execution on wrong executors.

**The Data Flow Problem:**

**Step 1 - Array Creation (Line 565):**
```scala
// tasks array is created from shuffledOffers (BEFORE filtering)
val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores / CPUS_PER_TASK))

// This creates an array where:
// tasks(0) corresponds to shuffledOffers(0)
// tasks(1) corresponds to shuffledOffers(1)
// tasks(2) corresponds to shuffledOffers(2)
// etc.
```

**Step 2 - Filtering (Line 399):**
```scala
// Some executors are removed
val filteredOffers = filterShuffleSkewExecutors(taskSet, shuffledOffers)

// Now filteredOffers has DIFFERENT indices!
```

**Step 3 - Wrong Access (Lines 420, 426):**
```scala
for (i <- filteredOffers.indices) {  // i is index in FILTERED list
  val execId = filteredOffers(i).executorId

  // Line 420: BUG - uses filteredOffers index to access shuffledOffers-sized array
  tasks(i) += task

  // Line 426: BUG - stores filteredOffers index for later use
  barrierTask.assignedOfferIndex = i
}
```

**Step 4 - Later Usage for Barrier Tasks (Lines 748-749):**
```scala
// Uses the wrong index stored earlier
tasks(task.assignedOfferIndex) += taskDesc
shuffledOffers(task.assignedOfferIndex).address.get -> taskDesc
```

**Concrete Example:**
```
Initial shuffledOffers (tasks array sized for this):
  Index 0: exec0
  Index 1: exec1 ← SKEWED, will be filtered
  Index 2: exec2
  Index 3: exec3

tasks array (aligned with shuffledOffers):
  tasks(0) = ArrayBuffer for exec0
  tasks(1) = ArrayBuffer for exec1
  tasks(2) = ArrayBuffer for exec2
  tasks(3) = ArrayBuffer for exec3

After filtering → filteredOffers:
  Index 0: exec0
  Index 1: exec2  ← exec1 removed, indices shift!
  Index 2: exec3

When scheduling a task at i=1:
  - We're scheduling on filteredOffers(1) = exec2
  - But we do: tasks(1) += task
  - tasks(1) is for exec1, NOT exec2!
  - Result: Task intended for exec2 added to exec1's task list

When launching barrier tasks:
  - assignedOfferIndex = 1 (from filteredOffers)
  - Later: shuffledOffers(1) = exec1
  - We launch on exec1 instead of exec2!
```

**Actual Impact:**
1. **Tasks launched on wrong executors:** Regular tasks end up in wrong executor's task buffer
2. **Barrier task failures:** Barrier tasks record wrong executor and fail coordination
3. **Resource accounting errors:** CPUs/resources decremented for wrong executor
4. **Data locality violations:** Tasks scheduled far from their preferred data locations
5. **Potential ArrayIndexOutOfBoundsException:** If filteredOffers has more elements than expected (edge case)

**Fix Strategy:**
Create an index mapping that translates filteredOffers indices to shuffledOffers indices.

```scala
// After line 399, create mapping
val offerIndexMap = filteredOffers.map(offer => shuffledOffers.indexOf(offer))

// This creates: [0, 2, 3] in our example above
// Meaning:
//   filteredOffers(0) → shuffledOffers(0)
//   filteredOffers(1) → shuffledOffers(2)
//   filteredOffers(2) → shuffledOffers(3)

// Then use mapped index when accessing arrays:
tasks(offerIndexMap(i)) += task                    // Line 420
barrierTask.assignedOfferIndex = offerIndexMap(i)  // Line 426
```

**Why This Fix Works:**
- `offerIndexMap(i)` gives us the correct position in the original shuffledOffers
- This position correctly indexes into the tasks array
- Barrier tasks store the correct original index for later use
- All array accesses remain aligned with shuffledOffers

---

### Bug #3: Incorrect Average Calculation (MEDIUM)
**Location:** `TaskSetManager.scala:1339-1345`

**Root Cause Analysis:**
The average calculation only considers executors that have completed at least one task, not all available executors. This skews the average upward and causes false positives in skew detection.

**Problematic Code:**
```scala
private def getAverageTaskNum() = {
  if (finishedTasksByExecutorId.nonEmpty) {
    // BUG: Divides by number of executors in the map, not total executors
    math.max(tasksSuccessful / finishedTasksByExecutorId.size, shuffleSkewMinFinishedTasks)
  } else {
    shuffleSkewMinFinishedTasks
  }
}
```

**Why This is Wrong:**
`finishedTasksByExecutorId` is a HashMap that only contains entries for executors that have finished at least one task. Idle executors are not in this map.

**Concrete Example:**
```
Scenario: 4 executors, 90 tasks completed

Distribution:
  exec0: 50 tasks ← looks skewed
  exec1: 40 tasks ← looks skewed
  exec2: 0 tasks  ← not in finishedTasksByExecutorId map!
  exec3: 0 tasks  ← not in finishedTasksByExecutorId map!

Current calculation:
  finishedTasksByExecutorId = {exec0: 50, exec1: 40}
  finishedTasksByExecutorId.size = 2
  tasksSuccessful = 90
  average = 90 / 2 = 45

What gets marked as skewed (ratio=1.5):
  exec0: 50 >= 45 * 1.5 = 67.5 ❌ Not skewed by this threshold
  exec1: 40 >= 67.5 ❌ Not skewed

But wait, let's say ratio=1.0:
  exec0: 50 >= 45 * 1.0 = 45 ✓ Marked as skewed
  exec1: 40 >= 45 ❌ Not skewed

Correct calculation should be:
  totalExecutors = 4 (including idle ones)
  average = 90 / 4 = 22.5

  With ratio=1.5:
    exec0: 50 >= 22.5 * 1.5 = 33.75 ✓ Correctly skewed!
    exec1: 40 >= 33.75 ✓ Correctly skewed!
```

**Actual Impact:**
1. **False negatives:** Skewed executors not detected because average is artificially inflated
2. **Inconsistent behavior:** Detection varies based on number of idle executors
3. **Defeats the purpose:** The feature fails to catch skew in common scenarios
4. **Configuration confusion:** Users must over-tune ratios to compensate for the bug

**Real-World Scenario:**
In a cluster with 100 executors where:
- 10 executors process all tasks (due to data locality)
- 90 executors are idle
- Current: average = total/10 (very high, nothing marked as skewed)
- Correct: average = total/100 (much lower, busy executors marked as skewed)

**Fix:**
Pass total executor count from TaskSchedulerImpl where it's known.

**Changes Required:**

1. **Update method signature (TaskSetManager.scala:1307):**
```scala
// Add parameter
def getSkewedExecutors(totalExecutors: Int): Set[String]
```

2. **Fix calculation (TaskSetManager.scala:1339-1345):**
```scala
private def getAverageTaskNum(totalExecutors: Int) = {
  if (tasksSuccessful > 0 && totalExecutors > 0) {
    math.max(tasksSuccessful / totalExecutors, shuffleSkewMinFinishedTasks)
  } else {
    shuffleSkewMinFinishedTasks
  }
}

// Update caller in getSkewedExecutors:
val averageTaskNum = getAverageTaskNum(totalExecutors)
```

3. **Update callers (TaskSchedulerImpl.scala):**
```scala
// When filtering, pass the total executor count
taskSet.getSkewedExecutors(shuffledOffers.length)
```

**Why This Fix Works:**
- Uses actual total number of executors available
- Average represents true distribution across cluster
- Skew detection works correctly even with idle executors
- Behavior is consistent and predictable

---

### Bug #4: Missing Minimum Threshold Check (MEDIUM)
**Location:** `TaskSetManager.scala:1316-1318`

**Root Cause Analysis:**
The skew detection only enforces the ratio threshold, not the minimum task count threshold. This allows executors with very few tasks to be marked as skewed based purely on ratio, even when the sample size is too small to be meaningful.

**Problematic Code:**
```scala
val skewedExecutors = finishedTasksByExecutorId.filter { case (_, numOutputs) =>
  // Only checks ratio threshold!
  numOutputs >= averageTaskNum * shuffleSkewRatio
}.toSeq
```

**Configuration Context:**
```scala
// Default config value
private val shuffleSkewMinFinishedTasks = conf.get(SHUFFLE_SKEW_MIN_FINISHED_TASKS) // default: 10
private val shuffleSkewRatio = conf.get(SHUFFLE_SKEW_RATIO) // default: 1.5
```

The `shuffleSkewMinFinishedTasks` config exists but is only used in the average calculation (as a floor), not as a filter criterion!

**Concrete Example:**
```
Scenario: Early in stage execution

Task distribution:
  exec0: 3 tasks
  exec1: 2 tasks
  exec2: 2 tasks

averageTaskNum calculation:
  With Bug #3 still present: 7 / 3 = 2.33
  Or with Bug #3 fixed: 7 / 3 = 2.33 (same in this case)
  But floor applied: max(2.33, 10) = 10

Skew check with ratio=1.5:
  Threshold = 10 * 1.5 = 15
  exec0: 3 >= 15? ❌ Not skewed (correct)

Now let's say averageTaskNum wasn't floored:
  averageTaskNum = 2.33
  Threshold = 2.33 * 1.5 = 3.5
  exec0: 3 >= 3.5? ❌ Not skewed

But what if distribution was:
  exec0: 4 tasks
  exec1: 2 tasks
  exec2: 1 task

  averageTaskNum = max(7/3, 10) = 10
  Threshold = 10 * 1.5 = 15
  exec0: 4 >= 15? ❌ Not skewed

Without the floor in average (hypothetically):
  averageTaskNum = 2.33
  Threshold = 2.33 * 1.5 = 3.5
  exec0: 4 >= 3.5? ✓ Marked as skewed with only 4 tasks!

The issue: Even though shuffleSkewMinFinishedTasks=10, an executor
could theoretically be marked skewed with fewer tasks if the average
calculation didn't apply the floor. The filter should explicitly check.
```

**Clearer Example - After Bug #3 Fix:**
```
After fixing Bug #3, average will use total executors:

Scenario:
  10 executors total
  exec0: 8 tasks
  exec1: 2 tasks
  exec2: 0 tasks
  ... (rest idle)

  averageTaskNum = 10 / 10 = 1
  But floored: max(1, 10) = 10

  Threshold = 10 * 1.5 = 15
  exec0: 8 >= 15? ❌ Not skewed (correct due to floor)

Without floor:
  averageTaskNum = 1
  Threshold = 1 * 1.5 = 1.5
  exec0: 8 >= 1.5? ✓ Marked as skewed with only 8 tasks!
  exec1: 2 >= 1.5? ✓ Marked as skewed with only 2 tasks!
```

**Actual Impact:**
1. **Premature filtering:** Executors marked as skewed before enough tasks complete
2. **Sample size too small:** Statistical significance requires minimum samples
3. **Config intent violated:** `shuffleSkewMinFinishedTasks` config is meant to prevent this
4. **Unstable behavior:** Early in execution, skew detection is unreliable

**The Intent:**
An executor should only be considered skewed if:
1. It has at least `shuffleSkewMinFinishedTasks` tasks (sufficient sample)
2. AND it exceeds the average by the ratio threshold (actual skew)

**Fix:**
Add explicit minimum threshold check:
```scala
val skewedExecutors = finishedTasksByExecutorId.filter { case (_, numOutputs) =>
  // Both conditions must be true:
  numOutputs >= shuffleSkewMinFinishedTasks &&        // Sufficient sample size
  numOutputs >= averageTaskNum * shuffleSkewRatio     // Actual skew detected
}.toSeq
```

**Why This Fix Works:**
- Enforces minimum sample size for statistical validity
- Prevents premature skew detection
- Makes config behavior explicit and predictable
- Aligns with user expectations from config documentation

---

### Bug #5: Unclear filteredSkewExecutors Counter (LOW)
**Location:** `TaskSetManager.scala:121, 1335, 595`

**Root Cause Analysis:**
The `filteredSkewExecutors` counter accumulates across multiple scheduling rounds but its purpose and semantics are unclear. It only logs at task set completion, making it unclear what value it provides.

**Problematic Code:**
```scala
// Line 121: Declaration
private val filteredSkewExecutors = new mutable.HashMap[String, Int]().withDefaultValue(0)

// Line 1335: Incremented on EVERY resourceOffers call
skewedExecutors.foreach(e => filteredSkewExecutors(e) += 1)

// Line 595: Only logged when task set finishes
if (filteredSkewExecutors.nonEmpty) {
  logInfo(s"Filtered shuffle skew executors for stage $stageId is: $filteredSkewExecutors")
}
```

**Issues:**

1. **Unclear semantics:** What does the count represent?
   - Number of scheduling rounds where executor was skewed?
   - Total opportunities where executor was filtered?
   - Something else?

2. **Unbounded growth:** On long-running stages with frequent scheduling:
   ```
   Example: Stage runs for 1 hour, resourceOffers called every 100ms
   - 36,000 scheduling rounds
   - If exec0 is consistently skewed: filteredSkewExecutors(exec0) = 36,000
   - This number doesn't provide actionable insight
   ```

3. **Only for shuffle map tasks:** Counter updated in `filterShuffleSkewExecutors` which returns early for non-shuffle tasks, creating inconsistent semantics.

4. **Delayed feedback:** Only logged at completion, can't monitor during execution.

**Potential Intent:**
Likely meant to provide observability into how often executors were excluded due to skew, but the implementation doesn't serve this well.

**Better Alternatives:**

**Option A - Document and Keep:**
```scala
// Counter tracking how many scheduling rounds each executor was filtered for skew.
// Used for post-execution analysis to understand filtering impact.
// Note: Can grow large on long-running stages with frequent scheduling.
private val filteredSkewExecutors = new mutable.HashMap[String, Int]().withDefaultValue(0)
```

**Option B - Track First/Last Filtered (more useful):**
```scala
// Track when executors were first and last marked as skewed
private val skewedExecutorTimestamps = new mutable.HashMap[String, (Long, Long)]()

// In filterShuffleSkewExecutors:
val now = System.currentTimeMillis()
skewedExecutors.foreach { e =>
  skewedExecutorTimestamps.get(e) match {
    case Some((first, _)) => skewedExecutorTimestamps(e) = (first, now)
    case None => skewedExecutorTimestamps(e) = (now, now)
  }
}
```

**Option C - Remove Entirely:**
If not providing value, remove the counter to simplify code.

**Recommendation:**
**Option A** - Add clear documentation. The counter provides some observability even if imperfect. In the future, could enhance with metrics export.

---

## Implementation Plan

### Phase 1: Fix Critical Bugs (High Priority)

#### Step 1.1: Fix Resource Profile Index Bug
**File:** `core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala:408`

Change:
```scala
.canBeScheduled(taskSetRpID, shuffledOffers(i).resourceProfileId))
```

To:
```scala
.canBeScheduled(taskSetRpID, filteredOffers(i).resourceProfileId))
```

**Test:** Run `TaskSchedulerImplSuite` to verify no regressions.

---

#### Step 1.2: Fix tasks Array Indexing with Mapping
**File:** `core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala`

**Context:** The `tasks` array is sized based on `shuffledOffers`, but we iterate over `filteredOffers`. We need to map filtered indices back to original indices.

**Detailed Changes:**

**Change 1 - Create Index Mapping (After line 399):**

Find this code:
```scala
val filteredOffers = filterShuffleSkewExecutors(taskSet, shuffledOffers)
for (i <- filteredOffers.indices) {
```

Add the index mapping right after `filterShuffleSkewExecutors`:
```scala
val filteredOffers = filterShuffleSkewExecutors(taskSet, shuffledOffers)
// Map filteredOffers indices to shuffledOffers indices
// This allows us to correctly access the tasks array which is sized for shuffledOffers
val offerIndexMap = filteredOffers.map(offer => shuffledOffers.indexOf(offer))
assert(offerIndexMap.forall(_ >= 0),
  "All filtered offers must exist in shuffled offers")
for (i <- filteredOffers.indices) {
```

**Change 2 - Fix tasks Array Access (Line 420):**

Find this code (~line 420):
```scala
for (task <- taskDescOption) {
  val (locality, resources) = if (task != null) {
    tasks(i) += task
```

Change to use mapped index:
```scala
for (task <- taskDescOption) {
  val (locality, resources) = if (task != null) {
    tasks(offerIndexMap(i)) += task
```

**Change 3 - Fix Barrier Task Index (Line 426):**

Find this code (~line 426):
```scala
} else {
  assert(taskSet.isBarrier, "TaskDescription can only be null for barrier task")
  val barrierTask = taskSet.barrierPendingLaunchTasks(index)
  barrierTask.assignedOfferIndex = i
```

Change to use mapped index:
```scala
} else {
  assert(taskSet.isBarrier, "TaskDescription can only be null for barrier task")
  val barrierTask = taskSet.barrierPendingLaunchTasks(index)
  barrierTask.assignedOfferIndex = offerIndexMap(i)
```

**Why This Works:**
- `offerIndexMap` is an `IndexedSeq[Int]` where `offerIndexMap(i)` gives the position in `shuffledOffers` that corresponds to `filteredOffers(i)`
- Example: If exec1 is filtered out, `offerIndexMap = IndexedSeq(0, 2, 3)` meaning filteredOffers(1) → shuffledOffers(2)
- Using `tasks(offerIndexMap(i))` ensures we access the correct executor's task buffer
- Barrier tasks store the correct original index for later lookup

**Verification After Changes:**
- Compile: `./build/sbt "core/compile"`
- Run tests: `./build/sbt "core/testOnly *TaskSchedulerImplSuite"`
- Run barrier task tests specifically

**Test:** Run full test suite, especially:
- `TaskSchedulerImplSuite` - general scheduler tests
- Barrier task tests - ensure barrier coordination still works
- Tests with shuffle skew filtering enabled - ensure filtering works correctly

---

### Phase 2: Fix Logic Errors (Medium Priority)

#### Step 2.1: Fix Average Calculation
**File:** `core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala`

1. **Update method signature (line 1307):**
```scala
def getSkewedExecutors(totalExecutors: Int): Set[String]
```

2. **Update getAverageTaskNum (line 1339):**
```scala
private def getAverageTaskNum(totalExecutors: Int) = {
  if (tasksSuccessful > 0 && totalExecutors > 0) {
    math.max(tasksSuccessful / totalExecutors, shuffleSkewMinFinishedTasks)
  } else {
    shuffleSkewMinFinishedTasks
  }
}
```

3. **Update callers:** Pass executor count from TaskSchedulerImpl
```scala
// In TaskSchedulerImpl.filterShuffleSkewExecutors:
taskSet.getSkewedExecutors(shuffledOffers.length)
```

**Test:** Update `TaskSetManagerSuite` tests to pass executor count.

---

#### Step 2.2: Add Minimum Threshold Check
**File:** `core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala:1316`

```scala
val skewedExecutors = finishedTasksByExecutorId.filter { case (_, numOutputs) =>
  numOutputs >= shuffleSkewMinFinishedTasks &&
  numOutputs >= averageTaskNum * shuffleSkewRatio
}.toSeq
```

**Test:** Add test case verifying executors below minFinishedTasks are not marked skewed.

---

### Phase 3: Code Quality Improvements (Low Priority)

#### Step 3.1: Document or Remove filteredSkewExecutors Counter

**Option 1 (Document):**
```scala
// Counter tracking how many scheduling rounds each executor was filtered for skew.
// Used for debugging and monitoring skew filtering effectiveness.
private val filteredSkewExecutors = new mutable.HashMap[String, Int]().withDefaultValue(0)
```

**Option 2 (Remove):** If not needed for monitoring, remove the counter entirely.

---

#### Step 3.2: Add Defensive Checks

Add assertions to catch indexing bugs early:
```scala
// After filtering
assert(filteredOffers.forall(o => tasks.contains(o.executorId)),
  "All filtered offers must have task entries")
```

---

### Phase 4: Testing (Critical)

#### Step 4.1: Add Missing Test Cases

**File:** `core/src/test/scala/org/apache/spark/scheduler/TaskSetManagerSuite.scala`

1. Test executor below minFinishedTasks not marked skewed
2. Test with idle executors (0 tasks on some executors)
3. Test tie-breaking when executors have equal task counts
4. Test maxExecutorsRatio with many executors (100+)
5. Test non-shuffle-map tasks (ResultTask) don't get filtered

**File:** `core/src/test/scala/org/apache/spark/scheduler/TaskSchedulerImplSuite.scala`

1. Replace mock-based test with actual ShuffleMapTask test
2. Test with multiple resource profiles and skewed executors
3. Test barrier tasks with filtered executors
4. Test edge case: all executors skewed
5. Test edge case: no executors skewed

---

#### Step 4.2: Run Scalastyle

```bash
./build/sbt "core/scalastyle"
```

Fix any violations (line length, trailing whitespace, etc.).

---

#### Step 4.3: Run Test Suite

```bash
./build/sbt "core/test"
```

Focus on:
- TaskSchedulerImplSuite
- TaskSetManagerSuite
- Barrier task tests

---

### Phase 5: Commit Strategy

Following the user's guidelines: "Every change in separate commit with test. Test must be passed."

**Commit 1:** Fix resource profile index bug
- File: `TaskSchedulerImpl.scala`
- Change: Line 408 only (use `filteredOffers(i)` instead of `shuffledOffers(i)`)
- Test: `./build/sbt "core/testOnly *TaskSchedulerImplSuite"`
- Verify: Tests pass
- Commit message: `"Fix resource profile check to use filteredOffers index"`
- Amend if needed for scalastyle

**Commit 2:** Fix tasks array indexing with mapping
- File: `TaskSchedulerImpl.scala`
- Changes:
  - Add `offerIndexMap` after line 399
  - Update line 420 to use `tasks(offerIndexMap(i))`
  - Update line 426 to use `barrierTask.assignedOfferIndex = offerIndexMap(i)`
- Test: `./build/sbt "core/testOnly *TaskSchedulerImplSuite"`
- Verify: All tests pass, especially barrier task tests
- Commit message: `"Fix tasks array indexing with offer index mapping"`
- Amend if needed for scalastyle

**Commit 3:** Fix average calculation to use total executors
- Files: `TaskSetManager.scala`, `TaskSchedulerImpl.scala`
- Changes:
  - Update `getSkewedExecutors` signature to take `totalExecutors: Int`
  - Update `getAverageTaskNum` to take and use `totalExecutors`
  - Update caller in `TaskSchedulerImpl` to pass `shuffledOffers.length`
  - Update test calls in `TaskSetManagerSuite`
- Test: `./build/sbt "core/testOnly *TaskSetManagerSuite"`
- Verify: Tests pass with new signature
- Commit message: `"Fix average task calculation to use total executor count"`
- Amend if needed for scalastyle

**Commit 4:** Add minimum threshold check for skew detection
- File: `TaskSetManager.scala`
- Change: Add `numOutputs >= shuffleSkewMinFinishedTasks &&` to filter at line 1316
- Test: Add test case, then run `./build/sbt "core/testOnly *TaskSetManagerSuite"`
- Verify: New test passes, existing tests pass
- Commit message: `"Add minimum threshold check for skew detection"`
- Amend if needed for scalastyle

**Commit 5:** Document filteredSkewExecutors counter purpose
- File: `TaskSetManager.scala`
- Change: Add documentation comment at line 121
- Test: `./build/sbt "core/compile"`
- Verify: Compiles cleanly
- Commit message: `"Document filteredSkewExecutors counter purpose"`
- Amend if needed for scalastyle

**Commit 6:** Add comprehensive test coverage
- Files: `TaskSchedulerImplSuite.scala`, `TaskSetManagerSuite.scala`
- Changes: Add all new edge case tests
- Test: `./build/sbt "core/test"`
- Verify: All tests pass
- Commit message: `"Add comprehensive test coverage for shuffle skew filtering"`
- Amend if needed for scalastyle

**Note:** Following user's requirement: "When fix error, fix should amend original commit" - If scalastyle or test failures occur, amend the related commit rather than creating fix commits.

---

## Risk Assessment

### High Risk
- **Bug #1 & #2:** Can cause production failures, data corruption, or crashes
- Must be fixed immediately
- Requires careful testing

### Medium Risk
- **Bug #3 & #4:** Cause incorrect behavior but not crashes
- Should be fixed before feature is widely used

### Low Risk
- **Bug #5:** Code quality issue, no functional impact

---

## Files to Modify

### Core Implementation Files

**1. `core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala`**
   - Line 408: Fix resource profile check (`filteredOffers(i)` instead of `shuffledOffers(i)`)
   - After line 399: Add `offerIndexMap` for index translation
   - Line 420: Use `tasks(offerIndexMap(i))` instead of `tasks(i)`
   - Line 426: Use `barrierTask.assignedOfferIndex = offerIndexMap(i)`
   - Line 452 (in `filterShuffleSkewExecutors`): Pass `shuffledOffers.length` to `getSkewedExecutors`

**2. `core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala`**
   - Line 121: Add documentation comment for `filteredSkewExecutors`
   - Line 1307: Update `getSkewedExecutors` signature to accept `totalExecutors: Int`
   - Line 1311: Pass `totalExecutors` to `getAverageTaskNum`
   - Line 1316: Add minimum threshold check (`numOutputs >= shuffleSkewMinFinishedTasks &&`)
   - Line 1339: Update `getAverageTaskNum` signature and implementation

### Test Files

**3. `core/src/test/scala/org/apache/spark/scheduler/TaskSchedulerImplSuite.scala`**
   - Replace mock-based shuffle skew test with actual implementation test
   - Add test for multiple resource profiles with skewed executors
   - Add test for barrier tasks with filtered executors
   - Add edge case tests (all executors skewed, none skewed)

**4. `core/src/test/scala/org/apache/spark/scheduler/TaskSetManagerSuite.scala`**
   - Update existing tests to pass `totalExecutors` parameter
   - Add test for executor below minFinishedTasks not marked skewed
   - Add test with idle executors (0 tasks)
   - Add test for tie-breaking with equal task counts
   - Add test for maxExecutorsRatio with many executors
   - Add test for non-shuffle-map tasks

---

## Success Criteria

- [ ] All critical bugs fixed (Bugs #1 and #2)
- [ ] All logic errors fixed (Bugs #3 and #4)
- [ ] Code quality improved (Bug #5 documented)
- [ ] All tests pass (existing + new tests)
- [ ] No scalastyle violations
- [ ] Code compiles without warnings
- [ ] New test coverage for edge cases added
- [ ] Clear commit history with passing tests per commit
- [ ] Each commit follows Spark's scalastyle rules

---

## Summary

### What This Plan Fixes

This plan addresses 5 bugs found in the shuffle skew executor filtering feature (commit 11e5783fb4b):

#### Critical Bugs (Must Fix Immediately)
1. **Resource Profile Index Mismatch** - Tasks could be scheduled on executors with wrong resource profiles
2. **Tasks Array Index Mismatch** - Tasks placed in wrong executor buffers, barrier tasks fail

#### Logic Errors (Should Fix)
3. **Incorrect Average Calculation** - Average doesn't account for idle executors, causing missed skew detection
4. **Missing Minimum Threshold** - Executors marked as skewed with too few tasks for statistical validity

#### Code Quality (Nice to Have)
5. **Unclear Counter Semantics** - `filteredSkewExecutors` counter purpose is unclear

### Key Implementation Decisions

**Chosen Approach for Bug #2:** Index mapping (Option B)
- Keeps existing array structure intact
- Minimal, conservative changes
- Maps filtered indices → original shuffled indices
- Lower risk than restructuring to Maps

**Rationale:**
- Avoids changing BarrierPendingLaunchTask case class
- Less code churn, easier to review
- Sufficient to fix the bug correctly

### Expected Outcomes

**After Fixes:**
- ✅ Tasks correctly scheduled to intended executors
- ✅ Resource profile compatibility checked accurately
- ✅ Barrier tasks coordinate correctly
- ✅ Skew detection works with idle executors
- ✅ Statistical significance enforced for skew detection
- ✅ Feature behavior matches config documentation

**Estimated Effort:**
- Implementation: ~4-6 commits
- Testing: Comprehensive test suite additions
- Time: 2-4 hours (depends on test failures to debug)

### Next Steps

1. **Review this plan** - Confirm approach and priorities
2. **Exit plan mode** - Begin implementation
3. **Follow commit strategy** - One commit per fix, tests must pass
4. **Run scalastyle after each commit** - Amend if violations found
5. **Test thoroughly** - Especially barrier tasks and resource profiles

---

## Questions for Review

Before proceeding, please confirm:

1. ✅ **Approach confirmed:** Index mapping (Option B) for Bug #2 - already confirmed by user
2. **Priority order:** Fix critical bugs (#1, #2) first, then logic errors (#3, #4), then quality (#5) - is this correct?
3. **Commit strategy:** 6 separate commits with tests passing between each - acceptable?
4. **Test coverage:** Should we add performance tests or just correctness tests?

---

*End of Plan*
