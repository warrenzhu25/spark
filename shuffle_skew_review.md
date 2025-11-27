# Review: Shuffle Skew Executor Filtering

## Issue 1 – Average task count divides by currently free offers, not real executor participation
- Evidence: `core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala:399-425` passes `shuffledOffers.length` into `TaskSetManager.filterShuffleSkewExecutors`, and `core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala:1311-1344` uses that count inside `getAverageTaskNum`.
- `shuffledOffers` only contains executors that happened to have free cores in the current scheduling wave. When the cluster is busy, this number can be orders of magnitude smaller than the executors that already processed shuffle tasks.
- Example: 100 map tasks have finished but only 2 executors report offers in this round. The average becomes `100 / 2 = 50`, so the skew threshold is `50 * 1.5 = 75`. Even an executor that has already produced all 100 outputs will not be marked skewed. The actual average across the 100 executors that processed tasks is `1`, so the executor with 100 outputs should clearly be filtered.
- Result: skew detection almost never triggers once the stage saturates the cluster, because the denominator shrinks whenever the cluster is busy. Executors that caused the skew continue to receive tasks.
- Fix: compute the denominator from the executors that have produced shuffle outputs (or the overall executor pool) rather than the transient list of offers. `TaskSchedulerImpl` already tracks `executorIdToRunningTaskIds` and knows how many executors exist; pass that value to `getSkewedExecutors` so the average represents the actual distribution.

## Issue 2 – Ratio cap ignores specification of “total executors”
- Evidence: `core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala:1312-1314` derives `maxSkewedNum` from `finishedTasksByExecutorId.size`, while the config doc in `core/src/main/scala/org/apache/spark/internal/config/package.scala:2102-2116` states “Maximum number/ratio of total executors being considered as skew.”
- Because the code only counts executors that have *already* finished a task, the ratio is silently applied to a tiny subset of the cluster during most of the stage. With the defaults (5% + absolute cap), a 200-executor stage where only 5 executors have reported completions can filter at most `ceil(5 * 0.05) = 1` executor even if 3 or 4 executors are already running away with the workload.
- Impact: the filtered list lags far behind the actual number of executors contributing to the skew. Most skewed executors continue to receive work because the cap prevents them from being added until many more executors finish tasks.
- Fix: derive the cap from the same executor count used in the average (all executors participating in the stage or at least the number of executors with offers), and then clamp with `shuffleSkewMaxExecutorsNum`. That keeps the implementation aligned with the documented semantics of “ratio of total executors.”

## Issue 3 – `shuffleSkewMinFinishedTasks` is misapplied
- Evidence: the config states “Minimum number of finished shuffle map tasks on one executor before being considered as skew” (`core/src/main/scala/org/apache/spark/internal/config/package.scala:2094-2100`). The implementation folds this value into the average (`core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala:1339-1344`) by taking `math.max(tasksSuccessful / totalExecutors, shuffleSkewMinFinishedTasks)` and then multiplying by the skew ratio.
- Consequence: the minimum is effectively multiplied by the ratio. With the defaults (min=10, ratio=1.5) an executor must finish at least 15 tasks before it even becomes eligible for filtering, regardless of how imbalanced the rest of the stage is. Small or moderate shuffles (e.g., 6 partitions finishing on one executor while the others are idle) can never trigger the feature.
- This also ignores the documented semantics: the minimum should act as a guard (“only evaluate skew once an executor has at least N completions”), not as an inflated baseline for the ratio calculation.
- Fix: remove the `math.max` floor in `getAverageTaskNum`, and instead require `numOutputs >= shuffleSkewMinFinishedTasks` alongside the ratio check, i.e.
  ```scala
  val skewedExecutors = finishedTasksByExecutorId.collect {
    case (exec, numOutputs) if numOutputs >= shuffleSkewMinFinishedTasks &&
        numOutputs >= averageTaskNum * shuffleSkewRatio => exec -> numOutputs
  }
  ```
  This keeps the average tied to the real distribution while still enforcing the user-configured minimum sample size.
