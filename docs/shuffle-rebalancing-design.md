# Design Document: Spark Shuffle Rebalancing

## 1. Overview
The Shuffle Rebalancing feature addresses data skew issues in Spark shuffle operations. In scenarios where certain executors accumulate significantly more shuffle data than others, performance bottlenecks occur, leading to straggler tasks and potential OOM errors. This feature introduces an automated mechanism to detect such imbalances and proactively move shuffle blocks from over-utilized executors to under-utilized ones.

## 2. Architecture
The system is composed of three main components:

### 2.1 ShuffleRebalanceManager (Driver Side)
*   **Role:** The "Brain" of the operation.
*   **Responsibility:**
    *   Monitors shuffle data distribution across executors using `MapOutputTracker`.
    *   Detects skewed executors based on configurable thresholds.
    *   Plans rebalancing operations (calculating which blocks to move and where).
    *   Coordinates the execution via a dedicated thread pool to avoid blocking the main scheduler.

### 2.2 Shuffle Block Transfer Service (Executor Side)
*   **Role:** The "Muscle" of the operation.
*   **Responsibility:**
    *   Extends `BlockManagerStorageEndpoint` to handle new rebalancing messages.
    *   Executes the actual data transfer between executors.
    *   Leverages existing `BlockManager.replicateBlock` functionality to ensure data integrity and create persistent replicas.

### 2.3 Configuration & Control
*   **Role:** Feature toggles and tuning.
*   **Responsibility:** Provides granular control over when rebalancing triggers (thresholds), how much data moves (min size), and resource usage (concurrency limits).

## 3. Detailed Design

### 3.1 Imbalance Detection Algorithm
The `ShuffleRebalanceManager` periodically checks the status of shuffle stages. An executor is considered for rebalancing if:
1.  **Eligibility Window:** Only runs once some tasks have finished but the stage is not done (`0.25 <= completedTasks/numTasks < 1.0`).
2.  **Ratio Check:** Its shuffle data size exceeds the cluster average by a factor defined in `spark.shuffle.rebalance.threshold` (default 1.5x). Today the code uses an asymmetric cut: sources are `> avg * threshold` and targets are `< avg / threshold`.
3.  **Absolute Size Check:** The difference between the max and min executor sizes exceeds `spark.shuffle.rebalance.minSizeMB` (default 100MB, computed as max−min).

### 3.2 Rebalancing Strategy (Greedy)
The system employs a greedy planning algorithm:
1.  Sort executors by shuffle data size.
2.  Identify **Source** executors (size > `avg * threshold`) and **Target** executors (size < `avg / threshold`). Sources are processed in descending size order so the largest holders are drained first.
3.  Pair Sources with Targets in order; for each pair compute `moveSize = min(sourceSize - avg, avg - targetSize)`.
4.  Skip pairs where `moveSize <= spark.shuffle.rebalance.minSizeMB`.
5.  Select specific shuffle blocks **on the source**:
    * Iterate map-output status entries located on the source executor.
    * Enumerate all `(shuffleId, mapId, partitionId)` blocks and their sizes; sort ascending by block size.
    * Pick blocks in ascending order until `moveSize` is met; small blocks go first to stay granular.
6.  Emit one `ShuffleRebalanceOperation` per source/target pair with the chosen blocks. Targets advance once they are within ~10% of the avg; otherwise the source pointer advances (current implementation’s pointer moves after one attempt, so sources rarely serve multiple targets).

### 3.3 Execution Flow
1.  **Trigger:** `ShuffleRebalanceManager` detects imbalance.
2.  **Plan:** A `ShuffleRebalanceOperation` is created, identifying the specific block IDs to move.
3.  **Command:** The Driver sends a `SendShuffleBlocks` message to the **Source Executor**.
4.  **Transfer:** The Source Executor reads the blocks and pushes them to the **Target Executor** via the `BlockManager`.
5.  **Update:** The Target Executor receives the blocks and reports the new block locations to the Driver (MapOutputTracker) via standard `UpdateBlockInfo` messages.
6.  **Persistence:** The system creates a dual-copy (multi-location) setup. The original block remains (or is cleaned up after a delay), and the new location is registered, allowing reducers to fetch from either.

## 4. Advanced Optimizations
To address the limitations of static concurrency limits and greedy algorithms, the following advanced mechanisms are proposed.

### 4.1 Adaptive Throttling
Replaces the static `maxConcurrent` limit with a feedback-driven resource budget to prevent network saturation and impact on active tasks.

*   **Bandwidth-Aware Rate Limiting:**
    *   **Concept:** Limit the *bytes per second* transferred rather than just the number of threads.
    *   **Mechanism:** Implement a token-bucket rate limiter on the executor side.
    *   **Configuration:** `spark.shuffle.rebalance.maxBandwidthPerExecutor` (e.g., 100 MB/s).
    *   **Benefit:** Ensures rebalancing traffic never consumes more than a set percentage (e.g., 10%) of available NIC bandwidth.

*   **Metric-Based Backpressure:**
    *   **Concept:** Pause rebalancing if the application layer shows signs of stress.
    *   **Monitor:** Watch `shuffleFetchWaitTime` metrics on executors.
    *   **Logic:**
        *   *Healthy (<10ms wait):* Increase rebalancing concurrency.
        *   *Stressed (>500ms wait):* Immediate pause/backoff of rebalancing.
    *   **Benefit:** Guarantees that active Spark tasks always take priority over background optimization.

### 4.2 Cost-Based Optimizer (CBO)
Replaces the simple "Greedy" strategy with an ROI (Return on Investment) calculation to ensure every data move yields a net positive performance gain.

*   **ROI Formula:**
    The system evaluates potential moves using: $$ \text{Score} = \text{Benefit (Time Saved)} - \text{Cost (Time Lost)} $$
    *   **Benefit:** `(Current_Size - Target_Size) / Est_Processing_Rate` (Time gained by the straggler).
    *   **Cost:** `Block_Size / Network_Bandwidth` + `Overhead_Penalty` (Time spent moving data).

*   **Heuristics & Rules:**
    1.  **The "Small Block" Rule:** Ignore blocks < 5MB. The IOPS/handshake overhead outweighs the balancing benefit.
    2.  **The "Topology" Penalty:** If Source and Target are on different racks, triple the calculated Cost. Cross-rack bandwidth is a scarce resource.
    3.  **The "Time Remaining" Check:** If the stage is >90% complete, abort rebalancing. The transfer won't finish before the stage does.

## 5. Configuration

| Parameter | Default | Description |
| :--- | :--- | :--- |
| `spark.shuffle.rebalance.enabled` | `false` | Master switch for the feature. |
| `spark.shuffle.rebalance.threshold` | `1.5` | Ratio (Max/Avg) to trigger rebalancing. |
| `spark.shuffle.rebalance.minSizeMB` | `100` | Minimum size diff (Max - Min) to justify a move. |
| `spark.shuffle.rebalance.checkIntervalMs` | `30000` | Frequency of imbalance checks. |
| `spark.shuffle.rebalance.maxConcurrent` | `2` | Max number of concurrent rebalancing tasks. |
| `spark.shuffle.rebalance.enableMultiLocation` | `true` | Keep multiple copies of blocks for fault tolerance. |

## 6. Review and Improvements

### Strengths
*   **Non-Blocking Design:** The use of `ThreadUtils.newDaemonFixedThreadPool` ensures that rebalancing logic does not block the main scheduling loop.
*   **Reuse of Existing Components:** Smartly leverages `BlockManager.replicateBlock` and `MapOutputTracker`, minimizing the surface area for new bugs.
*   **Safety Mechanisms:** The use of `ongoingMoves` (ConcurrentHashMap) prevents race conditions where the same block might be moved multiple times simultaneously.

### Concerns & Mitigations
*   **Network Saturation:** addressed by the proposed **Adaptive Throttling**.
*   **Inefficient Moves:** addressed by the proposed **Cost-Based Optimizer**.
*   **"While True" Loops:** The `getExecutorShuffleSizes` method currently uses exception-based flow control (`while(true) ... catch Exception`).
    *   **Fix:** Refactor to use proper iterators or direct access to partition counts from `ShuffleDependency`.
*   **Lack of Metrics:**
    *   **Fix:** Add `rebalance_bytes_moved`, `rebalance_ops_count`, and `rebalance_errors` to the metrics system for production visibility.
*   **Implementation gaps (current code):**
    * `BlockManagerId` for the target is hardcoded to localhost:7337; wire real IDs from the driver instead of placeholders.
    * `spark.shuffle.rebalance.enableMultiLocation` / `maxLocationsPerBlock` are not enforced when calling `replicateBlock`; add guardrails or cleanup to avoid exceeding replica limits.
    * There is no cleanup policy for extra replicas if multi-location is disabled; add delayed removal or TTL-driven cleanup.
    * No stage-completion gate beyond the 25–100% window; add a final check to avoid planning once the stage is almost done (ties into the ROI/time-remaining heuristic).
    * Planning stops after a single target per source due to pointer movement; adjust pairing so a hot source can feed multiple cold targets when needed.
    * Targets are chosen by size only; consider rack-awareness and a cap on parallel moves per target to prevent hotspotting.

## 7. New Gate: Disable Rebalance When Shuffle Fetch Wait Is High

### 7.1 Problem
Shuffle rebalancing is most useful when reducers are waiting on skewed sources. However, when average fetch wait for completed tasks is already high, extra transfers can worsen network congestion or extend stage time. We need a guard that avoids initiating new rebalancing once a shuffle map stage shows sustained high fetch wait in the tasks that already finished.

### 7.2 Signal
We can reuse existing shuffle read metrics already aggregated in `StageInfo`:
* `stage.latestInfo.taskMetrics.shuffleReadMetrics.fetchWaitTime` — cumulative fetch wait (milliseconds) across finished tasks in the current attempt.
* `completedTasks = stage.numTasks - stage.pendingPartitions.size` — count of successful tasks for the attempt.
Derive `avgFetchWaitMs = fetchWaitTime / completedTasks`, guarded for zero and null metrics.

### 7.3 Configuration
New driver-side conf to gate the feature:
* `spark.shuffle.rebalance.fetchWaitThresholdMs` (default `0` to preserve current behavior).
  * `<= 0`: gating disabled, existing behavior unchanged.
  * `> 0`: rebalance is skipped when `avgFetchWaitMs >= threshold`.
Optional follow-up (not required initially): a ratio guard comparing fetch wait to executor runtime, if a relative measure is preferred.

### 7.4 Driver-Side Flow
1. In `ShuffleRebalanceManager.checkAndInitiateShuffleRebalance`, before computing imbalance:
   * If gating is enabled, compute `avgFetchWaitMs` from the current stage attempt metrics.
   * If metrics are missing or `completedTasks == 0`, allow rebalancing (no signal yet).
2. When `avgFetchWaitMs >= threshold`, short-circuit and do not plan or launch moves for that shuffle attempt. Log once per shuffle attempt for observability.
3. Maintain a per-shuffle attempt flag (thread-safe map) to avoid repeated computation/log spam. Clear the flag on stage retry (new attempt id) so retries can reconsider.

### 7.5 Safety and Edge Cases
* Metrics are only available after at least one task finishes; before that we skip gating.
* Only successful task metrics are used; failed tasks do not contribute to `completedTasks`.
* Multi-attempt handling: reset gating state when the stage attempt id changes.
* Logging: include shuffle id, attempt, `avgFetchWaitMs`, threshold.

### 7.6 Testing Strategy
* Unit test: simulate a shuffle map stage with `completedTasks > 0` and synthetic `fetchWaitTime` exceeding the threshold; verify `checkAndInitiateShuffleRebalance` returns without planning moves.
* Unit test: with threshold disabled or below average, verify the existing imbalance check proceeds.
* Concurrency sanity: ensure the per-shuffle gating map is safe to read/write from the scheduler threads used by `ShuffleRebalanceManager`.
