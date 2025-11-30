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
1.  **Ratio Check:** Its shuffle data size exceeds the cluster average by a factor defined in `spark.shuffle.rebalance.threshold` (default 1.5x).
2.  **Absolute Size Check:** The difference between the max and min executor sizes exceeds `spark.shuffle.rebalance.minSizeMB` (default 100MB).

### 3.2 Rebalancing Strategy (Greedy)
The system employs a greedy planning algorithm:
1.  Sort executors by shuffle data size.
2.  Identify **Source** executors (those above the threshold) and **Target** executors (those below the threshold).
3.  Pair Sources with Targets.
4.  Select specific shuffle blocks (starting with smaller ones for granularity) to move from Source to Target until the Target reaches roughly the average load.

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
