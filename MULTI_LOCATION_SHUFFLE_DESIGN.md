# Design Doc: Multi-Location Shuffle Output

## 1. Overview
The Multi-Location Shuffle Output feature extends Spark's `MapOutputTracker` and `MapStatus` mechanisms to support registering and utilizing multiple storage locations for a single map output partition. This enables redundancy and load balancing for shuffle data, which is particularly useful in disaggregated storage environments or scenarios where shuffle data is replicated for fault tolerance.

## 2. Goals
*   **Redundancy Support:** Allow a single shuffle map output to be registered at multiple `BlockManager` locations.
*   **Load Balancing:** When multiple locations are available for a block, intelligently select the optimal location to minimize executor load skew during shuffle fetch.
*   **Backward Compatibility:** Ensure existing `MapStatus` implementations and serialization protocols remain compatible with single-location assumptions where applicable.

## 3. System Architecture

### 3.1. High-Level Data Flow
In a standard Spark shuffle, a Map task writes data to a local BlockManager and reports this single location to the Driver. In this multi-location design, the flow is enhanced to support replication:

1.  **Replication/Migration:** External systems (like a dedicated shuffle service) or internal processes (like block migration during decommissioning) can copy shuffle blocks to new nodes.
2.  **Reporting:** These new locations are reported to the `MapOutputTrackerMaster` on the Driver via the `AddMapOutputLocation` message.
3.  **Discovery:** When a Reducer requires shuffle data, it queries the Driver. The Driver looks up all available locations for the requested map partitions in `ShuffleStatus`.
4.  **Selection & Balancing:** The Driver applies a load-balancing policy (see 3.3) to select the *single best location* for each block from the available replicas and returns a precise fetch plan to the Reducer.
5.  **Fetching:** The Reducer connects to the selected BlockManager to retrieve the data.

### 3.2. Driver-Side State Management
The `MapOutputTrackerMaster` serves as the authoritative source of truth for shuffle block locations.

*   **ShuffleStatus Component:** This component manages the metadata for a single shuffle stage. It uses granular read/write locks to ensure thread safety during concurrent updates (registering new locations) and reads (reducers requesting locations).
*   **Atomic Updates:** Adding or removing a location is treated as an atomic operation. A map output is only considered "lost" (triggering re-computation) if *all* its registered locations are removed.
*   **Serialization Cache:** To maintain high performance, serialized chunks of map statuses are cached. Any update to the location list (add/remove) invalidates this cache for the affected shuffle ID, ensuring Reducers always receive the most up-to-date topology.

### 3.3. Load Balancing Logic
To prevent hot-spotting when multiple replicas exist, the system employs a "Greedy Least-Loaded" strategy during the map status conversion phase.

*   **Scope:** Load balancing is calculated per *request* (i.e., within the scope of a single `getMapSizesByExecutorId` call).
*   **Mechanism:**
    1.  The Driver creates a transient `executorLoadTracker` (a map of `BlockManagerId` -> `TotalBytes`) for the duration of the request.
    2.  It iterates through every requested map block.
    3.  For blocks with multiple locations, it checks the `executorLoadTracker` to see which candidate `BlockManager` currently has the lowest assigned byte count in this batch.
    4.  It assigns the block to that candidate and immediately updates the tracker with the block's size.
*   **Outcome:** This ensures that within a single reducer's fetch plan, requests are spread as evenly as possible across the available replicas, preventing any single node from being overwhelmed by that reducer.

## 4. Memory Optimization & Data Structures

### 4.1. The Scale Challenge
Spark drivers often manage millions of map output statuses (e.g., 50,000 tasks × 20 stages). Even a small increase in per-object overhead translates to gigabytes of additional heap usage, potentially causing Driver OOMs.
*   **Current Implementation:** Uses `Seq[BlockManagerId]`.
*   **Problem:** Scala `List` or `Vector` implementations incur high object overhead (node wrappers, headers, pointers) which is multiplied by the number of tasks.

### 4.2. Optimization Strategy: Primary + Overflow
To minimize memory footprint while supporting redundancy, the design should adopt a "Primary + Overflow" storage pattern within the `MapStatus` object.

**Concept:**
Since the vast majority of blocks (99%+) will have only one location, the data structure must be optimized for the cardinality of 1, paying *zero* memory penalty for the common case.

**Proposed Structure:**
Instead of a generic `Seq`, `MapStatus` will hold:
1.  `loc: BlockManagerId`: The primary location (existing field). **0 bytes extra overhead.**
2.  `extraLocations: Array[BlockManagerId]`: An array for additional locations, initialized to `null`.

**Memory Comparison (for 100,000 tasks):**

| Scenario | Generic List (Seq) | Array | Primary + Overflow |
| :--- | :--- | :--- | :--- |
| **1 Location (Standard)** | ~100,000 empty list objects | 100,000 empty arrays | **100,000 null references (Best)** |
| **3 Locations (Replica)** | ~300,000 node objects (High GC) | 100,000 array objects | 100,000 small array objects |

**Implementation Details:**
*   **Access:** The `locations` method checks if `extraLocations` is null.
    *   If null: Returns `Seq(loc)` (using a lightweight singleton wrapper).
    *   If not null: Allocates a new array combining `loc` + `extraLocations` and returns it wrapped.
*   **Mutation:**
    *   `addLocation`: If `extraLocations` is null, allocate size-1 array. Else, expand array.
    *   `removeLocation`: If removing primary, swap with first extra. If `extraLocations` becomes empty, set to null.

This approach ensures the feature incurs **zero memory penalty** for standard jobs that do not use replication.

## 5. API Changes

### Core Classes
*   **`org.apache.spark.scheduler.MapStatus`**:
    *   New accessor `def locations: Seq[BlockManagerId]`
    *   New mutator `def addLocation(newLoc: BlockManagerId): Unit`
*   **`org.apache.spark.MapOutputTracker`**:
    *   `convertMapStatuses` modified to implement the load balancing logic described in 3.3.
*   **`org.apache.spark.ShuffleStatus`**:
    *   `addMapOutputLocation(mapId, bmAddress)`: Interface for external components to register replicas.
    *   `removeMapOutputLocation(mapIndex, bmAddress)`: Interface to deregister replicas.

## 6. Review and Improvements

### 6.1. Strengths
*   **Non-Invasive:** The changes mostly extend existing structures rather than rewriting the shuffle protocol.
*   **Safety:** The "Primary + Overflow" optimization ensures no regression in memory usage for standard workloads.
*   **Robustness:** Handling of edge cases (e.g., removing the last location) is implemented correctly to prevent data loss.

### 6.2. Potential Issues & Limitations
1.  **Global Load Awareness:**
    *   *Issue:* The load balancer uses a "local scope" (per reducer request). It does not see concurrent requests from other reducers. Two reducers might simultaneously pick the same "idle" node.
    *   *Mitigation:* While a global view is ideal, it requires complex synchronization. The local greedy approach is a sufficient first step.

2.  **Serialization Cost:**
    *   *Issue:* `BlockManagerId` serialization is repeated for every location.
    *   *Mitigation:* Spark's existing string interning likely handles this, but it should be monitored.

### 6.3. Future Work
1.  **Weighted Load Balancing:** Incorporate executor metrics (CPU/Disk I/O) into the selection logic, rather than just byte counts.
2.  **Replica Limit:** Add a configuration to limit the maximum number of locations tracked per output to prevent metadata explosion in pathological cases (e.g., accidental broadcast of shuffle blocks).
3.  **Observability:** Expose metrics for "multi-location hits" (how often a secondary location is used) and "balanced selection" (variance in bytes assigned) to verify effectiveness in production.