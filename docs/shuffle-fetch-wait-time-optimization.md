# Shuffle Fetch Wait Time Optimization

## Overview

This document outlines the design and implementation plan for optimizing Spark's shuffle fetch mechanism by enabling server-side wait time estimation and client-side adaptive request scheduling.

### Problem Statement

Currently, Spark's shuffle fetch mechanism uses client-side throttling (`maxBytesInFlight`, `maxReqsInFlight`) to control concurrent requests. However, executors make decisions based solely on local state without visibility into server-side load conditions. This can lead to:

- Suboptimal request distribution (many requests to busy servers, few to idle ones)
- Unnecessary waiting when executors could proactively fetch from other sources
- Inefficient use of network and I/O resources

### Proposed Solution

Enable shuffle servers (executors) to respond to fetch requests with estimated wait time based on:
1. Current queue depth (number of pending requests)
2. Historical request processing time (exponential moving average)

Executors use this information to:
1. Reorder requests to prioritize less-loaded servers
2. Dynamically adjust parallelism to overlap waiting with additional fetches
3. Make smarter throttling decisions

---

## Design Specifications

### Scope
- **Target**: Executor-to-executor shuffle transfers only (not external shuffle service)
- **Strategy**: Combination approach (load-aware ordering + adaptive parallelism)
- **Estimation**: Queue wait time + average processing time
- **Compatibility**: Requires updated components (no backwards compatibility requirement)

### Architecture Components

```
┌─────────────┐                              ┌─────────────┐
│  Executor   │                              │  Executor   │
│  (Client)   │                              │  (Server)   │
├─────────────┤                              ├─────────────┤
│             │  1. FetchBlocks Request      │             │
│  Shuffle    │ ──────────────────────────>  │  NettyBlock │
│  Block      │                              │  Transfer   │
│  Fetcher    │  2. StreamHandle Response    │  Service    │
│  Iterator   │ <──────────────────────────  │             │
│             │     + estimatedWaitTimeMs    │  Wait Time  │
│             │                              │  Estimator  │
│  - Track    │  3. Chunk Requests           │             │
│    wait     │ ──────────────────────────>  │  - Track    │
│    times    │                              │    queue    │
│  - Reorder  │  4. Chunk Data               │    depth    │
│    requests │ <──────────────────────────  │  - Track    │
│  - Adjust   │                              │    avg      │
│    parallel │                              │    latency  │
└─────────────┘                              └─────────────┘
```

---

## Implementation Plan

### Phase 1: Server-Side Wait Time Tracking

**Objective**: Implement wait time estimation on the server side.

**Files to Modify**:
- `common/network-common/src/main/java/org/apache/spark/network/shuffle/ExternalBlockHandler.java`
- `core/src/main/scala/org/apache/spark/network/NettyBlockTransferService.scala`

**Implementation Details**:

1. **Add Wait Time Estimator Class** (`WaitTimeEstimator.java`):
```java
class WaitTimeEstimator {
  // Exponential moving average for request processing time
  private final AtomicLong avgProcessingTimeNs;

  // Track active requests per executor
  private final ConcurrentHashMap<String, AtomicInteger> activeRequestsPerExecutor;

  // Configuration
  private final double alpha = 0.3; // EMA smoothing factor

  long estimateWaitTime(String executorId) {
    int queueDepth = activeRequestsPerExecutor.getOrDefault(executorId, 0).get();
    long avgTimeNs = avgProcessingTimeNs.get();
    return (queueDepth * avgTimeNs) / 1_000_000; // Convert to milliseconds
  }

  void recordRequestStart(String executorId);
  void recordRequestComplete(String executorId, long durationNs);
}
```

2. **Integrate into ExternalBlockHandler**:
   - Add `WaitTimeEstimator` instance
   - Call `recordRequestStart()` when handling `OpenBlocks` message
   - Call `recordRequestComplete()` when stream is fully consumed
   - Calculate wait time estimate before sending response

3. **Add Server Metrics**:
   - `shuffleServerEstimatedWaitTimeMs` - Histogram of estimated wait times
   - `shuffleServerActiveRequestsPerExecutor` - Gauge of queue depth
   - `shuffleServerAvgProcessingTimeMs` - Gauge of average processing time

**Testing**:
- Unit tests for `WaitTimeEstimator` calculation logic
- Test EMA updates with various request patterns
- Test queue depth tracking with concurrent requests

**Commit**: "Add server-side wait time estimation for shuffle requests"

---

### Phase 2: Protocol Extension

**Objective**: Extend the shuffle fetch protocol to include wait time estimates.

**Files to Modify**:
- `common/network-shuffle/src/main/java/org/apache/spark/network/shuffle/protocol/StreamHandle.java`
- `common/network-shuffle/src/main/java/org/apache/spark/network/shuffle/ExternalBlockHandler.java`
- `common/network-shuffle/src/main/java/org/apache/spark/network/shuffle/OneForOneBlockFetcher.java`
- `core/src/main/scala/org/apache/spark/storage/BlockManager.scala`

**Implementation Details**:

1. **Extend StreamHandle Class**:
```java
public class StreamHandle extends BlockTransferMessage {
  public final long streamId;
  public final int numChunks;
  public final long estimatedWaitTimeMs; // NEW FIELD

  public StreamHandle(long streamId, int numChunks, long estimatedWaitTimeMs) {
    this.streamId = streamId;
    this.numChunks = numChunks;
    this.estimatedWaitTimeMs = estimatedWaitTimeMs;
  }

  // Update encode/decode methods
}
```

2. **Populate in ExternalBlockHandler.handleMessage()**:
```java
// Line ~178
long estimatedWaitTime = waitTimeEstimator.estimateWaitTime(message.execId);
StreamHandle streamHandle = new StreamHandle(
  streamId,
  buf.size(),
  estimatedWaitTime
);
```

3. **Parse in OneForOneBlockFetcher**:
   - Extract `estimatedWaitTimeMs` from `StreamHandle` response
   - Pass to `BlockFetchingListener` callback
   - Propagate to `ShuffleBlockFetcherIterator`

4. **Update Protocol Version**:
   - Increment protocol version constant
   - Document protocol changes

**Testing**:
- Unit tests for `StreamHandle` serialization/deserialization
- Integration tests for end-to-end protocol flow
- Test with various wait time values (0, small, large)

**Commit**: "Extend StreamHandle protocol with estimated wait time"

---

### Phase 3: Client-Side Adaptive Fetching

**Objective**: Enable executors to use wait time information for smart request scheduling.

**Files to Modify**:
- `core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala`
- `core/src/main/scala/org/apache/spark/executor/ShuffleReadMetrics.scala`
- `core/src/main/scala/org/apache/spark/internal/config/package.scala`

#### Phase 3a: Request Metadata Enhancement

**Implementation**:

1. **Add Wait Time Tracking to FetchRequest**:
```scala
case class FetchRequest(
  address: BlockManagerId,
  blocks: Seq[(BlockId, Long, Int)],
  var estimatedWaitTimeMs: Long = 0L // NEW FIELD
)
```

2. **Update BlockFetchingListener**:
```scala
// In sendRequest() method, capture wait time from response
new BlockFetchingListener {
  override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
    // Store wait time in request metadata
    req.estimatedWaitTimeMs = receivedWaitTime
    // ... existing logic
  }
}
```

3. **Track Per-Address Wait Time History**:
```scala
private val waitTimeHistory = new mutable.HashMap[BlockManagerId, Queue[Long]]()

def updateWaitTime(address: BlockManagerId, waitTimeMs: Long): Unit = {
  val history = waitTimeHistory.getOrElseUpdate(address, new Queue[Long]())
  history.enqueue(waitTimeMs)
  if (history.size > 10) history.dequeue() // Keep last 10 samples
}

def getAvgWaitTime(address: BlockManagerId): Long = {
  waitTimeHistory.get(address).map { h =>
    if (h.nonEmpty) h.sum / h.size else 0L
  }.getOrElse(0L)
}
```

**Commit**: "Track server wait times in shuffle fetch requests"

#### Phase 3b: Load-Aware Request Ordering

**Implementation**:

1. **Sort Fetch Queue by Estimated Wait Time**:
```scala
// In fetchUpToMaxBytes(), before processing fetchRequests
if (adaptiveSchedulingEnabled) {
  // Sort by estimated wait time (ascending) - prioritize fast servers
  fetchRequests.sortBy(req => getAvgWaitTime(req.address))
}
```

2. **Implement Request Scoring**:
```scala
def requestScore(req: FetchRequest): Double = {
  val waitTime = getAvgWaitTime(req.address)
  val requestSize = req.size

  // Lower score = higher priority
  // Favor low-latency servers and larger requests
  waitTime.toDouble / Math.max(requestSize, 1L)
}
```

**Commit**: "Implement load-aware shuffle request ordering"

#### Phase 3c: Dynamic Parallelism Adjustment

**Implementation**:

1. **Add Configuration Parameters**:
```scala
// In config/package.scala
val ADAPTIVE_SHUFFLE_FETCH_ENABLED =
  ConfigBuilder("spark.reducer.adaptiveMaxReqsInFlight.enabled")
    .doc("Enable adaptive adjustment of max concurrent shuffle fetch requests")
    .booleanConf
    .createWithDefault(false)

val SHUFFLE_WAIT_TIME_THRESHOLD_MS =
  ConfigBuilder("spark.reducer.waitTime.threshold.ms")
    .doc("Wait time threshold for adjusting parallelism (milliseconds)")
    .longConf
    .createWithDefault(100L)

val SHUFFLE_PARALLELISM_ADJUSTMENT_FACTOR =
  ConfigBuilder("spark.reducer.parallelism.adjustment.factor")
    .doc("Factor to multiply maxReqsInFlight when adjusting parallelism")
    .doubleConf
    .createWithDefault(1.5)
```

2. **Implement Dynamic Adjustment**:
```scala
// Track current effective max requests
private var effectiveMaxReqsInFlight = maxReqsInFlight

def adjustParallelism(): Unit = {
  if (!adaptiveSchedulingEnabled) return

  val avgWaitTime = waitTimeHistory.values.flatMap(_.lastOption).sum /
                    Math.max(waitTimeHistory.size, 1)

  if (avgWaitTime > waitTimeThresholdMs) {
    // Servers are busy - increase parallelism to overlap waiting
    effectiveMaxReqsInFlight = Math.min(
      (maxReqsInFlight * parallelismAdjustmentFactor).toInt,
      maxReqsInFlight * 2 // Cap at 2x original
    )
  } else if (avgWaitTime < waitTimeThresholdMs / 2) {
    // Servers are fast - reduce parallelism to original
    effectiveMaxReqsInFlight = maxReqsInFlight
  }

  logDebug(s"Adjusted shuffle fetch parallelism to $effectiveMaxReqsInFlight " +
           s"(avg wait time: ${avgWaitTime}ms)")
}
```

3. **Call Adjustment in Fetch Loop**:
```scala
// In fetchUpToMaxBytes()
if (deferredFetchRequests.isEmpty && fetchRequests.isEmpty) {
  adjustParallelism() // Adjust before next batch
}
```

**Commit**: "Add adaptive parallelism based on server wait times"

#### Phase 3d: Metrics and Testing

**Implementation**:

1. **Add Client Metrics**:
```scala
// In ShuffleReadMetrics.scala
private[spark] def _serverEstimatedWaitTime = new LongAccumulator
def serverEstimatedWaitTime: Long = _serverEstimatedWaitTime.sum

private[spark] def _parallelismAdjustments = new LongAccumulator
def parallelismAdjustments: Long = _parallelismAdjustments.sum

private[spark] def _avgServerWaitTime = new DoubleAccumulator
def avgServerWaitTime: Double = _avgServerWaitTime.avg
```

2. **Unit Tests**:
   - Test wait time tracking and history management
   - Test request ordering by wait time
   - Test parallelism adjustment logic
   - Test score calculation for request prioritization

3. **Integration Tests**:
   - Mock shuffle fetch with varying server wait times
   - Verify requests are reordered correctly
   - Verify parallelism increases when servers are busy
   - Verify metrics are recorded correctly

**Commit**: "Add metrics and tests for adaptive shuffle fetch"

---

### Phase 4: Validation and Documentation

**Objective**: Ensure quality and document the feature.

**Tasks**:

1. **Run Full Test Suite**:
```bash
./build/sbt "core/test"
./build/sbt "network-shuffle/test"
```

2. **Scalastyle Checks**:
```bash
./build/sbt "core/scalastyle"
./build/sbt "network-shuffle/scalastyle"
```

3. **Performance Testing**:
   - Create benchmark with shuffle-heavy workload (e.g., large aggregation)
   - Measure with and without adaptive fetch enabled
   - Key metrics:
     - Total shuffle fetch wait time
     - Request distribution across executors
     - Network throughput
     - Task completion time

4. **Documentation**:
   - Update `docs/configuration.md` with new configuration parameters
   - Add design doc explaining the optimization
   - Update shuffle fetch section in tuning guide

**Commit**: "Add documentation for adaptive shuffle fetch optimization"

---

## Configuration Parameters

### New Configuration Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `spark.reducer.adaptiveMaxReqsInFlight.enabled` | `false` | Enable adaptive adjustment of max concurrent shuffle fetch requests based on server wait times |
| `spark.reducer.waitTime.threshold.ms` | `100` | Wait time threshold (ms) for triggering parallelism adjustment |
| `spark.reducer.parallelism.adjustment.factor` | `1.5` | Factor to multiply maxReqsInFlight when servers are busy |
| `spark.shuffle.waitTime.history.size` | `10` | Number of historical wait time samples to track per server |

### Interaction with Existing Configuration

The adaptive fetch optimization works alongside existing shuffle configuration:

- **`spark.reducer.maxSizeInFlight`** (default: 48MB): Still enforced as upper bound on bytes in flight
- **`spark.reducer.maxReqsInFlight`** (default: Int.MaxValue): Becomes the baseline for adaptive adjustment
- **`spark.reducer.maxBlocksInFlightPerAddress`** (default: Int.MaxValue): Still enforced per-address

When adaptive mode is enabled, `effectiveMaxReqsInFlight` may temporarily exceed `maxReqsInFlight` up to 2x to overlap waiting.

---

## Metrics

### Server-Side Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `shuffle.server.estimatedWaitTimeMs` | Histogram | Distribution of estimated wait times sent to clients |
| `shuffle.server.activeRequestsPerExecutor` | Gauge | Current queue depth per executor |
| `shuffle.server.avgProcessingTimeMs` | Gauge | Exponential moving average of request processing time |

### Client-Side Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `shuffle.client.serverEstimatedWaitTime` | Counter | Sum of all estimated wait times received |
| `shuffle.client.parallelismAdjustments` | Counter | Number of times parallelism was adjusted |
| `shuffle.client.avgServerWaitTime` | Gauge | Average wait time across all servers |
| `shuffle.client.effectiveMaxReqsInFlight` | Gauge | Current effective max requests in flight |

---

## Testing Strategy

### Unit Tests

1. **WaitTimeEstimator**:
   - Test EMA calculation with varying request durations
   - Test queue depth tracking with concurrent requests
   - Test wait time estimation formula

2. **Request Ordering**:
   - Test sorting by wait time
   - Test request scoring
   - Test priority queue behavior

3. **Parallelism Adjustment**:
   - Test adjustment triggers (high/low wait time)
   - Test capping at 2x baseline
   - Test disabling adaptive mode

### Integration Tests

1. **Protocol Flow**:
   - Test `StreamHandle` with wait time field
   - Test end-to-end propagation to client
   - Test protocol versioning

2. **Adaptive Behavior**:
   - Mock multiple servers with different latencies
   - Verify requests prioritize fast servers
   - Verify parallelism increases with high wait times

### Performance Tests

1. **Benchmark Setup**:
   - Large shuffle (e.g., 10GB across 100 partitions)
   - Multiple executors with varying CPU/IO load
   - Measure with adaptive fetch on/off

2. **Metrics to Compare**:
   - Total shuffle read time
   - Shuffle fetch wait time
   - Network utilization
   - Request distribution fairness (coefficient of variation)

---

## Success Criteria

✅ **Correctness**:
- All unit tests pass
- All integration tests pass
- No scalastyle violations
- No regressions in existing shuffle tests

✅ **Performance**:
- Reduced shuffle fetch wait time in benchmarks (target: 10-20% improvement)
- More even request distribution across executors
- No negative impact on small shuffles

✅ **Code Quality**:
- Follows Spark coding conventions
- Clear documentation and comments
- Backward compatible protocol (fail gracefully with old versions)

✅ **Observability**:
- Metrics exposed via Spark UI
- Debug logging for troubleshooting
- Clear configuration documentation

---

## Future Enhancements

1. **External Shuffle Service Support**: Extend to external shuffle service (more complex due to separate process)

2. **Machine Learning-Based Estimation**: Use ML model to predict wait time based on block size, server load, time of day, etc.

3. **Global Shuffle Coordination**: Share wait time information across all executors for cluster-wide optimization

4. **Adaptive Timeout**: Adjust request timeout based on estimated wait time

5. **Push-Based Shuffle Integration**: Combine with push-based shuffle for hybrid approach

---

## References

**Related JIRAs**:
- [SPARK-25250](https://issues.apache.org/jira/browse/SPARK-25250) - Push-based shuffle
- [SPARK-30602](https://issues.apache.org/jira/browse/SPARK-30602) - Adaptive shuffle fetch

**Key Files**:
- `core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala` - Client fetch logic
- `common/network-shuffle/src/main/java/org/apache/spark/network/shuffle/ExternalBlockHandler.java` - Server handler
- `common/network-shuffle/src/main/java/org/apache/spark/network/shuffle/protocol/StreamHandle.java` - Protocol message

**Related Papers**:
- Riffle: Optimized Shuffle Service for Large-Scale Data Analytics
- Sailfish: A Framework for Large Scale Data Processing
