# Shuffle Fetch Wait Time Optimization

## Overview

This document outlines the design and implementation plan for optimizing Spark's shuffle fetch mechanism by enabling server-side wait time estimation and client-side adaptive request scheduling.

### Problem Statement

Currently, Spark's shuffle fetch mechanism uses client-side throttling (`maxBytesInFlight`, `maxReqsInFlight`) to control concurrent requests. However, executors make decisions based solely on local state without visibility into server-side load conditions. This can lead to:

- Suboptimal request distribution (many requests to busy servers, few to idle ones)
- Unnecessary waiting when executors could proactively fetch from other sources
- Inefficient use of network and I/O resources

### Proposed Solution

Enable shuffle servers (executors) to send **immediate wait time notifications** when fetch requests arrive and are queued in the separate thread pool. This allows clients to receive load information **before** the server is busy processing:

**Server-side**:
1. Use separate thread pool to handle fetch requests
2. When request arrives, immediately calculate estimated wait time based on:
   - Current queue depth (number of pending requests in thread pool)
   - Historical request processing time (exponential moving average)
3. Send immediate `WaitTimeNotification` message to client
4. Queue request for processing in thread pool
5. Eventually process request and send data (normal flow)

**Client-side** uses this information to:
1. Receive wait time estimates asynchronously as soon as requests are queued
2. Proactively issue more requests to other executors if current server is busy
3. Reorder pending requests to prioritize less-loaded servers
4. Dynamically adjust parallelism to overlap waiting with additional fetches

---

## Key Innovation: Immediate Wait Time Notification

**Problem with Traditional Approaches:**
- Clients typically learn about server load only AFTER requests complete
- By the time load information arrives, it's too late to act
- Servers that become overloaded continue receiving requests until clients notice

**Our Solution:**
- Server uses **separate thread pool** for request handling
- When request arrives, server **immediately** sends `WaitTimeNotification` message
- Client receives notification **asynchronously** while server processes request
- Client can **proactively** issue more requests to other servers during wait time
- No wasted time - client makes smart decisions in real-time

**Timeline Comparison:**

Traditional (no optimization):
```
Client → Server: Request
[Client waits... server processing... 500ms]
Client ← Server: Response + Data
Client: "That took long, maybe try another server next time"
```

With Immediate Notification:
```
Client → Server: Request
Client ← Server: WaitTimeNotification (estimatedWait: 500ms) ⚡ [sent immediately!]
Client: "500ms wait? Let me issue more requests to other servers now!"
Client → Other Servers: Additional requests [parallel fetching while waiting]
[200ms later, multiple responses arriving...]
Client ← Server: Response + Data [original request completes]
Client ← Other Servers: More data [reduced total wait time]
```

---

## Design Specifications

### Scope
- **Target**: Executor-to-executor shuffle transfers only (not external shuffle service)
- **Strategy**: Combination approach (load-aware ordering + adaptive parallelism)
- **Estimation**: Queue wait time + average processing time
- **Compatibility**: Requires updated components (no backwards compatibility requirement)

### Architecture Components

```
┌─────────────┐                              ┌──────────────────────┐
│  Executor   │                              │  Executor (Server)   │
│  (Client)   │                              ├──────────────────────┤
├─────────────┤                              │                      │
│             │  1. FetchBlocks Request      │  ┌────────────────┐  │
│  Shuffle    │ ──────────────────────────>  │  │ Request Thread │  │
│  Block      │                              │  │     (Fast)     │  │
│  Fetcher    │  2. WaitTimeNotification ⚡   │  └────────────────┘  │
│  Iterator   │ <──────────────────────────  │         │            │
│             │     estimatedWaitTimeMs=200  │         │ Queue      │
│  - Track    │     (sent immediately!)      │         ▼            │
│    wait     │                              │  ┌────────────────┐  │
│    times    │                              │  │  Thread Pool   │  │
│  - Reorder  │  [Client can now issue more  │  │  (Processing)  │  │
│    requests │   requests to other servers] │  └────────────────┘  │
│  - Adjust   │                              │         │            │
│    parallel │  3. StreamHandle Response    │         │            │
│             │ <──────────────────────────  │  ┌──────▼─────────┐  │
│             │     (after processing)       │  │ Wait Estimator │  │
│             │                              │  │ - Queue depth  │  │
│             │  4. Chunk Requests           │  │ - Avg latency  │  │
│             │ ──────────────────────────>  │  └────────────────┘  │
│             │                              │                      │
│             │  5. Chunk Data               │                      │
│             │ <──────────────────────────  │                      │
└─────────────┘                              └──────────────────────┘

Key Innovation: Separate thread pool allows immediate wait time notification
before request processing begins, enabling proactive client behavior.
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

### Phase 2: Protocol Extension - Immediate Wait Time Notification

**Objective**: Add new protocol message for immediate wait time notification sent before request processing.

**Files to Modify**:
- `common/network-shuffle/src/main/java/org/apache/spark/network/shuffle/protocol/WaitTimeNotification.java` (NEW)
- `common/network-shuffle/src/main/java/org/apache/spark/network/shuffle/ExternalBlockHandler.java`
- `common/network-shuffle/src/main/java/org/apache/spark/network/shuffle/OneForOneBlockFetcher.java`
- `common/network-common/src/main/java/org/apache/spark/network/client/TransportResponseHandler.java`
- `core/src/main/scala/org/apache/spark/storage/BlockManager.scala`

**Implementation Details**:

1. **Create New WaitTimeNotification Message Class**:
```java
public class WaitTimeNotification extends BlockTransferMessage {
  public final long requestId;  // To correlate with original request
  public final long estimatedWaitTimeMs;
  public final int queueDepth;  // Optional: expose queue depth for debugging

  public WaitTimeNotification(
      long requestId,
      long estimatedWaitTimeMs,
      int queueDepth) {
    this.requestId = requestId;
    this.estimatedWaitTimeMs = estimatedWaitTimeMs;
    this.queueDepth = queueDepth;
  }

  @Override
  public Type type() { return Type.WAIT_TIME_NOTIFICATION; }

  // Encode/decode methods for serialization
}
```

2. **Update BlockTransferMessage.Type Enum**:
```java
public enum Type implements Encodable {
  CHUNK_FETCH_REQUEST(0),
  CHUNK_FETCH_SUCCESS(1),
  CHUNK_FETCH_FAILURE(2),
  // ... existing types
  WAIT_TIME_NOTIFICATION(12);  // NEW
}
```

3. **Send Notification Immediately in ExternalBlockHandler**:
```java
// In handleMessage() for OpenBlocks/FetchShuffleBlocks
@Override
public void receive(TransportClient client, ByteBuffer message) {
  // Parse request
  OpenBlocks openBlocks = (OpenBlocks) BlockTransferMessage.Decoder.fromByteBuffer(message);

  // Calculate wait time IMMEDIATELY
  long estimatedWaitTime = waitTimeEstimator.estimateWaitTime(openBlocks.execId);
  int queueDepth = waitTimeEstimator.getQueueDepth(openBlocks.execId);

  // Send notification immediately (non-blocking)
  WaitTimeNotification notification = new WaitTimeNotification(
    client.getChannelId(),  // Use as request ID
    estimatedWaitTime,
    queueDepth
  );
  client.send(notification.toByteBuffer());

  // Now queue request for processing in thread pool (existing logic)
  ManagedBuffer[] buffers = streamManager.openBlocks(openBlocks);
  // ... rest of existing logic
}
```

4. **Handle Notification on Client Side**:
```java
// In OneForOneBlockFetcher or new handler
public void handle(WaitTimeNotification notification) {
  // Propagate to listener
  if (listener instanceof WaitTimeAwareListener) {
    ((WaitTimeAwareListener) listener).onWaitTimeReceived(
      notification.estimatedWaitTimeMs,
      notification.queueDepth
    );
  }
}
```

5. **Update Client Listener Interface**:
```java
// Extend BlockFetchingListener
public interface WaitTimeAwareBlockFetchingListener extends BlockFetchingListener {
  void onWaitTimeReceived(long estimatedWaitTimeMs, int queueDepth);
}
```

**Key Advantage**: Client receives wait time information **immediately** when request arrives at server, not after processing completes. This enables proactive behavior while server is still processing the request.

**Testing**:
- Unit tests for `WaitTimeNotification` serialization/deserialization
- Test immediate sending (before StreamHandle response)
- Test client receives notification asynchronously
- Integration tests for end-to-end protocol flow with timing

**Commit**: "Add immediate WaitTimeNotification protocol message"

---

### Phase 3: Client-Side Adaptive Fetching

**Objective**: Enable executors to use wait time information for smart request scheduling.

**Files to Modify**:
- `core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala`
- `core/src/main/scala/org/apache/spark/executor/ShuffleReadMetrics.scala`
- `core/src/main/scala/org/apache/spark/internal/config/package.scala`

#### Phase 3a: Request Metadata and Asynchronous Wait Time Handling

**Implementation**:

1. **Add Wait Time Tracking to FetchRequest**:
```scala
case class FetchRequest(
  address: BlockManagerId,
  blocks: Seq[(BlockId, Long, Int)],
  var estimatedWaitTimeMs: Long = 0L, // NEW FIELD
  var queueDepth: Int = 0              // NEW FIELD (from server)
)
```

2. **Implement WaitTimeAware BlockFetchingListener**:
```scala
// In sendRequest() method, create listener that handles immediate notifications
new WaitTimeAwareBlockFetchingListener {
  override def onWaitTimeReceived(waitTimeMs: Long, queueDepth: Int): Unit = {
    // Store wait time in request metadata IMMEDIATELY
    req.estimatedWaitTimeMs = waitTimeMs
    req.queueDepth = queueDepth

    // Update historical data
    updateWaitTime(req.address, waitTimeMs)

    // CRITICAL: If server is very busy, proactively issue more requests
    if (waitTimeMs > waitTimeThresholdMs) {
      logInfo(s"Server ${req.address} busy (wait: ${waitTimeMs}ms, " +
              s"queue: $queueDepth), triggering adaptive fetch")
      triggerAdaptiveFetch()  // Issue more requests to other servers
    }
  }

  override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
    // Existing logic - record actual fetch time
    val actualTime = System.nanoTime() - requestStartTime
    // ... existing logic
  }

  override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
    // Mark server as slow/unavailable
    updateWaitTime(req.address, Long.MaxValue)
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

  // Immediately trigger reordering if new data suggests server is slow
  if (adaptiveSchedulingEnabled && waitTimeMs > waitTimeThresholdMs) {
    reorderPendingRequests()
  }
}

def getAvgWaitTime(address: BlockManagerId): Long = {
  waitTimeHistory.get(address).map { h =>
    if (h.nonEmpty) h.sum / h.size else 0L
  }.getOrElse(0L)
}
```

4. **Add Proactive Request Triggering**:
```scala
// NEW: Trigger more requests when server busy notification received
def triggerAdaptiveFetch(): Unit = {
  if (!adaptiveSchedulingEnabled) return

  // Try to send more requests from pending queue to other servers
  // This happens WHILE waiting for the busy server to process
  fetchUpToMaxBytes()

  // Optionally: Temporarily increase parallelism limit
  if (effectiveMaxReqsInFlight < maxReqsInFlight * 2) {
    effectiveMaxReqsInFlight = Math.min(
      effectiveMaxReqsInFlight + 1,
      maxReqsInFlight * 2
    )
  }
}
```

**Key Innovation**: Wait time notifications are received and acted upon **asynchronously** while the original request is still being processed. This enables immediate adaptive behavior without waiting for request completion.

**Commit**: "Add asynchronous wait time notification handling"

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

**Commit**: "Implement dynamic load-aware shuffle request ordering"

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
| `shuffle.server.waitTimeNotificationsSent` | Counter | Total number of wait time notifications sent |
| `shuffle.server.estimatedWaitTimeMs` | Histogram | Distribution of estimated wait times sent in notifications |
| `shuffle.server.queueDepthPerExecutor` | Gauge | Current queue depth per requesting executor |
| `shuffle.server.avgProcessingTimeMs` | Gauge | Exponential moving average of request processing time |
| `shuffle.server.notificationLatencyUs` | Histogram | Time to send notification after request received |

### Client-Side Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `shuffle.client.waitTimeNotificationsReceived` | Counter | Total number of wait time notifications received |
| `shuffle.client.serverEstimatedWaitTime` | Histogram | Distribution of wait times received from servers |
| `shuffle.client.adaptiveFetchTriggered` | Counter | Number of times adaptive fetch was triggered by busy notification |
| `shuffle.client.parallelismAdjustments` | Counter | Number of times parallelism was adjusted dynamically |
| `shuffle.client.avgServerWaitTime` | Gauge | Average wait time across all servers (from notifications) |
| `shuffle.client.effectiveMaxReqsInFlight` | Gauge | Current effective max requests in flight |
| `shuffle.client.requestReorderings` | Counter | Number of times pending requests were reordered based on wait times |

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
   - Test `WaitTimeNotification` message serialization/deserialization
   - Test immediate notification sent before `StreamHandle`
   - Test end-to-end propagation to client
   - Test protocol versioning
   - **Timing test**: Verify notification arrives before data processing completes

2. **Adaptive Behavior**:
   - Mock multiple servers with different latencies and queue depths
   - Verify client receives notifications asynchronously
   - Verify `triggerAdaptiveFetch()` called when notification shows high wait time
   - Verify requests prioritize fast servers based on notification data
   - Verify parallelism increases dynamically when busy notifications received
   - **Key test**: Verify client issues additional requests to other servers WHILE waiting for busy server

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
