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

## Leveraging Existing Spark Infrastructure

### Prerequisites

This optimization builds on **existing** Spark shuffle infrastructure:

**1. Separate Thread Pool for Chunk Fetch** (already in production):
- **Config**: `spark.shuffle.server.chunkFetchHandlerThreadsPercent` (default: 0, disabled)
- **Location**: `TransportContext.java:143-146`
- **Thread pool**: `chunkFetchWorkers` EventLoopGroup
- **Handler**: `ChunkFetchRequestHandler` processes requests in dedicated threads
- **Requirement**: Must set `chunkFetchHandlerThreadsPercent > 0` to enable our optimization

**2. Existing Metrics Infrastructure**:
- **Location**: `ExternalBlockHandler.ShuffleMetrics` (line 315-373)
- **Metrics library**: Codahale Metrics (Dropwizard)
- **New Timer**: `diskIOLatencyMillis` tracks ONLY disk I/O time (streamManager.getChunk())
- **Why new metric**: Existing `openBlockRequestLatencyMillis` includes RPC parsing, authorization, and serialization overhead which inflates wait time estimates. We need isolated disk I/O timing for accurate estimation.

### Getting Real Queue Depth from Netty

**Netty EventLoop API**: Each EventLoop in the `chunkFetchWorkers` provides `pendingTasks()` method

**Important Constraints**:
- ⚠️ **Calling from outside event loop is expensive** (may block)
- ✅ **Calling from within event loop is fast** (just reads a counter)
- ✅ **Solution**: Schedule periodic monitoring task within each event loop

**Implementation Strategy**:
```java
// For each EventLoop in chunkFetchWorkers EventLoopGroup
eventLoop.scheduleAtFixedRate(() -> {
    int pending = eventLoop.pendingTasks();
    // Update shared counter
    totalPendingTasksCounter.addAndGet(pending);
}, 0, 100, TimeUnit.MILLISECONDS); // Check every 100ms
```

**Disk I/O Time from New Timer**:
```java
// New Timer tracks ONLY disk I/O latency (streamManager.getChunk)
Snapshot snapshot = diskIOLatencyMillis.getSnapshot();
double meanDiskIOMs = snapshot.getMean();           // Average disk I/O time
double p99DiskIOMs = snapshot.get99thPercentile();  // 99th percentile (conservative)
```

---

## Design Specifications

### Scope
- **Target**: Executor-to-executor shuffle transfers only (not external shuffle service)
- **Strategy**: Combination approach (load-aware ordering + adaptive parallelism)
- **Estimation**: Real Netty EventLoop queue depth + processing time from Timer metrics
- **Compatibility**: Requires updated components (no backwards compatibility requirement)
- **Prerequisite**: Must enable `spark.shuffle.server.chunkFetchHandlerThreadsPercent > 0`

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

### Phase 1: Server-Side Wait Time Tracking with Netty Metrics

**Objective**: Implement real-time wait time estimation using Netty EventLoop queue depth and existing Timer metrics.

**Files to Modify**:
- `common/network-common/src/main/java/org/apache/spark/network/shuffle/NettyBasedWaitTimeEstimator.java` (NEW)
- `common/network-common/src/main/java/org/apache/spark/network/TransportContext.java`
- `common/network-shuffle/src/main/java/org/apache/spark/network/shuffle/ExternalBlockHandler.java`
- `common/network-common/src/main/java/org/apache/spark/network/server/ChunkFetchRequestHandler.java`

**Implementation Details**:

1. **Add Netty-Based Wait Time Estimator Class** (`NettyBasedWaitTimeEstimator.java`):
```java
public class NettyBasedWaitTimeEstimator {
  private final EventLoopGroup chunkFetchWorkers;
  private final Timer diskIOLatencyTimer;  // Changed: Use disk I/O specific timer
  private final AtomicInteger totalPendingTasks = new AtomicInteger(0);
  private final boolean useP99;
  private final long updateIntervalMs;

  public NettyBasedWaitTimeEstimator(
      EventLoopGroup chunkFetchWorkers,
      Timer diskIOLatencyTimer,  // Changed: Renamed parameter
      boolean useP99,
      long updateIntervalMs) {
    this.chunkFetchWorkers = chunkFetchWorkers;
    this.diskIOLatencyTimer = diskIOLatencyTimer;
    this.useP99 = useP99;
    this.updateIntervalMs = updateIntervalMs;
  }

  /**
   * Initialize periodic queue depth monitoring.
   * Must be called after EventLoopGroup is created.
   */
  public void initialize() {
    // Schedule monitoring task on each EventLoop in the group
    chunkFetchWorkers.forEach(eventLoop -> {
      eventLoop.scheduleAtFixedRate(() -> {
        try {
          // pendingTasks() is cheap when called from within the event loop
          int pending = eventLoop.pendingTasks();
          totalPendingTasks.set(pending);
        } catch (Exception e) {
          // Log and continue
        }
      }, 0, updateIntervalMs, TimeUnit.MILLISECONDS);
    });
  }

  /**
   * Estimate wait time based on real Netty queue depth and disk I/O time.
   * @return Estimated wait time in milliseconds
   */
  public long estimateWaitTimeMs() {
    // Get current queue depth from Netty EventLoop
    int queueDepth = totalPendingTasks.get();

    if (queueDepth == 0) {
      return 0L; // No wait if queue is empty
    }

    // Get disk I/O time from new Timer metric (only tracks streamManager.getChunk)
    Snapshot snapshot = diskIOLatencyTimer.getSnapshot();
    double diskIOTimeMs = useP99
      ? snapshot.get99thPercentile()  // Conservative estimate
      : snapshot.getMean();            // Average estimate

    // Handle case where no requests processed yet
    if (Double.isNaN(diskIOTimeMs) || diskIOTimeMs <= 0) {
      diskIOTimeMs = 10.0; // Default 10ms assumption
    }

    // Estimate: queue_depth * avg_disk_io_time
    return (long) (queueDepth * diskIOTimeMs);
  }

  /**
   * Get current queue depth.
   * @return Number of pending tasks in Netty EventLoop
   */
  public int getQueueDepth() {
    return totalPendingTasks.get();
  }

  /**
   * Get average disk I/O time for debugging/monitoring.
   * @return Average disk I/O time in milliseconds
   */
  public double getAvgDiskIOTimeMs() {
    Snapshot snapshot = diskIOLatencyTimer.getSnapshot();
    return snapshot.getMean();
  }
}
```

2. **Instrument processFetchRequest to Track Disk I/O Time**:
```java
// In ChunkFetchRequestHandler.processFetchRequest()
public void processFetchRequest(final Channel channel, final ChunkFetchRequest msg) {
  if (logger.isTraceEnabled()) {
    logger.trace("Received req from {} to fetch block {}", getRemoteAddress(channel),
      msg.streamChunkId);
  }

  if (chunksBeingTransferred >= maxChunksBeingTransferred) {
    logger.warn("The number of chunks being transferred {} is above {}, close the connection.",
      chunksBeingTransferred, maxChunksBeingTransferred);
    channel.close();
    return;
  }

  ManagedBuffer buf;
  try {
    // Authorization check (NOT included in disk I/O time)
    streamManager.checkAuthorization(client, msg.streamChunkId.streamId());

    // NEW: Time ONLY the disk I/O operation
    long diskIOStartNanos = System.nanoTime();
    buf = streamManager.getChunk(msg.streamChunkId.streamId(), msg.streamChunkId.chunkIndex());
    long diskIODurationNanos = System.nanoTime() - diskIOStartNanos;

    // Update new disk I/O specific metric
    if (shuffleMetrics != null) {
      shuffleMetrics.diskIOLatencyMillis.update(diskIODurationNanos, TimeUnit.NANOSECONDS);
    }

    if (buf == null) {
      throw new IllegalStateException("Chunk was not found");
    }
  } catch (Exception e) {
    logger.error(String.format("Error opening block %s for request from %s",
      msg.streamChunkId, getRemoteAddress(channel)), e);
    respond(channel, new ChunkFetchFailure(msg.streamChunkId,
      Throwables.getStackTraceAsString(e)));
    return;
  }

  // Track chunk being transferred
  chunksBeingTransferred++;
  // ... rest of existing logic
}
```

**Why This Approach**:
- ✅ Measures ONLY `streamManager.getChunk()` - the actual disk read operation
- ✅ Excludes authorization overhead (checkAuthorization)
- ✅ Excludes serialization and network overhead
- ✅ Provides accurate estimate of disk I/O wait time for queued requests

3. **Add Disk I/O Metric to ShuffleMetrics**:
```java
// In ExternalBlockHandler.ShuffleMetrics
public class ShuffleMetrics implements MetricSet {
  // Existing metric
  private final Timer openBlockRequestLatencyMillis =
    new TimerWithCustomTimeUnit(TimeUnit.MILLISECONDS);

  // NEW: Disk I/O specific metric
  private final Timer diskIOLatencyMillis =
    new TimerWithCustomTimeUnit(TimeUnit.MILLISECONDS);

  @Override
  public Map<String, Metric> getMetrics() {
    Map<String, Metric> allMetrics = new HashMap<>();
    allMetrics.put("openBlockRequestLatencyMillis", openBlockRequestLatencyMillis);
    allMetrics.put("diskIOLatencyMillis", diskIOLatencyMillis);  // NEW
    // ... other metrics
    return allMetrics;
  }

  // Getter for wait time estimator
  public Timer getDiskIOLatencyMillis() {
    return diskIOLatencyMillis;
  }
}
```

4. **Initialize in TransportContext**:
```java
// In TransportContext constructor, after creating chunkFetchWorkers
if (chunkFetchWorkers != null) {
  this.waitTimeEstimator = new NettyBasedWaitTimeEstimator(
    chunkFetchWorkers,
    externalBlockHandler.getMetrics().getDiskIOLatencyMillis(),  // Changed: Use disk I/O timer
    conf.waitTimeEstimationUseP99(),
    conf.waitTimeEstimationUpdateIntervalMs()
  );
  this.waitTimeEstimator.initialize();
}
```

5. **Integrate into ChunkFetchRequestHandler**:
```java
// In ChunkFetchRequestHandler.channelRead0()
@Override
protected void channelRead0(
    ChannelHandlerContext ctx,
    final ChunkFetchRequest msg) throws Exception {
  Channel channel = ctx.channel();

  // NEW: Send immediate wait time notification
  if (waitTimeNotificationEnabled && waitTimeEstimator != null) {
    long estimatedWaitMs = waitTimeEstimator.estimateWaitTimeMs();
    int queueDepth = waitTimeEstimator.getQueueDepth();

    if (logger.isDebugEnabled()) {
      logger.debug("Queue depth: {}, estimated wait: {}ms",
                   queueDepth, estimatedWaitMs);
    }

    WaitTimeNotification notification = new WaitTimeNotification(
      msg.streamChunkId.streamId(),
      estimatedWaitMs,
      queueDepth
    );

    // Send immediately, non-blocking
    channel.writeAndFlush(notification);
  }

  // Continue with existing request processing
  processFetchRequest(channel, msg);
}
```

6. **Add Server Metrics to ShuffleMetrics**:
```java
// In ExternalBlockHandler.ShuffleMetrics
private final Counter waitTimeNotificationsSent = new Counter();
private final Histogram estimatedWaitTimeMs = new Histogram(new UniformReservoir());
private final Gauge<Integer> queueDepthGauge;
private final Gauge<Double> avgDiskIOTimeGauge;  // Changed: Track disk I/O time

public ShuffleMetrics(NettyBasedWaitTimeEstimator estimator) {
  // ... existing metrics
  allMetrics.put("waitTimeNotificationsSent", waitTimeNotificationsSent);
  allMetrics.put("estimatedWaitTimeMs", estimatedWaitTimeMs);

  // Gauges that read from estimator
  queueDepthGauge = () -> estimator != null ? estimator.getQueueDepth() : 0;
  allMetrics.put("queueDepth", queueDepthGauge);

  avgDiskIOTimeGauge = () -> estimator != null ? estimator.getAvgDiskIOTimeMs() : 0.0;
  allMetrics.put("avgDiskIOTimeMs", avgDiskIOTimeGauge);  // Changed metric name
}
```

7. **Add Configuration in TransportConf**:
```java
public boolean waitTimeNotificationEnabled() {
  return conf.getBoolean("spark.shuffle.waitTimeNotification.enabled", false);
}

public long waitTimeEstimationUpdateIntervalMs() {
  return conf.getLong("spark.shuffle.waitTimeEstimation.updateIntervalMs", 100L);
}

public boolean waitTimeEstimationUseP99() {
  return conf.getBoolean("spark.shuffle.waitTimeEstimation.useP99", true);
}
```

**Key Advantages**:
- ✅ Uses **real** Netty EventLoop queue depth via `pendingTasks()`
- ✅ Tracks **only disk I/O time** via new `diskIOLatencyMillis` Timer (excludes RPC/auth overhead)
- ✅ **Low overhead**: Queue depth checked periodically within event loop
- ✅ **Production-ready**: Uses Codahale Metrics already in Spark
- ✅ **Accurate**: Based on actual queue state and isolated disk I/O time, not total request time

**Testing**:
- Unit tests for `NettyBasedWaitTimeEstimator` calculation logic
- Test queue depth monitoring with mock EventLoopGroup
- Test estimation with various queue depths and processing times
- Test periodic update scheduling
- Integration test: verify estimator initialization in TransportContext

**Commit**: "Add Netty-based server-side wait time estimation for shuffle requests"

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

#### Server-Side (Wait Time Notification)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `spark.shuffle.server.chunkFetchHandlerThreadsPercent` | `0` | **REQUIRED**: Percentage of server threads for separate chunk fetch handler. Must be > 0 to enable wait time notifications. Recommended: 50-100. |
| `spark.shuffle.waitTimeNotification.enabled` | `false` | Enable immediate wait time notification messages sent to clients when requests arrive |
| `spark.shuffle.waitTimeEstimation.updateIntervalMs` | `100` | Interval (ms) for monitoring Netty EventLoop queue depth. Lower = more accurate, higher = less overhead |
| `spark.shuffle.waitTimeEstimation.useP99` | `true` | Use 99th percentile processing time instead of mean for more conservative estimates |

#### Client-Side (Adaptive Fetch)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `spark.reducer.adaptiveMaxReqsInFlight.enabled` | `false` | Enable adaptive adjustment of max concurrent shuffle fetch requests based on server wait times |
| `spark.reducer.waitTime.threshold.ms` | `100` | Wait time threshold (ms) for triggering parallelism adjustment and request reordering |
| `spark.reducer.parallelism.adjustment.factor` | `1.5` | Factor to multiply maxReqsInFlight when servers are busy |
| `spark.shuffle.waitTime.history.size` | `10` | Number of historical wait time samples to track per server |

### Interaction with Existing Configuration

The adaptive fetch optimization works alongside existing shuffle configuration:

- **`spark.reducer.maxSizeInFlight`** (default: 48MB): Still enforced as upper bound on bytes in flight
- **`spark.reducer.maxReqsInFlight`** (default: Int.MaxValue): Becomes the baseline for adaptive adjustment
- **`spark.reducer.maxBlocksInFlightPerAddress`** (default: Int.MaxValue): Still enforced per-address

When adaptive mode is enabled, `effectiveMaxReqsInFlight` may temporarily exceed `maxReqsInFlight` up to 2x to overlap waiting.

#### Global Coordination (NEW)

| Parameter | Default | Description |
|-----------|---------|-------------|
| `spark.shuffle.fetch.loadCoordination.enabled` | `false` | Enable global load coordination for shuffle fetch with driver-based coordination |
| `spark.shuffle.fetch.loadMetrics.expirationMs` | `30000` | Time (ms) after which executor load metrics are considered stale and removed |
| `spark.shuffle.fetch.loadBroadcast.intervalMs` | `5000` | Interval (ms) for driver to broadcast global load view to all executors |
| `spark.shuffle.fetch.maxQueueDepth` | `100` | Max expected queue depth for capacity calculation (normalizes load metric) |
| `spark.shuffle.fetch.maxWaitTimeMs` | `5000` | Max expected wait time (ms) for capacity calculation (normalizes load metric) |
| `spark.shuffle.fetch.capacityWeight` | `0.7` | Weight for executor capacity in location scoring (0.0 to 1.0) |
| `spark.shuffle.fetch.waitTimeWeight` | `0.3` | Weight for estimated wait time in location scoring (0.0 to 1.0) |

### Recommended Configuration for Shuffle-Heavy Workloads

**Basic Configuration (Immediate Wait Time Notifications)**:
```properties
# Server-side: Enable separate thread pool and wait time notifications
spark.shuffle.server.chunkFetchHandlerThreadsPercent=100
spark.shuffle.waitTimeNotification.enabled=true
spark.shuffle.waitTimeEstimation.updateIntervalMs=100
spark.shuffle.waitTimeEstimation.useP99=true

# Client-side: Enable adaptive fetch behavior
spark.reducer.adaptiveMaxReqsInFlight.enabled=true
spark.reducer.waitTime.threshold.ms=100
spark.reducer.parallelism.adjustment.factor=1.5
```

**Advanced Configuration (With Global Coordination)**:
```properties
# Server-side: Enable separate thread pool and wait time notifications
spark.shuffle.server.chunkFetchHandlerThreadsPercent=100
spark.shuffle.waitTimeNotification.enabled=true
spark.shuffle.waitTimeEstimation.updateIntervalMs=100
spark.shuffle.waitTimeEstimation.useP99=true

# Client-side: Enable adaptive fetch behavior
spark.reducer.adaptiveMaxReqsInFlight.enabled=true
spark.reducer.waitTime.threshold.ms=100
spark.reducer.parallelism.adjustment.factor=1.5

# NEW: Global coordination (recommended for large clusters)
spark.shuffle.fetch.loadCoordination.enabled=true
spark.shuffle.fetch.loadBroadcast.intervalMs=5000
spark.shuffle.fetch.loadMetrics.expirationMs=30000
spark.shuffle.fetch.capacityWeight=0.7
spark.shuffle.fetch.waitTimeWeight=0.3
```

### Configuration Dependencies

⚠️ **Important**: The following dependencies must be satisfied:

1. **`spark.shuffle.server.chunkFetchHandlerThreadsPercent`** must be > 0 for wait time notifications to work
   - This creates the separate EventLoopGroup needed for queue depth monitoring
   - Without this, wait time estimation is not possible

2. **`spark.shuffle.waitTimeNotification.enabled`** must be true on servers for clients to receive notifications
   - Clients can enable adaptive fetch without server changes (falls back to historical timing)
   - But full benefit requires server-side notifications

3. **Global Coordination** (`spark.shuffle.fetch.loadCoordination.enabled=true`) has the following requirements:
   - Requires immediate wait time notifications to be enabled (dependencies 1 and 2 above)
   - All executors must report metrics via heartbeat (automatic when enabled)
   - Driver must have sufficient memory to maintain global view (~100 bytes per executor)
   - Works independently but provides maximum benefit when combined with adaptive fetch

---

## Metrics

### Server-Side Metrics

| Metric Name | Type | Description |
|-------------|------|-------------|
| `shuffle.server.waitTimeNotificationsSent` | Counter | Total number of wait time notifications sent |
| `shuffle.server.estimatedWaitTimeMs` | Histogram | Distribution of estimated wait times sent in notifications |
| `shuffle.server.queueDepthPerExecutor` | Gauge | Current queue depth per requesting executor |
| `shuffle.server.diskIOLatencyMillis` | Timer | Disk I/O time distribution (streamManager.getChunk only) |
| `shuffle.server.avgDiskIOTimeMs` | Gauge | Average disk I/O time from diskIOLatencyMillis Timer |
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

## Global Coordination Architecture

### Overview

While the immediate wait time notification provides local load awareness, a global coordination system enables cluster-wide optimization of shuffle fetch decisions. This architecture allows executors to report their load metrics to the driver, which maintains a global view and broadcasts it back to all executors.

### Architecture Components

```
┌─────────────────────────────────────────────────────────────────┐
│                         DRIVER                                   │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │      ShuffleFetchLoadCoordinator                           │ │
│  │  - Receives executor metrics via heartbeat                 │ │
│  │  - Maintains Map[ExecutorId → LoadMetrics]                 │ │
│  │  - Broadcasts GlobalLoadView every 5 seconds               │ │
│  └────────────────────────────────────────────────────────────┘ │
│                 ▲                              │                 │
│                 │ Heartbeat                    │ Broadcast       │
│                 │ + metrics                    ▼ GlobalLoadView  │
└─────────────────┼──────────────────────────────┼─────────────────┘
                  │                              │
         ┌────────┴────────┐          ┌─────────▼────────┐
         │   EXECUTOR 1    │          │   EXECUTOR 2     │
         │  ┌────────────┐ │          │  ┌────────────┐  │
         │  │ Metrics    │ │          │  │ Metrics    │  │
         │  │ Collector  │ │          │  │ Collector  │  │
         │  │ - queue    │ │          │  │ - queue    │  │
         │  │ - diskIO   │ │          │  │ - diskIO   │  │
         │  └────────────┘ │          │  └────────────┘  │
         │        │         │          │        │         │
         │        ▼         │          │        ▼         │
         │  ┌────────────┐ │          │  ┌────────────┐  │
         │  │ Fetcher    │ │          │  │ Fetcher    │  │
         │  │ - Uses     │ │          │  │ - Uses     │  │
         │  │   global   │ │          │  │   global   │  │
         │  │   view     │ │          │  │   view     │  │
         │  └────────────┘ │          │  └────────────┘  │
         └─────────────────┘          └──────────────────┘
```

### Data Structures

**ExecutorLoadMetrics** (reported by each executor):
```scala
case class ExecutorLoadMetrics(
  executorId: String,
  queueDepth: Int,              // Pending fetch requests in queue
  avgDiskIOTimeMs: Double,      // Average disk I/O time from diskIOLatencyMillis
  p99DiskIOTimeMs: Double,      // 99th percentile for conservative estimates
  estimatedWaitTimeMs: Long,    // queueDepth × avgDiskIOTime
  availableCapacity: Double,    // 0.0 (full) to 1.0 (idle)
  timestamp: Long,
  hostPort: String
)
```

**GlobalLoadView** (broadcast by driver):
```scala
case class GlobalLoadView(
  executorLoads: Map[String, ExecutorLoadMetrics],
  updateTime: Long,
  epoch: Long  // Version number for cache invalidation
)
```

### Component Implementation

#### 1. Driver-Side Coordinator

**File**: `core/src/main/scala/org/apache/spark/shuffle/ShuffleFetchLoadCoordinator.scala` (NEW)

```scala
private[spark] class ShuffleFetchLoadCoordinator(
    conf: SparkConf,
    listenerBus: LiveListenerBus) extends Logging {

  private val executorLoads = new ConcurrentHashMap[String, ExecutorLoadMetrics]()
  private val globalViewEpoch = new AtomicLong(0)
  private var cachedGlobalView: Option[Broadcast[GlobalLoadView]] = None

  // Configuration
  private val metricsExpirationMs =
    conf.getLong("spark.shuffle.fetch.loadMetrics.expirationMs", 30000)
  private val broadcastIntervalMs =
    conf.getLong("spark.shuffle.fetch.loadBroadcast.intervalMs", 5000)

  private val broadcastScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("shuffle-load-broadcaster")

  def start(): Unit = {
    broadcastScheduler.scheduleAtFixedRate(
      () => broadcastGlobalView(),
      broadcastIntervalMs,
      broadcastIntervalMs,
      TimeUnit.MILLISECONDS
    )
  }

  def updateExecutorMetrics(executorId: String, metrics: ExecutorLoadMetrics): Unit = {
    executorLoads.put(executorId, metrics)
  }

  def removeExecutor(executorId: String): Unit = {
    executorLoads.remove(executorId)
    broadcastGlobalView()  // Immediate update
  }

  private def broadcastGlobalView(): Unit = {
    // Remove stale metrics
    val now = System.currentTimeMillis()
    executorLoads.entrySet().removeIf(entry =>
      now - entry.getValue.timestamp > metricsExpirationMs
    )

    val view = GlobalLoadView(
      executorLoads.asScala.toMap,
      now,
      globalViewEpoch.incrementAndGet()
    )

    val bc = SparkEnv.get.broadcastManager.newBroadcast(view, isLocal = false)
    cachedGlobalView.foreach(_.unpersist())
    cachedGlobalView = Some(bc)
  }

  def getGlobalView(): Option[Broadcast[GlobalLoadView]] = cachedGlobalView

  def stop(): Unit = {
    broadcastScheduler.shutdown()
    cachedGlobalView.foreach(_.unpersist())
  }
}
```

#### 2. Heartbeat Integration

**File**: `core/src/main/scala/org/apache/spark/HeartbeatReceiver.scala` (MODIFY)

```scala
// Extend Heartbeat case class
private[spark] case class Heartbeat(
    executorId: String,
    accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
    blockManagerId: BlockManagerId,
    executorUpdates: Map[(Int, Int), ExecutorMetrics],
    shuffleFetchMetrics: Option[ExecutorLoadMetrics] = None  // NEW
)

// In HeartbeatReceiver class
private var loadCoordinator: Option[ShuffleFetchLoadCoordinator] = None

override def onStart(): Unit = {
  // existing logic...

  if (sc.conf.getBoolean("spark.shuffle.fetch.loadCoordination.enabled", false)) {
    loadCoordinator = Some(new ShuffleFetchLoadCoordinator(sc.conf, sc.listenerBus))
    loadCoordinator.foreach(_.start())
  }
}

override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
  case heartbeat @ Heartbeat(executorId, accumUpdates, blockManagerId,
                              executorUpdates, shuffleMetrics) =>
    // existing processing...

    // Process shuffle metrics
    shuffleMetrics.foreach { metrics =>
      loadCoordinator.foreach(_.updateExecutorMetrics(executorId, metrics))
    }

    context.reply(HeartbeatResponse(reregisterBlockManager))
}
```

#### 3. Executor-Side Metrics Collection

**File**: `core/src/main/scala/org/apache/spark/executor/ShuffleFetchMetricsCollector.scala` (NEW)

```scala
private[spark] class ShuffleFetchMetricsCollector(
    blockManager: BlockManager,
    conf: SparkConf) extends Logging {

  def collectMetrics(): ExecutorLoadMetrics = {
    // Access network layer metrics through BlockManager
    val networkMetrics = blockManager.shuffleClient match {
      case netty: NettyBlockTransferService => netty.getShuffleMetrics()
      case _ => return ExecutorLoadMetrics.empty(blockManager.blockManagerId.executorId)
    }

    val queueDepth = networkMetrics.queueDepth.getValue
    val diskIOSnapshot = networkMetrics.diskIOLatencyMillis.getSnapshot
    val avgDiskIO = diskIOSnapshot.getMean()
    val p99DiskIO = diskIOSnapshot.get99thPercentile()

    val estimatedWait = if (queueDepth > 0) {
      (queueDepth * avgDiskIO).toLong
    } else {
      0L
    }

    val capacity = calculateCapacity(queueDepth, estimatedWait)

    ExecutorLoadMetrics(
      executorId = blockManager.blockManagerId.executorId,
      queueDepth = queueDepth,
      avgDiskIOTimeMs = avgDiskIO,
      p99DiskIOTimeMs = p99DiskIO,
      estimatedWaitTimeMs = estimatedWait,
      availableCapacity = capacity,
      timestamp = System.currentTimeMillis(),
      hostPort = blockManager.blockManagerId.hostPort
    )
  }

  private def calculateCapacity(queueDepth: Int, waitMs: Long): Double = {
    val maxQueue = conf.getInt("spark.shuffle.fetch.maxQueueDepth", 100)
    val maxWait = conf.getLong("spark.shuffle.fetch.maxWaitTimeMs", 5000)

    val queueLoad = Math.min(1.0, queueDepth.toDouble / maxQueue)
    val waitLoad = Math.min(1.0, waitMs.toDouble / maxWait)

    1.0 - Math.max(queueLoad, waitLoad)  // Inverse of load = capacity
  }
}
```

**File**: `core/src/main/scala/org/apache/spark/executor/Executor.scala` (MODIFY)

```scala
// Add field
private val shuffleMetricsCollector =
  new ShuffleFetchMetricsCollector(env.blockManager, conf)

// In heartbeat reporting method
private def reportHeartBeat(): Unit = {
  val shuffleMetrics = if (conf.getBoolean("spark.shuffle.fetch.loadCoordination.enabled", false)) {
    Some(shuffleMetricsCollector.collectMetrics())
  } else {
    None
  }

  val message = Heartbeat(
    executorId,
    accumUpdates,
    env.blockManager.blockManagerId,
    executorUpdates,
    shuffleMetrics  // NEW
  )

  // Send to driver
}
```

#### 4. Executor-Side Fetch Location Selection

**File**: `core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala` (MODIFY)

```scala
// Get global load view
private def getGlobalLoadView(): Option[GlobalLoadView] = {
  try {
    blockManager.master.getShuffleFetchLoadView()
  } catch {
    case NonFatal(e) =>
      logWarning(s"Failed to get global load view: ${e.getMessage}")
      None
  }
}

// Select best location using global view
private def selectBestLocation(
    blockId: BlockId,
    locations: Seq[BlockManagerId]): BlockManagerId = {

  if (!conf.getBoolean("spark.shuffle.fetch.loadCoordination.enabled", false)) {
    return locations(Random.nextInt(locations.size))
  }

  getGlobalLoadView() match {
    case Some(globalView) =>
      selectLocationByLoad(locations, globalView)
    case None =>
      locations(Random.nextInt(locations.size))  // Fallback
  }
}

private def selectLocationByLoad(
    locations: Seq[BlockManagerId],
    globalView: GlobalLoadView): BlockManagerId = {

  val loadsForLocations = locations.flatMap { loc =>
    globalView.executorLoads.get(loc.executorId).map(load => (loc, load))
  }

  if (loadsForLocations.isEmpty) {
    return locations(Random.nextInt(locations.size))
  }

  // Score each location (higher = better)
  val scored = loadsForLocations.map { case (loc, metrics) =>
    val score = computeLocationScore(metrics)
    (loc, score, metrics)
  }

  val (bestLoc, bestScore, bestMetrics) = scored.maxBy(_._2)

  logDebug(s"Selected $bestLoc with score $bestScore " +
           s"(queue: ${bestMetrics.queueDepth}, wait: ${bestMetrics.estimatedWaitTimeMs}ms)")

  bestLoc
}

private def computeLocationScore(metrics: ExecutorLoadMetrics): Double = {
  val capacityWeight = conf.getDouble("spark.shuffle.fetch.capacityWeight", 0.7)
  val waitTimeWeight = conf.getDouble("spark.shuffle.fetch.waitTimeWeight", 0.3)

  val capacityScore = metrics.availableCapacity * capacityWeight

  val waitTimeScore = if (metrics.estimatedWaitTimeMs > 0) {
    (1.0 / (1.0 + metrics.estimatedWaitTimeMs / 1000.0)) * waitTimeWeight
  } else {
    1.0 * waitTimeWeight
  }

  capacityScore + waitTimeScore
}

// Integrate into existing fetchUpToMaxBytes method
private def fetchUpToMaxBytes(): Unit = {
  // When grouping blocks by address, use selectBestLocation
  val blocksByAddress = blockInfos.groupBy { case (blockId, _, _) =>
    val locations = getLocationsFromMapOutputTracker(blockId)
    selectBestLocation(blockId, locations)  // NEW: Global load aware selection
  }
  // ... rest of existing logic
}
```

**File**: `core/src/main/scala/org/apache/spark/storage/BlockManagerMaster.scala` (MODIFY)

```scala
def getShuffleFetchLoadView(): Option[GlobalLoadView] = {
  if (driverEndpoint == null) return None
  driverEndpoint.askSync[Option[GlobalLoadView]](GetShuffleFetchLoadView)
}
```

### Data Flow

#### Metric Reporting (Every 10 seconds via heartbeat):
1. Executor serves shuffle fetch → tracks disk I/O time in `diskIOLatencyMillis`
2. `ShuffleFetchMetricsCollector` reads queue depth + disk I/O metrics
3. Executor sends `Heartbeat` + `ExecutorLoadMetrics` to driver
4. Driver's `HeartbeatReceiver` forwards to `ShuffleFetchLoadCoordinator`
5. Coordinator updates `Map[ExecutorId → LoadMetrics]`

#### Global View Broadcast (Every 5 seconds):
1. `ShuffleFetchLoadCoordinator` removes stale metrics (> 30s old)
2. Creates `GlobalLoadView` with all executor metrics
3. Broadcasts view using Spark's broadcast mechanism
4. Executors cache broadcast reference

#### Fetch Decision (Per block fetch):
1. `ShuffleBlockFetcherIterator` needs to fetch block with locations [E1, E2, E3]
2. Fetches `GlobalLoadView` from broadcast
3. Looks up metrics for E1, E2, E3
4. Scores each location: `score = 0.7 × capacity + 0.3 × (1 / waitTime)`
5. Selects highest scoring executor
6. Fetches block from best executor

### Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `spark.shuffle.fetch.loadCoordination.enabled` | `false` | Enable global load coordination for shuffle fetch |
| `spark.shuffle.fetch.loadMetrics.expirationMs` | `30000` | Time in ms after which executor load metrics are stale |
| `spark.shuffle.fetch.loadBroadcast.intervalMs` | `5000` | Interval in ms for broadcasting global load view |
| `spark.shuffle.fetch.maxQueueDepth` | `100` | Max expected queue depth for capacity calculation |
| `spark.shuffle.fetch.maxWaitTimeMs` | `5000` | Max expected wait time in ms for capacity calculation |
| `spark.shuffle.fetch.capacityWeight` | `0.7` | Weight for executor capacity in location scoring |
| `spark.shuffle.fetch.waitTimeWeight` | `0.3` | Weight for wait time in location scoring |

### Performance Analysis

**Overhead**:
- **Heartbeat**: +100 bytes per executor per 10s = negligible
- **Broadcast**: ~100 bytes × N executors every 5s
  - For 1000 executors: 100 KB broadcast every 5s = 20 KB/s
  - Uses BitTorrent-like distribution, shared among executors
- **Computation**: O(1) per executor, O(N) aggregation driver-side
- **Total**: < 1% of shuffle network traffic

**Scalability**:
- **Up to 1,000 executors**: Works well with defaults
- **Up to 10,000 executors**: Increase broadcast interval to 10-15s
- **Memory**: ~100 bytes per executor on driver

**Fault Tolerance**:
- Stale metrics removed automatically (3 missed heartbeats)
- Falls back to random selection if global view unavailable
- No correctness impact, only performance degradation
- Fast recovery on network partition resolution

### Files to Modify

1. `core/src/main/scala/org/apache/spark/shuffle/ShuffleFetchLoadCoordinator.scala` (NEW)
2. `core/src/main/scala/org/apache/spark/HeartbeatReceiver.scala`
3. `core/src/main/scala/org/apache/spark/executor/ShuffleFetchMetricsCollector.scala` (NEW)
4. `core/src/main/scala/org/apache/spark/executor/Executor.scala`
5. `core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala`
6. `core/src/main/scala/org/apache/spark/storage/BlockManagerMaster.scala`
7. `core/src/main/scala/org/apache/spark/internal/config/package.scala`

---

## Future Enhancements

1. **External Shuffle Service Support**: Extend to external shuffle service (more complex due to separate process)

2. **Machine Learning-Based Estimation**: Use ML model to predict wait time based on block size, server load, time of day, etc.

3. **Adaptive Timeout**: Adjust request timeout based on estimated wait time

4. **Push-Based Shuffle Integration**: Combine with push-based shuffle for hybrid approach

---

## References

**Related JIRAs**:
- [SPARK-25250](https://issues.apache.org/jira/browse/SPARK-25250) - Push-based shuffle
- [SPARK-30602](https://issues.apache.org/jira/browse/SPARK-30602) - Adaptive shuffle fetch

**Key Files (Existing Infrastructure)**:
- `common/network-common/src/main/java/org/apache/spark/network/TransportContext.java:143-146` - chunkFetchWorkers EventLoopGroup
- `common/network-common/src/main/java/org/apache/spark/network/server/ChunkFetchRequestHandler.java` - Separate thread pool handler
- `common/network-common/src/main/java/org/apache/spark/network/util/TransportConf.java:474-491` - chunkFetchHandlerThreadsPercent config
- `common/network-shuffle/src/main/java/org/apache/spark/network/shuffle/ExternalBlockHandler.java:315-373` - Existing ShuffleMetrics

**Key Files (Client-Side)**:
- `core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala` - Client fetch logic
- `core/src/main/scala/org/apache/spark/executor/ShuffleReadMetrics.scala` - Client metrics

**Key Files (Protocol)**:
- `common/network-shuffle/src/main/java/org/apache/spark/network/shuffle/protocol/StreamHandle.java` - Protocol messages
- `common/network-common/src/main/java/org/apache/spark/network/protocol/` - Base protocol classes

**Netty Monitoring Resources**:
- [Monitoring Netty EventLoop Queue Depth - Stack Overflow](https://stackoverflow.com/questions/32933367/monitoring-the-size-of-the-netty-event-loop-queues)
- [Netty Issue #8630 - EventLoopGroup Queue Monitoring](https://github.com/netty/netty/issues/8630)
- [Netty EventLoop.pendingTasks() API](https://netty.io/4.0/api/io/netty/channel/nio/NioEventLoop.html)

**Metrics Libraries**:
- [Codahale Metrics (Dropwizard Metrics)](https://metrics.dropwizard.io/) - Already used in Spark
- Timer, Histogram, Counter, Gauge documentation

**Related Papers**:
- Riffle: Optimized Shuffle Service for Large-Scale Data Analytics
- Sailfish: A Framework for Large Scale Data Processing
