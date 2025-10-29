# Server-Side Executor Shuffle Fetch Latency Reporting

## Problem Statement

Currently, the `fetch-metrics` branch tracks server-side shuffle fetch latency **per-shuffle** with breakdowns (disk I/O, queue depth, response send time). However, when shuffle operations are slow, we need to identify **which executor servers are slow at processing requests and WHY**.

### Existing Metrics:
- **Client-side** (`fetch-wait-dist` branch): Tracks wait time per remote executor (client perspective)
- **Server-side** (current `fetch-metrics` branch): Tracks processing time per shuffle, not per executor

### Gap:
We cannot answer: "Which executor servers are bottlenecks when serving shuffle data, and what's causing the slowness (disk I/O, queue contention, network send)?"

## Goals

1. **Aggregate server-side metrics per executor**: Each executor tracks its own performance serving shuffle fetch requests
2. **Report top-K slow executors**: After stage completion, identify top 3 executors with highest latency
3. **Provide actionable breakdown**: Include disk I/O, queue depth, and send time metrics to diagnose root cause
4. **Minimal overhead**: Leverage existing metrics collection, add lightweight aggregation

## Current Architecture

### Server-Side Metrics Collection

```
ChunkFetchRequestHandler (per connection)
  ├─ Tracks per-request timing:
  │    ├─ Total latency (processFetchRequest start to response callback)
  │    ├─ Disk read latency (ManagedBuffer read time)
  │    └─ Response send latency (writeAndFlush callback time)
  ├─ Updates global timers in ShuffleFetchMetrics
  └─ Updates per-shuffle timers (recent addition)

ShuffleFetchMetrics (per ExternalBlockHandler)
  ├─ Global metrics: chunkFetchLatencyMillis, chunkReadLatencyMillis, etc.
  ├─ Queue depth: chunkFetchQueueDepth counter
  └─ Per-shuffle timers: Map[shuffleId → Timer]

ExternalBlockHandler
  └─ Exposes ShuffleFetchMetrics via ShuffleMetricsSource interface
```

### Metrics Flow:
- Metrics are collected on the **serving executor** (the one holding shuffle data)
- Currently **NOT sent to driver** - only exposed via metrics system (Dropwizard)
- No aggregation or reporting mechanism at stage completion

### Understanding ChunkFetchRequestHandler vs ExternalBlockHandler

These two classes work together in different layers of the shuffle fetch protocol:

#### **ExternalBlockHandler** (High-Level RPC Handler)
- **Package**: `org.apache.spark.network.shuffle`
- **Extends**: `RpcHandler`
- **Role**: Application-level protocol handler for shuffle/block operations

**Responsibilities**:
1. **RPC message handling**: Processes high-level block transfer messages:
   - `FetchShuffleBlocks` - Request to fetch shuffle blocks
   - `RegisterExecutor` - Register executor metadata
   - `OpenBlocks` - Open RDD blocks
   - Push-based shuffle operations

2. **Stream registration**: When a fetch request comes in:
   - Resolves block IDs to disk files via `ExternalShuffleBlockResolver`
   - Creates iterator of `ManagedBuffer` objects
   - Registers stream with `OneForOneStreamManager`
   - Returns `streamId` to client

3. **Metrics ownership**: Owns the `ShuffleFetchMetrics` object
4. **Business logic**: Auth checks, executor registration, block resolution

#### **ChunkFetchRequestHandler** (Low-Level Data Transfer)
- **Package**: `org.apache.spark.network.server`
- **Extends**: `SimpleChannelInboundHandler<ChunkFetchRequest>`
- **Role**: Netty channel handler for actual data transfer

**Responsibilities**:
1. **Data transfer**: Handles `ChunkFetchRequest` messages:
   - Fetches chunks by streamId and chunkIndex
   - Reads data from disk (via `StreamManager.getChunk()`)
   - Sends `ChunkFetchSuccess` or `ChunkFetchFailure` responses

2. **Performance isolation**: Runs on separate event loop group to prevent disk I/O from blocking other RPCs

3. **Metrics collection**: Records timing for:
   - Total request processing time
   - Disk read latency
   - Network send latency
   - Queue depth

4. **Low-level operation**: Direct Netty channel operations, no business logic

#### **Two-Phase Fetch Protocol**

```
Client Request Flow:
═══════════════════

Phase 1: Stream Setup (via ExternalBlockHandler)
┌─────────────────────────────────────────────────┐
│ Client sends FetchShuffleBlocks RPC             │
│   ↓                                             │
│ ExternalBlockHandler.receive()                  │
│   ├─ Decode message                             │
│   ├─ Resolve blockIds → disk files             │
│   ├─ Register stream with StreamManager         │
│   └─ Return streamId to client                  │
│                                                 │
│ Response: StreamHandle(streamId, numChunks)     │
└─────────────────────────────────────────────────┘

Phase 2: Data Transfer (via ChunkFetchRequestHandler)
┌─────────────────────────────────────────────────┐
│ For each chunk (0 to numChunks-1):              │
│   Client sends ChunkFetchRequest(streamId, i)   │
│     ↓                                           │
│   ChunkFetchRequestHandler.channelRead0()       │
│     ├─ Get chunk from StreamManager             │
│     ├─ Read data from disk (TIMED)             │
│     ├─ Send ChunkFetchSuccess                   │
│     └─ Record metrics (latency breakdown)       │
│                                                 │
│   Response: ChunkFetchSuccess(chunk data)       │
└─────────────────────────────────────────────────┘
```

#### **Metrics Connection (TransportContext.java:241-253)**

```java
// When creating ChunkFetchRequestHandler, metrics are passed from
// ExternalBlockHandler if it implements ShuffleMetricsSource
if (rpcHandler instanceof ShuffleMetricsSource) {
  ShuffleFetchMetrics metrics =
    ((ShuffleMetricsSource) rpcHandler).getShuffleFetchMetrics();

  chunkFetchHandler = new ChunkFetchRequestHandler(
    client,
    rpcHandler.getStreamManager(),  // From ExternalBlockHandler
    maxChunks,
    syncMode,
    metrics);  // SHARED metrics object
}
```

**Key Point**: Both handlers share the **same `ShuffleFetchMetrics` instance**:
- `ChunkFetchRequestHandler` **writes** metrics (collects timing data)
- `ExternalBlockHandler` **owns** metrics (exposes for aggregation)

#### **Comparison Table**

| Aspect | ExternalBlockHandler | ChunkFetchRequestHandler |
|--------|---------------------|-------------------------|
| **Layer** | Application/RPC | Transport/Data |
| **Protocol** | BlockTransferMessage | ChunkFetchRequest |
| **Purpose** | Setup fetch operation | Execute data transfer |
| **When Called** | Once per fetch request | Once per chunk (N times) |
| **Operations** | Block resolution, stream setup | Disk I/O, data transfer |
| **Thread Pool** | Default RPC thread pool | Dedicated chunk fetch workers |
| **Metrics** | Owns ShuffleFetchMetrics | Updates ShuffleFetchMetrics |
| **Location** | `network-shuffle` module | `network-common` module |

#### **Why Two Separate Classes?**

1. **Separation of concerns**: RPC handling vs data transfer
2. **Performance isolation**: Disk I/O runs on separate thread pool to avoid blocking RPCs
3. **Protocol layering**: High-level (FetchShuffleBlocks) vs low-level (chunks)
4. **Reusability**: ChunkFetchRequestHandler works with any RpcHandler/StreamManager

#### **Implementation Impact**

For server-side executor metrics:
- ✅ **Metrics collection**: Already happening in `ChunkFetchRequestHandler` (no changes needed)
- ✅ **Metrics storage**: In `ShuffleFetchMetrics` owned by `ExternalBlockHandler`
- 🆕 **What we add**: Aggregate executor-level stats in `ShuffleFetchMetrics`
- 🆕 **Expose via**: Add method in `ExternalBlockHandler` to get aggregated stats
- 🆕 **Send to driver**: Collect from `ExternalBlockHandler` in `Executor.reportHeartBeat()`

The layered architecture means:
- **ChunkFetchRequestHandler**: Continue collecting per-request metrics (unchanged)
- **ShuffleFetchMetrics**: Add executor-level aggregation logic (new)
- **ExternalBlockHandler**: Expose aggregated stats method (new)
- **Executor**: Call ExternalBlockHandler to get stats for heartbeat (new)

## Proposed Solution

### Architecture Overview

```
┌────────────────────────────────────────────────────────────────┐
│ Server Executor (serves shuffle data)                          │
│                                                                 │
│  ExternalBlockHandler / ShuffleFetchMetrics                    │
│  ├─ Existing: per-request metrics collection                   │
│  └─ NEW: Aggregate into executor-level summary                 │
│                                                                 │
│  Executor.reportHeartBeat()                                    │
│  └─ NEW: Include server-side shuffle fetch stats in heartbeat  │
└────────────────────────────────────────────────────────────────┘
                            │
                            │ Heartbeat (every 10s)
                            ▼
┌────────────────────────────────────────────────────────────────┐
│ Driver (HeartbeatReceiver → TaskScheduler → DAGScheduler)      │
│                                                                 │
│  NEW: ServerShuffleFetchStatsAggregator                        │
│  ├─ Merge stats from executor heartbeats                       │
│  ├─ Track per-executor aggregates                              │
│  ├─ Maintain top-K executors by total latency                  │
│  └─ Handle executor removal (drop stats for lost executors)    │
│                                                                 │
│  DAGScheduler.markStageAsFinished()                            │
│  └─ NEW: Log top-K slow server executors with breakdown        │
└────────────────────────────────────────────────────────────────┘
```

### Key Design Decisions

#### 1. Aggregation Level: Per-Executor Self-Tracking
- Each executor tracks its **own** performance serving requests
- Answers: "How slow am I at serving shuffle data?"
- NOT tracking per-client (don't care which executor requested from me)

#### 2. Metrics to Track
- **Request count**: Number of fetch requests served
- **Total latency**: Sum/avg/percentiles of total request processing time
- **Disk I/O breakdown**: Sum/avg of disk read time
- **Send time breakdown**: Sum/avg of network send time
- **Queue depth**: Max concurrent requests (already tracked)
- **Time window**: Since last heartbeat (incremental updates)

#### 3. Transport Mechanism: Heartbeats
- Reuse existing heartbeat infrastructure
- Send incremental stats (delta since last heartbeat)
- Driver maintains cumulative view per stage/globally

#### 4. Top-K Selection: Total Latency
- Rank executors by **total cumulative latency** (sum of all request latencies)
- Rationale: Identifies executors that contribute most to overall shuffle time
- Alternative considered: avg latency (but doesn't account for request volume)

#### 5. Config-Gated: Opt-In Feature
- Disabled by default to avoid overhead
- Config: `spark.shuffle.serverFetchStats.enabled` (default: false)
- Config: `spark.shuffle.serverFetchStats.topK` (default: 3)

## Data Model

### 1. ServerShuffleFetchStats (Executor → Driver)

```scala
/**
 * Server-side shuffle fetch statistics for a single executor.
 * Represents incremental metrics since last heartbeat.
 */
private[spark] case class ServerShuffleFetchStats(
  executorId: String,

  // Request volume
  requestCount: Long,

  // Latency metrics (in milliseconds)
  totalLatencySum: Long,        // Sum of all request latencies
  totalLatencyMax: Long,        // Max single request latency

  // Breakdown metrics (sum and max for spike detection)
  diskReadLatencySum: Long,     // Sum of disk I/O time
  diskReadLatencyMax: Long,     // Max single disk read time
  sendLatencySum: Long,         // Sum of network send time
  sendLatencyMax: Long,         // Max single network send time

  // Queue wait time metrics (in milliseconds)
  queueWaitTimeSum: Long,       // Sum of queue wait times
  queueWaitTimeMax: Long,       // Max queue wait time (detects queueing delays)

  // Queue length metrics (sampled - requests waiting in queue)
  maxQueueLength: Int,          // Peak number of waiting requests (sampled)
  avgQueueLength: Double        // Average number of waiting requests (sampled)
)
```

### 2. ServerShuffleFetchAggregate (Driver-Side)

```scala
/**
 * Aggregated server-side shuffle fetch statistics at the driver.
 * Maintains cumulative view across heartbeats.
 */
private[spark] case class ServerShuffleFetchAggregate(
  executorId: String,

  // Cumulative metrics
  totalRequests: Long,
  totalLatencySum: Long,
  totalLatencyMax: Long,
  diskReadLatencySum: Long,
  diskReadLatencyMax: Long,
  sendLatencySum: Long,
  sendLatencyMax: Long,
  queueWaitTimeSum: Long,
  queueWaitTimeMax: Long,
  maxQueueLength: Int,
  avgQueueLength: Double
) {
  def avgLatency: Double =
    if (totalRequests > 0) totalLatencySum.toDouble / totalRequests else 0.0

  def avgDiskReadLatency: Double =
    if (totalRequests > 0) diskReadLatencySum.toDouble / totalRequests else 0.0

  def avgSendLatency: Double =
    if (totalRequests > 0) sendLatencySum.toDouble / totalRequests else 0.0

  def avgQueueWaitTime: Double =
    if (totalRequests > 0) queueWaitTimeSum.toDouble / totalRequests else 0.0

  def diskReadPercent: Double =
    if (totalLatencySum > 0) (diskReadLatencySum.toDouble / totalLatencySum) * 100 else 0.0

  def sendPercent: Double =
    if (totalLatencySum > 0) (sendLatencySum.toDouble / totalLatencySum) * 100 else 0.0

  def queueWaitPercent: Double =
    if (totalLatencySum > 0) (queueWaitTimeSum.toDouble / totalLatencySum) * 100 else 0.0

  def queueWaitImbalanceRatio: Double =
    if (avgQueueWaitTime > 0) queueWaitTimeMax.toDouble / avgQueueWaitTime else 1.0

  def queueLengthImbalanceRatio: Double =
    if (avgQueueLength > 0) maxQueueLength.toDouble / avgQueueLength else 1.0

  // Validation: queue wait time ≈ queue length × avg processing time
  def expectedQueueWaitTime: Double =
    avgQueueLength * avgLatency

  def queueWaitValidation: Double =
    if (expectedQueueWaitTime > 0) avgQueueWaitTime / expectedQueueWaitTime else 0.0
}
```

### 3. ServerShuffleFetchStatsAggregator (Driver-Side)

```scala
/**
 * Aggregates server-side shuffle fetch statistics across executors.
 * Maintains top-K executors by total latency.
 */
private[spark] class ServerShuffleFetchStatsAggregator(topK: Int) {

  // Map: executorId → cumulative aggregate
  private val executorStats = new ConcurrentHashMap[String, ServerShuffleFetchAggregate]

  /**
   * Merge incremental stats from executor heartbeat.
   */
  def mergeStats(stats: ServerShuffleFetchStats): Unit

  /**
   * Remove stats for lost executor.
   */
  def removeExecutor(executorId: String): Unit

  /**
   * Get top-K executors by total latency.
   */
  def getTopK(): Seq[ServerShuffleFetchAggregate]

  /**
   * Reset stats (called after stage completion).
   */
  def reset(): Unit
}
```

## Leveraging Existing Metrics (Commit b774aecc346)

### Available Metrics from ExternalBlockHandler.ShuffleMetrics

The server-side metrics infrastructure is **already implemented** in commit b774aecc346. We leverage existing Codahale metrics:

```java
// Located in: ExternalBlockHandler.ShuffleMetrics
// These metrics are already being collected!

Timer chunkFetchLatencyMillis;      // Total request processing time
Timer chunkReadLatencyMillis;       // Disk I/O breakdown
Timer responseSendLatencyMillis;    // Network send breakdown
Counter chunkFetchQueueDepth;       // Current concurrent requests
```

### What Codahale Metrics Provide

**Timer** (automatically tracks):
- `getCount()`: Number of events recorded
- `getSnapshot().getMean()`: Average duration
- `getSnapshot().getMax()`: Maximum duration
- `getSnapshot().getMin()`: Minimum duration
- Percentiles (p50, p75, p95, p99)

**Counter** (simple counter):
- `getCount()`: Current value
- Incremented when request starts, decremented when completes

### Queue Wait Time Measurement

**Goal**: Measure time requests spend waiting in queue before processing starts

**Key Insight**: Queue wait time = (processing start time) - (request arrival time)

```java
// Add to ExternalBlockHandler.ShuffleMetrics
private final Timer queueWaitTimeMillis =
  new TimerWithCustomTimeUnit(TimeUnit.MILLISECONDS);

public Timer getQueueWaitTimeMillis() {
  return queueWaitTimeMillis;
}
```

**Implementation in ChunkFetchRequestHandler**:

```java
@Override
protected void channelRead0(
    ChannelHandlerContext ctx,
    final ChunkFetchRequest msg) throws Exception {

  // Record arrival time
  final long arrivalTimeNanos = System.nanoTime();

  // If using dedicated thread pool, request is queued here
  // When processing starts, measure queue wait time
  final long processingStartNanos = System.nanoTime();
  final long queueWaitNanos = processingStartNanos - arrivalTimeNanos;

  // Record queue wait time
  if (queueWaitTimeMillis != null) {
    queueWaitTimeMillis.update(queueWaitNanos, TimeUnit.NANOSECONDS);
  }

  // Continue with existing processing...
  processFetchRequest(channel, msg);
}
```

**Note**: For executors without dedicated chunk fetch thread pool, queue wait time will be ~0 (no queueing).

### Queue Length Sampling (from Commit 9c40b5b702e)

**Goal**: Sample number of requests waiting in queue (not started processing yet)

**Source**: Netty EventLoop's `pendingTasks()` method (from commit 9c40b5b702e design)

```java
// Add to ExternalBlockHandler.ShuffleMetrics
private final Histogram queueLengthHistogram =
  new Histogram(new UniformReservoir());

// Scheduled sampling within each EventLoop (cheap when called from within)
public void initializeQueueLengthSampling(EventLoopGroup chunkFetchWorkers) {
  chunkFetchWorkers.forEach(eventLoop -> {
    eventLoop.scheduleAtFixedRate(() -> {
      try {
        // pendingTasks() is cheap when called from within event loop
        int pending = eventLoop.pendingTasks();
        queueLengthHistogram.update(pending);
      } catch (Exception e) {
        // Log and continue
      }
    }, 0, 100, TimeUnit.MILLISECONDS);  // Sample every 100ms
  });
}

// At heartbeat time, get statistics
Snapshot queueLengthSnap = queueLengthHistogram.getSnapshot();
int maxQueueLength = (int) queueLengthSnap.getMax();
double avgQueueLength = queueLengthSnap.getMean();
```

**Key Points**:
- `pendingTasks()` returns number of tasks **waiting** in the EventLoop queue
- Must be called from **within** the event loop to be cheap (non-blocking)
- Periodic sampling (100ms) provides accurate statistics without overhead

### Heartbeat Integration: Read and Send

At heartbeat time, **snapshot existing metrics** and send to driver:

```scala
// In Executor.reportHeartBeat()
def getServerShuffleStats(): Option[ServerShuffleFetchStats] = {
  if (!enabled) return None

  val handler = blockManager.externalBlockHandler
  val metrics = handler.getShuffleMetrics()

  // Snapshot Timers and Histograms
  val fetchSnap = metrics.chunkFetchLatencyMillis.getSnapshot()
  val diskSnap = metrics.chunkReadLatencyMillis.getSnapshot()
  val sendSnap = metrics.responseSendLatencyMillis.getSnapshot()
  val queueWaitSnap = metrics.queueWaitTimeMillis.getSnapshot()
  val queueLengthSnap = metrics.queueLengthHistogram.getSnapshot()

  val count = metrics.chunkFetchLatencyMillis.getCount()

  Some(ServerShuffleFetchStats(
    executorId = executorId,
    requestCount = count,

    // Total latency (derive sum from mean * count)
    totalLatencySum = (fetchSnap.getMean() * count).toLong,
    totalLatencyMax = fetchSnap.getMax(),

    // Disk breakdown
    diskReadLatencySum = (diskSnap.getMean() * count).toLong,
    diskReadLatencyMax = diskSnap.getMax(),

    // Send breakdown
    sendLatencySum = (sendSnap.getMean() * count).toLong,
    sendLatencyMax = sendSnap.getMax(),

    // Queue wait time statistics
    queueWaitTimeSum = (queueWaitSnap.getMean() * count).toLong,
    queueWaitTimeMax = queueWaitSnap.getMax(),

    // Queue length statistics
    maxQueueLength = queueLengthSnap.getMax().toInt,
    avgQueueLength = queueLengthSnap.getMean()
  ))
}
```

### Key Benefits

1. ✅ **Reuses existing infrastructure** - No new metric collection needed
2. ✅ **Production-tested** - Codahale metrics already in Spark
3. ✅ **Low overhead** - Metrics already being updated, just read at heartbeat time
4. ✅ **Simple implementation** - Just add Histogram for queue depth sampling
5. ✅ **Accurate** - Real metrics from actual request processing

### Implementation Simplification

This approach **eliminates** the need for:
- ❌ Custom metric tracking in ChunkFetchRequestHandler (already done!)
- ❌ New metric storage data structures (use Codahale!)
- ❌ Complex aggregation logic at collection time (Codahale does it!)

We **only need to add**:
- ✅ Timer for queue wait time measurement
- ✅ Histogram for queue length sampling (from Netty pendingTasks)
- ✅ Queue wait time tracking in ChunkFetchRequestHandler
- ✅ Queue length sampling initialization (periodic within EventLoop)
- ✅ Heartbeat integration to read and send snapshots
- ✅ Driver-side aggregation and validation formula

## Implementation Plan

### Phase 1: Data Model and Aggregation Logic
**Goal**: Define data structures and aggregation logic without integration

**Tasks**:
1. Create `ServerShuffleFetchStats.scala` with case classes
2. Create `ServerShuffleFetchStatsAggregator.scala` with aggregation logic
3. Write unit tests for aggregator (merge, top-K, executor removal)
4. **Commit**: "Add data model for server-side shuffle fetch stats"

**Files**:
- `core/src/main/scala/org/apache/spark/shuffle/ServerShuffleFetchStats.scala` (new)
- `core/src/main/scala/org/apache/spark/shuffle/ServerShuffleFetchStatsAggregator.scala` (new)
- `core/src/test/scala/org/apache/spark/shuffle/ServerShuffleFetchStatsAggregatorSuite.scala` (new)

### Phase 2: Queue Wait Time and Length Measurement
**Goal**: Add Timer for queue wait time and Histogram for queue length sampling

**Tasks**:
1. Add `Timer queueWaitTimeMillis` to `ExternalBlockHandler.ShuffleMetrics`
2. Add `Histogram queueLengthHistogram` to `ExternalBlockHandler.ShuffleMetrics`
3. Modify `ChunkFetchRequestHandler.channelRead0()` to:
   - Record arrival time at method entry
   - Record processing start time before calling `processFetchRequest()`
   - Update timer with `queueWaitNanos = startTime - arrivalTime`
4. Add `initializeQueueLengthSampling()` method in `ExternalBlockHandler`:
   - Schedule periodic sampling (100ms) within each EventLoop
   - Sample `eventLoop.pendingTasks()` and update histogram
5. Call initialization method when creating TransportContext with chunkFetchWorkers
6. Add config `spark.shuffle.serverFetchStats.enabled` to `package.scala`
7. Write tests for queue wait time and queue length tracking
8. **Commit**: "Add queue wait time and length measurement for server-side shuffle stats"

**Files**:
- `common/network-shuffle/src/main/java/org/apache/spark/network/shuffle/ExternalBlockHandler.java` (add metrics and sampling)
- `common/network-common/src/main/java/org/apache/spark/network/server/ChunkFetchRequestHandler.java` (add timing logic)
- `common/network-common/src/main/java/org/apache/spark/network/TransportContext.java` (initialize sampling)
- `core/src/main/scala/org/apache/spark/internal/config/package.scala` (add config)
- `common/network-common/src/test/java/org/apache/spark/network/ChunkFetchRequestHandlerSuite.java` (add tests)

### Phase 3: Heartbeat Integration
**Goal**: Send executor stats to driver via heartbeats

**Tasks**:
1. Add `serverShuffleStats: Option[ServerShuffleFetchStats]` to `Heartbeat` case class
2. Modify `Executor.reportHeartBeat()` to collect and include server stats
3. Modify `HeartbeatReceiver.receiveAndReply()` to extract and forward stats
4. Modify `TaskScheduler.executorHeartbeatReceived()` signature to accept stats
5. Modify `TaskSchedulerImpl` to pass stats to DAGScheduler
6. Update serialization (JSON, Protobuf) for new heartbeat field
7. Update tests for heartbeat flow
8. **Commit**: "Send server shuffle stats via executor heartbeats"

**Files**:
- `core/src/main/scala/org/apache/spark/HeartbeatReceiver.scala` (modify)
- `core/src/main/scala/org/apache/spark/executor/Executor.scala` (modify)
- `core/src/main/scala/org/apache/spark/scheduler/TaskScheduler.scala` (modify)
- `core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala` (modify)
- `core/src/main/scala/org/apache/spark/util/JsonProtocol.scala` (modify)
- `core/src/main/resources/org/apache/spark/status/protobuf/store_types.proto` (modify)
- `core/src/main/scala/org/apache/spark/status/protobuf/StoreTypes.scala` (modify)
- `core/src/test/scala/org/apache/spark/HeartbeatReceiverSuite.scala` (modify)

### Phase 4: Driver-Side Aggregation and Storage
**Goal**: Aggregate stats at driver, maintain top-K

**Tasks**:
1. Add `serverStatsAggregator` to `DAGScheduler`
2. Create method `handleServerShuffleStats()` to merge incoming stats
3. Hook into executor removal to clean up stats
4. Store aggregator state (consider persistence for UI later)
5. Write integration tests
6. **Commit**: "Aggregate server shuffle stats at driver"

**Files**:
- `core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala` (modify)
- `core/src/test/scala/org/apache/spark/scheduler/DAGSchedulerSuite.scala` (modify)

### Phase 5: Stage Completion Logging
**Goal**: Log top-K slow executors after stage finishes

**Tasks**:
1. Modify `DAGScheduler.markStageAsFinished()` to log server stats
2. Format output with breakdown percentages
3. Add optional periodic logging (similar to `fetch-wait-dist`)
4. Add end-to-end test with mock executors
5. Update documentation
6. **Commit**: "Log top-K slow server executors on stage completion"

**Files**:
- `core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala` (modify)
- `core/src/test/scala/org/apache/spark/scheduler/DAGSchedulerSuite.scala` (modify)
- `docs/monitoring.md` (update)

## Configuration

```scala
// Enable server-side shuffle fetch statistics
spark.shuffle.serverFetchStats.enabled = false  // default: disabled

// Number of top executors to track and report
spark.shuffle.serverFetchStats.topK = 3  // default: 3

// Optional: periodic logging interval (in addition to stage completion)
spark.shuffle.serverFetchStats.logInterval = 30s  // default: none
```

## Example Output

```
24/12/02 10:30:45 INFO DAGScheduler: Top shuffle fetch server executors (after stage 5):
  executor-3: total=15.2s (n=1024, avg=14.8ms, max=45ms)
    disk: 52% (avg=7.7ms, max=12ms)
    send: 31% (avg=4.6ms, max=8ms)
    queue wait: 8% (avg=1.2ms, max=2ms, ratio=1.67)
    queue length: max=2, avg=0.8
    ✅ Validation: 1.2ms ≈ 0.8 × 14.8ms = 11.8ms? (check!) → actual much lower

  executor-7: total=8.7s (n=512, avg=17.0ms, max=38ms)
    disk: 71% (avg=12.1ms, max=18ms)
    send: 22% (avg=3.7ms, max=6ms)
    queue wait: 5% (avg=0.9ms, max=1.5ms, ratio=1.67)
    queue length: max=1, avg=0.5
    ✅ Validation: 0.9ms ≈ 0.5 × 17.0ms = 8.5ms? → actual much lower (async)

  executor-12: total=6.4s (n=256, avg=25.0ms, max=2089ms)  ← spike!
    disk: 38% (avg=9.5ms, max=2000ms)  ← disk spike!
    send: 48% (avg=12.0ms, max=15ms)
    queue wait: 10% (avg=2.5ms, max=50ms, ratio=20.0)
    queue length: max=120, avg=12, ratio=10.0
    ⚠️  Validation: 2.5ms vs 12 × 25ms = 300ms → mismatch! (Netty async handling)
```

**Breakdown interpretation**:
- `total`: Sum of all request latencies (indicates overall contribution to shuffle time)
- `n`: Number of requests served
- `avg`: Average latency per request
- `max`: Maximum single request latency (identifies spikes)
- `disk%`: Percentage of time spent on disk I/O
  - `avg`: Average disk read time per request
  - `max`: Maximum disk read time (detects I/O spikes)
- `send%`: Percentage of time spent on network send
  - `avg`: Average network send time per request
  - `max`: Maximum network send time (detects network spikes)
- `queue wait`: Time requests spend waiting in queue before processing
  - `%`: Percentage of total latency spent waiting
  - `avg`: Average queue wait time per request
  - `max`: Maximum queue wait time (detects queueing delays)
  - `ratio`: Imbalance ratio (max/avg) - identifies queueing spikes
- `queue length`: Number of requests waiting in queue (not started processing)
  - `max`: Peak number of waiting requests
  - `avg`: Average number of waiting requests
  - `ratio`: Imbalance ratio (max/avg) - identifies queue depth spikes
- `Validation`: Check `queue wait time ≈ queue length × avg processing time`
  - ✅ Match: Metrics consistent, formula holds
  - ⚠️ Mismatch: May indicate async processing, measurement issues, or non-uniform request times

**Example analysis**:
- **executor-3 & executor-7**: Low queue length (avg<1), minimal wait time → thread pool has capacity
- **executor-12**: High queue length spikes (max=120, avg=12) but low actual wait time (avg=2.5ms) →
  - Validation mismatch (2.5ms vs expected 300ms) suggests Netty async handling
  - Queue length measures pending tasks, but wait time is much lower due to async dispatch

## Testing Strategy

### Unit Tests
1. **ServerShuffleFetchStatsAggregatorSuite**:
   - Merge stats from multiple executors
   - Top-K selection logic
   - Executor removal handling
   - Edge cases (empty stats, zero requests)

2. **ShuffleFetchMetricsSuite**:
   - Metrics collection and aggregation
   - Incremental stats retrieval
   - Thread safety

### Integration Tests
3. **HeartbeatReceiverSuite**:
   - Heartbeat with server stats
   - Stats forwarding to scheduler

4. **DAGSchedulerSuite**:
   - Stats aggregation across heartbeats
   - Stage completion logging
   - Executor loss handling

### End-to-End Tests
5. **ServerShuffleFetchStatsE2ESuite** (new):
   - Simulate shuffle with mock executors
   - Verify stats collection and reporting
   - Test with different topK values
   - Verify output format

## Serialization

### JSON (Event Log)
```json
{
  "Event": "SparkListenerExecutorMetricsUpdate",
  "Executor ID": "3",
  "Server Shuffle Stats": {
    "Request Count": 1024,
    "Total Latency Sum": 15200,
    "Total Latency Max": 45,
    "Disk Read Latency Sum": 7904,
    "Disk Read Latency Max": 12,
    "Send Latency Sum": 4712,
    "Send Latency Max": 8,
    "Queue Wait Time Sum": 1228,
    "Queue Wait Time Max": 2,
    "Max Queue Length": 2,
    "Avg Queue Length": 0.8
  }
}
```

### Protobuf (History Server)
```protobuf
message ServerShuffleFetchStats {
  optional string executor_id = 1;
  optional int64 request_count = 2;
  optional int64 total_latency_sum = 3;
  optional int64 total_latency_max = 4;
  optional int64 disk_read_latency_sum = 5;
  optional int64 disk_read_latency_max = 6;
  optional int64 send_latency_sum = 7;
  optional int64 send_latency_max = 8;
  optional int64 queue_wait_time_sum = 9;
  optional int64 queue_wait_time_max = 10;
  optional int32 max_queue_length = 11;
  optional double avg_queue_length = 12;
}

message Heartbeat {
  // ... existing fields ...
  optional ServerShuffleFetchStats server_shuffle_stats = 15;
}
```

## Performance Considerations

### Memory Overhead
- **Executor**: O(1) - single aggregate object per executor
- **Driver**: O(N) where N = number of executors (typically 10s-100s)
- **Heartbeat payload**: ~66 bytes per executor (9 fields: 7 longs + 1 int + string ID)

### CPU Overhead
- **Executor**: Minimal - simple counter updates on existing code paths
- **Driver**: O(N log K) per heartbeat for top-K maintenance (very fast with N < 1000)

### When Disabled
- Zero overhead: all code paths short-circuit on config check
- No memory allocation, no computation

## Alternatives Considered

### Alternative 1: Track Per-Client Instead of Per-Server
**Approach**: Each server tracks latency per requesting client executor

**Pros**:
- Could identify "executor A is slow when serving executor B"
- More granular data

**Cons**:
- Much higher memory overhead: O(N²) at driver
- Harder to interpret: "why is A slow serving B but fast serving C?"
- Doesn't directly answer "which servers are bottlenecks?"

**Decision**: Rejected - per-server self-tracking is simpler and more actionable

### Alternative 2: Use Metrics System Instead of Heartbeats
**Approach**: Expose via Dropwizard, poll from driver or external system

**Pros**:
- Reuses existing metrics infrastructure
- No heartbeat changes needed

**Cons**:
- Can't correlate with stage completion (metrics are always-on)
- Requires external polling system
- No integration with event log or History Server
- Misses opportunity to tie metrics to specific stages

**Decision**: Rejected - heartbeat integration provides better stage correlation

### Alternative 3: Include Distribution (Percentiles)
**Approach**: Track p50, p95, p99 like `fetch-wait-dist` branch

**Pros**:
- Richer statistics
- Better understanding of latency distribution

**Cons**:
- Higher memory overhead (quantile sketches)
- More complex serialization
- Incremental percentile merging is approximate and complex

**Decision**: Deferred - start with simple aggregates (sum/max/avg), add percentiles later if needed

## Future Enhancements

1. **UI Integration**: Display server stats in Spark UI stage details
2. **Percentile Tracking**: Add p50/p95/p99 using quantile sketches
3. **Per-Shuffle Breakdown**: Show which shuffles contribute to executor slowness
4. **Historical Trending**: Track executor performance across stages
5. **Automatic Diagnosis**: Suggest mitigations based on breakdown (e.g., "increase disk bandwidth")

## Success Criteria

1. **Functional**:
   - ✅ Collects server-side metrics per executor
   - ✅ Sends via heartbeats to driver
   - ✅ Logs top-K executors after stage completion
   - ✅ Shows latency breakdown (disk, send, queue)

2. **Quality**:
   - ✅ All tests pass
   - ✅ No scalastyle violations
   - ✅ Zero overhead when disabled
   - ✅ Thread-safe aggregation

3. **Usability**:
   - ✅ Clear, actionable log output
   - ✅ Simple configuration
   - ✅ Documented in monitoring guide

## References

- **Existing Work**: `fetch-wait-dist` branch (commit 3eb86e4d874) - client-side distribution tracking
- **Current Work**: `fetch-metrics` branch - server-side per-shuffle metrics
- **Related JIRA**: (to be created)
