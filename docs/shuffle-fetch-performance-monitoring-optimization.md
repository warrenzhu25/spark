# Shuffle Fetch Performance: Monitoring and Optimization

## Overview

This document presents a comprehensive approach to shuffle fetch performance in Apache Spark, covering both **real-time optimization** and **post-hoc observability**. Both capabilities leverage shared server-side infrastructure to provide actionable insights and performance improvements.

### Two Complementary Capabilities

**1. Real-Time Optimization (Client-Side Adaptive Scheduling)**
- **When**: During job execution, in real-time
- **Goal**: Make shuffle fetching faster by avoiding busy servers
- **How**: Server sends immediate wait time notifications; client adapts fetch strategy
- **Use Case**: Reduce shuffle time for running jobs

**2. Post-Hoc Observability (Server-Side Performance Reporting)**
- **When**: After stage completion, for analysis
- **Goal**: Identify which server executors are slow (and why)
- **How**: Aggregate server-side metrics, report top-K slow executors with breakdown
- **Use Case**: Debug shuffle bottlenecks, tune cluster configuration

### Why Both Matter

```
During job execution (Real-Time):
  Client: "Server executor-5 estimates 500ms wait, let me fetch from executor-3 instead"
  → Job runs faster through adaptive scheduling

After stage completion (Post-Hoc):
  Driver logs: "executor-5 was slow: 60% disk I/O, 30% queue wait"
  → Understand root cause, add more disk bandwidth or reduce load
```

### Shared Infrastructure

Both capabilities leverage the same underlying infrastructure:

```
┌─────────────────────────────────────────────────────────────┐
│ Server Executor (shared components)                         │
│                                                              │
│  Netty EventLoop Queue Metrics (pendingTasks)               │
│  ├─ Queue length: Number of waiting requests                │
│  └─ Sampled periodically within EventLoop (100ms)           │
│                                                              │
│  Codahale Metrics (Timers and Counters)                     │
│  ├─ chunkFetchLatencyMillis: Total processing time          │
│  ├─ chunkReadLatencyMillis: Disk I/O time                   │
│  ├─ responseSendLatencyMillis: Network send time            │
│  ├─ queueWaitTimeMillis: Time waiting in queue (NEW)        │
│  └─ queueLengthHistogram: Sampled queue length (NEW)        │
│                                                              │
│  Used by BOTH:                                              │
│  ├─ Real-time: Calculate estimated wait, send to client     │
│  └─ Post-hoc: Snapshot at heartbeat, send to driver         │
└─────────────────────────────────────────────────────────────┘
```

---

# Part I: Real-Time Optimization

## Problem Statement

Currently, Spark's shuffle fetch mechanism uses client-side throttling (`maxBytesInFlight`, `maxReqsInFlight`) without visibility into server-side load. This leads to:

- Suboptimal request distribution (many requests to busy servers, few to idle ones)
- Unnecessary waiting when executors could fetch from other sources
- Inefficient use of network and I/O resources

## Proposed Solution: Immediate Wait Time Notifications

Enable shuffle servers to send **immediate wait time notifications** when fetch requests arrive:

**Server-Side**:
1. When request arrives, immediately calculate estimated wait time:
   - `estimated_wait = queue_length × avg_processing_time`
2. Send `WaitTimeNotification` message to client (non-blocking)
3. Queue request for processing

**Client-Side**:
1. Receive wait time estimate asynchronously
2. If wait time is high, proactively fetch from other executors
3. Reorder pending requests to prioritize less-loaded servers

### Timeline Comparison

**Traditional** (no optimization):
```
Client → Server: Request
[Client waits... server processing... 500ms]
Client ← Server: Response + Data
```

**With Optimization**:
```
Client → Server: Request
Client ← Server: WaitTimeNotification (500ms) ⚡ [immediate!]
Client → Other Servers: Additional requests [parallel fetching]
[200ms later]
Client ← Server + Others: Multiple responses [reduced total time]
```

## Implementation: Real-Time Optimization

### Server-Side: Wait Time Estimator

```java
public class NettyBasedWaitTimeEstimator {
  private final EventLoopGroup chunkFetchWorkers;
  private final Timer requestLatencyTimer;
  private final Histogram queueLengthHistogram;

  // Periodically sample queue length within EventLoop
  public void initialize() {
    chunkFetchWorkers.forEach(eventLoop -> {
      eventLoop.scheduleAtFixedRate(() -> {
        int pending = eventLoop.pendingTasks();  // Cheap from within loop
        queueLengthHistogram.update(pending);
      }, 0, 100, TimeUnit.MILLISECONDS);
    });
  }

  // Estimate wait time for incoming request
  public long estimateWaitTimeMs() {
    Snapshot queueSnap = queueLengthHistogram.getSnapshot();
    Snapshot latencySnap = requestLatencyTimer.getSnapshot();

    double avgQueueLength = queueSnap.getMean();
    double avgProcessingTime = latencySnap.getMean();

    return (long) (avgQueueLength * avgProcessingTime);
  }
}
```

### Server-Side: Send Notification

```java
// In ChunkFetchRequestHandler.channelRead0()
@Override
protected void channelRead0(ChannelHandlerContext ctx, ChunkFetchRequest msg) {
  // Send immediate wait time notification
  if (waitTimeNotificationEnabled && waitTimeEstimator != null) {
    long estimatedWait = waitTimeEstimator.estimateWaitTimeMs();

    WaitTimeNotification notification = new WaitTimeNotification(
      msg.streamChunkId.streamId(),
      estimatedWait
    );

    // Non-blocking send
    ctx.channel().writeAndFlush(notification);
  }

  // Continue with normal processing
  processFetchRequest(ctx.channel(), msg);
}
```

### Client-Side: Adaptive Scheduling

```scala
// In ShuffleBlockFetcherIterator
private def handleWaitTimeNotification(notification: WaitTimeNotification): Unit = {
  val estimatedWait = notification.estimatedWaitTimeMs
  val threshold = conf.get(SHUFFLE_FETCH_WAIT_TIME_THRESHOLD)

  if (estimatedWait > threshold) {
    logInfo(s"Server reports high wait time ($estimatedWait ms), " +
            s"issuing additional requests to other servers")

    // Proactively fetch from other executors
    sendNextRequests(numRequestsToIssue = 2)
  }
}
```

### Configuration

```scala
// Enable wait time notifications (server-side)
spark.shuffle.waitTimeNotification.enabled = false  // default

// Client-side threshold for adaptive scheduling
spark.shuffle.adaptiveFetch.enabled = false  // default
spark.shuffle.adaptiveFetch.waitTimeThreshold = 100ms  // default
```

---

# Part II: Post-Hoc Observability

## Problem Statement

When shuffle operations are slow, we need to identify **which executor servers are bottlenecks and WHY**. Current metrics don't provide:
- Per-executor server-side performance breakdown
- Root cause analysis (disk I/O vs network vs queueing)
- Top-K ranking after stage completion

## Proposed Solution: Server-Side Performance Reporting

Aggregate server-side metrics per executor and report top-K slow executors after stage completion:

**At Each Executor**:
1. Collect metrics: disk I/O, network send, queue wait, queue length
2. At heartbeat time, snapshot metrics and send to driver

**At Driver**:
1. Aggregate metrics across executors
2. After stage completion, identify top-K slowest executors
3. Log with actionable breakdown

### Example Output

```
24/12/02 10:30:45 INFO DAGScheduler: Top shuffle fetch server executors (after stage 5):
  executor-3: total=15.2s (n=1024, avg=14.8ms, max=45ms)
    disk: 52% (avg=7.7ms, max=12ms)
    send: 31% (avg=4.6ms, max=8ms)
    queue wait: 8% (avg=1.2ms, max=2ms, ratio=1.67)
    queue length: max=2, avg=0.8
    ✅ Validation: 1.2ms ≈ 0.8 × 14.8ms ≈ 11.8ms

  executor-12: total=6.4s (n=256, avg=25.0ms, max=2089ms)  ← spike!
    disk: 38% (avg=9.5ms, max=2000ms)  ← disk spike!
    send: 48% (avg=12.0ms, max=15ms)
    queue wait: 10% (avg=2.5ms, max=50ms, ratio=20.0)
    queue length: max=120, avg=12, ratio=10.0
    ⚠️  Validation: 2.5ms vs 12 × 25ms = 300ms (Netty async)
```

## Data Model

```scala
/**
 * Server-side shuffle fetch statistics (executor → driver via heartbeat)
 */
case class ServerShuffleFetchStats(
  executorId: String,
  requestCount: Long,

  // Total latency
  totalLatencySum: Long,
  totalLatencyMax: Long,

  // Breakdown metrics (sum and max for spike detection)
  diskReadLatencySum: Long,
  diskReadLatencyMax: Long,
  sendLatencySum: Long,
  sendLatencyMax: Long,

  // Queue metrics (wait time + length for validation)
  queueWaitTimeSum: Long,
  queueWaitTimeMax: Long,
  maxQueueLength: Int,
  avgQueueLength: Double
)

/**
 * Aggregated statistics at driver (cumulative across heartbeats)
 */
case class ServerShuffleFetchAggregate(
  executorId: String,
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
  // Derived metrics
  def avgLatency: Double =
    if (totalRequests > 0) totalLatencySum.toDouble / totalRequests else 0.0

  def diskReadPercent: Double =
    if (totalLatencySum > 0) (diskReadLatencySum.toDouble / totalLatencySum) * 100 else 0.0

  def queueWaitPercent: Double =
    if (totalLatencySum > 0) (queueWaitTimeSum.toDouble / totalLatencySum) * 100 else 0.0

  // Validation: queue wait time ≈ queue length × avg processing time
  def expectedQueueWaitTime: Double = avgQueueLength * avgLatency
  def queueWaitValidation: Double =
    if (expectedQueueWaitTime > 0) avgQueueWaitTime / expectedQueueWaitTime else 0.0
}
```

## Implementation: Post-Hoc Observability

### Executor-Side: Collect and Snapshot

```scala
// In Executor.reportHeartBeat()
def getServerShuffleStats(): Option[ServerShuffleFetchStats] = {
  if (!conf.get(SHUFFLE_SERVER_FETCH_STATS_ENABLED)) return None

  val handler = blockManager.externalBlockHandler
  val metrics = handler.getShuffleMetrics()

  // Snapshot all metrics
  val fetchSnap = metrics.chunkFetchLatencyMillis.getSnapshot()
  val diskSnap = metrics.chunkReadLatencyMillis.getSnapshot()
  val sendSnap = metrics.responseSendLatencyMillis.getSnapshot()
  val queueWaitSnap = metrics.queueWaitTimeMillis.getSnapshot()
  val queueLengthSnap = metrics.queueLengthHistogram.getSnapshot()

  val count = metrics.chunkFetchLatencyMillis.getCount()

  Some(ServerShuffleFetchStats(
    executorId = executorId,
    requestCount = count,
    totalLatencySum = (fetchSnap.getMean() * count).toLong,
    totalLatencyMax = fetchSnap.getMax(),
    diskReadLatencySum = (diskSnap.getMean() * count).toLong,
    diskReadLatencyMax = diskSnap.getMax(),
    sendLatencySum = (sendSnap.getMean() * count).toLong,
    sendLatencyMax = sendSnap.getMax(),
    queueWaitTimeSum = (queueWaitSnap.getMean() * count).toLong,
    queueWaitTimeMax = queueWaitSnap.getMax(),
    maxQueueLength = queueLengthSnap.getMax().toInt,
    avgQueueLength = queueLengthSnap.getMean()
  ))
}
```

### Driver-Side: Aggregate and Report

```scala
// In DAGScheduler
private val serverStatsAggregator =
  new ServerShuffleFetchStatsAggregator(topK = 3)

// On heartbeat
def handleServerShuffleStats(stats: ServerShuffleFetchStats): Unit = {
  serverStatsAggregator.mergeStats(stats)
}

// On stage completion
private def logServerShuffleStats(stageId: Int): Unit = {
  val topExecutors = serverStatsAggregator.getTopK()

  if (topExecutors.nonEmpty) {
    logInfo(s"Top shuffle fetch server executors (after stage $stageId):")
    topExecutors.foreach { agg =>
      logInfo(f"  ${agg.executorId}: total=${formatDuration(agg.totalLatencySum)} " +
              f"(n=${agg.totalRequests}, avg=${formatDuration(agg.avgLatency)}, " +
              f"max=${formatDuration(agg.totalLatencyMax)})")
      logInfo(f"    disk: ${agg.diskReadPercent}%.0f%% " +
              f"(avg=${formatDuration(agg.avgDiskReadLatency)}, " +
              f"max=${formatDuration(agg.diskReadLatencyMax)})")
      logInfo(f"    queue wait: ${agg.queueWaitPercent}%.0f%% " +
              f"(avg=${formatDuration(agg.avgQueueWaitTime)}, " +
              f"max=${formatDuration(agg.queueWaitTimeMax)})")
      logInfo(f"    queue length: max=${agg.maxQueueLength}, " +
              f"avg=${agg.avgQueueLength}%.1f")

      // Validation
      val expected = agg.expectedQueueWaitTime
      val actual = agg.avgQueueWaitTime
      if (Math.abs(actual - expected) / expected > 0.5) {
        logInfo(f"    ⚠️  Validation: ${actual}ms vs expected ${expected}ms")
      }
    }
  }

  serverStatsAggregator.reset()
}
```

### Configuration

```scala
// Enable server-side stats reporting
spark.shuffle.serverFetchStats.enabled = false  // default

// Number of top executors to report
spark.shuffle.serverFetchStats.topK = 3  // default
```

---

# Shared Implementation

## Netty Queue Metrics (Used by Both)

Both real-time optimization and post-hoc observability rely on the same queue metrics:

```java
// In ExternalBlockHandler.ShuffleMetrics
public class ShuffleMetrics implements MetricSet {
  // Existing metrics from commit b774aecc346
  private final Timer chunkFetchLatencyMillis = new Timer();
  private final Timer chunkReadLatencyMillis = new Timer();
  private final Timer responseSendLatencyMillis = new Timer();

  // NEW: Queue wait time (arrival to processing start)
  private final Timer queueWaitTimeMillis = new Timer();

  // NEW: Queue length sampling (pendingTasks)
  private final Histogram queueLengthHistogram = new Histogram(new UniformReservoir());

  // Initialize queue length sampling
  public void initializeQueueLengthSampling(EventLoopGroup chunkFetchWorkers) {
    chunkFetchWorkers.forEach(eventLoop -> {
      eventLoop.scheduleAtFixedRate(() -> {
        int pending = eventLoop.pendingTasks();
        queueLengthHistogram.update(pending);
      }, 0, 100, TimeUnit.MILLISECONDS);
    });
  }
}
```

## Queue Wait Time Measurement

**Approach**: Message timestamping for accurate measurement

Queue wait time is measured from when the message is decoded (ready to process) to when
the EventLoop actually processes it (channelRead0 invoked). This captures the time spent
waiting in the Netty EventLoop's task queue.

```java
// Step 1: Add transient timestamp field to ChunkFetchRequest
public final class ChunkFetchRequest extends AbstractMessage implements RequestMessage {
  public final StreamChunkId streamChunkId;

  // Transient field (not serialized) - set server-side after decode
  private transient long arrivalTimeNanos;

  public void setArrivalTimeNanos(long nanos) {
    this.arrivalTimeNanos = nanos;
  }

  public long getArrivalTimeNanos() {
    return arrivalTimeNanos;
  }
}

// Step 2: Timestamp message immediately after decoding
// In MessageDecoder (or where ChunkFetchRequest.decode is called)
ChunkFetchRequest request = ChunkFetchRequest.decode(buf);
request.setArrivalTimeNanos(System.nanoTime());  // Timestamp before queueing

// Step 3: Measure queue wait time when processing starts
// In ChunkFetchRequestHandler.channelRead0()
@Override
protected void channelRead0(ChannelHandlerContext ctx, ChunkFetchRequest msg) {
  final long processingStartNanos = System.nanoTime();
  final long queueWaitNanos = processingStartNanos - msg.getArrivalTimeNanos();

  // Record queue wait time for post-hoc observability
  if (queueWaitTimeMillis != null) {
    queueWaitTimeMillis.update(queueWaitNanos, TimeUnit.NANOSECONDS);
  }

  // ... wait time notification (for real-time optimization) ...

  // Continue processing
  processFetchRequest(ctx.channel(), msg);
}
```

**Why this works**: The timestamp is captured immediately after the message is decoded,
before it enters the EventLoop's task queue. When channelRead0() is invoked, we measure
how long the message waited in the queue.

## Validation Formula

Both capabilities use the same validation:

```
queue_wait_time ≈ queue_length × avg_processing_time
```

- **Real-time**: Estimate future wait time for incoming requests
- **Post-hoc**: Validate measurement consistency and identify async behavior

---

# Combined Implementation Plan

## Phase 1: Shared Infrastructure

**Goal**: Implement queue metrics used by both capabilities

**Tasks**:
1. Add transient `arrivalTimeNanos` field to `ChunkFetchRequest` with getter/setter
2. Modify message decoder to timestamp `ChunkFetchRequest` after decoding
3. Add `Timer queueWaitTimeMillis` to `ShuffleFetchMetrics` and `ExternalBlockHandler.ShuffleMetrics`
4. Add `Histogram queueLengthHistogram` to `ShuffleFetchMetrics` and `ExternalBlockHandler.ShuffleMetrics`
5. Update `ChunkFetchRequestHandler` constructor to accept new metrics
6. Modify `ChunkFetchRequestHandler.channelRead0()` to measure queue wait time from arrival timestamp
7. Implement `initializeQueueLengthSampling()` in `ExternalBlockHandler` with EventLoop periodic sampling
8. Add configs for both features (disabled by default)
9. Write tests for queue metrics

**Commit**: "Add queue wait time and length metrics for shuffle fetch"

## Phase 2: Real-Time Optimization

**Goal**: Enable immediate wait time notifications for client-side adaptive scheduling

**Tasks**:
1. Create `NettyBasedWaitTimeEstimator` class
2. Create `WaitTimeNotification` protocol message
3. Modify `ChunkFetchRequestHandler` to send notifications
4. Modify `ShuffleBlockFetcherIterator` to handle notifications
5. Implement client-side adaptive fetch logic
6. Write tests for notification flow

**Commit**: "Add immediate wait time notifications for adaptive shuffle fetch"

## Phase 3: Post-Hoc Observability

**Goal**: Enable server-side performance reporting after stage completion

**Tasks**:
1. Create `ServerShuffleFetchStats` and `ServerShuffleFetchAggregate` data model
2. Create `ServerShuffleFetchStatsAggregator` for driver-side aggregation
3. Add `serverShuffleStats` field to `Heartbeat` message
4. Modify `Executor.reportHeartBeat()` to snapshot and send metrics
5. Modify `DAGScheduler` to aggregate stats and log on stage completion
6. Update serialization (JSON, Protobuf)
7. Write tests for aggregation and reporting

**Commit**: "Add server-side shuffle fetch performance reporting"

## Phase 4: Integration and Testing

**Goal**: End-to-end testing and documentation

**Tasks**:
1. Integration tests with both features enabled
2. Performance benchmarks
3. Documentation updates
4. Example configurations for different workloads

**Commit**: "Integration tests and documentation for shuffle performance features"

---

# Configuration Reference

```scala
// ===== Shared Infrastructure =====

// Enable dedicated thread pool for chunk fetch (prerequisite for both)
spark.shuffle.server.chunkFetchHandlerThreadsPercent = 0  // default: disabled


// ===== Real-Time Optimization =====

// Enable wait time notifications (server-side)
spark.shuffle.waitTimeNotification.enabled = false  // default

// Enable client-side adaptive scheduling
spark.shuffle.adaptiveFetch.enabled = false  // default

// Threshold for triggering adaptive behavior
spark.shuffle.adaptiveFetch.waitTimeThreshold = 100ms  // default


// ===== Post-Hoc Observability =====

// Enable server-side stats collection and reporting
spark.shuffle.serverFetchStats.enabled = false  // default

// Number of top slow executors to report
spark.shuffle.serverFetchStats.topK = 3  // default

// Optional: periodic logging during stage execution
spark.shuffle.serverFetchStats.logInterval = 30s  // default: none (only stage end)
```

---

# Benefits Summary

## Real-Time Optimization
- ✅ Reduced shuffle time through adaptive fetch scheduling
- ✅ Better resource utilization (avoid waiting on busy servers)
- ✅ Automatic load balancing across executors

## Post-Hoc Observability
- ✅ Identify slow server executors after stage completion
- ✅ Root cause analysis with latency breakdown
- ✅ Validation formula to detect measurement issues
- ✅ Actionable insights for tuning

## Shared Benefits
- ✅ Single infrastructure implementation serves both purposes
- ✅ Minimal overhead (leverages existing Codahale metrics)
- ✅ Config-gated: zero overhead when disabled
- ✅ Production-ready: builds on proven Netty and Spark patterns

---

# Testing Strategy

## Unit Tests
1. **NettyBasedWaitTimeEstimator**: Queue length sampling, wait time calculation
2. **ServerShuffleFetchStatsAggregator**: Merging, top-K selection, validation
3. **Queue metrics**: Timer and Histogram accuracy

## Integration Tests
1. **Real-time**: Client receives notifications, triggers adaptive fetching
2. **Post-hoc**: Metrics flow through heartbeat, aggregated at driver, logged correctly
3. **Combined**: Both features enabled, no interference

## Performance Tests
1. Overhead measurement with features enabled/disabled
2. Shuffle performance improvement with real-time optimization
3. Memory footprint of metric collection

---

# References

- Existing Work: Commit b774aecc346 - Server-side timing and queue depth metrics
- Netty Monitoring: Commit 9c40b5b702e - EventLoop pendingTasks() sampling design
- Client-side Metrics: Commit 3eb86e4d874 - Client-side wait time distribution tracking (fetch-wait-dist branch)
