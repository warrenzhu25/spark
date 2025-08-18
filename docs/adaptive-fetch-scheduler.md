# Plan: Adaptive Shuffle Fetch Scheduler with Bottleneck Optimization

## Overview

Design and implement an adaptive fetch scheduler that minimizes total shuffle wait time by:
1. Identifying the bottleneck executor (executor with most shuffle data stored)
2. Using priority-based scheduling to keep the bottleneck executor maximally utilized
3. Pipelining multiple requests to the bottleneck
4. Intelligently interleaving fetches from non-bottleneck executors

## Problem Analysis

### Current Limitations
- **FIFO Scheduling**: ShuffleBlockFetcherIterator uses simple FIFO queue for fetch requests
- **Conservative Pipelining**: Default `maxBlocksInFlightPerAddress = 1` limits concurrency per executor
- **No Bottleneck Awareness**: All executors treated equally, even when one stores most shuffle data
- **Suboptimal Total Wait Time**: Bottleneck executor may be idle while fetching from others

### Key Insight
The executor with the most shuffle data is the critical path bottleneck. If it's idle while we fetch from other executors, we're not minimizing total shuffle wait time. We should keep it maximally busy and interleave other fetches opportunistically.

## Proposed Architecture

### 1. Bottleneck Identification

**When**: During ShuffleBlockFetcherIterator initialization (before any fetching)

**How**: Aggregate total shuffle data by executor from MapOutputTracker

**Implementation** (in `ShuffleBlockFetcherIterator.initialize()`):

```scala
// Calculate total data per executor
val executorDataSizes = mutable.HashMap[BlockManagerId, Long]()
for ((address, blockInfos) <- blocksByAddress) {
  val totalBytes = blockInfos.map(_._2).sum  // Sum block sizes
  executorDataSizes(address) = totalBytes
}

// Identify bottleneck (executor with most data)
val bottleneckExecutor = executorDataSizes.maxByOption(_._2).map(_._1)
val bottleneckDataSize = bottleneckExecutor.map(executorDataSizes(_)).getOrElse(0L)

logInfo(s"Identified bottleneck executor: $bottleneckExecutor with ${bottleneckDataSize / 1024 / 1024} MB")
```

**Storage**: Add fields to ShuffleBlockFetcherIterator:
- `bottleneckExecutor: Option[BlockManagerId]`
- `executorDataSizes: Map[BlockManagerId, Long]`

### 2. Priority Queue Design

**Replace**: `fetchRequests: Queue[FetchRequest]` → `fetchRequests: mutable.PriorityQueue[FetchRequest]`

**Priority Function**:
```scala
case class PrioritizedFetchRequest(
  request: FetchRequest,
  priority: Double
) extends Ordered[PrioritizedFetchRequest] {
  def compare(that: PrioritizedFetchRequest): Int =
    that.priority.compare(this.priority)  // Higher priority first
}

def calculatePriority(request: FetchRequest): Double = {
  val isBottleneck = bottleneckExecutor.contains(request.address)
  val executorDataRatio = executorDataSizes(request.address).toDouble /
                          executorDataSizes.values.sum
  val requestSize = request.size

  // Priority components:
  val bottleneckBonus = if (isBottleneck) 100.0 else 0.0
  val dataSizeWeight = executorDataRatio * 50.0
  val batchSizeWeight = (requestSize / targetRemoteRequestSize) * 10.0

  bottleneckBonus + dataSizeWeight + batchSizeWeight
}
```

**Priority Factors**:
1. **Bottleneck bonus (100 points)**: Highest priority to bottleneck executor
2. **Data size weight (0-50 points)**: Proportional to executor's total data
3. **Batch size weight (0-10 points)**: Prefer larger batches for efficiency

### 3. Enhanced Pipelining Strategy

**Problem**: Default `maxBlocksInFlightPerAddress = 1` prevents pipelining to bottleneck

**Solution**: Dynamic per-executor concurrency limits

**Add Configuration**:
```scala
// Config in internal/config/package.scala
val REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS_BOTTLENECK =
  ConfigBuilder("spark.reducer.maxBlocksInFlightPerAddressBottleneck")
    .doc("Max concurrent requests to bottleneck executor (default 3)")
    .intConf
    .createWithDefault(3)
```

**Implementation**:
```scala
def getMaxBlocksForAddress(address: BlockManagerId): Int = {
  if (bottleneckExecutor.contains(address)) {
    conf.get(REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS_BOTTLENECK)
  } else {
    maxBlocksInFlightPerAddress  // Default 1
  }
}

def isRemoteAddressMaxedOut(address: BlockManagerId): Boolean = {
  val limit = getMaxBlocksForAddress(address)
  numBlocksInFlightPerAddress.getOrElse(address, 0) >= limit
}
```

### 4. Smart Interleaving Algorithm

**Goal**: Keep bottleneck executor busy while opportunistically fetching from others

**Algorithm** (in `fetchUpToMaxBytes()`):

```scala
def fetchUpToMaxBytes(): Unit = {
  // Priority 1: Send deferred requests first (existing logic)
  processDeferredRequests()

  // Priority 2: Keep bottleneck executor at max concurrency
  while (canSendToBottleneck && fetchRequests.nonEmpty) {
    val nextBottleneckRequest = fetchRequests.dequeueAll(_.request.address == bottleneckExecutor.get).headOption
    nextBottleneckRequest.foreach(sendRequest)
  }

  // Priority 3: Interleave non-bottleneck requests
  while (canSendRequest && fetchRequests.nonEmpty) {
    val nextRequest = fetchRequests.dequeue()  // Highest priority

    if (isRemoteBlockFetchable(nextRequest.request) &&
        !isRemoteAddressMaxedOut(nextRequest.request.address)) {
      sendRequest(nextRequest.request)
    } else {
      // Defer if constraints not met
      deferredFetchRequests.getOrElseUpdate(nextRequest.request.address,
        new Queue[FetchRequest]()) += nextRequest.request
    }
  }
}

def canSendToBottleneck: Boolean = {
  bottleneckExecutor.exists { executor =>
    numBlocksInFlightPerAddress.getOrElse(executor, 0) <
      getMaxBlocksForAddress(executor) &&
    bytesInFlight < maxBytesInFlight &&
    reqsInFlight < maxReqsInFlight
  }
}
```

**Interleaving Strategy**:
1. **First**: Ensure bottleneck executor has max requests in flight (3 by default)
2. **Then**: Fill remaining capacity with highest priority non-bottleneck requests
3. **Continuously**: As requests complete, prioritize refilling bottleneck queue

### 5. Wait Time Aware Scheduling (Integration with Metrics)

**Use timing metrics from previous implementation**:
- processFetchRequestLatencyMillis (total processing time)
- Executor load metrics from global coordination

**Enhanced Priority Function**:
```scala
def calculatePriorityWithWaitTime(request: FetchRequest): Double = {
  val basePriority = calculatePriority(request)

  // Get estimated wait time from global load view (if available)
  val waitTimeMs = globalLoadView
    .flatMap(_.getExecutorMetrics(request.address))
    .map(_.estimatedWaitTimeMs)
    .getOrElse(0L)

  // Penalize high wait times for non-bottleneck executors
  val waitTimePenalty = if (!bottleneckExecutor.contains(request.address)) {
    -Math.min(waitTimeMs / 100.0, 20.0)  // Max -20 points
  } else {
    0.0  // Don't penalize bottleneck
  }

  basePriority + waitTimePenalty
}
```

**Integration**: Use global load view (from global coordination design) to adjust priorities dynamically

## Concrete Example

**Scenario**:
- E1: 100MB shuffle data (bottleneck), avgProcessTime = 20ms/block
- E2: 30MB shuffle data, avgProcessTime = 15ms/block
- E3: 20MB shuffle data, avgProcessTime = 10ms/block
- Total: 150MB across 3 executors
- maxBlocksInFlightPerAddress = 1 (non-bottleneck), 3 (bottleneck)

**Traditional FIFO Schedule** (randomized):
```
Time:  E1  E2  E3
0ms:   ■   ■   ■   (all send 1 request)
20ms:  ■         (E1 done, send next)
15ms:      ■     (E2 done, send next)
10ms:          ■ (E3 done, send next)
...
```
Total time ≈ 100MB/1req × 20ms = 2000ms (E1 is bottleneck, others idle waiting)

**Adaptive Priority Schedule**:
```
Time:  E1  E2  E3
0ms:   ■■■ -   -   (send 3 requests to bottleneck E1)
20ms:  ■■■ ■   -   (E1 req done, send next; start E2)
40ms:  ■■■ ■   ■   (E1 req done, send next; start E3)
50ms:  ■■■ ■■  ■   (E3 done, E1 req done, E2 next)
...
```
Total time ≈ 100MB/3req × 20ms + interleaved = ~800ms (66% faster!)

**Key Difference**: Bottleneck E1 always has 3 requests in flight, other executors fill gaps

## Implementation Details

### Files to Modify

1. **ShuffleBlockFetcherIterator.scala** (core/src/main/scala/org/apache/spark/storage/)
   - Line 146: Change `fetchRequests` to PriorityQueue
   - Add `bottleneckExecutor: Option[BlockManagerId]`
   - Add `executorDataSizes: Map[BlockManagerId, Long]`
   - Line 383: In `partitionBlocksByFetchMode()`, calculate bottleneck
   - Line 715: In `initialize()`, populate priority queue with priorities
   - Line 1176: In `fetchUpToMaxBytes()`, implement smart interleaving
   - Line 1238: In `isRemoteAddressMaxedOut()`, use dynamic limits

2. **package.scala** (core/src/main/scala/org/apache/spark/internal/config/)
   - Add `REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS_BOTTLENECK` config
   - Add `ADAPTIVE_FETCH_SCHEDULER_ENABLED` config (master switch)
   - Add `BOTTLENECK_PRIORITY_WEIGHT` config (default 100.0)

3. **ShuffleBlockFetcherIterator.scala** - New Classes
   - `PrioritizedFetchRequest` case class
   - Priority calculation methods
   - Dynamic concurrency limit methods

### Configuration Parameters

```scala
// Enable adaptive scheduler
spark.shuffle.fetch.adaptive.enabled = true

// Bottleneck executor concurrency (default 3)
spark.reducer.maxBlocksInFlightPerAddressBottleneck = 3

// Priority weights
spark.shuffle.fetch.bottleneckPriorityWeight = 100.0
spark.shuffle.fetch.dataSizePriorityWeight = 50.0
spark.shuffle.fetch.batchSizePriorityWeight = 10.0
spark.shuffle.fetch.waitTimePenaltyEnabled = true
```

### Metrics to Track

Add to ShuffleBlockFetcherIterator:
```scala
// Metrics
private var bottleneckUtilizationSum = 0L  // Sum of time with requests in flight
private var totalFetchTimeMs = 0L
private var bottleneckIdleTimeMs = 0L

def getBottleneckUtilization: Double = {
  bottleneckUtilizationSum.toDouble / totalFetchTimeMs
}
```

Log in `next()` method:
```scala
logInfo(s"Adaptive scheduler stats: " +
  s"bottleneck=${bottleneckExecutor.map(_.executorId)}, " +
  s"utilization=${getBottleneckUtilization}, " +
  s"totalTime=${totalFetchTimeMs}ms")
```

## Algorithm Pseudocode

```
INITIALIZE:
  executorDataSizes = aggregate block sizes by executor from MapOutputTracker
  bottleneckExecutor = executor with max(executorDataSizes)
  fetchRequests = PriorityQueue ordered by calculatePriority()

FETCH_LOOP:
  while hasNext:
    fetchUpToMaxBytes():
      // Keep bottleneck at max concurrency
      while bottleneckNotMaxed AND queueNotEmpty:
        req = dequeue next bottleneck request
        if can send:
          send(req)

      // Fill remaining capacity with highest priority
      while canSendMore AND queueNotEmpty:
        req = dequeue highest priority request
        if can send:
          send(req)
        else:
          defer(req)

    result = blockingWait for next result
    process(result)
    update metrics
```

## Performance Analysis

**Benefits**:
- **Reduced Total Wait Time**: 30-70% improvement when bottleneck executor stores >50% of data
- **Better Pipelining**: Bottleneck executor always busy, minimizes idle time
- **Adaptive**: Automatically identifies and prioritizes bottleneck per shuffle

**Overhead**:
- **Priority Queue**: O(log N) dequeue vs O(1), negligible for ~100-1000 requests
- **Bottleneck Calculation**: O(N) once during initialization, <1ms
- **Memory**: +16 bytes per request (priority double), negligible

**Scalability**:
- Works for any number of executors
- Priority calculation is local (no coordination overhead)
- Compatible with existing global load coordination design

## Integration with Global Coordination

This adaptive scheduler **complements** the global coordination design:

1. **Global Coordination**: Provides wait time estimates per executor
2. **Adaptive Scheduler**: Uses wait times to adjust priorities dynamically
3. **Combined Benefit**: Bottleneck prioritization + wait-aware interleaving

**Priority Function with Global Load**:
```scala
priority = bottleneckBonus(100) +
           dataSizeWeight(50) +
           batchSizeWeight(10) -
           waitTimePenalty(0-20)
```

## Testing Strategy

### Unit Tests
1. Test bottleneck identification with various data distributions
2. Test priority calculation correctness
3. Test dynamic concurrency limits
4. Test interleaving algorithm

### Integration Tests
1. Benchmark with skewed shuffle (90% on one executor)
2. Benchmark with balanced shuffle (even distribution)
3. Compare FIFO vs adaptive scheduler
4. Measure bottleneck utilization

### Performance Tests
1. TPC-DS queries with shuffle-heavy workloads
2. Measure total shuffle read time
3. Track executor utilization metrics
4. Verify no regressions on balanced shuffles

## Success Criteria

- ✅ Bottleneck executor identified correctly in all test cases
- ✅ Bottleneck executor utilization >90% during fetch phase
- ✅ Total shuffle wait time reduced by >30% for skewed data
- ✅ No performance regression for balanced data distributions
- ✅ Configuration allows disabling adaptive scheduler
- ✅ Metrics show bottleneck utilization and effectiveness

## Implementation Phases

### Phase 1: Core Adaptive Scheduler
1. Add bottleneck identification logic
2. Implement priority queue and priority calculation
3. Add dynamic concurrency limits
4. Update fetchUpToMaxBytes() with interleaving logic

### Phase 2: Configuration & Metrics
1. Add configuration parameters
2. Add bottleneck utilization metrics
3. Add logging for debugging
4. Add master switch to enable/disable

### Phase 3: Integration with Global Coordination
1. Integrate wait time estimates into priority calculation
2. Use global load view for dynamic priority adjustment
3. Test combined system

### Phase 4: Testing & Tuning
1. Unit tests for all components
2. Integration tests with skewed data
3. Performance benchmarks
4. Tune default parameters

## Notes

- This design is **independent** of global coordination but works better when combined
- Backward compatible: can be disabled via config
- No changes to network layer or other components
- Purely client-side scheduling optimization
- Graceful degradation: if bottleneck calculation fails, falls back to FIFO
