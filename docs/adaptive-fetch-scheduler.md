# Plan: Global Optimal Shuffle Fetch Scheduler with Load Balancing

## Overview

Design an adaptive fetch scheduler that achieves **global optimal** by minimizing the maximum completion time across all parallel executor fetches. Since tasks wait for ALL parallel fetches to complete, we should balance the load so all executors finish at roughly the same time.

**Key Insight**: Task running time = max(executor completion times), not sum. We need load balancing, not bottleneck prioritization.

## Problem Analysis

### Current Limitations
- **FIFO Scheduling**: No awareness of executor load or completion times
- **Unbalanced Load**: May overload one executor while others are idle
- **Suboptimal Parallelism**: Doesn't minimize max(completion time) across parallel fetches

### Critical Insight: Global Optimal vs Bottleneck-First

**Example with 3 executors, 15 blocks total (1 min/block processing time)**:
- E1 has 10 blocks (most data)
- E2 has 3 blocks
- E3 has 2 blocks

**Bottleneck-First Approach** (WRONG):
```
Time: E1         E2    E3
0-10: ██████████ -     -    (process all E1 blocks first)
10-13: -         ███   -    (then E2)
13-15: -         -     ██   (then E3)
Total: 15 minutes
```

**Load-Balanced Approach** (GLOBAL OPTIMAL):
```
Time: E1    E2    E3
0-5:  █████ █████ █████  (balance load evenly: 5+5+5=15 blocks)
Total: 5 minutes (3x faster!)
```

**Key**: All executors finish at t=5, minimizing max completion time.

## Proposed Architecture

### 1. Global Optimal Objective

**Minimize**: `max(completion_time[executor_i] for all executors)`

Where: `completion_time[executor] = estimated_wait_time + fetch_time`

**Not**: Minimize total wait time or keep bottleneck busy

### 2. Load Balancing Algorithm

#### Phase 1: Initial Load Distribution

Calculate target load per executor to balance completion times:

```scala
def calculateOptimalDistribution(
    blocks: Seq[BlockInfo],
    executorSpeeds: Map[BlockManagerId, Double]): Map[BlockManagerId, Seq[BlockInfo]] = {

  // Group blocks by executor location
  val blocksByExecutor = blocks.groupBy(_.location)

  // Calculate total bytes per executor
  val executorTotalBytes = blocksByExecutor.mapValues(_.map(_.size).sum)

  // Equal shares: distribute evenly by bytes
  val totalBytes = executorTotalBytes.values.sum
  val numExecutors = blocksByExecutor.size
  val targetBytesPerExecutor = totalBytes / numExecutors

  // Balance: move blocks from overloaded to underloaded executors
  val balanced = balanceLoad(blocksByExecutor, targetBytesPerExecutor)

  balanced
}
```

**Note**: Blocks can be fetched from ANY executor that has them (due to replication or shuffle service), so we can redistribute load.

#### Phase 2: Dynamic Rebalancing

Track ongoing completion times and rebalance remaining fetches:

```scala
class DynamicLoadBalancer {
  // Track expected completion time per executor
  private val executorCompletionTimes = mutable.HashMap[BlockManagerId, Long]()

  // Track bytes in flight per executor
  private val executorBytesInFlight = mutable.HashMap[BlockManagerId, Long]()

  def selectExecutorForFetch(
      availableLocations: Seq[BlockManagerId],
      blockSize: Long): BlockManagerId = {

    // Calculate expected completion time if we send to each executor
    val completionTimes = availableLocations.map { executor =>
      val currentLoad = executorBytesInFlight.getOrElse(executor, 0L)
      val waitTime = estimateWaitTime(executor, currentLoad + blockSize)
      executor -> (currentTimeMillis + waitTime)
    }

    // Select executor with EARLIEST expected completion time (load balancing)
    val selected = completionTimes.minBy(_._2)._1

    // Update tracking
    executorBytesInFlight(selected) += blockSize
    executorCompletionTimes(selected) = completionTimes.find(_._1 == selected).get._2

    selected
  }

  def onFetchComplete(executor: BlockManagerId, blockSize: Long): Unit = {
    executorBytesInFlight(executor) -= blockSize
    // Recompute completion time based on remaining load
    if (executorBytesInFlight(executor) > 0) {
      executorCompletionTimes(executor) =
        currentTimeMillis + estimateWaitTime(executor, executorBytesInFlight(executor))
    } else {
      executorCompletionTimes.remove(executor)
    }
  }

  def estimateWaitTime(executor: BlockManagerId, bytes: Long): Long = {
    // Use metrics from previous implementation
    val metrics = globalLoadView.getExecutorMetrics(executor)
    val avgProcessTimePerByte = metrics.avgDiskIOTimeMs / avgBlockSize
    val queueDepth = metrics.queueDepth

    // Estimated wait = queue processing time + this request's processing time
    (queueDepth * metrics.avgDiskIOTimeMs) + (bytes * avgProcessTimePerByte)
  }
}
```

### 3. Fetch Request Scheduling

**Replace**: FIFO queue → Dynamic load-balanced selection

**Algorithm**:
```scala
def fetchUpToMaxBytes(): Unit = {
  while (canSendMore && pendingBlocks.nonEmpty) {
    val block = pendingBlocks.head
    val availableExecutors = getLocationsForBlock(block)

    // Select executor with minimum expected completion time
    val selectedExecutor = loadBalancer.selectExecutorForFetch(
      availableExecutors,
      block.size
    )

    if (canSendTo(selectedExecutor)) {
      sendFetchRequest(selectedExecutor, block)
      pendingBlocks.dequeue()
    } else {
      break  // Will retry later when capacity available
    }
  }
}

def onFetchComplete(result: FetchResult): Unit = {
  // Update load balancer
  loadBalancer.onFetchComplete(result.executor, result.blockSize)

  // Try to send more requests (may rebalance to different executors)
  fetchUpToMaxBytes()
}
```

### 4. Replication-Aware Load Balancing

Many shuffle blocks are replicated (push-based shuffle) or available from multiple locations (external shuffle service). We can exploit this:

```scala
def getLocationsForBlock(blockId: BlockId): Seq[BlockManagerId] = {
  blockId match {
    case shuffleBlock: ShuffleBlockId =>
      // External shuffle service: can fetch from any executor that ran the map task's executor
      // Push-based shuffle: multiple merger locations have the block
      mapOutputTracker.getLocationsForBlock(shuffleBlock)
    case _ =>
      // Regular block: single location
      Seq(blockManager.getLocations(blockId))
  }
}
```

**Key**: With multiple locations per block, we have flexibility to balance load.

### 5. Pipelining with Balanced Concurrency

Instead of prioritizing bottleneck executor, maintain balanced concurrency across all executors:

```scala
def getMaxConcurrencyForExecutor(executor: BlockManagerId): Int = {
  val numExecutors = activeExecutors.size
  val totalConcurrency = maxReqsInFlight

  // Distribute concurrency evenly
  math.max(1, totalConcurrency / numExecutors)
}
```

**Example**:
- maxReqsInFlight = 6
- 3 executors
- Each executor gets 2 concurrent requests (balanced)

## Concrete Example: Global Optimal vs Bottleneck-First

**Scenario**:
- 3 executors
- 150MB total shuffle data
- Each block is 10MB
- Processing time: 100ms per 10MB block

**Data Distribution**:
- E1: 100MB (10 blocks) - has most data
- E2: 30MB (3 blocks)
- E3: 20MB (2 blocks)

**Bottleneck-First Schedule**:
```
Time  E1  E2  E3  | Max Wait
0ms   ■■  -   -   | E1: 200ms (2 concurrent)
200   ■■  -   -   | E1: 400ms
400   ■■  -   -   | E1: 600ms
600   ■■  -   -   | E1: 800ms
800   ■■  -   -   | E1: 1000ms (all E1 done)
1000  -   ■■  -   | E2: 1200ms
1200  -   ■   -   | E2: 1300ms (all E2 done)
1300  -   -   ■■  | E3: 1500ms (all E3 done)
Total: 1500ms
```

**Load-Balanced Schedule** (Global Optimal):
```
Redistribute 150MB evenly: 50MB each (5 blocks each)

Time  E1  E2  E3  | Max Wait
0ms   ■■  ■■  ■■  | All: 200ms (2 concurrent each)
200   ■■  ■■  ■■  | All: 400ms
400   ■   ■   ■   | All: 500ms (all done)
Total: 500ms (3x faster!)
```

**Key Difference**:
- Bottleneck-first: E1 takes 1000ms while E2, E3 idle
- Load-balanced: All executors work equally, finish together at 500ms

## Implementation Details

### Files to Modify

1. **ShuffleBlockFetcherIterator.scala** (core/src/main/scala/org/apache/spark/storage/)
   - Add `DynamicLoadBalancer` class
   - Replace FIFO queue with load-balanced selection
   - Track executor completion times
   - Implement `selectExecutorForFetch()` with min-completion-time selection

2. **MapOutputTracker.scala** (core/src/main/scala/org/apache/spark/)
   - Add `getLocationsForBlock()` to support multiple locations per block
   - For external shuffle service: return all executors that can serve the block

3. **package.scala** (core/src/main/scala/org/apache/spark/internal/config/)
   - Add `ADAPTIVE_FETCH_LOAD_BALANCING_ENABLED` config
   - Add `FETCH_COMPLETION_TIME_TRACKING_ENABLED` config

### Key Data Structures

```scala
// Track expected completion time per executor
private val executorCompletionTimes = mutable.HashMap[BlockManagerId, Long]()

// Track bytes currently being fetched from each executor
private val executorBytesInFlight = mutable.HashMap[BlockManagerId, Long]()

// Track blocks pending fetch
private val pendingBlocks = mutable.Queue[BlockInfo]()
```

### Algorithm Pseudocode

```
INITIALIZE:
  For each block:
    locations = getLocationsForBlock(block)
    pendingBlocks.enqueue((block, locations))

FETCH_LOOP:
  while hasMoreBlocks:
    fetchUpToMaxBytes():
      while canSendMore:
        (block, locations) = pendingBlocks.head

        // Select executor with minimum expected completion time
        selectedExecutor = locations.minBy(executor =>
          estimatedCompletionTime(executor, block.size)
        )

        if canSendTo(selectedExecutor):
          send(selectedExecutor, block)
          update executorBytesInFlight
          update executorCompletionTimes
          pendingBlocks.dequeue()
        else:
          break

    result = blockingWait for next result

    onFetchComplete(result.executor, result.blockSize):
      update executorBytesInFlight
      recompute executorCompletionTimes
      fetchUpToMaxBytes()  // May send to different executor now
```

## Wait Time Estimation Integration

Use metrics from previous implementation:
- `processFetchRequestLatencyMillis` - total processing time per request
- `queueDepth` - pending requests on executor
- Global load view - cluster-wide wait time estimates

```scala
def estimateWaitTime(executor: BlockManagerId, additionalBytes: Long): Long = {
  val metrics = globalLoadView.flatMap(_.getExecutorMetrics(executor))

  metrics.map { m =>
    // Queue processing time
    val queueTime = m.queueDepth * m.avgProcessTimeMs

    // This request's processing time
    val processTime = (additionalBytes.toDouble / avgBlockSize) * m.avgProcessTimeMs

    queueTime + processTime
  }.getOrElse {
    // Fallback: use historical average or 0
    0L
  }
}
```

## Configuration Parameters

```scala
// Enable load-balanced scheduling
spark.shuffle.fetch.loadBalancing.enabled = true

// Track completion times for dynamic rebalancing
spark.shuffle.fetch.completionTimeTracking.enabled = true

// Rebalancing interval (ms) - how often to recompute load distribution
spark.shuffle.fetch.rebalancing.intervalMs = 100

// Enable wait time estimation
spark.shuffle.fetch.waitTimeEstimation.enabled = true
```

## Performance Analysis

### Example Improvement

**Skewed Data (90% on 1 executor)**:
- Traditional: 90% of time fetching from one executor, others idle
- Load-balanced: Redistribute to all executors, all finish together
- **Improvement**: Up to 10x faster (if 10 executors)

**Balanced Data (even distribution)**:
- Traditional: Already balanced naturally
- Load-balanced: Similar performance
- **Regression**: None, already optimal

### Overhead

- **Completion Time Tracking**: O(1) updates per fetch complete
- **Executor Selection**: O(N) where N = locations per block (typically 1-3)
- **Memory**: ~32 bytes per executor for tracking
- **Total**: <1% overhead

## Testing Strategy

### Unit Tests
1. Test load balancing with skewed data (90% on 1 executor)
2. Test dynamic rebalancing as executors finish
3. Test completion time estimation accuracy
4. Test fallback when wait times unavailable

### Integration Tests
1. Benchmark skewed shuffle (90-10 split)
2. Benchmark balanced shuffle (even split)
3. Compare max completion time vs total wait time
4. Verify all executors finish at similar times

### Performance Tests
1. TPC-DS queries with skewed shuffle
2. Measure max(executor fetch time) as primary metric
3. Track executor idle time
4. Compare against bottleneck-first approach

## Success Criteria

- ✅ Max(executor completion time) minimized for skewed data
- ✅ All executors finish within 10% of each other (balanced load)
- ✅ 3x+ improvement for skewed data (90% on 1 executor)
- ✅ No regression for balanced data
- ✅ Dynamic rebalancing adjusts as wait times change
- ✅ Works with external shuffle service and push-based shuffle

## Implementation Phases

### Phase 1: Load Balancing Core
1. Add DynamicLoadBalancer class
2. Implement executor selection by min-completion-time
3. Track executor bytes in flight and completion times
4. Implement basic load balancing algorithm

### Phase 2: Wait Time Integration
1. Integrate with global load view metrics
2. Add wait time estimation per executor
3. Use processFetchRequestLatencyMillis for accurate estimates
4. Add dynamic rebalancing based on changing wait times

### Phase 3: Multi-Location Support
1. Add getLocationsForBlock() to MapOutputTracker
2. Support external shuffle service multi-location
3. Support push-based shuffle merger locations
4. Test load balancing across replicas

### Phase 4: Testing & Optimization
1. Unit tests for load balancing algorithm
2. Integration tests with skewed data
3. Performance benchmarks
4. Tune rebalancing interval and thresholds

## Notes

- **Global optimal**: Minimize max(executor completion time), not sum
- **Load balancing**: Distribute bytes evenly across executors
- **Dynamic rebalancing**: Adjust as wait times change during execution
- **Multi-location**: Exploit replication to balance load
- **Complementary**: Works with wait time estimation from metrics implementation
- **Independent**: No network layer or driver-side coordination changes needed
