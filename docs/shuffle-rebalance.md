# Shuffle Rebalancing Feature

## Overview

The Shuffle Rebalancing feature in Apache Spark automatically balances shuffle data distribution across executors to improve performance in scenarios with skewed shuffle data. When some executors hold significantly more shuffle data than others, this feature can move shuffle blocks from over-loaded executors to under-loaded ones.

## When to Use

Shuffle Rebalancing is beneficial in scenarios where:

- **Data Skew**: Some shuffle partitions are much larger than others
- **Executor Imbalance**: Some executors are storing disproportionate amounts of shuffle data
- **Performance Issues**: Reduce tasks are taking longer on heavily loaded executors
- **Resource Utilization**: Want to better distribute shuffle data across available executors

## Configuration

### Enable Shuffle Rebalancing

```scala
spark.conf.set("spark.shuffle.rebalance.enabled", "true")
```

### Key Configuration Options

| Configuration | Default | Description |
|---------------|---------|-------------|
| `spark.shuffle.rebalance.enabled` | `false` | Whether to enable shuffle data movement between executors |
| `spark.shuffle.rebalance.threshold` | `1.5` | Imbalance ratio threshold above which shuffle rebalancing is triggered |
| `spark.shuffle.rebalance.minSizeMB` | `100` | Minimum size difference in MB required to trigger shuffle rebalancing |
| `spark.shuffle.rebalance.checkIntervalMs` | `30000` | Interval in milliseconds to check for shuffle imbalance |
| `spark.shuffle.rebalance.maxConcurrent` | `2` | Maximum number of concurrent shuffle rebalancing operations |

### Multi-Location Configuration

| Configuration | Default | Description |
|---------------|---------|-------------|
| `spark.shuffle.rebalance.enableMultiLocation` | `true` | Whether to enable multi-location support for shuffle blocks |
| `spark.shuffle.rebalance.maxLocationsPerBlock` | `2` | Maximum number of locations to maintain for each shuffle block |
| `spark.shuffle.rebalance.locationSelectionStrategy` | `LOCALITY_PREFERRED` | Strategy for selecting best location: ROUND_ROBIN, LOCALITY_PREFERRED, LOAD_BALANCED |

### Advanced Configuration

```scala
// Retry configuration
spark.conf.set("spark.shuffle.rebalance.maxRetries", "3")
spark.conf.set("spark.shuffle.rebalance.retryWaitMs", "5000")

// Performance tuning
spark.conf.set("spark.shuffle.rebalance.checkIntervalMs", "15000")  // More frequent checks
spark.conf.set("spark.shuffle.rebalance.maxConcurrent", "4")        // More concurrent moves
```

## How It Works

### 1. Detection Phase

The Shuffle Rebalancing Manager continuously monitors shuffle data distribution across executors:

- Calculates shuffle data size per executor
- Computes imbalance ratio (max_size / average_size)
- Triggers move operations when thresholds are exceeded

### 2. Planning Phase

When imbalance is detected:

- Identifies source (over-loaded) and target (under-loaded) executors
- Selects specific shuffle blocks to move
- Plans move operations to minimize disruption

### 3. Execution Phase

Move operations are executed:

- Transfers shuffle blocks between executors
- **Maintains permanent dual copies** for maximum fault tolerance (when enabled)
- Updates MapOutputTracker with multiple locations for intelligent fetching

### 4. Dual Copy Management

With multi-location support enabled:

- **Permanent Dual Copies**: During moves, blocks exist in both source and target locations permanently
- **Intelligent Fetching**: Executors can fetch from either location for better load balancing
- **Maximum Fault Tolerance**: Multiple locations ensure blocks remain available even if some executors fail
- **Load Balancing**: Multiple locations enable better distribution of fetch requests

## Example Usage

### Basic Setup

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder
  .appName("Shuffle Rebalancing Example")
  .config("spark.shuffle.rebalance.enabled", "true")
  .config("spark.shuffle.rebalance.threshold", "1.5")
  .config("spark.shuffle.rebalance.minSizeMB", "100")
  .getOrCreate()

// Your Spark application code here
val data = spark.read.parquet("input")
val result = data.groupBy("key").agg(count("*"))
result.write.parquet("output")
```

### With Custom Thresholds

```scala
val spark = SparkSession.builder
  .appName("Shuffle Rebalancing Example")
  .config("spark.shuffle.rebalance.enabled", "true")
  .config("spark.shuffle.rebalance.threshold", "2.0")      // More tolerance for imbalance
  .config("spark.shuffle.rebalance.minSizeMB", "500")      // Only move for larger differences
  .config("spark.shuffle.rebalance.maxConcurrent", "1")    // Conservative concurrency
  .getOrCreate()
```

### With Multi-Location Support

```scala
val spark = SparkSession.builder
  .appName("Shuffle Rebalancing with Dual Copy")
  .config("spark.shuffle.rebalance.enabled", "true")
  .config("spark.shuffle.rebalance.enableMultiLocation", "true")
  .config("spark.shuffle.rebalance.maxLocationsPerBlock", "3")
  .config("spark.shuffle.rebalance.locationSelectionStrategy", "LOAD_BALANCED")
  .getOrCreate()

// The application will automatically:
// 1. Maintain permanent dual copies during shuffle rebalancings
// 2. Allow executors to fetch from multiple locations for better load balancing
// 3. Use load balancing strategy for location selection
// 4. Provide maximum fault tolerance with multiple locations
```

## Monitoring and Debugging

### Log Messages

Enable debug logging to see shuffle rebalancing activity:

```scala
spark.conf.set("spark.sql.adaptive.logLevel", "DEBUG")
```

Look for log messages like:
```
INFO ShuffleRebalanceManager: Starting permanent dual-copy transfer of 15 blocks from exec1 to exec3
INFO ShuffleRebalanceManager: Successfully transferred 15 shuffle blocks from exec1 to exec3 with permanent dual copy
```

### Multi-Location Specific Logs

```
INFO MapOutputTrackerMasterMultiLocation: Added location BlockManagerId(exec3, host3, 7337) for shuffle 1 map 5. Total locations: 2
INFO MultiLocationShuffleBlockFetcher: Selected blocks from 3 locations for shuffle 1 partitions 0-10
INFO LocationSelectionStats: strategy: LOCALITY_PREFERRED, requests: 150, executors: 3, imbalance: 1.2
```

### Web UI Integration

The Spark Web UI shows shuffle rebalancing statistics:
- Navigate to "Stages" tab
- Look for "Shuffle Rebalancing" section in stage details
- View metrics like blocks moved, data transferred, success rate

### Metrics

Monitor shuffle rebalancing performance through:
- Number of move operations triggered
- Total bytes transferred
- Success/failure rates
- Average transfer time

## Performance Considerations

### Benefits

- **Improved Load Distribution**: Better utilization of cluster resources
- **Reduced Task Time Variance**: More consistent reduce task execution times
- **Better Parallelism**: Avoid bottlenecks on heavily loaded executors

### Overhead

- **Network Traffic**: Additional data transfer between executors
- **Monitoring Overhead**: Periodic checks for imbalance
- **Metadata Updates**: MapOutputTracker updates for new locations

### Best Practices

1. **Start Conservative**: Begin with default settings and tune based on workload
2. **Monitor Impact**: Watch for improvements in task time distribution
3. **Adjust Thresholds**: Fine-tune based on cluster size and data patterns
4. **Consider Workload**: More beneficial for workloads with known skew patterns

## Limitations

- **Only for Shuffle Data**: Does not rebalance cached RDDs or broadcast variables
- **Network Dependency**: Requires stable network connectivity between executors
- **Timing Sensitive**: Most effective when applied before reduce phase begins
- **Overhead vs Benefit**: May not be beneficial for small shuffles or balanced workloads

## Troubleshooting

### Common Issues

**Shuffle moves not triggering:**
- Check if `spark.shuffle.rebalance.enabled` is set to `true`
- Verify imbalance exceeds threshold settings
- Ensure minimum size difference is met

**Transfer failures:**
- Check network connectivity between executors
- Review retry configuration settings
- Examine executor logs for detailed error messages

**Performance regression:**
- Consider reducing `maxConcurrent` moves
- Increase `minSizeMB` threshold
- Adjust `checkIntervalMs` for less frequent checks

### Debugging Commands

```scala
// Check current shuffle distribution
spark.sparkContext.statusTracker.getExecutorInfos.foreach { exec =>
  println(s"Executor ${exec.executorId}: ${exec.diskUsed} bytes on disk")
}

// Monitor active stages
spark.sparkContext.statusTracker.getActiveStageIds().foreach { stageId =>
  val stage = spark.sparkContext.statusTracker.getStageInfo(stageId)
  println(s"Stage $stageId: ${stage.map(_.numTasks)} tasks")
}
```

## Integration with Adaptive Query Execution (AQE)

Shuffle Rebalancing works well with AQE features:

```scala
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.shuffle.rebalance.enabled", "true")
```

This combination provides comprehensive optimization for shuffle operations.