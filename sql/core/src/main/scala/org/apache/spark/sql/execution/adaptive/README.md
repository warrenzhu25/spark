# Adaptive Query Execution (AQE) Framework

## Overview

Adaptive Query Execution (AQE) is a runtime query optimization framework in Apache Spark SQL that re-optimizes and adjusts query plans based on runtime statistics collected during query execution. Unlike traditional static optimization that happens before execution begins, AQE enables dynamic plan modifications as data flows through the query pipeline.

## Core Concepts

### 1. Query Stages

A **Query Stage** (`QueryStageExec`) is an independent subgraph of the query plan that gets materialized before proceeding with further operators. Query stages are created at shuffle boundaries (exchanges) and broadcast operations.

**Key Properties:**
- Each stage has a unique ID within the query plan
- Stages are materialized asynchronously
- Runtime statistics are collected after materialization
- Statistics include: data size, row count, partition size distribution

**Types of Query Stages:**
- `ShuffleQueryStageExec`: Created for shuffle exchanges, collects `MapOutputStatistics`
- `BroadcastQueryStageExec`: Created for broadcast operations

### 2. Framework Architecture

```
AdaptiveSparkPlanExec (Root)
    │
    ├─> Create Initial Plan
    │   └─> Apply preprocessing rules
    │
    ├─> Stage Creation Loop
    │   ├─> Traverse plan bottom-up
    │   ├─> Create stages at exchange boundaries
    │   ├─> Materialize stages asynchronously
    │   └─> Wait for stage completion
    │
    └─> Re-optimization Loop (after each stage completes)
        ├─> Collect runtime statistics
        ├─> Re-optimize logical plan with AQEOptimizer
        ├─> Re-plan with updated statistics
        ├─> Apply queryStageOptimizerRules
        ├─> Validate plan requirements
        └─> Evaluate cost and decide adoption
```

### 3. Rule Types

AQE has four types of rules that are applied at different phases:

#### a. Query Post-Planner Strategy Rules (`queryPostPlannerStrategyRules`)
Applied after planning but before exchange injection. Gets the whole plan without exchanges.

**Example:** Planning for adaptive dynamic pruning filters

#### b. Query Stage Prep Rules (`queryStagePrepRules`)
Applied before creating query stages. Prepares the plan for stage boundaries.

**Example:** Adjusting shuffle exchange positions

#### c. Runtime Optimizer Rules (`runtimeOptimizerRules`)
Applied to the **logical plan** during re-optimization, using runtime statistics.

**Example:**
- `DynamicJoinSelection`: Changes join strategies based on data size
- Propagating empty relations

#### d. Query Stage Optimizer Rules (`queryStageOptimizerRules`)
Applied to the **physical plan** after new query stages are created. These are `AQEShuffleReadRule` implementations.

**Examples:**
- `CoalesceShufflePartitions`: Reduces partition count based on data size
- `OptimizeSkewedJoin`: Splits skewed partitions
- `OptimizeShuffleWithLocalRead`: Converts shuffle reads to local reads

### 4. Re-optimization Process

After each query stage completes materialization:

1. **Statistics Collection**: Runtime statistics are collected from completed stages
2. **Logical Re-optimization**: The remaining logical plan is re-optimized using `AQEOptimizer` with `runtimeOptimizerRules`
3. **Physical Re-planning**: The optimized logical plan is converted back to a physical plan
4. **Physical Optimization**: `queryStageOptimizerRules` are applied to the new physical plan
5. **Validation**: The new plan is validated against distribution requirements
6. **Cost Evaluation**: Cost comparison determines if the new plan should be adopted
7. **Adoption Decision**: If new cost ≤ old cost, the new plan is adopted

## How to Diagnose AQE Optimization Decisions

### 1. Enable Detailed Logging

Set the AQE log level to see detailed optimization decisions:

```scala
spark.conf.set("spark.sql.adaptive.logLevel", "INFO")  // or "DEBUG" for more details
```

Log levels:
- `TRACE`: Most verbose, logs all internal operations
- `DEBUG`: Logs optimization attempts, rule applications, statistics
- `INFO`: Logs when optimizations are applied or skipped (recommended)
- `WARN`: Only warnings about potential issues
- `ERROR`: Only error conditions

### 2. Key Configuration Parameters

```scala
// Enable/Disable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")

// Force apply AQE even for queries with cached data
spark.conf.set("spark.sql.adaptive.forceApply", "false")

// Enable/Disable specific optimizations
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

// Tuning parameters
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1MB")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

### 3. Understanding Log Messages

#### Coalesce Shuffle Partitions

```
[INFO] Coalescing shuffle partitions for stage 2: 200 -> 5 partitions (target size: 64MB)
```
**Meaning**: Stage 2's shuffle output was coalesced from 200 to 5 partitions to reach target size.

```
[DEBUG] Coalesce not applied to group: no partition specs generated
```
**Meaning**: Partition coalescing couldn't be applied (possibly too much data or other constraints).

#### Dynamic Join Selection

```
[INFO] Dynamic join selection for left: choosing SHUFFLE_HASH (demote broadcast + prefer shuffle hash)
```
**Meaning**: Left side of join switched from broadcast to shuffle hash due to runtime statistics.

```
[DEBUG] Dynamic join selection for right: no strategy hint
```
**Meaning**: Right side statistics didn't suggest any join strategy changes.

#### Skew Join Optimization

```
[INFO] Skew join optimization applied: 3 left and 5 right skewed partitions out of 200 total partitions
```
**Meaning**: 3 left-side and 5 right-side partitions were identified as skewed and will be split.

```
[INFO] Skew join optimization not applied: no skewed partitions found. Left threshold: 256MB, Right threshold: 256MB
```
**Meaning**: No partitions exceeded the skew thresholds, so skew handling wasn't needed.

#### Local Shuffle Read

```
[INFO] Optimizing shuffle with local read
```
**Meaning**: Shuffle read was converted to local read because data locality allows it.

#### Plan Re-optimization

```
[INFO] Plan changed after re-optimization. Cost: 1000 -> 500
```
**Meaning**: Re-optimization produced a better plan (lower cost), which was adopted.

```
[DEBUG] Re-optimized plan not adopted. Cost: 1000 vs 1200
```
**Meaning**: Re-optimization produced a worse plan (higher cost), original plan retained.

```
[INFO] Rule CoalesceShufflePartitions is not applied as it breaks the distribution requirement of the query plan. Required: HashPartitioning(...)
```
**Meaning**: An optimization was skipped because it would violate distribution requirements.

### 4. Monitoring AQE in Spark UI

1. **SQL Tab**: Check the "Details" section for query plans
2. **Query Execution Plan**: Look for:
   - `AdaptiveSparkPlan` nodes
   - `QueryStage` nodes with IDs
   - `AQEShuffleRead` nodes indicating AQE optimizations
3. **Stage Statistics**: Review stage-level metrics to understand data distribution
4. **Event Timeline**: See when stages completed and re-optimization occurred

### 5. Programmatic Debugging

```scala
// Access the final executed plan
val df = spark.sql("SELECT ...")
df.explain("cost")      // Show cost-based optimizations
df.explain("extended")  // Show all optimization phases

// Access execution metrics
val qe = df.queryExecution
println(qe.executedPlan)  // Shows the final adaptive plan

// For testing, you can disable AQE temporarily
spark.conf.set("spark.sql.adaptive.enabled", "false")
```

### 6. Common Diagnostic Scenarios

#### Why wasn't coalescing applied?

**Check:**
1. Is `spark.sql.adaptive.coalescePartitions.enabled` = true?
2. Are partitions already small enough?
3. Look for log: "Coalesce not applied to group"

#### Why wasn't skew join optimization applied?

**Check:**
1. Is `spark.sql.adaptive.skewJoin.enabled` = true?
2. Do any partitions exceed `skewedPartitionThresholdInBytes`?
3. Is the skew factor high enough (`skewedPartitionFactor`)?
4. Look for log: "Skew join optimization not applied"

#### Why did the plan not change after re-optimization?

**Check:**
1. Cost comparison: New cost must be ≤ old cost
2. Validation: New plan must satisfy distribution requirements
3. Look for log: "Re-optimized plan not adopted"

## How to Add New AQE Rules

### Step 1: Determine Rule Type

Choose the appropriate rule type based on when your optimization should run:

| Rule Type | When to Use | Operates On |
|-----------|-------------|-------------|
| `queryPostPlannerStrategyRules` | Need whole plan before exchanges | Physical Plan |
| `queryStagePrepRules` | Adjust plan before stage creation | Physical Plan |
| `runtimeOptimizerRules` | Use runtime statistics, modify join/aggregation strategies | Logical Plan |
| `queryStageOptimizerRules` | Modify shuffle reads based on statistics | Physical Plan |

### Step 2: Choose the Right Base Class

#### For Shuffle Read Optimizations

Extend `AQEShuffleReadRule`:

```scala
case class MyShuffleOptimization(session: SparkSession) extends AQEShuffleReadRule {

  override protected def supportedShuffleOrigins: Seq[ShuffleOrigin] =
    Seq(ENSURE_REQUIREMENTS)  // or other shuffle origins

  override def apply(plan: SparkPlan): SparkPlan = {
    // Check if optimization is enabled
    if (!conf.getConf(MY_OPTIMIZATION_ENABLED)) {
      logDebug("My optimization disabled by config")
      return plan
    }

    // Your optimization logic here
    plan.transformUp {
      case stage: ShuffleQueryStageExec if isSupported(stage.shuffle) =>
        // Access runtime statistics
        val stats = stage.mapStats.getOrElse(return stage)

        // Make optimization decision
        if (shouldOptimize(stats)) {
          logInfo(s"Applying my optimization to stage ${stage.id}")
          applyOptimization(stage)
        } else {
          logDebug(s"My optimization not applicable to stage ${stage.id}")
          stage
        }
    }
  }

  private def shouldOptimize(stats: MapOutputStatistics): Boolean = {
    // Your decision logic based on statistics
    ???
  }

  private def applyOptimization(stage: ShuffleQueryStageExec): SparkPlan = {
    // Create new partition specs or plan modifications
    ???
  }
}
```

#### For Logical Plan Optimizations

Extend `Rule[LogicalPlan]`:

```scala
object MyLogicalOptimization extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformUp {
      case join @ Join(left, right, joinType, condition, hint) =>
        // Access runtime statistics through LogicalQueryStage
        val leftStats = getStageStats(left)
        val rightStats = getStageStats(right)

        if (shouldChangeStrategy(leftStats, rightStats)) {
          logInfo(s"Changing join strategy: $joinType")
          // Return modified join with new hint
          join.copy(hint = newHint)
        } else {
          join
        }
    }
  }

  private def getStageStats(plan: LogicalPlan): Option[Statistics] = {
    plan.collectFirst {
      case LogicalQueryStage(_, stage: QueryStageExec) if stage.isMaterialized =>
        stage.getRuntimeStatistics
    }
  }
}
```

### Step 3: Add Logging

Follow the logging pattern used in existing rules:

```scala
// At rule entry
if (!conf.getConf(MY_OPTIMIZATION_ENABLED)) {
  logDebug(s"My optimization disabled by config (${MY_OPTIMIZATION_ENABLED.key}=false)")
  return plan
}

// When optimization is applied
logInfo(s"My optimization applied: transformed X into Y with parameters Z")

// When optimization is not applied
logInfo(s"My optimization not applied: reason for skipping")

// Detailed debugging information
logDebug(s"My optimization decision factors: " +
  s"factor1=$factor1, factor2=$factor2, threshold=$threshold")
```

### Step 4: Register the Rule

Add your rule to the appropriate sequence in the initialization code:

```scala
// In SessionState or extension point
override def adaptiveRulesHolder: AdaptiveRulesHolder = {
  new AdaptiveRulesHolder(
    queryStagePrepRules = Seq(...),
    runtimeOptimizerRules = Seq(
      ...,
      MyLogicalOptimization  // Add your logical rule here
    ),
    queryStageOptimizerRules = Seq(
      ...,
      MyShuffleOptimization(session)  // Add your shuffle read rule here
    ),
    queryPostPlannerStrategyRules = Seq(...)
  )
}
```

### Step 5: Add Configuration

Add SQL configuration for your rule:

```scala
// In SQLConf.scala
val MY_OPTIMIZATION_ENABLED = buildConf("spark.sql.adaptive.myOptimization.enabled")
  .doc("When true and 'spark.sql.adaptive.enabled' is true, " +
    "Spark will apply my optimization based on runtime statistics.")
  .version("3.x.0")
  .booleanConf
  .createWithDefault(true)

val MY_OPTIMIZATION_THRESHOLD = buildConf("spark.sql.adaptive.myOptimization.threshold")
  .doc("The threshold for applying my optimization.")
  .version("3.x.0")
  .bytesConf(ByteUnit.BYTE)
  .createWithDefault(64 * 1024 * 1024)  // 64MB

// Add accessor methods
def myOptimizationEnabled: Boolean = getConf(MY_OPTIMIZATION_ENABLED)
def myOptimizationThreshold: Long = getConf(MY_OPTIMIZATION_THRESHOLD)
```

### Step 6: Validate Plan Requirements

If your rule changes output partitioning, it must be validated:

```scala
// In AdaptiveSparkPlanExec.scala, the framework automatically validates
// Use ValidateRequirements.validate() if you need to validate manually

private def applyOptimization(stage: ShuffleQueryStageExec): SparkPlan = {
  val optimized = createOptimizedPlan(stage)

  // The AQE framework will validate this, but you can check early
  if (!ValidateRequirements.validate(optimized, requiredDistribution)) {
    logDebug("Optimization would break distribution requirements")
    return stage
  }

  optimized
}
```

### Step 7: Write Tests

Create tests in `AdaptiveQueryExecSuite.scala`:

```scala
test("my optimization should be applied when conditions are met") {
  withSQLConf(
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
    SQLConf.MY_OPTIMIZATION_ENABLED.key -> "true") {

    val df = spark.sql("your test query")

    // Collect the executed plan
    val adaptivePlan = collect(df.queryExecution.executedPlan) {
      case a: AdaptiveSparkPlanExec => a
    }.head

    // Verify your optimization was applied
    val optimizedStages = collect(adaptivePlan.executedPlan) {
      case MyOptimizedNode(...) => true
    }

    assert(optimizedStages.nonEmpty, "My optimization should be applied")
  }
}

test("my optimization should not be applied when disabled") {
  withSQLConf(
    SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
    SQLConf.MY_OPTIMIZATION_ENABLED.key -> "false") {

    val df = spark.sql("your test query")

    val optimizedStages = collect(df.queryExecution.executedPlan) {
      case MyOptimizedNode(...) => true
    }

    assert(optimizedStages.isEmpty, "My optimization should not be applied when disabled")
  }
}
```

## Example: Complete Shuffle Read Rule

Here's a complete example implementing a rule that skips shuffle reads for empty partitions:

```scala
/**
 * A rule that optimizes shuffle reads by skipping empty partitions.
 */
case class SkipEmptyShufflePartitions(session: SparkSession) extends AQEShuffleReadRule {

  override protected def supportedShuffleOrigins: Seq[ShuffleOrigin] =
    Seq(ENSURE_REQUIREMENTS, REBALANCE_PARTITIONS_BY_NONE)

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.SKIP_EMPTY_SHUFFLE_PARTITIONS_ENABLED)) {
      logDebug("Skip empty shuffle partitions optimization disabled by config " +
        s"(${SQLConf.SKIP_EMPTY_SHUFFLE_PARTITIONS_ENABLED.key}=false)")
      return plan
    }

    plan.transformUp {
      case stage: ShuffleQueryStageExec
          if stage.isMaterialized && isSupported(stage.shuffle) =>

        stage.mapStats match {
          case Some(stats) =>
            val nonEmptyPartitions = stats.bytesByPartitionId.zipWithIndex
              .filter(_._1 > 0)
              .map(_._2)

            val totalPartitions = stats.bytesByPartitionId.length
            val emptyCount = totalPartitions - nonEmptyPartitions.length

            if (emptyCount > 0) {
              logInfo(s"Skip empty shuffle partitions applied: " +
                s"skipping $emptyCount empty partitions out of $totalPartitions " +
                s"for stage ${stage.id}")

              // Create partition specs for non-empty partitions only
              val partitionSpecs = nonEmptyPartitions.map { partitionIndex =>
                CoalescedPartitionSpec(partitionIndex, partitionIndex + 1, stats)
              }

              AQEShuffleReadExec(stage, partitionSpecs)
            } else {
              logDebug(s"Skip empty shuffle partitions not applied: " +
                s"no empty partitions found in stage ${stage.id}")
              stage
            }

          case None =>
            logDebug(s"Skip empty shuffle partitions not applied: " +
              s"no statistics available for stage ${stage.id}")
            stage
        }
    }
  }
}
```

## Best Practices

### 1. Logging

- Always log when your rule is disabled by configuration
- Log INFO when optimization is applied with key metrics
- Log INFO when optimization is NOT applied with clear reason
- Log DEBUG for detailed decision factors
- Include relevant statistics in log messages (sizes, counts, thresholds)

### 2. Configuration

- Provide a boolean config to enable/disable the rule
- Provide tuning parameters with sensible defaults
- Document the interaction with `spark.sql.adaptive.enabled`
- Use consistent naming: `spark.sql.adaptive.<feature>.<property>`

### 3. Statistics Access

- Always check if stage is materialized before accessing statistics
- Handle `None` cases gracefully when statistics aren't available
- Consider data skew in your decision logic
- Account for empty partitions

### 4. Performance

- Avoid expensive computations in the rule application
- Cache computed values if the rule is applied multiple times
- Consider the cost of the optimization vs. benefit
- Don't create unnecessary intermediate objects

### 5. Testing

- Test with various data distributions (uniform, skewed, empty)
- Test with the rule enabled and disabled
- Test interaction with other AQE rules
- Test with different query patterns (joins, aggregations, etc.)
- Verify the optimization improves performance

## Debugging Tips

### 1. Enable Query Plan Printing

```scala
spark.conf.set("spark.sql.planChangeLog.level", "INFO")
```

### 2. Use Breakpoints in Tests

When debugging in tests, you can set breakpoints in your rule's `apply` method and inspect:
- Input plan structure
- Runtime statistics
- Decision logic variables
- Output plan changes

### 3. Compare Plans

Use the `explain()` method to compare plans with and without your optimization:

```scala
// Without optimization
spark.conf.set("spark.sql.adaptive.myOptimization.enabled", "false")
df.explain("cost")

// With optimization
spark.conf.set("spark.sql.adaptive.myOptimization.enabled", "true")
df.explain("cost")
```

### 4. Inspect Stage Statistics

Add temporary logging to inspect statistics:

```scala
stage.mapStats.foreach { stats =>
  logInfo(s"Stage ${stage.id} statistics:")
  logInfo(s"  Total bytes: ${stats.bytesByPartitionId.sum}")
  logInfo(s"  Partition sizes: ${stats.bytesByPartitionId.mkString(", ")}")
  logInfo(s"  Max partition: ${stats.bytesByPartitionId.max}")
  logInfo(s"  Min partition: ${stats.bytesByPartitionId.min}")
  logInfo(s"  Avg partition: ${stats.bytesByPartitionId.sum / stats.bytesByPartitionId.length}")
}
```

## Additional Resources

- **Source Files**: See existing rules in this directory for implementation patterns
- **Tests**: `AdaptiveQueryExecSuite.scala` contains comprehensive test examples
- **Configuration**: `SQLConf.scala` defines all AQE configuration parameters
- **Documentation**: Spark SQL documentation on Adaptive Query Execution

## Contributing

When adding new AQE rules:

1. Discuss the optimization idea on the Spark dev mailing list
2. Create a JIRA ticket for tracking
3. Implement the rule following patterns in this guide
4. Add comprehensive tests
5. Update this README if adding new concepts or patterns
6. Include performance benchmarks in the PR description
