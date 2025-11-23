# Spark SQL Join Execution: A Comprehensive Guide

## Table of Contents
1. [Join Implementations Overview](#join-implementations-overview)
2. [BroadcastHashJoinExec](#broadcasthashjoinexec)
3. [ShuffledHashJoinExec](#shuffledhashjoinexec)
4. [SortMergeJoinExec](#sortmergejoinexec)
5. [BroadcastNestedLoopJoinExec](#broadcastnestedloopjoinexec)
6. [CartesianProductExec](#cartesianproductexec)
7. [Build Side Selection](#build-side-selection)
8. [Hash Join Details](#hash-join-details)
9. [Sort Merge Join Details](#sort-merge-join-details)
10. [Join Iterators](#join-iterators)
11. [Code Generation](#code-generation)
12. [Metrics and Statistics](#metrics-and-statistics)
13. [Special Cases](#special-cases)
14. [Supporting Classes](#supporting-classes)
15. [Performance Characteristics](#performance-characteristics)

---

## Join Implementations Overview

Spark SQL provides five physical join implementations, each optimized for different scenarios:

| Join Type | Strategy | Best For | Time Complexity | Space Complexity |
|-----------|----------|----------|-----------------|------------------|
| **BroadcastHashJoinExec** | Broadcast small side, hash join | Small build side (<10MB) | O(n + m) | O(m) per executor |
| **ShuffledHashJoinExec** | Shuffle both sides, hash join | Medium build side, much smaller than stream | O(n + m) | O(m/p) per partition |
| **SortMergeJoinExec** | Shuffle + sort both sides, merge | Large tables, evenly distributed | O(n log n + m log m) | O(1) streaming |
| **BroadcastNestedLoopJoinExec** | Broadcast one side, nested loop | Non-equi joins, very small broadcast side | O(n × m) | O(m) per executor |
| **CartesianProductExec** | Cartesian product | Very small tables, no join keys | O(n × m) | O(m/p) per partition |

**File Locations**:
- All join implementations: `sql/core/src/main/scala/org/apache/spark/sql/execution/joins/`
- Base traits: `sql/core/src/main/scala/org/apache/spark/sql/execution/joins/BaseJoinExec.scala`
- Hash utilities: `sql/core/src/main/scala/org/apache/spark/sql/execution/joins/HashJoin.scala`

---

## BroadcastHashJoinExec

**File**: `sql/core/src/main/scala/org/apache/spark/sql/execution/joins/BroadcastHashJoinExec.scala`

### Class Definition

**Lines 40-49**:
```scala
case class BroadcastHashJoinExec(
    leftKeys: Seq[Expression],           // Join keys from left side
    rightKeys: Seq[Expression],          // Join keys from right side
    joinType: JoinType,                  // Inner, LeftOuter, RightOuter, etc.
    buildSide: BuildSide,                // BuildLeft or BuildRight
    condition: Option[Expression],       // Additional join condition
    left: SparkPlan,                     // Left child plan
    right: SparkPlan,                    // Right child plan
    isNullAwareAntiJoin: Boolean = false // Special flag for NAAJ
) extends HashJoin
```

### Main Execution Logic

**Lines 123-158**: The core execution method

```scala
protected override def doExecute(): RDD[InternalRow] = {
  val numOutputRows = longMetric("numOutputRows")

  // Execute broadcast exchange to get HashedRelation
  val broadcastRelation = buildPlan.executeBroadcast[HashedRelation]()

  if (isNullAwareAntiJoin) {
    // Special handling for NULL-aware anti join
    streamedPlan.execute().mapPartitionsInternal { streamedIter =>
      val hashed = broadcastRelation.value.asReadOnlyCopy()

      // Check for special cases
      if (hashed == EmptyHashedRelation) {
        // Build side is empty - return all streamed rows
        streamedIter
      } else if (hashed == HashedRelationWithAllNullKeys) {
        // All build side keys are NULL - return nothing (NULL never matches in NAAJ)
        Iterator.empty
      } else {
        // Standard NULL-aware anti join
        streamedIter.filter { row =>
          val lookupKey = keyGenerator(row)
          // Keep row if key has NULL or no match found
          !lookupKey.anyNull && hashed.get(lookupKey) == null
        }
      }
    }
  } else {
    // Standard broadcast hash join for all other join types
    streamedPlan.execute().mapPartitions { streamedIter =>
      // Get read-only copy of broadcast hash table
      val hashed = broadcastRelation.value.asReadOnlyCopy()

      // Perform hash join with appropriate iterator
      join(streamedIter, hashed, numOutputRows)
    }
  }
}
```

### Algorithm Flow

1. **Broadcast Phase**:
   - Build side is collected to driver
   - `HashedRelation` is constructed (hash table)
   - Hash table is broadcast to all executors
   - Serialized size: Must fit in `spark.sql.autoBroadcastJoinThreshold` (default 10MB)

2. **Probe Phase** (per partition):
   - Each task gets read-only copy of broadcast hash table
   - For each streamed row:
     - Generate hash key from join columns
     - Probe hash table
     - For each match, apply join condition (if exists)
     - Emit joined row

3. **Memory Management**:
   - Hash table is deserialized once per executor
   - All tasks on same executor share the same copy
   - Hash table stays in memory for query duration

### Metrics

**Lines 59-60**:
```scala
override lazy val metrics = Map(
  "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows")
)
```

### When to Use

- **Best**: Build side < 10MB (configurable)
- **Pros**: No shuffle required, very fast
- **Cons**: Can cause OOM if build side too large, broadcast overhead

---

## ShuffledHashJoinExec

**File**: `sql/core/src/main/scala/org/apache/spark/sql/execution/joins/ShuffledHashJoinExec.scala`

### Class Definition

**Lines 38-47**:
```scala
case class ShuffledHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean = false  // Set by AQE for skew-optimized joins
) extends HashJoin with ShuffledJoin
```

### Hash Table Construction

**Lines 82-101**:
```scala
def buildHashedRelation(iter: Iterator[InternalRow]): HashedRelation = {
  val buildDataSize = longMetric("buildDataSize")
  val buildTime = longMetric("buildTime")
  val start = System.nanoTime()
  val context = TaskContext.get()

  // Build hash table from iterator
  val relation = HashedRelation(
    iter,
    buildBoundKeys,
    taskMemoryManager = context.taskMemoryManager(),
    // Allow NULL keys for outer joins where build side can have NULLs
    allowsNullKey = joinType == FullOuter ||
      (joinType == LeftOuter && buildSide == BuildLeft) ||
      (joinType == RightOuter && buildSide == BuildRight),
    ignoresDuplicatedKey = ignoreDuplicatedKey
  )

  // Update metrics
  buildTime += NANOSECONDS.toMillis(System.nanoTime() - start)
  buildDataSize += relation.estimatedSize

  // Register cleanup listener
  context.addTaskCompletionListener[Unit](_ => relation.close())

  relation
}
```

### Main Execution Logic

**Lines 103-117**:
```scala
protected override def doExecute(): RDD[InternalRow] = {
  val numOutputRows = longMetric("numOutputRows")

  // Zip partitions from both sides (already co-partitioned by shuffle)
  streamedPlan.execute().zipPartitions(buildPlan.execute()) { (streamIter, buildIter) =>
    // Build hash table from build side partition
    val hashed = buildHashedRelation(buildIter)

    // Choose appropriate join algorithm based on join type
    joinType match {
      case FullOuter => buildSideOrFullOuterJoin(streamIter, hashed, numOutputRows)
      case LeftOuter if buildSide.equals(BuildLeft) => buildSideOrFullOuterJoin(streamIter, hashed, numOutputRows)
      case RightOuter if buildSide.equals(BuildRight) => buildSideOrFullOuterJoin(streamIter, hashed, numOutputRows)
      case _ => join(streamIter, hashed, numOutputRows)  // Inner/Semi/Anti joins
    }
  }
}
```

### Build-Side Outer Join with Unique Keys

**Lines 169-225**: Special optimization for outer joins when build side has unique keys

```scala
private def buildSideOrFullOuterJoinUniqueKey(
    streamIter: Iterator[InternalRow],
    hashedRelation: HashedRelation,
    numOutputRows: SQLMetric): Iterator[InternalRow] = {

  // BitSet to track which build-side keys matched
  val matchedKeys = new BitSet(hashedRelation.maxNumKeysIndex)

  val joinKeys = streamSideKeyGenerator()
  val joinedRow = new JoinedRow

  // Phase 1: Process stream side
  val streamResultIter = streamIter.flatMap { srow =>
    joinedRow.withLeft(srow)
    val keys = joinKeys(srow)

    if (keys.anyNull) {
      // NULL keys - emit with NULL build side
      numOutputRows += 1
      Some(joinedRow.withRight(buildNullRow))
    } else {
      // Lookup in hash table
      val matched = hashedRelation.getValueWithKeyIndex(keys)

      if (matched != null) {
        joinedRow.withRight(matched.getValue)
        if (boundCondition(joinedRow)) {
          // Mark this build key as matched
          matchedKeys.set(matched.getKeyIndex)
          numOutputRows += 1
          Some(joinedRow)
        } else {
          // Condition failed - emit with NULL build side
          numOutputRows += 1
          Some(joinedRow.withRight(buildNullRow))
        }
      } else {
        // No match - emit with NULL build side
        numOutputRows += 1
        Some(joinedRow.withRight(buildNullRow))
      }
    }
  }

  // Phase 2: Process unmatched build side rows
  val buildResultIter = hashedRelation.valuesWithKeyIndex().flatMap { valueRowWithKeyIndex =>
    if (!matchedKeys.get(valueRowWithKeyIndex.getKeyIndex)) {
      // This build row never matched - emit with NULL stream side
      numOutputRows += 1
      Some(streamNullJoinRowWithBuild(valueRowWithKeyIndex.getValue))
    } else {
      None
    }
  }

  // Concatenate both iterators
  streamResultIter ++ buildResultIter
}
```

**Algorithm**:
1. Iterate stream side, probe hash table, emit matches (or NULL if no match)
2. Track which build keys matched using BitSet
3. Iterate build side, emit unmatched rows with NULL stream side

### Metrics

**Lines 49-52**:
```scala
override lazy val metrics = Map(
  "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
  "buildDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size of build side"),
  "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map")
)
```

### When to Use

- **Best**: Build side 3x smaller than stream side, both sides too large to broadcast
- **Pros**: Works with larger data than broadcast, predictable memory per partition
- **Cons**: Requires shuffle on both sides, hash table construction overhead

---

## SortMergeJoinExec

**File**: `sql/core/src/main/scala/org/apache/spark/sql/execution/joins/SortMergeJoinExec.scala`

### Class Definition

**Lines 39-46**:
```scala
case class SortMergeJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean = false
) extends ShuffledJoin
```

### Required Ordering

**Lines 94-100**: Sort-merge join requires both sides to be sorted

```scala
override def requiredChildOrdering: Seq[Seq[SortOrder]] =
  requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
  // Must be ascending to agree with keyOrdering in doExecute()
  keys.map(SortOrder(_, Ascending))
}
```

Physical planning inserts `SortExec` operators to satisfy this requirement.

### Main Execution Logic

**Lines 124-153**:
```scala
protected override def doExecute(): RDD[InternalRow] = {
  // Create evaluator factory
  val evaluatorFactory = new SortMergeJoinEvaluatorFactory(
    leftKeys,
    rightKeys,
    joinType,
    condition,
    left,
    right,
    output,
    inMemoryThreshold,
    spillThreshold,
    spillSizeThreshold,
    numOutputRows,
    spillSize,
    onlyBufferFirstMatchedRow
  )

  // Use partition evaluator if enabled
  if (conf.usePartitionEvaluator) {
    left.execute().zipPartitionsWithEvaluator(right.execute(), evaluatorFactory)
  } else {
    // Standard zipPartitions
    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      val evaluator = evaluatorFactory.createEvaluator()
      evaluator.eval(0, leftIter, rightIter)
    }
  }
}
```

### Sort-Merge Join Scanner

**Lines 1105-1152**: Core algorithm for finding matching rows

```scala
final def findNextInnerJoinRows(): Boolean = {
  // Skip rows with null keys on streamed side
  while (advancedStreamed() && streamedRowKey.anyNull) {}

  if (streamedRow == null) {
    // Consumed entire streamed iterator
    matchJoinKey = null
    bufferedMatches.clear()
    return false
  } else if (matchJoinKey != null && keyOrdering.compare(streamedRowKey, matchJoinKey) == 0) {
    // Same key as previous match - reuse buffered matches
    return true
  } else if (bufferedRow == null) {
    // No more buffered rows
    matchJoinKey = null
    bufferedMatches.clear()
    return false
  } else {
    // Advance both sides to find matching keys
    var comp = keyOrdering.compare(streamedRowKey, bufferedRowKey)

    do {
      if (streamedRowKey.anyNull) {
        // Skip NULL keys on streamed side
        advancedStreamed()
      } else {
        assert(!bufferedRowKey.anyNull)
        comp = keyOrdering.compare(streamedRowKey, bufferedRowKey)

        if (comp > 0) {
          // Streamed key > buffered key - advance buffered
          advancedBufferedToRowWithNullFreeJoinKey()
        } else if (comp < 0) {
          // Streamed key < buffered key - advance streamed
          advancedStreamed()
        }
        // If comp == 0, we found a match
      }
    } while (streamedRow != null && bufferedRow != null && comp != 0)

    if (streamedRow == null || bufferedRow == null) {
      // One side exhausted
      return false
    } else {
      // Found matching keys - buffer all matching rows from buffered side
      bufferMatchingRows()
      return true
    }
  }
}
```

**Algorithm Steps**:
1. Maintain two iterators (streamed and buffered) over sorted inputs
2. Compare current keys using `keyOrdering.compare()`
3. If keys don't match, advance the side with smaller key
4. When keys match:
   - Buffer all rows with matching key from buffered side
   - Return matches for current streamed row
5. Advance streamed side, check if key still matches (reuse buffer)
6. When key changes, clear buffer and repeat

### Buffering Matching Rows

**Lines 1237-1252**: Buffer all rows with matching key from buffered side

```scala
private def bufferMatchingRows(): Unit = {
  // Save the current join key
  matchJoinKey = streamedRowKey.copy()
  bufferedMatches.clear()

  // Buffer all rows from buffered side with matching key
  do {
    if (!onlyBufferFirstMatch || bufferedMatches.isEmpty) {
      bufferedMatches.add(bufferedRow.asInstanceOf[UnsafeRow])
    }
    advancedBufferedToRowWithNullFreeJoinKey()
  } while (bufferedRow != null && keyOrdering.compare(streamedRowKey, bufferedRowKey) == 0)
}
```

**ExternalAppendOnlyUnsafeRowArray** (Lines 1081-1082):
```scala
private[this] val bufferedMatches: ExternalAppendOnlyUnsafeRowArray =
    new ExternalAppendOnlyUnsafeRowArray(inMemoryThreshold, spillThreshold, spillSizeThreshold)
```

This data structure can spill to disk if memory is exceeded, enabling sort-merge join to handle skewed keys with many duplicates.

### Full Outer Join Scanner

**Lines 1372-1547**: Special scanner for full outer joins

```scala
private class SortMergeFullOuterJoinScanner(
    leftKeyGenerator: Projection,
    rightKeyGenerator: Projection,
    keyOrdering: Ordering[InternalRow],
    leftIter: RowIterator,
    rightIter: RowIterator,
    boundCondition: InternalRow => Boolean,
    leftNullRow: InternalRow,
    rightNullRow: InternalRow) extends RowIterator {

  // Buffers for left and right matches
  private[this] val leftMatches: ArrayBuffer[InternalRow] = new ArrayBuffer[InternalRow]
  private[this] val rightMatches: ArrayBuffer[InternalRow] = new ArrayBuffer[InternalRow]

  // BitSets to track which rows matched
  private[this] var leftMatched: BitSet = new BitSet(1)
  private[this] var rightMatched: BitSet = new BitSet(1)

  private def findMatchingRows(matchingKey: InternalRow): Unit = {
    leftMatches.clear()
    rightMatches.clear()

    // Buffer all rows with matching key from left side
    while (leftRowKey != null && keyOrdering.compare(leftRowKey, matchingKey) == 0) {
      leftMatches += leftRow.copy()
      advancedLeft()
    }

    // Buffer all rows with matching key from right side
    while (rightRowKey != null && keyOrdering.compare(rightRowKey, matchingKey) == 0) {
      rightMatches += rightRow.copy()
      advancedRight()
    }

    // Reset bit sets to track which rows matched
    leftMatched.clearUntil(leftMatches.size)
    rightMatched.clearUntil(rightMatches.size)
  }

  private def scanNextInBuffered(): Boolean = {
    // Cartesian product of left and right matches
    while (leftIndex < leftMatches.size) {
      while (rightIndex < rightMatches.size) {
        joinedRow(leftMatches(leftIndex), rightMatches(rightIndex))
        if (boundCondition(joinedRow)) {
          // Matched - mark both sides
          leftMatched.set(leftIndex)
          rightMatched.set(rightIndex)
          rightIndex += 1
          return true
        }
        rightIndex += 1
      }

      // Emit left row with NULL if no match
      if (!leftMatched.get(leftIndex)) {
        joinedRow(leftMatches(leftIndex), rightNullRow)
        leftIndex += 1
        return true
      }
      leftIndex += 1
      rightIndex = 0
    }

    // Emit unmatched right rows with NULL
    while (rightIndex < rightMatches.size) {
      if (!rightMatched.get(rightIndex)) {
        joinedRow(leftNullRow, rightMatches(rightIndex))
        rightIndex += 1
        return true
      }
      rightIndex += 1
    }

    false
  }
}
```

**Full Outer Join Algorithm**:
1. Find next matching key on both sides
2. Buffer ALL rows with that key from BOTH sides
3. Compute cartesian product of left and right matches
4. Track which rows matched (BitSet)
5. Emit unmatched left rows with NULL right side
6. Emit unmatched right rows with NULL left side

### Metrics

**Lines 48-50**:
```scala
override lazy val metrics = Map(
  "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
  "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size")
)
```

### When to Use

- **Best**: Large tables with even distribution, or when sort is free (already sorted)
- **Pros**: Scalable, predictable memory (O(1) streaming), can spill to disk
- **Cons**: Sort overhead O(n log n), not optimal for small tables

---

## BroadcastNestedLoopJoinExec

**File**: `sql/core/src/main/scala/org/apache/spark/sql/execution/joins/BroadcastNestedLoopJoinExec.scala`

### Class Definition

**Lines 34-39**:
```scala
case class BroadcastNestedLoopJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    buildSide: BuildSide,
    joinType: JoinType,
    condition: Option[Expression]
) extends JoinCodegenSupport
```

**Key Difference**: No `leftKeys` or `rightKeys` - this join works without equi-join conditions.

### Inner Join Implementation

**Lines 120-134**:
```scala
private def innerJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
  streamed.execute().mapPartitionsInternal { streamedIter =>
    val buildRows = relation.value
    val joinedRow = new JoinedRow

    // Nested loop: for each streamed row, iterate all build rows
    streamedIter.flatMap { streamedRow =>
      val joinedRows = buildRows.iterator.map(r => joinedRow(streamedRow, r))

      if (condition.isDefined) {
        // Filter by condition
        joinedRows.filter(boundCondition)
      } else {
        // No condition - return all pairs
        joinedRows
      }
    }
  }
}
```

**Algorithm**: Simple nested loop
```
for each row s in streamed:
  for each row b in broadcast:
    if condition(s, b):
      emit (s, b)
```

### Outer Join Implementation

**Lines 146-207**: More complex due to tracking matches

```scala
private def outerJoin(
    relation: Broadcast[Array[InternalRow]],
    singleJoin: Boolean = false): RDD[InternalRow] = {

  streamed.execute().mapPartitionsInternal { streamedIter =>
    val buildRows = relation.value
    val joinedRow = new JoinedRow
    val nulls = new GenericInternalRow(broadcast.output.size)

    new Iterator[InternalRow] {
      private var streamRow: InternalRow = null
      private var foundMatch: Boolean = false
      private var resultRow: InternalRow = null
      private var nextIndex: Int = 0

      @tailrec
      private def findNextMatch(): Boolean = {
        if (streamRow == null) {
          if (!streamedIter.hasNext) return false
          streamRow = streamedIter.next()
          nextIndex = 0
          foundMatch = false
        }

        // Iterate through build rows looking for match
        while (nextIndex < buildRows.length) {
          resultRow = joinedRow(streamRow, buildRows(nextIndex))
          nextIndex += 1

          if (boundCondition(resultRow)) {
            if (foundMatch && singleJoin) {
              // Scalar subquery should return only one row
              throw QueryExecutionErrors.scalarSubqueryReturnsMultipleRows()
            }
            foundMatch = true
            return true
          }
        }

        // Finished iterating build side for this streamed row
        if (!foundMatch) {
          // No match found - emit with NULL build side
          resultRow = joinedRow(streamRow, nulls)
          streamRow = null
          return true
        } else {
          // Found match - move to next streamed row
          resultRow = null
          streamRow = null
          findNextMatch()
        }
      }

      override def hasNext: Boolean = resultRow != null || findNextMatch()

      override def next(): InternalRow = {
        val r = resultRow
        resultRow = null
        r
      }
    }
  }
}
```

**Outer Join Algorithm**:
1. For each streamed row:
   - Iterate all build rows
   - Emit matches (with condition check)
   - Track if any match found
2. If no match found, emit streamed row with NULL build side
3. Move to next streamed row

### When to Use

- **Best**: Non-equi joins (e.g., `a.x > b.y`), very small build side
- **Pros**: Supports any join condition, no shuffle required
- **Cons**: O(n × m) time complexity, very slow for large data

---

## CartesianProductExec

**File**: `sql/core/src/main/scala/org/apache/spark/sql/execution/joins/CartesianProductExec.scala`

### Class Definition

**Lines 62-70**:
```scala
case class CartesianProductExec(
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression]
) extends BaseJoinExec {

  override def joinType: JoinType = Inner
  override def leftKeys: Seq[Expression] = Nil
  override def rightKeys: Seq[Expression] = Nil
```

### UnsafeCartesianRDD

**Lines 35-59**: Specialized RDD for cartesian product with spill support

```scala
class UnsafeCartesianRDD(
    left: RDD[UnsafeRow],
    right: RDD[UnsafeRow],
    inMemoryBufferThreshold: Int,
    spillThreshold: Int,
    spillSizeThreshold: Long)
  extends CartesianRDD[UnsafeRow, UnsafeRow](left.sparkContext, left, right) {

  override def compute(split: Partition, context: TaskContext): Iterator[(UnsafeRow, UnsafeRow)] = {
    // Create array that can spill to disk
    val rowArray = new ExternalAppendOnlyUnsafeRowArray(
      inMemoryBufferThreshold,
      spillThreshold,
      spillSizeThreshold)

    val partition = split.asInstanceOf[CartesianPartition]

    // Buffer entire right partition into array
    rdd2.iterator(partition.s2, context).foreach(rowArray.add)

    // Create iterator factory for repeated iteration
    def createIter(): Iterator[UnsafeRow] = rowArray.generateIterator()

    // Cartesian product: for each left row, iterate all right rows
    val resultIter =
      for (x <- rdd1.iterator(partition.s1, context);
           y <- createIter()) yield (x, y)

    CompletionIterator(resultIter, rowArray.clear())
  }
}
```

**Algorithm**:
1. Buffer entire right partition in `ExternalAppendOnlyUnsafeRowArray`
2. For each left row, iterate all buffered right rows
3. Array can spill to disk if memory exceeded

### Main Execution

**Lines 76-106**:
```scala
protected override def doExecute(): RDD[InternalRow] = {
  val leftResults = left.execute().asInstanceOf[RDD[UnsafeRow]]
  val rightResults = right.execute().asInstanceOf[RDD[UnsafeRow]]

  // Create cartesian product RDD
  val pair = new UnsafeCartesianRDD(
    leftResults,
    rightResults,
    conf.cartesianProductExecBufferInMemoryThreshold,
    conf.cartesianProductExecBufferSpillThreshold,
    conf.cartesianProductExecBufferSizeSpillThreshold)

  pair.mapPartitionsWithIndexInternal { (index, iter) =>
    val joiner = GenerateUnsafeRowJoiner.create(left.schema, right.schema)
    val numOutputRows = longMetric("numOutputRows")

    // Apply condition filter if exists
    val filtered = if (condition.isDefined) {
      val boundCondition = Predicate.create(condition.get, left.output ++ right.output)
      boundCondition.initialize(index)
      val joined = new JoinedRow

      iter.filter { r =>
        boundCondition.eval(joined(r._1, r._2))
      }
    } else {
      iter
    }

    // Emit joined rows
    filtered.map { r =>
      numOutputRows += 1
      joiner.join(r._1, r._2)
    }
  }
}
```

### When to Use

- **Best**: CROSS JOIN with very small tables
- **Pros**: Simple, supports any condition
- **Cons**: O(n × m) output size, can explode to huge result

---

## Build Side Selection

**File**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/joins.scala`

### Core Build Side Selection

**Lines 514-530**:
```scala
private def getBuildSide(
    canBuildLeft: Boolean,
    canBuildRight: Boolean,
    left: LogicalPlan,
    right: LogicalPlan): Option[BuildSide] = {

  if (canBuildLeft && canBuildRight) {
    // Both sides can be built - choose smaller side
    Some(getSmallerSide(left, right))
  } else if (canBuildLeft) {
    Some(BuildLeft)
  } else if (canBuildRight) {
    Some(BuildRight)
  } else {
    None  // Neither side can be built
  }
}
```

**Lines 360-362**: Smaller side determination
```scala
def getSmallerSide(left: LogicalPlan, right: LogicalPlan): BuildSide = {
  if (right.stats.sizeInBytes <= left.stats.sizeInBytes) BuildRight else BuildLeft
}
```

### Broadcast Hash Join Build Side Selection

**Lines 292-316**:
```scala
def getBroadcastBuildSide(
    join: Join,
    hintOnly: Boolean,
    conf: SQLConf): Option[BuildSide] = {

  def shouldBuildLeft(): Boolean = {
    if (hintOnly) {
      // Only consider hints
      hintToBroadcastLeft(join.hint)
    } else {
      // Check size + hints
      canBroadcastBySize(join.left, conf) && !hintToNotBroadcastLeft(join.hint)
    }
  }

  def shouldBuildRight(): Boolean = {
    if (hintOnly) {
      hintToBroadcastRight(join.hint)
    } else {
      canBroadcastBySize(join.right, conf) && !hintToNotBroadcastRight(join.hint)
    }
  }

  getBuildSide(
    canBuildBroadcastLeft(join.joinType) && shouldBuildLeft(),
    canBuildBroadcastRight(join.joinType) && shouldBuildRight(),
    join.left,
    join.right
  )
}
```

**Lines 227-229**: Size check
```scala
def canBroadcastBySize(plan: LogicalPlan, conf: SQLConf): Boolean = {
  plan.stats.sizeInBytes <= conf.autoBroadcastJoinThreshold
}
```

### Join Type Constraints

**Lines 377-389**: Which sides can be broadcast for each join type

```scala
def canBuildBroadcastLeft(joinType: JoinType): Boolean = {
  joinType match {
    case _: InnerLike | RightOuter => true
    case _ => false
  }
}

def canBuildBroadcastRight(joinType: JoinType): Boolean = {
  joinType match {
    case _: InnerLike | LeftOuter | LeftSingle | LeftSemi | LeftAnti | _: ExistenceJoin => true
    case _ => false
  }
}
```

**Reasoning**:
- **LeftOuter**: Must build right (need to preserve all left rows)
- **RightOuter**: Must build left (need to preserve all right rows)
- **FullOuter**: Cannot use broadcast hash join (need to preserve both sides)
- **Inner**: Can build either side

### Shuffled Hash Join Build Side Selection

**Lines 318-348**:
```scala
def getShuffleHashJoinBuildSide(
    join: Join,
    hintOnly: Boolean,
    conf: SQLConf): Option[BuildSide] = {

  def shouldBuildLeft(): Boolean = {
    if (hintOnly) {
      hintToShuffleHashJoinLeft(join.hint)
    } else {
      hintToPreferShuffleHashJoinLeft(join.hint) ||
        (!conf.preferSortMergeJoin &&
         canBuildLocalHashMapBySize(join.left, conf) &&
         muchSmaller(join.left, join.right, conf)) ||
        forceApplyShuffledHashJoin(conf)
    }
  }

  // Similar for shouldBuildRight...

  getBuildSide(
    canBuildShuffledHashJoinLeft(join.joinType) && shouldBuildLeft(),
    canBuildShuffledHashJoinRight(join.joinType) && shouldBuildRight(),
    join.left,
    join.right
  )
}
```

**Lines 550-552**: "Much smaller" heuristic
```scala
private def muchSmaller(a: LogicalPlan, b: LogicalPlan, conf: SQLConf): Boolean = {
  a.stats.sizeInBytes * conf.getConf(SQLConf.SHUFFLE_HASH_JOIN_FACTOR) <= b.stats.sizeInBytes
}
```

Default `SHUFFLE_HASH_JOIN_FACTOR` is 3.0, meaning build side must be 3x smaller than stream side.

---

## Hash Join Details

### HashedRelation Interface

**File**: `sql/core/src/main/scala/org/apache/spark/sql/execution/joins/HashedRelation.scala`

**Lines 41-122**:
```scala
private[execution] sealed trait HashedRelation extends KnownSizeEstimation {
  // Lookup methods
  def get(key: InternalRow): Iterator[InternalRow]
  def get(key: Long): Iterator[InternalRow]
  def getValue(key: InternalRow): InternalRow  // For unique keys
  def getValue(key: Long): InternalRow

  // Lookup with key index (for tracking matches in outer joins)
  def getWithKeyIndex(key: InternalRow): Iterator[ValueRowWithKeyIndex]
  def getValueWithKeyIndex(key: InternalRow): ValueRowWithKeyIndex
  def valuesWithKeyIndex(): Iterator[ValueRowWithKeyIndex]

  // Properties
  def keyIsUnique: Boolean
  def maxNumKeysIndex: Int
  def keys(): Iterator[InternalRow]

  // Memory management
  def estimatedSize: Long
  def asReadOnlyCopy(): HashedRelation
  def close(): Unit
}
```

### UnsafeHashedRelation

**Lines 207-211**: Implementation using Tungsten BytesToBytesMap

```scala
private[joins] class UnsafeHashedRelation(
    private var numKeys: Int,
    private var numFields: Int,
    private var binaryMap: BytesToBytesMap  // Tungsten off-heap hash map
) extends HashedRelation
```

**Lines 229-248**: Lookup implementation

```scala
override def get(key: InternalRow): Iterator[InternalRow] = {
  val unsafeKey = key.asInstanceOf[UnsafeRow]
  val map = binaryMap
  val loc = new map.Location

  // Lookup in BytesToBytesMap
  binaryMap.safeLookup(
    unsafeKey.getBaseObject,
    unsafeKey.getBaseOffset,
    unsafeKey.getSizeInBytes,
    loc,
    unsafeKey.hashCode())

  if (loc.isDefined) {
    // Found - create iterator over collision chain
    new Iterator[UnsafeRow] {
      private var _hasNext = true

      override def hasNext: Boolean = _hasNext

      override def next(): UnsafeRow = {
        resultRow.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
        _hasNext = loc.nextValue()  // Advance to next value in collision chain
        resultRow
      }
    }
  } else {
    null  // Not found
  }
}
```

**Collision Handling**: Uses chaining - `loc.nextValue()` advances to next value with same key.

**Lines 453-498**: Construction from iterator

```scala
def apply(
    input: Iterator[InternalRow],
    key: Seq[Expression],
    sizeEstimate: Long,
    taskMemoryManager: TaskMemoryManager,
    allowsNullKey: Boolean,
    ignoresDuplicatedKey: Boolean): HashedRelation = {

  // Create BytesToBytesMap with estimated size
  val binaryMap = new BytesToBytesMap(
    taskMemoryManager,
    (sizeEstimate * 1.5 + 1).toInt,
    pageSizeBytes)

  val keyGenerator = UnsafeProjection.create(key)
  var numFields = 0

  // Insert all rows
  while (input.hasNext) {
    val row = input.next().asInstanceOf[UnsafeRow]
    numFields = row.numFields()
    val key = keyGenerator(row)

    if (!key.anyNull || allowsNullKey) {
      // Lookup or insert
      val loc = binaryMap.lookup(
        key.getBaseObject,
        key.getBaseOffset,
        key.getSizeInBytes)

      if (!(ignoresDuplicatedKey && loc.isDefined)) {
        // Append value
        val success = loc.append(
          key.getBaseObject, key.getBaseOffset, key.getSizeInBytes,
          row.getBaseObject, row.getBaseOffset, row.getSizeInBytes)

        if (!success) {
          // Failed to allocate memory
          binaryMap.free()
          throw QueryExecutionErrors.cannotAcquireMemoryToBuildUnsafeHashedRelationError()
        }
      }
    }
  }

  new UnsafeHashedRelation(key.size, numFields, binaryMap)
}
```

### LongHashedRelation

**Lines 534-989**: Specialized hash map for Long keys

**Data Structures** (Lines 500-567):
```scala
// Two modes: sparse and dense

// Sparse mode: array of [key][address] pairs
// [key1, addr1, key2, addr2, ...]
private var array: Array[Long] = null

// Dense mode: array indexed by (key - minKey)
// [addr_for_minKey, addr_for_minKey+1, ...]

// Page stores actual row data
private var page: Array[Long] = null
```

**Sparse Mode Insertion** (Lines 724-772):
```scala
def insertSparse(key: Long, value: UnsafeRow): Unit = {
  var pos = firstSlot(key)
  var step = 2

  // Quadratic probing
  while (array(pos + 1) != 0) {
    if (array(pos) == key) {
      // Key exists - append to collision chain
      insertCollision(array(pos + 1).toInt, value)
      return
    }
    pos = (pos + step) & mask
    step += 2
  }

  // Empty slot found
  array(pos) = key
  array(pos + 1) = insertNewValue(value)
  numKeys += 1
}
```

**Collision Handling** (Lines 624-632): Quadratic probing

```scala
private def firstSlot(key: Long): Int = {
  // Golden ratio hash for good distribution
  val h = key * 0x9E3779B9L
  (h ^ (h >> 32)).toInt & mask
}

private def nextSlot(pos: Int): Int = (pos + 2) & mask
```

**Dense Mode Optimization** (Lines 864-890):
```scala
def optimize(): Unit = {
  val range = maxKey - minKey

  // Convert to dense mode if range is small
  if (range >= 0 && (range < array.length || range < 1024)) {
    val denseArray = new Array[Long]((range + 1).toInt)

    // Copy from sparse to dense
    var i = 0
    while (i < array.length) {
      if (array(i + 1) > 0) {
        val idx = (array(i) - minKey).toInt
        denseArray(idx) = array(i + 1)
      }
      i += 2
    }

    array = denseArray
    isDense = true
  }
}
```

**Dense Mode Lookup** (Lines 791-802): O(1) array access
```scala
def getValueDense(key: Long): InternalRow = {
  if (key >= minKey && key <= maxKey) {
    val idx = (key - minKey).toInt
    val address = array(idx)
    if (address > 0) {
      return getRowFromPage(address.toInt)
    }
  }
  null
}
```

**When LongHashedRelation is Used**:
```scala
// From HashJoin.scala lines 727-755
object HashJoin {
  def rewriteKeyExpr(keys: Seq[Expression]): Seq[Expression] = {
    if (!canRewriteAsLongType(keys)) {
      return keys
    }

    // Pack all integral keys into single Long using bit operations
    var keyExpr: Expression = cast(keys.head, LongType)
    keys.tail.foreach { e =>
      val bits = e.dataType.defaultSize * 8
      keyExpr = BitwiseOr(
        ShiftLeft(keyExpr, Literal(bits)),
        BitwiseAnd(cast(e, LongType), Literal((1L << bits) - 1)))
    }
    keyExpr :: Nil
  }

  private def canRewriteAsLongType(keys: Seq[Expression]): Boolean = {
    keys.forall(_.dataType.isInstanceOf[IntegralType]) &&
      keys.map(_.dataType.defaultSize).sum <= 8
  }
}
```

**Example**: Join on `(ByteType, IntType, ShortType)` → packed into single `LongType`

---

## Sort Merge Join Details

### Sorting Implementation

Sort-merge join doesn't sort itself - it declares ordering requirements and physical planning inserts `SortExec` operators.

**Lines 94-100**:
```scala
override def requiredChildOrdering: Seq[Seq[SortOrder]] =
  requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
  // Must be ascending to agree with keyOrdering in doExecute()
  keys.map(SortOrder(_, Ascending))
}
```

### Key Comparison

**Lines 115-121**: Create ordering for comparing join keys
```scala
private def createKeyOrdering(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    output: Seq[Attribute]): Ordering[InternalRow] = {

  // Use RowOrdering for comparing rows based on join keys
  RowOrdering.create(leftKeys, output)
}
```

### Merge Algorithm State

**Lines 1059-1082**: Scanner maintains state across iterations

```scala
private[this] var streamedRow: InternalRow = _
private[this] var streamedRowKey: InternalRow = _
private[this] var bufferedRow: InternalRow = _
private[this] var bufferedRowKey: InternalRow = _
private[this] var matchJoinKey: InternalRow = _  // Current matching key

// Buffer for rows with matching key
private[this] val bufferedMatches: ExternalAppendOnlyUnsafeRowArray =
    new ExternalAppendOnlyUnsafeRowArray(
      inMemoryThreshold,
      spillThreshold,
      spillSizeThreshold)

// Iterators
private[this] val streamedIter: RowIterator = ...
private[this] val bufferedIter: RowIterator = ...

// Key ordering
private[this] val keyOrdering: Ordering[InternalRow] = ...
```

### Outer Join Iterator

**Lines 1302-1370**: OneSideOuterIterator handles left/right outer joins

```scala
private abstract class OneSideOuterIterator(
    smjScanner: SortMergeJoinScanner,
    bufferedSideNullRow: InternalRow,
    filter: InternalRow => Boolean) extends RowIterator {

  protected[this] val joinedRow: JoinedRow = new JoinedRow()
  private[this] var rightMatchesIterator: Iterator[UnsafeRow] = null

  private def advanceStream(): Boolean = {
    rightMatchesIterator = null

    if (smjScanner.findNextOuterJoinRows()) {
      setStreamSideOutput(smjScanner.getStreamedRow)

      if (smjScanner.getBufferedMatches.isEmpty) {
        // No matches - return NULL row
        setBufferedSideOutput(bufferedSideNullRow)
        return true
      } else {
        // Find first match satisfying condition
        rightMatchesIterator = smjScanner.getBufferedMatches.generateIterator()
        if (!advanceBufferUntilBoundConditionSatisfied()) {
          // No match satisfies condition - return NULL row
          setBufferedSideOutput(bufferedSideNullRow)
        }
        return true
      }
    } else {
      false
    }
  }

  private def advanceBufferUntilBoundConditionSatisfied(): Boolean = {
    var found = false
    while (!found && rightMatchesIterator.hasNext) {
      setBufferedSideOutput(rightMatchesIterator.next())
      found = filter(getJoinedRow())
    }
    found
  }

  override def advanceNext(): Boolean = {
    if (rightMatchesIterator != null && advanceBufferUntilBoundConditionSatisfied()) {
      true
    } else {
      advanceStream()
    }
  }
}
```

**Outer Join Algorithm**:
1. Find next streamed row with matches (or no matches)
2. If no matches, emit with NULL buffered side
3. If has matches, iterate through buffered matches
4. Emit all matches that satisfy condition
5. If no match satisfies condition, emit with NULL buffered side

---

## Join Iterators

### Inner Join Iterator

**From HashJoin.scala (Lines 161-190)**:

```scala
private def innerJoin(
    streamIter: Iterator[InternalRow],
    hashedRelation: HashedRelation,
    numOutputRows: SQLMetric): Iterator[InternalRow] = {

  val joinRow = new JoinedRow
  val joinKeys = streamSideKeyGenerator()

  if (hashedRelation == EmptyHashedRelation) {
    // Empty build side - return nothing
    Iterator.empty
  } else if (hashedRelation.keyIsUnique) {
    // Unique keys - direct lookup (no iteration over matches)
    streamIter.flatMap { srow =>
      joinRow.withLeft(srow)
      val matched = hashedRelation.getValue(joinKeys(srow))

      if (matched != null) {
        // Found match
        val result = joinRow.withRight(matched)
        if (condition.isEmpty || boundCondition(result)) {
          numOutputRows += 1
          Some(result)
        } else {
          None
        }
      } else {
        None
      }
    }
  } else {
    // Non-unique keys - iterate through all matches
    streamIter.flatMap { srow =>
      joinRow.withLeft(srow)
      val matches = hashedRelation.get(joinKeys(srow))

      if (matches != null) {
        matches.flatMap { matched =>
          val result = joinRow.withRight(matched)
          if (condition.isEmpty || boundCondition(result)) {
            numOutputRows += 1
            Some(result)
          } else {
            None
          }
        }
      } else {
        Seq.empty
      }
    }
  }
}
```

### Outer Join Iterator

**From HashJoin.scala (Lines 192-240)**:

```scala
private def outerJoin(
    streamedIter: Iterator[InternalRow],
    hashedRelation: HashedRelation,
    numOutputRows: SQLMetric,
    singleJoin: Boolean = false): Iterator[InternalRow] = {

  val joinedRow = new JoinedRow()
  val keyGenerator = streamSideKeyGenerator()
  val nullRow = new GenericInternalRow(buildPlan.output.length)

  if (hashedRelation.keyIsUnique) {
    // Unique keys - simpler logic
    streamedIter.map { currentRow =>
      val rowKey = keyGenerator(currentRow)
      joinedRow.withLeft(currentRow)
      val matched = hashedRelation.getValue(rowKey)

      if (matched != null && (condition.isEmpty || boundCondition(joinedRow.withRight(matched)))) {
        // Found matching row
        numOutputRows += 1
        joinedRow
      } else {
        // No match - emit with NULL build side
        numOutputRows += 1
        joinedRow.withRight(nullRow)
      }
    }
  } else {
    // Non-unique keys - must track if any match found
    streamedIter.flatMap { currentRow =>
      val rowKey = keyGenerator(currentRow)
      joinedRow.withLeft(currentRow)
      val buildIter = hashedRelation.get(rowKey)

      new RowIterator {
        private var found = false

        override def advanceNext(): Boolean = {
          // Iterate through build matches
          while (buildIter != null && buildIter.hasNext) {
            val nextBuildRow = buildIter.next()

            if (condition.isEmpty || boundCondition(joinedRow.withRight(nextBuildRow))) {
              if (found && singleJoin) {
                // Scalar subquery returned multiple rows - error
                throw QueryExecutionErrors.scalarSubqueryReturnsMultipleRows()
              }
              found = true
              numOutputRows += 1
              return true
            }
          }

          // Finished iterating build side
          if (!found) {
            // No match found - emit with NULL
            joinedRow.withRight(nullRow)
            found = true
            numOutputRows += 1
            return true
          }

          false
        }

        override def getRow: InternalRow = joinedRow
      }.toScala
    }
  }
}
```

### Semi Join Iterator

**From HashJoin.scala (Lines 242-266)**:

```scala
private def semiJoin(
    streamIter: Iterator[InternalRow],
    hashedRelation: HashedRelation,
    numOutputRows: SQLMetric): Iterator[InternalRow] = {

  val joinKeys = streamSideKeyGenerator()
  val joinedRow = new JoinedRow

  if (hashedRelation == EmptyHashedRelation) {
    // Empty build side - return nothing
    Iterator.empty
  } else if (hashedRelation.keyIsUnique) {
    // Unique keys - simple existence check
    streamIter.filter { current =>
      val key = joinKeys(current)
      lazy val matched = hashedRelation.getValue(key)

      // Keep if key is not NULL and match exists and condition passes
      !key.anyNull && matched != null &&
        (condition.isEmpty || boundCondition(joinedRow(current, matched)))
    }.map { row =>
      numOutputRows += 1
      row
    }
  } else {
    // Non-unique keys - check if any match satisfies condition
    streamIter.filter { current =>
      val key = joinKeys(current)
      lazy val buildIter = hashedRelation.get(key)

      !key.anyNull && buildIter != null && (condition.isEmpty || buildIter.exists {
        (row: InternalRow) => boundCondition(joinedRow(current, row))
      })
    }.map { row =>
      numOutputRows += 1
      row
    }
  }
}
```

**Semi Join**: Returns only streamed rows that have at least one match on build side.

### Anti Join Iterator

**From HashJoin.scala (Lines 297-324)**:

```scala
private def antiJoin(
    streamIter: Iterator[InternalRow],
    hashedRelation: HashedRelation,
    numOutputRows: SQLMetric): Iterator[InternalRow] = {

  // Special case: if build side is empty, return all streamed rows
  if (hashedRelation == EmptyHashedRelation) {
    return streamIter.map { row =>
      numOutputRows += 1
      row
    }
  }

  val joinKeys = streamSideKeyGenerator()
  val joinedRow = new JoinedRow

  if (hashedRelation.keyIsUnique) {
    streamIter.filter { current =>
      val key = joinKeys(current)
      lazy val matched = hashedRelation.getValue(key)

      // Keep if key is NULL or no match or condition fails
      key.anyNull || matched == null ||
        (condition.isDefined && !boundCondition(joinedRow(current, matched)))
    }.map { row =>
      numOutputRows += 1
      row
    }
  } else {
    streamIter.filter { current =>
      val key = joinKeys(current)
      lazy val buildIter = hashedRelation.get(key)

      key.anyNull || buildIter == null || (condition.isDefined && !buildIter.exists {
        row => boundCondition(joinedRow(current, row))
      })
    }.map { row =>
      numOutputRows += 1
      row
    }
  }
}
```

**Anti Join**: Returns streamed rows that have NO match on build side (or condition fails for all matches).

### Existence Join Iterator

**From HashJoin.scala (Lines 268-295)**:

```scala
private def existenceJoin(
    streamIter: Iterator[InternalRow],
    hashedRelation: HashedRelation,
    numOutputRows: SQLMetric): Iterator[InternalRow] = {

  val joinKeys = streamSideKeyGenerator()
  val result = new GenericInternalRow(Array[Any](null))
  val joinedRow = new JoinedRow

  if (hashedRelation.keyIsUnique) {
    streamIter.map { current =>
      val key = joinKeys(current)
      lazy val matched = hashedRelation.getValue(key)

      // Compute existence flag
      val exists = !key.anyNull && matched != null &&
        (condition.isEmpty || boundCondition(joinedRow(current, matched)))

      result.setBoolean(0, exists)
      numOutputRows += 1
      joinedRow(current, result)
    }
  } else {
    streamIter.map { current =>
      val key = joinKeys(current)
      lazy val buildIter = hashedRelation.get(key)

      val exists = !key.anyNull && buildIter != null && (condition.isEmpty || buildIter.exists {
        (row: InternalRow) => boundCondition(joinedRow(current, row))
      })

      result.setBoolean(0, exists)
      numOutputRows += 1
      joinedRow(current, result)
    }
  }
}
```

**Existence Join**: Used for `EXISTS` subqueries. Returns all streamed rows with extra boolean column indicating if match exists.

---

## Code Generation

### Hash Join Code Generation

**From HashJoin.scala (Lines 395-444)**: Inner join codegen

```scala
protected def codegenInner(ctx: CodegenContext, input: Seq[ExprCode]): String = {
  val HashedRelationInfo(relationTerm, keyIsUnique, isEmptyHashedRelation) =
    prepareRelation(ctx)

  val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
  val (matched, checkCondition, buildVars) = getJoinCondition(ctx, input, streamedPlan, buildPlan)
  val numOutput = metricTerm(ctx, "numOutputRows")

  val resultVars = buildSide match {
    case BuildLeft => buildVars ++ input
    case BuildRight => input ++ buildVars
  }

  if (isEmptyHashedRelation) {
    // Hash relation is empty - return nothing
    """
      |// If HashedRelation is empty, hash inner join simply returns nothing.
    """.stripMargin
  } else if (keyIsUnique) {
    // Unique keys - direct lookup
    s"""
       |// generate join key for stream side
       |${keyEv.code}
       |// find matches from HashedRelation
       |UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm.getValue(${keyEv.value});
       |if ($matched != null) {
       |  $checkCondition {
       |    $numOutput.add(1);
       |    ${consume(ctx, resultVars)}
       |  }
       |}
     """.stripMargin
  } else {
    // Non-unique keys - iterate through matches
    val matches = ctx.freshName("matches")
    val iteratorCls = classOf[Iterator[UnsafeRow]].getName

    s"""
       |// generate join key for stream side
       |${keyEv.code}
       |// find matches from HashRelation
       |$iteratorCls $matches = $anyNull ? null : ($iteratorCls)$relationTerm.get(${keyEv.value});
       |if ($matches != null) {
       |  while ($matches.hasNext()) {
       |    UnsafeRow $matched = (UnsafeRow) $matches.next();
       |    $checkCondition {
       |      $numOutput.add(1);
       |      ${consume(ctx, resultVars)}
       |    }
       |  }
       |}
     """.stripMargin
  }
}
```

### Generated Code Example

For inner join with unique keys:

```java
// Simplified generated code
public void processNext() throws java.io.IOException {
  while (streamedInput.hasNext()) {
    InternalRow streamedRow = (InternalRow) streamedInput.next();

    // Generate join key
    boolean streamedKeyIsNull = streamedRow.isNullAt(0);
    long streamedKeyValue = streamedKeyIsNull ? -1L : streamedRow.getLong(0);

    // Lookup in hash relation
    UnsafeRow matched = streamedKeyIsNull ? null :
        (UnsafeRow) relation.getValue(streamedKeyValue);

    if (matched != null) {
      // Check condition if exists
      boolean conditionPassed = true;
      if (hasCondition) {
        int buildValue = matched.getInt(1);
        int streamValue = streamedRow.getInt(1);
        conditionPassed = (buildValue > streamValue);
      }

      if (conditionPassed) {
        numOutputRows.add(1);

        // Emit joined row (avoiding boxing)
        streamedRow.writeToBuffer(buffer);
        matched.writeToBuffer(buffer);
        output.emit(buffer);
      }
    }
  }
}
```

**Key Optimizations**:
- No boxing/unboxing of primitive types
- Inline condition evaluation
- Direct buffer writes
- No JoinedRow object allocation

### Sort-Merge Join Code Generation

**From SortMergeJoinExec.scala (Lines 447-609)**: doProduce method

```scala
override def doProduce(ctx: CodegenContext): String = {
  if (joinType == FullOuter) {
    return codegenFullOuter(ctx)
  }

  // Generate scanner function to find matching rows
  val (findNextJoinRowsFuncName, streamedRow, matches) = genScanner(ctx)

  // Generate variables for streamed and buffered rows
  val (streamedVars, streamedVarDecl) = createStreamedVars(ctx, streamedRow)
  val bufferedRow = ctx.freshName("bufferedRow")
  val bufferedVars = genOneSideJoinVars(ctx, bufferedRow, bufferedPlan, setDefaultValue = true)

  val iterator = ctx.freshName("iterator")
  val numOutput = metricTerm(ctx, "numOutputRows")

  // Generate appropriate loop based on join type
  joinType match {
    case _: InnerLike =>
      codegenInner(findNextJoinRowsFuncName, streamedVarDecl, streamedVars,
                   bufferedRow, bufferedVars, iterator, numOutput)

    case LeftOuter | RightOuter =>
      codegenOuter(findNextJoinRowsFuncName, streamedVarDecl, streamedVars,
                   bufferedRow, bufferedVars, iterator, numOutput)

    // ... other join types
  }
}
```

**Generated Scanner Function** (simplified):

```java
// Generated function to find next matching rows
private boolean findNextJoinRows_0(
    scala.collection.Iterator streamedIter,
    scala.collection.Iterator bufferedIter) {

  streamedRow_0 = null;
  int comp = 0;

  while (streamedRow_0 == null) {
    if (!streamedIter.hasNext()) return false;
    streamedRow_0 = (InternalRow) streamedIter.next();

    // Generate join keys
    boolean streamedKeyIsNull = streamedRow_0.isNullAt(0);
    long streamedKey = streamedKeyIsNull ? -1L : streamedRow_0.getLong(0);

    if (streamedKeyIsNull) continue;  // Skip NULL keys

    // Check if same key as before (reuse matches)
    if (matchedKey_0 != null && streamedKey == matchedKey_0) {
      return true;
    }

    matches_0.clear();

    // Scan buffered side to find matching rows
    do {
      if (bufferedRow_0 == null) {
        if (!bufferedIter.hasNext()) {
          return !matches_0.isEmpty();
        }
        bufferedRow_0 = (InternalRow) bufferedIter.next();

        boolean bufferedKeyIsNull = bufferedRow_0.isNullAt(0);
        long bufferedKey = bufferedKeyIsNull ? -1L : bufferedRow_0.getLong(0);

        if (bufferedKeyIsNull) {
          bufferedRow_0 = null;
          continue;
        }
      }

      // Compare keys
      comp = Long.compare(streamedKey, bufferedKey);

      if (comp > 0) {
        bufferedRow_0 = null;  // Advance buffered
      } else if (comp < 0) {
        if (!matches_0.isEmpty()) return true;
        streamedRow_0 = null;  // Advance streamed
      } else {
        // Keys match - add to buffer
        matches_0.add((UnsafeRow) bufferedRow_0);
        bufferedRow_0 = null;
      }
    } while (streamedRow_0 != null);
  }

  return false;
}
```

---

## Metrics and Statistics

### BroadcastHashJoinExec Metrics

**Lines 59-60**:
```scala
override lazy val metrics = Map(
  "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows")
)
```

### ShuffledHashJoinExec Metrics

**Lines 49-52**:
```scala
override lazy val metrics = Map(
  "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
  "buildDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size of build side"),
  "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map")
)
```

### SortMergeJoinExec Metrics

**Lines 48-50**:
```scala
override lazy val metrics = Map(
  "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
  "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size")
)
```

### Reading Metrics in Spark UI

Metrics appear in the SQL tab under each operator:

1. **numOutputRows**: Total rows produced by join
   - Helps understand join cardinality
   - Inner join: ≤ left × right
   - Outer join: ≥ max(left, right)

2. **buildDataSize**: Memory used by hash table (shuffled hash join)
   - Should be much smaller than streamed side
   - If too large, consider sort-merge join

3. **buildTime**: Time spent building hash table
   - High value indicates CPU-intensive hash table construction
   - May benefit from more parallelism

4. **spillSize**: Data spilled to disk (sort-merge join)
   - Indicates memory pressure
   - High value suggests increasing executor memory
   - Only applies to buffering duplicate keys

---

## Special Cases

### Empty Table Handling

**EmptyHashedRelation** (Lines 1101-1121):

```scala
case object EmptyHashedRelation extends HashedRelation {
  override def get(key: Long): Iterator[InternalRow] = null
  override def get(key: InternalRow): Iterator[InternalRow] = null
  override def getValue(key: Long): InternalRow = null
  override def getValue(key: InternalRow): InternalRow = null
  override def getWithKeyIndex(key: InternalRow): Iterator[ValueRowWithKeyIndex] = null
  override def getValueWithKeyIndex(key: InternalRow): ValueRowWithKeyIndex = null
  override def valuesWithKeyIndex(): Iterator[ValueRowWithKeyIndex] = Iterator.empty
  override def keys(): Iterator[InternalRow] = Iterator.empty
  override def keyIsUnique: Boolean = true
  override def maxNumKeysIndex: Int = 0
  override def estimatedSize: Long = 0
  override def asReadOnlyCopy(): HashedRelation = this
  override def close(): Unit = {}
}
```

**Usage** (HashJoin.scala):

```scala
// Inner join with empty build side
if (hashedRelation == EmptyHashedRelation) {
  Iterator.empty  // Return nothing
}

// Anti join with empty build side
if (hashedRelation == EmptyHashedRelation) {
  streamIter  // Return all streamed rows
}
```

### Single-Row Optimization

For joins where build side has unique keys, optimized lookup path:

```scala
if (hashedRelation.keyIsUnique) {
  // Direct getValue() instead of get() which returns iterator
  streamIter.flatMap { srow =>
    val matched = hashedRelation.getValue(joinKeys(srow))
    if (matched != null) {
      Some(joinedRow(srow, matched)).filter(boundCondition)
    } else {
      None
    }
  }
}
```

**Benefit**: Avoids iterator allocation for each lookup.

### NULL-Aware Anti Join (NAAJ)

**HashedRelationWithAllNullKeys** (Lines 1127-1147):

```scala
case object HashedRelationWithAllNullKeys extends HashedRelation {
  // Special marker for when all build-side keys are NULL
  override def get(key: Long): Iterator[InternalRow] = Iterator.empty
  override def get(key: InternalRow): Iterator[InternalRow] = Iterator.empty
  override def getValue(key: Long): InternalRow = null
  override def getValue(key: InternalRow): InternalRow = null
  override def keyIsUnique: Boolean = false
  override def maxNumKeysIndex: Int = 0
  override def estimatedSize: Long = 0
  override def asReadOnlyCopy(): HashedRelation = this
  override def close(): Unit = {}
}
```

**Special handling in BroadcastHashJoinExec** (Lines 127-150):

```scala
if (isNullAwareAntiJoin) {
  streamedPlan.execute().mapPartitionsInternal { streamedIter =>
    val hashed = broadcastRelation.value.asReadOnlyCopy()

    if (hashed == EmptyHashedRelation) {
      // Build side is empty - return all streamed rows
      streamedIter
    } else if (hashed == HashedRelationWithAllNullKeys) {
      // All build keys are NULL - return nothing
      // (NULL NOT IN (NULL) = NULL → filtered out)
      Iterator.empty
    } else {
      // Standard NAAJ
      streamedIter.filter { row =>
        val lookupKey = keyGenerator(row)
        // Keep if key has NULL (NULL NOT IN anything = NULL → filtered out)
        // or no match found
        !lookupKey.anyNull && hashed.get(lookupKey) == null
      }
    }
  }
}
```

### Broadcast Timeout Handling

Broadcast joins use `executeBroadcast()` which returns a `Future[Broadcast[T]]`. If broadcast fails or times out, Spark will:

1. Throw exception for non-adaptive queries
2. With AQE enabled, fall back to shuffle-based join

### Skewed Data Handling (Non-AQE)

**isSkewJoin flag** indicates join was created by AQE for skew handling:

```scala
override def requiredChildDistribution: Seq[Distribution] = {
  if (isSkewJoin) {
    // Skew-optimized partitions don't satisfy ClusteredDistribution
    UnspecifiedDistribution :: UnspecifiedDistribution :: Nil
  } else {
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil
  }
}
```

When `isSkewJoin = true`, the join expects skewed partition specs from AQE's skew optimization.

---

## Supporting Classes

### HashJoin Trait

**File**: `sql/core/src/main/scala/org/apache/spark/sql/execution/joins/HashJoin.scala`

**Lines 43-102**: Common functionality for hash joins

```scala
trait HashJoin extends JoinCodegenSupport {
  def buildSide: BuildSide

  // Derived plans based on build side
  protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  protected lazy val (buildKeys, streamedKeys) = {
    buildSide match {
      case BuildLeft => (leftKeys, rightKeys)
      case BuildRight => (rightKeys, leftKeys)
    }
  }

  protected lazy val (buildOutput, streamedOutput) = {
    buildSide match {
      case BuildLeft => (left.output, right.output)
      case BuildRight => (right.output, left.output)
    }
  }

  // Key generators with rewriting optimization
  protected lazy val buildBoundKeys =
    bindReferences(HashJoin.rewriteKeyExpr(buildKeys), buildOutput)

  protected lazy val streamedBoundKeys =
    bindReferences(HashJoin.rewriteKeyExpr(streamedKeys), streamedOutput)

  // Null rows for outer joins
  protected lazy val buildNullRow = new GenericInternalRow(buildOutput.length)
  protected lazy val streamedNullRow = new GenericInternalRow(streamedOutput.length)

  // Join method selection
  protected def join(
      streamedIter: Iterator[InternalRow],
      hashed: HashedRelation,
      numOutputRows: SQLMetric): Iterator[InternalRow] = {

    joinType match {
      case _: InnerLike => innerJoin(streamedIter, hashed, numOutputRows)
      case LeftOuter | RightOuter => outerJoin(streamedIter, hashed, numOutputRows)
      case LeftSemi => semiJoin(streamedIter, hashed, numOutputRows)
      case LeftAnti => antiJoin(streamedIter, hashed, numOutputRows)
      case _: ExistenceJoin => existenceJoin(streamedIter, hashed, numOutputRows)
      case FullOuter =>
        throw new IllegalArgumentException("FullOuter joins should not use hash join")
    }
  }
}
```

### Key Rewriting Optimization

**Lines 727-755** (HashJoin.scala):

```scala
object HashJoin {
  /**
   * Rewrite multiple integral keys as a single LongType key.
   * This allows using LongHashedRelation instead of UnsafeHashedRelation.
   */
  def rewriteKeyExpr(keys: Seq[Expression]): Seq[Expression] = {
    if (!canRewriteAsLongType(keys)) {
      return keys
    }

    // Pack all keys into a single Long value using bit operations
    var keyExpr: Expression = if (keys.head.dataType != LongType) {
      Cast(keys.head, LongType)
    } else {
      keys.head
    }

    keys.tail.foreach { e =>
      val bits = e.dataType.defaultSize * 8
      keyExpr = BitwiseOr(
        ShiftLeft(keyExpr, Literal(bits)),
        BitwiseAnd(Cast(e, LongType), Literal((1L << bits) - 1)))
    }

    keyExpr :: Nil
  }

  private def canRewriteAsLongType(keys: Seq[Expression]): Boolean = {
    // All keys must be integral types and total size ≤ 8 bytes
    keys.forall(_.dataType.isInstanceOf[IntegralType]) &&
      keys.map(_.dataType.defaultSize).sum <= 8
  }
}
```

**Example**: Join on `(ByteType, IntType, ShortType)`
- Byte (1 byte) + Int (4 bytes) + Short (2 bytes) = 7 bytes ≤ 8 bytes
- Pack into single Long: `(byte << 48) | (int << 16) | short`

### JoinedRow

**File**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/JoinedRow.scala`

```scala
class JoinedRow extends InternalRow {
  private var row1: InternalRow = _
  private var row2: InternalRow = _

  def this(left: InternalRow, right: InternalRow) = {
    this()
    row1 = left
    row2 = right
  }

  def withLeft(newLeft: InternalRow): JoinedRow = {
    row1 = newLeft
    this
  }

  def withRight(newRight: InternalRow): JoinedRow = {
    row2 = newRight
    this
  }

  def apply(r1: InternalRow, r2: InternalRow): JoinedRow = {
    row1 = r1
    row2 = r2
    this
  }

  override def numFields: Int = row1.numFields + row2.numFields

  override def get(i: Int, dataType: DataType): AnyRef = {
    if (i < row1.numFields) {
      row1.get(i, dataType)
    } else {
      row2.get(i - row1.numFields, dataType)
    }
  }

  override def isNullAt(i: Int): Boolean = {
    if (i < row1.numFields) {
      row1.isNullAt(i)
    } else {
      row2.isNullAt(i - row1.numFields)
    }
  }

  // Similar for all other accessors (getLong, getInt, etc.)
}
```

**Purpose**: Efficiently represents concatenation of two rows without copying data.

### BaseJoinExec

**File**: `sql/core/src/main/scala/org/apache/spark/sql/execution/joins/BaseJoinExec.scala`

**Lines 27-58**:

```scala
trait BaseJoinExec extends BinaryExecNode {
  def joinType: JoinType
  def condition: Option[Expression]
  def leftKeys: Seq[Expression]
  def rightKeys: Seq[Expression]

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike => left.output ++ right.output
      case LeftOuter => left.output ++ right.output.map(_.withNullability(true))
      case RightOuter => left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case LeftSemi => left.output
      case LeftAnti => left.output
      case _: ExistenceJoin => left.output :+ existenceOutput
    }
  }

  override def verboseStringWithOperatorId(): String = {
    val joinCondStr = if (condition.isDefined) {
      s"${condition.get}"
    } else "None"

    if (leftKeys.nonEmpty || rightKeys.nonEmpty) {
      s"""
         |$formattedNodeName
         |${ExplainUtils.generateFieldString("Left keys", leftKeys)}
         |${ExplainUtils.generateFieldString("Right keys", rightKeys)}
         |${ExplainUtils.generateFieldString("Join type", joinType.toString)}
         |${ExplainUtils.generateFieldString("Join condition", joinCondStr)}
       """.stripMargin
    } else {
      s"""
         |$formattedNodeName
         |${ExplainUtils.generateFieldString("Join type", joinType.toString)}
         |${ExplainUtils.generateFieldString("Join condition", joinCondStr)}
       """.stripMargin
    }
  }
}
```

---

## Performance Characteristics

### BroadcastHashJoinExec

**Time Complexity**: O(n + m)
- Build phase: O(m) to build hash table
- Probe phase: O(n) to probe hash table
- Hash collisions add negligible overhead

**Space Complexity**: O(m) per executor
- Entire build side in memory on each executor
- Deserialized once per executor (shared across tasks)

**Best For**:
- Build side < 10MB (configurable via `spark.sql.autoBroadcastJoinThreshold`)
- High selectivity joins
- Dimension tables in star schemas

**Pros**:
- No shuffle required
- Very fast for small tables
- Low memory per task (shared broadcast)

**Cons**:
- OOM risk if build side too large
- Broadcast overhead (driver collection + network transfer)
- All executors must have enough memory

### ShuffledHashJoinExec

**Time Complexity**: O(n + m)
- Shuffle: O(n log n + m log m) for partitioning
- Build phase: O(m/p) per partition
- Probe phase: O(n/p) per partition

**Space Complexity**: O(m/p) per partition
- Hash table size proportional to build side partition
- Can spill if memory exceeded (with performance penalty)

**Best For**:
- Build side 3x+ smaller than stream side
- Both sides too large for broadcast
- `spark.sql.join.preferSortMergeJoin=false`

**Pros**:
- Predictable memory per partition
- Works with larger data than broadcast
- Good for skewed build side (smaller partitions on skewed keys)

**Cons**:
- Requires shuffle on both sides
- Hash table construction overhead
- Memory pressure if build partitions large

### SortMergeJoinExec

**Time Complexity**: O(n log n + m log m + n + m)
- Sort: O(n log n + m log m) if not already sorted
- Merge: O(n + m) streaming through sorted data

**Space Complexity**: O(1) for streaming, O(k) for buffering duplicates
- Streaming merge uses constant memory
- Buffers duplicate keys (can spill to disk)

**Best For**:
- Large tables with even distribution
- Already sorted data (sort is free)
- Default choice when broadcast/shuffle hash not applicable

**Pros**:
- Scalable to huge datasets
- Predictable memory usage (O(1) streaming)
- Can handle skewed keys (spill buffer to disk)
- No risk of OOM from hash table

**Cons**:
- Sort overhead (eliminated if already sorted)
- Slower than hash joins for small data
- Buffering duplicates can slow down

### BroadcastNestedLoopJoinExec

**Time Complexity**: O(n × m)
- Quadratic: for each streamed row, iterate all build rows

**Space Complexity**: O(m) per executor
- Entire build side broadcast

**Best For**:
- Non-equi joins (e.g., `a.x > b.y`, `a.x BETWEEN b.start AND b.end`)
- Very small build side (<< 1MB)

**Pros**:
- Supports any join condition
- No shuffle required

**Cons**:
- Quadratic time complexity
- Very slow for large data
- Only practical for tiny datasets

### CartesianProductExec

**Time Complexity**: O(n × m)
- Quadratic: cartesian product

**Space Complexity**: O(m/p) per partition
- Buffers right partition

**Best For**:
- CROSS JOIN with very small tables
- Intentional cartesian products

**Pros**:
- Simple
- Supports any condition

**Cons**:
- Quadratic output size (n × m rows)
- Can explode to huge result
- Usually indicates missing join condition (query bug)

---

## Summary

### Join Selection Decision Tree

```
Has equi-join keys?
├─ Yes
│  ├─ One side < 10MB?
│  │  ├─ Yes → BroadcastHashJoinExec
│  │  └─ No
│  │     ├─ One side 3x smaller AND preferSortMergeJoin=false?
│  │     │  ├─ Yes → ShuffledHashJoinExec
│  │     │  └─ No → SortMergeJoinExec (default)
│  │     └─ Large tables → SortMergeJoinExec
│  └─ AQE can dynamically change strategy
└─ No (non-equi join)
   ├─ One side small?
   │  ├─ Yes → BroadcastNestedLoopJoinExec
   │  └─ No → Error (non-equi join requires broadcast)
   └─ CROSS JOIN → CartesianProductExec
```

### Key Takeaways

1. **Hash Joins**: Fast (O(n + m)) but require memory for hash table
   - Broadcast: Small build side, no shuffle
   - Shuffled: Medium build side, both sides shuffle

2. **Sort-Merge Join**: Scalable default with sort overhead
   - O(n log n) sort + O(n) merge
   - Streaming merge uses O(1) memory
   - Can spill duplicate keys

3. **Nested Loop**: Only for non-equi joins
   - O(n × m) quadratic time
   - Avoid unless necessary

4. **Build Side Selection**: Always build smaller side
   - Based on table statistics
   - Can be forced with hints

5. **Code Generation**: Critical for performance
   - Eliminates virtual calls and boxing
   - Inlines conditions
   - Processes rows in tight loops

6. **Metrics**: Monitor in Spark UI
   - numOutputRows: Join cardinality
   - buildDataSize/buildTime: Hash table overhead
   - spillSize: Memory pressure indicator

This comprehensive guide covers all aspects of Spark SQL join execution with actual code from the codebase, explaining the algorithms, optimizations, and performance characteristics of each join implementation.
