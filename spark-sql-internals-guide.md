# Spark SQL Internals: A Comprehensive Guide

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Query Processing Pipeline](#query-processing-pipeline)
3. [Complete Optimizer Rules Reference](#complete-optimizer-rules-reference)
4. [Join Processing Deep Dive](#join-processing-deep-dive)
5. [Aggregate Processing Deep Dive](#aggregate-processing-deep-dive)
6. [Key Classes and Files Reference](#key-classes-and-files-reference)
7. [End-to-End Query Examples](#end-to-end-query-examples)
8. [Configuration and Tuning](#configuration-and-tuning)

---

## Architecture Overview

Spark SQL consists of four main components that transform SQL queries into distributed computations:

### 1. SQL Parser
**Location**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/`

- **Grammar Files**: `sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/`
  - `SqlBaseParser.g4` - ANTLR4 parser grammar defining SQL syntax
  - `SqlBaseLexer.g4` - Lexer rules for tokenization
- **Key Class**: `AstBuilder.scala` (254KB, line 65)
  - Converts ANTLR4 ParseTree into Catalyst expressions and logical plans
  - Implements visitor pattern for all SQL constructs
  - Handles SELECT, INSERT, CREATE, ALTER, and all DDL/DML statements

**Role**: Transforms SQL text into an unresolved logical plan (AST - Abstract Syntax Tree)

### 2. Analyzer
**Location**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/`

- **Key Class**: `Analyzer.scala` (195KB)
  - Main analyzer that resolves unresolved plans using 98+ resolution rules
  - Uses fixed-point iteration until plan is fully resolved
- **Important Components**:
  - `FunctionRegistry.scala` (50KB) - Catalog of built-in functions
  - `TypeCoercion.scala` (17KB) - Automatic type conversions
  - Resolution rules for references, subqueries, aggregates, window functions, etc.

**Role**: Resolves table/column references, validates semantics, performs type coercion

### 3. Optimizer
**Location**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/`

- **Key Class**: `Optimizer.scala` (116KB)
  - Abstract optimizer with 20+ batches containing 122+ optimization rules
  - Organized batches: Analysis finalization → Logical optimization → Cost-based optimization
- **Specialized Optimizers**:
  - `expressions.scala` (54KB) - Expression-level optimizations
  - `joins.scala` (22KB) - Join-specific optimizations
  - `subquery.scala` (53KB) - Subquery optimizations
  - `CostBasedJoinReorder.scala` (21KB) - Statistics-based join ordering

**Role**: Transforms logical plan into optimized logical plan using rule-based and cost-based optimization

### 4. Physical Planner (Execution)
**Location**: `sql/core/src/main/scala/org/apache/spark/sql/execution/`

- **Key Classes**:
  - `SparkPlanner.scala` (118 lines) - Main planner converting logical to physical plans
  - `SparkStrategies.scala` (53KB) - All physical planning strategies
  - `QueryExecution.scala` (27KB) - Orchestrates all query execution phases
  - `SparkPlan.scala` (23KB) - Base class for all physical operators

**Role**: Converts optimized logical plan into executable physical plan using Spark RDDs

---

## Query Processing Pipeline

Spark SQL transforms queries through five distinct phases:

```
┌─────────────────┐
│   SQL Text      │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  [1] PARSING                        │
│  Files: SqlBaseParser.g4,           │
│         AstBuilder.scala            │
│  Output: Unresolved LogicalPlan     │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  [2] ANALYSIS                       │
│  Files: Analyzer.scala              │
│  - Resolve table/column references  │
│  - Type coercion                    │
│  - Validation                       │
│  Output: Resolved LogicalPlan       │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  [3] OPTIMIZATION                   │
│  Files: Optimizer.scala             │
│  - Rule-based optimization (122+)   │
│  - Cost-based optimization          │
│  Output: Optimized LogicalPlan      │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  [4] PHYSICAL PLANNING              │
│  Files: SparkPlanner.scala,         │
│         SparkStrategies.scala       │
│  - Apply physical strategies        │
│  - Choose implementations           │
│  Output: SparkPlan (Physical Plan)  │
└────────┬────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  [5] EXECUTION                      │
│  - Whole-stage code generation      │
│  - RDD execution                    │
│  Output: RDD[InternalRow]           │
└─────────────────────────────────────┘
```

### Phase Details

#### [1] Parsing
- **Input**: SQL string
- **Process**: ANTLR4-based parser tokenizes and parses SQL according to grammar
- **Key Method**: `AstBuilder.visitQuerySpecification()` traverses parse tree
- **Output**: Unresolved logical plan with `UnresolvedRelation`, `UnresolvedAttribute` nodes

#### [2] Analysis
- **Input**: Unresolved logical plan
- **Process**:
  - Multiple passes using fixed-point iteration
  - Resolves table references using catalog (`ResolveRelations`)
  - Resolves column references (`ResolveReferences`)
  - Validates aggregate expressions (`ResolveReferencesInAggregate`)
  - Type coercion (`TypeCoercion`)
  - Subquery analysis (`ResolveSubquery`)
- **Output**: Fully resolved logical plan with concrete types and references

#### [3] Optimization
- **Input**: Resolved logical plan
- **Process**: Applies 122+ optimization rules organized in 20+ batches
  - Each batch runs until fixed point or max iterations
  - Logical optimizations: predicate pushdown, column pruning, constant folding
  - Cost-based optimizations: join reordering using statistics
- **Output**: Optimized logical plan

#### [4] Physical Planning
- **Input**: Optimized logical plan
- **Process**: `SparkPlanner` applies strategies sequentially
  - Strategies: `JoinSelection`, `Aggregation`, `BasicOperators`, etc.
  - First matching strategy wins
  - Chooses physical implementation based on data characteristics
- **Output**: Physical plan (SparkPlan tree)

#### [5] Execution
- **Input**: Physical plan
- **Process**:
  - Whole-stage code generation combines operators into single function
  - Executes using Spark RDD transformations
  - Adaptive Query Execution (AQE) can reoptimize at runtime
- **Output**: Result RDD

---

## Complete Optimizer Rules Reference

The optimizer applies rules in batches. Each batch runs its rules repeatedly until the plan stabilizes (fixed point) or max iterations reached.

**Location**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala` (lines 97-275)

### Batch 1: "Finish Analysis"
**Mode**: FixedPoint(1) - Runs once
**Purpose**: Complete analysis phase and prepare for optimization

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 1 | `EliminateResolvedHint` | Optimizer.scala | Removes resolved hint nodes from plan |
| 2 | `EliminateSubqueryAliases` | Optimizer.scala | Removes subquery alias nodes (used only for resolution) |
| 3 | `EliminatePipeOperators` | Optimizer.scala | Removes pipe operators after processing |
| 4 | `EliminateView` | Optimizer.scala | Removes view nodes, keeping underlying plan |
| 5 | `EliminateSQLFunctionNode` | Optimizer.scala | Removes SQL function wrapper nodes |
| 6 | `ReplaceExpressions` | Optimizer.scala | Replaces runtime replaceable expressions with concrete ones |
| 7 | `RewriteNonCorrelatedExists` | Optimizer.scala | Rewrites EXISTS subqueries without correlation to Limit(1) |
| 8 | `PullOutGroupingExpressions` | Optimizer.scala | Extracts complex grouping expressions to separate project |
| 9 | `InsertMapSortInGroupingExpressions` | Optimizer.scala | Adds sorting for map types in grouping keys |
| 10 | `InsertMapSortInRepartitionExpressions` | Optimizer.scala | Adds sorting for map types in repartition keys |
| 11 | `ComputeCurrentTime` | Optimizer.scala | Computes current timestamp once for entire query |
| 12 | `ReplaceCurrentLike` | Optimizer.scala | Replaces current_date, current_timestamp functions |
| 13 | `SpecialDatetimeValues` | Optimizer.scala | Handles special datetime literals |
| 14 | `RewriteAsOfJoin` | Optimizer.scala | Rewrites time-travel joins (AS OF) |
| 15 | `EvalInlineTables` | Optimizer.scala | Evaluates VALUES clauses at compile time |
| 16 | `ReplaceTranspose` | Optimizer.scala | Replaces TRANSPOSE with concrete operations |
| 17 | `RewriteCollationJoin` | Optimizer.scala | Rewrites joins with collation considerations |

### Batch 2: "Rewrite With expression"
**Mode**: FixedPoint - Runs until plan stabilizes
**Purpose**: Handle WITH expressions

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 18 | `RewriteWithExpression` | Optimizer.scala | Rewrites WITH expressions in scalar subqueries |

### Batch 3: "Eliminate Distinct"
**Mode**: Once
**Purpose**: Remove redundant DISTINCT operations

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 19 | `EliminateDistinct` | Optimizer.scala | Removes DISTINCT when child output is already unique |

### Batch 4: "Inline CTE"
**Mode**: Once
**Purpose**: Inline Common Table Expressions when beneficial

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 20 | `InlineCTE` | InlineCTE.scala | Inlines CTEs referenced only once or very small CTEs |

### Batch 5: "Union"
**Mode**: FixedPoint
**Purpose**: Optimize UNION operations

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 21 | `RemoveNoopOperators` | Optimizer.scala | Removes operators that don't change data |
| 22 | `CombineUnions` | Optimizer.scala | Flattens nested UNIONs into single UNION |
| 23 | `RemoveNoopUnion` | Optimizer.scala | Removes UNION with single child |

### Batch 6: "LocalRelation early"
**Mode**: FixedPoint
**Purpose**: Optimize queries that can be computed locally

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 24 | `ConvertToLocalRelation` | PropagateEmptyRelation.scala | Converts small plans to LocalRelation for local execution |
| 25 | `PropagateEmptyRelation` | PropagateEmptyRelation.scala | Propagates empty relations through operators |
| 26 | `UpdateAttributeNullability` | Optimizer.scala | Updates column nullability based on filters |

### Batch 7: "Pullup Correlated Expressions"
**Mode**: Once
**Purpose**: Extract correlated expressions from subqueries

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 27 | `OptimizeOneRowRelationSubquery` | Optimizer.scala | Optimizes subqueries returning single row |
| 28 | `PullOutNestedDataOuterRefExpressions` | Optimizer.scala | Extracts nested field access on correlated references |
| 29 | `PullupCorrelatedPredicates` | subquery.scala | Pulls correlated predicates above join |

### Batch 8: "Subquery"
**Mode**: FixedPoint(1)
**Purpose**: Optimize subqueries

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 30 | `OptimizeSubqueries` | Optimizer.scala | Recursively applies optimizer to subqueries |
| 31 | `OptimizeOneRowRelationSubquery` | Optimizer.scala | Optimizes single-row subqueries |

### Batch 9: "Replace Operators"
**Mode**: FixedPoint
**Purpose**: Replace set operations with joins/aggregates

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 32 | `RewriteExceptAll` | Optimizer.scala | Rewrites EXCEPT ALL using aggregate |
| 33 | `RewriteIntersectAll` | Optimizer.scala | Rewrites INTERSECT ALL using aggregate |
| 34 | `ReplaceIntersectWithSemiJoin` | Optimizer.scala | Replaces INTERSECT with LEFT SEMI JOIN |
| 35 | `ReplaceExceptWithFilter` | Optimizer.scala | Replaces EXCEPT with filter (when applicable) |
| 36 | `ReplaceExceptWithAntiJoin` | Optimizer.scala | Replaces EXCEPT with LEFT ANTI JOIN |
| 37 | `ReplaceDistinctWithAggregate` | Optimizer.scala | Replaces DISTINCT with GROUP BY |
| 38 | `ReplaceDeduplicateWithAggregate` | Optimizer.scala | Replaces DEDUPLICATE with aggregate |

### Batch 10: "Aggregate"
**Mode**: FixedPoint
**Purpose**: Optimize GROUP BY operations

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 39 | `RemoveLiteralFromGroupExpressions` | Optimizer.scala:2539 | Removes constant literals from GROUP BY (don't affect grouping) |
| 40 | `RemoveRepetitionFromGroupExpressions` | Optimizer.scala:2617 | Removes duplicate expressions from GROUP BY |

### Batch 11: "Operator Optimization before Inferring Filters"
**Mode**: FixedPoint
**Purpose**: Main optimization batch with 50+ rules

#### Operator Push Down Rules

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 41 | `PushProjectionThroughUnion` | Optimizer.scala | Pushes Project below Union to reduce data early |
| 42 | `PushProjectionThroughLimitAndOffset` | Optimizer.scala | Pushes projection below limit/offset |
| 43 | `ReorderJoin` | joins.scala:45-157 | Reorders inner joins using heuristics (prefer joins with conditions) |
| 44 | `EliminateOuterJoin` | joins.scala:158-244 | Converts outer joins to inner when filters make it safe |
| 45 | `PushDownPredicates` | Optimizer.scala | **CRITICAL**: Pushes filters as close to data sources as possible |
| 46 | `PushDownLeftSemiAntiJoin` | Optimizer.scala | Pushes SEMI/ANTI joins down through other joins |
| 47 | `PushLeftSemiLeftAntiThroughJoin` | Optimizer.scala | Pushes LEFT SEMI/ANTI through other joins |
| 48 | `OptimizeJoinCondition` | Optimizer.scala | Optimizes join predicates (extracts filters from join conditions) |
| 49 | `LimitPushDown` | Optimizer.scala | Pushes LIMIT through Project, Union, etc. |
| 50 | `LimitPushDownThroughWindow` | Optimizer.scala | Pushes LIMIT through Window when safe |
| 51 | `ColumnPruning` | Optimizer.scala | **CRITICAL**: Removes unused columns from scans and operators |
| 52 | `GenerateOptimization` | Optimizer.scala | Optimizes EXPLODE/GENERATE operations |

#### Operator Combine Rules

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 53 | `CollapseRepartition` | Optimizer.scala | Combines multiple repartition operations |
| 54 | `CollapseProject` | Optimizer.scala | Combines adjacent Project operators |
| 55 | `OptimizeWindowFunctions` | Optimizer.scala | Optimizes window function expressions |
| 56 | `CollapseWindow` | Optimizer.scala | Combines multiple Window operators with same partitioning |
| 57 | `EliminateOffsets` | Optimizer.scala | Removes unnecessary OFFSET operations |
| 58 | `EliminateLimits` | Optimizer.scala | Removes unnecessary LIMIT operations |
| 59 | `CombineUnions` | Optimizer.scala | Flattens nested UNIONs |

#### Constant Folding and Strength Reduction

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 60 | `OptimizeRepartition` | Optimizer.scala | Optimizes repartition operations |
| 61 | `EliminateWindowPartitions` | Optimizer.scala | Removes window partitioning when not needed |
| 62 | `TransposeWindow` | Optimizer.scala | Reorders window and other operators |
| 63 | `NullPropagation` | expressions.scala | **CRITICAL**: Simplifies expressions that always return NULL |
| 64 | `RewriteNonCorrelatedExists` | Optimizer.scala | Rewrites simple EXISTS to Limit(1) |
| 65 | `NullDownPropagation` | Optimizer.scala | Propagates NULL constraints downward |
| 66 | `ConstantPropagation` | Optimizer.scala | Propagates constant values through expressions |
| 67 | `FoldablePropagation` | expressions.scala | Propagates compile-time computable expressions |
| 68 | `OptimizeIn` | expressions.scala | Optimizes IN predicates (converts to InSet for large lists) |
| 69 | `OptimizeRand` | Optimizer.scala | Optimizes RAND() function calls |
| 70 | `ConstantFolding` | expressions.scala | **CRITICAL**: Evaluates constant expressions at compile time |
| 71 | `EliminateAggregateFilter` | Optimizer.scala:557 | Removes aggregate FILTER when always true/false |
| 72 | `ReorderAssociativeOperator` | expressions.scala | Reorders +, *, AND, OR for better constant folding |
| 73 | `LikeSimplification` | expressions.scala | Simplifies LIKE patterns to StartsWith, EndsWith, Contains |
| 74 | `BooleanSimplification` | expressions.scala | Simplifies boolean logic (true AND x → x, false OR x → x) |
| 75 | `SimplifyConditionals` | expressions.scala | Simplifies IF/CASE when condition is constant |
| 76 | `PushFoldableIntoBranches` | Optimizer.scala | Pushes constant expressions into IF/CASE branches |
| 77 | `SimplifyBinaryComparison` | expressions.scala | Simplifies comparisons (x = x → true, x > x → false) |
| 78 | `ReplaceNullWithFalseInPredicate` | Optimizer.scala | Replaces NULL with FALSE in filter predicates |
| 79 | `PruneFilters` | Optimizer.scala | Removes filters that are always true or duplicate |
| 80 | `SimplifyCasts` | expressions.scala | Removes unnecessary casts |
| 81 | `SimplifyCaseConversionExpressions` | expressions.scala | Simplifies upper/lower case conversions |
| 82 | `RewriteCorrelatedScalarSubquery` | subquery.scala | Rewrites correlated scalar subqueries to joins |
| 83 | `RewriteLateralSubquery` | subquery.scala | Rewrites LATERAL subqueries |
| 84 | `EliminateSerialization` | Optimizer.scala | Removes unnecessary serialization/deserialization |
| 85 | `RemoveRedundantAliases` | Optimizer.scala | Removes alias expressions that don't rename |
| 86 | `RemoveRedundantAggregates` | Optimizer.scala | Removes aggregates that don't aggregate (single group) |
| 87 | `UnwrapCastInBinaryComparison` | Optimizer.scala | Removes casts from both sides of comparisons |
| 88 | `RemoveNoopOperators` | Optimizer.scala | Removes Project/Filter that don't change data |
| 89 | `OptimizeUpdateFields` | Optimizer.scala | Optimizes struct field updates |
| 90 | `SimplifyExtractValueOps` | expressions.scala | Simplifies nested field extraction |
| 91 | `OptimizeCsvJsonExprs` | Optimizer.scala | Optimizes CSV/JSON parsing expressions |
| 92 | `CombineConcats` | expressions.scala | Combines adjacent string concatenations |
| 93 | `PushdownPredicatesAndPruneColumnsForCTEDef` | Optimizer.scala | Optimizes CTE definitions |

### Batch 12: "Infer Filters"
**Mode**: Once
**Purpose**: Infer additional filters from constraints

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 94 | `InferFiltersFromGenerate` | Optimizer.scala | Infers filters from GENERATE/EXPLODE |
| 95 | `InferFiltersFromConstraints` | Optimizer.scala | **IMPORTANT**: Infers filters from join conditions and constraints (e.g., a.id = b.id AND b.id > 10 → infer a.id > 10) |

### Batch 13: "Operator Optimization after Inferring Filters"
**Mode**: FixedPoint
**Purpose**: Re-run optimizations with new inferred filters

*Same 50+ rules as Batch 11*

### Batch 14: "Push extra predicate through join"
**Mode**: FixedPoint
**Purpose**: Push inferred predicates through joins

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 96 | `PushExtraPredicateThroughJoin` | Optimizer.scala | Pushes extra inferred predicates through joins |
| 97 | `PushDownPredicates` | Optimizer.scala | Standard predicate pushdown |

### Batch 15: "Clean Up Temporary CTE Info"
**Mode**: Once

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 98 | `CleanUpTempCTEInfo` | Optimizer.scala | Cleans temporary CTE metadata |

### Batch 16: "Pre CBO Rules"
**Mode**: Once
**Purpose**: Extension point before cost-based optimization

*Extension point for custom rules*

### Batch 17: "Early Filter and Projection Push-Down"
**Mode**: Once
**Purpose**: Push down operations to data sources

*Extension point for scan pushdown (Parquet, ORC, etc.)*

### Batch 18: "Update CTE Relation Stats"
**Mode**: Once

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 99 | `UpdateCTERelationStats` | Optimizer.scala | Updates statistics for CTE relations |

### Batch 19: "Join Reorder"
**Mode**: FixedPoint(1)
**Purpose**: Cost-based join reordering

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 100 | `CostBasedJoinReorder` | CostBasedJoinReorder.scala | **IMPORTANT**: Uses table/column statistics to find optimal join order (dynamic programming algorithm, only inner joins) |

### Batch 20: "Eliminate Sorts"
**Mode**: Once
**Purpose**: Remove unnecessary sorting

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 101 | `EliminateSorts` | Optimizer.scala | Removes sorts before operations that re-sort |
| 102 | `RemoveRedundantSorts` | Optimizer.scala | Removes duplicate sort operations |

### Batch 21: "Decimal Optimizations"
**Mode**: FixedPoint

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 103 | `DecimalAggregates` | Optimizer.scala:2238 | Optimizes decimal precision in aggregate functions |

### Batch 22: "Distinct Aggregate Rewrite"
**Mode**: Once

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 104 | `RewriteDistinctAggregates` | RewriteDistinctAggregates.scala | Rewrites multiple DISTINCT aggregates using expand + aggregate pattern |

### Batch 23: "Object Expressions Optimization"
**Mode**: FixedPoint

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 105 | `EliminateMapObjects` | Optimizer.scala | Eliminates unnecessary MapObjects operations |
| 106 | `CombineTypedFilters` | Optimizer.scala | Combines multiple typed filter operations |
| 107 | `ObjectSerializerPruning` | Optimizer.scala | Prunes unused fields from object serializers |
| 108 | `ReassignLambdaVariableID` | Optimizer.scala | Reassigns lambda variable IDs for correctness |

### Batch 24: "LocalRelation"
**Mode**: FixedPoint

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 109 | `ConvertToLocalRelation` | PropagateEmptyRelation.scala | Converts to local relation when beneficial |
| 110 | `PropagateEmptyRelation` | PropagateEmptyRelation.scala | Propagates empty relation knowledge |
| 111 | `UpdateAttributeNullability` | Optimizer.scala | Updates nullability information |

### Batch 25: "Optimize One Row Plan"
**Mode**: FixedPoint

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 112 | `OptimizeOneRowPlan` | Optimizer.scala | Optimizes plans known to return single row |

### Batch 26: "Check Cartesian Products"
**Mode**: Once

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 113 | `CheckCartesianProducts` | Optimizer.scala | Warns about unintended Cartesian products |

### Batch 27: "RewriteSubquery"
**Mode**: Once
**Purpose**: Final subquery rewriting

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 114 | `RewritePredicateSubquery` | subquery.scala | Rewrites IN/EXISTS subqueries to joins |
| 115 | `PushPredicateThroughJoin` | Optimizer.scala | Pushes predicates through joins |
| 116 | `LimitPushDown` | Optimizer.scala | Pushes limits down |
| 117 | `ColumnPruning` | Optimizer.scala | Final column pruning |
| 118 | `CollapseProject` | Optimizer.scala | Collapses projections |
| 119 | `RemoveRedundantAliases` | Optimizer.scala | Removes aliases |
| 120 | `RemoveNoopOperators` | Optimizer.scala | Removes no-ops |

### Batch 28: "NormalizeFloatingNumbers"
**Mode**: Once

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 121 | `NormalizeFloatingNumbers` | Optimizer.scala | Normalizes floating point representations (-0.0 → 0.0, NaN) |

### Batch 29: "ReplaceUpdateFieldsExpression"
**Mode**: Once

| # | Rule Name | File | Description |
|---|-----------|------|-------------|
| 122 | `ReplaceUpdateFieldsExpression` | Optimizer.scala | Replaces update fields expressions with concrete implementation |

---

## Join Processing Deep Dive

### Join Types
**Location**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/joinTypes.scala`

Spark SQL supports multiple join types categorized as:

- **Inner-like**: `Inner`, `Cross`, `LeftSingle`
- **Outer**: `LeftOuter`, `RightOuter`, `FullOuter`
- **Semi/Anti**: `LeftSemi`, `LeftAnti`
- **Existence**: `ExistenceJoin` (internal use)

### Physical Join Implementations

**Location**: `sql/core/src/main/scala/org/apache/spark/sql/execution/joins/`

#### 1. BroadcastHashJoinExec
**File**: `BroadcastHashJoinExec.scala` (10KB)

- **Strategy**: Broadcasts smaller side to all executors, builds hash table
- **Supported Join Types**: All except FullOuter
- **Requirements**: Equi-join keys (must have equality conditions)
- **When Used**: One side fits in memory (< `spark.sql.autoBroadcastJoinThreshold`, default 10MB)
- **Performance**: ★★★★★ (Best for small dimension tables)
- **Network**: High (broadcasts data to all executors)

**Example Plan**:
```
BroadcastHashJoinExec [dept_id#1], [id#5], Inner
:- FilterExec
:  +- Scan employees
+- BroadcastExchange
   +- Scan departments
```

#### 2. ShuffledHashJoinExec
**File**: `ShuffledHashJoinExec.scala` (25KB)

- **Strategy**: Shuffles both sides by join keys, builds hash table from smaller side
- **Supported Join Types**: All join types
- **Requirements**: Equi-join keys
- **When Used**:
  - `spark.sql.join.preferSortMergeJoin=false` AND
  - One side significantly smaller than other (3x default) AND
  - Both sides don't fit broadcast threshold
- **Performance**: ★★★★☆ (Good when one side much smaller)
- **Network**: High (shuffle both sides)

**Example Plan**:
```
ShuffledHashJoinExec [dept_id#1], [id#5], Inner, BuildRight
:- ShuffleExchange hashpartitioning(dept_id#1)
:  +- Scan employees
+- ShuffleExchange hashpartitioning(id#5)
   +- Scan departments
```

#### 3. SortMergeJoinExec
**File**: `SortMergeJoinExec.scala` (60KB)

- **Strategy**: Sorts both sides by join keys, then merges sorted streams
- **Supported Join Types**: All join types
- **Requirements**: Equi-join keys, sortable key types
- **When Used**: Default choice when broadcast not applicable
- **Performance**: ★★★☆☆ (Consistent, good for large datasets)
- **Network**: Medium (shuffle but no broadcast)
- **Memory**: Lower than hash joins (streams through data)

**Example Plan**:
```
SortMergeJoinExec [dept_id#1], [id#5], Inner
:- SortExec [dept_id#1 ASC]
:  +- ShuffleExchange hashpartitioning(dept_id#1)
:     +- Scan employees
+- SortExec [id#5 ASC]
   +- ShuffleExchange hashpartitioning(id#5)
      +- Scan departments
```

#### 4. BroadcastNestedLoopJoinExec
**File**: `BroadcastNestedLoopJoinExec.scala` (21KB)

- **Strategy**: Broadcasts one side, nested loop iteration
- **Supported Join Types**: All join types
- **Requirements**: None (works for non-equi joins)
- **When Used**:
  - Fallback when no equi-join keys OR
  - One side small enough to broadcast + non-equi conditions
- **Performance**: ★★☆☆☆ (Slow O(n*m) but most general)
- **Use Cases**: Non-equi joins (e.g., `a.val > b.val`), range joins

**Example Plan**:
```
BroadcastNestedLoopJoinExec Inner, (val#1 > val#5)
:- Scan table_a
+- BroadcastExchange
   +- Scan table_b
```

#### 5. CartesianProductExec
**File**: `CartesianProductExec.scala` (4KB)

- **Strategy**: Cartesian product of both sides
- **Supported Join Types**: Inner-like only
- **Requirements**: None
- **When Used**: CROSS JOIN or inner join with no conditions
- **Performance**: ★☆☆☆☆ (Very slow, avoid if possible)
- **Output Size**: O(n × m)

### Join Selection Strategy

**Location**: `sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala` (lines 178-421)

#### Decision Tree with Hints

```
┌─────────────────────────────────┐
│ Extract Join Keys & Conditions  │
└────────────┬────────────────────┘
             │
             ▼
    ┌────────────────────┐
    │  Broadcast Hint?   │───Yes──▶ BroadcastHashJoinExec
    └────────┬───────────┘
             │ No
             ▼
    ┌────────────────────┐
    │ Sort Merge Hint?   │───Yes──▶ SortMergeJoinExec
    └────────┬───────────┘
             │ No
             ▼
    ┌────────────────────┐
    │ Shuffle Hash Hint? │───Yes──▶ ShuffledHashJoinExec
    └────────┬───────────┘
             │ No
             ▼
    ┌────────────────────┐
    │ Shuffle NL Hint?   │───Yes──▶ BroadcastNestedLoopJoinExec
    └────────┬───────────┘
             │ No
             ▼
   Continue to heuristic selection...
```

#### Decision Tree without Hints

```
┌─────────────────────────────────┐
│    Has Equi-Join Keys?          │
└────────┬──────────┬─────────────┘
         │ Yes      │ No
         ▼          ▼
  ┌─────────────┐  ┌──────────────────────┐
  │ Hashable?   │  │ One Side Broadcastable?│
  └──┬──────────┘  └──┬──────────┬────────┘
     │ Yes            │ Yes      │ No
     ▼                ▼          ▼
┌──────────────────────┐  BroadcastNL   FAIL
│ One Side < Broadcast │              (No Strategy)
│    Threshold?        │
└──┬──────────┬────────┘
   │ Yes      │ No
   ▼          ▼
Broadcast  ┌──────────────────────┐
HashJoin   │ preferSortMergeJoin? │
           └──┬──────────┬────────┘
              │ false    │ true
              ▼          ▼
      ┌─────────────┐  ┌──────────────┐
      │One Side 3x  │  │Keys Sortable?│
      │  Smaller?   │  └──┬───────────┘
      └──┬──────────┘     │ Yes  │ No
         │ Yes   │ No     ▼      ▼
         ▼       ▼      Sort   Fallback to
    ShuffleHash Sort   Merge   BroadcastNL
        Join    Merge   Join   or Cartesian
                Join
```

#### Key Configuration Parameters

- **`spark.sql.autoBroadcastJoinThreshold`**: Size threshold for broadcast (default: 10MB)
  - Set to -1 to disable broadcast joins
- **`spark.sql.join.preferSortMergeJoin`**: Prefer sort-merge over shuffle hash (default: true)
- **`spark.sql.shuffle.partitions`**: Number of partitions for shuffle (default: 200)

### Join Optimization Rules

#### 1. ReorderJoin (Heuristic)
**File**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/joins.scala` (lines 45-157)

**Purpose**: Reorder inner joins for better performance using heuristics

**Algorithm**:
1. Only reorders inner joins (outer joins have fixed semantics)
2. Prefers joins with conditions over Cartesian products
3. Places larger filters earlier
4. Does NOT use statistics (rule-based only)

**Example**:
```sql
-- Original
FROM a JOIN b JOIN c ON a.id = c.id
-- Could reorder to
FROM a JOIN c ON a.id = c.id JOIN b
```

#### 2. EliminateOuterJoin
**File**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/joins.scala` (lines 158-244)

**Purpose**: Convert outer joins to inner joins when safe

**Algorithm**:
Analyzes filters above outer join. If filters null out the outer rows anyway, convert to inner join.

**Example**:
```sql
-- Original
SELECT * FROM a LEFT JOIN b ON a.id = b.id WHERE b.value > 10
-- b.value > 10 filters out NULL rows from b, so equivalent to
SELECT * FROM a JOIN b ON a.id = b.id WHERE b.value > 10
```

#### 3. CostBasedJoinReorder
**File**: `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/CostBasedJoinReorder.scala` (21KB)

**Purpose**: Reorder joins using table statistics for optimal cost

**Requirements**:
- `spark.sql.cbo.enabled=true`
- `spark.sql.cbo.joinReorder.enabled=true`
- Table statistics available (`ANALYZE TABLE`)

**Algorithm**: Dynamic programming
1. Consider all possible join orders
2. Estimate cost using statistics (row count, size)
3. Choose minimum cost order
4. Only reorders inner joins (max 12 tables by default)

**Statistics Used**:
- Row count
- Data size
- Column statistics (min, max, distinct count, nulls)

**Configuration**:
- `spark.sql.cbo.starSchemaDetection`: Optimize star schemas
- `spark.sql.cbo.joinReorder.dp.threshold`: Max tables for DP (default: 12)

---

## Aggregate Processing Deep Dive

### Physical Aggregate Implementations

**Location**: `sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/`

#### 1. HashAggregateExec
**File**: `HashAggregateExec.scala` (36KB)

- **Strategy**: Uses Tungsten hash map for aggregation
- **Performance**: ★★★★★ (Best for most cases)
- **Requirements**:
  - Aggregate functions support Tungsten
  - Grouping keys are hashable
  - Sufficient memory for hash table
- **Memory**: Uses Tungsten memory management (off-heap)
- **Supports**: Partial and final aggregation modes
- **When Used**: Default choice when conditions met

**Aggregation Modes**:
- **Partial**: Computes partial results per partition
- **Final**: Merges partial results into final results

**Example Plan**:
```
HashAggregateExec (Final)
  keys=[dept#1]
  functions=[sum(salary#2), count(*)]
+- ShuffleExchange hashpartitioning(dept#1)
   +- HashAggregateExec (Partial)
        keys=[dept#1]
        functions=[sum(salary#2), count(*)]
      +- Scan employees
```

#### 2. ObjectHashAggregateExec
**File**: `ObjectHashAggregateExec.scala` (6KB)

- **Strategy**: Uses Scala HashMap (not Tungsten)
- **Performance**: ★★★☆☆ (Slower than HashAggregateExec)
- **Requirements**: Complex types not supported by Tungsten
- **Memory**: Uses on-heap memory (JVM managed)
- **When Used**: Fallback when Tungsten not supported
- **Enabled**: `spark.sql.execution.useObjectHashAggregateExec=true`

#### 3. SortAggregateExec
**File**: `SortAggregateExec.scala` (5KB)

- **Strategy**: Sorts data by grouping keys, then aggregates sorted groups
- **Performance**: ★★☆☆☆ (Slower but more memory-efficient)
- **Memory**: Very low (streams through sorted data)
- **When Used**:
  - Forced by config OR
  - Hash aggregation not supported OR
  - Very high cardinality (hash map too large)
- **Configuration**: Set `spark.sql.execution.useObjectHashAggregateExec=false`

**Example Plan**:
```
SortAggregateExec
  keys=[high_cardinality_id#1]
  functions=[sum(value#2)]
+- SortExec [high_cardinality_id#1]
   +- ShuffleExchange hashpartitioning(high_cardinality_id#1)
      +- Scan large_table
```

### Aggregate Planning Logic

**Location**: `sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/AggUtils.scala`

#### Two-Phase Aggregation Pattern

Most aggregates use two phases for distributed computation:

```
┌────────────────────────────────────┐
│  Input Data (distributed)          │
└──────┬─────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│  PARTIAL AGGREGATION                │
│  (per partition, no shuffle)        │
│  - Computes partial results         │
│  - Example: partial_sum, partial_count│
└──────┬──────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│  SHUFFLE by Grouping Keys           │
│  (hash partitioning)                │
└──────┬──────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│  FINAL AGGREGATION                  │
│  (merge partial results)            │
│  - Produces final results           │
│  - Example: sum(partial_sums)       │
└──────┬──────────────────────────────┘
       │
       ▼
   Final Results
```

#### Selection Algorithm (AggUtils.scala lines 78-100)

```scala
def createAggregate(...): SparkPlan = {
  // Check if hash-based aggregation is supported
  val useHash = Aggregate.supportsHashAggregate(
    aggBufferAttributes, groupingExpressions)

  if (useHash && !forceSortAggregate) {
    // Preferred: HashAggregateExec
    HashAggregateExec(...)
  } else {
    // Check object hash support
    val useObjectHash = Aggregate.supportsObjectHashAggregate(...)

    if (objectHashEnabled && useObjectHash) {
      // Fallback 1: ObjectHashAggregateExec
      ObjectHashAggregateExec(...)
    } else {
      // Fallback 2: SortAggregateExec
      SortAggregateExec(...)
    }
  }
}
```

### Aggregate Strategy

**Location**: `sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala` (lines 575-668)

```scala
object Aggregation extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalAggregation(
      groupingExpressions,    // GROUP BY expressions
      aggExpressions,         // Aggregate functions
      resultExpressions,      // Final projections
      child) =>

      // Separate distinct and non-distinct aggregates
      val (functionsWithDistinct, functionsWithoutDistinct) =
        aggExpressions.partition(_.isDistinct)

      if (functionsWithDistinct.isEmpty) {
        // Standard two-phase aggregation
        AggUtils.planAggregateWithoutDistinct(...)
      } else if (functionsWithDistinct.length == 1) {
        // Single distinct aggregate
        AggUtils.planAggregateWithOneDistinct(...)
      } else {
        // Multiple distinct aggregates - more complex plan
        // Uses expand + aggregate pattern
        ...
      }
  }
}
```

### Aggregate Optimization Rules

#### 1. RemoveLiteralFromGroupExpressions
**File**: `Optimizer.scala` (line 2539)

**Purpose**: Remove constant literals from GROUP BY

**Example**:
```sql
-- Before optimization
SELECT dept, 'constant', COUNT(*) FROM employees GROUP BY dept, 'constant'

-- After optimization (constant removed from grouping)
SELECT dept, 'constant', COUNT(*) FROM employees GROUP BY dept
```

**Reason**: Literals don't affect grouping (all rows have same value)

#### 2. RemoveRepetitionFromGroupExpressions
**File**: `Optimizer.scala` (line 2617)

**Purpose**: Remove duplicate expressions from GROUP BY

**Example**:
```sql
-- Before optimization
SELECT dept, dept, COUNT(*) FROM employees GROUP BY dept, dept

-- After optimization
SELECT dept, dept, COUNT(*) FROM employees GROUP BY dept
```

#### 3. RewriteDistinctAggregates
**File**: `RewriteDistinctAggregates.scala` (20KB)

**Purpose**: Rewrite queries with multiple DISTINCT aggregates

**Problem**: Multiple distinct aggregates can't be computed in single pass

**Solution**: Expand + Aggregate pattern

**Example**:
```sql
-- Original query with multiple DISTINCTs
SELECT COUNT(DISTINCT dept), COUNT(DISTINCT city)
FROM employees

-- Rewritten as (simplified):
-- 1. Expand to create separate rows for each distinct aggregate
-- 2. First aggregation computes distinct values
-- 3. Second aggregation computes final counts
```

**Plan Pattern**:
```
HashAggregateExec (Final) - count distinct values
+- HashAggregateExec (Partial) - collect distinct values per group
   +- ExpandExec - duplicate rows with group indicators
      +- Scan
```

#### 4. DecimalAggregates
**File**: `Optimizer.scala` (line 2238)

**Purpose**: Optimize decimal precision in aggregate functions

**Example**: Avoids unnecessary precision increases in SUM/AVG of decimals

#### 5. EliminateAggregateFilter
**File**: `Optimizer.scala` (line 557)

**Purpose**: Remove aggregate FILTER clauses when always true/false

**Example**:
```sql
-- Before
SELECT COUNT(*) FILTER (WHERE true) FROM employees

-- After
SELECT COUNT(*) FROM employees
```

### Aggregate Functions Support

**Common Aggregate Functions**:
- **Distributive**: `SUM`, `COUNT`, `MIN`, `MAX` (perfect partial aggregation)
- **Algebraic**: `AVG` (partial sum + count)
- **Holistic**: `PERCENTILE`, `COLLECT_LIST` (require all data)

**Partial Aggregation**:
- `SUM(x)` → Partial: `SUM(x)`, Final: `SUM(partial_sum)`
- `COUNT(*)` → Partial: `COUNT(*)`, Final: `SUM(partial_count)`
- `AVG(x)` → Partial: `(SUM(x), COUNT(x))`, Final: `SUM(partial_sum) / SUM(partial_count)`

---

## Key Classes and Files Reference

### Parser Files

```
sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/
├── SqlBaseParser.g4          # ANTLR4 parser grammar (SQL syntax rules)
└── SqlBaseLexer.g4           # ANTLR4 lexer grammar (tokenization rules)

sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/
├── AstBuilder.scala          # 254KB - Converts parse tree to logical plan
├── AbstractSqlParser.scala   # 4KB - Base parser implementation
├── ParserInterface.scala     # 2KB - Parser trait definition
└── ParserUtils.scala         # 16KB - Parsing utilities and helpers
```

### Analyzer Files

```
sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/
├── Analyzer.scala                        # 195KB - Main analyzer with all resolution batches
├── FunctionRegistry.scala                # 50KB - Built-in function catalog
├── TypeCoercion.scala                    # 17KB - Type coercion base
├── ResolveReferencesInAggregate.scala    # Resolution for aggregate expressions
├── ResolveReferencesInSort.scala         # Resolution for sort expressions
├── ResolveWithCTE.scala                  # CTE resolution
├── ResolveSubquery.scala                 # Subquery analysis
└── [93 other resolution and analysis files]
```

### Optimizer Files

```
sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/
├── Optimizer.scala                   # 116KB - Main optimizer with all batches
├── expressions.scala                 # 54KB - Expression optimization rules
├── joins.scala                       # 22KB - Join optimization rules
├── subquery.scala                    # 53KB - Subquery optimization rules
├── CostBasedJoinReorder.scala        # 21KB - Cost-based join ordering with DP algorithm
├── PropagateEmptyRelation.scala      # 10KB - Empty relation propagation
├── InlineCTE.scala                   # 10KB - CTE inlining logic
├── RewriteDistinctAggregates.scala   # 20KB - Distinct aggregate rewriting
└── [37 other optimization rule files]
```

### Physical Planning Files

```
sql/core/src/main/scala/org/apache/spark/sql/execution/
├── SparkPlanner.scala        # 118 lines - Main physical planner
├── SparkStrategies.scala     # 53KB - All physical planning strategies
├── QueryExecution.scala      # 27KB - Orchestrates query execution phases
├── SparkPlan.scala           # 23KB - Base class for physical operators
└── SparkPlanInfo.scala       # 3KB - Physical plan metadata

sql/core/src/main/scala/org/apache/spark/sql/execution/joins/
├── BroadcastHashJoinExec.scala         # 10KB - Broadcast hash join implementation
├── ShuffledHashJoinExec.scala          # 25KB - Shuffle hash join implementation
├── SortMergeJoinExec.scala             # 60KB - Sort-merge join implementation
├── BroadcastNestedLoopJoinExec.scala   # 21KB - Broadcast nested loop join
├── CartesianProductExec.scala          # 4KB - Cartesian product implementation
├── HashJoin.scala                      # 28KB - Hash join utilities
└── [10 other join-related files]

sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/
├── AggUtils.scala                      # 27KB - Aggregate planning utilities
├── HashAggregateExec.scala             # 36KB - Hash-based aggregation
├── SortAggregateExec.scala             # 5KB - Sort-based aggregation
├── ObjectHashAggregateExec.scala       # 6KB - Object hash aggregation
├── MergingSessionsExec.scala           # 4KB - Session window aggregation
└── [15 other aggregate files]
```

### Logical Plan Files

```
sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/
├── LogicalPlan.scala               # 20KB - Base logical plan trait
├── basicLogicalOperators.scala     # 82KB - Project, Filter, Join, Aggregate, etc.
├── joinTypes.scala                 # Join type definitions
├── cteOperators.scala              # 11KB - CTE operators
└── [27 other logical plan operator files]
```

---

## End-to-End Query Examples

### Example 1: Aggregate Query

**SQL Query**:
```sql
SELECT dept, COUNT(*), AVG(salary)
FROM employees
WHERE age > 30
GROUP BY dept
```

#### Phase 1: Parsing

**Input**: SQL string
**Output**: Unresolved logical plan

```
Aggregate [dept#unresolved]
  aggregateExpressions=[
    Alias(Count(Literal(1)), "count(1)")#1,
    Alias(Avg(salary#unresolved), "avg(salary)")#2
  ]
  child=Filter (age#unresolved > 30)
    child=UnresolvedRelation [employees]
```

**Code**: `AstBuilder.visitQuerySpecification()` creates this structure

#### Phase 2: Analysis

**Process**:
1. `ResolveRelations` → Resolves `employees` table from catalog
2. `ResolveReferences` → Resolves `dept`, `age`, `salary` columns with types
3. `ResolveReferencesInAggregate` → Validates aggregate expressions
4. `TypeCoercion` → Ensures `age > 30` comparison is valid

**Output**: Resolved logical plan

```
Aggregate [dept#5: string]
  aggregateExpressions=[
    Alias(Count(1), "count(1)")#10L,
    Alias(Avg(salary#7: decimal(10,2)), "avg(salary)")#11: decimal(14,6)
  ]
  child=Filter (age#6: int > 30)
    child=Relation[employees] [id#4, dept#5, age#6, salary#7, ...]
```

#### Phase 3: Optimization

**Rules Applied**:

1. **PushDownPredicates** → Filter pushed to scan (if Parquet/ORC)
2. **ColumnPruning** → Only read needed columns: `dept`, `age`, `salary`
3. **ConstantFolding** → `30` already a literal, no change
4. **RemoveLiteralFromGroupExpressions** → No literals in GROUP BY
5. **PropagateEmptyRelation** → Check if table is empty (not applicable)

**Output**: Optimized logical plan

```
Aggregate [dept#5]
  aggregateExpressions=[count(1)#10L, avg(salary#7)#11]
  child=Filter (age#6 > 30)
    child=Relation[employees] [dept#5, age#6, salary#7]  // Only needed columns
```

#### Phase 4: Physical Planning

**Strategy**: `Aggregation` strategy matches

**Process** (`AggUtils.planAggregateWithoutDistinct`):
1. Check if hash aggregation supported → **Yes**
2. Create two-phase aggregation plan
3. Insert shuffle exchange for distribution

**Output**: Physical plan

```
HashAggregateExec (mode=Final)
  keys=[dept#5]
  functions=[sum(count#15L), sum(sum#16), sum(count#17L)]
  output=[dept#5, count(1)#10L, avg(salary)#11]
+- ShuffleExchangeExec hashpartitioning(dept#5, 200)
   +- HashAggregateExec (mode=Partial)
        keys=[dept#5]
        functions=[count(1), sum(salary#7), count(salary#7)]
        output=[dept#5, count#15L, sum#16, count#17L]
      +- FilterExec (age#6 > 30)
         +- FileSourceScanExec
              Location: hdfs://path/to/employees
              PushedFilters: [IsNotNull(age), GreaterThan(age,30)]
              ReadSchema: struct<dept:string,age:int,salary:decimal(10,2)>
```

#### Phase 5: Execution

**Execution Flow**:

1. **FileSourceScanExec**:
   - Reads Parquet/ORC files
   - Pushes filter `age > 30` to file format (skips row groups)
   - Only reads columns: `dept`, `age`, `salary`

2. **FilterExec**:
   - Additional in-memory filtering (if needed)

3. **HashAggregateExec (Partial)**:
   - Each partition builds hash map: `dept → (count, sum, count)`
   - No shuffle yet (local aggregation)

4. **ShuffleExchangeExec**:
   - Hash partitions by `dept` (same dept → same partition)
   - Network shuffle

5. **HashAggregateExec (Final)**:
   - Each partition receives all rows for subset of depts
   - Merges partial results: `sum(counts)`, `sum(sums) / sum(counts)`
   - Produces final results

**Performance Characteristics**:
- **Scan Efficiency**: Column pruning + filter pushdown
- **Aggregation**: Two-phase reduces shuffle data
- **Memory**: Hash map sized by number of unique depts

---

### Example 2: Join Query

**SQL Query**:
```sql
SELECT e.name, d.dept_name
FROM employees e
JOIN departments d ON e.dept_id = d.id
WHERE e.age > 30
```

#### Phase 1: Parsing

**Output**: Unresolved logical plan

```
Project [name#unresolved, dept_name#unresolved]
  child=Join Inner, (dept_id#unresolved = id#unresolved)
    left=Filter (age#unresolved > 30)
      child=SubqueryAlias e
        child=UnresolvedRelation [employees]
    right=SubqueryAlias d
      child=UnresolvedRelation [departments]
```

#### Phase 2: Analysis

**Output**: Resolved logical plan

```
Project [name#2: string, dept_name#12: string]
  child=Join Inner, (dept_id#3: int = id#11: int)
    left=Filter (age#4: int > 30)
      child=SubqueryAlias e
        child=Relation[employees] [id#1, name#2, dept_id#3, age#4, ...]
    right=SubqueryAlias d
      child=Relation[departments] [id#11, dept_name#12, ...]
```

#### Phase 3: Optimization

**Rules Applied**:

1. **EliminateSubqueryAliases** → Remove `SubqueryAlias` nodes (e, d)
2. **PushDownPredicates** → Push `age > 30` below join
3. **ColumnPruning** →
   - From `employees`: only need `name`, `dept_id`, `age`
   - From `departments`: only need `id`, `dept_name`
4. **ReorderJoin** → Not applicable (single join)
5. **CostBasedJoinReorder** → Not applicable (single join)
6. **InferFiltersFromConstraints** → Could infer filters on join keys

**Output**: Optimized logical plan

```
Project [name#2, dept_name#12]
  child=Join Inner, (dept_id#3 = id#11)
    left=Filter (age#4 > 30)
      child=Relation[employees] [name#2, dept_id#3, age#4]
    right=Relation[departments] [id#11, dept_name#12]
```

#### Phase 4: Physical Planning

**Strategy**: `JoinSelection` strategy

**Decision Process**:

1. **Check for equi-join keys** → Yes: `dept_id = id`
2. **Check if hashable** → Yes (both integers)
3. **Check broadcast eligibility**:
   - Get `departments` table size → Assume 500 KB
   - Compare to `spark.sql.autoBroadcastJoinThreshold` (10 MB)
   - **500 KB < 10 MB** → Can broadcast!
4. **Choose**: `BroadcastHashJoinExec` with BuildRight (broadcast departments)

**Output**: Physical plan

```
ProjectExec [name#2, dept_name#12]
+- BroadcastHashJoinExec [dept_id#3], [id#11], Inner, BuildRight
   :- FilterExec (age#4 > 30)
   :  +- FileSourceScanExec (employees)
   :       ReadSchema: struct<name:string,dept_id:int,age:int>
   :       PushedFilters: [IsNotNull(age), GreaterThan(age,30), IsNotNull(dept_id)]
   +- BroadcastExchangeExec
      +- FileSourceScanExec (departments)
           ReadSchema: struct<id:int,dept_name:string>
           PushedFilters: [IsNotNull(id)]
```

**Why Broadcast Join**:
- `departments` is small (500 KB)
- Broadcasting once to all executors cheaper than shuffling `employees`
- Each executor can independently join local `employees` data with broadcast `departments`

#### Phase 5: Execution

**Execution Flow**:

1. **FileSourceScanExec (departments)**:
   - Scan `departments` table
   - Read only columns: `id`, `dept_name`
   - Apply filter: `IsNotNull(id)`

2. **BroadcastExchangeExec**:
   - Collect all departments data to driver
   - Build hash table: `id → dept_name`
   - Broadcast hash table to all executors
   - Serialized size: ~500 KB

3. **FileSourceScanExec (employees)** (parallel on all executors):
   - Scan `employees` table (partitioned across executors)
   - Read only columns: `name`, `dept_id`, `age`
   - Apply pushed filters: `age > 30`, `IsNotNull(dept_id)`

4. **FilterExec**:
   - Additional filtering if needed

5. **BroadcastHashJoinExec** (parallel on all executors):
   - For each `employees` row:
     - Extract `dept_id`
     - Probe broadcast hash table with `dept_id`
     - If match found, output `(name, dept_name)`
   - No shuffle needed (all data available locally)!

6. **ProjectExec**:
   - Select final columns: `name`, `dept_name`

**Performance Characteristics**:
- **No shuffle**: Major performance win!
- **Network**: Only broadcast once (~500 KB) vs shuffle employees data
- **Parallelism**: Full parallelism on employees scan and join
- **Memory**: Each executor holds broadcast hash table (~500 KB)

**Alternative (if departments was large)**:

If `departments` was 100 GB, the plan would use `SortMergeJoinExec`:

```
ProjectExec [name#2, dept_name#12]
+- SortMergeJoinExec [dept_id#3], [id#11], Inner
   :- SortExec [dept_id#3 ASC]
   :  +- ShuffleExchangeExec hashpartitioning(dept_id#3, 200)
   :     +- FilterExec (age#4 > 30)
   :        +- FileSourceScanExec (employees)
   +- SortExec [id#11 ASC]
      +- ShuffleExchangeExec hashpartitioning(id#11, 200)
         +- FileSourceScanExec (departments)
```

**Execution with Sort-Merge**:
1. Scan both tables
2. Shuffle both sides by join key
3. Sort both sides
4. Merge sorted streams

**Performance**:
- **Two shuffles**: More expensive
- **Two sorts**: CPU overhead
- **Scalability**: Better for large-large joins

---

## Configuration and Tuning

### Optimizer Configuration

**Key Settings** (from `SQLConf.scala`):

| Configuration | Default | Description |
|--------------|---------|-------------|
| `spark.sql.optimizer.maxIterations` | 100 | Maximum iterations for optimizer fixed-point batches |
| `spark.sql.optimizer.excludedRules` | "" | Comma-separated list of rule names to exclude |
| `spark.sql.optimizer.inSetConversionThreshold` | 10 | Minimum list size for converting IN to InSet |
| `spark.sql.cbo.enabled` | false | Enable cost-based optimization |
| `spark.sql.cbo.joinReorder.enabled` | false | Enable cost-based join reordering |
| `spark.sql.cbo.joinReorder.dp.threshold` | 12 | Max tables for dynamic programming join reorder |
| `spark.sql.cbo.starSchemaDetection` | false | Enable star schema detection |
| `spark.sql.constraintPropagation.enabled` | true | Enable constraint and filter inference |

### Join Configuration

| Configuration | Default | Description |
|--------------|---------|-------------|
| `spark.sql.autoBroadcastJoinThreshold` | 10MB | Max size for broadcast join (set -1 to disable) |
| `spark.sql.join.preferSortMergeJoin` | true | Prefer sort-merge over shuffle hash join |
| `spark.sql.adaptive.enabled` | true | Enable adaptive query execution |
| `spark.sql.adaptive.coalescePartitions.enabled` | true | Coalesce shuffle partitions after shuffle |
| `spark.sql.adaptive.skewJoin.enabled` | true | Handle skewed joins adaptively |
| `spark.sql.shuffle.partitions` | 200 | Number of shuffle partitions |

### Aggregate Configuration

| Configuration | Default | Description |
|--------------|---------|-------------|
| `spark.sql.execution.useObjectHashAggregateExec` | true | Enable ObjectHashAggregateExec fallback |
| `spark.sql.aggregate.removeRepetitionFromGroupExpressions` | true | Remove duplicate grouping expressions |

### General Performance Tuning

| Configuration | Default | Description |
|--------------|---------|-------------|
| `spark.sql.files.maxPartitionBytes` | 128MB | Max bytes per file partition |
| `spark.sql.inMemoryColumnarStorage.compressed` | true | Compress cached data |
| `spark.sql.inMemoryColumnarStorage.batchSize` | 10000 | Batch size for columnar cache |
| `spark.sql.parquet.filterPushdown` | true | Push filters to Parquet |
| `spark.sql.parquet.aggregatePushdown` | false | Push aggregates to Parquet (experimental) |

### Enabling Cost-Based Optimization

**Step 1**: Collect table statistics
```sql
ANALYZE TABLE employees COMPUTE STATISTICS;
ANALYZE TABLE employees COMPUTE STATISTICS FOR ALL COLUMNS;
```

**Step 2**: Enable CBO
```python
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")
spark.conf.set("spark.sql.statistics.histogram.enabled", "true")
```

**Step 3**: Verify statistics
```sql
DESCRIBE EXTENDED employees;
```

### Debugging and Observability

**View Logical Plan**:
```scala
df.explain(mode = "simple")    // Physical plan only
df.explain(mode = "extended")  // Parsed, analyzed, optimized, physical
df.explain(mode = "cost")      // With cost info (if CBO enabled)
df.explain(mode = "formatted") // Pretty-printed
```

**View Optimizer Rules Applied**:
```scala
// In logs with DEBUG level for org.apache.spark.sql.catalyst.optimizer
```

**Adaptive Query Execution (AQE) Insights**:
- View in Spark UI → SQL tab → Query details
- Shows re-optimization decisions
- Displays skew handling, partition coalescing

---

## Summary

### Key Architectural Principles

1. **Separation of Concerns**:
   - Parser (syntax) → Analyzer (semantics) → Optimizer (performance) → Planner (execution)

2. **Rule-Based + Cost-Based**:
   - 122+ rule-based optimizations for correctness and common patterns
   - Cost-based optimization for join ordering using statistics

3. **Extensibility**:
   - Custom rules can be added
   - Custom strategies for physical planning
   - Catalog plugins for external metadata

4. **Two-Phase Execution**:
   - Logical optimization (platform-independent)
   - Physical planning (Spark-specific)

### Critical Optimization Rules

**Must-Know Rules**:
- **PushDownPredicates** (line 45): Pushes filters close to sources
- **ColumnPruning** (line 51): Removes unused columns
- **ConstantFolding** (line 70): Evaluates constants at compile time
- **CostBasedJoinReorder** (line 100): Optimal join ordering with stats
- **InferFiltersFromConstraints** (line 95): Infers additional filters

### Join Selection Priority

1. **Broadcast Hash Join**: Best performance for small tables
2. **Shuffle Hash Join**: Good for medium skewed joins
3. **Sort Merge Join**: Default for large-large joins
4. **Broadcast Nested Loop**: Fallback for non-equi joins
5. **Cartesian Product**: Avoid unless necessary

### Aggregate Selection

1. **HashAggregateExec**: Default, best performance
2. **ObjectHashAggregateExec**: Complex types fallback
3. **SortAggregateExec**: High cardinality, memory-constrained

### Performance Best Practices

1. **Enable Statistics**: Run `ANALYZE TABLE` for CBO
2. **Tune Broadcast Threshold**: Adjust `autoBroadcastJoinThreshold`
3. **Use Column Formats**: Parquet/ORC for predicate/column pushdown
4. **Enable AQE**: Better runtime optimizations
5. **Monitor Query Plans**: Use `explain()` to verify optimization

---

## File Location Quick Reference

| Component | Primary File | Location |
|-----------|-------------|----------|
| Parser | AstBuilder.scala | sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/ |
| Analyzer | Analyzer.scala | sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/ |
| Optimizer | Optimizer.scala | sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/ |
| Physical Planner | SparkStrategies.scala | sql/core/src/main/scala/org/apache/spark/sql/execution/ |
| Join Execution | joins/*.scala | sql/core/src/main/scala/org/apache/spark/sql/execution/joins/ |
| Aggregate Execution | aggregate/*.scala | sql/core/src/main/scala/org/apache/spark/sql/execution/aggregate/ |
| Query Execution | QueryExecution.scala | sql/core/src/main/scala/org/apache/spark/sql/execution/ |

---

**Total Optimizer Rules**: 122+
**Join Implementations**: 5
**Aggregate Implementations**: 3
**Optimization Batches**: 20+
**Codebase Size**: ~500MB (Catalyst + SQL Core)

This guide provides comprehensive coverage of Spark SQL internals from parsing through execution, with detailed explanations of all optimizer rules and end-to-end examples for joins and aggregates.
