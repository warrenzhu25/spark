# Design Document: Spark SQL Split Optimization via Column Pruning

## 1. Overview & Objective

In analytical workloads (e.g., TPCDS, enterprise data lakes), wide tables with hundreds of columns are common. However, individual queries often project only a small subset of these columns. 

Standard Spark SQL file source scanning partitions data into splits based purely on on-disk file sizes (`spark.sql.files.maxPartitionBytes`, defaulting to 128MB). When a query prunes 90% of the columns, a 128MB on-disk split might yield only 12MB of actual data in memory. This results in:
* Excessive task scheduling overhead.
* Suboptimal I/O throughput due to small read requests.
* Unnecessary metadata calls to storage systems (like HDFS or GCS).

**Objective**: Dynamically increase the maximum split size for file scans when column pruning is active, enabling Spark to pack more rows into a single task partition and significantly reducing end-to-end query latency and storage API calls.

---

## 2. High-Level Architecture

The optimization intercepts the physical planning phase of file source scans (specifically `FileSourceScanExec`). Before constructing the `RDD[InternalRow]`, the system calculates a **Column Pruning Reduction Factor (`pruningFactor`)**.

```
pruningFactor = (Estimated Required Row Size) / (Estimated Total Row Size)
```

This factor (where `0 < pruningFactor <= 1.0`) is passed into `FilePartition.maxSplitBytes`, which scales the target split size:

```
Adjusted Max Split Bytes = min( Default Max Split Bytes / pruningFactor, Total Partition Bytes )
```

If a query projects only 20% of a table's data volume (`pruningFactor = 0.2`), the maximum split size is increased by 5x (e.g., from 128MB to 640MB). Tasks will read larger chunks of files on disk but consume roughly the same uncompressed memory as an unpruned 128MB scan.

---

## 3. Detailed Technical Design & Code Reference

### 3.1. Hooking into `FileSourceScanExec`
The optimization is anchored in `FileSourceScanExec.scala`. During the initialization of `inputRDD`, the system checks if the optimization should run and delegates factor calculation to `DataSourceScanExecUtil`.

```scala
// In FileSourceScanExec.scala

private[sql] lazy val shouldOptimizeSplit = {
  val forceDisableOptimizeSplit = this.getTagValue(DISABLE_OPTIMIZED_SPLIT_TAG)
  if (forceDisableOptimizeSplit.isDefined && forceDisableOptimizeSplit.get) {
    false
  } else {
    conf.optimizedSplitEnabled
  }
}

lazy val inputRDD: RDD[InternalRow] = {
  // ... reader initialization ...

  val colPruningReductionFactor =
    DataSourceScanExecUtil.calculateColPruningReductionFactor(
      shouldOptimizeSplit,
      enableMultiThread = true,
      sparkContext.statusTracker.getExecutorInfos.length
        * relation.sparkSession.conf.get(SparkLauncher.EXECUTOR_CORES, "1").toInt,
      relation.fileFormat,
      requiredSchema,
      relation.dataSchema,
      conf.getConf(SQLConf.PARQUET_COMPRESSION_FACTOR),
      conf.getConf(SQLConf.PARQUET_SPLIT_ESTIMATE_SAMPLE_SIZE),
      dynamicallySelectedPartitions
    )

  val readRDD = if (bucketedScan) {
    createBucketedReadRDD(relation.bucketSpec.get, readFile, dynamicallySelectedPartitions)
  } else {
    createReadRDD(readFile, dynamicallySelectedPartitions, colPruningReductionFactor)
  }
  // ...
}
```

### 3.2. Operator Guardrail: `DisableOptimizedSplit` Rule
`ExpandExec` duplicates rows in memory (used for `ROLLUP`/`CUBE`). If large input splits are fed into `ExpandExec`, executors can suffer acute Out-Of-Memory (OOM) errors.

A dedicated physical plan rule (`DisableOptimizedSplit.scala`) traverses the plan. If an `ExpandExec` is detected, it tags all `FileSourceScanExec` nodes to disable split optimization.

```scala
// In DisableOptimizedSplit.scala

object DisableOptimizedSplit extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.DATAPROC_OPTIMIZED_SPLIT_ENABLED) ||
      conf.getConf(SQLConf.DATAPROC_OPTIMIZED_SPLIT_WITH_EXPAND_ENABLED) ||
      !containsExpand(plan)) {
      return plan
    }

    plan.transform {
      case f: FileSourceScanExec =>
        f.setTagValue(DISABLE_OPTIMIZED_SPLIT_TAG, true)
        f
    }
  }

  @tailrec
  private def containsExpand(plan: SparkPlan): Boolean = {
    plan match {
      case a: AdaptiveSparkPlanExec => containsExpand(a.inputPlan)
      case other => other.exists(_.isInstanceOf[ExpandExec])
    }
  }
}
```

### 3.3. The Core Estimation Engine: `DataSourceScanExecUtil`
The estimation engine handles fixed-length, nested, and variable-length schemas.

```scala
// In DataSourceScanExecUtil.scala

def calculateColPruningReductionFactor(
    optimizedSplitEnabled: Boolean,
    enableMultiThread: Boolean,
    totalExecutorsCoreCnt: Int,
    fileFormat: FileFormat,
    requiredSchema: StructType,
    relationOriginalDataSchema: StructType,
    parquetCompressionFactor: Double,
    parquetSplitEstimateSampleSize: Int,
    filePathList: Array[Path]): Double = {
  
  if (optimizedSplitEnabled && fileFormat.isInstanceOf[ParquetSource]) {
    var baseRowSize = relationOriginalDataSchema.fields
      .map(_.dataType.defaultSize.toLong).sum.toDouble
    var outputRowSize = requiredSchema.map(_.dataType.defaultSize.toLong).sum.toDouble
    
    // Tier 1 & 2: Fixed-Length and verified nested fixed-length schemas
    if (verifyFixedLengthRecursively(requiredSchema)) {
      return Math.min(outputRowSize / baseRowSize * parquetCompressionFactor, 1.0)
    } else {
      // Tier 3: Variable-length estimation via dynamic footer sampling
      try {
        if (parquetSplitEstimateSampleSize == 0) return 1.0
        
        var filePaths: Seq[Path] = filePathList.toSeq
        // Early exit if file count is too low relative to executor capacity
        if (totalExecutorsCoreCnt >= filePaths.length * 2) return 1.0

        if (filePaths.size > parquetSplitEstimateSampleSize) {
          filePaths = scala.util.Random.shuffle(filePaths).take(parquetSplitEstimateSampleSize)
        } else {
          // Do not sample if file count <= sample size threshold
          return 1.0 
        }

        val nonFixedLengthColumnsPaths = getFlattenedNonFixedLengthColumnPaths(requiredSchema)
        val nonFixedLengthColumnsIndex = findNonFixedLengthColumnsIndexFromColumnChunk(
          filePaths, nonFixedLengthColumnsPaths.map(_._1)
        )

        implicit val ec: ExecutionContext = getExecutionContext(enableMultiThread)
        val conf = new Configuration()
        
        // Async footer reading
        val futures = filePaths.map { path =>
          Future {
            val footers = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER)
            var blockRowCount: Long = 0L
            val blockColumnSizes = new Array[Long](nonFixedLengthColumnsIndex.length)

            footers.getBlocks.forEach { block =>
              blockRowCount += block.getRowCount
              val blockColumns = block.getColumns.asScala
              nonFixedLengthColumnsIndex.zipWithIndex.foreach { case (columnIndex, index) =>
                blockColumnSizes(index) += blockColumns(columnIndex).getTotalUncompressedSize
              }
            }
            (blockRowCount, blockColumnSizes)
          }
        }

        // Timeboxed resolution (3 seconds SLA)
        val tableSampleScanTimeout = 3.seconds
        val results = try {
          ThreadUtils.awaitResult(Future.sequence(futures), tableSampleScanTimeout)
        } catch {
          case e: Throwable =>
            // Collect partial successes if timeout reached
            ThreadUtils.awaitResult(Future.sequence(futures.filter(_.isCompleted)), 1.millisecond)
        }

        // Fallback if < 50% of sampled files succeed
        if (results.length <= 0 || results.length * 2 < filePaths.length) return 1.0

        val rowCounter = results.map(_._1).sum
        val nonFixedLengthTypeSizeByColumn = results.map(_._2).transpose.map(_.sum)

        // Recalibrate base and output row sizes with actual uncompressed estimates
        for (index <- nonFixedLengthColumnsIndex.indices) {
          val estimateSizeThisIndex: Double = nonFixedLengthTypeSizeByColumn(index) / rowCounter.toDouble
          
          // Nuance: Divide by compression factor when adding to outputRowSize 
          // because final return multiplies outputRowSize by compression factor.
          outputRowSize += estimateSizeThisIndex / parquetCompressionFactor
          outputRowSize -= nonFixedLengthColumnsPaths(index)._2.defaultSize.toDouble
          baseRowSize += estimateSizeThisIndex
          baseRowSize -= nonFixedLengthColumnsPaths(index)._2.defaultSize.toDouble
        }

        return Math.min(outputRowSize / baseRowSize * parquetCompressionFactor, 1.0)
      } catch {
        case e: Throwable => logWarning(s"Failed estimation: ${e.getMessage}")
      }
      return 1.0
    }
  }
  1.0
}
```

### 3.4. Reconciling User Configurations in `FilePartition`
When `pruningFactor` is passed down to `FilePartition.maxSplitBytes`, the system checks if the user explicitly configured `spark.sql.files.maxPartitionBytes`. If so, the user's configuration acts as a strict upper ceiling, preventing the system from generating splits larger than the user intended.

```scala
// In FilePartition.scala

def maxSplitBytes(sparkSession: SparkSession, calculateTotalBytes: => Long,
                  pruningFactor: Double): Long = {
  val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
  
  // Scale based on the baseline default value (128MB)
  val optimizedMaxSplitBytes =
    (SQLConf.FILES_MAX_PARTITION_BYTES.defaultValue.get / pruningFactor).toLong
  
  val prunedMaxSplitBytes =
    if (sparkSession.sessionState.conf.settings.containsKey(SQLConf.FILES_MAX_PARTITION_BYTES.key)) {
      // If user explicitly configured maxPartitionBytes, respect it as a hard ceiling
      logInfo(s"spark.sql.files.maxPartitionBytes is set to ${defaultMaxSplitBytes}, respect " +
        s"this config by minimizing it with pruned max splits to avoid big table scan tasks")
      Math.min(optimizedMaxSplitBytes, defaultMaxSplitBytes)
    } else {
      optimizedMaxSplitBytes
    }
    
  val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
  val minPartitionNum = sparkSession.sessionState.conf.filesMinPartitionNum
    .getOrElse(sparkSession.leafNodeDefaultParallelism)
  val totalBytes = calculateTotalBytes
  val bytesPerCore = totalBytes / minPartitionNum

  Math.min(prunedMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
}
```

---

## 4. Configurations

| Configuration Key | Default | Description |
| :--- | :--- | :--- |
| `spark.dataproc.sql.optimized.split.enabled` | `false` | Master switch to enable optimized splitting based on column pruning. Requires enhanced execution flags to be active. |
| `spark.dataproc.sql.optimized.split.with.expand.enabled` | `false` | If true, allows split optimization even when `ExpandExec` is present in the plan. |
| `spark.dataproc.sql.compressionFactor.parquet`| `3.0` | Compression factor used to calibrate split size estimation for Parquet files. |
| `spark.dataproc.sql.parquet.splitEstimate.sampleSize` | `10` | Number of files to sample when estimating variable-length column sizes. |

---

## 5. Summary of Guardrails & Fallbacks
1. **Timeout Fallback**: If Parquet footer sampling exceeds 3 seconds and fails to collect >50% of samples, defaults to `pruningFactor = 1.0`.
2. **Exception Fallback**: Any exception during schema flattening or footer reading gets caught, logged as a warning, and defaults to `pruningFactor = 1.0`.
3. **Expand Protection**: Disabled if `ExpandExec` is present in the physical plan unless explicitly overridden by configuration.
4. **Small Scan Bypass**: If total executor cores `>= filePaths.length * 2`, or if `filePaths.size <= sampleSize`, estimation is bypassed (`pruningFactor = 1.0`).
