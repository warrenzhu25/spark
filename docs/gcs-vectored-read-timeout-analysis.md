# GCS Vectored Read Timeout Analysis

## Problem Statement

When VM network latency increases from 1ms to 5ms, `TimeoutException` occurs in
`ParquetFileReader.readFromVectoredRange()` during Parquet file reads from Google Cloud Storage.

## Complete Call Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           PARQUET LAYER                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ParquetFileReader.readRowGroup()                                           │
│         │                                                                   │
│         ▼                                                                   │
│  internalReadRowGroup()                                                     │
│         │                                                                   │
│         ▼                                                                   │
│  readAllPartsVectoredOrNormal()                                             │
│         │                                                                   │
│         ├── shouldUseVectoredIo() ── true ──►                               │
│         │                                                                   │
│         ▼                                                                   │
│  readVectored()                                                             │
│         │                                                                   │
│         ├── Create List<ParquetFileRange> from ConsecutivePartList          │
│         │   (groups column chunks by offset for efficient I/O)              │
│         │                                                                   │
│         ▼                                                                   │
│  f.readVectored(ranges, allocator)   ◄── Submit to filesystem               │
│         │                                                                   │
│         ▼                                                                   │
│  readFromVectoredRange() ◄── Called per ConsecutivePartList                 │
│         │                                                                   │
│         ├── FutureIO.awaitFuture(                                           │
│         │       range.getDataReadFuture(),                                  │
│         │       HADOOP_VECTORED_READ_TIMEOUT_SECONDS,  ◄── 300 seconds      │
│         │       TimeUnit.SECONDS)                                           │
│         │                                                                   │
│         ├── On timeout: throw IOException(                                  │
│         │     "Timeout while fetching result for %s with time limit %d")    │
│         │                                                                   │
│         ▼                                                                   │
│  [Returns ByteBuffer, builds ChunkList]                                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           HADOOP LAYER                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  HadoopStreams$SeekableInputStream.readVectored()                           │
│         │                                                                   │
│         ▼                                                                   │
│  FSDataInputStream.readVectored(ranges, allocator)                          │
│         │                                                                   │
│         ▼                                                                   │
│  PositionedReadable.readVectored()  ◄── Hadoop 3.3+ API                     │
│         │                                                                   │
│         ▼                                                                   │
│  [Delegates to GCS connector's InputStream]                                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       GCS CONNECTOR LAYER                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  GoogleHadoopFSInputStream.readVectored(ranges, allocator)                  │
│         │                                                                   │
│         ├── Check: channel instanceof ReadVectoredSeekableByteChannel?      │
│         │                                                                   │
│         ▼  (Yes - native vectored support)                                  │
│  Convert FileRange → VectoredIORange                                        │
│         │                                                                   │
│         ├── For each range:                                                 │
│         │     CompletableFuture<ByteBuffer> future = new CompletableFuture  │
│         │     range.setData(future)  ◄── Future returned to Parquet         │
│         │                                                                   │
│         ▼                                                                   │
│  channel.readVectored(vectoredRanges, allocator)                            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       GCS BIDI CHANNEL LAYER                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  GoogleCloudStorageBidiReadChannel.readVectored(ranges, allocator)          │
│         │                                                                   │
│         ▼                                                                   │
│  getBlobReadSession()                                                       │
│         │                                                                   │
│         ├── sessionFuture.get(                                              │
│         │       readOptions.getBidiClientTimeout(),  ◄── TIMEOUT POINT 1   │
│         │       TimeUnit.SECONDS)                        (default: 30s)    │
│         │                                                                   │
│         ├── Creates bidirectional streaming session to GCS                  │
│         │                                                                   │
│         ▼                                                                   │
│  BlobReadSession session = ...                                              │
│         │                                                                   │
│         ▼                                                                   │
│  For each VectoredIORange:                                                  │
│         │                                                                   │
│         ├── ApiFuture<DisposableByteString> readFuture =                    │
│         │       session.readAs(RangeSpec.of(offset, length))                │
│         │                                                                   │
│         ├── ApiFutures.addCallback(readFuture, new FutureCallback<>() {     │
│         │       onSuccess(data) {                                           │
│         │         ByteBuffer buffer = allocate.apply(length);               │
│         │         buffer.put(data.asByteBuffer());                          │
│         │         completableFuture.complete(buffer);                       │
│         │       }                                                           │
│         │       onFailure(throwable) {                                      │
│         │         completableFuture.completeExceptionally(throwable);       │
│         │       }                                                           │
│         │     }, boundedThreadPool);                                        │
│         │                                                                   │
│         ▼                                                                   │
│  [Returns immediately - async completion via callbacks]                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       GCS gRPC CLIENT LAYER                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  BlobReadSession.readAs(RangeSpec)                                          │
│         │                                                                   │
│         ▼                                                                   │
│  gRPC ReadObjectRequest                                                     │
│         │                                                                   │
│         ├── Per-message timeout  ◄── TIMEOUT POINT 2 (default: 3s)          │
│         │   Controls max wait time for each gRPC message                    │
│         │                                                                   │
│         ├── Stream timeout  ◄── TIMEOUT POINT 3 (default: 1 hour)           │
│         │   Controls overall stream duration                                │
│         │                                                                   │
│         ▼                                                                   │
│  [Network I/O: TLS handshake, HTTP/2 frames, data transfer]                 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       NETWORK / GCS SERVICE                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐         ┌─────────────┐         ┌─────────────┐           │
│  │   Client    │ ──5ms──►│   Network   │ ──5ms──►│  GCS API    │           │
│  │    (VM)     │◄──5ms── │  (latency)  │◄──5ms── │  (Server)   │           │
│  └─────────────┘         └─────────────┘         └─────────────┘           │
│                                                                             │
│  Round-trip latency impact:                                                 │
│  - Connection establishment: ~3 RTT = 30ms                                  │
│  - TLS handshake: ~2-3 RTT = 20-30ms                                        │
│  - HTTP/2 setup: ~1 RTT = 10ms                                              │
│  - Per-message overhead: ~1 RTT = 10ms                                      │
│  - With retries on failure: multiplied                                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Timeout Configuration Summary

| Layer | Timeout Point | Default Value | Configuration Property |
|-------|---------------|---------------|------------------------|
| Parquet | Await vectored read future | 300 seconds | `HADOOP_VECTORED_READ_TIMEOUT_SECONDS` (hardcoded) |
| GCS Connector | Bidi session initialization | 30 seconds | `fs.gs.bidi.client.timeout` |
| GCS gRPC | Per-message timeout | 3 seconds | `fs.gs.grpc.read.message.timeout` |
| GCS gRPC | Stream timeout | 1 hour | `fs.gs.grpc.read.timeout` |
| GCS Retry | Max backoff elapsed time | 2 minutes | Backoff configuration |

## Root Cause Analysis

### Why Latency Increase Causes Timeout

When network latency increases from 1ms to 5ms (5x increase), the cumulative effect
across multiple operations can exceed timeout thresholds:

1. **Bidi Session Initialization (30s timeout)**
   - Requires multiple round-trips: DNS, TCP connect, TLS handshake, HTTP/2 setup
   - With 5ms latency: ~10 RTT × 10ms = 100ms minimum
   - Under load or with retries, can approach timeout threshold

2. **gRPC Message Timeout (3s timeout)** - Most likely culprit
   - Each range read involves multiple gRPC messages
   - With vectored reads, many concurrent requests compete for resources
   - Network jitter on top of base latency can push individual messages past 3s
   - Calculation: If message processing + 2 RTT + jitter > 3s → timeout

3. **Cascading Failures**
   - One slow request can block thread pool workers
   - Retry attempts add more latency
   - Connection pool exhaustion leads to new connection overhead

### Vectored Read Amplification Factor

For a typical Parquet read with N column chunks:
- Sequential read: N requests, serialized latency
- Vectored read: N concurrent requests, parallel but resource-contending

With increased latency:
- More requests in-flight simultaneously
- Higher memory pressure (buffers allocated but waiting)
- Thread pool saturation
- Connection multiplexing overhead

## Solutions

### Solution 1: Increase GCS Timeout Configurations (Recommended)

```properties
# Spark configuration (spark-defaults.conf or SparkSession)

# Increase gRPC per-message timeout (default: 3 seconds)
# This is the most likely cause of timeout with increased latency
spark.hadoop.fs.gs.grpc.read.message.timeout=30000

# Increase bidi client initialization timeout (default: 30 seconds)
spark.hadoop.fs.gs.bidi.client.timeout=60

# Increase overall gRPC read timeout if needed (default: 1 hour)
spark.hadoop.fs.gs.grpc.read.timeout=7200000
```

Or in Hadoop `core-site.xml`:
```xml
<configuration>
  <property>
    <name>fs.gs.grpc.read.message.timeout</name>
    <value>30000</value>
    <description>gRPC message timeout in milliseconds</description>
  </property>
  <property>
    <name>fs.gs.bidi.client.timeout</name>
    <value>60</value>
    <description>Bidi client initialization timeout in seconds</description>
  </property>
</configuration>
```

### Solution 2: Tune Vectored Read Parameters

Reduce concurrency and merge more ranges to decrease request count:

```properties
# Reduce concurrent read threads (default: 16)
# Fewer threads = less contention, more predictable latency
spark.hadoop.fs.gs.vectored.read.threads=4

# Increase merged range size (default varies)
# Larger ranges = fewer requests = less timeout risk
spark.hadoop.fs.gs.vectored.read.merged.range.max.size=8388608

# Increase minimum seek distance before splitting ranges
spark.hadoop.fs.gs.vectored.read.min.range.seek.size=524288
```

### Solution 3: Disable Vectored IO (Fallback)

If timeouts persist, disable vectored IO to use sequential reads:

```properties
# Disable vectored IO in Parquet
spark.hadoop.parquet.hadoop.vectored.io.enabled=false
```

Trade-off: Lower throughput but more reliable with high latency.

### Solution 4: Connection Pool Tuning

Ensure sufficient connections for parallel requests:

```properties
# Increase max connections if using HTTP/1.1 fallback
spark.hadoop.fs.gs.http.max.connections=20

# Connection timeout
spark.hadoop.fs.gs.http.connect-timeout=30000
```

### Solution 5: Retry Configuration

Configure more aggressive retries for transient failures:

```properties
# Increase max retry attempts
spark.hadoop.fs.gs.max.retries=10

# Adjust backoff parameters
spark.hadoop.fs.gs.backoff.initial.interval.millis=500
spark.hadoop.fs.gs.backoff.max.interval.millis=30000
spark.hadoop.fs.gs.backoff.max.elapsed.time.millis=300000
```

## Recommended Configuration for High-Latency Environments

```properties
# === GCS Timeout Configuration for High-Latency Networks ===

# gRPC timeouts (most important for latency tolerance)
spark.hadoop.fs.gs.grpc.read.message.timeout=30000
spark.hadoop.fs.gs.grpc.read.timeout=7200000
spark.hadoop.fs.gs.bidi.client.timeout=60

# Vectored read tuning (reduce parallelism, increase batch size)
spark.hadoop.fs.gs.vectored.read.threads=4
spark.hadoop.fs.gs.vectored.read.merged.range.max.size=8388608
spark.hadoop.fs.gs.vectored.read.min.range.seek.size=524288

# Retry configuration (more tolerant of transient failures)
spark.hadoop.fs.gs.max.retries=10
spark.hadoop.fs.gs.backoff.max.elapsed.time.millis=300000
```

## Monitoring and Debugging

### Enable Debug Logging

```properties
# GCS connector debug logging
spark.hadoop.fs.gs.logging.level=DEBUG

# gRPC debug logging (verbose)
spark.executor.extraJavaOptions=-Dio.grpc.netty.level=DEBUG
```

### Key Metrics to Monitor

1. **Request latency distribution** - P50, P95, P99
2. **Timeout rate** - Timeouts per minute
3. **Retry rate** - Retries per request
4. **Connection pool utilization** - Active vs idle connections
5. **Thread pool queue depth** - Pending vectored read tasks

### Debugging Commands

```scala
// Check active GCS configuration
spark.sparkContext.hadoopConfiguration.iterator().asScala
  .filter(e => e.getKey.startsWith("fs.gs"))
  .foreach { e => println(s"${e.getKey} = ${e.getValue}") }

// Check Parquet vectored IO setting
spark.conf.get("spark.hadoop.parquet.hadoop.vectored.io.enabled", "not set")
```

## References

- [GCS Connector Configuration](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md)
- [Parquet Vectored IO Support](https://github.com/apache/parquet-java/pull/1139)
- [Hadoop Vectored IO API](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/filesystem/fsdatainputstream.html#void_readVectored_List_FileRange_ranges_IntFunction_allocate_)
