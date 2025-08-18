# Shuffle Fetch Wait Top-K Contributors: Executor → Driver Reporting

## Goal
Surface shuffle fetch wait hotspots. Each executor reports its top 3 remote executors contributing to shuffle fetch wait time (total wait + distribution). The driver aggregates across executors and logs the global top 3 offenders.

## Scope and Phasing
1) **Data model & configs**
   - Add `TopShuffleWaitStat(executorId, totalWaitMs, count, p50, p90, p95, p99, max)`.
   - Configs:
     - `spark.shuffle.fetchWaitStats.enabled` (default: false).
     - `spark.shuffle.fetchWaitStats.topK` (default: 3).
     - `spark.shuffle.fetchWaitStats.logInterval` (optional, e.g., seconds or heartbeats).
2) **Executor-side collection & reporting**
   - `ShuffleBlockFetcherIterator` already tracks per-fetch waits. Export top-K per task.
   - Plumb top-K into `TaskMetrics` (new optional field).
   - Executor-level aggregator merges per-task stats into executor top-K using bounded percentile sketches (e.g., `QuantileSummaries` or reservoir) to avoid unbounded samples.
   - Extend executor heartbeat payload to carry executor top-K when enabled.
3) **Driver-side aggregation & logging**
   - Extend heartbeat handling to accept executor top-K stats.
   - Maintain `remoteExecId -> AggregatedWait { totalWait, count, percentileSketch }`.
   - Periodically (logInterval) and/or on stage completion compute global top-K by total wait and log:
     - `Global shuffle wait top3: exec-5 total=XXs (count=…, p50=…, p90=…, p95=…, p99=…, max=…)`.
   - Exclude local executor IDs and fallback IDs.
4) **Tests**
   - Serialization/deserialization of new payloads (JSON/protobuf/task metrics).
   - Executor aggregator unit tests (merge and emit top-K).
   - Driver aggregator unit tests (ingest heartbeats, produce top-K, log selection).
   - Ensure existing suites stay green (network-common/shuffle unaffected).

## Design Details
### Data Structures
- `TopShuffleWaitStat`: per-remote-executor summary.
- `ExecutorTopShuffleWaitStats`: `Seq[TopShuffleWaitStat]`, bounded by `topK`.
- Aggregator sketch: bounded percentile structure (prefer `QuantileSummaries`; fallback reservoir + sort).

### Executor Side
- Source: `ShuffleBlockFetcherIterator`’s wait tracker; expose top-K per task when enabled.
- Task metrics: new optional field in `TaskMetrics` carrying `ExecutorTopShuffleWaitStats`.
- Executor aggregator:
  - On task completion, ingest task top-K into a per-executor map.
  - Maintain per remote exec `{totalWait, count, sketch}`.
  - On heartbeat, emit executor top-K by `totalWait` with percentiles computed from the sketch.
- Heartbeat payload: optional field for executor top-K when config enabled. Backward compatible (older drivers ignore).

### Driver Side
- Heartbeat handling: parse optional executor top-K and update global aggregator.
- Aggregator: merges totals/counts and sketches for each remote executor; computes percentiles on demand.
- Logging: on a schedule (logInterval) and/or stage completion, log global top-K by total wait with distributions. Include stage/app context; exclude local/fallback executors.

### Configs
- `spark.shuffle.fetchWaitStats.enabled` (boolean, default false).
- `spark.shuffle.fetchWaitStats.topK` (int, default 3).
- `spark.shuffle.fetchWaitStats.logInterval` (duration; if unset, log on stage completion only).
- Optional: `spark.shuffle.fetchWaitStats.sampleSize` if reservoir sampling is chosen.

### Compatibility & Safety
- Fully optional; disabled by default. No behavioral change when off.
- New fields are optional in task metrics/heartbeat payloads; deserialization must default safely.
- Percentile sketches keep memory bounded; top-K keeps payload small.

## Work Breakdown
1) Data model & configs:
   - Add `TopShuffleWaitStat`, `ExecutorTopShuffleWaitStats`.
   - Add configs to `SparkConf`/`config`.
2) Executor task metrics:
   - Extend `TaskMetrics` with optional top-K; update JSON/protobuf serializers.
   - Export top-K from `ShuffleBlockFetcherIterator` when enabled.
3) Executor aggregator + heartbeat:
   - Implement per-executor aggregator with sketches.
   - Extend heartbeat message to include executor top-K (config-gated).
4) Driver aggregation + logging:
   - Extend heartbeat handling to accept stats.
   - Implement driver aggregator and logging hook (logInterval + stage completion).
5) Tests:
   - Serialization tests for new fields.
   - Aggregator unit tests (executor and driver).
   - Existing suites (network-common/shuffle/core TaskMetrics) stay green.
6) Validation:
   - Run targeted module tests; optional integration-style heartbeat ingestion/log assertion.

## Open Questions
- Percentile implementation choice (QuantileSummaries vs reservoir).
- Logging cadence default (stage completion vs periodic).
- UI exposure (future): per-stage top-K contributors table.
