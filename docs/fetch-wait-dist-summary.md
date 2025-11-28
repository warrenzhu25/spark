# Shuffle Fetch Wait Distribution: Full Change Summary

## Scope
Capture shuffle fetch wait-time distributions per remote executor, rank top contributors at the task, executor, and driver levels, and log the worst offenders. Disabled by default and fully config-gated.

## Data model
- `ShuffleFetchWaitStat` combines an aggregate (`ShuffleFetchWaitAggregate`: total wait, count) and a distribution (`ShuffleFetchWaitDistribution`: quantiles + values).
- Container: `ExecutorShuffleFetchWaitStats(stats: Seq[ShuffleFetchWaitStat])` using Spark's standard quantiles `[0.0, 0.25, 0.5, 0.75, 1.0]`.

## Configs
- `spark.shuffle.fetchWaitStats.enabled` (default `false`)
- `spark.shuffle.fetchWaitStats.topK` (default `3`)
- `spark.shuffle.fetchWaitStats.logInterval` (optional duration; if set, the driver logs periodically in addition to stage-completion logging)

## Collection and propagation
- Task-level capture: `ShuffleBlockFetcherIterator` records wait by remote executor and writes top-K summaries into `TaskMetrics.shuffleFetchWaitStats` when enabled.
- Executor aggregation: `ShuffleFetchWaitStatsAggregator` merges per-task stats into executor-level top-K using weighted buckets over the standard quantiles; heartbeats carry these summaries.
- Driver aggregation/logging: DAGScheduler merges executor summaries, drops state when executors are lost, and logs the global top-K periodically (if `logInterval` set) and after every stage completion.

## Serialization and APIs
- Proto: `TaskMetrics` includes repeated `shuffle_fetch_wait_stats` entries; each `ShuffleFetchWaitStat` carries aggregate fields plus `quantiles` and `values` arrays.
- JSON / event log: `JsonProtocol` reads and writes `shuffleFetchWaitStats`.
- Status store: `StageDataWrapperSerializer` persists the new stats.
- Heartbeats: payload includes optional `ExecutorShuffleFetchWaitStats`; driver/task scheduler plumbing accepts the new field.

## Logging example
`Top shuffle fetch wait contributors (after stage 5): exec-3 total=1.2 s (count=42, min=50, p25=80, p50=120, p75=150, max=220), exec-7 ...`
