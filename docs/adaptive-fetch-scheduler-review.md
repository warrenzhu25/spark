# Adaptive Fetch Scheduler Review Notes

This document expands the review feedback for the adaptive shuffle fetch scheduler design, with more context, impact, and remediation suggestions.

## 1) Bottleneck dequeue drops unsent requests
- **Where**: `docs/adaptive-fetch-scheduler.md:129-132`
- **Issue**: `fetchRequests.dequeueAll(...).headOption` removes *all* bottleneck requests from the queue but only sends the first one; the remaining bottleneck requests are discarded and never re-enqueued.
- **Impact**: Bottleneck host can stall permanently once the queue is accidentally drained, causing hangs or silent data loss in the scheduler state machine.
- **Suggestion**: Avoid destructive filtering. Prefer a non-destructive peek/find for the next bottleneck request, or iterate the dequeued list and immediately re-enqueue the unused requests. Alternatively, keep two queues (bottleneck/non-bottleneck) to avoid O(N) filtering and accidental drops.

## 2) Missing executor size defaults can throw
- **Where**: `docs/adaptive-fetch-scheduler.md:65-74`
- **Issue**: `executorDataSizes(request.address)` assumes the address is present. Retries after block movement or unexpected locals will throw `NoSuchElementException`.
- **Impact**: Scheduler can fail mid-fetch when block locations change or when local/peer addresses are absent from the initial size map.
- **Suggestion**: Use `getOrElse` with a safe default (0) and treat unknown addresses as non-bottleneck with minimal weight. Also guard the denominator when computing ratios to avoid division by zero.

## 3) Bottleneck never re-evaluates as data drains
- **Where**: `docs/adaptive-fetch-scheduler.md:24-47`
- **Issue**: Bottleneck is picked once at initialization using total data per executor; it never changes as data is consumed.
- **Impact**: After the initial bottleneck drains, it continues to receive top priority even when it no longer dominates remaining bytes. The next heaviest executor may be starved, elongating tail latency and reducing utilization.
- **Suggestion**: Track remaining bytes per executor and recompute the bottleneck when a fetch completes (or periodically). A simple heuristic: when remaining bytes on the current bottleneck fall below the next executor, rotate the bottleneck. Add hysteresis to avoid flapping.

## 4) Bottleneck concurrency lacks safety rails
- **Where**: `docs/adaptive-fetch-scheduler.md:85-116, 128-163`
- **Issue**: Bottleneck is allowed 3 concurrent requests without per-address byte caps, backoff, or fairness guarantees.
- **Impact**: The bottleneck host can monopolize `bytesInFlight`, starving other executors and potentially exceeding remote limits, leading to timeouts or regressions on balanced workloads.
- **Suggestion**: Add per-address byte ceilings, adaptive backoff when RTT/timeout increases, and a fairness floor (e.g., always reserve at least one slot for non-bottleneck requests when there is bytes budget). Consider decaying concurrency for the bottleneck if throughput drops.

## 5) Wait-time feedback ignored for bottleneck
- **Where**: `docs/adaptive-fetch-scheduler.md:164-193`
- **Issue**: Wait-time penalties are skipped for the bottleneck, so a heavily overloaded bottleneck host still dominates scheduling even when global coordination reports high wait times.
- **Impact**: Self-inflicted slowdown: the scheduler keeps sending to a saturated host, prolonging total fetch time and increasing tail latency.
- **Suggestion**: Blend wait-time feedback into the bottleneck path. Examples: cap the bottleneck’s share when observed latency exceeds a threshold, reduce its bonus proportionally to wait time, or temporarily downgrade it until latency recovers. Document the interplay between bottleneck priority and wait-aware penalties.
