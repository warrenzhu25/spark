# Detailed Decommission Reasoning Plan

Goal: replace the generic `spark scale down` decommission reason with structured, specific messages such as `Idle after 120s` or `Shuffle 100 MB could not migrate before 120s timeout`, and surface the detail consistently in logs, event logs, UI, and APIs.

## Objectives and Success Criteria
- Provide reason codes plus human-readable templates that include relevant parameters (idle duration, configured thresholds, shuffle bytes/timeouts, host-level signals).
- Preserve backward compatibility: existing consumers still receive the message string, but new structured fields are added and populated.
- Ensure the extra detail flows end-to-end: creation (`ExecutorAllocationManager`), scheduler state (`ExecutorDecommissionReason`/`DecommissionSummary`), event log, REST API, and UI.
- Add tests that assert both the structured payload and rendered messages for idle scale-down and shuffle-timeout scenarios.

## Current Behavior (baseline)
- Dynamic allocation calls `ExecutorDecommissionReason("spark scale down")` when removing idle executors (core `ExecutorAllocationManager`, streaming variant too).
- Scheduler and UI only see the opaque string; no parameters (idle duration, thresholds, shuffle migration state) are recorded, so diagnoses rely on logs.
- `ExecutorDecommissionReason` carries only `message: String` and optional `workerHost`, limiting structured reporting.

## Proposed Schema and Message Catalog
- Extend `ExecutorDecommissionReason` with optional structured fields (kept binary-compatible by adding `Option[...]` defaults):
  - `reasonCode: String` (e.g., `idle_timeout`, `shuffle_timeout`, `manual`, `host_lost`, `preemption`).
  - `details: Map[String, String]` for parameters: `idleDurationMs`, `idleThresholdMs`, `shuffleBytes`, `shuffleMigratedBytes`, `shuffleTimeoutMs`, `stageId`, `rpId`, `trigger`.
  - `timestampMs` to record when the decision was made.
- Keep `message: String` as a rendered template for legacy consumers; generate it from code + details via a small catalog:
  - `idle_timeout`: `Idle after {idleDurationMs} ms (threshold {idleThresholdMs} ms)`
  - `shuffle_timeout`: `Shuffle {shuffleBytes} bytes could not migrate before {shuffleTimeoutMs} ms (migrated {shuffleMigratedBytes} bytes)`
  - `manual` / `admin`: `Manually decommissioned ({trigger})`
  - `host_lost`: `Host {workerHost} decommissioned; shuffle may be lost`
- Add a helper to produce both the structured payload and the formatted message to avoid hand-built strings across call sites.

## Instrumentation Plan
- Idle decommission path (`ExecutorAllocationManager.removeExecutors` and streaming counterpart):
  - Capture last idle start time, current time, configured idle timeout, resource profile id, and number of tasks at decision time.
  - Populate `reasonCode = idle_timeout`, `idleDurationMs`, `idleThresholdMs`, `rpId`, `trigger = dynamic_allocation`.
- Shuffle/time-based decommission (when storage migration exceeds timeout or fails):
  - Capture target bytes to migrate, migrated bytes, timeout thresholds, number of failed blocks/peers.
  - Populate `reasonCode = shuffle_timeout`, `shuffleBytes`, `shuffleMigratedBytes`, `shuffleTimeoutMs`, `failedPeers`.
- Host-level decommission:
  - Preserve `workerHost`; ensure reason code distinguishes host vs. executor-only decommission.

## Propagation and Surfacing
- Scheduler state: store structured info on `ExecutorDecommissionReason` and ensure `ExecutorDecommissionState` and `DecommissionSummary` include reason code/message.
- Event log: extend `SparkListenerExecutorRemoved` payload (or add a side-channel) to emit `reasonCode`, `details`, and `timestampMs` fields; keep the legacy `reason` string intact.
- REST API / JSON responses: include the new fields for executor removal/decommission endpoints.
- UI: Executor tab and history server should show the concise rendered message, with an expandable view or tooltip for key parameters.
- Driver/executor logs: log both the reason code and the rendered message for grep-ability.
- Metrics/telemetry: tag existing decommission metrics (e.g., `numberExecutorsDecommissionUnfinished`) with reason code where cheap.

## Compatibility and Configurability
- Defaults: if no structured fields are provided (older callers), render the existing message unchanged.
- Config flag (optional): `spark.decommission.reason.detail.enabled` to allow rolling back to string-only behavior if downstream tooling breaks.
- Serialization: adding optional fields with defaults keeps Jackson/RPC compatible; add regression tests across event-log replay.

## Testing Plan
- Unit tests for new reason builder: map codes + details to messages; missing fields fall back gracefully.
- Dynamic allocation tests: assert that idle scale-down emits `idle_timeout` with correct durations; streaming allocation variant covered.
- Shuffle decommission tests: simulate migration timeout and validate emitted `shuffle_timeout` details.
- Event log/History Server tests: ensure new fields appear in JSON and UI renders the enriched message without NPEs when absent.
- Backward compatibility tests: replay an event log without new fields to ensure parsing still works.

## Delivery Steps
1. Add schema changes (`ExecutorDecommissionReason`), reason catalog helper, and default rendering.
2. Instrument dynamic allocation idle path (core + streaming) to populate structured fields; keep message backward compatible.
3. Instrument storage/shuffle timeout paths to emit structured details.
4. Propagate through scheduler summaries, event log payloads, REST API, and UI rendering.
5. Update docs/config descriptions and add tests (unit + integration + history server replay).
6. Roll out behind optional config if needed; monitor for downstream ingestion issues before removing the legacy string-only usage.
