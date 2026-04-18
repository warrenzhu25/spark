# Spark History Server: Export / Import Snapshot Design

## Summary

Large Spark event logs are expensive to move, expensive to replay, and awkward to share across
environments. A Spark History Server (SHS) export / import feature should allow an already-built
application state store to be packaged into a compact, portable snapshot and restored later without
replaying the original event log.

The feature should satisfy four requirements:

1. `Export` must be smaller and easier to distribute than raw event logs for large applications.
2. `Import` must be much faster than rebuilding UI state from event log replay.
3. The format must support multiple versions with explicit compatibility rules.
4. Exported data must preserve enough information for normal SHS UI and API behavior.

This design introduces a versioned `SHS snapshot` artifact containing:

- snapshot manifest and compatibility metadata
- application and attempt identity
- compressed serialized entity records for the app store
- optional integrity metadata and indexes for fast import

The exported snapshot is an interchange format. It is not the same as the local LevelDB / RocksDB
layout used by the current SHS disk cache.

## Problem

Today the SHS reconstructs application UI state by replaying event logs into an `ElementTrackingStore`
through `FsHistoryProvider.rebuildAppStore()`.

That works, but it has drawbacks for large applications:

- event logs can be extremely large and inconvenient to share
- replay is CPU-heavy and latency-heavy
- local app stores are backend-specific and not intended as portable artifacts
- sharing raw logs leaks more information than a curated UI-state snapshot may need to

The current SHS disk cache helps repeated local access, but it does not solve portability across:

- machines
- clusters
- users
- storage backends
- SHS versions

## Goals

- Export a completed application attempt from SHS into a portable artifact.
- Import the artifact into another SHS instance without replaying the original event log.
- Keep export / import independent of local KVStore backend details.
- Make export compact, streamable, and integrity-checked.
- Support multiple snapshot format versions and explicit compatibility validation.
- Preserve UI/API semantics for the imported application.

## Non-Goals

- Replacing the current KVStore implementation.
- Supporting arbitrary cross-major-version compatibility forever.
- Exporting in-progress applications in the first version.
- Making the artifact a general-purpose Spark debugging format outside SHS.
- Preserving event-log-level fidelity for reprocessing unrelated to SHS UI behavior.

## Design Overview

### Core Idea

Instead of shipping the original event log, SHS exports the already-materialized app state store into
a portable `snapshot` file.

The flow becomes:

1. SHS reconstructs app state once from the event log, as it does today.
2. User exports that state into a versioned snapshot artifact.
3. Another SHS imports the snapshot.
4. Imported snapshot is validated and materialized into a normal local app store.
5. SHS opens that local store directly, bypassing event-log replay.

This keeps the existing `AppStatusStore` and UI code paths mostly unchanged after import.

### Why Not Export the Local LevelDB / RocksDB Directory Directly

Exporting the raw local store directory is fast, but it is the wrong interchange boundary:

- format depends on backend choice (`LevelDB` or `RocksDB`)
- serialization format can differ by configuration
- metadata compatibility is tied to current local store semantics
- backend files are not stable or ergonomic as a portable artifact

Instead, export should use a backend-neutral logical record stream and let import rebuild the local
store in the target SHS's preferred backend.

## Snapshot Format

### Artifact

Export produces a single file:

```text
<app-id>[_<attempt-id>].shs
```

Internally, the artifact is a container with:

- `manifest.json`
- `records.bin`
- optional `checksums.bin`
- optional `stats.json`

The outer container can be a standard archive format such as ZIP for simplicity and streaming
support. The payload record stream should be independently compressed for better size efficiency.

### Manifest

The manifest is mandatory and versioned.

Fields:

- `snapshotFormatVersion`
- `minReaderVersion`
- `maxReaderVersion` or compatible reader range
- `sparkVersion`
- `appStatusStoreVersion`
- `serializerFormat`
- `appId`
- `attemptId`
- `eventLogRoot` if known
- `applicationCompleted`
- `exportTimestamp`
- `recordCounts` by entity type
- `compressionCodec`
- `checksumAlgorithm`
- `featureFlags`

The compatibility contract is based on the snapshot format version, not on the event log version.

### Records

`records.bin` stores entity records grouped by logical type, not by backend file layout.

Examples:

- `ApplicationInfoWrapper`
- `ApplicationEnvironmentInfoWrapper`
- `JobDataWrapper`
- `StageDataWrapper`
- `TaskDataWrapper` or future light form
- `ExecutorSummaryWrapper`
- `ExecutorStageSummaryWrapper`
- `RDDStorageInfoWrapper`
- `CachedQuantile`
- plugin-provided records, if exportable

Each record is serialized using a snapshot-specific serializer contract.

### Compression

Compression should be optimized for:

- better density than raw JSON event logs
- fast decompression during import
- streamability

Recommended default:

- block compression with Zstandard

Why:

- better compression ratio than gzip in practice
- faster decode than high-ratio gzip
- supports independent blocks for chunked import

### Record Grouping

To make import quick, records should be grouped by entity type and optionally by parent locality.

Suggested order:

1. metadata and top-level singleton records
2. application / environment / resource profile records
3. jobs and stages
4. executors and executor-stage summaries
5. task records
6. cached quantiles and derived summaries
7. plugin extension records

This allows import to:

- write records sequentially
- build frequently-used metadata early
- stream large task sections without holding the whole artifact in memory

## Export Design

### Trigger Points

Export should work for completed applications already known to SHS.

Two entry points:

1. HTTP endpoint in History Server
2. local CLI command for admin / batch use

Recommended v1:

- add server-side export endpoint first
- optionally add CLI wrapper later using the same internal service

### HTTP API

New endpoint:

```text
GET /api/v1/applications/<appId>/export
GET /api/v1/applications/<appId>/<attemptId>/export
```

Behavior:

- validates access permissions the same way application UI access is validated
- streams the snapshot to the client
- does not require the raw event log to be shipped to the caller

Optional query parameters:

- `formatVersion`
- `compression`
- `includeTasks=true|false`
- `taskDetailLevel=full|light`

For v1, keep this simple:

- completed applications only
- include all records needed for equivalent UI/API behavior

### Export Source

Export must read from a fully built app store, not from event logs.

Preferred source order:

1. already-open app store from app cache if available
2. on-disk app store if available
3. rebuild store from event log only if no app store exists

That preserves the "quick share" goal in the common case.

### Export Implementation

Add an internal `HistoryServerSnapshotService` responsible for:

- opening the source app store
- enumerating exportable entity types
- streaming typed records into the snapshot
- writing manifest and checksums

Suggested interface:

```scala
trait AppStoreSnapshotService {
  def exportSnapshot(
      store: KVStore,
      appId: String,
      attemptId: Option[String],
      out: OutputStream,
      options: SnapshotExportOptions): Unit

  def importSnapshot(
      in: InputStream,
      target: File,
      conf: SparkConf,
      options: SnapshotImportOptions): ImportedSnapshotInfo
}
```

## Import Design

### Import Goal

Import should produce a ready-to-open SHS local app store without replaying event logs.

Import path:

1. read manifest
2. validate compatibility
3. create temporary target store
4. stream records into the target `KVStore`
5. commit the store into the SHS disk manager location
6. register application metadata in the listing store

After that, SHS loads the imported app exactly as if it had rebuilt the app from the event log.

### HTTP API

New endpoint:

```text
POST /api/v1/applications/import
```

Request body:

- snapshot artifact stream

Response:

- imported `appId`
- `attemptId`
- snapshot metadata
- whether the import replaced an existing store

For v1, imports should be admin-only.

### Import Validation

Import must reject incompatible artifacts before writing data.

Checks:

- snapshot format version is supported
- required features are supported
- snapshot says the application is complete
- serializer version is supported
- `appStatusStoreVersion` is compatible with the running SHS
- checksums match
- imported app identity does not conflict unexpectedly

### Conflict Policy

If the target SHS already has the same app / attempt:

- default behavior: reject unless `replace=true`

If `replace=true`:

- remove the existing local store for that app attempt
- replace it with the imported snapshot result

This keeps behavior explicit and safe.

## Versioning and Compatibility

### Why a Separate Snapshot Version

Snapshot compatibility must not be inferred from:

- Spark event log version
- local KVStore backend
- serializer implementation detail

Export / import needs its own stable compatibility contract.

### Version Model

Manifest fields:

- `snapshotFormatVersion`
- `minReaderVersion`
- `writerSparkVersion`
- `writerStoreVersion`
- `featureFlags`

Rules:

- readers must reject newer mandatory features they do not understand
- readers may accept older snapshots if format version is in the supported range
- backward compatibility is best-effort across nearby SHS versions, not unbounded forever

Recommended compatibility target:

- same major snapshot format version
- same or compatible `AppStatusStore.CURRENT_VERSION`

### Migration Strategy

When `AppStatusStore` schema evolves:

- either keep snapshot readers able to map old record forms into current wrappers
- or bump the snapshot format and reject incompatible imports clearly

Do not silently import partial or semantically changed data.

## Size Optimization

### Main Principle

The snapshot should optimize for SHS behavior, not for reproducing the raw event stream.

That means export should store:

- final app UI state
- derived summaries
- only the detail needed by UI/API

and avoid:

- replay-only transient structures
- redundant ordering data already recoverable from record values
- backend-specific file overhead

### Space Reduction Techniques

- use protobuf or another compact binary encoding instead of JSON
- compress blocks with Zstandard
- deduplicate repeated strings through per-block dictionaries
- group records by type to improve compression ratio
- store optional large fields only when non-empty
- allow a future `light` export mode that omits task-level details

### Export Profiles

Support explicit profiles:

- `full`
  - preserve full SHS task-level behavior
- `light`
  - preserve stage/job/executor summaries and limited task detail
- `summary`
  - preserve overview pages and aggregated metrics only

Recommended v1:

- implement `full`
- reserve manifest support for `light` and `summary`

## Performance Design

### Fast Export

Export should avoid loading all records into memory.

Requirements:

- iterate source store by record type
- stream encode and compress incrementally
- compute checksums per block
- avoid creating giant intermediate byte arrays

### Fast Import

Import should also be streaming:

- read manifest first
- decode one block at a time
- write directly into a temporary target store
- finalize metadata at the end

Import must bypass:

- event-log parsing
- listener replay
- derived metric recomputation

This is the primary performance win.

### Chunking

Use chunked record blocks, for example by:

- entity type
- fixed uncompressed target size

Benefits:

- lower peak memory
- parallelizable checksum generation later
- better recovery and diagnostics

## Plugin and Extension Support

SHS plugins can write their own records during replay. Export/import must not silently corrupt or
drop plugin data that the UI depends on.

Manifest should support plugin sections:

- plugin identifier
- plugin snapshot schema version
- plugin-required flag

Rules:

- if a required plugin payload is present but the importer lacks that plugin, reject import
- optional plugin payload may be skipped with a warning

## Security and Access Control

### Export

Export exposes application data, so it must honor the same application view permissions used by SHS.

### Import

Import changes server state, so it should be admin-only.

### Integrity

Each snapshot should include:

- manifest checksum
- per-block checksums
- optional full-artifact checksum

This protects against accidental corruption in transfer and storage.

### Authenticity

Optional later enhancement:

- signed snapshots for trusted exchange between environments

Not required for v1.

## Operational Behavior

### Storage Placement

Imported applications should be stored in the same local SHS disk manager path used for normal app
stores so existing eviction and lifecycle logic continues to work.

### Listing Store Integration

After import, SHS should create or update the listing metadata so the imported application appears in:

- application list page
- REST API listing
- cache loading path

### Eviction

Imported stores should be subject to the same disk usage and eviction policies as replay-built stores.

### Provenance

Manifest should preserve provenance fields:

- imported from snapshot
- exporter Spark version
- export timestamp
- optional original log path

This helps debugging and support.

## Implementation Plan

### New Internal Components

1. `HistoryServerSnapshotService`
   - core export/import orchestration
2. `SnapshotManifest`
   - versioned metadata model
3. `SnapshotRecordWriter`
   - typed streaming writer with compression
4. `SnapshotRecordReader`
   - typed streaming reader with validation
5. `SnapshotCompatibility`
   - version / feature gating logic

### History Server Integration

Add to `HistoryServer` / `ApplicationHistoryProvider` stack:

- export servlet / REST endpoint
- import servlet / REST endpoint
- provider method to open imported stores
- optional listing annotations for imported apps

### FsHistoryProvider Integration

Extend the provider to:

- import snapshots into `HistoryServerDiskManager` storage
- open imported app stores with existing `KVUtils.open(...)`
- skip `rebuildAppStore(...)` when a valid imported store already exists

## Testing

Add tests for:

- export then import round-trip preserves key UI/API data
- import is faster than replay for representative large applications
- incompatible snapshot version is rejected cleanly
- corrupted payload checksum is rejected
- plugin-required snapshot without plugin is rejected
- `replace=true` conflict policy works
- imported stores are evicted correctly by disk manager
- `full` snapshot preserves task pages and quantile summaries

## Recommendation

Implement export/import as a portable, versioned SHS snapshot format built from app store contents,
not from raw local DB files and not from event logs.

Recommended v1 decisions:

- completed applications only
- one-file snapshot artifact
- backend-neutral logical record stream
- protobuf-like compact binary records
- Zstandard block compression
- streaming export/import
- admin-only import, permission-checked export
- explicit snapshot format versioning with strict validation

This provides the biggest operational win:

- much smaller and easier artifacts to share
- far faster restore than event-log replay
- compatibility control across SHS versions
- minimal disruption to existing `AppStatusStore` and UI code paths
