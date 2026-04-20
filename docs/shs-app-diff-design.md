# SHS App Diff Design

## Summary

Add a cross-application compare flow to the Spark History Server (SHS):

1. On the SHS application list page, the user can select exactly two application attempts.
2. SHS opens a new `Diff` tab / page with those two attempts encoded in the URL.
3. A new SHS plugin owns the diff page, computes the comparison from the two loaded app stores,
   and renders:
   - app-level metrics derived from stage-level metrics
   - top 3 diff jobs
   - top 3 diff stages
   - top 3 diff SQL executions
4. Within each section, diff order is `abs(durationDiffMs)` descending.

This needs a small SHS core change first, because the current plugin API only extends a
single loaded `SparkUI`. It does not let a plugin add a history-server-level tab or page that can
access two applications at once.

## Goals

- Support comparing two completed application attempts from the SHS app list page.
- Keep the compare URL shareable and deterministic.
- Reuse existing SHS loading, ACL, and app-store code paths.
- Keep most diff logic in a plugin, not in SHS core.
- Reuse SQL history data when present.

## Non-Goals

- N-way comparison.
- Comparing running applications.
- Perfect semantic matching for every job / stage / SQL across unrelated applications.
- Introducing a new public REST API unless it is clearly needed later.

## Current State

### App list page

The SHS home page is rendered by:

- [HistoryPage.scala](/Users/warren/github/spark/core/src/main/scala/org/apache/spark/deploy/history/HistoryPage.scala)
- [historypage.js](/Users/warren/github/spark/core/src/main/resources/org/apache/spark/ui/static/historypage.js)
- [historypage-template.html](/Users/warren/github/spark/core/src/main/resources/org/apache/spark/ui/static/historypage-template.html)

The table is fully client-rendered from `/api/v1/applications`.

### Plugin model

Current history plugins implement:

- [AppHistoryServerPlugin.scala](/Users/warren/github/spark/core/src/main/scala/org/apache/spark/status/AppHistoryServerPlugin.scala)

and are invoked from [FsHistoryProvider.scala](/Users/warren/github/spark/core/src/main/scala/org/apache/spark/deploy/history/FsHistoryProvider.scala)
only after a single application attempt has been loaded into a `SparkUI`:

- `plugin.setupUI(ui)`

This is sufficient for SQL / streaming / thriftserver tabs inside one app UI, but it is not
sufficient for a history-server-level compare page.

### Existing diff backend

There is already a coarse endpoint:

- [ApplicationDiffResource.scala](/Users/warren/github/spark/core/src/main/scala/org/apache/spark/status/api/v1/ApplicationDiffResource.scala)

It compares two apps, but only returns a small set of aggregate counters. It does not provide:

- app metrics derived from stage metrics
- top diff jobs
- top diff stages
- top diff SQL executions
- a UI entry point from the SHS app list page

That existing endpoint is useful context, but it is not sufficient for the requested feature.

## Main Constraint

The feature is cross-application, but the current plugin API is single-application.

That means the implementation must first add a history-server-level extension point so a plugin can:

- register a `Diff` tab under the SHS root UI
- receive app selection from the app list page
- load two application UIs or stores through `HistoryServer.withSparkUI(...)`
- enforce existing SHS ACL checks for both applications

## Proposal

### Phase 1: Minimal SHS Core Changes

### 1. Add a history-server-level plugin hook

Extend `AppHistoryServerPlugin` with a new no-op hook:

```scala
def setupHistoryServerUI(historyServer: HistoryServer): Unit = {}
```

Then call it once from `HistoryServer.initialize()`, before the server binds.

Why this shape:

- It keeps one plugin discovery mechanism.
- Existing plugins remain unchanged.
- A diff plugin can mount a root-level `WebUITab` on `HistoryServer`.
- Per-app plugin setup in `FsHistoryProvider.getAppUI()` remains unchanged.

Alternative:

- create a new `HistoryServerRootPlugin` trait

This is cleaner conceptually, but it adds another service-loading surface. For this feature, the
single-trait extension is simpler.

### 2. Allow SHS root UI tabs

No major framework change is needed here. `HistoryServer` already extends `WebUI`, so a plugin can
attach a root-level `WebUITab`.

The diff plugin will create something like:

```scala
private class HistoryDiffTab(server: HistoryServer) extends WebUITab(server, "diff")
```

and attach one main page:

```scala
attachPage(new HistoryDiffPage(this, server))
server.attachTab(this)
```

### 3. Add two-app selection to the history app list page

Update the SHS app list page JS / template so the table includes a selection control per row.

Selection rules:

- selection is at the application attempt row level, not the logical app row level
- exactly two rows can be selected
- rows from incomplete apps are excluded in v1
- when a row has no `attemptId`, the URL simply omits that parameter

UI behavior:

- add a leading checkbox column
- show a sticky or toolbar-level `Compare` action above the table
- disable `Compare` until exactly two rows are selected
- when clicked, open:

```text
/diff/?appId1=...&attemptId1=...&appId2=...&attemptId2=...
```

This URL is the contract between SHS core and the plugin.

### 4. Keep the compare URL plugin-owned

SHS core should not compute diff data. Core only:

- collects two selected attempts
- navigates to the plugin page
- provides the plugin mount point

This keeps the feature modular and minimizes core ownership.

### Phase 2: SHS Diff Plugin

Implement a new plugin, ideally under `sql/core`, because it needs direct access to
`SQLAppStatusStore` for SQL execution diffing and can also read core `AppStatusStore` data.

Suggested classes:

- `ApplicationDiffHistoryServerPlugin`
- `HistoryDiffTab`
- `HistoryDiffPage`
- `HistoryDiffModel`
- `HistoryDiffStoreHelper`

### Plugin lifecycle

At SHS startup:

- `HistoryServer.initialize()` loads plugins
- diff plugin receives `setupHistoryServerUI(historyServer)`
- plugin registers `/diff` page under the SHS root

At runtime:

1. User selects two app attempts from `/`
2. browser opens `/diff/?appId1=...&attemptId1=...&appId2=...&attemptId2=...`
3. `HistoryDiffPage` validates params
4. page loads both apps through `historyServer.withSparkUI(...)`
5. plugin computes compare payload
6. page renders summary + top diff sections

### Why plugin-side computation instead of extending `/api/v1/applications/diff`

For the first implementation, plugin-owned computation is the better boundary:

- no public API expansion is required
- no `api.scala` model churn is required
- the compare page can evolve without locking a public API surface
- the feature remains optional and modular

Later, if external consumers need the same data, we can promote the JSON schema to a public
`/api/v1/applications/diff` contract.

## Diff Data Model

The page needs five groups of data:

1. Compared app identity
2. App-level aggregate metrics
3. Top 3 diff jobs
4. Top 3 diff stages
5. Top 3 diff SQL executions

### App identity

For each side:

- `appId`
- `attemptId`
- `appName`
- `sparkUser`
- `startTime`
- `endTime`
- `durationMs`

### App-level aggregate metrics

These metrics should be derived from stage-level metrics so the page is aligned with the stored
execution data already used by SHS.

Initial metric set:

- `durationMs`
- `numStages`
- `numTasks`
- `numCompletedTasks`
- `numFailedTasks`
- `executorRunTime`
- `executorCpuTime` if available from stage task summaries, otherwise omit in v1
- `jvmGcTime`
- `inputBytes`
- `outputBytes`
- `shuffleReadBytes`
- `shuffleWriteBytes`
- `memoryBytesSpilled`
- `diskBytesSpilled`

For each metric, return:

- `app1`
- `app2`
- `diff = app2 - app1`
- optionally `ratio` when both sides are positive

### Job / stage / SQL diff rows

Each row should contain:

- `key`
- `name`
- `app1DurationMs`
- `app2DurationMs`
- `durationDiffMs`
- `absDurationDiffMs`
- `app1Metadata`
- `app2Metadata`
- `matchType`

The page keeps only the top 3 rows for each entity type after sorting by `absDurationDiffMs`
descending.

## Matching Strategy

Cross-app IDs are not stable enough to compare directly. The plugin must match logical entities.

### Jobs

Use:

- primary key: normalized job group description or terminal stage name set
- secondary key: ordinal occurrence among entities with the same normalized key

If no safe semantic key exists, fall back to:

- normalized display name
- then occurrence index by submission time

Unmatched jobs are still shown:

- missing side duration is treated as `0`
- this makes added / removed jobs visible in top diff results

### Stages

Use:

- primary key: normalized stage name
- secondary key: ordinal occurrence of that stage name by submission time

We should compare logical stage occurrences, not raw `(stageId, attemptId)` pairs across apps.

Within one app, use the latest attempt for a logical stage occurrence as the user-facing duration.
This avoids over-counting retries in the top diff stage list.

### SQL executions

Use `SQLAppStatusStore` when available.

Matching key:

- primary key: normalized SQL execution description
- secondary key: ordinal occurrence by submission time

If SQL data is absent for either app:

- render the SQL section as empty with a short `No SQL execution data` message
- do not fail the full diff page

## Aggregate Metric Semantics

This needs one explicit rule because stage retries can otherwise distort totals.

For v1:

- app-level aggregate metrics should be computed from the latest attempt of each logical stage
- top diff stages should also use that same logical-stage view
- retry / duplicate attempt cost is not surfaced separately in v1

Reasoning:

- this matches how users usually reason about final DAG shape and stage performance
- it avoids inflating totals for apps with retries
- it keeps app-level totals and stage-level top diff rows internally consistent

If we later need retry-cost analysis, add a separate section instead of changing these semantics.

## Page Layout

The `Diff` page should have three vertical sections.

### 1. Header

- left app summary
- right app summary
- compare key details
- swap action
- back-to-history action

### 2. App metrics table

A dense table with columns:

- metric
- app 1
- app 2
- diff
- ratio or delta%

Default sort:

- static metric order, not user-driven diff order

### 3. Top diff entity sections

Three tables:

- top 3 jobs
- top 3 stages
- top 3 SQL executions

Default sort:

- `absDurationDiffMs DESC`

Each row should link back to the underlying application UI when possible:

- jobs: `/history/<app>/<attempt>/jobs/job/?id=...`
- stages: `/history/<app>/<attempt>/stages/stage/?id=...&attempt=...`
- SQL: existing SQL tab / execution page route when the SQL plugin is present

If a row is unmatched on one side, only the existing-side link is shown.

## URL Contract

The page URL should be:

```text
/diff/?appId1=<id>&attemptId1=<id?>&appId2=<id>&attemptId2=<id?>
```

Rules:

- `appId1` and `appId2` are required
- `attemptId1` and `attemptId2` are optional
- order matters because `diff = app2 - app1`
- the UI should preserve user order from selection, but also expose a `Swap` action

## ACL / Security

The diff page must enforce existing SHS UI ACLs for both selected apps.

Validation flow:

1. resolve app 1 via `historyServer.withSparkUI(appId1, attemptId1)`
2. check UI view permission for current user
3. resolve app 2 via `historyServer.withSparkUI(appId2, attemptId2)`
4. check UI view permission for current user
5. only then compute diff

Failure behavior:

- if either app does not exist: `404`
- if either app is unauthorized: `403`
- if parameters are malformed: `400`

The page should not leak the existence of app metadata after authorization failure.

## Performance

The compare page loads two SHS app UIs at once, so we should avoid unnecessary scans.

Guidelines:

- compute only the metrics needed for page render
- do not materialize task-level records
- rely on `AppStatusStore.jobsList`, `stageList`, and `SQLAppStatusStore.executionsList`
- derive aggregates from stage summaries already stored in the KV store
- keep top-N selection streaming-friendly where practical

For v1, computing from already-materialized store summaries should be fast enough for a single
interactive compare request.

## Testing

### Core SHS tests

- `HistoryServerPageSuite`:
  - app list includes compare controls
  - compare action encodes two attempt selections correctly
- new unit tests for parameter encoding / parsing in history page JS if needed

### Plugin tests

- compare page returns `400` when fewer than two apps are specified
- compare page returns `404` for missing app / attempt
- compare page returns `403` when either app is unauthorized
- aggregate metrics are computed from stage summaries correctly
- top diff jobs are sorted by `abs(durationDiffMs)`
- top diff stages are sorted by `abs(durationDiffMs)`
- top diff SQL executions are sorted by `abs(durationDiffMs)`
- unmatched entities appear with zero on the missing side

### UI / selenium tests

Add one end-to-end SHS UI test:

1. open SHS app list
2. select two completed app attempts
3. click `Compare`
4. verify the `Diff` page opens
5. verify summary metrics and at least one diff section render

## Rollout Plan

### Step 1: Core plumbing

- extend plugin hook for history-server-level UI
- call plugin hook from `HistoryServer.initialize()`
- add compare selection controls to SHS app list page
- navigate to `/diff` with two selected attempts

### Step 2: Plugin skeleton

- add diff plugin class and service registration
- mount `Diff` tab and page
- validate parameters and ACLs
- render empty / loading / error states

### Step 3: Aggregate metrics

- compute app-level metrics from logical stage summaries
- render metrics table

### Step 4: Top diff entities

- add jobs diff
- add stages diff
- add SQL diff

### Step 5: polish

- add row links back to source UIs
- add swap action
- add better unmatched-row labels

## Open Questions

1. Should the `Diff` tab always be visible in SHS, or only after opening a compare URL?

Recommendation:
Always visible. When no parameters are present, render a simple empty state that instructs the
user to pick two apps from the history page.

2. Should app-level aggregates include retry cost from failed stage attempts?

Recommendation:
No in v1. Use latest logical stage attempt only, and add retry-cost analysis separately later.

3. Should the compare feature live in `core` or `sql/core`?

Recommendation:
Implement the plugin in `sql/core` so it can use `SQLAppStatusStore` directly, while still reading
core job / stage data from `AppStatusStore`. If SQL data is absent, degrade the SQL section only.

4. Should we reuse the existing `/api/v1/applications/diff` endpoint?

Recommendation:
Not for v1. Keep the compare implementation plugin-owned. If external consumers later need the
same data, align the plugin JSON model with a promoted public API.

## Recommended Implementation Direction

Keep SHS core changes intentionally small:

- one new history-server-level plugin hook
- one app-list compare selection flow

Put the actual diff feature in a plugin:

- root `Diff` tab
- compare page
- cross-app metric loading
- top diff jobs / stages / SQL logic

That split matches the request, keeps the feature modular, and avoids turning SHS core into the
owner of comparison-specific business logic.
