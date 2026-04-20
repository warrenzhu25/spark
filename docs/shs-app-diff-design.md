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

## Dependencies

- No new external dependencies required.
- Relies on existing `AppStatusStore` and `SQLAppStatusStore` APIs.
- JavaScript changes use vanilla JS with existing DataTables library.

## Current State

### WebUI Architecture

Spark's UI framework has three main abstractions:

```
WebUI (base class)
  ├── attachTab(tab)      // Attaches tab with all its pages
  ├── attachPage(page)    // Attaches page directly to root
  └── attachHandler(...)  // Low-level servlet handler

WebUITab(parent: WebUI, prefix: String)
  ├── attachPage(page)    // Page URL becomes /<tab-prefix>/<page-prefix>
  └── pages: ArrayBuffer[WebUIPage]

WebUIPage(prefix: String)
  ├── render(request): Seq[Node]      // Returns HTML
  └── renderJson(request): JValue     // Returns JSON
```

Key files:

- [WebUI.scala](core/src/main/scala/org/apache/spark/ui/WebUI.scala) - base UI framework
- [HistoryServer.scala](core/src/main/scala/org/apache/spark/deploy/history/HistoryServer.scala) -
  extends `WebUI`

`HistoryServer` extends `WebUI`, which means plugins can call `historyServer.attachTab()` to mount
root-level tabs without additional framework changes.

### App list page

The SHS home page is rendered by:

- [HistoryPage.scala](core/src/main/scala/org/apache/spark/deploy/history/HistoryPage.scala)
- [historypage.js](core/src/main/resources/org/apache/spark/ui/static/historypage.js)
- [historypage-template.html](core/src/main/resources/org/apache/spark/ui/static/historypage-template.html)

The table is fully client-rendered from `/api/v1/applications`.

### Plugin model

Current history plugins implement:

- [AppHistoryServerPlugin.scala](core/src/main/scala/org/apache/spark/status/AppHistoryServerPlugin.scala)

and are invoked from [FsHistoryProvider.scala](core/src/main/scala/org/apache/spark/deploy/history/FsHistoryProvider.scala)
only after a single application attempt has been loaded into a `SparkUI`:

```scala
// Current: per-app hook only
trait AppHistoryServerPlugin {
  def setupUI(ui: SparkUI): Unit
}
```

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

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                    SHS History Page (/)                          │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ [☐] App1  [☐] App2  [☐] App3  ...       [Compare Button]    │ │
│  └─────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
                              │
                              │ /diff/?appId1=...&appId2=...
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                      HistoryDiffPage                             │
│  ┌────────────────────┐  ┌────────────────────┐                  │
│  │ withSparkUI(app1)  │  │ withSparkUI(app2)  │                  │
│  │  ├─ AppStatusStore │  │  ├─ AppStatusStore │                  │
│  │  └─ SQLAppStatus   │  │  └─ SQLAppStatus   │                  │
│  └─────────┬──────────┘  └─────────┬──────────┘                  │
│            │                       │                             │
│            └───────────┬───────────┘                             │
│                        ▼                                         │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                 HistoryDiffStoreHelper                      │ │
│  │  • Compute aggregates from stage summaries                  │ │
│  │  • Match jobs/stages/SQL by normalized key + ordinal        │ │
│  │  • Sort by |durationDiff| descending                        │ │
│  │  • Return top N for each entity type                        │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                        │                                         │
│                        ▼                                         │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                   Rendered Page                             │ │
│  │  • Header (app1 vs app2 summary)                            │ │
│  │  • App-level metrics table                                  │ │
│  │  • Top 3 diff jobs                                          │ │
│  │  • Top 3 diff stages                                        │ │
│  │  • Top 3 diff SQL executions                                │ │
│  └─────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

## Proposal

### Phase 1: Minimal SHS Core Changes

#### 1. Add a history-server-level plugin hook

Extend `AppHistoryServerPlugin` with a new default no-op hook:

```scala
// core/src/main/scala/org/apache/spark/status/AppHistoryServerPlugin.scala
trait AppHistoryServerPlugin {
  // Existing: called per-app after loading into SparkUI
  def setupUI(ui: SparkUI): Unit

  // NEW: called once at HistoryServer startup
  def setupHistoryServerUI(historyServer: HistoryServer): Unit = {}
}
```

Then call it from `HistoryServer.initialize()`:

```scala
// core/src/main/scala/org/apache/spark/deploy/history/HistoryServer.scala
def initialize(): Unit = {
  attachPage(new HistoryPage(this))
  attachHandler(ApiRootResource.getServletHandler(this))
  addStaticHandler(SparkUI.STATIC_RESOURCE_DIR)

  // ... existing loader servlet setup ...

  // NEW: invoke history-server-level plugin hooks
  val plugins = ServiceLoader.load(
    classOf[AppHistoryServerPlugin],
    Utils.getContextOrSparkClassLoader)
  plugins.asScala.foreach { plugin =>
    try {
      plugin.setupHistoryServerUI(this)
    } catch {
      case e: Exception =>
        logWarning(s"Failed to initialize history server plugin: ${plugin.getClass.getName}", e)
    }
  }
}
```

Why this shape:

- It keeps one plugin discovery mechanism (no new `ServiceLoader` surface).
- Existing plugins remain unchanged (default no-op implementation).
- A diff plugin can mount a root-level `WebUITab` on `HistoryServer`.
- Per-app plugin setup in `FsHistoryProvider.getAppUI()` remains unchanged.

Alternative considered:

- Create a new `HistoryServerRootPlugin` trait

This is cleaner conceptually, but it adds another service-loading surface. For this feature, the
single-trait extension is simpler.

#### 2. Allow SHS root UI tabs

No major framework change is needed. `HistoryServer` already extends `WebUI`, so a plugin can
call `historyServer.attachTab()` directly to mount a root-level tab.

#### 3. Add two-app selection to the history app list page

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

#### 4. Keep the compare URL plugin-owned

SHS core should not compute diff data. Core only:

- collects two selected attempts
- navigates to the plugin page
- provides the plugin mount point

This keeps the feature modular and minimizes core ownership.

#### Summary of Core Changes

| File | Change |
|------|--------|
| `AppHistoryServerPlugin.scala` | Add `setupHistoryServerUI(HistoryServer)` with default no-op |
| `HistoryServer.scala` | Call `plugin.setupHistoryServerUI(this)` in `initialize()` |
| `historypage.js` | Add checkbox column, selection logic (max 2), Compare button handler |
| `historypage-template.html` | Add Compare button/toolbar element |

### Phase 2: SHS Diff Plugin

Implement a new plugin under `sql/core`, because it needs direct access to `SQLAppStatusStore` for
SQL execution diffing and can also read core `AppStatusStore` data.

#### Plugin Classes

```
sql/core/src/main/scala/org/apache/spark/sql/execution/ui/
├── ApplicationDiffHistoryServerPlugin.scala   # Plugin entry point
├── HistoryDiffTab.scala                       # Root-level tab
├── HistoryDiffPage.scala                      # Main diff page
├── HistoryDiffModel.scala                     # Data models for diff results
└── HistoryDiffStoreHelper.scala               # Cross-app metric computation
```

#### Plugin Implementation

```scala
// sql/core/.../ApplicationDiffHistoryServerPlugin.scala
package org.apache.spark.sql.execution.ui

import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.status.AppHistoryServerPlugin
import org.apache.spark.ui.SparkUI

class ApplicationDiffHistoryServerPlugin extends AppHistoryServerPlugin {

  // Existing per-app hook - unchanged for SQL tab setup
  override def setupUI(ui: SparkUI): Unit = {
    // ... existing SQL tab setup ...
  }

  // NEW: history-server-level hook for diff feature
  override def setupHistoryServerUI(historyServer: HistoryServer): Unit = {
    val diffTab = new HistoryDiffTab(historyServer)
    historyServer.attachTab(diffTab)
  }
}
```

```scala
// sql/core/.../HistoryDiffTab.scala
package org.apache.spark.sql.execution.ui

import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.ui.WebUITab

private[ui] class HistoryDiffTab(historyServer: HistoryServer)
  extends WebUITab(historyServer, "diff") {

  override val name = "Diff"

  attachPage(new HistoryDiffPage(this, historyServer))
}
```

```scala
// sql/core/.../HistoryDiffPage.scala
package org.apache.spark.sql.execution.ui

import javax.servlet.http.HttpServletRequest
import scala.xml.Node
import org.json4s.JsonAST.JValue

import org.apache.spark.deploy.history.HistoryServer
import org.apache.spark.ui.{UIUtils, WebUIPage}

private[ui] class HistoryDiffPage(parent: HistoryDiffTab, historyServer: HistoryServer)
  extends WebUIPage("") {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val appId1 = Option(request.getParameter("appId1"))
    val attemptId1 = Option(request.getParameter("attemptId1"))
    val appId2 = Option(request.getParameter("appId2"))
    val attemptId2 = Option(request.getParameter("attemptId2"))

    // Validate required parameters
    (appId1, appId2) match {
      case (Some(id1), Some(id2)) =>
        renderDiff(request, id1, attemptId1, id2, attemptId2)
      case _ =>
        renderEmptyState(request)
    }
  }

  private def renderDiff(
      request: HttpServletRequest,
      appId1: String,
      attemptId1: Option[String],
      appId2: String,
      attemptId2: Option[String]): Seq[Node] = {
    // Load both apps and compute diff
    val diffResult = historyServer.withSparkUI(appId1, attemptId1) { ui1 =>
      historyServer.withSparkUI(appId2, attemptId2) { ui2 =>
        HistoryDiffStoreHelper.computeDiff(ui1, ui2)
      }
    }
    // Render diff result...
  }

  private def renderEmptyState(request: HttpServletRequest): Seq[Node] = {
    val content =
      <div class="row">
        <div class="col-12">
          <h4>Compare Applications</h4>
          <p>Select two completed applications from the
            <a href="/">History</a> page to compare them.</p>
        </div>
      </div>
    UIUtils.headerSparkPage(request, "Diff", content, parent)
  }

  override def renderJson(request: HttpServletRequest): JValue = {
    // Return diff as JSON for programmatic access
  }
}
```

#### Service Registration

Register the plugin for `ServiceLoader` discovery:

```
# sql/core/src/main/resources/META-INF/services/org.apache.spark.status.AppHistoryServerPlugin
org.apache.spark.sql.execution.ui.ApplicationDiffHistoryServerPlugin
```

#### Plugin Lifecycle

At SHS startup:

1. `HistoryServer.initialize()` loads plugins via `ServiceLoader`
2. Diff plugin receives `setupHistoryServerUI(historyServer)`
3. Plugin creates `HistoryDiffTab` and attaches to `historyServer`
4. `/diff` page is now mounted at SHS root

At runtime:

1. User selects two app attempts from `/`
2. Browser opens `/diff/?appId1=...&attemptId1=...&appId2=...&attemptId2=...`
3. `HistoryDiffPage.render()` validates params
4. Page loads both apps through `historyServer.withSparkUI(...)`
5. `HistoryDiffStoreHelper` computes diff from both `AppStatusStore` instances
6. Page renders summary + top diff sections

#### URL Routing

| URL | Handler | Description |
|-----|---------|-------------|
| `/` | `HistoryPage` | App list with checkboxes |
| `/diff/` | `HistoryDiffPage` | Empty state (no params) |
| `/diff/?appId1=...&appId2=...` | `HistoryDiffPage` | Comparison view |
| `/history/<appId>/<attempt>/` | Per-app `SparkUI` | Application detail |
| `/history/<appId>/<attempt>/SQL/` | Per-app `SQLTab` | SQL executions |

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

- `app1` - value from first app
- `app2` - value from second app
- `diff = app2 - app1` - absolute difference
- `ratio = app2 / app1` when `app1 > 0`, otherwise `null` (displayed as "N/A")

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

### General Approach

For each entity type (jobs, stages, SQL):

1. **Normalize** the entity's semantic key (name/description)
2. **Group** entities by normalized key within each app
3. **Assign ordinal** based on submission time within each group
4. **Match** entities with the same `(normalizedKey, ordinalIndex)` pair

### Tie-breaking for Identical Keys

When multiple entities share the same normalized key within one app:

1. Sort by submission time ascending
2. Assign ordinal occurrence index (0, 1, 2, ...)
3. Match entities with the same `(normalizedKey, ordinalIndex)` pair across apps

Example:

```
App1 stages: ["Scan", "Scan", "Agg"]  → Scan#0, Scan#1, Agg#0
App2 stages: ["Scan", "Agg", "Scan"]  → Scan#0, Agg#0, Scan#1
Matches: (Scan#0 ↔ Scan#0), (Scan#1 ↔ Scan#1), (Agg#0 ↔ Agg#0)
```

### Jobs

Matching key:

- **Primary**: normalized job group description or terminal stage name set
- **Secondary**: ordinal occurrence among entities with the same normalized key

If no safe semantic key exists, fall back to:

- normalized display name
- then occurrence index by submission time

Unmatched jobs are still shown:

- missing side duration is treated as `0`
- this makes added / removed jobs visible in top diff results

### Stages

Matching key:

- **Primary**: normalized stage name
- **Secondary**: ordinal occurrence of that stage name by submission time

We compare logical stage occurrences, not raw `(stageId, attemptId)` pairs across apps.

Within one app, use the latest attempt for a logical stage occurrence as the user-facing duration.
This avoids over-counting retries in the top diff stage list.

### SQL Executions

Use `SQLAppStatusStore` when available.

Matching key:

- **Primary**: normalized SQL execution description
- **Secondary**: ordinal occurrence by submission time

SQL normalization rules:

1. Remove literal values: `SELECT * FROM t WHERE id = 123` → `SELECT * FROM t WHERE id = ?`
2. Collapse whitespace: multiple spaces → single space
3. Remove execution-specific prefixes (e.g., "Query 1:", "Execution:")

If SQL data is absent for either app:

- render the SQL section as empty with a short "No SQL execution data" message
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

## Accessibility

- Checkbox column uses `<input type="checkbox">` with `aria-label="Select {appName}"`
- Compare button disabled state uses `aria-disabled` attribute
- Diff table uses semantic `<th>` headers with `scope="col"`
- Color-coded diffs (green/red) also include `+`/`-` prefix for color-blind users
- Keyboard navigation: Tab through checkboxes, Enter/Space to toggle selection

## URL Contract

The page URL should be:

```text
/diff/?appId1=<id>&attemptId1=<id?>&appId2=<id>&attemptId2=<id?>
```

Example:

```text
/diff/?appId1=app-20240101120000-0001&attemptId1=1&appId2=app-20240101130000-0002&attemptId2=1
```

Rules:

- `appId1` and `appId2` are required
- `attemptId1` and `attemptId2` are optional (omit for apps without attempts)
- Order matters because `diff = app2 - app1`
- The UI should preserve user order from selection, but also expose a `Swap` action
- App IDs containing special characters must be URL-encoded

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

## Error States

| Scenario | HTTP Status | User Message |
|----------|-------------|--------------|
| Missing required params | 200 | Empty state: "Select two apps from History page" |
| App not found | 404 | "Application not found: {appId}" |
| Unauthorized | 403 | "Access denied" (no metadata leak) |
| App still running | 400 | "Cannot compare running applications" |
| Same app selected twice | 400 | "Please select two different applications" |
| Computation timeout | 504 | "Comparison timed out. Try selecting smaller applications." |
| App exceeds size limit | 413 | "Application too large to compare (>{maxStages} stages)" |

## Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `spark.history.ui.diff.enabled` | `true` | Enable/disable diff feature |
| `spark.history.ui.diff.topN` | `3` | Number of top diff entities per section |
| `spark.history.ui.diff.timeout.seconds` | `30` | Max computation time |
| `spark.history.ui.diff.maxStages` | `10000` | Reject apps exceeding this stage count |

When `spark.history.ui.diff.enabled=false`:

- The Compare checkbox column is hidden from the app list page
- Direct `/diff` URLs return 404
- Plugin still loads but the tab is not registered

## Performance

The compare page loads two SHS app UIs at once, so we should avoid unnecessary scans.

### Guidelines

- Compute only the metrics needed for page render
- Do not materialize task-level records
- Rely on `AppStatusStore.jobsList`, `stageList`, and `SQLAppStatusStore.executionsList`
- Derive aggregates from stage summaries already stored in the KV store
- Keep top-N selection streaming-friendly where practical

For v1, computing from already-materialized store summaries should be fast enough for a single
interactive compare request.

### Safeguards

- **Size limit**: If either app exceeds `spark.history.ui.diff.maxStages` (default 10,000),
  return `413 Payload Too Large` with a user-friendly message
- **Timeout**: Plugin computation respects `spark.history.ui.diff.timeout.seconds` (default 30s)
  to prevent long-running requests from blocking resources
- **Lazy loading**: Consider rendering app-level metrics first, then loading top-diff sections
  asynchronously in future versions

### Caching (Future)

For v1, no caching is implemented. Each request recomputes the diff.

Future consideration:

- Cache diff results keyed by `(appId1, attemptId1, appId2, attemptId2)`
- TTL: 5 minutes (completed apps are immutable)
- Invalidate on SHS restart

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

## Rollback Plan

If issues arise post-deployment:

1. **Disable without code change**: Set `spark.history.ui.diff.enabled=false`
   - The app list page hides Compare controls
   - Direct `/diff` URLs return 404
   - No SHS restart required (config change takes effect on next page load)

2. **Full rollback**: Revert the plugin JAR
   - Remove the diff plugin from classpath
   - SHS functions normally without the feature
   - No data migration required (feature is stateless)

## Open Questions

### Resolved

1. **Should the `Diff` tab always be visible in SHS, or only after opening a compare URL?**

   **Decision**: Always visible. When no parameters are present, render a simple empty state with
   instructions and a link to the History page.

2. **Should app-level aggregates include retry cost from failed stage attempts?**

   **Decision**: No in v1. Use latest logical stage attempt only. Add retry-cost analysis as a
   separate section later if needed.

3. **Should the compare feature live in `core` or `sql/core`?**

   **Decision**: Implement the plugin in `sql/core` so it can use `SQLAppStatusStore` directly,
   while still reading core job/stage data from `AppStatusStore`. If SQL data is absent, degrade
   the SQL section only.

4. **Should we reuse the existing `/api/v1/applications/diff` endpoint?**

   **Decision**: Not for v1. Keep the compare implementation plugin-owned. If external consumers
   later need the same data, promote the plugin JSON model to a public API.

### Open

5. **Should the top-N count be user-adjustable in the UI, or config-only?**

   Recommendation: Config-only (`spark.history.ui.diff.topN`) for v1. Add UI control later if
   users request it.

6. **Should unmatched entities show as "Added" / "Removed" labels?**

   Recommendation: Yes. Show "Only in App1" or "Only in App2" labels for clarity.

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
