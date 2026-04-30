# Spark Insight: Next-Generation Spark History Server

## Overview

Spark Insight is a **drop-in replacement** for the official Apache Spark History Server. It provides SHS-compatible REST APIs, a modern UI, AI-powered analysis, and MCP integration for LLM tools.

### Goals

1. **SHS Compatibility** - Drop-in replacement with same REST API endpoints
2. **Modern Experience** - Beautiful, responsive UI with interactive visualizations
3. **AI-Powered Analysis** - Natural language queries to understand failures, performance issues
4. **MCP Integration** - Expose Spark data to LLM tools (Claude, etc.)
5. **Unified Architecture** - Single Python service, CLI/UI/MCP all use REST API

### Design Principles

1. **Python Only** - No Rust, no multi-language complexity
2. **REST API First** - One service hosts all functionality
3. **SHS Compatible** - Existing tools work without modification
4. **Simple Deployment** - Single process, single container

### Deployment Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **Service** | REST API server with UI | Team/org-wide (replaces SHS) |
| **CLI** | Talks to service via REST API | Developer debugging |
| **MCP** | Connects to service REST API | AI-assisted debugging |

### Non-Goals (v1.0)

- Real-time streaming of running applications (focus on completed apps)
- Modify or write to Spark clusters
- Kerberos authentication (use SHS proxy mode instead)

### Production Readiness

| Workload | Supported | Notes |
|----------|-----------|-------|
| <100 apps/day | ✅ Yes | Direct replacement for SHS |
| 100-500 apps/day | ✅ Yes | Enable caching, background parsing |
| 500+ apps/day | ⚠️ Maybe | Test performance, consider SHS proxy mode |
| Event logs <1GB | ✅ Yes | ~15s parse time |
| Event logs 1-5GB | ✅ Yes | ~75s parse time, use background parsing |
| Event logs >5GB | ⚠️ Maybe | Consider sampling or SHS proxy mode |

---

## Architecture

### Unified Service Architecture

All clients (CLI, UI, MCP) communicate with a single REST API service:

```
┌────────────────────────────────────────────────────────────────────────────┐
│                              CLIENTS                                        │
│  ┌─────────────┐  ┌─────────────────┐  ┌─────────────┐  ┌───────────────┐  │
│  │  CLI        │  │  Web UI         │  │  MCP Server │  │  Spark Tools  │  │
│  │  (Python)   │  │  (Streamlit)    │  │  (Python)   │  │  (existing)   │  │
│  └──────┬──────┘  └────────┬────────┘  └──────┬──────┘  └───────┬───────┘  │
│         │                  │                  │                  │          │
│         └──────────────────┼──────────────────┼──────────────────┘          │
│                            │                  │                             │
│                            ▼                  ▼                             │
├────────────────────────────────────────────────────────────────────────────┤
│                    REST API (FastAPI) - SHS Compatible                      │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │  /api/v1/applications                    (SHS compatible)            │  │
│  │  /api/v1/applications/{appId}/jobs       (SHS compatible)            │  │
│  │  /api/v1/applications/{appId}/stages     (SHS compatible)            │  │
│  │  /api/v1/applications/{appId}/executors  (SHS compatible)            │  │
│  ├──────────────────────────────────────────────────────────────────────┤  │
│  │  /api/insight/analyze                    (AI analysis)               │  │
│  │  /api/insight/diff                       (app comparison)            │  │
│  │  /api/insight/upload                     (event log upload)          │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
├────────────────────────────────────────────────────────────────────────────┤
│                           Core Services                                     │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │  Event Log      │  │  Query Engine   │  │  LLM Service    │             │
│  │  Parser         │  │  (DuckDB)       │  │  (Claude API)   │             │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘             │
├────────────────────────────────────────────────────────────────────────────┤
│                           Storage Layer                                     │
│  ┌────────────────────────────────────┐  ┌─────────────────────────────┐   │
│  │  Event Log Storage                 │  │  Parsed Data Cache          │   │
│  │  (Local / S3 / HDFS)               │  │  (DuckDB per app)           │   │
│  └────────────────────────────────────┘  └─────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────┘
```

### Client Architecture

```
CLI                          UI                           MCP
 │                            │                            │
 │  spark-insight apps        │  Browser → Streamlit       │  Claude Desktop
 │  spark-insight jobs app1   │  → Renders REST data       │  → Calls MCP tools
 │  spark-insight ask "..."   │  → AI chat interface       │  → query_spark_data
 │                            │                            │  → analyze_failures
 └───────────────┬────────────┴─────────────┬──────────────┘
                 │                          │
                 ▼                          ▼
         REST API (http://localhost:18080/api/v1/...)
```

---

## Technology Stack

| Layer | Technology | Rationale |
|-------|------------|-----------|
| **API Server** | FastAPI | Async, auto-docs, SHS-compatible routes |
| **Parser** | Polars | Rust-based, 3-4x faster than pure Python |
| **Query Engine** | DuckDB | Analytical queries, zero-config, fast |
| **Frontend** | Streamlit | Python-native, rapid iteration |
| **Charts** | Plotly | Interactive, built-in Streamlit support |
| **Storage** | Local / S3 / HDFS | Event log storage |
| **LLM** | Claude API / Ollama | Cloud or local LLM options |
| **MCP** | mcp-python SDK | Official MCP implementation |

### Why Python Only?

**Complexity vs Performance Trade-off:**

| Approach | Parsing 1GB | Dev Complexity | Maintenance |
|----------|-------------|----------------|-------------|
| Rust + Python | ~5s | High (2 languages, PyO3, maturin) | Hard |
| Python (pure) | ~60s | Low (1 language) | Easy |
| **Python + Polars** | **~15s** | **Low (1 language)** | **Easy** |

**Polars** is a Rust-based DataFrame library with Python bindings. We get Rust performance with Python simplicity:
- Single language (Python) for development
- Rust-speed parsing under the hood
- No build complexity (just `pip install polars`)
- Well-maintained, production-ready library

### Performance with Polars + Parquet Cache

| Event Log Size | Cold Parse | Cache Write | Cached Load | SQL Query |
|----------------|------------|-------------|-------------|-----------|
| 100 MB | <2s | <0.5s | <0.2s | <50ms |
| 1 GB | ~15s | ~2s | <1s | <100ms |
| 5 GB | ~75s | ~10s | ~3s | <100ms |
| 10 GB | ~2.5 min | ~20s | ~5s | <100ms |

**Key optimizations:**
1. **Polars NDJSON reader** - Rust-based, uses SIMD instructions
2. **Parquet cache** - Parse once, load instantly forever (~6x compression)
3. **DuckDB queries** - Zero-copy from Parquet via Arrow
4. **Background parsing** - Parse on upload, not on request

### Why Streamlit for UI?

| Aspect | Streamlit | React/Next.js |
|--------|-----------|---------------|
| **Learning curve** | Hours (Python only) | Weeks (JS, React, CSS) |
| **Development speed** | Very fast | Slower |
| **Code complexity** | ~500 lines | ~5000+ lines |
| **Maintenance** | Easy | Requires frontend expertise |
| **Deployment** | Simple | More complex |

For a data analysis tool, Streamlit provides 90% of the functionality with 10% of the effort.

---

## Core Features

### 0. SHS-Compatible REST API

The core of Spark Insight is a FastAPI server that implements the same REST API as Apache Spark History Server. This means **existing tools and integrations work without modification**.

**SHS API Compatibility:**

| Endpoint | SHS | Spark Insight | Notes |
|----------|-----|---------------|-------|
| `GET /api/v1/applications` | ✅ | ✅ | List all applications |
| `GET /api/v1/applications/{appId}` | ✅ | ✅ | Application details |
| `GET /api/v1/applications/{appId}/jobs` | ✅ | ✅ | List jobs |
| `GET /api/v1/applications/{appId}/jobs/{jobId}` | ✅ | ✅ | Job details |
| `GET /api/v1/applications/{appId}/stages` | ✅ | ✅ | List stages |
| `GET /api/v1/applications/{appId}/stages/{stageId}` | ✅ | ✅ | Stage details |
| `GET /api/v1/applications/{appId}/executors` | ✅ | ✅ | List executors |
| `GET /api/v1/applications/{appId}/environment` | ✅ | ✅ | Spark config |
| **Extended API** | | | |
| `POST /api/insight/analyze` | ❌ | ✅ | AI analysis |
| `POST /api/insight/diff` | ❌ | ✅ | Compare apps |
| `POST /api/insight/upload` | ❌ | ✅ | Upload event log |

**FastAPI Server Implementation:**

```python
from fastapi import FastAPI, HTTPException, UploadFile, File
from pydantic import BaseModel
from typing import List, Optional
import duckdb

app = FastAPI(title="Spark Insight", version="1.0.0")

# ============= SHS-Compatible Endpoints =============

@app.get("/api/v1/applications")
async def list_applications(
    status: Optional[str] = None,
    minDate: Optional[str] = None,
    maxDate: Optional[str] = None,
    limit: int = 100
) -> List[ApplicationInfo]:
    """List all applications (SHS compatible)."""
    return app_service.list_applications(status, minDate, maxDate, limit)


@app.get("/api/v1/applications/{app_id}")
async def get_application(app_id: str) -> ApplicationInfo:
    """Get application details (SHS compatible)."""
    app = app_service.get_application(app_id)
    if not app:
        raise HTTPException(404, f"Application {app_id} not found")
    return app


@app.get("/api/v1/applications/{app_id}/jobs")
async def list_jobs(
    app_id: str,
    status: Optional[str] = None
) -> List[JobData]:
    """List jobs for an application (SHS compatible)."""
    return app_service.get_jobs(app_id, status)


@app.get("/api/v1/applications/{app_id}/jobs/{job_id}")
async def get_job(app_id: str, job_id: int) -> JobData:
    """Get job details (SHS compatible)."""
    return app_service.get_job(app_id, job_id)


@app.get("/api/v1/applications/{app_id}/stages")
async def list_stages(
    app_id: str,
    status: Optional[str] = None
) -> List[StageData]:
    """List stages for an application (SHS compatible)."""
    return app_service.get_stages(app_id, status)


@app.get("/api/v1/applications/{app_id}/stages/{stage_id}/{attempt_id}")
async def get_stage(
    app_id: str,
    stage_id: int,
    attempt_id: int
) -> StageData:
    """Get stage details (SHS compatible)."""
    return app_service.get_stage(app_id, stage_id, attempt_id)


@app.get("/api/v1/applications/{app_id}/stages/{stage_id}/{attempt_id}/taskList")
async def get_task_list(
    app_id: str,
    stage_id: int,
    attempt_id: int,
    offset: int = 0,
    length: int = 100,
    sortBy: str = "taskId"
) -> List[TaskData]:
    """Get tasks for a stage (SHS compatible)."""
    return app_service.get_tasks(app_id, stage_id, attempt_id, offset, length, sortBy)


@app.get("/api/v1/applications/{app_id}/executors")
async def list_executors(app_id: str) -> List[ExecutorSummary]:
    """List executors for an application (SHS compatible)."""
    return app_service.get_executors(app_id)


@app.get("/api/v1/applications/{app_id}/environment")
async def get_environment(app_id: str) -> EnvironmentInfo:
    """Get Spark configuration (SHS compatible)."""
    return app_service.get_environment(app_id)


# ============= Extended API (Spark Insight specific) =============

@app.post("/api/insight/analyze")
async def analyze_application(
    app_id: str,
    question: str
) -> AnalysisResult:
    """AI-powered analysis of application."""
    return await llm_service.analyze(app_id, question)


@app.post("/api/insight/diff")
async def diff_applications(
    app_id_1: str,
    app_id_2: str
) -> DiffResult:
    """Compare two applications."""
    return diff_service.compare(app_id_1, app_id_2)


@app.post("/api/insight/upload")
async def upload_eventlog(
    file: UploadFile = File(...)
) -> ApplicationInfo:
    """Upload and parse an event log."""
    return await upload_service.upload(file)


# ============= Health & Info =============

@app.get("/api/v1/version")
async def get_version():
    """Get Spark Insight version."""
    return {"spark-insight": "1.0.0"}
```

### 1. Event Log Parsing (Python)

Streaming parser for Spark event logs using pure Python with optimizations.

**Data Models (Pydantic):**

```python
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from enum import Enum

class JobStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"

class StageStatus(str, Enum):
    ACTIVE = "ACTIVE"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"
    PENDING = "PENDING"
    SKIPPED = "SKIPPED"

class ApplicationInfo(BaseModel):
    """SHS-compatible application info."""
    id: str
    name: str
    attempts: List["ApplicationAttemptInfo"]

class ApplicationAttemptInfo(BaseModel):
    attemptId: Optional[str] = None
    startTime: str
    endTime: str
    lastUpdated: str
    duration: int
    sparkUser: str
    completed: bool
    appSparkVersion: str

class JobData(BaseModel):
    """SHS-compatible job data."""
    jobId: int
    name: str
    submissionTime: Optional[str]
    completionTime: Optional[str]
    stageIds: List[int]
    status: str
    numTasks: int
    numActiveTasks: int
    numCompletedTasks: int
    numSkippedTasks: int
    numFailedTasks: int
    numKilledTasks: int
    numActiveStages: int
    numCompletedStages: int
    numSkippedStages: int
    numFailedStages: int

class StageData(BaseModel):
    """SHS-compatible stage data."""
    status: str
    stageId: int
    attemptId: int
    numTasks: int
    numActiveTasks: int
    numCompleteTasks: int
    numFailedTasks: int
    numKilledTasks: int
    executorRunTime: int
    executorCpuTime: int
    inputBytes: int
    inputRecords: int
    outputBytes: int
    outputRecords: int
    shuffleReadBytes: int
    shuffleReadRecords: int
    shuffleWriteBytes: int
    shuffleWriteRecords: int
    memoryBytesSpilled: int
    diskBytesSpilled: int
    name: str
    details: str
    schedulingPool: str

class ExecutorSummary(BaseModel):
    """SHS-compatible executor summary."""
    id: str
    hostPort: str
    isActive: bool
    rddBlocks: int
    memoryUsed: int
    diskUsed: int
    totalCores: int
    maxTasks: int
    activeTasks: int
    failedTasks: int
    completedTasks: int
    totalTasks: int
    totalDuration: int
    totalGCTime: int
    totalInputBytes: int
    totalShuffleRead: int
    totalShuffleWrite: int
    maxMemory: int
    addTime: str
    removeTime: Optional[str] = None
    removeReason: Optional[str] = None
```

**Polars-Based Parser (Rust speed, Python simplicity):**

```python
import polars as pl
import gzip
import tempfile
from pathlib import Path
from typing import Optional
import duckdb


class EventLogParser:
    """
    Fast event log parser using Polars (Rust-based).

    Performance: ~15s for 1GB event log (vs ~60s pure Python, ~5s pure Rust)
    """

    def parse(self, path: str) -> "ParsedApplication":
        """Parse event log file using Polars."""
        # Decompress if needed
        path = self._ensure_decompressed(path)

        # Read all events with Polars (Rust-fast NDJSON reader)
        events = pl.read_ndjson(path)

        # Extract data by event type (all done in Rust/Polars)
        app_info = self._extract_app_info(events)
        jobs = self._extract_jobs(events)
        stages = self._extract_stages(events)
        tasks = self._extract_tasks(events)
        executors = self._extract_executors(events)
        environment = self._extract_environment(events)

        return ParsedApplication(
            app_info=app_info,
            jobs=jobs,
            stages=stages,
            tasks=tasks,
            executors=executors,
            environment=environment,
        )

    def parse_to_duckdb(self, path: str, db_path: str) -> duckdb.DuckDBPyConnection:
        """Parse event log directly into DuckDB for fast queries."""
        path = self._ensure_decompressed(path)
        events = pl.read_ndjson(path)

        conn = duckdb.connect(db_path)

        # Register Polars DataFrames directly with DuckDB (zero-copy)
        conn.register("jobs", self._extract_jobs_df(events))
        conn.register("stages", self._extract_stages_df(events))
        conn.register("tasks", self._extract_tasks_df(events))
        conn.register("executors", self._extract_executors_df(events))

        # Create persistent tables
        conn.execute("CREATE TABLE jobs AS SELECT * FROM jobs")
        conn.execute("CREATE TABLE stages AS SELECT * FROM stages")
        conn.execute("CREATE TABLE tasks AS SELECT * FROM tasks")
        conn.execute("CREATE TABLE executors AS SELECT * FROM executors")

        return conn

    def _ensure_decompressed(self, path: str) -> str:
        """Decompress gzip/lz4 if needed, return path to plain JSON."""
        p = Path(path)
        if p.suffix == '.gz':
            # Decompress to temp file
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.json')
            with gzip.open(path, 'rb') as f_in:
                tmp.write(f_in.read())
            tmp.close()
            return tmp.name
        return path

    def _extract_jobs_df(self, events: pl.DataFrame) -> pl.DataFrame:
        """Extract jobs from events as Polars DataFrame."""
        # Filter to job events
        job_starts = events.filter(
            pl.col("Event") == "SparkListenerJobStart"
        ).select([
            pl.col("Job ID").alias("jobId"),
            pl.col("Submission Time").alias("submissionTime"),
            pl.col("Stage IDs").alias("stageIds"),
        ])

        job_ends = events.filter(
            pl.col("Event") == "SparkListenerJobEnd"
        ).select([
            pl.col("Job ID").alias("jobId"),
            pl.col("Completion Time").alias("completionTime"),
            pl.col("Job Result").struct.field("Result").alias("status"),
        ])

        # Join start and end events
        return job_starts.join(job_ends, on="jobId", how="left")

    def _extract_tasks_df(self, events: pl.DataFrame) -> pl.DataFrame:
        """Extract tasks from events as Polars DataFrame."""
        return events.filter(
            pl.col("Event") == "SparkListenerTaskEnd"
        ).select([
            pl.col("Task Info").struct.field("Task ID").alias("taskId"),
            pl.col("Stage ID").alias("stageId"),
            pl.col("Stage Attempt ID").alias("attemptId"),
            pl.col("Task Info").struct.field("Executor ID").alias("executorId"),
            pl.col("Task Info").struct.field("Host").alias("host"),
            pl.col("Task Info").struct.field("Launch Time").alias("launchTime"),
            pl.col("Task Info").struct.field("Finish Time").alias("finishTime"),
            (pl.col("Task Info").struct.field("Finish Time") -
             pl.col("Task Info").struct.field("Launch Time")).alias("duration"),
            pl.col("Task Info").struct.field("Failed").alias("failed"),
            pl.col("Task Metrics").struct.field("JVM GC Time").alias("gcTime"),
            pl.col("Task Metrics").struct.field("Peak Execution Memory").alias("peakMemory"),
        ])

    def _extract_stages_df(self, events: pl.DataFrame) -> pl.DataFrame:
        """Extract stages from events as Polars DataFrame."""
        return events.filter(
            pl.col("Event") == "SparkListenerStageCompleted"
        ).select([
            pl.col("Stage Info").struct.field("Stage ID").alias("stageId"),
            pl.col("Stage Info").struct.field("Stage Attempt ID").alias("attemptId"),
            pl.col("Stage Info").struct.field("Stage Name").alias("name"),
            pl.col("Stage Info").struct.field("Number of Tasks").alias("numTasks"),
            pl.col("Stage Info").struct.field("Failure Reason").alias("failureReason"),
        ]).with_columns([
            pl.when(pl.col("failureReason").is_null())
              .then(pl.lit("COMPLETE"))
              .otherwise(pl.lit("FAILED"))
              .alias("status")
        ])

    def _extract_executors_df(self, events: pl.DataFrame) -> pl.DataFrame:
        """Extract executors from events as Polars DataFrame."""
        added = events.filter(
            pl.col("Event") == "SparkListenerExecutorAdded"
        ).select([
            pl.col("Executor ID").alias("id"),
            pl.col("Executor Info").struct.field("Host").alias("hostPort"),
            pl.col("Executor Info").struct.field("Total Cores").alias("totalCores"),
            pl.col("Timestamp").alias("addTime"),
        ])

        removed = events.filter(
            pl.col("Event") == "SparkListenerExecutorRemoved"
        ).select([
            pl.col("Executor ID").alias("id"),
            pl.col("Timestamp").alias("removeTime"),
            pl.col("Removed Reason").alias("removeReason"),
        ])

        return added.join(removed, on="id", how="left").with_columns([
            pl.col("removeTime").is_null().alias("isActive")
        ])

    def _extract_app_info(self, events: pl.DataFrame) -> dict:
        """Extract application info."""
        start = events.filter(
            pl.col("Event") == "SparkListenerApplicationStart"
        ).to_dicts()

        end = events.filter(
            pl.col("Event") == "SparkListenerApplicationEnd"
        ).to_dicts()

        start_event = start[0] if start else {}
        end_event = end[0] if end else {}

        return {
            "id": start_event.get("App ID", ""),
            "name": start_event.get("App Name", ""),
            "startTime": start_event.get("Timestamp", 0),
            "endTime": end_event.get("Timestamp", 0),
            "sparkUser": start_event.get("User", ""),
            "appSparkVersion": start_event.get("Spark Version", ""),
            "completed": bool(end_event),
        }

    def _extract_environment(self, events: pl.DataFrame) -> dict:
        """Extract Spark configuration."""
        env = events.filter(
            pl.col("Event") == "SparkListenerEnvironmentUpdate"
        ).to_dicts()

        if env:
            return env[0].get("Spark Properties", {})
        return {}
```

**Why Polars is fast:**
- Written in Rust, uses Apache Arrow memory format
- SIMD-accelerated JSON parsing
- Zero-copy integration with DuckDB
- Lazy evaluation - only computes what's needed

**Event Types Parsed:**
- `SparkListenerApplicationStart/End`
- `SparkListenerJobStart/End`
- `SparkListenerStageSubmitted/Completed`
- `SparkListenerTaskEnd`
- `SparkListenerExecutorAdded/Removed`
- `SparkListenerEnvironmentUpdate`

### 2. Caching & Query Engine

**Hybrid approach:** Parquet for storage, DuckDB for queries. Zero-copy via Apache Arrow.

```
Event Log (.json.gz)
        │
        ▼ parse (Polars)
   Polars DataFrames
        │
        ▼ cache (Parquet)
   ~/.spark-insight/cache/{app_id}/
        ├── jobs.parquet
        ├── stages.parquet
        ├── tasks.parquet
        └── executors.parquet
        │
        ▼ query (DuckDB)
   DuckDB (in-memory, reads Parquet directly)
        │
        ▼
   Query Results (Polars DataFrame)
```

**Cache Manager:**

```python
from pathlib import Path
from hashlib import sha256
import polars as pl
import duckdb


class CacheManager:
    """
    Parquet-based cache with DuckDB query engine.

    - Parquet: Fast storage, compressed, Polars-native
    - DuckDB: Powerful SQL queries, zero-copy from Parquet
    """

    def __init__(self, cache_dir: str = "~/.spark-insight/cache"):
        self.cache_dir = Path(cache_dir).expanduser()
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get_cache_path(self, eventlog_path: str) -> Path:
        """Get cache directory for an event log."""
        # Hash the path + mtime for cache key
        p = Path(eventlog_path)
        mtime = p.stat().st_mtime if p.exists() else 0
        key = sha256(f"{eventlog_path}:{mtime}".encode()).hexdigest()[:16]
        return self.cache_dir / key

    def is_cached(self, eventlog_path: str) -> bool:
        """Check if event log is already parsed and cached."""
        cache_path = self.get_cache_path(eventlog_path)
        return (cache_path / "jobs.parquet").exists()

    def cache_parsed(self, eventlog_path: str, parsed: "ParsedApplication"):
        """Save parsed data to Parquet cache."""
        cache_path = self.get_cache_path(eventlog_path)
        cache_path.mkdir(parents=True, exist_ok=True)

        # Write Polars DataFrames to Parquet (fast, compressed)
        parsed.jobs.write_parquet(cache_path / "jobs.parquet")
        parsed.stages.write_parquet(cache_path / "stages.parquet")
        parsed.tasks.write_parquet(cache_path / "tasks.parquet")
        parsed.executors.write_parquet(cache_path / "executors.parquet")

        # Save app info as JSON (small)
        import json
        with open(cache_path / "app_info.json", "w") as f:
            json.dump(parsed.app_info, f)

    def load_cached(self, eventlog_path: str) -> "ParsedApplication":
        """Load parsed data from Parquet cache."""
        cache_path = self.get_cache_path(eventlog_path)

        import json
        with open(cache_path / "app_info.json") as f:
            app_info = json.load(f)

        return ParsedApplication(
            app_info=app_info,
            jobs=pl.read_parquet(cache_path / "jobs.parquet"),
            stages=pl.read_parquet(cache_path / "stages.parquet"),
            tasks=pl.read_parquet(cache_path / "tasks.parquet"),
            executors=pl.read_parquet(cache_path / "executors.parquet"),
        )

    def get_or_parse(self, eventlog_path: str) -> "ParsedApplication":
        """Get from cache or parse and cache."""
        if self.is_cached(eventlog_path):
            return self.load_cached(eventlog_path)

        # Parse (slow, but only once)
        parser = EventLogParser()
        parsed = parser.parse(eventlog_path)

        # Cache for next time
        self.cache_parsed(eventlog_path, parsed)

        return parsed

    def cleanup(self, max_age_days: int = 30):
        """Remove old cache entries."""
        import time
        cutoff = time.time() - (max_age_days * 86400)

        for cache_path in self.cache_dir.iterdir():
            if cache_path.is_dir():
                mtime = cache_path.stat().st_mtime
                if mtime < cutoff:
                    import shutil
                    shutil.rmtree(cache_path)
```

**Query Engine (DuckDB + Parquet):**

```python
class QueryEngine:
    """
    SQL query engine using DuckDB.

    Reads directly from Parquet cache (zero-copy via Arrow).
    """

    def __init__(self, cache_path: Path):
        self.cache_path = cache_path
        self.conn = duckdb.connect()  # In-memory
        self._load_tables()

    def _load_tables(self):
        """Load Parquet files as DuckDB tables (zero-copy)."""
        tables = ["jobs", "stages", "tasks", "executors"]
        for table in tables:
            parquet_path = self.cache_path / f"{table}.parquet"
            if parquet_path.exists():
                self.conn.execute(f"""
                    CREATE OR REPLACE VIEW {table} AS
                    SELECT * FROM read_parquet('{parquet_path}')
                """)

    def query(self, sql: str) -> pl.DataFrame:
        """Execute SQL query, return Polars DataFrame."""
        return self.conn.execute(sql).pl()

    def query_pandas(self, sql: str):
        """Execute SQL query, return Pandas DataFrame."""
        return self.conn.execute(sql).fetchdf()

    # ============= Pre-built Analytical Queries =============

    def get_failed_tasks_summary(self) -> pl.DataFrame:
        return self.query("""
            SELECT
                COALESCE(errorMessage, 'Unknown') as error,
                COUNT(*) as count,
                AVG(duration) as avg_duration_ms,
                MAX(duration) as max_duration_ms
            FROM tasks
            WHERE failed = true
            GROUP BY errorMessage
            ORDER BY count DESC
            LIMIT 20
        """)

    def get_executor_stats(self) -> pl.DataFrame:
        return self.query("""
            SELECT
                e.id,
                e.hostPort,
                e.totalCores,
                e.isActive,
                e.removeReason,
                COUNT(t.taskId) as totalTasks,
                SUM(CASE WHEN t.failed THEN 1 ELSE 0 END) as failedTasks,
                SUM(t.duration) as totalDuration,
                SUM(t.gcTime) as totalGcTime,
                ROUND(SUM(t.gcTime) * 100.0 / NULLIF(SUM(t.duration), 0), 2) as gcPercent
            FROM executors e
            LEFT JOIN tasks t ON e.id = t.executorId
            GROUP BY e.id, e.hostPort, e.totalCores, e.isActive, e.removeReason
            ORDER BY totalTasks DESC
        """)

    def get_stage_skew_analysis(self) -> pl.DataFrame:
        return self.query("""
            SELECT
                stageId,
                attemptId,
                COUNT(*) as numTasks,
                MIN(duration) as minDuration,
                MAX(duration) as maxDuration,
                AVG(duration) as avgDuration,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration) as medianDuration,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration) as p99Duration,
                ROUND(MAX(duration) / NULLIF(AVG(duration), 0), 2) as skewRatio
            FROM tasks
            GROUP BY stageId, attemptId
            HAVING COUNT(*) > 1
            ORDER BY skewRatio DESC
        """)

    def get_shuffle_stats(self) -> pl.DataFrame:
        return self.query("""
            SELECT
                stageId,
                name,
                SUM(shuffleReadBytes) as totalShuffleRead,
                SUM(shuffleWriteBytes) as totalShuffleWrite,
                SUM(inputBytes) as totalInput,
                SUM(outputBytes) as totalOutput
            FROM stages
            GROUP BY stageId, name
            ORDER BY totalShuffleRead + totalShuffleWrite DESC
        """)

    def get_timeline(self) -> pl.DataFrame:
        """Get task timeline for visualization."""
        return self.query("""
            SELECT
                taskId,
                stageId,
                executorId,
                launchTime,
                finishTime,
                duration,
                failed
            FROM tasks
            ORDER BY launchTime
        """)
```

**Performance comparison:**

| Operation | First Access | Cached Access |
|-----------|--------------|---------------|
| Parse 1GB log | ~15s | - |
| Write to Parquet | ~2s | - |
| Load from Parquet | - | <1s |
| Complex SQL query | - | <100ms |

**Cache storage:**

| Log Size | Parquet Cache Size | Compression |
|----------|-------------------|-------------|
| 100 MB | ~15 MB | 6-7x |
| 1 GB | ~150 MB | 6-7x |
| 5 GB | ~750 MB | 6-7x |

### 3. LLM Service

Natural language interface for querying Spark application data.

```python
class LLMService:
    def __init__(self, query_engine: QueryEngine, model: str = "claude-sonnet-4-20250514"):
        self.query_engine = query_engine
        self.client = anthropic.Anthropic()
        self.model = model

    async def analyze(self, question: str) -> AnalysisResult:
        """Answer natural language questions about Spark application."""

        # Build context from application data
        context = self._build_context()

        # System prompt with Spark expertise
        system_prompt = """You are a Spark performance expert. Analyze the provided
        Spark application data and answer the user's question.

        Focus on:
        - Identifying root causes of failures
        - Performance bottlenecks (shuffle, GC, skew)
        - Resource utilization issues
        - Actionable recommendations

        Be specific and reference actual data (job IDs, stage IDs, executor IDs)."""

        response = await self.client.messages.create(
            model=self.model,
            system=system_prompt,
            messages=[
                {"role": "user", "content": f"Application Data:\n{context}\n\nQuestion: {question}"}
            ],
            max_tokens=2000
        )

        return AnalysisResult(
            answer=response.content[0].text,
            relevant_data=self._extract_relevant_data(question)
        )

    def _build_context(self) -> str:
        """Build context string from application data."""
        sections = []

        # Application summary
        summary = self.query_engine.get_application_summary()
        sections.append(f"## Application Summary\n{summary.to_markdown()}")

        # Failed tasks
        failed = self.query_engine.get_failed_tasks_summary()
        if not failed.empty:
            sections.append(f"## Failed Tasks\n{failed.to_markdown()}")

        # Executor removals
        removals = self.query_engine.get_executor_removal_reasons()
        if not removals.empty:
            sections.append(f"## Executor Removals\n{removals.to_markdown()}")

        # Stage metrics
        stages = self.query_engine.get_stage_summary()
        sections.append(f"## Stage Summary\n{stages.to_markdown()}")

        return "\n\n".join(sections)
```

**Example Queries:**
- "What caused the most task failures?"
- "Why were executors removed from this application?"
- "Which stages have the worst data skew?"
- "What's causing the long GC pauses?"
- "Compare shuffle performance between stage 3 and stage 7"
- "What configuration changes would improve this job?"

### 4. MCP Server (REST API Client)

The MCP server connects to the Spark Insight REST API, exposing Spark data to LLM tools like Claude.

```python
from mcp import Server, Resource, Tool
import httpx

class SparkInsightMCPServer:
    """MCP server that proxies requests to Spark Insight REST API."""

    def __init__(self, server_url: str = "http://localhost:18080"):
        self.server = Server("spark-insight")
        self.api = httpx.AsyncClient(base_url=server_url)
        self._register_resources()
        self._register_tools()

    def _register_resources(self):
        """Register MCP resources for Spark data."""

        @self.server.resource("spark://applications")
        async def list_applications() -> Resource:
            """List all Spark applications."""
            resp = await self.api.get("/api/v1/applications")
            return Resource(
                uri="spark://applications",
                name="Applications",
                mimeType="application/json",
                content=resp.text
            )

        @self.server.resource("spark://applications/{app_id}")
        async def get_application(app_id: str) -> Resource:
            """Get application details."""
            resp = await self.api.get(f"/api/v1/applications/{app_id}")
            return Resource(
                uri=f"spark://applications/{app_id}",
                name=f"Application {app_id}",
                mimeType="application/json",
                content=resp.text
            )

        @self.server.resource("spark://applications/{app_id}/jobs")
        async def get_jobs(app_id: str) -> Resource:
            """Get jobs for an application."""
            resp = await self.api.get(f"/api/v1/applications/{app_id}/jobs")
            return Resource(
                uri=f"spark://applications/{app_id}/jobs",
                name=f"Jobs for {app_id}",
                mimeType="application/json",
                content=resp.text
            )

        @self.server.resource("spark://applications/{app_id}/stages")
        async def get_stages(app_id: str) -> Resource:
            """Get stages for an application."""
            resp = await self.api.get(f"/api/v1/applications/{app_id}/stages")
            return Resource(
                uri=f"spark://applications/{app_id}/stages",
                name=f"Stages for {app_id}",
                mimeType="application/json",
                content=resp.text
            )

        @self.server.resource("spark://applications/{app_id}/executors")
        async def get_executors(app_id: str) -> Resource:
            """Get executors for an application."""
            resp = await self.api.get(f"/api/v1/applications/{app_id}/executors")
            return Resource(
                uri=f"spark://applications/{app_id}/executors",
                name=f"Executors for {app_id}",
                mimeType="application/json",
                content=resp.text
            )

    def _register_tools(self):
        """Register MCP tools - all use AI-optimized endpoints."""

        @self.server.tool("list_spark_apps")
        async def list_spark_apps() -> str:
            """List all Spark applications."""
            resp = await self.api.get("/api/v1/applications", params={"limit": 20})
            apps = resp.json()
            lines = ["# Spark Applications", ""]
            for app in apps:
                attempt = app["attempts"][0] if app.get("attempts") else {}
                status = "✓" if attempt.get("completed") else "⋯"
                dur = f"{attempt.get('duration', 0) // 1000}s"
                lines.append(f"- `{app['id']}`: {app['name']} ({status}, {dur})")
            return "\n".join(lines)

        @self.server.tool("get_spark_summary")
        async def get_spark_summary(app_id: str) -> str:
            """Get application summary with key metrics.

            Args:
                app_id: The application ID
            """
            resp = await self.api.get(f"/api/ai/{app_id}/summary")
            return resp.text  # Already markdown

        @self.server.tool("get_spark_problems")
        async def get_spark_problems(app_id: str) -> str:
            """Get problems/issues for an application (failures, skew, GC).

            Args:
                app_id: The application ID
            """
            resp = await self.api.get(f"/api/ai/{app_id}/problems")
            return resp.text  # Already markdown

        @self.server.tool("get_spark_stages")
        async def get_spark_stages(app_id: str) -> str:
            """Get stage summary table.

            Args:
                app_id: The application ID
            """
            resp = await self.api.get(f"/api/ai/{app_id}/stages")
            return resp.text  # Already markdown

        @self.server.tool("get_spark_executors")
        async def get_spark_executors(app_id: str) -> str:
            """Get executor health summary.

            Args:
                app_id: The application ID
            """
            resp = await self.api.get(f"/api/ai/{app_id}/executors")
            return resp.text  # Already markdown

        @self.server.tool("analyze_spark_app")
        async def analyze_spark_app(app_id: str, question: str) -> str:
            """Ask a question about an application (uses LLM).

            Args:
                app_id: The application ID
                question: Your question (e.g., "Why did tasks fail?")
            """
            resp = await self.api.post(
                "/api/insight/analyze",
                params={"app_id": app_id, "question": question}
            )
            return resp.json()["answer"]

        @self.server.tool("compare_spark_apps")
        async def compare_spark_apps(app_id_1: str, app_id_2: str) -> str:
            """Compare two applications side by side.

            Args:
                app_id_1: First application ID
                app_id_2: Second application ID
            """
            resp = await self.api.post(
                "/api/insight/diff",
                params={"app_id_1": app_id_1, "app_id_2": app_id_2}
            )
            return resp.text  # Already markdown

    async def run(self, transport: str = "stdio"):
        """Run the MCP server."""
        await self.server.run(transport)


# CLI command to run MCP server
@cli.command()
@click.option('--server', '-s', default='http://localhost:18080',
              help='Spark Insight server URL')
def mcp(server: str):
    """Run MCP server for Claude integration."""
    import asyncio
    mcp_server = SparkInsightMCPServer(server_url=server)
    console.print(f"[bold green]Starting MCP server[/bold green]")
    console.print(f"  Connecting to: {server}")
    asyncio.run(mcp_server.run())
```

**MCP Configuration (claude_desktop_config.json):**

```json
{
  "mcpServers": {
    "spark-insight": {
      "command": "spark-insight",
      "args": ["mcp", "--server", "http://localhost:18080"]
    }
  }
}
```

**For remote Spark Insight server:**

```json
{
  "mcpServers": {
    "spark-insight": {
      "command": "spark-insight",
      "args": ["mcp", "--server", "http://spark-insight.corp:18080"]
    }
  }
}
```

### 5. Application Diff Engine

Compare two Spark applications side-by-side.

```python
@dataclass
class DiffResult:
    app1: SparkApplication
    app2: SparkApplication

    # Metric comparisons
    duration_diff: int
    job_count_diff: int
    stage_count_diff: int
    task_count_diff: int

    # Detailed comparisons
    stage_diffs: List[StageDiff]
    executor_diffs: List[ExecutorDiff]
    config_diffs: List[ConfigDiff]

    # Analysis
    performance_change: str  # "improved", "degraded", "similar"
    key_differences: List[str]

@dataclass
class StageDiff:
    stage_name: str
    app1_metrics: StageMetrics
    app2_metrics: StageMetrics
    duration_change_percent: float
    shuffle_change_percent: float

class DiffEngine:
    def compare(self, app1: SparkApplication, app2: SparkApplication) -> DiffResult:
        """Compare two Spark applications."""

        # Match stages by name/description
        stage_diffs = self._compare_stages(app1.stages, app2.stages)

        # Compare executor behavior
        executor_diffs = self._compare_executors(app1.executors, app2.executors)

        # Compare Spark configurations
        config_diffs = self._compare_configs(app1.environment, app2.environment)

        # Generate key differences summary
        key_differences = self._identify_key_differences(
            stage_diffs, executor_diffs, config_diffs
        )

        return DiffResult(
            app1=app1,
            app2=app2,
            duration_diff=app2.duration_ms - app1.duration_ms,
            stage_diffs=stage_diffs,
            executor_diffs=executor_diffs,
            config_diffs=config_diffs,
            performance_change=self._assess_performance_change(app1, app2),
            key_differences=key_differences
        )

    def _compare_stages(self, stages1: List[Stage], stages2: List[Stage]) -> List[StageDiff]:
        """Match and compare stages between two applications."""
        diffs = []

        # Match by stage name
        stages1_by_name = {s.name: s for s in stages1}
        stages2_by_name = {s.name: s for s in stages2}

        all_names = set(stages1_by_name.keys()) | set(stages2_by_name.keys())

        for name in all_names:
            s1 = stages1_by_name.get(name)
            s2 = stages2_by_name.get(name)

            if s1 and s2:
                duration_change = ((s2.executor_run_time - s1.executor_run_time) /
                                   max(s1.executor_run_time, 1)) * 100
                shuffle_change = ((s2.shuffle_read_bytes - s1.shuffle_read_bytes) /
                                  max(s1.shuffle_read_bytes, 1)) * 100

                diffs.append(StageDiff(
                    stage_name=name,
                    app1_metrics=s1,
                    app2_metrics=s2,
                    duration_change_percent=duration_change,
                    shuffle_change_percent=shuffle_change
                ))

        return sorted(diffs, key=lambda d: abs(d.duration_change_percent), reverse=True)
```

### 6. Application Service (with Caching)

The application service integrates parsing, caching, and queries.

```python
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional, List

from fastapi import UploadFile, HTTPException
from pydantic import BaseModel


class ApplicationService:
    """
    Main service for managing Spark applications.

    Handles:
    - Event log discovery (from log directory)
    - Event log upload
    - Parsing with caching
    - Query execution
    """

    def __init__(
        self,
        log_dir: str = "./data/eventlogs",
        cache_dir: str = "./data/cache"
    ):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.cache = CacheManager(cache_dir)
        self._app_index: dict[str, Path] = {}  # app_id -> eventlog path

    def refresh_index(self):
        """Scan log directory for event logs."""
        self._app_index.clear()

        for path in self.log_dir.glob("**/*"):
            if path.is_file() and self._is_eventlog(path):
                app_id = self._extract_app_id(path)
                if app_id:
                    self._app_index[app_id] = path

    def list_applications(
        self,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[ApplicationInfo]:
        """List all applications (SHS compatible)."""
        self.refresh_index()
        apps = []

        for app_id, path in list(self._app_index.items())[:limit]:
            try:
                parsed = self.cache.get_or_parse(str(path))
                apps.append(self._to_application_info(parsed))
            except Exception:
                continue  # Skip unparseable logs

        return apps

    def get_application(self, app_id: str) -> ApplicationInfo:
        """Get application details (SHS compatible)."""
        path = self._app_index.get(app_id)
        if not path:
            raise HTTPException(404, f"Application {app_id} not found")

        parsed = self.cache.get_or_parse(str(path))
        return self._to_application_info(parsed)

    def get_query_engine(self, app_id: str) -> QueryEngine:
        """Get query engine for an application."""
        path = self._app_index.get(app_id)
        if not path:
            raise HTTPException(404, f"Application {app_id} not found")

        # Ensure cached
        if not self.cache.is_cached(str(path)):
            self.cache.get_or_parse(str(path))

        cache_path = self.cache.get_cache_path(str(path))
        return QueryEngine(cache_path)

    def get_jobs(self, app_id: str) -> List[JobData]:
        """Get jobs for an application."""
        engine = self.get_query_engine(app_id)
        df = engine.query("SELECT * FROM jobs ORDER BY jobId")
        return [JobData(**row) for row in df.to_dicts()]

    def get_stages(self, app_id: str) -> List[StageData]:
        """Get stages for an application."""
        engine = self.get_query_engine(app_id)
        df = engine.query("SELECT * FROM stages ORDER BY stageId, attemptId")
        return [StageData(**row) for row in df.to_dicts()]

    def get_executors(self, app_id: str) -> List[ExecutorSummary]:
        """Get executors for an application."""
        engine = self.get_query_engine(app_id)
        df = engine.get_executor_stats()
        return [ExecutorSummary(**row) for row in df.to_dicts()]

    async def upload_eventlog(self, file: UploadFile) -> ApplicationInfo:
        """Upload and parse an event log."""
        # Save file
        upload_id = str(uuid.uuid4())[:8]
        filename = file.filename or "eventlog"
        file_path = self.log_dir / f"{upload_id}_{filename}"

        content = await file.read()
        with open(file_path, "wb") as f:
            f.write(content)

        # Parse and cache
        try:
            parsed = self.cache.get_or_parse(str(file_path))
            app_id = parsed.app_info.get("id", upload_id)
            self._app_index[app_id] = file_path
            return self._to_application_info(parsed)
        except Exception as e:
            file_path.unlink()  # Clean up on failure
            raise HTTPException(400, f"Failed to parse event log: {e}")

    def _is_eventlog(self, path: Path) -> bool:
        """Check if file is a Spark event log."""
        name = path.name.lower()
        return (
            name.startswith("app-") or
            name.startswith("application_") or
            "eventlog" in name
        ) and (
            name.endswith(".json") or
            name.endswith(".json.gz") or
            name.endswith(".json.lz4") or
            not path.suffix  # Spark often writes without extension
        )

    def _extract_app_id(self, path: Path) -> Optional[str]:
        """Extract app ID from event log path or content."""
        name = path.stem.replace(".json", "")
        if name.startswith("app-") or name.startswith("application_"):
            return name
        # TODO: Read first line to get app ID
        return name

    def _to_application_info(self, parsed: ParsedApplication) -> ApplicationInfo:
        """Convert parsed data to SHS-compatible ApplicationInfo."""
        info = parsed.app_info
        return ApplicationInfo(
            id=info.get("id", "unknown"),
            name=info.get("name", "unknown"),
            attempts=[
                ApplicationAttemptInfo(
                    startTime=self._format_time(info.get("startTime", 0)),
                    endTime=self._format_time(info.get("endTime", 0)),
                    lastUpdated=self._format_time(info.get("endTime", 0)),
                    duration=info.get("endTime", 0) - info.get("startTime", 0),
                    sparkUser=info.get("sparkUser", ""),
                    completed=info.get("completed", False),
                    appSparkVersion=info.get("appSparkVersion", ""),
                )
            ]
        )

    @staticmethod
    def _format_time(ts: int) -> str:
        """Format timestamp for SHS API."""
        if ts:
            return datetime.fromtimestamp(ts / 1000).isoformat()
        return ""
```

**Storage Backends:**

```python
from abc import ABC, abstractmethod


class StorageBackend(ABC):
    """Abstract storage backend for event logs."""

    @abstractmethod
    def list_eventlogs(self) -> List[Path]:
        """List all event logs in storage."""
        pass

    @abstractmethod
    def read_eventlog(self, path: str) -> Path:
        """Read event log, return local path (may download)."""
        pass


class LocalStorage(StorageBackend):
    """Local filesystem storage."""

    def __init__(self, log_dir: str):
        self.log_dir = Path(log_dir)

    def list_eventlogs(self) -> List[Path]:
        return list(self.log_dir.glob("**/*"))

    def read_eventlog(self, path: str) -> Path:
        return Path(path)  # Already local


class S3Storage(StorageBackend):
    """AWS S3 storage backend."""

    def __init__(self, bucket: str, prefix: str = ""):
        import boto3
        self.s3 = boto3.client("s3")
        self.bucket = bucket
        self.prefix = prefix
        self.local_cache = Path("/tmp/spark-insight-s3")
        self.local_cache.mkdir(exist_ok=True)

    def list_eventlogs(self) -> List[Path]:
        """List event logs in S3 bucket."""
        paths = []
        paginator = self.s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for obj in page.get("Contents", []):
                paths.append(Path(obj["Key"]))

        return paths

    def read_eventlog(self, path: str) -> Path:
        """Download event log from S3 to local cache."""
        local_path = self.local_cache / path.replace("/", "_")

        if not local_path.exists():
            self.s3.download_file(self.bucket, path, str(local_path))

        return local_path
```

### 7. CLI (REST API Client)

The CLI is a thin client that talks to the Spark Insight REST API service.

```python
import click
import httpx
from rich.console import Console
from rich.table import Table

console = Console()

# Default service URL (can be overridden with --server)
DEFAULT_SERVER = "http://localhost:18080"


@click.group()
@click.option('--server', '-s', default=DEFAULT_SERVER, envvar='SPARK_INSIGHT_SERVER',
              help='Spark Insight server URL')
@click.pass_context
def cli(ctx, server: str):
    """Spark Insight CLI - Query Spark applications via REST API."""
    ctx.ensure_object(dict)
    ctx.obj['server'] = server
    ctx.obj['client'] = httpx.Client(base_url=server)


# ============= Application Commands =============

@cli.command()
@click.option('--status', type=click.Choice(['completed', 'running']))
@click.option('--limit', default=20)
@click.pass_context
def apps(ctx, status: str, limit: int):
    """List all applications (like SHS)."""
    client = ctx.obj['client']
    params = {"limit": limit}
    if status:
        params["status"] = status

    resp = client.get("/api/v1/applications", params=params)
    resp.raise_for_status()

    table = Table(title="Applications")
    table.add_column("App ID")
    table.add_column("Name")
    table.add_column("Status")
    table.add_column("Duration")
    table.add_column("Start Time")

    for app in resp.json():
        attempt = app["attempts"][0] if app.get("attempts") else {}
        table.add_row(
            app["id"],
            app["name"],
            "✓" if attempt.get("completed") else "⋯",
            f"{attempt.get('duration', 0) / 1000:.1f}s",
            attempt.get("startTime", "")[:19]
        )

    console.print(table)


@cli.command()
@click.argument('app_id')
@click.pass_context
def jobs(ctx, app_id: str):
    """List jobs for an application."""
    client = ctx.obj['client']
    resp = client.get(f"/api/v1/applications/{app_id}/jobs")
    resp.raise_for_status()

    table = Table(title=f"Jobs for {app_id}")
    table.add_column("Job ID")
    table.add_column("Name")
    table.add_column("Status")
    table.add_column("Tasks")
    table.add_column("Stages")

    for job in resp.json():
        status_icon = {"SUCCEEDED": "✓", "FAILED": "✗", "RUNNING": "⋯"}.get(
            job["status"], "?"
        )
        table.add_row(
            str(job["jobId"]),
            job["name"][:40],
            status_icon,
            f"{job['numCompletedTasks']}/{job['numTasks']}",
            f"{job['numCompletedStages']}/{len(job['stageIds'])}"
        )

    console.print(table)


@cli.command()
@click.argument('app_id')
@click.pass_context
def stages(ctx, app_id: str):
    """List stages for an application."""
    client = ctx.obj['client']
    resp = client.get(f"/api/v1/applications/{app_id}/stages")
    resp.raise_for_status()

    table = Table(title=f"Stages for {app_id}")
    table.add_column("Stage")
    table.add_column("Name")
    table.add_column("Status")
    table.add_column("Tasks")
    table.add_column("Input")
    table.add_column("Shuffle R/W")

    for stage in resp.json():
        table.add_row(
            f"{stage['stageId']}.{stage['attemptId']}",
            stage["name"][:30],
            stage["status"],
            f"{stage['numCompleteTasks']}/{stage['numTasks']}",
            _format_bytes(stage["inputBytes"]),
            f"{_format_bytes(stage['shuffleReadBytes'])}/{_format_bytes(stage['shuffleWriteBytes'])}"
        )

    console.print(table)


@cli.command()
@click.argument('app_id')
@click.pass_context
def executors(ctx, app_id: str):
    """List executors for an application."""
    client = ctx.obj['client']
    resp = client.get(f"/api/v1/applications/{app_id}/executors")
    resp.raise_for_status()

    table = Table(title=f"Executors for {app_id}")
    table.add_column("ID")
    table.add_column("Host")
    table.add_column("Status")
    table.add_column("Cores")
    table.add_column("Tasks")
    table.add_column("GC Time")

    for exec in resp.json():
        table.add_row(
            exec["id"],
            exec["hostPort"],
            "Active" if exec["isActive"] else exec.get("removeReason", "Removed"),
            str(exec["totalCores"]),
            f"{exec['completedTasks']}/{exec['totalTasks']}",
            f"{exec['totalGCTime'] / 1000:.1f}s"
        )

    console.print(table)


# ============= AI-Optimized Commands (compact output) =============

@cli.command()
@click.argument('app_id')
@click.pass_context
def summary(ctx, app_id: str):
    """Show application summary (compact, AI-friendly)."""
    client = ctx.obj['client']
    resp = client.get(f"/api/ai/{app_id}/summary")
    resp.raise_for_status()
    console.print(Markdown(resp.text))


@cli.command()
@click.argument('app_id')
@click.pass_context
def problems(ctx, app_id: str):
    """Show problems only (failures, skew, GC issues)."""
    client = ctx.obj['client']
    resp = client.get(f"/api/ai/{app_id}/problems")
    resp.raise_for_status()
    console.print(Markdown(resp.text))


@cli.command()
@click.argument('app_id')
@click.argument('question')
@click.pass_context
def ask(ctx, app_id: str, question: str):
    """Ask AI a question about an application."""
    client = ctx.obj['client']
    resp = client.post(
        "/api/insight/analyze",
        params={"app_id": app_id, "question": question}
    )
    resp.raise_for_status()
    console.print(Markdown(resp.json()["answer"]))


@cli.command()
@click.argument('app_id_1')
@click.argument('app_id_2')
@click.pass_context
def diff(ctx, app_id_1: str, app_id_2: str):
    """Compare two applications."""
    client = ctx.obj['client']
    resp = client.post(
        "/api/insight/diff",
        params={"app_id_1": app_id_1, "app_id_2": app_id_2}
    )
    resp.raise_for_status()
    console.print(Markdown(resp.text))


@cli.command()
@click.argument('eventlog', type=click.Path(exists=True))
@click.pass_context
def upload(ctx, eventlog: str):
    """Upload an event log to the server."""
    client = ctx.obj['client']
    with open(eventlog, 'rb') as f:
        resp = client.post(
            "/api/insight/upload",
            files={"file": f}
        )
    resp.raise_for_status()

    app = resp.json()
    console.print(f"[green]Uploaded:[/green] {app['id']} - {app['name']}")


# ============= Server Commands =============

@cli.command()
@click.option('--port', default=18080, help='Server port')
@click.option('--host', default='0.0.0.0', help='Server host')
@click.option('--log-dir', type=click.Path(), help='Event log directory')
def serve(port: int, host: str, log_dir: str):
    """Start Spark Insight server (REST API + UI)."""
    import uvicorn
    from spark_insight.server import create_app

    app = create_app(log_dir=log_dir)
    console.print(f"[bold green]Starting Spark Insight server[/bold green]")
    console.print(f"  REST API: http://{host}:{port}/api/v1/")
    console.print(f"  Web UI:   http://{host}:{port}/")

    uvicorn.run(app, host=host, port=port)


def _format_bytes(b: int) -> str:
    """Format bytes for display."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if abs(b) < 1024:
            return f"{b:.1f}{unit}"
        b /= 1024
    return f"{b:.1f}PB"
```

**Usage Examples:**

```bash
# Start the server (required for all other commands)
spark-insight serve --port 18080 --log-dir /path/to/eventlogs

# List applications
spark-insight apps
spark-insight apps --status completed --limit 10

# Quick summary (AI-optimized, compact output)
spark-insight summary app-20240101120000-0001

# Show problems only (failures, skew, GC)
spark-insight problems app-20240101120000-0001

# Detailed views (verbose, for debugging)
spark-insight jobs app-20240101120000-0001
spark-insight stages app-20240101120000-0001
spark-insight executors app-20240101120000-0001

# Ask AI questions
spark-insight ask app-20240101120000-0001 "What caused task failures?"
spark-insight ask app-20240101120000-0001 "Why is stage 5 slow?"

# Compare two applications
spark-insight diff app-20240101-0001 app-20240102-0001

# Upload an event log
spark-insight upload /path/to/eventlog.json.gz

# Use a different server
spark-insight --server http://spark-insight.corp:18080 apps
SPARK_INSIGHT_SERVER=http://remote:18080 spark-insight apps
```

---

## Web UI Design (Streamlit)

Streamlit allows building the entire UI in Python with minimal code. No React, JavaScript, or CSS knowledge required.

### Complete UI in ~300 Lines of Python

```python
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Page config
st.set_page_config(
    page_title="Spark Insight",
    page_icon="⚡",
    layout="wide"
)

# Sidebar - Upload or Select App
with st.sidebar:
    st.title("⚡ Spark Insight")

    # Upload new event log
    uploaded_file = st.file_uploader("Upload Event Log", type=["json", "gz", "lz4"])
    if uploaded_file:
        with st.spinner("Processing..."):
            app = process_upload(uploaded_file)
            st.session_state.current_app = app
            st.success(f"Loaded: {app.name}")

    # Or select from existing uploads
    st.divider()
    uploads = list_uploads()
    if uploads:
        selected = st.selectbox(
            "Or select existing:",
            options=uploads,
            format_func=lambda x: f"{x.app_name} ({x.upload_time:%Y-%m-%d})"
        )
        if st.button("Load"):
            st.session_state.current_app = load_app(selected.id)

# Main content - Tabs
if "current_app" in st.session_state:
    app = st.session_state.current_app

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Dashboard",
        "📋 Jobs",
        "🔄 Stages",
        "💻 Executors",
        "🤖 AI Analysis",
        "🔀 Compare"
    ])

    # ==================== DASHBOARD ====================
    with tab1:
        st.header(f"Application: {app.name}")

        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Duration", f"{app.duration_ms / 1000:.1f}s")
        col2.metric("Jobs", len(app.jobs), delta=f"{app.failed_jobs} failed")
        col3.metric("Stages", len(app.stages))
        col4.metric("Executors", len(app.executors))

        # Timeline chart
        st.subheader("Job Timeline")
        fig = create_job_timeline(app.jobs)
        st.plotly_chart(fig, use_container_width=True)

        # AI Quick Insights
        st.subheader("🤖 Quick Insights")
        with st.spinner("Analyzing..."):
            insights = get_quick_insights(app)
        for insight in insights:
            st.info(insight)

    # ==================== JOBS ====================
    with tab2:
        st.header("Jobs")

        # Filter
        status_filter = st.multiselect(
            "Filter by status",
            ["SUCCEEDED", "FAILED", "RUNNING"],
            default=["SUCCEEDED", "FAILED"]
        )

        # Jobs table
        jobs_df = get_jobs_dataframe(app, status_filter)
        st.dataframe(
            jobs_df,
            use_container_width=True,
            column_config={
                "duration": st.column_config.ProgressColumn(
                    "Duration",
                    min_value=0,
                    max_value=jobs_df["duration"].max()
                ),
                "status": st.column_config.TextColumn("Status")
            }
        )

        # Job detail expander
        selected_job = st.selectbox("Select job for details", jobs_df["job_id"])
        if selected_job:
            job = get_job(app, selected_job)
            with st.expander(f"Job {selected_job} Details", expanded=True):
                st.json(job.to_dict())

    # ==================== STAGES ====================
    with tab3:
        st.header("Stages")

        # Stage metrics table
        stages_df = get_stages_dataframe(app)
        st.dataframe(stages_df, use_container_width=True)

        # Skew analysis
        st.subheader("Data Skew Analysis")
        skew_df = analyze_skew(app)
        if not skew_df.empty:
            fig = px.bar(skew_df, x="stage_name", y="skew_ratio",
                        title="Task Duration Skew by Stage")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.success("No significant data skew detected")

    # ==================== EXECUTORS ====================
    with tab4:
        st.header("Executors")

        # Executor timeline
        st.subheader("Executor Lifecycle")
        fig = create_executor_timeline(app.executors)
        st.plotly_chart(fig, use_container_width=True)

        # Removal reasons
        st.subheader("Executor Removal Reasons")
        removal_df = get_executor_removals(app)
        if not removal_df.empty:
            fig = px.pie(removal_df, values="count", names="reason",
                        title="Removal Reasons")
            st.plotly_chart(fig)
        else:
            st.info("No executors were removed")

        # GC analysis
        st.subheader("GC Time Analysis")
        gc_df = analyze_gc_time(app)
        fig = px.bar(gc_df, x="executor_id", y="gc_percent",
                    title="GC Time % by Executor")
        st.plotly_chart(fig, use_container_width=True)

    # ==================== AI ANALYSIS ====================
    with tab5:
        st.header("🤖 AI Analysis")

        # Suggested questions
        st.write("**Suggested questions:**")
        suggestions = [
            "What caused the most task failures?",
            "Why were executors removed?",
            "Which stages have data skew?",
            "How can I improve performance?",
        ]
        cols = st.columns(len(suggestions))
        for i, suggestion in enumerate(suggestions):
            if cols[i].button(suggestion, key=f"sug_{i}"):
                st.session_state.question = suggestion

        # Question input
        question = st.text_input(
            "Ask a question about your Spark application:",
            value=st.session_state.get("question", ""),
            key="question_input"
        )

        if st.button("Analyze", type="primary") and question:
            with st.spinner("Thinking..."):
                response = analyze_with_llm(app, question)

            st.markdown("### Answer")
            st.markdown(response.answer)

            if response.relevant_data:
                with st.expander("📊 Relevant Data"):
                    st.dataframe(response.relevant_data)

        # Chat history
        if "chat_history" in st.session_state:
            st.divider()
            st.subheader("Chat History")
            for msg in st.session_state.chat_history:
                with st.chat_message(msg["role"]):
                    st.write(msg["content"])

    # ==================== COMPARE ====================
    with tab6:
        st.header("🔀 Compare Applications")

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Application 1")
            app1_select = st.selectbox(
                "Select first app",
                options=uploads,
                format_func=lambda x: x.app_name,
                key="app1"
            )

        with col2:
            st.subheader("Application 2")
            app2_select = st.selectbox(
                "Select second app",
                options=uploads,
                format_func=lambda x: x.app_name,
                key="app2"
            )

        if st.button("Compare", type="primary"):
            if app1_select and app2_select:
                with st.spinner("Comparing..."):
                    diff = compare_apps(app1_select.id, app2_select.id)

                # Summary metrics comparison
                st.subheader("Summary")
                metrics_df = create_comparison_table(diff)
                st.dataframe(metrics_df, use_container_width=True)

                # Duration comparison
                col1, col2, col3 = st.columns(3)
                col1.metric("App 1 Duration", f"{diff.app1.duration_ms/1000:.1f}s")
                col2.metric("App 2 Duration", f"{diff.app2.duration_ms/1000:.1f}s")
                delta = diff.duration_diff / 1000
                col3.metric("Difference", f"{abs(delta):.1f}s",
                           delta=f"{delta:+.1f}s",
                           delta_color="inverse")

                # Stage-by-stage comparison
                st.subheader("Stage Comparison")
                stage_diff_df = create_stage_diff_table(diff)
                st.dataframe(
                    stage_diff_df,
                    use_container_width=True,
                    column_config={
                        "change_percent": st.column_config.NumberColumn(
                            "Change %",
                            format="%.1f%%"
                        )
                    }
                )

                # Config diff
                if diff.config_diffs:
                    st.subheader("Configuration Differences")
                    for cfg in diff.config_diffs:
                        st.code(f"{cfg.key}:\n  App1: {cfg.value1}\n  App2: {cfg.value2}")

else:
    # No app loaded - show welcome
    st.title("⚡ Spark Insight")
    st.markdown("""
    Welcome to Spark Insight! Upload a Spark event log to get started.

    ### Features
    - 📊 **Dashboard** - Application overview and metrics
    - 🤖 **AI Analysis** - Ask questions in natural language
    - 🔀 **Compare** - Diff two application runs
    - 📋 **Detailed Views** - Jobs, stages, executors

    ### Getting Started
    1. Upload an event log using the sidebar
    2. Or select from previously uploaded logs
    """)

    # Demo mode
    if st.button("Load Demo Data"):
        st.session_state.current_app = load_demo_app()
        st.rerun()
```

### Running the Streamlit UI

```bash
# Install dependencies
pip install streamlit plotly pandas

# Run the app
streamlit run app.py

# Or via CLI
spark-insight serve /path/to/eventlog
```

### Key Streamlit Benefits

1. **No frontend build step** - Just Python
2. **Hot reload** - Changes appear instantly
3. **Built-in widgets** - File upload, tables, charts, chat
4. **Easy deployment** - Streamlit Cloud, Docker, any Python host
5. **State management** - Simple `st.session_state`

---

## API Design

### SHS-Compatible REST Endpoints (drop-in replacement)

These endpoints match the official Spark History Server API exactly:

```yaml
# Applications
GET  /api/v1/applications                                    # List all apps
GET  /api/v1/applications/{appId}                           # App details
GET  /api/v1/applications/{appId}/jobs                      # List jobs
GET  /api/v1/applications/{appId}/jobs/{jobId}              # Job details
GET  /api/v1/applications/{appId}/stages                    # List stages
GET  /api/v1/applications/{appId}/stages/{stageId}/{attemptId}      # Stage details
GET  /api/v1/applications/{appId}/stages/{stageId}/{attemptId}/taskList  # Tasks
GET  /api/v1/applications/{appId}/stages/{stageId}/{attemptId}/taskSummary
GET  /api/v1/applications/{appId}/executors                 # List executors
GET  /api/v1/applications/{appId}/allexecutors              # All executors (incl. removed)
GET  /api/v1/applications/{appId}/storage/rdd               # RDD storage info
GET  /api/v1/applications/{appId}/environment               # Spark config
GET  /api/v1/applications/{appId}/logs                      # Driver logs
GET  /api/v1/version                                         # API version
```

### Extended REST Endpoints (Spark Insight specific)

```yaml
# AI Analysis
POST /api/insight/analyze                  # LLM analysis
     params: app_id, question

# Application Comparison
POST /api/insight/diff                     # Compare two apps
     params: app_id_1, app_id_2

# Event Log Upload
POST /api/insight/upload                   # Upload event log
     body: multipart/form-data (file)
```

### AI-Optimized Endpoints (Compact for LLM/CLI/MCP)

SHS API returns verbose JSON (~10K tokens per app). These endpoints return **compact markdown** (~1K tokens) - used by CLI, MCP, and LLM analysis.

```yaml
# Compact summaries - markdown format, <2K tokens each
GET /api/ai/{appId}/summary      # App overview + key metrics
GET /api/ai/{appId}/problems     # Only failures, skew, issues
GET /api/ai/{appId}/stages       # Stage summary table
GET /api/ai/{appId}/executors    # Executor health summary
GET /api/ai/{appId}/config       # Key Spark configs only
```

**Design principles:**
1. **Markdown output** - Structured text, not verbose JSON
2. **Problems first** - Surface issues prominently
3. **Token budget** - Each endpoint <2000 tokens
4. **Truncate intelligently** - Error messages capped at 200 chars

**Implementation:**

```python
@app.get("/api/ai/{app_id}/summary")
async def get_summary_ai(app_id: str) -> PlainTextResponse:
    """App summary - compact markdown for AI/CLI/MCP."""
    engine = app_service.get_query_engine(app_id)
    info = app_service.get_application(app_id)
    attempt = info.attempts[0] if info.attempts else None

    stats = engine.query("""
        SELECT
            (SELECT COUNT(*) FROM jobs) as jobs,
            (SELECT COUNT(*) FROM jobs WHERE status='FAILED') as failed_jobs,
            (SELECT COUNT(*) FROM stages) as stages,
            (SELECT COUNT(*) FROM tasks) as tasks,
            (SELECT SUM(CASE WHEN failed THEN 1 ELSE 0 END) FROM tasks) as failed_tasks,
            (SELECT SUM(inputBytes) FROM stages) as input_bytes,
            (SELECT SUM(shuffleReadBytes + shuffleWriteBytes) FROM stages) as shuffle_bytes
    """).to_dicts()[0]

    return PlainTextResponse(f"""# {info.name}

| Metric | Value |
|--------|-------|
| App ID | `{app_id}` |
| Duration | {attempt.duration // 1000}s |
| Status | {'✓ Completed' if attempt.completed else '⋯ Running'} |
| Jobs | {stats['jobs']} ({stats['failed_jobs']} failed) |
| Stages | {stats['stages']} |
| Tasks | {stats['tasks']:,} ({stats['failed_tasks']} failed) |
| Input | {_fmt_bytes(stats['input_bytes'])} |
| Shuffle | {_fmt_bytes(stats['shuffle_bytes'])} |
""")


@app.get("/api/ai/{app_id}/problems")
async def get_problems_ai(app_id: str) -> PlainTextResponse:
    """Problems only - what's wrong with this app."""
    engine = app_service.get_query_engine(app_id)
    sections = []

    # Failed tasks
    failed = engine.query("""
        SELECT COUNT(*) as n,
               SUBSTR(COALESCE(errorMessage, 'Unknown'), 1, 200) as error
        FROM tasks WHERE failed GROUP BY error
        ORDER BY n DESC LIMIT 5
    """).to_dicts()
    if failed:
        sections.append("## Failed Tasks")
        for f in failed:
            sections.append(f"- **{f['n']}x**: {f['error']}")

    # Data skew (max/avg > 5x)
    skew = engine.query("""
        SELECT stageId, name,
               ROUND(MAX(duration)*1.0 / AVG(duration), 1) as ratio
        FROM tasks GROUP BY stageId, name
        HAVING ratio > 5 AND COUNT(*) > 10
        ORDER BY ratio DESC LIMIT 5
    """).to_dicts()
    if skew:
        sections.append("\n## Data Skew")
        for s in skew:
            sections.append(f"- Stage {s['stageId']}: {s['ratio']}x skew ({s['name'][:40]})")

    # Executor removals
    removed = engine.query("""
        SELECT id, SUBSTR(removeReason, 1, 100) as reason
        FROM executors WHERE removeReason IS NOT NULL LIMIT 5
    """).to_dicts()
    if removed:
        sections.append("\n## Executor Failures")
        for r in removed:
            sections.append(f"- {r['id']}: {r['reason']}")

    # GC issues (>20% time in GC)
    gc = engine.query("""
        SELECT executorId,
               ROUND(SUM(gcTime)*100.0 / SUM(duration), 0) as pct
        FROM tasks GROUP BY executorId
        HAVING pct > 20 ORDER BY pct DESC LIMIT 3
    """).to_dicts()
    if gc:
        sections.append("\n## High GC")
        for g in gc:
            sections.append(f"- Executor {g['executorId']}: {g['pct']:.0f}% GC time")

    if not sections:
        return PlainTextResponse("# No Problems\n\nNo significant issues detected.")

    return PlainTextResponse("# Problems\n\n" + "\n".join(sections))


@app.get("/api/ai/{app_id}/stages")
async def get_stages_ai(app_id: str) -> PlainTextResponse:
    """Stage summary table - compact."""
    engine = app_service.get_query_engine(app_id)

    stages = engine.query("""
        SELECT stageId, status, name, numTasks,
               inputBytes, shuffleReadBytes, shuffleWriteBytes
        FROM stages ORDER BY stageId LIMIT 50
    """).to_dicts()

    lines = ["# Stages", "",
             "| ID | Status | Tasks | Input | Shuffle | Name |",
             "|---:|--------|------:|------:|--------:|------|"]
    for s in stages:
        status = "✓" if s['status'] == 'COMPLETE' else "✗" if s['status'] == 'FAILED' else "⋯"
        lines.append(
            f"| {s['stageId']} | {status} | {s['numTasks']} | "
            f"{_fmt_bytes(s['inputBytes'])} | {_fmt_bytes(s['shuffleReadBytes'])} | "
            f"{s['name'][:30]} |"
        )

    return PlainTextResponse("\n".join(lines))
```

**Token comparison:**

| Endpoint | SHS API | AI API | Reduction |
|----------|---------|--------|-----------|
| App details | ~10,000 | ~500 | **20x** |
| All stages | ~20,000 | ~1,000 | **20x** |
| Problems | N/A | ~800 | **New** |

**Usage from CLI:**
```bash
# Compact output by default
spark-insight summary app-123      # calls /api/ai/{id}/summary
spark-insight problems app-123     # calls /api/ai/{id}/problems

# Verbose JSON if needed
spark-insight jobs app-123 --json  # calls /api/v1/.../jobs
```

**Usage from MCP:**
```python
@self.server.tool("get_spark_summary")
async def get_spark_summary(app_id: str) -> str:
    """Get compact app summary."""
    resp = await self.api.get(f"/api/ai/{app_id}/summary")
    return resp.text  # Already markdown, ready for LLM
```

---

## Deployment Options

### 1. Local Development

```bash
# Install via pip
pip install spark-insight

# Start the server (REST API + UI)
spark-insight serve --port 18080 --log-dir /path/to/eventlogs

# Server exposes:
#   - REST API: http://localhost:18080/api/v1/
#   - Web UI:   http://localhost:18080/
```

### 2. Docker (Recommended)

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY pyproject.toml .
RUN pip install .

# Create data directory
RUN mkdir -p /data/eventlogs /data/cache

ENV LOG_DIR=/data/eventlogs
ENV CACHE_DIR=/data/cache
EXPOSE 18080

# Run the unified server (REST API + UI)
CMD ["spark-insight", "serve", "--host", "0.0.0.0", "--port", "18080"]
```

**docker-compose.yml:**

```yaml
version: '3.8'

services:
  spark-insight:
    build: .
    ports:
      - "18080:18080"
    volumes:
      - ./eventlogs:/data/eventlogs
      - spark-insight-cache:/data/cache
    environment:
      - ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY}
      - LOG_DIR=/data/eventlogs
    restart: unless-stopped

volumes:
  spark-insight-cache:
```

```bash
# Run with Docker Compose
docker-compose up -d

# Or run directly
docker run -d \
  -p 18080:18080 \
  -v /path/to/eventlogs:/data/eventlogs \
  -e ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY \
  spark-insight:latest
```

### 3. Drop-in SHS Replacement

Since Spark Insight implements the same REST API as SHS, you can use it as a drop-in replacement:

```bash
# Instead of:
# $SPARK_HOME/sbin/start-history-server.sh

# Use:
spark-insight serve --port 18080 --log-dir hdfs:///spark-logs

# Or with Docker:
docker run -d \
  -p 18080:18080 \
  -e LOG_DIR=s3://my-bucket/spark-logs \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  spark-insight:latest
```

**Update existing Spark configs:**

```properties
# spark-defaults.conf
spark.eventLog.enabled=true
spark.eventLog.dir=s3://my-bucket/spark-logs

# Point to Spark Insight instead of SHS
spark.yarn.historyServer.address=spark-insight.corp:18080
```

### 4. Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: spark-insight
spec:
  replicas: 2
  selector:
    matchLabels:
      app: spark-insight
  template:
    metadata:
      labels:
        app: spark-insight
    spec:
      containers:
      - name: spark-insight
        image: spark-insight:latest
        ports:
        - containerPort: 18080
        env:
        - name: LOG_DIR
          value: "s3://my-bucket/spark-logs"
        - name: AWS_ACCESS_KEY_ID
          valueFrom:
            secretKeyRef:
              name: aws-credentials
              key: access_key
        - name: AWS_SECRET_ACCESS_KEY
          valueFrom:
            secretKeyRef:
              name: aws-credentials
              key: secret_key
        - name: ANTHROPIC_API_KEY
          valueFrom:
            secretKeyRef:
              name: spark-insight-secrets
              key: anthropic_api_key
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "2000m"
        livenessProbe:
          httpGet:
            path: /api/v1/version
            port: 18080
          initialDelaySeconds: 10
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /api/v1/version
            port: 18080
          initialDelaySeconds: 5
          periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: spark-insight
spec:
  selector:
    app: spark-insight
  ports:
  - port: 80
    targetPort: 18080
  type: LoadBalancer
```

### 5. Cloud Platforms

| Platform | Service | Command |
|----------|---------|---------|
| **AWS** | ECS Fargate | `aws ecs create-service ...` |
| **AWS** | App Runner | Auto-deploy from ECR |
| **GCP** | Cloud Run | `gcloud run deploy spark-insight` |
| **Azure** | Container Apps | `az containerapp create` |

**Example: Google Cloud Run:**

```bash
# Build and push
gcloud builds submit --tag gcr.io/PROJECT/spark-insight

# Deploy
gcloud run deploy spark-insight \
  --image gcr.io/PROJECT/spark-insight \
  --platform managed \
  --port 18080 \
  --set-env-vars "LOG_DIR=gs://my-bucket/spark-logs" \
  --set-secrets "ANTHROPIC_API_KEY=anthropic-key:latest"
```

---

## Project Structure

```
spark-insight/
├── README.md
├── pyproject.toml                 # Single Python package
├── Dockerfile
├── docker-compose.yml
│
└── src/spark_insight/
    ├── __init__.py
    ├── __main__.py               # Entry point: python -m spark_insight
    │
    ├── cli.py                    # CLI commands (click + rich)
    │
    ├── server/                   # REST API Server
    │   ├── __init__.py
    │   ├── app.py               # FastAPI app factory
    │   ├── routes/
    │   │   ├── __init__.py
    │   │   ├── shs.py           # SHS-compatible endpoints
    │   │   └── insight.py       # Extended API (analyze, diff, upload)
    │   └── deps.py              # Dependency injection
    │
    ├── core/                     # Core business logic
    │   ├── __init__.py
    │   ├── parser.py            # Event log parser (Polars)
    │   ├── cache.py             # Parquet cache manager
    │   ├── query.py             # DuckDB query engine
    │   ├── models.py            # Pydantic models (SHS-compatible)
    │   └── diff.py              # Application diff engine
    │
    ├── storage/                  # Storage backends
    │   ├── __init__.py
    │   ├── base.py              # Abstract storage interface
    │   ├── local.py             # Local filesystem
    │   ├── s3.py                # AWS S3
    │   └── hdfs.py              # HDFS (optional)
    │
    ├── llm/                      # LLM integration
    │   ├── __init__.py
    │   ├── service.py           # LLM analysis service
    │   └── prompts.py           # System prompts
    │
    ├── mcp/                      # MCP server
    │   ├── __init__.py
    │   └── server.py            # MCP server (REST API client)
    │
    └── ui/                       # Streamlit UI
        ├── __init__.py
        ├── app.py               # Main Streamlit app
        └── pages/               # Multi-page Streamlit
            ├── 1_dashboard.py
            ├── 2_jobs.py
            ├── 3_stages.py
            ├── 4_executors.py
            ├── 5_ai_analysis.py
            └── 6_compare.py

├── tests/
│   ├── conftest.py              # Pytest fixtures
│   ├── test_parser.py
│   ├── test_query.py
│   ├── test_api.py
│   ├── test_llm.py
│   └── test_mcp.py
│
└── examples/
    └── eventlogs/               # Sample event logs for testing
        ├── small_app.json       # ~1MB
        └── large_app.json.gz    # ~100MB compressed
```

### Installation

```bash
# Install from PyPI
pip install spark-insight

# Install from source
pip install -e .

# Install with optional dependencies
pip install spark-insight[s3]     # S3 storage support
pip install spark-insight[hdfs]   # HDFS storage support
pip install spark-insight[all]    # All optional dependencies
```

### Development Workflow

```bash
# Install in development mode
pip install -e ".[dev]"

# Run tests
pytest

# Run linting
ruff check src/
mypy src/

# Run the server
spark-insight serve --port 18080

# Run Streamlit UI separately (for development)
streamlit run src/spark_insight/ui/app.py

# Format code
ruff format src/
```

### Dependencies (pyproject.toml)

```toml
[project]
name = "spark-insight"
version = "1.0.0"
description = "Next-generation Spark History Server with AI analysis"
requires-python = ">=3.10"
dependencies = [
    # API & Web
    "fastapi>=0.100",
    "uvicorn[standard]>=0.20",
    "httpx>=0.24",
    "pydantic>=2.0",
    # Data Processing (Polars = Rust speed)
    "polars>=0.20",
    "duckdb>=0.9",
    # CLI
    "click>=8.0",
    "rich>=13.0",
    # UI
    "streamlit>=1.30",
    "plotly>=5.0",
    # LLM
    "anthropic>=0.20",
    "mcp>=0.1",
]

[project.optional-dependencies]
s3 = ["boto3>=1.28"]
hdfs = ["pyarrow>=14.0"]
dev = [
    "pytest>=7.0",
    "pytest-asyncio>=0.21",
    "ruff>=0.1",
    "mypy>=1.0",
]
all = ["spark-insight[s3,hdfs]"]

[project.scripts]
spark-insight = "spark_insight.cli:cli"
```

---

## Implementation Roadmap

### Phase 1: Core Foundation (Week 1)
- [ ] Project setup (pyproject.toml, structure)
- [ ] Pydantic models (SHS-compatible)
- [ ] Streaming event log parser (orjson + gzip/lz4)
- [ ] DuckDB query engine
- [ ] Basic tests

### Phase 2: REST API Server (Week 2)
- [ ] FastAPI application
- [ ] SHS-compatible endpoints (all /api/v1/* routes)
- [ ] Application service (list, get, query)
- [ ] Storage backend (local filesystem)
- [ ] API tests

### Phase 3: CLI Client (Week 3)
- [ ] Click CLI with rich output
- [ ] All SHS query commands (apps, jobs, stages, executors)
- [ ] Upload command
- [ ] Server command
- [ ] CLI tests

### Phase 4: Streamlit UI (Week 4)
- [ ] Dashboard page
- [ ] Jobs/Stages/Executors pages
- [ ] Compare page
- [ ] File upload
- [ ] Connection to REST API

### Phase 5: LLM Integration (Week 5)
- [ ] LLM service with Claude API
- [ ] Analysis endpoint (/api/insight/analyze)
- [ ] AI chat in Streamlit
- [ ] CLI ask command
- [ ] Prompts and context building

### Phase 6: MCP Server + Release (Week 6)
- [ ] MCP server (REST API client)
- [ ] MCP resources and tools
- [ ] Diff engine (/api/insight/diff)
- [ ] Docker image
- [ ] PyPI release
- [ ] Documentation

### Estimated Total: 6 weeks

| Component | Lines of Code (est.) |
|-----------|---------------------|
| Core (parser, models, query) | ~800 |
| REST API server | ~500 |
| CLI client | ~400 |
| Streamlit UI | ~600 |
| LLM service | ~300 |
| MCP server | ~300 |
| Storage backends | ~200 |
| Tests | ~800 |
| **Total** | **~4,000** |

### Comparison with Previous Design

| Aspect | Rust + Python | Python + Polars |
|--------|---------------|-----------------|
| Lines of code | ~5,000 | ~4,000 |
| Languages | 2 (Rust, Python) | 1 (Python) |
| Build complexity | High (maturin, PyO3) | Low (`pip install`) |
| Parse 1GB | ~5s | ~15s |
| Parse 5GB | ~25s | ~75s |
| Development time | 7 weeks | 6 weeks |
| Maintenance | Harder | Easier |

**Trade-off:** ~3x slower parsing for significantly simpler development. Polars gives us Rust performance under the hood while keeping everything in Python. For production workloads (event logs up to 5GB), this is acceptable.

---

## Future Enhancements

1. **HDFS/S3 streaming** - Parse directly from cloud storage without download
2. **In-progress app support** - Tail event logs for running applications
3. **Alerts & anomaly detection** - Flag unusual patterns automatically
4. **Collaboration** - Share analysis links, annotations
5. **Custom dashboards** - User-defined metrics and views
6. **Plugin system** - Extend with custom analyzers
7. **Ollama support** - Local LLM for air-gapped environments
8. **Metrics export** - Prometheus/Grafana integration
9. **SHS proxy mode** - Augment existing SHS with AI features

---

## Summary: Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Language** | Python only | Simpler dev, maintenance |
| **Parser** | Polars (Rust-based) | ~15s/GB, no build complexity |
| **Cache** | Parquet files | Fast, compressed, Polars-native |
| **Query** | DuckDB | SQL queries, zero-copy from Parquet |
| **Architecture** | Single REST API service | One service to deploy, maintain |
| **API** | SHS-compatible | Drop-in replacement, existing tools work |
| **Clients** | CLI, UI, MCP all via REST | Consistent behavior, testable |
| **UI** | Streamlit | Python-native, fast iteration |

---

## References

- [Spark History Server REST API](https://spark.apache.org/docs/latest/monitoring.html#rest-api)
- [Spark Event Log Format](https://spark.apache.org/docs/latest/monitoring.html#viewing-after-the-fact)
- [Model Context Protocol](https://modelcontextprotocol.io/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Claude API Documentation](https://docs.anthropic.com/)
