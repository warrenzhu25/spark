# Spark Insight: Next-Generation Spark History Server

## Overview

Spark Insight is a modern, AI-powered tool for analyzing Apache Spark application history. It provides a beautiful UI, natural language querying via LLM integration, MCP server capabilities, and can run in serverless mode with a single event log.

### Goals

1. **Modern Experience** - Beautiful, responsive UI with interactive visualizations
2. **AI-Powered Analysis** - Natural language queries to understand failures, performance issues
3. **MCP Integration** - Expose Spark data to LLM tools (Claude, etc.)
4. **Serverless Mode** - Zero-config analysis of individual event logs
5. **Comparative Analysis** - Side-by-side diff of application runs

### Deployment Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| **CLI** | Single event log, local analysis | Developer debugging |
| **Service** | Multi-user, upload event logs | Team/org-wide analysis |
| **MCP** | Expose data to LLM tools | AI-assisted debugging |

### Non-Goals

- Replace the official Spark History Server in production clusters
- Real-time streaming of running applications (focus on completed apps)
- Modify or write to Spark clusters

---

## Architecture

### Service Mode (Multi-user with Upload)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Web UI (Streamlit)                                │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────────────────┐│
│  │  Upload     │ │  Dashboard  │ │  Diff View  │ │  AI Chat Interface      ││
│  │  Event Log  │ │  & Analysis │ │             │ │                         ││
│  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────────────────┘│
├─────────────────────────────────────────────────────────────────────────────┤
│                           API Layer (FastAPI)                               │
├───────────────┬───────────────┬───────────────┬─────────────────────────────┤
│  Upload       │    Query      │     LLM       │         MCP                 │
│  Service      │    Engine     │   Service     │       Server                │
├───────────────┴───────────────┴───────────────┴─────────────────────────────┤
│                           Data Layer                                        │
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌──────────────────────┐ │
│  │  Event Log Storage  │  │  DuckDB             │  │  Vector Store        │ │
│  │  (S3 / Local Disk)  │  │  (Per-app Analysis) │  │  (Embeddings)        │ │
│  └─────────────────────┘  └─────────────────────┘  └──────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

### CLI Mode (Single Event Log)

```
┌──────────────────────────────────────────┐
│          spark-insight CLI               │
├──────────────────────────────────────────┤
│  serve   → Streamlit UI (localhost)      │
│  ask     → LLM Q&A in terminal           │
│  mcp     → MCP server for Claude         │
│  diff    → Compare two apps              │
└──────────────────────────────────────────┘
```

---

## Technology Stack

| Layer | Technology | Rationale |
|-------|------------|-----------|
| **Frontend** | Streamlit | Python-native, no React needed, rapid iteration |
| **Charts** | Plotly (via Streamlit) | Interactive, built-in Streamlit support |
| **Backend** | FastAPI (Python) | Async, fast, great for LLM integration |
| **Database** | DuckDB (embedded) | Analytical queries, zero-config, fast |
| **Storage** | S3 / Local disk | Event log storage for service mode |
| **Vector Store** | ChromaDB (embedded) | Local embeddings for semantic search |
| **LLM** | Claude API / Ollama | Cloud or local LLM options |
| **MCP** | mcp-python SDK | Official MCP implementation |

### Why Streamlit Instead of React?

| Aspect | Streamlit | React/Next.js |
|--------|-----------|---------------|
| **Learning curve** | Hours (Python only) | Weeks (JS, React, CSS) |
| **Development speed** | Very fast | Slower |
| **Code complexity** | ~500 lines | ~5000+ lines |
| **Maintenance** | Easy | Requires frontend expertise |
| **Customization** | Good enough | Unlimited |
| **Deployment** | Simple | More complex |

For a data analysis tool, Streamlit provides 90% of the functionality with 10% of the effort.

---

## Core Features

### 1. Event Log Parsing

Parse Spark event logs (JSON format) into structured data.

```python
# Core data models
@dataclass
class SparkApplication:
    app_id: str
    app_name: str
    start_time: datetime
    end_time: datetime
    duration_ms: int
    spark_version: str
    user: str

@dataclass
class Job:
    job_id: int
    submission_time: datetime
    completion_time: datetime
    status: JobStatus  # SUCCEEDED, FAILED, RUNNING
    num_stages: int
    num_tasks: int
    failure_reason: Optional[str]

@dataclass
class Stage:
    stage_id: int
    attempt_id: int
    name: str
    status: StageStatus
    num_tasks: int
    input_bytes: int
    output_bytes: int
    shuffle_read_bytes: int
    shuffle_write_bytes: int
    executor_run_time: int
    failure_reason: Optional[str]

@dataclass
class Executor:
    executor_id: str
    host: str
    add_time: datetime
    remove_time: Optional[datetime]
    remove_reason: Optional[str]
    total_cores: int
    max_memory: int
    total_tasks: int
    failed_tasks: int
    total_duration: int
    total_gc_time: int

@dataclass
class Task:
    task_id: int
    stage_id: int
    executor_id: str
    status: TaskStatus
    duration: int
    gc_time: int
    error_message: Optional[str]
```

**Event Types to Parse:**
- `SparkListenerApplicationStart/End`
- `SparkListenerJobStart/End`
- `SparkListenerStageSubmitted/Completed`
- `SparkListenerTaskStart/End`
- `SparkListenerExecutorAdded/Removed`
- `SparkListenerBlockManagerAdded/Removed`
- `SparkListenerEnvironmentUpdate`

### 2. Query Engine

SQL-like queries over parsed data using DuckDB.

```python
class QueryEngine:
    def __init__(self, db_path: str = ":memory:"):
        self.conn = duckdb.connect(db_path)

    def load_application(self, app: SparkApplication):
        """Load parsed application data into DuckDB tables."""
        pass

    def query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return results."""
        return self.conn.execute(sql).fetchdf()

    # Pre-built analytical queries
    def get_failed_tasks_summary(self) -> pd.DataFrame:
        return self.query("""
            SELECT
                error_message,
                COUNT(*) as count,
                AVG(duration) as avg_duration
            FROM tasks
            WHERE status = 'FAILED'
            GROUP BY error_message
            ORDER BY count DESC
        """)

    def get_executor_removal_reasons(self) -> pd.DataFrame:
        return self.query("""
            SELECT
                remove_reason,
                COUNT(*) as count,
                AVG(EXTRACT(EPOCH FROM (remove_time - add_time))) as avg_lifetime_sec
            FROM executors
            WHERE remove_reason IS NOT NULL
            GROUP BY remove_reason
            ORDER BY count DESC
        """)

    def get_stage_skew_analysis(self) -> pd.DataFrame:
        return self.query("""
            SELECT
                stage_id,
                name,
                MIN(duration) as min_task_duration,
                MAX(duration) as max_task_duration,
                AVG(duration) as avg_task_duration,
                MAX(duration) / NULLIF(AVG(duration), 0) as skew_ratio
            FROM tasks
            GROUP BY stage_id, name
            HAVING COUNT(*) > 1
            ORDER BY skew_ratio DESC
        """)
```

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

### 4. MCP Server

Expose Spark data via Model Context Protocol for integration with Claude and other tools.

```python
from mcp import Server, Resource, Tool

class SparkInsightMCPServer:
    def __init__(self, query_engine: QueryEngine):
        self.server = Server("spark-insight")
        self.query_engine = query_engine
        self._register_resources()
        self._register_tools()

    def _register_resources(self):
        """Register MCP resources for Spark data."""

        @self.server.resource("spark://application/summary")
        async def get_app_summary() -> Resource:
            summary = self.query_engine.get_application_summary()
            return Resource(
                uri="spark://application/summary",
                name="Application Summary",
                mimeType="application/json",
                content=summary.to_json()
            )

        @self.server.resource("spark://jobs")
        async def get_jobs() -> Resource:
            jobs = self.query_engine.query("SELECT * FROM jobs")
            return Resource(
                uri="spark://jobs",
                name="Jobs",
                mimeType="application/json",
                content=jobs.to_json()
            )

        @self.server.resource("spark://stages")
        async def get_stages() -> Resource:
            stages = self.query_engine.query("SELECT * FROM stages")
            return Resource(
                uri="spark://stages",
                name="Stages",
                mimeType="application/json",
                content=stages.to_json()
            )

        @self.server.resource("spark://executors")
        async def get_executors() -> Resource:
            executors = self.query_engine.query("SELECT * FROM executors")
            return Resource(
                uri="spark://executors",
                name="Executors",
                mimeType="application/json",
                content=executors.to_json()
            )

        @self.server.resource("spark://failures")
        async def get_failures() -> Resource:
            failures = self.query_engine.get_failed_tasks_summary()
            return Resource(
                uri="spark://failures",
                name="Failure Summary",
                mimeType="application/json",
                content=failures.to_json()
            )

    def _register_tools(self):
        """Register MCP tools for Spark analysis."""

        @self.server.tool("query_spark_data")
        async def query_spark_data(sql: str) -> str:
            """Execute SQL query against Spark application data.

            Tables available: jobs, stages, tasks, executors, environment
            """
            try:
                result = self.query_engine.query(sql)
                return result.to_markdown()
            except Exception as e:
                return f"Query error: {str(e)}"

        @self.server.tool("analyze_failures")
        async def analyze_failures() -> str:
            """Analyze task and stage failures in the application."""
            failed_tasks = self.query_engine.get_failed_tasks_summary()
            failed_stages = self.query_engine.query(
                "SELECT * FROM stages WHERE status = 'FAILED'"
            )
            return f"## Failed Tasks\n{failed_tasks.to_markdown()}\n\n## Failed Stages\n{failed_stages.to_markdown()}"

        @self.server.tool("analyze_performance")
        async def analyze_performance() -> str:
            """Analyze performance metrics including skew, GC, and shuffle."""
            skew = self.query_engine.get_stage_skew_analysis()
            gc = self.query_engine.query("""
                SELECT executor_id, SUM(gc_time) as total_gc,
                       SUM(duration) as total_duration,
                       SUM(gc_time) * 100.0 / SUM(duration) as gc_percent
                FROM tasks GROUP BY executor_id ORDER BY gc_percent DESC
            """)
            return f"## Data Skew\n{skew.to_markdown()}\n\n## GC Analysis\n{gc.to_markdown()}"

        @self.server.tool("compare_stages")
        async def compare_stages(stage_id_1: int, stage_id_2: int) -> str:
            """Compare metrics between two stages."""
            comparison = self.query_engine.query(f"""
                SELECT
                    stage_id,
                    name,
                    num_tasks,
                    input_bytes,
                    shuffle_read_bytes,
                    shuffle_write_bytes,
                    executor_run_time
                FROM stages
                WHERE stage_id IN ({stage_id_1}, {stage_id_2})
            """)
            return comparison.to_markdown()

    async def run(self, transport: str = "stdio"):
        """Run the MCP server."""
        await self.server.run(transport)
```

**MCP Configuration (claude_desktop_config.json):**
```json
{
  "mcpServers": {
    "spark-insight": {
      "command": "spark-insight",
      "args": ["mcp", "--eventlog", "/path/to/eventlog"]
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

### 6. Service Mode (Multi-user with Upload)

Deploy as a shared service where users can upload event logs.

```python
import os
import uuid
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel

class UploadedApp(BaseModel):
    id: str
    filename: str
    upload_time: datetime
    app_name: str
    app_id: str
    duration_ms: int
    status: str  # "processing", "ready", "failed"

class StorageService:
    def __init__(self, storage_path: str = "./data/eventlogs"):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        self.metadata_db = DuckDB(self.storage_path / "metadata.db")

    async def save_eventlog(self, file: UploadFile) -> UploadedApp:
        """Save uploaded event log and parse it."""
        upload_id = str(uuid.uuid4())[:8]
        file_path = self.storage_path / f"{upload_id}_{file.filename}"

        # Save file
        content = await file.read()
        with open(file_path, "wb") as f:
            f.write(content)

        # Parse and extract metadata
        try:
            app = parse_eventlog(file_path)
            uploaded = UploadedApp(
                id=upload_id,
                filename=file.filename,
                upload_time=datetime.now(),
                app_name=app.name,
                app_id=app.app_id,
                duration_ms=app.duration_ms,
                status="ready"
            )
        except Exception as e:
            uploaded = UploadedApp(
                id=upload_id,
                filename=file.filename,
                upload_time=datetime.now(),
                app_name="Unknown",
                app_id="Unknown",
                duration_ms=0,
                status="failed"
            )

        # Store metadata
        self.metadata_db.insert("uploads", uploaded)
        return uploaded

    def list_uploads(self) -> List[UploadedApp]:
        """List all uploaded event logs."""
        return self.metadata_db.query("SELECT * FROM uploads ORDER BY upload_time DESC")

    def get_eventlog_path(self, upload_id: str) -> Path:
        """Get path to event log file."""
        matches = list(self.storage_path.glob(f"{upload_id}_*"))
        if not matches:
            raise HTTPException(404, f"Event log {upload_id} not found")
        return matches[0]

    def delete_eventlog(self, upload_id: str):
        """Delete an uploaded event log."""
        path = self.get_eventlog_path(upload_id)
        path.unlink()
        self.metadata_db.execute(f"DELETE FROM uploads WHERE id = '{upload_id}'")


# FastAPI routes for upload
app = FastAPI()
storage = StorageService()

@app.post("/api/upload")
async def upload_eventlog(file: UploadFile = File(...)) -> UploadedApp:
    """Upload a Spark event log for analysis."""
    if not file.filename:
        raise HTTPException(400, "No file provided")

    return await storage.save_eventlog(file)

@app.get("/api/uploads")
async def list_uploads() -> List[UploadedApp]:
    """List all uploaded event logs."""
    return storage.list_uploads()

@app.delete("/api/uploads/{upload_id}")
async def delete_upload(upload_id: str):
    """Delete an uploaded event log."""
    storage.delete_eventlog(upload_id)
    return {"status": "deleted"}

@app.get("/api/uploads/{upload_id}/analyze")
async def analyze_upload(upload_id: str, question: str) -> AnalysisResult:
    """Ask a question about an uploaded event log."""
    path = storage.get_eventlog_path(upload_id)
    engine = load_eventlog(path)
    llm = LLMService(engine)
    return await llm.analyze(question)
```

**Cloud Storage Support (S3):**

```python
import boto3
from urllib.parse import urlparse

class S3StorageService(StorageService):
    def __init__(self, bucket: str, prefix: str = "eventlogs/"):
        self.s3 = boto3.client("s3")
        self.bucket = bucket
        self.prefix = prefix

    async def save_eventlog(self, file: UploadFile) -> UploadedApp:
        upload_id = str(uuid.uuid4())[:8]
        key = f"{self.prefix}{upload_id}_{file.filename}"

        content = await file.read()
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=content)

        # Download to temp for parsing
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(content)
            app = parse_eventlog(tmp.name)

        return UploadedApp(
            id=upload_id,
            filename=file.filename,
            upload_time=datetime.now(),
            app_name=app.name,
            app_id=app.app_id,
            duration_ms=app.duration_ms,
            status="ready"
        )

    def load_from_s3_url(self, s3_url: str) -> QueryEngine:
        """Load event log directly from S3 URL."""
        parsed = urlparse(s3_url)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            self.s3.download_file(bucket, key, tmp.name)
            return load_eventlog(tmp.name)
```

### 7. CLI Mode

Run analysis on a single event log without any setup.

```python
# CLI interface
@click.group()
def cli():
    """Spark Insight - AI-powered Spark analysis."""
    pass

@cli.command()
@click.argument('eventlog', type=click.Path(exists=True))
@click.option('--port', default=8080, help='Web UI port')
@click.option('--no-browser', is_flag=True, help='Do not open browser')
def serve(eventlog: str, port: int, no_browser: bool):
    """Start web UI for analyzing an event log."""
    app = create_app(eventlog)

    if not no_browser:
        webbrowser.open(f"http://localhost:{port}")

    uvicorn.run(app, host="0.0.0.0", port=port)

@cli.command()
@click.argument('eventlog', type=click.Path(exists=True))
@click.option('--question', '-q', help='Question to ask about the application')
def ask(eventlog: str, question: str):
    """Ask a question about a Spark application (CLI mode)."""
    engine = load_eventlog(eventlog)
    llm = LLMService(engine)

    if question:
        result = asyncio.run(llm.analyze(question))
        click.echo(result.answer)
    else:
        # Interactive mode
        while True:
            question = click.prompt("Ask about your Spark app")
            result = asyncio.run(llm.analyze(question))
            click.echo(f"\n{result.answer}\n")

@cli.command()
@click.argument('eventlog', type=click.Path(exists=True))
def mcp(eventlog: str):
    """Run as MCP server for LLM integration."""
    engine = load_eventlog(eventlog)
    server = SparkInsightMCPServer(engine)
    asyncio.run(server.run())

@cli.command()
@click.argument('eventlog1', type=click.Path(exists=True))
@click.argument('eventlog2', type=click.Path(exists=True))
@click.option('--output', '-o', type=click.Path(), help='Output file for diff report')
def diff(eventlog1: str, eventlog2: str, output: str):
    """Compare two Spark applications."""
    app1 = load_eventlog(eventlog1)
    app2 = load_eventlog(eventlog2)

    diff_engine = DiffEngine()
    result = diff_engine.compare(app1, app2)

    report = generate_diff_report(result)

    if output:
        with open(output, 'w') as f:
            f.write(report)
    else:
        click.echo(report)
```

**Usage Examples:**
```bash
# Start web UI for single event log
spark-insight serve /path/to/eventlog

# Ask a question via CLI
spark-insight ask /path/to/eventlog -q "What caused task failures?"

# Interactive Q&A mode
spark-insight ask /path/to/eventlog

# Run as MCP server
spark-insight mcp /path/to/eventlog

# Compare two applications
spark-insight diff /path/to/eventlog1 /path/to/eventlog2

# Generate diff report
spark-insight diff app1.log app2.log -o diff-report.md
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

### REST Endpoints

```yaml
# Application
GET  /api/application              # Get application summary
GET  /api/application/environment  # Get environment/config

# Jobs
GET  /api/jobs                     # List all jobs
GET  /api/jobs/{jobId}             # Get job details

# Stages
GET  /api/stages                   # List all stages
GET  /api/stages/{stageId}         # Get stage details
GET  /api/stages/{stageId}/tasks   # Get tasks for stage

# Executors
GET  /api/executors                # List all executors
GET  /api/executors/{executorId}   # Get executor details

# Analysis
POST /api/analyze                  # LLM analysis endpoint
     body: { "question": "string" }

GET  /api/insights                 # Pre-computed insights
GET  /api/insights/failures        # Failure analysis
GET  /api/insights/performance     # Performance analysis
GET  /api/insights/recommendations # Optimization recommendations

# Diff
POST /api/diff                     # Compare two applications
     body: { "eventlog1": "path", "eventlog2": "path" }
```

### WebSocket

```yaml
WS /ws/analyze    # Streaming LLM responses
```

---

## Deployment Options

### 1. Local CLI (Development / Single User)

```bash
# Install via pip
pip install spark-insight

# Run with local event log
spark-insight serve /path/to/eventlog

# Or interactive Q&A
spark-insight ask /path/to/eventlog
```

### 2. Docker (Recommended for Service Mode)

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY . .
RUN pip install -e .

# Create data directory for uploads
RUN mkdir -p /data/eventlogs

ENV STORAGE_PATH=/data/eventlogs
EXPOSE 8501

# Run Streamlit
CMD ["streamlit", "run", "src/spark_insight/app.py", \
     "--server.port=8501", "--server.address=0.0.0.0"]
```

```bash
# Run as service with persistent storage
docker run -d \
  -p 8501:8501 \
  -v /path/to/storage:/data/eventlogs \
  -e ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY \
  spark-insight

# Or with S3 storage
docker run -d \
  -p 8501:8501 \
  -e STORAGE_TYPE=s3 \
  -e S3_BUCKET=my-spark-logs \
  -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
  -e ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY \
  spark-insight
```

### 3. Streamlit Cloud (Easiest for Sharing)

```bash
# 1. Push to GitHub
git push origin main

# 2. Go to share.streamlit.io
# 3. Connect your repo
# 4. Set secrets (ANTHROPIC_API_KEY, etc.)
# 5. Deploy!
```

Streamlit Cloud provides:
- Free hosting for public apps
- Automatic HTTPS
- Easy sharing via URL
- Secrets management

### 4. Kubernetes (Production)

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
        - containerPort: 8501
        env:
        - name: STORAGE_TYPE
          value: "s3"
        - name: S3_BUCKET
          valueFrom:
            configMapKeyRef:
              name: spark-insight-config
              key: s3_bucket
        - name: ANTHROPIC_API_KEY
          valueFrom:
            secretKeyRef:
              name: spark-insight-secrets
              key: anthropic_api_key
        volumeMounts:
        - name: cache
          mountPath: /tmp/cache
      volumes:
      - name: cache
        emptyDir: {}
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
    targetPort: 8501
  type: LoadBalancer
```

### 5. AWS/GCP/Azure (Managed Services)

| Platform | Service | Notes |
|----------|---------|-------|
| **AWS** | ECS Fargate / App Runner | Serverless containers |
| **GCP** | Cloud Run | Serverless, scales to zero |
| **Azure** | Container Apps | Serverless containers |

Example for Google Cloud Run:
```bash
# Build and push
gcloud builds submit --tag gcr.io/PROJECT/spark-insight

# Deploy
gcloud run deploy spark-insight \
  --image gcr.io/PROJECT/spark-insight \
  --platform managed \
  --allow-unauthenticated \
  --set-env-vars ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY
```

---

## Project Structure

```
spark-insight/
├── pyproject.toml
├── README.md
├── Dockerfile
├── src/
│   └── spark_insight/
│       ├── __init__.py
│       ├── cli.py                 # CLI commands (click)
│       ├── app.py                 # Streamlit UI (single file!)
│       ├── parser/
│       │   ├── __init__.py
│       │   ├── eventlog.py        # Event log parsing
│       │   └── models.py          # Data models
│       ├── query/
│       │   ├── __init__.py
│       │   └── engine.py          # DuckDB query engine
│       ├── storage/
│       │   ├── __init__.py
│       │   ├── local.py           # Local file storage
│       │   └── s3.py              # S3 storage
│       ├── llm/
│       │   ├── __init__.py
│       │   ├── service.py         # LLM service
│       │   └── prompts.py         # System prompts
│       ├── mcp/
│       │   ├── __init__.py
│       │   └── server.py          # MCP server
│       ├── diff/
│       │   ├── __init__.py
│       │   └── engine.py          # Diff engine
│       └── api/
│           ├── __init__.py
│           ├── app.py             # FastAPI app (for programmatic access)
│           └── routes/
│               ├── upload.py      # File upload endpoints
│               ├── application.py
│               └── analyze.py
├── tests/
│   ├── test_parser.py
│   ├── test_query.py
│   ├── test_llm.py
│   ├── test_mcp.py
│   └── fixtures/                  # Sample event logs
│       └── sample_eventlog.json
└── examples/
    └── eventlogs/                 # Example event logs for demo
```

**Note:** The entire UI is in a single `app.py` file (~300 lines). No separate frontend build process.

---

## Implementation Roadmap

### Phase 1: Core Foundation (Week 1)
- [ ] Event log parser (JSON parsing, data extraction)
- [ ] Data models (Application, Job, Stage, Task, Executor)
- [ ] DuckDB query engine (load data, pre-built queries)
- [ ] Basic CLI (`serve`, `ask`)

### Phase 2: Streamlit UI (Week 2)
- [ ] Dashboard page with metrics and charts
- [ ] Jobs/Stages/Executors views with tables
- [ ] File upload functionality
- [ ] Basic Plotly visualizations

### Phase 3: LLM Integration (Week 3)
- [ ] LLM service with Claude API
- [ ] AI chat tab in Streamlit
- [ ] Pre-built insights generation
- [ ] Suggested questions

### Phase 4: Service Mode (Week 4)
- [ ] Upload storage (local + S3)
- [ ] Multi-app management
- [ ] Docker deployment
- [ ] Environment configuration

### Phase 5: MCP Server (Week 5)
- [ ] MCP resources (application, jobs, stages, etc.)
- [ ] MCP tools (query, analyze)
- [ ] Documentation for Claude Desktop integration

### Phase 6: Diff Engine & Polish (Week 6)
- [ ] Diff comparison logic
- [ ] Diff view in Streamlit
- [ ] PyPI package
- [ ] Documentation & examples

### Estimated Total: 6 weeks

With Streamlit, the UI development is ~3x faster compared to React/Next.js.

---

## Future Enhancements

1. **Multi-app support** - Load multiple event logs, browse history
2. **S3/HDFS integration** - Read event logs directly from cloud storage
3. **Alerts & anomaly detection** - Flag unusual patterns automatically
4. **Collaboration** - Share analysis links, annotations
5. **Custom dashboards** - User-defined metrics and views
6. **Plugin system** - Extend with custom analyzers
7. **Ollama support** - Local LLM for air-gapped environments

---

## References

- [Spark Event Log Format](https://spark.apache.org/docs/latest/monitoring.html)
- [Model Context Protocol](https://modelcontextprotocol.io/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [shadcn/ui Components](https://ui.shadcn.com/)
- [Claude API Documentation](https://docs.anthropic.com/)
