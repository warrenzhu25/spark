# Spark Insight: Next-Generation Spark History Server

## Overview

Spark Insight is a modern, AI-powered tool for analyzing Apache Spark application history. It provides a beautiful UI, natural language querying via LLM integration, MCP server capabilities, and can run in serverless mode with a single event log.

### Goals

1. **Modern Experience** - Beautiful, responsive UI with interactive visualizations
2. **AI-Powered Analysis** - Natural language queries to understand failures, performance issues
3. **MCP Integration** - Expose Spark data to LLM tools (Claude, etc.)
4. **Serverless Mode** - Zero-config analysis of individual event logs
5. **Comparative Analysis** - Side-by-side diff of application runs

### Non-Goals

- Replace the official Spark History Server in production clusters
- Real-time streaming of running applications (focus on completed apps)
- Modify or write to Spark clusters

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Web UI (Next.js)                               │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────────────────┐│
│  │  Dashboard  │ │  App Detail │ │  Diff View  │ │  AI Chat Interface      ││
│  └─────────────┘ └─────────────┘ └─────────────┘ └─────────────────────────┘│
├─────────────────────────────────────────────────────────────────────────────┤
│                           API Gateway (FastAPI)                             │
├───────────────┬───────────────┬───────────────┬─────────────────────────────┤
│  Event Log    │    Query      │     LLM       │         MCP                 │
│  Service      │    Engine     │   Service     │       Server                │
├───────────────┴───────────────┴───────────────┴─────────────────────────────┤
│                           Data Layer                                        │
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌──────────────────────┐ │
│  │  Event Log Parser   │  │  SQLite/DuckDB      │  │  Vector Store        │ │
│  │  (Spark JSON)       │  │  (Structured Data)  │  │  (Embeddings)        │ │
│  └─────────────────────┘  └─────────────────────┘  └──────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Layer | Technology | Rationale |
|-------|------------|-----------|
| **Frontend** | Next.js 14 + React | Server components, great DX, easy deployment |
| **UI Components** | shadcn/ui + Tailwind | Beautiful, accessible, customizable |
| **Charts** | Recharts or Apache ECharts | Interactive, performant visualizations |
| **Backend** | FastAPI (Python) | Async, fast, great for LLM integration |
| **Database** | DuckDB (embedded) | Analytical queries, zero-config, fast |
| **Vector Store** | ChromaDB (embedded) | Local embeddings for semantic search |
| **LLM** | Claude API / Ollama | Cloud or local LLM options |
| **MCP** | mcp-python SDK | Official MCP implementation |

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

### 6. Serverless Mode

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

## Web UI Design

### Pages

#### 1. Dashboard
- Application summary cards (duration, jobs, stages, tasks)
- Timeline visualization of jobs/stages
- Resource utilization chart (executors over time)
- Quick insights panel (AI-generated summary)

#### 2. Jobs View
- Job list with status, duration, stages
- Gantt chart of job execution
- Click to drill down to stages

#### 3. Stages View
- Stage list with metrics (duration, shuffle, I/O)
- Task distribution histogram
- Skew detection highlighting
- Click to see task details

#### 4. Executors View
- Executor timeline (add/remove events)
- Resource usage per executor
- GC time analysis
- Failure reasons breakdown

#### 5. Environment View
- Spark configuration
- System properties
- Classpath entries

#### 6. AI Chat Interface
- Natural language input
- Streaming responses
- Context-aware suggestions
- Export analysis to markdown

#### 7. Diff View
- Side-by-side application comparison
- Metric diff highlighting (green/red for better/worse)
- Stage-by-stage comparison
- Configuration diff

### UI Components

```tsx
// Example: AI Chat Component
export function AIChat({ queryEngine }: { queryEngine: QueryEngine }) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);

  const suggestions = [
    "What caused the most task failures?",
    "Why were executors removed?",
    "Which stages have data skew?",
    "How can I improve shuffle performance?",
  ];

  const handleSubmit = async () => {
    if (!input.trim()) return;

    setMessages(prev => [...prev, { role: 'user', content: input }]);
    setIsLoading(true);

    const response = await fetch('/api/analyze', {
      method: 'POST',
      body: JSON.stringify({ question: input }),
    });

    const result = await response.json();
    setMessages(prev => [...prev, { role: 'assistant', content: result.answer }]);
    setIsLoading(false);
    setInput('');
  };

  return (
    <div className="flex flex-col h-full">
      <div className="flex-1 overflow-y-auto p-4 space-y-4">
        {messages.map((msg, i) => (
          <ChatMessage key={i} message={msg} />
        ))}
        {isLoading && <LoadingIndicator />}
      </div>

      <div className="border-t p-4">
        <div className="flex gap-2 mb-2">
          {suggestions.map((s, i) => (
            <Button key={i} variant="outline" size="sm" onClick={() => setInput(s)}>
              {s}
            </Button>
          ))}
        </div>
        <div className="flex gap-2">
          <Input
            value={input}
            onChange={e => setInput(e.target.value)}
            placeholder="Ask about your Spark application..."
            onKeyDown={e => e.key === 'Enter' && handleSubmit()}
          />
          <Button onClick={handleSubmit}>Send</Button>
        </div>
      </div>
    </div>
  );
}
```

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

### 1. Local Binary (Recommended for serverless mode)

```bash
# Install via pip
pip install spark-insight

# Or download binary
curl -L https://github.com/user/spark-insight/releases/latest/download/spark-insight-$(uname -s)-$(uname -m) -o spark-insight
chmod +x spark-insight
```

### 2. Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY . .
RUN pip install -e .

EXPOSE 8080
ENTRYPOINT ["spark-insight"]
```

```bash
# Run with event log mounted
docker run -p 8080:8080 -v /path/to/logs:/logs spark-insight serve /logs/eventlog
```

### 3. Cloud Functions (for MCP)

Deploy as a serverless function that accepts event logs via URL/S3 path.

---

## Project Structure

```
spark-insight/
├── pyproject.toml
├── README.md
├── src/
│   └── spark_insight/
│       ├── __init__.py
│       ├── cli.py                 # CLI commands
│       ├── parser/
│       │   ├── __init__.py
│       │   ├── eventlog.py        # Event log parsing
│       │   └── models.py          # Data models
│       ├── query/
│       │   ├── __init__.py
│       │   └── engine.py          # DuckDB query engine
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
│           ├── app.py             # FastAPI app
│           └── routes/
│               ├── application.py
│               ├── jobs.py
│               ├── stages.py
│               ├── executors.py
│               ├── analyze.py
│               └── diff.py
├── web/                           # Next.js frontend
│   ├── package.json
│   ├── app/
│   │   ├── page.tsx               # Dashboard
│   │   ├── jobs/
│   │   ├── stages/
│   │   ├── executors/
│   │   ├── environment/
│   │   ├── chat/
│   │   └── diff/
│   └── components/
│       ├── ui/                    # shadcn components
│       ├── charts/
│       └── chat/
└── tests/
    ├── test_parser.py
    ├── test_query.py
    ├── test_llm.py
    └── test_mcp.py
```

---

## Implementation Roadmap

### Phase 1: Core Foundation (Week 1-2)
- [ ] Event log parser
- [ ] Data models
- [ ] DuckDB query engine
- [ ] Basic CLI (`serve`, `ask`)

### Phase 2: Web UI (Week 3-4)
- [ ] Next.js setup with shadcn/ui
- [ ] Dashboard page
- [ ] Jobs/Stages/Executors views
- [ ] Basic charts

### Phase 3: LLM Integration (Week 5)
- [ ] LLM service with Claude API
- [ ] AI chat interface
- [ ] Pre-built insights
- [ ] Streaming responses

### Phase 4: MCP Server (Week 6)
- [ ] MCP resources (application, jobs, stages, etc.)
- [ ] MCP tools (query, analyze)
- [ ] Documentation for Claude Desktop integration

### Phase 5: Diff Engine (Week 7)
- [ ] Diff comparison logic
- [ ] Diff UI view
- [ ] Diff CLI command
- [ ] Report generation

### Phase 6: Polish & Release (Week 8)
- [ ] Docker image
- [ ] PyPI package
- [ ] Documentation
- [ ] Example event logs

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
