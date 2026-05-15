# Design Document: Next-Generation Spark History Server (Drop-In Replacement)

## 1. Executive Summary & Objectives

Apache Spark History Server (SHS) is the standard tool for investigating completed Spark applications. However, its architecture and UI have lagged behind modern engineering requirements. It suffers from high memory consumption when parsing large event logs, lacks intuitive comparative diagnostics, and does not provide automated root-cause analysis.

This document outlines the design for a **Next-Generation Spark History Server** that serves as a seamless drop-in replacement while introducing a modern UI, robust side-by-side comparison capabilities (App, Stage, and SQL levels), and an LLM-powered natural language troubleshooting engine.

### 1.1 Key Objectives
* **Drop-In Replacement:** Completely compatible with existing Spark application configurations (`spark.eventLog.enabled=true`, `spark.eventLog.dir`). No changes required on the Spark driver/executor side.
* **Serverless & Stateless Cloud Architecture:** Fully decoupled, event-driven, and stateless. Deploys on serverless cloud infrastructure (Cloud Run, AWS Lambda/Fargate, BigQuery/Athena) that scales to zero when idle.
* **Modern User Interface:** Fast, responsive analytical dashboard built on Python-native frameworks (Streamlit), supporting dark mode and intuitive workflows.
* **Side-by-Side Comparison:** Dedicated UX and backend engine to compare multiple applications, stages, or SQL executions side-by-side to identify performance regressions and configuration diffs.
* **Embedded & Hybrid AI Troubleshooting:** Combines an embedded, quantized Small Language Model (SLM) for instant, zero-latency local inference with optional routing to cloud LLMs for complex architectural Q&A.

---

## 2. Serverless & Stateless Cloud Architecture

To eliminate infrastructure management and minimize cloud compute costs, the system is designed as a fully stateless, event-driven architecture. There are no long-running daemon servers or persistent local disks.

```
================================================================================
                        SERVERLESS CLOUD ARCHITECTURE
================================================================================

[ Spark Environment ]
  Spark Apps ---> (Writes Event Logs) ---> [ Cloud Object Storage: S3 / GCS ]
                                                     |
                                          (Storage Event Trigger)
                                                     |
                                                     v
[ Serverless Ingestion ]                 [ Cloud Functions / AWS Lambda ]
                                                     |
                                   (Parses JSON Logs to Partitioned Parquet)
                                                     |
                                                     v
[ Structured Cloud Lake ]                [ Cloud Storage: /parquet/app_id=... ]
                                                     ^
                                                     |  (HTTP Range Requests)
                                                     v
[ Serverless Serving & UI ]              [ Cloud Run / AWS App Runner (Stateless) ]
                                           |-- Embedded DuckDB over S3/GCS
                                           |-- Streamlit UI / FastAPI Backend
                                           |-- Embedded SLM (Llama 3.2 3B GGUF)
                                           |
                                           +<--> [ Serverless LLM: Vertex / Bedrock ]
```

### 2.1 Language & Core Technology Stack
* **Backend & UI Engine:** **Python 3.11+**. Python is chosen because it is the lingua franca of data engineering and AI/ML. It allows seamless integration with LLM frameworks (LangChain, LlamaIndex) while remaining highly accessible to Spark developers.
* **Serverless Query Engine:** **DuckDB over S3/GCS** (embedded inside stateless containers) or **Serverless Cloud Warehouses** (Google BigQuery, AWS Athena). DuckDB can query remote Parquet files directly over HTTP range requests without downloading the entire file, enabling lightning-fast analytical queries inside stateless containers.

### 2.2 Serverless Ingestion Layer (Event-Driven)
* **Event Trigger:** Instead of a long-running polling daemon, Cloud Storage event notifications (S3 Event Notifications / GCS Pub/Sub triggers) fire whenever a Spark application completes or rolls an event log file.
* **Serverless Parser (AWS Lambda / Google Cloud Function):** The event triggers a serverless function that spins up instantly, parses the raw Spark JSON event log, and transforms it into highly compressed, partitioned Parquet files stored back in S3/GCS (`s3://bucket/spark-history-lake/app_id=123/`).

### 2.3 Stateless Serving Layer (Cloud Run / Fargate)
* **Stateless Containers:** The Streamlit UI and FastAPI endpoints are packaged as stateless Docker containers deployed on serverless compute (Google Cloud Run, AWS Fargate, Azure Container Apps).
* **Scale-to-Zero:** When no developers are actively viewing dashboards, the containers automatically scale down to zero, resulting in zero compute cost.
* **Zero Shared State:** Any container instance can handle any user request. User sessions and comparison navigation state are managed entirely within the client browser or lightweight serverless Redis (ElastiCache / Memorystore).

---

## 3. Streamlit as the Frontend Architecture

Using **Streamlit** as the primary UI framework is an exceptional choice for a Spark History Server replacement. It provides an ideal balance between developer velocity (zero frontend experience required) and rich data visualization.

```
================================================================================
                          STREAMLIT FRONTEND FLOW
================================================================================

[ Data / Backend Engineer ]
  Write Pure Python Scripts
        |
        +---> [@st.cache_data] ----> Instant In-Memory Query Results
        |
        +---> [st.plotly_chart] ---> Rich Analytical Charts & ECharts Widgets
        |
        +---> [st.chat_message] ---> Native Streaming AI Chat UI
```

### 3.1 Advantages of Streamlit for this Use Case
1. **Zero Frontend Knowledge Required:** Data engineers who write PySpark can instantly build or modify UIs using familiar Python syntax.
2. **First-Class AI/LLM Chat Components:** Streamlit includes native components (`st.chat_input`, `st.chat_message`) that support streaming markdown output, making the "Ask Spark AI" troubleshooting panel trivial to implement.
3. **Rich Analytical Ecosystem:** Native support for rendering massive Pandas/Polars dataframes, Plotly charts, Altair visualizations, and Apache ECharts (via `streamlit-echarts`).
4. **Rapid Prototyping & Maintenance:** Adding a new comparison metric or debugging view takes minutes instead of requiring full frontend sprint cycles.

### 3.2 Overcoming Streamlit Architectural Challenges in Serverless

Streamlit operates on a **script re-run model**. To ensure enterprise-grade performance when deployed on serverless containers (Cloud Run/Fargate), we implement three critical architectural patterns:

#### 3.2.1 Remote Parquet Caching (`@st.cache_data`)
To avoid re-fetching remote Parquet files from S3/GCS on every UI interaction, queries executed by DuckDB are cached in container memory.

```python
import streamlit as st
import duckdb

@st.cache_data(ttl=3600)
def get_stage_metrics(app_id: str, stage_id: int):
    # DuckDB reads remote Parquet via HTTP range requests; results cached in container memory
    return duckdb.query(f"""
        SELECT task_id, duration, spill 
        FROM 's3://spark-lake/parquet/app_id={app_id}/stages.parquet' 
        WHERE stage_id = {stage_id}
    """).df()
```

#### 3.2.2 Session State (`st.session_state`) for Stateless Scaling
Because serverless containers can spin up and down, any persistent user session data (such as selected comparison apps) is stored in client-side cookies or serverless Redis, synchronized with `st.session_state`.

#### 3.2.3 Custom Bi-Directional Components for SQL DAG Diffing
While Streamlit excels at grids and charts, rendering an interactive, color-coded physical execution DAG requires wrapping a lightweight React DAG viewer or Apache ECharts tree graph into a custom component (`streamlit-echarts`).

---

## 4. Core Functional Modules & UI/UX Design

### 4.1 Modern UI/UX Foundation
* **Design System:** Clean, high-contrast Streamlit dashboard, utilizing `st.set_page_config(layout="wide")`, supporting Dark/Light modes, collapsible sidebars (`st.sidebar`), and multi-page navigation.
* **Application Overview Page:** Real-time filtering by user, status (Failed/Succeeded/Running), duration, and tag using `st.dataframe` with column sorting and filtering. Includes a flame graph of execution time and a resource utilization heatmap.

### 4.2 Side-by-Side Comparison Engine

The ability to compare executions is critical for regression testing, tuning, and debugging.

#### 4.2.1 App-Level Comparison
* **Configuration Diffing:** Highlights modified `spark.*` properties, executor counts, memory allocations, and JVM flags between App A and App B using `st.columns`.
* **Timeline & Resource Alignment:** Overlays execution timelines, CPU utilization, and memory consumption charts on a normalized time axis (0% to 100% completion).
* **Summary Table:** Diff of total duration, total shuffle read/write, total tasks, and cluster cost.

```
+-----------------------------------------------------------------------+
| Comparing: [App ID: app-20260512-1001] vs [App ID: app-20260514-2004] |
+-----------------------------------------------------------------------+
| Metric               | App A (Baseline)   | App B (Candidate)  | Diff |
+----------------------+--------------------+--------------------+------+
| Total Duration       | 45m 12s            | 58m 30s            | +29% |
| spark.executor.memory| 16g                | 8g                 | DIFF |
| Shuffle Read         | 1.2 TB             | 1.8 TB             | +50% |
| Spill (Memory/Disk)  | 0 GB               | 45 GB              | DIFF |
+-----------------------------------------------------------------------+
```

#### 4.2.2 Stage-Level Comparison
* **Task Skew Analysis:** Compares task duration distribution (box-and-whisker plots) between two stages.
* **Shuffle & GC Bottlenecks:** Side-by-side breakdown of time spent in Scheduler Delay, Task Deserialization, Execution, Shuffle Write/Read, and JVM GC.
* **Straggler Identification:** Pinpoints specific executors or hosts causing slowdowns across compared stages.

#### 4.2.3 SQL & DataFrame Plan Comparison
* **Visual DAG Diff:** Renders physical execution plans side-by-side. Nodes are color-coded:
  * <span style="color:green">Green:</span> Identical node structure & similar performance.
  * <span style="color:orange">Yellow:</span> Identical node structure but significant metric divergence.
  * <span style="color:red">Red:</span> Structural divergence (e.g., SortMergeJoin vs BroadcastHashJoin).
* **Metric Overlay:** Clicking a node in the diff view displays a side-by-side comparison of metrics.

---

## 5. Embedded & Hybrid AI Troubleshooting Architecture

To provide instantaneous, zero-latency error summaries while avoiding the cold-start penalties of massive LLMs in serverless environments, we establish a **Hybrid AI Architecture** combining embedded local models with cloud LLM gateways.

```
================================================================================
                    HYBRID AI TROUBLESHOOTING WORKFLOW
================================================================================

 [User]                 [Streamlit UI]           [LLM Router]          [Embedded Quantized SLM]          [Cloud LLM Gateway]
   |                          |                       |                           |                               |
   |-- 1. Submits prompt ---->|                       |                           |                               |
   |   in st.chat_input()     |-- 2. Route Query ---->|                           |                               |
   |                          |                       |                           |                               |
   |                          |                       |-- [IF: Quick Error / OOM] |                               |
   |                          |                       |   3. Local Prompt ------->|                               |
   |                          |                       |                           |<-- 4. Instant Local Stream ---|
   |                          |                       |                           |                               |
   |                          |                       |-- [IF: Complex Tuning]    |                               |
   |                          |                       |   5. Cloud Prompt --------------------------------------->|
   |                          |                       |                                                           |<-- 6. Deep Stream ---
   |                          |<-- 7. Stream Output --|
   |<-- 8. Display UI Card ---|
```

### 5.1 Approach: Embedded Quantized SLM (Llama 3.2 3B / Phi-3)
For lightning-fast root cause analysis of common errors (e.g. Out of Memory, Task Skew, ClassNotFound), we embed a **Small Language Model (SLM)** directly inside the container image.

```
================================================================================
                          EMBEDDED SLM RUNTIME
================================================================================

+------------------------------------------------------------------------------+
| Serverless Container Image                                                   |
|                                                                              |
|  [ FastAPI / Streamlit Runtime ]                                             |
|               |                                                              |
|               v                                                              |
|  [ Llama.cpp Engine: CPU AVX2 / ARM NEON ]                                   |
|               |                                                              |
|               v                                                              |
|  [ Weights: Llama-3.2-3B-Instruct.Q4_K_M.gguf < 2GB ]                        |
+------------------------------------------------------------------------------+
```

* **Quantization & Footprint:** We use GGUF format models (e.g., `Llama-3.2-3B-Instruct.Q4_K_M.gguf`), which consume under 2.2 GB of disk space. This keeps the serverless Docker container image small and fast to spin up.
* **Local CPU Inference (`llama-cpp-python`):** Highly optimized C++ bindings allow the model to execute inference directly on serverless container CPUs (utilizing AVX2/AVX-512 or ARM NEON vector instructions) at 30–50 tokens per second without requiring expensive GPUs.
* **Cold Start Elimination:** Because the weights are baked into the container image, loading the model into container RAM takes less than 1 second during cold spin-up.

### 5.2 Hybrid Routing Strategy
To balance speed, cost, and intelligence, the backend implements an intelligent routing layer:

```python
import streamlit as st
from llama_cpp import Llama
import google.generativeai as genai

# Initialize Embedded Model once at container startup
llm_local = Llama(model_path="./models/llama-3.2-3b.gguf", n_ctx=4096)

def troubleshoot_query(prompt: str, is_complex: bool):
    if not is_complex:
        # Fast Local Inference for quick error summaries (Zero latency, zero cost)
        for chunk in llm_local.create_chat_completion([{"role": "user", "content": prompt}], stream=True):
            yield chunk['choices'][0]['delta'].get('content', '')
    else:
        # Route to Frontier Cloud Model for deep architectural tuning Q&A
        model = genai.GenerativeModel('gemini-1.5-pro')
        for chunk in model.generate_content(prompt, stream=True):
            yield chunk.text
```

### 5.3 Prompt & Context Engineering (RAG Architecture)
To ensure highly accurate, hallucination-free diagnostics, the backend synthesizes precise context before querying the LLM:
1. **System Prompt:** Instructs the LLM to act as an expert Apache Spark distributed systems architect.
2. **Structured Execution Context:** Injects JSON summaries of cluster topology, Spark configs, stage/task metrics, and data skew statistics.
3. **Log & Trace Context:** Injects the exact exception stack traces and relevant executor error logs.
4. **Knowledge Base Ingestion:** Incorporates Spark documentation, performance tuning guides, and internal company-specific runbooks.

---

## 6. Backend & Data Model Specification

### 6.1 Storage Schemas (Cloud Parquet Lake Format)

#### Parquet Schema: `spark_app_metrics.parquet`
```sql
CREATE TABLE spark_app_metrics (
    app_id VARCHAR,
    app_name VARCHAR,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_millis BIGINT,
    status VARCHAR, -- SUCCEEDED, FAILED, KILLED
    total_tasks INT,
    failed_tasks INT,
    total_shuffle_read_bytes BIGINT,
    total_shuffle_write_bytes BIGINT,
    total_spill_bytes BIGINT
);
```

#### Parquet Schema: `spark_stage_metrics.parquet`
```sql
CREATE TABLE spark_stage_metrics (
    app_id VARCHAR,
    stage_id INT,
    attempt_id INT,
    stage_name VARCHAR,
    num_tasks INT,
    executor_run_time_millis BIGINT,
    gc_time_millis BIGINT,
    shuffle_read_bytes BIGINT,
    shuffle_write_bytes BIGINT,
    memory_bytes_spilled BIGINT,
    disk_bytes_spilled BIGINT,
    peak_execution_memory_bytes BIGINT
);
```

### 6.2 Key REST / Internal Functions

#### 1. Compare Applications
`get_app_comparison(app_id_a: str, app_id_b: str) -> DataFrame`
* **Response:** Returns a structured diff of configurations, aggregated stage metrics, and execution timelines.

#### 2. Compare SQL Plans
`get_sql_plan_comparison(app_a: str, sql_a: int, app_b: str, sql_b: int) -> Dict`
* **Response:** Returns aligned DAG representations with diff tags on modified operators and delta metrics.

#### 3. LLM Troubleshooting Streaming
`stream_troubleshooting_response(app_id: str, query: str) -> Generator`
* **Response:** Yields streaming markdown text for `st.write_stream()`.

---

## 7. Deployment & Operational Model

```
================================================================================
                     CLOUD DEPLOYMENT & SCALING MODEL
================================================================================

[ AWS / GCP / Azure Cloud ]
  |
  |-- Ingestion:  AWS Lambda / Google Cloud Functions (Event-driven scale)
  |-- Storage:    Amazon S3 / Google Cloud Storage (Parquet data lake)
  |-- Compute:    AWS App Runner / Google Cloud Run (Stateless containers)
  |-- AI Runtime: Embedded llama.cpp (Instant CPU inference) + Cloud LLMs
  +-- Cache:      Serverless Redis / Upstash (Session synchronization)
```

* **Fully Serverless & Scale-to-Zero:** The entire infrastructure scales dynamically based on developer usage. Cloud Run / Fargate instances scale down to zero when idle, eliminating baseline compute costs.
* **Zero Local Storage Dependencies:** All historical metrics and plans are stored in highly compressed Parquet format on S3/GCS. Embedded DuckDB queries these files directly over HTTP range requests.
* **Embedded, Zero-Latency AI:** Baking a quantized 3B SLM into the container image guarantees instant error summaries without external API costs or massive container cold-start penalties.
