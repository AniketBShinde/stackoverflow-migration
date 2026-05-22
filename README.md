# StackOverflow Migration — Autonomous Cross-Cloud Data Platform

A production-grade, autonomous cross-cloud data platform that migrates the StackOverflow archive dataset from Google Cloud Platform (GCP) to AWS, uses programmatic AI via the Model Context Protocol (MCP) to automatically design and deploy database infrastructure on the fly, and applies dbt Medallion modeling and validation testing to expose highly optimized analytical data marts.

The entire end-to-end data lifecycle is scheduled, monitored, validated, and orchestrated via Apache Airflow.

---

# 1. Executive Summary & Core Objective

Traditional enterprise data migrations suffer from significant operational friction, requiring manual schema mapping, tedious DDL script generation, and fragile pipeline synchronization.

This project completely automates those components by decoupling extraction from infrastructure initialization. By pairing the Anthropic Claude API with the Model Context Protocol (MCP) inside an Airflow DAG workflow, the system dynamically inspects raw data staging files, auto-compiles optimal ClickHouse target tables, validates transformed data quality, and triggers downstream dbt models to transform raw historical strings into high-performance analytical indices — all without any hardcoded passwords or human engineering intervention.

---

# 2. Architecture & Core Data Flow

The conceptual flow below illustrates how data routes across cloud boundaries and execution ecosystems:

```text
[Google BigQuery (GCP)]
       │
       ▼ (Orchestrated AWS Glue Managed Ingestion)
[Amazon S3 (Staging Lake)]
       ▲
       │ (Secured Token Access via IAM Role ARN)
       ▼ (Anthropic API / Claude Infrastructure Generation via MCP)
[ClickHouse Cloud (Bronze Target)]
       │
       ▼ (dbt Incremental Processing & Array Normalization)
[dbt Validation Layer]
       │
       ▼
[Final Analytics Marts (Silver & Gold Layers)]
```

---

## Technical Execution Sequence

### Cross-Cloud Extraction

Apache Airflow initiates the workflow and triggers a serverless AWS Glue job. Glue securely queries Google BigQuery, extracts the immutable archival shards, and records them safely into an Amazon S3 staging bucket as compressed Parquet files.

### Passwordless Token Authorization

Rather than using static database passwords, authorization across the S3 data lake and ClickHouse is bound dynamically to an AWS IAM Role ARN. This implements zero-trust identity policies by removing plain-text credentials from repositories and log telemetry.

### Autonomous Infrastructure Generation

The orchestration pipeline passes execution states to the Anthropic API. Operating over an MCP connection to ClickHouse Cloud, Claude reads the raw S3 Parquet metadata headers, determines data-type alignments, and programmatically compiles and executes optimal target MergeTree DDL schemas along with streaming S3Queue mechanisms.

### Continuous Ingestion

The instantiation of the S3Queue engine sets up an instant streaming ingestion loop. The millisecond new objects land inside S3, ClickHouse processes the events via native Materialized Views, pulling records into raw Bronze target landing layers without any polling overhead.

### Medallion Modeling via dbt

With data landed, Airflow commands dbt execution structures to clean, validate, and compile the analytical records into Silver and Gold layers.

---

# 3. Core Technology Stack Matrix

| Tool / Component | Architectural Layer | Operational Purpose |
| :--- | :--- | :--- |
| Google BigQuery | Source Data Layer (GCP) | Hosts the master raw StackOverflow archive dataset and serves as the immutable source database for migration initiation. |
| AWS Glue | Managed Ingestion / Extraction | Executes secure serverless cross-cloud extraction from BigQuery, landing structured objects directly into the AWS cloud ecosystem. |
| Amazon S3 | Intermediate Staging Layer | Stores extracted data as highly compressed, partitioned Parquet files, acting as an asynchronous buffer and data lake source for ClickHouse. |
| AWS IAM Role ARN | Security & Identity Management | Provides secure, passwordless authentication between ClickHouse Cloud and S3, eliminating credential leakage inside repositories. |
| Apache Airflow | Central Orchestration Engine | Coordinates the end-to-end event timeline, chaining ingestion, AI infrastructure generation, dbt transformations, and validation workflows. |
| Anthropic API (Claude) | Autonomous Engineering Layer | Acts as the programmatic virtual Data Engineer, evaluating incoming source schemas and automatically generating precise database tables. |
| Model Context Protocol (MCP) | AI-to-Database Interface | Provides a secure localized connection enabling Claude to dynamically inspect S3 metadata and self-execute commands directly within ClickHouse. |
| ClickHouse Cloud | Target Analytics Store | Serves as the central analytical warehouse engine leveraging MergeTree storage and high-speed aggregation capabilities. |
| dbt (Data Build Tool) | Transformation & Modeling | Executes SQL transformation layers using Medallion architecture patterns while also enforcing data quality validation rules. |

---

# 4. Repository Layout & Component Structure

This repository separates the scheduling orchestration layer cleanly from the downstream transformation logic, matching enterprise-grade engineering directory patterns.

```text
stackoverflow-migration/
├── dags/
│   └── stackoverflow_migration_dag.py
│
├── stackoverflow_analytics/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   │
│   └── models/
│       ├── marts/
│       │   └── fct_tag_analysis.sql
│       │
│       └── staging/
│           ├── src_stackoverflow.yml
│           ├── stg_posts.sql
│           └── stg_posts.yml
│
├── Dockerfile
├── docker-compose.yaml
└── requirements.txt
```

---

# 5. Medallion Transformation Details (dbt)

Data loaded into ClickHouse Cloud is processed through a strict Medallion architecture inside the `stackoverflow_analytics` module.

---

## Bronze Layer (Raw Landing Zone)

Tracks raw immutable events streamed directly from Amazon S3 through the ClickHouse S3Queue ingestion engine.

The Bronze layer preserves source-level fidelity and acts as the canonical historical ingestion layer.

---

## Silver Layer (Normalized Transformation Layer)

Cleans and normalizes structural formatting anomalies.

One major transformation involves converting raw StackOverflow tag strings such as:

```text
<python><aws><pandas>
```

into native ClickHouse analytical structures optimized for aggregation and filtering.

This transformation layer powers:

- efficient tag analytics,
- fast filtering,
- array-based processing,
- and optimized query execution.

Primary staging model:

```text
stg_posts
```

---

## Gold Layer (Analytical Mart Layer)

The Gold layer exposes high-value analytical marts optimized for reporting and aggregation workloads.

Primary mart:

```text
fct_tag_analysis
```

This layer aggregates:

- technology adoption trends,
- post activity metrics,
- view accumulation,
- scoring behavior,
- and temporal analytics.

---

## Incremental Optimization

The dbt transformation layer embeds a rolling 5-day incremental lookback window to protect against delayed arriving records and avoid expensive full historical reprocessing scans.

This improves:

- execution efficiency,
- compute optimization,
- and orchestration scalability.

---

## Data Quality Validation (dbt Tests)

To improve transformation reliability, the platform integrates dbt tests directly into the orchestration workflow.

The `stg_posts.yml` model configuration defines automated validation constraints on critical analytical fields:

- `post_id` must be unique
- `post_id` must be non-null

These validation gates help detect:

- duplicate ingestion events,
- malformed transformations,
- incomplete records,
- and downstream referential inconsistencies

before invalid data propagates into Gold analytical marts.

The validation logic is executed automatically through Apache Airflow as part of the DAG execution chain:

```python
run_dbt_tests = BashOperator(
    task_id='run_dbt_tests',
    bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --select staging',
)
```

The final orchestration sequence becomes:

```text
Glue Extraction
      ↓
Claude Infrastructure Generation
      ↓
S3Queue Ingestion Completion Gate
      ↓
dbt Staging Models
      ↓
dbt Data Quality Tests
      ↓
Gold Mart Materialization
```

This establishes a controlled reliability boundary between ingestion and downstream analytics while aligning the platform more closely with production-grade data engineering practices.

---

# 6. Autonomous Infrastructure Automation via Claude + MCP

One of the most experimental and powerful components of the platform is the autonomous infrastructure generation layer.

Instead of manually writing ClickHouse DDL statements, the orchestration pipeline delegates infrastructure initialization to Anthropic Claude through MCP.

The workflow dynamically performs the following:

1. Inspects staged Parquet metadata inside Amazon S3
2. Infers schema mappings
3. Generates ClickHouse MergeTree tables
4. Creates S3Queue ingestion engines
5. Deploys Materialized Views
6. Starts continuous ingestion automatically

This transforms Claude from a passive assistant into an active infrastructure automation layer operating inside controlled orchestration boundaries.

---

# 7. Continuous Streaming Ingestion via ClickHouse

ClickHouse continuously monitors incoming Parquet objects through the S3Queue engine.

As new files land inside Amazon S3:

- ClickHouse automatically detects objects,
- Materialized Views trigger ingestion,
- and records flow directly into MergeTree analytical storage.

This eliminates the need for:

- polling workers,
- custom ingestion microservices,
- Kafka clusters,
- or external stream processors.

The ingestion design remains lightweight while still supporting near-real-time streaming behavior.

While ClickHouse handles ingestion autonomously, the orchestration layer includes an explicit completion gate before downstream transformations begin. A polling task observes the row count of the Bronze landing table and only releases control to dbt once the count has remained stable across consecutive checks, signaling that S3Queue has finished draining all available Parquet objects. This prevents the well-known race condition where transformation models execute against a partially loaded source table.

---

# 8. Security Architecture

One of the major architectural priorities of the platform was reducing static credential exposure.

Instead of embedding cloud credentials directly into ingestion logic, the system uses AWS IAM Role ARNs for controlled authorization between S3 and ClickHouse Cloud.

Benefits include:

- passwordless authentication,
- reduced credential leakage risk,
- simplified access rotation,
- and alignment with zero-trust infrastructure principles.

This removes the need for hardcoded secrets inside:

- repositories,
- DAG definitions,
- notebooks,
- or orchestration logs.

---

# 9. Analytical Workloads

After Gold marts are materialized, ClickHouse supports several high-speed analytical workloads.

Examples include:

- technology trend aggregation,
- rolling window analysis,
- temporal post analytics,
- grouped OLAP aggregation,
- and comparative growth analysis.

Despite scanning millions of transformed records, analytical queries consistently execute in sub-second latency windows due to ClickHouse's columnar architecture and MergeTree indexing strategy.

---

# 10. Dataset Scale & Performance

| Metric | Value |
| :--- | :--- |
| Source Dataset | StackOverflow Archive |
| Total Records Processed | ~23 Million Rows |
| Parquet Files Generated | 76 Files |
| Average Parquet Size | 230–260 MB |
| Transformation Layer | dbt Medallion Models |
| Analytical Mart | `fct_tag_analysis` |
| Query Latency | Sub-second aggregations |
| Target Engine | ClickHouse Cloud |

---

# 11. How to Run Locally

To initialize the orchestration workspace locally:

## Step 1 — Clone Repository

```bash
git clone https://github.com/AniketBShinde/stackoverflow-migration.git
```

---

## Step 2 — Navigate Into Project

```bash
cd stackoverflow-migration
```

---

## Step 3 — Start Services

```bash
docker compose up -d --build
```

---

## Step 4 — Verify Containers

```bash
docker compose ps
```
