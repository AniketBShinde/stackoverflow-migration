# StackOverflow Migration — Autonomous Cloud Data Platform

A production-grade, autonomous cross-cloud data platform that migrates the StackOverflow archive dataset from Google Cloud Platform (GCP) to AWS, uses programmatic AI via the Model Context Protocol (MCP) to automatically design and deploy database infrastructure on the fly, and applies dbt Medallion modeling to expose highly optimized analytical data marts.

The entire end-to-end data lifecycle is scheduled, monitored, and orchestrated via Apache Airflow.

---

## 1. Executive Summary & Core Objective

Traditional enterprise data migrations suffer from significant operational friction, requiring manual schema mapping, tedious DDL script generation, and fragile pipeline synchronization.

This project completely automates those components by decoupling extraction from infrastructure initialization. By pairing the Anthropic Claude API with the Model Context Protocol (MCP) inside an Airflow DAG workflow, the system dynamically inspects raw data staging files, auto-compiles optimal ClickHouse target tables, and triggers downstream dbt models to transform raw historical strings into high-performance analytical indices — all without any hardcoded passwords or human engineering intervention.

---

## 2. Architecture & Core Data Flow

The conceptual flow below illustrates how data routes across cloud boundaries and execution ecosystems:

[Google BigQuery (GCP)] 
       │
       ▼ (Orchestrated AWS Glue Managed Ingestion)
[Amazon S3 (Staging Lake)] <─── (Secured Token Access via IAM Role ARN)
       │
       ▼ (Anthropic API / Claude Infrastructure Generation via MCP)
[ClickHouse Cloud (Bronze Target)]
       │
       ▼ (dbt Incremental Processing & Array Normalization)
[Final Analytics Marts (Silver & Gold Layers)]

### The Technical Execution Sequence

* Cross-Cloud Extraction — Apache Airflow initiates the workflow and triggers a serverless AWS Glue job. Glue securely queries Google BigQuery, extracts the immutable archival shards, and records them safely into an Amazon S3 staging bucket as compressed Parquet files.
* Passwordless Token Authorization — Rather than using static database passwords, authorization across the S3 data lake and ClickHouse is bound dynamically to an AWS IAM Role ARN. This implements zero-trust identity policies by removing plain-text credentials from repositories and log telemetry.
* Autonomous Infrastructure Generation — The orchestration pipeline passes execution states to the Anthropic API. Operating over an MCP connection to ClickHouse Cloud, Claude reads the raw S3 Parquet metadata headers, determines data-type alignments, and programmatically compiles and executes optimal target MergeTree DDL schemas along with streaming S3Queue mechanisms.
* Continuous Ingestion — The instantiation of the S3Queue engine sets up an instant streaming ingestion loop. The millisecond new objects land inside S3, ClickHouse processes the events via native Materialized Views, pulling records into raw Bronze target landing layers without any polling overhead.
* Medallion Modeling via dbt — With data landed, Airflow commands dbt execution structures to clean and compile the analytical records into Silver and Gold layers.

---

## 3. Core Technology Stack Matrix

| Tool / Component | Architectural Layer | Operational Purpose |
| :--- | :--- | :--- |
| Google BigQuery | Source Data Layer (GCP) | Hosts the master raw StackOverflow archive dataset and serves as the immutable source database for the migration initiation. |
| AWS Glue | Managed Ingestion / Extraction | Executes secure serverless cross-cloud extraction from BigQuery, landing structured objects directly into the AWS cloud ecosystem. |
| Amazon S3 | Intermediate Staging Layer | Stores extracted data as highly compressed, partitioned Parquet files, acting as an asynchronous buffer and data lake source for ClickHouse. |
| AWS IAM Role ARN | Security & Identity Management | Provides secure, passwordless authentication between ClickHouse Cloud and S3, eliminating credential leakage inside repositories. |
| Apache Airflow | Central Orchestration Engine | Coordinates the end-to-end event timeline, programmatically chaining ingestion, AI infrastructure generation, and dbt transformation DAGs. |
| Anthropic API (Claude) | Autonomous Engineering Layer | Acts as the programmatic virtual Data Engineer, evaluating incoming source schemas and automatically generating precise database tables. |
| Model Context Protocol (MCP) | AI-to-Database Interface | Provides a secure, localized connection enabling Claude to dynamically inspect S3 metadata and self-execute commands directly within ClickHouse. |
| ClickHouse Cloud | Target Analytics Store | Serves as the central data warehouse engine, leveraging specialized MergeTree storage engines to resolve sub-second aggregations. |
| dbt (Data Build Tool) | Transformation & Modeling | Executes localized SQL compilation to structural layers (Bronze, Silver, Gold Medallion schema patterns) to refine and expose analytical indices. |

---

## 4. Repository Layout & Component Structure

This repository separates the scheduling orchestration layer cleanly from the downstream model logic, matching standard enterprise engineering directory patterns:

* stackoverflow-migration/
* ├── dags/
* │   └── stackoverflow_migration_dag.py (Main Airflow DAG defining task groups & AI hooks)
* ├── stackoverflow_analytics/ (Isolated dbt Transformation Core)
* │   ├── dbt_project.yml (dbt project configuration, variables, and models)
* │   ├── profiles.yml (ClickHouse warehouse connection settings)
* │   └── models/
* │       ├── marts/
* │       │   └── fct_tag_analysis.sql (Gold Layer: High-value technology matrix metrics)
* │       └── staging/
* │           ├── src_stackoverflow.yml (Source freshness declarations)
* │           ├── stg_posts.sql (Silver Layer: Heavy text-parsing and tag-to-array mapping)
* │           └── stg_posts.yml (Data quality testing constraints and validation keys)
* ├── Dockerfile (Container build setup for isolated execution)
* ├── docker-compose.yaml (Local multi-service environment setup configuration)
* └── requirements.txt (Explicit Python package dependencies)

---

## 5. Medallion Transformation Details (dbt)

[cite_start]Data loaded into ClickHouse Cloud is processed through a strict Medallion data structure architecture within the stackoverflow_analytics module[cite: 53, 54]:

* [cite_start]**Bronze (Raw Landing Layer)** — Tracks raw, immutable events streamed instantly from staging storage folders through the S3Queue interface engine[cite: 54].
* [cite_start]**Silver (Normalized Data Layer)** — Cleans and normalizes formatting anomalies[cite: 54]. [cite_start]Specifically, loose concatenated string formats (e.g., tags stored as raw text strings like <python><aws>) are parsed directly into indexable, native ClickHouse arrays for lightning-fast lookups[cite: 55].
* [cite_start]**Gold (Analytical Marts Layer)** — Creates the high-value analytics marts, aggregating community technology adoption matrices and reputation indexing ready to be consumed immediately by downstream dashboard engines[cite: 55].
* [cite_start]**Incremental Optimization** — Embeds a 7-day lookback buffer window to effortlessly protect against backfilled out-of-order records while saving computing fees by bypassing full historic table re-scans[cite: 55].

---

## 6. How to Run Locally

To spin up a local instance of this orchestration workspace for debugging or walkthrough verification, initialize the core application configuration files via terminal:

Step 1: Clone the repository
git clone https://github.com/AniketBShinde/stackoverflow-migration.git

Step 2: Navigate to directory
cd stackoverflow-migration

Step 3: Build and start services in background mode
docker-compose up -d --build

Step 4: Verify all pipeline containers are running cleanly
docker compose ps
