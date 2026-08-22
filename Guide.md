# Local Open Data Lakehouse: Comprehensive Guide

Welcome to your Local Open Data Lakehouse! This document provides a deep dive into the architecture, the components used, how data flows through the system, the critical configurations making it all work under the hood, and pathways to extend it.

---

## 1. Project Overview & Setup

The goal of this project is to build a **modern, open-source Data Lakehouse** entirely on your local machine using Docker. A Lakehouse combines the flexibility and low cost of data lakes (object storage) with the ACID transactions and governance of data warehouses.

The setup is orchestrated via `docker-compose.yml`, which spins up a network (`lakehouse`) of containers that mimic a production cloud environment (like AWS S3 + EMR + Glue/Athena + MSK), but locally.

### Key Setup Operations:
- **`minio-init`**: A transient container that uses the MinIO Client (`mc`) to create the `warehouse` and `flink` buckets automatically on startup.
- **Custom Flink Image**: We build a custom Docker image (`lakehouse-flink:1.20.0`) because vanilla Flink lacks the "fat" JARs needed to connect to Kafka, Iceberg, and S3. Our Dockerfile injects `hadoop-client-api`, `hadoop-client-runtime`, `iceberg-flink-runtime`, `flink-s3-fs-hadoop`, and Kafka clients directly into the Flink classpath (`/opt/flink/lib`).
- **Python-First via uv**: We use PyFlink (`kafka_to_iceberg.py`) and PyFlink SQL (`setup_catalog.py`) to orchestrate the ingestion and catalog creation, entirely bypassing the need to write Java. We rely on `uv` to natively execute Python tools outside of Docker.

---

## 2. The Components

### 1. Kafka & Kafka UI (The Ingestion Layer)
- **Role**: A distributed streaming platform. It acts as the buffer and transport layer for raw events.
- **Why**: Handles high-throughput streaming data and decouples the message producers from the data processing engine.
- **Kafka UI**: A web interface to monitor topics, consumer groups, and messages.

### 2. event-generator (The Data Source)
- **Role**: A Python REST API and continuous background task that generates mock JSON events and publishes them to the Kafka topic (`benchmark_events`).
- **Why**: Simulates real-world application telemetry, IoT sensor data, or clickstream events. Runs automatically if `ACTIVE_GENERATION=true` is set in your `.env`.

### 3. Apache Flink (The Processing Engine)
- **Role**: A distributed stream processing engine. 
- **Components**: 
  - **JobManager**: Coordinates the distributed execution (schedules tasks, manages checkpoints).
  - **TaskManager**: The worker node that executes the data processing (reading from Kafka, converting to Parquet, writing to MinIO).
- **Why**: Flink provides exactly-once processing semantics, powerful windowing, and native SQL support for streaming data.

### 4. Apache Iceberg (The Table Format)
- **Role**: An open table format for huge analytic datasets. It brings SQL-like behavior to raw files in object storage.
- **Why**: Allows you to perform ACID transactions (UPSERT, DELETE), schema evolution (adding/renaming columns without rewriting data), and time travel (querying historical snapshots). It tracks the state of the table using metadata files (`.json`, `.avro`) that point to data files (`.parquet`).

### 5. Project Nessie (The Catalog)
- **Role**: The metadata catalog for Iceberg. It acts like "Git for Data."
- **Why**: Iceberg needs a catalog to track the "current state" (the latest snapshot) of a table. Nessie allows multi-table transactions, branching, and tagging. It ensures that when Flink writes a new batch of Parquet files, readers don't see them until Nessie officially "commits" the new Iceberg snapshot.

### 6. MinIO (The Storage Layer)
- **Role**: High-performance, S3-compatible object storage.
- **Why**: Acts as the physical storage tier (the "Lake"). All Iceberg data files (`.parquet`) and metadata files live here.

---

## 3. Data Flow Architecture

1. **Generation**: You trigger `event-generator` via its REST API or its automated background task. It serializes JSON payloads and pushes them to the `benchmark_events` Kafka topic.
2. **Streaming Read**: Flink's Kafka SQL Connector continuously polls the Kafka topic.
3. **Processing**: The Flink JobManager executes `kafka_to_iceberg.py`. The TaskManager parses the JSON events into Flink SQL internal rows.
4. **Buffering & Writing**: Flink buffers the rows in memory. Periodically (e.g., every 60 seconds), Flink triggers a **Checkpoint**. 
5. **Iceberg Commit**: Upon a successful Flink checkpoint, the Iceberg Sink writes the buffered data as highly compressed `.parquet` files into the `warehouse/lakehouse/benchmark_events/data/` bucket in MinIO. 
6. **Nessie Update**: Once the files are safely in MinIO, Flink contacts Project Nessie to atomically commit a new Iceberg snapshot. Nessie updates the `main` branch pointer to the new metadata tree.
7. **Querying**: Downstream engines (like Trino, DuckDB, or Flink Batch) ask Nessie for the latest snapshot, read the metadata from MinIO to find which Parquet files to scan, and return the data to the user.

---

## 4. Important Under-the-Hood Configurations

To truly understand why this works, you must understand these critical configurations:

### Flink S3 Configurations (`flink-conf.yaml` / Docker Env Vars)
```yaml
s3.endpoint: http://minio:9000
s3.path.style.access: true
```
* **Why it matters**: AWS S3 natively uses virtual-host-style URLs (`http://bucket.s3.amazonaws.com`). MinIO locally requires **path-style access** (`http://minio:9000/bucket`). If this is missing, Flink/Iceberg will try to resolve `http://warehouse.minio:9000` and fail.

### Flink Checkpointing (`kafka_to_iceberg.py`)
```python
env.enable_checkpointing(60000) # 60 seconds
env.get_checkpoint_config().set_checkpoint_storage_dir("s3a://flink/checkpoints")
```
* **Why it matters**: In Flink, **Iceberg commits are strictly tied to Flink checkpoints**. If checkpointing is disabled, Flink will write hidden, uncommitted files to MinIO, but Nessie will *never* be updated. Checkpointing guarantees exactly-once delivery.

### Nessie API Versioning (`setup_catalog.py`)
```python
'uri'='http://nessie:19120/api/v1'
```
* **Why it matters**: Iceberg catalogs have different REST specifications. Nessie has a native API (`v1`/`v2`) and a standard Iceberg REST API. Flink's `iceberg-nessie` catalog requires the native `api/v1` endpoint, while Python's `pyiceberg` strictly requires the Iceberg REST API standard. This mismatch is why we had to use PyFlink SQL to initialize the catalog.

### Kafka Startup Mode (`kafka_to_iceberg.py`)
```sql
'scan.startup.mode'='earliest-offset'
```
* **Why it matters**: Tells Flink where to start reading if no previous consumer offset is saved. `earliest-offset` ensures you process all historical data in the topic.

---

## 5. How to Learn and Extend from Here

You now have a production-grade skeleton. Here are the logical next steps to extend your knowledge and the platform:

### 1. Add an OLAP Query Engine (Trino or Presto)
Currently, you can query data via Flink SQL. Adding **Trino** via Docker allows you to run blazing-fast, distributed BI queries using standard SQL. Trino connects directly to Nessie and MinIO, completely independent of Flink.
* **Goal**: Spin up Trino in docker-compose, configure a Nessie catalog, and query `lakehouse.benchmark_events` using DBeaver.

### 2. Implement Data Branching (WAP Pattern)
Because we use Nessie, you have "Git for Data".
* **Goal**: Modify the PyFlink job to write to a branch called `audit_branch`. Run a data quality check, and if it passes, use the Nessie UI/CLI to merge `audit_branch` into `main`. This is the **Write-Audit-Publish (WAP)** pattern.

### 3. Add Change Data Capture (CDC) with Debezium
Instead of a Python generator, connect a real PostgreSQL database.
* **Goal**: Add PostgreSQL and Debezium to docker-compose. Debezium will stream row-level `INSERT`/`UPDATE`/`DELETE` events into Kafka. Configure Flink Iceberg to consume these CDC logs and apply Upserts to the Iceberg table using the `id` primary key.

### 4. Transformation with dbt
* **Goal**: Use `dbt-trino` or `dbt-duckdb` to build a Medallion architecture (Bronze -> Silver -> Gold). Read the raw Iceberg table, clean the JSON, and write aggregates back as new Iceberg tables.

### 5. DuckDB for Local Analytics (Already Implemented!)
* **How it works**: By running `cd analytics && uv run python query_with_duckdb.py`, DuckDB uses its native `iceberg` and `httpfs` extensions to bypass PyIceberg's REST catalog completely. It reads credentials dynamically from your `.env` file, finds the latest metadata snapshot in MinIO, and queries the Parquet files locally on your machine.
