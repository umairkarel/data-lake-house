# Local Open Data Lakehouse Setup Complete

We have successfully built and deployed a Python-first local Data Lakehouse replicating the architecture of the article using **Apache Flink**, **Apache Iceberg**, **Project Nessie**, and **MinIO**. 

Here is what we accomplished:

## 1. Infrastructure (Docker Compose)
We have a fully functional local stack running:
- **Kafka & Kafka-UI**: For real-time streaming data ingestion.
- **MinIO**: Acting as our AWS S3-compatible object storage (the `warehouse`).
- **Nessie (0.59.0)**: Acting as the Git-like transactional catalog for Iceberg tables.
- **Flink Cluster**: A JobManager and TaskManager with our custom `lakehouse-flink:1.20.0` image containing all necessary dependencies (Hadoop, Iceberg, Kafka connectors).
- **Event Generator**: Your Python-based Kafka producer API and background task (`event-generator`).

## 2. Catalog and Table Setup
Instead of relying on `pyiceberg` (which dropped support for Nessie's v1 native endpoints), we wrote a **PyFlink SQL script** (`setup_catalog.py`). This script executes Flink DDL to directly connect to Nessie using the Java `iceberg-nessie` catalog integration.
- Created the Iceberg catalog `nessie_catalog`.
- Created the namespace `lakehouse`.
- Created the Iceberg table `benchmark_events` with an `id` primary key and upsert support.

## 3. Real-Time Streaming Job
We adapted the Java streaming logic into a pure PyFlink script (`jobs/kafka_to_iceberg.py`). 
- **Source**: Flink SQL Kafka connector reads JSON data continuously from the `benchmark_events` topic.
- **Sink**: Flink SQL Iceberg connector writes the events into the MinIO `warehouse` and commits snapshots to Nessie.
- **Status**: The job was successfully submitted to the Flink JobManager and is currently **RUNNING** (`JobID: 63501c9d8df65238a84ab2ca3bd43a31`).

## 4. How to Test
The `event-generator` container is fully configured via your `.env` file. If you set `ACTIVE_GENERATION=true`, it automatically pushes background events to Kafka every few seconds.

### Step A: Generate Data (Optional if ACTIVE_GENERATION=true)
Trigger the REST API to push messages into Kafka by passing the required schema for our table:

**If using Bash / WSL / Mac or Windows Command Prompt (with `make` installed):**
```bash
# Generate 100 events
make generate-events COUNT=100

# Or run continuous generation
make generate-continuous
```

### Step B: Verify in MinIO
Open the MinIO UI at http://localhost:9001 (Credentials: configured in your `.env`, default is `minioadmin` / `minioadmin`).
Browse to `warehouse` > `lakehouse` > `benchmark_events` to see the Iceberg `.parquet` data files and `metadata` folders appearing as Flink checkpoints occur (every 60 seconds).

### Step C: Verify via Kafka UI
Open http://localhost:8080 to browse the incoming streaming messages in the `benchmark_events` topic.

### Step D: Query the Data using DuckDB
You can query your Iceberg tables locally using native DuckDB extensions (`httpfs` + `iceberg`), which dynamically reads your MinIO credentials from `.env`.

```bash
uv run python analytics/query_with_duckdb.py
```
This avoids heavy Java JAR dependencies on your host machine!
