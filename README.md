# Open Data Lakehouse — Python-First Local Setup

A local data lakehouse built with **Python-first** tooling, replacing the original Java/Maven stack.

## Stack

| Component | Role | Access |
|---|---|---|
| **Apache Flink 1.20** (PyFlink) | Stream processing engine | http://localhost:8081 |
| **Apache Iceberg 1.8** | Open table format (ACID, time travel, schema evolution) | — |
| **Project Nessie** | Git-like catalog (branch/tag/merge Iceberg tables) | http://localhost:19120 |
| **MinIO** | S3-compatible local object storage | http://localhost:9001 |
| **Kafka** (KRaft) | Event streaming source | — |
| **Kafka UI** | Browse topics & messages | http://localhost:8080 |
| **event-generator** | REST API & Background Task to push synthetic events | http://localhost:8090 |

## Architecture

```
event-generator (REST API / Background)
    │  POST /generate or Background Task → JSON events
    ▼
Kafka (KRaft, no Zookeeper)
    │  topic: benchmark_events
    ▼
Flink Cluster (PyFlink job)       ←── nessie_catalog registered via SQL DDL
    │  kafka_to_iceberg.py
    ▼
Apache Iceberg Tables
    │  Parquet files (Snappy)
    ▼
MinIO (s3://warehouse/)          ←── Nessie tracks versions / branches
    │
    ▼
DuckDB (native httpfs + iceberg) ←── direct queries against MinIO, SQL
```

## Quick Start

### Prerequisites
- Docker Desktop (with WSL2 on Windows)
- `make` (Git Bash / WSL provides this)
- `uv` (Fast Python package manager)
- Python 3.10+ (for local scripts)

### 1. Configure Environment
Copy the example config and adjust if needed:
```bash
cp .env.example .env
```

### 2. Build the Flink image
```bash
make build
```
This downloads all required JARs (Iceberg, Hadoop S3, AWS SDK, Kafka connector) into the image. Takes ~3-5 minutes on first run.

### 3. Start all services
```bash
make up
```

### 3. Initialize the catalog
```bash
make setup-catalog
```
Creates the `lakehouse_db` namespace and `benchmark_events` Iceberg table in Nessie.

### 5. Generate events to Kafka
The **event-generator** automatically starts pushing continuous background events to Kafka if `ACTIVE_GENERATION=true` is set in your `.env`.

Alternatively, use the convenient Make commands (which use the REST API on port 8090):
```bash
# Generate a specific number of events (default is 100)
make generate-events COUNT=1000

# Or simulate continuous background generation from your terminal
make generate-continuous
```

### 6. Start the streaming job
```bash
make run-kafka-job
```
Flink reads from Kafka → writes Parquet files to MinIO via Iceberg + Nessie catalog.

### 7. Query the data
```bash
# Powered by DuckDB native iceberg_scan (no PyIceberg REST catalog required)
# Uses `uv run` to seamlessly execute script with dependencies
make query
```

---

## Project Structure

```
lake-house/
├── docker/
│   ├── docker-compose.yml        # All services
│   └── flink/
│       └── Dockerfile            # Flink + PyFlink + all JARs
│
├── catalog/
│   ├── catalog_config.yaml       # Configuration metadata
│   ├── schema.py                 # PyIceberg schema definitions
│   └── setup_catalog.py          # Bootstrap: create namespace + tables via PyFlink SQL
│
├── jobs/
│   ├── kafka_to_iceberg.py       # PyFlink: Kafka → Iceberg streaming job
│   └── compaction.py             # PyFlink + Py4J: small-file compaction via Java Action
│
├── nessie/
│   └── nessie_branches.py        # CLI: create/merge/tag Nessie branches
│
├── analytics/
│   └── query_with_duckdb.py      # DuckDB native MinIO scan queries
│
├── event-generator/              # Service to push continuous events to Kafka
│
├── Makefile                      # Convenience commands
├── .env.example                  # Template for MinIO/Kafka credentials
├── requirements.txt              # Python deps for local dev
└── Plan.md                       # Architecture analysis & design decisions
```

---

## Nessie Branch Workflow

```bash
# Create a dev branch for experimental writes
make nessie-dev

# Submit the Flink job pointing to the dev branch
NESSIE_REF=dev make run-kafka-job

# Query dev branch data (WIP)
uv run python analytics/query_with_duckdb.py --branch dev

# Once validated, merge dev → main
make nessie-merge
```

## Time Travel

```bash
# List all snapshots (WIP)
uv run python analytics/query_with_duckdb.py --snapshots

# Query a specific snapshot (WIP)
uv run python analytics/query_with_duckdb.py --snapshot 1234567890
```

## Compaction

```bash
# Compact small files in benchmark_events (keeps MinIO tidy)
make compact
```

---

## Key Design Decisions vs. Original Java Project

| Decision | Original (Java) | This project (Python) |
|---|---|---|
| **Job code** | Java DataStream API, Maven | PyFlink Table API / SQL DDL |
| **Catalog management** | `CatalogCreator.java` (Java Iceberg API) | `setup_catalog.py` (PyFlink SQL) |
| **Table creation** | `TableCreator.java` | `setup_catalog.py` (PyFlink SQL) |
| **Compaction** | `CompactionService.java` (Java) | `compaction.py` (PyFlink + Py4J Gateway) |
| **Infrastructure** | Minikube (Kubernetes) | Docker Compose |
| **Analytics** | Not implemented | DuckDB native MinIO scan (`iceberg_scan`) |
| **JARs required?** | Yes (Java deps) | Yes (Flink JVM runtime) — but only in Docker |

---

## Acknowledgments

This Python-first Lakehouse architecture was heavily inspired by and adapted from the excellent Java-based implementation at **[subbota19/flinkerManager](https://github.com/subbota19/flinkerManager)**. We transitioned the core Java concepts into a fully Python-native experience using PyFlink, PyIceberg, and DuckDB.
