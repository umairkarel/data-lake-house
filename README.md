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
| **msgGeneratorKafka** | REST API to push synthetic events to Kafka | http://localhost:8090 |

## Architecture

```
msgGeneratorKafka (REST API)
    │  POST /generate → JSON events
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
DuckDB / PyIceberg (analytics)   ←── time travel, branch reads, SQL
```

## Quick Start

### Prerequisites
- Docker Desktop (with WSL2 on Windows)
- `make` (Git Bash / WSL provides this)
- Python 3.10+ (for local analytics scripts)

### 1. Build the Flink image
```bash
make build
```
This downloads all required JARs (Iceberg, Hadoop S3, AWS SDK, Kafka connector) into the image. Takes ~3-5 minutes on first run.

### 2. Start all services
```bash
make up
```

### 3. Initialize the catalog
```bash
make setup-catalog
```
Creates the `lakehouse_db` namespace and `benchmark_events` Iceberg table in Nessie.

### 4. Generate events to Kafka
Using the **msgGeneratorKafka** REST API (auto-started on port 8090):
```bash
# Create Kafka topic
curl -X POST http://localhost:8090/kafka/topic \
  -d "topic_name=benchmark_events&bootstrap_servers=kafka:9092&num_partitions=3&replication_factor=1"

# Push 1000 events with this schema
curl -X POST http://localhost:8090/kafka/generate \
  -d 'topic_name=benchmark_events' \
  -d 'bootstrap_servers=kafka:9092' \
  -d 'count=1000' \
  -d 'parallelism=4' \
  -d 'schema={"id":"STRING","value":"INTEGER","amount":"FLOAT","event_time":"DATETIME"}'
```

### 5. Start the streaming job
```bash
make run-kafka-job
```
Flink reads from Kafka → writes Parquet files to MinIO via Iceberg + Nessie catalog.

### 6. Query the data
```bash
# Requires: pip install pyiceberg[s3fs,nessie] duckdb pyyaml pyarrow
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
│   ├── catalog_config.yaml       # Nessie + MinIO connection config
│   ├── schema.py                 # PyIceberg schema definitions
│   └── setup_catalog.py          # Bootstrap: create namespace + tables
│
├── jobs/
│   ├── kafka_to_iceberg.py       # PyFlink: Kafka → Iceberg streaming job
│   └── compaction.py             # PyIceberg: small-file compaction
│
├── nessie/
│   └── nessie_branches.py        # CLI: create/merge/tag Nessie branches
│
├── analytics/
│   └── query_with_duckdb.py      # DuckDB + PyIceberg: SQL queries & time travel
│
├── Makefile                      # Convenience commands
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

# Query dev branch data
python analytics/query_with_duckdb.py --branch dev

# Once validated, merge dev → main
make nessie-merge
```

## Time Travel

```bash
# List all snapshots
python analytics/query_with_duckdb.py --snapshots

# Query a specific snapshot
python analytics/query_with_duckdb.py --snapshot 1234567890
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
| **Catalog management** | `CatalogCreator.java` (Java Iceberg API) | `setup_catalog.py` (PyIceberg) |
| **Table creation** | `TableCreator.java` | `schema.py` + PyIceberg |
| **Compaction** | `CompactionService.java` (Java) | `compaction.py` (PyIceberg) |
| **Infrastructure** | Minikube (Kubernetes) | Docker Compose |
| **Analytics** | Not implemented | DuckDB + PyIceberg |
| **JARs required?** | Yes (Java deps) | Yes (Flink JVM runtime) — but only in Docker |
