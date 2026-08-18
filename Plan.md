# Open Data Lakehouse — Analysis & Python-First Plan

## What the Original Author Built

**Stack:** Apache Flink 1.20 · Apache Iceberg 1.8 · Project Nessie · MinIO · Kafka · Kubernetes (Minikube) · Java 8 (Maven)

### Architecture

```
Kafka (data source)
    │  JSON events
    ▼
Flink Cluster (JobManager + TaskManager on Minikube)
    │  Reads Kafka, maps to Iceberg RowData, writes via IcebergSink
    ▼
Apache Iceberg Tables  ──────────────► Nessie Catalog (Git-like versioning)
    │  Parquet files                        (branching, tagging)
    ▼
MinIO (S3-compatible object store)
    │
    ▼
Query Layer (Trino / Dremio / Spark — not wired in the repo)
```

### Key Java components he wrote

| File | What it does |
|---|---|
| `KafkaIcebergDataStreamJob.java` | Main job: reads from Kafka → maps JSON → writes to Iceberg (NessieCatalog + S3/MinIO) |
| `CreateIcebergTableJob.java` | One-off job to create an Iceberg table via the catalog |
| `CatalogCreator.java` | Reads a YAML config, builds `NessieCatalog` via `CatalogLoader` |
| `TableCreator.java` | Programmatically creates Iceberg tables with schema, partitioning |
| `CompactionService.java` | Triggers Iceberg `RewriteDataFilesAction` (small file compaction) |
| `BenchmarkMessageRowDataMapper.java` | Maps a POJO → Flink `RowData` (Iceberg-compatible format) |
| `BenchmarkMessageIcebergSink.java` | Wraps Iceberg's `FlinkSink` |
| `QueryBuilder.java` | Builds Flink SQL DDL strings programmatically |

### Infrastructure he used (Docker / Kubernetes)

| Component | Image used |
|---|---|
| Flink JM + TM | Custom `docker/flink/Dockerfile` based on `flink:1.20.0` — pre-loads Iceberg, Hadoop, AWS SDK JARs |
| MinIO | `quay.io/minio/minio` (on Minikube) |
| Nessie | `projectnessie/nessie` (on Minikube) |
| Postgres + pgAdmin | For Nessie metadata storage |
| Apache Gravitino | (experimental — separate deployment) |
| Python sidecar | `docker/python/Dockerfile` based on `flink:1.19.0` — installs Python 3.8 + Kafka connector JAR |

---

## What to Reuse vs. Replace

### ✅ REUSE AS-IS (infrastructure layer — language-agnostic)

These are pure Docker/Kubernetes configs that don't care about Java vs Python:

| Component | File(s) | Why reusable |
|---|---|---|
| **Flink Docker image** | `docker/flink/Dockerfile` | Pre-downloads all needed JARs (Iceberg runtime, S3 FS Hadoop, AWS SDK, Hadoop uber). The JAR list is identical whether you use Java or PyFlink. |
| **MinIO deployment** | `minikube/deployment/minio.yaml` | S3-compatible storage — completely language-agnostic |
| **Nessie deployment** | `minikube/deployment/nessie.yaml` | REST catalog server — completely language-agnostic |
| **Postgres/pgAdmin** | `minikube/deployment/postgres.yaml` + `pgadmin.yaml` | Nessie's backend storage |
| **Kubernetes namespace** | `minikube/namespace/` | Namespace setup, no language dependency |
| **PVC / PV** | `minikube/persistent-volume*/` | Storage for Flink checkpoints on MinIO |
| **Services** | `minikube/service/` | NodePort/ClusterIP configs |
| **Makefile** | `Makefile` | kubectl orchestration commands — adapt for your own targets |

> **Bottom line:** You can `kubectl apply` the entire `minikube/` folder almost untouched. The JARs in the Flink Docker image are also directly usable.

---

### 🔄 ADAPT (keep the concept, rewrite for Python)

| Original Java thing | Python equivalent |
|---|---|
| `CatalogCreator.java` — reads YAML, calls `CatalogLoader` | Python dict / YAML config passed to `StreamTableEnvironment` catalog registration |
| `TableCreator.java` — programmatic schema + partitioning via Iceberg Java API | **PyIceberg** (`pyiceberg.catalog`, `pyiceberg.schema`, `pyiceberg.transforms`) |
| `KafkaIcebergDataStreamJob.java` — DataStream API | **PyFlink** `StreamExecutionEnvironment` + Table API |
| `CompactionService.java` — `RewriteDataFilesAction` | `pyiceberg.table.rewrite` (PyIceberg's compaction API) |
| `BenchmarkMessageRowDataMapper.java` — POJO → `RowData` | PyFlink `Row` / Table API handles this natively |
| `QueryBuilder.java` — builds DDL strings | Python f-strings or PyFlink SQL DDL via `t_env.execute_sql()` |
| Maven `pom.xml` — dependency management | `requirements.txt` / `pyproject.toml` + JAR downloads in Dockerfile |

---

### 🆕 BUILD FROM SCRATCH (things he didn't build or you want differently)

| What | Recommendation |
|---|---|
| **Docker Compose for local dev** | He used Minikube. For a simpler local setup, use **Docker Compose** instead — easier on Windows, no Minikube overhead. This is something you'll create yourself. |
| **PyFlink job scripts** | Replace all Java jobs with `.py` files |
| **PyIceberg catalog management** | Use `pyiceberg` library directly to create tables, inspect metadata, do time travel |
| **Nessie Python client** | `pynessie` library for branch/tag/merge operations |
| **Analytics layer** | He left this empty. Wire in **DuckDB** or **Polars** (both can read Iceberg via PyIceberg) for ad-hoc queries |
| **Ingestion orchestration** | He used raw Kafka. Add **Kafka producer scripts** in Python using `confluent-kafka` |

---

## Recommended Python-First Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Docker Compose (local)                       │
│                                                                 │
│  ┌──────────┐   ┌─────────────────────────────────────────────┐ │
│  │  Kafka   │   │      Flink Cluster (PyFlink jobs)           │ │
│  │(Confluent│──▶│  JobManager + TaskManager                   │ │
│  │ or KRaft)│   │  (custom flink:1.20.0 image with JARs)     │ │
│  └──────────┘   └──────────────────┬────────────────────────--┘ │
│                                    │ writes Iceberg via          │
│                                    │ FlinkSink / Table API       │
│  ┌──────────┐   ┌──────────────────▼──────────────────────────┐ │
│  │ PyIceberg│   │         MinIO (S3-compatible)               │ │
│  │ / DuckDB │◀──│   s3://warehouse/ (Parquet files)           │ │
│  │(analytics│   └──────────────────┬──────────────────────────┘ │
│  │layer)    │                      │ metadata                    │
│  └──────────┘   ┌──────────────────▼──────────────────────────┐ │
│                 │         Nessie (REST Catalog)                │ │
│                 │   http://nessie:19120/api/v2                 │ │
│                 │   (Git-like branching for Iceberg tables)    │ │
│                 └─────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## Proposed Project Structure

```
lake-house/
├── docker/
│   ├── docker-compose.yml          # Flink + MinIO + Nessie + Kafka + Postgres
│   └── flink/
│       └── Dockerfile              # Reuse/adapt from original (JAR downloads)
│
├── jobs/                           # PyFlink job scripts (replaces Java jobs)
│   ├── create_iceberg_table.py     # Replaces CreateIcebergTableJob.java
│   ├── kafka_to_iceberg.py         # Replaces KafkaIcebergDataStreamJob.java
│   └── compaction.py               # Replaces CompactionService.java
│
├── catalog/                        # Catalog management (replaces CatalogCreator + TableCreator)
│   ├── catalog_config.yaml         # Nessie + MinIO connection config
│   ├── schema.py                   # PyIceberg table schema definitions
│   └── setup_catalog.py            # Create namespaces, tables via PyIceberg
│
├── ingestion/
│   └── kafka_producer.py           # Python Kafka producer (generate test events)
│
├── analytics/
│   ├── query_with_duckdb.py        # Ad-hoc queries on Iceberg via DuckDB
│   └── query_with_pyiceberg.py     # Time travel, snapshots, branch reads
│
├── nessie/
│   └── branch_operations.py        # Create/merge Nessie branches via pynessie
│
└── requirements.txt
    # apache-flink==1.20.0
    # pyiceberg[s3fs,nessie]==0.8.x
    # pynessie
    # confluent-kafka
    # duckdb
    # pyyaml
```

---

## Key Python Libraries

| Library | Replaces | Purpose |
|---|---|---|
| `apache-flink` (PyFlink) | Java DataStream API | Flink jobs in Python |
| `pyiceberg` | `CatalogLoader`, `TableCreator`, `CompactionService` | Table creation, schema, compaction, time travel |
| `pynessie` | None (not in original) | Nessie branch/tag operations from Python |
| `confluent-kafka` | Separate Kafka infra project | Produce test events |
| `duckdb` | None (he left analytics empty) | Query Iceberg tables locally |
| `boto3` / `s3fs` | AWS SDK in Java | MinIO access from Python |

---

## Java-Free Considerations

> [!IMPORTANT]
> **JARs are still required** — PyFlink submits jobs to the JVM-based Flink runtime. You still need the same JARs as the original (Iceberg Flink runtime, S3 Hadoop FS, AWS SDK). The key difference is that **you write Python, not Java**. The Dockerfile from the original repo downloads these JARs for you.

> [!NOTE]
> **Two approaches for PyFlink catalog registration:**
> 1. **Table API** — Register the NessieCatalog via `t_env.register_catalog()` using a Python dict of catalog properties (same key-value pairs as Java's `CatalogLoader`).
> 2. **SQL DDL** — `t_env.execute_sql("CREATE CATALOG nessie_catalog WITH (...)")` — cleanest approach.

> [!TIP]
> For table creation and metadata operations (schema inspection, time travel, branch reads), **skip Flink entirely** and use `pyiceberg` directly. It talks to Nessie over HTTP and to MinIO over S3 — no JVM needed.

---

## Simplified Docker Compose vs. Minikube Decision

He used Minikube (Kubernetes locally) because he was also learning K8s deployment patterns. For you:

| | Minikube (his way) | Docker Compose (recommended for you) |
|---|---|---|
| **Complexity** | High — kubectl, PVCs, Services, namespaces | Low — single `docker-compose up` |
| **Windows support** | Needs Hyper-V / WSL2 | Native Docker Desktop |
| **Learning focus** | K8s ops | Data lake concepts |
| **Production-like** | Yes (K8s is prod-like) | Less so |

**Recommendation:** Start with Docker Compose. You can always graduate to Minikube/K8s later once the pipeline logic is solid.
