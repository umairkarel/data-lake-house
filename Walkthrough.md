# Local Open Data Lakehouse Setup Complete

We have successfully built and deployed a Python-first local Data Lakehouse replicating the architecture of the article using **Apache Flink**, **Apache Iceberg**, **Project Nessie**, and **MinIO**. 

Here is what we accomplished:

## 1. Infrastructure (Docker Compose)
We have a fully functional local stack running:
- **Kafka & Kafka-UI**: For real-time streaming data ingestion.
- **MinIO**: Acting as our AWS S3-compatible object storage (the `warehouse`).
- **Nessie (0.59.0)**: Acting as the Git-like transactional catalog for Iceberg tables.
- **Flink Cluster**: A JobManager and TaskManager with our custom `lakehouse-flink:1.20.0` image containing all necessary dependencies (Hadoop, Iceberg, Kafka connectors).
- **Message Generator**: Your Python-based Kafka producer API (`msgGeneratorKafka`).

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
The `msg-generator` container is currently installing dependencies and starting up. Once it's ready, you can start the data flow!

### Step A: Generate Data
Trigger the REST API to push messages into Kafka by passing the required schema for our table:

**If using Bash / WSL / Mac:**
```bash
curl -X POST http://localhost:8090/kafka/generateMessages \
  -d "topic_name=benchmark_events" \
  -d "bootstrap_servers=kafka:9092" \
  -d 'schema={"id":"INTEGER","type":"STRING","event_time":"DATETIME"}' \
  -d "count=100"
```

**If using Windows Command Prompt (CMD):**
```cmd
curl -X POST http://localhost:8090/kafka/generateMessages ^
  -d "topic_name=benchmark_events" ^
  -d "bootstrap_servers=kafka:9092" ^
  -d "schema={\"id\":\"INTEGER\",\"type\":\"STRING\",\"event_time\":\"DATETIME\"}" ^
  -d "count=100"
```

```powershell
$body = @{
    topic_name = "benchmark_events"
    bootstrap_servers = "kafka:9092"
    schema = '{"id":"INTEGER","type":"STRING","event_time":"DATETIME"}'
    count = 100
}
Invoke-RestMethod -Uri http://localhost:8090/kafka/generateMessages -Method Post -Body $body
```

### Step B: Verify in MinIO
Open the MinIO UI at http://localhost:9001 (Credentials: `minioadmin` / `minioadmin`).
Browse to `warehouse` > `lakehouse` > `benchmark_events` to see the Iceberg `.parquet` data files and `metadata` folders appearing as Flink checkpoints occur (every 60 seconds).

### Step C: Verify via Kafka UI
Open http://localhost:8080 to browse the incoming streaming messages in the `benchmark_events` topic.

> [!TIP]
> You can also run batch Flink SQL queries against the Iceberg table to read the data back using the Flink SQL Client inside the `lakehouse-jobmanager` container.
