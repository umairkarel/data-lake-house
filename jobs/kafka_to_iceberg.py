"""
kafka_to_iceberg.py
-------------------
PyFlink job: reads order_events JSON from Kafka → writes to an Iceberg table
via the Nessie catalog on MinIO.

Run inside the Flink cluster:
    flink run --python /opt/flink/jobs/kafka_to_iceberg.py

Or from the jobmanager container:
    docker exec lakehouse-jobmanager flink run --python /opt/flink/jobs/kafka_to_iceberg.py
"""

import os
import sys
from pathlib import Path

from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.types import DataTypes


# ---------------------------------------------------------------------------
# Configuration — override via environment variables in docker-compose
# ---------------------------------------------------------------------------
KAFKA_BROKERS     = os.getenv("KAFKA_BROKERS",    "kafka:9092")
KAFKA_TOPIC       = os.getenv("KAFKA_TOPIC",      "order_events")
KAFKA_GROUP_ID    = os.getenv("KAFKA_GROUP_ID",   "flink-lakehouse")
KAFKA_START_OFFSET = os.getenv("KAFKA_START_OFFSET", "earliest-offset")

NESSIE_URI  = os.getenv("NESSIE_URI",         "http://lakehouse-nessie:19120/api/v1")
NESSIE_REF  = os.getenv("NESSIE_REF",         "main")
WAREHOUSE   = os.getenv("WAREHOUSE",           "s3://warehouse/")
NAMESPACE   = os.getenv("ICEBERG_NAMESPACE",   "lakehouse")
TABLE_NAME  = os.getenv("ICEBERG_TABLE",       "order_events")

S3_ENDPOINT   = os.getenv("S3_ENDPOINT",   "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")

CHECKPOINT_PATH        = os.getenv("CHECKPOINT_PATH",        "s3://flink/checkpoints")
CHECKPOINT_INTERVAL_MS = int(os.getenv("CHECKPOINT_INTERVAL_MS", "60000"))  # 1 min

TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS"


def main():
    # -------------------------------------------------------------------------
    # 1. Set up Flink execution environment
    # -------------------------------------------------------------------------
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_checkpoint_storage_dir(CHECKPOINT_PATH)
    env.set_parallelism(1)

    t_env = StreamTableEnvironment.create(
        stream_execution_environment=env,
        environment_settings=EnvironmentSettings.in_streaming_mode(),
    )

    # -------------------------------------------------------------------------
    # 2. Hadoop / S3 configuration (passed to Flink's FileSystem for MinIO)
    # -------------------------------------------------------------------------
    t_env.get_config().set("fs.s3a.endpoint",              S3_ENDPOINT)
    t_env.get_config().set("fs.s3a.access.key",            S3_ACCESS_KEY)
    t_env.get_config().set("fs.s3a.secret.key",            S3_SECRET_KEY)
    t_env.get_config().set("fs.s3a.path.style.access",     "true")
    t_env.get_config().set("fs.s3a.impl",                  "org.apache.hadoop.fs.s3a.S3AFileSystem")
    t_env.get_config().set("fs.s3a.connection.ssl.enabled","false")

    # -------------------------------------------------------------------------
    # 3. Register Nessie / Iceberg catalog
    # -------------------------------------------------------------------------
    t_env.execute_sql(f"""
        CREATE CATALOG nessie_catalog WITH (
            'type'                 = 'iceberg',
            'catalog-impl'         = 'org.apache.iceberg.nessie.NessieCatalog',
            'uri'                  = '{NESSIE_URI}',
            'ref'                  = '{NESSIE_REF}',
            'warehouse'            = '{WAREHOUSE}',
            'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint'          = '{S3_ENDPOINT}',
            's3.access-key-id'     = '{S3_ACCESS_KEY}',
            's3.secret-access-key' = '{S3_SECRET_KEY}',
            's3.path-style-access' = 'true',
            'client.region'        = 'us-east-1'
        )
    """)

    t_env.use_catalog("nessie_catalog")
    t_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    t_env.use_database(NAMESPACE)

    print(f"[OK] Using catalog: nessie_catalog | database: {NAMESPACE}")

    # -------------------------------------------------------------------------
    # 4. Create Kafka source table
    #    Schema matches order_events produced by OrderEventGenerator
    # -------------------------------------------------------------------------
    t_env.execute_sql(f"""
        CREATE TEMPORARY TABLE kafka_source (
            event_id        STRING,
            order_id        STRING,
            user_id         STRING,
            product_id      STRING,
            category        STRING,
            status          STRING,
            quantity        INT,
            unit_price      DOUBLE,
            total_amount    DOUBLE,
            discount_pct    DOUBLE,
            region          STRING,
            platform        STRING,
            event_time      STRING,
            event_timestamp AS TO_TIMESTAMP(event_time, '{TIMESTAMP_PATTERN}'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = '{KAFKA_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BROKERS}',
            'properties.group.id'          = '{KAFKA_GROUP_ID}',
            'scan.startup.mode'            = '{KAFKA_START_OFFSET}',
            'format'                       = 'json',
            'json.ignore-parse-errors'     = 'true'
        )
    """)

    print(f"[OK] Kafka source table created: topic={KAFKA_TOPIC}")

    # -------------------------------------------------------------------------
    # 5. Insert into Iceberg table (Nessie catalog handles versioning)
    #
    #    NOTE: No deduplication or lifecycle logic here — this is the raw
    #    bronze ingestion layer. Downstream Flink jobs (to be added later)
    #    will handle dedup, aggregations, and CEP on top of this raw table.
    # -------------------------------------------------------------------------
    print(f"[OK] Starting streaming insert into {NAMESPACE}.{TABLE_NAME} ...")

    statement_set = t_env.create_statement_set()
    statement_set.add_insert_sql(f"""
        INSERT INTO `{NAMESPACE}`.`{TABLE_NAME}`
        SELECT
            event_id,
            order_id,
            user_id,
            product_id,
            category,
            status,
            quantity,
            unit_price,
            total_amount,
            discount_pct,
            region,
            platform,
            CAST(event_timestamp AS TIMESTAMP(6)) AS event_time,
            CURRENT_TIMESTAMP                     AS ingestion_time
        FROM kafka_source
    """)

    result = statement_set.execute()
    print(f"[RUNNING] Job submitted. Job ID: {result.get_job_client().get_job_id()}")


if __name__ == "__main__":
    main()
