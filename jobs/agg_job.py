import os
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import lit, col
from pyflink.table.window import Tumble


KAFKA_BROKERS     = os.getenv("KAFKA_BROKERS",    "kafka:9092")
KAFKA_TOPIC       = os.getenv("KAFKA_TOPIC",      "order_events")
KAFKA_GROUP_ID    = os.getenv("KAFKA_GROUP_ID_AGG", "flink-agg-job")
KAFKA_START_OFFSET = "latest-offset"

NESSIE_URI  = os.getenv("NESSIE_URI",         "http://lakehouse-nessie:19120/api/v1")

# Using dev ref for practicing
NESSIE_REF  = 'dev' # os.getenv("NESSIE_REF",         "main")
WAREHOUSE   = os.getenv("WAREHOUSE",           "s3://warehouse/")
NAMESPACE   = os.getenv("ICEBERG_NAMESPACE",   "lakehouse")
SINK_TABLE_NAME  = os.getenv("ICEBERG_TABLE",       "products_sold")

S3_ENDPOINT   = os.getenv("S3_ENDPOINT",   "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")

CHECKPOINT_PATH        = os.getenv("CHECKPOINT_PATH",        "s3://flink/checkpoints")
CHECKPOINT_INTERVAL_MS = int(os.getenv("CHECKPOINT_INTERVAL_MS", "60000"))  # 1 min

TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS"


def process_events(t_env):
    t_env.execute_sql(f"DROP TABLE IF EXISTS {SINK_TABLE_NAME}")
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS {SINK_TABLE_NAME} (
            event_time TIMESTAMP(3),
            product_id STRING,
            orders BIGINT,
            PRIMARY KEY (event_time, product_id) NOT ENFORCED
        ) WITH (
            'format-version'        = '2',
            'write.upsert.enabled'  = 'true'
        )
    """)

    t_env.from_path('kafka_source') \
        .window(
            Tumble.over(lit(1).minutes).on(col("event_timestamp")).alias("w")
        ).group_by(
            col("w"),
            col("product_id"),
        ) \
        .select(
            col("w").start.alias("event_time"),
            col("product_id"),
            col("order_id").count.distinct.alias("orders")
        ) \
        .execute_insert(SINK_TABLE_NAME)


def main():
    stream_env = StreamExecutionEnvironment.get_execution_environment()
    stream_env.enable_checkpointing(CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE)
    stream_env.get_checkpoint_config().set_checkpoint_storage_dir(CHECKPOINT_PATH)
    stream_env.set_parallelism(1) # This is IMP to set it to 1 since we only have 1 parition in our kafka topic.

    t_env = StreamTableEnvironment.create(
        stream_execution_environment=stream_env,
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

    try:
        process_events(t_env)
    except Exception as e:
        print("Writing records from Kafka to ICeberg failed:", str(e))

if __name__ == '__main__':
    main()

