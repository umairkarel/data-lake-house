import os
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

KAFKA_BROKERS     = os.getenv("KAFKA_BROKERS",    "kafka:9092")
KAFKA_TOPIC       = os.getenv("KAFKA_TOPIC",      "order_events")
KAFKA_GROUP_ID    = "flink-enrich-job-regions"
KAFKA_START_OFFSET = os.getenv("KAFKA_START_OFFSET", "latest-offset")

NESSIE_URI  = os.getenv("NESSIE_URI",         "http://lakehouse-nessie:19120/api/v1")
NESSIE_REF  = 'dev'
WAREHOUSE   = os.getenv("WAREHOUSE",           "s3://warehouse/")
NAMESPACE   = os.getenv("ICEBERG_NAMESPACE",   "lakehouse")
SINK_TABLE_NAME  = "regional_orders"

S3_ENDPOINT   = os.getenv("S3_ENDPOINT",   "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")

CHECKPOINT_PATH        = os.getenv("CHECKPOINT_PATH",        "s3://flink/checkpoints")
CHECKPOINT_INTERVAL_MS = int(os.getenv("CHECKPOINT_INTERVAL_MS", "60000"))  # 1 min
TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS"

def process_events(t_env):
    # t_env.execute_sql(f"DROP TABLE IF EXISTS {SINK_TABLE_NAME}")
    t_env.execute_sql(f"""
        CREATE TABLE IF NOT EXISTS {SINK_TABLE_NAME} (
            event_time TIMESTAMP(3),
            order_id STRING,
            region STRING,
            data_center STRING,
            tax_rate DOUBLE,
            compliance_type STRING,
            total_amount DOUBLE,
            PRIMARY KEY (order_id) NOT ENFORCED
        ) WITH (
            'format-version'        = '2',
            'write.upsert.enabled'  = 'true'
        )
    """)

    # 1. Bounded Static Table (CSV)
    # The /opt/flink/seeds folder is mounted directly into the Flink containers
    t_env.execute_sql("""
        CREATE TEMPORARY TABLE regions_csv (
            region STRING,
            data_center STRING,
            tax_rate DOUBLE,
            compliance_type STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '/opt/flink/seeds/regions.csv',
            'format' = 'csv',
            'csv.ignore-parse-errors' = 'true',
            'csv.field-delimiter' = ','
        )
    """)

    # 2. Stream-to-Batch Join and Insert
    # This enriches the continuous Kafka stream with the static CSV data based on 'region'!
    t_env.execute_sql(f"""
        INSERT INTO {SINK_TABLE_NAME}
        SELECT 
            k.event_timestamp AS event_time,
            k.order_id,
            k.region,
            r.data_center,
            r.tax_rate,
            r.compliance_type,
            k.total_amount
        FROM kafka_source k
        LEFT JOIN regions_csv r ON k.region = r.region
    """)


def main():
    stream_env = StreamExecutionEnvironment.get_execution_environment()
    stream_env.enable_checkpointing(CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE)
    stream_env.get_checkpoint_config().set_checkpoint_storage_dir(CHECKPOINT_PATH)
    stream_env.set_parallelism(1)

    t_env = StreamTableEnvironment.create(
        stream_execution_environment=stream_env,
        environment_settings=EnvironmentSettings.in_streaming_mode(),
    )

    t_env.get_config().set("fs.s3a.endpoint",              S3_ENDPOINT)
    t_env.get_config().set("fs.s3a.access.key",            S3_ACCESS_KEY)
    t_env.get_config().set("fs.s3a.secret.key",            S3_SECRET_KEY)
    t_env.get_config().set("fs.s3a.path.style.access",     "true")
    t_env.get_config().set("fs.s3a.impl",                  "org.apache.hadoop.fs.s3a.S3AFileSystem")
    t_env.get_config().set("fs.s3a.connection.ssl.enabled","false")

    t_env.execute_sql(f'''
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
    ''')

    t_env.use_catalog("nessie_catalog")
    t_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {NAMESPACE}")
    t_env.use_database(NAMESPACE)

    t_env.execute_sql(f'''
        CREATE TEMPORARY TABLE kafka_source (
            order_id        STRING,
            region          STRING,
            total_amount    DOUBLE,
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
    ''')

    print(f"[OK] Enrichment Job Initialized: Joining Kafka topic={KAFKA_TOPIC} with regions.csv")
    
    try:
        process_events(t_env)
    except Exception as e:
        print("Writing records from Kafka to Iceberg failed:", str(e))

if __name__ == '__main__':
    main()
