import os
import datetime
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, Schema, DataTypes
from pyflink.common import Types, Time, WatermarkStrategy, Row
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common.watermark_strategy import Duration
from pyflink.datastream import OutputTag

KAFKA_BROKERS     = os.getenv("KAFKA_BROKERS",    "kafka:9092")
KAFKA_TOPIC       = os.getenv("KAFKA_TOPIC",      "order_events")
KAFKA_GROUP_ID    = "flink-late-job"
KAFKA_START_OFFSET = os.getenv("KAFKA_START_OFFSET", "latest-offset")

NESSIE_URI  = os.getenv("NESSIE_URI",         "http://lakehouse-nessie:19120/api/v1")
NESSIE_REF  = 'dev'
WAREHOUSE   = os.getenv("WAREHOUSE",           "s3://warehouse/")
NAMESPACE   = os.getenv("ICEBERG_NAMESPACE",   "lakehouse")

S3_ENDPOINT   = os.getenv("S3_ENDPOINT",   "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")

CHECKPOINT_PATH        = os.getenv("CHECKPOINT_PATH",        "s3://flink/checkpoints")
CHECKPOINT_INTERVAL_MS = int(os.getenv("CHECKPOINT_INTERVAL_MS", "60000"))  # 1 min
TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS"


# -----------------------------------------------------------------------------
# Custom ProcessWindowFunction to extract Window Start/End times
# -----------------------------------------------------------------------------
class SumWindowFunction(ProcessWindowFunction):
    def process(self, key, context, elements):
        # Calculate total sales for this window
        total = sum([e[1] for e in elements])
        
        # Convert window epochs to Python datetimes so Iceberg can store them
        w_start = datetime.datetime.fromtimestamp(context.window().start / 1000.0)
        w_end = datetime.datetime.fromtimestamp(context.window().end / 1000.0)
        
        # Format as string to avoid PyFlink DataStream <-> Table API Timestamp ClassCastExceptions
        w_start_str = w_start.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        w_end_str = w_end.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        # Return a PyFlink Row matching our Iceberg schema
        yield Row(w_start_str, w_end_str, key, total)


def process_events(t_env):
    # -------------------------------------------------------------------------
    # 1. Create the Sinks (Aggregated Sales & Dead Letter Queue)
    # -------------------------------------------------------------------------
    t_env.execute_sql("DROP TABLE IF EXISTS windowed_sales")
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS windowed_sales (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            region STRING,
            total_sales DOUBLE,
            PRIMARY KEY (window_start, region) NOT ENFORCED
        ) WITH ('format-version'='2', 'write.upsert.enabled'='true')
    """)

    t_env.execute_sql("DROP TABLE IF EXISTS late_events_dlq")
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS late_events_dlq (
            region STRING,
            total_amount DOUBLE,
            event_time TIMESTAMP(3)
        ) WITH ('format-version'='2')
    """)

    kafka_source = t_env.from_path("kafka_source")

    # -------------------------------------------------------------------------
    # 2. Extract DataStream and Assign Watermarks
    # -------------------------------------------------------------------------
    ds = t_env.to_data_stream(
        kafka_source.select(kafka_source.region, kafka_source.total_amount, kafka_source.event_timestamp)
    )

    from pyflink.common.watermark_strategy import TimestampAssigner

    class MyTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp: int) -> int:
            return int(value[2].timestamp() * 1000)

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)) \
        .with_timestamp_assigner(MyTimestampAssigner())
    ds = ds.assign_timestamps_and_watermarks(watermark_strategy)

    # Convert to standard PyFlink Rows to make aggregation easy
    # We keep the event_timestamp (row[2]) as a string so we can write it to the DLQ!
    simple_ds = ds.map(
        # [region, total_amount, event_timestamp]
        lambda row: Row(str(row[0]), float(row[1]), row[2].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] if hasattr(row[2], 'strftime') else str(row[2])), 
        output_type=Types.ROW([Types.STRING(), Types.DOUBLE(), Types.STRING()])
    )

    # -------------------------------------------------------------------------
    # 3. STRATEGY: Allowed Lateness & Side Output (Dead Letter Queue)
    # -------------------------------------------------------------------------
    late_data_tag = OutputTag(
        "late_events_tag", 
        Types.ROW([Types.STRING(), Types.DOUBLE(), Types.STRING()])
    )

    windowed_stream = simple_ds \
        .key_by(lambda x: x[0]) \
        .window(TumblingEventTimeWindows.of(Time.minutes(1))) \
        .allowed_lateness(Time.seconds(60)) \
        .side_output_late_data(late_data_tag) \
        .process(
            SumWindowFunction(), 
            Types.ROW_NAMED(
                ['window_start_str', 'window_end_str', 'region', 'total_sales'],
                [Types.STRING(), Types.STRING(), Types.STRING(), Types.DOUBLE()]
            )
        )

    late_stream = windowed_stream.get_side_output(late_data_tag)

    # -------------------------------------------------------------------------
    # 4. Convert back to Table API and INSERT INTO Iceberg
    # -------------------------------------------------------------------------
    windowed_table_raw = t_env.from_data_stream(
        windowed_stream,
        Schema.new_builder()
            .column("window_start_str", DataTypes.STRING())
            .column("window_end_str", DataTypes.STRING())
            .column("region", DataTypes.STRING().not_null())
            .column("total_sales", DataTypes.DOUBLE())
            .build()
    )
    
    t_env.create_temporary_view("windowed_table_raw", windowed_table_raw)
    
    windowed_table_cast = t_env.sql_query("""
        SELECT 
            CAST(window_start_str AS TIMESTAMP(3)) AS window_start,
            CAST(window_end_str AS TIMESTAMP(3)) AS window_end,
            region,
            total_sales
        FROM windowed_table_raw
    """)

    late_table = t_env.from_data_stream(
        late_stream,
        Schema.new_builder()
            .column("f0", DataTypes.STRING())
            .column("f1", DataTypes.DOUBLE())
            .column("f2", DataTypes.STRING())
            .build()
    )
    
    t_env.create_temporary_view("late_table_raw", late_table)
    late_table_cast = t_env.sql_query("""
        SELECT 
            f0 AS region, 
            f1 AS total_amount,
            CAST(f2 AS TIMESTAMP(3)) AS event_time
        FROM late_table_raw
    """)

    # Use a StatementSet to execute both insertions in the same Flink Job!
    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert("windowed_sales", windowed_table_cast)
    stmt_set.add_insert("late_events_dlq", late_table_cast)
    stmt_set.execute()


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
            event_timestamp AS TO_TIMESTAMP(event_time, '{TIMESTAMP_PATTERN}')
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

    print(f"[OK] Late Events Job Initialized: Catching late data for topic={KAFKA_TOPIC}")

    try:
        process_events(t_env)
    except Exception as e:
        print("Writing records from Kafka to Iceberg failed:", str(e))

if __name__ == '__main__':
    main()
