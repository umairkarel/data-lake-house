from pyflink.common import Encoder
from pyflink.datastream.connectors.file_system import FileSink
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common import Types, WatermarkStrategy, Time, Row, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.window import TumblingEventTimeWindows

event_schema = Types.ROW_NAMED(
    [
        "event_id", "order_id", "user_id", "product_id", "category", 
        "status", "quantity", "unit_price", "total_amount", 
        "discount_pct", "region", "platform", "event_time"
    ],
    [
        Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), 
        Types.STRING(), Types.INT(), Types.DOUBLE(), Types.DOUBLE(), 
        Types.DOUBLE(), Types.STRING(), Types.STRING(), Types.STRING()
    ]
)

json_deserializer = JsonRowDeserializationSchema.builder() \
                        .type_info(event_schema) \
                        .build()

env = StreamExecutionEnvironment.get_execution_environment()

# Checkpointing is MANDATORY for FileSink! 
# Files remain in an ".in-progress" state and are never finished/written until a checkpoint completes.
env.enable_checkpointing(10000) # Checkpoint every 10 seconds

file_sink = FileSink.for_row_format(
    '/opt/flink/jobs/30-Day-Flink/output/day10',
    Encoder.simple_string_encoder()
).build()

kafka_source = KafkaSource.builder() \
                .set_bootstrap_servers("kafka:9092") \
                .set_topics("order_events") \
                .set_group_id("job10") \
                .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
                .set_value_only_deserializer(json_deserializer) \
                .build()

class JsonEventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, event, record_ts):
        from datetime import datetime
        try:
            # In PyFlink, the Row object passed to TimestampAssigner is often 
            # coerced to a tuple, causing event["event_time"] to throw a TypeError.
            # We use index 12 (the 13th column) instead to be safe.
            time_str = event[12] if isinstance(event, tuple) else event["event_time"]
            
            dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            # Fallback if there are no milliseconds
            dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return record_ts # Fallback to Kafka timestamp on any other error
            
        return int(dt.timestamp() * 1000)


ds = env.from_source(
    kafka_source,
    WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10))
        # .with_timestamp_assigner(JsonEventTimestampAssigner())
    , "Kafka Order Events"
)



category_counts = (
    ds.key_by(lambda event: event["category"])
    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
    .reduce(lambda e1, e2: e1["total_amount"]+e2["total_amount"])
)

# The output of reduce is still a 13-column Row.
# Let's map it down to just a 2-column dictionary/Row!
final_stream = category_counts.map(
    lambda reduced_event: Row(
        category=reduced_event["category"],
        total_revenue=reduced_event["total_amount"],
        latest_event_time=reduced_event["event_time"]
    ),
    output_type=Types.ROW_NAMED(
        ["category", "total_revenue", "latest_event_time"], 
        [Types.STRING(), Types.DOUBLE(), Types.STRING()]
    ))


final_stream.sink_to(file_sink)


env.execute("Day 10 - Revenue By Category Window")

