from pyflink.common import Encoder
from pyflink.datastream.connectors.file_system import FileSink
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common import Types, WatermarkStrategy, Time, Row

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

json_serializer = JsonRowSerializationSchema.builder() \
                        .with_type_info(event_schema) \
                        .build()

env = StreamExecutionEnvironment.get_execution_environment()

# Checkpointing is MANDATORY for FileSink! 
# Files remain in an ".in-progress" state and are never finished/written until a checkpoint completes.
env.enable_checkpointing(10000) # Checkpoint every 10 seconds

file_sink = FileSink.for_row_format(
    '/opt/flink/jobs/30-Day-Flink/output/day08',
    Encoder.simple_string_encoder()
).build()


kafka_source = KafkaSource.builder() \
                .set_bootstrap_servers("kafka:9092") \
                .set_topics("order_events") \
                .set_group_id("job08") \
                .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
                .set_value_only_deserializer(json_deserializer) \
                .build()

ds = env.from_source(
    kafka_source,
    WatermarkStrategy.no_watermarks(),
    "Kafka Order Events"
)


def reduce_fn(e1, e2):
    # reduce must return the exact same type as the input stream (event_schema)
    # So we simply update the running totals on the first element and return it
    e1["total_amount"] = e1["total_amount"] + e2["total_amount"]
    
    # We can track the max event_time in the event_time field
    if e2["event_time"] > e1["event_time"]:
        e1["event_time"] = e2["event_time"]
        
    return e1

category_counts = (
    ds.key_by(lambda event: event["category"])
    .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    .reduce(lambda e1, e2: reduce_fn(e1, e2))
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


env.execute("Day 08 - Window and processing time watermark")

