import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaRecordSerializationSchema, KafkaOffsetsInitializer, KafkaSink
from pyflink.common import Types, SimpleStringSchema, WatermarkStrategy
from pyflink.datastream.formats.json import JsonRowDeserializationSchema

env = StreamExecutionEnvironment.get_execution_environment()

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

# 2. Build the JSON deserializer
json_deserializer = JsonRowDeserializationSchema.builder() \
    .type_info(event_schema) \
    .build()


# env.add_jars("/opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar")

kafka_src = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_topics("order_events") \
    .set_group_id("flink-kafka-source") \
    .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
    .set_value_only_deserializer(json_deserializer) \
    .build()

ds = env.from_source(
    kafka_src,
    WatermarkStrategy.no_watermarks(),
    "Kafka Order Events"
)

def get_kafka_sink(topic):
    return KafkaSink.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
    ) \
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
    .build()


us_east_kafka_sink = get_kafka_sink("order_events_us_east")
ap_south_kafka_sink = get_kafka_sink("order_events_ap_south")
us_west_kafka_sink = get_kafka_sink("order_events_us_west")
eu_central_kafka_sink = get_kafka_sink("order_events_eu_central")


us_east_ds = ds.filter(lambda row: row['region'] == 'us-east')
ap_south_ds = ds.filter(lambda row: row['region'] == 'ap-south')
us_west_ds = ds.filter(lambda row: row['region'] == 'us-west')
eu_central_ds = ds.filter(lambda row: row['region'] == 'eu-central')

# Convert each Row stream to a JSON string stream to match SimpleStringSchema in KafkaSink
# Route events by region to different sinks
us_east_ds.map(lambda row: json.dumps(row.as_dict(recursive=True)), Types.STRING()).sink_to(us_east_kafka_sink)
ap_south_ds.map(lambda row: json.dumps(row.as_dict(recursive=True)), Types.STRING()).sink_to(ap_south_kafka_sink)
us_west_ds.map(lambda row: json.dumps(row.as_dict(recursive=True)), Types.STRING()).sink_to(us_west_kafka_sink)
eu_central_ds.map(lambda row: json.dumps(row.as_dict(recursive=True)), Types.STRING()).sink_to(eu_central_kafka_sink)


### File Sink ###

# file_sink = FileSink.for_row_format(
#     '/opt/flink/jobs/30-Day-Flink/output',
#     Encoder.simple_string_encoder()
# ).build()

# json_output_stream = ds.map(
#     lambda row: json.dumps(row.as_dict(recursive=True)),
#     Types.STRING()
# )

# json_output_stream.sink_to(file_sink)

####



env.execute("kafka-source")
