from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaSink,
    KafkaRecordSerializationSchema,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.formats.json import (
    JsonRowDeserializationSchema,
    JsonRowSerializationSchema,
)
from pyflink.common import Types, WatermarkStrategy


env = StreamExecutionEnvironment.get_execution_environment()


# ---------------------------------------------------------
# 1. Define the event schema
# ---------------------------------------------------------

event_schema = Types.ROW_NAMED(
    [
        "event_id",
        "order_id",
        "user_id",
        "product_id",
        "category",
        "status",
        "quantity",
        "unit_price",
        "total_amount",
        "discount_pct",
        "region",
        "platform",
        "event_time",
    ],
    [
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.INT(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
    ],
)


# ---------------------------------------------------------
# 2. JSON deserializer for Kafka source
# ---------------------------------------------------------

json_deserializer = (
    JsonRowDeserializationSchema.builder()
    .type_info(event_schema)
    .build()
)


# ---------------------------------------------------------
# 3. Kafka source
# ---------------------------------------------------------

kafka_src = (
    KafkaSource.builder()
    .set_bootstrap_servers("kafka:9092")
    .set_topics("order_events")
    .set_group_id("flink-kafka-source")
    .set_starting_offsets(KafkaOffsetsInitializer.latest())
    .set_value_only_deserializer(json_deserializer)
    .build()
)


ds = env.from_source(
    kafka_src,
    WatermarkStrategy.no_watermarks(),
    "Kafka Order Events",
)


# ---------------------------------------------------------
# 4. JSON serializer for Kafka sink
# ---------------------------------------------------------

json_serializer = (
    JsonRowSerializationSchema.builder()
    .with_type_info(event_schema)
    .build()
)


# ---------------------------------------------------------
# 5. Dynamic topic selection
# ---------------------------------------------------------

def select_topic(row):
    region = row["region"]

    return f"order_events_{region}"


# ---------------------------------------------------------
# 6. ONE Kafka sink
# ---------------------------------------------------------

kafka_sink = (
    KafkaSink.builder()
    .set_bootstrap_servers("kafka:9092")
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic_selector(select_topic)
        .set_value_serialization_schema(json_serializer)
        .build()
    )
    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    .build()
)


# ---------------------------------------------------------
# 7. Write to dynamically selected topic
# ---------------------------------------------------------

ds.sink_to(kafka_sink).name("Dynamic Region Kafka Sink")


# ---------------------------------------------------------
# 8. Execute
# ---------------------------------------------------------

env.execute("kafka-order-routing")
