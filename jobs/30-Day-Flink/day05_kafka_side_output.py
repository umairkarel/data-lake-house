from pyflink.datastream import StreamExecutionEnvironment, OutputTag
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
from pyflink.datastream.functions import ProcessFunction
from pyflink.common import Types, WatermarkStrategy

# ---------------------------------------------------------
# Monkey-patch OutputTag to recursively clear Java type info references.
# This prevents Flink from crashing with "TypeError: cannot pickle '_thread.RLock' object"
# due to nested type information fields holding Java object references in Python.
# ---------------------------------------------------------
def patched_getstate(self):
    def clear_j_typeinfo(ti):
        if hasattr(ti, "_j_typeinfo"):
            ti._j_typeinfo = None
        if hasattr(ti, "_field_types"):
            for ft in ti._field_types:
                clear_j_typeinfo(ft)
    clear_j_typeinfo(self.type_info)
    return self.tag_id, self.type_info

OutputTag.__getstate__ = patched_getstate

# =========================================================
# 1. Environment
# =========================================================
env = StreamExecutionEnvironment.get_execution_environment()

# =========================================================
# 2. Event schema
# =========================================================
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

# =========================================================
# 3. Side Output Tags
# =========================================================
# We must pass event_schema here so that Flink knows the types
# of records on the side output streams when writing to Kafka.
# The monkey-patch at the top handles the pickling serialization issue.
us_east_tag = OutputTag("us-east-tag", event_schema)
ap_south_tag = OutputTag("ap-south-tag", event_schema)
us_west_tag = OutputTag("us-west-tag", event_schema)
eu_central_tag = OutputTag("eu-central-tag", event_schema)

# =========================================================
# 4. JSON Deserializer
# =========================================================
json_deserializer = (
    JsonRowDeserializationSchema.builder()
    .type_info(event_schema)
    .build()
)

# =========================================================
# 5. Kafka Source
# =========================================================
kafka_src = (
    KafkaSource.builder()
    .set_bootstrap_servers("kafka:9092")
    .set_topics("order_events")
    .set_group_id("flink-kafka-source")
    .set_starting_offsets(KafkaOffsetsInitializer.latest())
    .set_value_only_deserializer(json_deserializer)
    .build()
)

# =========================================================
# 6. Source DataStream
# =========================================================
ds = env.from_source(
    kafka_src,
    WatermarkStrategy.no_watermarks(),
    "Kafka Order Events",
)

# =========================================================
# 7. Region Router
# =========================================================
class RegionSplitter(ProcessFunction):
    def process_element(self, row, ctx):
        region = row["region"]

        # Note: In PyFlink, we emit to side outputs by yielding:
        # yield output_tag, value
        if region == "us-east":
            yield us_east_tag, row
        elif region == "ap-south":
            yield ap_south_tag, row
        elif region == "us-west":
            yield us_west_tag, row
        elif region == "eu-central":
            yield eu_central_tag, row

# =========================================================
# 8. Run Region Splitter
# =========================================================
main_stream = ds.process(
    RegionSplitter(),
    output_type=event_schema
)

# =========================================================
# 9. Extract Side Outputs
# =========================================================
us_east_ds = main_stream.get_side_output(us_east_tag)
ap_south_ds = main_stream.get_side_output(ap_south_tag)
us_west_ds = main_stream.get_side_output(us_west_tag)
eu_central_ds = main_stream.get_side_output(eu_central_tag)

# =========================================================
# 10. JSON Serializer
# =========================================================
json_serializer = (
    JsonRowSerializationSchema.builder()
    .with_type_info(event_schema)
    .build()
)

# =========================================================
# 11. Kafka Sink Factory
# =========================================================
def get_kafka_sink(topic):
    record_serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(topic)
        .set_value_serialization_schema(json_serializer)
        .build()
    )
    return (
        KafkaSink.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_record_serializer(record_serializer)
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )

# =========================================================
# 12. Create Kafka Sinks
# =========================================================
us_east_sink = get_kafka_sink("order_events_us-east")
ap_south_sink = get_kafka_sink("order_events_ap-south")
us_west_sink = get_kafka_sink("order_events_us-west")
eu_central_sink = get_kafka_sink("order_events_eu-central")

# =========================================================
# 13. Connect Side Outputs to Kafka Sinks
# =========================================================
us_east_ds.sink_to(us_east_sink).name("US East Kafka Sink")
ap_south_ds.sink_to(ap_south_sink).name("AP South Kafka Sink")
us_west_ds.sink_to(us_west_sink).name("US West Kafka Sink")
eu_central_ds.sink_to(eu_central_sink).name("EU Central Kafka Sink")

# =========================================================
# 14. Execute
# =========================================================
env.execute("kafka-side-output-routing")
