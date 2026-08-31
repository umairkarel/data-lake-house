from apache_beam import pvalue
from pyflink.datastream.functions import RuntimeContext
from pyflink.datastream.functions import ProcessFunction
from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, KafkaOffsetsInitializer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common import Types, WatermarkStrategy

## OutputTag Patch
##############################################
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
##############################################


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


us_east_tag = OutputTag("us-east-tag", event_schema)
ap_south_tag = OutputTag("ap-south-tag", event_schema)
us_west_tag = OutputTag("us-west-tag", event_schema)
eu_central_tag = OutputTag("eu-central-tag", event_schema)


env = StreamExecutionEnvironment.get_execution_environment()

kafka_source = KafkaSource.builder() \
                .set_bootstrap_servers("kafka:9092") \
                .set_topics("order_events") \
                .set_group_id("job07") \
                .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
                .set_value_only_deserializer(json_deserializer) \
                .build()

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

class ValidatingRouter(ProcessFunction):
    def __init__(self):
        self.high_value_evnt_cnt = None
        self.low_value_evnt_cnt = None

    def open(self, ctx: RuntimeContext):
        metrics = ctx.get_metrics_group()
        self.high_value_evnt_cnt = metrics.counter("high_value_records")
        self.low_value_evnt_cnt = metrics.counter("low_value_records")

    def process_element(self, row, ctx):
        if row["total_amount"] >= 100:
            self.high_value_evnt_cnt.inc()
        else:
            self.low_value_evnt_cnt.inc()
        
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


ds = env.from_source(
    kafka_source,
    WatermarkStrategy.no_watermarks(),
    "Kafka Order Events"
)

main_stream = ds.process(
    ValidatingRouter(),
    output_type=event_schema
)

us_east_ds = main_stream.get_side_output(us_east_tag)
ap_south_ds = main_stream.get_side_output(ap_south_tag)
us_west_ds = main_stream.get_side_output(us_west_tag)
eu_central_ds = main_stream.get_side_output(eu_central_tag)


us_east_sink = get_kafka_sink("order_events_us-east")
ap_south_sink = get_kafka_sink("order_events_ap-south")
us_west_sink = get_kafka_sink("order_events_us-west")
eu_central_sink = get_kafka_sink("order_events_eu-central")


us_east_ds.sink_to(us_east_sink).name("US East Kafka Sink")
ap_south_ds.sink_to(ap_south_sink).name("AP South Kafka Sink")
us_west_ds.sink_to(us_west_sink).name("US West Kafka Sink")
eu_central_ds.sink_to(eu_central_sink).name("EU Central Kafka Sink")



env.execute("Week 1 - Mini Proj")

