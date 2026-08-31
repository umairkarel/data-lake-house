import json
from typing import Tuple, Iterable
from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.functions import AggregateFunction
from pyflink.datastream.window import TumblingEventTimeWindows, ProcessWindowFunction
from pyflink.datastream.connectors.file_system import FileSink
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Types, Time, Duration, Encoder, Row

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

class JsonEventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, event, record_ts):
        from datetime import datetime
        try:
            time_str = event[12] if isinstance(event, tuple) else event["event_time"]
            dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S.%f")
        except ValueError:
            dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return record_ts

        return int(dt.timestamp() * 1000)

env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(10000) # Checkpoint every 10 seconds

kafka_source = KafkaSource.builder() \
                .set_bootstrap_servers("kafka:9092") \
                .set_topics("order_events") \
                .set_group_id("week2-final") \
                .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
                .set_value_only_deserializer(json_deserializer) \
                .build()

# 1 Minute out-of-orderness
watermark_strategy = WatermarkStrategy \
                    .for_bounded_out_of_orderness(Duration.of_minutes(1)) \
                    .with_timestamp_assigner(JsonEventTimestampAssigner())

ds = env.from_source(
    kafka_source,
    WatermarkStrategy.no_watermarks(),
    "Kafka Order Events"
)

ds = ds.assign_timestamps_and_watermarks(watermark_strategy)

# ==============================================================================
# WEEK 2 FINAL CHALLENGE
# ==============================================================================

class SalesVolumeAggregator(AggregateFunction):
    def create_accumulator(self) -> float:
        return 0.0
    
    def add(self, value: Tuple[str, float, str], accumulator: float) -> float:
        return accumulator + value[1]
        
    def get_result(self, accumulator: float) -> float:
        return accumulator
        
    def merge(self, a: float, b: float) -> float:
        return a + b

class WindowMetadataProcessFunction(ProcessWindowFunction):
    def process(self, key: str, context: 'ProcessWindowFunction.Context', elements: Iterable[float]) -> Iterable[Tuple[str, float, int, int]]:
        window_start = context.window().start
        window_end = context.window().end
        total_volume = next(iter(elements))
        yield (key, total_volume, window_start, window_end)

late_data_tag = OutputTag("late-data", Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.STRING()]))

mapped_ds = ds.map(
    lambda event: (event[4], event[8], event[12]), # category, total_amount, event_time
    output_type=Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.STRING()]) 
)

windowed_stream = (
    mapped_ds
    .key_by(lambda event: event[0]) 
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .allowed_lateness(Time.minutes(10))
    .side_output_late_data(late_data_tag)
    .aggregate(SalesVolumeAggregator(),
               accumulator_type=Types.DOUBLE(),
               output_type=Types.DOUBLE(),
               window_function=WindowMetadataProcessFunction(),
               window_output_type=Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.LONG(), Types.LONG()]))
)

# SINK 1: Main output to FileSink
file_sink = FileSink.for_row_format(
    '/opt/flink/jobs/30-Day-Flink/output/week2',
    Encoder.simple_string_encoder()
).build()

windowed_stream.sink_to(file_sink)

# SINK 2: Late output to Kafka DLQ
late_stream = windowed_stream.get_side_output(late_data_tag)

def to_json_str(late_tuple):
    # late_tuple: (category, total_amount, event_time)
    return json.dumps({
        "category": late_tuple[0],
        "total_amount": late_tuple[1],
        "event_time": late_tuple[2],
        "status": "LATE_REJECTED"
    })

json_late_stream = late_stream.map(to_json_str, output_type=Types.STRING())

kafka_sink = KafkaSink.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("order_events_late_dlq")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
    ) \
    .build()

json_late_stream.sink_to(kafka_sink)

env.execute("Week 2 Final Challenge")
