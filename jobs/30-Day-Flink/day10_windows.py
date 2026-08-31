from pyflink.datastream.functions import AggregateFunction
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.window import EventTimeSessionWindows
from pyflink.datastream.connectors.file_system import FileSink
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common import Types, Time, Duration, Encoder, Row
from typing import Tuple

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


env = StreamExecutionEnvironment.get_execution_environment()

# Checkpointing is MANDATORY for FileSink! 
# Files remain in an ".in-progress" state and are never finished/written until a checkpoint completes.
env.enable_checkpointing(10000) # Checkpoint every 10 seconds


kafka_source = KafkaSource.builder() \
                .set_bootstrap_servers("kafka:9092") \
                .set_topics("order_events") \
                .set_group_id("day10-windows") \
                .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
                .set_value_only_deserializer(json_deserializer) \
                .build()

watermark_strategy = WatermarkStrategy \
                    .for_bounded_out_of_orderness(Duration.of_seconds(50)) \
                    .with_timestamp_assigner(JsonEventTimestampAssigner())

# In PyFlink, Python TimestampAssigners CANNOT be passed to from_source()
# because the KafkaSource runs entirely in Java. We must read the stream first, 
# then apply the Python timestamp assigner to the DataStream!
ds = env.from_source(
    kafka_source,
    WatermarkStrategy.no_watermarks(), # No watermarks at the source level
    "Kafka Order Events"
)

# Apply the Python watermark strategy on the DataStream
ds = ds.assign_timestamps_and_watermarks(watermark_strategy)

def sliding_reduce_fn(e1, e2):
    # reduce MUST return the same type as input (13-column Row)
    e1["total_amount"] = e1["total_amount"] + e2["total_amount"]
    return e1

## ==============================================================================
## 1. SLIDING WINDOW
## ==============================================================================
# sliding_revenue = (
#     ds.key_by(lambda e: e["category"])
#     .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10)))
#     .reduce(sliding_reduce_fn)
#     .map(
#         lambda reduced: Row(category=reduced["category"], revenue=reduced["total_amount"]),
#         output_type=Types.ROW_NAMED(["category", "revenue"], [Types.STRING(), Types.DOUBLE()])
#     )
# )

## ==============================================================================
## 2. SESSION WINDOW - Event Counting
## ==============================================================================
# session_counts = (
#     ds.map(
#         lambda e: (e["user_id"], 1), 
#         output_type=Types.TUPLE([Types.STRING(), Types.INT()])
#     )
#     .key_by(lambda e: e[0]) # Group by user_id
#     .window(EventTimeSessionWindows.with_gap(Time.seconds(30)))
#     .reduce(lambda e1, e2: (e1[0], e1[1] + e2[1])) # e1[1] + e2[1] accumulates the count!
# )


## ==============================================================================
## 3. TUMBLING WINDOW
## ==============================================================================

class AverageAggregator(AggregateFunction):
    def create_accumulator(self) -> Tuple[str, float, float, str, str]:
        # Category, Sum, Count, Min Time, Max Time
        return ("", 0.0, 0.0, "", "")
    
    def add(self, value: Tuple[str, float, str], accumulator: Tuple[str, float, float, str, str]) -> Tuple[str, float, float, str, str]:
        category = value[0] if accumulator[0] == "" else accumulator[0]
        event_time = value[2]
        
        # Track Min
        min_time = event_time if accumulator[3] == "" or event_time < accumulator[3] else accumulator[3]
        # Track Max
        max_time = event_time if accumulator[4] == "" or event_time > accumulator[4] else accumulator[4]
        
        return (category, accumulator[1] + value[1], accumulator[2] + 1.0, min_time, max_time)

    def get_result(self, accumulator: Tuple[str, float, float, str, str]) -> Tuple[str, float, str, str]:
        if accumulator[2] == 0:
            return (accumulator[0], 0.0, accumulator[3], accumulator[4])
        return (accumulator[0], accumulator[1] / accumulator[2], accumulator[3], accumulator[4])
    
    def merge(self, a: Tuple[str, float, float, str, str], b: Tuple[str, float, float, str, str]) -> Tuple[str, float, float, str, str]:
        category = a[0] if a[0] != "" else b[0]
        min_time = a[3] if b[3] == "" or (a[3] != "" and a[3] < b[3]) else b[3]
        max_time = a[4] if b[4] == "" or (a[4] != "" and a[4] > b[4]) else b[4]
        return (category, a[1] + b[1], a[2] + b[2], min_time, max_time)


category_avg = (
    ds.map(
        lambda event: (event[4], event[8], event[12]),
        output_type=Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.STRING()]) 
    )
    .key_by(lambda event: event[0]) 
    .window(TumblingEventTimeWindows.of(Time.seconds(10)))
    .aggregate(AverageAggregator(),
               accumulator_type=Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), Types.STRING(), Types.STRING()]),
               output_type=Types.TUPLE([Types.STRING(), Types.DOUBLE(), Types.STRING(), Types.STRING()]))
)

file_sink = FileSink.for_row_format(
    '/opt/flink/jobs/30-Day-Flink/output/day10',
    Encoder.simple_string_encoder()
).build()

# sliding_revenue.sink_to(file_sink)
# session_counts.sink_to(file_sink)
category_avg.sink_to(file_sink)

env.execute("Day 10 All Windows Exercise")
