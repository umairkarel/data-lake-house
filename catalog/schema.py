"""
schema.py
---------
Central place to define all Iceberg table schemas used in this lakehouse.

The msgGeneratorKafka produces messages with fields of types:
  STRING, INTEGER, FLOAT, DATETIME

We define a BenchmarkEvent schema that mirrors those field types.
Add more schemas here as the project grows.
"""

from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    LongType,
    FloatType,
    TimestampType,
)
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.partitioning import PartitionSpec, PartitionField


# ---------------------------------------------------------------------------
# Schema: benchmark_events
# Matches the default schema that msgGeneratorKafka generates
# Fields: id (STRING), value (INTEGER), amount (FLOAT), event_time (DATETIME)
# ---------------------------------------------------------------------------
BENCHMARK_EVENTS_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=StringType(), required=True),
    NestedField(field_id=2, name="value", field_type=LongType(), required=False),
    NestedField(field_id=3, name="amount", field_type=FloatType(), required=False),
    NestedField(field_id=4, name="event_time", field_type=TimestampType(), required=False),
    NestedField(field_id=5, name="ingestion_time", field_type=TimestampType(), required=False),
)

# Partition by day on event_time (keeps files manageable, enables time-based pruning)
BENCHMARK_EVENTS_PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=4,           # event_time
        field_id=1000,
        transform=DayTransform(),
        name="event_day",
    )
)

# ---------------------------------------------------------------------------
# Helper: Flink-compatible DDL type string (used in SQL DDL jobs)
# ---------------------------------------------------------------------------
BENCHMARK_EVENTS_FLINK_DDL_FIELDS = """
    id          STRING          NOT NULL,
    value       BIGINT,
    amount      FLOAT,
    event_time  TIMESTAMP(6),
    ingestion_time TIMESTAMP(6)
"""

BENCHMARK_EVENTS_FLINK_PARTITION = "PARTITIONED BY (day(event_time))"
