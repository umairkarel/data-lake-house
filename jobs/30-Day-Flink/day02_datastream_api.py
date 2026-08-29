from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.functions import MapFunction, RuntimeContext
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink
from pyflink.datastream.connectors.number_seq import NumberSequenceSource
from pyflink.table import WriteMode
from pyflink.common import Types, WatermarkStrategy, Row, Encoder


env = StreamExecutionEnvironment.get_execution_environment()

######### Ex. 1: Basics
# # DAG Creation
# ds = env.from_collection([1,12,5,7,13,9], Types.INT())
# # filter ds
# ds = ds.filter(lambda x: x > 10)

# # ds.print()


######### Ex. 2

class CountMap(MapFunction):
    def open(self, runtime_context: RuntimeContext):
        state_desc = ValueStateDescriptor('cnt', Types.PICKLED_BYTE_ARRAY())
        self.cnt_state = runtime_context.get_state(state_desc)
        # ValueState is normall for keyed state like in our case
    
    def map(self, value):
        cnt = self.cnt_state.value()
        if cnt is None or cnt < 2:
            self.cnt_state.update(1 if cnt is None else cnt + 1)
            return value[0], value[1] + 1
        else:
            return value[0], value[1]

num_seq = NumberSequenceSource(1,10000)
ds = env.from_source(
    source=num_seq,
    watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
    source_name='source__num_seq',
    type_info=Types.LONG()
)

ds = ds.map(lambda x: Row(x % 4, 1), output_type=Types.ROW([Types.LONG(), Types.LONG()])) \
        .key_by(lambda x: x[0]) \
        .map(CountMap(), Types.TUPLE([Types.LONG(), Types.LONG()]))


# Define the file sink for text output
file_sink = FileSink.for_row_format(
    '/opt/flink/jobs/30-Day-Flink/output',
    Encoder.simple_string_encoder()
).build()

ds.sink_to(file_sink)

# Execution
env.execute("Pyflink CustomMap State")
