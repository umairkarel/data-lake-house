from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink
from pyflink.common import Types, Encoder

env = StreamExecutionEnvironment.get_execution_environment()

data = ["Hello this is Pyflink", "PyFlink is great", "this is a word this word is my great word"]
ds = env.from_collection(
    data,
    type_info=Types.STRING()
)


word_stream = ds.flat_map(lambda sentence: sentence.split(" "), Types.STRING())
words_map = word_stream.map(lambda word: (word, 1), Types.TUPLE([Types.STRING(), Types.LONG()]))
words = words_map.key_by(lambda x: x[0])
counts = words.sum(1)

# Define the file sink for text output
file_sink = FileSink.for_row_format(
    '/opt/flink/jobs/30-Day-Flink/output',
    Encoder.simple_string_encoder()
).build()

# Convert Tuple(String, Int) to String so the file sink can write it
output_stream = counts.map(
    lambda x: f"{x[0]}: {x[1]}",
    Types.STRING()
)
output_stream.sink_to(file_sink)

# Submit Job
env.execute()
