from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink
from pyflink.datastream.functions import MapFunction, RuntimeContext
from pyflink.common import Types, Encoder

env = StreamExecutionEnvironment.get_execution_environment()


class UserInfoMap(MapFunction):
    def open(self, runtime_context: RuntimeContext):
        self.users_info = {
            101: {'name': 'Umair', 'city': 'Mangaon'},
            102: {'name': 'Affan', 'city': 'Delhi'},
        }

    def map(self, transaction):
        user_info = self.users_info.get(transaction[0], {'name': 'Unknown', 'city': 'Unknown'})
        return transaction[0], user_info['name'], user_info['city'], transaction[1]

    def close(self):
        self.users_info.clear()

user_transactions = [
    (101, 24.5),
    (102, 36.2),
    (103, 30.1)
]

ds = env.from_collection(
    user_transactions,
    type_info=Types.ROW([Types.LONG(), Types.DOUBLE()])
)

enrich = ds.map(
    UserInfoMap(),
    Types.TUPLE([Types.LONG(), Types.STRING(), Types.STRING(), Types.DOUBLE()])
)

output_stream = enrich.map(
    lambda x: f"{x[0]},{x[1]},{x[2]},{x[3]}",
    Types.STRING()
)


file_sink = FileSink.for_row_format(
    '/opt/flink/jobs/30-Day-Flink/output',
    Encoder.simple_string_encoder()
).build()


output_stream.sink_to(file_sink)

# Submit DAG
env.execute()
