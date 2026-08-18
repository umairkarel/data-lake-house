from functools import (
    cached_property,
)

from kafka.admin import (
    KafkaAdminClient,
    NewTopic,
)
from kafka.errors import (
    TopicAlreadyExistsError,
)


class KafkaClientSingleton:
    _client_instance = {}

    def __new__(cls, bootstrap_servers):
        if bootstrap_servers not in cls._client_instance:
            kafka_client = super(KafkaClientSingleton, cls).__new__(cls)
            kafka_client._initialize(bootstrap_servers=bootstrap_servers)
            cls._client_instance[bootstrap_servers] = kafka_client
        return cls._client_instance[bootstrap_servers]

    def _initialize(self, bootstrap_servers):
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers
        )

    def close(self):
        if self.admin_client:
            self.admin_client.close()


class KafkaClientManager:
    def __init__(self, bootstrap_servers):
        self.bootstrap_servers = bootstrap_servers

    @cached_property
    def admin_client(self):
        return KafkaClientSingleton(
            bootstrap_servers=self.bootstrap_servers
        ).admin_client

    def create_topic(
        self,
        topic_name,
        num_partitions,
        replication_factor,
        timeout_ms,
    ):
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )

        try:
            return self.admin_client.create_topics(
                [topic], timeout_ms=timeout_ms
            )
        except TopicAlreadyExistsError:
            raise Exception(f"Topic {topic_name} already exists.")
        except Exception as exc:
            raise Exception(f"Failed to create topic {topic_name}: {exc}")

    def info(self, topic_name):
        try:
            return self.admin_client.describe_topics([topic_name])
        except Exception as exc:
            raise Exception(
                f"Failed to get info about topic {topic_name}: {exc}"
            )

    def close(self):
        if hasattr(self, "admin_client"):
            self.admin_client.close()

    def delete(self, topic_name):
        try:
            return self.admin_client.delete_topics([topic_name])
        except Exception as exc:
            raise Exception(
                f"Failed to delete about topic {topic_name}: {exc}"
            )
