from app.providers.kafka.managers.client_manager import (
    KafkaClientManager,
)


class TopicBuilder:
    __slots__ = [
        "topic_name",
        "bootstrap_servers",
        "num_partitions",
        "replication_factor",
        "timeout_ms",
    ]

    def set_topic_name(self, topic_name):
        setattr(self, "topic_name", topic_name)
        return self

    def set_bootstrap_servers(self, bootstrap_servers):
        setattr(
            self,
            "bootstrap_servers",
            bootstrap_servers,
        )
        return self

    def set_num_partitions(self, num_partitions=1):
        setattr(self, "num_partitions", num_partitions)
        return self

    def set_replication_factor(self, replication_factor=1):
        setattr(
            self,
            "replication_factor",
            replication_factor,
        )
        return self

    def set_timeout_ms(self, timeout_ms=10000):
        setattr(self, "timeout_ms", timeout_ms)
        return self

    def build(self):
        if not hasattr(self, "topic_name") or not hasattr(
            self, "bootstrap_servers"
        ):
            raise ValueError("Missing required attributes")

        topic = KafkaClientManager(
            bootstrap_servers=self.bootstrap_servers
        ).create_topic(
            topic_name=self.topic_name,
            num_partitions=self.num_partitions,
            replication_factor=self.replication_factor,
            timeout_ms=self.timeout_ms,
        )
        return topic

    def info(self):
        return KafkaClientManager(
            bootstrap_servers=self.bootstrap_servers
        ).info(
            topic_name=self.topic_name,
        )

    def delete(self) -> None:
        KafkaClientManager(bootstrap_servers=self.bootstrap_servers).delete(
            topic_name=self.topic_name
        )
