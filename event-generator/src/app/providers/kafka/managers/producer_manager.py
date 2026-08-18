from functools import (
    cached_property,
)
from json import dumps

from kafka import KafkaProducer

from app.abstracts.producer_manger import AbstractProducerManager


DEFAULT_ENCODING = "utf-8"


class KafkaProducerSingleton:
    _producer_instance = {}

    def __new__(
        cls,
        bootstrap_servers,
        value_serializer,
        key_serializer,
    ):
        if bootstrap_servers not in cls._producer_instance:
            producer_instance = super(KafkaProducerSingleton, cls).__new__(cls)
            producer_instance._initialize(
                bootstrap_servers=bootstrap_servers,
                value_serializer=value_serializer,
                key_serializer=key_serializer,
            )
            cls._producer_instance[bootstrap_servers] = producer_instance
        return cls._producer_instance[bootstrap_servers]

    def _initialize(
        self,
        bootstrap_servers,
        value_serializer,
        key_serializer,
    ):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=value_serializer,
            key_serializer=key_serializer,
        )


class KafkaProducerManager(AbstractProducerManager):
    def __init__(
        self,
        bootstrap_servers,
        encoding=DEFAULT_ENCODING,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.encoding = encoding

    @cached_property
    def admin_producer(self):
        return KafkaProducerSingleton(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: dumps(v).encode(self.encoding),
            key_serializer=lambda v: v.encode(self.encoding)
            if v is not None
            else None,
        ).producer

    def publish_msg(
        self,
        topic,
        value,
        key=None,
        headers=None,
        partition=None,
        timestamp_ms=None,
    ):
        self.admin_producer.send(
            topic,
            value=value,
            key=key,
            headers=headers,
            partition=partition,
            timestamp_ms=timestamp_ms,
        )

    def flush_msg(self):
        self.admin_producer.flush()

    def close(self):
        if hasattr(self, "producer"):
            self.producer.flush()
            self.producer.close()
