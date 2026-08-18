from abc import ABC, abstractmethod


class AbstractProducerManager(ABC):
    """
    Abstract base class for a producer interface.
    """

    @abstractmethod
    def publish_msg(
        self,
        topic,
        value,
        key=None,
        headers=None,
        partition=None,
        timestamp_ms=None,
    ):
        """
        Publish a message to a topic.
        """
        pass

    @abstractmethod
    def flush_msg(self):
        """
        Flush any outstanding messages.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Close the producer and release resources.
        """
        pass
