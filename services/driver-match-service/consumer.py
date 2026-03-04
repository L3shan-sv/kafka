import logging
from confluent_kafka import Consumer, KafkaError, KafkaException

logger = logging.getLogger(__name__)


class KafkaConsumer:
    def __init__(self, bootstrap_servers: str, group_id: str, topics: list[str]):
        self._consumer = Consumer({
            "bootstrap.servers":        bootstrap_servers,
            "group.id":                 group_id,
            "auto.offset.reset":        "earliest",   # never miss an event
            "enable.auto.commit":       False,         # manual commit only
            "max.poll.interval.ms":     300000,
            "session.timeout.ms":       30000,
            "heartbeat.interval.ms":    10000,
        })
        self._consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {topics} | group={group_id}")

    def poll(self, timeout: float = 1.0):
        """
        Poll for a single message.
        Returns the raw message or None.
        Caller is responsible for committing after successful processing.
        """
        msg = self._consumer.poll(timeout)

        if msg is None:
            return None

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return None
            raise KafkaException(msg.error())

        return msg

    def commit(self, msg) -> None:
        """Commit offset only after successful processing — at-least-once guarantee."""
        self._consumer.commit(message=msg, asynchronous=False)

    def close(self) -> None:
        self._consumer.close()
        logger.info("Kafka consumer closed.")