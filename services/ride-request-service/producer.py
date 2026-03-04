import logging
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient
from schemas.base import EventEnvelope

logger = logging.getLogger(__name__)


class KafkaProducer:
    def __init__(self, bootstrap_servers: str):
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",               # wait for all replicas to confirm
            "retries": 5,                # retry on transient failures
            "retry.backoff.ms": 300,
            "enable.idempotence": True,  # exactly-once delivery guarantee
            "compression.type": "snappy",
        })

    def _delivery_report(self, err, msg):
        if err:
            logger.error(f"Delivery failed | topic={msg.topic()} error={err}")
        else:
            logger.info(
                f"Event delivered | topic={msg.topic()} "
                f"partition={msg.partition()} offset={msg.offset()}"
            )

    def publish(self, topic: str, event: EventEnvelope, key: str | None = None) -> None:
        """
        Publish a single event to a Kafka topic.
        Key is used for partitioning — always pass rider_id so all
        events for the same rider land on the same partition.
        """
        try:
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8") if key else None,
                value=event.to_kafka(),
                on_delivery=self._delivery_report,
            )
            self._producer.poll(0)   # trigger delivery callbacks non-blocking
        except KafkaException as e:
            logger.error(f"Failed to publish event | topic={topic} error={e}")
            raise

    def flush(self) -> None:
        """Wait for all pending messages to be delivered."""
        self._producer.flush()