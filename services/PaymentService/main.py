import logging
import os
import random
import time
import uuid
import redis

from prometheus_client import Counter, Histogram, start_http_server
from producer import KafkaProducer
from consumer import KafkaConsumer
from schemas.base import EventEnvelope, EventType
from schemas.payment_events import (
    make_payment_charged,
    make_payment_failed,
    make_payment_cancellation_charged,
)

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_URL     = os.getenv("REDIS_URL", "redis://localhost:6379")
SERVICE_NAME  = os.getenv("SERVICE_NAME", "payment-service")

# ─── Prometheus Metrics ───────────────────────────────────────────────────────
payments_charged      = Counter("payments_charged_total",       "Total successful payments")
payments_failed       = Counter("payments_failed_total",        "Total failed payments")
cancellation_fees     = Counter("cancellation_fees_total",      "Total cancellation fees charged")
payment_latency       = Histogram("payment_duration_seconds",   "Payment processing latency")

# ─── Cancellation Fee Rules ───────────────────────────────────────────────────
CANCELLATION_FEES = {
    "before_match":  0.00,   # rider cancelled before any driver found
    "after_match":   2.50,   # driver was dispatched
    "after_pickup":  5.00,   # driver was en route or arrived
}

# ─── Clients ──────────────────────────────────────────────────────────────────
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

producer = KafkaProducer(KAFKA_SERVERS)
consumer = KafkaConsumer(
    bootstrap_servers=KAFKA_SERVERS,
    group_id="payment-service-group",
    topics=[
        EventType.RIDE_COMPLETED,
        EventType.RIDE_CANCELLED_BY_RIDER,
        EventType.RIDE_CANCELLED_BY_DRIVER,
    ],
)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def is_already_processed(event_id: str) -> bool:
    key = f"payment-service:processed:{event_id}"
    return not redis_client.set(key, "1", ex=86400, nx=True)


def resolve_simultaneous_cancel(cid: str) -> bool:
    """
    Atomic Redis lock for simultaneous rider + driver cancellation.
    Returns True if payment service should process this event (first to arrive).
    Returns False if the other cancel event already claimed processing rights.
    """
    lock_key = f"payment-service:cancel_lock:{cid}"
    return bool(redis_client.set(lock_key, "1", ex=86400, nx=True))


def simulate_payment(amount_usd: float) -> tuple[bool, str]:
    """
    Simulates calling a payment gateway.
    Returns (success, reason).
    In production this would be Stripe / Braintree / etc.
    """
    time.sleep(random.uniform(0.1, 0.4))   # simulate network latency

    # 95% success rate
    if random.random() < 0.95:
        return True, "ok"

    reason = random.choice([
        "insufficient_funds",
        "card_declined",
        "timeout",
    ])
    return False, reason


# ─── Event Handlers ───────────────────────────────────────────────────────────

def handle_ride_completed(event: EventEnvelope) -> None:
    cid       = event.correlation_id
    data      = event.data
    rider_id  = data["rider_id"]
    driver_id = data["driver_id"]
    fare_usd  = data["fare_usd"]
    ride_id   = cid   # use correlation_id as ride_id

    start = time.time()
    logger.info(f"Processing payment | cid={cid} fare=${fare_usd}")

    success, reason = simulate_payment(fare_usd)

    if success:
        payment_id = f"pay_{uuid.uuid4().hex[:10]}"
        charged_event = make_payment_charged(
            correlation_id=cid,
            rider_id=rider_id,
            driver_id=driver_id,
            amount_usd=fare_usd,
            payment_id=payment_id,
            ride_id=ride_id,
        )
        producer.publish(EventType.PAYMENT_CHARGED, charged_event, key=rider_id)
        payments_charged.inc()
        logger.info(f"Payment charged | cid={cid} amount=${fare_usd} payment_id={payment_id}")

    else:
        failed_event = make_payment_failed(
            correlation_id=cid,
            rider_id=rider_id,
            ride_id=ride_id,
            reason=reason,
            amount_usd=fare_usd,
        )
        producer.publish(EventType.PAYMENT_FAILED, failed_event, key=rider_id)

        # Also send to DLQ for manual review
        producer.publish(EventType.PAYMENT_DLQ, failed_event, key=rider_id)
        payments_failed.inc()
        logger.warning(f"Payment failed | cid={cid} reason={reason}")

    payment_latency.observe(time.time() - start)


def handle_cancellation(event: EventEnvelope, cancelled_by: str) -> None:
    cid      = event.correlation_id
    data     = event.data
    rider_id = data["rider_id"]

    # ── Simultaneous cancellation resolution ─────────────────────────────────
    # Both rider and driver cancelled at the same time.
    # Only the first event to arrive processes — second is dropped cleanly.
    won_lock = resolve_simultaneous_cancel(cid)

    if not won_lock:
        logger.info(
            f"Simultaneous cancel — already processed by other party | "
            f"cid={cid} cancelled_by={cancelled_by} → no fee charged"
        )
        return

    # ── Driver cancellation → no fee ever ────────────────────────────────────
    if cancelled_by == "driver":
        logger.info(f"Driver cancelled — no fee charged | cid={cid}")
        return

    # ── Rider cancellation → apply fee based on stage ────────────────────────
    cancel_stage = data.get("cancel_stage", "before_match")
    fee          = CANCELLATION_FEES.get(cancel_stage, 0.00)

    if fee == 0.00:
        logger.info(f"Rider cancelled before match — no fee | cid={cid}")
        return

    start   = time.time()
    success, reason = simulate_payment(fee)

    if success:
        cancel_event = make_payment_cancellation_charged(
            correlation_id=cid,
            rider_id=rider_id,
            ride_id=cid,
            amount_usd=fee,
            cancel_stage=cancel_stage,
            cancelled_by=cancelled_by,
        )
        producer.publish(
            EventType.PAYMENT_CANCELLATION_CHARGED,
            cancel_event,
            key=rider_id,
        )
        cancellation_fees.inc()
        logger.info(
            f"Cancellation fee charged | cid={cid} "
            f"stage={cancel_stage} fee=${fee}"
        )
    else:
        # Cancellation fee failed — log and move on, don't block the flow
        logger.warning(
            f"Cancellation fee failed | cid={cid} "
            f"stage={cancel_stage} fee=${fee} reason={reason}"
        )

    payment_latency.observe(time.time() - start)


# ─── Main Consumer Loop ───────────────────────────────────────────────────────

def run():
    logger.info(f"{SERVICE_NAME} started. Listening for events...")
    start_http_server(8000)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            try:
                event = EventEnvelope.from_kafka(msg.value())

                if is_already_processed(event.event_id):
                    logger.info(f"Duplicate skipped | event_id={event.event_id}")
                    consumer.commit(msg)
                    continue

                logger.info(
                    f"Event received | type={event.event_type} "
                    f"cid={event.correlation_id}"
                )

                if event.event_type == EventType.RIDE_COMPLETED:
                    handle_ride_completed(event)

                elif event.event_type == EventType.RIDE_CANCELLED_BY_RIDER:
                    handle_cancellation(event, cancelled_by="rider")

                elif event.event_type == EventType.RIDE_CANCELLED_BY_DRIVER:
                    handle_cancellation(event, cancelled_by="driver")

                consumer.commit(msg)

            except Exception as e:
                logger.error(f"Failed to process event | error={e}", exc_info=True)
                # No commit — Kafka redelivers

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    run()