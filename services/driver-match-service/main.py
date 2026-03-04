import asyncio
import logging
import os
import random
import time
import uuid
import threading
import redis

from prometheus_client import Counter, Histogram, start_http_server
from producer import KafkaProducer
from consumer import KafkaConsumer
from schemas.base import EventEnvelope, EventType, RideState
from schemas.ride_events import (
    make_ride_driver_matched,
    make_ride_driver_accepted,
    make_ride_driver_declined,
    make_ride_started,
    make_ride_completed,
    make_ride_cancelled_by_driver,
    make_ride_dlq,
)

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
KAFKA_SERVERS     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_URL         = os.getenv("REDIS_URL", "redis://localhost:6379")
SERVICE_NAME      = os.getenv("SERVICE_NAME", "driver-match-service")
MAX_RETRIES       = int(os.getenv("MAX_DRIVER_RETRIES", 5))

# ─── Prometheus Metrics ───────────────────────────────────────────────────────
drivers_matched   = Counter("drivers_matched_total",   "Total successful driver matches")
drivers_declined  = Counter("drivers_declined_total",  "Total driver declines")
rides_completed   = Counter("rides_completed_total",   "Total rides completed")
rides_dlq         = Counter("rides_dlq_total",         "Total rides sent to DLQ")
rides_cancelled   = Counter("rides_cancelled_total",   "Total rides cancelled by driver")
match_latency     = Histogram("driver_match_duration_seconds", "Time to match a driver")

# ─── Redis Client ─────────────────────────────────────────────────────────────
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# ─── Kafka Clients ────────────────────────────────────────────────────────────
producer = KafkaProducer(KAFKA_SERVERS)
consumer = KafkaConsumer(
    bootstrap_servers=KAFKA_SERVERS,
    group_id="driver-match-service-group",
    topics=[
        EventType.RIDE_REQUESTED,
        EventType.RIDE_DRIVER_DECLINED,  # re-queued rides come back here
    ],
)


# ─── State Helpers ────────────────────────────────────────────────────────────

def get_ride_state(cid: str) -> str | None:
    return redis_client.get(f"ride:{cid}:state")

def set_ride_state(cid: str, state: RideState) -> None:
    redis_client.set(f"ride:{cid}:state", state.value, ex=86400)

def is_already_processed(event_id: str) -> bool:
    """Idempotency check — have we already handled this event?"""
    key = f"driver-match-service:processed:{event_id}"
    # SET NX returns True if key was set (first time), False if already existed
    return not redis_client.set(key, "1", ex=86400, nx=True)

def lock_cancellation(cid: str) -> bool:
    """
    Atomic Redis lock for simultaneous cancellations.
    Returns True if this service wins the lock (should process).
    Returns False if already cancelled by rider.
    """
    locked = redis_client.set(
        f"ride:{cid}:state",
        RideState.CANCELLED.value,
        ex=86400,
        nx=True,
    )
    return bool(locked)


# ─── Driver Simulation ────────────────────────────────────────────────────────

def find_nearby_driver() -> dict | None:
    """
    Simulates querying a driver pool.
    In production this would hit a geo-index (e.g. Redis GEOSEARCH).
    Returns a fake driver or None if no drivers available.
    """
    # 80% chance a driver is available
    if random.random() < 0.8:
        return {
            "driver_id": f"drv_{uuid.uuid4().hex[:8]}",
            "eta_mins":  random.randint(2, 10),
        }
    return None


def driver_accepts() -> bool:
    """Simulates driver accept/decline decision. 75% accept rate."""
    return random.random() < 0.75


def simulate_ride(correlation_id: str, rider_id: str,
                  driver_id: str, pickup: dict, dropoff: dict) -> None:
    """
    Simulates a full ride lifecycle after driver accepts.
    Publishes ride.started → ride.completed (or cancelled by driver).
    In production, these events would come from the driver's mobile app.
    """
    time.sleep(2)  # simulate driver en route

    # 10% chance driver cancels after accepting
    if random.random() < 0.10:
        # Attempt to lock cancellation state
        won_lock = lock_cancellation(correlation_id)

        event = make_ride_cancelled_by_driver(
            correlation_id=correlation_id,
            rider_id=rider_id,
            driver_id=driver_id,
            reason="driver_emergency",
        )
        producer.publish(EventType.RIDE_CANCELLED_BY_DRIVER, event, key=rider_id)
        rides_cancelled.inc()

        if not won_lock:
            logger.warning(
                f"Simultaneous cancel resolved | correlation_id={correlation_id} "
                f"driver={driver_id} — rider cancelled first"
            )
        else:
            logger.info(f"Driver cancelled ride | correlation_id={correlation_id}")
        return

    # Publish ride.started
    started_event = make_ride_started(
        correlation_id=correlation_id,
        rider_id=rider_id,
        driver_id=driver_id,
        pickup=pickup,
    )
    set_ride_state(correlation_id, RideState.STARTED)
    producer.publish(EventType.RIDE_STARTED, started_event, key=rider_id)
    logger.info(f"Ride started | correlation_id={correlation_id}")

    time.sleep(3)  # simulate ride in progress

    # Publish ride.completed
    distance_km   = round(random.uniform(2.0, 25.0), 2)
    duration_mins = random.randint(5, 45)
    fare_usd      = round(2.50 + (distance_km * 1.20) + (duration_mins * 0.25), 2)

    completed_event = make_ride_completed(
        correlation_id=correlation_id,
        rider_id=rider_id,
        driver_id=driver_id,
        pickup=pickup,
        dropoff=dropoff,
        distance_km=distance_km,
        duration_mins=duration_mins,
        fare_usd=fare_usd,
    )
    set_ride_state(correlation_id, RideState.COMPLETED)
    producer.publish(EventType.RIDE_COMPLETED, completed_event, key=rider_id)
    rides_completed.inc()
    logger.info(
        f"Ride completed | correlation_id={correlation_id} "
        f"fare=${fare_usd} distance={distance_km}km"
    )


# ─── Event Handlers ───────────────────────────────────────────────────────────

def handle_ride_requested(event: EventEnvelope) -> None:
    cid        = event.correlation_id
    data       = event.data
    rider_id   = data["rider_id"]
    retry      = event.retry_count
    pickup     = data["pickup"]
    dropoff    = data["dropoff"]

    # Check if ride was already cancelled by rider before we matched
    current_state = get_ride_state(cid)
    if current_state == RideState.CANCELLED:
        logger.info(f"Ride already cancelled, skipping match | cid={cid}")
        return

    # DLQ if too many retries
    if retry >= MAX_RETRIES:
        dlq_event = make_ride_dlq(
            correlation_id=cid,
            rider_id=rider_id,
            retry_count=retry,
            original_event=data,
        )
        producer.publish(EventType.RIDE_DLQ, dlq_event, key=rider_id)
        rides_dlq.inc()
        logger.warning(f"Ride sent to DLQ | cid={cid} retries={retry}")
        return

    start = time.time()

    # Find a driver
    driver = find_nearby_driver()
    if not driver:
        # No drivers at all — send straight to DLQ
        dlq_event = make_ride_dlq(
            correlation_id=cid,
            rider_id=rider_id,
            retry_count=retry,
            original_event=data,
        )
        producer.publish(EventType.RIDE_DLQ, dlq_event, key=rider_id)
        rides_dlq.inc()
        logger.warning(f"No drivers available | cid={cid}")
        return

    driver_id = driver["driver_id"]
    eta_mins  = driver["eta_mins"]

    # Publish ride.driver_matched — offer sent to driver
    matched_event = make_ride_driver_matched(cid, rider_id, driver_id, eta_mins)
    set_ride_state(cid, RideState.MATCHED)
    producer.publish(EventType.RIDE_DRIVER_MATCHED, matched_event, key=rider_id)
    match_latency.observe(time.time() - start)

    # Simulate driver accept/decline decision
    time.sleep(1)

    if driver_accepts():
        accepted_event = make_ride_driver_accepted(cid, rider_id, driver_id, eta_mins)
        set_ride_state(cid, RideState.ACCEPTED)
        producer.publish(EventType.RIDE_DRIVER_ACCEPTED, accepted_event, key=rider_id)
        drivers_matched.inc()
        logger.info(f"Driver accepted | cid={cid} driver={driver_id}")

        # Run full ride simulation in background thread
        t = threading.Thread(
            target=simulate_ride,
            args=(cid, rider_id, driver_id, pickup, dropoff),
            daemon=True,
        )
        t.start()
    else:
        # Driver declined — re-queue with incremented retry_count
        declined_event = make_ride_driver_declined(
            correlation_id=cid,
            rider_id=rider_id,
            driver_id=driver_id,
            retry_count=retry + 1,
            reason="driver_declined",
        )
        producer.publish(EventType.RIDE_DRIVER_DECLINED, declined_event, key=rider_id)
        drivers_declined.inc()
        logger.info(f"Driver declined | cid={cid} driver={driver_id} retry={retry + 1}")


def handle_ride_driver_declined(event: EventEnvelope) -> None:
    """
    Driver declined — re-publish as ride.requested with incremented retry_count.
    This loops back into handle_ride_requested automatically.
    """
    cid      = event.correlation_id
    data     = event.data
    rider_id = data["rider_id"]
    retry    = event.retry_count

    logger.info(f"Re-queuing ride after decline | cid={cid} retry={retry}")

    # Rebuild ride.requested event with same correlation_id + incremented retry
    requeue_event = EventEnvelope(
        event_type=EventType.RIDE_REQUESTED,
        producer=SERVICE_NAME,
        correlation_id=cid,
        retry_count=retry,
        data={
            "rider_id":  rider_id,
            "pickup":    data.get("pickup",   {}),
            "dropoff":   data.get("dropoff",  {}),
            "ride_type": data.get("ride_type", "standard"),
        },
    )
    producer.publish(EventType.RIDE_REQUESTED, requeue_event, key=rider_id)


# ─── Main Consumer Loop ───────────────────────────────────────────────────────

def run():
    logger.info(f"{SERVICE_NAME} started. Listening for events...")
    start_http_server(8000)   # expose /metrics for Prometheus

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            try:
                event = EventEnvelope.from_kafka(msg.value())

                # Idempotency — skip already processed events
                if is_already_processed(event.event_id):
                    logger.info(f"Duplicate event skipped | event_id={event.event_id}")
                    consumer.commit(msg)
                    continue

                logger.info(
                    f"Event received | type={event.event_type} "
                    f"cid={event.correlation_id} event_id={event.event_id}"
                )

                if event.event_type == EventType.RIDE_REQUESTED:
                    handle_ride_requested(event)

                elif event.event_type == EventType.RIDE_DRIVER_DECLINED:
                    handle_ride_driver_declined(event)

                # Commit offset only after successful processing
                consumer.commit(msg)

            except Exception as e:
                logger.error(f"Failed to process event | error={e}", exc_info=True)
                # Do NOT commit — Kafka will redeliver this message

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    run()