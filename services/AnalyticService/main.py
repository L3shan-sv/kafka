import logging
import os
import time
import redis

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from contextlib import asynccontextmanager
from prometheus_client import (
    Counter, Gauge, Histogram,
    generate_latest, CONTENT_TYPE_LATEST,
)
import threading
import uvicorn

from consumer import KafkaConsumer
from schemas.base import EventEnvelope, EventType

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_URL     = os.getenv("REDIS_URL", "redis://localhost:6379")
SERVICE_NAME  = os.getenv("SERVICE_NAME", "analytics-service")

# ─── Redis Client ─────────────────────────────────────────────────────────────
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

# ─── Prometheus Metrics ───────────────────────────────────────────────────────

# Ride metrics
rides_requested_total  = Counter("rideflow_rides_requested_total",  "Total ride requests")
rides_completed_total  = Counter("rideflow_rides_completed_total",  "Total rides completed")
rides_cancelled_total  = Counter("rideflow_rides_cancelled_total",  "Total rides cancelled", ["cancelled_by"])
rides_dlq_total        = Counter("rideflow_rides_dlq_total",        "Total rides sent to DLQ")

# Driver metrics
driver_accepts_total   = Counter("rideflow_driver_accepts_total",   "Total driver acceptances")
driver_declines_total  = Counter("rideflow_driver_declines_total",  "Total driver declines")

# Payment metrics
revenue_total          = Counter("rideflow_revenue_total_usd",      "Total revenue collected in USD")
payments_failed_total  = Counter("rideflow_payments_failed_total",  "Total failed payments")
cancellation_fees_total= Counter("rideflow_cancellation_fees_total_usd", "Total cancellation fees collected")

# Notification metrics
notifications_total    = Counter("rideflow_notifications_total",    "Total notifications sent", ["channel"])

# Live gauges — current state of the platform right now
active_rides_gauge     = Gauge("rideflow_active_rides",             "Currently active rides")
revenue_today_gauge    = Gauge("rideflow_revenue_today_usd",        "Revenue collected today")
rides_today_gauge      = Gauge("rideflow_rides_today",              "Rides completed today")

# Fare distribution
fare_histogram         = Histogram(
    "rideflow_fare_usd",
    "Distribution of ride fares",
    buckets=[5, 10, 15, 20, 30, 50, 75, 100],
)


# ─── Redis Counter Helpers ────────────────────────────────────────────────────

def redis_incr(key: str, amount: float = 1) -> None:
    """Increment a Redis counter. Uses INCRBYFLOAT for revenue tracking."""
    if isinstance(amount, float):
        redis_client.incrbyfloat(key, amount)
    else:
        redis_client.incr(key)
    redis_client.expire(key, 86400)   # reset daily


def redis_decr(key: str) -> None:
    redis_client.decr(key)


def is_already_processed(event_id: str) -> bool:
    key = f"analytics-service:processed:{event_id}"
    return not redis_client.set(key, "1", ex=86400, nx=True)


def sync_gauges() -> None:
    """Sync Prometheus gauges from Redis — keeps live values accurate."""
    revenue = float(redis_client.get("analytics:revenue_today") or 0)
    rides   = int(redis_client.get("analytics:rides_completed_today") or 0)
    active  = int(redis_client.get("analytics:active_rides") or 0)

    revenue_today_gauge.set(revenue)
    rides_today_gauge.set(rides)
    active_rides_gauge.set(max(active, 0))


# ─── Event Handlers ───────────────────────────────────────────────────────────

def handle_ride_requested(data: dict) -> None:
    rides_requested_total.inc()
    redis_incr("analytics:rides_requested_today")
    active_rides_gauge.inc()
    redis_incr("analytics:active_rides")
    logger.info(f"[analytics] ride requested | rider={data.get('rider_id')}")


def handle_ride_completed(data: dict) -> None:
    fare = float(data.get("fare_usd", 0))

    rides_completed_total.inc()
    revenue_total.inc(fare)
    fare_histogram.observe(fare)

    redis_incr("analytics:rides_completed_today")
    redis_incr("analytics:revenue_today", fare)

    # Ride is no longer active
    active_rides_gauge.dec()
    redis_decr("analytics:active_rides")

    logger.info(f"[analytics] ride completed | fare=${fare}")


def handle_ride_cancelled(data: dict, cancelled_by: str) -> None:
    rides_cancelled_total.labels(cancelled_by=cancelled_by).inc()
    redis_incr(f"analytics:rides_cancelled_by_{cancelled_by}_today")

    active_rides_gauge.dec()
    redis_decr("analytics:active_rides")

    logger.info(f"[analytics] ride cancelled | by={cancelled_by}")


def handle_driver_accepted(data: dict) -> None:
    driver_accepts_total.inc()
    redis_incr("analytics:driver_accepts_today")


def handle_driver_declined(data: dict) -> None:
    driver_declines_total.inc()
    redis_incr("analytics:driver_declines_today")


def handle_payment_charged(data: dict) -> None:
    # Revenue already tracked in ride.completed — avoid double counting
    # Track payment success rate separately
    redis_incr("analytics:payments_charged_today")
    logger.info(f"[analytics] payment charged | amount=${data.get('amount_usd')}")


def handle_payment_failed(data: dict) -> None:
    payments_failed_total.inc()
    redis_incr("analytics:payments_failed_today")
    logger.warning(f"[analytics] payment failed | reason={data.get('reason')}")


def handle_cancellation_fee(data: dict) -> None:
    fee = float(data.get("amount_usd", 0))
    cancellation_fees_total.inc(fee)
    redis_incr("analytics:cancellation_fees_today", fee)


def handle_notification_sent(data: dict) -> None:
    channel = data.get("channel", "unknown")
    notifications_total.labels(channel=channel).inc()
    redis_incr(f"analytics:notifications_{channel}_today")


def handle_ride_dlq(data: dict) -> None:
    rides_dlq_total.inc()
    redis_incr("analytics:rides_dlq_today")

    # DLQ ride is no longer active
    active_rides_gauge.dec()
    redis_decr("analytics:active_rides")

    logger.warning(f"[analytics] ride sent to DLQ | rider={data.get('rider_id')}")


# ─── Event Router ─────────────────────────────────────────────────────────────

HANDLERS = {
    EventType.RIDE_REQUESTED:              handle_ride_requested,
    EventType.RIDE_COMPLETED:              handle_ride_completed,
    EventType.RIDE_CANCELLED_BY_RIDER:     lambda d: handle_ride_cancelled(d, "rider"),
    EventType.RIDE_CANCELLED_BY_DRIVER:    lambda d: handle_ride_cancelled(d, "driver"),
    EventType.RIDE_DRIVER_ACCEPTED:        handle_driver_accepted,
    EventType.RIDE_DRIVER_DECLINED:        handle_driver_declined,
    EventType.PAYMENT_CHARGED:             handle_payment_charged,
    EventType.PAYMENT_FAILED:              handle_payment_failed,
    EventType.PAYMENT_CANCELLATION_CHARGED:handle_cancellation_fee,
    EventType.NOTIFICATION_SENT:           handle_notification_sent,
    EventType.RIDE_DLQ:                    handle_ride_dlq,
}


# ─── Kafka Consumer Loop (runs in background thread) ─────────────────────────

def consume_loop() -> None:
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_SERVERS,
        group_id="analytics-service-group",
        topics=list(HANDLERS.keys()),
    )

    logger.info(f"{SERVICE_NAME} consumer started.")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            try:
                event = EventEnvelope.from_kafka(msg.value())

                if is_already_processed(event.event_id):
                    consumer.commit(msg)
                    continue

                handler = HANDLERS.get(event.event_type)
                if handler:
                    handler(event.data)
                    sync_gauges()

                consumer.commit(msg)

            except Exception as e:
                logger.error(f"Failed to process event | error={e}", exc_info=True)

    except Exception as e:
        logger.error(f"Consumer loop crashed | error={e}", exc_info=True)
    finally:
        consumer.close()


# ─── FastAPI App (exposes /metrics + /stats) ──────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start Kafka consumer in background thread
    t = threading.Thread(target=consume_loop, daemon=True)
    t.start()
    logger.info("Analytics consumer thread started.")
    yield
    logger.info("Analytics service shutting down.")


app = FastAPI(
    title="RideFlow — Analytics Service",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    """Prometheus scrape endpoint."""
    sync_gauges()
    return PlainTextResponse(
        generate_latest().decode("utf-8"),
        media_type=CONTENT_TYPE_LATEST,
    )


@app.get("/stats")
async def stats():
    """
    Human-readable live stats endpoint.
    Useful for the Grafana dashboard and for demoing the platform.
    """
    sync_gauges()
    return {
        "rides": {
            "requested_today":   int(redis_client.get("analytics:rides_requested_today") or 0),
            "completed_today":   int(redis_client.get("analytics:rides_completed_today") or 0),
            "active_now":        max(int(redis_client.get("analytics:active_rides") or 0), 0),
            "cancelled_by_rider":int(redis_client.get("analytics:rides_cancelled_by_rider_today") or 0),
            "cancelled_by_driver":int(redis_client.get("analytics:rides_cancelled_by_driver_today") or 0),
            "sent_to_dlq":       int(redis_client.get("analytics:rides_dlq_today") or 0),
        },
        "drivers": {
            "accepts_today":     int(redis_client.get("analytics:driver_accepts_today") or 0),
            "declines_today":    int(redis_client.get("analytics:driver_declines_today") or 0),
        },
        "revenue": {
            "total_today_usd":        float(redis_client.get("analytics:revenue_today") or 0),
            "cancellation_fees_today":float(redis_client.get("analytics:cancellation_fees_today") or 0),
            "payments_failed_today":  int(redis_client.get("analytics:payments_failed_today") or 0),
        },
        "notifications": {
            "push_today":  int(redis_client.get("analytics:notifications_push_today") or 0),
            "sms_today":   int(redis_client.get("analytics:notifications_sms_today") or 0),
            "email_today": int(redis_client.get("analytics:notifications_email_today") or 0),
        },
    }


@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME}


# ─── Entrypoint ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, log_level="info")