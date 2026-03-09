import logging
import os
import redis
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, make_asgi_app
import time

from producer import KafkaProducer
from schemas.base import EventType, RideState
from schemas.ride_events import (
    make_ride_requested,
    make_ride_cancelled_by_rider,
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
SERVICE_NAME  = os.getenv("SERVICE_NAME", "ride-request-service")

# ─── Prometheus Metrics ───────────────────────────────────────────────────────
rides_requested  = Counter("rides_requested_total",   "Total ride requests received")
rides_cancelled  = Counter("rides_cancelled_total",   "Total ride cancellations by rider")
request_latency  = Histogram("request_duration_seconds", "API request latency", ["endpoint"])

# ─── App State ────────────────────────────────────────────────────────────────
producer:     KafkaProducer
redis_client: redis.Redis


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, redis_client
    logger.info("Starting ride-request-service...")

    producer     = KafkaProducer(KAFKA_SERVERS)
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)

    logger.info("Kafka producer and Redis connected.")
    yield

    producer.flush()
    redis_client.close()
    logger.info("ride-request-service shut down cleanly.")


app = FastAPI(
    title="RideFlow — Ride Request Service",
    version="1.0.0",
    lifespan=lifespan,
)

# ─── CORS ─────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount Prometheus metrics endpoint — must come AFTER middleware
app.mount("/metrics", make_asgi_app())


# ─── Request / Response Models ────────────────────────────────────────────────

class LocationInput(BaseModel):
    address: str
    lat:     float
    lng:     float


class RideRequestInput(BaseModel):
    rider_id:  str
    pickup:    LocationInput
    dropoff:   LocationInput
    ride_type: str = "standard"


class RideCancelInput(BaseModel):
    rider_id:       str
    correlation_id: str
    driver_id:      str | None = None
    reason:         str | None = None


class RideRequestResponse(BaseModel):
    message:        str
    correlation_id: str
    event_id:       str


# ─── Helpers ──────────────────────────────────────────────────────────────────

def get_ride_state(correlation_id: str) -> str | None:
    """Check current ride state from Redis."""
    return redis_client.get(f"ride:{correlation_id}:state")


def set_ride_state(correlation_id: str, state: RideState) -> None:
    """Store ride state in Redis with 24hr TTL."""
    redis_client.set(
        f"ride:{correlation_id}:state",
        state.value,
        ex=86400,
    )


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "service": SERVICE_NAME}


@app.post(
    "/ride/request",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=RideRequestResponse,
)
async def request_ride(body: RideRequestInput):
    start = time.time()

    event = make_ride_requested(
        rider_id=body.rider_id,
        pickup=body.pickup.model_dump(),
        dropoff=body.dropoff.model_dump(),
        ride_type=body.ride_type,
    )

    set_ride_state(event.correlation_id, RideState.REQUESTED)

    producer.publish(
        topic=EventType.RIDE_REQUESTED,
        event=event,
        key=body.rider_id,
    )

    rides_requested.inc()
    request_latency.labels(endpoint="/ride/request").observe(time.time() - start)

    logger.info(
        f"Ride requested | rider={body.rider_id} "
        f"correlation_id={event.correlation_id} event_id={event.event_id}"
    )

    return RideRequestResponse(
        message="Ride request received. Finding you a driver.",
        correlation_id=event.correlation_id,
        event_id=event.event_id,
    )


@app.post(
    "/ride/cancel",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=RideRequestResponse,
)
async def cancel_ride(body: RideCancelInput):
    start = time.time()

    current_state = get_ride_state(body.correlation_id)

    if not current_state:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Ride {body.correlation_id} not found.",
        )

    if current_state == RideState.CANCELLED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ride is already cancelled.",
        )

    if current_state == RideState.COMPLETED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Cannot cancel a completed ride.",
        )

    cancel_stage_map = {
        RideState.REQUESTED: "before_match",
        RideState.MATCHED:   "after_match",
        RideState.ACCEPTED:  "after_match",
        RideState.STARTED:   "after_pickup",
    }
    cancel_stage = cancel_stage_map.get(current_state, "before_match")

    event = make_ride_cancelled_by_rider(
        correlation_id=body.correlation_id,
        rider_id=body.rider_id,
        cancel_stage=cancel_stage,
        driver_id=body.driver_id,
        reason=body.reason,
    )

    locked = redis_client.set(
        f"ride:{body.correlation_id}:state",
        RideState.CANCELLED.value,
        ex=86400,
        nx=True,
    )

    if not locked:
        logger.warning(
            f"Race condition detected on cancel | "
            f"correlation_id={body.correlation_id} rider={body.rider_id}"
        )

    producer.publish(
        topic=EventType.RIDE_CANCELLED_BY_RIDER,
        event=event,
        key=body.rider_id,
    )

    rides_cancelled.inc()
    request_latency.labels(endpoint="/ride/cancel").observe(time.time() - start)

    logger.info(
        f"Ride cancelled by rider | rider={body.rider_id} "
        f"stage={cancel_stage} correlation_id={body.correlation_id}"
    )

    return RideRequestResponse(
        message="Ride cancellation received.",
        correlation_id=body.correlation_id,
        event_id=event.event_id,
    )