from datetime import datetime, timezone
from typing import Optional
from pydantic import BaseModel, Field
from .base import EventEnvelope, EventType


# ─── Ride Data Payloads ──────────────────────────────────────────────────────

class Location(BaseModel):
    address: str
    lat:     float
    lng:     float


class RideRequestedData(BaseModel):
    rider_id:    str
    pickup:      Location
    dropoff:     Location
    ride_type:   str = "standard"        # standard | premium | xl


class RideDriverMatchedData(BaseModel):
    rider_id:  str
    driver_id: str
    eta_mins:  int                       # estimated pickup time


class RideDriverAcceptedData(BaseModel):
    rider_id:  str
    driver_id: str
    eta_mins:  int


class RideDriverDeclinedData(BaseModel):
    rider_id:  str
    driver_id: str
    reason:    Optional[str] = None      # too far, going offline, etc.


class RideStartedData(BaseModel):
    rider_id:  str
    driver_id: str
    pickup:    Location


class RideCompletedData(BaseModel):
    rider_id:       str
    driver_id:      str
    pickup:         Location
    dropoff:        Location
    distance_km:    float
    duration_mins:  int
    fare_usd:       float


class RideCancelledByRiderData(BaseModel):
    rider_id:    str
    driver_id:   Optional[str] = None   # may not have a driver yet
    reason:      Optional[str] = None
    cancel_stage: str                   # before_match | after_match | after_pickup


class RideCancelledByDriverData(BaseModel):
    rider_id:  str
    driver_id: str
    reason:    Optional[str] = None


class RideDLQData(BaseModel):
    rider_id:      str
    reason:        str                  # no_drivers_available
    retry_count:   int
    original_event: dict                # original ride.requested payload


# ─── Event Factories ─────────────────────────────────────────────────────────
# Clean constructors so services never build envelopes manually

def make_ride_requested(rider_id: str, pickup: dict, dropoff: dict,
                        ride_type: str = "standard") -> EventEnvelope:
    import uuid
    data = RideRequestedData(
        rider_id=rider_id,
        pickup=Location(**pickup),
        dropoff=Location(**dropoff),
        ride_type=ride_type,
    )
    return EventEnvelope(
        event_type=EventType.RIDE_REQUESTED,
        producer="ride-request-service",
        correlation_id=f"ride_{uuid.uuid4().hex[:10]}",
        data=data.model_dump(),
    )


def make_ride_driver_matched(correlation_id: str, rider_id: str,
                              driver_id: str, eta_mins: int) -> EventEnvelope:
    data = RideDriverMatchedData(rider_id=rider_id, driver_id=driver_id, eta_mins=eta_mins)
    return EventEnvelope(
        event_type=EventType.RIDE_DRIVER_MATCHED,
        producer="driver-match-service",
        correlation_id=correlation_id,
        data=data.model_dump(),
    )


def make_ride_driver_accepted(correlation_id: str, rider_id: str,
                               driver_id: str, eta_mins: int) -> EventEnvelope:
    data = RideDriverAcceptedData(rider_id=rider_id, driver_id=driver_id, eta_mins=eta_mins)
    return EventEnvelope(
        event_type=EventType.RIDE_DRIVER_ACCEPTED,
        producer="driver-match-service",
        correlation_id=correlation_id,
        data=data.model_dump(),
    )


def make_ride_driver_declined(correlation_id: str, rider_id: str,
                               driver_id: str, retry_count: int,
                               reason: Optional[str] = None) -> EventEnvelope:
    data = RideDriverDeclinedData(rider_id=rider_id, driver_id=driver_id, reason=reason)
    return EventEnvelope(
        event_type=EventType.RIDE_DRIVER_DECLINED,
        producer="driver-match-service",
        correlation_id=correlation_id,
        retry_count=retry_count,
        data=data.model_dump(),
    )


def make_ride_started(correlation_id: str, rider_id: str,
                      driver_id: str, pickup: dict) -> EventEnvelope:
    data = RideStartedData(rider_id=rider_id, driver_id=driver_id, pickup=Location(**pickup))
    return EventEnvelope(
        event_type=EventType.RIDE_STARTED,
        producer="driver-match-service",
        correlation_id=correlation_id,
        data=data.model_dump(),
    )


def make_ride_completed(correlation_id: str, rider_id: str, driver_id: str,
                        pickup: dict, dropoff: dict, distance_km: float,
                        duration_mins: int, fare_usd: float) -> EventEnvelope:
    data = RideCompletedData(
        rider_id=rider_id, driver_id=driver_id,
        pickup=Location(**pickup), dropoff=Location(**dropoff),
        distance_km=distance_km, duration_mins=duration_mins, fare_usd=fare_usd,
    )
    return EventEnvelope(
        event_type=EventType.RIDE_COMPLETED,
        producer="driver-match-service",
        correlation_id=correlation_id,
        data=data.model_dump(),
    )


def make_ride_cancelled_by_rider(correlation_id: str, rider_id: str,
                                  cancel_stage: str,
                                  driver_id: Optional[str] = None,
                                  reason: Optional[str] = None) -> EventEnvelope:
    data = RideCancelledByRiderData(
        rider_id=rider_id, driver_id=driver_id,
        reason=reason, cancel_stage=cancel_stage,
    )
    return EventEnvelope(
        event_type=EventType.RIDE_CANCELLED_BY_RIDER,
        producer="ride-request-service",
        correlation_id=correlation_id,
        data=data.model_dump(),
    )


def make_ride_cancelled_by_driver(correlation_id: str, rider_id: str,
                                   driver_id: str,
                                   reason: Optional[str] = None) -> EventEnvelope:
    data = RideCancelledByDriverData(rider_id=rider_id, driver_id=driver_id, reason=reason)
    return EventEnvelope(
        event_type=EventType.RIDE_CANCELLED_BY_DRIVER,
        producer="driver-match-service",
        correlation_id=correlation_id,
        data=data.model_dump(),
    )


def make_ride_dlq(correlation_id: str, rider_id: str,
                  retry_count: int, original_event: dict) -> EventEnvelope:
    data = RideDLQData(
        rider_id=rider_id,
        reason="no_drivers_available",
        retry_count=retry_count,
        original_event=original_event,
    )
    return EventEnvelope(
        event_type=EventType.RIDE_DLQ,
        producer="driver-match-service",
        correlation_id=correlation_id,
        retry_count=retry_count,
        data=data.model_dump(),
    )