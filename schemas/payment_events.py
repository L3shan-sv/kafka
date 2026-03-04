
from typing import Optional
from pydantic import BaseModel
from .base import EventEnvelope, EventType


# ─── Payment Data Payloads ───────────────────────────────────────────────────

class PaymentChargedData(BaseModel):
    rider_id:   str
    driver_id:  str
    amount_usd: float
    payment_id: str
    ride_id:    str


class PaymentFailedData(BaseModel):
    rider_id:   str
    ride_id:    str
    reason:     str                      # insufficient_funds | card_declined | timeout
    amount_usd: float


class PaymentCancellationChargedData(BaseModel):
    rider_id:        str
    ride_id:         str
    amount_usd:      float
    cancel_stage:    str                 # after_match | after_pickup
    cancelled_by:    str                 # rider | driver | both


# ─── Payment Event Factories ─────────────────────────────────────────────────

def make_payment_charged(correlation_id: str, rider_id: str, driver_id: str,
                          amount_usd: float, payment_id: str, ride_id: str) -> EventEnvelope:
    data = PaymentChargedData(
        rider_id=rider_id, driver_id=driver_id,
        amount_usd=amount_usd, payment_id=payment_id, ride_id=ride_id,
    )
    return EventEnvelope(
        event_type=EventType.PAYMENT_CHARGED,
        producer="payment-service",
        correlation_id=correlation_id,
        data=data.model_dump(),
    )


def make_payment_failed(correlation_id: str, rider_id: str, ride_id: str,
                         reason: str, amount_usd: float) -> EventEnvelope:
    data = PaymentFailedData(
        rider_id=rider_id, ride_id=ride_id,
        reason=reason, amount_usd=amount_usd,
    )
    return EventEnvelope(
        event_type=EventType.PAYMENT_FAILED,
        producer="payment-service",
        correlation_id=correlation_id,
        data=data.model_dump(),
    )


def make_payment_cancellation_charged(correlation_id: str, rider_id: str,
                                       ride_id: str, amount_usd: float,
                                       cancel_stage: str,
                                       cancelled_by: str) -> EventEnvelope:
    data = PaymentCancellationChargedData(
        rider_id=rider_id, ride_id=ride_id,
        amount_usd=amount_usd, cancel_stage=cancel_stage,
        cancelled_by=cancelled_by,
    )
    return EventEnvelope(
        event_type=EventType.PAYMENT_CANCELLATION_CHARGED,
        producer="payment-service",
        correlation_id=correlation_id,
        data=data.model_dump(),
    )
