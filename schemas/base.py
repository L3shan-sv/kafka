import uuid
from datetime import datetime, timezone
from enum import Enum
from pydantic import BaseModel, Field


# ─── Event Types ────────────────────────────────────────────────────────────

class EventType(str, Enum):
    # Ride lifecycle
    RIDE_REQUESTED            = "ride.requested"
    RIDE_DRIVER_MATCHED       = "ride.driver_matched"
    RIDE_DRIVER_ACCEPTED      = "ride.driver_accepted"
    RIDE_DRIVER_DECLINED      = "ride.driver_declined"
    RIDE_STARTED              = "ride.started"
    RIDE_COMPLETED            = "ride.completed"
    RIDE_CANCELLED_BY_RIDER   = "ride.cancelled_by_rider"
    RIDE_CANCELLED_BY_DRIVER  = "ride.cancelled_by_driver"

    # Payment
    PAYMENT_CHARGED               = "payment.charged"
    PAYMENT_FAILED                = "payment.failed"
    PAYMENT_CANCELLATION_CHARGED  = "payment.cancellation_charged"

    # Notification
    NOTIFICATION_SENT = "notification.sent"

    # Dead Letter Queues
    RIDE_DLQ    = "ride.DLQ"
    PAYMENT_DLQ = "payment.DLQ"


# ─── Ride States (stored in Redis) ──────────────────────────────────────────

class RideState(str, Enum):
    REQUESTED  = "requested"
    MATCHED    = "matched"
    ACCEPTED   = "accepted"
    STARTED    = "started"
    COMPLETED  = "completed"
    CANCELLED  = "cancelled"


# ─── Base Event Envelope ─────────────────────────────────────────────────────

class EventEnvelope(BaseModel):
    event_id:       str       = Field(default_factory=lambda: f"evt_{uuid.uuid4().hex[:12]}")
    event_type:     EventType
    version:        str       = "v1"
    timestamp:      datetime  = Field(default_factory=lambda: datetime.now(timezone.utc))
    producer:       str                        # which service produced this
    correlation_id: str                        # ties all events for one ride together
    retry_count:    int       = 0              # driver decline retry counter
    data:           dict      = Field(default_factory=dict)

    model_config = {"use_enum_values": True}

    def to_kafka(self) -> bytes:
        """Serialize event to bytes for Kafka producer."""
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_kafka(cls, raw: bytes) -> "EventEnvelope":
        """Deserialize bytes from Kafka consumer."""
        return cls.model_validate_json(raw.decode("utf-8"))