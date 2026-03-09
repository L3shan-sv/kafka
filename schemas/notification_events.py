from pydantic import BaseModel
from .base import EventEnvelope, EventType


# ─── Notification Data Payloads ──────────────────────────────────────────────

class NotificationSentData(BaseModel):
    recipient_id:   str
    recipient_type: str              # rider | driver
    channel:        str              # sms | email | push
    event_trigger:  str              # which event caused this notification
    message:        str


# ─── Notification Event Factory ──────────────────────────────────────────────

def make_notification_sent(correlation_id: str, recipient_id: str,
                            recipient_type: str, channel: str,
                            event_trigger: str, message: str) -> EventEnvelope:
    data = NotificationSentData(
        recipient_id=recipient_id,
        recipient_type=recipient_type,
        channel=channel,
        event_trigger=event_trigger,
        message=message,
    )
    return EventEnvelope(
        event_type=EventType.NOTIFICATION_SENT,
        producer="notification-service",
        correlation_id=correlation_id,
        data=data.model_dump(),
    )
