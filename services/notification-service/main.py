import logging
import os
import time
import redis

from prometheus_client import Counter, start_http_server
from producer import KafkaProducer
from consumer import KafkaConsumer
from schemas.base import EventEnvelope, EventType
from schemas.notification_events import make_notification_sent

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_URL     = os.getenv("REDIS_URL", "redis://localhost:6379")
SERVICE_NAME  = os.getenv("SERVICE_NAME", "notification-service")

# ─── Prometheus Metrics ───────────────────────────────────────────────────────
notifications_sent   = Counter("notifications_sent_total",   "Total notifications sent", ["channel", "recipient_type"])
notifications_failed = Counter("notifications_failed_total", "Total notification failures")

# ─── Clients ──────────────────────────────────────────────────────────────────
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

producer = KafkaProducer(KAFKA_SERVERS)
consumer = KafkaConsumer(
    bootstrap_servers=KAFKA_SERVERS,
    group_id="notification-service-group",
    topics=[
        EventType.RIDE_REQUESTED,
        EventType.RIDE_DRIVER_MATCHED,
        EventType.RIDE_DRIVER_ACCEPTED,
        EventType.RIDE_DRIVER_DECLINED,
        EventType.RIDE_STARTED,
        EventType.RIDE_COMPLETED,
        EventType.RIDE_CANCELLED_BY_RIDER,
        EventType.RIDE_CANCELLED_BY_DRIVER,
        EventType.PAYMENT_CHARGED,
        EventType.PAYMENT_FAILED,
        EventType.PAYMENT_CANCELLATION_CHARGED,
        EventType.RIDE_DLQ,
        EventType.PAYMENT_DLQ,
    ],
)

# ─── Notification Templates ───────────────────────────────────────────────────
# Every event type maps to one or more notifications.
# Each entry: (recipient_id_key, recipient_type, channel, message_fn)

def _templates(event_type: str, data: dict) -> list[dict]:
    """
    Returns list of notifications to send for a given event type.
    Each notification targets a specific recipient with a specific message.
    """
    rider_id  = data.get("rider_id",  "unknown")
    driver_id = data.get("driver_id", None)

    templates = {
        EventType.RIDE_REQUESTED: [
            {
                "recipient_id":   rider_id,
                "recipient_type": "rider",
                "channel":        "push",
                "message":        "🔍 Looking for a driver near you...",
            }
        ],

        EventType.RIDE_DRIVER_MATCHED: [
            {
                "recipient_id":   rider_id,
                "recipient_type": "rider",
                "channel":        "push",
                "message":        f"🚖 Driver found! Waiting for confirmation. ETA {data.get('eta_mins')} mins.",
            },
            {
                "recipient_id":   driver_id,
                "recipient_type": "driver",
                "channel":        "push",
                "message":        "🔔 New ride request nearby. Accept or decline.",
            },
        ],

        EventType.RIDE_DRIVER_ACCEPTED: [
            {
                "recipient_id":   rider_id,
                "recipient_type": "rider",
                "channel":        "push",
                "message":        f"✅ Driver accepted your ride! Arriving in {data.get('eta_mins')} mins.",
            },
            {
                "recipient_id":   driver_id,
                "recipient_type": "driver",
                "channel":        "push",
                "message":        "✅ Ride accepted. Navigate to pickup location.",
            },
        ],

        EventType.RIDE_DRIVER_DECLINED: [
            {
                "recipient_id":   rider_id,
                "recipient_type": "rider",
                "channel":        "push",
                "message":        "🔄 Driver unavailable. Finding you another driver...",
            }
        ],

        EventType.RIDE_STARTED: [
            {
                "recipient_id":   rider_id,
                "recipient_type": "rider",
                "channel":        "push",
                "message":        "🚗 Your ride has started. Enjoy the trip!",
            },
            {
                "recipient_id":   driver_id,
                "recipient_type": "driver",
                "channel":        "push",
                "message":        "🚗 Ride started. Navigate to dropoff.",
            },
        ],

        EventType.RIDE_COMPLETED: [
            {
                "recipient_id":   rider_id,
                "recipient_type": "rider",
                "channel":        "push",
                "message":        f"🏁 Ride complete! Fare: ${data.get('fare_usd')}. Thanks for riding with RideFlow.",
            },
            {
                "recipient_id":   driver_id,
                "recipient_type": "driver",
                "channel":        "push",
                "message":        f"🏁 Ride complete! Earning: ${data.get('fare_usd')}.",
            },
        ],

        EventType.RIDE_CANCELLED_BY_RIDER: [
            {
                "recipient_id":   rider_id,
                "recipient_type": "rider",
                "channel":        "push",
                "message":        "❌ Your ride has been cancelled.",
            },
            *(
                [{
                    "recipient_id":   driver_id,
                    "recipient_type": "driver",
                    "channel":        "push",
                    "message":        "❌ Rider cancelled the ride.",
                }] if driver_id else []
            ),
        ],

        EventType.RIDE_CANCELLED_BY_DRIVER: [
            {
                "recipient_id":   rider_id,
                "recipient_type": "rider",
                "channel":        "push",
                "message":        "❌ Your driver cancelled. Finding you a new driver...",
            },
            {
                "recipient_id":   driver_id,
                "recipient_type": "driver",
                "channel":        "push",
                "message":        "❌ Ride cancelled. You are now available for new requests.",
            },
        ],

        EventType.PAYMENT_CHARGED: [
            {
                "recipient_id":   rider_id,
                "recipient_type": "rider",
                "channel":        "email",
                "message":        f"💳 Payment of ${data.get('amount_usd')} received. Receipt ID: {data.get('payment_id')}.",
            }
        ],

        EventType.PAYMENT_FAILED: [
            {
                "recipient_id":   rider_id,
                "recipient_type": "rider",
                "channel":        "sms",
                "message":        f"⚠️ Payment of ${data.get('amount_usd')} failed: {data.get('reason')}. Please update your payment method.",
            }
        ],

        EventType.PAYMENT_CANCELLATION_CHARGED: [
            {
                "recipient_id":   rider_id,
                "recipient_type": "rider",
                "channel":        "push",
                "message":        f"💳 Cancellation fee of ${data.get('amount_usd')} charged.",
            }
        ],

        EventType.RIDE_DLQ: [
            {
                "recipient_id":   rider_id,
                "recipient_type": "rider",
                "channel":        "push",
                "message":        "😔 Sorry, no drivers are available in your area right now. Please try again shortly.",
            }
        ],

        EventType.PAYMENT_DLQ: [
            {
                "recipient_id":   rider_id,
                "recipient_type": "rider",
                "channel":        "sms",
                "message":        "⚠️ We could not process your payment. Our team has been notified and will follow up.",
            }
        ],
    }

    return templates.get(event_type, [])


# ─── Helpers ──────────────────────────────────────────────────────────────────

def is_already_processed(event_id: str) -> bool:
    key = f"notification-service:processed:{event_id}"
    return not redis_client.set(key, "1", ex=86400, nx=True)


def dispatch_notification(recipient_id: str, recipient_type: str,
                           channel: str, message: str) -> bool:
    """
    Simulates dispatching a notification via SMS / email / push.
    In production: integrate Twilio (SMS), SendGrid (email), FCM (push).
    Returns True on success.
    """
    if not recipient_id or recipient_id == "unknown":
        logger.warning("Skipping notification — no recipient_id")
        return False

    # Simulate network call
    time.sleep(0.05)

    logger.info(
        f"[{channel.upper()}] → {recipient_type} {recipient_id} | {message}"
    )
    return True


# ─── Main Event Handler ───────────────────────────────────────────────────────

def handle_event(event: EventEnvelope) -> None:
    notifications = _templates(event.event_type, event.data)

    if not notifications:
        logger.debug(f"No notifications configured for {event.event_type}")
        return

    for n in notifications:
        try:
            success = dispatch_notification(
                recipient_id=n["recipient_id"],
                recipient_type=n["recipient_type"],
                channel=n["channel"],
                message=n["message"],
            )

            if success:
                # Publish notification.sent for Analytics to track
                sent_event = make_notification_sent(
                    correlation_id=event.correlation_id,
                    recipient_id=n["recipient_id"],
                    recipient_type=n["recipient_type"],
                    channel=n["channel"],
                    event_trigger=event.event_type,
                    message=n["message"],
                )
                producer.publish(
                    EventType.NOTIFICATION_SENT,
                    sent_event,
                    key=n["recipient_id"],
                )
                notifications_sent.labels(
                    channel=n["channel"],
                    recipient_type=n["recipient_type"],
                ).inc()

        except Exception as e:
            notifications_failed.inc()
            logger.error(
                f"Notification failed | recipient={n['recipient_id']} "
                f"channel={n['channel']} error={e}"
            )


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

                handle_event(event)
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