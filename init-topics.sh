#!/bin/bash
echo "Creating RideFlow Kafka topics..."
TOPICS="ride.requested ride.driver_matched ride.driver_accepted ride.driver_declined ride.started ride.completed ride.cancelled_by_rider ride.cancelled_by_driver payment.charged payment.failed payment.cancellation_charged notification.sent ride.DLQ payment.DLQ"
for topic in $TOPICS; do
  docker exec rideflow-kafka kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic $topic \
    --partitions 4 \
    --replication-factor 1
  echo "✓ $topic"
done
echo "Done."
