RideFlow — Official Technical Documentation
Complete reference for system logic, event flows, failure scenarios, and recovery strategies.

1. Complete Event Flow
Happy Path — Ride Requested to Completed
1.  Rider sends POST /ride/request
2.  Ride Request Service validates input (Pydantic)
3.  Ride Request Service generates correlation_id (ride_abc123)
4.  Redis: SET ride:ride_abc123:state = "requested"
5.  Kafka: PRODUCE ride.requested (key = rider_id)
6.  Ride Request Service returns 202 Accepted immediately

7.  Driver Match Service CONSUMES ride.requested
8.  Redis idempotency check: SET NX driver-match:processed:{event_id} → pass
9.  Redis: GET ride:ride_abc123:state → "requested" (not cancelled)
10. find_nearby_driver() returns driver drv_abc
11. Kafka: PRODUCE ride.driver_matched
12. Redis: SET ride:ride_abc123:state = "matched"
13. Notification Service CONSUMES ride.driver_matched → push to rider + driver
14. Driver accept simulation → accepted

15. Driver Match Service PRODUCES ride.driver_accepted
16. Redis: SET ride:ride_abc123:state = "accepted"
17. Notification Service CONSUMES ride.driver_accepted → push to rider + driver
18. Analytics Service CONSUMES ride.driver_accepted → INCR driver_accepts_today

19. [2 second simulation] Driver en route
20. Driver Match Service PRODUCES ride.started
21. Redis: SET ride:ride_abc123:state = "started"
22. Notification Service CONSUMES ride.started → push to rider + driver
23. Analytics Service CONSUMES ride.started → INCR active_rides

24. [3 second simulation] Ride in progress
25. Driver Match Service calculates: distance=8.5km, duration=18min, fare=$15.45
26. Driver Match Service PRODUCES ride.completed
27. Redis: SET ride:ride_abc123:state = "completed"
28. Notification Service CONSUMES ride.completed → push to rider + driver
29. Analytics Service CONSUMES ride.completed → INCR rides_completed, revenue += 15.45

30. Payment Service CONSUMES ride.completed
31. Redis idempotency check → pass
32. simulate_payment($15.45) → success
33. Kafka: PRODUCE payment.charged
34. Notification Service CONSUMES payment.charged → email receipt to rider
35. Analytics Service CONSUMES payment.charged → INCR payments_charged_today
Total events produced: 8 (ride.requested → payment.charged)
Total consumer calls: ~24 (each event consumed by 2–4 services)
User-visible latency: < 100ms (202 Accepted response)
End-to-end ride duration: ~5 seconds (simulated)

Driver Decline + Retry Path
1–6.  Same as happy path

7.  Driver Match Service CONSUMES ride.requested
8.  find_nearby_driver() returns driver drv_xyz
9.  Kafka: PRODUCE ride.driver_matched (retry_count = 0)
10. driver_accepts() → False (declined)
11. Kafka: PRODUCE ride.driver_declined (retry_count = 1)
12. Notification Service CONSUMES ride.driver_declined → push to rider: "finding another driver"

13. Driver Match Service CONSUMES ride.driver_declined
14. retry_count (1) < MAX_RETRIES (5) → re-queue
15. Kafka: PRODUCE ride.requested (retry_count = 1, same correlation_id)
16. Steps 7–15 repeat until driver accepts OR retry_count = 5

17. If retry_count = 5:
    Kafka: PRODUCE ride.DLQ
    Notification Service CONSUMES ride.DLQ → push to rider: "no drivers available"
    Analytics Service CONSUMES ride.DLQ → INCR rides_dlq_today
    Flow terminates.

Simultaneous Cancellation Path
T1. Rider sends POST /ride/cancel
T2. Driver sends cancel signal (via Driver Match Service)

T3. Ride Request Service:
    Redis: SET ride:ride_abc123:state = "cancelled" NX → SUCCESS (rider wins lock)
    Kafka: PRODUCE ride.cancelled_by_rider (cancel_stage = "after_pickup")

T4. Driver Match Service (milliseconds later):
    Redis: SET ride:ride_abc123:state = "cancelled" NX → FAIL (key exists)
    Logs: "simultaneous cancel — rider cancelled first"
    Kafka: PRODUCE ride.cancelled_by_driver (still produced for audit trail)

T5. Payment Service CONSUMES ride.cancelled_by_rider:
    Redis: SET payment-service:cancel_lock:ride_abc123 = "1" NX → SUCCESS
    cancel_stage = "after_pickup" → fee = $5.00
    simulate_payment($5.00) → charged
    Kafka: PRODUCE payment.cancellation_charged

T6. Payment Service CONSUMES ride.cancelled_by_driver:
    Redis: SET payment-service:cancel_lock:ride_abc123 = "1" NX → FAIL (lock exists)
    Logs: "simultaneous cancel already processed — skipping"
    Returns without charging.

Result: One cancellation fee charged ($5.00). Zero double charges.
        Both events exist in Kafka for full audit trail.

2. Failure Scenarios & Recovery Strategies

Scenario A: Ride Request Service Goes Down
What happens:

New ride requests cannot be accepted
ALB health checks fail on /health
ALB stops routing traffic to the failed pod
Kubernetes detects pod failure (liveness probe) and schedules replacement
HPA may spin up additional pods if remaining pods are overloaded

What does NOT happen:

No in-flight rides are affected — they are already in Kafka
No data is lost — Ride Request Service only produces events, it doesn't hold state
Payment, notification, and analytics continue processing normally

Recovery:

Kubernetes auto-restarts the pod (restart: unless-stopped)
New pod connects to Kafka and Redis on startup
Service resumes accepting requests
No manual intervention required
Recovery time: typically 15–30 seconds

Manual intervention trigger: Pod crash-loops more than 5 times → PagerDuty alert → investigate application logs

Scenario B: Driver Match Service Goes Down
What happens:

ride.requested events accumulate in Kafka (consumer lag increases)
Kafka holds all messages — nothing is lost (7-day retention)
In-flight rides that already have a driver continue — ride lifecycle threads are lost if the pod crashes mid-ride
No new rides are matched during the outage

What does NOT happen:

Rider API still accepts new requests (202 Accepted) — Kafka buffers them
Payment and notification services are unaffected
Redis state for in-progress rides is preserved

Recovery:

Kubernetes restarts the pod
Driver Match Service reconnects to Kafka with the same group.id
Kafka resumes delivery from the last committed offset
Backlog of ride.requested events is processed immediately
Rides whose lifecycle threads were lost mid-execution: their correlation_id state in Redis will remain as "accepted" or "started" indefinitely — these require a background job to detect stale rides and publish cancellations. This is a known gap.

At scale mitigation: Add a Redis TTL watchdog — a separate process that scans for rides stuck in started state for more than 2 hours and publishes a ride.cancelled_by_system event.

Scenario C: Payment Service Goes Down
What happens:

ride.completed events accumulate in Kafka
No payments are processed during the outage
Riders complete their rides but are not charged immediately
Notification Service continues operating normally
Analytics Service continues operating normally

What does NOT happen:

Rides do not stop completing
Notifications for ride completion still fire (they don't depend on Payment Service)
No events are lost

Recovery:

Kubernetes restarts the pod
Payment Service reconnects with same consumer group — resumes from last offset
All accumulated ride.completed events are processed
Idempotency check ensures no ride is charged twice even if the same event was partially processed before the crash
Recovery is fully automatic — payments are delayed, not lost

Business impact: Delayed payment collection. Acceptable for minutes, not hours. Alert threshold: consumer lag on ride.completed > 100 messages → PagerDuty.

Scenario D: Redis Goes Down
What happens:

All services lose idempotency checking capability
Ride state can no longer be read or written
Race condition locks are unavailable
Analytics counters stop updating

Immediate impact:

Ride Request Service: /ride/cancel cannot read current ride state → returns 503
Driver Match Service: cannot check if ride was already cancelled → may match driver for cancelled ride
Payment Service: cannot resolve simultaneous cancellations → risk of double charge

Recovery:

ElastiCache auto-recovers within 1–2 minutes (single-node failover)
Services reconnect automatically on next Redis operation
Events that were processed during the outage without idempotency checks may have been duplicated — review logs for double-processed event_id values

Mitigation at scale: Redis Cluster with 3 nodes across 3 AZs. A single node failure triggers automatic failover to a replica in under 30 seconds without data loss.

Scenario E: Kafka Goes Down (MSK Broker Failure)
What happens:

Producers (Ride Request Service, Driver Match Service) cannot publish events
Consumers cannot receive events
All ride operations halt
New ride requests receive 500 errors (Kafka publish fails)

MSK behaviour:

MSK runs 2 brokers across 2 AZs
If one broker fails, MSK automatically promotes partition leaders to the surviving broker
With replication-factor 1 (our dev setting), a broker failure means data on that broker's partitions is temporarily unavailable
With replication-factor 2 (production), the surviving broker serves all partitions immediately

Recovery:

MSK auto-recovers the failed broker — typically 3–5 minutes
Producers resume publishing once broker is available
Consumers resume from last committed offset
No data lost if replication factor ≥ 2

Production fix: Set replication-factor = 2 for all topics, min.insync.replicas = 2 on the broker, acks = all on producers (already configured). This ensures a message is only acknowledged after both brokers have written it.

Scenario F: Both Rider and Driver Cancel — Redis Also Down
What happens:

Race condition locks are unavailable
Both ride.cancelled_by_rider and ride.cancelled_by_driver flow to Payment Service
Payment Service cannot check the cancellation lock
Without the lock, both events process independently
Risk: double charge or double fee

Mitigation:
Payment Service should treat Redis unavailability as a hard stop for cancellation processing:
pythontry:
    won_lock = resolve_simultaneous_cancel(cid)
except redis.RedisError:
    # Redis is down — cannot safely process cancellation
    # Route to DLQ for manual review rather than risk double charge
    producer.publish(EventType.PAYMENT_DLQ, event, key=rider_id)
    logger.critical(f"Redis down during cancellation — routed to DLQ | cid={cid}")
    return
This is a safe-fail pattern: when in doubt, do nothing and alert. A delayed cancellation fee is better than an incorrect double charge.

Scenario G: Kafka Event Schema Mismatch (Bad Deployment)
What happens:

A new version of Driver Match Service produces ride.completed events with a new required field
Old Payment Service consumers try to parse the event
Pydantic raises ValidationError
Payment Service logs the error but does NOT commit the offset
Kafka redelivers the unprocessable event repeatedly
Consumer lag grows on ride.completed

Recovery:

Roll back the Driver Match Service deployment
Or deploy the new Payment Service (with updated schema) first before deploying the new producer
Schema versioning (version field in envelope) allows consumers to route v2 events to a separate handler while still processing v1

Prevention: Always deploy consumers before producers when adding required fields. Never remove fields from an event — mark them optional instead. This is the expand-then-migrate pattern.

3. Monitoring & Alerting Reference
AlertTriggerSeverityResponseHigh consumer lagride.completed lag > 100P1Check Payment Service podsRide DLQ spikeride.DLQ > 10/minP2Check driver availability simulationPayment DLQ spikepayment.DLQ > 5/minP1Check payment gateway + RedisPod crash loopAny pod restarts > 5P1Check application logsRedis downRedis ping failsP1Check ElastiCache consoleLow driver accept rateaccepts/(accepts+declines) < 0.5P2Business alertRevenue anomalyRevenue drop > 30% vs prev hourP2Business alert

4. Data Retention & Cleanup
Kafka: Events retained for 7 days (log.retention.hours = 168). After 7 days events are deleted automatically. This means services that have been down for more than 7 days cannot recover missed events.
Redis: All keys set with 24-hour TTL. Idempotency keys, ride state, and analytics counters reset daily. This means a service recovering after 24+ hours may re-process old events.
Production recommendation: Extend idempotency key TTL to match Kafka retention (7 days). Set analytics counters to expire at midnight UTC rather than 24 hours after creation.