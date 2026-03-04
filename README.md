🚖 RideFlow
Uber-Style Event-Driven Ride Dispatch Platform
A production-grade distributed microservices platform built on Apache Kafka, simulating a real-time ride dispatch system. Every service is independently containerised, communicates exclusively through Kafka events, and is deployable to AWS EKS via Terraform.

What This Is
RideFlow demonstrates the core architectural patterns used by Uber, Lyft, and similar companies at scale:

Event-driven architecture — services never call each other directly
Distributed state management — Redis atomic locks resolve race conditions
Fault tolerance — idempotency, retries, and dead letter queues at every layer
Production observability — Prometheus metrics, Grafana dashboards, OpenTelemetry tracing


Architecture
Rider App
    │
    ▼
AWS ALB ──► Ride Request Service (FastAPI)
                    │
              ride.requested
                    │
              ┌─────▼──────────────────────┐
              │      Apache Kafka           │
              │  14 topics · 4 partitions   │
              └──┬──────┬──────┬────────┬──┘
                 │      │      │        │
                 ▼      ▼      ▼        ▼
           Driver   Payment  Notif  Analytics
           Match    Service  Service  Service
           Service
Microservices
ServiceRoleKafkaReplicasRide Request ServiceREST API entry pointProducer2–10Driver Match ServiceState machine + retry loopProducer + Consumer3–12Payment ServiceFare + cancellation fee processingConsumer2–8Notification ServiceSMS / Email / Push dispatchConsumer2–8Analytics ServiceMetrics + live statsConsumer1
Kafka Topics
TopicDescriptionride.requestedRider opens app and requests rideride.driver_matchedDriver found and offer sentride.driver_acceptedDriver confirmed rideride.driver_declinedDriver declined — triggers retryride.startedDriver picked up riderride.completedRider dropped offride.cancelled_by_riderRider cancelledride.cancelled_by_driverDriver cancelledpayment.chargedPayment successfulpayment.failedPayment failedpayment.cancellation_chargedCancellation fee appliednotification.sentNotification dispatchedride.DLQRide failed after 5 driver declinespayment.DLQPayment failed — queued for manual review

Tech Stack
Application

Python 3.12, FastAPI, Pydantic v2
confluent-kafka-python, redis-py
prometheus-client, opentelemetry-sdk

Infrastructure

Docker + Docker Compose (local)
Kubernetes EKS (production)
Terraform (IaC)
AWS MSK (Kafka), ElastiCache (Redis), ALB

Observability

Prometheus, Grafana, Kafka Exporter, OpenTelemetry


Production Engineering Patterns
Idempotency — every service checks Redis before processing any event using SET NX on event_id. Kafka's at-least-once delivery guarantee means duplicates will arrive — this blocks them from being processed twice.
Race condition resolution — when a rider and driver cancel simultaneously, both ride.cancelled_by_rider and ride.cancelled_by_driver hit the Payment Service. A Redis SET NX lock ensures only the first event to arrive processes. The second is dropped cleanly. No double charge, no crash.
Cancellation fee rules — the cancel stage (before_match / after_match / after_pickup) is determined at cancellation time by the Ride Request Service and embedded in the event. Payment Service reads the field and applies the correct fee without needing external state.
Dead Letter Queue — rides that fail 5 consecutive driver declines are routed to ride.DLQ. Payment failures are routed to payment.DLQ. Both trigger notifications to the user and are available for ops team review.
Schema versioning — every event carries a version field. Old consumers keep reading v1 while new consumers can handle v2. Topics are never broken by schema changes.
Correlation ID — every event carries correlation_id generated at ride request time. This ties every event across every service for a single ride together, enabling full end-to-end tracing.

Local Development
Prerequisites: Docker, Docker Compose
bash# Clone the repo
git clone https://github.com/yourname/rideflow.git
cd rideflow

# Start the full platform
docker compose up --build

# Services available at:
# http://localhost:8001  →  Ride Request API
# http://localhost:8005  →  Analytics /stats + /metrics
# http://localhost:8080  →  Kafka UI
# http://localhost:9090  →  Prometheus
# http://localhost:3000  →  Grafana (admin / rideflow123)
# localhost:6379         →  Redis
# localhost:9092         →  Kafka broker
Request a ride:
bashcurl -X POST http://localhost:8001/ride/request \
  -H "Content-Type: application/json" \
  -d '{
    "rider_id": "user_42",
    "pickup":  { "address": "Times Square, NY",  "lat": 40.7580, "lng": -73.9855 },
    "dropoff": { "address": "JFK Airport, NY",   "lat": 40.6413, "lng": -73.7781 },
    "ride_type": "standard"
  }'
Cancel a ride:
bashcurl -X POST http://localhost:8001/ride/cancel \
  -H "Content-Type: application/json" \
  -d '{
    "rider_id": "user_42",
    "correlation_id": "ride_abc123",
    "reason": "changed_plans"
  }'
Check live stats:
bashcurl http://localhost:8005/stats

Production Deployment
1. Provision AWS infrastructure with Terraform
bashcd terraform
terraform init
terraform plan
terraform apply
This provisions: VPC, subnets, EKS cluster, MSK Kafka, ElastiCache Redis, ALB, IAM roles, and Security Groups.
2. Connect kubectl to EKS
bashaws eks update-kubeconfig \
  --name rideflow-cluster \
  --region us-east-1
3. Deploy Kubernetes manifests
bashkubectl apply -f kubernetes/namespace.yml
kubectl apply -f kubernetes/
4. Verify deployments
bashkubectl get pods -n rideflow
kubectl get hpa -n rideflow

Project Structure
rideflow/
├── schemas/                        # Shared Pydantic event models
│   ├── base.py                     # EventEnvelope + EventType + RideState
│   ├── ride_events.py              # Ride event payloads + factories
│   ├── payment_events.py           # Payment event payloads + factories
│   └── notification_events.py      # Notification event payloads + factories
│
├── services/
│   ├── ride-request-service/       # FastAPI producer
│   ├── driver-match-service/       # State machine consumer/producer
│   ├── payment-service/            # Payment consumer
│   ├── notification-service/       # Notification consumer
│   └── analytics-service/          # Analytics consumer + /stats API
│
├── kubernetes/
│   ├── namespace.yml
│   ├── deployments/                # All 5 service deployments
│   ├── services/                   # ClusterIP + LoadBalancer services
│   └── hpa/                        # Horizontal Pod Autoscalers
│
├── terraform/
│   ├── main.tf                     # Provider + S3 backend
│   ├── vpc.tf                      # VPC, subnets, IGW, NAT
│   ├── eks.tf                      # EKS cluster + node group + IAM
│   ├── kafka.tf                    # MSK cluster + security group
│   ├── redis.tf                    # ElastiCache + security group
│   ├── alb.tf                      # Application Load Balancer
│   └── outputs.tf                  # Cluster name, broker endpoints, ALB DNS
│
├── monitoring/
│   ├── prometheus/prometheus.yml   # Scrape config for all services
│   └── grafana/dashboards/         # Pre-built dashboard JSON
│
├── docker-compose.yml              # Full local environment
└── README.md

Observability
Grafana Dashboards at localhost:3000 (credentials: admin / rideflow123)
Panels include: rides per minute, revenue per hour, driver accept rate, consumer lag per topic, payment failure rate, notification delivery by channel, fare distribution histogram, service p95 latency.
Live stats API at localhost:8005/stats returns a JSON snapshot of current platform state pulled directly from Redis.
Distributed traces — every request carries a correlation_id tracked through OpenTelemetry, linking every Kafka event for a single ride across all 5 services.

Key Engineering Decisions
See TRADEOFFS.md for a full discussion of every architectural decision, what was considered, what was chosen, and what would change in a larger production system.
See DOCUMENTATION.md for the complete logic flow, failure scenarios, and recovery strategies.