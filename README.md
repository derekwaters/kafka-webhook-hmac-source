# Example Python REST server (Flask) with Kafka events

This repository contains a Flask-based REST server with:
- Kafka event producer library for publishing events to Kafka topics
- Comprehensive test suite with mocked Kafka integration
- HMAC message validation for GitHub webhooks

## Quick start

1. Create and activate a virtual environment (recommended):

```bash
python -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run the server with Flask:

```bash
flask run --host 0.0.0.0 --port 8000
```

Or run directly:

```bash
python app/main.py
```

The server will be available at http://127.0.0.1:8000.

## Endpoints

**Health:**
- `GET /health` — returns `{"status": "ok"}`

**Kafka Events:**
- `POST /events` — post an event to Kafka (202 Accepted)
  - JSON body: Entire body is sent to the configured Kafka topic (see config below)

## Kafka configuration

Set environment variables to control Kafka behavior:

```bash
export KAFKA_BROKERS="localhost:9092"      # Comma-separated brokers
export KAFKA_TOPIC="events"                 # Default topic for events
export HMAC_HEADER="x-hub-signature-256"    # Name of the input HTTP header for HMAC token
export HMAC_SECRET="github_hmac_secret"     # The HMAC secret shared with the client application
```

## Kafka producer library

The `lib/kafka_producer.py` module provides:

```python
from lib.kafka_producer import EventProducer, KafkaConfig

# Configure
config = KafkaConfig(
    bootstrap_servers="localhost:9092",
    topic="my-events",
    acks="all",
    retries=3,
)

# Create producer
producer = EventProducer(config)
producer.connect()

# Send single event
producer.send_event(
    event={"user_id": 123, "action": "login"},
    topic="events",
    key="user-123",
)

# Send batch
producer.send_batch(
    events=[
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ],
    topic="users"
)

# Cleanup
producer.close()
```

## Run tests

```bash
pytest -q
```

All kafka-producer tests use mocks for Kafka, so a live Kafka instance is only required
during testing the api. You can create a Kafka testing instance locally using podman:

```
podman pull apache/kafka:4.1.1

podman run -p 9092:9092 apache/kafka:4.1.1
```