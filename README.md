# Example Python REST server (Flask) with Kafka events

This repository contains a Flask-based REST server with:
- A small in-memory store for items
- Kafka event producer library for publishing events to Kafka topics
- Comprehensive test suite with mocked Kafka integration

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

**Health & Items:**
- `GET /health` — returns `{"status": "ok"}`
- `POST /items` — create an item, JSON body: `{"id": "1", "name": "Foo", "value": 1.23}`
- `GET /items/{id}` — retrieve an item

**Kafka Events:**
- `POST /events` — post an event to Kafka (202 Accepted)
  - JSON body: `{"event_type": "user.login", "payload": {"user_id": 123}, "topic": "events"}`
  - `event_type`: required string describing the event
  - `payload`: required dict with event data
  - `topic`: optional, defaults to `KAFKA_TOPIC` env var or "events"

## Kafka configuration

Set environment variables to control Kafka behavior:

```bash
export KAFKA_BROKERS="localhost:9092"      # Comma-separated brokers
export KAFKA_TOPIC="events"                 # Default topic for events
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

All tests use mocks for Kafka, so no live Kafka instance is required during testing.
