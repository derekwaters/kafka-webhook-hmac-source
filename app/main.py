import logging
import os
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from lib.kafka_producer import EventProducer, KafkaConfig

logger = logging.getLogger(__name__)

app = FastAPI(title="Example REST Server")

# Initialize Kafka producer from environment or defaults
_kafka_config = KafkaConfig(
    bootstrap_servers=os.getenv("KAFKA_BROKERS", "localhost:9092"),
    topic=os.getenv("KAFKA_TOPIC", "events"),
)
_kafka_producer = EventProducer(_kafka_config)

@app.get("/health")
async def health():
    """Liveness/health endpoint."""
    return {"status": "ok"}

@app.post("/events", status_code=202)
async def post_event(event: object):
    """Post an event to Kafka. Returns 202 Accepted (async)."""
    try:
        _kafka_producer.connect()
        success = _kafka_producer.send_event(event, topic=_kafka_config.topic)
        if not success:
            raise HTTPException(
                status_code=500,
                detail="Failed to publish event to Kafka",
            )
        return {
            "status": "accepted"
        }
    except Exception as e:
        logger.error(f"Error posting event: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    # Run with: python -m app.main or uvicorn app.main:app --reload
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
