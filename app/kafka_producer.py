"""
Kafka producer library for posting events to Kafka topics.

Provides both synchronous and asynchronous interfaces for publishing messages.
"""

import json
import logging
from typing import Any, Dict, List, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


class KafkaConfig:
    """Kafka connection configuration."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "events",
        acks: str = "all",
        retries: int = 3,
    ):
        """
        Initialize Kafka configuration.

        Args:
            bootstrap_servers: Comma-separated Kafka broker addresses.
            topic: Default topic to publish messages to.
            acks: Acknowledgment level ("all", "1", "0").
            retries: Number of retries on send failure.
        """
        self.bootstrap_servers = (
            bootstrap_servers
            if isinstance(bootstrap_servers, list)
            else [s.strip() for s in bootstrap_servers.split(",")]
        )
        self.topic = topic
        self.acks = acks
        self.retries = retries


class EventProducer:
    """
    Synchronous Kafka producer for publishing events to topics.

    Example:
        config = KafkaConfig(bootstrap_servers="localhost:9092", topic="events")
        producer = EventProducer(config)
        producer.send_event({"user_id": 123, "action": "login"})
        producer.close()
    """

    def __init__(self, config: KafkaConfig):
        """
        Initialize the event producer.

        Args:
            config: KafkaConfig instance with connection parameters.
        """
        self.config = config
        self._producer: Optional[KafkaProducer] = None

    def connect(self) -> None:
        """Establish connection to Kafka cluster."""
        if self._producer is not None:
            return

        try:
            self._producer = KafkaProducer(
                bootstrap_servers=self.config.bootstrap_servers,
                acks=self.config.acks,
                retries=self.config.retries,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            logger.info(
                f"Connected to Kafka at {self.config.bootstrap_servers}",
            )
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def send_event(
        self,
        event: Dict[str, Any],
        topic: Optional[str] = None,
        key: Optional[str] = None,
    ) -> bool:
        """
        Send an event to Kafka.

        Args:
            event: Dictionary event payload to publish.
            topic: Target topic (uses config default if None).
            key: Optional message key for partitioning.

        Returns:
            True if sent successfully, False otherwise.

        Raises:
            RuntimeError: If not connected to Kafka.
        """
        if self._producer is None:
            raise RuntimeError(
                "Producer not connected. Call connect() first.",
            )

        topic = topic or self.config.topic
        try:
            future = self._producer.send(
                topic,
                value=event,
                key=key.encode("utf-8") if key else None,
            )
            # Wait for the send to complete (with timeout to avoid blocking forever)
            future.get(timeout=10)
            logger.debug(f"Event sent to {topic}: {event}")
            return True
        except KafkaError as e:
            logger.error(f"Failed to send event to {topic}: {e}")
            return False

    def send_batch(
        self,
        events: List[Dict[str, Any]],
        topic: Optional[str] = None,
    ) -> int:
        """
        Send multiple events to Kafka.

        Args:
            events: List of event dictionaries.
            topic: Target topic (uses config default if None).

        Returns:
            Number of events successfully sent.
        """
        sent_count = 0
        for event in events:
            if self.send_event(event, topic=topic):
                sent_count += 1
        return sent_count

    def close(self) -> None:
        """Close the producer connection and flush pending messages."""
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()
            self._producer = None
            logger.info("Producer connection closed.")
