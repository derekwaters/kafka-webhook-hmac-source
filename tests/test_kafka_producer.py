"""
Tests for the Kafka event producer library.
"""

from unittest.mock import MagicMock, patch

import pytest

from lib.kafka_producer import EventProducer, KafkaConfig


@pytest.fixture
def kafka_config():
    """Fixture providing a test Kafka config."""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        topic="test-events",
        acks="1",
        retries=1,
    )


@pytest.fixture
def mock_producer(kafka_config):
    """Fixture providing an EventProducer with mocked KafkaProducer."""
    with patch("lib.kafka_producer.KafkaProducer") as mock_kafka:
        mock_instance = MagicMock()
        mock_kafka.return_value = mock_instance
        producer = EventProducer(kafka_config)
        producer.connect()
        yield producer, mock_instance


def test_kafka_config_init():
    """Test KafkaConfig initialization."""
    config = KafkaConfig(
        bootstrap_servers="broker1:9092,broker2:9092",
        topic="my-topic",
    )
    assert config.bootstrap_servers == ["broker1:9092", "broker2:9092"]
    assert config.topic == "my-topic"
    assert config.acks == "all"
    assert config.retries == 3


def test_kafka_config_single_broker():
    """Test KafkaConfig with single bootstrap server."""
    config = KafkaConfig(bootstrap_servers="localhost:9092")
    assert config.bootstrap_servers == ["localhost:9092"]


def test_producer_connect(kafka_config):
    """Test that producer can connect."""
    with patch("lib.kafka_producer.KafkaProducer") as mock_kafka:
        mock_instance = MagicMock()
        mock_kafka.return_value = mock_instance

        producer = EventProducer(kafka_config)
        producer.connect()

        assert producer._producer is not None
        mock_kafka.assert_called_once()
        call_kwargs = mock_kafka.call_args[1]
        assert call_kwargs["bootstrap_servers"] == ["localhost:9092"]
        assert call_kwargs["acks"] == "1"


def test_send_event_success(mock_producer):
    """Test sending a single event."""
    producer, mock_instance = mock_producer

    mock_future = MagicMock()
    mock_instance.send.return_value = mock_future

    event = {"user_id": 123, "action": "login"}
    result = producer.send_event(event)

    assert result is True
    mock_instance.send.assert_called_once()
    args, kwargs = mock_instance.send.call_args
    assert args[0] == "test-events"
    assert kwargs["value"] == event


def test_send_event_with_custom_topic(mock_producer):
    """Test sending an event to a custom topic."""
    producer, mock_instance = mock_producer

    mock_future = MagicMock()
    mock_instance.send.return_value = mock_future

    event = {"message": "hello"}
    producer.send_event(event, topic="custom-topic")

    args, kwargs = mock_instance.send.call_args
    assert args[0] == "custom-topic"


def test_send_event_with_key(mock_producer):
    """Test sending an event with a partition key."""
    producer, mock_instance = mock_producer

    mock_future = MagicMock()
    mock_instance.send.return_value = mock_future

    event = {"data": "value"}
    producer.send_event(event, key="user-123")

    args, kwargs = mock_instance.send.call_args
    assert kwargs["key"] == b"user-123"


def test_send_event_not_connected(kafka_config):
    """Test that send_event raises if not connected."""
    producer = EventProducer(kafka_config)

    with pytest.raises(RuntimeError, match="not connected"):
        producer.send_event({"test": "event"})


def test_send_event_kafka_error(mock_producer):
    """Test handling of Kafka send error."""
    producer, mock_instance = mock_producer

    from kafka.errors import KafkaError

    mock_future = MagicMock()
    mock_future.get.side_effect = KafkaError("Connection failed")
    mock_instance.send.return_value = mock_future

    result = producer.send_event({"test": "event"})

    assert result is False


def test_send_batch(mock_producer):
    """Test sending multiple events."""
    producer, mock_instance = mock_producer

    mock_future = MagicMock()
    mock_instance.send.return_value = mock_future

    events = [
        {"user_id": 1, "action": "login"},
        {"user_id": 2, "action": "logout"},
        {"user_id": 3, "action": "update"},
    ]

    sent_count = producer.send_batch(events)

    assert sent_count == 3
    assert mock_instance.send.call_count == 3


def test_close(mock_producer):
    """Test closing the producer."""
    producer, mock_instance = mock_producer

    producer.close()

    mock_instance.flush.assert_called_once()
    mock_instance.close.assert_called_once()
    assert producer._producer is None


def test_close_when_not_connected(kafka_config):
    """Test that close works safely even if not connected."""
    producer = EventProducer(kafka_config)
    producer.close()  # Should not raise
    assert producer._producer is None
