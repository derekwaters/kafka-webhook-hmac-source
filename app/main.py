"""
Flask-based REST server with Kafka event publishing.
"""

import hashlib
import hmac
import logging
import os
from typing import Any, Dict, Optional, Tuple

from flask import Flask, jsonify, request

from lib.kafka_producer import EventProducer, KafkaConfig

logger = logging.getLogger(__name__)

app = Flask(__name__)

# Initialize Kafka producer from environment or defaults
_kafka_config = KafkaConfig(
    bootstrap_servers=os.getenv("KAFKA_BROKERS", "localhost:9092"),
    topic=os.getenv("KAFKA_TOPIC", "events"),
)
_kafka_producer = EventProducer(_kafka_config)

_github_hmac_header = os.getenv("HMAC_HEADER", "x-hub-signature-256")
_github_secret = os.getenv("HMAC_SECRET", "github_hmac_secret")


# Validate HMAC Signatures
def verify_signature(payload_body, secret_token, signature_header):
    """Verify that the payload was sent from GitHub by validating SHA256.

    Raise ValueError if not authorized.

    Args:
        payload_body: original request body to verify (request.body())
        secret_token: GitHub app webhook token (WEBHOOK_SECRET)
        signature_header: header received from GitHub (x-hub-signature-256)
    """
    if not signature_header:
        raise ValueError(f"{_github_hmac_header} header is missing!")
    hash_object = hmac.new(
        secret_token.encode("utf-8"),
        msg=payload_body,
        digestmod=hashlib.sha256,
    )
    expected_signature = "sha256=" + hash_object.hexdigest()
    if not hmac.compare_digest(expected_signature, signature_header):
        raise ValueError("Request signatures didn't match!")


@app.route("/health", methods=["GET"])
def health() -> Tuple[Dict[str, str], int]:
    """Liveness/health endpoint."""
    return jsonify({"status": "ok"}), 200


@app.route("/events", methods=["POST"])
def post_event() -> Tuple[Dict[str, Any], int]:
    """Post an event to Kafka. Returns 202 Accepted (async)."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Request body required"}), 400

        # Check the HMAC GitHub webhook header
        hmac_header = request.headers.get(_github_hmac_header)
        verify_signature(request.get_data(), _github_secret, hmac_header)

        _kafka_producer.connect()
        success = _kafka_producer.send_event(data, topic=_kafka_config.topic)
        if not success:
            return (
                jsonify(
                    {"error": "Failed to publish event to Kafka"},
                ),
                500,
            )
        return (
            jsonify(
                {
                    "status": "accepted"
                },
            ),
            202,
        )
    except Exception as e:
        logger.error(f"Error posting event: {e}")
        return jsonify({"error": str(e)}), 500


@app.teardown_appcontext
def cleanup(error: Optional[Exception]) -> None:
    """Clean up Kafka producer on app shutdown."""
    _kafka_producer.close()


if __name__ == "__main__":
    # Run with: python -m app.main or flask run
    app.run(host="0.0.0.0", port=8000, debug=True)

