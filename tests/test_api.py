"""
Tests for the Flask REST API endpoints.
"""

import pytest
import hmac
import hashlib
import json

from app.main import app


@pytest.fixture
def client():
    """Fixture providing a Flask test client."""
    app.config["TESTING"] = True
    with app.test_client() as test_client:
        yield test_client


def test_health(client):
    """Test health endpoint."""
    r = client.get("/health")
    assert r.status_code == 200
    assert r.get_json() == {"status": "ok"}

def test_post_event(client):
    """Test posting a new event to Kafka."""
    payload = {"id": "1", "name": "Test", "value": 3.14}
    payload_str = json.dumps(payload).encode('utf-8')

    secret_token = "github_hmac_secret"
    hash_object = hmac.new(secret_token.encode('utf-8'), msg=payload_str, digestmod=hashlib.sha256)
    expected_signature = "sha256=" + hash_object.hexdigest()

    r = client.post("/events", json=payload, headers=[("x-hub-signature-256", expected_signature)])
    assert r.status_code == 202
    body = r.get_json()
    assert body["status"] == "accepted"

def test_post_event_without_hmac(client):
    """Test posting a new event to Kafka."""
    payload = {"id": "1", "name": "Test", "value": 3.14}

    r = client.post("/events", json=payload)
    assert r.status_code == 500

def test_post_event_with_invalid_hmac(client):
    """Test posting a new event to Kafka."""
    payload = {"id": "1", "name": "Test", "value": 3.14}

    r = client.post("/events", json=payload, headers=[("x-hub-signature-256", "dummy_header")])
    assert r.status_code == 500
