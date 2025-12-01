"""
Tests for the Flask REST API endpoints.
"""

import pytest

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


def test_create_and_get_item(client):
    """Test creating and retrieving an item."""
    payload = {"id": "1", "name": "Test", "value": 3.14}
    r = client.post("/items", json=payload)
    assert r.status_code == 201
    body = r.get_json()
    assert body["id"] == "1"

    r2 = client.get("/items/1")
    assert r2.status_code == 200
    assert r2.get_json()["name"] == "Test"


def test_create_duplicate_fails(client):
    """Test that creating duplicate items fails."""
    payload = {"id": "dup", "name": "X"}
    r = client.post("/items", json=payload)
    assert r.status_code == 201

    r2 = client.post("/items", json=payload)
    assert r2.status_code == 400
