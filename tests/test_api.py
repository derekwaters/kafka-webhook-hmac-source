from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_create_and_get_item():
    payload = {"id": "1", "name": "Test", "value": 3.14}
    r = client.post("/items", json=payload)
    assert r.status_code == 201
    body = r.json()
    assert body["id"] == "1"

    r2 = client.get("/items/1")
    assert r2.status_code == 200
    assert r2.json()["name"] == "Test"


def test_create_duplicate_fails():
    payload = {"id": "dup", "name": "X"}
    r = client.post("/items", json=payload)
    assert r.status_code == 201

    r2 = client.post("/items", json=payload)
    assert r2.status_code == 400
