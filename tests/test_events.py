from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone


def test_ingest_single_event(client, make_event):
    e = make_event()
    r = client.post("/events", json=e)
    assert r.status_code == 202, r.text
    body = r.json()
    assert body["received"] == 1
    assert body["accepted"] == 1
    assert body["duplicates"] == 0
    assert body["transactions_touched"] == 1


def test_ingest_batch(client, make_event):
    events = [make_event() for _ in range(5)]
    r = client.post("/events", json=events)
    assert r.status_code == 202, r.text
    body = r.json()
    assert body["accepted"] == 5
    assert body["duplicates"] == 0


def test_event_idempotency_same_request(client, make_event):
    """Submitting the same event twice in one call only inserts it once."""
    e = make_event()
    r = client.post("/events", json=[e, e, e], params={"verbose": "true"})
    body = r.json()
    assert body["accepted"] == 1
    assert body["duplicates"] == 2
    statuses = [item["status"] for item in body["results"]]
    assert statuses.count("accepted") == 1
    assert statuses.count("duplicate") == 2


def test_event_idempotency_across_requests(client, make_event):
    e = make_event()
    client.post("/events", json=e)
    r = client.post("/events", json=e)
    body = r.json()
    assert body["accepted"] == 0
    assert body["duplicates"] == 1


def test_invalid_event_type_rejected(client, make_event):
    e = make_event(event_type="not_a_real_event")
    r = client.post("/events", json=e)
    assert r.status_code == 422


def test_negative_amount_rejected(client, make_event):
    e = make_event(amount=-1)
    r = client.post("/events", json=e)
    assert r.status_code == 422


def test_empty_batch_rejected(client):
    r = client.post("/events", json=[])
    assert r.status_code == 400


def test_full_lifecycle_creates_settled_transaction(client, make_event):
    tx = str(uuid.uuid4())
    base = datetime.now(timezone.utc) - timedelta(hours=2)
    events = [
        make_event(
            event_type="payment_initiated",
            transaction_id=tx,
            timestamp=base,
        ),
        make_event(
            event_type="payment_processed",
            transaction_id=tx,
            timestamp=base + timedelta(minutes=30),
        ),
        make_event(
            event_type="settled",
            transaction_id=tx,
            timestamp=base + timedelta(hours=1),
        ),
    ]
    r = client.post("/events", json=events)
    assert r.status_code == 202

    detail = client.get(f"/transactions/{tx}").json()
    assert detail["status"] == "settled"
    assert detail["payment_status"] == "processed"
    assert detail["settlement_status"] == "settled"
    assert detail["has_discrepancy"] is False
    assert detail["event_count"] == 3
    assert len(detail["events"]) == 3
