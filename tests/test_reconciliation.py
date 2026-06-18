from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone


def _ingest_lifecycle(client, make_event, *, merchant_id, merchant_name, types, base):
    tx = str(uuid.uuid4())
    events = []
    for offset, et in enumerate(types):
        events.append(
            make_event(
                event_type=et,
                transaction_id=tx,
                merchant_id=merchant_id,
                merchant_name=merchant_name,
                timestamp=base + timedelta(minutes=offset),
            )
        )
    client.post("/events", json=events)
    return tx


def test_summary_grouped_by_merchant(client, make_event):
    base = datetime.now(timezone.utc) - timedelta(days=2)
    _ingest_lifecycle(
        client,
        make_event,
        merchant_id="merchant_1",
        merchant_name="QuickMart",
        types=["payment_initiated", "payment_processed", "settled"],
        base=base,
    )
    _ingest_lifecycle(
        client,
        make_event,
        merchant_id="merchant_2",
        merchant_name="FreshBasket",
        types=["payment_initiated", "payment_failed"],
        base=base,
    )

    r = client.get("/reconciliation/summary", params={"group_by": "merchant"})
    assert r.status_code == 200
    body = r.json()
    assert body["group_by"] == ["merchant"]
    assert body["totals"]["transaction_count"] == 2
    ids = {row["merchant_id"] for row in body["rows"]}
    assert ids == {"merchant_1", "merchant_2"}


def test_summary_grouped_by_status(client, make_event):
    base = datetime.now(timezone.utc) - timedelta(days=2)
    _ingest_lifecycle(
        client,
        make_event,
        merchant_id="merchant_1",
        merchant_name="QuickMart",
        types=["payment_initiated", "payment_processed", "settled"],
        base=base,
    )
    _ingest_lifecycle(
        client,
        make_event,
        merchant_id="merchant_1",
        merchant_name="QuickMart",
        types=["payment_initiated", "payment_failed"],
        base=base,
    )
    r = client.get("/reconciliation/summary", params={"group_by": "status"})
    body = r.json()
    statuses = {row["status"] for row in body["rows"]}
    assert "settled" in statuses
    assert "failed" in statuses


def test_summary_invalid_group_by(client):
    r = client.get("/reconciliation/summary", params={"group_by": "nonsense"})
    assert r.status_code == 400


def test_discrepancy_settled_after_failure(client, make_event):
    tx = str(uuid.uuid4())
    base = datetime.now(timezone.utc) - timedelta(days=1)
    events = [
        make_event(
            event_type="payment_initiated",
            transaction_id=tx,
            timestamp=base,
        ),
        make_event(
            event_type="payment_failed",
            transaction_id=tx,
            timestamp=base + timedelta(minutes=10),
        ),
        make_event(
            event_type="settled",
            transaction_id=tx,
            timestamp=base + timedelta(minutes=20),
        ),
    ]
    client.post("/events", json=events)

    r = client.get("/reconciliation/discrepancies")
    body = r.json()
    assert body["total"] >= 1
    flagged = next(item for item in body["items"] if item["transaction_id"] == tx)
    assert "settled_after_failure" in flagged["reasons"]


def test_discrepancy_conflicting_payment_states(client, make_event):
    tx = str(uuid.uuid4())
    base = datetime.now(timezone.utc) - timedelta(days=1)
    events = [
        make_event(event_type="payment_initiated", transaction_id=tx, timestamp=base),
        make_event(
            event_type="payment_processed",
            transaction_id=tx,
            timestamp=base + timedelta(minutes=10),
        ),
        make_event(
            event_type="payment_failed",
            transaction_id=tx,
            timestamp=base + timedelta(minutes=20),
        ),
    ]
    client.post("/events", json=events)

    r = client.get(
        "/reconciliation/discrepancies", params={"reason": "conflicting_payment_states"}
    )
    body = r.json()
    assert any(item["transaction_id"] == tx for item in body["items"])


def test_discrepancy_pending_settlement(client, make_event):
    """processed > 24h ago but still no settled event → pending_settlement."""
    tx = str(uuid.uuid4())
    base = datetime.now(timezone.utc) - timedelta(hours=48)
    events = [
        make_event(event_type="payment_initiated", transaction_id=tx, timestamp=base),
        make_event(
            event_type="payment_processed",
            transaction_id=tx,
            timestamp=base + timedelta(minutes=30),
        ),
    ]
    client.post("/events", json=events)

    r = client.get(
        "/reconciliation/discrepancies", params={"reason": "pending_settlement"}
    )
    assert any(item["transaction_id"] == tx for item in r.json()["items"])


def test_no_discrepancy_for_recent_processed(client, make_event):
    """A processed event from 1h ago should NOT be flagged as pending_settlement yet."""
    tx = str(uuid.uuid4())
    base = datetime.now(timezone.utc) - timedelta(hours=1)
    events = [
        make_event(event_type="payment_initiated", transaction_id=tx, timestamp=base),
        make_event(
            event_type="payment_processed",
            transaction_id=tx,
            timestamp=base + timedelta(minutes=10),
        ),
    ]
    client.post("/events", json=events)
    detail = client.get(f"/transactions/{tx}").json()
    assert detail["has_discrepancy"] is False


def test_discrepancy_amount_mismatch(client, make_event):
    tx = str(uuid.uuid4())
    base = datetime.now(timezone.utc) - timedelta(days=1)
    events = [
        make_event(
            event_type="payment_initiated",
            transaction_id=tx,
            amount=100,
            timestamp=base,
        ),
        make_event(
            event_type="payment_processed",
            transaction_id=tx,
            amount=110,  # mismatch
            timestamp=base + timedelta(minutes=10),
        ),
    ]
    client.post("/events", json=events)
    r = client.get("/reconciliation/discrepancies", params={"reason": "amount_mismatch"})
    assert any(item["transaction_id"] == tx for item in r.json()["items"])
