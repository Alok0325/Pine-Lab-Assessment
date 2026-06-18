from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone


def _seed_mixed(client, make_event):
    base = datetime.now(timezone.utc) - timedelta(days=3)
    events = []
    # 3 settled txns for merchant_1
    for i in range(3):
        tx = str(uuid.uuid4())
        events += [
            make_event(
                event_type="payment_initiated",
                transaction_id=tx,
                merchant_id="merchant_1",
                merchant_name="QuickMart",
                amount=100 + i,
                timestamp=base + timedelta(minutes=i),
            ),
            make_event(
                event_type="payment_processed",
                transaction_id=tx,
                merchant_id="merchant_1",
                merchant_name="QuickMart",
                amount=100 + i,
                timestamp=base + timedelta(minutes=i, hours=1),
            ),
            make_event(
                event_type="settled",
                transaction_id=tx,
                merchant_id="merchant_1",
                merchant_name="QuickMart",
                amount=100 + i,
                timestamp=base + timedelta(minutes=i, hours=2),
            ),
        ]
    # 2 failed txns for merchant_2
    for i in range(2):
        tx = str(uuid.uuid4())
        events += [
            make_event(
                event_type="payment_initiated",
                transaction_id=tx,
                merchant_id="merchant_2",
                merchant_name="FreshBasket",
                timestamp=base + timedelta(minutes=10 + i),
            ),
            make_event(
                event_type="payment_failed",
                transaction_id=tx,
                merchant_id="merchant_2",
                merchant_name="FreshBasket",
                timestamp=base + timedelta(minutes=11 + i),
            ),
        ]
    client.post("/events", json=events)


def test_list_transactions_basic(client, make_event):
    _seed_mixed(client, make_event)
    r = client.get("/transactions")
    body = r.json()
    assert r.status_code == 200
    assert body["total"] == 5
    assert len(body["items"]) == 5


def test_filter_by_merchant(client, make_event):
    _seed_mixed(client, make_event)
    r = client.get("/transactions", params={"merchant_id": "merchant_1"})
    body = r.json()
    assert body["total"] == 3
    assert all(t["merchant_id"] == "merchant_1" for t in body["items"])


def test_filter_by_status(client, make_event):
    _seed_mixed(client, make_event)
    r = client.get("/transactions", params={"status": "failed"})
    body = r.json()
    assert body["total"] == 2
    assert all(t["status"] == "failed" for t in body["items"])


def test_pagination(client, make_event):
    _seed_mixed(client, make_event)
    p1 = client.get("/transactions", params={"limit": 2, "offset": 0}).json()
    p2 = client.get("/transactions", params={"limit": 2, "offset": 2}).json()
    assert p1["total"] == p2["total"] == 5
    ids1 = {t["transaction_id"] for t in p1["items"]}
    ids2 = {t["transaction_id"] for t in p2["items"]}
    assert ids1.isdisjoint(ids2)


def test_sort_amount_asc(client, make_event):
    _seed_mixed(client, make_event)
    r = client.get(
        "/transactions",
        params={"merchant_id": "merchant_1", "sort": "amount", "order": "asc"},
    )
    items = r.json()["items"]
    amounts = [float(t["amount"]) for t in items]
    assert amounts == sorted(amounts)


def test_date_range_filter(client, make_event):
    _seed_mixed(client, make_event)
    far_future = (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
    r = client.get("/transactions", params={"start": far_future})
    assert r.json()["total"] == 0


def test_invalid_sort_rejected(client):
    r = client.get("/transactions", params={"sort": "no_such_column"})
    assert r.status_code == 400


def test_transaction_detail_has_events(client, make_event):
    tx = str(uuid.uuid4())
    base = datetime.now(timezone.utc) - timedelta(hours=2)
    events = [
        make_event(event_type="payment_initiated", transaction_id=tx, timestamp=base),
        make_event(
            event_type="payment_processed",
            transaction_id=tx,
            timestamp=base + timedelta(minutes=30),
        ),
    ]
    client.post("/events", json=events)
    r = client.get(f"/transactions/{tx}")
    body = r.json()
    assert r.status_code == 200
    assert body["transaction_id"] == tx
    assert body["event_count"] == 2
    assert body["merchant"] is not None
    assert len(body["events"]) == 2
    # Events sorted by timestamp ascending
    ts = [e["timestamp"] for e in body["events"]]
    assert ts == sorted(ts)


def test_transaction_not_found(client):
    r = client.get("/transactions/does-not-exist")
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "transaction_not_found"
