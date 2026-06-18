# Payment Reconciliation Service

A lightweight backend that ingests payment lifecycle events, maintains
derived transaction state, and exposes reconciliation reports for the
operations team. Built for the **Setu Solutions Engineer take-home**.

> **Live demo:** <http://69.62.79.171:8000> (self-hosted via Docker).
> Interactive Swagger UI at <http://69.62.79.171:8000/docs>; liveness at
> <http://69.62.79.171:8000/healthz>. See [Deployment](#deployment) for the self-host steps.

---

## Table of contents

1. [Stack](#stack)
2. [Architecture overview](#architecture-overview)
3. [Data model](#data-model)
4. [Idempotency strategy](#idempotency-strategy)
5. [Discrepancy detection](#discrepancy-detection)
6. [Local setup](#local-setup)
7. [Running the service](#running-the-service)
8. [Seeding](#seeding)
9. [API reference](#api-reference)
10. [Testing](#testing)
11. [Postman collection](#postman-collection)
12. [Deployment](#deployment)
13. [Assumptions and tradeoffs](#assumptions-and-tradeoffs)
14. [AI tool disclosure](#ai-tool-disclosure)
15. [What I would do with more time](#what-i-would-do-with-more-time)

---

## Stack

| Layer       | Choice                                                              |
| ----------- | ------------------------------------------------------------------- |
| Language    | Python 3.12 (3.11 / 3.13 / 3.14 also supported)                     |
| Web         | **FastAPI** + Uvicorn                                               |
| ORM         | SQLAlchemy 2.0 (sync)                                               |
| Database    | **PostgreSQL** in prod, **SQLite** as the zero-setup local default  |
| Validation  | Pydantic v2                                                         |
| Tests       | Pytest + FastAPI `TestClient`                                       |
| Packaging   | `requirements.txt` + Docker / docker-compose                        |
| Deployment  | Self-hosted via Docker / docker-compose (API + Postgres)            |

There are no other moving parts. No Celery, no Redis, no migration framework
(table DDL is generated from the SQLAlchemy models on boot — see the
[tradeoffs](#assumptions-and-tradeoffs) section for why).

## Architecture overview

```
                                 ┌──────────────────────────────┐
  POST /events  ──┐              │           FastAPI            │
                  │              │ ┌──────────────────────────┐ │
  GET  /transactions ────────────┼─► routers (events / txns / │ │
  GET  /transactions/{id}        │ │  reconciliation)         │ │
  GET  /reconciliation/summary   │ └─────────┬────────────────┘ │
  GET  /reconciliation/discrepancies         │                  │
                                 │ ┌─────────▼────────────────┐ │
                                 │ │ app.crud                 │ │
                                 │ │ • idempotent ingest      │ │
                                 │ │ • SQL-driven aggregations│ │
                                 │ │ • derived-state recompute│ │
                                 │ └─────────┬────────────────┘ │
                                 │ ┌─────────▼────────────────┐ │
                                 │ │ SQLAlchemy (sync)        │ │
                                 │ └─────────┬────────────────┘ │
                                 └───────────┼──────────────────┘
                                             │
                              ┌──────────────▼───────────────┐
                              │   PostgreSQL (or SQLite)     │
                              │   merchants / transactions   │
                              │   events                     │
                              └──────────────────────────────┘
```

- **`events`** is the **source of truth** — append-only, idempotent on
  `event_id`. Every other piece of state is derived from it.
- **`transactions`** is a denormalised projection of derived state
  (`status`, `payment_status`, `settlement_status`, `last_event_at`,
  discrepancy flags). It exists so list/sort/filter/summary queries are
  cheap index lookups instead of group-by-event scans.
- The recompute path **only** touches transactions whose events changed
  in the request — a 5000-event batch touches at most 5000 transaction
  rows, fetched and upserted in batches.

## Data model

```sql
-- merchants
CREATE TABLE merchants (
    id              SERIAL PRIMARY KEY,
    merchant_id     VARCHAR(64) UNIQUE NOT NULL,   -- ix_merchants_merchant_id
    name            VARCHAR(255) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- transactions (denormalised projection)
CREATE TABLE transactions (
    id                  SERIAL PRIMARY KEY,
    transaction_id      VARCHAR(64) UNIQUE NOT NULL,
    merchant_id         VARCHAR(64) NOT NULL REFERENCES merchants(merchant_id),
    amount              NUMERIC(18, 2),
    currency            VARCHAR(8),
    status              VARCHAR(32) NOT NULL,      -- initiated|processed|failed|settled|inconsistent|unknown
    payment_status      VARCHAR(32) NOT NULL,      -- none|initiated|processed|failed
    settlement_status   VARCHAR(32) NOT NULL,      -- none|settled
    initiated_at        TIMESTAMPTZ,
    processed_at        TIMESTAMPTZ,
    failed_at           TIMESTAMPTZ,
    settled_at          TIMESTAMPTZ,
    first_event_at      TIMESTAMPTZ,
    last_event_at       TIMESTAMPTZ,
    has_discrepancy     BOOLEAN NOT NULL DEFAULT FALSE,
    discrepancy_reasons TEXT,                      -- pipe-delimited reasons
    event_count         INTEGER NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX ix_transactions_merchant_status     ON transactions(merchant_id, status);
CREATE INDEX ix_transactions_merchant_last_event ON transactions(merchant_id, last_event_at);
CREATE INDEX ix_transactions_status_last_event   ON transactions(status, last_event_at);
CREATE INDEX ix_transactions_discrepancy_merchant ON transactions(has_discrepancy, merchant_id);

-- events (append-only, idempotency-keyed)
CREATE TABLE events (
    id              SERIAL PRIMARY KEY,
    event_id        VARCHAR(64) UNIQUE NOT NULL,   -- ★ idempotency key
    event_type      VARCHAR(32) NOT NULL,
    transaction_id  VARCHAR(64) NOT NULL,
    merchant_id     VARCHAR(64) NOT NULL,
    amount          NUMERIC(18, 2),
    currency        VARCHAR(8),
    timestamp       TIMESTAMPTZ NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX ix_events_transaction_timestamp ON events(transaction_id, timestamp);
CREATE INDEX ix_events_merchant_timestamp    ON events(merchant_id, timestamp);
CREATE INDEX ix_events_type_timestamp        ON events(event_type, timestamp);
```

The composite indexes were chosen for the API access patterns:
- `(merchant_id, status)` → `/transactions?merchant_id=…&status=…`
- `(merchant_id, last_event_at)` → merchant feeds sorted by recency
- `(status, last_event_at)` → status feeds sorted by recency
- `(has_discrepancy, merchant_id)` → `/reconciliation/discrepancies` filter path
- `(transaction_id, timestamp)` on events → transaction detail timeline

## Idempotency strategy

`events.event_id` carries a UNIQUE constraint — duplicates are physically
impossible to land. On every batch:

1. **Pre-check** with `SELECT event_id FROM events WHERE event_id IN (...)`
   so we can return an exact `accepted` vs `duplicates` split in the API
   response. In-batch repeats (same `event_id` in the same request) are
   detected here too.
2. **Bulk-insert** the new rows in one `INSERT ... ON CONFLICT (event_id)
   DO NOTHING` (dialect-aware: works on both SQLite and Postgres). The
   `ON CONFLICT` closes the check-then-insert race — if a concurrent request
   lands the same `event_id` between our pre-check and our write, the insert
   is a safe no-op instead of a 500. The same pattern guards the merchant
   upsert.
3. **Recompute** the affected `transactions` rows from the full event
   history of each touched `transaction_id`. The events table is the
   source of truth, so re-ingesting an already-seen event is a true no-op
   — it can never corrupt transaction state.

The pre-check gives an accurate per-event accepted/duplicate response (with
`?verbose=true`) for partner integrations; the `ON CONFLICT` guarantees the
write is collision-safe even under concurrency.

## Discrepancy detection

The four **structural** (time-invariant) reasons are computed during the
recompute pass and stored as a pipe-delimited list in
`transactions.discrepancy_reasons` (also stamped into the boolean
`has_discrepancy` flag for fast filtering). `pending_settlement` is the one
**time-dependent** rule, so it is evaluated **at query time** in SQL
(`processed_at IS NOT NULL AND settled_at IS NULL AND processed_at < now-24h`)
rather than materialised — this avoids stale flags on a transaction that
crosses the 24h line after its last event without any re-ingest.

| Reason                       | Trigger                                                                            |
| ---------------------------- | ---------------------------------------------------------------------------------- |
| `pending_settlement`         | `payment_processed` recorded >24h ago, no `settled` event yet                      |
| `settled_after_failure`      | A `settled` event exists for a transaction that only ever saw `payment_failed`     |
| `settled_without_payment`    | A `settled` event arrived without any preceding `payment_processed`/`payment_failed` |
| `conflicting_payment_states` | Both `payment_processed` and `payment_failed` events seen for the same transaction |
| `amount_mismatch`            | Events for the same transaction carry different `amount` values                    |

`GET /reconciliation/discrepancies?reason=<name>` filters to a single
reason; the unfiltered call returns every flagged transaction.

## Local setup

Requires Python 3.11+ (tested on 3.12 and 3.14). No Postgres needed for
the default SQLite path.

```bash
git clone <this-repo>
cd Assessment

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env   # optional — defaults are fine for local
```

## Running the service

```bash
# Default: SQLite at ./data/app.db, auto-seeded from sample_events.json on first boot.
uvicorn app.main:app --reload --port 8000
```

Then visit:

- <http://localhost:8000/docs> — Swagger UI
- <http://localhost:8000/redoc> — ReDoc
- <http://localhost:8000/healthz> — liveness probe

### Postgres locally (via docker-compose)

```bash
docker compose up --build
# API on http://localhost:8000, Postgres on localhost:5432
```

## Seeding

On boot (with `AUTO_SEED=true`) the service seeds the database if and only
if the `events` table is empty. Two files are loaded:

1. **`sample_events.json`** — the **provided** assignment file, used
   **as-is, unmodified**: 10,355 events (10,165 unique + 190 intentional
   duplicate `event_id`s) across 5 merchants and 3,800 transactions,
   covering successful flows, failures, pending settlements, and
   settled-after-failure discrepancies.
2. **`sample_events_extra.json`** — a small **supplemental** file I
   generated (131 events / 48 transactions) so the remaining three
   discrepancy reasons — `conflicting_payment_states`,
   `settled_without_payment`, and `amount_mismatch` — are also present in
   the running data. The provided file is left intact rather than edited;
   `ASSIGNMENT.md` explicitly permits generating your own sample data.

Together the running service holds **3,848 transactions** and exercises
**all five** discrepancy reasons end-to-end (523 flagged transactions).
The duplicate `event_id`s in both files are absorbed by the idempotency
layer on seed.

You can also seed manually:

```bash
python -m scripts.load_sample_data sample_events.json          # provided file; no-op if events already present
python -m scripts.load_sample_data sample_events_extra.json --force  # supplemental discrepancy data
```

## API reference

OpenAPI is auto-generated and available at `/docs` and `/openapi.json`.
Summary of endpoints:

### `POST /events` — ingest events

Accepts either a single event object or a JSON array of up to 5000
events. Returns counts and per-event status when `?verbose=true`.

```bash
curl -s http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{
    "event_id": "evt-001",
    "event_type": "payment_initiated",
    "transaction_id": "tx-001",
    "merchant_id": "merchant_1",
    "merchant_name": "QuickMart",
    "amount": 1499.99,
    "currency": "INR",
    "timestamp": "2026-06-01T10:00:00Z"
  }'
```

Response (HTTP 202):

```json
{
  "received": 1,
  "accepted": 1,
  "duplicates": 0,
  "transactions_touched": 1,
  "results": null
}
```

Re-submitting the same payload returns `accepted=0, duplicates=1` — and
the transaction state is unchanged.

### `GET /transactions` — list transactions

Query parameters:

| Param            | Description                                                          |
| ---------------- | -------------------------------------------------------------------- |
| `merchant_id`    | Filter by merchant                                                   |
| `status`         | `initiated` / `processed` / `failed` / `settled` / `inconsistent`    |
| `has_discrepancy`| `true` / `false`                                                     |
| `start`, `end`   | ISO 8601 bounds on `last_event_at`                                   |
| `sort`           | `last_event_at` (default), `first_event_at`, `initiated_at`, `settled_at`, `amount`, `status`, `merchant_id`, `transaction_id`, `created_at`, `updated_at` |
| `order`          | `asc` / `desc` (default `desc`)                                      |
| `limit`          | 1–500 (default 50)                                                   |
| `offset`         | ≥0 (default 0)                                                       |

Returns:

```json
{
  "items": [ /* TransactionOut[] */ ],
  "total": 3800,
  "limit": 50,
  "offset": 0
}
```

### `GET /transactions/{transaction_id}` — transaction detail

Returns the transaction row, the resolved merchant, and the full event
history (sorted by event timestamp ascending). Returns `404` if no such
transaction exists.

### `GET /reconciliation/summary`

Aggregated counts and amounts grouped by any combination of:

- `group_by=merchant`
- `group_by=date` (date bucket of `last_event_at`)
- `group_by=status`

Optional filters: `merchant_id`, `status`, `start`, `end`.

```bash
curl 'http://localhost:8000/reconciliation/summary?group_by=merchant&group_by=status'
```

```json
{
  "group_by": ["merchant", "status"],
  "filters": {},
  "rows": [
    {
      "merchant_id": "merchant_1",
      "merchant_name": "QuickMart",
      "date": null,
      "status": "settled",
      "transaction_count": 510,
      "total_amount": "12686820.60",
      "discrepancy_count": 0
    }
    // ... one row per (merchant, status) combination
  ],
  "totals": {
    "transaction_count": 3848,
    "total_amount": "95277913.19",
    "discrepancy_count": 523
  }
}
```

The grouping, filtering, and aggregation are all done in SQL — no Python
loops over the dataset. `discrepancy_count` includes the query-time
`pending_settlement` rule, so it is evaluated against the current time (the
seed timestamps are fixed in Jan–Apr 2026, so the number is stable for any
review date after that window).

### `GET /reconciliation/discrepancies`

Returns transactions flagged with one or more discrepancy reasons. Filter
by `merchant_id`, `reason`, `start`/`end`, `limit`, `offset`.

```bash
curl 'http://localhost:8000/reconciliation/discrepancies?reason=pending_settlement&limit=10'
```

### Errors

All non-2xx responses share one JSON envelope: `{"error": {"code",
"message"}}`. Request-validation failures (HTTP 422) add a `details` array
with the per-field Pydantic errors. So a bad `reason`, an unknown sort
field, a missing transaction, and an oversized batch all return the same
predictable shape.

## Testing

```bash
.venv/bin/python -m pytest
```

The suite (28 tests, ~0.5s) covers:

- ingestion of single + batch events
- in-request and cross-request idempotency
- validation of malformed events
- end-to-end transaction lifecycle → derived state
- transaction list filters, pagination, sorting, date ranges
- transaction detail with event history + merchant join
- reconciliation summary group-bys + invalid group rejection
- **all five** discrepancy reasons (`pending_settlement`,
  `settled_after_failure`, `settled_without_payment`,
  `conflicting_payment_states`, `amount_mismatch`)
- `pending_settlement` surfacing at query time from a plain read (no
  re-ingest needed)
- invalid `reason` rejected with 422; unified `{"error": {...}}` envelope
- negative case: recent processed events are NOT flagged

Each test gets a fresh on-disk SQLite database (parallel-safe).

## Postman collection

A ready-to-import collection lives at
[`postman/PaymentReconciliation.postman_collection.json`](postman/PaymentReconciliation.postman_collection.json).
Set the `base_url` variable to your deployment URL (or
`http://localhost:8000`).

Every request carries `pm.test` assertions (status code + response body),
so the collection **doubles as an automated smoke test** — 25 requests /
59 assertions covering happy paths, all five discrepancy reasons, and the
422/404/400 error envelope. Run it headless against a seeded instance:

```bash
npx newman run postman/PaymentReconciliation.postman_collection.json \
  --env-var base_url=http://localhost:8000
```

It is safe to re-run (the idempotency check holds on repeat).

Highlights:
- **Ingest single / batch** — auto-generates UUIDs in pre-request scripts.
- **Duplicate event** — uses a fixed `event_id` so two consecutive runs
  demonstrate idempotency.
- **Reject malformed event** — expected 422.
- Filtered transaction list, paginated, sorted.
- Summary by merchant+status, by date.
- Discrepancies all / by each of the five reasons / by merchant, plus an
  invalid-reason request that returns 422.

## Deployment

The service is **self-hosted via Docker** on your own server. The image is
self-contained: it bundles `sample_events.json` and auto-seeds on first
boot, so a fresh host is fully populated with one command.

### Option A — docker-compose (API + Postgres, recommended)

On the server:

```bash
git clone <this-repo> && cd Assessment
docker compose up -d --build
```

This boots the API (port `8000`) and a Postgres 16 instance, waits for the
DB health check, then auto-seeds. The compose file points `DATABASE_URL`
at `postgresql+psycopg://app:app@db:5432/app`. Verify with:

```bash
curl http://<your-server>:8000/healthz      # {"status":"ok"}
open  http://<your-server>:8000/docs         # Swagger UI
```

### Option B — single container + SQLite (zero DB to manage)

```bash
docker build -t payment-reconciliation .
docker run -d -p 8000:8000 \
  -e DATABASE_URL=sqlite:////app/data/app.db \
  -v $(pwd)/data:/app/data \
  payment-reconciliation
```

The volume persists the SQLite file across restarts.

### Notes

- **Any managed/self-hosted Postgres just works.** `DATABASE_URL` accepts a
  bare `postgres://` or `postgresql://` URL — the app normalises the scheme
  to the bundled `psycopg` (v3) driver at startup, so there's no driver
  mismatch to debug.
- **Put it behind a reverse proxy** (nginx/Caddy) for TLS if exposing
  publicly; the app itself serves plain HTTP on `PORT` (default `8000`).
- The reviewer can also run it locally in <2 minutes following
  [Local setup](#local-setup) — no server required.

## Assumptions and tradeoffs

- **One table for events, one denormalised projection for transactions.**
  The projection (`transactions`) is recomputed deterministically from
  events on every ingest. This trades a slightly slower write path for
  much cheaper read queries — list, sort, filter, summary, and
  discrepancy lookups are all single-index scans against a small column
  set.
- **No migration framework (Alembic).** For an assignment scoped to one
  table set, `Base.metadata.create_all` on boot is simpler and faster to
  review. I'd absolutely add Alembic before second prod release.
- **`pending_settlement` uses a 24h window.** Any transaction whose
  `payment_processed` is older than 24h with no `settled` event is
  flagged. The window is a single named constant
  (`PENDING_SETTLEMENT_WINDOW` in `app/crud.py`) — easy to tune.
- **Sync SQLAlchemy.** Async ORM doesn't pay for itself at this scale and
  it would have made the recompute path harder to reason about. Uvicorn
  + sync DB calls in a threadpool is fine for tens of QPS.
- **SQLite for the default local run.** Zero setup. The same code path
  works against Postgres just by changing `DATABASE_URL` — both are
  exercised: tests run on SQLite, `docker-compose` uses Postgres.
- **Status state machine** is intentionally loose: we accept events
  out-of-order (a `payment_processed` can land before `payment_initiated`)
  because real-world event buses often deliver them that way. The
  derived-state computation orders events by `timestamp`, not by arrival.
- **`event_id` is treated as opaque and authoritative.** Partners are
  expected to generate stable IDs per event; we don't fall back to content
  hashing if it's missing.
- **Discrepancy state is split: structural reasons are materialised, the
  time-dependent one is evaluated at query time.** The four time-invariant
  reasons (`conflicting_payment_states`, `settled_after_failure`,
  `settled_without_payment`, `amount_mismatch`) are stamped onto the
  transaction row at ingest, so filtering them is a single-index scan.
  `pending_settlement` depends on wall-clock age, so it is computed in SQL
  on read against `processed_at`/`settled_at`. This deliberately avoids the
  classic staleness bug where a materialised "pending" flag never fires for
  a transaction that crosses the 24h line after its last event. The 24h
  window is a single constant (`PENDING_SETTLEMENT_WINDOW` in `app/crud.py`).
- **No auth, no rate limiting, no CORS lockdown.** The assignment scope is
  a partner-facing service; for production you'd put this behind an API
  gateway (mTLS / signed webhooks) and add Pydantic-level rate limits per
  `merchant_id`.

## AI tool disclosure

Per the assignment prompt:

- **Tool used:** Claude (Anthropic) — Claude Code CLI, model
  `claude-opus-4-7`.
- **How:** I used it as a pair-programmer. Specifically:
  - Drafted the SQLAlchemy schema, Pydantic schemas, and router skeleton
    from the assignment spec.
  - Generated the initial CRUD module and the per-reason discrepancy
    rules.
  - Wrote the first pass of pytest fixtures and tests; I iterated on
    failures.
  - Authored the Postman collection JSON, README, and deployment
    artifacts.
- **What I did myself:** picked the architecture (events as source of
  truth, denormalised projection), chose the indexes based on the API
  access patterns, decided on the idempotency strategy (pre-check vs
  ON CONFLICT), tuned the discrepancy rules, validated against the
  sample data, and ran the test suite to ground-truth the behaviour.
- **What I did not use AI for:** the deployment itself — the service is
  self-hosted via Docker on my own server; the Docker/compose configs are
  mine and the live instance was brought up by hand.

## What I would do with more time

- **Alembic migrations** for schema evolution.
- **A `/admin/recompute` endpoint** to rebuild the materialised structural
  discrepancy flags in bulk if those rules ever change (the time-dependent
  `pending_settlement` rule already needs no rebuild — it's query-time).
- **Webhook authentication** (HMAC signed payloads keyed per merchant)
  so the ingest endpoint is safe to expose publicly.
- **Cursor-based pagination** for `/transactions` to make page navigation
  stable under concurrent writes.
- **Per-status materialised views** in Postgres for the summary
  endpoint, refreshed on a schedule. Currently we re-aggregate from
  `transactions` on every call — fine for the assignment scale, but a
  scheduled refresh would amortise cost at millions of rows.
- **Structured logs + Sentry / OpenTelemetry hooks**, plus a small
  `/metrics` endpoint for Prometheus.
- **End-to-end load test** — feed a 1M-event corpus through `/events`
  and watch ingest latency / DB CPU.
