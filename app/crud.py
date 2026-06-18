"""Persistence layer.

Idempotency strategy
--------------------
``events.event_id`` carries a UNIQUE constraint. On every batch we:

1. Resolve which incoming ``event_id``s already exist with a single
   ``SELECT event_id FROM events WHERE event_id IN (...)`` and skip them.
2. Bulk-insert the new rows.
3. Recompute the derived ``transactions`` row for every affected
   ``transaction_id`` from the *full* event history (the events table is
   the source of truth).

The two-step approach is portable across SQLite and Postgres, returns an
exact accepted/duplicate split (useful for the API response), and never
risks corrupting transaction state on re-submission.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Iterable, Sequence

from sqlalchemy import (
    Select,
    and_,
    asc,
    case,
    desc,
    func,
    or_,
    select,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from app import models, schemas

PENDING_SETTLEMENT_WINDOW = timedelta(hours=24)

# Order of precedence when a transaction is in a clearly terminal payment state.
_PAYMENT_RANK = {
    "none": 0,
    "initiated": 1,
    "processed": 2,
    "failed": 2,  # treated equal — both are terminal payment-level outcomes
}

_ALLOWED_SORT_FIELDS = {
    "last_event_at": models.Transaction.last_event_at,
    "first_event_at": models.Transaction.first_event_at,
    "initiated_at": models.Transaction.initiated_at,
    "settled_at": models.Transaction.settled_at,
    "amount": models.Transaction.amount,
    "status": models.Transaction.status,
    "merchant_id": models.Transaction.merchant_id,
    "updated_at": models.Transaction.updated_at,
    "created_at": models.Transaction.created_at,
    "transaction_id": models.Transaction.transaction_id,
}

# The five discrepancy reasons surfaced by the API. ``pending_settlement`` is the
# only time-dependent one, so it is evaluated at query time (see below) rather
# than materialised; the other four are structural and stamped at ingest.
DISCREPANCY_REASONS = (
    "pending_settlement",
    "settled_after_failure",
    "settled_without_payment",
    "conflicting_payment_states",
    "amount_mismatch",
)


def _insert_ignore_conflicts(session: Session, model, rows: list[dict], index_elements: list[str]) -> None:
    """Bulk INSERT that silently skips rows colliding on a unique key.

    Closes the check-then-insert race: even if a concurrent request inserts the
    same ``event_id`` / ``merchant_id`` between our pre-check and our write, the
    ON CONFLICT DO NOTHING makes the insert a safe no-op instead of a 500. The
    events table is the source of truth, so a skipped row is recomputed correctly
    from the full history regardless.
    """
    if not rows:
        return
    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":
        stmt = pg_insert(model).values(rows).on_conflict_do_nothing(index_elements=index_elements)
        session.execute(stmt)
    elif dialect == "sqlite":
        stmt = sqlite_insert(model).values(rows).on_conflict_do_nothing(index_elements=index_elements)
        session.execute(stmt)
    else:  # pragma: no cover - portability fallback
        session.execute(model.__table__.insert(), rows)


# ---------------- Pending-settlement (evaluated at query time) ---------------- #


def pending_settlement_cutoff(now: datetime | None = None) -> datetime:
    """Instant before which an unsettled-but-processed txn counts as pending."""
    return (now or datetime.now(timezone.utc)) - PENDING_SETTLEMENT_WINDOW


def _pending_settlement_sql(cutoff: datetime):
    """SQL predicate: processed, not settled, and processed more than 24h ago."""
    return and_(
        models.Transaction.processed_at.isnot(None),
        models.Transaction.settled_at.is_(None),
        models.Transaction.processed_at <= cutoff,
    )


def is_pending_settlement(tx: models.Transaction, cutoff: datetime) -> bool:
    return (
        tx.processed_at is not None
        and tx.settled_at is None
        and _ensure_aware(tx.processed_at) <= cutoff
    )


# ---------------- Merchants ---------------- #


def upsert_merchants_from_events(session: Session, events: Iterable[schemas.EventIn]) -> int:
    """Insert any merchants seen in the batch that don't already exist.

    We only set the name on first sight — later events that carry the same
    ``merchant_id`` with a different display name don't overwrite the row.
    """
    pairs: dict[str, str | None] = {}
    for e in events:
        pairs.setdefault(e.merchant_id, e.merchant_name)

    if not pairs:
        return 0

    existing = {
        m
        for m, in session.execute(
            select(models.Merchant.merchant_id).where(
                models.Merchant.merchant_id.in_(pairs.keys())
            )
        ).all()
    }

    new = [
        {"merchant_id": mid, "name": name or mid}
        for mid, name in pairs.items()
        if mid not in existing
    ]
    if new:
        _insert_ignore_conflicts(session, models.Merchant, new, ["merchant_id"])
        session.flush()
    return len(new)


# ---------------- Events ---------------- #


def _split_new_vs_duplicate(
    session: Session, events: Sequence[schemas.EventIn]
) -> tuple[list[schemas.EventIn], set[str]]:
    if not events:
        return [], set()

    incoming_ids = [e.event_id for e in events]
    existing_ids: set[str] = {
        eid
        for eid, in session.execute(
            select(models.Event.event_id).where(models.Event.event_id.in_(incoming_ids))
        ).all()
    }

    # Drop in-batch duplicates while preserving order.
    seen: set[str] = set()
    new: list[schemas.EventIn] = []
    duplicates: set[str] = set()
    for e in events:
        if e.event_id in existing_ids or e.event_id in seen:
            duplicates.add(e.event_id)
            continue
        seen.add(e.event_id)
        new.append(e)

    return new, duplicates


def ingest_events(
    session: Session,
    events: Sequence[schemas.EventIn],
    *,
    return_per_event: bool = False,
) -> schemas.EventBatchResult:
    if not events:
        return schemas.EventBatchResult(
            received=0,
            accepted=0,
            duplicates=0,
            transactions_touched=0,
            results=[] if return_per_event else None,
        )

    upsert_merchants_from_events(session, events)

    new_events, duplicate_ids = _split_new_vs_duplicate(session, events)

    if new_events:
        _insert_ignore_conflicts(
            session,
            models.Event,
            [
                {
                    "event_id": e.event_id,
                    "event_type": e.event_type.value,
                    "transaction_id": e.transaction_id,
                    "merchant_id": e.merchant_id,
                    "amount": e.amount,
                    "currency": e.currency,
                    "timestamp": _ensure_aware(e.timestamp),
                }
                for e in new_events
            ],
            ["event_id"],
        )

    affected_tx = {e.transaction_id for e in new_events}
    if affected_tx:
        recompute_transactions(session, affected_tx)

    session.commit()

    results: list[schemas.EventIngestResult] | None = None
    if return_per_event:
        accepted_set = {e.event_id for e in new_events}
        # Mark only the *first* occurrence of each accepted id as "accepted";
        # any repeats within the same request are duplicates too.
        seen_accepted: set[str] = set()
        results = []
        for e in events:
            if e.event_id in accepted_set and e.event_id not in seen_accepted:
                seen_accepted.add(e.event_id)
                results.append(
                    schemas.EventIngestResult(event_id=e.event_id, status="accepted")
                )
            else:
                results.append(
                    schemas.EventIngestResult(event_id=e.event_id, status="duplicate")
                )

    return schemas.EventBatchResult(
        received=len(events),
        accepted=len(new_events),
        duplicates=len(events) - len(new_events),
        transactions_touched=len(affected_tx),
        results=results,
    )


# ---------------- Transaction state derivation ---------------- #


def _ensure_aware(ts: datetime) -> datetime:
    """Canonicalise to UTC. Naive inputs are assumed UTC; aware inputs are
    converted, so the stored instant is correct regardless of the source offset
    (SQLite would otherwise drop the offset and keep the wall-clock value)."""
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _derive_state(events: list[models.Event]) -> dict:
    """Compute transaction-level derived fields from a list of events."""
    events = sorted(events, key=lambda x: x.timestamp)

    first_event_at = events[0].timestamp
    last_event_at = events[-1].timestamp

    initiated_at: datetime | None = None
    processed_at: datetime | None = None
    failed_at: datetime | None = None
    settled_at: datetime | None = None

    has_initiated = has_processed = has_failed = has_settled = False
    amount: Decimal | None = None
    currency: str | None = None

    for e in events:
        if e.amount is not None and amount is None:
            amount = e.amount
        if e.currency and currency is None:
            currency = e.currency

        et = e.event_type
        if et == "payment_initiated":
            has_initiated = True
            initiated_at = e.timestamp if initiated_at is None else min(initiated_at, e.timestamp)
        elif et == "payment_processed":
            has_processed = True
            processed_at = e.timestamp if processed_at is None else min(processed_at, e.timestamp)
        elif et == "payment_failed":
            has_failed = True
            failed_at = e.timestamp if failed_at is None else min(failed_at, e.timestamp)
        elif et == "settled":
            has_settled = True
            settled_at = e.timestamp if settled_at is None else min(settled_at, e.timestamp)

    payment_status = "none"
    if has_failed and has_processed:
        # Both terminal states recorded — keep "processed" as the latest *successful*
        # signal but the discrepancy logic will flag this.
        payment_status = "processed"
    elif has_processed:
        payment_status = "processed"
    elif has_failed:
        payment_status = "failed"
    elif has_initiated:
        payment_status = "initiated"

    settlement_status = "settled" if has_settled else "none"

    # Only *structural* (time-invariant) discrepancies are materialised here.
    # `pending_settlement` depends on wall-clock age, so it is evaluated at query
    # time against processed_at/settled_at — see _pending_settlement_sql — which
    # avoids stale flags on transactions that cross the 24h line after their last
    # event without any re-ingest.
    reasons: list[str] = []
    if has_processed and has_failed:
        reasons.append("conflicting_payment_states")
    if has_settled and has_failed and not has_processed:
        reasons.append("settled_after_failure")
    if has_settled and not has_processed and not has_failed:
        reasons.append("settled_without_payment")
    # Amount mismatch across events.
    amounts = {e.amount for e in events if e.amount is not None}
    if len(amounts) > 1:
        reasons.append("amount_mismatch")

    has_discrepancy = bool(reasons)

    # Reasons that indicate the transaction is fundamentally inconsistent
    # (rather than just "in flight"). `pending_settlement` is a soft flag —
    # the payment side is healthy, settlement is just late.
    hard_reasons = {
        "conflicting_payment_states",
        "settled_after_failure",
        "settled_without_payment",
        "amount_mismatch",
    }
    if hard_reasons.intersection(reasons):
        status = "inconsistent"
    elif has_settled:
        status = "settled"
    elif has_processed:
        status = "processed"
    elif has_failed:
        status = "failed"
    elif has_initiated:
        status = "initiated"
    else:
        status = "unknown"

    return {
        "amount": amount,
        "currency": currency,
        "status": status,
        "payment_status": payment_status,
        "settlement_status": settlement_status,
        "initiated_at": initiated_at,
        "processed_at": processed_at,
        "failed_at": failed_at,
        "settled_at": settled_at,
        "first_event_at": first_event_at,
        "last_event_at": last_event_at,
        "has_discrepancy": has_discrepancy,
        "discrepancy_reasons": "|".join(reasons) if reasons else None,
        "event_count": len(events),
    }


def recompute_transactions(session: Session, transaction_ids: Iterable[str]) -> int:
    """Recompute derived state for the given transactions from their events.

    A single ``SELECT * FROM events WHERE transaction_id IN (...)`` is issued
    regardless of how many transactions are affected, then the per-transaction
    derived dicts are upserted in bulk.
    """
    ids = list({tid for tid in transaction_ids if tid})
    if not ids:
        return 0

    rows = (
        session.execute(
            select(models.Event).where(models.Event.transaction_id.in_(ids))
        )
        .scalars()
        .all()
    )

    by_tx: dict[str, list[models.Event]] = defaultdict(list)
    for r in rows:
        by_tx[r.transaction_id].append(r)

    existing = {
        t.transaction_id: t
        for t in session.execute(
            select(models.Transaction).where(models.Transaction.transaction_id.in_(ids))
        )
        .scalars()
        .all()
    }

    touched = 0
    for tid in ids:
        evs = by_tx.get(tid, [])
        if not evs:
            continue
        derived = _derive_state(evs)
        merchant_id = evs[0].merchant_id
        if tid in existing:
            t = existing[tid]
            for k, v in derived.items():
                setattr(t, k, v)
            # Merchant should not change; if it does we keep first one for stability.
        else:
            t = models.Transaction(
                transaction_id=tid,
                merchant_id=merchant_id,
                **derived,
            )
            session.add(t)
        touched += 1

    session.flush()
    return touched


# ---------------- Transaction reads ---------------- #


def list_transactions(
    session: Session,
    *,
    merchant_id: str | None = None,
    status: str | None = None,
    has_discrepancy: bool | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    sort: str = "last_event_at",
    order: str = "desc",
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[models.Transaction], int]:
    if sort not in _ALLOWED_SORT_FIELDS:
        raise ValueError(
            f"Unsupported sort field '{sort}'. Allowed: {sorted(_ALLOWED_SORT_FIELDS)}"
        )
    order_dir = asc if order.lower() == "asc" else desc

    stmt: Select = select(models.Transaction)

    if merchant_id:
        stmt = stmt.where(models.Transaction.merchant_id == merchant_id)
    if status:
        stmt = stmt.where(models.Transaction.status == status)
    if has_discrepancy is not None:
        flagged = or_(
            models.Transaction.has_discrepancy.is_(True),
            _pending_settlement_sql(pending_settlement_cutoff()),
        )
        stmt = stmt.where(flagged if has_discrepancy else ~flagged)
    if start:
        stmt = stmt.where(models.Transaction.last_event_at >= start)
    if end:
        stmt = stmt.where(models.Transaction.last_event_at <= end)

    total = session.execute(
        select(func.count()).select_from(stmt.subquery())
    ).scalar_one()

    rows = (
        session.execute(
            stmt.order_by(order_dir(_ALLOWED_SORT_FIELDS[sort]), desc(models.Transaction.id))
            .limit(limit)
            .offset(offset)
        )
        .scalars()
        .all()
    )

    return list(rows), int(total)


def get_transaction(session: Session, transaction_id: str) -> models.Transaction | None:
    return (
        session.execute(
            select(models.Transaction).where(
                models.Transaction.transaction_id == transaction_id
            )
        )
        .scalars()
        .first()
    )


def get_merchant(session: Session, merchant_id: str) -> models.Merchant | None:
    return (
        session.execute(
            select(models.Merchant).where(models.Merchant.merchant_id == merchant_id)
        )
        .scalars()
        .first()
    )


# ---------------- Reconciliation ---------------- #


_GROUP_DIMENSIONS = {"merchant", "date", "status"}


def reconciliation_summary(
    session: Session,
    *,
    group_by: list[str] | None = None,
    merchant_id: str | None = None,
    status: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> schemas.ReconciliationSummary:
    group_by = group_by or ["merchant", "status"]
    invalid = set(group_by) - _GROUP_DIMENSIONS
    if invalid:
        raise ValueError(
            f"Invalid group_by dimensions: {sorted(invalid)}. Allowed: {sorted(_GROUP_DIMENSIONS)}"
        )

    cols: list = []
    label_map: dict[str, str] = {}
    if "merchant" in group_by:
        cols.append(models.Transaction.merchant_id.label("merchant_id"))
        cols.append(models.Merchant.name.label("merchant_name"))
    if "date" in group_by:
        cols.append(func.date(models.Transaction.last_event_at).label("date"))
    if "status" in group_by:
        cols.append(models.Transaction.status.label("status"))

    flagged = or_(
        models.Transaction.has_discrepancy.is_(True),
        _pending_settlement_sql(pending_settlement_cutoff()),
    )
    cols.extend(
        [
            func.count(models.Transaction.id).label("transaction_count"),
            func.coalesce(func.sum(models.Transaction.amount), 0).label("total_amount"),
            func.sum(case((flagged, 1), else_=0)).label("discrepancy_count"),
        ]
    )

    stmt = select(*cols).select_from(
        models.Transaction.__table__.join(
            models.Merchant.__table__,
            models.Transaction.merchant_id == models.Merchant.merchant_id,
        )
    )

    conds = []
    if merchant_id:
        conds.append(models.Transaction.merchant_id == merchant_id)
    if status:
        conds.append(models.Transaction.status == status)
    if start:
        conds.append(models.Transaction.last_event_at >= start)
    if end:
        conds.append(models.Transaction.last_event_at <= end)
    if conds:
        stmt = stmt.where(and_(*conds))

    group_cols = []
    if "merchant" in group_by:
        group_cols.extend([models.Transaction.merchant_id, models.Merchant.name])
    if "date" in group_by:
        group_cols.append(func.date(models.Transaction.last_event_at))
    if "status" in group_by:
        group_cols.append(models.Transaction.status)

    stmt = stmt.group_by(*group_cols)

    # Stable ordering for deterministic responses.
    order_cols = []
    if "merchant" in group_by:
        order_cols.append(asc(models.Transaction.merchant_id))
    if "date" in group_by:
        order_cols.append(asc(func.date(models.Transaction.last_event_at)))
    if "status" in group_by:
        order_cols.append(asc(models.Transaction.status))
    stmt = stmt.order_by(*order_cols)

    rows_raw = session.execute(stmt).mappings().all()

    rows: list[schemas.SummaryRow] = []
    totals_count = 0
    totals_amount = Decimal("0")
    totals_disc = 0
    for r in rows_raw:
        d = dict(r)
        date_val = d.get("date")
        if isinstance(date_val, str):
            try:
                date_val = datetime.fromisoformat(date_val).date()
            except ValueError:
                date_val = None
        rows.append(
            schemas.SummaryRow(
                merchant_id=d.get("merchant_id"),
                merchant_name=d.get("merchant_name"),
                date=date_val,
                status=d.get("status"),
                transaction_count=int(d["transaction_count"]),
                total_amount=Decimal(str(d["total_amount"] or 0)),
                discrepancy_count=int(d["discrepancy_count"] or 0),
            )
        )
        totals_count += int(d["transaction_count"])
        totals_amount += Decimal(str(d["total_amount"] or 0))
        totals_disc += int(d["discrepancy_count"] or 0)

    filters = {
        k: v
        for k, v in {
            "merchant_id": merchant_id,
            "status": status,
            "start": start.isoformat() if start else None,
            "end": end.isoformat() if end else None,
        }.items()
        if v is not None
    }

    return schemas.ReconciliationSummary(
        group_by=group_by,
        filters=filters,
        rows=rows,
        totals={
            "transaction_count": totals_count,
            "total_amount": str(totals_amount),
            "discrepancy_count": totals_disc,
        },
    )


def list_discrepancies(
    session: Session,
    *,
    merchant_id: str | None = None,
    reason: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 50,
    offset: int = 0,
) -> schemas.DiscrepancyResponse:
    cutoff = pending_settlement_cutoff()
    pending_sql = _pending_settlement_sql(cutoff)

    if reason == "pending_settlement":
        flag_filter = pending_sql
    elif reason:
        # A specific structural reason. The five reason names are never
        # substrings of one another, so a LIKE on the pipe-delimited column is
        # unambiguous (and `reason` is enum-validated upstream).
        flag_filter = and_(
            models.Transaction.has_discrepancy.is_(True),
            models.Transaction.discrepancy_reasons.like(f"%{reason}%"),
        )
    else:
        flag_filter = or_(models.Transaction.has_discrepancy.is_(True), pending_sql)

    stmt = (
        select(models.Transaction, models.Merchant.name.label("merchant_name"))
        .join(
            models.Merchant,
            models.Merchant.merchant_id == models.Transaction.merchant_id,
            isouter=True,
        )
        .where(flag_filter)
    )
    if merchant_id:
        stmt = stmt.where(models.Transaction.merchant_id == merchant_id)
    if start:
        stmt = stmt.where(models.Transaction.last_event_at >= start)
    if end:
        stmt = stmt.where(models.Transaction.last_event_at <= end)

    total = session.execute(
        select(func.count()).select_from(stmt.subquery())
    ).scalar_one()

    rows = session.execute(
        stmt.order_by(desc(models.Transaction.last_event_at), desc(models.Transaction.id))
        .limit(limit)
        .offset(offset)
    ).all()

    items: list[schemas.DiscrepancyRow] = []
    for tx, merchant_name in rows:
        reasons = (
            [r for r in tx.discrepancy_reasons.split("|") if r]
            if tx.discrepancy_reasons
            else []
        )
        if is_pending_settlement(tx, cutoff) and "pending_settlement" not in reasons:
            reasons.append("pending_settlement")
        items.append(
            schemas.DiscrepancyRow(
                transaction_id=tx.transaction_id,
                merchant_id=tx.merchant_id,
                merchant_name=merchant_name,
                amount=tx.amount,
                currency=tx.currency,
                status=tx.status,
                payment_status=tx.payment_status,
                settlement_status=tx.settlement_status,
                reasons=reasons,
                first_event_at=tx.first_event_at,
                last_event_at=tx.last_event_at,
                event_count=tx.event_count,
            )
        )

    filters = {
        k: v
        for k, v in {
            "merchant_id": merchant_id,
            "reason": reason,
            "start": start.isoformat() if start else None,
            "end": end.isoformat() if end else None,
        }.items()
        if v is not None
    }

    return schemas.DiscrepancyResponse(
        filters=filters,
        total=int(total),
        limit=limit,
        offset=offset,
        items=items,
    )


# ---------------- Misc ---------------- #


def get_transaction_events(
    session: Session, transaction_id: str
) -> list[models.Event]:
    return list(
        session.execute(
            select(models.Event)
            .where(models.Event.transaction_id == transaction_id)
            .order_by(models.Event.timestamp.asc(), models.Event.id.asc())
        )
        .scalars()
        .all()
    )
