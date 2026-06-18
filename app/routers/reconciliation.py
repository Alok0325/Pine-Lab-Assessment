from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app import crud, schemas
from app.database import get_db

router = APIRouter(prefix="/reconciliation", tags=["reconciliation"])


@router.get(
    "/summary",
    response_model=schemas.ReconciliationSummary,
    summary="Aggregated reconciliation summary",
    description=(
        "Returns aggregated counts and amounts grouped by one or more of "
        "`merchant`, `date`, `status`. Filters supported: merchant_id, status, "
        "start, end (all on last_event_at)."
    ),
)
def summary(
    group_by: Annotated[
        list[str],
        Query(
            description=(
                "Dimension(s) to group by: merchant | date | status. "
                "Pass multiple values: ?group_by=merchant&group_by=status"
            )
        ),
    ] = ["merchant", "status"],
    merchant_id: Annotated[str | None, Query()] = None,
    status_: Annotated[str | None, Query(alias="status")] = None,
    start: Annotated[datetime | None, Query(description="Inclusive lower bound on last_event_at")] = None,
    end: Annotated[datetime | None, Query(description="Inclusive upper bound on last_event_at")] = None,
    db: Session = Depends(get_db),
):
    try:
        return crud.reconciliation_summary(
            db,
            group_by=group_by,
            merchant_id=merchant_id,
            status=status_,
            start=start,
            end=end,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "invalid_group_by", "message": str(exc)},
        )


@router.get(
    "/discrepancies",
    response_model=schemas.DiscrepancyResponse,
    summary="Transactions where payment / settlement state is inconsistent",
    description=(
        "Returns transactions flagged with one or more discrepancy reasons. "
        "Reasons currently surfaced:\n"
        "- `pending_settlement` — payment_processed >24h ago but no settled event\n"
        "- `settled_after_failure` — settled event for a payment_failed transaction\n"
        "- `settled_without_payment` — settled event without any payment_processed/failed event\n"
        "- `conflicting_payment_states` — both payment_processed and payment_failed seen\n"
        "- `amount_mismatch` — events for the same transaction carry different amounts"
    ),
)
def discrepancies(
    merchant_id: Annotated[str | None, Query()] = None,
    reason: Annotated[
        str | None,
        Query(
            description=(
                "Filter to a single discrepancy reason "
                "(pending_settlement | settled_after_failure | settled_without_payment | "
                "conflicting_payment_states | amount_mismatch)"
            )
        ),
    ] = None,
    start: Annotated[datetime | None, Query()] = None,
    end: Annotated[datetime | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    db: Session = Depends(get_db),
):
    return crud.list_discrepancies(
        db,
        merchant_id=merchant_id,
        reason=reason,
        start=start,
        end=end,
        limit=limit,
        offset=offset,
    )
