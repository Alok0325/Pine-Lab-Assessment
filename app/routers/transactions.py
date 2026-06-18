from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app import crud, schemas
from app.database import get_db

router = APIRouter(prefix="/transactions", tags=["transactions"])


@router.get(
    "",
    response_model=schemas.Page[schemas.TransactionOut],
    summary="List transactions",
    response_description="Page of transactions matching the supplied filters",
)
def list_transactions(
    merchant_id: Annotated[str | None, Query(description="Filter by merchant_id")] = None,
    status_: Annotated[
        schemas.TransactionStatus | None,
        Query(alias="status", description="Filter by transaction status"),
    ] = None,
    has_discrepancy: Annotated[
        bool | None, Query(description="Only flagged/clean transactions")
    ] = None,
    start: Annotated[
        datetime | None,
        Query(description="Inclusive lower bound on last_event_at (ISO 8601)"),
    ] = None,
    end: Annotated[
        datetime | None,
        Query(description="Inclusive upper bound on last_event_at (ISO 8601)"),
    ] = None,
    sort: Annotated[
        str,
        Query(
            description=(
                "Sort field: last_event_at | first_event_at | initiated_at | "
                "settled_at | amount | status | merchant_id | transaction_id | "
                "created_at | updated_at"
            )
        ),
    ] = "last_event_at",
    order: Annotated[str, Query(pattern="^(asc|desc)$")] = "desc",
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
    db: Session = Depends(get_db),
):
    try:
        items, total = crud.list_transactions(
            db,
            merchant_id=merchant_id,
            status=status_.value if status_ else None,
            has_discrepancy=has_discrepancy,
            start=start,
            end=end,
            sort=sort,
            order=order,
            limit=limit,
            offset=offset,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "invalid_sort", "message": str(exc)},
        )
    return schemas.Page[schemas.TransactionOut](
        items=[schemas.TransactionOut.model_validate(i) for i in items],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/{transaction_id}",
    response_model=schemas.TransactionDetail,
    summary="Fetch a transaction with merchant info and event history",
)
def get_transaction(transaction_id: str, db: Session = Depends(get_db)):
    tx = crud.get_transaction(db, transaction_id)
    if not tx:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "code": "transaction_not_found",
                "message": f"Transaction {transaction_id} does not exist",
            },
        )
    merchant = crud.get_merchant(db, tx.merchant_id)
    events = crud.get_transaction_events(db, transaction_id)

    detail = schemas.TransactionDetail.model_validate(tx)
    detail.merchant = schemas.MerchantOut.model_validate(merchant) if merchant else None
    detail.events = [schemas.EventOut.model_validate(e) for e in events]
    return detail
