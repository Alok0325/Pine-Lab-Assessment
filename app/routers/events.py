from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from app import crud, schemas
from app.database import get_db

router = APIRouter(prefix="/events", tags=["events"])


@router.post(
    "",
    response_model=schemas.EventBatchResult,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest one or many payment lifecycle events",
    response_description="Counts of accepted vs duplicate events",
)
def ingest(
    payload: Annotated[
        schemas.EventIn | list[schemas.EventIn],
        Body(
            ...,
            description=(
                "Either a single event object or a list of events. "
                "Events are identified by their `event_id` (idempotency key). "
                "Re-submitting an event with the same `event_id` is a safe no-op."
            ),
        ),
    ],
    verbose: Annotated[
        bool,
        Query(description="If true, include per-event accepted/duplicate status."),
    ] = False,
    db: Session = Depends(get_db),
) -> schemas.EventBatchResult:
    events = payload if isinstance(payload, list) else [payload]
    if not events:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"code": "empty_payload", "message": "No events provided"},
        )
    if len(events) > 5000:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail={
                "code": "batch_too_large",
                "message": "Up to 5000 events per request",
            },
        )

    result = crud.ingest_events(db, events, return_per_event=verbose)
    return result
