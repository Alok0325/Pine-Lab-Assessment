from __future__ import annotations

import datetime as _dt
from decimal import Decimal
from enum import Enum
from typing import Generic, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, field_validator

datetime = _dt.datetime
DateType = _dt.date


class EventType(str, Enum):
    payment_initiated = "payment_initiated"
    payment_processed = "payment_processed"
    payment_failed = "payment_failed"
    settled = "settled"


class TransactionStatus(str, Enum):
    unknown = "unknown"
    initiated = "initiated"
    processed = "processed"
    failed = "failed"
    settled = "settled"
    inconsistent = "inconsistent"


# -------- Event ingestion -------- #


class EventIn(BaseModel):
    model_config = ConfigDict(extra="ignore")

    event_id: str = Field(..., min_length=1, max_length=64, description="Idempotency key")
    event_type: EventType
    transaction_id: str = Field(..., min_length=1, max_length=64)
    merchant_id: str = Field(..., min_length=1, max_length=64)
    merchant_name: str | None = Field(default=None, max_length=255)
    amount: Decimal | None = Field(default=None, ge=0)
    currency: str | None = Field(default=None, min_length=3, max_length=8)
    timestamp: datetime


class EventIngestResult(BaseModel):
    event_id: str
    status: Literal["accepted", "duplicate"]


class EventBatchResult(BaseModel):
    received: int
    accepted: int
    duplicates: int
    transactions_touched: int
    results: list[EventIngestResult] | None = None


class EventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    event_id: str
    event_type: EventType
    transaction_id: str
    merchant_id: str
    amount: Decimal | None
    currency: str | None
    timestamp: datetime
    received_at: datetime


# -------- Merchant / Transaction -------- #


class MerchantOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    merchant_id: str
    name: str
    created_at: datetime


class TransactionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    transaction_id: str
    merchant_id: str
    amount: Decimal | None
    currency: str | None
    status: TransactionStatus
    payment_status: str
    settlement_status: str
    initiated_at: datetime | None
    processed_at: datetime | None
    failed_at: datetime | None
    settled_at: datetime | None
    first_event_at: datetime | None
    last_event_at: datetime | None
    has_discrepancy: bool
    discrepancy_reasons: list[str] = Field(default_factory=list)
    event_count: int
    created_at: datetime
    updated_at: datetime

    @field_validator("discrepancy_reasons", mode="before")
    @classmethod
    def split_reasons(cls, v):
        if v is None or v == "":
            return []
        if isinstance(v, list):
            return v
        return [r.strip() for r in str(v).split("|") if r.strip()]


class TransactionDetail(TransactionOut):
    merchant: MerchantOut | None = None
    events: list[EventOut] = Field(default_factory=list)


T = TypeVar("T")


class Page(BaseModel, Generic[T]):
    items: list[T]
    total: int
    limit: int
    offset: int


# -------- Reconciliation -------- #


class SummaryRow(BaseModel):
    merchant_id: str | None = None
    merchant_name: str | None = None
    date: DateType | None = None
    status: str | None = None
    transaction_count: int
    total_amount: Decimal
    discrepancy_count: int


class ReconciliationSummary(BaseModel):
    group_by: list[str]
    filters: dict
    rows: list[SummaryRow]
    totals: dict


class DiscrepancyRow(BaseModel):
    transaction_id: str
    merchant_id: str
    merchant_name: str | None = None
    amount: Decimal | None
    currency: str | None
    status: TransactionStatus
    payment_status: str
    settlement_status: str
    reasons: list[str]
    first_event_at: datetime | None
    last_event_at: datetime | None
    event_count: int


class DiscrepancyResponse(BaseModel):
    filters: dict
    total: int
    limit: int
    offset: int
    items: list[DiscrepancyRow]


# -------- Errors -------- #


class ErrorDetail(BaseModel):
    code: str
    message: str
    field: str | None = None


class ErrorResponse(BaseModel):
    error: ErrorDetail
