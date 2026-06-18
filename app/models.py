from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class Merchant(Base):
    __tablename__ = "merchants"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    merchant_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    transactions: Mapped[list["Transaction"]] = relationship(
        back_populates="merchant", cascade="save-update", lazy="select"
    )


class Transaction(Base):
    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    transaction_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    merchant_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("merchants.merchant_id", ondelete="RESTRICT"), nullable=False
    )

    amount: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(8), nullable=True)

    # Overall transaction status (derived from events).
    # One of: initiated | processed | failed | settled | inconsistent | unknown
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="unknown", index=True)

    # Payment lifecycle state (last non-settled state).
    # One of: none | initiated | processed | failed
    payment_status: Mapped[str] = mapped_column(String(32), nullable=False, default="none")

    # Settlement lifecycle state.
    # One of: none | settled
    settlement_status: Mapped[str] = mapped_column(String(32), nullable=False, default="none")

    initiated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    failed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    settled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    first_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)

    has_discrepancy: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, index=True)
    discrepancy_reasons: Mapped[str | None] = mapped_column(Text, nullable=True)

    event_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    # Both relationships are lazy="select" (load only on explicit access) so the
    # high-traffic list path stays a single indexed SELECT. The detail endpoint
    # loads events explicitly (ordered) via crud.get_transaction_events.
    merchant: Mapped[Merchant] = relationship(back_populates="transactions", lazy="select")
    events: Mapped[list["Event"]] = relationship(
        back_populates="transaction",
        primaryjoin="Transaction.transaction_id == foreign(Event.transaction_id)",
        viewonly=True,
        lazy="select",
    )

    __table_args__ = (
        Index("ix_transactions_merchant_status", "merchant_id", "status"),
        Index("ix_transactions_merchant_last_event", "merchant_id", "last_event_at"),
        Index("ix_transactions_status_last_event", "status", "last_event_at"),
        Index("ix_transactions_discrepancy_merchant", "has_discrepancy", "merchant_id"),
    )


class Event(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Idempotency key — UNIQUE constraint guarantees no duplicates land.
    event_id: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)

    event_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    transaction_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    merchant_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    amount: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    currency: Mapped[str | None] = mapped_column(String(8), nullable=True)

    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)

    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Relationship via transaction_id (string) — not enforced FK because events can arrive
    # for transactions that don't yet have a row (we backfill on the first event).
    transaction: Mapped[Transaction | None] = relationship(
        back_populates="events",
        primaryjoin="foreign(Event.transaction_id) == Transaction.transaction_id",
        viewonly=True,
        lazy="select",
    )

    __table_args__ = (
        Index("ix_events_transaction_timestamp", "transaction_id", "timestamp"),
        Index("ix_events_merchant_timestamp", "merchant_id", "timestamp"),
        Index("ix_events_type_timestamp", "event_type", "timestamp"),
    )
