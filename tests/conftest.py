from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Ensure project root is on sys.path when running pytest from anywhere.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Hard-disable auto seed and force isolated SQLite for tests *before* app import.
os.environ["AUTO_SEED"] = "false"
os.environ["DATABASE_URL"] = "sqlite:///./data/test.db"

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import delete  # noqa: E402

from app import models  # noqa: E402
from app.config import get_settings  # noqa: E402
from app.database import (  # noqa: E402
    Base,
    get_engine,
    get_sessionmaker,
    init_db,
    reset_engine,
)


def _fresh_engine() -> None:
    """Build a fresh in-memory sqlite engine for each test."""
    reset_engine()
    get_settings.cache_clear()  # type: ignore[attr-defined]
    # Use a unique on-disk DB per test session so connections share state.
    db_path = ROOT / "data" / f"test-{uuid.uuid4().hex}.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    os.environ["DATABASE_URL"] = f"sqlite:///{db_path.as_posix()}"
    get_engine()
    init_db()
    return db_path


@pytest.fixture
def db_path():
    path = _fresh_engine()
    yield path
    reset_engine()
    if path.exists():
        path.unlink(missing_ok=True)


@pytest.fixture
def client(db_path):
    from app.main import create_app

    app = create_app()
    with TestClient(app) as c:
        yield c


@pytest.fixture
def session(db_path):
    SessionLocal = get_sessionmaker()
    with SessionLocal() as s:
        yield s


def _event(
    *,
    event_id: str | None = None,
    event_type: str = "payment_initiated",
    transaction_id: str | None = None,
    merchant_id: str = "merchant_1",
    merchant_name: str = "QuickMart",
    amount: float | None = 100.00,
    currency: str | None = "INR",
    timestamp: datetime | None = None,
) -> dict:
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "event_type": event_type,
        "transaction_id": transaction_id or str(uuid.uuid4()),
        "merchant_id": merchant_id,
        "merchant_name": merchant_name,
        "amount": amount,
        "currency": currency,
        "timestamp": (timestamp or datetime.now(timezone.utc)).isoformat(),
    }


@pytest.fixture
def make_event():
    return _event
