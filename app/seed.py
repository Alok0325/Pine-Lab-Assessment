"""Seed the database from a JSON file of events.

Used at startup (``AUTO_SEED=true``) and from the ``scripts/load_sample_data.py``
CLI. Idempotent — re-running on an already-seeded database is a no-op.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterable

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app import crud, schemas
from app.config import get_settings
from app.database import session_scope
from app.models import Event

log = logging.getLogger(__name__)


def _chunked(iterable: list, size: int) -> Iterable[list]:
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def seed_from_file(
    path: str | Path,
    *,
    batch_size: int = 1000,
    force: bool = False,
) -> dict:
    settings = get_settings()
    p = Path(path)
    if not p.is_absolute():
        p = settings.project_root / p

    if not p.exists():
        log.warning("Seed file %s not found — skipping seeding", p)
        return {"seeded": False, "reason": "missing_file"}

    with session_scope() as db:
        already = db.execute(select(func.count()).select_from(Event)).scalar_one()
        if already and not force:
            log.info("Events table already has %d rows — skipping seed", already)
            return {"seeded": False, "reason": "already_populated", "events": already}

    raw = json.loads(p.read_text())
    if not isinstance(raw, list):
        raise ValueError("Seed file must contain a JSON array of events")

    events = [schemas.EventIn.model_validate(r) for r in raw]
    log.info("Seeding %d events from %s", len(events), p)

    total_accepted = 0
    total_duplicates = 0
    for chunk in _chunked(events, batch_size):
        with session_scope() as db:
            res = crud.ingest_events(db, chunk)
        total_accepted += res.accepted
        total_duplicates += res.duplicates

    log.info("Seed complete — accepted=%d duplicates=%d", total_accepted, total_duplicates)
    return {
        "seeded": True,
        "accepted": total_accepted,
        "duplicates": total_duplicates,
        "source": str(p),
    }
