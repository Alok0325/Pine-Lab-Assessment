from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.config import Settings, get_settings


class Base(DeclarativeBase):
    pass


def _normalize_db_url(url: str) -> str:
    """Pin the Postgres driver to psycopg (v3).

    Managed/self-hosted Postgres typically hands back a bare ``postgres://`` or
    ``postgresql://`` URL. SQLAlchemy maps both to the ``psycopg2`` DBAPI, which
    this project does not install (we ship psycopg v3). Rewrite the scheme so the
    same ``DATABASE_URL`` boots everywhere without a code change.
    """
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://") :]
    if url.startswith("postgresql://"):
        url = "postgresql+psycopg://" + url[len("postgresql://") :]
    return url


def _make_engine(settings: Settings) -> Engine:
    if settings.is_sqlite:
        # Ensure the parent directory exists for file-based SQLite.
        url = settings.database_url
        if url.startswith("sqlite:///") and not url.startswith("sqlite:////"):
            rel = url.replace("sqlite:///", "", 1)
            if rel and rel != ":memory:":
                Path(rel).parent.mkdir(parents=True, exist_ok=True)
        engine = create_engine(
            settings.database_url,
            connect_args={"check_same_thread": False},
            future=True,
        )

        @event.listens_for(engine, "connect")
        def _enable_sqlite_pragmas(dbapi_conn, _record):
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA foreign_keys=ON")
            cur.execute("PRAGMA journal_mode=WAL")
            cur.execute("PRAGMA synchronous=NORMAL")
            cur.close()

        return engine

    return create_engine(
        _normalize_db_url(settings.database_url),
        pool_pre_ping=True,
        pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
        max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "20")),
        future=True,
    )


_engine: Engine | None = None
_SessionLocal: sessionmaker[Session] | None = None


def get_engine() -> Engine:
    global _engine, _SessionLocal
    if _engine is None:
        _engine = _make_engine(get_settings())
        _SessionLocal = sessionmaker(bind=_engine, autoflush=False, expire_on_commit=False)
    return _engine


def get_sessionmaker() -> sessionmaker[Session]:
    if _SessionLocal is None:
        get_engine()
    assert _SessionLocal is not None
    return _SessionLocal


def get_db() -> Iterator[Session]:
    SessionLocal = get_sessionmaker()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def session_scope() -> Iterator[Session]:
    SessionLocal = get_sessionmaker()
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def init_db() -> None:
    """Create tables if they don't exist."""
    from app import models  # noqa: F401 — ensure models are registered

    Base.metadata.create_all(bind=get_engine())


def reset_engine() -> None:
    """Used by tests to swap engines between runs."""
    global _engine, _SessionLocal
    if _engine is not None:
        _engine.dispose()
    _engine = None
    _SessionLocal = None
