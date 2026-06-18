from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.config import get_settings
from app.database import get_engine, init_db
from app.routers import events as events_router
from app.routers import reconciliation as reconciliation_router
from app.routers import transactions as transactions_router

logger = logging.getLogger("app")


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    _configure_logging(settings.log_level)
    logger.info("Starting payment-reconciliation-service env=%s", settings.app_env)
    get_engine()
    init_db()
    if settings.auto_seed:
        try:
            from app.seed import seed_from_file

            result = seed_from_file(settings.seed_file)
            logger.info("Auto-seed: %s", result)
        except Exception:  # noqa: BLE001
            logger.exception("Auto-seed failed (continuing without seed data)")
    yield
    logger.info("Shutting down")


def create_app() -> FastAPI:
    app = FastAPI(
        title="Payment Reconciliation Service",
        version="0.1.0",
        description=(
            "Ingests payment lifecycle events, maintains derived transaction "
            "state, and exposes reconciliation reports. "
            "See `/docs` for interactive OpenAPI."
        ),
        lifespan=lifespan,
        contact={"name": "Assessment submission"},
    )

    @app.get("/", tags=["meta"], summary="Service banner")
    def root():
        return {
            "name": "payment-reconciliation-service",
            "version": "0.1.0",
            "docs": "/docs",
            "openapi": "/openapi.json",
            "health": "/healthz",
        }

    @app.get("/healthz", tags=["meta"], summary="Health check")
    def healthz():
        return {"status": "ok"}

    app.include_router(events_router.router)
    app.include_router(transactions_router.router)
    app.include_router(reconciliation_router.router)

    @app.exception_handler(RequestValidationError)
    async def validation_handler(_: Request, exc: RequestValidationError):
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=jsonable_encoder(
                {
                    "error": {
                        "code": "validation_error",
                        "message": "Request validation failed",
                        "details": exc.errors(),
                    }
                }
            ),
        )

    return app


app = create_app()
