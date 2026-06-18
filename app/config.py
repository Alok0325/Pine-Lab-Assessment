from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    database_url: str = "sqlite:///./data/app.db"
    app_env: str = "development"
    log_level: str = "INFO"

    host: str = "0.0.0.0"
    port: int = 8000

    auto_seed: bool = True
    seed_file: str = "sample_events.json"
    # Optional supplemental data (kept separate so the provided sample_events.json
    # stays untouched). Adds the discrepancy types absent from the provided file.
    seed_extra_file: str = "sample_events_extra.json"

    @property
    def is_sqlite(self) -> bool:
        return self.database_url.startswith("sqlite")

    @property
    def project_root(self) -> Path:
        return Path(__file__).resolve().parent.parent


@lru_cache
def get_settings() -> Settings:
    return Settings()
