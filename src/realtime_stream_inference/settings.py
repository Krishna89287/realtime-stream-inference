from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RSI_", env_file=".env", extra="ignore")

    alpha: float = 0.05
    threshold: float = 3.0
    warmup: int = 20
    queue_size: int = 1000
    workers: int = 4


settings = Settings()
