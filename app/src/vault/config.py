"""Configuration settings for the Vault service."""

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(env_prefix="VAULT_", extra="ignore")

    # Database configuration
    database_url: str = "sqlite+aiosqlite:///data/vault.db"

    # MinIO configuration
    minio_endpoint: str = "minio:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "vault"
    minio_secure: bool = False

    # Collections root directory
    collections_root: Path = Path("/data/collections")

    # API configuration
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    @property
    def sync_database_url(self) -> str:
        """Return synchronous database URL for Alembic migrations."""
        if self.database_url.startswith("sqlite+aiosqlite"):
            return self.database_url.replace("sqlite+aiosqlite", "sqlite")
        if self.database_url.startswith("postgresql+asyncpg"):
            return self.database_url.replace("postgresql+asyncpg", "postgresql")
        return self.database_url


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
