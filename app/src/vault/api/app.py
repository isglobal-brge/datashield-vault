"""FastAPI application factory."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from vault.api.routes import router
from vault.db.session import close_db, init_db
from vault.minio_client.client import ensure_bucket_exists
from vault.watcher.watcher import get_watcher

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup and shutdown."""
    import asyncio

    # Startup
    logger.info("Starting Vault service...")

    # Initialize database
    await init_db()
    logger.info("Database initialized")

    # Ensure MinIO bucket exists
    ensure_bucket_exists()
    logger.info("MinIO bucket ready")

    # Start filesystem watcher
    loop = asyncio.get_running_loop()
    watcher = get_watcher()
    watcher.start(loop)

    # Scan existing files
    await watcher.scan_existing_files()

    logger.info("Vault service ready")

    yield

    # Shutdown
    logger.info("Shutting down Vault service...")

    # Stop watcher
    watcher.stop()

    # Close database
    await close_db()

    logger.info("Vault service stopped")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="DataSHIELD Vault",
        description="Containerized vault for file storage and hashing for DataSHIELD",
        version="0.1.0",
        lifespan=lifespan,
    )

    # Include routers
    app.include_router(router)

    @app.get("/health", tags=["health"])
    async def health_check():
        """Health check endpoint."""
        return {"status": "healthy"}

    return app
