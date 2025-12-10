"""FastAPI application factory."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from vault.api.routes import router
from vault.db.session import close_db, get_engine, init_db, reset_engine
from vault.minio_client.client import ensure_bucket_exists
from vault.monitoring import health_router
from vault.monitoring.background import get_task_manager
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

    # Start background monitoring tasks
    task_manager = get_task_manager()
    await task_manager.start()

    logger.info("Vault service ready")

    yield

    # Shutdown
    logger.info("Shutting down Vault service...")

    # Stop background tasks
    await task_manager.stop()

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
    app.include_router(health_router)

    @app.get("/health", tags=["health"])
    async def health_check():
        """Health check endpoint."""
        return {"status": "healthy"}

    @app.post("/admin/reset-pool", tags=["admin"])
    async def admin_reset_pool():
        """
        Reset the database connection pool (admin endpoint).

        Use this when the pool is exhausted or connections are stale.
        This is a hot-reload friendly operation - no server restart needed.
        """
        logger.info("Admin: Resetting database connection pool")
        await reset_engine()
        logger.info("Admin: Database connection pool reset complete")
        return {"status": "ok", "message": "Connection pool reset successfully"}

    @app.get("/admin/pool-stats", tags=["admin"])
    async def admin_pool_stats():
        """
        Get database connection pool statistics (admin endpoint).

        Returns pool size, checked out connections, overflow status.
        Note: SQLite uses NullPool so stats will show N/A.
        """
        engine = get_engine()
        pool = engine.pool

        # NullPool doesn't have these attributes
        try:
            stats = {
                "pool_class": type(pool).__name__,
                "pool_size": getattr(pool, "size", lambda: "N/A")() if callable(getattr(pool, "size", None)) else "N/A",
                "checked_out": getattr(pool, "checkedout", lambda: "N/A")() if callable(getattr(pool, "checkedout", None)) else "N/A",
                "overflow": getattr(pool, "overflow", lambda: "N/A")() if callable(getattr(pool, "overflow", None)) else "N/A",
                "checked_in": getattr(pool, "checkedin", lambda: "N/A")() if callable(getattr(pool, "checkedin", None)) else "N/A",
            }
        except Exception as e:
            stats = {
                "pool_class": type(pool).__name__,
                "error": str(e),
            }

        return stats

    return app
