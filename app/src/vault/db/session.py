"""Database session management."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from vault.config import get_settings
from vault.db.models import Base

_engine = None
_session_factory = None


def _set_sqlite_pragma(dbapi_conn, connection_record):
    """Set SQLite PRAGMAs on each new connection for better concurrency."""
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout=60000")  # 60 second busy timeout
    cursor.execute("PRAGMA synchronous=NORMAL")  # Faster writes with WAL
    cursor.close()


def get_engine():
    """Get or create the async database engine."""
    global _engine
    if _engine is None:
        settings = get_settings()

        # For SQLite, use NullPool to avoid connection issues
        # For other databases, use optimized pool settings
        is_sqlite = "sqlite" in settings.database_url

        if is_sqlite:
            _engine = create_async_engine(
                settings.database_url,
                echo=False,
                pool_pre_ping=True,
                poolclass=NullPool,  # SQLite works better without pooling
                connect_args={
                    "check_same_thread": False,
                    "timeout": 60,  # 60 second timeout for lock acquisition
                },
            )
            # Register event listener to set PRAGMAs on each connection
            # For async engines, we need to use the sync engine's pool events
            event.listen(_engine.sync_engine, "connect", _set_sqlite_pragma)
        else:
            _engine = create_async_engine(
                settings.database_url,
                echo=False,
                pool_pre_ping=True,
                pool_size=20,           # Increased from default 5
                max_overflow=30,        # Increased from default 10 (total: 50)
                pool_recycle=3600,      # Recycle connections every hour
                pool_timeout=60,        # 60s timeout waiting for connection
            )
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Get or create the session factory."""
    global _session_factory
    if _session_factory is None:
        _session_factory = async_sessionmaker(
            get_engine(),
            class_=AsyncSession,
            expire_on_commit=False,
        )
    return _session_factory


async def init_db() -> None:
    """Initialize the database, creating all tables."""
    engine = get_engine()
    settings = get_settings()

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        # Enable WAL mode for SQLite for better concurrent access
        if "sqlite" in settings.database_url:
            await conn.execute(text("PRAGMA journal_mode=WAL"))
            await conn.execute(text("PRAGMA busy_timeout=60000"))  # 60 second busy timeout


async def close_db() -> None:
    """Close the database connection."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None


async def reset_engine() -> None:
    """
    Reset the database engine (hot-reload friendly).

    This disposes the current engine and creates a new one with fresh settings.
    Useful when connection pool is exhausted or after configuration changes.
    """
    global _engine, _session_factory

    old_engine = _engine
    _engine = None
    _session_factory = None

    if old_engine is not None:
        try:
            await old_engine.dispose()
        except Exception:
            pass  # Ignore errors during dispose

    # Get new engine (will be created on next request)
    get_engine()


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """Get a database session as an async context manager."""
    factory = get_session_factory()
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency for database sessions."""
    async with get_session() as session:
        yield session
