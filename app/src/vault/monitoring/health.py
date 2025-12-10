"""Comprehensive health check system for the vault service."""

import asyncio
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional

from sqlalchemy import text

from vault.config import get_settings
from vault.db.session import get_engine, get_session
from vault.monitoring.metrics import metrics

logger = logging.getLogger(__name__)


class ComponentStatus(str, Enum):
    """Health status of a component."""

    UP = "up"
    DOWN = "down"
    DEGRADED = "degraded"
    UNKNOWN = "unknown"


@dataclass
class ComponentHealth:
    """Health status of a single component."""

    name: str
    status: ComponentStatus
    latency_ms: float
    message: str = ""
    details: Optional[Dict[str, Any]] = None


@dataclass
class SystemHealth:
    """Overall system health status."""

    status: ComponentStatus
    timestamp: float
    components: Dict[str, ComponentHealth]
    critical_failures: list


async def check_database() -> ComponentHealth:
    """Check database connectivity and performance."""
    name = "database"
    start = time.time()

    try:
        async with get_session() as session:
            result = await asyncio.wait_for(
                session.execute(text("SELECT 1")),
                timeout=5.0,
            )
            _ = result.scalar()

        latency = (time.time() - start) * 1000

        # Get pool stats
        engine = get_engine()
        pool_status = "NullPool (SQLite)"
        try:
            pool = engine.pool
            pool_class = type(pool).__name__
            if pool_class != "NullPool":
                size = getattr(pool, "size", lambda: "N/A")
                size = size() if callable(size) else size
                checkedout = getattr(pool, "checkedout", lambda: "N/A")
                checkedout = checkedout() if callable(checkedout) else checkedout
                pool_status = f"{pool_class}: size={size}, checked_out={checkedout}"
            else:
                pool_status = pool_class
        except Exception:
            pass

        return ComponentHealth(
            name=name,
            status=ComponentStatus.UP,
            latency_ms=latency,
            message="Database operational",
            details={"pool_status": pool_status},
        )

    except asyncio.TimeoutError:
        metrics.db_connection_errors.inc()
        return ComponentHealth(
            name=name,
            status=ComponentStatus.DOWN,
            latency_ms=(time.time() - start) * 1000,
            message="Database query timeout (>5s)",
        )
    except Exception as e:
        metrics.db_connection_errors.inc()
        logger.error(f"Database health check failed: {e}")
        return ComponentHealth(
            name=name,
            status=ComponentStatus.DOWN,
            latency_ms=(time.time() - start) * 1000,
            message=str(e),
        )


async def check_minio() -> ComponentHealth:
    """Check MinIO connectivity and bucket access."""
    name = "minio"
    start = time.time()

    try:
        from vault.minio_client.client import get_minio_client

        settings = get_settings()
        client = get_minio_client()

        # Test bucket exists
        exists = await asyncio.wait_for(
            asyncio.to_thread(client.bucket_exists, settings.minio_bucket),
            timeout=5.0,
        )

        latency = (time.time() - start) * 1000

        if not exists:
            metrics.minio_connection_errors.inc()
            return ComponentHealth(
                name=name,
                status=ComponentStatus.DOWN,
                latency_ms=latency,
                message=f"Bucket '{settings.minio_bucket}' does not exist",
            )

        return ComponentHealth(
            name=name,
            status=ComponentStatus.UP,
            latency_ms=latency,
            message="MinIO operational",
            details={"bucket": settings.minio_bucket},
        )

    except asyncio.TimeoutError:
        metrics.minio_connection_errors.inc()
        return ComponentHealth(
            name=name,
            status=ComponentStatus.DOWN,
            latency_ms=(time.time() - start) * 1000,
            message="MinIO connection timeout (>5s)",
        )
    except Exception as e:
        metrics.minio_connection_errors.inc()
        logger.error(f"MinIO health check failed: {e}")
        return ComponentHealth(
            name=name,
            status=ComponentStatus.DOWN,
            latency_ms=(time.time() - start) * 1000,
            message=str(e),
        )


async def check_filesystem() -> ComponentHealth:
    """Check filesystem accessibility."""
    name = "filesystem"
    start = time.time()

    try:
        settings = get_settings()
        test_file = settings.collections_root / ".health_check"

        # Test write
        test_file.write_text("ok")

        # Test read
        content = test_file.read_text()
        if content != "ok":
            raise ValueError("Read content mismatch")

        # Cleanup
        test_file.unlink()

        latency = (time.time() - start) * 1000

        return ComponentHealth(
            name=name,
            status=ComponentStatus.UP,
            latency_ms=latency,
            message="Filesystem operational",
            details={"collections_root": str(settings.collections_root)},
        )

    except Exception as e:
        logger.error(f"Filesystem health check failed: {e}")
        return ComponentHealth(
            name=name,
            status=ComponentStatus.DOWN,
            latency_ms=(time.time() - start) * 1000,
            message=str(e),
        )


async def check_watcher() -> ComponentHealth:
    """Check file watcher thread status."""
    name = "watcher"
    start = time.time()

    try:
        from vault.watcher.watcher import get_watcher

        watcher = get_watcher()

        is_running = watcher.is_running
        observer_alive = False
        last_event_age = None

        if hasattr(watcher, "observer") and watcher.observer is not None:
            # PollingObserver inherits from threading.Thread
            if hasattr(watcher.observer, "is_alive"):
                observer_alive = watcher.observer.is_alive()
            elif hasattr(watcher.observer, "_thread") and watcher.observer._thread is not None:
                observer_alive = watcher.observer._thread.is_alive()

        # Check last event time if available
        if hasattr(watcher, "_last_event_time"):
            last_event_age = time.time() - watcher._last_event_time

        latency = (time.time() - start) * 1000

        if not is_running:
            return ComponentHealth(
                name=name,
                status=ComponentStatus.DOWN,
                latency_ms=latency,
                message="Watcher not running",
            )

        if not observer_alive:
            return ComponentHealth(
                name=name,
                status=ComponentStatus.DOWN,
                latency_ms=latency,
                message="Observer thread not alive",
            )

        # Warn if no events in 10 minutes (but not critical)
        status = ComponentStatus.UP
        message = "Watcher operational"
        if last_event_age and last_event_age > 600:  # 10 minutes
            status = ComponentStatus.DEGRADED
            message = f"No events in {int(last_event_age)}s"

        return ComponentHealth(
            name=name,
            status=status,
            latency_ms=latency,
            message=message,
            details={
                "running": is_running,
                "observer_alive": observer_alive,
                "last_event_age_s": int(last_event_age) if last_event_age else None,
            },
        )

    except Exception as e:
        logger.error(f"Watcher health check failed: {e}")
        return ComponentHealth(
            name=name,
            status=ComponentStatus.DOWN,
            latency_ms=(time.time() - start) * 1000,
            message=str(e),
        )


async def check_consistency() -> ComponentHealth:
    """Check data consistency between folder, database, and MinIO."""
    name = "consistency"
    start = time.time()

    try:
        from vault.sync.state import get_folder_files, get_processing_files_for_collection
        from vault.db.repository import CollectionRepository, ObjectRepository
        from vault.minio_client.client import object_exists

        settings = get_settings()
        pending_total = 0
        orphaned_total = 0
        missing_total = 0
        collections_checked = 0

        async with get_session() as session:
            collection_repo = CollectionRepository(session)
            object_repo = ObjectRepository(session)
            collections = await collection_repo.list_all()

            for collection in collections:
                collections_checked += 1
                collection_name = collection.name

                # Get folder files
                folder_files = get_folder_files(collection_name)
                processing_files = get_processing_files_for_collection(collection_name)

                # Get DB files
                db_objects = await object_repo.list_ready_by_collection(collection_name)
                db_files = {obj.name for obj in db_objects}

                # Pending: in folder but not in DB
                pending = folder_files - db_files - processing_files
                pending_total += len(pending)

                # Check MinIO for DB objects (sample check, not all)
                sample_size = min(5, len(db_objects))
                for obj in list(db_objects)[:sample_size]:
                    if not object_exists(obj.object_key):
                        missing_total += 1

        latency = (time.time() - start) * 1000

        # Update metrics
        metrics.consistency_pending_files.set(pending_total)
        metrics.consistency_missing_objects.set(missing_total)
        metrics.consistency_checks_total.inc()

        if pending_total > 0 or missing_total > 0:
            metrics.consistency_errors_found.inc(pending_total + missing_total)

        # Determine status
        if missing_total > 0:
            status = ComponentStatus.DOWN
            message = f"Data integrity issue: {missing_total} objects in DB but missing from MinIO"
        elif pending_total > 10:
            status = ComponentStatus.DEGRADED
            message = f"High sync backlog: {pending_total} files pending"
        elif pending_total > 0:
            status = ComponentStatus.UP
            message = f"Minor sync backlog: {pending_total} files pending"
        else:
            status = ComponentStatus.UP
            message = "Data consistent"

        return ComponentHealth(
            name=name,
            status=status,
            latency_ms=latency,
            message=message,
            details={
                "collections_checked": collections_checked,
                "pending_files": pending_total,
                "missing_from_minio": missing_total,
            },
        )

    except Exception as e:
        logger.error(f"Consistency check failed: {e}")
        return ComponentHealth(
            name=name,
            status=ComponentStatus.UNKNOWN,
            latency_ms=(time.time() - start) * 1000,
            message=str(e),
        )


async def get_system_health(include_consistency: bool = False) -> SystemHealth:
    """Get comprehensive system health.

    Args:
        include_consistency: Whether to include consistency check (slower)
    """
    checks = [
        check_database(),
        check_minio(),
        check_filesystem(),
        check_watcher(),
    ]

    if include_consistency:
        checks.append(check_consistency())

    results = await asyncio.gather(*checks, return_exceptions=True)

    components = {}
    critical_failures = []

    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Health check exception: {result}")
            components["unknown"] = ComponentHealth(
                name="unknown",
                status=ComponentStatus.DOWN,
                latency_ms=0,
                message=str(result),
            )
            critical_failures.append("unknown")
        else:
            components[result.name] = result
            if result.status == ComponentStatus.DOWN:
                # Database and filesystem are critical
                if result.name in ["database", "filesystem"]:
                    critical_failures.append(result.name)

    # Determine overall status
    if not critical_failures:
        all_up = all(c.status == ComponentStatus.UP for c in components.values())
        overall = ComponentStatus.UP if all_up else ComponentStatus.DEGRADED
    else:
        overall = ComponentStatus.DOWN

    # Update metrics
    metrics.last_health_check_time.set(time.time())
    if critical_failures:
        metrics.health_check_failures.inc()

    return SystemHealth(
        status=overall,
        timestamp=time.time(),
        components=components,
        critical_failures=critical_failures,
    )
