"""Filesystem watcher for monitoring collection directories."""

import asyncio
import logging
import time
from pathlib import Path

from watchdog.observers.polling import PollingObserver

from vault.config import get_settings
from vault.monitoring.metrics import metrics
from vault.watcher.handler import (
    VaultEventHandler,
    ensure_collection_exists,
    handle_file_created_or_modified,
)

logger = logging.getLogger(__name__)


class CollectionWatcher:
    """
    Watches the collections directory for file changes.

    Automatically discovers new collections and objects,
    uploads to MinIO, and updates the database.
    """

    def __init__(self):
        self.observer: PollingObserver | None = None
        self._running = False
        self._last_event_time: float | None = None
        self._event_loop: asyncio.AbstractEventLoop | None = None
        self._restart_count = 0

    async def scan_existing_files(self) -> None:
        """
        Scan existing files in the collections directory.

        This is called on startup to sync any files that were
        added while the watcher was not running.
        """
        settings = get_settings()
        collections_root = settings.collections_root

        if not collections_root.exists():
            logger.warning(f"Collections root does not exist: {collections_root}")
            collections_root.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created collections root: {collections_root}")
            return

        logger.info(f"Scanning existing files in {collections_root}")

        for collection_dir in collections_root.iterdir():
            if not collection_dir.is_dir():
                continue

            collection_name = collection_dir.name
            logger.info(f"Scanning collection: {collection_name}")

            # Ensure collection exists (creates .vault_key if new)
            await ensure_collection_exists(collection_name)

            for file_path in collection_dir.iterdir():
                if file_path.is_file():
                    await handle_file_created_or_modified(file_path)

        logger.info("Initial scan complete")

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        """
        Start the filesystem watcher.

        Args:
            loop: The asyncio event loop to use for async operations
        """
        if self._running:
            logger.warning("Watcher is already running")
            return

        self._event_loop = loop
        settings = get_settings()
        collections_root = settings.collections_root

        # Ensure the directory exists
        collections_root.mkdir(parents=True, exist_ok=True)

        # Create the event handler with event tracking
        handler = VaultEventHandler(loop)

        # Create and configure the observer (polling for Docker volume compatibility)
        self.observer = PollingObserver(timeout=5)
        self.observer.schedule(handler, str(collections_root), recursive=True)

        # Start watching
        self.observer.start()
        self._running = True
        self._last_event_time = time.time()

        logger.info(f"Started watching: {collections_root}")

    def stop(self) -> None:
        """Stop the filesystem watcher."""
        if self.observer is not None:
            self.observer.stop()
            self.observer.join(timeout=5)
            self.observer = None
            self._running = False
            logger.info("Watcher stopped")

    def restart(self) -> None:
        """Restart the watcher (e.g., after thread death)."""
        if self._event_loop is None:
            logger.error("Cannot restart watcher: no event loop stored")
            return

        logger.warning("Restarting watcher...")
        self.stop()
        self.start(self._event_loop)
        self._restart_count += 1
        metrics.watcher_thread_restarts.inc()
        logger.info(f"Watcher restarted (restart count: {self._restart_count})")

    def record_event(self) -> None:
        """Record that an event was processed (for health monitoring)."""
        self._last_event_time = time.time()

    @property
    def is_running(self) -> bool:
        """Check if the watcher is running."""
        return self._running

    @property
    def observer_alive(self) -> bool:
        """Check if the observer thread is alive."""
        if self.observer is None:
            return False
        # PollingObserver inherits from threading.Thread, check is_alive() directly
        if hasattr(self.observer, "is_alive"):
            return self.observer.is_alive()
        # Fallback for other observer types
        if hasattr(self.observer, "_thread") and self.observer._thread is not None:
            return self.observer._thread.is_alive()
        return False

    @property
    def last_event_age(self) -> float | None:
        """Get seconds since last event, or None if no events yet."""
        if self._last_event_time is None:
            return None
        return time.time() - self._last_event_time

    def get_health_status(self) -> dict:
        """Get watcher health status for monitoring."""
        return {
            "running": self._running,
            "observer_alive": self.observer_alive,
            "last_event_age_s": round(self.last_event_age, 1) if self.last_event_age else None,
            "restart_count": self._restart_count,
        }


# Global watcher instance
_watcher: CollectionWatcher | None = None


def get_watcher() -> CollectionWatcher:
    """Get or create the global watcher instance."""
    global _watcher
    if _watcher is None:
        _watcher = CollectionWatcher()
    return _watcher
