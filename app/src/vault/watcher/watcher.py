"""Filesystem watcher for monitoring collection directories."""

import asyncio
import logging
from pathlib import Path

from watchdog.observers.polling import PollingObserver

from vault.config import get_settings
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

        settings = get_settings()
        collections_root = settings.collections_root

        # Ensure the directory exists
        collections_root.mkdir(parents=True, exist_ok=True)

        # Create the event handler
        handler = VaultEventHandler(loop)

        # Create and configure the observer (polling for Docker volume compatibility)
        self.observer = PollingObserver(timeout=5)
        self.observer.schedule(handler, str(collections_root), recursive=True)

        # Start watching
        self.observer.start()
        self._running = True

        logger.info(f"Started watching: {collections_root}")

    def stop(self) -> None:
        """Stop the filesystem watcher."""
        if self.observer is not None:
            self.observer.stop()
            self.observer.join(timeout=5)
            self.observer = None
            self._running = False
            logger.info("Watcher stopped")

    @property
    def is_running(self) -> bool:
        """Check if the watcher is running."""
        return self._running


# Global watcher instance
_watcher: CollectionWatcher | None = None


def get_watcher() -> CollectionWatcher:
    """Get or create the global watcher instance."""
    global _watcher
    if _watcher is None:
        _watcher = CollectionWatcher()
    return _watcher
