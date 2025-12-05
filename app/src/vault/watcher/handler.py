"""Filesystem event handler for the collection watcher."""

import asyncio
import logging
import time
from pathlib import Path
from typing import Dict

from watchdog.events import FileSystemEvent, FileSystemEventHandler

from vault.config import get_settings
from vault.db.repository import CollectionRepository, ObjectRepository
from vault.db.session import get_session
from vault.minio_client.client import (
    compute_file_hash,
    delete_object,
    upload_file,
)

logger = logging.getLogger(__name__)

# Files to ignore
IGNORED_FILES = {".vault_key", ".DS_Store"}

# Debouncing: track files being processed and their last event time
_processing_lock = asyncio.Lock()
_files_in_progress: Dict[str, float] = {}  # path -> timestamp when processing started
_last_event_time: Dict[str, float] = {}  # path -> last event timestamp
DEBOUNCE_SECONDS = 2.0  # Ignore events for same file within this window
PROCESSING_TIMEOUT = 300.0  # Consider stuck after 5 minutes


def should_ignore(path: Path) -> bool:
    """Check if a file should be ignored by the watcher."""
    name = path.name
    # Ignore hidden files and specific files
    if name.startswith(".") or name in IGNORED_FILES:
        return True
    return False


def parse_collection_path(file_path: Path) -> tuple[str, str] | None:
    """
    Parse a file path to extract collection name and object name.

    Expected structure: /data/collections/<collection>/<filename>

    Returns:
        Tuple of (collection_name, object_name) or None if invalid path
    """
    settings = get_settings()
    collections_root = settings.collections_root

    try:
        rel_path = file_path.relative_to(collections_root)
        parts = rel_path.parts
        if len(parts) != 2:
            # Only handle files directly in collection folders (not nested)
            return None
        collection_name, object_name = parts
        return collection_name, object_name
    except ValueError:
        return None


def parse_collection_dir(dir_path: Path) -> str | None:
    """
    Parse a directory path to extract collection name.

    Expected structure: /data/collections/<collection>

    Returns:
        Collection name or None if not a direct child of collections root
    """
    settings = get_settings()
    collections_root = settings.collections_root

    try:
        rel_path = dir_path.relative_to(collections_root)
        parts = rel_path.parts
        if len(parts) != 1:
            # Only handle direct children of collections root
            return None
        return parts[0]
    except ValueError:
        return None


async def handle_directory_created(dir_path: Path) -> None:
    """Handle a directory creation event (new collection)."""
    collection_name = parse_collection_dir(dir_path)
    if collection_name is None:
        return

    logger.info(f"New collection directory detected: {collection_name}")

    try:
        await ensure_collection_exists(collection_name)
    except Exception as e:
        logger.error(f"Failed to create collection {collection_name}: {e}", exc_info=True)


async def ensure_collection_exists(collection_name: str) -> None:
    """
    Ensure a collection exists in the database.
    If .vault_key exists, use that key. Otherwise generate a new one.
    """
    settings = get_settings()
    key_file = settings.collections_root / collection_name / ".vault_key"

    # Check if .vault_key already exists
    existing_key = None
    if key_file.exists():
        existing_key = key_file.read_text().strip()

    async with get_session() as session:
        repo = CollectionRepository(session)
        _, api_key = await repo.get_or_create(collection_name, api_key=existing_key)

        # If api_key is not None, this is a new collection - write the key file
        if api_key is not None and existing_key is None:
            key_file.write_text(api_key)
            logger.info(f"Created collection '{collection_name}' with API key in {key_file}")
        elif api_key is not None and existing_key is not None:
            logger.info(f"Created collection '{collection_name}' using existing .vault_key")


async def handle_vault_key_modified(collection_name: str) -> None:
    """Handle .vault_key file modification - update the API key in database."""
    settings = get_settings()
    key_file = settings.collections_root / collection_name / ".vault_key"

    if not key_file.exists():
        logger.warning(f".vault_key deleted for collection {collection_name}")
        return

    new_key = key_file.read_text().strip()
    if not new_key:
        logger.warning(f".vault_key is empty for collection {collection_name}")
        return

    async with get_session() as session:
        repo = CollectionRepository(session)
        if await repo.update_api_key(collection_name, new_key):
            logger.info(f"Updated API key for collection '{collection_name}'")


async def handle_file_created_or_modified(file_path: Path) -> None:
    """Handle a file create or modify event."""
    if should_ignore(file_path):
        return

    if not file_path.is_file():
        return

    parsed = parse_collection_path(file_path)
    if parsed is None:
        logger.debug(f"Ignoring file outside collection structure: {file_path}")
        return

    collection_name, object_name = parsed
    object_key = f"{collection_name}/{object_name}"
    path_str = str(file_path)
    now = time.time()

    # Debouncing: check if we should skip this event
    async with _processing_lock:
        # Check if file is currently being processed
        if path_str in _files_in_progress:
            started_at = _files_in_progress[path_str]
            if now - started_at < PROCESSING_TIMEOUT:
                logger.debug(f"Skipping {object_key}: already being processed")
                return
            else:
                # Processing stuck, allow retry
                logger.warning(f"Processing timeout for {object_key}, allowing retry")
                del _files_in_progress[path_str]

        # Check debounce window
        if path_str in _last_event_time:
            last_time = _last_event_time[path_str]
            if now - last_time < DEBOUNCE_SECONDS:
                logger.debug(f"Debouncing {object_key}: event too soon after last")
                return

        # Mark as in progress
        _files_in_progress[path_str] = now
        _last_event_time[path_str] = now

    logger.info(f"Processing file: {file_path} -> {object_key}")

    try:
        # Ensure collection exists
        await ensure_collection_exists(collection_name)

        # Compute hash
        hash_sha256, size_bytes = compute_file_hash(file_path)
        logger.debug(f"Computed hash for {object_key}: {hash_sha256}")

        # Upload to MinIO
        upload_file(object_key, file_path)
        logger.debug(f"Uploaded {object_key} to MinIO")

        # Update database
        async with get_session() as session:
            repo = ObjectRepository(session)
            await repo.create_or_replace(
                collection=collection_name,
                name=object_name,
                object_key=object_key,
                hash_sha256=hash_sha256,
                size_bytes=size_bytes,
            )
            logger.info(f"Registered object: {object_key} (hash: {hash_sha256[:16]}...)")

    except Exception as e:
        logger.error(f"Failed to process file {file_path}: {e}", exc_info=True)
    finally:
        # Remove from in-progress
        async with _processing_lock:
            _files_in_progress.pop(path_str, None)


async def handle_file_deleted(file_path: Path) -> None:
    """Handle a file delete event."""
    if should_ignore(file_path):
        return

    parsed = parse_collection_path(file_path)
    if parsed is None:
        return

    collection_name, object_name = parsed
    object_key = f"{collection_name}/{object_name}"
    path_str = str(file_path)
    now = time.time()

    # Check if file actually exists - PollingObserver can report false deletions
    if file_path.exists():
        logger.debug(f"Ignoring false deletion for {object_key}: file still exists")
        return

    # Debouncing for deletions
    async with _processing_lock:
        # If file is being processed for creation, skip deletion
        if path_str in _files_in_progress:
            logger.debug(f"Skipping deletion {object_key}: file is being processed")
            return

        # Check debounce window
        if path_str in _last_event_time:
            last_time = _last_event_time[path_str]
            if now - last_time < DEBOUNCE_SECONDS:
                logger.debug(f"Debouncing deletion {object_key}: event too soon")
                return

        _last_event_time[path_str] = now

    logger.info(f"Processing deletion: {object_key}")

    try:
        # Delete from MinIO
        delete_object(object_key)
        logger.debug(f"Deleted {object_key} from MinIO")

        # Mark as deleted in database
        async with get_session() as session:
            repo = ObjectRepository(session)
            await repo.mark_deleted(collection_name, object_name)
            logger.info(f"Marked object as deleted: {object_key}")

    except Exception as e:
        logger.error(f"Failed to process deletion {file_path}: {e}", exc_info=True)


class VaultEventHandler(FileSystemEventHandler):
    """Watchdog event handler for the vault filesystem watcher."""

    def __init__(self, loop: asyncio.AbstractEventLoop):
        super().__init__()
        self.loop = loop

    def _run_async(self, coro):
        """Schedule a coroutine to run in the event loop."""
        asyncio.run_coroutine_threadsafe(coro, self.loop)

    def on_created(self, event: FileSystemEvent) -> None:
        """Handle file/directory creation events."""
        if event.is_directory:
            self._run_async(handle_directory_created(Path(event.src_path)))
            return
        self._run_async(handle_file_created_or_modified(Path(event.src_path)))

    def on_modified(self, event: FileSystemEvent) -> None:
        """Handle file modification events."""
        if event.is_directory:
            return
        path = Path(event.src_path)
        # Handle .vault_key modifications
        if path.name == ".vault_key":
            collection_name = parse_collection_dir(path.parent)
            if collection_name:
                self._run_async(handle_vault_key_modified(collection_name))
            return
        self._run_async(handle_file_created_or_modified(path))

    def on_deleted(self, event: FileSystemEvent) -> None:
        """Handle file deletion events."""
        if event.is_directory:
            return
        self._run_async(handle_file_deleted(Path(event.src_path)))

    def on_moved(self, event: FileSystemEvent) -> None:
        """Handle file move events (treat as delete + create)."""
        if event.is_directory:
            return
        # Handle the old path as deleted
        self._run_async(handle_file_deleted(Path(event.src_path)))
        # Handle the new path as created
        self._run_async(handle_file_created_or_modified(Path(event.dest_path)))
