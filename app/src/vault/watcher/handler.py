"""Filesystem event handler for the collection watcher."""

import asyncio
import logging
from pathlib import Path

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
    If not, create it and write the .vault_key file.
    """
    settings = get_settings()

    async with get_session() as session:
        repo = CollectionRepository(session)
        _, api_key = await repo.get_or_create(collection_name)

        # If api_key is not None, this is a new collection
        if api_key is not None:
            key_file = settings.collections_root / collection_name / ".vault_key"
            key_file.write_text(api_key)
            logger.info(f"Created collection '{collection_name}' with API key in {key_file}")


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


async def handle_file_deleted(file_path: Path) -> None:
    """Handle a file delete event."""
    if should_ignore(file_path):
        return

    parsed = parse_collection_path(file_path)
    if parsed is None:
        return

    collection_name, object_name = parsed
    object_key = f"{collection_name}/{object_name}"

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
        self._run_async(handle_file_created_or_modified(Path(event.src_path)))

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
