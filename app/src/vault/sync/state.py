"""
Synchronization state management for vault collections.

This module provides utilities to:
1. Check if a collection is fully synchronized (folder matches MinIO/DB)
2. Wait for synchronization to complete before serving requests
3. Track files currently being processed by the watcher
"""

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Set

from sqlalchemy.ext.asyncio import AsyncSession

from vault.config import get_settings
from vault.db.repository import ObjectRepository
from vault.db.session import get_session
from vault.watcher.handler import _files_in_progress, _processing_lock, should_ignore

logger = logging.getLogger(__name__)

# Default timeout for waiting on sync (seconds)
DEFAULT_SYNC_TIMEOUT = 30.0

# Polling interval when waiting for sync (seconds)
SYNC_POLL_INTERVAL = 0.5


@dataclass
class SyncState:
    """Current synchronization state for a collection."""

    collection: str
    is_synced: bool
    files_in_folder: int
    files_in_db: int
    files_processing: int
    pending_files: Set[str]  # Files in folder but not in DB


def get_folder_files(collection: str) -> Set[str]:
    """
    Get all files in a collection folder.

    Returns a set of filenames (not full paths).
    """
    settings = get_settings()
    collection_path = settings.collections_root / collection

    if not collection_path.exists() or not collection_path.is_dir():
        return set()

    files = set()
    for item in collection_path.iterdir():
        if item.is_file() and not should_ignore(item):
            files.add(item.name)

    return files


def get_processing_files_for_collection(collection: str) -> Set[str]:
    """
    Get files currently being processed for a collection.

    Note: This accesses the global _files_in_progress from the watcher handler.
    """
    settings = get_settings()
    collection_path = settings.collections_root / collection
    collection_prefix = str(collection_path) + "/"

    processing = set()
    # We don't need the lock for reading since we're just checking
    for path_str in _files_in_progress:
        if path_str.startswith(collection_prefix):
            filename = Path(path_str).name
            processing.add(filename)

    return processing


async def get_sync_state(
    collection: str,
    session: Optional[AsyncSession] = None,
) -> SyncState:
    """
    Get the current synchronization state for a collection.

    Compares files in the folder with files in the database to determine
    if there are any pending uploads.

    Args:
        collection: Collection name
        session: Optional existing database session to reuse (avoids creating new connections)
    """
    # Get files in folder
    folder_files = get_folder_files(collection)

    # Get files being processed
    processing_files = get_processing_files_for_collection(collection)

    # Get files in database - reuse session if provided
    if session is not None:
        repo = ObjectRepository(session)
        db_objects = await repo.list_ready_by_collection(collection)
        db_files = {obj.name for obj in db_objects}
    else:
        async with get_session() as new_session:
            repo = ObjectRepository(new_session)
            db_objects = await repo.list_ready_by_collection(collection)
            db_files = {obj.name for obj in db_objects}

    # Files in folder but not in DB (and not currently processing)
    pending_files = folder_files - db_files - processing_files

    # Collection is synced if:
    # 1. No files are being processed
    # 2. No files are pending (in folder but not in DB)
    is_synced = len(processing_files) == 0 and len(pending_files) == 0

    return SyncState(
        collection=collection,
        is_synced=is_synced,
        files_in_folder=len(folder_files),
        files_in_db=len(db_files),
        files_processing=len(processing_files),
        pending_files=pending_files,
    )


async def is_collection_synced(collection: str) -> bool:
    """Check if a collection is fully synchronized."""
    state = await get_sync_state(collection)
    return state.is_synced


async def wait_for_sync(
    collection: str,
    timeout: float = DEFAULT_SYNC_TIMEOUT,
    poll_interval: float = SYNC_POLL_INTERVAL,
    session: Optional[AsyncSession] = None,
) -> SyncState:
    """
    Wait for a collection to be fully synchronized.

    Args:
        collection: Collection name
        timeout: Maximum time to wait in seconds
        poll_interval: How often to check sync status
        session: Optional existing database session to reuse (avoids creating 60+ connections during polling)

    Returns:
        Final SyncState after waiting

    The function will return early if sync completes before timeout.
    """
    elapsed = 0.0

    while elapsed < timeout:
        state = await get_sync_state(collection, session=session)

        if state.is_synced:
            logger.debug(
                f"Collection '{collection}' is synced: "
                f"{state.files_in_db} files in DB"
            )
            return state

        logger.debug(
            f"Waiting for sync on '{collection}': "
            f"{state.files_processing} processing, "
            f"{len(state.pending_files)} pending"
        )

        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

    # Timeout reached - return final state
    final_state = await get_sync_state(collection, session=session)
    if not final_state.is_synced:
        logger.warning(
            f"Sync timeout for '{collection}' after {timeout}s: "
            f"{final_state.files_processing} still processing, "
            f"{len(final_state.pending_files)} pending"
        )

    return final_state
