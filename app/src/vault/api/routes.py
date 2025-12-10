"""API routes for the Vault service."""

import logging
from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from vault.api.auth import CollectionKeyDep
from vault.api.schemas import (
    HashListResponse,
    ObjectHash,
    ObjectListResponse,
    SingleHashResponse,
)
from vault.db.repository import ObjectRepository
from vault.db.session import get_db_session
from vault.minio_client.client import get_object_stream, object_exists
from vault.sync.state import wait_for_sync

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["collections"])

# Default timeout for waiting on sync (seconds)
DEFAULT_SYNC_TIMEOUT = 30.0


@router.get(
    "/collections/{collection}/objects",
    response_model=ObjectListResponse,
    summary="List objects in a collection",
)
async def list_objects(
    collection: CollectionKeyDep,
    session: AsyncSession = Depends(get_db_session),
    sync_timeout: Annotated[
        Optional[float],
        Query(
            description="Timeout in seconds to wait for sync to complete. "
            "Set to 0 to disable sync waiting.",
            ge=0,
            le=300,
        ),
    ] = DEFAULT_SYNC_TIMEOUT,
):
    """
    List all objects in a collection.

    Waits for the collection to be fully synchronized before returning.
    This ensures the response includes all files from the collection folder.

    Requires X-Collection-Key header.
    """
    # Wait for sync if timeout > 0 - REUSE SESSION to avoid connection pool exhaustion
    if sync_timeout and sync_timeout > 0:
        sync_state = await wait_for_sync(collection, timeout=sync_timeout, session=session)
        if not sync_state.is_synced:
            logger.warning(
                f"list_objects: collection '{collection}' not fully synced after {sync_timeout}s "
                f"({sync_state.files_processing} processing, {len(sync_state.pending_files)} pending)"
            )

    repo = ObjectRepository(session)
    objects = await repo.list_ready_by_collection(collection)

    return ObjectListResponse(
        collection=collection,
        objects=[obj.name for obj in objects],
    )


@router.get(
    "/collections/{collection}/hashes",
    response_model=HashListResponse,
    summary="List hashes for all objects in a collection",
)
async def list_hashes(
    collection: CollectionKeyDep,
    session: AsyncSession = Depends(get_db_session),
    sync_timeout: Annotated[
        Optional[float],
        Query(
            description="Timeout in seconds to wait for sync to complete. "
            "Set to 0 to disable sync waiting.",
            ge=0,
            le=300,
        ),
    ] = DEFAULT_SYNC_TIMEOUT,
):
    """
    List all object hashes in a collection.

    Waits for the collection to be fully synchronized before returning.
    This ensures the response includes all files from the collection folder.

    Requires X-Collection-Key header.
    """
    # Wait for sync if timeout > 0 - REUSE SESSION to avoid connection pool exhaustion
    if sync_timeout and sync_timeout > 0:
        sync_state = await wait_for_sync(collection, timeout=sync_timeout, session=session)
        if not sync_state.is_synced:
            logger.warning(
                f"list_hashes: collection '{collection}' not fully synced after {sync_timeout}s "
                f"({sync_state.files_processing} processing, {len(sync_state.pending_files)} pending)"
            )

    repo = ObjectRepository(session)
    objects = await repo.list_ready_by_collection(collection)

    return HashListResponse(
        collection=collection,
        items=[ObjectHash(name=obj.name, hash_sha256=obj.hash_sha256) for obj in objects],
    )


@router.get(
    "/collections/{collection}/objects/{name:path}",
    summary="Download an object",
    responses={
        200: {
            "content": {"application/octet-stream": {}},
            "description": "Object file stream",
        },
        404: {"description": "Object not found"},
    },
)
async def download_object(
    collection: CollectionKeyDep,
    name: Annotated[str, Path(description="Object name")],
    session: AsyncSession = Depends(get_db_session),
):
    """
    Download an object from the collection.

    Streams the object directly from MinIO.
    Requires X-Collection-Key header.

    Note: This endpoint does NOT wait for sync since it targets a specific object.
    """
    repo = ObjectRepository(session)
    obj = await repo.get_ready_by_collection_and_name(collection, name)

    if obj is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Object '{name}' not found in collection '{collection}'",
        )

    # Verify object exists in MinIO
    if not object_exists(obj.object_key):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Object '{name}' not found in storage",
        )

    return StreamingResponse(
        get_object_stream(obj.object_key),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{name}"',
            "Content-Length": str(obj.size_bytes),
            "X-Object-Hash-SHA256": obj.hash_sha256,
        },
    )


@router.get(
    "/collections/{collection}/hashes/{name:path}",
    response_model=SingleHashResponse,
    summary="Get hash of a single object",
)
async def get_object_hash(
    collection: CollectionKeyDep,
    name: Annotated[str, Path(description="Object name")],
    session: AsyncSession = Depends(get_db_session),
):
    """
    Get the SHA-256 hash of a specific object.

    Requires X-Collection-Key header.

    Note: This endpoint does NOT wait for sync since it targets a specific object.
    """
    repo = ObjectRepository(session)
    obj = await repo.get_ready_by_collection_and_name(collection, name)

    if obj is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Object '{name}' not found in collection '{collection}'",
        )

    return SingleHashResponse(
        collection=collection,
        name=name,
        hash_sha256=obj.hash_sha256,
    )
