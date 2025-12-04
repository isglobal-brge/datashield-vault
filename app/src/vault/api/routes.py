"""API routes for the Vault service."""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, status
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

router = APIRouter(prefix="/api/v1", tags=["collections"])


@router.get(
    "/collections/{collection}/objects",
    response_model=ObjectListResponse,
    summary="List objects in a collection",
)
async def list_objects(
    collection: CollectionKeyDep,
    session: AsyncSession = Depends(get_db_session),
):
    """
    List all objects in a collection.

    Requires X-Collection-Key header.
    """
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
):
    """
    List all object hashes in a collection.

    Requires X-Collection-Key header.
    """
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
