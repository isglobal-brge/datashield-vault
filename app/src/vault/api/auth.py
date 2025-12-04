"""Authentication dependencies for the Vault API."""

from typing import Annotated

from fastapi import Depends, Header, HTTPException, Path, status
from sqlalchemy.ext.asyncio import AsyncSession

from vault.db.repository import CollectionRepository
from vault.db.session import get_db_session


async def verify_collection_key(
    collection: Annotated[str, Path(description="Collection name")],
    x_collection_key: Annotated[str, Header()],
    session: AsyncSession = Depends(get_db_session),
) -> str:
    """
    Verify access to a collection via collection key.

    Returns the collection name if authorized.
    Raises HTTPException if not authorized.
    """
    repo = CollectionRepository(session)
    if await repo.verify_api_key(collection, x_collection_key):
        return collection

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing X-Collection-Key header.",
    )


# Type alias for dependency injection
CollectionKeyDep = Annotated[str, Depends(verify_collection_key)]
