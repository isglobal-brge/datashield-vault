"""Database repository for collections and objects."""

import hashlib
import secrets
from datetime import datetime, timezone

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from vault.db.models import Collection, Object, ObjectStatus


def hash_api_key(api_key: str) -> str:
    """Hash an API key using SHA-256."""
    return hashlib.sha256(api_key.encode()).hexdigest()


def generate_api_key() -> str:
    """Generate a random API key."""
    return secrets.token_hex(32)


class CollectionRepository:
    """Repository for collection operations."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_name(self, name: str) -> Collection | None:
        """Get a collection by name."""
        result = await self.session.execute(
            select(Collection).where(Collection.name == name)
        )
        return result.scalar_one_or_none()

    async def get_active_by_name(self, name: str) -> Collection | None:
        """Get an active collection by name."""
        result = await self.session.execute(
            select(Collection).where(Collection.name == name, Collection.is_active == True)
        )
        return result.scalar_one_or_none()

    async def list_all(self) -> list[Collection]:
        """List all collections."""
        result = await self.session.execute(
            select(Collection).order_by(Collection.name)
        )
        return list(result.scalars().all())

    async def list_active(self) -> list[Collection]:
        """List all active collections."""
        result = await self.session.execute(
            select(Collection).where(Collection.is_active == True).order_by(Collection.name)
        )
        return list(result.scalars().all())

    async def create(self, name: str, api_key: str) -> Collection:
        """Create a new collection with the given API key."""
        collection = Collection(
            name=name,
            api_key_hash=hash_api_key(api_key),
            is_active=True,
        )
        self.session.add(collection)
        await self.session.flush()
        return collection

    async def get_or_create(self, name: str) -> tuple[Collection, str | None]:
        """
        Get existing collection or create a new one.

        Returns:
            Tuple of (collection, api_key). api_key is None if collection already existed.
        """
        existing = await self.get_by_name(name)
        if existing is not None:
            return existing, None

        # Generate new API key and create
        api_key = generate_api_key()
        collection = Collection(
            name=name,
            api_key_hash=hash_api_key(api_key),
            is_active=True,
        )
        self.session.add(collection)
        await self.session.flush()
        return collection, api_key

    async def verify_api_key(self, name: str, api_key: str) -> bool:
        """Verify an API key for a collection."""
        collection = await self.get_active_by_name(name)
        if collection is None:
            return False
        return collection.api_key_hash == hash_api_key(api_key)

    async def rotate_api_key(self, name: str) -> str | None:
        """Rotate the API key for a collection. Returns new key or None if not found."""
        collection = await self.get_by_name(name)
        if collection is None:
            return None
        new_key = generate_api_key()
        collection.api_key_hash = hash_api_key(new_key)
        await self.session.flush()
        return new_key

    async def deactivate(self, name: str) -> bool:
        """Deactivate a collection."""
        result = await self.session.execute(
            update(Collection).where(Collection.name == name).values(is_active=False)
        )
        return result.rowcount > 0


class ObjectRepository:
    """Repository for object operations."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_key(self, object_key: str) -> Object | None:
        """Get an object by its MinIO key."""
        result = await self.session.execute(
            select(Object).where(Object.object_key == object_key)
        )
        return result.scalar_one_or_none()

    async def get_ready_by_collection_and_name(
        self, collection: str, name: str
    ) -> Object | None:
        """Get a ready object by collection and name."""
        result = await self.session.execute(
            select(Object).where(
                Object.collection == collection,
                Object.name == name,
                Object.status == ObjectStatus.READY,
            )
        )
        return result.scalar_one_or_none()

    async def list_ready_by_collection(self, collection: str) -> list[Object]:
        """List all ready objects in a collection."""
        result = await self.session.execute(
            select(Object)
            .where(Object.collection == collection, Object.status == ObjectStatus.READY)
            .order_by(Object.name)
        )
        return list(result.scalars().all())

    async def create_or_replace(
        self,
        collection: str,
        name: str,
        object_key: str,
        hash_sha256: str,
        size_bytes: int,
    ) -> Object:
        """Create or replace an object. Deletes existing object with same key first."""
        # Delete any existing object with same object_key (to handle UNIQUE constraint)
        await self.session.execute(
            delete(Object).where(Object.object_key == object_key)
        )

        # Create new object
        obj = Object(
            collection=collection,
            name=name,
            object_key=object_key,
            hash_sha256=hash_sha256,
            size_bytes=size_bytes,
            status=ObjectStatus.READY,
        )
        self.session.add(obj)
        await self.session.flush()
        return obj

    async def mark_deleted(self, collection: str, name: str) -> bool:
        """Mark an object as deleted."""
        result = await self.session.execute(
            update(Object)
            .where(
                Object.collection == collection,
                Object.name == name,
                Object.status == ObjectStatus.READY,
            )
            .values(status=ObjectStatus.DELETED, updated_at=datetime.now(timezone.utc))
        )
        return result.rowcount > 0

    async def mark_deleted_by_key(self, object_key: str) -> bool:
        """Mark an object as deleted by its key."""
        result = await self.session.execute(
            update(Object)
            .where(Object.object_key == object_key, Object.status == ObjectStatus.READY)
            .values(status=ObjectStatus.DELETED, updated_at=datetime.now(timezone.utc))
        )
        return result.rowcount > 0
