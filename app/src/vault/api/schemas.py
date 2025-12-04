"""Pydantic schemas for API request/response models."""

from pydantic import BaseModel


class ObjectHash(BaseModel):
    """Object with hash information."""

    name: str
    hash_sha256: str


class ObjectListResponse(BaseModel):
    """Response for listing objects in a collection."""

    collection: str
    objects: list[str]


class HashListResponse(BaseModel):
    """Response for listing hashes in a collection."""

    collection: str
    items: list[ObjectHash]


class SingleHashResponse(BaseModel):
    """Response for a single object hash."""

    collection: str
    name: str
    hash_sha256: str
