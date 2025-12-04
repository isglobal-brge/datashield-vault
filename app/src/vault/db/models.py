"""SQLAlchemy database models."""

from datetime import datetime
from enum import Enum

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, Integer, String, Text, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all models."""

    pass


class ObjectStatus(str, Enum):
    """Status of an object in the vault."""

    READY = "READY"
    UPDATING = "UPDATING"
    DELETED = "DELETED"


class Collection(Base):
    """A collection (namespace) for objects."""

    __tablename__ = "collections"

    name: Mapped[str] = mapped_column(String(255), primary_key=True)
    api_key_hash: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    objects: Mapped[list["Object"]] = relationship(
        "Object", back_populates="collection_rel", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Collection(name={self.name!r}, is_active={self.is_active})>"


class Object(Base):
    """An object stored in the vault."""

    __tablename__ = "objects"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    collection: Mapped[str] = mapped_column(
        String(255), ForeignKey("collections.name", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(1024), nullable=False)
    object_key: Mapped[str] = mapped_column(String(2048), unique=True, nullable=False)
    hash_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[ObjectStatus] = mapped_column(
        String(20), default=ObjectStatus.READY, nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    collection_rel: Mapped["Collection"] = relationship("Collection", back_populates="objects")

    __table_args__ = (
        Index("ix_objects_collection_name_status", "collection", "name", "status"),
        Index("ix_objects_collection_status", "collection", "status"),
    )

    def __repr__(self) -> str:
        return f"<Object(id={self.id}, collection={self.collection!r}, name={self.name!r}, status={self.status})>"
