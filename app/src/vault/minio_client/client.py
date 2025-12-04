"""MinIO client wrapper for object storage operations."""

import hashlib
import io
from collections.abc import Generator
from pathlib import Path

from minio import Minio
from minio.error import S3Error

from vault.config import get_settings

_client: Minio | None = None

CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks for streaming


def get_minio_client() -> Minio:
    """Get or create the MinIO client."""
    global _client
    if _client is None:
        settings = get_settings()
        _client = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        )
    return _client


def ensure_bucket_exists() -> None:
    """Ensure the configured bucket exists."""
    settings = get_settings()
    client = get_minio_client()
    if not client.bucket_exists(settings.minio_bucket):
        client.make_bucket(settings.minio_bucket)


def compute_file_hash(file_path: Path) -> tuple[str, int]:
    """
    Compute SHA-256 hash of a file using streaming.

    Returns:
        Tuple of (hash_hex, size_bytes)
    """
    sha256 = hashlib.sha256()
    size = 0
    with open(file_path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            sha256.update(chunk)
            size += len(chunk)
    return sha256.hexdigest(), size


def upload_file(object_key: str, file_path: Path) -> None:
    """
    Upload a file to MinIO.

    Args:
        object_key: The key (path) in MinIO
        file_path: Local path to the file
    """
    settings = get_settings()
    client = get_minio_client()

    client.fput_object(
        settings.minio_bucket,
        object_key,
        str(file_path),
    )


def delete_object(object_key: str) -> bool:
    """
    Delete an object from MinIO.

    Args:
        object_key: The key (path) in MinIO

    Returns:
        True if deleted, False if not found
    """
    settings = get_settings()
    client = get_minio_client()

    try:
        client.remove_object(settings.minio_bucket, object_key)
        return True
    except S3Error as e:
        if e.code == "NoSuchKey":
            return False
        raise


def object_exists(object_key: str) -> bool:
    """Check if an object exists in MinIO."""
    settings = get_settings()
    client = get_minio_client()

    try:
        client.stat_object(settings.minio_bucket, object_key)
        return True
    except S3Error as e:
        if e.code == "NoSuchKey":
            return False
        raise


def get_object_stream(object_key: str) -> Generator[bytes, None, None]:
    """
    Get an object from MinIO as a streaming generator.

    Args:
        object_key: The key (path) in MinIO

    Yields:
        Chunks of the object data
    """
    settings = get_settings()
    client = get_minio_client()

    response = None
    try:
        response = client.get_object(settings.minio_bucket, object_key)
        while chunk := response.read(CHUNK_SIZE):
            yield chunk
    finally:
        if response is not None:
            response.close()
            response.release_conn()


def get_object_info(object_key: str) -> dict | None:
    """
    Get object metadata from MinIO.

    Returns:
        Dict with size and content_type, or None if not found
    """
    settings = get_settings()
    client = get_minio_client()

    try:
        stat = client.stat_object(settings.minio_bucket, object_key)
        return {
            "size": stat.size,
            "content_type": stat.content_type,
            "etag": stat.etag,
            "last_modified": stat.last_modified,
        }
    except S3Error as e:
        if e.code == "NoSuchKey":
            return None
        raise
