"""MinIO client wrapper for object storage operations."""

import hashlib
import io
import logging
import time
from collections.abc import Generator
from pathlib import Path

from minio import Minio
from minio.error import S3Error

from vault.config import get_settings
from vault.minio_client.circuit_breaker import (
    CircuitBreakerError,
    minio_circuit_breaker,
)
from vault.monitoring.metrics import metrics

logger = logging.getLogger(__name__)

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

    Raises:
        CircuitBreakerError: If MinIO circuit breaker is open
    """
    # Check circuit breaker first
    minio_circuit_breaker.check_state()

    settings = get_settings()
    client = get_minio_client()

    start = time.time()
    try:
        client.fput_object(
            settings.minio_bucket,
            object_key,
            str(file_path),
        )
        # Record success
        minio_circuit_breaker.record_success()
        latency = (time.time() - start) * 1000
        metrics.minio_operation_latency.observe(latency)
        metrics.minio_uploads_total.inc()
        logger.debug(f"Uploaded {object_key} in {latency:.1f}ms")
    except Exception as e:
        # Record failure for circuit breaker
        minio_circuit_breaker.record_failure()
        metrics.minio_upload_errors.inc()
        metrics.minio_circuit_breaker_state.set(1 if minio_circuit_breaker.is_open else 0)
        logger.error(f"MinIO upload failed for {object_key}: {e}")
        raise


def delete_object(object_key: str) -> bool:
    """
    Delete an object from MinIO.

    Args:
        object_key: The key (path) in MinIO

    Returns:
        True if deleted, False if not found

    Raises:
        CircuitBreakerError: If MinIO circuit breaker is open
    """
    # Check circuit breaker first
    minio_circuit_breaker.check_state()

    settings = get_settings()
    client = get_minio_client()

    start = time.time()
    try:
        client.remove_object(settings.minio_bucket, object_key)
        minio_circuit_breaker.record_success()
        latency = (time.time() - start) * 1000
        metrics.minio_operation_latency.observe(latency)
        metrics.minio_deletes_total.inc()
        return True
    except S3Error as e:
        if e.code == "NoSuchKey":
            # Not a failure - object just doesn't exist
            return False
        minio_circuit_breaker.record_failure()
        metrics.minio_delete_errors.inc()
        metrics.minio_circuit_breaker_state.set(1 if minio_circuit_breaker.is_open else 0)
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
