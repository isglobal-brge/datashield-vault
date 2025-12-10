"""Authentication dependencies for the Vault API."""

import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Annotated, Dict

from fastapi import Depends, Header, HTTPException, Path, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from vault.db.repository import CollectionRepository
from vault.db.session import get_db_session
from vault.monitoring.metrics import metrics

logger = logging.getLogger(__name__)


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""

    max_failures: int = 5  # Max failures before blocking
    window_seconds: float = 60.0  # Window to count failures
    block_seconds: float = 300.0  # Block duration after exceeding limit


class AuthRateLimiter:
    """
    Rate limiter for authentication failures.

    Tracks failed auth attempts per IP/collection and blocks
    excessive failures to prevent brute force attacks.
    """

    def __init__(self, config: RateLimitConfig | None = None):
        self.config = config or RateLimitConfig()
        self._failures: Dict[str, list] = defaultdict(list)  # key -> [timestamps]
        self._blocked_until: Dict[str, float] = {}  # key -> unblock_timestamp
        self._lock = threading.Lock()

    def _make_key(self, ip: str, collection: str) -> str:
        """Create a unique key for IP + collection."""
        return f"{ip}:{collection}"

    def _cleanup_old_failures(self, key: str) -> None:
        """Remove failures outside the window."""
        cutoff = time.time() - self.config.window_seconds
        self._failures[key] = [t for t in self._failures[key] if t > cutoff]

    def is_blocked(self, ip: str, collection: str) -> tuple[bool, float]:
        """
        Check if an IP/collection combo is blocked.

        Returns:
            Tuple of (is_blocked, seconds_remaining)
        """
        key = self._make_key(ip, collection)
        with self._lock:
            if key in self._blocked_until:
                remaining = self._blocked_until[key] - time.time()
                if remaining > 0:
                    return True, remaining
                else:
                    # Block expired
                    del self._blocked_until[key]
                    self._failures.pop(key, None)
            return False, 0

    def record_failure(self, ip: str, collection: str) -> bool:
        """
        Record an auth failure.

        Returns:
            True if the IP/collection is now blocked
        """
        key = self._make_key(ip, collection)
        now = time.time()

        with self._lock:
            self._cleanup_old_failures(key)
            self._failures[key].append(now)

            if len(self._failures[key]) >= self.config.max_failures:
                # Block this IP/collection
                self._blocked_until[key] = now + self.config.block_seconds
                logger.warning(
                    f"Rate limiting: blocking {ip} for collection {collection} "
                    f"after {len(self._failures[key])} failures"
                )
                return True
        return False

    def record_success(self, ip: str, collection: str) -> None:
        """Clear failures on successful auth."""
        key = self._make_key(ip, collection)
        with self._lock:
            self._failures.pop(key, None)
            self._blocked_until.pop(key, None)

    def get_stats(self) -> dict:
        """Get rate limiter statistics."""
        with self._lock:
            active_blocks = sum(
                1 for t in self._blocked_until.values() if t > time.time()
            )
            return {
                "tracked_keys": len(self._failures),
                "active_blocks": active_blocks,
                "config": {
                    "max_failures": self.config.max_failures,
                    "window_seconds": self.config.window_seconds,
                    "block_seconds": self.config.block_seconds,
                },
            }


# Global rate limiter instance
_auth_rate_limiter = AuthRateLimiter()


def get_client_ip(request: Request) -> str:
    """Extract client IP from request, handling proxies."""
    # Check X-Forwarded-For for clients behind proxies
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        # Take the first IP (original client)
        return forwarded.split(",")[0].strip()
    # Fall back to direct client
    return request.client.host if request.client else "unknown"


async def verify_collection_key(
    request: Request,
    collection: Annotated[str, Path(description="Collection name")],
    x_collection_key: Annotated[str, Header()],
    session: AsyncSession = Depends(get_db_session),
) -> str:
    """
    Verify access to a collection via collection key.

    Includes rate limiting to prevent brute force attacks.

    Returns the collection name if authorized.
    Raises HTTPException if not authorized or rate limited.
    """
    client_ip = get_client_ip(request)

    # Check if rate limited
    is_blocked, remaining = _auth_rate_limiter.is_blocked(client_ip, collection)
    if is_blocked:
        metrics.api_rate_limit_hits.inc()
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Too many failed authentication attempts. Try again in {int(remaining)} seconds.",
            headers={"Retry-After": str(int(remaining))},
        )

    repo = CollectionRepository(session)
    if await repo.verify_api_key(collection, x_collection_key):
        _auth_rate_limiter.record_success(client_ip, collection)
        return collection

    # Record failure and check if now blocked
    metrics.api_auth_failures.inc()
    was_blocked = _auth_rate_limiter.record_failure(client_ip, collection)

    if was_blocked:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"Too many failed authentication attempts. Try again in {int(_auth_rate_limiter.config.block_seconds)} seconds.",
            headers={"Retry-After": str(int(_auth_rate_limiter.config.block_seconds))},
        )

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing X-Collection-Key header.",
    )


# Type alias for dependency injection
CollectionKeyDep = Annotated[str, Depends(verify_collection_key)]


def get_auth_rate_limiter() -> AuthRateLimiter:
    """Get the global auth rate limiter for monitoring."""
    return _auth_rate_limiter
