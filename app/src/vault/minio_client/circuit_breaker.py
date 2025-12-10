"""Circuit breaker pattern for MinIO client operations."""

import logging
import threading
import time
from dataclasses import dataclass
from enum import Enum
from functools import wraps
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


class CircuitState(str, Enum):
    """Circuit breaker states."""

    CLOSED = "closed"  # Normal operation, requests pass through
    OPEN = "open"  # Failures exceeded threshold, requests fail fast
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""

    failure_threshold: int = 5  # Failures before opening
    success_threshold: int = 2  # Successes in half-open to close
    timeout: float = 30.0  # Seconds before trying half-open
    excluded_exceptions: tuple = ()  # Exceptions that don't count as failures


class CircuitBreakerError(Exception):
    """Raised when circuit is open and request is rejected."""

    def __init__(self, message: str, time_remaining: float):
        super().__init__(message)
        self.time_remaining = time_remaining


class CircuitBreaker:
    """
    Circuit breaker implementation.

    Prevents cascading failures by temporarily blocking calls to a failing service.
    """

    def __init__(self, name: str, config: CircuitBreakerConfig | None = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: float | None = None
        self._lock = threading.RLock()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state, transitioning if timeout expired."""
        with self._lock:
            if self._state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._transition_to(CircuitState.HALF_OPEN)
            return self._state

    @property
    def failure_count(self) -> int:
        return self._failure_count

    @property
    def is_open(self) -> bool:
        return self.state == CircuitState.OPEN

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to try half-open."""
        if self._last_failure_time is None:
            return True
        return (time.time() - self._last_failure_time) >= self.config.timeout

    def _transition_to(self, state: CircuitState) -> None:
        """Transition to a new state."""
        if self._state != state:
            logger.info(f"Circuit breaker '{self.name}': {self._state.value} -> {state.value}")
            self._state = state
            if state == CircuitState.CLOSED:
                self._failure_count = 0
                self._success_count = 0
            elif state == CircuitState.HALF_OPEN:
                self._success_count = 0

    def record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._transition_to(CircuitState.CLOSED)
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = 0

    def record_failure(self) -> None:
        """Record a failed call."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                # Any failure in half-open immediately opens
                self._transition_to(CircuitState.OPEN)
            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self.config.failure_threshold:
                    self._transition_to(CircuitState.OPEN)

    def check_state(self) -> None:
        """Check if request should proceed, raise if circuit is open."""
        current_state = self.state  # This may transition from OPEN to HALF_OPEN

        if current_state == CircuitState.OPEN:
            time_remaining = 0.0
            if self._last_failure_time:
                time_remaining = max(
                    0, self.config.timeout - (time.time() - self._last_failure_time)
                )
            raise CircuitBreakerError(
                f"Circuit breaker '{self.name}' is open. "
                f"Service unavailable, retry in {time_remaining:.1f}s",
                time_remaining,
            )

    def reset(self) -> None:
        """Manually reset the circuit breaker to closed state."""
        with self._lock:
            self._transition_to(CircuitState.CLOSED)
            self._last_failure_time = None

    def get_status(self) -> dict:
        """Get circuit breaker status for monitoring."""
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "last_failure_time": self._last_failure_time,
            "config": {
                "failure_threshold": self.config.failure_threshold,
                "success_threshold": self.config.success_threshold,
                "timeout": self.config.timeout,
            },
        }


def circuit_breaker_protected(breaker: CircuitBreaker) -> Callable[[F], F]:
    """
    Decorator to protect a function with a circuit breaker.

    Usage:
        @circuit_breaker_protected(minio_circuit_breaker)
        def upload_file(...):
            ...
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Check if circuit allows the call
            breaker.check_state()

            try:
                result = func(*args, **kwargs)
                breaker.record_success()
                return result
            except breaker.config.excluded_exceptions:
                # These exceptions don't count as failures
                raise
            except Exception as e:
                breaker.record_failure()
                raise

        return wrapper  # type: ignore

    return decorator


# Global circuit breaker for MinIO operations
minio_circuit_breaker = CircuitBreaker(
    name="minio",
    config=CircuitBreakerConfig(
        failure_threshold=5,  # Open after 5 consecutive failures
        success_threshold=2,  # Close after 2 successes in half-open
        timeout=30.0,  # Wait 30s before trying again
    ),
)
