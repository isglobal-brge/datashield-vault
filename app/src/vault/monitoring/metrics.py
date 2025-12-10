"""Prometheus-style metrics for vault monitoring.

Simple in-memory metrics that can be scraped via /metrics endpoint.
For production, integrate with prometheus_client library.
"""

import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class Counter:
    """Simple counter metric."""

    name: str
    description: str
    _value: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def inc(self, value: float = 1.0) -> None:
        with self._lock:
            self._value += value

    def get(self) -> float:
        return self._value


@dataclass
class Gauge:
    """Simple gauge metric."""

    name: str
    description: str
    _value: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def set(self, value: float) -> None:
        with self._lock:
            self._value = value

    def inc(self, value: float = 1.0) -> None:
        with self._lock:
            self._value += value

    def dec(self, value: float = 1.0) -> None:
        with self._lock:
            self._value -= value

    def get(self) -> float:
        return self._value


@dataclass
class Histogram:
    """Simple histogram metric with buckets."""

    name: str
    description: str
    buckets: List[float] = field(default_factory=lambda: [0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0])
    _counts: Dict[float, int] = field(default_factory=dict)
    _sum: float = 0.0
    _count: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def __post_init__(self):
        self._counts = {b: 0 for b in self.buckets}
        self._counts[float("inf")] = 0

    def observe(self, value: float) -> None:
        with self._lock:
            self._sum += value
            self._count += 1
            for bucket in self.buckets:
                if value <= bucket:
                    self._counts[bucket] += 1
            self._counts[float("inf")] += 1

    def get_count(self) -> int:
        return self._count

    def get_sum(self) -> float:
        return self._sum


class MetricsRegistry:
    """Registry for all vault metrics."""

    def __init__(self):
        # Database metrics
        self.db_lock_timeouts = Counter(
            "vault_db_lock_timeouts_total",
            "Total database lock timeout errors",
        )
        self.db_connection_errors = Counter(
            "vault_db_connection_errors_total",
            "Total database connection errors",
        )
        self.db_query_duration = Histogram(
            "vault_db_query_duration_seconds",
            "Database query duration in seconds",
        )
        self.db_connections_active = Gauge(
            "vault_db_connections_active",
            "Number of active database connections",
        )

        # MinIO metrics
        self.minio_uploads_total = Counter(
            "vault_minio_uploads_total",
            "Total MinIO uploads",
        )
        self.minio_upload_errors = Counter(
            "vault_minio_upload_errors_total",
            "Total MinIO upload errors",
        )
        self.minio_deletes_total = Counter(
            "vault_minio_deletes_total",
            "Total MinIO deletes",
        )
        self.minio_delete_errors = Counter(
            "vault_minio_delete_errors_total",
            "Total MinIO delete errors",
        )
        self.minio_download_errors = Counter(
            "vault_minio_download_errors_total",
            "Total MinIO download errors",
        )
        self.minio_connection_errors = Counter(
            "vault_minio_connection_errors_total",
            "Total MinIO connection errors",
        )
        self.minio_operation_latency = Histogram(
            "vault_minio_operation_latency_ms",
            "MinIO operation latency in milliseconds",
            buckets=[10, 50, 100, 250, 500, 1000, 2500, 5000, 10000],
        )
        self.minio_circuit_breaker_state = Gauge(
            "vault_minio_circuit_breaker_open",
            "MinIO circuit breaker state (0=closed, 1=open)",
        )

        # Watcher metrics
        self.watcher_files_processed = Counter(
            "vault_watcher_files_processed_total",
            "Total files processed by watcher",
        )
        self.watcher_files_failed = Counter(
            "vault_watcher_files_failed_total",
            "Total files that failed processing",
        )
        self.watcher_files_in_progress = Gauge(
            "vault_watcher_files_in_progress",
            "Number of files currently being processed",
        )
        self.watcher_processing_timeouts = Counter(
            "vault_watcher_processing_timeouts_total",
            "Total file processing timeouts",
        )
        self.watcher_semaphore_queue_depth = Gauge(
            "vault_watcher_semaphore_queue_depth",
            "Number of files waiting for semaphore",
        )
        self.watcher_thread_restarts = Counter(
            "vault_watcher_thread_restarts_total",
            "Total watcher thread restarts",
        )

        # API metrics
        self.api_requests_total = Counter(
            "vault_api_requests_total",
            "Total API requests",
        )
        self.api_request_errors = Counter(
            "vault_api_request_errors_total",
            "Total API request errors",
        )
        self.api_request_duration = Histogram(
            "vault_api_request_duration_seconds",
            "API request duration in seconds",
        )
        self.api_auth_failures = Counter(
            "vault_api_auth_failures_total",
            "Total API authentication failures",
        )
        self.api_rate_limit_hits = Counter(
            "vault_api_rate_limit_hits_total",
            "Total rate limit rejections",
        )

        # Consistency metrics
        self.consistency_pending_files = Gauge(
            "vault_consistency_pending_files",
            "Number of files pending sync",
        )
        self.consistency_orphaned_objects = Gauge(
            "vault_consistency_orphaned_objects",
            "Number of orphaned MinIO objects",
        )
        self.consistency_missing_objects = Gauge(
            "vault_consistency_missing_objects",
            "Number of objects in DB but missing from MinIO",
        )
        self.consistency_checks_total = Counter(
            "vault_consistency_checks_total",
            "Total consistency checks performed",
        )
        self.consistency_errors_found = Counter(
            "vault_consistency_errors_found_total",
            "Total consistency errors found",
        )

        # Health check metrics
        self.health_check_failures = Counter(
            "vault_health_check_failures_total",
            "Total health check failures",
        )
        self.last_health_check_time = Gauge(
            "vault_last_health_check_timestamp",
            "Timestamp of last health check",
        )

    def get_all_metrics(self) -> Dict[str, float]:
        """Get all metrics as a dictionary."""
        result = {}
        for name, value in vars(self).items():
            if isinstance(value, Counter):
                result[value.name] = value.get()
            elif isinstance(value, Gauge):
                result[value.name] = value.get()
            elif isinstance(value, Histogram):
                result[f"{value.name}_count"] = value.get_count()
                result[f"{value.name}_sum"] = value.get_sum()
        return result

    def format_prometheus(self) -> str:
        """Format metrics in Prometheus exposition format."""
        lines = []
        for name, value in vars(self).items():
            if isinstance(value, Counter):
                lines.append(f"# HELP {value.name} {value.description}")
                lines.append(f"# TYPE {value.name} counter")
                lines.append(f"{value.name} {value.get()}")
            elif isinstance(value, Gauge):
                lines.append(f"# HELP {value.name} {value.description}")
                lines.append(f"# TYPE {value.name} gauge")
                lines.append(f"{value.name} {value.get()}")
            elif isinstance(value, Histogram):
                lines.append(f"# HELP {value.name} {value.description}")
                lines.append(f"# TYPE {value.name} histogram")
                lines.append(f"{value.name}_count {value.get_count()}")
                lines.append(f"{value.name}_sum {value.get_sum()}")
        return "\n".join(lines)

    def export_prometheus(self) -> str:
        """Alias for format_prometheus for consistency with router."""
        return self.format_prometheus()

    def export_json(self) -> Dict[str, float]:
        """Alias for get_all_metrics for consistency with router."""
        return self.get_all_metrics()


# Global metrics registry
metrics = MetricsRegistry()
