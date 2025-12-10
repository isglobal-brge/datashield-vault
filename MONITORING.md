# Vault Monitoring and Reliability System

This document describes the comprehensive monitoring, self-healing, and reliability measures implemented in the Vault service.

## Table of Contents

- [Overview](#overview)
- [Health Check System](#health-check-system)
- [Metrics Collection](#metrics-collection)
- [Circuit Breaker Pattern](#circuit-breaker-pattern)
- [Rate Limiting](#rate-limiting)
- [Background Monitoring Tasks](#background-monitoring-tasks)
- [Concurrency Control](#concurrency-control)
- [Database Reliability](#database-reliability)
- [API Endpoints](#api-endpoints)

---

## Overview

The monitoring system provides:

- **Real-time health checks** for all critical components
- **Prometheus-compatible metrics** for observability
- **Circuit breaker protection** for external services (MinIO)
- **Rate limiting** to prevent brute force attacks
- **Background self-healing** tasks
- **Data consistency verification** between filesystem, database, and MinIO

---

## Health Check System

### Components Monitored

| Component | Critical | Check Method |
|-----------|----------|--------------|
| Database | Yes | Execute `SELECT 1` with 5s timeout |
| MinIO | No | Verify bucket exists with 5s timeout |
| Filesystem | Yes | Write/read/delete test file |
| Watcher | No | Verify observer thread is alive |
| Consistency | No | Three-way comparison (folder/DB/MinIO) |

### Health Status Types

```python
class ComponentStatus(str, Enum):
    UP = "up"           # Component fully operational
    DOWN = "down"       # Component failed
    DEGRADED = "degraded"  # Component working but with issues
    UNKNOWN = "unknown"    # Unable to determine status
```

### Overall System Status

- **UP**: All components operational
- **DEGRADED**: Non-critical components have issues
- **DOWN**: Any critical component (database, filesystem) is down

### Implementation

Location: `app/src/vault/monitoring/health.py`

Each health check function:
1. Measures latency
2. Performs component-specific validation
3. Returns detailed status with optional metadata
4. Handles timeouts and exceptions gracefully

---

## Metrics Collection

### Metric Types

| Type | Description | Example |
|------|-------------|---------|
| Counter | Monotonically increasing value | `vault_minio_uploads_total` |
| Gauge | Value that can go up or down | `vault_watcher_files_in_progress` |
| Histogram | Distribution of values in buckets | `vault_minio_operation_latency_ms` |

### Available Metrics

#### Database Metrics
- `vault_db_lock_timeouts_total` - Database lock timeout errors
- `vault_db_connection_errors_total` - Connection errors
- `vault_db_query_duration_seconds` - Query duration histogram
- `vault_db_connections_active` - Active connections gauge

#### MinIO Metrics
- `vault_minio_uploads_total` - Total uploads
- `vault_minio_upload_errors_total` - Upload failures
- `vault_minio_deletes_total` - Total deletes
- `vault_minio_delete_errors_total` - Delete failures
- `vault_minio_download_errors_total` - Download failures
- `vault_minio_connection_errors_total` - Connection failures
- `vault_minio_operation_latency_ms` - Operation latency histogram
- `vault_minio_circuit_breaker_open` - Circuit breaker state (0/1)

#### Watcher Metrics
- `vault_watcher_files_processed_total` - Files successfully processed
- `vault_watcher_files_failed_total` - Files that failed processing
- `vault_watcher_files_in_progress` - Currently processing
- `vault_watcher_processing_timeouts_total` - Processing timeouts
- `vault_watcher_semaphore_queue_depth` - Files waiting in queue
- `vault_watcher_thread_restarts_total` - Thread restart count

#### API Metrics
- `vault_api_requests_total` - Total API requests
- `vault_api_request_errors_total` - Request errors
- `vault_api_request_duration_seconds` - Request duration histogram
- `vault_api_auth_failures_total` - Authentication failures
- `vault_api_rate_limit_hits_total` - Rate limit rejections

#### Consistency Metrics
- `vault_consistency_pending_files` - Files awaiting sync
- `vault_consistency_orphaned_objects` - Orphaned MinIO objects
- `vault_consistency_missing_objects` - Objects in DB but missing from MinIO
- `vault_consistency_checks_total` - Total checks performed
- `vault_consistency_errors_found_total` - Errors discovered

### Implementation

Location: `app/src/vault/monitoring/metrics.py`

Thread-safe implementation using locks for all metric updates.

---

## Circuit Breaker Pattern

Protects MinIO operations from cascading failures.

### States

```
CLOSED ──(failures >= threshold)──> OPEN
   ^                                   │
   │                                   │
   └──(successes >= threshold)── HALF_OPEN <──(timeout expires)──┘
```

### Configuration

```python
CircuitBreakerConfig(
    failure_threshold=5,    # Failures before opening
    success_threshold=2,    # Successes to close from half-open
    timeout=30.0,           # Seconds before trying half-open
)
```

### Behavior

| State | Behavior |
|-------|----------|
| CLOSED | Normal operation, track failures |
| OPEN | Reject all requests immediately |
| HALF_OPEN | Allow limited requests to test recovery |

### Implementation

Location: `app/src/vault/minio_client/circuit_breaker.py`

Applied to:
- `upload_file()` - File uploads to MinIO
- `delete_object()` - Object deletion from MinIO

---

## Rate Limiting

Prevents brute force attacks on collection authentication.

### Configuration

```python
RateLimitConfig(
    max_failures=5,        # Max failures before blocking
    window_seconds=60.0,   # Window to count failures
    block_seconds=300.0,   # Block duration (5 minutes)
)
```

### Key Design

- **Per IP + Collection**: Limits are applied per unique (IP, collection) pair
- **Sliding Window**: Only recent failures within the window count
- **Auto-Clear on Success**: Successful authentication clears failure history

### HTTP Responses

| Status | Meaning |
|--------|---------|
| 401 | Invalid or missing authentication |
| 429 | Rate limited - includes `Retry-After` header |

### Implementation

Location: `app/src/vault/api/auth.py`

---

## Background Monitoring Tasks

### Watcher Health Monitor

**Interval**: Every 30 seconds

**Actions**:
1. Check if watcher is supposed to be running
2. Verify observer thread is alive
3. Auto-restart if thread died
4. Log warnings if no events in 10+ minutes

```python
# Auto-restart logic
if watcher.is_running and not watcher.observer_alive:
    logger.error("Watcher observer thread is dead, restarting...")
    watcher.restart()
```

### Consistency Check

**Interval**: Every 5 minutes (with 60s initial delay)

**Checks**:
1. Files in folder but not in database (pending)
2. Objects in database but missing from MinIO (data loss)
3. Sample verification of MinIO objects

**Status Determination**:
- **DOWN**: Objects missing from MinIO (data integrity issue)
- **DEGRADED**: More than 10 files pending sync
- **UP**: All data consistent or minor backlog

### Implementation

Location: `app/src/vault/monitoring/background.py`

---

## Concurrency Control

### Semaphore with Timeout

Controls concurrent file processing to prevent resource exhaustion.

```python
MAX_CONCURRENT_UPLOADS = 10
SEMAPHORE_TIMEOUT = 60.0  # seconds
```

**Features**:
- Queue depth tracking via metrics
- Timeout prevents indefinite blocking
- Logs warnings when queue builds up

### Debouncing

File events are debounced to prevent duplicate processing:

```python
DEBOUNCE_SECONDS = 0.5  # 500ms debounce window
```

### Implementation

Location: `app/src/vault/watcher/handler.py`

---

## Database Reliability

### SQLite Configuration

```python
# WAL mode for better concurrency
"PRAGMA journal_mode=WAL"
"PRAGMA synchronous=NORMAL"
"PRAGMA busy_timeout=30000"  # 30 second lock timeout
```

### Connection Pooling

Using `NullPool` for SQLite to avoid connection sharing issues:

```python
engine = create_async_engine(
    database_url,
    poolclass=NullPool,  # Each request gets fresh connection
)
```

### Session Management

- Async context manager for automatic cleanup
- Explicit commit/rollback handling
- Connection error tracking via metrics

### Implementation

Location: `app/src/vault/db/session.py`

---

## API Endpoints

### Health Endpoints

| Endpoint | Purpose | Response |
|----------|---------|----------|
| `GET /health/live` | Liveness check | `{"status": "ok"}` |
| `GET /health/ready` | Readiness check | Status based on critical components |
| `GET /health/status` | Full system status | Detailed component health |
| `GET /health/status?consistency=true` | Include consistency check | Slower, more thorough |

### Metrics Endpoints

| Endpoint | Format |
|----------|--------|
| `GET /health/metrics` | Prometheus exposition format |
| `GET /health/metrics/json` | JSON format |

### Example Responses

#### Liveness Check
```json
{"status": "ok"}
```

#### Readiness Check (Healthy)
```json
{
  "status": "ready",
  "checks": {
    "database": true,
    "filesystem": true
  }
}
```

#### Full Status
```json
{
  "status": "up",
  "timestamp": 1702234567.123,
  "components": {
    "database": {
      "name": "database",
      "status": "up",
      "latency_ms": 1.23,
      "message": "Database operational",
      "details": {"pool_status": "NullPool"}
    },
    "minio": {
      "name": "minio",
      "status": "up",
      "latency_ms": 45.67,
      "message": "MinIO operational",
      "details": {"bucket": "vault"}
    }
  },
  "critical_failures": []
}
```

### Implementation

Location: `app/src/vault/monitoring/router.py`

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         FastAPI Application                      │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │ API Router  │  │Health Router│  │  Background Tasks       │  │
│  │             │  │             │  │  ┌───────────────────┐  │  │
│  │ /collections│  │ /health/*   │  │  │ Watcher Monitor   │  │  │
│  │ /objects    │  │ /metrics    │  │  │ (30s interval)    │  │  │
│  │             │  │             │  │  ├───────────────────┤  │  │
│  │ Rate Limit  │  │             │  │  │ Consistency Check │  │  │
│  │ ↓           │  │             │  │  │ (5min interval)   │  │  │
│  └─────────────┘  └─────────────┘  │  └───────────────────┘  │  │
├─────────────────────────────────────────────────────────────────┤
│                       Core Services                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │  Database   │  │   MinIO     │  │    File Watcher         │  │
│  │  (SQLite)   │  │  Client     │  │                         │  │
│  │             │  │             │  │  PollingObserver        │  │
│  │  NullPool   │  │  Circuit    │  │  ↓                      │  │
│  │  WAL Mode   │  │  Breaker    │  │  VaultEventHandler      │  │
│  │             │  │             │  │  ↓                      │  │
│  └─────────────┘  └─────────────┘  │  Semaphore (max 10)     │  │
│                                    │  ↓                      │  │
│                                    │  Debounce (500ms)       │  │
│                                    └─────────────────────────┘  │
├─────────────────────────────────────────────────────────────────┤
│                    Metrics Registry                              │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ Counters: uploads, errors, auth_failures, rate_limits      ││
│  │ Gauges: in_progress, pending_files, circuit_breaker_state  ││
│  │ Histograms: latency, query_duration, request_duration      ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

