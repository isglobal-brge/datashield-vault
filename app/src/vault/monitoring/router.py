"""Health check and metrics API routes."""

import logging
import time
from typing import Any, Dict

from fastapi import APIRouter, Query, Response

from vault.monitoring.health import (
    ComponentStatus,
    get_system_health,
)
from vault.monitoring.metrics import metrics

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/health", tags=["health"])


@router.get("/live")
async def liveness() -> Dict[str, str]:
    """
    Kubernetes liveness probe.

    Returns 200 if the process is running.
    This should be a very fast check with no dependencies.
    """
    return {"status": "alive"}


@router.get("/ready")
async def readiness() -> Dict[str, Any]:
    """
    Kubernetes readiness probe.

    Returns 200 if the service is ready to accept traffic.
    Checks critical components: database and filesystem.
    """
    health = await get_system_health(include_consistency=False)

    if health.status == ComponentStatus.DOWN:
        return Response(
            content='{"status": "not_ready", "critical_failures": ' +
                    str(health.critical_failures).replace("'", '"') + '}',
            status_code=503,
            media_type="application/json",
        )

    return {
        "status": "ready",
        "timestamp": health.timestamp,
    }


@router.get("/status")
async def status(
    include_consistency: bool = Query(
        default=False,
        description="Include data consistency check (slower)",
    ),
) -> Dict[str, Any]:
    """
    Detailed health status of all components.

    Returns comprehensive health information including:
    - Overall system status
    - Individual component status (database, minio, filesystem, watcher)
    - Latency measurements
    - Optional data consistency check
    """
    health = await get_system_health(include_consistency=include_consistency)

    components = {}
    for name, component in health.components.items():
        components[name] = {
            "status": component.status.value,
            "latency_ms": round(component.latency_ms, 2),
            "message": component.message,
        }
        if component.details:
            components[name]["details"] = component.details

    response = {
        "status": health.status.value,
        "timestamp": health.timestamp,
        "components": components,
    }

    if health.critical_failures:
        response["critical_failures"] = health.critical_failures

    # Return 503 if system is down
    if health.status == ComponentStatus.DOWN:
        return Response(
            content=str(response).replace("'", '"'),
            status_code=503,
            media_type="application/json",
        )

    return response


@router.get("/metrics")
async def prometheus_metrics() -> Response:
    """
    Prometheus metrics endpoint.

    Returns metrics in Prometheus text format for scraping.
    """
    output = metrics.export_prometheus()
    return Response(
        content=output,
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


@router.get("/metrics/json")
async def json_metrics() -> Dict[str, Any]:
    """
    JSON format metrics endpoint.

    Returns all metrics in JSON format for easier debugging.
    """
    return metrics.export_json()
