"""Background tasks for monitoring and self-healing."""

import asyncio
import logging
import time
from typing import Callable, Coroutine, Any

from vault.monitoring.health import check_consistency, check_watcher, ComponentStatus
from vault.monitoring.metrics import metrics
from vault.watcher.watcher import get_watcher

logger = logging.getLogger(__name__)


class BackgroundTaskManager:
    """
    Manages background monitoring and self-healing tasks.

    Tasks:
    - Watcher health monitoring (auto-restart on thread death)
    - Periodic consistency checks
    - Metrics updates
    """

    def __init__(self):
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        """Start all background tasks."""
        if self._running:
            return

        self._running = True
        self._stop_event.clear()

        # Start monitoring tasks
        self._tasks = [
            asyncio.create_task(self._watcher_health_loop(), name="watcher_health"),
            asyncio.create_task(self._consistency_check_loop(), name="consistency_check"),
        ]

        logger.info("Background monitoring tasks started")

    async def stop(self) -> None:
        """Stop all background tasks gracefully."""
        if not self._running:
            return

        self._running = False
        self._stop_event.set()

        # Cancel all tasks
        for task in self._tasks:
            task.cancel()

        # Wait for tasks to finish
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        logger.info("Background monitoring tasks stopped")

    async def _watcher_health_loop(self) -> None:
        """
        Monitor watcher health and auto-restart if thread dies.

        Checks every 30 seconds.
        """
        check_interval = 30  # seconds

        while self._running:
            try:
                await asyncio.sleep(check_interval)

                watcher = get_watcher()

                # Check if watcher should be running but observer is dead
                if watcher.is_running and not watcher.observer_alive:
                    logger.error("Watcher observer thread is dead, restarting...")
                    watcher.restart()
                    continue

                # Log status periodically
                status = watcher.get_health_status()
                if status["last_event_age_s"] and status["last_event_age_s"] > 600:
                    logger.warning(
                        f"No watcher events in {status['last_event_age_s']:.0f}s - "
                        "this may be normal if no files are changing"
                    )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in watcher health loop: {e}")
                await asyncio.sleep(5)  # Brief pause before retry

    async def _consistency_check_loop(self) -> None:
        """
        Perform periodic consistency checks.

        Runs every 5 minutes by default.
        """
        check_interval = 300  # 5 minutes

        # Initial delay to let system stabilize
        await asyncio.sleep(60)

        while self._running:
            try:
                start = time.time()
                result = await check_consistency()

                elapsed = time.time() - start
                logger.info(
                    f"Consistency check completed in {elapsed:.1f}s: "
                    f"status={result.status.value}, {result.message}"
                )

                # Update metrics based on result
                if result.status == ComponentStatus.DOWN:
                    logger.error(f"Consistency check FAILED: {result.message}")
                elif result.status == ComponentStatus.DEGRADED:
                    logger.warning(f"Consistency check DEGRADED: {result.message}")

                await asyncio.sleep(check_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in consistency check loop: {e}")
                await asyncio.sleep(60)  # Brief pause before retry


# Global task manager
_task_manager: BackgroundTaskManager | None = None


def get_task_manager() -> BackgroundTaskManager:
    """Get or create the global background task manager."""
    global _task_manager
    if _task_manager is None:
        _task_manager = BackgroundTaskManager()
    return _task_manager
