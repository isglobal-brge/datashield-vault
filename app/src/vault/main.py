"""Main entry point for the Vault service."""

import logging
import sys

import uvicorn

from vault.api.app import create_app
from vault.config import get_settings


def setup_logging() -> None:
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    # Set specific loggers
    logging.getLogger("watchdog").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def main() -> None:
    """Run the Vault service."""
    setup_logging()
    settings = get_settings()

    app = create_app()

    uvicorn.run(
        app,
        host=settings.api_host,
        port=settings.api_port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
