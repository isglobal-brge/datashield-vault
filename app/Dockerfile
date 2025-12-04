# Build stage
FROM python:3.11-slim as builder

WORKDIR /app

# Install build dependencies
RUN pip install --no-cache-dir hatchling

# Copy project files
COPY pyproject.toml .
COPY src/ src/

# Build the wheel
RUN pip wheel --no-cache-dir --wheel-dir /wheels .

# Runtime stage
FROM python:3.11-slim

WORKDIR /app

# Create non-root user
RUN useradd --create-home --shell /bin/bash vault

# Copy wheels from builder
COPY --from=builder /wheels /wheels

# Install the package
RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels

# Switch to non-root user
USER vault

# Expose API port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')" || exit 1

# Run the application
CMD ["python", "-m", "vault.main"]
