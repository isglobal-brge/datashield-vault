FROM python:3.11-slim@sha256:9534e5a8e315485d4061ed659af0fd78a284c015f9b73661b41d6bab25604534
ARG VCS_REF=""
LABEL org.opencontainers.image.source="https://github.com/isglobal-brge/dsimaging-store" \
      org.opencontainers.image.version="0.3.8" \
      org.opencontainers.image.revision="${VCS_REF}"
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY controller.py .
RUN python -c "import controller"
RUN groupadd --system --gid 10001 controller && \
    useradd --system --uid 10001 --gid controller --create-home \
      --home-dir /home/controller --shell /usr/sbin/nologin controller
USER controller:controller
EXPOSE 8080
CMD ["python", "controller.py"]
