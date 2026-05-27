# syntax=docker/dockerfile:1
#
# parser-os-worker — Container Apps Job image
#
# Pulls one queue message per process invocation, runs parser-os compile,
# uploads envelope to blob, exits.  See README.md.
#
# Build:
#   DOCKER_BUILDKIT=1 docker build \
#     --no-cache \
#     --build-arg GIT_SHA=$(git rev-parse HEAD) \
#     --build-arg PARSER_OS_SHA=$(git -C ../parser-os rev-parse v45.2) \
#     --build-arg BUILD_LABEL=v45.2 \
#     -t parser-os-worker:dev \
#     .
#
# Or use scripts/build_image.sh which does --no-cache + fresh-clone for you.

FROM python:3.11-slim AS runtime

# Install OS deps that parser-os needs for OCR + image rendering at compile time
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        poppler-utils \
        tesseract-ocr \
        libgl1 \
        libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 10001 parserosworker
WORKDIR /build

# Copy parser-os and parser-os-service (worker reuses projector) as siblings
COPY parser-os ./parser-os
COPY parser-os-service ./parser-os-service
COPY parser-os-worker ./parser-os-worker

ENV PIP_ROOT_USER_ACTION=ignore

RUN set -eux; \
    pip install --no-cache-dir --upgrade pip setuptools wheel; \
    # Install parser-os from local source so it matches the SHA we cloned
    pip install --no-cache-dir ./parser-os; \
    # Install parser-os-service (need projector — no extra deps)
    pip install --no-cache-dir --no-deps ./parser-os-service; \
    # parser-os-service deps that the projector needs
    pip install --no-cache-dir \
      "azure-identity>=1.15" \
      "azure-storage-blob>=12.19" \
      "azure-storage-queue>=12.10" \
      "fastapi>=0.110" \
      "pydantic>=2.5" \
      "structlog>=24.1"; \
    # Worker itself (no-deps because pyproject pins parser-os from git — we
    # already installed from local above)
    pip install --no-cache-dir --no-deps ./parser-os-worker; \
    rm -rf /build

WORKDIR /app

# ─── v45.2: SHA stamping (same pattern as parser-os-service) ──────────────
ARG GIT_SHA=unknown
ARG PARSER_OS_SHA=unknown
ARG BUILD_LABEL=unknown

ENV PARSER_OS_WORKER_SHA=$GIT_SHA
ENV PARSER_OS_SHA=$PARSER_OS_SHA
ENV PARSER_OS_BUILD_LABEL=$BUILD_LABEL

RUN echo "$GIT_SHA" > /app/.git_sha \
 && echo "$PARSER_OS_SHA" > /app/.parser_os_sha \
 && echo "$BUILD_LABEL" > /app/.build_label \
 && chown parserosworker:parserosworker /app/.git_sha /app/.parser_os_sha /app/.build_label

USER parserosworker

# This is a Job, not a server — no port to expose, no health check needed.
# Container Apps Jobs let the process exit cleanly when done.
ENV PYTHONUNBUFFERED=1

# One process = one message = exit.  Container Apps Jobs + KEDA handle the rest.
CMD ["python", "-m", "parser_os_worker.main"]
