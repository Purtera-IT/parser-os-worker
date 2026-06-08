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

# v51: install tailscale + curl + iptables so the worker can join the
# tailnet at startup and reach Mac Studio Ollama directly via
# 100.114.102.122:11434 — bypasses the broken HTTPS proxy that drops
# responses mid-stream (IncompleteRead). Same pattern as
# orbitbrief-core-worker (Platform-infra task #19).
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        poppler-utils \
        tesseract-ocr \
        libgl1 \
        libglib2.0-0 \
        ca-certificates \
        curl \
        iptables \
    && curl -fsSL https://tailscale.com/install.sh | sh \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 10001 parserosworker
WORKDIR /build

# Copy parser-os and parser-os-service (worker reuses projector) as siblings.
# SowSmith — separate library, copied as local source so we don't depend on
# git clone at build time (ACR build agent isn't reliably outbound for git).
COPY parser-os ./parser-os
COPY parser-os-service ./parser-os-service
COPY parser-os-worker ./parser-os-worker
COPY SowSmith ./SowSmith

ENV PIP_ROOT_USER_ACTION=ignore

RUN set -eux; \
    pip install --no-cache-dir --upgrade pip setuptools wheel; \
    # Install parser-os from local source so it matches the SHA we cloned
    pip install --no-cache-dir ./parser-os; \
    # Install parser-os-service (need projector — no extra deps)
    pip install --no-cache-dir --no-deps ./parser-os-service; \
    # Belt-and-suspenders: explicit dep list matching orbitbrief-core-worker's
    # set so missing-transitively-required modules can't break us silently.
    pip install --no-cache-dir \
      "azure-identity>=1.15" \
      "azure-storage-blob>=12.19" \
      "azure-storage-queue>=12.10" \
      "azure-ai-documentintelligence>=1.0.0b1" \
      "azure-core>=1.30" \
      "fastapi>=0.110" \
      "pydantic>=2.5" \
      "structlog>=24.1" \
      "PyYAML>=6.0.1" \
      "openpyxl>=3.1.2" \
      "jsonschema>=4.23.0" \
      "httpx>=0.27.0" \
      "requests>=2.31.0" \
      "sqlmodel>=0.0.16" \
      "python-docx>=1.1" \
      "beautifulsoup4>=4.12" \
      "typer>=0.12" \
      "rich>=13.0" \
      "rapidfuzz>=3.0" \
      "numpy>=1.26" \
      "scikit-learn>=1.3" \
      "PyMuPDF>=1.24" \
      "opencv-python-headless>=4.9" \
      "Pillow>=10.0" \
      "pypdfium2>=4.30"; \
    pip install --no-cache-dir ./SowSmith; \
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

# v51: copy the tailscale entrypoint. Mirrors orbitbrief-core-worker.
# The entrypoint joins the tailnet, exports HTTP_PROXY/HTTPS_PROXY so
# urllib (used by parser-os LLM clients) routes through the local
# tailscaled HTTP-CONNECT proxy, then execs the worker.
COPY parser-os-worker/entrypoint.sh /entrypoint.sh
COPY parser-os-worker/fetch_ml.py /fetch_ml.py
RUN chmod +x /entrypoint.sh

# Don't set HTTP_PROXY here: tailscaled must reach controlplane before
# the proxy listens. entrypoint.sh exports them after `tailscale up`.
ENV TS_STATE_DIR=/var/lib/tailscale \
    TS_ACCEPT_DNS=false

# Tailscale needs to write state — run as root inside the container.
# parser-os already runs in a sandboxed Container App so this is fine.
# USER parserosworker  # disabled for tailscaled state writes

ENV PYTHONUNBUFFERED=1

# Entrypoint joins tailnet, then exec the worker.
ENTRYPOINT ["/entrypoint.sh"]
# One process = one message = exit.  Container Apps Jobs + KEDA handle the rest.
CMD ["python", "-m", "parser_os_worker.main"]
