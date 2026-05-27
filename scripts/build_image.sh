#!/usr/bin/env bash
# Build the parser-os-worker image with proper SHA stamping + cache-busting.
#
# Mirrors parser-os-service/scripts/build_image.sh so the deploy story is
# identical for both.
#
# Usage:
#   PUSH=true scripts/build_image.sh dev v45.2

set -euo pipefail

ENV_TAG="${1:?usage: $0 <env-tag (e.g. dev)> <parser-os-ref (e.g. v45.2)>}"
PARSER_OS_REF="${2:?usage: $0 <env-tag> <parser-os-ref>}"

PARSER_OS_REPO="${PARSER_OS_REPO:-https://github.com/Purtera-IT/parser-os.git}"
PARSER_OS_SERVICE_REPO="${PARSER_OS_SERVICE_REPO:-https://github.com/Purtera-IT/parser-os-service.git}"
PARSER_OS_SERVICE_REF="${PARSER_OS_SERVICE_REF:-main}"
ACR_REGISTRY="${ACR_REGISTRY:-purpulsedevacr.azurecr.io}"
IMAGE_NAME="${IMAGE_NAME:-parser-os-worker}"
PUSH="${PUSH:-false}"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# 1. Resolve parser-os ref to a concrete SHA
echo "→ resolving parser-os ref '$PARSER_OS_REF'..."
RESOLVED_SHA=$(
  GIT_TERMINAL_PROMPT=0 git ls-remote "$PARSER_OS_REPO" "$PARSER_OS_REF" \
    | awk '{print $1}' | head -n 1
)
if [[ -z "$RESOLVED_SHA" ]]; then
  if [[ "$PARSER_OS_REF" =~ ^[a-f0-9]{7,40}$ ]]; then
    RESOLVED_SHA="$PARSER_OS_REF"
  else
    echo "✗ FAIL — could not resolve '$PARSER_OS_REF'" >&2
    exit 1
  fi
fi
echo "  parser-os @ $RESOLVED_SHA"

# 2. Build context with all 3 repos as siblings
BUILD_CTX=$(mktemp -d)
trap "rm -rf $BUILD_CTX" EXIT

GIT_TERMINAL_PROMPT=0 git clone --depth 1 --branch "$PARSER_OS_REF" \
  "$PARSER_OS_REPO" "$BUILD_CTX/parser-os" 2>/dev/null \
  || git clone "$PARSER_OS_REPO" "$BUILD_CTX/parser-os"
(cd "$BUILD_CTX/parser-os" && git checkout "$RESOLVED_SHA")

GIT_TERMINAL_PROMPT=0 git clone --depth 1 --branch "$PARSER_OS_SERVICE_REF" \
  "$PARSER_OS_SERVICE_REPO" "$BUILD_CTX/parser-os-service" 2>/dev/null \
  || git clone "$PARSER_OS_SERVICE_REPO" "$BUILD_CTX/parser-os-service"

cp -r "$REPO_ROOT" "$BUILD_CTX/parser-os-worker"
rm -rf "$BUILD_CTX/parser-os-worker/.git" "$BUILD_CTX/parser-os-worker/.venv"

WORKER_SHA=$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo "unknown")
if [[ "$PARSER_OS_REF" =~ ^v[0-9]+(\.[0-9]+){1,2}$ ]]; then
  BUILD_LABEL="$PARSER_OS_REF"
else
  BUILD_LABEL="${RESOLVED_SHA:0:7}"
fi

IMAGE_TAG="${ACR_REGISTRY}/${IMAGE_NAME}:${ENV_TAG}"

echo "→ docker build $IMAGE_TAG"
DOCKER_BUILDKIT=1 docker build \
  --no-cache \
  --pull \
  --build-arg "GIT_SHA=$WORKER_SHA" \
  --build-arg "PARSER_OS_SHA=$RESOLVED_SHA" \
  --build-arg "BUILD_LABEL=$BUILD_LABEL" \
  --label "org.opencontainers.image.revision=$WORKER_SHA" \
  --label "org.opencontainers.image.version=$BUILD_LABEL" \
  -f "$BUILD_CTX/parser-os-worker/Dockerfile" \
  -t "$IMAGE_TAG" \
  "$BUILD_CTX"

echo "✓ Built $IMAGE_TAG"

if [[ "$PUSH" == "true" ]]; then
  echo "→ pushing to $ACR_REGISTRY"
  docker push "$IMAGE_TAG"
  echo "✓ pushed"
fi
