#!/usr/bin/env bash
# v51 — parser-os-worker entrypoint with Tailscale userspace networking.
#
# Mirrors orbitbrief-core-worker (Platform-infra task #19). Brings
# tailscaled up in userspace mode so the worker reaches Mac Studio Ollama
# DIRECTLY at the Tailscale IP (100.114.102.122) — bypasses the HTTPS
# proxy that drops responses mid-stream.
#
# parser-os uses stdlib urllib for LLM calls, which doesn't speak SOCKS5.
# So tailscaled is started in HTTP-CONNECT proxy mode
# (--outbound-http-proxy-listen) and HTTP_PROXY/HTTPS_PROXY are exported
# so urllib forwards through it.

set -euo pipefail

# tailscaled must reach controlplane without any proxy env interfering.
unset ALL_PROXY all_proxy HTTP_PROXY HTTPS_PROXY http_proxy https_proxy

mkdir -p "${TS_STATE_DIR:-/var/lib/tailscale}"

tailscaled --tun=userspace-networking \
  --state="${TS_STATE_DIR:-/var/lib/tailscale}/tailscaled.state" \
  --socket=/tmp/tailscaled.sock \
  --outbound-http-proxy-listen="${TS_HTTP_PROXY_LISTEN:-localhost:1055}" \
  --socks5-server="${TS_SOCKS5_SERVER:-localhost:1055}" &

until [[ -S /tmp/tailscaled.sock ]]; do
  sleep 0.3
done
sleep 0.5

if [[ -z "${TS_AUTHKEY:-}" ]]; then
  echo "TS_AUTHKEY is required — falling back to proxy (no Tailscale)" >&2
  # Don't fail; allow the worker to run via the existing proxy if auth
  # missing. Once TS_AUTHKEY is added as a secret env var this exits
  # the fallback and uses the tailnet.
  exec "$@"
fi

# v57.3.2: each Container App Job invocation is short-lived (≤30 min for
# a full parser-os compile). Without --ephemeral, every invocation
# registers a permanent Tailscale node — after a few hundred runs we hit
# the tailnet's node quota and new workers fail with "node quota reached
# on this tailnet". --ephemeral marks the node as auto-expiring: when
# tailscaled exits at job-end, the controlplane cleans up the node
# entry automatically (no admin cleanup required). Also unique hostname
# per invocation so concurrent workers don't fight for the same node
# slot (hostname collisions force-replace the previous node).
_UNIQUE_HOST="${TAILSCALE_HOSTNAME:-parser-os-worker}-$(hostname 2>/dev/null || echo unknown)-$(date +%s)"

if [[ -n "${TAILSCALE_UP_EXTRA_ARGS:-}" ]]; then
  # shellcheck disable=SC2086
  tailscale --socket=/tmp/tailscaled.sock up \
    --authkey="${TS_AUTHKEY}" \
    --hostname="${_UNIQUE_HOST}" \
    --accept-dns="${TS_ACCEPT_DNS:-false}" \
    --ephemeral \
    ${TAILSCALE_UP_EXTRA_ARGS}
else
  tailscale --socket=/tmp/tailscaled.sock up \
    --authkey="${TS_AUTHKEY}" \
    --hostname="${_UNIQUE_HOST}" \
    --accept-dns="${TS_ACCEPT_DNS:-false}" \
    --ephemeral
fi

# urllib reads HTTP_PROXY / HTTPS_PROXY for forward + CONNECT proxying.
# Tailscale's HTTP proxy resolves MagicDNS names internally so we don't
# need accept-dns on the host. After this export, ALL parser-os LLM calls
# route through the local tailscaled proxy → direct Tailscale → Mac Studio.
export HTTP_PROXY="http://127.0.0.1:${TS_HTTP_PROXY_PORT:-1055}"
export HTTPS_PROXY="http://127.0.0.1:${TS_HTTP_PROXY_PORT:-1055}"
export http_proxy="$HTTP_PROXY"
export https_proxy="$HTTPS_PROXY"
# Keep ALL_PROXY in case anything else (httpx, requests) is in the path.
export ALL_PROXY="socks5h://127.0.0.1:${TS_SOCKS5_PORT:-1055}"
# Don't proxy local + Azure metadata + queue/blob endpoints (those reach
# Azure directly, not through Mac).
export NO_PROXY="localhost,127.0.0.1,169.254.169.254,.internal.cloudapp.net,.windows.net,.azure.com,.azurewebsites.net,.azurecr.io"
export no_proxy="$NO_PROXY"

# v51: override OLLAMA_HOST to the Mac's Tailscale IP so the proxy
# forwards through the tailnet instead of trying the legacy HTTPS
# proxy URL.
export OLLAMA_HOST="${OLLAMA_HOST_TAILSCALE:-http://100.114.102.122:11434}"

exec "$@"
