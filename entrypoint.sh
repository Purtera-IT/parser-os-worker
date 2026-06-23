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

# Fetch + unpack the ML head artifacts (gate/facet/type) to ML_ARTIFACT_DIR.
# Runs BEFORE the proxy is set so blob traffic reaches Azure directly. Best-effort:
# on any failure the heads abstain and the compile runs LLM-only (byte-identical).
python -m parser_os_worker.fetch_ml || echo "[entrypoint] fetch_ml failed; heads will abstain" >&2

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

# v57.3.3: --ephemeral is a server-side property of the auth key, not a
# CLI flag here — Tailscale 1.98+ rejects it on ``tailscale up`` and
# dumps USAGE help instead of registering, causing every job to fail.
# The ephemeral property must be set when the AUTH KEY is generated in
# the Tailscale admin console (https://login.tailscale.com/admin/settings/keys
# → Generate auth key → check Ephemeral box). Each new node registered
# with that key inherits the ephemeral status and auto-expires when
# tailscaled exits. Keep the unique hostname per invocation so
# concurrent workers don't collide on the same node slot.
# v57.3.4: hostname max length is 63 bytes in Tailscale. The container's
# own hostname is already ~32 chars (parser-os-worker-dev-eus2-XXXXX-YYYYY),
# and prepending the env-var prefix + appending epoch pushed previous
# attempts to 70+ chars. Use a short prefix + last 8 of epoch only —
# well under 63 chars, still unique enough for concurrent runs.
_UNIQUE_HOST="parser-os-worker-$(date +%s | tail -c 9)"

if [[ -n "${TAILSCALE_UP_EXTRA_ARGS:-}" ]]; then
  # shellcheck disable=SC2086
  tailscale --socket=/tmp/tailscaled.sock up \
    --authkey="${TS_AUTHKEY}" \
    --hostname="${_UNIQUE_HOST}" \
    --accept-dns="${TS_ACCEPT_DNS:-false}" \
    ${TAILSCALE_UP_EXTRA_ARGS}
else
  tailscale --socket=/tmp/tailscaled.sock up \
    --authkey="${TS_AUTHKEY}" \
    --hostname="${_UNIQUE_HOST}" \
    --accept-dns="${TS_ACCEPT_DNS:-false}"
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
