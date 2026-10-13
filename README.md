# parser-os-worker

Queue-driven worker that runs heavy `parser-os` compile jobs offloaded from
`parser-os-service`'s HTTP request path.

## Why

`parser-os-service` exposed `/v1/orbitbrief/rebuild-latest` as a synchronous
HTTP endpoint, but a full v45.2 compile is 8–15 minutes and ~4 GiB peak
memory.  Doing that inside a request:

- Hit Container Apps' 240s ingress timeout on real workloads
- OOMed a single replica → broke the whole endpoint for every subsequent
  caller (`Connection refused`)
- No retry, no progress visibility, no dead-letter

This worker decouples that.  `parser-os-service` enqueues a job message and
returns 202 immediately.  This worker is a Container Apps Job triggered by
KEDA's `azure-queue` scaler — fresh container per message, automatic retry
on failure via queue message visibility, scales to zero when idle.

## Architecture

```
parser-os-service  (HTTP API)
       │
       └─► /v1/compile/async  enqueues message ──┐
                                                 ▼
                                ┌──────────────────────────────┐
                                │ Storage Queue                │
                                │   parser-os-compile-jobs     │
                                └────────────┬─────────────────┘
                                             │ KEDA queue trigger
                                             ▼
                          ┌──────────────────────────────────┐
                          │ parser-os-worker  (this repo)    │
                          │  - 4 GiB / 2 vCPU                │
                          │  - one container per message     │
                          │  - exits cleanly after compile   │
                          │  - max 3 parallel                │
                          └──────────────┬───────────────────┘
                                         │ writes
                                         ▼
                          ┌──────────────────────────────────┐
                          │ Blob: deals/{deal_id}/orbitbrief │
                          │   /latest/envelope.json          │
                          │   /parser-jobs/{compile_id}.json │
                          └──────────────────────────────────┘
```

## Job message shape

```json
{
  "compile_id": "uuid",
  "deal_id": "uuid",
  "manifest_blob_url": "https://...blob.core.windows.net/.../manifest.json",
  "domain_pack": "optional override",
  "compile_options": { "use_cache": false, "allow_errors": true }
}
```

## Status surface

Worker writes `deals/{deal_id}/parser-jobs/{compile_id}.json` as it progresses:

```json
{
  "compile_id": "...",
  "status": "queued|running|completed|failed",
  "started_at": "ISO",
  "updated_at": "ISO",
  "stage": "discover_artifacts|parse_artifacts|enrich_entities|...",
  "percent_complete": 0-100,
  "entity_count": 0,
  "atom_count": 0,
  "error": null
}
```

`parser-os-service` exposes a `GET /v1/compile/status/{compile_id}` that
reads this blob — that's the polling endpoint for the SPA.

## When a compile dies

A compile killed for memory (exit 137) runs no Python, so nothing it could do
on the way out happens. Three things cover it:

- **Restart recovery.** Each process has a boot id, and `_active-compiles/<id>.json`
  records the replica and boot that wrote it. A container restarted in the
  same replica fails its predecessor's runs at startup: compile-progress.json
  goes to `failed / killed` with the stage and item count reached, so the page
  frees Re-parse within seconds. The page's own 5-minute stale rule covers a
  replica that never comes back.
- **Memory guard.** Past `SOWSMITH_MEMORY_GUARD_FRACTION` (default 0.9) of the
  container's cgroup limit, the worker fails the run as `out_of_memory` and exits
  before the kernel kills it. `0` turns it off.
- **No identical re-runs.** A message whose last attempt ended `timeout`,
  `killed` or `out_of_memory` goes to the poison queue instead of running again.
  Every path that gives up on a message also fails the deal's progress document,
  but only while that document still belongs to the same compile.

### Keeping a replica up while a compile runs (needs an infra change)

KEDA's azure-queue scaler counts visible messages, and a running compile's
message is invisible, so the warm worker can be scaled to zero mid-compile.
With `AZURE_STORAGE_QUEUE_INFLIGHT` set, the worker keeps one visible message
per running compile in that queue (TTL `MESSAGE_LEASE_MAX_SEC`) and deletes it
when the compile ends. It only helps once the warm worker also scales on that
queue. Not applied anywhere yet; per environment:

```bash
az containerapp update -n <warm-worker-app> -g <rg> \
  --scale-rule-name inflight-compiles --scale-rule-type azure-queue \
  --scale-rule-metadata queueName=parser-os-compile-inflight queueLength=1 accountName=<storage-account> \
  --scale-rule-auth connection=<storage-connection-secret-name> \
  --set-env-vars AZURE_STORAGE_QUEUE_INFLIGHT=parser-os-compile-inflight
```

Note `az containerapp update --scale-rule-*` replaces the app's scale rules, so
the existing queue rules must be passed again in the same update (or edit the
YAML with `az containerapp show -o yaml` / `update --yaml`).

## Local dev

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e .
# Need a parser-os checkout to install from (the worker imports parser-os).
pip install -e ../parser-os

# Set env to point at dev queue + blob:
export AZURE_STORAGE_ACCOUNT=purpulsedevstg01
export AZURE_STORAGE_QUEUE=parser-os-compile-jobs
export AZURE_STORAGE_BLOB_CONTAINER=orbitbrief-artifacts
export OLLAMA_HOST=http://localhost:11434

# Process one message and exit (Container Apps Job pattern):
python -m parser_os_worker.main
```

## Build + deploy

```bash
# Same pattern as parser-os-service — fresh clones into temp build context,
# --no-cache build, SHA stamping.  See scripts/build_image.sh.

./scripts/build_image.sh dev v45.2

# Update the Container Apps Job to the new image:
az containerapp job update -n parser-os-worker-dev-eus2 -g purtera-dev-rg \
  --image purpulsedevacr.azurecr.io/parser-os-worker@sha256:...
```

## ADR

- One message → one process → exit.  Don't loop.  Container Apps Jobs +
  KEDA handle the scaling and dispatch; we just consume.
- On exception, exit non-zero.  The queue message becomes visible again
  after the visibility-timeout and a new replica picks it up.
- 5 retries max before going to the poison queue (configured at the Job).
