"""parser-os-worker entry point.

Container Apps Job pattern: each replica runs this module once, processes
exactly ONE queue message, then exits.  KEDA's azure-queue scaler launches
new replicas based on queue depth, so concurrency = number of in-flight
messages (capped at maxReplicas).

Exit codes:
  0   message processed (or queue empty)
  1   transient failure — message will reappear after visibility timeout
  2   poison message — drop without retry (rare)
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import sys
import tempfile
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueServiceClient, TextBase64EncodePolicy

# ─── Configuration ─────────────────────────────────────────────────────────

ACCOUNT_NAME = os.environ.get("AZURE_STORAGE_ACCOUNT", "purpulsedevstg01")
QUEUE_NAME = os.environ.get("AZURE_STORAGE_QUEUE", "parser-os-compile-jobs")
BLOB_CONTAINER = os.environ.get("AZURE_STORAGE_BLOB_CONTAINER", "orbitbrief-artifacts")
COMPILE_TIMEOUT_SEC = int(os.environ.get("COMPILE_TIMEOUT_SEC", "1500"))  # 25 min hard
VISIBILITY_TIMEOUT_SEC = int(os.environ.get("MESSAGE_VISIBILITY_TIMEOUT_SEC", "1800"))  # 30 min
MAX_DEQUEUE_COUNT = int(os.environ.get("MAX_DEQUEUE_COUNT", "3"))  # poison after 3 retries
WORKER_SHA = os.environ.get("PARSER_OS_WORKER_SHA", "unknown")
PARSER_OS_SHA = os.environ.get("PARSER_OS_SHA", "unknown")
# v45.2: queue to notify brief-gen worker that a fresh envelope is ready.
# Empty string disables (worker won't enqueue; useful for ad-hoc replays).
BRIEF_GEN_QUEUE_NAME = os.environ.get(
    "BRIEF_GEN_QUEUE_NAME", "parser-os-orbitbrief-jobs"
)
# Prefer connection string (avoids needing Storage Data Contributor role on the
# managed identity).  Falls back to DefaultAzureCredential when not set.
CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING") or \
    os.environ.get("ORBITBRIEF_ARTIFACTS_CONNECTION_STRING")


# ─── Logging setup ─────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("parser-os-worker")


# ─── Helpers ───────────────────────────────────────────────────────────────


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _blob_path_from_url(blob_url: str) -> tuple[str, str]:
    """Parse 'https://<acct>.blob.core.windows.net/<container>/<path>' →
    (container, path)."""
    p = urlparse(blob_url)
    parts = p.path.lstrip("/").split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Cannot parse blob url: {blob_url}")
    return parts[0], parts[1]


@dataclass
class JobMessage:
    compile_id: str
    deal_id: str
    manifest_blob_url: str
    domain_pack: str | None = None
    compile_options: dict[str, Any] | None = None

    @classmethod
    def from_raw(cls, raw: str) -> "JobMessage":
        d = json.loads(raw)
        return cls(
            compile_id=str(d["compile_id"]),
            deal_id=str(d["deal_id"]),
            manifest_blob_url=str(d["manifest_blob_url"]),
            domain_pack=d.get("domain_pack"),
            compile_options=d.get("compile_options") or {},
        )


# ─── Status surface (written to blob during compile) ──────────────────────


def _status_blob_path(job: JobMessage) -> str:
    return f"deals/{job.deal_id}/parser-jobs/{job.compile_id}.json"


def _write_status(
    blob_service: BlobServiceClient,
    job: JobMessage,
    status: str,
    **extra: Any,
) -> None:
    payload = {
        "compile_id": job.compile_id,
        "deal_id": job.deal_id,
        "status": status,
        "updated_at": _iso_now(),
        "worker_sha": WORKER_SHA,
        "parser_os_sha": PARSER_OS_SHA,
        **extra,
    }
    try:
        blob_client = blob_service.get_blob_client(
            container=BLOB_CONTAINER, blob=_status_blob_path(job)
        )
        blob_client.upload_blob(
            json.dumps(payload, indent=2),
            overwrite=True,
            content_type="application/json",
        )
    except Exception as exc:  # never let status writes kill the worker
        log.warning("Failed to write status blob: %s", exc)


# ─── Manifest + envelope read/write ───────────────────────────────────────


def _download_manifest(blob_service: BlobServiceClient, blob_url: str) -> dict[str, Any]:
    container, path = _blob_path_from_url(blob_url)
    client = blob_service.get_blob_client(container=container, blob=path)
    data = client.download_blob().readall()
    return json.loads(data)


def _upload_envelope(
    blob_service: BlobServiceClient, deal_id: str, envelope: dict[str, Any]
) -> str:
    path = f"deals/{deal_id}/orbitbrief/latest/envelope.json"
    client = blob_service.get_blob_client(container=BLOB_CONTAINER, blob=path)
    client.upload_blob(
        json.dumps(envelope, indent=2, ensure_ascii=False),
        overwrite=True,
        content_type="application/json",
    )
    return path


def _enqueue_brief_gen(
    queue_service: QueueServiceClient, job: "JobMessage"
) -> None:
    """Notify the brief-gen queue (parser-os-orbitbrief-jobs) that a fresh
    v45.2 envelope has been written.  Function App queueTrigger picks this up
    with envelopeReady:true and skips its inline regex-only rebuild step,
    going straight to brief-gen against the fresh envelope.

    Message format matches what the Function App enqueueJson helper writes:
    base64-encoded JSON, fields dealId + compileId + envelopeReady.
    """
    if not BRIEF_GEN_QUEUE_NAME:
        log.info("BRIEF_GEN_QUEUE_NAME unset; skipping brief-gen enqueue.")
        return
    bqc = queue_service.get_queue_client(
        BRIEF_GEN_QUEUE_NAME,
        message_encode_policy=TextBase64EncodePolicy(),
    )
    payload = {
        "dealId": job.deal_id,
        "compileId": job.compile_id,
        "envelopeReady": True,
    }
    bqc.send_message(json.dumps(payload))
    log.info(
        "Enqueued brief-gen for deal=%s compile=%s on %s",
        job.deal_id,
        job.compile_id,
        BRIEF_GEN_QUEUE_NAME,
    )


# ─── The actual compile ───────────────────────────────────────────────────


def _do_compile(
    job: JobMessage,
    manifest: dict[str, Any],
    blob_service: BlobServiceClient,
) -> dict[str, Any]:
    """Run parser-os compile_project, write envelope.json + scope-process-v1
    sidecar + SOW_DRAFT.md + compile-trace.json, return summary.

    Mirrors parser-os-service /v1/orbitbrief/rebuild-latest in shape (uses the
    canonical build_orbitbrief_envelope for envelope.json — to_scope_process_v1
    is for DB persistence, NOT for the envelope blob), plus the v45.2 split-out
    SowSmith render + structured compile trace.
    """
    from app.core.compiler import compile_project  # parser-os (installed via pyproject)
    from app.core.orbitbrief_envelope import build_orbitbrief_envelope  # type: ignore
    from parser_os_service.server.projector import to_scope_process_v1  # type: ignore
    import app.core.multi_entity_llm as _llm_mod1  # type: ignore
    import app.core.site_llm_verify as _llm_mod2  # type: ignore

    t_start = time.time()
    started_at = _iso_now()
    work_root = Path(tempfile.mkdtemp(prefix=f"parser-os-worker-{job.compile_id}-"))
    project_dir = work_root / "project"
    project_dir.mkdir(parents=True, exist_ok=True)

    # Compile trace: monkey-patch _call_ollama in both call sites so we record
    # every LLM HTTP call (model, sizes, latency) for byte-identical diff vs
    # Mac local.  Restored in finally to avoid cross-invocation leakage.
    llm_calls: list[dict[str, Any]] = []

    def _make_wrapper(original, source: str):
        def wrapped(prompt: str, *, max_tokens: int = 1024) -> str:
            t0 = time.time()
            response = original(prompt, max_tokens=max_tokens)
            llm_calls.append({
                "ts": _iso_now(),
                "source": source,
                "prompt_size": len(prompt),
                "max_tokens": max_tokens,
                "response_size": len(response or ""),
                "latency_sec": round(time.time() - t0, 3),
                "model": os.environ.get("OLLAMA_MODEL", "qwen3:14b"),
            })
            return response
        return wrapped

    _orig_call_1 = _llm_mod1._call_ollama
    _orig_call_2 = _llm_mod2._call_ollama
    _llm_mod1._call_ollama = _make_wrapper(_orig_call_1, "multi_entity_llm")
    _llm_mod2._call_ollama = _make_wrapper(_orig_call_2, "site_llm_verify")

    try:
        # 1. Download artifacts referenced by manifest into project_dir.
        artifacts = manifest.get("artifacts") or []
        log.info("Downloading %d artifacts to %s", len(artifacts), project_dir)
        for a in artifacts:
            blob_url = a.get("blob_url") or a.get("url")
            rel_path = a.get("filename") or a.get("path") or a.get("name")
            if not blob_url or not rel_path:
                log.warning("Skipping artifact without blob_url/filename: %s", a)
                continue
            dest = project_dir / rel_path.lstrip("/").lstrip("\\")
            dest.parent.mkdir(parents=True, exist_ok=True)
            container, blob_path = _blob_path_from_url(blob_url)
            client = blob_service.get_blob_client(container=container, blob=blob_path)
            with open(dest, "wb") as fh:
                downloader = client.download_blob()
                fh.write(downloader.readall())

        # 2. Resolve compile options
        opts = dict(job.compile_options or {})
        domain_pack = (
            job.domain_pack
            or (manifest.get("context") or {}).get("domain_pack")
            or manifest.get("domain_pack")
        )
        _write_status(blob_service, job, "running", stage="compile", percent_complete=10)

        # 3. Compile
        log.info("Starting compile_project (compile_id=%s)", job.compile_id)
        t0 = time.time()
        result = compile_project(
            project_dir=project_dir,
            project_id=job.deal_id,
            domain_pack=domain_pack,
            allow_errors=opts.get("allow_errors", True),
            allow_unverified_receipts=opts.get("allow_unverified_receipts", True),
            use_cache=opts.get("use_cache", False),  # default False to honor fresh-compile intent
            abstain_threshold=opts.get("abstain_threshold"),
            persistence_hook=None,
        )
        elapsed = time.time() - t0
        log.info("compile_project done in %.1fs", elapsed)

        # 4a. Build canonical OrbitBrief envelope (atoms/entities/cockpit
        # surfaces).  THIS is what brief gen and SowSmith read.
        _write_status(
            blob_service, job, "running",
            stage="projection", percent_complete=85,
            entity_count=len(result.entities),
            atom_count=len(result.atoms),
        )
        envelope = build_orbitbrief_envelope(
            project_dir=project_dir, compile_result=result,
        )
        envelope["compile_id"] = job.compile_id

        # 4b. Also produce scope_process_v1 (DB persistence shape — different
        # from envelope).  Uploaded as a sidecar so the Function App queue
        # trigger can persist it to opportunities.quote_data.scope_process_v1.
        scope_process_v1 = to_scope_process_v1(
            result, manifest=manifest, manifest_blob_url=job.manifest_blob_url,
        )

        # 5a. Upload envelope.json
        env_path = _upload_envelope(blob_service, job.deal_id, envelope)
        log.info("Uploaded envelope to %s", env_path)

        # 5b. Upload scope-process-v1.json sidecar
        try:
            scope_path = f"deals/{job.deal_id}/orbitbrief/latest/scope-process-v1.json"
            blob_service.get_blob_client(
                container=BLOB_CONTAINER, blob=scope_path,
            ).upload_blob(
                json.dumps(scope_process_v1, indent=2, ensure_ascii=False).encode("utf-8"),
                overwrite=True,
                content_type="application/json",
            )
            log.info("Uploaded scope-process-v1 sidecar to %s", scope_path)
        except Exception as exc:
            log.warning("scope-process-v1 sidecar upload failed: %s", exc)

        # 5c. SowSmith — deterministic SOW render from envelope.  Sub-second,
        # no LLM, no dependencies.  Decoupled from Orbitbrief-Core brief gen.
        sow_version: str | None = None
        try:
            from sowsmith import build_sow_markdown, SOW_VERSION  # type: ignore
            sow_md = build_sow_markdown(envelope)
            sow_path = f"deals/{job.deal_id}/orbitbrief/latest/SOW_DRAFT.md"
            blob_service.get_blob_client(
                container=BLOB_CONTAINER, blob=sow_path,
            ).upload_blob(
                sow_md.encode("utf-8"),
                overwrite=True,
                content_type="text/markdown; charset=utf-8",
            )
            sow_version = SOW_VERSION
            log.info(
                "SowSmith %s wrote SOW_DRAFT.md (%d chars) for deal=%s",
                SOW_VERSION, len(sow_md), job.deal_id,
            )
        except Exception as exc:
            log.warning("SowSmith SOW render failed: %s", exc)

        # 5d. Compile trace — stage timeline + every LLM call.  For
        # byte-identical-pipeline verification against Mac local: same stage
        # names, same atom/entity counts, same LLM call counts per source/model
        # → same pipeline ran.  Latencies will differ (Tailscale overhead).
        try:
            from dataclasses import asdict as _asdict, is_dataclass as _is_dc
            parser_trace = None
            if hasattr(result, "trace") and result.trace is not None:
                if _is_dc(result.trace):
                    parser_trace = _asdict(result.trace)
                elif hasattr(result.trace, "__dict__"):
                    parser_trace = dict(result.trace.__dict__)

            by_source: dict[str, int] = {}
            by_model: dict[str, int] = {}
            total_latency = 0.0
            for c in llm_calls:
                by_source[c["source"]] = by_source.get(c["source"], 0) + 1
                by_model[c["model"]] = by_model.get(c["model"], 0) + 1
                total_latency += c["latency_sec"]

            trace_payload = {
                "compile_id": job.compile_id,
                "deal_id": job.deal_id,
                "worker_sha": WORKER_SHA,
                "parser_os_sha": PARSER_OS_SHA,
                "sow_version": sow_version,
                "started_at": started_at,
                "finished_at": _iso_now(),
                "wall_clock_sec": round(time.time() - t_start, 3),
                "compile_elapsed_sec": round(elapsed, 3),
                "entity_count": len(getattr(result, "entities", []) or []),
                "atom_count": len(getattr(result, "atoms", []) or []),
                "llm_calls": llm_calls,
                "llm_call_summary": {
                    "total_calls": len(llm_calls),
                    "total_latency_sec": round(total_latency, 3),
                    "by_source": by_source,
                    "by_model": by_model,
                },
                "parser_trace": parser_trace,
            }
            trace_path = f"deals/{job.deal_id}/orbitbrief/latest/compile-trace.json"
            blob_service.get_blob_client(
                container=BLOB_CONTAINER, blob=trace_path,
            ).upload_blob(
                json.dumps(trace_payload, indent=2, default=str).encode("utf-8"),
                overwrite=True,
                content_type="application/json",
            )
            log.info(
                "Wrote compile-trace.json (%d LLM calls, %.1fs total LLM latency)",
                len(llm_calls), total_latency,
            )
        except Exception as exc:
            log.warning("compile-trace upload failed: %s", exc)

        return {
            "envelope_path": env_path,
            "entity_count": len(result.entities),
            "atom_count": len(result.atoms),
            "elapsed_sec": elapsed,
            "sow_version": sow_version,
            "llm_call_count": len(llm_calls),
        }
    finally:
        # Restore monkey-patched call sites and clean tempdir.
        try:
            _llm_mod1._call_ollama = _orig_call_1
            _llm_mod2._call_ollama = _orig_call_2
        except Exception:
            pass
        shutil.rmtree(work_root, ignore_errors=True)


# ─── Main loop (well — main one-shot) ──────────────────────────────────────


def main() -> int:
    log.info(
        "parser-os-worker starting (worker_sha=%s parser_os_sha=%s)",
        WORKER_SHA,
        PARSER_OS_SHA,
    )

    if CONNECTION_STRING:
        log.info("Using connection-string auth for Storage.")
        queue_service = QueueServiceClient.from_connection_string(CONNECTION_STRING)
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    else:
        log.info("No AZURE_STORAGE_CONNECTION_STRING; falling back to DefaultAzureCredential.")
        cred = DefaultAzureCredential()
        queue_service = QueueServiceClient(
            account_url=f"https://{ACCOUNT_NAME}.queue.core.windows.net",
            credential=cred,
        )
        blob_service = BlobServiceClient(
            account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
            credential=cred,
        )
    queue_client = queue_service.get_queue_client(QUEUE_NAME)

    # Pull ONE message (Container Apps Jobs pattern: one process = one unit of work)
    log.info("Dequeuing one message from %s ...", QUEUE_NAME)
    messages = list(
        queue_client.receive_messages(
            visibility_timeout=VISIBILITY_TIMEOUT_SEC,
            messages_per_page=1,
        )
    )

    if not messages:
        log.info("Queue empty; nothing to do.")
        return 0
    msg = messages[0]

    log.info("Got message (dequeue_count=%d, id=%s)", msg.dequeue_count, msg.id)

    # Decode
    try:
        # Storage Queue messages may be base64-encoded; the SDK handles that.
        raw = msg.content
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        job = JobMessage.from_raw(raw)
    except Exception as exc:
        log.exception("Poison message — cannot decode: %s", exc)
        # Drop it so we don't retry forever
        queue_client.delete_message(msg)
        return 2

    # Poison guard: too many retries
    if msg.dequeue_count > MAX_DEQUEUE_COUNT:
        log.error(
            "Message exceeded MAX_DEQUEUE_COUNT=%d (this dequeue %d), dropping.",
            MAX_DEQUEUE_COUNT,
            msg.dequeue_count,
        )
        _write_status(
            blob_service, job, "failed",
            stage="exhausted_retries",
            error=f"dequeue_count {msg.dequeue_count} > {MAX_DEQUEUE_COUNT}",
        )
        queue_client.delete_message(msg)
        return 2

    # Mark running
    _write_status(blob_service, job, "running", stage="starting", percent_complete=0)

    # Do the work
    try:
        log.info("Downloading manifest %s", job.manifest_blob_url)
        manifest = _download_manifest(blob_service, job.manifest_blob_url)
        result = _do_compile(job, manifest, blob_service)

        _write_status(
            blob_service, job, "completed",
            stage="done",
            percent_complete=100,
            entity_count=result["entity_count"],
            atom_count=result["atom_count"],
            elapsed_sec=round(result["elapsed_sec"], 2),
            envelope_path=result["envelope_path"],
        )

        # Notify brief-gen worker that a fresh v45.2 envelope is ready.
        # Best-effort — log loudly on failure but don't fail the compile job.
        try:
            _enqueue_brief_gen(queue_service, job)
        except Exception as exc:
            log.warning(
                "Brief-gen enqueue failed for deal=%s compile=%s: %s",
                job.deal_id, job.compile_id, exc,
            )

        queue_client.delete_message(msg)
        log.info("Job complete: compile_id=%s", job.compile_id)
        return 0

    except Exception as exc:
        log.exception("Compile failed: %s", exc)
        _write_status(
            blob_service, job, "failed",
            stage="exception",
            error=f"{type(exc).__name__}: {exc}",
            traceback=traceback.format_exc()[:4000],
        )
        # Don't delete the message — it'll reappear after visibility_timeout for retry
        return 1


if __name__ == "__main__":
    sys.exit(main())
