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
# v59: interactive re-parses (UI Re-parse button → Function App → /v1/compile/async,
# which defaults priority=true) land on this PRIORITY queue. The worker drains it
# BEFORE the normal queue so a user's click never waits behind a bulk/batch backlog
# (the "40 deals queued, my reparse hangs" problem). Bulk callers pass priority=false.
PRIORITY_QUEUE_NAME = os.environ.get(
    "AZURE_STORAGE_QUEUE_PRIORITY", "parser-os-compile-jobs-priority"
)
BLOB_CONTAINER = os.environ.get("AZURE_STORAGE_BLOB_CONTAINER", "orbitbrief-artifacts")
COMPILE_TIMEOUT_SEC = int(os.environ.get("COMPILE_TIMEOUT_SEC", "1500"))  # 25 min hard
VISIBILITY_TIMEOUT_SEC = int(os.environ.get("MESSAGE_VISIBILITY_TIMEOUT_SEC", "1800"))  # 30 min
MAX_DEQUEUE_COUNT = int(os.environ.get("MAX_DEQUEUE_COUNT", "3"))  # poison after 3 retries
# Dev skip-list: deal_ids this worker ack-and-drops without compiling. Used to keep
# a giant deal (e.g. a 20k-atom deal that monopolizes the single LLM) off the dev
# worker so interactive reparses always get the slot. Comma-separated; reversible.
SKIP_DEAL_IDS = {d.strip() for d in os.environ.get("SOWSMITH_WORKER_SKIP_DEALS", "").split(",") if d.strip()}
# Post-compile cross-run retrain holds the slot (embeds via Ollama; can hang under
# contention). Default ON; set 0 in dev so executions free the slot immediately.
RETRAIN_ENABLED = os.environ.get("SOWSMITH_WORKER_RETRAIN", "1").strip().lower() not in ("0", "false", "no")
WORKER_SHA = os.environ.get("PARSER_OS_WORKER_SHA", "unknown")
PARSER_OS_SHA = os.environ.get("PARSER_OS_SHA", "unknown")
# v45.2: queue to notify brief-gen / Function App that a fresh envelope is ready.
# Set AZURE_STORAGE_QUEUE_BRIEF_GEN="" to disable enqueue (HTTP trigger still runs).
# Previously this constant was referenced but never defined → NameError on every
# compile, so auto OrbitBrief never queued.
BRIEF_GEN_QUEUE_NAME = os.environ.get(
    "AZURE_STORAGE_QUEUE_BRIEF_GEN", "parser-os-orbitbrief-jobs"
)
POISON_QUEUE_NAME = os.environ.get(
    "AZURE_STORAGE_QUEUE_POISON", "parser-os-compile-jobs-poison"
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


def _is_benign_queue_delete_error(exc: BaseException) -> bool:
    name = type(exc).__name__
    if "ResourceNotFound" in name or "MessageNotFound" in name:
        return True
    msg = str(exc).lower()
    return "messagenotfound" in msg or "specified message does not exist" in msg


def _safe_delete_queue_message(queue_client: Any, msg: Any, *, context: str = "") -> None:
    """Delete queue message; visibility-timeout races are benign after success."""
    try:
        queue_client.delete_message(msg)
    except Exception as exc:
        if _is_benign_queue_delete_error(exc):
            log.warning(
                "Queue message already gone after %s (benign ack race): %s",
                context or "processing",
                exc,
            )
            return
        raise


def _blob_path_from_url(blob_url: str) -> tuple[str, str]:
    """Parse 'https://<acct>.blob.core.windows.net/<container>/<path>' →
    (container, path).

    v57.6 fix: URL-decode the path so blob keys containing spaces, parens,
    and other percent-encoded chars (``SC%20AP%20pSOW%20(4.27.26).docx`` →
    ``SC AP pSOW (4.27.26).docx``) are looked up correctly. Without the
    decode every artifact whose filename has a space → BlobNotFound at
    download time.
    """
    from urllib.parse import unquote
    p = urlparse(blob_url)
    parts = p.path.lstrip("/").split("/", 1)
    if len(parts) != 2:
        raise ValueError(f"Cannot parse blob url: {blob_url}")
    return unquote(parts[0]), unquote(parts[1])


def _parse_queue_json(raw: str) -> dict[str, Any]:
    """Accept plain JSON or base64-wrapped JSON.

    parser-os-service enqueues plain JSON. Some SDK producers use
    TextBase64EncodePolicy; without a matching decode policy the worker
    would poison-drop those messages. Try JSON first, then base64→JSON.
    """
    import base64

    text = (raw or "").strip()
    try:
        out = json.loads(text)
        if isinstance(out, dict):
            return out
    except json.JSONDecodeError:
        pass
    try:
        decoded = base64.b64decode(text, validate=False).decode("utf-8")
        out = json.loads(decoded)
        if isinstance(out, dict):
            return out
    except Exception as exc:
        raise ValueError(
            f"Queue message is neither JSON nor base64 JSON ({type(exc).__name__}: {exc})"
        ) from exc
    raise ValueError("Queue message decoded but was not a JSON object")


@dataclass
class JobMessage:
    compile_id: str
    deal_id: str
    manifest_blob_url: str
    domain_pack: str | None = None
    compile_options: dict[str, Any] | None = None
    # v61: when true, bypass worker-side change-detection (always compile). Read
    # from a top-level `force` or compile_options.force so any caller can request
    # a forced re-parse; the timer-driven bulk floods never set it -> get deduped.
    force: bool = False

    @classmethod
    def from_raw(cls, raw: str) -> "JobMessage":
        d = _parse_queue_json(raw)
        opts = d.get("compile_options") or {}
        return cls(
            compile_id=str(d["compile_id"]),
            deal_id=str(d["deal_id"]),
            manifest_blob_url=str(d["manifest_blob_url"]),
            domain_pack=d.get("domain_pack"),
            compile_options=opts,
            force=bool(d.get("force") or opts.get("force")),
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


def _unchanged_since_last_compile(
    blob_service: BlobServiceClient, deal_id: str, manifest: dict[str, Any]
) -> bool:
    """v62: True when this compile would be REDUNDANT — the deal's DOCUMENTS are
    byte-identical to the last successful compile (deals/<id>/orbitbrief/latest/
    compile-idempotency.json). Product rule: parser + brief run ONLY when a deal is
    NEW (no fingerprint -> returns False -> compile) or a document was added/changed
    (artifact hashes differ -> compile). They do NOT re-run on a code deploy or on a
    repeat trigger of an unchanged deal — which is what kills the timer-driven bulk
    floods (hubspot-sync / orbitbrief-runs re-compiling unchanged deals every cycle).
    force=true bypasses (re-validate everything after a parser change). Fails CLOSED
    (returns False -> compile) on any error."""
    try:
        import hashlib
        shas = sorted(
            str(a.get("content_sha256") or "")
            for a in (manifest.get("artifacts") or [])
        )
        artifact_key = hashlib.sha256("\n".join(shas).encode("utf-8")).hexdigest()
        rec = json.loads(
            blob_service.get_blob_client(
                container=BLOB_CONTAINER,
                blob=f"deals/{deal_id}/orbitbrief/latest/compile-idempotency.json",
            ).download_blob().readall()
        )
        # Documents-only: NOT keyed on parser_os_sha/worker_sha, so an unchanged
        # deal never re-runs just because code shipped.
        return rec.get("artifact_key") == artifact_key
    except Exception:
        return False


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


def _forward_to_poison_queue(
    queue_service: QueueServiceClient,
    *,
    raw_message: str,
    reason: str,
    job: "JobMessage | None" = None,
    dequeue_count: int = 0,
    source_queue: str = "",
) -> None:
    """Archive poison / exhausted messages for ops replay instead of silent drop."""
    if not POISON_QUEUE_NAME:
        return
    try:
        pqc = queue_service.get_queue_client(
            POISON_QUEUE_NAME,
            message_encode_policy=TextBase64EncodePolicy(),
        )
        pqc.create_queue()
        doc = {
            "reason": reason,
            "dequeue_count": dequeue_count,
            "source_queue": source_queue,
            "forwarded_at": _iso_now(),
            "original": raw_message,
        }
        if job is not None:
            doc["deal_id"] = job.deal_id
            doc["compile_id"] = job.compile_id
        pqc.send_message(json.dumps(doc))
        log.info("Forwarded poison message to %s (reason=%s)", POISON_QUEUE_NAME, reason)
    except Exception as exc:
        log.warning("Failed to forward poison message to %s: %s", POISON_QUEUE_NAME, exc)


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
    try:
        bqc.send_message(json.dumps(payload))
    except Exception:
        # Queue missing in a fresh env — create once and retry.
        try:
            queue_service.create_queue(BRIEF_GEN_QUEUE_NAME)
        except Exception:
            pass
        bqc.send_message(json.dumps(payload))
    log.info(
        "Enqueued brief-gen for deal=%s compile=%s on %s",
        job.deal_id,
        job.compile_id,
        BRIEF_GEN_QUEUE_NAME,
    )


def _trigger_brief_gen_http(job: "JobMessage") -> None:
    """Kick brief-gen (PM_HANDOFF etc.) on orbitbrief-core-worker right after the
    parse, so the OrbitBrief page is fresh on EVERY run.

    Queue path (parser-os-orbitbrief-jobs) is the primary notify; this HTTP call
    is the live backup when the Function App consumer is slow/absent.
    Best-effort: any failure just leaves the brief stale; it never fails compile.

    NOTE: the worker routes HTTP through the Tailscale proxy (for Ollama), but
    orbitbrief-core-worker is a public Azure ingress URL — so we use an opener with
    an EMPTY ProxyHandler to connect DIRECT, bypassing the proxy.
    """
    base = os.environ.get("ORBITBRIEF_CORE_WORKER_URL", "").strip()
    if not base:
        log.info("ORBITBRIEF_CORE_WORKER_URL unset; skipping brief-gen trigger.")
        return
    import time as _time
    import urllib.error
    import urllib.request
    import uuid

    bearer = os.environ.get("ORBITBRIEF_CORE_WORKER_BEARER", "").strip()
    run_id = uuid.uuid4().hex
    payload = json.dumps(
        {"deal_id": job.deal_id, "run_id": run_id, "mirror_latest": True}
    ).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    url = f"{base.rstrip('/')}/v1/compile-run?async=1"
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))  # direct, no proxy
    last_exc: Exception | None = None
    for attempt in range(1, 4):
        req = urllib.request.Request(url, data=payload, method="POST", headers=headers)
        try:
            with opener.open(req, timeout=20) as resp:
                status = getattr(resp, "status", None) or resp.getcode()
                if 200 <= int(status) < 300:
                    log.info(
                        "Brief-gen triggered (HTTP %s) deal=%s run=%s attempt=%s",
                        status, job.deal_id, run_id, attempt,
                    )
                    return
                body = resp.read()[:300]
                last_exc = RuntimeError(f"HTTP {status}: {body!r}")
        except Exception as exc:
            last_exc = exc
        log.warning(
            "Brief-gen HTTP attempt %s failed deal=%s: %s",
            attempt, job.deal_id, last_exc,
        )
        _time.sleep(min(2 * attempt, 6))
    raise RuntimeError(
        f"Brief-gen HTTP trigger failed after retries deal={job.deal_id}: {last_exc}"
    )


# v56: Side-channel atoms.json with the raw parser-os atom list — bypasses
# the OrbitBrief envelope projection (which overlays fixture data on the
# site_registry field). This is what the Deal Artifacts UI page reads so
# the PM sees real parser-os output without depending on OrbitBrief at all.
def _serialize_atom_for_ui(atom: Any) -> dict[str, Any]:
    """Convert an EvidenceAtom (pydantic model) to the dict shape the UI
    expects. Pulls every field Template D needs in one pass:
      atom_type, confidence, verified, raw_text, value, entity_keys,
      section_path, source_artifact, locator (page/table/row/char ranges).
    """
    # atom_type might be an enum — get the string form
    at = getattr(atom, "atom_type", None)
    atom_type_str = at.value if hasattr(at, "value") else str(at or "unknown")

    # Pick the canonical source_ref for locator + source artifact
    srefs = getattr(atom, "source_refs", []) or []
    primary = srefs[0] if srefs else None
    locator = {}
    source_artifact_id = getattr(atom, "artifact_id", "") or ""
    source_filename = ""
    extraction_method = ""
    if primary is not None:
        loc = getattr(primary, "locator", None) or {}
        if isinstance(loc, dict):
            locator = loc
        source_filename = getattr(primary, "filename", "") or ""
        extraction_method = getattr(primary, "extraction_method", "") or ""
        source_artifact_id = getattr(primary, "artifact_id", "") or source_artifact_id

    # section_path lives inside locator for some parsers, top-level for others
    section_path = locator.get("section_path") if isinstance(locator, dict) else None

    # Verified status: an atom is "verified" if it has any receipt that succeeded.
    receipts = getattr(atom, "receipts", []) or []
    verified = False
    receipt_kinds: list[str] = []
    for r in receipts:
        status = getattr(r, "status", None)
        status_str = status.value if hasattr(status, "value") else str(status or "")
        kind = getattr(r, "kind", None)
        kind_str = kind.value if hasattr(kind, "value") else str(kind or "")
        if kind_str:
            receipt_kinds.append(kind_str)
        if status_str.lower() in ("verified", "ok", "supported"):
            verified = True

    authority_class = getattr(atom, "authority_class", None)
    authority_str = authority_class.value if hasattr(authority_class, "value") else str(authority_class or "")

    review_status = getattr(atom, "review_status", None)
    review_str = review_status.value if hasattr(review_status, "value") else str(review_status or "")

    value = getattr(atom, "value", {}) or {}
    if not isinstance(value, dict):
        value = {}
    else:
        value = dict(value)
    said_by = str(value.get("said_by") or "").strip()
    speaker = str(value.get("speaker") or "").strip()
    if not speaker and isinstance(locator, dict):
        speaker = str(locator.get("speaker") or "").strip()
    label = said_by or speaker
    # Prefer "Trent Torrence · Purtera" as the section title for Atom Quality.
    # STRING (not list): Lovable adaptToEnvelopeAtoms only keeps string paths
    # (arrays were dropped → silent missing speakers in the audit UI).
    if label:
        section_path = label
        if isinstance(locator, dict):
            locator = dict(locator)
            locator["section_path"] = label
            if speaker and not locator.get("speaker"):
                locator["speaker"] = speaker
            if value.get("speaker_role") and not locator.get("speaker_role"):
                locator["speaker_role"] = value.get("speaker_role")
            for key in ("affiliation", "party", "voice", "org_role"):
                if value.get(key) and not locator.get(key):
                    locator[key] = value.get(key)

    # Heads / embeddings / FeedbackStore must see CLEAN utterance text.
    # Speaker identity lives on value.speaker / said_by / party / voice —
    # the UI adapter prefixes for display; do NOT poison raw_text.
    body = str(value.get("text") or "").strip() or (getattr(atom, "raw_text", "") or "")
    # Strip a prior display prefix if a bad serialize already wrote one.
    if label and body.startswith(label) and " — " in body[: len(label) + 4]:
        body = body.split(" — ", 1)[-1].strip() or body
    if body:
        value["text"] = body
    if speaker and not value.get("speaker"):
        value["speaker"] = speaker
    if said_by and not value.get("said_by"):
        value["said_by"] = said_by

    return {
        "id": getattr(atom, "id", ""),
        "atom_type": atom_type_str,
        "confidence": getattr(atom, "confidence", None),
        "calibrated_confidence": getattr(atom, "calibrated_confidence", None),
        "verified": verified,
        "receipt_kinds": receipt_kinds,
        "authority_class": authority_str,
        "review_status": review_str,
        "raw_text": body,
        "normalized_text": body or (getattr(atom, "normalized_text", "") or ""),
        "value": value,
        "entity_keys": list(getattr(atom, "entity_keys", []) or []),
        "section_path": section_path,
        "source_artifact_id": source_artifact_id,
        "source_filename": source_filename,
        "extraction_method": extraction_method,
        "locator": locator if isinstance(locator, dict) else {},
        "parser_version": getattr(atom, "parser_version", "") or "",
    }


def _upload_atoms(
    blob_service: BlobServiceClient,
    deal_id: str,
    compile_id: str,
    result: Any,
    elapsed_sec: float,
) -> str:
    """Write the raw parser-os atom list (plus per-stage timings + counts)
    to a side-channel blob the UI can read directly.

    Path: deals/<deal_id>/parser-os/latest/atoms.json
    """
    from collections import Counter

    atoms_list = list(getattr(result, "atoms", []) or [])
    serialized = [_serialize_atom_for_ui(a) for a in atoms_list]

    # Per-doc atom counts + per-type counts (lets the UI render the chip
    # cloud + Files table without computing across the full list).
    by_artifact: dict[str, dict[str, Any]] = {}
    type_counter: Counter = Counter()
    for a in serialized:
        type_counter[a["atom_type"]] += 1
        src = a.get("source_filename") or "(unknown)"
        slot = by_artifact.setdefault(src, {"atom_count": 0, "by_type": Counter()})
        slot["atom_count"] += 1
        slot["by_type"][a["atom_type"]] += 1
    by_artifact_out = {
        fn: {"atom_count": v["atom_count"], "by_type": dict(v["by_type"])}
        for fn, v in by_artifact.items()
    }

    # Stage timings from trace (if available on the result)
    stages: list[dict[str, Any]] = []
    try:
        trace = getattr(result, "trace", None) or {}
        if isinstance(trace, dict):
            evts = trace.get("stages") or trace.get("events") or []
            for e in evts:
                if isinstance(e, dict):
                    stages.append({
                        "stage": e.get("stage"),
                        "duration_ms": e.get("duration_ms"),
                        "counts": e.get("counts") or {},
                        "warning_count": e.get("warning_count", 0),
                        "error_count": e.get("error_count", 0),
                    })
    except Exception:
        pass

    payload = {
        "schema": "parser_os.atoms.v1",
        "compile_id": compile_id,
        "deal_id": deal_id,
        "generated_at_utc": __import__("datetime").datetime.utcnow().isoformat() + "Z",
        "elapsed_sec": round(elapsed_sec, 2),
        "counts": {
            "atoms": len(serialized),
            "entities": len(getattr(result, "entities", []) or []),
            "edges": len(getattr(result, "edges", []) or []),
            "packets": len(getattr(result, "packets", []) or []),
            "by_atom_type": dict(type_counter),
        },
        "by_artifact": by_artifact_out,
        "stages": stages,
        "atoms": serialized,
    }

    path = f"deals/{deal_id}/parser-os/latest/atoms.json"
    client = blob_service.get_blob_client(container=BLOB_CONTAINER, blob=path)
    client.upload_blob(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
        overwrite=True,
        content_type="application/json",
    )

    # Full CompileResult (rich atoms + packets with the feature fields) — the
    # features feed the nightly calibrator fit needs. atoms.json above is a
    # UI-compact projection that lacks the per-atom signals build_atom_feature_row
    # consumes. Best-effort: never let it fail the compile.
    try:
        if hasattr(result, "model_dump"):
            rc = blob_service.get_blob_client(
                container=BLOB_CONTAINER,
                blob=f"deals/{deal_id}/parser-os/latest/result.json",
            )
            rc.upload_blob(
                json.dumps(result.model_dump(mode="json"), default=str),
                overwrite=True,
                content_type="application/json",
            )
    except Exception as exc:  # pragma: no cover - calibrator feed is additive
        log.warning("result.json persist skipped: %s", exc)
    return path


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
    # Prevent Errno 28 recurrence: scrub orphans, then refuse to start if /tmp
    # is critically low (better a clear poison than a half-written workdir).
    _scrub_stale_worker_tmp()
    _assert_tmp_has_space(min_free_bytes=512 * 1024 * 1024)
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
        #    Dedupe hs-email rows, then upgrade plain-text stubs to a larger
        #    multipart sibling already under deals/<deal>/artifacts/*/ so CID
        #    OCR runs even when the compile base still points at a 2KB stub.
        from app.core.manifest_artifact_dedup import (
            dedupe_manifest_email_artifacts,
            upgrade_stub_hs_email_artifacts_from_siblings,
        )

        def _list_artifact_siblings(container: str, prefix: str) -> list[tuple[str, int]]:
            client = blob_service.get_container_client(container)
            out: list[tuple[str, int]] = []
            for blob in client.list_blobs(name_starts_with=prefix):
                size = int(getattr(blob, "size", None) or getattr(getattr(blob, "properties", None), "size", 0) or 0)
                out.append((str(blob.name), size))
            return out

        account_host = urlparse(
            str((manifest.get("artifacts") or [{}])[0].get("blob_url") or "")
        ).netloc or f"{ACCOUNT_NAME}.blob.core.windows.net"

        artifacts = upgrade_stub_hs_email_artifacts_from_siblings(
            dedupe_manifest_email_artifacts(manifest.get("artifacts") or []),
            list_blobs=_list_artifact_siblings,
            account_host=account_host,
        )
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

        # v57: live compile-progress.json writer — fires after every
        # parser-os stage so the UI can render an accurate timeline
        # (current stage + elapsed + ETA) instead of an unbounded spinner.
        compile_started_iso = _iso_now()
        progress_path = (
            f"deals/{job.deal_id}/orbitbrief/latest/compile-progress.json"
        )
        progress_started_perf = time.time()

        def _on_stage_end(stage, all_stages):  # type: ignore[no-untyped-def]
            done = [
                {
                    "stage_name": s.stage_name,
                    "duration_ms": float(s.duration_ms or 0.0),
                    "input_count": s.input_count,
                    "output_count": s.output_count,
                }
                for s in all_stages
            ]
            payload = {
                "compile_id": job.compile_id,
                "deal_id": job.deal_id,
                "status": "running",
                "current_stage": stage.stage_name,
                "stages": done,
                "stage_count_done": len(done),
                "stage_count_total_estimate": 14,
                "started_at": compile_started_iso,
                "updated_at": _iso_now(),
                "elapsed_ms": int(
                    (time.time() - progress_started_perf) * 1000.0
                ),
                "worker_sha": WORKER_SHA,
                "parser_os_sha": PARSER_OS_SHA,
            }
            try:
                blob_service.get_blob_client(
                    container=BLOB_CONTAINER, blob=progress_path,
                ).upload_blob(
                    json.dumps(payload, indent=2, default=str).encode("utf-8"),
                    overwrite=True,
                    content_type="application/json",
                )
            except Exception as exc:  # pragma: no cover — best-effort UX
                log.warning("compile-progress upload failed: %s", exc)

        # v57: seed compile-progress.json BEFORE compile starts so the
        # UI's polling hook can render a "starting compile…" state the
        # moment the page sees a fresh compile_id in flight, instead of
        # waiting for the first stage to finish.
        try:
            blob_service.get_blob_client(
                container=BLOB_CONTAINER, blob=progress_path,
            ).upload_blob(
                json.dumps({
                    "compile_id": job.compile_id,
                    "deal_id": job.deal_id,
                    "status": "starting",
                    "current_stage": None,
                    "stages": [],
                    "stage_count_done": 0,
                    "stage_count_total_estimate": 14,
                    "started_at": compile_started_iso,
                    "updated_at": _iso_now(),
                    "elapsed_ms": 0,
                    "worker_sha": WORKER_SHA,
                    "parser_os_sha": PARSER_OS_SHA,
                }, indent=2).encode("utf-8"),
                overwrite=True,
                content_type="application/json",
            )
        except Exception as exc:
            log.warning("compile-progress (starting) upload failed: %s", exc)

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
            stage_callback=_on_stage_end,
        )
        elapsed = time.time() - t0
        log.info("compile_project done in %.1fs", elapsed)

        # v58: the parser STAGES are done — but build_orbitbrief_envelope()
        # below (the OrbitBrief projection: cockpit surfaces, facet sections,
        # service_routing head) still takes ~90s and IS the deliverable the UI
        # renders. The old code marked compile-progress "done" HERE — ~90s
        # before envelope.json existed — so the cockpit showed the pipeline
        # DONE with NO results ("Parsing… / Results appear when the compile
        # completes") for that entire window. Keep status "running" through the
        # projection and flip to "done" only AFTER envelope.json is uploaded
        # (below), so the tracker hitting DONE coincides with results being
        # available — the UI's liveDoneButEnvelopeStale gate clears and the
        # cockpit populates in the SAME poll instead of ~90s later.
        final_stages = [
            {
                "stage_name": s.stage_name,
                "duration_ms": float(s.duration_ms or 0.0),
                "input_count": s.input_count,
                "output_count": s.output_count,
            }
            for s in getattr(result, "trace", None).stages
        ] if getattr(result, "trace", None) is not None else []
        n_stages = len(final_stages)

        def _write_compile_progress(
            status: str, current_stage: str | None, total_estimate: int,
        ) -> None:
            """Single writer for the post-compile compile-progress.json
            transitions (projection → done) so they stay byte-consistent."""
            try:
                blob_service.get_blob_client(
                    container=BLOB_CONTAINER, blob=progress_path,
                ).upload_blob(
                    json.dumps({
                        "compile_id": job.compile_id,
                        "deal_id": job.deal_id,
                        "status": status,
                        "current_stage": current_stage,
                        "stages": final_stages,
                        "stage_count_done": n_stages,
                        "stage_count_total_estimate": total_estimate,
                        "started_at": compile_started_iso,
                        "updated_at": _iso_now(),
                        "elapsed_ms": int(
                            (time.time() - progress_started_perf) * 1000.0
                        ),
                        "worker_sha": WORKER_SHA,
                        "parser_os_sha": PARSER_OS_SHA,
                    }, indent=2, default=str).encode("utf-8"),
                    overwrite=True,
                    content_type="application/json",
                )
            except Exception as exc:  # pragma: no cover — best-effort UX
                log.warning("compile-progress (%s) upload failed: %s", status, exc)

        # Parser stages complete; envelope projection now in flight. Show it as
        # one extra "projection" stage (N/N+1 ≈ finalizing) so the bar does NOT
        # read a premature N/N DONE while the deliverable is still being built.
        _write_compile_progress(
            "running", "projection", (n_stages + 1) if n_stages else 15,
        )

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

        # v58: envelope.json now exists → flip compile-progress to "done".
        # The cockpit's refetch-on-done fires and renders results in the SAME
        # poll cycle (≤1.5s while running), because envelope.compile_id now
        # equals the live "done" compile_id (liveDoneButEnvelopeStale clears).
        # N/N == 100%. THIS is the write that was previously ~90s too early.
        _write_compile_progress("done", None, n_stages or 14)

        # v60: write the change-detection fingerprint. parser-os-service reads this
        # on the next enqueue and SKIPS a redundant compile when the artifacts are
        # byte-identical — killing the ~4-hourly auto-finalize floods that re-compile
        # unchanged deals (the queue's main backlog source). Keyed on the sorted set
        # of artifact content hashes; parser/worker SHAs are recorded for audit.
        try:
            import hashlib
            shas = sorted(
                str(a.get("content_sha256") or "")
                for a in (manifest.get("artifacts") or [])
            )
            artifact_key = hashlib.sha256(
                "\n".join(shas).encode("utf-8")
            ).hexdigest()
            blob_service.get_blob_client(
                container=BLOB_CONTAINER,
                blob=f"deals/{job.deal_id}/orbitbrief/latest/compile-idempotency.json",
            ).upload_blob(
                json.dumps({
                    "artifact_key": artifact_key,
                    "compile_id": job.compile_id,
                    "parser_os_sha": PARSER_OS_SHA,
                    "worker_sha": WORKER_SHA,
                    "completed_at": _iso_now(),
                }, indent=2).encode("utf-8"),
                overwrite=True,
                content_type="application/json",
            )
        except Exception as exc:
            log.warning("idempotency fingerprint write failed: %s", exc)

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

        # 5e. v56: ALSO upload raw atoms.json side-channel for the UI
        # to read directly (bypasses the OrbitBrief envelope projection
        # which overlays fixture data on site_registry). One source of
        # truth for Template D — every field the deal-artifacts page
        # needs without depending on the OrbitBrief brief-gen pipeline.
        try:
            atoms_path = _upload_atoms(
                blob_service, job.deal_id, job.compile_id, result, elapsed
            )
            log.info("Uploaded atoms to %s", atoms_path)
        except Exception as atoms_exc:
            # Never let atoms.json write failure kill the compile —
            # envelope.json already uploaded above is the contract.
            log.warning("Failed to write atoms.json side-channel: %s", atoms_exc)
            atoms_path = None

        return {
            "envelope_path": env_path,
            "atoms_path": atoms_path,
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


def _tmp_root() -> Path:
    return Path(os.environ.get("TMPDIR") or tempfile.gettempdir())


def _scrub_stale_worker_tmp() -> int:
    """Remove leftover ``parser-os-worker-*`` workdirs (crash / kill orphans)."""
    root = _tmp_root()
    removed = 0
    try:
        for path in root.glob("parser-os-worker-*"):
            if not path.is_dir():
                continue
            shutil.rmtree(path, ignore_errors=True)
            if not path.exists():
                removed += 1
    except Exception as exc:
        log.warning("tmp scrub failed under %s: %s", root, exc)
        return removed
    if removed:
        log.info("Scrubbed %d stale parser-os-worker-* dir(s) under %s", removed, root)
    return removed


def _assert_tmp_has_space(min_free_bytes: int) -> None:
    root = _tmp_root()
    try:
        usage = shutil.disk_usage(str(root))
    except Exception as exc:
        log.warning("disk_usage(%s) failed: %s", root, exc)
        return
    if usage.free >= min_free_bytes:
        return
    # One more scrub pass, then hard-fail so we don't enqueue disk-full poison.
    _scrub_stale_worker_tmp()
    try:
        usage = shutil.disk_usage(str(root))
    except Exception:
        return
    if usage.free < min_free_bytes:
        raise OSError(
            28,
            f"Insufficient space under {root}: free={usage.free} need>={min_free_bytes}",
        )


def main() -> int:
    log.info(
        "parser-os-worker starting (worker_sha=%s parser_os_sha=%s)",
        WORKER_SHA,
        PARSER_OS_SHA,
    )
    _scrub_stale_worker_tmp()

    # Job replicas yield to the warm persistent poller when configured — prevents
    # two consumers racing the same queue message (warm + job MessageNotFound).
    if os.environ.get("SOWSMITH_DEFER_TO_WARM_WORKER", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    ):
        if os.environ.get("WORKER_LOOP", "").strip().lower() not in (
            "1",
            "true",
            "yes",
            "on",
        ):
            log.info("SOWSMITH_DEFER_TO_WARM_WORKER — job replica exiting without dequeue")
            return 0

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
    # v59: drain the PRIORITY queue (interactive re-parses) BEFORE the normal
    # queue (bulk/batch). One message per process (Container Apps Jobs pattern).
    # A user's UI click must never wait behind a bulk backlog — so we always check
    # priority first and only fall through to the normal queue when it's empty.
    # The priority check is best-effort: if the queue is missing/unreachable we
    # silently use the normal queue (byte-identical to pre-v59 behavior).
    queue_client = queue_service.get_queue_client(QUEUE_NAME)
    source_queue = QUEUE_NAME
    # Prefer singular receive_message when available (avoids draining the queue
    # via list(receive_messages(...))). Fall back to a one-message page.
    msg = None
    try:
        priority_client = queue_service.get_queue_client(PRIORITY_QUEUE_NAME)
        if hasattr(priority_client, "receive_message"):
            msg = priority_client.receive_message(visibility_timeout=VISIBILITY_TIMEOUT_SEC)
        else:
            page = list(
                priority_client.receive_messages(
                    visibility_timeout=VISIBILITY_TIMEOUT_SEC,
                    max_messages=1,
                )
            )
            msg = page[0] if page else None
        if msg is not None:
            queue_client = priority_client
            source_queue = PRIORITY_QUEUE_NAME
            log.info("Priority queue hit (id=%s)", getattr(msg, "id", "?"))
    except Exception as exc:
        log.warning("Priority queue poll failed (%s); using normal queue.", exc)

    if msg is None:
        log.info("Dequeuing one message from %s ...", QUEUE_NAME)
        if hasattr(queue_client, "receive_message"):
            msg = queue_client.receive_message(visibility_timeout=VISIBILITY_TIMEOUT_SEC)
        else:
            page = list(
                queue_client.receive_messages(
                    visibility_timeout=VISIBILITY_TIMEOUT_SEC,
                    max_messages=1,
                )
            )
            msg = page[0] if page else None

    if msg is None:
        log.info("Both queues empty; nothing to do.")
        return 0

    log.info(
        "Got message from %s (dequeue_count=%d, id=%s)",
        source_queue, int(getattr(msg, "dequeue_count", 0) or 0), getattr(msg, "id", "?"),
    )

    # Decode
    try:
        # Storage Queue messages may be base64-encoded; the SDK handles that.
        raw = msg.content
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        job = JobMessage.from_raw(raw)
    except Exception as exc:
        log.exception("Poison message — cannot decode: %s", exc)
        poison_raw = msg.content
        if isinstance(poison_raw, bytes):
            poison_raw = poison_raw.decode("utf-8", errors="replace")
        _forward_to_poison_queue(
            queue_service,
            raw_message=str(poison_raw),
            reason="decode_error",
            dequeue_count=getattr(msg, "dequeue_count", 0),
            source_queue=source_queue,
        )
        _safe_delete_queue_message(queue_client, msg, context="poison decode")
        return 2

    # Ops visibility: warn before poison threshold; log approximate backlog.
    dq = int(getattr(msg, "dequeue_count", 0) or 0)
    if dq >= 3:
        log.error(
            "High dequeue_count=%d (MAX=%d) deal=%s compile=%s queue=%s — near poison",
            dq,
            MAX_DEQUEUE_COUNT,
            getattr(job, "deal_id", "?"),
            getattr(job, "compile_id", "?"),
            source_queue,
        )
    try:
        pri_approx = None
        main_approx = None
        try:
            pri_approx = int(
                getattr(
                    queue_service.get_queue_client(PRIORITY_QUEUE_NAME).get_queue_properties(),
                    "approximate_message_count",
                    0,
                )
                or 0
            )
        except Exception:
            pass
        try:
            main_approx = int(
                getattr(
                    queue_service.get_queue_client(QUEUE_NAME).get_queue_properties(),
                    "approximate_message_count",
                    0,
                )
                or 0
            )
        except Exception:
            pass
        log.info(
            "Queue depth after receive deal=%s lane=%s priority_approx=%s main_approx=%s",
            getattr(job, "deal_id", "?"),
            source_queue,
            pri_approx,
            main_approx,
        )
    except Exception as exc:
        log.debug("Queue depth log skipped: %s", exc)

    # Poison guard: too many retries
    if msg.dequeue_count > MAX_DEQUEUE_COUNT:
        log.error(
            "Message exceeded MAX_DEQUEUE_COUNT=%d (this dequeue %d), forwarding to poison.",
            MAX_DEQUEUE_COUNT,
            msg.dequeue_count,
        )
        _write_status(
            blob_service, job, "failed",
            stage="exhausted_retries",
            error=f"dequeue_count {msg.dequeue_count} > {MAX_DEQUEUE_COUNT}",
        )
        _forward_to_poison_queue(
            queue_service,
            raw_message=raw if isinstance(raw, str) else str(raw),
            reason="exhausted_retries",
            job=job,
            dequeue_count=msg.dequeue_count,
            source_queue=source_queue,
        )
        _safe_delete_queue_message(queue_client, msg, context="exhausted retries")
        return 2

    # Dev skip-list: ack-and-drop deals we deliberately don't run on this worker
    # (a giant deal hogging the single LLM in dev → interactive reparses starve).
    # Reversible via SOWSMITH_WORKER_SKIP_DEALS. Universal mechanism (env-driven id
    # list), not parser logic.
    if job.deal_id in SKIP_DEAL_IDS:
        log.warning("deal_id %s in SOWSMITH_WORKER_SKIP_DEALS — acking without compiling", job.deal_id)
        _safe_delete_queue_message(queue_client, msg, context="skip deal")
        return 0

    # Mark running
    _write_status(blob_service, job, "running", stage="starting", percent_complete=0)

    # Do the work
    try:
        log.info("Downloading manifest %s", job.manifest_blob_url)
        manifest = _download_manifest(blob_service, job.manifest_blob_url)
        # v61: worker-side change-detection — skip a redundant compile when the
        # deal's artifacts + parser/worker SHA are unchanged since the last
        # successful one. This is what kills the timer-driven bulk floods
        # (hubspot-sync / orbitbrief-runs re-compiling unchanged deals every cycle)
        # that starve interactive work — no matter how they were enqueued. force
        # bypasses, so a deliberate re-parse always runs.
        if not job.force and _unchanged_since_last_compile(
            blob_service, job.deal_id, manifest
        ):
            log.info(
                "Skip (unchanged) deal=%s compile=%s — artifacts + code match last compile",
                job.deal_id, job.compile_id,
            )
            _write_status(
                blob_service, job, "completed",
                stage="skipped_unchanged", percent_complete=100,
            )
            _safe_delete_queue_message(queue_client, msg, context="skipped unchanged")
            return 0
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
        # The queue consumer above isn't deployed, so ALSO trigger brief-gen
        # directly over HTTP — this is the live auto-trigger that keeps OrbitBrief
        # fresh after every parse. Best-effort; never fails the compile.
        try:
            _trigger_brief_gen_http(job)
        except Exception as exc:
            log.warning(
                "Brief-gen HTTP trigger failed for deal=%s compile=%s: %s",
                job.deal_id, job.compile_id, exc,
            )

        _safe_delete_queue_message(queue_client, msg, context="compile success")
        log.info("Job complete: compile_id=%s", job.compile_id)

        # Cross-run learning (fully non-fatal, AFTER the result is delivered):
        # the compile just appended new teacher rows (+ any PM corrections) to
        # the persisted training log. Retrain the eval-gated deflector heads on
        # the grown log (no-op until the log grows past the staleness gate) and
        # persist the log + retrained heads back to blob so the NEXT run loads
        # improved heads. This is what makes the dev deflectors learn live.
        # Gated: the retrain embeds the log via Ollama, which under LLM contention
        # can HANG and hold the worker slot for many minutes (starving the next
        # reparse). OFF in dev (SOWSMITH_WORKER_RETRAIN=0) so the execution returns
        # the instant the result is delivered → the slot frees immediately.
        if RETRAIN_ENABLED:
            try:
                from app.core.type_head import retrain_if_stale
                retrain_if_stale()
            except Exception as exc:
                log.warning("type-head retrain skipped: %s", exc)
            try:
                from app.core.span_extractor import retrain_span_heads
                retrain_span_heads()
            except Exception as exc:
                log.warning("span-head retrain skipped: %s", exc)
            try:
                import subprocess
                import sys as _sys
                subprocess.run([_sys.executable, "/write_back_ml.py"], timeout=180, check=False)
            except Exception as exc:
                log.warning("ml write-back skipped: %s", exc)

        return 0

    except Exception as exc:
        log.exception("Compile failed: %s", exc)
        # v58: ensure compile-progress.json reaches a TERMINAL state on failure.
        # We now keep compile-progress "running" through the ~90s envelope
        # projection (so the tracker doesn't report DONE before results exist),
        # which means a failure anywhere in the compile must be reflected here —
        # otherwise the cockpit fast-polls "running" forever and shows "Parsing…"
        # with no end. Best-effort; the authoritative job status is written below.
        try:
            blob_service.get_blob_client(
                container=BLOB_CONTAINER,
                blob=f"deals/{job.deal_id}/orbitbrief/latest/compile-progress.json",
            ).upload_blob(
                json.dumps({
                    "compile_id": job.compile_id,
                    "deal_id": job.deal_id,
                    "status": "failed",
                    "current_stage": None,
                    "updated_at": _iso_now(),
                    "error": f"{type(exc).__name__}: {exc}"[:500],
                    "worker_sha": WORKER_SHA,
                    "parser_os_sha": PARSER_OS_SHA,
                }, indent=2, default=str).encode("utf-8"),
                overwrite=True,
                content_type="application/json",
            )
        except Exception:
            pass
        # Permanent failure (dead deal: manifest/artifact blob deleted) → DROP the
        # message immediately. Otherwise it cycles every visibility_timeout (30min)
        # x MAX_DEQUEUE, clogging the single-slot queue for ~90min and starving
        # interactive reparses (the dev "zombie storm"). Transient failures still
        # retry.
        msg_l = f"{type(exc).__name__}: {exc}".lower()
        permanent = (
            "blobnotfound" in msg_l
            or "the specified blob does not exist" in msg_l
            or ("notfound" in type(exc).__name__.lower() and "blob" in msg_l)
        )
        _write_status(
            blob_service, job, "failed",
            stage=("dead_deal" if permanent else "exception"),
            error=f"{type(exc).__name__}: {exc}",
            traceback=traceback.format_exc()[:4000],
        )
        if permanent:
            log.error("Permanent failure (dead deal / missing blob) — dropping message to break the zombie cycle")
            try:
                _safe_delete_queue_message(queue_client, msg, context="permanent failure")
            except Exception:
                pass
            return 2
        # transient → leave for retry
        return 1


if __name__ == "__main__":
    # WORKER_LOOP=1 → warm persistent poller (Container App, minReplicas=1). The pod
    # stays alive polling the queue, so fetch_ml + the bge gate/span torch models load
    # ONCE at startup and stay hot in-process across messages — every reparse is
    # picked up instantly with zero cold-start. Default (unset) = one-shot Job mode.
    if os.environ.get("WORKER_LOOP", "").strip().lower() in ("1", "true", "yes", "on"):
        import time as _time
        _sleep = int(os.environ.get("WORKER_LOOP_SLEEP_SEC", "3"))
        log.info("WORKER_LOOP mode — warm persistent poller (sleep=%ss between polls)", _sleep)
        while True:
            try:
                main()
            except Exception as exc:  # never let one bad message kill the warm pod
                log.exception("loop iteration error: %s", exc)
            _time.sleep(_sleep)
    else:
        sys.exit(main())
