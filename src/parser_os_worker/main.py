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
import threading
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
COMPILE_TIMEOUT_SEC = int(os.environ.get("COMPILE_TIMEOUT_SEC", "1500"))  # 25 min floor
#: Seconds of budget per document in the manifest, on top of the floor.
#:
#: A flat 25 minutes is not a budget, it is a bet that every deal is the same
#: size. They are not. 010180 has 9 documents and compiles in 3 minutes;
#: 010347 has 68 and spent 881s in `parse_artifacts` alone -- 13s a document,
#: 59% of the whole allowance -- then died at `bom_owner` with every stage
#: after it unrun. 010264 did the same at ~18s a document. Both were reported
#: as "still parsing after 120 minutes", which they were not: they were dead,
#: deterministically, and re-running them as-is fails the same way.
#:
#: 45s is deliberately generous against the 13-18s observed, because the
#: downstream stages grow with the ATOM count and a 68-document deal produced
#: 8,486 atoms where a 9-document one produced 249.
COMPILE_SEC_PER_DOC = float(os.environ.get("COMPILE_SEC_PER_DOC", "45"))
VISIBILITY_TIMEOUT_SEC = int(os.environ.get("MESSAGE_VISIBILITY_TIMEOUT_SEC", "1800"))  # 30 min
# A compile legitimately runs longer than its lease: source_replay alone took 33
# minutes on a 17,986-atom deal, whole compiles 45+. When the lease lapses the
# queue hands the SAME message to the next poll -- dequeue_count=2 was observed
# live on deal 2fd8baf1 -- and the deal is compiled again from scratch while the
# first run is still going. That is the "it keeps re-running" symptom. Renew
# the lease while the compile is in progress instead of guessing a ceiling.
LEASE_RENEW_SEC = int(os.environ.get("MESSAGE_LEASE_RENEW_SEC", "600"))  # renew every 10 min
LEASE_MAX_SEC = int(os.environ.get("MESSAGE_LEASE_MAX_SEC", str(4 * 3600)))  # hard stop: 4 h
MAX_DEQUEUE_COUNT = int(os.environ.get("MAX_DEQUEUE_COUNT", "3"))  # poison after 3 retries
#: How much of the lease a compile may spend. The lease RENEWER keeps the
#: message ours for as long as we work, so the ceiling is LEASE_MAX_SEC and not
#: the visibility timeout -- raising the compile budget past
#: MESSAGE_VISIBILITY_TIMEOUT_SEC does NOT cause a duplicate compile, which is
#: the thing that made a bigger budget look unsafe. Staying under the lease
#: backstop is what matters, with room for the status write on the way out.
COMPILE_BUDGET_LEASE_FRACTION = float(
    os.environ.get("COMPILE_BUDGET_LEASE_FRACTION", "0.75"))


def compile_budget_sec(manifest: Any) -> float:
    """The seconds this particular compile gets, from how much work it is.

    Floor is COMPILE_TIMEOUT_SEC so small deals are unchanged. Ceiling is a
    fraction of LEASE_MAX_SEC so a pathological manifest cannot outlive the
    lease that keeps its message invisible.
    """
    try:
        docs = len((manifest or {}).get("artifacts") or [])
    except Exception:  # noqa: BLE001 - a malformed manifest gets the floor
        docs = 0
    ceiling = max(float(COMPILE_TIMEOUT_SEC),
                  LEASE_MAX_SEC * COMPILE_BUDGET_LEASE_FRACTION)
    return min(ceiling, max(float(COMPILE_TIMEOUT_SEC), COMPILE_SEC_PER_DOC * docs))
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

# Azure Core's HttpLoggingPolicy logs the URL, method and every request/response
# header at INFO -- not DEBUG -- so an INFO root logger turns each storage and
# queue call into a dozen log lines. Measured on the dev worker: 15,668,964 lines
# in 7 days, ~8.6 GB, which was the entire cost of the dev Log Analytics
# workspace and the largest single logging line item in the subscription. The
# content is header names with values already REDACTED, so it buys nothing.
#
# Raise the azure logger specifically rather than the root, so our own INFO logs
# (stage heartbeats, compile progress) are untouched. Override with
# PARSER_OS_AZURE_LOG_LEVEL=INFO when actually debugging an SDK call.
logging.getLogger("azure").setLevel(
    os.environ.get("PARSER_OS_AZURE_LOG_LEVEL", "WARNING").upper()
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


class _CompileWatchdog:
    """Kill a compile that has outrun ``COMPILE_TIMEOUT_SEC``.

    The constant existed and was set on the live worker, and nothing read it —
    it was assigned on import and never referenced again, so no compile has ever
    been bounded. On 2026-09-05 a 35-artifact deal sat in ``parse_artifacts``
    for 6.8 hours: heartbeating, the two large spreadsheets already parsed, the
    twenty artifacts left 0-2 KB apiece, and the model host answering in 0.3s.
    Nothing was being accomplished and nothing was ever going to stop it.

    A blocked C call does not run Python bytecode, so a signal-based deadline
    can be swallowed by exactly the hang it is meant to catch. This is therefore
    a plain daemon thread that writes the failure and then takes the process
    down. Blunt on purpose: the container restarts, the queue message becomes
    visible again, and ``MAX_DEQUEUE_COUNT`` poisons the deal after three tries
    instead of one deal holding the only slot forever.
    """

    #: How long to let the clean status write finish before exiting anyway.
    GRACE_SEC = 20

    def __init__(self, blob_service: Any, job: Any, timeout_sec: float) -> None:
        self._blob_service = blob_service
        self._job = job
        # float, not int: truncating a sub-second budget to 0 would read as
        # "disabled" and silently restore the unbounded behaviour this exists
        # to end. Only a value that is genuinely <= 0 turns the bound off.
        self._timeout = max(0.0, float(timeout_sec))
        self._done = threading.Event()
        self._thread: threading.Thread | None = None
        self.fired = False

    def start(self) -> "_CompileWatchdog":
        if self._timeout <= 0:  # 0 disables the bound, deliberately and visibly
            log.warning("COMPILE_TIMEOUT_SEC=%s — this compile is unbounded.", self._timeout)
            return self
        self._thread = threading.Thread(target=self._run, name="compile-watchdog", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._done.set()

    def _run(self) -> None:
        if self._done.wait(self._timeout):
            return
        self.fired = True
        log.error(
            "Compile exceeded COMPILE_TIMEOUT_SEC=%ss (deal=%s compile=%s). "
            "Killing the worker so the slot is freed; the message returns to the queue.",
            self._timeout, self._job.deal_id, self._job.compile_id,
        )
        # Say so on the deal before dying, or the brief shows a run that never
        # ends and nobody can tell a wedged compile from a slow one.
        try:
            _write_status(
                self._blob_service, self._job, "failed",
                stage="timeout",
                error=f"compile exceeded COMPILE_TIMEOUT_SEC={self._timeout:g}s",
            )
        except Exception as exc:  # pragma: no cover - best effort before exit
            log.error("Could not record the timeout on the deal: %s", exc)
        try:
            _clear_compile_active(self._blob_service, self._job)
        except Exception:
            pass
        time.sleep(0.1)
        os._exit(75)


#: Where a request to stop a deal's running compile is written. One per deal,
#: beside the progress file the panel already reads, so the same SAS and the
#: same container serve both.
#: One tiny blob per RUNNING compile, under a flat prefix.
#:
#: The queue panel used to answer "what is compiling?" by listing every deal
#: folder and reading all ~500 progress documents. Measured against live dev
#: that is 911ms just to list the folders, before a single read -- so the
#: endpoint took 4-6s on a consumption plan and the panel polled at 2s against
#: an answer it could not get in 2s.
#:
#: The worker already knows. It writes a marker when it starts and deletes it
#: when it stops, so the reader lists ONE small prefix: 231ms for the listing
#: and 122ms for four parallel reads, measured the same way.
#:
#: Named by compile id, not deal id: a deal can legitimately have two compiles
#: in flight, and keying on the deal would have one silently erase the other.
ACTIVE_INDEX_PREFIX = "_active-compiles"


def _active_index_path(compile_id: str) -> str:
    return f"{ACTIVE_INDEX_PREFIX}/{compile_id}.json"


def _mark_compile_active(
    blob_service: BlobServiceClient, job: "JobMessage", stage: str | None = None
) -> None:
    """Say, in one small blob, that this compile is running.

    Best-effort on purpose. This is an INDEX, not the truth: the progress
    document remains the record, and a reader that finds a marker still reads
    the progress beside it. A marker that fails to write costs a compile its
    place in a fast listing, not its existence -- the periodic full sweep on
    the reading side still finds it.
    """
    try:
        blob_service.get_blob_client(
            container=BLOB_CONTAINER, blob=_active_index_path(job.compile_id),
        ).upload_blob(
            json.dumps({
                "compile_id": job.compile_id,
                "deal_id": job.deal_id,
                "stage": stage,
                "updated_at": _iso_now(),
                "worker_sha": WORKER_SHA,
            }).encode("utf-8"),
            overwrite=True,
            content_type="application/json",
        )
    except Exception as exc:  # pragma: no cover - the index must never fail a compile
        log.warning("active-index write failed for %s: %s", job.compile_id, exc)


def _clear_compile_active(blob_service: BlobServiceClient, job: "JobMessage") -> None:
    """Take this compile out of the index, however it ended.

    Called from a `finally`, and from the cancel path, and on the way out of a
    watchdog kill -- every exit, because a marker left behind is a compile the
    panel shows as running forever. The stale rule on the reading side is the
    backstop for the exits nothing can catch, like SIGKILL.
    """
    try:
        blob_service.get_blob_client(
            container=BLOB_CONTAINER, blob=_active_index_path(job.compile_id),
        ).delete_blob()
    except Exception:
        # Already gone is the normal case for a double-call, and a failure here
        # must not mask whatever the compile was actually doing.
        pass


def _cancel_blob_path(deal_id: str) -> str:
    return f"deals/{deal_id}/orbitbrief/latest/compile-cancel.json"


#: How often a running compile asks whether it has been told to stop.
CANCEL_POLL_SEC = max(1.0, float(os.environ.get("SOWSMITH_CANCEL_POLL_SEC", "3") or 3))

#: How often the progress document is re-written while a stage is still
#: running. Four seconds: fast enough that a rate can be read off it within a
#: few samples, slow enough that a long compile costs a few hundred small blob
#: writes rather than thousands.
PROGRESS_HEARTBEAT_SEC = max(1.0, float(os.environ.get("SOWSMITH_PROGRESS_HEARTBEAT_SEC", "4") or 4))


class _ProgressHeartbeat:
    """Re-writes the running compile's progress document on a timer, and can be
    stopped for good.

    The heartbeat repeats the LAST stage the compile reported. Once the parser
    stages are done that is the final stage ("quality_gates"), and the worker
    then writes "projection" and, after envelope.json is uploaded, "done". Live
    010353 (compile b40e9bb3, 2026-10-02): the heartbeat was stopped only after
    the whole job returned, so every tick re-wrote "running quality_gates" over
    those writes, and the progress document stayed there from 14:26:41 -- the
    moment envelope.json was written -- onward.

    ``stop()`` takes the same lock a tick writes under, so when it returns no
    tick is mid-write and none will follow: whatever is written next is last.
    """

    def __init__(self, write, interval: float) -> None:  # type: ignore[no-untyped-def]
        self._write = write
        self._interval = interval
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._run, name="progress-heartbeat", daemon=True)

    def start(self) -> "_ProgressHeartbeat":
        self._thread.start()
        return self

    def _run(self) -> None:  # pragma: no cover - timing thread
        while not self._stop.wait(self._interval):
            with self._lock:
                if self._stop.is_set():
                    return
                try:
                    self._write()
                except Exception as exc:
                    # Never let the heartbeat take a compile down; it is a
                    # reporting nicety and the stage callbacks still fire.
                    log.debug("progress heartbeat write failed: %s", exc)

    def stop(self) -> None:
        with self._lock:
            self._stop.set()

    def set(self) -> None:
        """Event-compatible alias: callers that held the old stop Event call set()."""
        self.stop()

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()


class _CancelWatcher:
    """Stop a compile somebody has asked to stop, in seconds rather than stages.

    A PM dragging a deal onto a busy slot means "run mine instead of that one",
    and until this existed there was nothing that could honour it: nothing in
    this worker read a cancel signal at all, and `cancelQueuedCompile` only
    drains messages that have not started.

    WHY A THREAD AND NOT THE STAGE CALLBACKS. The obvious hook is
    ``stage_start_callback`` -- the worker already fires it around every stage.
    But a cancel checked between stages is only as responsive as the longest
    stage, and ``enrich_entities`` is 669 of 673 seconds on a models-on
    compile. Somebody asking for their deal to jump the queue would wait up to
    eleven minutes for the reply. That is not a cancel, it is a request.

    So: a daemon thread on a short poll, and a hard exit. Blunt on purpose, and
    the same shape as _CompileWatchdog above, which kills for the same reason.
    Nothing under ``latest/`` is written mid-compile except the progress file
    -- the envelope and atoms are written at the end -- so a compile killed
    here leaves no half-finished artifact behind it.

    Before exiting it does three things, in this order and all best-effort:
    marks the deal cancelled so nobody sees a run that never ends, DELETES the
    queue message so the cancelled compile is not redelivered and quietly run
    again, and removes the request so it cannot cancel the next compile of the
    same deal.
    """

    def __init__(
        self,
        blob_service: Any,
        job: Any,
        queue_client: Any,
        msg: Any,
        *,
        poll_sec: float = CANCEL_POLL_SEC,
    ) -> None:
        self._blob_service = blob_service
        self._job = job
        self._q = queue_client
        self._msg = msg
        # The same path `_do_compile` writes its live progress to. Derived
        # rather than passed, so this can be started beside the watchdog in the
        # outer function where the queue message is still in scope.
        self._progress_path = f"deals/{job.deal_id}/orbitbrief/latest/compile-progress.json"
        self._started_iso = _iso_now()
        self._poll = max(1.0, float(poll_sec))
        self._done = threading.Event()
        self._thread: threading.Thread | None = None
        self.fired = False

    def start(self) -> "_CancelWatcher":
        self._thread = threading.Thread(target=self._run, name="cancel-watcher", daemon=True)
        self._thread.start()
        return self

    def stop(self) -> None:
        self._done.set()

    def _request(self) -> dict[str, Any] | None:
        """The cancel request for THIS compile, or None.

        A request naming a different compile is not ours: the deal may have
        been re-queued since, and cancelling the wrong run is worse than
        cancelling none. A request naming no compile at all means "whatever is
        running on this deal", which is what a slot on a dashboard means.
        """
        try:
            blob = self._blob_service.get_blob_client(
                container=BLOB_CONTAINER, blob=_cancel_blob_path(self._job.deal_id),
            )
            doc = json.loads(blob.download_blob().readall())
        except Exception:
            return None
        if not isinstance(doc, dict):
            return None
        wanted = str(doc.get("compile_id") or "").strip()
        if wanted and wanted != str(self._job.compile_id):
            return None
        return doc

    def _run(self) -> None:
        while not self._done.wait(self._poll):
            req = self._request()
            if req is None:
                continue
            self.fired = True
            by = str(req.get("requested_by") or "?")
            log.error(
                "Compile cancelled by %s (deal=%s compile=%s). Freeing the slot now.",
                by, self._job.deal_id, self._job.compile_id,
            )
            # 1. Say it on the deal, in BOTH places: parser-jobs/ is this
            #    worker's own record, compile-progress.json is what the queue
            #    panel reads. Only the second one frees the slot on screen.
            try:
                _write_status(
                    self._blob_service, self._job, "cancelled",
                    stage="cancelled", cancelled_by=by,
                )
            except Exception as exc:  # pragma: no cover - best effort before exit
                log.error("Could not record the cancellation on the deal: %s", exc)
            try:
                self._blob_service.get_blob_client(
                    container=BLOB_CONTAINER, blob=self._progress_path,
                ).upload_blob(
                    json.dumps({
                        "compile_id": self._job.compile_id,
                        "deal_id": self._job.deal_id,
                        "status": "cancelled",
                        "current_stage": None,
                        "stages": [],
                        "started_at": self._started_iso,
                        "updated_at": _iso_now(),
                        "cancelled_by": by,
                        "triggered_by": self._job.triggered_by,
                        "worker_sha": WORKER_SHA,
                        "parser_os_sha": PARSER_OS_SHA,
                    }, indent=2).encode("utf-8"),
                    overwrite=True,
                    content_type="application/json",
                )
            except Exception as exc:  # pragma: no cover - best effort before exit
                log.error("Could not mark compile-progress cancelled: %s", exc)
            # 2. Drop the message. Without this the lease lapses and the
            #    compile somebody just cancelled is redelivered and run again.
            try:
                _safe_delete_queue_message(self._q, self._msg, context="cancelled")
            except Exception as exc:  # pragma: no cover - best effort before exit
                log.error("Could not delete the cancelled message: %s", exc)
            # 3. Remove the request, or it cancels this deal's NEXT compile
            #    the moment one starts.
            try:
                self._blob_service.get_blob_client(
                    container=BLOB_CONTAINER, blob=_cancel_blob_path(self._job.deal_id),
                ).delete_blob()
            except Exception:
                pass
            # Nothing is holding a half-written artifact: envelope and atoms are
            # written at the end of a compile, and the progress file above is
            # already consistent.
            try:
                _clear_compile_active(self._blob_service, self._job)
            except Exception:
                pass
            _INFLIGHT.clear()
            time.sleep(0.1)
            os._exit(0)


class _LeaseRenewer:
    """Keep a dequeued message invisible for as long as its compile is running.

    Azure Storage Queues have no server-side "I'm still working" signal; the
    only lease is the visibility timeout fixed at receive time. A compile that
    outlives it is redelivered and re-run in parallel. This thread calls
    update_message on a fixed cadence well inside the lease, and rebinds the
    message's pop_receipt each time -- every subsequent update AND the final
    delete must use the newest receipt or the queue rejects them.

    Failure inside here must never take the compile down: a renewal that
    errors is logged and retried on the next tick. LEASE_MAX_SEC is a backstop
    against a zombie holding a message forever, not a compile budget.
    """

    def __init__(self, queue_client: Any, msg: Any, *, every: int, max_total: int, lease: int) -> None:
        self._q = queue_client
        self._msg = msg
        self._every = max(30, int(every))
        self._max_total = max(self._every, int(max_total))
        self._lease = int(lease)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="lease-renewer", daemon=True)
        self.renewals = 0
        self.errors = 0
        self._started = time.monotonic()

    def start(self) -> "_LeaseRenewer":
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop.wait(self._every):
            if time.monotonic() - self._started > self._max_total:
                log.error(
                    "Lease renewer hit LEASE_MAX_SEC=%ds for message %s; letting the lease lapse.",
                    self._max_total, getattr(self._msg, "id", "?"),
                )
                return
            try:
                updated = self._q.update_message(
                    self._msg, pop_receipt=getattr(self._msg, "pop_receipt", None),
                    visibility_timeout=self._lease,
                )
                new_receipt = getattr(updated, "pop_receipt", None)
                if new_receipt:
                    try:
                        self._msg.pop_receipt = new_receipt
                    except Exception:
                        pass
                self.renewals += 1
                log.info(
                    "Renewed queue lease for message %s (renewal #%d, +%ds)",
                    getattr(self._msg, "id", "?"), self.renewals, self._lease,
                )
            except Exception as exc:
                self.errors += 1
                log.warning("Queue lease renewal failed (will retry next tick): %s", exc)


#: The one message this process currently holds, so a termination signal can
#: hand it back. Live 2026-09-03: the drain sentinel was removed once the roll
#: started, an OLD replica polled during its shutdown window, took a priority
#: message at 19:02:45 and was killed. Nothing released the lease, so the deal
#: sat at "running / discover_artifacts" for the full 30-minute visibility
#: timeout before anyone else could take it. A killed compile must cost seconds,
#: not half an hour, and it must say it was killed.
_INFLIGHT: dict[str, Any] = {}


def _compile_accepts_stage_start() -> bool:
    """True when the bundled parser-os exposes ``stage_start_callback``.

    The worker image can carry an older parser-os than this code expects;
    passing an unknown keyword would fail every compile, so ask first.
    """
    try:
        import inspect
        from app.core.compiler import compile_project as _cp
        return "stage_start_callback" in inspect.signature(_cp).parameters
    except Exception:
        return False


def _release_inflight(reason: str) -> None:
    """Make the held message visible again NOW and mark the compile interrupted.

    Best-effort and idempotent: every step is wrapped, because this runs from a
    signal handler on a process that is about to die either way.
    """
    st = dict(_INFLIGHT)
    _INFLIGHT.clear()
    if not st:
        return
    queue_client, msg, job, blob_service = (
        st.get("queue_client"), st.get("msg"), st.get("job"), st.get("blob_service"),
    )
    try:
        renewer = st.get("renewer")
        if renewer is not None:
            renewer.stop()
    except Exception:
        pass
    try:
        queue_client.update_message(
            msg, pop_receipt=getattr(msg, "pop_receipt", None), visibility_timeout=0,
        )
        log.warning(
            "Released queue lease for message %s (%s): visible again immediately",
            getattr(msg, "id", "?"), reason,
        )
    except Exception as exc:
        log.warning("Could not release queue lease on %s: %s", reason, exc)
    if job is None or blob_service is None:
        return
    # Out of the index before anything else. This runs when a deploy kills the
    # replica, which is the commonest way a marker would be orphaned -- and an
    # orphan is a compile the panel shows as running forever.
    _clear_compile_active(blob_service, job)
    try:
        blob_service.get_blob_client(
            container=BLOB_CONTAINER,
            blob=f"deals/{job.deal_id}/orbitbrief/latest/compile-progress.json",
        ).upload_blob(
            json.dumps({
                "compile_id": job.compile_id,
                "deal_id": job.deal_id,
                "status": "interrupted",
                "current_stage": None,
                "updated_at": _iso_now(),
                "error": f"worker terminated ({reason}); message released for retry",
                "worker_sha": WORKER_SHA,
                "parser_os_sha": PARSER_OS_SHA,
            }, indent=2, default=str).encode("utf-8"),
            overwrite=True,
            content_type="application/json",
        )
    except Exception:
        pass
    try:
        _write_status(
            blob_service, job, "interrupted", stage="terminated",
            error=f"worker terminated ({reason}); message released for retry",
        )
    except Exception:
        pass


def _install_termination_release() -> None:
    """SIGTERM/SIGINT -> release the in-flight message, then exit 143."""
    import signal

    def _handler(signum, _frame):
        name = getattr(signal, "Signals", None)
        try:
            label = name(signum).name if name else str(signum)
        except Exception:
            label = str(signum)
        _release_inflight(label)
        os._exit(143)

    for sig in (getattr(signal, "SIGTERM", None), getattr(signal, "SIGINT", None)):
        if sig is None:
            continue
        try:
            signal.signal(sig, _handler)
        except Exception as exc:  # pragma: no cover - non-main thread / platform
            log.warning("Could not install handler for signal %s: %s", sig, exc)


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


def _who_asked(d: dict[str, Any]) -> str | None:
    """Who asked for a compile, across both producers' shapes.

    The PM queue panel sends a flat ``triggered_by``. parser-os-service sends
    ``trigger: {"kind": "manual", "by": "griffin"}``. Reading only the first
    leaves every service-initiated compile anonymous, and the service is what
    auto-finalize goes through -- so that is most of them.

    Empty becomes None, not "": a blank byline reads as "nobody" rather than
    "we do not know".
    """
    flat = str(d.get("triggered_by") or d.get("triggeredBy") or "").strip()
    if flat:
        return flat
    trig = d.get("trigger")
    if isinstance(trig, dict):
        by = str(trig.get("by") or "").strip()
        if by:
            return by
        # A timer has no person behind it, and saying so is more useful than a
        # blank -- "nobody asked for this" is the answer to "why is this here".
        kind = str(trig.get("kind") or "").strip()
        if kind:
            return kind
    return None

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
    #: Who asked for this compile, and what kind of thing asked.
    #:
    #: The message has carried this all along and this class dropped it. Two
    #: producers, two shapes: the PM panel sets a flat ``triggered_by``, and
    #: parser-os-service sets ``trigger: {"kind": ..., "by": ...}`` -- the
    #: richer one, added precisely so a manual Re-parse and a four-hourly timer
    #: stop being the same message. Reading only the flat field would have left
    #: every service-initiated compile anonymous, which is most of them.
    triggered_by: str | None = None
    #: "manual", "timer", … from the service. None when the producer did not say.
    trigger_kind: str | None = None

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
            triggered_by=_who_asked(d),
            trigger_kind=(
                str((d.get("trigger") or {}).get("kind") or "").strip() or None
            ),
        )


# ─── Status surface (written to blob during compile) ──────────────────────


def _status_blob_path(job: JobMessage) -> str:
    return f"deals/{job.deal_id}/parser-jobs/{job.compile_id}.json"


def _read_status(blob_service: BlobServiceClient, job: JobMessage) -> dict[str, Any] | None:
    """The last status this worker wrote for this compile, or None."""
    try:
        blob_client = blob_service.get_blob_client(container=BLOB_CONTAINER, blob=_status_blob_path(job))
        data = blob_client.download_blob().readall()
        payload = json.loads(data if isinstance(data, str) else data.decode("utf-8", errors="replace"))
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _previous_attempt_timed_out(blob_service: BlobServiceClient, job: JobMessage) -> bool:
    """Did an earlier attempt at THIS compile run out of COMPILE_TIMEOUT_SEC?

    The watchdog records ``failed / timeout`` on the deal and kills the
    process; the queue then hands the same message to the next poll, which
    runs the same compile against the same budget and dies the same way --
    up to MAX_DEQUEUE_COUNT times. Measured on the dev worker (2026-09-15,
    Log Analytics): 5-12 such pickups an hour all day, each holding a replica
    for the full 25 minutes, while priority compiles queued behind them. A
    compile that has already exhausted its budget once will exhaust it
    again; it goes to the poison queue on the second sight, not the fourth.
    """
    prev = _read_status(blob_service, job)
    if not prev or prev.get("compile_id") != job.compile_id:
        return False
    return str(prev.get("status") or "") == "failed" and str(prev.get("stage") or "") == "timeout"


# A durable, findable record of every compile's outcome. The per-compile status
# blob above sits under its deal, so reading "every compile this week" meant
# listing every deal; and compile-progress.json is one file per deal that the
# next compile overwrites. On a final status the worker also writes a small
# entry under a date prefix, with the facts a reader needs in blob metadata, so
# PM Console › Admin › Models can list a day without downloading anything.
# The traceback stays in the per-deal blob; the index carries a short error.
RUN_INDEX_PREFIX = "_parser-runs"
_TERMINAL_STATUSES = frozenset({"completed", "failed", "interrupted"})
_RUN_INDEX_FIELDS = (
    "compile_id", "deal_id", "status", "stage", "updated_at", "worker_sha",
    "parser_os_sha", "elapsed_sec", "entity_count", "atom_count", "percent_complete",
)
_RUN_INDEX_ERROR_MAX = 500


def _run_index_blob_path(compile_id: str, updated_at: str) -> str:
    return f"{RUN_INDEX_PREFIX}/{updated_at[:10]}/{compile_id}.json"


def _metadata_value(value: Any, limit: int = 128) -> str:
    """Blob metadata travels as HTTP headers: printable ASCII only, kept short."""
    text = "" if value is None else str(value)
    return "".join(ch for ch in text if 32 <= ord(ch) < 127)[:limit]


def _run_index_metadata(payload: dict[str, Any]) -> dict[str, str]:
    keys = ("status", "stage", "deal_id", "compile_id", "updated_at", "elapsed_sec", "worker_sha")
    out = {k: _metadata_value(payload.get(k)) for k in keys}
    return {k: v for k, v in out.items() if v}


def _write_run_index(blob_service: BlobServiceClient, payload: dict[str, Any]) -> None:
    """Best effort: a failed index write is logged, never raised."""
    try:
        body = {k: payload[k] for k in _RUN_INDEX_FIELDS if k in payload}
        err = payload.get("error")
        if err:
            text = str(err)
            body["error"] = text if len(text) <= _RUN_INDEX_ERROR_MAX else text[: _RUN_INDEX_ERROR_MAX - 1] + "…"
        blob_service.get_blob_client(
            container=BLOB_CONTAINER,
            blob=_run_index_blob_path(str(payload["compile_id"]), str(payload["updated_at"])),
        ).upload_blob(
            json.dumps(body, indent=2, default=str),
            overwrite=True,
            content_type="application/json",
            metadata=_run_index_metadata(payload),
        )
    except Exception as exc:  # never let the index kill the worker
        log.warning("Failed to write run index: %s", exc)


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
    if status in _TERMINAL_STATUSES:
        _write_run_index(blob_service, payload)


# ─── Manifest + envelope read/write ───────────────────────────────────────


def _download_manifest(blob_service: BlobServiceClient, blob_url: str) -> dict[str, Any]:
    container, path = _blob_path_from_url(blob_url)
    client = blob_service.get_blob_client(container=container, blob=path)
    data = client.download_blob().readall()
    return json.loads(data)


def _manifest_as_of(manifest: dict[str, Any]) -> str | None:
    """The run cutoff this manifest was built with, or None for the full corpus."""
    ctx = manifest.get("context") if isinstance(manifest, dict) else None
    v = (ctx or {}).get("as_of") if isinstance(ctx, dict) else None
    v = str(v).strip() if v is not None else ""
    return v or None


def _norm_as_of(v: Any) -> str | None:
    v = str(v).strip() if v is not None else ""
    return v or None


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
        #
        # ...but keyed on the RUN SCOPE too. A cut compile (18 artifacts) and a
        # full one (72) are different products of the same deal. Keying on the
        # artifact set alone made each invalidate the other: after a cut, the
        # next no-op finalize enqueue read "artifacts changed", ran a FULL
        # recompile, and silently replaced the user's cut envelope (Marion
        # County, compile 07d69348, 16:14 UTC). A record for a different scope
        # is not evidence that THIS scope is up to date.
        return (
            rec.get("artifact_key") == artifact_key
            and _norm_as_of(rec.get("as_of")) == _norm_as_of(_manifest_as_of(manifest))
        )
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


def _upload_sheet_renders(blob_service, deal_id: str, project_dir) -> int:
    """Publish parser-os's `*.sheet.svg` renders so the UI can show a drawing.

    Best-effort by design: a picture is a convenience, and a compile that
    parsed a drawing correctly must not fail because its render could not be
    stored. Named by the SOURCE stem so a caller who knows the artifact
    filename can construct the URL without an index.
    """
    from pathlib import Path as _Path  # noqa: PLC0415

    count = 0
    try:
        for svg in _Path(project_dir).rglob("*.sheet.svg"):
            try:
                body = svg.read_text(encoding="utf-8")
            except Exception:  # noqa: BLE001
                continue
            stem = svg.name[: -len(".sheet.svg")]
            blob_service.get_blob_client(
                container=BLOB_CONTAINER,
                blob=f"deals/{deal_id}/parser-os/latest/sheets/{stem}.svg",
            ).upload_blob(body, overwrite=True, content_type="image/svg+xml")
            count += 1
    except Exception as exc:  # pragma: no cover - renders are additive
        log.warning("sheet render upload skipped: %s", exc)
    if count:
        log.info("Uploaded %d sheet render(s)", count)
    return count


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

    # Drop the manifest beside the artifacts so the compile can read its own
    # provenance. parser-os looks for `.parser_manifest.json` in project_dir
    # (orbitbrief_envelope.PARSER_MANIFEST_SIDECAR) to recover
    # `context.crm` — the deal name, account and HubSpot id the compile was
    # requested for. Nothing had ever written that file, so the read always
    # returned None and `summary.crm` was absent from every envelope we have
    # ever produced. Anything keyed on the deal's own identity therefore could
    # not work: the foreign-artifact check declines to guess without it, and
    # correctly emitted nothing on a deal visibly holding another deal's files.
    try:
        (project_dir / ".parser_manifest.json").write_text(
            json.dumps(manifest), encoding="utf-8"
        )
    except OSError as exc:  # a missing sidecar degrades, it must not fail a compile
        log.warning("could not write manifest sidecar: %s", exc)

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

        # The last thing the compile told us, so the heartbeat below can re-write
        # the SAME document between stage boundaries instead of inventing a
        # second one. One writer, one shape.
        _last_seen: dict[str, Any] = {"stage": None, "stages": [], "phase": "running"}

        def _stage_items() -> tuple[int, int]:
            """How far through its own work the running stage says it is.

            parser-os runs IN THIS PROCESS, so this reads the counter the
            compile is updating on its own thread. (0, 0) when the stage does
            not count -- most do not, and a made-up denominator is worse than
            an honest silence.
            """
            try:
                from app.core import telemetry as _t
                return _t.stage_progress()
            except Exception:
                return (0, 0)

        def _write_progress(current_stage, all_stages, *, phase):  # type: ignore[no-untyped-def]
            _last_seen["stage"] = current_stage
            _last_seen["stages"] = all_stages
            _last_seen["phase"] = phase
            _items_done, _items_total = _stage_items()
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
                # The stage running NOW (on start) or the one that just closed
                # (on end); `stage_phase` says which. Live 010300: the cockpit
                # sat on "discover_artifacts" through a 171-second parse
                # because only stage ends were written.
                "current_stage": current_stage,
                "stage_phase": phase,
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
                # Who asked for it. A running compile is no longer a queue
                # message, so this record is the only place left that knows.
                "triggered_by": job.triggered_by,
                "trigger_kind": job.trigger_kind,
                # HOW FAR THROUGH THE CURRENT STAGE.
                #
                # The median compile spends 47% of its wall clock inside its
                # single longest stage, and this document was only ever written
                # at stage boundaries -- so for about half of every compile
                # there was nothing to see. Ten estimators fitted against that
                # blindness topped out at 61% median error.
                #
                # With these two numbers the reader can measure a rate on THIS
                # run and do arithmetic instead of extrapolating from a corpus.
                "stage_items_done": _items_done,
                "stage_items_total": _items_total,
            }
            try:
                blob_service.get_blob_client(
                    container=BLOB_CONTAINER, blob=progress_path,
                ).upload_blob(
                    json.dumps(payload, indent=2, default=str).encode("utf-8"),
                    overwrite=True,
                    content_type="application/json",
                )
                log.info(
                    "compile-progress: %s %s (%d stage(s) done)",
                    phase, current_stage, len(done),
                )
            except Exception as exc:  # pragma: no cover — best-effort UX
                log.warning("compile-progress upload failed (%s %s): %s", phase, current_stage, exc)
            # Same tick keeps the index entry fresh, so "stale" means the same
            # thing to a reader whichever of the two it is looking at.
            _mark_compile_active(blob_service, job, stage=current_stage)

        # RE-WRITE THE SAME DOCUMENT WHILE A STAGE IS STILL RUNNING.
        #
        # Stage callbacks fire at boundaries, and the long stages are where all
        # the time goes -- typed_atom_classification is the longest stage in
        # 44% of compiles and ran 18.7 minutes on one deal without a word. This
        # thread re-writes the progress document on a short timer so the count
        # inside that stage actually reaches anyone.
        #
        # It writes nothing until a stage has reported once: before that there
        # is no shape to write, and an empty document would read as a compile
        # with no stages rather than one that has not spoken yet.
        def _heartbeat_tick() -> None:
            if _last_seen["stage"] is None:
                return
            _write_progress(_last_seen["stage"], _last_seen["stages"], phase=_last_seen["phase"])

        _progress_heartbeat = _ProgressHeartbeat(_heartbeat_tick, PROGRESS_HEARTBEAT_SEC).start()
        # Handed to the caller so its `finally` can stop this on EVERY exit.
        # A heartbeat that outlives the compile re-writes "running" over the
        # "done" this function is about to set, and the deal then shows as
        # compiling forever -- with results already on disk.
        _INFLIGHT["progress_stop"] = _progress_heartbeat

        def _on_stage_end(stage, all_stages):  # type: ignore[no-untyped-def]
            _write_progress(stage.stage_name, all_stages, phase="completed")

        def _on_stage_start(stage_name, all_stages):  # type: ignore[no-untyped-def]
            _write_progress(stage_name, all_stages, phase="running")

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
                    "triggered_by": job.triggered_by,
                    "trigger_kind": job.trigger_kind,
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
            **({"stage_start_callback": _on_stage_start} if _compile_accepts_stage_start() else {}),
        )
        elapsed = time.time() - t0
        log.info("compile_project done in %.1fs", elapsed)
        # The parser stages are over: stop re-writing the last one BEFORE the
        # projection/done writes below, or a tick lands on top of them and the
        # document reads "running quality_gates" after envelope.json exists.
        _progress_heartbeat.stop()

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

        # 4c. The drawings, as pictures a PM can actually open.
        #
        # parser-os renders every .dwg it converts to a vector SVG beside the
        # artifact. `project_dir` is a scratch directory this replica deletes
        # on exit, so without this the render is made and thrown away -- and
        # the labeling pane keeps showing "…SP-6.dwg can't be previewed
        # inline" beside a deal whose entire scope came off that sheet.
        _upload_sheet_renders(blob_service, job.deal_id, project_dir)

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
                    "as_of": _manifest_as_of(manifest),
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


from parser_os_worker.drain import drain_requested


def main() -> int:
    log.info(
        "parser-os-worker starting (worker_sha=%s parser_os_sha=%s)",
        WORKER_SHA,
        PARSER_OS_SHA,
    )
    _scrub_stale_worker_tmp()
    _install_termination_release()

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
    # ── drain gate ───────────────────────────────────────────────────────
    #
    # Rolling this Container App restarts it, killing whatever compile it is
    # running. That destroyed three compiles in one batch, and again on 09-02 a
    # recompile of 010215 was killed mid-run: it wrote a manifest and never an
    # envelope, which reads downstream as "the compile did nothing" rather than
    # "the compile was killed".
    #
    # The first guard simply waited for an idle worker before rolling. On a busy
    # dev environment that window never opened -- a deploy on 09-02 waited the
    # full 15 minutes while two unrelated deals compiled back to back, then
    # failed. Waiting for quiet cannot be relied on when work is continuous.
    #
    # So the deploy DRAINS instead: it writes a sentinel blob, this worker
    # finishes the message it already holds and then takes no new one, and the
    # roll happens against an idle process. The sentinel is removed immediately
    # after the roll.
    #
    # Checked here, before either queue is touched, so a message is never
    # dequeued and then abandoned. A message already in flight is unaffected --
    # this only stops the NEXT one.
    #
    # Fails open: if the sentinel cannot be read (permissions, transient blob
    # error) the worker keeps consuming. A drain that silently halts the queue
    # forever is worse than a roll that interrupts one compile.
    if drain_requested(blob_service, log):
        log.info("Drain sentinel present — not taking new work (in-flight work is unaffected).")
        return 0

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
    # From here until the message is deleted, a termination must hand it back.
    _INFLIGHT.update({"queue_client": queue_client, "msg": msg, "blob_service": blob_service})

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

    # A compile that already ran out of its budget once is not run again: it
    # would hold a replica for the full budget and die the same way.
    if msg.dequeue_count > 1 and _previous_attempt_timed_out(blob_service, job):
        log.error(
            "Compile %s for deal %s timed out on an earlier attempt (dequeue %d); forwarding to poison instead of re-running.",
            job.compile_id, job.deal_id, msg.dequeue_count,
        )
        _write_status(
            blob_service, job, "failed",
            stage="timeout",
            error=f"compile exceeded COMPILE_TIMEOUT_SEC on an earlier attempt; not re-run (dequeue_count {msg.dequeue_count})",
        )
        _forward_to_poison_queue(
            queue_service,
            raw_message=raw if isinstance(raw, str) else str(raw),
            reason="timed_out_before",
            job=job,
            dequeue_count=msg.dequeue_count,
            source_queue=source_queue,
        )
        _safe_delete_queue_message(queue_client, msg, context="timed out before")
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
    _INFLIGHT["job"] = job
    _write_status(blob_service, job, "running", stage="starting", percent_complete=0)
    # In the index from the moment work begins, so a reader never sees a gap
    # between "the queue message is gone" and "something is running".
    _mark_compile_active(blob_service, job, stage="starting")

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
        renewer = _LeaseRenewer(
            queue_client, msg,
            every=LEASE_RENEW_SEC, max_total=LEASE_MAX_SEC, lease=VISIBILITY_TIMEOUT_SEC,
        ).start()
        _INFLIGHT["renewer"] = renewer
        # The lease renewer keeps the message OURS while we work; it is not a
        # bound on the work. Without this the only ceiling was LEASE_MAX_SEC,
        # which merely stops renewing — the compile ran on regardless, and the
        # released message let the same deal start compiling on top of itself.
        budget = compile_budget_sec(manifest)
        log.info(
            "Compile budget %.0fs for %d document(s) (floor %ds, %.0fs/doc)",
            budget, len((manifest or {}).get("artifacts") or []),
            COMPILE_TIMEOUT_SEC, COMPILE_SEC_PER_DOC,
        )
        watchdog = _CompileWatchdog(blob_service, job, budget).start()
        # Somebody can ask for this compile to stop while it runs. Started here,
        # beside the watchdog, because it needs the queue message: a cancelled
        # compile whose message survives is simply run again a minute later.
        canceller = _CancelWatcher(blob_service, job, queue_client, msg).start()
        try:
            result = _do_compile(job, manifest, blob_service)
        finally:
            # Stop the progress heartbeat FIRST. It re-writes the progress
            # document on a timer, and one more tick after this point would
            # overwrite the terminal status with a stale "running".
            _ps = _INFLIGHT.pop("progress_stop", None)
            if _ps is not None:
                _ps.set()
            # However this ended -- done, raised, timed out -- it is no longer
            # running, and a marker left behind is a compile the panel shows as
            # running forever.
            _clear_compile_active(blob_service, job)
            canceller.stop()
            watchdog.stop()
            renewer.stop()
            _INFLIGHT.pop("renewer", None)
            if renewer.renewals or renewer.errors:
                log.info(
                    "Lease renewer: %d renewal(s), %d error(s) over the compile",
                    renewer.renewals, renewer.errors,
                )

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
        _INFLIGHT.clear()
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
