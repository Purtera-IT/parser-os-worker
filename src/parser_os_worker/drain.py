"""Whether a deploy has asked this worker to stop taking new messages.

Rolling parser-os-worker-warm restarts it, killing whatever compile it is
running. That destroyed three compiles in one batch, and again on 2026-09-02 a
recompile of deal 010215 was killed mid-run -- it wrote a manifest and never an
envelope, which reads downstream as "the compile did nothing" rather than "the
compile was killed".

The first guard WAITED for an idle worker before rolling. On a busy environment
that window never opens: a deploy waited the full 15 minutes while two unrelated
deals compiled back to back, then failed. Waiting for quiet cannot be relied on
when work is continuous.

So the deploy drains instead: it writes a sentinel, the worker finishes the
message it already holds and takes no new one, and the roll happens against an
idle process.

This lives apart from main.py so it can be tested without the Azure SDK. It
takes a blob service rather than building one, so it has no import of its own.
"""

from __future__ import annotations

import os

BLOB_CONTAINER = os.environ.get("AZURE_STORAGE_BLOB_CONTAINER", "orbitbrief-artifacts")
DRAIN_SENTINEL_BLOB = os.environ.get("PARSER_OS_DRAIN_BLOB", "control/parser-os-worker.drain")


def drain_requested(blob_service, log) -> bool:
    """True when a deploy has asked this worker to stop taking new messages.

    Fails OPEN on purpose. The cost of a false negative is one interrupted
    compile. The cost of a false positive is a silently stalled pipeline nobody
    is watching -- the worker would stop taking work forever and every deal
    would simply look idle. An unreadable sentinel must never halt the queue.
    """
    try:
        return blob_service.get_blob_client(
            container=BLOB_CONTAINER, blob=DRAIN_SENTINEL_BLOB
        ).exists()
    except Exception as exc:  # noqa: BLE001 - see docstring
        log.warning("Drain sentinel check failed (%s); continuing to consume.", exc)
        return False
