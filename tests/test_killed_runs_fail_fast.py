"""A compile that dies without a word still ends as "failed", at once.

Deal 010264, 2026-10-13: the compile was killed for memory (exit 137) three
times. Each time compile-progress.json stayed "running", which greys out
Re-parse and Clear; the queue handed the same message back half an hour later
to die the same way; and after the third try the poison forward raised "Queue
already exists", so the job was dropped while the page kept waiting on it.
"""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

from parser_os_worker import main as m

PROGRESS = "deals/d1/orbitbrief/latest/compile-progress.json"


class _Blobs:
    """Just enough of BlobServiceClient: blobs are dicts, etags are versions."""

    def __init__(self, blobs: dict[str, dict] | None = None):
        self.blobs = {k: json.dumps(v).encode() for k, v in (blobs or {}).items()}
        self.versions = {k: 1 for k in self.blobs}
        self.deleted: list[str] = []

    def json(self, name):
        return json.loads(self.blobs[name])

    def get_container_client(self, container):
        outer = self

        class _C:
            def list_blobs(self, name_starts_with=""):
                return [SimpleNamespace(name=n) for n in list(outer.blobs) if n.startswith(name_starts_with)]

        return _C()

    def get_blob_client(self, container, blob):
        outer = self

        class _B:
            def download_blob(self):
                if blob not in outer.blobs:
                    raise ResourceNotFoundError("missing")
                data, version = outer.blobs[blob], outer.versions[blob]
                return SimpleNamespace(readall=lambda: data, properties=SimpleNamespace(etag=str(version)))

            def upload_blob(self, data, overwrite=False, content_type=None, etag=None, match_condition=None, **_):
                if etag is not None and str(outer.versions.get(blob)) != str(etag):
                    raise RuntimeError("ConditionNotMet")
                outer.blobs[blob] = data if isinstance(data, bytes) else data.encode()
                outer.versions[blob] = outer.versions.get(blob, 0) + 1

            def delete_blob(self):
                outer.blobs.pop(blob, None)
                outer.deleted.append(blob)

        return _B()


def _marker(compile_id="c1", replica=None, boot_id="old-boot", stage="source_replay"):
    return {
        "compile_id": compile_id, "deal_id": "d1", "stage": stage,
        "replica": replica or m._REPLICA, "boot_id": boot_id,
    }


def _running(compile_id="c1", done=4283, total=16001):
    return {
        "compile_id": compile_id, "deal_id": "d1", "status": "running",
        "current_stage": "source_replay", "stage_items_done": done, "stage_items_total": total,
        "stages": [{"stage_name": "parse_artifacts", "duration_ms": 1.0}],
    }


@pytest.fixture
def statuses(monkeypatch):
    seen: list[tuple] = []
    monkeypatch.setattr(m, "_write_status", lambda bs, job, status, **kw: seen.append((job.compile_id, status, kw.get("stage"))))
    return seen


def test_a_restarted_container_fails_its_predecessors_run_at_once(statuses):
    blobs = _Blobs({"_active-compiles/c1.json": _marker(), PROGRESS: _running()})

    assert m.recover_dead_runs(blobs) == 1

    progress = blobs.json(PROGRESS)
    assert progress["status"] == "failed"
    assert progress["stage"] == "killed"
    assert "source_replay at 4,283 of 16,001 items" in progress["error"]
    assert "Re-parse" in progress["error"]
    assert progress["stages"], "what the run did finish is still shown"
    assert statuses == [("c1", "failed", "killed")]
    assert "_active-compiles/c1.json" not in blobs.blobs


def test_another_replicas_run_is_left_alone(statuses):
    blobs = _Blobs({"_active-compiles/c1.json": _marker(replica="someone-else"), PROGRESS: _running()})
    assert m.recover_dead_runs(blobs) == 0
    assert blobs.json(PROGRESS)["status"] == "running"
    assert statuses == []


def test_this_processs_own_run_is_left_alone(statuses):
    blobs = _Blobs({"_active-compiles/c1.json": _marker(boot_id=m._BOOT_ID), PROGRESS: _running()})
    assert m.recover_dead_runs(blobs) == 0
    assert blobs.json(PROGRESS)["status"] == "running"


def test_a_newer_compile_on_the_same_deal_is_never_failed_for_an_old_one(statuses):
    """The user pressed Re-parse: the progress document is the new run's now."""
    blobs = _Blobs({"_active-compiles/c1.json": _marker(), PROGRESS: _running(compile_id="c2")})
    m.recover_dead_runs(blobs)
    assert blobs.json(PROGRESS)["compile_id"] == "c2"
    assert blobs.json(PROGRESS)["status"] == "running"


def test_the_progress_write_loses_to_a_compile_that_starts_mid_write():
    blobs = _Blobs({PROGRESS: _running()})
    real = blobs.get_blob_client

    def racing(container, blob):
        client = real(container, blob)
        download = client.download_blob

        def download_then_new_compile_writes():
            d = download()
            blobs.blobs[PROGRESS] = json.dumps(_running(compile_id="c2")).encode()
            blobs.versions[PROGRESS] += 1
            return d

        client.download_blob = download_then_new_compile_writes
        return client

    blobs.get_blob_client = racing
    job = SimpleNamespace(deal_id="d1", compile_id="c1")
    assert m._fail_progress_if_ours(blobs, job, stage="killed", error="x") is False
    assert json.loads(blobs.blobs[PROGRESS])["compile_id"] == "c2"


def test_a_missing_progress_document_is_written():
    blobs = _Blobs()
    job = SimpleNamespace(deal_id="d1", compile_id="c1")
    assert m._fail_progress_if_ours(blobs, job, stage="exhausted_retries", error="gave up")
    assert blobs.json(PROGRESS)["status"] == "failed"


@pytest.mark.parametrize("stage", ["killed", "out_of_memory", "timeout"])
def test_a_run_that_cannot_succeed_is_not_run_again(stage):
    job = SimpleNamespace(deal_id="d1", compile_id="c1")
    blobs = _Blobs({"deals/d1/parser-jobs/c1.json": {"compile_id": "c1", "status": "failed", "stage": stage}})
    assert m._previous_attempt_cannot_succeed(blobs, job) == stage


@pytest.mark.parametrize("stage", ["exception", "terminated", "done"])
def test_an_ordinary_failure_is_still_retried(stage):
    job = SimpleNamespace(deal_id="d1", compile_id="c1")
    blobs = _Blobs({"deals/d1/parser-jobs/c1.json": {"compile_id": "c1", "status": "failed", "stage": stage}})
    assert m._previous_attempt_cannot_succeed(blobs, job) is None


def test_poison_forward_survives_an_existing_poison_queue(monkeypatch):
    sent: list[str] = []

    class _Q:
        def create_queue(self):
            raise ResourceExistsError("The specified queue already exists.")

        def send_message(self, body, **_):
            sent.append(body)

    qs = SimpleNamespace(get_queue_client=lambda name, **kw: _Q())
    m._forward_to_poison_queue(qs, raw_message="{}", reason="exhausted_retries", dequeue_count=4)
    assert len(sent) == 1, "the message must be archived, not dropped"
    assert json.loads(sent[0])["reason"] == "exhausted_retries"


def test_the_memory_guard_fails_the_run_before_the_kernel_kills_it(monkeypatch, statuses):
    exits: list[int] = []
    monkeypatch.setattr(m.os, "_exit", lambda code: exits.append(code))
    blobs = _Blobs({PROGRESS: _running()})
    job = SimpleNamespace(deal_id="d1", compile_id="c1")
    guard = m._MemoryGuard(blobs, job, limit_bytes=8 * 2**30, fraction=0.9, poll_sec=0.01,
                           rss=lambda: int(7.5 * 2**30)).start()
    import time
    deadline = time.time() + 3
    while not exits and time.time() < deadline:
        time.sleep(0.01)
    guard.stop()
    assert exits == [75]
    progress = blobs.json(PROGRESS)
    assert progress["status"] == "failed" and progress["stage"] == "out_of_memory"
    assert "7.5 GB used of the worker's 8.0 GB" in progress["error"]
    assert ("c1", "failed", "out_of_memory") in statuses


def test_the_memory_guard_leaves_a_run_under_its_limit_alone(monkeypatch):
    exits: list[int] = []
    monkeypatch.setattr(m.os, "_exit", lambda code: exits.append(code))
    guard = m._MemoryGuard(_Blobs(), SimpleNamespace(deal_id="d1", compile_id="c1"),
                           limit_bytes=8 * 2**30, fraction=0.9, poll_sec=0.01,
                           rss=lambda: 2 * 2**30).start()
    import time
    time.sleep(0.1)
    guard.stop()
    assert exits == [] and guard.fired is False


def test_the_keepalive_is_sent_while_a_compile_runs_and_deleted_after(monkeypatch):
    calls: list[tuple] = []

    class _Q:
        def create_queue(self):
            raise ResourceExistsError("exists")

        def send_message(self, body, time_to_live=None):
            calls.append(("send", json.loads(body)["compile_id"], time_to_live))
            return SimpleNamespace(id="k1", pop_receipt="p1")

        def delete_message(self, msg_id, pop_receipt=None):
            calls.append(("delete", msg_id, pop_receipt))

    monkeypatch.setattr(m, "INFLIGHT_QUEUE_NAME", "parser-os-compile-inflight")
    qs = SimpleNamespace(get_queue_client=lambda name, **kw: _Q())
    keep = m._send_keepalive(qs, SimpleNamespace(deal_id="d1", compile_id="c1"))
    m._delete_keepalive(keep)
    assert calls == [("send", "c1", m.LEASE_MAX_SEC), ("delete", "k1", "p1")]


def test_no_keepalive_without_the_queue_configured(monkeypatch):
    monkeypatch.setattr(m, "INFLIGHT_QUEUE_NAME", "")
    assert m._send_keepalive(object(), SimpleNamespace(deal_id="d1", compile_id="c1")) is None


def test_the_active_marker_says_which_process_wrote_it():
    blobs = _Blobs()
    m._mark_compile_active(blobs, SimpleNamespace(deal_id="d1", compile_id="c1"), stage="source_replay")
    marker = blobs.json("_active-compiles/c1.json")
    assert marker["replica"] == m._REPLICA and marker["boot_id"] == m._BOOT_ID
