"""A compile somebody asked to stop stops, in seconds rather than stages.

Dragging a deal onto a busy slot means "run mine instead of that one", and
until this existed nothing could honour it: `cancel` appeared nowhere in this
worker, and `cancelQueuedCompile` on the API side only drains messages that
have not started yet. A running compile could not be stopped by anybody.

The obvious hook would be `stage_start_callback`, which already fires around
every stage. But a cancel checked between stages is only as responsive as the
longest stage, and `enrich_entities` is 669 of 673 seconds on a models-on
compile -- so the reply to "stop that one" could take eleven minutes. That is
not a cancel. Hence a short-poll thread and a hard exit.
"""

from __future__ import annotations

import json
import time
from types import SimpleNamespace

import pytest

from parser_os_worker import main as m


@pytest.fixture
def job():
    return SimpleNamespace(deal_id="d1", compile_id="c1")


class FakeBlob:
    def __init__(self, store, name):
        self._store = store
        self._name = name

    def download_blob(self):
        if self._name not in self._store:
            raise RuntimeError("BlobNotFound")
        return SimpleNamespace(readall=lambda: self._store[self._name])

    def upload_blob(self, data, **_kw):
        self._store[self._name] = data

    def delete_blob(self):
        self._store.pop(self._name, None)


class FakeBlobService:
    def __init__(self, store):
        self.store = store

    def get_blob_client(self, container=None, blob=None):
        return FakeBlob(self.store, blob)


class FakeQueue:
    def __init__(self):
        self.deleted = []

    def delete_message(self, msg):
        self.deleted.append(msg)


def _request(store, deal_id, **body):
    store[m._cancel_blob_path(deal_id)] = json.dumps(body).encode("utf-8")


def _run_until(watcher, predicate, timeout=3.0):
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def test_a_compile_nobody_cancelled_is_left_alone(job, monkeypatch):
    exits: list[int] = []
    monkeypatch.setattr(m.os, "_exit", lambda code: exits.append(code))
    q = FakeQueue()
    w = m._CancelWatcher(FakeBlobService({}), job, q, object(), poll_sec=1).start()
    time.sleep(0.05)
    w.stop()
    assert w.fired is False
    assert exits == []
    assert q.deleted == []


def test_a_cancelled_compile_stops_and_says_so(job, monkeypatch):
    exits: list[int] = []
    monkeypatch.setattr(m.os, "_exit", lambda code: exits.append(code))
    store: dict = {}
    _request(store, "d1", compile_id="c1", requested_by="griffin@purtera-it.com")
    q = FakeQueue()
    msg = object()
    w = m._CancelWatcher(FakeBlobService(store), job, q, msg, poll_sec=1).start()
    assert _run_until(w, lambda: exits), "the watcher never fired"

    progress = json.loads(store["deals/d1/orbitbrief/latest/compile-progress.json"])
    # The panel reads compile-progress.json, and ONLY this frees the slot on
    # screen -- parser-jobs/ is the worker's own record and nothing renders it.
    assert progress["status"] == "cancelled"
    assert progress["cancelled_by"] == "griffin@purtera-it.com"
    # Without the delete, the lease lapses and the compile somebody just
    # cancelled is redelivered and quietly run again.
    assert q.deleted == [msg]
    # And the request is gone, or it cancels this deal's NEXT compile the
    # moment one starts.
    assert m._cancel_blob_path("d1") not in store


def test_a_request_naming_a_DIFFERENT_compile_is_not_ours(job, monkeypatch):
    exits: list[int] = []
    monkeypatch.setattr(m.os, "_exit", lambda code: exits.append(code))
    store: dict = {}
    # The deal was re-queued since somebody asked; cancelling the wrong run is
    # worse than cancelling none.
    _request(store, "d1", compile_id="some-older-compile", requested_by="x")
    q = FakeQueue()
    w = m._CancelWatcher(FakeBlobService(store), job, q, object(), poll_sec=1).start()
    time.sleep(1.4)
    w.stop()
    assert exits == []
    assert q.deleted == []
    assert m._cancel_blob_path("d1") in store, "somebody else's request was eaten"


def test_a_request_naming_no_compile_means_whatever_is_running(job, monkeypatch):
    # What a slot on a dashboard means: stop the thing in THAT box.
    exits: list[int] = []
    monkeypatch.setattr(m.os, "_exit", lambda code: exits.append(code))
    store: dict = {}
    _request(store, "d1", requested_by="griffin@purtera-it.com")
    q = FakeQueue()
    w = m._CancelWatcher(FakeBlobService(store), job, q, object(), poll_sec=1).start()
    assert _run_until(w, lambda: exits), "the watcher never fired"
    assert q.deleted


def test_a_stopped_watcher_does_not_kill_a_finished_compile(job, monkeypatch):
    # The compile finished and the request arrives a moment later. Exiting then
    # would kill a replica that is writing its envelope.
    exits: list[int] = []
    monkeypatch.setattr(m.os, "_exit", lambda code: exits.append(code))
    store: dict = {}
    q = FakeQueue()
    w = m._CancelWatcher(FakeBlobService(store), job, q, object(), poll_sec=1).start()
    w.stop()
    _request(store, "d1", compile_id="c1", requested_by="x")
    time.sleep(1.3)
    assert exits == []
