"""One small blob says a compile is running, so nobody has to sweep 500 deals.

The queue panel answered "what is compiling?" by listing every deal folder and
reading all ~500 progress documents. Measured against live dev that is 911ms
just to LIST the folders, before a single read -- so the endpoint took 4-6s on
a consumption plan while the panel polled every 2s.

The worker already knows. It writes a marker when a compile starts and deletes
it when it stops, and the reader lists one small prefix: 231ms for the listing,
122ms for four parallel reads.

The marker is an INDEX, never the truth. The progress document stays the
record. So these tests are about one thing above all: a marker must not
outlive the compile it describes, because an orphan is a compile the panel
shows as running forever -- and that is the exact failure the 45-minute stale
window already caused once.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from parser_os_worker import main as m


@pytest.fixture
def job():
    return SimpleNamespace(deal_id="d1", compile_id="c1", triggered_by="griffin@purtera-it.com")


class FakeBlob:
    def __init__(self, store, name):
        self._store, self._name = store, name

    def upload_blob(self, data, **_kw):
        self._store[self._name] = data

    def delete_blob(self):
        if self._name not in self._store:
            raise RuntimeError("BlobNotFound")
        del self._store[self._name]

    def download_blob(self):
        if self._name not in self._store:
            raise RuntimeError("BlobNotFound")
        return SimpleNamespace(readall=lambda: self._store[self._name])


class FakeBlobService:
    def __init__(self, store):
        self.store = store

    def get_blob_client(self, container=None, blob=None):
        return FakeBlob(self.store, blob)


def test_a_running_compile_is_in_the_index(job):
    store: dict = {}
    m._mark_compile_active(FakeBlobService(store), job, stage="enrich_entities")
    path = m._active_index_path("c1")
    assert path in store
    doc = json.loads(store[path])
    assert doc["compile_id"] == "c1"
    assert doc["deal_id"] == "d1"
    assert doc["stage"] == "enrich_entities"
    assert doc["updated_at"]


def test_it_is_keyed_by_COMPILE_not_by_deal(job):
    # A deal can legitimately have two compiles in flight; keying on the deal
    # would have one silently erase the other, and the panel would show one
    # where there are two.
    store: dict = {}
    svc = FakeBlobService(store)
    m._mark_compile_active(svc, SimpleNamespace(deal_id="d1", compile_id="first"))
    m._mark_compile_active(svc, SimpleNamespace(deal_id="d1", compile_id="second"))
    assert len(store) == 2


def test_clearing_takes_it_out(job):
    store: dict = {}
    svc = FakeBlobService(store)
    m._mark_compile_active(svc, job)
    m._clear_compile_active(svc, job)
    assert store == {}


def test_clearing_twice_is_not_an_error(job):
    # It is called from a finally, from the cancel path and from the watchdog;
    # a double-clear is the normal case and must never mask the real outcome.
    store: dict = {}
    svc = FakeBlobService(store)
    m._mark_compile_active(svc, job)
    m._clear_compile_active(svc, job)
    m._clear_compile_active(svc, job)
    assert store == {}


def test_a_write_that_fails_does_not_raise(job):
    # The index must never be able to fail a compile. Losing a marker costs a
    # place in a fast listing, not an existence -- the reader's periodic sweep
    # still finds it.
    class Broken(FakeBlobService):
        def get_blob_client(self, container=None, blob=None):
            raise RuntimeError("storage is having a day")

    m._mark_compile_active(Broken({}), job)
    m._clear_compile_active(Broken({}), job)


def test_the_prefix_is_flat_and_separate_from_deal_data(job):
    # It has to be listable on its own; under deals/ it would be back inside
    # the 500-folder walk this exists to avoid.
    assert m._active_index_path("c1").startswith(m.ACTIVE_INDEX_PREFIX + "/")
    assert not m._active_index_path("c1").startswith("deals/")


class TestTheProgressHeartbeatIsBounded:
    """The heartbeat must not outlive the compile it reports on.

    It re-writes the progress document on a timer so the count INSIDE a long
    stage reaches the panel -- the median compile spends 47% of its wall clock
    in one stage, and this document was only ever written at stage boundaries.

    One tick after the compile ends would overwrite the terminal status with a
    stale "running", and the deal would show as compiling forever with its
    results already on disk. That is the same shape as the 45-minute stale
    window and the orphaned index marker: a reporting thread telling the truth
    about a moment that has passed.
    """

    def test_the_interval_is_short_enough_to_read_a_rate_from(self):
        # A rate needs several samples inside one stage. At 4s a two-minute
        # stage yields ~30 of them; at 30s it would yield four.
        assert 1.0 <= m.PROGRESS_HEARTBEAT_SEC <= 10.0

    def test_the_stop_event_is_published_for_the_caller(self, monkeypatch):
        # The stop lives in _INFLIGHT precisely so the OUTER finally -- which
        # already runs on every exit, including a raise and a timeout -- can
        # end it without _do_compile needing its own try/finally.
        import inspect
        src = inspect.getsource(m)
        assert '_INFLIGHT["progress_stop"] = _progress_heartbeat' in src
        assert '_ps = _INFLIGHT.pop("progress_stop", None)' in src

    def test_stage_items_are_read_from_the_parser_in_this_process(self):
        # parser-os runs in-process, so the worker reads the very counter the
        # compile is updating on another thread. No IPC, no file, no guess.
        import inspect
        src = inspect.getsource(m)
        assert "from app.core import telemetry as _t" in src
        assert "_t.stage_progress()" in src
