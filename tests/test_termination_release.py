"""A terminated worker hands its message back immediately and says so.

Live 2026-09-03: an old replica took a priority message during its shutdown
window and was killed. Nothing released the lease, so the deal showed
"running / discover_artifacts" for the full 30-minute visibility timeout
before another consumer could take it.
"""
from __future__ import annotations

import json

import parser_os_worker.main as m


class _Queue:
    def __init__(self):
        self.calls = []

    def update_message(self, msg, pop_receipt=None, visibility_timeout=None):
        self.calls.append({"id": msg.id, "pop_receipt": pop_receipt, "visibility_timeout": visibility_timeout})
        return msg


class _Msg:
    id = "m1"
    pop_receipt = "r1"


class _Blob:
    def __init__(self):
        self.uploads = {}

    def get_blob_client(self, container, blob):
        outer = self

        class _C:
            def upload_blob(self_inner, data, overwrite=False, content_type=None):
                outer.uploads[blob] = json.loads(data.decode("utf-8"))

        return _C()


class _Job:
    deal_id = "deal-1"
    compile_id = "cmp-1"


def test_release_makes_message_visible_and_marks_interrupted(monkeypatch):
    q, blob = _Queue(), _Blob()
    statuses = []
    monkeypatch.setattr(m, "_write_status", lambda bs, job, status, **kw: statuses.append((status, kw.get("stage"))))
    m._INFLIGHT.clear()
    m._INFLIGHT.update({"queue_client": q, "msg": _Msg(), "job": _Job(), "blob_service": blob})

    m._release_inflight("SIGTERM")

    assert q.calls == [{"id": "m1", "pop_receipt": "r1", "visibility_timeout": 0}]
    progress = blob.uploads["deals/deal-1/orbitbrief/latest/compile-progress.json"]
    assert progress["status"] == "interrupted"
    assert "SIGTERM" in progress["error"]
    assert statuses == [("interrupted", "terminated")]
    assert m._INFLIGHT == {}, "release must be one-shot"


def test_release_is_a_noop_when_nothing_is_held(monkeypatch):
    m._INFLIGHT.clear()
    monkeypatch.setattr(m, "_write_status", lambda *a, **k: (_ for _ in ()).throw(AssertionError("must not write")))
    m._release_inflight("SIGTERM")


def test_release_survives_a_failing_queue(monkeypatch):
    class _Bad:
        def update_message(self, *a, **k):
            raise RuntimeError("gone")

    blob = _Blob()
    monkeypatch.setattr(m, "_write_status", lambda *a, **k: None)
    m._INFLIGHT.clear()
    m._INFLIGHT.update({"queue_client": _Bad(), "msg": _Msg(), "job": _Job(), "blob_service": blob})
    m._release_inflight("SIGINT")
    assert blob.uploads["deals/deal-1/orbitbrief/latest/compile-progress.json"]["status"] == "interrupted"
