"""The worker records the parser's compile_state, never a silent "done".

parser-os (PR #425) sets ``compile_capabilities["compile_state"]``:
``failed`` (zero evidence atoms) or ``degraded`` (a C3 heads read fell back to
the LLM, with ``c3_failures``). The worker used to write "done"/"completed"
regardless.
"""

from __future__ import annotations

import inspect
import json
import types

import pytest

from parser_os_worker import main as m


class _QueueService:
    def __init__(self):
        self.sent: list[str] = []

    def get_queue_client(self, name, message_encode_policy=None):
        return types.SimpleNamespace(send_message=self.sent.append)


def _job() -> m.JobMessage:
    return m.JobMessage.from_raw(json.dumps({
        "compile_id": "c-1", "deal_id": "d-1",
        "manifest_blob_url": "https://x/parser-manifests/c-1.json",
        "compile_options": {},
    }))


@pytest.fixture
def http_calls(monkeypatch):
    import urllib.request
    calls: list[str] = []
    monkeypatch.setattr(urllib.request, "build_opener", lambda *a, **k: calls.append("opener"))
    monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **k: calls.append("urlopen"))
    monkeypatch.setattr(m, "BRIEF_GEN_QUEUE_NAME", "parser-os-orbitbrief-jobs")
    return calls


def _result(caps):
    return types.SimpleNamespace(compile_capabilities=caps, atoms=[])


@pytest.mark.parametrize(
    "caps, state",
    [
        (None, "ok"),  # older parser: no verdict -> previous behaviour
        ({}, "ok"),
        ({"compile_state": "ok"}, "ok"),
        ({"compile_state": "degraded", "c3_failures": [{"where": "type", "state": "unreachable", "reason": ""}]}, "degraded"),
        ({"compile_state": "failed"}, "failed"),
        ({"compile_state": "something_new"}, "degraded"),  # unknown is never "ok"
    ],
)
def test_compile_state_is_read_from_the_parser(caps, state):
    got, failures = m._compile_state(_result(caps))
    assert got == state
    if state == "degraded" and caps and caps.get("c3_failures"):
        assert failures and failures[0]["where"] == "type"


def test_fields_the_pm_page_reads():
    f = m._compile_state_fields("degraded", [{"where": "type"}])
    assert f == {"compile_state": "degraded", "degraded": True, "c3_failures": [{"where": "type"}]}
    assert m._compile_state_fields("ok", []) == {"compile_state": "ok", "degraded": False}


def test_a_failed_compile_stops_before_envelope_and_done():
    src = inspect.getsource(m._do_compile)
    check = src.index('if compile_state == "failed":')
    raise_ = src.index("raise CompileStateFailed(")
    assert src.index("_progress_heartbeat.stop()") < check < raise_
    assert raise_ < src.index("build_orbitbrief_envelope(")
    assert raise_ < src.index("_upload_envelope(")
    assert raise_ < src.index('_write_compile_progress("done"')
    # every progress write carries the verdict
    assert "**_compile_state_fields(compile_state, c3_failures, c3_notices)" in src


def test_a_failed_compile_is_recorded_failed_and_not_retried():
    src = inspect.getsource(m.main)
    assert "state_failed = isinstance(exc, CompileStateFailed)" in src
    assert "permanent = state_failed or (" in src
    assert '"no_evidence" if state_failed' in src
    # the success branch (status completed + notify) records the state too
    done = src.index('"completed"')
    notify = src.index("_notify_orbitbrief_core(")
    assert done < src.index("_compile_state_fields(", done) < notify
    assert 'compile_state=result.get("compile_state", "ok")' in src


@pytest.mark.parametrize("state", ["ok", "degraded"])
def test_the_queue_message_carries_the_state(http_calls, state):
    qs = _QueueService()
    m._notify_orbitbrief_core(qs, _job(), compile_state=state)
    doc = json.loads(qs.sent[0])
    assert doc["compileState"] == state
    assert doc["degraded"] is (state == "degraded")
    assert len(qs.sent) == 1 and http_calls == []


# ------------------------------------------------ heads notices (deal page banners)

def test_notices_ride_on_the_fields_the_pm_page_reads():
    deg = {"lines": 9000, "max_atoms": 4096, "max_atoms_top": 8192, "oom_retries": 1, "reads": 1}
    f = m._compile_state_fields("degraded", [], {"heads_read_degraded": deg})
    assert f == {"compile_state": "degraded", "degraded": True, "notices": {"heads_read_degraded": deg}}
    # Nothing to say: no key at all (a warm, full-size read shows nothing).
    assert "notices" not in m._compile_state_fields("ok", [], {})


def test_a_finished_compile_keeps_its_read_notices_but_never_a_wait():
    caps = {"compile_state": "degraded", "c3_notices": {
        "heads_read_degraded": {"lines": 9000, "max_atoms": 2048},
        "large_deal": {"lines": 9000, "threshold": 8000},
        "heads_warming": {"since": "2026-10-27T00:00:00+00:00"},  # stale: the wait ended
        "something_else": {"x": 1}}}
    assert set(m._compile_notices(_result(caps))) == {"heads_read_degraded", "large_deal"}
    assert m._compile_notices(_result(None)) == {}
    assert m._compile_notices(_result({"c3_notices": "nonsense"})) == {}


def test_live_notices_are_read_from_the_running_parser(monkeypatch):
    import sys

    fake = types.SimpleNamespace(compile_notices=lambda: {
        "heads_warming": {"since": "2026-10-27T00:00:00+00:00", "waited_s": 63.0, "attempts": 4},
        "unrelated": {"x": 1}})
    monkeypatch.setitem(sys.modules, "app.core.telemetry", fake)
    monkeypatch.setitem(sys.modules, "app.core", types.SimpleNamespace(telemetry=fake))
    assert set(m._live_notices()) == {"heads_warming"}


def test_live_notices_from_a_parser_without_them_are_empty(monkeypatch):
    import sys

    fake = types.SimpleNamespace()  # an older parser: no compile_notices
    monkeypatch.setitem(sys.modules, "app.core.telemetry", fake)
    monkeypatch.setitem(sys.modules, "app.core", types.SimpleNamespace(telemetry=fake))
    assert m._live_notices() == {}


def test_every_progress_write_carries_the_notices():
    src = inspect.getsource(m._do_compile)
    live = src.index("_notices = _live_notices()")
    assert src.index('"stage_items_total": _items_total,') < live < src.index("upload_blob(", live)
    assert "_compile_state_fields(compile_state, c3_failures, c3_notices)" in src
