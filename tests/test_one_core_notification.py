"""One parse -> exactly one OrbitBrief Core notification.

Dev 2026-10-26/27 (Log Analytics): 36 parser compiles produced 72
``POST /v1/compile-run`` on orbitbrief-core-worker. The worker sent the
brief-gen queue message (-> Function App -> orbitbrief_runs row -> timer ->
Core) AND posted Core directly over HTTP, so every deal compiled twice
concurrently and the last writer won ``latest/``. Only the queue path has the DB
row, duplicate guard, concurrency cap, PM-answer replay and stuck-run recovery,
so it is the one notifier.
"""

from __future__ import annotations

import base64
import inspect
import json
import urllib.request

import pytest

from parser_os_worker import main as m


class _QueueClient:
    def __init__(self, sent: list[str]):
        self.sent = sent

    def send_message(self, body):
        self.sent.append(body)


class _QueueService:
    def __init__(self):
        self.sent: list[str] = []
        self.queues: list[str] = []

    def get_queue_client(self, name, message_encode_policy=None):
        self.queues.append(name)
        return _QueueClient(self.sent)

    def create_queue(self, name):  # pragma: no cover - only on a send failure
        pass


def _job(mode: str | None = None) -> m.JobMessage:
    opts = {"use_cache": True}
    if mode is not None:
        opts["mode"] = mode
    raw = json.dumps(
        {
            "compile_id": "c-1",
            "deal_id": "6282d1fc-0000-0000-0000-000000000000",
            "manifest_blob_url": "https://x/parser-manifests/c-1.json",
            "compile_options": opts,
        }
    )
    return m.JobMessage.from_raw(raw)


@pytest.fixture
def http_calls(monkeypatch):
    """Record any outbound HTTP the notify step makes (there must be none)."""
    calls: list[str] = []

    class _Opener:
        def open(self, req, *a, **k):
            calls.append(getattr(req, "full_url", str(req)))
            raise AssertionError("worker must not POST OrbitBrief Core directly")

    monkeypatch.setattr(urllib.request, "build_opener", lambda *a, **k: _Opener())
    monkeypatch.setattr(
        urllib.request, "urlopen",
        lambda req, *a, **k: calls.append(getattr(req, "full_url", str(req))),
    )
    monkeypatch.setenv("ORBITBRIEF_CORE_WORKER_URL", "https://core.example")
    monkeypatch.setattr(m, "BRIEF_GEN_QUEUE_NAME", "parser-os-orbitbrief-jobs")
    return calls


def _payload(body: str) -> dict:
    # TextBase64EncodePolicy is applied by the real client; the fake gets text.
    try:
        return json.loads(body)
    except ValueError:
        return json.loads(base64.b64decode(body))


def test_one_parse_sends_exactly_one_core_notification(http_calls):
    qs = _QueueService()
    m._notify_orbitbrief_core(qs, _job())
    assert http_calls == [], "no direct HTTP call to Core"
    assert qs.queues == ["parser-os-orbitbrief-jobs"]
    assert len(qs.sent) == 1
    doc = _payload(qs.sent[0])
    assert doc["envelopeReady"] is True
    assert doc["dealId"].startswith("6282d1fc")
    assert doc["compileId"] == "c-1"


def test_queue_failure_is_not_retried_over_http(http_calls):
    class _Broken(_QueueService):
        def get_queue_client(self, name, message_encode_policy=None):
            raise RuntimeError("queue down")

    m._notify_orbitbrief_core(_Broken(), _job())  # must not raise
    assert http_calls == []


def test_the_direct_http_trigger_is_gone():
    assert not hasattr(m, "_trigger_brief_gen_http")
    src = inspect.getsource(m)
    assert "compile-run?async" not in src
    assert "ORBITBRIEF_CORE_WORKER_URL" not in src


def test_the_compile_loop_notifies_through_the_one_helper_once():
    src = inspect.getsource(m.main)
    assert src.count("_notify_orbitbrief_core(") == 1
    assert "_enqueue_brief_gen(" not in src


@pytest.mark.parametrize(
    "mode, wanted",
    [("parse_only", False), ("parse_and_brief", True), ("PARSE_ONLY", False)],
)
def test_mode_is_forwarded_and_the_message_is_still_sent(http_calls, mode, wanted):
    # Re-parse (parse_only) still sends the message: the consumer's completion
    # step (attachment status, scope, amount) must run on every parse.
    qs = _QueueService()
    m._notify_orbitbrief_core(qs, _job(mode))
    assert len(qs.sent) == 1 and http_calls == []
    doc = _payload(qs.sent[0])
    assert doc["mode"] == mode.lower()
    assert doc["briefWanted"] is wanted


def test_no_mode_leaves_the_message_unchanged(http_calls):
    qs = _QueueService()
    m._notify_orbitbrief_core(qs, _job(None))
    doc = _payload(qs.sent[0])
    assert set(doc) == {"dealId", "compileId", "envelopeReady"}
