"""SowSmith Run: a sow_draft job writes c3_sow into the latest envelope and PM handoff, and says how it went."""
import json
import sys
import types

from parser_os_worker import main as w

DEAL = "0452cc64-9cf2-4d7f-ab28-6c8f08f6fbc6"


class _Blob:
    def __init__(self, store, name):
        self.store, self.name = store, name

    def download_blob(self):
        data = self.store[self.name]
        return types.SimpleNamespace(readall=lambda: data)

    def upload_blob(self, data, overwrite=False):
        self.store[self.name] = data if isinstance(data, bytes) else str(data).encode()


class _Svc:
    def __init__(self, store):
        self.store = store

    def get_blob_client(self, container, blob):
        return _Blob(self.store, blob)


def _fake_sow(monkeypatch, sow):
    core = types.ModuleType("app.core")
    core.c3_sow = types.SimpleNamespace(for_envelope=lambda env: dict(sow))
    core.c3_sow_trace = types.SimpleNamespace(attach=lambda env: None)
    monkeypatch.setitem(sys.modules, "app.core", core)
    monkeypatch.setitem(sys.modules, "app.core.c3_sow", core.c3_sow)
    monkeypatch.setitem(sys.modules, "app.core.c3_sow_trace", core.c3_sow_trace)


def test_sow_draft_writes_envelope_and_handoff(monkeypatch):
    latest = f"deals/{DEAL}/orbitbrief/latest/"
    store = {latest + "envelope.json": json.dumps({"compile_id": "c1", "c3_brief": {"sections": {}}}).encode(),
             latest + "PM_HANDOFF.json": json.dumps({"sites": []}).encode()}
    _fake_sow(monkeypatch, {"locations": [{"site": "dc 5841"}]})
    monkeypatch.setattr(w, "_upload_envelope", lambda svc, deal, env: svc.get_blob_client("c", latest + "envelope.json").upload_blob(json.dumps(env).encode(), overwrite=True))
    w._run_sow_draft(_Svc(store), DEAL, {"kind": "sow_draft", "deal_id": DEAL})
    env = json.loads(store[latest + "envelope.json"])
    pmh = json.loads(store[latest + "PM_HANDOFF.json"])
    status = json.loads(store[latest + "sow-draft-status.json"])
    assert env["c3_sow"]["written_on"] == "sowsmith_run" and pmh["c3_sow"]["locations"] == [{"site": "dc 5841"}]
    assert status["state"] == "done" and status["locations"] == 1


def test_sow_draft_without_a_brief_fails_loudly(monkeypatch):
    latest = f"deals/{DEAL}/orbitbrief/latest/"
    store = {latest + "envelope.json": json.dumps({"compile_id": "c1"}).encode()}
    _fake_sow(monkeypatch, {})
    w._run_sow_draft(_Svc(store), DEAL, {"kind": "sow_draft", "deal_id": DEAL})
    status = json.loads(store[latest + "sow-draft-status.json"])
    assert status["state"] == "failed" and "c3_brief" in status["error"]


def test_sow_draft_rejects_a_bad_deal_id():
    store = {}
    w._run_sow_draft(_Svc(store), "../../etc", {"kind": "sow_draft"})
    assert store == {}
