"""A cut compile and a full compile of the same deal must not invalidate each other."""
from __future__ import annotations
import json
from types import SimpleNamespace
from parser_os_worker.main import _unchanged_since_last_compile

class _Blob:
    def __init__(self, store, path): self.store, self.path = store, path
    def download_blob(self): return SimpleNamespace(readall=lambda: json.dumps(self.store[self.path]).encode())
class _BlobService:
    def __init__(self, store): self.store = store
    def get_blob_client(self, container, blob): return _Blob(self.store, blob)

def _manifest(shas, as_of):
    return {"artifacts": [{"content_sha256": s} for s in shas], "context": {"as_of": as_of} if as_of else {}}

def _key(shas):
    import hashlib; return hashlib.sha256("\n".join(sorted(shas)).encode()).hexdigest()

REC = "deals/d1/orbitbrief/latest/compile-idempotency.json"

def test_same_artifacts_same_scope_is_unchanged():
    bs = _BlobService({REC: {"artifact_key": _key(["a","b"]), "as_of": "2026-08-13T15:33:00.994Z"}})
    assert _unchanged_since_last_compile(bs, "d1", _manifest(["a","b"], "2026-08-13T15:33:00.994Z")) is True

def test_a_full_run_after_a_cut_is_not_redundant_and_vice_versa():
    bs = _BlobService({REC: {"artifact_key": _key(["a","b"]), "as_of": "2026-08-13T15:33:00.994Z"}})
    assert _unchanged_since_last_compile(bs, "d1", _manifest(["a","b"], None)) is False
    bs2 = _BlobService({REC: {"artifact_key": _key(["a","b"]), "as_of": None}})
    assert _unchanged_since_last_compile(bs2, "d1", _manifest(["a","b"], "2026-08-13T15:33:00.994Z")) is False

def test_legacy_record_without_as_of_matches_a_full_run_only():
    bs = _BlobService({REC: {"artifact_key": _key(["a"])}})
    assert _unchanged_since_last_compile(bs, "d1", _manifest(["a"], None)) is True
    assert _unchanged_since_last_compile(bs, "d1", _manifest(["a"], "2026-08-13T15:33:00.994Z")) is False

def test_changed_artifacts_always_compile():
    bs = _BlobService({REC: {"artifact_key": _key(["a"]), "as_of": None}})
    assert _unchanged_since_last_compile(bs, "d1", _manifest(["a","NEW"], None)) is False
