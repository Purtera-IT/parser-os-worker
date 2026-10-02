"""Every compile's final outcome is findable by date, without listing deals."""
import json
from types import SimpleNamespace

import parser_os_worker.main as m


class _Recorder:
    """A blob service that records uploads; `fail` makes a path raise."""

    def __init__(self, fail: str | None = None):
        self.uploads: dict[str, dict] = {}
        self.fail = fail

    def get_blob_client(self, container, blob):
        rec = self

        class _C:
            def upload_blob(self_inner, data, overwrite=False, content_type=None, metadata=None):
                if rec.fail and blob.startswith(rec.fail):
                    raise RuntimeError("storage down")
                rec.uploads[blob] = {"data": json.loads(data), "metadata": metadata, "container": container}

        return _C()


def _job():
    return SimpleNamespace(deal_id="deal-1", compile_id="cmp-1")


def _index(rec):
    return {k: v for k, v in rec.uploads.items() if k.startswith(m.RUN_INDEX_PREFIX + "/")}


def test_a_completed_compile_is_indexed_under_its_date_with_metadata(monkeypatch):
    monkeypatch.setattr(m, "_iso_now", lambda: "2026-09-29T14:03:00+00:00")
    rec = _Recorder()
    m._write_status(rec, _job(), "completed", stage="done", elapsed_sec=812.4, atom_count=40, envelope_path="x")
    idx = _index(rec)
    assert list(idx) == ["_parser-runs/2026-09-29/cmp-1.json"]
    entry = idx["_parser-runs/2026-09-29/cmp-1.json"]
    assert entry["metadata"] == {
        "status": "completed",
        "stage": "done",
        "deal_id": "deal-1",
        "compile_id": "cmp-1",
        "updated_at": "2026-09-29T14:03:00+00:00",
        "elapsed_sec": "812.4",
        "worker_sha": m.WORKER_SHA,
    }
    assert entry["data"]["atom_count"] == 40
    assert "envelope_path" not in entry["data"]
    # The per-deal status blob is still written as before.
    assert "deals/deal-1/parser-jobs/cmp-1.json" in rec.uploads


def test_a_failure_keeps_a_short_error_and_never_the_traceback(monkeypatch):
    monkeypatch.setattr(m, "_iso_now", lambda: "2026-09-29T15:00:00+00:00")
    rec = _Recorder()
    m._write_status(rec, _job(), "failed", stage="exception", error="E" * 900, traceback="Traceback …")
    entry = _index(rec)["_parser-runs/2026-09-29/cmp-1.json"]
    assert len(entry["data"]["error"]) == 500
    assert "traceback" not in entry["data"]
    assert entry["metadata"]["stage"] == "exception"
    # The full traceback still reaches the per-deal blob.
    assert rec.uploads["deals/deal-1/parser-jobs/cmp-1.json"]["data"]["traceback"] == "Traceback …"


def test_running_states_are_not_indexed(monkeypatch):
    rec = _Recorder()
    for status in ("running", "starting"):
        m._write_status(rec, _job(), status, stage="compile")
    assert _index(rec) == {}


def test_interrupted_is_indexed_so_a_released_compile_is_visible(monkeypatch):
    monkeypatch.setattr(m, "_iso_now", lambda: "2026-09-29T16:00:00+00:00")
    rec = _Recorder()
    m._write_status(rec, _job(), "interrupted", stage="terminated", error="worker terminated")
    assert _index(rec)["_parser-runs/2026-09-29/cmp-1.json"]["metadata"]["status"] == "interrupted"


def test_a_failed_index_write_never_raises_or_stops_the_status_write(monkeypatch):
    rec = _Recorder(fail=m.RUN_INDEX_PREFIX)
    m._write_status(rec, _job(), "completed", stage="done")
    assert "deals/deal-1/parser-jobs/cmp-1.json" in rec.uploads
    assert _index(rec) == {}


def test_metadata_is_header_safe():
    md = m._run_index_metadata({"status": "failed", "stage": "exceptión\n", "deal_id": None, "compile_id": "c" * 300})
    assert md["stage"] == "exceptin"
    assert "deal_id" not in md
    assert len(md["compile_id"]) == 128
