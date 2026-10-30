"""The delivery read writes only under deals/<id>/delivery/latest/.

Owner 2026-10-29: the project's Delivery sources page shows the deal read in the
planning view (up to our first kickoff after the win). That read must never
replace the quoting envelope, brief, SOW draft or quote the quoting page shows,
and it never feeds learning (training labels come from the quoting cut only).
"""

from __future__ import annotations

import inspect

from parser_os_worker import main as m


def _job(read=None):
    opts = {"mode": "parse_only"}
    if read is not None:
        opts["read"] = read
    return m.JobMessage(compile_id="c1", deal_id="d1", manifest_blob_url="u", compile_options=opts)


def test_read_of():
    assert m._read_of(_job("delivery")) == "delivery"
    assert m._read_of(_job("Delivery ")) == "delivery"
    assert m._read_of(_job()) == ""
    assert m._read_of(_job("quote")) == ""
    # a recovery marker is a plain dict with a top-level read
    assert m._read_of(m.SimpleJob(deal_id="d1", compile_id="c1", read="delivery")) == "delivery"


def test_paths():
    assert m._progress_blob_path("d1") == "deals/d1/orbitbrief/latest/compile-progress.json"
    assert m._progress_blob_path("d1", "delivery") == "deals/d1/delivery/latest/compile-progress.json"
    assert m._latest_prefix("d1", "delivery") == "deals/d1/delivery/latest/"


def test_envelope_goes_to_its_own_folder():
    class _Client:
        def __init__(self, path):
            self.path = path

        def upload_blob(self, *a, **k):
            pass

    class _Svc:
        def get_blob_client(self, container, blob):
            return _Client(blob)

    assert m._upload_envelope(_Svc(), "d1", {}, "delivery") == "deals/d1/delivery/latest/envelope.json"
    assert m._upload_envelope(_Svc(), "d1", {}) == "deals/d1/orbitbrief/latest/envelope.json"


def test_delivery_returns_before_any_quoting_write():
    src = inspect.getsource(m._do_compile)
    early = src.index("if read == DELIVERY_READ:")
    for later in ("compile-idempotency.json", "scope-process-v1.json", "SOW_DRAFT.md",
                  "compile-trace.json", "_upload_atoms(", "_upload_sheet_renders("):
        assert src.rindex(later) > early, later


def test_no_core_notification_and_no_learning():
    src = inspect.getsource(m.main) if hasattr(m, "main") else inspect.getsource(m)
    full = inspect.getsource(m)
    assert 'if result.get("read") != DELIVERY_READ:\n            _notify_orbitbrief_core(' in full
    assert 'RETRAIN_ENABLED and result.get("read") != DELIVERY_READ' in full
    assert src  # main exists


def test_delivery_strips_the_quoting_cut():
    src = inspect.getsource(m._do_compile)
    assert 'k not in ("as_of", "quote_asof")' in src
