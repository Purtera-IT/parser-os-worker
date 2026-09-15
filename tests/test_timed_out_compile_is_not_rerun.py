"""A compile that ran out of its budget once is poisoned on its next sight,
not re-run for another full budget."""
import json
from types import SimpleNamespace

import parser_os_worker.main as m


class _Blob:
    def __init__(self, payload): self.payload = payload
    def get_blob_client(self, container, blob):
        payload = self.payload
        class _C:
            def download_blob(self_inner):
                class _D:
                    def readall(self_d):
                        if payload is None: raise RuntimeError("BlobNotFound")
                        return json.dumps(payload).encode()
                return _D()
        return _C()


def _job(): return SimpleNamespace(deal_id="deal-1", compile_id="cmp-1")


def test_an_earlier_timeout_on_this_compile_is_recognised():
    assert m._previous_attempt_timed_out(_Blob({"compile_id": "cmp-1", "status": "failed", "stage": "timeout"}), _job()) is True


def test_other_outcomes_and_other_compiles_are_not():
    assert m._previous_attempt_timed_out(_Blob({"compile_id": "cmp-1", "status": "failed", "stage": "exhausted_retries"}), _job()) is False
    assert m._previous_attempt_timed_out(_Blob({"compile_id": "cmp-0", "status": "failed", "stage": "timeout"}), _job()) is False
    assert m._previous_attempt_timed_out(_Blob({"compile_id": "cmp-1", "status": "completed", "stage": "done"}), _job()) is False
    assert m._previous_attempt_timed_out(_Blob(None), _job()) is False
