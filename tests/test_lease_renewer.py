"""The lease follows the compile, not a clock.

A compile that outlives its 30-minute visibility lease is redelivered and
re-run in parallel (dequeue_count=2 observed live on deal 2fd8baf1). The
renewer keeps the message invisible while work is in progress and rebinds the
pop_receipt each time, because the queue rejects any update or delete that
presents a stale receipt.
"""

from __future__ import annotations

import time
from types import SimpleNamespace

from parser_os_worker.main import _LeaseRenewer


class _FakeQueue:
    def __init__(self, fail_first=False):
        self.calls = []
        self.fail_first = fail_first

    def update_message(self, msg, pop_receipt=None, visibility_timeout=None):
        self.calls.append((msg.id, pop_receipt, visibility_timeout))
        if self.fail_first and len(self.calls) == 1:
            raise RuntimeError("transient")
        return SimpleNamespace(id=msg.id, pop_receipt=f"r{len(self.calls)}")


def _msg():
    return SimpleNamespace(id="m1", pop_receipt="r0", dequeue_count=1)


def test_renews_on_cadence_and_rebinds_the_receipt():
    q, m = _FakeQueue(), _msg()
    r = _LeaseRenewer(q, m, every=30, max_total=3600, lease=1800)
    r._every = 0.05  # test cadence; the ctor floors real cadence at 30 s
    r.start()
    time.sleep(0.3)
    r.stop()
    assert r.renewals >= 2
    # every call after the first must present the receipt the previous one returned
    receipts = [c[1] for c in q.calls]
    assert receipts[0] == "r0"
    for i in range(1, len(receipts)):
        assert receipts[i] == f"r{i}"
    assert all(c[2] == 1800 for c in q.calls)
    assert m.pop_receipt == f"r{len(q.calls)}"


def test_a_failed_renewal_is_retried_not_fatal():
    q, m = _FakeQueue(fail_first=True), _msg()
    r = _LeaseRenewer(q, m, every=30, max_total=3600, lease=1800)
    r._every = 0.05
    r.start()
    time.sleep(0.3)
    r.stop()
    assert r.errors == 1
    assert r.renewals >= 1, "must keep renewing after a transient failure"


def test_stop_ends_renewal():
    q, m = _FakeQueue(), _msg()
    r = _LeaseRenewer(q, m, every=30, max_total=3600, lease=1800)
    r._every = 0.05
    r.start(); time.sleep(0.12); r.stop()
    n = len(q.calls)
    time.sleep(0.2)
    assert len(q.calls) == n, "no renewals after stop()"


def test_hard_ceiling_lets_the_lease_lapse():
    q, m = _FakeQueue(), _msg()
    r = _LeaseRenewer(q, m, every=30, max_total=30, lease=1800)
    r._every = 0.05
    r._max_total = 0.1  # already past the ceiling on the first tick
    r._started = time.monotonic() - 1
    r.start(); time.sleep(0.2); r.stop()
    assert q.calls == [], "past LEASE_MAX_SEC the renewer must stop renewing"
