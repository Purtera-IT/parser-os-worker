"""The drain gate: stop taking new work, without abandoning work in flight.

Rolling parser-os-worker-warm restarts it, killing whatever compile it is
running. That destroyed three compiles in one batch, and again on 2026-09-02 a
recompile of deal 010215 was killed mid-run -- it wrote a manifest and never an
envelope, which reads downstream as "the compile did nothing" rather than "the
compile was killed".

The first guard WAITED for an idle worker before rolling. On a busy environment
that window never opens: a deploy waited the full 15 minutes while two unrelated
deals compiled back to back, then failed. Waiting for quiet cannot be relied on
when work is continuous, so the deploy drains instead.
"""

from __future__ import annotations

import logging

import pytest

from parser_os_worker.drain import drain_requested


class _Blob:
    def __init__(self, exists, raises=False):
        self._exists = exists
        self._raises = raises

    def exists(self):
        if self._raises:
            raise RuntimeError("blob unreachable")
        return self._exists


class _Service:
    def __init__(self, exists=False, raises=False):
        self._blob = _Blob(exists, raises)
        self.asked = []

    def get_blob_client(self, container, blob):
        self.asked.append((container, blob))
        return self._blob


@pytest.fixture
def log():
    return logging.getLogger("test-drain")


def test_no_sentinel_means_keep_consuming(log):
    assert drain_requested(_Service(exists=False), log) is False


def test_sentinel_present_stops_new_work(log):
    assert drain_requested(_Service(exists=True), log) is True


def test_it_looks_in_the_artifacts_container(log):
    svc = _Service(exists=True)
    drain_requested(svc, log)
    container, blob = svc.asked[0]
    assert container == "orbitbrief-artifacts"
    assert blob == "control/parser-os-worker.drain"


def test_an_unreadable_sentinel_fails_OPEN(log):
    # The cost of a false negative is one interrupted compile. The cost of a
    # false positive is a silently stalled pipeline nobody is watching -- the
    # worker would stop taking work forever and every deal would look idle.
    # So a sentinel we cannot read must never halt the queue.
    assert drain_requested(_Service(raises=True), log) is False
