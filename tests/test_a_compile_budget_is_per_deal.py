"""A flat compile budget is a bet that every deal is the same size.

They are not. 010180 carries 9 documents and compiles in about three minutes;
010347 carries 68, spent 881s in `parse_artifacts` alone — 13s a document, 59%
of the whole 1500s allowance — and died at `bom_owner` with every stage after
it unrun. 010264 did the same at roughly 18s a document.

Both were reported as "still parsing after 120 minutes". They were not. They
were dead, and deterministically so: re-running them unchanged fails at the
same place every time. The threshold is around 33 documents, which is why
every smaller deal in the corpus looks fine — they never approach the wall.

Raising the budget is safe in a way that is easy to get wrong. The lease
RENEWER keeps the queue message invisible for as long as the compile runs, so
`MESSAGE_VISIBILITY_TIMEOUT_SEC` is not a ceiling on compile length — the
renewer's own docstring says LEASE_MAX_SEC "is a backstop against a zombie
holding a message forever, not a compile budget". The ceiling is the lease
backstop, four hours, and 1500s was never near it.
"""

from __future__ import annotations

import pytest

from parser_os_worker.main import (
    COMPILE_SEC_PER_DOC,
    COMPILE_TIMEOUT_SEC,
    LEASE_MAX_SEC,
    compile_budget_sec,
)


def _manifest(n: int) -> dict:
    return {"artifacts": [{"blob_url": f"b{i}"} for i in range(n)]}


def test_a_small_deal_is_unchanged():
    """9 documents is 010180 and 010237. Both compile in about three minutes,
    so nothing about them should move."""
    assert compile_budget_sec(_manifest(9)) == float(COMPILE_TIMEOUT_SEC)


def test_the_deal_that_died_now_fits():
    """010347: 68 documents, ~1482s used before it was killed at 1500."""
    budget = compile_budget_sec(_manifest(68))
    assert budget > 1500
    assert budget >= 68 * COMPILE_SEC_PER_DOC
    assert budget / 60 >= 45          # comfortably past the ~40 min it needs


def test_the_budget_grows_with_the_work():
    small, mid, large = (compile_budget_sec(_manifest(n)) for n in (9, 68, 120))
    assert small <= mid < large


def test_it_never_outlives_the_lease():
    """A pathological manifest must not outlive the message lease, or the
    queue redelivers and the deal compiles on top of itself."""
    assert compile_budget_sec(_manifest(100_000)) < LEASE_MAX_SEC


def test_a_malformed_manifest_gets_the_floor():
    """A manifest we cannot count is not a licence to run unbounded, and not a
    reason to fail either."""
    for bad in (None, {}, {"artifacts": None}, {"artifacts": "nope"}, object()):
        assert compile_budget_sec(bad) == float(COMPILE_TIMEOUT_SEC)


def test_zero_documents_still_gets_the_floor():
    assert compile_budget_sec(_manifest(0)) == float(COMPILE_TIMEOUT_SEC)
