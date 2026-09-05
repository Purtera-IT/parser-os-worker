"""A compile that outruns its timeout is killed, not waited on.

`COMPILE_TIMEOUT_SEC` was read from the environment on import and never
referenced again — the only occurrence in the repository was its own
assignment. It was set to 1500 on the live dev worker and bounded nothing.

On 2026-09-05 a 35-artifact deal sat in `parse_artifacts` for 6.8 hours:
heartbeating, both large spreadsheets already parsed, the twenty artifacts left
0-2 KB apiece, the model host answering in 0.3s. Nothing was being accomplished
and nothing was going to stop it. It also blocked every deploy, because the
drain will not roll over a running compile.
"""

from __future__ import annotations

import threading
import time
from types import SimpleNamespace

import pytest

from parser_os_worker import main as m


@pytest.fixture
def job():
    return SimpleNamespace(deal_id="d1", compile_id="c1")


def test_a_compile_inside_its_budget_is_left_alone(job, monkeypatch):
    exits: list[int] = []
    monkeypatch.setattr(m.os, "_exit", lambda code: exits.append(code))
    w = m._CompileWatchdog(object(), job, timeout_sec=30).start()
    time.sleep(0.05)
    w.stop()
    assert w.fired is False
    assert exits == []


def test_a_compile_that_outruns_its_budget_is_killed(job, monkeypatch):
    exits: list[int] = []
    written: list[tuple] = []
    monkeypatch.setattr(m.os, "_exit", lambda code: exits.append(code))
    monkeypatch.setattr(
        m, "_write_status",
        lambda blob, j, status, **kw: written.append((status, kw.get("stage"))),
    )
    w = m._CompileWatchdog(object(), job, timeout_sec=0.05).start()
    time.sleep(0.8)
    assert w.fired is True
    assert exits == [75], "the worker must go down so the slot is freed"
    assert written == [("failed", "timeout")], "the deal must show why it stopped"


def test_the_deal_is_told_even_if_the_status_write_throws(job, monkeypatch):
    """A blob write that fails must not leave the process running forever."""
    exits: list[int] = []
    monkeypatch.setattr(m.os, "_exit", lambda code: exits.append(code))
    monkeypatch.setattr(m, "_write_status", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("blob down")))
    m._CompileWatchdog(object(), job, timeout_sec=0.05).start()
    time.sleep(0.8)
    assert exits == [75]


def test_a_zero_timeout_disables_the_bound_visibly(job, monkeypatch):
    exits: list[int] = []
    monkeypatch.setattr(m.os, "_exit", lambda code: exits.append(code))
    w = m._CompileWatchdog(object(), job, timeout_sec=0).start()
    time.sleep(0.2)
    assert w.fired is False and exits == []


def test_the_watchdog_thread_does_not_outlive_the_compile(job, monkeypatch):
    monkeypatch.setattr(m.os, "_exit", lambda code: None)
    before = threading.active_count()
    w = m._CompileWatchdog(object(), job, timeout_sec=30).start()
    w.stop()
    time.sleep(0.2)
    assert threading.active_count() <= before


def test_the_constant_is_actually_used_now():
    """The whole defect: it was assigned and never read."""
    import inspect

    src = inspect.getsource(m)
    uses = [
        ln for ln in src.splitlines()
        if "COMPILE_TIMEOUT_SEC" in ln and not ln.strip().startswith("#")
    ]
    assert len(uses) >= 2, "assigned and never referenced is how this happened"
    assert any("_CompileWatchdog(" in ln for ln in uses)
