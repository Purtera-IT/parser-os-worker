"""The progress document ends on "done", not on the last parser stage.

Live 010353 (compile b40e9bb3, 2026-10-02): compile-progress.json stayed at
"running quality_gates" from 14:26:41 onward although envelope.json was written
then. The heartbeat re-writes the last stage the compile reported, and it was
stopped only after the whole job returned -- after the "projection" and "done"
writes -- so its next tick put "running quality_gates" back on top of them.
"""

from __future__ import annotations

import inspect
import threading
import time

from parser_os_worker import main as m


def test_nothing_is_written_after_stop():
    written: list[str] = []
    hb = m._ProgressHeartbeat(lambda: written.append("running quality_gates"), 0.01).start()
    time.sleep(0.08)
    hb.stop()
    written.append("done")
    time.sleep(0.08)
    assert written[-1] == "done"
    assert hb.stopped


def test_stop_waits_for_a_tick_already_writing():
    # A tick mid-upload when the compile finishes must land BEFORE the terminal
    # write, never after it.
    entered, release = threading.Event(), threading.Event()
    written: list[str] = []

    def slow_write():
        entered.set()
        release.wait(2)
        written.append("running quality_gates")

    hb = m._ProgressHeartbeat(slow_write, 0.01).start()
    assert entered.wait(2)
    stopper = threading.Thread(target=hb.stop)
    stopper.start()
    time.sleep(0.05)
    assert stopper.is_alive(), "stop() must wait for the tick that is writing"
    release.set()
    stopper.join(2)
    written.append("done")
    time.sleep(0.05)
    assert written == ["running quality_gates", "done"]


def test_the_old_event_interface_still_stops_it():
    hb = m._ProgressHeartbeat(lambda: None, 0.01).start()
    hb.set()
    assert hb.stopped


def test_the_compile_stops_the_heartbeat_before_projection_and_done():
    src = inspect.getsource(m._do_compile)
    compiled = src.index("compile_project(\n")
    stopped = src.index("_progress_heartbeat.stop()")
    projection = src.index('_write_compile_progress(\n            "running", "projection"')
    envelope = src.index("_upload_envelope(")
    done = src.index('_write_compile_progress("done"')
    assert compiled < stopped < projection < envelope < done
