"""A running compile says who asked for it.

The queue message has carried `triggered_by` all along -- every producer sets
it -- and `JobMessage` dropped it on the floor. So once a compile stopped being
a queued message and started running, nothing anywhere knew who had started it:
the queue panel could show an owner against a WAITING deal and had nothing to
show against a RUNNING one, which is the moment somebody actually wants to ask.
"""

from __future__ import annotations

import json

from parser_os_worker import main as m


def _job(**over):
    """A queue message, parsed the way the worker parses one."""
    body = {
        "compile_id": "c1",
        "deal_id": "d1",
        "manifest_blob_url": "https://x/y.json",
    }
    body.update(over)
    return m.JobMessage.from_raw(json.dumps(body))


def test_the_job_keeps_who_asked_for_it():
    job = _job(triggered_by="griffin@purtera-it.com")
    assert job.triggered_by == "griffin@purtera-it.com"


def test_camelCase_is_accepted_too():
    # Producers are not consistent; the decoder on the API side accepts both
    # shapes for the same reason.
    job = _job(triggeredBy="chase@purtera-it.com")
    assert job.triggered_by == "chase@purtera-it.com"


def test_no_trigger_is_None_rather_than_an_empty_string():
    # An empty string renders as a blank byline, which reads as "nobody" rather
    # than "we do not know".
    assert _job().triggered_by is None
    assert _job(triggered_by="   ").triggered_by is None


def test_it_survives_the_rest_of_the_message_being_normal():
    job = _job(triggered_by="auto-finalize", force=True, compile_options={"mode": "parse_only"})
    assert job.triggered_by == "auto-finalize"
    assert job.force is True
    assert job.compile_id == "c1"
