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


def test_the_service_shape_is_read_too():
    """parser-os-service sends `trigger: {kind, by}`, not a flat field.

    Reading only `triggered_by` left every service-initiated compile anonymous
    -- and the service is what auto-finalize goes through, so that is most of
    them. Live dev showed four compiles running with `by: None` against a
    worker that had just shipped the flat-field version.
    """
    job = _job(trigger={"kind": "manual", "by": "griffin@purtera-it.com"})
    assert job.triggered_by == "griffin@purtera-it.com"
    assert job.trigger_kind == "manual"


def test_a_timer_says_so_rather_than_saying_nothing():
    # Nobody asked for it, and "nobody asked for this" is the answer to "why is
    # this here" -- more useful than a blank byline.
    job = _job(trigger={"kind": "timer"})
    assert job.triggered_by == "timer"
    assert job.trigger_kind == "timer"


def test_the_flat_field_wins_when_both_are_present():
    job = _job(triggered_by="panel@purtera-it.com", trigger={"kind": "timer", "by": ""})
    assert job.triggered_by == "panel@purtera-it.com"
    assert job.trigger_kind == "timer"


def test_an_empty_trigger_object_is_still_nobody():
    assert _job(trigger={}).triggered_by is None
    assert _job(trigger={"kind": "", "by": ""}).triggered_by is None
