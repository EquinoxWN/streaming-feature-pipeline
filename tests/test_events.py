"""The event wire format: round trip and strict validation of untrusted input."""

from __future__ import annotations

import pytest

from streaming_feature_pipeline.events import ClickEvent


def test_round_trip_is_exact() -> None:
    e = ClickEvent("e1", "u1", "i1", "click", 1_767_225_600_000)
    assert ClickEvent.from_json(e.to_json()) == e
    assert (
        e.to_json()
        == '{"event_id":"e1","event_time":1767225600000,"item_id":"i1","kind":"click","user_id":"u1"}'
    )


@pytest.mark.parametrize(
    ("raw", "why"),
    [
        ("{oops", "not JSON"),
        ("[]", "exactly"),
        ('{"event_id":"e","user_id":"u","item_id":"i","kind":"click"}', "exactly"),
        (
            '{"event_id":"e","user_id":"u","item_id":"i","kind":"click","event_time":1,"x":1}',
            "exactly",
        ),
        (
            '{"event_id":"e 1","user_id":"u","item_id":"i","kind":"click","event_time":1}',
            "event_id",
        ),
        (
            '{"event_id":"e","user_id":"u","item_id":"'
            + "i" * 65
            + '","kind":"click","event_time":1}',
            "item_id",
        ),
        ('{"event_id":"e","user_id":"u","item_id":"i","kind":"buy","event_time":1}', "kind"),
        (
            '{"event_id":"e","user_id":"u","item_id":"i","kind":"click","event_time":true}',
            "event_time",
        ),
        (
            '{"event_id":"e","user_id":"u","item_id":"i","kind":"click","event_time":-5}',
            "event_time",
        ),
        (
            '{"event_id":"e","user_id":"u","item_id":"i","kind":"click","event_time":1.5}',
            "event_time",
        ),
    ],
)
def test_malformed_events_are_rejected(raw: str, why: str) -> None:
    with pytest.raises(ValueError, match=why):
        ClickEvent.from_json(raw)
