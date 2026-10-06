"""The click event and its JSON wire format."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass

_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
KINDS = frozenset({"click", "view"})


@dataclass(frozen=True)
class ClickEvent:
    """One user interaction; event_time is when it happened, in Unix milliseconds."""

    event_id: str
    user_id: str
    item_id: str
    kind: str
    event_time: int

    def to_json(self) -> str:
        """Compact JSON with sorted keys."""
        return json.dumps(asdict(self), separators=(",", ":"), sort_keys=True)

    @staticmethod
    def from_json(data: str | bytes) -> ClickEvent:
        """Parse and validate one event; raises ValueError for anything malformed."""
        try:
            raw = json.loads(data)
        except json.JSONDecodeError as e:
            raise ValueError(f"not JSON: {e.msg}") from e
        if not isinstance(raw, dict) or set(raw) != {
            "event_id",
            "user_id",
            "item_id",
            "kind",
            "event_time",
        }:
            raise ValueError("event must have exactly event_id, user_id, item_id, kind, event_time")
        for field in ("event_id", "user_id", "item_id"):
            if not isinstance(raw[field], str) or not _ID.match(raw[field]):
                raise ValueError(f"invalid {field}")
        if raw["kind"] not in KINDS:
            raise ValueError("invalid kind")
        t = raw["event_time"]
        if isinstance(t, bool) or not isinstance(t, int) or not 0 <= t < 2**53:
            raise ValueError("event_time must be a non-negative integer of milliseconds")
        return ClickEvent(raw["event_id"], raw["user_id"], raw["item_id"], raw["kind"], t)
