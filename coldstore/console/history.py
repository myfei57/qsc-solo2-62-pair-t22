"""Console event log endpoints backed by the retention ledger."""

from __future__ import annotations

from coldstore.ledger import Ledger

from .http import Request, RequestError, Response, json_response


def append(ledger: Ledger, request: Request) -> Response:
    kind = request.str_field("kind")
    if not kind:
        raise RequestError(400, "kind is required")
    ledger.append_event(kind, request.str_field("value"))
    return json_response({"appended": True})


def listing(ledger: Ledger) -> Response:
    events = ledger.events()
    return json_response(
        {
            "events": [event.as_dict() for event in events],
            "count": len(events),
            "has_events": bool(events),
        }
    )


def clear(ledger: Ledger) -> Response:
    removed = ledger.clear_events()
    return json_response({"cleared": True, "removed": removed})
