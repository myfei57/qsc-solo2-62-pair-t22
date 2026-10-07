"""Retention ledger: the single source of truth for console events.

The ledger keeps two streams in the same store:

* ``ledger:events`` — the original events. An event is immutable once
  filed: the ledger exposes no update operation, and every event carries
  a digest of its content so later tampering is detectable. Events leave
  the ledger only through a tombstoned deletion.
* ``ledger:trail`` — the append only audit trail. Retention window
  settings, deletion records and run records are written here and never
  purged, so a deleted event always leaves a trace behind.

The console history endpoints and the audit page both read from this one
ledger, so they can never disagree about what is stored.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from dataclasses import dataclass

from coldstore.store.commands import append_command, load_commands
from coldstore.store.store import Store

from .report import LedgerState

EVENTS_KEY = "ledger:events"
TRAIL_KEY = "ledger:trail"

RECORD_WINDOW = "window"
RECORD_DELETION = "deletion"
RECORD_RUN = "run"


def validate_window(seconds: int) -> None:
    """Reject a retention window that is not a positive number of seconds."""

    if seconds <= 0:
        raise ValueError("window seconds must be positive")


def _digest(payload: dict[str, object]) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class LedgerEvent:
    """One immutable event filed in the ledger."""

    id: str
    at: int
    kind: str
    value: str
    digest: str

    def content(self) -> dict[str, object]:
        return {"id": self.id, "at": self.at, "kind": self.kind, "value": self.value}

    def as_dict(self) -> dict[str, object]:
        return {
            "id": self.id,
            "at": self.at,
            "kind": self.kind,
            "value": self.value,
            "digest": self.digest,
        }


class Ledger:
    """Files events and the retention trail in one store."""

    def __init__(self, store: Store) -> None:
        self._store = store

    def append_event(self, kind: str, value: str, at: int | None = None) -> LedgerEvent:
        """File a new event. Events are never updated once filed."""

        content = {
            "id": uuid.uuid4().hex,
            "at": int(time.time()) if at is None else int(at),
            "kind": kind,
            "value": value,
        }
        event = LedgerEvent(
            id=str(content["id"]),
            at=int(content["at"]),
            kind=kind,
            value=value,
            digest=_digest(content),
        )
        append_command(self._store, EVENTS_KEY, json.dumps(event.as_dict(), ensure_ascii=False))
        return event

    def events(self) -> list[LedgerEvent]:
        events: list[LedgerEvent] = []
        for item in load_commands(self._store, EVENTS_KEY):
            try:
                payload = json.loads(item)
            except ValueError:
                continue
            if not isinstance(payload, dict):
                continue
            events.append(
                LedgerEvent(
                    id=str(payload.get("id", "")),
                    at=int(payload.get("at", 0)),
                    kind=str(payload.get("kind", "")),
                    value=str(payload.get("value", "")),
                    digest=str(payload.get("digest", "")),
                )
            )
        return events

    def event_count(self) -> int:
        return len(self.events())

    def kinds(self) -> list[str]:
        seen: list[str] = []
        for event in self.events():
            if event.kind not in seen:
                seen.append(event.kind)
        return seen

    def purge(self, ids: list[str]) -> int:
        """Physically remove events by id. Callers must tombstone first."""

        doomed = set(ids)
        if not doomed:
            return 0
        kept: list[str] = []
        removed = 0
        for item in load_commands(self._store, EVENTS_KEY):
            try:
                payload = json.loads(item)
            except ValueError:
                kept.append(item)
                continue
            if isinstance(payload, dict) and str(payload.get("id", "")) in doomed:
                removed += 1
                continue
            kept.append(item)
        if kept:
            self._store.put(EVENTS_KEY, json.dumps(kept, ensure_ascii=False))
        else:
            self._store.delete(EVENTS_KEY)
        return removed

    def record(self, record_type: str, **fields: object) -> dict[str, object]:
        """Append one entry to the never purged audit trail."""

        entry: dict[str, object] = {
            "id": uuid.uuid4().hex,
            "type": record_type,
            "at": int(time.time()),
        }
        entry.update(fields)
        append_command(self._store, TRAIL_KEY, json.dumps(entry, ensure_ascii=False))
        return entry

    def trail(self) -> list[dict[str, object]]:
        records: list[dict[str, object]] = []
        for item in load_commands(self._store, TRAIL_KEY):
            try:
                payload = json.loads(item)
            except ValueError:
                continue
            if isinstance(payload, dict):
                records.append(payload)
        return records

    def _records(self, record_type: str) -> list[dict[str, object]]:
        return [entry for entry in self.trail() if entry.get("type") == record_type]

    def set_window(self, kind: str, seconds: int) -> dict[str, object]:
        """Set the retention window for one event category."""

        if not kind:
            raise ValueError("kind is required")
        validate_window(seconds)
        return self.record(RECORD_WINDOW, kind=kind, seconds=int(seconds))

    def windows(self) -> dict[str, int]:
        """Current window per category, latest setting wins."""

        current: dict[str, int] = {}
        for entry in self._records(RECORD_WINDOW):
            current[str(entry.get("kind", ""))] = int(entry.get("seconds", 0))
        return current

    def window_history(self) -> list[dict[str, object]]:
        return self._records(RECORD_WINDOW)

    def deletions(self) -> list[dict[str, object]]:
        return self._records(RECORD_DELETION)

    def runs(self) -> list[dict[str, object]]:
        return self._records(RECORD_RUN)

    def record_deletion(
        self, run: str, kind: str, phase: str, cutoff: int, events: list[LedgerEvent]
    ) -> dict[str, object]:
        """Tombstone one category deletion, before or after it is applied."""

        digest = hashlib.sha256(
            "\n".join(event.digest for event in events).encode("utf-8")
        ).hexdigest()
        return self.record(
            RECORD_DELETION,
            run=run,
            kind=kind,
            phase=phase,
            cutoff=cutoff,
            removed=len(events),
            ids=[event.id for event in events],
            digest=digest,
        )

    def clear_events(self) -> int:
        """Manual clear: tombstone every filed event, then purge them all."""

        events = self.events()
        if not events:
            return 0
        self.record_deletion("manual", "*", "applied", 0, events)
        return self.purge([event.id for event in events])

    def verify(self) -> dict[str, object]:
        """Recompute event digests to prove filed events were not altered."""

        mismatches: list[str] = []
        seen: set[str] = set()
        duplicates: list[str] = []
        events = self.events()
        for event in events:
            if event.id in seen:
                duplicates.append(event.id)
            seen.add(event.id)
            if _digest(event.content()) != event.digest:
                mismatches.append(event.id)
        return {
            "checked": len(events),
            "mismatches": mismatches,
            "duplicates": duplicates,
            "ok": not mismatches and not duplicates,
        }

    def state(self) -> LedgerState:
        events = self.events()
        return LedgerState(
            events=[event.as_dict() for event in events],
            windows=self.windows(),
            window_records=self.window_history(),
            deletions=self.deletions(),
            runs=self.runs(),
        )

    def describe(self) -> str:
        return f"ledger events={self.event_count()} windows={len(self.windows())}"
