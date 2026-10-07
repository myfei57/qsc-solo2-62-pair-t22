"""Bounded, resumable deletion of expired ledger events.

Decisions baked in here:

* Windows are time based. The cutoff is ``as_of - window`` and ``as_of``
  is fixed when a run starts, so an interrupted run resumed later keeps
  deleting exactly the same set — resume is idempotent.
* Deletion is per category, never one big batch. The store persists one
  key atomically, so purging one category is one atomic rewrite and the
  only crash seam is between categories, right where the resume cursor
  points.
* Every deletion is tombstoned before it happens: a ``planned`` record
  carrying the doomed ids and a digest of their content lands in the
  trail first, then the events are purged, then an ``applied`` record
  closes the pair. A crash anywhere leaves enough state to finish.
* A run deletes at most ``budget`` events. The budget is soft: a run
  always finishes at least one category so a huge category cannot
  starve the sweep, and overshoot is bounded by that one category.
"""

from __future__ import annotations

import json
import time
import uuid

from coldstore.store.store import Store

from .ledger import RECORD_DELETION, RECORD_RUN, Ledger, LedgerEvent
from .report import RetentionStatus, SweepSummary

CURSOR_KEY = "retention:cursor"
DEFAULT_BUDGET = 500


class Sweeper:
    """Deletes expired events category by category within a budget."""

    def __init__(self, store: Store, ledger: Ledger) -> None:
        self._store = store
        self._ledger = ledger

    def run(self, budget: int = DEFAULT_BUDGET, now: int | None = None) -> SweepSummary:
        """Delete expired events, resuming any interrupted run first."""

        if budget <= 0:
            raise ValueError("budget must be positive")
        at = int(time.time()) if now is None else int(now)
        cursor = self._load_cursor()
        if cursor is None:
            run_id = uuid.uuid4().hex
            as_of = at
            remaining = self._sweepable_kinds()
        else:
            run_id = str(cursor["run"])
            as_of = int(cursor["as_of"])
            remaining = [str(kind) for kind in cursor["kinds"]]
        self._save_cursor(run_id, as_of, remaining)

        removed = 0
        swept: list[dict[str, object]] = []

        # A crash can strand a planned tombstone before its purge ran.
        # Re-applying it is idempotent because purging is by event id.
        for orphan in self._unapplied(run_id):
            self._ledger.purge([str(event_id) for event_id in orphan["ids"]])
            self._ledger.record(
                RECORD_DELETION,
                run=run_id,
                kind=orphan["kind"],
                phase="applied",
                cutoff=orphan["cutoff"],
                removed=orphan["removed"],
                ids=orphan["ids"],
                digest=orphan["digest"],
            )
            removed += int(orphan["removed"])
            swept.append({"kind": orphan["kind"], "removed": orphan["removed"], "resumed": True})
            remaining = [kind for kind in remaining if kind != orphan["kind"]]
            self._save_cursor(run_id, as_of, remaining)

        windows = self._ledger.windows()
        while remaining:
            kind = remaining[0]
            seconds = windows.get(kind)
            expired: list[LedgerEvent] = []
            if seconds is not None:
                cutoff = as_of - seconds
                expired = [
                    event
                    for event in self._ledger.events()
                    if event.kind == kind and event.at < cutoff
                ]
            if not expired:
                remaining.pop(0)
                self._save_cursor(run_id, as_of, remaining)
                continue
            if removed > 0 and removed + len(expired) > budget:
                break
            cutoff = as_of - int(seconds)
            self._ledger.record_deletion(run_id, kind, "planned", cutoff, expired)
            self._ledger.purge([event.id for event in expired])
            self._ledger.record_deletion(run_id, kind, "applied", cutoff, expired)
            removed += len(expired)
            swept.append({"kind": kind, "removed": len(expired), "cutoff": cutoff})
            remaining.pop(0)
            self._save_cursor(run_id, as_of, remaining)

        status = "partial" if remaining else "completed"
        self._ledger.record(
            RECORD_RUN,
            run=run_id,
            status=status,
            budget=budget,
            removed=removed,
            pending=list(remaining),
        )
        if not remaining:
            self._store.delete(CURSOR_KEY)
        return SweepSummary(
            run=run_id,
            status=status,
            budget=budget,
            removed=removed,
            kinds=swept,
            pending=list(remaining),
            at=at,
        )

    def status(self, now: int | None = None) -> RetentionStatus:
        """Report the cursor and what a sweep would delete right now."""

        at = int(time.time()) if now is None else int(now)
        windows = self._ledger.windows()
        events = self._ledger.events()
        categories: list[dict[str, object]] = []
        for kind in sorted({event.kind for event in events} | set(windows)):
            seconds = windows.get(kind)
            owned = [event for event in events if event.kind == kind]
            expired = 0
            if seconds is not None:
                cutoff = at - seconds
                expired = len([event for event in owned if event.at < cutoff])
            categories.append(
                {
                    "kind": kind,
                    "events": len(owned),
                    "window": seconds,
                    "expired": expired,
                }
            )
        cursor = self._load_cursor()
        return RetentionStatus(
            pending=cursor is not None,
            cursor=cursor,
            categories=categories,
            budget=DEFAULT_BUDGET,
        )

    def _sweepable_kinds(self) -> list[str]:
        windows = self._ledger.windows()
        return sorted(kind for kind in self._ledger.kinds() if kind in windows)

    def _unapplied(self, run_id: str) -> list[dict[str, object]]:
        planned: dict[str, dict[str, object]] = {}
        applied: set[str] = set()
        for entry in self._ledger.deletions():
            if str(entry.get("run", "")) != run_id:
                continue
            key = str(entry.get("kind", ""))
            if entry.get("phase") == "planned":
                planned[key] = entry
            elif entry.get("phase") == "applied":
                applied.add(key)
        return [planned[kind] for kind in sorted(planned) if kind not in applied]

    def _load_cursor(self) -> dict[str, object] | None:
        raw, present = self._store.get(CURSOR_KEY)
        if not present:
            return None
        try:
            cursor = json.loads(raw)
        except ValueError:
            return None
        if not isinstance(cursor, dict) or not isinstance(cursor.get("kinds"), list):
            return None
        return cursor

    def _save_cursor(self, run_id: str, as_of: int, kinds: list[str]) -> None:
        payload = {"run": run_id, "as_of": as_of, "kinds": list(kinds)}
        self._store.put(CURSOR_KEY, json.dumps(payload, ensure_ascii=False))
