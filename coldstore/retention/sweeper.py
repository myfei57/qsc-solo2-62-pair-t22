"""Budgeted per category sweeps from the live event log into the ledger.

Sweeps work one category at a time instead of in a single batch: each
category is a bounded unit of work, so a run can stop when its deletion
budget is spent and the next run resumes from the persisted cursor. Every
deleted event is archived into the hash chained ledger first, so pruning
never loses the ability to answer what happened.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field

from coldstore.store.history import Event, event_count, list_events
from coldstore.store.store import Store

from .ledger import Ledger, event_digest
from .policy import load_windows, save_windows, validate_category, validate_window
from .report import RetentionState

CURSOR_KEY = "retention:cursor"
DEFAULT_BUDGET = 500
MAX_BUDGET = 100_000


@dataclass(frozen=True)
class SweepResult:
    """The outcome of one sweep run."""

    run: str
    status: str
    scanned: int
    deleted: int
    budget: int
    cursor: str
    categories: list[dict[str, object]] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return {
            "run": self.run,
            "status": self.status,
            "scanned": self.scanned,
            "deleted": self.deleted,
            "budget": self.budget,
            "cursor": self.cursor,
            "categories": self.categories,
        }


def validate_budget(budget: int) -> int:
    if budget <= 0:
        raise ValueError("budget must be positive")
    if budget > MAX_BUDGET:
        raise ValueError("budget exceeds the supported range")
    return budget


class Sweeper:
    """Applies retention windows to one event log within a deletion budget."""

    def __init__(self, store: Store, source: str = "console:events") -> None:
        self._store = store
        self._source = source
        self._ledger = Ledger(store)

    def windows(self) -> dict[str, int]:
        return load_windows(self._store)

    def set_window(self, category: str, seconds: int, at: int | None = None) -> int:
        """Persist a window and record the change in the ledger."""

        validate_category(category)
        seconds = validate_window(seconds)
        windows = self.windows()
        windows[category] = seconds
        save_windows(self._store, windows)
        self._ledger.append("window", {"category": category, "seconds": seconds}, at=at)
        return seconds

    def cursor(self) -> dict[str, str] | None:
        """The category an interrupted or budget spent run stopped at."""

        category = self._cursor_category()
        return {"category": category} if category else None

    def _cursor_category(self) -> str:
        raw, present = self._store.get(CURSOR_KEY)
        if not present:
            return ""
        try:
            payload = json.loads(raw)
        except ValueError:
            return ""
        if not isinstance(payload, dict):
            return ""
        return str(payload.get("category", ""))

    def sweep(self, now: int | None = None, budget: int = DEFAULT_BUDGET) -> SweepResult:
        """Archive then delete expired events, category by category.

        The run stops once ``budget`` deletions are spent; the cursor then
        points at the unfinished category so the next run resumes there. A
        run that never records its end entry is reported as interrupted and
        continued by the next one.
        """

        budget = validate_budget(budget)
        moment = int(time.time()) if now is None else int(now)
        if moment <= 0:
            raise ValueError("now must be a positive epoch second")
        windows = self.windows()
        run_id = uuid.uuid4().hex
        resume = self._cursor_category()
        runs = self._ledger.runs()
        continues = str(runs[-1]["run"]) if runs and runs[-1]["status"] == "interrupted" else ""
        self._ledger.append(
            "sweep",
            {
                "run": run_id,
                "phase": "begin",
                "budget": budget,
                "source": self._source,
                "now": moment,
                "resumed_from": resume,
                "continues": continues,
            },
            at=moment,
        )

        events = list_events(self._store, self._source)
        categories = sorted(windows)
        if resume:
            categories = [category for category in categories if category >= resume]

        remaining = budget
        scanned = 0
        deleted = 0
        status = "done"
        stop = ""
        per_category: list[dict[str, object]] = []
        for category in categories:
            if remaining <= 0:
                status = "partial"
                stop = category
                break
            cutoff = moment - windows[category]
            mine = [index for index, event in enumerate(events) if event.kind == category]
            expired = [index for index in mine if events[index].at < cutoff]
            take = expired[:remaining]
            scanned += len(mine)
            if take:
                self._archive(category, run_id, [events[index] for index in take], moment)
                drop = set(take)
                events = [event for index, event in enumerate(events) if index not in drop]
                self._save_events(events)
            deleted += len(take)
            remaining -= len(take)
            outcome = {
                "category": category,
                "scanned": len(mine),
                "deleted": len(take),
                "kept": len(mine) - len(take),
            }
            per_category.append(outcome)
            self._ledger.append(
                "sweep",
                {"run": run_id, "phase": "category", **outcome},
                at=moment,
            )
            if len(take) < len(expired):
                status = "partial"
                stop = category
                break

        if status == "done":
            self._store.delete(CURSOR_KEY)
        else:
            self._store.put(CURSOR_KEY, json.dumps({"category": stop}, ensure_ascii=False))
        self._ledger.append(
            "sweep",
            {
                "run": run_id,
                "phase": "end",
                "status": status,
                "scanned": scanned,
                "deleted": deleted,
                "reason": "" if status == "done" else "budget",
            },
            at=moment,
        )
        return SweepResult(
            run=run_id,
            status=status,
            scanned=scanned,
            deleted=deleted,
            budget=budget,
            cursor=stop,
            categories=per_category,
        )

    def _archive(self, category: str, run_id: str, batch: list[Event], at: int) -> None:
        """Append the batch to the ledger, skipping copies already archived.

        A crash between the ledger append and the live log rewrite is
        replayed on the next run; matching digests keep the archive free of
        duplicates while repeated identical events keep their copies.
        """

        have = self._ledger.archived_digests(category)
        seen: dict[str, int] = {}
        fresh: list[dict[str, object]] = []
        for event in batch:
            digest = event_digest(event.as_dict())
            seen[digest] = seen.get(digest, 0) + 1
            if seen[digest] > have.get(digest, 0):
                fresh.append(event.as_dict())
        if fresh:
            self._ledger.append(
                "archive", {"category": category, "run": run_id, "events": fresh}, at=at
            )

    def _save_events(self, events: list[Event]) -> None:
        if not events:
            self._store.delete(self._source)
            return
        raw = [json.dumps(event.as_dict(), ensure_ascii=False) for event in events]
        self._store.put(self._source, json.dumps(raw, ensure_ascii=False))

    def live_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for event in list_events(self._store, self._source):
            counts[event.kind] = counts.get(event.kind, 0) + 1
        return counts

    def archived_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for entry in self._ledger.by_kind("archive"):
            category = str(entry.payload.get("category", ""))
            batch = entry.payload.get("events", [])
            if isinstance(batch, list):
                counts[category] = counts.get(category, 0) + len(batch)
        return counts

    def archived(self, category: str = "") -> list[dict[str, object]]:
        return self._ledger.archived(category)

    def runs(self) -> list[dict[str, object]]:
        return self._ledger.runs()

    def last_run(self) -> dict[str, object] | None:
        runs = self.runs()
        return runs[-1] if runs else None

    def ledger_entries(self) -> list[dict[str, object]]:
        return [entry.as_dict() for entry in self._ledger.entries()]

    def ledger_count(self) -> int:
        return self._ledger.count()

    def verify(self) -> bool:
        return self._ledger.verify()

    def state(self) -> RetentionState:
        runs = self.runs()
        return RetentionState(
            windows=self.windows(),
            live=event_count(self._store, self._source),
            archived=sum(self.archived_counts().values()),
            runs=len(runs),
            last_status=str(runs[-1]["status"]) if runs else "",
            ledger_ok=self.verify(),
        )

    def describe(self) -> str:
        state = self.state()
        return (
            f"retention windows={len(state.windows)} live={state.live} "
            f"archived={state.archived} runs={state.runs} last={state.last_status or 'none'}"
        )
