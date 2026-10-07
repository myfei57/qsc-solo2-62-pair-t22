"""Ledger and sweep projections."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LedgerState:
    """The retention ledger as the audit page renders it."""

    events: list[dict[str, object]]
    windows: dict[str, int]
    window_records: list[dict[str, object]]
    deletions: list[dict[str, object]]
    runs: list[dict[str, object]]

    def as_dict(self) -> dict[str, object]:
        return {
            "events": self.events,
            "count": len(self.events),
            "windows": self.windows,
            "window_records": self.window_records,
            "deletions": self.deletions,
            "runs": self.runs,
        }


@dataclass(frozen=True)
class SweepSummary:
    """Outcome of one bounded deletion run."""

    run: str
    status: str
    budget: int
    removed: int
    kinds: list[dict[str, object]]
    pending: list[str]
    at: int

    def as_dict(self) -> dict[str, object]:
        return {
            "run": self.run,
            "status": self.status,
            "budget": self.budget,
            "removed": self.removed,
            "kinds": self.kinds,
            "pending": self.pending,
            "at": self.at,
        }


@dataclass(frozen=True)
class RetentionStatus:
    """Cursor state plus per category expiry at a glance."""

    pending: bool
    cursor: dict[str, object] | None
    categories: list[dict[str, object]]
    budget: int

    def as_dict(self) -> dict[str, object]:
        return {
            "pending": self.pending,
            "cursor": self.cursor,
            "categories": self.categories,
            "budget": self.budget,
        }
