"""Retention projections."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RetentionState:
    """Configured windows, live and archived totals and the last run status."""

    windows: dict[str, int]
    live: int
    archived: int
    runs: int
    last_status: str
    ledger_ok: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "windows": self.windows,
            "live": self.live,
            "archived": self.archived,
            "runs": self.runs,
            "last_status": self.last_status,
            "ledger_ok": self.ledger_ok,
        }
