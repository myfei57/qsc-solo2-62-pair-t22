"""Append only hash chained ledger: the retention trail.

Window changes, sweep runs and the archived original events all live in one
chain inside the shared store. Every entry digests its predecessor, so a
rewritten or dropped entry breaks the chain: appends refuse to continue and
``verify`` reports the tampering. The ledger is the single source the audit
endpoints read from, so the console and the storage never disagree.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass

from coldstore.store.commands import append_command, load_commands
from coldstore.store.store import Store

LEDGER_KEY = "retention:ledger"


def _canonical(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def event_digest(payload: dict[str, object]) -> str:
    """Stable digest of one stored event, used to deduplicate the archive."""

    return hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest()


def _entry_digest(seq: int, at: int, kind: str, payload: dict[str, object], prev: str) -> str:
    body = {"seq": seq, "at": at, "kind": kind, "payload": payload, "prev": prev}
    return hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class LedgerEntry:
    """One link in the retention chain."""

    seq: int
    at: int
    kind: str
    payload: dict[str, object]
    prev: str
    digest: str

    def as_dict(self) -> dict[str, object]:
        return {
            "seq": self.seq,
            "at": self.at,
            "kind": self.kind,
            "payload": self.payload,
            "prev": self.prev,
            "digest": self.digest,
        }


def _chain_ok(entries: list[LedgerEntry]) -> bool:
    prev = ""
    for position, entry in enumerate(entries, start=1):
        if entry.seq != position or entry.prev != prev:
            return False
        if entry.digest != _entry_digest(
            entry.seq, entry.at, entry.kind, entry.payload, entry.prev
        ):
            return False
        prev = entry.digest
    return True


class Ledger:
    """Reads and appends the retention ledger stored in the shared store."""

    def __init__(self, store: Store) -> None:
        self._store = store

    def entries(self) -> list[LedgerEntry]:
        entries: list[LedgerEntry] = []
        for item in load_commands(self._store, LEDGER_KEY):
            try:
                payload = json.loads(item)
            except ValueError:
                continue
            if not isinstance(payload, dict):
                continue
            body = payload.get("payload")
            entries.append(
                LedgerEntry(
                    seq=int(payload.get("seq", 0)),
                    at=int(payload.get("at", 0)),
                    kind=str(payload.get("kind", "")),
                    payload=dict(body) if isinstance(body, dict) else {},
                    prev=str(payload.get("prev", "")),
                    digest=str(payload.get("digest", "")),
                )
            )
        return entries

    def append(self, kind: str, payload: dict[str, object], at: int | None = None) -> LedgerEntry:
        """Append one entry, refusing to extend a tampered chain."""

        entries = self.entries()
        if not _chain_ok(entries):
            raise ValueError("retention ledger failed its integrity check")
        seq = len(entries) + 1
        prev = entries[-1].digest if entries else ""
        moment = int(time.time()) if at is None else int(at)
        digest = _entry_digest(seq, moment, kind, payload, prev)
        entry = LedgerEntry(
            seq=seq, at=moment, kind=kind, payload=dict(payload), prev=prev, digest=digest
        )
        append_command(self._store, LEDGER_KEY, json.dumps(entry.as_dict(), ensure_ascii=False))
        return entry

    def verify(self) -> bool:
        return _chain_ok(self.entries())

    def count(self) -> int:
        return len(self.entries())

    def by_kind(self, kind: str) -> list[LedgerEntry]:
        return [entry for entry in self.entries() if entry.kind == kind]

    def archived(self, category: str = "") -> list[dict[str, object]]:
        """Flatten archived original events, oldest first."""

        events: list[dict[str, object]] = []
        for entry in self.by_kind("archive"):
            archived_category = str(entry.payload.get("category", ""))
            if category and archived_category != category:
                continue
            batch = entry.payload.get("events", [])
            if not isinstance(batch, list):
                continue
            for event in batch:
                if not isinstance(event, dict):
                    continue
                events.append(
                    {
                        "at": int(event.get("at", 0)),
                        "kind": str(event.get("kind", "")),
                        "value": str(event.get("value", "")),
                        "category": archived_category,
                        "run": str(entry.payload.get("run", "")),
                        "archived_at": entry.at,
                    }
                )
        return events

    def archived_digests(self, category: str) -> dict[str, int]:
        """Count archived copies of each event digest for one category."""

        counts: dict[str, int] = {}
        for entry in self.by_kind("archive"):
            if str(entry.payload.get("category", "")) != category:
                continue
            batch = entry.payload.get("events", [])
            if not isinstance(batch, list):
                continue
            for event in batch:
                if isinstance(event, dict):
                    digest = event_digest(event)
                    counts[digest] = counts.get(digest, 0) + 1
        return counts

    def runs(self) -> list[dict[str, object]]:
        """Derive one view per sweep run from the begin/category/end entries.

        A run with a begin entry but no end entry was interrupted before it
        could record its outcome, so it is reported as ``interrupted``.
        """

        views: list[dict[str, object]] = []
        by_run: dict[str, dict[str, object]] = {}
        for entry in self.by_kind("sweep"):
            run_id = str(entry.payload.get("run", ""))
            view = by_run.get(run_id)
            if view is None:
                view = {
                    "run": run_id,
                    "began_at": 0,
                    "ended_at": 0,
                    "budget": 0,
                    "status": "interrupted",
                    "scanned": 0,
                    "deleted": 0,
                    "resumed_from": "",
                    "continues": "",
                    "categories": [],
                }
                by_run[run_id] = view
                views.append(view)
            phase = str(entry.payload.get("phase", ""))
            if phase == "begin":
                view["began_at"] = entry.at
                view["budget"] = int(entry.payload.get("budget", 0))
                view["resumed_from"] = str(entry.payload.get("resumed_from", ""))
                view["continues"] = str(entry.payload.get("continues", ""))
            elif phase == "category":
                categories = view["categories"]
                if isinstance(categories, list):
                    categories.append(
                        {
                            "category": str(entry.payload.get("category", "")),
                            "scanned": int(entry.payload.get("scanned", 0)),
                            "deleted": int(entry.payload.get("deleted", 0)),
                            "kept": int(entry.payload.get("kept", 0)),
                        }
                    )
            elif phase == "end":
                view["ended_at"] = entry.at
                view["status"] = str(entry.payload.get("status", "done"))
                view["scanned"] = int(entry.payload.get("scanned", 0))
                view["deleted"] = int(entry.payload.get("deleted", 0))
        return views
