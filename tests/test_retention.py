"""Behavioural tests for retention windows and the deletion ledger."""

from __future__ import annotations

import json
import tempfile
import unittest

from coldstore.console.seed import seed_defaults
from coldstore.console.server import Server
from coldstore.ledger import EVENTS_KEY, Ledger, Sweeper
from coldstore.store import Store

OLD = 1_000_000
NOW = 2_000_000


class RetentionCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.store = Store.open(f"{self._tmp.name}/state.json")
        seed_defaults(self.store)
        self.server = Server(self.store)
        self.ledger: Ledger = self.server.runtime.ledger
        self.sweeper: Sweeper = self.server.runtime.sweeper

    def tearDown(self) -> None:
        self.store.close()
        self._tmp.cleanup()

    def call(self, method: str, path: str, payload: dict | None = None):
        response = self.server.dispatch(method, path, payload)
        body = response.body.decode("utf-8")
        parsed = json.loads(body) if response.content_type.startswith("application/json") else body
        return response.status, parsed

    def add_events(self, kind: str, count: int, at: int) -> None:
        for index in range(count):
            self.ledger.append_event(kind, f"{kind}-{index}", at=at)

    def deletion_records(self, phase: str) -> list[dict]:
        return [d for d in self.ledger.deletions() if d.get("phase") == phase]

    def test_window_is_recorded_in_the_ledger(self) -> None:
        status, record = self.call("POST", "/retention/window", {"kind": "manual", "seconds": 3600})
        self.assertEqual(status, 200)
        self.assertEqual(record["type"], "window")

        status, windows = self.call("GET", "/retention/windows")
        self.assertEqual(status, 200)
        self.assertEqual(windows["windows"], {"manual": 3600})
        self.assertEqual(len(windows["history"]), 1)

        status, ledger = self.call("GET", "/ledger")
        self.assertEqual(status, 200)
        self.assertEqual(ledger["windows"], {"manual": 3600})
        self.assertEqual(len(ledger["window_records"]), 1)

    def test_window_must_be_positive(self) -> None:
        status, error = self.call("POST", "/retention/window", {"kind": "manual", "seconds": 0})
        self.assertEqual(status, 400)
        self.assertIn("window", error["error"])
        status, error = self.call("POST", "/retention/window", {"seconds": 10})
        self.assertEqual(status, 400)
        self.assertIn("kind", error["error"])

    def test_sweep_removes_only_expired_events(self) -> None:
        self.add_events("manual", 3, at=OLD)
        self.add_events("manual", 2, at=NOW)
        self.ledger.set_window("manual", 3600)

        summary = self.sweeper.run(budget=10, now=NOW)
        self.assertEqual(summary.status, "completed")
        self.assertEqual(summary.removed, 3)

        remaining = [event for event in self.ledger.events() if event.kind == "manual"]
        self.assertEqual(len(remaining), 2)

        planned = self.deletion_records("planned")
        applied = self.deletion_records("applied")
        self.assertEqual(len(planned), 1)
        self.assertEqual(len(applied), 1)
        self.assertEqual(planned[0]["removed"], 3)
        self.assertEqual(planned[0]["ids"], applied[0]["ids"])
        self.assertEqual(planned[0]["digest"], applied[0]["digest"])
        self.assertEqual(planned[0]["cutoff"], NOW - 3600)
        runs = self.ledger.runs()
        self.assertEqual(runs[-1]["status"], "completed")
        self.assertEqual(runs[-1]["removed"], 3)

    def test_kind_without_window_is_never_swept(self) -> None:
        self.add_events("manual", 2, at=OLD)
        self.add_events("alarm", 2, at=OLD)
        self.ledger.set_window("manual", 3600)

        summary = self.sweeper.run(budget=10, now=NOW)
        self.assertEqual(summary.removed, 2)
        self.assertEqual(len([e for e in self.ledger.events() if e.kind == "alarm"]), 2)

        status = self.sweeper.status(now=NOW)
        categories = {c["kind"]: c for c in status.categories}
        self.assertIsNone(categories["alarm"]["window"])
        self.assertEqual(categories["alarm"]["expired"], 0)

    def test_budget_splits_run_and_resume_finishes(self) -> None:
        for kind in ("alpha", "beta", "gamma"):
            self.add_events(kind, 2, at=OLD)
            self.ledger.set_window(kind, 3600)

        first = self.sweeper.run(budget=3, now=NOW)
        self.assertEqual(first.status, "partial")
        self.assertEqual(first.removed, 2)
        self.assertEqual(first.pending, ["beta", "gamma"])

        status = self.sweeper.status(now=NOW)
        self.assertTrue(status.pending)
        self.assertEqual(status.cursor["kinds"], ["beta", "gamma"])

        second = self.sweeper.run(budget=3, now=NOW + 100)
        self.assertEqual(second.status, "partial")
        self.assertEqual(second.run, first.run)

        third = self.sweeper.run(budget=3, now=NOW + 200)
        self.assertEqual(third.status, "completed")
        self.assertEqual(third.run, first.run)
        self.assertFalse(self.sweeper.status(now=NOW).pending)

        self.assertEqual(self.ledger.event_count(), 1)  # only the seed event
        self.assertEqual([r["status"] for r in self.ledger.runs()], ["partial", "partial", "completed"])
        # the resumed run kept the original as_of cutoff
        cutoffs = {d["cutoff"] for d in self.deletion_records("planned")}
        self.assertEqual(cutoffs, {NOW - 3600})

    def test_overshoot_is_bounded_to_one_category(self) -> None:
        self.add_events("alpha", 10, at=OLD)
        self.add_events("beta", 1, at=OLD)
        self.ledger.set_window("alpha", 3600)
        self.ledger.set_window("beta", 3600)

        summary = self.sweeper.run(budget=5, now=NOW)
        self.assertEqual(summary.status, "partial")
        self.assertEqual(summary.removed, 10)  # one category always finishes
        self.assertEqual(summary.pending, ["beta"])

    def test_crash_between_tombstone_and_purge_is_reconciled(self) -> None:
        self.add_events("manual", 3, at=OLD)
        self.ledger.set_window("manual", 3600)

        original_purge = self.ledger.purge

        def failing_purge(ids: list[str]) -> int:
            raise RuntimeError("simulated crash")

        self.ledger.purge = failing_purge  # type: ignore[assignment]
        with self.assertRaises(RuntimeError):
            self.sweeper.run(budget=10, now=NOW)
        self.ledger.purge = original_purge  # type: ignore[assignment]

        # the tombstone is there, the events are still there
        self.assertEqual(len(self.deletion_records("planned")), 1)
        self.assertEqual(len(self.deletion_records("applied")), 0)
        self.assertEqual(len([e for e in self.ledger.events() if e.kind == "manual"]), 3)

        summary = self.sweeper.run(budget=10, now=NOW)
        self.assertEqual(summary.status, "completed")
        self.assertEqual(summary.removed, 3)
        self.assertEqual(len([e for e in self.ledger.events() if e.kind == "manual"]), 0)
        self.assertEqual(len(self.deletion_records("planned")), 1)
        self.assertEqual(len(self.deletion_records("applied")), 1)

    def test_ledger_and_history_read_the_same_events(self) -> None:
        self.call("POST", "/history", {"kind": "manual", "value": "start"})
        _, history = self.call("GET", "/history")
        _, ledger = self.call("GET", "/ledger")
        self.assertEqual(
            [event["id"] for event in history["events"]],
            [event["id"] for event in ledger["events"]],
        )

    def test_manual_clear_leaves_a_tombstone(self) -> None:
        self.call("POST", "/history", {"kind": "manual", "value": "start"})
        status, cleared = self.call("POST", "/history/clear")
        self.assertEqual(status, 200)
        self.assertEqual(cleared["removed"], 2)  # seed event plus the posted one

        _, history = self.call("GET", "/history")
        self.assertEqual(history["count"], 0)

        _, ledger = self.call("GET", "/ledger")
        self.assertEqual(len(ledger["deletions"]), 1)
        record = ledger["deletions"][0]
        self.assertEqual(record["run"], "manual")
        self.assertEqual(record["removed"], 2)
        self.assertEqual(len(record["ids"]), 2)

    def test_filed_events_cannot_be_altered_without_detection(self) -> None:
        event = self.ledger.append_event("manual", "original", at=NOW)
        self.assertTrue(self.ledger.verify()["ok"])

        raw, _ = self.store.get(EVENTS_KEY)
        items = json.loads(raw)
        tampered = []
        for item in items:
            payload = json.loads(item)
            if payload["id"] == event.id:
                payload["value"] = "rewritten"
            tampered.append(json.dumps(payload, ensure_ascii=False))
        self.store.put(EVENTS_KEY, json.dumps(tampered, ensure_ascii=False))

        status, verdict = self.call("GET", "/ledger/verify")
        self.assertEqual(status, 200)
        self.assertFalse(verdict["ok"])
        self.assertEqual(verdict["mismatches"], [event.id])

    def test_sweep_endpoint_and_status(self) -> None:
        self.add_events("manual", 2, at=OLD)
        status, _ = self.call("POST", "/retention/window", {"kind": "manual", "seconds": 3600})
        self.assertEqual(status, 200)

        status, summary = self.call("POST", "/retention/sweep", {"budget": 100})
        self.assertEqual(status, 200)
        self.assertEqual(summary["status"], "completed")
        self.assertEqual(summary["removed"], 2)

        status, state = self.call("GET", "/retention/status")
        self.assertEqual(status, 200)
        self.assertFalse(state["pending"])
        categories = {c["kind"]: c for c in state["categories"]}
        self.assertEqual(categories["manual"]["events"], 0)

        status, error = self.call("POST", "/retention/sweep", {"budget": 0})
        self.assertEqual(status, 400)
        self.assertIn("budget", error["error"])


if __name__ == "__main__":
    unittest.main()
