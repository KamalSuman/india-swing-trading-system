from __future__ import annotations

import ast
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing.paper_trades import LocalPaperTradeLedger, PaperTradeEventType
from india_swing.paper_trades.store import _encode, _REGISTRATION_CODEC
from india_swing.paper_outcomes import (
    PaperOutcomeReconciliationError,
    PaperOutcomeStatus,
    ReconciliationStatus,
    reconcile_paper_outcome,
    replay_paper_outcome,
)
from tests.test_paper_outcomes import _binding, _calendar, _observation, _registration


UTC = timezone.utc


def _persist_registration(ledger, registration):
    ledger.registrations_root.mkdir(parents=True, exist_ok=True)
    target = ledger.registration_path(registration.registration_id)
    ledger._create_once(target, _encode(registration, _REGISTRATION_CODEC), ledger.registrations_root)
    return registration


class PaperOutcomeReconciliationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.ledger = LocalPaperTradeLedger(Path(self.temp.name))
        self.registration = _registration()
        self.binding = _binding(self.registration)
        self.calendar = _calendar()
        _persist_registration(self.ledger, self.registration)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def replay(self, observations, *, as_of=None):
        return replay_paper_outcome(
            registration=self.registration,
            binding=self.binding,
            calendar=self.calendar,
            observations=tuple(observations),
            as_of=as_of or datetime(2026, 1, 10, tzinfo=UTC),
        )

    def test_waiting_replay_produces_no_change_and_creates_nothing(self) -> None:
        replay = self.replay((), as_of=self.registration.decision_time + timedelta(seconds=1))
        self.assertIs(replay.status, PaperOutcomeStatus.WAITING)

        result = reconcile_paper_outcome(ledger=self.ledger, replay=replay)

        self.assertIs(result.status, ReconciliationStatus.NO_CHANGE)
        self.assertEqual(result.events, ())
        self.assertEqual(result.appended_event_ids, ())
        self.assertFalse((self.ledger.events_root / self.registration.registration_id).exists())

    def test_blocked_replay_with_a_computed_entry_still_writes_nothing(self) -> None:
        observations = (
            _observation(self.calendar, date(2026, 1, 2)),
            _observation(self.calendar, date(2026, 1, 3)),
            _observation(self.calendar, date(2026, 1, 4), traded=False),
        )
        replay = self.replay(observations)
        self.assertIs(replay.status, PaperOutcomeStatus.BLOCKED)
        self.assertIsNotNone(replay.entry)

        result = reconcile_paper_outcome(ledger=self.ledger, replay=replay)

        self.assertIs(result.status, ReconciliationStatus.NO_CHANGE)
        self.assertEqual(result.events, ())
        self.assertEqual(result.appended_event_ids, ())
        self.assertFalse((self.ledger.events_root / self.registration.registration_id).exists())

    def test_open_replay_appends_one_lineage_complete_entry_and_is_idempotent(self) -> None:
        observation = _observation(self.calendar, date(2026, 1, 2))
        replay = self.replay((observation,), as_of=observation.knowledge_time + timedelta(seconds=1))
        self.assertIs(replay.status, PaperOutcomeStatus.OPEN)

        first = reconcile_paper_outcome(ledger=self.ledger, replay=replay)
        self.assertIs(first.status, ReconciliationStatus.RECONCILED)
        self.assertEqual(len(first.events), 1)
        entry = first.events[0]
        self.assertIs(entry.event_type, PaperTradeEventType.ENTRY_RECORDED)
        self.assertEqual(entry.replay_id, replay.replay_id)
        self.assertEqual(entry.outcome_policy_id, replay.policy_id)
        self.assertEqual(entry.instrument_binding_id, replay.binding_id)
        self.assertEqual(entry.calendar_snapshot_id, replay.calendar_snapshot_id)
        self.assertEqual(entry.market_session, replay.entry.market_session)

        directory = self.ledger.events_root / self.registration.registration_id
        path = next(directory.glob("*.json"))
        original_bytes = path.read_bytes()

        second = reconcile_paper_outcome(ledger=self.ledger, replay=replay)
        self.assertIs(second.status, ReconciliationStatus.NO_CHANGE)
        self.assertEqual(second.events, first.events)
        self.assertEqual(second.appended_event_ids, ())
        self.assertEqual(path.read_bytes(), original_bytes)
        self.assertEqual(len(list(directory.glob("*.json"))), 1)

    def test_closed_target_exit_reconciliation_is_idempotent(self) -> None:
        observations = (
            _observation(self.calendar, date(2026, 1, 2), high="111", low="95"),
            _observation(self.calendar, date(2026, 1, 3), high="111", low="95"),
        )
        replay = self.replay(observations)
        self.assertIs(replay.status, PaperOutcomeStatus.CLOSED)

        first = reconcile_paper_outcome(ledger=self.ledger, replay=replay)
        self.assertIs(first.status, ReconciliationStatus.RECONCILED)
        self.assertEqual(len(first.events), 2)
        entry, exit_event = first.events
        self.assertIs(entry.event_type, PaperTradeEventType.ENTRY_RECORDED)
        self.assertIs(exit_event.event_type, PaperTradeEventType.EXIT_RECORDED)
        self.assertEqual(exit_event.reason_code, "TARGET_EXIT")
        self.assertGreater(exit_event.market_session, entry.market_session)
        self.assertNotEqual(exit_event.evidence_id, entry.evidence_id)

        second = reconcile_paper_outcome(ledger=self.ledger, replay=replay)
        self.assertIs(second.status, ReconciliationStatus.NO_CHANGE)
        self.assertEqual(second.events, first.events)
        self.assertEqual(second.appended_event_ids, ())

    def test_closed_time_exit_reconciliation_is_idempotent(self) -> None:
        observations = (
            _observation(self.calendar, date(2026, 1, 2)),
            _observation(self.calendar, date(2026, 1, 3)),
            _observation(self.calendar, date(2026, 1, 4), close="104"),
        )
        replay = self.replay(observations)
        self.assertIs(replay.status, PaperOutcomeStatus.CLOSED)

        first = reconcile_paper_outcome(ledger=self.ledger, replay=replay)
        self.assertEqual(len(first.events), 2)
        self.assertEqual(first.events[1].reason_code, "TIME_EXIT")

        second = reconcile_paper_outcome(ledger=self.ledger, replay=replay)
        self.assertIs(second.status, ReconciliationStatus.NO_CHANGE)
        self.assertEqual(second.events, first.events)

    def test_closed_same_session_stop_reconciliation_is_idempotent(self) -> None:
        observation = _observation(self.calendar, date(2026, 1, 2), high="111", low="89")
        replay = self.replay((observation,))
        self.assertIs(replay.status, PaperOutcomeStatus.CLOSED)
        self.assertEqual(replay.entry.market_session, replay.exit.market_session)
        self.assertEqual(replay.entry.evidence_id, replay.exit.evidence_id)

        first = reconcile_paper_outcome(ledger=self.ledger, replay=replay)
        self.assertIs(first.status, ReconciliationStatus.RECONCILED)
        entry, exit_event = first.events
        self.assertEqual(exit_event.reason_code, "STOP_EXIT")
        self.assertEqual(exit_event.market_session, entry.market_session)
        self.assertLessEqual(exit_event.observed_price, self.registration.stop)

        second = reconcile_paper_outcome(ledger=self.ledger, replay=replay)
        self.assertIs(second.status, ReconciliationStatus.NO_CHANGE)
        self.assertEqual(second.events, first.events)

    def test_crash_after_entry_recovers_only_missing_exit(self) -> None:
        observations = (
            _observation(self.calendar, date(2026, 1, 2), high="111", low="95"),
            _observation(self.calendar, date(2026, 1, 3), high="111", low="95"),
        )
        replay = self.replay(observations)
        entry = replay.entry
        self.ledger.append(
            registration_id=self.registration.registration_id,
            event_type=PaperTradeEventType.ENTRY_RECORDED,
            occurred_at=entry.observed_at,
            observed_price=entry.price,
            evidence_id=entry.evidence_id,
            market_session=entry.market_session,
            replay_id=replay.replay_id,
            outcome_policy_id=replay.policy_id,
            instrument_binding_id=replay.binding_id,
            calendar_snapshot_id=replay.calendar_snapshot_id,
        )

        result = reconcile_paper_outcome(ledger=self.ledger, replay=replay)

        self.assertIs(result.status, ReconciliationStatus.RECONCILED)
        self.assertEqual(len(result.appended_event_ids), 1)
        self.assertEqual(len(result.events), 2)
        self.assertIs(result.events[1].event_type, PaperTradeEventType.EXIT_RECORDED)

    def test_mismatching_entry_prefix_fails_closed_without_appending(self) -> None:
        observations = (
            _observation(self.calendar, date(2026, 1, 2), high="111", low="95"),
            _observation(self.calendar, date(2026, 1, 3), high="111", low="95"),
        )
        replay = self.replay(observations)
        entry = replay.entry
        self.ledger.append(
            registration_id=self.registration.registration_id,
            event_type=PaperTradeEventType.ENTRY_RECORDED,
            occurred_at=entry.observed_at,
            observed_price=self.registration.entry_low,
            evidence_id=entry.evidence_id,
            market_session=entry.market_session,
            replay_id=replay.replay_id,
            outcome_policy_id=replay.policy_id,
            instrument_binding_id=replay.binding_id,
            calendar_snapshot_id=replay.calendar_snapshot_id,
        )

        with self.assertRaises(PaperOutcomeReconciliationError):
            reconcile_paper_outcome(ledger=self.ledger, replay=replay)

        self.assertEqual(len(self.ledger.list_events(self.registration.registration_id)), 1)

    def test_open_replay_evolves_into_closed_replay_for_the_same_registration(self) -> None:
        open_observation = _observation(self.calendar, date(2026, 1, 2))
        open_replay = self.replay(
            (open_observation,), as_of=open_observation.knowledge_time + timedelta(seconds=1)
        )
        self.assertIs(open_replay.status, PaperOutcomeStatus.OPEN)

        first = reconcile_paper_outcome(ledger=self.ledger, replay=open_replay)
        self.assertIs(first.status, ReconciliationStatus.RECONCILED)
        self.assertEqual(len(first.events), 1)
        stored_entry = first.events[0]
        self.assertEqual(stored_entry.replay_id, open_replay.replay_id)

        target_observation = _observation(self.calendar, date(2026, 1, 3), high="111", low="95")
        closed_replay = self.replay((open_observation, target_observation))
        self.assertIs(closed_replay.status, PaperOutcomeStatus.CLOSED)
        self.assertNotEqual(closed_replay.replay_id, open_replay.replay_id)

        second = reconcile_paper_outcome(ledger=self.ledger, replay=closed_replay)
        self.assertIs(second.status, ReconciliationStatus.RECONCILED)
        self.assertEqual(len(second.appended_event_ids), 1)
        self.assertEqual(len(second.events), 2)
        entry, exit_event = second.events
        self.assertEqual(entry.event_id, stored_entry.event_id)
        self.assertEqual(entry.replay_id, open_replay.replay_id)
        self.assertIs(exit_event.event_type, PaperTradeEventType.EXIT_RECORDED)
        self.assertEqual(exit_event.reason_code, "TARGET_EXIT")
        self.assertEqual(exit_event.replay_id, closed_replay.replay_id)

        third = reconcile_paper_outcome(ledger=self.ledger, replay=closed_replay)
        self.assertIs(third.status, ReconciliationStatus.NO_CHANGE)
        self.assertEqual(third.events, second.events)
        self.assertEqual(third.appended_event_ids, ())

    def _open_and_closed_replay(self):
        open_observation = _observation(self.calendar, date(2026, 1, 2))
        open_replay = self.replay(
            (open_observation,), as_of=open_observation.knowledge_time + timedelta(seconds=1)
        )
        target_observation = _observation(self.calendar, date(2026, 1, 3), high="111", low="95")
        closed_replay = self.replay((open_observation, target_observation))
        self.assertIs(closed_replay.status, PaperOutcomeStatus.CLOSED)
        return open_replay, closed_replay

    def _append_stored_entry(self, replay, replay_id, **overrides) -> None:
        entry = replay.entry
        kwargs = dict(
            registration_id=self.registration.registration_id,
            event_type=PaperTradeEventType.ENTRY_RECORDED,
            occurred_at=entry.observed_at,
            observed_price=entry.price,
            evidence_id=entry.evidence_id,
            market_session=entry.market_session,
            replay_id=replay_id,
            outcome_policy_id=replay.policy_id,
            instrument_binding_id=replay.binding_id,
            calendar_snapshot_id=replay.calendar_snapshot_id,
        )
        kwargs.update(overrides)
        self.ledger.append(**kwargs)

    def test_open_to_closed_evolution_rejects_mismatching_entry_price(self) -> None:
        open_replay, closed_replay = self._open_and_closed_replay()
        self._append_stored_entry(
            closed_replay, open_replay.replay_id, observed_price=self.registration.entry_low
        )

        with self.assertRaises(PaperOutcomeReconciliationError):
            reconcile_paper_outcome(ledger=self.ledger, replay=closed_replay)
        self.assertEqual(len(self.ledger.list_events(self.registration.registration_id)), 1)

    def test_open_to_closed_evolution_rejects_mismatching_entry_evidence_id(self) -> None:
        open_replay, closed_replay = self._open_and_closed_replay()
        self._append_stored_entry(closed_replay, open_replay.replay_id, evidence_id="9" * 64)

        with self.assertRaises(PaperOutcomeReconciliationError):
            reconcile_paper_outcome(ledger=self.ledger, replay=closed_replay)
        self.assertEqual(len(self.ledger.list_events(self.registration.registration_id)), 1)

    def test_open_to_closed_evolution_rejects_mismatching_entry_market_session(self) -> None:
        open_replay, closed_replay = self._open_and_closed_replay()
        self._append_stored_entry(
            closed_replay, open_replay.replay_id, market_session=date(2026, 1, 3)
        )

        with self.assertRaises(PaperOutcomeReconciliationError):
            reconcile_paper_outcome(ledger=self.ledger, replay=closed_replay)
        self.assertEqual(len(self.ledger.list_events(self.registration.registration_id)), 1)

    def test_open_to_closed_evolution_rejects_mismatching_entry_occurred_at(self) -> None:
        open_replay, closed_replay = self._open_and_closed_replay()
        self._append_stored_entry(
            closed_replay,
            open_replay.replay_id,
            occurred_at=closed_replay.entry.observed_at + timedelta(seconds=1),
        )

        with self.assertRaises(PaperOutcomeReconciliationError):
            reconcile_paper_outcome(ledger=self.ledger, replay=closed_replay)
        self.assertEqual(len(self.ledger.list_events(self.registration.registration_id)), 1)

    def test_open_to_closed_evolution_rejects_mismatching_policy_id(self) -> None:
        open_replay, closed_replay = self._open_and_closed_replay()
        self._append_stored_entry(
            closed_replay, open_replay.replay_id, outcome_policy_id="9" * 64
        )

        with self.assertRaises(PaperOutcomeReconciliationError):
            reconcile_paper_outcome(ledger=self.ledger, replay=closed_replay)
        self.assertEqual(len(self.ledger.list_events(self.registration.registration_id)), 1)

    def test_open_to_closed_evolution_rejects_mismatching_binding_id(self) -> None:
        open_replay, closed_replay = self._open_and_closed_replay()
        self._append_stored_entry(
            closed_replay, open_replay.replay_id, instrument_binding_id="9" * 64
        )

        with self.assertRaises(PaperOutcomeReconciliationError):
            reconcile_paper_outcome(ledger=self.ledger, replay=closed_replay)
        self.assertEqual(len(self.ledger.list_events(self.registration.registration_id)), 1)

    def test_open_to_closed_evolution_rejects_mismatching_calendar_snapshot_id(self) -> None:
        open_replay, closed_replay = self._open_and_closed_replay()
        self._append_stored_entry(
            closed_replay, open_replay.replay_id, calendar_snapshot_id="9" * 64
        )

        with self.assertRaises(PaperOutcomeReconciliationError):
            reconcile_paper_outcome(ledger=self.ledger, replay=closed_replay)
        self.assertEqual(len(self.ledger.list_events(self.registration.registration_id)), 1)

    def test_expired_replay_appends_one_lineage_complete_expiry_and_is_idempotent(self) -> None:
        observations = (
            _observation(self.calendar, date(2026, 1, 2), open="95", high="100", low="94", close="99"),
            _observation(self.calendar, date(2026, 1, 3), open="95", high="98", low="94", close="97"),
        )
        replay = self.replay(observations)
        self.assertIs(replay.status, PaperOutcomeStatus.EXPIRED)

        first = reconcile_paper_outcome(ledger=self.ledger, replay=replay)
        self.assertIs(first.status, ReconciliationStatus.RECONCILED)
        self.assertEqual(len(first.events), 1)
        expiry = first.events[0]
        self.assertIs(expiry.event_type, PaperTradeEventType.EXPIRED)
        self.assertEqual(expiry.occurred_at, replay.as_of)
        self.assertEqual(expiry.reason_code, "ENTRY_WINDOW_EXPIRED_UNFILLED")
        self.assertIsNone(expiry.market_session)
        self.assertEqual(expiry.replay_id, replay.replay_id)

        second = reconcile_paper_outcome(ledger=self.ledger, replay=replay)
        self.assertIs(second.status, ReconciliationStatus.NO_CHANGE)
        self.assertEqual(second.events, first.events)

    def test_expiry_after_an_entry_fails_closed(self) -> None:
        observations = (
            _observation(self.calendar, date(2026, 1, 2), open="95", high="100", low="94", close="99"),
            _observation(self.calendar, date(2026, 1, 3), open="95", high="98", low="94", close="97"),
        )
        replay = self.replay(observations)
        self.ledger.append(
            registration_id=self.registration.registration_id,
            event_type=PaperTradeEventType.ENTRY_RECORDED,
            occurred_at=self.registration.earliest_entry_at,
            observed_price=self.registration.entry_low,
            evidence_id="1" * 64,
            market_session=date(2026, 1, 2),
        )

        with self.assertRaises(PaperOutcomeReconciliationError):
            reconcile_paper_outcome(ledger=self.ledger, replay=replay)
        self.assertEqual(len(self.ledger.list_events(self.registration.registration_id)), 1)

    def test_expiry_against_another_terminal_event_fails_closed(self) -> None:
        observations = (
            _observation(self.calendar, date(2026, 1, 2), open="95", high="100", low="94", close="99"),
            _observation(self.calendar, date(2026, 1, 3), open="95", high="98", low="94", close="97"),
        )
        replay = self.replay(observations)
        self.ledger.append(
            registration_id=self.registration.registration_id,
            event_type=PaperTradeEventType.INVALIDATED,
            occurred_at=self.registration.decision_time,
            reason_code="BAD_SOURCE_DATA",
        )

        with self.assertRaises(PaperOutcomeReconciliationError):
            reconcile_paper_outcome(ledger=self.ledger, replay=replay)
        self.assertEqual(len(self.ledger.list_events(self.registration.registration_id)), 1)

    def test_replay_for_unregistered_registration_fails_closed(self) -> None:
        other_registration = _registration(holding_sessions=4)
        other_binding = _binding(other_registration)
        replay = replay_paper_outcome(
            registration=other_registration,
            binding=other_binding,
            calendar=self.calendar,
            observations=(),
            as_of=other_registration.decision_time + timedelta(seconds=1),
        )

        with self.assertRaises(PaperOutcomeReconciliationError):
            reconcile_paper_outcome(ledger=self.ledger, replay=replay)

    def test_recomputed_id_replay_mutation_fails_closed(self) -> None:
        replay = self.replay((), as_of=self.registration.decision_time + timedelta(seconds=1))
        object.__setattr__(replay, "reason_code", "not-a-code!!")
        object.__setattr__(replay, "replay_id", replay._calculated_id())

        with self.assertRaises(PaperOutcomeReconciliationError):
            reconcile_paper_outcome(ledger=self.ledger, replay=replay)

    def test_reconciliation_module_has_no_forbidden_capability(self) -> None:
        import india_swing.paper_outcomes.reconciliation as reconciliation_module

        source = Path(reconciliation_module.__file__).read_text(encoding="utf-8")
        tree = ast.parse(source)

        forbidden_modules = {
            "socket",
            "requests",
            "urllib",
            "http",
            "httpx",
            "subprocess",
            "os",
            "shutil",
            "google",
            "boto3",
            "ftplib",
            "smtplib",
            "sqlite3",
            "asyncio",
            "multiprocessing",
        }
        forbidden_calls = {"open", "eval", "exec", "compile", "__import__"}

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    self.assertNotIn(alias.name.split(".")[0], forbidden_modules)
            elif isinstance(node, ast.ImportFrom):
                self.assertNotIn((node.module or "").split(".")[0], forbidden_modules)
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    self.assertNotIn(node.func.id, forbidden_calls)
                elif isinstance(node.func, ast.Attribute):
                    if isinstance(node.func.value, ast.Name) and node.func.value.id == "ledger":
                        self.assertIn(
                            node.func.attr,
                            {"append", "get_registration", "list_events"},
                        )


if __name__ == "__main__":
    unittest.main()
