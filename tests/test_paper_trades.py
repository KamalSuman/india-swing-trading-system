from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.demo import build_demo
from india_swing.shadow_alerts import build_shadow_alert
from india_swing.paper_trades import (
    LocalPaperTradeLedger,
    PaperTradeConflict,
    PaperTradeError,
    PaperTradeEventType,
    PaperTradeIntegrityError,
    PaperTradeStatus,
)


IST = timezone(timedelta(hours=5, minutes=30))


def _candidate_alert():
    pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
    return build_shadow_alert(
        pipeline.run(snapshot, instruments, portfolio, reference_context)
    )


def _no_trade_alert():
    pipeline, snapshot, instruments, portfolio, reference_context = build_demo()
    locked = [replace(instrument, lower_circuit_locked=True) for instrument in instruments]
    return build_shadow_alert(
        pipeline.run(snapshot, locked, portfolio, reference_context)
    )


class PaperTradeLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.ledger = LocalPaperTradeLedger(Path(self.temp.name))
        self.alert = _candidate_alert()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_candidate_registration_is_exact_create_once_and_idempotent(self) -> None:
        first = self.ledger.register(self.alert)
        original = self.ledger.registration_path(first.registration_id).read_bytes()
        second = self.ledger.register(self.alert)

        self.assertEqual(first, second)
        self.assertEqual(first.alert_id, self.alert.alert_id)
        self.assertEqual(
            first.source_decision_integrity_hash,
            self.alert.decision.integrity_hash,
        )
        self.assertEqual(
            self.ledger.registration_path(first.registration_id).read_bytes(),
            original,
        )
        self.assertFalse(first.actionable)
        self.assertEqual(first.mode, "PAPER_ONLY")

    def test_no_trade_alert_cannot_register_a_paper_position(self) -> None:
        with self.assertRaisesRegex(PaperTradeError, "invalid"):
            self.ledger.register(_no_trade_alert())

    def test_entry_exit_chain_produces_paper_only_net_outcome(self) -> None:
        registration = self.ledger.register(self.alert)
        entry_time = registration.earliest_entry_at + timedelta(minutes=1)
        entry_session = registration.earliest_entry_at.astimezone(IST).date()
        entry_price = registration.entry_low
        exit_price = registration.target
        entry = self.ledger.append(
            registration_id=registration.registration_id,
            event_type=PaperTradeEventType.ENTRY_RECORDED,
            occurred_at=entry_time,
            observed_price=entry_price,
            evidence_id="1" * 64,
            market_session=entry_session,
        )
        exit_event = self.ledger.append(
            registration_id=registration.registration_id,
            event_type=PaperTradeEventType.EXIT_RECORDED,
            occurred_at=entry_time + timedelta(days=1),
            observed_price=exit_price,
            evidence_id="2" * 64,
            market_session=entry_session + timedelta(days=1),
        )

        summary = self.ledger.summary(registration.registration_id)

        expected_gross = (exit_price - entry_price) * registration.quantity
        self.assertIs(summary.status, PaperTradeStatus.CLOSED)
        self.assertEqual(summary.gross_pnl, expected_gross)
        self.assertEqual(
            summary.estimated_net_pnl,
            expected_gross - registration.estimated_round_trip_cost,
        )
        self.assertEqual(summary.event_ids, (entry.event_id, exit_event.event_id))
        self.assertEqual(summary.mode, "PAPER_ONLY")
        self.assertFalse(summary.actionable)

    def test_fill_requires_content_evidence(self) -> None:
        registration = self.ledger.register(self.alert)

        with self.assertRaisesRegex(PaperTradeError, "evidence"):
            self.ledger.append(
                registration_id=registration.registration_id,
                event_type=PaperTradeEventType.ENTRY_RECORDED,
                occurred_at=registration.earliest_entry_at,
                observed_price=registration.entry_low,
            )

        self.assertEqual(self.ledger.list_events(registration.registration_id), ())

    def test_entry_outside_time_or_price_plan_is_rejected(self) -> None:
        registration = self.ledger.register(self.alert)
        first_session = registration.earliest_entry_at.astimezone(IST).date()
        expiry_session = registration.entry_expires_at.astimezone(IST).date()

        with self.assertRaisesRegex(PaperTradeConflict, "validity"):
            self.ledger.append(
                registration_id=registration.registration_id,
                event_type=PaperTradeEventType.ENTRY_RECORDED,
                occurred_at=registration.entry_expires_at,
                observed_price=registration.entry_low,
                evidence_id="3" * 64,
                market_session=expiry_session + timedelta(days=1),
            )
        with self.assertRaisesRegex(PaperTradeConflict, "range"):
            self.ledger.append(
                registration_id=registration.registration_id,
                event_type=PaperTradeEventType.ENTRY_RECORDED,
                occurred_at=registration.earliest_entry_at,
                observed_price=registration.entry_high + Decimal("0.01"),
                evidence_id="4" * 64,
                market_session=first_session,
            )

    def test_exit_before_entry_and_events_after_terminal_are_rejected(self) -> None:
        registration = self.ledger.register(self.alert)
        first_session = registration.earliest_entry_at.astimezone(IST).date()
        with self.assertRaisesRegex(PaperTradeConflict, "requires"):
            self.ledger.append(
                registration_id=registration.registration_id,
                event_type=PaperTradeEventType.EXIT_RECORDED,
                occurred_at=registration.earliest_entry_at,
                observed_price=registration.target,
                evidence_id="5" * 64,
                market_session=first_session,
            )
        self.ledger.append(
            registration_id=registration.registration_id,
            event_type=PaperTradeEventType.EXPIRED,
            occurred_at=registration.entry_expires_at,
            reason_code="ENTRY_WINDOW_CLOSED",
        )
        self.assertIs(
            self.ledger.summary(registration.registration_id).status,
            PaperTradeStatus.EXPIRED,
        )
        with self.assertRaisesRegex(PaperTradeConflict, "terminal"):
            self.ledger.append(
                registration_id=registration.registration_id,
                event_type=PaperTradeEventType.INVALIDATED,
                occurred_at=registration.entry_expires_at,
                reason_code="BAD_SOURCE_DATA",
            )

    def test_same_timestamp_or_same_evidence_cannot_resolve_an_exit(self) -> None:
        registration = self.ledger.register(self.alert)
        entry_time = registration.earliest_entry_at
        entry_session = entry_time.astimezone(IST).date()
        self.ledger.append(
            registration_id=registration.registration_id,
            event_type=PaperTradeEventType.ENTRY_RECORDED,
            occurred_at=entry_time,
            observed_price=registration.entry_low,
            evidence_id="7" * 64,
            market_session=entry_session,
        )
        with self.assertRaisesRegex(PaperTradeConflict, "matching automated stop"):
            self.ledger.append(
                registration_id=registration.registration_id,
                event_type=PaperTradeEventType.EXIT_RECORDED,
                occurred_at=entry_time,
                observed_price=registration.target,
                evidence_id="8" * 64,
                market_session=entry_session,
            )
        with self.assertRaisesRegex(PaperTradeConflict, "independently"):
            self.ledger.append(
                registration_id=registration.registration_id,
                event_type=PaperTradeEventType.EXIT_RECORDED,
                occurred_at=entry_time + timedelta(days=1),
                observed_price=registration.target,
                evidence_id="7" * 64,
                market_session=entry_session + timedelta(days=1),
            )

    def test_same_session_automated_stop_exit_is_accepted(self) -> None:
        registration = self.ledger.register(self.alert)
        entry_time = registration.earliest_entry_at
        entry_session = entry_time.astimezone(IST).date()
        lineage = dict(
            replay_id="9" * 64,
            outcome_policy_id="a" * 64,
            instrument_binding_id="b" * 64,
            calendar_snapshot_id="c" * 64,
        )
        self.ledger.append(
            registration_id=registration.registration_id,
            event_type=PaperTradeEventType.ENTRY_RECORDED,
            occurred_at=entry_time,
            observed_price=registration.entry_low,
            evidence_id="7" * 64,
            market_session=entry_session,
            **lineage,
        )
        exit_event = self.ledger.append(
            registration_id=registration.registration_id,
            event_type=PaperTradeEventType.EXIT_RECORDED,
            occurred_at=entry_time,
            observed_price=registration.stop,
            evidence_id="7" * 64,
            reason_code="STOP_EXIT",
            market_session=entry_session,
            **lineage,
        )
        self.assertIs(
            self.ledger.summary(registration.registration_id).status,
            PaperTradeStatus.CLOSED,
        )
        self.assertEqual(exit_event.reason_code, "STOP_EXIT")

    def test_same_session_target_or_time_exit_is_rejected_even_with_matching_replay(self) -> None:
        registration = self.ledger.register(self.alert)
        entry_time = registration.earliest_entry_at
        entry_session = entry_time.astimezone(IST).date()
        lineage = dict(
            replay_id="9" * 64,
            outcome_policy_id="a" * 64,
            instrument_binding_id="b" * 64,
            calendar_snapshot_id="c" * 64,
        )
        self.ledger.append(
            registration_id=registration.registration_id,
            event_type=PaperTradeEventType.ENTRY_RECORDED,
            occurred_at=entry_time,
            observed_price=registration.entry_low,
            evidence_id="7" * 64,
            market_session=entry_session,
            **lineage,
        )
        with self.assertRaisesRegex(PaperTradeConflict, "matching automated stop"):
            self.ledger.append(
                registration_id=registration.registration_id,
                event_type=PaperTradeEventType.EXIT_RECORDED,
                occurred_at=entry_time,
                observed_price=registration.target,
                evidence_id="8" * 64,
                reason_code="TARGET_EXIT",
                market_session=entry_session,
                **lineage,
            )

    def test_same_session_stop_exit_above_registered_stop_is_rejected(self) -> None:
        registration = self.ledger.register(self.alert)
        entry_time = registration.earliest_entry_at
        entry_session = entry_time.astimezone(IST).date()
        lineage = dict(
            replay_id="9" * 64,
            outcome_policy_id="a" * 64,
            instrument_binding_id="b" * 64,
            calendar_snapshot_id="c" * 64,
        )
        self.ledger.append(
            registration_id=registration.registration_id,
            event_type=PaperTradeEventType.ENTRY_RECORDED,
            occurred_at=entry_time,
            observed_price=registration.entry_low,
            evidence_id="7" * 64,
            market_session=entry_session,
            **lineage,
        )
        with self.assertRaisesRegex(PaperTradeConflict, "matching automated stop"):
            self.ledger.append(
                registration_id=registration.registration_id,
                event_type=PaperTradeEventType.EXIT_RECORDED,
                occurred_at=entry_time,
                observed_price=registration.stop + Decimal("0.01"),
                evidence_id="8" * 64,
                reason_code="STOP_EXIT",
                market_session=entry_session,
                **lineage,
            )

    def test_same_session_exit_without_matching_replay_lineage_is_rejected(self) -> None:
        registration = self.ledger.register(self.alert)
        entry_time = registration.earliest_entry_at
        entry_session = entry_time.astimezone(IST).date()
        self.ledger.append(
            registration_id=registration.registration_id,
            event_type=PaperTradeEventType.ENTRY_RECORDED,
            occurred_at=entry_time,
            observed_price=registration.entry_low,
            evidence_id="7" * 64,
            market_session=entry_session,
            replay_id="9" * 64,
            outcome_policy_id="a" * 64,
            instrument_binding_id="b" * 64,
            calendar_snapshot_id="c" * 64,
        )
        with self.assertRaisesRegex(PaperTradeConflict, "matching automated stop"):
            self.ledger.append(
                registration_id=registration.registration_id,
                event_type=PaperTradeEventType.EXIT_RECORDED,
                occurred_at=entry_time,
                observed_price=registration.stop,
                evidence_id="8" * 64,
                reason_code="STOP_EXIT",
                market_session=entry_session,
                replay_id="d" * 64,
                outcome_policy_id="a" * 64,
                instrument_binding_id="b" * 64,
                calendar_snapshot_id="c" * 64,
            )

    def test_malformed_or_partial_lineage_is_rejected(self) -> None:
        registration = self.ledger.register(self.alert)
        first_session = registration.earliest_entry_at.astimezone(IST).date()
        with self.assertRaisesRegex(PaperTradeError, "SHA-256"):
            self.ledger.append(
                registration_id=registration.registration_id,
                event_type=PaperTradeEventType.ENTRY_RECORDED,
                occurred_at=registration.earliest_entry_at,
                observed_price=registration.entry_low,
                evidence_id="1" * 64,
                market_session=first_session,
                replay_id="not-a-hash",
                outcome_policy_id="a" * 64,
                instrument_binding_id="b" * 64,
                calendar_snapshot_id="c" * 64,
            )
        with self.assertRaisesRegex(PaperTradeError, "fully present or fully absent"):
            self.ledger.append(
                registration_id=registration.registration_id,
                event_type=PaperTradeEventType.ENTRY_RECORDED,
                occurred_at=registration.earliest_entry_at,
                observed_price=registration.entry_low,
                evidence_id="1" * 64,
                market_session=first_session,
                replay_id="9" * 64,
            )

    def test_market_session_required_on_fill_and_forbidden_on_manual_non_fill(self) -> None:
        registration = self.ledger.register(self.alert)
        first_session = registration.earliest_entry_at.astimezone(IST).date()
        with self.assertRaisesRegex(PaperTradeError, "market_session"):
            self.ledger.append(
                registration_id=registration.registration_id,
                event_type=PaperTradeEventType.ENTRY_RECORDED,
                occurred_at=registration.earliest_entry_at,
                observed_price=registration.entry_low,
                evidence_id="1" * 64,
            )
        with self.assertRaisesRegex(PaperTradeError, "market_session"):
            self.ledger.append(
                registration_id=registration.registration_id,
                event_type=PaperTradeEventType.EXPIRED,
                occurred_at=registration.entry_expires_at,
                reason_code="ENTRY_WINDOW_CLOSED",
                market_session=first_session,
            )

    def test_tampered_event_breaks_content_identity(self) -> None:
        registration = self.ledger.register(self.alert)
        first_session = registration.earliest_entry_at.astimezone(IST).date()
        event = self.ledger.append(
            registration_id=registration.registration_id,
            event_type=PaperTradeEventType.ENTRY_RECORDED,
            occurred_at=registration.earliest_entry_at,
            observed_price=registration.entry_low,
            evidence_id="6" * 64,
            market_session=first_session,
        )
        path = next((self.ledger.events_root / registration.registration_id).glob("*.json"))
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["event"]["observed_price"] = str(registration.entry_high)
        path.write_text(json.dumps(payload), encoding="utf-8")

        with self.assertRaisesRegex(PaperTradeIntegrityError, "read"):
            self.ledger.list_events(registration.registration_id)
        self.assertEqual(event.sequence, 1)

    def test_altered_codec_schema_version_fails_closed(self) -> None:
        registration = self.ledger.register(self.alert)
        first_session = registration.earliest_entry_at.astimezone(IST).date()
        self.ledger.append(
            registration_id=registration.registration_id,
            event_type=PaperTradeEventType.ENTRY_RECORDED,
            occurred_at=registration.earliest_entry_at,
            observed_price=registration.entry_low,
            evidence_id="6" * 64,
            market_session=first_session,
        )
        path = next((self.ledger.events_root / registration.registration_id).glob("*.json"))
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["codec_schema_version"] = "paper-trade-event-json/v1"
        path.write_text(json.dumps(payload), encoding="utf-8")

        with self.assertRaisesRegex(PaperTradeIntegrityError, "could not be read"):
            self.ledger.list_events(registration.registration_id)

    def test_duplicate_and_gapped_event_files_fail_closed(self) -> None:
        registration = self.ledger.register(self.alert)
        first_session = registration.earliest_entry_at.astimezone(IST).date()
        self.ledger.append(
            registration_id=registration.registration_id,
            event_type=PaperTradeEventType.ENTRY_RECORDED,
            occurred_at=registration.earliest_entry_at,
            observed_price=registration.entry_low,
            evidence_id="6" * 64,
            market_session=first_session,
        )
        self.ledger.append(
            registration_id=registration.registration_id,
            event_type=PaperTradeEventType.EXIT_RECORDED,
            occurred_at=registration.earliest_entry_at + timedelta(days=1),
            observed_price=registration.target,
            evidence_id="7" * 64,
            market_session=first_session + timedelta(days=1),
        )
        directory = self.ledger.events_root / registration.registration_id
        first_path = next(directory.glob("00000000000000000001-*.json"))

        duplicate_path = directory / first_path.name.replace(
            "00000000000000000001", "00000000000000000003"
        )
        duplicate_path.write_bytes(first_path.read_bytes())
        with self.assertRaisesRegex(PaperTradeIntegrityError, "identity differs"):
            self.ledger.list_events(registration.registration_id)
        duplicate_path.unlink()

        first_path.unlink()
        with self.assertRaisesRegex(PaperTradeIntegrityError, "chain is broken"):
            self.ledger.list_events(registration.registration_id)

    def test_rehashed_registration_tamper_still_differs_from_filename(self) -> None:
        registration = self.ledger.register(self.alert)
        path = self.ledger.registration_path(registration.registration_id)
        payload = json.loads(path.read_text(encoding="utf-8"))
        object.__setattr__(registration, "source_run_id", "changed-run")
        object.__setattr__(registration, "registration_id", registration._calculated_id())
        payload["registration"]["source_run_id"] = registration.source_run_id
        payload["registration"]["registration_id"] = registration.registration_id
        path.write_text(json.dumps(payload), encoding="utf-8")

        with self.assertRaisesRegex(PaperTradeIntegrityError, "path"):
            self.ledger.get_registration(path.stem)

    def test_mutated_alert_is_rejected_before_store_creation(self) -> None:
        object.__setattr__(self.alert.decision, "target", self.alert.decision.stop)

        with self.assertRaisesRegex(PaperTradeError, "invalid"):
            self.ledger.register(self.alert)

        self.assertFalse(self.ledger.registrations_root.exists())


if __name__ == "__main__":
    unittest.main()
