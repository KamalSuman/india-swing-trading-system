from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.daily_workflow import (
    DailyPaperWorkflowError,
    DailyPaperWorkflowEventStatus,
    DailyPaperWorkflowExecutionError,
    DailyPaperWorkflowOutput,
    DailyPaperWorkflowOutputStatus,
    DailyPaperWorkflowRejected,
    DailyPaperWorkflowRetryExhausted,
    DailyPaperWorkflowSpec,
    DailyPaperWorkflowTerminal,
    LocalDailyPaperWorkflowStore,
    LocalDailyPaperWorkflowWorker,
    PublishedManifestPin,
    run_daily_paper_workflow,
)
from india_swing.notifications import TelegramBotConfig
from india_swing.daily_workflow.store import (
    decode_workflow_event,
    decode_workflow_spec,
    decode_workflow_terminal,
    encode_workflow_event,
    encode_workflow_spec,
    encode_workflow_terminal,
)


UTC = timezone.utc
NOW = datetime(2026, 7, 22, 12, tzinfo=UTC)


class _Clock:
    def __init__(self) -> None:
        self.value = NOW

    def __call__(self) -> datetime:
        result = self.value
        self.value += timedelta(seconds=1)
        return result


class _Worker:
    def __init__(self, *results: object) -> None:
        self.results = list(results)
        self.calls: list[str] = []

    def execute(self, spec: DailyPaperWorkflowSpec) -> DailyPaperWorkflowOutput:
        self.calls.append(spec.workflow_id)
        result = self.results.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result


class _UnusedWriter:
    def __init__(self) -> None:
        self.calls = 0

    def create_or_verify(self, **_: object) -> object:
        self.calls += 1
        raise AssertionError("no-position workflow must not publish portfolio state")


class _TelegramTransport:
    def __init__(self) -> None:
        self.calls = 0

    def post_json(self, **_: object) -> bytes:
        self.calls += 1
        return b'{"ok":true,"result":{"message_id":7}}'


def _spec(*, maximum_attempts: int = 3) -> DailyPaperWorkflowSpec:
    return DailyPaperWorkflowSpec(
        run_id="a" * 64,
        derived_evidence_id="b" * 64,
        state_bucket="paper-state-bucket",
        daily_loss_limit=Decimal("1000"),
        cumulative_loss_limit=Decimal("2000"),
        maximum_attempts=maximum_attempts,
    )


def _pin(name: str, marker: str) -> PublishedManifestPin:
    return PublishedManifestPin(
        object_name=name,
        generation=1,
        sha256=marker * 64,
    )


def _complete_output() -> DailyPaperWorkflowOutput:
    pins = tuple(
        sorted(
            (
                _pin("paper-outcomes/a/manifests/a.json", "1"),
                _pin("paper-outcomes/b/manifests/b.json", "2"),
            ),
            key=lambda value: value.pin_id,
        )
    )
    return DailyPaperWorkflowOutput(
        status=DailyPaperWorkflowOutputStatus.COMPLETE,
        preparation_id="c" * 64,
        batch_id="d" * 64,
        state_id="e" * 64,
        outcome_manifest_pins=pins,
        portfolio_manifest_pin=_pin("paper-portfolios/d/manifests/e.json", "3"),
        telegram_receipt_id="f" * 64,
    )


def _empty_output() -> DailyPaperWorkflowOutput:
    return DailyPaperWorkflowOutput(
        status=DailyPaperWorkflowOutputStatus.NO_ACTIVE_POSITIONS,
        preparation_id=None,
        batch_id=None,
        state_id=None,
        outcome_manifest_pins=(),
        portfolio_manifest_pin=None,
        telegram_receipt_id="9" * 64,
    )


class DailyWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store = LocalDailyPaperWorkflowStore(self.root)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_canonical_spec_event_and_terminal_codecs_round_trip(self) -> None:
        spec = _spec()
        output = _complete_output()
        worker = _Worker(output)
        terminal = run_daily_paper_workflow(
            spec=spec, worker=worker, store=self.store, clock=_Clock()
        )
        events = self.store.list_events(spec.workflow_id)

        self.assertEqual(decode_workflow_spec(encode_workflow_spec(spec)), spec)
        self.assertEqual(
            decode_workflow_terminal(encode_workflow_terminal(terminal)), terminal
        )
        for event in events:
            self.assertEqual(decode_workflow_event(encode_workflow_event(event)), event)
        with self.assertRaises(DailyPaperWorkflowError):
            decode_workflow_spec(
                encode_workflow_spec(spec).replace(
                    b'"maximum_attempts":3', b'"maximum_attempts":3,"maximum_attempts":3'
                )
            )

    def test_complete_run_is_terminal_last_and_retry_does_not_call_worker(self) -> None:
        spec = _spec()
        worker = _Worker(_complete_output())
        first = run_daily_paper_workflow(
            spec=spec, worker=worker, store=self.store, clock=_Clock()
        )
        second = run_daily_paper_workflow(
            spec=spec, worker=worker, store=self.store, clock=_Clock()
        )

        self.assertEqual(second, first)
        self.assertEqual(worker.calls, [spec.workflow_id])
        self.assertEqual(
            tuple(value.status for value in self.store.list_events(spec.workflow_id)),
            (
                DailyPaperWorkflowEventStatus.STARTED,
                DailyPaperWorkflowEventStatus.COMPLETED,
            ),
        )
        self.assertEqual(
            self.store.list_events(spec.workflow_id)[-1].terminal_id,
            first.terminal_id,
        )

    def test_no_active_positions_is_a_durable_non_trade_terminal(self) -> None:
        spec = _spec()
        worker = _Worker(_empty_output())
        terminal = run_daily_paper_workflow(
            spec=spec, worker=worker, store=self.store, clock=_Clock()
        )

        self.assertIs(
            terminal.output.status,
            DailyPaperWorkflowOutputStatus.NO_ACTIVE_POSITIONS,
        )
        events = self.store.list_events(spec.workflow_id)
        self.assertEqual(events[-1].status, DailyPaperWorkflowEventStatus.REJECTED)
        self.assertEqual(events[-1].reason_code, "NO_ACTIVE_POSITIONS")
        self.assertIsNone(events[-1].terminal_id)
        self.assertEqual(
            run_daily_paper_workflow(
                spec=spec, worker=worker, store=self.store, clock=_Clock()
            ),
            terminal,
        )
        self.assertEqual(len(worker.calls), 1)

    def test_failures_are_sanitized_and_retry_budget_is_bounded(self) -> None:
        spec = _spec(maximum_attempts=2)
        worker = _Worker(RuntimeError("secret-one"), RuntimeError("secret-two"))
        clock = _Clock()

        for _ in range(2):
            with self.assertRaisesRegex(
                DailyPaperWorkflowExecutionError, "failed safely"
            ) as raised:
                run_daily_paper_workflow(
                    spec=spec, worker=worker, store=self.store, clock=clock
                )
            self.assertNotIn("secret", str(raised.exception))
        with self.assertRaises(DailyPaperWorkflowRetryExhausted):
            run_daily_paper_workflow(
                spec=spec, worker=worker, store=self.store, clock=clock
            )

        events = self.store.list_events(spec.workflow_id)
        self.assertEqual(
            tuple(value.status for value in events),
            (
                DailyPaperWorkflowEventStatus.STARTED,
                DailyPaperWorkflowEventStatus.FAILED,
                DailyPaperWorkflowEventStatus.STARTED,
                DailyPaperWorkflowEventStatus.FAILED,
            ),
        )
        self.assertTrue(
            all(
                value.reason_code == "WORKFLOW_EXECUTION_FAILED"
                for value in events
                if value.status is DailyPaperWorkflowEventStatus.FAILED
            )
        )
        self.assertIsNone(self.store.get_terminal(spec.workflow_id))
        self.assertEqual(len(worker.calls), 2)

    def test_domain_rejection_is_recorded_without_leaking_exception_text(self) -> None:
        spec = _spec()
        worker = _Worker(DailyPaperWorkflowRejected("EVIDENCE_REJECTED"))
        with self.assertRaises(DailyPaperWorkflowRejected):
            run_daily_paper_workflow(
                spec=spec, worker=worker, store=self.store, clock=_Clock()
            )
        events = self.store.list_events(spec.workflow_id)
        self.assertEqual(events[-1].status, DailyPaperWorkflowEventStatus.REJECTED)
        self.assertEqual(events[-1].reason_code, "EVIDENCE_REJECTED")
        self.assertIsNone(self.store.get_terminal(spec.workflow_id))

    def test_invalid_worker_output_never_creates_a_terminal(self) -> None:
        spec = _spec()
        worker = _Worker(object())
        with self.assertRaises(DailyPaperWorkflowExecutionError):
            run_daily_paper_workflow(
                spec=spec, worker=worker, store=self.store, clock=_Clock()
            )
        self.assertIsNone(self.store.get_terminal(spec.workflow_id))
        self.assertEqual(
            self.store.list_events(spec.workflow_id)[-1].status,
            DailyPaperWorkflowEventStatus.FAILED,
        )

    def test_retry_repairs_terminal_written_before_completion_event(self) -> None:
        spec = _spec()
        self.store.put_spec(spec)
        self.store.append_event(
            workflow_id=spec.workflow_id,
            status=DailyPaperWorkflowEventStatus.STARTED,
            occurred_at=NOW,
        )
        terminal = self.store.put_terminal(
            DailyPaperWorkflowTerminal(
                workflow_id=spec.workflow_id,
                output=_complete_output(),
                started_at=NOW,
                completed_at=NOW + timedelta(seconds=1),
            )
        )
        worker = _Worker(RuntimeError("must not run"))

        restored = run_daily_paper_workflow(
            spec=spec, worker=worker, store=self.store, clock=_Clock()
        )

        self.assertEqual(restored, terminal)
        self.assertEqual(worker.calls, [])
        events = self.store.list_events(spec.workflow_id)
        self.assertEqual(events[-1].status, DailyPaperWorkflowEventStatus.COMPLETED)
        self.assertEqual(events[-1].terminal_id, terminal.terminal_id)

    def test_invalid_attempt_transition_is_rejected_before_a_file_is_created(self) -> None:
        spec = _spec()
        self.store.put_spec(spec)
        self.store.append_event(
            workflow_id=spec.workflow_id,
            status=DailyPaperWorkflowEventStatus.STARTED,
            occurred_at=NOW,
        )
        with self.assertRaisesRegex(DailyPaperWorkflowError, "transition"):
            self.store.append_event(
                workflow_id=spec.workflow_id,
                status=DailyPaperWorkflowEventStatus.STARTED,
                occurred_at=NOW + timedelta(seconds=1),
            )
        self.assertEqual(len(self.store.list_events(spec.workflow_id)), 1)

    def test_store_rejects_unexpected_event_files_and_identity_mutation(self) -> None:
        spec = _spec()
        self.store.put_spec(spec)
        object.__setattr__(spec, "run_id", "0" * 64)
        with self.assertRaisesRegex(DailyPaperWorkflowError, "identity"):
            spec.verify_content_identity()

        directory = self.store.events_root(_spec().workflow_id)
        directory.mkdir(parents=True)
        (directory / "latest.json").write_text("{}", encoding="utf-8")
        with self.assertRaisesRegex(DailyPaperWorkflowError, "file set"):
            self.store.list_events(_spec().workflow_id)

    def test_output_rejects_missing_lineage_and_unsorted_manifest_pins(self) -> None:
        with self.assertRaises(DailyPaperWorkflowError):
            DailyPaperWorkflowOutput(
                status=DailyPaperWorkflowOutputStatus.COMPLETE,
                preparation_id=None,
                batch_id="d" * 64,
                state_id="e" * 64,
                outcome_manifest_pins=(),
                portfolio_manifest_pin=None,
                telegram_receipt_id="f" * 64,
            )
        pins = (_pin("z/a.json", "1"), _pin("a/b.json", "2"))
        if tuple(value.pin_id for value in pins) == tuple(
            sorted(value.pin_id for value in pins)
        ):
            pins = tuple(reversed(pins))
        with self.assertRaisesRegex(DailyPaperWorkflowError, "ordered"):
            DailyPaperWorkflowOutput(
                status=DailyPaperWorkflowOutputStatus.COMPLETE,
                preparation_id="c" * 64,
                batch_id="d" * 64,
                state_id="e" * 64,
                outcome_manifest_pins=pins,
                portfolio_manifest_pin=_pin("p/m.json", "3"),
                telegram_receipt_id="f" * 64,
            )

    def test_concrete_worker_sends_one_idempotent_no_position_heartbeat(self) -> None:
        evidence_root = self.root / "evidence"
        state_root = self.root / "state"
        evidence_root.mkdir()
        state_root.mkdir()
        writer = _UnusedWriter()
        transport = _TelegramTransport()
        clock = _Clock()
        worker = LocalDailyPaperWorkflowWorker(
            evidence_root=evidence_root,
            state_root=state_root,
            writer=writer,
            telegram_config=TelegramBotConfig(
                bot_token="12345:" + "a" * 20,
                chat_id="123456",
            ),
            telegram_transport=transport,
            clock=clock,
        )
        spec = _spec()
        store = LocalDailyPaperWorkflowStore(state_root / "daily_workflow")

        first = run_daily_paper_workflow(
            spec=spec, worker=worker, store=store, clock=clock
        )
        second = run_daily_paper_workflow(
            spec=spec, worker=worker, store=store, clock=clock
        )

        self.assertEqual(second, first)
        self.assertIs(
            first.output.status,
            DailyPaperWorkflowOutputStatus.NO_ACTIVE_POSITIONS,
        )
        self.assertEqual(transport.calls, 1)
        self.assertEqual(writer.calls, 0)


if __name__ == "__main__":
    unittest.main()
