from __future__ import annotations

import inspect
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

from india_swing.evaluation import nse_archive_research_identity as identity_module
from india_swing.evaluation.nse_archive_research_identity import (
    NseArchiveResearchIdentityAdmissionSession,
    NseArchiveResearchIdentityAdmissionStatus,
    NseArchiveResearchIdentityBasis,
    NseArchiveResearchIdentityDecision,
    NseArchiveResearchPairedSession,
    research_identity_id_for_isin,
)
from india_swing.evaluation.nse_archive_research_price_stream import (
    NseArchiveResearchPriceObservation,
    NseArchiveResearchPriceStreamSession,
    iter_nse_archive_research_price_stream_sessions,
)
from india_swing.forward_paper import history as history_module
from india_swing.forward_paper.history import (
    ForwardPaperHistoryCandidate,
    ForwardPaperHistoryError,
    ForwardPaperHistoryVeto,
    ForwardPaperHistoryVetoReason,
    ForwardPaperHistoryWindowSpec,
    ForwardPaperRawHistoryWindow,
    build_forward_paper_raw_history_window,
)

from tests.test_nse_archive_research_dataset import OBSERVED_AT, _baseline_dataset, _fake_sha256
from tests.test_nse_archive_research_identity import (
    _FixedSessionsIterator,
    _record,
    _session,
    _unresolved_record,
)

DATASET_ID = _fake_sha256("direct-dataset")
WINDOW_SIZE = history_module.FORWARD_PAPER_HISTORY_WINDOW_SESSION_COUNT
START_DATE = date(2024, 1, 1)
ISIN_A = "INE009A01021"
ISIN_B = "INE467B01029"
ISIN_C = "INE001A01036"


def _dates(count: int = WINDOW_SIZE, start: date = START_DATE) -> tuple:
    return tuple(start + timedelta(days=i) for i in range(count))


def _decision_cutoff(offset_hours: int = 1) -> datetime:
    return OBSERVED_AT + timedelta(hours=offset_hours)


def _spec(expected_market_sessions, *, dataset_id=DATASET_ID, decision_cutoff=None):
    return ForwardPaperHistoryWindowSpec(
        dataset_id=dataset_id,
        signal_session=expected_market_sessions[-1],
        decision_cutoff=decision_cutoff if decision_cutoff is not None else _decision_cutoff(),
        expected_market_sessions=tuple(expected_market_sessions),
    )


def _two_identity_replay_sessions(dates, *, symbol_a: str = "AAA", symbol_b: str = "BBB"):
    sessions = []
    for market_session in dates:
        record_a = _record(market_session, symbol=symbol_a, validated_isin=ISIN_A)
        record_b = _record(market_session, symbol=symbol_b, validated_isin=ISIN_B)
        sessions.append(_session(market_session, (record_a, record_b)))
    return tuple(sessions)


def _stream_sessions_for(dataset, replay_sessions):
    seam = _FixedSessionsIterator(tuple(replay_sessions))
    with patch.object(
        identity_module, "iter_verified_nse_archive_research_sessions", seam
    ):
        return list(
            iter_nse_archive_research_price_stream_sessions(dataset, object())
        ), seam


def _window_for(dataset, replay_sessions, dates, **spec_overrides):
    stream_sessions, _seam = _stream_sessions_for(dataset, replay_sessions)
    spec = _spec(dates, **spec_overrides)
    window = build_forward_paper_raw_history_window(spec, iter(stream_sessions))
    return window, stream_sessions


def _direct_duplicate_identity_stream_session(
    market_session: date, isin: str, *, other_isin: str | None = None
) -> NseArchiveResearchPriceStreamSession:
    """One legitimately self-consistent session with two observations sharing one identity.

    The normal admission pipeline's same-session collision detector always
    nulls out two decisions that claim the same ISIN, so this shape can
    never arise through the public seam. It is only reachable through a
    tampered/malformed nested session -- built here via direct construction
    (never through ``_build_admission_session_decisions_and_transitions``)
    solely to prove the history-window builder's own defense catches it.

    ``other_isin``, when given, adds one ordinary unrelated third record so
    an unrelated identity's own required-session coverage stays intact.
    """

    record_x = _record(market_session, symbol="XXX", validated_isin=isin)
    record_y = _record(market_session, symbol="YYY", validated_isin=isin)
    records = (record_x, record_y)
    other_record = None
    if other_isin is not None:
        other_record = _record(market_session, symbol="BBB", validated_isin=other_isin)
        records = records + (other_record,)
    replay_session = _session(market_session, records)
    research_identity_id = research_identity_id_for_isin(isin)

    def _decision(record):
        return NseArchiveResearchIdentityDecision(
            dataset_id=replay_session.dataset_id,
            replay_session_id=replay_session.replay_session_id,
            session_snapshot_id=replay_session.session_snapshot_id,
            market_session=replay_session.market_session,
            partition_id=replay_session.partition_id,
            partition_role=replay_session.partition_role,
            record_id=record.record_id,
            listing_key=record.listing_key,
            symbol=record.symbol,
            series=record.series,
            source_claim_id=None,
            source_isin=isin,
            basis=NseArchiveResearchIdentityBasis.VALIDATED_SAME_SESSION_ISIN,
            admission_status=NseArchiveResearchIdentityAdmissionStatus.ADMITTED_VALIDATED,
            research_identity_id=research_identity_id,
        )

    decision_x = _decision(record_x)
    decision_y = _decision(record_y)
    decisions = (decision_x, decision_y)
    observations = (
        NseArchiveResearchPriceObservation(replay_record=record_x, identity_decision=decision_x),
        NseArchiveResearchPriceObservation(replay_record=record_y, identity_decision=decision_y),
    )
    if other_record is not None:
        other_decision = NseArchiveResearchIdentityDecision(
            dataset_id=replay_session.dataset_id,
            replay_session_id=replay_session.replay_session_id,
            session_snapshot_id=replay_session.session_snapshot_id,
            market_session=replay_session.market_session,
            partition_id=replay_session.partition_id,
            partition_role=replay_session.partition_role,
            record_id=other_record.record_id,
            listing_key=other_record.listing_key,
            symbol=other_record.symbol,
            series=other_record.series,
            source_claim_id=None,
            source_isin=other_isin,
            basis=NseArchiveResearchIdentityBasis.VALIDATED_SAME_SESSION_ISIN,
            admission_status=NseArchiveResearchIdentityAdmissionStatus.ADMITTED_VALIDATED,
            research_identity_id=research_identity_id_for_isin(other_isin),
        )
        decisions = decisions + (other_decision,)
        observations = observations + (
            NseArchiveResearchPriceObservation(
                replay_record=other_record, identity_decision=other_decision
            ),
        )

    admission_session = NseArchiveResearchIdentityAdmissionSession(
        dataset_id=replay_session.dataset_id,
        replay_session_id=replay_session.replay_session_id,
        session_snapshot_id=replay_session.session_snapshot_id,
        market_session=replay_session.market_session,
        partition_id=replay_session.partition_id,
        partition_role=replay_session.partition_role,
        decisions=decisions,
        transitions=(),
        admitted_validated_count=len(decisions),
        admitted_source_attested_count=0,
        blocked_unresolved_count=0,
        blocked_collision_count=0,
    )
    paired = NseArchiveResearchPairedSession(
        replay_session=replay_session, admission_session=admission_session
    )
    return NseArchiveResearchPriceStreamSession(
        paired_session=paired,
        observations=observations,
        transitions=(),
    )


class _CountingIterator:
    def __init__(self, items) -> None:
        self._iterator = iter(items)
        self.calls = 0

    def __iter__(self):
        return self

    def __next__(self):
        self.calls += 1
        return next(self._iterator)


class ForwardPaperHistoryHappyPathTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()

    def test_two_identities_produce_ordered_complete_candidates_with_exact_counts(self) -> None:
        dates = _dates()
        replay_sessions = _two_identity_replay_sessions(dates)
        stream_sessions, seam = _stream_sessions_for(self.dataset, replay_sessions)
        spec = _spec(dates)
        window = build_forward_paper_raw_history_window(spec, iter(stream_sessions))

        self.assertEqual(window.expected_session_count, 60)
        self.assertEqual(window.consumed_session_count, 60)
        self.assertEqual(window.signal_subject_count, 2)
        self.assertEqual(window.complete_candidate_count, 2)
        self.assertEqual(window.veto_count, 0)
        self.assertIs(window.signal_session, stream_sessions[-1])

        by_symbol = {}
        for outcome in window.outcomes:
            self.assertIsInstance(outcome, ForwardPaperHistoryCandidate)
            by_symbol[outcome.signal_observation.symbol] = outcome
        self.assertEqual(set(by_symbol), {"AAA", "BBB"})

        candidate_a = by_symbol["AAA"]
        self.assertEqual(len(candidate_a.history_observations), 60)
        for expected_date, observation, stream_session in zip(
            dates, candidate_a.history_observations, stream_sessions, strict=True
        ):
            self.assertEqual(observation.market_session, expected_date)
            self.assertIs(
                observation,
                next(o for o in stream_session.observations if o.symbol == "AAA"),
            )
        self.assertIs(candidate_a.signal_observation, candidate_a.history_observations[-1])

        again = ForwardPaperHistoryCandidate(
            spec_id=candidate_a.spec_id,
            research_identity_id=candidate_a.research_identity_id,
            history_observations=candidate_a.history_observations,
        )
        self.assertEqual(candidate_a.candidate_id, again.candidate_id)
        window.verify_content_identity()
        self.assertEqual(seam.calls, 60)

    def test_exactly_one_upstream_traversal(self) -> None:
        dates = _dates()
        replay_sessions = _two_identity_replay_sessions(dates)
        stream_sessions, seam = _stream_sessions_for(self.dataset, replay_sessions)
        self.assertEqual(seam.invocation_count, 1)
        spec = _spec(dates)
        build_forward_paper_raw_history_window(spec, iter(stream_sessions))
        # The builder consumes a plain caller-supplied iterator -- it must
        # never call back into the upstream seam a second time.
        self.assertEqual(seam.invocation_count, 1)


class ForwardPaperHistoryStreamingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()

    def test_sessions_before_window_are_skipped_and_never_pulls_past_signal_session(self) -> None:
        window_dates = _dates()
        before_dates = tuple(window_dates[0] - timedelta(days=i) for i in (3, 2, 1))
        after_date = window_dates[-1] + timedelta(days=1)
        all_dates = before_dates + window_dates + (after_date,)
        replay_sessions = _two_identity_replay_sessions(all_dates)
        stream_sessions, _seam = _stream_sessions_for(self.dataset, replay_sessions)

        spec = _spec(window_dates)
        counting = _CountingIterator(stream_sessions)
        window = build_forward_paper_raw_history_window(spec, counting)

        self.assertEqual(counting.calls, len(before_dates) + len(window_dates))
        self.assertEqual(window.consumed_session_count, 60)
        self.assertEqual(
            window.signal_session.paired_session.replay_session.market_session,
            window_dates[-1],
        )


class ForwardPaperHistoryVetoTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()

    def test_signal_session_unresolved_and_collision_rows_become_signal_identity_unresolved(
        self,
    ) -> None:
        dates = _dates()
        replay_sessions = list(_two_identity_replay_sessions(dates))
        signal_date = dates[-1]
        record_a = _record(signal_date, symbol="AAA", validated_isin=ISIN_A)
        record_b = _record(signal_date, symbol="BBB", validated_isin=ISIN_B)
        unresolved = _unresolved_record(signal_date, symbol="CCC")
        collision_x = _record(signal_date, symbol="DDD", validated_isin="INE000A01001")
        collision_y = _record(signal_date, symbol="EEE", validated_isin="INE000A01001")
        replay_sessions[-1] = _session(
            signal_date, (record_a, record_b, unresolved, collision_x, collision_y)
        )

        window, _stream_sessions = _window_for(self.dataset, replay_sessions, dates)
        self.assertEqual(window.signal_subject_count, 5)
        self.assertEqual(window.complete_candidate_count, 2)
        self.assertEqual(window.veto_count, 3)
        vetoes_by_symbol = {
            outcome.signal_observation.symbol: outcome
            for outcome in window.outcomes
            if isinstance(outcome, ForwardPaperHistoryVeto)
        }
        self.assertEqual(set(vetoes_by_symbol), {"CCC", "DDD", "EEE"})
        for veto in vetoes_by_symbol.values():
            self.assertIs(veto.reason, ForwardPaperHistoryVetoReason.SIGNAL_IDENTITY_UNRESOLVED)
            self.assertIsNone(veto.research_identity_id)

    def test_identity_missing_from_one_required_session_never_shrinks_other_subjects(
        self,
    ) -> None:
        dates = _dates()
        replay_sessions = list(_two_identity_replay_sessions(dates))
        missing_index = 30
        missing_date = dates[missing_index]
        replay_sessions[missing_index] = _session(
            missing_date, (_record(missing_date, symbol="AAA", validated_isin=ISIN_A),)
        )

        window, _stream_sessions = _window_for(self.dataset, replay_sessions, dates)
        self.assertEqual(window.signal_subject_count, 2)
        self.assertEqual(window.complete_candidate_count, 1)
        self.assertEqual(window.veto_count, 1)
        outcomes_by_symbol = {o.signal_observation.symbol: o for o in window.outcomes}
        self.assertIsInstance(outcomes_by_symbol["AAA"], ForwardPaperHistoryCandidate)
        veto_b = outcomes_by_symbol["BBB"]
        self.assertIsInstance(veto_b, ForwardPaperHistoryVeto)
        self.assertIs(veto_b.reason, ForwardPaperHistoryVetoReason.REQUIRED_SESSION_MISSING)

    def test_duplicate_same_identity_observations_become_required_session_duplicated(
        self,
    ) -> None:
        dates = _dates()
        replay_sessions = _two_identity_replay_sessions(dates)
        stream_sessions, _seam = _stream_sessions_for(self.dataset, replay_sessions)
        dup_index = 15
        stream_sessions[dup_index] = _direct_duplicate_identity_stream_session(
            dates[dup_index], ISIN_A, other_isin=ISIN_B
        )

        spec = _spec(dates)
        window = build_forward_paper_raw_history_window(spec, iter(stream_sessions))
        self.assertEqual(window.complete_candidate_count, 1)
        self.assertEqual(window.veto_count, 1)
        outcomes_by_symbol = {o.signal_observation.symbol: o for o in window.outcomes}
        self.assertIsInstance(outcomes_by_symbol["BBB"], ForwardPaperHistoryCandidate)
        veto_a = outcomes_by_symbol["AAA"]
        self.assertIsInstance(veto_a, ForwardPaperHistoryVeto)
        self.assertIs(veto_a.reason, ForwardPaperHistoryVetoReason.REQUIRED_SESSION_DUPLICATED)

    def test_missing_and_duplicated_together_deterministically_prefers_duplicated(
        self,
    ) -> None:
        dates = _dates()
        replay_sessions = list(_two_identity_replay_sessions(dates))
        missing_index = 10
        missing_date = dates[missing_index]
        replay_sessions[missing_index] = _session(
            missing_date, (_record(missing_date, symbol="BBB", validated_isin=ISIN_B),)
        )
        stream_sessions, _seam = _stream_sessions_for(self.dataset, replay_sessions)
        dup_index = 20
        stream_sessions[dup_index] = _direct_duplicate_identity_stream_session(
            dates[dup_index], ISIN_A, other_isin=ISIN_B
        )

        spec = _spec(dates)
        window = build_forward_paper_raw_history_window(spec, iter(stream_sessions))
        outcomes_by_symbol = {o.signal_observation.symbol: o for o in window.outcomes}
        veto_a = outcomes_by_symbol["AAA"]
        self.assertIsInstance(veto_a, ForwardPaperHistoryVeto)
        self.assertIs(veto_a.reason, ForwardPaperHistoryVetoReason.REQUIRED_SESSION_DUPLICATED)


class ForwardPaperHistoryTransitionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()

    def test_symbol_change_for_same_identity_retained_in_order(self) -> None:
        dates = _dates()
        change_index = 40
        replay_sessions = []
        for index, market_session in enumerate(dates):
            symbol_a = "AAA" if index < change_index else "ZZZ"
            record_a = _record(market_session, symbol=symbol_a, validated_isin=ISIN_A)
            record_b = _record(market_session, symbol="BBB", validated_isin=ISIN_B)
            replay_sessions.append(_session(market_session, (record_a, record_b)))

        window, _stream_sessions = _window_for(self.dataset, replay_sessions, dates)
        candidate_a = next(
            outcome
            for outcome in window.outcomes
            if isinstance(outcome, ForwardPaperHistoryCandidate)
            and outcome.research_identity_id == research_identity_id_for_isin(ISIN_A)
        )
        self.assertEqual(len(candidate_a.history_observations), 60)
        for index, observation in enumerate(candidate_a.history_observations):
            expected_symbol = "AAA" if index < change_index else "ZZZ"
            self.assertEqual(observation.symbol, expected_symbol)
        self.assertEqual(candidate_a.signal_observation.symbol, "ZZZ")

    def test_listing_key_rebound_to_different_identity_is_never_spliced_into_history(
        self,
    ) -> None:
        dates = _dates()
        rebound_index = 30
        replay_sessions = []
        for index, market_session in enumerate(dates):
            isin_for_aaa = ISIN_A if index < rebound_index else ISIN_C
            record_a = _record(market_session, symbol="AAA", validated_isin=isin_for_aaa)
            record_b = _record(market_session, symbol="BBB", validated_isin=ISIN_B)
            replay_sessions.append(_session(market_session, (record_a, record_b)))

        window, _stream_sessions = _window_for(self.dataset, replay_sessions, dates)
        outcome_aaa = next(
            outcome for outcome in window.outcomes if outcome.signal_observation.symbol == "AAA"
        )
        self.assertIsInstance(outcome_aaa, ForwardPaperHistoryVeto)
        self.assertIs(outcome_aaa.reason, ForwardPaperHistoryVetoReason.REQUIRED_SESSION_MISSING)
        self.assertEqual(outcome_aaa.research_identity_id, research_identity_id_for_isin(ISIN_C))


class ForwardPaperHistorySpecTests(unittest.TestCase):
    def test_spec_rejects_wrong_session_count(self) -> None:
        dates = _dates(59)
        with self.assertRaises(ForwardPaperHistoryError):
            ForwardPaperHistoryWindowSpec(
                dataset_id=DATASET_ID,
                signal_session=dates[-1],
                decision_cutoff=_decision_cutoff(),
                expected_market_sessions=dates,
            )

    def test_spec_rejects_duplicate_sessions(self) -> None:
        dates = list(_dates(60))
        dates[1] = dates[0]
        with self.assertRaises(ForwardPaperHistoryError):
            ForwardPaperHistoryWindowSpec(
                dataset_id=DATASET_ID,
                signal_session=dates[-1],
                decision_cutoff=_decision_cutoff(),
                expected_market_sessions=tuple(dates),
            )

    def test_spec_rejects_non_increasing_sessions(self) -> None:
        dates = list(_dates(60))
        dates[0], dates[1] = dates[1], dates[0]
        with self.assertRaises(ForwardPaperHistoryError):
            ForwardPaperHistoryWindowSpec(
                dataset_id=DATASET_ID,
                signal_session=dates[-1],
                decision_cutoff=_decision_cutoff(),
                expected_market_sessions=tuple(dates),
            )

    def test_spec_rejects_sessions_not_ending_on_signal_session(self) -> None:
        dates = _dates(60)
        with self.assertRaises(ForwardPaperHistoryError):
            ForwardPaperHistoryWindowSpec(
                dataset_id=DATASET_ID,
                signal_session=dates[-1] + timedelta(days=1),
                decision_cutoff=_decision_cutoff(),
                expected_market_sessions=dates,
            )

    def test_spec_rejects_naive_decision_cutoff(self) -> None:
        dates = _dates(60)
        with self.assertRaises(ForwardPaperHistoryError):
            ForwardPaperHistoryWindowSpec(
                dataset_id=DATASET_ID,
                signal_session=dates[-1],
                decision_cutoff=datetime(2026, 1, 1),
                expected_market_sessions=dates,
            )

    def test_spec_rejects_non_utc_decision_cutoff(self) -> None:
        dates = _dates(60)
        ist = timezone(timedelta(hours=5, minutes=30))
        with self.assertRaises(ForwardPaperHistoryError):
            ForwardPaperHistoryWindowSpec(
                dataset_id=DATASET_ID,
                signal_session=dates[-1],
                decision_cutoff=datetime(2026, 1, 1, tzinfo=ist),
                expected_market_sessions=dates,
            )

    def test_spec_rejects_invalid_dataset_id(self) -> None:
        dates = _dates(60)
        with self.assertRaises(ForwardPaperHistoryError):
            ForwardPaperHistoryWindowSpec(
                dataset_id="not-a-sha256",
                signal_session=dates[-1],
                decision_cutoff=_decision_cutoff(),
                expected_market_sessions=dates,
            )

    def test_spec_content_identity_is_deterministic(self) -> None:
        dates = _dates(60)
        spec = _spec(dates)
        again = ForwardPaperHistoryWindowSpec(
            dataset_id=spec.dataset_id,
            signal_session=spec.signal_session,
            decision_cutoff=spec.decision_cutoff,
            expected_market_sessions=spec.expected_market_sessions,
        )
        self.assertEqual(spec.spec_id, again.spec_id)


class ForwardPaperHistoryBuilderRejectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()
        self.dates = _dates()
        replay_sessions = _two_identity_replay_sessions(self.dates)
        self.stream_sessions, _seam = _stream_sessions_for(self.dataset, replay_sessions)
        self.spec = _spec(self.dates)

    def test_rejects_invalid_spec_type(self) -> None:
        with self.assertRaises(ForwardPaperHistoryError):
            build_forward_paper_raw_history_window(object(), iter(self.stream_sessions))

    def test_rejects_wrong_dataset_session(self) -> None:
        tampered = list(self.stream_sessions)
        object.__setattr__(
            tampered[0].paired_session.replay_session,
            "dataset_id",
            _fake_sha256("different-dataset"),
        )
        with self.assertRaises(ForwardPaperHistoryError):
            build_forward_paper_raw_history_window(self.spec, iter(tampered))

    def test_rejects_reordered_session(self) -> None:
        tampered = list(self.stream_sessions)
        tampered[0], tampered[1] = tampered[1], tampered[0]
        with self.assertRaises(ForwardPaperHistoryError):
            build_forward_paper_raw_history_window(self.spec, iter(tampered))

    def test_rejects_missing_session(self) -> None:
        tampered = list(self.stream_sessions)
        del tampered[30]
        with self.assertRaises(ForwardPaperHistoryError):
            build_forward_paper_raw_history_window(self.spec, iter(tampered))

    def test_rejects_duplicated_session(self) -> None:
        tampered = list(self.stream_sessions)
        tampered[30] = tampered[29]
        with self.assertRaises(ForwardPaperHistoryError):
            build_forward_paper_raw_history_window(self.spec, iter(tampered))

    def test_rejects_stream_ending_before_window_is_complete(self) -> None:
        with self.assertRaises(ForwardPaperHistoryError):
            build_forward_paper_raw_history_window(self.spec, iter(self.stream_sessions[:59]))

    def test_rejects_future_observed_at(self) -> None:
        spec = _spec(self.dates, decision_cutoff=OBSERVED_AT - timedelta(hours=1))
        with self.assertRaises(ForwardPaperHistoryError):
            build_forward_paper_raw_history_window(spec, iter(self.stream_sessions))

    def test_rejects_session_with_forged_content_id(self) -> None:
        tampered = list(self.stream_sessions)
        object.__setattr__(tampered[10], "price_stream_session_id", "0" * 64)
        with self.assertRaises(ForwardPaperHistoryError):
            build_forward_paper_raw_history_window(self.spec, iter(tampered))

    def test_rejects_foreign_session_type(self) -> None:
        tampered = list(self.stream_sessions)
        tampered[5] = object()
        with self.assertRaises(ForwardPaperHistoryError):
            build_forward_paper_raw_history_window(self.spec, iter(tampered))


class ForwardPaperHistoryTamperingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()
        self.dates = _dates()
        replay_sessions = list(_two_identity_replay_sessions(self.dates))
        signal_date = self.dates[-1]
        record_a = _record(signal_date, symbol="AAA", validated_isin=ISIN_A)
        record_b = _record(signal_date, symbol="BBB", validated_isin=ISIN_B)
        unresolved = _unresolved_record(signal_date, symbol="CCC")
        replay_sessions[-1] = _session(signal_date, (record_a, record_b, unresolved))
        self.window, self.stream_sessions = _window_for(self.dataset, replay_sessions, self.dates)
        self.candidate = next(
            o for o in self.window.outcomes if isinstance(o, ForwardPaperHistoryCandidate)
        )
        self.veto = next(o for o in self.window.outcomes if isinstance(o, ForwardPaperHistoryVeto))

    def test_tampered_candidate_id_rejected(self) -> None:
        object.__setattr__(self.candidate, "candidate_id", "0" * 64)
        with self.assertRaises(ForwardPaperHistoryError):
            self.candidate.verify_content_identity()

    def test_tampered_veto_id_rejected(self) -> None:
        object.__setattr__(self.veto, "veto_id", "0" * 64)
        with self.assertRaises(ForwardPaperHistoryError):
            self.veto.verify_content_identity()

    def test_tampered_spec_id_rejected(self) -> None:
        object.__setattr__(self.window.spec, "spec_id", "0" * 64)
        with self.assertRaises(ForwardPaperHistoryError):
            self.window.spec.verify_content_identity()

    def test_tampered_window_id_rejected(self) -> None:
        object.__setattr__(self.window, "window_id", "0" * 64)
        with self.assertRaises(ForwardPaperHistoryError):
            self.window.verify_content_identity()

    def test_tampered_candidate_within_window_rejected_on_reverification(self) -> None:
        object.__setattr__(self.candidate, "research_identity_id", "1" * 64)
        with self.assertRaises(ForwardPaperHistoryError):
            self.window.verify_content_identity()

    def test_foreign_outcome_type_in_window_rejected(self) -> None:
        object.__setattr__(
            self.window,
            "outcomes",
            tuple(
                object() if isinstance(o, ForwardPaperHistoryCandidate) else o
                for o in self.window.outcomes
            ),
        )
        with self.assertRaises(ForwardPaperHistoryError):
            self.window.verify_content_identity()

    def test_wrong_counts_rejected(self) -> None:
        object.__setattr__(self.window, "veto_count", self.window.veto_count + 1)
        with self.assertRaises(ForwardPaperHistoryError):
            self.window.verify_content_identity()

    def test_altered_authority_flag_rejected(self) -> None:
        object.__setattr__(self.window, "training_eligible", True)
        with self.assertRaises(ForwardPaperHistoryError):
            self.window.verify_content_identity()

    def test_candidate_rejects_observation_with_mismatched_identity(self) -> None:
        wrong_identity_observation = next(
            observation
            for stream_session in self.stream_sessions
            for observation in stream_session.observations
            if observation.research_identity_id is not None
            and observation.research_identity_id != self.candidate.research_identity_id
        )
        tampered_history = (
            self.candidate.history_observations[:-1] + (wrong_identity_observation,)
        )
        with self.assertRaises(ForwardPaperHistoryError):
            ForwardPaperHistoryCandidate(
                spec_id=self.candidate.spec_id,
                research_identity_id=self.candidate.research_identity_id,
                history_observations=tampered_history,
            )


class ForwardPaperHistoryErrorSanitizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()

    def test_invalid_spec_type_error_has_no_cause_or_context(self) -> None:
        with self.assertRaises(ForwardPaperHistoryError) as context:
            build_forward_paper_raw_history_window(object(), iter(()))
        exc = context.exception
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)

    def test_stream_exhaustion_error_has_no_cause_or_context(self) -> None:
        dates = _dates()
        replay_sessions = _two_identity_replay_sessions(dates)
        stream_sessions, _seam = _stream_sessions_for(self.dataset, replay_sessions)
        spec = _spec(dates)
        with self.assertRaises(ForwardPaperHistoryError) as context:
            build_forward_paper_raw_history_window(spec, iter(stream_sessions[:10]))
        exc = context.exception
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)

    def test_foreign_exception_with_planted_secret_does_not_leak(self) -> None:
        secret = "SECRET-PLANTED-VALUE-MUST-NOT-LEAK/var/data/topsecret.json"

        def _boom():
            raise ValueError(secret)
            yield  # pragma: no cover - makes this a generator function

        dates = _dates()
        spec = _spec(dates)
        with self.assertRaises(ForwardPaperHistoryError) as context:
            build_forward_paper_raw_history_window(spec, _boom())
        exc = context.exception
        self.assertNotIn(secret, str(exc))
        self.assertNotIn(secret, repr(exc))
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)


class ForwardPaperHistoryPostureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()
        self.dates = _dates()
        replay_sessions = _two_identity_replay_sessions(self.dates)
        self.window, self.stream_sessions = _window_for(self.dataset, replay_sessions, self.dates)

    def _assert_fixed_posture(self, obj) -> None:
        self.assertTrue(obj.collection_only)
        self.assertFalse(obj.training_eligible)
        self.assertFalse(obj.feature_eligible)
        self.assertFalse(obj.label_eligible)
        self.assertFalse(obj.ranking_eligible)
        self.assertFalse(obj.alert_eligible)
        self.assertFalse(obj.paper_trade_eligible)
        self.assertFalse(obj.notification_eligible)
        self.assertFalse(obj.execution_eligible)
        self.assertFalse(obj.production_identity_resolution_complete)
        self.assertFalse(obj.corporate_action_adjustment_complete)

    def test_spec_posture(self) -> None:
        self._assert_fixed_posture(self.window.spec)

    def test_candidate_posture(self) -> None:
        candidate = next(
            o for o in self.window.outcomes if isinstance(o, ForwardPaperHistoryCandidate)
        )
        self._assert_fixed_posture(candidate)

    def test_veto_posture(self) -> None:
        replay_sessions = list(_two_identity_replay_sessions(self.dates))
        signal_date = self.dates[-1]
        record_a = _record(signal_date, symbol="AAA", validated_isin=ISIN_A)
        record_b = _record(signal_date, symbol="BBB", validated_isin=ISIN_B)
        unresolved = _unresolved_record(signal_date, symbol="CCC")
        replay_sessions[-1] = _session(signal_date, (record_a, record_b, unresolved))
        window, _stream_sessions = _window_for(self.dataset, replay_sessions, self.dates)
        veto = next(o for o in window.outcomes if isinstance(o, ForwardPaperHistoryVeto))
        self._assert_fixed_posture(veto)

    def test_window_posture(self) -> None:
        self._assert_fixed_posture(self.window)

    def test_spec_posture_property_has_no_backing_slot(self) -> None:
        with self.assertRaises(AttributeError):
            object.__setattr__(self.window.spec, "training_eligible", True)

    def test_candidate_posture_property_has_no_backing_slot(self) -> None:
        candidate = next(
            o for o in self.window.outcomes if isinstance(o, ForwardPaperHistoryCandidate)
        )
        with self.assertRaises(AttributeError):
            object.__setattr__(candidate, "execution_eligible", True)


class ForwardPaperHistoryStructuralTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source = inspect.getsource(history_module)

    def test_no_filesystem_network_environment_or_clock_access(self) -> None:
        forbidden = (
            "open(",
            "Path(",
            "os.environ",
            "os.getenv",
            "socket.",
            "requests.",
            "urllib.",
            "httpx.",
            "datetime.now(",
            "datetime.utcnow(",
            "time.time(",
            "time.sleep(",
        )
        for token in forbidden:
            self.assertNotIn(token, self.source, msg=f"forbidden token found: {token}")

    def test_no_broker_cloud_corporate_action_feature_or_execution_capability(self) -> None:
        forbidden = (
            "kite",
            "gcs.",
            "cloud_run",
            "cloudrun",
            "telegram",
            "broker",
            "apply_corporate_action",
            "adjust_corporate_action",
            "adjusted_price",
            "tick_size",
            "compute_feature",
            "calculate_return",
            "generate_label",
            "generate_signal",
            "rank(",
            "confidence_score",
            "send_alert",
            "place_order",
            "execute_order",
            "register_paper_trade",
            "shadowalert",
            "send_notification",
            "kronos",
            "openai",
            "anthropic",
            "pickle.",
            "shelve.",
            "sqlite3.",
            "json.dump",
            ".glob(",
            ".iterdir(",
            ".listdir(",
            "marketsnapshotstore(",
            ".put(",
        )
        lowered = self.source.lower()
        for token in forbidden:
            self.assertNotIn(token.lower(), lowered, msg=f"forbidden token found: {token}")


def _rebuild_window_with_outcomes(window, outcomes, *, sessions=None):
    return ForwardPaperRawHistoryWindow(
        spec=window.spec,
        sessions=window.sessions if sessions is None else sessions,
        outcomes=outcomes,
        expected_session_count=window.expected_session_count,
        consumed_session_count=(
            window.consumed_session_count if sessions is None else len(sessions)
        ),
        signal_subject_count=window.signal_subject_count,
        complete_candidate_count=window.complete_candidate_count,
        veto_count=window.veto_count,
    )


class ForwardPaperHistoryLineageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()
        self.dates = _dates()
        replay_sessions = _two_identity_replay_sessions(self.dates)
        self.window, self.stream_sessions = _window_for(self.dataset, replay_sessions, self.dates)

    def test_window_retains_exact_60_sessions_by_reference_in_expected_order(self) -> None:
        self.assertEqual(len(self.window.sessions), WINDOW_SIZE)
        for expected_date, session, stream_session in zip(
            self.dates, self.window.sessions, self.stream_sessions, strict=True
        ):
            self.assertIs(session, stream_session)
            self.assertEqual(
                session.paired_session.replay_session.market_session, expected_date
            )
        self.assertIs(self.window.signal_session, self.window.sessions[-1])

    def test_window_id_changes_when_an_unrelated_historical_session_changes(self) -> None:
        change_index = 20
        change_date = self.dates[change_index]
        replay_sessions_altered = list(_two_identity_replay_sessions(self.dates))
        extra_record = _record(change_date, symbol="ZZZ", validated_isin=ISIN_C)
        original_records = replay_sessions_altered[change_index].records
        replay_sessions_altered[change_index] = _session(
            change_date, original_records + (extra_record,)
        )
        altered_window, _stream_sessions = _window_for(
            self.dataset, replay_sessions_altered, self.dates
        )

        original_identities = {
            o.research_identity_id
            for o in self.window.outcomes
            if isinstance(o, ForwardPaperHistoryCandidate)
        }
        altered_identities = {
            o.research_identity_id
            for o in altered_window.outcomes
            if isinstance(o, ForwardPaperHistoryCandidate)
        }
        # Adding an unrelated third identity's record to one historical
        # session never introduces or removes a current-subject candidate:
        # the exact same identities remain candidates in both windows (the
        # current-cross-section partition is unchanged; A and B's own
        # per-session content naturally still shifts, since every decision
        # in that session shares its replay_session_id lineage).
        self.assertEqual(original_identities, altered_identities)
        self.assertEqual(self.window.complete_candidate_count, altered_window.complete_candidate_count)
        self.assertEqual(self.window.veto_count, altered_window.veto_count)
        # The changed historical session's own price_stream_session_id
        # differs, and window_id must reflect that even though the current
        # cross-section partition is otherwise equivalent.
        self.assertNotEqual(
            self.window.sessions[change_index].price_stream_session_id,
            altered_window.sessions[change_index].price_stream_session_id,
        )
        self.assertNotEqual(self.window.window_id, altered_window.window_id)

    def test_rejects_missing_session_in_retained_tuple(self) -> None:
        truncated = self.window.sessions[:-1]
        with self.assertRaises(ForwardPaperHistoryError):
            _rebuild_window_with_outcomes(self.window, self.window.outcomes, sessions=truncated)

    def test_rejects_reordered_session_in_retained_tuple(self) -> None:
        reordered = list(self.window.sessions)
        reordered[0], reordered[1] = reordered[1], reordered[0]
        with self.assertRaises(ForwardPaperHistoryError):
            _rebuild_window_with_outcomes(
                self.window, self.window.outcomes, sessions=tuple(reordered)
            )

    def test_rejects_duplicated_session_in_retained_tuple(self) -> None:
        duplicated = list(self.window.sessions)
        duplicated[1] = duplicated[0]
        with self.assertRaises(ForwardPaperHistoryError):
            _rebuild_window_with_outcomes(
                self.window, self.window.outcomes, sessions=tuple(duplicated)
            )

    def test_rejects_foreign_session_in_retained_tuple(self) -> None:
        tampered = list(self.window.sessions)
        tampered[5] = object()
        with self.assertRaises(ForwardPaperHistoryError):
            _rebuild_window_with_outcomes(
                self.window, self.window.outcomes, sessions=tuple(tampered)
            )

    def test_rejects_session_with_forged_content_id_in_retained_tuple(self) -> None:
        tampered = list(self.window.sessions)
        object.__setattr__(tampered[5], "price_stream_session_id", "0" * 64)
        with self.assertRaises(ForwardPaperHistoryError):
            _rebuild_window_with_outcomes(
                self.window, self.window.outcomes, sessions=tuple(tampered)
            )

    def test_rejects_wrong_dataset_session_in_retained_tuple(self) -> None:
        tampered = list(self.window.sessions)
        object.__setattr__(
            tampered[5].paired_session.replay_session,
            "dataset_id",
            _fake_sha256("different-dataset"),
        )
        with self.assertRaises(ForwardPaperHistoryError):
            _rebuild_window_with_outcomes(
                self.window, self.window.outcomes, sessions=tuple(tampered)
            )

    def test_rejects_future_observed_session_in_retained_tuple(self) -> None:
        tampered = list(self.window.sessions)
        object.__setattr__(
            tampered[5].paired_session.replay_session,
            "observed_at",
            self.window.spec.decision_cutoff + timedelta(hours=1),
        )
        with self.assertRaises(ForwardPaperHistoryError):
            _rebuild_window_with_outcomes(
                self.window, self.window.outcomes, sessions=tuple(tampered)
            )

    def test_rejects_session_whose_date_does_not_match_its_expected_slot(self) -> None:
        outside_date = self.dates[0] - timedelta(days=1)
        outside_sessions, _seam = _stream_sessions_for(
            self.dataset, _two_identity_replay_sessions((outside_date,))
        )
        tampered = list(self.window.sessions)
        tampered[5] = outside_sessions[0]
        with self.assertRaises(ForwardPaperHistoryError):
            _rebuild_window_with_outcomes(
                self.window, self.window.outcomes, sessions=tuple(tampered)
            )

    def test_rejects_candidate_observation_that_is_self_consistent_but_not_retained(
        self,
    ) -> None:
        candidate_a = next(
            o
            for o in self.window.outcomes
            if isinstance(o, ForwardPaperHistoryCandidate) and o.signal_observation.symbol == "AAA"
        )
        forged_index = 20
        forged_date = self.dates[forged_index]
        forged_record = _record(
            forged_date, symbol="AAA", validated_isin=ISIN_A, close=Decimal("9999.99")
        )
        forged_session = _session(forged_date, (forged_record,))
        forged_stream_sessions, _seam = _stream_sessions_for(self.dataset, (forged_session,))
        [forged_observation] = forged_stream_sessions[0].observations
        self.assertEqual(
            forged_observation.research_identity_id, research_identity_id_for_isin(ISIN_A)
        )
        self.assertNotEqual(
            forged_observation.observation_id,
            candidate_a.history_observations[forged_index].observation_id,
        )

        tampered_history = (
            candidate_a.history_observations[:forged_index]
            + (forged_observation,)
            + candidate_a.history_observations[forged_index + 1 :]
        )
        tampered_candidate = ForwardPaperHistoryCandidate(
            spec_id=candidate_a.spec_id,
            research_identity_id=candidate_a.research_identity_id,
            history_observations=tampered_history,
        )
        outcomes = tuple(
            tampered_candidate if o is candidate_a else o for o in self.window.outcomes
        )
        with self.assertRaises(ForwardPaperHistoryError):
            _rebuild_window_with_outcomes(self.window, outcomes)


class ForwardPaperHistoryVetoEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()
        self.dates = _dates()

    def test_missing_veto_binds_precise_affected_session_ids(self) -> None:
        replay_sessions = list(_two_identity_replay_sessions(self.dates))
        missing_indices = (10, 40)
        for missing_index in missing_indices:
            missing_date = self.dates[missing_index]
            replay_sessions[missing_index] = _session(
                missing_date, (_record(missing_date, symbol="AAA", validated_isin=ISIN_A),)
            )
        window, _stream_sessions = _window_for(self.dataset, replay_sessions, self.dates)
        veto_b = next(o for o in window.outcomes if isinstance(o, ForwardPaperHistoryVeto))
        self.assertIs(veto_b.reason, ForwardPaperHistoryVetoReason.REQUIRED_SESSION_MISSING)
        expected_ids = tuple(
            window.sessions[index].price_stream_session_id for index in missing_indices
        )
        self.assertEqual(veto_b.evidence_session_ids, expected_ids)
        self.assertEqual(veto_b.evidence_observation_ids, ())

    def test_missing_veto_evidence_rejected_at_window_cross_check_when_unrelated(self) -> None:
        replay_sessions = list(_two_identity_replay_sessions(self.dates))
        missing_index = 10
        missing_date = self.dates[missing_index]
        replay_sessions[missing_index] = _session(
            missing_date, (_record(missing_date, symbol="AAA", validated_isin=ISIN_A),)
        )
        window, _stream_sessions = _window_for(self.dataset, replay_sessions, self.dates)
        veto_b = next(o for o in window.outcomes if isinstance(o, ForwardPaperHistoryVeto))
        unrelated_session_id = window.sessions[0].price_stream_session_id
        tampered = ForwardPaperHistoryVeto(
            spec_id=veto_b.spec_id,
            research_identity_id=veto_b.research_identity_id,
            signal_observation=veto_b.signal_observation,
            reason=veto_b.reason,
            evidence_session_ids=(unrelated_session_id,),
            evidence_observation_ids=(),
        )
        outcomes = tuple(tampered if o is veto_b else o for o in window.outcomes)
        with self.assertRaises(ForwardPaperHistoryError):
            _rebuild_window_with_outcomes(window, outcomes)

    def test_missing_veto_evidence_rejected_when_omitting_a_genuine_affected_session(
        self,
    ) -> None:
        replay_sessions = list(_two_identity_replay_sessions(self.dates))
        missing_indices = (10, 40)
        for missing_index in missing_indices:
            missing_date = self.dates[missing_index]
            replay_sessions[missing_index] = _session(
                missing_date, (_record(missing_date, symbol="AAA", validated_isin=ISIN_A),)
            )
        window, _stream_sessions = _window_for(self.dataset, replay_sessions, self.dates)
        veto_b = next(o for o in window.outcomes if isinstance(o, ForwardPaperHistoryVeto))
        self.assertEqual(len(veto_b.evidence_session_ids), 2)
        # A veto naming only one of the two genuine affected sessions is a
        # valid subset -- it must still be rejected, since evidence must be
        # the exact complete set, not merely a valid subset.
        truncated = ForwardPaperHistoryVeto(
            spec_id=veto_b.spec_id,
            research_identity_id=veto_b.research_identity_id,
            signal_observation=veto_b.signal_observation,
            reason=veto_b.reason,
            evidence_session_ids=veto_b.evidence_session_ids[:1],
            evidence_observation_ids=(),
        )
        outcomes = tuple(truncated if o is veto_b else o for o in window.outcomes)
        with self.assertRaises(ForwardPaperHistoryError):
            _rebuild_window_with_outcomes(window, outcomes)

    def _duplicated_fixture(self, dup_indices):
        replay_sessions = _two_identity_replay_sessions(self.dates)
        stream_sessions, _seam = _stream_sessions_for(self.dataset, replay_sessions)
        for dup_index in dup_indices:
            stream_sessions[dup_index] = _direct_duplicate_identity_stream_session(
                self.dates[dup_index], ISIN_A, other_isin=ISIN_B
            )
        spec = _spec(self.dates)
        window = build_forward_paper_raw_history_window(spec, iter(stream_sessions))
        veto_a = next(o for o in window.outcomes if isinstance(o, ForwardPaperHistoryVeto))
        return window, veto_a

    def test_duplicated_veto_binds_precise_session_and_observation_ids_in_order(self) -> None:
        dup_index = 15
        window, veto_a = self._duplicated_fixture((dup_index,))
        self.assertIs(veto_a.reason, ForwardPaperHistoryVetoReason.REQUIRED_SESSION_DUPLICATED)
        self.assertEqual(
            veto_a.evidence_session_ids,
            (window.sessions[dup_index].price_stream_session_id,),
        )
        expected_observation_ids = tuple(
            o.observation_id
            for o in window.sessions[dup_index].observations
            if o.research_identity_id == veto_a.research_identity_id
        )
        self.assertEqual(len(expected_observation_ids), 2)
        self.assertEqual(veto_a.evidence_observation_ids, expected_observation_ids)

    def test_duplicated_veto_rejects_first_wins_truncated_evidence(self) -> None:
        dup_index = 15
        window, veto_a = self._duplicated_fixture((dup_index,))
        truncated = ForwardPaperHistoryVeto(
            spec_id=veto_a.spec_id,
            research_identity_id=veto_a.research_identity_id,
            signal_observation=veto_a.signal_observation,
            reason=veto_a.reason,
            evidence_session_ids=veto_a.evidence_session_ids,
            evidence_observation_ids=veto_a.evidence_observation_ids[:1],
        )
        outcomes = tuple(truncated if o is veto_a else o for o in window.outcomes)
        with self.assertRaises(ForwardPaperHistoryError):
            _rebuild_window_with_outcomes(window, outcomes)

    def test_duplicated_veto_rejects_reordered_observation_evidence(self) -> None:
        dup_index = 15
        window, veto_a = self._duplicated_fixture((dup_index,))
        reversed_ids = tuple(reversed(veto_a.evidence_observation_ids))
        self.assertNotEqual(reversed_ids, veto_a.evidence_observation_ids)
        reordered = ForwardPaperHistoryVeto(
            spec_id=veto_a.spec_id,
            research_identity_id=veto_a.research_identity_id,
            signal_observation=veto_a.signal_observation,
            reason=veto_a.reason,
            evidence_session_ids=veto_a.evidence_session_ids,
            evidence_observation_ids=reversed_ids,
        )
        outcomes = tuple(reordered if o is veto_a else o for o in window.outcomes)
        with self.assertRaises(ForwardPaperHistoryError):
            _rebuild_window_with_outcomes(window, outcomes)

    def test_duplicated_veto_rejects_unrelated_extra_session_evidence(self) -> None:
        dup_index = 15
        window, veto_a = self._duplicated_fixture((dup_index,))
        unrelated_session_id = window.sessions[0].price_stream_session_id
        extended = ForwardPaperHistoryVeto(
            spec_id=veto_a.spec_id,
            research_identity_id=veto_a.research_identity_id,
            signal_observation=veto_a.signal_observation,
            reason=veto_a.reason,
            evidence_session_ids=veto_a.evidence_session_ids + (unrelated_session_id,),
            evidence_observation_ids=veto_a.evidence_observation_ids,
        )
        outcomes = tuple(extended if o is veto_a else o for o in window.outcomes)
        with self.assertRaises(ForwardPaperHistoryError):
            _rebuild_window_with_outcomes(window, outcomes)

    def test_duplicated_veto_binds_complete_multi_session_evidence(self) -> None:
        dup_indices = (15, 45)
        window, veto_a = self._duplicated_fixture(dup_indices)
        self.assertIs(veto_a.reason, ForwardPaperHistoryVetoReason.REQUIRED_SESSION_DUPLICATED)
        expected_session_ids = tuple(
            window.sessions[index].price_stream_session_id for index in dup_indices
        )
        self.assertEqual(veto_a.evidence_session_ids, expected_session_ids)
        expected_observation_ids: list[str] = []
        for index in dup_indices:
            expected_observation_ids.extend(
                o.observation_id
                for o in window.sessions[index].observations
                if o.research_identity_id == veto_a.research_identity_id
            )
        self.assertEqual(veto_a.evidence_observation_ids, tuple(expected_observation_ids))
        # The complete canonical multi-session tuple -- exactly what the
        # builder itself produces -- must verify successfully.
        window.verify_content_identity()

    def test_duplicated_veto_evidence_rejected_when_omitting_a_genuine_affected_session(
        self,
    ) -> None:
        dup_indices = (15, 45)
        window, veto_a = self._duplicated_fixture(dup_indices)
        first_session_observation_ids = tuple(
            o.observation_id
            for o in window.sessions[dup_indices[0]].observations
            if o.research_identity_id == veto_a.research_identity_id
        )
        # A veto naming only the first of the two genuine affected sessions,
        # with only that session's own genuine duplicate observation IDs, is
        # a valid subset of the real anomaly -- it must still be rejected.
        truncated = ForwardPaperHistoryVeto(
            spec_id=veto_a.spec_id,
            research_identity_id=veto_a.research_identity_id,
            signal_observation=veto_a.signal_observation,
            reason=veto_a.reason,
            evidence_session_ids=veto_a.evidence_session_ids[:1],
            evidence_observation_ids=first_session_observation_ids,
        )
        outcomes = tuple(truncated if o is veto_a else o for o in window.outcomes)
        with self.assertRaises(ForwardPaperHistoryError):
            _rebuild_window_with_outcomes(window, outcomes)

    def _unresolved_fixture(self):
        replay_sessions = list(_two_identity_replay_sessions(self.dates))
        signal_date = self.dates[-1]
        record_a = _record(signal_date, symbol="AAA", validated_isin=ISIN_A)
        record_b = _record(signal_date, symbol="BBB", validated_isin=ISIN_B)
        unresolved = _unresolved_record(signal_date, symbol="CCC")
        replay_sessions[-1] = _session(signal_date, (record_a, record_b, unresolved))
        window, _stream_sessions = _window_for(self.dataset, replay_sessions, self.dates)
        veto_c = next(o for o in window.outcomes if isinstance(o, ForwardPaperHistoryVeto))
        return window, veto_c

    def test_unresolved_veto_binds_only_signal_session_evidence(self) -> None:
        window, veto_c = self._unresolved_fixture()
        self.assertIs(veto_c.reason, ForwardPaperHistoryVetoReason.SIGNAL_IDENTITY_UNRESOLVED)
        self.assertEqual(
            veto_c.evidence_session_ids, (window.signal_session.price_stream_session_id,)
        )
        self.assertEqual(veto_c.evidence_observation_ids, ())

    def test_unresolved_veto_rejects_evidence_from_another_session(self) -> None:
        window, veto_c = self._unresolved_fixture()
        wrong_session_id = window.sessions[0].price_stream_session_id
        tampered = ForwardPaperHistoryVeto(
            spec_id=veto_c.spec_id,
            research_identity_id=veto_c.research_identity_id,
            signal_observation=veto_c.signal_observation,
            reason=veto_c.reason,
            evidence_session_ids=(wrong_session_id,),
            evidence_observation_ids=(),
        )
        outcomes = tuple(tampered if o is veto_c else o for o in window.outcomes)
        with self.assertRaises(ForwardPaperHistoryError):
            _rebuild_window_with_outcomes(window, outcomes)


class ForwardPaperHistoryIteratorConstructionTests(unittest.TestCase):
    def test_iterator_construction_failure_with_planted_secret_is_sanitized(self) -> None:
        secret = "SECRET-PLANTED-VALUE-MUST-NOT-LEAK/var/data/topsecret.json"

        class _BoomIterable:
            def __iter__(self):
                raise ValueError(secret)

        dates = _dates()
        spec = _spec(dates)
        with self.assertRaises(ForwardPaperHistoryError) as context:
            build_forward_paper_raw_history_window(spec, _BoomIterable())
        exc = context.exception
        self.assertNotIn(secret, str(exc))
        self.assertNotIn(secret, repr(exc))
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)


class ForwardPaperHistoryRegressionTests(unittest.TestCase):
    def test_schema_versions_and_window_size_are_pinned(self) -> None:
        self.assertEqual(
            history_module.FORWARD_PAPER_HISTORY_WINDOW_SPEC_SCHEMA_VERSION,
            "forward-paper-history-window-spec/v1",
        )
        self.assertEqual(
            history_module.FORWARD_PAPER_HISTORY_CANDIDATE_SCHEMA_VERSION,
            "forward-paper-history-candidate/v1",
        )
        self.assertEqual(
            history_module.FORWARD_PAPER_HISTORY_VETO_SCHEMA_VERSION,
            "forward-paper-history-veto/v2",
        )
        self.assertEqual(
            history_module.FORWARD_PAPER_RAW_HISTORY_WINDOW_SCHEMA_VERSION,
            "forward-paper-raw-history-window/v2",
        )
        self.assertEqual(history_module.FORWARD_PAPER_HISTORY_WINDOW_SESSION_COUNT, 60)


if __name__ == "__main__":
    unittest.main()
