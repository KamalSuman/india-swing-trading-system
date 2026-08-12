from __future__ import annotations

import inspect
import unittest
from dataclasses import replace
from datetime import date
from decimal import Decimal
from unittest.mock import patch

from india_swing.evaluation import nse_archive_research_identity as identity_module
from india_swing.evaluation.nse_archive_research_identity import (
    RESEARCH_IDENTITY_SCHEMA_VERSION,
    NseArchiveResearchIdentityAdmissionSession,
    NseArchiveResearchIdentityAdmissionStatus,
    NseArchiveResearchIdentityBasis,
    NseArchiveResearchIdentityDecision,
    NseArchiveResearchIdentityError,
    NseArchiveResearchIdentityTransition,
    NseArchiveResearchIdentityTransitionKind,
    iter_nse_archive_research_identity_admission_sessions,
    research_identity_id_for_isin,
)
from india_swing.evaluation.nse_archive_research_replay import (
    NseArchiveResearchReplaySession,
    _build_replay_record,
    _build_replay_source_identity_claim,
)
from india_swing.market_data.nse_archive import EVIDENCE_PROFILE_UNRECONCILED

from tests.test_nse_archive_research_dataset import _baseline_dataset, _fake_sha256
from tests.test_nse_archive_research_replay import (
    _direct_session_kwargs,
    _valid_record,
    _valid_source_identity_claim,
)


def _record(session: date, *, symbol: str = "INFY", **overrides: object):
    return _build_replay_record(_valid_record(session, symbol=symbol, **overrides))


def _claim(session: date, *, symbol: str = "INFY", **overrides: object):
    return _build_replay_source_identity_claim(
        _valid_source_identity_claim(session, symbol=symbol, **overrides)
    )


def _session(market_session: date, records: tuple, **overrides: object) -> NseArchiveResearchReplaySession:
    kwargs = _direct_session_kwargs(market_session=market_session, records=records)
    kwargs.update(overrides)
    return NseArchiveResearchReplaySession(**kwargs)


def _unresolved_record(session: date, *, symbol: str = "CCC", **overrides: object):
    kwargs = dict(
        identity_status="SECURITY_MASTER_MISSING",
        validated_isin=None,
        security_master_financial_instrument_id=None,
        security_master_source_identifier=None,
    )
    kwargs.update(overrides)
    return _record(session, symbol=symbol, **kwargs)


class _FixedSessionsIterator:
    """A public replay-seam stand-in.

    ``invocation_count`` counts how many times the seam itself was called
    (i.e. how many separate upstream iterator constructions occurred) --
    this increments eagerly, the instant ``iter_verified_nse_archive_research_sessions(...)``
    is called, regardless of whether the resulting generator is ever
    iterated. ``calls`` counts how many sessions were actually pulled from
    the resulting generator via ``next()``. These are deliberately distinct:
    a caller that constructs the seam's generator once but only partially
    consumes it must show ``invocation_count == 1`` with ``calls`` possibly
    less than ``len(sessions)``.
    """

    def __init__(self, sessions: tuple) -> None:
        self._sessions = sessions
        self.invocation_count = 0
        self.calls = 0

    def __call__(self, dataset, reader):
        self.invocation_count += 1
        return self._generate()

    def _generate(self):
        for session in self._sessions:
            self.calls += 1
            yield session


class NseArchiveResearchIdentityHappyPathTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()

    def _run(self, sessions: tuple) -> list[NseArchiveResearchIdentityAdmissionSession]:
        with patch.object(
            identity_module,
            "iter_verified_nse_archive_research_sessions",
            _FixedSessionsIterator(sessions),
        ):
            return list(
                iter_nse_archive_research_identity_admission_sessions(
                    self.dataset, object()
                )
            )

    def test_validated_same_session_match_admits_deterministic_research_identity(self) -> None:
        record = _record(date(2024, 1, 1), symbol="INFY")
        session = _session(date(2024, 1, 1), (record,))
        [result] = self._run((session,))
        [decision] = result.decisions
        self.assertIs(decision.basis, NseArchiveResearchIdentityBasis.VALIDATED_SAME_SESSION_ISIN)
        self.assertIs(
            decision.admission_status,
            NseArchiveResearchIdentityAdmissionStatus.ADMITTED_VALIDATED,
        )
        self.assertIsNone(decision.source_claim_id)
        self.assertEqual(decision.source_isin, record.validated_isin)
        self.assertEqual(
            decision.research_identity_id, research_identity_id_for_isin(record.validated_isin)
        )
        self.assertEqual(result.admitted_validated_count, 1)
        self.assertTrue(result.research_identity_admission_complete)
        self.assertFalse(result.production_identity_resolution_complete)
        self.assertFalse(result.corporate_action_adjustment_complete)
        self.assertFalse(result.actionable)
        self.assertFalse(result.feature_eligible)

    def test_legacy_source_attested_claim_admits_and_matches_validated_isin_research_identity(
        self,
    ) -> None:
        isin = "INE467B01029"
        legacy_record = _record(
            date(2024, 1, 1),
            symbol="20MICRONS",
            identity_status="UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE",
            validated_isin=None,
            security_master_financial_instrument_id=None,
            security_master_source_identifier=None,
            financial_instrument_id=None,
            security_source_record_id=None,
            udiff_source_identifier=None,
        )
        claim = _claim(date(2024, 1, 1), symbol="20MICRONS", claimed_isin=isin)
        legacy_session = _session(
            date(2024, 1, 1),
            (legacy_record,),
            evidence_profile=EVIDENCE_PROFILE_UNRECONCILED,
            source_identity_claims=(claim,),
        )
        [legacy_result] = self._run((legacy_session,))
        [legacy_decision] = legacy_result.decisions
        self.assertIs(
            legacy_decision.basis, NseArchiveResearchIdentityBasis.LEGACY_SOURCE_ATTESTED_ISIN
        )
        self.assertIs(
            legacy_decision.admission_status,
            NseArchiveResearchIdentityAdmissionStatus.ADMITTED_SOURCE_ATTESTED,
        )
        self.assertEqual(legacy_decision.source_claim_id, claim.claim_id)
        self.assertEqual(legacy_decision.source_isin, isin)
        self.assertEqual(legacy_result.admitted_source_attested_count, 1)
        self.assertTrue(legacy_result.research_identity_admission_complete)

        validated_record = _record(date(2024, 1, 2), symbol="20MICRONS", validated_isin=isin)
        validated_session = _session(date(2024, 1, 2), (validated_record,))
        [validated_result] = self._run((validated_session,))
        [validated_decision] = validated_result.decisions
        self.assertEqual(
            validated_decision.research_identity_id, legacy_decision.research_identity_id
        )


class NseArchiveResearchIdentityUnresolvedAndAmbiguousTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()

    def _run(self, sessions: tuple) -> list[NseArchiveResearchIdentityAdmissionSession]:
        with patch.object(
            identity_module,
            "iter_verified_nse_archive_research_sessions",
            _FixedSessionsIterator(sessions),
        ):
            return list(
                iter_nse_archive_research_identity_admission_sessions(
                    self.dataset, object()
                )
            )

    def test_unresolved_record_is_blocked_with_no_research_identity(self) -> None:
        record = _unresolved_record(date(2024, 1, 1))
        session = _session(date(2024, 1, 1), (record,))
        [result] = self._run((session,))
        [decision] = result.decisions
        self.assertIs(decision.basis, NseArchiveResearchIdentityBasis.UNAVAILABLE)
        self.assertIs(
            decision.admission_status,
            NseArchiveResearchIdentityAdmissionStatus.BLOCKED_UNRESOLVED,
        )
        self.assertIsNone(decision.research_identity_id)
        self.assertIsNone(decision.source_isin)
        self.assertIsNone(decision.source_claim_id)
        self.assertEqual(result.blocked_unresolved_count, 1)
        self.assertFalse(result.research_identity_admission_complete)

    def test_record_with_both_validated_and_legacy_evidence_rejects_session(self) -> None:
        record = _record(date(2024, 1, 1), symbol="20MICRONS")
        claim = _claim(date(2024, 1, 1), symbol="20MICRONS")
        session = _session(
            date(2024, 1, 1),
            (record,),
            evidence_profile=EVIDENCE_PROFILE_UNRECONCILED,
            source_identity_claims=(claim,),
        )
        with self.assertRaises(NseArchiveResearchIdentityError):
            self._run((session,))


class NseArchiveResearchIdentityIsinValidationTests(unittest.TestCase):
    def _decision_kwargs(self, **overrides: object) -> dict:
        kwargs = dict(
            dataset_id=_fake_sha256("dataset"),
            replay_session_id=_fake_sha256("replay-session"),
            session_snapshot_id=_fake_sha256("session-snapshot"),
            market_session=date(2024, 1, 1),
            partition_id=_fake_sha256("partition"),
            partition_role=identity_module.ResearchSplitRole.TRAIN,
            record_id=_fake_sha256("record"),
            listing_key="NSE:INFY",
            symbol="INFY",
            series="EQ",
            source_claim_id=None,
            source_isin="INE009A01021",
            basis=NseArchiveResearchIdentityBasis.VALIDATED_SAME_SESSION_ISIN,
            admission_status=NseArchiveResearchIdentityAdmissionStatus.ADMITTED_VALIDATED,
            research_identity_id=research_identity_id_for_isin("INE009A01021"),
        )
        kwargs.update(overrides)
        return kwargs

    def test_research_identity_id_for_isin_rejects_non_canonical_values(self) -> None:
        cases = (
            "ABC",
            "NOT-AN-ISIN",
            "INE009A0102",
            "INE009A010211",
            "ine009a01021",
            "INE009A0102 ",
            "",
            12345,
            None,
        )
        for value in cases:
            with self.subTest(repr(value)):
                with self.assertRaises(NseArchiveResearchIdentityError):
                    research_identity_id_for_isin(value)

    def test_research_identity_id_for_isin_accepts_canonical_isins_deterministically(
        self,
    ) -> None:
        for isin in ("INE009A01021", "INE467B01029", "INE000A01001"):
            with self.subTest(isin):
                first = research_identity_id_for_isin(isin)
                second = research_identity_id_for_isin(isin)
                self.assertTrue(identity_module._is_sha256(first))
                self.assertEqual(first, second)

    def test_decision_construction_rejects_non_canonical_source_isin(self) -> None:
        cases = ("ABC", "NOT-AN-ISIN", "ine009a01021", "INE009A0102", "INE009A010211", " ")
        for isin in cases:
            with self.subTest(isin):
                with self.assertRaises(NseArchiveResearchIdentityError):
                    NseArchiveResearchIdentityDecision(
                        **self._decision_kwargs(
                            source_isin=isin,
                            research_identity_id=_fake_sha256("placeholder"),
                        )
                    )

    def test_decision_construction_accepts_representative_canonical_isins(self) -> None:
        for isin in ("INE009A01021", "INE467B01029"):
            with self.subTest(isin):
                decision = NseArchiveResearchIdentityDecision(
                    **self._decision_kwargs(
                        source_isin=isin, research_identity_id=research_identity_id_for_isin(isin)
                    )
                )
                decision.verify_content_identity()


class NseArchiveResearchIdentityAdmissionPathIsinRejectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()

    def _run(self, sessions: tuple) -> list[NseArchiveResearchIdentityAdmissionSession]:
        with patch.object(
            identity_module,
            "iter_verified_nse_archive_research_sessions",
            _FixedSessionsIterator(sessions),
        ):
            return list(
                iter_nse_archive_research_identity_admission_sessions(
                    self.dataset, object()
                )
            )

    def test_validated_evidence_with_non_isin_value_rejects_session(self) -> None:
        record = _record(
            date(2024, 1, 1),
            symbol="INFY",
            validated_isin="ABC",
            udiff_source_identifier="ABC",
            security_master_source_identifier="ABC",
        )
        session = _session(date(2024, 1, 1), (record,))
        with self.assertRaises(NseArchiveResearchIdentityError):
            self._run((session,))

    def test_legacy_source_attested_evidence_with_non_isin_value_rejects_session(self) -> None:
        legacy_record = _record(
            date(2024, 1, 1),
            symbol="20MICRONS",
            identity_status="UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE",
            validated_isin=None,
            security_master_financial_instrument_id=None,
            security_master_source_identifier=None,
            financial_instrument_id=None,
            security_source_record_id=None,
            udiff_source_identifier=None,
        )
        # Internally rehashed/valid under the replay layer's own broader
        # source-shape claim pattern, but "ABC" is not a canonical ISIN.
        claim = _claim(date(2024, 1, 1), symbol="20MICRONS", claimed_isin="ABC")
        session = _session(
            date(2024, 1, 1),
            (legacy_record,),
            evidence_profile=EVIDENCE_PROFILE_UNRECONCILED,
            source_identity_claims=(claim,),
        )
        with self.assertRaises(NseArchiveResearchIdentityError):
            self._run((session,))


class NseArchiveResearchIdentityDuplicateLaneTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()

    def _run(self, sessions: tuple) -> list[NseArchiveResearchIdentityAdmissionSession]:
        with patch.object(
            identity_module,
            "iter_verified_nse_archive_research_sessions",
            _FixedSessionsIterator(sessions),
        ):
            return list(
                iter_nse_archive_research_identity_admission_sessions(
                    self.dataset, object()
                )
            )

    def test_two_distinct_unresolved_records_on_same_lane_fail_closed(self) -> None:
        record_a = _unresolved_record(date(2024, 1, 1), symbol="CCC")
        # A genuinely different EQ record (distinct delivery_quantity, hence
        # a distinct record_id) on the exact same (listing_key, series) lane.
        record_b = _unresolved_record(date(2024, 1, 1), symbol="CCC", delivery_quantity=999)
        self.assertNotEqual(record_a.record_id, record_b.record_id)
        self.assertEqual(record_a.listing_key, record_b.listing_key)
        session = _session(date(2024, 1, 1), (record_a, record_b))
        with self.assertRaises(NseArchiveResearchIdentityError):
            self._run((session,))

    def test_duplicate_record_id_fails_closed(self) -> None:
        record = _unresolved_record(date(2024, 1, 1), symbol="CCC")
        session = _session(date(2024, 1, 1), (record, record))
        with self.assertRaises(NseArchiveResearchIdentityError):
            self._run((session,))

    def test_failed_session_never_updates_history_for_following_valid_session(self) -> None:
        record_a = _unresolved_record(date(2024, 1, 1), symbol="CCC")
        record_b = _unresolved_record(date(2024, 1, 1), symbol="CCC", delivery_quantity=999)
        failing_session = _session(date(2024, 1, 1), (record_a, record_b))
        later_record = _record(date(2024, 1, 2), symbol="CCC", validated_isin="INE009A01021")
        later_session = _session(date(2024, 1, 2), (later_record,))

        with self.assertRaises(NseArchiveResearchIdentityError):
            self._run((failing_session, later_session))

        # Re-running with only the later session must behave identically to
        # a first-ever observation of this listing key -- proving the failed
        # session never updated cross-session state.
        [later_result] = self._run((later_session,))
        self.assertEqual(later_result.transitions, ())
        self.assertIs(
            later_result.decisions[0].admission_status,
            NseArchiveResearchIdentityAdmissionStatus.ADMITTED_VALIDATED,
        )


class NseArchiveResearchIdentityCollisionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()

    def _run(self, sessions: tuple) -> list[NseArchiveResearchIdentityAdmissionSession]:
        with patch.object(
            identity_module,
            "iter_verified_nse_archive_research_sessions",
            _FixedSessionsIterator(sessions),
        ):
            return list(
                iter_nse_archive_research_identity_admission_sessions(
                    self.dataset, object()
                )
            )

    def test_same_session_isin_collision_blocks_both_lanes_with_no_winner(self) -> None:
        isin = "INE009A01021"
        record_a = _record(date(2024, 1, 1), symbol="AAA", validated_isin=isin)
        record_b = _record(date(2024, 1, 1), symbol="BBB", validated_isin=isin)
        session = _session(date(2024, 1, 1), (record_a, record_b))
        [result] = self._run((session,))
        statuses = {decision.listing_key: decision.admission_status for decision in result.decisions}
        self.assertEqual(
            statuses,
            {
                "NSE:AAA": NseArchiveResearchIdentityAdmissionStatus.BLOCKED_SAME_SESSION_ISIN_COLLISION,
                "NSE:BBB": NseArchiveResearchIdentityAdmissionStatus.BLOCKED_SAME_SESSION_ISIN_COLLISION,
            },
        )
        self.assertTrue(all(d.research_identity_id is None for d in result.decisions))
        self.assertEqual(result.blocked_collision_count, 2)
        self.assertFalse(result.research_identity_admission_complete)

    def test_collision_result_is_order_independent(self) -> None:
        isin = "INE009A01021"
        record_a = _record(date(2024, 1, 1), symbol="AAA", validated_isin=isin)
        record_b = _record(date(2024, 1, 1), symbol="BBB", validated_isin=isin)
        forward = _session(date(2024, 1, 1), (record_a, record_b))
        reversed_session = _session(date(2024, 1, 1), (record_b, record_a))
        [forward_result] = self._run((forward,))
        [reversed_result] = self._run((reversed_session,))
        forward_statuses = {d.listing_key: d.admission_status for d in forward_result.decisions}
        reversed_statuses = {d.listing_key: d.admission_status for d in reversed_result.decisions}
        self.assertEqual(forward_statuses, reversed_statuses)

    def test_blocked_rows_do_not_update_cross_session_state(self) -> None:
        isin = "INE009A01021"
        record_a = _record(date(2024, 1, 1), symbol="AAA", validated_isin=isin)
        record_b = _record(date(2024, 1, 1), symbol="BBB", validated_isin=isin)
        colliding_session = _session(date(2024, 1, 1), (record_a, record_b))
        later_record = _record(date(2024, 1, 2), symbol="AAA", validated_isin=isin)
        later_session = _session(date(2024, 1, 2), (later_record,))
        [_, later_result] = self._run((colliding_session, later_session))
        # No transition should reference the blocked session -- the listing
        # key had never been successfully admitted before this later session.
        self.assertEqual(later_result.transitions, ())
        [later_decision] = later_result.decisions
        self.assertIs(
            later_decision.admission_status,
            NseArchiveResearchIdentityAdmissionStatus.ADMITTED_VALIDATED,
        )


class NseArchiveResearchIdentityTransitionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()

    def _run(self, sessions: tuple) -> list[NseArchiveResearchIdentityAdmissionSession]:
        with patch.object(
            identity_module,
            "iter_verified_nse_archive_research_sessions",
            _FixedSessionsIterator(sessions),
        ):
            return list(
                iter_nse_archive_research_identity_admission_sessions(
                    self.dataset, object()
                )
            )

    def test_listing_key_rebound_transition_emitted_and_deterministic(self) -> None:
        first_record = _record(date(2024, 1, 1), symbol="AAA", validated_isin="INE009A01021")
        first_session = _session(date(2024, 1, 1), (first_record,))
        second_record = _record(date(2024, 1, 2), symbol="AAA", validated_isin="INE467B01029")
        second_session = _session(date(2024, 1, 2), (second_record,))
        [first_result, second_result] = self._run((first_session, second_session))
        self.assertEqual(first_result.transitions, ())
        [transition] = second_result.transitions
        self.assertIs(transition.kind, NseArchiveResearchIdentityTransitionKind.LISTING_KEY_REBOUND)
        self.assertEqual(transition.previous_listing_key, "NSE:AAA")
        self.assertEqual(transition.current_listing_key, "NSE:AAA")
        self.assertEqual(
            transition.previous_research_identity_id,
            research_identity_id_for_isin("INE009A01021"),
        )
        self.assertEqual(
            transition.current_research_identity_id,
            research_identity_id_for_isin("INE467B01029"),
        )
        # New identity is still admitted normally -- a rebound never blocks it.
        [decision] = second_result.decisions
        self.assertIs(
            decision.admission_status, NseArchiveResearchIdentityAdmissionStatus.ADMITTED_VALIDATED
        )
        # Determinism: rerunning independently produces the same transition_id.
        [_, second_result_again] = self._run((first_session, second_session))
        self.assertEqual(
            second_result_again.transitions[0].transition_id, transition.transition_id
        )

    def test_identity_symbol_changed_transition_emitted_and_deterministic(self) -> None:
        isin = "INE009A01021"
        first_record = _record(date(2024, 1, 1), symbol="AAA", validated_isin=isin)
        first_session = _session(date(2024, 1, 1), (first_record,))
        second_record = _record(date(2024, 1, 2), symbol="ZZZ", validated_isin=isin)
        second_session = _session(date(2024, 1, 2), (second_record,))
        [first_result, second_result] = self._run((first_session, second_session))
        self.assertEqual(first_result.transitions, ())
        [transition] = second_result.transitions
        self.assertIs(
            transition.kind, NseArchiveResearchIdentityTransitionKind.IDENTITY_SYMBOL_CHANGED
        )
        self.assertEqual(transition.previous_symbol, "AAA")
        self.assertEqual(transition.current_symbol, "ZZZ")
        self.assertEqual(
            transition.previous_research_identity_id, transition.current_research_identity_id
        )

    def test_rebound_does_not_block_new_identity_or_rewrite_earlier_session(self) -> None:
        first_record = _record(date(2024, 1, 1), symbol="AAA", validated_isin="INE009A01021")
        first_session = _session(date(2024, 1, 1), (first_record,))
        second_record = _record(date(2024, 1, 2), symbol="AAA", validated_isin="INE467B01029")
        second_session = _session(date(2024, 1, 2), (second_record,))
        [first_result, second_result] = self._run((first_session, second_session))
        first_decision_id_before = first_result.decisions[0].decision_id
        first_admission_session_id_before = first_result.admission_session_id
        # Processing the later session must not mutate the earlier result object.
        self.assertEqual(first_result.decisions[0].decision_id, first_decision_id_before)
        self.assertEqual(first_result.admission_session_id, first_admission_session_id_before)
        self.assertIs(
            second_result.decisions[0].admission_status,
            NseArchiveResearchIdentityAdmissionStatus.ADMITTED_VALIDATED,
        )


class NseArchiveResearchIdentityStreamingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()

    def test_first_session_yielded_without_consuming_later_sessions(self) -> None:
        session1 = _session(date(2024, 1, 1), (_record(date(2024, 1, 1), symbol="INFY"),))
        session2 = _session(date(2024, 1, 2), (_record(date(2024, 1, 2), symbol="OTHERCO"),))
        seam = _FixedSessionsIterator((session1, session2))
        with patch.object(
            identity_module, "iter_verified_nse_archive_research_sessions", seam
        ):
            iterator = iter_nse_archive_research_identity_admission_sessions(
                self.dataset, object()
            )
            first = next(iterator)
        self.assertEqual(seam.calls, 1)
        self.assertEqual(first.market_session, date(2024, 1, 1))

    def test_reader_capability_is_never_inspected_only_passed_through(self) -> None:
        source = inspect.getsource(identity_module)
        tree = __import__("ast").parse(source)
        reader_attribute_accesses = set()
        for node in __import__("ast").walk(tree):
            if (
                isinstance(node, __import__("ast").Attribute)
                and isinstance(node.value, __import__("ast").Name)
                and node.value.id == "reader"
            ):
                reader_attribute_accesses.add(node.attr)
        self.assertEqual(reader_attribute_accesses, set())


class NseArchiveResearchIdentityDirectConstructionTests(unittest.TestCase):
    def _decision_kwargs(self, **overrides: object) -> dict:
        isin = "INE009A01021"
        kwargs = dict(
            dataset_id=_fake_sha256("dataset"),
            replay_session_id=_fake_sha256("replay-session"),
            session_snapshot_id=_fake_sha256("session-snapshot"),
            market_session=date(2024, 1, 1),
            partition_id=_fake_sha256("partition"),
            partition_role=identity_module.ResearchSplitRole.TRAIN,
            record_id=_fake_sha256("record"),
            listing_key="NSE:INFY",
            symbol="INFY",
            series="EQ",
            source_claim_id=None,
            source_isin=isin,
            basis=NseArchiveResearchIdentityBasis.VALIDATED_SAME_SESSION_ISIN,
            admission_status=NseArchiveResearchIdentityAdmissionStatus.ADMITTED_VALIDATED,
            research_identity_id=research_identity_id_for_isin(isin),
        )
        kwargs.update(overrides)
        return kwargs

    def test_valid_decision_recomputed_id_matches_source_and_is_deterministic(self) -> None:
        decision = NseArchiveResearchIdentityDecision(**self._decision_kwargs())
        decision.verify_content_identity()
        again = NseArchiveResearchIdentityDecision(**self._decision_kwargs())
        self.assertEqual(decision.decision_id, again.decision_id)

    def test_decision_construction_rejects_malformed_fields(self) -> None:
        cases = {
            "malformed_isin": {"source_isin": "not-an-isin"},
            "noncanonical_symbol": {"symbol": "in fy", "listing_key": "NSE:in fy"},
            "non_eq_series": {"series": "BE"},
            "mismatched_listing_key": {"listing_key": "NSE:WRONG"},
            "stray_claim_id_on_validated_basis": {"source_claim_id": _fake_sha256("stray")},
            "missing_isin_on_validated_basis": {"source_isin": None},
            "blocked_status_with_research_identity": {
                "admission_status": NseArchiveResearchIdentityAdmissionStatus.BLOCKED_UNRESOLVED,
            },
            "admitted_status_without_research_identity": {"research_identity_id": None},
            "unavailable_basis_with_isin": {
                "basis": NseArchiveResearchIdentityBasis.UNAVAILABLE,
                "admission_status": NseArchiveResearchIdentityAdmissionStatus.BLOCKED_UNRESOLVED,
            },
            "forged_research_identity_id": {
                "research_identity_id": _fake_sha256("forged"),
            },
        }
        for label, overrides in cases.items():
            with self.subTest(label):
                with self.assertRaises(NseArchiveResearchIdentityError) as context:
                    NseArchiveResearchIdentityDecision(**self._decision_kwargs(**overrides))
                exc = context.exception
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

    def test_legacy_basis_requires_both_isin_and_claim_id(self) -> None:
        isin = "INE467B01029"
        kwargs = self._decision_kwargs(
            basis=NseArchiveResearchIdentityBasis.LEGACY_SOURCE_ATTESTED_ISIN,
            admission_status=NseArchiveResearchIdentityAdmissionStatus.ADMITTED_SOURCE_ATTESTED,
            source_isin=isin,
            source_claim_id=_fake_sha256("claim"),
            research_identity_id=research_identity_id_for_isin(isin),
        )
        decision = NseArchiveResearchIdentityDecision(**kwargs)
        decision.verify_content_identity()

        with self.assertRaises(NseArchiveResearchIdentityError):
            NseArchiveResearchIdentityDecision(**{**kwargs, "source_claim_id": None})

    def test_unavailable_basis_requires_blocked_unresolved_and_no_evidence(self) -> None:
        kwargs = self._decision_kwargs(
            basis=NseArchiveResearchIdentityBasis.UNAVAILABLE,
            admission_status=NseArchiveResearchIdentityAdmissionStatus.BLOCKED_UNRESOLVED,
            source_isin=None,
            source_claim_id=None,
            research_identity_id=None,
        )
        decision = NseArchiveResearchIdentityDecision(**kwargs)
        decision.verify_content_identity()

    def test_decision_dataclass_rejects_post_construction_tampering(self) -> None:
        decision = NseArchiveResearchIdentityDecision(**self._decision_kwargs())
        with self.assertRaises(NseArchiveResearchIdentityError):
            replace(decision, research_identity_id=_fake_sha256("forged-research-identity"))

    def _transition_kwargs(self, **overrides: object) -> dict:
        identity_id = _fake_sha256("identity")
        kwargs = dict(
            kind=NseArchiveResearchIdentityTransitionKind.LISTING_KEY_REBOUND,
            previous_market_session=date(2024, 1, 1),
            current_market_session=date(2024, 1, 2),
            previous_record_id=_fake_sha256("prev-record"),
            current_record_id=_fake_sha256("cur-record"),
            previous_research_identity_id=identity_id,
            current_research_identity_id=_fake_sha256("other-identity"),
            previous_listing_key="NSE:AAA",
            current_listing_key="NSE:AAA",
            previous_symbol="AAA",
            current_symbol="AAA",
        )
        kwargs.update(overrides)
        return kwargs

    def test_valid_transition_recomputed_id_matches_source_and_is_deterministic(self) -> None:
        transition = NseArchiveResearchIdentityTransition(**self._transition_kwargs())
        transition.verify_content_identity()
        again = NseArchiveResearchIdentityTransition(**self._transition_kwargs())
        self.assertEqual(transition.transition_id, again.transition_id)

    def test_transition_construction_rejects_malformed_fields(self) -> None:
        cases = {
            "non_increasing_sessions": {
                "current_market_session": date(2024, 1, 1),
            },
            "noncanonical_symbol": {"previous_symbol": "a a"},
            "mismatched_previous_listing_key": {"previous_listing_key": "NSE:WRONG"},
            "rebound_with_changed_listing_key": {"current_listing_key": "NSE:BBB", "current_symbol": "BBB"},
            "rebound_with_same_identity": {
                "current_research_identity_id": self._transition_kwargs()[
                    "previous_research_identity_id"
                ],
            },
        }
        for label, overrides in cases.items():
            with self.subTest(label):
                with self.assertRaises(NseArchiveResearchIdentityError) as context:
                    NseArchiveResearchIdentityTransition(**self._transition_kwargs(**overrides))
                exc = context.exception
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

    def test_identity_symbol_changed_transition_requires_same_identity_and_different_symbol(
        self,
    ) -> None:
        identity_id = _fake_sha256("shared-identity")
        kwargs = self._transition_kwargs(
            kind=NseArchiveResearchIdentityTransitionKind.IDENTITY_SYMBOL_CHANGED,
            previous_research_identity_id=identity_id,
            current_research_identity_id=identity_id,
            previous_listing_key="NSE:AAA",
            current_listing_key="NSE:ZZZ",
            previous_symbol="AAA",
            current_symbol="ZZZ",
        )
        transition = NseArchiveResearchIdentityTransition(**kwargs)
        transition.verify_content_identity()

        with self.assertRaises(NseArchiveResearchIdentityError):
            NseArchiveResearchIdentityTransition(**{**kwargs, "current_symbol": "AAA"})

    def test_transition_dataclass_rejects_post_construction_tampering(self) -> None:
        transition = NseArchiveResearchIdentityTransition(**self._transition_kwargs())
        with self.assertRaises(NseArchiveResearchIdentityError):
            replace(transition, current_symbol="ZZZ")

    def _valid_transition_for_decision(
        self, decision: NseArchiveResearchIdentityDecision, **overrides: object
    ) -> NseArchiveResearchIdentityTransition:
        kwargs = dict(
            kind=NseArchiveResearchIdentityTransitionKind.LISTING_KEY_REBOUND,
            previous_market_session=date(2023, 12, 31),
            current_market_session=decision.market_session,
            previous_record_id=_fake_sha256("prev-record"),
            current_record_id=decision.record_id,
            previous_research_identity_id=_fake_sha256("prev-identity"),
            current_research_identity_id=decision.research_identity_id,
            previous_listing_key=decision.listing_key,
            current_listing_key=decision.listing_key,
            previous_symbol=decision.symbol,
            current_symbol=decision.symbol,
        )
        kwargs.update(overrides)
        return NseArchiveResearchIdentityTransition(**kwargs)

    def _admission_session_kwargs(self, **overrides: object) -> dict:
        decision = NseArchiveResearchIdentityDecision(**self._decision_kwargs())
        kwargs = dict(
            dataset_id=decision.dataset_id,
            replay_session_id=decision.replay_session_id,
            session_snapshot_id=decision.session_snapshot_id,
            market_session=decision.market_session,
            partition_id=decision.partition_id,
            partition_role=decision.partition_role,
            decisions=(decision,),
            transitions=(),
            admitted_validated_count=1,
            admitted_source_attested_count=0,
            blocked_unresolved_count=0,
            blocked_collision_count=0,
        )
        kwargs.update(overrides)
        return kwargs

    def test_valid_admission_session_recomputed_id_matches_source_and_is_deterministic(
        self,
    ) -> None:
        session = NseArchiveResearchIdentityAdmissionSession(**self._admission_session_kwargs())
        session.verify_content_identity()
        again = NseArchiveResearchIdentityAdmissionSession(**self._admission_session_kwargs())
        self.assertEqual(session.admission_session_id, again.admission_session_id)
        self.assertTrue(session.research_identity_admission_complete)
        self.assertFalse(session.production_identity_resolution_complete)
        self.assertFalse(session.corporate_action_adjustment_complete)

    def test_admission_session_rejects_mismatched_counts(self) -> None:
        with self.assertRaises(NseArchiveResearchIdentityError):
            NseArchiveResearchIdentityAdmissionSession(
                **self._admission_session_kwargs(admitted_validated_count=2)
            )

    def test_admission_session_rejects_duplicate_decisions(self) -> None:
        decision = NseArchiveResearchIdentityDecision(**self._decision_kwargs())
        with self.assertRaises(NseArchiveResearchIdentityError):
            NseArchiveResearchIdentityAdmissionSession(
                **self._admission_session_kwargs(
                    decisions=(decision, decision), admitted_validated_count=2
                )
            )

    def test_admission_session_rejects_cross_session_decision_lineage(self) -> None:
        wrong_decision = NseArchiveResearchIdentityDecision(
            **self._decision_kwargs(dataset_id=_fake_sha256("different-dataset"))
        )
        with self.assertRaises(NseArchiveResearchIdentityError):
            NseArchiveResearchIdentityAdmissionSession(
                **self._admission_session_kwargs(decisions=(wrong_decision,))
            )

    def test_admission_session_rejects_duplicate_and_reordered_transitions(self) -> None:
        decision = NseArchiveResearchIdentityDecision(**self._decision_kwargs())
        first = self._valid_transition_for_decision(decision)
        second = self._valid_transition_for_decision(
            decision, previous_research_identity_id=_fake_sha256("yet-another-prior-identity")
        )
        ordered = tuple(sorted((first, second), key=lambda value: value.transition_id))

        with self.assertRaises(NseArchiveResearchIdentityError):
            NseArchiveResearchIdentityAdmissionSession(
                **self._admission_session_kwargs(decisions=(decision,), transitions=(first, first))
            )
        if ordered[0] is ordered[1]:
            self.fail("expected two distinct transitions")
        reordered = tuple(reversed(ordered))
        if reordered != ordered:
            with self.assertRaises(NseArchiveResearchIdentityError):
                NseArchiveResearchIdentityAdmissionSession(
                    **self._admission_session_kwargs(decisions=(decision,), transitions=reordered)
                )
        # The canonically (transition_id-sorted) ordered pair remains valid.
        session = NseArchiveResearchIdentityAdmissionSession(
            **self._admission_session_kwargs(decisions=(decision,), transitions=ordered)
        )
        session.verify_content_identity()

    def test_admission_session_rejects_transition_that_looks_ahead(self) -> None:
        decision = NseArchiveResearchIdentityDecision(**self._decision_kwargs())
        future_transition = self._valid_transition_for_decision(
            decision, current_market_session=date(2099, 1, 1)
        )
        with self.assertRaises(NseArchiveResearchIdentityError):
            NseArchiveResearchIdentityAdmissionSession(
                **self._admission_session_kwargs(
                    decisions=(decision,), transitions=(future_transition,)
                )
            )

    def test_admission_session_accepts_genuine_transition_bound_to_admitted_decision(
        self,
    ) -> None:
        decision = NseArchiveResearchIdentityDecision(**self._decision_kwargs())
        transition = self._valid_transition_for_decision(decision)
        session = NseArchiveResearchIdentityAdmissionSession(
            **self._admission_session_kwargs(decisions=(decision,), transitions=(transition,))
        )
        session.verify_content_identity()

    def test_admission_session_rejects_transition_earlier_than_containing_session(self) -> None:
        decision = NseArchiveResearchIdentityDecision(**self._decision_kwargs())
        earlier_transition = self._valid_transition_for_decision(
            decision,
            previous_market_session=date(2019, 1, 1),
            current_market_session=date(2020, 1, 1),
        )
        with self.assertRaises(NseArchiveResearchIdentityError):
            NseArchiveResearchIdentityAdmissionSession(
                **self._admission_session_kwargs(
                    decisions=(decision,), transitions=(earlier_transition,)
                )
            )

    def test_admission_session_rejects_transition_with_orphan_current_record_id(self) -> None:
        decision = NseArchiveResearchIdentityDecision(**self._decision_kwargs())
        orphan_transition = self._valid_transition_for_decision(
            decision, current_record_id=_fake_sha256("no-such-record")
        )
        with self.assertRaises(NseArchiveResearchIdentityError):
            NseArchiveResearchIdentityAdmissionSession(
                **self._admission_session_kwargs(
                    decisions=(decision,), transitions=(orphan_transition,)
                )
            )

    def test_admission_session_rejects_transition_with_mismatched_current_fields(self) -> None:
        decision = NseArchiveResearchIdentityDecision(**self._decision_kwargs())
        cases = {
            "identity": {"current_research_identity_id": _fake_sha256("wrong-identity")},
            "listing_key_and_symbol": {
                # Internally self-consistent (previous == current, satisfying
                # the transition's own LISTING_KEY_REBOUND shape rule) but
                # for a listing key that is not this decision's own -- must
                # be caught by the session's cross-check against the decision.
                "previous_listing_key": "NSE:WRONG",
                "current_listing_key": "NSE:WRONG",
                "previous_symbol": "WRONG",
                "current_symbol": "WRONG",
            },
        }
        for label, overrides in cases.items():
            with self.subTest(label):
                mismatched = self._valid_transition_for_decision(decision, **overrides)
                with self.assertRaises(NseArchiveResearchIdentityError):
                    NseArchiveResearchIdentityAdmissionSession(
                        **self._admission_session_kwargs(
                            decisions=(decision,), transitions=(mismatched,)
                        )
                    )

    def test_admission_session_rejects_transition_referencing_blocked_decision(self) -> None:
        blocked_decision = NseArchiveResearchIdentityDecision(
            **self._decision_kwargs(
                basis=NseArchiveResearchIdentityBasis.UNAVAILABLE,
                admission_status=NseArchiveResearchIdentityAdmissionStatus.BLOCKED_UNRESOLVED,
                source_isin=None,
                source_claim_id=None,
                research_identity_id=None,
            )
        )
        transition = self._valid_transition_for_decision(
            blocked_decision, current_research_identity_id=_fake_sha256("would-be-identity")
        )
        with self.assertRaises(NseArchiveResearchIdentityError):
            NseArchiveResearchIdentityAdmissionSession(
                **self._admission_session_kwargs(
                    decisions=(blocked_decision,),
                    transitions=(transition,),
                    admitted_validated_count=0,
                    blocked_unresolved_count=1,
                )
            )

    def test_admission_session_dataclass_rejects_post_construction_tampering(self) -> None:
        session = NseArchiveResearchIdentityAdmissionSession(**self._admission_session_kwargs())
        with self.assertRaises(NseArchiveResearchIdentityError):
            replace(session, blocked_unresolved_count=1)


class NseArchiveResearchIdentityErrorSanitizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()

    def test_reader_exception_with_planted_secret_does_not_leak(self) -> None:
        secret = "SECRET-PLANTED-VALUE-MUST-NOT-LEAK/var/data/topsecret.json"

        def _boom(dataset, reader):
            raise ValueError(secret)
            yield  # pragma: no cover - makes this a generator function

        with patch.object(
            identity_module, "iter_verified_nse_archive_research_sessions", _boom
        ):
            with self.assertRaises(NseArchiveResearchIdentityError) as context:
                list(
                    iter_nse_archive_research_identity_admission_sessions(
                        self.dataset, object()
                    )
                )
        exc = context.exception
        self.assertNotIn(secret, str(exc))
        self.assertNotIn(secret, repr(exc))
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)

    def test_malformed_replay_session_type_with_planted_secret_does_not_leak(self) -> None:
        secret = "PATH-SECRET/leak/should/not/appear"

        class _ExplodingSession:
            def __repr__(self) -> str:
                raise TypeError(secret)

        def _yields_wrong_type(dataset, reader):
            yield _ExplodingSession()

        with patch.object(
            identity_module, "iter_verified_nse_archive_research_sessions", _yields_wrong_type
        ):
            with self.assertRaises(NseArchiveResearchIdentityError) as context:
                list(
                    iter_nse_archive_research_identity_admission_sessions(
                        self.dataset, object()
                    )
                )
        exc = context.exception
        self.assertNotIn(secret, str(exc))
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)

    def test_tampered_replay_session_from_seam_is_independently_reverified_and_rejected(
        self,
    ) -> None:
        record = _record(date(2024, 1, 1), symbol="INFY")
        session = _session(date(2024, 1, 1), (record,))
        # Post-construction tamper bypassing __post_init__: swap in a record
        # whose session no longer matches the containing session's own
        # market_session. session.verify_content_identity() must catch this.
        other_record = _record(date(2024, 1, 2), symbol="OTHERCO")
        object.__setattr__(session, "records", (other_record,))

        with patch.object(
            identity_module,
            "iter_verified_nse_archive_research_sessions",
            _FixedSessionsIterator((session,)),
        ):
            with self.assertRaises(NseArchiveResearchIdentityError) as context:
                list(
                    iter_nse_archive_research_identity_admission_sessions(
                        self.dataset, object()
                    )
                )
        exc = context.exception
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)


class NseArchiveResearchIdentityStructuralTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source = inspect.getsource(identity_module)

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

    def test_no_store_construction_persistence_or_discovery(self) -> None:
        forbidden = (
            "MarketSnapshotStore(",
            ".put(",
            "pickle.",
            "shelve.",
            "sqlite3.",
            "json.dump",
            ".glob(",
            ".iterdir(",
            ".listdir(",
            "find_by_selection_key",
            "latest_at_or_before",
            ".list(",
            "resume",
            "checkpoint",
            "manifest_path",
            "cache_dir",
        )
        lowered = self.source.lower()
        for token in forbidden:
            self.assertNotIn(token.lower(), lowered, msg=f"forbidden token found: {token}")

    def test_no_corporate_action_feature_signal_or_execution_capability(self) -> None:
        # Note: corporate_action_adjustment_complete is a required always-False
        # safety flag (architecture_contract), not a capability -- only actual
        # adjustment/computation verbs are forbidden here, not that field name.
        forbidden = (
            "apply_corporate_action",
            "adjust_corporate_action",
            "adjusted_price",
            "compute_feature",
            "calculate_return",
            "generate_signal",
            "rank(",
            "send_alert",
            "place_order",
            "execute_order",
            "confidence_score",
            "kronos",
            "telegram",
            "broker",
            "openai",
            "anthropic",
        )
        lowered = self.source.lower()
        for token in forbidden:
            self.assertNotIn(token, lowered, msg=f"forbidden token found: {token}")

    def test_reader_capability_is_only_ever_passed_through_never_inspected(self) -> None:
        import ast

        tree = ast.parse(self.source)
        reader_attribute_accesses = set()
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id == "reader"
            ):
                reader_attribute_accesses.add(node.attr)
        self.assertEqual(reader_attribute_accesses, set())


class NseArchiveResearchPairedSessionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()

    def test_paired_iterator_invokes_upstream_seam_exactly_once_per_session(self) -> None:
        session1 = _session(date(2024, 1, 1), (_record(date(2024, 1, 1), symbol="INFY"),))
        session2 = _session(date(2024, 1, 2), (_record(date(2024, 1, 2), symbol="OTHERCO"),))
        seam = _FixedSessionsIterator((session1, session2))
        with patch.object(
            identity_module, "iter_verified_nse_archive_research_sessions", seam
        ):
            results = list(
                identity_module.iter_nse_archive_research_paired_sessions(
                    self.dataset, object()
                )
            )
        # Exactly one upstream iterator construction for this one call to the
        # public iterator, regardless of how many sessions it yields.
        self.assertEqual(seam.invocation_count, 1)
        self.assertEqual(seam.calls, 2)
        self.assertEqual(len(results), 2)
        self.assertIs(results[0].replay_session, session1)
        self.assertIs(results[1].replay_session, session2)
        self.assertEqual(results[0].admission_session.market_session, date(2024, 1, 1))
        self.assertEqual(results[1].admission_session.market_session, date(2024, 1, 2))

    def test_paired_and_admission_iterators_each_construct_upstream_iterator_exactly_once(
        self,
    ) -> None:
        # A stricter regression than counting sessions consumed: separately
        # prove the upstream seam FUNCTION itself (not just its yielded
        # sessions) is invoked exactly once per public iterator call, for
        # both the paired iterator and the admission iterator it now
        # projects from.
        record = _record(date(2024, 1, 1), symbol="INFY")
        session = _session(date(2024, 1, 1), (record,))

        paired_seam = _FixedSessionsIterator((session,))
        with patch.object(
            identity_module, "iter_verified_nse_archive_research_sessions", paired_seam
        ):
            paired_results = list(
                identity_module.iter_nse_archive_research_paired_sessions(
                    self.dataset, object()
                )
            )
        self.assertEqual(paired_seam.invocation_count, 1)
        self.assertEqual(paired_seam.calls, 1)
        self.assertEqual(len(paired_results), 1)

        admission_seam = _FixedSessionsIterator((session,))
        with patch.object(
            identity_module, "iter_verified_nse_archive_research_sessions", admission_seam
        ):
            admission_results = list(
                iter_nse_archive_research_identity_admission_sessions(
                    self.dataset, object()
                )
            )
        self.assertEqual(admission_seam.invocation_count, 1)
        self.assertEqual(admission_seam.calls, 1)
        self.assertEqual(len(admission_results), 1)

    def test_paired_iterator_is_lazy_and_only_retains_bounded_prior_state(self) -> None:
        session1 = _session(date(2024, 1, 1), (_record(date(2024, 1, 1), symbol="INFY"),))
        session2 = _session(date(2024, 1, 2), (_record(date(2024, 1, 2), symbol="OTHERCO"),))
        seam = _FixedSessionsIterator((session1, session2))
        with patch.object(
            identity_module, "iter_verified_nse_archive_research_sessions", seam
        ):
            iterator = identity_module.iter_nse_archive_research_paired_sessions(
                self.dataset, object()
            )
            first = next(iterator)
        self.assertEqual(seam.invocation_count, 1)
        self.assertEqual(seam.calls, 1)
        self.assertEqual(first.replay_session.market_session, date(2024, 1, 1))

    def test_boundary_iterator_warms_prior_identity_state_without_yielding_prior_pairs(
        self,
    ) -> None:
        first = _session(
            date(2024, 1, 1),
            (_record(date(2024, 1, 1), symbol="AAA", validated_isin="INE009A01021"),),
        )
        second = _session(
            date(2024, 1, 2),
            (_record(date(2024, 1, 2), symbol="AAA", validated_isin="INE467B01029"),),
        )
        third = _session(
            date(2024, 1, 3),
            (_record(date(2024, 1, 3), symbol="BBB", validated_isin="INE467B01029"),),
        )
        seam = _FixedSessionsIterator((first, second, third))
        with patch.object(
            identity_module, "iter_verified_nse_archive_research_sessions", seam
        ):
            results = list(
                identity_module.iter_nse_archive_research_paired_sessions_from(
                    self.dataset,
                    object(),
                    start_session=date(2024, 1, 2),
                )
            )

        self.assertEqual(seam.calls, 3)
        self.assertEqual(
            tuple(value.replay_session.market_session for value in results),
            (date(2024, 1, 2), date(2024, 1, 3)),
        )
        self.assertIs(
            results[0].admission_session.transitions[0].kind,
            NseArchiveResearchIdentityTransitionKind.LISTING_KEY_REBOUND,
        )
        self.assertIs(
            results[1].admission_session.transitions[0].kind,
            NseArchiveResearchIdentityTransitionKind.IDENTITY_SYMBOL_CHANGED,
        )

    def test_admission_iterator_still_works_via_single_upstream_traversal(self) -> None:
        # Refactoring the admission iterator to project from the paired
        # iterator must still call the upstream replay seam exactly once
        # per session and preserve every existing admission output.
        record = _record(date(2024, 1, 1), symbol="INFY")
        session = _session(date(2024, 1, 1), (record,))
        seam = _FixedSessionsIterator((session,))
        with patch.object(
            identity_module, "iter_verified_nse_archive_research_sessions", seam
        ):
            admission_results = list(
                iter_nse_archive_research_identity_admission_sessions(
                    self.dataset, object()
                )
            )
        self.assertEqual(seam.calls, 1)
        [result] = admission_results
        self.assertIs(
            result.decisions[0].admission_status,
            NseArchiveResearchIdentityAdmissionStatus.ADMITTED_VALIDATED,
        )

    def test_paired_type_recomputed_id_matches_source_and_is_deterministic(self) -> None:
        record = _record(date(2024, 1, 1), symbol="INFY")
        session = _session(date(2024, 1, 1), (record,))
        seam = _FixedSessionsIterator((session,))
        with patch.object(
            identity_module, "iter_verified_nse_archive_research_sessions", seam
        ):
            [paired] = list(
                identity_module.iter_nse_archive_research_paired_sessions(
                    self.dataset, object()
                )
            )
        paired.verify_content_identity()
        again = identity_module.NseArchiveResearchPairedSession(
            replay_session=paired.replay_session, admission_session=paired.admission_session
        )
        self.assertEqual(paired.paired_session_id, again.paired_session_id)

    def test_paired_session_from_seam_rejects_tampered_replay_content(self) -> None:
        record = _record(date(2024, 1, 1), symbol="INFY")
        session = _session(date(2024, 1, 1), (record,))
        other_record = _record(date(2024, 1, 2), symbol="OTHERCO")
        # Post-construction tamper bypassing __post_init__: the session's own
        # verify_content_identity() must catch the session/record mismatch.
        object.__setattr__(session, "records", (other_record,))
        seam = _FixedSessionsIterator((session,))
        with patch.object(
            identity_module, "iter_verified_nse_archive_research_sessions", seam
        ):
            with self.assertRaises(NseArchiveResearchIdentityError) as context:
                list(
                    identity_module.iter_nse_archive_research_paired_sessions(
                        self.dataset, object()
                    )
                )
        exc = context.exception
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)

    def _real_paired_session(self, replay_session: NseArchiveResearchReplaySession):
        with patch.object(
            identity_module,
            "iter_verified_nse_archive_research_sessions",
            _FixedSessionsIterator((replay_session,)),
        ):
            [paired] = list(
                identity_module.iter_nse_archive_research_paired_sessions(
                    self.dataset, object()
                )
            )
        return paired

    def test_paired_direct_construction_rejects_mismatched_lineage_fields(self) -> None:
        record = _record(date(2024, 1, 1), symbol="INFY")
        session_a = _session(date(2024, 1, 1), (record,))
        session_b_kwargs = _direct_session_kwargs(
            market_session=date(2024, 1, 1), records=(record,)
        )
        session_b_kwargs["dataset_id"] = _fake_sha256("different-dataset")
        session_b = NseArchiveResearchReplaySession(**session_b_kwargs)

        paired_a = self._real_paired_session(session_a)
        paired_b = self._real_paired_session(session_b)
        with self.assertRaises(NseArchiveResearchIdentityError) as context:
            identity_module.NseArchiveResearchPairedSession(
                replay_session=paired_b.replay_session,
                admission_session=paired_a.admission_session,
            )
        exc = context.exception
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)

    def test_paired_type_rejects_each_shared_lineage_mismatch_independently(self) -> None:
        # Table-driven: each of the six shared-lineage fields must be able,
        # on its own, to trigger the paired-lineage rejection.
        def variant_admission_session(**overrides: object):
            record = overrides.pop("record", None) or _record(
                date(2024, 1, 1), symbol="INFY"
            )
            kwargs = _direct_session_kwargs(
                market_session=overrides.pop("market_session", date(2024, 1, 1)),
                records=(record,),
            )
            kwargs.update(overrides)
            session = NseArchiveResearchReplaySession(**kwargs)
            return self._real_paired_session(session).admission_session

        baseline_replay = _session(date(2024, 1, 1), (_record(date(2024, 1, 1), symbol="INFY"),))

        cases = {
            "dataset_id": variant_admission_session(dataset_id=_fake_sha256("other-dataset")),
            "session_snapshot_id": variant_admission_session(
                session_snapshot_id=_fake_sha256("other-snapshot")
            ),
            "partition_id": variant_admission_session(
                partition_id=_fake_sha256("other-partition")
            ),
            "partition_role": variant_admission_session(
                partition_role=identity_module.ResearchSplitRole.VALIDATION
            ),
            "market_session": variant_admission_session(
                record=_record(date(2024, 1, 2), symbol="INFY"),
                market_session=date(2024, 1, 2),
            ),
            "replay_session_id": variant_admission_session(
                record=_record(date(2024, 1, 1), symbol="INFY", delivery_quantity=999),
            ),
        }
        for label, mismatched_admission_session in cases.items():
            with self.subTest(label):
                with self.assertRaises(NseArchiveResearchIdentityError) as context:
                    identity_module.NseArchiveResearchPairedSession(
                        replay_session=baseline_replay,
                        admission_session=mismatched_admission_session,
                    )
                exc = context.exception
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)

    def test_paired_type_rejects_wrong_nested_types(self) -> None:
        record = _record(date(2024, 1, 1), symbol="INFY")
        session = _session(date(2024, 1, 1), (record,))
        with patch.object(
            identity_module,
            "iter_verified_nse_archive_research_sessions",
            _FixedSessionsIterator((session,)),
        ):
            [paired] = list(
                identity_module.iter_nse_archive_research_paired_sessions(
                    self.dataset, object()
                )
            )
        with self.assertRaises(NseArchiveResearchIdentityError):
            identity_module.NseArchiveResearchPairedSession(
                replay_session=object(), admission_session=paired.admission_session
            )
        with self.assertRaises(NseArchiveResearchIdentityError):
            identity_module.NseArchiveResearchPairedSession(
                replay_session=paired.replay_session, admission_session=object()
            )

    def test_paired_session_reports_fail_closed_posture(self) -> None:
        record = _record(date(2024, 1, 1), symbol="INFY")
        session = _session(date(2024, 1, 1), (record,))
        paired = self._real_paired_session(session)
        self.assertTrue(paired.collection_only)
        self.assertFalse(paired.actionable)
        self.assertFalse(paired.training_eligible)
        self.assertFalse(paired.feature_eligible)
        self.assertFalse(paired.label_eligible)
        self.assertFalse(paired.alert_eligible)
        self.assertFalse(paired.execution_eligible)
        self.assertFalse(paired.production_identity_resolution_complete)
        self.assertFalse(paired.corporate_action_adjustment_complete)
        # The posture is a fixed constant of the type, not stored state, and
        # never enters the paired identity.
        again = identity_module.NseArchiveResearchPairedSession(
            replay_session=paired.replay_session, admission_session=paired.admission_session
        )
        self.assertEqual(paired.paired_session_id, again.paired_session_id)


class NseArchiveResearchIdentityRegressionTests(unittest.TestCase):
    def test_research_identity_schema_version_is_named_v1(self) -> None:
        self.assertEqual(RESEARCH_IDENTITY_SCHEMA_VERSION, "nse-archive-research-identity/v1")

    def test_research_identity_paired_session_schema_version_is_named_v1(self) -> None:
        self.assertEqual(
            identity_module.RESEARCH_IDENTITY_PAIRED_SESSION_SCHEMA_VERSION,
            "nse-archive-research-paired-session/v1",
        )


if __name__ == "__main__":
    unittest.main()
