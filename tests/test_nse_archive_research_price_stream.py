from __future__ import annotations

import ast
import inspect
import unittest
from datetime import date
from unittest.mock import patch

from india_swing.evaluation import nse_archive_research_identity as identity_module
from india_swing.evaluation import nse_archive_research_price_stream as stream_module
from india_swing.evaluation.nse_archive_research_identity import (
    NseArchiveResearchIdentityTransitionKind,
)
from india_swing.evaluation.nse_archive_research_price_stream import (
    NseArchiveResearchPriceObservation,
    NseArchiveResearchPriceStreamError,
    NseArchiveResearchPriceStreamSession,
    iter_nse_archive_research_price_stream_sessions,
)
from tests.test_nse_archive_research_dataset import _baseline_dataset
from tests.test_nse_archive_research_identity import (
    EVIDENCE_PROFILE_UNRECONCILED,
    _FixedSessionsIterator,
    _claim,
    _record,
    _session,
    _unresolved_record,
)


def _paired_from_sessions(dataset, sessions):
    with patch.object(
        identity_module,
        "iter_verified_nse_archive_research_sessions",
        _FixedSessionsIterator(sessions),
    ):
        return list(
            identity_module.iter_nse_archive_research_paired_sessions(dataset, object())
        )


class NseArchiveResearchPriceStreamHappyPathTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()

    def _run(self, sessions: tuple) -> list:
        with patch.object(
            identity_module,
            "iter_verified_nse_archive_research_sessions",
            _FixedSessionsIterator(sessions),
        ):
            return list(
                iter_nse_archive_research_price_stream_sessions(self.dataset, object())
            )

    def test_observation_exposes_exact_nested_record_and_decision_with_stable_identity(
        self,
    ) -> None:
        record = _record(date(2024, 1, 1), symbol="INFY")
        session = _session(date(2024, 1, 1), (record,))
        [result] = self._run((session,))
        [observation] = result.observations
        self.assertIs(observation.replay_record, record)
        self.assertEqual(observation.replay_record.close, record.close)
        self.assertEqual(observation.replay_record.volume, record.volume)
        self.assertEqual(observation.replay_record.delivery_quantity, record.delivery_quantity)
        self.assertEqual(observation.identity_decision.record_id, record.record_id)
        again = NseArchiveResearchPriceObservation(
            replay_record=observation.replay_record,
            identity_decision=observation.identity_decision,
        )
        self.assertEqual(observation.observation_id, again.observation_id)
        observation.verify_content_identity()

    def test_observation_reports_fail_closed_posture(self) -> None:
        record = _record(date(2024, 1, 1), symbol="INFY")
        session = _session(date(2024, 1, 1), (record,))
        [result] = self._run((session,))
        [observation] = result.observations
        self.assertTrue(observation.collection_only)
        self.assertFalse(observation.actionable)
        self.assertFalse(observation.training_eligible)
        self.assertFalse(observation.feature_eligible)
        self.assertFalse(observation.label_eligible)
        self.assertFalse(observation.alert_eligible)
        self.assertFalse(observation.execution_eligible)
        self.assertFalse(observation.production_identity_resolution_complete)
        self.assertFalse(observation.corporate_action_adjustment_complete)
        # Fixed constant of the type, not stored state; never enters the
        # observation's own content identity.
        again = NseArchiveResearchPriceObservation(
            replay_record=observation.replay_record,
            identity_decision=observation.identity_decision,
        )
        self.assertEqual(observation.observation_id, again.observation_id)

    def test_price_stream_session_reports_fail_closed_posture(self) -> None:
        record = _record(date(2024, 1, 1), symbol="INFY")
        session = _session(date(2024, 1, 1), (record,))
        [result] = self._run((session,))
        self.assertTrue(result.collection_only)
        self.assertFalse(result.actionable)
        self.assertFalse(result.training_eligible)
        self.assertFalse(result.feature_eligible)
        self.assertFalse(result.label_eligible)
        self.assertFalse(result.alert_eligible)
        self.assertFalse(result.execution_eligible)
        self.assertFalse(result.production_identity_resolution_complete)
        self.assertFalse(result.corporate_action_adjustment_complete)

    def test_admitted_validated_unresolved_and_collision_rows_all_retained(self) -> None:
        validated_record = _record(date(2024, 1, 1), symbol="AAA", validated_isin="INE009A01021")
        unresolved_record = _unresolved_record(date(2024, 1, 1), symbol="CCC")
        collision_a = _record(date(2024, 1, 1), symbol="DDD", validated_isin="INE000A01001")
        collision_b = _record(date(2024, 1, 1), symbol="EEE", validated_isin="INE000A01001")
        session = _session(
            date(2024, 1, 1),
            (validated_record, unresolved_record, collision_a, collision_b),
        )
        [result] = self._run((session,))
        self.assertEqual(len(result.observations), 4)
        by_symbol = {o.symbol: o for o in result.observations}

        validated = by_symbol["AAA"]
        self.assertIs(
            validated.admission_status,
            identity_module.NseArchiveResearchIdentityAdmissionStatus.ADMITTED_VALIDATED,
        )
        self.assertIsNotNone(validated.research_identity_id)

        unresolved = by_symbol["CCC"]
        self.assertIs(
            unresolved.admission_status,
            identity_module.NseArchiveResearchIdentityAdmissionStatus.BLOCKED_UNRESOLVED,
        )
        self.assertIsNone(unresolved.research_identity_id)

        for symbol in ("DDD", "EEE"):
            collided = by_symbol[symbol]
            self.assertIs(
                collided.admission_status,
                identity_module.NseArchiveResearchIdentityAdmissionStatus.BLOCKED_SAME_SESSION_ISIN_COLLISION,
            )
            self.assertIsNone(collided.research_identity_id)

        # No authority/actionability flags anywhere in the stream session.
        self.assertTrue(result.collection_only)
        self.assertFalse(result.actionable)
        self.assertFalse(result.training_eligible)
        self.assertFalse(result.feature_eligible)
        self.assertFalse(result.label_eligible)
        self.assertFalse(result.alert_eligible)
        self.assertFalse(result.execution_eligible)
        self.assertFalse(result.production_identity_resolution_complete)
        self.assertFalse(result.corporate_action_adjustment_complete)

    def test_legacy_source_attested_rows_retained_with_correct_basis(self) -> None:
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
        claim = _claim(date(2024, 1, 1), symbol="20MICRONS", claimed_isin="INE467B01029")
        session = _session(
            date(2024, 1, 1),
            (legacy_record,),
            evidence_profile=EVIDENCE_PROFILE_UNRECONCILED,
            source_identity_claims=(claim,),
        )
        [result] = self._run((session,))
        [observation] = result.observations
        self.assertIs(
            observation.basis,
            identity_module.NseArchiveResearchIdentityBasis.LEGACY_SOURCE_ATTESTED_ISIN,
        )
        self.assertIs(
            observation.admission_status,
            identity_module.NseArchiveResearchIdentityAdmissionStatus.ADMITTED_SOURCE_ATTESTED,
        )
        self.assertEqual(observation.research_identity_id, identity_module.research_identity_id_for_isin("INE467B01029"))


class NseArchiveResearchPriceStreamBijectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()

    def test_missing_observation_rejected(self) -> None:
        record_a = _record(date(2024, 1, 1), symbol="AAA")
        record_b = _unresolved_record(date(2024, 1, 1), symbol="BBB")
        session = _session(date(2024, 1, 1), (record_a, record_b))
        [paired] = _paired_from_sessions(self.dataset, (session,))
        observations = tuple(
            NseArchiveResearchPriceObservation(replay_record=r, identity_decision=d)
            for r, d in zip(
                paired.replay_session.records, paired.admission_session.decisions, strict=True
            )
        )
        with self.assertRaises(NseArchiveResearchPriceStreamError):
            NseArchiveResearchPriceStreamSession(
                paired_session=paired,
                observations=observations[:1],
                transitions=paired.admission_session.transitions,
            )

    def test_duplicate_observation_rejected(self) -> None:
        record_a = _record(date(2024, 1, 1), symbol="AAA")
        record_b = _unresolved_record(date(2024, 1, 1), symbol="BBB")
        session = _session(date(2024, 1, 1), (record_a, record_b))
        [paired] = _paired_from_sessions(self.dataset, (session,))
        first_observation = NseArchiveResearchPriceObservation(
            replay_record=paired.replay_session.records[0],
            identity_decision=paired.admission_session.decisions[0],
        )
        with self.assertRaises(NseArchiveResearchPriceStreamError):
            NseArchiveResearchPriceStreamSession(
                paired_session=paired,
                observations=(first_observation, first_observation),
                transitions=paired.admission_session.transitions,
            )

    def test_reordered_observations_rejected(self) -> None:
        record_a = _record(date(2024, 1, 1), symbol="AAA")
        record_b = _unresolved_record(date(2024, 1, 1), symbol="BBB")
        session = _session(date(2024, 1, 1), (record_a, record_b))
        [paired] = _paired_from_sessions(self.dataset, (session,))
        observations = tuple(
            NseArchiveResearchPriceObservation(replay_record=r, identity_decision=d)
            for r, d in zip(
                paired.replay_session.records, paired.admission_session.decisions, strict=True
            )
        )
        reordered = (observations[1], observations[0])
        with self.assertRaises(NseArchiveResearchPriceStreamError):
            NseArchiveResearchPriceStreamSession(
                paired_session=paired,
                observations=reordered,
                transitions=paired.admission_session.transitions,
            )

    def test_orphaned_observation_from_a_different_session_rejected(self) -> None:
        record_a = _record(date(2024, 1, 1), symbol="AAA")
        session_a = _session(date(2024, 1, 1), (record_a,))
        record_b = _record(date(2024, 1, 2), symbol="BBB")
        session_b = _session(date(2024, 1, 2), (record_b,))
        [paired_a] = _paired_from_sessions(self.dataset, (session_a,))
        [paired_b] = _paired_from_sessions(self.dataset, (session_b,))
        orphan_observation = NseArchiveResearchPriceObservation(
            replay_record=paired_b.replay_session.records[0],
            identity_decision=paired_b.admission_session.decisions[0],
        )
        with self.assertRaises(NseArchiveResearchPriceStreamError):
            NseArchiveResearchPriceStreamSession(
                paired_session=paired_a,
                observations=(orphan_observation,),
                transitions=paired_a.admission_session.transitions,
            )

    def test_substituted_record_within_observation_rejected_at_construction(self) -> None:
        record_a = _record(date(2024, 1, 1), symbol="AAA")
        record_b = _record(date(2024, 1, 1), symbol="BBB", validated_isin="INE467B01029")
        session = _session(date(2024, 1, 1), (record_a, record_b))
        [paired] = _paired_from_sessions(self.dataset, (session,))
        with self.assertRaises(NseArchiveResearchPriceStreamError):
            NseArchiveResearchPriceObservation(
                replay_record=paired.replay_session.records[0],
                identity_decision=paired.admission_session.decisions[1],
            )

    def test_mismatched_transitions_rejected(self) -> None:
        first_record = _record(date(2024, 1, 1), symbol="AAA", validated_isin="INE009A01021")
        first_session = _session(date(2024, 1, 1), (first_record,))
        second_record = _record(date(2024, 1, 2), symbol="AAA", validated_isin="INE467B01029")
        second_session = _session(date(2024, 1, 2), (second_record,))
        [_, paired_second] = _paired_from_sessions(
            self.dataset, (first_session, second_session)
        )
        self.assertEqual(len(paired_second.admission_session.transitions), 1)
        observations = tuple(
            NseArchiveResearchPriceObservation(replay_record=r, identity_decision=d)
            for r, d in zip(
                paired_second.replay_session.records,
                paired_second.admission_session.decisions,
                strict=True,
            )
        )
        with self.assertRaises(NseArchiveResearchPriceStreamError):
            NseArchiveResearchPriceStreamSession(
                paired_session=paired_second,
                observations=observations,
                transitions=(),
            )

    def test_tampered_paired_session_rejected(self) -> None:
        record = _record(date(2024, 1, 1), symbol="AAA")
        session = _session(date(2024, 1, 1), (record,))
        [paired] = _paired_from_sessions(self.dataset, (session,))
        observations = tuple(
            NseArchiveResearchPriceObservation(replay_record=r, identity_decision=d)
            for r, d in zip(
                paired.replay_session.records, paired.admission_session.decisions, strict=True
            )
        )
        # Post-construction tamper on the already-verified nested replay
        # session -- the paired session's own re-verification must catch it.
        # Direct dataclass construction (not through the public iterator
        # boundary) surfaces the paired session's own error type; the public
        # iterator's own sanitized-wrapping is proven separately below.
        object.__setattr__(paired, "replay_session", None)
        with self.assertRaises(identity_module.NseArchiveResearchIdentityError):
            NseArchiveResearchPriceStreamSession(
                paired_session=paired,
                observations=observations,
                transitions=paired.admission_session.transitions,
            )


class NseArchiveResearchPriceStreamTransitionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()

    def _run(self, sessions: tuple) -> list:
        with patch.object(
            identity_module,
            "iter_verified_nse_archive_research_sessions",
            _FixedSessionsIterator(sessions),
        ):
            return list(
                iter_nse_archive_research_price_stream_sessions(self.dataset, object())
            )

    def test_listing_key_rebound_transition_retained_byte_for_byte(self) -> None:
        first_record = _record(date(2024, 1, 1), symbol="AAA", validated_isin="INE009A01021")
        first_session = _session(date(2024, 1, 1), (first_record,))
        second_record = _record(date(2024, 1, 2), symbol="AAA", validated_isin="INE467B01029")
        second_session = _session(date(2024, 1, 2), (second_record,))
        [first_result, second_result] = self._run((first_session, second_session))
        self.assertEqual(first_result.transitions, ())
        [transition] = second_result.transitions
        self.assertIs(transition.kind, NseArchiveResearchIdentityTransitionKind.LISTING_KEY_REBOUND)
        self.assertIs(
            transition, second_result.paired_session.admission_session.transitions[0]
        )
        self.assertEqual(second_result.observations[0].replay_record.close, second_record.close)
        self.assertNotEqual(
            first_result.observations[0].research_identity_id,
            second_result.observations[0].research_identity_id,
        )

    def test_identity_symbol_changed_transition_retained_byte_for_byte(self) -> None:
        isin = "INE009A01021"
        first_record = _record(date(2024, 1, 1), symbol="AAA", validated_isin=isin)
        first_session = _session(date(2024, 1, 1), (first_record,))
        second_record = _record(date(2024, 1, 2), symbol="ZZZ", validated_isin=isin)
        second_session = _session(date(2024, 1, 2), (second_record,))
        [first_result, second_result] = self._run((first_session, second_session))
        [transition] = second_result.transitions
        self.assertIs(
            transition.kind, NseArchiveResearchIdentityTransitionKind.IDENTITY_SYMBOL_CHANGED
        )
        self.assertEqual(
            first_result.observations[0].research_identity_id,
            second_result.observations[0].research_identity_id,
        )
        # Raw price history is untouched -- each session's own observation
        # still exposes only its own session's raw record.
        self.assertEqual(first_result.observations[0].replay_record.symbol, "AAA")
        self.assertEqual(second_result.observations[0].replay_record.symbol, "ZZZ")


class NseArchiveResearchPriceStreamStreamingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()

    def test_stream_is_lazy_and_one_upstream_session_produces_one_output_session(self) -> None:
        session1 = _session(date(2024, 1, 1), (_record(date(2024, 1, 1), symbol="INFY"),))
        session2 = _session(date(2024, 1, 2), (_record(date(2024, 1, 2), symbol="OTHERCO"),))
        seam = _FixedSessionsIterator((session1, session2))
        with patch.object(
            identity_module, "iter_verified_nse_archive_research_sessions", seam
        ):
            iterator = iter_nse_archive_research_price_stream_sessions(
                self.dataset, object()
            )
            first = next(iterator)
        self.assertEqual(seam.calls, 1)
        self.assertEqual(first.paired_session.replay_session.market_session, date(2024, 1, 1))

        with patch.object(
            identity_module,
            "iter_verified_nse_archive_research_sessions",
            _FixedSessionsIterator((session1, session2)),
        ):
            results = list(
                iter_nse_archive_research_price_stream_sessions(self.dataset, object())
            )
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].paired_session.replay_session.market_session, date(2024, 1, 1))
        self.assertEqual(results[1].paired_session.replay_session.market_session, date(2024, 1, 2))


class NseArchiveResearchPriceStreamErrorSanitizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()

    def test_invalid_dataset_type_rejected_with_sanitized_error(self) -> None:
        with self.assertRaises(NseArchiveResearchPriceStreamError) as context:
            list(iter_nse_archive_research_price_stream_sessions(object(), object()))
        exc = context.exception
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)

    def test_reader_exception_with_planted_secret_does_not_leak(self) -> None:
        secret = "SECRET-PLANTED-VALUE-MUST-NOT-LEAK/var/data/topsecret.json"

        def _boom(dataset, reader):
            raise ValueError(secret)
            yield  # pragma: no cover - makes this a generator function

        with patch.object(
            identity_module, "iter_verified_nse_archive_research_sessions", _boom
        ):
            with self.assertRaises(NseArchiveResearchPriceStreamError) as context:
                list(
                    iter_nse_archive_research_price_stream_sessions(
                        self.dataset, object()
                    )
                )
        exc = context.exception
        self.assertNotIn(secret, str(exc))
        self.assertNotIn(secret, repr(exc))
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)

    def test_tampered_paired_session_from_seam_is_rejected_with_sanitized_error(
        self,
    ) -> None:
        record = _record(date(2024, 1, 1), symbol="INFY")
        session = _session(date(2024, 1, 1), (record,))
        other_record = _record(date(2024, 1, 2), symbol="OTHERCO")
        object.__setattr__(session, "records", (other_record,))
        with patch.object(
            identity_module,
            "iter_verified_nse_archive_research_sessions",
            _FixedSessionsIterator((session,)),
        ):
            with self.assertRaises(NseArchiveResearchPriceStreamError) as context:
                list(
                    iter_nse_archive_research_price_stream_sessions(
                        self.dataset, object()
                    )
                )
        exc = context.exception
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)


class NseArchiveResearchPriceStreamStructuralTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source = inspect.getsource(stream_module)

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

    def test_no_store_persistence_discovery_feature_or_adjustment_capability(self) -> None:
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
            self.assertNotIn(token.lower(), lowered, msg=f"forbidden token found: {token}")

    def test_reader_capability_is_only_ever_passed_through_never_inspected(self) -> None:
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


class NseArchiveResearchPriceStreamRegressionTests(unittest.TestCase):
    def test_price_stream_schema_versions_are_named_v1(self) -> None:
        self.assertEqual(
            stream_module.PRICE_STREAM_OBSERVATION_SCHEMA_VERSION,
            "nse-archive-research-price-observation/v1",
        )
        self.assertEqual(
            stream_module.PRICE_STREAM_SESSION_SCHEMA_VERSION,
            "nse-archive-research-price-stream-session/v1",
        )


if __name__ == "__main__":
    unittest.main()
