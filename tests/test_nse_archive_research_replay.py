from __future__ import annotations

import ast
import inspect
import tempfile
import unittest
import zipfile
from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal, localcontext
from pathlib import Path
from unittest.mock import patch

from india_swing.evaluation import nse_archive_research_replay as replay_module
from india_swing.evaluation.nse_archive_research_dataset import (
    MINIMUM_FORWARD_LABEL_HORIZON_SESSIONS,
    ResearchArchiveSplitPolicy,
    ResearchSplitRole,
    build_nse_archive_research_dataset,
)
from india_swing.evaluation.nse_archive_research_replay import (
    NseArchiveResearchReplayError,
    NseArchiveResearchReplayRecord,
    NseArchiveResearchReplaySession,
    NseArchiveResearchSourceIdentityClaim,
    _build_replay_record,
    _build_replay_source_identity_claim,
    iter_verified_nse_archive_research_sessions,
)
from india_swing.market_data.nse_archive import (
    EVIDENCE_PROFILE_COMPLETE,
    EVIDENCE_PROFILE_PRICE_UDIFF,
    EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY,
    EVIDENCE_PROFILE_UNRECONCILED,
    IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE,
    NSE_HISTORICAL_ARCHIVE_EQ_DATASET,
    NSE_HISTORICAL_ARCHIVE_INDEX_DATASET,
    NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION,
    NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION_V1,
    NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION_V2,
    SOURCE_IDENTITY_CLAIM_KIND_LEGACY_BHAVCOPY_ISIN,
    SOURCE_IDENTITY_CLAIM_STATUS_SOURCE_CLAIMED_UNVERIFIED,
    _legacy_bhavcopy_stem,
)
from india_swing.market_data.nse_archive_range import VerifiedNseHistoricalArchiveRange
from india_swing.market_data.snapshot_store import (
    LocalMarketSnapshotStore,
    MarketSnapshotManifest,
    StoredMarketSnapshot,
)

from tests.test_nse_archive_research_dataset import (
    OBSERVED_AT,
    _baseline_dataset,
    _fake_sha256,
    _import_range,
    _stage_sessions,
)
from tests.test_nse_historical_archive import (
    _one_file_archive_bytes,
    _recompute_claim_id,
    _recompute_issue_id,
    _recompute_record_id,
    archive_bytes,
)


HORIZON = MINIMUM_FORWARD_LABEL_HORIZON_SESSIONS
PER_ROLE = HORIZON + 1


# ---------------------------------------------------------------------------
# Real-archive fixtures (end-to-end through the actual public trust boundary).
# ---------------------------------------------------------------------------


def _build_real_two_range_dataset(root: Path):
    """Build one real, two-binding dataset spanning 3 * (HORIZON + 1) days.

    The binding split point deliberately falls inside the validation block
    (not on a role boundary), proving role membership is derived purely from
    the split policy, never from which range binding contributed a session.
    """

    train_start = date(2024, 1, 1)
    train_end = train_start + timedelta(days=PER_ROLE - 1)
    validation_start = train_end + timedelta(days=1)
    validation_end = validation_start + timedelta(days=PER_ROLE - 1)
    test_start = validation_end + timedelta(days=1)
    test_end = test_start + timedelta(days=PER_ROLE - 1)

    split_point = train_start + timedelta(days=34)

    _stage_sessions(root, train_start, split_point)
    _stage_sessions(root, split_point + timedelta(days=1), test_end)

    store = LocalMarketSnapshotStore(root / "canonical")
    _, index_a = _import_range(root, store, train_start, split_point)
    _, index_b = _import_range(root, store, split_point + timedelta(days=1), test_end)

    policy = ResearchArchiveSplitPolicy(
        train_end=train_end,
        validation_start=validation_start,
        validation_end=validation_end,
        test_start=test_start,
        maximum_forward_label_horizon_sessions=HORIZON,
    )
    ids = (index_a.manifest.snapshot_id, index_b.manifest.snapshot_id)
    dataset = build_nse_archive_research_dataset(
        store, index_snapshot_ids=ids, split_policy=policy
    )
    return dataset, store


def _stage_sessions_with_override(
    root: Path, start: date, end: date, overrides: dict[date, bytes]
) -> None:
    staging = root / "staging"
    archives = root / "source-archives"
    day = start
    while day <= end:
        session_staging = staging / day.isoformat()
        session_archives = archives / day.isoformat()
        session_staging.mkdir(parents=True, exist_ok=True)
        session_archives.mkdir(parents=True, exist_ok=True)
        archive_path = session_archives / f"Reports-Archives-Multiple-{day:%d%m%Y}.zip"
        payload = overrides.get(day) or archive_bytes(session=day)
        archive_path.write_bytes(payload)
        with zipfile.ZipFile(archive_path) as archive:
            for name in archive.namelist():
                (session_staging / name).write_bytes(archive.read(name))
        day += timedelta(days=1)


class _RecordingTrapReader:
    """A reader exposing only `.get`; any other attribute access is a trap."""

    def __init__(self, store: LocalMarketSnapshotStore) -> None:
        self._store = store
        self.calls: list[tuple[str, str]] = []

    def get(self, dataset: str, snapshot_id: str) -> StoredMarketSnapshot:
        self.calls.append((dataset, snapshot_id))
        return self._store.get(dataset, snapshot_id)

    def __getattr__(self, name: str) -> None:
        raise AssertionError(f"unexpected reader capability accessed: {name}")


# ---------------------------------------------------------------------------
# Synthetic fixtures (fast, in-memory, for the replay module's own defensive
# re-verification and error-sanitization logic).
# ---------------------------------------------------------------------------


def _valid_record(session: date, *, symbol: str = "INFY", **overrides: object) -> dict:
    record = {
        "session": session,
        "listing_key": f"NSE:{symbol}",
        "symbol": symbol,
        "series": "EQ",
        "financial_instrument_id": 1,
        "security_master_financial_instrument_id": 1,
        "security_source_record_id": "SRC-1",
        "security_master_source_identifier": "INE000A01001",
        "udiff_source_identifier": "INE000A01001",
        "identity_status": "MATCHED_SAME_SESSION",
        "validated_isin": "INE000A01001",
        "normal_market_status": 1,
        "normal_market_eligible": True,
        "permitted_to_trade": 1,
        "delete_flag": "N",
        "previous_close": Decimal("100.00"),
        "open": Decimal("101.00"),
        "high": Decimal("105.00"),
        "low": Decimal("99.00"),
        "last": Decimal("102.00"),
        "close": Decimal("102.00"),
        "average_price": Decimal("101.50"),
        "volume": 1000,
        "turnover_lacs": Decimal("10.00"),
        "trade_count": 50,
        "delivery_quantity": 500,
        "delivery_percent": Decimal("50.00"),
        "surveillance_indicators": {},
    }
    record.update(overrides)
    _recompute_record_id(record)
    return record


def _valid_identity_issue(session: date, *, symbol: str = "TESTX", **overrides: object) -> dict:
    issue = {
        "session": session,
        "listing_key": f"NSE:{symbol}",
        "series": "EQ",
        "udiff_financial_instrument_id": None,
        "security_master_financial_instrument_id": None,
        "security_master_source_identifier": None,
        "udiff_source_identifier": None,
        "status": IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE,
    }
    issue.update(overrides)
    _recompute_issue_id(issue)
    return issue


def _valid_source_identity_claim(
    session: date, *, symbol: str = "INFY", **overrides: object
) -> dict:
    claim = {
        "session": session,
        "listing_key": f"NSE:{symbol}",
        "symbol": symbol,
        "series": "EQ",
        "claimed_isin": "INE000A01001",
        "source_kind": SOURCE_IDENTITY_CLAIM_KIND_LEGACY_BHAVCOPY_ISIN,
        "source_entry_name": f"{_legacy_bhavcopy_stem(session)}.csv",
        "source_entry_sha256": _fake_sha256("legacy-inner-csv"),
        "source_row_number": 2,
        "status": SOURCE_IDENTITY_CLAIM_STATUS_SOURCE_CLAIMED_UNVERIFIED,
    }
    claim.update(overrides)
    _recompute_claim_id(claim)
    return claim


def _full_stored_session(
    session: date,
    *,
    snapshot_id: str | None = None,
    records: tuple[dict, ...] | None = None,
    identity_issues: tuple[dict, ...] = (),
    identity_issue_count: int | None = None,
    evidence_profile: str = EVIDENCE_PROFILE_COMPLETE,
    missing_evidence: tuple[str, ...] = (),
    knowledge_time_status: str = "MANUAL_HISTORICAL_IMPORT_UNVERIFIED",
    collection_only: bool = True,
    actionable: bool = False,
    training_eligible: bool = False,
    record_count: int | None = None,
    source_identity_claims: tuple[dict, ...] = (),
) -> StoredMarketSnapshot:
    resolved_records = (_valid_record(session),) if records is None else records
    resolved_id = snapshot_id or _fake_sha256(f"synthetic-session-{session.isoformat()}")
    manifest = MarketSnapshotManifest(
        schema_version="test-schema/v1",
        codec_version="test-codec/v1",
        snapshot_id=resolved_id,
        dataset=NSE_HISTORICAL_ARCHIVE_EQ_DATASET,
        selection_key=session.isoformat(),
        provider="NSE_ARCHIVE",
        provider_version="test",
        observed_at=OBSERVED_AT,
        record_count=(
            record_count if record_count is not None else len(resolved_records)
        ),
        payload_filename="payload.json",
        payload_sha256=_fake_sha256(f"payload-{session.isoformat()}-{resolved_id}"),
    )
    payload: dict[str, object] = {
        "schema_version": NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION,
        "session": session,
        "exchange": "NSE",
        "series_scope": ("EQ",),
        "evidence_profile": evidence_profile,
        "missing_evidence": missing_evidence,
        "source_mode": "OFFICIAL_OUTER_ZIP",
        "source_container_sha256": _fake_sha256(f"container-{session.isoformat()}"),
        "source_entry_sha256": (
            (f"entry-{session.isoformat()}", _fake_sha256("entry")),
        ),
        "security_master_source_schema_version": "test/v1",
        "security_master_header_sha256": _fake_sha256("header"),
        "scope_exclusion_policy": "ALL_NON_EQ_ROWS_EXCLUDED",
        "reg1_row_count": 1,
        "identity_issue_count": (
            len(identity_issues) if identity_issue_count is None else identity_issue_count
        ),
        "identity_issues": identity_issues,
        "source_identity_claims": source_identity_claims,
        "collection_only": collection_only,
        "actionable": actionable,
        "training_eligible": training_eligible,
        "knowledge_time_status": knowledge_time_status,
        "records": resolved_records,
    }
    return StoredMarketSnapshot(
        path=Path("synthetic"),
        manifest=manifest,
        normalized_payload=payload,
        payload_bytes=b"",
    )


def _full_sessions_for_binding(binding) -> tuple[StoredMarketSnapshot, ...]:
    return tuple(
        _full_stored_session(session, snapshot_id=snapshot_id)
        for session, snapshot_id in zip(
            binding.accepted_sessions, binding.session_snapshot_ids, strict=True
        )
    )


def _verified_from_binding(
    binding, sessions: tuple[StoredMarketSnapshot, ...]
) -> VerifiedNseHistoricalArchiveRange:
    return VerifiedNseHistoricalArchiveRange(
        index_snapshot_id=binding.index_snapshot_id,
        range_start=binding.range_start,
        range_end=binding.range_end,
        session_snapshot_ids=binding.session_snapshot_ids,
        sessions=sessions,
        record_count=binding.record_count,
        identity_issue_count=binding.identity_issue_count,
        identity_quarantined_session_count=binding.identity_quarantined_session_count,
        incomplete_evidence_session_count=binding.incomplete_evidence_session_count,
        evidence_profile_counts=dict(binding.evidence_profile_counts),
    )


# ---------------------------------------------------------------------------
# Happy path / streaming / range-bounded behaviour (real archive fixtures).
# ---------------------------------------------------------------------------


class NseArchiveResearchReplayHappyPathTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._temporary = tempfile.TemporaryDirectory()
        root = Path(cls._temporary.name)
        cls.dataset, cls.store = _build_real_two_range_dataset(root)

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temporary.cleanup()

    def test_full_replay_matches_dataset_lineage_and_is_deterministic(self) -> None:
        reader = _RecordingTrapReader(self.store)
        sessions = list(iter_verified_nse_archive_research_sessions(self.dataset, reader))

        self.assertEqual(len(sessions), len(self.dataset.accepted_sessions))
        self.assertEqual(
            tuple(value.market_session for value in sessions),
            self.dataset.accepted_sessions,
        )
        self.assertEqual(
            tuple(value.session_snapshot_id for value in sessions),
            self.dataset.session_snapshot_ids,
        )

        for session in sessions:
            self.assertEqual(session.dataset_id, self.dataset.dataset_id)
            self.assertEqual(session.split_policy_id, self.dataset.split_policy_id)
            self.assertTrue(session.collection_only)
            self.assertFalse(session.actionable)
            self.assertFalse(session.training_eligible)
            self.assertFalse(session.feature_eligible)
            self.assertFalse(session.label_eligible)
            self.assertFalse(session.alert_eligible)
            self.assertFalse(session.execution_eligible)
            session.verify_content_identity()
            self.assertEqual(session.record_count, len(session.records))
            for record in session.records:
                self.assertIsInstance(record.previous_close, Decimal)
                self.assertNotIsInstance(record.previous_close, float)
                self.assertIsInstance(record.volume, int)
                self.assertTrue(record.identity_matched)

        roles_seen = [value.partition_role for value in sessions]
        self.assertEqual(roles_seen[:PER_ROLE], [ResearchSplitRole.TRAIN] * PER_ROLE)
        self.assertEqual(
            roles_seen[PER_ROLE : 2 * PER_ROLE], [ResearchSplitRole.VALIDATION] * PER_ROLE
        )
        self.assertEqual(
            roles_seen[2 * PER_ROLE :], [ResearchSplitRole.UNTOUCHED_TEST] * PER_ROLE
        )

        binding_a, binding_b = self.dataset.range_bindings
        expected_bindings = (
            [binding_a] * len(binding_a.accepted_sessions)
            + [binding_b] * len(binding_b.accepted_sessions)
        )
        for session, expected_binding in zip(sessions, expected_bindings, strict=True):
            self.assertEqual(session.range_binding_id, expected_binding.binding_id)
            self.assertEqual(session.index_snapshot_id, expected_binding.index_snapshot_id)

        reader_2 = _RecordingTrapReader(self.store)
        sessions_2 = list(
            iter_verified_nse_archive_research_sessions(self.dataset, reader_2)
        )
        self.assertEqual(
            [value.replay_session_id for value in sessions],
            [value.replay_session_id for value in sessions_2],
        )

    def test_reader_only_invoked_via_get_once_per_index_in_stored_order(self) -> None:
        reader = _RecordingTrapReader(self.store)
        list(iter_verified_nse_archive_research_sessions(self.dataset, reader))

        index_calls = [
            call for call in reader.calls if call[0] == NSE_HISTORICAL_ARCHIVE_INDEX_DATASET
        ]
        self.assertEqual(
            index_calls,
            [
                (NSE_HISTORICAL_ARCHIVE_INDEX_DATASET, binding.index_snapshot_id)
                for binding in self.dataset.range_bindings
            ],
        )

    def test_second_range_is_not_loaded_before_first_range_is_fully_consumed(self) -> None:
        reader = _RecordingTrapReader(self.store)
        iterator = iter_verified_nse_archive_research_sessions(self.dataset, reader)
        binding_a, binding_b = self.dataset.range_bindings
        first_range_size = len(binding_a.accepted_sessions)

        for _ in range(first_range_size):
            next(iterator)

        index_calls = [
            call for call in reader.calls if call[0] == NSE_HISTORICAL_ARCHIVE_INDEX_DATASET
        ]
        self.assertEqual(
            index_calls, [(NSE_HISTORICAL_ARCHIVE_INDEX_DATASET, binding_a.index_snapshot_id)]
        )
        eq_calls = [
            call for call in reader.calls if call[0] == NSE_HISTORICAL_ARCHIVE_EQ_DATASET
        ]
        self.assertEqual(len(eq_calls), first_range_size)

        next(iterator)

        index_calls_after = [
            call for call in reader.calls if call[0] == NSE_HISTORICAL_ARCHIVE_INDEX_DATASET
        ]
        self.assertEqual(
            index_calls_after,
            [
                (NSE_HISTORICAL_ARCHIVE_INDEX_DATASET, binding_a.index_snapshot_id),
                (NSE_HISTORICAL_ARCHIVE_INDEX_DATASET, binding_b.index_snapshot_id),
            ],
        )

    def test_stopping_iteration_early_never_loads_a_later_range(self) -> None:
        reader = _RecordingTrapReader(self.store)
        iterator = iter_verified_nse_archive_research_sessions(self.dataset, reader)

        next(iterator)

        index_calls = [
            call for call in reader.calls if call[0] == NSE_HISTORICAL_ARCHIVE_INDEX_DATASET
        ]
        self.assertEqual(len(index_calls), 1)
        self.assertEqual(
            index_calls[0][1], self.dataset.range_bindings[0].index_snapshot_id
        )


# ---------------------------------------------------------------------------
# Retained-not-filtered data (real archive fixture with one unreconciled day).
# ---------------------------------------------------------------------------


class NseArchiveResearchReplayRetainedDataTests(unittest.TestCase):
    def test_unresolved_identity_and_incomplete_evidence_are_retained_not_filtered(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            train_start = date(2024, 1, 1)
            train_end = train_start + timedelta(days=PER_ROLE - 1)
            validation_start = train_end + timedelta(days=1)
            validation_end = validation_start + timedelta(days=PER_ROLE - 1)
            test_start = validation_end + timedelta(days=1)
            test_end = test_start + timedelta(days=PER_ROLE - 1)
            unreconciled_day = validation_start

            _stage_sessions_with_override(
                root,
                train_start,
                test_end,
                {unreconciled_day: _one_file_archive_bytes(session=unreconciled_day)},
            )
            store = LocalMarketSnapshotStore(root / "canonical")
            _, index = _import_range(root, store, train_start, test_end)
            policy = ResearchArchiveSplitPolicy(
                train_end=train_end,
                validation_start=validation_start,
                validation_end=validation_end,
                test_start=test_start,
                maximum_forward_label_horizon_sessions=HORIZON,
            )
            dataset = build_nse_archive_research_dataset(
                store,
                index_snapshot_ids=(index.manifest.snapshot_id,),
                split_policy=policy,
            )
            self.assertGreater(dataset.incomplete_evidence_session_count, 0)

            reader = _RecordingTrapReader(store)
            sessions = list(iter_verified_nse_archive_research_sessions(dataset, reader))
            unresolved_session = next(
                value for value in sessions if value.market_session == unreconciled_day
            )
            self.assertEqual(unresolved_session.evidence_profile, EVIDENCE_PROFILE_UNRECONCILED)
            self.assertGreater(len(unresolved_session.records), 0)
            for record in unresolved_session.records:
                self.assertEqual(
                    record.identity_status,
                    IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE,
                )
                self.assertIsNone(record.validated_isin)
                self.assertIsNone(record.security_master_financial_instrument_id)
                self.assertFalse(record.identity_matched)
                self.assertFalse(record.normal_market_eligibility_verified)


class NseArchiveResearchReplayRecordProjectionTests(unittest.TestCase):
    def test_retains_zero_volume_delete_flag_and_non_normal_market_status(self) -> None:
        record = _valid_record(
            date(2024, 1, 2),
            volume=0,
            trade_count=0,
            delete_flag="Y",
            normal_market_status=0,
            normal_market_eligible=False,
        )
        built = _build_replay_record(record)
        self.assertEqual(built.volume, 0)
        self.assertEqual(built.trade_count, 0)
        self.assertEqual(built.delete_flag, "Y")
        self.assertEqual(built.normal_market_status, 0)
        self.assertFalse(built.normal_market_eligible)
        self.assertTrue(built.normal_market_eligibility_verified)

    def test_retains_unresolved_identity_with_missing_security_evidence(self) -> None:
        record = _valid_record(
            date(2024, 1, 2),
            identity_status="SECURITY_MASTER_EVIDENCE_UNAVAILABLE",
            validated_isin=None,
            security_master_financial_instrument_id=None,
            security_master_source_identifier=None,
            normal_market_status=None,
            normal_market_eligible=None,
            permitted_to_trade=None,
            delete_flag=None,
        )
        built = _build_replay_record(record)
        self.assertFalse(built.identity_matched)
        self.assertFalse(built.normal_market_eligibility_verified)
        self.assertIsNone(built.normal_market_eligible)
        self.assertIsNone(built.delete_flag)


# ---------------------------------------------------------------------------
# Adversarial mismatch injection (synthetic fixtures, patched loader).
# ---------------------------------------------------------------------------


class NseArchiveResearchReplayAdversarialTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()
        self.binding = self.dataset.range_bindings[0]
        self.sessions = _full_sessions_for_binding(self.binding)
        self.verified = _verified_from_binding(self.binding, self.sessions)

    def _replay_with_patched_loader(self, verified_or_effect) -> list:
        with patch.object(
            replay_module, "load_verified_nse_historical_archive_range"
        ) as mock_loader:
            if isinstance(verified_or_effect, VerifiedNseHistoricalArchiveRange):
                mock_loader.return_value = verified_or_effect
            else:
                mock_loader.side_effect = verified_or_effect
            reader = object()
            return list(iter_verified_nse_archive_research_sessions(self.dataset, reader))

    def _mutated_verified_with_first_session_payload(
        self, **payload_overrides: object
    ) -> VerifiedNseHistoricalArchiveRange:
        original = self.sessions[0]
        bad_payload = {**original.normalized_payload, **payload_overrides}
        bad_session = replace(original, normalized_payload=bad_payload)
        mutated_sessions = (bad_session,) + self.sessions[1:]
        return replace(self.verified, sessions=mutated_sessions)

    def test_baseline_fixture_is_internally_consistent(self) -> None:
        sessions = self._replay_with_patched_loader(self.verified)
        self.assertEqual(len(sessions), len(self.binding.accepted_sessions))

    def test_index_snapshot_id_mismatch_fails_closed(self) -> None:
        mutated = replace(self.verified, index_snapshot_id=_fake_sha256("wrong-index"))
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_range_start_mismatch_fails_closed(self) -> None:
        mutated = replace(
            self.verified, range_start=self.binding.range_start + timedelta(days=1)
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_range_end_mismatch_fails_closed(self) -> None:
        mutated = replace(
            self.verified, range_end=self.binding.range_end - timedelta(days=1)
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_session_snapshot_ids_reordered_fails_closed(self) -> None:
        ids = self.verified.session_snapshot_ids
        swapped = (ids[1], ids[0]) + ids[2:]
        mutated = replace(self.verified, session_snapshot_ids=swapped)
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_record_count_mismatch_fails_closed(self) -> None:
        mutated = replace(self.verified, record_count=self.verified.record_count + 1)
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_identity_issue_count_mismatch_fails_closed(self) -> None:
        mutated = replace(
            self.verified, identity_issue_count=self.verified.identity_issue_count + 1
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_identity_quarantined_session_count_mismatch_fails_closed(self) -> None:
        mutated = replace(
            self.verified,
            identity_quarantined_session_count=(
                self.verified.identity_quarantined_session_count + 1
            ),
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_incomplete_evidence_session_count_mismatch_fails_closed(self) -> None:
        mutated = replace(
            self.verified,
            incomplete_evidence_session_count=(
                self.verified.incomplete_evidence_session_count + 1
            ),
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_evidence_profile_counts_mismatch_fails_closed(self) -> None:
        wrong_counts = dict(self.verified.evidence_profile_counts)
        wrong_counts[EVIDENCE_PROFILE_COMPLETE] -= 1
        wrong_counts[EVIDENCE_PROFILE_UNRECONCILED] += 1
        mutated = replace(self.verified, evidence_profile_counts=wrong_counts)
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_session_date_mismatch_fails_closed(self) -> None:
        original = self.sessions[0]
        bad_payload = {
            **original.normalized_payload,
            "session": original.normalized_payload["session"] + timedelta(days=100),
        }
        bad_session = replace(original, normalized_payload=bad_payload)
        mutated_sessions = (bad_session,) + self.sessions[1:]
        mutated = replace(self.verified, sessions=mutated_sessions)
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_source_snapshot_manifest_mismatch_fails_closed(self) -> None:
        original = self.sessions[0]
        bad_manifest = replace(
            original.manifest, snapshot_id=_fake_sha256("wrong-manifest-id")
        )
        bad_session = replace(original, manifest=bad_manifest)
        mutated_sessions = (bad_session,) + self.sessions[1:]
        mutated = replace(self.verified, sessions=mutated_sessions)
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_record_missing_key_fails_closed(self) -> None:
        original = self.sessions[0]
        bad_record = dict(original.normalized_payload["records"][0])
        del bad_record["record_id"]
        bad_payload = {**original.normalized_payload, "records": (bad_record,)}
        bad_session = replace(original, normalized_payload=bad_payload)
        mutated_sessions = (bad_session,) + self.sessions[1:]
        mutated = replace(self.verified, sessions=mutated_sessions)
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_record_id_not_a_hash_fails_closed(self) -> None:
        original = self.sessions[0]
        bad_record = dict(original.normalized_payload["records"][0])
        bad_record["record_id"] = "not-a-sha256-hash"
        bad_payload = {**original.normalized_payload, "records": (bad_record,)}
        bad_session = replace(original, normalized_payload=bad_payload)
        mutated_sessions = (bad_session,) + self.sessions[1:]
        mutated = replace(self.verified, sessions=mutated_sessions)
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_record_price_type_coercion_to_float_fails_closed(self) -> None:
        original = self.sessions[0]
        bad_record = dict(original.normalized_payload["records"][0])
        bad_record["close"] = float(bad_record["close"])
        _recompute_record_id(bad_record)
        bad_payload = {**original.normalized_payload, "records": (bad_record,)}
        bad_session = replace(original, normalized_payload=bad_payload)
        mutated_sessions = (bad_session,) + self.sessions[1:]
        mutated = replace(self.verified, sessions=mutated_sessions)
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_surveillance_indicators_shape_mismatch_fails_closed(self) -> None:
        original = self.sessions[0]
        bad_record = dict(original.normalized_payload["records"][0])
        bad_record["surveillance_indicators"] = ["not", "a", "mapping"]
        _recompute_record_id(bad_record)
        bad_payload = {**original.normalized_payload, "records": (bad_record,)}
        bad_session = replace(original, normalized_payload=bad_payload)
        mutated_sessions = (bad_session,) + self.sessions[1:]
        mutated = replace(self.verified, sessions=mutated_sessions)
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_session_safety_posture_mismatch_fails_closed(self) -> None:
        original = self.sessions[0]
        bad_payload = {**original.normalized_payload, "actionable": True}
        bad_session = replace(original, normalized_payload=bad_payload)
        mutated_sessions = (bad_session,) + self.sessions[1:]
        mutated = replace(self.verified, sessions=mutated_sessions)
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_partition_coverage_corruption_fails_closed(self) -> None:
        train_partition = self.dataset.partitions[0]
        object.__setattr__(
            train_partition, "sessions", train_partition.sessions[1:]
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(self.verified)

    def test_duplicate_session_snapshot_id_fails_closed(self) -> None:
        ids = self.verified.session_snapshot_ids
        duplicated = (ids[0],) + ids[1:-1] + (ids[0],)
        mutated = replace(self.verified, session_snapshot_ids=duplicated)
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    # -----------------------------------------------------------------
    # Codex revision-1 rejection regressions: identity-issue accounting.
    # -----------------------------------------------------------------

    def test_identity_issue_count_without_matching_issues_fails_closed(self) -> None:
        mutated = self._mutated_verified_with_first_session_payload(
            identity_issue_count=1, identity_issues=()
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_identity_issues_extra_row_beyond_count_fails_closed(self) -> None:
        session = self.binding.accepted_sessions[0]
        extra_issue = _valid_identity_issue(session)
        mutated = self._mutated_verified_with_first_session_payload(
            identity_issue_count=0, identity_issues=(extra_issue,)
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_identity_issues_non_tuple_fails_closed(self) -> None:
        session = self.binding.accepted_sessions[0]
        issue = _valid_identity_issue(session)
        mutated = self._mutated_verified_with_first_session_payload(
            identity_issue_count=1, identity_issues=[issue]
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_identity_issue_wrong_session_fails_closed(self) -> None:
        session = self.binding.accepted_sessions[0]
        wrong_session_issue = _valid_identity_issue(session + timedelta(days=1))
        mutated = self._mutated_verified_with_first_session_payload(
            identity_issue_count=1, identity_issues=(wrong_session_issue,)
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_identity_issue_valid_shaped_stale_issue_id_fails_closed(self) -> None:
        session = self.binding.accepted_sessions[0]
        issue = _valid_identity_issue(session)
        stale_issue = {**issue, "issue_id": _fake_sha256("stale-issue-id")}
        mutated = self._mutated_verified_with_first_session_payload(
            identity_issue_count=1, identity_issues=(stale_issue,)
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_identity_issue_malformed_key_set_fails_closed(self) -> None:
        session = self.binding.accepted_sessions[0]
        issue = dict(_valid_identity_issue(session))
        del issue["status"]
        mutated = self._mutated_verified_with_first_session_payload(
            identity_issue_count=1, identity_issues=(issue,)
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_identity_issue_malformed_field_type_fails_closed(self) -> None:
        session = self.binding.accepted_sessions[0]
        issue = _valid_identity_issue(session, udiff_financial_instrument_id="not-an-int")
        mutated = self._mutated_verified_with_first_session_payload(
            identity_issue_count=1, identity_issues=(issue,)
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    # -----------------------------------------------------------------
    # Codex revision-1 rejection regressions: cross-session record binding.
    # -----------------------------------------------------------------

    def test_record_session_mismatch_with_recomputed_id_fails_closed(self) -> None:
        original = self.sessions[0]
        bad_record = dict(original.normalized_payload["records"][0])
        bad_record["session"] = bad_record["session"] + timedelta(days=1)
        _recompute_record_id(bad_record)
        mutated = self._mutated_verified_with_first_session_payload(
            records=(bad_record,)
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    # -----------------------------------------------------------------
    # Codex revision-1 rejection regressions: stale record-id acceptance.
    # -----------------------------------------------------------------

    def test_record_content_changed_without_recomputed_id_fails_closed(self) -> None:
        original = self.sessions[0]
        bad_record = dict(original.normalized_payload["records"][0])
        bad_record["close"] = bad_record["close"] + Decimal("1.00")
        mutated = self._mutated_verified_with_first_session_payload(
            records=(bad_record,)
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_claimed_point_in_time_knowledge_status_fails_closed(self) -> None:
        mutated = self._mutated_verified_with_first_session_payload(
            knowledge_time_status="POINT_IN_TIME_VERIFIED"
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    # -----------------------------------------------------------------
    # Legacy source-identity-claim sidecar binding, through the full
    # patched-loader pipeline.
    # -----------------------------------------------------------------

    def test_legacy_shaped_claim_binds_through_full_replay_pipeline(self) -> None:
        session_date = self.binding.accepted_sessions[0]
        claim = _valid_source_identity_claim(session_date, symbol="INFY")
        mutated = self._mutated_verified_with_first_session_payload(
            source_identity_claims=(claim,),
            evidence_profile=EVIDENCE_PROFILE_UNRECONCILED,
        )
        sessions = self._replay_with_patched_loader(mutated)
        self.assertEqual(len(sessions[0].source_identity_claims), 1)
        self.assertEqual(
            sessions[0].source_identity_claims[0].claimed_isin, claim["claimed_isin"]
        )
        self.assertEqual(sessions[0].source_identity_claims[0].claim_id, claim["claim_id"])

    def test_claim_wrong_session_fails_closed_through_full_pipeline(self) -> None:
        session_date = self.binding.accepted_sessions[0]
        wrong_session_claim = _valid_source_identity_claim(
            session_date + timedelta(days=1), symbol="INFY"
        )
        mutated = self._mutated_verified_with_first_session_payload(
            source_identity_claims=(wrong_session_claim,),
            evidence_profile=EVIDENCE_PROFILE_UNRECONCILED,
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_claim_symbol_mismatch_fails_closed_through_full_pipeline(self) -> None:
        session_date = self.binding.accepted_sessions[0]
        wrong_symbol_claim = _valid_source_identity_claim(session_date, symbol="OTHERCO")
        mutated = self._mutated_verified_with_first_session_payload(
            source_identity_claims=(wrong_symbol_claim,),
            evidence_profile=EVIDENCE_PROFILE_UNRECONCILED,
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)

    def test_claim_rejected_for_non_unreconciled_evidence_profile_through_full_pipeline(
        self,
    ) -> None:
        session_date = self.binding.accepted_sessions[0]
        claim = _valid_source_identity_claim(session_date, symbol="INFY")
        mutated = self._mutated_verified_with_first_session_payload(
            source_identity_claims=(claim,)
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            self._replay_with_patched_loader(mutated)


# ---------------------------------------------------------------------------
# Direct construction: each record/session independently proves its own
# content identity, cross-session binding, and derived-flag correctness --
# regardless of whether it was built through the replay pipeline.
# ---------------------------------------------------------------------------


def _direct_session_kwargs(*, market_session: date, records: tuple) -> dict:
    return dict(
        dataset_id=_fake_sha256("direct-dataset"),
        split_policy_id=_fake_sha256("direct-split-policy"),
        partition_id=_fake_sha256("direct-partition"),
        partition_role=ResearchSplitRole.TRAIN,
        index_snapshot_id=_fake_sha256("direct-index"),
        range_binding_id=_fake_sha256("direct-binding"),
        market_session=market_session,
        session_snapshot_id=_fake_sha256("direct-session-snapshot"),
        observed_at=OBSERVED_AT,
        evidence_profile=EVIDENCE_PROFILE_COMPLETE,
        missing_evidence=(),
        knowledge_time_status="MANUAL_HISTORICAL_IMPORT_UNVERIFIED",
        records=records,
        record_count=len(records),
        identity_issue_count=0,
        source_identity_claims=(),
    )


class NseArchiveResearchReplayDirectConstructionTests(unittest.TestCase):
    def test_valid_record_recomputed_id_matches_source_and_is_deterministic(self) -> None:
        record = _build_replay_record(_valid_record(date(2024, 1, 2)))
        record.verify_content_identity()
        with localcontext() as context:
            context.prec = 5
            record.verify_content_identity()

    def test_direct_record_construction_rejects_stale_record_id(self) -> None:
        stale_record_dict = _valid_record(date(2024, 1, 2))
        stale_record_dict = {**stale_record_dict, "close": Decimal("103.00")}
        with self.assertRaises(NseArchiveResearchReplayError):
            _build_replay_record(stale_record_dict)

    def test_direct_record_construction_rejects_correctly_rehashed_non_eq_series(
        self,
    ) -> None:
        non_eq_record = _valid_record(date(2024, 1, 2), series="BE")
        with self.assertRaises(NseArchiveResearchReplayError) as context:
            _build_replay_record(non_eq_record)
        exc = context.exception
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)

    def test_direct_record_dataclass_construction_rejects_altered_content(self) -> None:
        valid_record = _build_replay_record(_valid_record(date(2024, 1, 2)))
        with self.assertRaises(NseArchiveResearchReplayError):
            replace(valid_record, close=Decimal("103.00"))

    def test_direct_record_dataclass_construction_rejects_forged_identity_matched(
        self,
    ) -> None:
        valid_record = _build_replay_record(_valid_record(date(2024, 1, 2)))
        self.assertTrue(valid_record.identity_matched)
        with self.assertRaises(NseArchiveResearchReplayError):
            replace(valid_record, identity_matched=False)

    def test_direct_record_dataclass_construction_rejects_forged_eligibility_verified(
        self,
    ) -> None:
        valid_record = _build_replay_record(_valid_record(date(2024, 1, 2)))
        self.assertTrue(valid_record.normal_market_eligibility_verified)
        with self.assertRaises(NseArchiveResearchReplayError):
            replace(valid_record, normal_market_eligibility_verified=False)

    def test_direct_session_construction_rejects_cross_session_record(self) -> None:
        record = _build_replay_record(_valid_record(date(2024, 1, 2)))
        kwargs = _direct_session_kwargs(
            market_session=date(2024, 1, 1), records=(record,)
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            NseArchiveResearchReplaySession(**kwargs)

    def test_direct_session_rejects_claimed_point_in_time_knowledge(self) -> None:
        record = _build_replay_record(_valid_record(date(2024, 1, 1)))
        kwargs = _direct_session_kwargs(
            market_session=date(2024, 1, 1), records=(record,)
        )
        kwargs["knowledge_time_status"] = "POINT_IN_TIME_VERIFIED"
        with self.assertRaises(NseArchiveResearchReplayError):
            NseArchiveResearchReplaySession(**kwargs)

    def test_session_reverification_rejects_cross_session_record_after_tampering(
        self,
    ) -> None:
        record = _build_replay_record(_valid_record(date(2024, 1, 1)))
        kwargs = _direct_session_kwargs(
            market_session=date(2024, 1, 1), records=(record,)
        )
        session = NseArchiveResearchReplaySession(**kwargs)
        session.verify_content_identity()

        tampered_record = _build_replay_record(_valid_record(date(2024, 1, 2)))
        object.__setattr__(session, "records", (tampered_record,))
        with self.assertRaises(NseArchiveResearchReplayError):
            session.verify_content_identity()

    def test_valid_session_verify_content_identity_is_deterministic(self) -> None:
        record = _build_replay_record(_valid_record(date(2024, 1, 1)))
        kwargs = _direct_session_kwargs(
            market_session=date(2024, 1, 1), records=(record,)
        )
        session = NseArchiveResearchReplaySession(**kwargs)
        session.verify_content_identity()
        with localcontext() as context:
            context.prec = 5
            session.verify_content_identity()


# ---------------------------------------------------------------------------
# Source-identity-claim direct construction: an official legacy Bhavcopy
# ISIN claim independently proves its own content identity, regardless of
# whether it was built through the replay pipeline.
# ---------------------------------------------------------------------------


class NseArchiveResearchReplaySourceIdentityClaimDirectConstructionTests(unittest.TestCase):
    def test_valid_claim_recomputed_id_matches_source_and_is_deterministic(self) -> None:
        claim = _build_replay_source_identity_claim(
            _valid_source_identity_claim(date(2024, 1, 2))
        )
        claim.verify_content_identity()
        with localcontext() as context:
            context.prec = 5
            claim.verify_content_identity()

    def test_direct_claim_construction_rejects_stale_claim_id(self) -> None:
        stale = _valid_source_identity_claim(date(2024, 1, 2))
        stale = {**stale, "claimed_isin": "INE999Z99999"}
        with self.assertRaises(NseArchiveResearchReplayError):
            _build_replay_source_identity_claim(stale)

    def test_direct_claim_dataclass_construction_rejects_altered_content(self) -> None:
        valid_claim = _build_replay_source_identity_claim(
            _valid_source_identity_claim(date(2024, 1, 2))
        )
        with self.assertRaises(NseArchiveResearchReplayError):
            replace(valid_claim, claimed_isin="INE999Z99999")

    def test_direct_claim_construction_rejects_malformed_fields(self) -> None:
        base = _valid_source_identity_claim(date(2024, 1, 2))
        cases = {
            "bad_source_kind": {**base, "source_kind": "OTHER"},
            "bad_status": {**base, "status": "SOURCE_VALIDATED"},
            "row_number_bool": {**base, "source_row_number": True},
            "row_number_zero": {**base, "source_row_number": 0},
            "bad_listing_key": {**base, "listing_key": "NSE:WRONG"},
            "empty_isin": {**base, "claimed_isin": ""},
            "malformed_isin": {**base, "claimed_isin": "not-an-isin"},
            "lowercase_symbol": {
                **base, "symbol": "infy", "listing_key": "NSE:infy",
            },
            "noncanonical_symbol": {
                **base, "symbol": "IN FY", "listing_key": "NSE:IN FY",
            },
            "non_eq_series": {**base, "series": "BE"},
            "wrong_inner_entry_name": {**base, "source_entry_name": "wrong.csv"},
        }
        for label, mutated in cases.items():
            with self.subTest(label):
                mutated = dict(mutated)
                _recompute_claim_id(mutated)
                with self.assertRaises(NseArchiveResearchReplayError) as context:
                    _build_replay_source_identity_claim(mutated)
                exc = context.exception
                self.assertIsNone(exc.__cause__)
                self.assertIsNone(exc.__context__)


# ---------------------------------------------------------------------------
# Source-identity-claim session binding: a replay session must independently
# verify every claim and bind it 1:1 (ordered) to its records.
# ---------------------------------------------------------------------------


class NseArchiveResearchReplaySourceIdentityClaimSessionTests(unittest.TestCase):
    def test_session_validate_calls_every_claim_verification(self) -> None:
        record = _build_replay_record(_valid_record(date(2024, 1, 1), symbol="20MICRONS"))
        claim = _build_replay_source_identity_claim(
            _valid_source_identity_claim(date(2024, 1, 1), symbol="20MICRONS")
        )
        kwargs = _direct_session_kwargs(
            market_session=date(2024, 1, 1), records=(record,)
        )
        kwargs["evidence_profile"] = EVIDENCE_PROFILE_UNRECONCILED
        kwargs["source_identity_claims"] = (claim,)
        session = NseArchiveResearchReplaySession(**kwargs)
        session.verify_content_identity()

        tampered_claim = _build_replay_source_identity_claim(
            _valid_source_identity_claim(
                date(2024, 1, 1), symbol="20MICRONS", claimed_isin="INE999Z99999"
            )
        )
        object.__setattr__(session, "source_identity_claims", (tampered_claim,))
        with self.assertRaises(NseArchiveResearchReplayError):
            session.verify_content_identity()

    def test_session_validate_rejects_claim_bound_to_different_symbol(self) -> None:
        record = _build_replay_record(_valid_record(date(2024, 1, 1), symbol="20MICRONS"))
        wrong_claim = _build_replay_source_identity_claim(
            _valid_source_identity_claim(date(2024, 1, 1), symbol="OTHERCO")
        )
        kwargs = _direct_session_kwargs(
            market_session=date(2024, 1, 1), records=(record,)
        )
        kwargs["evidence_profile"] = EVIDENCE_PROFILE_UNRECONCILED
        kwargs["source_identity_claims"] = (wrong_claim,)
        with self.assertRaises(NseArchiveResearchReplayError):
            NseArchiveResearchReplaySession(**kwargs)

    def test_session_validate_rejects_claim_wrong_session(self) -> None:
        record = _build_replay_record(_valid_record(date(2024, 1, 1), symbol="20MICRONS"))
        wrong_session_claim = _build_replay_source_identity_claim(
            _valid_source_identity_claim(date(2024, 1, 2), symbol="20MICRONS")
        )
        kwargs = _direct_session_kwargs(
            market_session=date(2024, 1, 1), records=(record,)
        )
        kwargs["evidence_profile"] = EVIDENCE_PROFILE_UNRECONCILED
        kwargs["source_identity_claims"] = (wrong_session_claim,)
        with self.assertRaises(NseArchiveResearchReplayError):
            NseArchiveResearchReplaySession(**kwargs)

    def test_session_validate_rejects_claim_count_mismatch(self) -> None:
        record = _build_replay_record(_valid_record(date(2024, 1, 1), symbol="20MICRONS"))
        claim = _build_replay_source_identity_claim(
            _valid_source_identity_claim(date(2024, 1, 1), symbol="20MICRONS")
        )
        extra_claim = _build_replay_source_identity_claim(
            _valid_source_identity_claim(
                date(2024, 1, 1), symbol="20MICRONS", source_row_number=3
            )
        )
        kwargs = _direct_session_kwargs(
            market_session=date(2024, 1, 1), records=(record,)
        )
        kwargs["evidence_profile"] = EVIDENCE_PROFILE_UNRECONCILED
        kwargs["source_identity_claims"] = (claim, extra_claim)
        with self.assertRaises(NseArchiveResearchReplayError):
            NseArchiveResearchReplaySession(**kwargs)

    def test_session_validate_rejects_claims_for_non_unreconciled_evidence_profiles(
        self,
    ) -> None:
        record = _build_replay_record(_valid_record(date(2024, 1, 1), symbol="20MICRONS"))
        claim = _build_replay_source_identity_claim(
            _valid_source_identity_claim(date(2024, 1, 1), symbol="20MICRONS")
        )
        for profile in (
            EVIDENCE_PROFILE_COMPLETE,
            EVIDENCE_PROFILE_PRICE_UDIFF,
            EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY,
        ):
            with self.subTest(profile):
                kwargs = _direct_session_kwargs(
                    market_session=date(2024, 1, 1), records=(record,)
                )
                kwargs["evidence_profile"] = profile
                kwargs["source_identity_claims"] = (claim,)
                with self.assertRaises(NseArchiveResearchReplayError):
                    NseArchiveResearchReplaySession(**kwargs)

        kwargs = _direct_session_kwargs(
            market_session=date(2024, 1, 1), records=(record,)
        )
        kwargs["evidence_profile"] = EVIDENCE_PROFILE_UNRECONCILED
        kwargs["source_identity_claims"] = (claim,)
        session = NseArchiveResearchReplaySession(**kwargs)
        self.assertEqual(len(session.source_identity_claims), 1)

    def test_v1_v2_style_direct_session_defaults_to_empty_claims(self) -> None:
        record = _build_replay_record(_valid_record(date(2024, 1, 1)))
        kwargs = _direct_session_kwargs(
            market_session=date(2024, 1, 1), records=(record,)
        )
        session = NseArchiveResearchReplaySession(**kwargs)
        self.assertEqual(session.source_identity_claims, ())

    def test_replay_session_schema_version_is_v2_and_claim_ids_affect_identity(
        self,
    ) -> None:
        self.assertEqual(
            replay_module.REPLAY_SESSION_SCHEMA_VERSION_V1,
            "nse-archive-research-replay-session/v1",
        )
        self.assertEqual(
            replay_module.REPLAY_SESSION_SCHEMA_VERSION,
            "nse-archive-research-replay-session/v2",
        )

        record = _build_replay_record(_valid_record(date(2024, 1, 1), symbol="20MICRONS"))
        base_kwargs = _direct_session_kwargs(
            market_session=date(2024, 1, 1), records=(record,)
        )
        base_kwargs["evidence_profile"] = EVIDENCE_PROFILE_UNRECONCILED

        without_claims = NseArchiveResearchReplaySession(
            **{**base_kwargs, "source_identity_claims": ()}
        )
        claim = _build_replay_source_identity_claim(
            _valid_source_identity_claim(date(2024, 1, 1), symbol="20MICRONS")
        )
        with_claim = NseArchiveResearchReplaySession(
            **{**base_kwargs, "source_identity_claims": (claim,)}
        )
        self.assertNotEqual(without_claims.replay_session_id, with_claim.replay_session_id)


# ---------------------------------------------------------------------------
# Backward compatibility: v1/v2 stored session payloads never have claims
# inferred or fabricated -- they verify with an empty claims tuple.
# ---------------------------------------------------------------------------


class NseArchiveResearchReplaySourceIdentityClaimBackwardCompatTests(unittest.TestCase):
    def test_v1_and_v2_session_payloads_verify_with_empty_claims(self) -> None:
        cases = {
            "v1": (NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION_V1, {"evidence_profile", "missing_evidence"}),
            "v2": (NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION_V2, set()),
        }
        for label, (schema, drop_keys) in cases.items():
            with self.subTest(label):
                session = date(2024, 1, 2)
                stored = _full_stored_session(session)
                payload = dict(stored.normalized_payload)
                del payload["source_identity_claims"]
                for key in drop_keys:
                    del payload[key]
                payload["schema_version"] = schema
                stored = replace(stored, normalized_payload=payload)
                result = replay_module._verify_session_payload(stored, session)
                source_identity_claims_payload = result[-1]
                self.assertEqual(source_identity_claims_payload, ())


# ---------------------------------------------------------------------------
# Error sanitization (no secret leakage, no nested cause/context).
# ---------------------------------------------------------------------------


class NseArchiveResearchReplayErrorSanitizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dataset = _baseline_dataset()

    def test_reader_exception_with_planted_secret_does_not_leak(self) -> None:
        secret = "SECRET-PLANTED-VALUE-MUST-NOT-LEAK/var/data/topsecret.json"

        def _boom(reader, *, index_snapshot_id):
            raise ValueError(secret)

        with patch.object(
            replay_module,
            "load_verified_nse_historical_archive_range",
            side_effect=_boom,
        ):
            reader = object()
            with self.assertRaises(NseArchiveResearchReplayError) as context:
                list(iter_verified_nse_archive_research_sessions(self.dataset, reader))
        exc = context.exception
        self.assertNotIn(secret, str(exc))
        self.assertNotIn(secret, repr(exc))
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)

    def test_malformed_record_mapping_with_planted_secret_does_not_leak(self) -> None:
        binding = self.dataset.range_bindings[0]
        secret = "PATH-SECRET/leak/should/not/appear"

        class _ExplodingRecord(dict):
            def __iter__(self):
                raise TypeError(secret)

        sessions = list(_full_sessions_for_binding(binding))
        original = sessions[0]
        bad_record = _ExplodingRecord(original.normalized_payload["records"][0])
        bad_payload = {**original.normalized_payload, "records": (bad_record,)}
        sessions[0] = replace(original, normalized_payload=bad_payload)
        verified = _verified_from_binding(binding, tuple(sessions))

        with patch.object(
            replay_module,
            "load_verified_nse_historical_archive_range",
            return_value=verified,
        ):
            reader = object()
            with self.assertRaises(NseArchiveResearchReplayError) as context:
                list(iter_verified_nse_archive_research_sessions(self.dataset, reader))
        exc = context.exception
        self.assertNotIn(secret, str(exc))
        self.assertNotIn(secret, repr(exc))
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)

    def test_corrupted_dataset_identity_failure_does_not_leak_internal_state(self) -> None:
        train_partition = self.dataset.partitions[0]
        removed_session = train_partition.sessions[0]
        object.__setattr__(
            train_partition, "sessions", train_partition.sessions[1:]
        )
        reader = object()
        with self.assertRaises(NseArchiveResearchReplayError) as context:
            list(iter_verified_nse_archive_research_sessions(self.dataset, reader))
        exc = context.exception
        self.assertNotIn(removed_session.isoformat(), str(exc))
        self.assertIsNone(exc.__cause__)
        self.assertIsNone(exc.__context__)


# ---------------------------------------------------------------------------
# Structural tests: no I/O, no persistence, no discovery, no calculation.
# ---------------------------------------------------------------------------


class NseArchiveResearchReplayStructuralTests(unittest.TestCase):
    def setUp(self) -> None:
        self.source = inspect.getsource(replay_module)

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

    def test_no_store_construction_or_persistence(self) -> None:
        forbidden = (
            "MarketSnapshotStore(",
            ".put(",
            "pickle.",
            "shelve.",
            "sqlite3.",
            "json.dump",
        )
        for token in forbidden:
            self.assertNotIn(token, self.source, msg=f"forbidden token found: {token}")

    def test_no_discovery_or_latest_selection_fallback(self) -> None:
        forbidden = (
            ".glob(",
            ".iterdir(",
            ".listdir(",
            "find_by_selection_key",
            "latest_at_or_before",
            ".list(",
        )
        for token in forbidden:
            self.assertNotIn(token, self.source, msg=f"forbidden token found: {token}")

    def test_no_feature_signal_ranking_or_execution_calculation(self) -> None:
        forbidden = (
            "compute_feature",
            "calculate_return",
            "generate_signal",
            "rank(",
            "send_alert",
            "place_order",
            "execute_order",
            "confidence_score",
        )
        for token in forbidden:
            self.assertNotIn(token, self.source, msg=f"forbidden token found: {token}")

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

    def test_no_persisted_manifest_cache_or_resume_marker(self) -> None:
        forbidden = ("resume", "checkpoint", "manifest_path", "cache_dir")
        lowered = self.source.lower()
        for token in forbidden:
            self.assertNotIn(token, lowered, msg=f"forbidden token found: {token}")
