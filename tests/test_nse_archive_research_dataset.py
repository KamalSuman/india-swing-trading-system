from __future__ import annotations

import ast
import dataclasses
import inspect
import tempfile
import unittest
import zipfile
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing.evaluation import nse_archive_research_dataset
from india_swing.evaluation.nse_archive_research_dataset import (
    MINIMUM_FORWARD_LABEL_HORIZON_SESSIONS,
    NseArchiveResearchDataset,
    NseArchiveResearchDatasetError,
    NseArchiveResearchDatasetIntegrityError,
    NseArchiveResearchDatasetSplitPartition,
    NseArchiveResearchRangeBinding,
    ResearchArchiveExclusion,
    ResearchArchiveExclusionReason,
    ResearchArchiveSplitPolicy,
    ResearchSplitRole,
    _range_gap_is_weekend_only,
    build_nse_archive_research_dataset,
)
from india_swing.market_data.nse_archive import (
    EVIDENCE_PROFILE_COMPLETE,
    EVIDENCE_PROFILE_PRICE_UDIFF,
    EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY,
    EVIDENCE_PROFILE_UNRECONCILED,
    NSE_HISTORICAL_ARCHIVE_EQ_DATASET,
    NSE_HISTORICAL_ARCHIVE_INDEX_DATASET,
    import_nse_historical_range,
)
from india_swing.market_data.nse_archive_range import VerifiedNseHistoricalArchiveRange
from india_swing.market_data.snapshot_store import (
    LocalMarketSnapshotStore,
    MarketSnapshotManifest,
    StoredMarketSnapshot,
)
from tests.test_nse_historical_archive import archive_bytes


UTC = timezone.utc
OBSERVED_AT = datetime(2026, 8, 2, 0, 0, tzinfo=UTC)
HORIZON = MINIMUM_FORWARD_LABEL_HORIZON_SESSIONS


# ---------------------------------------------------------------------------
# Real-archive fixtures (used only where the test genuinely needs the real
# load_verified_nse_historical_archive_range trust boundary).
# ---------------------------------------------------------------------------


def _stage_sessions(root: Path, start: date, end: date) -> None:
    staging = root / "staging"
    archives = root / "source-archives"
    day = start
    while day <= end:
        session_staging = staging / day.isoformat()
        session_archives = archives / day.isoformat()
        session_staging.mkdir(parents=True, exist_ok=True)
        session_archives.mkdir(parents=True, exist_ok=True)
        archive_path = session_archives / f"Reports-Archives-Multiple-{day:%d%m%Y}.zip"
        archive_path.write_bytes(archive_bytes(session=day))
        with zipfile.ZipFile(archive_path) as archive:
            for name in archive.namelist():
                (session_staging / name).write_bytes(archive.read(name))
        day += timedelta(days=1)


def _import_range(root: Path, store: LocalMarketSnapshotStore, start: date, end: date):
    return import_nse_historical_range(
        staging_root=root / "staging",
        archive_root=root / "source-archives",
        store=store,
        start=start,
        end=end,
        observed_at=OBSERVED_AT,
    )


class _RecordingReader:
    def __init__(self, store: LocalMarketSnapshotStore) -> None:
        self._store = store
        self.calls: list[tuple[str, str]] = []

    def get(self, dataset: str, snapshot_id: str) -> StoredMarketSnapshot:
        self.calls.append((dataset, snapshot_id))
        return self._store.get(dataset, snapshot_id)


# ---------------------------------------------------------------------------
# Synthetic fixtures (fast, in-memory, no filesystem, for unit-level tests of
# the dataset's own validation and identity logic).
# ---------------------------------------------------------------------------


def _fake_sha256(label: str) -> str:
    import hashlib

    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def _stored_session(
    session: date,
    *,
    snapshot_id: str | None = None,
    record_count: int = 1,
    identity_issue_count: int = 0,
    collection_only: bool = True,
    actionable: bool = False,
    training_eligible: bool = False,
) -> StoredMarketSnapshot:
    resolved_id = snapshot_id or _fake_sha256(f"session-{session.isoformat()}")
    manifest = MarketSnapshotManifest(
        schema_version="test-schema/v1",
        codec_version="test-codec/v1",
        snapshot_id=resolved_id,
        dataset=NSE_HISTORICAL_ARCHIVE_EQ_DATASET,
        selection_key=session.isoformat(),
        provider="NSE_ARCHIVE",
        provider_version="test",
        observed_at=OBSERVED_AT,
        record_count=record_count,
        payload_filename="payload.json",
        payload_sha256=_fake_sha256(f"payload-{session.isoformat()}-{resolved_id}"),
    )
    payload = {
        "session": session,
        "collection_only": collection_only,
        "actionable": actionable,
        "training_eligible": training_eligible,
        "identity_issue_count": identity_issue_count,
    }
    return StoredMarketSnapshot(
        path=Path("synthetic"),
        manifest=manifest,
        normalized_payload=payload,
        payload_bytes=b"",
    )


def _verified_range(
    sessions: tuple[date, ...],
    *,
    index_snapshot_id: str | None = None,
    range_start: date | None = None,
    range_end: date | None = None,
    stored_sessions: tuple[StoredMarketSnapshot, ...] | None = None,
    per_session_record_count: int = 1,
    per_session_identity_issue_count: int = 0,
    record_count: int | None = None,
    identity_issue_count: int | None = None,
    identity_quarantined_session_count: int | None = None,
    incomplete_evidence_session_count: int = 0,
    evidence_profile_counts: dict | None = None,
) -> VerifiedNseHistoricalArchiveRange:
    resolved_stored = stored_sessions or tuple(
        _stored_session(
            value,
            record_count=per_session_record_count,
            identity_issue_count=per_session_identity_issue_count,
        )
        for value in sessions
    )
    resolved_record_count = (
        sum(value.manifest.record_count for value in resolved_stored)
        if record_count is None
        else record_count
    )
    resolved_issue_count = (
        sum(
            value.normalized_payload["identity_issue_count"]
            for value in resolved_stored
        )
        if identity_issue_count is None
        else identity_issue_count
    )
    resolved_quarantined = (
        sum(
            value.normalized_payload["identity_issue_count"] > 0
            for value in resolved_stored
        )
        if identity_quarantined_session_count is None
        else identity_quarantined_session_count
    )
    resolved_profile_counts = evidence_profile_counts or {
        EVIDENCE_PROFILE_COMPLETE: len(resolved_stored),
        EVIDENCE_PROFILE_PRICE_UDIFF: 0,
        EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY: 0,
        EVIDENCE_PROFILE_UNRECONCILED: 0,
    }
    label = "-".join(value.isoformat() for value in sessions)
    return VerifiedNseHistoricalArchiveRange(
        index_snapshot_id=index_snapshot_id or _fake_sha256(f"index-{label}"),
        range_start=range_start or sessions[0],
        range_end=range_end or sessions[-1],
        session_snapshot_ids=tuple(
            value.manifest.snapshot_id for value in resolved_stored
        ),
        sessions=resolved_stored,
        record_count=resolved_record_count,
        identity_issue_count=resolved_issue_count,
        identity_quarantined_session_count=resolved_quarantined,
        incomplete_evidence_session_count=incomplete_evidence_session_count,
        evidence_profile_counts=resolved_profile_counts,
    )


def _binding(
    sessions: tuple[date, ...],
    *,
    index_snapshot_id: str | None = None,
    range_start: date | None = None,
    range_end: date | None = None,
    **kwargs: object,
) -> NseArchiveResearchRangeBinding:
    verified = _verified_range(
        sessions,
        index_snapshot_id=index_snapshot_id,
        range_start=range_start,
        range_end=range_end,
        **kwargs,
    )
    return NseArchiveResearchRangeBinding.from_verified_range(verified)


def _partition(
    role: ResearchSplitRole, sessions: tuple[date, ...], horizon: int = HORIZON
) -> NseArchiveResearchDatasetSplitPartition:
    return NseArchiveResearchDatasetSplitPartition(
        role=role,
        sessions=sessions,
        candidate_label_origin_sessions=sessions[:-horizon],
        unavailable_label_tail_sessions=sessions[-horizon:],
        maximum_forward_label_horizon_sessions=horizon,
    )


def _sum_profile_counts(
    bindings: tuple[NseArchiveResearchRangeBinding, ...]
) -> tuple[tuple[str, int], ...]:
    totals = {
        EVIDENCE_PROFILE_COMPLETE: 0,
        EVIDENCE_PROFILE_PRICE_UDIFF: 0,
        EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY: 0,
        EVIDENCE_PROFILE_UNRECONCILED: 0,
    }
    for binding in bindings:
        for profile, count in binding.evidence_profile_counts:
            totals[profile] += count
    return tuple(sorted(totals.items()))


def _baseline_components(
    *,
    shift_days: int = 0,
    horizon: int = HORIZON,
    gap_before_validation: bool = False,
    split_policy: ResearchArchiveSplitPolicy | None = None,
    **binding_kwargs: object,
) -> dict[str, object]:
    """Build a fully self-consistent set of constructor kwargs for
    NseArchiveResearchDataset: one binding spanning three exact, horizon-sized
    (horizon + 1 session) TRAIN/VALIDATION/UNTOUCHED_TEST blocks, a matching
    ResearchArchiveSplitPolicy, and matching partitions -- entirely synthetic
    and in-memory, so it stays fast even though a valid dataset now always
    needs at least horizon + 1 sessions per role.
    """

    per_role = horizon + 1
    base = date(2024, 1, 1) + timedelta(days=shift_days)
    train_sessions = tuple(base + timedelta(days=i) for i in range(per_role))
    train_end = train_sessions[-1]
    # The policy boundary itself is always exactly calendar-adjacent (no gap
    # is ever allowed there). When gap_before_validation is set, the single
    # skipped calendar day is validation_start itself: it lies inside the
    # policy's validation window but is simply absent from accepted_sessions,
    # exactly like a real quarantined/missing trading session -- callers
    # record it as an explicit ResearchArchiveExclusion, never a policy gap.
    validation_start = train_end + timedelta(days=1)
    validation_block_start = (
        validation_start + timedelta(days=1) if gap_before_validation else validation_start
    )
    validation_sessions = tuple(
        validation_block_start + timedelta(days=i) for i in range(per_role)
    )
    validation_end = validation_sessions[-1]
    test_start = validation_end + timedelta(days=1)
    test_sessions = tuple(test_start + timedelta(days=i) for i in range(per_role))
    all_sessions = train_sessions + validation_sessions + test_sessions

    binding = _binding(
        all_sessions,
        range_start=train_sessions[0],
        range_end=test_sessions[-1],
        **binding_kwargs,
    )
    policy = split_policy or ResearchArchiveSplitPolicy(
        train_end=train_end,
        validation_start=validation_start,
        validation_end=validation_end,
        test_start=test_start,
        maximum_forward_label_horizon_sessions=horizon,
    )
    partitions = (
        _partition(ResearchSplitRole.TRAIN, train_sessions, horizon),
        _partition(ResearchSplitRole.VALIDATION, validation_sessions, horizon),
        _partition(ResearchSplitRole.UNTOUCHED_TEST, test_sessions, horizon),
    )
    return {
        "index_snapshot_ids": (binding.index_snapshot_id,),
        "range_bindings": (binding,),
        "accepted_sessions": binding.accepted_sessions,
        "session_snapshot_ids": binding.session_snapshot_ids,
        "exclusions": (),
        "partitions": partitions,
        "record_count": binding.record_count,
        "identity_issue_count": binding.identity_issue_count,
        "identity_quarantined_session_count": binding.identity_quarantined_session_count,
        "incomplete_evidence_session_count": binding.incomplete_evidence_session_count,
        "evidence_profile_counts": binding.evidence_profile_counts,
        "split_policy": policy,
    }


def _baseline_dataset(
    *,
    exclusions: tuple[ResearchArchiveExclusion, ...] = (),
    split_policy: ResearchArchiveSplitPolicy | None = None,
    shift_days: int = 0,
    horizon: int = HORIZON,
    gap_before_validation: bool = False,
    **binding_kwargs: object,
) -> NseArchiveResearchDataset:
    components = _baseline_components(
        shift_days=shift_days,
        horizon=horizon,
        gap_before_validation=gap_before_validation,
        split_policy=split_policy,
        **binding_kwargs,
    )
    components["exclusions"] = exclusions
    return NseArchiveResearchDataset(**components)


def _dataset_from_bindings(
    bindings: tuple[NseArchiveResearchRangeBinding, ...],
    *,
    exclusions: tuple[ResearchArchiveExclusion, ...] = (),
    partitions: tuple[NseArchiveResearchDatasetSplitPartition, ...] | None = None,
    split_policy: ResearchArchiveSplitPolicy | None = None,
) -> NseArchiveResearchDataset:
    """Assemble a dataset directly from caller-supplied bindings.

    Uses an unrelated, internally valid filler split_policy/partitions pair
    by default (from a fresh _baseline_components() call) for tests whose
    rejection is expected to fire on the bindings themselves (adjacency,
    duplicate sessions/snapshot ids, aggregate counts) before the dataset's
    validation ever reaches the partition/policy cross-checks.
    """

    filler = _baseline_components()
    accepted = tuple(session for binding in bindings for session in binding.accepted_sessions)
    snapshot_ids = tuple(
        value for binding in bindings for value in binding.session_snapshot_ids
    )
    return NseArchiveResearchDataset(
        index_snapshot_ids=tuple(binding.index_snapshot_id for binding in bindings),
        range_bindings=tuple(bindings),
        accepted_sessions=accepted,
        session_snapshot_ids=snapshot_ids,
        exclusions=exclusions,
        partitions=partitions if partitions is not None else filler["partitions"],
        record_count=sum(binding.record_count for binding in bindings),
        identity_issue_count=sum(binding.identity_issue_count for binding in bindings),
        identity_quarantined_session_count=sum(
            binding.identity_quarantined_session_count for binding in bindings
        ),
        incomplete_evidence_session_count=sum(
            binding.incomplete_evidence_session_count for binding in bindings
        ),
        evidence_profile_counts=_sum_profile_counts(bindings),
        split_policy=split_policy if split_policy is not None else filler["split_policy"],
    )


# With shift_days=0, horizon=HORIZON, gap_before_validation=True, the single
# calendar day skipped between TRAIN and VALIDATION is always this date.
_GAP_DAY = date(2024, 1, 1) + timedelta(days=HORIZON + 1)


class NseArchiveResearchDatasetHappyPathTests(unittest.TestCase):
    def test_three_range_happy_path_is_deterministic_and_fully_accounted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            train_start, train_end = date(2024, 1, 1), date(2024, 1, 21)
            validation_start, validation_end = date(2024, 1, 22), date(2024, 2, 11)
            test_start, test_end = date(2024, 2, 12), date(2024, 3, 3)

            _stage_sessions(root, train_start, train_end)
            _stage_sessions(root, validation_start, validation_end)
            _stage_sessions(root, test_start, test_end)

            store = LocalMarketSnapshotStore(root / "canonical")
            _, train_index = _import_range(root, store, train_start, train_end)
            _, validation_index = _import_range(
                root, store, validation_start, validation_end
            )
            _, test_index = _import_range(root, store, test_start, test_end)

            policy = ResearchArchiveSplitPolicy(
                train_end=train_end,
                validation_start=validation_start,
                validation_end=validation_end,
                test_start=test_start,
                maximum_forward_label_horizon_sessions=HORIZON,
            )
            ids = (
                train_index.manifest.snapshot_id,
                validation_index.manifest.snapshot_id,
                test_index.manifest.snapshot_id,
            )

            dataset = build_nse_archive_research_dataset(
                store, index_snapshot_ids=ids, split_policy=policy
            )

            self.assertEqual(dataset.index_snapshot_ids, ids)
            self.assertEqual(dataset.record_count, 63)
            self.assertEqual(len(dataset.accepted_sessions), 63)
            self.assertEqual(
                dataset.accepted_sessions, tuple(sorted(set(dataset.accepted_sessions)))
            )
            self.assertEqual(len(dataset.partitions), 3)
            for partition in dataset.partitions:
                self.assertEqual(len(partition.sessions), 21)
                self.assertEqual(len(partition.unavailable_label_tail_sessions), 20)
                self.assertEqual(len(partition.candidate_label_origin_sessions), 1)
                self.assertEqual(partition.maximum_forward_label_horizon_sessions, 20)
            self.assertEqual(dataset.exclusions, ())
            self.assertTrue(dataset.coverage_complete)
            self.assertTrue(dataset.collection_only)
            self.assertFalse(dataset.actionable)
            self.assertFalse(dataset.training_eligible)
            for flag in (
                "feature_eligible",
                "label_eligible",
                "alert_eligible",
                "execution_eligible",
                "identity_resolution_complete",
                "corporate_action_adjustment_complete",
            ):
                self.assertFalse(getattr(dataset, flag))
            self.assertIs(dataset.split_policy, policy)
            self.assertEqual(dataset.split_policy_id, policy.policy_id)
            dataset.verify_content_identity()

            rebuilt = build_nse_archive_research_dataset(
                store, index_snapshot_ids=ids, split_policy=policy
            )
            self.assertEqual(dataset.dataset_id, rebuilt.dataset_id)


class NseArchiveResearchDatasetBuilderLoaderInteractionTests(unittest.TestCase):
    def test_missing_non_sha_and_repeated_index_ids_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _stage_sessions(root, date(2024, 6, 1), date(2024, 6, 1))
            store = LocalMarketSnapshotStore(root / "canonical")
            _, index = _import_range(root, store, date(2024, 6, 1), date(2024, 6, 1))
            valid_id = index.manifest.snapshot_id
            policy = ResearchArchiveSplitPolicy(
                train_end=date(2024, 6, 1),
                validation_start=date(2024, 6, 2),
                validation_end=date(2024, 6, 2),
                test_start=date(2024, 6, 3),
                maximum_forward_label_horizon_sessions=20,
            )

            with self.subTest("non_sha_id"):
                with self.assertRaisesRegex(
                    NseArchiveResearchDatasetError, "index snapshot ids are invalid"
                ):
                    build_nse_archive_research_dataset(
                        store,
                        index_snapshot_ids=("not-a-sha",),
                        split_policy=policy,
                    )

            with self.subTest("repeated_id"):
                with self.assertRaisesRegex(
                    NseArchiveResearchDatasetError, "index snapshot ids are invalid"
                ):
                    build_nse_archive_research_dataset(
                        store,
                        index_snapshot_ids=(valid_id, valid_id),
                        split_policy=policy,
                    )

            with self.subTest("missing_id"):
                with self.assertRaisesRegex(
                    NseArchiveResearchDatasetError, "could not be loaded"
                ):
                    build_nse_archive_research_dataset(
                        store,
                        index_snapshot_ids=(_fake_sha256("missing-index"),),
                        split_policy=policy,
                    )

            with self.subTest("wrong_dataset_id"):
                stored_session_id = index.normalized_payload["records"][0]["snapshot_id"]
                with self.assertRaisesRegex(
                    NseArchiveResearchDatasetError, "could not be loaded"
                ):
                    build_nse_archive_research_dataset(
                        store,
                        index_snapshot_ids=(stored_session_id,),
                        split_policy=policy,
                    )

    def test_mismatched_index_identity_is_rejected_without_leaking_nested_detail(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _stage_sessions(root, date(2024, 6, 1), date(2024, 6, 1))
            _stage_sessions(root, date(2024, 6, 10), date(2024, 6, 10))
            store = LocalMarketSnapshotStore(root / "canonical")
            _, index_a = _import_range(root, store, date(2024, 6, 1), date(2024, 6, 1))
            _, index_b = _import_range(root, store, date(2024, 6, 10), date(2024, 6, 10))

            class _AlwaysIndexAReader:
                def get(self, dataset: str, snapshot_id: str) -> StoredMarketSnapshot:
                    if dataset == NSE_HISTORICAL_ARCHIVE_INDEX_DATASET:
                        return store.get(dataset, index_a.manifest.snapshot_id)
                    return store.get(dataset, snapshot_id)

            policy = ResearchArchiveSplitPolicy(
                train_end=date(2024, 6, 10),
                validation_start=date(2024, 6, 11),
                validation_end=date(2024, 6, 11),
                test_start=date(2024, 6, 12),
                maximum_forward_label_horizon_sessions=20,
            )
            with self.assertRaises(NseArchiveResearchDatasetError) as ctx:
                build_nse_archive_research_dataset(
                    _AlwaysIndexAReader(),
                    index_snapshot_ids=(index_b.manifest.snapshot_id,),
                    split_policy=policy,
                )
            message = str(ctx.exception)
            self.assertEqual(message, "research dataset archive range could not be loaded")
            self.assertNotIn(index_a.manifest.snapshot_id, message)
            self.assertNotIn(index_b.manifest.snapshot_id, message)
            self.assertNotIn("index payload is invalid", message)
            self.assertIsNone(ctx.exception.__cause__)
            self.assertIsNone(ctx.exception.__context__)

    def test_builder_calls_loader_exactly_once_per_id_in_order_no_listing(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _stage_sessions(root, date(2024, 7, 1), date(2024, 7, 2))
            store = LocalMarketSnapshotStore(root / "canonical")
            _, index_1 = _import_range(root, store, date(2024, 7, 1), date(2024, 7, 1))
            _, index_2 = _import_range(root, store, date(2024, 7, 2), date(2024, 7, 2))
            session_id_1 = index_1.normalized_payload["records"][0]["snapshot_id"]
            session_id_2 = index_2.normalized_payload["records"][0]["snapshot_id"]

            recorder = _RecordingReader(store)
            policy = ResearchArchiveSplitPolicy(
                train_end=date(2024, 7, 1),
                validation_start=date(2024, 7, 2),
                validation_end=date(2024, 7, 2),
                test_start=date(2024, 7, 3),
                maximum_forward_label_horizon_sessions=20,
            )
            with self.assertRaises(NseArchiveResearchDatasetError):
                build_nse_archive_research_dataset(
                    recorder,
                    index_snapshot_ids=(
                        index_1.manifest.snapshot_id,
                        index_2.manifest.snapshot_id,
                    ),
                    split_policy=policy,
                )
            self.assertEqual(
                recorder.calls,
                [
                    (NSE_HISTORICAL_ARCHIVE_INDEX_DATASET, index_1.manifest.snapshot_id),
                    (NSE_HISTORICAL_ARCHIVE_EQ_DATASET, session_id_1),
                    (NSE_HISTORICAL_ARCHIVE_INDEX_DATASET, index_2.manifest.snapshot_id),
                    (NSE_HISTORICAL_ARCHIVE_EQ_DATASET, session_id_2),
                ],
            )


class NseArchiveResearchDatasetSplitRoleTests(unittest.TestCase):
    def test_reject_role_with_no_sessions_and_role_too_short_for_horizon(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            start, end = date(2024, 5, 1), date(2024, 5, 21)
            _stage_sessions(root, start, end)
            store = LocalMarketSnapshotStore(root / "canonical")
            _, index = _import_range(root, store, start, end)

            with self.subTest("role_has_no_accepted_sessions"):
                policy = ResearchArchiveSplitPolicy(
                    train_end=end,
                    validation_start=end + timedelta(days=1),
                    validation_end=end + timedelta(days=30),
                    test_start=end + timedelta(days=31),
                    maximum_forward_label_horizon_sessions=20,
                )
                with self.assertRaisesRegex(
                    NseArchiveResearchDatasetError, "no accepted sessions"
                ):
                    build_nse_archive_research_dataset(
                        store,
                        index_snapshot_ids=(index.manifest.snapshot_id,),
                        split_policy=policy,
                    )

            with self.subTest("role_too_short_for_horizon"):
                policy = ResearchArchiveSplitPolicy(
                    train_end=date(2024, 5, 5),
                    validation_start=date(2024, 5, 6),
                    validation_end=date(2024, 5, 20),
                    test_start=date(2024, 5, 21),
                    maximum_forward_label_horizon_sessions=20,
                )
                with self.assertRaisesRegex(
                    NseArchiveResearchDatasetError,
                    "no candidate label origin sessions",
                ):
                    build_nse_archive_research_dataset(
                        store,
                        index_snapshot_ids=(index.manifest.snapshot_id,),
                        split_policy=policy,
                    )


class ResearchArchiveSplitPolicyTests(unittest.TestCase):
    def test_reject_malformed_split_policies(self) -> None:
        good_kwargs = dict(
            train_end=date(2024, 1, 21),
            validation_start=date(2024, 1, 22),
            validation_end=date(2024, 2, 11),
            test_start=date(2024, 2, 12),
            maximum_forward_label_horizon_sessions=20,
        )
        ResearchArchiveSplitPolicy(**good_kwargs)

        with self.subTest("train_validation_gap"):
            bad = dict(
                good_kwargs,
                validation_start=good_kwargs["validation_start"] + timedelta(days=1),
            )
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError, "calendar-adjacent"
            ):
                ResearchArchiveSplitPolicy(**bad)

        with self.subTest("train_validation_overlap"):
            bad = dict(good_kwargs, validation_start=good_kwargs["train_end"])
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError, "calendar-adjacent"
            ):
                ResearchArchiveSplitPolicy(**bad)

        with self.subTest("validation_window_reversed"):
            bad = dict(
                good_kwargs,
                validation_end=good_kwargs["validation_start"] - timedelta(days=1),
            )
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError, "reversed or empty"
            ):
                ResearchArchiveSplitPolicy(**bad)

        with self.subTest("validation_test_gap"):
            bad = dict(
                good_kwargs, test_start=good_kwargs["test_start"] + timedelta(days=1)
            )
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError, "calendar-adjacent"
            ):
                ResearchArchiveSplitPolicy(**bad)

        with self.subTest("non_date_subclass"):
            bad = dict(good_kwargs, train_end=datetime(2024, 1, 21, tzinfo=UTC))
            with self.assertRaisesRegex(NseArchiveResearchDatasetError, "exact date"):
                ResearchArchiveSplitPolicy(**bad)

        with self.subTest("bool_horizon"):
            bad = dict(good_kwargs, maximum_forward_label_horizon_sessions=True)
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError, "exact integer"
            ):
                ResearchArchiveSplitPolicy(**bad)

        with self.subTest("non_int_horizon"):
            bad = dict(good_kwargs, maximum_forward_label_horizon_sessions=20.0)
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError, "exact integer"
            ):
                ResearchArchiveSplitPolicy(**bad)

        with self.subTest("horizon_below_minimum"):
            bad = dict(good_kwargs, maximum_forward_label_horizon_sessions=19)
            with self.assertRaisesRegex(NseArchiveResearchDatasetError, "at least 20"):
                ResearchArchiveSplitPolicy(**bad)


class NseArchiveResearchDatasetSplitPartitionTests(unittest.TestCase):
    """Direct regression coverage for Codex's rejected-revision-1 probe:
    a partition's own construction must independently prove its
    candidate/tail split matches its declared horizon -- a zero, short, or
    otherwise arbitrary tail must fail here, before any dataset exists."""

    def test_reject_zero_short_or_arbitrary_tail_for_a_fixed_horizon(self) -> None:
        sessions = tuple(date(2024, 1, 1) + timedelta(days=i) for i in range(21))

        with self.subTest("zero_tail_all_sessions_marked_candidate"):
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError, "does not match its declared horizon"
            ):
                NseArchiveResearchDatasetSplitPartition(
                    role=ResearchSplitRole.TRAIN,
                    sessions=sessions,
                    candidate_label_origin_sessions=sessions,
                    unavailable_label_tail_sessions=(),
                    maximum_forward_label_horizon_sessions=20,
                )

        with self.subTest("tail_length_1"):
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError, "does not match its declared horizon"
            ):
                NseArchiveResearchDatasetSplitPartition(
                    role=ResearchSplitRole.TRAIN,
                    sessions=sessions,
                    candidate_label_origin_sessions=sessions[:-1],
                    unavailable_label_tail_sessions=sessions[-1:],
                    maximum_forward_label_horizon_sessions=20,
                )

        with self.subTest("tail_length_19"):
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError, "does not match its declared horizon"
            ):
                NseArchiveResearchDatasetSplitPartition(
                    role=ResearchSplitRole.TRAIN,
                    sessions=sessions,
                    candidate_label_origin_sessions=sessions[:-19],
                    unavailable_label_tail_sessions=sessions[-19:],
                    maximum_forward_label_horizon_sessions=20,
                )

        with self.subTest("exact_horizon_length_leaves_no_candidate"):
            exact = sessions[:20]
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError,
                "no candidate label origin sessions",
            ):
                NseArchiveResearchDatasetSplitPartition(
                    role=ResearchSplitRole.TRAIN,
                    sessions=exact,
                    candidate_label_origin_sessions=(),
                    unavailable_label_tail_sessions=exact,
                    maximum_forward_label_horizon_sessions=20,
                )

        with self.subTest("horizon_below_minimum"):
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError, "at least 20"
            ):
                NseArchiveResearchDatasetSplitPartition(
                    role=ResearchSplitRole.TRAIN,
                    sessions=sessions,
                    candidate_label_origin_sessions=sessions[:2],
                    unavailable_label_tail_sessions=sessions[2:],
                    maximum_forward_label_horizon_sessions=19,
                )

        with self.subTest("bool_horizon"):
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError, "exact integer"
            ):
                NseArchiveResearchDatasetSplitPartition(
                    role=ResearchSplitRole.TRAIN,
                    sessions=sessions,
                    candidate_label_origin_sessions=sessions,
                    unavailable_label_tail_sessions=(),
                    maximum_forward_label_horizon_sessions=True,
                )

        # Correctly derived candidate/tail for horizon=20 must still work.
        NseArchiveResearchDatasetSplitPartition(
            role=ResearchSplitRole.TRAIN,
            sessions=sessions,
            candidate_label_origin_sessions=sessions[:-20],
            unavailable_label_tail_sessions=sessions[-20:],
            maximum_forward_label_horizon_sessions=20,
        )


class NseArchiveResearchDatasetSplitPolicyBindingTests(unittest.TestCase):
    """The dataset must retain and independently re-verify the actual
    ResearchArchiveSplitPolicy object -- not merely accept a free-standing
    hash -- and derive every partition's expected session tuple and horizon
    from it."""

    def test_dataset_constructor_rejects_a_free_standing_split_policy_id(self) -> None:
        components = _baseline_components()
        with self.assertRaises(TypeError):
            NseArchiveResearchDataset(
                index_snapshot_ids=components["index_snapshot_ids"],
                range_bindings=components["range_bindings"],
                accepted_sessions=components["accepted_sessions"],
                session_snapshot_ids=components["session_snapshot_ids"],
                exclusions=components["exclusions"],
                partitions=components["partitions"],
                record_count=components["record_count"],
                identity_issue_count=components["identity_issue_count"],
                identity_quarantined_session_count=components[
                    "identity_quarantined_session_count"
                ],
                incomplete_evidence_session_count=components[
                    "incomplete_evidence_session_count"
                ],
                evidence_profile_counts=components["evidence_profile_counts"],
                split_policy_id=_fake_sha256("free-standing"),
            )

    def test_tampering_the_retained_split_policy_is_rejected_independently(
        self,
    ) -> None:
        dataset = _baseline_dataset()
        object.__setattr__(
            dataset.split_policy, "maximum_forward_label_horizon_sessions", 25
        )
        with self.assertRaises(NseArchiveResearchDatasetIntegrityError):
            dataset.verify_content_identity()

    def test_replacing_the_retained_split_policy_has_a_sanitized_rejection(
        self,
    ) -> None:
        dataset = _baseline_dataset()
        object.__setattr__(dataset, "split_policy", object())
        with self.assertRaisesRegex(
            NseArchiveResearchDatasetError, "split policy is invalid"
        ) as raised:
            dataset.verify_content_identity()
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)

    def test_replacing_partitions_has_a_sanitized_rejection(self) -> None:
        dataset = _baseline_dataset()
        object.__setattr__(dataset, "partitions", (object(), object(), object()))
        with self.assertRaisesRegex(
            NseArchiveResearchDatasetError, "partitions are invalid"
        ) as raised:
            dataset.verify_content_identity()
        self.assertIsNone(raised.exception.__cause__)
        self.assertIsNone(raised.exception.__context__)

    def test_reject_partition_horizon_mismatch_versus_split_policy(self) -> None:
        components = dict(_baseline_components())
        base_train_sessions = components["partitions"][0].sessions
        widened_sessions = base_train_sessions + (
            base_train_sessions[-1] + timedelta(days=1),
        )
        mismatched_train = NseArchiveResearchDatasetSplitPartition(
            role=ResearchSplitRole.TRAIN,
            sessions=widened_sessions,
            candidate_label_origin_sessions=widened_sessions[:-21],
            unavailable_label_tail_sessions=widened_sessions[-21:],
            maximum_forward_label_horizon_sessions=21,
        )
        components["partitions"] = (mismatched_train,) + components["partitions"][1:]
        with self.assertRaisesRegex(
            NseArchiveResearchDatasetError,
            "horizon does not match its split policy",
        ):
            NseArchiveResearchDataset(**components)

    def test_reject_partition_sessions_assigned_to_wrong_role_boundary(self) -> None:
        components = dict(_baseline_components())
        train_partition, validation_partition, test_partition = components[
            "partitions"
        ]
        # Swap TRAIN's and VALIDATION's session content under the wrong role
        # label. Each swapped partition is still internally self-consistent
        # (candidate/tail correctly match its own horizon) and the three
        # partitions together remain disjoint and exhaustive -- only the
        # role assignment is wrong.
        swapped_train = NseArchiveResearchDatasetSplitPartition(
            role=ResearchSplitRole.TRAIN,
            sessions=validation_partition.sessions,
            candidate_label_origin_sessions=(
                validation_partition.candidate_label_origin_sessions
            ),
            unavailable_label_tail_sessions=(
                validation_partition.unavailable_label_tail_sessions
            ),
            maximum_forward_label_horizon_sessions=(
                validation_partition.maximum_forward_label_horizon_sessions
            ),
        )
        swapped_validation = NseArchiveResearchDatasetSplitPartition(
            role=ResearchSplitRole.VALIDATION,
            sessions=train_partition.sessions,
            candidate_label_origin_sessions=(
                train_partition.candidate_label_origin_sessions
            ),
            unavailable_label_tail_sessions=(
                train_partition.unavailable_label_tail_sessions
            ),
            maximum_forward_label_horizon_sessions=(
                train_partition.maximum_forward_label_horizon_sessions
            ),
        )
        components["partitions"] = (swapped_train, swapped_validation, test_partition)
        with self.assertRaisesRegex(
            NseArchiveResearchDatasetError,
            "derived split policy role boundary",
        ):
            NseArchiveResearchDataset(**components)


class NseArchiveResearchRangeBindingTests(unittest.TestCase):
    def test_reject_duplicate_accepted_session_within_one_binding(self) -> None:
        with self.assertRaisesRegex(
            NseArchiveResearchDatasetError, "sorted and unique"
        ):
            NseArchiveResearchRangeBinding(
                index_snapshot_id=_fake_sha256("idx-dup"),
                range_start=date(2024, 1, 1),
                range_end=date(2024, 1, 3),
                session_snapshot_ids=(
                    _fake_sha256("s1"),
                    _fake_sha256("s2"),
                ),
                accepted_sessions=(date(2024, 1, 1), date(2024, 1, 1)),
                record_count=2,
                identity_issue_count=0,
                identity_quarantined_session_count=0,
                incomplete_evidence_session_count=0,
                evidence_profile_counts=(
                    (EVIDENCE_PROFILE_COMPLETE, 2),
                    (EVIDENCE_PROFILE_PRICE_UDIFF, 0),
                    (EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY, 0),
                    (EVIDENCE_PROFILE_UNRECONCILED, 0),
                ),
            )

    def test_reject_session_outside_its_own_calendar_envelope(self) -> None:
        with self.assertRaisesRegex(
            NseArchiveResearchDatasetError, "outside its calendar envelope"
        ):
            NseArchiveResearchRangeBinding(
                index_snapshot_id=_fake_sha256("idx-outside"),
                range_start=date(2024, 1, 1),
                range_end=date(2024, 1, 2),
                session_snapshot_ids=(_fake_sha256("s3"),),
                accepted_sessions=(date(2024, 1, 5),),
                record_count=1,
                identity_issue_count=0,
                identity_quarantined_session_count=0,
                incomplete_evidence_session_count=0,
                evidence_profile_counts=(
                    (EVIDENCE_PROFILE_COMPLETE, 1),
                    (EVIDENCE_PROFILE_PRICE_UDIFF, 0),
                    (EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY, 0),
                    (EVIDENCE_PROFILE_UNRECONCILED, 0),
                ),
            )

    def test_from_verified_range_rejects_mismatched_stored_manifest_identity(
        self,
    ) -> None:
        with self.subTest("mismatched_session_snapshot_id"):
            stored = _stored_session(date(2024, 1, 1))
            verified = _verified_range((date(2024, 1, 1),), stored_sessions=(stored,))
            tampered = replace(
                verified, session_snapshot_ids=(_fake_sha256("different"),)
            )
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError,
                "session snapshot identity is inconsistent",
            ):
                NseArchiveResearchRangeBinding.from_verified_range(tampered)

        with self.subTest("mismatched_record_count"):
            stored = _stored_session(date(2024, 1, 1), record_count=1)
            verified = _verified_range(
                (date(2024, 1, 1),), stored_sessions=(stored,), record_count=999
            )
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError, "record count is inconsistent"
            ):
                NseArchiveResearchRangeBinding.from_verified_range(verified)

        with self.subTest("mismatched_identity_issue_count"):
            stored = _stored_session(date(2024, 1, 1), identity_issue_count=0)
            verified = _verified_range(
                (date(2024, 1, 1),),
                stored_sessions=(stored,),
                identity_issue_count=5,
            )
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError,
                "identity issue accounting is inconsistent",
            ):
                NseArchiveResearchRangeBinding.from_verified_range(verified)

        with self.subTest("mismatched_evidence_profile_counts"):
            stored = _stored_session(date(2024, 1, 1))
            verified = _verified_range(
                (date(2024, 1, 1),),
                stored_sessions=(stored,),
                evidence_profile_counts={
                    EVIDENCE_PROFILE_COMPLETE: 1,
                    EVIDENCE_PROFILE_PRICE_UDIFF: 1,
                    EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY: 0,
                    EVIDENCE_PROFILE_UNRECONCILED: 0,
                },
            )
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError,
                "evidence profile counts are inconsistent",
            ):
                NseArchiveResearchRangeBinding.from_verified_range(verified)

        with self.subTest("unsafe_session_posture"):
            stored = _stored_session(date(2024, 1, 1), actionable=True)
            verified = _verified_range((date(2024, 1, 1),), stored_sessions=(stored,))
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError, "safety posture is invalid"
            ):
                NseArchiveResearchRangeBinding.from_verified_range(verified)


class NseArchiveResearchDatasetRangeAssemblyTests(unittest.TestCase):
    def test_range_boundary_allows_weekends_but_rejects_weekdays(self) -> None:
        self.assertTrue(
            _range_gap_is_weekend_only(date(2021, 12, 31), date(2022, 1, 3))
        )
        self.assertTrue(
            _range_gap_is_weekend_only(date(2022, 12, 30), date(2023, 1, 2))
        )
        self.assertTrue(
            _range_gap_is_weekend_only(date(2023, 12, 29), date(2024, 1, 1))
        )
        self.assertFalse(
            _range_gap_is_weekend_only(date(2024, 1, 3), date(2024, 1, 5))
        )

    def test_reject_reordering_overlap_and_gap_between_range_bindings(self) -> None:
        first = _binding(
            (date(2024, 1, 1),), range_start=date(2024, 1, 1), range_end=date(2024, 1, 3)
        )

        with self.subTest("gap"):
            second = _binding(
                (date(2024, 1, 6),),
                range_start=date(2024, 1, 5),
                range_end=date(2024, 1, 6),
            )
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError, "gap contains a weekday"
            ):
                _dataset_from_bindings((first, second))

        with self.subTest("overlap"):
            second = _binding(
                (date(2024, 1, 3),),
                range_start=date(2024, 1, 2),
                range_end=date(2024, 1, 3),
            )
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError, "overlap or reorder"
            ):
                _dataset_from_bindings((first, second))

        with self.subTest("reordering"):
            second = _binding(
                (date(2023, 12, 1),),
                range_start=date(2023, 12, 1),
                range_end=date(2023, 12, 1),
            )
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError, "overlap or reorder"
            ):
                _dataset_from_bindings((first, second))

    def test_reject_duplicate_session_snapshot_id_across_non_overlapping_bindings(
        self,
    ) -> None:
        shared_id = _fake_sha256("shared-eq-snapshot")
        stored_1 = _stored_session(date(2024, 1, 1), snapshot_id=shared_id)
        binding_1 = NseArchiveResearchRangeBinding.from_verified_range(
            _verified_range(
                (date(2024, 1, 1),),
                stored_sessions=(stored_1,),
                range_start=date(2024, 1, 1),
                range_end=date(2024, 1, 3),
            )
        )
        stored_2 = _stored_session(date(2024, 1, 10), snapshot_id=shared_id)
        binding_2 = NseArchiveResearchRangeBinding.from_verified_range(
            _verified_range(
                (date(2024, 1, 10),),
                stored_sessions=(stored_2,),
                range_start=date(2024, 1, 4),
                range_end=date(2024, 1, 10),
            )
        )
        with self.assertRaisesRegex(
            NseArchiveResearchDatasetError, "session snapshot ids must be unique"
        ):
            _dataset_from_bindings((binding_1, binding_2))

    def test_reject_aggregate_count_mismatch(self) -> None:
        components = dict(_baseline_components())
        components["record_count"] = components["record_count"] + 1
        with self.assertRaisesRegex(
            NseArchiveResearchDatasetError, "record count is inconsistent"
        ):
            NseArchiveResearchDataset(**components)


class NseArchiveResearchDatasetExclusionTests(unittest.TestCase):
    def test_reject_malformed_exclusions(self) -> None:
        good = ResearchArchiveExclusion(
            _GAP_DAY, ResearchArchiveExclusionReason.SOURCE_ACCOUNTING_FAILED
        )

        with self.subTest("duplicated"):
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError, "sorted and unique"
            ):
                _baseline_dataset(
                    gap_before_validation=True, exclusions=(good, good)
                )

        with self.subTest("outside_envelope"):
            outside = ResearchArchiveExclusion(
                date(2023, 12, 31),
                ResearchArchiveExclusionReason.SOURCE_ACCOUNTING_FAILED,
            )
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError,
                "outside the combined calendar envelope",
            ):
                _baseline_dataset(
                    gap_before_validation=True, exclusions=(outside,)
                )

        with self.subTest("collides_with_accepted_session"):
            colliding = ResearchArchiveExclusion(
                date(2024, 1, 1), ResearchArchiveExclusionReason.SOURCE_ACCOUNTING_FAILED
            )
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError,
                "collides with an accepted session",
            ):
                _baseline_dataset(
                    gap_before_validation=True, exclusions=(colliding,)
                )

        with self.subTest("non_exact_reason_type"):
            with self.assertRaisesRegex(
                NseArchiveResearchDatasetError, "exact enum member"
            ):
                ResearchArchiveExclusion(_GAP_DAY, "SOURCE_ACCOUNTING_FAILED")

        with self.subTest("non_exact_session_type"):
            with self.assertRaisesRegex(NseArchiveResearchDatasetError, "exact date"):
                ResearchArchiveExclusion(
                    datetime(2024, 1, 3, tzinfo=UTC),
                    ResearchArchiveExclusionReason.SOURCE_ACCOUNTING_FAILED,
                )

    def test_coverage_complete_tracks_exclusions_while_other_flags_stay_fixed(
        self,
    ) -> None:
        baseline = _baseline_dataset(gap_before_validation=True)
        excluded = _baseline_dataset(
            gap_before_validation=True,
            exclusions=(
                ResearchArchiveExclusion(
                    _GAP_DAY, ResearchArchiveExclusionReason.SOURCE_ACCOUNTING_FAILED
                ),
            ),
        )
        self.assertTrue(baseline.coverage_complete)
        self.assertFalse(excluded.coverage_complete)
        for flag in (
            "collection_only",
            "actionable",
            "training_eligible",
            "feature_eligible",
            "label_eligible",
            "alert_eligible",
            "execution_eligible",
            "identity_resolution_complete",
            "corporate_action_adjustment_complete",
        ):
            self.assertEqual(getattr(baseline, flag), getattr(excluded, flag))
        self.assertTrue(baseline.collection_only)
        self.assertFalse(baseline.actionable)
        self.assertFalse(baseline.training_eligible)
        self.assertNotEqual(baseline.dataset_id, excluded.dataset_id)


class NseArchiveResearchDatasetAdversarialIdentityTests(unittest.TestCase):
    def test_tampering_a_frozen_field_is_detected_by_verify_content_identity(
        self,
    ) -> None:
        with self.subTest("exclusion"):
            exclusion = ResearchArchiveExclusion(
                _GAP_DAY, ResearchArchiveExclusionReason.SOURCE_ACCOUNTING_FAILED
            )
            object.__setattr__(exclusion, "session", date(2024, 1, 9))
            with self.assertRaises(NseArchiveResearchDatasetIntegrityError):
                exclusion.verify_content_identity()

        with self.subTest("split_policy"):
            policy = ResearchArchiveSplitPolicy(
                train_end=date(2024, 1, 21),
                validation_start=date(2024, 1, 22),
                validation_end=date(2024, 2, 11),
                test_start=date(2024, 2, 12),
                maximum_forward_label_horizon_sessions=20,
            )
            object.__setattr__(
                policy, "maximum_forward_label_horizon_sessions", 25
            )
            with self.assertRaises(NseArchiveResearchDatasetIntegrityError):
                policy.verify_content_identity()

        with self.subTest("range_binding"):
            binding = _binding(
                (date(2024, 1, 1), date(2024, 1, 2)),
                range_start=date(2024, 1, 1),
                range_end=date(2024, 1, 2),
            )
            object.__setattr__(binding, "record_count", binding.record_count + 1)
            with self.assertRaises(NseArchiveResearchDatasetIntegrityError):
                binding.verify_content_identity()

        with self.subTest("partition"):
            partition = _partition(
                ResearchSplitRole.TRAIN,
                tuple(date(2024, 1, 1) + timedelta(days=i) for i in range(21)),
            )
            object.__setattr__(partition, "role", ResearchSplitRole.VALIDATION)
            with self.assertRaises(NseArchiveResearchDatasetIntegrityError):
                partition.verify_content_identity()

        with self.subTest("dataset"):
            dataset = _baseline_dataset()
            object.__setattr__(
                dataset, "identity_issue_count", dataset.identity_issue_count + 1
            )
            # Every dataset field is now cross-checked structurally in
            # _validate(), so this no longer relies purely on the final
            # dataset_id recomputation -- it may fail at either layer.
            # Both are NseArchiveResearchDatasetError, and rejection (not
            # silent acceptance) is what this test proves.
            with self.assertRaises(NseArchiveResearchDatasetError):
                dataset.verify_content_identity()

    def test_dataset_id_changes_when_any_bound_component_changes(self) -> None:
        baseline = _baseline_dataset(gap_before_validation=True)

        variant_exclusion = _baseline_dataset(
            gap_before_validation=True,
            exclusions=(
                ResearchArchiveExclusion(
                    _GAP_DAY, ResearchArchiveExclusionReason.SOURCE_ACCOUNTING_FAILED
                ),
            ),
        )
        variant_horizon_and_boundary = _baseline_dataset(
            gap_before_validation=True, horizon=HORIZON + 1
        )
        variant_session_lineage = _baseline_dataset(
            gap_before_validation=True, shift_days=100
        )
        variant_count = _baseline_dataset(
            gap_before_validation=True, per_session_record_count=2
        )
        per_role = HORIZON + 1
        variant_profile_counts = _baseline_dataset(
            gap_before_validation=True,
            evidence_profile_counts={
                EVIDENCE_PROFILE_COMPLETE: 3 * per_role - 1,
                EVIDENCE_PROFILE_PRICE_UDIFF: 0,
                EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY: 0,
                EVIDENCE_PROFILE_UNRECONCILED: 1,
            },
            incomplete_evidence_session_count=1,
        )

        for variant in (
            variant_exclusion,
            variant_horizon_and_boundary,
            variant_session_lineage,
            variant_count,
            variant_profile_counts,
        ):
            self.assertNotEqual(baseline.dataset_id, variant.dataset_id)


class NseArchiveResearchDatasetCapabilityTests(unittest.TestCase):
    def test_module_imports_are_limited_to_a_pure_offline_allowlist(self) -> None:
        source_path = inspect.getsourcefile(nse_archive_research_dataset)
        assert source_path is not None
        tree = ast.parse(Path(source_path).read_text(encoding="utf-8"))
        allowed_roots = {
            "__future__",
            "re",
            "dataclasses",
            "datetime",
            "enum",
            "typing",
            "india_swing",
        }
        imported_roots: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imported_roots.add(alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imported_roots.add(node.module.split(".")[0])
        self.assertTrue(imported_roots.issubset(allowed_roots), imported_roots)

    def test_module_source_has_no_filesystem_network_clock_or_broker_tokens(
        self,
    ) -> None:
        source_path = inspect.getsourcefile(nse_archive_research_dataset)
        assert source_path is not None
        lowered = Path(source_path).read_text(encoding="utf-8").lower()
        forbidden_tokens = (
            "open(",
            "path(",
            "socket",
            "requests",
            "urllib",
            "http.client",
            "os.environ",
            "getenv",
            "time.time",
            "datetime.now",
            "utcnow",
            "subprocess",
            "telegram",
            "boto3",
            "google.cloud",
            "storage.client",
            ".glob(",
            "listdir",
            "iterdir",
        )
        for token in forbidden_tokens:
            self.assertNotIn(token, lowered, token)

    def test_dataset_safety_flags_are_not_caller_controllable(self) -> None:
        flag_fields = (
            "collection_only",
            "actionable",
            "training_eligible",
            "feature_eligible",
            "label_eligible",
            "alert_eligible",
            "execution_eligible",
            "identity_resolution_complete",
            "corporate_action_adjustment_complete",
            "coverage_complete",
            "dataset_id",
            "split_policy_id",
        )
        by_name = {
            value.name: value for value in dataclasses.fields(NseArchiveResearchDataset)
        }
        for name in flag_fields:
            with self.subTest(name):
                self.assertFalse(by_name[name].init)
        signature = inspect.signature(build_nse_archive_research_dataset)
        for name in flag_fields:
            self.assertNotIn(name, signature.parameters)


if __name__ == "__main__":
    unittest.main()
