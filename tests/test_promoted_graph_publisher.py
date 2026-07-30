from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing.calendar_data.materialization_store import (
    LocalCalendarMaterializationStore,
)
from india_swing.daily_pipeline.acquisition import GCSLandingObjectReader
from india_swing.market_data.promoted_session_frame import (
    PromotedSessionMarketDataFrameService,
)
from india_swing.historical_prices.promoted_history import (
    PromotedStableListingHistoryService,
)
from india_swing.identity_registry.promoted_intake import PromotedIdentityIntakeService
from india_swing.identity_decisions.promoted_materialize import (
    PromotedIdentityAdjudicationService,
)
from india_swing.promoted_graph_publisher import (
    LocalPromotedGraphPublicationStore,
    PromotedGraphPromotionBinding,
    PromotedGraphPublicationManifest,
    PromotedGraphPublicationSpec,
    PromotedGraphPublisher,
    PromotedGraphPublisherConflict,
    PromotedGraphPublisherError,
    PromotedGraphPublisherNotFound,
    PromotedGraphSessionArtifacts,
    PromotedGraphSessionBinding,
    PromotedGraphStores,
    _ReplayScope,
    _ScopedExactResolver,
    _compute_spec_id,
    build_promoted_graph_stores,
    decode_promoted_graph_publication_manifest,
    encode_promoted_graph_publication_manifest,
)
from india_swing.promoted_graph_store import (
    LocalPromotedCorporateActionAdjustmentStore,
    LocalPromotedEffectiveSessionTickStore,
    LocalPromotedIdentityAdjudicationStore,
    LocalPromotedIdentityIntakeStore,
    LocalPromotedIdentitySessionUniverseStore,
    LocalPromotedSessionMarketDataFrameStore,
    LocalPromotedSessionTickSnapshotStore,
    LocalPromotedStableListingHistoryStore,
)
from india_swing.reference_data.acquisition_join import ReferenceAcquisitionJoinService
from india_swing.reference_data.acquisition_promotion import (
    ReferenceArtifactPromotionService,
)
from india_swing.reference_data.acquisition_receipt import (
    ReferenceAcquisitionReceiptVerifier,
    TrustedReferenceAcquisitionBinding,
)
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore
from india_swing.reference_data.promotion_store import (
    LocalReferenceArtifactPromotionStore,
)
from india_swing.tick_sizes.promoted_session import PromotedSessionTickSizeService
from india_swing.universe.promoted_identity import (
    PromotedIdentitySessionUniverseService,
)
from india_swing.corporate_actions.snapshot_store import LocalCorporateActionSnapshotStore
from india_swing.identity_decisions.artifact_store import LocalIdentityReviewBundleStore
from india_swing.identity_evidence.artifact_store import LocalIdentityEvidenceArtifactStore
from india_swing.market_data.historical_corpus import (
    LocalHistoricalEvaluationCorpusStore,
)
from india_swing.calendar_data import (
    CollectionCalendarMaterialization,
    LocalCalendarSourceArtifactStore,
    materialize_collection_calendar,
)
from india_swing.calendar_data.models import CALENDAR_DECLARATION_SCHEMA_VERSION
from tests.test_promoted_corporate_action_bridge import BRIDGE_CUTOFF, _event, _snapshot
from tests.test_promoted_identity_session_universe import (
    ACQUIRER_ID,
    ADJUDICATION_CUTOFF,
    BUCKET,
    CALENDAR_CUTOFF,
    D0,
    D1,
    D2,
    SESSION_CUTOFF,
    FakeGCSObjectReader,
    _base_event,
    _filename,
    _object_name,
    _requested_url,
    _security_master_gzip,
    build_evidence,
    build_review,
    security_row,
)
from tests.test_promoted_session_market_data import FRAME_CUTOFF, _bar, _corpus
from tests.test_promoted_session_tick_sizes import TICK_CUTOFF

UTC = timezone.utc
PANEL_CUTOFF = TICK_CUTOFF + timedelta(hours=1)


class _CountingResolver:
    def __init__(self) -> None:
        self.calls = 0

    def get(self, identity: str) -> tuple[str, int]:
        self.calls += 1
        return identity, self.calls


def _build_promotion_into(
    artifact_store: LocalReferenceArtifactStore,
    root: Path,
    *,
    report_date: date,
    generation: int,
    rows: list[dict[str, str]],
    first_seen: datetime,
    validated: datetime,
):
    """Build one real promotion whose artifact is sealed into the exact
    shared ``artifact_store`` (and therefore its exact shared root), rather
    than into a private per-call archive directory. This mirrors
    ``build_promotion``'s own body exactly, except that every promotion
    built this way shares one production-style ``LocalReferenceArtifactStore``
    root, matching how ``build_promoted_graph_stores`` composes it.
    """

    gz_bytes = _security_master_gzip(rows)
    fn = _filename(report_date)
    raw_sha256 = hashlib.sha256(gz_bytes).hexdigest()
    receipt_dict = {
        "schema_version": 1,
        "dataset": "nse-cm-mii-security",
        "authority": "NSE",
        "acquirer_id": ACQUIRER_ID,
        "acquired_at": f"{report_date.isoformat()}T13:30:00Z",
        "report_date": report_date.isoformat(),
        "requested_url": _requested_url(report_date),
        "response_status": 200,
        "response_media_type": "application/gzip",
        "raw_byte_count": len(gz_bytes),
        "raw_sha256": raw_sha256,
        "landing_object": {
            "file_type": "SECURITY_MASTER",
            "bucket": BUCKET,
            "object_name": _object_name(report_date),
            "generation": generation,
            "sha256": raw_sha256,
        },
    }
    receipt_bytes = json.dumps(receipt_dict, separators=(",", ":")).encode("utf-8")
    not_before = datetime.combine(report_date, datetime.min.time(), tzinfo=UTC)
    cutoff_bound = datetime.combine(
        report_date, datetime.max.time(), tzinfo=UTC
    ).replace(microsecond=0)
    binding = TrustedReferenceAcquisitionBinding(
        expected_receipt_sha256=hashlib.sha256(receipt_bytes).hexdigest(),
        expected_raw_sha256=raw_sha256,
        allowed_bucket=BUCKET,
        target_report_date=report_date,
        not_before=not_before,
        cutoff=cutoff_bound,
        trusted_acquirer_id=ACQUIRER_ID,
    )
    receipt = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
    fake = FakeGCSObjectReader(generation=generation, content_bytes=gz_bytes)
    reader = GCSLandingObjectReader(fake)
    join = ReferenceAcquisitionJoinService(reader).join(receipt)

    source_dir = root / f"source-{report_date.isoformat()}-{generation}"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_file = source_dir / fn
    source_file.write_bytes(gz_bytes)
    calls = iter((first_seen, validated))
    artifact_store.clock = lambda: next(calls)
    artifact = artifact_store.import_security_master(source_file)
    return ReferenceArtifactPromotionService().promote(join, artifact)


def _build_calendar_into(calendar_root: Path) -> CollectionCalendarMaterialization:
    """Build one calendar whose source artifact is sealed directly into
    ``calendar_root``, matching how ``LocalCalendarMaterializationStore``
    itself resolves calendar sources (it constructs its own
    ``LocalCalendarSourceArtifactStore`` at its exact ``self.root``, not at
    any private per-document subdirectory). This mirrors ``build_calendar``/
    ``import_calendar_source``'s own body exactly, sealing only the base
    weekly-schedule source (no holiday override needed here).
    """

    source_store = LocalCalendarSourceArtifactStore(calendar_root)
    document_id = "CMTR-BASE"
    events = [_base_event()]
    validated_at = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)
    source_bytes = f"%PDF-1.7\n{document_id}\n%%EOF\n".encode("ascii")
    input_root = calendar_root / "cal_inputs"
    input_root.mkdir(parents=True, exist_ok=True)
    source_path = input_root / f"{document_id}.pdf"
    declaration_path = input_root / f"{document_id}.events.json"
    source_path.write_bytes(source_bytes)
    declaration = {
        "schema_version": CALENDAR_DECLARATION_SCHEMA_VERSION,
        "exchange": "NSE",
        "segment": "CM",
        "claimed_authority": "NSE",
        "claimed_document_id": document_id,
        "claimed_issue_date": "2026-01-01",
        "claimed_source_url": f"https://example.invalid/{document_id}.pdf",
        "source_filename": source_path.name,
        "source_media_type": "application/pdf",
        "source_byte_count": len(source_bytes),
        "source_sha256": hashlib.sha256(source_bytes).hexdigest(),
        "events": events,
    }
    declaration_path.write_text(
        json.dumps(declaration, separators=(",", ":")), encoding="utf-8"
    )
    calls = iter((validated_at - timedelta(seconds=1), validated_at))
    source_store.clock = lambda: next(calls)
    base = source_store.import_source(source_path, declaration_path)
    return materialize_collection_calendar(
        sources=(base,), coverage_start=D0, coverage_end=D2, cutoff=CALENDAR_CUTOFF
    )


def _build_fixture_and_stores(root: Path):
    """Build one full fixture (real, disk-backed evidence) and one real
    seven-root store composition, returning (stores, spec, roots).

    Both promotions are sealed into one shared ``LocalReferenceArtifactStore``
    rooted at the exact ``reference_root`` the production factory also uses,
    so the real ``LocalReferenceArtifactPromotionStore`` composed by
    ``build_promoted_graph_stores`` can independently resolve them (matching
    ``build_intake_and_adjudication``'s otherwise-identical promotion content
    is not required for that; only sharing its exact evidence-building shape
    for the RELIANCE candidate is). A small reference replay (using the exact
    same services the publisher itself calls) is used only to learn the
    resolved stable IDs needed to build a matching corporate-action snapshot
    -- the publisher under test independently redoes the entire
    materialization from the same stored promotions, calendar, and corpus.
    """

    fixture_root = root / "fixture"
    reference_root = root / "reference"
    artifact_store = LocalReferenceArtifactStore(reference_root)
    p1 = _build_promotion_into(
        artifact_store,
        fixture_root,
        report_date=D1,
        generation=100,
        rows=[security_row()],
        first_seen=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
        validated=datetime(2026, 7, 15, 12, 0, 2, tzinfo=UTC),
    )
    p2 = _build_promotion_into(
        artifact_store,
        fixture_root,
        report_date=D2,
        generation=200,
        rows=[security_row()],
        first_seen=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
        validated=datetime(2026, 7, 16, 12, 0, 2, tzinfo=UTC),
    )

    intake = PromotedIdentityIntakeService().materialize(
        promotions=(p1, p2),
        expected_report_dates=(D1, D2),
        cutoff=BRIDGE_CUTOFF,
    )
    observation_by_id = {value.observation_id: value for value in intake.observations}
    candidate_observation_ids = {
        value.candidate_id: value.observation_ids for value in intake.candidates
    }
    reliance_case = next(
        case
        for case in intake.queue.cases
        if any(
            observation_by_id[oid].ticker_symbol == "RELIANCE"
            for oid in candidate_observation_ids[case.candidate_id]
        )
    )
    status = next(
        value
        for value in intake.requirement_statuses
        if value.candidate_id == reliance_case.candidate_id
    )
    evidence = build_evidence(
        fixture_root,
        candidate_id=reliance_case.candidate_id,
        requirements=status.unresolved_requirements,
        symbol="RELIANCE",
        series="EQ",
        isin="INE002A01018",
        suffix="reliance",
    )
    review = build_review(
        fixture_root,
        queue_id=intake.queue.queue_id,
        source_registry_id=intake.source_graph_id,
        candidate_id=reliance_case.candidate_id,
        requirements=status.unresolved_requirements,
        evidence=evidence,
        suffix="reliance",
    )
    adjudication = PromotedIdentityAdjudicationService().materialize(
        intake=intake,
        evidence_artifacts=(evidence,),
        review_bundles=(review,),
        cutoff=ADJUDICATION_CUTOFF,
    )
    calendar_root = root / "calendar"
    calendar = _build_calendar_into(calendar_root)

    reference_snapshots = []
    corpora = []
    for session in (D1, D2):
        universe = PromotedIdentitySessionUniverseService().materialize(
            adjudication=adjudication,
            calendar=calendar,
            market_session=session,
            cutoff=SESSION_CUTOFF,
        )
        by_symbol = {entry.symbol: entry for entry in universe.entries}
        bars = (_bar(by_symbol["RELIANCE"], market_session=session, label=f"r-{session}"),)
        index, partition = _corpus(market_session=session, bars=bars)
        frame = PromotedSessionMarketDataFrameService().materialize(
            universe=universe, corpus_index=index, partition=partition, cutoff=FRAME_CUTOFF
        )
        snapshot = PromotedSessionTickSizeService().materialize(
            frame=frame, cutoff=TICK_CUTOFF
        )
        reference_snapshots.append(snapshot)
        corpora.append((index, partition))
    reference_history = PromotedStableListingHistoryService().materialize(
        tick_snapshots=tuple(reference_snapshots), calendar=calendar, cutoff=PANEL_CUTOFF
    )
    actions = _snapshot(_event(reference_history))

    roots = {
        "reference_root": reference_root,
        "identity_evidence_root": fixture_root / "evidence",
        "calendar_root": calendar_root,
        "daily_reports_root": root / "daily-reports",
        "historical_corpus_root": root / "historical-corpus",
        "promoted_root": root / "promoted",
        "publication_root": root / "publications",
    }
    stores = build_promoted_graph_stores(**roots)
    stores.promotions.put(p1)
    stores.promotions.put(p2)
    LocalCalendarMaterializationStore(
        roots["calendar_root"], roots["daily_reports_root"]
    ).put(calendar)
    for index, partition in corpora:
        stores.historical_corpus.put(index, (partition,))
    stores.corporate_action_snapshots.put(actions)

    evidence_ids = tuple(
        sorted(value.manifest.artifact_id for value in adjudication.evidence_artifacts)
    )
    review_ids = tuple(
        sorted(value.manifest.bundle_id for value in adjudication.review_bundles)
    )

    spec = PromotedGraphPublicationSpec(
        promotion_bindings=(
            PromotedGraphPromotionBinding(
                promotion_id=p1.promotion_id, expected_report_date=D1
            ),
            PromotedGraphPromotionBinding(
                promotion_id=p2.promotion_id, expected_report_date=D2
            ),
        ),
        identity_evidence_artifact_ids=evidence_ids,
        identity_review_bundle_ids=review_ids,
        calendar_materialization_id=calendar.materialization_id,
        session_bindings=tuple(
            PromotedGraphSessionBinding(
                market_session=session, historical_corpus_id=index.corpus_id
            )
            for session, (index, _partition) in zip((D1, D2), corpora)
        ),
        corporate_action_snapshot_id=actions.snapshot_id,
        cutoff=BRIDGE_CUTOFF,
    )
    return stores, spec, roots


class ReplayScopeTests(unittest.TestCase):
    def test_cache_is_shared_only_inside_one_outer_operation(self) -> None:
        scope = _ReplayScope()
        source = _CountingResolver()
        resolver = _ScopedExactResolver("test", source, scope)

        with scope.open():
            first = resolver.get("a" * 64)
            self.assertEqual(resolver.get("a" * 64), first)
            with scope.open():
                self.assertEqual(resolver.get("a" * 64), first)
            self.assertEqual(source.calls, 1)

        with scope.open():
            second = resolver.get("a" * 64)
            self.assertEqual(source.calls, 2)
            self.assertNotEqual(second, first)

        resolver.get("a" * 64)
        resolver.get("a" * 64)
        self.assertEqual(source.calls, 4)


class PromotedGraphPublicationSpecTests(unittest.TestCase):
    def test_promotion_bindings_must_be_sorted_unique_nonempty(self) -> None:
        with self.assertRaises(PromotedGraphPublisherError):
            PromotedGraphPublicationSpec(
                promotion_bindings=(),
                identity_evidence_artifact_ids=(),
                identity_review_bundle_ids=(),
                calendar_materialization_id="a" * 64,
                session_bindings=(
                    PromotedGraphSessionBinding(
                        market_session=D1, historical_corpus_id="b" * 64
                    ),
                ),
                corporate_action_snapshot_id="c" * 64,
                cutoff=BRIDGE_CUTOFF,
            )

    def test_conflicting_duplicate_promotion_date_is_rejected(self) -> None:
        with self.assertRaises(PromotedGraphPublisherError):
            PromotedGraphPublicationSpec(
                promotion_bindings=(
                    PromotedGraphPromotionBinding(
                        promotion_id="a" * 64, expected_report_date=D1
                    ),
                    PromotedGraphPromotionBinding(
                        promotion_id="b" * 64, expected_report_date=D1
                    ),
                ),
                identity_evidence_artifact_ids=(),
                identity_review_bundle_ids=(),
                calendar_materialization_id="a" * 64,
                session_bindings=(
                    PromotedGraphSessionBinding(
                        market_session=D1, historical_corpus_id="b" * 64
                    ),
                ),
                corporate_action_snapshot_id="c" * 64,
                cutoff=BRIDGE_CUTOFF,
            )

    def test_conflicting_duplicate_session_is_rejected(self) -> None:
        with self.assertRaises(PromotedGraphPublisherError):
            PromotedGraphPublicationSpec(
                promotion_bindings=(
                    PromotedGraphPromotionBinding(
                        promotion_id="a" * 64, expected_report_date=D1
                    ),
                    PromotedGraphPromotionBinding(
                        promotion_id="b" * 64, expected_report_date=D2
                    ),
                ),
                identity_evidence_artifact_ids=(),
                identity_review_bundle_ids=(),
                calendar_materialization_id="a" * 64,
                session_bindings=(
                    PromotedGraphSessionBinding(
                        market_session=D1, historical_corpus_id="b" * 64
                    ),
                    PromotedGraphSessionBinding(
                        market_session=D1, historical_corpus_id="c" * 64
                    ),
                ),
                corporate_action_snapshot_id="c" * 64,
                cutoff=BRIDGE_CUTOFF,
            )

    def test_naive_cutoff_is_rejected(self) -> None:
        with self.assertRaises(PromotedGraphPublisherError):
            PromotedGraphPublicationSpec(
                promotion_bindings=(
                    PromotedGraphPromotionBinding(
                        promotion_id="a" * 64, expected_report_date=D1
                    ),
                    PromotedGraphPromotionBinding(
                        promotion_id="b" * 64, expected_report_date=D2
                    ),
                ),
                identity_evidence_artifact_ids=(),
                identity_review_bundle_ids=(),
                calendar_materialization_id="a" * 64,
                session_bindings=(
                    PromotedGraphSessionBinding(
                        market_session=D1, historical_corpus_id="b" * 64
                    ),
                ),
                corporate_action_snapshot_id="c" * 64,
                cutoff=datetime(2026, 7, 17, 14, 0),
            )

    def test_spec_id_changes_with_cutoff(self) -> None:
        kwargs = dict(
            promotion_bindings=(
                PromotedGraphPromotionBinding(
                    promotion_id="a" * 64, expected_report_date=D1
                ),
                PromotedGraphPromotionBinding(
                    promotion_id="b" * 64, expected_report_date=D2
                ),
            ),
            identity_evidence_artifact_ids=(),
            identity_review_bundle_ids=(),
            calendar_materialization_id="a" * 64,
            session_bindings=(
                PromotedGraphSessionBinding(
                    market_session=D1, historical_corpus_id="b" * 64
                ),
            ),
            corporate_action_snapshot_id="c" * 64,
        )
        first = PromotedGraphPublicationSpec(
            cutoff=datetime(2026, 7, 17, 14, 0, tzinfo=UTC), **kwargs
        )
        second = PromotedGraphPublicationSpec(
            cutoff=datetime(2026, 7, 17, 15, 0, tzinfo=UTC), **kwargs
        )
        self.assertNotEqual(first.spec_id, second.spec_id)


class PromotedGraphPublisherAcceptanceTests(unittest.TestCase):
    def test_happy_path_ends_in_fresh_resolvable_terminal_roots(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, spec, roots = _build_fixture_and_stores(root)
            manifest = PromotedGraphPublisher().publish(spec, stores)

            self.assertTrue(manifest.paper_only)
            self.assertFalse(manifest.execution_eligible)
            self.assertEqual(
                tuple(value.market_session for value in manifest.session_artifacts),
                (D1, D2),
            )

            fresh_stores = build_promoted_graph_stores(**roots)
            adjustment = fresh_stores.corporate_action_adjustments.get(
                manifest.adjustment_bridge_id
            )
            self.assertEqual(adjustment.bridge_id, manifest.adjustment_bridge_id)
            effective_ticks = fresh_stores.effective_session_ticks.get(
                manifest.effective_tick_panel_id
            )
            self.assertEqual(effective_ticks.panel_id, manifest.effective_tick_panel_id)

    def test_repeated_publish_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, spec, _roots = _build_fixture_and_stores(root)
            first = PromotedGraphPublisher().publish(spec, stores)
            second = PromotedGraphPublisher().publish(spec, stores)
            self.assertEqual(first, second)
            self.assertEqual(first.manifest_id, second.manifest_id)

    def test_fresh_process_reconstruction_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, spec, roots = _build_fixture_and_stores(root)
            original = PromotedGraphPublisher().publish(spec, stores)

            fresh_stores = build_promoted_graph_stores(**roots)
            replayed = fresh_stores.publications.get(original.manifest_id)
            self.assertEqual(replayed, original)

    def test_no_terminal_manifest_when_an_intermediate_step_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, spec, _roots = _build_fixture_and_stores(root)
            # Corrupt the corporate-action-snapshot ID so the run fails late
            # (after several intermediates already exist) but before a
            # terminal manifest could ever be constructed.
            tampered_spec = PromotedGraphPublicationSpec(
                promotion_bindings=spec.promotion_bindings,
                identity_evidence_artifact_ids=spec.identity_evidence_artifact_ids,
                identity_review_bundle_ids=spec.identity_review_bundle_ids,
                calendar_materialization_id=spec.calendar_materialization_id,
                session_bindings=spec.session_bindings,
                corporate_action_snapshot_id="f" * 64,
                cutoff=spec.cutoff,
            )
            with self.assertRaises(PromotedGraphPublisherError):
                PromotedGraphPublisher().publish(tampered_spec, stores)
            manifests_dir = stores.publications.root
            if manifests_dir.exists():
                stored = [p for p in manifests_dir.glob("*.json")]
                self.assertEqual(stored, [])


class PromotedGraphPublisherRejectionTests(unittest.TestCase):
    def test_unresolvable_promotion_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, spec, _roots = _build_fixture_and_stores(root)
            tampered = PromotedGraphPublicationSpec(
                promotion_bindings=(
                    PromotedGraphPromotionBinding(
                        promotion_id="0" * 64,
                        expected_report_date=spec.promotion_bindings[0].expected_report_date,
                    ),
                    spec.promotion_bindings[1],
                ),
                identity_evidence_artifact_ids=spec.identity_evidence_artifact_ids,
                identity_review_bundle_ids=spec.identity_review_bundle_ids,
                calendar_materialization_id=spec.calendar_materialization_id,
                session_bindings=spec.session_bindings,
                corporate_action_snapshot_id=spec.corporate_action_snapshot_id,
                cutoff=spec.cutoff,
            )
            with self.assertRaises(PromotedGraphPublisherError):
                PromotedGraphPublisher().publish(tampered, stores)

    def test_unresolvable_calendar_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, spec, _roots = _build_fixture_and_stores(root)
            tampered = PromotedGraphPublicationSpec(
                promotion_bindings=spec.promotion_bindings,
                identity_evidence_artifact_ids=spec.identity_evidence_artifact_ids,
                identity_review_bundle_ids=spec.identity_review_bundle_ids,
                calendar_materialization_id="0" * 64,
                session_bindings=spec.session_bindings,
                corporate_action_snapshot_id=spec.corporate_action_snapshot_id,
                cutoff=spec.cutoff,
            )
            with self.assertRaises(PromotedGraphPublisherError):
                PromotedGraphPublisher().publish(tampered, stores)

    def test_unresolvable_corporate_action_snapshot_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, spec, _roots = _build_fixture_and_stores(root)
            tampered = PromotedGraphPublicationSpec(
                promotion_bindings=spec.promotion_bindings,
                identity_evidence_artifact_ids=spec.identity_evidence_artifact_ids,
                identity_review_bundle_ids=spec.identity_review_bundle_ids,
                calendar_materialization_id=spec.calendar_materialization_id,
                session_bindings=spec.session_bindings,
                corporate_action_snapshot_id="0" * 64,
                cutoff=spec.cutoff,
            )
            with self.assertRaises(PromotedGraphPublisherError):
                PromotedGraphPublisher().publish(tampered, stores)

    def test_unresolvable_historical_corpus_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, spec, _roots = _build_fixture_and_stores(root)
            tampered = PromotedGraphPublicationSpec(
                promotion_bindings=spec.promotion_bindings,
                identity_evidence_artifact_ids=spec.identity_evidence_artifact_ids,
                identity_review_bundle_ids=spec.identity_review_bundle_ids,
                calendar_materialization_id=spec.calendar_materialization_id,
                session_bindings=(
                    PromotedGraphSessionBinding(
                        market_session=spec.session_bindings[0].market_session,
                        historical_corpus_id="0" * 64,
                    ),
                    spec.session_bindings[1],
                ),
                corporate_action_snapshot_id=spec.corporate_action_snapshot_id,
                cutoff=spec.cutoff,
            )
            with self.assertRaises(PromotedGraphPublisherError):
                PromotedGraphPublisher().publish(tampered, stores)

    def test_zero_matching_corpus_partitions_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, spec, _roots = _build_fixture_and_stores(root)
            # The D1 binding's corpus only contains a D1 partition; claim D2
            # for it instead so zero partitions match.
            tampered = PromotedGraphPublicationSpec(
                promotion_bindings=spec.promotion_bindings,
                identity_evidence_artifact_ids=spec.identity_evidence_artifact_ids,
                identity_review_bundle_ids=spec.identity_review_bundle_ids,
                calendar_materialization_id=spec.calendar_materialization_id,
                session_bindings=(
                    PromotedGraphSessionBinding(
                        market_session=D2,
                        historical_corpus_id=spec.session_bindings[0].historical_corpus_id,
                    ),
                ),
                corporate_action_snapshot_id=spec.corporate_action_snapshot_id,
                cutoff=spec.cutoff,
            )
            with self.assertRaises(PromotedGraphPublisherError):
                PromotedGraphPublisher().publish(tampered, stores)

    def test_cutoff_before_promotion_knowledge_time_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, spec, _roots = _build_fixture_and_stores(root)
            tampered = PromotedGraphPublicationSpec(
                promotion_bindings=spec.promotion_bindings,
                identity_evidence_artifact_ids=spec.identity_evidence_artifact_ids,
                identity_review_bundle_ids=spec.identity_review_bundle_ids,
                calendar_materialization_id=spec.calendar_materialization_id,
                session_bindings=spec.session_bindings,
                corporate_action_snapshot_id=spec.corporate_action_snapshot_id,
                cutoff=datetime(2020, 1, 1, tzinfo=UTC),
            )
            with self.assertRaises(PromotedGraphPublisherError):
                PromotedGraphPublisher().publish(tampered, stores)

    def test_expected_report_date_mismatch_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, spec, _roots = _build_fixture_and_stores(root)
            tampered = PromotedGraphPublicationSpec(
                promotion_bindings=(
                    PromotedGraphPromotionBinding(
                        promotion_id=spec.promotion_bindings[0].promotion_id,
                        expected_report_date=D1 - timedelta(days=1),
                    ),
                    spec.promotion_bindings[1],
                ),
                identity_evidence_artifact_ids=spec.identity_evidence_artifact_ids,
                identity_review_bundle_ids=spec.identity_review_bundle_ids,
                calendar_materialization_id=spec.calendar_materialization_id,
                session_bindings=spec.session_bindings,
                corporate_action_snapshot_id=spec.corporate_action_snapshot_id,
                cutoff=spec.cutoff,
            )
            with self.assertRaises(PromotedGraphPublisherError):
                PromotedGraphPublisher().publish(tampered, stores)


class PromotedGraphPublicationStoreAdversarialTests(unittest.TestCase):
    def test_self_consistent_tampered_manifest_fails_on_reconstructed_lineage(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, spec, _roots = _build_fixture_and_stores(root)
            manifest = PromotedGraphPublisher().publish(spec, stores)

            substituted_snapshot_id = "f" * 64
            self.assertNotEqual(
                substituted_snapshot_id, manifest.corporate_action_snapshot_id
            )
            tampered_spec_id = _compute_spec_id(
                promotion_bindings=manifest.promotion_bindings,
                identity_evidence_artifact_ids=manifest.identity_evidence_artifact_ids,
                identity_review_bundle_ids=manifest.identity_review_bundle_ids,
                calendar_materialization_id=manifest.calendar_materialization_id,
                session_bindings=manifest.session_bindings,
                corporate_action_snapshot_id=substituted_snapshot_id,
                cutoff=manifest.cutoff,
            )
            tampered = PromotedGraphPublicationManifest(
                schema_version=manifest.schema_version,
                spec_id=tampered_spec_id,
                promotion_bindings=manifest.promotion_bindings,
                identity_evidence_artifact_ids=manifest.identity_evidence_artifact_ids,
                identity_review_bundle_ids=manifest.identity_review_bundle_ids,
                calendar_materialization_id=manifest.calendar_materialization_id,
                session_bindings=manifest.session_bindings,
                corporate_action_snapshot_id=substituted_snapshot_id,
                cutoff=manifest.cutoff,
                intake_id=manifest.intake_id,
                adjudication_id=manifest.adjudication_id,
                session_artifacts=manifest.session_artifacts,
                stable_history_panel_id=manifest.stable_history_panel_id,
                adjustment_bridge_id=manifest.adjustment_bridge_id,
                effective_tick_panel_id=manifest.effective_tick_panel_id,
                adjustment_readiness=manifest.adjustment_readiness,
                adjustment_actionable=manifest.adjustment_actionable,
                effective_tick_readiness=manifest.effective_tick_readiness,
                effective_tick_actionable=manifest.effective_tick_actionable,
                paper_only=True,
                execution_eligible=False,
            )
            # Fully self-consistent: both recomputed on construction.
            tampered.verify_content_identity()
            self.assertNotEqual(tampered.manifest_id, manifest.manifest_id)

            tampered_path = stores.publications.path_for(tampered.manifest_id)
            tampered_path.parent.mkdir(parents=True, exist_ok=True)
            tampered_path.write_bytes(
                encode_promoted_graph_publication_manifest(tampered)
            )
            with self.assertRaises(PromotedGraphPublisherConflict):
                stores.publications.get(tampered.manifest_id)

    def test_opaque_spec_id_mismatch_cannot_survive_decode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, spec, _roots = _build_fixture_and_stores(root)
            manifest = PromotedGraphPublisher().publish(spec, stores)
            path = stores.publications.path_for(manifest.manifest_id)
            decoded = json.loads(path.read_text("utf-8"))
            decoded["spec_id"] = "0" * 64
            payload = (
                json.dumps(decoded, separators=(",", ":"), sort_keys=True) + "\n"
            ).encode("utf-8")
            with self.assertRaises(PromotedGraphPublisherError):
                decode_promoted_graph_publication_manifest(payload)

    def test_tampered_output_id_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, spec, _roots = _build_fixture_and_stores(root)
            manifest = PromotedGraphPublisher().publish(spec, stores)
            path = stores.publications.path_for(manifest.manifest_id)
            decoded = json.loads(path.read_text("utf-8"))
            decoded["adjustment_bridge_id"] = "f" * 64
            payload = (
                json.dumps(decoded, separators=(",", ":"), sort_keys=True) + "\n"
            ).encode("utf-8")
            path.write_bytes(payload)
            with self.assertRaises(PromotedGraphPublisherError):
                stores.publications.get(manifest.manifest_id)

    def test_create_once_conflict_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stores, spec, _roots = _build_fixture_and_stores(root)
            manifest = PromotedGraphPublisher().publish(spec, stores)
            conflicting_store = LocalPromotedGraphPublicationStore(
                root / "conflict-publications",
                identity_intakes=stores.identity_intakes,
                identity_adjudications=stores.identity_adjudications,
                calendars=stores.calendars,
                identity_session_universes=stores.identity_session_universes,
                session_market_data_frames=stores.session_market_data_frames,
                session_tick_snapshots=stores.session_tick_snapshots,
                stable_listing_histories=stores.stable_listing_histories,
                corporate_action_adjustments=stores.corporate_action_adjustments,
                effective_session_ticks=stores.effective_session_ticks,
                replay_scope=stores._replay_scope,
            )
            path = conflicting_store.path_for(manifest.manifest_id)
            path.parent.mkdir(parents=True)
            path.write_bytes(b"{}\n")
            with self.assertRaises(PromotedGraphPublisherConflict):
                conflicting_store.put(manifest)

    def test_missing_publication_is_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            stores, _spec, _roots = _build_fixture_and_stores(Path(tmp))
            with self.assertRaises(PromotedGraphPublisherNotFound):
                stores.publications.get("0" * 64)


class PromotedGraphPublisherCapabilityTests(unittest.TestCase):
    def test_no_listing_latest_nearest_find_or_live_capability_exists(self) -> None:
        # "list" alone is deliberately excluded from the publication store's
        # own scan surface elsewhere in this module (via PromotedGraphStores),
        # since legitimate domain fields such as
        # "stable_listing_histories" contain it as a substring of "listing";
        # LocalPromotedGraphPublicationStore itself carries no such field,
        # so it can be scanned for the full discovery-verb set including
        # "list" without a false positive.
        banned_substrings = (
            "list",
            "latest",
            "nearest",
            "find",
            "network",
            "gcp",
            "broker",
            "telegram",
            "order",
            "alert",
        )
        members = [
            name
            for name in dir(LocalPromotedGraphPublicationStore)
            if not name.startswith("__")
        ]
        for name in members:
            lowered = name.lower()
            self.assertFalse(
                any(bad in lowered for bad in banned_substrings),
                f"LocalPromotedGraphPublicationStore unexpectedly exposes {name!r}",
            )
        # PromotedGraphStores is a plain composition container, not a store
        # with methods; check it only for the unambiguous discovery/live
        # verbs that cannot collide with a legitimate domain noun.
        narrower_banned = (
            "latest",
            "nearest",
            "network",
            "gcp",
            "broker",
            "telegram",
            "order",
            "alert",
        )
        for name in dir(PromotedGraphStores):
            if name.startswith("__"):
                continue
            lowered = name.lower()
            self.assertFalse(
                any(bad in lowered for bad in narrower_banned),
                f"PromotedGraphStores unexpectedly exposes {name!r}",
            )
        public_members = {
            name
            for name in dir(LocalPromotedGraphPublicationStore)
            if not name.startswith("_")
        }
        self.assertEqual(public_members, {"path_for", "put", "get"})


class BuildPromotedGraphStoresTests(unittest.TestCase):
    """Direct, unmocked coverage of the real seven-root store factory."""

    def test_real_factory_wires_every_store_to_the_intended_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            reference_root = root / "reference"
            identity_evidence_root = root / "identity-evidence"
            calendar_root = root / "calendar"
            daily_reports_root = root / "daily-reports"
            historical_corpus_root = root / "historical-corpus"
            promoted_root = root / "promoted"
            publication_root = root / "publication"

            stores = build_promoted_graph_stores(
                reference_root=reference_root,
                identity_evidence_root=identity_evidence_root,
                calendar_root=calendar_root,
                daily_reports_root=daily_reports_root,
                historical_corpus_root=historical_corpus_root,
                promoted_root=promoted_root,
                publication_root=publication_root,
            )

            self.assertIs(type(stores), PromotedGraphStores)
            self.assertIs(
                type(stores.promotions), LocalReferenceArtifactPromotionStore
            )
            self.assertEqual(stores.promotions.root, promoted_root / "promotions")
            self.assertIs(
                type(stores.promotions.artifacts), LocalReferenceArtifactStore
            )
            self.assertEqual(stores.promotions.artifacts.root, reference_root)
            self.assertIs(
                type(stores.identity_evidence), LocalIdentityEvidenceArtifactStore
            )
            self.assertEqual(stores.identity_evidence.root, identity_evidence_root)
            self.assertIs(
                type(stores.identity_reviews), LocalIdentityReviewBundleStore
            )
            self.assertEqual(stores.identity_reviews.root, identity_evidence_root)
            self.assertIs(
                type(stores.historical_corpus), LocalHistoricalEvaluationCorpusStore
            )
            self.assertEqual(stores.historical_corpus.root, historical_corpus_root)
            self.assertIs(
                type(stores.corporate_action_snapshots),
                LocalCorporateActionSnapshotStore,
            )
            self.assertEqual(
                stores.corporate_action_snapshots.root,
                promoted_root / "corporate-action-snapshots",
            )

            self.assertIs(
                type(stores.identity_intakes), LocalPromotedIdentityIntakeStore
            )
            self.assertIs(
                stores.identity_intakes.promotions.resolver, stores.promotions
            )
            self.assertIs(
                type(stores.identity_adjudications),
                LocalPromotedIdentityAdjudicationStore,
            )
            self.assertIs(
                stores.identity_adjudications.intakes.resolver,
                stores.identity_intakes,
            )
            self.assertIs(
                stores.identity_adjudications.evidence.resolver,
                stores.identity_evidence,
            )
            self.assertIs(
                stores.identity_adjudications.reviews.resolver,
                stores.identity_reviews,
            )
            self.assertIs(
                type(stores.identity_session_universes),
                LocalPromotedIdentitySessionUniverseStore,
            )
            self.assertIs(
                stores.identity_session_universes.adjudications.resolver,
                stores.identity_adjudications,
            )
            self.assertIs(
                stores.identity_session_universes.calendars.resolver,
                stores.calendars,
            )
            self.assertEqual(
                stores.calendars._store.__class__.__name__,
                "LocalCalendarMaterializationStore",
            )
            self.assertEqual(stores.calendars._store.root, calendar_root)
            self.assertEqual(
                stores.calendars._store.daily_reports_root, daily_reports_root
            )
            self.assertIs(
                type(stores.session_market_data_frames),
                LocalPromotedSessionMarketDataFrameStore,
            )
            self.assertIs(
                stores.session_market_data_frames.universes.resolver,
                stores.identity_session_universes,
            )
            self.assertIs(
                stores.session_market_data_frames.corpora.resolver,
                stores.historical_corpus,
            )
            self.assertIs(
                type(stores.session_tick_snapshots),
                LocalPromotedSessionTickSnapshotStore,
            )
            self.assertIs(
                stores.session_tick_snapshots.frames.resolver,
                stores.session_market_data_frames,
            )
            self.assertIs(
                type(stores.stable_listing_histories),
                LocalPromotedStableListingHistoryStore,
            )
            self.assertIs(
                stores.stable_listing_histories.tick_snapshots.resolver,
                stores.session_tick_snapshots,
            )
            self.assertIs(
                stores.stable_listing_histories.calendars.resolver,
                stores.calendars,
            )
            self.assertIs(
                type(stores.corporate_action_adjustments),
                LocalPromotedCorporateActionAdjustmentStore,
            )
            self.assertIs(
                stores.corporate_action_adjustments.histories.resolver,
                stores.stable_listing_histories,
            )
            self.assertIs(
                stores.corporate_action_adjustments.corporate_actions.resolver,
                stores.corporate_action_snapshots,
            )
            self.assertIs(
                type(stores.effective_session_ticks),
                LocalPromotedEffectiveSessionTickStore,
            )
            self.assertIs(
                stores.effective_session_ticks.histories.resolver,
                stores.stable_listing_histories,
            )
            self.assertIs(
                type(stores.publications), LocalPromotedGraphPublicationStore
            )
            self.assertEqual(
                stores.publications.root, publication_root / "promoted-graph-publications"
            )
            self.assertIs(
                stores.publications.corporate_action_adjustments,
                stores.corporate_action_adjustments,
            )
            self.assertIs(
                stores.publications.effective_session_ticks,
                stores.effective_session_ticks,
            )


if __name__ == "__main__":
    unittest.main()
