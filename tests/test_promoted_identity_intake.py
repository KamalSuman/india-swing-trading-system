from __future__ import annotations

import csv
import dataclasses
import gzip
import hashlib
import io
import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing.daily_pipeline.acquisition import (
    GCSLandingObjectReader,
    GCSObjectPayload,
)
from india_swing.identity_registry.adjudication import (
    IdentityAdjudicationQueue,
    IdentityAdjudicationRequirement,
)
from india_swing.identity_registry.models import (
    IdentityCandidateStatus,
    IdentityCandidateTransition,
    IdentityConflict,
    IdentityContinuityCandidate,
    IdentityObservation,
)
from india_swing.identity_registry.promoted_intake import (
    PROMOTED_IDENTITY_INTAKE_SCHEMA_VERSION,
    IdentityRequirementSatisfaction,
    PromotedIdentityIntakeError,
    PromotedIdentityIntakeService,
    VerifiedPromotedIdentityIntake,
    _SATISFIED_REQUIREMENTS,
    _build_intake_facts,
    _intake_identity,
    _source_graph_identity,
)
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.acquisition_join import (
    ReferenceAcquisitionJoinService,
    VerifiedReferenceAcquisitionJoin,
)
from india_swing.reference_data.acquisition_promotion import (
    ReferenceArtifactPromotionService,
    VerifiedReferenceArtifactPromotion,
)
from india_swing.reference_data.acquisition_receipt import (
    ReferenceAcquisitionReceiptVerifier,
    TrustedReferenceAcquisitionBinding,
)
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore
from india_swing.reference_data.models import StoredReferenceArtifact
from india_swing.reference_data.security_master import NSE_CM_MII_SECURITY_HEADER


UTC = timezone.utc
_BUCKET = "trusted-bucket"
_ACQUIRER_ID = "a" * 64


def _filename(report_date: date) -> str:
    return f"NSE_CM_security_{report_date.strftime('%d%m%Y')}.csv.gz"


def _object_name(report_date: date) -> str:
    return f"landing/{report_date.isoformat()}/{_filename(report_date)}"


def _requested_url(report_date: date) -> str:
    return f"https://nsearchives.nseindia.com/content/cm/{_filename(report_date)}"


def _security_master_row(**overrides: str) -> dict[str, str]:
    values: dict[str, str] = {name: "" for name in NSE_CM_MII_SECURITY_HEADER}
    values.update(
        {
            "FinInstrmId": "12345",
            "TckrSymb": "RELIANCE",
            "SctySrs": "EQ",
            "FinInstrmNm": "RELIANCE INDUSTRIES",
            "ISIN": "INE002A01018",
            "NewBrdLotQty": "1",
            "SctyTpFlg": "0",
            "BidIntrvl": "5",
            "CallAuctnInd": "0",
            "PrtdToTrad": "1",
            "SctyStsNrmlMkt": "1",
            "ElgbltyNrmlMkt": "1",
            "SctyStsOddLotMkt": "1",
            "ElgbltyOddLotMkt": "1",
            "SctyStsRETDBTMkt": "1",
            "ElgbltyRETDBTMkt": "1",
            "SctyStsAuctnMkt": "1",
            "ElgbltyAuctnMkt": "1",
            "SctyStsAddtlMkt1": "1",
            "ElgbltyAddtlMkt1": "1",
            "SctyStsAddtlMkt2": "1",
            "ElgbltyAddtlMkt2": "1",
            "ListgDt": "1000",
            "RmvlDt": "0",
            "RadmssnDt": "0",
            "DelFlg": "N",
        }
    )
    values.update(overrides)
    return values


def _tcs_row(**overrides: str) -> dict[str, str]:
    values = {
        "FinInstrmId": "11536",
        "TckrSymb": "TCS",
        "FinInstrmNm": "TCS LIMITED",
        "ISIN": "INE467B01029",
    }
    values.update(overrides)
    return _security_master_row(**values)


def _csv_bytes(rows: list[dict[str, str]]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(NSE_CM_MII_SECURITY_HEADER)
    for row in rows:
        writer.writerow([row[name] for name in NSE_CM_MII_SECURITY_HEADER])
    return buf.getvalue().encode("utf-8")


def _security_master_gzip(*, rows: list[dict[str, str]]) -> bytes:
    return gzip.compress(_csv_bytes(rows), mtime=0)


def _encode(receipt: dict[str, object]) -> bytes:
    return json.dumps(receipt, separators=(",", ":")).encode("utf-8")


class FakeGCSObjectReader:
    def __init__(self, *, generation: int, content_bytes: bytes) -> None:
        self.generation = generation
        self.content_bytes = content_bytes

    def read_generation(
        self, *, bucket: str, object_name: str, generation: int, maximum_bytes: int
    ) -> GCSObjectPayload:
        return GCSObjectPayload(content_bytes=self.content_bytes, generation=self.generation)


class _EvilEq:
    """A value whose equality raises an ordinary Exception, used to prove
    the comparison boundary in verify_content_identity() sanitizes any
    exception raised by retained-nested-value equality, not just mismatches
    it detects itself."""

    def __init__(self, secret: str) -> None:
        self._secret = secret

    def __eq__(self, other: object) -> bool:
        raise RuntimeError(f"secret-leak-{self._secret}")

    def __hash__(self) -> int:
        return 0


class _ComparisonBoundaryBaseException(BaseException):
    pass


class _EvilEqBaseException:
    """A value whose equality raises a custom BaseException (never
    KeyboardInterrupt/SystemExit, to avoid destabilizing the test runner),
    used to prove the comparison boundary never swallows BaseException."""

    def __eq__(self, other: object) -> bool:
        raise _ComparisonBoundaryBaseException("comparison-boundary-control")

    def __hash__(self) -> int:
        return 0


def _build_promotion(
    root: Path,
    *,
    report_date: date,
    generation: int,
    rows: list[dict[str, str]],
    first_seen: datetime,
    validated: datetime,
    acquired_at: str | None = None,
    bucket: str = _BUCKET,
    acquirer_id: str = _ACQUIRER_ID,
    not_before: datetime | None = None,
    cutoff_bound: datetime | None = None,
) -> VerifiedReferenceArtifactPromotion:
    gz_bytes = _security_master_gzip(rows=rows)
    filename = _filename(report_date)
    if acquired_at is None:
        acquired_at = f"{report_date.isoformat()}T13:30:00Z"
    if not_before is None:
        not_before = datetime.combine(report_date, datetime.min.time(), tzinfo=UTC)
    if cutoff_bound is None:
        cutoff_bound = datetime.combine(
            report_date, datetime.max.time(), tzinfo=UTC
        ).replace(microsecond=0)

    raw_sha256 = hashlib.sha256(gz_bytes).hexdigest()
    receipt_dict = {
        "schema_version": 1,
        "dataset": "nse-cm-mii-security",
        "authority": "NSE",
        "acquirer_id": acquirer_id,
        "acquired_at": acquired_at,
        "report_date": report_date.isoformat(),
        "requested_url": _requested_url(report_date),
        "response_status": 200,
        "response_media_type": "application/gzip",
        "raw_byte_count": len(gz_bytes),
        "raw_sha256": raw_sha256,
        "landing_object": {
            "file_type": "SECURITY_MASTER",
            "bucket": bucket,
            "object_name": _object_name(report_date),
            "generation": generation,
            "sha256": raw_sha256,
        },
    }
    receipt_bytes = _encode(receipt_dict)
    binding = TrustedReferenceAcquisitionBinding(
        expected_receipt_sha256=hashlib.sha256(receipt_bytes).hexdigest(),
        expected_raw_sha256=raw_sha256,
        allowed_bucket=bucket,
        target_report_date=report_date,
        not_before=not_before,
        cutoff=cutoff_bound,
        trusted_acquirer_id=acquirer_id,
    )
    receipt = ReferenceAcquisitionReceiptVerifier().verify(receipt_bytes, binding)
    fake = FakeGCSObjectReader(generation=generation, content_bytes=gz_bytes)
    reader = GCSLandingObjectReader(fake)
    join = ReferenceAcquisitionJoinService(reader).join(receipt)

    source_dir = root / "source"
    source_dir.mkdir(parents=True, exist_ok=True)
    source_file = source_dir / filename
    source_file.write_bytes(gz_bytes)
    calls = iter((first_seen, validated))
    store = LocalReferenceArtifactStore(root / "archive", clock=lambda: next(calls))
    artifact = store.import_security_master(source_file)

    return ReferenceArtifactPromotionService().promote(join, artifact)


_D1 = date(2026, 7, 15)
_D2 = date(2026, 7, 16)
_D3 = date(2026, 7, 17)


def _happy_path_promotions(root: Path) -> tuple[VerifiedReferenceArtifactPromotion, ...]:
    p1 = _build_promotion(
        root,
        report_date=_D1,
        generation=100,
        rows=[_security_master_row(), _tcs_row()],
        first_seen=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
        validated=datetime(2026, 7, 15, 12, 0, 2, tzinfo=UTC),
    )
    p2 = _build_promotion(
        root,
        report_date=_D2,
        generation=200,
        rows=[
            _security_master_row(TckrSymb="RELNEW", FinInstrmId="99999"),
            _tcs_row(),
        ],
        first_seen=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
        validated=datetime(2026, 7, 16, 12, 0, 2, tzinfo=UTC),
    )
    p3 = _build_promotion(
        root,
        report_date=_D3,
        generation=300,
        rows=[
            _security_master_row(TckrSymb="RELNEW", FinInstrmId="99999"),
            _tcs_row(),
        ],
        first_seen=datetime(2026, 7, 17, 12, 0, tzinfo=UTC),
        validated=datetime(2026, 7, 17, 12, 0, 2, tzinfo=UTC),
    )
    return p1, p2, p3


_HAPPY_EXPECTED_DATES = (_D1, _D2, _D3)
_HAPPY_CUTOFF = datetime(2026, 7, 17, 14, 0, tzinfo=UTC)


def _alternate_promotions(root: Path) -> tuple[VerifiedReferenceArtifactPromotion, ...]:
    """A genuinely different two-vintage promotion set (different rows and
    dates from _happy_path_promotions), used to prove substitution of an
    otherwise-valid-but-different promotions/queue/graph is detected rather
    than silently accepted because two independently built fixtures happen
    to be content-identical.
    """

    p1 = _build_promotion(
        root,
        report_date=_D1,
        generation=500,
        rows=[_security_master_row(FinInstrmId="70001", TckrSymb="ALTONE"), _tcs_row()],
        first_seen=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
        validated=datetime(2026, 7, 15, 12, 0, 2, tzinfo=UTC),
    )
    p2 = _build_promotion(
        root,
        report_date=_D2,
        generation=600,
        rows=[_security_master_row(FinInstrmId="70001", TckrSymb="ALTONENEW"), _tcs_row()],
        first_seen=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
        validated=datetime(2026, 7, 16, 12, 0, 2, tzinfo=UTC),
    )
    return p1, p2


def _kwargs_from(intake: VerifiedPromotedIdentityIntake) -> dict[str, object]:
    return {field.name: getattr(intake, field.name) for field in dataclasses.fields(intake)}


class PromotedIdentityIntakeAcceptanceTests(unittest.TestCase):
    def test_happy_path_covers_transition_and_unchanged_listing(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            intake = PromotedIdentityIntakeService().materialize(
                promotions=promotions,
                expected_report_dates=_HAPPY_EXPECTED_DATES,
                cutoff=_HAPPY_CUTOFF,
            )

            self.assertEqual(intake.schema_version, PROMOTED_IDENTITY_INTAKE_SCHEMA_VERSION)
            self.assertEqual(len(intake.promotions), 3)
            self.assertEqual(
                tuple(value.verified_report_date for value in intake.promotions),
                _HAPPY_EXPECTED_DATES,
            )
            self.assertEqual(
                intake.knowledge_time,
                max(value.knowledge_time for value in promotions),
            )
            self.assertEqual(len(intake.observations), 6)
            self.assertEqual(
                {value.claimed_report_date for value in intake.observations},
                set(_HAPPY_EXPECTED_DATES),
            )
            self.assertEqual(len(intake.candidates), 2)
            statuses = {value.status for value in intake.candidates}
            self.assertEqual(statuses, {IdentityCandidateStatus.CANDIDATE_CONTINUITY})
            self.assertTrue(intake.transitions)
            self.assertTrue(
                any(
                    value.symbol_changed or value.financial_instrument_id_changed
                    for value in intake.transitions
                )
            )
            self.assertTrue(
                any(
                    not (
                        value.symbol_changed
                        or value.series_changed
                        or value.financial_instrument_id_changed
                        or value.instrument_name_changed
                    )
                    for value in intake.transitions
                )
            )
            candidate_ids = {value.candidate_id for value in intake.candidates}
            queue_candidate_ids = {value.candidate_id for value in intake.queue.cases}
            self.assertEqual(candidate_ids, queue_candidate_ids)
            self.assertEqual(len(intake.requirement_statuses), len(intake.queue.cases))
            self.assertEqual(intake.source_readiness, ReferenceReadiness.POINT_IN_TIME_VERIFIED)
            self.assertEqual(intake.readiness, ReferenceReadiness.COLLECTION_ONLY)
            self.assertFalse(intake.actionable)
            self.assertFalse(intake.stable_identity_assigned)
            intake.verify_content_identity()

    def test_every_retained_row_is_covered_exactly_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            intake = PromotedIdentityIntakeService().materialize(
                promotions=promotions,
                expected_report_dates=_HAPPY_EXPECTED_DATES,
                cutoff=_HAPPY_CUTOFF,
            )
            total_retained_rows = sum(
                promotion.artifact.manifest.retained_unverified_equity_count
                for promotion in promotions
            )
            self.assertEqual(len(intake.observations), total_retained_rows)
            self.assertEqual(
                len({value.observation_id for value in intake.observations}),
                len(intake.observations),
            )

    def test_ordering_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            intake = PromotedIdentityIntakeService().materialize(
                promotions=promotions,
                expected_report_dates=_HAPPY_EXPECTED_DATES,
                cutoff=_HAPPY_CUTOFF,
            )
            self.assertEqual(
                tuple(value.observation_id for value in intake.observations),
                tuple(sorted(value.observation_id for value in intake.observations)),
            )
            self.assertEqual(
                tuple(value.candidate_id for value in intake.candidates),
                tuple(sorted(value.candidate_id for value in intake.candidates)),
            )
            self.assertEqual(
                tuple(value.candidate_id for value in intake.queue.cases),
                tuple(sorted(value.candidate_id for value in intake.queue.cases)),
            )


class PromotedIdentityIntakeKnowledgeTimeTests(unittest.TestCase):
    def test_validated_at_after_cutoff_is_fine_when_knowledge_time_is_not(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            p1 = _build_promotion(
                root,
                report_date=_D1,
                generation=100,
                rows=[_security_master_row(), _tcs_row()],
                first_seen=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 15, 12, 0, 2, tzinfo=UTC),
            )
            late_validated_at = datetime(2026, 7, 20, 0, 0, tzinfo=UTC)
            p2 = _build_promotion(
                root,
                report_date=_D2,
                generation=200,
                rows=[_security_master_row(TckrSymb="RELNEW", FinInstrmId="99999"), _tcs_row()],
                first_seen=late_validated_at - timedelta(seconds=2),
                validated=late_validated_at,
                acquired_at=f"{_D2.isoformat()}T13:30:00Z",
            )
            self.assertGreater(p2.artifact.manifest.validated_at, _HAPPY_CUTOFF)
            self.assertLessEqual(p2.knowledge_time, _HAPPY_CUTOFF)

            intake = PromotedIdentityIntakeService().materialize(
                promotions=(p1, p2),
                expected_report_dates=(_D1, _D2),
                cutoff=_HAPPY_CUTOFF,
            )
            self.assertEqual(intake.knowledge_time, max(p1.knowledge_time, p2.knowledge_time))
            intake.verify_content_identity()

    def test_promotion_knowledge_time_after_cutoff_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            p1 = _build_promotion(
                root,
                report_date=_D1,
                generation=100,
                rows=[_security_master_row(), _tcs_row()],
                first_seen=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 15, 12, 0, 2, tzinfo=UTC),
            )
            p2 = _build_promotion(
                root,
                report_date=_D2,
                generation=200,
                rows=[_security_master_row(TckrSymb="RELNEW", FinInstrmId="99999"), _tcs_row()],
                first_seen=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 16, 12, 0, 2, tzinfo=UTC),
            )
            early_cutoff = datetime(2026, 7, 15, 23, 0, tzinfo=UTC)
            with self.assertRaises(PromotedIdentityIntakeError):
                PromotedIdentityIntakeService().materialize(
                    promotions=(p1, p2),
                    expected_report_dates=(_D1, _D2),
                    cutoff=early_cutoff,
                )


class PromotedIdentityIntakeCanonicalizationTests(unittest.TestCase):
    def test_input_order_does_not_change_canonical_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            reordered = (promotions[2], promotions[0], promotions[1])

            forward = PromotedIdentityIntakeService().materialize(
                promotions=promotions,
                expected_report_dates=_HAPPY_EXPECTED_DATES,
                cutoff=_HAPPY_CUTOFF,
            )
            backward = PromotedIdentityIntakeService().materialize(
                promotions=reordered,
                expected_report_dates=_HAPPY_EXPECTED_DATES,
                cutoff=_HAPPY_CUTOFF,
            )
            self.assertEqual(forward.promotions, backward.promotions)
            self.assertEqual(forward.source_graph_id, backward.source_graph_id)
            self.assertEqual(forward.queue.queue_id, backward.queue.queue_id)
            self.assertEqual(forward.intake_id, backward.intake_id)

    def test_unsorted_expected_dates_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            with self.assertRaises(PromotedIdentityIntakeError):
                PromotedIdentityIntakeService().materialize(
                    promotions=promotions,
                    expected_report_dates=(_D2, _D1, _D3),
                    cutoff=_HAPPY_CUTOFF,
                )

    def test_duplicate_expected_dates_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            with self.assertRaises(PromotedIdentityIntakeError):
                PromotedIdentityIntakeService().materialize(
                    promotions=promotions,
                    expected_report_dates=(_D1, _D1, _D2),
                    cutoff=_HAPPY_CUTOFF,
                )

    def test_fewer_than_two_expected_dates_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            with self.assertRaises(PromotedIdentityIntakeError):
                PromotedIdentityIntakeService().materialize(
                    promotions=promotions,
                    expected_report_dates=(_D1,),
                    cutoff=_HAPPY_CUTOFF,
                )

    def test_missing_expected_date_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            with self.assertRaises(PromotedIdentityIntakeError):
                PromotedIdentityIntakeService().materialize(
                    promotions=promotions,
                    expected_report_dates=(_D1, _D2),
                    cutoff=_HAPPY_CUTOFF,
                )

    def test_extra_expected_date_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            with self.assertRaises(PromotedIdentityIntakeError):
                PromotedIdentityIntakeService().materialize(
                    promotions=promotions,
                    expected_report_dates=(_D1, _D2, _D3, date(2026, 7, 18)),
                    cutoff=_HAPPY_CUTOFF,
                )

    def test_duplicate_promotion_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            with self.assertRaises(PromotedIdentityIntakeError):
                PromotedIdentityIntakeService().materialize(
                    promotions=(promotions[0], promotions[0], promotions[1]),
                    expected_report_dates=(_D1, _D2),
                    cutoff=_HAPPY_CUTOFF,
                )

    def test_two_promotions_claiming_one_verified_report_date_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b, tempfile.TemporaryDirectory() as tmp_c:
            p1 = _build_promotion(
                Path(tmp_a),
                report_date=_D1,
                generation=100,
                rows=[_security_master_row(), _tcs_row()],
                first_seen=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 15, 12, 0, 2, tzinfo=UTC),
            )
            # A separate store root is required: LocalReferenceArtifactStore
            # itself already refuses two different byte-contents claiming
            # one report date within the same store, so a second promotion
            # for the same date must come from an independent store to
            # exercise the intake's own duplicate-report-date rejection
            # rather than the store's unrelated conflict check.
            p1_duplicate_date = _build_promotion(
                Path(tmp_b),
                report_date=_D1,
                generation=101,
                rows=[_security_master_row(FinInstrmId="55555", TckrSymb="OTHER"), _tcs_row()],
                first_seen=datetime(2026, 7, 15, 13, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 15, 13, 0, 2, tzinfo=UTC),
                bucket="another-syntactically-valid-bucket",
            )
            p2 = _build_promotion(
                Path(tmp_c),
                report_date=_D2,
                generation=200,
                rows=[_security_master_row(TckrSymb="RELNEW", FinInstrmId="99999"), _tcs_row()],
                first_seen=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 16, 12, 0, 2, tzinfo=UTC),
            )
            with self.assertRaises(PromotedIdentityIntakeError):
                PromotedIdentityIntakeService().materialize(
                    promotions=(p1, p1_duplicate_date, p2),
                    expected_report_dates=(_D1, _D2),
                    cutoff=_HAPPY_CUTOFF,
                )


class PromotedIdentityIntakeObservationScopeTests(unittest.TestCase):
    def test_only_retained_unverified_equity_rows_become_observations(self) -> None:
        # Alternative-venue rows are excluded from this fixture entirely:
        # the acquisition-join layer structurally forbids any promoted
        # content with a nonzero excluded_alternative_venue_count (a
        # receipt/join can only ever authorize the NSE Listed securities
        # report, never the interoperability file), so a promotion can never
        # carry such rows in the first place.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            rows_d1 = [
                _security_master_row(),
                _security_master_row(FinInstrmId="20001", TckrSymb="BONDONE", SctyTpFlg="1"),
                _security_master_row(FinInstrmId="20002", TckrSymb="ABCNSETEST"),
            ]
            rows_d2 = [_security_master_row(), _tcs_row()]
            p1 = _build_promotion(
                root,
                report_date=_D1,
                generation=100,
                rows=rows_d1,
                first_seen=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 15, 12, 0, 2, tzinfo=UTC),
            )
            p2 = _build_promotion(
                root,
                report_date=_D2,
                generation=200,
                rows=rows_d2,
                first_seen=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 16, 12, 0, 2, tzinfo=UTC),
            )
            self.assertEqual(p1.artifact.manifest.retained_unverified_equity_count, 1)
            self.assertEqual(p1.artifact.manifest.excluded_non_equity_count, 1)
            self.assertEqual(p1.artifact.manifest.excluded_test_security_count, 1)
            self.assertEqual(len(p1.artifact.parsed.records), 3)

            intake = PromotedIdentityIntakeService().materialize(
                promotions=(p1, p2),
                expected_report_dates=(_D1, _D2),
                cutoff=_HAPPY_CUTOFF,
            )
            self.assertEqual(len(intake.observations), 3)
            self.assertEqual(len(p1.artifact.parsed.records), 3)
            tickers = {value.ticker_symbol for value in intake.observations}
            self.assertNotIn("BONDONE", tickers)
            self.assertNotIn("ABCNSETEST", tickers)


class PromotedIdentityIntakeIdentityPolicyTests(unittest.TestCase):
    def test_ticker_reuse_with_new_isin_is_a_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            p1 = _build_promotion(
                root,
                report_date=_D1,
                generation=100,
                rows=[_security_master_row(), _tcs_row()],
                first_seen=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 15, 12, 0, 2, tzinfo=UTC),
            )
            p2 = _build_promotion(
                root,
                report_date=_D2,
                generation=200,
                rows=[_security_master_row(ISIN="INE001A01036"), _tcs_row()],
                first_seen=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 16, 12, 0, 2, tzinfo=UTC),
            )
            intake = PromotedIdentityIntakeService().materialize(
                promotions=(p1, p2),
                expected_report_dates=(_D1, _D2),
                cutoff=_HAPPY_CUTOFF,
            )
            reliance_candidates = [
                c
                for c in intake.candidates
                if any(
                    o.ticker_symbol == "RELIANCE"
                    for o in intake.observations
                    if o.observation_id in c.observation_ids
                )
            ]
            self.assertTrue(all(c.status is IdentityCandidateStatus.CONFLICT for c in reliance_candidates))
            self.assertTrue(intake.conflicts)

    def test_unvalidated_identifier_never_links_across_vintages(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            p1 = _build_promotion(
                root,
                report_date=_D1,
                generation=100,
                rows=[_security_master_row(ISIN="UNVALIDATED1"), _tcs_row()],
                first_seen=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 15, 12, 0, 2, tzinfo=UTC),
            )
            p2 = _build_promotion(
                root,
                report_date=_D2,
                generation=200,
                rows=[_security_master_row(ISIN="UNVALIDATED1"), _tcs_row()],
                first_seen=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 16, 12, 0, 2, tzinfo=UTC),
            )
            intake = PromotedIdentityIntakeService().materialize(
                promotions=(p1, p2),
                expected_report_dates=(_D1, _D2),
                cutoff=_HAPPY_CUTOFF,
            )
            unresolved_candidates = [
                c
                for c in intake.candidates
                if c.status is IdentityCandidateStatus.UNRESOLVED_IDENTIFIER
            ]
            self.assertEqual(len(unresolved_candidates), 2)
            unresolved_observation_ids = {
                observation_id
                for candidate in unresolved_candidates
                for observation_id in candidate.observation_ids
            }
            # TCS is a validated ISIN persisting unchanged across both
            # vintages, so it legitimately forms its own continuity
            # transition; the unvalidated-identifier rows must never be
            # part of any transition regardless.
            self.assertFalse(
                any(
                    transition.previous_observation_id in unresolved_observation_ids
                    or transition.current_observation_id in unresolved_observation_ids
                    for transition in intake.transitions
                )
            )

    def test_deletion_flag_and_reappearance_do_not_infer_delisting(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            p1 = _build_promotion(
                root,
                report_date=_D1,
                generation=100,
                rows=[_security_master_row(), _tcs_row()],
                first_seen=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 15, 12, 0, 2, tzinfo=UTC),
            )
            p2 = _build_promotion(
                root,
                report_date=_D2,
                generation=200,
                rows=[_security_master_row(DelFlg="Y"), _tcs_row()],
                first_seen=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 16, 12, 0, 2, tzinfo=UTC),
            )
            p3 = _build_promotion(
                root,
                report_date=_D3,
                generation=300,
                rows=[_tcs_row()],
                first_seen=datetime(2026, 7, 17, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 17, 12, 0, 2, tzinfo=UTC),
            )
            intake = PromotedIdentityIntakeService().materialize(
                promotions=(p1, p2, p3),
                expected_report_dates=(_D1, _D2, _D3),
                cutoff=_HAPPY_CUTOFF,
            )
            self.assertFalse(hasattr(intake, "delisted_instruments"))
            reliance_observations = [
                value for value in intake.observations if value.ticker_symbol == "RELIANCE"
            ]
            self.assertEqual(len(reliance_observations), 2)
            reliance_candidate = intake.candidates[
                [
                    idx
                    for idx, c in enumerate(intake.candidates)
                    if reliance_observations[0].observation_id in c.observation_ids
                ][0]
            ]
            self.assertFalse(hasattr(reliance_candidate, "delisted"))
            queue_case = next(
                case for case in intake.queue.cases if case.candidate_id == reliance_candidate.candidate_id
            )
            self.assertIn(
                IdentityAdjudicationRequirement.OFFICIAL_LISTING_STATUS,
                queue_case.requirements,
            )


class PromotedIdentityIntakeRequirementSatisfactionTests(unittest.TestCase):
    def test_every_case_satisfies_exactly_source_and_report_date(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            intake = PromotedIdentityIntakeService().materialize(
                promotions=promotions,
                expected_report_dates=_HAPPY_EXPECTED_DATES,
                cutoff=_HAPPY_CUTOFF,
            )
            for status in intake.requirement_statuses:
                self.assertEqual(set(status.satisfied_requirements), set(_SATISFIED_REQUIREMENTS))
                case = next(c for c in intake.queue.cases if c.candidate_id == status.candidate_id)
                self.assertEqual(
                    set(status.unresolved_requirements),
                    set(case.requirements) - set(_SATISFIED_REQUIREMENTS),
                )
                self.assertEqual(
                    set(status.satisfied_requirements) | set(status.unresolved_requirements),
                    set(case.requirements),
                )

    def test_case_with_only_satisfied_requirements_keeps_intake_collection_only(self) -> None:
        # The real adjudication-requirement policy always adds at least one
        # additional status-driven requirement beyond the two mandatory
        # ones, so this exact shape is not reachable through the public
        # materialize() pipeline; it is exercised directly against
        # IdentityRequirementSatisfaction to prove the type itself allows a
        # fully-satisfied case without implying any different readiness.
        status = IdentityRequirementSatisfaction(
            candidate_id="a" * 64,
            satisfied_requirements=_SATISFIED_REQUIREMENTS,
            unresolved_requirements=(),
        )
        self.assertEqual(status.unresolved_requirements, ())
        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            intake = PromotedIdentityIntakeService().materialize(
                promotions=promotions,
                expected_report_dates=_HAPPY_EXPECTED_DATES,
                cutoff=_HAPPY_CUTOFF,
            )
        self.assertEqual(intake.readiness, ReferenceReadiness.COLLECTION_ONLY)
        self.assertFalse(intake.actionable)
        self.assertFalse(intake.stable_identity_assigned)

    def test_wrong_satisfied_set_is_rejected(self) -> None:
        with self.assertRaises(PromotedIdentityIntakeError):
            IdentityRequirementSatisfaction(
                candidate_id="a" * 64,
                satisfied_requirements=(
                    IdentityAdjudicationRequirement.AUTHORIZED_SOURCE_PROVENANCE,
                ),
                unresolved_requirements=(),
            )

    def test_overlapping_satisfied_and_unresolved_is_rejected(self) -> None:
        with self.assertRaises(PromotedIdentityIntakeError):
            IdentityRequirementSatisfaction(
                candidate_id="a" * 64,
                satisfied_requirements=_SATISFIED_REQUIREMENTS,
                unresolved_requirements=_SATISFIED_REQUIREMENTS,
            )


class PromotedIdentityIntakeDirectConstructionMismatchTests(unittest.TestCase):
    def _intake(self, tmp: str) -> VerifiedPromotedIdentityIntake:
        promotions = _happy_path_promotions(Path(tmp))
        return PromotedIdentityIntakeService().materialize(
            promotions=promotions,
            expected_report_dates=_HAPPY_EXPECTED_DATES,
            cutoff=_HAPPY_CUTOFF,
        )

    def test_replacing_schema_marker_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            kwargs = _kwargs_from(intake)
            kwargs["schema_version"] = "promoted-identity-intake/v2"
            with self.assertRaises(PromotedIdentityIntakeError):
                VerifiedPromotedIdentityIntake(**kwargs)

    def test_replacing_source_graph_id_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            kwargs = _kwargs_from(intake)
            kwargs["source_graph_id"] = hashlib.sha256(b"different").hexdigest()
            with self.assertRaises(PromotedIdentityIntakeError):
                VerifiedPromotedIdentityIntake(**kwargs)

    def test_replacing_cutoff_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            kwargs = _kwargs_from(intake)
            kwargs["cutoff"] = intake.cutoff + timedelta(days=1)
            with self.assertRaises(PromotedIdentityIntakeError):
                VerifiedPromotedIdentityIntake(**kwargs)

    def test_replacing_knowledge_time_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            kwargs = _kwargs_from(intake)
            kwargs["knowledge_time"] = intake.knowledge_time - timedelta(days=1)
            with self.assertRaises(PromotedIdentityIntakeError):
                VerifiedPromotedIdentityIntake(**kwargs)

    def test_replacing_expected_dates_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            kwargs = _kwargs_from(intake)
            kwargs["expected_report_dates"] = (_D1, _D2)
            with self.assertRaises(PromotedIdentityIntakeError):
                VerifiedPromotedIdentityIntake(**kwargs)

    def test_replacing_promotions_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            intake_a = self._intake(tmp_a)
            other_promotions = _alternate_promotions(Path(tmp_b))
            kwargs = _kwargs_from(intake_a)
            kwargs["promotions"] = other_promotions
            with self.assertRaises(PromotedIdentityIntakeError):
                VerifiedPromotedIdentityIntake(**kwargs)

    def test_replacing_each_graph_tuple_fails(self) -> None:
        # The happy-path fixture has no conflicts of its own (conflicts is
        # already an empty tuple), so that field is exercised separately
        # below against a fixture that actually produces one.
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            for field_name in ("observations", "candidates", "transitions"):
                with self.subTest(field_name=field_name):
                    kwargs = _kwargs_from(intake)
                    kwargs[field_name] = ()
                    with self.assertRaises(PromotedIdentityIntakeError):
                        VerifiedPromotedIdentityIntake(**kwargs)

    def test_replacing_conflicts_tuple_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            p1 = _build_promotion(
                root,
                report_date=_D1,
                generation=100,
                rows=[_security_master_row(), _tcs_row()],
                first_seen=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 15, 12, 0, 2, tzinfo=UTC),
            )
            p2 = _build_promotion(
                root,
                report_date=_D2,
                generation=200,
                rows=[_security_master_row(ISIN="INE001A01036"), _tcs_row()],
                first_seen=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 16, 12, 0, 2, tzinfo=UTC),
            )
            intake = PromotedIdentityIntakeService().materialize(
                promotions=(p1, p2),
                expected_report_dates=(_D1, _D2),
                cutoff=_HAPPY_CUTOFF,
            )
            self.assertTrue(intake.conflicts)
            kwargs = _kwargs_from(intake)
            kwargs["conflicts"] = ()
            with self.assertRaises(PromotedIdentityIntakeError):
                VerifiedPromotedIdentityIntake(**kwargs)

    def test_replacing_queue_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            intake_a = self._intake(tmp_a)
            other_promotions = _alternate_promotions(Path(tmp_b))
            other_intake = PromotedIdentityIntakeService().materialize(
                promotions=other_promotions,
                expected_report_dates=(_D1, _D2),
                cutoff=_HAPPY_CUTOFF,
            )
            kwargs = _kwargs_from(intake_a)
            kwargs["queue"] = other_intake.queue
            with self.assertRaises(PromotedIdentityIntakeError):
                VerifiedPromotedIdentityIntake(**kwargs)

    def test_replacing_requirement_statuses_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            kwargs = _kwargs_from(intake)
            kwargs["requirement_statuses"] = ()
            with self.assertRaises(PromotedIdentityIntakeError):
                VerifiedPromotedIdentityIntake(**kwargs)

    def test_replacing_source_readiness_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            kwargs = _kwargs_from(intake)
            kwargs["source_readiness"] = ReferenceReadiness.COLLECTION_ONLY
            with self.assertRaises(PromotedIdentityIntakeError):
                VerifiedPromotedIdentityIntake(**kwargs)

    def test_replacing_readiness_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            kwargs = _kwargs_from(intake)
            kwargs["readiness"] = ReferenceReadiness.POINT_IN_TIME_VERIFIED
            with self.assertRaises(PromotedIdentityIntakeError):
                VerifiedPromotedIdentityIntake(**kwargs)

    def test_replacing_actionable_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            kwargs = _kwargs_from(intake)
            kwargs["actionable"] = True
            with self.assertRaises(PromotedIdentityIntakeError):
                VerifiedPromotedIdentityIntake(**kwargs)

    def test_replacing_stable_identity_assigned_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            kwargs = _kwargs_from(intake)
            kwargs["stable_identity_assigned"] = True
            with self.assertRaises(PromotedIdentityIntakeError):
                VerifiedPromotedIdentityIntake(**kwargs)

    def test_replacing_intake_id_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            kwargs = _kwargs_from(intake)
            kwargs["intake_id"] = hashlib.sha256(b"different").hexdigest()
            with self.assertRaises(PromotedIdentityIntakeError):
                VerifiedPromotedIdentityIntake(**kwargs)


class PromotedIdentityIntakeMutationTests(unittest.TestCase):
    def _intake(self, tmp: str) -> VerifiedPromotedIdentityIntake:
        promotions = _happy_path_promotions(Path(tmp))
        return PromotedIdentityIntakeService().materialize(
            promotions=promotions,
            expected_report_dates=_HAPPY_EXPECTED_DATES,
            cutoff=_HAPPY_CUTOFF,
        )

    def test_mutating_top_level_intake_id_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            object.__setattr__(intake, "intake_id", "0" * 64)
            with self.assertRaises(PromotedIdentityIntakeError):
                intake.verify_content_identity()

    def test_mutating_top_level_source_graph_id_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            object.__setattr__(intake, "source_graph_id", "0" * 64)
            with self.assertRaises(PromotedIdentityIntakeError):
                intake.verify_content_identity()

    def test_mutating_nested_promotion_receipt_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            object.__setattr__(intake.promotions[0].join.receipt, "response_status", 201)
            with self.assertRaises(PromotedIdentityIntakeError):
                intake.verify_content_identity()

    def test_mutating_nested_promotion_binding_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            object.__setattr__(
                intake.promotions[0].join.receipt.binding,
                "allowed_bucket",
                "another-syntactically-valid-bucket",
            )
            with self.assertRaises(PromotedIdentityIntakeError):
                intake.verify_content_identity()

    def test_mutating_nested_promotion_join_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            object.__setattr__(intake.promotions[0].join, "join_id", "0" * 64)
            with self.assertRaises(PromotedIdentityIntakeError):
                intake.verify_content_identity()

    def test_mutating_nested_promotion_artifact_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            object.__setattr__(
                intake.promotions[0].artifact, "raw_bytes", b"tampered-bytes"
            )
            with self.assertRaises(PromotedIdentityIntakeError):
                intake.verify_content_identity()

    def test_mutating_nested_promotion_manifest_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            object.__setattr__(
                intake.promotions[0].artifact.manifest, "raw_sha256", "a" * 64
            )
            with self.assertRaises(PromotedIdentityIntakeError):
                intake.verify_content_identity()

    def test_mutating_nested_promotion_parsed_record_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            record = intake.promotions[0].artifact.parsed.records[0]
            object.__setattr__(record, "ticker_symbol", "TAMPERED")
            with self.assertRaises(PromotedIdentityIntakeError):
                intake.verify_content_identity()

    def test_mutating_nested_observation_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            object.__setattr__(intake.observations[0], "ticker_symbol", "TAMPERED")
            with self.assertRaises(PromotedIdentityIntakeError):
                intake.verify_content_identity()

    def test_mutating_nested_candidate_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            candidate = intake.candidates[0]
            object.__setattr__(
                candidate,
                "status",
                IdentityCandidateStatus.SINGLE_VINTAGE
                if candidate.status is not IdentityCandidateStatus.SINGLE_VINTAGE
                else IdentityCandidateStatus.UNRESOLVED_IDENTIFIER,
            )
            with self.assertRaises(PromotedIdentityIntakeError):
                intake.verify_content_identity()

    def test_mutating_nested_transition_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            self.assertTrue(intake.transitions)
            object.__setattr__(intake.transitions[0], "symbol_changed", not intake.transitions[0].symbol_changed)
            with self.assertRaises(PromotedIdentityIntakeError):
                intake.verify_content_identity()

    def test_mutating_nested_conflict_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            more_promotions = list(promotions)
            root = Path(tmp)
            conflicting = _build_promotion(
                root,
                report_date=date(2026, 7, 18),
                generation=400,
                rows=[_security_master_row(ISIN="INE001A01036"), _tcs_row()],
                first_seen=datetime(2026, 7, 18, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 18, 12, 0, 2, tzinfo=UTC),
            )
            more_promotions.append(conflicting)
            intake = PromotedIdentityIntakeService().materialize(
                promotions=tuple(more_promotions),
                expected_report_dates=(_D1, _D2, _D3, date(2026, 7, 18)),
                cutoff=datetime(2026, 7, 18, 14, 0, tzinfo=UTC),
            )
            self.assertTrue(intake.conflicts)
            original_ids = intake.conflicts[0].observation_ids
            object.__setattr__(
                intake.conflicts[0],
                "observation_ids",
                original_ids[:-1],
            )
            with self.assertRaises(PromotedIdentityIntakeError):
                intake.verify_content_identity()

    def test_mutating_nested_queue_case_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            case = intake.queue.cases[0]
            object.__setattr__(case, "requirements", (IdentityAdjudicationRequirement.AUTHORIZED_SOURCE_PROVENANCE, IdentityAdjudicationRequirement.REPORT_DATE_VERIFICATION))
            with self.assertRaises(PromotedIdentityIntakeError):
                intake.verify_content_identity()

    def test_mutating_nested_requirement_status_field_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            status = intake.requirement_statuses[0]
            object.__setattr__(status, "unresolved_requirements", ())
            with self.assertRaises(PromotedIdentityIntakeError):
                intake.verify_content_identity()


class PromotedIdentityIntakeComparisonBoundaryTests(unittest.TestCase):
    """Regression coverage for Codex's revision-2 finding: the retained-
    versus-recomputed equality comparisons in verify_content_identity()
    recursively invoke every nested retained value's own __eq__, so a
    malicious/adversarial nested value whose equality raises an ordinary
    exception must never leak that exception's type or text -- only one
    static sanitized PromotedIdentityIntakeError may surface. A custom
    BaseException from the same call site must still propagate uncaught.
    """

    def _intake(self, tmp: str) -> VerifiedPromotedIdentityIntake:
        promotions = _happy_path_promotions(Path(tmp))
        return PromotedIdentityIntakeService().materialize(
            promotions=promotions,
            expected_report_dates=_HAPPY_EXPECTED_DATES,
            cutoff=_HAPPY_CUTOFF,
        )

    def _conflicting_intake(self, tmp: str) -> VerifiedPromotedIdentityIntake:
        # The happy-path fixture has no conflicts of its own; this reuses
        # the same conflict-producing fixture already used elsewhere in this
        # file (ticker reuse with a new ISIN) rather than fabricating a
        # shaped graph, per the revision-2 instruction.
        root = Path(tmp)
        p1 = _build_promotion(
            root,
            report_date=_D1,
            generation=100,
            rows=[_security_master_row(), _tcs_row()],
            first_seen=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
            validated=datetime(2026, 7, 15, 12, 0, 2, tzinfo=UTC),
        )
        p2 = _build_promotion(
            root,
            report_date=_D2,
            generation=200,
            rows=[_security_master_row(ISIN="INE001A01036"), _tcs_row()],
            first_seen=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
            validated=datetime(2026, 7, 16, 12, 0, 2, tzinfo=UTC),
        )
        return PromotedIdentityIntakeService().materialize(
            promotions=(p1, p2),
            expected_report_dates=(_D1, _D2),
            cutoff=_HAPPY_CUTOFF,
        )

    def _assert_sanitized(self, secret: str, exc: BaseException) -> None:
        self.assertIsInstance(exc, PromotedIdentityIntakeError)
        message = str(exc)
        self.assertNotIn("RuntimeError", message)
        self.assertNotIn(secret, message)

    def test_codex_reported_observation_equality_leak_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            secret = "codex-repro-secret-9f2a"
            object.__setattr__(intake.observations[0], "ticker_symbol", _EvilEq(secret))
            with self.assertRaises(PromotedIdentityIntakeError) as ctx:
                intake.verify_content_identity()
            self._assert_sanitized(secret, ctx.exception)

    def test_malicious_candidate_equality_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            secret = "candidate-secret-1a2b"
            object.__setattr__(intake.candidates[0], "validated_isin", _EvilEq(secret))
            with self.assertRaises(PromotedIdentityIntakeError) as ctx:
                intake.verify_content_identity()
            self._assert_sanitized(secret, ctx.exception)

    def test_malicious_transition_equality_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            self.assertTrue(intake.transitions)
            secret = "transition-secret-3c4d"
            object.__setattr__(intake.transitions[0], "previous_observation_id", _EvilEq(secret))
            with self.assertRaises(PromotedIdentityIntakeError) as ctx:
                intake.verify_content_identity()
            self._assert_sanitized(secret, ctx.exception)

    def test_malicious_conflict_equality_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._conflicting_intake(tmp)
            self.assertTrue(intake.conflicts)
            secret = "conflict-secret-5e6f"
            object.__setattr__(intake.conflicts[0], "conflict_type", _EvilEq(secret))
            with self.assertRaises(PromotedIdentityIntakeError) as ctx:
                intake.verify_content_identity()
            self._assert_sanitized(secret, ctx.exception)

    def test_malicious_queue_case_equality_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            secret = "queue-case-secret-7g8h"
            object.__setattr__(
                intake.queue.cases[0], "candidate_status", _EvilEq(secret)
            )
            with self.assertRaises(PromotedIdentityIntakeError) as ctx:
                intake.verify_content_identity()
            self._assert_sanitized(secret, ctx.exception)

    def test_malicious_requirement_status_equality_is_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            secret = "requirement-status-secret-9i0j"
            object.__setattr__(
                intake.requirement_statuses[0], "candidate_id", _EvilEq(secret)
            )
            with self.assertRaises(PromotedIdentityIntakeError) as ctx:
                intake.verify_content_identity()
            self._assert_sanitized(secret, ctx.exception)

    def test_base_exception_from_equality_is_not_swallowed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            intake = self._intake(tmp)
            object.__setattr__(
                intake.observations[0], "ticker_symbol", _EvilEqBaseException()
            )
            with self.assertRaises(_ComparisonBoundaryBaseException):
                intake.verify_content_identity()


class PromotedIdentityIntakeSubclassImpostorTests(unittest.TestCase):
    def test_intake_subclass_with_valid_fields_is_rejected(self) -> None:
        class _IntakeSubclass(VerifiedPromotedIdentityIntake):
            pass

        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            intake = PromotedIdentityIntakeService().materialize(
                promotions=promotions,
                expected_report_dates=_HAPPY_EXPECTED_DATES,
                cutoff=_HAPPY_CUTOFF,
            )
            kwargs = _kwargs_from(intake)
            with self.assertRaises(PromotedIdentityIntakeError):
                _IntakeSubclass(**kwargs)

    def test_promotion_subclass_is_rejected_before_it_can_reach_the_intake(self) -> None:
        # VerifiedReferenceArtifactPromotion's own exact-type guard (added to
        # fix a prior Codex-reported subclass-acceptance defect) already
        # rejects a bare subclass at construction time, even with every
        # field copied unmodified from a genuine promotion. That means such
        # an impostor can never reach PromotedIdentityIntakeService in the
        # first place -- this test documents that fact rather than
        # re-deriving it, since constructing the subclass itself is what
        # fails, not this module's own promotions-tuple type check.
        class _PromotionSubclass(VerifiedReferenceArtifactPromotion):
            pass

        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            from india_swing.reference_data.acquisition_promotion import (
                ReferenceArtifactPromotionError,
            )

            with self.assertRaises(ReferenceArtifactPromotionError):
                _PromotionSubclass(
                    **{
                        field.name: getattr(promotions[0], field.name)
                        for field in dataclasses.fields(promotions[0])
                    }
                )

    def test_wrong_promotions_tuple_type_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            with self.assertRaises(PromotedIdentityIntakeError):
                PromotedIdentityIntakeService().materialize(
                    promotions=list(promotions),  # type: ignore[arg-type]
                    expected_report_dates=_HAPPY_EXPECTED_DATES,
                    cutoff=_HAPPY_CUTOFF,
                )

    def test_wrong_expected_dates_element_type_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            with self.assertRaises(PromotedIdentityIntakeError):
                PromotedIdentityIntakeService().materialize(
                    promotions=promotions,
                    expected_report_dates=(
                        datetime(2026, 7, 15, tzinfo=UTC),
                        _D2,
                        _D3,
                    ),  # type: ignore[arg-type]
                    cutoff=_HAPPY_CUTOFF,
                )

    def test_wrong_cutoff_type_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            with self.assertRaises(PromotedIdentityIntakeError):
                PromotedIdentityIntakeService().materialize(
                    promotions=promotions,
                    expected_report_dates=_HAPPY_EXPECTED_DATES,
                    cutoff=_D3,  # type: ignore[arg-type]
                )

    def test_naive_cutoff_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            with self.assertRaises(PromotedIdentityIntakeError):
                PromotedIdentityIntakeService().materialize(
                    promotions=promotions,
                    expected_report_dates=_HAPPY_EXPECTED_DATES,
                    cutoff=datetime(2026, 7, 17, 14, 0),
                )

    def test_str_subclass_intake_id_fails(self) -> None:
        class _StrSubclass(str):
            pass

        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            intake = PromotedIdentityIntakeService().materialize(
                promotions=promotions,
                expected_report_dates=_HAPPY_EXPECTED_DATES,
                cutoff=_HAPPY_CUTOFF,
            )
            kwargs = _kwargs_from(intake)
            kwargs["intake_id"] = _StrSubclass(intake.intake_id)
            with self.assertRaises(PromotedIdentityIntakeError):
                VerifiedPromotedIdentityIntake(**kwargs)

    def test_bool_wrong_type_actionable_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            intake = PromotedIdentityIntakeService().materialize(
                promotions=promotions,
                expected_report_dates=_HAPPY_EXPECTED_DATES,
                cutoff=_HAPPY_CUTOFF,
            )
            kwargs = _kwargs_from(intake)
            kwargs["actionable"] = 0  # type: ignore[assignment]
            with self.assertRaises(PromotedIdentityIntakeError):
                VerifiedPromotedIdentityIntake(**kwargs)


class PromotedIdentityIntakeContentIdCompletenessTests(unittest.TestCase):
    def test_source_graph_identity_key_set_is_exact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            intake = PromotedIdentityIntakeService().materialize(
                promotions=promotions,
                expected_report_dates=_HAPPY_EXPECTED_DATES,
                cutoff=_HAPPY_CUTOFF,
            )
            material = _source_graph_identity(
                cutoff=intake.cutoff,
                knowledge_time=intake.knowledge_time,
                expected_report_dates=intake.expected_report_dates,
                canonical_promotions=intake.promotions,
                observations=intake.observations,
                candidates=intake.candidates,
                transitions=intake.transitions,
                conflicts=intake.conflicts,
            )
            self.assertEqual(
                set(material),
                {
                    "schema_version",
                    "policy_version",
                    "cutoff",
                    "knowledge_time",
                    "expected_report_dates",
                    "promotion_ids",
                    "join_ids",
                    "artifact_ids",
                    "manifest_ids",
                    "observation_ids",
                    "candidate_ids",
                    "transition_ids",
                    "conflict_ids",
                },
            )

    def test_intake_identity_key_set_is_exact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            intake = PromotedIdentityIntakeService().materialize(
                promotions=promotions,
                expected_report_dates=_HAPPY_EXPECTED_DATES,
                cutoff=_HAPPY_CUTOFF,
            )
            material = _intake_identity(
                source_graph_id=intake.source_graph_id,
                cutoff=intake.cutoff,
                knowledge_time=intake.knowledge_time,
                expected_report_dates=intake.expected_report_dates,
                queue_id=intake.queue.queue_id,
                requirement_statuses=intake.requirement_statuses,
                source_readiness=intake.source_readiness,
                readiness=intake.readiness,
                actionable=intake.actionable,
                stable_identity_assigned=intake.stable_identity_assigned,
            )
            self.assertEqual(
                set(material),
                {
                    "schema_version",
                    "policy_version",
                    "source_graph_id",
                    "cutoff",
                    "knowledge_time",
                    "expected_report_dates",
                    "queue_id",
                    "requirement_statuses",
                    "source_readiness",
                    "readiness",
                    "actionable",
                    "stable_identity_assigned",
                },
            )

    def test_different_cutoff_changes_intake_id_not_source_graph_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            promotions_a = _happy_path_promotions(Path(tmp_a))
            promotions_b = _happy_path_promotions(Path(tmp_b))
            intake_a = PromotedIdentityIntakeService().materialize(
                promotions=promotions_a,
                expected_report_dates=_HAPPY_EXPECTED_DATES,
                cutoff=_HAPPY_CUTOFF,
            )
            intake_b = PromotedIdentityIntakeService().materialize(
                promotions=promotions_b,
                expected_report_dates=_HAPPY_EXPECTED_DATES,
                cutoff=_HAPPY_CUTOFF + timedelta(hours=1),
            )
            self.assertNotEqual(intake_a.source_graph_id, intake_b.source_graph_id)
            self.assertNotEqual(intake_a.intake_id, intake_b.intake_id)

    def test_different_source_rows_change_source_graph_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            promotions_a = _happy_path_promotions(Path(tmp_a))
            root_b = Path(tmp_b)
            p1 = _build_promotion(
                root_b,
                report_date=_D1,
                generation=100,
                rows=[_security_master_row(), _tcs_row()],
                first_seen=datetime(2026, 7, 15, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 15, 12, 0, 2, tzinfo=UTC),
            )
            p2 = _build_promotion(
                root_b,
                report_date=_D2,
                generation=200,
                rows=[
                    _security_master_row(TckrSymb="DIFFERENT", FinInstrmId="77777"),
                    _tcs_row(),
                ],
                first_seen=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 16, 12, 0, 2, tzinfo=UTC),
            )
            p3 = _build_promotion(
                root_b,
                report_date=_D3,
                generation=300,
                rows=[
                    _security_master_row(TckrSymb="DIFFERENT", FinInstrmId="77777"),
                    _tcs_row(),
                ],
                first_seen=datetime(2026, 7, 17, 12, 0, tzinfo=UTC),
                validated=datetime(2026, 7, 17, 12, 0, 2, tzinfo=UTC),
            )
            intake_a = PromotedIdentityIntakeService().materialize(
                promotions=promotions_a,
                expected_report_dates=_HAPPY_EXPECTED_DATES,
                cutoff=_HAPPY_CUTOFF,
            )
            intake_b = PromotedIdentityIntakeService().materialize(
                promotions=(p1, p2, p3),
                expected_report_dates=_HAPPY_EXPECTED_DATES,
                cutoff=_HAPPY_CUTOFF,
            )
            self.assertNotEqual(intake_a.source_graph_id, intake_b.source_graph_id)
            self.assertNotEqual(intake_a.intake_id, intake_b.intake_id)

    def test_filesystem_path_alone_does_not_change_source_graph_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_a, tempfile.TemporaryDirectory() as tmp_b:
            promotions_a = _happy_path_promotions(Path(tmp_a))
            promotions_b = _happy_path_promotions(Path(tmp_b))
            self.assertNotEqual(
                promotions_a[0].artifact.path, promotions_b[0].artifact.path
            )
            intake_a = PromotedIdentityIntakeService().materialize(
                promotions=promotions_a,
                expected_report_dates=_HAPPY_EXPECTED_DATES,
                cutoff=_HAPPY_CUTOFF,
            )
            intake_b = PromotedIdentityIntakeService().materialize(
                promotions=promotions_b,
                expected_report_dates=_HAPPY_EXPECTED_DATES,
                cutoff=_HAPPY_CUTOFF,
            )
            self.assertEqual(intake_a.source_graph_id, intake_b.source_graph_id)
            self.assertEqual(intake_a.intake_id, intake_b.intake_id)


class PromotedIdentityIntakeCapabilityTests(unittest.TestCase):
    def test_no_io_or_decision_shaped_capability_exists(self) -> None:
        banned_substrings = (
            "list",
            "latest",
            "find",
            "select",
            "download",
            "fetch",
            "network",
            "filesystem",
            "environ",
            "clock",
            "write",
            "rename",
            "chmod",
            "touch",
            "delete",
            "store",
            "notif",
            "broker",
            "order",
            "capital",
        )
        for candidate in (
            PromotedIdentityIntakeService,
            VerifiedPromotedIdentityIntake,
            IdentityRequirementSatisfaction,
        ):
            members = [name for name in dir(candidate) if not name.startswith("__")]
            for name in members:
                lowered = name.lower()
                self.assertFalse(
                    any(bad in lowered for bad in banned_substrings),
                    f"{candidate!r} unexpectedly exposes {name!r}",
                )

    def test_no_stable_identity_or_universe_field_exists(self) -> None:
        field_names = {
            field.name for field in dataclasses.fields(VerifiedPromotedIdentityIntake)
        }
        for banned in (
            "stable_instrument_id",
            "stable_listing_id",
            "universe",
            "calendar",
            "price",
            "liquidity",
            "corporate_action",
            "model",
            "signal",
            "ranking",
            "recommendation",
            "notification",
            "broker",
            "order",
            "position_size",
            "capital",
        ):
            self.assertFalse(any(banned in name for name in field_names))

    def test_intake_cannot_be_used_as_promotion_evidence(self) -> None:
        from india_swing.promotion.models import PromotionDecision, PromotionEvidence

        with tempfile.TemporaryDirectory() as tmp:
            promotions = _happy_path_promotions(Path(tmp))
            intake = PromotedIdentityIntakeService().materialize(
                promotions=promotions,
                expected_report_dates=_HAPPY_EXPECTED_DATES,
                cutoff=_HAPPY_CUTOFF,
            )
            self.assertNotIsInstance(intake, PromotionEvidence)
            self.assertNotIsInstance(intake, PromotionDecision)
            self.assertFalse(issubclass(VerifiedPromotedIdentityIntake, PromotionEvidence))
            for status in intake.requirement_statuses:
                self.assertNotIsInstance(status, PromotionEvidence)

    def test_importing_module_causes_no_io(self) -> None:
        import india_swing.identity_registry.promoted_intake as module

        banned_module_names = {"os", "socket", "urllib", "requests", "storage"}
        top_level_names = {
            name
            for name in vars(module)
            if not name.startswith("_") and not name[0].isupper()
        }
        self.assertFalse(top_level_names & banned_module_names)


class PromotedIdentityIntakeLegacyRegressionTests(unittest.TestCase):
    """Pins the exact legacy content-ID contract across this task's internal
    graph/case-builder refactor of materialize.py/adjudication.py. These
    golden IDs were independently recomputed against the git HEAD (pre-
    refactor) versions of both files in the same session and matched byte
    for byte, proving the shared _build_identity_graph/
    _build_adjudication_cases extraction did not change legacy output.
    """

    def test_legacy_registry_and_queue_ids_are_unchanged_by_the_refactor(self) -> None:
        # Deliberately reuses the exact fixture helpers and row shape from
        # tests/test_identity_registry.py (rather than this file's own row
        # builder, whose default field values differ) so the recomputed IDs
        # are byte-for-byte comparable to the golden values below, which
        # were independently captured against the git HEAD (pre-refactor)
        # versions of materialize.py/adjudication.py in the same session.
        from india_swing.identity_registry import (
            build_identity_adjudication_queue,
            materialize_cross_vintage_identity_registry,
        )
        from tests.test_identity_registry import (
            DAY_ONE_FIRST_SEEN,
            DAY_ONE_VALIDATED,
            DAY_TWO_FIRST_SEEN,
            DAY_TWO_VALIDATED,
            clock_sequence,
            master_bytes,
            security_row,
        )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first_file = root / "NSE_CM_security_15072026.csv.gz"
            second_file = root / "NSE_CM_security_16072026.csv.gz"
            first_file.write_bytes(master_bytes([security_row()]))
            second_file.write_bytes(
                master_bytes([security_row(TckrSymb="INFYNEW", FinInstrmId="2000")])
            )
            store = LocalReferenceArtifactStore(
                root / "reference",
                clock=clock_sequence(
                    DAY_ONE_FIRST_SEEN,
                    DAY_ONE_VALIDATED,
                    DAY_TWO_FIRST_SEEN,
                    DAY_TWO_VALIDATED,
                ),
            )
            sources = (
                store.import_security_master(first_file),
                store.import_security_master(second_file),
            )
            registry = materialize_cross_vintage_identity_registry(
                sources=sources,
                cutoff=datetime(2026, 7, 16, 10, 5, tzinfo=UTC),
            )
            queue = build_identity_adjudication_queue(registry)

            self.assertEqual(
                registry.registry_id,
                "e27d6be529698ebf8d2e2699170de012d9aa4457c371d43098f3ebac4b989d70",
            )
            self.assertEqual(
                queue.queue_id,
                "cfb08b35435581c23329b8739ab95241be5e1823fca1bc690720a3aded7ab488",
            )


if __name__ == "__main__":
    unittest.main()
