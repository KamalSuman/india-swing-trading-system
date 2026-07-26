from __future__ import annotations

import hashlib
from collections import Counter
from dataclasses import dataclass
from datetime import datetime

from india_swing.identity import content_id

from .backfill import HistoricalBackfillPlan, LocalHistoricalBackfillProgressStore
from .backfill_gaps import LocalHistoricalBackfillSessionGapStore
from .codec import (
    MARKET_PAYLOAD_CODEC_VERSION,
    encode_market_payload,
    market_payload_record_count,
)
from .collection import historical_dataset_name
from .dataset_admission import (
    HistoricalDatasetAdmissionReport,
    LocalHistoricalDatasetAdmissionReportStore,
    build_historical_dataset_admission_report,
)
from .gap_adjudication import (
    HistoricalGapAdjudicationReport,
    LocalHistoricalGapAdjudicationReportStore,
)
from .models import SHA256_IDENTIFIER
from .reconciliation import (
    HISTORICAL_RECONCILIATION_DATASET,
    HISTORICAL_RECONCILIATION_PROVIDER,
    HistoricalCandleReconciliationReport,
)
from .snapshot_store import (
    PAYLOAD_FILENAME,
    SNAPSHOT_SCHEMA_VERSION,
    LocalMarketSnapshotStore,
    MarketSnapshotManifest,
    StoredMarketSnapshot,
)


class HistoricalDatasetAdmissionServiceError(ValueError):
    pass


class HistoricalDatasetAdmissionServiceIntegrityError(
    HistoricalDatasetAdmissionServiceError
):
    pass


def _sha256(value: object, field_name: str) -> str:
    if type(value) is not str or SHA256_IDENTIFIER.fullmatch(value) is None:
        raise HistoricalDatasetAdmissionServiceError(
            f"{field_name} must be a lowercase SHA-256"
        )
    return value


def _sorted_unique_sha256_tuple(
    values: object, field_name: str
) -> tuple[str, ...]:
    if type(values) is not tuple or any(type(value) is not str for value in values):
        raise HistoricalDatasetAdmissionServiceError(
            f"{field_name} must be an exact tuple of strings"
        )
    for value in values:
        _sha256(value, field_name)
    if len(set(values)) != len(values):
        raise HistoricalDatasetAdmissionServiceError(
            f"{field_name} must not contain duplicates"
        )
    return tuple(sorted(values))


def _aware_utc(value: object, field_name: str) -> datetime:
    if type(value) is not datetime:
        raise HistoricalDatasetAdmissionServiceError(
            f"{field_name} must be an exact datetime"
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise HistoricalDatasetAdmissionServiceError(
            f"{field_name} must be timezone-aware"
        )
    return value


@dataclass(frozen=True, slots=True)
class HistoricalDatasetAdmissionServiceResult:
    """A sealed admission report plus a small read-only aggregate summary.

    Carries no decision of its own: every disposition and completeness flag
    is copied verbatim from the already-sealed HistoricalDatasetAdmissionReport.
    """

    report: HistoricalDatasetAdmissionReport
    disposition_counts: tuple[tuple[str, int], ...]


def _load_snapshots(
    store: LocalMarketSnapshotStore,
    *,
    dataset: str,
    snapshot_ids: tuple[str, ...],
) -> tuple[StoredMarketSnapshot, ...]:
    return tuple(store.get(dataset, snapshot_id) for snapshot_id in snapshot_ids)


def _expected_reconciliation_snapshot_id(manifest) -> str:
    return content_id(
        {
            "schema_version": manifest.schema_version,
            "codec_version": manifest.codec_version,
            "dataset": manifest.dataset,
            "selection_key": manifest.selection_key,
            "provider": manifest.provider,
            "provider_version": manifest.provider_version,
            "observed_at": manifest.observed_at,
            "record_count": manifest.record_count,
            "payload_filename": manifest.payload_filename,
            "payload_sha256": manifest.payload_sha256,
        },
        length=64,
    )


def _load_reconciliation(
    store: LocalMarketSnapshotStore, snapshot_id: str
) -> HistoricalCandleReconciliationReport:
    """Independently verify the entire injected envelope, not only its inner payload.

    The store dependency is caller-injected and may be a fake or proxy, so a
    genuinely valid HistoricalCandleReconciliationReport alone proves nothing:
    every manifest field and the canonical payload bytes must be recomputed
    and cross-checked, exactly as the approved admission builder already does
    for provider snapshots.
    """

    try:
        stored = store.get(HISTORICAL_RECONCILIATION_DATASET, snapshot_id)
        if type(stored) is not StoredMarketSnapshot:
            raise TypeError("reconciliation snapshot must be an exact StoredMarketSnapshot")
        if type(stored.manifest) is not MarketSnapshotManifest:
            raise TypeError(
                "reconciliation snapshot manifest must be an exact "
                "MarketSnapshotManifest"
            )
        if type(stored.payload_bytes) is not bytes:
            raise TypeError("reconciliation snapshot payload bytes must be exact bytes")
        payload = stored.normalized_payload
        if type(payload) is not HistoricalCandleReconciliationReport:
            raise TypeError(
                "reconciliation snapshot payload must be an exact "
                "HistoricalCandleReconciliationReport"
            )
        payload.verify_content_identity()
        manifest = stored.manifest
        if (
            manifest.schema_version != SNAPSHOT_SCHEMA_VERSION
            or manifest.codec_version != MARKET_PAYLOAD_CODEC_VERSION
            or manifest.payload_filename != PAYLOAD_FILENAME
            or manifest.snapshot_id != snapshot_id
            or manifest.dataset != HISTORICAL_RECONCILIATION_DATASET
            or manifest.selection_key != payload.historical_batch_id
            or manifest.provider != HISTORICAL_RECONCILIATION_PROVIDER
            or manifest.provider_version != payload.policy_version
            or manifest.observed_at != payload.reconciled_at
            or manifest.record_count != market_payload_record_count(payload)
        ):
            raise ValueError(
                "reconciliation snapshot manifest disagrees with its payload"
            )
        if stored.payload_bytes != encode_market_payload(payload):
            raise ValueError(
                "reconciliation snapshot payload bytes disagree with its report"
            )
        if (
            manifest.payload_sha256
            != hashlib.sha256(stored.payload_bytes).hexdigest()
        ):
            raise ValueError("reconciliation snapshot payload hash is invalid")
        if manifest.snapshot_id != _expected_reconciliation_snapshot_id(manifest):
            raise ValueError(
                "reconciliation snapshot identifier does not match its manifest"
            )
    except (TypeError, ValueError):
        raise HistoricalDatasetAdmissionServiceIntegrityError(
            "stored reconciliation snapshot failed envelope validation"
        ) from None
    return payload


class HistoricalDatasetAdmissionService:
    """Orchestrates one exact-ID admission run; makes no admission decisions.

    Every evidence object is loaded only by an exact caller-pinned ID -- never
    by listing, latest, or inference -- and handed unmodified to the approved
    build_historical_dataset_admission_report trust boundary, which alone
    decides every disposition.
    """

    def __init__(
        self,
        *,
        progress_store: LocalHistoricalBackfillProgressStore,
        snapshot_store: LocalMarketSnapshotStore,
        gap_store: LocalHistoricalBackfillSessionGapStore,
        gap_adjudication_store: LocalHistoricalGapAdjudicationReportStore,
        admission_store: LocalHistoricalDatasetAdmissionReportStore,
    ) -> None:
        self._progress_store = progress_store
        self._snapshot_store = snapshot_store
        self._gap_store = gap_store
        self._gap_adjudication_store = gap_adjudication_store
        self._admission_store = admission_store

    def run(
        self,
        *,
        plan: HistoricalBackfillPlan,
        expected_plan_id: str,
        expected_progress_id: str,
        reconciliation_snapshot_ids: tuple[str, ...],
        expected_gap_evidence_ids: tuple[str, ...],
        gap_adjudication_report_id: str | None,
        assessed_at: datetime,
    ) -> HistoricalDatasetAdmissionServiceResult:
        if type(plan) is not HistoricalBackfillPlan:
            raise HistoricalDatasetAdmissionServiceError(
                "plan must be an exact HistoricalBackfillPlan"
            )
        _sha256(expected_plan_id, "expected_plan_id")
        _sha256(expected_progress_id, "expected_progress_id")
        reconciliation_snapshot_ids = _sorted_unique_sha256_tuple(
            reconciliation_snapshot_ids, "reconciliation_snapshot_ids"
        )
        expected_gap_evidence_ids = _sorted_unique_sha256_tuple(
            expected_gap_evidence_ids, "expected_gap_evidence_ids"
        )
        if gap_adjudication_report_id is not None:
            _sha256(gap_adjudication_report_id, "gap_adjudication_report_id")
        assessed_at = _aware_utc(assessed_at, "assessed_at")

        plan.verify_content_identity()
        if plan.plan_id != expected_plan_id:
            raise HistoricalDatasetAdmissionServiceError(
                "reconstructed plan does not match expected_plan_id"
            )

        progress = self._progress_store.load(expected_plan_id)
        if progress is None:
            raise HistoricalDatasetAdmissionServiceError(
                "no backfill progress exists for the exact plan"
            )
        if progress.progress_id != expected_progress_id:
            raise HistoricalDatasetAdmissionServiceError(
                "loaded progress does not match expected_progress_id"
            )

        dataset = historical_dataset_name(plan.provider)
        snapshots = _load_snapshots(
            self._snapshot_store,
            dataset=dataset,
            snapshot_ids=tuple(
                completion.snapshot_id for completion in progress.completions
            ),
        )

        reconciliations = tuple(
            _load_reconciliation(self._snapshot_store, snapshot_id)
            for snapshot_id in reconciliation_snapshot_ids
        )

        gaps = self._gap_store.load_unresolved(expected_plan_id)
        loaded_gap_ids = tuple(sorted(value.evidence_id for value in gaps))
        if loaded_gap_ids != expected_gap_evidence_ids:
            raise HistoricalDatasetAdmissionServiceError(
                "unresolved gap evidence does not match expected_gap_evidence_ids"
            )

        gap_adjudication: HistoricalGapAdjudicationReport | None = None
        if gap_adjudication_report_id is not None:
            gap_adjudication = self._gap_adjudication_store.get(
                gap_adjudication_report_id
            )

        report = build_historical_dataset_admission_report(
            plan=plan,
            progress=progress,
            snapshots=snapshots,
            reconciliations=reconciliations,
            gaps=gaps,
            gap_adjudication=gap_adjudication,
            assessed_at=assessed_at,
        )
        stored = self._admission_store.put(report)
        disposition_counts = tuple(
            sorted(Counter(entry.disposition.value for entry in stored.entries).items())
        )
        return HistoricalDatasetAdmissionServiceResult(
            report=stored,
            disposition_counts=disposition_counts,
        )
