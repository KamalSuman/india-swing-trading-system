from __future__ import annotations

from datetime import datetime, timezone

from india_swing.reference_data import SourceRowDisposition, StoredReferenceArtifact
from india_swing.reference_data.artifact_store import (
    verify_stored_reference_provenance,
)

from .models import (
    CollectedUniverseObservation,
    CollectionUniverseDisposition,
    CollectionUniverseSnapshot,
)


_DISPOSITIONS = {
    SourceRowDisposition.RETAINED_UNVERIFIED_EQUITY: (
        CollectionUniverseDisposition.IN_SCOPE_UNVERIFIED_EQUITY
    ),
    SourceRowDisposition.EXCLUDED_NON_EQUITY: (
        CollectionUniverseDisposition.EXCLUDED_NON_EQUITY
    ),
    SourceRowDisposition.EXCLUDED_TEST_SECURITY: (
        CollectionUniverseDisposition.EXCLUDED_TEST_SECURITY
    ),
    SourceRowDisposition.EXCLUDED_ALTERNATIVE_VENUE: (
        CollectionUniverseDisposition.EXCLUDED_ALTERNATIVE_VENUE
    ),
}


def materialize_collection_universe(
    source: StoredReferenceArtifact,
    *,
    cutoff: datetime,
    calendar_snapshot_id: str,
) -> CollectionUniverseSnapshot:
    if type(source) is not StoredReferenceArtifact:
        raise TypeError("universe source must be an exact stored reference artifact")
    verify_stored_reference_provenance(source)
    if not isinstance(cutoff, datetime):
        raise TypeError("universe cutoff must be a datetime")
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise ValueError("universe cutoff must be timezone-aware")
    cutoff = cutoff.astimezone(timezone.utc)
    manifest = source.manifest
    if manifest.validated_at > cutoff:
        raise ValueError("security master was unavailable at the universe cutoff")

    observations = []
    for record in source.parsed.records:
        disposition = _DISPOSITIONS[record.disposition]
        normal_market = record.market_eligibility[0]
        observations.append(
            CollectedUniverseObservation(
                market_session_claim=manifest.claimed_report_date,
                knowledge_time=manifest.validated_at,
                source_artifact_id=manifest.artifact_id,
                source_manifest_id=manifest.manifest_id,
                source_record_id=record.source_record_id,
                financial_instrument_id=record.financial_instrument_id,
                symbol=record.ticker_symbol,
                series=record.security_series,
                validated_isin=record.validated_isin,
                disposition=disposition,
                included_in_broad_equity_scope=(
                    disposition
                    is CollectionUniverseDisposition.IN_SCOPE_UNVERIFIED_EQUITY
                ),
                permitted_to_trade=record.permitted_to_trade,
                normal_market_status=normal_market.status,
                normal_market_eligible=normal_market.eligible,
                delete_flag=record.delete_flag,
                listing_timestamp=record.listing_timestamp,
                removal_timestamp=record.removal_timestamp,
                readmission_timestamp=record.readmission_timestamp,
            )
        )
    return CollectionUniverseSnapshot(
        market_session_claim=manifest.claimed_report_date,
        cutoff=cutoff,
        knowledge_time=manifest.validated_at,
        calendar_snapshot_id=calendar_snapshot_id,
        source_artifact_id=manifest.artifact_id,
        source_manifest_id=manifest.manifest_id,
        source_raw_sha256=manifest.raw_sha256,
        source_normalized_sha256=manifest.normalized_sha256,
        observations=tuple(
            sorted(observations, key=lambda value: value.source_record_id)
        ),
        reason_codes=(
            "BOARD_CLASSIFICATION_UNVERIFIED",
            "CALENDAR_PROVENANCE_UNVERIFIED",
            "POINT_IN_TIME_LISTING_STATE_UNVERIFIED",
            "STABLE_IDENTITY_UNAVAILABLE",
            "SURVEILLANCE_STATE_UNAVAILABLE",
            "UNVERIFIED_MANUAL_ACQUISITION",
            "UNVERIFIED_REPORT_DATE",
        ),
    )
