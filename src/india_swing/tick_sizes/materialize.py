from __future__ import annotations

from datetime import datetime, timezone

from india_swing.reference_data import SourceRowDisposition, StoredReferenceArtifact
from india_swing.reference_data.artifact_store import verify_stored_reference_provenance
from india_swing.reference_data.security_master import NSE_CM_MII_SECURITY_HEADER_INDEX

from .models import CollectedTickSizeObservation, CollectionTickSizeSnapshot


def materialize_collection_tick_sizes(
    source: StoredReferenceArtifact,
    *,
    cutoff: datetime,
) -> CollectionTickSizeSnapshot:
    if type(source) is not StoredReferenceArtifact:
        raise TypeError("tick-size source must be an exact stored reference artifact")
    verify_stored_reference_provenance(source)
    if not isinstance(cutoff, datetime):
        raise TypeError("tick-size cutoff must be a datetime")
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise ValueError("tick-size cutoff must be timezone-aware")
    cutoff = cutoff.astimezone(timezone.utc)
    manifest = source.manifest
    if manifest.validated_at > cutoff:
        raise ValueError("security master was unavailable at the tick-size cutoff")
    tick_size_index = NSE_CM_MII_SECURITY_HEADER_INDEX["TickSz"]
    observations = []
    for record in source.parsed.records:
        if record.disposition is not SourceRowDisposition.RETAINED_UNVERIFIED_EQUITY:
            continue
        if record.raw_fields[tick_size_index] != "":
            raise ValueError(
                "reserved TickSz became populated; the source contract requires review"
            )
        observations.append(
            CollectedTickSizeObservation(
                market_session_claim=manifest.claimed_report_date,
                knowledge_time=manifest.validated_at,
                source_artifact_id=manifest.artifact_id,
                source_manifest_id=manifest.manifest_id,
                source_record_id=record.source_record_id,
                financial_instrument_id=record.financial_instrument_id,
                symbol=record.ticker_symbol,
                series=record.security_series,
                validated_isin=record.validated_isin,
                bid_interval_paise=record.bid_interval_paise,
            )
        )
    return CollectionTickSizeSnapshot(
        market_session_claim=manifest.claimed_report_date,
        cutoff=cutoff,
        knowledge_time=manifest.validated_at,
        source_artifact_id=manifest.artifact_id,
        source_manifest_id=manifest.manifest_id,
        source_raw_sha256=manifest.raw_sha256,
        source_normalized_sha256=manifest.normalized_sha256,
        observations=tuple(sorted(observations, key=lambda value: value.source_record_id)),
        reason_codes=(
            "STABLE_IDENTITY_UNAVAILABLE",
            "UNVERIFIED_MANUAL_ACQUISITION",
            "UNVERIFIED_REPORT_DATE",
        ),
    )
