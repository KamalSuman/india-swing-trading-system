from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from typing import Mapping, Protocol

from india_swing.identity import content_id

from .nse_archive import (
    EVIDENCE_PROFILE_COMPLETE,
    EVIDENCE_PROFILE_PRICE_UDIFF,
    EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY,
    EVIDENCE_PROFILE_UNRECONCILED,
    IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE,
    NSE_HISTORICAL_ARCHIVE_EQ_DATASET,
    NSE_HISTORICAL_ARCHIVE_INDEX_DATASET,
    NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION,
    NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION_V2,
    NSE_HISTORICAL_ARCHIVE_PROVIDER,
    NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION,
    NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION_V1,
    _expected_legacy_names,
)
from .codec import decode_market_payload
from .snapshot_store import StoredMarketSnapshot


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTITY_STATUSES = {
    "MATCHED_SAME_SESSION",
    "SECURITY_MASTER_MISSING",
    "SECURITY_MASTER_EVIDENCE_UNAVAILABLE",
    "FINANCIAL_INSTRUMENT_ID_MISMATCH",
    "SOURCE_IDENTIFIER_MISMATCH",
    IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE,
}
_INDEX_KEYS_V2 = {
    "schema_version",
    "range_start",
    "range_end",
    "collection_only",
    "actionable",
    "training_eligible",
    "identity_issue_count",
    "identity_quarantined_session_count",
    "records",
}
_INDEX_KEYS_V3 = _INDEX_KEYS_V2 | {
    "incomplete_evidence_session_count",
    "evidence_profile_counts",
}
_INDEX_RECORD_KEYS_V2 = {
    "session",
    "snapshot_id",
    "record_count",
    "source_container_sha256",
    "identity_issue_count",
}
_INDEX_RECORD_KEYS_V3 = _INDEX_RECORD_KEYS_V2 | {"evidence_profile"}
_SESSION_KEYS_V1 = {
    "schema_version",
    "session",
    "exchange",
    "series_scope",
    "source_mode",
    "source_container_sha256",
    "source_entry_sha256",
    "security_master_source_schema_version",
    "security_master_header_sha256",
    "scope_exclusion_policy",
    "reg1_row_count",
    "identity_issue_count",
    "identity_issues",
    "collection_only",
    "actionable",
    "training_eligible",
    "knowledge_time_status",
    "records",
}
_SESSION_KEYS_V2 = _SESSION_KEYS_V1 | {
    "evidence_profile",
    "missing_evidence",
}
_EVIDENCE_PROFILE_MISSING = {
    EVIDENCE_PROFILE_PRICE_UDIFF: (
        "REG1_SURVEILLANCE",
        "NSE_CM_SECURITY_MASTER",
    ),
    EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY: ("REG1_SURVEILLANCE",),
    EVIDENCE_PROFILE_COMPLETE: (),
    EVIDENCE_PROFILE_UNRECONCILED: (
        "UDIFF_BHAVCOPY",
        "NSE_CM_SECURITY_MASTER",
        "REG1_SURVEILLANCE",
    ),
}
_RECORD_KEYS = {
    "session",
    "listing_key",
    "symbol",
    "series",
    "financial_instrument_id",
    "security_master_financial_instrument_id",
    "security_source_record_id",
    "security_master_source_identifier",
    "udiff_source_identifier",
    "identity_status",
    "validated_isin",
    "normal_market_status",
    "normal_market_eligible",
    "permitted_to_trade",
    "delete_flag",
    "previous_close",
    "open",
    "high",
    "low",
    "last",
    "close",
    "average_price",
    "volume",
    "turnover_lacs",
    "trade_count",
    "delivery_quantity",
    "delivery_percent",
    "surveillance_indicators",
    "record_id",
}
_ISSUE_KEYS = {
    "session",
    "listing_key",
    "series",
    "udiff_financial_instrument_id",
    "security_master_financial_instrument_id",
    "security_master_source_identifier",
    "udiff_source_identifier",
    "status",
    "issue_id",
}


class NseHistoricalArchiveRangeError(ValueError):
    pass


class NseHistoricalArchiveSnapshotReader(Protocol):
    def get(self, dataset: str, snapshot_id: str) -> StoredMarketSnapshot: ...


@dataclass(frozen=True, slots=True)
class VerifiedNseHistoricalArchiveRange:
    index_snapshot_id: str
    range_start: date
    range_end: date
    session_snapshot_ids: tuple[str, ...]
    sessions: tuple[StoredMarketSnapshot, ...]
    record_count: int
    identity_issue_count: int
    identity_quarantined_session_count: int
    incomplete_evidence_session_count: int
    evidence_profile_counts: Mapping[str, int]


def _fail(message: str) -> None:
    raise NseHistoricalArchiveRangeError(message)


def _mapping(value: object, keys: set[str], message: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != keys:
        _fail(message)
    return value


def _sha256(value: object, message: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        _fail(message)
    return value


def _replay_record_id(record: Mapping[str, object]) -> str:
    payload = {key: value for key, value in record.items() if key != "record_id"}
    return content_id(
        {"schema": "nse-historical-archive-eq-record/v1", **payload},
        length=64,
    )


def _replay_issue_id(issue: Mapping[str, object]) -> str:
    payload = {key: value for key, value in issue.items() if key != "issue_id"}
    return content_id(
        {"schema": "nse-historical-archive-identity-issue/v1", **payload},
        length=64,
    )


_UNRECONCILED_NULL_RECORD_FIELDS = (
    "financial_instrument_id",
    "security_master_financial_instrument_id",
    "security_source_record_id",
    "security_master_source_identifier",
    "udiff_source_identifier",
    "normal_market_status",
    "normal_market_eligible",
    "permitted_to_trade",
    "delete_flag",
)
_UNRECONCILED_NULL_ISSUE_FIELDS = (
    "udiff_financial_instrument_id",
    "security_master_financial_instrument_id",
    "security_master_source_identifier",
    "udiff_source_identifier",
)


def _verify_source_entries(
    value: object,
    *,
    session: date,
    evidence_profile: str,
) -> None:
    expected = (
        f"sec_bhavdata_full_{session:%d%m%Y}.csv",
        f"BhavCopy_NSE_CM_0_0_0_{session:%Y%m%d}_F_0000.csv.zip",
        f"REG1_IND{session:%d%m%y}.csv",
        f"NSE_CM_security_{session:%d%m%Y}.csv.gz",
    )
    legacy = _expected_legacy_names(session)
    # EVIDENCE_PROFILE_UNRECONCILED accepts exactly one of two canonical
    # source-name shapes -- the single modern file, or the legacy
    # Bhavcopy/MTO pair -- never a mixture of the two.
    accepted_shapes_by_profile = {
        EVIDENCE_PROFILE_UNRECONCILED: (expected[:1], legacy),
        EVIDENCE_PROFILE_PRICE_UDIFF: (expected[:2],),
        EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY: (
            (expected[0], expected[1], expected[3]),
        ),
        EVIDENCE_PROFILE_COMPLETE: (expected,),
    }
    if type(value) is not tuple:
        _fail("archive range source entries are invalid")
    pairs = value
    if any(
        type(pair) is not tuple or len(pair) != 2 or type(pair[0]) is not str
        for pair in pairs
    ):
        _fail("archive range source entries are invalid")
    shapes = accepted_shapes_by_profile[evidence_profile]
    names = {pair[0] for pair in pairs}
    matching_shape = next(
        (shape for shape in shapes if names == set(shape)), None
    )
    if (
        matching_shape is None
        or len(pairs) != len(matching_shape)
        or pairs != tuple(sorted(pairs))
    ):
        _fail("archive range source entries are invalid")
    for _, digest in pairs:
        _sha256(digest, "archive range source entry identity is invalid")


def _verify_record(value: object, session: date) -> Mapping[str, object]:
    record = _mapping(value, _RECORD_KEYS, "archive range record is invalid")
    if record["session"] != session:
        _fail("archive range record session is invalid")
    _sha256(record["record_id"], "archive range record identity is invalid")
    status = record["identity_status"]
    if status not in _IDENTITY_STATUSES:
        _fail("archive range record identity status is invalid")
    matched = status == "MATCHED_SAME_SESSION"
    if matched:
        if (
            type(record["validated_isin"]) is not str
            or record["validated_isin"] != record["udiff_source_identifier"]
            or record["validated_isin"] != record["security_master_source_identifier"]
            or record["financial_instrument_id"]
            != record["security_master_financial_instrument_id"]
        ):
            _fail("archive range matched identity is inconsistent")
    elif record["validated_isin"] is not None:
        _fail("archive range unresolved identity was treated as validated")
    return record


def _verify_issue(value: object, session: date) -> Mapping[str, object]:
    issue = _mapping(value, _ISSUE_KEYS, "archive range identity issue is invalid")
    if issue["session"] != session or issue["status"] == "MATCHED_SAME_SESSION":
        _fail("archive range identity issue is invalid")
    _sha256(issue["issue_id"], "archive range issue identity is invalid")
    return issue


def _verify_session(
    stored: StoredMarketSnapshot,
    index_record: Mapping[str, object],
    index_observed_at: object,
) -> str:
    if type(stored) is not StoredMarketSnapshot:
        _fail("archive range session snapshot type is invalid")
    manifest = stored.manifest
    session = index_record["session"]
    if (
        hashlib.sha256(stored.payload_bytes).hexdigest() != manifest.payload_sha256
        or content_id(
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
        != manifest.snapshot_id
    ):
        _fail("archive range session snapshot identity is invalid")
    try:
        replayed_payload = decode_market_payload(stored.payload_bytes)
    except Exception:
        raise NseHistoricalArchiveRangeError(
            "archive range session payload bytes are invalid"
        ) from None
    if replayed_payload != stored.normalized_payload:
        _fail("archive range session payload bytes are invalid")
    if (
        type(session) is not date
        or manifest.dataset != NSE_HISTORICAL_ARCHIVE_EQ_DATASET
        or manifest.snapshot_id != index_record["snapshot_id"]
        or manifest.selection_key != session.isoformat()
        or manifest.provider != NSE_HISTORICAL_ARCHIVE_PROVIDER
        or manifest.observed_at != index_observed_at
        or manifest.record_count != index_record["record_count"]
    ):
        _fail("archive range session manifest is invalid")
    if not isinstance(stored.normalized_payload, Mapping):
        _fail("archive range session payload is invalid")
    session_schema = stored.normalized_payload.get("schema_version")
    if session_schema == NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION_V1:
        session_keys = _SESSION_KEYS_V1
        evidence_profile = EVIDENCE_PROFILE_COMPLETE
    elif session_schema == NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION:
        session_keys = _SESSION_KEYS_V2
        evidence_profile = stored.normalized_payload.get("evidence_profile")
        if evidence_profile not in _EVIDENCE_PROFILE_MISSING:
            _fail("archive range session evidence profile is invalid")
    else:
        _fail("archive range session schema is invalid")
    payload = _mapping(
        stored.normalized_payload,
        session_keys,
        "archive range session payload is invalid",
    )
    records = payload["records"]
    issues = payload["identity_issues"]
    if (
        payload["session"] != session
        or payload["exchange"] != "NSE"
        or payload["series_scope"] != ("EQ",)
        or payload["collection_only"] is not True
        or payload["actionable"] is not False
        or payload["training_eligible"] is not False
        or type(records) is not tuple
        or len(records) != manifest.record_count
        or type(issues) is not tuple
        or payload["source_container_sha256"]
        != index_record["source_container_sha256"]
        or payload["identity_issue_count"] != index_record["identity_issue_count"]
        or len(issues) != payload["identity_issue_count"]
    ):
        _fail("archive range session payload is invalid")
    if session_schema == NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION:
        if payload["missing_evidence"] != _EVIDENCE_PROFILE_MISSING[evidence_profile]:
            _fail("archive range session missing-evidence accounting is invalid")
    _verify_source_entries(
        payload["source_entry_sha256"],
        session=session,
        evidence_profile=evidence_profile,
    )
    security_available = evidence_profile not in {
        EVIDENCE_PROFILE_PRICE_UDIFF,
        EVIDENCE_PROFILE_UNRECONCILED,
    }
    surveillance_available = evidence_profile == EVIDENCE_PROFILE_COMPLETE
    if (
        security_available
        and (
            type(payload["security_master_source_schema_version"]) is not str
            or _SHA256.fullmatch(payload["security_master_header_sha256"] or "")
            is None
        )
    ) or (
        not security_available
        and (
            payload["security_master_source_schema_version"] is not None
            or payload["security_master_header_sha256"] is not None
        )
    ):
        _fail("archive range security-master evidence is invalid")
    if (
        surveillance_available
        and (
            type(payload["reg1_row_count"]) is not int
            or payload["reg1_row_count"] < 0
        )
    ) or (
        not surveillance_available and payload["reg1_row_count"] is not None
    ):
        _fail("archive range surveillance evidence is invalid")
    verified_records = tuple(_verify_record(value, session) for value in records)
    lanes = tuple((value["listing_key"], value["series"]) for value in verified_records)
    if len(set(lanes)) != len(lanes):
        _fail("archive range session contains duplicate listing lanes")
    verified_issues = tuple(_verify_issue(value, session) for value in issues)
    unresolved = {
        (value["listing_key"], value["series"], value["identity_status"])
        for value in verified_records
        if value["identity_status"] != "MATCHED_SAME_SESSION"
    }
    issue_keys = {
        (value["listing_key"], value["series"], value["status"])
        for value in verified_issues
    }
    if unresolved != issue_keys:
        _fail("archive range identity issue accounting is invalid")
    if evidence_profile == EVIDENCE_PROFILE_UNRECONCILED:
        if any(
            value["identity_status"]
            != IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE
            or any(
                value[field] is not None
                for field in _UNRECONCILED_NULL_RECORD_FIELDS
            )
            or value["record_id"] != _replay_record_id(value)
            for value in verified_records
        ):
            _fail("archive range record evidence profile is inconsistent")
        if (
            len(verified_issues) != len(verified_records)
            or len(
                {
                    (value["listing_key"], value["series"])
                    for value in verified_issues
                }
            )
            != len(verified_issues)
            or any(
                value["status"]
                != IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE
                or any(
                    value[field] is not None
                    for field in _UNRECONCILED_NULL_ISSUE_FIELDS
                )
                or value["issue_id"] != _replay_issue_id(value)
                for value in verified_issues
            )
        ):
            _fail("archive range identity issue accounting is invalid")
    elif any(
        (value["identity_status"] == "SECURITY_MASTER_EVIDENCE_UNAVAILABLE")
        != (not security_available)
        for value in verified_records
    ):
        _fail("archive range record evidence profile is inconsistent")
    if not surveillance_available and any(
        value["surveillance_indicators"] != {} for value in verified_records
    ):
        _fail("archive range record surveillance evidence is inconsistent")
    return evidence_profile


def load_verified_nse_historical_archive_range(
    reader: NseHistoricalArchiveSnapshotReader,
    *,
    index_snapshot_id: str,
) -> VerifiedNseHistoricalArchiveRange:
    _sha256(index_snapshot_id, "archive range index snapshot id is invalid")
    try:
        index = reader.get(NSE_HISTORICAL_ARCHIVE_INDEX_DATASET, index_snapshot_id)
    except Exception:
        raise NseHistoricalArchiveRangeError(
            "archive range index snapshot could not be loaded"
        ) from None
    if type(index) is not StoredMarketSnapshot:
        _fail("archive range index snapshot type is invalid")
    manifest = index.manifest
    if not isinstance(index.normalized_payload, Mapping):
        _fail("archive range index payload is invalid")
    index_schema = index.normalized_payload.get("schema_version")
    if index_schema == NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION_V2:
        index_keys = _INDEX_KEYS_V2
        index_record_keys = _INDEX_RECORD_KEYS_V2
    elif index_schema == NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION:
        index_keys = _INDEX_KEYS_V3
        index_record_keys = _INDEX_RECORD_KEYS_V3
    else:
        _fail("archive range index schema is invalid")
    payload = _mapping(
        index.normalized_payload,
        index_keys,
        "archive range index payload is invalid",
    )
    records = payload["records"]
    if (
        manifest.dataset != NSE_HISTORICAL_ARCHIVE_INDEX_DATASET
        or manifest.snapshot_id != index_snapshot_id
        or manifest.provider != NSE_HISTORICAL_ARCHIVE_PROVIDER
        or type(payload["range_start"]) is not date
        or type(payload["range_end"]) is not date
        or payload["range_start"] > payload["range_end"]
        or manifest.selection_key
        != f"{payload['range_start'].isoformat()}:{payload['range_end'].isoformat()}"
        or payload["collection_only"] is not True
        or payload["actionable"] is not False
        or payload["training_eligible"] is not False
        or type(records) is not tuple
        or not records
        or len(records) != manifest.record_count
    ):
        _fail("archive range index payload is invalid")
    index_records = tuple(
        _mapping(value, index_record_keys, "archive range index record is invalid")
        for value in records
    )
    sessions = tuple(value["session"] for value in index_records)
    if (
        any(type(value) is not date for value in sessions)
        or sessions != tuple(sorted(set(sessions)))
        or any(not payload["range_start"] <= value <= payload["range_end"] for value in sessions)
        or any(type(value["record_count"]) is not int or value["record_count"] <= 0 for value in index_records)
        or any(type(value["identity_issue_count"]) is not int or value["identity_issue_count"] < 0 for value in index_records)
    ):
        _fail("archive range index record is invalid")
    for value in index_records:
        _sha256(value["snapshot_id"], "archive range session snapshot id is invalid")
        _sha256(value["source_container_sha256"], "archive range source hash is invalid")
    if (
        payload["identity_issue_count"]
        != sum(value["identity_issue_count"] for value in index_records)
        or payload["identity_quarantined_session_count"]
        != sum(value["identity_issue_count"] > 0 for value in index_records)
    ):
        _fail("archive range identity accounting is invalid")
    loaded: list[StoredMarketSnapshot] = []
    loaded_profiles: list[str] = []
    for value in index_records:
        try:
            stored = reader.get(
                NSE_HISTORICAL_ARCHIVE_EQ_DATASET,
                value["snapshot_id"],
            )
        except Exception:
            raise NseHistoricalArchiveRangeError(
                "archive range session snapshot could not be loaded"
            ) from None
        evidence_profile = _verify_session(stored, value, manifest.observed_at)
        if (
            index_schema == NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION
            and value["evidence_profile"] != evidence_profile
        ):
            _fail("archive range index evidence profile is inconsistent")
        loaded.append(stored)
        loaded_profiles.append(evidence_profile)
    evidence_profile_counts = {
        profile: loaded_profiles.count(profile)
        for profile in _EVIDENCE_PROFILE_MISSING
    }
    incomplete_evidence_session_count = sum(
        profile != EVIDENCE_PROFILE_COMPLETE for profile in loaded_profiles
    )
    if index_schema == NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION:
        claimed_counts = payload["evidence_profile_counts"]
        if (
            not isinstance(claimed_counts, Mapping)
            or set(claimed_counts) != set(_EVIDENCE_PROFILE_MISSING)
            or any(type(value) is not int or value < 0 for value in claimed_counts.values())
            or dict(claimed_counts) != evidence_profile_counts
            or payload["incomplete_evidence_session_count"]
            != incomplete_evidence_session_count
        ):
            _fail("archive range evidence-profile accounting is invalid")
    return VerifiedNseHistoricalArchiveRange(
        index_snapshot_id=index_snapshot_id,
        range_start=payload["range_start"],
        range_end=payload["range_end"],
        session_snapshot_ids=tuple(value["snapshot_id"] for value in index_records),
        sessions=tuple(loaded),
        record_count=sum(value["record_count"] for value in index_records),
        identity_issue_count=payload["identity_issue_count"],
        identity_quarantined_session_count=payload[
            "identity_quarantined_session_count"
        ],
        incomplete_evidence_session_count=incomplete_evidence_session_count,
        evidence_profile_counts=evidence_profile_counts,
    )
