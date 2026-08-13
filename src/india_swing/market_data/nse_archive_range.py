from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from typing import Mapping, Protocol

from india_swing.identity import content_id

from india_swing.daily_reports.parser import _RAW_IDENTIFIER, _SYMBOL

from .nse_archive import (
    EVIDENCE_PROFILE_COMPLETE,
    EVIDENCE_PROFILE_PRICE_UDIFF,
    EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY,
    EVIDENCE_PROFILE_UNRECONCILED,
    IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE,
    NSE_HISTORICAL_ARCHIVE_EQ_DATASET,
    NSE_HISTORICAL_ARCHIVE_INDEX_DATASET,
    NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION,
    NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION_V1,
    NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION_V2,
    NSE_HISTORICAL_ARCHIVE_PROVIDER,
    NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION,
    NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION_V1,
    NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION_V2,
    SOURCE_IDENTITY_CLAIM_KIND_LEGACY_BHAVCOPY_ISIN,
    SOURCE_IDENTITY_CLAIM_STATUS_SOURCE_CLAIMED_UNVERIFIED,
    _expected_legacy_names,
    _legacy_bhavcopy_stem,
)
from .codec import decode_market_payload
from .snapshot_store import HashVerifiedMarketSnapshot, StoredMarketSnapshot


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTITY_STATUSES = {
    "MATCHED_SAME_SESSION",
    "SECURITY_MASTER_MISSING",
    "SECURITY_MASTER_EVIDENCE_UNAVAILABLE",
    "FINANCIAL_INSTRUMENT_ID_MISMATCH",
    "SOURCE_IDENTIFIER_MISMATCH",
    IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE,
}
_INDEX_KEYS_V1 = {
    "schema_version",
    "range_start",
    "range_end",
    "collection_only",
    "actionable",
    "training_eligible",
    "records",
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
_INDEX_RECORD_KEYS_V1 = {
    "session",
    "snapshot_id",
    "record_count",
    "source_container_sha256",
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
_SESSION_KEYS_V3 = _SESSION_KEYS_V2 | {"source_identity_claims"}
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
_CLAIM_KEYS = {
    "claim_id",
    "session",
    "listing_key",
    "symbol",
    "series",
    "claimed_isin",
    "source_kind",
    "source_entry_name",
    "source_entry_sha256",
    "source_row_number",
    "status",
}


class NseHistoricalArchiveRangeError(ValueError):
    pass


class NseHistoricalArchiveSnapshotReader(Protocol):
    def get(self, dataset: str, snapshot_id: str) -> StoredMarketSnapshot: ...


def _get_session_snapshot(
    reader: NseHistoricalArchiveSnapshotReader,
    *,
    partition_date: date,
    snapshot_id: str,
) -> StoredMarketSnapshot | HashVerifiedMarketSnapshot:
    """Use an exact date-partition read when the reader provides one.

    The fallback preserves the original reader contract for test doubles and
    other providers.  No discovery or latest selection is introduced.
    """

    get_hash_verified = getattr(
        type(reader), "get_hash_verified_from_date_partition", None
    )
    if callable(get_hash_verified):
        return get_hash_verified(
            reader,
            NSE_HISTORICAL_ARCHIVE_EQ_DATASET,
            partition_date,
            snapshot_id,
        )
    get_from_date_partition = getattr(
        type(reader), "get_from_date_partition", None
    )
    if callable(get_from_date_partition):
        return get_from_date_partition(
            reader,
            NSE_HISTORICAL_ARCHIVE_EQ_DATASET,
            partition_date,
            snapshot_id,
        )
    return reader.get(NSE_HISTORICAL_ARCHIVE_EQ_DATASET, snapshot_id)


def _materialize_session_snapshot(
    value: StoredMarketSnapshot | HashVerifiedMarketSnapshot,
) -> StoredMarketSnapshot:
    if type(value) is HashVerifiedMarketSnapshot:
        try:
            normalized_payload = decode_market_payload(value.payload_bytes)
        except Exception:
            raise NseHistoricalArchiveRangeError(
                "archive range session payload bytes are invalid"
            ) from None
        return StoredMarketSnapshot(
            path=value.path,
            manifest=value.manifest,
            normalized_payload=normalized_payload,
            payload_bytes=value.payload_bytes,
        )
    if type(value) is not StoredMarketSnapshot:
        _fail("archive range session snapshot type is invalid")
    try:
        replayed_payload = decode_market_payload(value.payload_bytes)
    except Exception:
        raise NseHistoricalArchiveRangeError(
            "archive range session payload bytes are invalid"
        ) from None
    if replayed_payload != value.normalized_payload:
        _fail("archive range session payload bytes are invalid")
    return value


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


def _replay_claim_id(claim: Mapping[str, object]) -> str:
    payload = {key: value for key, value in claim.items() if key != "claim_id"}
    return content_id(
        {"schema": "nse-historical-archive-source-identity-claim/v1", **payload},
        length=64,
    )


def _is_legacy_unreconciled_source(
    source_entry_sha256: object, session: date
) -> bool:
    if type(source_entry_sha256) is not tuple:
        return False
    names = {
        pair[0]
        for pair in source_entry_sha256
        if type(pair) is tuple and len(pair) == 2
    }
    return names == set(_expected_legacy_names(session))


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
    symbol = record["symbol"]
    if (
        type(symbol) is not str
        or _SYMBOL.fullmatch(symbol) is None
        or record["series"] != "EQ"
        or record["listing_key"] != f"NSE:{symbol}"
    ):
        _fail("archive range record listing binding is invalid")
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


def _verify_claim(value: object, session: date) -> Mapping[str, object]:
    claim = _mapping(
        value, _CLAIM_KEYS, "archive range source identity claim is invalid"
    )
    if claim["session"] != session:
        _fail("archive range source identity claim session is invalid")
    symbol = claim["symbol"]
    if (
        type(symbol) is not str
        or _SYMBOL.fullmatch(symbol) is None
        or claim["series"] != "EQ"
        or claim["listing_key"] != f"NSE:{symbol}"
    ):
        _fail("archive range source identity claim listing binding is invalid")
    claimed_isin = claim["claimed_isin"]
    if type(claimed_isin) is not str or _RAW_IDENTIFIER.fullmatch(claimed_isin) is None:
        _fail("archive range source identity claim ISIN is invalid")
    if claim["source_kind"] != SOURCE_IDENTITY_CLAIM_KIND_LEGACY_BHAVCOPY_ISIN:
        _fail("archive range source identity claim source kind is invalid")
    if claim["status"] != SOURCE_IDENTITY_CLAIM_STATUS_SOURCE_CLAIMED_UNVERIFIED:
        _fail("archive range source identity claim status is invalid")
    if type(claim["source_entry_name"]) is not str or not claim["source_entry_name"]:
        _fail("archive range source identity claim entry name is invalid")
    _sha256(
        claim["source_entry_sha256"],
        "archive range source identity claim entry hash is invalid",
    )
    row_number = claim["source_row_number"]
    if type(row_number) is not int or isinstance(row_number, bool) or row_number < 2:
        _fail("archive range source identity claim row number is invalid")
    _sha256(claim["claim_id"], "archive range source identity claim identity is invalid")
    if claim["claim_id"] != _replay_claim_id(claim):
        _fail("archive range source identity claim identity is invalid")
    return claim


def _verify_legacy_source_identity_claims(
    claims_value: object,
    *,
    session: date,
    records: tuple[Mapping[str, object], ...],
) -> tuple[Mapping[str, object], ...]:
    if type(claims_value) is not tuple:
        _fail("archive range source identity claims are invalid")
    verified_claims = tuple(_verify_claim(value, session) for value in claims_value)
    if len(verified_claims) != len(records):
        _fail("archive range source identity claim count is invalid")
    expected_entry_name = f"{_legacy_bhavcopy_stem(session)}.csv"
    reference_entry_name = verified_claims[0]["source_entry_name"]
    reference_entry_sha256 = verified_claims[0]["source_entry_sha256"]
    seen_row_numbers: set[int] = set()
    for claim, record in zip(verified_claims, records, strict=True):
        if (
            claim["session"] != record["session"]
            or claim["listing_key"] != record["listing_key"]
            or claim["symbol"] != record["symbol"]
            or claim["series"] != record["series"]
        ):
            _fail("archive range source identity claim binding is invalid")
        if claim["source_entry_name"] != expected_entry_name:
            _fail("archive range source identity claim entry name is invalid")
        if (
            claim["source_entry_name"] != reference_entry_name
            or claim["source_entry_sha256"] != reference_entry_sha256
        ):
            _fail(
                "archive range source identity claim entry evidence is inconsistent"
            )
        if claim["source_row_number"] in seen_row_numbers:
            _fail("archive range source identity claim row numbers are not unique")
        seen_row_numbers.add(claim["source_row_number"])
    return verified_claims


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
    elif session_schema == NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION_V2:
        session_keys = _SESSION_KEYS_V2
        evidence_profile = stored.normalized_payload.get("evidence_profile")
        if evidence_profile not in _EVIDENCE_PROFILE_MISSING:
            _fail("archive range session evidence profile is invalid")
    elif session_schema == NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION:
        session_keys = _SESSION_KEYS_V3
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
    claimed_identity_issue_count = payload["identity_issue_count"]
    if (
        # Exact-type/non-negativity is required before any numeric
        # equality is evaluated: Python considers ``True == 1`` and
        # ``False == 0``, so a bool claim here must be rejected on type
        # alone -- it can never be allowed to reach (and silently satisfy)
        # the count-equality checks below. This applies uniformly to every
        # accepted session schema (v1/v2/v3), not just the legacy path.
        type(claimed_identity_issue_count) is not int
        or claimed_identity_issue_count < 0
        or payload["session"] != session
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
        or claimed_identity_issue_count != index_record["identity_issue_count"]
        or len(issues) != claimed_identity_issue_count
    ):
        _fail("archive range session payload is invalid")
    if session_schema in (
        NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION_V2,
        NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION,
    ):
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
    if session_schema == NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION:
        is_legacy = evidence_profile == EVIDENCE_PROFILE_UNRECONCILED and (
            _is_legacy_unreconciled_source(payload["source_entry_sha256"], session)
        )
        claims_value = payload["source_identity_claims"]
        if type(claims_value) is not tuple:
            _fail("archive range source identity claims are invalid")
        if is_legacy:
            _verify_legacy_source_identity_claims(
                claims_value, session=session, records=verified_records
            )
        elif claims_value:
            _fail("archive range source identity claims are invalid")
    return evidence_profile


def _derive_legacy_identity_issue_count(stored: StoredMarketSnapshot) -> int:
    """The sole, independently-derived source of truth for one v1
    session's identity-issue count -- computed as ``len(identity_issues)``
    and never read back from the payload's own separate
    ``identity_issue_count`` claim, even after that claim has been cross-
    checked and matched inside ``_verify_session``. The caller retains
    this exact int locally and uses it both as the value fed into
    ``_verify_session``'s existing required field (so a tampered/
    inconsistent claim inside the session payload still fails that
    boundary's own cross-check against the real issue list length) and as
    the value accumulated into the v1 range-level totals. Returns ``-1``
    -- a value that can never equal a genuine non-negative count -- when
    the session payload is not even shaped as a mapping with a tuple
    ``identity_issues`` field, so a malformed session still fails closed
    through that same existing comparison."""

    payload = getattr(stored, "normalized_payload", None)
    if not isinstance(payload, Mapping):
        return -1
    issues = payload.get("identity_issues")
    if type(issues) is not tuple:
        return -1
    return len(issues)


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
    if index_schema == NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION_V1:
        index_keys = _INDEX_KEYS_V1
        index_record_keys = _INDEX_RECORD_KEYS_V1
    elif index_schema == NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION_V2:
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
    is_v1_index = index_schema == NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION_V1
    sessions = tuple(value["session"] for value in index_records)
    if (
        any(type(value) is not date for value in sessions)
        or sessions != tuple(sorted(set(sessions)))
        or any(not payload["range_start"] <= value <= payload["range_end"] for value in sessions)
        or any(type(value["record_count"]) is not int or value["record_count"] <= 0 for value in index_records)
    ):
        _fail("archive range index record is invalid")
    if not is_v1_index:
        if any(
            type(value["identity_issue_count"]) is not int or value["identity_issue_count"] < 0
            for value in index_records
        ):
            _fail("archive range index record is invalid")
    for value in index_records:
        _sha256(value["snapshot_id"], "archive range session snapshot id is invalid")
        _sha256(value["source_container_sha256"], "archive range source hash is invalid")
    if not is_v1_index:
        if (
            payload["identity_issue_count"]
            != sum(value["identity_issue_count"] for value in index_records)
            or payload["identity_quarantined_session_count"]
            != sum(value["identity_issue_count"] > 0 for value in index_records)
        ):
            _fail("archive range identity accounting is invalid")
    loaded: list[StoredMarketSnapshot] = []
    loaded_profiles: list[str] = []
    derived_legacy_identity_issue_counts: list[int] = []
    for value in index_records:
        try:
            session_value = _get_session_snapshot(
                reader,
                partition_date=manifest.observed_at.date(),
                snapshot_id=value["snapshot_id"],
            )
        except Exception:
            raise NseHistoricalArchiveRangeError(
                "archive range session snapshot could not be loaded"
            ) from None
        stored = _materialize_session_snapshot(session_value)
        if is_v1_index:
            # v1 index records never declared identity_issue_count -- bind
            # _verify_session's existing required field to a value derived
            # independently from the session's own identity_issues list
            # length (never a blind copy of its own separate claim), so a
            # tampered/inconsistent claim inside the session payload still
            # fails _verify_session's own existing len(issues) cross-check
            # exactly as it would for v2/v3. This exact int is retained
            # locally and reused below -- never reread from the payload's
            # own claim after verification, even though it matched.
            derived_count = _derive_legacy_identity_issue_count(stored)
            session_record = dict(value)
            session_record["identity_issue_count"] = derived_count
        else:
            session_record = value
        evidence_profile = _verify_session(stored, session_record, manifest.observed_at)
        if (
            index_schema == NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION
            and value["evidence_profile"] != evidence_profile
        ):
            _fail("archive range index evidence profile is inconsistent")
        loaded.append(stored)
        loaded_profiles.append(evidence_profile)
        if is_v1_index:
            # _verify_session has now independently verified this session
            # end-to-end (identity, manifest, decode, and the cross-check
            # of its own claimed identity_issue_count against derived_count
            # itself) -- accumulate the same locally retained derived_count,
            # never a fresh read of the payload's own claim.
            derived_legacy_identity_issue_counts.append(derived_count)
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
            or not set(claimed_counts) <= set(_EVIDENCE_PROFILE_MISSING)
            or any(type(value) is not int or value < 0 for value in claimed_counts.values())
        ):
            _fail("archive range evidence-profile accounting is invalid")
        # A missing known key is a claimed zero -- so an omitted key
        # passes only when its independently derived count is actually
        # zero; an omitted nonzero bucket still fails the exact-equality
        # check just below, since the normalized mapping would then
        # disagree with the derived one.
        normalized_claimed_counts = {
            profile: claimed_counts.get(profile, 0) for profile in _EVIDENCE_PROFILE_MISSING
        }
        if (
            normalized_claimed_counts != evidence_profile_counts
            or payload["incomplete_evidence_session_count"]
            != incomplete_evidence_session_count
        ):
            _fail("archive range evidence-profile accounting is invalid")
    if is_v1_index:
        identity_issue_count = sum(derived_legacy_identity_issue_counts)
        identity_quarantined_session_count = sum(
            count > 0 for count in derived_legacy_identity_issue_counts
        )
    else:
        identity_issue_count = payload["identity_issue_count"]
        identity_quarantined_session_count = payload["identity_quarantined_session_count"]
    return VerifiedNseHistoricalArchiveRange(
        index_snapshot_id=index_snapshot_id,
        range_start=payload["range_start"],
        range_end=payload["range_end"],
        session_snapshot_ids=tuple(value["snapshot_id"] for value in index_records),
        sessions=tuple(loaded),
        record_count=sum(value["record_count"] for value in index_records),
        identity_issue_count=identity_issue_count,
        identity_quarantined_session_count=identity_quarantined_session_count,
        incomplete_evidence_session_count=incomplete_evidence_session_count,
        evidence_profile_counts=evidence_profile_counts,
    )
