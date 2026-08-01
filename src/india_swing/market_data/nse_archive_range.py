from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from typing import Mapping, Protocol

from india_swing.identity import content_id

from .nse_archive import (
    NSE_HISTORICAL_ARCHIVE_EQ_DATASET,
    NSE_HISTORICAL_ARCHIVE_INDEX_DATASET,
    NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION,
    NSE_HISTORICAL_ARCHIVE_PROVIDER,
    NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION,
)
from .codec import decode_market_payload
from .snapshot_store import StoredMarketSnapshot


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTITY_STATUSES = {
    "MATCHED_SAME_SESSION",
    "SECURITY_MASTER_MISSING",
    "FINANCIAL_INSTRUMENT_ID_MISMATCH",
    "SOURCE_IDENTIFIER_MISMATCH",
}
_INDEX_KEYS = {
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
_INDEX_RECORD_KEYS = {
    "session",
    "snapshot_id",
    "record_count",
    "source_container_sha256",
    "identity_issue_count",
}
_SESSION_KEYS = {
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
) -> None:
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
    payload = _mapping(
        stored.normalized_payload,
        _SESSION_KEYS,
        "archive range session payload is invalid",
    )
    records = payload["records"]
    issues = payload["identity_issues"]
    if (
        payload["schema_version"] != NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION
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
        or payload["identity_issue_count"] != index_record["identity_issue_count"]
        or len(issues) != payload["identity_issue_count"]
    ):
        _fail("archive range session payload is invalid")
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
    payload = _mapping(
        index.normalized_payload,
        _INDEX_KEYS,
        "archive range index payload is invalid",
    )
    records = payload["records"]
    if (
        manifest.dataset != NSE_HISTORICAL_ARCHIVE_INDEX_DATASET
        or manifest.snapshot_id != index_snapshot_id
        or manifest.provider != NSE_HISTORICAL_ARCHIVE_PROVIDER
        or payload["schema_version"] != NSE_HISTORICAL_ARCHIVE_INDEX_SCHEMA_VERSION
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
        _mapping(value, _INDEX_RECORD_KEYS, "archive range index record is invalid")
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
        _verify_session(stored, value, manifest.observed_at)
        loaded.append(stored)
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
    )
