"""NSE archive research replay bridge.

Streams typed, exact-lineage research session records from one already
sealed :class:`~india_swing.evaluation.nse_archive_research_dataset.NseArchiveResearchDataset`
by replaying only its explicitly pinned range and session snapshot IDs
through the existing public archive trust boundary
``load_verified_nse_historical_archive_range``. This module never reads the
filesystem, network, environment, or clock; never constructs a store; never
lists, discovers, or selects a "latest" artifact; and never marks a session
or record as feature-, label-, alert-, or execution-eligible.

Range-bounded: only the one ``VerifiedNseHistoricalArchiveRange`` currently
being replayed is ever held in memory. A consumer that stops iterating
early, or only partially consumes the iterator, never triggers a load of a
later range and never constitutes a completed or publishable research
artifact -- normal iterator exhaustion after the final session of the final
range is the only completion signal this module provides.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Iterator, Mapping

from india_swing.daily_reports.parser import _RAW_IDENTIFIER, _SYMBOL
from india_swing.identity import content_id
from india_swing.market_data.nse_archive import (
    EVIDENCE_PROFILE_COMPLETE,
    EVIDENCE_PROFILE_PRICE_UDIFF,
    EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY,
    EVIDENCE_PROFILE_UNRECONCILED,
    IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE,
    NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION,
    NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION_V1,
    NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION_V2,
    SOURCE_IDENTITY_CLAIM_KIND_LEGACY_BHAVCOPY_ISIN,
    SOURCE_IDENTITY_CLAIM_STATUS_SOURCE_CLAIMED_UNVERIFIED,
    _legacy_bhavcopy_stem,
)
from india_swing.market_data.nse_archive_range import (
    NseHistoricalArchiveLegacyIndexSchema,
    NseHistoricalArchiveSnapshotReader,
    StreamingVerifiedNseHistoricalArchiveRange,
    VerifiedNseHistoricalArchiveRange,
    load_verified_nse_historical_archive_range,
    stream_verified_nse_historical_archive_range,
)
from india_swing.market_data.snapshot_store import StoredMarketSnapshot

from .nse_archive_research_dataset import (
    NseArchiveResearchDataset,
    NseArchiveResearchDatasetSplitPartition,
    NseArchiveResearchRangeBinding,
    ResearchSplitRole,
)


REPLAY_SESSION_SCHEMA_VERSION_V1 = "nse-archive-research-replay-session/v1"
REPLAY_SESSION_SCHEMA_VERSION = "nse-archive-research-replay-session/v2"
_UNVERIFIED_KNOWLEDGE_TIME_STATUS = "MANUAL_HISTORICAL_IMPORT_UNVERIFIED"
_RECORD_CONTENT_SCHEMA_VERSION = "nse-historical-archive-eq-record/v1"
_IDENTITY_ISSUE_CONTENT_SCHEMA_VERSION = "nse-historical-archive-identity-issue/v1"
_SOURCE_IDENTITY_CLAIM_CONTENT_SCHEMA_VERSION = (
    "nse-historical-archive-source-identity-claim/v1"
)

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_KNOWN_EVIDENCE_PROFILES = (
    EVIDENCE_PROFILE_PRICE_UDIFF,
    EVIDENCE_PROFILE_PRICE_UDIFF_SECURITY,
    EVIDENCE_PROFILE_COMPLETE,
    EVIDENCE_PROFILE_UNRECONCILED,
)
_IDENTITY_STATUSES = frozenset(
    {
        "MATCHED_SAME_SESSION",
        "SECURITY_MASTER_MISSING",
        "SECURITY_MASTER_EVIDENCE_UNAVAILABLE",
        "FINANCIAL_INSTRUMENT_ID_MISMATCH",
        "SOURCE_IDENTIFIER_MISMATCH",
        IDENTITY_STATUS_UDIFF_AND_SECURITY_MASTER_EVIDENCE_UNAVAILABLE,
    }
)
_RECORD_KEYS = frozenset(
    {
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
)
_IDENTITY_ISSUE_KEYS = frozenset(
    {
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
)
_SESSION_KEYS_V1 = frozenset(
    {
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
)
_SESSION_KEYS_V2 = _SESSION_KEYS_V1 | {"evidence_profile", "missing_evidence"}
_SESSION_KEYS_V3 = _SESSION_KEYS_V2 | {"source_identity_claims"}
_SOURCE_IDENTITY_CLAIM_KEYS = frozenset(
    {
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
)


class NseArchiveResearchReplayError(ValueError):
    """A replay input, capability, or reconstructed artifact failed a static safety rule."""


def _fail(message: str) -> None:
    raise NseArchiveResearchReplayError(message)


def _is_sha256(value: object) -> bool:
    return type(value) is str and _SHA256_PATTERN.fullmatch(value) is not None


def _required(value: object, expected_type: type, message: str) -> object:
    if type(value) is not expected_type:
        _fail(message)
    return value


def _optional(value: object, expected_type: type, message: str) -> object:
    if value is not None and type(value) is not expected_type:
        _fail(message)
    return value


@dataclass(frozen=True, slots=True)
class NseArchiveResearchReplayRecord:
    """One immutable, lossless projection of a verified historical EQ record.

    Diagnostic booleans (``identity_matched``, ``normal_market_eligibility_verified``)
    are mechanically derived from the record's own fields only. They never
    set training/feature/actionable authority.
    """

    record_id: str
    session: date
    listing_key: str
    symbol: str
    series: str
    financial_instrument_id: int | None
    security_master_financial_instrument_id: int | None
    security_source_record_id: str | None
    security_master_source_identifier: str | None
    udiff_source_identifier: str | None
    identity_status: str
    validated_isin: str | None
    normal_market_status: int | None
    normal_market_eligible: bool | None
    permitted_to_trade: int | None
    delete_flag: str | None
    previous_close: Decimal
    open: Decimal
    high: Decimal
    low: Decimal
    last: Decimal
    close: Decimal
    average_price: Decimal
    volume: int
    turnover_lacs: Decimal
    trade_count: int
    delivery_quantity: int | None
    delivery_percent: Decimal | None
    surveillance_indicators: tuple[tuple[str, str], ...]
    identity_matched: bool
    normal_market_eligibility_verified: bool

    def __post_init__(self) -> None:
        self.verify_content_identity()

    def _validate_shape(self) -> None:
        if not _is_sha256(self.record_id):
            _fail("research replay record identity is invalid")
        if type(self.session) is not date:
            _fail("research replay record session is invalid")
        if type(self.symbol) is not str or _SYMBOL.fullmatch(self.symbol) is None:
            _fail("research replay record symbol is invalid")
        if self.series != "EQ":
            _fail("research replay record series is invalid")
        if type(self.listing_key) is not str or self.listing_key != f"NSE:{self.symbol}":
            _fail("research replay record listing key is invalid")
        if self.financial_instrument_id is not None and type(self.financial_instrument_id) is not int:
            _fail("research replay record financial instrument id is invalid")
        if (
            self.security_master_financial_instrument_id is not None
            and type(self.security_master_financial_instrument_id) is not int
        ):
            _fail(
                "research replay record security master financial instrument id is invalid"
            )
        if (
            self.security_source_record_id is not None
            and type(self.security_source_record_id) is not str
        ):
            _fail("research replay record security source record id is invalid")
        if (
            self.security_master_source_identifier is not None
            and type(self.security_master_source_identifier) is not str
        ):
            _fail(
                "research replay record security master source identifier is invalid"
            )
        if (
            self.udiff_source_identifier is not None
            and type(self.udiff_source_identifier) is not str
        ):
            _fail("research replay record udiff source identifier is invalid")
        if self.identity_status not in _IDENTITY_STATUSES:
            _fail("research replay record identity status is invalid")
        if self.validated_isin is not None and type(self.validated_isin) is not str:
            _fail("research replay record validated isin is invalid")
        if self.normal_market_status is not None and type(self.normal_market_status) is not int:
            _fail("research replay record normal market status is invalid")
        if (
            self.normal_market_eligible is not None
            and type(self.normal_market_eligible) is not bool
        ):
            _fail("research replay record normal market eligibility is invalid")
        if self.permitted_to_trade is not None and type(self.permitted_to_trade) is not int:
            _fail("research replay record permitted-to-trade flag is invalid")
        if self.delete_flag is not None and type(self.delete_flag) is not str:
            _fail("research replay record delete flag is invalid")
        for value in (
            self.previous_close,
            self.open,
            self.high,
            self.low,
            self.last,
            self.close,
            self.average_price,
            self.turnover_lacs,
        ):
            if type(value) is not Decimal:
                _fail("research replay record price is invalid")
        if type(self.volume) is not int:
            _fail("research replay record volume is invalid")
        if type(self.trade_count) is not int:
            _fail("research replay record trade count is invalid")
        if self.delivery_quantity is not None and type(self.delivery_quantity) is not int:
            _fail("research replay record delivery quantity is invalid")
        if self.delivery_percent is not None and type(self.delivery_percent) is not Decimal:
            _fail("research replay record delivery percent is invalid")
        if type(self.surveillance_indicators) is not tuple or any(
            type(pair) is not tuple
            or len(pair) != 2
            or type(pair[0]) is not str
            or type(pair[1]) is not str
            for pair in self.surveillance_indicators
        ):
            _fail("research replay record surveillance evidence is invalid")
        if tuple(sorted(self.surveillance_indicators)) != self.surveillance_indicators:
            _fail("research replay record surveillance evidence is invalid")
        if type(self.identity_matched) is not bool:
            _fail("research replay record identity match flag is invalid")
        if type(self.normal_market_eligibility_verified) is not bool:
            _fail("research replay record eligibility verification flag is invalid")

    def verify_content_identity(self) -> None:
        self._validate_shape()
        canonical_record = {
            "session": self.session,
            "listing_key": self.listing_key,
            "symbol": self.symbol,
            "series": self.series,
            "financial_instrument_id": self.financial_instrument_id,
            "security_master_financial_instrument_id": (
                self.security_master_financial_instrument_id
            ),
            "security_source_record_id": self.security_source_record_id,
            "security_master_source_identifier": self.security_master_source_identifier,
            "udiff_source_identifier": self.udiff_source_identifier,
            "identity_status": self.identity_status,
            "validated_isin": self.validated_isin,
            "normal_market_status": self.normal_market_status,
            "normal_market_eligible": self.normal_market_eligible,
            "permitted_to_trade": self.permitted_to_trade,
            "delete_flag": self.delete_flag,
            "previous_close": self.previous_close,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "last": self.last,
            "close": self.close,
            "average_price": self.average_price,
            "volume": self.volume,
            "turnover_lacs": self.turnover_lacs,
            "trade_count": self.trade_count,
            "delivery_quantity": self.delivery_quantity,
            "delivery_percent": self.delivery_percent,
            "surveillance_indicators": dict(self.surveillance_indicators),
        }
        expected_record_id = content_id(
            {"schema": _RECORD_CONTENT_SCHEMA_VERSION, **canonical_record}, length=64
        )
        if self.record_id != expected_record_id:
            _fail("research replay record identity failed")
        expected_identity_matched = (
            self.identity_status == "MATCHED_SAME_SESSION"
            and type(self.validated_isin) is str
            and self.validated_isin != ""
        )
        if self.identity_matched is not expected_identity_matched:
            _fail("research replay record identity failed")
        expected_eligibility_verified = (
            type(self.normal_market_eligible) is bool
            and self.security_master_financial_instrument_id is not None
            and self.security_master_source_identifier is not None
        )
        if self.normal_market_eligibility_verified is not expected_eligibility_verified:
            _fail("research replay record identity failed")


@dataclass(frozen=True, slots=True)
class NseArchiveResearchSourceIdentityClaim:
    """One immutable, lossless projection of an official legacy Bhavcopy ISIN claim.

    The claim is exactly what the legacy source publisher wrote for one EQ
    row -- ``status`` is always ``SOURCE_CLAIMED_UNVERIFIED``. It never sets
    ``validated_isin``, never implies identity match, and never carries
    feature/training/actionable authority; a later, separately reviewed
    historical identity-admission task decides whether a corroborated claim
    can ever be promoted.
    """

    claim_id: str
    session: date
    listing_key: str
    symbol: str
    series: str
    claimed_isin: str
    source_kind: str
    source_entry_name: str
    source_entry_sha256: str
    source_row_number: int
    status: str

    def __post_init__(self) -> None:
        self.verify_content_identity()

    def _validate_shape(self) -> None:
        if not _is_sha256(self.claim_id):
            _fail("research replay source identity claim identity is invalid")
        if type(self.session) is not date:
            _fail("research replay source identity claim session is invalid")
        if type(self.symbol) is not str or _SYMBOL.fullmatch(self.symbol) is None:
            _fail("research replay source identity claim symbol is invalid")
        if self.series != "EQ":
            _fail("research replay source identity claim series is invalid")
        if type(self.listing_key) is not str or self.listing_key != f"NSE:{self.symbol}":
            _fail("research replay source identity claim listing key is invalid")
        if (
            type(self.claimed_isin) is not str
            or _RAW_IDENTIFIER.fullmatch(self.claimed_isin) is None
        ):
            _fail("research replay source identity claim ISIN is invalid")
        if self.source_kind != SOURCE_IDENTITY_CLAIM_KIND_LEGACY_BHAVCOPY_ISIN:
            _fail("research replay source identity claim source kind is invalid")
        if (
            type(self.source_entry_name) is not str
            or self.source_entry_name != f"{_legacy_bhavcopy_stem(self.session)}.csv"
        ):
            _fail("research replay source identity claim entry name is invalid")
        if not _is_sha256(self.source_entry_sha256):
            _fail("research replay source identity claim entry hash is invalid")
        if (
            type(self.source_row_number) is not int
            or isinstance(self.source_row_number, bool)
            or self.source_row_number < 2
        ):
            _fail("research replay source identity claim row number is invalid")
        if self.status != SOURCE_IDENTITY_CLAIM_STATUS_SOURCE_CLAIMED_UNVERIFIED:
            _fail("research replay source identity claim status is invalid")

    def verify_content_identity(self) -> None:
        self._validate_shape()
        canonical_claim = {
            "session": self.session,
            "listing_key": self.listing_key,
            "symbol": self.symbol,
            "series": self.series,
            "claimed_isin": self.claimed_isin,
            "source_kind": self.source_kind,
            "source_entry_name": self.source_entry_name,
            "source_entry_sha256": self.source_entry_sha256,
            "source_row_number": self.source_row_number,
            "status": self.status,
        }
        expected_claim_id = content_id(
            {
                "schema": _SOURCE_IDENTITY_CLAIM_CONTENT_SCHEMA_VERSION,
                **canonical_claim,
            },
            length=64,
        )
        if self.claim_id != expected_claim_id:
            _fail("research replay source identity claim identity failed")


@dataclass(frozen=True, slots=True)
class NseArchiveResearchReplaySession:
    """One immutable replayed session, exactly bound to its dataset lineage."""

    dataset_id: str
    split_policy_id: str
    partition_id: str
    partition_role: ResearchSplitRole
    index_snapshot_id: str
    range_binding_id: str
    market_session: date
    session_snapshot_id: str
    observed_at: datetime
    evidence_profile: str
    missing_evidence: tuple[str, ...]
    knowledge_time_status: str
    records: tuple[NseArchiveResearchReplayRecord, ...]
    record_count: int
    identity_issue_count: int
    source_identity_claims: tuple[NseArchiveResearchSourceIdentityClaim, ...]
    collection_only: bool = field(init=False)
    actionable: bool = field(init=False)
    training_eligible: bool = field(init=False)
    feature_eligible: bool = field(init=False)
    label_eligible: bool = field(init=False)
    alert_eligible: bool = field(init=False)
    execution_eligible: bool = field(init=False)
    replay_session_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "collection_only", True)
        object.__setattr__(self, "actionable", False)
        object.__setattr__(self, "training_eligible", False)
        object.__setattr__(self, "feature_eligible", False)
        object.__setattr__(self, "label_eligible", False)
        object.__setattr__(self, "alert_eligible", False)
        object.__setattr__(self, "execution_eligible", False)
        self._validate()
        object.__setattr__(self, "replay_session_id", self._calculated_id())

    def _validate(self) -> None:
        if not _is_sha256(self.dataset_id):
            _fail("research replay session dataset id is invalid")
        if not _is_sha256(self.split_policy_id):
            _fail("research replay session split policy id is invalid")
        if not _is_sha256(self.partition_id):
            _fail("research replay session partition id is invalid")
        if type(self.partition_role) is not ResearchSplitRole:
            _fail("research replay session partition role is invalid")
        if not _is_sha256(self.index_snapshot_id):
            _fail("research replay session index snapshot id is invalid")
        if not _is_sha256(self.range_binding_id):
            _fail("research replay session range binding id is invalid")
        if type(self.market_session) is not date:
            _fail("research replay session market session is invalid")
        if not _is_sha256(self.session_snapshot_id):
            _fail("research replay session snapshot id is invalid")
        if type(self.observed_at) is not datetime or self.observed_at.tzinfo is None:
            _fail("research replay session observed_at is invalid")
        if self.evidence_profile not in _KNOWN_EVIDENCE_PROFILES:
            _fail("research replay session evidence profile is invalid")
        if type(self.missing_evidence) is not tuple or any(
            type(value) is not str for value in self.missing_evidence
        ):
            _fail("research replay session missing evidence is invalid")
        if self.knowledge_time_status != _UNVERIFIED_KNOWLEDGE_TIME_STATUS:
            _fail("research replay session knowledge time status is invalid")
        if type(self.records) is not tuple or any(
            type(value) is not NseArchiveResearchReplayRecord for value in self.records
        ):
            _fail("research replay session records are invalid")
        for record in self.records:
            record.verify_content_identity()
            if record.session != self.market_session:
                _fail("research replay session record session does not match its session")
        if type(self.record_count) is not int or self.record_count != len(self.records):
            _fail("research replay session record count is invalid")
        if type(self.identity_issue_count) is not int or self.identity_issue_count < 0:
            _fail("research replay session identity issue count is invalid")
        if type(self.source_identity_claims) is not tuple or any(
            type(value) is not NseArchiveResearchSourceIdentityClaim
            for value in self.source_identity_claims
        ):
            _fail("research replay session source identity claims are invalid")
        for claim in self.source_identity_claims:
            claim.verify_content_identity()
            if claim.session != self.market_session:
                _fail(
                    "research replay session source identity claim session does "
                    "not match its session"
                )
        if self.source_identity_claims:
            if self.evidence_profile != EVIDENCE_PROFILE_UNRECONCILED:
                _fail(
                    "research replay session source identity claims are only "
                    "valid for legacy unreconciled evidence"
                )
            if len(self.source_identity_claims) != len(self.records):
                _fail(
                    "research replay session source identity claim count is invalid"
                )
            for claim, record in zip(
                self.source_identity_claims, self.records, strict=True
            ):
                if (
                    claim.session != record.session
                    or claim.listing_key != record.listing_key
                    or claim.symbol != record.symbol
                    or claim.series != record.series
                ):
                    _fail(
                        "research replay session source identity claim binding "
                        "is invalid"
                    )
        if (
            self.collection_only is not True
            or self.actionable is not False
            or self.training_eligible is not False
            or self.feature_eligible is not False
            or self.label_eligible is not False
            or self.alert_eligible is not False
            or self.execution_eligible is not False
        ):
            _fail("research replay session safety posture is invalid")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": REPLAY_SESSION_SCHEMA_VERSION,
                "dataset_id": self.dataset_id,
                "split_policy_id": self.split_policy_id,
                "partition_id": self.partition_id,
                "partition_role": self.partition_role,
                "index_snapshot_id": self.index_snapshot_id,
                "range_binding_id": self.range_binding_id,
                "market_session": self.market_session,
                "session_snapshot_id": self.session_snapshot_id,
                "observed_at": self.observed_at,
                "evidence_profile": self.evidence_profile,
                "missing_evidence": self.missing_evidence,
                "knowledge_time_status": self.knowledge_time_status,
                "record_ids": tuple(value.record_id for value in self.records),
                "record_count": self.record_count,
                "identity_issue_count": self.identity_issue_count,
                "source_identity_claim_ids": tuple(
                    value.claim_id for value in self.source_identity_claims
                ),
                "collection_only": self.collection_only,
                "actionable": self.actionable,
                "training_eligible": self.training_eligible,
                "feature_eligible": self.feature_eligible,
                "label_eligible": self.label_eligible,
                "alert_eligible": self.alert_eligible,
                "execution_eligible": self.execution_eligible,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.replay_session_id != self._calculated_id():
            raise NseArchiveResearchReplayError(
                "research replay session identity failed"
            )

    @classmethod
    def _from_freshly_verified_archive(
        cls,
        *,
        dataset_id: str,
        split_policy_id: str,
        partition_id: str,
        partition_role: ResearchSplitRole,
        index_snapshot_id: str,
        range_binding_id: str,
        market_session: date,
        session_snapshot_id: str,
        observed_at: datetime,
        evidence_profile: str,
        missing_evidence: tuple[str, ...],
        knowledge_time_status: str,
        records: tuple[NseArchiveResearchReplayRecord, ...],
        identity_issue_count: int,
        source_identity_claims: tuple[NseArchiveResearchSourceIdentityClaim, ...],
    ) -> "NseArchiveResearchReplaySession":
        """Assemble one session after the archive and every leaf were verified."""

        value = object.__new__(cls)
        for name, item in (
            ("dataset_id", dataset_id),
            ("split_policy_id", split_policy_id),
            ("partition_id", partition_id),
            ("partition_role", partition_role),
            ("index_snapshot_id", index_snapshot_id),
            ("range_binding_id", range_binding_id),
            ("market_session", market_session),
            ("session_snapshot_id", session_snapshot_id),
            ("observed_at", observed_at),
            ("evidence_profile", evidence_profile),
            ("missing_evidence", missing_evidence),
            ("knowledge_time_status", knowledge_time_status),
            ("records", records),
            ("record_count", len(records)),
            ("identity_issue_count", identity_issue_count),
            ("source_identity_claims", source_identity_claims),
            ("collection_only", True),
            ("actionable", False),
            ("training_eligible", False),
            ("feature_eligible", False),
            ("label_eligible", False),
            ("alert_eligible", False),
            ("execution_eligible", False),
        ):
            object.__setattr__(value, name, item)
        # The archive verifier already re-derived every record/claim identity;
        # retain all session-level lineage, shape, and posture checks without
        # serializing every leaf a second time in the same construction turn.
        if (
            not _is_sha256(value.dataset_id)
            or not _is_sha256(value.split_policy_id)
            or not _is_sha256(value.partition_id)
            or type(value.partition_role) is not ResearchSplitRole
            or not _is_sha256(value.index_snapshot_id)
            or not _is_sha256(value.range_binding_id)
            or type(value.market_session) is not date
            or not _is_sha256(value.session_snapshot_id)
            or type(value.observed_at) is not datetime
            or value.observed_at.tzinfo is None
            or value.evidence_profile not in _KNOWN_EVIDENCE_PROFILES
            or type(value.missing_evidence) is not tuple
            or any(type(item) is not str for item in value.missing_evidence)
            or value.knowledge_time_status != _UNVERIFIED_KNOWLEDGE_TIME_STATUS
            or type(value.records) is not tuple
            or any(type(item) is not NseArchiveResearchReplayRecord for item in value.records)
            or any(item.session != value.market_session for item in value.records)
            or type(value.identity_issue_count) is not int
            or value.identity_issue_count < 0
            or type(value.source_identity_claims) is not tuple
            or any(
                type(item) is not NseArchiveResearchSourceIdentityClaim
                or item.session != value.market_session
                for item in value.source_identity_claims
            )
        ):
            _fail("research replay session freshly verified assembly failed")
        if value.source_identity_claims:
            if (
                value.evidence_profile != EVIDENCE_PROFILE_UNRECONCILED
                or len(value.source_identity_claims) != len(value.records)
            ):
                _fail("research replay session source identity claim count is invalid")
            for claim, record in zip(
                value.source_identity_claims,
                value.records,
                strict=True,
            ):
                if (
                    claim.session != record.session
                    or claim.listing_key != record.listing_key
                    or claim.symbol != record.symbol
                    or claim.series != record.series
                ):
                    _fail(
                        "research replay session source identity claim binding is invalid"
                    )
        object.__setattr__(value, "replay_session_id", value._calculated_id())
        return value


def _verify_dataset_safety_posture(dataset: NseArchiveResearchDataset) -> None:
    if (
        dataset.collection_only is not True
        or dataset.actionable is not False
        or dataset.training_eligible is not False
        or dataset.feature_eligible is not False
        or dataset.label_eligible is not False
        or dataset.alert_eligible is not False
        or dataset.execution_eligible is not False
        or dataset.identity_resolution_complete is not False
        or dataset.corporate_action_adjustment_complete is not False
    ):
        _fail("research replay dataset safety posture is invalid")


def _session_partition_index(
    dataset: NseArchiveResearchDataset,
) -> dict[date, NseArchiveResearchDatasetSplitPartition]:
    index: dict[date, NseArchiveResearchDatasetSplitPartition] = {}
    for partition in dataset.partitions:
        for session in partition.sessions:
            if session in index:
                _fail("research replay dataset partitions overlap")
            index[session] = partition
    return index


def _verify_range_matches_binding(
    verified: VerifiedNseHistoricalArchiveRange | StreamingVerifiedNseHistoricalArchiveRange,
    binding: NseArchiveResearchRangeBinding,
) -> None:
    if type(verified) not in (
        VerifiedNseHistoricalArchiveRange,
        StreamingVerifiedNseHistoricalArchiveRange,
    ):
        _fail("research replay archive range type is invalid")
    deferred_streaming_evidence = (
        type(verified) is StreamingVerifiedNseHistoricalArchiveRange
        and not verified.evidence_profile_counts
    )
    deferred_streaming_identity = (
        type(verified) is StreamingVerifiedNseHistoricalArchiveRange
        and verified.identity_issue_count == -1
        and verified.identity_quarantined_session_count == -1
    )
    if (
        verified.index_snapshot_id != binding.index_snapshot_id
        or verified.range_start != binding.range_start
        or verified.range_end != binding.range_end
        or verified.session_snapshot_ids != binding.session_snapshot_ids
        or verified.record_count != binding.record_count
        or (
            not deferred_streaming_identity
            and verified.identity_issue_count != binding.identity_issue_count
        )
        or (
            not deferred_streaming_identity
            and verified.identity_quarantined_session_count
            != binding.identity_quarantined_session_count
        )
        or (
            not deferred_streaming_evidence
            and verified.incomplete_evidence_session_count
            != binding.incomplete_evidence_session_count
        )
    ):
        _fail("research replay archive range does not match its binding")
    profile_counts = verified.evidence_profile_counts
    if not deferred_streaming_evidence:
        if (
            not isinstance(profile_counts, Mapping)
            or set(profile_counts) != set(_KNOWN_EVIDENCE_PROFILES)
            or any(type(value) is not int or value < 0 for value in profile_counts.values())
            or tuple(sorted(profile_counts.items())) != binding.evidence_profile_counts
        ):
            _fail("research replay archive range evidence profile counts do not match its binding")
    if type(verified) is VerifiedNseHistoricalArchiveRange:
        if (
            type(verified.sessions) is not tuple
            or len(verified.sessions) != len(binding.accepted_sessions)
            or len(verified.sessions) != len(binding.session_snapshot_ids)
        ):
            _fail("research replay archive range session lineage does not match its binding")
        for stored, expected_snapshot_id, expected_session in zip(
            verified.sessions,
            binding.session_snapshot_ids,
            binding.accepted_sessions,
            strict=True,
        ):
            if type(stored) is not StoredMarketSnapshot:
                _fail("research replay archive range session snapshot type is invalid")
            if stored.manifest.snapshot_id != expected_snapshot_id:
                _fail("research replay archive range session snapshot id does not match its binding")
            payload = stored.normalized_payload
            if not isinstance(payload, Mapping) or payload.get("session") != expected_session:
                _fail("research replay archive range session date does not match its binding")


def _verify_identity_issue_payload(value: object, expected_session: date) -> None:
    if not isinstance(value, Mapping) or set(value) != _IDENTITY_ISSUE_KEYS:
        _fail("research replay identity issue shape is invalid")
    issue_id = value["issue_id"]
    if not _is_sha256(issue_id):
        _fail("research replay identity issue identity is invalid")
    session = _required(
        value["session"], date, "research replay identity issue session is invalid"
    )
    if session != expected_session:
        _fail("research replay identity issue session does not match its session")
    listing_key = _required(
        value["listing_key"], str, "research replay identity issue listing key is invalid"
    )
    series = _required(
        value["series"], str, "research replay identity issue series is invalid"
    )
    udiff_financial_instrument_id = _optional(
        value["udiff_financial_instrument_id"],
        int,
        "research replay identity issue udiff financial instrument id is invalid",
    )
    security_master_financial_instrument_id = _optional(
        value["security_master_financial_instrument_id"],
        int,
        "research replay identity issue security master financial instrument id is invalid",
    )
    security_master_source_identifier = _optional(
        value["security_master_source_identifier"],
        str,
        "research replay identity issue security master source identifier is invalid",
    )
    udiff_source_identifier = _optional(
        value["udiff_source_identifier"],
        str,
        "research replay identity issue udiff source identifier is invalid",
    )
    status = _required(
        value["status"], str, "research replay identity issue status is invalid"
    )
    if status not in _IDENTITY_STATUSES:
        _fail("research replay identity issue status is invalid")
    canonical_issue = {
        "session": session,
        "listing_key": listing_key,
        "series": series,
        "udiff_financial_instrument_id": udiff_financial_instrument_id,
        "security_master_financial_instrument_id": security_master_financial_instrument_id,
        "security_master_source_identifier": security_master_source_identifier,
        "udiff_source_identifier": udiff_source_identifier,
        "status": status,
    }
    expected_issue_id = content_id(
        {"schema": _IDENTITY_ISSUE_CONTENT_SCHEMA_VERSION, **canonical_issue}, length=64
    )
    if issue_id != expected_issue_id:
        _fail("research replay identity issue identity failed")


def _verify_identity_issues_payload(
    identity_issues: object, identity_issue_count: int, expected_session: date
) -> None:
    if type(identity_issues) is not tuple or len(identity_issues) != identity_issue_count:
        _fail("research replay session identity issue accounting is invalid")
    for issue in identity_issues:
        _verify_identity_issue_payload(issue, expected_session)


def _verify_session_payload(
    stored: StoredMarketSnapshot, expected_session: date
) -> tuple[str, tuple[str, ...], str, tuple[object, ...], int, tuple[object, ...]]:
    payload = stored.normalized_payload
    if not isinstance(payload, Mapping) or payload.get("session") != expected_session:
        _fail("research replay session payload is invalid")
    schema = payload.get("schema_version")
    if schema == NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION_V1:
        if set(payload) != _SESSION_KEYS_V1:
            _fail("research replay session payload shape is invalid")
        evidence_profile = EVIDENCE_PROFILE_COMPLETE
        missing_evidence: object = ()
        source_identity_claims_payload: object = ()
    elif schema == NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION_V2:
        if set(payload) != _SESSION_KEYS_V2:
            _fail("research replay session payload shape is invalid")
        evidence_profile = payload.get("evidence_profile")
        missing_evidence = payload.get("missing_evidence")
        source_identity_claims_payload = ()
    elif schema == NSE_HISTORICAL_ARCHIVE_SCHEMA_VERSION:
        if set(payload) != _SESSION_KEYS_V3:
            _fail("research replay session payload shape is invalid")
        evidence_profile = payload.get("evidence_profile")
        missing_evidence = payload.get("missing_evidence")
        source_identity_claims_payload = payload.get("source_identity_claims")
        if type(source_identity_claims_payload) is not tuple:
            _fail("research replay session source identity claims are invalid")
    else:
        _fail("research replay session schema is invalid")
    if evidence_profile not in _KNOWN_EVIDENCE_PROFILES:
        _fail("research replay session evidence profile is invalid")
    if type(missing_evidence) is not tuple or any(
        type(value) is not str for value in missing_evidence
    ):
        _fail("research replay session missing evidence is invalid")
    knowledge_time_status = payload.get("knowledge_time_status")
    if knowledge_time_status != _UNVERIFIED_KNOWLEDGE_TIME_STATUS:
        _fail("research replay session knowledge time status is invalid")
    records = payload.get("records")
    identity_issue_count = payload.get("identity_issue_count")
    if (
        type(records) is not tuple
        or type(identity_issue_count) is not int
        or identity_issue_count < 0
        or stored.manifest.record_count != len(records)
    ):
        _fail("research replay session record accounting is invalid")
    _verify_identity_issues_payload(
        payload.get("identity_issues"), identity_issue_count, expected_session
    )
    if (
        payload.get("collection_only") is not True
        or payload.get("actionable") is not False
        or payload.get("training_eligible") is not False
    ):
        _fail("research replay session safety posture is invalid")
    return (
        evidence_profile,
        missing_evidence,
        knowledge_time_status,
        records,
        identity_issue_count,
        source_identity_claims_payload,
    )


def _build_replay_record(value: object) -> NseArchiveResearchReplayRecord:
    if not isinstance(value, Mapping) or set(value) != _RECORD_KEYS:
        _fail("research replay record shape is invalid")
    record_id = _required(value["record_id"], str, "research replay record identity is invalid")
    if not _is_sha256(record_id):
        _fail("research replay record identity is invalid")
    session = _required(value["session"], date, "research replay record session is invalid")
    listing_key = _required(
        value["listing_key"], str, "research replay record listing key is invalid"
    )
    symbol = _required(value["symbol"], str, "research replay record symbol is invalid")
    series = _required(value["series"], str, "research replay record series is invalid")
    financial_instrument_id = _optional(
        value["financial_instrument_id"],
        int,
        "research replay record financial instrument id is invalid",
    )
    security_master_financial_instrument_id = _optional(
        value["security_master_financial_instrument_id"],
        int,
        "research replay record security master financial instrument id is invalid",
    )
    security_source_record_id = _optional(
        value["security_source_record_id"],
        str,
        "research replay record security source record id is invalid",
    )
    security_master_source_identifier = _optional(
        value["security_master_source_identifier"],
        str,
        "research replay record security master source identifier is invalid",
    )
    udiff_source_identifier = _optional(
        value["udiff_source_identifier"],
        str,
        "research replay record udiff source identifier is invalid",
    )
    identity_status = _required(
        value["identity_status"], str, "research replay record identity status is invalid"
    )
    if identity_status not in _IDENTITY_STATUSES:
        _fail("research replay record identity status is invalid")
    validated_isin = _optional(
        value["validated_isin"], str, "research replay record validated isin is invalid"
    )
    normal_market_status = _optional(
        value["normal_market_status"],
        int,
        "research replay record normal market status is invalid",
    )
    normal_market_eligible = _optional(
        value["normal_market_eligible"],
        bool,
        "research replay record normal market eligibility is invalid",
    )
    permitted_to_trade = _optional(
        value["permitted_to_trade"],
        int,
        "research replay record permitted-to-trade flag is invalid",
    )
    delete_flag = _optional(
        value["delete_flag"], str, "research replay record delete flag is invalid"
    )
    previous_close = _required(
        value["previous_close"], Decimal, "research replay record previous close is invalid"
    )
    open_price = _required(
        value["open"], Decimal, "research replay record open price is invalid"
    )
    high = _required(value["high"], Decimal, "research replay record high price is invalid")
    low = _required(value["low"], Decimal, "research replay record low price is invalid")
    last = _required(value["last"], Decimal, "research replay record last price is invalid")
    close = _required(value["close"], Decimal, "research replay record close price is invalid")
    average_price = _required(
        value["average_price"], Decimal, "research replay record average price is invalid"
    )
    volume = _required(value["volume"], int, "research replay record volume is invalid")
    turnover_lacs = _required(
        value["turnover_lacs"], Decimal, "research replay record turnover is invalid"
    )
    trade_count = _required(
        value["trade_count"], int, "research replay record trade count is invalid"
    )
    delivery_quantity = _optional(
        value["delivery_quantity"], int, "research replay record delivery quantity is invalid"
    )
    delivery_percent = _optional(
        value["delivery_percent"], Decimal, "research replay record delivery percent is invalid"
    )
    surveillance = value["surveillance_indicators"]
    if not isinstance(surveillance, Mapping) or any(
        type(key) is not str or type(item) is not str for key, item in surveillance.items()
    ):
        _fail("research replay record surveillance evidence is invalid")
    surveillance_tuple = tuple(sorted(surveillance.items()))

    identity_matched = (
        identity_status == "MATCHED_SAME_SESSION"
        and type(validated_isin) is str
        and validated_isin != ""
    )
    normal_market_eligibility_verified = (
        type(normal_market_eligible) is bool
        and security_master_financial_instrument_id is not None
        and security_master_source_identifier is not None
    )

    return NseArchiveResearchReplayRecord(
        record_id=record_id,
        session=session,
        listing_key=listing_key,
        symbol=symbol,
        series=series,
        financial_instrument_id=financial_instrument_id,
        security_master_financial_instrument_id=security_master_financial_instrument_id,
        security_source_record_id=security_source_record_id,
        security_master_source_identifier=security_master_source_identifier,
        udiff_source_identifier=udiff_source_identifier,
        identity_status=identity_status,
        validated_isin=validated_isin,
        normal_market_status=normal_market_status,
        normal_market_eligible=normal_market_eligible,
        permitted_to_trade=permitted_to_trade,
        delete_flag=delete_flag,
        previous_close=previous_close,
        open=open_price,
        high=high,
        low=low,
        last=last,
        close=close,
        average_price=average_price,
        volume=volume,
        turnover_lacs=turnover_lacs,
        trade_count=trade_count,
        delivery_quantity=delivery_quantity,
        delivery_percent=delivery_percent,
        surveillance_indicators=surveillance_tuple,
        identity_matched=identity_matched,
        normal_market_eligibility_verified=normal_market_eligibility_verified,
    )


def _build_replay_source_identity_claim(
    value: object,
) -> NseArchiveResearchSourceIdentityClaim:
    if not isinstance(value, Mapping) or set(value) != _SOURCE_IDENTITY_CLAIM_KEYS:
        _fail("research replay source identity claim shape is invalid")
    claim_id = _required(
        value["claim_id"], str, "research replay source identity claim identity is invalid"
    )
    if not _is_sha256(claim_id):
        _fail("research replay source identity claim identity is invalid")
    session = _required(
        value["session"], date, "research replay source identity claim session is invalid"
    )
    listing_key = _required(
        value["listing_key"],
        str,
        "research replay source identity claim listing key is invalid",
    )
    symbol = _required(
        value["symbol"], str, "research replay source identity claim symbol is invalid"
    )
    series = _required(
        value["series"], str, "research replay source identity claim series is invalid"
    )
    claimed_isin = _required(
        value["claimed_isin"],
        str,
        "research replay source identity claim ISIN is invalid",
    )
    source_kind = _required(
        value["source_kind"],
        str,
        "research replay source identity claim source kind is invalid",
    )
    source_entry_name = _required(
        value["source_entry_name"],
        str,
        "research replay source identity claim entry name is invalid",
    )
    source_entry_sha256 = _required(
        value["source_entry_sha256"],
        str,
        "research replay source identity claim entry hash is invalid",
    )
    source_row_number = value["source_row_number"]
    if type(source_row_number) is not int or isinstance(source_row_number, bool):
        _fail("research replay source identity claim row number is invalid")
    status = _required(
        value["status"], str, "research replay source identity claim status is invalid"
    )
    return NseArchiveResearchSourceIdentityClaim(
        claim_id=claim_id,
        session=session,
        listing_key=listing_key,
        symbol=symbol,
        series=series,
        claimed_isin=claimed_isin,
        source_kind=source_kind,
        source_entry_name=source_entry_name,
        source_entry_sha256=source_entry_sha256,
        source_row_number=source_row_number,
        status=status,
    )


def _build_replay_session(
    dataset: NseArchiveResearchDataset,
    binding: NseArchiveResearchRangeBinding,
    partition: NseArchiveResearchDatasetSplitPartition,
    session_snapshot_id: str,
    accepted_session: date,
    stored: StoredMarketSnapshot,
) -> NseArchiveResearchReplaySession:
    (
        evidence_profile,
        missing_evidence,
        knowledge_time_status,
        records_payload,
        identity_issue_count,
        source_identity_claims_payload,
    ) = _verify_session_payload(stored, accepted_session)
    records = tuple(_build_replay_record(value) for value in records_payload)
    source_identity_claims = tuple(
        _build_replay_source_identity_claim(value)
        for value in source_identity_claims_payload
    )
    return NseArchiveResearchReplaySession._from_freshly_verified_archive(
        dataset_id=dataset.dataset_id,
        split_policy_id=dataset.split_policy_id,
        partition_id=partition.partition_id,
        partition_role=partition.role,
        index_snapshot_id=binding.index_snapshot_id,
        range_binding_id=binding.binding_id,
        market_session=accepted_session,
        session_snapshot_id=session_snapshot_id,
        observed_at=stored.manifest.observed_at,
        evidence_profile=evidence_profile,
        missing_evidence=missing_evidence,
        knowledge_time_status=knowledge_time_status,
        records=records,
        identity_issue_count=identity_issue_count,
        source_identity_claims=source_identity_claims,
    )


def _replay_range(
    dataset: NseArchiveResearchDataset,
    binding: NseArchiveResearchRangeBinding,
    reader: NseHistoricalArchiveSnapshotReader,
    partition_index: Mapping[date, NseArchiveResearchDatasetSplitPartition],
) -> Iterator[NseArchiveResearchReplaySession]:
    range_load_failed = False
    verified: (
        VerifiedNseHistoricalArchiveRange
        | StreamingVerifiedNseHistoricalArchiveRange
        | None
    ) = None
    try:
        if callable(
            getattr(type(reader), "get_hash_verified_from_date_partition", None)
        ):
            try:
                verified = stream_verified_nse_historical_archive_range(
                    reader,
                    index_snapshot_id=binding.index_snapshot_id,
                )
            except NseHistoricalArchiveLegacyIndexSchema:
                # Legacy v1/v2 ranges retain the original full-range verifier.
                verified = load_verified_nse_historical_archive_range(
                    reader,
                    index_snapshot_id=binding.index_snapshot_id,
                )
        else:
            verified = load_verified_nse_historical_archive_range(
                reader, index_snapshot_id=binding.index_snapshot_id
            )
    except Exception:
        range_load_failed = True
    if range_load_failed or verified is None:
        _fail("research replay archive range could not be loaded")

    match_failed = False
    try:
        _verify_range_matches_binding(verified, binding)
    except NseArchiveResearchReplayError:
        raise
    except Exception:
        match_failed = True
    if match_failed:
        _fail("research replay archive range does not match its binding")

    streamed_profile_counts = {profile: 0 for profile in _KNOWN_EVIDENCE_PROFILES}
    streamed_session_count = 0
    streamed_identity_issue_count = 0
    streamed_quarantined_session_count = 0
    for session_snapshot_id, accepted_session, stored in zip(
        binding.session_snapshot_ids,
        binding.accepted_sessions,
        verified.sessions,
        strict=True,
    ):
        if type(stored) is not StoredMarketSnapshot:
            _fail("research replay archive range session snapshot type is invalid")
        if stored.manifest.snapshot_id != session_snapshot_id:
            _fail("research replay archive range session snapshot id does not match its binding")
        payload = stored.normalized_payload
        if not isinstance(payload, Mapping) or payload.get("session") != accepted_session:
            _fail("research replay archive range session date does not match its binding")
        evidence_profile = payload.get("evidence_profile")
        if evidence_profile not in _KNOWN_EVIDENCE_PROFILES:
            _fail("research replay archive range session evidence profile is invalid")
        streamed_profile_counts[evidence_profile] += 1
        streamed_session_count += 1
        identity_issue_count = payload.get("identity_issue_count")
        if type(identity_issue_count) is not int or identity_issue_count < 0:
            _fail("research replay archive range session identity accounting is invalid")
        streamed_identity_issue_count += identity_issue_count
        streamed_quarantined_session_count += identity_issue_count > 0
        partition = partition_index.get(accepted_session)
        if partition is None:
            _fail("research replay session partition role is invalid")
        build_failed = False
        session_obj: NseArchiveResearchReplaySession | None = None
        try:
            session_obj = _build_replay_session(
                dataset,
                binding,
                partition,
                session_snapshot_id,
                accepted_session,
                stored,
            )
        except NseArchiveResearchReplayError:
            raise
        except Exception:
            build_failed = True
        if build_failed or session_obj is None:
            _fail("research replay session could not be reconstructed")
        if (
            type(verified) is StreamingVerifiedNseHistoricalArchiveRange
            and streamed_session_count == len(binding.accepted_sessions)
            and (
                tuple(sorted(streamed_profile_counts.items()))
                != binding.evidence_profile_counts
                or sum(
                    count
                    for profile, count in streamed_profile_counts.items()
                    if profile != EVIDENCE_PROFILE_COMPLETE
                )
                != binding.incomplete_evidence_session_count
                or streamed_identity_issue_count != binding.identity_issue_count
                or streamed_quarantined_session_count
                != binding.identity_quarantined_session_count
            )
        ):
            _fail(
                "research replay archive range evidence profile counts do not "
                "match its binding"
            )
        yield session_obj
    if type(verified) is StreamingVerifiedNseHistoricalArchiveRange:
        if (
            streamed_session_count != len(binding.accepted_sessions)
        ):
            _fail(
                "research replay archive range session lineage does not match "
                "its binding"
            )


def _iter_replay_sessions(
    dataset: NseArchiveResearchDataset,
    partition_index: Mapping[date, NseArchiveResearchDatasetSplitPartition],
    reader: NseHistoricalArchiveSnapshotReader,
) -> Iterator[NseArchiveResearchReplaySession]:
    for binding in dataset.range_bindings:
        yield from _replay_range(dataset, binding, reader, partition_index)


def iter_verified_nse_archive_research_sessions(
    dataset: NseArchiveResearchDataset,
    reader: NseHistoricalArchiveSnapshotReader,
) -> Iterator[NseArchiveResearchReplaySession]:
    """Replay one sealed research dataset's pinned ranges into typed sessions.

    Calls ``load_verified_nse_historical_archive_range`` exactly once per
    exact ``dataset.range_bindings`` index ID, in stored order, and never
    otherwise touches ``reader``. Only the range currently being replayed is
    held in memory; stopping iteration early never loads a later range and
    never constitutes a completed or publishable research artifact.
    """

    if type(dataset) is not NseArchiveResearchDataset:
        _fail("research replay dataset is invalid")
    if reader is None:
        _fail("research replay reader is invalid")

    dataset_identity_failed = False
    try:
        dataset.verify_content_identity()
    except Exception:
        dataset_identity_failed = True
    if dataset_identity_failed:
        _fail("research replay dataset identity failed")
    _verify_dataset_safety_posture(dataset)

    partition_index = _session_partition_index(dataset)
    return _iter_replay_sessions(dataset, partition_index, reader)


def _iter_freshly_verified_nse_archive_research_sessions(
    dataset: NseArchiveResearchDataset,
    reader: NseHistoricalArchiveSnapshotReader,
) -> Iterator[NseArchiveResearchReplaySession]:
    """Private construction-chain iterator for exact hash-verified readers."""

    if not callable(
        getattr(type(reader), "get_hash_verified_from_date_partition", None)
    ):
        _fail("research replay reader is not an exact hash-verified reader")
    return iter_verified_nse_archive_research_sessions(dataset, reader)
