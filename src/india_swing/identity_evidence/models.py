from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path, PurePath
from urllib.parse import urlsplit

from india_swing.identity import content_id
from india_swing.identity_registry import IdentityAdjudicationRequirement
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.models import AcquisitionMode


IDENTITY_EVIDENCE_DECLARATION_SCHEMA_VERSION = "nse-cm-identity-evidence-declaration/v1"
IDENTITY_EVIDENCE_CLAIM_SCHEMA_VERSION = "nse-cm-identity-evidence-claim/v1"
IDENTITY_EVIDENCE_ARTIFACT_SCHEMA_VERSION = "identity-evidence-artifact/v1"
IDENTITY_EVIDENCE_CODEC_VERSION = "identity-evidence-normalized-json/v1"
IDENTITY_EVIDENCE_PARSER_VERSION = "identity-evidence-parser/v1"
IDENTITY_EVIDENCE_POLICY_VERSION = "collection-only-no-adjudication/v1"
IDENTITY_EVIDENCE_DATASET = "nse-cm-identity-evidence"
IDENTITY_EVIDENCE_PUBLICATION_TIME_STATUS = "CLAIMED_UNVERIFIED_LOCAL_OBSERVATION_ONLY"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_DOCUMENT_ID = re.compile(r"[A-Z0-9][A-Z0-9/._-]{2,127}\Z")
_SYMBOL = re.compile(r"[A-Z0-9][A-Z0-9&._-]{0,31}\Z")
_SERIES = re.compile(r"[A-Z0-9][A-Z0-9_-]{0,7}\Z")
_ISIN = re.compile(r"IN[A-Z0-9]{9}[0-9]\Z")
_NSE_HOSTS = {"www.nseindia.com", "nseindia.com", "nsearchives.nseindia.com"}


class IdentityEvidenceError(RuntimeError):
    pass


class IdentityEvidenceIntegrityError(IdentityEvidenceError):
    pass


class IdentityEvidenceConflict(IdentityEvidenceError):
    pass


class IdentityEvidenceNotFound(IdentityEvidenceError):
    pass


class IdentityEvidenceSourceKind(str, Enum):
    CORPORATE_ACTION_CSV = "CORPORATE_ACTION_CSV"
    LISTING_CIRCULAR_PDF = "LISTING_CIRCULAR_PDF"

    @property
    def media_type(self) -> str:
        if self is IdentityEvidenceSourceKind.CORPORATE_ACTION_CSV:
            return "text/csv"
        return "application/pdf"


def _sha(value: str, name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{name} must be a full lowercase SHA-256")


def _canonical(value: str, name: str, maximum: int) -> None:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError(f"{name} must be canonical non-empty text")


def _safe_basename(value: str, name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or PurePath(value).name != value
        or "/" in value
        or "\\" in value
    ):
        raise ValueError(f"{name} must be a safe basename")


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _official_nse_url(value: str, name: str) -> None:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be an official NSE HTTPS URL")
    parts = urlsplit(value)
    if (
        parts.scheme != "https"
        or parts.hostname not in _NSE_HOSTS
        or parts.username is not None
        or parts.password is not None
        or parts.fragment
        or len(value) > 2_048
    ):
        raise ValueError(f"{name} must be an official NSE HTTPS URL")


@dataclass(frozen=True, slots=True)
class IdentityEvidenceLocator:
    page: int | None
    row: int | None
    section: str

    def __post_init__(self) -> None:
        if self.page is not None and (type(self.page) is not int or self.page <= 0):
            raise ValueError("locator page must be null or a positive integer")
        if self.row is not None and (type(self.row) is not int or self.row <= 0):
            raise ValueError("locator row must be null or a positive integer")
        _canonical(self.section, "locator section", 256)


@dataclass(frozen=True, slots=True)
class IdentityEvidenceClaim:
    source_sha256: str
    claimed_document_id: str
    source_kind: IdentityEvidenceSourceKind
    candidate_id: str
    requirement: IdentityAdjudicationRequirement
    effective_date: date | None
    symbol: str
    series: str
    isin: str | None
    locator: IdentityEvidenceLocator
    claim_text: str
    schema_version: str = IDENTITY_EVIDENCE_CLAIM_SCHEMA_VERSION
    claim_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.source_sha256, "claim source_sha256")
        if not isinstance(self.claimed_document_id, str) or _DOCUMENT_ID.fullmatch(
            self.claimed_document_id
        ) is None:
            raise ValueError("claimed_document_id must be canonical uppercase text")
        if type(self.source_kind) is not IdentityEvidenceSourceKind:
            raise TypeError("source_kind must be exact")
        _sha(self.candidate_id, "claim candidate_id")
        if type(self.requirement) is not IdentityAdjudicationRequirement:
            raise TypeError("claim requirement must be exact")
        if self.effective_date is not None and type(self.effective_date) is not date:
            raise TypeError("effective_date must be a date or null")
        if self.requirement in {
            IdentityAdjudicationRequirement.OFFICIAL_LISTING_LIFECYCLE,
            IdentityAdjudicationRequirement.OFFICIAL_LISTING_STATUS,
        } and self.effective_date is None:
            raise ValueError("listing lifecycle/status claims require an effective_date")
        if not isinstance(self.symbol, str) or _SYMBOL.fullmatch(self.symbol) is None:
            raise ValueError("claim symbol must be canonical uppercase NSE text")
        if not isinstance(self.series, str) or _SERIES.fullmatch(self.series) is None:
            raise ValueError("claim series must be canonical uppercase NSE text")
        if self.isin is not None and (
            not isinstance(self.isin, str) or _ISIN.fullmatch(self.isin) is None
        ):
            raise ValueError("claim isin must be null or a syntactically valid Indian ISIN")
        if type(self.locator) is not IdentityEvidenceLocator:
            raise TypeError("claim locator must be exact")
        if self.source_kind is IdentityEvidenceSourceKind.LISTING_CIRCULAR_PDF:
            if self.locator.page is None or self.locator.row is not None:
                raise ValueError("PDF claims require page and prohibit row")
        else:
            if self.locator.row is None or self.locator.page is not None:
                raise ValueError("CSV claims require row and prohibit page")
        _canonical(self.claim_text, "claim_text", 1_000)
        if self.schema_version != IDENTITY_EVIDENCE_CLAIM_SCHEMA_VERSION:
            raise ValueError("unsupported identity-evidence claim schema")
        object.__setattr__(self, "claim_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema_version": self.schema_version,
                "source_sha256": self.source_sha256,
                "claimed_document_id": self.claimed_document_id,
                "source_kind": self.source_kind,
                "candidate_id": self.candidate_id,
                "requirement": self.requirement,
                "effective_date": self.effective_date,
                "symbol": self.symbol,
                "series": self.series,
                "isin": self.isin,
                "locator": self.locator,
                "claim_text": self.claim_text,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.claim_id != self._calculated_id():
            raise IdentityEvidenceIntegrityError("evidence claim identity failed")


@dataclass(frozen=True, slots=True)
class ParsedIdentityEvidenceDeclaration:
    exchange: str
    segment: str
    claimed_authority: str
    source_kind: IdentityEvidenceSourceKind
    claimed_document_id: str
    claimed_issue_date: date
    claimed_publication_at: datetime | None
    claimed_source_url: str
    source_filename: str
    source_media_type: str
    source_byte_count: int
    source_sha256: str
    claims: tuple[IdentityEvidenceClaim, ...]
    schema_version: str = IDENTITY_EVIDENCE_DECLARATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (self.exchange, self.segment, self.claimed_authority) != ("NSE", "CM", "NSE"):
            raise ValueError("identity evidence must be pinned to NSE CM")
        if type(self.source_kind) is not IdentityEvidenceSourceKind:
            raise TypeError("source_kind must be exact")
        if not isinstance(self.claimed_document_id, str) or _DOCUMENT_ID.fullmatch(
            self.claimed_document_id
        ) is None:
            raise ValueError("claimed_document_id must be canonical uppercase text")
        if type(self.claimed_issue_date) is not date:
            raise TypeError("claimed_issue_date must be a date")
        if self.claimed_publication_at is not None:
            object.__setattr__(
                self,
                "claimed_publication_at",
                _utc(self.claimed_publication_at, "claimed_publication_at"),
            )
        _official_nse_url(self.claimed_source_url, "claimed_source_url")
        _safe_basename(self.source_filename, "source_filename")
        if self.source_media_type != self.source_kind.media_type:
            raise ValueError("source media type disagrees with source_kind")
        if type(self.source_byte_count) is not int or self.source_byte_count <= 0:
            raise ValueError("source_byte_count must be positive")
        _sha(self.source_sha256, "source_sha256")
        if (
            type(self.claims) is not tuple
            or not self.claims
            or any(type(value) is not IdentityEvidenceClaim for value in self.claims)
            or self.claims != tuple(sorted(self.claims, key=lambda value: value.claim_id))
            or len({value.claim_id for value in self.claims}) != len(self.claims)
        ):
            raise ValueError("claims must be a non-empty unique claim-ID-ordered exact tuple")
        for claim in self.claims:
            claim.verify_content_identity()
            if (
                claim.source_sha256 != self.source_sha256
                or claim.claimed_document_id != self.claimed_document_id
                or claim.source_kind is not self.source_kind
            ):
                raise ValueError("claim lineage disagrees with its declaration")
        if self.schema_version != IDENTITY_EVIDENCE_DECLARATION_SCHEMA_VERSION:
            raise ValueError("unsupported identity-evidence declaration schema")

    @property
    def claim_ids(self) -> tuple[str, ...]:
        return tuple(value.claim_id for value in self.claims)

    def verify_content_identity(self) -> None:
        for value in self.claims:
            if type(value) is not IdentityEvidenceClaim:
                raise IdentityEvidenceIntegrityError("declaration contains an invalid claim")
            value.verify_content_identity()


@dataclass(frozen=True, slots=True)
class IdentityEvidenceArtifactManifest:
    artifact_id: str
    manifest_id: str
    first_seen_at: datetime
    validated_at: datetime
    original_source_filename: str
    original_declaration_filename: str
    source_sha256: str
    declaration_sha256: str
    normalized_sha256: str
    source_byte_count: int
    declaration_byte_count: int
    normalized_byte_count: int
    claimed_document_id: str
    claimed_issue_date: date
    claimed_source_url: str
    source_kind: IdentityEvidenceSourceKind
    claim_ids: tuple[str, ...]
    acquisition_mode: AcquisitionMode = AcquisitionMode.UNVERIFIED_MANUAL_FILE
    readiness: ReferenceReadiness = ReferenceReadiness.COLLECTION_ONLY
    actionable: bool = False
    stable_identity_assigned: bool = False
    publication_time_status: str = IDENTITY_EVIDENCE_PUBLICATION_TIME_STATUS
    parser_version: str = IDENTITY_EVIDENCE_PARSER_VERSION
    codec_version: str = IDENTITY_EVIDENCE_CODEC_VERSION
    policy_version: str = IDENTITY_EVIDENCE_POLICY_VERSION
    schema_version: str = IDENTITY_EVIDENCE_ARTIFACT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        for value, name in (
            (self.artifact_id, "artifact_id"),
            (self.manifest_id, "manifest_id"),
            (self.source_sha256, "source_sha256"),
            (self.declaration_sha256, "declaration_sha256"),
            (self.normalized_sha256, "normalized_sha256"),
        ):
            _sha(value, name)
        object.__setattr__(self, "first_seen_at", _utc(self.first_seen_at, "first_seen_at"))
        object.__setattr__(self, "validated_at", _utc(self.validated_at, "validated_at"))
        if self.validated_at < self.first_seen_at:
            raise ValueError("evidence validation cannot precede observation")
        _safe_basename(self.original_source_filename, "original_source_filename")
        _safe_basename(self.original_declaration_filename, "original_declaration_filename")
        for value in (
            self.source_byte_count,
            self.declaration_byte_count,
            self.normalized_byte_count,
        ):
            if type(value) is not int or value <= 0:
                raise ValueError("evidence byte counts must be positive")
        if type(self.source_kind) is not IdentityEvidenceSourceKind:
            raise TypeError("manifest source_kind must be exact")
        if not isinstance(self.claimed_document_id, str) or _DOCUMENT_ID.fullmatch(
            self.claimed_document_id
        ) is None:
            raise ValueError("manifest claimed_document_id must be canonical uppercase text")
        if type(self.claimed_issue_date) is not date:
            raise TypeError("manifest claimed_issue_date must be a date")
        _official_nse_url(self.claimed_source_url, "manifest claimed_source_url")
        if self.claim_ids != tuple(sorted(set(self.claim_ids))) or not self.claim_ids:
            raise ValueError("manifest claim_ids must be non-empty, sorted, and unique")
        for value in self.claim_ids:
            _sha(value, "claim_id")
        if (
            self.acquisition_mode is not AcquisitionMode.UNVERIFIED_MANUAL_FILE
            or self.readiness is not ReferenceReadiness.COLLECTION_ONLY
            or self.actionable is not False
            or self.stable_identity_assigned is not False
            or self.publication_time_status != IDENTITY_EVIDENCE_PUBLICATION_TIME_STATUS
            or self.parser_version != IDENTITY_EVIDENCE_PARSER_VERSION
            or self.codec_version != IDENTITY_EVIDENCE_CODEC_VERSION
            or self.policy_version != IDENTITY_EVIDENCE_POLICY_VERSION
            or self.schema_version != IDENTITY_EVIDENCE_ARTIFACT_SCHEMA_VERSION
        ):
            raise ValueError("identity evidence must remain collection-only and non-actionable")


@dataclass(frozen=True, slots=True)
class StoredIdentityEvidenceArtifact:
    path: Path
    manifest: IdentityEvidenceArtifactManifest
    parsed: ParsedIdentityEvidenceDeclaration
    source_bytes: bytes
    declaration_bytes: bytes
    normalized_bytes: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.path, Path):
            raise TypeError("stored evidence path must be a Path")
        if type(self.manifest) is not IdentityEvidenceArtifactManifest:
            raise TypeError("stored evidence manifest must be exact")
        if type(self.parsed) is not ParsedIdentityEvidenceDeclaration:
            raise TypeError("stored evidence declaration must be exact")
        if any(type(value) is not bytes for value in (
            self.source_bytes, self.declaration_bytes, self.normalized_bytes
        )):
            raise TypeError("stored evidence payloads must be exact bytes")
