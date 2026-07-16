from __future__ import annotations

import csv
import hashlib
import io
import json
from datetime import date, datetime

from india_swing.identity_registry import IdentityAdjudicationRequirement

from .models import (
    IDENTITY_EVIDENCE_DECLARATION_SCHEMA_VERSION,
    IdentityEvidenceClaim,
    IdentityEvidenceIntegrityError,
    IdentityEvidenceLocator,
    IdentityEvidenceSourceKind,
    ParsedIdentityEvidenceDeclaration,
)


MAXIMUM_IDENTITY_EVIDENCE_SOURCE_BYTES = 32 * 1024 * 1024
MAXIMUM_IDENTITY_EVIDENCE_DECLARATION_BYTES = 2 * 1024 * 1024
MAXIMUM_IDENTITY_EVIDENCE_CLAIMS = 20_000

_ROOT_KEYS = {
    "schema_version", "exchange", "segment", "claimed_authority", "source_kind",
    "claimed_document_id", "claimed_issue_date", "claimed_publication_at",
    "claimed_source_url", "source_filename", "source_media_type",
    "source_byte_count", "source_sha256", "claims",
}
_CLAIM_KEYS = {
    "candidate_id", "requirement", "effective_date", "symbol", "series", "isin",
    "locator", "claim_text",
}
_LOCATOR_KEYS = {"page", "row", "section"}
_CORPORATE_ACTION_REQUIRED_HEADERS = {
    "SYMBOL", "COMPANY NAME", "SERIES", "PURPOSE", "EX-DATE",
}


def _reject_duplicate_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise IdentityEvidenceIntegrityError("identity evidence JSON contains a duplicate key")
        value[key] = item
    return value


def decode_strict_json(payload: bytes) -> object:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise IdentityEvidenceIntegrityError("identity evidence declaration must be UTF-8") from exc
    if text.startswith("\ufeff"):
        raise IdentityEvidenceIntegrityError("identity evidence declaration cannot contain a BOM")
    try:
        return json.loads(text, object_pairs_hook=_reject_duplicate_pairs)
    except json.JSONDecodeError as exc:
        raise IdentityEvidenceIntegrityError("identity evidence declaration is invalid JSON") from exc


def _object(value: object, keys: set[str], name: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != keys:
        raise IdentityEvidenceIntegrityError(f"{name} schema mismatch")
    return value


def _date(value: object, name: str) -> date:
    if type(value) is not str:
        raise IdentityEvidenceIntegrityError(f"{name} must be YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise IdentityEvidenceIntegrityError(f"{name} must be YYYY-MM-DD") from exc


def _publication(value: object) -> datetime | None:
    if value is None:
        return None
    if type(value) is not str:
        raise IdentityEvidenceIntegrityError("claimed_publication_at must be ISO-8601 or null")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise IdentityEvidenceIntegrityError("claimed_publication_at must be ISO-8601 or null") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise IdentityEvidenceIntegrityError("claimed_publication_at must include an offset")
    return parsed


def _validate_source(payload: bytes, filename: str, kind: IdentityEvidenceSourceKind) -> int | None:
    if kind is IdentityEvidenceSourceKind.LISTING_CIRCULAR_PDF:
        if not filename.lower().endswith(".pdf"):
            raise IdentityEvidenceIntegrityError("listing circular source requires a .pdf filename")
        if not payload.startswith(b"%PDF-") or b"%%EOF" not in payload[-2_048:]:
            raise IdentityEvidenceIntegrityError("listing circular does not satisfy the PDF envelope")
        return None
    if not filename.lower().endswith(".csv"):
        raise IdentityEvidenceIntegrityError("corporate-action source requires a .csv filename")
    try:
        text = payload.decode("utf-8-sig")
        rows = list(csv.reader(io.StringIO(text, newline="")))
        headers = rows[0]
    except (UnicodeDecodeError, csv.Error, IndexError) as exc:
        raise IdentityEvidenceIntegrityError("corporate-action source is not a readable UTF-8 CSV") from exc
    normalized = {value.strip().upper() for value in headers}
    if not _CORPORATE_ACTION_REQUIRED_HEADERS.issubset(normalized):
        raise IdentityEvidenceIntegrityError("corporate-action CSV header is unsupported")
    return len(rows)


class IdentityEvidenceDeclarationParser:
    maximum_source_bytes = MAXIMUM_IDENTITY_EVIDENCE_SOURCE_BYTES
    maximum_declaration_bytes = MAXIMUM_IDENTITY_EVIDENCE_DECLARATION_BYTES

    def parse_bytes(
        self,
        declaration_bytes: bytes,
        *,
        source_bytes: bytes,
        source_filename: str,
        declaration_filename: str,
    ) -> ParsedIdentityEvidenceDeclaration:
        if type(source_bytes) is not bytes or not source_bytes:
            raise IdentityEvidenceIntegrityError("identity evidence source must be non-empty bytes")
        if len(source_bytes) > self.maximum_source_bytes:
            raise IdentityEvidenceIntegrityError("identity evidence source exceeds the size limit")
        if type(declaration_bytes) is not bytes or not declaration_bytes:
            raise IdentityEvidenceIntegrityError("identity evidence declaration must be non-empty bytes")
        if len(declaration_bytes) > self.maximum_declaration_bytes:
            raise IdentityEvidenceIntegrityError("identity evidence declaration exceeds the size limit")
        if not isinstance(source_filename, str) or not source_filename:
            raise IdentityEvidenceIntegrityError("source filename is required")
        if not isinstance(declaration_filename, str) or not declaration_filename.lower().endswith(".json"):
            raise IdentityEvidenceIntegrityError("identity evidence declaration requires a .json filename")

        root = _object(decode_strict_json(declaration_bytes), _ROOT_KEYS, "identity evidence declaration")
        if root["schema_version"] != IDENTITY_EVIDENCE_DECLARATION_SCHEMA_VERSION:
            raise IdentityEvidenceIntegrityError("unsupported identity evidence declaration schema")
        if (root["exchange"], root["segment"], root["claimed_authority"]) != ("NSE", "CM", "NSE"):
            raise IdentityEvidenceIntegrityError("identity evidence declaration must be pinned to NSE CM")
        if root["source_filename"] != source_filename:
            raise IdentityEvidenceIntegrityError("declaration is bound to another source filename")
        try:
            kind = IdentityEvidenceSourceKind(root["source_kind"])
        except (TypeError, ValueError) as exc:
            raise IdentityEvidenceIntegrityError("unsupported identity evidence source_kind") from exc
        if root["source_media_type"] != kind.media_type:
            raise IdentityEvidenceIntegrityError("source media type disagrees with source_kind")
        if type(root["source_byte_count"]) is not int:
            raise IdentityEvidenceIntegrityError("source_byte_count must be an integer")
        source_sha256 = hashlib.sha256(source_bytes).hexdigest()
        if root["source_byte_count"] != len(source_bytes) or root["source_sha256"] != source_sha256:
            raise IdentityEvidenceIntegrityError("declaration does not bind the exact source bytes")
        csv_row_count = _validate_source(source_bytes, source_filename, kind)

        raw_claims = root["claims"]
        if type(raw_claims) is not list or not raw_claims or len(raw_claims) > MAXIMUM_IDENTITY_EVIDENCE_CLAIMS:
            raise IdentityEvidenceIntegrityError("claims must be a bounded non-empty array")
        claims: list[IdentityEvidenceClaim] = []
        for value in raw_claims:
            raw = _object(value, _CLAIM_KEYS, "identity evidence claim")
            locator = _object(raw["locator"], _LOCATOR_KEYS, "identity evidence locator")
            try:
                parsed_claim = IdentityEvidenceClaim(
                        source_sha256=source_sha256,
                        claimed_document_id=root["claimed_document_id"],
                        source_kind=kind,
                        candidate_id=raw["candidate_id"],
                        requirement=IdentityAdjudicationRequirement(raw["requirement"]),
                        effective_date=None if raw["effective_date"] is None else _date(raw["effective_date"], "effective_date"),
                        symbol=raw["symbol"],
                        series=raw["series"],
                        isin=raw["isin"],
                        locator=IdentityEvidenceLocator(
                            page=locator["page"], row=locator["row"], section=locator["section"]
                        ),
                        claim_text=raw["claim_text"],
                    )
                if csv_row_count is not None and parsed_claim.locator.row is not None and parsed_claim.locator.row > csv_row_count:
                    raise ValueError("CSV claim row does not exist in the source")
                claims.append(parsed_claim)
            except (TypeError, ValueError) as exc:
                raise IdentityEvidenceIntegrityError("invalid identity evidence claim") from exc
        claims_tuple = tuple(sorted(claims, key=lambda value: value.claim_id))
        if len({value.claim_id for value in claims_tuple}) != len(claims_tuple):
            raise IdentityEvidenceIntegrityError("identity evidence contains duplicate claims")
        try:
            return ParsedIdentityEvidenceDeclaration(
                exchange="NSE", segment="CM", claimed_authority="NSE", source_kind=kind,
                claimed_document_id=root["claimed_document_id"],
                claimed_issue_date=_date(root["claimed_issue_date"], "claimed_issue_date"),
                claimed_publication_at=_publication(root["claimed_publication_at"]),
                claimed_source_url=root["claimed_source_url"],
                source_filename=source_filename,
                source_media_type=kind.media_type,
                source_byte_count=len(source_bytes), source_sha256=source_sha256,
                claims=claims_tuple,
            )
        except (TypeError, ValueError) as exc:
            raise IdentityEvidenceIntegrityError("identity evidence declaration violates the pinned contract") from exc
