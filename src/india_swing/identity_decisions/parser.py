from __future__ import annotations

import json
from datetime import datetime

from india_swing.identity_registry import IdentityAdjudicationRequirement

from .models import (
    IDENTITY_REVIEW_DECLARATION_SCHEMA_VERSION,
    IdentityDecisionIntegrityError,
    IdentityReviewDecision,
    IdentityReviewOutcome,
    ParsedIdentityReviewBundle,
)


MAXIMUM_IDENTITY_REVIEW_DECLARATION_BYTES = 8 * 1024 * 1024
MAXIMUM_IDENTITY_REVIEW_DECISIONS = 50_000
_ROOT_KEYS = {
    "schema_version", "queue_id", "source_registry_id", "reviewer_id",
    "reviewed_at", "decisions",
}
_DECISION_KEYS = {
    "candidate_id", "requirement", "outcome", "evidence_artifact_id",
    "evidence_claim_id", "rationale",
}


def _reject_duplicate_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise IdentityDecisionIntegrityError("identity review JSON contains a duplicate key")
        value[key] = item
    return value


def decode_strict_review_json(payload: bytes) -> object:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise IdentityDecisionIntegrityError("identity review declaration must be UTF-8") from exc
    if text.startswith("\ufeff"):
        raise IdentityDecisionIntegrityError("identity review declaration cannot contain a BOM")
    try:
        return json.loads(text, object_pairs_hook=_reject_duplicate_pairs)
    except json.JSONDecodeError as exc:
        raise IdentityDecisionIntegrityError("identity review declaration is invalid JSON") from exc


def _object(value: object, keys: set[str], name: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != keys:
        raise IdentityDecisionIntegrityError(f"{name} schema mismatch")
    return value


def _datetime(value: object) -> datetime:
    if type(value) is not str:
        raise IdentityDecisionIntegrityError("reviewed_at must be ISO-8601 text")
    try:
        result = datetime.fromisoformat(value)
    except ValueError as exc:
        raise IdentityDecisionIntegrityError("reviewed_at must be ISO-8601 text") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise IdentityDecisionIntegrityError("reviewed_at must include a timezone offset")
    return result


class IdentityReviewDeclarationParser:
    maximum_declaration_bytes = MAXIMUM_IDENTITY_REVIEW_DECLARATION_BYTES

    def parse_bytes(self, payload: bytes, *, declaration_filename: str) -> ParsedIdentityReviewBundle:
        if type(payload) is not bytes or not payload:
            raise IdentityDecisionIntegrityError("identity review declaration must be non-empty bytes")
        if len(payload) > self.maximum_declaration_bytes:
            raise IdentityDecisionIntegrityError("identity review declaration exceeds the size limit")
        if not isinstance(declaration_filename, str) or not declaration_filename.lower().endswith(".json"):
            raise IdentityDecisionIntegrityError("identity review declaration requires a .json filename")
        root = _object(decode_strict_review_json(payload), _ROOT_KEYS, "identity review declaration")
        if root["schema_version"] != IDENTITY_REVIEW_DECLARATION_SCHEMA_VERSION:
            raise IdentityDecisionIntegrityError("unsupported identity review declaration schema")
        raw_decisions = root["decisions"]
        if type(raw_decisions) is not list or not raw_decisions or len(raw_decisions) > MAXIMUM_IDENTITY_REVIEW_DECISIONS:
            raise IdentityDecisionIntegrityError("review decisions must be a bounded non-empty array")
        decisions: list[IdentityReviewDecision] = []
        for value in raw_decisions:
            raw = _object(value, _DECISION_KEYS, "identity review decision")
            try:
                decisions.append(IdentityReviewDecision(
                    queue_id=root["queue_id"],
                    source_registry_id=root["source_registry_id"],
                    candidate_id=raw["candidate_id"],
                    requirement=IdentityAdjudicationRequirement(raw["requirement"]),
                    outcome=IdentityReviewOutcome(raw["outcome"]),
                    evidence_artifact_id=raw["evidence_artifact_id"],
                    evidence_claim_id=raw["evidence_claim_id"],
                    rationale=raw["rationale"],
                ))
            except (TypeError, ValueError) as exc:
                raise IdentityDecisionIntegrityError("invalid identity review decision") from exc
        try:
            return ParsedIdentityReviewBundle(
                queue_id=root["queue_id"],
                source_registry_id=root["source_registry_id"],
                reviewer_id=root["reviewer_id"],
                reviewed_at=_datetime(root["reviewed_at"]),
                decisions=tuple(sorted(decisions, key=lambda value: value.decision_id)),
            )
        except (TypeError, ValueError) as exc:
            raise IdentityDecisionIntegrityError("identity review declaration violates the pinned contract") from exc
