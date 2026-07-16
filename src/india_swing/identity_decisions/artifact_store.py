from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from dataclasses import fields, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from india_swing._filesystem import FileSafetyError, advisory_file_lock, read_stable_regular_file
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.models import AcquisitionMode

from .codec import encode_identity_review_bundle
from .models import (
    IDENTITY_REVIEW_BUNDLE_SCHEMA_VERSION,
    IDENTITY_REVIEW_CODEC_VERSION,
    IDENTITY_REVIEW_DATASET,
    IDENTITY_REVIEW_PARSER_VERSION,
    IDENTITY_REVIEW_POLICY_VERSION,
    IdentityDecisionConflict,
    IdentityDecisionIntegrityError,
    IdentityDecisionNotFound,
    IdentityReviewBundleManifest,
    StoredIdentityReviewBundle,
)
from .parser import IdentityReviewDeclarationParser, decode_strict_review_json


MANIFEST_FILENAME = "manifest.json"
DECLARATION_FILENAME = "declaration.json"
NORMALIZED_FILENAME = "normalized.json"
MAXIMUM_REVIEW_MANIFEST_BYTES = 512 * 1024
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _is_link_like(path: Path) -> bool:
    junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(junction and junction())


def _manifest_value(value: IdentityReviewBundleManifest, *, include_id: bool) -> dict[str, object]:
    result: dict[str, object] = {
        "bundle_id": value.bundle_id,
        "first_seen_at": value.first_seen_at.isoformat(),
        "validated_at": value.validated_at.isoformat(),
        "original_declaration_filename": value.original_declaration_filename,
        "declaration_byte_count": value.declaration_byte_count,
        "declaration_sha256": value.declaration_sha256,
        "normalized_byte_count": value.normalized_byte_count,
        "normalized_sha256": value.normalized_sha256,
        "queue_id": value.queue_id,
        "source_registry_id": value.source_registry_id,
        "reviewer_id": value.reviewer_id,
        "reviewed_at": value.reviewed_at.isoformat(),
        "decision_ids": list(value.decision_ids),
        "acquisition_mode": value.acquisition_mode.value,
        "readiness": value.readiness.value,
        "actionable": value.actionable,
        "parser_version": value.parser_version,
        "codec_version": value.codec_version,
        "policy_version": value.policy_version,
        "schema_version": value.schema_version,
    }
    if include_id:
        result["manifest_id"] = value.manifest_id
    return result


def _manifest_bytes(value: IdentityReviewBundleManifest) -> bytes:
    return (json.dumps(_manifest_value(value, include_id=True), indent=2, sort_keys=True) + "\n").encode("utf-8")


def _parse_manifest(payload: bytes) -> IdentityReviewBundleManifest:
    raw = decode_strict_review_json(payload)
    expected = {item.name for item in fields(IdentityReviewBundleManifest)}
    if type(raw) is not dict or set(raw) != expected:
        raise IdentityDecisionIntegrityError("identity review manifest schema mismatch")
    if type(raw["decision_ids"]) is not list:
        raise IdentityDecisionIntegrityError("identity review manifest decision_ids must be an array")
    try:
        return IdentityReviewBundleManifest(
            bundle_id=raw["bundle_id"], manifest_id=raw["manifest_id"],
            first_seen_at=datetime.fromisoformat(raw["first_seen_at"]),
            validated_at=datetime.fromisoformat(raw["validated_at"]),
            original_declaration_filename=raw["original_declaration_filename"],
            declaration_byte_count=raw["declaration_byte_count"],
            declaration_sha256=raw["declaration_sha256"],
            normalized_byte_count=raw["normalized_byte_count"],
            normalized_sha256=raw["normalized_sha256"],
            queue_id=raw["queue_id"], source_registry_id=raw["source_registry_id"],
            reviewer_id=raw["reviewer_id"], reviewed_at=datetime.fromisoformat(raw["reviewed_at"]),
            decision_ids=tuple(raw["decision_ids"]),
            acquisition_mode=AcquisitionMode(raw["acquisition_mode"]),
            readiness=ReferenceReadiness(raw["readiness"]), actionable=raw["actionable"],
            parser_version=raw["parser_version"], codec_version=raw["codec_version"],
            policy_version=raw["policy_version"], schema_version=raw["schema_version"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise IdentityDecisionIntegrityError("identity review manifest is invalid") from exc


class LocalIdentityReviewBundleStore:
    """Create-once archive for explicit manual review declarations."""

    def __init__(
        self,
        root: Path,
        *,
        parser: IdentityReviewDeclarationParser | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.root = Path(root)
        self.parser = parser or IdentityReviewDeclarationParser()
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    @property
    def dataset_root(self) -> Path:
        return self.root / IDENTITY_REVIEW_DATASET

    def path_for(self, bundle_id: str) -> Path:
        if not isinstance(bundle_id, str) or _SHA256.fullmatch(bundle_id) is None:
            raise IdentityDecisionNotFound("invalid identity review bundle ID")
        return self.dataset_root / bundle_id

    def import_declaration(self, declaration_file: Path) -> StoredIdentityReviewBundle:
        path = Path(declaration_file)
        try:
            declaration = read_stable_regular_file(path, maximum_bytes=self.parser.maximum_declaration_bytes)
        except FileSafetyError as exc:
            raise IdentityDecisionIntegrityError("identity review declaration is unavailable or unsafe") from exc
        first_seen = _utc(self.clock(), "first_seen_at")
        parsed = self.parser.parse_bytes(declaration, declaration_filename=path.name)
        if parsed.reviewed_at > first_seen:
            raise IdentityDecisionIntegrityError("reviewed_at cannot follow local observation")
        normalized = encode_identity_review_bundle(parsed)
        validated = _utc(self.clock(), "validated_at")
        if validated < first_seen:
            raise IdentityDecisionIntegrityError("identity review validation clock moved backwards")
        provisional = IdentityReviewBundleManifest(
            bundle_id=parsed.bundle_id, manifest_id="0" * 64,
            first_seen_at=first_seen, validated_at=validated,
            original_declaration_filename=path.name,
            declaration_byte_count=len(declaration), declaration_sha256=_sha(declaration),
            normalized_byte_count=len(normalized), normalized_sha256=_sha(normalized),
            queue_id=parsed.queue_id, source_registry_id=parsed.source_registry_id,
            reviewer_id=parsed.reviewer_id, reviewed_at=parsed.reviewed_at,
            decision_ids=tuple(value.decision_id for value in parsed.decisions),
        )
        manifest = replace(
            provisional,
            manifest_id=content_id(_manifest_value(provisional, include_id=False), length=64),
        )
        existing = self._existing(parsed.bundle_id)
        if existing is not None:
            return existing
        self.dataset_root.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.dataset_root) or not self.dataset_root.is_dir():
            raise IdentityDecisionIntegrityError("identity review root is unsafe")
        try:
            with advisory_file_lock(self.dataset_root / ".identity-review.lock"):
                existing = self._existing(parsed.bundle_id)
                if existing is not None:
                    return existing
                temporary = Path(tempfile.mkdtemp(prefix=".identity-review-", dir=self.dataset_root))
                try:
                    for name, payload in (
                        (DECLARATION_FILENAME, declaration),
                        (NORMALIZED_FILENAME, normalized),
                        (MANIFEST_FILENAME, _manifest_bytes(manifest)),
                    ):
                        with (temporary / name).open("xb") as handle:
                            handle.write(payload)
                            handle.flush()
                            os.fsync(handle.fileno())
                    os.replace(temporary, self.path_for(parsed.bundle_id))
                finally:
                    if temporary.exists():
                        shutil.rmtree(temporary)
        except FileSafetyError as exc:
            raise IdentityDecisionConflict("identity review archive is unavailable") from exc
        return self.get(parsed.bundle_id)

    def _existing(self, bundle_id: str) -> StoredIdentityReviewBundle | None:
        try:
            return self.get(bundle_id)
        except IdentityDecisionNotFound:
            return None

    def get(self, bundle_id: str) -> StoredIdentityReviewBundle:
        path = self.path_for(bundle_id)
        if not path.exists():
            raise IdentityDecisionNotFound("identity review bundle was not found")
        if not path.is_dir() or _is_link_like(path):
            raise IdentityDecisionIntegrityError("identity review bundle path is unsafe")
        if {value.name for value in path.iterdir()} != {MANIFEST_FILENAME, DECLARATION_FILENAME, NORMALIZED_FILENAME}:
            raise IdentityDecisionIntegrityError("identity review bundle file set is invalid")
        try:
            manifest_bytes = read_stable_regular_file(path / MANIFEST_FILENAME, maximum_bytes=MAXIMUM_REVIEW_MANIFEST_BYTES)
            declaration = read_stable_regular_file(path / DECLARATION_FILENAME, maximum_bytes=self.parser.maximum_declaration_bytes)
            normalized = read_stable_regular_file(path / NORMALIZED_FILENAME, maximum_bytes=self.parser.maximum_declaration_bytes * 2)
        except FileSafetyError as exc:
            raise IdentityDecisionIntegrityError("stored identity review could not be read safely") from exc
        manifest = _parse_manifest(manifest_bytes)
        if manifest.bundle_id != bundle_id or content_id(_manifest_value(manifest, include_id=False), length=64) != manifest.manifest_id:
            raise IdentityDecisionIntegrityError("identity review manifest identity failed")
        if (
            (len(declaration), _sha(declaration)) != (manifest.declaration_byte_count, manifest.declaration_sha256)
            or (len(normalized), _sha(normalized)) != (manifest.normalized_byte_count, manifest.normalized_sha256)
        ):
            raise IdentityDecisionIntegrityError("identity review payload digest failed")
        parsed = self.parser.parse_bytes(declaration, declaration_filename=manifest.original_declaration_filename)
        if (
            parsed.bundle_id != manifest.bundle_id
            or parsed.queue_id != manifest.queue_id
            or parsed.source_registry_id != manifest.source_registry_id
            or parsed.reviewer_id != manifest.reviewer_id
            or parsed.reviewed_at != manifest.reviewed_at
            or tuple(value.decision_id for value in parsed.decisions) != manifest.decision_ids
            or encode_identity_review_bundle(parsed) != normalized
        ):
            raise IdentityDecisionIntegrityError("identity review normalized replay failed")
        return StoredIdentityReviewBundle(path, manifest, parsed, declaration, normalized)

    def list_bundles(self) -> tuple[StoredIdentityReviewBundle, ...]:
        if not self.dataset_root.exists():
            return ()
        if not self.dataset_root.is_dir() or _is_link_like(self.dataset_root):
            raise IdentityDecisionIntegrityError("identity review root is unsafe")
        result = []
        for path in sorted(self.dataset_root.iterdir(), key=lambda value: value.name):
            if path.name == ".identity-review.lock":
                continue
            if _SHA256.fullmatch(path.name) is None:
                raise IdentityDecisionIntegrityError("identity review archive contains an unexpected entry")
            result.append(self.get(path.name))
        return tuple(result)


def verify_stored_identity_review_provenance(
    bundle: StoredIdentityReviewBundle,
) -> None:
    """Re-open a sealed review bundle and reject memory-only substitution."""

    if type(bundle) is not StoredIdentityReviewBundle:
        raise TypeError("identity review bundle must be exact")
    try:
        reloaded = LocalIdentityReviewBundleStore(bundle.path.parent.parent).get(
            bundle.manifest.bundle_id
        )
    except (IdentityDecisionNotFound, IdentityDecisionConflict) as exc:
        raise IdentityDecisionIntegrityError(
            "identity review sealed provenance is ambiguous or missing"
        ) from exc
    if (
        reloaded.path.resolve() != bundle.path.resolve()
        or reloaded.manifest != bundle.manifest
        or reloaded.parsed != bundle.parsed
        or reloaded.declaration_bytes != bundle.declaration_bytes
        or reloaded.normalized_bytes != bundle.normalized_bytes
    ):
        raise IdentityDecisionIntegrityError(
            "identity review memory graph disagrees with sealed provenance"
        )
