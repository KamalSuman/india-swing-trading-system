from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from dataclasses import fields, replace
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable

from india_swing._filesystem import FileSafetyError, advisory_file_lock, read_stable_regular_file
from india_swing.domain.models import INDIA_STANDARD_TIME
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.models import AcquisitionMode

from .codec import encode_identity_evidence_declaration
from .models import (
    IDENTITY_EVIDENCE_ARTIFACT_SCHEMA_VERSION,
    IDENTITY_EVIDENCE_CODEC_VERSION,
    IDENTITY_EVIDENCE_DATASET,
    IDENTITY_EVIDENCE_PARSER_VERSION,
    IDENTITY_EVIDENCE_POLICY_VERSION,
    IDENTITY_EVIDENCE_PUBLICATION_TIME_STATUS,
    IdentityEvidenceArtifactManifest,
    IdentityEvidenceConflict,
    IdentityEvidenceIntegrityError,
    IdentityEvidenceNotFound,
    IdentityEvidenceSourceKind,
    StoredIdentityEvidenceArtifact,
)
from .parser import IdentityEvidenceDeclarationParser, decode_strict_json


MANIFEST_FILENAME = "manifest.json"
SOURCE_FILENAME = "source.bin"
DECLARATION_FILENAME = "declaration.json"
NORMALIZED_FILENAME = "normalized.json"
MAXIMUM_MANIFEST_BYTES = 512 * 1024
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


def _manifest_value(value: IdentityEvidenceArtifactManifest, *, include_id: bool) -> dict[str, object]:
    result: dict[str, object] = {
        "schema_version": value.schema_version,
        "artifact_id": value.artifact_id,
        "first_seen_at": value.first_seen_at.isoformat(),
        "validated_at": value.validated_at.isoformat(),
        "original_source_filename": value.original_source_filename,
        "original_declaration_filename": value.original_declaration_filename,
        "source_sha256": value.source_sha256,
        "declaration_sha256": value.declaration_sha256,
        "normalized_sha256": value.normalized_sha256,
        "source_byte_count": value.source_byte_count,
        "declaration_byte_count": value.declaration_byte_count,
        "normalized_byte_count": value.normalized_byte_count,
        "claimed_document_id": value.claimed_document_id,
        "claimed_issue_date": value.claimed_issue_date.isoformat(),
        "claimed_source_url": value.claimed_source_url,
        "source_kind": value.source_kind.value,
        "claim_ids": list(value.claim_ids),
        "acquisition_mode": value.acquisition_mode.value,
        "readiness": value.readiness.value,
        "actionable": value.actionable,
        "stable_identity_assigned": value.stable_identity_assigned,
        "publication_time_status": value.publication_time_status,
        "parser_version": value.parser_version,
        "codec_version": value.codec_version,
        "policy_version": value.policy_version,
    }
    if include_id:
        result["manifest_id"] = value.manifest_id
    return result


def _manifest_bytes(value: IdentityEvidenceArtifactManifest) -> bytes:
    return (json.dumps(_manifest_value(value, include_id=True), indent=2, sort_keys=True) + "\n").encode("utf-8")


def _parse_manifest(payload: bytes) -> IdentityEvidenceArtifactManifest:
    raw = decode_strict_json(payload)
    expected = {item.name for item in fields(IdentityEvidenceArtifactManifest)}
    if type(raw) is not dict or set(raw) != expected:
        raise IdentityEvidenceIntegrityError("identity evidence manifest schema mismatch")
    if type(raw["claim_ids"]) is not list:
        raise IdentityEvidenceIntegrityError("identity evidence manifest claim_ids must be an array")
    try:
        return IdentityEvidenceArtifactManifest(
            artifact_id=raw["artifact_id"], manifest_id=raw["manifest_id"],
            first_seen_at=datetime.fromisoformat(raw["first_seen_at"]),
            validated_at=datetime.fromisoformat(raw["validated_at"]),
            original_source_filename=raw["original_source_filename"],
            original_declaration_filename=raw["original_declaration_filename"],
            source_sha256=raw["source_sha256"], declaration_sha256=raw["declaration_sha256"],
            normalized_sha256=raw["normalized_sha256"],
            source_byte_count=raw["source_byte_count"],
            declaration_byte_count=raw["declaration_byte_count"],
            normalized_byte_count=raw["normalized_byte_count"],
            claimed_document_id=raw["claimed_document_id"],
            claimed_issue_date=date.fromisoformat(raw["claimed_issue_date"]),
            claimed_source_url=raw["claimed_source_url"],
            source_kind=IdentityEvidenceSourceKind(raw["source_kind"]),
            claim_ids=tuple(raw["claim_ids"]),
            acquisition_mode=AcquisitionMode(raw["acquisition_mode"]),
            readiness=ReferenceReadiness(raw["readiness"]),
            actionable=raw["actionable"], stable_identity_assigned=raw["stable_identity_assigned"],
            publication_time_status=raw["publication_time_status"],
            parser_version=raw["parser_version"], codec_version=raw["codec_version"],
            policy_version=raw["policy_version"], schema_version=raw["schema_version"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise IdentityEvidenceIntegrityError("identity evidence manifest is invalid") from exc


def _artifact_identity(parsed: object, declaration_sha256: str, normalized_sha256: str) -> str:
    return content_id(
        {
            "schema_version": IDENTITY_EVIDENCE_ARTIFACT_SCHEMA_VERSION,
            "dataset": IDENTITY_EVIDENCE_DATASET,
            "parsed": parsed,
            "declaration_sha256": declaration_sha256,
            "normalized_sha256": normalized_sha256,
            "acquisition_mode": AcquisitionMode.UNVERIFIED_MANUAL_FILE,
            "readiness": ReferenceReadiness.COLLECTION_ONLY,
            "actionable": False,
            "stable_identity_assigned": False,
            "publication_time_status": IDENTITY_EVIDENCE_PUBLICATION_TIME_STATUS,
            "parser_version": IDENTITY_EVIDENCE_PARSER_VERSION,
            "codec_version": IDENTITY_EVIDENCE_CODEC_VERSION,
            "policy_version": IDENTITY_EVIDENCE_POLICY_VERSION,
        },
        length=64,
    )


class LocalIdentityEvidenceArtifactStore:
    """Create-once archive for exact NSE evidence bytes and manual declarations."""

    def __init__(self, root: Path, *, parser: IdentityEvidenceDeclarationParser | None = None, clock: Callable[[], datetime] | None = None) -> None:
        self.root = Path(root)
        self.parser = parser or IdentityEvidenceDeclarationParser()
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    @property
    def dataset_root(self) -> Path:
        return self.root / IDENTITY_EVIDENCE_DATASET

    def path_for(self, artifact_id: str) -> Path:
        if not isinstance(artifact_id, str) or _SHA256.fullmatch(artifact_id) is None:
            raise IdentityEvidenceNotFound("invalid identity evidence artifact ID")
        return self.dataset_root / artifact_id

    def import_source(self, source_file: Path, declaration_file: Path) -> StoredIdentityEvidenceArtifact:
        source_path, declaration_path = Path(source_file), Path(declaration_file)
        try:
            source_bytes = read_stable_regular_file(source_path, maximum_bytes=self.parser.maximum_source_bytes)
            declaration_bytes = read_stable_regular_file(declaration_path, maximum_bytes=self.parser.maximum_declaration_bytes)
        except FileSafetyError as exc:
            raise IdentityEvidenceIntegrityError("identity evidence inputs are unavailable or unsafe") from exc
        first_seen = _utc(self.clock(), "first_seen_at")
        parsed = self.parser.parse_bytes(
            declaration_bytes, source_bytes=source_bytes, source_filename=source_path.name,
            declaration_filename=declaration_path.name,
        )
        if parsed.claimed_issue_date > first_seen.astimezone(INDIA_STANDARD_TIME).date():
            raise IdentityEvidenceIntegrityError("identity evidence claims a future issue date")
        if parsed.claimed_publication_at is not None and parsed.claimed_publication_at > first_seen:
            raise IdentityEvidenceIntegrityError("identity evidence claims publication after local observation")
        normalized = encode_identity_evidence_declaration(parsed)
        validated = _utc(self.clock(), "validated_at")
        if validated < first_seen:
            raise IdentityEvidenceIntegrityError("identity evidence validation clock moved backwards")
        declaration_sha, normalized_sha = _sha(declaration_bytes), _sha(normalized)
        artifact_id = _artifact_identity(parsed, declaration_sha, normalized_sha)
        provisional = IdentityEvidenceArtifactManifest(
            artifact_id=artifact_id, manifest_id="0" * 64, first_seen_at=first_seen,
            validated_at=validated, original_source_filename=source_path.name,
            original_declaration_filename=declaration_path.name, source_sha256=_sha(source_bytes),
            declaration_sha256=declaration_sha, normalized_sha256=normalized_sha,
            source_byte_count=len(source_bytes), declaration_byte_count=len(declaration_bytes),
            normalized_byte_count=len(normalized), claimed_document_id=parsed.claimed_document_id,
            claimed_issue_date=parsed.claimed_issue_date, claimed_source_url=parsed.claimed_source_url,
            source_kind=parsed.source_kind, claim_ids=parsed.claim_ids,
        )
        manifest_id = content_id(_manifest_value(provisional, include_id=False), length=64)
        manifest = replace(provisional, manifest_id=manifest_id)
        existing = self._existing(artifact_id)
        if existing is not None:
            return existing
        self.dataset_root.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.dataset_root) or not self.dataset_root.is_dir():
            raise IdentityEvidenceIntegrityError("identity evidence root is unsafe")
        try:
            with advisory_file_lock(self.dataset_root / ".identity-evidence.lock"):
                existing = self._existing(artifact_id)
                if existing is not None:
                    return existing
                temporary = Path(tempfile.mkdtemp(prefix=".identity-evidence-", dir=self.dataset_root))
                try:
                    for name, payload in (
                        (SOURCE_FILENAME, source_bytes), (DECLARATION_FILENAME, declaration_bytes),
                        (NORMALIZED_FILENAME, normalized), (MANIFEST_FILENAME, _manifest_bytes(manifest)),
                    ):
                        with (temporary / name).open("xb") as handle:
                            handle.write(payload)
                            handle.flush()
                            os.fsync(handle.fileno())
                    os.replace(temporary, self.path_for(artifact_id))
                finally:
                    if temporary.exists():
                        shutil.rmtree(temporary)
        except FileSafetyError as exc:
            raise IdentityEvidenceConflict("identity evidence archive is unavailable") from exc
        return self.get(artifact_id)

    def _existing(self, artifact_id: str) -> StoredIdentityEvidenceArtifact | None:
        try:
            return self.get(artifact_id)
        except IdentityEvidenceNotFound:
            return None

    def get(self, artifact_id: str) -> StoredIdentityEvidenceArtifact:
        path = self.path_for(artifact_id)
        if not path.exists():
            raise IdentityEvidenceNotFound("identity evidence artifact was not found")
        if not path.is_dir() or _is_link_like(path):
            raise IdentityEvidenceIntegrityError("identity evidence artifact path is unsafe")
        if {value.name for value in path.iterdir()} != {MANIFEST_FILENAME, SOURCE_FILENAME, DECLARATION_FILENAME, NORMALIZED_FILENAME}:
            raise IdentityEvidenceIntegrityError("identity evidence artifact file set is invalid")
        try:
            manifest_bytes = read_stable_regular_file(path / MANIFEST_FILENAME, maximum_bytes=MAXIMUM_MANIFEST_BYTES)
            source_bytes = read_stable_regular_file(path / SOURCE_FILENAME, maximum_bytes=self.parser.maximum_source_bytes)
            declaration_bytes = read_stable_regular_file(path / DECLARATION_FILENAME, maximum_bytes=self.parser.maximum_declaration_bytes)
            normalized_bytes = read_stable_regular_file(path / NORMALIZED_FILENAME, maximum_bytes=self.parser.maximum_declaration_bytes * 2)
        except FileSafetyError as exc:
            raise IdentityEvidenceIntegrityError("stored identity evidence could not be read safely") from exc
        manifest = _parse_manifest(manifest_bytes)
        if manifest.artifact_id != artifact_id or content_id(_manifest_value(manifest, include_id=False), length=64) != manifest.manifest_id:
            raise IdentityEvidenceIntegrityError("identity evidence manifest identity failed")
        if (
            (len(source_bytes), _sha(source_bytes)) != (manifest.source_byte_count, manifest.source_sha256)
            or (len(declaration_bytes), _sha(declaration_bytes)) != (manifest.declaration_byte_count, manifest.declaration_sha256)
            or (len(normalized_bytes), _sha(normalized_bytes)) != (manifest.normalized_byte_count, manifest.normalized_sha256)
        ):
            raise IdentityEvidenceIntegrityError("identity evidence payload digest failed")
        parsed = self.parser.parse_bytes(
            declaration_bytes, source_bytes=source_bytes,
            source_filename=manifest.original_source_filename,
            declaration_filename=manifest.original_declaration_filename,
        )
        expected_normalized = encode_identity_evidence_declaration(parsed)
        if (
            normalized_bytes != expected_normalized
            or parsed.claim_ids != manifest.claim_ids
            or parsed.claimed_document_id != manifest.claimed_document_id
            or parsed.claimed_issue_date != manifest.claimed_issue_date
            or parsed.claimed_source_url != manifest.claimed_source_url
            or parsed.source_kind is not manifest.source_kind
        ):
            raise IdentityEvidenceIntegrityError("identity evidence normalized replay failed")
        if _artifact_identity(parsed, manifest.declaration_sha256, manifest.normalized_sha256) != artifact_id:
            raise IdentityEvidenceIntegrityError("identity evidence artifact identity failed")
        return StoredIdentityEvidenceArtifact(path, manifest, parsed, source_bytes, declaration_bytes, normalized_bytes)

    def list_artifacts(self) -> tuple[StoredIdentityEvidenceArtifact, ...]:
        if not self.dataset_root.exists():
            return ()
        if not self.dataset_root.is_dir() or _is_link_like(self.dataset_root):
            raise IdentityEvidenceIntegrityError("identity evidence root is unsafe")
        result = []
        for path in sorted(self.dataset_root.iterdir(), key=lambda value: value.name):
            if path.name == ".identity-evidence.lock":
                continue
            if _SHA256.fullmatch(path.name) is None:
                raise IdentityEvidenceIntegrityError("identity evidence archive contains an unexpected entry")
            result.append(self.get(path.name))
        return tuple(result)
