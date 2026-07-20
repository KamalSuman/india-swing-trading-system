from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import fields, replace
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Iterator

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.domain.models import INDIA_STANDARD_TIME
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.models import AcquisitionMode

from .codec import encode_calendar_declaration
from .models import (
    CALENDAR_DECLARATION_PARSER_VERSION,
    CALENDAR_DECLARATION_SCHEMA_VERSION,
    CALENDAR_EVENT_POLICY_VERSION,
    CALENDAR_EVENT_SCHEMA_VERSION,
    CALENDAR_NORMALIZED_CODEC_VERSION,
    CALENDAR_PUBLICATION_TIME_STATUS,
    CALENDAR_SOURCE_ARTIFACT_SCHEMA_VERSION,
    CALENDAR_SOURCE_DATASET,
    CalendarSourceArtifactConflict,
    CalendarSourceArtifactIntegrityError,
    CalendarSourceArtifactManifest,
    CalendarSourceArtifactNotFound,
    ParsedCalendarDeclaration,
    StoredCalendarSourceArtifact,
)
from .parser import (
    MAXIMUM_CALENDAR_DECLARATION_BYTES,
    MAXIMUM_CALENDAR_SOURCE_BYTES,
    CalendarDeclarationParser,
    decode_strict_json,
)


MANIFEST_FILENAME = "manifest.json"
RAW_FILENAME = "source.bin"
DECLARATION_FILENAME = "declaration.json"
NORMALIZED_FILENAME = "normalized.json"
MAXIMUM_MANIFEST_BYTES = 512 * 1024

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _is_link_like(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def _manifest_json(manifest: CalendarSourceArtifactManifest) -> bytes:
    value = {
        "schema_version": manifest.schema_version,
        "manifest_id": manifest.manifest_id,
        "artifact_id": manifest.artifact_id,
        "dataset": manifest.dataset,
        "exchange": manifest.exchange,
        "segment": manifest.segment,
        "claimed_authority": manifest.claimed_authority,
        "acquisition_mode": manifest.acquisition_mode.value,
        "readiness": manifest.readiness.value,
        "actionable": manifest.actionable,
        "publication_time_status": manifest.publication_time_status,
        "first_seen_at": manifest.first_seen_at.isoformat(),
        "validated_at": manifest.validated_at.isoformat(),
        "original_source_filename": manifest.original_source_filename,
        "original_declaration_filename": manifest.original_declaration_filename,
        "claimed_document_id": manifest.claimed_document_id,
        "claimed_issue_date": manifest.claimed_issue_date.isoformat(),
        "claimed_source_url": manifest.claimed_source_url,
        "source_media_type": manifest.source_media_type,
        "source_byte_count": manifest.source_byte_count,
        "source_sha256": manifest.source_sha256,
        "declaration_byte_count": manifest.declaration_byte_count,
        "declaration_sha256": manifest.declaration_sha256,
        "normalized_byte_count": manifest.normalized_byte_count,
        "normalized_sha256": manifest.normalized_sha256,
        "event_count": manifest.event_count,
        "event_ids": list(manifest.event_ids),
        "parser_version": manifest.parser_version,
        "declaration_schema_version": manifest.declaration_schema_version,
        "event_schema_version": manifest.event_schema_version,
        "event_policy_version": manifest.event_policy_version,
        "normalized_codec_version": manifest.normalized_codec_version,
        "raw_filename": manifest.raw_filename,
        "declaration_filename": manifest.declaration_filename,
        "normalized_filename": manifest.normalized_filename,
    }
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _write_new_file(path: Path, payload: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _manifest_from_value(value: object) -> CalendarSourceArtifactManifest:
    expected_keys = {item.name for item in fields(CalendarSourceArtifactManifest)}
    if type(value) is not dict or set(value) != expected_keys:
        raise CalendarSourceArtifactIntegrityError(
            "calendar source manifest schema mismatch"
        )
    event_ids = value["event_ids"]
    if type(event_ids) is not list:
        raise CalendarSourceArtifactIntegrityError(
            "calendar source manifest event_ids must be an array"
        )
    claimed_source_url = value["claimed_source_url"]
    if claimed_source_url is not None and type(claimed_source_url) is not str:
        raise CalendarSourceArtifactIntegrityError(
            "calendar source manifest URL must be text or null"
        )
    try:
        return CalendarSourceArtifactManifest(
            schema_version=value["schema_version"],
            manifest_id=value["manifest_id"],
            artifact_id=value["artifact_id"],
            dataset=value["dataset"],
            exchange=value["exchange"],
            segment=value["segment"],
            claimed_authority=value["claimed_authority"],
            acquisition_mode=AcquisitionMode(value["acquisition_mode"]),
            readiness=ReferenceReadiness(value["readiness"]),
            actionable=value["actionable"],
            publication_time_status=value["publication_time_status"],
            first_seen_at=datetime.fromisoformat(value["first_seen_at"]),
            validated_at=datetime.fromisoformat(value["validated_at"]),
            original_source_filename=value["original_source_filename"],
            original_declaration_filename=value[
                "original_declaration_filename"
            ],
            claimed_document_id=value["claimed_document_id"],
            claimed_issue_date=date.fromisoformat(value["claimed_issue_date"]),
            claimed_source_url=claimed_source_url,
            source_media_type=value["source_media_type"],
            source_byte_count=value["source_byte_count"],
            source_sha256=value["source_sha256"],
            declaration_byte_count=value["declaration_byte_count"],
            declaration_sha256=value["declaration_sha256"],
            normalized_byte_count=value["normalized_byte_count"],
            normalized_sha256=value["normalized_sha256"],
            event_count=value["event_count"],
            event_ids=tuple(event_ids),
            parser_version=value["parser_version"],
            declaration_schema_version=value["declaration_schema_version"],
            event_schema_version=value["event_schema_version"],
            event_policy_version=value["event_policy_version"],
            normalized_codec_version=value["normalized_codec_version"],
            raw_filename=value["raw_filename"],
            declaration_filename=value["declaration_filename"],
            normalized_filename=value["normalized_filename"],
        )
    except (TypeError, ValueError, KeyError) as exc:
        raise CalendarSourceArtifactIntegrityError(
            "calendar source manifest is malformed"
        ) from exc


def _validate_parsed_manifest(
    parsed: ParsedCalendarDeclaration,
    manifest: CalendarSourceArtifactManifest,
) -> None:
    values = {
        "exchange": parsed.exchange,
        "segment": parsed.segment,
        "claimed_authority": parsed.claimed_authority,
        "original_source_filename": parsed.source_filename,
        "claimed_document_id": parsed.claimed_document_id,
        "claimed_issue_date": parsed.claimed_issue_date,
        "claimed_source_url": parsed.claimed_source_url,
        "source_media_type": parsed.source_media_type,
        "source_byte_count": parsed.source_byte_count,
        "source_sha256": parsed.source_sha256,
        "event_count": len(parsed.events),
        "event_ids": parsed.event_ids,
        "declaration_schema_version": parsed.schema_version,
    }
    if any(getattr(manifest, name) != expected for name, expected in values.items()):
        raise CalendarSourceArtifactIntegrityError(
            "parsed calendar declaration and manifest disagree"
        )


def verify_stored_calendar_source_provenance(
    artifact: StoredCalendarSourceArtifact,
) -> None:
    """Re-open the sealed source and fully reparse both raw inputs."""

    if type(artifact) is not StoredCalendarSourceArtifact:
        raise TypeError("artifact must be an exact StoredCalendarSourceArtifact")
    path = artifact.path
    try:
        reloaded = LocalCalendarSourceArtifactStore(path.parent.parent.parent).get(
            artifact.manifest.artifact_id
        )
    except (CalendarSourceArtifactNotFound, CalendarSourceArtifactConflict) as exc:
        raise CalendarSourceArtifactIntegrityError(
            "calendar source sealed provenance is ambiguous or missing"
        ) from exc
    if (
        reloaded.path.resolve() != path.resolve()
        or reloaded.manifest != artifact.manifest
        or reloaded.parsed != artifact.parsed
        or reloaded.source_bytes != artifact.source_bytes
        or reloaded.declaration_bytes != artifact.declaration_bytes
        or reloaded.normalized_bytes != artifact.normalized_bytes
    ):
        raise CalendarSourceArtifactIntegrityError(
            "calendar source memory graph disagrees with sealed provenance"
        )


class LocalCalendarSourceArtifactStore:
    """Create-once archive for a manual official PDF plus strict declaration."""

    def __init__(
        self,
        root: Path,
        *,
        parser: CalendarDeclarationParser | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.root = Path(root)
        self.parser = parser or CalendarDeclarationParser()
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def import_source(
        self,
        source_file: Path,
        declaration_file: Path,
    ) -> StoredCalendarSourceArtifact:
        source_path = Path(source_file)
        declaration_path = Path(declaration_file)
        try:
            source_bytes = read_stable_regular_file(
                source_path,
                maximum_bytes=self.parser.maximum_source_bytes,
            )
            declaration_bytes = read_stable_regular_file(
                declaration_path,
                maximum_bytes=self.parser.maximum_declaration_bytes,
            )
        except FileSafetyError as exc:
            raise CalendarSourceArtifactIntegrityError(
                "calendar source inputs are unavailable or unsafe"
            ) from exc
        first_seen_at = _utc(self.clock(), "first_seen_at")
        parsed = self.parser.parse_bytes(
            declaration_bytes,
            source_bytes=source_bytes,
            source_filename=source_path.name,
            declaration_filename=declaration_path.name,
        )
        if parsed.claimed_issue_date > first_seen_at.astimezone(
            INDIA_STANDARD_TIME
        ).date():
            raise CalendarSourceArtifactIntegrityError(
                "calendar source claims an issue date after local observation"
            )
        normalized_bytes = encode_calendar_declaration(parsed)
        validated_at = _utc(self.clock(), "validated_at")
        if validated_at < first_seen_at:
            raise CalendarSourceArtifactIntegrityError(
                "calendar source validation clock moved backwards"
            )

        provisional = CalendarSourceArtifactManifest(
            schema_version=CALENDAR_SOURCE_ARTIFACT_SCHEMA_VERSION,
            manifest_id="0" * 64,
            artifact_id="0" * 64,
            dataset=CALENDAR_SOURCE_DATASET,
            exchange="NSE",
            segment="CM",
            claimed_authority="NSE",
            acquisition_mode=AcquisitionMode.UNVERIFIED_MANUAL_FILE,
            readiness=ReferenceReadiness.COLLECTION_ONLY,
            actionable=False,
            publication_time_status=CALENDAR_PUBLICATION_TIME_STATUS,
            first_seen_at=first_seen_at,
            validated_at=validated_at,
            original_source_filename=source_path.name,
            original_declaration_filename=declaration_path.name,
            claimed_document_id=parsed.claimed_document_id,
            claimed_issue_date=parsed.claimed_issue_date,
            claimed_source_url=parsed.claimed_source_url,
            source_media_type=parsed.source_media_type,
            source_byte_count=len(source_bytes),
            source_sha256=_sha256(source_bytes),
            declaration_byte_count=len(declaration_bytes),
            declaration_sha256=_sha256(declaration_bytes),
            normalized_byte_count=len(normalized_bytes),
            normalized_sha256=_sha256(normalized_bytes),
            event_count=len(parsed.events),
            event_ids=parsed.event_ids,
            parser_version=CALENDAR_DECLARATION_PARSER_VERSION,
            declaration_schema_version=CALENDAR_DECLARATION_SCHEMA_VERSION,
            event_schema_version=CALENDAR_EVENT_SCHEMA_VERSION,
            event_policy_version=CALENDAR_EVENT_POLICY_VERSION,
            normalized_codec_version=CALENDAR_NORMALIZED_CODEC_VERSION,
            raw_filename=RAW_FILENAME,
            declaration_filename=DECLARATION_FILENAME,
            normalized_filename=NORMALIZED_FILENAME,
        )
        artifact_id = provisional._calculated_artifact_id()
        with_artifact_id = replace(provisional, artifact_id=artifact_id)
        manifest_id = with_artifact_id._calculated_manifest_id()
        manifest = replace(with_artifact_id, manifest_id=manifest_id)

        existing = self._existing(artifact_id)
        if existing is not None:
            return existing
        with self._artifact_lock(artifact_id):
            existing = self._existing(artifact_id)
            if existing is not None:
                return existing
            return self._publish(
                manifest,
                source_bytes,
                declaration_bytes,
                normalized_bytes,
            )

    def get(self, artifact_id: str) -> StoredCalendarSourceArtifact:
        if not isinstance(artifact_id, str) or _SHA256.fullmatch(artifact_id) is None:
            raise CalendarSourceArtifactNotFound("invalid calendar source artifact ID")
        dataset_root = self.root / CALENDAR_SOURCE_DATASET
        if not dataset_root.exists():
            raise CalendarSourceArtifactNotFound("calendar source artifact was not found")
        safe_root = self._assert_safe_dataset_root()
        matches = [
            path
            for path in safe_root.glob(f"*/{artifact_id}")
            if path.parent.name != ".locks"
        ]
        if not matches:
            raise CalendarSourceArtifactNotFound("calendar source artifact was not found")
        if len(matches) != 1:
            raise CalendarSourceArtifactConflict(
                "calendar source artifact exists in multiple availability partitions"
            )
        return self._read_path(matches[0])

    def _existing(self, artifact_id: str) -> StoredCalendarSourceArtifact | None:
        try:
            return self.get(artifact_id)
        except CalendarSourceArtifactNotFound:
            return None

    @contextmanager
    def _artifact_lock(self, artifact_id: str) -> Iterator[None]:
        dataset_root = self._ensure_safe_dataset_root()
        lock_root = dataset_root / ".locks"
        if not lock_root.exists():
            try:
                lock_root.mkdir()
            except FileExistsError:
                pass
        if _is_link_like(lock_root) or not lock_root.is_dir():
            raise CalendarSourceArtifactIntegrityError(
                "calendar source lock root is unsafe"
            )
        lock_path = lock_root / f"{artifact_id}.lock"
        try:
            with advisory_file_lock(lock_path):
                yield
        except (FileLockUnavailable, FileSafetyError) as exc:
            raise CalendarSourceArtifactConflict(
                "calendar source artifact import is already in progress"
            ) from exc

    def _publish(
        self,
        manifest: CalendarSourceArtifactManifest,
        source_bytes: bytes,
        declaration_bytes: bytes,
        normalized_bytes: bytes,
    ) -> StoredCalendarSourceArtifact:
        dataset_root = self._ensure_safe_dataset_root()
        partition = dataset_root / manifest.validated_at.date().isoformat()
        if not partition.exists():
            try:
                partition.mkdir()
            except FileExistsError:
                pass
        if _is_link_like(partition) or not partition.is_dir():
            raise CalendarSourceArtifactIntegrityError(
                "calendar source availability partition is unsafe"
            )
        target = partition / manifest.artifact_id
        if target.exists():
            return self._read_path(target)
        temporary = Path(
            tempfile.mkdtemp(prefix=f".{manifest.artifact_id}.", dir=partition)
        )
        try:
            _write_new_file(temporary / MANIFEST_FILENAME, _manifest_json(manifest))
            _write_new_file(temporary / RAW_FILENAME, source_bytes)
            _write_new_file(temporary / DECLARATION_FILENAME, declaration_bytes)
            _write_new_file(temporary / NORMALIZED_FILENAME, normalized_bytes)
            _fsync_directory(temporary)
            try:
                os.rename(temporary, target)
            except OSError:
                if target.exists():
                    return self._read_path(target)
                raise
            _fsync_directory(partition)
            _fsync_directory(dataset_root)
        finally:
            if temporary.exists():
                resolved_partition = partition.resolve()
                resolved_temporary = temporary.resolve()
                if not resolved_temporary.is_relative_to(resolved_partition):
                    raise CalendarSourceArtifactIntegrityError(
                        "unsafe temporary calendar-source path"
                    )
                shutil.rmtree(temporary)
        return self._read_path(target)

    def _read_path(self, path: Path) -> StoredCalendarSourceArtifact:
        try:
            self._assert_internal_path(path)
            if _is_link_like(path) or not path.is_dir():
                raise CalendarSourceArtifactIntegrityError(
                    "calendar source artifact path is not a real directory"
                )
            entries = tuple(path.iterdir())
            expected_names = {
                MANIFEST_FILENAME,
                RAW_FILENAME,
                DECLARATION_FILENAME,
                NORMALIZED_FILENAME,
            }
            if {entry.name for entry in entries} != expected_names:
                raise CalendarSourceArtifactIntegrityError(
                    "calendar source artifact has unexpected entries"
                )
            if any(_is_link_like(entry) or not entry.is_file() for entry in entries):
                raise CalendarSourceArtifactIntegrityError(
                    "calendar source artifact entries must be regular files"
                )

            manifest_bytes = read_stable_regular_file(
                path / MANIFEST_FILENAME,
                maximum_bytes=MAXIMUM_MANIFEST_BYTES,
            )
            manifest = _manifest_from_value(
                decode_strict_json(manifest_bytes, label="calendar source manifest")
            )
            if manifest_bytes != _manifest_json(manifest):
                raise CalendarSourceArtifactIntegrityError(
                    "calendar source manifest encoding is not canonical"
                )
            self._validate_manifest_constants(manifest)
            if (
                path.parent.parent.name != manifest.dataset
                or path.parent.name != manifest.validated_at.date().isoformat()
                or path.name != manifest.artifact_id
            ):
                raise CalendarSourceArtifactIntegrityError(
                    "calendar source artifact path and manifest disagree"
                )
            if (
                manifest.source_byte_count > MAXIMUM_CALENDAR_SOURCE_BYTES
                or manifest.declaration_byte_count
                > MAXIMUM_CALENDAR_DECLARATION_BYTES
                or manifest.normalized_byte_count
                > MAXIMUM_CALENDAR_DECLARATION_BYTES * 4
            ):
                raise CalendarSourceArtifactIntegrityError(
                    "calendar source artifact exceeds a pinned size limit"
                )
            source_bytes = read_stable_regular_file(
                path / RAW_FILENAME,
                maximum_bytes=manifest.source_byte_count,
            )
            declaration_bytes = read_stable_regular_file(
                path / DECLARATION_FILENAME,
                maximum_bytes=manifest.declaration_byte_count,
            )
            normalized_bytes = read_stable_regular_file(
                path / NORMALIZED_FILENAME,
                maximum_bytes=manifest.normalized_byte_count,
            )
            if (
                len(source_bytes) != manifest.source_byte_count
                or _sha256(source_bytes) != manifest.source_sha256
                or len(declaration_bytes) != manifest.declaration_byte_count
                or _sha256(declaration_bytes) != manifest.declaration_sha256
                or len(normalized_bytes) != manifest.normalized_byte_count
                or _sha256(normalized_bytes) != manifest.normalized_sha256
            ):
                raise CalendarSourceArtifactIntegrityError(
                    "calendar source artifact payload hash or size mismatch"
                )
            parsed = self.parser.parse_bytes(
                declaration_bytes,
                source_bytes=source_bytes,
                source_filename=manifest.original_source_filename,
                declaration_filename=manifest.original_declaration_filename,
            )
            expected_normalized = encode_calendar_declaration(parsed)
            if normalized_bytes != expected_normalized:
                raise CalendarSourceArtifactIntegrityError(
                    "normalized calendar declaration is not deterministic"
                )
            _validate_parsed_manifest(parsed, manifest)
            manifest.verify_content_identity()
        except CalendarSourceArtifactIntegrityError:
            raise
        except (FileSafetyError, OSError, TypeError, ValueError, KeyError) as exc:
            raise CalendarSourceArtifactIntegrityError(
                "calendar source artifact is incomplete or malformed"
            ) from exc
        return StoredCalendarSourceArtifact(
            path=path,
            manifest=manifest,
            parsed=parsed,
            source_bytes=source_bytes,
            declaration_bytes=declaration_bytes,
            normalized_bytes=normalized_bytes,
        )

    def _assert_safe_dataset_root(self) -> Path:
        configured_root = self.root.resolve()
        dataset_root = self.root / CALENDAR_SOURCE_DATASET
        if not dataset_root.exists():
            raise CalendarSourceArtifactIntegrityError(
                "calendar source dataset root does not exist"
            )
        if _is_link_like(dataset_root) or not dataset_root.is_dir():
            raise CalendarSourceArtifactIntegrityError(
                "calendar source dataset root cannot be a link"
            )
        expected = configured_root / CALENDAR_SOURCE_DATASET
        if dataset_root.resolve() != expected:
            raise CalendarSourceArtifactIntegrityError(
                "calendar source dataset root escapes the configured root"
            )
        return expected

    def _ensure_safe_dataset_root(self) -> Path:
        self.root.mkdir(parents=True, exist_ok=True)
        dataset_root = self.root / CALENDAR_SOURCE_DATASET
        if not dataset_root.exists():
            try:
                dataset_root.mkdir()
            except FileExistsError:
                pass
        return self._assert_safe_dataset_root()

    def _assert_internal_path(self, path: Path) -> None:
        dataset_root = self._assert_safe_dataset_root()
        resolved = path.resolve()
        if not resolved.is_relative_to(dataset_root):
            raise CalendarSourceArtifactIntegrityError(
                "calendar source path escapes the configured dataset root"
            )
        current = path
        while current != self.root and current != current.parent:
            if current.exists() and _is_link_like(current):
                raise CalendarSourceArtifactIntegrityError(
                    "calendar source path contains a link or junction"
                )
            if current == self.root / CALENDAR_SOURCE_DATASET:
                break
            current = current.parent

    @staticmethod
    def _validate_manifest_constants(
        manifest: CalendarSourceArtifactManifest,
    ) -> None:
        if (
            manifest.raw_filename != RAW_FILENAME
            or manifest.declaration_filename != DECLARATION_FILENAME
            or manifest.normalized_filename != NORMALIZED_FILENAME
        ):
            raise CalendarSourceArtifactIntegrityError(
                "calendar source archive filenames are not canonical"
            )
        if (
            manifest.first_seen_at.utcoffset() != timezone.utc.utcoffset(None)
            or manifest.validated_at.utcoffset() != timezone.utc.utcoffset(None)
        ):
            raise CalendarSourceArtifactIntegrityError(
                "calendar source availability timestamps must be UTC"
            )
