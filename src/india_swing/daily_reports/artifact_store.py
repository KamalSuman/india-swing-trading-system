from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from collections import Counter
from contextlib import contextmanager
from dataclasses import fields
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.models import AcquisitionMode

from .codec import encode_daily_bundle
from .models import (
    NSE_DAILY_BUNDLE_ARTIFACT_SCHEMA_VERSION,
    NSE_DAILY_BUNDLE_CODEC_VERSION,
    NSE_DAILY_BUNDLE_DATASET,
    NSE_DAILY_BUNDLE_PARSER_VERSION,
    BundleEntryDisposition,
    DailyBundleArtifactManifest,
    DailyReportConflict,
    DailyReportIntegrityError,
    DailyReportNotFound,
    StoredDailyBundleArtifact,
)
from .parser import NseDailyBundleParser


MANIFEST_FILENAME = "manifest.json"
RAW_FILENAME = "bundle.zip"
NORMALIZED_FILENAME = "normalized.json"
NSE_DAILY_REPORTS_CLAIMED_CATALOG_URL = "https://www.nseindia.com/all-reports"
_SHA256_IDENTIFIER = re.compile(r"[0-9a-f]{64}\Z")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _is_link_like(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def _require_utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _artifact_identity(manifest: DailyBundleArtifactManifest) -> dict[str, object]:
    return {
        "schema_version": manifest.schema_version,
        "dataset": manifest.dataset,
        "claimed_authority": manifest.claimed_authority,
        "acquisition_mode": manifest.acquisition_mode,
        "readiness": manifest.readiness,
        "actionable": manifest.actionable,
        "original_filename": manifest.original_filename,
        "claimed_source_catalog_url": manifest.claimed_source_catalog_url,
        "source_media_type": manifest.source_media_type,
        "parser_version": manifest.parser_version,
        "normalized_codec_version": manifest.normalized_codec_version,
        "raw_sha256": manifest.raw_sha256,
        "normalized_sha256": manifest.normalized_sha256,
        "byte_count": manifest.byte_count,
        "outer_entry_count": manifest.outer_entry_count,
        "selected_report_count": manifest.selected_report_count,
        "quarantined_report_count": manifest.quarantined_report_count,
        "deferred_report_count": manifest.deferred_report_count,
        "ignored_entry_count": manifest.ignored_entry_count,
        "selected_row_count": manifest.selected_row_count,
        "raw_filename": manifest.raw_filename,
        "normalized_filename": manifest.normalized_filename,
    }


def _manifest_identity(manifest: DailyBundleArtifactManifest) -> dict[str, object]:
    return {
        field.name: getattr(manifest, field.name)
        for field in fields(DailyBundleArtifactManifest)
        if field.name != "manifest_id"
    }


def _manifest_json(manifest: DailyBundleArtifactManifest) -> bytes:
    value = {
        "schema_version": manifest.schema_version,
        "manifest_id": manifest.manifest_id,
        "artifact_id": manifest.artifact_id,
        "dataset": manifest.dataset,
        "claimed_authority": manifest.claimed_authority,
        "acquisition_mode": manifest.acquisition_mode.value,
        "readiness": manifest.readiness.value,
        "actionable": manifest.actionable,
        "original_filename": manifest.original_filename,
        "claimed_source_catalog_url": manifest.claimed_source_catalog_url,
        "source_media_type": manifest.source_media_type,
        "first_seen_at": manifest.first_seen_at.isoformat(),
        "validated_at": manifest.validated_at.isoformat(),
        "parser_version": manifest.parser_version,
        "normalized_codec_version": manifest.normalized_codec_version,
        "raw_sha256": manifest.raw_sha256,
        "normalized_sha256": manifest.normalized_sha256,
        "byte_count": manifest.byte_count,
        "outer_entry_count": manifest.outer_entry_count,
        "selected_report_count": manifest.selected_report_count,
        "quarantined_report_count": manifest.quarantined_report_count,
        "deferred_report_count": manifest.deferred_report_count,
        "ignored_entry_count": manifest.ignored_entry_count,
        "selected_row_count": manifest.selected_row_count,
        "raw_filename": manifest.raw_filename,
        "normalized_filename": manifest.normalized_filename,
    }
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def verify_stored_daily_bundle_provenance(
    artifact: StoredDailyBundleArtifact,
) -> None:
    """Require an in-memory artifact to match its create-once archive on disk.

    Artifact IDs intentionally identify content rather than observation time.
    Consequently, recomputing a self-consistent in-memory manifest is not proof
    of when that content was first seen.  The canonical manifest and payloads
    persisted by the local store are the sealed provenance record.
    """

    if type(artifact) is not StoredDailyBundleArtifact:
        raise TypeError("artifact must be an exact StoredDailyBundleArtifact")
    path = Path(artifact.path)
    manifest = artifact.manifest
    try:
        if (
            _is_link_like(path)
            or _is_link_like(path.parent)
            or _is_link_like(path.parent.parent)
            or not path.is_dir()
        ):
            raise DailyReportIntegrityError(
                "daily-bundle provenance path is not a sealed directory"
            )
        if (
            path.name != manifest.artifact_id
            or path.parent.name != manifest.validated_at.date().isoformat()
            or path.parent.parent.name != manifest.dataset
        ):
            raise DailyReportIntegrityError(
                "daily-bundle provenance path and manifest disagree"
            )
        entries = tuple(path.iterdir())
        expected_names = {
            MANIFEST_FILENAME,
            RAW_FILENAME,
            NORMALIZED_FILENAME,
        }
        if {entry.name for entry in entries} != expected_names:
            raise DailyReportIntegrityError(
                "daily-bundle provenance archive has unexpected entries"
            )
        if any(_is_link_like(entry) or not entry.is_file() for entry in entries):
            raise DailyReportIntegrityError(
                "daily-bundle provenance entries must be regular files"
            )

        expected_manifest = _manifest_json(manifest)
        archived_manifest = read_stable_regular_file(
            path / MANIFEST_FILENAME,
            maximum_bytes=max(len(expected_manifest), 1),
        )
        archived_raw = read_stable_regular_file(
            path / RAW_FILENAME,
            maximum_bytes=max(len(artifact.raw_bytes), 1),
        )
        archived_normalized = read_stable_regular_file(
            path / NORMALIZED_FILENAME,
            maximum_bytes=max(len(artifact.normalized_bytes), 1),
        )
    except DailyReportIntegrityError:
        raise
    except (FileSafetyError, OSError, ValueError) as exc:
        raise DailyReportIntegrityError(
            "daily-bundle provenance archive is unavailable or unsafe"
        ) from exc

    if archived_manifest != expected_manifest:
        raise DailyReportIntegrityError(
            "daily-bundle in-memory manifest disagrees with its sealed provenance"
        )
    if archived_raw != artifact.raw_bytes:
        raise DailyReportIntegrityError(
            "daily-bundle in-memory raw bytes disagree with sealed provenance"
        )
    if archived_normalized != artifact.normalized_bytes:
        raise DailyReportIntegrityError(
            "daily-bundle in-memory normalized bytes disagree with sealed provenance"
        )

    # Re-open through the same strict path used by ``get``. This re-runs the
    # pinned parser over the archived raw ZIP, validates every manifest counter
    # and constant, and proves that normalized bytes derive from those raw
    # bytes rather than from a substituted in-memory parsed tree.
    try:
        reloaded = LocalDailyBundleArtifactStore(
            path.parent.parent.parent
        ).get(artifact.manifest.artifact_id)
    except DailyReportIntegrityError:
        raise
    except (DailyReportConflict, DailyReportNotFound) as exc:
        raise DailyReportIntegrityError(
            "daily-bundle sealed provenance is ambiguous or missing"
        ) from exc
    except (OSError, TypeError, ValueError) as exc:
        raise DailyReportIntegrityError(
            "daily-bundle sealed provenance cannot be fully revalidated"
        ) from exc
    if (
        reloaded.path.resolve() != path.resolve()
        or reloaded.manifest != artifact.manifest
        or reloaded.parsed != artifact.parsed
        or reloaded.raw_bytes != artifact.raw_bytes
        or reloaded.normalized_bytes != artifact.normalized_bytes
    ):
        raise DailyReportIntegrityError(
            "daily-bundle memory graph disagrees with its fully parsed archive"
        )


def _write_fsynced(path: Path, payload: bytes) -> None:
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


class LocalDailyBundleArtifactStore:
    """Create-once local archive for manually downloaded NSE report bundles."""

    def __init__(
        self,
        root: Path,
        *,
        parser: NseDailyBundleParser | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.root = Path(root)
        self.parser = parser or NseDailyBundleParser()
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def import_bundle(self, source_file: Path) -> StoredDailyBundleArtifact:
        source_path = Path(source_file)
        try:
            raw_bytes = read_stable_regular_file(
                source_path,
                maximum_bytes=self.parser.maximum_bundle_bytes,
            )
        except FileSafetyError as exc:
            raise DailyReportIntegrityError(f"daily-bundle {exc}") from exc
        first_seen_at = _require_utc(self.clock(), "first_seen_at")

        parsed = self.parser.parse_bytes(
            raw_bytes,
            original_filename=source_path.name,
        )
        normalized_bytes = encode_daily_bundle(parsed)
        validated_at = _require_utc(self.clock(), "validated_at")
        if validated_at < first_seen_at:
            raise DailyReportIntegrityError("daily-bundle validation clock moved backwards")

        dispositions = Counter(entry.disposition for entry in parsed.entries)
        selected_reports = tuple(
            report
            for report in parsed.reports
            if report.disposition is BundleEntryDisposition.SELECTED_VALIDATED
        )
        provisional = DailyBundleArtifactManifest(
            schema_version=NSE_DAILY_BUNDLE_ARTIFACT_SCHEMA_VERSION,
            manifest_id="0" * 64,
            artifact_id="0" * 64,
            dataset=NSE_DAILY_BUNDLE_DATASET,
            claimed_authority="NSE",
            acquisition_mode=AcquisitionMode.UNVERIFIED_MANUAL_FILE,
            readiness=ReferenceReadiness.COLLECTION_ONLY,
            actionable=False,
            original_filename=parsed.original_filename,
            claimed_source_catalog_url=NSE_DAILY_REPORTS_CLAIMED_CATALOG_URL,
            source_media_type="application/zip",
            first_seen_at=first_seen_at,
            validated_at=validated_at,
            parser_version=NSE_DAILY_BUNDLE_PARSER_VERSION,
            normalized_codec_version=NSE_DAILY_BUNDLE_CODEC_VERSION,
            raw_sha256=parsed.raw_sha256,
            normalized_sha256=_sha256(normalized_bytes),
            byte_count=parsed.byte_count,
            outer_entry_count=len(parsed.entries),
            selected_report_count=dispositions[
                BundleEntryDisposition.SELECTED_VALIDATED
            ],
            quarantined_report_count=dispositions[
                BundleEntryDisposition.QUARANTINED_INTEROPERABILITY_SECURITY_MASTER
            ],
            deferred_report_count=dispositions[
                BundleEntryDisposition.DEFERRED_NSE_ONLY_SECURITY_MASTER
            ],
            ignored_entry_count=dispositions[
                BundleEntryDisposition.IGNORED_UNAPPROVED
            ],
            selected_row_count=sum(report.row_count for report in selected_reports),
            raw_filename=RAW_FILENAME,
            normalized_filename=NORMALIZED_FILENAME,
        )
        artifact_id = content_id(_artifact_identity(provisional), length=64)
        with_artifact_id = DailyBundleArtifactManifest(
            **{
                field.name: (
                    artifact_id
                    if field.name == "artifact_id"
                    else getattr(provisional, field.name)
                )
                for field in fields(DailyBundleArtifactManifest)
            }
        )
        manifest_id = content_id(_manifest_identity(with_artifact_id), length=64)
        manifest = DailyBundleArtifactManifest(
            **{
                field.name: (
                    manifest_id
                    if field.name == "manifest_id"
                    else getattr(with_artifact_id, field.name)
                )
                for field in fields(DailyBundleArtifactManifest)
            }
        )

        existing = self._existing_artifact(artifact_id)
        if existing is not None:
            return existing
        with self._artifact_lock(artifact_id):
            existing = self._existing_artifact(artifact_id)
            if existing is not None:
                return existing
            return self._publish(manifest, raw_bytes, normalized_bytes)

    def _existing_artifact(
        self,
        artifact_id: str,
    ) -> StoredDailyBundleArtifact | None:
        matches = [path for path in self._artifact_paths() if path.name == artifact_id]
        if len(matches) > 1:
            raise DailyReportIntegrityError(
                "daily-bundle artifact ID appears in multiple partitions"
            )
        return self._read_path(matches[0]) if matches else None

    def get(self, artifact_id: str) -> StoredDailyBundleArtifact:
        if (
            not isinstance(artifact_id, str)
            or _SHA256_IDENTIFIER.fullmatch(artifact_id) is None
        ):
            raise ValueError("artifact_id must be a full SHA-256 identifier")
        matches = [path for path in self._artifact_paths() if path.name == artifact_id]
        if not matches:
            raise DailyReportNotFound(f"daily-bundle artifact not found: {artifact_id}")
        if len(matches) != 1:
            raise DailyReportIntegrityError(
                "daily-bundle artifact ID appears in multiple partitions"
            )
        return self._read_path(matches[0])

    def _artifact_paths(self) -> list[Path]:
        base = self.root / NSE_DAILY_BUNDLE_DATASET
        if not base.exists():
            return []
        self._assert_safe_dataset_root()
        return sorted(
            path
            for path in base.glob("*/*")
            if path.is_dir()
            and not path.name.startswith(".")
            and not path.parent.name.startswith(".")
        )

    @contextmanager
    def _artifact_lock(self, artifact_id: str):
        self._ensure_safe_dataset_root()
        lock_root = self.root / NSE_DAILY_BUNDLE_DATASET / ".locks"
        if lock_root.exists():
            self._assert_internal_path(lock_root)
        else:
            lock_root.mkdir(exist_ok=True)
        self._assert_internal_path(lock_root)
        lock_path = lock_root / f".{artifact_id}.advisory-lock"
        if lock_path.exists():
            self._assert_internal_path(lock_path)
        try:
            with advisory_file_lock(lock_path):
                yield
        except FileLockUnavailable as exc:
            raise DailyReportConflict(
                "another import is validating this daily bundle"
            ) from exc
        except FileSafetyError as exc:
            raise DailyReportIntegrityError(
                "daily-bundle import lock is unsafe"
            ) from exc

    def _publish(
        self,
        manifest: DailyBundleArtifactManifest,
        raw_bytes: bytes,
        normalized_bytes: bytes,
    ) -> StoredDailyBundleArtifact:
        parent = (
            self.root
            / NSE_DAILY_BUNDLE_DATASET
            / manifest.validated_at.date().isoformat()
        )
        target = parent / manifest.artifact_id
        parent.mkdir(parents=True, exist_ok=True)
        self._assert_internal_path(parent)
        if target.exists():
            return self._read_path(target)
        temporary = Path(
            tempfile.mkdtemp(dir=parent, prefix=f".{manifest.artifact_id}.")
        )
        try:
            _write_fsynced(temporary / RAW_FILENAME, raw_bytes)
            _write_fsynced(temporary / NORMALIZED_FILENAME, normalized_bytes)
            _write_fsynced(temporary / MANIFEST_FILENAME, _manifest_json(manifest))
            self._read_path(temporary)
            _fsync_directory(temporary)
            try:
                os.rename(temporary, target)
            except FileExistsError:
                return self._read_path(target)
            _fsync_directory(parent)
        finally:
            if temporary.exists():
                resolved_parent = parent.resolve()
                resolved_temporary = temporary.resolve()
                if not resolved_temporary.is_relative_to(resolved_parent):
                    raise DailyReportIntegrityError(
                        "unsafe temporary daily-bundle path"
                    )
                shutil.rmtree(temporary)
        return self._read_path(target)

    def _read_path(self, path: Path) -> StoredDailyBundleArtifact:
        try:
            self._assert_internal_path(path)
            if _is_link_like(path) or not path.is_dir():
                raise DailyReportIntegrityError(
                    "daily-bundle artifact path is not a real directory"
                )
            entries = list(path.iterdir())
            if any(_is_link_like(entry) for entry in entries):
                raise DailyReportIntegrityError(
                    "daily-bundle artifact cannot contain links"
                )
            expected_entries = {
                MANIFEST_FILENAME,
                RAW_FILENAME,
                NORMALIZED_FILENAME,
            }
            if {entry.name for entry in entries} != expected_entries:
                raise DailyReportIntegrityError(
                    "daily-bundle artifact contains unexpected entries"
                )
            if not all(entry.is_file() for entry in entries):
                raise DailyReportIntegrityError(
                    "daily-bundle artifact entries must be files"
                )
            value = json.loads((path / MANIFEST_FILENAME).read_text(encoding="utf-8"))
            expected_keys = {field.name for field in fields(DailyBundleArtifactManifest)}
            if not isinstance(value, dict) or set(value) != expected_keys:
                raise DailyReportIntegrityError(
                    "daily-bundle manifest schema mismatch"
                )
            manifest = DailyBundleArtifactManifest(
                schema_version=str(value["schema_version"]),
                manifest_id=str(value["manifest_id"]),
                artifact_id=str(value["artifact_id"]),
                dataset=str(value["dataset"]),
                claimed_authority=str(value["claimed_authority"]),
                acquisition_mode=AcquisitionMode(value["acquisition_mode"]),
                readiness=ReferenceReadiness(value["readiness"]),
                actionable=value["actionable"],
                original_filename=str(value["original_filename"]),
                claimed_source_catalog_url=str(value["claimed_source_catalog_url"]),
                source_media_type=str(value["source_media_type"]),
                first_seen_at=datetime.fromisoformat(str(value["first_seen_at"])),
                validated_at=datetime.fromisoformat(str(value["validated_at"])),
                parser_version=str(value["parser_version"]),
                normalized_codec_version=str(value["normalized_codec_version"]),
                raw_sha256=str(value["raw_sha256"]),
                normalized_sha256=str(value["normalized_sha256"]),
                byte_count=value["byte_count"],
                outer_entry_count=value["outer_entry_count"],
                selected_report_count=value["selected_report_count"],
                quarantined_report_count=value["quarantined_report_count"],
                deferred_report_count=value["deferred_report_count"],
                ignored_entry_count=value["ignored_entry_count"],
                selected_row_count=value["selected_row_count"],
                raw_filename=str(value["raw_filename"]),
                normalized_filename=str(value["normalized_filename"]),
            )
            self._validate_manifest_constants(manifest)
            if path.parent.parent.name != manifest.dataset:
                raise DailyReportIntegrityError(
                    "daily-bundle dataset partition mismatch"
                )
            if path.parent.name != manifest.validated_at.date().isoformat():
                raise DailyReportIntegrityError(
                    "daily-bundle validation partition mismatch"
                )
            if path.name != manifest.artifact_id and not path.name.startswith(
                f".{manifest.artifact_id}."
            ):
                raise DailyReportIntegrityError(
                    "daily-bundle directory and manifest disagree"
                )
            raw_bytes = (path / RAW_FILENAME).read_bytes()
            normalized_bytes = (path / NORMALIZED_FILENAME).read_bytes()
            if _sha256(raw_bytes) != manifest.raw_sha256:
                raise DailyReportIntegrityError("daily-bundle raw hash mismatch")
            if _sha256(normalized_bytes) != manifest.normalized_sha256:
                raise DailyReportIntegrityError("daily-bundle normalized hash mismatch")
            parsed = self.parser.parse_bytes(
                raw_bytes,
                original_filename=manifest.original_filename,
            )
            if encode_daily_bundle(parsed) != normalized_bytes:
                raise DailyReportIntegrityError(
                    "daily-bundle normalization is not deterministic"
                )
            self._validate_parsed_manifest(parsed, manifest)
            if content_id(_artifact_identity(manifest), length=64) != manifest.artifact_id:
                raise DailyReportIntegrityError(
                    "daily-bundle artifact ID does not match its content"
                )
            if content_id(_manifest_identity(manifest), length=64) != manifest.manifest_id:
                raise DailyReportIntegrityError(
                    "daily-bundle manifest ID does not match its content"
                )
        except DailyReportIntegrityError:
            raise
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise DailyReportIntegrityError(
                "daily-bundle artifact is incomplete or malformed"
            ) from exc
        return StoredDailyBundleArtifact(
            path=path,
            manifest=manifest,
            parsed=parsed,
            raw_bytes=raw_bytes,
            normalized_bytes=normalized_bytes,
        )

    def _assert_safe_dataset_root(self) -> Path:
        configured_root = self.root.resolve()
        dataset_root = self.root / NSE_DAILY_BUNDLE_DATASET
        if not dataset_root.exists():
            raise DailyReportIntegrityError(
                "daily-bundle dataset root does not exist"
            )
        if _is_link_like(dataset_root):
            raise DailyReportIntegrityError(
                "daily-bundle dataset root cannot be a link or junction"
            )
        expected = configured_root / NSE_DAILY_BUNDLE_DATASET
        if dataset_root.resolve() != expected:
            raise DailyReportIntegrityError(
                "daily-bundle dataset root escapes the configured root"
            )
        return expected

    def _ensure_safe_dataset_root(self) -> Path:
        self.root.mkdir(parents=True, exist_ok=True)
        dataset_root = self.root / NSE_DAILY_BUNDLE_DATASET
        if not dataset_root.exists():
            try:
                dataset_root.mkdir()
            except FileExistsError:
                pass
        return self._assert_safe_dataset_root()

    def _assert_internal_path(self, path: Path) -> None:
        expected_dataset_root = self._assert_safe_dataset_root()
        resolved = path.resolve()
        if not resolved.is_relative_to(expected_dataset_root):
            raise DailyReportIntegrityError(
                "daily-bundle path escapes the configured dataset root"
            )
        current = path
        while current != self.root and current != current.parent:
            if current.exists() and _is_link_like(current):
                raise DailyReportIntegrityError(
                    "daily-bundle path contains a link or junction"
                )
            if current == self.root / NSE_DAILY_BUNDLE_DATASET:
                break
            current = current.parent

    @staticmethod
    def _validate_manifest_constants(manifest: DailyBundleArtifactManifest) -> None:
        if manifest.schema_version != NSE_DAILY_BUNDLE_ARTIFACT_SCHEMA_VERSION:
            raise DailyReportIntegrityError(
                "unsupported daily-bundle artifact schema"
            )
        if manifest.parser_version != NSE_DAILY_BUNDLE_PARSER_VERSION:
            raise DailyReportIntegrityError("unsupported daily-bundle parser version")
        if manifest.normalized_codec_version != NSE_DAILY_BUNDLE_CODEC_VERSION:
            raise DailyReportIntegrityError("unsupported daily-bundle codec version")
        if (
            manifest.claimed_source_catalog_url
            != NSE_DAILY_REPORTS_CLAIMED_CATALOG_URL
        ):
            raise DailyReportIntegrityError(
                "unexpected daily-bundle claimed catalog URL"
            )
        if manifest.raw_filename != RAW_FILENAME:
            raise DailyReportIntegrityError("unexpected daily-bundle raw filename")
        if manifest.normalized_filename != NORMALIZED_FILENAME:
            raise DailyReportIntegrityError(
                "unexpected daily-bundle normalized filename"
            )
        if (
            manifest.first_seen_at.utcoffset() != timedelta(0)
            or manifest.validated_at.utcoffset() != timedelta(0)
        ):
            raise DailyReportIntegrityError(
                "daily-bundle availability timestamps must be UTC"
            )

    @staticmethod
    def _validate_parsed_manifest(parsed, manifest: DailyBundleArtifactManifest) -> None:
        dispositions = Counter(entry.disposition for entry in parsed.entries)
        selected_reports = tuple(
            report
            for report in parsed.reports
            if report.disposition is BundleEntryDisposition.SELECTED_VALIDATED
        )
        values = {
            "original_filename": parsed.original_filename,
            "raw_sha256": parsed.raw_sha256,
            "byte_count": parsed.byte_count,
            "outer_entry_count": len(parsed.entries),
            "selected_report_count": dispositions[
                BundleEntryDisposition.SELECTED_VALIDATED
            ],
            "quarantined_report_count": dispositions[
                BundleEntryDisposition.QUARANTINED_INTEROPERABILITY_SECURITY_MASTER
            ],
            "deferred_report_count": dispositions[
                BundleEntryDisposition.DEFERRED_NSE_ONLY_SECURITY_MASTER
            ],
            "ignored_entry_count": dispositions[
                BundleEntryDisposition.IGNORED_UNAPPROVED
            ],
            "selected_row_count": sum(report.row_count for report in selected_reports),
        }
        if any(getattr(manifest, name) != value for name, value in values.items()):
            raise DailyReportIntegrityError(
                "parsed daily bundle and manifest disagree"
            )
