from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import fields
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.identity import content_id
from india_swing.domain.models import INDIA_STANDARD_TIME
from india_swing.reference.models import ReferenceReadiness

from .codec import encode_security_master
from .models import (
    NSE_CM_SECURITY_DATASET,
    NSE_CM_SECURITY_PARSER_VERSION,
    NSE_CM_SECURITY_SCOPE_POLICY_VERSION,
    NSE_CM_SECURITY_SOURCE_SCHEMA_VERSION,
    REFERENCE_ARTIFACT_SCHEMA_VERSION,
    REFERENCE_NORMALIZED_CODEC_VERSION,
    AcquisitionMode,
    ReferenceArtifactConflict,
    ReferenceArtifactIntegrityError,
    ReferenceArtifactManifest,
    ReferenceArtifactNotFound,
    ReferenceArtifactStale,
    ReferenceArtifactUnverifiedReportDate,
    StoredReferenceArtifact,
)
from .security_master import NseCmSecurityMasterParser


MANIFEST_FILENAME = "manifest.json"
RAW_FILENAME = "source.csv.gz"
NORMALIZED_FILENAME = "normalized.json"
NSE_CLAIMED_REPORT_CATALOG_URL = "https://www.nseindia.com/all-reports"
NSE_CM_CLAIMED_DOWNLOAD_ROOT = "https://nsearchives.nseindia.com/content/cm/"
NSE_CM_MII_PUBLIC_CHANNEL_START = date(2024, 2, 5)
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


def _artifact_identity(manifest: ReferenceArtifactManifest) -> dict[str, object]:
    return {
        "schema_version": manifest.schema_version,
        "dataset": manifest.dataset,
        "claimed_authority": manifest.claimed_authority,
        "acquisition_mode": manifest.acquisition_mode,
        "readiness": manifest.readiness,
        "actionable": manifest.actionable,
        "original_filename": manifest.original_filename,
        "claimed_report_date": manifest.claimed_report_date,
        "verified_report_date": manifest.verified_report_date,
        "claimed_source_catalog_url": manifest.claimed_source_catalog_url,
        "claimed_download_url": manifest.claimed_download_url,
        "source_media_type": manifest.source_media_type,
        "parser_version": manifest.parser_version,
        "source_schema_version": manifest.source_schema_version,
        "scope_policy_version": manifest.scope_policy_version,
        "normalized_codec_version": manifest.normalized_codec_version,
        "compressed_byte_count": manifest.compressed_byte_count,
        "uncompressed_byte_count": manifest.uncompressed_byte_count,
        "raw_sha256": manifest.raw_sha256,
        "uncompressed_sha256": manifest.uncompressed_sha256,
        "normalized_sha256": manifest.normalized_sha256,
        "header_sha256": manifest.header_sha256,
        "raw_row_count": manifest.raw_row_count,
        "parsed_row_count": manifest.parsed_row_count,
        "retained_unverified_equity_count": (
            manifest.retained_unverified_equity_count
        ),
        "excluded_non_equity_count": manifest.excluded_non_equity_count,
        "excluded_test_security_count": manifest.excluded_test_security_count,
        "excluded_alternative_venue_count": (
            manifest.excluded_alternative_venue_count
        ),
        "ordered_row_digest": manifest.ordered_row_digest,
        "raw_filename": manifest.raw_filename,
        "normalized_filename": manifest.normalized_filename,
    }


def _manifest_identity(manifest: ReferenceArtifactManifest) -> dict[str, object]:
    return {
        field.name: getattr(manifest, field.name)
        for field in fields(ReferenceArtifactManifest)
        if field.name != "manifest_id"
    }


def _manifest_json(manifest: ReferenceArtifactManifest) -> bytes:
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
        "claimed_report_date": manifest.claimed_report_date.isoformat(),
        "verified_report_date": (
            manifest.verified_report_date.isoformat()
            if manifest.verified_report_date is not None
            else None
        ),
        "claimed_source_catalog_url": manifest.claimed_source_catalog_url,
        "claimed_download_url": manifest.claimed_download_url,
        "source_media_type": manifest.source_media_type,
        "publication_time_status": manifest.publication_time_status,
        "first_seen_at": manifest.first_seen_at.isoformat(),
        "validated_at": manifest.validated_at.isoformat(),
        "parser_version": manifest.parser_version,
        "source_schema_version": manifest.source_schema_version,
        "scope_policy_version": manifest.scope_policy_version,
        "normalized_codec_version": manifest.normalized_codec_version,
        "compressed_byte_count": manifest.compressed_byte_count,
        "uncompressed_byte_count": manifest.uncompressed_byte_count,
        "raw_sha256": manifest.raw_sha256,
        "uncompressed_sha256": manifest.uncompressed_sha256,
        "normalized_sha256": manifest.normalized_sha256,
        "header_sha256": manifest.header_sha256,
        "raw_row_count": manifest.raw_row_count,
        "parsed_row_count": manifest.parsed_row_count,
        "retained_unverified_equity_count": (
            manifest.retained_unverified_equity_count
        ),
        "excluded_non_equity_count": manifest.excluded_non_equity_count,
        "excluded_test_security_count": manifest.excluded_test_security_count,
        "excluded_alternative_venue_count": (
            manifest.excluded_alternative_venue_count
        ),
        "ordered_row_digest": manifest.ordered_row_digest,
        "raw_filename": manifest.raw_filename,
        "normalized_filename": manifest.normalized_filename,
    }
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def verify_stored_reference_provenance(
    artifact: StoredReferenceArtifact,
) -> None:
    """Require an in-memory reference artifact to match its sealed archive."""

    if type(artifact) is not StoredReferenceArtifact:
        raise TypeError("artifact must be an exact StoredReferenceArtifact")
    path = Path(artifact.path)
    manifest = artifact.manifest
    try:
        if (
            _is_link_like(path)
            or _is_link_like(path.parent)
            or _is_link_like(path.parent.parent)
            or not path.is_dir()
        ):
            raise ReferenceArtifactIntegrityError(
                "reference provenance path is not a sealed directory"
            )
        if (
            path.name != manifest.artifact_id
            or path.parent.name != manifest.validated_at.date().isoformat()
            or path.parent.parent.name != manifest.dataset
        ):
            raise ReferenceArtifactIntegrityError(
                "reference provenance path and manifest disagree"
            )
        entries = tuple(path.iterdir())
        expected_names = {
            MANIFEST_FILENAME,
            RAW_FILENAME,
            NORMALIZED_FILENAME,
        }
        if {entry.name for entry in entries} != expected_names:
            raise ReferenceArtifactIntegrityError(
                "reference provenance archive has unexpected entries"
            )
        if any(_is_link_like(entry) or not entry.is_file() for entry in entries):
            raise ReferenceArtifactIntegrityError(
                "reference provenance entries must be regular files"
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
    except ReferenceArtifactIntegrityError:
        raise
    except (FileSafetyError, OSError, ValueError) as exc:
        raise ReferenceArtifactIntegrityError(
            "reference provenance archive is unavailable or unsafe"
        ) from exc

    if archived_manifest != expected_manifest:
        raise ReferenceArtifactIntegrityError(
            "reference in-memory manifest disagrees with its sealed provenance"
        )
    if archived_raw != artifact.raw_bytes:
        raise ReferenceArtifactIntegrityError(
            "reference in-memory raw bytes disagree with sealed provenance"
        )
    if archived_normalized != artifact.normalized_bytes:
        raise ReferenceArtifactIntegrityError(
            "reference in-memory normalized bytes disagree with sealed provenance"
        )

    # Re-open through the store's strict reader to bind the raw gzip bytes to
    # the parsed records, all manifest counters, and deterministic normalized
    # output. Byte/hash agreement alone cannot prevent parsed-tree substitution.
    try:
        reloaded = LocalReferenceArtifactStore(
            path.parent.parent.parent
        ).get(artifact.manifest.artifact_id)
    except ReferenceArtifactIntegrityError:
        raise
    except (ReferenceArtifactConflict, ReferenceArtifactNotFound) as exc:
        raise ReferenceArtifactIntegrityError(
            "reference sealed provenance is ambiguous or missing"
        ) from exc
    except (OSError, TypeError, ValueError) as exc:
        raise ReferenceArtifactIntegrityError(
            "reference sealed provenance cannot be fully revalidated"
        ) from exc
    if (
        reloaded.path.resolve() != path.resolve()
        or reloaded.manifest != artifact.manifest
        or reloaded.parsed != artifact.parsed
        or reloaded.raw_bytes != artifact.raw_bytes
        or reloaded.normalized_bytes != artifact.normalized_bytes
    ):
        raise ReferenceArtifactIntegrityError(
            "reference memory graph disagrees with its fully parsed archive"
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


class LocalReferenceArtifactStore:
    """Create-once archive for manually obtained official reference artifacts."""

    def __init__(
        self,
        root: Path,
        *,
        parser: NseCmSecurityMasterParser | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.root = Path(root)
        self.parser = parser or NseCmSecurityMasterParser()
        self.clock = clock or (lambda: datetime.now(timezone.utc))

    def import_security_master(self, source_file: Path) -> StoredReferenceArtifact:
        source_path = Path(source_file)
        try:
            raw_bytes = read_stable_regular_file(
                source_path,
                maximum_bytes=self.parser.maximum_compressed_bytes,
            )
        except FileSafetyError as exc:
            raise ReferenceArtifactIntegrityError(
                f"security-master {exc}"
            ) from exc
        first_seen_at = _require_utc(self.clock(), "first_seen_at")

        parsed = self.parser.parse_bytes(
            raw_bytes,
            original_filename=source_path.name,
        )
        first_seen_india_date = first_seen_at.astimezone(INDIA_STANDARD_TIME).date()
        if parsed.claimed_report_date < NSE_CM_MII_PUBLIC_CHANNEL_START:
            raise ReferenceArtifactIntegrityError(
                "claimed report date predates the public NSE MII security-master channel"
            )
        if parsed.claimed_report_date > first_seen_india_date + timedelta(days=1):
            raise ReferenceArtifactIntegrityError(
                "claimed report date is implausibly later than local first-seen time"
            )
        if parsed.excluded_alternative_venue_count:
            raise ReferenceArtifactIntegrityError(
                "alternative-venue records indicate the interoperability file; "
                "use the NSE Listed securities report"
            )
        normalized_bytes = encode_security_master(parsed)
        validated_at = _require_utc(self.clock(), "validated_at")
        if validated_at < first_seen_at:
            raise ReferenceArtifactIntegrityError(
                "validation clock moved backwards"
            )

        provisional = ReferenceArtifactManifest(
            schema_version=REFERENCE_ARTIFACT_SCHEMA_VERSION,
            manifest_id="0" * 64,
            artifact_id="0" * 64,
            dataset=NSE_CM_SECURITY_DATASET,
            claimed_authority="NSE",
            acquisition_mode=AcquisitionMode.UNVERIFIED_MANUAL_FILE,
            readiness=ReferenceReadiness.COLLECTION_ONLY,
            actionable=False,
            original_filename=parsed.original_filename,
            claimed_report_date=parsed.claimed_report_date,
            verified_report_date=None,
            claimed_source_catalog_url=NSE_CLAIMED_REPORT_CATALOG_URL,
            claimed_download_url=(
                NSE_CM_CLAIMED_DOWNLOAD_ROOT + parsed.original_filename
            ),
            source_media_type="application/gzip",
            publication_time_status="UNVERIFIED_MANUAL_FILE",
            first_seen_at=first_seen_at,
            validated_at=validated_at,
            parser_version=NSE_CM_SECURITY_PARSER_VERSION,
            source_schema_version=parsed.source_schema_version,
            scope_policy_version=NSE_CM_SECURITY_SCOPE_POLICY_VERSION,
            normalized_codec_version=REFERENCE_NORMALIZED_CODEC_VERSION,
            compressed_byte_count=parsed.compressed_byte_count,
            uncompressed_byte_count=parsed.uncompressed_byte_count,
            raw_sha256=parsed.raw_sha256,
            uncompressed_sha256=parsed.uncompressed_sha256,
            normalized_sha256=_sha256(normalized_bytes),
            header_sha256=parsed.header_sha256,
            raw_row_count=len(parsed.records),
            parsed_row_count=len(parsed.records),
            retained_unverified_equity_count=(
                parsed.retained_unverified_equity_count
            ),
            excluded_non_equity_count=parsed.excluded_non_equity_count,
            excluded_test_security_count=parsed.excluded_test_security_count,
            excluded_alternative_venue_count=(
                parsed.excluded_alternative_venue_count
            ),
            ordered_row_digest=parsed.ordered_row_digest,
            raw_filename=RAW_FILENAME,
            normalized_filename=NORMALIZED_FILENAME,
        )
        artifact_id = content_id(_artifact_identity(provisional), length=64)
        with_artifact_id = ReferenceArtifactManifest(
            **{
                field.name: (
                    artifact_id
                    if field.name == "artifact_id"
                    else getattr(provisional, field.name)
                )
                for field in fields(ReferenceArtifactManifest)
            }
        )
        manifest_id = content_id(_manifest_identity(with_artifact_id), length=64)
        manifest = ReferenceArtifactManifest(
            **{
                field.name: (
                    manifest_id
                    if field.name == "manifest_id"
                    else getattr(with_artifact_id, field.name)
                )
                for field in fields(ReferenceArtifactManifest)
            }
        )

        existing = self._existing_for_import(
            artifact_id,
            manifest.claimed_report_date,
        )
        if existing is not None:
            return existing
        with self._claimed_report_date_lock(manifest.claimed_report_date):
            existing = self._existing_for_import(
                artifact_id,
                manifest.claimed_report_date,
            )
            if existing is not None:
                return existing
            return self._publish(manifest, raw_bytes, normalized_bytes)

    def _existing_for_import(
        self,
        artifact_id: str,
        claimed_report_date: date,
    ) -> StoredReferenceArtifact | None:
        for existing_path in self._artifact_paths():
            existing = self._read_path(existing_path)
            if existing.manifest.artifact_id == artifact_id:
                return existing
            if existing.manifest.claimed_report_date == claimed_report_date:
                raise ReferenceArtifactConflict(
                    "a different security master is already archived for "
                    "this claimed report date"
                )
        return None

    def get(self, artifact_id: str) -> StoredReferenceArtifact:
        if (
            not isinstance(artifact_id, str)
            or _SHA256_IDENTIFIER.fullmatch(artifact_id) is None
        ):
            raise ValueError("artifact_id must be a full SHA-256 identifier")
        matches = [
            path for path in self._artifact_paths() if path.name == artifact_id
        ]
        if not matches:
            raise ReferenceArtifactNotFound(f"reference artifact not found: {artifact_id}")
        if len(matches) != 1:
            raise ReferenceArtifactIntegrityError(
                "reference artifact ID appears in multiple availability partitions"
            )
        return self._read_path(matches[0])

    def latest_at_or_before(
        self,
        cutoff: datetime,
        *,
        max_age: timedelta,
    ) -> StoredReferenceArtifact:
        cutoff = _require_utc(cutoff, "cutoff")
        if max_age <= timedelta(0):
            raise ValueError("max_age must be positive")
        cutoff_india_date = cutoff.astimezone(INDIA_STANDARD_TIME).date()
        known_candidates = [
            self._read_path(path)
            for path in self._artifact_paths()
        ]
        known_candidates = [
            artifact
            for artifact in known_candidates
            if artifact.manifest.validated_at <= cutoff
        ]
        if not known_candidates:
            raise ReferenceArtifactNotFound(
                "no security master had been validated by the requested cutoff"
            )
        verified_candidates = [
            artifact
            for artifact in known_candidates
            if artifact.manifest.verified_report_date is not None
            and artifact.manifest.verified_report_date <= cutoff_india_date
        ]
        if not verified_candidates:
            raise ReferenceArtifactUnverifiedReportDate(
                "no security master has a verified report date; filename claims "
                "cannot satisfy freshness selection"
            )
        candidates = [
            artifact
            for artifact in verified_candidates
            if cutoff_india_date - artifact.manifest.verified_report_date <= max_age
        ]
        if not candidates:
            raise ReferenceArtifactStale(
                "latest verified security master exceeds max_age"
            )
        return max(
            candidates,
            key=lambda item: (
                item.manifest.verified_report_date,
                item.manifest.validated_at,
                item.manifest.artifact_id,
            ),
        )

    def _artifact_paths(self) -> list[Path]:
        base = self.root / NSE_CM_SECURITY_DATASET
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
    def _claimed_report_date_lock(self, claimed_report_date: date):
        self._ensure_safe_dataset_root()
        lock_root = self.root / NSE_CM_SECURITY_DATASET / ".locks"
        if lock_root.exists():
            self._assert_internal_path(lock_root)
        else:
            # Another importer may create this shared directory after the
            # existence check.  The per-report-date directory below is the
            # actual exclusive lock, so concurrent creation here is benign.
            lock_root.mkdir(exist_ok=True)
        self._assert_internal_path(lock_root)
        lock_path = (
            lock_root
            / f".{claimed_report_date.isoformat()}.advisory-lock"
        )
        if lock_path.exists():
            self._assert_internal_path(lock_path)
        try:
            with advisory_file_lock(lock_path):
                yield
        except FileLockUnavailable as exc:
            raise ReferenceArtifactConflict(
                "another import is already validating this report date"
            ) from exc
        except FileSafetyError as exc:
            raise ReferenceArtifactIntegrityError(
                "reference import lock is unsafe"
            ) from exc

    def _publish(
        self,
        manifest: ReferenceArtifactManifest,
        raw_bytes: bytes,
        normalized_bytes: bytes,
    ) -> StoredReferenceArtifact:
        parent = (
            self.root
            / NSE_CM_SECURITY_DATASET
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
                    raise ReferenceArtifactIntegrityError(
                        "unsafe temporary reference-artifact path"
                    )
                shutil.rmtree(temporary)
        return self._read_path(target)

    def _read_path(self, path: Path) -> StoredReferenceArtifact:
        try:
            self._assert_internal_path(path)
            if _is_link_like(path) or not path.is_dir():
                raise ReferenceArtifactIntegrityError(
                    "reference-artifact path is not a real directory"
                )
            entries = list(path.iterdir())
            if any(_is_link_like(entry) for entry in entries):
                raise ReferenceArtifactIntegrityError(
                    "reference artifact cannot contain symbolic links"
                )
            expected_entries = {
                MANIFEST_FILENAME,
                RAW_FILENAME,
                NORMALIZED_FILENAME,
            }
            if {entry.name for entry in entries} != expected_entries:
                raise ReferenceArtifactIntegrityError(
                    "reference artifact contains unexpected entries"
                )
            if not all(entry.is_file() for entry in entries):
                raise ReferenceArtifactIntegrityError(
                    "reference-artifact entries must be files"
                )

            manifest_value = json.loads(
                (path / MANIFEST_FILENAME).read_text(encoding="utf-8")
            )
            expected_keys = {
                field.name for field in fields(ReferenceArtifactManifest)
            }
            if not isinstance(manifest_value, dict) or set(manifest_value) != expected_keys:
                raise ReferenceArtifactIntegrityError(
                    "reference-artifact manifest schema mismatch"
                )
            manifest = ReferenceArtifactManifest(
                schema_version=str(manifest_value["schema_version"]),
                manifest_id=str(manifest_value["manifest_id"]),
                artifact_id=str(manifest_value["artifact_id"]),
                dataset=str(manifest_value["dataset"]),
                claimed_authority=str(manifest_value["claimed_authority"]),
                acquisition_mode=AcquisitionMode(
                    manifest_value["acquisition_mode"]
                ),
                readiness=ReferenceReadiness(manifest_value["readiness"]),
                actionable=manifest_value["actionable"],
                original_filename=str(manifest_value["original_filename"]),
                claimed_report_date=date.fromisoformat(
                    str(manifest_value["claimed_report_date"])
                ),
                verified_report_date=(
                    date.fromisoformat(str(manifest_value["verified_report_date"]))
                    if manifest_value["verified_report_date"] is not None
                    else None
                ),
                claimed_source_catalog_url=str(
                    manifest_value["claimed_source_catalog_url"]
                ),
                claimed_download_url=str(
                    manifest_value["claimed_download_url"]
                ),
                source_media_type=str(manifest_value["source_media_type"]),
                publication_time_status=str(
                    manifest_value["publication_time_status"]
                ),
                first_seen_at=datetime.fromisoformat(
                    str(manifest_value["first_seen_at"])
                ),
                validated_at=datetime.fromisoformat(
                    str(manifest_value["validated_at"])
                ),
                parser_version=str(manifest_value["parser_version"]),
                source_schema_version=str(
                    manifest_value["source_schema_version"]
                ),
                scope_policy_version=str(manifest_value["scope_policy_version"]),
                normalized_codec_version=str(
                    manifest_value["normalized_codec_version"]
                ),
                compressed_byte_count=manifest_value["compressed_byte_count"],
                uncompressed_byte_count=manifest_value[
                    "uncompressed_byte_count"
                ],
                raw_sha256=str(manifest_value["raw_sha256"]),
                uncompressed_sha256=str(manifest_value["uncompressed_sha256"]),
                normalized_sha256=str(manifest_value["normalized_sha256"]),
                header_sha256=str(manifest_value["header_sha256"]),
                raw_row_count=manifest_value["raw_row_count"],
                parsed_row_count=manifest_value["parsed_row_count"],
                retained_unverified_equity_count=manifest_value[
                    "retained_unverified_equity_count"
                ],
                excluded_non_equity_count=manifest_value[
                    "excluded_non_equity_count"
                ],
                excluded_test_security_count=manifest_value[
                    "excluded_test_security_count"
                ],
                excluded_alternative_venue_count=manifest_value[
                    "excluded_alternative_venue_count"
                ],
                ordered_row_digest=str(manifest_value["ordered_row_digest"]),
                raw_filename=str(manifest_value["raw_filename"]),
                normalized_filename=str(manifest_value["normalized_filename"]),
            )
            self._validate_manifest_constants(manifest)
            if path.parent.parent.name != manifest.dataset:
                raise ReferenceArtifactIntegrityError(
                    "reference-artifact dataset partition mismatch"
                )
            if path.parent.name != manifest.validated_at.date().isoformat():
                raise ReferenceArtifactIntegrityError(
                    "reference-artifact availability partition mismatch"
                )
            if path.name != manifest.artifact_id and not path.name.startswith(
                f".{manifest.artifact_id}."
            ):
                raise ReferenceArtifactIntegrityError(
                    "reference-artifact directory and manifest disagree"
                )

            raw_bytes = (path / RAW_FILENAME).read_bytes()
            normalized_bytes = (path / NORMALIZED_FILENAME).read_bytes()
            if _sha256(raw_bytes) != manifest.raw_sha256:
                raise ReferenceArtifactIntegrityError(
                    "reference-artifact raw hash mismatch"
                )
            if _sha256(normalized_bytes) != manifest.normalized_sha256:
                raise ReferenceArtifactIntegrityError(
                    "reference-artifact normalized hash mismatch"
                )
            parsed = self.parser.parse_bytes(
                raw_bytes,
                original_filename=manifest.original_filename,
            )
            if encode_security_master(parsed) != normalized_bytes:
                raise ReferenceArtifactIntegrityError(
                    "normalized reference artifact is not deterministic"
                )
            self._validate_parsed_manifest(parsed, manifest)
            if content_id(_artifact_identity(manifest), length=64) != manifest.artifact_id:
                raise ReferenceArtifactIntegrityError(
                    "reference artifact ID does not match its content"
                )
            if content_id(_manifest_identity(manifest), length=64) != manifest.manifest_id:
                raise ReferenceArtifactIntegrityError(
                    "reference manifest ID does not match its content"
                )
        except ReferenceArtifactIntegrityError:
            raise
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ReferenceArtifactIntegrityError(
                "reference artifact is incomplete or malformed"
            ) from exc
        return StoredReferenceArtifact(
            path=path,
            manifest=manifest,
            parsed=parsed,
            raw_bytes=raw_bytes,
            normalized_bytes=normalized_bytes,
        )

    def _assert_safe_dataset_root(self) -> Path:
        configured_root = self.root.resolve()
        dataset_root = self.root / NSE_CM_SECURITY_DATASET
        if not dataset_root.exists():
            raise ReferenceArtifactIntegrityError(
                "reference-artifact dataset root does not exist"
            )
        if _is_link_like(dataset_root):
            raise ReferenceArtifactIntegrityError(
                "reference-artifact dataset root cannot be a link or junction"
            )
        expected = configured_root / NSE_CM_SECURITY_DATASET
        if dataset_root.resolve() != expected:
            raise ReferenceArtifactIntegrityError(
                "reference-artifact dataset root escapes the configured root"
            )
        return expected

    def _ensure_safe_dataset_root(self) -> Path:
        self.root.mkdir(parents=True, exist_ok=True)
        dataset_root = self.root / NSE_CM_SECURITY_DATASET
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
            raise ReferenceArtifactIntegrityError(
                "reference-artifact path escapes the configured dataset root"
            )
        current = path
        while current != self.root and current != current.parent:
            if current.exists() and _is_link_like(current):
                raise ReferenceArtifactIntegrityError(
                    "reference-artifact path contains a link or junction"
                )
            if current == self.root / NSE_CM_SECURITY_DATASET:
                break
            current = current.parent

    @staticmethod
    def _validate_manifest_constants(manifest: ReferenceArtifactManifest) -> None:
        if manifest.schema_version != REFERENCE_ARTIFACT_SCHEMA_VERSION:
            raise ReferenceArtifactIntegrityError(
                "unsupported reference-artifact schema"
            )
        if manifest.parser_version != NSE_CM_SECURITY_PARSER_VERSION:
            raise ReferenceArtifactIntegrityError("unsupported parser version")
        if manifest.source_schema_version != NSE_CM_SECURITY_SOURCE_SCHEMA_VERSION:
            raise ReferenceArtifactIntegrityError("unsupported NSE source schema")
        if manifest.scope_policy_version != NSE_CM_SECURITY_SCOPE_POLICY_VERSION:
            raise ReferenceArtifactIntegrityError("unsupported scope policy")
        if manifest.normalized_codec_version != REFERENCE_NORMALIZED_CODEC_VERSION:
            raise ReferenceArtifactIntegrityError("unsupported normalized codec")
        if manifest.claimed_source_catalog_url != NSE_CLAIMED_REPORT_CATALOG_URL:
            raise ReferenceArtifactIntegrityError(
                "unexpected claimed source catalog URL"
            )
        expected_download_url = NSE_CM_CLAIMED_DOWNLOAD_ROOT + manifest.original_filename
        if manifest.claimed_download_url != expected_download_url:
            raise ReferenceArtifactIntegrityError(
                "unexpected claimed download URL"
            )
        if manifest.source_media_type != "application/gzip":
            raise ReferenceArtifactIntegrityError("unexpected source media type")
        if manifest.publication_time_status != "UNVERIFIED_MANUAL_FILE":
            raise ReferenceArtifactIntegrityError(
                "unexpected publication-time status"
            )
        if manifest.raw_filename != RAW_FILENAME:
            raise ReferenceArtifactIntegrityError("unexpected raw archive filename")
        if manifest.normalized_filename != NORMALIZED_FILENAME:
            raise ReferenceArtifactIntegrityError(
                "unexpected normalized archive filename"
            )
        if (
            manifest.first_seen_at.utcoffset() != timedelta(0)
            or manifest.validated_at.utcoffset() != timedelta(0)
        ):
            raise ReferenceArtifactIntegrityError(
                "artifact availability timestamps must be UTC"
            )

    @staticmethod
    def _validate_parsed_manifest(parsed, manifest: ReferenceArtifactManifest) -> None:
        values = {
            "original_filename": parsed.original_filename,
            "claimed_report_date": parsed.claimed_report_date,
            "source_schema_version": parsed.source_schema_version,
            "compressed_byte_count": parsed.compressed_byte_count,
            "uncompressed_byte_count": parsed.uncompressed_byte_count,
            "raw_sha256": parsed.raw_sha256,
            "uncompressed_sha256": parsed.uncompressed_sha256,
            "header_sha256": parsed.header_sha256,
            "raw_row_count": len(parsed.records),
            "parsed_row_count": len(parsed.records),
            "retained_unverified_equity_count": (
                parsed.retained_unverified_equity_count
            ),
            "excluded_non_equity_count": parsed.excluded_non_equity_count,
            "excluded_test_security_count": parsed.excluded_test_security_count,
            "excluded_alternative_venue_count": (
                parsed.excluded_alternative_venue_count
            ),
            "ordered_row_digest": parsed.ordered_row_digest,
        }
        if any(getattr(manifest, name) != value for name, value in values.items()):
            raise ReferenceArtifactIntegrityError(
                "parsed security master and manifest disagree"
            )
