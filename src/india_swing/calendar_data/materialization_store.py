from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, fields, field, replace
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path, PurePath
from typing import Iterator

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.calendar_evidence import build_observed_market_date_artifact
from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness

from .artifact_store import (
    LocalCalendarSourceArtifactStore,
    _manifest_from_value as _calendar_source_manifest_from_value,
)
from .materialization import (
    CALENDAR_MATERIALIZATION_POLICY_VERSION,
    CALENDAR_MATERIALIZATION_SCHEMA_VERSION,
    CalendarMaterializationError,
    CollectionCalendarMaterialization,
    ObservedDateEvidenceBinding,
    materialize_collection_calendar,
)
from .materialization_codec import (
    MAXIMUM_CALENDAR_MATERIALIZATION_BYTES,
    encode_calendar_materialization,
)
from .models import (
    CalendarSourceArtifactError,
    CalendarSourceArtifactManifest,
)
from .parser import decode_strict_json


CALENDAR_MATERIALIZATION_STORE_DATASET = "nse-cm-calendar-materializations"
CALENDAR_MATERIALIZATION_STORE_SCHEMA_VERSION = (
    "nse-cm-calendar-materialization-store/v1"
)
CALENDAR_MATERIALIZATION_STORE_CODEC_VERSION = (
    "nse-cm-calendar-materialization-json/v1"
)
MANIFEST_FILENAME = "manifest.json"
MATERIALIZATION_FILENAME = "materialization.json"
MAXIMUM_MANIFEST_BYTES = 64 * 1024 * 1024
# Exact compatibility alias: materialization_codec.py is now the byte-ceiling
# authority; this name is retained unchanged for every existing caller.
MAXIMUM_MATERIALIZATION_BYTES = MAXIMUM_CALENDAR_MATERIALIZATION_BYTES

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class CalendarMaterializationStoreError(RuntimeError):
    pass


class CalendarMaterializationStoreIntegrityError(CalendarMaterializationStoreError):
    pass


class CalendarMaterializationStoreConflict(CalendarMaterializationStoreError):
    pass


class CalendarMaterializationStoreNotFound(CalendarMaterializationStoreError):
    pass


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _require_sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a full lowercase SHA-256")


def _utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _safe_basename(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or PurePath(value).name != value
        or "/" in value
        or "\\" in value
    ):
        raise ValueError(f"{field_name} must be a safe basename")


def _is_link_like(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(is_junction and is_junction())


def _json_value(value: object) -> object:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    if value is None or type(value) in {bool, int, str}:
        return value
    raise TypeError("manifest contains an unsupported JSON value")


def _source_manifest_value(
    manifest: CalendarSourceArtifactManifest,
) -> dict[str, object]:
    return {
        item.name: _json_value(getattr(manifest, item.name))
        for item in fields(CalendarSourceArtifactManifest)
    }


def _binding_value(binding: ObservedDateEvidenceBinding) -> dict[str, object]:
    return {
        "artifact_id": binding.artifact_id,
        "cutoff": binding.cutoff.isoformat(),
        "knowledge_time": binding.knowledge_time.isoformat(),
        "source_bundle_artifact_id": binding.source_bundle_artifact_id,
        "source_bundle_manifest_id": binding.source_bundle_manifest_id,
        "observed_dates": [value.isoformat() for value in binding.observed_dates],
        "binding_id": binding.binding_id,
    }


def _binding_from_value(value: object) -> ObservedDateEvidenceBinding:
    expected = {
        "artifact_id",
        "cutoff",
        "knowledge_time",
        "source_bundle_artifact_id",
        "source_bundle_manifest_id",
        "observed_dates",
        "binding_id",
    }
    if type(value) is not dict or set(value) != expected:
        raise CalendarMaterializationStoreIntegrityError(
            "observed-date binding schema mismatch"
        )
    observed_dates = value["observed_dates"]
    if type(observed_dates) is not list:
        raise CalendarMaterializationStoreIntegrityError(
            "observed-date binding dates must be an array"
        )
    try:
        binding = ObservedDateEvidenceBinding(
            artifact_id=value["artifact_id"],
            cutoff=datetime.fromisoformat(value["cutoff"]),
            knowledge_time=datetime.fromisoformat(value["knowledge_time"]),
            source_bundle_artifact_id=value["source_bundle_artifact_id"],
            source_bundle_manifest_id=value["source_bundle_manifest_id"],
            observed_dates=tuple(date.fromisoformat(item) for item in observed_dates),
        )
    except (TypeError, ValueError) as exc:
        raise CalendarMaterializationStoreIntegrityError(
            "observed-date binding is malformed"
        ) from exc
    if value["binding_id"] != binding.binding_id:
        raise CalendarMaterializationStoreIntegrityError(
            "observed-date binding content identity mismatch"
        )
    return binding


@dataclass(frozen=True, slots=True)
class CalendarMaterializationStoreManifest:
    schema_version: str
    manifest_id: str
    artifact_id: str
    dataset: str
    exchange: str
    segment: str
    cutoff: datetime
    coverage_start: date
    coverage_end: date
    readiness: ReferenceReadiness
    actionable: bool
    materialization_schema_version: str
    materialization_policy_version: str
    materialization_codec_version: str
    materialization_filename: str
    materialization_byte_count: int
    materialization_sha256: str
    calendar_snapshot_id: str
    calendar_snapshot_version: str
    source_manifests: tuple[CalendarSourceArtifactManifest, ...]
    observed_evidence_bindings: tuple[ObservedDateEvidenceBinding, ...]
    source_count: int
    day_count: int
    session_count: int
    observed_evidence_count: int
    observed_date_count: int

    def __post_init__(self) -> None:
        for value, name in (
            (self.manifest_id, "manifest_id"),
            (self.artifact_id, "artifact_id"),
            (self.materialization_sha256, "materialization_sha256"),
            (self.calendar_snapshot_id, "calendar_snapshot_id"),
        ):
            _require_sha256(value, name)
        if (
            self.schema_version != CALENDAR_MATERIALIZATION_STORE_SCHEMA_VERSION
            or self.dataset != CALENDAR_MATERIALIZATION_STORE_DATASET
            or (self.exchange, self.segment) != ("NSE", "CM")
        ):
            raise ValueError("unsupported calendar materialization store contract")
        object.__setattr__(self, "cutoff", _utc(self.cutoff, "cutoff"))
        if type(self.coverage_start) is not date or type(self.coverage_end) is not date:
            raise TypeError("coverage bounds must be dates")
        if self.coverage_end < self.coverage_start:
            raise ValueError("coverage end precedes coverage start")
        if (
            self.readiness is not ReferenceReadiness.COLLECTION_ONLY
            or type(self.actionable) is not bool
            or self.actionable
        ):
            raise ValueError("stored materializations must remain collection-only")
        if (
            self.materialization_schema_version
            != CALENDAR_MATERIALIZATION_SCHEMA_VERSION
            or self.materialization_policy_version
            != CALENDAR_MATERIALIZATION_POLICY_VERSION
            or self.materialization_codec_version
            != CALENDAR_MATERIALIZATION_STORE_CODEC_VERSION
        ):
            raise ValueError("unsupported materialization schema, policy, or codec")
        _safe_basename(self.materialization_filename, "materialization_filename")
        if self.materialization_filename != MATERIALIZATION_FILENAME:
            raise ValueError("unexpected materialization archive filename")
        if (
            type(self.materialization_byte_count) is not int
            or self.materialization_byte_count <= 0
            or self.materialization_byte_count > MAXIMUM_MATERIALIZATION_BYTES
        ):
            raise ValueError("materialization byte count is outside the pinned bound")
        if (
            not isinstance(self.calendar_snapshot_version, str)
            or not self.calendar_snapshot_version
        ):
            raise ValueError("calendar snapshot version is required")
        if type(self.source_manifests) is not tuple or not self.source_manifests or any(
            type(value) is not CalendarSourceArtifactManifest
            for value in self.source_manifests
        ):
            raise TypeError("source_manifests must be a non-empty exact tuple")
        if tuple(
            sorted(
                self.source_manifests,
                key=lambda value: (value.artifact_id, value.manifest_id),
            )
        ) != self.source_manifests:
            raise ValueError("source manifests must be deterministically sorted")
        if len({value.artifact_id for value in self.source_manifests}) != len(
            self.source_manifests
        ):
            raise ValueError("source manifests contain duplicate artifact IDs")
        if type(self.observed_evidence_bindings) is not tuple or any(
            type(value) is not ObservedDateEvidenceBinding
            for value in self.observed_evidence_bindings
        ):
            raise TypeError("observed evidence bindings must be an exact tuple")
        if tuple(
            sorted(
                self.observed_evidence_bindings,
                key=lambda value: value.artifact_id,
            )
        ) != self.observed_evidence_bindings:
            raise ValueError("observed evidence bindings must be sorted")
        for value in self.observed_evidence_bindings:
            value.verify_content_identity()
        for value, name in (
            (self.source_count, "source_count"),
            (self.day_count, "day_count"),
            (self.session_count, "session_count"),
            (self.observed_evidence_count, "observed_evidence_count"),
            (self.observed_date_count, "observed_date_count"),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        expected_days = (self.coverage_end - self.coverage_start).days + 1
        if (
            self.source_count != len(self.source_manifests)
            or self.day_count != expected_days
            or self.session_count > self.day_count
            or self.observed_evidence_count != len(self.observed_evidence_bindings)
            or self.observed_date_count
            != sum(
                len(value.observed_dates)
                for value in self.observed_evidence_bindings
            )
        ):
            raise ValueError("calendar materialization store counts are inconsistent")

    def _calculated_manifest_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(CalendarMaterializationStoreManifest)
                if item.name != "manifest_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.manifest_id != self._calculated_manifest_id():
            raise CalendarMaterializationStoreIntegrityError(
                "calendar materialization store manifest identity mismatch"
            )


@dataclass(frozen=True, slots=True)
class StoredCalendarMaterialization:
    path: Path
    manifest: CalendarMaterializationStoreManifest
    materialization: CollectionCalendarMaterialization
    encoded_bytes: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.path, Path):
            raise TypeError("stored materialization path must be a Path")
        if type(self.manifest) is not CalendarMaterializationStoreManifest:
            raise TypeError("stored materialization manifest must be exact")
        if type(self.materialization) is not CollectionCalendarMaterialization:
            raise TypeError("stored materialization must be exact")
        if type(self.encoded_bytes) is not bytes:
            raise TypeError("stored materialization bytes must be exact bytes")


def _manifest_value(
    manifest: CalendarMaterializationStoreManifest,
) -> dict[str, object]:
    return {
        "schema_version": manifest.schema_version,
        "manifest_id": manifest.manifest_id,
        "artifact_id": manifest.artifact_id,
        "dataset": manifest.dataset,
        "exchange": manifest.exchange,
        "segment": manifest.segment,
        "cutoff": manifest.cutoff.isoformat(),
        "coverage_start": manifest.coverage_start.isoformat(),
        "coverage_end": manifest.coverage_end.isoformat(),
        "readiness": manifest.readiness.value,
        "actionable": manifest.actionable,
        "materialization_schema_version": manifest.materialization_schema_version,
        "materialization_policy_version": manifest.materialization_policy_version,
        "materialization_codec_version": manifest.materialization_codec_version,
        "materialization_filename": manifest.materialization_filename,
        "materialization_byte_count": manifest.materialization_byte_count,
        "materialization_sha256": manifest.materialization_sha256,
        "calendar_snapshot_id": manifest.calendar_snapshot_id,
        "calendar_snapshot_version": manifest.calendar_snapshot_version,
        "source_manifests": [
            _source_manifest_value(value) for value in manifest.source_manifests
        ],
        "observed_evidence_bindings": [
            _binding_value(value) for value in manifest.observed_evidence_bindings
        ],
        "source_count": manifest.source_count,
        "day_count": manifest.day_count,
        "session_count": manifest.session_count,
        "observed_evidence_count": manifest.observed_evidence_count,
        "observed_date_count": manifest.observed_date_count,
    }


def _manifest_json(manifest: CalendarMaterializationStoreManifest) -> bytes:
    return (json.dumps(_manifest_value(manifest), indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )


def _manifest_from_value(value: object) -> CalendarMaterializationStoreManifest:
    expected = {item.name for item in fields(CalendarMaterializationStoreManifest)}
    if type(value) is not dict or set(value) != expected:
        raise CalendarMaterializationStoreIntegrityError(
            "calendar materialization store manifest schema mismatch"
        )
    source_values = value["source_manifests"]
    binding_values = value["observed_evidence_bindings"]
    if type(source_values) is not list or type(binding_values) is not list:
        raise CalendarMaterializationStoreIntegrityError(
            "calendar materialization lineage must use arrays"
        )
    try:
        return CalendarMaterializationStoreManifest(
            schema_version=value["schema_version"],
            manifest_id=value["manifest_id"],
            artifact_id=value["artifact_id"],
            dataset=value["dataset"],
            exchange=value["exchange"],
            segment=value["segment"],
            cutoff=datetime.fromisoformat(value["cutoff"]),
            coverage_start=date.fromisoformat(value["coverage_start"]),
            coverage_end=date.fromisoformat(value["coverage_end"]),
            readiness=ReferenceReadiness(value["readiness"]),
            actionable=value["actionable"],
            materialization_schema_version=value["materialization_schema_version"],
            materialization_policy_version=value["materialization_policy_version"],
            materialization_codec_version=value["materialization_codec_version"],
            materialization_filename=value["materialization_filename"],
            materialization_byte_count=value["materialization_byte_count"],
            materialization_sha256=value["materialization_sha256"],
            calendar_snapshot_id=value["calendar_snapshot_id"],
            calendar_snapshot_version=value["calendar_snapshot_version"],
            source_manifests=tuple(
                _calendar_source_manifest_from_value(item) for item in source_values
            ),
            observed_evidence_bindings=tuple(
                _binding_from_value(item) for item in binding_values
            ),
            source_count=value["source_count"],
            day_count=value["day_count"],
            session_count=value["session_count"],
            observed_evidence_count=value["observed_evidence_count"],
            observed_date_count=value["observed_date_count"],
        )
    except CalendarMaterializationStoreIntegrityError:
        raise
    except (TypeError, ValueError, KeyError) as exc:
        raise CalendarMaterializationStoreIntegrityError(
            "calendar materialization store manifest is malformed"
        ) from exc


def _write_new(path: Path, payload: bytes) -> None:
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


class LocalCalendarMaterializationStore:
    """Sealed, replay-verified store for collection-only calendar materializations."""

    def __init__(self, root: Path, daily_reports_root: Path) -> None:
        self.root = Path(root)
        self.daily_reports_root = Path(daily_reports_root)

    def put(
        self,
        materialization: CollectionCalendarMaterialization,
    ) -> StoredCalendarMaterialization:
        if type(materialization) is not CollectionCalendarMaterialization:
            raise TypeError("materialization must be exact")
        try:
            materialization.verify_content_identity()
        except (TypeError, ValueError) as exc:
            raise CalendarMaterializationStoreIntegrityError(
                "materialization failed content identity verification"
            ) from exc
        rebuilt = self._replay(
            source_manifests=materialization.source_manifests,
            evidence_bindings=materialization.observed_evidence_bindings,
            cutoff=materialization.cutoff,
            coverage_start=materialization.coverage_start,
            coverage_end=materialization.coverage_end,
        )
        self._require_exact_replay(materialization, rebuilt)
        encoded = encode_calendar_materialization(rebuilt)
        manifest = self._build_manifest(rebuilt, encoded)

        existing = self._existing(manifest.artifact_id)
        if existing is not None:
            return existing
        with self._artifact_lock(manifest.artifact_id):
            existing = self._existing(manifest.artifact_id)
            if existing is not None:
                return existing
            return self._publish(manifest, encoded)

    def get(self, artifact_id: str) -> StoredCalendarMaterialization:
        if not isinstance(artifact_id, str) or _SHA256.fullmatch(artifact_id) is None:
            raise CalendarMaterializationStoreNotFound("invalid materialization ID")
        dataset_root = self.root / CALENDAR_MATERIALIZATION_STORE_DATASET
        if not dataset_root.exists():
            raise CalendarMaterializationStoreNotFound("materialization was not found")
        safe_root = self._assert_safe_dataset_root()
        matches = list(safe_root.glob(f"*/{artifact_id}"))
        if not matches:
            raise CalendarMaterializationStoreNotFound("materialization was not found")
        if len(matches) != 1:
            raise CalendarMaterializationStoreConflict(
                "materialization ID appears in multiple partitions"
            )
        return self._read_path(matches[0])

    def _replay(
        self,
        *,
        source_manifests: tuple[CalendarSourceArtifactManifest, ...],
        evidence_bindings: tuple[ObservedDateEvidenceBinding, ...],
        cutoff: datetime,
        coverage_start: date,
        coverage_end: date,
    ) -> CollectionCalendarMaterialization:
        source_store = LocalCalendarSourceArtifactStore(self.root)
        daily_store = LocalDailyBundleArtifactStore(self.daily_reports_root)
        sources = []
        evidence = []
        try:
            for embedded in source_manifests:
                loaded = source_store.get(embedded.artifact_id)
                if loaded.manifest != embedded:
                    raise CalendarMaterializationStoreIntegrityError(
                        "embedded calendar source manifest differs from sealed source"
                    )
                sources.append(loaded)
            for binding in evidence_bindings:
                bundle = daily_store.get(binding.source_bundle_artifact_id)
                if bundle.manifest.manifest_id != binding.source_bundle_manifest_id:
                    raise CalendarMaterializationStoreIntegrityError(
                        "observed-date binding differs from sealed daily-bundle manifest"
                    )
                rebuilt_evidence = build_observed_market_date_artifact(
                    bundle,
                    cutoff=binding.cutoff,
                )
                if (
                    rebuilt_evidence.artifact_id != binding.artifact_id
                    or rebuilt_evidence.cutoff != binding.cutoff
                    or rebuilt_evidence.knowledge_time != binding.knowledge_time
                    or rebuilt_evidence.source_bundle_artifact_id
                    != binding.source_bundle_artifact_id
                    or rebuilt_evidence.source_bundle_manifest_id
                    != binding.source_bundle_manifest_id
                    or rebuilt_evidence.observed_dates != binding.observed_dates
                ):
                    raise CalendarMaterializationStoreIntegrityError(
                        "observed-date evidence cannot be rebuilt from its exact binding"
                    )
                evidence.append(rebuilt_evidence)
            return materialize_collection_calendar(
                sources=tuple(sources),
                coverage_start=coverage_start,
                coverage_end=coverage_end,
                cutoff=cutoff,
                observed_date_artifacts=tuple(evidence),
            )
        except CalendarMaterializationStoreIntegrityError:
            raise
        except (CalendarSourceArtifactError, CalendarMaterializationError, TypeError, ValueError) as exc:
            raise CalendarMaterializationStoreIntegrityError(
                "calendar materialization replay failed"
            ) from exc

    @staticmethod
    def _require_exact_replay(
        expected: CollectionCalendarMaterialization,
        rebuilt: CollectionCalendarMaterialization,
    ) -> None:
        expected_bytes = encode_calendar_materialization(expected)
        rebuilt_bytes = encode_calendar_materialization(rebuilt)
        expected_counts = (
            len(expected.source_manifests),
            len(expected.day_resolutions),
            len(expected.observed_evidence_bindings),
            sum(day.is_session for day in expected.calendar_snapshot.days),
        )
        rebuilt_counts = (
            len(rebuilt.source_manifests),
            len(rebuilt.day_resolutions),
            len(rebuilt.observed_evidence_bindings),
            sum(day.is_session for day in rebuilt.calendar_snapshot.days),
        )
        if (
            expected != rebuilt
            or expected_bytes != rebuilt_bytes
            or expected.materialization_id != rebuilt.materialization_id
            or expected.calendar_snapshot.snapshot_id
            != rebuilt.calendar_snapshot.snapshot_id
            or expected.calendar_snapshot.version != rebuilt.calendar_snapshot.version
            or expected_counts != rebuilt_counts
        ):
            raise CalendarMaterializationStoreIntegrityError(
                "materialization does not equal its exact source replay"
            )

    @staticmethod
    def _build_manifest(
        materialization: CollectionCalendarMaterialization,
        encoded: bytes,
    ) -> CalendarMaterializationStoreManifest:
        provisional = CalendarMaterializationStoreManifest(
            schema_version=CALENDAR_MATERIALIZATION_STORE_SCHEMA_VERSION,
            manifest_id="0" * 64,
            artifact_id=materialization.materialization_id,
            dataset=CALENDAR_MATERIALIZATION_STORE_DATASET,
            exchange="NSE",
            segment="CM",
            cutoff=materialization.cutoff,
            coverage_start=materialization.coverage_start,
            coverage_end=materialization.coverage_end,
            readiness=ReferenceReadiness.COLLECTION_ONLY,
            actionable=False,
            materialization_schema_version=materialization.schema_version,
            materialization_policy_version=materialization.policy_version,
            materialization_codec_version=CALENDAR_MATERIALIZATION_STORE_CODEC_VERSION,
            materialization_filename=MATERIALIZATION_FILENAME,
            materialization_byte_count=len(encoded),
            materialization_sha256=_sha256(encoded),
            calendar_snapshot_id=materialization.calendar_snapshot.snapshot_id,
            calendar_snapshot_version=materialization.calendar_snapshot.version,
            source_manifests=materialization.source_manifests,
            observed_evidence_bindings=materialization.observed_evidence_bindings,
            source_count=len(materialization.source_manifests),
            day_count=len(materialization.day_resolutions),
            session_count=sum(
                day.is_session for day in materialization.calendar_snapshot.days
            ),
            observed_evidence_count=len(materialization.observed_evidence_bindings),
            observed_date_count=sum(
                len(value.observed_dates)
                for value in materialization.observed_evidence_bindings
            ),
        )
        return replace(
            provisional,
            manifest_id=provisional._calculated_manifest_id(),
        )

    @staticmethod
    def _validate_manifest_materialization(
        manifest: CalendarMaterializationStoreManifest,
        materialization: CollectionCalendarMaterialization,
        encoded: bytes,
    ) -> None:
        expected = LocalCalendarMaterializationStore._build_manifest(
            materialization,
            encoded,
        )
        if manifest != expected:
            raise CalendarMaterializationStoreIntegrityError(
                "stored manifest does not equal the replayed materialization"
            )

    def _existing(self, artifact_id: str) -> StoredCalendarMaterialization | None:
        try:
            return self.get(artifact_id)
        except CalendarMaterializationStoreNotFound:
            return None

    @contextmanager
    def _artifact_lock(self, artifact_id: str) -> Iterator[None]:
        dataset_root = self._ensure_safe_dataset_root()
        lock_root = dataset_root / ".locks"
        lock_root.mkdir(exist_ok=True)
        if _is_link_like(lock_root) or not lock_root.is_dir():
            raise CalendarMaterializationStoreIntegrityError(
                "materialization lock root is unsafe"
            )
        try:
            with advisory_file_lock(lock_root / f"{artifact_id}.lock"):
                yield
        except FileLockUnavailable as exc:
            raise CalendarMaterializationStoreConflict(
                "materialization publication is already in progress"
            ) from exc
        except FileSafetyError as exc:
            raise CalendarMaterializationStoreIntegrityError(
                "materialization lock is unsafe"
            ) from exc

    def _publish(
        self,
        manifest: CalendarMaterializationStoreManifest,
        encoded: bytes,
    ) -> StoredCalendarMaterialization:
        dataset_root = self._ensure_safe_dataset_root()
        parent = dataset_root / manifest.cutoff.date().isoformat()
        parent.mkdir(exist_ok=True)
        self._assert_internal_path(parent)
        target = parent / manifest.artifact_id
        if target.exists():
            return self._read_path(target)
        temporary = Path(
            tempfile.mkdtemp(prefix=f".{manifest.artifact_id}.", dir=parent)
        )
        try:
            _write_new(temporary / MANIFEST_FILENAME, _manifest_json(manifest))
            _write_new(temporary / MATERIALIZATION_FILENAME, encoded)
            self._read_path(temporary, allow_temporary=True)
            _fsync_directory(temporary)
            try:
                os.rename(temporary, target)
            except OSError:
                if target.exists():
                    return self._read_path(target)
                raise
            _fsync_directory(parent)
            _fsync_directory(dataset_root)
        finally:
            if temporary.exists():
                resolved_parent = parent.resolve()
                resolved_temporary = temporary.resolve()
                if not resolved_temporary.is_relative_to(resolved_parent):
                    raise CalendarMaterializationStoreIntegrityError(
                        "unsafe temporary materialization path"
                    )
                shutil.rmtree(temporary)
        return self._read_path(target)

    def _read_path(
        self,
        path: Path,
        *,
        allow_temporary: bool = False,
    ) -> StoredCalendarMaterialization:
        try:
            self._assert_internal_path(path)
            if _is_link_like(path) or not path.is_dir():
                raise CalendarMaterializationStoreIntegrityError(
                    "materialization path is not a real directory"
                )
            entries = tuple(path.iterdir())
            if {entry.name for entry in entries} != {
                MANIFEST_FILENAME,
                MATERIALIZATION_FILENAME,
            }:
                raise CalendarMaterializationStoreIntegrityError(
                    "materialization archive has unexpected entries"
                )
            if any(_is_link_like(entry) or not entry.is_file() for entry in entries):
                raise CalendarMaterializationStoreIntegrityError(
                    "materialization archive entries must be regular files"
                )
            manifest_bytes = read_stable_regular_file(
                path / MANIFEST_FILENAME,
                maximum_bytes=MAXIMUM_MANIFEST_BYTES,
            )
            manifest = _manifest_from_value(
                decode_strict_json(
                    manifest_bytes,
                    label="calendar materialization store manifest",
                )
            )
            manifest.verify_content_identity()
            if manifest_bytes != _manifest_json(manifest):
                raise CalendarMaterializationStoreIntegrityError(
                    "materialization manifest encoding is not canonical"
                )
            expected_name = manifest.artifact_id
            if allow_temporary:
                valid_name = path.name.startswith(f".{expected_name}.")
            else:
                valid_name = path.name == expected_name
            if (
                path.parent.parent.name != manifest.dataset
                or path.parent.name != manifest.cutoff.date().isoformat()
                or not valid_name
            ):
                raise CalendarMaterializationStoreIntegrityError(
                    "materialization path and manifest disagree"
                )
            encoded = read_stable_regular_file(
                path / MATERIALIZATION_FILENAME,
                maximum_bytes=manifest.materialization_byte_count,
            )
            if (
                len(encoded) != manifest.materialization_byte_count
                or _sha256(encoded) != manifest.materialization_sha256
            ):
                raise CalendarMaterializationStoreIntegrityError(
                    "materialization bytes fail their manifest"
                )
            rebuilt = self._replay(
                source_manifests=manifest.source_manifests,
                evidence_bindings=manifest.observed_evidence_bindings,
                cutoff=manifest.cutoff,
                coverage_start=manifest.coverage_start,
                coverage_end=manifest.coverage_end,
            )
            rebuilt_bytes = encode_calendar_materialization(rebuilt)
            if encoded != rebuilt_bytes:
                raise CalendarMaterializationStoreIntegrityError(
                    "stored materialization bytes differ from exact source replay"
                )
            self._validate_manifest_materialization(manifest, rebuilt, rebuilt_bytes)
        except CalendarMaterializationStoreIntegrityError:
            raise
        except (FileSafetyError, OSError, TypeError, ValueError, KeyError) as exc:
            raise CalendarMaterializationStoreIntegrityError(
                "materialization archive is incomplete or malformed"
            ) from exc
        return StoredCalendarMaterialization(
            path=path,
            manifest=manifest,
            materialization=rebuilt,
            encoded_bytes=encoded,
        )

    def _assert_safe_dataset_root(self) -> Path:
        configured_root = self.root.resolve()
        dataset_root = self.root / CALENDAR_MATERIALIZATION_STORE_DATASET
        if not dataset_root.exists():
            raise CalendarMaterializationStoreIntegrityError(
                "materialization dataset root does not exist"
            )
        if _is_link_like(dataset_root) or not dataset_root.is_dir():
            raise CalendarMaterializationStoreIntegrityError(
                "materialization dataset root cannot be a link"
            )
        expected = configured_root / CALENDAR_MATERIALIZATION_STORE_DATASET
        if dataset_root.resolve() != expected:
            raise CalendarMaterializationStoreIntegrityError(
                "materialization dataset root escapes configured root"
            )
        return expected

    def _ensure_safe_dataset_root(self) -> Path:
        self.root.mkdir(parents=True, exist_ok=True)
        dataset_root = self.root / CALENDAR_MATERIALIZATION_STORE_DATASET
        dataset_root.mkdir(exist_ok=True)
        return self._assert_safe_dataset_root()

    def _assert_internal_path(self, path: Path) -> None:
        dataset_root = self._assert_safe_dataset_root()
        resolved = path.resolve()
        if not resolved.is_relative_to(dataset_root):
            raise CalendarMaterializationStoreIntegrityError(
                "materialization path escapes configured dataset root"
            )
        current = path
        while current != self.root and current != current.parent:
            if current.exists() and _is_link_like(current):
                raise CalendarMaterializationStoreIntegrityError(
                    "materialization path contains a link or junction"
                )
            if current == self.root / CALENDAR_MATERIALIZATION_STORE_DATASET:
                break
            current = current.parent
