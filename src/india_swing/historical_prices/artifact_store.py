from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, fields
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.daily_reports.artifact_store import LocalDailyBundleArtifactStore
from india_swing.daily_reports.models import (
    DailyReportConflict,
    DailyReportIntegrityError,
    DailyReportNotFound,
)
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness

from .codec import encode_historical_price_artifact
from .materialize import materialize_nse_eod_session
from .models import (
    HISTORICAL_PRICE_CODEC_VERSION,
    HISTORICAL_PRICE_POLICY_VERSION,
    HISTORICAL_PRICE_SCHEMA_VERSION,
    RAW_UNADJUSTED,
    TRADED_ROWS_ONLY,
    HistoricalPriceError,
    HistoricalPriceIntegrityError,
    NseEodSessionArtifact,
)


HISTORICAL_PRICE_DATASET = "nse-cm-raw-eod-sessions"
HISTORICAL_PRICE_STORE_SCHEMA_VERSION = "historical-price-artifact-store/v1"
MANIFEST_FILENAME = "manifest.json"
ARTIFACT_FILENAME = "artifact.json"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_MANIFEST_BYTES = 1024 * 1024
_MAX_ARTIFACT_BYTES = 512 * 1024 * 1024


class HistoricalPriceArtifactNotFound(HistoricalPriceError):
    pass


class HistoricalPriceStoreConflict(HistoricalPriceError):
    pass


@dataclass(frozen=True, slots=True)
class HistoricalPriceStoreManifest:
    schema_version: str
    manifest_id: str
    artifact_id: str
    dataset: str
    artifact_schema_version: str
    policy_version: str
    codec_version: str
    market_session: date
    cutoff: datetime
    knowledge_time: datetime
    source_bundle_artifact_id: str
    source_bundle_manifest_id: str
    source_bundle_raw_sha256: str
    source_bundle_normalized_sha256: str
    bar_count: int
    udiff_row_count: int
    full_delivery_row_count: int
    price_basis: str
    coverage_scope: str
    readiness: ReferenceReadiness
    actionable: bool
    payload_filename: str
    payload_sha256: str
    payload_byte_count: int

    def __post_init__(self) -> None:
        for value, name in (
            (self.manifest_id, "manifest_id"),
            (self.artifact_id, "artifact_id"),
            (self.source_bundle_artifact_id, "source_bundle_artifact_id"),
            (self.source_bundle_manifest_id, "source_bundle_manifest_id"),
            (self.source_bundle_raw_sha256, "source_bundle_raw_sha256"),
            (
                self.source_bundle_normalized_sha256,
                "source_bundle_normalized_sha256",
            ),
            (self.payload_sha256, "payload_sha256"),
        ):
            if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
                raise ValueError(f"{name} must be a full lowercase SHA-256")
        if (
            self.schema_version != HISTORICAL_PRICE_STORE_SCHEMA_VERSION
            or self.dataset != HISTORICAL_PRICE_DATASET
            or self.artifact_schema_version != HISTORICAL_PRICE_SCHEMA_VERSION
            or self.policy_version != HISTORICAL_PRICE_POLICY_VERSION
            or self.codec_version != HISTORICAL_PRICE_CODEC_VERSION
        ):
            raise ValueError("unsupported historical-price store contract")
        if type(self.market_session) is not date:
            raise TypeError("market_session must be a date")
        for name in ("cutoff", "knowledge_time"):
            value = getattr(self, name)
            if (
                not isinstance(value, datetime)
                or value.tzinfo is None
                or value.utcoffset() != timedelta(0)
            ):
                raise ValueError(f"{name} must use UTC")
            object.__setattr__(self, name, value.astimezone(timezone.utc))
        if self.knowledge_time > self.cutoff:
            raise ValueError("knowledge_time cannot follow cutoff")
        for value, name in (
            (self.bar_count, "bar_count"),
            (self.udiff_row_count, "udiff_row_count"),
            (self.full_delivery_row_count, "full_delivery_row_count"),
            (self.payload_byte_count, "payload_byte_count"),
        ):
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.bar_count != self.udiff_row_count:
            raise ValueError("every UDiFF row must own one stored bar")
        if self.full_delivery_row_count > self.bar_count:
            raise ValueError("full-delivery rows cannot exceed UDiFF rows")
        if self.price_basis != RAW_UNADJUSTED or self.coverage_scope != TRADED_ROWS_ONLY:
            raise ValueError("stored prices must remain raw traded-row evidence")
        if self.readiness is not ReferenceReadiness.COLLECTION_ONLY or self.actionable is not False:
            raise ValueError("stored historical prices must remain collection-only")
        if self.payload_filename != ARTIFACT_FILENAME:
            raise ValueError("unexpected historical-price payload filename")


@dataclass(frozen=True, slots=True)
class StoredHistoricalPriceArtifact:
    path: Path
    manifest: HistoricalPriceStoreManifest
    artifact: NseEodSessionArtifact
    payload_bytes: bytes


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _manifest_identity(manifest: HistoricalPriceStoreManifest) -> dict[str, object]:
    return {
        item.name: getattr(manifest, item.name)
        for item in fields(HistoricalPriceStoreManifest)
        if item.name != "manifest_id"
    }


def _manifest_json(manifest: HistoricalPriceStoreManifest) -> bytes:
    value = {
        item.name: (
            getattr(manifest, item.name).value
            if isinstance(getattr(manifest, item.name), ReferenceReadiness)
            else getattr(manifest, item.name).isoformat()
            if isinstance(getattr(manifest, item.name), (date, datetime))
            else getattr(manifest, item.name)
        )
        for item in fields(HistoricalPriceStoreManifest)
    }
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    reparse_attribute = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0) & reparse_attribute
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


class LocalHistoricalPriceArtifactStore:
    """Create-once derived store whose reads replay sealed source evidence."""

    def __init__(self, root: Path, daily_reports_root: Path) -> None:
        self.root = Path(root)
        self.daily_reports_root = Path(daily_reports_root)

    @property
    def daily_store(self) -> LocalDailyBundleArtifactStore:
        return LocalDailyBundleArtifactStore(self.daily_reports_root)

    def put(self, artifact: NseEodSessionArtifact) -> StoredHistoricalPriceArtifact:
        if type(artifact) is not NseEodSessionArtifact:
            raise TypeError("artifact must be an exact NseEodSessionArtifact")
        artifact.verify_content_identity()
        source = self._load_exact_source(artifact)
        expected = materialize_nse_eod_session(
            source,
            market_session=artifact.market_session,
            cutoff=artifact.cutoff,
        )
        payload = encode_historical_price_artifact(artifact)
        if (
            expected.artifact_id != artifact.artifact_id
            or encode_historical_price_artifact(expected) != payload
        ):
            raise HistoricalPriceIntegrityError(
                "historical-price artifact does not replay from sealed source"
            )
        manifest = self._build_manifest(artifact, payload)
        existing = self._existing_artifact(artifact.artifact_id)
        if existing is not None:
            return existing
        with self._artifact_lock(artifact.artifact_id):
            existing = self._existing_artifact(artifact.artifact_id)
            if existing is not None:
                return existing
            return self._publish(manifest, payload)

    def get(self, artifact_id: str) -> StoredHistoricalPriceArtifact:
        if not isinstance(artifact_id, str) or _SHA256.fullmatch(artifact_id) is None:
            raise ValueError("artifact_id must be a full SHA-256 identifier")
        matches = [path for path in self._artifact_paths() if path.name == artifact_id]
        if not matches:
            raise HistoricalPriceArtifactNotFound(
                f"historical-price artifact not found: {artifact_id}"
            )
        if len(matches) != 1:
            raise HistoricalPriceIntegrityError(
                "historical-price artifact ID appears in multiple partitions"
            )
        return self._read_path(matches[0])

    def _existing_artifact(
        self,
        artifact_id: str,
    ) -> StoredHistoricalPriceArtifact | None:
        matches = [path for path in self._artifact_paths() if path.name == artifact_id]
        if len(matches) > 1:
            raise HistoricalPriceIntegrityError(
                "historical-price artifact ID appears in multiple partitions"
            )
        return self._read_path(matches[0]) if matches else None

    @staticmethod
    def _build_manifest(
        artifact: NseEodSessionArtifact,
        payload: bytes,
    ) -> HistoricalPriceStoreManifest:
        source = artifact.source_bundle_manifest
        provisional = HistoricalPriceStoreManifest(
            schema_version=HISTORICAL_PRICE_STORE_SCHEMA_VERSION,
            manifest_id="0" * 64,
            artifact_id=artifact.artifact_id,
            dataset=HISTORICAL_PRICE_DATASET,
            artifact_schema_version=artifact.schema_version,
            policy_version=artifact.policy_version,
            codec_version=HISTORICAL_PRICE_CODEC_VERSION,
            market_session=artifact.market_session,
            cutoff=artifact.cutoff,
            knowledge_time=artifact.knowledge_time,
            source_bundle_artifact_id=source.artifact_id,
            source_bundle_manifest_id=source.manifest_id,
            source_bundle_raw_sha256=source.raw_sha256,
            source_bundle_normalized_sha256=source.normalized_sha256,
            bar_count=len(artifact.bars),
            udiff_row_count=artifact.report_refs[0].row_count,
            full_delivery_row_count=artifact.report_refs[1].row_count,
            price_basis=artifact.price_basis,
            coverage_scope=artifact.coverage_scope,
            readiness=artifact.readiness,
            actionable=artifact.actionable,
            payload_filename=ARTIFACT_FILENAME,
            payload_sha256=_sha256(payload),
            payload_byte_count=len(payload),
        )
        manifest_id = content_id(_manifest_identity(provisional), length=64)
        return HistoricalPriceStoreManifest(
            **{
                item.name: (
                    manifest_id
                    if item.name == "manifest_id"
                    else getattr(provisional, item.name)
                )
                for item in fields(HistoricalPriceStoreManifest)
            }
        )

    def _load_exact_source(self, artifact: NseEodSessionArtifact):
        try:
            source = self.daily_store.get(
                artifact.source_bundle_manifest.artifact_id
            )
        except (DailyReportConflict, DailyReportNotFound, DailyReportIntegrityError) as exc:
            raise HistoricalPriceIntegrityError(
                "embedded daily-bundle source is unavailable or invalid"
            ) from exc
        if source.manifest != artifact.source_bundle_manifest:
            raise HistoricalPriceIntegrityError(
                "embedded source manifest disagrees with sealed daily bundle"
            )
        return source

    def _load_manifest_source(self, manifest: HistoricalPriceStoreManifest):
        try:
            source = self.daily_store.get(manifest.source_bundle_artifact_id)
        except (DailyReportConflict, DailyReportNotFound, DailyReportIntegrityError) as exc:
            raise HistoricalPriceIntegrityError(
                "stored daily-bundle source is unavailable or invalid"
            ) from exc
        source_manifest = source.manifest
        if (
            source_manifest.manifest_id != manifest.source_bundle_manifest_id
            or source_manifest.raw_sha256 != manifest.source_bundle_raw_sha256
            or source_manifest.normalized_sha256
            != manifest.source_bundle_normalized_sha256
        ):
            raise HistoricalPriceIntegrityError(
                "stored source lineage disagrees with sealed daily bundle"
            )
        return source

    def _artifact_paths(self) -> list[Path]:
        base = self.root / HISTORICAL_PRICE_DATASET
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
        lock_root = self.root / HISTORICAL_PRICE_DATASET / ".locks"
        if not lock_root.exists():
            try:
                lock_root.mkdir()
            except FileExistsError:
                pass
        self._assert_internal_path(lock_root)
        lock_path = lock_root / f".{artifact_id}.advisory-lock"
        try:
            with advisory_file_lock(lock_path):
                yield
        except FileLockUnavailable as exc:
            raise HistoricalPriceStoreConflict(
                "another process is publishing this historical-price artifact"
            ) from exc
        except FileSafetyError as exc:
            raise HistoricalPriceIntegrityError(
                "historical-price publication lock is unsafe"
            ) from exc

    def _publish(
        self,
        manifest: HistoricalPriceStoreManifest,
        payload: bytes,
    ) -> StoredHistoricalPriceArtifact:
        parent = self.root / HISTORICAL_PRICE_DATASET / manifest.market_session.isoformat()
        target = parent / manifest.artifact_id
        parent.mkdir(parents=True, exist_ok=True)
        self._assert_internal_path(parent)
        if target.exists():
            return self._read_path(target)
        temporary = Path(
            tempfile.mkdtemp(dir=parent, prefix=f".{manifest.artifact_id}.")
        )
        try:
            _write_fsynced(temporary / ARTIFACT_FILENAME, payload)
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
                    raise HistoricalPriceIntegrityError(
                        "unsafe temporary historical-price path"
                    )
                shutil.rmtree(temporary)
        return self._read_path(target)

    def _read_path(self, path: Path) -> StoredHistoricalPriceArtifact:
        try:
            self._assert_internal_path(path)
            if _is_link_like(path) or not path.is_dir():
                raise HistoricalPriceIntegrityError(
                    "historical-price artifact path is not a real directory"
                )
            entries = tuple(path.iterdir())
            if any(_is_link_like(entry) or not entry.is_file() for entry in entries):
                raise HistoricalPriceIntegrityError(
                    "historical-price artifact entries must be regular files"
                )
            if {entry.name for entry in entries} != {
                MANIFEST_FILENAME,
                ARTIFACT_FILENAME,
            }:
                raise HistoricalPriceIntegrityError(
                    "historical-price artifact contains unexpected entries"
                )
            manifest_bytes = read_stable_regular_file(
                path / MANIFEST_FILENAME,
                maximum_bytes=_MAX_MANIFEST_BYTES,
            )
            payload_bytes = read_stable_regular_file(
                path / ARTIFACT_FILENAME,
                maximum_bytes=_MAX_ARTIFACT_BYTES,
            )
            value = json.loads(manifest_bytes)
            expected_keys = {item.name for item in fields(HistoricalPriceStoreManifest)}
            if not isinstance(value, dict) or set(value) != expected_keys:
                raise HistoricalPriceIntegrityError(
                    "historical-price manifest schema mismatch"
                )
            manifest = HistoricalPriceStoreManifest(
                schema_version=str(value["schema_version"]),
                manifest_id=str(value["manifest_id"]),
                artifact_id=str(value["artifact_id"]),
                dataset=str(value["dataset"]),
                artifact_schema_version=str(value["artifact_schema_version"]),
                policy_version=str(value["policy_version"]),
                codec_version=str(value["codec_version"]),
                market_session=date.fromisoformat(str(value["market_session"])),
                cutoff=datetime.fromisoformat(str(value["cutoff"])),
                knowledge_time=datetime.fromisoformat(str(value["knowledge_time"])),
                source_bundle_artifact_id=str(value["source_bundle_artifact_id"]),
                source_bundle_manifest_id=str(value["source_bundle_manifest_id"]),
                source_bundle_raw_sha256=str(value["source_bundle_raw_sha256"]),
                source_bundle_normalized_sha256=str(
                    value["source_bundle_normalized_sha256"]
                ),
                bar_count=value["bar_count"],
                udiff_row_count=value["udiff_row_count"],
                full_delivery_row_count=value["full_delivery_row_count"],
                price_basis=str(value["price_basis"]),
                coverage_scope=str(value["coverage_scope"]),
                readiness=ReferenceReadiness(value["readiness"]),
                actionable=value["actionable"],
                payload_filename=str(value["payload_filename"]),
                payload_sha256=str(value["payload_sha256"]),
                payload_byte_count=value["payload_byte_count"],
            )
            if manifest_bytes != _manifest_json(manifest):
                raise HistoricalPriceIntegrityError(
                    "historical-price manifest is not canonically encoded"
                )
            if path.parent.parent.name != manifest.dataset:
                raise HistoricalPriceIntegrityError("historical-price dataset mismatch")
            if path.parent.name != manifest.market_session.isoformat():
                raise HistoricalPriceIntegrityError("historical-price session partition mismatch")
            if path.name != manifest.artifact_id and not path.name.startswith(
                f".{manifest.artifact_id}."
            ):
                raise HistoricalPriceIntegrityError(
                    "historical-price directory and manifest disagree"
                )
            if _sha256(payload_bytes) != manifest.payload_sha256:
                raise HistoricalPriceIntegrityError("historical-price payload hash mismatch")
            if len(payload_bytes) != manifest.payload_byte_count:
                raise HistoricalPriceIntegrityError("historical-price payload size mismatch")
            if content_id(_manifest_identity(manifest), length=64) != manifest.manifest_id:
                raise HistoricalPriceIntegrityError("historical-price manifest ID mismatch")

            payload_value = json.loads(payload_bytes)
            if not isinstance(payload_value, dict):
                raise HistoricalPriceIntegrityError("historical-price payload is not an object")
            embedded_source = payload_value.get("source_bundle_manifest")
            if (
                not isinstance(embedded_source, dict)
                or embedded_source.get("artifact_id")
                != manifest.source_bundle_artifact_id
                or embedded_source.get("manifest_id")
                != manifest.source_bundle_manifest_id
                or embedded_source.get("raw_sha256")
                != manifest.source_bundle_raw_sha256
                or embedded_source.get("normalized_sha256")
                != manifest.source_bundle_normalized_sha256
            ):
                raise HistoricalPriceIntegrityError(
                    "embedded source manifest disagrees with store manifest"
                )
            source = self._load_manifest_source(manifest)
            artifact = materialize_nse_eod_session(
                source,
                market_session=manifest.market_session,
                cutoff=manifest.cutoff,
            )
            replay_bytes = encode_historical_price_artifact(artifact)
            if (
                artifact.artifact_id != manifest.artifact_id
                or replay_bytes != payload_bytes
                or len(artifact.bars) != manifest.bar_count
                or artifact.report_refs[0].row_count != manifest.udiff_row_count
                or artifact.report_refs[1].row_count
                != manifest.full_delivery_row_count
                or artifact.knowledge_time != manifest.knowledge_time
            ):
                raise HistoricalPriceIntegrityError(
                    "historical-price payload does not replay from sealed source"
                )
        except HistoricalPriceIntegrityError:
            raise
        except (
            FileSafetyError,
            OSError,
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as exc:
            raise HistoricalPriceIntegrityError(
                "historical-price artifact is incomplete or malformed"
            ) from exc
        return StoredHistoricalPriceArtifact(path, manifest, artifact, payload_bytes)

    def _assert_safe_dataset_root(self) -> Path:
        if _is_link_like(self.root):
            raise HistoricalPriceIntegrityError(
                "historical-price configured root cannot be a link or junction"
            )
        configured_root = self.root.resolve()
        dataset_root = self.root / HISTORICAL_PRICE_DATASET
        if not dataset_root.exists() or _is_link_like(dataset_root):
            raise HistoricalPriceIntegrityError(
                "historical-price dataset root is unavailable or unsafe"
            )
        expected = configured_root / HISTORICAL_PRICE_DATASET
        if dataset_root.resolve() != expected:
            raise HistoricalPriceIntegrityError(
                "historical-price dataset root escapes configured root"
            )
        return expected

    def _ensure_safe_dataset_root(self) -> Path:
        self.root.mkdir(parents=True, exist_ok=True)
        dataset_root = self.root / HISTORICAL_PRICE_DATASET
        if not dataset_root.exists():
            try:
                dataset_root.mkdir()
            except FileExistsError:
                pass
        return self._assert_safe_dataset_root()

    def _assert_internal_path(self, path: Path) -> None:
        expected_root = self._assert_safe_dataset_root()
        if not path.resolve().is_relative_to(expected_root):
            raise HistoricalPriceIntegrityError(
                "historical-price path escapes configured dataset root"
            )
        current = path
        while current != self.root and current != current.parent:
            if current.exists() and _is_link_like(current):
                raise HistoricalPriceIntegrityError(
                    "historical-price path contains a link or junction"
                )
            if current == self.root / HISTORICAL_PRICE_DATASET:
                break
            current = current.parent
