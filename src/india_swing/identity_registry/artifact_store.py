from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
from dataclasses import dataclass, fields
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore

from .codec import encode_identity_registry
from .materialize import materialize_cross_vintage_identity_registry
from .models import (
    IDENTITY_REGISTRY_CODEC_VERSION,
    IDENTITY_REGISTRY_DATASET,
    IDENTITY_REGISTRY_POLICY_VERSION,
    IDENTITY_REGISTRY_SCHEMA_VERSION,
    CrossVintageIdentityRegistry,
    IdentityRegistryError,
    IdentityRegistryIntegrityError,
)


IDENTITY_REGISTRY_STORE_SCHEMA_VERSION = "identity-registry-store/v1"
MANIFEST_FILENAME = "manifest.json"
PAYLOAD_FILENAME = "registry.json"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAX_MANIFEST_BYTES = 1024 * 1024
_MAX_PAYLOAD_BYTES = 512 * 1024 * 1024


class IdentityRegistryArtifactNotFound(IdentityRegistryError):
    pass


class IdentityRegistryStoreConflict(IdentityRegistryError):
    pass


@dataclass(frozen=True, slots=True)
class IdentityRegistryStoreManifest:
    schema_version: str
    manifest_id: str
    registry_id: str
    dataset: str
    registry_schema_version: str
    policy_version: str
    codec_version: str
    cutoff: datetime
    knowledge_time: datetime
    source_artifact_ids: tuple[str, ...]
    source_manifest_ids: tuple[str, ...]
    source_claimed_report_dates: tuple[date, ...]
    observation_count: int
    candidate_count: int
    transition_count: int
    conflict_count: int
    readiness: ReferenceReadiness
    actionable: bool
    payload_filename: str
    payload_sha256: str
    payload_byte_count: int

    def __post_init__(self) -> None:
        for value, name in (
            (self.manifest_id, "manifest_id"),
            (self.registry_id, "registry_id"),
            (self.payload_sha256, "payload_sha256"),
        ):
            if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
                raise ValueError(f"{name} must be a full lowercase SHA-256")
        if (
            self.schema_version != IDENTITY_REGISTRY_STORE_SCHEMA_VERSION
            or self.dataset != IDENTITY_REGISTRY_DATASET
            or self.registry_schema_version != IDENTITY_REGISTRY_SCHEMA_VERSION
            or self.policy_version != IDENTITY_REGISTRY_POLICY_VERSION
            or self.codec_version != IDENTITY_REGISTRY_CODEC_VERSION
        ):
            raise ValueError("unsupported identity-registry store contract")
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
        groups = (
            (self.source_artifact_ids, "source_artifact_ids"),
            (self.source_manifest_ids, "source_manifest_ids"),
        )
        for values, name in groups:
            if type(values) is not tuple or not values:
                raise ValueError(f"{name} must be a non-empty tuple")
            for value in values:
                if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
                    raise ValueError(f"{name} must contain full SHA-256 identifiers")
        if len(set(self.source_artifact_ids)) != len(self.source_artifact_ids):
            raise ValueError("source artifact IDs must be unique")
        if len(self.source_manifest_ids) != len(self.source_artifact_ids):
            raise ValueError("source manifest lineage length mismatch")
        if (
            type(self.source_claimed_report_dates) is not tuple
            or len(self.source_claimed_report_dates) != len(self.source_artifact_ids)
            or any(type(value) is not date for value in self.source_claimed_report_dates)
            or tuple(sorted(set(self.source_claimed_report_dates)))
            != self.source_claimed_report_dates
        ):
            raise ValueError("source claimed dates must be sorted and unique")
        for value, name, allow_zero in (
            (self.observation_count, "observation_count", False),
            (self.candidate_count, "candidate_count", False),
            (self.transition_count, "transition_count", True),
            (self.conflict_count, "conflict_count", True),
            (self.payload_byte_count, "payload_byte_count", False),
        ):
            if type(value) is not int or value < (0 if allow_zero else 1):
                raise ValueError(f"{name} has an invalid count")
        if self.readiness is not ReferenceReadiness.COLLECTION_ONLY or self.actionable is not False:
            raise ValueError("stored identity registry must remain collection-only")
        if self.payload_filename != PAYLOAD_FILENAME:
            raise ValueError("unexpected identity-registry payload filename")


@dataclass(frozen=True, slots=True)
class StoredIdentityRegistry:
    path: Path
    manifest: IdentityRegistryStoreManifest
    registry: CrossVintageIdentityRegistry
    payload_bytes: bytes


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _manifest_identity(manifest: IdentityRegistryStoreManifest) -> dict[str, object]:
    return {
        item.name: getattr(manifest, item.name)
        for item in fields(IdentityRegistryStoreManifest)
        if item.name != "manifest_id"
    }


def _manifest_bytes(manifest: IdentityRegistryStoreManifest) -> bytes:
    value = {
        item.name: (
            getattr(manifest, item.name).value
            if isinstance(getattr(manifest, item.name), ReferenceReadiness)
            else getattr(manifest, item.name).isoformat()
            if isinstance(getattr(manifest, item.name), (date, datetime))
            else [
                nested.isoformat() if isinstance(nested, date) else nested
                for nested in getattr(manifest, item.name)
            ]
            if isinstance(getattr(manifest, item.name), tuple)
            else getattr(manifest, item.name)
        )
        for item in fields(IdentityRegistryStoreManifest)
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


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise IdentityRegistryIntegrityError(
                "identity-registry manifest contains a duplicate JSON key"
            )
        result[key] = value
    return result


def _decode_manifest(payload: bytes) -> IdentityRegistryStoreManifest:
    try:
        value = json.loads(
            payload.decode("utf-8", errors="strict"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        expected = {item.name for item in fields(IdentityRegistryStoreManifest)}
        if type(value) is not dict or set(value) != expected:
            raise ValueError
        return IdentityRegistryStoreManifest(
            schema_version=value["schema_version"],
            manifest_id=value["manifest_id"],
            registry_id=value["registry_id"],
            dataset=value["dataset"],
            registry_schema_version=value["registry_schema_version"],
            policy_version=value["policy_version"],
            codec_version=value["codec_version"],
            cutoff=datetime.fromisoformat(value["cutoff"]),
            knowledge_time=datetime.fromisoformat(value["knowledge_time"]),
            source_artifact_ids=tuple(value["source_artifact_ids"]),
            source_manifest_ids=tuple(value["source_manifest_ids"]),
            source_claimed_report_dates=tuple(
                date.fromisoformat(item) for item in value["source_claimed_report_dates"]
            ),
            observation_count=value["observation_count"],
            candidate_count=value["candidate_count"],
            transition_count=value["transition_count"],
            conflict_count=value["conflict_count"],
            readiness=ReferenceReadiness(value["readiness"]),
            actionable=value["actionable"],
            payload_filename=value["payload_filename"],
            payload_sha256=value["payload_sha256"],
            payload_byte_count=value["payload_byte_count"],
        )
    except IdentityRegistryIntegrityError:
        raise
    except (KeyError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise IdentityRegistryIntegrityError(
            "identity-registry manifest is invalid"
        ) from exc


class LocalIdentityRegistryStore:
    """Create-once registry store whose reads replay every sealed master."""

    def __init__(self, root: Path, reference_data_root: Path) -> None:
        self.root = Path(root)
        self.reference_data_root = Path(reference_data_root)

    @property
    def dataset_root(self) -> Path:
        return self.root / IDENTITY_REGISTRY_DATASET

    def put(self, registry: CrossVintageIdentityRegistry) -> StoredIdentityRegistry:
        if type(registry) is not CrossVintageIdentityRegistry:
            raise TypeError("registry must be an exact CrossVintageIdentityRegistry")
        registry.verify_content_identity()
        sources = tuple(
            LocalReferenceArtifactStore(self.reference_data_root).get(value)
            for value in registry.source_artifact_ids
        )
        replayed = materialize_cross_vintage_identity_registry(
            sources=sources,
            cutoff=registry.cutoff,
        )
        if replayed != registry:
            raise IdentityRegistryIntegrityError(
                "identity registry does not replay from sealed source artifacts"
            )
        del replayed
        del sources
        payload = encode_identity_registry(registry)
        provisional = IdentityRegistryStoreManifest(
            schema_version=IDENTITY_REGISTRY_STORE_SCHEMA_VERSION,
            manifest_id="0" * 64,
            registry_id=registry.registry_id,
            dataset=IDENTITY_REGISTRY_DATASET,
            registry_schema_version=registry.schema_version,
            policy_version=registry.policy_version,
            codec_version=registry.codec_version,
            cutoff=registry.cutoff,
            knowledge_time=registry.knowledge_time,
            source_artifact_ids=registry.source_artifact_ids,
            source_manifest_ids=tuple(
                value.manifest_id for value in registry.source_manifests
            ),
            source_claimed_report_dates=tuple(
                value.claimed_report_date for value in registry.source_manifests
            ),
            observation_count=len(registry.observations),
            candidate_count=len(registry.candidates),
            transition_count=len(registry.transitions),
            conflict_count=len(registry.conflicts),
            readiness=registry.readiness,
            actionable=registry.actionable,
            payload_filename=PAYLOAD_FILENAME,
            payload_sha256=_sha256(payload),
            payload_byte_count=len(payload),
        )
        manifest = IdentityRegistryStoreManifest(
            **{
                item.name: (
                    content_id(_manifest_identity(provisional), length=64)
                    if item.name == "manifest_id"
                    else getattr(provisional, item.name)
                )
                for item in fields(IdentityRegistryStoreManifest)
            }
        )
        target = self.dataset_root / registry.registry_id
        self.dataset_root.mkdir(parents=True, exist_ok=True)
        if _is_link_like(self.dataset_root):
            raise IdentityRegistryIntegrityError("identity-registry root cannot be a link")
        lock = self.dataset_root / ".identity-registry.lock"
        try:
            with advisory_file_lock(lock):
                if target.exists():
                    existing = self._read_path(target)
                    if existing.registry != registry:
                        raise IdentityRegistryStoreConflict(
                            "registry ID already stores different content"
                        )
                    return existing
                temporary = Path(
                    tempfile.mkdtemp(prefix=".identity-registry-", dir=self.dataset_root)
                )
                try:
                    _write_fsynced(temporary / MANIFEST_FILENAME, _manifest_bytes(manifest))
                    _write_fsynced(temporary / PAYLOAD_FILENAME, payload)
                    os.replace(temporary, target)
                except Exception:
                    shutil.rmtree(temporary, ignore_errors=True)
                    raise
        except (FileLockUnavailable, FileSafetyError) as exc:
            raise IdentityRegistryStoreConflict(
                "identity-registry store is currently unavailable"
            ) from exc
        return self._read_path(
            target,
            expected_created=(manifest, registry, payload),
        )

    def get(self, registry_id: str) -> StoredIdentityRegistry:
        if not isinstance(registry_id, str) or _SHA256.fullmatch(registry_id) is None:
            raise ValueError("registry_id must be a full lowercase SHA-256")
        target = self.dataset_root / registry_id
        if not target.exists():
            raise IdentityRegistryArtifactNotFound(
                f"identity registry not found: {registry_id}"
            )
        return self._read_path(target)

    def _read_path(
        self,
        path: Path,
        *,
        expected_created: tuple[
            IdentityRegistryStoreManifest,
            CrossVintageIdentityRegistry,
            bytes,
        ]
        | None = None,
    ) -> StoredIdentityRegistry:
        try:
            if not path.is_dir() or _is_link_like(path):
                raise IdentityRegistryIntegrityError(
                    "identity-registry artifact path must be a regular directory"
                )
            children = tuple(path.iterdir())
            if {value.name for value in children} != {
                MANIFEST_FILENAME,
                PAYLOAD_FILENAME,
            } or any(_is_link_like(value) or not value.is_file() for value in children):
                raise IdentityRegistryIntegrityError(
                    "identity-registry artifact file set is invalid"
                )
            manifest_payload = read_stable_regular_file(
                path / MANIFEST_FILENAME,
                maximum_bytes=_MAX_MANIFEST_BYTES,
            )
            payload = read_stable_regular_file(
                path / PAYLOAD_FILENAME,
                maximum_bytes=_MAX_PAYLOAD_BYTES,
            )
        except FileSafetyError as exc:
            raise IdentityRegistryIntegrityError(
                "identity-registry artifact could not be read safely"
            ) from exc
        manifest = _decode_manifest(manifest_payload)
        if path.name != manifest.registry_id:
            raise IdentityRegistryIntegrityError("registry path identity mismatch")
        if manifest.manifest_id != content_id(_manifest_identity(manifest), length=64):
            raise IdentityRegistryIntegrityError("registry manifest identity mismatch")
        if (
            len(payload) != manifest.payload_byte_count
            or _sha256(payload) != manifest.payload_sha256
        ):
            raise IdentityRegistryIntegrityError("registry payload integrity mismatch")
        if expected_created is not None:
            expected_manifest, expected_registry, expected_payload = (
                expected_created
            )
            expected_registry.verify_content_identity()
            if (
                manifest != expected_manifest
                or payload != expected_payload
                or expected_registry.registry_id != manifest.registry_id
                or expected_registry.knowledge_time != manifest.knowledge_time
                or len(expected_registry.observations)
                != manifest.observation_count
                or len(expected_registry.candidates)
                != manifest.candidate_count
                or len(expected_registry.transitions)
                != manifest.transition_count
                or len(expected_registry.conflicts)
                != manifest.conflict_count
            ):
                raise IdentityRegistryIntegrityError(
                    "newly stored registry disagrees with verified content"
                )
            return StoredIdentityRegistry(
                path=path,
                manifest=manifest,
                registry=expected_registry,
                payload_bytes=payload,
            )

        source_store = LocalReferenceArtifactStore(self.reference_data_root)
        sources = tuple(source_store.get(value) for value in manifest.source_artifact_ids)
        if tuple(value.manifest.manifest_id for value in sources) != manifest.source_manifest_ids:
            raise IdentityRegistryIntegrityError("registry source manifest lineage mismatch")
        if tuple(value.manifest.claimed_report_date for value in sources) != (
            manifest.source_claimed_report_dates
        ):
            raise IdentityRegistryIntegrityError("registry source claimed-date lineage mismatch")
        replayed = materialize_cross_vintage_identity_registry(
            sources=sources,
            cutoff=manifest.cutoff,
        )
        replayed_payload = encode_identity_registry(replayed)
        if (
            replayed.registry_id != manifest.registry_id
            or replayed.knowledge_time != manifest.knowledge_time
            or len(replayed.observations) != manifest.observation_count
            or len(replayed.candidates) != manifest.candidate_count
            or len(replayed.transitions) != manifest.transition_count
            or len(replayed.conflicts) != manifest.conflict_count
            or replayed_payload != payload
        ):
            raise IdentityRegistryIntegrityError(
                "stored registry does not replay from sealed masters"
            )
        return StoredIdentityRegistry(
            path=path,
            manifest=manifest,
            registry=replayed,
            payload_bytes=payload,
        )
