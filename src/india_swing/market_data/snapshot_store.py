from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from india_swing.identity import content_id

from .codec import (
    MARKET_PAYLOAD_CODEC_VERSION,
    MarketPayloadCodecError,
    MarketPayloadSecretError,
    decode_market_payload,
    encode_market_payload,
    market_payload_record_count,
)


SNAPSHOT_SCHEMA_VERSION = "market-snapshot-v2"
PAYLOAD_FILENAME = "payload.json"
_SAFE_COMPONENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_SNAPSHOT_ID = re.compile(r"[0-9a-f]{64}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class MarketSnapshotError(RuntimeError):
    pass


class MarketSnapshotIntegrityError(MarketSnapshotError):
    pass


class MarketSnapshotSecretError(MarketSnapshotError):
    pass


class MarketSnapshotNotFound(MarketSnapshotError):
    pass


class MarketSnapshotStale(MarketSnapshotError):
    pass


@dataclass(frozen=True, slots=True)
class MarketSnapshotManifest:
    schema_version: str
    codec_version: str
    snapshot_id: str
    dataset: str
    selection_key: str
    provider: str
    provider_version: str
    observed_at: datetime
    record_count: int
    payload_filename: str
    payload_sha256: str


@dataclass(frozen=True, slots=True)
class StoredMarketSnapshot:
    path: Path
    manifest: MarketSnapshotManifest
    normalized_payload: Any
    payload_bytes: bytes


def _safe_component(value: str, field_name: str) -> str:
    if not isinstance(value, str) or _SAFE_COMPONENT.fullmatch(value) is None:
        raise ValueError(f"{field_name} contains unsafe path characters")
    return value


def _selection_key(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("selection_key is required")
    if len(value) > 1024 or any(ord(character) < 32 for character in value):
        raise ValueError("selection_key contains invalid characters")
    return value


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _snapshot_identity(manifest: MarketSnapshotManifest) -> dict[str, object]:
    return {
        "schema_version": manifest.schema_version,
        "codec_version": manifest.codec_version,
        "dataset": manifest.dataset,
        "selection_key": manifest.selection_key,
        "provider": manifest.provider,
        "provider_version": manifest.provider_version,
        "observed_at": manifest.observed_at,
        "record_count": manifest.record_count,
        "payload_filename": manifest.payload_filename,
        "payload_sha256": manifest.payload_sha256,
    }


def _manifest_json(manifest: MarketSnapshotManifest) -> bytes:
    value = {
        "schema_version": manifest.schema_version,
        "codec_version": manifest.codec_version,
        "snapshot_id": manifest.snapshot_id,
        "dataset": manifest.dataset,
        "selection_key": manifest.selection_key,
        "provider": manifest.provider,
        "provider_version": manifest.provider_version,
        "observed_at": manifest.observed_at.isoformat(),
        "record_count": manifest.record_count,
        "payload_filename": manifest.payload_filename,
        "payload_sha256": manifest.payload_sha256,
    }
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


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


class LocalMarketSnapshotStore:
    """Create-once local snapshot store; production will use conditional GCS writes."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def put(
        self,
        *,
        dataset: str,
        selection_key: str,
        provider: str,
        provider_version: str,
        observed_at: datetime,
        normalized_payload: Any,
        raw_payload: bytes | None = None,
    ) -> StoredMarketSnapshot:
        """Persist only the typed, secret-rejecting adapter representation.

        Arbitrary raw bytes are intentionally disabled. A future wire archive
        must have a provider-specific schema and redaction contract first.
        """

        if raw_payload is not None:
            raise MarketSnapshotSecretError("arbitrary raw market payload storage is disabled")
        dataset = _safe_component(dataset, "dataset")
        selection_key = _selection_key(selection_key)
        provider = _safe_component(provider, "provider")
        if not isinstance(provider_version, str) or not provider_version.strip():
            raise ValueError("provider_version is required")
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        observed_at = observed_at.astimezone(timezone.utc)

        try:
            payload_bytes = encode_market_payload(normalized_payload)
            record_count = market_payload_record_count(normalized_payload)
        except MarketPayloadSecretError as exc:
            raise MarketSnapshotSecretError(str(exc)) from None
        except MarketPayloadCodecError as exc:
            raise MarketSnapshotIntegrityError(str(exc)) from None
        payload_hash = _sha256(payload_bytes)
        provisional = MarketSnapshotManifest(
            schema_version=SNAPSHOT_SCHEMA_VERSION,
            codec_version=MARKET_PAYLOAD_CODEC_VERSION,
            snapshot_id="0" * 64,
            dataset=dataset,
            selection_key=selection_key,
            provider=provider,
            provider_version=provider_version,
            observed_at=observed_at,
            record_count=record_count,
            payload_filename=PAYLOAD_FILENAME,
            payload_sha256=payload_hash,
        )
        snapshot_id = content_id(_snapshot_identity(provisional), length=64)
        manifest = MarketSnapshotManifest(
            schema_version=provisional.schema_version,
            codec_version=provisional.codec_version,
            snapshot_id=snapshot_id,
            dataset=provisional.dataset,
            selection_key=provisional.selection_key,
            provider=provisional.provider,
            provider_version=provisional.provider_version,
            observed_at=provisional.observed_at,
            record_count=provisional.record_count,
            payload_filename=provisional.payload_filename,
            payload_sha256=provisional.payload_sha256,
        )

        parent = self.root / dataset / observed_at.date().isoformat()
        target = parent / snapshot_id
        parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            return self._read_path(target, expected_dataset=dataset)

        temporary = Path(tempfile.mkdtemp(dir=parent, prefix=f".{snapshot_id}."))
        try:
            _write_fsynced(temporary / PAYLOAD_FILENAME, payload_bytes)
            _write_fsynced(temporary / "manifest.json", _manifest_json(manifest))
            self._read_path(temporary, expected_dataset=dataset)
            try:
                os.rename(temporary, target)
            except FileExistsError:
                return self._read_path(target, expected_dataset=dataset)
            _fsync_directory(parent)
        finally:
            if temporary.exists():
                resolved_parent = parent.resolve()
                resolved_temporary = temporary.resolve()
                if not resolved_temporary.is_relative_to(resolved_parent):
                    raise MarketSnapshotIntegrityError("unsafe temporary snapshot path")
                shutil.rmtree(temporary)
        return self._read_path(target, expected_dataset=dataset)

    def get(self, dataset: str, snapshot_id: str) -> StoredMarketSnapshot:
        dataset = _safe_component(dataset, "dataset")
        if _SNAPSHOT_ID.fullmatch(snapshot_id) is None:
            raise ValueError("snapshot_id must be a full SHA-256 identifier")
        base = self.root / dataset
        matches = list(base.glob(f"*/{snapshot_id}")) if base.exists() else []
        if not matches:
            raise MarketSnapshotNotFound(f"snapshot not found: {dataset}/{snapshot_id}")
        if len(matches) != 1:
            raise MarketSnapshotIntegrityError("snapshot ID appears in multiple vintages")
        return self._read_path(matches[0], expected_dataset=dataset)

    def find_by_selection_key(
        self,
        dataset: str,
        selection_key: str,
    ) -> tuple[StoredMarketSnapshot, ...]:
        """Return every verified snapshot for one exact semantic selection."""

        dataset = _safe_component(dataset, "dataset")
        selection_key = _selection_key(selection_key)
        base = self.root / dataset
        if not base.exists():
            return ()
        matches: list[StoredMarketSnapshot] = []
        for path in sorted(base.glob("*/*")):
            if path.is_dir() and not path.name.startswith("."):
                stored = self._read_path(path, expected_dataset=dataset)
                if stored.manifest.selection_key == selection_key:
                    matches.append(stored)
        return tuple(
            sorted(
                matches,
                key=lambda value: (
                    value.manifest.observed_at,
                    value.manifest.snapshot_id,
                ),
            )
        )

    def latest_at_or_before(
        self,
        dataset: str,
        selection_key: str,
        cutoff: datetime,
        *,
        max_age: timedelta,
    ) -> StoredMarketSnapshot:
        dataset = _safe_component(dataset, "dataset")
        selection_key = _selection_key(selection_key)
        if cutoff.tzinfo is None or cutoff.utcoffset() is None:
            raise ValueError("cutoff must be timezone-aware")
        if max_age <= timedelta(0):
            raise ValueError("max_age must be positive")
        cutoff = cutoff.astimezone(timezone.utc)
        base = self.root / dataset
        candidates: list[StoredMarketSnapshot] = []
        if base.exists():
            for path in base.glob("*/*"):
                if path.is_dir() and not path.name.startswith("."):
                    stored = self._read_path(path, expected_dataset=dataset)
                    if (
                        stored.manifest.selection_key == selection_key
                        and stored.manifest.observed_at <= cutoff
                    ):
                        candidates.append(stored)
        if not candidates:
            raise MarketSnapshotNotFound(
                f"no {dataset} snapshot for the selection key was available at the cutoff"
            )
        selected = max(
            candidates,
            key=lambda item: (item.manifest.observed_at, item.manifest.snapshot_id),
        )
        if cutoff - selected.manifest.observed_at > max_age:
            raise MarketSnapshotStale(
                f"latest {dataset} snapshot for the selection key exceeds max_age"
            )
        return selected

    @staticmethod
    def _read_path(
        path: Path,
        *,
        expected_dataset: str | None = None,
    ) -> StoredMarketSnapshot:
        try:
            if path.is_symlink() or not path.is_dir():
                raise MarketSnapshotIntegrityError("market snapshot path is not a real directory")
            entries = list(path.iterdir())
            if any(entry.is_symlink() for entry in entries):
                raise MarketSnapshotIntegrityError("market snapshot cannot contain symbolic links")
            if {entry.name for entry in entries} != {"manifest.json", PAYLOAD_FILENAME}:
                raise MarketSnapshotIntegrityError("market snapshot contains unexpected entries")
            if not all(entry.is_file() for entry in entries):
                raise MarketSnapshotIntegrityError("market snapshot entries must be files")
            manifest_value = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
            required_keys = {
                "schema_version",
                "codec_version",
                "snapshot_id",
                "dataset",
                "selection_key",
                "provider",
                "provider_version",
                "observed_at",
                "record_count",
                "payload_filename",
                "payload_sha256",
            }
            if not isinstance(manifest_value, dict) or set(manifest_value) != required_keys:
                raise MarketSnapshotIntegrityError("market snapshot manifest schema mismatch")
            record_count = manifest_value["record_count"]
            if type(record_count) is not int or record_count < 0:
                raise MarketSnapshotIntegrityError("invalid market snapshot record count")
            observed_at = datetime.fromisoformat(str(manifest_value["observed_at"]))
            manifest = MarketSnapshotManifest(
                schema_version=str(manifest_value["schema_version"]),
                codec_version=str(manifest_value["codec_version"]),
                snapshot_id=str(manifest_value["snapshot_id"]),
                dataset=str(manifest_value["dataset"]),
                selection_key=str(manifest_value["selection_key"]),
                provider=str(manifest_value["provider"]),
                provider_version=str(manifest_value["provider_version"]),
                observed_at=observed_at,
                record_count=record_count,
                payload_filename=str(manifest_value["payload_filename"]),
                payload_sha256=str(manifest_value["payload_sha256"]),
            )
            _safe_component(manifest.dataset, "manifest.dataset")
            _selection_key(manifest.selection_key)
            _safe_component(manifest.provider, "manifest.provider")
            if not manifest.provider_version.strip():
                raise MarketSnapshotIntegrityError("manifest provider_version is required")
            if manifest.schema_version != SNAPSHOT_SCHEMA_VERSION:
                raise MarketSnapshotIntegrityError("unsupported market snapshot schema")
            if manifest.codec_version != MARKET_PAYLOAD_CODEC_VERSION:
                raise MarketSnapshotIntegrityError("unsupported market payload codec")
            if manifest.payload_filename != PAYLOAD_FILENAME:
                raise MarketSnapshotIntegrityError("unexpected market snapshot payload filename")
            if _SNAPSHOT_ID.fullmatch(manifest.snapshot_id) is None:
                raise MarketSnapshotIntegrityError("invalid snapshot identifier")
            if _SHA256.fullmatch(manifest.payload_sha256) is None:
                raise MarketSnapshotIntegrityError("invalid market payload hash")
            if (
                manifest.observed_at.tzinfo is None
                or manifest.observed_at.utcoffset() != timedelta(0)
            ):
                raise MarketSnapshotIntegrityError("manifest observed_at must be UTC")
            if expected_dataset is not None and manifest.dataset != expected_dataset:
                raise MarketSnapshotIntegrityError("requested dataset and manifest disagree")
            if path.parent.parent.name != manifest.dataset:
                raise MarketSnapshotIntegrityError("snapshot dataset partition and manifest disagree")
            if path.parent.name != manifest.observed_at.date().isoformat():
                raise MarketSnapshotIntegrityError("snapshot date partition and manifest disagree")
            expected_snapshot_id = content_id(_snapshot_identity(manifest), length=64)
            if manifest.snapshot_id != expected_snapshot_id:
                raise MarketSnapshotIntegrityError("snapshot identifier does not match its manifest")
            if path.name != manifest.snapshot_id and not path.name.startswith(
                f".{manifest.snapshot_id}."
            ):
                raise MarketSnapshotIntegrityError("snapshot directory and manifest disagree")
            payload_bytes = (path / manifest.payload_filename).read_bytes()
            if _sha256(payload_bytes) != manifest.payload_sha256:
                raise MarketSnapshotIntegrityError("market snapshot payload hash mismatch")
            normalized_payload = decode_market_payload(payload_bytes)
            if market_payload_record_count(normalized_payload) != manifest.record_count:
                raise MarketSnapshotIntegrityError("market snapshot record count mismatch")
        except MarketSnapshotIntegrityError:
            raise
        except MarketPayloadCodecError as exc:
            raise MarketSnapshotIntegrityError("market snapshot payload is malformed") from exc
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise MarketSnapshotIntegrityError("market snapshot is incomplete or malformed") from exc
        return StoredMarketSnapshot(path, manifest, normalized_payload, payload_bytes)
