from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Protocol

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.identity import content_id
from india_swing.market_data.models import MAXIMUM_AGGREGATED_QUOTE_KEYS
from india_swing.reference.calendar import CalendarSnapshot

from .deterministic_swing import DeterministicSwingSignalConfig
from .proposal_batch import SwingProposalBatch, assemble_swing_proposal_batch
from .universe_batch import SwingUniverseInputBatch


PROPOSAL_ARTIFACT_SCHEMA_VERSION = "swing-proposal-artifact-manifest/v1"
PROPOSAL_ARTIFACT_CODEC_VERSION = "swing-proposal-artifact-json/v1"
MAXIMUM_PROPOSAL_ARTIFACT_BYTES = 4 * 1024 * 1024

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class SwingProposalArtifactError(ValueError):
    pass


class SwingProposalArtifactNotFound(SwingProposalArtifactError):
    pass


def _sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise SwingProposalArtifactError(f"{name} must be a lowercase SHA-256")
    return value


def _utc(value: object, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise SwingProposalArtifactError(f"{name} must be timezone-aware")
    try:
        offset = value.utcoffset()
    except Exception:
        raise SwingProposalArtifactError(f"{name} has invalid timezone behavior") from None
    if offset is None:
        raise SwingProposalArtifactError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _id_tuple(value: object, name: str) -> tuple[str, ...]:
    if type(value) is not tuple:
        raise SwingProposalArtifactError(f"{name} must be an exact tuple")
    for item in value:
        _sha(item, name)
    if len(value) != len(set(value)):
        raise SwingProposalArtifactError(f"{name} must be unique")
    if len(value) > MAXIMUM_AGGREGATED_QUOTE_KEYS:
        raise SwingProposalArtifactError(f"{name} exceeds the operational universe limit")
    return value


@dataclass(frozen=True, slots=True)
class SwingProposalBatchManifest:
    """Small replay manifest for one exact deterministic proposal batch.

    The manifest deliberately stores identities rather than a second copy of
    the historical-price graph. A load succeeds only after exact typed inputs
    are resolved, independently verified, replayed, and compared with every
    identity and count recorded here.
    """

    proposal_batch_id: str
    universe_batch_id: str
    universe_snapshot_id: str
    calendar_snapshot_id: str
    signal_config_id: str
    signal_session: date
    cutoff: datetime
    assembly_ids: tuple[str, ...]
    proposal_ids: tuple[str, ...]
    veto_ids: tuple[str, ...]
    scoped_subject_count: int
    proposal_subject_count: int
    veto_subject_count: int
    schema_version: str = PROPOSAL_ARTIFACT_SCHEMA_VERSION
    manifest_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.proposal_batch_id, "proposal_batch_id"),
            (self.universe_batch_id, "universe_batch_id"),
            (self.universe_snapshot_id, "universe_snapshot_id"),
            (self.calendar_snapshot_id, "calendar_snapshot_id"),
            (self.signal_config_id, "signal_config_id"),
        ):
            _sha(value, name)
        if type(self.signal_session) is not date:
            raise SwingProposalArtifactError("signal_session must be an exact date")
        object.__setattr__(self, "cutoff", _utc(self.cutoff, "cutoff"))
        _id_tuple(self.assembly_ids, "assembly_ids")
        _id_tuple(self.proposal_ids, "proposal_ids")
        _id_tuple(self.veto_ids, "veto_ids")
        for value in (
            self.scoped_subject_count,
            self.proposal_subject_count,
            self.veto_subject_count,
        ):
            if type(value) is not int or value < 0:
                raise SwingProposalArtifactError("subject counts must be non-negative integers")
        if (
            self.proposal_subject_count != len(self.proposal_ids)
            or self.proposal_subject_count != len(self.assembly_ids)
            or self.veto_subject_count != len(self.veto_ids)
            or self.scoped_subject_count
            != self.proposal_subject_count + self.veto_subject_count
            or self.scoped_subject_count > MAXIMUM_AGGREGATED_QUOTE_KEYS
        ):
            raise SwingProposalArtifactError("proposal manifest coverage is inconsistent")
        if self.schema_version != PROPOSAL_ARTIFACT_SCHEMA_VERSION:
            raise SwingProposalArtifactError("unsupported proposal artifact schema")
        object.__setattr__(self, "manifest_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "manifest_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        try:
            fresh = SwingProposalBatchManifest(
                **{
                    item.name: getattr(self, item.name)
                    for item in fields(self)
                    if item.name != "manifest_id"
                }
            )
        except Exception:
            raise SwingProposalArtifactError(
                "proposal manifest content identity failed"
            ) from None
        if self.manifest_id != fresh.manifest_id:
            raise SwingProposalArtifactError("proposal manifest content identity failed")


def manifest_from_proposal_batch(batch: SwingProposalBatch) -> SwingProposalBatchManifest:
    if type(batch) is not SwingProposalBatch:
        raise SwingProposalArtifactError("proposal batch must be exact")
    try:
        batch.verify_content_identity()
    except Exception:
        raise SwingProposalArtifactError("proposal batch content is invalid") from None
    universe_batch = batch.universe_batch
    return SwingProposalBatchManifest(
        proposal_batch_id=batch.batch_id,
        universe_batch_id=universe_batch.batch_id,
        universe_snapshot_id=universe_batch.universe_snapshot_id,
        calendar_snapshot_id=batch.calendar.snapshot_id,
        signal_config_id=batch.config.config_id,
        signal_session=universe_batch.signal_session,
        cutoff=universe_batch.cutoff,
        assembly_ids=tuple(value.assembly_id for value in universe_batch.assemblies),
        proposal_ids=tuple(value.proposal_id for value in batch.proposals),
        veto_ids=tuple(value.veto_id for value in batch.vetoes),
        scoped_subject_count=batch.scoped_subject_count,
        proposal_subject_count=batch.proposal_subject_count,
        veto_subject_count=batch.veto_subject_count,
    )


def _manifest_data(value: SwingProposalBatchManifest) -> dict[str, object]:
    value.verify_content_identity()
    return {
        "assembly_ids": list(value.assembly_ids),
        "calendar_snapshot_id": value.calendar_snapshot_id,
        "cutoff": value.cutoff.isoformat(),
        "manifest_id": value.manifest_id,
        "proposal_batch_id": value.proposal_batch_id,
        "proposal_ids": list(value.proposal_ids),
        "proposal_subject_count": value.proposal_subject_count,
        "schema_version": value.schema_version,
        "scoped_subject_count": value.scoped_subject_count,
        "signal_config_id": value.signal_config_id,
        "signal_session": value.signal_session.isoformat(),
        "universe_batch_id": value.universe_batch_id,
        "universe_snapshot_id": value.universe_snapshot_id,
        "veto_ids": list(value.veto_ids),
        "veto_subject_count": value.veto_subject_count,
    }


def encode_swing_proposal_manifest(value: SwingProposalBatchManifest) -> bytes:
    if type(value) is not SwingProposalBatchManifest:
        raise SwingProposalArtifactError("proposal manifest must be exact")
    payload = (
        json.dumps(
            {
                "codec_schema_version": PROPOSAL_ARTIFACT_CODEC_VERSION,
                "manifest": _manifest_data(value),
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(payload) > MAXIMUM_PROPOSAL_ARTIFACT_BYTES:
        raise SwingProposalArtifactError("proposal manifest exceeds the size limit")
    return payload


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise SwingProposalArtifactError("proposal manifest contains duplicate keys")
        result[key] = value
    return result


_MANIFEST_FIELDS = {
    "assembly_ids",
    "calendar_snapshot_id",
    "cutoff",
    "manifest_id",
    "proposal_batch_id",
    "proposal_ids",
    "proposal_subject_count",
    "schema_version",
    "scoped_subject_count",
    "signal_config_id",
    "signal_session",
    "universe_batch_id",
    "universe_snapshot_id",
    "veto_ids",
    "veto_subject_count",
}


def _strict_date(value: object) -> date:
    if type(value) is not str:
        raise ValueError
    result = date.fromisoformat(value)
    if result.isoformat() != value:
        raise ValueError
    return result


def _strict_datetime(value: object) -> datetime:
    if type(value) is not str:
        raise ValueError
    result = datetime.fromisoformat(value)
    if (
        result.tzinfo is None
        or result.isoformat() != value
        or result.astimezone(timezone.utc).isoformat() != value
    ):
        raise ValueError
    return result


def _string_tuple(value: object) -> tuple[str, ...]:
    if type(value) is not list or any(type(item) is not str for item in value):
        raise ValueError
    return tuple(value)


def decode_swing_proposal_manifest(payload: bytes) -> SwingProposalBatchManifest:
    if type(payload) is not bytes or not payload or len(payload) > MAXIMUM_PROPOSAL_ARTIFACT_BYTES:
        raise SwingProposalArtifactError("stored proposal manifest is invalid")
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if type(raw) is not dict or set(raw) != {"codec_schema_version", "manifest"}:
            raise ValueError
        if raw["codec_schema_version"] != PROPOSAL_ARTIFACT_CODEC_VERSION:
            raise ValueError
        value = raw["manifest"]
        if type(value) is not dict or set(value) != _MANIFEST_FIELDS:
            raise ValueError
        stored_manifest_id = _sha(value["manifest_id"], "manifest_id")
        manifest = SwingProposalBatchManifest(
            proposal_batch_id=value["proposal_batch_id"],
            universe_batch_id=value["universe_batch_id"],
            universe_snapshot_id=value["universe_snapshot_id"],
            calendar_snapshot_id=value["calendar_snapshot_id"],
            signal_config_id=value["signal_config_id"],
            signal_session=_strict_date(value["signal_session"]),
            cutoff=_strict_datetime(value["cutoff"]),
            assembly_ids=_string_tuple(value["assembly_ids"]),
            proposal_ids=_string_tuple(value["proposal_ids"]),
            veto_ids=_string_tuple(value["veto_ids"]),
            scoped_subject_count=value["scoped_subject_count"],
            proposal_subject_count=value["proposal_subject_count"],
            veto_subject_count=value["veto_subject_count"],
            schema_version=value["schema_version"],
        )
        if manifest.manifest_id != stored_manifest_id:
            raise SwingProposalArtifactError("stored proposal manifest identity differs")
        return manifest
    except SwingProposalArtifactError:
        raise
    except (KeyError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        raise SwingProposalArtifactError("stored proposal manifest is invalid") from None


class SwingProposalBatchInputResolver(Protocol):
    """Resolve exact typed parents; implementations must never select latest."""

    def get_universe_batch(self, universe_batch_id: str) -> SwingUniverseInputBatch: ...

    def get_calendar_snapshot(self, calendar_snapshot_id: str) -> CalendarSnapshot: ...

    def get_signal_config(self, signal_config_id: str) -> DeterministicSwingSignalConfig: ...


@dataclass(frozen=True, slots=True)
class FixedSwingProposalBatchInputResolver:
    """In-memory resolver for a single pinned graph, useful for composition and tests."""

    universe_batch: SwingUniverseInputBatch
    calendar: CalendarSnapshot
    config: DeterministicSwingSignalConfig

    def __post_init__(self) -> None:
        if type(self.universe_batch) is not SwingUniverseInputBatch:
            raise SwingProposalArtifactError("universe batch must be exact")
        if type(self.calendar) is not CalendarSnapshot:
            raise SwingProposalArtifactError("calendar snapshot must be exact")
        if type(self.config) is not DeterministicSwingSignalConfig:
            raise SwingProposalArtifactError("signal config must be exact")
        self.universe_batch.verify_content_identity()
        self.calendar.verify_content_identity()
        self.config.verify_content_identity()

    def get_universe_batch(self, universe_batch_id: str) -> SwingUniverseInputBatch:
        if universe_batch_id != self.universe_batch.batch_id:
            raise SwingProposalArtifactNotFound("universe batch is unavailable")
        return self.universe_batch

    def get_calendar_snapshot(self, calendar_snapshot_id: str) -> CalendarSnapshot:
        if calendar_snapshot_id != self.calendar.snapshot_id:
            raise SwingProposalArtifactNotFound("calendar snapshot is unavailable")
        return self.calendar

    def get_signal_config(self, signal_config_id: str) -> DeterministicSwingSignalConfig:
        if signal_config_id != self.config.config_id:
            raise SwingProposalArtifactNotFound("signal config is unavailable")
        return self.config


def replay_swing_proposal_batch(
    manifest: SwingProposalBatchManifest,
    resolver: SwingProposalBatchInputResolver,
) -> SwingProposalBatch:
    if type(manifest) is not SwingProposalBatchManifest:
        raise SwingProposalArtifactError("proposal manifest must be exact")
    manifest.verify_content_identity()
    try:
        universe_batch = resolver.get_universe_batch(manifest.universe_batch_id)
        calendar = resolver.get_calendar_snapshot(manifest.calendar_snapshot_id)
        config = resolver.get_signal_config(manifest.signal_config_id)
        if type(universe_batch) is not SwingUniverseInputBatch:
            raise TypeError
        if type(calendar) is not CalendarSnapshot:
            raise TypeError
        if type(config) is not DeterministicSwingSignalConfig:
            raise TypeError
        universe_batch.verify_content_identity()
        calendar.verify_content_identity()
        config.verify_content_identity()
    except Exception:
        raise SwingProposalArtifactError("proposal inputs could not be resolved safely") from None
    if (
        universe_batch.batch_id != manifest.universe_batch_id
        or universe_batch.universe_snapshot_id != manifest.universe_snapshot_id
        or universe_batch.signal_session != manifest.signal_session
        or universe_batch.cutoff != manifest.cutoff
        or calendar.snapshot_id != manifest.calendar_snapshot_id
        or config.config_id != manifest.signal_config_id
    ):
        raise SwingProposalArtifactError("resolved proposal lineage differs from the manifest")
    try:
        rebuilt = assemble_swing_proposal_batch(
            universe_batch=universe_batch,
            calendar=calendar,
            config=config,
        )
        rebuilt.verify_content_identity()
    except Exception:
        raise SwingProposalArtifactError("proposal batch could not be replayed safely") from None
    if (
        rebuilt.batch_id != manifest.proposal_batch_id
        or tuple(value.assembly_id for value in rebuilt.universe_batch.assemblies)
        != manifest.assembly_ids
        or tuple(value.proposal_id for value in rebuilt.proposals) != manifest.proposal_ids
        or tuple(value.veto_id for value in rebuilt.vetoes) != manifest.veto_ids
        or rebuilt.scoped_subject_count != manifest.scoped_subject_count
        or rebuilt.proposal_subject_count != manifest.proposal_subject_count
        or rebuilt.veto_subject_count != manifest.veto_subject_count
    ):
        raise SwingProposalArtifactError("replayed proposal batch differs from the manifest")
    return rebuilt


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


class LocalSwingProposalBatchStore:
    """Create-once replay manifests addressed only by exact proposal-batch ID."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def manifests_root(self) -> Path:
        return self.root / "proposal_batches"

    def path_for(self, proposal_batch_id: str) -> Path:
        try:
            value = _sha(proposal_batch_id, "proposal_batch_id")
        except SwingProposalArtifactError:
            raise SwingProposalArtifactError("proposal_batch_id is invalid") from None
        return self.manifests_root / f"{value}.json"

    def publish(self, batch: SwingProposalBatch) -> SwingProposalBatchManifest:
        manifest = manifest_from_proposal_batch(batch)
        payload = encode_swing_proposal_manifest(manifest)
        try:
            self.root.mkdir(parents=True, exist_ok=True)
        except OSError:
            raise SwingProposalArtifactError("proposal store is unavailable") from None
        if _is_link_like(self.root):
            raise SwingProposalArtifactError("proposal store path cannot be a link")
        try:
            self.manifests_root.mkdir(exist_ok=True)
        except OSError:
            raise SwingProposalArtifactError("proposal store is unavailable") from None
        if _is_link_like(self.manifests_root):
            raise SwingProposalArtifactError("proposal store path cannot be a link")
        target = self.path_for(manifest.proposal_batch_id)
        try:
            with advisory_file_lock(self.manifests_root / ".proposal-artifacts.lock"):
                if target.exists():
                    stored = self.get_manifest(manifest.proposal_batch_id)
                    if stored != manifest:
                        raise SwingProposalArtifactError(
                            "proposal batch ID already stores different lineage"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".proposal-artifact-",
                    suffix=".tmp",
                    dir=self.manifests_root,
                )
                temporary = Path(temporary_name)
                try:
                    with os.fdopen(descriptor, "wb") as handle:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.link(temporary, target)
                finally:
                    temporary.unlink(missing_ok=True)
        except SwingProposalArtifactError:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise SwingProposalArtifactError("proposal manifest could not be published") from None
        return self.get_manifest(manifest.proposal_batch_id)

    def get_manifest(self, proposal_batch_id: str) -> SwingProposalBatchManifest:
        path = self.path_for(proposal_batch_id)
        try:
            exists = path.exists()
        except OSError:
            raise SwingProposalArtifactError("proposal store is unavailable") from None
        if not exists:
            raise SwingProposalArtifactNotFound("proposal manifest was not found")
        try:
            payload = read_stable_regular_file(
                path,
                maximum_bytes=MAXIMUM_PROPOSAL_ARTIFACT_BYTES,
            )
            manifest = decode_swing_proposal_manifest(payload)
        except SwingProposalArtifactError:
            raise
        except FileSafetyError:
            raise SwingProposalArtifactError(
                "proposal manifest could not be read safely"
            ) from None
        if manifest.proposal_batch_id != proposal_batch_id:
            raise SwingProposalArtifactError("proposal manifest path differs from its content")
        return manifest

    def load(
        self,
        proposal_batch_id: str,
        resolver: SwingProposalBatchInputResolver,
    ) -> SwingProposalBatch:
        return replay_swing_proposal_batch(
            self.get_manifest(proposal_batch_id),
            resolver,
        )

    def require_persisted(
        self,
        batch: SwingProposalBatch,
        resolver: SwingProposalBatchInputResolver,
    ) -> SwingProposalBatch:
        expected = manifest_from_proposal_batch(batch)
        stored = self.get_manifest(expected.proposal_batch_id)
        if stored != expected:
            raise SwingProposalArtifactError("persisted proposal manifest differs")
        rebuilt = replay_swing_proposal_batch(stored, resolver)
        if rebuilt != batch:
            raise SwingProposalArtifactError("persisted proposal batch differs")
        return rebuilt

    def list_manifests(self) -> tuple[SwingProposalBatchManifest, ...]:
        try:
            exists = self.manifests_root.exists()
        except OSError:
            raise SwingProposalArtifactError("proposal store is unavailable") from None
        if not exists:
            return ()
        if not self.manifests_root.is_dir() or _is_link_like(self.manifests_root):
            raise SwingProposalArtifactError("proposal manifest file set is invalid")
        manifests: list[SwingProposalBatchManifest] = []
        try:
            paths = tuple(self.manifests_root.iterdir())
        except OSError:
            raise SwingProposalArtifactError("proposal store is unavailable") from None
        for path in paths:
            if path.name == ".proposal-artifacts.lock":
                continue
            if (
                path.suffix != ".json"
                or _SHA256.fullmatch(path.stem) is None
                or not path.is_file()
                or _is_link_like(path)
            ):
                raise SwingProposalArtifactError("proposal manifest file set is invalid")
            manifests.append(self.get_manifest(path.stem))
        return tuple(
            sorted(
                manifests,
                key=lambda value: (
                    value.signal_session,
                    value.proposal_batch_id,
                ),
            )
        )
