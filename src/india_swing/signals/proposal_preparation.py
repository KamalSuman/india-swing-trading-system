from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.identity import content_id
from india_swing.reference.calendar import CalendarSnapshot
from india_swing.reference.models import ReferenceReadiness

from .deterministic_swing import DeterministicSwingSignalConfig
from .proposal_artifacts import LocalSwingProposalBatchStore, SwingProposalBatchManifest
from .proposal_batch import assemble_swing_proposal_batch
from .proposal_parent_store import (
    LocalSwingProposalParentStore,
    SwingProposalParentStoreError,
    publish_swing_proposal_with_parents,
)
from .universe_batch import SwingUniverseInputBatch


PROPOSAL_PREPARATION_SCHEMA = "swing-proposal-preparation/v1"
PROPOSAL_PREPARATION_CODEC = "swing-proposal-preparation-json/v1"
SUBJECT_BINDING_SCHEMA = "swing-proposal-subject-binding/v1"
_MAXIMUM_BYTES = 8 * 1024 * 1024
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class SwingProposalPreparationError(ValueError):
    pass


class SwingProposalPreparationConflict(SwingProposalPreparationError):
    pass


class SwingProposalPreparationNotFound(SwingProposalPreparationError):
    pass


def _sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise SwingProposalPreparationError(f"{name} must be a lowercase SHA-256")
    return value


def _utc(value: object, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise SwingProposalPreparationError(f"{name} must be timezone-aware")
    try:
        offset = value.utcoffset()
    except Exception:
        raise SwingProposalPreparationError(f"{name} has invalid timezone behavior") from None
    if offset is None:
        raise SwingProposalPreparationError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


@dataclass(frozen=True, slots=True)
class SwingProposalSubjectBinding:
    stable_instrument_id: str
    stable_listing_id: str
    assembly_id: str
    promotion_decision_id: str
    schema_version: str = SUBJECT_BINDING_SCHEMA
    binding_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.stable_instrument_id, "stable_instrument_id"),
            (self.stable_listing_id, "stable_listing_id"),
        ):
            if type(value) is not str or not value.strip() or value != value.strip():
                raise SwingProposalPreparationError(f"{name} is invalid")
        _sha(self.assembly_id, "assembly_id")
        _sha(self.promotion_decision_id, "promotion_decision_id")
        if self.schema_version != SUBJECT_BINDING_SCHEMA:
            raise SwingProposalPreparationError("subject binding schema is unsupported")
        object.__setattr__(self, "binding_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "binding_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.binding_id != self._calculated_id():
            raise SwingProposalPreparationError("subject binding identity failed")


@dataclass(frozen=True, slots=True)
class SwingProposalPreparationSpec:
    universe_batch_id: str
    calendar_snapshot_id: str
    signal_config_id: str
    expected_proposal_batch_id: str
    signal_session: date
    cutoff: datetime
    readiness: ReferenceReadiness
    subject_bindings: tuple[SwingProposalSubjectBinding, ...]
    scoped_subject_count: int
    proposal_subject_count: int
    veto_subject_count: int
    research_only: bool
    mode: str = "PAPER_ONLY"
    schema_version: str = PROPOSAL_PREPARATION_SCHEMA
    preparation_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.universe_batch_id, "universe_batch_id"),
            (self.calendar_snapshot_id, "calendar_snapshot_id"),
            (self.signal_config_id, "signal_config_id"),
            (self.expected_proposal_batch_id, "expected_proposal_batch_id"),
        ):
            _sha(value, name)
        if type(self.signal_session) is not date:
            raise SwingProposalPreparationError("signal_session must be an exact date")
        object.__setattr__(self, "cutoff", _utc(self.cutoff, "cutoff"))
        if type(self.readiness) is not ReferenceReadiness:
            raise SwingProposalPreparationError("readiness must be exact")
        if self.readiness is ReferenceReadiness.COLLECTION_ONLY:
            raise SwingProposalPreparationError(
                "collection-only inputs cannot enter proposal preparation"
            )
        if (
            type(self.subject_bindings) is not tuple
            or any(type(value) is not SwingProposalSubjectBinding for value in self.subject_bindings)
        ):
            raise SwingProposalPreparationError("subject bindings must be an exact tuple")
        for value in self.subject_bindings:
            value.verify_content_identity()
        keys = tuple(
            (value.stable_instrument_id, value.stable_listing_id)
            for value in self.subject_bindings
        )
        if keys != tuple(sorted(set(keys))):
            raise SwingProposalPreparationError("subject bindings must be unique and ordered")
        for value in (
            self.scoped_subject_count,
            self.proposal_subject_count,
            self.veto_subject_count,
        ):
            if type(value) is not int or type(value) is bool or value < 0:
                raise SwingProposalPreparationError("proposal subject counts are invalid")
        if (
            self.proposal_subject_count != len(self.subject_bindings)
            or self.scoped_subject_count
            != self.proposal_subject_count + self.veto_subject_count
        ):
            raise SwingProposalPreparationError("proposal subject counts are inconsistent")
        if type(self.research_only) is not bool:
            raise SwingProposalPreparationError("research_only must be bool")
        if self.research_only != (
            self.readiness is not ReferenceReadiness.POINT_IN_TIME_VERIFIED
        ):
            raise SwingProposalPreparationError("proposal authority differs from readiness")
        if self.mode != "PAPER_ONLY" or self.schema_version != PROPOSAL_PREPARATION_SCHEMA:
            raise SwingProposalPreparationError("proposal preparation authority is invalid")
        object.__setattr__(self, "preparation_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "preparation_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        for value in self.subject_bindings:
            value.verify_content_identity()
        if self.preparation_id != self._calculated_id():
            raise SwingProposalPreparationError("proposal preparation identity failed")


def _bindings(batch: SwingUniverseInputBatch) -> tuple[SwingProposalSubjectBinding, ...]:
    return tuple(
        SwingProposalSubjectBinding(
            stable_instrument_id=value.stable_instrument_id,
            stable_listing_id=value.stable_listing_id,
            assembly_id=value.assembly_id,
            promotion_decision_id=value.promotion_decision_id,
        )
        for value in batch.assemblies
    )


def build_swing_proposal_preparation_spec(
    *,
    universe_batch: SwingUniverseInputBatch,
    calendar: CalendarSnapshot,
    signal_config: DeterministicSwingSignalConfig,
) -> SwingProposalPreparationSpec:
    if type(universe_batch) is not SwingUniverseInputBatch:
        raise SwingProposalPreparationError("universe batch must be exact")
    if type(calendar) is not CalendarSnapshot:
        raise SwingProposalPreparationError("calendar snapshot must be exact")
    if type(signal_config) is not DeterministicSwingSignalConfig:
        raise SwingProposalPreparationError("signal config must be exact")
    try:
        universe_batch.verify_content_identity()
        calendar.verify_content_identity()
        signal_config.verify_content_identity()
        proposal = assemble_swing_proposal_batch(
            universe_batch=universe_batch,
            calendar=calendar,
            config=signal_config,
        )
    except Exception:
        raise SwingProposalPreparationError(
            "promoted proposal inputs could not be replayed safely"
        ) from None
    return SwingProposalPreparationSpec(
        universe_batch_id=universe_batch.batch_id,
        calendar_snapshot_id=calendar.snapshot_id,
        signal_config_id=signal_config.config_id,
        expected_proposal_batch_id=proposal.batch_id,
        signal_session=universe_batch.signal_session,
        cutoff=universe_batch.cutoff,
        readiness=universe_batch.readiness,
        subject_bindings=_bindings(universe_batch),
        scoped_subject_count=proposal.scoped_subject_count,
        proposal_subject_count=proposal.proposal_subject_count,
        veto_subject_count=proposal.veto_subject_count,
        research_only=proposal.research_only,
    )


def prepare_stored_swing_proposal_graph(
    *,
    spec: SwingProposalPreparationSpec,
    parent_store: LocalSwingProposalParentStore,
    proposal_store: LocalSwingProposalBatchStore,
    preparation_store: "LocalSwingProposalPreparationStore",
) -> SwingProposalBatchManifest:
    if type(spec) is not SwingProposalPreparationSpec:
        raise SwingProposalPreparationError("proposal preparation spec must be exact")
    if type(parent_store) is not LocalSwingProposalParentStore:
        raise SwingProposalPreparationError("proposal parent store must be exact")
    if type(proposal_store) is not LocalSwingProposalBatchStore:
        raise SwingProposalPreparationError("proposal store must be exact")
    if type(preparation_store) is not LocalSwingProposalPreparationStore:
        raise SwingProposalPreparationError("preparation store must be exact")
    spec.verify_content_identity()
    try:
        universe_batch = parent_store.get_universe_batch(spec.universe_batch_id)
        calendar = parent_store.get_calendar_snapshot(spec.calendar_snapshot_id)
        signal_config = parent_store.get_signal_config(spec.signal_config_id)
        replayed_spec = build_swing_proposal_preparation_spec(
            universe_batch=universe_batch,
            calendar=calendar,
            signal_config=signal_config,
        )
        if replayed_spec != spec:
            raise SwingProposalPreparationError(
                "stored promoted inputs differ from the preparation spec"
            )
        proposal = assemble_swing_proposal_batch(
            universe_batch=universe_batch,
            calendar=calendar,
            config=signal_config,
        )
        stored_spec = preparation_store.put(spec)
        if stored_spec != spec:
            raise SwingProposalPreparationError("stored preparation differs")
        manifest = publish_swing_proposal_with_parents(
            batch=proposal,
            proposal_store=proposal_store,
            parent_store=parent_store,
        )
        rebuilt = proposal_store.load(manifest.proposal_batch_id, parent_store)
        if (
            manifest.proposal_batch_id != spec.expected_proposal_batch_id
            or rebuilt != proposal
        ):
            raise SwingProposalPreparationError("published proposal graph differs")
        return manifest
    except SwingProposalPreparationError:
        raise
    except SwingProposalParentStoreError:
        raise SwingProposalPreparationError(
            "stored promoted inputs could not be loaded safely"
        ) from None
    except Exception:
        raise SwingProposalPreparationError(
            "proposal graph preparation failed safely"
        ) from None


def _binding_data(value: SwingProposalSubjectBinding) -> dict[str, object]:
    value.verify_content_identity()
    return {
        "assembly_id": value.assembly_id,
        "binding_id": value.binding_id,
        "promotion_decision_id": value.promotion_decision_id,
        "schema_version": value.schema_version,
        "stable_instrument_id": value.stable_instrument_id,
        "stable_listing_id": value.stable_listing_id,
    }


def encode_swing_proposal_preparation_spec(value: SwingProposalPreparationSpec) -> bytes:
    if type(value) is not SwingProposalPreparationSpec:
        raise SwingProposalPreparationError("proposal preparation spec must be exact")
    value.verify_content_identity()
    payload = (
        json.dumps(
            {
                "codec_schema_version": PROPOSAL_PREPARATION_CODEC,
                "spec": {
                    "calendar_snapshot_id": value.calendar_snapshot_id,
                    "cutoff": value.cutoff.isoformat(),
                    "expected_proposal_batch_id": value.expected_proposal_batch_id,
                    "mode": value.mode,
                    "preparation_id": value.preparation_id,
                    "proposal_subject_count": value.proposal_subject_count,
                    "readiness": value.readiness.value,
                    "research_only": value.research_only,
                    "schema_version": value.schema_version,
                    "scoped_subject_count": value.scoped_subject_count,
                    "signal_config_id": value.signal_config_id,
                    "signal_session": value.signal_session.isoformat(),
                    "subject_bindings": [_binding_data(item) for item in value.subject_bindings],
                    "universe_batch_id": value.universe_batch_id,
                    "veto_subject_count": value.veto_subject_count,
                },
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(payload) > _MAXIMUM_BYTES:
        raise SwingProposalPreparationError("proposal preparation spec is too large")
    return payload


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise SwingProposalPreparationError("proposal preparation has duplicate keys")
        result[key] = value
    return result


def decode_swing_proposal_preparation_spec(payload: bytes) -> SwingProposalPreparationSpec:
    try:
        root = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if type(root) is not dict or set(root) != {"codec_schema_version", "spec"}:
            raise ValueError
        if root["codec_schema_version"] != PROPOSAL_PREPARATION_CODEC:
            raise ValueError
        raw = root["spec"]
        expected = {
            "calendar_snapshot_id", "cutoff", "expected_proposal_batch_id", "mode",
            "preparation_id", "proposal_subject_count", "readiness", "research_only",
            "schema_version", "scoped_subject_count", "signal_config_id", "signal_session",
            "subject_bindings", "universe_batch_id", "veto_subject_count",
        }
        if type(raw) is not dict or set(raw) != expected or type(raw["subject_bindings"]) is not list:
            raise ValueError
        bindings = []
        binding_fields = {
            "assembly_id", "binding_id", "promotion_decision_id", "schema_version",
            "stable_instrument_id", "stable_listing_id",
        }
        for item in raw["subject_bindings"]:
            if type(item) is not dict or set(item) != binding_fields:
                raise ValueError
            binding = SwingProposalSubjectBinding(
                stable_instrument_id=item["stable_instrument_id"],
                stable_listing_id=item["stable_listing_id"],
                assembly_id=item["assembly_id"],
                promotion_decision_id=item["promotion_decision_id"],
                schema_version=item["schema_version"],
            )
            if binding.binding_id != item["binding_id"]:
                raise ValueError
            bindings.append(binding)
        value = SwingProposalPreparationSpec(
            universe_batch_id=raw["universe_batch_id"],
            calendar_snapshot_id=raw["calendar_snapshot_id"],
            signal_config_id=raw["signal_config_id"],
            expected_proposal_batch_id=raw["expected_proposal_batch_id"],
            signal_session=date.fromisoformat(raw["signal_session"]),
            cutoff=datetime.fromisoformat(raw["cutoff"]),
            readiness=ReferenceReadiness(raw["readiness"]),
            subject_bindings=tuple(bindings),
            scoped_subject_count=raw["scoped_subject_count"],
            proposal_subject_count=raw["proposal_subject_count"],
            veto_subject_count=raw["veto_subject_count"],
            research_only=raw["research_only"],
            mode=raw["mode"],
            schema_version=raw["schema_version"],
        )
        if (
            value.preparation_id != raw["preparation_id"]
            or encode_swing_proposal_preparation_spec(value) != payload
        ):
            raise ValueError
        return value
    except SwingProposalPreparationError:
        raise
    except Exception:
        raise SwingProposalPreparationError("proposal preparation spec is invalid") from None


def load_swing_proposal_preparation_spec_file(path: Path) -> SwingProposalPreparationSpec:
    if type(path) is not type(Path()):
        raise SwingProposalPreparationError("proposal preparation path is invalid")
    try:
        return decode_swing_proposal_preparation_spec(
            read_stable_regular_file(path, maximum_bytes=_MAXIMUM_BYTES)
        )
    except SwingProposalPreparationError:
        raise
    except Exception:
        raise SwingProposalPreparationError(
            "proposal preparation file is unavailable"
        ) from None


class LocalSwingProposalPreparationStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def specifications_root(self) -> Path:
        return self.root / "proposal_preparations"

    def path_for(self, preparation_id: str) -> Path:
        _sha(preparation_id, "preparation_id")
        return self.specifications_root / f"{preparation_id}.json"

    def get(self, preparation_id: str) -> SwingProposalPreparationSpec:
        path = self.path_for(preparation_id)
        try:
            safe = (
                self.root.is_dir()
                and not _is_link_like(self.root)
                and self.specifications_root.is_dir()
                and not _is_link_like(self.specifications_root)
                and path.is_file()
                and not _is_link_like(path)
            )
        except OSError:
            safe = False
        if not safe:
            raise SwingProposalPreparationNotFound("proposal preparation was not found safely")
        try:
            value = decode_swing_proposal_preparation_spec(
                read_stable_regular_file(path, maximum_bytes=_MAXIMUM_BYTES)
            )
        except SwingProposalPreparationError:
            raise
        except Exception:
            raise SwingProposalPreparationError("stored proposal preparation is invalid") from None
        if value.preparation_id != preparation_id:
            raise SwingProposalPreparationError("stored proposal preparation differs from its path")
        return value

    def put(self, value: SwingProposalPreparationSpec) -> SwingProposalPreparationSpec:
        if type(value) is not SwingProposalPreparationSpec:
            raise SwingProposalPreparationError("proposal preparation must be exact")
        payload = encode_swing_proposal_preparation_spec(value)
        try:
            self.root.mkdir(parents=True, exist_ok=True)
            self.specifications_root.mkdir(exist_ok=True)
            if _is_link_like(self.root) or _is_link_like(self.specifications_root):
                raise SwingProposalPreparationError("proposal preparation store is unsafe")
            target = self.path_for(value.preparation_id)
            with advisory_file_lock(self.root / ".proposal-preparations.lock"):
                if target.exists():
                    stored = self.get(value.preparation_id)
                    if stored != value:
                        raise SwingProposalPreparationConflict(
                            "proposal preparation ID already stores different content"
                        )
                    return stored
                descriptor, name = tempfile.mkstemp(
                    prefix=".proposal-preparation-", suffix=".tmp", dir=self.specifications_root
                )
                temporary = Path(name)
                try:
                    with os.fdopen(descriptor, "wb") as handle:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.link(temporary, target)
                finally:
                    temporary.unlink(missing_ok=True)
        except SwingProposalPreparationError:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise SwingProposalPreparationConflict(
                "proposal preparation store is unavailable"
            ) from None
        return self.get(value.preparation_id)
