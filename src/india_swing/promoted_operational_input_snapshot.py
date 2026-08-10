"""Pure, deterministic input-snapshot foundation for placing the promoted
paper engine inside an ephemeral Cloud Run container.

Defines the exact immutable models, deterministic local-filesystem
inventory construction, canonical JSON codec, destination-path mapping,
and post-hydration re-verification for exactly the mandatory assembly-spec
file plus the eleven read-only input roots already bound by
``PromotedOperationalCloudRunControl``. ``state_root`` is categorically
excluded from every model, scan, and codec in this module -- it is never
read, represented, or referenced here.

This module never touches GCS, the network, credentials, the clock, or the
environment; every capability here is a pure local filesystem read plus
in-memory canonical encoding/decoding. GCS publication, acquisition, and
local hydration writes live in the separate ``promoted_operational_input_gcs``
module, which composes -- but never duplicates -- the primitives defined
here.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from india_swing._filesystem import FileSafetyError, read_stable_regular_file
from india_swing.promoted_operational_cloud_control import PromotedOperationalCloudRunControl


class PromotedOperationalInputSnapshotError(ValueError):
    pass


_ERR = "promoted operational input snapshot call is invalid"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_CONCRETE_PATH_TYPE = type(Path())

MAXIMUM_INCLUDED_FILES = 100_000
MAXIMUM_FILE_BYTES = 256 * 1024 * 1024
MAXIMUM_TOTAL_BYTES = 2 * 1024 * 1024 * 1024
MAXIMUM_ENCODED_BYTES = 16 * 1024 * 1024

_SCHEMA_VERSION = 1

FILE_INPUT_NAME = "assembly_spec_file"
ROOT_INPUT_NAMES: tuple[str, ...] = (
    "reference_root",
    "identity_evidence_root",
    "calendar_root",
    "daily_reports_root",
    "historical_corpus_root",
    "promoted_root",
    "graph_publication_root",
    "engine_run_root",
    "research_run_root",
    "operational_preparation_root",
    "portfolio_artifact_root",
)
INPUT_NAMES: tuple[str, ...] = (FILE_INPUT_NAME,) + ROOT_INPUT_NAMES

_SEGMENT_PATTERN_CHARS = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-"
)
_SEGMENT_LEADING_CHARS = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
)
_MAXIMUM_SEGMENT_LENGTH = 128
_MAXIMUM_RELATIVE_PATH_LENGTH = 512

_TOP_LEVEL_KEYS = frozenset(
    {
        "schema_version",
        "expected_assembly_spec_id",
        "target_session",
        "input_names",
        "entries",
        "entry_count",
        "total_bytes",
        "inventory_id",
    }
)
_ENTRY_KEYS = frozenset({"input_name", "relative_path", "byte_count", "sha256"})


def _validate_relative_path(input_name: str, value: str) -> None:
    if input_name == FILE_INPUT_NAME:
        if value != "":
            raise PromotedOperationalInputSnapshotError(_ERR)
        return
    if not value or len(value) > _MAXIMUM_RELATIVE_PATH_LENGTH:
        raise PromotedOperationalInputSnapshotError(_ERR)
    for segment in value.split("/"):
        if not segment or len(segment) > _MAXIMUM_SEGMENT_LENGTH:
            raise PromotedOperationalInputSnapshotError(_ERR)
        if segment[0] not in _SEGMENT_LEADING_CHARS:
            raise PromotedOperationalInputSnapshotError(_ERR)
        if not _SEGMENT_PATTERN_CHARS.issuperset(segment):
            raise PromotedOperationalInputSnapshotError(_ERR)


def _validate_sha256(value: object) -> None:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise PromotedOperationalInputSnapshotError(_ERR)


@dataclass(frozen=True, slots=True)
class PromotedOperationalInputEntry:
    """One exact included file: either the mandatory assembly-spec file
    (``input_name == FILE_INPUT_NAME``, ``relative_path == ""``) or one
    regular file under one of the eleven fixed read-only roots."""

    input_name: str
    relative_path: str
    byte_count: int
    sha256: str

    def __post_init__(self) -> None:
        if type(self.input_name) is not str or self.input_name not in INPUT_NAMES:
            raise PromotedOperationalInputSnapshotError(_ERR)
        if type(self.relative_path) is not str:
            raise PromotedOperationalInputSnapshotError(_ERR)
        _validate_relative_path(self.input_name, self.relative_path)
        if (
            type(self.byte_count) is not int
            or self.byte_count <= 0
            or self.byte_count > MAXIMUM_FILE_BYTES
        ):
            raise PromotedOperationalInputSnapshotError(_ERR)
        _validate_sha256(self.sha256)


def _reconstructed_entry(value: object) -> PromotedOperationalInputEntry:
    if type(value) is not PromotedOperationalInputEntry:
        raise PromotedOperationalInputSnapshotError(_ERR)
    return PromotedOperationalInputEntry(
        input_name=value.input_name,
        relative_path=value.relative_path,
        byte_count=value.byte_count,
        sha256=value.sha256,
    )


def _validated_entries(value: object) -> tuple[PromotedOperationalInputEntry, ...]:
    if type(value) is not tuple:
        raise PromotedOperationalInputSnapshotError(_ERR)
    reconstructed = tuple(_reconstructed_entry(item) for item in value)
    previous_key: tuple[int, str] | None = None
    seen_casefold: set[tuple[str, str]] = set()
    seen_file = False
    for entry in reconstructed:
        index = INPUT_NAMES.index(entry.input_name)
        key = (index, entry.relative_path)
        if previous_key is not None and not previous_key < key:
            raise PromotedOperationalInputSnapshotError(_ERR)
        previous_key = key
        casefold_key = (entry.input_name, entry.relative_path.casefold())
        if casefold_key in seen_casefold:
            raise PromotedOperationalInputSnapshotError(_ERR)
        seen_casefold.add(casefold_key)
        if entry.input_name == FILE_INPUT_NAME:
            if seen_file:
                raise PromotedOperationalInputSnapshotError(_ERR)
            seen_file = True
    if not seen_file:
        # An empty overall snapshot is impossible: the assembly-spec file
        # is always mandatory.
        raise PromotedOperationalInputSnapshotError(_ERR)
    return reconstructed


def _entry_body(entry: PromotedOperationalInputEntry) -> dict[str, object]:
    return {
        "byte_count": entry.byte_count,
        "input_name": entry.input_name,
        "relative_path": entry.relative_path,
        "sha256": entry.sha256,
    }


def _inventory_body(
    inventory: "PromotedOperationalInputInventory", *, include_inventory_id: bool
) -> dict[str, object]:
    body: dict[str, object] = {
        "entries": [_entry_body(entry) for entry in inventory.entries],
        "entry_count": inventory.entry_count,
        "expected_assembly_spec_id": inventory.expected_assembly_spec_id,
        "input_names": list(inventory.input_names),
        "schema_version": inventory.schema_version,
        "target_session": inventory.target_session.isoformat(),
        "total_bytes": inventory.total_bytes,
    }
    if include_inventory_id:
        body["inventory_id"] = inventory.inventory_id
    return body


def _canonical_json_bytes(value: dict[str, object]) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True)
        + "\n"
    ).encode("utf-8")


def _validate_inventory_state(
    candidate: "PromotedOperationalInputInventory",
) -> tuple[PromotedOperationalInputEntry, ...]:
    """Independently re-verify every scalar and aggregate invariant,
    shared by ``__post_init__`` and ``verify_content_identity`` so a
    tampered field is caught even if a recomputed hash happens to match."""

    if type(candidate.schema_version) is not int or candidate.schema_version != _SCHEMA_VERSION:
        raise PromotedOperationalInputSnapshotError(_ERR)
    _validate_sha256(candidate.expected_assembly_spec_id)
    if type(candidate.target_session) is not date:
        raise PromotedOperationalInputSnapshotError(_ERR)
    if type(candidate.input_names) is not tuple or candidate.input_names != INPUT_NAMES:
        raise PromotedOperationalInputSnapshotError(_ERR)
    for name in candidate.input_names:
        if type(name) is not str:
            raise PromotedOperationalInputSnapshotError(_ERR)

    reconstructed = _validated_entries(candidate.entries)
    if len(reconstructed) > MAXIMUM_INCLUDED_FILES:
        raise PromotedOperationalInputSnapshotError(_ERR)

    total_bytes = 0
    for entry in reconstructed:
        total_bytes += entry.byte_count
    if total_bytes > MAXIMUM_TOTAL_BYTES:
        raise PromotedOperationalInputSnapshotError(_ERR)

    if type(candidate.entry_count) is not int or candidate.entry_count != len(reconstructed):
        raise PromotedOperationalInputSnapshotError(_ERR)
    if type(candidate.total_bytes) is not int or candidate.total_bytes != total_bytes:
        raise PromotedOperationalInputSnapshotError(_ERR)
    return reconstructed


@dataclass(frozen=True, slots=True)
class PromotedOperationalInputInventory:
    """Deterministic, content-addressed local scan result for exactly the
    twelve fixed inputs. Purely local: carries no bucket, no GCS object
    name, and no ``PublishedStateObject`` -- those belong to the manifest
    type in ``promoted_operational_input_gcs``."""

    schema_version: int
    expected_assembly_spec_id: str
    target_session: date
    input_names: tuple[str, ...]
    entries: tuple[PromotedOperationalInputEntry, ...]
    entry_count: int
    total_bytes: int
    inventory_id: str = field(init=False)

    def __post_init__(self) -> None:
        reconstructed_entries = _validate_inventory_state(self)
        object.__setattr__(self, "entries", reconstructed_entries)
        object.__setattr__(self, "input_names", INPUT_NAMES)
        object.__setattr__(self, "inventory_id", self._calculated_inventory_id())

    def _calculated_inventory_id(self) -> str:
        failed = False
        digest = ""
        try:
            body_bytes = _canonical_json_bytes(_inventory_body(self, include_inventory_id=False))
            digest = hashlib.sha256(body_bytes).hexdigest()
        except Exception:
            failed = True
        if failed:
            raise PromotedOperationalInputSnapshotError(_ERR)
        return digest

    def verify_content_identity(self) -> None:
        failed = False
        try:
            if type(self) is not PromotedOperationalInputInventory:
                raise PromotedOperationalInputSnapshotError(_ERR)
            _validate_inventory_state(self)
            if self.inventory_id != self._calculated_inventory_id():
                raise PromotedOperationalInputSnapshotError(_ERR)
        except Exception:
            failed = True
        if failed:
            raise PromotedOperationalInputSnapshotError(_ERR)


def encode_promoted_operational_input_inventory(
    inventory: PromotedOperationalInputInventory,
) -> bytes:
    if type(inventory) is not PromotedOperationalInputInventory:
        raise PromotedOperationalInputSnapshotError(_ERR)
    inventory.verify_content_identity()
    encode_failed = False
    encoded = b""
    try:
        encoded = _canonical_json_bytes(_inventory_body(inventory, include_inventory_id=True))
    except Exception:
        encode_failed = True
    if encode_failed:
        raise PromotedOperationalInputSnapshotError(_ERR)
    if len(encoded) > MAXIMUM_ENCODED_BYTES:
        raise PromotedOperationalInputSnapshotError(_ERR)
    return encoded


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedOperationalInputSnapshotError(_ERR)
        result[key] = value
    return result


def _reject_number(_token: str) -> object:
    raise PromotedOperationalInputSnapshotError(_ERR)


def decode_promoted_operational_input_inventory(payload: bytes) -> PromotedOperationalInputInventory:
    if type(payload) is not bytes or not (0 < len(payload) <= MAXIMUM_ENCODED_BYTES):
        raise PromotedOperationalInputSnapshotError(_ERR)

    decode_failed = False
    inventory: PromotedOperationalInputInventory | None = None
    try:
        text = payload.decode("utf-8", errors="strict")
        raw = json.loads(
            text, object_pairs_hook=_unique_object, parse_float=_reject_number, parse_constant=_reject_number,
        )
        if type(raw) is not dict or set(raw) != _TOP_LEVEL_KEYS:
            raise PromotedOperationalInputSnapshotError(_ERR)
        raw_entries = raw["entries"]
        if type(raw_entries) is not list:
            raise PromotedOperationalInputSnapshotError(_ERR)
        entries: list[PromotedOperationalInputEntry] = []
        for raw_entry in raw_entries:
            if type(raw_entry) is not dict or set(raw_entry) != _ENTRY_KEYS:
                raise PromotedOperationalInputSnapshotError(_ERR)
            entries.append(
                PromotedOperationalInputEntry(
                    input_name=raw_entry["input_name"],
                    relative_path=raw_entry["relative_path"],
                    byte_count=raw_entry["byte_count"],
                    sha256=raw_entry["sha256"],
                )
            )
        raw_input_names = raw["input_names"]
        if type(raw_input_names) is not list:
            raise PromotedOperationalInputSnapshotError(_ERR)
        inventory = PromotedOperationalInputInventory(
            schema_version=raw["schema_version"],
            expected_assembly_spec_id=raw["expected_assembly_spec_id"],
            target_session=date.fromisoformat(raw["target_session"]),
            input_names=tuple(raw_input_names),
            entries=tuple(entries),
            entry_count=raw["entry_count"],
            total_bytes=raw["total_bytes"],
        )
        if inventory.inventory_id != raw["inventory_id"]:
            raise PromotedOperationalInputSnapshotError(_ERR)
        if encode_promoted_operational_input_inventory(inventory) != payload:
            raise PromotedOperationalInputSnapshotError(_ERR)
    except Exception:
        decode_failed = True
    if decode_failed or inventory is None:
        raise PromotedOperationalInputSnapshotError(_ERR)
    return inventory


def input_destination_path(control: PromotedOperationalCloudRunControl, entry: PromotedOperationalInputEntry) -> Path:
    """Pure path mapping: never touches the filesystem. ``state_root`` is
    never reachable through this function since ``entry.input_name`` is
    restricted to ``INPUT_NAMES``, which excludes it."""

    if type(control) is not PromotedOperationalCloudRunControl:
        raise PromotedOperationalInputSnapshotError(_ERR)
    entry = _reconstructed_entry(entry)
    if entry.input_name == FILE_INPUT_NAME:
        return control.assembly_spec_file
    root_path = getattr(control, entry.input_name)
    if type(root_path) is not _CONCRETE_PATH_TYPE:
        raise PromotedOperationalInputSnapshotError(_ERR)
    return root_path.joinpath(*entry.relative_path.split("/"))


def _link_like(status: os.stat_result) -> bool:
    if stat.S_ISLNK(status.st_mode):
        return True
    attribute = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool(getattr(status, "st_file_attributes", 0) & attribute)


def _lstat_or_raise(path: Path) -> os.stat_result:
    failed = False
    status: os.stat_result | None = None
    try:
        status = os.lstat(path)
    except OSError:
        failed = True
    if failed or status is None:
        raise PromotedOperationalInputSnapshotError(_ERR)
    return status


def _scandir_names(path: Path) -> list[str]:
    failed = False
    names: list[str] = []
    try:
        with os.scandir(path) as iterator:
            names = sorted(entry.name for entry in iterator)
    except OSError:
        failed = True
    if failed:
        raise PromotedOperationalInputSnapshotError(_ERR)
    return names


def _read_included_file(path: Path) -> bytes:
    failed = False
    payload = b""
    try:
        payload = read_stable_regular_file(path, maximum_bytes=MAXIMUM_FILE_BYTES)
    except FileSafetyError:
        failed = True
    if failed:
        raise PromotedOperationalInputSnapshotError(_ERR)
    return payload


def _file_identity(status: os.stat_result) -> tuple[int, int, int, int]:
    return (status.st_dev, status.st_ino, status.st_size, status.st_mtime_ns)


def _snapshot_directory(
    path: Path, before_status: os.stat_result
) -> tuple[tuple[int, int, int, int], tuple[str, ...]]:
    names = tuple(_scandir_names(path))
    after_status = _lstat_or_raise(path)
    if not stat.S_ISDIR(after_status.st_mode) or _link_like(after_status):
        raise PromotedOperationalInputSnapshotError(_ERR)
    if _file_identity(before_status) != _file_identity(after_status):
        raise PromotedOperationalInputSnapshotError(_ERR)
    return (_file_identity(after_status), names)


def _verify_directory_unchanged(
    path: Path, expected_identity: tuple[int, int, int, int], expected_names: tuple[str, ...]
) -> None:
    status = _lstat_or_raise(path)
    if not stat.S_ISDIR(status.st_mode) or _link_like(status):
        raise PromotedOperationalInputSnapshotError(_ERR)
    observed_identity, observed_names = _snapshot_directory(path, status)
    if observed_identity != expected_identity or observed_names != expected_names:
        raise PromotedOperationalInputSnapshotError(_ERR)


def build_promoted_operational_input_inventory(
    control: PromotedOperationalCloudRunControl,
) -> PromotedOperationalInputInventory:
    """Deterministically scan exactly the mandatory assembly-spec file and
    the eleven fixed read-only roots into one canonical inventory.

    Every non-regular filesystem entry (directory, symlink, reparse point,
    or unsupported type) under a root fails closed. The control and every
    scanned file/directory are re-verified unchanged across the full scan
    window before an inventory is ever returned. ``state_root`` is never
    read, listed, or referenced -- it has no attribute access here at all.
    """

    if type(control) is not PromotedOperationalCloudRunControl:
        raise PromotedOperationalInputSnapshotError(_ERR)
    control_verify_failed = False
    try:
        control.verify_content_identity()
    except Exception:
        control_verify_failed = True
    if control_verify_failed:
        raise PromotedOperationalInputSnapshotError(_ERR)

    bound_assembly_spec_id = control.expected_assembly_spec_id
    bound_target_session = control.target_session

    collected: list[PromotedOperationalInputEntry] = []
    file_count = 0
    total_bytes = 0
    directory_fingerprints: list[tuple[Path, tuple[int, int, int, int], tuple[str, ...]]] = []

    assembly_spec_file = control.assembly_spec_file
    file_status = _lstat_or_raise(assembly_spec_file)
    if not stat.S_ISREG(file_status.st_mode) or _link_like(file_status):
        raise PromotedOperationalInputSnapshotError(_ERR)
    payload = _read_included_file(assembly_spec_file)
    byte_count = len(payload)
    total_bytes += byte_count
    if total_bytes > MAXIMUM_TOTAL_BYTES:
        raise PromotedOperationalInputSnapshotError(_ERR)
    file_count += 1
    collected.append(
        PromotedOperationalInputEntry(
            input_name=FILE_INPUT_NAME,
            relative_path="",
            byte_count=byte_count,
            sha256=hashlib.sha256(payload).hexdigest(),
        )
    )

    for root_name in ROOT_INPUT_NAMES:
        root_path = getattr(control, root_name)
        root_status = _lstat_or_raise(root_path)
        if not stat.S_ISDIR(root_status.st_mode) or _link_like(root_status):
            raise PromotedOperationalInputSnapshotError(_ERR)

        root_identity, root_names = _snapshot_directory(root_path, root_status)
        directory_fingerprints.append((root_path, root_identity, root_names))

        stack: list[tuple[Path, tuple[str, ...], tuple[str, ...]]] = [(root_path, (), root_names)]
        while stack:
            directory, segments, child_names = stack.pop()
            for name in child_names:
                child_path = directory / name
                child_status = _lstat_or_raise(child_path)
                if _link_like(child_status):
                    raise PromotedOperationalInputSnapshotError(_ERR)

                if stat.S_ISDIR(child_status.st_mode):
                    grandchild_identity, grandchild_names = _snapshot_directory(child_path, child_status)
                    directory_fingerprints.append((child_path, grandchild_identity, grandchild_names))
                    stack.append((child_path, segments + (name,), grandchild_names))
                    continue

                if not stat.S_ISREG(child_status.st_mode):
                    raise PromotedOperationalInputSnapshotError(_ERR)

                file_count += 1
                if file_count > MAXIMUM_INCLUDED_FILES:
                    raise PromotedOperationalInputSnapshotError(_ERR)

                child_payload = _read_included_file(child_path)
                child_byte_count = len(child_payload)
                total_bytes += child_byte_count
                if total_bytes > MAXIMUM_TOTAL_BYTES:
                    raise PromotedOperationalInputSnapshotError(_ERR)

                relative_path = "/".join(segments + (name,))
                collected.append(
                    PromotedOperationalInputEntry(
                        input_name=root_name,
                        relative_path=relative_path,
                        byte_count=child_byte_count,
                        sha256=hashlib.sha256(child_payload).hexdigest(),
                    )
                )

    for path, identity, names in directory_fingerprints:
        _verify_directory_unchanged(path, identity, names)

    after_file_status = _lstat_or_raise(assembly_spec_file)
    if (
        not stat.S_ISREG(after_file_status.st_mode)
        or _link_like(after_file_status)
        or _file_identity(after_file_status) != _file_identity(file_status)
    ):
        raise PromotedOperationalInputSnapshotError(_ERR)

    control_changed = False
    try:
        control.verify_content_identity()
        if (
            control.expected_assembly_spec_id != bound_assembly_spec_id
            or control.target_session != bound_target_session
        ):
            control_changed = True
    except Exception:
        control_changed = True
    if control_changed:
        raise PromotedOperationalInputSnapshotError(_ERR)

    collected.sort(key=lambda entry: (INPUT_NAMES.index(entry.input_name), entry.relative_path))

    return PromotedOperationalInputInventory(
        schema_version=_SCHEMA_VERSION,
        expected_assembly_spec_id=bound_assembly_spec_id,
        target_session=bound_target_session,
        input_names=INPUT_NAMES,
        entries=tuple(collected),
        entry_count=len(collected),
        total_bytes=total_bytes,
    )


def verify_hydrated_input_inventory(
    control: PromotedOperationalCloudRunControl,
    expected_inventory: PromotedOperationalInputInventory,
) -> None:
    """Independently rebuild the inventory from the control's own
    destinations and require it to match the expected (acquired) inventory
    byte-for-byte/identity-for-identity. Composes
    ``build_promoted_operational_input_inventory`` rather than duplicating
    its scan; never itself performs a filesystem write."""

    if type(expected_inventory) is not PromotedOperationalInputInventory:
        raise PromotedOperationalInputSnapshotError(_ERR)
    verify_failed = False
    try:
        expected_inventory.verify_content_identity()
        rebuilt = build_promoted_operational_input_inventory(control)
        if encode_promoted_operational_input_inventory(
            rebuilt
        ) != encode_promoted_operational_input_inventory(expected_inventory):
            raise PromotedOperationalInputSnapshotError(_ERR)
    except Exception:
        verify_failed = True
    if verify_failed:
        raise PromotedOperationalInputSnapshotError(_ERR)
