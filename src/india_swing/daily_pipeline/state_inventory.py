from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

from india_swing._filesystem import FileSafetyError, read_stable_regular_file

from .models import DailyPipelineRun


ROOT_NAMES: tuple[str, ...] = (
    "calendar_data",
    "identity_registry",
    "historical_prices",
    "daily_reports",
    "reference_data",
    "daily_pipeline",
)

MAXIMUM_INCLUDED_FILES = 100_000
MAXIMUM_FILE_BYTES = 256 * 1024 * 1024
MAXIMUM_TOTAL_BYTES = 2 * 1024 * 1024 * 1024
MAXIMUM_ENCODED_BYTES = 16 * 1024 * 1024

_LOCK_DIRECTORY_NAME = ".locks"
_LOCK_FILE_NAMES = frozenset(
    {
        ".daily-runs.lock",
        ".adjudication-queues.lock",
        ".identity-registry.lock",
        ".derived-evidence.lock",
    }
)

_SEGMENT_PATTERN_CHARS = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-"
)
_SEGMENT_LEADING_CHARS = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
)
_MAXIMUM_SEGMENT_LENGTH = 128
_MAXIMUM_RELATIVE_PATH_LENGTH = 512
_SHA256_CHARS = frozenset("0123456789abcdef")

_CONCRETE_PATH_TYPE: type = type(Path())

_TOP_LEVEL_KEYS = frozenset(
    {
        "schema_version",
        "run_id",
        "previous_run_id",
        "market_session",
        "cutoff",
        "entries",
        "entry_count",
        "total_bytes",
        "inventory_id",
    }
)
_ENTRY_KEYS = frozenset({"root_name", "relative_path", "byte_count", "sha256"})

_ERROR_ROOT_TYPE = "pipeline state root must be an exact absolute path value"
_ERROR_ROOT_NOT_ABSOLUTE = "pipeline state root must be an exact absolute path value"
_ERROR_ROOT_DOT_DOT = "pipeline state root must not contain a dot-dot component"
_ERROR_ROOT_OVERLAP = "pipeline state roots must be lexically distinct and non-overlapping"
_ERROR_ROOT_MISSING = "pipeline state root is unavailable"
_ERROR_ROOT_NOT_DIRECTORY = "pipeline state root must be a real non-reparse directory"
_ERROR_TREE_UNREADABLE = "pipeline state tree could not be listed safely"
_ERROR_TREE_MUTATED = "pipeline state tree changed during inventory collection"
_ERROR_RUN_VERIFICATION_FAILED = "pipeline state run content identity verification failed"
_ERROR_RUN_MUTATED = "pipeline state run changed during inventory collection"
_ERROR_ENCODE_FAILED = "pipeline state inventory could not be encoded"
_ERROR_LINK_LIKE_ENTRY = "pipeline state tree contains a link or reparse entry"
_ERROR_UNSUPPORTED_ENTRY_TYPE = "pipeline state tree contains an unsupported entry type"
_ERROR_FILE_UNREADABLE = "pipeline state file could not be read safely"
_ERROR_UNSAFE_PATH = "pipeline state relative path is unsafe"
_ERROR_TOO_MANY_FILES = "pipeline state inventory exceeds the included-file ceiling"
_ERROR_TOTAL_BYTES_EXCEEDED = "pipeline state inventory exceeds the total-byte ceiling"
_ERROR_ENCODED_TOO_LARGE = "pipeline state inventory exceeds the encoded-byte ceiling"
_ERROR_RUN_TYPE = "run must be an exact DailyPipelineRun"
_ERROR_ROOTS_TYPE = "roots must be an exact PipelineStateRoots"
_ERROR_ENTRY_TYPE = "pipeline state entry must be exact"
_ERROR_ENTRY_ROOT_NAME = "pipeline state entry root name is invalid"
_ERROR_ENTRY_BYTE_COUNT = "pipeline state entry byte count is invalid"
_ERROR_ENTRY_HASH = "pipeline state entry hash is invalid"
_ERROR_INVENTORY_TYPE = "inventory must be an exact PipelineStateInventory"
_ERROR_SCHEMA_VERSION = "pipeline state inventory schema version is unsupported"
_ERROR_RUN_ID = "pipeline state inventory run identifier is invalid"
_ERROR_PREVIOUS_RUN_ID = "pipeline state inventory previous run identifier is invalid"
_ERROR_MARKET_SESSION = "pipeline state inventory market session is invalid"
_ERROR_CUTOFF = "pipeline state inventory cutoff is invalid"
_ERROR_ENTRIES_TYPE = "pipeline state inventory entries must be an exact tuple"
_ERROR_ENTRIES_NOT_ORDERED = "pipeline state inventory entries are not canonically ordered"
_ERROR_ENTRIES_CASEFOLD_COLLISION = "pipeline state inventory entries collide within a root"
_ERROR_COUNT_MISMATCH = "pipeline state inventory entry count is inconsistent"
_ERROR_TOTAL_BYTES_MISMATCH = "pipeline state inventory total bytes is inconsistent"
_ERROR_INVENTORY_ID = "pipeline state inventory identity verification failed"
_ERROR_PAYLOAD_TYPE = "pipeline state inventory payload must be bytes"
_ERROR_PAYLOAD_EMPTY = "pipeline state inventory payload is empty"
_ERROR_PAYLOAD_TOO_LARGE = "pipeline state inventory payload exceeds the encoded-byte ceiling"
_ERROR_PAYLOAD_MALFORMED = "pipeline state inventory payload is not valid canonical JSON"
_ERROR_PAYLOAD_SHAPE = "pipeline state inventory payload has an invalid shape"
_ERROR_PAYLOAD_NONCANONICAL = "pipeline state inventory payload is not canonical"


class PipelineStateInventoryError(ValueError):
    pass


def _link_like(status: os.stat_result) -> bool:
    if stat.S_ISLNK(status.st_mode):
        return True
    attribute = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool(getattr(status, "st_file_attributes", 0) & attribute)


def _lexical_root_key(value: Path) -> str:
    # os.path.normcase is a no-op on POSIX and lowercases plus normalizes
    # separators on Windows, so two case-aliased or slash-style-aliased
    # paths to the same location always normalize to the same key. This is
    # a pure string transform: no filesystem access, no symlink following.
    return os.path.normcase(os.path.normpath(str(value)))


def _key_overlaps(candidate: str, other: str) -> bool:
    if candidate == other:
        return True
    prefix = other if other.endswith(os.sep) else other + os.sep
    return candidate.startswith(prefix)


def _validate_relative_path(value: str) -> None:
    if not value or len(value) > _MAXIMUM_RELATIVE_PATH_LENGTH:
        raise PipelineStateInventoryError(_ERROR_UNSAFE_PATH)
    for segment in value.split("/"):
        if not segment or len(segment) > _MAXIMUM_SEGMENT_LENGTH:
            raise PipelineStateInventoryError(_ERROR_UNSAFE_PATH)
        if segment[0] not in _SEGMENT_LEADING_CHARS:
            raise PipelineStateInventoryError(_ERROR_UNSAFE_PATH)
        if not _SEGMENT_PATTERN_CHARS.issuperset(segment):
            raise PipelineStateInventoryError(_ERROR_UNSAFE_PATH)


def _validate_sha256(value: object) -> None:
    if (
        type(value) is not str
        or len(value) != 64
        or not _SHA256_CHARS.issuperset(value)
    ):
        raise PipelineStateInventoryError(_ERROR_ENTRY_HASH)


@dataclass(frozen=True, slots=True)
class PipelineStateRoots:
    calendar_data: Path
    identity_registry: Path
    historical_prices: Path
    daily_reports: Path
    reference_data: Path
    daily_pipeline: Path

    def __post_init__(self) -> None:
        keys: list[str] = []
        for root_name in ROOT_NAMES:
            value = getattr(self, root_name)
            if type(value) is not _CONCRETE_PATH_TYPE:
                raise PipelineStateInventoryError(_ERROR_ROOT_TYPE)
            if not value.is_absolute():
                raise PipelineStateInventoryError(_ERROR_ROOT_NOT_ABSOLUTE)
            if ".." in value.parts:
                raise PipelineStateInventoryError(_ERROR_ROOT_DOT_DOT)
            keys.append(_lexical_root_key(value))
        for index, left in enumerate(keys):
            for right in keys[index + 1 :]:
                if _key_overlaps(left, right) or _key_overlaps(right, left):
                    raise PipelineStateInventoryError(_ERROR_ROOT_OVERLAP)


@dataclass(frozen=True, slots=True)
class PipelineStateEntry:
    root_name: str
    relative_path: str
    byte_count: int
    sha256: str

    def __post_init__(self) -> None:
        if type(self.root_name) is not str or self.root_name not in ROOT_NAMES:
            raise PipelineStateInventoryError(_ERROR_ENTRY_ROOT_NAME)
        if type(self.relative_path) is not str:
            raise PipelineStateInventoryError(_ERROR_UNSAFE_PATH)
        _validate_relative_path(self.relative_path)
        if (
            type(self.byte_count) is not int
            or self.byte_count <= 0
            or self.byte_count > MAXIMUM_FILE_BYTES
        ):
            raise PipelineStateInventoryError(_ERROR_ENTRY_BYTE_COUNT)
        _validate_sha256(self.sha256)


def _reconstructed_entry(value: object) -> PipelineStateEntry:
    if type(value) is not PipelineStateEntry:
        raise PipelineStateInventoryError(_ERROR_ENTRY_TYPE)
    return PipelineStateEntry(
        root_name=value.root_name,
        relative_path=value.relative_path,
        byte_count=value.byte_count,
        sha256=value.sha256,
    )


def _validated_entries(value: object) -> tuple[PipelineStateEntry, ...]:
    if type(value) is not tuple:
        raise PipelineStateInventoryError(_ERROR_ENTRIES_TYPE)
    reconstructed = tuple(_reconstructed_entry(item) for item in value)
    previous_key: tuple[int, str] | None = None
    seen_casefold: set[tuple[str, str]] = set()
    for entry in reconstructed:
        root_index = ROOT_NAMES.index(entry.root_name)
        key = (root_index, entry.relative_path)
        if previous_key is not None and not previous_key < key:
            raise PipelineStateInventoryError(_ERROR_ENTRIES_NOT_ORDERED)
        previous_key = key
        casefold_key = (entry.root_name, entry.relative_path.casefold())
        if casefold_key in seen_casefold:
            raise PipelineStateInventoryError(_ERROR_ENTRIES_CASEFOLD_COLLISION)
        seen_casefold.add(casefold_key)
    return reconstructed


def _entry_body(entry: PipelineStateEntry) -> dict[str, object]:
    return {
        "byte_count": entry.byte_count,
        "relative_path": entry.relative_path,
        "root_name": entry.root_name,
        "sha256": entry.sha256,
    }


def _inventory_body(
    inventory: "PipelineStateInventory", *, include_inventory_id: bool
) -> dict[str, object]:
    body: dict[str, object] = {
        "cutoff": inventory.cutoff.isoformat(),
        "entries": [_entry_body(entry) for entry in inventory.entries],
        "entry_count": inventory.entry_count,
        "market_session": inventory.market_session.isoformat(),
        "previous_run_id": inventory.previous_run_id,
        "run_id": inventory.run_id,
        "schema_version": inventory.schema_version,
        "total_bytes": inventory.total_bytes,
    }
    if include_inventory_id:
        body["inventory_id"] = inventory.inventory_id
    return body


def _canonical_json_bytes(value: dict[str, object]) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


@dataclass(frozen=True, slots=True)
class PipelineStateInventory:
    schema_version: int
    run_id: str
    previous_run_id: str | None
    market_session: date
    cutoff: datetime
    entries: tuple[PipelineStateEntry, ...]
    entry_count: int
    total_bytes: int
    inventory_id: str = field(init=False)

    def __post_init__(self) -> None:
        reconstructed_entries = _validate_inventory_state(self)
        object.__setattr__(self, "entries", reconstructed_entries)
        object.__setattr__(self, "inventory_id", self._calculated_inventory_id())

    def _calculated_inventory_id(self) -> str:
        failed = False
        digest = ""
        try:
            body_bytes = _canonical_json_bytes(
                _inventory_body(self, include_inventory_id=False)
            )
            digest = hashlib.sha256(body_bytes).hexdigest()
        except Exception:
            failed = True
        if failed:
            raise PipelineStateInventoryError(_ERROR_INVENTORY_ID)
        return digest

    def verify_content_identity(self) -> None:
        failed = False
        try:
            if type(self) is not PipelineStateInventory:
                raise PipelineStateInventoryError(_ERROR_INVENTORY_TYPE)
            _validate_inventory_state(self)
            if self.inventory_id != self._calculated_inventory_id():
                raise PipelineStateInventoryError(_ERROR_INVENTORY_ID)
        except Exception:
            failed = True
        if failed:
            raise PipelineStateInventoryError(_ERROR_INVENTORY_ID)


def _validate_sha256_field(value: object, error_message: str) -> None:
    if (
        type(value) is not str
        or len(value) != 64
        or not _SHA256_CHARS.issuperset(value)
    ):
        raise PipelineStateInventoryError(error_message)


def _validate_inventory_state(
    candidate: "PipelineStateInventory",
) -> tuple[PipelineStateEntry, ...]:
    """Independently re-verify every scalar and aggregate invariant.

    Shared by __post_init__ and verify_content_identity so a caller that
    mutates a field and recomputes inventory_id through a private helper
    still fails: the scalar/aggregate checks below never depend on
    inventory_id and are re-run in full regardless of whether the hash
    happens to match.
    """

    if type(candidate.schema_version) is not int or candidate.schema_version != 1:
        raise PipelineStateInventoryError(_ERROR_SCHEMA_VERSION)
    _validate_sha256_field(candidate.run_id, _ERROR_RUN_ID)
    if candidate.previous_run_id is not None:
        _validate_sha256_field(candidate.previous_run_id, _ERROR_PREVIOUS_RUN_ID)
    if type(candidate.market_session) is not date:
        raise PipelineStateInventoryError(_ERROR_MARKET_SESSION)
    if type(candidate.cutoff) is not datetime:
        raise PipelineStateInventoryError(_ERROR_CUTOFF)

    failed = False
    offset = None
    try:
        offset = candidate.cutoff.utcoffset()
    except Exception:
        failed = True
    if failed:
        raise PipelineStateInventoryError(_ERROR_CUTOFF)
    if candidate.cutoff.tzinfo is None or offset != timedelta(0):
        raise PipelineStateInventoryError(_ERROR_CUTOFF)

    reconstructed_entries = _validated_entries(candidate.entries)
    if len(reconstructed_entries) > MAXIMUM_INCLUDED_FILES:
        raise PipelineStateInventoryError(_ERROR_TOO_MANY_FILES)

    total_bytes = 0
    for entry in reconstructed_entries:
        total_bytes += entry.byte_count
    if total_bytes > MAXIMUM_TOTAL_BYTES:
        raise PipelineStateInventoryError(_ERROR_TOTAL_BYTES_EXCEEDED)

    if (
        type(candidate.entry_count) is not int
        or candidate.entry_count != len(reconstructed_entries)
    ):
        raise PipelineStateInventoryError(_ERROR_COUNT_MISMATCH)
    if type(candidate.total_bytes) is not int or candidate.total_bytes != total_bytes:
        raise PipelineStateInventoryError(_ERROR_TOTAL_BYTES_MISMATCH)

    return reconstructed_entries


def _lstat_or_raise(path: Path, error_message: str) -> os.stat_result:
    failed = False
    status: os.stat_result | None = None
    try:
        status = os.lstat(path)
    except OSError:
        failed = True
    if failed or status is None:
        raise PipelineStateInventoryError(error_message)
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
        raise PipelineStateInventoryError(_ERROR_TREE_UNREADABLE)
    return names


def _read_included_file(path: Path) -> bytes:
    failed = False
    payload = b""
    try:
        payload = read_stable_regular_file(path, maximum_bytes=MAXIMUM_FILE_BYTES)
    except FileSafetyError:
        failed = True
    if failed:
        raise PipelineStateInventoryError(_ERROR_FILE_UNREADABLE)
    return payload


def _directory_identity(status: os.stat_result) -> tuple[int, int, int, int]:
    return (status.st_dev, status.st_ino, status.st_size, status.st_mtime_ns)


def _snapshot_directory(
    path: Path, before_status: os.stat_result
) -> tuple[tuple[int, int, int, int], tuple[str, ...]]:
    """Scan a directory whose pre-scan lstat is already known, then
    lstat it again and require identity/version to be unchanged across
    the scan window. Returns (identity, sorted child names)."""

    names = tuple(_scandir_names(path))
    after_status = _lstat_or_raise(path, _ERROR_TREE_MUTATED)
    if not stat.S_ISDIR(after_status.st_mode) or _link_like(after_status):
        raise PipelineStateInventoryError(_ERROR_TREE_MUTATED)
    before_identity = _directory_identity(before_status)
    after_identity = _directory_identity(after_status)
    if before_identity != after_identity:
        raise PipelineStateInventoryError(_ERROR_TREE_MUTATED)
    return (after_identity, names)


def _verify_directory_unchanged(
    path: Path,
    expected_identity: tuple[int, int, int, int],
    expected_names: tuple[str, ...],
) -> None:
    status = _lstat_or_raise(path, _ERROR_TREE_MUTATED)
    if not stat.S_ISDIR(status.st_mode) or _link_like(status):
        raise PipelineStateInventoryError(_ERROR_TREE_MUTATED)
    observed_identity, observed_names = _snapshot_directory(path, status)
    if observed_identity != expected_identity or observed_names != expected_names:
        raise PipelineStateInventoryError(_ERROR_TREE_MUTATED)


def build_pipeline_state_inventory(
    run: DailyPipelineRun, roots: PipelineStateRoots
) -> PipelineStateInventory:
    if type(run) is not DailyPipelineRun:
        raise PipelineStateInventoryError(_ERROR_RUN_TYPE)
    if type(roots) is not PipelineStateRoots:
        raise PipelineStateInventoryError(_ERROR_ROOTS_TYPE)
    roots = PipelineStateRoots(
        calendar_data=roots.calendar_data,
        identity_registry=roots.identity_registry,
        historical_prices=roots.historical_prices,
        daily_reports=roots.daily_reports,
        reference_data=roots.reference_data,
        daily_pipeline=roots.daily_pipeline,
    )

    failed = False
    bound_run_id = ""
    bound_previous_run_id: str | None = None
    bound_market_session: date | None = None
    bound_cutoff: datetime | None = None
    try:
        run.verify_content_identity()
        bound_run_id = run.run_id
        bound_previous_run_id = run.previous_run_id
        bound_market_session = run.market_session
        bound_cutoff = run.cutoff
    except Exception:
        failed = True
    if failed:
        raise PipelineStateInventoryError(_ERROR_RUN_VERIFICATION_FAILED)

    collected: list[PipelineStateEntry] = []
    file_count = 0
    total_bytes = 0
    directory_fingerprints: list[
        tuple[Path, tuple[int, int, int, int], tuple[str, ...]]
    ] = []

    for root_name in ROOT_NAMES:
        root_path = getattr(roots, root_name)
        root_status = _lstat_or_raise(root_path, _ERROR_ROOT_MISSING)
        if not stat.S_ISDIR(root_status.st_mode) or _link_like(root_status):
            raise PipelineStateInventoryError(_ERROR_ROOT_NOT_DIRECTORY)

        root_identity, root_names = _snapshot_directory(root_path, root_status)
        directory_fingerprints.append((root_path, root_identity, root_names))

        stack: list[tuple[Path, tuple[str, ...], tuple[str, ...]]] = [
            (root_path, (), root_names)
        ]
        while stack:
            directory, segments, child_names = stack.pop()
            for name in child_names:
                child_path = directory / name
                child_status = _lstat_or_raise(child_path, _ERROR_TREE_UNREADABLE)
                if _link_like(child_status):
                    raise PipelineStateInventoryError(_ERROR_LINK_LIKE_ENTRY)

                if stat.S_ISDIR(child_status.st_mode):
                    if name == _LOCK_DIRECTORY_NAME:
                        continue
                    grandchild_identity, grandchild_names = _snapshot_directory(
                        child_path, child_status
                    )
                    directory_fingerprints.append(
                        (child_path, grandchild_identity, grandchild_names)
                    )
                    stack.append((child_path, segments + (name,), grandchild_names))
                    continue

                if name in _LOCK_FILE_NAMES and stat.S_ISREG(child_status.st_mode):
                    continue
                if not stat.S_ISREG(child_status.st_mode):
                    raise PipelineStateInventoryError(_ERROR_UNSUPPORTED_ENTRY_TYPE)

                file_count += 1
                if file_count > MAXIMUM_INCLUDED_FILES:
                    raise PipelineStateInventoryError(_ERROR_TOO_MANY_FILES)

                payload = _read_included_file(child_path)
                byte_count = len(payload)
                total_bytes += byte_count
                if total_bytes > MAXIMUM_TOTAL_BYTES:
                    raise PipelineStateInventoryError(_ERROR_TOTAL_BYTES_EXCEEDED)

                relative_path = "/".join(segments + (name,))
                digest = hashlib.sha256(payload).hexdigest()
                collected.append(
                    PipelineStateEntry(
                        root_name=root_name,
                        relative_path=relative_path,
                        byte_count=byte_count,
                        sha256=digest,
                    )
                )

    for path, identity, names in directory_fingerprints:
        _verify_directory_unchanged(path, identity, names)

    run_changed = False
    try:
        run.verify_content_identity()
        if (
            run.run_id != bound_run_id
            or run.previous_run_id != bound_previous_run_id
            or run.market_session != bound_market_session
            or run.cutoff != bound_cutoff
        ):
            run_changed = True
    except Exception:
        run_changed = True
    if run_changed:
        raise PipelineStateInventoryError(_ERROR_RUN_MUTATED)

    collected.sort(key=lambda entry: (ROOT_NAMES.index(entry.root_name), entry.relative_path))

    return PipelineStateInventory(
        schema_version=1,
        run_id=bound_run_id,
        previous_run_id=bound_previous_run_id,
        market_session=bound_market_session,
        cutoff=bound_cutoff,
        entries=tuple(collected),
        entry_count=len(collected),
        total_bytes=total_bytes,
    )


def encode_pipeline_state_inventory(inventory: PipelineStateInventory) -> bytes:
    if type(inventory) is not PipelineStateInventory:
        raise PipelineStateInventoryError(_ERROR_INVENTORY_TYPE)
    inventory.verify_content_identity()

    failed = False
    encoded = b""
    try:
        encoded = _canonical_json_bytes(
            _inventory_body(inventory, include_inventory_id=True)
        )
    except Exception:
        failed = True
    if failed:
        raise PipelineStateInventoryError(_ERROR_ENCODE_FAILED)

    if len(encoded) > MAXIMUM_ENCODED_BYTES:
        raise PipelineStateInventoryError(_ERROR_ENCODED_TOO_LARGE)
    return encoded


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _reject_constant(_value: str) -> None:
    raise ValueError("non-canonical numeric constant")


def _entry_from_raw(value: object) -> PipelineStateEntry:
    failed = False
    root_name = relative_path = sha256_value = None
    byte_count: object = None
    try:
        if type(value) is not dict or set(value) != _ENTRY_KEYS:
            raise ValueError
        root_name = value["root_name"]
        relative_path = value["relative_path"]
        byte_count = value["byte_count"]
        sha256_value = value["sha256"]
        if type(byte_count) is not int:
            raise ValueError
    except ValueError:
        failed = True
    if failed:
        raise PipelineStateInventoryError(_ERROR_PAYLOAD_SHAPE)
    return PipelineStateEntry(
        root_name=root_name,
        relative_path=relative_path,
        byte_count=byte_count,
        sha256=sha256_value,
    )


def _inventory_from_raw(raw: object) -> PipelineStateInventory:
    failed = False
    schema_version: object = None
    run_id = previous_run_id = None
    entries_raw: object = None
    entry_count = total_bytes = None
    market_session = cutoff = None
    declared_inventory_id = None
    try:
        if type(raw) is not dict or set(raw) != _TOP_LEVEL_KEYS:
            raise ValueError
        schema_version = raw["schema_version"]
        run_id = raw["run_id"]
        previous_run_id = raw["previous_run_id"]
        market_session_raw = raw["market_session"]
        cutoff_raw = raw["cutoff"]
        entries_raw = raw["entries"]
        entry_count = raw["entry_count"]
        total_bytes = raw["total_bytes"]
        declared_inventory_id = raw["inventory_id"]
        if type(market_session_raw) is not str or type(cutoff_raw) is not str:
            raise ValueError
        if type(entries_raw) is not list:
            raise ValueError
        market_session = date.fromisoformat(market_session_raw)
        cutoff = datetime.fromisoformat(cutoff_raw)
    except ValueError:
        failed = True
    if failed:
        raise PipelineStateInventoryError(_ERROR_PAYLOAD_SHAPE)

    entries = tuple(_entry_from_raw(item) for item in entries_raw)

    construction_failed = False
    inventory: PipelineStateInventory | None = None
    try:
        inventory = PipelineStateInventory(
            schema_version=schema_version,
            run_id=run_id,
            previous_run_id=previous_run_id,
            market_session=market_session,
            cutoff=cutoff,
            entries=entries,
            entry_count=entry_count,
            total_bytes=total_bytes,
        )
    except PipelineStateInventoryError:
        raise
    except Exception:
        construction_failed = True
    if construction_failed or inventory is None:
        raise PipelineStateInventoryError(_ERROR_PAYLOAD_SHAPE)

    if inventory.inventory_id != declared_inventory_id:
        raise PipelineStateInventoryError(_ERROR_INVENTORY_ID)
    return inventory


def parse_pipeline_state_inventory(payload: bytes) -> PipelineStateInventory:
    if type(payload) is not bytes:
        raise PipelineStateInventoryError(_ERROR_PAYLOAD_TYPE)
    if not payload:
        raise PipelineStateInventoryError(_ERROR_PAYLOAD_EMPTY)
    if len(payload) > MAXIMUM_ENCODED_BYTES:
        raise PipelineStateInventoryError(_ERROR_PAYLOAD_TOO_LARGE)

    failed = False
    raw: object = None
    try:
        text = payload.decode("utf-8", errors="strict")
        raw = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_float=lambda value: _reject_constant(value),
            parse_constant=lambda value: _reject_constant(value),
        )
    except (UnicodeDecodeError, ValueError):
        failed = True
    if failed:
        raise PipelineStateInventoryError(_ERROR_PAYLOAD_MALFORMED)

    inventory = _inventory_from_raw(raw)

    reencoded = encode_pipeline_state_inventory(inventory)
    if reencoded != payload:
        raise PipelineStateInventoryError(_ERROR_PAYLOAD_NONCANONICAL)
    return inventory
