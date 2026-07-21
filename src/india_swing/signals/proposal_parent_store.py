from __future__ import annotations

import json
import os
import re
import stat
import tempfile
import types
from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from functools import lru_cache
from pathlib import Path
from types import MappingProxyType
from typing import Union, get_args, get_origin, get_type_hints

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.reference.calendar import CalendarSnapshot

from .deterministic_swing import DeterministicSwingSignalConfig
from .proposal_artifacts import (
    LocalSwingProposalBatchStore,
    SwingProposalArtifactError,
    SwingProposalBatchInputResolver,
    SwingProposalBatchManifest,
)
from .proposal_batch import SwingProposalBatch
from .universe_batch import SwingUniverseInputBatch


PROPOSAL_PARENT_CODEC_VERSION = "swing-proposal-parent-json/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAXIMUM_GRAPH_DEPTH = 64
_MAXIMUM_GRAPH_NODES = 25_000_000


class SwingProposalParentKind(str, Enum):
    UNIVERSE_BATCH = "universe-batches"
    CALENDAR_SNAPSHOT = "calendar-snapshots"
    SIGNAL_CONFIG = "signal-configs"


_ROOT_TYPE_BY_KIND = MappingProxyType(
    {
        SwingProposalParentKind.UNIVERSE_BATCH: SwingUniverseInputBatch,
        SwingProposalParentKind.CALENDAR_SNAPSHOT: CalendarSnapshot,
        SwingProposalParentKind.SIGNAL_CONFIG: DeterministicSwingSignalConfig,
    }
)
_IDENTITY_FIELD_BY_KIND = MappingProxyType(
    {
        SwingProposalParentKind.UNIVERSE_BATCH: "batch_id",
        SwingProposalParentKind.CALENDAR_SNAPSHOT: "snapshot_id",
        SwingProposalParentKind.SIGNAL_CONFIG: "config_id",
    }
)
_MAXIMUM_BYTES_BY_KIND = MappingProxyType(
    {
        # Align with the existing pipeline-state inventory/publication ceiling
        # so every accepted parent remains eligible for GCS backup/restore.
        SwingProposalParentKind.UNIVERSE_BATCH: 256 * 1024 * 1024,
        SwingProposalParentKind.CALENDAR_SNAPSHOT: 64 * 1024 * 1024,
        SwingProposalParentKind.SIGNAL_CONFIG: 64 * 1024,
    }
)


class SwingProposalParentStoreError(SwingProposalArtifactError):
    pass


class SwingProposalParentNotFound(SwingProposalParentStoreError):
    pass


def _qualified_name(value_type: type[object]) -> str:
    return f"{value_type.__module__}.{value_type.__qualname__}"


@dataclass(slots=True)
class _GraphBudget:
    nodes: int = 0

    def consume(self, depth: int) -> None:
        if type(depth) is not int or depth < 0 or depth > _MAXIMUM_GRAPH_DEPTH:
            raise SwingProposalParentStoreError("stored parent graph exceeds its depth limit")
        self.nodes += 1
        if self.nodes > _MAXIMUM_GRAPH_NODES:
            raise SwingProposalParentStoreError("stored parent graph exceeds its node limit")


def _encode_graph(
    value: object,
    *,
    depth: int,
    budget: _GraphBudget,
    stack: set[int],
) -> object:
    budget.consume(depth)
    if value is None or type(value) in {bool, int, str}:
        return value
    if type(value) is Decimal:
        if not value.is_finite():
            raise SwingProposalParentStoreError("parent graph contains a non-finite Decimal")
        return {"$decimal": str(value)}
    if type(value) is datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise SwingProposalParentStoreError("parent graph contains a naive datetime")
        return {"$datetime": value.isoformat(timespec="microseconds")}
    if type(value) is date:
        return {"$date": value.isoformat()}
    if isinstance(value, Enum):
        return {
            "$enum": _qualified_name(type(value)),
            "value": _encode_graph(
                value.value,
                depth=depth + 1,
                budget=budget,
                stack=stack,
            ),
        }
    marker = id(value)
    if marker in stack:
        raise SwingProposalParentStoreError("parent graph contains a cycle")
    stack.add(marker)
    try:
        if type(value) is tuple:
            return {
                "$tuple": [
                    _encode_graph(
                        item,
                        depth=depth + 1,
                        budget=budget,
                        stack=stack,
                    )
                    for item in value
                ]
            }
        if is_dataclass(value) and not isinstance(value, type):
            return {
                "$dataclass": _qualified_name(type(value)),
                "fields": {
                    item.name: _encode_graph(
                        getattr(value, item.name),
                        depth=depth + 1,
                        budget=budget,
                        stack=stack,
                    )
                    for item in fields(value)
                },
            }
    finally:
        stack.remove(marker)
    raise SwingProposalParentStoreError("parent graph contains an unsupported value")


@lru_cache(maxsize=None)
def _annotations(value_type: type[object]) -> dict[str, object]:
    return get_type_hints(value_type)


def _strict_object(value: object, expected: set[str]) -> dict[str, object]:
    if type(value) is not dict or set(value) != expected:
        raise SwingProposalParentStoreError("stored parent graph has invalid fields")
    return value


def _decode_datetime(value: object) -> datetime:
    raw = _strict_object(value, {"$datetime"})["$datetime"]
    if type(raw) is not str:
        raise SwingProposalParentStoreError("stored parent datetime is invalid")
    try:
        result = datetime.fromisoformat(raw)
    except ValueError:
        raise SwingProposalParentStoreError("stored parent datetime is invalid") from None
    if (
        result.tzinfo is None
        or result.utcoffset() is None
        or result.isoformat(timespec="microseconds") != raw
    ):
        raise SwingProposalParentStoreError("stored parent datetime is invalid")
    return result


def _decode_date(value: object) -> date:
    raw = _strict_object(value, {"$date"})["$date"]
    if type(raw) is not str:
        raise SwingProposalParentStoreError("stored parent date is invalid")
    try:
        result = date.fromisoformat(raw)
    except ValueError:
        raise SwingProposalParentStoreError("stored parent date is invalid") from None
    if result.isoformat() != raw:
        raise SwingProposalParentStoreError("stored parent date is invalid")
    return result


def _decode_decimal(value: object) -> Decimal:
    raw = _strict_object(value, {"$decimal"})["$decimal"]
    if type(raw) is not str:
        raise SwingProposalParentStoreError("stored parent Decimal is invalid")
    try:
        result = Decimal(raw)
    except InvalidOperation:
        raise SwingProposalParentStoreError("stored parent Decimal is invalid") from None
    if not result.is_finite() or str(result) != raw:
        raise SwingProposalParentStoreError("stored parent Decimal is invalid")
    return result


def _decode_union(
    value: object,
    expected_type: object,
    *,
    depth: int,
    budget: _GraphBudget,
) -> object:
    members = get_args(expected_type)
    if value is None and type(None) in members:
        return None
    candidates = tuple(item for item in members if item is not type(None))
    if len(candidates) != 1:
        raise SwingProposalParentStoreError("stored parent union is unsupported")
    return _decode_graph(
        value,
        candidates[0],
        depth=depth,
        budget=budget,
    )


def _decode_tuple(
    value: object,
    expected_type: object,
    *,
    depth: int,
    budget: _GraphBudget,
) -> tuple[object, ...]:
    items = _strict_object(value, {"$tuple"})["$tuple"]
    if type(items) is not list:
        raise SwingProposalParentStoreError("stored parent tuple is invalid")
    arguments = get_args(expected_type)
    if len(arguments) == 2 and arguments[1] is Ellipsis:
        return tuple(
            _decode_graph(
                item,
                arguments[0],
                depth=depth + 1,
                budget=budget,
            )
            for item in items
        )
    if len(arguments) != len(items):
        raise SwingProposalParentStoreError("stored parent tuple has invalid length")
    return tuple(
        _decode_graph(item, item_type, depth=depth + 1, budget=budget)
        for item, item_type in zip(items, arguments, strict=True)
    )


def _decode_dataclass(
    value: object,
    expected_type: type[object],
    *,
    depth: int,
    budget: _GraphBudget,
) -> object:
    raw = _strict_object(value, {"$dataclass", "fields"})
    if raw["$dataclass"] != _qualified_name(expected_type):
        raise SwingProposalParentStoreError("stored parent dataclass type is invalid")
    raw_fields = raw["fields"]
    expected_fields = tuple(fields(expected_type))
    if type(raw_fields) is not dict or set(raw_fields) != {
        item.name for item in expected_fields
    }:
        raise SwingProposalParentStoreError("stored parent dataclass fields are invalid")
    annotations = _annotations(expected_type)
    decoded = {
        item.name: _decode_graph(
            raw_fields[item.name],
            annotations[item.name],
            depth=depth + 1,
            budget=budget,
        )
        for item in expected_fields
    }
    try:
        result = expected_type(
            **{item.name: decoded[item.name] for item in expected_fields if item.init}
        )
    except Exception:
        raise SwingProposalParentStoreError(
            "stored parent dataclass could not be reconstructed"
        ) from None
    if any(
        getattr(result, item.name) != decoded[item.name]
        for item in expected_fields
        if not item.init
    ):
        raise SwingProposalParentStoreError("stored parent computed identity differs")
    verifier = getattr(result, "verify_content_identity", None)
    if verifier is not None:
        try:
            verifier()
        except Exception:
            raise SwingProposalParentStoreError(
                "stored parent content identity failed"
            ) from None
    return result


def _decode_graph(
    value: object,
    expected_type: object,
    *,
    depth: int,
    budget: _GraphBudget,
) -> object:
    budget.consume(depth)
    origin = get_origin(expected_type)
    if origin in (Union, types.UnionType):
        return _decode_union(
            value,
            expected_type,
            depth=depth + 1,
            budget=budget,
        )
    if origin is tuple:
        return _decode_tuple(
            value,
            expected_type,
            depth=depth,
            budget=budget,
        )
    if expected_type is type(None):
        if value is not None:
            raise SwingProposalParentStoreError("stored parent null is invalid")
        return None
    if expected_type in (bool, int, str):
        if type(value) is not expected_type:
            raise SwingProposalParentStoreError("stored parent scalar type is invalid")
        return value
    if expected_type is Decimal:
        return _decode_decimal(value)
    if expected_type is datetime:
        return _decode_datetime(value)
    if expected_type is date:
        return _decode_date(value)
    if isinstance(expected_type, type) and issubclass(expected_type, Enum):
        raw = _strict_object(value, {"$enum", "value"})
        if raw["$enum"] != _qualified_name(expected_type):
            raise SwingProposalParentStoreError("stored parent enum type is invalid")
        try:
            result = expected_type(raw["value"])
        except (TypeError, ValueError):
            raise SwingProposalParentStoreError("stored parent enum is invalid") from None
        if type(result.value) is not type(raw["value"]):
            raise SwingProposalParentStoreError("stored parent enum value is invalid")
        return result
    if isinstance(expected_type, type) and is_dataclass(expected_type):
        return _decode_dataclass(
            value,
            expected_type,
            depth=depth,
            budget=budget,
        )
    raise SwingProposalParentStoreError("stored parent annotation is unsupported")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise SwingProposalParentStoreError("stored parent contains duplicate keys")
        result[key] = value
    return result


def _identity(value: object, kind: SwingProposalParentKind) -> str:
    result = getattr(value, _IDENTITY_FIELD_BY_KIND[kind], None)
    if type(result) is not str or _SHA256.fullmatch(result) is None:
        raise SwingProposalParentStoreError("proposal parent identity is invalid")
    return result


def encode_swing_proposal_parent(
    value: object,
    kind: SwingProposalParentKind,
) -> bytes:
    if type(kind) is not SwingProposalParentKind:
        raise SwingProposalParentStoreError("proposal parent kind must be exact")
    expected_type = _ROOT_TYPE_BY_KIND[kind]
    if type(value) is not expected_type:
        raise SwingProposalParentStoreError("proposal parent root type is invalid")
    verifier = getattr(value, "verify_content_identity", None)
    if verifier is not None:
        try:
            verifier()
        except Exception:
            raise SwingProposalParentStoreError("proposal parent identity failed") from None
    root_id = _identity(value, kind)
    graph = _encode_graph(value, depth=0, budget=_GraphBudget(), stack=set())
    payload = (
        json.dumps(
            {
                "codec_schema_version": PROPOSAL_PARENT_CODEC_VERSION,
                "kind": kind.value,
                "root_id": root_id,
                "value": graph,
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(payload) > _MAXIMUM_BYTES_BY_KIND[kind]:
        raise SwingProposalParentStoreError("proposal parent exceeds its size limit")
    return payload


def decode_swing_proposal_parent(
    payload: bytes,
    kind: SwingProposalParentKind,
) -> object:
    if type(kind) is not SwingProposalParentKind:
        raise SwingProposalParentStoreError("proposal parent kind must be exact")
    if (
        type(payload) is not bytes
        or not payload
        or len(payload) > _MAXIMUM_BYTES_BY_KIND[kind]
    ):
        raise SwingProposalParentStoreError("stored proposal parent is invalid")
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        envelope = _strict_object(
            raw,
            {"codec_schema_version", "kind", "root_id", "value"},
        )
        if (
            envelope["codec_schema_version"] != PROPOSAL_PARENT_CODEC_VERSION
            or envelope["kind"] != kind.value
            or type(envelope["root_id"]) is not str
            or _SHA256.fullmatch(envelope["root_id"]) is None
        ):
            raise SwingProposalParentStoreError("stored parent envelope is invalid")
        result = _decode_graph(
            envelope["value"],
            _ROOT_TYPE_BY_KIND[kind],
            depth=0,
            budget=_GraphBudget(),
        )
        if _identity(result, kind) != envelope["root_id"]:
            raise SwingProposalParentStoreError("stored parent root identity differs")
        if encode_swing_proposal_parent(result, kind) != payload:
            raise SwingProposalParentStoreError("stored parent encoding is not canonical")
        return result
    except SwingProposalParentStoreError:
        raise
    except (
        InvalidOperation,
        KeyError,
        RecursionError,
        TypeError,
        UnicodeDecodeError,
        ValueError,
        json.JSONDecodeError,
    ):
        raise SwingProposalParentStoreError("stored proposal parent is invalid") from None


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


class LocalSwingProposalParentStore(SwingProposalBatchInputResolver):
    """Restart-safe exact-ID resolver for the three proposal parent roots."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def parents_root(self) -> Path:
        return self.root / "proposal_parents"

    def path_for(self, kind: SwingProposalParentKind, root_id: str) -> Path:
        if type(kind) is not SwingProposalParentKind:
            raise SwingProposalParentStoreError("proposal parent kind must be exact")
        if type(root_id) is not str or _SHA256.fullmatch(root_id) is None:
            raise SwingProposalParentStoreError("proposal parent ID is invalid")
        return self.parents_root / kind.value / f"{root_id}.json"

    def _prepare_kind_root(self, kind: SwingProposalParentKind) -> Path:
        try:
            self.root.mkdir(parents=True, exist_ok=True)
        except OSError:
            raise SwingProposalParentStoreError("proposal parent store is unavailable") from None
        if _is_link_like(self.root):
            raise SwingProposalParentStoreError("proposal parent store root cannot be a link")
        try:
            self.parents_root.mkdir(exist_ok=True)
        except OSError:
            raise SwingProposalParentStoreError("proposal parent store is unavailable") from None
        if _is_link_like(self.parents_root):
            raise SwingProposalParentStoreError("proposal parent store root cannot be a link")
        kind_root = self.parents_root / kind.value
        try:
            kind_root.mkdir(exist_ok=True)
        except OSError:
            raise SwingProposalParentStoreError("proposal parent store is unavailable") from None
        if _is_link_like(kind_root):
            raise SwingProposalParentStoreError("proposal parent store root cannot be a link")
        self._assert_kind_root(kind)
        return kind_root

    def _assert_kind_root(self, kind: SwingProposalParentKind) -> Path:
        if type(kind) is not SwingProposalParentKind:
            raise SwingProposalParentStoreError("proposal parent kind must be exact")
        kind_root = self.parents_root / kind.value
        try:
            paths = (self.root, self.parents_root, kind_root)
            if any(
                not path.exists() or not path.is_dir() or _is_link_like(path)
                for path in paths
            ):
                raise SwingProposalParentStoreError(
                    "proposal parent store root is unsafe"
                )
            resolved_root = self.root.resolve()
            resolved_parents = self.parents_root.resolve()
            resolved_kind = kind_root.resolve()
        except SwingProposalParentStoreError:
            raise
        except OSError:
            raise SwingProposalParentStoreError(
                "proposal parent store is unavailable"
            ) from None
        if (
            resolved_parents != resolved_root / "proposal_parents"
            or resolved_kind != resolved_parents / kind.value
        ):
            raise SwingProposalParentStoreError("proposal parent store root is unsafe")
        return kind_root

    def put(self, value: object, kind: SwingProposalParentKind) -> object:
        payload = encode_swing_proposal_parent(value, kind)
        root_id = _identity(value, kind)
        kind_root = self._prepare_kind_root(kind)
        target = self.path_for(kind, root_id)
        try:
            with advisory_file_lock(self.parents_root / ".proposal-parents.lock"):
                if target.exists():
                    stored = self.get(kind, root_id)
                    if stored != value:
                        raise SwingProposalParentStoreError(
                            "proposal parent ID already stores different content"
                        )
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".proposal-parent-",
                    suffix=".tmp",
                    dir=kind_root,
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
        except SwingProposalParentStoreError:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise SwingProposalParentStoreError(
                "proposal parent could not be published"
            ) from None
        return self.get(kind, root_id)

    def get(self, kind: SwingProposalParentKind, root_id: str) -> object:
        path = self.path_for(kind, root_id)
        kind_root = self.parents_root / kind.value
        try:
            kind_exists = kind_root.exists()
        except OSError:
            raise SwingProposalParentStoreError("proposal parent store is unavailable") from None
        if not kind_exists:
            raise SwingProposalParentNotFound("proposal parent was not found")
        self._assert_kind_root(kind)
        try:
            payload = read_stable_regular_file(
                path,
                maximum_bytes=_MAXIMUM_BYTES_BY_KIND[kind],
            )
        except FileNotFoundError:
            raise SwingProposalParentNotFound("proposal parent was not found") from None
        except FileSafetyError:
            try:
                exists = path.exists()
            except OSError:
                raise SwingProposalParentStoreError(
                    "proposal parent store is unavailable"
                ) from None
            if not exists:
                raise SwingProposalParentNotFound("proposal parent was not found") from None
            raise SwingProposalParentStoreError(
                "proposal parent could not be read safely"
            ) from None
        value = decode_swing_proposal_parent(payload, kind)
        if _identity(value, kind) != root_id:
            raise SwingProposalParentStoreError("proposal parent path differs from content")
        return value

    def put_universe_batch(self, value: SwingUniverseInputBatch) -> SwingUniverseInputBatch:
        result = self.put(value, SwingProposalParentKind.UNIVERSE_BATCH)
        if type(result) is not SwingUniverseInputBatch:
            raise SwingProposalParentStoreError("stored universe batch type differs")
        return result

    def put_calendar_snapshot(self, value: CalendarSnapshot) -> CalendarSnapshot:
        result = self.put(value, SwingProposalParentKind.CALENDAR_SNAPSHOT)
        if type(result) is not CalendarSnapshot:
            raise SwingProposalParentStoreError("stored calendar type differs")
        return result

    def put_signal_config(
        self, value: DeterministicSwingSignalConfig
    ) -> DeterministicSwingSignalConfig:
        result = self.put(value, SwingProposalParentKind.SIGNAL_CONFIG)
        if type(result) is not DeterministicSwingSignalConfig:
            raise SwingProposalParentStoreError("stored signal config type differs")
        return result

    def get_universe_batch(self, universe_batch_id: str) -> SwingUniverseInputBatch:
        result = self.get(SwingProposalParentKind.UNIVERSE_BATCH, universe_batch_id)
        if type(result) is not SwingUniverseInputBatch:
            raise SwingProposalParentStoreError("stored universe batch type differs")
        return result

    def get_calendar_snapshot(self, calendar_snapshot_id: str) -> CalendarSnapshot:
        result = self.get(SwingProposalParentKind.CALENDAR_SNAPSHOT, calendar_snapshot_id)
        if type(result) is not CalendarSnapshot:
            raise SwingProposalParentStoreError("stored calendar type differs")
        return result

    def get_signal_config(
        self, signal_config_id: str
    ) -> DeterministicSwingSignalConfig:
        result = self.get(SwingProposalParentKind.SIGNAL_CONFIG, signal_config_id)
        if type(result) is not DeterministicSwingSignalConfig:
            raise SwingProposalParentStoreError("stored signal config type differs")
        return result


def publish_swing_proposal_with_parents(
    *,
    batch: SwingProposalBatch,
    proposal_store: LocalSwingProposalBatchStore,
    parent_store: LocalSwingProposalParentStore,
) -> SwingProposalBatchManifest:
    """Seal all exact parents first and the small proposal manifest last."""

    if type(batch) is not SwingProposalBatch:
        raise SwingProposalParentStoreError("proposal batch must be exact")
    if type(proposal_store) is not LocalSwingProposalBatchStore:
        raise SwingProposalParentStoreError("proposal store must be exact")
    if type(parent_store) is not LocalSwingProposalParentStore:
        raise SwingProposalParentStoreError("parent store must be exact")
    try:
        batch.verify_content_identity()
        parent_store.put_universe_batch(batch.universe_batch)
        parent_store.put_calendar_snapshot(batch.calendar)
        parent_store.put_signal_config(batch.config)
        manifest = proposal_store.publish(batch)
        rebuilt = proposal_store.require_persisted(batch, parent_store)
    except SwingProposalArtifactError:
        raise
    except Exception:
        raise SwingProposalParentStoreError(
            "proposal artifact graph could not be published safely"
        ) from None
    if rebuilt.batch_id != batch.batch_id:
        raise SwingProposalParentStoreError("published proposal graph differs")
    return manifest
