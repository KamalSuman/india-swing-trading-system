"""Durable, exact-ID store for CorporateActionSnapshot.

CorporateActionSnapshot has no lower durable source: this is a leaf/root
persistence boundary only. It never claims that successful storage
independently proves official provenance, and it never upgrades or
downgrades whatever valid readiness/complete/actionable/reason state the
source snapshot already carries. ``get`` never trusts a stored ID: it
strictly decodes every field and constructs genuine ``CorporateActionEvent``/
``CorporateActionSnapshot`` values, so their own construction-time
validation and content identity independently replay.
"""

from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.reference.models import ReferenceReadiness

from .models import (
    CorporateActionEvent,
    CorporateActionIntegrityError,
    CorporateActionSnapshot,
    CorporateActionStatus,
    CorporateActionType,
)


class CorporateActionSnapshotStoreError(ValueError):
    pass


class CorporateActionSnapshotStoreConflict(CorporateActionSnapshotStoreError):
    pass


class CorporateActionSnapshotStoreNotFound(CorporateActionSnapshotStoreError):
    pass


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_CANONICAL_DATE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
_CURRENCY = re.compile(r"[A-Z]{3}\Z")
_REASON_CODE = re.compile(r"[A-Z][A-Z0-9_]{2,127}\Z")

CORPORATE_ACTION_SNAPSHOT_STORE_CODEC_VERSION = "corporate-action-snapshot-store-json/v1"
_KIND = "CORPORATE_ACTION_SNAPSHOT"
_MAXIMUM_MANIFEST_BYTES = 8 * 1024 * 1024
_MAXIMUM_LIST_LENGTH = 100_000
_STORE_DIRECTORY = "corporate-action-snapshots"
_LOCK_FILENAME = ".corporate-action-snapshot.lock"

_EVENT_KEYS = frozenset(
    {
        "schema_version",
        "event_id",
        "stable_instrument_id",
        "stable_listing_id",
        "action_type",
        "status",
        "effective_session",
        "announcement_time",
        "knowledge_time",
        "source_artifact_id",
        "source_row_id",
        "pre_action_shares",
        "post_action_shares",
        "cash_amount_per_share",
        "currency",
        "supersedes_event_id",
    }
)
_SNAPSHOT_KEYS = frozenset(
    {
        "codec_schema_version",
        "kind",
        "snapshot_id",
        "schema_version",
        "policy_version",
        "cutoff",
        "coverage_start",
        "coverage_end",
        "source_artifact_ids",
        "readiness",
        "complete",
        "actionable",
        "reason_codes",
        "events",
    }
)

_ERR_TYPE = "corporate action snapshot store type is invalid"
_ERR_VERIFY = (
    "corporate action snapshot store could not verify the supplied snapshot"
)
_ERR_PATH_IDENTITY = "corporate action snapshot store path identity is invalid"
_ERR_CONFLICT = "corporate action snapshot store already has different content"
_ERR_STORE_UNAVAILABLE = "corporate action snapshot store is unavailable"
_ERR_UNSAFE_PATH = "corporate action snapshot store path is unsafe"
_ERR_NOT_FOUND = "corporate action snapshot was not found"
_ERR_BYTES = "corporate action snapshot manifest bytes are invalid"
_ERR_UTF8 = "corporate action snapshot manifest is not valid UTF-8"
_ERR_JSON = "corporate action snapshot manifest is not valid JSON"
_ERR_SHAPE = "corporate action snapshot manifest shape is invalid"
_ERR_NONCANONICAL = "corporate action snapshot manifest is not canonical"


def _require_sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)
    return value


def _optional_sha(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _require_sha(value, name)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise CorporateActionSnapshotStoreError(_ERR_SHAPE)
        result[key] = value
    return result


def _reject_number(_token: str) -> object:
    raise CorporateActionSnapshotStoreError(_ERR_SHAPE)


def _canonical_bytes(value: dict[str, object]) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _canonical_date(value: object) -> date:
    if type(value) is not str or _CANONICAL_DATE.fullmatch(value) is None:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)
    try:
        result = date.fromisoformat(value)
    except ValueError:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE) from None
    if result.isoformat() != value:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)
    return result


def _canonical_datetime(value: object) -> datetime:
    if type(value) is not str:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)
    try:
        result = datetime.fromisoformat(value)
    except ValueError:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE) from None
    offset = result.utcoffset()
    if (
        result.tzinfo is None
        or offset is None
        or offset.total_seconds() != 0
        or result.isoformat() != value
    ):
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)
    return result


def _optional_decimal(value: object, name: str) -> Decimal | None:
    if value is None:
        return None
    # Exact canonical-text validation, not a restrictive shape regex: every
    # Decimal term this schema carries is required strictly positive by
    # CorporateActionEvent's own validation, so this accepts every positive
    # representation Decimal can produce via str(Decimal(...)) -- including
    # canonical scientific-notation forms such as "1E+2" or "1E-7" -- and
    # rejects only non-finite, non-positive (zero or negative, which also
    # catches every signed-zero spelling), and noncanonical-round-trip
    # values. No float is used anywhere in this parse.
    if type(value) is not str:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)
    try:
        result = Decimal(value)
    except InvalidOperation:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE) from None
    if not result.is_finite() or result <= 0:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)
    if str(result) != value:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)
    return result


def _optional_currency(value: object) -> str | None:
    if value is None:
        return None
    if type(value) is not str or _CURRENCY.fullmatch(value) is None:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)
    return value


def _is_link_like(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    if path.is_symlink() or bool(is_junction and is_junction()):
        return True
    try:
        status = os.lstat(path)
    except OSError:
        return False
    attribute = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool(getattr(status, "st_file_attributes", 0) & attribute)


def _encode_event(event: CorporateActionEvent) -> dict[str, object]:
    return {
        "schema_version": event.schema_version,
        "event_id": event.event_id,
        "stable_instrument_id": event.stable_instrument_id,
        "stable_listing_id": event.stable_listing_id,
        "action_type": event.action_type.value,
        "status": event.status.value,
        "effective_session": event.effective_session.isoformat(),
        "announcement_time": event.announcement_time.isoformat(),
        "knowledge_time": event.knowledge_time.isoformat(),
        "source_artifact_id": event.source_artifact_id,
        "source_row_id": event.source_row_id,
        "pre_action_shares": (
            None if event.pre_action_shares is None else str(event.pre_action_shares)
        ),
        "post_action_shares": (
            None if event.post_action_shares is None else str(event.post_action_shares)
        ),
        "cash_amount_per_share": (
            None
            if event.cash_amount_per_share is None
            else str(event.cash_amount_per_share)
        ),
        "currency": event.currency,
        "supersedes_event_id": event.supersedes_event_id,
    }


def encode_corporate_action_snapshot(snapshot: CorporateActionSnapshot) -> bytes:
    if type(snapshot) is not CorporateActionSnapshot:
        raise TypeError("corporate action snapshot must be exact")
    return _canonical_bytes(
        {
            "codec_schema_version": CORPORATE_ACTION_SNAPSHOT_STORE_CODEC_VERSION,
            "kind": _KIND,
            "snapshot_id": snapshot.snapshot_id,
            "schema_version": snapshot.schema_version,
            "policy_version": snapshot.policy_version,
            "cutoff": snapshot.cutoff.isoformat(),
            "coverage_start": snapshot.coverage_start.isoformat(),
            "coverage_end": snapshot.coverage_end.isoformat(),
            "source_artifact_ids": list(snapshot.source_artifact_ids),
            "readiness": snapshot.readiness.value,
            "complete": snapshot.complete,
            "actionable": snapshot.actionable,
            "reason_codes": list(snapshot.reason_codes),
            "events": [_encode_event(value) for value in snapshot.events],
        }
    )


def _decode_event(value: object) -> CorporateActionEvent:
    if type(value) is not dict or set(value) != _EVENT_KEYS:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)

    stored_event_id = _require_sha(value["event_id"], "event_id")
    schema_version = value["schema_version"]
    if type(schema_version) is not str:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)

    stable_instrument_id = _require_sha(
        value["stable_instrument_id"], "stable_instrument_id"
    )
    stable_listing_id = _optional_sha(value["stable_listing_id"], "stable_listing_id")

    action_type_raw = value["action_type"]
    if type(action_type_raw) is not str:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)
    try:
        action_type = CorporateActionType(action_type_raw)
    except ValueError:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE) from None

    status_raw = value["status"]
    if type(status_raw) is not str:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)
    try:
        status = CorporateActionStatus(status_raw)
    except ValueError:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE) from None

    effective_session = _canonical_date(value["effective_session"])
    announcement_time = _canonical_datetime(value["announcement_time"])
    knowledge_time = _canonical_datetime(value["knowledge_time"])
    source_artifact_id = _require_sha(value["source_artifact_id"], "source_artifact_id")
    source_row_id = _require_sha(value["source_row_id"], "source_row_id")
    pre_action_shares = _optional_decimal(value["pre_action_shares"], "pre_action_shares")
    post_action_shares = _optional_decimal(
        value["post_action_shares"], "post_action_shares"
    )
    cash_amount_per_share = _optional_decimal(
        value["cash_amount_per_share"], "cash_amount_per_share"
    )
    currency = _optional_currency(value["currency"])
    supersedes_event_id = _optional_sha(
        value["supersedes_event_id"], "supersedes_event_id"
    )

    try:
        event = CorporateActionEvent(
            stable_instrument_id=stable_instrument_id,
            stable_listing_id=stable_listing_id,
            action_type=action_type,
            status=status,
            effective_session=effective_session,
            announcement_time=announcement_time,
            knowledge_time=knowledge_time,
            source_artifact_id=source_artifact_id,
            source_row_id=source_row_id,
            pre_action_shares=pre_action_shares,
            post_action_shares=post_action_shares,
            cash_amount_per_share=cash_amount_per_share,
            currency=currency,
            supersedes_event_id=supersedes_event_id,
            schema_version=schema_version,
        )
    except CorporateActionIntegrityError:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE) from None
    except Exception:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE) from None

    if event.event_id != stored_event_id:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)
    return event


def decode_corporate_action_snapshot(payload: bytes) -> CorporateActionSnapshot:
    """The single strict corporate-action-snapshot decoder.

    Rejects duplicate keys, float/NaN/Infinity tokens, unknown/missing
    fields, malformed hashes/dates/aware-UTC datetimes/enums/optionals/
    sequences, bool-as-int, noncanonical Decimal/signed-zero terms,
    oversized/empty/non-bytes input, and noncanonical JSON. Constructs exact
    CorporateActionEvent and CorporateActionSnapshot values so their own
    validation and content identities independently replay -- a stored
    event_id/snapshot_id is never trusted on its own, only cross-checked
    against the freshly (re)computed identity.
    """

    if type(payload) is not bytes or not payload or len(payload) > _MAXIMUM_MANIFEST_BYTES:
        raise CorporateActionSnapshotStoreError(_ERR_BYTES)
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise CorporateActionSnapshotStoreError(_ERR_UTF8) from None

    try:
        decoded = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
    except CorporateActionSnapshotStoreError:
        raise
    except (json.JSONDecodeError, RecursionError):
        raise CorporateActionSnapshotStoreError(_ERR_JSON) from None

    if type(decoded) is not dict or set(decoded) != _SNAPSHOT_KEYS:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)
    if (
        decoded["codec_schema_version"] != CORPORATE_ACTION_SNAPSHOT_STORE_CODEC_VERSION
        or decoded["kind"] != _KIND
    ):
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)

    stored_snapshot_id = _require_sha(decoded["snapshot_id"], "snapshot_id")
    schema_version = decoded["schema_version"]
    policy_version = decoded["policy_version"]
    if type(schema_version) is not str or type(policy_version) is not str:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)

    cutoff = _canonical_datetime(decoded["cutoff"])
    coverage_start = _canonical_date(decoded["coverage_start"])
    coverage_end = _canonical_date(decoded["coverage_end"])

    raw_source_ids = decoded["source_artifact_ids"]
    if type(raw_source_ids) is not list or len(raw_source_ids) > _MAXIMUM_LIST_LENGTH:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)
    source_artifact_ids = tuple(
        _require_sha(item, "source_artifact_id") for item in raw_source_ids
    )

    readiness_raw = decoded["readiness"]
    if type(readiness_raw) is not str:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)
    try:
        readiness = ReferenceReadiness(readiness_raw)
    except ValueError:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE) from None

    complete = decoded["complete"]
    actionable = decoded["actionable"]
    if type(complete) is not bool or type(actionable) is not bool:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)

    raw_reason_codes = decoded["reason_codes"]
    if type(raw_reason_codes) is not list or len(raw_reason_codes) > _MAXIMUM_LIST_LENGTH:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)
    reason_codes_list = []
    for item in raw_reason_codes:
        if type(item) is not str or _REASON_CODE.fullmatch(item) is None:
            raise CorporateActionSnapshotStoreError(_ERR_SHAPE)
        reason_codes_list.append(item)
    reason_codes = tuple(reason_codes_list)

    raw_events = decoded["events"]
    if type(raw_events) is not list or len(raw_events) > _MAXIMUM_LIST_LENGTH:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)
    events = tuple(_decode_event(item) for item in raw_events)

    try:
        snapshot = CorporateActionSnapshot(
            cutoff=cutoff,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            source_artifact_ids=source_artifact_ids,
            events=events,
            readiness=readiness,
            complete=complete,
            actionable=actionable,
            reason_codes=reason_codes,
            policy_version=policy_version,
            schema_version=schema_version,
        )
    except CorporateActionIntegrityError:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE) from None
    except Exception:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE) from None

    if snapshot.snapshot_id != stored_snapshot_id:
        raise CorporateActionSnapshotStoreError(_ERR_SHAPE)
    if encode_corporate_action_snapshot(snapshot) != payload:
        raise CorporateActionSnapshotStoreError(_ERR_NONCANONICAL)
    return snapshot


class LocalCorporateActionSnapshotStore:
    """Durable exact-ID store for CorporateActionSnapshot.

    Exposes only ``put``, ``get``, and ``path_for`` -- no list/latest/
    nearest/find/discovery operation of any kind. Persistence neither
    upgrades nor downgrades whatever valid readiness/complete/actionable/
    reason state the source snapshot already carries, and it is not
    independent official-source provenance: a self-consistent stored
    snapshot proves only that it was durably persisted, never that its
    underlying corporate-action claims are officially verified.
    """

    def __init__(self, root: Path) -> None:
        self.root = Path(root) / _STORE_DIRECTORY

    def path_for(self, snapshot_id: str) -> Path:
        return self.root / f"{_require_sha(snapshot_id, 'snapshot_id')}.json"

    def put(self, value: CorporateActionSnapshot) -> CorporateActionSnapshot:
        if type(value) is not CorporateActionSnapshot:
            raise TypeError("corporate action snapshot must be an exact CorporateActionSnapshot")
        try:
            value.verify_content_identity()
        except Exception:
            raise CorporateActionSnapshotStoreError(_ERR_VERIFY) from None

        payload = encode_corporate_action_snapshot(value)
        # Prove the codec can reconstruct this exact content before writing
        # anything: an input the decoder cannot faithfully replay (for
        # example a value whose encoding this store's own decoder would
        # reject) must leave no target artifact behind rather than
        # publishing an artifact that later fails to read back.
        try:
            replayed = decode_corporate_action_snapshot(payload)
        except CorporateActionSnapshotStoreError:
            raise
        except Exception:
            raise CorporateActionSnapshotStoreError(_ERR_VERIFY) from None
        if replayed != value or replayed.snapshot_id != value.snapshot_id:
            raise CorporateActionSnapshotStoreError(_ERR_VERIFY)

        self._publish(value.snapshot_id, payload)
        return self.get(value.snapshot_id)

    def get(self, snapshot_id: str) -> CorporateActionSnapshot:
        _require_sha(snapshot_id, "snapshot_id")
        path = self.path_for(snapshot_id)
        payload = self._read(path)
        snapshot = decode_corporate_action_snapshot(payload)
        if snapshot.snapshot_id != snapshot_id:
            raise CorporateActionSnapshotStoreError(_ERR_PATH_IDENTITY)
        return snapshot

    def _read(self, path: Path) -> bytes:
        if not path.exists():
            raise CorporateActionSnapshotStoreNotFound(_ERR_NOT_FOUND)
        if not path.is_file() or _is_link_like(path):
            raise CorporateActionSnapshotStoreError(_ERR_UNSAFE_PATH)
        try:
            return read_stable_regular_file(path, maximum_bytes=_MAXIMUM_MANIFEST_BYTES)
        except FileSafetyError:
            raise CorporateActionSnapshotStoreError(_ERR_UNSAFE_PATH) from None

    def _publish(self, snapshot_id: str, payload: bytes) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        if not self.root.is_dir() or _is_link_like(self.root):
            raise CorporateActionSnapshotStoreError(_ERR_UNSAFE_PATH)
        target = self.path_for(snapshot_id)
        try:
            with advisory_file_lock(self.root / _LOCK_FILENAME):
                if target.exists():
                    if _is_link_like(target) or self._read(target) != payload:
                        raise CorporateActionSnapshotStoreConflict(_ERR_CONFLICT)
                    return
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".corporate-action-snapshot-",
                    suffix=".tmp",
                    dir=self.root,
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
        except CorporateActionSnapshotStoreConflict:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise CorporateActionSnapshotStoreConflict(_ERR_STORE_UNAVAILABLE) from None
