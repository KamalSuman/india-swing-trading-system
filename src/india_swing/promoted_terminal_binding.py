"""Pure artifact for the independent trusted-terminal-binding control plane.

Defines the sealed binding record (``spec_id -> expected_terminal_id``), its
strict canonical JSON codec, the deterministic spec-derived object name, the
pure builder that derives a record only from an already-verified terminal
and run spec, and the projection into the existing
``TrustedPromotedOperationalTerminalBinding``. Contains no storage port, no
adapter, and no GCS-shaped code at all -- see
``promoted_terminal_binding_control_plane.py`` for that.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timedelta

from india_swing.identity import content_id
from india_swing.promoted_operational_persistence import (
    PromotedOperationalTerminalRecord,
    _verify_terminal_matches_spec,
)
from india_swing.promoted_operational_runner import PromotedOperationalRunSpec
from india_swing.promoted_operational_service import TrustedPromotedOperationalTerminalBinding


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")

TERMINAL_BINDING_SCHEMA_VERSION = "promoted-operational-terminal-binding/v1"
_TERMINAL_BINDING_CODEC_SCHEMA_VERSION = "promoted-operational-terminal-binding-json/v1"
MAXIMUM_TERMINAL_BINDING_BYTES = 4096

_OBJECT_NAME_PREFIX = "promoted-operational/terminal-bindings"


class PromotedTerminalBindingError(ValueError):
    pass


_ERR_TYPE = "promoted terminal binding type is invalid"
_ERR_RECORD = "promoted terminal binding record is invalid"
_ERR_PAYLOAD = "promoted terminal binding payload is invalid"


def _require_sha(value: object) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise PromotedTerminalBindingError(_ERR_RECORD)
    return value


@dataclass(frozen=True, slots=True, kw_only=True)
class PromotedOperationalTerminalBindingRecord:
    """One sealed, content-addressed binding of a run spec to the exact
    terminal_id an independent control plane observed at seal time.

    Not cryptographic authentication or independent provenance: its
    authority comes from create-once object immutability plus
    independently observed generation pinning at the control-plane layer,
    never from this record's own content hash alone. A self-consistent
    forged record whose binding_id recomputes correctly and whose spec
    cross-checks pass will be accepted by the load path -- the real
    defense is that a binding object for a given spec can never be
    replaced once sealed.
    """

    schema_version: str = TERMINAL_BINDING_SCHEMA_VERSION
    spec_id: str
    target_session: date
    preparation_id: str
    expected_terminal_id: str
    terminal_completed_at: datetime
    binding_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._verify()
        object.__setattr__(self, "binding_id", self._calculated_id())

    def _verify(self) -> None:
        if self.schema_version != TERMINAL_BINDING_SCHEMA_VERSION:
            raise PromotedTerminalBindingError(_ERR_RECORD)
        _require_sha(self.spec_id)
        if type(self.target_session) is not date:
            raise PromotedTerminalBindingError(_ERR_RECORD)
        _require_sha(self.preparation_id)
        _require_sha(self.expected_terminal_id)
        if type(self.terminal_completed_at) is not datetime:
            raise PromotedTerminalBindingError(_ERR_RECORD)
        offset_failed = False
        offset = None
        try:
            offset = self.terminal_completed_at.utcoffset()
        except Exception:
            offset_failed = True
        if offset_failed:
            raise PromotedTerminalBindingError(_ERR_RECORD)
        if self.terminal_completed_at.tzinfo is None or offset != timedelta(0):
            raise PromotedTerminalBindingError(_ERR_RECORD)

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
        self._verify()
        if self.binding_id != self._calculated_id():
            raise PromotedTerminalBindingError(_ERR_RECORD)


def build_promoted_operational_terminal_binding_record(
    terminal: PromotedOperationalTerminalRecord,
    spec: PromotedOperationalRunSpec,
) -> PromotedOperationalTerminalBindingRecord:
    """Pure builder: derive one exact binding record from an already-
    verified terminal and its exact live run spec. Never reads a clock,
    never accepts a caller-supplied binding_id, and never invents,
    defaults, coerces, or reformats any value."""

    if type(terminal) is not PromotedOperationalTerminalRecord:
        raise PromotedTerminalBindingError(_ERR_TYPE)
    if type(spec) is not PromotedOperationalRunSpec:
        raise PromotedTerminalBindingError(_ERR_TYPE)
    verify_failed = False
    try:
        terminal.verify_content_identity()
        spec.verify_content_identity()
        _verify_terminal_matches_spec(terminal, spec)
    except Exception:
        verify_failed = True
    if verify_failed:
        raise PromotedTerminalBindingError(_ERR_RECORD)

    manifest = spec.quote_gate_spec.preparation.manifest
    return PromotedOperationalTerminalBindingRecord(
        spec_id=spec.spec_id,
        target_session=manifest.target_session,
        preparation_id=manifest.preparation_id,
        expected_terminal_id=terminal.terminal_id,
        terminal_completed_at=terminal.completed_at,
    )


def promoted_operational_terminal_binding_object_name(
    spec: PromotedOperationalRunSpec,
) -> str:
    """The exact, deterministic binding object name for one run spec.

    Derived ONLY from the verified live spec -- never from a terminal
    record, a binding record, or any free-form string -- so the read
    path's safety depends on the path being fixed by the caller's live
    spec rather than by any stored artifact.
    """

    if type(spec) is not PromotedOperationalRunSpec:
        raise PromotedTerminalBindingError(_ERR_TYPE)
    verify_failed = False
    target_session: date | None = None
    try:
        spec.verify_content_identity()
        target_session = spec.quote_gate_spec.preparation.manifest.target_session
    except Exception:
        verify_failed = True
    if verify_failed or type(target_session) is not date:
        raise PromotedTerminalBindingError(_ERR_RECORD)
    return f"{_OBJECT_NAME_PREFIX}/{target_session.isoformat()}/{spec.spec_id}.json"


def trusted_binding_from_record(
    record: PromotedOperationalTerminalBindingRecord,
    spec: PromotedOperationalRunSpec,
) -> TrustedPromotedOperationalTerminalBinding:
    if type(record) is not PromotedOperationalTerminalBindingRecord:
        raise PromotedTerminalBindingError(_ERR_TYPE)
    if type(spec) is not PromotedOperationalRunSpec:
        raise PromotedTerminalBindingError(_ERR_TYPE)
    verify_failed = False
    manifest: object = None
    try:
        record.verify_content_identity()
        spec.verify_content_identity()
        manifest = spec.quote_gate_spec.preparation.manifest
    except Exception:
        verify_failed = True
    if verify_failed or manifest is None:
        raise PromotedTerminalBindingError(_ERR_RECORD)
    if (
        record.spec_id != spec.spec_id
        or record.target_session != manifest.target_session
        or record.preparation_id != manifest.preparation_id
    ):
        raise PromotedTerminalBindingError(_ERR_RECORD)
    return TrustedPromotedOperationalTerminalBinding(
        spec_id=record.spec_id, expected_terminal_id=record.expected_terminal_id
    )


_ENVELOPE_KEYS = frozenset({"codec_schema_version", "terminal_binding"})
_BODY_KEYS = frozenset(
    {
        "schema_version",
        "spec_id",
        "target_session",
        "preparation_id",
        "expected_terminal_id",
        "terminal_completed_at",
        "binding_id",
    }
)


def _binding_body(value: PromotedOperationalTerminalBindingRecord) -> dict[str, object]:
    return {
        "schema_version": value.schema_version,
        "spec_id": value.spec_id,
        "target_session": value.target_session.isoformat(),
        "preparation_id": value.preparation_id,
        "expected_terminal_id": value.expected_terminal_id,
        "terminal_completed_at": value.terminal_completed_at.isoformat(),
        "binding_id": value.binding_id,
    }


def encode_promoted_operational_terminal_binding_record(
    value: PromotedOperationalTerminalBindingRecord,
) -> bytes:
    if type(value) is not PromotedOperationalTerminalBindingRecord:
        raise PromotedTerminalBindingError(_ERR_TYPE)
    value.verify_content_identity()
    payload = (
        json.dumps(
            {
                "codec_schema_version": _TERMINAL_BINDING_CODEC_SCHEMA_VERSION,
                "terminal_binding": _binding_body(value),
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(payload) > MAXIMUM_TERMINAL_BINDING_BYTES:
        raise PromotedTerminalBindingError(_ERR_PAYLOAD)
    return payload


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedTerminalBindingError(_ERR_PAYLOAD)
        result[key] = value
    return result


def _reject_number(_token: str) -> object:
    raise PromotedTerminalBindingError(_ERR_PAYLOAD)


def decode_promoted_operational_terminal_binding_record(
    payload: bytes,
) -> PromotedOperationalTerminalBindingRecord:
    if (
        type(payload) is not bytes
        or not payload
        or len(payload) > MAXIMUM_TERMINAL_BINDING_BYTES
    ):
        raise PromotedTerminalBindingError(_ERR_PAYLOAD)
    decode_failed = False
    text = ""
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        decode_failed = True
    if decode_failed:
        raise PromotedTerminalBindingError(_ERR_PAYLOAD)

    parse_failed = False
    reraise: PromotedTerminalBindingError | None = None
    root: object = None
    try:
        root = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
    except PromotedTerminalBindingError as error:
        reraise = error
    except (json.JSONDecodeError, RecursionError):
        parse_failed = True
    if reraise is not None:
        raise reraise
    if parse_failed:
        raise PromotedTerminalBindingError(_ERR_PAYLOAD)

    if (
        type(root) is not dict
        or set(root) != _ENVELOPE_KEYS
        or root["codec_schema_version"] != _TERMINAL_BINDING_CODEC_SCHEMA_VERSION
    ):
        raise PromotedTerminalBindingError(_ERR_PAYLOAD)
    raw = root["terminal_binding"]
    if type(raw) is not dict or set(raw) != _BODY_KEYS:
        raise PromotedTerminalBindingError(_ERR_PAYLOAD)

    construction_failed = False
    construction_reraise: PromotedTerminalBindingError | None = None
    value: PromotedOperationalTerminalBindingRecord | None = None
    try:
        value = PromotedOperationalTerminalBindingRecord(
            schema_version=raw["schema_version"],
            spec_id=raw["spec_id"],
            target_session=date.fromisoformat(raw["target_session"]),
            preparation_id=raw["preparation_id"],
            expected_terminal_id=raw["expected_terminal_id"],
            terminal_completed_at=datetime.fromisoformat(raw["terminal_completed_at"]),
        )
    except PromotedTerminalBindingError as error:
        construction_reraise = error
    except Exception:
        construction_failed = True
    if construction_reraise is not None:
        raise construction_reraise
    if construction_failed or value is None:
        raise PromotedTerminalBindingError(_ERR_PAYLOAD)
    if value.binding_id != raw["binding_id"]:
        raise PromotedTerminalBindingError(_ERR_PAYLOAD)
    if encode_promoted_operational_terminal_binding_record(value) != payload:
        raise PromotedTerminalBindingError(_ERR_PAYLOAD)
    return value
