"""Pure, deterministic control-spec boundary for the promoted operational
Cloud Run wrapper.

Defines one exact, content-addressed ``PromotedOperationalCloudRunControl``
that binds the expected assembly-spec/operational-run-spec identities, the
target session, the state bucket, the absolute non-traversing assembly-spec
path, all twelve exact path arguments already required by
``promoted_operational_job`` (ten preparation roots, the portfolio-artifact
root, the state root), and an optional externally pinned
``PromotedOperationalGCSRestoreRequest``, plus a strict canonical JSON codec
for it.

This module never touches the filesystem, environment, wall clock, network,
or any GCP/Kite capability. It never lists, resolves, or infers a
relationship between paths beyond pure ``pathlib.Path`` component
comparison -- no path is ever opened, stat'd, or resolved here. It never
discovers, selects latest, or synthesizes a prior state: a restore is either
an exact externally pinned request or exactly absent. It never reproduces
any assembly, strategy, quote, allocation, risk, sizing, terminal-binding,
advisory, registration, or GCS-state codec -- it only composes the
already-accepted identity fields and the already-accepted
``PromotedOperationalGCSRestoreRequest`` type.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, fields
from datetime import date
from pathlib import Path

from india_swing.identity import content_id
from india_swing.promoted_operational_gcs_state import PromotedOperationalGCSRestoreRequest


class PromotedOperationalCloudControlError(ValueError):
    pass


_ERR_CONTROL = "promoted operational cloud control is invalid"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]\Z")

PROMOTED_OPERATIONAL_CLOUD_CONTROL_SCHEMA_VERSION = "promoted-operational-cloud-control/v1"
_CONTROL_CODEC_SCHEMA_VERSION = "promoted-operational-cloud-control-json/v1"
MAXIMUM_CLOUD_CONTROL_BYTES = 128 * 1024

_CONCRETE_PATH_TYPE = type(Path())

_PATH_FIELD_NAMES = (
    "assembly_spec_file",
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
    "state_root",
)


def _require_path(value: object) -> Path:
    if type(value) is not _CONCRETE_PATH_TYPE:
        raise PromotedOperationalCloudControlError(_ERR_CONTROL)
    if not value.is_absolute() or ".." in value.parts:
        raise PromotedOperationalCloudControlError(_ERR_CONTROL)
    return value


def _paths_overlap(first: Path, second: Path) -> bool:
    """Pure lexical containment check over already-split path components --
    never touches the filesystem. Two paths overlap if they are equal or if
    one's component sequence is a prefix of the other's (i.e. one would sit
    inside the other on disk)."""

    first_parts = first.parts
    second_parts = second.parts
    if len(first_parts) <= len(second_parts):
        shorter, longer = first_parts, second_parts
    else:
        shorter, longer = second_parts, first_parts
    return longer[: len(shorter)] == shorter


@dataclass(frozen=True, slots=True)
class PromotedOperationalCloudRunControl:
    """One exact, content-addressed operator-authored launch control.

    ``prior_state_restore`` is either an exact externally pinned
    ``PromotedOperationalGCSRestoreRequest`` or exactly ``None`` -- never
    discovered, listed, selected latest, or inferred. All thirteen path
    fields must be concrete absolute non-traversing ``pathlib.Path`` values
    that are pairwise non-overlapping, checked purely by comparing
    ``.parts``.
    """

    expected_assembly_spec_id: str
    expected_operational_run_spec_id: str
    target_session: date
    state_bucket: str
    assembly_spec_file: Path
    reference_root: Path
    identity_evidence_root: Path
    calendar_root: Path
    daily_reports_root: Path
    historical_corpus_root: Path
    promoted_root: Path
    graph_publication_root: Path
    engine_run_root: Path
    research_run_root: Path
    operational_preparation_root: Path
    portfolio_artifact_root: Path
    state_root: Path
    prior_state_restore: PromotedOperationalGCSRestoreRequest | None = None
    schema_version: str = PROMOTED_OPERATIONAL_CLOUD_CONTROL_SCHEMA_VERSION
    control_id: str = field(init=False)

    def __post_init__(self) -> None:
        if self.schema_version != PROMOTED_OPERATIONAL_CLOUD_CONTROL_SCHEMA_VERSION:
            raise PromotedOperationalCloudControlError(_ERR_CONTROL)
        for value in (self.expected_assembly_spec_id, self.expected_operational_run_spec_id):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise PromotedOperationalCloudControlError(_ERR_CONTROL)
        if type(self.target_session) is not date:
            raise PromotedOperationalCloudControlError(_ERR_CONTROL)
        if type(self.state_bucket) is not str or _BUCKET.fullmatch(self.state_bucket) is None:
            raise PromotedOperationalCloudControlError(_ERR_CONTROL)

        paths: list[Path] = [_require_path(getattr(self, name)) for name in _PATH_FIELD_NAMES]
        for index, first in enumerate(paths):
            for second in paths[index + 1 :]:
                if _paths_overlap(first, second):
                    raise PromotedOperationalCloudControlError(_ERR_CONTROL)

        if self.prior_state_restore is not None:
            if type(self.prior_state_restore) is not PromotedOperationalGCSRestoreRequest:
                raise PromotedOperationalCloudControlError(_ERR_CONTROL)
            restore_failed = False
            reconstructed: PromotedOperationalGCSRestoreRequest | None = None
            try:
                reconstructed = PromotedOperationalGCSRestoreRequest(
                    bucket=self.prior_state_restore.bucket,
                    manifest_object_name=self.prior_state_restore.manifest_object_name,
                    generation=self.prior_state_restore.generation,
                    expected_sha256=self.prior_state_restore.expected_sha256,
                    expected_spec_id=self.prior_state_restore.expected_spec_id,
                )
            except Exception:
                restore_failed = True
            if restore_failed or reconstructed is None:
                raise PromotedOperationalCloudControlError(_ERR_CONTROL)
            if (
                reconstructed.bucket != self.state_bucket
                or reconstructed.expected_spec_id != self.expected_operational_run_spec_id
            ):
                raise PromotedOperationalCloudControlError(_ERR_CONTROL)
            object.__setattr__(self, "prior_state_restore", reconstructed)

        object.__setattr__(self, "control_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {item.name: getattr(self, item.name) for item in fields(self) if item.name != "control_id"}

    def _calculated_id(self) -> str:
        return content_id(self._identity(), length=64)

    def verify_content_identity(self) -> None:
        """Reconstruct a fresh instance from this object's own retained
        field values -- rerunning every validation in ``__post_init__`` --
        and require its re-derived ID to match. Never merely compares a
        caller-supplied hash."""

        if type(self) is not PromotedOperationalCloudRunControl:
            raise PromotedOperationalCloudControlError(_ERR_CONTROL)
        reconstruct_failed = False
        reraise: PromotedOperationalCloudControlError | None = None
        fresh: PromotedOperationalCloudRunControl | None = None
        try:
            fresh = PromotedOperationalCloudRunControl(**self._identity())
        except PromotedOperationalCloudControlError as error:
            reraise = error
        except Exception:
            reconstruct_failed = True
        if reraise is not None:
            raise reraise
        if reconstruct_failed or fresh is None:
            raise PromotedOperationalCloudControlError(_ERR_CONTROL)
        if self.control_id != fresh.control_id:
            raise PromotedOperationalCloudControlError(_ERR_CONTROL)


_ENVELOPE_KEYS = frozenset({"codec_schema_version", "cloud_control"})
_RESTORE_KEYS = frozenset(
    {"bucket", "manifest_object_name", "generation", "expected_sha256", "expected_spec_id"}
)
_BODY_KEYS = frozenset(
    {
        "schema_version",
        "expected_assembly_spec_id",
        "expected_operational_run_spec_id",
        "target_session",
        "state_bucket",
        "prior_state_restore",
        "control_id",
        *_PATH_FIELD_NAMES,
    }
)


def _restore_body(value: PromotedOperationalGCSRestoreRequest) -> dict[str, object]:
    return {
        "bucket": value.bucket,
        "expected_sha256": value.expected_sha256,
        "expected_spec_id": value.expected_spec_id,
        "generation": value.generation,
        "manifest_object_name": value.manifest_object_name,
    }


def _control_body(
    value: PromotedOperationalCloudRunControl, *, include_control_id: bool
) -> dict[str, object]:
    body: dict[str, object] = {
        "schema_version": value.schema_version,
        "expected_assembly_spec_id": value.expected_assembly_spec_id,
        "expected_operational_run_spec_id": value.expected_operational_run_spec_id,
        "target_session": value.target_session.isoformat(),
        "state_bucket": value.state_bucket,
        "prior_state_restore": (
            None if value.prior_state_restore is None else _restore_body(value.prior_state_restore)
        ),
    }
    for name in _PATH_FIELD_NAMES:
        body[name] = str(getattr(value, name))
    if include_control_id:
        body["control_id"] = value.control_id
    return body


def encode_promoted_operational_cloud_control(value: PromotedOperationalCloudRunControl) -> bytes:
    if type(value) is not PromotedOperationalCloudRunControl:
        raise PromotedOperationalCloudControlError(_ERR_CONTROL)
    value.verify_content_identity()
    payload = (
        json.dumps(
            {
                "codec_schema_version": _CONTROL_CODEC_SCHEMA_VERSION,
                "cloud_control": _control_body(value, include_control_id=True),
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(payload) > MAXIMUM_CLOUD_CONTROL_BYTES:
        raise PromotedOperationalCloudControlError(_ERR_CONTROL)
    return payload


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedOperationalCloudControlError(_ERR_CONTROL)
        result[key] = value
    return result


def _reject_number(_token: str) -> object:
    raise PromotedOperationalCloudControlError(_ERR_CONTROL)


def decode_promoted_operational_cloud_control(payload: bytes) -> PromotedOperationalCloudRunControl:
    if type(payload) is not bytes or not (0 < len(payload) <= MAXIMUM_CLOUD_CONTROL_BYTES):
        raise PromotedOperationalCloudControlError(_ERR_CONTROL)

    decode_failed = False
    value: PromotedOperationalCloudRunControl | None = None
    try:
        text = payload.decode("utf-8", errors="strict")
        raw = json.loads(
            text, object_pairs_hook=_unique_object, parse_float=_reject_number, parse_constant=_reject_number,
        )
        if (
            type(raw) is not dict
            or set(raw) != _ENVELOPE_KEYS
            or raw["codec_schema_version"] != _CONTROL_CODEC_SCHEMA_VERSION
        ):
            raise PromotedOperationalCloudControlError(_ERR_CONTROL)
        body = raw["cloud_control"]
        if type(body) is not dict or set(body) != _BODY_KEYS:
            raise PromotedOperationalCloudControlError(_ERR_CONTROL)

        raw_restore = body["prior_state_restore"]
        restore: PromotedOperationalGCSRestoreRequest | None = None
        if raw_restore is not None:
            if type(raw_restore) is not dict or set(raw_restore) != _RESTORE_KEYS:
                raise PromotedOperationalCloudControlError(_ERR_CONTROL)
            restore = PromotedOperationalGCSRestoreRequest(
                bucket=raw_restore["bucket"],
                manifest_object_name=raw_restore["manifest_object_name"],
                generation=raw_restore["generation"],
                expected_sha256=raw_restore["expected_sha256"],
                expected_spec_id=raw_restore["expected_spec_id"],
            )

        path_kwargs = {name: Path(body[name]) for name in _PATH_FIELD_NAMES}
        value = PromotedOperationalCloudRunControl(
            schema_version=body["schema_version"],
            expected_assembly_spec_id=body["expected_assembly_spec_id"],
            expected_operational_run_spec_id=body["expected_operational_run_spec_id"],
            target_session=date.fromisoformat(body["target_session"]),
            state_bucket=body["state_bucket"],
            prior_state_restore=restore,
            **path_kwargs,
        )
        if value.control_id != body["control_id"]:
            raise PromotedOperationalCloudControlError(_ERR_CONTROL)
        if encode_promoted_operational_cloud_control(value) != payload:
            raise PromotedOperationalCloudControlError(_ERR_CONTROL)
    except Exception:
        decode_failed = True
    if decode_failed or value is None:
        raise PromotedOperationalCloudControlError(_ERR_CONTROL)
    return value
