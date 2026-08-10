"""Path-free, portable launch-control boundary bridging one already-
published promoted-operational input snapshot into the already-accepted
Cloud Run-shaped paper job.

Defines one exact, content-addressed ``PromotedOperationalHydratedCloudLaunch``
that binds the expected assembly-spec/operational-run-spec identities, the
target session, the state bucket, the mandatory externally pinned
``PromotedOperationalInputRestoreRequest``, and an optional externally
pinned ``PromotedOperationalGCSRestoreRequest`` for prior promoted-
operational state -- plus a strict canonical JSON codec for it.

This launch control carries no local filesystem path: it is meant to be
published once by an offline local publisher and later read by a fresh
Cloud Run container, which derives its OWN local runtime destinations
from a fixed parent -- never from anything serialized here. This module
never touches the filesystem, environment, wall clock, network, or any
GCP/Kite capability; it never lists, discovers, or selects a "latest"
restore -- every restore is either an exact externally pinned request or
exactly absent. It never reproduces any input-snapshot, GCS-state, or
cloud-control codec -- it only composes the already-accepted
``PromotedOperationalInputRestoreRequest``/``PromotedOperationalGCSRestoreRequest``
types.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field, fields
from datetime import date

from india_swing.identity import content_id
from india_swing.promoted_operational_gcs_state import PromotedOperationalGCSRestoreRequest
from india_swing.promoted_operational_input_gcs import PromotedOperationalInputRestoreRequest


class PromotedOperationalHydratedCloudLaunchError(ValueError):
    pass


_ERR = "promoted operational hydrated cloud launch is invalid"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]\Z")

PROMOTED_OPERATIONAL_HYDRATED_CLOUD_LAUNCH_SCHEMA_VERSION = "promoted-operational-hydrated-cloud-launch/v1"
_LAUNCH_CODEC_SCHEMA_VERSION = "promoted-operational-hydrated-cloud-launch-json/v1"
MAXIMUM_HYDRATED_CLOUD_LAUNCH_BYTES = 128 * 1024


@dataclass(frozen=True, slots=True)
class PromotedOperationalHydratedCloudLaunch:
    """One exact, content-addressed, path-free launch control.

    ``input_restore`` is mandatory and externally pinned; ``prior_state_restore``
    is either an exact externally pinned ``PromotedOperationalGCSRestoreRequest``
    or exactly ``None`` -- never discovered, listed, selected latest, or
    inferred. No field here is a local filesystem path.
    """

    expected_assembly_spec_id: str
    expected_operational_run_spec_id: str
    target_session: date
    state_bucket: str
    input_restore: PromotedOperationalInputRestoreRequest
    prior_state_restore: PromotedOperationalGCSRestoreRequest | None = None
    schema_version: str = PROMOTED_OPERATIONAL_HYDRATED_CLOUD_LAUNCH_SCHEMA_VERSION
    launch_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.schema_version) is not str
            or self.schema_version != PROMOTED_OPERATIONAL_HYDRATED_CLOUD_LAUNCH_SCHEMA_VERSION
        ):
            raise PromotedOperationalHydratedCloudLaunchError(_ERR)
        for value in (self.expected_assembly_spec_id, self.expected_operational_run_spec_id):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise PromotedOperationalHydratedCloudLaunchError(_ERR)
        if type(self.target_session) is not date:
            raise PromotedOperationalHydratedCloudLaunchError(_ERR)
        if type(self.state_bucket) is not str or _BUCKET.fullmatch(self.state_bucket) is None:
            raise PromotedOperationalHydratedCloudLaunchError(_ERR)

        if type(self.input_restore) is not PromotedOperationalInputRestoreRequest:
            raise PromotedOperationalHydratedCloudLaunchError(_ERR)
        input_failed = False
        reconstructed_input: PromotedOperationalInputRestoreRequest | None = None
        try:
            reconstructed_input = PromotedOperationalInputRestoreRequest(
                bucket=self.input_restore.bucket,
                manifest_object_name=self.input_restore.manifest_object_name,
                generation=self.input_restore.generation,
                expected_sha256=self.input_restore.expected_sha256,
                expected_snapshot_id=self.input_restore.expected_snapshot_id,
                expected_assembly_spec_id=self.input_restore.expected_assembly_spec_id,
                target_session=self.input_restore.target_session,
            )
        except Exception:
            input_failed = True
        if input_failed or reconstructed_input is None:
            raise PromotedOperationalHydratedCloudLaunchError(_ERR)
        if (
            reconstructed_input.bucket != self.state_bucket
            or reconstructed_input.expected_assembly_spec_id != self.expected_assembly_spec_id
            or reconstructed_input.target_session != self.target_session
        ):
            raise PromotedOperationalHydratedCloudLaunchError(_ERR)
        object.__setattr__(self, "input_restore", reconstructed_input)

        if self.prior_state_restore is not None:
            if type(self.prior_state_restore) is not PromotedOperationalGCSRestoreRequest:
                raise PromotedOperationalHydratedCloudLaunchError(_ERR)
            prior_failed = False
            reconstructed_prior: PromotedOperationalGCSRestoreRequest | None = None
            try:
                reconstructed_prior = PromotedOperationalGCSRestoreRequest(
                    bucket=self.prior_state_restore.bucket,
                    manifest_object_name=self.prior_state_restore.manifest_object_name,
                    generation=self.prior_state_restore.generation,
                    expected_sha256=self.prior_state_restore.expected_sha256,
                    expected_spec_id=self.prior_state_restore.expected_spec_id,
                )
            except Exception:
                prior_failed = True
            if prior_failed or reconstructed_prior is None:
                raise PromotedOperationalHydratedCloudLaunchError(_ERR)
            if (
                reconstructed_prior.bucket != self.state_bucket
                or reconstructed_prior.expected_spec_id != self.expected_operational_run_spec_id
            ):
                raise PromotedOperationalHydratedCloudLaunchError(_ERR)
            object.__setattr__(self, "prior_state_restore", reconstructed_prior)

        object.__setattr__(self, "launch_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {item.name: getattr(self, item.name) for item in fields(self) if item.name != "launch_id"}

    def _calculated_id(self) -> str:
        return content_id(self._identity(), length=64)

    def verify_content_identity(self) -> None:
        """Reconstruct a fresh instance from this object's own retained
        field values -- rerunning every validation in ``__post_init__`` --
        and require its re-derived ID to match. Never merely compares a
        caller-supplied hash."""

        if type(self) is not PromotedOperationalHydratedCloudLaunch:
            raise PromotedOperationalHydratedCloudLaunchError(_ERR)
        reconstruct_failed = False
        reraise: PromotedOperationalHydratedCloudLaunchError | None = None
        fresh: PromotedOperationalHydratedCloudLaunch | None = None
        try:
            fresh = PromotedOperationalHydratedCloudLaunch(**self._identity())
        except PromotedOperationalHydratedCloudLaunchError as error:
            reraise = error
        except Exception:
            reconstruct_failed = True
        if reraise is not None:
            raise reraise
        if reconstruct_failed or fresh is None:
            raise PromotedOperationalHydratedCloudLaunchError(_ERR)
        if self.launch_id != fresh.launch_id:
            raise PromotedOperationalHydratedCloudLaunchError(_ERR)


_ENVELOPE_KEYS = frozenset({"codec_schema_version", "hydrated_cloud_launch"})
_INPUT_RESTORE_KEYS = frozenset(
    {
        "bucket",
        "manifest_object_name",
        "generation",
        "expected_sha256",
        "expected_snapshot_id",
        "expected_assembly_spec_id",
        "target_session",
    }
)
_PRIOR_RESTORE_KEYS = frozenset(
    {"bucket", "manifest_object_name", "generation", "expected_sha256", "expected_spec_id"}
)
_BODY_KEYS = frozenset(
    {
        "schema_version",
        "expected_assembly_spec_id",
        "expected_operational_run_spec_id",
        "target_session",
        "state_bucket",
        "input_restore",
        "prior_state_restore",
        "launch_id",
    }
)


def _input_restore_body(value: PromotedOperationalInputRestoreRequest) -> dict[str, object]:
    return {
        "bucket": value.bucket,
        "expected_assembly_spec_id": value.expected_assembly_spec_id,
        "expected_sha256": value.expected_sha256,
        "expected_snapshot_id": value.expected_snapshot_id,
        "generation": value.generation,
        "manifest_object_name": value.manifest_object_name,
        "target_session": value.target_session.isoformat(),
    }


def _prior_restore_body(value: PromotedOperationalGCSRestoreRequest) -> dict[str, object]:
    return {
        "bucket": value.bucket,
        "expected_sha256": value.expected_sha256,
        "expected_spec_id": value.expected_spec_id,
        "generation": value.generation,
        "manifest_object_name": value.manifest_object_name,
    }


def _launch_body(
    value: PromotedOperationalHydratedCloudLaunch, *, include_launch_id: bool
) -> dict[str, object]:
    body: dict[str, object] = {
        "schema_version": value.schema_version,
        "expected_assembly_spec_id": value.expected_assembly_spec_id,
        "expected_operational_run_spec_id": value.expected_operational_run_spec_id,
        "target_session": value.target_session.isoformat(),
        "state_bucket": value.state_bucket,
        "input_restore": _input_restore_body(value.input_restore),
        "prior_state_restore": (
            None if value.prior_state_restore is None else _prior_restore_body(value.prior_state_restore)
        ),
    }
    if include_launch_id:
        body["launch_id"] = value.launch_id
    return body


def encode_promoted_operational_hydrated_cloud_launch(
    value: PromotedOperationalHydratedCloudLaunch,
) -> bytes:
    if type(value) is not PromotedOperationalHydratedCloudLaunch:
        raise PromotedOperationalHydratedCloudLaunchError(_ERR)
    value.verify_content_identity()
    payload = (
        json.dumps(
            {
                "codec_schema_version": _LAUNCH_CODEC_SCHEMA_VERSION,
                "hydrated_cloud_launch": _launch_body(value, include_launch_id=True),
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(payload) > MAXIMUM_HYDRATED_CLOUD_LAUNCH_BYTES:
        raise PromotedOperationalHydratedCloudLaunchError(_ERR)
    return payload


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedOperationalHydratedCloudLaunchError(_ERR)
        result[key] = value
    return result


def _reject_number(_token: str) -> object:
    raise PromotedOperationalHydratedCloudLaunchError(_ERR)


def decode_promoted_operational_hydrated_cloud_launch(
    payload: bytes,
) -> PromotedOperationalHydratedCloudLaunch:
    if type(payload) is not bytes or not (0 < len(payload) <= MAXIMUM_HYDRATED_CLOUD_LAUNCH_BYTES):
        raise PromotedOperationalHydratedCloudLaunchError(_ERR)

    decode_failed = False
    value: PromotedOperationalHydratedCloudLaunch | None = None
    try:
        text = payload.decode("utf-8", errors="strict")
        raw = json.loads(
            text, object_pairs_hook=_unique_object, parse_float=_reject_number, parse_constant=_reject_number,
        )
        if (
            type(raw) is not dict
            or set(raw) != _ENVELOPE_KEYS
            or raw["codec_schema_version"] != _LAUNCH_CODEC_SCHEMA_VERSION
        ):
            raise PromotedOperationalHydratedCloudLaunchError(_ERR)
        body = raw["hydrated_cloud_launch"]
        if type(body) is not dict or set(body) != _BODY_KEYS:
            raise PromotedOperationalHydratedCloudLaunchError(_ERR)

        raw_input_restore = body["input_restore"]
        if type(raw_input_restore) is not dict or set(raw_input_restore) != _INPUT_RESTORE_KEYS:
            raise PromotedOperationalHydratedCloudLaunchError(_ERR)
        input_restore = PromotedOperationalInputRestoreRequest(
            bucket=raw_input_restore["bucket"],
            manifest_object_name=raw_input_restore["manifest_object_name"],
            generation=raw_input_restore["generation"],
            expected_sha256=raw_input_restore["expected_sha256"],
            expected_snapshot_id=raw_input_restore["expected_snapshot_id"],
            expected_assembly_spec_id=raw_input_restore["expected_assembly_spec_id"],
            target_session=date.fromisoformat(raw_input_restore["target_session"]),
        )

        raw_prior_restore = body["prior_state_restore"]
        prior_restore: PromotedOperationalGCSRestoreRequest | None = None
        if raw_prior_restore is not None:
            if type(raw_prior_restore) is not dict or set(raw_prior_restore) != _PRIOR_RESTORE_KEYS:
                raise PromotedOperationalHydratedCloudLaunchError(_ERR)
            prior_restore = PromotedOperationalGCSRestoreRequest(
                bucket=raw_prior_restore["bucket"],
                manifest_object_name=raw_prior_restore["manifest_object_name"],
                generation=raw_prior_restore["generation"],
                expected_sha256=raw_prior_restore["expected_sha256"],
                expected_spec_id=raw_prior_restore["expected_spec_id"],
            )

        value = PromotedOperationalHydratedCloudLaunch(
            schema_version=body["schema_version"],
            expected_assembly_spec_id=body["expected_assembly_spec_id"],
            expected_operational_run_spec_id=body["expected_operational_run_spec_id"],
            target_session=date.fromisoformat(body["target_session"]),
            state_bucket=body["state_bucket"],
            input_restore=input_restore,
            prior_state_restore=prior_restore,
        )
        if value.launch_id != body["launch_id"]:
            raise PromotedOperationalHydratedCloudLaunchError(_ERR)
        if encode_promoted_operational_hydrated_cloud_launch(value) != payload:
            raise PromotedOperationalHydratedCloudLaunchError(_ERR)
    except Exception:
        decode_failed = True
    if decode_failed or value is None:
        raise PromotedOperationalHydratedCloudLaunchError(_ERR)
    return value
