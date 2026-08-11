"""Portable, path-free launch control for the forward-paper cloud job."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from india_swing.evaluation.nse_archive_research_dataset_gcs import (
    PinnedNseArchiveResearchDatasetRequest,
)
from india_swing.identity import content_id
from india_swing.forward_paper.history import ForwardPaperHistoryWindowSpec
from india_swing.promoted_operational_hydrated_cloud_control import (
    PromotedOperationalHydratedCloudLaunch,
    decode_promoted_operational_hydrated_cloud_launch,
    encode_promoted_operational_hydrated_cloud_launch,
)


class ForwardPaperHydratedCloudLaunchError(ValueError):
    """Static failure for a malformed or inconsistent portable launch."""


_ERROR = "forward paper hydrated cloud launch is invalid"
_SCHEMA = "forward-paper-hydrated-cloud-launch/v1"
_CODEC = "forward-paper-hydrated-cloud-launch-json/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]\Z")
MAXIMUM_FORWARD_PAPER_HYDRATED_LAUNCH_BYTES = 256 * 1024


def _utc(value: object) -> datetime:
    if (
        type(value) is not datetime
        or value.tzinfo is None
        or value.utcoffset() is None
        or value.utcoffset() != timedelta(0)
    ):
        raise ForwardPaperHydratedCloudLaunchError(_ERROR)
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class ForwardPaperHydratedCloudLaunch:
    promoted_input_launch: PromotedOperationalHydratedCloudLaunch
    dataset_request: PinnedNseArchiveResearchDatasetRequest
    decision_cutoff: datetime
    expected_market_sessions: tuple[date, ...]
    corporate_action_snapshot_id: str
    tick_panel_id: str
    output_bucket: str
    schema_version: str = _SCHEMA
    launch_id: str = field(init=False)

    def __post_init__(self) -> None:
        failed = False
        promoted = dataset = None
        cutoff = None
        try:
            if type(self.promoted_input_launch) is not PromotedOperationalHydratedCloudLaunch:
                raise ValueError
            self.promoted_input_launch.verify_content_identity()
            if self.promoted_input_launch.prior_state_restore is not None:
                raise ValueError
            promoted = decode_promoted_operational_hydrated_cloud_launch(
                encode_promoted_operational_hydrated_cloud_launch(
                    self.promoted_input_launch
                )
            )
            if type(self.dataset_request) is not PinnedNseArchiveResearchDatasetRequest:
                raise ValueError
            dataset = PinnedNseArchiveResearchDatasetRequest(
                bucket=self.dataset_request.bucket,
                dataset_id=self.dataset_request.dataset_id,
                generation=self.dataset_request.generation,
                expected_sha256=self.dataset_request.expected_sha256,
            )
            cutoff = _utc(self.decision_cutoff)
        except Exception:
            failed = True
        if failed or promoted is None or dataset is None or cutoff is None:
            raise ForwardPaperHydratedCloudLaunchError(_ERROR)
        if (
            type(self.expected_market_sessions) is not tuple
            or len(self.expected_market_sessions) != 60
            or any(type(value) is not date for value in self.expected_market_sessions)
            or self.expected_market_sessions
            != tuple(sorted(set(self.expected_market_sessions)))
            or self.expected_market_sessions[-1] != promoted.target_session
            or cutoff.date() < promoted.target_session
        ):
            raise ForwardPaperHydratedCloudLaunchError(_ERROR)
        spec_failed = False
        try:
            history_spec = ForwardPaperHistoryWindowSpec(
                dataset_id=dataset.dataset_id,
                signal_session=promoted.target_session,
                decision_cutoff=cutoff,
                expected_market_sessions=self.expected_market_sessions,
            )
            history_spec.verify_content_identity()
        except Exception:
            spec_failed = True
        if spec_failed:
            raise ForwardPaperHydratedCloudLaunchError(_ERROR)
        for value in (self.corporate_action_snapshot_id, self.tick_panel_id):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise ForwardPaperHydratedCloudLaunchError(_ERROR)
        if (
            type(self.output_bucket) is not str
            or _BUCKET.fullmatch(self.output_bucket) is None
            or self.schema_version != _SCHEMA
        ):
            raise ForwardPaperHydratedCloudLaunchError(_ERROR)
        object.__setattr__(self, "promoted_input_launch", promoted)
        object.__setattr__(self, "dataset_request", dataset)
        object.__setattr__(self, "decision_cutoff", cutoff)
        object.__setattr__(self, "launch_id", self._calculated_id())

    @property
    def signal_session(self) -> date:
        return self.promoted_input_launch.target_session

    def _identity(self) -> dict[str, object]:
        return {
            "schema": self.schema_version,
            "promoted_input_launch_id": self.promoted_input_launch.launch_id,
            "dataset_bucket": self.dataset_request.bucket,
            "dataset_id": self.dataset_request.dataset_id,
            "dataset_generation": self.dataset_request.generation,
            "dataset_sha256": self.dataset_request.expected_sha256,
            "decision_cutoff": self.decision_cutoff,
            "expected_market_sessions": self.expected_market_sessions,
            "corporate_action_snapshot_id": self.corporate_action_snapshot_id,
            "tick_panel_id": self.tick_panel_id,
            "output_bucket": self.output_bucket,
        }

    def _calculated_id(self) -> str:
        return content_id(self._identity(), length=64)

    def verify_content_identity(self) -> None:
        failed = False
        fresh = None
        try:
            fresh = ForwardPaperHydratedCloudLaunch(
                promoted_input_launch=self.promoted_input_launch,
                dataset_request=self.dataset_request,
                decision_cutoff=self.decision_cutoff,
                expected_market_sessions=self.expected_market_sessions,
                corporate_action_snapshot_id=self.corporate_action_snapshot_id,
                tick_panel_id=self.tick_panel_id,
                output_bucket=self.output_bucket,
                schema_version=self.schema_version,
            )
        except Exception:
            failed = True
        if failed or fresh is None or fresh.launch_id != self.launch_id:
            raise ForwardPaperHydratedCloudLaunchError(_ERROR)


def _body(value: ForwardPaperHydratedCloudLaunch) -> dict[str, object]:
    promoted = json.loads(
        encode_promoted_operational_hydrated_cloud_launch(
            value.promoted_input_launch
        ).decode("utf-8")
    )
    return {
        "corporate_action_snapshot_id": value.corporate_action_snapshot_id,
        "dataset_request": {
            "bucket": value.dataset_request.bucket,
            "dataset_id": value.dataset_request.dataset_id,
            "expected_sha256": value.dataset_request.expected_sha256,
            "generation": value.dataset_request.generation,
        },
        "decision_cutoff": value.decision_cutoff.isoformat(),
        "expected_market_sessions": [
            item.isoformat() for item in value.expected_market_sessions
        ],
        "launch_id": value.launch_id,
        "output_bucket": value.output_bucket,
        "promoted_input_launch": promoted,
        "schema_version": value.schema_version,
        "tick_panel_id": value.tick_panel_id,
    }


def encode_forward_paper_hydrated_cloud_launch(
    value: ForwardPaperHydratedCloudLaunch,
) -> bytes:
    if type(value) is not ForwardPaperHydratedCloudLaunch:
        raise ForwardPaperHydratedCloudLaunchError(_ERROR)
    value.verify_content_identity()
    payload = (
        json.dumps(
            {"codec_schema_version": _CODEC, "launch": _body(value)},
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(payload) > MAXIMUM_FORWARD_PAPER_HYDRATED_LAUNCH_BYTES:
        raise ForwardPaperHydratedCloudLaunchError(_ERROR)
    return payload


def _unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ForwardPaperHydratedCloudLaunchError(_ERROR)
        result[key] = value
    return result


def _reject_number(_value: str) -> object:
    raise ForwardPaperHydratedCloudLaunchError(_ERROR)


def decode_forward_paper_hydrated_cloud_launch(
    payload: bytes,
) -> ForwardPaperHydratedCloudLaunch:
    if type(payload) is not bytes or not (
        0 < len(payload) <= MAXIMUM_FORWARD_PAPER_HYDRATED_LAUNCH_BYTES
    ):
        raise ForwardPaperHydratedCloudLaunchError(_ERROR)
    failed = False
    value = None
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
        if type(raw) is not dict or set(raw) != {"codec_schema_version", "launch"}:
            raise ValueError
        if raw["codec_schema_version"] != _CODEC:
            raise ValueError
        body = raw["launch"]
        if type(body) is not dict or set(body) != {
            "corporate_action_snapshot_id",
            "dataset_request",
            "decision_cutoff",
            "expected_market_sessions",
            "launch_id",
            "output_bucket",
            "promoted_input_launch",
            "schema_version",
            "tick_panel_id",
        }:
            raise ValueError
        dataset = body["dataset_request"]
        if type(dataset) is not dict or set(dataset) != {
            "bucket", "dataset_id", "expected_sha256", "generation"
        }:
            raise ValueError
        promoted_bytes = (
            json.dumps(
                body["promoted_input_launch"],
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
        sessions = body["expected_market_sessions"]
        if type(sessions) is not list:
            raise ValueError
        value = ForwardPaperHydratedCloudLaunch(
            promoted_input_launch=decode_promoted_operational_hydrated_cloud_launch(
                promoted_bytes
            ),
            dataset_request=PinnedNseArchiveResearchDatasetRequest(
                bucket=dataset["bucket"],
                dataset_id=dataset["dataset_id"],
                generation=dataset["generation"],
                expected_sha256=dataset["expected_sha256"],
            ),
            decision_cutoff=datetime.fromisoformat(body["decision_cutoff"]),
            expected_market_sessions=tuple(date.fromisoformat(item) for item in sessions),
            corporate_action_snapshot_id=body["corporate_action_snapshot_id"],
            tick_panel_id=body["tick_panel_id"],
            output_bucket=body["output_bucket"],
            schema_version=body["schema_version"],
        )
        if value.launch_id != body["launch_id"]:
            raise ValueError
        if encode_forward_paper_hydrated_cloud_launch(value) != payload:
            raise ValueError
    except Exception:
        failed = True
    if failed or value is None:
        raise ForwardPaperHydratedCloudLaunchError(_ERROR)
    return value
