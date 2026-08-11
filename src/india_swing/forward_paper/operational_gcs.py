"""Pinned GCS manifest boundary for forward-paper operational research graphs."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Protocol

from india_swing.corporate_actions.models import CorporateActionSnapshot
from india_swing.daily_pipeline.acquisition import GCSObjectPayload, GCSObjectReader
from india_swing.daily_pipeline.state_publication import (
    PublishedStateObject,
    StateObjectWriter,
)
from india_swing.forward_paper.history import (
    ForwardPaperHistoryWindowSpec,
    ForwardPaperRawHistoryWindow,
)
from india_swing.forward_paper.operational import (
    ForwardPaperOperationalResearchGraph,
    assemble_forward_paper_operational_research_graph,
)
from india_swing.identity import content_id
from india_swing.tick_sizes.effective_session import (
    VerifiedPromotedEffectiveSessionTickPanel,
)


FORWARD_PAPER_OPERATIONAL_MANIFEST_SCHEMA_VERSION = (
    "forward-paper-operational-manifest-v1"
)
FORWARD_PAPER_OPERATIONAL_MANIFEST_POLICY_VERSION = (
    "exact-generation-recompute-before-use-v1"
)
FORWARD_PAPER_OPERATIONAL_MANIFEST_MAXIMUM_BYTES = 32 * 1024

_SHA256 = re.compile(r"[0-9a-f]{64}")
_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]")
_ROOT = "research/forward-paper-operational/v1"
_CONTENT_TYPE = "application/json"


class ForwardPaperOperationalManifestError(ValueError):
    """Static, sanitized failure at the durable operational graph boundary."""


class ForwardPaperRawHistoryWindowResolver(Protocol):
    def build(self, spec: ForwardPaperHistoryWindowSpec) -> ForwardPaperRawHistoryWindow: ...


class ForwardPaperCorporateActionSnapshotResolver(Protocol):
    def get(self, snapshot_id: str) -> CorporateActionSnapshot: ...


class ForwardPaperEffectiveTickPanelResolver(Protocol):
    def get(self, panel_id: str) -> VerifiedPromotedEffectiveSessionTickPanel: ...


def _fail(message: str) -> None:
    raise ForwardPaperOperationalManifestError(message)


def _sha(value: object) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        _fail("forward paper operational manifest identity is invalid")
    return value


def _bucket(value: object) -> str:
    if type(value) is not str or _BUCKET.fullmatch(value) is None:
        _fail("forward paper operational manifest bucket is invalid")
    return value


def _utc(value: object) -> datetime:
    if (
        type(value) is not datetime
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        _fail("forward paper operational manifest cutoff is invalid")
    return value.astimezone(timezone.utc)


def _canonical_bytes(value: dict[str, object]) -> bytes:
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
class ForwardPaperOperationalGraphManifest:
    bucket: str
    graph_id: str
    source_window_id: str
    source_spec_id: str
    dataset_id: str
    expected_market_sessions: tuple[date, ...]
    corporate_action_snapshot_id: str
    tick_panel_id: str
    adjusted_window_id: str
    feature_input_window_id: str
    technical_feature_window_id: str
    signal_session: date
    decision_cutoff: datetime
    schema_version: str = FORWARD_PAPER_OPERATIONAL_MANIFEST_SCHEMA_VERSION
    policy_version: str = FORWARD_PAPER_OPERATIONAL_MANIFEST_POLICY_VERSION
    manifest_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "decision_cutoff", _utc(self.decision_cutoff))
        object.__setattr__(self, "manifest_id", self._calculated_id())

    def _validate(self) -> None:
        _bucket(self.bucket)
        for value in (
            self.graph_id,
            self.source_window_id,
            self.source_spec_id,
            self.dataset_id,
            self.corporate_action_snapshot_id,
            self.tick_panel_id,
            self.adjusted_window_id,
            self.feature_input_window_id,
            self.technical_feature_window_id,
        ):
            _sha(value)
        if type(self.signal_session) is not date:
            _fail("forward paper operational manifest signal session is invalid")
        if (
            type(self.expected_market_sessions) is not tuple
            or len(self.expected_market_sessions) != 60
            or any(type(value) is not date for value in self.expected_market_sessions)
            or self.expected_market_sessions
            != tuple(sorted(set(self.expected_market_sessions)))
            or self.expected_market_sessions[-1] != self.signal_session
        ):
            _fail("forward paper operational manifest expected sessions are invalid")
        _utc(self.decision_cutoff)
        if self.schema_version != FORWARD_PAPER_OPERATIONAL_MANIFEST_SCHEMA_VERSION:
            _fail("forward paper operational manifest schema is invalid")
        if self.policy_version != FORWARD_PAPER_OPERATIONAL_MANIFEST_POLICY_VERSION:
            _fail("forward paper operational manifest policy is invalid")

    def _calculated_id(self) -> str:
        return content_id(_manifest_body(self, include_manifest_id=False), length=64)

    def verify_content_identity(self) -> None:
        self._validate()
        if self.manifest_id != self._calculated_id():
            _fail("forward paper operational manifest identity failed")


def _manifest_body(
    value: ForwardPaperOperationalGraphManifest, *, include_manifest_id: bool
) -> dict[str, object]:
    body: dict[str, object] = {
        "adjusted_window_id": value.adjusted_window_id,
        "bucket": value.bucket,
        "corporate_action_snapshot_id": value.corporate_action_snapshot_id,
        "decision_cutoff": value.decision_cutoff.isoformat(),
        "dataset_id": value.dataset_id,
        "expected_market_sessions": [
            item.isoformat() for item in value.expected_market_sessions
        ],
        "feature_input_window_id": value.feature_input_window_id,
        "graph_id": value.graph_id,
        "policy_version": value.policy_version,
        "schema_version": value.schema_version,
        "signal_session": value.signal_session.isoformat(),
        "source_window_id": value.source_window_id,
        "source_spec_id": value.source_spec_id,
        "technical_feature_window_id": value.technical_feature_window_id,
        "tick_panel_id": value.tick_panel_id,
    }
    if include_manifest_id:
        body["manifest_id"] = value.manifest_id
    return body


def operational_graph_manifest_from_graph(
    graph: ForwardPaperOperationalResearchGraph, bucket: str
) -> ForwardPaperOperationalGraphManifest:
    failed = False
    try:
        if type(graph) is not ForwardPaperOperationalResearchGraph:
            failed = True
        else:
            graph.verify_content_identity()
    except Exception:
        failed = True
    if failed:
        _fail("forward paper operational graph failed manifest verification")
    bucket = _bucket(bucket)
    return ForwardPaperOperationalGraphManifest(
        bucket=bucket,
        graph_id=graph.graph_id,
        source_window_id=graph.source_window.window_id,
        source_spec_id=graph.source_window.spec.spec_id,
        dataset_id=graph.source_window.spec.dataset_id,
        expected_market_sessions=graph.source_window.spec.expected_market_sessions,
        corporate_action_snapshot_id=graph.corporate_actions.snapshot_id,
        tick_panel_id=graph.tick_panel.panel_id,
        adjusted_window_id=graph.adjusted_window.window_id,
        feature_input_window_id=graph.feature_input_window.window_id,
        technical_feature_window_id=graph.technical_feature_window.window_id,
        signal_session=graph.source_window.spec.signal_session,
        decision_cutoff=graph.source_window.spec.decision_cutoff,
    )


def encode_forward_paper_operational_manifest(
    manifest: ForwardPaperOperationalGraphManifest,
) -> bytes:
    if type(manifest) is not ForwardPaperOperationalGraphManifest:
        _fail("forward paper operational manifest type is invalid")
    manifest.verify_content_identity()
    encoded = _canonical_bytes(_manifest_body(manifest, include_manifest_id=True))
    if not encoded or len(encoded) > FORWARD_PAPER_OPERATIONAL_MANIFEST_MAXIMUM_BYTES:
        _fail("forward paper operational manifest payload is invalid")
    return encoded


def _reject_float(_: str) -> object:
    raise ValueError


def _reject_constant(_: str) -> object:
    raise ValueError


def _object_no_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def decode_forward_paper_operational_manifest(
    payload: bytes,
) -> ForwardPaperOperationalGraphManifest:
    failed = False
    raw: object = None
    try:
        if (
            type(payload) is not bytes
            or not payload
            or len(payload) > FORWARD_PAPER_OPERATIONAL_MANIFEST_MAXIMUM_BYTES
        ):
            raise ValueError
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_object_no_duplicates,
            parse_float=_reject_float,
            parse_constant=_reject_constant,
        )
    except Exception:
        failed = True
    if failed or type(raw) is not dict:
        _fail("forward paper operational manifest payload is invalid")
    expected = {
        "adjusted_window_id",
        "bucket",
        "corporate_action_snapshot_id",
        "decision_cutoff",
        "dataset_id",
        "expected_market_sessions",
        "feature_input_window_id",
        "graph_id",
        "manifest_id",
        "policy_version",
        "schema_version",
        "signal_session",
        "source_window_id",
        "source_spec_id",
        "technical_feature_window_id",
        "tick_panel_id",
    }
    if set(raw) != expected:
        _fail("forward paper operational manifest payload shape is invalid")
    constructed = None
    failed = False
    try:
        constructed = ForwardPaperOperationalGraphManifest(
            bucket=raw["bucket"],
            graph_id=raw["graph_id"],
            source_window_id=raw["source_window_id"],
            source_spec_id=raw["source_spec_id"],
            dataset_id=raw["dataset_id"],
            expected_market_sessions=tuple(
                date.fromisoformat(value) for value in raw["expected_market_sessions"]
            ),
            corporate_action_snapshot_id=raw["corporate_action_snapshot_id"],
            tick_panel_id=raw["tick_panel_id"],
            adjusted_window_id=raw["adjusted_window_id"],
            feature_input_window_id=raw["feature_input_window_id"],
            technical_feature_window_id=raw["technical_feature_window_id"],
            signal_session=date.fromisoformat(raw["signal_session"]),
            decision_cutoff=datetime.fromisoformat(raw["decision_cutoff"]),
            schema_version=raw["schema_version"],
            policy_version=raw["policy_version"],
        )
    except Exception:
        failed = True
    if (
        failed
        or constructed is None
        or constructed.manifest_id != raw["manifest_id"]
        or encode_forward_paper_operational_manifest(constructed) != payload
    ):
        _fail("forward paper operational manifest payload failed verification")
    return constructed


def forward_paper_operational_manifest_object_name(
    manifest: ForwardPaperOperationalGraphManifest,
) -> str:
    if type(manifest) is not ForwardPaperOperationalGraphManifest:
        _fail("forward paper operational manifest type is invalid")
    manifest.verify_content_identity()
    return (
        f"{_ROOT}/{manifest.signal_session.isoformat()}/"
        f"{manifest.graph_id}/{manifest.manifest_id}.json"
    )


@dataclass(frozen=True, slots=True)
class CompletedForwardPaperOperationalGraphPublication:
    manifest: ForwardPaperOperationalGraphManifest
    manifest_object: PublishedStateObject

    def __post_init__(self) -> None:
        if type(self.manifest) is not ForwardPaperOperationalGraphManifest:
            _fail("forward paper operational publication manifest is invalid")
        self.manifest.verify_content_identity()
        expected = encode_forward_paper_operational_manifest(self.manifest)
        published = _published(self.manifest_object)
        if (
            published.object_name
            != forward_paper_operational_manifest_object_name(self.manifest)
            or published.byte_count != len(expected)
            or published.sha256 != hashlib.sha256(expected).hexdigest()
        ):
            _fail("forward paper operational published manifest differs")


def _published(value: object) -> PublishedStateObject:
    failed = False
    result = None
    try:
        if type(value) is not PublishedStateObject:
            raise ValueError
        result = PublishedStateObject(
            object_name=value.object_name,
            generation=value.generation,
            byte_count=value.byte_count,
            sha256=value.sha256,
        )
    except Exception:
        failed = True
    if failed or result is None:
        _fail("forward paper operational published object is invalid")
    return result


def publish_forward_paper_operational_graph(
    *,
    graph: ForwardPaperOperationalResearchGraph,
    bucket: str,
    writer: StateObjectWriter,
) -> CompletedForwardPaperOperationalGraphPublication:
    manifest = operational_graph_manifest_from_graph(graph, bucket)
    payload = encode_forward_paper_operational_manifest(manifest)
    name = forward_paper_operational_manifest_object_name(manifest)
    failed = False
    published = None
    try:
        if not callable(getattr(writer, "create_or_verify", None)):
            raise ValueError
        published = writer.create_or_verify(
            bucket=manifest.bucket,
            object_name=name,
            content_bytes=payload,
            content_type=_CONTENT_TYPE,
            maximum_bytes=FORWARD_PAPER_OPERATIONAL_MANIFEST_MAXIMUM_BYTES,
        )
    except Exception:
        failed = True
    if failed:
        _fail("forward paper operational manifest publication failed safely")
    published = _published(published)
    if (
        published.object_name != name
        or published.byte_count != len(payload)
        or published.sha256 != hashlib.sha256(payload).hexdigest()
    ):
        _fail("forward paper operational published manifest differs")
    return CompletedForwardPaperOperationalGraphPublication(
        manifest=manifest,
        manifest_object=published,
    )


def restore_forward_paper_operational_graph(
    *,
    expected_graph_id: str,
    bucket: str,
    manifest_object_name: str,
    manifest_generation: int,
    manifest_sha256: str,
    reader: GCSObjectReader,
    history_windows: ForwardPaperRawHistoryWindowResolver,
    corporate_actions: ForwardPaperCorporateActionSnapshotResolver,
    tick_panels: ForwardPaperEffectiveTickPanelResolver,
) -> ForwardPaperOperationalResearchGraph:
    expected_graph_id = _sha(expected_graph_id)
    bucket = _bucket(bucket)
    manifest_sha256 = _sha(manifest_sha256)
    if (
        type(manifest_generation) is not int
        or type(manifest_generation) is bool
        or manifest_generation <= 0
    ):
        _fail("forward paper operational manifest generation is invalid")
    expected_prefix = f"{_ROOT}/"
    if (
        type(manifest_object_name) is not str
        or not manifest_object_name.startswith(expected_prefix)
        or not manifest_object_name.endswith(".json")
        or ".." in manifest_object_name
        or "\\" in manifest_object_name
    ):
        _fail("forward paper operational manifest object name is invalid")

    failed = False
    raw = None
    try:
        raw = reader.read_generation(
            bucket=bucket,
            object_name=manifest_object_name,
            generation=manifest_generation,
            maximum_bytes=FORWARD_PAPER_OPERATIONAL_MANIFEST_MAXIMUM_BYTES,
        )
    except Exception:
        failed = True
    if (
        failed
        or type(raw) is not GCSObjectPayload
        or raw.generation != manifest_generation
        or type(raw.content_bytes) is not bytes
        or not raw.content_bytes
        or len(raw.content_bytes) > FORWARD_PAPER_OPERATIONAL_MANIFEST_MAXIMUM_BYTES
        or hashlib.sha256(raw.content_bytes).hexdigest() != manifest_sha256
    ):
        _fail("forward paper operational pinned manifest read failed")

    manifest = decode_forward_paper_operational_manifest(raw.content_bytes)
    if (
        manifest.bucket != bucket
        or manifest.graph_id != expected_graph_id
        or forward_paper_operational_manifest_object_name(manifest)
        != manifest_object_name
    ):
        _fail("forward paper operational manifest binding differs")

    failed = False
    source = actions = ticks = graph = None
    try:
        spec = ForwardPaperHistoryWindowSpec(
            dataset_id=manifest.dataset_id,
            signal_session=manifest.signal_session,
            decision_cutoff=manifest.decision_cutoff,
            expected_market_sessions=manifest.expected_market_sessions,
        )
        if spec.spec_id != manifest.source_spec_id:
            raise ValueError
        source = history_windows.build(spec)
        actions = corporate_actions.get(manifest.corporate_action_snapshot_id)
        ticks = tick_panels.get(manifest.tick_panel_id)
        if (
            type(source) is not ForwardPaperRawHistoryWindow
            or source.window_id != manifest.source_window_id
            or source.spec.spec_id != manifest.source_spec_id
            or type(actions) is not CorporateActionSnapshot
            or actions.snapshot_id != manifest.corporate_action_snapshot_id
            or type(ticks) is not VerifiedPromotedEffectiveSessionTickPanel
            or ticks.panel_id != manifest.tick_panel_id
        ):
            raise ValueError
        graph = assemble_forward_paper_operational_research_graph(
            source_window=source,
            corporate_actions=actions,
            tick_panel=ticks,
        )
    except Exception:
        failed = True
    if failed or graph is None:
        _fail("forward paper operational exact artifact restore failed safely")
    if (
        graph.graph_id != manifest.graph_id
        or graph.adjusted_window.window_id != manifest.adjusted_window_id
        or graph.feature_input_window.window_id != manifest.feature_input_window_id
        or graph.technical_feature_window.window_id
        != manifest.technical_feature_window_id
        or graph.source_window.spec.signal_session != manifest.signal_session
        or graph.source_window.spec.decision_cutoff != manifest.decision_cutoff
    ):
        _fail("forward paper operational recomputed graph differs")
    graph.verify_content_identity()
    return graph
