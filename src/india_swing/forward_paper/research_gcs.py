"""Exact-pinned GCS boundary for forward-paper baseline/challenger research."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date

from india_swing.daily_pipeline.acquisition import GCSObjectPayload, GCSObjectReader
from india_swing.daily_pipeline.state_publication import (
    PublishedStateObject,
    StateObjectWriter,
)
from india_swing.features.promoted_cross_section import PromotedCrossSectionConfig
from india_swing.identity import content_id

from .operational_gcs import (
    ForwardPaperCorporateActionSnapshotResolver,
    ForwardPaperEffectiveTickPanelResolver,
    ForwardPaperRawHistoryWindowResolver,
    restore_forward_paper_operational_graph,
)
from .research import (
    ForwardPaperBaselineChallengerRun,
    _run_forward_paper_baseline_challenger_research_from_verified_graph,
)


FORWARD_PAPER_RESEARCH_MANIFEST_SCHEMA_VERSION = (
    "forward-paper-baseline-challenger-manifest-v1"
)
FORWARD_PAPER_RESEARCH_MANIFEST_POLICY_VERSION = (
    "exact-operational-pin-recompute-before-use-v1"
)
FORWARD_PAPER_RESEARCH_MANIFEST_MAXIMUM_BYTES = 32 * 1024

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]\Z")
_SOURCE_OBJECT = re.compile(
    r"research/forward-paper-operational/v1/\d{4}-\d{2}-\d{2}/"
    r"[0-9a-f]{64}/[0-9a-f]{64}\.json\Z"
)
_ROOT = "research/forward-paper-baseline-challenger/v1"
_CONTENT_TYPE = "application/json"


class ForwardPaperResearchManifestError(ValueError):
    """Static, sanitized failure at the durable research boundary."""


def _fail(message: str) -> None:
    raise ForwardPaperResearchManifestError(message)


def _sha(value: object) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        _fail("forward paper research manifest identity is invalid")
    return value


def _bucket(value: object) -> str:
    if type(value) is not str or _BUCKET.fullmatch(value) is None:
        _fail("forward paper research manifest bucket is invalid")
    return value


def _generation(value: object) -> int:
    if type(value) is not int or isinstance(value, bool) or value <= 0:
        _fail("forward paper research manifest generation is invalid")
    return value


def _canonical_bytes(value: dict[str, object]) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


@dataclass(frozen=True, slots=True)
class ForwardPaperOperationalManifestPin:
    bucket: str
    expected_graph_id: str
    object_name: str
    generation: int
    sha256: str
    pin_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "pin_id", self._calculated_id())

    def _validate(self) -> None:
        _bucket(self.bucket)
        _sha(self.expected_graph_id)
        _generation(self.generation)
        _sha(self.sha256)
        if type(self.object_name) is not str or _SOURCE_OBJECT.fullmatch(
            self.object_name
        ) is None:
            _fail("forward paper research source manifest path is invalid")

    def _calculated_id(self) -> str:
        return content_id(
            {
                "bucket": self.bucket,
                "expected_graph_id": self.expected_graph_id,
                "generation": self.generation,
                "object_name": self.object_name,
                "sha256": self.sha256,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.pin_id != self._calculated_id():
            _fail("forward paper research source manifest pin identity failed")


@dataclass(frozen=True, slots=True)
class ForwardPaperResearchRunManifest:
    bucket: str
    signal_session: date
    source_pin: ForwardPaperOperationalManifestPin
    run_id: str
    baseline_config_id: str
    challenger_config_id: str
    baseline_arm_id: str
    challenger_arm_id: str
    comparison_top_tiers: int
    baseline_top_count: int
    challenger_top_count: int
    overlap_count: int
    schema_version: str = FORWARD_PAPER_RESEARCH_MANIFEST_SCHEMA_VERSION
    policy_version: str = FORWARD_PAPER_RESEARCH_MANIFEST_POLICY_VERSION
    manifest_id: str = field(init=False)

    def __post_init__(self) -> None:
        self._validate()
        object.__setattr__(self, "manifest_id", self._calculated_id())

    def _validate(self) -> None:
        _bucket(self.bucket)
        if type(self.signal_session) is not date:
            _fail("forward paper research manifest signal session is invalid")
        if type(self.source_pin) is not ForwardPaperOperationalManifestPin:
            _fail("forward paper research manifest source pin is invalid")
        failed = False
        try:
            self.source_pin.verify_content_identity()
        except Exception:
            failed = True
        if failed:
            _fail("forward paper research manifest source pin failed verification")
        for value in (
            self.run_id,
            self.baseline_config_id,
            self.challenger_config_id,
            self.baseline_arm_id,
            self.challenger_arm_id,
        ):
            _sha(value)
        if self.baseline_config_id == self.challenger_config_id:
            _fail("forward paper research manifest configurations are invalid")
        if (
            type(self.comparison_top_tiers) is not int
            or isinstance(self.comparison_top_tiers, bool)
            or self.comparison_top_tiers <= 0
            or any(
                type(value) is not int or isinstance(value, bool) or value < 0
                for value in (
                    self.baseline_top_count,
                    self.challenger_top_count,
                    self.overlap_count,
                )
            )
            or self.overlap_count
            > min(self.baseline_top_count, self.challenger_top_count)
        ):
            _fail("forward paper research manifest comparison is invalid")
        if self.schema_version != FORWARD_PAPER_RESEARCH_MANIFEST_SCHEMA_VERSION:
            _fail("forward paper research manifest schema is invalid")
        if self.policy_version != FORWARD_PAPER_RESEARCH_MANIFEST_POLICY_VERSION:
            _fail("forward paper research manifest policy is invalid")

    def _calculated_id(self) -> str:
        return content_id(_manifest_body(self, include_manifest_id=False), length=64)

    def verify_content_identity(self) -> None:
        self._validate()
        if self.manifest_id != self._calculated_id():
            _fail("forward paper research manifest identity failed")


def _source_pin_body(value: ForwardPaperOperationalManifestPin) -> dict[str, object]:
    return {
        "bucket": value.bucket,
        "expected_graph_id": value.expected_graph_id,
        "generation": value.generation,
        "object_name": value.object_name,
        "pin_id": value.pin_id,
        "sha256": value.sha256,
    }


def _manifest_body(
    value: ForwardPaperResearchRunManifest, *, include_manifest_id: bool
) -> dict[str, object]:
    body: dict[str, object] = {
        "baseline_arm_id": value.baseline_arm_id,
        "baseline_config_id": value.baseline_config_id,
        "baseline_top_count": value.baseline_top_count,
        "bucket": value.bucket,
        "challenger_arm_id": value.challenger_arm_id,
        "challenger_config_id": value.challenger_config_id,
        "challenger_top_count": value.challenger_top_count,
        "comparison_top_tiers": value.comparison_top_tiers,
        "overlap_count": value.overlap_count,
        "policy_version": value.policy_version,
        "run_id": value.run_id,
        "schema_version": value.schema_version,
        "signal_session": value.signal_session.isoformat(),
        "source_pin": _source_pin_body(value.source_pin),
    }
    if include_manifest_id:
        body["manifest_id"] = value.manifest_id
    return body


def research_run_manifest_from_run(
    *,
    run: ForwardPaperBaselineChallengerRun,
    source_pin: ForwardPaperOperationalManifestPin,
    bucket: str,
) -> ForwardPaperResearchRunManifest:
    failed = False
    try:
        if type(run) is not ForwardPaperBaselineChallengerRun:
            raise ValueError
        run.verify_content_identity()
        source_pin.verify_content_identity()
    except Exception:
        failed = True
    if failed:
        _fail("forward paper research run failed manifest verification")
    if run.source_graph.graph_id != source_pin.expected_graph_id:
        _fail("forward paper research run source binding differs")
    return ForwardPaperResearchRunManifest(
        bucket=_bucket(bucket),
        signal_session=run.source_graph.source_window.spec.signal_session,
        source_pin=source_pin,
        run_id=run.run_id,
        baseline_config_id=run.baseline.config.config_id,
        challenger_config_id=run.challenger.config.config_id,
        baseline_arm_id=run.baseline.arm_id,
        challenger_arm_id=run.challenger.arm_id,
        comparison_top_tiers=run.comparison_top_tiers,
        baseline_top_count=run.baseline_top_count,
        challenger_top_count=run.challenger_top_count,
        overlap_count=run.overlap_count,
    )


def _research_run_manifest_from_verified_run(
    *,
    run: ForwardPaperBaselineChallengerRun,
    source_pin: ForwardPaperOperationalManifestPin,
    bucket: str,
) -> ForwardPaperResearchRunManifest:
    """Bind a run derived immediately from an exact restored graph."""

    failed = False
    try:
        if (
            type(run) is not ForwardPaperBaselineChallengerRun
            or run.run_id != run._calculated_id()
            or run.baseline.arm_id != run.baseline._calculated_id()
            or run.challenger.arm_id != run.challenger._calculated_id()
        ):
            raise ValueError
        source_pin.verify_content_identity()
    except Exception:
        failed = True
    if failed:
        _fail("forward paper research run failed manifest verification")
    if run.source_graph.graph_id != source_pin.expected_graph_id:
        _fail("forward paper research run source binding differs")
    return ForwardPaperResearchRunManifest(
        bucket=_bucket(bucket),
        signal_session=run.source_graph.source_window.spec.signal_session,
        source_pin=source_pin,
        run_id=run.run_id,
        baseline_config_id=run.baseline.config.config_id,
        challenger_config_id=run.challenger.config.config_id,
        baseline_arm_id=run.baseline.arm_id,
        challenger_arm_id=run.challenger.arm_id,
        comparison_top_tiers=run.comparison_top_tiers,
        baseline_top_count=run.baseline_top_count,
        challenger_top_count=run.challenger_top_count,
        overlap_count=run.overlap_count,
    )


def encode_forward_paper_research_manifest(
    manifest: ForwardPaperResearchRunManifest,
) -> bytes:
    if type(manifest) is not ForwardPaperResearchRunManifest:
        _fail("forward paper research manifest type is invalid")
    manifest.verify_content_identity()
    payload = _canonical_bytes(_manifest_body(manifest, include_manifest_id=True))
    if not payload or len(payload) > FORWARD_PAPER_RESEARCH_MANIFEST_MAXIMUM_BYTES:
        _fail("forward paper research manifest payload is invalid")
    return payload


def _object_no_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _reject_number(_: str) -> object:
    raise ValueError


def decode_forward_paper_research_manifest(
    payload: bytes,
) -> ForwardPaperResearchRunManifest:
    raw: object = None
    failed = False
    try:
        if (
            type(payload) is not bytes
            or not payload
            or len(payload) > FORWARD_PAPER_RESEARCH_MANIFEST_MAXIMUM_BYTES
        ):
            raise ValueError
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_object_no_duplicates,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
    except Exception:
        failed = True
    if failed or type(raw) is not dict:
        _fail("forward paper research manifest payload is invalid")
    expected = {
        "baseline_arm_id",
        "baseline_config_id",
        "baseline_top_count",
        "bucket",
        "challenger_arm_id",
        "challenger_config_id",
        "challenger_top_count",
        "comparison_top_tiers",
        "manifest_id",
        "overlap_count",
        "policy_version",
        "run_id",
        "schema_version",
        "signal_session",
        "source_pin",
    }
    source_expected = {
        "bucket",
        "expected_graph_id",
        "generation",
        "object_name",
        "pin_id",
        "sha256",
    }
    if set(raw) != expected or type(raw["source_pin"]) is not dict:
        _fail("forward paper research manifest payload shape is invalid")
    source_raw = raw["source_pin"]
    if set(source_raw) != source_expected:
        _fail("forward paper research manifest payload shape is invalid")
    constructed = None
    failed = False
    try:
        source_pin = ForwardPaperOperationalManifestPin(
            bucket=source_raw["bucket"],
            expected_graph_id=source_raw["expected_graph_id"],
            object_name=source_raw["object_name"],
            generation=source_raw["generation"],
            sha256=source_raw["sha256"],
        )
        if source_pin.pin_id != source_raw["pin_id"]:
            raise ValueError
        constructed = ForwardPaperResearchRunManifest(
            bucket=raw["bucket"],
            signal_session=date.fromisoformat(raw["signal_session"]),
            source_pin=source_pin,
            run_id=raw["run_id"],
            baseline_config_id=raw["baseline_config_id"],
            challenger_config_id=raw["challenger_config_id"],
            baseline_arm_id=raw["baseline_arm_id"],
            challenger_arm_id=raw["challenger_arm_id"],
            comparison_top_tiers=raw["comparison_top_tiers"],
            baseline_top_count=raw["baseline_top_count"],
            challenger_top_count=raw["challenger_top_count"],
            overlap_count=raw["overlap_count"],
            schema_version=raw["schema_version"],
            policy_version=raw["policy_version"],
        )
    except Exception:
        failed = True
    if (
        failed
        or constructed is None
        or constructed.manifest_id != raw["manifest_id"]
        or encode_forward_paper_research_manifest(constructed) != payload
    ):
        _fail("forward paper research manifest payload failed verification")
    return constructed


def forward_paper_research_manifest_object_name(
    manifest: ForwardPaperResearchRunManifest,
) -> str:
    if type(manifest) is not ForwardPaperResearchRunManifest:
        _fail("forward paper research manifest type is invalid")
    manifest.verify_content_identity()
    return (
        f"{_ROOT}/{manifest.signal_session.isoformat()}/"
        f"{manifest.source_pin.expected_graph_id}/{manifest.run_id}/"
        f"{manifest.manifest_id}.json"
    )


def _published(value: object) -> PublishedStateObject:
    result = None
    failed = False
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
        _fail("forward paper research published object is invalid")
    return result


@dataclass(frozen=True, slots=True)
class CompletedForwardPaperResearchPublication:
    manifest: ForwardPaperResearchRunManifest
    manifest_object: PublishedStateObject

    def __post_init__(self) -> None:
        if type(self.manifest) is not ForwardPaperResearchRunManifest:
            _fail("forward paper research publication manifest is invalid")
        self.manifest.verify_content_identity()
        payload = encode_forward_paper_research_manifest(self.manifest)
        published = _published(self.manifest_object)
        if (
            published.object_name
            != forward_paper_research_manifest_object_name(self.manifest)
            or published.byte_count != len(payload)
            or published.sha256 != hashlib.sha256(payload).hexdigest()
        ):
            _fail("forward paper research published manifest differs")


def _publish_forward_paper_research_manifest(
    manifest: ForwardPaperResearchRunManifest,
    writer: StateObjectWriter,
) -> CompletedForwardPaperResearchPublication:
    payload = encode_forward_paper_research_manifest(manifest)
    object_name = forward_paper_research_manifest_object_name(manifest)
    published = None
    failed = False
    try:
        if not callable(getattr(writer, "create_or_verify", None)):
            raise ValueError
        published = writer.create_or_verify(
            bucket=manifest.bucket,
            object_name=object_name,
            content_bytes=payload,
            content_type=_CONTENT_TYPE,
            maximum_bytes=FORWARD_PAPER_RESEARCH_MANIFEST_MAXIMUM_BYTES,
        )
    except Exception:
        failed = True
    if failed:
        _fail("forward paper research manifest publication failed safely")
    published = _published(published)
    if (
        published.object_name != object_name
        or published.byte_count != len(payload)
        or published.sha256 != hashlib.sha256(payload).hexdigest()
    ):
        _fail("forward paper research published manifest differs")
    return CompletedForwardPaperResearchPublication(
        manifest=manifest,
        manifest_object=published,
    )


def publish_forward_paper_research_run(
    *,
    run: ForwardPaperBaselineChallengerRun,
    source_pin: ForwardPaperOperationalManifestPin,
    bucket: str,
    writer: StateObjectWriter,
) -> CompletedForwardPaperResearchPublication:
    manifest = research_run_manifest_from_run(
        run=run,
        source_pin=source_pin,
        bucket=bucket,
    )
    return _publish_forward_paper_research_manifest(manifest, writer)


def _publish_forward_paper_research_run_from_verified_run(
    *,
    run: ForwardPaperBaselineChallengerRun,
    source_pin: ForwardPaperOperationalManifestPin,
    bucket: str,
    writer: StateObjectWriter,
) -> CompletedForwardPaperResearchPublication:
    manifest = _research_run_manifest_from_verified_run(
        run=run,
        source_pin=source_pin,
        bucket=bucket,
    )
    return _publish_forward_paper_research_manifest(manifest, writer)


def restore_forward_paper_research_run(
    *,
    expected_run_id: str,
    bucket: str,
    manifest_object_name: str,
    manifest_generation: int,
    manifest_sha256: str,
    baseline_config: PromotedCrossSectionConfig,
    challenger_config: PromotedCrossSectionConfig,
    reader: GCSObjectReader,
    history_windows: ForwardPaperRawHistoryWindowResolver,
    corporate_actions: ForwardPaperCorporateActionSnapshotResolver,
    tick_panels: ForwardPaperEffectiveTickPanelResolver,
) -> ForwardPaperBaselineChallengerRun:
    expected_run_id = _sha(expected_run_id)
    bucket = _bucket(bucket)
    manifest_generation = _generation(manifest_generation)
    manifest_sha256 = _sha(manifest_sha256)
    if (
        type(manifest_object_name) is not str
        or not manifest_object_name.startswith(f"{_ROOT}/")
        or not manifest_object_name.endswith(".json")
        or ".." in manifest_object_name
        or "\\" in manifest_object_name
    ):
        _fail("forward paper research manifest object name is invalid")
    raw = None
    failed = False
    try:
        raw = reader.read_generation(
            bucket=bucket,
            object_name=manifest_object_name,
            generation=manifest_generation,
            maximum_bytes=FORWARD_PAPER_RESEARCH_MANIFEST_MAXIMUM_BYTES,
        )
    except Exception:
        failed = True
    if (
        failed
        or type(raw) is not GCSObjectPayload
        or raw.generation != manifest_generation
        or type(raw.content_bytes) is not bytes
        or not raw.content_bytes
        or len(raw.content_bytes) > FORWARD_PAPER_RESEARCH_MANIFEST_MAXIMUM_BYTES
        or hashlib.sha256(raw.content_bytes).hexdigest() != manifest_sha256
    ):
        _fail("forward paper research pinned manifest read failed")
    manifest = decode_forward_paper_research_manifest(raw.content_bytes)
    if (
        manifest.bucket != bucket
        or manifest.run_id != expected_run_id
        or forward_paper_research_manifest_object_name(manifest)
        != manifest_object_name
    ):
        _fail("forward paper research manifest binding differs")
    failed = False
    run = None
    try:
        baseline_config.verify_content_identity()
        challenger_config.verify_content_identity()
        if (
            baseline_config.config_id != manifest.baseline_config_id
            or challenger_config.config_id != manifest.challenger_config_id
        ):
            raise ValueError
        pin = manifest.source_pin
        graph = restore_forward_paper_operational_graph(
            expected_graph_id=pin.expected_graph_id,
            bucket=pin.bucket,
            manifest_object_name=pin.object_name,
            manifest_generation=pin.generation,
            manifest_sha256=pin.sha256,
            reader=reader,
            history_windows=history_windows,
            corporate_actions=corporate_actions,
            tick_panels=tick_panels,
        )
        run = _run_forward_paper_baseline_challenger_research_from_verified_graph(
            source_graph=graph,
            baseline_config=baseline_config,
            challenger_config=challenger_config,
            comparison_top_tiers=manifest.comparison_top_tiers,
        )
    except Exception:
        failed = True
    if failed or run is None:
        _fail("forward paper research exact artifact restore failed safely")
    if (
        run.run_id != manifest.run_id
        or run.source_graph.source_window.spec.signal_session
        != manifest.signal_session
        or run.baseline.arm_id != manifest.baseline_arm_id
        or run.challenger.arm_id != manifest.challenger_arm_id
        or run.baseline_top_count != manifest.baseline_top_count
        or run.challenger_top_count != manifest.challenger_top_count
        or run.overlap_count != manifest.overlap_count
    ):
        _fail("forward paper research recomputed run differs")
    return run
