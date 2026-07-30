"""One restart-safe, exact-manifest bridge from a published promoted graph
into the existing promoted engine.

This module never republishes the upstream promoted graph and never invents
a trading-calendar, sizing, cost, liquidity, stop, target, or exposure
policy: every engine root pin is *derived* from one exact, already-verified
``PromotedGraphPublicationManifest`` (never accepted or overridden
separately), and the existing engine services/config types run unchanged. A
combined publication is paper research evidence only -- ``paper_only`` is
always true and both ``notification_eligible``/``execution_eligible`` are
always false, regardless of the resolved graph/engine readiness.
"""

from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from india_swing._exact_replay import ExactReplayScope
from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.evaluation.promoted_intents import PromotedIntentPolicyConfig
from india_swing.features.promoted_cross_section import PromotedCrossSectionConfig
from india_swing.features.promoted_technical import PromotedTechnicalFeatureConfig
from india_swing.identity import content_id
from india_swing.promoted_engine import (
    PromotedEngineRunManifest,
    PromotedEngineRunRequest,
    PromotedEngineRunner,
    PromotedEngineStores,
    build_promoted_engine_downstream_stores,
)
from india_swing.promoted_graph_publisher import (
    LocalPromotedGraphPublicationStore,
    PromotedGraphPublicationManifest,
    ReferenceReadiness,
    build_promoted_graph_stores,
)


class PromotedResearchError(ValueError):
    pass


class PromotedResearchConflict(PromotedResearchError):
    pass


class PromotedResearchNotFound(PromotedResearchError):
    pass


PROMOTED_RESEARCH_REQUEST_SCHEMA_VERSION = "promoted-research-run-request/v1"
PROMOTED_RESEARCH_MANIFEST_SCHEMA_VERSION = "promoted-research-run-manifest/v1"
PROMOTED_RESEARCH_MANIFEST_CODEC_VERSION = "promoted-research-run-manifest-json/v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAXIMUM_MANIFEST_BYTES = 1 * 1024 * 1024

_ERR_TYPE = "promoted research type is invalid"
_ERR_REQUEST = "promoted research request is invalid"
_ERR_ID = "promoted research identifier is invalid"
_ERR_GRAPH_SOURCE = "promoted research could not resolve its exact graph manifest"
_ERR_GRAPH_NOT_ELIGIBLE = (
    "promoted research resolved graph manifest is not eligible for a paper"
    " research run"
)
_ERR_ENGINE = "promoted research could not run the promoted engine"
_ERR_GRAPH = "promoted research manifest graph is invalid"
_ERR_REQUEST_MISMATCH = (
    "promoted research manifest request preimage does not recompute to its"
    " stored research_request_id"
)
_ERR_VERIFY = "promoted research manifest could not be verified"
_ERR_CONFLICT = "promoted research run already stores different content"
_ERR_NOT_FOUND = "promoted research run was not found"
_ERR_UNSAFE_PATH = "promoted research run path is unsafe"
_ERR_BYTES = "promoted research run manifest bytes are invalid"
_ERR_SHAPE = "promoted research run manifest shape is invalid"


def _sha(value: object) -> bool:
    return type(value) is str and _SHA256.fullmatch(value) is not None


def _require_sha(value: object, message: str) -> str:
    if not _sha(value):
        raise PromotedResearchError(message)
    return value  # type: ignore[return-value]


def _utc(value: object, message: str) -> datetime:
    if type(value) is not datetime:
        raise PromotedResearchError(message)
    try:
        offset = value.utcoffset()
    except Exception:
        raise PromotedResearchError(message) from None
    if value.tzinfo is None or offset is None:
        raise PromotedResearchError(message)
    return value.astimezone(timezone.utc)


def _positive_decimal(value: object, message: str) -> Decimal:
    if type(value) is not str:
        raise PromotedResearchError(message)
    try:
        result = Decimal(value)
    except InvalidOperation:
        raise PromotedResearchError(message) from None
    if not result.is_finite() or result <= 0 or str(result) != value:
        raise PromotedResearchError(message)
    return result


def _canonical_date(value: object, message: str) -> date:
    if type(value) is not str:
        raise PromotedResearchError(message)
    try:
        result = date.fromisoformat(value)
    except ValueError:
        raise PromotedResearchError(message) from None
    if result.isoformat() != value:
        raise PromotedResearchError(message)
    return result


def _canonical_datetime(value: object, message: str) -> datetime:
    if type(value) is not str:
        raise PromotedResearchError(message)
    try:
        result = datetime.fromisoformat(value)
    except ValueError:
        raise PromotedResearchError(message) from None
    offset = result.utcoffset()
    if (
        result.tzinfo is None
        or offset is None
        or offset.total_seconds() != 0
        or result.isoformat() != value
    ):
        raise PromotedResearchError(message)
    return result


def _compute_research_request_id(
    *,
    graph_manifest_id: str,
    signal_session: date,
    entry_session: date,
    cutoff: datetime,
    initial_capital: Decimal,
    technical_config_id: str,
    cross_section_config_id: str,
    intent_config_id: str,
) -> str:
    """The single research-request-identity derivation shared by the request
    and the manifest.

    Both ``PromotedResearchRunRequest`` (at construction) and
    ``PromotedResearchRunManifest`` (on every decode/verify) call this exact
    function over the same named fields, so a durable manifest can prove its
    retained root-pin preimage recomputes to its own stored
    ``research_request_id`` rather than persisting an opaque, unverifiable
    hash.
    """

    return content_id(
        {
            "schema_version": PROMOTED_RESEARCH_REQUEST_SCHEMA_VERSION,
            "graph_manifest_id": graph_manifest_id,
            "signal_session": signal_session,
            "entry_session": entry_session,
            "cutoff": cutoff,
            "initial_capital": initial_capital,
            "technical_config_id": technical_config_id,
            "cross_section_config_id": cross_section_config_id,
            "intent_config_id": intent_config_id,
        },
        length=64,
    )


@dataclass(frozen=True, slots=True)
class PromotedResearchRunRequest:
    """Immutable, content-addressed request for one combined paper research run.

    Every engine root pin (``adjustment_bridge_id``,
    ``effective_tick_panel_id``, the reference-promotion set, the
    corporate-action snapshot) is deliberately absent here: it is derived
    from the exact resolved ``graph_manifest_id`` at run time, never
    supplied or overridden separately.
    """

    graph_manifest_id: str
    signal_session: date
    entry_session: date
    cutoff: datetime
    initial_capital: Decimal
    technical_config: PromotedTechnicalFeatureConfig
    cross_section_config: PromotedCrossSectionConfig
    intent_config: PromotedIntentPolicyConfig
    research_request_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            not _sha(self.graph_manifest_id)
            or type(self.signal_session) is not date
            or type(self.entry_session) is not date
            or self.entry_session <= self.signal_session
            or type(self.technical_config) is not PromotedTechnicalFeatureConfig
            or type(self.cross_section_config) is not PromotedCrossSectionConfig
            or type(self.intent_config) is not PromotedIntentPolicyConfig
            or type(self.initial_capital) is not Decimal
            or not self.initial_capital.is_finite()
            or self.initial_capital <= 0
        ):
            raise PromotedResearchError(_ERR_REQUEST)
        cutoff = _utc(self.cutoff, _ERR_REQUEST)
        try:
            self.technical_config.verify_content_identity()
            self.cross_section_config.verify_content_identity()
            self.intent_config.verify_content_identity()
        except Exception:
            raise PromotedResearchError(_ERR_REQUEST) from None
        if cutoff != self.cutoff:
            object.__setattr__(self, "cutoff", cutoff)
        object.__setattr__(self, "research_request_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return _compute_research_request_id(
            graph_manifest_id=self.graph_manifest_id,
            signal_session=self.signal_session,
            entry_session=self.entry_session,
            cutoff=self.cutoff,
            initial_capital=self.initial_capital,
            technical_config_id=self.technical_config.config_id,
            cross_section_config_id=self.cross_section_config.config_id,
            intent_config_id=self.intent_config.config_id,
        )

    def verify_content_identity(self) -> None:
        if type(self) is not PromotedResearchRunRequest:
            raise PromotedResearchError(_ERR_TYPE)
        if self.research_request_id != self._calculated_id():
            raise PromotedResearchError(_ERR_ID)


@dataclass(frozen=True, slots=True)
class PromotedResearchRunManifest:
    """Canonical, content-derived, create-once manifest for one combined
    graph-to-engine paper research run.

    Retains the complete research-request preimage (``graph_manifest_id``,
    sessions, cutoff, capital, and all three config IDs) alongside every
    graph root pin and every engine output ID, so a durable manifest can
    prove its own ``research_request_id`` recomputes from its own retained
    fields rather than persisting an opaque, unverifiable hash. Grants no
    trading, alert, or execution authority: ``paper_only`` is always true and
    both ``notification_eligible``/``execution_eligible`` are always false.
    """

    schema_version: str
    research_request_id: str
    graph_manifest_id: str
    graph_spec_id: str
    adjustment_bridge_id: str
    effective_tick_panel_id: str
    expected_reference_promotion_ids: tuple[str, ...]
    expected_corporate_action_snapshot_id: str
    engine_request_id: str
    engine_run_id: str
    feature_input_panel_id: str
    technical_config_id: str
    technical_panel_id: str
    cross_section_config_id: str
    cross_section_panel_id: str
    intent_config_id: str
    research_intent_batch_id: str
    replay_run_id: str
    signal_session: date
    entry_session: date
    cutoff: datetime
    initial_capital: Decimal
    candidate_count: int
    intent_count: int
    adjustment_readiness: ReferenceReadiness
    adjustment_actionable: bool
    effective_tick_readiness: ReferenceReadiness
    effective_tick_actionable: bool
    paper_only: bool
    notification_eligible: bool
    execution_eligible: bool
    research_run_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            self.schema_version != PROMOTED_RESEARCH_MANIFEST_SCHEMA_VERSION
            or not _sha(self.graph_manifest_id)
            or not _sha(self.graph_spec_id)
            or not _sha(self.adjustment_bridge_id)
            or not _sha(self.effective_tick_panel_id)
            or type(self.expected_reference_promotion_ids) is not tuple
            or not self.expected_reference_promotion_ids
            or any(
                not _sha(value)
                for value in self.expected_reference_promotion_ids
            )
            or self.expected_reference_promotion_ids
            != tuple(sorted(set(self.expected_reference_promotion_ids)))
            or not _sha(self.expected_corporate_action_snapshot_id)
            or not _sha(self.engine_request_id)
            or not _sha(self.engine_run_id)
            or not _sha(self.feature_input_panel_id)
            or not _sha(self.technical_config_id)
            or not _sha(self.technical_panel_id)
            or not _sha(self.cross_section_config_id)
            or not _sha(self.cross_section_panel_id)
            or not _sha(self.intent_config_id)
            or not _sha(self.research_intent_batch_id)
            or not _sha(self.replay_run_id)
            or type(self.signal_session) is not date
            or type(self.entry_session) is not date
            or self.entry_session <= self.signal_session
            or type(self.candidate_count) is not int
            or self.candidate_count <= 0
            or type(self.intent_count) is not int
            or self.intent_count < 0
            or self.intent_count > self.candidate_count
            or type(self.adjustment_readiness) is not ReferenceReadiness
            or type(self.adjustment_actionable) is not bool
            or type(self.effective_tick_readiness) is not ReferenceReadiness
            or type(self.effective_tick_actionable) is not bool
            or self.paper_only is not True
            or self.notification_eligible is not False
            or self.execution_eligible is not False
            or type(self.initial_capital) is not Decimal
            or not self.initial_capital.is_finite()
            or self.initial_capital <= 0
        ):
            raise PromotedResearchError(_ERR_GRAPH)
        cutoff = _utc(self.cutoff, _ERR_GRAPH)
        if cutoff != self.cutoff:
            object.__setattr__(self, "cutoff", cutoff)
        expected_research_request_id = _compute_research_request_id(
            graph_manifest_id=self.graph_manifest_id,
            signal_session=self.signal_session,
            entry_session=self.entry_session,
            cutoff=self.cutoff,
            initial_capital=self.initial_capital,
            technical_config_id=self.technical_config_id,
            cross_section_config_id=self.cross_section_config_id,
            intent_config_id=self.intent_config_id,
        )
        if self.research_request_id != expected_research_request_id:
            raise PromotedResearchError(_ERR_REQUEST_MISMATCH)
        object.__setattr__(self, "research_run_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            value.name: getattr(self, value.name)
            for value in fields(self)
            if value.name != "research_run_id"
        }

    def _calculated_id(self) -> str:
        return content_id(
            {"schema": PROMOTED_RESEARCH_MANIFEST_SCHEMA_VERSION, **self._identity()},
            length=64,
        )

    def verify_content_identity(self) -> None:
        if type(self) is not PromotedResearchRunManifest:
            raise PromotedResearchError(_ERR_TYPE)
        expected = PromotedResearchRunManifest(**self._identity())
        if self.research_run_id != expected.research_run_id:
            raise PromotedResearchError(_ERR_ID)


@dataclass(slots=True)
class PromotedResearchStores:
    """Local exact-store composition boundary for the promoted-research bridge.

    Exposes only the exact graph-publication resolver, the existing
    ``PromotedEngineStores``, and the new combined-manifest store -- no
    list/latest/nearest/find/discovery capability anywhere in this
    composition.
    """

    graph_publications: LocalPromotedGraphPublicationStore
    engine: PromotedEngineStores
    research_runs: "LocalPromotedResearchRunStore"
    _replay_scope: ExactReplayScope = field(repr=False)


def build_promoted_research_stores(
    *,
    reference_root: Path,
    identity_evidence_root: Path,
    calendar_root: Path,
    daily_reports_root: Path,
    historical_corpus_root: Path,
    promoted_root: Path,
    graph_publication_root: Path,
    engine_run_root: Path,
    research_run_root: Path,
) -> PromotedResearchStores:
    """Construct every real durable store from nine explicit roots.

    This is the only place the bridge's stores are assembled. It builds the
    promoted graph exactly once via ``build_promoted_graph_stores`` and then
    composes the engine's own downstream stores from that graph's exact
    ``corporate_action_adjustments``/``effective_session_ticks`` stores via
    ``build_promoted_engine_downstream_stores``, reusing the graph's own
    operation-scoped replay cache throughout -- it never builds a second,
    independent upstream identity/session/history resolver graph, and it
    never accepts a caller-supplied fake in place of a real store.
    """

    graph_stores = build_promoted_graph_stores(
        reference_root=reference_root,
        identity_evidence_root=identity_evidence_root,
        calendar_root=calendar_root,
        daily_reports_root=daily_reports_root,
        historical_corpus_root=historical_corpus_root,
        promoted_root=promoted_root,
        publication_root=graph_publication_root,
    )
    replay_scope = graph_stores._replay_scope
    engine_stores = build_promoted_engine_downstream_stores(
        corporate_action_adjustments=graph_stores.corporate_action_adjustments,
        effective_session_ticks=graph_stores.effective_session_ticks,
        promoted_root=promoted_root,
        engine_run_root=engine_run_root,
        replay_scope=replay_scope,
    )
    research_runs = LocalPromotedResearchRunStore(
        research_run_root,
        graph_publications=graph_stores.publications,
        engine_runs=engine_stores.engine_runs,
        replay_scope=replay_scope,
    )
    return PromotedResearchStores(
        graph_publications=graph_stores.publications,
        engine=engine_stores,
        research_runs=research_runs,
        _replay_scope=replay_scope,
    )


class PromotedResearchOrchestrator:
    """Derives the engine's exact root pins from one published promoted
    graph, runs the existing engine unchanged, and durably binds both
    terminal IDs.

    Never accepts ``adjustment_bridge_id``/``effective_tick_panel_id``/the
    reference-promotion set/the corporate-action snapshot ID separately from
    the resolved graph manifest, and never calls a lower-level feature/
    cross-section/intent service directly. A collection-only, not-yet-
    actionable graph is not rejected: its exact readiness/actionable
    projections are preserved unchanged on the combined manifest, and the
    existing engine already runs correctly for collection-only paper
    research (readiness/actionability is descriptive evidence carried
    through, never a gate to trading authority -- ``paper_only`` stays true
    and both ``notification_eligible``/``execution_eligible`` stay false
    regardless).
    """

    def run(
        self,
        request: PromotedResearchRunRequest,
        stores: PromotedResearchStores,
    ) -> PromotedResearchRunManifest:
        if type(request) is not PromotedResearchRunRequest:
            raise TypeError("promoted research request must be exact")
        if type(stores) is not PromotedResearchStores:
            raise TypeError("promoted research stores must be exact")
        request.verify_content_identity()

        with stores._replay_scope.open():
            research_run_id = self._run_within_scope(request, stores).research_run_id
        # One final cold get, entirely outside the scope just closed: this
        # proves the published combined manifest independently re-verifies
        # both the graph and the engine run from scratch rather than
        # inheriting trust from the construction above.
        return stores.research_runs.get(research_run_id)

    def _run_within_scope(
        self,
        request: PromotedResearchRunRequest,
        stores: PromotedResearchStores,
    ) -> PromotedResearchRunManifest:
        try:
            graph_manifest = stores.graph_publications.get(request.graph_manifest_id)
        except Exception:
            raise PromotedResearchError(_ERR_GRAPH_SOURCE) from None
        if (
            type(graph_manifest) is not PromotedGraphPublicationManifest
            or graph_manifest.manifest_id != request.graph_manifest_id
            or graph_manifest.paper_only is not True
            or graph_manifest.execution_eligible is not False
        ):
            raise PromotedResearchError(_ERR_GRAPH_NOT_ELIGIBLE)

        expected_reference_promotion_ids = tuple(
            sorted(value.promotion_id for value in graph_manifest.promotion_bindings)
        )

        try:
            engine_request = PromotedEngineRunRequest(
                adjustment_bridge_id=graph_manifest.adjustment_bridge_id,
                effective_tick_panel_id=graph_manifest.effective_tick_panel_id,
                expected_reference_promotion_ids=expected_reference_promotion_ids,
                expected_corporate_action_snapshot_id=(
                    graph_manifest.corporate_action_snapshot_id
                ),
                signal_session=request.signal_session,
                entry_session=request.entry_session,
                cutoff=request.cutoff,
                initial_capital=request.initial_capital,
                technical_config=request.technical_config,
                cross_section_config=request.cross_section_config,
                intent_config=request.intent_config,
            )
            engine_manifest = PromotedEngineRunner().run(engine_request, stores.engine)
        except Exception:
            raise PromotedResearchError(_ERR_ENGINE) from None

        manifest = PromotedResearchRunManifest(
            schema_version=PROMOTED_RESEARCH_MANIFEST_SCHEMA_VERSION,
            research_request_id=request.research_request_id,
            graph_manifest_id=graph_manifest.manifest_id,
            graph_spec_id=graph_manifest.spec_id,
            adjustment_bridge_id=graph_manifest.adjustment_bridge_id,
            effective_tick_panel_id=graph_manifest.effective_tick_panel_id,
            expected_reference_promotion_ids=expected_reference_promotion_ids,
            expected_corporate_action_snapshot_id=(
                graph_manifest.corporate_action_snapshot_id
            ),
            engine_request_id=engine_manifest.request_id,
            engine_run_id=engine_manifest.run_id,
            feature_input_panel_id=engine_manifest.feature_input_panel_id,
            technical_config_id=engine_manifest.technical_config_id,
            technical_panel_id=engine_manifest.technical_panel_id,
            cross_section_config_id=engine_manifest.cross_section_config_id,
            cross_section_panel_id=engine_manifest.cross_section_panel_id,
            intent_config_id=engine_manifest.intent_config_id,
            research_intent_batch_id=engine_manifest.research_intent_batch_id,
            replay_run_id=engine_manifest.replay_run_id,
            signal_session=request.signal_session,
            entry_session=request.entry_session,
            cutoff=request.cutoff,
            initial_capital=request.initial_capital,
            candidate_count=engine_manifest.candidate_count,
            intent_count=engine_manifest.intent_count,
            adjustment_readiness=graph_manifest.adjustment_readiness,
            adjustment_actionable=graph_manifest.adjustment_actionable,
            effective_tick_readiness=graph_manifest.effective_tick_readiness,
            effective_tick_actionable=graph_manifest.effective_tick_actionable,
            paper_only=True,
            notification_eligible=False,
            execution_eligible=False,
        )
        return stores.research_runs.put(manifest)


_MANIFEST_KEYS = frozenset(
    {
        "codec_schema_version",
        "schema_version",
        "research_request_id",
        "graph_manifest_id",
        "graph_spec_id",
        "adjustment_bridge_id",
        "effective_tick_panel_id",
        "expected_reference_promotion_ids",
        "expected_corporate_action_snapshot_id",
        "engine_request_id",
        "engine_run_id",
        "feature_input_panel_id",
        "technical_config_id",
        "technical_panel_id",
        "cross_section_config_id",
        "cross_section_panel_id",
        "intent_config_id",
        "research_intent_batch_id",
        "replay_run_id",
        "signal_session",
        "entry_session",
        "cutoff",
        "initial_capital",
        "candidate_count",
        "intent_count",
        "adjustment_readiness",
        "adjustment_actionable",
        "effective_tick_readiness",
        "effective_tick_actionable",
        "paper_only",
        "notification_eligible",
        "execution_eligible",
        "research_run_id",
    }
)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedResearchError(_ERR_SHAPE)
        result[key] = value
    return result


def _reject_number(_token: str) -> object:
    raise PromotedResearchError(_ERR_SHAPE)


def encode_promoted_research_run_manifest(
    manifest: PromotedResearchRunManifest,
) -> bytes:
    if type(manifest) is not PromotedResearchRunManifest:
        raise TypeError("promoted research run manifest must be exact")
    manifest.verify_content_identity()
    return (
        json.dumps(
            {
                "codec_schema_version": PROMOTED_RESEARCH_MANIFEST_CODEC_VERSION,
                "schema_version": manifest.schema_version,
                "research_request_id": manifest.research_request_id,
                "graph_manifest_id": manifest.graph_manifest_id,
                "graph_spec_id": manifest.graph_spec_id,
                "adjustment_bridge_id": manifest.adjustment_bridge_id,
                "effective_tick_panel_id": manifest.effective_tick_panel_id,
                "expected_reference_promotion_ids": list(
                    manifest.expected_reference_promotion_ids
                ),
                "expected_corporate_action_snapshot_id": (
                    manifest.expected_corporate_action_snapshot_id
                ),
                "engine_request_id": manifest.engine_request_id,
                "engine_run_id": manifest.engine_run_id,
                "feature_input_panel_id": manifest.feature_input_panel_id,
                "technical_config_id": manifest.technical_config_id,
                "technical_panel_id": manifest.technical_panel_id,
                "cross_section_config_id": manifest.cross_section_config_id,
                "cross_section_panel_id": manifest.cross_section_panel_id,
                "intent_config_id": manifest.intent_config_id,
                "research_intent_batch_id": manifest.research_intent_batch_id,
                "replay_run_id": manifest.replay_run_id,
                "signal_session": manifest.signal_session.isoformat(),
                "entry_session": manifest.entry_session.isoformat(),
                "cutoff": manifest.cutoff.isoformat(),
                "initial_capital": str(manifest.initial_capital),
                "candidate_count": manifest.candidate_count,
                "intent_count": manifest.intent_count,
                "adjustment_readiness": manifest.adjustment_readiness.value,
                "adjustment_actionable": manifest.adjustment_actionable,
                "effective_tick_readiness": manifest.effective_tick_readiness.value,
                "effective_tick_actionable": manifest.effective_tick_actionable,
                "paper_only": manifest.paper_only,
                "notification_eligible": manifest.notification_eligible,
                "execution_eligible": manifest.execution_eligible,
                "research_run_id": manifest.research_run_id,
            },
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def decode_promoted_research_run_manifest(
    payload: bytes,
) -> PromotedResearchRunManifest:
    if (
        type(payload) is not bytes
        or not payload
        or len(payload) > _MAXIMUM_MANIFEST_BYTES
    ):
        raise PromotedResearchError(_ERR_BYTES)
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise PromotedResearchError(_ERR_BYTES) from None
    try:
        decoded = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
    except PromotedResearchError:
        raise
    except (json.JSONDecodeError, RecursionError):
        raise PromotedResearchError(_ERR_SHAPE) from None
    if type(decoded) is not dict or set(decoded) != _MANIFEST_KEYS:
        raise PromotedResearchError(_ERR_SHAPE)
    if (
        decoded["codec_schema_version"] != PROMOTED_RESEARCH_MANIFEST_CODEC_VERSION
        or decoded["schema_version"] != PROMOTED_RESEARCH_MANIFEST_SCHEMA_VERSION
    ):
        raise PromotedResearchError(_ERR_SHAPE)
    id_fields = (
        "research_request_id",
        "graph_manifest_id",
        "graph_spec_id",
        "adjustment_bridge_id",
        "effective_tick_panel_id",
        "expected_corporate_action_snapshot_id",
        "engine_request_id",
        "engine_run_id",
        "feature_input_panel_id",
        "technical_config_id",
        "technical_panel_id",
        "cross_section_config_id",
        "cross_section_panel_id",
        "intent_config_id",
        "research_intent_batch_id",
        "replay_run_id",
        "research_run_id",
    )
    ids = {name: _require_sha(decoded[name], _ERR_SHAPE) for name in id_fields}
    raw_promotion_ids = decoded["expected_reference_promotion_ids"]
    if type(raw_promotion_ids) is not list or not raw_promotion_ids:
        raise PromotedResearchError(_ERR_SHAPE)
    expected_reference_promotion_ids = tuple(
        _require_sha(value, _ERR_SHAPE) for value in raw_promotion_ids
    )
    if expected_reference_promotion_ids != tuple(
        sorted(set(expected_reference_promotion_ids))
    ):
        raise PromotedResearchError(_ERR_SHAPE)
    signal_session = _canonical_date(decoded["signal_session"], _ERR_SHAPE)
    entry_session = _canonical_date(decoded["entry_session"], _ERR_SHAPE)
    cutoff = _canonical_datetime(decoded["cutoff"], _ERR_SHAPE)
    initial_capital = _positive_decimal(decoded["initial_capital"], _ERR_SHAPE)
    candidate_count = decoded["candidate_count"]
    intent_count = decoded["intent_count"]
    try:
        adjustment_readiness = ReferenceReadiness(decoded["adjustment_readiness"])
        effective_tick_readiness = ReferenceReadiness(
            decoded["effective_tick_readiness"]
        )
    except ValueError:
        raise PromotedResearchError(_ERR_SHAPE) from None
    adjustment_actionable = decoded["adjustment_actionable"]
    effective_tick_actionable = decoded["effective_tick_actionable"]
    paper_only = decoded["paper_only"]
    notification_eligible = decoded["notification_eligible"]
    execution_eligible = decoded["execution_eligible"]
    if (
        type(candidate_count) is not int
        or type(intent_count) is not int
        or type(adjustment_actionable) is not bool
        or type(effective_tick_actionable) is not bool
        or type(paper_only) is not bool
        or type(notification_eligible) is not bool
        or type(execution_eligible) is not bool
    ):
        raise PromotedResearchError(_ERR_SHAPE)
    try:
        manifest = PromotedResearchRunManifest(
            schema_version=decoded["schema_version"],
            research_request_id=ids["research_request_id"],
            graph_manifest_id=ids["graph_manifest_id"],
            graph_spec_id=ids["graph_spec_id"],
            adjustment_bridge_id=ids["adjustment_bridge_id"],
            effective_tick_panel_id=ids["effective_tick_panel_id"],
            expected_reference_promotion_ids=expected_reference_promotion_ids,
            expected_corporate_action_snapshot_id=(
                ids["expected_corporate_action_snapshot_id"]
            ),
            engine_request_id=ids["engine_request_id"],
            engine_run_id=ids["engine_run_id"],
            feature_input_panel_id=ids["feature_input_panel_id"],
            technical_config_id=ids["technical_config_id"],
            technical_panel_id=ids["technical_panel_id"],
            cross_section_config_id=ids["cross_section_config_id"],
            cross_section_panel_id=ids["cross_section_panel_id"],
            intent_config_id=ids["intent_config_id"],
            research_intent_batch_id=ids["research_intent_batch_id"],
            replay_run_id=ids["replay_run_id"],
            signal_session=signal_session,
            entry_session=entry_session,
            cutoff=cutoff,
            initial_capital=initial_capital,
            candidate_count=candidate_count,
            intent_count=intent_count,
            adjustment_readiness=adjustment_readiness,
            adjustment_actionable=adjustment_actionable,
            effective_tick_readiness=effective_tick_readiness,
            effective_tick_actionable=effective_tick_actionable,
            paper_only=paper_only,
            notification_eligible=notification_eligible,
            execution_eligible=execution_eligible,
        )
    except PromotedResearchError:
        raise PromotedResearchError(_ERR_SHAPE) from None
    if manifest.research_run_id != ids["research_run_id"]:
        raise PromotedResearchError(_ERR_SHAPE)
    if encode_promoted_research_run_manifest(manifest) != payload:
        raise PromotedResearchError(_ERR_SHAPE)
    return manifest


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


class LocalPromotedResearchRunStore:
    """Durable, create-once root store for PromotedResearchRunManifest.

    Exposes only ``put``, ``get``, and ``path_for`` -- no list/latest/
    nearest/find/discovery operation. ``get`` never trusts the stored
    manifest as authority: it strictly decodes it, resolves the graph
    publication and the engine run *exactly once each* through their own
    already-hardened durable stores (each of which independently replays its
    own complete graph on every call), and requires every retained
    relationship between them and this combined manifest to agree before
    returning anything. It never re-walks the deeper identity/session graph
    itself -- that full replay already happened inside the two terminal
    ``get`` calls.
    """

    _DIRECTORY = "promoted-research-runs"
    _LOCK_FILENAME = ".promoted-research-run.lock"

    def __init__(
        self,
        root: Path,
        *,
        graph_publications,
        engine_runs,
        replay_scope: ExactReplayScope,
    ) -> None:
        self.root = Path(root) / self._DIRECTORY
        self.graph_publications = graph_publications
        self.engine_runs = engine_runs
        self._replay_scope = replay_scope

    def path_for(self, research_run_id: str) -> Path:
        return self.root / f"{_require_sha(research_run_id, _ERR_ID)}.json"

    def put(
        self, manifest: PromotedResearchRunManifest
    ) -> PromotedResearchRunManifest:
        if type(manifest) is not PromotedResearchRunManifest:
            raise TypeError("promoted research run manifest must be exact")
        manifest.verify_content_identity()
        self._verify_downstream(manifest)
        payload = encode_promoted_research_run_manifest(manifest)
        try:
            replayed = decode_promoted_research_run_manifest(payload)
        except PromotedResearchError:
            raise
        except Exception:
            raise PromotedResearchError(_ERR_VERIFY) from None
        if (
            replayed != manifest
            or replayed.research_run_id != manifest.research_run_id
        ):
            raise PromotedResearchError(_ERR_VERIFY)
        self._publish(manifest.research_run_id, payload)
        return self.get(manifest.research_run_id)

    def get(self, research_run_id: str) -> PromotedResearchRunManifest:
        with self._replay_scope.open():
            return self._get(research_run_id)

    def _get(self, research_run_id: str) -> PromotedResearchRunManifest:
        path = self.path_for(research_run_id)
        payload = self._read(path)
        manifest = decode_promoted_research_run_manifest(payload)
        if manifest.research_run_id != research_run_id:
            raise PromotedResearchError(_ERR_SHAPE)
        self._verify_downstream(manifest)
        return manifest

    def _verify_downstream(self, manifest: PromotedResearchRunManifest) -> None:
        try:
            graph_manifest = self.graph_publications.get(manifest.graph_manifest_id)
        except Exception:
            raise PromotedResearchConflict(_ERR_VERIFY) from None
        try:
            engine_manifest = self.engine_runs.get(manifest.engine_run_id)
        except Exception:
            raise PromotedResearchConflict(_ERR_VERIFY) from None

        expected_reference_promotion_ids = tuple(
            sorted(
                value.promotion_id for value in graph_manifest.promotion_bindings
            )
        )
        if (
            type(graph_manifest) is not PromotedGraphPublicationManifest
            or graph_manifest.manifest_id != manifest.graph_manifest_id
            or graph_manifest.spec_id != manifest.graph_spec_id
            or graph_manifest.adjustment_bridge_id != manifest.adjustment_bridge_id
            or graph_manifest.effective_tick_panel_id
            != manifest.effective_tick_panel_id
            or graph_manifest.corporate_action_snapshot_id
            != manifest.expected_corporate_action_snapshot_id
            or expected_reference_promotion_ids
            != manifest.expected_reference_promotion_ids
            or graph_manifest.paper_only is not True
            or graph_manifest.execution_eligible is not False
            or graph_manifest.adjustment_readiness != manifest.adjustment_readiness
            or graph_manifest.adjustment_actionable != manifest.adjustment_actionable
            or graph_manifest.effective_tick_readiness
            != manifest.effective_tick_readiness
            or graph_manifest.effective_tick_actionable
            != manifest.effective_tick_actionable
        ):
            raise PromotedResearchConflict(_ERR_VERIFY)

        if (
            type(engine_manifest) is not PromotedEngineRunManifest
            or engine_manifest.run_id != manifest.engine_run_id
            or engine_manifest.request_id != manifest.engine_request_id
            or engine_manifest.adjustment_bridge_id != manifest.adjustment_bridge_id
            or engine_manifest.effective_tick_panel_id
            != manifest.effective_tick_panel_id
            or engine_manifest.expected_reference_promotion_ids
            != manifest.expected_reference_promotion_ids
            or engine_manifest.expected_corporate_action_snapshot_id
            != manifest.expected_corporate_action_snapshot_id
            or engine_manifest.feature_input_panel_id
            != manifest.feature_input_panel_id
            or engine_manifest.technical_config_id != manifest.technical_config_id
            or engine_manifest.technical_panel_id != manifest.technical_panel_id
            or engine_manifest.cross_section_config_id
            != manifest.cross_section_config_id
            or engine_manifest.cross_section_panel_id
            != manifest.cross_section_panel_id
            or engine_manifest.intent_config_id != manifest.intent_config_id
            or engine_manifest.research_intent_batch_id
            != manifest.research_intent_batch_id
            or engine_manifest.replay_run_id != manifest.replay_run_id
            or engine_manifest.signal_session != manifest.signal_session
            or engine_manifest.entry_session != manifest.entry_session
            or engine_manifest.cutoff != manifest.cutoff
            or engine_manifest.initial_capital != manifest.initial_capital
            or engine_manifest.candidate_count != manifest.candidate_count
            or engine_manifest.intent_count != manifest.intent_count
            or engine_manifest.paper_only is not True
        ):
            raise PromotedResearchConflict(_ERR_VERIFY)

    def _read(self, path: Path) -> bytes:
        if not path.exists():
            raise PromotedResearchNotFound(_ERR_NOT_FOUND)
        if not path.is_file() or _is_link_like(path):
            raise PromotedResearchError(_ERR_UNSAFE_PATH)
        try:
            return read_stable_regular_file(
                path, maximum_bytes=_MAXIMUM_MANIFEST_BYTES
            )
        except FileSafetyError:
            raise PromotedResearchError(_ERR_UNSAFE_PATH) from None

    def _publish(self, research_run_id: str, payload: bytes) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        if not self.root.is_dir() or _is_link_like(self.root):
            raise PromotedResearchError(_ERR_UNSAFE_PATH)
        target = self.path_for(research_run_id)
        try:
            with advisory_file_lock(self.root / self._LOCK_FILENAME):
                if target.exists():
                    if _is_link_like(target) or self._read(target) != payload:
                        raise PromotedResearchConflict(_ERR_CONFLICT)
                    return
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".promoted-research-run-",
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
        except PromotedResearchConflict:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise PromotedResearchConflict(_ERR_CONFLICT) from None
