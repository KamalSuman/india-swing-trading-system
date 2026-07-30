"""One restart-safe, exact-ID promoted-graph publisher.

Composes the existing promoted services -- identity intake, identity
adjudication, per-session identity universe, per-session market-data frame,
per-session tick snapshot, stable-listing history, corporate-action
adjustment, and effective-session ticks -- from exact stored-evidence roots,
persisting and independently replaying every intermediate artifact before a
terminal publication manifest can ever be written. It creates no new
financial algorithm: every materialization is delegated unchanged to its
existing service.

This module is the bounded bridge from verified stored evidence to the two
exact roots (``adjustment_bridge_id``, ``effective_tick_panel_id``) the
existing ``PromotedEngineRunner`` already consumes. It stops there
deliberately: it never runs the promoted engine, never prepares a proposal,
never sends an alert, and never touches a broker, Telegram, GCP, the
network, or a live store. A successful publication is only an auditable
graph-construction record -- ``paper_only`` is always true and
``execution_eligible`` is always false, regardless of the downstream
readiness/actionability state, which is preserved and reported exactly as
the composed services produced it.
"""

from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone
from pathlib import Path

from india_swing._exact_replay import ExactReplayScope, ScopedExactResolver
from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.calendar_data import CollectionCalendarMaterialization
from india_swing.calendar_data.materialization_store import (
    LocalCalendarMaterializationStore,
)
from india_swing.corporate_actions.models import CorporateActionSnapshot
from india_swing.corporate_actions.promoted_adjustments import (
    PromotedCorporateActionAdjustmentService,
    VerifiedPromotedCorporateActionAdjustmentPanel,
)
from india_swing.corporate_actions.snapshot_store import (
    LocalCorporateActionSnapshotStore,
)
from india_swing.historical_prices.promoted_history import (
    PromotedStableListingHistoryService,
    VerifiedPromotedStableListingHistoryPanel,
)
from india_swing.identity import content_id
from india_swing.identity_decisions.artifact_store import (
    LocalIdentityReviewBundleStore,
)
from india_swing.identity_decisions.promoted_materialize import (
    PromotedIdentityAdjudicationService,
    VerifiedPromotedIdentityAdjudication,
)
from india_swing.identity_evidence.artifact_store import (
    LocalIdentityEvidenceArtifactStore,
)
from india_swing.identity_registry.promoted_intake import (
    PromotedIdentityIntakeService,
    VerifiedPromotedIdentityIntake,
)
from india_swing.market_data.historical_corpus import (
    LocalHistoricalEvaluationCorpusStore,
)
from india_swing.market_data.promoted_session_frame import (
    PromotedSessionMarketDataFrameService,
    VerifiedPromotedSessionMarketDataFrame,
)
from india_swing.promoted_graph_store import (
    LocalPromotedCorporateActionAdjustmentStore,
    LocalPromotedEffectiveSessionTickStore,
    LocalPromotedIdentityAdjudicationStore,
    LocalPromotedIdentityIntakeStore,
    LocalPromotedIdentitySessionUniverseStore,
    LocalPromotedSessionMarketDataFrameStore,
    LocalPromotedSessionTickSnapshotStore,
    LocalPromotedStableListingHistoryStore,
)
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.acquisition_promotion import (
    VerifiedReferenceArtifactPromotion,
)
from india_swing.reference_data.artifact_store import LocalReferenceArtifactStore
from india_swing.reference_data.promotion_store import (
    LocalReferenceArtifactPromotionStore,
)
from india_swing.tick_sizes.effective_session import (
    PromotedEffectiveSessionTickService,
    VerifiedPromotedEffectiveSessionTickPanel,
)
from india_swing.tick_sizes.promoted_session import (
    PromotedSessionTickSizeService,
    VerifiedPromotedSessionTickSnapshot,
)
from india_swing.universe.promoted_identity import (
    PromotedIdentitySessionUniverseService,
    VerifiedPromotedIdentitySessionUniverse,
)


class PromotedGraphPublisherError(ValueError):
    pass


class PromotedGraphPublisherConflict(PromotedGraphPublisherError):
    pass


class PromotedGraphPublisherNotFound(PromotedGraphPublisherError):
    pass


# Private aliases preserved so this module's own composition/store code and
# every existing test that imports these two names from here (including the
# adversarial ReplayScopeTests suite) keep working unchanged. The actual
# implementation now lives in india_swing._exact_replay so promoted_engine.py
# and promoted_research_run.py can share the exact same mechanism instead of
# each re-deriving their own upstream identity/session/history graph.
_ReplayScope = ExactReplayScope
_ScopedExactResolver = ScopedExactResolver


PROMOTED_GRAPH_SPEC_SCHEMA_VERSION = "promoted-graph-publication-spec/v1"
PROMOTED_GRAPH_MANIFEST_SCHEMA_VERSION = "promoted-graph-publication-manifest/v1"
PROMOTED_GRAPH_MANIFEST_CODEC_VERSION = "promoted-graph-publication-manifest-json/v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAXIMUM_MANIFEST_BYTES = 2 * 1024 * 1024

_ERR_TYPE = "promoted graph publisher type is invalid"
_ERR_ID = "promoted graph publisher identifier is invalid"
_ERR_PROMOTION_BINDING = "promoted graph promotion binding is invalid"
_ERR_SESSION_BINDING = "promoted graph session binding is invalid"
_ERR_SPEC = "promoted graph publication spec is invalid"
_ERR_SPEC_ID = "promoted graph publication spec_id disagrees with independent content"
_ERR_CUTOFF = "promoted graph publication cutoff is invalid"
_ERR_SOURCE = "promoted graph publisher source could not be resolved"
_ERR_PROMOTION_MISMATCH = (
    "promoted graph publisher resolved promotion does not match its binding"
)
_ERR_INTAKE = "promoted graph publisher could not materialize identity intake"
_ERR_EVIDENCE_MISMATCH = (
    "promoted graph publisher resolved identity evidence does not match its"
    " requested exact IDs"
)
_ERR_REVIEW_MISMATCH = (
    "promoted graph publisher resolved identity review does not match its"
    " requested exact IDs"
)
_ERR_ADJUDICATION = "promoted graph publisher could not materialize adjudication"
_ERR_CALENDAR_MISMATCH = (
    "promoted graph publisher resolved calendar does not match its exact ID"
)
_ERR_UNIVERSE = "promoted graph publisher could not materialize session universe"
_ERR_PARTITION_SELECTION = (
    "promoted graph publisher could not select exactly one corpus partition"
    " for the requested session"
)
_ERR_FRAME = "promoted graph publisher could not materialize session frame"
_ERR_TICK_SNAPSHOT = "promoted graph publisher could not materialize tick snapshot"
_ERR_HISTORY = "promoted graph publisher could not materialize stable listing history"
_ERR_ACTIONS_MISMATCH = (
    "promoted graph publisher resolved corporate-action snapshot does not"
    " match its exact ID"
)
_ERR_ADJUSTMENT = (
    "promoted graph publisher could not materialize the corporate-action"
    " adjustment"
)
_ERR_EFFECTIVE_TICKS = (
    "promoted graph publisher could not materialize effective session ticks"
)
_ERR_GRAPH = "promoted graph publication manifest graph is invalid"
_ERR_SPEC_MISMATCH = (
    "promoted graph publication manifest spec preimage does not recompute to"
    " its stored spec_id"
)
_ERR_VERIFY = "promoted graph publication manifest could not be verified"
_ERR_CONFLICT = "promoted graph publication already stores different content"
_ERR_NOT_FOUND = "promoted graph publication was not found"
_ERR_UNSAFE_PATH = "promoted graph publication path is unsafe"
_ERR_BYTES = "promoted graph publication manifest bytes are invalid"
_ERR_SHAPE = "promoted graph publication manifest shape is invalid"


def _sha(value: object) -> bool:
    return type(value) is str and _SHA256.fullmatch(value) is not None


def _require_sha(value: object, message: str) -> str:
    if not _sha(value):
        raise PromotedGraphPublisherError(message)
    return value  # type: ignore[return-value]


def _utc(value: object, message: str) -> datetime:
    if type(value) is not datetime:
        raise PromotedGraphPublisherError(message)
    try:
        offset = value.utcoffset()
    except Exception:
        raise PromotedGraphPublisherError(message) from None
    if value.tzinfo is None or offset is None:
        raise PromotedGraphPublisherError(message)
    return value.astimezone(timezone.utc)


def _canonical_date(value: object, message: str) -> date:
    if type(value) is not str:
        raise PromotedGraphPublisherError(message)
    try:
        result = date.fromisoformat(value)
    except ValueError:
        raise PromotedGraphPublisherError(message) from None
    if result.isoformat() != value:
        raise PromotedGraphPublisherError(message)
    return result


def _canonical_datetime(value: object, message: str) -> datetime:
    if type(value) is not str:
        raise PromotedGraphPublisherError(message)
    try:
        result = datetime.fromisoformat(value)
    except ValueError:
        raise PromotedGraphPublisherError(message) from None
    offset = result.utcoffset()
    if (
        result.tzinfo is None
        or offset is None
        or offset.total_seconds() != 0
        or result.isoformat() != value
    ):
        raise PromotedGraphPublisherError(message)
    return result


@dataclass(frozen=True, slots=True)
class PromotedGraphPromotionBinding:
    """Pins one exact reference-artifact promotion to its expected report date."""

    promotion_id: str
    expected_report_date: date

    def __post_init__(self) -> None:
        if not _sha(self.promotion_id) or type(self.expected_report_date) is not date:
            raise PromotedGraphPublisherError(_ERR_PROMOTION_BINDING)


@dataclass(frozen=True, slots=True)
class PromotedGraphSessionBinding:
    """Pins one exact market session to the historical corpus that must supply it."""

    market_session: date
    historical_corpus_id: str

    def __post_init__(self) -> None:
        if type(self.market_session) is not date or not _sha(
            self.historical_corpus_id
        ):
            raise PromotedGraphPublisherError(_ERR_SESSION_BINDING)


def _compute_spec_id(
    *,
    promotion_bindings: tuple[PromotedGraphPromotionBinding, ...],
    identity_evidence_artifact_ids: tuple[str, ...],
    identity_review_bundle_ids: tuple[str, ...],
    calendar_materialization_id: str,
    session_bindings: tuple[PromotedGraphSessionBinding, ...],
    corporate_action_snapshot_id: str,
    cutoff: datetime,
) -> str:
    """The single spec-identity derivation shared by the spec and the manifest.

    Both ``PromotedGraphPublicationSpec`` (at construction) and
    ``PromotedGraphPublicationManifest`` (on every decode/verify) call this
    exact function over the same named fields, so a durable manifest can
    prove its retained root-pin preimage recomputes to its own stored
    ``spec_id`` rather than persisting an opaque, unverifiable hash.
    """

    return content_id(
        {
            "schema_version": PROMOTED_GRAPH_SPEC_SCHEMA_VERSION,
            "promotion_bindings": promotion_bindings,
            "identity_evidence_artifact_ids": identity_evidence_artifact_ids,
            "identity_review_bundle_ids": identity_review_bundle_ids,
            "calendar_materialization_id": calendar_materialization_id,
            "session_bindings": session_bindings,
            "corporate_action_snapshot_id": corporate_action_snapshot_id,
            "cutoff": cutoff,
        },
        length=64,
    )


def _require_bindings_shape(
    ids: tuple[str, ...], message: str
) -> None:
    if (
        type(ids) is not tuple
        or any(not _sha(value) for value in ids)
        or len(set(ids)) != len(ids)
        or ids != tuple(sorted(ids))
    ):
        raise PromotedGraphPublisherError(message)


@dataclass(frozen=True, slots=True)
class PromotedGraphPublicationSpec:
    """Immutable, content-addressed request to publish one exact promoted graph.

    Every exact ID here is a caller-supplied pin, never discovered: the
    publisher resolves only these exact roots and never lists, scans,
    selects latest, chooses nearest, falls back, fetches network data, or
    synthesizes missing evidence.
    """

    promotion_bindings: tuple[PromotedGraphPromotionBinding, ...]
    identity_evidence_artifact_ids: tuple[str, ...]
    identity_review_bundle_ids: tuple[str, ...]
    calendar_materialization_id: str
    session_bindings: tuple[PromotedGraphSessionBinding, ...]
    corporate_action_snapshot_id: str
    cutoff: datetime
    spec_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            type(self.promotion_bindings) is not tuple
            or not self.promotion_bindings
            or any(
                type(value) is not PromotedGraphPromotionBinding
                for value in self.promotion_bindings
            )
        ):
            raise PromotedGraphPublisherError(_ERR_SPEC)
        dates = tuple(value.expected_report_date for value in self.promotion_bindings)
        promotion_ids = tuple(
            value.promotion_id for value in self.promotion_bindings
        )
        if (
            dates != tuple(sorted(dates))
            or len(set(dates)) != len(dates)
            or len(set(promotion_ids)) != len(promotion_ids)
        ):
            raise PromotedGraphPublisherError(_ERR_SPEC)

        _require_bindings_shape(self.identity_evidence_artifact_ids, _ERR_SPEC)
        _require_bindings_shape(self.identity_review_bundle_ids, _ERR_SPEC)
        if not _sha(self.calendar_materialization_id):
            raise PromotedGraphPublisherError(_ERR_SPEC)

        if (
            type(self.session_bindings) is not tuple
            or not self.session_bindings
            or any(
                type(value) is not PromotedGraphSessionBinding
                for value in self.session_bindings
            )
        ):
            raise PromotedGraphPublisherError(_ERR_SPEC)
        sessions = tuple(value.market_session for value in self.session_bindings)
        if sessions != tuple(sorted(sessions)) or len(set(sessions)) != len(sessions):
            raise PromotedGraphPublisherError(_ERR_SPEC)

        if not _sha(self.corporate_action_snapshot_id):
            raise PromotedGraphPublisherError(_ERR_SPEC)

        cutoff = _utc(self.cutoff, _ERR_CUTOFF)
        if cutoff != self.cutoff:
            object.__setattr__(self, "cutoff", cutoff)
        object.__setattr__(self, "spec_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return _compute_spec_id(
            promotion_bindings=self.promotion_bindings,
            identity_evidence_artifact_ids=self.identity_evidence_artifact_ids,
            identity_review_bundle_ids=self.identity_review_bundle_ids,
            calendar_materialization_id=self.calendar_materialization_id,
            session_bindings=self.session_bindings,
            corporate_action_snapshot_id=self.corporate_action_snapshot_id,
            cutoff=self.cutoff,
        )

    def verify_content_identity(self) -> None:
        if type(self) is not PromotedGraphPublicationSpec:
            raise PromotedGraphPublisherError(_ERR_TYPE)
        if self.spec_id != self._calculated_id():
            raise PromotedGraphPublisherError(_ERR_SPEC_ID)


@dataclass(frozen=True, slots=True)
class PromotedGraphSessionArtifacts:
    """The exact intermediate IDs produced for one market session."""

    market_session: date
    universe_id: str
    frame_id: str
    tick_snapshot_id: str

    def __post_init__(self) -> None:
        if (
            type(self.market_session) is not date
            or not _sha(self.universe_id)
            or not _sha(self.frame_id)
            or not _sha(self.tick_snapshot_id)
        ):
            raise PromotedGraphPublisherError(_ERR_GRAPH)


class _CalendarResolverAdapter:
    """Adapts LocalCalendarMaterializationStore.get to a bare-materialization resolver."""

    def __init__(self, store: LocalCalendarMaterializationStore) -> None:
        self._store = store

    def get(self, materialization_id: str) -> CollectionCalendarMaterialization:
        return self._store.get(materialization_id).materialization


@dataclass(frozen=True, slots=True)
class PromotedGraphPublicationManifest:
    """Canonical, content-derived, create-once record of one completed publication.

    Retains the complete spec preimage (every root pin) alongside every
    intermediate/terminal artifact ID, so a durable manifest can prove its
    own ``spec_id`` recomputes from its own retained fields rather than
    persisting an opaque, unverifiable hash. Publication is only an
    auditable graph-construction record: ``paper_only`` is always true and
    ``execution_eligible`` is always false.
    """

    schema_version: str
    spec_id: str
    promotion_bindings: tuple[PromotedGraphPromotionBinding, ...]
    identity_evidence_artifact_ids: tuple[str, ...]
    identity_review_bundle_ids: tuple[str, ...]
    calendar_materialization_id: str
    session_bindings: tuple[PromotedGraphSessionBinding, ...]
    corporate_action_snapshot_id: str
    cutoff: datetime
    intake_id: str
    adjudication_id: str
    session_artifacts: tuple[PromotedGraphSessionArtifacts, ...]
    stable_history_panel_id: str
    adjustment_bridge_id: str
    effective_tick_panel_id: str
    adjustment_readiness: ReferenceReadiness
    adjustment_actionable: bool
    effective_tick_readiness: ReferenceReadiness
    effective_tick_actionable: bool
    paper_only: bool
    execution_eligible: bool
    manifest_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            self.schema_version != PROMOTED_GRAPH_MANIFEST_SCHEMA_VERSION
            or type(self.promotion_bindings) is not tuple
            or not self.promotion_bindings
            or any(
                type(value) is not PromotedGraphPromotionBinding
                for value in self.promotion_bindings
            )
            or type(self.session_bindings) is not tuple
            or not self.session_bindings
            or any(
                type(value) is not PromotedGraphSessionBinding
                for value in self.session_bindings
            )
            or type(self.session_artifacts) is not tuple
            or len(self.session_artifacts) != len(self.session_bindings)
            or any(
                type(value) is not PromotedGraphSessionArtifacts
                for value in self.session_artifacts
            )
            or not _sha(self.calendar_materialization_id)
            or not _sha(self.corporate_action_snapshot_id)
            or not _sha(self.intake_id)
            or not _sha(self.adjudication_id)
            or not _sha(self.stable_history_panel_id)
            or not _sha(self.adjustment_bridge_id)
            or not _sha(self.effective_tick_panel_id)
            or type(self.adjustment_readiness) is not ReferenceReadiness
            or type(self.adjustment_actionable) is not bool
            or type(self.effective_tick_readiness) is not ReferenceReadiness
            or type(self.effective_tick_actionable) is not bool
            or self.paper_only is not True
            or self.execution_eligible is not False
        ):
            raise PromotedGraphPublisherError(_ERR_GRAPH)
        _require_bindings_shape(self.identity_evidence_artifact_ids, _ERR_GRAPH)
        _require_bindings_shape(self.identity_review_bundle_ids, _ERR_GRAPH)
        if tuple(
            value.market_session for value in self.session_artifacts
        ) != tuple(value.market_session for value in self.session_bindings):
            raise PromotedGraphPublisherError(_ERR_GRAPH)

        cutoff = _utc(self.cutoff, _ERR_CUTOFF)
        if cutoff != self.cutoff:
            object.__setattr__(self, "cutoff", cutoff)

        expected_spec_id = _compute_spec_id(
            promotion_bindings=self.promotion_bindings,
            identity_evidence_artifact_ids=self.identity_evidence_artifact_ids,
            identity_review_bundle_ids=self.identity_review_bundle_ids,
            calendar_materialization_id=self.calendar_materialization_id,
            session_bindings=self.session_bindings,
            corporate_action_snapshot_id=self.corporate_action_snapshot_id,
            cutoff=self.cutoff,
        )
        if not _sha(self.spec_id) or self.spec_id != expected_spec_id:
            raise PromotedGraphPublisherError(_ERR_SPEC_MISMATCH)

        object.__setattr__(self, "manifest_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            value.name: getattr(self, value.name)
            for value in fields(self)
            if value.name != "manifest_id"
        }

    def _calculated_id(self) -> str:
        return content_id(
            {"schema": PROMOTED_GRAPH_MANIFEST_SCHEMA_VERSION, **self._identity()},
            length=64,
        )

    def verify_content_identity(self) -> None:
        if type(self) is not PromotedGraphPublicationManifest:
            raise PromotedGraphPublisherError(_ERR_TYPE)
        expected = PromotedGraphPublicationManifest(**self._identity())
        if self.manifest_id != expected.manifest_id:
            raise PromotedGraphPublisherError(_ERR_ID)


@dataclass(slots=True)
class PromotedGraphStores:
    """Local exact-store composition boundary for the promoted-graph publisher.

    Every field is a real durable store or resolver; the publisher never
    instantiates a hidden default. There is no list/latest/nearest/find
    capability anywhere in this composition.
    """

    promotions: object
    identity_evidence: object
    identity_reviews: object
    calendars: object
    historical_corpus: object
    corporate_action_snapshots: LocalCorporateActionSnapshotStore
    identity_intakes: LocalPromotedIdentityIntakeStore
    identity_adjudications: LocalPromotedIdentityAdjudicationStore
    identity_session_universes: LocalPromotedIdentitySessionUniverseStore
    session_market_data_frames: LocalPromotedSessionMarketDataFrameStore
    session_tick_snapshots: LocalPromotedSessionTickSnapshotStore
    stable_listing_histories: LocalPromotedStableListingHistoryStore
    corporate_action_adjustments: LocalPromotedCorporateActionAdjustmentStore
    effective_session_ticks: LocalPromotedEffectiveSessionTickStore
    publications: "LocalPromotedGraphPublicationStore"
    _replay_scope: _ReplayScope = field(repr=False)


def build_promoted_graph_stores(
    *,
    reference_root: Path,
    identity_evidence_root: Path,
    calendar_root: Path,
    daily_reports_root: Path,
    historical_corpus_root: Path,
    promoted_root: Path,
    publication_root: Path,
) -> PromotedGraphStores:
    """Construct every real durable store from explicit roots.

    This is the only place the publisher's stores are assembled. It never
    accepts a caller-supplied fake in place of a real store, and it exposes
    no discovery: every store here resolves only the exact ID it is asked
    for.
    """

    replay_scope = _ReplayScope()
    reference_artifacts = LocalReferenceArtifactStore(reference_root)
    promotions = LocalReferenceArtifactPromotionStore(
        promoted_root, reference_artifacts
    )
    identity_evidence = LocalIdentityEvidenceArtifactStore(identity_evidence_root)
    identity_reviews = LocalIdentityReviewBundleStore(identity_evidence_root)
    promotion_resolver = _ScopedExactResolver(
        "reference-promotion", promotions, replay_scope
    )
    evidence_resolver = _ScopedExactResolver(
        "identity-evidence", identity_evidence, replay_scope
    )
    review_resolver = _ScopedExactResolver(
        "identity-review", identity_reviews, replay_scope
    )
    identity_intakes = LocalPromotedIdentityIntakeStore(
        promoted_root, promotion_resolver
    )
    intake_resolver = _ScopedExactResolver(
        "identity-intake", identity_intakes, replay_scope
    )
    identity_adjudications = LocalPromotedIdentityAdjudicationStore(
        promoted_root, intake_resolver, evidence_resolver, review_resolver
    )
    adjudication_resolver = _ScopedExactResolver(
        "identity-adjudication", identity_adjudications, replay_scope
    )
    calendars_store = LocalCalendarMaterializationStore(calendar_root, daily_reports_root)
    calendars = _CalendarResolverAdapter(calendars_store)
    calendar_resolver = _ScopedExactResolver(
        "calendar-materialization", calendars, replay_scope
    )
    identity_session_universes = LocalPromotedIdentitySessionUniverseStore(
        promoted_root, adjudication_resolver, calendar_resolver
    )
    universe_resolver = _ScopedExactResolver(
        "identity-session-universe", identity_session_universes, replay_scope
    )
    historical_corpus = LocalHistoricalEvaluationCorpusStore(historical_corpus_root)
    corpus_resolver = _ScopedExactResolver(
        "historical-corpus", historical_corpus, replay_scope
    )
    session_market_data_frames = LocalPromotedSessionMarketDataFrameStore(
        promoted_root, universe_resolver, corpus_resolver
    )
    frame_resolver = _ScopedExactResolver(
        "session-market-data-frame", session_market_data_frames, replay_scope
    )
    session_tick_snapshots = LocalPromotedSessionTickSnapshotStore(
        promoted_root, frame_resolver
    )
    tick_snapshot_resolver = _ScopedExactResolver(
        "session-tick-snapshot", session_tick_snapshots, replay_scope
    )
    stable_listing_histories = LocalPromotedStableListingHistoryStore(
        promoted_root, tick_snapshot_resolver, calendar_resolver
    )
    history_resolver = _ScopedExactResolver(
        "stable-listing-history", stable_listing_histories, replay_scope
    )
    corporate_action_snapshots = LocalCorporateActionSnapshotStore(promoted_root)
    corporate_action_resolver = _ScopedExactResolver(
        "corporate-action-snapshot", corporate_action_snapshots, replay_scope
    )
    corporate_action_adjustments = LocalPromotedCorporateActionAdjustmentStore(
        promoted_root, history_resolver, corporate_action_resolver
    )
    effective_session_ticks = LocalPromotedEffectiveSessionTickStore(
        promoted_root, history_resolver
    )
    publications = LocalPromotedGraphPublicationStore(
        publication_root,
        identity_intakes=identity_intakes,
        identity_adjudications=identity_adjudications,
        calendars=calendars,
        identity_session_universes=identity_session_universes,
        session_market_data_frames=session_market_data_frames,
        session_tick_snapshots=session_tick_snapshots,
        stable_listing_histories=stable_listing_histories,
        corporate_action_adjustments=corporate_action_adjustments,
        effective_session_ticks=effective_session_ticks,
        replay_scope=replay_scope,
    )
    return PromotedGraphStores(
        promotions=promotions,
        identity_evidence=identity_evidence,
        identity_reviews=identity_reviews,
        calendars=calendars,
        historical_corpus=historical_corpus,
        corporate_action_snapshots=corporate_action_snapshots,
        identity_intakes=identity_intakes,
        identity_adjudications=identity_adjudications,
        identity_session_universes=identity_session_universes,
        session_market_data_frames=session_market_data_frames,
        session_tick_snapshots=session_tick_snapshots,
        stable_listing_histories=stable_listing_histories,
        corporate_action_adjustments=corporate_action_adjustments,
        effective_session_ticks=effective_session_ticks,
        publications=publications,
        _replay_scope=replay_scope,
    )


class PromotedGraphPublisher:
    """Composes the existing promoted services into one durable graph publication.

    Consumes only already-stored, exact-ID evidence; never discovers,
    scans, selects latest, chooses nearest, falls back, fetches network
    data, or synthesizes missing evidence. A failed intermediate step never
    reaches terminal publication: the manifest is constructed and persisted
    only after every intermediate artifact is durable and independently
    read back.
    """

    def publish(
        self,
        spec: PromotedGraphPublicationSpec,
        stores: PromotedGraphStores,
    ) -> PromotedGraphPublicationManifest:
        if type(spec) is not PromotedGraphPublicationSpec:
            raise TypeError("promoted graph publication spec must be exact")
        if type(stores) is not PromotedGraphStores:
            raise TypeError("promoted graph stores must be exact")
        with stores._replay_scope.open():
            published = self._publish(spec, stores)
        # A new outer scope forces one final cold replay of the persisted graph.
        # No value verified during construction can satisfy this read from cache.
        return stores.publications.get(published.manifest_id)

    def _publish(
        self,
        spec: PromotedGraphPublicationSpec,
        stores: PromotedGraphStores,
    ) -> PromotedGraphPublicationManifest:
        if type(spec) is not PromotedGraphPublicationSpec:
            raise TypeError("promoted graph publication spec must be exact")
        if type(stores) is not PromotedGraphStores:
            raise TypeError("promoted graph stores must be exact")
        spec.verify_content_identity()

        try:
            promotions = tuple(
                stores.promotions.get(binding.promotion_id)
                for binding in spec.promotion_bindings
            )
        except Exception:
            raise PromotedGraphPublisherError(_ERR_SOURCE) from None
        for promotion, binding in zip(promotions, spec.promotion_bindings):
            if (
                type(promotion) is not VerifiedReferenceArtifactPromotion
                or promotion.promotion_id != binding.promotion_id
                or promotion.verified_report_date != binding.expected_report_date
            ):
                raise PromotedGraphPublisherError(_ERR_PROMOTION_MISMATCH)

        expected_report_dates = tuple(
            binding.expected_report_date for binding in spec.promotion_bindings
        )
        try:
            intake = PromotedIdentityIntakeService().materialize(
                promotions=promotions,
                expected_report_dates=expected_report_dates,
                cutoff=spec.cutoff,
            )
            intake = stores.identity_intakes.put(intake)
        except PromotedGraphPublisherError:
            raise
        except Exception:
            raise PromotedGraphPublisherError(_ERR_INTAKE) from None

        try:
            evidence_artifacts = tuple(
                stores.identity_evidence.get(artifact_id)
                for artifact_id in spec.identity_evidence_artifact_ids
            )
            review_bundles = tuple(
                stores.identity_reviews.get(bundle_id)
                for bundle_id in spec.identity_review_bundle_ids
            )
        except Exception:
            raise PromotedGraphPublisherError(_ERR_SOURCE) from None
        if (
            tuple(value.manifest.artifact_id for value in evidence_artifacts)
            != spec.identity_evidence_artifact_ids
        ):
            raise PromotedGraphPublisherError(_ERR_EVIDENCE_MISMATCH)
        if (
            tuple(value.manifest.bundle_id for value in review_bundles)
            != spec.identity_review_bundle_ids
        ):
            raise PromotedGraphPublisherError(_ERR_REVIEW_MISMATCH)

        try:
            adjudication = PromotedIdentityAdjudicationService().materialize(
                intake=intake,
                evidence_artifacts=evidence_artifacts,
                review_bundles=review_bundles,
                cutoff=spec.cutoff,
            )
            adjudication = stores.identity_adjudications.put(adjudication)
        except PromotedGraphPublisherError:
            raise
        except Exception:
            raise PromotedGraphPublisherError(_ERR_ADJUDICATION) from None

        try:
            calendar = stores.calendars.get(spec.calendar_materialization_id)
        except Exception:
            raise PromotedGraphPublisherError(_ERR_SOURCE) from None
        if (
            type(calendar) is not CollectionCalendarMaterialization
            or calendar.materialization_id != spec.calendar_materialization_id
        ):
            raise PromotedGraphPublisherError(_ERR_CALENDAR_MISMATCH)

        session_artifact_list: list[PromotedGraphSessionArtifacts] = []
        tick_snapshot_list: list[VerifiedPromotedSessionTickSnapshot] = []
        for binding in spec.session_bindings:
            try:
                universe = PromotedIdentitySessionUniverseService().materialize(
                    adjudication=adjudication,
                    calendar=calendar,
                    market_session=binding.market_session,
                    cutoff=spec.cutoff,
                )
                universe = stores.identity_session_universes.put(universe)
            except PromotedGraphPublisherError:
                raise
            except Exception:
                raise PromotedGraphPublisherError(_ERR_UNIVERSE) from None

            try:
                corpus_index, partitions = stores.historical_corpus.get(
                    binding.historical_corpus_id
                )
            except Exception:
                raise PromotedGraphPublisherError(_ERR_SOURCE) from None
            matches = tuple(
                value
                for value in partitions
                if value.market_session == binding.market_session
            )
            if len(matches) != 1:
                raise PromotedGraphPublisherError(_ERR_PARTITION_SELECTION)
            partition = matches[0]

            try:
                frame = PromotedSessionMarketDataFrameService().materialize(
                    universe=universe,
                    corpus_index=corpus_index,
                    partition=partition,
                    cutoff=spec.cutoff,
                )
                frame = stores.session_market_data_frames.put(frame)
            except PromotedGraphPublisherError:
                raise
            except Exception:
                raise PromotedGraphPublisherError(_ERR_FRAME) from None

            try:
                snapshot = PromotedSessionTickSizeService().materialize(
                    frame=frame, cutoff=spec.cutoff
                )
                snapshot = stores.session_tick_snapshots.put(snapshot)
            except PromotedGraphPublisherError:
                raise
            except Exception:
                raise PromotedGraphPublisherError(_ERR_TICK_SNAPSHOT) from None

            tick_snapshot_list.append(snapshot)
            session_artifact_list.append(
                PromotedGraphSessionArtifacts(
                    market_session=binding.market_session,
                    universe_id=universe.universe_id,
                    frame_id=frame.frame_id,
                    tick_snapshot_id=snapshot.snapshot_id,
                )
            )
        tick_snapshots = tuple(tick_snapshot_list)
        session_artifacts = tuple(session_artifact_list)

        try:
            history = PromotedStableListingHistoryService().materialize(
                tick_snapshots=tick_snapshots, calendar=calendar, cutoff=spec.cutoff
            )
            history = stores.stable_listing_histories.put(history)
        except PromotedGraphPublisherError:
            raise
        except Exception:
            raise PromotedGraphPublisherError(_ERR_HISTORY) from None

        try:
            corporate_actions = stores.corporate_action_snapshots.get(
                spec.corporate_action_snapshot_id
            )
        except Exception:
            raise PromotedGraphPublisherError(_ERR_SOURCE) from None
        if (
            type(corporate_actions) is not CorporateActionSnapshot
            or corporate_actions.snapshot_id != spec.corporate_action_snapshot_id
        ):
            raise PromotedGraphPublisherError(_ERR_ACTIONS_MISMATCH)

        try:
            adjustment = PromotedCorporateActionAdjustmentService().materialize(
                source_panel=history,
                corporate_actions=corporate_actions,
                cutoff=spec.cutoff,
            )
            adjustment = stores.corporate_action_adjustments.put(adjustment)
        except PromotedGraphPublisherError:
            raise
        except Exception:
            raise PromotedGraphPublisherError(_ERR_ADJUSTMENT) from None

        try:
            effective_ticks = PromotedEffectiveSessionTickService().materialize(
                source_panel=history, cutoff=spec.cutoff
            )
            effective_ticks = stores.effective_session_ticks.put(effective_ticks)
        except PromotedGraphPublisherError:
            raise
        except Exception:
            raise PromotedGraphPublisherError(_ERR_EFFECTIVE_TICKS) from None

        manifest = PromotedGraphPublicationManifest(
            schema_version=PROMOTED_GRAPH_MANIFEST_SCHEMA_VERSION,
            spec_id=spec.spec_id,
            promotion_bindings=spec.promotion_bindings,
            identity_evidence_artifact_ids=spec.identity_evidence_artifact_ids,
            identity_review_bundle_ids=spec.identity_review_bundle_ids,
            calendar_materialization_id=spec.calendar_materialization_id,
            session_bindings=spec.session_bindings,
            corporate_action_snapshot_id=spec.corporate_action_snapshot_id,
            cutoff=spec.cutoff,
            intake_id=intake.intake_id,
            adjudication_id=adjudication.adjudication_id,
            session_artifacts=session_artifacts,
            stable_history_panel_id=history.panel_id,
            adjustment_bridge_id=adjustment.bridge_id,
            effective_tick_panel_id=effective_ticks.panel_id,
            adjustment_readiness=adjustment.readiness,
            adjustment_actionable=adjustment.actionable,
            effective_tick_readiness=effective_ticks.readiness,
            effective_tick_actionable=effective_ticks.actionable,
            paper_only=True,
            execution_eligible=False,
        )
        return stores.publications.put(manifest)


_MANIFEST_KEYS = frozenset(
    {
        "codec_schema_version",
        "schema_version",
        "spec_id",
        "promotion_bindings",
        "identity_evidence_artifact_ids",
        "identity_review_bundle_ids",
        "calendar_materialization_id",
        "session_bindings",
        "corporate_action_snapshot_id",
        "cutoff",
        "intake_id",
        "adjudication_id",
        "session_artifacts",
        "stable_history_panel_id",
        "adjustment_bridge_id",
        "effective_tick_panel_id",
        "adjustment_readiness",
        "adjustment_actionable",
        "effective_tick_readiness",
        "effective_tick_actionable",
        "paper_only",
        "execution_eligible",
        "manifest_id",
    }
)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedGraphPublisherError(_ERR_SHAPE)
        result[key] = value
    return result


def _reject_number(_token: str) -> object:
    raise PromotedGraphPublisherError(_ERR_SHAPE)


def _encode_promotion_binding(value: PromotedGraphPromotionBinding) -> dict[str, object]:
    return {
        "promotion_id": value.promotion_id,
        "expected_report_date": value.expected_report_date.isoformat(),
    }


def _decode_promotion_binding(value: object) -> PromotedGraphPromotionBinding:
    if type(value) is not dict or set(value) != {"promotion_id", "expected_report_date"}:
        raise PromotedGraphPublisherError(_ERR_SHAPE)
    return PromotedGraphPromotionBinding(
        promotion_id=_require_sha(value["promotion_id"], _ERR_SHAPE),
        expected_report_date=_canonical_date(value["expected_report_date"], _ERR_SHAPE),
    )


def _encode_session_binding(value: PromotedGraphSessionBinding) -> dict[str, object]:
    return {
        "market_session": value.market_session.isoformat(),
        "historical_corpus_id": value.historical_corpus_id,
    }


def _decode_session_binding(value: object) -> PromotedGraphSessionBinding:
    if type(value) is not dict or set(value) != {"market_session", "historical_corpus_id"}:
        raise PromotedGraphPublisherError(_ERR_SHAPE)
    return PromotedGraphSessionBinding(
        market_session=_canonical_date(value["market_session"], _ERR_SHAPE),
        historical_corpus_id=_require_sha(value["historical_corpus_id"], _ERR_SHAPE),
    )


def _encode_session_artifacts(value: PromotedGraphSessionArtifacts) -> dict[str, object]:
    return {
        "market_session": value.market_session.isoformat(),
        "universe_id": value.universe_id,
        "frame_id": value.frame_id,
        "tick_snapshot_id": value.tick_snapshot_id,
    }


def _decode_session_artifacts(value: object) -> PromotedGraphSessionArtifacts:
    expected = {"market_session", "universe_id", "frame_id", "tick_snapshot_id"}
    if type(value) is not dict or set(value) != expected:
        raise PromotedGraphPublisherError(_ERR_SHAPE)
    return PromotedGraphSessionArtifacts(
        market_session=_canonical_date(value["market_session"], _ERR_SHAPE),
        universe_id=_require_sha(value["universe_id"], _ERR_SHAPE),
        frame_id=_require_sha(value["frame_id"], _ERR_SHAPE),
        tick_snapshot_id=_require_sha(value["tick_snapshot_id"], _ERR_SHAPE),
    )


def encode_promoted_graph_publication_manifest(
    manifest: PromotedGraphPublicationManifest,
) -> bytes:
    if type(manifest) is not PromotedGraphPublicationManifest:
        raise TypeError("promoted graph publication manifest must be exact")
    manifest.verify_content_identity()
    return (
        json.dumps(
            {
                "codec_schema_version": PROMOTED_GRAPH_MANIFEST_CODEC_VERSION,
                "schema_version": manifest.schema_version,
                "spec_id": manifest.spec_id,
                "promotion_bindings": [
                    _encode_promotion_binding(value)
                    for value in manifest.promotion_bindings
                ],
                "identity_evidence_artifact_ids": list(
                    manifest.identity_evidence_artifact_ids
                ),
                "identity_review_bundle_ids": list(
                    manifest.identity_review_bundle_ids
                ),
                "calendar_materialization_id": manifest.calendar_materialization_id,
                "session_bindings": [
                    _encode_session_binding(value)
                    for value in manifest.session_bindings
                ],
                "corporate_action_snapshot_id": manifest.corporate_action_snapshot_id,
                "cutoff": manifest.cutoff.isoformat(),
                "intake_id": manifest.intake_id,
                "adjudication_id": manifest.adjudication_id,
                "session_artifacts": [
                    _encode_session_artifacts(value)
                    for value in manifest.session_artifacts
                ],
                "stable_history_panel_id": manifest.stable_history_panel_id,
                "adjustment_bridge_id": manifest.adjustment_bridge_id,
                "effective_tick_panel_id": manifest.effective_tick_panel_id,
                "adjustment_readiness": manifest.adjustment_readiness.value,
                "adjustment_actionable": manifest.adjustment_actionable,
                "effective_tick_readiness": manifest.effective_tick_readiness.value,
                "effective_tick_actionable": manifest.effective_tick_actionable,
                "paper_only": manifest.paper_only,
                "execution_eligible": manifest.execution_eligible,
                "manifest_id": manifest.manifest_id,
            },
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def decode_promoted_graph_publication_manifest(
    payload: bytes,
) -> PromotedGraphPublicationManifest:
    if (
        type(payload) is not bytes
        or not payload
        or len(payload) > _MAXIMUM_MANIFEST_BYTES
    ):
        raise PromotedGraphPublisherError(_ERR_BYTES)
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise PromotedGraphPublisherError(_ERR_BYTES) from None
    try:
        decoded = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
    except PromotedGraphPublisherError:
        raise
    except (json.JSONDecodeError, RecursionError):
        raise PromotedGraphPublisherError(_ERR_SHAPE) from None
    if type(decoded) is not dict or set(decoded) != _MANIFEST_KEYS:
        raise PromotedGraphPublisherError(_ERR_SHAPE)
    if (
        decoded["codec_schema_version"] != PROMOTED_GRAPH_MANIFEST_CODEC_VERSION
        or decoded["schema_version"] != PROMOTED_GRAPH_MANIFEST_SCHEMA_VERSION
    ):
        raise PromotedGraphPublisherError(_ERR_SHAPE)

    raw_promotion_bindings = decoded["promotion_bindings"]
    if type(raw_promotion_bindings) is not list or not raw_promotion_bindings:
        raise PromotedGraphPublisherError(_ERR_SHAPE)
    promotion_bindings = tuple(
        _decode_promotion_binding(value) for value in raw_promotion_bindings
    )

    raw_evidence_ids = decoded["identity_evidence_artifact_ids"]
    if type(raw_evidence_ids) is not list:
        raise PromotedGraphPublisherError(_ERR_SHAPE)
    identity_evidence_artifact_ids = tuple(
        _require_sha(value, _ERR_SHAPE) for value in raw_evidence_ids
    )

    raw_review_ids = decoded["identity_review_bundle_ids"]
    if type(raw_review_ids) is not list:
        raise PromotedGraphPublisherError(_ERR_SHAPE)
    identity_review_bundle_ids = tuple(
        _require_sha(value, _ERR_SHAPE) for value in raw_review_ids
    )

    calendar_materialization_id = _require_sha(
        decoded["calendar_materialization_id"], _ERR_SHAPE
    )

    raw_session_bindings = decoded["session_bindings"]
    if type(raw_session_bindings) is not list or not raw_session_bindings:
        raise PromotedGraphPublisherError(_ERR_SHAPE)
    session_bindings = tuple(
        _decode_session_binding(value) for value in raw_session_bindings
    )

    corporate_action_snapshot_id = _require_sha(
        decoded["corporate_action_snapshot_id"], _ERR_SHAPE
    )
    cutoff = _canonical_datetime(decoded["cutoff"], _ERR_SHAPE)
    intake_id = _require_sha(decoded["intake_id"], _ERR_SHAPE)
    adjudication_id = _require_sha(decoded["adjudication_id"], _ERR_SHAPE)

    raw_session_artifacts = decoded["session_artifacts"]
    if type(raw_session_artifacts) is not list or not raw_session_artifacts:
        raise PromotedGraphPublisherError(_ERR_SHAPE)
    session_artifacts = tuple(
        _decode_session_artifacts(value) for value in raw_session_artifacts
    )

    stable_history_panel_id = _require_sha(
        decoded["stable_history_panel_id"], _ERR_SHAPE
    )
    adjustment_bridge_id = _require_sha(decoded["adjustment_bridge_id"], _ERR_SHAPE)
    effective_tick_panel_id = _require_sha(
        decoded["effective_tick_panel_id"], _ERR_SHAPE
    )

    try:
        adjustment_readiness = ReferenceReadiness(decoded["adjustment_readiness"])
        effective_tick_readiness = ReferenceReadiness(
            decoded["effective_tick_readiness"]
        )
    except ValueError:
        raise PromotedGraphPublisherError(_ERR_SHAPE) from None

    adjustment_actionable = decoded["adjustment_actionable"]
    effective_tick_actionable = decoded["effective_tick_actionable"]
    paper_only = decoded["paper_only"]
    execution_eligible = decoded["execution_eligible"]
    if (
        type(adjustment_actionable) is not bool
        or type(effective_tick_actionable) is not bool
        or type(paper_only) is not bool
        or type(execution_eligible) is not bool
    ):
        raise PromotedGraphPublisherError(_ERR_SHAPE)

    stored_manifest_id = _require_sha(decoded["manifest_id"], _ERR_SHAPE)
    stored_spec_id = _require_sha(decoded["spec_id"], _ERR_SHAPE)

    try:
        manifest = PromotedGraphPublicationManifest(
            schema_version=decoded["schema_version"],
            spec_id=stored_spec_id,
            promotion_bindings=promotion_bindings,
            identity_evidence_artifact_ids=identity_evidence_artifact_ids,
            identity_review_bundle_ids=identity_review_bundle_ids,
            calendar_materialization_id=calendar_materialization_id,
            session_bindings=session_bindings,
            corporate_action_snapshot_id=corporate_action_snapshot_id,
            cutoff=cutoff,
            intake_id=intake_id,
            adjudication_id=adjudication_id,
            session_artifacts=session_artifacts,
            stable_history_panel_id=stable_history_panel_id,
            adjustment_bridge_id=adjustment_bridge_id,
            effective_tick_panel_id=effective_tick_panel_id,
            adjustment_readiness=adjustment_readiness,
            adjustment_actionable=adjustment_actionable,
            effective_tick_readiness=effective_tick_readiness,
            effective_tick_actionable=effective_tick_actionable,
            paper_only=paper_only,
            execution_eligible=execution_eligible,
        )
    except PromotedGraphPublisherError:
        raise PromotedGraphPublisherError(_ERR_SHAPE) from None
    if manifest.manifest_id != stored_manifest_id:
        raise PromotedGraphPublisherError(_ERR_SHAPE)
    if encode_promoted_graph_publication_manifest(manifest) != payload:
        raise PromotedGraphPublisherError(_ERR_SHAPE)
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


class LocalPromotedGraphPublicationStore:
    """Durable, create-once root store for PromotedGraphPublicationManifest.

    Exposes only ``put``, ``get``, and ``path_for`` -- no list/latest/
    nearest/find/discovery operation. ``get`` never trusts the stored
    manifest as authority: it strictly decodes it, independently re-resolves
    the identity intake, adjudication, calendar, every per-session universe/
    frame/tick-snapshot, the stable-listing history, the corporate-action
    adjustment, and the effective-session ticks through their own real
    durable stores, and requires the recomputed ``spec_id``, ``manifest_id``,
    and canonical bytes to match before returning anything.
    """

    _DIRECTORY = "promoted-graph-publications"
    _LOCK_FILENAME = ".promoted-graph-publication.lock"

    def __init__(
        self,
        root: Path,
        *,
        identity_intakes,
        identity_adjudications,
        calendars,
        identity_session_universes,
        session_market_data_frames,
        session_tick_snapshots,
        stable_listing_histories,
        corporate_action_adjustments,
        effective_session_ticks,
        replay_scope: _ReplayScope,
    ) -> None:
        self.root = Path(root) / self._DIRECTORY
        self.identity_intakes = identity_intakes
        self.identity_adjudications = identity_adjudications
        self.calendars = calendars
        self.identity_session_universes = identity_session_universes
        self.session_market_data_frames = session_market_data_frames
        self.session_tick_snapshots = session_tick_snapshots
        self.stable_listing_histories = stable_listing_histories
        self.corporate_action_adjustments = corporate_action_adjustments
        self.effective_session_ticks = effective_session_ticks
        self._replay_scope = replay_scope

    def path_for(self, manifest_id: str) -> Path:
        return self.root / f"{_require_sha(manifest_id, _ERR_ID)}.json"

    def put(
        self, manifest: PromotedGraphPublicationManifest
    ) -> PromotedGraphPublicationManifest:
        if type(manifest) is not PromotedGraphPublicationManifest:
            raise TypeError("promoted graph publication manifest must be exact")
        manifest.verify_content_identity()
        self._verify_downstream(manifest)
        payload = encode_promoted_graph_publication_manifest(manifest)
        try:
            replayed = decode_promoted_graph_publication_manifest(payload)
        except PromotedGraphPublisherError:
            raise
        except Exception:
            raise PromotedGraphPublisherError(_ERR_VERIFY) from None
        if replayed != manifest or replayed.manifest_id != manifest.manifest_id:
            raise PromotedGraphPublisherError(_ERR_VERIFY)
        self._publish(manifest.manifest_id, payload)
        return self.get(manifest.manifest_id)

    def get(self, manifest_id: str) -> PromotedGraphPublicationManifest:
        with self._replay_scope.open():
            return self._get(manifest_id)

    def _get(self, manifest_id: str) -> PromotedGraphPublicationManifest:
        path = self.path_for(manifest_id)
        payload = self._read(path)
        manifest = decode_promoted_graph_publication_manifest(payload)
        if manifest.manifest_id != manifest_id:
            raise PromotedGraphPublisherError(_ERR_SHAPE)
        self._verify_downstream(manifest)
        return manifest

    def _verify_downstream(self, manifest: PromotedGraphPublicationManifest) -> None:
        """Replay both terminal roots, then verify their reconstructed graph once.

        The terminal stores already resolve every retained ancestor through the
        exact durable stores wired into them.  Reading each ancestor again here
        caused the same immutable graph to be recursively reconstructed hundreds
        of times.  Traversing the two independently replayed terminal objects
        preserves the full root/substitution checks while avoiding those
        redundant store reads.
        """

        try:
            adjustment = self.corporate_action_adjustments.get(
                manifest.adjustment_bridge_id
            )
            effective_ticks = self.effective_session_ticks.get(
                manifest.effective_tick_panel_id
            )
        except Exception:
            raise PromotedGraphPublisherConflict(_ERR_VERIFY) from None

        if (
            type(adjustment) is not VerifiedPromotedCorporateActionAdjustmentPanel
            or adjustment.bridge_id != manifest.adjustment_bridge_id
            or adjustment.source_panel.panel_id != manifest.stable_history_panel_id
            or adjustment.corporate_actions.snapshot_id
            != manifest.corporate_action_snapshot_id
            or adjustment.readiness != manifest.adjustment_readiness
            or adjustment.actionable != manifest.adjustment_actionable
            or type(effective_ticks) is not VerifiedPromotedEffectiveSessionTickPanel
            or effective_ticks.panel_id != manifest.effective_tick_panel_id
            or effective_ticks.source_panel.panel_id
            != manifest.stable_history_panel_id
            or effective_ticks.readiness != manifest.effective_tick_readiness
            or effective_ticks.actionable != manifest.effective_tick_actionable
            or effective_ticks.source_panel != adjustment.source_panel
        ):
            raise PromotedGraphPublisherConflict(_ERR_VERIFY)

        history = adjustment.source_panel
        if (
            type(history) is not VerifiedPromotedStableListingHistoryPanel
            or history.panel_id != manifest.stable_history_panel_id
            or type(history.calendar) is not CollectionCalendarMaterialization
            or history.calendar.materialization_id
            != manifest.calendar_materialization_id
            or len(history.tick_snapshots) != len(manifest.session_bindings)
        ):
            raise PromotedGraphPublisherConflict(_ERR_VERIFY)

        adjudication: VerifiedPromotedIdentityAdjudication | None = None
        for binding, artifacts, snapshot in zip(
            manifest.session_bindings,
            manifest.session_artifacts,
            history.tick_snapshots,
        ):
            if type(snapshot) is not VerifiedPromotedSessionTickSnapshot:
                raise PromotedGraphPublisherConflict(_ERR_VERIFY)
            frame = snapshot.frame
            if type(frame) is not VerifiedPromotedSessionMarketDataFrame:
                raise PromotedGraphPublisherConflict(_ERR_VERIFY)
            universe = frame.universe
            if (
                type(universe) is not VerifiedPromotedIdentitySessionUniverse
                or artifacts.market_session != binding.market_session
                or universe.universe_id != artifacts.universe_id
                or universe.market_session != binding.market_session
                or universe.calendar.materialization_id
                != manifest.calendar_materialization_id
                or frame.frame_id != artifacts.frame_id
                or frame.universe.universe_id != artifacts.universe_id
                or frame.corpus_index.corpus_id != binding.historical_corpus_id
                or snapshot.snapshot_id != artifacts.tick_snapshot_id
                or snapshot.frame.frame_id != artifacts.frame_id
            ):
                raise PromotedGraphPublisherConflict(_ERR_VERIFY)

            if adjudication is None:
                adjudication = universe.adjudication
            elif universe.adjudication != adjudication:
                raise PromotedGraphPublisherConflict(_ERR_VERIFY)

        if type(adjudication) is not VerifiedPromotedIdentityAdjudication:
            raise PromotedGraphPublisherConflict(_ERR_VERIFY)
        intake = adjudication.intake
        expected_dates = tuple(
            value.expected_report_date for value in manifest.promotion_bindings
        )
        expected_promotion_ids = tuple(
            value.promotion_id for value in manifest.promotion_bindings
        )
        if (
            adjudication.adjudication_id != manifest.adjudication_id
            or type(intake) is not VerifiedPromotedIdentityIntake
            or intake.intake_id != manifest.intake_id
            or intake.expected_report_dates != expected_dates
            or tuple(value.promotion_id for value in intake.promotions)
            != expected_promotion_ids
            or tuple(
                value.manifest.artifact_id
                for value in adjudication.evidence_artifacts
            )
            != manifest.identity_evidence_artifact_ids
            or tuple(
                value.manifest.bundle_id for value in adjudication.review_bundles
            )
            != manifest.identity_review_bundle_ids
        ):
            raise PromotedGraphPublisherConflict(_ERR_VERIFY)

    def _read(self, path: Path) -> bytes:
        if not path.exists():
            raise PromotedGraphPublisherNotFound(_ERR_NOT_FOUND)
        if not path.is_file() or _is_link_like(path):
            raise PromotedGraphPublisherError(_ERR_UNSAFE_PATH)
        try:
            return read_stable_regular_file(
                path, maximum_bytes=_MAXIMUM_MANIFEST_BYTES
            )
        except FileSafetyError:
            raise PromotedGraphPublisherError(_ERR_UNSAFE_PATH) from None

    def _publish(self, manifest_id: str, payload: bytes) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        if not self.root.is_dir() or _is_link_like(self.root):
            raise PromotedGraphPublisherError(_ERR_UNSAFE_PATH)
        target = self.path_for(manifest_id)
        try:
            with advisory_file_lock(self.root / self._LOCK_FILENAME):
                if target.exists():
                    if _is_link_like(target) or self._read(target) != payload:
                        raise PromotedGraphPublisherConflict(_ERR_CONFLICT)
                    return
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".promoted-graph-publication-",
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
        except PromotedGraphPublisherConflict:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise PromotedGraphPublisherConflict(_ERR_CONFLICT) from None
