from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum

from india_swing.identity import content_id
from india_swing.market_data.promoted_session_frame import (
    PromotedSessionMarketDataEntry,
    VerifiedPromotedSessionMarketDataFrame,
)
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference_data.acquisition_promotion import (
    VerifiedReferenceArtifactPromotion,
)
from india_swing.reference_data.models import SourceRowDisposition
from india_swing.reference_data.security_master import NSE_CM_MII_SECURITY_HEADER_INDEX
from india_swing.universe.promoted_identity import PromotedIdentitySessionDisposition

from .models import CollectedTickSizeObservation


class PromotedSessionTickSizeError(ValueError):
    pass


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_REASON = re.compile(r"[A-Z][A-Z0-9_]{2,127}\Z")
_TICK_SIZE_INDEX = NSE_CM_MII_SECURITY_HEADER_INDEX["TickSz"]

PROMOTED_SESSION_TICK_SIZE_SCHEMA_VERSION = "promoted-session-tick-size/v1"
PROMOTED_SESSION_TICK_SIZE_POLICY_VERSION = (
    "promoted-session-tick-size/same-session-observation-not-effective-interval-v1"
)

_NOT_EFFECTIVE_REASON = "SINGLE_SESSION_TICK_OBSERVATION_NOT_EFFECTIVE_INTERVAL"
_COLLECTION_ONLY_REASON = "COLLECTION_ONLY_TICK_SIZE_EVIDENCE"
_NO_TICK_AUTHORITY_REASON = "SOURCE_EXCLUDED_NO_TICK_AUTHORITY"

_ERR_TYPE = "promoted session tick size type is invalid"
_ERR_SCHEMA_VERSION = "promoted session tick size schema version is unsupported"
_ERR_FRAME = "promoted session tick size frame is invalid"
_ERR_FRAME_VERIFY = "promoted session tick size could not verify the source frame"
_ERR_FRAME_STATE = "promoted session tick size frame is not collection-only"
_ERR_CUTOFF = "promoted session tick size cutoff is invalid"
_ERR_CUTOFF_BEFORE_KNOWLEDGE = (
    "promoted session tick size cutoff precedes an authoritative input time"
)
_ERR_PROMOTION_SELECTION = (
    "promoted session tick size could not select exactly one retained promotion "
    "for the frame's session"
)
_ERR_GRAPH = "promoted session tick size could not build its entries"
_ERR_RESERVED_FIELD = (
    "promoted session tick size reserved source field is unexpectedly populated"
)
_ERR_DERIVED_FIELD = "promoted session tick size derived field is invalid"
_ERR_COMPARISON_FAILED = (
    "promoted session tick size retained content could not be verified"
)
_ERR_SNAPSHOT_ID = (
    "promoted session tick size identifier disagrees with independently "
    "recomputed content"
)


def _utc(value: datetime) -> datetime:
    if type(value) is not datetime:
        raise PromotedSessionTickSizeError(_ERR_CUTOFF)
    try:
        offset = value.utcoffset()
    except Exception:
        raise PromotedSessionTickSizeError(_ERR_CUTOFF) from None
    if value.tzinfo is None or offset is None:
        raise PromotedSessionTickSizeError(_ERR_CUTOFF)
    return value.astimezone(timezone.utc)


class PromotedSessionTickStatus(str, Enum):
    TICK_OBSERVED_IDENTITY_RESOLVED_COLLECTION_ONLY = (
        "TICK_OBSERVED_IDENTITY_RESOLVED_COLLECTION_ONLY"
    )
    TICK_OBSERVED_IDENTITY_UNRESOLVED = "TICK_OBSERVED_IDENTITY_UNRESOLVED"
    TICK_SOURCE_EXCLUDED_NON_EQUITY = "TICK_SOURCE_EXCLUDED_NON_EQUITY"
    TICK_SOURCE_EXCLUDED_TEST_SECURITY = "TICK_SOURCE_EXCLUDED_TEST_SECURITY"
    TICK_SOURCE_EXCLUDED_ALTERNATIVE_VENUE = "TICK_SOURCE_EXCLUDED_ALTERNATIVE_VENUE"


_EXCLUDED_TICK_STATUS = {
    SourceRowDisposition.EXCLUDED_NON_EQUITY: (
        PromotedSessionTickStatus.TICK_SOURCE_EXCLUDED_NON_EQUITY
    ),
    SourceRowDisposition.EXCLUDED_TEST_SECURITY: (
        PromotedSessionTickStatus.TICK_SOURCE_EXCLUDED_TEST_SECURITY
    ),
    SourceRowDisposition.EXCLUDED_ALTERNATIVE_VENUE: (
        PromotedSessionTickStatus.TICK_SOURCE_EXCLUDED_ALTERNATIVE_VENUE
    ),
}
_RESOLVED_TICK_STATUSES = frozenset(
    (
        PromotedSessionTickStatus.TICK_OBSERVED_IDENTITY_RESOLVED_COLLECTION_ONLY,
        PromotedSessionTickStatus.TICK_OBSERVED_IDENTITY_UNRESOLVED,
    )
)


def _tick_reasons(
    frame_entry: PromotedSessionMarketDataEntry, status: PromotedSessionTickStatus
) -> tuple[str, ...]:
    if status in _RESOLVED_TICK_STATUSES:
        extra = {_NOT_EFFECTIVE_REASON, _COLLECTION_ONLY_REASON}
    else:
        extra = {_NO_TICK_AUTHORITY_REASON}
    return tuple(sorted({*frame_entry.reason_codes, *extra}))


@dataclass(frozen=True, slots=True)
class PromotedSessionTickEntry:
    """One immutable same-session tick-size disposition for exactly one row
    of the source promoted-session market-data frame.

    A retained equity row -- resolved or unresolved identity alike -- always
    carries exactly one CollectedTickSizeObservation built from the selected
    trusted security master's own BidIntrvl column; identity resolution
    never controls whether a tick observation is attached. An excluded
    source row never carries an observation. ``effective_interval_verified``
    is always False: a single same-session BidIntrvl value is evidence, not
    an effective-dated tick-size interval.
    """

    frame_entry: PromotedSessionMarketDataEntry
    status: PromotedSessionTickStatus
    observation: CollectedTickSizeObservation | None
    effective_interval_verified: bool
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if type(self.frame_entry) is not PromotedSessionMarketDataEntry:
            raise PromotedSessionTickSizeError(_ERR_GRAPH)
        if type(self.status) is not PromotedSessionTickStatus:
            raise PromotedSessionTickSizeError(_ERR_GRAPH)
        if (
            self.observation is not None
            and type(self.observation) is not CollectedTickSizeObservation
        ):
            raise PromotedSessionTickSizeError(_ERR_GRAPH)
        if self.observation is not None:
            try:
                self.observation.verify_content_identity()
            except Exception:
                raise PromotedSessionTickSizeError(_ERR_GRAPH) from None
        if (
            type(self.effective_interval_verified) is not bool
            or self.effective_interval_verified is not False
        ):
            raise PromotedSessionTickSizeError(_ERR_GRAPH)
        if (
            type(self.reason_codes) is not tuple
            or not self.reason_codes
            or self.reason_codes != tuple(sorted(set(self.reason_codes)))
            or any(_REASON.fullmatch(value) is None for value in self.reason_codes)
            or self.reason_codes != _tick_reasons(self.frame_entry, self.status)
        ):
            raise PromotedSessionTickSizeError(_ERR_GRAPH)

        universe_entry = self.frame_entry.universe_entry
        resolved = (
            universe_entry.disposition
            is PromotedIdentitySessionDisposition.IDENTITY_RESOLVED_COLLECTION_ONLY
        )
        unresolved = (
            universe_entry.disposition
            is PromotedIdentitySessionDisposition.IDENTITY_UNRESOLVED
        )
        if resolved:
            expected_status = (
                PromotedSessionTickStatus.TICK_OBSERVED_IDENTITY_RESOLVED_COLLECTION_ONLY
            )
        elif unresolved:
            expected_status = PromotedSessionTickStatus.TICK_OBSERVED_IDENTITY_UNRESOLVED
        else:
            expected_status = _EXCLUDED_TICK_STATUS.get(
                universe_entry.source_disposition
            )
        if self.status is not expected_status:
            raise PromotedSessionTickSizeError(_ERR_GRAPH)
        if self.status in _RESOLVED_TICK_STATUSES:
            if self.observation is None:
                raise PromotedSessionTickSizeError(_ERR_GRAPH)
            if (
                self.observation.source_record_id != universe_entry.source_record_id
                or self.observation.financial_instrument_id
                != universe_entry.financial_instrument_id
                or self.observation.symbol != universe_entry.symbol
                or self.observation.series != universe_entry.series
                or self.observation.validated_isin != universe_entry.validated_isin
            ):
                raise PromotedSessionTickSizeError(_ERR_GRAPH)
        elif self.observation is not None:
            raise PromotedSessionTickSizeError(_ERR_GRAPH)

    @property
    def source_record_id(self) -> str:
        return self.frame_entry.source_record_id

    def _identity(self) -> dict[str, object]:
        return {
            "frame_entry": self.frame_entry._identity(),
            "status": self.status,
            "observation_id": None if self.observation is None else self.observation.observation_id,
            "observation_bid_interval_paise": (
                None if self.observation is None else self.observation.bid_interval_paise
            ),
            "effective_interval_verified": self.effective_interval_verified,
            "reason_codes": self.reason_codes,
        }


def _snapshot_identity(
    *,
    frame_id: str,
    universe_id: str,
    adjudication_id: str,
    identity_snapshot_id: str,
    selected_promotion_id: str,
    selected_source_artifact_id: str,
    selected_source_manifest_id: str,
    selected_raw_sha256: str,
    selected_normalized_sha256: str,
    market_session: date,
    cutoff: datetime,
    knowledge_time: datetime,
    entries: tuple[PromotedSessionTickEntry, ...],
    status_counts: tuple[tuple[str, int], ...],
    reason_counts: tuple[tuple[str, int], ...],
    readiness: ReferenceReadiness,
    actionable: bool,
    training_eligible: bool,
    alert_eligible: bool,
    execution_eligible: bool,
) -> dict[str, object]:
    return {
        "schema_version": PROMOTED_SESSION_TICK_SIZE_SCHEMA_VERSION,
        "policy_version": PROMOTED_SESSION_TICK_SIZE_POLICY_VERSION,
        "frame_id": frame_id,
        "universe_id": universe_id,
        "adjudication_id": adjudication_id,
        "identity_snapshot_id": identity_snapshot_id,
        "selected_promotion_id": selected_promotion_id,
        "selected_source_artifact_id": selected_source_artifact_id,
        "selected_source_manifest_id": selected_source_manifest_id,
        "selected_raw_sha256": selected_raw_sha256,
        "selected_normalized_sha256": selected_normalized_sha256,
        "market_session": market_session,
        "cutoff": cutoff,
        "knowledge_time": knowledge_time,
        "entry_identities": tuple(value._identity() for value in entries),
        "status_counts": status_counts,
        "reason_counts": reason_counts,
        "readiness": readiness,
        "actionable": actionable,
        "training_eligible": training_eligible,
        "alert_eligible": alert_eligible,
        "execution_eligible": execution_eligible,
    }


@dataclass(frozen=True)
class _TickSnapshotFacts:
    """Plain normalized facts returned by the single strict decoder. Never a
    VerifiedPromotedSessionTickSnapshot."""

    cutoff: datetime
    knowledge_time: datetime
    selected_promotion_id: str
    selected_source_artifact_id: str
    selected_source_manifest_id: str
    selected_raw_sha256: str
    selected_normalized_sha256: str
    entries: tuple[PromotedSessionTickEntry, ...]
    status_counts: tuple[tuple[str, int], ...]
    reason_counts: tuple[tuple[str, int], ...]
    readiness: ReferenceReadiness
    actionable: bool
    training_eligible: bool
    alert_eligible: bool
    execution_eligible: bool
    snapshot_id: str


def _select_promotion(
    frame: VerifiedPromotedSessionMarketDataFrame,
) -> VerifiedReferenceArtifactPromotion:
    universe = frame.universe
    matches = tuple(
        value
        for value in universe.adjudication.intake.promotions
        if value.promotion_id == universe.selected_promotion_id
    )
    if len(matches) != 1 or type(matches[0]) is not VerifiedReferenceArtifactPromotion:
        raise PromotedSessionTickSizeError(_ERR_PROMOTION_SELECTION)
    try:
        matches[0].verify_content_identity()
    except Exception:
        raise PromotedSessionTickSizeError(_ERR_PROMOTION_SELECTION) from None
    if (
        matches[0].verified_report_date != universe.market_session
        or matches[0].artifact.manifest.artifact_id != universe.selected_source_artifact_id
        or matches[0].artifact.manifest.manifest_id != universe.selected_source_manifest_id
        or matches[0].artifact.manifest.raw_sha256 != universe.selected_raw_sha256
        or matches[0].artifact.manifest.normalized_sha256
        != universe.selected_normalized_sha256
    ):
        raise PromotedSessionTickSizeError(_ERR_PROMOTION_SELECTION)
    return matches[0]


def _build_tick_entry(
    frame_entry: PromotedSessionMarketDataEntry,
    records_by_id: dict[str, object],
    selected_promotion: VerifiedReferenceArtifactPromotion,
    market_session: date,
) -> PromotedSessionTickEntry:
    universe_entry = frame_entry.universe_entry
    manifest = selected_promotion.artifact.manifest
    if universe_entry.source_disposition is SourceRowDisposition.RETAINED_UNVERIFIED_EQUITY:
        record = records_by_id.get(universe_entry.source_record_id)
        if (
            record is None
            or universe_entry.source_artifact_id != manifest.artifact_id
            or universe_entry.source_manifest_id != manifest.manifest_id
            or record.financial_instrument_id != universe_entry.financial_instrument_id
            or record.ticker_symbol != universe_entry.symbol
            or record.security_series != universe_entry.series
            or record.validated_isin != universe_entry.validated_isin
        ):
            raise PromotedSessionTickSizeError(_ERR_GRAPH)
        if record.raw_fields[_TICK_SIZE_INDEX] != "":
            raise PromotedSessionTickSizeError(_ERR_RESERVED_FIELD)
        try:
            observation = CollectedTickSizeObservation(
                market_session_claim=market_session,
                knowledge_time=selected_promotion.knowledge_time,
                source_artifact_id=manifest.artifact_id,
                source_manifest_id=manifest.manifest_id,
                source_record_id=record.source_record_id,
                financial_instrument_id=record.financial_instrument_id,
                symbol=record.ticker_symbol,
                series=record.security_series,
                validated_isin=record.validated_isin,
                bid_interval_paise=record.bid_interval_paise,
            )
        except PromotedSessionTickSizeError:
            raise
        except Exception:
            raise PromotedSessionTickSizeError(_ERR_GRAPH) from None
        resolved = (
            universe_entry.disposition
            is PromotedIdentitySessionDisposition.IDENTITY_RESOLVED_COLLECTION_ONLY
        )
        status = (
            PromotedSessionTickStatus.TICK_OBSERVED_IDENTITY_RESOLVED_COLLECTION_ONLY
            if resolved
            else PromotedSessionTickStatus.TICK_OBSERVED_IDENTITY_UNRESOLVED
        )
        return PromotedSessionTickEntry(
            frame_entry=frame_entry,
            status=status,
            observation=observation,
            effective_interval_verified=False,
            reason_codes=_tick_reasons(frame_entry, status),
        )
    status = _EXCLUDED_TICK_STATUS.get(universe_entry.source_disposition)
    if status is None:
        raise PromotedSessionTickSizeError(_ERR_GRAPH)
    return PromotedSessionTickEntry(
        frame_entry=frame_entry,
        status=status,
        observation=None,
        effective_interval_verified=False,
        reason_codes=_tick_reasons(frame_entry, status),
    )


def _build_tick_snapshot_facts(
    frame: VerifiedPromotedSessionMarketDataFrame,
    cutoff: datetime,
) -> _TickSnapshotFacts:
    """The single strict tick-size derivation routine.

    Requires an exact VerifiedPromotedSessionMarketDataFrame, independently
    replays its content identity, requires it to remain COLLECTION_ONLY and
    non-actionable/non-training/non-alert/non-execution-eligible, finds the
    exact retained promotion the frame's own universe already selected and
    re-verifies its lineage independently, requires cutoff at or after both
    the frame's and that promotion's own knowledge time, and builds exactly
    one PromotedSessionTickEntry per frame entry -- identity resolution never
    controls whether a retained equity row keeps its tick observation.
    Never constructs VerifiedPromotedSessionTickSnapshot.

    The retained frame/promotion graph is treated as untrusted at every
    replay: every step below deliberately catches ordinary ``Exception``
    (never ``BaseException``, so ``KeyboardInterrupt``/``SystemExit`` still
    propagate) so a malformed nested field fails closed with one static
    sanitized error instead of leaking a raw nested exception.
    """

    if type(frame) is not VerifiedPromotedSessionMarketDataFrame:
        raise PromotedSessionTickSizeError(_ERR_FRAME)

    cutoff = _utc(cutoff)

    try:
        frame.verify_content_identity()
    except Exception:
        raise PromotedSessionTickSizeError(_ERR_FRAME_VERIFY) from None

    if (
        frame.readiness is not ReferenceReadiness.COLLECTION_ONLY
        or frame.actionable is not False
        or frame.training_eligible is not False
        or frame.alert_eligible is not False
        or frame.execution_eligible is not False
    ):
        raise PromotedSessionTickSizeError(_ERR_FRAME_STATE)

    try:
        selected_promotion = _select_promotion(frame)
    except PromotedSessionTickSizeError:
        raise
    except Exception:
        raise PromotedSessionTickSizeError(_ERR_PROMOTION_SELECTION) from None

    knowledge_time = max(frame.knowledge_time, selected_promotion.knowledge_time)
    if cutoff < frame.knowledge_time or cutoff < selected_promotion.knowledge_time:
        raise PromotedSessionTickSizeError(_ERR_CUTOFF_BEFORE_KNOWLEDGE)

    try:
        records_by_id: dict[str, object] = {}
        for record in selected_promotion.artifact.parsed.records:
            if record.source_record_id in records_by_id:
                raise PromotedSessionTickSizeError(_ERR_GRAPH)
            records_by_id[record.source_record_id] = record

        entries = tuple(
            _build_tick_entry(
                frame_entry, records_by_id, selected_promotion, frame.market_session
            )
            for frame_entry in frame.entries
        )
    except PromotedSessionTickSizeError:
        raise
    except Exception:
        raise PromotedSessionTickSizeError(_ERR_GRAPH) from None

    entries = tuple(sorted(entries, key=lambda value: value.source_record_id))
    if len(entries) != len(frame.entries) or {
        value.source_record_id for value in entries
    } != {value.source_record_id for value in frame.entries}:
        raise PromotedSessionTickSizeError(_ERR_GRAPH)

    status_totals: dict[str, int] = {}
    reason_totals: dict[str, int] = {}
    for entry in entries:
        status_totals[entry.status.value] = status_totals.get(entry.status.value, 0) + 1
        for reason in entry.reason_codes:
            reason_totals[reason] = reason_totals.get(reason, 0) + 1
    status_counts = tuple(sorted(status_totals.items()))
    reason_counts = tuple(sorted(reason_totals.items()))

    readiness = ReferenceReadiness.COLLECTION_ONLY
    actionable = False
    training_eligible = False
    alert_eligible = False
    execution_eligible = False

    manifest = selected_promotion.artifact.manifest
    snapshot_id = content_id(
        _snapshot_identity(
            frame_id=frame.frame_id,
            universe_id=frame.universe.universe_id,
            adjudication_id=frame.universe.adjudication.adjudication_id,
            identity_snapshot_id=frame.universe.adjudication.snapshot.snapshot_id,
            selected_promotion_id=selected_promotion.promotion_id,
            selected_source_artifact_id=manifest.artifact_id,
            selected_source_manifest_id=manifest.manifest_id,
            selected_raw_sha256=manifest.raw_sha256,
            selected_normalized_sha256=manifest.normalized_sha256,
            market_session=frame.market_session,
            cutoff=cutoff,
            knowledge_time=knowledge_time,
            entries=entries,
            status_counts=status_counts,
            reason_counts=reason_counts,
            readiness=readiness,
            actionable=actionable,
            training_eligible=training_eligible,
            alert_eligible=alert_eligible,
            execution_eligible=execution_eligible,
        ),
        length=64,
    )

    return _TickSnapshotFacts(
        cutoff=cutoff,
        knowledge_time=knowledge_time,
        selected_promotion_id=selected_promotion.promotion_id,
        selected_source_artifact_id=manifest.artifact_id,
        selected_source_manifest_id=manifest.manifest_id,
        selected_raw_sha256=manifest.raw_sha256,
        selected_normalized_sha256=manifest.normalized_sha256,
        entries=entries,
        status_counts=status_counts,
        reason_counts=reason_counts,
        readiness=readiness,
        actionable=actionable,
        training_eligible=training_eligible,
        alert_eligible=alert_eligible,
        execution_eligible=execution_eligible,
        snapshot_id=snapshot_id,
    )


@dataclass(frozen=True, slots=True)
class VerifiedPromotedSessionTickSnapshot:
    """Immutable, content-addressed same-session tick-size attachment
    bridging one VerifiedPromotedSessionMarketDataFrame and its exact
    selected trusted security master.

    A same-session BidIntrvl observation is never treated as an effective
    tick-size interval, listing interval, active listing, or tradability
    state: ``effective_interval_verified`` is always False on every entry,
    and ``readiness`` is always COLLECTION_ONLY with ``actionable``,
    ``training_eligible``, ``alert_eligible``, and ``execution_eligible``
    always False. __post_init__ calls verify_content_identity(), which
    requires the exact concrete type (rejecting subclasses/impostors even
    when every retained field is otherwise valid) and independently replays
    the complete derivation, so direct construction with a mismatched value
    or post-construction mutation anywhere in the retained graph fails
    closed with one static sanitized PromotedSessionTickSizeError.
    """

    schema_version: str
    policy_version: str
    frame: VerifiedPromotedSessionMarketDataFrame
    market_session: date
    cutoff: datetime
    knowledge_time: datetime
    selected_promotion_id: str
    selected_source_artifact_id: str
    selected_source_manifest_id: str
    selected_raw_sha256: str
    selected_normalized_sha256: str
    entries: tuple[PromotedSessionTickEntry, ...]
    status_counts: tuple[tuple[str, int], ...]
    reason_counts: tuple[tuple[str, int], ...]
    readiness: ReferenceReadiness
    actionable: bool
    training_eligible: bool
    alert_eligible: bool
    execution_eligible: bool
    snapshot_id: str

    def __post_init__(self) -> None:
        self.verify_content_identity()

    def verify_content_identity(self) -> None:
        if type(self) is not VerifiedPromotedSessionTickSnapshot:
            raise PromotedSessionTickSizeError(_ERR_TYPE)
        if (
            type(self.schema_version) is not str
            or self.schema_version != PROMOTED_SESSION_TICK_SIZE_SCHEMA_VERSION
        ):
            raise PromotedSessionTickSizeError(_ERR_SCHEMA_VERSION)
        if (
            type(self.policy_version) is not str
            or self.policy_version != PROMOTED_SESSION_TICK_SIZE_POLICY_VERSION
        ):
            raise PromotedSessionTickSizeError(_ERR_SCHEMA_VERSION)
        if type(self.frame) is not VerifiedPromotedSessionMarketDataFrame:
            raise PromotedSessionTickSizeError(_ERR_FRAME)
        if type(self.market_session) is not date:
            raise PromotedSessionTickSizeError(_ERR_DERIVED_FIELD)
        if type(self.cutoff) is not datetime:
            raise PromotedSessionTickSizeError(_ERR_CUTOFF)
        if type(self.knowledge_time) is not datetime:
            raise PromotedSessionTickSizeError(_ERR_DERIVED_FIELD)
        for value in (
            self.selected_promotion_id,
            self.selected_source_artifact_id,
            self.selected_source_manifest_id,
            self.selected_raw_sha256,
            self.selected_normalized_sha256,
        ):
            if type(value) is not str or _SHA256.fullmatch(value) is None:
                raise PromotedSessionTickSizeError(_ERR_DERIVED_FIELD)
        if type(self.entries) is not tuple or any(
            type(value) is not PromotedSessionTickEntry for value in self.entries
        ):
            raise PromotedSessionTickSizeError(_ERR_DERIVED_FIELD)
        for values in (self.status_counts, self.reason_counts):
            if type(values) is not tuple or any(
                type(pair) is not tuple or len(pair) != 2 for pair in values
            ):
                raise PromotedSessionTickSizeError(_ERR_DERIVED_FIELD)
        if (
            type(self.readiness) is not ReferenceReadiness
            or self.readiness is not ReferenceReadiness.COLLECTION_ONLY
        ):
            raise PromotedSessionTickSizeError(_ERR_DERIVED_FIELD)
        for flag_name in (
            "actionable",
            "training_eligible",
            "alert_eligible",
            "execution_eligible",
        ):
            value = getattr(self, flag_name)
            if type(value) is not bool or value is not False:
                raise PromotedSessionTickSizeError(_ERR_DERIVED_FIELD)
        if type(self.snapshot_id) is not str or _SHA256.fullmatch(self.snapshot_id) is None:
            raise PromotedSessionTickSizeError(_ERR_SNAPSHOT_ID)

        try:
            facts = _build_tick_snapshot_facts(self.frame, self.cutoff)
        except PromotedSessionTickSizeError:
            raise
        except Exception:
            raise PromotedSessionTickSizeError(_ERR_DERIVED_FIELD) from None

        # Every comparison below may recursively invoke a retained nested
        # value's own __eq__, so a single fail-closed boundary wraps all of
        # them: an already-raised PromotedSessionTickSizeError from an
        # intentional mismatch below propagates unchanged, while any other
        # ordinary Exception raised by equality itself (never BaseException/
        # KeyboardInterrupt/SystemExit) is translated to one static
        # sanitized message.
        try:
            if self.market_session != self.frame.market_session:
                raise PromotedSessionTickSizeError(_ERR_DERIVED_FIELD)
            if self.cutoff != facts.cutoff:
                raise PromotedSessionTickSizeError(_ERR_DERIVED_FIELD)
            if self.knowledge_time != facts.knowledge_time:
                raise PromotedSessionTickSizeError(_ERR_DERIVED_FIELD)
            if self.selected_promotion_id != facts.selected_promotion_id:
                raise PromotedSessionTickSizeError(_ERR_DERIVED_FIELD)
            if self.selected_source_artifact_id != facts.selected_source_artifact_id:
                raise PromotedSessionTickSizeError(_ERR_DERIVED_FIELD)
            if self.selected_source_manifest_id != facts.selected_source_manifest_id:
                raise PromotedSessionTickSizeError(_ERR_DERIVED_FIELD)
            if self.selected_raw_sha256 != facts.selected_raw_sha256:
                raise PromotedSessionTickSizeError(_ERR_DERIVED_FIELD)
            if self.selected_normalized_sha256 != facts.selected_normalized_sha256:
                raise PromotedSessionTickSizeError(_ERR_DERIVED_FIELD)
            if self.entries != facts.entries:
                raise PromotedSessionTickSizeError(_ERR_DERIVED_FIELD)
            if self.status_counts != facts.status_counts:
                raise PromotedSessionTickSizeError(_ERR_DERIVED_FIELD)
            if self.reason_counts != facts.reason_counts:
                raise PromotedSessionTickSizeError(_ERR_DERIVED_FIELD)
            if self.snapshot_id != facts.snapshot_id:
                raise PromotedSessionTickSizeError(_ERR_SNAPSHOT_ID)
        except PromotedSessionTickSizeError:
            raise
        except Exception:
            raise PromotedSessionTickSizeError(_ERR_COMPARISON_FAILED) from None


class PromotedSessionTickSizeService:
    """Attaches exact same-session tick-size observations from the selected
    trusted promoted security master to every retained row of one
    VerifiedPromotedSessionMarketDataFrame.

    Never infers, rounds, or projects an effective tick-size interval,
    listing interval, active listing, suspension, surveillance, liquidity,
    corporate-action, or tradability state. Never selects a latest/nearest
    promotion or uses the filesystem/wall clock.
    """

    def materialize(
        self,
        *,
        frame: VerifiedPromotedSessionMarketDataFrame,
        cutoff: datetime,
    ) -> VerifiedPromotedSessionTickSnapshot:
        facts = _build_tick_snapshot_facts(frame, cutoff)
        return VerifiedPromotedSessionTickSnapshot(
            schema_version=PROMOTED_SESSION_TICK_SIZE_SCHEMA_VERSION,
            policy_version=PROMOTED_SESSION_TICK_SIZE_POLICY_VERSION,
            frame=frame,
            market_session=frame.market_session,
            cutoff=facts.cutoff,
            knowledge_time=facts.knowledge_time,
            selected_promotion_id=facts.selected_promotion_id,
            selected_source_artifact_id=facts.selected_source_artifact_id,
            selected_source_manifest_id=facts.selected_source_manifest_id,
            selected_raw_sha256=facts.selected_raw_sha256,
            selected_normalized_sha256=facts.selected_normalized_sha256,
            entries=facts.entries,
            status_counts=facts.status_counts,
            reason_counts=facts.reason_counts,
            readiness=facts.readiness,
            actionable=facts.actionable,
            training_eligible=facts.training_eligible,
            alert_eligible=facts.alert_eligible,
            execution_eligible=facts.execution_eligible,
            snapshot_id=facts.snapshot_id,
        )
