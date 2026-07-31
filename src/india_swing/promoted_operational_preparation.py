"""One restart-safe, exact-manifest bridge from a published promoted
research run into a paper-only operational-preparation boundary.

This module deliberately does not create or coerce a ``SwingProposalBatch``/
``SwingTechnicalProposal``: a ``PromotedResearchTradeIntent`` does not carry
the exact ``SwingInputAssembly``, ``UniverseEntry``, ``CalendarSnapshot``,
metrics, or configuration that graph requires, and fabricating a partial one
would be worse than not producing one. Instead it retains the exact
``PromotedResearchTradeIntent`` objects a promoted research run selected,
together with their complete promoted lineage, and derives only a canonical
NSE listing key and target session for each -- no price range, probability,
confidence, ATR, calendar window, quantity, stop, or target is invented; all
of those values remain authoritative only inside the retained intent. A
published preparation is paper research evidence only: ``paper_only`` is
always true and both ``notification_eligible``/``execution_eligible`` are
always false, and the retained readiness/authority flags are carried through
exactly as the promoted research run produced them, never upgraded.
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

from india_swing._exact_replay import ExactReplayScope
from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.evaluation.promoted_intent_store import (
    LocalPromotedResearchIntentStore,
)
from india_swing.evaluation.promoted_intents import (
    PromotedResearchTradeIntent,
    VerifiedPromotedResearchIntentBatch,
)
from india_swing.identity import content_id
from india_swing.promoted_engine import (
    LocalPromotedEngineRunStore,
    PromotedEngineRunManifest,
)
from india_swing.promoted_graph_publisher import ReferenceReadiness
from india_swing.promoted_research_run import (
    LocalPromotedResearchRunStore,
    PromotedResearchRunManifest,
    build_promoted_research_stores,
)


class PromotedOperationalPreparationError(ValueError):
    pass


class PromotedOperationalPreparationConflict(PromotedOperationalPreparationError):
    pass


class PromotedOperationalPreparationNotFound(PromotedOperationalPreparationError):
    pass


PROMOTED_OPERATIONAL_CANDIDATE_SCHEMA_VERSION = "promoted-operational-candidate/v1"
PROMOTED_OPERATIONAL_PREPARATION_MANIFEST_SCHEMA_VERSION = (
    "promoted-operational-preparation-manifest/v1"
)
PROMOTED_OPERATIONAL_PREPARATION_MANIFEST_CODEC_VERSION = (
    "promoted-operational-preparation-manifest-json/v1"
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_LISTING_KEY = re.compile(r"NSE:[A-Z0-9&.\-]{1,32}\Z")
_MAXIMUM_MANIFEST_BYTES = 4 * 1024 * 1024

_ERR_TYPE = "promoted operational preparation type is invalid"
_ERR_CANDIDATE = "promoted operational candidate is invalid"
_ERR_GRAPH = "promoted operational preparation manifest is invalid"
_ERR_ID = "promoted operational preparation identifier is invalid"
_ERR_SOURCE = "promoted operational preparation could not resolve its exact lineage"
_ERR_LINEAGE_MISMATCH = (
    "promoted operational preparation resolved lineage does not agree with"
    " its own retained cross-references"
)
_ERR_DUPLICATE = "promoted operational preparation contains a duplicate candidate"
_ERR_VERIFY = "promoted operational preparation manifest could not be verified"
_ERR_CONFLICT = (
    "promoted operational preparation already stores different content"
)
_ERR_NOT_FOUND = "promoted operational preparation was not found"
_ERR_UNSAFE_PATH = "promoted operational preparation path is unsafe"
_ERR_BYTES = "promoted operational preparation manifest bytes are invalid"
_ERR_SHAPE = "promoted operational preparation manifest shape is invalid"


def _sha(value: object) -> bool:
    return type(value) is str and _SHA256.fullmatch(value) is not None


def _require_sha(value: object, message: str) -> str:
    if not _sha(value):
        raise PromotedOperationalPreparationError(message)
    return value  # type: ignore[return-value]


def _utc(value: object, message: str) -> datetime:
    if type(value) is not datetime:
        raise PromotedOperationalPreparationError(message)
    try:
        offset = value.utcoffset()
    except Exception:
        raise PromotedOperationalPreparationError(message) from None
    if value.tzinfo is None or offset is None:
        raise PromotedOperationalPreparationError(message)
    return value.astimezone(timezone.utc)


def _canonical_date(value: object, message: str) -> date:
    if type(value) is not str:
        raise PromotedOperationalPreparationError(message)
    try:
        result = date.fromisoformat(value)
    except ValueError:
        raise PromotedOperationalPreparationError(message) from None
    if result.isoformat() != value:
        raise PromotedOperationalPreparationError(message)
    return result


def _canonical_datetime(value: object, message: str) -> datetime:
    if type(value) is not str:
        raise PromotedOperationalPreparationError(message)
    try:
        result = datetime.fromisoformat(value)
    except ValueError:
        raise PromotedOperationalPreparationError(message) from None
    offset = result.utcoffset()
    if (
        result.tzinfo is None
        or offset is None
        or offset.total_seconds() != 0
        or result.isoformat() != value
    ):
        raise PromotedOperationalPreparationError(message)
    return result


@dataclass(frozen=True, slots=True)
class PromotedOperationalCandidate:
    """One retained research trade intent bound to its exact source lineage.

    Never invents a price range, probability, confidence, ATR, calendar
    window, or a new quantity/stop/target: every such value remains
    authoritative only inside the retained ``research_intent``. The only new
    values this type derives are the canonical NSE ``listing_key`` (from
    ``research_intent.evaluation_intent.entry_order.symbol`` alone) and
    ``target_session`` (the entry order's own, single-day
    ``first_eligible_session``/``expiry_session``, which must already agree).
    """

    research_run_id: str
    research_intent_batch_id: str
    research_intent: PromotedResearchTradeIntent
    listing_key: str
    target_session: date
    candidate_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            not _sha(self.research_run_id)
            or not _sha(self.research_intent_batch_id)
            or type(self.research_intent) is not PromotedResearchTradeIntent
            or type(self.listing_key) is not str
            or _LISTING_KEY.fullmatch(self.listing_key) is None
            or type(self.target_session) is not date
        ):
            raise PromotedOperationalPreparationError(_ERR_CANDIDATE)
        try:
            self.research_intent.verify_content_identity()
        except Exception:
            raise PromotedOperationalPreparationError(_ERR_CANDIDATE) from None
        entry_order = self.research_intent.evaluation_intent.entry_order
        expected_listing_key = f"NSE:{entry_order.symbol}"
        if (
            self.listing_key != expected_listing_key
            or entry_order.first_eligible_session != entry_order.expiry_session
            or self.target_session != entry_order.first_eligible_session
        ):
            raise PromotedOperationalPreparationError(_ERR_CANDIDATE)
        object.__setattr__(self, "candidate_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_OPERATIONAL_CANDIDATE_SCHEMA_VERSION,
                "research_run_id": self.research_run_id,
                "research_intent_batch_id": self.research_intent_batch_id,
                "research_intent_id": self.research_intent.research_intent_id,
                "listing_key": self.listing_key,
                "target_session": self.target_session,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if type(self) is not PromotedOperationalCandidate:
            raise PromotedOperationalPreparationError(_ERR_TYPE)
        try:
            fresh = PromotedOperationalCandidate(
                research_run_id=self.research_run_id,
                research_intent_batch_id=self.research_intent_batch_id,
                research_intent=self.research_intent,
                listing_key=self.listing_key,
                target_session=self.target_session,
            )
        except PromotedOperationalPreparationError:
            raise
        except Exception:
            raise PromotedOperationalPreparationError(_ERR_CANDIDATE) from None
        if self.candidate_id != fresh.candidate_id:
            raise PromotedOperationalPreparationError(_ERR_ID)


@dataclass(frozen=True, slots=True)
class PromotedOperationalPreparationManifest:
    """Canonical, content-derived, create-once manifest for one operational
    preparation.

    ``candidate_ids``/``research_intent_ids``/``listing_keys`` preserve the
    exact canonical order of ``VerifiedPromotedResearchIntentBatch.intents``
    -- never re-ranked by symbol and never silently deduplicated. Grants no
    trading, alert, or execution authority: ``paper_only`` is always true and
    both ``notification_eligible``/``execution_eligible`` are always false;
    ``readiness`` is carried through from the source research batch exactly,
    never upgraded.
    """

    schema_version: str
    research_run_id: str
    research_request_id: str
    graph_manifest_id: str
    graph_request_id: str
    engine_run_id: str
    engine_request_id: str
    research_intent_batch_id: str
    signal_session: date
    target_session: date
    cutoff: datetime
    candidate_ids: tuple[str, ...]
    research_intent_ids: tuple[str, ...]
    listing_keys: tuple[str, ...]
    selected_count: int
    blocked_count: int
    source_universe_complete: bool
    readiness: ReferenceReadiness
    paper_only: bool
    notification_eligible: bool
    execution_eligible: bool
    preparation_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != PROMOTED_OPERATIONAL_PREPARATION_MANIFEST_SCHEMA_VERSION
            or not _sha(self.research_run_id)
            or not _sha(self.research_request_id)
            or not _sha(self.graph_manifest_id)
            or not _sha(self.graph_request_id)
            or not _sha(self.engine_run_id)
            or not _sha(self.engine_request_id)
            or not _sha(self.research_intent_batch_id)
            or type(self.signal_session) is not date
            or type(self.target_session) is not date
            or type(self.candidate_ids) is not tuple
            or any(not _sha(value) for value in self.candidate_ids)
            or len(set(self.candidate_ids)) != len(self.candidate_ids)
            or type(self.research_intent_ids) is not tuple
            or any(not _sha(value) for value in self.research_intent_ids)
            or len(set(self.research_intent_ids)) != len(self.research_intent_ids)
            or type(self.listing_keys) is not tuple
            or any(
                type(value) is not str or _LISTING_KEY.fullmatch(value) is None
                for value in self.listing_keys
            )
            or len(set(self.listing_keys)) != len(self.listing_keys)
            or len(
                {
                    (candidate, intent, key)
                    for candidate, intent, key in zip(
                        self.candidate_ids,
                        self.research_intent_ids,
                        self.listing_keys,
                    )
                }
            )
            != len(self.candidate_ids)
            or len(self.candidate_ids) != len(self.research_intent_ids)
            or len(self.candidate_ids) != len(self.listing_keys)
            or type(self.selected_count) is not int
            or self.selected_count < 0
            or self.selected_count != len(self.candidate_ids)
            or type(self.blocked_count) is not int
            or self.blocked_count < 0
            or type(self.source_universe_complete) is not bool
            or type(self.readiness) is not ReferenceReadiness
            or self.readiness is not ReferenceReadiness.COLLECTION_ONLY
            or self.target_session <= self.signal_session
            or self.paper_only is not True
            or self.notification_eligible is not False
            or self.execution_eligible is not False
        ):
            raise PromotedOperationalPreparationError(_ERR_GRAPH)
        cutoff = _utc(self.cutoff, _ERR_GRAPH)
        if cutoff != self.cutoff:
            object.__setattr__(self, "cutoff", cutoff)
        object.__setattr__(self, "preparation_id", self._calculated_id())

    def _identity(self) -> dict[str, object]:
        return {
            value.name: getattr(self, value.name)
            for value in fields(self)
            if value.name != "preparation_id"
        }

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": PROMOTED_OPERATIONAL_PREPARATION_MANIFEST_SCHEMA_VERSION,
                **self._identity(),
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if type(self) is not PromotedOperationalPreparationManifest:
            raise PromotedOperationalPreparationError(_ERR_TYPE)
        expected = PromotedOperationalPreparationManifest(**self._identity())
        if self.preparation_id != expected.preparation_id:
            raise PromotedOperationalPreparationError(_ERR_ID)


@dataclass(frozen=True, slots=True)
class VerifiedPromotedOperationalPreparation:
    """Immutable, independently re-verifiable bridge from one exact promoted
    research run to its paper-only operational-preparation candidates.

    Retains the complete resolved lineage (the research-run manifest, the
    engine-run manifest, and the research-intent batch) alongside the
    derived manifest and exact candidate tuple, and re-checks every
    cross-object ID, session, cutoff, count, readiness, and authority flag
    on every construction.
    """

    research_run_manifest: PromotedResearchRunManifest
    engine_run_manifest: PromotedEngineRunManifest
    research_intent_batch: VerifiedPromotedResearchIntentBatch
    manifest: PromotedOperationalPreparationManifest
    candidates: tuple[PromotedOperationalCandidate, ...]

    def __post_init__(self) -> None:
        if (
            type(self.research_run_manifest) is not PromotedResearchRunManifest
            or type(self.engine_run_manifest) is not PromotedEngineRunManifest
            or type(self.research_intent_batch)
            is not VerifiedPromotedResearchIntentBatch
            or type(self.manifest) is not PromotedOperationalPreparationManifest
            or type(self.candidates) is not tuple
            or any(
                type(value) is not PromotedOperationalCandidate
                for value in self.candidates
            )
        ):
            raise PromotedOperationalPreparationError(_ERR_TYPE)
        try:
            self.research_run_manifest.verify_content_identity()
            self.engine_run_manifest.verify_content_identity()
            self.research_intent_batch.verify_content_identity()
            self.manifest.verify_content_identity()
            for value in self.candidates:
                value.verify_content_identity()
        except PromotedOperationalPreparationError:
            raise
        except Exception:
            raise PromotedOperationalPreparationError(_ERR_LINEAGE_MISMATCH) from None

        manifest = self.manifest
        if (
            self.research_run_manifest.research_run_id != manifest.research_run_id
            or self.research_run_manifest.research_request_id
            != manifest.research_request_id
            or self.research_run_manifest.graph_manifest_id
            != manifest.graph_manifest_id
            or self.research_run_manifest.graph_spec_id != manifest.graph_request_id
            or self.research_run_manifest.engine_run_id != manifest.engine_run_id
            or self.research_run_manifest.engine_request_id
            != manifest.engine_request_id
            or self.research_run_manifest.signal_session != manifest.signal_session
            or self.research_run_manifest.entry_session != manifest.target_session
            or self.research_run_manifest.cutoff != manifest.cutoff
            or self.research_run_manifest.paper_only is not True
            or self.research_run_manifest.notification_eligible is not False
            or self.research_run_manifest.execution_eligible is not False
        ):
            raise PromotedOperationalPreparationError(_ERR_LINEAGE_MISMATCH)
        if (
            self.engine_run_manifest.run_id != manifest.engine_run_id
            or self.engine_run_manifest.request_id != manifest.engine_request_id
            or self.engine_run_manifest.research_intent_batch_id
            != manifest.research_intent_batch_id
            or self.engine_run_manifest.signal_session != manifest.signal_session
            or self.engine_run_manifest.entry_session != manifest.target_session
            or self.engine_run_manifest.cutoff != manifest.cutoff
            or self.engine_run_manifest.paper_only is not True
        ):
            raise PromotedOperationalPreparationError(_ERR_LINEAGE_MISMATCH)
        if (
            self.research_intent_batch.batch_id != manifest.research_intent_batch_id
            or self.research_intent_batch.signal_session != manifest.signal_session
            or self.research_intent_batch.entry_session != manifest.target_session
            or self.research_intent_batch.selected_count != manifest.selected_count
            or self.research_intent_batch.blocked_count != manifest.blocked_count
            or self.research_intent_batch.source_universe_complete
            != manifest.source_universe_complete
            or self.research_intent_batch.readiness != manifest.readiness
            or self.research_intent_batch.actionable is not False
            or self.research_intent_batch.alert_eligible is not False
            or self.research_intent_batch.execution_eligible is not False
            or len(self.research_intent_batch.intents) != len(self.candidates)
        ):
            raise PromotedOperationalPreparationError(_ERR_LINEAGE_MISMATCH)

        for candidate, intent, candidate_id, research_intent_id, listing_key in zip(
            self.candidates,
            self.research_intent_batch.intents,
            manifest.candidate_ids,
            manifest.research_intent_ids,
            manifest.listing_keys,
        ):
            if (
                candidate.research_intent != intent
                or candidate.research_run_id != manifest.research_run_id
                or candidate.research_intent_batch_id
                != manifest.research_intent_batch_id
                or candidate.candidate_id != candidate_id
                or candidate.research_intent.research_intent_id
                != research_intent_id
                or candidate.listing_key != listing_key
                or candidate.target_session != manifest.target_session
            ):
                raise PromotedOperationalPreparationError(_ERR_LINEAGE_MISMATCH)

    def verify_content_identity(self) -> None:
        if type(self) is not VerifiedPromotedOperationalPreparation:
            raise PromotedOperationalPreparationError(_ERR_TYPE)
        self.__post_init__()


class PromotedOperationalPreparationService:
    """Derives one paper-only operational preparation from exact, already-
    verified research-run/engine-run/research-intent-batch objects.

    Never discovers a different run, never selects or reranks a candidate,
    and never changes a risk/quantity/entry/stop/target/tick/cost-buffer/
    holding-period value retained inside a research intent.
    """

    def prepare(
        self,
        *,
        research_run_manifest: PromotedResearchRunManifest,
        engine_run_manifest: PromotedEngineRunManifest,
        research_intent_batch: VerifiedPromotedResearchIntentBatch,
    ) -> VerifiedPromotedOperationalPreparation:
        if type(research_run_manifest) is not PromotedResearchRunManifest:
            raise TypeError("promoted research run manifest must be exact")
        if type(engine_run_manifest) is not PromotedEngineRunManifest:
            raise TypeError("promoted engine run manifest must be exact")
        if (
            type(research_intent_batch)
            is not VerifiedPromotedResearchIntentBatch
        ):
            raise TypeError("promoted research intent batch must be exact")
        research_run_manifest.verify_content_identity()
        engine_run_manifest.verify_content_identity()
        research_intent_batch.verify_content_identity()

        if (
            research_run_manifest.engine_run_id != engine_run_manifest.run_id
            or engine_run_manifest.research_intent_batch_id
            != research_intent_batch.batch_id
            or research_run_manifest.signal_session
            != engine_run_manifest.signal_session
            or research_run_manifest.entry_session
            != engine_run_manifest.entry_session
            or research_run_manifest.cutoff != engine_run_manifest.cutoff
            or engine_run_manifest.signal_session
            != research_intent_batch.signal_session
            or engine_run_manifest.entry_session
            != research_intent_batch.entry_session
            or research_run_manifest.paper_only is not True
            or research_run_manifest.notification_eligible is not False
            or research_run_manifest.execution_eligible is not False
        ):
            raise PromotedOperationalPreparationError(_ERR_LINEAGE_MISMATCH)

        candidates: list[PromotedOperationalCandidate] = []
        for intent in research_intent_batch.intents:
            entry_order = intent.evaluation_intent.entry_order
            candidates.append(
                PromotedOperationalCandidate(
                    research_run_id=research_run_manifest.research_run_id,
                    research_intent_batch_id=research_intent_batch.batch_id,
                    research_intent=intent,
                    listing_key=f"NSE:{entry_order.symbol}",
                    target_session=research_intent_batch.entry_session,
                )
            )
        candidates_tuple = tuple(candidates)

        candidate_ids = tuple(value.candidate_id for value in candidates_tuple)
        research_intent_ids = tuple(
            value.research_intent.research_intent_id for value in candidates_tuple
        )
        listing_keys = tuple(value.listing_key for value in candidates_tuple)
        stable_instrument_ids = tuple(
            value.research_intent.stable_instrument_id for value in candidates_tuple
        )
        stable_listing_ids = tuple(
            value.research_intent.stable_listing_id for value in candidates_tuple
        )
        for values in (
            candidate_ids,
            research_intent_ids,
            listing_keys,
            stable_instrument_ids,
            stable_listing_ids,
        ):
            if len(set(values)) != len(values):
                raise PromotedOperationalPreparationError(_ERR_DUPLICATE)

        manifest = PromotedOperationalPreparationManifest(
            schema_version=PROMOTED_OPERATIONAL_PREPARATION_MANIFEST_SCHEMA_VERSION,
            research_run_id=research_run_manifest.research_run_id,
            research_request_id=research_run_manifest.research_request_id,
            graph_manifest_id=research_run_manifest.graph_manifest_id,
            graph_request_id=research_run_manifest.graph_spec_id,
            engine_run_id=engine_run_manifest.run_id,
            engine_request_id=engine_run_manifest.request_id,
            research_intent_batch_id=research_intent_batch.batch_id,
            signal_session=research_run_manifest.signal_session,
            target_session=research_intent_batch.entry_session,
            cutoff=research_run_manifest.cutoff,
            candidate_ids=candidate_ids,
            research_intent_ids=research_intent_ids,
            listing_keys=listing_keys,
            selected_count=research_intent_batch.selected_count,
            blocked_count=research_intent_batch.blocked_count,
            source_universe_complete=research_intent_batch.source_universe_complete,
            readiness=research_intent_batch.readiness,
            paper_only=True,
            notification_eligible=False,
            execution_eligible=False,
        )

        return VerifiedPromotedOperationalPreparation(
            research_run_manifest=research_run_manifest,
            engine_run_manifest=engine_run_manifest,
            research_intent_batch=research_intent_batch,
            manifest=manifest,
            candidates=candidates_tuple,
        )


_MANIFEST_KEYS = frozenset(
    {
        "codec_schema_version",
        "schema_version",
        "research_run_id",
        "research_request_id",
        "graph_manifest_id",
        "graph_request_id",
        "engine_run_id",
        "engine_request_id",
        "research_intent_batch_id",
        "signal_session",
        "target_session",
        "cutoff",
        "candidate_ids",
        "research_intent_ids",
        "listing_keys",
        "selected_count",
        "blocked_count",
        "source_universe_complete",
        "readiness",
        "paper_only",
        "notification_eligible",
        "execution_eligible",
        "preparation_id",
    }
)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedOperationalPreparationError(_ERR_SHAPE)
        result[key] = value
    return result


def _reject_number(_token: str) -> object:
    raise PromotedOperationalPreparationError(_ERR_SHAPE)


def encode_promoted_operational_preparation_manifest(
    manifest: PromotedOperationalPreparationManifest,
) -> bytes:
    if type(manifest) is not PromotedOperationalPreparationManifest:
        raise TypeError("promoted operational preparation manifest must be exact")
    manifest.verify_content_identity()
    payload = (
        json.dumps(
            {
                "codec_schema_version": (
                    PROMOTED_OPERATIONAL_PREPARATION_MANIFEST_CODEC_VERSION
                ),
                "schema_version": manifest.schema_version,
                "research_run_id": manifest.research_run_id,
                "research_request_id": manifest.research_request_id,
                "graph_manifest_id": manifest.graph_manifest_id,
                "graph_request_id": manifest.graph_request_id,
                "engine_run_id": manifest.engine_run_id,
                "engine_request_id": manifest.engine_request_id,
                "research_intent_batch_id": manifest.research_intent_batch_id,
                "signal_session": manifest.signal_session.isoformat(),
                "target_session": manifest.target_session.isoformat(),
                "cutoff": manifest.cutoff.isoformat(),
                "candidate_ids": list(manifest.candidate_ids),
                "research_intent_ids": list(manifest.research_intent_ids),
                "listing_keys": list(manifest.listing_keys),
                "selected_count": manifest.selected_count,
                "blocked_count": manifest.blocked_count,
                "source_universe_complete": manifest.source_universe_complete,
                "readiness": manifest.readiness.value,
                "paper_only": manifest.paper_only,
                "notification_eligible": manifest.notification_eligible,
                "execution_eligible": manifest.execution_eligible,
                "preparation_id": manifest.preparation_id,
            },
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if not payload or len(payload) > _MAXIMUM_MANIFEST_BYTES:
        raise PromotedOperationalPreparationError(_ERR_BYTES)
    return payload


def decode_promoted_operational_preparation_manifest(
    payload: bytes,
) -> PromotedOperationalPreparationManifest:
    if (
        type(payload) is not bytes
        or not payload
        or len(payload) > _MAXIMUM_MANIFEST_BYTES
    ):
        raise PromotedOperationalPreparationError(_ERR_BYTES)
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise PromotedOperationalPreparationError(_ERR_BYTES) from None
    try:
        decoded = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
    except PromotedOperationalPreparationError:
        raise
    except (json.JSONDecodeError, RecursionError):
        raise PromotedOperationalPreparationError(_ERR_SHAPE) from None
    if type(decoded) is not dict or set(decoded) != _MANIFEST_KEYS:
        raise PromotedOperationalPreparationError(_ERR_SHAPE)
    if (
        decoded["codec_schema_version"]
        != PROMOTED_OPERATIONAL_PREPARATION_MANIFEST_CODEC_VERSION
        or decoded["schema_version"]
        != PROMOTED_OPERATIONAL_PREPARATION_MANIFEST_SCHEMA_VERSION
    ):
        raise PromotedOperationalPreparationError(_ERR_SHAPE)
    id_fields = (
        "research_run_id",
        "research_request_id",
        "graph_manifest_id",
        "graph_request_id",
        "engine_run_id",
        "engine_request_id",
        "research_intent_batch_id",
        "preparation_id",
    )
    ids = {name: _require_sha(decoded[name], _ERR_SHAPE) for name in id_fields}
    tuple_fields = {}
    for name in ("candidate_ids", "research_intent_ids"):
        raw = decoded[name]
        if type(raw) is not list:
            raise PromotedOperationalPreparationError(_ERR_SHAPE)
        tuple_fields[name] = tuple(_require_sha(value, _ERR_SHAPE) for value in raw)
    raw_listing_keys = decoded["listing_keys"]
    if type(raw_listing_keys) is not list:
        raise PromotedOperationalPreparationError(_ERR_SHAPE)
    listing_keys = tuple(raw_listing_keys)
    if any(
        type(value) is not str or _LISTING_KEY.fullmatch(value) is None
        for value in listing_keys
    ):
        raise PromotedOperationalPreparationError(_ERR_SHAPE)
    signal_session = _canonical_date(decoded["signal_session"], _ERR_SHAPE)
    target_session = _canonical_date(decoded["target_session"], _ERR_SHAPE)
    cutoff = _canonical_datetime(decoded["cutoff"], _ERR_SHAPE)
    selected_count = decoded["selected_count"]
    blocked_count = decoded["blocked_count"]
    source_universe_complete = decoded["source_universe_complete"]
    paper_only = decoded["paper_only"]
    notification_eligible = decoded["notification_eligible"]
    execution_eligible = decoded["execution_eligible"]
    if (
        type(selected_count) is not int
        or type(blocked_count) is not int
        or type(source_universe_complete) is not bool
        or type(paper_only) is not bool
        or type(notification_eligible) is not bool
        or type(execution_eligible) is not bool
    ):
        raise PromotedOperationalPreparationError(_ERR_SHAPE)
    try:
        readiness = ReferenceReadiness(decoded["readiness"])
    except ValueError:
        raise PromotedOperationalPreparationError(_ERR_SHAPE) from None
    try:
        manifest = PromotedOperationalPreparationManifest(
            schema_version=decoded["schema_version"],
            research_run_id=ids["research_run_id"],
            research_request_id=ids["research_request_id"],
            graph_manifest_id=ids["graph_manifest_id"],
            graph_request_id=ids["graph_request_id"],
            engine_run_id=ids["engine_run_id"],
            engine_request_id=ids["engine_request_id"],
            research_intent_batch_id=ids["research_intent_batch_id"],
            signal_session=signal_session,
            target_session=target_session,
            cutoff=cutoff,
            candidate_ids=tuple_fields["candidate_ids"],
            research_intent_ids=tuple_fields["research_intent_ids"],
            listing_keys=listing_keys,
            selected_count=selected_count,
            blocked_count=blocked_count,
            source_universe_complete=source_universe_complete,
            readiness=readiness,
            paper_only=paper_only,
            notification_eligible=notification_eligible,
            execution_eligible=execution_eligible,
        )
    except PromotedOperationalPreparationError:
        raise PromotedOperationalPreparationError(_ERR_SHAPE) from None
    if manifest.preparation_id != ids["preparation_id"]:
        raise PromotedOperationalPreparationError(_ERR_SHAPE)
    if encode_promoted_operational_preparation_manifest(manifest) != payload:
        raise PromotedOperationalPreparationError(_ERR_SHAPE)
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


class LocalPromotedOperationalPreparationStore:
    """Durable, create-once root store for VerifiedPromotedOperationalPreparation.

    Exposes only ``put``, ``get``, and ``path_for`` -- no list/latest/
    nearest/find/discovery operation. ``get`` never trusts the stored
    manifest as authority: it strictly decodes it, then independently
    resolves the exact research run, derives and resolves its exact engine
    run, derives and resolves that run's exact research-intent batch, reruns
    the preparation service, and requires the reconstructed canonical
    manifest bytes and preparation_id to match the stored artifact exactly.
    """

    _DIRECTORY = "promoted-operational-preparations"
    _LOCK_FILENAME = ".promoted-operational-preparation.lock"

    def __init__(
        self,
        root: Path,
        *,
        research_runs: LocalPromotedResearchRunStore,
        engine_runs: LocalPromotedEngineRunStore,
        research_intents: LocalPromotedResearchIntentStore,
        replay_scope: ExactReplayScope,
    ) -> None:
        self.root = Path(root) / self._DIRECTORY
        self.research_runs = research_runs
        self.engine_runs = engine_runs
        self.research_intents = research_intents
        self._replay_scope = replay_scope

    def path_for(self, preparation_id: str) -> Path:
        return self.root / f"{_require_sha(preparation_id, _ERR_ID)}.json"

    def put(
        self, preparation: VerifiedPromotedOperationalPreparation
    ) -> VerifiedPromotedOperationalPreparation:
        if type(preparation) is not VerifiedPromotedOperationalPreparation:
            raise TypeError(
                "promoted operational preparation must be exact"
            )
        preparation.verify_content_identity()
        reconstructed = self._verify_downstream(preparation.manifest)
        if reconstructed != preparation:
            raise PromotedOperationalPreparationError(_ERR_VERIFY)
        payload = encode_promoted_operational_preparation_manifest(
            preparation.manifest
        )
        try:
            replayed = decode_promoted_operational_preparation_manifest(payload)
        except PromotedOperationalPreparationError:
            raise
        except Exception:
            raise PromotedOperationalPreparationError(_ERR_VERIFY) from None
        if (
            replayed != preparation.manifest
            or replayed.preparation_id != preparation.manifest.preparation_id
        ):
            raise PromotedOperationalPreparationError(_ERR_VERIFY)
        self._publish(preparation.manifest.preparation_id, payload)
        return self.get(preparation.manifest.preparation_id)

    def get(
        self, preparation_id: str
    ) -> VerifiedPromotedOperationalPreparation:
        with self._replay_scope.open():
            return self._get(preparation_id)

    def _get(
        self, preparation_id: str
    ) -> VerifiedPromotedOperationalPreparation:
        path = self.path_for(preparation_id)
        payload = self._read(path)
        manifest = decode_promoted_operational_preparation_manifest(payload)
        if manifest.preparation_id != preparation_id:
            raise PromotedOperationalPreparationError(_ERR_SHAPE)
        return self._verify_downstream(manifest)

    def _verify_downstream(
        self, manifest: PromotedOperationalPreparationManifest
    ) -> VerifiedPromotedOperationalPreparation:
        try:
            research_run_manifest = self.research_runs.get(manifest.research_run_id)
        except Exception:
            raise PromotedOperationalPreparationConflict(_ERR_SOURCE) from None
        try:
            engine_run_manifest = self.engine_runs.get(
                research_run_manifest.engine_run_id
            )
        except Exception:
            raise PromotedOperationalPreparationConflict(_ERR_SOURCE) from None
        try:
            research_intent_batch = self.research_intents.get(
                engine_run_manifest.research_intent_batch_id
            )
        except Exception:
            raise PromotedOperationalPreparationConflict(_ERR_SOURCE) from None
        try:
            preparation = PromotedOperationalPreparationService().prepare(
                research_run_manifest=research_run_manifest,
                engine_run_manifest=engine_run_manifest,
                research_intent_batch=research_intent_batch,
            )
        except PromotedOperationalPreparationError:
            raise
        except Exception:
            raise PromotedOperationalPreparationConflict(_ERR_VERIFY) from None
        if (
            preparation.manifest.preparation_id != manifest.preparation_id
            or encode_promoted_operational_preparation_manifest(preparation.manifest)
            != encode_promoted_operational_preparation_manifest(manifest)
        ):
            raise PromotedOperationalPreparationConflict(_ERR_LINEAGE_MISMATCH)
        return preparation

    def _read(self, path: Path) -> bytes:
        if not path.exists():
            raise PromotedOperationalPreparationNotFound(_ERR_NOT_FOUND)
        if not path.is_file() or _is_link_like(path):
            raise PromotedOperationalPreparationError(_ERR_UNSAFE_PATH)
        try:
            return read_stable_regular_file(
                path, maximum_bytes=_MAXIMUM_MANIFEST_BYTES
            )
        except FileSafetyError:
            raise PromotedOperationalPreparationError(_ERR_UNSAFE_PATH) from None

    def _publish(self, preparation_id: str, payload: bytes) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        if not self.root.is_dir() or _is_link_like(self.root):
            raise PromotedOperationalPreparationError(_ERR_UNSAFE_PATH)
        target = self.path_for(preparation_id)
        try:
            with advisory_file_lock(self.root / self._LOCK_FILENAME):
                if target.exists():
                    if _is_link_like(target) or self._read(target) != payload:
                        raise PromotedOperationalPreparationConflict(_ERR_CONFLICT)
                    return
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".promoted-operational-preparation-",
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
        except PromotedOperationalPreparationConflict:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise PromotedOperationalPreparationConflict(_ERR_CONFLICT) from None


def build_promoted_operational_preparation_store(
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
    operational_preparation_root: Path,
):
    """Construct every real durable store from ten explicit roots.

    Reuses ``build_promoted_research_stores`` unchanged (including its one
    already-constructed shared ``ExactReplayScope``) and adds only the new
    preparation store on top -- it never constructs a second promoted graph
    or a second, unrelated replay scope.

    Returns the underlying ``PromotedResearchStores`` object plus the new
    ``LocalPromotedOperationalPreparationStore`` so a caller can resolve
    research runs directly without a second, separate composition.
    """

    research_stores = build_promoted_research_stores(
        reference_root=reference_root,
        identity_evidence_root=identity_evidence_root,
        calendar_root=calendar_root,
        daily_reports_root=daily_reports_root,
        historical_corpus_root=historical_corpus_root,
        promoted_root=promoted_root,
        graph_publication_root=graph_publication_root,
        engine_run_root=engine_run_root,
        research_run_root=research_run_root,
    )
    preparations = LocalPromotedOperationalPreparationStore(
        operational_preparation_root,
        research_runs=research_stores.research_runs,
        engine_runs=research_stores.engine.engine_runs,
        research_intents=research_stores.engine.research_intents,
        replay_scope=research_stores._replay_scope,
    )
    return research_stores, preparations


def prepare_and_publish(
    research_run_id: str,
    research_stores,
    preparations: LocalPromotedOperationalPreparationStore,
) -> VerifiedPromotedOperationalPreparation:
    """Resolve one exact research run, derive and resolve its exact engine
    run and research-intent batch, prepare the operational candidates, and
    durably publish the result.

    Never discovers a different run: `research_run_id` is the only caller
    input, and every other root is derived strictly from what the resolved
    research run itself retains.
    """

    with research_stores._replay_scope.open():
        research_run_manifest = research_stores.research_runs.get(research_run_id)
        engine_run_manifest = research_stores.engine.engine_runs.get(
            research_run_manifest.engine_run_id
        )
        research_intent_batch = research_stores.engine.research_intents.get(
            engine_run_manifest.research_intent_batch_id
        )
        preparation = PromotedOperationalPreparationService().prepare(
            research_run_manifest=research_run_manifest,
            engine_run_manifest=engine_run_manifest,
            research_intent_batch=research_intent_batch,
        )
        preparation_id = preparations.put(preparation).manifest.preparation_id
    # One final cold get, entirely outside the scope just closed: this
    # proves the published preparation independently re-verifies its whole
    # research/engine/intent lineage from scratch rather than inheriting
    # trust from the construction above.
    return preparations.get(preparation_id)
