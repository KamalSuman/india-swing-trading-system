from __future__ import annotations

import re
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timezone

from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness
from india_swing.reference.universe import UniverseDisposition, UniverseSnapshot

from .input_assembly import SwingInputAssembly


UNIVERSE_BATCH_SCHEMA_VERSION = "swing-universe-input-batch/v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class SwingUniverseBatchError(ValueError):
    pass


def _sha(value: str, name: str) -> None:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise SwingUniverseBatchError(f"{name} must be a lowercase SHA-256")


def _utc(value: datetime, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None:
        raise SwingUniverseBatchError(f"{name} must be timezone-aware")
    try:
        offset = value.utcoffset()
    except Exception:
        raise SwingUniverseBatchError(f"{name} has invalid timezone behavior") from None
    if offset is None:
        raise SwingUniverseBatchError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _subject_key(instrument_id: str, listing_id: str) -> tuple[str, str]:
    return (instrument_id, listing_id)


@dataclass(frozen=True, slots=True)
class SwingUniverseVeto:
    """An immutable, content-addressed veto derived from one non-actionable universe entry."""

    stable_instrument_id: str
    stable_listing_id: str
    tradingsymbol: str
    disposition: UniverseDisposition
    reason_codes: tuple[str, ...]
    veto_id: str = field(init=False)

    def __post_init__(self) -> None:
        for value, name in (
            (self.stable_instrument_id, "stable_instrument_id"),
            (self.stable_listing_id, "stable_listing_id"),
        ):
            _sha(value, name)
        if (
            type(self.tradingsymbol) is not str
            or not self.tradingsymbol
            or self.tradingsymbol != self.tradingsymbol.strip().upper()
        ):
            raise SwingUniverseBatchError("veto tradingsymbol must be normalized uppercase text")
        if type(self.disposition) is not UniverseDisposition:
            raise SwingUniverseBatchError("veto disposition must be exact")
        if self.disposition is UniverseDisposition.ACTIONABLE:
            raise SwingUniverseBatchError("actionable entries cannot be vetoed")
        if (
            type(self.reason_codes) is not tuple
            or not self.reason_codes
            or self.reason_codes != tuple(sorted(set(self.reason_codes)))
        ):
            raise SwingUniverseBatchError(
                "veto reason codes must be a non-empty sorted unique tuple"
            )
        object.__setattr__(self, "veto_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "veto_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.veto_id != self._calculated_id():
            raise SwingUniverseBatchError("universe veto content identity failed")


@dataclass(frozen=True, slots=True)
class SwingUniverseInputBatch:
    """Exact full-universe coverage: one assembly per ACTIONABLE entry, one veto per the rest."""

    current_universe: UniverseSnapshot
    universe_snapshot_id: str
    signal_session: date
    cutoff: datetime
    readiness: ReferenceReadiness
    assemblies: tuple[SwingInputAssembly, ...]
    vetoes: tuple[SwingUniverseVeto, ...]
    scoped_subject_count: int
    actionable_subject_count: int
    veto_subject_count: int
    schema_version: str = UNIVERSE_BATCH_SCHEMA_VERSION
    batch_id: str = field(init=False)

    def __post_init__(self) -> None:
        _sha(self.universe_snapshot_id, "universe_snapshot_id")
        if type(self.current_universe) is not UniverseSnapshot:
            raise SwingUniverseBatchError("current_universe must be exact")
        if type(self.signal_session) is not date:
            raise SwingUniverseBatchError("signal_session must be a date")
        object.__setattr__(self, "cutoff", _utc(self.cutoff, "batch cutoff"))
        if type(self.readiness) is not ReferenceReadiness:
            raise SwingUniverseBatchError("batch readiness must be exact")
        if self.readiness is ReferenceReadiness.COLLECTION_ONLY:
            raise SwingUniverseBatchError("collection-only universe cannot enter the batch engine")
        if type(self.assemblies) is not tuple or any(
            type(value) is not SwingInputAssembly for value in self.assemblies
        ):
            raise SwingUniverseBatchError("assemblies must be an exact tuple")
        if type(self.vetoes) is not tuple or any(
            type(value) is not SwingUniverseVeto for value in self.vetoes
        ):
            raise SwingUniverseBatchError("vetoes must be an exact tuple")
        for value in (
            self.scoped_subject_count,
            self.actionable_subject_count,
            self.veto_subject_count,
        ):
            if type(value) is not int or value < 0:
                raise SwingUniverseBatchError("batch subject counts must be non-negative")
        if self.schema_version != UNIVERSE_BATCH_SCHEMA_VERSION:
            raise SwingUniverseBatchError("unsupported swing universe input batch schema")
        self._verify_coverage()
        object.__setattr__(self, "batch_id", self._calculated_id())

    def _verify_coverage(self) -> None:
        universe = self.current_universe
        universe.verify_content_identity()
        if universe.snapshot_id != self.universe_snapshot_id:
            raise SwingUniverseBatchError("universe snapshot ID does not match the batch lineage")
        if universe.market_session != self.signal_session:
            raise SwingUniverseBatchError("signal session does not match the current universe")
        if universe.cutoff != self.cutoff:
            raise SwingUniverseBatchError("cutoff does not match the current universe")
        if universe.readiness is not self.readiness:
            raise SwingUniverseBatchError("readiness does not match the current universe")

        actionable_entries: dict[tuple[str, str], object] = {}
        veto_entries: dict[tuple[str, str], object] = {}
        for entry in universe.entries:
            key = _subject_key(entry.listing.instrument_id, entry.listing.listing_id)
            if key in actionable_entries or key in veto_entries:
                raise SwingUniverseBatchError("current universe contains a duplicate subject key")
            if entry.disposition is UniverseDisposition.ACTIONABLE:
                actionable_entries[key] = entry
            else:
                veto_entries[key] = entry
        expected_actionable_keys = tuple(sorted(actionable_entries))
        expected_veto_keys = tuple(sorted(veto_entries))

        for value in self.assemblies:
            value.verify_content_identity()
        assembly_keys = tuple(
            _subject_key(value.stable_instrument_id, value.stable_listing_id)
            for value in self.assemblies
        )
        if len(set(assembly_keys)) != len(assembly_keys):
            raise SwingUniverseBatchError("assemblies contain a duplicate subject")
        if assembly_keys != tuple(sorted(assembly_keys)):
            raise SwingUniverseBatchError("assemblies must be canonically ordered by subject")
        if assembly_keys != expected_actionable_keys:
            raise SwingUniverseBatchError(
                "assembled subjects do not exactly cover the actionable universe"
            )

        anchor = self.assemblies[0] if self.assemblies else None
        for value in self.assemblies:
            entry = actionable_entries[
                _subject_key(value.stable_instrument_id, value.stable_listing_id)
            ]
            if (
                value.signal_session != self.signal_session
                or value.cutoff != self.cutoff
                or value.readiness is not self.readiness
                or not value.universe_snapshot_ids
                or value.universe_snapshot_ids[-1] != self.universe_snapshot_id
            ):
                raise SwingUniverseBatchError("assembly lineage differs from the current universe")
            if not entry.listing.is_valid_on(value.signal_session):
                raise SwingUniverseBatchError("current listing is not valid on the signal session")
            if anchor is not None and (
                value.stable_identity_snapshot_id != anchor.stable_identity_snapshot_id
                or value.raw_artifact_ids != anchor.raw_artifact_ids
                or value.universe_snapshot_ids != anchor.universe_snapshot_ids
                or value.adjusted_history.corporate_action_snapshot_id
                != anchor.adjusted_history.corporate_action_snapshot_id
            ):
                raise SwingUniverseBatchError("assemblies do not share exact engine lineage")

        for value in self.vetoes:
            value.verify_content_identity()
        veto_keys = tuple(
            _subject_key(value.stable_instrument_id, value.stable_listing_id)
            for value in self.vetoes
        )
        if len(set(veto_keys)) != len(veto_keys):
            raise SwingUniverseBatchError("vetoes contain a duplicate subject")
        if veto_keys != tuple(sorted(veto_keys)):
            raise SwingUniverseBatchError("vetoes must be canonically ordered by subject")
        if veto_keys != expected_veto_keys:
            raise SwingUniverseBatchError(
                "derived vetoes do not exactly cover the non-actionable universe"
            )
        for value in self.vetoes:
            entry = veto_entries[
                _subject_key(value.stable_instrument_id, value.stable_listing_id)
            ]
            if (
                value.disposition is not entry.disposition
                or value.reason_codes != entry.reason_codes
                or value.tradingsymbol != entry.listing.tradingsymbol
            ):
                raise SwingUniverseBatchError("veto content differs from the current universe entry")

        if (
            self.scoped_subject_count != len(universe.entries)
            or self.actionable_subject_count != len(self.assemblies)
            or self.veto_subject_count != len(self.vetoes)
            or self.scoped_subject_count
            != self.actionable_subject_count + self.veto_subject_count
        ):
            raise SwingUniverseBatchError("batch subject counts are inconsistent")

    @property
    def actionable(self) -> bool:
        return self.readiness is ReferenceReadiness.POINT_IN_TIME_VERIFIED and all(
            value.actionable for value in self.assemblies
        )

    def _calculated_id(self) -> str:
        return content_id(
            {
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "batch_id"
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._verify_coverage()
        if self.batch_id != self._calculated_id():
            raise SwingUniverseBatchError("universe input batch content identity failed")


def assemble_universe_input_batch(
    *,
    current_universe: UniverseSnapshot,
    assemblies: tuple[SwingInputAssembly, ...],
) -> SwingUniverseInputBatch:
    """Bind an exact current-universe snapshot to full actionable-subject coverage.

    Every currently ACTIONABLE listing must have exactly one already-created
    SwingInputAssembly; every other listing receives a derived veto. This
    function does not create assemblies -- it only verifies and packages
    coverage that already exists.
    """

    if type(current_universe) is not UniverseSnapshot:
        raise SwingUniverseBatchError("current_universe must be exact")
    current_universe.verify_content_identity()
    if current_universe.readiness is ReferenceReadiness.COLLECTION_ONLY:
        raise SwingUniverseBatchError("collection-only universe cannot enter the batch engine")
    if type(assemblies) is not tuple or any(
        type(value) is not SwingInputAssembly for value in assemblies
    ):
        raise SwingUniverseBatchError("assemblies must be an exact tuple")

    vetoes = tuple(
        SwingUniverseVeto(
            stable_instrument_id=entry.listing.instrument_id,
            stable_listing_id=entry.listing.listing_id,
            tradingsymbol=entry.listing.tradingsymbol,
            disposition=entry.disposition,
            reason_codes=entry.reason_codes,
        )
        for entry in sorted(
            (
                value
                for value in current_universe.entries
                if value.disposition is not UniverseDisposition.ACTIONABLE
            ),
            key=lambda value: _subject_key(
                value.listing.instrument_id, value.listing.listing_id
            ),
        )
    )

    return SwingUniverseInputBatch(
        current_universe=current_universe,
        universe_snapshot_id=current_universe.snapshot_id,
        signal_session=current_universe.market_session,
        cutoff=current_universe.cutoff,
        readiness=current_universe.readiness,
        assemblies=assemblies,
        vetoes=vetoes,
        scoped_subject_count=len(current_universe.entries),
        actionable_subject_count=len(assemblies),
        veto_subject_count=len(vetoes),
    )
