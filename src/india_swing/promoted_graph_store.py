"""Create-once replay manifests for the promoted research source graph.

The manifests deliberately retain only exact content identities and explicit
replay parameters.  They never contain a serialized object that can be trusted
as authority: every read resolves the pinned sources, reruns the owning
materializer, and compares both the output ID and the canonical manifest.
"""

from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Generic, Protocol, TypeVar

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.calendar_data.materialization import (
    CollectionCalendarMaterialization,
)
from india_swing.corporate_actions.models import CorporateActionSnapshot
from india_swing.corporate_actions.promoted_adjustments import (
    PromotedCorporateActionAdjustmentService,
    VerifiedPromotedCorporateActionAdjustmentPanel,
)
from india_swing.historical_prices.promoted_history import (
    PromotedStableListingHistoryService,
    VerifiedPromotedStableListingHistoryPanel,
)
from india_swing.identity_decisions.models import StoredIdentityReviewBundle
from india_swing.identity_decisions.promoted_materialize import (
    PromotedIdentityAdjudicationService,
    VerifiedPromotedIdentityAdjudication,
)
from india_swing.identity_evidence.models import StoredIdentityEvidenceArtifact
from india_swing.identity_registry.promoted_intake import (
    PromotedIdentityIntakeService,
    VerifiedPromotedIdentityIntake,
)
from india_swing.market_data.historical_corpus import (
    HistoricalEvaluationCorpusIndex,
    HistoricalEvaluationCorpusSessionPartition,
)
from india_swing.market_data.promoted_session_frame import (
    PromotedSessionMarketDataFrameService,
    VerifiedPromotedSessionMarketDataFrame,
)
from india_swing.reference_data.acquisition_promotion import (
    VerifiedReferenceArtifactPromotion,
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


PROMOTED_GRAPH_MANIFEST_CODEC_VERSION = "promoted-graph-replay-manifest-json/v1"
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MAXIMUM_MANIFEST_BYTES = 4 * 1024 * 1024
_MAXIMUM_IDS_PER_GROUP = 100_000
_MAXIMUM_DATES = 100_000

_IDENTITY_INTAKE = "PROMOTED_IDENTITY_INTAKE"
_IDENTITY_ADJUDICATION = "PROMOTED_IDENTITY_ADJUDICATION"
_IDENTITY_SESSION_UNIVERSE = "PROMOTED_IDENTITY_SESSION_UNIVERSE"
_SESSION_MARKET_DATA_FRAME = "PROMOTED_SESSION_MARKET_DATA_FRAME"
_SESSION_TICK_SNAPSHOT = "PROMOTED_SESSION_TICK_SNAPSHOT"
_STABLE_LISTING_HISTORY = "PROMOTED_STABLE_LISTING_HISTORY"
_CORPORATE_ACTION_ADJUSTMENT = "PROMOTED_CORPORATE_ACTION_ADJUSTMENT"
_EFFECTIVE_SESSION_TICK = "PROMOTED_EFFECTIVE_SESSION_TICK"
_KINDS = frozenset(
    {
        _IDENTITY_INTAKE,
        _IDENTITY_ADJUDICATION,
        _IDENTITY_SESSION_UNIVERSE,
        _SESSION_MARKET_DATA_FRAME,
        _SESSION_TICK_SNAPSHOT,
        _STABLE_LISTING_HISTORY,
        _CORPORATE_ACTION_ADJUSTMENT,
        _EFFECTIVE_SESSION_TICK,
    }
)


class PromotedGraphStoreError(ValueError):
    pass


class PromotedGraphStoreConflict(PromotedGraphStoreError):
    pass


class PromotedGraphStoreNotFound(PromotedGraphStoreError):
    pass


@dataclass(frozen=True, slots=True)
class PromotedGraphReplayRecord:
    kind: str
    artifact_id: str
    primary_ids: tuple[str, ...]
    secondary_ids: tuple[str, ...]
    tertiary_ids: tuple[str, ...]
    dates: tuple[date, ...]
    cutoff: datetime


class ReferencePromotionResolver(Protocol):
    def get(self, promotion_id: str) -> VerifiedReferenceArtifactPromotion: ...


class IdentityIntakeResolver(Protocol):
    def get(self, intake_id: str) -> VerifiedPromotedIdentityIntake: ...


class IdentityEvidenceResolver(Protocol):
    def get(self, artifact_id: str) -> StoredIdentityEvidenceArtifact: ...


class IdentityReviewResolver(Protocol):
    def get(self, bundle_id: str) -> StoredIdentityReviewBundle: ...


class IdentityAdjudicationResolver(Protocol):
    def get(
        self, adjudication_id: str
    ) -> VerifiedPromotedIdentityAdjudication: ...


class CalendarMaterializationResolver(Protocol):
    def get(
        self, materialization_id: str
    ) -> CollectionCalendarMaterialization: ...


class IdentitySessionUniverseResolver(Protocol):
    def get(
        self, universe_id: str
    ) -> VerifiedPromotedIdentitySessionUniverse: ...


class HistoricalCorpusResolver(Protocol):
    def get(
        self, corpus_id: str
    ) -> tuple[
        HistoricalEvaluationCorpusIndex,
        tuple[HistoricalEvaluationCorpusSessionPartition, ...],
    ]: ...


class SessionMarketDataFrameResolver(Protocol):
    def get(
        self, frame_id: str
    ) -> VerifiedPromotedSessionMarketDataFrame: ...


class SessionTickSnapshotResolver(Protocol):
    def get(
        self, snapshot_id: str
    ) -> VerifiedPromotedSessionTickSnapshot: ...


class StableListingHistoryResolver(Protocol):
    def get(
        self, panel_id: str
    ) -> VerifiedPromotedStableListingHistoryPanel: ...


class CorporateActionSnapshotResolver(Protocol):
    def get(self, snapshot_id: str) -> CorporateActionSnapshot: ...


def _canonical(value: dict[str, object]) -> bytes:
    return (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _sha(value: object) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise PromotedGraphStoreError("promoted graph identity is invalid")
    return value


def _aware_utc(value: object) -> datetime:
    if type(value) is not str:
        raise PromotedGraphStoreError("promoted graph cutoff is invalid")
    try:
        result = datetime.fromisoformat(value)
        offset = result.utcoffset()
    except Exception:
        raise PromotedGraphStoreError(
            "promoted graph cutoff is invalid"
        ) from None
    if (
        result.tzinfo is None
        or offset is None
        or offset.total_seconds() != 0
        or result.isoformat() != value
    ):
        raise PromotedGraphStoreError("promoted graph cutoff is invalid")
    return result


def _date(value: object) -> date:
    if type(value) is not str:
        raise PromotedGraphStoreError("promoted graph date is invalid")
    try:
        result = date.fromisoformat(value)
    except Exception:
        raise PromotedGraphStoreError(
            "promoted graph date is invalid"
        ) from None
    if result.isoformat() != value:
        raise PromotedGraphStoreError("promoted graph date is invalid")
    return result


def _ids(value: object) -> tuple[str, ...]:
    if type(value) is not list or len(value) > _MAXIMUM_IDS_PER_GROUP:
        raise PromotedGraphStoreError("promoted graph sources are invalid")
    result = tuple(_sha(item) for item in value)
    if len(set(result)) != len(result):
        raise PromotedGraphStoreError("promoted graph sources are invalid")
    return result


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedGraphStoreError(
                "promoted graph manifest contains duplicate keys"
            )
        result[key] = value
    return result


def _reject_number(_: str) -> object:
    raise PromotedGraphStoreError(
        "promoted graph manifest contains a forbidden number"
    )


def encode_promoted_graph_record(value: PromotedGraphReplayRecord) -> bytes:
    if type(value) is not PromotedGraphReplayRecord:
        raise TypeError("promoted graph record must be exact")
    if value.kind not in _KINDS:
        raise PromotedGraphStoreError("promoted graph kind is invalid")
    _sha(value.artifact_id)
    for values in (
        value.primary_ids,
        value.secondary_ids,
        value.tertiary_ids,
    ):
        if type(values) is not tuple:
            raise PromotedGraphStoreError(
                "promoted graph sources are invalid"
            )
        _ids(list(values))
    if (
        type(value.dates) is not tuple
        or len(value.dates) > _MAXIMUM_DATES
        or any(type(item) is not date for item in value.dates)
        or len(set(value.dates)) != len(value.dates)
    ):
        raise PromotedGraphStoreError("promoted graph dates are invalid")
    if type(value.cutoff) is not datetime:
        raise PromotedGraphStoreError("promoted graph cutoff is invalid")
    cutoff = _aware_utc(value.cutoff.isoformat())
    if cutoff != value.cutoff:
        raise PromotedGraphStoreError("promoted graph cutoff is invalid")
    return _canonical(
        {
            "codec_schema_version": PROMOTED_GRAPH_MANIFEST_CODEC_VERSION,
            "kind": value.kind,
            "artifact_id": value.artifact_id,
            "primary_ids": value.primary_ids,
            "secondary_ids": value.secondary_ids,
            "tertiary_ids": value.tertiary_ids,
            "dates": tuple(item.isoformat() for item in value.dates),
            "cutoff": value.cutoff.isoformat(),
        }
    )


def decode_promoted_graph_record(
    payload: bytes, *, expected_kind: str
) -> PromotedGraphReplayRecord:
    if (
        type(payload) is not bytes
        or not payload
        or len(payload) > _MAXIMUM_MANIFEST_BYTES
    ):
        raise PromotedGraphStoreError(
            "promoted graph manifest bytes are invalid"
        )
    try:
        decoded = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
        expected = {
            "codec_schema_version",
            "kind",
            "artifact_id",
            "primary_ids",
            "secondary_ids",
            "tertiary_ids",
            "dates",
            "cutoff",
        }
        if type(decoded) is not dict or set(decoded) != expected:
            raise PromotedGraphStoreError(
                "promoted graph manifest fields are invalid"
            )
        if (
            decoded["codec_schema_version"]
            != PROMOTED_GRAPH_MANIFEST_CODEC_VERSION
            or decoded["kind"] != expected_kind
        ):
            raise PromotedGraphStoreError(
                "promoted graph manifest kind is invalid"
            )
        raw_dates = decoded["dates"]
        if type(raw_dates) is not list or len(raw_dates) > _MAXIMUM_DATES:
            raise PromotedGraphStoreError(
                "promoted graph dates are invalid"
            )
        dates = tuple(_date(item) for item in raw_dates)
        if len(set(dates)) != len(dates):
            raise PromotedGraphStoreError(
                "promoted graph dates are invalid"
            )
        result = PromotedGraphReplayRecord(
            kind=expected_kind,
            artifact_id=_sha(decoded["artifact_id"]),
            primary_ids=_ids(decoded["primary_ids"]),
            secondary_ids=_ids(decoded["secondary_ids"]),
            tertiary_ids=_ids(decoded["tertiary_ids"]),
            dates=dates,
            cutoff=_aware_utc(decoded["cutoff"]),
        )
        if encode_promoted_graph_record(result) != payload:
            raise PromotedGraphStoreError(
                "promoted graph manifest is not canonical"
            )
        return result
    except PromotedGraphStoreError:
        raise
    except Exception:
        raise PromotedGraphStoreError(
            "promoted graph manifest is invalid"
        ) from None


def _is_link_like(path: Path) -> bool:
    try:
        status = os.lstat(path)
    except OSError:
        return path.is_symlink()
    return path.is_symlink() or bool(
        getattr(status, "st_file_attributes", 0)
        & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    )


def _path(root: Path, artifact_id: str) -> Path:
    return root / f"{_sha(artifact_id)}.json"


def _read(root: Path, artifact_id: str) -> bytes:
    path = _path(root, artifact_id)
    if not path.exists():
        raise PromotedGraphStoreNotFound(
            "promoted graph artifact was not found"
        )
    if not path.is_file() or _is_link_like(path):
        raise PromotedGraphStoreError(
            "promoted graph artifact path is unsafe"
        )
    try:
        return read_stable_regular_file(
            path, maximum_bytes=_MAXIMUM_MANIFEST_BYTES
        )
    except FileSafetyError:
        raise PromotedGraphStoreError(
            "promoted graph artifact read was unsafe"
        ) from None


def _put(root: Path, artifact_id: str, payload: bytes) -> None:
    root.mkdir(parents=True, exist_ok=True)
    if not root.is_dir() or _is_link_like(root):
        raise PromotedGraphStoreError(
            "promoted graph artifact root is unsafe"
        )
    target = _path(root, artifact_id)
    try:
        with advisory_file_lock(root / ".promoted-graph.lock"):
            if target.exists():
                if _read(root, artifact_id) != payload:
                    raise PromotedGraphStoreConflict(
                        "promoted graph identity stores different content"
                    )
                return
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=".promoted-graph-",
                suffix=".tmp",
                dir=root,
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
    except PromotedGraphStoreConflict:
        raise
    except (FileLockUnavailable, FileSafetyError, OSError):
        raise PromotedGraphStoreConflict(
            "promoted graph artifact store is unavailable"
        ) from None


T = TypeVar("T")


class _LocalReplayStore(Generic[T]):
    def __init__(
        self,
        *,
        root: Path,
        directory: str,
        kind: str,
        exact_type: type[T],
        identity: Callable[[T], str],
        record: Callable[[T], PromotedGraphReplayRecord],
        replay: Callable[[PromotedGraphReplayRecord], T],
    ) -> None:
        self.root = Path(root) / directory
        self.kind = kind
        self.exact_type = exact_type
        self.identity = identity
        self.record = record
        self.replay = replay

    def path_for(self, artifact_id: str) -> Path:
        return _path(self.root, artifact_id)

    def put(self, value: T) -> T:
        if type(value) is not self.exact_type:
            raise TypeError("promoted graph artifact must be exact")
        self._verify(value)
        record = self.record(value)
        if record.kind != self.kind:
            raise PromotedGraphStoreError(
                "promoted graph record kind differs"
            )
        replayed = self._replay(record)
        if replayed != value:
            raise PromotedGraphStoreError(
                "promoted graph source replay differs"
            )
        payload = encode_promoted_graph_record(record)
        _put(self.root, record.artifact_id, payload)
        return self.get(record.artifact_id)

    def get(self, artifact_id: str) -> T:
        payload = _read(self.root, artifact_id)
        try:
            record = decode_promoted_graph_record(
                payload, expected_kind=self.kind
            )
            if record.artifact_id != artifact_id:
                raise PromotedGraphStoreError(
                    "promoted graph path identity differs"
                )
            replayed = self._replay(record)
            if (
                self.identity(replayed) != artifact_id
                or self.record(replayed) != record
                or encode_promoted_graph_record(
                    self.record(replayed)
                )
                != payload
            ):
                raise PromotedGraphStoreError(
                    "promoted graph replay differs"
                )
            return replayed
        except PromotedGraphStoreError:
            raise
        except Exception:
            raise PromotedGraphStoreError(
                "stored promoted graph artifact is invalid"
            ) from None

    def _verify(self, value: T) -> None:
        try:
            verifier = getattr(value, "verify_content_identity")
            verifier()
            _sha(self.identity(value))
        except Exception:
            raise PromotedGraphStoreError(
                "promoted graph artifact identity is invalid"
            ) from None

    def _replay(self, record: PromotedGraphReplayRecord) -> T:
        try:
            result = self.replay(record)
            if type(result) is not self.exact_type:
                raise PromotedGraphStoreError(
                    "promoted graph replay type differs"
                )
            self._verify(result)
            return result
        except PromotedGraphStoreError:
            raise
        except Exception:
            raise PromotedGraphStoreError(
                "promoted graph source replay failed"
            ) from None


def _require_shape(
    record: PromotedGraphReplayRecord,
    *,
    primary: int | None,
    secondary: int | None,
    tertiary: int | None,
    dates: int | None,
) -> None:
    checks = (
        (len(record.primary_ids), primary),
        (len(record.secondary_ids), secondary),
        (len(record.tertiary_ids), tertiary),
        (len(record.dates), dates),
    )
    if any(
        expected is not None and actual != expected
        for actual, expected in checks
    ):
        raise PromotedGraphStoreError(
            "promoted graph source shape is invalid"
        )


class LocalPromotedIdentityIntakeStore:
    def __init__(self, root: Path, promotions: ReferencePromotionResolver) -> None:
        self.promotions = promotions
        self._store = _LocalReplayStore[
            VerifiedPromotedIdentityIntake
        ](
            root=root,
            directory="identity-intakes",
            kind=_IDENTITY_INTAKE,
            exact_type=VerifiedPromotedIdentityIntake,
            identity=lambda value: value.intake_id,
            record=self._record,
            replay=self._replay,
        )

    def path_for(self, intake_id: str) -> Path:
        return self._store.path_for(intake_id)

    def put(
        self, value: VerifiedPromotedIdentityIntake
    ) -> VerifiedPromotedIdentityIntake:
        return self._store.put(value)

    def get(self, intake_id: str) -> VerifiedPromotedIdentityIntake:
        return self._store.get(intake_id)

    @staticmethod
    def _record(
        value: VerifiedPromotedIdentityIntake,
    ) -> PromotedGraphReplayRecord:
        return PromotedGraphReplayRecord(
            kind=_IDENTITY_INTAKE,
            artifact_id=value.intake_id,
            primary_ids=tuple(item.promotion_id for item in value.promotions),
            secondary_ids=(),
            tertiary_ids=(),
            dates=value.expected_report_dates,
            cutoff=value.cutoff,
        )

    def _replay(
        self, record: PromotedGraphReplayRecord
    ) -> VerifiedPromotedIdentityIntake:
        _require_shape(
            record,
            primary=None,
            secondary=0,
            tertiary=0,
            dates=None,
        )
        if not record.primary_ids or not record.dates:
            raise PromotedGraphStoreError(
                "promoted identity intake sources are incomplete"
            )
        promotions = tuple(
            self.promotions.get(item) for item in record.primary_ids
        )
        if any(
            type(value) is not VerifiedReferenceArtifactPromotion
            or value.promotion_id != identity
            for value, identity in zip(promotions, record.primary_ids)
        ):
            raise PromotedGraphStoreError(
                "promoted identity intake source differs"
            )
        return PromotedIdentityIntakeService().materialize(
            promotions=promotions,
            expected_report_dates=record.dates,
            cutoff=record.cutoff,
        )


class LocalPromotedIdentityAdjudicationStore:
    def __init__(
        self,
        root: Path,
        intakes: IdentityIntakeResolver,
        evidence: IdentityEvidenceResolver,
        reviews: IdentityReviewResolver,
    ) -> None:
        self.intakes = intakes
        self.evidence = evidence
        self.reviews = reviews
        self._store = _LocalReplayStore[
            VerifiedPromotedIdentityAdjudication
        ](
            root=root,
            directory="identity-adjudications",
            kind=_IDENTITY_ADJUDICATION,
            exact_type=VerifiedPromotedIdentityAdjudication,
            identity=lambda value: value.adjudication_id,
            record=self._record,
            replay=self._replay,
        )

    def path_for(self, adjudication_id: str) -> Path:
        return self._store.path_for(adjudication_id)

    def put(
        self, value: VerifiedPromotedIdentityAdjudication
    ) -> VerifiedPromotedIdentityAdjudication:
        return self._store.put(value)

    def get(
        self, adjudication_id: str
    ) -> VerifiedPromotedIdentityAdjudication:
        return self._store.get(adjudication_id)

    @staticmethod
    def _record(
        value: VerifiedPromotedIdentityAdjudication,
    ) -> PromotedGraphReplayRecord:
        return PromotedGraphReplayRecord(
            kind=_IDENTITY_ADJUDICATION,
            artifact_id=value.adjudication_id,
            primary_ids=(value.intake.intake_id,),
            secondary_ids=tuple(
                item.manifest.artifact_id
                for item in value.evidence_artifacts
            ),
            tertiary_ids=tuple(
                item.manifest.bundle_id for item in value.review_bundles
            ),
            dates=(),
            cutoff=value.cutoff,
        )

    def _replay(
        self, record: PromotedGraphReplayRecord
    ) -> VerifiedPromotedIdentityAdjudication:
        _require_shape(
            record,
            primary=1,
            secondary=None,
            tertiary=None,
            dates=0,
        )
        intake = self.intakes.get(record.primary_ids[0])
        evidence = tuple(
            self.evidence.get(item) for item in record.secondary_ids
        )
        reviews = tuple(
            self.reviews.get(item) for item in record.tertiary_ids
        )
        if (
            type(intake) is not VerifiedPromotedIdentityIntake
            or intake.intake_id != record.primary_ids[0]
            or any(
                type(value) is not StoredIdentityEvidenceArtifact
                or value.manifest.artifact_id != identity
                for value, identity in zip(evidence, record.secondary_ids)
            )
            or any(
                type(value) is not StoredIdentityReviewBundle
                or value.manifest.bundle_id != identity
                for value, identity in zip(reviews, record.tertiary_ids)
            )
        ):
            raise PromotedGraphStoreError(
                "promoted identity adjudication source differs"
            )
        return PromotedIdentityAdjudicationService().materialize(
            intake=intake,
            evidence_artifacts=evidence,
            review_bundles=reviews,
            cutoff=record.cutoff,
        )


class LocalPromotedIdentitySessionUniverseStore:
    def __init__(
        self,
        root: Path,
        adjudications: IdentityAdjudicationResolver,
        calendars: CalendarMaterializationResolver,
    ) -> None:
        self.adjudications = adjudications
        self.calendars = calendars
        self._store = _LocalReplayStore[
            VerifiedPromotedIdentitySessionUniverse
        ](
            root=root,
            directory="identity-session-universes",
            kind=_IDENTITY_SESSION_UNIVERSE,
            exact_type=VerifiedPromotedIdentitySessionUniverse,
            identity=lambda value: value.universe_id,
            record=self._record,
            replay=self._replay,
        )

    def path_for(self, universe_id: str) -> Path:
        return self._store.path_for(universe_id)

    def put(
        self, value: VerifiedPromotedIdentitySessionUniverse
    ) -> VerifiedPromotedIdentitySessionUniverse:
        return self._store.put(value)

    def get(
        self, universe_id: str
    ) -> VerifiedPromotedIdentitySessionUniverse:
        return self._store.get(universe_id)

    @staticmethod
    def _record(
        value: VerifiedPromotedIdentitySessionUniverse,
    ) -> PromotedGraphReplayRecord:
        return PromotedGraphReplayRecord(
            kind=_IDENTITY_SESSION_UNIVERSE,
            artifact_id=value.universe_id,
            primary_ids=(
                value.adjudication.adjudication_id,
                value.calendar.materialization_id,
            ),
            secondary_ids=(),
            tertiary_ids=(),
            dates=(value.market_session,),
            cutoff=value.cutoff,
        )

    def _replay(
        self, record: PromotedGraphReplayRecord
    ) -> VerifiedPromotedIdentitySessionUniverse:
        _require_shape(
            record,
            primary=2,
            secondary=0,
            tertiary=0,
            dates=1,
        )
        adjudication = self.adjudications.get(record.primary_ids[0])
        calendar = self.calendars.get(record.primary_ids[1])
        if (
            type(adjudication) is not VerifiedPromotedIdentityAdjudication
            or adjudication.adjudication_id != record.primary_ids[0]
            or type(calendar) is not CollectionCalendarMaterialization
            or calendar.materialization_id != record.primary_ids[1]
        ):
            raise PromotedGraphStoreError(
                "promoted identity universe source differs"
            )
        return PromotedIdentitySessionUniverseService().materialize(
            adjudication=adjudication,
            calendar=calendar,
            market_session=record.dates[0],
            cutoff=record.cutoff,
        )


class LocalPromotedSessionMarketDataFrameStore:
    def __init__(
        self,
        root: Path,
        universes: IdentitySessionUniverseResolver,
        corpora: HistoricalCorpusResolver,
    ) -> None:
        self.universes = universes
        self.corpora = corpora
        self._store = _LocalReplayStore[
            VerifiedPromotedSessionMarketDataFrame
        ](
            root=root,
            directory="session-market-data-frames",
            kind=_SESSION_MARKET_DATA_FRAME,
            exact_type=VerifiedPromotedSessionMarketDataFrame,
            identity=lambda value: value.frame_id,
            record=self._record,
            replay=self._replay,
        )

    def path_for(self, frame_id: str) -> Path:
        return self._store.path_for(frame_id)

    def put(
        self, value: VerifiedPromotedSessionMarketDataFrame
    ) -> VerifiedPromotedSessionMarketDataFrame:
        return self._store.put(value)

    def get(
        self, frame_id: str
    ) -> VerifiedPromotedSessionMarketDataFrame:
        return self._store.get(frame_id)

    @staticmethod
    def _record(
        value: VerifiedPromotedSessionMarketDataFrame,
    ) -> PromotedGraphReplayRecord:
        return PromotedGraphReplayRecord(
            kind=_SESSION_MARKET_DATA_FRAME,
            artifact_id=value.frame_id,
            primary_ids=(
                value.universe.universe_id,
                value.corpus_index.corpus_id,
                value.partition.partition_id,
            ),
            secondary_ids=(),
            tertiary_ids=(),
            dates=(),
            cutoff=value.cutoff,
        )

    def _replay(
        self, record: PromotedGraphReplayRecord
    ) -> VerifiedPromotedSessionMarketDataFrame:
        _require_shape(
            record,
            primary=3,
            secondary=0,
            tertiary=0,
            dates=0,
        )
        universe = self.universes.get(record.primary_ids[0])
        corpus_index, partitions = self.corpora.get(
            record.primary_ids[1]
        )
        matches = tuple(
            value
            for value in partitions
            if value.partition_id == record.primary_ids[2]
        )
        if (
            type(universe) is not VerifiedPromotedIdentitySessionUniverse
            or universe.universe_id != record.primary_ids[0]
            or type(corpus_index) is not HistoricalEvaluationCorpusIndex
            or corpus_index.corpus_id != record.primary_ids[1]
            or type(partitions) is not tuple
            or any(
                type(value)
                is not HistoricalEvaluationCorpusSessionPartition
                for value in partitions
            )
            or len(matches) != 1
            or matches[0].partition_id not in corpus_index.partition_ids
        ):
            raise PromotedGraphStoreError(
                "promoted session frame source differs"
            )
        return PromotedSessionMarketDataFrameService().materialize(
            universe=universe,
            corpus_index=corpus_index,
            partition=matches[0],
            cutoff=record.cutoff,
        )


class LocalPromotedSessionTickSnapshotStore:
    def __init__(
        self,
        root: Path,
        frames: SessionMarketDataFrameResolver,
    ) -> None:
        self.frames = frames
        self._store = _LocalReplayStore[
            VerifiedPromotedSessionTickSnapshot
        ](
            root=root,
            directory="session-tick-snapshots",
            kind=_SESSION_TICK_SNAPSHOT,
            exact_type=VerifiedPromotedSessionTickSnapshot,
            identity=lambda value: value.snapshot_id,
            record=self._record,
            replay=self._replay,
        )

    def path_for(self, snapshot_id: str) -> Path:
        return self._store.path_for(snapshot_id)

    def put(
        self, value: VerifiedPromotedSessionTickSnapshot
    ) -> VerifiedPromotedSessionTickSnapshot:
        return self._store.put(value)

    def get(
        self, snapshot_id: str
    ) -> VerifiedPromotedSessionTickSnapshot:
        return self._store.get(snapshot_id)

    @staticmethod
    def _record(
        value: VerifiedPromotedSessionTickSnapshot,
    ) -> PromotedGraphReplayRecord:
        return PromotedGraphReplayRecord(
            kind=_SESSION_TICK_SNAPSHOT,
            artifact_id=value.snapshot_id,
            primary_ids=(value.frame.frame_id,),
            secondary_ids=(),
            tertiary_ids=(),
            dates=(),
            cutoff=value.cutoff,
        )

    def _replay(
        self, record: PromotedGraphReplayRecord
    ) -> VerifiedPromotedSessionTickSnapshot:
        _require_shape(
            record,
            primary=1,
            secondary=0,
            tertiary=0,
            dates=0,
        )
        frame = self.frames.get(record.primary_ids[0])
        if (
            type(frame) is not VerifiedPromotedSessionMarketDataFrame
            or frame.frame_id != record.primary_ids[0]
        ):
            raise PromotedGraphStoreError(
                "promoted session tick source differs"
            )
        return PromotedSessionTickSizeService().materialize(
            frame=frame,
            cutoff=record.cutoff,
        )


class LocalPromotedStableListingHistoryStore:
    def __init__(
        self,
        root: Path,
        tick_snapshots: SessionTickSnapshotResolver,
        calendars: CalendarMaterializationResolver,
    ) -> None:
        self.tick_snapshots = tick_snapshots
        self.calendars = calendars
        self._store = _LocalReplayStore[
            VerifiedPromotedStableListingHistoryPanel
        ](
            root=root,
            directory="stable-listing-histories",
            kind=_STABLE_LISTING_HISTORY,
            exact_type=VerifiedPromotedStableListingHistoryPanel,
            identity=lambda value: value.panel_id,
            record=self._record,
            replay=self._replay,
        )

    def path_for(self, panel_id: str) -> Path:
        return self._store.path_for(panel_id)

    def put(
        self, value: VerifiedPromotedStableListingHistoryPanel
    ) -> VerifiedPromotedStableListingHistoryPanel:
        return self._store.put(value)

    def get(
        self, panel_id: str
    ) -> VerifiedPromotedStableListingHistoryPanel:
        return self._store.get(panel_id)

    @staticmethod
    def _record(
        value: VerifiedPromotedStableListingHistoryPanel,
    ) -> PromotedGraphReplayRecord:
        return PromotedGraphReplayRecord(
            kind=_STABLE_LISTING_HISTORY,
            artifact_id=value.panel_id,
            primary_ids=tuple(
                item.snapshot_id for item in value.tick_snapshots
            ),
            secondary_ids=(value.calendar.materialization_id,),
            tertiary_ids=(),
            dates=(),
            cutoff=value.cutoff,
        )

    def _replay(
        self, record: PromotedGraphReplayRecord
    ) -> VerifiedPromotedStableListingHistoryPanel:
        _require_shape(
            record,
            primary=None,
            secondary=1,
            tertiary=0,
            dates=0,
        )
        if not record.primary_ids:
            raise PromotedGraphStoreError(
                "promoted stable history sources are incomplete"
            )
        snapshots = tuple(
            self.tick_snapshots.get(item)
            for item in record.primary_ids
        )
        calendar = self.calendars.get(record.secondary_ids[0])
        if (
            any(
                type(value) is not VerifiedPromotedSessionTickSnapshot
                or value.snapshot_id != identity
                for value, identity in zip(
                    snapshots, record.primary_ids
                )
            )
            or type(calendar) is not CollectionCalendarMaterialization
            or calendar.materialization_id != record.secondary_ids[0]
        ):
            raise PromotedGraphStoreError(
                "promoted stable history source differs"
            )
        return PromotedStableListingHistoryService().materialize(
            tick_snapshots=snapshots,
            calendar=calendar,
            cutoff=record.cutoff,
        )


class LocalPromotedCorporateActionAdjustmentStore:
    def __init__(
        self,
        root: Path,
        histories: StableListingHistoryResolver,
        corporate_actions: CorporateActionSnapshotResolver,
    ) -> None:
        self.histories = histories
        self.corporate_actions = corporate_actions
        self._store = _LocalReplayStore[
            VerifiedPromotedCorporateActionAdjustmentPanel
        ](
            root=root,
            directory="corporate-action-adjustments",
            kind=_CORPORATE_ACTION_ADJUSTMENT,
            exact_type=VerifiedPromotedCorporateActionAdjustmentPanel,
            identity=lambda value: value.bridge_id,
            record=self._record,
            replay=self._replay,
        )

    def path_for(self, bridge_id: str) -> Path:
        return self._store.path_for(bridge_id)

    def put(
        self,
        value: VerifiedPromotedCorporateActionAdjustmentPanel,
    ) -> VerifiedPromotedCorporateActionAdjustmentPanel:
        return self._store.put(value)

    def get(
        self, bridge_id: str
    ) -> VerifiedPromotedCorporateActionAdjustmentPanel:
        return self._store.get(bridge_id)

    @staticmethod
    def _record(
        value: VerifiedPromotedCorporateActionAdjustmentPanel,
    ) -> PromotedGraphReplayRecord:
        return PromotedGraphReplayRecord(
            kind=_CORPORATE_ACTION_ADJUSTMENT,
            artifact_id=value.bridge_id,
            primary_ids=(
                value.source_panel.panel_id,
                value.corporate_actions.snapshot_id,
            ),
            secondary_ids=(),
            tertiary_ids=(),
            dates=(),
            cutoff=value.cutoff,
        )

    def _replay(
        self, record: PromotedGraphReplayRecord
    ) -> VerifiedPromotedCorporateActionAdjustmentPanel:
        _require_shape(
            record,
            primary=2,
            secondary=0,
            tertiary=0,
            dates=0,
        )
        history = self.histories.get(record.primary_ids[0])
        actions = self.corporate_actions.get(record.primary_ids[1])
        if (
            type(history) is not VerifiedPromotedStableListingHistoryPanel
            or history.panel_id != record.primary_ids[0]
            or type(actions) is not CorporateActionSnapshot
            or actions.snapshot_id != record.primary_ids[1]
        ):
            raise PromotedGraphStoreError(
                "promoted adjustment source differs"
            )
        return PromotedCorporateActionAdjustmentService().materialize(
            source_panel=history,
            corporate_actions=actions,
            cutoff=record.cutoff,
        )


class LocalPromotedEffectiveSessionTickStore:
    def __init__(
        self,
        root: Path,
        histories: StableListingHistoryResolver,
    ) -> None:
        self.histories = histories
        self._store = _LocalReplayStore[
            VerifiedPromotedEffectiveSessionTickPanel
        ](
            root=root,
            directory="effective-session-ticks",
            kind=_EFFECTIVE_SESSION_TICK,
            exact_type=VerifiedPromotedEffectiveSessionTickPanel,
            identity=lambda value: value.panel_id,
            record=self._record,
            replay=self._replay,
        )

    def path_for(self, panel_id: str) -> Path:
        return self._store.path_for(panel_id)

    def put(
        self, value: VerifiedPromotedEffectiveSessionTickPanel
    ) -> VerifiedPromotedEffectiveSessionTickPanel:
        return self._store.put(value)

    def get(
        self, panel_id: str
    ) -> VerifiedPromotedEffectiveSessionTickPanel:
        return self._store.get(panel_id)

    @staticmethod
    def _record(
        value: VerifiedPromotedEffectiveSessionTickPanel,
    ) -> PromotedGraphReplayRecord:
        return PromotedGraphReplayRecord(
            kind=_EFFECTIVE_SESSION_TICK,
            artifact_id=value.panel_id,
            primary_ids=(value.source_panel.panel_id,),
            secondary_ids=(),
            tertiary_ids=(),
            dates=(),
            cutoff=value.cutoff,
        )

    def _replay(
        self, record: PromotedGraphReplayRecord
    ) -> VerifiedPromotedEffectiveSessionTickPanel:
        _require_shape(
            record,
            primary=1,
            secondary=0,
            tertiary=0,
            dates=0,
        )
        history = self.histories.get(record.primary_ids[0])
        if (
            type(history) is not VerifiedPromotedStableListingHistoryPanel
            or history.panel_id != record.primary_ids[0]
        ):
            raise PromotedGraphStoreError(
                "promoted effective tick source differs"
            )
        return PromotedEffectiveSessionTickService().materialize(
            source_panel=history,
            cutoff=record.cutoff,
        )
