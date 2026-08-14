"""Exact, collection-only identity-state checkpoints for bounded replay.

The checkpoint is a deterministic cache produced only after replay has
authenticated every session through its checkpoint boundary.  A later run
pins the checkpoint object by generation and SHA-256, verifies its canonical
content and its exact dataset/session binding, then may skip the already
authenticated prefix.  It never supplies trading, feature, training, alert,
or execution authority.
"""

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
from india_swing.daily_reports.parser import _SYMBOL
from india_swing.identity import content_id

from .nse_archive_research_dataset import NseArchiveResearchDataset
from .nse_archive_research_identity import (
    RESEARCH_IDENTITY_ADMISSION_POLICY_VERSION,
    research_identity_id_for_isin,
)


NSE_ARCHIVE_RESEARCH_IDENTITY_CHECKPOINT_SCHEMA_VERSION = (
    "nse-archive-research-identity-checkpoint/v1"
)
NSE_ARCHIVE_RESEARCH_IDENTITY_CHECKPOINT_POLICY_VERSION = (
    "exact-dataset-point-in-time-state/v1"
)
NSE_ARCHIVE_RESEARCH_IDENTITY_CHECKPOINT_STORE_SCHEMA_VERSION = (
    "nse-archive-research-identity-checkpoint-store/v1"
)
MAXIMUM_NSE_ARCHIVE_RESEARCH_IDENTITY_CHECKPOINT_BYTES = 16 * 1024 * 1024

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]\Z")
_MAXIMUM_GENERATION = 9_223_372_036_854_775_807
_OBJECT_PREFIX = "research/nse-archive-identity-checkpoints/v1"
_CONTENT_TYPE = "application/json"
_ROOT_KEYS = {
    "store_schema_version",
    "checkpoint_schema_version",
    "checkpoint_policy_version",
    "identity_admission_policy_version",
    "checkpoint_id",
    "dataset_id",
    "checkpoint_session",
    "checkpoint_session_snapshot_id",
    "latest_by_listing_key",
    "latest_by_identity",
    "collection_only",
    "actionable",
    "training_eligible",
    "feature_eligible",
    "label_eligible",
    "alert_eligible",
    "execution_eligible",
}
_LISTING_STATE_KEYS = {
    "listing_key",
    "research_identity_id",
    "source_isin",
    "symbol",
    "record_id",
    "market_session",
}
_IDENTITY_STATE_KEYS = {
    "research_identity_id",
    "listing_key",
    "source_isin",
    "symbol",
    "record_id",
    "market_session",
}


class NseArchiveResearchIdentityCheckpointError(ValueError):
    """A checkpoint or its exact dataset binding failed a static safety rule."""


class NseArchiveResearchIdentityCheckpointGCSError(ValueError):
    """A pinned GCS checkpoint read failed without exposing storage details."""


def _fail(message: str) -> None:
    raise NseArchiveResearchIdentityCheckpointError(message)


def _sha256(value: object, message: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        _fail(message)
    return value


def _state_date(value: object, message: str) -> date:
    if type(value) is not date:
        _fail(message)
    return value


@dataclass(frozen=True, slots=True)
class NseArchiveResearchIdentityListingState:
    """The latest admitted observation for one listing key."""

    listing_key: str
    research_identity_id: str
    source_isin: str
    symbol: str
    record_id: str
    market_session: date

    def __post_init__(self) -> None:
        self.verify_content_identity()

    def verify_content_identity(self) -> None:
        if (
            type(self.symbol) is not str
            or _SYMBOL.fullmatch(self.symbol) is None
            or self.listing_key != f"NSE:{self.symbol}"
        ):
            _fail("research identity checkpoint listing state is invalid")
        _sha256(
            self.research_identity_id,
            "research identity checkpoint listing state is invalid",
        )
        identity_failed = False
        expected_identity_id = None
        try:
            expected_identity_id = research_identity_id_for_isin(self.source_isin)
        except Exception:
            identity_failed = True
        if identity_failed or expected_identity_id != self.research_identity_id:
            _fail("research identity checkpoint listing state is invalid")
        _sha256(self.record_id, "research identity checkpoint listing state is invalid")
        _state_date(
            self.market_session,
            "research identity checkpoint listing state is invalid",
        )


@dataclass(frozen=True, slots=True)
class NseArchiveResearchIdentityState:
    """The latest admitted observation for one research identity."""

    research_identity_id: str
    listing_key: str
    source_isin: str
    symbol: str
    record_id: str
    market_session: date

    def __post_init__(self) -> None:
        self.verify_content_identity()

    def verify_content_identity(self) -> None:
        if (
            type(self.symbol) is not str
            or _SYMBOL.fullmatch(self.symbol) is None
            or self.listing_key != f"NSE:{self.symbol}"
        ):
            _fail("research identity checkpoint identity state is invalid")
        _sha256(
            self.research_identity_id,
            "research identity checkpoint identity state is invalid",
        )
        identity_failed = False
        expected_identity_id = None
        try:
            expected_identity_id = research_identity_id_for_isin(self.source_isin)
        except Exception:
            identity_failed = True
        if identity_failed or expected_identity_id != self.research_identity_id:
            _fail("research identity checkpoint identity state is invalid")
        _sha256(self.record_id, "research identity checkpoint identity state is invalid")
        _state_date(
            self.market_session,
            "research identity checkpoint identity state is invalid",
        )


def _verify_listing_state(
    values: tuple[NseArchiveResearchIdentityListingState, ...],
    checkpoint_session: date,
) -> None:
    if type(values) is not tuple or any(
        type(value) is not NseArchiveResearchIdentityListingState for value in values
    ):
        _fail("research identity checkpoint listing state is invalid")
    keys = tuple(value.listing_key for value in values)
    if keys != tuple(sorted(keys)) or len(set(keys)) != len(keys):
        _fail("research identity checkpoint listing state is invalid")
    if any(value.market_session > checkpoint_session for value in values):
        _fail("research identity checkpoint listing state is invalid")
    for value in values:
        value.verify_content_identity()


def _verify_identity_state(
    values: tuple[NseArchiveResearchIdentityState, ...],
    checkpoint_session: date,
) -> None:
    if type(values) is not tuple or any(
        type(value) is not NseArchiveResearchIdentityState for value in values
    ):
        _fail("research identity checkpoint identity state is invalid")
    keys = tuple(value.research_identity_id for value in values)
    if keys != tuple(sorted(keys)) or len(set(keys)) != len(keys):
        _fail("research identity checkpoint identity state is invalid")
    if any(value.market_session > checkpoint_session for value in values):
        _fail("research identity checkpoint identity state is invalid")
    for value in values:
        value.verify_content_identity()


def _verify_cross_map_consistency(
    listing_values: tuple[NseArchiveResearchIdentityListingState, ...],
    identity_values: tuple[NseArchiveResearchIdentityState, ...],
) -> None:
    """Check the monotonic links that remain valid after rebinding.

    The maps are intentionally not a bijection: a listing may later rebound
    and an identity may later appear under another symbol.  Each counterpart
    therefore has to be at least as recent as the observation it supersedes;
    equal timestamps must describe the identical admitted observation.
    """

    by_listing = {value.listing_key: value for value in listing_values}
    by_identity = {value.research_identity_id: value for value in identity_values}
    for listing in listing_values:
        identity = by_identity.get(listing.research_identity_id)
        if identity is None or identity.market_session < listing.market_session:
            _fail("research identity checkpoint state maps are inconsistent")
        if identity.market_session == listing.market_session and (
            identity.listing_key != listing.listing_key
            or identity.source_isin != listing.source_isin
            or identity.symbol != listing.symbol
            or identity.record_id != listing.record_id
        ):
            _fail("research identity checkpoint state maps are inconsistent")
    for identity in identity_values:
        listing = by_listing.get(identity.listing_key)
        if listing is None or listing.market_session < identity.market_session:
            _fail("research identity checkpoint state maps are inconsistent")
        if listing.market_session == identity.market_session and (
            listing.research_identity_id != identity.research_identity_id
            or listing.source_isin != identity.source_isin
            or listing.symbol != identity.symbol
            or listing.record_id != identity.record_id
        ):
            _fail("research identity checkpoint state maps are inconsistent")


@dataclass(frozen=True, slots=True)
class NseArchiveResearchIdentityCheckpoint:
    """Canonical latest-observation state after one exact accepted session."""

    dataset_id: str
    checkpoint_session: date
    checkpoint_session_snapshot_id: str
    latest_by_listing_key: tuple[NseArchiveResearchIdentityListingState, ...]
    latest_by_identity: tuple[NseArchiveResearchIdentityState, ...]
    schema_version: str = NSE_ARCHIVE_RESEARCH_IDENTITY_CHECKPOINT_SCHEMA_VERSION
    policy_version: str = NSE_ARCHIVE_RESEARCH_IDENTITY_CHECKPOINT_POLICY_VERSION
    identity_admission_policy_version: str = RESEARCH_IDENTITY_ADMISSION_POLICY_VERSION
    collection_only: bool = field(init=False)
    actionable: bool = field(init=False)
    training_eligible: bool = field(init=False)
    feature_eligible: bool = field(init=False)
    label_eligible: bool = field(init=False)
    alert_eligible: bool = field(init=False)
    execution_eligible: bool = field(init=False)
    checkpoint_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "collection_only", True)
        object.__setattr__(self, "actionable", False)
        object.__setattr__(self, "training_eligible", False)
        object.__setattr__(self, "feature_eligible", False)
        object.__setattr__(self, "label_eligible", False)
        object.__setattr__(self, "alert_eligible", False)
        object.__setattr__(self, "execution_eligible", False)
        self._validate()
        object.__setattr__(self, "checkpoint_id", self._calculated_id())

    def _validate(self) -> None:
        _sha256(self.dataset_id, "research identity checkpoint dataset id is invalid")
        _sha256(
            self.checkpoint_session_snapshot_id,
            "research identity checkpoint session snapshot id is invalid",
        )
        _state_date(
            self.checkpoint_session,
            "research identity checkpoint session is invalid",
        )
        if (
            self.schema_version
            != NSE_ARCHIVE_RESEARCH_IDENTITY_CHECKPOINT_SCHEMA_VERSION
            or self.policy_version
            != NSE_ARCHIVE_RESEARCH_IDENTITY_CHECKPOINT_POLICY_VERSION
            or self.identity_admission_policy_version
            != RESEARCH_IDENTITY_ADMISSION_POLICY_VERSION
            or self.collection_only is not True
            or self.actionable is not False
            or self.training_eligible is not False
            or self.feature_eligible is not False
            or self.label_eligible is not False
            or self.alert_eligible is not False
            or self.execution_eligible is not False
        ):
            _fail("research identity checkpoint safety posture is invalid")
        _verify_listing_state(self.latest_by_listing_key, self.checkpoint_session)
        _verify_identity_state(self.latest_by_identity, self.checkpoint_session)
        _verify_cross_map_consistency(
            self.latest_by_listing_key,
            self.latest_by_identity,
        )

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": self.schema_version,
                "policy_version": self.policy_version,
                "identity_admission_policy_version": (
                    self.identity_admission_policy_version
                ),
                "dataset_id": self.dataset_id,
                "checkpoint_session": self.checkpoint_session,
                "checkpoint_session_snapshot_id": self.checkpoint_session_snapshot_id,
                "latest_by_listing_key": tuple(
                    (
                        value.listing_key,
                        value.research_identity_id,
                        value.source_isin,
                        value.symbol,
                        value.record_id,
                        value.market_session,
                    )
                    for value in self.latest_by_listing_key
                ),
                "latest_by_identity": tuple(
                    (
                        value.research_identity_id,
                        value.listing_key,
                        value.source_isin,
                        value.symbol,
                        value.record_id,
                        value.market_session,
                    )
                    for value in self.latest_by_identity
                ),
                "collection_only": self.collection_only,
                "actionable": self.actionable,
                "training_eligible": self.training_eligible,
                "feature_eligible": self.feature_eligible,
                "label_eligible": self.label_eligible,
                "alert_eligible": self.alert_eligible,
                "execution_eligible": self.execution_eligible,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        self._validate()
        if self.checkpoint_id != self._calculated_id():
            _fail("research identity checkpoint identity failed")


def verify_nse_archive_research_identity_checkpoint_for_dataset(
    checkpoint: NseArchiveResearchIdentityCheckpoint,
    dataset: NseArchiveResearchDataset,
) -> tuple[
    dict[str, tuple[str, str, str, str, date]],
    dict[str, tuple[str, str, str, str, date]],
]:
    """Return verified mutable state maps only for the exact bound dataset."""

    failed = False
    result = None
    try:
        if (
            type(checkpoint) is not NseArchiveResearchIdentityCheckpoint
            or type(dataset) is not NseArchiveResearchDataset
        ):
            raise ValueError
        checkpoint.verify_content_identity()
        dataset.verify_content_identity()
        if checkpoint.dataset_id != dataset.dataset_id:
            raise ValueError
        position = dataset.accepted_sessions.index(checkpoint.checkpoint_session)
        if (
            dataset.session_snapshot_ids[position]
            != checkpoint.checkpoint_session_snapshot_id
        ):
            raise ValueError
        accepted_prefix = set(dataset.accepted_sessions[: position + 1])
        if any(
            value.market_session not in accepted_prefix
            for value in checkpoint.latest_by_listing_key
        ) or any(
            value.market_session not in accepted_prefix
            for value in checkpoint.latest_by_identity
        ):
            raise ValueError
        result = (
            {
                value.listing_key: (
                    value.research_identity_id,
                    value.source_isin,
                    value.symbol,
                    value.record_id,
                    value.market_session,
                )
                for value in checkpoint.latest_by_listing_key
            },
            {
                value.research_identity_id: (
                    value.listing_key,
                    value.source_isin,
                    value.symbol,
                    value.record_id,
                    value.market_session,
                )
                for value in checkpoint.latest_by_identity
            },
        )
    except Exception:
        failed = True
    if failed or result is None:
        _fail("research identity checkpoint does not match its dataset")
    return result


def _listing_state_value(value: NseArchiveResearchIdentityListingState) -> dict[str, object]:
    return {
        "listing_key": value.listing_key,
        "research_identity_id": value.research_identity_id,
        "source_isin": value.source_isin,
        "symbol": value.symbol,
        "record_id": value.record_id,
        "market_session": value.market_session.isoformat(),
    }


def _identity_state_value(value: NseArchiveResearchIdentityState) -> dict[str, object]:
    return {
        "research_identity_id": value.research_identity_id,
        "listing_key": value.listing_key,
        "source_isin": value.source_isin,
        "symbol": value.symbol,
        "record_id": value.record_id,
        "market_session": value.market_session.isoformat(),
    }


def encode_nse_archive_research_identity_checkpoint(
    value: NseArchiveResearchIdentityCheckpoint,
) -> bytes:
    if type(value) is not NseArchiveResearchIdentityCheckpoint:
        raise TypeError("research identity checkpoint must be exact")
    value.verify_content_identity()
    payload = {
        "store_schema_version": NSE_ARCHIVE_RESEARCH_IDENTITY_CHECKPOINT_STORE_SCHEMA_VERSION,
        "checkpoint_schema_version": value.schema_version,
        "checkpoint_policy_version": value.policy_version,
        "identity_admission_policy_version": (
            value.identity_admission_policy_version
        ),
        "checkpoint_id": value.checkpoint_id,
        "dataset_id": value.dataset_id,
        "checkpoint_session": value.checkpoint_session.isoformat(),
        "checkpoint_session_snapshot_id": value.checkpoint_session_snapshot_id,
        "latest_by_listing_key": [
            _listing_state_value(item) for item in value.latest_by_listing_key
        ],
        "latest_by_identity": [
            _identity_state_value(item) for item in value.latest_by_identity
        ],
        "collection_only": value.collection_only,
        "actionable": value.actionable,
        "training_eligible": value.training_eligible,
        "feature_eligible": value.feature_eligible,
        "label_eligible": value.label_eligible,
        "alert_eligible": value.alert_eligible,
        "execution_eligible": value.execution_eligible,
    }
    return (
        json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _mapping(value: object, keys: set[str]) -> dict[str, object]:
    if type(value) is not dict or set(value) != keys:
        raise ValueError
    return value


def _decode_date(value: object) -> date:
    if type(value) is not str:
        raise ValueError
    result = date.fromisoformat(value)
    if result.isoformat() != value:
        raise ValueError
    return result


def _decode_listing_state(value: object) -> NseArchiveResearchIdentityListingState:
    raw = _mapping(value, _LISTING_STATE_KEYS)
    return NseArchiveResearchIdentityListingState(
        listing_key=raw["listing_key"],
        research_identity_id=raw["research_identity_id"],
        source_isin=raw["source_isin"],
        symbol=raw["symbol"],
        record_id=raw["record_id"],
        market_session=_decode_date(raw["market_session"]),
    )


def _decode_identity_state(value: object) -> NseArchiveResearchIdentityState:
    raw = _mapping(value, _IDENTITY_STATE_KEYS)
    return NseArchiveResearchIdentityState(
        research_identity_id=raw["research_identity_id"],
        listing_key=raw["listing_key"],
        source_isin=raw["source_isin"],
        symbol=raw["symbol"],
        record_id=raw["record_id"],
        market_session=_decode_date(raw["market_session"]),
    )


def decode_nse_archive_research_identity_checkpoint(
    payload: bytes,
) -> NseArchiveResearchIdentityCheckpoint:
    """Decode one exact canonical checkpoint without leaking parser details."""

    if (
        type(payload) is not bytes
        or not payload
        or len(payload) > MAXIMUM_NSE_ARCHIVE_RESEARCH_IDENTITY_CHECKPOINT_BYTES
    ):
        _fail("stored research identity checkpoint is invalid")
    malformed = False
    checkpoint = None
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        root = _mapping(raw, _ROOT_KEYS)
        if (
            root["store_schema_version"]
            != NSE_ARCHIVE_RESEARCH_IDENTITY_CHECKPOINT_STORE_SCHEMA_VERSION
            or root["checkpoint_schema_version"]
            != NSE_ARCHIVE_RESEARCH_IDENTITY_CHECKPOINT_SCHEMA_VERSION
            or root["checkpoint_policy_version"]
            != NSE_ARCHIVE_RESEARCH_IDENTITY_CHECKPOINT_POLICY_VERSION
            or root["identity_admission_policy_version"]
            != RESEARCH_IDENTITY_ADMISSION_POLICY_VERSION
            or type(root["latest_by_listing_key"]) is not list
            or type(root["latest_by_identity"]) is not list
        ):
            raise ValueError
        checkpoint = NseArchiveResearchIdentityCheckpoint(
            dataset_id=root["dataset_id"],
            checkpoint_session=_decode_date(root["checkpoint_session"]),
            checkpoint_session_snapshot_id=root["checkpoint_session_snapshot_id"],
            latest_by_listing_key=tuple(
                _decode_listing_state(item) for item in root["latest_by_listing_key"]
            ),
            latest_by_identity=tuple(
                _decode_identity_state(item) for item in root["latest_by_identity"]
            ),
        )
        for name in (
            "collection_only",
            "actionable",
            "training_eligible",
            "feature_eligible",
            "label_eligible",
            "alert_eligible",
            "execution_eligible",
        ):
            if getattr(checkpoint, name) is not root[name]:
                raise ValueError
        if checkpoint.checkpoint_id != root["checkpoint_id"]:
            raise ValueError
        if encode_nse_archive_research_identity_checkpoint(checkpoint) != payload:
            raise ValueError
    except Exception:
        malformed = True
    if malformed or checkpoint is None:
        _fail("stored research identity checkpoint is invalid")
    return checkpoint


def nse_archive_research_identity_checkpoint_object_name(checkpoint_id: str) -> str:
    _sha256(checkpoint_id, "research identity checkpoint object id is invalid")
    return f"{_OBJECT_PREFIX}/{checkpoint_id}.json"


@dataclass(frozen=True, slots=True)
class PinnedNseArchiveResearchIdentityCheckpointRequest:
    bucket: str
    checkpoint_id: str
    generation: int
    expected_sha256: str

    def __post_init__(self) -> None:
        invalid = False
        if type(self.bucket) is not str or _BUCKET.fullmatch(self.bucket) is None:
            invalid = True
        try:
            nse_archive_research_identity_checkpoint_object_name(self.checkpoint_id)
        except Exception:
            invalid = True
        if (
            type(self.generation) is not int
            or type(self.generation) is bool
            or self.generation <= 0
            or self.generation > _MAXIMUM_GENERATION
            or type(self.expected_sha256) is not str
            or _SHA256.fullmatch(self.expected_sha256) is None
        ):
            invalid = True
        if invalid:
            raise NseArchiveResearchIdentityCheckpointGCSError(
                "pinned research identity checkpoint read failed"
            )

    @property
    def object_name(self) -> str:
        return nse_archive_research_identity_checkpoint_object_name(self.checkpoint_id)


def read_pinned_nse_archive_research_identity_checkpoint(
    *,
    request: PinnedNseArchiveResearchIdentityCheckpointRequest,
    reader: GCSObjectReader,
) -> NseArchiveResearchIdentityCheckpoint:
    """Read one generation- and SHA-pinned checkpoint object exactly once."""

    failed = False
    checkpoint = None
    try:
        if type(request) is not PinnedNseArchiveResearchIdentityCheckpointRequest:
            raise ValueError
        payload = reader.read_generation(
            bucket=request.bucket,
            object_name=request.object_name,
            generation=request.generation,
            maximum_bytes=MAXIMUM_NSE_ARCHIVE_RESEARCH_IDENTITY_CHECKPOINT_BYTES,
        )
        if (
            type(payload) is not GCSObjectPayload
            or type(payload.generation) is not int
            or payload.generation != request.generation
            or type(payload.content_bytes) is not bytes
            or not payload.content_bytes
            or len(payload.content_bytes)
            > MAXIMUM_NSE_ARCHIVE_RESEARCH_IDENTITY_CHECKPOINT_BYTES
            or hashlib.sha256(payload.content_bytes).hexdigest()
            != request.expected_sha256
        ):
            raise ValueError
        checkpoint = decode_nse_archive_research_identity_checkpoint(
            payload.content_bytes
        )
        if checkpoint.checkpoint_id != request.checkpoint_id:
            raise ValueError
        checkpoint.verify_content_identity()
    except Exception:
        failed = True
    if failed or checkpoint is None:
        raise NseArchiveResearchIdentityCheckpointGCSError(
            "pinned research identity checkpoint read failed"
        )
    return checkpoint


def publish_nse_archive_research_identity_checkpoint(
    *,
    checkpoint: NseArchiveResearchIdentityCheckpoint,
    bucket: str,
    writer: StateObjectWriter,
) -> PublishedStateObject:
    """Create or byte-verify one content-addressed checkpoint object."""

    failed = False
    published = None
    payload = b""
    object_name = ""
    try:
        if (
            type(checkpoint) is not NseArchiveResearchIdentityCheckpoint
            or type(bucket) is not str
            or _BUCKET.fullmatch(bucket) is None
            or not callable(getattr(writer, "create_or_verify", None))
        ):
            raise ValueError
        checkpoint.verify_content_identity()
        payload = encode_nse_archive_research_identity_checkpoint(checkpoint)
        object_name = nse_archive_research_identity_checkpoint_object_name(
            checkpoint.checkpoint_id
        )
        published = writer.create_or_verify(
            bucket=bucket,
            object_name=object_name,
            content_bytes=payload,
            content_type=_CONTENT_TYPE,
            maximum_bytes=MAXIMUM_NSE_ARCHIVE_RESEARCH_IDENTITY_CHECKPOINT_BYTES,
        )
        if (
            type(published) is not PublishedStateObject
            or published.object_name != object_name
            or published.byte_count != len(payload)
            or published.sha256 != hashlib.sha256(payload).hexdigest()
        ):
            raise ValueError
        published = PublishedStateObject(
            object_name=published.object_name,
            generation=published.generation,
            byte_count=published.byte_count,
            sha256=published.sha256,
        )
    except Exception:
        failed = True
    if failed or published is None:
        _fail("research identity checkpoint publication failed safely")
    return published
