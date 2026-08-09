"""HYP-002 quality pilot: create-only immutable storage for canonical_response_v1.

Publishes each accepted, replay-verified :class:`QualityPilotObservation` at
a deterministic observation-ID path through the existing
:class:`~india_swing.daily_pipeline.state_publication.StateObjectWriter`,
and reloads only an explicitly pinned GCS object generation through the
existing :class:`~india_swing.daily_pipeline.acquisition.GCSObjectReader`.
Every returned storage lineage value and the complete decoded observation
are independently reverified before being trusted.

This module never constructs a GCS SDK client, never lists a bucket, never
resolves a "latest" object, and never overwrites, deletes, or aliases an
existing object -- append-only correction behavior emerges entirely from
observation identity: a correction has a different ``observation_id``, and
therefore a different object path, so publishing it can never alter the
original. All I/O capability is injected through the two accepted
protocols; this module performs no filesystem, environment, clock, network,
GCP SDK construction, Kite/broker, process, scheduler, or trading capability
of its own. Every artifact produced here remains permanently quality-only.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from hashlib import sha256

from india_swing.daily_pipeline.acquisition import GCSObjectPayload, GCSObjectReader
from india_swing.daily_pipeline.state_publication import PublishedStateObject, StateObjectWriter

from .canonical_response import (
    EndpointFamily,
    MAXIMUM_CHUNK_COUNT,
    MAXIMUM_ENCODED_BYTES,
    PILOT_PROTOCOL_SHA256,
    QualityPilotObservation,
    ScheduledWindowKind,
    encode_observation,
    replay_verify,
)


QUALITY_OBSERVATION_STORE_POLICY_VERSION = "quality_observation_store_v1"
QUALITY_OBSERVATION_CONTENT_TYPE = "application/json"

_MAXIMUM_GENERATION = 9223372036854775807  # 2**63 - 1, positive signed 64-bit max

_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET_PATTERN = re.compile(r"[a-z0-9][a-z0-9\-_.]{1,61}[a-z0-9]\Z")
_IP_SHAPED_PATTERN = re.compile(r"\d+\.\d+\.\d+\.\d+\Z")

_OBJECT_PATH_PREFIX = "quality-pilot/v1"

# The canonical object path encodes window_kind but not endpoint_family
# directly, so route consistency for a caller-supplied endpoint_family can
# only be enforced by independently re-deriving canonical_response.py's own
# fixed window-kind/endpoint-family pairing here (that table is private to
# canonical_response.py and intentionally not re-exported).
_WINDOW_KIND_ENDPOINT_FAMILY = {
    ScheduledWindowKind.CATALOG_PREOPEN: EndpointFamily.CATALOG,
    ScheduledWindowKind.QUOTE_0920: EndpointFamily.FULL_QUOTE,
    ScheduledWindowKind.QUOTE_CLOSE: EndpointFamily.FULL_QUOTE,
    ScheduledWindowKind.OHLCV_CLOSE: EndpointFamily.DAILY_OHLCV,
}


def _is_sha256(value: object) -> bool:
    return type(value) is str and _SHA256_PATTERN.fullmatch(value) is not None


class QualityPilotObservationStoreError(ValueError):
    """A quality-pilot observation-store input, capability, or artifact failed a static safety rule."""


def _fail(message: str) -> None:
    raise QualityPilotObservationStoreError(message)


def _validate_bucket(value: object) -> str:
    if (
        type(value) is not str
        or len(value) < 3
        or len(value) > 63
        or _BUCKET_PATTERN.fullmatch(value) is None
        or ".." in value
        or _IP_SHAPED_PATTERN.fullmatch(value) is not None
    ):
        _fail("observation store bucket name is invalid")
    return value


def _validate_generation(value: object) -> int:
    if type(value) is bool or type(value) is not int or value <= 0 or value > _MAXIMUM_GENERATION:
        _fail("observation store generation must be a positive non-bool integer")
    return value


def _validate_chunk_route(chunk_index: object, chunk_count: object) -> tuple[int, int]:
    if type(chunk_index) is bool or type(chunk_index) is not int or chunk_index < 1:
        _fail("observation store chunk_index is invalid")
    if (
        type(chunk_count) is bool
        or type(chunk_count) is not int
        or chunk_count < 1
        or chunk_count > MAXIMUM_CHUNK_COUNT
    ):
        _fail("observation store chunk_count is invalid")
    if chunk_index > chunk_count:
        _fail("observation store chunk_index exceeds chunk_count")
    return chunk_index, chunk_count


def canonical_observation_object_name(
    pilot_run_id: str,
    market_session: date,
    window_kind: ScheduledWindowKind,
    chunk_index: int,
    chunk_count: int,
    observation_id: str,
) -> str:
    """The one deterministic create-only path for a replay-verified observation.

    Derived only from already-validated canonical fields and exact enum
    values -- never a caller-selected publication path, and never involves
    listing, discovery, or "latest" resolution.
    """

    if not _is_sha256(pilot_run_id):
        _fail("observation store pilot_run_id is invalid")
    if type(market_session) is not date:
        _fail("observation store market_session is invalid")
    if type(window_kind) is not ScheduledWindowKind:
        _fail("observation store window_kind is invalid")
    _validate_chunk_route(chunk_index, chunk_count)
    if not _is_sha256(observation_id):
        _fail("observation store observation_id is invalid")
    return (
        f"{_OBJECT_PATH_PREFIX}/{pilot_run_id}/{market_session.isoformat()}/"
        f"{window_kind.value}/{chunk_index}-of-{chunk_count}/{observation_id}.json"
    )


def _encode_or_fail(observation: QualityPilotObservation) -> bytes:
    encode_failed = False
    encoded = b""
    try:
        encoded = encode_observation(observation)
    except Exception:
        encode_failed = True
    if encode_failed:
        _fail("observation could not be canonically encoded")
    return encoded


def _replay_bytes_or_fail(data: bytes) -> QualityPilotObservation:
    replay_failed = False
    replayed: QualityPilotObservation | None = None
    try:
        replayed = replay_verify(data)
    except Exception:
        replay_failed = True
    if replay_failed or replayed is None:
        _fail("observation could not be replay-verified")
    return replayed


_POSTURE_NAMES = (
    "quality_only",
    "counts_toward_o0",
    "counts_toward_clean_accumulation",
    "research_partition_eligible",
    "training_eligible",
    "feature_eligible",
    "label_eligible",
    "signal_eligible",
    "paper_trade_eligible",
    "notification_eligible",
    "execution_eligible",
    "capital_eligible",
)


class _FixedPostureMixin:
    """Read-only, fixed fail-closed posture shared by every store value.

    Plain properties, never dataclass fields -- no per-instance state, and
    storage never upgrades evidence authority.
    """

    @property
    def quality_only(self) -> bool:
        return True

    @property
    def counts_toward_o0(self) -> bool:
        return False

    @property
    def counts_toward_clean_accumulation(self) -> bool:
        return False

    @property
    def research_partition_eligible(self) -> bool:
        return False

    @property
    def training_eligible(self) -> bool:
        return False

    @property
    def feature_eligible(self) -> bool:
        return False

    @property
    def label_eligible(self) -> bool:
        return False

    @property
    def signal_eligible(self) -> bool:
        return False

    @property
    def paper_trade_eligible(self) -> bool:
        return False

    @property
    def notification_eligible(self) -> bool:
        return False

    @property
    def execution_eligible(self) -> bool:
        return False

    @property
    def capital_eligible(self) -> bool:
        return False


@dataclass(frozen=True, slots=True)
class PublishedQualityPilotObservation(_FixedPostureMixin):
    """One immutable, independently re-verifiable record of a create-only publication."""

    storage_policy_version: str
    protocol_sha256: str
    observation_id: str
    pilot_run_id: str
    market_session: date
    window_kind: ScheduledWindowKind
    endpoint_family: EndpointFamily
    chunk_index: int
    chunk_count: int
    bucket: str
    object_name: str
    generation: int
    encoded_byte_count: int
    encoded_sha256: str

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        if self.storage_policy_version != QUALITY_OBSERVATION_STORE_POLICY_VERSION:
            _fail("published observation storage policy version is invalid")
        if self.protocol_sha256 != PILOT_PROTOCOL_SHA256:
            _fail("published observation protocol hash is invalid")
        if not _is_sha256(self.observation_id):
            _fail("published observation id is invalid")
        if not _is_sha256(self.pilot_run_id):
            _fail("published observation pilot run id is invalid")
        if type(self.market_session) is not date:
            _fail("published observation market session is invalid")
        if type(self.window_kind) is not ScheduledWindowKind:
            _fail("published observation window kind is invalid")
        if type(self.endpoint_family) is not EndpointFamily:
            _fail("published observation endpoint family is invalid")
        if _WINDOW_KIND_ENDPOINT_FAMILY[self.window_kind] != self.endpoint_family:
            _fail("published observation window kind and endpoint family do not pair")
        _validate_chunk_route(self.chunk_index, self.chunk_count)
        _validate_bucket(self.bucket)
        expected_object_name = canonical_observation_object_name(
            self.pilot_run_id,
            self.market_session,
            self.window_kind,
            self.chunk_index,
            self.chunk_count,
            self.observation_id,
        )
        if self.object_name != expected_object_name:
            _fail("published observation object name disagrees with its exact canonical route")
        _validate_generation(self.generation)
        if (
            type(self.encoded_byte_count) is not int
            or self.encoded_byte_count <= 0
            or self.encoded_byte_count > MAXIMUM_ENCODED_BYTES
        ):
            _fail("published observation encoded byte count is invalid")
        if not _is_sha256(self.encoded_sha256):
            _fail("published observation encoded sha256 is invalid")
        if any(getattr(self, name) != (name == "quality_only") for name in _POSTURE_NAMES):
            _fail("published observation safety posture is invalid")


def publish_quality_pilot_observation(
    observation: QualityPilotObservation, bucket: str, writer: StateObjectWriter
) -> PublishedQualityPilotObservation:
    """Publish one accepted observation to its exact create-only canonical path.

    Encodes and replay-verifies the observation, independently requires
    the replayed value's immutable IDs/route fields to match the supplied
    observation, computes the exact path and SHA-256 locally, and calls
    ``writer.create_or_verify`` exactly once. Never retries. The writer's
    result is treated as untrusted and independently reconstructed/
    reverified against the local values before being accepted.
    """

    if type(observation) is not QualityPilotObservation:
        _fail("observation must be an exact QualityPilotObservation")

    encoded = _encode_or_fail(observation)
    replayed = _replay_bytes_or_fail(encoded)

    if replayed.observation_id != observation.observation_id:
        _fail("replayed observation id disagrees with the supplied observation")
    if replayed.canonical_response_sha256 != observation.canonical_response_sha256:
        _fail("replayed canonical response hash disagrees with the supplied observation")
    if replayed.normalized_content_sha256 != observation.normalized_content_sha256:
        _fail("replayed normalized content hash disagrees with the supplied observation")
    if replayed.window.window_id != observation.window.window_id:
        _fail("replayed window id disagrees with the supplied observation")
    if replayed.request.request_id != observation.request.request_id:
        _fail("replayed request id disagrees with the supplied observation")
    if replayed.corrects_observation_id != observation.corrects_observation_id:
        _fail("replayed correction lineage disagrees with the supplied observation")

    pilot_run_id = observation.window.pilot_run_id
    market_session = observation.window.market_session
    window_kind = observation.window.window_kind
    endpoint_family = observation.window.endpoint_family
    chunk_index = observation.request.chunk_index
    chunk_count = observation.request.chunk_count
    observation_id = observation.observation_id

    _validate_bucket(bucket)
    object_name = canonical_observation_object_name(
        pilot_run_id, market_session, window_kind, chunk_index, chunk_count, observation_id
    )
    encoded_byte_count = len(encoded)
    if encoded_byte_count > MAXIMUM_ENCODED_BYTES:
        _fail("encoded observation exceeds the maximum encoded byte ceiling")
    encoded_sha256 = sha256(encoded).hexdigest()

    write_failed = False
    published: PublishedStateObject | None = None
    try:
        published = writer.create_or_verify(
            bucket=bucket,
            object_name=object_name,
            content_bytes=encoded,
            content_type=QUALITY_OBSERVATION_CONTENT_TYPE,
            maximum_bytes=MAXIMUM_ENCODED_BYTES,
        )
    except Exception:
        write_failed = True
    if write_failed:
        _fail("observation could not be published")

    if type(published) is not PublishedStateObject:
        _fail("writer result type is invalid")

    reconstruct_failed = False
    reconstructed: PublishedStateObject | None = None
    try:
        reconstructed = PublishedStateObject(
            object_name=published.object_name,
            generation=published.generation,
            byte_count=published.byte_count,
            sha256=published.sha256,
        )
    except Exception:
        reconstruct_failed = True
    if reconstruct_failed or reconstructed is None:
        _fail("writer result could not be independently reverified")

    if reconstructed.object_name != object_name:
        _fail("writer result object name disagrees with the exact canonical route")
    if reconstructed.byte_count != encoded_byte_count:
        _fail("writer result byte count disagrees with the encoded observation")
    if reconstructed.sha256 != encoded_sha256:
        _fail("writer result sha256 disagrees with the encoded observation")
    _validate_generation(reconstructed.generation)

    return PublishedQualityPilotObservation(
        storage_policy_version=QUALITY_OBSERVATION_STORE_POLICY_VERSION,
        protocol_sha256=observation.request.protocol_sha256,
        observation_id=observation_id,
        pilot_run_id=pilot_run_id,
        market_session=market_session,
        window_kind=window_kind,
        endpoint_family=endpoint_family,
        chunk_index=chunk_index,
        chunk_count=chunk_count,
        bucket=bucket,
        object_name=object_name,
        generation=reconstructed.generation,
        encoded_byte_count=encoded_byte_count,
        encoded_sha256=encoded_sha256,
    )


@dataclass(frozen=True, slots=True)
class PinnedQualityPilotObservationRequest(_FixedPostureMixin):
    """One immutable, exact pin of a single GCS object generation to reload.

    Never accepts "latest" or an absent generation -- ``generation`` is a
    required positive integer field, and the exact canonical object name is
    independently derived and required to match ``object_name``.
    """

    bucket: str
    object_name: str
    generation: int
    expected_encoded_sha256: str
    expected_observation_id: str
    pilot_run_id: str
    market_session: date
    window_kind: ScheduledWindowKind
    endpoint_family: EndpointFamily
    chunk_index: int
    chunk_count: int

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        _validate_bucket(self.bucket)
        _validate_generation(self.generation)
        if not _is_sha256(self.expected_encoded_sha256):
            _fail("pinned request expected encoded sha256 is invalid")
        if not _is_sha256(self.expected_observation_id):
            _fail("pinned request expected observation id is invalid")
        if not _is_sha256(self.pilot_run_id):
            _fail("pinned request pilot run id is invalid")
        if type(self.market_session) is not date:
            _fail("pinned request market session is invalid")
        if type(self.window_kind) is not ScheduledWindowKind:
            _fail("pinned request window kind is invalid")
        if type(self.endpoint_family) is not EndpointFamily:
            _fail("pinned request endpoint family is invalid")
        if _WINDOW_KIND_ENDPOINT_FAMILY[self.window_kind] != self.endpoint_family:
            _fail("pinned request window kind and endpoint family do not pair")
        _validate_chunk_route(self.chunk_index, self.chunk_count)
        expected_object_name = canonical_observation_object_name(
            self.pilot_run_id,
            self.market_session,
            self.window_kind,
            self.chunk_index,
            self.chunk_count,
            self.expected_observation_id,
        )
        if self.object_name != expected_object_name:
            _fail("pinned request object name disagrees with its exact canonical route")


@dataclass(frozen=True, slots=True)
class LoadedQualityPilotObservation(_FixedPostureMixin):
    """One immutable, independently re-verified reload of a pinned observation."""

    observation: QualityPilotObservation
    request: PinnedQualityPilotObservationRequest

    def __post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        if type(self.observation) is not QualityPilotObservation:
            _fail("loaded observation type is invalid")
        verify_failed = False
        try:
            self.observation.verify_content_identity()
        except Exception:
            verify_failed = True
        if verify_failed:
            _fail("loaded observation failed independent verification")
        if type(self.request) is not PinnedQualityPilotObservationRequest:
            _fail("loaded observation request type is invalid")
        self.request._validate()

        if self.observation.observation_id != self.request.expected_observation_id:
            _fail("loaded observation id disagrees with its pinned request")
        if self.observation.request.protocol_sha256 != PILOT_PROTOCOL_SHA256:
            _fail("loaded observation protocol hash is invalid")
        if self.observation.window.pilot_run_id != self.request.pilot_run_id:
            _fail("loaded observation pilot run id disagrees with its pinned request")
        if self.observation.window.market_session != self.request.market_session:
            _fail("loaded observation market session disagrees with its pinned request")
        if self.observation.window.window_kind != self.request.window_kind:
            _fail("loaded observation window kind disagrees with its pinned request")
        if self.observation.window.endpoint_family != self.request.endpoint_family:
            _fail("loaded observation endpoint family disagrees with its pinned request")
        if self.observation.request.chunk_index != self.request.chunk_index:
            _fail("loaded observation chunk index disagrees with its pinned request")
        if self.observation.request.chunk_count != self.request.chunk_count:
            _fail("loaded observation chunk count disagrees with its pinned request")


def read_pinned_quality_pilot_observation(
    request: PinnedQualityPilotObservationRequest, reader: GCSObjectReader
) -> LoadedQualityPilotObservation:
    """Reload exactly one pinned GCS object generation and replay-verify its content.

    Calls ``reader.read_generation`` exactly once with the pinned bucket/
    object/generation and the shared maximum encoded byte ceiling. The
    reader's result is treated as untrusted: exact type, exact non-bool
    generation equality, bytes type, nonempty bounded content, and local
    SHA-256 equality are all required before any decode is attempted.
    """

    if type(request) is not PinnedQualityPilotObservationRequest:
        _fail("request must be an exact PinnedQualityPilotObservationRequest")

    read_failed = False
    payload: GCSObjectPayload | None = None
    try:
        payload = reader.read_generation(
            bucket=request.bucket,
            object_name=request.object_name,
            generation=request.generation,
            maximum_bytes=MAXIMUM_ENCODED_BYTES,
        )
    except Exception:
        read_failed = True
    if read_failed:
        _fail("pinned observation could not be read")

    if type(payload) is not GCSObjectPayload:
        _fail("reader result type is invalid")
    # GCSObjectPayload has no __post_init__ of its own, so `generation` may
    # be any caller-supplied value, including a hostile object whose own
    # __eq__/__ne__ would otherwise execute (and could raise/leak) during a
    # bare `!=` comparison. Requiring the exact int type first -- never an
    # isinstance/bool-only check, which numeric equality would still let a
    # float slip through -- means no foreign dunder method is ever invoked:
    # only a confirmed plain int is ever compared with request.generation.
    if type(payload.generation) is not int:
        _fail("reader result generation is invalid")
    if payload.generation != request.generation:
        _fail("reader result generation disagrees with the pinned request")
    if type(payload.content_bytes) is not bytes or len(payload.content_bytes) == 0:
        _fail("reader result content is invalid")
    if len(payload.content_bytes) > MAXIMUM_ENCODED_BYTES:
        _fail("reader result content exceeds the maximum encoded byte ceiling")
    actual_sha256 = sha256(payload.content_bytes).hexdigest()
    if actual_sha256 != request.expected_encoded_sha256:
        _fail("reader result content does not match its expected sha256")

    observation = _replay_bytes_or_fail(payload.content_bytes)

    return LoadedQualityPilotObservation(observation=observation, request=request)
