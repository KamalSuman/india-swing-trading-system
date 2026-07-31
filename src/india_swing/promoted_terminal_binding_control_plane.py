"""Read/write control plane for the independent trusted-terminal-binding
artifact defined in ``promoted_terminal_binding.py``.

Holds only the reader port, the observed-object type, the duck-typed
storage adapter, and the seal/load orchestration functions. Reuses the
existing ``StateObjectWriter``/``PublishedStateObject`` write-side
abstractions from ``daily_pipeline/state_publication.py`` unchanged --
their production implementation already provides create-once plus conflict
detection plus idempotent replay, so no second writer exists here. The
existing generation-pinned ``GCSObjectReader`` is insufficient for this
artifact because at restart the caller knows only the run spec, not a
generation, so this module adds exactly one small read port that observes
the current generation of one deterministic object name and then pins it --
never a bucket listing, never a "latest object" selection.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Protocol

from india_swing.daily_pipeline.state_publication import PublishedStateObject, StateObjectWriter
from india_swing.promoted_operational_persistence import PromotedOperationalTerminalRecord
from india_swing.promoted_operational_runner import PromotedOperationalRunSpec
from india_swing.promoted_operational_service import TrustedPromotedOperationalTerminalBinding
from india_swing.promoted_terminal_binding import (
    MAXIMUM_TERMINAL_BINDING_BYTES,
    PromotedOperationalTerminalBindingRecord,
    PromotedTerminalBindingError,
    build_promoted_operational_terminal_binding_record,
    decode_promoted_operational_terminal_binding_record,
    encode_promoted_operational_terminal_binding_record,
    promoted_operational_terminal_binding_object_name,
    trusted_binding_from_record,
)

try:
    from google.api_core.exceptions import NotFound
except ImportError:  # pragma: no cover - optional dependency
    NotFound = None


class PromotedTerminalBindingControlPlaneError(PromotedTerminalBindingError):
    pass


_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]\Z")
_MAXIMUM_GENERATION = 9_223_372_036_854_775_807
_ERR_CONTROL_PLANE = "promoted terminal binding control plane call is invalid"


def _validate_bucket(value: object) -> str:
    if type(value) is not str or _BUCKET.fullmatch(value) is None:
        raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
    return value


@dataclass(frozen=True, slots=True)
class ObservedTerminalBindingObject:
    object_name: str
    generation: int
    byte_count: int
    sha256: str
    content_bytes: bytes

    def __post_init__(self) -> None:
        if type(self.object_name) is not str or not self.object_name:
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
        if (
            type(self.generation) is bool
            or type(self.generation) is not int
            or not (1 <= self.generation <= _MAXIMUM_GENERATION)
        ):
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
        if type(self.content_bytes) is not bytes or not (
            0 < len(self.content_bytes) <= MAXIMUM_TERMINAL_BINDING_BYTES
        ):
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
        if type(self.byte_count) is not int or self.byte_count != len(self.content_bytes):
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
        if (
            type(self.sha256) is not str
            or self.sha256 != hashlib.sha256(self.content_bytes).hexdigest()
        ):
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)


class PromotedTerminalBindingObjectReader(Protocol):
    """Reads exactly one deterministic object name at its currently
    observed generation, then re-pins that generation on a second handle
    before trusting the downloaded bytes. Never lists a bucket and never
    selects a "latest" object."""

    def read_current(
        self, *, bucket: str, object_name: str, maximum_bytes: int
    ) -> ObservedTerminalBindingObject: ...

    def read_current_optional(
        self, *, bucket: str, object_name: str, maximum_bytes: int
    ) -> ObservedTerminalBindingObject | None:
        """Identical to ``read_current`` except it returns ``None`` ONLY
        when the exact object is proven not to exist. Every other
        failure -- malformed content, an oversized payload, a permission
        failure, a generation change between observation and download, or
        any other error -- must still raise, never be treated as
        absence."""
        ...


class GoogleCloudStorageTerminalBindingReader:
    """Production PromotedTerminalBindingObjectReader backed by
    google-cloud-storage.

    The constructor requires an already-constructed client; it never
    imports google.cloud.storage, never constructs a client itself, never
    reads an environment variable or credential, and never falls back to
    an ambient default client. The deployment caller constructs and
    injects the real client in a later, separately authorized increment.
    """

    def __init__(self, client: object) -> None:
        if client is None:
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
        self._client = client

    def _validated_read_request(
        self, *, bucket: str, object_name: str, maximum_bytes: int
    ) -> str:
        bucket = _validate_bucket(bucket)
        if type(object_name) is not str or not object_name:
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
        if type(maximum_bytes) is not int or maximum_bytes <= 0:
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
        return bucket

    def _observe_then_pin(
        self, *, bucket: str, object_name: str, maximum_bytes: int
    ) -> ObservedTerminalBindingObject:
        """The exact observe-then-pin-then-verify sequence shared by
        ``read_current`` and ``read_current_optional``. Raises the SDK's
        own exception unmodified (never wrapped) so a caller can inspect
        its exact type -- ``read_current`` collapses any failure here into
        one sanitized error, while ``read_current_optional`` additionally
        distinguishes an exact NotFound from every other failure."""

        blob = self._client.bucket(bucket).blob(object_name)
        blob.reload(retry=None)
        observed_generation = blob.generation
        if (
            type(observed_generation) is bool
            or type(observed_generation) is not int
            or not (1 <= observed_generation <= _MAXIMUM_GENERATION)
        ):
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)

        pinned_blob = self._client.bucket(bucket).blob(
            object_name, generation=observed_generation
        )
        downloaded = pinned_blob.download_as_bytes(
            end=maximum_bytes,
            raw_download=True,
            if_generation_match=observed_generation,
            retry=None,
        )
        pinned_generation = pinned_blob.generation
        if (
            type(pinned_generation) is bool
            or type(pinned_generation) is not int
            or pinned_generation != observed_generation
        ):
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
        if type(downloaded) is not bytes or not (0 < len(downloaded) <= maximum_bytes):
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)

        return ObservedTerminalBindingObject(
            object_name=object_name,
            generation=observed_generation,
            byte_count=len(downloaded),
            sha256=hashlib.sha256(downloaded).hexdigest(),
            content_bytes=downloaded,
        )

    def read_current(
        self, *, bucket: str, object_name: str, maximum_bytes: int
    ) -> ObservedTerminalBindingObject:
        bucket = self._validated_read_request(
            bucket=bucket, object_name=object_name, maximum_bytes=maximum_bytes
        )

        failed = False
        observed_object: ObservedTerminalBindingObject | None = None
        try:
            observed_object = self._observe_then_pin(
                bucket=bucket, object_name=object_name, maximum_bytes=maximum_bytes
            )
        except Exception:
            failed = True
        if failed or observed_object is None:
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
        return observed_object

    def read_current_optional(
        self, *, bucket: str, object_name: str, maximum_bytes: int
    ) -> ObservedTerminalBindingObject | None:
        bucket = self._validated_read_request(
            bucket=bucket, object_name=object_name, maximum_bytes=maximum_bytes
        )

        not_found = False
        failed = False
        observed_object: ObservedTerminalBindingObject | None = None
        try:
            observed_object = self._observe_then_pin(
                bucket=bucket, object_name=object_name, maximum_bytes=maximum_bytes
            )
        except Exception as error:
            if NotFound is not None and isinstance(error, NotFound):
                not_found = True
            else:
                failed = True
        if not_found:
            return None
        if failed or observed_object is None:
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
        return observed_object


@dataclass(frozen=True, slots=True)
class SealedPromotedOperationalTerminalBinding:
    record: PromotedOperationalTerminalBindingRecord
    bucket: str
    published: PublishedStateObject

    def __post_init__(self) -> None:
        if type(self.record) is not PromotedOperationalTerminalBindingRecord:
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
        self.record.verify_content_identity()
        bucket = _validate_bucket(self.bucket)
        object.__setattr__(self, "bucket", bucket)
        if type(self.published) is not PublishedStateObject:
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
        payload = encode_promoted_operational_terminal_binding_record(self.record)
        if (
            self.published.byte_count != len(payload)
            or self.published.sha256 != hashlib.sha256(payload).hexdigest()
        ):
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)


def seal_promoted_operational_terminal_binding(
    *,
    terminal: PromotedOperationalTerminalRecord,
    spec: PromotedOperationalRunSpec,
    bucket: str,
    writer: StateObjectWriter,
) -> SealedPromotedOperationalTerminalBinding:
    """Seal one create-once binding object for this exact spec/terminal
    pair through the existing StateObjectWriter port.

    A writer conflict against different pre-existing bytes propagates as a
    sanitized failure and is never retried, overwritten, deleted, or
    repaired -- ``writer.create_or_verify`` is called exactly once.
    """

    bucket = _validate_bucket(bucket)

    build_failed = False
    record: PromotedOperationalTerminalBindingRecord | None = None
    payload = b""
    object_name = ""
    try:
        record = build_promoted_operational_terminal_binding_record(terminal, spec)
        payload = encode_promoted_operational_terminal_binding_record(record)
        object_name = promoted_operational_terminal_binding_object_name(spec)
    except Exception:
        build_failed = True
    if build_failed or record is None:
        raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)

    expected_sha256 = hashlib.sha256(payload).hexdigest()
    write_failed = False
    result: object = None
    try:
        result = writer.create_or_verify(
            bucket=bucket,
            object_name=object_name,
            content_bytes=payload,
            content_type="application/json",
            maximum_bytes=MAXIMUM_TERMINAL_BINDING_BYTES,
        )
    except Exception:
        write_failed = True
    if write_failed:
        raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)

    if (
        type(result) is not PublishedStateObject
        or result.object_name != object_name
        or result.byte_count != len(payload)
        or result.sha256 != expected_sha256
    ):
        raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)

    construct_failed = False
    sealed: SealedPromotedOperationalTerminalBinding | None = None
    try:
        sealed = SealedPromotedOperationalTerminalBinding(
            record=record, bucket=bucket, published=result
        )
    except Exception:
        construct_failed = True
    if construct_failed or sealed is None:
        raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
    return sealed


@dataclass(frozen=True, slots=True)
class LoadedPromotedOperationalTerminalBinding:
    binding: TrustedPromotedOperationalTerminalBinding
    record: PromotedOperationalTerminalBindingRecord
    bucket: str
    object_name: str
    generation: int
    sha256: str
    byte_count: int

    def __post_init__(self) -> None:
        if type(self.binding) is not TrustedPromotedOperationalTerminalBinding:
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
        if type(self.record) is not PromotedOperationalTerminalBindingRecord:
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
        self.record.verify_content_identity()
        bucket = _validate_bucket(self.bucket)
        object.__setattr__(self, "bucket", bucket)
        if type(self.object_name) is not str or not self.object_name:
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
        if (
            type(self.generation) is bool
            or type(self.generation) is not int
            or not (1 <= self.generation <= _MAXIMUM_GENERATION)
        ):
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
        if type(self.byte_count) is not int or self.byte_count <= 0:
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
        if type(self.sha256) is not str or len(self.sha256) != 64:
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
        if (
            self.binding.spec_id != self.record.spec_id
            or self.binding.expected_terminal_id != self.record.expected_terminal_id
        ):
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)


def _validated_bucket_and_object_name(
    *, spec: PromotedOperationalRunSpec, bucket: str
) -> tuple[str, str]:
    bucket = _validate_bucket(bucket)
    name_failed = False
    object_name = ""
    try:
        if type(spec) is not PromotedOperationalRunSpec:
            raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
        spec.verify_content_identity()
        object_name = promoted_operational_terminal_binding_object_name(spec)
    except Exception:
        name_failed = True
    if name_failed:
        raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
    return bucket, object_name


def _verify_and_build_loaded_binding(
    *,
    observed: object,
    spec: PromotedOperationalRunSpec,
    object_name: str,
    bucket: str,
) -> LoadedPromotedOperationalTerminalBinding:
    """The exact verification path shared by both the mandatory and
    optional load functions, so the two can never drift: independently
    re-verify the observed object, decode the record strictly, project it
    through ``trusted_binding_from_record``, and construct the final
    result."""

    if (
        type(observed) is not ObservedTerminalBindingObject
        or observed.object_name != object_name
        or observed.byte_count != len(observed.content_bytes)
        or observed.sha256 != hashlib.sha256(observed.content_bytes).hexdigest()
    ):
        raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)

    decode_failed = False
    record: PromotedOperationalTerminalBindingRecord | None = None
    binding: TrustedPromotedOperationalTerminalBinding | None = None
    try:
        record = decode_promoted_operational_terminal_binding_record(observed.content_bytes)
        binding = trusted_binding_from_record(record, spec)
    except Exception:
        decode_failed = True
    if decode_failed or record is None or binding is None:
        raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)

    construct_failed = False
    loaded: LoadedPromotedOperationalTerminalBinding | None = None
    try:
        loaded = LoadedPromotedOperationalTerminalBinding(
            binding=binding,
            record=record,
            bucket=bucket,
            object_name=observed.object_name,
            generation=observed.generation,
            sha256=observed.sha256,
            byte_count=observed.byte_count,
        )
    except Exception:
        construct_failed = True
    if construct_failed or loaded is None:
        raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
    return loaded


def load_trusted_promoted_operational_terminal_binding(
    *,
    spec: PromotedOperationalRunSpec,
    bucket: str,
    reader: PromotedTerminalBindingObjectReader,
) -> LoadedPromotedOperationalTerminalBinding:
    """Load and independently re-verify exactly one sealed binding object
    for this exact live spec.

    There is no optional/None variant: absence, permission failure,
    malformed content, oversized payload, a generation change between
    observation and download, or any spec mismatch all raise one
    sanitized PromotedTerminalBindingControlPlaneError. Absence is never
    treated as a benign case, and this function never derives, infers, or
    reconstructs a binding from anything other than the sealed remote
    object it reads.
    """

    bucket, object_name = _validated_bucket_and_object_name(spec=spec, bucket=bucket)

    read_failed = False
    observed: object = None
    try:
        observed = reader.read_current(
            bucket=bucket,
            object_name=object_name,
            maximum_bytes=MAXIMUM_TERMINAL_BINDING_BYTES,
        )
    except Exception:
        read_failed = True
    if read_failed:
        raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)

    return _verify_and_build_loaded_binding(
        observed=observed, spec=spec, object_name=object_name, bucket=bucket
    )


def load_optional_trusted_promoted_operational_terminal_binding(
    *,
    spec: PromotedOperationalRunSpec,
    bucket: str,
    reader: PromotedTerminalBindingObjectReader,
) -> LoadedPromotedOperationalTerminalBinding | None:
    """Identical to ``load_trusted_promoted_operational_terminal_binding``
    except it returns ``None`` ONLY when ``reader.read_current_optional``
    itself proved the object does not exist (returned ``None`` without
    raising). Every other failure -- malformed content, oversized payload,
    a generation change, a spec mismatch, or any other error -- still
    raises the sanitized PromotedTerminalBindingControlPlaneError and is
    never treated as absence.
    """

    bucket, object_name = _validated_bucket_and_object_name(spec=spec, bucket=bucket)

    read_failed = False
    observed: object = None
    try:
        observed = reader.read_current_optional(
            bucket=bucket,
            object_name=object_name,
            maximum_bytes=MAXIMUM_TERMINAL_BINDING_BYTES,
        )
    except Exception:
        read_failed = True
    if read_failed:
        raise PromotedTerminalBindingControlPlaneError(_ERR_CONTROL_PLANE)
    if observed is None:
        return None

    return _verify_and_build_loaded_binding(
        observed=observed, spec=spec, object_name=object_name, bucket=bucket
    )
