from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field, fields
from datetime import date, datetime, timedelta, timezone

from india_swing.identity import content_id

from .acquisition import AcquiredFile, AcquisitionError, AcquisitionFileType, LandingObjectRequest
from .landing_inputs import VerifiedLandingInputs
from .landing_manifest import (
    LandingManifestError,
    LandingManifestVerifier,
    TrustedLandingManifestBinding,
    VerifiedLandingManifest,
)

LANDING_INPUT_LINEAGE_SCHEMA_VERSION = "nse-cm-landing-input-lineage/v1"

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET_NAME = re.compile(r"[a-z0-9][a-z0-9\-_.]{1,61}[a-z0-9]\Z")

_ERR_OBJECT_LINEAGE = "landing lineage object could not be verified"
_ERR_SCHEMA_VERSION = "landing lineage schema version is unsupported"
_ERR_MANIFEST_HASH = "landing lineage manifest hash is invalid"
_ERR_TIME = "landing lineage time is invalid"
_ERR_TIME_ORDER = "landing lineage time ordering is invalid"
_ERR_SESSION = "landing lineage target session is invalid"
_ERR_SESSION_MISMATCH = "landing lineage target session does not match its objects"
_ERR_OBJECT_ROLE = "landing lineage object role is invalid"
_ERR_INPUTS = "landing lineage inputs value is invalid"
_ERR_INPUTS_MISMATCH = "landing lineage inputs could not be independently verified"


class LandingLineageError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class LandingObjectLineage:
    """Immutable, content-addressable lineage for one landing object.

    __post_init__ reconstructs an exact LandingObjectRequest from these
    fields so canonical object paths, positive signed-int64 generation,
    session binding, file type, bucket, and lowercase SHA-256 remain
    governed by that single existing authority rather than a second,
    possibly-drifting copy of the same rules.
    """

    file_type: AcquisitionFileType
    bucket: str
    object_name: str
    generation: int
    target_session: date
    sha256_hash: str

    def __post_init__(self) -> None:
        try:
            LandingObjectRequest(
                bucket=self.bucket,
                object_name=self.object_name,
                generation=self.generation,
                expected_sha256=self.sha256_hash,
                target_session=self.target_session,
                file_type=self.file_type,
            )
        except AcquisitionError:
            raise LandingLineageError(_ERR_OBJECT_LINEAGE) from None


@dataclass(frozen=True, slots=True)
class LandingInputLineage:
    """One immutable, content-addressed lineage projection of a
    VerifiedLandingInputs value, intended for later persistence in
    DailyPipelineRun. Carries no raw manifest bytes, acquired content
    bytes, reader, credentials, or mutable mapping.
    """

    schema_version: str
    manifest_sha256: str
    manifest_knowledge_time: datetime
    binding_not_before: datetime
    binding_cutoff: datetime
    target_session: date
    security_master: LandingObjectLineage
    daily_bundle: LandingObjectLineage
    lineage_id: str = field(init=False)

    def __post_init__(self) -> None:
        if self.schema_version != LANDING_INPUT_LINEAGE_SCHEMA_VERSION:
            raise LandingLineageError(_ERR_SCHEMA_VERSION)
        if not isinstance(self.manifest_sha256, str) or _SHA256.fullmatch(self.manifest_sha256) is None:
            raise LandingLineageError(_ERR_MANIFEST_HASH)

        for value in (self.manifest_knowledge_time, self.binding_not_before, self.binding_cutoff):
            if type(value) is not datetime:
                raise LandingLineageError(_ERR_TIME)
            if value.tzinfo is None or value.utcoffset() is None:
                raise LandingLineageError(_ERR_TIME)
        object.__setattr__(
            self, "manifest_knowledge_time", self.manifest_knowledge_time.astimezone(timezone.utc)
        )
        object.__setattr__(self, "binding_not_before", self.binding_not_before.astimezone(timezone.utc))
        object.__setattr__(self, "binding_cutoff", self.binding_cutoff.astimezone(timezone.utc))
        if not (self.binding_not_before <= self.manifest_knowledge_time <= self.binding_cutoff):
            raise LandingLineageError(_ERR_TIME_ORDER)

        if type(self.target_session) is not date:
            raise LandingLineageError(_ERR_SESSION)

        if (
            type(self.security_master) is not LandingObjectLineage
            or self.security_master.file_type is not AcquisitionFileType.SECURITY_MASTER
        ):
            raise LandingLineageError(_ERR_OBJECT_ROLE)
        if (
            type(self.daily_bundle) is not LandingObjectLineage
            or self.daily_bundle.file_type is not AcquisitionFileType.DAILY_BUNDLE
        ):
            raise LandingLineageError(_ERR_OBJECT_ROLE)
        if (
            self.security_master.target_session != self.target_session
            or self.daily_bundle.target_session != self.target_session
        ):
            raise LandingLineageError(_ERR_SESSION_MISMATCH)

        object.__setattr__(self, "lineage_id", self._calculated_lineage_id())

    def _identity_material(self) -> dict[str, object]:
        return {
            value.name: getattr(self, value.name)
            for value in fields(self)
            if value.name != "lineage_id"
        }

    def _calculated_lineage_id(self) -> str:
        return content_id(self._identity_material(), length=64)


def _verify_acquired_object(acquired: AcquiredFile, request: LandingObjectRequest) -> None:
    """Independently re-derives the acquired-object guarantee from scratch.

    Duplicated deliberately rather than reused from landing_inputs.py: this
    module must not assume that VerifiedLandingInputs's own prior
    __post_init__ validation still holds, since a frozen dataclass can be
    mutated after construction via object.__setattr__.
    """

    if type(acquired) is not AcquiredFile:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)
    if type(request) is not LandingObjectRequest:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)
    if type(acquired.bucket) is not str or acquired.bucket != request.bucket:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)
    if type(acquired.object_name) is not str or acquired.object_name != request.object_name:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)
    if (
        type(acquired.generation) is bool
        or type(acquired.generation) is not int
        or acquired.generation != request.generation
    ):
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)
    if (
        type(acquired.target_session) is not date
        or acquired.target_session != request.target_session
    ):
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)
    if acquired.file_type is not request.file_type:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)

    content_bytes = acquired.content_bytes
    if type(content_bytes) is not bytes or len(content_bytes) == 0:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)
    if type(acquired.sha256_hash) is not str or _SHA256.fullmatch(acquired.sha256_hash) is None:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)
    if hashlib.sha256(content_bytes).hexdigest() != acquired.sha256_hash:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)
    if acquired.sha256_hash != request.expected_sha256:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)


def _object_lineage_from(request: LandingObjectRequest) -> LandingObjectLineage:
    """Builds the lineage record from the reverified request, not the
    acquired object's own metadata, so the externally hash-bound manifest
    bytes remain authoritative through the final projection even though
    _verify_acquired_object has already confirmed the two agree.
    """

    return LandingObjectLineage(
        file_type=request.file_type,
        bucket=request.bucket,
        object_name=request.object_name,
        generation=request.generation,
        target_session=request.target_session,
        sha256_hash=request.expected_sha256,
    )


def build_landing_input_lineage(inputs: VerifiedLandingInputs) -> LandingInputLineage:
    """Projects an immutable, content-addressed lineage record from an
    already-verified VerifiedLandingInputs value.

    Discards manifest_bytes and acquired content_bytes; retains only
    canonical identifiers. Performs no filesystem, environment, clock,
    network, GCP SDK, store, listing/latest, retry, or persistence
    operation, and never derives trust from the input's own prior
    validation alone: every relevant guarantee is independently
    re-checked here before any byte is discarded.
    """

    if type(inputs) is not VerifiedLandingInputs:
        raise LandingLineageError(_ERR_INPUTS)

    manifest = inputs.manifest
    if type(manifest) is not VerifiedLandingManifest:
        raise LandingLineageError(_ERR_INPUTS)

    binding = manifest.binding
    if type(binding) is not TrustedLandingManifestBinding:
        raise LandingLineageError(_ERR_INPUTS)

    if type(manifest.schema_version) is not int or manifest.schema_version != 1:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)

    manifest_bytes = manifest.manifest_bytes
    if type(manifest_bytes) is not bytes or len(manifest_bytes) == 0:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)

    manifest_sha256 = manifest.manifest_sha256
    if type(manifest_sha256) is not str or _SHA256.fullmatch(manifest_sha256) is None:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)

    binding_expected_sha256 = binding.expected_manifest_sha256
    if type(binding_expected_sha256) is not str or _SHA256.fullmatch(binding_expected_sha256) is None:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)

    if hashlib.sha256(manifest_bytes).hexdigest() != manifest_sha256:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)
    if manifest_sha256 != binding_expected_sha256:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)

    # LandingManifestVerifier.verify() dereferences binding.target_session,
    # binding.not_before, and binding.cutoff directly without re-checking
    # their shape, on the assumption that TrustedLandingManifestBinding's
    # own __post_init__ still holds. A binding mutated in place via
    # object.__setattr__ after construction can violate that assumption, so
    # this module must re-validate those fields itself before calling
    # verify(), the same way it already treats every other frozen value it
    # did not just construct.
    if type(binding.target_session) is not date:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)
    if (
        type(binding.not_before) is not datetime
        or binding.not_before.tzinfo is None
        or binding.not_before.utcoffset() is None
    ):
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)
    if (
        type(binding.cutoff) is not datetime
        or binding.cutoff.tzinfo is None
        or binding.cutoff.utcoffset() is None
    ):
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)

    manifest_knowledge_time = manifest.knowledge_time
    if (
        type(manifest_knowledge_time) is not datetime
        or manifest_knowledge_time.tzinfo is None
        or manifest_knowledge_time.utcoffset() != timedelta(0)
    ):
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)

    # manifest_bytes and binding are now known-safe exact values. Reparsing
    # manifest_bytes against binding here is the sole object-request
    # authority from this point on: a structured field on `manifest`
    # mutated in place via object.__setattr__ after its own __post_init__
    # ran cannot change what is encoded in manifest_bytes, so re-deriving
    # from those trusted bytes cannot inherit such a mutation.
    try:
        reverified = LandingManifestVerifier().verify(manifest_bytes, binding)
    except LandingManifestError:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH) from None

    # Fail closed rather than silently repair: if the manifest object this
    # unit was handed no longer agrees with a fresh reverification of its
    # own trusted bytes, treat it as corrupted instead of proceeding on the
    # (correct) reverified values while `manifest` itself stays tampered.
    if (
        manifest.schema_version != reverified.schema_version
        or manifest_sha256 != reverified.manifest_sha256
        or manifest.knowledge_time != reverified.knowledge_time
        or manifest.target_session != reverified.target_session
        or manifest.security_master != reverified.security_master
        or manifest.daily_bundle != reverified.daily_bundle
    ):
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)

    manifest_target_session = reverified.target_session
    binding_target_session = binding.target_session
    market_session = inputs.market_session
    if (
        type(manifest_target_session) is not date
        or type(binding_target_session) is not date
        or type(market_session) is not date
    ):
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)
    if manifest_target_session != binding_target_session or manifest_target_session != market_session:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)

    knowledge_time = reverified.knowledge_time
    if (
        type(knowledge_time) is not datetime
        or knowledge_time.tzinfo is None
        or knowledge_time.utcoffset() != timedelta(0)
    ):
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)

    not_before = binding.not_before
    cutoff = binding.cutoff
    run_cutoff = inputs.run_cutoff
    for value in (not_before, cutoff, run_cutoff):
        if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
            raise LandingLineageError(_ERR_INPUTS_MISMATCH)
    if not (not_before <= knowledge_time <= cutoff <= run_cutoff):
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)

    allowed_bucket = binding.allowed_bucket
    if type(allowed_bucket) is not str or _BUCKET_NAME.fullmatch(allowed_bucket) is None:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)

    security_master_request = reverified.security_master
    daily_bundle_request = reverified.daily_bundle
    if (
        type(security_master_request) is not LandingObjectRequest
        or type(daily_bundle_request) is not LandingObjectRequest
    ):
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)
    if security_master_request.file_type is not AcquisitionFileType.SECURITY_MASTER:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)
    if daily_bundle_request.file_type is not AcquisitionFileType.DAILY_BUNDLE:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)
    if (
        security_master_request.target_session != manifest_target_session
        or daily_bundle_request.target_session != manifest_target_session
    ):
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)
    if security_master_request.bucket != allowed_bucket or daily_bundle_request.bucket != allowed_bucket:
        raise LandingLineageError(_ERR_INPUTS_MISMATCH)

    _verify_acquired_object(inputs.security_master, security_master_request)
    _verify_acquired_object(inputs.daily_bundle, daily_bundle_request)

    return LandingInputLineage(
        schema_version=LANDING_INPUT_LINEAGE_SCHEMA_VERSION,
        manifest_sha256=reverified.manifest_sha256,
        manifest_knowledge_time=knowledge_time,
        binding_not_before=not_before,
        binding_cutoff=cutoff,
        target_session=manifest_target_session,
        security_master=_object_lineage_from(security_master_request),
        daily_bundle=_object_lineage_from(daily_bundle_request),
    )
