from __future__ import annotations

import hashlib
from dataclasses import dataclass

from .acquisition import GCSObjectPayload, GCSObjectReader, LandingManifestObjectRequest
from .landing_manifest import (
    MAXIMUM_LANDING_MANIFEST_BYTES,
    LandingManifestVerifier,
    TrustedLandingManifestBinding,
    VerifiedLandingManifest,
)


class LandingManifestAcquisitionError(Exception):
    pass


_ERR_REQUEST_BINDING = "landing manifest acquisition request/binding validation failed"
_ERR_READ = "landing manifest acquisition read failed"
_ERR_PAYLOAD = "landing manifest acquisition payload validation failed"
_ERR_VERIFICATION = "landing manifest acquisition verification failed"
_ERR_RESULT = "landing manifest acquisition result validation failed"


@dataclass(frozen=True, slots=True)
class AcquiredLandingManifest:
    """One acquisition-time snapshot pairing the exact GCS source request
    that produced a landing manifest with its independently verified
    content.

    VerifiedLandingManifest alone discards the manifest object's bucket,
    canonical object_name, and pinned generation once verification
    succeeds; this wrapper retains that exact source lineage alongside the
    verified bytes/hash/binding, without duplicating any of those fields.
    There is no default, so this cannot be constructed without both an
    exact request and an exact manifest.

    __post_init__ independently re-derives manifest from its own retained
    manifest_bytes and binding (rather than trusting that a caller
    assembled a self-consistent instance correctly, mirroring the
    defensive-reconstruction pattern VerifiedLandingManifest and
    LandingInputLineage already use elsewhere in this package), and
    cross-checks request against that reverified manifest's bucket and
    session.
    """

    request: LandingManifestObjectRequest
    manifest: VerifiedLandingManifest

    def __post_init__(self) -> None:
        try:
            if type(self.request) is not LandingManifestObjectRequest:
                raise ValueError("acquisition result request must be exact")
            if type(self.manifest) is not VerifiedLandingManifest:
                raise ValueError("acquisition result manifest must be exact")
            LandingManifestObjectRequest(
                bucket=self.request.bucket,
                object_name=self.request.object_name,
                generation=self.request.generation,
                target_session=self.request.target_session,
            )
            reverified = LandingManifestVerifier().verify(
                self.manifest.manifest_bytes, self.manifest.binding
            )
            if reverified != self.manifest:
                raise ValueError("acquisition result manifest does not match its own content")
            if self.request.bucket != self.manifest.binding.allowed_bucket:
                raise ValueError("acquisition result request bucket does not match the manifest")
            if self.request.target_session != self.manifest.target_session:
                raise ValueError(
                    "acquisition result request target session does not match the manifest"
                )
            if self.request.target_session != self.manifest.binding.target_session:
                raise ValueError(
                    "acquisition result request target session does not match the manifest binding"
                )
        except Exception:
            raise LandingManifestAcquisitionError(_ERR_RESULT) from None


def acquire_verified_landing_manifest(
    request: LandingManifestObjectRequest,
    binding: TrustedLandingManifestBinding,
    reader: GCSObjectReader,
) -> AcquiredLandingManifest:
    """Reads exactly one explicit, generation-pinned landing-manifest object
    and verifies it against binding, returning an AcquiredLandingManifest
    that retains both the exact GCS source request and the verified manifest.

    Never constructs a GCS/storage client, never lists a bucket, never
    selects a "latest" object, never retries or falls back to a second
    source, and never reads an environment variable or the current clock.

    At function entry, request and binding are independently reconstructed
    into request_snapshot and binding_snapshot -- new, field-copied
    instances decoupled from the caller's original objects -- and
    cross-checked (bucket and target_session must agree). Every subsequent
    step -- the reader call, the payload-generation check, the expected-hash
    check, the verifier call, and the returned wrapper -- uses only these
    snapshots and never re-reads the caller's original request or binding.
    This closes a validation-to-use race: a reader (or concurrent caller)
    that mutates the original request/binding objects in place via
    object.__setattr__ while read_generation is running cannot retroactively
    change which bucket/object/generation was requested or which hash a
    downloaded payload is checked against, so a reader cannot make a
    tampered payload acceptable by mutating the original binding's expected
    hash during the read.

    Ordinary failures (never BaseException) at each of these stages --
    request/binding validation, the reader call, payload validation, and
    manifest verification -- are collapsed into one static, stage-specific
    LandingManifestAcquisitionError with chaining suppressed, so neither
    bucket/object names, generations, hashes, manifest bytes, nor nested
    exception text can leak through this boundary.
    """

    try:
        if type(request) is not LandingManifestObjectRequest:
            raise ValueError("acquisition request must be exact")
        if type(binding) is not TrustedLandingManifestBinding:
            raise ValueError("acquisition binding must be exact")
        request_snapshot = LandingManifestObjectRequest(
            bucket=request.bucket,
            object_name=request.object_name,
            generation=request.generation,
            target_session=request.target_session,
        )
        binding_snapshot = TrustedLandingManifestBinding(
            expected_manifest_sha256=binding.expected_manifest_sha256,
            allowed_bucket=binding.allowed_bucket,
            target_session=binding.target_session,
            not_before=binding.not_before,
            cutoff=binding.cutoff,
        )
        if request_snapshot.bucket != binding_snapshot.allowed_bucket:
            raise ValueError("acquisition bucket does not match the trusted binding")
        if request_snapshot.target_session != binding_snapshot.target_session:
            raise ValueError("acquisition target session does not match the trusted binding")
    except Exception:
        raise LandingManifestAcquisitionError(_ERR_REQUEST_BINDING) from None

    try:
        payload = reader.read_generation(
            bucket=request_snapshot.bucket,
            object_name=request_snapshot.object_name,
            generation=request_snapshot.generation,
            maximum_bytes=MAXIMUM_LANDING_MANIFEST_BYTES,
        )
    except Exception:
        raise LandingManifestAcquisitionError(_ERR_READ) from None

    try:
        if type(payload) is not GCSObjectPayload:
            raise ValueError("acquisition reader returned an invalid payload")
        if (
            type(payload.generation) is not int
            or payload.generation != request_snapshot.generation
        ):
            raise ValueError(
                "acquisition object generation does not match the requested generation"
            )
        content_bytes = payload.content_bytes
        if type(content_bytes) is not bytes or len(content_bytes) == 0:
            raise ValueError("acquisition object content must be non-empty bytes")
        if len(content_bytes) > MAXIMUM_LANDING_MANIFEST_BYTES:
            raise ValueError("acquisition object exceeds the maximum allowed size")
        if hashlib.sha256(content_bytes).hexdigest() != binding_snapshot.expected_manifest_sha256:
            raise ValueError("acquisition object SHA-256 does not match the expected digest")
    except Exception:
        raise LandingManifestAcquisitionError(_ERR_PAYLOAD) from None

    try:
        verified_manifest = LandingManifestVerifier().verify(content_bytes, binding_snapshot)
    except Exception:
        raise LandingManifestAcquisitionError(_ERR_VERIFICATION) from None

    return AcquiredLandingManifest(request=request_snapshot, manifest=verified_manifest)
