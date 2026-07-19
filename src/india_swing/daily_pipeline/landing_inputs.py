from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol

from .acquisition import AcquiredFile, LandingManifestObjectRequest, LandingObjectRequest
from .landing_manifest import (
    LandingManifestVerifier,
    TrustedLandingManifestBinding,
    VerifiedLandingManifest,
)
from .landing_manifest_acquisition import AcquiredLandingManifest


class LandingInputError(ValueError):
    pass


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")

_ERR_MANIFEST = "landing input manifest is invalid"
_ERR_SESSION = "landing input market session is invalid"
_ERR_SESSION_MISMATCH = "landing input market session does not match the verified manifest"
_ERR_CUTOFF = "landing input run cutoff is invalid"
_ERR_CUTOFF_ORDER = "landing input run cutoff precedes the manifest knowledge time or binding cutoff"
_ERR_ACQUIRED_SHAPE = "landing input acquired object is invalid"
_ERR_ACQUIRED_MISMATCH = "landing input acquired object does not match its manifest request"
_ERR_ACQUIRED_CONTENT = "landing input acquired object content could not be verified"
_ERR_MANIFEST_ACQUISITION = "landing input manifest acquisition is invalid"
_ERR_MANIFEST_ACQUISITION_MISMATCH = (
    "landing input manifest acquisition does not match the verified manifest"
)


class LandingObjectReader(Protocol):
    """Reads exactly one already-verified landing object request.

    The only capability is read(request) -> AcquiredFile. There is no
    listing method and no "latest object" resolution anywhere on this
    interface.
    """

    def read(self, request: LandingObjectRequest) -> AcquiredFile: ...


def _verify_acquired_matches_request(
    acquired: AcquiredFile, request: LandingObjectRequest
) -> None:
    if type(acquired) is not AcquiredFile:
        raise LandingInputError(_ERR_ACQUIRED_SHAPE)
    if type(request) is not LandingObjectRequest:
        raise LandingInputError(_ERR_ACQUIRED_SHAPE)
    if type(acquired.bucket) is not str or acquired.bucket != request.bucket:
        raise LandingInputError(_ERR_ACQUIRED_MISMATCH)
    if type(acquired.object_name) is not str or acquired.object_name != request.object_name:
        raise LandingInputError(_ERR_ACQUIRED_MISMATCH)
    if (
        type(acquired.generation) is bool
        or type(acquired.generation) is not int
        or acquired.generation != request.generation
    ):
        raise LandingInputError(_ERR_ACQUIRED_MISMATCH)
    if (
        type(acquired.target_session) is not date
        or acquired.target_session != request.target_session
    ):
        raise LandingInputError(_ERR_ACQUIRED_MISMATCH)
    if acquired.file_type is not request.file_type:
        raise LandingInputError(_ERR_ACQUIRED_MISMATCH)

    content_bytes = acquired.content_bytes
    if type(content_bytes) is not bytes or len(content_bytes) == 0:
        raise LandingInputError(_ERR_ACQUIRED_CONTENT)
    if type(acquired.sha256_hash) is not str or _SHA256.fullmatch(acquired.sha256_hash) is None:
        raise LandingInputError(_ERR_ACQUIRED_CONTENT)
    if hashlib.sha256(content_bytes).hexdigest() != acquired.sha256_hash:
        raise LandingInputError(_ERR_ACQUIRED_CONTENT)
    if acquired.sha256_hash != request.expected_sha256:
        raise LandingInputError(_ERR_ACQUIRED_CONTENT)


def _verify_manifest_acquisition(
    manifest_acquisition: AcquiredLandingManifest | None, manifest: VerifiedLandingManifest
) -> AcquiredLandingManifest | None:
    """Independently reconstructs and reverifies an optional manifest
    acquisition record, returning a defensively rebuilt snapshot decoupled
    from the caller's original object (or None when omitted).

    Duplicated deliberately rather than trusting AcquiredLandingManifest's
    own prior __post_init__ validation, since a frozen dataclass can be
    mutated after construction via object.__setattr__. The acquisition's
    own retained binding must be exactly TrustedLandingManifestBinding --
    not a subclass, proxy, or equality-poisoned impostor carrying the same
    attribute names -- before any of its fields are read at all, so a
    shaped impostor whose __eq__ always returns True cannot be laundered
    into a real binding by reconstruction and then accepted by value
    comparison. The request and (now type-verified) binding are both
    reconstructed from their primitive fields before the manifest is
    reverified from manifest_bytes against that reconstructed binding, so a
    malformed nested binding field (wrong type, poisoned comparison,
    missing attribute) is caught by TrustedLandingManifestBinding's own
    validation instead of reaching an unguarded comparison inside
    LandingManifestVerifier.verify.

    Every ordinary failure along this path (never BaseException) collapses
    to one static, sanitized error; the only exception is the final,
    distinct mismatch raised when a well-formed, independently reverified
    acquisition simply belongs to a different outer manifest.
    """

    if manifest_acquisition is None:
        return None

    try:
        if type(manifest_acquisition) is not AcquiredLandingManifest:
            raise ValueError("acquisition must be exact")

        request = manifest_acquisition.request
        acquired_manifest = manifest_acquisition.manifest
        if type(request) is not LandingManifestObjectRequest:
            raise ValueError("acquisition request must be exact")
        if type(acquired_manifest) is not VerifiedLandingManifest:
            raise ValueError("acquisition manifest must be exact")

        request_snapshot = LandingManifestObjectRequest(
            bucket=request.bucket,
            object_name=request.object_name,
            generation=request.generation,
            target_session=request.target_session,
        )

        binding = acquired_manifest.binding
        if type(binding) is not TrustedLandingManifestBinding:
            raise ValueError("acquisition manifest binding must be exact")
        binding_snapshot = TrustedLandingManifestBinding(
            expected_manifest_sha256=binding.expected_manifest_sha256,
            allowed_bucket=binding.allowed_bucket,
            target_session=binding.target_session,
            not_before=binding.not_before,
            cutoff=binding.cutoff,
        )

        manifest_snapshot = LandingManifestVerifier().verify(
            acquired_manifest.manifest_bytes, binding_snapshot
        )

        # Fail closed rather than silently repair: if the acquisition's own
        # manifest attribute no longer agrees with a fresh reverification of
        # its own trusted bytes and reconstructed binding, treat it as
        # corrupted instead of proceeding on the (correct) reverified value
        # while acquired_manifest itself stays tampered.
        if acquired_manifest != manifest_snapshot:
            raise ValueError("acquisition manifest does not match its own reverified content")
        if request_snapshot.bucket != manifest_snapshot.binding.allowed_bucket:
            raise ValueError("acquisition request bucket does not match the manifest")
        if request_snapshot.target_session != manifest_snapshot.target_session:
            raise ValueError("acquisition request target session does not match the manifest")

        acquisition_snapshot = AcquiredLandingManifest(
            request=request_snapshot, manifest=manifest_snapshot
        )
    except Exception:
        raise LandingInputError(_ERR_MANIFEST_ACQUISITION) from None

    if manifest_snapshot != manifest:
        raise LandingInputError(_ERR_MANIFEST_ACQUISITION_MISMATCH)

    return acquisition_snapshot


def _verify_temporal_bounds(
    manifest: VerifiedLandingManifest, market_session: date, run_cutoff: datetime
) -> None:
    if type(manifest) is not VerifiedLandingManifest:
        raise LandingInputError(_ERR_MANIFEST)
    if type(market_session) is not date:
        raise LandingInputError(_ERR_SESSION)
    if market_session != manifest.target_session:
        raise LandingInputError(_ERR_SESSION_MISMATCH)
    if type(run_cutoff) is not datetime:
        raise LandingInputError(_ERR_CUTOFF)
    if run_cutoff.tzinfo is None or run_cutoff.utcoffset() is None:
        raise LandingInputError(_ERR_CUTOFF)
    if manifest.knowledge_time > run_cutoff:
        raise LandingInputError(_ERR_CUTOFF_ORDER)
    if manifest.binding.cutoff > run_cutoff:
        raise LandingInputError(_ERR_CUTOFF_ORDER)


@dataclass(frozen=True, slots=True)
class VerifiedLandingInputs:
    """One daily run's exact, verified security-master and daily-bundle
    inputs, bound to the VerifiedLandingManifest and run_cutoff that
    authorized acquiring them.

    __post_init__ repeats every applicable session, temporal,
    request-lineage, and raw-byte hash check performed during
    acquisition, so a buggy reader or direct construction cannot
    silently produce an inconsistent instance.
    """

    manifest: VerifiedLandingManifest
    market_session: date
    run_cutoff: datetime
    security_master: AcquiredFile
    daily_bundle: AcquiredFile
    manifest_acquisition: AcquiredLandingManifest | None = None

    def __post_init__(self) -> None:
        _verify_temporal_bounds(self.manifest, self.market_session, self.run_cutoff)
        _verify_acquired_matches_request(self.security_master, self.manifest.security_master)
        _verify_acquired_matches_request(self.daily_bundle, self.manifest.daily_bundle)
        manifest_acquisition_snapshot = _verify_manifest_acquisition(
            self.manifest_acquisition, self.manifest
        )
        object.__setattr__(self, "manifest_acquisition", manifest_acquisition_snapshot)


def acquire_verified_landing_inputs(
    *,
    manifest: VerifiedLandingManifest,
    market_session: date,
    run_cutoff: datetime,
    reader: LandingObjectReader,
    manifest_acquisition: AcquiredLandingManifest | None = None,
) -> VerifiedLandingInputs:
    """Reads exactly the two objects named by an already-verified manifest.

    Never lists a bucket, never selects a "latest" object, never retries,
    falls back, or substitutes a second source. A reader failure on
    either read propagates unchanged; the second read is only ever
    attempted after the first one succeeds.

    When manifest_acquisition is supplied, it is independently validated
    against manifest before either data-object read (so an invalid or
    mismatched acquisition record never reaches the reader), and only a
    defensively reconstructed snapshot of it -- decoupled from the
    caller's original object -- is retained on the returned value.
    """

    _verify_temporal_bounds(manifest, market_session, run_cutoff)
    manifest_acquisition_snapshot = _verify_manifest_acquisition(manifest_acquisition, manifest)

    security_master = reader.read(manifest.security_master)
    _verify_acquired_matches_request(security_master, manifest.security_master)

    daily_bundle = reader.read(manifest.daily_bundle)
    _verify_acquired_matches_request(daily_bundle, manifest.daily_bundle)

    return VerifiedLandingInputs(
        manifest=manifest,
        market_session=market_session,
        run_cutoff=run_cutoff,
        security_master=security_master,
        daily_bundle=daily_bundle,
        manifest_acquisition=manifest_acquisition_snapshot,
    )
