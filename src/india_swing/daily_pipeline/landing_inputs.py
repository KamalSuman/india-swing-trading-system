from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol

from .acquisition import AcquiredFile, LandingObjectRequest
from .landing_manifest import VerifiedLandingManifest


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

    def __post_init__(self) -> None:
        _verify_temporal_bounds(self.manifest, self.market_session, self.run_cutoff)
        _verify_acquired_matches_request(self.security_master, self.manifest.security_master)
        _verify_acquired_matches_request(self.daily_bundle, self.manifest.daily_bundle)


def acquire_verified_landing_inputs(
    *,
    manifest: VerifiedLandingManifest,
    market_session: date,
    run_cutoff: datetime,
    reader: LandingObjectReader,
) -> VerifiedLandingInputs:
    """Reads exactly the two objects named by an already-verified manifest.

    Never lists a bucket, never selects a "latest" object, never retries,
    falls back, or substitutes a second source. A reader failure on
    either read propagates unchanged; the second read is only ever
    attempted after the first one succeeds.
    """

    _verify_temporal_bounds(manifest, market_session, run_cutoff)

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
    )
