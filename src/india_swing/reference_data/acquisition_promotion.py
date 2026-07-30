from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime

from india_swing.identity import content_id
from india_swing.reference.models import ReferenceReadiness

from .acquisition_join import VerifiedReferenceAcquisitionJoin
from .acquisition_receipt import TrustedReferenceAcquisitionBinding
from .codec import encode_security_master
from .models import (
    NSE_CM_SECURITY_PARSER_VERSION,
    NSE_CM_SECURITY_SCOPE_POLICY_VERSION,
    REFERENCE_NORMALIZED_CODEC_VERSION,
    AcquisitionMode,
    ParsedNseCmSecurityMaster,
    StoredReferenceArtifact,
)


class ReferenceArtifactPromotionError(ValueError):
    pass


def _reference_data_artifact_store():
    """Import lazily, mirroring acquisition_receipt.py/acquisition_join.py's
    own helper for the same package-init-time circular-import hazard."""

    from . import artifact_store

    return artifact_store


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")

REFERENCE_ARTIFACT_PROMOTION_SCHEMA_VERSION = "reference-artifact-promotion/v2"

_ERR_TYPE = "reference artifact promotion type is invalid"
_ERR_SCHEMA_VERSION = "reference artifact promotion schema version is unsupported"
_ERR_JOIN = "reference artifact promotion join is invalid"
_ERR_ARTIFACT = "reference artifact promotion source artifact is invalid"
_ERR_BINDING = "reference artifact promotion trusted binding is invalid"
_ERR_PROVENANCE = (
    "reference artifact promotion source provenance could not be verified"
)
_ERR_SOURCE_STATE = (
    "reference artifact promotion source manifest is not in its original "
    "collection-only state"
)
_ERR_LINEAGE = (
    "reference artifact promotion lineage disagrees between the trusted join "
    "and the sealed source artifact"
)
_ERR_NORMALIZED = (
    "reference artifact promotion normalized bytes disagree with the sealed "
    "source artifact"
)
_ERR_DERIVED_FIELD = "reference artifact promotion derived field is invalid"
_ERR_PROMOTION_ID = (
    "reference artifact promotion identifier disagrees with independently "
    "recomputed content"
)


def _require_source_state(manifest) -> None:
    """The source manifest must remain exactly the original collection-only
    state the manual importer produces. ReferenceArtifactManifest's own
    construction-time validation already forbids constructing most other
    combinations; this independently replays that requirement so a
    post-construction mutation of a sealed manifest also fails closed.
    """

    if (
        manifest.acquisition_mode is not AcquisitionMode.UNVERIFIED_MANUAL_FILE
        or manifest.readiness is not ReferenceReadiness.COLLECTION_ONLY
        or manifest.actionable is not False
        or manifest.verified_report_date is not None
        or manifest.publication_time_status != "UNVERIFIED_MANUAL_FILE"
    ):
        raise ReferenceArtifactPromotionError(_ERR_SOURCE_STATE)


def _require_lineage_agreement(
    join: VerifiedReferenceAcquisitionJoin, artifact: StoredReferenceArtifact
) -> None:
    """Independently prove exact equality between the trusted join and the
    sealed source artifact across every byte, semantic fact, version, and
    count this promotion is allowed to rely on.
    """

    manifest = artifact.manifest
    parsed = join.parsed
    receipt = join.receipt

    if type(artifact.parsed) is not ParsedNseCmSecurityMaster or artifact.parsed != parsed:
        raise ReferenceArtifactPromotionError(_ERR_LINEAGE)
    if type(artifact.raw_bytes) is not bytes or artifact.raw_bytes != join.acquired_file.content_bytes:
        raise ReferenceArtifactPromotionError(_ERR_LINEAGE)

    observed_raw_sha256 = hashlib.sha256(artifact.raw_bytes).hexdigest()
    if observed_raw_sha256 != parsed.raw_sha256 or observed_raw_sha256 != manifest.raw_sha256:
        raise ReferenceArtifactPromotionError(_ERR_LINEAGE)
    if (
        len(artifact.raw_bytes) != parsed.compressed_byte_count
        or len(artifact.raw_bytes) != manifest.compressed_byte_count
    ):
        raise ReferenceArtifactPromotionError(_ERR_LINEAGE)

    if (
        manifest.claimed_report_date != receipt.report_date
        or manifest.claimed_report_date != parsed.claimed_report_date
    ):
        raise ReferenceArtifactPromotionError(_ERR_LINEAGE)
    if manifest.original_filename != parsed.original_filename:
        raise ReferenceArtifactPromotionError(_ERR_LINEAGE)
    if manifest.claimed_authority != receipt.authority:
        raise ReferenceArtifactPromotionError(_ERR_LINEAGE)
    if manifest.claimed_download_url != receipt.requested_url:
        raise ReferenceArtifactPromotionError(_ERR_LINEAGE)
    if manifest.source_media_type != receipt.response_media_type:
        raise ReferenceArtifactPromotionError(_ERR_LINEAGE)

    if manifest.parser_version != NSE_CM_SECURITY_PARSER_VERSION:
        raise ReferenceArtifactPromotionError(_ERR_LINEAGE)
    if manifest.source_schema_version != parsed.source_schema_version:
        raise ReferenceArtifactPromotionError(_ERR_LINEAGE)
    if manifest.scope_policy_version != NSE_CM_SECURITY_SCOPE_POLICY_VERSION:
        raise ReferenceArtifactPromotionError(_ERR_LINEAGE)
    if manifest.normalized_codec_version != REFERENCE_NORMALIZED_CODEC_VERSION:
        raise ReferenceArtifactPromotionError(_ERR_LINEAGE)

    if manifest.uncompressed_sha256 != parsed.uncompressed_sha256:
        raise ReferenceArtifactPromotionError(_ERR_LINEAGE)
    if manifest.header_sha256 != parsed.header_sha256:
        raise ReferenceArtifactPromotionError(_ERR_LINEAGE)
    if manifest.ordered_row_digest != parsed.ordered_row_digest:
        raise ReferenceArtifactPromotionError(_ERR_LINEAGE)

    parsed_row_count = len(parsed.records)
    if manifest.raw_row_count != parsed_row_count or manifest.parsed_row_count != parsed_row_count:
        raise ReferenceArtifactPromotionError(_ERR_LINEAGE)
    if (
        manifest.retained_unverified_equity_count != parsed.retained_unverified_equity_count
        or manifest.excluded_non_equity_count != parsed.excluded_non_equity_count
        or manifest.excluded_test_security_count != parsed.excluded_test_security_count
        or manifest.excluded_alternative_venue_count != parsed.excluded_alternative_venue_count
    ):
        raise ReferenceArtifactPromotionError(_ERR_LINEAGE)


def _recomputed_normalized_sha256(
    join: VerifiedReferenceAcquisitionJoin, artifact: StoredReferenceArtifact
) -> str:
    """Recompute encode_security_master(join.parsed) and its SHA-256, rather
    than trusting the retained normalized bytes or manifest.normalized_sha256.
    """

    recomputed_bytes = encode_security_master(join.parsed)
    if type(artifact.normalized_bytes) is not bytes or recomputed_bytes != artifact.normalized_bytes:
        raise ReferenceArtifactPromotionError(_ERR_NORMALIZED)
    recomputed_sha256 = hashlib.sha256(recomputed_bytes).hexdigest()
    if recomputed_sha256 != artifact.manifest.normalized_sha256:
        raise ReferenceArtifactPromotionError(_ERR_NORMALIZED)
    return recomputed_sha256


def _promotion_identity(
    join: VerifiedReferenceAcquisitionJoin,
    artifact: StoredReferenceArtifact,
    normalized_sha256: str,
    verified_report_date: date,
    knowledge_time: datetime,
) -> dict[str, object]:
    """The complete canonical mapping promotion_id is derived from.

    Binds schema, join_id, receipt hash, source artifact_id, source
    manifest_id, trusted raw/normalized/uncompressed/header/ordered-row
    hashes, report date, knowledge time, every field of the exact trusted
    TrustedReferenceAcquisitionBinding this receipt was verified against,
    and the fixed promoted acquisition mode/readiness/actionable facts --
    no filesystem path, mtime, repr, or runtime identity.

    Content-binding the trusted binding itself (not merely the receipt it
    produced) is deliberate: the binding is a local trust-root input, not
    something independently authenticated by the receipt's own bytes. A
    still-individually-valid change to any binding field (for example
    widening ``cutoff``) must therefore produce a different promotion_id
    rather than silently redefining the trust boundary under the same
    identity.
    """

    binding = join.receipt.binding
    if type(binding) is not TrustedReferenceAcquisitionBinding:
        raise ReferenceArtifactPromotionError(_ERR_BINDING)

    return {
        "schema_version": REFERENCE_ARTIFACT_PROMOTION_SCHEMA_VERSION,
        "join_id": join.join_id,
        "receipt_sha256": join.receipt.receipt_sha256,
        "source_artifact_id": artifact.manifest.artifact_id,
        "source_manifest_id": artifact.manifest.manifest_id,
        "raw_sha256": join.parsed.raw_sha256,
        "normalized_sha256": normalized_sha256,
        "uncompressed_sha256": join.parsed.uncompressed_sha256,
        "header_sha256": join.parsed.header_sha256,
        "ordered_row_digest": join.parsed.ordered_row_digest,
        "report_date": verified_report_date,
        "knowledge_time": knowledge_time,
        "trusted_binding_expected_receipt_sha256": binding.expected_receipt_sha256,
        "trusted_binding_expected_raw_sha256": binding.expected_raw_sha256,
        "trusted_binding_allowed_bucket": binding.allowed_bucket,
        "trusted_binding_target_report_date": binding.target_report_date,
        "trusted_binding_not_before": binding.not_before,
        "trusted_binding_cutoff": binding.cutoff,
        "trusted_binding_trusted_acquirer_id": binding.trusted_acquirer_id,
        "promoted_acquisition_mode": AcquisitionMode.TRUSTED_PINNED_GCS_RECEIPT,
        "promoted_readiness": ReferenceReadiness.POINT_IN_TIME_VERIFIED,
        "actionable": False,
    }


def _build_promotion_facts(
    join: VerifiedReferenceAcquisitionJoin,
    artifact: StoredReferenceArtifact,
) -> tuple[str, date, datetime]:
    """The single strict promotion derivation routine.

    Never trusts artifact fields merely because a LocalReferenceArtifactStore
    constructed them: requires exact types, verifies the join's own content
    identity, verifies the artifact's sealed on-disk provenance, requires the
    source manifest to remain in its original collection-only state,
    independently proves lineage agreement between the join and the sealed
    artifact, recomputes the normalized encoding and its hash, and returns
    the deterministic promotion_id plus the two derived promotion facts. Never
    constructs VerifiedReferenceArtifactPromotion -- this is the one
    promotion decoder both ReferenceArtifactPromotionService.promote and
    VerifiedReferenceArtifactPromotion's own defensive check call.

    The retained join and artifact graphs are treated as untrusted at every
    replay: every step below deliberately catches ordinary ``Exception``
    (never ``BaseException``, so ``KeyboardInterrupt``/``SystemExit`` still
    propagate) so a malformed nested field -- an invalid path type, a
    ``None``/impostor manifest, or a wrong-typed byte field the artifact
    store itself does not defensively guard against -- fails closed with one
    static sanitized error instead of leaking a raw nested exception.
    """

    if type(join) is not VerifiedReferenceAcquisitionJoin:
        raise ReferenceArtifactPromotionError(_ERR_JOIN)
    if type(artifact) is not StoredReferenceArtifact:
        raise ReferenceArtifactPromotionError(_ERR_ARTIFACT)

    try:
        join.verify_content_identity()
    except ReferenceArtifactPromotionError:
        raise
    except Exception:
        raise ReferenceArtifactPromotionError(_ERR_JOIN) from None

    artifact_store = _reference_data_artifact_store()
    try:
        artifact_store.verify_stored_reference_provenance(artifact)
    except ReferenceArtifactPromotionError:
        raise
    except Exception:
        raise ReferenceArtifactPromotionError(_ERR_PROVENANCE) from None

    try:
        _require_source_state(artifact.manifest)
        _require_lineage_agreement(join, artifact)
        normalized_sha256 = _recomputed_normalized_sha256(join, artifact)

        verified_report_date = join.receipt.report_date
        knowledge_time = join.receipt.acquired_at

        promotion_id = content_id(
            _promotion_identity(
                join, artifact, normalized_sha256, verified_report_date, knowledge_time
            ),
            length=64,
        )
    except ReferenceArtifactPromotionError:
        raise
    except Exception:
        raise ReferenceArtifactPromotionError(_ERR_LINEAGE) from None

    return promotion_id, verified_report_date, knowledge_time


@dataclass(frozen=True, slots=True)
class VerifiedReferenceArtifactPromotion:
    """Immutable, content-addressed evidence that one sealed collection-only
    security-master artifact is exactly the artifact represented by one
    already-verified acquisition join.

    ``POINT_IN_TIME_VERIFIED`` on this record means only that this exact
    security-master artifact vintage has independently pinned acquisition
    provenance. It does not establish stable cross-vintage identity,
    calendar, universe, surveillance, liquidity, prices, corporate actions,
    model validation, alert eligibility, or profitability, and it never
    rewrites, upgrades, or replaces the original sealed collection archive.
    __post_init__ calls verify_content_identity(), which requires the exact
    concrete type (rejecting subclasses/impostors even when every retained
    field is otherwise valid) and independently replays the retained join's
    own defensive check, the artifact's sealed provenance, the complete
    join/artifact comparison, and every derived promotion field and
    promotion_id -- so a caller cannot bypass verification by
    hand-assembling an instance whose typed fields disagree with what the
    retained join and sealed artifact actually prove.
    """

    schema_version: str
    join: VerifiedReferenceAcquisitionJoin
    artifact: StoredReferenceArtifact
    promotion_id: str
    promoted_acquisition_mode: AcquisitionMode
    promoted_readiness: ReferenceReadiness
    verified_report_date: date
    knowledge_time: datetime
    actionable: bool

    def __post_init__(self) -> None:
        self.verify_content_identity()

    def verify_content_identity(self) -> None:
        if type(self) is not VerifiedReferenceArtifactPromotion:
            raise ReferenceArtifactPromotionError(_ERR_TYPE)
        if (
            type(self.schema_version) is not str
            or self.schema_version != REFERENCE_ARTIFACT_PROMOTION_SCHEMA_VERSION
        ):
            raise ReferenceArtifactPromotionError(_ERR_SCHEMA_VERSION)
        if type(self.join) is not VerifiedReferenceAcquisitionJoin:
            raise ReferenceArtifactPromotionError(_ERR_JOIN)
        if type(self.artifact) is not StoredReferenceArtifact:
            raise ReferenceArtifactPromotionError(_ERR_ARTIFACT)
        if (
            type(self.promoted_acquisition_mode) is not AcquisitionMode
            or self.promoted_acquisition_mode is not AcquisitionMode.TRUSTED_PINNED_GCS_RECEIPT
        ):
            raise ReferenceArtifactPromotionError(_ERR_DERIVED_FIELD)
        if (
            type(self.promoted_readiness) is not ReferenceReadiness
            or self.promoted_readiness is not ReferenceReadiness.POINT_IN_TIME_VERIFIED
        ):
            raise ReferenceArtifactPromotionError(_ERR_DERIVED_FIELD)
        if type(self.actionable) is not bool or self.actionable is not False:
            raise ReferenceArtifactPromotionError(_ERR_DERIVED_FIELD)
        if type(self.verified_report_date) is not date:
            raise ReferenceArtifactPromotionError(_ERR_DERIVED_FIELD)
        if type(self.knowledge_time) is not datetime:
            raise ReferenceArtifactPromotionError(_ERR_DERIVED_FIELD)
        if type(self.promotion_id) is not str or _SHA256.fullmatch(self.promotion_id) is None:
            raise ReferenceArtifactPromotionError(_ERR_PROMOTION_ID)

        promotion_id, verified_report_date, knowledge_time = _build_promotion_facts(
            self.join, self.artifact
        )

        if self.verified_report_date != verified_report_date:
            raise ReferenceArtifactPromotionError(_ERR_DERIVED_FIELD)
        if self.knowledge_time != knowledge_time:
            raise ReferenceArtifactPromotionError(_ERR_DERIVED_FIELD)
        if self.promotion_id != promotion_id:
            raise ReferenceArtifactPromotionError(_ERR_PROMOTION_ID)


class ReferenceArtifactPromotionService:
    """Promotes one already-verified acquisition join and its sealed source
    artifact into an immutable point-in-time provenance-promotion record.

    Never writes, replaces, renames, or otherwise mutates the source archive;
    the original collection-only artifact remains sealed exactly as
    imported. Produces evidence only -- no signal, recommendation,
    notification, order, broker, or capital authority, and no change to the
    stored artifact's own AcquisitionMode, readiness, or actionable flag.
    """

    def promote(
        self,
        join: VerifiedReferenceAcquisitionJoin,
        artifact: StoredReferenceArtifact,
    ) -> VerifiedReferenceArtifactPromotion:
        promotion_id, verified_report_date, knowledge_time = _build_promotion_facts(
            join, artifact
        )
        return VerifiedReferenceArtifactPromotion(
            schema_version=REFERENCE_ARTIFACT_PROMOTION_SCHEMA_VERSION,
            join=join,
            artifact=artifact,
            promotion_id=promotion_id,
            promoted_acquisition_mode=AcquisitionMode.TRUSTED_PINNED_GCS_RECEIPT,
            promoted_readiness=ReferenceReadiness.POINT_IN_TIME_VERIFIED,
            verified_report_date=verified_report_date,
            knowledge_time=knowledge_time,
            actionable=False,
        )
