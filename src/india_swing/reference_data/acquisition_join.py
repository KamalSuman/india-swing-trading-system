from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from pathlib import PurePath
from typing import TYPE_CHECKING

from india_swing.identity import content_id

from .acquisition_receipt import (
    MAXIMUM_RAW_BYTES,
    ReferenceAcquisitionReceiptError,
    VerifiedReferenceAcquisitionReceipt,
)
from .models import (
    NSE_CM_SECURITY_PARSER_VERSION,
    NSE_CM_SECURITY_SCOPE_POLICY_VERSION,
    ParsedNseCmSecurityMaster,
    ReferenceArtifactIntegrityError,
)
from .security_master import NseCmSecurityMasterParser

if TYPE_CHECKING:
    from india_swing.daily_pipeline.acquisition import AcquiredFile, GCSLandingObjectReader


class ReferenceAcquisitionJoinError(ValueError):
    pass


def _daily_pipeline_acquisition():
    """Import lazily, mirroring acquisition_receipt.py's own helper.

    Importing daily_pipeline.acquisition at module scope inside the
    reference_data package can complete a latent circular-import cycle
    (reference_data -> daily_pipeline -> daily_reports ->
    reference_data.models) before reference_data's own package body has
    finished initializing. Deferring the import to first actual use avoids
    reintroducing that hazard.
    """

    from india_swing.daily_pipeline import acquisition

    return acquisition


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")

REFERENCE_ACQUISITION_JOIN_SCHEMA_VERSION = "reference-acquisition-join/v1"

_ERR_SCHEMA_VERSION = "reference acquisition join schema version is unsupported"
_ERR_RECEIPT = "reference acquisition join receipt is invalid"
_ERR_READER = "reference acquisition join reader is invalid"
_ERR_ACQUISITION_READ = "reference acquisition join could not read the pinned landing object"
_ERR_ACQUIRED_FILE = "reference acquisition join acquired file is invalid"
_ERR_LINEAGE = "reference acquisition join lineage disagrees with the verified receipt"
_ERR_RAW_BYTES = "reference acquisition join raw bytes are invalid"
_ERR_RAW_HASH = "reference acquisition join raw hash disagrees with the verified receipt"
_ERR_FILENAME = "reference acquisition join filename disagrees with the verified receipt"
_ERR_PARSE = (
    "reference acquisition join could not independently parse the acquired security master"
)
_ERR_REPORT_DATE = "reference acquisition join report date disagrees with the verified receipt"
_ERR_ALTERNATIVE_VENUE = (
    "reference acquisition join content includes interoperability alternative-venue rows"
)
_ERR_PARSED_IDENTITY = (
    "reference acquisition join retained parsed facts disagree with independently "
    "reparsed content"
)
_ERR_JOIN_ID = (
    "reference acquisition join identifier disagrees with independently recomputed content"
)


def _join_identity(
    receipt: VerifiedReferenceAcquisitionReceipt,
    acquired_file: "AcquiredFile",
    parsed: ParsedNseCmSecurityMaster,
) -> dict[str, object]:
    """The complete canonical mapping join_id is derived from.

    Binds schema, receipt hash, raw hash and byte count, GCS
    bucket/object/generation, target report date, parser/source/scope
    semantics, header/uncompressed/ordered-row hashes, and every row/
    disposition count -- no repr, object identity, or filesystem path.
    """

    return {
        "schema_version": REFERENCE_ACQUISITION_JOIN_SCHEMA_VERSION,
        "receipt_sha256": receipt.receipt_sha256,
        "raw_sha256": parsed.raw_sha256,
        "raw_byte_count": parsed.compressed_byte_count,
        "bucket": acquired_file.bucket,
        "object_name": acquired_file.object_name,
        "generation": acquired_file.generation,
        "target_report_date": receipt.report_date,
        "parser_version": NSE_CM_SECURITY_PARSER_VERSION,
        "source_schema_version": parsed.source_schema_version,
        "scope_policy_version": NSE_CM_SECURITY_SCOPE_POLICY_VERSION,
        "header_sha256": parsed.header_sha256,
        "uncompressed_sha256": parsed.uncompressed_sha256,
        "ordered_row_digest": parsed.ordered_row_digest,
        "row_count": len(parsed.records),
        "retained_unverified_equity_count": parsed.retained_unverified_equity_count,
        "excluded_non_equity_count": parsed.excluded_non_equity_count,
        "excluded_test_security_count": parsed.excluded_test_security_count,
        "excluded_alternative_venue_count": parsed.excluded_alternative_venue_count,
    }


def _build_join_facts(
    receipt: VerifiedReferenceAcquisitionReceipt,
    acquired_file: "AcquiredFile",
) -> tuple[ParsedNseCmSecurityMaster, str]:
    """The single strict join derivation routine.

    Never trusts acquired_file fields merely because a GCSLandingObjectReader
    constructed them: independently recomputes the raw hash and compares
    every lineage field to receipt.landing_object and receipt's own raw
    facts, derives the parser filename only from the canonical final
    component of receipt.landing_object.object_name, freshly reparses the
    retained bytes with a new NseCmSecurityMasterParser, and returns plain
    normalized facts plus the deterministic join_id. Never constructs
    VerifiedReferenceAcquisitionJoin -- this is the one join decoder both
    ReferenceAcquisitionJoinService.join and
    VerifiedReferenceAcquisitionJoin's own defensive check call, so
    defensive validation always compares against these plain values rather
    than trusting a caller-assembled typed instance.
    """

    if type(receipt) is not VerifiedReferenceAcquisitionReceipt:
        raise ReferenceAcquisitionJoinError(_ERR_RECEIPT)

    acquisition = _daily_pipeline_acquisition()
    if type(acquired_file) is not acquisition.AcquiredFile:
        raise ReferenceAcquisitionJoinError(_ERR_ACQUIRED_FILE)
    if (
        type(acquired_file.bucket) is not str
        or type(acquired_file.object_name) is not str
        or type(acquired_file.generation) is not int
        or type(acquired_file.sha256_hash) is not str
        or type(acquired_file.target_session) is not date
        or type(acquired_file.file_type) is not acquisition.AcquisitionFileType
    ):
        raise ReferenceAcquisitionJoinError(_ERR_ACQUIRED_FILE)

    landing_object = receipt.landing_object
    if (
        acquired_file.bucket != landing_object.bucket
        or acquired_file.object_name != landing_object.object_name
        or acquired_file.generation != landing_object.generation
        or acquired_file.target_session != landing_object.target_session
        or acquired_file.file_type is not landing_object.file_type
    ):
        raise ReferenceAcquisitionJoinError(_ERR_LINEAGE)

    content_bytes = acquired_file.content_bytes
    if type(content_bytes) is not bytes or len(content_bytes) == 0:
        raise ReferenceAcquisitionJoinError(_ERR_RAW_BYTES)
    if len(content_bytes) > MAXIMUM_RAW_BYTES:
        raise ReferenceAcquisitionJoinError(_ERR_RAW_BYTES)
    if len(content_bytes) != receipt.raw_byte_count:
        raise ReferenceAcquisitionJoinError(_ERR_RAW_BYTES)

    observed_raw_sha256 = hashlib.sha256(content_bytes).hexdigest()
    if (
        observed_raw_sha256 != acquired_file.sha256_hash
        or observed_raw_sha256 != receipt.raw_sha256
    ):
        raise ReferenceAcquisitionJoinError(_ERR_RAW_HASH)

    original_filename = PurePath(landing_object.object_name).name
    expected_filename = f"NSE_CM_security_{receipt.report_date.strftime('%d%m%Y')}.csv.gz"
    if original_filename != expected_filename:
        raise ReferenceAcquisitionJoinError(_ERR_FILENAME)

    parser = NseCmSecurityMasterParser()
    try:
        parsed = parser.parse_bytes(content_bytes, original_filename=original_filename)
    except ReferenceArtifactIntegrityError:
        raise ReferenceAcquisitionJoinError(_ERR_PARSE) from None

    if parsed.claimed_report_date != receipt.report_date:
        raise ReferenceAcquisitionJoinError(_ERR_REPORT_DATE)
    if parsed.excluded_alternative_venue_count != 0:
        raise ReferenceAcquisitionJoinError(_ERR_ALTERNATIVE_VENUE)

    join_id = content_id(_join_identity(receipt, acquired_file, parsed), length=64)
    return parsed, join_id


@dataclass(frozen=True, slots=True)
class VerifiedReferenceAcquisitionJoin:
    """Immutable, content-addressed evidence joining one verified NSE
    acquisition receipt to its exact GCS-pinned raw bytes and an
    independently reparsed security master.

    Carries no readiness, actionability, AcquisitionMode, promotion,
    signal, recommendation, notification, order, broker, or capital field:
    this is evidence assembly only. __post_init__ calls
    verify_content_identity(), which independently replays the retained
    receipt's own defensive check plus every join-specific lineage/parse/
    join_id check, so a caller cannot bypass verification by
    hand-assembling an instance whose typed fields disagree with what the
    retained receipt and acquired bytes actually prove.
    """

    schema_version: str
    receipt: VerifiedReferenceAcquisitionReceipt
    acquired_file: "AcquiredFile"
    parsed: ParsedNseCmSecurityMaster
    join_id: str

    def __post_init__(self) -> None:
        self.verify_content_identity()

    def verify_content_identity(self) -> None:
        """Replay every defensive check and require exact agreement.

        Requires the exact schema marker and exact VerifiedReferenceAcquisitionReceipt
        type, independently replays the receipt's own verify_content_identity
        (detecting mutation anywhere inside the receipt, its binding, or its
        landing object), then independently re-derives the join facts via
        _build_join_facts from self.receipt and self.acquired_file and
        requires exact type-and-value agreement with the retained parsed
        result and join_id. Detects post-construction object.__setattr__
        mutation of any top-level field or nested receipt/acquired-file/
        parsed-record field.
        """

        if (
            type(self.schema_version) is not str
            or self.schema_version != REFERENCE_ACQUISITION_JOIN_SCHEMA_VERSION
        ):
            raise ReferenceAcquisitionJoinError(_ERR_SCHEMA_VERSION)
        if type(self.receipt) is not VerifiedReferenceAcquisitionReceipt:
            raise ReferenceAcquisitionJoinError(_ERR_RECEIPT)
        try:
            self.receipt.verify_content_identity()
        except ReferenceAcquisitionReceiptError:
            raise ReferenceAcquisitionJoinError(_ERR_RECEIPT) from None

        if type(self.join_id) is not str or _SHA256.fullmatch(self.join_id) is None:
            raise ReferenceAcquisitionJoinError(_ERR_JOIN_ID)

        parsed, join_id = _build_join_facts(self.receipt, self.acquired_file)

        if type(self.parsed) is not type(parsed) or self.parsed != parsed:
            raise ReferenceAcquisitionJoinError(_ERR_PARSED_IDENTITY)
        if self.join_id != join_id:
            raise ReferenceAcquisitionJoinError(_ERR_JOIN_ID)


class ReferenceAcquisitionJoinService:
    """Joins one verified acquisition receipt to its exact GCS bytes and an
    independently reparsed security master.

    Verifies the receipt before any read. Calls the injected exact
    GCSLandingObjectReader exactly once per join, pinned to
    receipt.landing_object; never lists a bucket, never selects a "latest"
    object, and never constructs its own GCS/network client. Produces
    immutable, content-addressed evidence only -- no AcquisitionMode,
    readiness, promotion, signal, or trading authority.
    """

    def __init__(self, reader: "GCSLandingObjectReader") -> None:
        acquisition = _daily_pipeline_acquisition()
        if type(reader) is not acquisition.GCSLandingObjectReader:
            raise ReferenceAcquisitionJoinError(_ERR_READER)
        self._reader = reader

    def join(
        self, receipt: VerifiedReferenceAcquisitionReceipt
    ) -> VerifiedReferenceAcquisitionJoin:
        if type(receipt) is not VerifiedReferenceAcquisitionReceipt:
            raise ReferenceAcquisitionJoinError(_ERR_RECEIPT)
        try:
            receipt.verify_content_identity()
        except ReferenceAcquisitionReceiptError:
            raise ReferenceAcquisitionJoinError(_ERR_RECEIPT) from None

        try:
            acquired_file = self._reader.read(receipt.landing_object)
        except Exception:
            raise ReferenceAcquisitionJoinError(_ERR_ACQUISITION_READ) from None

        parsed, join_id = _build_join_facts(receipt, acquired_file)

        return VerifiedReferenceAcquisitionJoin(
            schema_version=REFERENCE_ACQUISITION_JOIN_SCHEMA_VERSION,
            receipt=receipt,
            acquired_file=acquired_file,
            parsed=parsed,
            join_id=join_id,
        )
