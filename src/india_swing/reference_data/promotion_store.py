"""Durable, exact-ID root store for VerifiedReferenceArtifactPromotion.

Persistence here is a replay boundary only.  A stored manifest is never
trusted as authority: every read resolves the pinned sealed reference
artifact, independently reconstructs the trusted binding, the verified
receipt, the acquisition join (through a private in-process reader that
serves only the artifact's own already-sealed raw bytes for the exact
receipt-pinned bucket/object/generation -- never GCP, network, or listing),
and the promotion, then requires exact agreement with the retained manifest
before returning anything.
"""

from __future__ import annotations

import base64
import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from india_swing._filesystem import (
    FileLockUnavailable,
    FileSafetyError,
    advisory_file_lock,
    read_stable_regular_file,
)
from india_swing.daily_pipeline.acquisition import (
    GCSLandingObjectReader,
    GCSObjectPayload,
)

from .acquisition_join import ReferenceAcquisitionJoinService
from .acquisition_promotion import (
    ReferenceArtifactPromotionService,
    VerifiedReferenceArtifactPromotion,
)
from .acquisition_receipt import (
    MAXIMUM_RECEIPT_BYTES,
    ReferenceAcquisitionReceiptVerifier,
    TrustedReferenceAcquisitionBinding,
)
from .artifact_store import LocalReferenceArtifactStore
from .models import StoredReferenceArtifact


class ReferenceArtifactPromotionStoreError(ValueError):
    pass


class ReferenceArtifactPromotionStoreConflict(ReferenceArtifactPromotionStoreError):
    pass


class ReferenceArtifactPromotionStoreNotFound(ReferenceArtifactPromotionStoreError):
    pass


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET_NAME = re.compile(r"[a-z0-9][a-z0-9\-_.]{1,61}[a-z0-9]\Z")
_CANONICAL_DATE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")

REFERENCE_ARTIFACT_PROMOTION_STORE_CODEC_VERSION = (
    "reference-artifact-promotion-store-json/v1"
)
_KIND = "REFERENCE_ARTIFACT_PROMOTION"
_MAXIMUM_MANIFEST_BYTES = 256 * 1024
_STORE_DIRECTORY = "promotions"
_LOCK_FILENAME = ".reference-artifact-promotion.lock"

_MANIFEST_KEYS = frozenset(
    {
        "codec_schema_version",
        "kind",
        "promotion_id",
        "join_id",
        "artifact_id",
        "manifest_id",
        "raw_sha256",
        "normalized_sha256",
        "receipt_bytes_base64",
        "binding",
    }
)
_BINDING_KEYS = frozenset(
    {
        "expected_receipt_sha256",
        "expected_raw_sha256",
        "allowed_bucket",
        "target_report_date",
        "not_before",
        "cutoff",
        "trusted_acquirer_id",
    }
)

_ERR_TYPE = "reference artifact promotion store type is invalid"
_ERR_VERIFY = (
    "reference artifact promotion store could not verify the supplied promotion"
)
_ERR_ARTIFACT = (
    "reference artifact promotion store could not resolve its sealed source artifact"
)
_ERR_RECEIPT = "reference artifact promotion store could not verify its receipt"
_ERR_READER = "reference artifact promotion store reader request is invalid"
_ERR_JOIN = "reference artifact promotion store could not verify its join"
_ERR_PROMOTION = "reference artifact promotion store could not verify its promotion"
_ERR_REPLAY = "reference artifact promotion store replay disagrees with stored content"
_ERR_PATH_IDENTITY = "reference artifact promotion store path identity is invalid"
_ERR_CONFLICT = "reference artifact promotion store already has different content"
_ERR_STORE_UNAVAILABLE = "reference artifact promotion store is unavailable"
_ERR_UNSAFE_PATH = "reference artifact promotion store path is unsafe"
_ERR_NOT_FOUND = "reference artifact promotion was not found"
_ERR_BYTES = "reference artifact promotion manifest bytes are invalid"
_ERR_UTF8 = "reference artifact promotion manifest is not valid UTF-8"
_ERR_JSON = "reference artifact promotion manifest is not valid JSON"
_ERR_SHAPE = "reference artifact promotion manifest shape is invalid"
_ERR_BASE64 = "reference artifact promotion manifest receipt encoding is invalid"
_ERR_NONCANONICAL = "reference artifact promotion manifest is not canonical"


def _require_sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise ReferenceArtifactPromotionStoreError(_ERR_SHAPE)
    return value


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ReferenceArtifactPromotionStoreError(_ERR_SHAPE)
        result[key] = value
    return result


def _reject_number(_token: str) -> object:
    raise ReferenceArtifactPromotionStoreError(_ERR_SHAPE)


def _canonical_bytes(value: dict[str, object]) -> bytes:
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


def _canonical_date(value: object) -> date:
    if type(value) is not str or _CANONICAL_DATE.fullmatch(value) is None:
        raise ReferenceArtifactPromotionStoreError(_ERR_SHAPE)
    try:
        result = date.fromisoformat(value)
    except ValueError:
        raise ReferenceArtifactPromotionStoreError(_ERR_SHAPE) from None
    if result.isoformat() != value:
        raise ReferenceArtifactPromotionStoreError(_ERR_SHAPE)
    return result


def _canonical_datetime(value: object) -> datetime:
    if type(value) is not str:
        raise ReferenceArtifactPromotionStoreError(_ERR_SHAPE)
    try:
        result = datetime.fromisoformat(value)
    except ValueError:
        raise ReferenceArtifactPromotionStoreError(_ERR_SHAPE) from None
    offset = result.utcoffset()
    if (
        result.tzinfo is None
        or offset is None
        or offset.total_seconds() != 0
        or result.isoformat() != value
    ):
        raise ReferenceArtifactPromotionStoreError(_ERR_SHAPE)
    return result


def _is_link_like(path: Path) -> bool:
    is_junction = getattr(path, "is_junction", None)
    if path.is_symlink() or bool(is_junction and is_junction()):
        return True
    try:
        status = os.lstat(path)
    except OSError:
        return False
    attribute = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    return bool(getattr(status, "st_file_attributes", 0) & attribute)


@dataclass(frozen=True, slots=True)
class _DecodedPromotionRecord:
    """Plain normalized manifest facts. Never a VerifiedReferenceArtifactPromotion."""

    promotion_id: str
    join_id: str
    artifact_id: str
    manifest_id: str
    raw_sha256: str
    normalized_sha256: str
    receipt_bytes: bytes
    binding: TrustedReferenceAcquisitionBinding


def _record_for(promotion: VerifiedReferenceArtifactPromotion) -> _DecodedPromotionRecord:
    manifest = promotion.artifact.manifest
    return _DecodedPromotionRecord(
        promotion_id=promotion.promotion_id,
        join_id=promotion.join.join_id,
        artifact_id=manifest.artifact_id,
        manifest_id=manifest.manifest_id,
        raw_sha256=manifest.raw_sha256,
        normalized_sha256=manifest.normalized_sha256,
        receipt_bytes=promotion.join.receipt.receipt_bytes,
        binding=promotion.join.receipt.binding,
    )


def encode_promotion_manifest(record: _DecodedPromotionRecord) -> bytes:
    if type(record) is not _DecodedPromotionRecord:
        raise TypeError("promotion store record must be exact")
    binding = record.binding
    if type(binding) is not TrustedReferenceAcquisitionBinding:
        raise ReferenceArtifactPromotionStoreError(_ERR_SHAPE)
    if type(record.receipt_bytes) is not bytes:
        raise ReferenceArtifactPromotionStoreError(_ERR_SHAPE)
    return _canonical_bytes(
        {
            "codec_schema_version": REFERENCE_ARTIFACT_PROMOTION_STORE_CODEC_VERSION,
            "kind": _KIND,
            "promotion_id": record.promotion_id,
            "join_id": record.join_id,
            "artifact_id": record.artifact_id,
            "manifest_id": record.manifest_id,
            "raw_sha256": record.raw_sha256,
            "normalized_sha256": record.normalized_sha256,
            "receipt_bytes_base64": base64.b64encode(record.receipt_bytes).decode("ascii"),
            "binding": {
                "expected_receipt_sha256": binding.expected_receipt_sha256,
                "expected_raw_sha256": binding.expected_raw_sha256,
                "allowed_bucket": binding.allowed_bucket,
                "target_report_date": binding.target_report_date.isoformat(),
                "not_before": binding.not_before.isoformat(),
                "cutoff": binding.cutoff.isoformat(),
                "trusted_acquirer_id": binding.trusted_acquirer_id,
            },
        }
    )


def decode_promotion_manifest(payload: bytes) -> _DecodedPromotionRecord:
    """The single strict promotion-manifest decoder.

    Rejects duplicate keys, float/NaN/Infinity tokens, unknown/missing
    fields, malformed hashes/dates/aware-UTC datetimes, noncanonical or
    oversized base64, oversized/empty/non-bytes input, and noncanonical
    JSON. Never constructs VerifiedReferenceArtifactPromotion.
    """

    if type(payload) is not bytes or not payload or len(payload) > _MAXIMUM_MANIFEST_BYTES:
        raise ReferenceArtifactPromotionStoreError(_ERR_BYTES)
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        raise ReferenceArtifactPromotionStoreError(_ERR_UTF8) from None

    try:
        decoded = json.loads(
            text,
            object_pairs_hook=_unique_object,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
    except ReferenceArtifactPromotionStoreError:
        raise
    except (json.JSONDecodeError, RecursionError):
        raise ReferenceArtifactPromotionStoreError(_ERR_JSON) from None

    if type(decoded) is not dict or set(decoded) != _MANIFEST_KEYS:
        raise ReferenceArtifactPromotionStoreError(_ERR_SHAPE)
    if (
        decoded["codec_schema_version"] != REFERENCE_ARTIFACT_PROMOTION_STORE_CODEC_VERSION
        or decoded["kind"] != _KIND
    ):
        raise ReferenceArtifactPromotionStoreError(_ERR_SHAPE)

    promotion_id = _require_sha(decoded["promotion_id"], "promotion_id")
    join_id = _require_sha(decoded["join_id"], "join_id")
    artifact_id = _require_sha(decoded["artifact_id"], "artifact_id")
    manifest_id = _require_sha(decoded["manifest_id"], "manifest_id")
    raw_sha256 = _require_sha(decoded["raw_sha256"], "raw_sha256")
    normalized_sha256 = _require_sha(decoded["normalized_sha256"], "normalized_sha256")

    receipt_b64 = decoded["receipt_bytes_base64"]
    if type(receipt_b64) is not str or not receipt_b64:
        raise ReferenceArtifactPromotionStoreError(_ERR_BASE64)
    try:
        receipt_bytes = base64.b64decode(receipt_b64, validate=True)
    except (ValueError, TypeError):
        raise ReferenceArtifactPromotionStoreError(_ERR_BASE64) from None
    if (
        base64.b64encode(receipt_bytes).decode("ascii") != receipt_b64
        or len(receipt_bytes) > MAXIMUM_RECEIPT_BYTES
        or not receipt_bytes
    ):
        raise ReferenceArtifactPromotionStoreError(_ERR_BASE64)

    binding_raw = decoded["binding"]
    if type(binding_raw) is not dict or set(binding_raw) != _BINDING_KEYS:
        raise ReferenceArtifactPromotionStoreError(_ERR_SHAPE)

    expected_receipt_sha256 = _require_sha(
        binding_raw["expected_receipt_sha256"], "expected_receipt_sha256"
    )
    expected_raw_sha256 = _require_sha(
        binding_raw["expected_raw_sha256"], "expected_raw_sha256"
    )
    allowed_bucket = binding_raw["allowed_bucket"]
    if type(allowed_bucket) is not str or _BUCKET_NAME.fullmatch(allowed_bucket) is None:
        raise ReferenceArtifactPromotionStoreError(_ERR_SHAPE)
    target_report_date = _canonical_date(binding_raw["target_report_date"])
    not_before = _canonical_datetime(binding_raw["not_before"])
    cutoff = _canonical_datetime(binding_raw["cutoff"])
    trusted_acquirer_id = _require_sha(
        binding_raw["trusted_acquirer_id"], "trusted_acquirer_id"
    )

    try:
        binding = TrustedReferenceAcquisitionBinding(
            expected_receipt_sha256=expected_receipt_sha256,
            expected_raw_sha256=expected_raw_sha256,
            allowed_bucket=allowed_bucket,
            target_report_date=target_report_date,
            not_before=not_before,
            cutoff=cutoff,
            trusted_acquirer_id=trusted_acquirer_id,
        )
    except Exception:
        raise ReferenceArtifactPromotionStoreError(_ERR_SHAPE) from None

    record = _DecodedPromotionRecord(
        promotion_id=promotion_id,
        join_id=join_id,
        artifact_id=artifact_id,
        manifest_id=manifest_id,
        raw_sha256=raw_sha256,
        normalized_sha256=normalized_sha256,
        receipt_bytes=receipt_bytes,
        binding=binding,
    )
    if encode_promotion_manifest(record) != payload:
        raise ReferenceArtifactPromotionStoreError(_ERR_NONCANONICAL)
    return record


class _SealedArtifactObjectReader:
    """Serves bytes only for the exact bucket/object/generation implied by
    one already-verified receipt's own landing_object, sourced from an
    already-sealed StoredReferenceArtifact's own raw_bytes.

    Never lists a bucket, never selects a "latest" object, and never
    accesses GCP, the network, the filesystem, an environment variable, or
    a clock -- it is a pure in-process byte source bound to exactly one
    request shape at construction time.
    """

    def __init__(
        self, *, bucket: str, object_name: str, generation: int, content_bytes: bytes
    ) -> None:
        self._bucket = bucket
        self._object_name = object_name
        self._generation = generation
        self._content_bytes = content_bytes

    def read_generation(
        self, *, bucket: str, object_name: str, generation: int, maximum_bytes: int
    ) -> GCSObjectPayload:
        if (
            bucket != self._bucket
            or object_name != self._object_name
            or generation != self._generation
        ):
            raise ReferenceArtifactPromotionStoreError(_ERR_READER)
        return GCSObjectPayload(
            content_bytes=self._content_bytes[: maximum_bytes + 1],
            generation=self._generation,
        )


def _reconstruct_promotion(
    record: _DecodedPromotionRecord,
    artifacts: LocalReferenceArtifactStore,
) -> VerifiedReferenceArtifactPromotion:
    """The single strict promotion-replay routine.

    Resolves only the exact artifact_id, reconstructs the trusted binding
    and verified receipt, joins through a private sealed-artifact reader
    bound to the receipt's own landing_object and the artifact's own
    already-sealed raw bytes, promotes, and requires exact agreement with
    every pinned identity. Never trusts the stored manifest as authority --
    every fact is independently rebuilt from the pinned sealed artifact and
    the retained receipt bytes/binding.
    """

    try:
        artifact = artifacts.get(record.artifact_id)
    except Exception:
        raise ReferenceArtifactPromotionStoreError(_ERR_ARTIFACT) from None
    if (
        type(artifact) is not StoredReferenceArtifact
        or artifact.manifest.artifact_id != record.artifact_id
        or artifact.manifest.manifest_id != record.manifest_id
        or artifact.manifest.raw_sha256 != record.raw_sha256
        or artifact.manifest.normalized_sha256 != record.normalized_sha256
    ):
        raise ReferenceArtifactPromotionStoreError(_ERR_ARTIFACT)

    try:
        receipt = ReferenceAcquisitionReceiptVerifier().verify(
            record.receipt_bytes, record.binding
        )
    except Exception:
        raise ReferenceArtifactPromotionStoreError(_ERR_RECEIPT) from None

    landing_object = receipt.landing_object
    reader = GCSLandingObjectReader(
        _SealedArtifactObjectReader(
            bucket=landing_object.bucket,
            object_name=landing_object.object_name,
            generation=landing_object.generation,
            content_bytes=artifact.raw_bytes,
        )
    )
    try:
        join = ReferenceAcquisitionJoinService(reader).join(receipt)
    except ReferenceArtifactPromotionStoreError:
        raise
    except Exception:
        raise ReferenceArtifactPromotionStoreError(_ERR_JOIN) from None
    if join.join_id != record.join_id:
        raise ReferenceArtifactPromotionStoreError(_ERR_JOIN)

    try:
        promotion = ReferenceArtifactPromotionService().promote(join, artifact)
    except Exception:
        raise ReferenceArtifactPromotionStoreError(_ERR_PROMOTION) from None
    if promotion.promotion_id != record.promotion_id:
        raise ReferenceArtifactPromotionStoreError(_ERR_PROMOTION)
    return promotion


class LocalReferenceArtifactPromotionStore:
    """Durable exact-ID root store for VerifiedReferenceArtifactPromotion.

    Exposes only ``put``, ``get``, and ``path_for`` -- no list/latest/
    nearest/find/discovery operation of any kind. ``put`` independently
    replays the complete receipt/join/promotion derivation before writing
    anything, so a supplied promotion that cannot be independently
    reconstructed from its own retained receipt bytes/binding and the
    pinned sealed reference artifact leaves no target artifact behind.
    ``get`` never trusts the stored manifest as authority: it strictly
    decodes the manifest, then performs that same independent
    reconstruction before returning a result.
    """

    def __init__(self, root: Path, artifacts: LocalReferenceArtifactStore) -> None:
        if type(artifacts) is not LocalReferenceArtifactStore:
            raise TypeError("reference artifact store must be exact")
        self.root = Path(root) / _STORE_DIRECTORY
        self.artifacts = artifacts

    def path_for(self, promotion_id: str) -> Path:
        return self.root / f"{_require_sha(promotion_id, 'promotion_id')}.json"

    def put(
        self, value: VerifiedReferenceArtifactPromotion
    ) -> VerifiedReferenceArtifactPromotion:
        if type(value) is not VerifiedReferenceArtifactPromotion:
            raise TypeError("promotion must be an exact VerifiedReferenceArtifactPromotion")
        try:
            value.verify_content_identity()
        except Exception:
            raise ReferenceArtifactPromotionStoreError(_ERR_VERIFY) from None

        try:
            resolved_artifact = self.artifacts.get(value.artifact.manifest.artifact_id)
        except Exception:
            raise ReferenceArtifactPromotionStoreError(_ERR_ARTIFACT) from None
        if (
            type(resolved_artifact) is not StoredReferenceArtifact
            or resolved_artifact.manifest != value.artifact.manifest
            or resolved_artifact != value.artifact
        ):
            raise ReferenceArtifactPromotionStoreError(_ERR_ARTIFACT)

        record = _record_for(value)
        replayed = _reconstruct_promotion(record, self.artifacts)
        if type(replayed) is not VerifiedReferenceArtifactPromotion or replayed != value:
            raise ReferenceArtifactPromotionStoreError(_ERR_REPLAY)

        payload = encode_promotion_manifest(record)
        self._publish(value.promotion_id, payload)
        return self.get(value.promotion_id)

    def get(self, promotion_id: str) -> VerifiedReferenceArtifactPromotion:
        _require_sha(promotion_id, "promotion_id")
        path = self.path_for(promotion_id)
        payload = self._read(path)
        record = decode_promotion_manifest(payload)
        if record.promotion_id != promotion_id:
            raise ReferenceArtifactPromotionStoreError(_ERR_PATH_IDENTITY)
        promotion = _reconstruct_promotion(record, self.artifacts)
        if encode_promotion_manifest(_record_for(promotion)) != payload:
            raise ReferenceArtifactPromotionStoreError(_ERR_REPLAY)
        return promotion

    def _read(self, path: Path) -> bytes:
        if not path.exists():
            raise ReferenceArtifactPromotionStoreNotFound(_ERR_NOT_FOUND)
        if not path.is_file() or _is_link_like(path):
            raise ReferenceArtifactPromotionStoreError(_ERR_UNSAFE_PATH)
        try:
            return read_stable_regular_file(path, maximum_bytes=_MAXIMUM_MANIFEST_BYTES)
        except FileSafetyError:
            raise ReferenceArtifactPromotionStoreError(_ERR_UNSAFE_PATH) from None

    def _publish(self, promotion_id: str, payload: bytes) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        if not self.root.is_dir() or _is_link_like(self.root):
            raise ReferenceArtifactPromotionStoreError(_ERR_UNSAFE_PATH)
        target = self.path_for(promotion_id)
        try:
            with advisory_file_lock(self.root / _LOCK_FILENAME):
                if target.exists():
                    if _is_link_like(target) or self._read(target) != payload:
                        raise ReferenceArtifactPromotionStoreConflict(_ERR_CONFLICT)
                    return
                descriptor, temporary_name = tempfile.mkstemp(
                    prefix=".reference-artifact-promotion-",
                    suffix=".tmp",
                    dir=self.root,
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
        except ReferenceArtifactPromotionStoreConflict:
            raise
        except (FileLockUnavailable, FileSafetyError, OSError):
            raise ReferenceArtifactPromotionStoreConflict(_ERR_STORE_UNAVAILABLE) from None
