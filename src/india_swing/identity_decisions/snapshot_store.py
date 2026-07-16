from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import date, datetime
from pathlib import Path

from india_swing._filesystem import FileSafetyError, advisory_file_lock, read_stable_regular_file
from india_swing.identity_registry import IdentityAdjudicationRequirement
from india_swing.reference.models import ReferenceReadiness

from .models import (
    ADJUDICATED_IDENTITY_POLICY_VERSION,
    ADJUDICATED_IDENTITY_SCHEMA_VERSION,
    AdjudicatedIdentitySnapshot,
    CandidateIdentityResolution,
    EffectiveStableListingObservation,
    IdentityDecisionConflict,
    IdentityDecisionIntegrityError,
    IdentityDecisionNotFound,
    IdentityResolutionBlocker,
)
from .parser import decode_strict_review_json


ADJUDICATED_IDENTITY_DATASET = "adjudicated-identity-snapshots"
ADJUDICATED_IDENTITY_CODEC_VERSION = "adjudicated-identity-json/v1"
MAXIMUM_ADJUDICATED_IDENTITY_BYTES = 256 * 1024 * 1024
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_ROOT_KEYS = {
    "codec_version", "schema_version", "policy_version", "snapshot_id",
    "source_registry_id", "source_queue_id", "cutoff", "knowledge_time",
    "evidence_artifact_ids", "review_bundle_ids", "resolutions",
    "listing_observations", "readiness", "actionable", "stable_identity_assigned",
}
_RESOLUTION_KEYS = {
    "candidate_id", "required_requirements", "accepted_decision_ids",
    "rejected_decision_ids", "missing_requirements", "blocker_codes",
    "stable_instrument_id",
}
_LISTING_KEYS = {
    "record_id", "candidate_id", "source_observation_id", "stable_instrument_id",
    "stable_listing_id", "effective_on", "symbol", "series", "isin",
}


def _object(value: object, keys: set[str], name: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != keys:
        raise IdentityDecisionIntegrityError(f"{name} schema mismatch")
    return value


def _string_list(value: object, name: str) -> tuple[str, ...]:
    if type(value) is not list or any(type(item) is not str for item in value):
        raise IdentityDecisionIntegrityError(f"{name} must be a JSON string array")
    return tuple(value)


def encode_adjudicated_identity_snapshot(value: AdjudicatedIdentitySnapshot) -> bytes:
    if type(value) is not AdjudicatedIdentitySnapshot:
        raise TypeError("adjudicated identity codec requires an exact snapshot")
    value.verify_content_identity()
    payload = {
        "codec_version": ADJUDICATED_IDENTITY_CODEC_VERSION,
        "schema_version": value.schema_version,
        "policy_version": value.policy_version,
        "snapshot_id": value.snapshot_id,
        "source_registry_id": value.source_registry_id,
        "source_queue_id": value.source_queue_id,
        "cutoff": value.cutoff.isoformat(),
        "knowledge_time": value.knowledge_time.isoformat(),
        "evidence_artifact_ids": list(value.evidence_artifact_ids),
        "review_bundle_ids": list(value.review_bundle_ids),
        "resolutions": [
            {
                "candidate_id": item.candidate_id,
                "required_requirements": [entry.value for entry in item.required_requirements],
                "accepted_decision_ids": list(item.accepted_decision_ids),
                "rejected_decision_ids": list(item.rejected_decision_ids),
                "missing_requirements": [entry.value for entry in item.missing_requirements],
                "blocker_codes": [entry.value for entry in item.blocker_codes],
                "stable_instrument_id": item.stable_instrument_id,
            }
            for item in value.resolutions
        ],
        "listing_observations": [
            {
                "record_id": item.record_id,
                "candidate_id": item.candidate_id,
                "source_observation_id": item.source_observation_id,
                "stable_instrument_id": item.stable_instrument_id,
                "stable_listing_id": item.stable_listing_id,
                "effective_on": item.effective_on.isoformat(),
                "symbol": item.symbol,
                "series": item.series,
                "isin": item.isin,
            }
            for item in value.listing_observations
        ],
        "readiness": value.readiness.value,
        "actionable": value.actionable,
        "stable_identity_assigned": value.stable_identity_assigned,
    }
    return (json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")


def decode_adjudicated_identity_snapshot(payload: bytes) -> AdjudicatedIdentitySnapshot:
    raw = _object(decode_strict_review_json(payload), _ROOT_KEYS, "adjudicated identity snapshot")
    if raw["codec_version"] != ADJUDICATED_IDENTITY_CODEC_VERSION:
        raise IdentityDecisionIntegrityError("unsupported adjudicated identity codec")
    raw_resolutions = raw["resolutions"]
    raw_listings = raw["listing_observations"]
    if type(raw_resolutions) is not list or type(raw_listings) is not list:
        raise IdentityDecisionIntegrityError("adjudicated identity arrays are invalid")
    try:
        resolutions = tuple(
            CandidateIdentityResolution(
                candidate_id=item["candidate_id"],
                required_requirements=tuple(IdentityAdjudicationRequirement(value) for value in item["required_requirements"]),
                accepted_decision_ids=_string_list(item["accepted_decision_ids"], "accepted_decision_ids"),
                rejected_decision_ids=_string_list(item["rejected_decision_ids"], "rejected_decision_ids"),
                missing_requirements=tuple(IdentityAdjudicationRequirement(value) for value in item["missing_requirements"]),
                blocker_codes=tuple(IdentityResolutionBlocker(value) for value in item["blocker_codes"]),
                stable_instrument_id=item["stable_instrument_id"],
            )
            for item in (_object(value, _RESOLUTION_KEYS, "identity resolution") for value in raw_resolutions)
        )
        listings = []
        for item in (_object(value, _LISTING_KEYS, "stable listing observation") for value in raw_listings):
            parsed = EffectiveStableListingObservation(
                candidate_id=item["candidate_id"],
                source_observation_id=item["source_observation_id"],
                stable_instrument_id=item["stable_instrument_id"],
                stable_listing_id=item["stable_listing_id"],
                effective_on=date.fromisoformat(item["effective_on"]),
                symbol=item["symbol"], series=item["series"], isin=item["isin"],
            )
            if parsed.record_id != item["record_id"]:
                raise IdentityDecisionIntegrityError("stored stable listing record identity differs")
            listings.append(parsed)
        result = AdjudicatedIdentitySnapshot(
            source_registry_id=raw["source_registry_id"],
            source_queue_id=raw["source_queue_id"],
            cutoff=datetime.fromisoformat(raw["cutoff"]),
            knowledge_time=datetime.fromisoformat(raw["knowledge_time"]),
            evidence_artifact_ids=_string_list(raw["evidence_artifact_ids"], "evidence_artifact_ids"),
            review_bundle_ids=_string_list(raw["review_bundle_ids"], "review_bundle_ids"),
            resolutions=resolutions,
            listing_observations=tuple(listings),
            readiness=ReferenceReadiness(raw["readiness"]),
            actionable=raw["actionable"],
            schema_version=raw["schema_version"], policy_version=raw["policy_version"],
        )
    except IdentityDecisionIntegrityError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise IdentityDecisionIntegrityError("stored adjudicated identity snapshot is invalid") from exc
    if result.snapshot_id != raw["snapshot_id"] or result.stable_identity_assigned is not raw["stable_identity_assigned"]:
        raise IdentityDecisionIntegrityError("stored adjudicated identity snapshot identity differs")
    return result


def _is_link_like(path: Path) -> bool:
    junction = getattr(path, "is_junction", None)
    return path.is_symlink() or bool(junction and junction())


class LocalAdjudicatedIdentitySnapshotStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def dataset_root(self) -> Path:
        return self.root / ADJUDICATED_IDENTITY_DATASET

    def path_for(self, snapshot_id: str) -> Path:
        if not isinstance(snapshot_id, str) or _SHA256.fullmatch(snapshot_id) is None:
            raise IdentityDecisionNotFound("invalid adjudicated identity snapshot ID")
        return self.dataset_root / f"{snapshot_id}.json"

    def put(self, value: AdjudicatedIdentitySnapshot) -> AdjudicatedIdentitySnapshot:
        if type(value) is not AdjudicatedIdentitySnapshot:
            raise TypeError("adjudicated identity snapshot must be exact")
        value.verify_content_identity()
        payload = encode_adjudicated_identity_snapshot(value)
        self.dataset_root.mkdir(parents=True, exist_ok=True)
        if not self.dataset_root.is_dir() or _is_link_like(self.dataset_root):
            raise IdentityDecisionIntegrityError("adjudicated identity root is unsafe")
        target = self.path_for(value.snapshot_id)
        try:
            with advisory_file_lock(self.dataset_root / ".adjudicated-identity.lock"):
                if target.exists():
                    stored = self.get(value.snapshot_id)
                    if stored != value:
                        raise IdentityDecisionConflict("snapshot ID already stores different content")
                    return stored
                descriptor, temporary_name = tempfile.mkstemp(prefix=".adjudicated-", suffix=".tmp", dir=self.dataset_root)
                temporary = Path(temporary_name)
                try:
                    with os.fdopen(descriptor, "wb") as handle:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.link(temporary, target)
                finally:
                    temporary.unlink(missing_ok=True)
        except FileSafetyError as exc:
            raise IdentityDecisionConflict("adjudicated identity store is unavailable") from exc
        return self.get(value.snapshot_id)

    def get(self, snapshot_id: str) -> AdjudicatedIdentitySnapshot:
        path = self.path_for(snapshot_id)
        if not path.exists():
            raise IdentityDecisionNotFound("adjudicated identity snapshot was not found")
        if not path.is_file() or _is_link_like(path):
            raise IdentityDecisionIntegrityError("adjudicated identity snapshot path is unsafe")
        try:
            return decode_adjudicated_identity_snapshot(
                read_stable_regular_file(path, maximum_bytes=MAXIMUM_ADJUDICATED_IDENTITY_BYTES)
            )
        except FileSafetyError as exc:
            raise IdentityDecisionIntegrityError("adjudicated identity snapshot could not be read safely") from exc

    def list_snapshots(self) -> tuple[AdjudicatedIdentitySnapshot, ...]:
        if not self.dataset_root.exists():
            return ()
        if not self.dataset_root.is_dir() or _is_link_like(self.dataset_root):
            raise IdentityDecisionIntegrityError("adjudicated identity root is unsafe")
        result = []
        for path in sorted(self.dataset_root.iterdir(), key=lambda value: value.name):
            if path.name == ".adjudicated-identity.lock":
                continue
            if path.suffix != ".json" or _SHA256.fullmatch(path.stem) is None:
                raise IdentityDecisionIntegrityError("adjudicated identity store contains an unexpected entry")
            result.append(self.get(path.stem))
        return tuple(result)
