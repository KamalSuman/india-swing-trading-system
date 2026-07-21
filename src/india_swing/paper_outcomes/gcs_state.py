from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from enum import Enum

from india_swing.daily_pipeline.acquisition import GCSObjectPayload, GCSObjectReader
from india_swing.daily_pipeline.state_publication import PublishedStateObject, StateObjectWriter
from india_swing.identity import content_id
from india_swing.paper_trades import (
    LocalPaperTradeLedger,
    PaperTradeEvent,
    PaperTradeRegistration,
    decode_paper_trade_event,
    decode_paper_trade_registration,
    encode_paper_trade_event,
    encode_paper_trade_registration,
)

from .operational import (
    LocalPaperOutcomeRunStore,
    PaperOutcomeOperationalError,
    PaperOutcomeRunRecord,
    decode_paper_outcome_record,
    encode_paper_outcome_record,
)
from .models import PaperOutcomeStatus


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]\Z")
_MANIFEST_SCHEMA = "paper-outcome-state-manifest/v1"
_MANIFEST_CODEC = "paper-outcome-state-manifest-json/v1"
_MAXIMUM_MANIFEST_BYTES = 4 * 1024 * 1024
_MAXIMUM_REGISTRATION_BYTES = 1024 * 1024
_MAXIMUM_EVENT_BYTES = 1024 * 1024
_MAXIMUM_RECORD_BYTES = 4 * 1024 * 1024


class PaperOutcomeStateError(PaperOutcomeOperationalError):
    pass


class PaperOutcomeStateArtifactKind(str, Enum):
    REGISTRATION = "REGISTRATION"
    EVENT = "EVENT"
    RECORD = "RECORD"


def _sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise PaperOutcomeStateError(f"{name} must be a lowercase SHA-256")
    return value


def validate_paper_outcome_state_bucket(value: object) -> str:
    if type(value) is not str or _BUCKET.fullmatch(value) is None:
        raise PaperOutcomeStateError("paper outcome state bucket is invalid")
    return value


def _maximum(kind: PaperOutcomeStateArtifactKind) -> int:
    return {
        PaperOutcomeStateArtifactKind.REGISTRATION: _MAXIMUM_REGISTRATION_BYTES,
        PaperOutcomeStateArtifactKind.EVENT: _MAXIMUM_EVENT_BYTES,
        PaperOutcomeStateArtifactKind.RECORD: _MAXIMUM_RECORD_BYTES,
    }[kind]


def _artifact_object_name(
    job_spec_id: str,
    kind: PaperOutcomeStateArtifactKind,
    artifact_id: str,
    *,
    sequence: int | None = None,
) -> str:
    _sha(job_spec_id, "job_spec_id")
    _sha(artifact_id, "artifact_id")
    if kind is PaperOutcomeStateArtifactKind.EVENT:
        if type(sequence) is not int or sequence <= 0:
            raise PaperOutcomeStateError("paper outcome event sequence is invalid")
        return f"paper-outcomes/{job_spec_id}/events/{sequence:020d}-{artifact_id}.json"
    if sequence is not None:
        raise PaperOutcomeStateError("non-event artifact cannot carry a sequence")
    directory = "registration" if kind is PaperOutcomeStateArtifactKind.REGISTRATION else "record"
    return f"paper-outcomes/{job_spec_id}/{directory}/{artifact_id}.json"


def _manifest_object_name(job_spec_id: str, publication_id: str) -> str:
    _sha(job_spec_id, "job_spec_id")
    _sha(publication_id, "publication_id")
    return f"paper-outcomes/{job_spec_id}/manifests/{publication_id}.json"


@dataclass(frozen=True, slots=True)
class PaperOutcomeStateArtifact:
    kind: PaperOutcomeStateArtifactKind
    artifact_id: str
    sequence: int | None
    published: PublishedStateObject

    def __post_init__(self) -> None:
        if type(self.kind) is not PaperOutcomeStateArtifactKind:
            raise PaperOutcomeStateError("paper outcome artifact kind must be exact")
        _sha(self.artifact_id, "artifact_id")
        if self.kind is PaperOutcomeStateArtifactKind.EVENT:
            if type(self.sequence) is not int or self.sequence <= 0:
                raise PaperOutcomeStateError("paper outcome event sequence is invalid")
        elif self.sequence is not None:
            raise PaperOutcomeStateError("non-event artifact cannot carry a sequence")
        if type(self.published) is not PublishedStateObject:
            raise PaperOutcomeStateError("published paper outcome artifact must be exact")


def _artifact_identity(value: PaperOutcomeStateArtifact) -> dict[str, object]:
    return {
        "artifact_id": value.artifact_id,
        "byte_count": value.published.byte_count,
        "generation": value.published.generation,
        "kind": value.kind.value,
        "object_name": value.published.object_name,
        "sequence": value.sequence,
        "sha256": value.published.sha256,
    }


@dataclass(frozen=True, slots=True)
class PaperOutcomeStateManifest:
    bucket: str
    job_spec_id: str
    registration_id: str
    record_id: str
    replay_id: str
    artifacts: tuple[PaperOutcomeStateArtifact, ...]
    schema_version: str = _MANIFEST_SCHEMA
    publication_id: str = field(init=False)

    def __post_init__(self) -> None:
        validate_paper_outcome_state_bucket(self.bucket)
        for value, name in (
            (self.job_spec_id, "job_spec_id"),
            (self.registration_id, "registration_id"),
            (self.record_id, "record_id"),
            (self.replay_id, "replay_id"),
        ):
            _sha(value, name)
        if type(self.artifacts) is not tuple or len(self.artifacts) < 2:
            raise PaperOutcomeStateError("paper outcome publication artifacts are incomplete")
        registration = tuple(
            value for value in self.artifacts
            if value.kind is PaperOutcomeStateArtifactKind.REGISTRATION
        )
        records = tuple(
            value for value in self.artifacts
            if value.kind is PaperOutcomeStateArtifactKind.RECORD
        )
        events = tuple(
            value for value in self.artifacts
            if value.kind is PaperOutcomeStateArtifactKind.EVENT
        )
        if len(registration) != 1 or len(records) != 1:
            raise PaperOutcomeStateError("paper outcome publication terminal artifacts differ")
        if registration[0].artifact_id != self.registration_id or records[0].artifact_id != self.record_id:
            raise PaperOutcomeStateError("paper outcome publication artifact lineage differs")
        if tuple(value.sequence for value in events) != tuple(range(1, len(events) + 1)):
            raise PaperOutcomeStateError("paper outcome publication events are not ordered")
        expected_order = registration + events + records
        if self.artifacts != expected_order:
            raise PaperOutcomeStateError("paper outcome publication artifacts are not canonical")
        for value in self.artifacts:
            expected_name = _artifact_object_name(
                self.job_spec_id,
                value.kind,
                value.artifact_id,
                sequence=value.sequence,
            )
            if value.published.object_name != expected_name:
                raise PaperOutcomeStateError("paper outcome publication object path differs")
        if self.schema_version != _MANIFEST_SCHEMA:
            raise PaperOutcomeStateError("unsupported paper outcome state manifest")
        object.__setattr__(
            self,
            "publication_id",
            content_id(
                {
                    "artifacts": tuple(_artifact_identity(value) for value in self.artifacts),
                    "bucket": self.bucket,
                    "job_spec_id": self.job_spec_id,
                    "record_id": self.record_id,
                    "registration_id": self.registration_id,
                    "replay_id": self.replay_id,
                    "schema_version": self.schema_version,
                },
                length=64,
            ),
        )

    def verify_content_identity(self) -> None:
        try:
            fresh = PaperOutcomeStateManifest(
                bucket=self.bucket,
                job_spec_id=self.job_spec_id,
                registration_id=self.registration_id,
                record_id=self.record_id,
                replay_id=self.replay_id,
                artifacts=self.artifacts,
                schema_version=self.schema_version,
            )
        except Exception:
            raise PaperOutcomeStateError("paper outcome manifest identity verification failed") from None
        if fresh.publication_id != self.publication_id:
            raise PaperOutcomeStateError("paper outcome manifest identity verification failed")


def _manifest_body(value: PaperOutcomeStateManifest) -> dict[str, object]:
    return {
        "artifacts": [_artifact_identity(item) for item in value.artifacts],
        "bucket": value.bucket,
        "job_spec_id": value.job_spec_id,
        "publication_id": value.publication_id,
        "record_id": value.record_id,
        "registration_id": value.registration_id,
        "replay_id": value.replay_id,
        "schema_version": value.schema_version,
    }


def encode_paper_outcome_state_manifest(value: PaperOutcomeStateManifest) -> bytes:
    if type(value) is not PaperOutcomeStateManifest:
        raise PaperOutcomeStateError("paper outcome manifest must be exact")
    value.verify_content_identity()
    return (
        json.dumps(
            {"codec_schema_version": _MANIFEST_CODEC, "manifest": _manifest_body(value)},
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def decode_paper_outcome_state_manifest(payload: bytes) -> PaperOutcomeStateManifest:
    if (
        type(payload) is not bytes
        or not payload
        or len(payload) > _MAXIMUM_MANIFEST_BYTES
    ):
        raise PaperOutcomeStateError("paper outcome state manifest bytes are invalid")
    try:
        root = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        if type(root) is not dict or set(root) != {"codec_schema_version", "manifest"}:
            raise ValueError
        if root["codec_schema_version"] != _MANIFEST_CODEC:
            raise ValueError
        raw = root["manifest"]
        expected = {
            "artifacts", "bucket", "job_spec_id", "publication_id", "record_id",
            "registration_id", "replay_id", "schema_version",
        }
        if type(raw) is not dict or set(raw) != expected or type(raw["artifacts"]) is not list:
            raise ValueError
        artifacts = []
        artifact_keys = {
            "artifact_id", "byte_count", "generation", "kind", "object_name",
            "sequence", "sha256",
        }
        for item in raw["artifacts"]:
            if type(item) is not dict or set(item) != artifact_keys:
                raise ValueError
            artifacts.append(
                PaperOutcomeStateArtifact(
                    kind=PaperOutcomeStateArtifactKind(item["kind"]),
                    artifact_id=item["artifact_id"],
                    sequence=item["sequence"],
                    published=PublishedStateObject(
                        object_name=item["object_name"],
                        generation=item["generation"],
                        byte_count=item["byte_count"],
                        sha256=item["sha256"],
                    ),
                )
            )
        stored_id = raw["publication_id"]
        value = PaperOutcomeStateManifest(
            bucket=raw["bucket"],
            job_spec_id=raw["job_spec_id"],
            registration_id=raw["registration_id"],
            record_id=raw["record_id"],
            replay_id=raw["replay_id"],
            artifacts=tuple(artifacts),
            schema_version=raw["schema_version"],
        )
        if value.publication_id != stored_id or encode_paper_outcome_state_manifest(value) != payload:
            raise ValueError
        return value
    except Exception:
        raise PaperOutcomeStateError("paper outcome state manifest is invalid") from None


@dataclass(frozen=True, slots=True)
class CompletedPaperOutcomeStatePublication:
    manifest: PaperOutcomeStateManifest
    manifest_object: PublishedStateObject

    def __post_init__(self) -> None:
        if type(self.manifest) is not PaperOutcomeStateManifest or type(self.manifest_object) is not PublishedStateObject:
            raise PaperOutcomeStateError("completed paper outcome publication is invalid")
        self.manifest.verify_content_identity()
        if self.manifest_object.object_name != _manifest_object_name(
            self.manifest.job_spec_id, self.manifest.publication_id
        ):
            raise PaperOutcomeStateError("paper outcome manifest publication path differs")


def _verified_publication(
    published: PublishedStateObject,
    *,
    object_name: str,
    payload: bytes,
) -> PublishedStateObject:
    if type(published) is not PublishedStateObject:
        raise PaperOutcomeStateError("state writer returned an invalid object")
    if (
        published.object_name != object_name
        or published.byte_count != len(payload)
        or published.sha256 != hashlib.sha256(payload).hexdigest()
    ):
        raise PaperOutcomeStateError("state writer returned differing object evidence")
    return published


def publish_paper_outcome_state_to_gcs(
    *,
    record: PaperOutcomeRunRecord,
    bucket: str,
    writer: StateObjectWriter,
    ledger: LocalPaperTradeLedger,
) -> CompletedPaperOutcomeStatePublication:
    if type(record) is not PaperOutcomeRunRecord or type(ledger) is not LocalPaperTradeLedger:
        raise PaperOutcomeStateError("paper outcome publication inputs must be exact")
    record.verify_content_identity()
    bucket = validate_paper_outcome_state_bucket(bucket)
    try:
        registration = ledger.get_registration(record.registration_id)
        all_events = ledger.list_events(record.registration_id)
        if tuple(value.event_id for value in all_events[: len(record.event_ids)]) != record.event_ids:
            raise ValueError
        if record.outcome_status is PaperOutcomeStatus.CLOSED:
            summary = ledger.summary(record.registration_id)
            if (
                summary.gross_pnl != record.gross_pnl
                or summary.estimated_net_pnl != record.estimated_net_pnl
            ):
                raise ValueError
        events = all_events[: len(record.event_ids)]
        values: list[tuple[PaperOutcomeStateArtifactKind, str, int | None, bytes]] = [
            (
                PaperOutcomeStateArtifactKind.REGISTRATION,
                registration.registration_id,
                None,
                encode_paper_trade_registration(registration),
            )
        ]
        values.extend(
            (
                PaperOutcomeStateArtifactKind.EVENT,
                event.event_id,
                event.sequence,
                encode_paper_trade_event(event),
            )
            for event in events
        )
        values.append(
            (
                PaperOutcomeStateArtifactKind.RECORD,
                record.record_id,
                None,
                encode_paper_outcome_record(record),
            )
        )
        artifacts = []
        for kind, artifact_id, sequence, payload in values:
            object_name = _artifact_object_name(
                record.job_spec_id, kind, artifact_id, sequence=sequence
            )
            published = writer.create_or_verify(
                bucket=bucket,
                object_name=object_name,
                content_bytes=payload,
                content_type="application/json",
                maximum_bytes=_maximum(kind),
            )
            artifacts.append(
                PaperOutcomeStateArtifact(
                    kind=kind,
                    artifact_id=artifact_id,
                    sequence=sequence,
                    published=_verified_publication(
                        published, object_name=object_name, payload=payload
                    ),
                )
            )
        manifest = PaperOutcomeStateManifest(
            bucket=bucket,
            job_spec_id=record.job_spec_id,
            registration_id=record.registration_id,
            record_id=record.record_id,
            replay_id=record.replay_id,
            artifacts=tuple(artifacts),
        )
        manifest_payload = encode_paper_outcome_state_manifest(manifest)
        manifest_name = _manifest_object_name(record.job_spec_id, manifest.publication_id)
        manifest_object = writer.create_or_verify(
            bucket=bucket,
            object_name=manifest_name,
            content_bytes=manifest_payload,
            content_type="application/json",
            maximum_bytes=_MAXIMUM_MANIFEST_BYTES,
        )
        return CompletedPaperOutcomeStatePublication(
            manifest=manifest,
            manifest_object=_verified_publication(
                manifest_object, object_name=manifest_name, payload=manifest_payload
            ),
        )
    except PaperOutcomeStateError:
        raise
    except Exception:
        raise PaperOutcomeStateError("paper outcome state publication failed safely") from None


def _read_exact(
    *,
    reader: GCSObjectReader,
    bucket: str,
    published: PublishedStateObject,
    maximum_bytes: int,
) -> bytes:
    payload = reader.read_generation(
        bucket=bucket,
        object_name=published.object_name,
        generation=published.generation,
        maximum_bytes=maximum_bytes,
    )
    if (
        type(payload) is not GCSObjectPayload
        or payload.generation != published.generation
        or type(payload.content_bytes) is not bytes
        or not payload.content_bytes
        or len(payload.content_bytes) > maximum_bytes
        or len(payload.content_bytes) != published.byte_count
        or hashlib.sha256(payload.content_bytes).hexdigest() != published.sha256
    ):
        raise PaperOutcomeStateError("paper outcome state object verification failed")
    return payload.content_bytes


def restore_paper_outcome_state_from_gcs(
    *,
    expected_job_spec_id: str,
    bucket: str,
    manifest_object_name: str,
    manifest_generation: int,
    manifest_sha256: str,
    reader: GCSObjectReader,
    ledger: LocalPaperTradeLedger,
    record_store: LocalPaperOutcomeRunStore,
) -> PaperOutcomeRunRecord:
    _sha(expected_job_spec_id, "expected_job_spec_id")
    bucket = validate_paper_outcome_state_bucket(bucket)
    _sha(manifest_sha256, "manifest_sha256")
    if type(manifest_generation) is not int or type(manifest_generation) is bool or manifest_generation <= 0:
        raise PaperOutcomeStateError("paper outcome manifest generation is invalid")
    prefix = f"paper-outcomes/{expected_job_spec_id}/manifests/"
    if (
        type(manifest_object_name) is not str
        or not manifest_object_name.startswith(prefix)
        or not manifest_object_name.endswith(".json")
        or _SHA256.fullmatch(manifest_object_name[len(prefix):-5]) is None
    ):
        raise PaperOutcomeStateError("paper outcome manifest object name is invalid")
    try:
        manifest_payload = reader.read_generation(
            bucket=bucket,
            object_name=manifest_object_name,
            generation=manifest_generation,
            maximum_bytes=_MAXIMUM_MANIFEST_BYTES,
        )
        if (
            type(manifest_payload) is not GCSObjectPayload
            or manifest_payload.generation != manifest_generation
            or type(manifest_payload.content_bytes) is not bytes
            or not manifest_payload.content_bytes
            or len(manifest_payload.content_bytes) > _MAXIMUM_MANIFEST_BYTES
            or hashlib.sha256(manifest_payload.content_bytes).hexdigest() != manifest_sha256
        ):
            raise ValueError
        manifest = decode_paper_outcome_state_manifest(manifest_payload.content_bytes)
        if (
            manifest.bucket != bucket
            or manifest.job_spec_id != expected_job_spec_id
            or manifest_object_name != _manifest_object_name(
                expected_job_spec_id, manifest.publication_id
            )
        ):
            raise ValueError
        registration: PaperTradeRegistration | None = None
        events: list[PaperTradeEvent] = []
        record: PaperOutcomeRunRecord | None = None
        for artifact in manifest.artifacts:
            payload = _read_exact(
                reader=reader,
                bucket=bucket,
                published=artifact.published,
                maximum_bytes=_maximum(artifact.kind),
            )
            if artifact.kind is PaperOutcomeStateArtifactKind.REGISTRATION:
                registration = decode_paper_trade_registration(payload)
                if registration.registration_id != artifact.artifact_id:
                    raise ValueError
            elif artifact.kind is PaperOutcomeStateArtifactKind.EVENT:
                event = decode_paper_trade_event(payload)
                if event.event_id != artifact.artifact_id or event.sequence != artifact.sequence:
                    raise ValueError
                events.append(event)
            else:
                record = decode_paper_outcome_record(payload)
                if record.record_id != artifact.artifact_id:
                    raise ValueError
        if registration is None or record is None:
            raise ValueError
        if (
            registration.registration_id != manifest.registration_id
            or record.record_id != manifest.record_id
            or record.replay_id != manifest.replay_id
            or tuple(value.event_id for value in events) != record.event_ids
        ):
            raise ValueError

        ledger.register_value(registration)
        existing = ledger.list_events(registration.registration_id)
        shared_count = min(len(existing), len(events))
        if existing[:shared_count] != tuple(events[:shared_count]):
            raise ValueError
        for event in events[len(existing):]:
            restored = ledger.append(
                registration_id=event.registration_id,
                event_type=event.event_type,
                occurred_at=event.occurred_at,
                observed_price=event.observed_price,
                evidence_id=event.evidence_id,
                reason_code=event.reason_code,
                market_session=event.market_session,
                replay_id=event.replay_id,
                outcome_policy_id=event.outcome_policy_id,
                instrument_binding_id=event.instrument_binding_id,
                calendar_snapshot_id=event.calendar_snapshot_id,
            )
            if restored != event:
                raise ValueError
        if record.outcome_status is PaperOutcomeStatus.CLOSED:
            summary = ledger.summary(record.registration_id)
            if (
                summary.gross_pnl != record.gross_pnl
                or summary.estimated_net_pnl != record.estimated_net_pnl
            ):
                raise ValueError
        stored = record_store.put(record)
        if stored != record:
            raise ValueError
        return stored
    except PaperOutcomeStateError:
        raise
    except Exception:
        raise PaperOutcomeStateError("paper outcome state restoration failed safely") from None
