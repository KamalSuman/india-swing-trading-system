from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field, fields
from datetime import date
from enum import Enum

from india_swing.daily_pipeline.acquisition import GCSObjectPayload, GCSObjectReader
from india_swing.daily_pipeline.state_publication import (
    PublishedStateObject,
    StateObjectWriter,
)
from india_swing.identity import content_id
from india_swing.paper_trades import LocalPaperTradeLedger, PaperTradeRegistration
from india_swing.paper_trades.store import (
    decode_paper_trade_registration,
    encode_paper_trade_registration,
)
from india_swing.recommendations import (
    LocalSwingDecisionOutbox,
    SwingDecisionAction,
    SwingDecisionNotification,
)
from india_swing.recommendations.store import (
    decode_swing_decision_notification,
    encode_swing_decision_notification,
)

from .models import SwingOperationalRunRecord, SwingOperationalStatus
from .store import (
    LocalSwingOperationalRunStore,
    decode_operational_run_record,
    encode_operational_run_record,
)


_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MANIFEST_PATH = re.compile(
    r"operational-state/(\d{4}-\d{2}-\d{2})/([0-9a-f]{64})/"
    r"manifests/([0-9a-f]{64})\.json\Z"
)
_MAXIMUM_GENERATION = 9_223_372_036_854_775_807
_MAXIMUM_MANIFEST_BYTES = 256 * 1024
_MAXIMUM_NOTIFICATION_BYTES = 256 * 1024
_MAXIMUM_REGISTRATION_BYTES = 1024 * 1024
_MAXIMUM_RECORD_BYTES = 512 * 1024
_SCHEMA_VERSION = 1


class SwingOperationalStateError(ValueError):
    pass


class SwingOperationalStateArtifactKind(str, Enum):
    NOTIFICATION = "NOTIFICATION"
    PAPER_REGISTRATION = "PAPER_REGISTRATION"
    RUN_RECORD = "RUN_RECORD"


def _sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise SwingOperationalStateError(f"{name} must be a lowercase SHA-256")
    return value


def _bucket(value: object) -> str:
    if type(value) is not str or _BUCKET.fullmatch(value) is None:
        raise SwingOperationalStateError("operational state bucket is invalid")
    return value


def validate_operational_state_bucket(value: object) -> str:
    return _bucket(value)


def _published(value: object) -> PublishedStateObject:
    if type(value) is not PublishedStateObject:
        raise SwingOperationalStateError("published state object must be exact")
    return PublishedStateObject(
        object_name=value.object_name,
        generation=value.generation,
        byte_count=value.byte_count,
        sha256=value.sha256,
    )


def _maximum_bytes(kind: SwingOperationalStateArtifactKind) -> int:
    return {
        SwingOperationalStateArtifactKind.NOTIFICATION: _MAXIMUM_NOTIFICATION_BYTES,
        SwingOperationalStateArtifactKind.PAPER_REGISTRATION: _MAXIMUM_REGISTRATION_BYTES,
        SwingOperationalStateArtifactKind.RUN_RECORD: _MAXIMUM_RECORD_BYTES,
    }[kind]


@dataclass(frozen=True, slots=True)
class SwingOperationalStateArtifact:
    kind: SwingOperationalStateArtifactKind
    published_object: PublishedStateObject

    def __post_init__(self) -> None:
        if type(self.kind) is not SwingOperationalStateArtifactKind:
            raise SwingOperationalStateError("operational artifact kind must be exact")
        published = _published(self.published_object)
        if published.byte_count > _maximum_bytes(self.kind):
            raise SwingOperationalStateError("operational artifact is oversized")
        object.__setattr__(self, "published_object", published)


def _artifact_identity(
    manifest: "SwingOperationalStateManifest",
    kind: SwingOperationalStateArtifactKind,
) -> str:
    if kind is SwingOperationalStateArtifactKind.NOTIFICATION:
        if manifest.notification_id is None:
            raise SwingOperationalStateError("notification identity is missing")
        return manifest.notification_id
    if kind is SwingOperationalStateArtifactKind.PAPER_REGISTRATION:
        if manifest.paper_registration_id is None:
            raise SwingOperationalStateError("paper registration identity is missing")
        return manifest.paper_registration_id
    return manifest.record_id


def operational_state_artifact_object_name(
    manifest: "SwingOperationalStateManifest",
    kind: SwingOperationalStateArtifactKind,
) -> str:
    if type(manifest) is not SwingOperationalStateManifest:
        raise SwingOperationalStateError("operational state manifest must be exact")
    if type(kind) is not SwingOperationalStateArtifactKind:
        raise SwingOperationalStateError("operational artifact kind must be exact")
    identity = _artifact_identity(manifest, kind)
    return (
        f"operational-state/{manifest.target_session.isoformat()}/"
        f"{manifest.spec_id}/artifacts/{kind.value.lower()}/{identity}.json"
    )


@dataclass(frozen=True, slots=True)
class SwingOperationalStateManifest:
    bucket: str
    spec_id: str
    record_id: str
    target_session: date
    status: SwingOperationalStatus
    action: SwingDecisionAction
    decision_id: str | None
    notification_id: str | None
    paper_registration_id: str | None
    artifacts: tuple[SwingOperationalStateArtifact, ...]
    schema_version: int = _SCHEMA_VERSION
    publication_id: str = field(init=False)

    def __post_init__(self) -> None:
        _bucket(self.bucket)
        _sha(self.spec_id, "spec_id")
        _sha(self.record_id, "record_id")
        if type(self.target_session) is not date:
            raise SwingOperationalStateError("target_session must be exact")
        if type(self.status) is not SwingOperationalStatus:
            raise SwingOperationalStateError("operational status must be exact")
        if type(self.action) is not SwingDecisionAction:
            raise SwingOperationalStateError("decision action must be exact")
        for value, name in (
            (self.decision_id, "decision_id"),
            (self.notification_id, "notification_id"),
            (self.paper_registration_id, "paper_registration_id"),
        ):
            if value is not None:
                _sha(value, name)
        if type(self.artifacts) is not tuple:
            raise SwingOperationalStateError("operational artifacts must be a tuple")
        reconstructed: list[SwingOperationalStateArtifact] = []
        for value in self.artifacts:
            if type(value) is not SwingOperationalStateArtifact:
                raise SwingOperationalStateError("operational artifact must be exact")
            reconstructed.append(
                SwingOperationalStateArtifact(
                    kind=value.kind,
                    published_object=value.published_object,
                )
            )
        artifacts = tuple(reconstructed)
        kinds = tuple(value.kind for value in artifacts)
        required = (
            (SwingOperationalStateArtifactKind.RUN_RECORD,)
            if self.status is SwingOperationalStatus.FAILED
            else (
                SwingOperationalStateArtifactKind.NOTIFICATION,
                *(
                    (SwingOperationalStateArtifactKind.PAPER_REGISTRATION,)
                    if self.action is SwingDecisionAction.BUY
                    else ()
                ),
                SwingOperationalStateArtifactKind.RUN_RECORD,
            )
        )
        if kinds != required:
            raise SwingOperationalStateError("operational artifact set is invalid")
        if self.status is SwingOperationalStatus.FAILED:
            if (
                self.action is not SwingDecisionAction.NO_TRADE
                or self.decision_id is not None
                or self.notification_id is not None
                or self.paper_registration_id is not None
            ):
                raise SwingOperationalStateError("failed publication lineage is invalid")
        else:
            if self.decision_id is None or self.notification_id is None:
                raise SwingOperationalStateError("complete publication lineage is missing")
            if (self.action is SwingDecisionAction.BUY) != (
                self.paper_registration_id is not None
            ):
                raise SwingOperationalStateError("paper publication lineage is invalid")
        if type(self.schema_version) is not int or self.schema_version != _SCHEMA_VERSION:
            raise SwingOperationalStateError("operational state schema is unsupported")
        object.__setattr__(self, "artifacts", artifacts)
        for artifact in artifacts:
            if artifact.published_object.object_name != operational_state_artifact_object_name(
                self, artifact.kind
            ):
                raise SwingOperationalStateError("operational artifact path differs")
        object.__setattr__(self, "publication_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(_manifest_body(self, include_publication_id=False), length=64)

    def verify_content_identity(self) -> None:
        fresh = SwingOperationalStateManifest(
            **{
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "publication_id"
            }
        )
        if self.publication_id != fresh.publication_id:
            raise SwingOperationalStateError("operational state identity failed")


def operational_state_manifest_object_name(
    manifest: SwingOperationalStateManifest,
) -> str:
    if type(manifest) is not SwingOperationalStateManifest:
        raise SwingOperationalStateError("operational state manifest must be exact")
    manifest.verify_content_identity()
    return (
        f"operational-state/{manifest.target_session.isoformat()}/"
        f"{manifest.spec_id}/manifests/{manifest.publication_id}.json"
    )


def _published_body(value: PublishedStateObject) -> dict[str, object]:
    return {
        "byte_count": value.byte_count,
        "generation": value.generation,
        "object_name": value.object_name,
        "sha256": value.sha256,
    }


def _manifest_body(
    value: SwingOperationalStateManifest,
    *,
    include_publication_id: bool,
) -> dict[str, object]:
    body: dict[str, object] = {
        "action": value.action.value,
        "artifacts": [
            {
                "kind": item.kind.value,
                "published_object": _published_body(item.published_object),
            }
            for item in value.artifacts
        ],
        "bucket": value.bucket,
        "decision_id": value.decision_id,
        "notification_id": value.notification_id,
        "paper_registration_id": value.paper_registration_id,
        "record_id": value.record_id,
        "schema_version": value.schema_version,
        "spec_id": value.spec_id,
        "status": value.status.value,
        "target_session": value.target_session.isoformat(),
    }
    if include_publication_id:
        body["publication_id"] = value.publication_id
    return body


def encode_operational_state_manifest(value: SwingOperationalStateManifest) -> bytes:
    if type(value) is not SwingOperationalStateManifest:
        raise SwingOperationalStateError("operational state manifest must be exact")
    value.verify_content_identity()
    return (
        json.dumps(
            _manifest_body(value, include_publication_id=True),
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
            raise SwingOperationalStateError("operational state JSON has duplicate keys")
        result[key] = value
    return result


def _exact(value: object, keys: set[str], name: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != keys:
        raise SwingOperationalStateError(f"operational {name} fields are invalid")
    return value


def decode_operational_state_manifest(payload: bytes) -> SwingOperationalStateManifest:
    if type(payload) is not bytes or not (0 < len(payload) <= _MAXIMUM_MANIFEST_BYTES):
        raise SwingOperationalStateError("operational state manifest payload is invalid")
    try:
        raw = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_float=lambda _: (_ for _ in ()).throw(ValueError()),
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
        root = _exact(
            raw,
            {
                "action",
                "artifacts",
                "bucket",
                "decision_id",
                "notification_id",
                "paper_registration_id",
                "publication_id",
                "record_id",
                "schema_version",
                "spec_id",
                "status",
                "target_session",
            },
            "state manifest",
        )
        raw_artifacts = root["artifacts"]
        if type(raw_artifacts) is not list:
            raise SwingOperationalStateError("operational artifacts are invalid")
        artifacts: list[SwingOperationalStateArtifact] = []
        for raw_artifact in raw_artifacts:
            artifact = _exact(raw_artifact, {"kind", "published_object"}, "artifact")
            published = _exact(
                artifact["published_object"],
                {"byte_count", "generation", "object_name", "sha256"},
                "published object",
            )
            artifacts.append(
                SwingOperationalStateArtifact(
                    kind=SwingOperationalStateArtifactKind(artifact["kind"]),
                    published_object=PublishedStateObject(**published),
                )
            )
        manifest = SwingOperationalStateManifest(
            bucket=root["bucket"],
            spec_id=root["spec_id"],
            record_id=root["record_id"],
            target_session=date.fromisoformat(root["target_session"]),
            status=SwingOperationalStatus(root["status"]),
            action=SwingDecisionAction(root["action"]),
            decision_id=root["decision_id"],
            notification_id=root["notification_id"],
            paper_registration_id=root["paper_registration_id"],
            artifacts=tuple(artifacts),
            schema_version=root["schema_version"],
        )
        if manifest.publication_id != root["publication_id"]:
            raise SwingOperationalStateError("stored publication identity differs")
        if encode_operational_state_manifest(manifest) != payload:
            raise SwingOperationalStateError("operational state manifest is not canonical")
        return manifest
    except SwingOperationalStateError:
        raise
    except Exception:
        raise SwingOperationalStateError("stored operational state manifest is invalid") from None


@dataclass(frozen=True, slots=True)
class CompletedSwingOperationalStatePublication:
    manifest: SwingOperationalStateManifest
    manifest_object: PublishedStateObject

    def __post_init__(self) -> None:
        if type(self.manifest) is not SwingOperationalStateManifest:
            raise SwingOperationalStateError("publication manifest must be exact")
        self.manifest.verify_content_identity()
        published = _published(self.manifest_object)
        payload = encode_operational_state_manifest(self.manifest)
        if (
            published.object_name != operational_state_manifest_object_name(self.manifest)
            or published.byte_count != len(payload)
            or published.sha256 != hashlib.sha256(payload).hexdigest()
        ):
            raise SwingOperationalStateError("published manifest object differs")
        object.__setattr__(self, "manifest_object", published)


def _verify_notification(
    record: SwingOperationalRunRecord,
    notification: SwingDecisionNotification,
) -> None:
    notification.verify_content_identity()
    if (
        notification.decision_id != record.decision_id
        or notification.notification_id != record.notification_id
        or notification.message != record.message
        or notification.message_sha256 != record.message_sha256
    ):
        raise SwingOperationalStateError("notification differs from terminal record")


def _verify_registration(
    record: SwingOperationalRunRecord,
    registration: PaperTradeRegistration,
) -> None:
    registration.verify_content_identity()
    if (
        registration.registration_id != record.paper_registration_id
        or registration.alert_id != record.notification_id
        or registration.source_run_id != record.spec_id
        or registration.source_pipeline_integrity_hash != record.package_id
        or registration.source_decision_integrity_hash != record.decision_id
    ):
        raise SwingOperationalStateError("paper registration differs from terminal record")


def _artifact_path_for_record(
    record: SwingOperationalRunRecord,
    kind: SwingOperationalStateArtifactKind,
) -> str:
    identity = {
        SwingOperationalStateArtifactKind.NOTIFICATION: record.notification_id,
        SwingOperationalStateArtifactKind.PAPER_REGISTRATION: record.paper_registration_id,
        SwingOperationalStateArtifactKind.RUN_RECORD: record.record_id,
    }[kind]
    if identity is None:
        raise SwingOperationalStateError("operational artifact identity is missing")
    return (
        f"operational-state/{record.target_session.isoformat()}/{record.spec_id}/"
        f"artifacts/{kind.value.lower()}/{identity}.json"
    )


def publish_swing_operational_state_to_gcs(
    *,
    record: SwingOperationalRunRecord,
    bucket: str,
    writer: StateObjectWriter,
    decision_outbox: LocalSwingDecisionOutbox,
    paper_ledger: LocalPaperTradeLedger,
) -> CompletedSwingOperationalStatePublication:
    """Publish verified side effects and the terminal record, then seal a manifest."""

    if type(record) is not SwingOperationalRunRecord:
        raise SwingOperationalStateError("operational record must be exact")
    record.verify_content_identity()
    bucket = _bucket(bucket)
    if type(decision_outbox) is not LocalSwingDecisionOutbox:
        raise SwingOperationalStateError("decision outbox must be exact")
    if type(paper_ledger) is not LocalPaperTradeLedger:
        raise SwingOperationalStateError("paper ledger must be exact")
    payloads: list[tuple[SwingOperationalStateArtifactKind, bytes]] = []
    try:
        if record.status is SwingOperationalStatus.COMPLETE:
            notification = decision_outbox.get(record.decision_id)
            _verify_notification(record, notification)
            payloads.append(
                (
                    SwingOperationalStateArtifactKind.NOTIFICATION,
                    encode_swing_decision_notification(notification),
                )
            )
            if record.paper_registration_id is not None:
                registration = paper_ledger.get_registration(record.paper_registration_id)
                _verify_registration(record, registration)
                payloads.append(
                    (
                        SwingOperationalStateArtifactKind.PAPER_REGISTRATION,
                        encode_paper_trade_registration(registration),
                    )
                )
        payloads.append(
            (
                SwingOperationalStateArtifactKind.RUN_RECORD,
                encode_operational_run_record(record),
            )
        )
    except Exception:
        raise SwingOperationalStateError("operational state inputs are invalid") from None

    artifacts: list[SwingOperationalStateArtifact] = []
    try:
        for kind, payload in payloads:
            object_name = _artifact_path_for_record(record, kind)
            published = writer.create_or_verify(
                bucket=bucket,
                object_name=object_name,
                content_bytes=payload,
                content_type="application/json",
                maximum_bytes=_maximum_bytes(kind),
            )
            if (
                type(published) is not PublishedStateObject
                or published.object_name != object_name
                or published.byte_count != len(payload)
                or published.sha256 != hashlib.sha256(payload).hexdigest()
            ):
                raise SwingOperationalStateError(
                    "published operational artifact differs"
                )
            artifacts.append(
                SwingOperationalStateArtifact(kind=kind, published_object=published)
            )
        manifest = SwingOperationalStateManifest(
            bucket=bucket,
            spec_id=record.spec_id,
            record_id=record.record_id,
            target_session=record.target_session,
            status=record.status,
            action=record.action,
            decision_id=record.decision_id,
            notification_id=record.notification_id,
            paper_registration_id=record.paper_registration_id,
            artifacts=tuple(artifacts),
        )
        manifest_payload = encode_operational_state_manifest(manifest)
        manifest_object = writer.create_or_verify(
            bucket=bucket,
            object_name=operational_state_manifest_object_name(manifest),
            content_bytes=manifest_payload,
            content_type="application/json",
            maximum_bytes=_MAXIMUM_MANIFEST_BYTES,
        )
        return CompletedSwingOperationalStatePublication(
            manifest=manifest,
            manifest_object=manifest_object,
        )
    except SwingOperationalStateError:
        raise
    except Exception:
        raise SwingOperationalStateError("operational state GCS publication failed") from None


@dataclass(frozen=True, slots=True)
class SwingOperationalStateRestoreRequest:
    bucket: str
    manifest_object_name: str
    generation: int
    expected_sha256: str
    expected_spec_id: str

    def __post_init__(self) -> None:
        _bucket(self.bucket)
        _sha(self.expected_sha256, "expected_sha256")
        _sha(self.expected_spec_id, "expected_spec_id")
        if (
            type(self.generation) is not int
            or self.generation <= 0
            or self.generation > _MAXIMUM_GENERATION
        ):
            raise SwingOperationalStateError("manifest generation is invalid")
        if type(self.manifest_object_name) is not str:
            raise SwingOperationalStateError("manifest object name is invalid")
        match = _MANIFEST_PATH.fullmatch(self.manifest_object_name)
        if match is None or match.group(2) != self.expected_spec_id:
            raise SwingOperationalStateError("manifest object name is invalid")
        try:
            if date.fromisoformat(match.group(1)).isoformat() != match.group(1):
                raise ValueError
        except Exception:
            raise SwingOperationalStateError("manifest object name is invalid") from None


def _restore_request(value: object) -> SwingOperationalStateRestoreRequest:
    if type(value) is not SwingOperationalStateRestoreRequest:
        raise SwingOperationalStateError("restore request must be exact")
    return SwingOperationalStateRestoreRequest(
        bucket=value.bucket,
        manifest_object_name=value.manifest_object_name,
        generation=value.generation,
        expected_sha256=value.expected_sha256,
        expected_spec_id=value.expected_spec_id,
    )


@dataclass(frozen=True, slots=True)
class CompletedSwingOperationalStateRestore:
    request: SwingOperationalStateRestoreRequest
    manifest: SwingOperationalStateManifest
    record: SwingOperationalRunRecord
    notification: SwingDecisionNotification | None
    paper_registration: PaperTradeRegistration | None

    def __post_init__(self) -> None:
        request = _restore_request(self.request)
        if type(self.manifest) is not SwingOperationalStateManifest:
            raise SwingOperationalStateError("restored manifest must be exact")
        self.manifest.verify_content_identity()
        if type(self.record) is not SwingOperationalRunRecord:
            raise SwingOperationalStateError("restored record must be exact")
        self.record.verify_content_identity()
        _verify_restored_values(
            self.manifest,
            self.record,
            self.notification,
            self.paper_registration,
        )
        object.__setattr__(self, "request", request)


def _read_generation(
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
        or type(payload.generation) is not int
        or payload.generation != published.generation
        or type(payload.content_bytes) is not bytes
        or not (0 < len(payload.content_bytes) <= maximum_bytes)
        or len(payload.content_bytes) != published.byte_count
        or hashlib.sha256(payload.content_bytes).hexdigest() != published.sha256
    ):
        raise SwingOperationalStateError("operational state object verification failed")
    return payload.content_bytes


def _verify_restored_values(
    manifest: SwingOperationalStateManifest,
    record: SwingOperationalRunRecord,
    notification: SwingDecisionNotification | None,
    registration: PaperTradeRegistration | None,
) -> None:
    if (
        record.spec_id != manifest.spec_id
        or record.record_id != manifest.record_id
        or record.target_session != manifest.target_session
        or record.status is not manifest.status
        or record.action is not manifest.action
        or record.decision_id != manifest.decision_id
        or record.notification_id != manifest.notification_id
        or record.paper_registration_id != manifest.paper_registration_id
    ):
        raise SwingOperationalStateError("restored terminal record differs from manifest")
    if record.status is SwingOperationalStatus.COMPLETE:
        if type(notification) is not SwingDecisionNotification:
            raise SwingOperationalStateError("restored notification is missing")
        _verify_notification(record, notification)
    elif notification is not None:
        raise SwingOperationalStateError("failed run cannot restore a notification")
    if record.paper_registration_id is not None:
        if type(registration) is not PaperTradeRegistration:
            raise SwingOperationalStateError("restored paper registration is missing")
        _verify_registration(record, registration)
    elif registration is not None:
        raise SwingOperationalStateError("unexpected paper registration was restored")


def restore_swing_operational_state_from_gcs(
    *,
    request: SwingOperationalStateRestoreRequest,
    reader: GCSObjectReader,
    run_store: LocalSwingOperationalRunStore,
    decision_outbox: LocalSwingDecisionOutbox,
    paper_ledger: LocalPaperTradeLedger,
) -> CompletedSwingOperationalStateRestore:
    """Restore one externally pinned publication, writing the terminal record last."""

    request = _restore_request(request)
    if type(run_store) is not LocalSwingOperationalRunStore:
        raise SwingOperationalStateError("run store must be exact")
    if type(decision_outbox) is not LocalSwingDecisionOutbox:
        raise SwingOperationalStateError("decision outbox must be exact")
    if type(paper_ledger) is not LocalPaperTradeLedger:
        raise SwingOperationalStateError("paper ledger must be exact")
    try:
        manifest_payload = reader.read_generation(
            bucket=request.bucket,
            object_name=request.manifest_object_name,
            generation=request.generation,
            maximum_bytes=_MAXIMUM_MANIFEST_BYTES,
        )
        if (
            type(manifest_payload) is not GCSObjectPayload
            or type(manifest_payload.generation) is not int
            or manifest_payload.generation != request.generation
            or type(manifest_payload.content_bytes) is not bytes
            or not (0 < len(manifest_payload.content_bytes) <= _MAXIMUM_MANIFEST_BYTES)
            or hashlib.sha256(manifest_payload.content_bytes).hexdigest()
            != request.expected_sha256
        ):
            raise SwingOperationalStateError("pinned manifest verification failed")
        manifest = decode_operational_state_manifest(manifest_payload.content_bytes)
        if (
            manifest.bucket != request.bucket
            or manifest.spec_id != request.expected_spec_id
            or operational_state_manifest_object_name(manifest)
            != request.manifest_object_name
        ):
            raise SwingOperationalStateError("pinned manifest binding differs")

        decoded: dict[SwingOperationalStateArtifactKind, object] = {}
        for artifact in manifest.artifacts:
            payload = _read_generation(
                reader=reader,
                bucket=request.bucket,
                published=artifact.published_object,
                maximum_bytes=_maximum_bytes(artifact.kind),
            )
            if artifact.kind is SwingOperationalStateArtifactKind.NOTIFICATION:
                decoded[artifact.kind] = decode_swing_decision_notification(payload)
            elif artifact.kind is SwingOperationalStateArtifactKind.PAPER_REGISTRATION:
                decoded[artifact.kind] = decode_paper_trade_registration(payload)
            else:
                decoded[artifact.kind] = decode_operational_run_record(payload)

        record = decoded[SwingOperationalStateArtifactKind.RUN_RECORD]
        notification = decoded.get(SwingOperationalStateArtifactKind.NOTIFICATION)
        registration = decoded.get(
            SwingOperationalStateArtifactKind.PAPER_REGISTRATION
        )
        if type(record) is not SwingOperationalRunRecord:
            raise SwingOperationalStateError("restored record is invalid")
        if notification is not None and type(notification) is not SwingDecisionNotification:
            raise SwingOperationalStateError("restored notification is invalid")
        if registration is not None and type(registration) is not PaperTradeRegistration:
            raise SwingOperationalStateError("restored paper registration is invalid")
        _verify_restored_values(manifest, record, notification, registration)

        if notification is not None:
            decision_outbox.put_notification(notification)
        if registration is not None:
            paper_ledger.register_value(registration)
        stored_record = run_store.put_record(record)
        return CompletedSwingOperationalStateRestore(
            request=request,
            manifest=manifest,
            record=stored_record,
            notification=notification,
            paper_registration=registration,
        )
    except SwingOperationalStateError:
        raise
    except Exception:
        raise SwingOperationalStateError("operational state GCS restoration failed") from None
