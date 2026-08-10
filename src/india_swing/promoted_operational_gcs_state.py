"""Immutable, generation-pinned GCS publication and restoration boundary for
one already-verified promoted paper-operational result.

Modeled on ``src/india_swing/operations/gcs_state.py``, but composes only
the accepted promoted-operational record types
(``PromotedOperationalTerminalRecord``/``PromotedOperationalAdvisoryRecord``/
``PaperTradeRegistration``), their existing strict codecs, and
``verify_promoted_operational_published_bundle``. It never reproduces any
strategy, quote, allocation, risk, sizing, terminal-binding, or paper-
registration logic, and it never modifies an existing accepted module.

Publication writes the exact advisory, an optional paper-trade
registration, and the terminal record (in that canonical dependency order),
then seals one manifest last. Restoration reads one externally pinned
manifest generation first, independently re-verifies every pinned artifact
generation/hash, restores the accepted local parent stores (advisory
outbox, paper ledger) first, and writes the terminal store last -- so a
partial crash can leave immutable orphan GCS artifacts or an unfinished
local restore, but never a locally-stored terminal record whose referenced
advisory/registration were never durably restored alongside it.

This module accesses no environment variable, wall clock, filesystem path,
network SDK, credential, subprocess, notification, Telegram, broker API,
order, live store, or ambient/default GCP client. Every cloud-shaped
capability arrives only through an injected ``StateObjectWriter``/
``GCSObjectReader`` port; every local write arrives only through the three
existing exact local store types. It performs no deployment and no live
call, and it grants no notification or execution authority: every record it
publishes or restores is already permanently ``paper_only=True`` and
``notification_eligible=False``/``execution_eligible=False`` on its own
accepted terminal/advisory type.
"""

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
from india_swing.promoted_operational_persistence import (
    LocalPromotedOperationalAdvisoryOutbox,
    LocalPromotedOperationalTerminalStore,
    PromotedOperationalAdvisoryRecord,
    PromotedOperationalDecisionAction,
    PromotedOperationalRunStatus,
    PromotedOperationalTerminalRecord,
    decode_promoted_operational_advisory,
    decode_promoted_operational_terminal_record,
    encode_promoted_operational_advisory,
    encode_promoted_operational_terminal_record,
    verify_promoted_operational_published_bundle,
)


_BUCKET = re.compile(r"[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_MANIFEST_PATH = re.compile(
    r"promoted-operational-state/v1/(\d{4}-\d{2}-\d{2})/([0-9a-f]{64})/"
    r"manifests/([0-9a-f]{64})\.json\Z"
)
_MAXIMUM_GENERATION = 9_223_372_036_854_775_807
_MAXIMUM_MANIFEST_BYTES = 256 * 1024
_MAXIMUM_ADVISORY_BYTES = 512 * 1024
_MAXIMUM_TERMINAL_BYTES = 512 * 1024
_MAXIMUM_REGISTRATION_BYTES = 1024 * 1024
_SCHEMA_VERSION = 1


class PromotedOperationalGCSStateError(ValueError):
    pass


class PromotedOperationalGCSArtifactKind(str, Enum):
    ADVISORY = "ADVISORY"
    PAPER_REGISTRATION = "PAPER_REGISTRATION"
    TERMINAL = "TERMINAL"


_CANONICAL_ARTIFACT_ORDER = (
    PromotedOperationalGCSArtifactKind.ADVISORY,
    PromotedOperationalGCSArtifactKind.PAPER_REGISTRATION,
    PromotedOperationalGCSArtifactKind.TERMINAL,
)


def _sha(value: object, name: str) -> str:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise PromotedOperationalGCSStateError(f"{name} must be a lowercase SHA-256")
    return value


def _optional_sha(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _sha(value, name)


def _bucket(value: object) -> str:
    if type(value) is not str or _BUCKET.fullmatch(value) is None:
        raise PromotedOperationalGCSStateError("promoted operational state bucket is invalid")
    return value


def _published(value: object) -> PublishedStateObject:
    if type(value) is not PublishedStateObject:
        raise PromotedOperationalGCSStateError("published state object must be exact")
    reconstruction_failed = False
    reconstructed: PublishedStateObject | None = None
    try:
        reconstructed = PublishedStateObject(
            object_name=value.object_name,
            generation=value.generation,
            byte_count=value.byte_count,
            sha256=value.sha256,
        )
    except Exception:
        reconstruction_failed = True
    if reconstruction_failed or reconstructed is None:
        raise PromotedOperationalGCSStateError("published state object must be exact")
    return reconstructed


def _maximum_bytes(kind: PromotedOperationalGCSArtifactKind) -> int:
    return {
        PromotedOperationalGCSArtifactKind.ADVISORY: _MAXIMUM_ADVISORY_BYTES,
        PromotedOperationalGCSArtifactKind.PAPER_REGISTRATION: _MAXIMUM_REGISTRATION_BYTES,
        PromotedOperationalGCSArtifactKind.TERMINAL: _MAXIMUM_TERMINAL_BYTES,
    }[kind]


@dataclass(frozen=True, slots=True)
class PromotedOperationalGCSArtifact:
    kind: PromotedOperationalGCSArtifactKind
    published_object: PublishedStateObject

    def __post_init__(self) -> None:
        if type(self.kind) is not PromotedOperationalGCSArtifactKind:
            raise PromotedOperationalGCSStateError("promoted operational artifact kind must be exact")
        published = _published(self.published_object)
        if published.byte_count > _maximum_bytes(self.kind):
            raise PromotedOperationalGCSStateError("promoted operational artifact is oversized")
        object.__setattr__(self, "published_object", published)


def _artifact_object_name(
    *, target_session: date, spec_id: str, kind: PromotedOperationalGCSArtifactKind, identity: str
) -> str:
    return (
        f"promoted-operational-state/v1/{target_session.isoformat()}/{spec_id}/"
        f"artifacts/{kind.value.lower()}/{identity}.json"
    )


def _artifact_identity(
    manifest: "PromotedOperationalGCSManifest", kind: PromotedOperationalGCSArtifactKind
) -> str:
    if kind is PromotedOperationalGCSArtifactKind.ADVISORY:
        return manifest.advisory_id
    if kind is PromotedOperationalGCSArtifactKind.PAPER_REGISTRATION:
        if manifest.paper_registration_id is None:
            raise PromotedOperationalGCSStateError("paper registration identity is missing")
        return manifest.paper_registration_id
    return manifest.terminal_id


def promoted_operational_gcs_artifact_object_name(
    manifest: "PromotedOperationalGCSManifest", kind: PromotedOperationalGCSArtifactKind
) -> str:
    if type(manifest) is not PromotedOperationalGCSManifest:
        raise PromotedOperationalGCSStateError("promoted operational manifest must be exact")
    if type(kind) is not PromotedOperationalGCSArtifactKind:
        raise PromotedOperationalGCSStateError("promoted operational artifact kind must be exact")
    return _artifact_object_name(
        target_session=manifest.target_session, spec_id=manifest.spec_id, kind=kind,
        identity=_artifact_identity(manifest, kind),
    )


@dataclass(frozen=True, slots=True)
class PromotedOperationalGCSManifest:
    bucket: str
    spec_id: str
    preparation_id: str
    target_session: date
    terminal_id: str
    status: PromotedOperationalRunStatus
    action: PromotedOperationalDecisionAction
    advisory_id: str
    paper_registration_id: str | None
    artifacts: tuple[PromotedOperationalGCSArtifact, ...]
    schema_version: int = _SCHEMA_VERSION
    publication_id: str = field(init=False)

    def __post_init__(self) -> None:
        _bucket(self.bucket)
        _sha(self.spec_id, "spec_id")
        _sha(self.preparation_id, "preparation_id")
        _sha(self.terminal_id, "terminal_id")
        _sha(self.advisory_id, "advisory_id")
        _optional_sha(self.paper_registration_id, "paper_registration_id")
        if type(self.target_session) is not date:
            raise PromotedOperationalGCSStateError("target_session must be exact")
        if type(self.status) is not PromotedOperationalRunStatus:
            raise PromotedOperationalGCSStateError("promoted operational status must be exact")
        if type(self.action) is not PromotedOperationalDecisionAction:
            raise PromotedOperationalGCSStateError("promoted operational action must be exact")
        if type(self.artifacts) is not tuple:
            raise PromotedOperationalGCSStateError("promoted operational artifacts must be a tuple")
        reconstructed: list[PromotedOperationalGCSArtifact] = []
        for value in self.artifacts:
            if type(value) is not PromotedOperationalGCSArtifact:
                raise PromotedOperationalGCSStateError("promoted operational artifact must be exact")
            reconstructed.append(
                PromotedOperationalGCSArtifact(kind=value.kind, published_object=value.published_object)
            )
        artifacts = tuple(reconstructed)
        kinds = tuple(value.kind for value in artifacts)
        required = (
            _CANONICAL_ARTIFACT_ORDER
            if self.paper_registration_id is not None
            else (PromotedOperationalGCSArtifactKind.ADVISORY, PromotedOperationalGCSArtifactKind.TERMINAL)
        )
        if kinds != required:
            raise PromotedOperationalGCSStateError("promoted operational artifact set is invalid")

        if self.status is PromotedOperationalRunStatus.FAILED:
            if self.action is not PromotedOperationalDecisionAction.NO_TRADE or self.paper_registration_id is not None:
                raise PromotedOperationalGCSStateError("failed publication lineage is invalid")
        else:
            if (self.action is PromotedOperationalDecisionAction.PAPER_BUY) != (
                self.paper_registration_id is not None
            ):
                raise PromotedOperationalGCSStateError("paper publication lineage is invalid")

        if type(self.schema_version) is not int or self.schema_version != _SCHEMA_VERSION:
            raise PromotedOperationalGCSStateError("promoted operational state schema is unsupported")
        object.__setattr__(self, "artifacts", artifacts)
        for artifact in artifacts:
            if artifact.published_object.object_name != promoted_operational_gcs_artifact_object_name(
                self, artifact.kind
            ):
                raise PromotedOperationalGCSStateError("promoted operational artifact path differs")
        object.__setattr__(self, "publication_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(_manifest_body(self, include_publication_id=False), length=64)

    def verify_content_identity(self) -> None:
        fresh = PromotedOperationalGCSManifest(
            **{
                item.name: getattr(self, item.name)
                for item in fields(self)
                if item.name != "publication_id"
            }
        )
        if self.publication_id != fresh.publication_id:
            raise PromotedOperationalGCSStateError("promoted operational state identity failed")


def promoted_operational_gcs_manifest_object_name(manifest: PromotedOperationalGCSManifest) -> str:
    if type(manifest) is not PromotedOperationalGCSManifest:
        raise PromotedOperationalGCSStateError("promoted operational manifest must be exact")
    manifest.verify_content_identity()
    return (
        f"promoted-operational-state/v1/{manifest.target_session.isoformat()}/{manifest.spec_id}/"
        f"manifests/{manifest.publication_id}.json"
    )


def _published_body(value: PublishedStateObject) -> dict[str, object]:
    return {
        "byte_count": value.byte_count,
        "generation": value.generation,
        "object_name": value.object_name,
        "sha256": value.sha256,
    }


def _manifest_body(value: PromotedOperationalGCSManifest, *, include_publication_id: bool) -> dict[str, object]:
    body: dict[str, object] = {
        "action": value.action.value,
        "advisory_id": value.advisory_id,
        "artifacts": [
            {"kind": item.kind.value, "published_object": _published_body(item.published_object)}
            for item in value.artifacts
        ],
        "bucket": value.bucket,
        "paper_registration_id": value.paper_registration_id,
        "preparation_id": value.preparation_id,
        "schema_version": value.schema_version,
        "spec_id": value.spec_id,
        "status": value.status.value,
        "target_session": value.target_session.isoformat(),
        "terminal_id": value.terminal_id,
    }
    if include_publication_id:
        body["publication_id"] = value.publication_id
    return body


def encode_promoted_operational_gcs_manifest(value: PromotedOperationalGCSManifest) -> bytes:
    if type(value) is not PromotedOperationalGCSManifest:
        raise PromotedOperationalGCSStateError("promoted operational manifest must be exact")
    value.verify_content_identity()
    payload = (
        json.dumps(
            _manifest_body(value, include_publication_id=True),
            allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(payload) > _MAXIMUM_MANIFEST_BYTES:
        raise PromotedOperationalGCSStateError("promoted operational manifest encoding exceeds its bounded size")
    return payload


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise PromotedOperationalGCSStateError("promoted operational state JSON has duplicate keys")
        result[key] = value
    return result


def _exact(value: object, keys: set[str], name: str) -> dict[str, object]:
    if type(value) is not dict or set(value) != keys:
        raise PromotedOperationalGCSStateError(f"promoted operational {name} fields are invalid")
    return value


def _reject_number(_token: str) -> object:
    raise PromotedOperationalGCSStateError("promoted operational state JSON contains a rejected numeric literal")


def decode_promoted_operational_gcs_manifest(payload: bytes) -> PromotedOperationalGCSManifest:
    if type(payload) is not bytes or not (0 < len(payload) <= _MAXIMUM_MANIFEST_BYTES):
        raise PromotedOperationalGCSStateError("promoted operational manifest payload is invalid")
    decode_failed = False
    identity_failed = False
    canonical_failed = False
    manifest: PromotedOperationalGCSManifest | None = None
    try:
        text = payload.decode("utf-8", errors="strict")
        raw = json.loads(
            text, object_pairs_hook=_unique_object, parse_float=_reject_number, parse_constant=_reject_number,
        )
        root = _exact(
            raw,
            {
                "action", "advisory_id", "artifacts", "bucket", "paper_registration_id", "preparation_id",
                "publication_id", "schema_version", "spec_id", "status", "target_session", "terminal_id",
            },
            "state manifest",
        )
        raw_artifacts = root["artifacts"]
        if type(raw_artifacts) is not list:
            raise PromotedOperationalGCSStateError("promoted operational artifacts are invalid")
        artifacts: list[PromotedOperationalGCSArtifact] = []
        for raw_artifact in raw_artifacts:
            artifact = _exact(raw_artifact, {"kind", "published_object"}, "artifact")
            published = _exact(
                artifact["published_object"], {"byte_count", "generation", "object_name", "sha256"}, "published object",
            )
            artifacts.append(
                PromotedOperationalGCSArtifact(
                    kind=PromotedOperationalGCSArtifactKind(artifact["kind"]),
                    published_object=PublishedStateObject(**published),
                )
            )
        manifest = PromotedOperationalGCSManifest(
            bucket=root["bucket"],
            spec_id=root["spec_id"],
            preparation_id=root["preparation_id"],
            target_session=date.fromisoformat(root["target_session"]),
            terminal_id=root["terminal_id"],
            status=PromotedOperationalRunStatus(root["status"]),
            action=PromotedOperationalDecisionAction(root["action"]),
            advisory_id=root["advisory_id"],
            paper_registration_id=root["paper_registration_id"],
            artifacts=tuple(artifacts),
            schema_version=root["schema_version"],
        )
        if manifest.publication_id != root["publication_id"]:
            identity_failed = True
        elif encode_promoted_operational_gcs_manifest(manifest) != payload:
            canonical_failed = True
    except Exception:
        decode_failed = True
    if identity_failed:
        raise PromotedOperationalGCSStateError("stored publication identity differs")
    if canonical_failed:
        raise PromotedOperationalGCSStateError("promoted operational manifest is not canonical")
    if decode_failed or manifest is None:
        raise PromotedOperationalGCSStateError("stored promoted operational manifest is invalid")
    return manifest


@dataclass(frozen=True, slots=True)
class CompletedPromotedOperationalGCSPublication:
    manifest: PromotedOperationalGCSManifest
    manifest_object: PublishedStateObject

    def __post_init__(self) -> None:
        if type(self.manifest) is not PromotedOperationalGCSManifest:
            raise PromotedOperationalGCSStateError("publication manifest must be exact")
        self.manifest.verify_content_identity()
        published = _published(self.manifest_object)
        payload = encode_promoted_operational_gcs_manifest(self.manifest)
        if (
            published.object_name != promoted_operational_gcs_manifest_object_name(self.manifest)
            or published.byte_count != len(payload)
            or published.sha256 != hashlib.sha256(payload).hexdigest()
        ):
            raise PromotedOperationalGCSStateError("published manifest object differs")
        object.__setattr__(self, "manifest_object", published)


def _artifact_path_for_terminal(
    terminal: PromotedOperationalTerminalRecord, kind: PromotedOperationalGCSArtifactKind
) -> str:
    identity = {
        PromotedOperationalGCSArtifactKind.ADVISORY: terminal.advisory_id,
        PromotedOperationalGCSArtifactKind.PAPER_REGISTRATION: terminal.paper_registration_id,
        PromotedOperationalGCSArtifactKind.TERMINAL: terminal.terminal_id,
    }[kind]
    if identity is None:
        raise PromotedOperationalGCSStateError("promoted operational artifact identity is missing")
    return _artifact_object_name(
        target_session=terminal.target_session, spec_id=terminal.spec_id, kind=kind, identity=identity,
    )


def publish_promoted_operational_state_to_gcs(
    *,
    terminal: PromotedOperationalTerminalRecord,
    bucket: str,
    writer: StateObjectWriter,
    advisory_outbox: LocalPromotedOperationalAdvisoryOutbox,
    paper_ledger: LocalPaperTradeLedger,
) -> CompletedPromotedOperationalGCSPublication:
    """Publish the exact advisory, optional paper registration, and terminal
    record, then seal a manifest. Resolves the advisory only by
    ``terminal.advisory_id`` and the optional registration only by
    ``terminal.paper_registration_id`` -- never a caller-supplied value --
    and independently re-verifies the full cross-artifact bundle before any
    writer call."""

    if type(terminal) is not PromotedOperationalTerminalRecord:
        raise PromotedOperationalGCSStateError("promoted operational terminal record must be exact")
    terminal_invalid = False
    try:
        terminal.verify_content_identity()
    except Exception:
        terminal_invalid = True
    if terminal_invalid:
        raise PromotedOperationalGCSStateError("promoted operational terminal record must be exact")
    bucket = _bucket(bucket)
    if type(advisory_outbox) is not LocalPromotedOperationalAdvisoryOutbox:
        raise PromotedOperationalGCSStateError("advisory outbox must be exact")
    if type(paper_ledger) is not LocalPaperTradeLedger:
        raise PromotedOperationalGCSStateError("paper ledger must be exact")

    inputs_failed = False
    payloads: list[tuple[PromotedOperationalGCSArtifactKind, bytes]] = []
    try:
        advisory = advisory_outbox.get(terminal.advisory_id)
        registration: PaperTradeRegistration | None = None
        if terminal.paper_registration_id is not None:
            registration = paper_ledger.get_registration(terminal.paper_registration_id)
        verify_promoted_operational_published_bundle(terminal, advisory, registration)
        payloads.append(
            (PromotedOperationalGCSArtifactKind.ADVISORY, encode_promoted_operational_advisory(advisory))
        )
        if registration is not None:
            payloads.append(
                (PromotedOperationalGCSArtifactKind.PAPER_REGISTRATION, encode_paper_trade_registration(registration))
            )
        payloads.append(
            (PromotedOperationalGCSArtifactKind.TERMINAL, encode_promoted_operational_terminal_record(terminal))
        )
    except Exception:
        inputs_failed = True
    if inputs_failed:
        raise PromotedOperationalGCSStateError("promoted operational state inputs are invalid")

    write_failed = False
    artifacts: list[PromotedOperationalGCSArtifact] = []
    manifest: PromotedOperationalGCSManifest | None = None
    manifest_object: PublishedStateObject | None = None
    try:
        for kind, payload in payloads:
            object_name = _artifact_path_for_terminal(terminal, kind)
            published = writer.create_or_verify(
                bucket=bucket, object_name=object_name, content_bytes=payload,
                content_type="application/json", maximum_bytes=_maximum_bytes(kind),
            )
            if (
                type(published) is not PublishedStateObject
                or published.object_name != object_name
                or published.byte_count != len(payload)
                or published.sha256 != hashlib.sha256(payload).hexdigest()
            ):
                write_failed = True
                break
            artifacts.append(PromotedOperationalGCSArtifact(kind=kind, published_object=published))

        if not write_failed:
            manifest = PromotedOperationalGCSManifest(
                bucket=bucket,
                spec_id=terminal.spec_id,
                preparation_id=terminal.preparation_id,
                target_session=terminal.target_session,
                terminal_id=terminal.terminal_id,
                status=terminal.status,
                action=terminal.action,
                advisory_id=terminal.advisory_id,
                paper_registration_id=terminal.paper_registration_id,
                artifacts=tuple(artifacts),
            )
            manifest_payload = encode_promoted_operational_gcs_manifest(manifest)
            manifest_object = writer.create_or_verify(
                bucket=bucket, object_name=promoted_operational_gcs_manifest_object_name(manifest),
                content_bytes=manifest_payload, content_type="application/json", maximum_bytes=_MAXIMUM_MANIFEST_BYTES,
            )
            if type(manifest_object) is not PublishedStateObject:
                write_failed = True
    except Exception:
        write_failed = True
    if write_failed or manifest is None or manifest_object is None:
        raise PromotedOperationalGCSStateError("promoted operational state GCS publication failed")
    return CompletedPromotedOperationalGCSPublication(manifest=manifest, manifest_object=manifest_object)


@dataclass(frozen=True, slots=True)
class PromotedOperationalGCSRestoreRequest:
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
            raise PromotedOperationalGCSStateError("manifest generation is invalid")
        if type(self.manifest_object_name) is not str:
            raise PromotedOperationalGCSStateError("manifest object name is invalid")
        match = _MANIFEST_PATH.fullmatch(self.manifest_object_name)
        if match is None or match.group(2) != self.expected_spec_id:
            raise PromotedOperationalGCSStateError("manifest object name is invalid")
        date_invalid = False
        try:
            if date.fromisoformat(match.group(1)).isoformat() != match.group(1):
                date_invalid = True
        except Exception:
            date_invalid = True
        if date_invalid:
            raise PromotedOperationalGCSStateError("manifest object name is invalid")


def _restore_request(value: object) -> PromotedOperationalGCSRestoreRequest:
    if type(value) is not PromotedOperationalGCSRestoreRequest:
        raise PromotedOperationalGCSStateError("restore request must be exact")
    return PromotedOperationalGCSRestoreRequest(
        bucket=value.bucket,
        manifest_object_name=value.manifest_object_name,
        generation=value.generation,
        expected_sha256=value.expected_sha256,
        expected_spec_id=value.expected_spec_id,
    )


def _verify_restored_values(
    manifest: PromotedOperationalGCSManifest,
    terminal: PromotedOperationalTerminalRecord,
    advisory: PromotedOperationalAdvisoryRecord,
    registration: PaperTradeRegistration | None,
) -> None:
    if (
        terminal.spec_id != manifest.spec_id
        or terminal.preparation_id != manifest.preparation_id
        or terminal.target_session != manifest.target_session
        or terminal.terminal_id != manifest.terminal_id
        or terminal.status is not manifest.status
        or terminal.action is not manifest.action
        or terminal.advisory_id != manifest.advisory_id
        or terminal.paper_registration_id != manifest.paper_registration_id
    ):
        raise PromotedOperationalGCSStateError("restored terminal record differs from manifest")
    if advisory.advisory_id != manifest.advisory_id:
        raise PromotedOperationalGCSStateError("restored advisory differs from manifest")
    if manifest.paper_registration_id is not None:
        if type(registration) is not PaperTradeRegistration or registration.registration_id != manifest.paper_registration_id:
            raise PromotedOperationalGCSStateError("restored paper registration differs from manifest")
    elif registration is not None:
        raise PromotedOperationalGCSStateError("unexpected paper registration was restored")


@dataclass(frozen=True, slots=True)
class CompletedPromotedOperationalGCSRestore:
    request: PromotedOperationalGCSRestoreRequest
    manifest: PromotedOperationalGCSManifest
    terminal: PromotedOperationalTerminalRecord
    advisory: PromotedOperationalAdvisoryRecord
    paper_registration: PaperTradeRegistration | None

    def __post_init__(self) -> None:
        request = _restore_request(self.request)
        if type(self.manifest) is not PromotedOperationalGCSManifest:
            raise PromotedOperationalGCSStateError("restored manifest must be exact")
        self.manifest.verify_content_identity()
        if type(self.terminal) is not PromotedOperationalTerminalRecord:
            raise PromotedOperationalGCSStateError("restored terminal record must be exact")
        if type(self.advisory) is not PromotedOperationalAdvisoryRecord:
            raise PromotedOperationalGCSStateError("restored advisory must be exact")
        if self.paper_registration is not None and type(self.paper_registration) is not PaperTradeRegistration:
            raise PromotedOperationalGCSStateError("restored paper registration must be exact")
        verification_failed = False
        try:
            self.terminal.verify_content_identity()
            self.advisory.verify_content_identity()
            if self.paper_registration is not None:
                self.paper_registration.verify_content_identity()
            verify_promoted_operational_published_bundle(self.terminal, self.advisory, self.paper_registration)
        except Exception:
            verification_failed = True
        if verification_failed:
            raise PromotedOperationalGCSStateError("restored promoted operational bundle is invalid")
        _verify_restored_values(self.manifest, self.terminal, self.advisory, self.paper_registration)
        object.__setattr__(self, "request", request)


def _read_generation(
    *, reader: GCSObjectReader, bucket: str, published: PublishedStateObject, maximum_bytes: int,
) -> bytes:
    payload = reader.read_generation(
        bucket=bucket, object_name=published.object_name, generation=published.generation, maximum_bytes=maximum_bytes,
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
        raise PromotedOperationalGCSStateError("promoted operational state object verification failed")
    return payload.content_bytes


def restore_promoted_operational_state_from_gcs(
    *,
    request: PromotedOperationalGCSRestoreRequest,
    reader: GCSObjectReader,
    terminal_store: LocalPromotedOperationalTerminalStore,
    advisory_outbox: LocalPromotedOperationalAdvisoryOutbox,
    paper_ledger: LocalPaperTradeLedger,
) -> CompletedPromotedOperationalGCSRestore:
    """Restore one externally pinned publication. Reads the exact manifest
    generation first, verifies every pinned artifact generation/hash before
    decoding, independently re-verifies the full cross-artifact bundle, then
    restores the advisory outbox and paper ledger before writing the
    terminal store last -- if any earlier read/decode/identity/cross-link
    or local parent write fails, the terminal store is never touched."""

    request = _restore_request(request)
    if type(terminal_store) is not LocalPromotedOperationalTerminalStore:
        raise PromotedOperationalGCSStateError("terminal store must be exact")
    if type(advisory_outbox) is not LocalPromotedOperationalAdvisoryOutbox:
        raise PromotedOperationalGCSStateError("advisory outbox must be exact")
    if type(paper_ledger) is not LocalPaperTradeLedger:
        raise PromotedOperationalGCSStateError("paper ledger must be exact")

    read_failed = False
    manifest: PromotedOperationalGCSManifest | None = None
    terminal: PromotedOperationalTerminalRecord | None = None
    advisory: PromotedOperationalAdvisoryRecord | None = None
    registration: PaperTradeRegistration | None = None
    try:
        manifest_payload = reader.read_generation(
            bucket=request.bucket, object_name=request.manifest_object_name,
            generation=request.generation, maximum_bytes=_MAXIMUM_MANIFEST_BYTES,
        )
        if (
            type(manifest_payload) is not GCSObjectPayload
            or type(manifest_payload.generation) is not int
            or manifest_payload.generation != request.generation
            or type(manifest_payload.content_bytes) is not bytes
            or not (0 < len(manifest_payload.content_bytes) <= _MAXIMUM_MANIFEST_BYTES)
            or hashlib.sha256(manifest_payload.content_bytes).hexdigest() != request.expected_sha256
        ):
            read_failed = True
        else:
            manifest = decode_promoted_operational_gcs_manifest(manifest_payload.content_bytes)
            if (
                manifest.bucket != request.bucket
                or manifest.spec_id != request.expected_spec_id
                or promoted_operational_gcs_manifest_object_name(manifest) != request.manifest_object_name
            ):
                read_failed = True
            else:
                decoded: dict[PromotedOperationalGCSArtifactKind, object] = {}
                for artifact in manifest.artifacts:
                    payload = _read_generation(
                        reader=reader, bucket=request.bucket, published=artifact.published_object,
                        maximum_bytes=_maximum_bytes(artifact.kind),
                    )
                    if artifact.kind is PromotedOperationalGCSArtifactKind.ADVISORY:
                        decoded[artifact.kind] = decode_promoted_operational_advisory(payload)
                    elif artifact.kind is PromotedOperationalGCSArtifactKind.PAPER_REGISTRATION:
                        decoded[artifact.kind] = decode_paper_trade_registration(payload)
                    else:
                        decoded[artifact.kind] = decode_promoted_operational_terminal_record(payload)

                terminal = decoded[PromotedOperationalGCSArtifactKind.TERMINAL]
                advisory = decoded[PromotedOperationalGCSArtifactKind.ADVISORY]
                registration = decoded.get(PromotedOperationalGCSArtifactKind.PAPER_REGISTRATION)
                if (
                    type(terminal) is not PromotedOperationalTerminalRecord
                    or type(advisory) is not PromotedOperationalAdvisoryRecord
                    or (registration is not None and type(registration) is not PaperTradeRegistration)
                ):
                    read_failed = True
                else:
                    verify_promoted_operational_published_bundle(terminal, advisory, registration)
                    _verify_restored_values(manifest, terminal, advisory, registration)
    except Exception:
        read_failed = True
    if read_failed or manifest is None or terminal is None or advisory is None:
        raise PromotedOperationalGCSStateError("promoted operational state GCS restoration failed")

    write_failed = False
    stored_advisory: PromotedOperationalAdvisoryRecord | None = None
    stored_registration: PaperTradeRegistration | None = None
    stored_terminal: PromotedOperationalTerminalRecord | None = None
    try:
        # A faulty store may mutate its input in place and return that same
        # object, aliasing expected/returned and making dataclass equality
        # trivially true. Canonical bytes are captured before any store call
        # so a same-object-but-mutated return is still caught.
        baseline_advisory_bytes = encode_promoted_operational_advisory(advisory)
        baseline_registration_bytes = (
            None if registration is None else encode_paper_trade_registration(registration)
        )
        baseline_terminal_bytes = encode_promoted_operational_terminal_record(terminal)

        returned_advisory = advisory_outbox.put(advisory)
        if type(returned_advisory) is not PromotedOperationalAdvisoryRecord:
            write_failed = True
        else:
            returned_advisory.verify_content_identity()
            if encode_promoted_operational_advisory(returned_advisory) != baseline_advisory_bytes:
                write_failed = True
            else:
                stored_advisory = returned_advisory

        if not write_failed:
            if registration is None:
                stored_registration = None
            else:
                returned_registration = paper_ledger.register_value(registration)
                if type(returned_registration) is not PaperTradeRegistration:
                    write_failed = True
                else:
                    returned_registration.verify_content_identity()
                    if encode_paper_trade_registration(returned_registration) != baseline_registration_bytes:
                        write_failed = True
                    else:
                        stored_registration = returned_registration

        if not write_failed:
            returned_terminal = terminal_store.put(terminal)
            if type(returned_terminal) is not PromotedOperationalTerminalRecord:
                write_failed = True
            else:
                returned_terminal.verify_content_identity()
                if encode_promoted_operational_terminal_record(returned_terminal) != baseline_terminal_bytes:
                    write_failed = True
                else:
                    stored_terminal = returned_terminal
    except Exception:
        write_failed = True
    if write_failed or stored_advisory is None or stored_terminal is None:
        raise PromotedOperationalGCSStateError("promoted operational state local restoration failed")

    result_failed = False
    result: CompletedPromotedOperationalGCSRestore | None = None
    try:
        result = CompletedPromotedOperationalGCSRestore(
            request=request, manifest=manifest, terminal=stored_terminal,
            advisory=stored_advisory, paper_registration=stored_registration,
        )
    except Exception:
        result_failed = True
    if result_failed or result is None:
        raise PromotedOperationalGCSStateError("promoted operational state local restoration failed")
    return result
