from __future__ import annotations

import json
from datetime import date, datetime

from india_swing.reference.calendar import (
    CalendarDay,
    CalendarDayKind,
    CalendarSnapshot,
    SessionWindow,
    SessionWindowPhase,
)
from india_swing.reference.models import ExternalRecordRef, ReferenceReadiness
from india_swing.reference_data.models import AcquisitionMode

from .materialization import (
    CalendarDayResolution,
    CollectionCalendarMaterialization,
    ObservedDateEvidenceBinding,
)
from .models import CalendarSourceArtifactManifest


MAXIMUM_CALENDAR_MATERIALIZATION_BYTES = 256 * 1024 * 1024

_ERR = (
    "calendar materialization payload is malformed, noncanonical, or fails "
    "content-identity verification"
)

_ROOT_KEYS = frozenset(
    {
        "schema_version",
        "policy_version",
        "materialization_id",
        "exchange",
        "segment",
        "cutoff",
        "coverage_start",
        "coverage_end",
        "readiness",
        "actionable",
        "source_manifests",
        "day_resolutions",
        "observed_evidence_bindings",
        "calendar_snapshot",
    }
)
_SOURCE_MANIFEST_KEYS = frozenset(
    {
        "schema_version",
        "manifest_id",
        "artifact_id",
        "dataset",
        "exchange",
        "segment",
        "claimed_authority",
        "acquisition_mode",
        "readiness",
        "actionable",
        "publication_time_status",
        "first_seen_at",
        "validated_at",
        "original_source_filename",
        "original_declaration_filename",
        "claimed_document_id",
        "claimed_issue_date",
        "claimed_source_url",
        "source_media_type",
        "source_byte_count",
        "source_sha256",
        "declaration_byte_count",
        "declaration_sha256",
        "normalized_byte_count",
        "normalized_sha256",
        "event_count",
        "event_ids",
        "parser_version",
        "declaration_schema_version",
        "event_schema_version",
        "event_policy_version",
        "normalized_codec_version",
        "raw_filename",
        "declaration_filename",
        "normalized_filename",
    }
)
_DAY_RESOLUTION_KEYS = frozenset(
    {
        "day",
        "state_chain_event_ids",
        "non_executable_event_ids",
        "applied_event_ids",
        "source_artifact_ids",
        "source_manifest_ids",
        "source_snapshot_id",
        "resolution_id",
    }
)
_BINDING_KEYS = frozenset(
    {
        "artifact_id",
        "cutoff",
        "knowledge_time",
        "source_bundle_artifact_id",
        "source_bundle_manifest_id",
        "observed_dates",
        "binding_id",
    }
)
_SNAPSHOT_KEYS = frozenset(
    {
        "schema_version",
        "snapshot_id",
        "version",
        "exchange",
        "segment",
        "cutoff",
        "coverage_start",
        "coverage_end",
        "source_snapshot_ids",
        "readiness",
        "days",
    }
)
_DAY_KEYS = frozenset({"day", "kind", "data_ready_at", "reference", "session_windows"})
_REFERENCE_KEYS = frozenset(
    {"event_time", "knowledge_time", "source", "content_hash", "source_snapshot_id"}
)
_WINDOW_KEYS = frozenset({"opens_at", "closes_at", "phase"})


class CalendarMaterializationCodecError(ValueError):
    pass


def _manifest(value: object) -> dict[str, object]:
    return {
        "schema_version": value.schema_version,
        "manifest_id": value.manifest_id,
        "artifact_id": value.artifact_id,
        "dataset": value.dataset,
        "exchange": value.exchange,
        "segment": value.segment,
        "claimed_authority": value.claimed_authority,
        "acquisition_mode": value.acquisition_mode.value,
        "readiness": value.readiness.value,
        "actionable": value.actionable,
        "publication_time_status": value.publication_time_status,
        "first_seen_at": value.first_seen_at.isoformat(),
        "validated_at": value.validated_at.isoformat(),
        "original_source_filename": value.original_source_filename,
        "original_declaration_filename": value.original_declaration_filename,
        "claimed_document_id": value.claimed_document_id,
        "claimed_issue_date": value.claimed_issue_date.isoformat(),
        "claimed_source_url": value.claimed_source_url,
        "source_media_type": value.source_media_type,
        "source_byte_count": value.source_byte_count,
        "source_sha256": value.source_sha256,
        "declaration_byte_count": value.declaration_byte_count,
        "declaration_sha256": value.declaration_sha256,
        "normalized_byte_count": value.normalized_byte_count,
        "normalized_sha256": value.normalized_sha256,
        "event_count": value.event_count,
        "event_ids": list(value.event_ids),
        "parser_version": value.parser_version,
        "declaration_schema_version": value.declaration_schema_version,
        "event_schema_version": value.event_schema_version,
        "event_policy_version": value.event_policy_version,
        "normalized_codec_version": value.normalized_codec_version,
        "raw_filename": value.raw_filename,
        "declaration_filename": value.declaration_filename,
        "normalized_filename": value.normalized_filename,
    }


def encode_calendar_materialization(
    materialization: CollectionCalendarMaterialization,
) -> bytes:
    if type(materialization) is not CollectionCalendarMaterialization:
        raise TypeError("calendar materialization codec requires an exact artifact")
    materialization.verify_content_identity()
    calendar = materialization.calendar_snapshot
    value = {
        "schema_version": materialization.schema_version,
        "policy_version": materialization.policy_version,
        "materialization_id": materialization.materialization_id,
        "exchange": materialization.exchange,
        "segment": materialization.segment,
        "cutoff": materialization.cutoff.isoformat(),
        "coverage_start": materialization.coverage_start.isoformat(),
        "coverage_end": materialization.coverage_end.isoformat(),
        "readiness": materialization.readiness.value,
        "actionable": materialization.actionable,
        "source_manifests": [
            _manifest(manifest) for manifest in materialization.source_manifests
        ],
        "day_resolutions": [
            {
                "day": resolution.day.isoformat(),
                "state_chain_event_ids": list(resolution.state_chain_event_ids),
                "non_executable_event_ids": list(
                    resolution.non_executable_event_ids
                ),
                "applied_event_ids": list(resolution.applied_event_ids),
                "source_artifact_ids": list(resolution.source_artifact_ids),
                "source_manifest_ids": list(resolution.source_manifest_ids),
                "source_snapshot_id": resolution.source_snapshot_id,
                "resolution_id": resolution.resolution_id,
            }
            for resolution in materialization.day_resolutions
        ],
        "observed_evidence_bindings": [
            {
                "artifact_id": binding.artifact_id,
                "cutoff": binding.cutoff.isoformat(),
                "knowledge_time": binding.knowledge_time.isoformat(),
                "source_bundle_artifact_id": binding.source_bundle_artifact_id,
                "source_bundle_manifest_id": binding.source_bundle_manifest_id,
                "observed_dates": [
                    value.isoformat() for value in binding.observed_dates
                ],
                "binding_id": binding.binding_id,
            }
            for binding in materialization.observed_evidence_bindings
        ],
        "calendar_snapshot": {
            "schema_version": calendar.schema_version,
            "snapshot_id": calendar.snapshot_id,
            "version": calendar.version,
            "exchange": calendar.exchange,
            "segment": calendar.segment,
            "cutoff": calendar.cutoff.isoformat(),
            "coverage_start": calendar.coverage_start.isoformat(),
            "coverage_end": calendar.coverage_end.isoformat(),
            "source_snapshot_ids": list(calendar.source_snapshot_ids),
            "readiness": calendar.readiness.value,
            "days": [
                {
                    "day": day.day.isoformat(),
                    "kind": day.kind.value,
                    "data_ready_at": None,
                    "reference": {
                        "event_time": day.reference.event_time.isoformat(),
                        "knowledge_time": day.reference.knowledge_time.isoformat(),
                        "source": day.reference.source,
                        "content_hash": day.reference.content_hash,
                        "source_snapshot_id": day.reference.source_snapshot_id,
                    },
                    "session_windows": [
                        {
                            "opens_at": window.opens_at.isoformat(),
                            "closes_at": window.closes_at.isoformat(),
                            "phase": window.phase.value,
                        }
                        for window in day.session_windows
                    ],
                }
                for day in calendar.days
            ],
        },
    }
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


# --------------------------------------------------------------------------
# Strict decoding: a pure trust boundary for the exact canonical bytes
# encode_calendar_materialization produces. Every nested domain type is
# reconstructed through its real constructor/enum, never a parallel DTO;
# every content-identity check that a constructor does not already
# guarantee is invoked explicitly; the final requirement that re-encoding
# the decoded object reproduces the exact input bytes is what ultimately
# rejects any noncanonical (reordered, rewhitespaced, differently-offset,
# or otherwise semantically-equivalent-but-not-identical) representation.
# --------------------------------------------------------------------------


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise CalendarMaterializationCodecError(_ERR)
        result[key] = value
    return result


def _reject_numeric(_token: str) -> None:
    raise CalendarMaterializationCodecError(_ERR)


def _object(value: object, expected_keys: frozenset) -> dict[str, object]:
    if type(value) is not dict or set(value) != expected_keys:
        raise CalendarMaterializationCodecError(_ERR)
    return value


def _array(value: object) -> list:
    if type(value) is not list:
        raise CalendarMaterializationCodecError(_ERR)
    return value


def _text(value: object) -> str:
    if type(value) is not str:
        raise CalendarMaterializationCodecError(_ERR)
    return value


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    return _text(value)


def _integer(value: object) -> int:
    if type(value) is not int:
        raise CalendarMaterializationCodecError(_ERR)
    return value


def _boolean(value: object) -> bool:
    if type(value) is not bool:
        raise CalendarMaterializationCodecError(_ERR)
    return value


def _text_tuple(value: object) -> tuple[str, ...]:
    return tuple(_text(item) for item in _array(value))


def _date_value(value: object) -> date:
    text = _text(value)
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise CalendarMaterializationCodecError(_ERR) from None


def _date_tuple(value: object) -> tuple[date, ...]:
    return tuple(_date_value(item) for item in _array(value))


def _datetime_value(value: object) -> datetime:
    text = _text(value)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        raise CalendarMaterializationCodecError(_ERR) from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CalendarMaterializationCodecError(_ERR)
    return parsed


def _enum_value(enum_type: type, value: object):
    text = _text(value)
    try:
        return enum_type(text)
    except ValueError:
        raise CalendarMaterializationCodecError(_ERR) from None


def _decode_source_manifest(raw: object) -> CalendarSourceArtifactManifest:
    value = _object(raw, _SOURCE_MANIFEST_KEYS)
    manifest = CalendarSourceArtifactManifest(
        schema_version=_text(value["schema_version"]),
        manifest_id=_text(value["manifest_id"]),
        artifact_id=_text(value["artifact_id"]),
        dataset=_text(value["dataset"]),
        exchange=_text(value["exchange"]),
        segment=_text(value["segment"]),
        claimed_authority=_text(value["claimed_authority"]),
        acquisition_mode=_enum_value(AcquisitionMode, value["acquisition_mode"]),
        readiness=_enum_value(ReferenceReadiness, value["readiness"]),
        actionable=_boolean(value["actionable"]),
        publication_time_status=_text(value["publication_time_status"]),
        first_seen_at=_datetime_value(value["first_seen_at"]),
        validated_at=_datetime_value(value["validated_at"]),
        original_source_filename=_text(value["original_source_filename"]),
        original_declaration_filename=_text(value["original_declaration_filename"]),
        claimed_document_id=_text(value["claimed_document_id"]),
        claimed_issue_date=_date_value(value["claimed_issue_date"]),
        claimed_source_url=_optional_text(value["claimed_source_url"]),
        source_media_type=_text(value["source_media_type"]),
        source_byte_count=_integer(value["source_byte_count"]),
        source_sha256=_text(value["source_sha256"]),
        declaration_byte_count=_integer(value["declaration_byte_count"]),
        declaration_sha256=_text(value["declaration_sha256"]),
        normalized_byte_count=_integer(value["normalized_byte_count"]),
        normalized_sha256=_text(value["normalized_sha256"]),
        event_count=_integer(value["event_count"]),
        event_ids=_text_tuple(value["event_ids"]),
        parser_version=_text(value["parser_version"]),
        declaration_schema_version=_text(value["declaration_schema_version"]),
        event_schema_version=_text(value["event_schema_version"]),
        event_policy_version=_text(value["event_policy_version"]),
        normalized_codec_version=_text(value["normalized_codec_version"]),
        raw_filename=_text(value["raw_filename"]),
        declaration_filename=_text(value["declaration_filename"]),
        normalized_filename=_text(value["normalized_filename"]),
    )
    # CalendarSourceArtifactManifest accepts manifest_id/artifact_id as
    # ordinary fields (not field(init=False)); the constructor alone cannot
    # detect a forged-but-syntactically-valid identifier, so its own pure
    # verify_content_identity() behavior is invoked explicitly here. Any
    # failure is an ordinary exception collapsed by this module's outer
    # boundary, not caught locally.
    manifest.verify_content_identity()
    return manifest


def _decode_day_resolution(raw: object) -> CalendarDayResolution:
    value = _object(raw, _DAY_RESOLUTION_KEYS)
    resolution_id = _text(value["resolution_id"])
    resolution = CalendarDayResolution(
        day=_date_value(value["day"]),
        state_chain_event_ids=_text_tuple(value["state_chain_event_ids"]),
        non_executable_event_ids=_text_tuple(value["non_executable_event_ids"]),
        applied_event_ids=_text_tuple(value["applied_event_ids"]),
        source_artifact_ids=_text_tuple(value["source_artifact_ids"]),
        source_manifest_ids=_text_tuple(value["source_manifest_ids"]),
        source_snapshot_id=_text(value["source_snapshot_id"]),
    )
    if resolution.resolution_id != resolution_id:
        raise CalendarMaterializationCodecError(_ERR)
    return resolution


def _decode_binding(raw: object) -> ObservedDateEvidenceBinding:
    value = _object(raw, _BINDING_KEYS)
    binding_id = _text(value["binding_id"])
    binding = ObservedDateEvidenceBinding(
        artifact_id=_text(value["artifact_id"]),
        cutoff=_datetime_value(value["cutoff"]),
        knowledge_time=_datetime_value(value["knowledge_time"]),
        source_bundle_artifact_id=_text(value["source_bundle_artifact_id"]),
        source_bundle_manifest_id=_text(value["source_bundle_manifest_id"]),
        observed_dates=_date_tuple(value["observed_dates"]),
    )
    if binding.binding_id != binding_id:
        raise CalendarMaterializationCodecError(_ERR)
    return binding


def _decode_window(raw: object) -> SessionWindow:
    value = _object(raw, _WINDOW_KEYS)
    return SessionWindow(
        opens_at=_datetime_value(value["opens_at"]),
        closes_at=_datetime_value(value["closes_at"]),
        phase=_enum_value(SessionWindowPhase, value["phase"]),
    )


def _decode_reference(raw: object) -> ExternalRecordRef:
    value = _object(raw, _REFERENCE_KEYS)
    return ExternalRecordRef(
        event_time=_datetime_value(value["event_time"]),
        knowledge_time=_datetime_value(value["knowledge_time"]),
        source=_text(value["source"]),
        content_hash=_text(value["content_hash"]),
        source_snapshot_id=_text(value["source_snapshot_id"]),
    )


def _decode_calendar_day(raw: object) -> CalendarDay:
    value = _object(raw, _DAY_KEYS)
    if value["data_ready_at"] is not None:
        # The encoder always writes null: materialize_collection_calendar
        # never produces a schedule-only day carrying a finality overlay.
        raise CalendarMaterializationCodecError(_ERR)
    windows = tuple(_decode_window(item) for item in _array(value["session_windows"]))
    return CalendarDay(
        day=_date_value(value["day"]),
        kind=_enum_value(CalendarDayKind, value["kind"]),
        reference=_decode_reference(value["reference"]),
        session_windows=windows,
        data_ready_at=None,
    )


def _decode_calendar_snapshot(raw: object) -> CalendarSnapshot:
    value = _object(raw, _SNAPSHOT_KEYS)
    snapshot_id = _text(value["snapshot_id"])
    version = _text(value["version"])
    days = tuple(_decode_calendar_day(item) for item in _array(value["days"]))
    snapshot = CalendarSnapshot(
        exchange=_text(value["exchange"]),
        segment=_text(value["segment"]),
        cutoff=_datetime_value(value["cutoff"]),
        coverage_start=_date_value(value["coverage_start"]),
        coverage_end=_date_value(value["coverage_end"]),
        days=days,
        source_snapshot_ids=_text_tuple(value["source_snapshot_ids"]),
        readiness=_enum_value(ReferenceReadiness, value["readiness"]),
        schema_version=_text(value["schema_version"]),
    )
    if snapshot.snapshot_id != snapshot_id or snapshot.version != version:
        raise CalendarMaterializationCodecError(_ERR)
    return snapshot


def decode_calendar_materialization(payload: bytes) -> CollectionCalendarMaterialization:
    """Strictly decodes the exact canonical bytes encode_calendar_materialization
    produces, returning a fully reconstructed, content-verified
    CollectionCalendarMaterialization.

    This is a pure trust boundary only: no filesystem, environment,
    current-clock, network, GCS, listing/latest, store, CLI, logger,
    subprocess, or mutation capability exists here. It never upgrades
    readiness or claims official source provenance -- decoded bytes remain
    exactly as collection-only and non-actionable as the constructors they
    pass through require.

    Rejects non-exact bytes, empty bytes, and payloads over
    MAXIMUM_CALENDAR_MATERIALIZATION_BYTES before any UTF-8/JSON work.
    Requires strict UTF-8 (no BOM), rejects duplicate JSON keys at every
    nesting level, and rejects floats/NaN/Infinity. Requires the exact key
    set at the materialization root and every nested source manifest, day
    resolution, observed-evidence binding, calendar snapshot, calendar day,
    point-in-time reference, and session-window object.

    Every nested value is reconstructed through its real domain constructor
    or enum -- CalendarSourceArtifactManifest, CalendarDayResolution,
    ObservedDateEvidenceBinding, CalendarSnapshot, CalendarDay,
    ExternalRecordRef, SessionWindow, and finally
    CollectionCalendarMaterialization itself -- never a parallel DTO. Every
    field(init=False) content-identity a constructor already computes
    (resolution_id, binding_id, snapshot_id/version, materialization_id) is
    compared against the payload's own claimed value; the one type whose
    identity fields are ordinary constructor arguments
    (CalendarSourceArtifactManifest.manifest_id/artifact_id) is verified by
    invoking that type's own pure verify_content_identity() behavior, never
    a duplicated algorithm or a store-module dependency.
    CollectionCalendarMaterialization.verify_content_identity() is invoked
    explicitly as a final cross-check, and the last
    gate -- requiring encode_calendar_materialization(decoded) == payload
    exactly -- is what ultimately rejects any noncanonical byte
    representation that individually-valid-but-differently-formatted
    fields could otherwise slip through.

    Every ordinary failure (never BaseException), including a nested
    constructor or parser error, collapses to one static, sanitized
    CalendarMaterializationCodecError with chaining suppressed. The message
    never includes raw JSON, source filenames/URLs, IDs, hashes, dates, or
    any nested exception type or text.
    """

    try:
        if type(payload) is not bytes:
            raise CalendarMaterializationCodecError(_ERR)
        if len(payload) == 0 or len(payload) > MAXIMUM_CALENDAR_MATERIALIZATION_BYTES:
            raise CalendarMaterializationCodecError(_ERR)
        if payload[:3] == b"\xef\xbb\xbf":
            raise CalendarMaterializationCodecError(_ERR)

        try:
            text = payload.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            raise CalendarMaterializationCodecError(_ERR) from None

        try:
            raw = json.loads(
                text,
                object_pairs_hook=_reject_duplicate_keys,
                parse_float=_reject_numeric,
                parse_constant=_reject_numeric,
            )
        except CalendarMaterializationCodecError:
            raise
        except (json.JSONDecodeError, RecursionError):
            raise CalendarMaterializationCodecError(_ERR) from None

        root = _object(raw, _ROOT_KEYS)
        claimed_materialization_id = _text(root["materialization_id"])

        source_manifests = tuple(
            _decode_source_manifest(item) for item in _array(root["source_manifests"])
        )
        day_resolutions = tuple(
            _decode_day_resolution(item) for item in _array(root["day_resolutions"])
        )
        observed_evidence_bindings = tuple(
            _decode_binding(item) for item in _array(root["observed_evidence_bindings"])
        )
        calendar_snapshot = _decode_calendar_snapshot(root["calendar_snapshot"])

        materialization = CollectionCalendarMaterialization(
            exchange=_text(root["exchange"]),
            segment=_text(root["segment"]),
            cutoff=_datetime_value(root["cutoff"]),
            coverage_start=_date_value(root["coverage_start"]),
            coverage_end=_date_value(root["coverage_end"]),
            source_manifests=source_manifests,
            day_resolutions=day_resolutions,
            observed_evidence_bindings=observed_evidence_bindings,
            calendar_snapshot=calendar_snapshot,
            readiness=_enum_value(ReferenceReadiness, root["readiness"]),
            actionable=_boolean(root["actionable"]),
            schema_version=_text(root["schema_version"]),
            policy_version=_text(root["policy_version"]),
        )
        if materialization.materialization_id != claimed_materialization_id:
            raise CalendarMaterializationCodecError(_ERR)

        materialization.verify_content_identity()

        if encode_calendar_materialization(materialization) != payload:
            raise CalendarMaterializationCodecError(_ERR)
    except CalendarMaterializationCodecError:
        raise
    except Exception:
        raise CalendarMaterializationCodecError(_ERR) from None
    return materialization
