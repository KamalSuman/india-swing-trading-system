from __future__ import annotations

import ast
import hashlib
import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from india_swing.calendar_data.artifact_store import LocalCalendarSourceArtifactStore
from india_swing.calendar_data.materialization import (
    CollectionCalendarMaterialization,
    materialize_collection_calendar,
)
from india_swing.calendar_data.materialization_codec import (
    MAXIMUM_CALENDAR_MATERIALIZATION_BYTES,
    _BINDING_KEYS,
    _DAY_KEYS,
    _DAY_RESOLUTION_KEYS,
    _REFERENCE_KEYS,
    _ROOT_KEYS,
    _SNAPSHOT_KEYS,
    _SOURCE_MANIFEST_KEYS,
    _WINDOW_KEYS,
    CalendarMaterializationCodecError,
    decode_calendar_materialization,
    encode_calendar_materialization,
)
from india_swing.calendar_data.materialization_store import MAXIMUM_MATERIALIZATION_BYTES
from india_swing.calendar_data.models import CALENDAR_DECLARATION_SCHEMA_VERSION
from india_swing.calendar_evidence import build_observed_market_date_artifact
from tests.test_calendar_evidence import stored_bundle


UTC = timezone.utc

# Reproduced deterministic fixture patterns (per architecture_contract's own
# "reuse ... but do not modify existing tests"), matching
# tests/test_calendar_materialization.py's event-declaration builders.
_MS_START = date(2026, 7, 17)  # Friday
_MS_END = date(2026, 7, 19)  # Sunday


def _window(phase: str, opens: str, closes: str) -> dict[str, str]:
    return {"phase": phase, "opens": opens, "closes": closes}


def _locator(record: str) -> dict[str, object]:
    return {"page": 1, "section": "CM schedule", "record": record}


def _base_event(
    *,
    effective_from: str = "2026-01-01",
    effective_to_exclusive: str = "2027-01-01",
) -> dict[str, object]:
    return {
        "event_type": "BASE_WEEKLY_SCHEDULE",
        "effective_from": effective_from,
        "effective_to_exclusive": effective_to_exclusive,
        "weekdays": ["MON", "TUE", "WED", "THU", "FRI"],
        "windows": [_window("LIVE_CONTINUOUS", "09:15:00", "15:30:00")],
        "supersedes_event_ids": [],
        "source_locator": _locator("regular"),
        "reason": "Regular capital-market schedule",
    }


def _closed(day: date, predecessor: str, kind: str = "HOLIDAY") -> dict[str, object]:
    return {
        "event_type": "DATE_CLOSED",
        "date": day.isoformat(),
        "day_kind": kind,
        "supersedes_event_ids": [predecessor],
        "source_locator": _locator(f"closed-{day.isoformat()}"),
        "reason": "Explicit dated closure",
    }


def _special(day: date, predecessor: str) -> dict[str, object]:
    return {
        "event_type": "DATE_SESSION_REPLACED",
        "date": day.isoformat(),
        "day_kind": "SPECIAL",
        "windows": [
            _window("PRE_OPEN", "17:45:00", "18:00:00"),
            _window("LIVE_CONTINUOUS", "18:00:00", "19:00:00"),
        ],
        "supersedes_event_ids": [predecessor],
        "source_locator": _locator(f"special-{day.isoformat()}"),
        "reason": "Exact replacement special-session schedule",
    }


def _mock(day: date) -> dict[str, object]:
    return {
        "event_type": "NON_EXECUTABLE_ACTIVITY",
        "date": day.isoformat(),
        "windows": [_window("MOCK_TEST", "11:00:00", "12:00:00")],
        "supersedes_event_ids": [],
        "source_locator": _locator(f"mock-{day.isoformat()}"),
        "reason": "Non-executable mock activity",
    }


def _mutated(payload: bytes, mutate) -> bytes:
    """Round-trips payload through plain json (not the strict decoder),
    applies `mutate` to the parsed dict in place, and re-serializes with
    the encoder's own canonical separators/sort_keys so only the intended
    field actually changed."""

    value = json.loads(payload.decode("utf-8"))
    mutate(value)
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")


def _at(value: object, path: list):
    current = value
    for step in path:
        current = current[step]
    return current


class _FixtureTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.sequence = 0

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def import_source(
        self,
        document_id: str,
        events: list[dict[str, object]],
        *,
        validated_at: datetime,
    ):
        self.sequence += 1
        source_name = f"{document_id}-{self.sequence}.pdf"
        declaration_name = f"{document_id}-{self.sequence}.events.json"
        source_bytes = f"%PDF-1.7\n{document_id}-{self.sequence}\n%%EOF\n".encode("ascii")
        input_root = self.root / "inputs"
        input_root.mkdir(exist_ok=True)
        source_path = input_root / source_name
        declaration_path = input_root / declaration_name
        source_path.write_bytes(source_bytes)
        declaration = {
            "schema_version": CALENDAR_DECLARATION_SCHEMA_VERSION,
            "exchange": "NSE",
            "segment": "CM",
            "claimed_authority": "NSE",
            "claimed_document_id": document_id,
            "claimed_issue_date": "2026-01-01",
            "claimed_source_url": f"https://example.invalid/{source_name}",
            "source_filename": source_name,
            "source_media_type": "application/pdf",
            "source_byte_count": len(source_bytes),
            "source_sha256": hashlib.sha256(source_bytes).hexdigest(),
            "events": events,
        }
        declaration_path.write_text(
            json.dumps(declaration, separators=(",", ":")), encoding="utf-8"
        )
        values = iter((validated_at - timedelta(seconds=1), validated_at))
        return LocalCalendarSourceArtifactStore(
            self.root / "archive",
            clock=lambda: next(values),
        ).import_source(source_path, declaration_path)

    def schedule_only_materialization(self) -> CollectionCalendarMaterialization:
        """Minimal fixture: one base-weekly-schedule source, no overrides,
        no observed-evidence bindings."""

        base = self.import_source(
            "CMTR-CODEC-BASE",
            [_base_event()],
            validated_at=datetime(2026, 7, 13, 12, 0, tzinfo=UTC),
        )
        return materialize_collection_calendar(
            sources=(base,),
            coverage_start=date(2026, 7, 13),  # Monday
            coverage_end=date(2026, 7, 14),  # Tuesday
            cutoff=datetime(2026, 7, 14, 16, 0, tzinfo=UTC),
        )

    def multi_day_materialization_with_evidence(self) -> CollectionCalendarMaterialization:
        """Multiple days/windows plus one observed-evidence binding."""

        base = self.import_source(
            "CMTR-CODEC-MULTI",
            [_base_event()],
            validated_at=datetime(2026, 7, 15, 13, 0, tzinfo=UTC),
        )
        bundle = stored_bundle(
            (date(2026, 7, 16),),
            storage_root=self.root / "bundle",
            first_seen=datetime(2026, 7, 16, 14, 37, tzinfo=UTC),
            validated=datetime(2026, 7, 16, 14, 37, 7, tzinfo=UTC),
        )
        evidence = build_observed_market_date_artifact(
            bundle, cutoff=datetime(2026, 7, 16, 14, 37, 8, tzinfo=UTC)
        )
        return materialize_collection_calendar(
            sources=(base,),
            coverage_start=date(2026, 7, 16),  # Thursday (session)
            coverage_end=date(2026, 7, 18),  # Saturday (weekend)
            cutoff=datetime(2026, 7, 18, 10, 0, tzinfo=UTC),
            observed_date_artifacts=(evidence,),
        )

    def multi_source_materialization(self) -> CollectionCalendarMaterialization:
        """Four distinct, all-used source manifests (base/holiday/special/
        mock), for testing reordering of an identity-bearing array."""

        base = self.import_source(
            "CMTR-CODEC-MS-BASE",
            [_base_event()],
            validated_at=datetime(2026, 7, 16, 12, 0, tzinfo=UTC),
        )
        base_id = base.parsed.events[0].event_id
        holiday = self.import_source(
            "CMTR-CODEC-MS-HOLIDAY",
            [_closed(_MS_START, base_id)],
            validated_at=datetime(2026, 7, 16, 12, 1, tzinfo=UTC),
        )
        special = self.import_source(
            "CMTR-CODEC-MS-SPECIAL",
            [_special(_MS_START + timedelta(days=1), base_id)],
            validated_at=datetime(2026, 7, 16, 12, 2, tzinfo=UTC),
        )
        mock = self.import_source(
            "CMTR-CODEC-MS-MOCK",
            [_mock(_MS_START + timedelta(days=2))],
            validated_at=datetime(2026, 7, 16, 12, 3, tzinfo=UTC),
        )
        return materialize_collection_calendar(
            sources=(base, holiday, special, mock),
            coverage_start=_MS_START,
            coverage_end=_MS_END,
            cutoff=datetime(2026, 7, 19, 16, 0, tzinfo=UTC),
        )


class RoundTripTests(_FixtureTestCase):
    def test_schedule_only_round_trip_is_exact(self) -> None:
        original = self.schedule_only_materialization()
        payload = encode_calendar_materialization(original)

        decoded = decode_calendar_materialization(payload)

        self.assertEqual(type(decoded), CollectionCalendarMaterialization)
        self.assertEqual(decoded, original)
        decoded.verify_content_identity()
        self.assertEqual(decoded.materialization_id, original.materialization_id)
        self.assertEqual(encode_calendar_materialization(decoded), payload)

    def test_multi_day_with_evidence_round_trip_is_exact(self) -> None:
        original = self.multi_day_materialization_with_evidence()
        payload = encode_calendar_materialization(original)

        decoded = decode_calendar_materialization(payload)

        self.assertEqual(type(decoded), CollectionCalendarMaterialization)
        self.assertEqual(decoded, original)
        self.assertEqual(len(decoded.observed_evidence_bindings), 1)
        decoded.verify_content_identity()
        self.assertEqual(decoded.materialization_id, original.materialization_id)
        self.assertEqual(encode_calendar_materialization(decoded), payload)

    def test_multi_source_round_trip_is_exact(self) -> None:
        original = self.multi_source_materialization()
        payload = encode_calendar_materialization(original)

        decoded = decode_calendar_materialization(payload)

        self.assertEqual(decoded, original)
        self.assertEqual(len(decoded.source_manifests), 4)
        self.assertEqual(encode_calendar_materialization(decoded), payload)


class BoundaryRejectionTests(_FixtureTestCase):
    def test_rejects_non_bytes(self) -> None:
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization("not-bytes")  # type: ignore[arg-type]

    def test_rejects_empty_bytes(self) -> None:
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(b"")

    def test_rejects_over_limit_payload_via_patched_ceiling(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        with patch(
            "india_swing.calendar_data.materialization_codec."
            "MAXIMUM_CALENDAR_MATERIALIZATION_BYTES",
            len(payload) - 1,
        ):
            with self.assertRaises(CalendarMaterializationCodecError):
                decode_calendar_materialization(payload)

    def test_accepts_content_exactly_at_the_patched_ceiling(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        with patch(
            "india_swing.calendar_data.materialization_codec."
            "MAXIMUM_CALENDAR_MATERIALIZATION_BYTES",
            len(payload),
        ):
            decode_calendar_materialization(payload)

    def test_default_ceiling_is_256_mib_and_shared_with_the_store_alias(self) -> None:
        self.assertEqual(MAXIMUM_CALENDAR_MATERIALIZATION_BYTES, 256 * 1024 * 1024)
        self.assertEqual(MAXIMUM_MATERIALIZATION_BYTES, MAXIMUM_CALENDAR_MATERIALIZATION_BYTES)

    def test_rejects_bom(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(b"\xef\xbb\xbf" + payload)

    def test_rejects_invalid_utf8(self) -> None:
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(b"\xff\xfe\x00\x01not-valid-utf8")

    def test_rejects_malformed_json(self) -> None:
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(b'{"schema_version": 1, "objects": [')

    def _duplicate_key_at(self, text: str, needle: str) -> str:
        # needle is a "key":" prefix for a quoted-string field; locate its
        # unique occurrence, capture the full "key":"value" fragment up to
        # the value's closing quote, and splice in an exact duplicate of
        # that same key/value pair immediately after the original -- this
        # keeps the surrounding document syntactically well-formed JSON so
        # only the intended duplicate-key condition is exercised.
        self.assertEqual(text.count(needle), 1)
        start = text.index(needle)
        value_end = text.index('"', start + len(needle)) + 1
        fragment = text[start:value_end]
        return text[:value_end] + "," + fragment + text[value_end:]

    def test_rejects_duplicate_root_key(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        text = payload.decode("utf-8")
        tampered = self._duplicate_key_at(text, '"materialization_id":"')
        self.assertNotEqual(tampered, text)
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(tampered.encode("utf-8"))

    def test_rejects_duplicate_nested_key(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        text = payload.decode("utf-8")
        tampered = self._duplicate_key_at(text, '"snapshot_id":"')
        self.assertNotEqual(tampered, text)
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(tampered.encode("utf-8"))

    def test_rejects_float(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        mutated = _mutated(
            payload,
            lambda v: v["source_manifests"][0].__setitem__("source_byte_count", 1.5),
        )
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(mutated)

    def test_rejects_nan_and_infinity_literals(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        text = payload.decode("utf-8")
        needle = '"source_byte_count":'
        self.assertIn(needle, text)
        start = text.index(needle) + len(needle)
        end = text.index(",", start)
        for literal in ("NaN", "Infinity", "-Infinity"):
            tampered = (text[:start] + literal + text[end:]).encode("utf-8")
            with self.assertRaises(CalendarMaterializationCodecError):
                decode_calendar_materialization(tampered)


class KeySetRejectionTests(_FixtureTestCase):
    def _assert_missing_and_extra_rejected(
        self, payload: bytes, path: list, valid_keys: frozenset
    ) -> None:
        remove_key = next(iter(valid_keys))
        missing = _mutated(payload, lambda v: _at(v, path).pop(remove_key))
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(missing)

        extra = _mutated(payload, lambda v: _at(v, path).update({"unexpected_extra": "x"}))
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(extra)

    def test_rejects_missing_and_extra_root_key(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        self._assert_missing_and_extra_rejected(payload, [], _ROOT_KEYS)

    def test_rejects_missing_and_extra_source_manifest_key(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        self._assert_missing_and_extra_rejected(
            payload, ["source_manifests", 0], _SOURCE_MANIFEST_KEYS
        )

    def test_rejects_missing_and_extra_day_resolution_key(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        self._assert_missing_and_extra_rejected(
            payload, ["day_resolutions", 0], _DAY_RESOLUTION_KEYS
        )

    def test_rejects_missing_and_extra_evidence_binding_key(self) -> None:
        payload = encode_calendar_materialization(
            self.multi_day_materialization_with_evidence()
        )
        self._assert_missing_and_extra_rejected(
            payload, ["observed_evidence_bindings", 0], _BINDING_KEYS
        )

    def test_rejects_missing_and_extra_snapshot_key(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        self._assert_missing_and_extra_rejected(payload, ["calendar_snapshot"], _SNAPSHOT_KEYS)

    def test_rejects_missing_and_extra_day_key(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        self._assert_missing_and_extra_rejected(
            payload, ["calendar_snapshot", "days", 0], _DAY_KEYS
        )

    def test_rejects_missing_and_extra_reference_key(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        self._assert_missing_and_extra_rejected(
            payload, ["calendar_snapshot", "days", 0, "reference"], _REFERENCE_KEYS
        )

    def test_rejects_missing_and_extra_window_key(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        self._assert_missing_and_extra_rejected(
            payload,
            ["calendar_snapshot", "days", 0, "session_windows", 0],
            _WINDOW_KEYS,
        )


class TypeAndCanonicalFormRejectionTests(_FixtureTestCase):
    def test_rejects_bool_as_int(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        mutated = _mutated(
            payload,
            lambda v: v["source_manifests"][0].__setitem__("source_byte_count", True),
        )
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(mutated)

    def test_rejects_int_as_bool(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        mutated = _mutated(payload, lambda v: v.__setitem__("actionable", 0))
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(mutated)

    def test_rejects_wrong_enum_value(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        mutated = _mutated(
            payload,
            lambda v: v["calendar_snapshot"]["days"][0].__setitem__("kind", "NOT_A_KIND"),
        )
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(mutated)

    def test_rejects_wrong_container_type_for_array_field(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        mutated = _mutated(payload, lambda v: v.__setitem__("source_manifests", {}))
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(mutated)

    def test_rejects_naive_datetime(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        mutated = _mutated(
            payload,
            lambda v: v["source_manifests"][0].__setitem__(
                "first_seen_at", "2026-07-13T12:00:00"
            ),
        )
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(mutated)

    def test_rejects_invalid_date(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        mutated = _mutated(payload, lambda v: v.__setitem__("coverage_start", "2026-07-32"))
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(mutated)

    def test_rejects_reordered_source_manifests_array(self) -> None:
        payload = encode_calendar_materialization(self.multi_source_materialization())

        def _swap(v: dict) -> None:
            manifests = v["source_manifests"]
            self.assertGreaterEqual(len(manifests), 2)
            manifests[0], manifests[1] = manifests[1], manifests[0]

        mutated = _mutated(payload, _swap)
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(mutated)

    def test_rejects_mutated_nested_hash(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        other_hash = hashlib.sha256(b"attacker-content").hexdigest()
        mutated = _mutated(
            payload,
            lambda v: v["source_manifests"][0].__setitem__("source_sha256", other_hash),
        )
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(mutated)

    def test_rejects_mutated_nested_id(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        other_id = "f" * 64
        mutated = _mutated(
            payload,
            lambda v: v["day_resolutions"][0].__setitem__("resolution_id", other_id),
        )
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(mutated)

    def test_rejects_mutated_top_level_materialization_id(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        mutated = _mutated(payload, lambda v: v.__setitem__("materialization_id", "0" * 64))
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(mutated)

    def test_rejects_data_ready_at_not_null(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        mutated = _mutated(
            payload,
            lambda v: v["calendar_snapshot"]["days"][0].__setitem__(
                "data_ready_at", "2026-07-13T15:45:00+05:30"
            ),
        )
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(mutated)


class CanonicalByteEqualityRejectionTests(_FixtureTestCase):
    def test_rejects_added_whitespace(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        with_whitespace = payload.replace(b'","', b'", "')
        self.assertNotEqual(with_whitespace, payload)
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(with_whitespace)

    def test_rejects_reordered_top_level_keys(self) -> None:
        payload = encode_calendar_materialization(self.schedule_only_materialization())
        value = json.loads(payload.decode("utf-8"))
        # sort_keys=True is what the encoder relies on for its canonical
        # byte form; dumping with reversed insertion order but *without*
        # sort_keys reproduces a semantically identical document with a
        # different key order and therefore different bytes.
        reordered = {key: value[key] for key in reversed(list(value.keys()))}
        reordered_bytes = json.dumps(
            reordered, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode("utf-8")
        self.assertNotEqual(reordered_bytes, payload)
        with self.assertRaises(CalendarMaterializationCodecError):
            decode_calendar_materialization(reordered_bytes)


class SanitizationTests(_FixtureTestCase):
    def _assert_sanitized(self, exc: Exception, *secrets: str) -> None:
        message = str(exc)
        rendered = repr(exc)
        for secret in secrets:
            self.assertNotIn(secret, message)
            self.assertNotIn(secret, rendered)

    def test_secret_bearing_source_fields_never_leak(self) -> None:
        secret_url = "https://example.invalid/SECRET-URL-DO-NOT-LEAK-4f2a.pdf"
        secret_filename = "SECRET-FILENAME-DO-NOT-LEAK-91bd.pdf"
        secret_hash = hashlib.sha256(b"SECRET-CONTENT-DO-NOT-LEAK").hexdigest()

        payload = encode_calendar_materialization(self.schedule_only_materialization())

        def _inject(v: dict) -> None:
            manifest = v["source_manifests"][0]
            manifest["claimed_source_url"] = secret_url
            manifest["original_source_filename"] = secret_filename
            manifest["source_sha256"] = secret_hash

        mutated = _mutated(payload, _inject)
        with self.assertRaises(CalendarMaterializationCodecError) as ctx:
            decode_calendar_materialization(mutated)
        self._assert_sanitized(ctx.exception, secret_url, secret_filename, secret_hash)

    def test_secret_bearing_nested_error_sentinel_never_leaks(self) -> None:
        secret = "SECRET-NESTED-CONSTRUCTOR-ERROR-DO-NOT-LEAK-7c3e"
        payload = encode_calendar_materialization(self.schedule_only_materialization())

        # A malformed claimed_document_id makes CalendarSourceArtifactManifest's
        # own constructor raise a ValueError; embed the sentinel in that
        # rejected value to prove the nested exception's own text (which
        # normally does not even echo the value, but this proves no
        # incidental leak occurs regardless of what a future constructor
        # change might include) never surfaces.
        mutated = _mutated(
            payload,
            lambda v: v["source_manifests"][0].__setitem__(
                "claimed_document_id", f"lowercase-{secret}"
            ),
        )
        with self.assertRaises(CalendarMaterializationCodecError) as ctx:
            decode_calendar_materialization(mutated)
        self._assert_sanitized(ctx.exception, secret)
        self.assertIsNone(ctx.exception.__cause__)


class CapabilityLockTests(unittest.TestCase):
    """Proves materialization_codec.py's decode path introduces no
    filesystem, environment, current-clock, network, GCS, listing/latest,
    store, CLI, logger, subprocess, or mutation capability."""

    def _module_ast(self) -> ast.Module:
        source = (
            Path(__file__).resolve().parent.parent
            / "src"
            / "india_swing"
            / "calendar_data"
            / "materialization_codec.py"
        ).read_text(encoding="utf-8")
        return ast.parse(source)

    def test_no_disallowed_capability_token_anywhere_in_the_module(self) -> None:
        # Exact-identifier matches only: several legitimate domain field
        # names/builtins incidentally *contain* capability-sounding
        # substrings (e.g. SessionWindow.opens_at contains "open",
        # knowledge_time contains "now", and the pre-existing encoder's own
        # list(...) calls are the builtin constructor, not a store listing
        # operation), so blanket substring matching produces false
        # positives against this module's real, allowed vocabulary.
        exact_forbidden_tokens = frozenset(
            {
                "open",
                "environ",
                "getenv",
                "now",
                "utcnow",
                "today",
                "socket",
                "requests",
                "urllib",
                "http",
                "google",
                "storage",
                "client",
                "latest",
                "subprocess",
                "logging",
                "logger",
                "print",
                "input",
            }
        )
        tree = self._module_ast()
        offenders = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Name):
                candidate = node.id.lower()
            elif isinstance(node, ast.Attribute):
                candidate = node.attr.lower()
            else:
                continue
            if candidate in exact_forbidden_tokens:
                offenders.append(candidate)
        self.assertEqual(offenders, [])

    def test_imports_no_filesystem_network_or_store_module(self) -> None:
        forbidden_roots = {
            "os",
            "pathlib",
            "socket",
            "subprocess",
            "logging",
            "shutil",
            "tempfile",
            "requests",
            "urllib",
        }
        tree = self._module_ast()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                # node.module is the dotted target even for a relative
                # import (e.g. `from .artifact_store import x` has
                # module == "artifact_store", level == 1), so this also
                # catches same-package store-module imports.
                names = [node.module or ""]
            else:
                continue
            for name in names:
                self.assertNotIn(name.split(".")[0], forbidden_roots, name)
                # No direct or relative dependency on this module's own
                # store siblings (artifact_store, materialization_store, or
                # any other *_store module) is ever allowed: the decoder
                # must verify content identity only through the pure model
                # layer, never a filesystem-backed store module.
                self.assertNotIn("store", name.lower(), name)

    def test_decode_never_touches_the_filesystem(self) -> None:
        # Structural proof only: decode_calendar_materialization takes bytes
        # and returns a value object with no path/handle anywhere in its
        # signature or (per the AST tests above) its imports.
        import inspect

        from india_swing.calendar_data.materialization_codec import (
            decode_calendar_materialization as target,
        )

        signature = inspect.signature(target)
        self.assertEqual(list(signature.parameters), ["payload"])
        # The module uses `from __future__ import annotations`, so
        # annotations are lazily-stringified rather than resolved classes.
        self.assertEqual(signature.parameters["payload"].annotation, "bytes")


if __name__ == "__main__":
    unittest.main()
