from __future__ import annotations

import ast
import json
import tempfile
import unittest
from pathlib import Path

from india_swing.daily_pipeline.pinned_gcs_state_restore_spec import (
    MAXIMUM_PINNED_GCS_STATE_RESTORE_SPEC_BYTES,
    PINNED_GCS_STATE_RESTORE_SPEC_SCHEMA_VERSION,
    PinnedGCSStateRestoreSpec,
    PinnedGCSStateRestoreSpecError,
    encode_pinned_gcs_state_restore_spec,
    parse_pinned_gcs_state_restore_spec,
)
from india_swing.daily_pipeline.state_publication_acquisition import (
    PinnedStatePublicationRequest,
)

from tests.test_state_blob_acquisition import _build_control


_MODULE_PATH = (
    Path(__file__).parents[1]
    / "src"
    / "india_swing"
    / "daily_pipeline"
    / "pinned_gcs_state_restore_spec.py"
)


def _canonical_raw_bytes(value: dict[str, object]) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


class SpecTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.parent = Path(self.temporary.name).resolve()
        control, _ = _build_control()
        self.request = control.request
        self.destination = self.parent / self.request.expected_run_id
        self.spec = PinnedGCSStateRestoreSpec(
            schema_version=PINNED_GCS_STATE_RESTORE_SPEC_SCHEMA_VERSION,
            publication_request=self.request,
            destination=self.destination,
        )
        self.encoded = encode_pinned_gcs_state_restore_spec(self.spec)
        self.raw = json.loads(self.encoded)

    def tearDown(self) -> None:
        self.temporary.cleanup()


class RoundTripTests(SpecTestCase):
    def test_canonical_round_trip_is_stable_and_reconstructs_request(self) -> None:
        parsed = parse_pinned_gcs_state_restore_spec(self.encoded)
        self.assertIs(type(parsed), PinnedGCSStateRestoreSpec)
        self.assertIsNot(parsed.publication_request, self.request)
        self.assertEqual(parsed.destination, self.destination)
        self.assertEqual(encode_pinned_gcs_state_restore_spec(parsed), self.encoded)

    def test_encoder_is_deterministic_and_newline_terminated(self) -> None:
        self.assertEqual(
            encode_pinned_gcs_state_restore_spec(self.spec),
            encode_pinned_gcs_state_restore_spec(self.spec),
        )
        self.assertTrue(self.encoded.endswith(b"\n"))
        self.assertLessEqual(
            len(self.encoded),
            MAXIMUM_PINNED_GCS_STATE_RESTORE_SPEC_BYTES,
        )

    def test_encoder_rejects_wrong_type_and_mutated_spec(self) -> None:
        with self.assertRaises(PinnedGCSStateRestoreSpecError):
            encode_pinned_gcs_state_restore_spec(object())  # type: ignore[arg-type]
        object.__setattr__(self.spec, "schema_version", True)
        with self.assertRaisesRegex(
            PinnedGCSStateRestoreSpecError,
            "^pinned state restoration spec could not be encoded$",
        ):
            encode_pinned_gcs_state_restore_spec(self.spec)


class ByteAndJSONRejectionTests(SpecTestCase):
    def test_rejects_nonbytes_empty_and_oversized_input(self) -> None:
        for value in (
            "{}",
            bytearray(b"{}"),
            b"",
            b" " * (MAXIMUM_PINNED_GCS_STATE_RESTORE_SPEC_BYTES + 1),
        ):
            with self.subTest(value_type=type(value)):
                with self.assertRaises(PinnedGCSStateRestoreSpecError):
                    parse_pinned_gcs_state_restore_spec(value)  # type: ignore[arg-type]

    def test_rejects_invalid_utf8_and_malformed_json(self) -> None:
        for value in (b"\xff", b"{", b"[]"):
            with self.subTest(value=value):
                with self.assertRaises(PinnedGCSStateRestoreSpecError):
                    parse_pinned_gcs_state_restore_spec(value)

    def test_rejects_duplicate_top_level_and_nested_keys(self) -> None:
        text = self.encoded.decode("utf-8")
        duplicated_top = text.replace(
            '"schema_version":1',
            '"schema_version":1,"schema_version":1',
        ).encode("utf-8")
        duplicated_nested = text.replace(
            '"generation":',
            '"generation":1,"generation":',
        ).encode("utf-8")
        for value in (duplicated_top, duplicated_nested):
            with self.assertRaisesRegex(
                PinnedGCSStateRestoreSpecError,
                "duplicate key",
            ):
                parse_pinned_gcs_state_restore_spec(value)

    def test_rejects_floats_constants_and_overlong_integers(self) -> None:
        text = self.encoded.decode("utf-8")
        generation = self.request.generation
        variants = (
            text.replace(f'"generation":{generation}', '"generation":1.0'),
            text.replace(f'"generation":{generation}', '"generation":NaN'),
            text.replace(
                f'"generation":{generation}',
                '"generation":123456789012345678901',
            ),
        )
        for value in variants:
            with self.assertRaises(PinnedGCSStateRestoreSpecError):
                parse_pinned_gcs_state_restore_spec(value.encode("utf-8"))


class ShapeAndBindingRejectionTests(SpecTestCase):
    def test_rejects_missing_or_extra_top_level_key(self) -> None:
        missing = dict(self.raw)
        del missing["destination"]
        extra = dict(self.raw)
        extra["extra"] = "value"
        for value in (missing, extra):
            with self.assertRaises(PinnedGCSStateRestoreSpecError):
                parse_pinned_gcs_state_restore_spec(_canonical_raw_bytes(value))

    def test_rejects_bool_or_unsupported_schema_version(self) -> None:
        for value in (True, 0, 2, "1"):
            raw = dict(self.raw)
            raw["schema_version"] = value
            with self.subTest(value=value):
                with self.assertRaises(PinnedGCSStateRestoreSpecError):
                    parse_pinned_gcs_state_restore_spec(_canonical_raw_bytes(raw))

    def test_rejects_wrong_nested_request_shape(self) -> None:
        request = dict(self.raw["publication_request"])
        del request["expected_sha256"]
        raw = dict(self.raw)
        raw["publication_request"] = request
        with self.assertRaises(PinnedGCSStateRestoreSpecError):
            parse_pinned_gcs_state_restore_spec(_canonical_raw_bytes(raw))

    def test_rejects_invalid_request_fields(self) -> None:
        variants = {
            "bucket": "Bad_Bucket!",
            "generation": True,
            "expected_sha256": "not-a-hash",
            "expected_run_id": "not-a-run-id",
            "publication_object_name": "../escape.json",
        }
        for key, value in variants.items():
            request = dict(self.raw["publication_request"])
            request[key] = value
            raw = dict(self.raw)
            raw["publication_request"] = request
            with self.subTest(key=key):
                with self.assertRaises(PinnedGCSStateRestoreSpecError):
                    parse_pinned_gcs_state_restore_spec(_canonical_raw_bytes(raw))

    def test_rejects_relative_wrong_run_and_dot_dot_destinations(self) -> None:
        destinations = (
            self.request.expected_run_id,
            str(self.parent / ("f" * 64)),
            str(self.parent / "nested" / ".." / self.request.expected_run_id),
        )
        for destination in destinations:
            raw = dict(self.raw)
            raw["destination"] = destination
            with self.subTest(destination=destination):
                with self.assertRaises(PinnedGCSStateRestoreSpecError):
                    parse_pinned_gcs_state_restore_spec(_canonical_raw_bytes(raw))


class CanonicalEncodingTests(SpecTestCase):
    def test_rejects_whitespace_missing_newline_and_reordered_keys(self) -> None:
        compact_without_newline = self.encoded[:-1]
        pretty = json.dumps(self.raw, indent=2).encode("utf-8")
        reversed_raw = dict(reversed(tuple(self.raw.items())))
        reordered = (
            json.dumps(
                reversed_raw,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=False,
            )
            + "\n"
        ).encode("utf-8")
        for value in (compact_without_newline, pretty, reordered):
            with self.subTest(value=value[:20]):
                with self.assertRaisesRegex(
                    PinnedGCSStateRestoreSpecError,
                    "not canonical",
                ):
                    parse_pinned_gcs_state_restore_spec(value)

    def test_rejects_alternate_destination_separator_spelling(self) -> None:
        if "\\" not in str(self.destination):
            self.skipTest("alternate Windows separator spelling is not applicable")
        raw = dict(self.raw)
        raw["destination"] = str(self.destination).replace("\\", "/")
        with self.assertRaisesRegex(
            PinnedGCSStateRestoreSpecError,
            "not canonical",
        ):
            parse_pinned_gcs_state_restore_spec(_canonical_raw_bytes(raw))


class DirectConstructionTests(SpecTestCase):
    def test_rejects_request_subclass_and_mutation(self) -> None:
        class SubclassedRequest(PinnedStatePublicationRequest):
            pass

        subclassed = SubclassedRequest(
            bucket=self.request.bucket,
            publication_object_name=self.request.publication_object_name,
            generation=self.request.generation,
            expected_sha256=self.request.expected_sha256,
            expected_run_id=self.request.expected_run_id,
        )
        with self.assertRaises(PinnedGCSStateRestoreSpecError):
            PinnedGCSStateRestoreSpec(1, subclassed, self.destination)

        object.__setattr__(self.request, "expected_sha256", "secret-invalid-hash")
        with self.assertRaises(PinnedGCSStateRestoreSpecError) as caught:
            PinnedGCSStateRestoreSpec(1, self.request, self.destination)
        self.assertNotIn("secret", str(caught.exception))

    def test_rejects_string_destination(self) -> None:
        with self.assertRaises(PinnedGCSStateRestoreSpecError):
            PinnedGCSStateRestoreSpec(
                1,
                self.request,
                str(self.destination),  # type: ignore[arg-type]
            )


class CapabilityLockTests(unittest.TestCase):
    _EXACT_ALLOWED_IMPORTS = frozenset(
        {
            (0, "__future__", "annotations", None),
            (0, "json", None, None),
            (0, "dataclasses", "dataclass", None),
            (0, "pathlib", "Path", None),
            (1, "state_publication_acquisition", "PinnedStatePublicationRequest", None),
        }
    )

    def _module_ast(self) -> ast.Module:
        return ast.parse(_MODULE_PATH.read_text(encoding="utf-8"))

    def test_imports_match_exact_pure_codec_allowlist(self) -> None:
        actual: set[tuple[int, str, str | None, str | None]] = set()
        for node in ast.walk(self._module_ast()):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    actual.add((0, alias.name, None, alias.asname))
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    actual.add((node.level or 0, node.module or "", alias.name, alias.asname))
        self.assertEqual(actual, self._EXACT_ALLOWED_IMPORTS)

    def test_has_no_filesystem_network_environment_or_clock_capability(self) -> None:
        exact_forbidden = frozenset(
            {
                "open",
                "read",
                "write",
                "stat",
                "resolve",
                "iterdir",
                "glob",
                "now",
            }
        )
        forbidden_tokens = (
            "client",
            "storage",
            "google",
            "environ",
            "getenv",
            "latest",
            "list_blobs",
        )
        offenders: list[str] = []
        for node in ast.walk(self._module_ast()):
            if isinstance(node, ast.Name):
                candidate = node.id.lower()
            elif isinstance(node, ast.Attribute):
                candidate = node.attr.lower()
            else:
                continue
            if candidate in exact_forbidden or any(
                token in candidate for token in forbidden_tokens
            ):
                offenders.append(candidate)
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()
