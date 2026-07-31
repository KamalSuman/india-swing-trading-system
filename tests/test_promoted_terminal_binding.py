from __future__ import annotations

import inspect
import json
import unittest
from datetime import datetime, timedelta, timezone

from india_swing.promoted_operational_persistence import (
    build_promoted_operational_advisory,
    build_promoted_operational_terminal_record,
    promoted_paper_registration_from_result,
)
from india_swing.promoted_operational_service import TrustedPromotedOperationalTerminalBinding
from india_swing.promoted_terminal_binding import (
    MAXIMUM_TERMINAL_BINDING_BYTES,
    PromotedOperationalTerminalBindingRecord,
    PromotedTerminalBindingError,
    build_promoted_operational_terminal_binding_record,
    decode_promoted_operational_terminal_binding_record,
    encode_promoted_operational_terminal_binding_record,
    promoted_operational_terminal_binding_object_name,
    trusted_binding_from_record,
)

from tests import test_promoted_operational_persistence as _persistence_tests


def _flip_hex(value: str) -> str:
    replacement = "1" if value[0] == "0" else "0"
    return replacement + value[1:]


class PromotedTerminalBindingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.result = _persistence_tests._complete_paper_buy_result()
        self.spec = self.result.spec
        advisory = build_promoted_operational_advisory(self.result)
        registration = promoted_paper_registration_from_result(self.result, advisory)
        self.terminal = build_promoted_operational_terminal_record(
            self.result, advisory, registration
        )

    def test_binding_record_is_derived_exactly_from_terminal_and_spec_without_clock_or_invention(
        self,
    ) -> None:
        record = build_promoted_operational_terminal_binding_record(self.terminal, self.spec)
        manifest = self.spec.quote_gate_spec.preparation.manifest
        self.assertEqual(record.spec_id, self.spec.spec_id)
        self.assertEqual(record.target_session, manifest.target_session)
        self.assertEqual(record.preparation_id, manifest.preparation_id)
        self.assertEqual(record.expected_terminal_id, self.terminal.terminal_id)
        self.assertEqual(record.terminal_completed_at, self.terminal.completed_at)
        record.verify_content_identity()
        self.assertEqual(
            build_promoted_operational_terminal_binding_record(
                self.terminal, self.spec
            ).binding_id,
            record.binding_id,
        )

    def test_binding_record_rejects_foreign_spec_mismatched_session_preparation_or_terminal_identity(
        self,
    ) -> None:
        other_result = _persistence_tests._complete_no_trade_result()
        with self.assertRaises(PromotedTerminalBindingError):
            build_promoted_operational_terminal_binding_record(self.terminal, other_result.spec)

        with self.assertRaises(PromotedTerminalBindingError):
            build_promoted_operational_terminal_binding_record("not-a-terminal", self.spec)
        with self.assertRaises(PromotedTerminalBindingError):
            build_promoted_operational_terminal_binding_record(self.terminal, "not-a-spec")

        manifest = self.spec.quote_gate_spec.preparation.manifest
        base_kwargs = dict(
            spec_id=self.spec.spec_id,
            target_session=manifest.target_session,
            preparation_id=manifest.preparation_id,
            expected_terminal_id=self.terminal.terminal_id,
            terminal_completed_at=self.terminal.completed_at,
        )

        with self.subTest(case="naive_datetime"):
            with self.assertRaises(PromotedTerminalBindingError):
                PromotedOperationalTerminalBindingRecord(
                    **{**base_kwargs, "terminal_completed_at": datetime(2026, 1, 1, 12, 0, 0)}
                )

        with self.subTest(case="non_utc_offset"):
            with self.assertRaises(PromotedTerminalBindingError):
                PromotedOperationalTerminalBindingRecord(
                    **{
                        **base_kwargs,
                        "terminal_completed_at": datetime(
                            2026,
                            1,
                            1,
                            12,
                            0,
                            0,
                            tzinfo=timezone(timedelta(hours=5, minutes=30)),
                        ),
                    }
                )

        with self.subTest(case="datetime_where_date_required"):
            with self.assertRaises(PromotedTerminalBindingError):
                PromotedOperationalTerminalBindingRecord(
                    **{
                        **base_kwargs,
                        "target_session": datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                    }
                )

        with self.subTest(case="non_lowercase_sha"):
            with self.assertRaises(PromotedTerminalBindingError):
                PromotedOperationalTerminalBindingRecord(**{**base_kwargs, "spec_id": "A" * 64})

        with self.subTest(case="short_sha"):
            with self.assertRaises(PromotedTerminalBindingError):
                PromotedOperationalTerminalBindingRecord(
                    **{**base_kwargs, "preparation_id": "a" * 63}
                )

    def test_binding_object_name_is_derived_only_from_the_live_spec_and_is_never_listed_or_latest_selected(
        self,
    ) -> None:
        name = promoted_operational_terminal_binding_object_name(self.spec)
        manifest = self.spec.quote_gate_spec.preparation.manifest
        expected = (
            f"promoted-operational/terminal-bindings/"
            f"{manifest.target_session.isoformat()}/{self.spec.spec_id}.json"
        )
        self.assertEqual(name, expected)

        import india_swing.promoted_terminal_binding as module

        module_function_names = [
            member_name for member_name, _ in inspect.getmembers(module, inspect.isfunction)
        ]
        name_deriving = [
            member_name for member_name in module_function_names if "object_name" in member_name
        ]
        self.assertEqual(name_deriving, ["promoted_operational_terminal_binding_object_name"])
        signature = inspect.signature(promoted_operational_terminal_binding_object_name)
        self.assertEqual(list(signature.parameters), ["spec"])

    def test_binding_codec_rejects_duplicate_unknown_missing_float_malformed_oversized_noncanonical_and_identity_mismatch(
        self,
    ) -> None:
        record = build_promoted_operational_terminal_binding_record(self.terminal, self.spec)
        good = encode_promoted_operational_terminal_binding_record(record)
        self.assertEqual(
            decode_promoted_operational_terminal_binding_record(good).binding_id,
            record.binding_id,
        )

        with self.subTest(case="duplicate_key_envelope_level"):
            envelope_kv = b'"codec_schema_version":"promoted-operational-terminal-binding-json/v1"'
            self.assertIn(envelope_kv, good)
            malformed = good.replace(envelope_kv, envelope_kv + b"," + envelope_kv, 1)
            with self.assertRaises(PromotedTerminalBindingError):
                decode_promoted_operational_terminal_binding_record(malformed)

        with self.subTest(case="duplicate_key_body_level"):
            body_kv = ('"binding_id":"' + record.binding_id + '"').encode()
            self.assertIn(body_kv, good)
            malformed = good.replace(body_kv, body_kv + b"," + body_kv, 1)
            with self.assertRaises(PromotedTerminalBindingError):
                decode_promoted_operational_terminal_binding_record(malformed)

        with self.subTest(case="unknown_key"):
            malformed = good.replace(b'"binding_id"', b'"unexpected_field":true,"binding_id"', 1)
            with self.assertRaises(PromotedTerminalBindingError):
                decode_promoted_operational_terminal_binding_record(malformed)

        with self.subTest(case="missing_key"):
            schema_kv = ('"schema_version":"' + record.schema_version + '",').encode()
            self.assertIn(schema_kv, good)
            malformed = good.replace(schema_kv, b"", 1)
            with self.assertRaises(PromotedTerminalBindingError):
                decode_promoted_operational_terminal_binding_record(malformed)

        with self.subTest(case="float_value"):
            completed_kv = (
                '"terminal_completed_at":"' + record.terminal_completed_at.isoformat() + '"'
            ).encode()
            self.assertIn(completed_kv, good)
            malformed = good.replace(completed_kv, b'"terminal_completed_at":1.5', 1)
            with self.assertRaises(PromotedTerminalBindingError):
                decode_promoted_operational_terminal_binding_record(malformed)

        with self.subTest(case="invalid_utf8"):
            with self.assertRaises(PromotedTerminalBindingError):
                decode_promoted_operational_terminal_binding_record(b"\xff\xfe\x00\x01")

        with self.subTest(case="oversized"):
            huge = good + b" " * MAXIMUM_TERMINAL_BINDING_BYTES
            with self.assertRaises(PromotedTerminalBindingError):
                decode_promoted_operational_terminal_binding_record(huge)

        with self.subTest(case="noncanonical_encoding"):
            reformatted = json.dumps(json.loads(good), sort_keys=False, indent=2).encode("utf-8")
            with self.assertRaises(PromotedTerminalBindingError):
                decode_promoted_operational_terminal_binding_record(reformatted)

        with self.subTest(case="binding_id_mismatch"):
            tampered_id = "0" * 64 if record.binding_id[0] != "0" else "1" * 64
            malformed = good.replace(
                ('"binding_id":"' + record.binding_id + '"').encode(),
                ('"binding_id":"' + tampered_id + '"').encode(),
                1,
            )
            with self.assertRaises(PromotedTerminalBindingError):
                decode_promoted_operational_terminal_binding_record(malformed)

    def test_trusted_binding_projection_returns_exact_service_type_and_rejects_spec_mismatch(
        self,
    ) -> None:
        record = build_promoted_operational_terminal_binding_record(self.terminal, self.spec)
        result = trusted_binding_from_record(record, self.spec)
        self.assertIs(type(result), TrustedPromotedOperationalTerminalBinding)
        self.assertEqual(result.spec_id, record.spec_id)
        self.assertEqual(result.expected_terminal_id, record.expected_terminal_id)

        other_result = _persistence_tests._complete_no_trade_result()
        with self.assertRaises(PromotedTerminalBindingError):
            trusted_binding_from_record(record, other_result.spec)

        manifest = self.spec.quote_gate_spec.preparation.manifest
        wrong_session_record = PromotedOperationalTerminalBindingRecord(
            spec_id=record.spec_id,
            target_session=manifest.target_session + timedelta(days=1),
            preparation_id=record.preparation_id,
            expected_terminal_id=record.expected_terminal_id,
            terminal_completed_at=record.terminal_completed_at,
        )
        with self.assertRaises(PromotedTerminalBindingError):
            trusted_binding_from_record(wrong_session_record, self.spec)

        wrong_preparation_record = PromotedOperationalTerminalBindingRecord(
            spec_id=record.spec_id,
            target_session=record.target_session,
            preparation_id=_flip_hex(record.preparation_id),
            expected_terminal_id=record.expected_terminal_id,
            terminal_completed_at=record.terminal_completed_at,
        )
        with self.assertRaises(PromotedTerminalBindingError):
            trusted_binding_from_record(wrong_preparation_record, self.spec)


if __name__ == "__main__":
    unittest.main()
