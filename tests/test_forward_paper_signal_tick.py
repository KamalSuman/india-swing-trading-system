from __future__ import annotations

import contextlib
import inspect
import io
import json
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from india_swing.forward_paper import signal_tick as signal_tick_module
from india_swing.forward_paper.signal_tick import (
    ExactForwardPaperTickPanelResolver,
    ForwardPaperSignalTickConflict,
    ForwardPaperSignalTickError,
    LocalForwardPaperSignalTickPanelStore,
    decode_forward_paper_signal_tick_panel,
    encode_forward_paper_signal_tick_panel,
    materialize_forward_paper_signal_tick_panel,
)
from india_swing.forward_paper.signal_tick_cli import main
from india_swing.reference_data.security_master import NseCmSecurityMasterParser

from tests.test_reference_data_import import security_master_bytes, security_row


FILENAME = "NSE_CM_security_31072026.csv.gz"
KNOWN = datetime(2026, 7, 31, 12, 30, tzinfo=timezone.utc)
CUTOFF = datetime(2026, 7, 31, 12, 31, tzinfo=timezone.utc)


def _parsed():
    return NseCmSecurityMasterParser().parse_bytes(
        security_master_bytes(
            [
                security_row(BidIntrvl="5"),
                security_row(
                    FinInstrmId="2000",
                    TckrSymb="PERMITTED1",
                    ISIN="INE467B01029",
                    PrtdToTrad="1",
                ),
                security_row(
                    FinInstrmId="3000",
                    TckrSymb="EXCLUDED",
                    ISIN="INE002A01018",
                    PrtdToTrad="2",
                ),
            ]
        ),
        original_filename=FILENAME,
    )


def _panel():
    return materialize_forward_paper_signal_tick_panel(
        _parsed(), knowledge_time=KNOWN, cutoff=CUTOFF
    )


class ForwardPaperSignalTickTests(unittest.TestCase):
    def test_materializes_only_active_normal_market_equity(self) -> None:
        panel = _panel()
        self.assertEqual(panel.signal_session.isoformat(), "2026-07-31")
        self.assertEqual(len(panel.entries), 2)
        self.assertEqual(panel.excluded_record_count, 1)
        entry = next(value for value in panel.entries if value.symbol == "INFY")
        self.assertEqual(entry.symbol, "INFY")
        self.assertEqual(entry.tick_specification.tick_size, Decimal("0.05"))
        self.assertEqual(entry.market_session, panel.signal_session)
        self.assertEqual(entry.tick_specification.knowledge_time, KNOWN)
        self.assertIn("PERMITTED1", {value.symbol for value in panel.entries})
        panel.verify_content_identity()

    def test_same_exact_source_produces_same_identity(self) -> None:
        self.assertEqual(_panel().panel_id, _panel().panel_id)

    def test_future_known_source_fails_closed(self) -> None:
        with self.assertRaises(ForwardPaperSignalTickError):
            materialize_forward_paper_signal_tick_panel(
                _parsed(), knowledge_time=CUTOFF, cutoff=KNOWN
            )

    def test_codec_round_trip_is_byte_canonical(self) -> None:
        panel = _panel()
        payload = encode_forward_paper_signal_tick_panel(panel)
        restored = decode_forward_paper_signal_tick_panel(payload)
        self.assertEqual(restored.panel_id, panel.panel_id)
        self.assertEqual(encode_forward_paper_signal_tick_panel(restored), payload)

    def test_codec_rejects_duplicate_and_noncanonical_payloads(self) -> None:
        payload = encode_forward_paper_signal_tick_panel(_panel())
        duplicate = payload.replace(
            b'{"codec_version":', b'{"codec_version":"duplicate","codec_version":', 1
        )
        with self.assertRaises(ForwardPaperSignalTickError):
            decode_forward_paper_signal_tick_panel(duplicate)
        with self.assertRaises(ForwardPaperSignalTickError):
            decode_forward_paper_signal_tick_panel(payload[:-1] + b" \n")

    def test_store_create_verify_tamper_and_exact_resolver(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            store = LocalForwardPaperSignalTickPanelStore(root)
            panel = _panel()
            first = store.put(panel)
            second = store.put(panel)
            self.assertEqual(first.panel_id, second.panel_id)
            legacy = _FailIfCalled()
            resolved = ExactForwardPaperTickPanelResolver(store, legacy).get(
                panel.panel_id
            )
            self.assertEqual(resolved.panel_id, panel.panel_id)
            self.assertEqual(legacy.calls, 0)
            store.path_for(panel.panel_id).write_bytes(b"tampered")
            with self.assertRaises(ForwardPaperSignalTickConflict):
                store.put(panel)

    def test_cli_materializes_exact_file_without_listing(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / FILENAME
            source.write_bytes(security_master_bytes())
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                result = main(
                    [
                        "--security-master",
                        str(source),
                        "--knowledge-time",
                        KNOWN.isoformat(),
                        "--cutoff",
                        CUTOFF.isoformat(),
                        "--state-root",
                        str(root),
                    ]
                )
            body = json.loads(output.getvalue())
            self.assertEqual(result, 0)
            self.assertEqual(body["status"], "FORWARD_PAPER_SIGNAL_TICK_READY")
            restored = LocalForwardPaperSignalTickPanelStore(root).get(
                body["panel_id"]
            )
            self.assertEqual(restored.signal_session.isoformat(), "2026-07-31")

    def test_module_has_no_clock_network_provider_or_execution_capability(self) -> None:
        source = inspect.getsource(signal_tick_module).lower()
        for token in (
            "datetime.now(",
            "os.environ",
            "requests.",
            "google.cloud",
            "kite",
            "telegram",
            "place_order",
            "send_alert",
            "list_blobs",
        ):
            self.assertNotIn(token, source)


class _FailIfCalled:
    def __init__(self) -> None:
        self.calls = 0

    def get(self, _panel_id: str):
        self.calls += 1
        raise AssertionError("legacy fallback must not be called")


if __name__ == "__main__":
    unittest.main()
