from __future__ import annotations

import ast
import hashlib
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import india_swing.promoted_paper_portfolio_genesis as genesis_module
from india_swing.operations.portfolio_store import (
    LocalSwingPortfolioArtifactStore,
    SwingPortfolioEvidenceKind,
)
from india_swing.promoted_paper_portfolio_genesis import (
    GENESIS_REQUEST_SCHEMA_VERSION,
    MANUAL_RECONCILIATION_ACK,
    MAXIMUM_GENESIS_EVIDENCE_BYTES,
    MAXIMUM_GENESIS_REQUEST_BYTES,
    GenesisEvidenceDescriptor,
    LocalPromotedPortfolioEvidenceArchive,
    PromotedPaperPortfolioGenesisError,
    PromotedPaperPortfolioGenesisRequest,
    decode_promoted_paper_portfolio_genesis_request,
    encode_promoted_paper_portfolio_genesis_request,
    seal_promoted_paper_portfolio_genesis,
)
from india_swing.promoted_paper_portfolio_genesis_cli import main as cli_main


_ROOT = Path(__file__).resolve().parents[1]


class RecordingArchive:
    def __init__(self) -> None:
        self.calls: list[tuple[SwingPortfolioEvidenceKind, str, bytes]] = []

    def create_or_verify(self, kind, expected_sha256, payload):
        self.calls.append((kind, expected_sha256, payload))
        return Path("unused")


class PromotedPaperPortfolioGenesisTests(unittest.TestCase):
    def setUp(self) -> None:
        self.as_of = datetime(2026, 8, 3, 3, 44, tzinfo=timezone.utc)
        self.payloads = {
            kind: (kind.value + "\nmanual paper reconciliation\n").encode()
            for kind in SwingPortfolioEvidenceKind
        }
        self.request = PromotedPaperPortfolioGenesisRequest(
            as_of=self.as_of,
            capital=Decimal("100000"),
            manual_reconciliation_ack=MANUAL_RECONCILIATION_ACK,
            evidence=tuple(
                GenesisEvidenceDescriptor(
                    kind=kind,
                    expected_sha256=hashlib.sha256(self.payloads[kind]).hexdigest(),
                    observed_at=self.as_of - timedelta(seconds=index + 1),
                    source_version="manual-paper-reconciliation/v1",
                )
                for index, kind in enumerate(SwingPortfolioEvidenceKind)
            ),
        )

    def assert_sanitized(self, action) -> None:
        with self.assertRaises(PromotedPaperPortfolioGenesisError) as caught:
            action()
        self.assertEqual(str(caught.exception), "promoted paper portfolio genesis is invalid")
        self.assertIsNone(caught.exception.__cause__)
        self.assertIsNone(caught.exception.__context__)

    def test_request_codec_round_trip_and_strict_malformed_rejection(self) -> None:
        payload = encode_promoted_paper_portfolio_genesis_request(self.request)
        self.assertEqual(decode_promoted_paper_portfolio_genesis_request(payload), self.request)
        raw = json.loads(payload)
        mutations = []
        for name, mutate in (
            ("extra", lambda v: v.__setitem__("extra", True)),
            ("float", lambda v: v.__setitem__("capital", 100000.0)),
            ("decimal", lambda v: v.__setitem__("capital", "100000.0")),
            ("exponent", lambda v: v.__setitem__("capital", "1E+999999")),
            ("timestamp", lambda v: v.__setitem__("as_of", "2026-08-03T09:14:00+05:30")),
            ("ack", lambda v: v.__setitem__("manual_reconciliation_ack", "SECRET")),
            ("order", lambda v: v.__setitem__("evidence", list(reversed(v["evidence"])))),
            ("future", lambda v: v["evidence"][0].__setitem__("observed_at", "2026-08-03T03:45:00Z")),
            ("hash", lambda v: v["evidence"][0].__setitem__("expected_sha256", "A" * 64)),
        ):
            value = json.loads(json.dumps(raw))
            mutate(value)
            mutations.append((name, json.dumps(value, separators=(",", ":"), sort_keys=True).encode()))
        mutations.extend(
            (
                ("duplicate", payload.replace(b'{"as_of":', b'{"as_of":"2026-08-03T03:44:00Z","as_of":', 1)),
                ("oversized", b"{" + b" " * MAXIMUM_GENESIS_REQUEST_BYTES + b"}"),
            )
        )
        for name, candidate in mutations:
            with self.subTest(name=name):
                self.assert_sanitized(lambda candidate=candidate: decode_promoted_paper_portfolio_genesis_request(candidate))

    def test_happy_genesis_is_empty_artifact_last_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            archive = RecordingArchive()
            store = LocalSwingPortfolioArtifactStore(root)
            with patch.object(LocalSwingPortfolioArtifactStore, "put", wraps=store.put) as put:
                first = seal_promoted_paper_portfolio_genesis(
                    request=self.request,
                    evidence_payloads=self.payloads,
                    evidence_archive=archive,
                    portfolio_store=store,
                )
            self.assertEqual([call[0] for call in archive.calls], list(SwingPortfolioEvidenceKind))
            self.assertEqual(put.call_count, 1)
            self.assertEqual(first.portfolio.capital, Decimal("100000"))
            self.assertEqual(first.portfolio.cash_available, Decimal("100000"))
            self.assertEqual(first.portfolio.gross_exposure, 0)
            self.assertEqual(first.portfolio.open_risk, 0)
            self.assertEqual(first.portfolio.open_positions, 0)
            self.assertEqual(first.portfolio.daily_realized_pnl, 0)
            self.assertEqual(first.portfolio.pilot_realized_pnl, 0)
            second = seal_promoted_paper_portfolio_genesis(
                request=self.request,
                evidence_payloads=self.payloads,
                evidence_archive=archive,
                portfolio_store=store,
            )
            self.assertEqual(second, first)

    def test_hash_mismatch_and_request_tamper_make_zero_writes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            archive = RecordingArchive()
            store = LocalSwingPortfolioArtifactStore(root)
            bad = dict(self.payloads)
            bad[SwingPortfolioEvidenceKind.BROKER_FUNDS] += b"SECRET"
            self.assert_sanitized(lambda: seal_promoted_paper_portfolio_genesis(
                request=self.request, evidence_payloads=bad, evidence_archive=archive, portfolio_store=store
            ))
            self.assertEqual(archive.calls, [])

            original_evidence = self.request.evidence
            object.__setattr__(self.request, "evidence", (object(),) * 4)
            try:
                self.assert_sanitized(lambda: seal_promoted_paper_portfolio_genesis(
                    request=self.request, evidence_payloads=self.payloads, evidence_archive=archive, portfolio_store=store
                ))
            finally:
                object.__setattr__(self.request, "evidence", original_evidence)
            self.assertEqual(archive.calls, [])
            self.assertFalse((root / "portfolio_snapshots").exists())

            original = self.request.manual_reconciliation_ack
            object.__setattr__(self.request, "manual_reconciliation_ack", "SECRET")
            try:
                self.assert_sanitized(lambda: seal_promoted_paper_portfolio_genesis(
                    request=self.request, evidence_payloads=self.payloads, evidence_archive=archive, portfolio_store=store
                ))
            finally:
                object.__setattr__(self.request, "manual_reconciliation_ack", original)
            self.assertEqual(archive.calls, [])

    def test_local_archive_create_once_conflict_and_partial_poison_fail_closed(self) -> None:
        kind = SwingPortfolioEvidenceKind.BROKER_FUNDS
        payload = self.payloads[kind]
        digest = hashlib.sha256(payload).hexdigest()
        with tempfile.TemporaryDirectory() as directory:
            archive = LocalPromotedPortfolioEvidenceArchive(Path(directory).resolve())
            path = archive.create_or_verify(kind, digest, payload)
            self.assertEqual(path.read_bytes(), payload)
            self.assertEqual(archive.create_or_verify(kind, digest, payload), path)
            path.write_bytes(b"poison")
            self.assert_sanitized(lambda: archive.create_or_verify(kind, digest, payload))
            self.assertEqual(path.read_bytes(), b"poison")

        with tempfile.TemporaryDirectory() as directory:
            archive = LocalPromotedPortfolioEvidenceArchive(Path(directory).resolve())
            real_fdopen = os.fdopen

            class PartialHandle:
                def __init__(self, descriptor, mode, closefd=True):
                    self.handle = real_fdopen(descriptor, mode, closefd=closefd)
                def __enter__(self): return self
                def __exit__(self, *args): self.handle.close()
                def write(self, value):
                    self.handle.write(value[: max(1, len(value) // 2)])
                    self.handle.flush()
                    raise OSError("SECRET partial failure")
                def flush(self): self.handle.flush()
                def fileno(self): return self.handle.fileno()

            with patch.object(genesis_module.os, "fdopen", PartialHandle):
                self.assert_sanitized(lambda: archive.create_or_verify(kind, digest, payload))
            target = archive.path_for(kind, digest)
            poison = target.read_bytes()
            self.assertNotEqual(poison, payload)
            self.assert_sanitized(lambda: archive.create_or_verify(kind, digest, payload))
            self.assertEqual(target.read_bytes(), poison)

    def test_all_hashes_validate_before_archive_and_archive_failure_blocks_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            store = LocalSwingPortfolioArtifactStore(root)

            class FailingArchive(RecordingArchive):
                def create_or_verify(self, kind, expected_sha256, payload):
                    super().create_or_verify(kind, expected_sha256, payload)
                    if kind is SwingPortfolioEvidenceKind.ENGINE_RISK_LEDGER:
                        raise OSError("SECRET")
                    return Path("unused")

            archive = FailingArchive()
            self.assert_sanitized(lambda: seal_promoted_paper_portfolio_genesis(
                request=self.request, evidence_payloads=self.payloads, evidence_archive=archive, portfolio_store=store
            ))
            self.assertEqual([item[0] for item in archive.calls], list(SwingPortfolioEvidenceKind)[:3])
            self.assertFalse((root / "portfolio_snapshots").exists())

    def test_artifact_failure_happens_only_after_all_evidence_is_archived(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            archive = LocalPromotedPortfolioEvidenceArchive(root)
            store = LocalSwingPortfolioArtifactStore(root)
            with patch.object(store, "put", side_effect=OSError("SECRET store failure")) as put:
                self.assert_sanitized(lambda: seal_promoted_paper_portfolio_genesis(
                    request=self.request,
                    evidence_payloads=self.payloads,
                    evidence_archive=archive,
                    portfolio_store=store,
                ))
            put.assert_called_once()
            for descriptor in self.request.evidence:
                self.assertEqual(
                    archive.path_for(descriptor.kind, descriptor.expected_sha256).read_bytes(),
                    self.payloads[descriptor.kind],
                )
            self.assertFalse((root / "portfolio_snapshots").exists())

    def test_cli_success_and_failures_are_sanitized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            request_file = root / "request.json"
            request_file.write_bytes(encode_promoted_paper_portfolio_genesis_request(self.request))
            options = ["--request-file", str(request_file), "--portfolio-artifact-root", str(root / "portfolio")]
            names = {
                SwingPortfolioEvidenceKind.BROKER_FUNDS: "--broker-funds-file",
                SwingPortfolioEvidenceKind.BROKER_POSITIONS: "--broker-positions-file",
                SwingPortfolioEvidenceKind.ENGINE_RISK_LEDGER: "--engine-risk-ledger-file",
                SwingPortfolioEvidenceKind.ENGINE_PNL_LEDGER: "--engine-pnl-ledger-file",
            }
            for kind in SwingPortfolioEvidenceKind:
                path = root / f"{kind.value}.bin"
                path.write_bytes(self.payloads[kind])
                options.extend((names[kind], str(path)))
            stdout, stderr = io.StringIO(), io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = cli_main(options)
            self.assertEqual(code, 0)
            self.assertEqual(stderr.getvalue(), "")
            result = json.loads(stdout.getvalue())
            self.assertEqual(result["status"], "PORTFOLIO_GENESIS_SEALED")
            self.assertFalse(result["broker_reconciled_automatically"])
            self.assertTrue(result["paper_only"])
            self.assertNotIn(str(root), stdout.getvalue())

            stderr = io.StringIO()
            with redirect_stderr(stderr):
                code = cli_main(["--request-file", "SECRET-relative"])
            self.assertEqual(code, 2)
            self.assertEqual(json.loads(stderr.getvalue()), {"error_type": "PromotedPaperPortfolioGenesisError", "status": "FAILED"})
            self.assertNotIn("SECRET", stderr.getvalue())

    def test_cli_rejects_empty_evidence_before_creating_output_root(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            request_file = root / "request.json"
            request_file.write_bytes(encode_promoted_paper_portfolio_genesis_request(self.request))
            output = root / "not-created"
            options = ["--request-file", str(request_file), "--portfolio-artifact-root", str(output)]
            names = (
                "--broker-funds-file",
                "--broker-positions-file",
                "--engine-risk-ledger-file",
                "--engine-pnl-ledger-file",
            )
            for index, (kind, option) in enumerate(zip(SwingPortfolioEvidenceKind, names)):
                path = root / f"input-{index}.bin"
                path.write_bytes(b"" if index == 2 else self.payloads[kind])
                options.extend((option, str(path)))
            stderr = io.StringIO()
            with redirect_stderr(stderr):
                code = cli_main(options)
            self.assertEqual(code, 2)
            self.assertEqual(json.loads(stderr.getvalue())["status"], "FAILED")
            self.assertFalse(output.exists())

    def test_modules_have_no_ambient_or_execution_capability(self) -> None:
        for relative in (
            "src/india_swing/promoted_paper_portfolio_genesis.py",
            "src/india_swing/promoted_paper_portfolio_genesis_cli.py",
        ):
            source = (_ROOT / relative).read_text(encoding="utf-8")
            tree = ast.parse(source)
            names: set[str] = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Name):
                    names.add(node.id.casefold())
                elif isinstance(node, ast.Attribute):
                    names.add(node.attr.casefold())
                elif isinstance(node, ast.Import):
                    names.update(alias.name.casefold() for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    names.add(node.module.casefold())
            for forbidden in (
                "environ",
                "now",
                "utcnow",
                "kite",
                "telegram",
                "google",
                "place_order",
                "subprocess",
                "importlib",
                "eval",
                "exec",
                "glob",
                "list_blobs",
                "latest",
            ):
                self.assertNotIn(forbidden, names)


if __name__ == "__main__":
    unittest.main()
