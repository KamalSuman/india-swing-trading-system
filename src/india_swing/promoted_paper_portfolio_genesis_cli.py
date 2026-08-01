"""Sanitized offline CLI for the initial promoted paper portfolio seal."""

from __future__ import annotations

import json
import sys
from collections.abc import Sequence
from pathlib import Path

from india_swing._filesystem import read_stable_regular_file
from india_swing.operations.portfolio_store import (
    LocalSwingPortfolioArtifactStore,
    SwingPortfolioEvidenceKind,
)
from india_swing.promoted_paper_portfolio_genesis import (
    MAXIMUM_GENESIS_EVIDENCE_BYTES,
    MAXIMUM_GENESIS_REQUEST_BYTES,
    LocalPromotedPortfolioEvidenceArchive,
    PromotedPaperPortfolioGenesisError,
    canonical_utc_z_timestamp,
    decode_promoted_paper_portfolio_genesis_request,
    seal_promoted_paper_portfolio_genesis,
)


_PATH_OPTIONS = {
    "--request-file": None,
    "--portfolio-artifact-root": None,
    "--broker-funds-file": SwingPortfolioEvidenceKind.BROKER_FUNDS,
    "--broker-positions-file": SwingPortfolioEvidenceKind.BROKER_POSITIONS,
    "--engine-risk-ledger-file": SwingPortfolioEvidenceKind.ENGINE_RISK_LEDGER,
    "--engine-pnl-ledger-file": SwingPortfolioEvidenceKind.ENGINE_PNL_LEDGER,
}


def _arguments(argv: Sequence[str]) -> dict[str, Path]:
    values: dict[str, Path] = {}
    index = 0
    while index < len(argv):
        token = argv[index]
        if token not in _PATH_OPTIONS or token in values or index + 1 >= len(argv):
            raise PromotedPaperPortfolioGenesisError("promoted paper portfolio genesis is invalid")
        raw = argv[index + 1]
        path = Path(raw)
        if type(raw) is not str or not raw or not path.is_absolute() or ".." in path.parts:
            raise PromotedPaperPortfolioGenesisError("promoted paper portfolio genesis is invalid")
        values[token] = path
        index += 2
    if set(values) != set(_PATH_OPTIONS):
        raise PromotedPaperPortfolioGenesisError("promoted paper portfolio genesis is invalid")
    return values


def main(argv: Sequence[str] | None = None) -> int:
    try:
        paths = _arguments(list(argv) if argv is not None else sys.argv[1:])
        request = decode_promoted_paper_portfolio_genesis_request(
            read_stable_regular_file(paths["--request-file"], maximum_bytes=MAXIMUM_GENESIS_REQUEST_BYTES)
        )
        payloads = {
            kind: read_stable_regular_file(paths[option], maximum_bytes=MAXIMUM_GENESIS_EVIDENCE_BYTES)
            for option, kind in _PATH_OPTIONS.items()
            if kind is not None
        }
        root = paths["--portfolio-artifact-root"]
        artifact = seal_promoted_paper_portfolio_genesis(
            request=request,
            evidence_payloads=payloads,
            evidence_archive=LocalPromotedPortfolioEvidenceArchive(root),
            portfolio_store=LocalSwingPortfolioArtifactStore(root),
        )
        print(json.dumps({
            "artifact_id": artifact.artifact_id,
            "as_of": canonical_utc_z_timestamp(artifact.portfolio.as_of),
            "broker_reconciled_automatically": False,
            "capital": str(artifact.portfolio.capital),
            "evidence_binding_ids": [item.binding_id for item in artifact.evidence],
            "execution_eligible": False,
            "mode": artifact.mode,
            "notification_eligible": False,
            "paper_only": True,
            "portfolio_snapshot_id": artifact.portfolio_snapshot_id,
            "status": "PORTFOLIO_GENESIS_SEALED",
            "verification_status": artifact.verification_status.value,
        }, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
        return 0
    except Exception:
        print(json.dumps({"error_type": PromotedPaperPortfolioGenesisError.__name__, "status": "FAILED"}, separators=(",", ":"), sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
