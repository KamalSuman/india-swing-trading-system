"""Sanitized offline CLI for the promoted paper-pilot first-session
packager.

Reads one explicit local session-package-request file, the ten explicit
promoted-preparation roots, the portfolio-artifact root, one explicit
paper-portfolio genesis request file, the four exact evidence files the
accepted genesis boundary requires, and one explicit create-once output
assembly-spec file. Resolves and durably publishes the operational
preparation, seals the initial empty paper portfolio, dry-assembles the
existing ``PromotedOperationalLaunchRequest``, and publishes the
resulting assembly spec -- in that exact order.

This CLI never reads an environment variable or the current time, and
never constructs a Kite/GCP/Telegram/network/broker/runtime client --
this packager is entirely offline. The next commands remain the already-
accepted cloud-control preparer and input publisher; this CLI does not
run them.
"""

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
from india_swing.promoted_operational_preparation import (
    build_promoted_operational_preparation_store,
)
from india_swing.promoted_paper_pilot_session_package import (
    MAXIMUM_SESSION_PACKAGE_REQUEST_BYTES,
    PromotedPaperPilotSessionPackageError,
    decode_promoted_paper_pilot_session_package_request,
    prepare_promoted_paper_pilot_first_session_package,
)
from india_swing.promoted_paper_portfolio_genesis import (
    MAXIMUM_GENESIS_EVIDENCE_BYTES,
    MAXIMUM_GENESIS_REQUEST_BYTES,
    LocalPromotedPortfolioEvidenceArchive,
    decode_promoted_paper_portfolio_genesis_request,
)

_ERR = "promoted paper pilot session package call is invalid"

_ROOT_OPTIONS = (
    "--reference-root",
    "--identity-evidence-root",
    "--calendar-root",
    "--daily-reports-root",
    "--historical-corpus-root",
    "--promoted-root",
    "--graph-publication-root",
    "--engine-run-root",
    "--research-run-root",
    "--operational-preparation-root",
)
_EVIDENCE_OPTIONS = {
    "--broker-funds-file": SwingPortfolioEvidenceKind.BROKER_FUNDS,
    "--broker-positions-file": SwingPortfolioEvidenceKind.BROKER_POSITIONS,
    "--engine-risk-ledger-file": SwingPortfolioEvidenceKind.ENGINE_RISK_LEDGER,
    "--engine-pnl-ledger-file": SwingPortfolioEvidenceKind.ENGINE_PNL_LEDGER,
}
_OPTIONS = (
    ("--package-request-file",)
    + _ROOT_OPTIONS
    + ("--portfolio-artifact-root", "--genesis-request-file")
    + tuple(_EVIDENCE_OPTIONS)
    + ("--output-assembly-spec-file",)
)


def _arguments(argv: Sequence[str]) -> dict[str, Path]:
    values: dict[str, str] = {}
    index = 0
    while index < len(argv):
        token = argv[index]
        if token not in _OPTIONS or token in values:
            raise PromotedPaperPilotSessionPackageError(_ERR)
        if index + 1 >= len(argv):
            raise PromotedPaperPilotSessionPackageError(_ERR)
        value = argv[index + 1]
        if type(value) is not str or not value:
            raise PromotedPaperPilotSessionPackageError(_ERR)
        values[token] = value
        index += 2
    if set(values) != set(_OPTIONS):
        raise PromotedPaperPilotSessionPackageError(_ERR)

    paths: dict[str, Path] = {}
    for token, raw in values.items():
        path = Path(raw)
        if not path.is_absolute() or ".." in path.parts:
            raise PromotedPaperPilotSessionPackageError(_ERR)
        paths[token] = path
    return paths


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv) if argv is not None else sys.argv[1:]
    try:
        paths = _arguments(args)

        package_request = decode_promoted_paper_pilot_session_package_request(
            read_stable_regular_file(
                paths["--package-request-file"], maximum_bytes=MAXIMUM_SESSION_PACKAGE_REQUEST_BYTES
            )
        )
        genesis_request = decode_promoted_paper_portfolio_genesis_request(
            read_stable_regular_file(
                paths["--genesis-request-file"], maximum_bytes=MAXIMUM_GENESIS_REQUEST_BYTES
            )
        )
        genesis_evidence_payloads = {
            kind: read_stable_regular_file(paths[option], maximum_bytes=MAXIMUM_GENESIS_EVIDENCE_BYTES)
            for option, kind in _EVIDENCE_OPTIONS.items()
        }

        research_stores, preparations = build_promoted_operational_preparation_store(
            reference_root=paths["--reference-root"],
            identity_evidence_root=paths["--identity-evidence-root"],
            calendar_root=paths["--calendar-root"],
            daily_reports_root=paths["--daily-reports-root"],
            historical_corpus_root=paths["--historical-corpus-root"],
            promoted_root=paths["--promoted-root"],
            graph_publication_root=paths["--graph-publication-root"],
            engine_run_root=paths["--engine-run-root"],
            research_run_root=paths["--research-run-root"],
            operational_preparation_root=paths["--operational-preparation-root"],
        )
        portfolio_artifact_root = paths["--portfolio-artifact-root"]
        portfolio_store = LocalSwingPortfolioArtifactStore(portfolio_artifact_root)
        evidence_archive = LocalPromotedPortfolioEvidenceArchive(portfolio_artifact_root)

        result = prepare_promoted_paper_pilot_first_session_package(
            package_request=package_request,
            genesis_request=genesis_request,
            genesis_evidence_payloads=genesis_evidence_payloads,
            research_stores=research_stores,
            preparations=preparations,
            evidence_archive=evidence_archive,
            portfolio_store=portfolio_store,
            output_assembly_spec_file=paths["--output-assembly-spec-file"],
        )

        envelope = {
            "status": "PROMOTED_PAPER_PILOT_SESSION_PACKAGE_READY",
            "target_session": result.target_session.isoformat(),
            "research_run_id": result.research_run_id,
            "preparation_id": result.preparation_id,
            "portfolio_artifact_id": result.portfolio_artifact_id,
            "portfolio_snapshot_id": result.portfolio_snapshot_id,
            "assembly_spec_id": result.assembly_spec_id,
            "candidate_count": result.candidate_count,
            "open_position_count": result.open_position_count,
            "paper_only": True,
            "notification_eligible": False,
            "execution_eligible": False,
        }
        print(
            json.dumps(
                envelope, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True,
            )
        )
        return 0
    except Exception:
        print(
            json.dumps(
                {"error_type": PromotedPaperPilotSessionPackageError.__name__, "status": "FAILED"},
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
