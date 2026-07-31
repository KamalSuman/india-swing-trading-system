"""CLI for the restart-safe, exact-manifest promoted-research-to-
operational-preparation bridge.

Every invocation is explicit: the ten durable roots required to reconstruct
the promoted research stores plus the new preparation store, and one exact
``--research-run-id``. There is no discovery, no latest-selection, and no
network/broker/Telegram capability anywhere in this command. This bridge
never creates or coerces a ``SwingProposalBatch``/``SwingTechnicalProposal``:
it retains the exact selected ``PromotedResearchTradeIntent`` objects a
promoted research run produced, with a derived canonical NSE listing key and
target session, and nothing more. A zero-candidate preparation is a normal,
successful, auditable result -- not an exception.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from india_swing.promoted_operational_preparation import (
    build_promoted_operational_preparation_store,
    prepare_and_publish,
)


class _CliArgumentError(Exception):
    """Raised by SanitizedArgumentParser instead of printing usage and exiting."""


class SanitizedArgumentParser(argparse.ArgumentParser):
    """An ArgumentParser whose parse failures never print raw argparse text.

    The default ``ArgumentParser.error`` prints a usage message (which can
    include argument values) and calls ``sys.exit(2)`` directly, bypassing
    any surrounding try/except. Raising instead lets ``main`` catch every
    parse failure -- missing required arguments, unknown options, malformed
    values -- through the same sanitized ``{status: FAILED, error_type}``
    boundary as every other failure. ``-h``/``--help`` is unaffected: it
    exits via ``SystemExit`` before ever reaching ``error``.
    """

    def error(self, message: str) -> None:
        raise _CliArgumentError(message)


def build_parser() -> argparse.ArgumentParser:
    parser = SanitizedArgumentParser(
        prog="india-swing-promoted-operational-prepare",
        description=(
            "Prepare one restart-safe, paper-only operational-preparation "
            "boundary from one exact, already-published promoted research "
            "run."
        ),
    )
    parser.add_argument("--reference-root", required=True, type=Path)
    parser.add_argument("--identity-evidence-root", required=True, type=Path)
    parser.add_argument("--calendar-root", required=True, type=Path)
    parser.add_argument("--daily-reports-root", required=True, type=Path)
    parser.add_argument("--historical-corpus-root", required=True, type=Path)
    parser.add_argument("--promoted-root", required=True, type=Path)
    parser.add_argument("--graph-publication-root", required=True, type=Path)
    parser.add_argument("--engine-run-root", required=True, type=Path)
    parser.add_argument("--research-run-root", required=True, type=Path)
    parser.add_argument("--operational-preparation-root", required=True, type=Path)
    parser.add_argument("--research-run-id", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        parser = build_parser()
        args = parser.parse_args(argv)
        research_stores, preparations = build_promoted_operational_preparation_store(
            reference_root=args.reference_root,
            identity_evidence_root=args.identity_evidence_root,
            calendar_root=args.calendar_root,
            daily_reports_root=args.daily_reports_root,
            historical_corpus_root=args.historical_corpus_root,
            promoted_root=args.promoted_root,
            graph_publication_root=args.graph_publication_root,
            engine_run_root=args.engine_run_root,
            research_run_root=args.research_run_root,
            operational_preparation_root=args.operational_preparation_root,
        )
        preparation = prepare_and_publish(
            args.research_run_id, research_stores, preparations
        )
    except Exception as exc:
        print(json.dumps({"status": "FAILED", "error_type": type(exc).__name__}))
        return 2

    manifest = preparation.manifest
    print(
        json.dumps(
            {
                "status": "COMPLETE",
                "preparation_id": manifest.preparation_id,
                "research_run_id": manifest.research_run_id,
                "research_request_id": manifest.research_request_id,
                "graph_manifest_id": manifest.graph_manifest_id,
                "graph_request_id": manifest.graph_request_id,
                "engine_run_id": manifest.engine_run_id,
                "engine_request_id": manifest.engine_request_id,
                "research_intent_batch_id": manifest.research_intent_batch_id,
                "signal_session": manifest.signal_session.isoformat(),
                "target_session": manifest.target_session.isoformat(),
                "cutoff": manifest.cutoff.isoformat(),
                "candidate_ids": list(manifest.candidate_ids),
                "research_intent_ids": list(manifest.research_intent_ids),
                "listing_keys": list(manifest.listing_keys),
                "selected_count": manifest.selected_count,
                "blocked_count": manifest.blocked_count,
                "source_universe_complete": manifest.source_universe_complete,
                "readiness": manifest.readiness.value,
                "paper_only": manifest.paper_only,
                "notification_eligible": manifest.notification_eligible,
                "execution_eligible": manifest.execution_eligible,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
