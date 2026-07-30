"""CLI for the restart-safe, exact-manifest promoted-graph-to-engine bridge.

Every invocation is explicit: nine durable roots, one exact
``--graph-manifest-id``, and the request's own sessions/cutoff/capital.
There is no discovery, no latest-selection, and no network/broker/Telegram
capability anywhere in this command. Every engine root pin (adjustment
bridge, effective-tick panel, reference-promotion set, corporate-action
snapshot) is derived from the resolved graph manifest, never accepted as a
separate flag. A collection-only graph is not rejected: the resolved
graph's exact readiness/actionable projections are preserved unchanged in
the success JSON, and running the research computation never itself grants
notification or execution authority.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from india_swing.evaluation.promoted_intents import PromotedIntentPolicyConfig
from india_swing.features.promoted_cross_section import PromotedCrossSectionConfig
from india_swing.features.promoted_technical import PromotedTechnicalFeatureConfig
from india_swing.promoted_research_run import (
    PromotedResearchOrchestrator,
    PromotedResearchRunRequest,
    build_promoted_research_stores,
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
        prog="india-swing-promoted-research-run",
        description=(
            "Run one restart-safe, paper-only combined promoted-research "
            "pass by deriving every engine root pin from one exact, "
            "already-published promoted graph."
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
    parser.add_argument("--graph-manifest-id", required=True)
    parser.add_argument("--signal-session", required=True, help="YYYY-MM-DD")
    parser.add_argument("--entry-session", required=True, help="YYYY-MM-DD")
    parser.add_argument("--cutoff", required=True, help="Aware ISO-8601 datetime")
    parser.add_argument("--initial-capital", required=True)
    return parser


def _build_request(args: argparse.Namespace) -> PromotedResearchRunRequest:
    return PromotedResearchRunRequest(
        graph_manifest_id=args.graph_manifest_id,
        signal_session=date.fromisoformat(args.signal_session),
        entry_session=date.fromisoformat(args.entry_session),
        cutoff=datetime.fromisoformat(args.cutoff),
        initial_capital=Decimal(args.initial_capital),
        technical_config=PromotedTechnicalFeatureConfig(),
        cross_section_config=PromotedCrossSectionConfig(),
        intent_config=PromotedIntentPolicyConfig(),
    )


def main(argv: list[str] | None = None) -> int:
    try:
        parser = build_parser()
        args = parser.parse_args(argv)
        request = _build_request(args)
        stores = build_promoted_research_stores(
            reference_root=args.reference_root,
            identity_evidence_root=args.identity_evidence_root,
            calendar_root=args.calendar_root,
            daily_reports_root=args.daily_reports_root,
            historical_corpus_root=args.historical_corpus_root,
            promoted_root=args.promoted_root,
            graph_publication_root=args.graph_publication_root,
            engine_run_root=args.engine_run_root,
            research_run_root=args.research_run_root,
        )
        manifest = PromotedResearchOrchestrator().run(request, stores)
    except Exception as exc:
        print(json.dumps({"status": "FAILED", "error_type": type(exc).__name__}))
        return 2

    print(
        json.dumps(
            {
                "status": "COMPLETE",
                "research_run_id": manifest.research_run_id,
                "research_request_id": manifest.research_request_id,
                "graph_manifest_id": manifest.graph_manifest_id,
                "graph_spec_id": manifest.graph_spec_id,
                "adjustment_bridge_id": manifest.adjustment_bridge_id,
                "effective_tick_panel_id": manifest.effective_tick_panel_id,
                "expected_reference_promotion_ids": list(
                    manifest.expected_reference_promotion_ids
                ),
                "expected_corporate_action_snapshot_id": (
                    manifest.expected_corporate_action_snapshot_id
                ),
                "engine_request_id": manifest.engine_request_id,
                "engine_run_id": manifest.engine_run_id,
                "feature_input_panel_id": manifest.feature_input_panel_id,
                "technical_config_id": manifest.technical_config_id,
                "technical_panel_id": manifest.technical_panel_id,
                "cross_section_config_id": manifest.cross_section_config_id,
                "cross_section_panel_id": manifest.cross_section_panel_id,
                "intent_config_id": manifest.intent_config_id,
                "research_intent_batch_id": manifest.research_intent_batch_id,
                "replay_run_id": manifest.replay_run_id,
                "signal_session": manifest.signal_session.isoformat(),
                "entry_session": manifest.entry_session.isoformat(),
                "cutoff": manifest.cutoff.isoformat(),
                "initial_capital": str(manifest.initial_capital),
                "candidate_count": manifest.candidate_count,
                "intent_count": manifest.intent_count,
                "adjustment_readiness": manifest.adjustment_readiness.value,
                "adjustment_actionable": manifest.adjustment_actionable,
                "effective_tick_readiness": manifest.effective_tick_readiness.value,
                "effective_tick_actionable": manifest.effective_tick_actionable,
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
