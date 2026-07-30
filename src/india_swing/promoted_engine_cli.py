"""CLI for the restart-safe, single-signal-session, paper-only promoted engine.

Every invocation is explicit: seven durable roots and exact source IDs. There
is no discovery, no latest-selection, and no network/broker/Telegram
capability anywhere in this command. A valid zero-intent/blocked research
batch is a successful, auditable paper result -- not an exception -- so the
success path is the same whether or not any research intent was selected.
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
from india_swing.promoted_engine import (
    PromotedEngineRunner,
    PromotedEngineRunRequest,
    build_promoted_engine_stores,
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
        prog="india-swing-promoted-engine",
        description=(
            "Run one restart-safe, paper-only promoted-engine signal session "
            "from exact already-published promoted evidence."
        ),
    )
    parser.add_argument("--reference-root", required=True, type=Path)
    parser.add_argument("--identity-evidence-root", required=True, type=Path)
    parser.add_argument("--calendar-root", required=True, type=Path)
    parser.add_argument("--daily-reports-root", required=True, type=Path)
    parser.add_argument("--historical-corpus-root", required=True, type=Path)
    parser.add_argument("--promoted-root", required=True, type=Path)
    parser.add_argument("--engine-run-root", required=True, type=Path)
    parser.add_argument("--adjustment-bridge-id", required=True)
    parser.add_argument("--effective-tick-panel-id", required=True)
    parser.add_argument(
        "--reference-promotion-id",
        required=True,
        action="append",
        dest="reference_promotion_ids",
        help="Repeatable; supply one or more exact promotion IDs.",
    )
    parser.add_argument("--corporate-action-snapshot-id", required=True)
    parser.add_argument("--signal-session", required=True, help="YYYY-MM-DD")
    parser.add_argument("--entry-session", required=True, help="YYYY-MM-DD")
    parser.add_argument("--cutoff", required=True, help="Aware ISO-8601 datetime")
    parser.add_argument("--initial-capital", required=True)
    return parser


def _build_request(args: argparse.Namespace) -> PromotedEngineRunRequest:
    return PromotedEngineRunRequest(
        adjustment_bridge_id=args.adjustment_bridge_id,
        effective_tick_panel_id=args.effective_tick_panel_id,
        expected_reference_promotion_ids=tuple(
            sorted(set(args.reference_promotion_ids))
        ),
        expected_corporate_action_snapshot_id=args.corporate_action_snapshot_id,
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
        stores = build_promoted_engine_stores(
            reference_root=args.reference_root,
            identity_evidence_root=args.identity_evidence_root,
            calendar_root=args.calendar_root,
            daily_reports_root=args.daily_reports_root,
            historical_corpus_root=args.historical_corpus_root,
            promoted_root=args.promoted_root,
            engine_run_root=args.engine_run_root,
        )
        manifest = PromotedEngineRunner().run(request, stores)
    except Exception as exc:
        print(json.dumps({"status": "FAILED", "error_type": type(exc).__name__}))
        return 2

    print(
        json.dumps(
            {
                "status": "COMPLETE",
                "run_id": manifest.run_id,
                "request_id": manifest.request_id,
                "adjustment_bridge_id": request.adjustment_bridge_id,
                "effective_tick_panel_id": request.effective_tick_panel_id,
                "expected_reference_promotion_ids": list(
                    manifest.expected_reference_promotion_ids
                ),
                "expected_corporate_action_snapshot_id": (
                    manifest.expected_corporate_action_snapshot_id
                ),
                "feature_input_panel_id": manifest.feature_input_panel_id,
                "technical_panel_id": manifest.technical_panel_id,
                "cross_section_panel_id": manifest.cross_section_panel_id,
                "research_intent_batch_id": manifest.research_intent_batch_id,
                "replay_run_id": manifest.replay_run_id,
                "technical_config_id": manifest.technical_config_id,
                "cross_section_config_id": manifest.cross_section_config_id,
                "intent_config_id": manifest.intent_config_id,
                "signal_session": manifest.signal_session.isoformat(),
                "entry_session": manifest.entry_session.isoformat(),
                "cutoff": manifest.cutoff.isoformat(),
                "candidate_count": manifest.candidate_count,
                "intent_count": manifest.intent_count,
                "paper_only": manifest.paper_only,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
