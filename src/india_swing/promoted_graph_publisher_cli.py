"""CLI for the restart-safe, exact-ID promoted-graph publisher.

Every invocation is explicit: seven durable roots and exact source IDs.
There is no discovery, no latest-selection, and no network/broker/Telegram
capability anywhere in this command. Publication is only an auditable
graph-construction record -- it never runs the promoted engine, prepares a
proposal, sends an alert, or authorizes execution.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

from india_swing.promoted_graph_publisher import (
    PromotedGraphPromotionBinding,
    PromotedGraphPublicationSpec,
    PromotedGraphPublisher,
    PromotedGraphSessionBinding,
    build_promoted_graph_stores,
)


class _CliArgumentError(Exception):
    """Raised by SanitizedArgumentParser instead of printing usage and exiting."""


class SanitizedArgumentParser(argparse.ArgumentParser):
    """An ArgumentParser whose parse failures never print raw argparse text.

    The default ``ArgumentParser.error`` prints a usage message (which can
    include argument values) and calls ``sys.exit(2)`` directly, bypassing
    any surrounding try/except. Raising instead lets ``main`` catch every
    parse failure -- missing required arguments, unknown options, malformed
    binding syntax -- through the same sanitized ``{status: FAILED,
    error_type}`` boundary as every other failure. ``-h``/``--help`` is
    unaffected: it exits via ``SystemExit`` before ever reaching ``error``.
    """

    def error(self, message: str) -> None:
        raise _CliArgumentError(message)


def _parse_promotion_binding(raw: str) -> PromotedGraphPromotionBinding:
    parts = raw.split("@")
    if len(parts) != 2:
        raise ValueError("promotion binding must be SHA256@YYYY-MM-DD")
    promotion_id, date_text = parts
    return PromotedGraphPromotionBinding(
        promotion_id=promotion_id,
        expected_report_date=date.fromisoformat(date_text),
    )


def _parse_session_binding(raw: str) -> PromotedGraphSessionBinding:
    parts = raw.split("@")
    if len(parts) != 2:
        raise ValueError("session binding must be YYYY-MM-DD@SHA256")
    date_text, corpus_id = parts
    return PromotedGraphSessionBinding(
        market_session=date.fromisoformat(date_text),
        historical_corpus_id=corpus_id,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = SanitizedArgumentParser(
        prog="india-swing-promoted-graph-publish",
        description=(
            "Publish one restart-safe, exact-ID promoted graph from already"
            " stored evidence, ending in exact adjustment_bridge_id and"
            " effective_tick_panel_id roots."
        ),
    )
    parser.add_argument("--reference-root", required=True, type=Path)
    parser.add_argument("--identity-evidence-root", required=True, type=Path)
    parser.add_argument("--calendar-root", required=True, type=Path)
    parser.add_argument("--daily-reports-root", required=True, type=Path)
    parser.add_argument("--historical-corpus-root", required=True, type=Path)
    parser.add_argument("--promoted-root", required=True, type=Path)
    parser.add_argument("--publication-root", required=True, type=Path)
    parser.add_argument(
        "--promotion-binding",
        required=True,
        action="append",
        type=_parse_promotion_binding,
        dest="promotion_bindings",
        help="Repeatable; SHA256@YYYY-MM-DD.",
    )
    parser.add_argument(
        "--identity-evidence-id",
        action="append",
        dest="identity_evidence_ids",
        help="Repeatable; optional.",
    )
    parser.add_argument(
        "--identity-review-id",
        action="append",
        dest="identity_review_ids",
        help="Repeatable; optional.",
    )
    parser.add_argument("--calendar-materialization-id", required=True)
    parser.add_argument(
        "--session-binding",
        required=True,
        action="append",
        type=_parse_session_binding,
        dest="session_bindings",
        help="Repeatable; YYYY-MM-DD@SHA256.",
    )
    parser.add_argument("--corporate-action-snapshot-id", required=True)
    parser.add_argument("--cutoff", required=True, help="Aware ISO-8601 datetime")
    return parser


def _build_spec(args: argparse.Namespace) -> PromotedGraphPublicationSpec:
    promotion_bindings = tuple(
        sorted(args.promotion_bindings, key=lambda value: value.expected_report_date)
    )
    session_bindings = tuple(
        sorted(args.session_bindings, key=lambda value: value.market_session)
    )
    return PromotedGraphPublicationSpec(
        promotion_bindings=promotion_bindings,
        identity_evidence_artifact_ids=tuple(
            sorted(set(args.identity_evidence_ids or ()))
        ),
        identity_review_bundle_ids=tuple(
            sorted(set(args.identity_review_ids or ()))
        ),
        calendar_materialization_id=args.calendar_materialization_id,
        session_bindings=session_bindings,
        corporate_action_snapshot_id=args.corporate_action_snapshot_id,
        cutoff=datetime.fromisoformat(args.cutoff),
    )


def main(argv: list[str] | None = None) -> int:
    try:
        parser = build_parser()
        args = parser.parse_args(argv)
        spec = _build_spec(args)
        stores = build_promoted_graph_stores(
            reference_root=args.reference_root,
            identity_evidence_root=args.identity_evidence_root,
            calendar_root=args.calendar_root,
            daily_reports_root=args.daily_reports_root,
            historical_corpus_root=args.historical_corpus_root,
            promoted_root=args.promoted_root,
            publication_root=args.publication_root,
        )
        manifest = PromotedGraphPublisher().publish(spec, stores)
    except Exception as exc:
        print(json.dumps({"status": "FAILED", "error_type": type(exc).__name__}))
        return 2

    print(
        json.dumps(
            {
                "status": "COMPLETE",
                "spec_id": manifest.spec_id,
                "manifest_id": manifest.manifest_id,
                "promotion_bindings": [
                    {
                        "promotion_id": value.promotion_id,
                        "expected_report_date": value.expected_report_date.isoformat(),
                    }
                    for value in manifest.promotion_bindings
                ],
                "identity_evidence_artifact_ids": list(
                    manifest.identity_evidence_artifact_ids
                ),
                "identity_review_bundle_ids": list(
                    manifest.identity_review_bundle_ids
                ),
                "calendar_materialization_id": manifest.calendar_materialization_id,
                "session_bindings": [
                    {
                        "market_session": value.market_session.isoformat(),
                        "historical_corpus_id": value.historical_corpus_id,
                    }
                    for value in manifest.session_bindings
                ],
                "corporate_action_snapshot_id": manifest.corporate_action_snapshot_id,
                "cutoff": manifest.cutoff.isoformat(),
                "intake_id": manifest.intake_id,
                "adjudication_id": manifest.adjudication_id,
                "session_artifacts": [
                    {
                        "market_session": value.market_session.isoformat(),
                        "universe_id": value.universe_id,
                        "frame_id": value.frame_id,
                        "tick_snapshot_id": value.tick_snapshot_id,
                    }
                    for value in manifest.session_artifacts
                ],
                "stable_history_panel_id": manifest.stable_history_panel_id,
                "adjustment_bridge_id": manifest.adjustment_bridge_id,
                "effective_tick_panel_id": manifest.effective_tick_panel_id,
                "adjustment_readiness": manifest.adjustment_readiness.value,
                "adjustment_actionable": manifest.adjustment_actionable,
                "effective_tick_readiness": manifest.effective_tick_readiness.value,
                "effective_tick_actionable": manifest.effective_tick_actionable,
                "paper_only": manifest.paper_only,
                "execution_eligible": manifest.execution_eligible,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
