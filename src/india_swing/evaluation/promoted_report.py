"""Human-readable evidence report for one promoted walk-forward run."""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from india_swing.identity import content_id

from .baselines import DeterministicComparisonRun
from .promoted_walk_forward_store import (
    PromotedWalkForwardRunManifest,
    PromotedWalkForwardStoreError,
)


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True, slots=True)
class PromotedWalkForwardReport:
    trial_id: str
    promoted_run_id: str
    markdown: str
    report_id: str = field(init=False)

    def __post_init__(self) -> None:
        if (
            _SHA256.fullmatch(self.trial_id) is None
            or _SHA256.fullmatch(self.promoted_run_id) is None
            or not isinstance(self.markdown, str)
            or not self.markdown
            or self.markdown != self.markdown.strip() + "\n"
        ):
            raise PromotedWalkForwardStoreError(
                "promoted report content is invalid"
            )
        object.__setattr__(
            self,
            "report_id",
            content_id(
                {
                    "schema": "promoted-walk-forward-report/v1",
                    "trial_id": self.trial_id,
                    "promoted_run_id": self.promoted_run_id,
                    "markdown": self.markdown,
                },
                length=64,
            ),
        )

    def verify_content_identity(self) -> None:
        expected = PromotedWalkForwardReport(
            trial_id=self.trial_id,
            promoted_run_id=self.promoted_run_id,
            markdown=self.markdown,
        )
        if self.report_id != expected.report_id:
            raise PromotedWalkForwardStoreError(
                "promoted report identity failed"
            )


def _metric_rows(run: DeterministicComparisonRun) -> list[str]:
    comparison = run.comparison
    strategy_base = dict(comparison.strategy_base.metrics)
    benchmark_base = dict(comparison.benchmark_base.metrics)
    strategy_stressed = (
        {}
        if comparison.strategy_stressed is None
        else dict(comparison.strategy_stressed.metrics)
    )
    benchmark_stressed = (
        {}
        if comparison.benchmark_stressed is None
        else dict(comparison.benchmark_stressed.metrics)
    )
    names = tuple(strategy_base)
    return [
        "| "
        + " | ".join(
            (
                name,
                str(strategy_base[name]),
                str(benchmark_base[name]),
                str(strategy_stressed.get(name, "n/a")),
                str(benchmark_stressed.get(name, "n/a")),
            )
        )
        + " |"
        for name in names
    ]


def _trade_rows(run: DeterministicComparisonRun) -> list[str]:
    base_trade_by_intent = {
        value.intent_id: value
        for value in run.comparison.strategy_base.trades
    }
    intent_by_signal = {
        value.signal_id: value for value in run.strategy_batch.intents
    }
    rows = []
    for decision in run.strategy_batch.decisions:
        intent = intent_by_signal.get(decision.decision_id)
        trade = (
            None
            if intent is None
            else base_trade_by_intent.get(intent.intent_id)
        )
        if trade is None:
            entry = "not filled" if intent is not None else "n/a"
            exit_value = "n/a"
            pnl = "n/a"
        else:
            entry = (
                f"{trade.entry_fill.session.isoformat()} "
                f"@ {trade.entry_fill.fill_price}"
            )
            reason = trade.exit_fill.exit_reason
            reason_text = "UNKNOWN" if reason is None else reason.value
            exit_value = (
                f"{trade.exit_fill.session.isoformat()} "
                f"@ {trade.exit_fill.fill_price} ({reason_text})"
            )
            pnl = str(trade.gross_pnl)
        rows.append(
            "| "
            + " | ".join(
                (
                    decision.signal_session.isoformat(),
                    decision.symbol,
                    str(
                        decision.score
                        if decision.score is not None
                        else "n/a"
                    ),
                    decision.reason,
                    entry,
                    exit_value,
                    pnl,
                )
            )
            + " |"
        )
    return rows


def build_promoted_walk_forward_report(
    *,
    manifest: PromotedWalkForwardRunManifest,
    run: DeterministicComparisonRun,
) -> PromotedWalkForwardReport:
    if type(manifest) is not PromotedWalkForwardRunManifest:
        raise TypeError("manifest must be exact")
    if type(run) is not DeterministicComparisonRun:
        raise TypeError("run must be exact")
    manifest.verify_content_identity()
    run.verify_content_identity()
    comparison = run.comparison
    if (
        manifest.trial_id != comparison.trial_id
        or manifest.deterministic_run_id != run.run_id
        or manifest.strategy_batch_id != run.strategy_batch.batch_id
        or manifest.comparison_id != comparison.comparison_id
    ):
        raise PromotedWalkForwardStoreError(
            "promoted report inputs have different lineage"
        )
    strategy_charges = comparison.strategy_base.charges
    benchmark_charges = comparison.benchmark_base.charges
    lines = [
        f"# Promoted walk-forward evaluation: {manifest.trial_id}",
        "",
        f"- Promoted run ID: `{manifest.promoted_run_id}`",
        f"- Deterministic run ID: `{manifest.deterministic_run_id}`",
        f"- Comparison ID: `{manifest.comparison_id}`",
        f"- Fold count: {len(manifest.bindings)}",
        f"- Research-batch count: {len(manifest.research_batch_ids)}",
        f"- Comparison gate passed: `{'YES' if comparison.passed else 'NO'}`",
        "",
        "## Portfolio metrics",
        "",
        "| Metric | Strategy base | Benchmark base | Strategy stress | Benchmark stress |",
        "|---|---:|---:|---:|---:|",
        *_metric_rows(run),
        "",
        "## Cost evidence",
        "",
        (
            "- Strategy base charges: "
            + (
                "0"
                if strategy_charges is None
                else str(strategy_charges.total)
            )
        ),
        (
            "- Benchmark base charges: "
            + (
                "0"
                if benchmark_charges is None
                else str(benchmark_charges.total)
            )
        ),
        "",
        "## Fold stability",
        "",
        "| Fold | Sessions | Base excess | Stress excess | Outperformed both |",
        "|---|---|---:|---:|:---:|",
    ]
    for summary in run.fold_summaries:
        metrics = dict(summary.comparison_metrics)
        lines.append(
            "| "
            + " | ".join(
                (
                    f"`{summary.fold_id}`",
                    (
                        f"{summary.first_session.isoformat()} to "
                        f"{summary.last_session.isoformat()}"
                    ),
                    str(metrics["base_primary_excess"]),
                    str(
                        metrics.get(
                            "stressed_primary_excess",
                            "n/a",
                        )
                    ),
                    "YES" if summary.outperformed else "NO",
                )
            )
            + " |"
        )
    lines.extend(
        (
            "",
            "## Strategy decision and trade audit",
            "",
            "| Signal | Symbol | Score | Decision | Entry | Exit | Gross P&L |",
            "|---|---|---:|---|---|---|---:|",
            *_trade_rows(run),
            "",
            "## Interpretation boundary",
            "",
            (
                "Scores are deterministic ranks, not confidence or return "
                "probabilities. This report is offline research evidence, "
                "not a trade alert. Passing one experiment does not authorize "
                "capital; promotion requires repeated point-in-time folds, "
                "cost stress, complete data, and a sealed holdout."
            ),
            "",
        )
    )
    return PromotedWalkForwardReport(
        trial_id=manifest.trial_id,
        promoted_run_id=manifest.promoted_run_id,
        markdown="\n".join(lines),
    )
