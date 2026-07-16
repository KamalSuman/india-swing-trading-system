from __future__ import annotations

import re
from dataclasses import dataclass, field

from india_swing.identity import content_id

from .baselines import DeterministicComparisonRun
from .family_aggregation import (
    TrialFamilyAggregationError,
    TrialFamilyEvaluationAggregate,
)


_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


@dataclass(frozen=True, slots=True)
class TrialFamilyEvaluationReport:
    aggregate_id: str
    markdown: str
    report_id: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.aggregate_id, str) or _SHA256.fullmatch(self.aggregate_id) is None:
            raise TrialFamilyAggregationError("report aggregate_id must be a full SHA-256")
        if (
            not isinstance(self.markdown, str)
            or not self.markdown
            or self.markdown != self.markdown.strip() + "\n"
        ):
            raise TrialFamilyAggregationError("report markdown must be canonical newline text")
        object.__setattr__(self, "report_id", self._calculated_id())

    def _calculated_id(self) -> str:
        return content_id(
            {
                "schema": "trial-family-evaluation-report/v1",
                "aggregate_id": self.aggregate_id,
                "markdown": self.markdown,
            },
            length=64,
        )

    def verify_content_identity(self) -> None:
        if self.report_id != self._calculated_id():
            raise TrialFamilyAggregationError("family report content identity failed")


def build_trial_family_evaluation_report(
    *,
    aggregate: TrialFamilyEvaluationAggregate,
    runs: tuple[DeterministicComparisonRun, ...],
) -> TrialFamilyEvaluationReport:
    if type(aggregate) is not TrialFamilyEvaluationAggregate:
        raise TypeError("aggregate must be exact")
    aggregate.verify_content_identity()
    if type(runs) is not tuple or any(type(value) is not DeterministicComparisonRun for value in runs):
        raise TrialFamilyAggregationError("report runs must be an exact tuple")
    run_by_trial = {value.comparison.trial_id: value for value in runs}
    if len(run_by_trial) != len(runs) or set(run_by_trial) != set(aggregate.registered_trial_ids):
        raise TrialFamilyAggregationError("report runs must cover the aggregate exactly")
    for run in runs:
        run.verify_content_identity()
    lines = [
        f"# Trial family evaluation: {aggregate.strategy_family_id}",
        "",
        f"- Aggregate ID: `{aggregate.aggregate_id}`",
        f"- Multiple-testing policy: `{aggregate.policy}`",
        f"- Familywise alpha: `{aggregate.alpha}`",
        f"- Family gate passed: `{'YES' if aggregate.passed else 'NO'}`",
        f"- Eligible trial IDs: {', '.join(f'`{value}`' for value in aggregate.eligible_trial_ids) if aggregate.eligible_trial_ids else 'none'}",
        "",
        "## Family decisions",
        "",
        "| Rank | Trial ID | Folds | Base wins | Stress wins | Raw p | Holm cutoff | Comparison passed | Eligible |",
        "|---:|---|---:|---:|---:|---:|---:|:---:|:---:|",
    ]
    for decision in aggregate.decisions:
        lines.append(
            "| "
            + " | ".join(
                (
                    str(decision.holm_rank),
                    f"`{decision.trial_id}`",
                    str(decision.fold_count),
                    str(decision.base_wins),
                    str(decision.stressed_wins),
                    str(decision.raw_p_value),
                    str(decision.holm_threshold),
                    "YES" if decision.comparison_passed else "NO",
                    "YES" if decision.eligible else "NO",
                )
            )
            + " |"
        )
    lines.extend(("", "## Fold evidence"))
    for decision in aggregate.decisions:
        run = run_by_trial[decision.trial_id]
        lines.extend(
            (
                "",
                f"### Trial `{decision.trial_id}`",
                "",
                f"Comparison ID: `{run.comparison.comparison_id}`  ",
                f"Run ID: `{run.run_id}`",
                "",
                "| Fold | Sessions | Base excess | Stress excess | Outperformed both |",
                "|---|---|---:|---:|:---:|",
            )
        )
        for summary in run.fold_summaries:
            metrics = dict(summary.comparison_metrics)
            lines.append(
                "| "
                + " | ".join(
                    (
                        f"`{summary.fold_id}`",
                        f"{summary.first_session.isoformat()} to {summary.last_session.isoformat()}",
                        str(metrics["base_primary_excess"]),
                        str(metrics.get("stressed_primary_excess", "n/a")),
                        "YES" if summary.outperformed else "NO",
                    )
                )
                + " |"
            )
    lines.extend(
        (
            "",
            "## Interpretation boundary",
            "",
            "This report is deterministic research evidence, not a profit forecast or trade alert. "
            "Holm controls the preregistered family under the specified fold-sign procedure, but "
            "non-overlapping folds may remain regime-correlated. Promotion does not make collection-only "
            "data actionable and does not bypass portfolio, liquidity, news, or live-data gates.",
            "",
        )
    )
    return TrialFamilyEvaluationReport(
        aggregate_id=aggregate.aggregate_id,
        markdown="\n".join(lines),
    )
