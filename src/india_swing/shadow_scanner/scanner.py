from __future__ import annotations

from collections import Counter
from decimal import Decimal

from india_swing.daily_pipeline.derived_evidence import DailyDerivedEvidence
from india_swing.historical_prices.models import NseEodSessionArtifact, RawNseEodBar
from india_swing.liquidity.models import (
    CollectedLiquidityObservation,
    CollectionLiquiditySnapshot,
)
from india_swing.reference.models import ReferenceReadiness
from india_swing.tick_sizes.models import (
    CollectedTickSizeObservation,
    CollectionTickSizeSnapshot,
)
from india_swing.universe.models import (
    CollectedUniverseObservation,
    CollectionUniverseSnapshot,
)

from .models import (
    CollectionShadowCandidate,
    CollectionShadowScanResult,
    CollectionShadowScannerConfig,
    ShadowScanError,
    ShadowScanStatus,
)


_FIXED_BLOCKERS = (
    "COLLECTION_ONLY_NON_ACTIONABLE",
    "CORPORATE_ACTIONS_UNAPPLIED",
    "RAW_UNADJUSTED_PRICES",
    "STABLE_IDENTITY_UNVERIFIED",
)
_FIXED_CANDIDATE_WARNINGS = (
    "COLLECTION_ONLY_NON_ACTIONABLE",
    "CORPORATE_ACTIONS_UNAPPLIED",
    "RESEARCH_ONLY_DO_NOT_EXECUTE",
    "STABLE_IDENTITY_UNVERIFIED",
)


def _unique_by_identity(values: tuple[object, ...], key, name: str) -> dict[object, object]:
    result: dict[object, object] = {}
    for value in values:
        identity = key(value)
        if identity in result:
            raise ShadowScanError(f"{name} contains duplicate identities")
        result[identity] = value
    return result


def _verify_inputs(
    derived: DailyDerivedEvidence,
    history: tuple[NseEodSessionArtifact, ...],
    universe: CollectionUniverseSnapshot,
    liquidity: CollectionLiquiditySnapshot,
    ticks: CollectionTickSizeSnapshot,
    config: CollectionShadowScannerConfig,
) -> None:
    if type(derived) is not DailyDerivedEvidence:
        raise ShadowScanError("derived evidence must be exact")
    if type(history) is not tuple or any(
        type(value) is not NseEodSessionArtifact for value in history
    ):
        raise ShadowScanError("history must be an exact artifact tuple")
    if type(universe) is not CollectionUniverseSnapshot:
        raise ShadowScanError("universe snapshot must be exact")
    if type(liquidity) is not CollectionLiquiditySnapshot:
        raise ShadowScanError("liquidity snapshot must be exact")
    if type(ticks) is not CollectionTickSizeSnapshot:
        raise ShadowScanError("tick-size snapshot must be exact")
    if type(config) is not CollectionShadowScannerConfig:
        raise ShadowScanError("scanner configuration must be exact")
    try:
        derived.verify_content_identity()
        universe.verify_content_identity()
        liquidity.verify_content_identity()
        ticks.verify_content_identity()
        config.verify_content_identity()
        for value in history:
            value.verify_content_identity()
    except Exception:
        raise ShadowScanError("scanner input identity verification failed") from None
    if not history:
        raise ShadowScanError("scanner history cannot be empty")
    if history != tuple(sorted(history, key=lambda value: value.market_session)):
        raise ShadowScanError("scanner history must be market-session ordered")
    if len({value.market_session for value in history}) != len(history):
        raise ShadowScanError("scanner history sessions must be unique")
    if tuple(value.artifact_id for value in history) != (
        derived.historical_price_artifact_ids
    ):
        raise ShadowScanError("scanner history differs from derived evidence")
    if (
        derived.universe_snapshot_id != universe.snapshot_id
        or derived.liquidity_snapshot_id != liquidity.snapshot_id
        or derived.tick_size_snapshot_id != ticks.snapshot_id
    ):
        raise ShadowScanError("scanner snapshots differ from derived evidence")
    if (
        derived.market_session != history[-1].market_session
        or derived.market_session != universe.market_session_claim
        or derived.market_session != ticks.market_session_claim
    ):
        raise ShadowScanError("scanner inputs disagree on the target session")
    if (
        derived.cutoff != universe.cutoff
        or derived.cutoff != liquidity.decision_cutoff
        or derived.cutoff != ticks.cutoff
    ):
        raise ShadowScanError("scanner inputs disagree on the decision cutoff")
    if (
        derived.minimum_history_sessions != config.minimum_history_sessions
        or liquidity.minimum_history_sessions != config.minimum_history_sessions
    ):
        raise ShadowScanError("scanner history policy differs from derived evidence")
    if (
        derived.readiness is not ReferenceReadiness.COLLECTION_ONLY
        or derived.actionable
        or universe.readiness is not ReferenceReadiness.COLLECTION_ONLY
        or universe.actionable
        or liquidity.readiness is not ReferenceReadiness.COLLECTION_ONLY
        or liquidity.actionable
        or ticks.readiness is not ReferenceReadiness.COLLECTION_ONLY
        or ticks.actionable
        or any(
            value.readiness is not ReferenceReadiness.COLLECTION_ONLY
            or value.actionable
            for value in history
        )
    ):
        raise ShadowScanError("scanner accepts collection-only non-actionable evidence")
    expected_sources = tuple(
        (value.market_session, value.artifact_id) for value in history
    )
    actual_sources = tuple(
        (value.market_session, value.artifact_id)
        for value in liquidity.source_sessions
    )
    if actual_sources != expected_sources:
        raise ShadowScanError("liquidity sources differ from scanner history")


def _result(
    *,
    derived: DailyDerivedEvidence,
    config: CollectionShadowScannerConfig,
    candidates: tuple[CollectionShadowCandidate, ...],
    exclusions: Counter[str],
    insufficient_history: bool,
) -> CollectionShadowScanResult:
    blockers = set(_FIXED_BLOCKERS)
    if insufficient_history:
        blockers.add("INSUFFICIENT_HISTORY")
    return CollectionShadowScanResult(
        market_session=derived.market_session,
        cutoff=derived.cutoff,
        derived_evidence_id=derived.evidence_id,
        universe_snapshot_id=derived.universe_snapshot_id,
        liquidity_snapshot_id=derived.liquidity_snapshot_id,
        tick_size_snapshot_id=derived.tick_size_snapshot_id,
        historical_price_artifact_ids=derived.historical_price_artifact_ids,
        config_id=config.config_id,
        candidates=candidates,
        exclusion_counts=tuple(sorted(exclusions.items())),
        blockers=tuple(sorted(blockers)),
        status=(
            ShadowScanStatus.RANKED
            if candidates
            else ShadowScanStatus.NO_CANDIDATE
        ),
    )


def scan_collection_artifacts(
    *,
    derived: DailyDerivedEvidence,
    history: tuple[NseEodSessionArtifact, ...],
    universe: CollectionUniverseSnapshot,
    liquidity: CollectionLiquiditySnapshot,
    ticks: CollectionTickSizeSnapshot,
    config: CollectionShadowScannerConfig | None = None,
) -> CollectionShadowScanResult:
    """Rank observations without granting trade or notification authority."""

    if config is None:
        config = CollectionShadowScannerConfig()
    _verify_inputs(derived, history, universe, liquidity, ticks, config)
    exclusions: Counter[str] = Counter()
    in_scope_count = sum(
        value.included_in_broad_equity_scope for value in universe.observations
    )
    if len(history) < config.minimum_history_sessions:
        if in_scope_count:
            exclusions["INSUFFICIENT_GLOBAL_HISTORY"] = in_scope_count
        return _result(
            derived=derived,
            config=config,
            candidates=(),
            exclusions=exclusions,
            insufficient_history=True,
        )

    current = history[-1]
    lookback_artifacts = history[-config.momentum_lookback_sessions :]
    current_bars = _unique_by_identity(
        current.bars,
        lambda value: value.listing_key,
        "current bars",
    )
    history_bars = tuple(
        _unique_by_identity(
            artifact.bars,
            lambda value: (value.validated_isin, value.series),
            "historical bars",
        )
        for artifact in lookback_artifacts
    )
    liquidity_by_identity = _unique_by_identity(
        liquidity.observations,
        lambda value: (value.validated_isin, value.series),
        "liquidity observations",
    )
    ticks_by_listing = _unique_by_identity(
        ticks.observations,
        lambda value: (value.symbol, value.series),
        "tick-size observations",
    )
    candidates: list[CollectionShadowCandidate] = []

    for observation in universe.observations:
        if type(observation) is not CollectedUniverseObservation:
            raise ShadowScanError("universe observation graph is invalid")
        if not observation.included_in_broad_equity_scope:
            exclusions["OUT_OF_BROAD_EQUITY_SCOPE"] += 1
            continue
        if observation.series not in config.allowed_series:
            exclusions["SERIES_NOT_ALLOWED"] += 1
            continue
        if (
            not observation.normal_market_eligible
            or observation.delete_flag != "N"
        ):
            exclusions["SOURCE_MARKET_FLAGS_BLOCKED"] += 1
            continue
        if observation.validated_isin is None:
            exclusions["VALIDATED_ISIN_MISSING"] += 1
            continue
        current_bar = current_bars.get((observation.symbol, observation.series))
        if current_bar is None:
            exclusions["CURRENT_BAR_MISSING"] += 1
            continue
        if (
            current_bar.validated_isin != observation.validated_isin
            or current_bar.financial_instrument_id
            != observation.financial_instrument_id
        ):
            exclusions["CURRENT_IDENTITY_MISMATCH"] += 1
            continue
        identity_key = (observation.validated_isin, observation.series)
        liquidity_value = liquidity_by_identity.get(identity_key)
        if liquidity_value is None:
            exclusions["LIQUIDITY_MISSING"] += 1
            continue
        if not liquidity_value.meets_minimum_history:
            exclusions["INSUFFICIENT_CANDIDATE_HISTORY"] += 1
            continue
        if (
            liquidity_value.median_daily_traded_value
            < config.minimum_median_traded_value
        ):
            exclusions["LIQUIDITY_BELOW_MINIMUM"] += 1
            continue
        if liquidity_value.median_delivery_percent is None:
            exclusions["DELIVERY_EVIDENCE_MISSING"] += 1
            continue
        if (
            liquidity_value.median_delivery_percent
            < config.minimum_delivery_percent
        ):
            exclusions["DELIVERY_BELOW_MINIMUM"] += 1
            continue
        tick = ticks_by_listing.get((observation.symbol, observation.series))
        if tick is None:
            exclusions["TICK_SIZE_MISSING"] += 1
            continue
        if (
            tick.financial_instrument_id != observation.financial_instrument_id
            or tick.validated_isin != observation.validated_isin
        ):
            exclusions["TICK_IDENTITY_MISMATCH"] += 1
            continue
        bars: list[RawNseEodBar] = []
        for artifact_bars in history_bars:
            bar = artifact_bars.get(identity_key)
            if bar is None:
                break
            bars.append(bar)
        if len(bars) != config.momentum_lookback_sessions:
            exclusions["LOOKBACK_BAR_MISSING"] += 1
            continue
        start_price = bars[0].previous_close
        lookback_return_pct = (
            (bars[-1].close / start_price) - Decimal("1")
        ) * Decimal("100")
        positive_fraction = Decimal(
            sum(value.close > value.previous_close for value in bars)
        ) / Decimal(len(bars))
        candidates.append(
            CollectionShadowCandidate(
                market_session=derived.market_session,
                symbol=observation.symbol,
                series=observation.series,
                validated_isin=observation.validated_isin,
                financial_instrument_id=observation.financial_instrument_id,
                current_close=current_bar.close,
                tick_size_rupees=tick.tick_size_rupees,
                lookback_sessions=tuple(
                    value.market_session for value in lookback_artifacts
                ),
                bar_ids=tuple(value.bar_id for value in bars),
                lookback_return_pct=lookback_return_pct,
                positive_session_fraction=positive_fraction,
                median_daily_traded_value=liquidity_value.median_daily_traded_value,
                median_daily_volume=liquidity_value.median_daily_volume,
                median_delivery_percent=liquidity_value.median_delivery_percent,
                evidence_ids=(
                    derived.evidence_id,
                    universe.snapshot_id,
                    liquidity.snapshot_id,
                    ticks.snapshot_id,
                    *tuple(value.bar_id for value in bars),
                ),
                warnings=_FIXED_CANDIDATE_WARNINGS,
            )
        )

    ranked = tuple(
        sorted(
            candidates,
            key=lambda value: (
                -value.lookback_return_pct,
                -value.median_daily_traded_value,
                value.symbol,
                value.series,
            ),
        )[: config.top_n]
    )
    return _result(
        derived=derived,
        config=config,
        candidates=ranked,
        exclusions=exclusions,
        insufficient_history=False,
    )
