# Regime-aware alpha engine

The first alpha-brain increment is a deterministic, content-addressed forecast
challenger. It is designed to answer two separate questions without confusing
them:

1. Which instruments have the strongest cross-sectional swing characteristics
   at this exact decision cutoff?
2. Has that ranking method earned the right to influence a real alert?

The implementation answers the first question. The existing purged walk-forward
evaluation, calibration, promotion, and risk gates answer the second. A high
ensemble score is therefore not an 80% win probability and is not permission to
trade.

## Inputs and point-in-time boundary

`RegimeAwareForecastProvider` consumes one exact `DataSnapshot`, one exact
calendar snapshot, and a complete instrument-ordered tuple of adjusted swing
histories. All histories must:

- end on the same signal session and have identical session coverage;
- use corporate-action-adjusted prices as known at the cutoff;
- bind every bar, tick-size observation, and adjustment observation to the
  exact content hash and availability time in the decision snapshot;
- contain no future session or future-known evidence; and
- retain valid content identities after construction.

The provider cannot promote collection-only evidence. The pipeline's existing
reference-readiness and risk gates remain authoritative.

## Market regimes

The regime is computed once from the eligible cross-section, never separately
per security:

- `TRENDING`: broad positive trend participation and positive median momentum;
- `RANGE_BOUND`: neither a broad trend nor a defensive regime;
- `HIGH_VOLATILITY`: median realized volatility exceeds the configured limit;
- `RISK_OFF`: weak breadth and non-positive median momentum.

High volatility takes precedence so an unstable tape cannot be mislabeled as a
healthy trend merely because a volatile rebound lifted breadth.

## Specialists

Four independently visible specialist scores are retained in every assessment:

- `MOMENTUM_BREAKOUT`: short/long cross-sectional momentum, proximity to the
  prior high, and current volume confirmation;
- `PULLBACK_CONTINUATION`: positive-trend gating plus the quality of a controlled
  retracement and volume confirmation;
- `VOLATILITY_CONTRACTION`: recent range contraction, breakout proximity,
  cross-sectional low-volatility quality, and volume confirmation;
- `LIQUIDITY_QUALITY`: cross-sectional median traded-value rank.

Regime weights are immutable, sum exactly to one, and are part of the config's
content identity. Equal metrics receive equal ranks: instrument ID and symbol
are never used to manufacture an ordering among ties.

Liquidity is a defensive selection characteristic, not evidence of a price rise.
The score-implied return also includes the instrument's own signed momentum and
is attenuated in high-volatility and risk-off regimes. Downside is based on the
instrument's cutoff-bound ATR fraction and declared horizon.

## Outputs

Each assessment preserves:

- config, data-snapshot, instrument, listing, and history lineage;
- the regime and its breadth, median momentum, and median volatility evidence;
- the full input metric vector;
- every raw specialist score, regime weight, weighted score, and rationale;
- the exact ensemble sum; and
- score-implied median return, downside, and uncertainty.

The forecast provider conforms to the existing `ForecastProvider` protocol, so
it can be injected into the deterministic pipeline. Its `sample_count` is the
number of ensemble specialists, not a historical calibration sample.

## Wealth-protection status

This module is a challenger, not a performance claim. Before it can contribute
to a real alert, the remaining sequence is:

1. supply authenticated/licensed point-in-time calendar, universe, identity,
   adjustment, tick-size, and EOD inputs;
2. assemble leak-free daily cross-sections over multiple market regimes;
3. preregister the config and compare it with the existing deterministic
   close-momentum baseline and liquid equal-weight benchmark;
4. run purged expanding walk-forward folds with full cost and stress models;
5. calibrate target/stop outcomes only from untouched out-of-sample predictions;
6. require promotion thresholds, multiple-testing control, and paper-shadow
   evidence; and
7. preserve the current `NO_TRADE` default whenever any lineage, calibration,
   risk, liquidity, or operational gate fails.

Kronos, news models, and LLM research agents may later enter as separately
measured challengers. They must not select the universe, overwrite market facts,
convert scores into confidence percentages, bypass risk policy, or execute an
order.
