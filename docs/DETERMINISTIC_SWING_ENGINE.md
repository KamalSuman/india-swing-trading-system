# Deterministic swing signal engine

`DeterministicSwingSignalProvider` is the first real signal-generation
implementation behind the existing `Pipeline`. It replaces the demo's static
signal values; it does not replace the point-in-time universe, Kronos forecast,
research review, risk engine, or notification boundary.

## Bound inputs

One provider instance is bound to one immutable decision snapshot, one exact
calendar snapshot, and an ordered tuple of instrument histories. Every history
must:

- end on the signal session and contain at least 60 declared trading sessions;
- bind every adjusted EOD bar to a snapshot evidence ID, content hash, and
  availability timestamp;
- bind its effective tick size to separate cutoff-safe evidence;
- bind a separate corporate-action adjustment artifact and declare
  `CORPORATE_ACTION_ADJUSTED_AS_OF_CUTOFF`;
- contain no session or evidence known after the decision cutoff.

Raw unadjusted history is rejected. The current collection archive therefore
cannot be passed directly to this provider.

## Input assembly and promotion replay

`assemble_swing_inputs` is the audited bridge from stored raw NSE session
artifacts to the adjustment and signal-history contracts. For every requested
session it requires one exact universe snapshot, resolves one stable instrument
and listing, matches the raw bar by symbol, series, and validated ISIN, and
creates a content-addressed identity binding. The adjusted bar's availability
time is the latest of the raw report, identity snapshot, and corporate-action
evidence; later identity knowledge can therefore never be backdated to the raw
report timestamp.

The supplied promotion decision is not trusted as a status label. The assembler
re-runs the promotion policy from its nested evidence and requires exact source
IDs for raw prices, universe, stable identity, corporate actions, and tick size.
Synthetic inputs can produce a non-actionable research assembly. Collection-only
inputs are rejected. `assemble_alert_swing_inputs` additionally rejects every
assembly that is not point-in-time verified and alert actionable.

The repository still deliberately prevents construction of a
`POINT_IN_TIME_VERIFIED` universe until an official-source importer or equivalent
audited promotion path exists. Consequently, this bridge can exercise the real
engine with synthetic research fixtures today, but cannot turn the manually
downloaded collection archive into a live notification merely by changing a
readiness flag.

`assemble_universe_input_batch` closes the next coverage boundary above the
per-listing assembler. It requires exactly one verified assembly for every
currently actionable universe entry and derives an explicit veto for every
watch-only, excluded, or unverified entry. Missing or extra actionable subjects
fail the entire batch, so a scan cannot silently shrink its universe or omit
smaller companies. Synthetic batches remain research-only.

## Features and levels

The version-1 policy calculates only deterministic, explainable quantities:

- 20-session close momentum, normalized into a strength score;
- 50-session trend quality from positive-session frequency, moving-average
  position, and proximity to the prior 20-session high;
- current volume relative to the preceding 20-session median;
- recent median traded value as a liquidity-quality input;
- a 14-session average true range (ATR).

The entry range starts at the signal close and extends by 0.10 ATR. The stop is
1.50 ATR below the entry zone. The target is rounded against the strategy and
is high enough to preserve at least 2.5 net reward/risk after the greater of the
configured cost assumption and observed spread. All levels use the
effective-dated tick size supplied by the bound history.

The calendar determines the next eligible session and the exact executable
window. The default entry begins five minutes after its live-continuous open and
expires fifteen minutes before close. A signal-session bar can never define a
same-session entry.

## Confidence boundary

Without a calibration artifact, the provider emits `PROVISIONAL` probability
status, zero target/stop probabilities, and a zero calibration sample. It does
not translate a technical score or a model narrative into an “80% confidence”
claim. The default risk policy consequently rejects the setup.

A provider may emit `VALIDATED` only when it is bound to a content-addressed
`WalkForwardCalibration` that is also present as exact evidence in the decision
snapshot. The artifact is accepted only when all of these conditions hold:

- the calibration plan was registered before any selected outcome was known;
- the plan names the exact signal configuration and complete, fixed set of
  evaluation trials;
- every observation comes from the untouched `TEST` partition of one exact,
  completed comparison result per trial;
- signal, trade, result, completion-event, split-plan, and trial identities are
  preserved and verified instead of reconstructed from labels;
- no trade or signal is counted twice, no preregistered trial is omitted, and
  every outcome was known by the calibration cutoff;
- at least 100 resolved trades are present; and
- the registered version-1 method adds ten adverse stop outcomes before
  calculating the target and stop probabilities.

The adverse prior deliberately lowers the reported target rate and raises the
reported stop rate. Time exits retain their own empirical average net R. These
numbers describe one frozen, out-of-sample evidence set; they are not a promise
of future profit and are not equivalent to subjective model confidence. In
particular, an `80%` trading threshold must never be satisfied by a technical
score, an LLM opinion, or in-sample performance.

Current imported NSE files do not by themselves satisfy this boundary. A real
validated artifact requires completed point-in-time walk-forward trials with
charges, fills, exits, and lifecycle completion evidence.

## Focused verification

```powershell
python -m unittest tests.test_swing_input_assembly tests.test_signal_calibration tests.test_deterministic_swing_signals -v
```

The tests cover deterministic replay, future-known bar and tick rejection,
corporate-action adjustment provenance, snapshot content binding, minimum
history, exact next-session timing, tick alignment, and cost-adjusted 2.5R
target construction. They also cover preregistration, untouched-test-only
selection, exact result/completion lineage, duplicate exclusion, cutoff safety,
minimum sample size, adverse priors, mutation detection, and snapshot binding of
the final calibration artifact. Input-assembly tests additionally cover exact
promotion-source binding, promotion replay, late identity knowledge, actionable
signal-session eligibility, future-known evidence, mutation detection, and the
synthetic-to-alert prohibition.
