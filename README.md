# India Swing

This repository is the deterministic core of a long-only Indian equity swing-trade
research and alert system. It is deliberately separated from the upstream
TradingAgents repository.

The current vertical slice implements:

- strict point-in-time evidence validation;
- NSE main-board eligibility without a market-cap cutoff;
- explicit Kronos, signal, and TradingAgents adapter contracts;
- deterministic ranking, sizing, cost, liquidity, and portfolio-risk gates;
- `BUY` or `NO_TRADE` output only;
- typed failed-run output for data/model/research outages, always with `NO_TRADE`;
- create-once local JSON audit records with hash verification;
- explicit trial, model, universe, calendar, data, source, execution, and cost lineage;
- evidence-based post-trade reviews that preserve unresolved causes;
- a synthetic demo and standard-library unit tests.

It does **not** yet use live market data, real Kronos weights, an LLM, Zerodha,
or automatic execution. The demo symbols are fictional and cannot generate a
real trade.

The default risk policy rejects provisional or unvalidated probability estimates.
The fictional demo opts out explicitly so the plumbing can be exercised; its
probabilities and expected return are not performance claims.

## Run locally

Use Python 3.12 or newer from the repository root:

```powershell
$env:PYTHONPATH = "src"
python -m unittest discover -s tests -v
python -m india_swing.demo --output-dir var/audit
```

The demo creates one create-once audit file in `var/audit`. Running the exact same
snapshot again intentionally refuses to overwrite that file. The local record is
hash-verified and published atomically, but a filesystem administrator can still
delete or replace it. Production needs conditional Cloud Storage writes,
retention controls, and access logs.

## Safety boundary

TradingAgents will be an advisory research adapter. It cannot choose the
universe, size a position, override a veto, write an alert, or execute an order.
See `docs/TRADINGAGENTS_ADAPTER.md` and `docs/BIAS_INVARIANTS.md`.

The bias-invariant document is the release target, not a claim that every named
suite already exists. This increment covers the point-in-time cutoff,
next-session entry, a same-session EOD finality contract, main-board eligibility,
deterministic risk gates, explicit lineage, and local audit integrity. A dated
NSE calendar, historical security master and delistings, corporate-action
vintages, complete Indian charge schedule, purged walk-forward evaluation, trial
registry, immutable cloud storage, and live adapters remain required before any
real alert.

## Pilot risk defaults

For the intended Rs 1,00,000 pilot, the deterministic defaults are Rs 250 planned
risk per trade, Rs 500 aggregate open risk, at most two open positions, Rs 20,000
per position, and Rs 40,000 gross exposure. Sizing is also capped by remaining
cash and 0.25% of median daily traded value. New positions halt after Rs 750 of
daily realized loss or Rs 1,500 of cumulative pilot realized loss, preserving a
Rs 500 reserve inside the user's Rs 2,000 maximum-loss envelope for gap and
execution risk. These controls cannot guarantee a market gap will not lose more.
