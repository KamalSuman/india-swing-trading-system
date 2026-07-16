# Delivery costs and conservative daily-bar execution

This layer prices **Zerodha resident-retail NSE cash equity delivery** fills and
simulates limit entries plus protective exits from daily OHLCV bars. It is an
evaluation primitive, not permission to alert or trade. NRI, HUF/corporate,
three-in-one, BSE, intraday, auction, pledge, call-and-trade, and automatic
square-off tariffs are outside this schedule and must fail closed.

## Effective-dated tariff

`zerodha_nse_delivery_schedule_2026()` starts on 2026-03-01 because NSE cash
transaction charges changed on that date. It deliberately cannot price an
earlier fill. An earlier backtest needs a separate schedule with the rates and
effective interval that actually applied then; extending the current schedule
backward would introduce cost look-ahead.

The implemented rates are:

| Component | Buy | Sell | Calculation |
|---|---:|---:|---|
| Zerodha delivery brokerage | 0 | 0 | turnover |
| STT | 0.1% | 0.1% | security/day values aggregated and rounded to rupee |
| NSE transaction charge including IPFT | 0.00307% | 0.00307% | total contract-day turnover, rounded to paise |
| SEBI turnover fee | 0.00010% | 0.00010% | total contract-day turnover, rounded to paise |
| Stamp duty | 0.015% | 0 | buy turnover, rounded to rupee |
| GST | 18% | 18% | brokerage + NSE charge + SEBI fee |
| DP debit | 0 | Rs 13.00 + 18% GST | once per distinct sold scrip per contract day |

NSE's 2026 circular states Rs 306.99/crore transaction charge plus Rs
0.01/crore IPFT, so the combined 0.00307% rate is one field. IPFT must not be
added again. The standard DP debit is Rs 15.34 gross. CDSL lists a Rs 0.25
discount for a female first holder; this is available only through an explicit
tariff enum and is never inferred from a user or account name.

Primary/current references:

- [Zerodha charges](https://zerodha.com/charges)
- [NSE circular FA73061, effective 1 March 2026](https://nsearchives.nseindia.com/content/circulars/FA73061.pdf)
- [Income Tax Department section 98 STT rates](https://www.incometaxindia.gov.in/w/section-98-55)
- [Zerodha STT aggregation and rounding](https://support.zerodha.com/category/account-opening/resident-individual/ri-charges/articles/how-is-the-securities-transaction-tax-stt-calculated)
- [SEBI regulatory fee schedule](https://www.sebi.gov.in/sebi_data/attachdocs/aug-2021/1628678904669.pdf)
- [India Code section 9A stamp collection](https://www.indiacode.nic.in/show-data?abv=CEN&actid=AC_CEN_2_2_00036_189902_1523339055436&orderno=18&orgactid=AC_CEN_2_2_00036_189902_1523339055436&sectionId=49724&sectionno=9A&statehandle=123456789%2F1362)
- [CDSL Zerodha DP tariff](https://www.cdslindia.com/dp/dpdetails.aspx?dp_id=81600)

Each schedule has a 64-character content identity covering dates, tariff,
rates, sources, and policy version. Each calculation is also content-bound.
Duplicate fills are rejected. A same-day buy and sell of one scrip is rejected
because treating a netted/intraday position as delivery could apply the wrong
STT and DP rules.

Actual Zerodha contract notes remain the reconciliation authority for live
shadow trades. If component rounding differs, add a new versioned policy and
retain the old result; never rewrite historical calculations.

## Daily-bar fill policy

The simulator intentionally chooses pessimistic rules where daily OHLC cannot
prove the intraday path:

1. A signal-session bar can never fill its own entry. Entry begins on the
   declared next eligible session and ends at the declared expiry.
2. A buy limit gaps below the limit from the open; otherwise it fills only if
   the low touches the limit. Adverse slippage is capped at the limit price.
3. Slippage rounds against the strategy using the order's explicit
   point-in-time tick size: buys round upward and sells downward.
4. A full fill is allowed only within the declared maximum share-of-volume.
   No partial fill is invented by this first version.
5. If stop and target are both touched on a later daily bar, the stop wins.
6. A later gap through the stop uses the opening price before adverse
   slippage. A gap above a sell target uses the opening price but cannot fill
   below the target limit.
7. On the entry bar, a touched stop is assumed to happen after entry, while a
   target-only touch is not booked because its ordering cannot be proven.
8. A lower-circuit sell lock produces no assumed exit fill. The position stays
   open for the outcome engine to value on later evidence.

The simulator consumes a typed `SimulationBar`; it does not authorize raw or
collection-only NSE artifacts. A future evaluation engine must select bars only
through the cutoff, calendar, identity, corporate-action, and readiness gates,
then calculate trial outcomes itself. Callers must not submit hand-authored
win/loss metrics.

## Current boundary

Implemented now: itemized costs, source/effective-date identity, fill identity,
next-session limit entry, gaps, adverse tick rounding, participation limits,
same-bar ambiguity, and lower-circuit non-fill.

Still required: historical schedules before 2026-03-01, point-in-time tick-size
materialization, partial-fill/order-book models, corporate-action adjusted
evaluation views, engine-generated trial metrics, and contract-note
reconciliation from shadow trading.
