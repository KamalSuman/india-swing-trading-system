# Cash-equity costs and conservative daily-bar execution

This layer prices **Zerodha resident-retail NSE cash equity delivery and fully
netted intraday** fills and simulates limit entries plus protective exits from
daily OHLCV bars. It is an evaluation primitive, not permission to alert or
trade. NRI, HUF/corporate, three-in-one, BSE, auction, pledge, call-and-trade,
dealer/automatic-square-off surcharges, and partially netted orders without
allocation evidence are outside this schedule and must fail closed.

## Effective-dated tariff

`zerodha_nse_delivery_schedule_2026()` starts on 2026-03-01 because NSE cash
transaction charges changed on that date. It deliberately cannot price an
earlier fill. An earlier backtest needs a separate schedule with the rates and
effective interval that actually applied then; extending the current schedule
backward would introduce cost look-ahead.

The implemented delivery rates are:

| Component | Buy | Sell | Calculation |
|---|---:|---:|---|
| Zerodha delivery brokerage | 0 | 0 | turnover |
| STT | 0.1% | 0.1% | security/day values aggregated and rounded to rupee |
| NSE transaction charge including IPFT | 0.00307% | 0.00307% | total contract-day turnover, rounded to paise |
| SEBI turnover fee | 0.00010% | 0.00010% | total contract-day turnover, rounded to paise |
| Stamp duty | 0.015% | 0 | buy turnover, rounded to rupee |
| GST | 18% | 18% | brokerage + NSE charge + SEBI fee |
| DP debit | 0 | Rs 13.00 + 18% GST | once per distinct sold scrip per contract day |

The implemented intraday rates are:

| Component | Buy | Sell | Calculation |
|---|---:|---:|---|
| Zerodha brokerage | 0.03%, capped at Rs 20 | 0.03%, capped at Rs 20 | per executed order |
| STT | 0 | 0.025% | security/day sell value, rounded to rupee |
| NSE transaction charge including IPFT | 0.00307% | 0.00307% | total contract-day turnover, rounded to paise |
| SEBI turnover fee | 0.00010% | 0.00010% | total contract-day turnover, rounded to paise |
| Stamp duty | 0.003% | 0 | buy turnover, rounded to rupee |
| GST | 18% | 18% | brokerage + NSE charge + SEBI fee |
| DP debit | 0 | 0 | no demat debit for fully squared-off quantity |

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
- [Zerodha same-day CNC/intraday brokerage classification](https://support.zerodha.com/category/account-opening/resident-individual/ri-charges/articles/brokerage-charged-for-partial-delivery)
- [SEBI regulatory fee schedule](https://www.sebi.gov.in/sebi_data/attachdocs/aug-2021/1628678904669.pdf)
- [India Code section 9A stamp collection](https://www.indiacode.nic.in/show-data?abv=CEN&actid=AC_CEN_2_2_00036_189902_1523339055436&orderno=18&orgactid=AC_CEN_2_2_00036_189902_1523339055436&sectionId=49724&sectionno=9A&statehandle=123456789%2F1362)
- [CDSL Zerodha DP tariff](https://www.cdslindia.com/dp/dpdetails.aspx?dp_id=81600)

Each schedule has a 64-character content identity covering dates, tariff,
rates, sources, and policy version. Each calculation is also content-bound.
Duplicate fills are rejected. `calculate_delivery_charges` remains strict and
rejects same-day buy/sell pairs. `calculate_equity_cash_charges` classifies a
fully offset same-day quantity as intraday from the actual fills, including CNC
square-offs, and applies brokerage per distinct executed order. Unequal
same-day buy/sell quantities are rejected until explicit fill-allocation
evidence is modeled; they are never guessed into intraday and delivery pieces.

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
collection-only NSE artifacts. The evaluation engine selects eligible bars
through its split/data/policy bindings and calculates outcomes itself. Callers
cannot submit hand-authored completion metrics.

## Current boundary

Implemented now: itemized delivery and fully netted intraday costs,
per-executed-order brokerage caps, source/effective-date identity, fill
identity, next-session limit entry, gaps, adverse tick rounding, participation
limits, same-bar ambiguity, and lower-circuit non-fill.

Still required: historical schedules before 2026-03-01, point-in-time tick-size
materialization, explicit allocation for partially netted orders,
partial-fill/order-book models, corporate-action adjusted evaluation views,
and contract-note reconciliation from shadow trading.
