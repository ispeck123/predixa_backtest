# Trend Engine Audit Report

## Executive summary

This audit did not modify production trend logic.  It traced the cash API to
the active engine, ran six deterministic implementation-conformance tests, and
walked `icici_lombard_gic` forward at 37 historical cutoffs (2020-10-19 through
2026-08-06) using its configured `weekly / daily / sixty` E/A/X mapping.

The code is internally consistent in the exercised paths: the deterministic
tests passed and the replay calls the production zone generator and trend
calculator as of each timestamp.  However, the independent confirmed-pivot
reference often disagreed directionally: severe UP-vs-DN disagreement was 6/37
(16.2%) at E, 11/37 (29.7%) at A, and 6/37 (16.2%) at X.  This is evidence of a
methodology/price-action disagreement, not by itself an implementation defect.

## Engine definition of trend and decision flow

```
CSV OHLC through last_d_time
 -> process_trend_zones() (regular/gap zone detection and qualification)
 -> valid non-invalidated nearest BZ below / SZ above CMP
 -> E calculate_trend()
 -> A calculate_trend(E as HTF regime)
 -> X calculate_trend(A as HTF regime)
 -> BUY_MATRIX/SELL_MATRIX keyed only by (E,A)
 -> HTFVetoEngine.check_veto()
 -> TrendContext -> API to_api() -> Decision Snapshot
```

`UP` means zone-side dominance, not a direct HH/HL test.  Rule B returns UP
when a valid nearest BZ exists below CMP and no valid nearest SZ exists above;
the mirror only-SZ condition returns DN.  Rule C uses any close after a zone's
creation that crosses the opposing distal.  Scenario 1 has no next opposing
zone; Scenario 3 crosses two; Scenario 2 defaults to SW unless acceptance plus
swing breach upgrade it.  Otherwise it is SW.

## Static code findings

- Data and E/A/X paths are set from `time_list[0]`, `[1]`, and `[-1]` in
  `scripts/trend_engine.py:1491-1501`; cash mappings are declared in
  `shared/config/settings.py:43-51`.  The audited mapping was weekly/daily/60m.
- Zone generation enters through `process_trend_zones` at
  `scripts/trend_engine.py:1556-1558`; its caller's cutoff is `last_d_time`.
  The API invokes this production path directly at
  `apps/finprod_backend/endpoints/candle_stick.py:573-606` and serializes the
  returned context unchanged with `to_api`; no UI transformation was found.
- Nearest BZ is the non-invalidated BZ with `distal < CMP` and maximum
  proximal; nearest SZ is the equivalent SZ with minimum proximal
  (`scripts/trend_engine.py:932-956`).  There is no age, retest, penetration,
  or state filter there.  Therefore a non-invalidated old zone can remain
  eligible.  Unaccepted gap zones are excluded.
- Rule B is implemented at `scripts/trend_engine.py:1093-1141`.  Its `htf_sz_overhead`
  and `htf_bz_below` arguments are not used in those conditions despite their
  diagnosis wording; only `htf_regime` gates the returned SW.
- Cascade max/min are from `nearest_bz.created_idx` / `nearest_sz.created_idx`
  through the final available candle (`1165-1243`), not a bounded calendar
  window.  In the ordinary API `calculate_full_context()` is called without
  cascade arrays, so cascade is `None`; the setup pipeline explicitly supplies
  raw E/A arrays (`scripts/setup_engine_new.py:550-555`).
- X regime does not determine final Bias: matrices are keyed only by `(E,A)`
  and `get_permissions` receives X **quadrant** only (`1624-1625`, `1847-1906`).
  The quadrant enforcement code is commented out (`1880-1892`).  X can only
  alter a setup-pipeline bias where E=A=SW (`setup_engine_new.py:570-578`).
- `HTFVetoEngine.check_veto` currently returns all-false/no-reason: all intended
  executable conditions are commented out (`1913-1977`).
- Thus E=UP+A=UP mechanically produces `Strong Bullish`, continuation longs,
  and blocked shorts through the matrices (`1794-1817`).  This is independent
  of X=DN or X=SW.

## Test methodology and coverage

Track A (`tests/trend_engine_audit/test_conformance.py`) creates lightweight
zones/candles and proves only-BZ, only-SZ, no-zone, HTF guards, Rule C
one/two-zone cases, both/neither breach, and matrix/X behavior.  Result:
**6 passed, 0 failed**. Existing `tests/engine_test.py` and
`tests/engine_test_fc.py` collected no tests.

Track B uses `audit/trend_engine/price_action_oracle.py`, not production zones.
Pivots use look-left=3/look-right=3; only confirmed pivots are used.  EMA20
slope, close relation, 20-bar return, range direction and last confirmed swing
levels are recorded as diagnostics in `observations.csv`.

Walk-forward data were the repository's ICICI Lombard CSVs: weekly 2020-01-01
to 2026-09-15 (351 bars), daily 2020-01-01 to 2026-09-16 (1,669 bars), and 60m
2020-01-01 to 2026-09-16 (11,623 bars). Each engine call filtered input at T.

## Results

| TF | engine UP/SW/DN | reference UP/SW/DN | exact agreement | severe opposite direction |
|---|---:|---:|---:|---:|
| E | 24/9/4 | 8/19/10 | 24.3% | 6 (16.2%) |
| A | 20/14/3 | 8/14/15 | 21.6% | 11 (29.7%) |
| X | 15/18/4 | 18/8/11 | 29.7% | 6 (16.2%) |

E flipped 15 times (40.5 per 100 sampled observations), A 15 (40.5), and X
11 (29.7). Sampling is approximately 40 daily bars, so these are not
bar-by-bar latency estimates.  The observed persistent severe episodes were
two sampled observations (about 59 calendar days): E UP/reference DN from
2022-02-01 to 2022-03-31 (-3.52% CMP), A UP/reference DN from 2022-09-23 to
2022-11-23 (-5.28%), X UP/reference DN from 2021-08-09 to 2021-10-06 (+7.57%),
and X UP/reference DN from 2024-10-29 to 2024-12-27 (-1.81%).  Finer latency is
inconclusive under this sampled replay; no numerical reversal-latency claim is
made.

Rule C Scenario 3 accounted for 14/37 E observations and is the dominant rule
in multiple opposite-direction episodes; Rule B only-BZ also produced A UP
against reference DN on 2024-05-08. This demonstrates that rising/falling price
structure and zone dominance can disagree under the coded methodology.

## ICICI_LOMBARD_GIC case study

At the reproducible 2026-09-15 12:15 cutoff, production returned E=UP, A=UP,
X=SW, Bias=Strong Bullish, long allowed, short blocked, long trade type
CONTINUATION. E and A used Rule C Scenario 3. The E nearest SZ had proximal
**1570.40** and distal 1771.00; E nearest BZ was 1419.40/1332.25. This matches
the distinctive 1570.40 value in the prompt. The exact 1374.20/1324.52
entry/stop pair could not be tied to the same snapshot from repository logs,
so it is not claimed reproduced.

This is **EXPECTED BY CURRENT RULES / METHODOLOGY-PRICE-ACTION DISAGREEMENT**:
the result follows historic close violations of two supply zones and preserves
that dominance. It is not proof that a visually declining chart must be DN.

## Conformance versus alignment, limitations, final findings

No exercised case showed the code doing something different from its own
conditions.  The counterintuitive cases are explained by historical zone
dominance and nearest-zone eligibility, rather than a direct market-structure
classifier.  A genuine defect was found only in the limited sense that the
documented HTF veto and quadrant enforcement have no active executable effect;
this is an implementation/documentation discrepancy, not a recommended change.

Scope is one symbol and 37 spaced cutoffs, selected to complete a full
production replay reproducibly. It cannot support the requested five examples
in every category, full-population regime-duration statistics, chart evidence,
or bar-precise reversal latency. Pivot confirmation necessarily delays the
oracle by three bars. No fixes are proposed.
