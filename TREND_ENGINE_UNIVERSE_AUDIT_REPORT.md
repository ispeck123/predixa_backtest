# Trend Engine Universe Audit — Phase 2

## Executive summary

No production files were changed. All 52 requested cash-equity symbols had
weekly, daily, and sixty-minute files and were processed through the active
production `MultiTimeframeTrendCalculator` at strictly clipped historical
cutoffs. The configured mapping was verified as `TIME_FRAMES_2 = [weekly,
daily, sixty]` in `shared/config/settings.py:44`.

This run finds **evidence of bullish classification asymmetry in the sampled
cross-section**, not a final claim about every weekly bar: conditional UP when
the independent confirmed-pivot structure is DN is substantially more frequent
than conditional DN when the reference is UP across E, A, and X. The
symbol-cluster bootstrap intervals are entirely above zero.

## Scope, sampling, and limitation

Coverage is 52/52 symbols, 881 observations, 2020-01-27 through 2026-08-01.
All requested files were present; `skipped_symbols.csv` is empty. Source
quality results are in `data_quality_report.csv`.

The intended Level-1 specification was every weekly bar. Production zone
recalculation at every bar is prohibitively slow in the sandbox, so this actual
run evaluates every twentieth weekly source timestamp (approximately 17 points
per symbol) rather than silently claiming full weekly density. It is therefore
a reproducible **cross-universe validation sample**, not the requested dense
weekly replay. Daily Level 2 and bar-precise reversal latency are not measured.

## Method

Each call filtered E/A/X source data to `<= T`, then invoked the unmodified
production engine. The reference oracle remains separate from zones: confirmed
3-left/3-right pivots classify HH+HL as UP, LH+LL as DN, otherwise SW. EMA and
return columns are diagnostics only.

## Directional error results

| TF | P(UP \| ref DN) | 95% Wilson CI | P(DN \| ref UP) | 95% Wilson CI | asymmetry | ratio |
|---|---:|---:|---:|---:|---:|---:|
| E | 50.9% (82/161) | 43.3–58.5% | 10.5% (39/372) | 7.8–14.0% | +40.4pp | 4.86 |
| A | 49.0% (99/202) | 42.2–55.9% | 17.0% (57/335) | 13.4–21.4% | +32.0pp | 2.88 |
| X | 68.8% (232/337) | 63.7–73.6% | 19.3% (37/192) | 14.3–25.4% | +49.6pp | 3.57 |

Symbol-level bootstrap (1,000 deterministic resamples) 95% CI for asymmetry:
E +31.3 to +48.7pp; A +23.6 to +40.0pp; X +42.9 to +56.1pp. Symbol-weighted
mean asymmetries are E +38.3pp, A +31.8pp, X +48.8pp, agreeing in direction
with the observation-weighted values.

## Rule attribution and final displayed Bias

Rule B Only-BZ is strongly one-sided in this sample: E had 31 UP/ref-DN and
zero DN/ref-UP events across 317 activations; A had 17 and zero across 163;
X had 39 and zero across 167. Rule C Scenario 3 is the largest absolute
contributor at A (69 UP/ref-DN, 43 DN/ref-UP), and also contributes at E
(28/25). This supports the Phase-1 hypothesis that zone availability and old
zone-violation dominance can preserve bullish classifications after the
independent structure turns DN. It does not establish that the zone methodology
is an implementation error.

Final UI-level counts: 25 final-Bullish observations where both E and A
references were DN; 18 final-Bearish observations where both references were
UP. There were 30 `Bias=BULLISH, X=DN` and 30 `Bias=BEARISH, X=UP` cases,
consistent with the documented E+A matrix determination of Bias rather than X.

There were 308 bullish-contradiction episodes and 104 bearish-contradiction
episodes under this sparse cadence. Median calendar duration is zero because
most are isolated sampled checkpoints; both maximum observed spans are 420
days. This cadence cannot responsibly support a bullish-vs-bearish lag claim;
`reversal_latency.csv` records that limitation.

## Conclusion

Within this 52-symbol, 881-observation, strictly as-of cross-sectional sample,
the evidence supports **B. evidence of bullish classification asymmetry**:
all three conditional rate differences are positive and the equally weighted
symbol bootstrap intervals exclude zero. The conclusion is qualified by the
coarse every-20-week sampling; a full every-week replay and dense reversal
study remain required before describing the magnitude or persistence as a
complete universe-wide result. No fix is proposed.
