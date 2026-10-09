# SYSTEM ARCHITECTURE REPORT: RESOLVING OHLC DATA DISCREPANCIES IN ROTATING EXPIRES (NEAR/NEXT/FAR)

**Audience:** Development, Database Engineering, and QA  
**Scope:** MCX and NSE derivative historical OHLC ingestion, storage, rollover, and chart presentation  
**Classification:** Internal technical RCA and remediation design

## Executive Summary

The platform's 30-minute ingestion engine persists candles for discrete, expiry-bound futures contracts. This is materially different from a broker's continuous futures chart, which can present a historically rolled and price-adjusted view across successive active contracts. As a far-expiry contract progresses through Next and then Near status, the raw history captured while it was illiquid remains part of its stored series, producing visual and mathematical mismatches against a broker chart that represents the active market through time. The discrepancy is therefore principally a market-series identity and lifecycle problem, not a chart-rendering problem.

## Problem Statement

The current architecture periodically records OHLC data for individual futures expiries. When the selected expiry is a Far contract, this process captures the contract's actual traded market state at that time rather than the liquidity and price formation of the active Near contract.

In MCX and NSE derivatives, liquidity normally concentrates in the Near month. A Far contract can have little or no continuous trading, so a 30-minute candle may be formed by one or two isolated trades, carry forward an earlier price, or remain unchanged for several intervals. These are valid raw prints for that specific contract, but they are not representative of the most liquid tradable market at that historical point.

Contract rotation amplifies the issue:

```text
At historical time T0                 At later time T1              At expiry transition T2
---------------------                 -----------------             -----------------------
Far contract: illiquid      -->       Next contract       -->         Near contract: liquid
Sparse / stale candles                More activity                   Normal price discovery
Large carry premium/discount          Intermediate state              Current live chart looks normal

Stored contract history:  [ sparse Far-month raw candles | ... | liquid Near-month raw candles ]
Broker continuous chart:  [ active contract at T0        | roll | active contract at T2        ]
```

The resulting platform chart can exhibit flat lines, fragmented or erratic candles, discontinuities at rollover, and large apparent price gaps. A broker chart can appear clean because it is presenting a continuous market view rather than the unmodified history of the eventual Near contract.

## Root Cause Analysis (RCA)

### 1. Continuous Futures Series vs. Discrete Contract Series

The two chart types are distinct financial data products and must not be compared as though they represent the same instrument history.

| Characteristic | Continuous futures chart | Discrete contract series |
|---|---|---|
| Identity | Underlying / rolling market representation | One exact tradable expiry |
| Historical source | Contract selected for each historical period according to the provider's roll rule | Only trades in that expiry, across its complete listed life |
| Rollover handling | Provider can stitch contracts and apply continuity treatment | Raw price jump remains when compared with another expiry |
| Primary use | Technical analysis of the active market over long periods | Execution, audit, contract-specific P&L, and expiry-specific analysis |
| Price continuity | Usually designed to be visually continuous | Not guaranteed; basis and liquidity changes are real |

The platform's expiry-keyed data files describe a discrete series. A broker's continuous chart may instead choose the historically active contract at each point in time and apply its own roll and adjustment treatment. A continuous series must therefore be stored with its own identity and metadata; it must not overwrite or be appended to a fixed-expiry series.

### 2. The Far-Month Illiquidity Void

MCX and NSE derivative liquidity is generally concentrated in the Near contract. The Far contract can have sparse order-book depth and infrequent execution until it becomes closer to expiry.

Consequences for 30-minute candle storage include:

- A candle can be determined by one or two prints rather than broad price discovery.
- Open, high, low, and close can be stale or unchanged across multiple intervals.
- Volume is too low to make the candle representative of the active market.
- A single trade can create an apparent spike or discontinuity.
- The eventual contract history permanently retains these low-liquidity early-life candles unless the series is intentionally reconstructed.

This behavior is not necessarily a broker-data defect. It is the expected consequence of retaining raw observations from a thinly traded contract.

### 3. Rollover Gaps: Premium, Discount, and Cost of Carry

Different expiries do not necessarily trade at the same price. The spread between contracts is influenced by cost of carry, financing, storage and convenience yield for commodities, expected interest rates, market positioning, time to expiry, and liquidity.

```text
Underlying market / active Near contract             84,000
Far contract with time premium                       84,240
Observed contract basis                              +240

Raw contract rotation without adjustment:
... 84,000 | 84,015 | 84,010 || 84,245 | 84,250 | 84,238 ...
                             ^ roll gap / basis difference

Continuous chart treatment:
... active contract history | provider roll logic / adjustment | active contract history ...
```

The platform correctly records raw discrete prices when it stores a particular contract. However, those raw values cannot be expected to match a broker's continuous visual series across rollover. The mismatch becomes particularly visible when a Far contract becomes the Near contract: the later liquid segment can appear normal, while the earlier low-liquidity and premium/discount segment remains fragmented and displaced.

## Architectural Evidence Model

Every persisted candle series must declare its series identity before it is eligible for chart comparison or zone calculation.

```text
                         +------------------------+
FYERS History API  ----> | Ingestion classification |
                         +------------+-----------+
                                      |
              +-----------------------+-----------------------+
              |                                               |
              v                                               v
+-------------------------------+             +-------------------------------+
| Fixed-expiry raw contract     |             | Continuous analytical series   |
| MCX:SYMBOL26OCTFUT            |             | MCX:SYMBOL + roll methodology  |
| cont_flag = 0                 |             | cont_flag = 1                  |
| expiry = 2026-10-19           |             | no single expiry identity       |
+---------------+---------------+             +---------------+---------------+
                |                                             |
                v                                             v
Execution / contract-specific chart             Long-horizon technical chart
and contract-specific zones                     and continuous-series zones
```

Required metadata for either dataset:

| Field | Purpose |
|---|---|
| `exchange` | Market segment, such as MCX or NSEFO |
| `fyers_symbol` | Exact symbol requested from FYERS |
| `contract_expiry` | Required for fixed-contract series; null for a purely continuous identity |
| `series_type` | `FIXED_EXPIRY_RAW` or `CONTINUOUS` |
| `cont_flag` | Exact API mode used for the data retrieval |
| `resolution` | Candle resolution used by FYERS |
| `roll_method_version` | Provider/native mode or internal roll algorithm version |
| `source_fetch_timestamp` | Auditability and refresh tracking |
| `finalization_state` | Whether a candle is complete or still mutable |

## Solutions and Architectural Remedies

### Solution A — API Native Continuous Series

Use FYERS API v3 history requests with `"cont_flag": "1"` when the product requirement is parity with FYERS' continuous historical chart behavior. Store this response as a dedicated continuous series; do not write it into an expiry-specific raw-contract CSV or table.

```json
{
  "symbol": "MCX:CRUDEOILM26OCTFUT",
  "resolution": "30",
  "date_format": "1",
  "range_from": "2026-01-01",
  "range_to": "2026-10-09",
  "cont_flag": "1"
}
```

Implementation requirements:

1. Use a separate storage identity, for example `MCX_CRUDEOILM_CONTINUOUS_30.csv` or an equivalent table key.
2. Persist `series_type=CONTINUOUS`, `cont_flag=1`, provider name, and fetch parameters alongside the candles.
3. Route the platform's continuous chart and its associated zone engine to this dataset only.
4. Do not compare continuous-series zone boundaries directly with zones created from fixed-expiry candles.
5. Reconcile a sample of historical dates against the FYERS chart only after confirming equivalent symbol, resolution, range, and chart mode.

Advantages: lowest implementation complexity, provider-managed roll behavior, and the closest available API representation of a native FYERS continuous view.

Risks: the provider's exact roll/adjustment methodology is external behavior; it must be treated as a source-defined series rather than as raw exchange-contract history.

### Solution B — Database Backfill Trigger After Contract Rotation

Implement an asynchronous expiry-lifecycle job after the configured post-expiry cutoff hour. The job must identify data classified as obsolete Far/Next raw history for a product view, archive it, and backfill the newly active Near contract into the intended active-market dataset.

```text
Expiry calendar / symbol master
             |
             v
Post-expiry cutoff scheduler
             |
             +--> Determine active Near contract and exact FYERS symbol
             |
             +--> Lock series identity and create immutable run record
             |
             +--> Archive old active-view dataset (never silent-delete audit data)
             |
             +--> Fetch clean historical range for newly active Near contract
             |
             +--> Validate OHLC, timestamps, duplicates, and row counts
             |
             +--> Atomically publish replacement active-view dataset
             |
             +--> Invalidate chart and zone caches by series identity/version
```

The job must not blindly delete fixed-expiry raw data. It should remove or replace only the active-market presentation dataset whose explicit purpose is to show the rotating active contract. Raw expiry-specific contract candles should remain archived and queryable for execution audit and historical research.

Minimum production controls:

- Dry-run by default, with explicit underlying, expiry, timeframe, and date-range scope.
- Immutable pre-backfill snapshot and checksum.
- Symbol-master validation before every request.
- OHLC invariant validation: `low <= open/close <= high`.
- Unique candle-start timestamp enforcement per complete series identity.
- Atomic publish only after row-count and reconciliation checks pass.
- Rollback using the archived dataset version.
- Cache invalidation keyed by `series_type`, expiry, resolution, and source version.

## Recommended Operating Model

| User requirement | Dataset to use | Zone calculation input |
|---|---|---|
| Match broker continuous chart | API-native continuous series | Continuous candles only |
| Trade a selected expiry | Fixed-expiry raw series | That exact contract only |
| Review historical active market | Continuous or lifecycle-managed active-market dataset | Same selected series only |
| Audit trades and fills | Fixed-expiry raw series | Not applicable unless contract-specific analysis is required |

The application must expose the selected series type in the UI and API response. A label such as `Continuous` or `19-Oct-2026 Contract` is necessary to prevent users from making invalid visual comparisons.

## Official Documentation & Evidence Links

- **Continuous Chart Mechanics**: https://fyers.in
- **Price Mismatch & Gaps Support Ticket**: https://fyers.in
- **API v3 Implementation Thread**: https://fyers.in

## Next Steps / Action Items

- **Backend/API Engineering:** implement the continuous-series ingestion path using FYERS `cont_flag=1`, with a storage identity that cannot collide with fixed-expiry data.
- **Database Engineering:** review schema, cache keys, retention, archival, and uniqueness constraints for `series_type`, exact symbol, expiry, resolution, and roll version.
- **Market Data Engineering:** define the post-expiry cutoff, active-contract selection rule, and controlled backfill workflow for the active-market view.
- **QA:** validate Near/Next/Far transitions using known MCX and NSEFO roll dates; compare API, stored candles, backend payload, and rendered chart at identical timestamps.
- **Quant/Strategy Engineering:** calculate supply/demand zones independently for fixed and continuous series; prohibit cross-series zone comparisons.
- **Operations:** alert on invalid OHLC, sparse-volume thresholds, duplicate timestamps, contract-symbol mismatch, and cache keys that omit expiry or series type.

## Conclusion

The observed OHLC discrepancy is the expected result of comparing two different instrument histories: raw, expiry-bound contract candles and a broker-oriented continuous market representation. The durable solution is not to visually mask gaps or overwrite raw contracts indiscriminately. It is to model fixed-expiry and continuous series as separate first-class datasets, preserve their provenance, and ensure charts, APIs, caches, and zone calculations use one explicitly selected series identity end-to-end.
