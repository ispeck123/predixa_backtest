# Cash Live-Order Review — 3 August to 1 September 2026

## Executive summary

The current rerun report must not be treated as a historical reconstruction of the live engine. Live cash ordering began on 3 August 2026, while the engine was subsequently changed twice. The rerun evaluates retained historical `order_master` rows with today's engine and today's candle-derived zone state. Consequently, a missing current setup or a changed zone classification is expected and does not prove what the live engine emitted.

The two reported `SZ-BUY` cases were not SZ setups in the original live scanner output. Both were emitted as BUY trades from `BZ` (demand) zones, for which `is_buy_zone=True`. Their SZ labels in `all_orders_engine_enriched.csv` are rerun-report matching artifacts.

The engine invocation is stateless with respect to past orders and consumed zones. No `already_traded_zones` or equivalent order-history input is supplied to `process_setup`, `SDEnginePipeline.run`, or `format_calculate_setup_response`. Existing duplicate checks operate on trade/order prices and recent trade similarity, not persistent zone identity. Therefore, consumed-zone/re-fire suppression belongs in the orchestration layer that has access to `order_master`.

## 1. The two alleged SZ-BUY orders

| Order | Live emission | Live setup | Live execute zone | `ztype` | `is_buy_zone` | Finding |
|---|---|---:|---|---|---|---|
| 164261 — ICICI Bank, TF 5 | 2026-08-03 13:23 | BUY 1409.9117 / SL 1389.04796 / target 1458.730585 | `TEST_X_BZ_1436`, range 1400.0–1407.8 | `BZ` | `True` | The current CSV's SZ label is false |
| 164272 — Britannia, TF 2 | 2026-08-04 17:41 | BUY 5333.989 / SL 5224.95743 / target 5640.5 | `TEST_X_BZ_3361`, range 5274.0–5326.0 | `BZ` | `True` | The current CSV's SZ label is false |

Evidence comes from the original append-only `cash_wetrun_scan_results.jsonl`, not a present-day rerun. The live records contain the exact order prices, BUY direction, and corresponding execute-timeframe zones under `ZONES_X.Buy`. The model defines `is_buy_zone` as true only for `BZ` and `GDZ`; `SZ` and `GSZ` are sell zones.

The formatted live output did not persist a direct `selected_zone_id` inside the BUY trade dictionary. The zone IDs above are the matching live execute-timeframe BUY candidates whose boundaries generate the recorded entries. This is still conclusive on direction/type: the live payload lists these zones under `Buy`, labels them `BZ`, and contains no BUY setup originating from an SZ.

### Why the rerun produced SZ

For both orders, the current engine produced no corresponding setup:

- `engine_generated_setups_count = 0`
- `matched_setup = null`
- `match_reason = NO_SAME_SIDE_SETUP`
- `selected_zone_id` is empty

The `match_reason` column is the reliability/relationship column that must be read alongside every rerun-derived zone field. `NO_SAME_SIDE_SETUP` means the same setup does not exist in the current engine result.

Despite that, `extract_selected_zone_fields()` continued with a null zone ID. With no timeframe, zone type, or creation index to filter on, it selected the first item returned from `raw_setup_payload.zones_X`. That unrelated first item happened to be an SZ with retest count zero. The report then copied its fields into the historical order row. Thus the SZ label is a report fallback defect, not a live engine defect or logging mislabel.

Required report rule: when `selected_zone_id` is absent or `match_reason` is `NO_SAME_SIDE_SETUP`, all selected-zone attributes must remain null/unknown. The report must never substitute the first available zone.

## 2. SL-hit post-stop price path

There are 17 SL-hit rows. Two companion files were generated:

- `sl_post_stop_20_bars.csv`: one row per execute-timeframe candle strictly after `completed_on`, including OHLC and per-bar target-touch status.
- `sl_post_stop_20_bar_summary.csv`: one row per trade with coverage, maximum high, minimum low, target recovery, and classification.

Of the 17 SL-hit trades:

- 10 have a complete 20-bar window.
- 7 have incomplete coverage because the candle dataset ends on 1 September 2026.
- None touched its original target within the available post-stop window.
- All 10 complete windows are classified `NO_TARGET_RECOVERY_IN_20_BARS`.
- The remaining 7 are `INCONCLUSIVE_INCOMPLETE_20_BAR_COVERAGE`; they must not be used to conclude that a rebound could not occur within 20 bars.

This sample does not currently show the strong SL-hunt signature defined as “stop hit, then original target reached within the next 20 execute-timeframe bars.” It does not by itself prove that stops are optimal: stop quality also needs normalized post-stop excursion, volatility/ATR context, slippage, and a larger sample.

Important methodology: the window starts strictly after the recorded stop timestamp. It excludes the stop candle itself and uses the order's execute timeframe (daily, 60, 125, 25, or 75 minutes), not a common timeframe across all trades.

## 3. Stateless scan and re-fire/freshness ownership

The live scan call chain is:

1. The orchestrator calls `process_setup(symbol, time_list, last_d_time)`.
2. `process_setup` creates a new `SDEnginePipeline` and calls `run(tick, time_list, last_d_time)`.
3. The orchestrator calls `format_calculate_setup_response(raw, stock_name, time_fr, last_d_time, is_cash=True)`.
4. If a BUY and minimum RR are present, the orchestrator inserts a trade signal and order.

No call passes order history, prior trade IDs, consumed zone IDs, or an already-traded-zone list. `format_calculate_setup_response` receives the scan result plus formatting/data-location arguments only. It cannot know that the same zone generated an earlier order.

There are existing trade-level safeguards, but they do not solve consumed-zone re-fire:

- `insert_trade_signals` calls `find_similar_trade` using symbol, timeframe, side, prices and a two-day cutoff.
- `insert_order_and_oms_bucket` checks for an exact same symbol/timeframe/entry/SL/target order.
- Active trades are queried during a separate revalidation path.

These checks are price/signature based and can miss the same zone when recalculated entry, stop, target, zone index, or code version changes. They also do not establish a durable “zone has already been traded” state.

## Recommended ownership and next step

The re-fire/freshness problem should be fixed at the orchestration boundary, not inside `format_calculate_setup_response`:

```text
scan symbol/TF
  -> obtain selected setup and stable zone identity
  -> query order_master / zone-trade ledger for prior consumption
  -> suppress consumed zone (with an explicit reason)
  -> only then insert trade signal and order
```

The engine must expose and persist a stable selected-zone identity with every emitted setup. The orchestration layer must then query which zones were previously traded and enforce the agreed consumption/reset policy before order creation. The precise database query, identity key, reactivation rules, and migration/backfill policy remain to be specified after the proposed design is shared.

## Reporting limitations and immediate corrections

- Historical performance should use emission-time payloads wherever available. A current-engine rerun must be labelled as a compatibility comparison, not historical truth.
- Add `same_setup_exists_now` derived from `match_reason == PRICE_MATCH`; retain `match_reason` beside it.
- Null all zone fields when no zone ID is established. Do not perform an unqualified first-zone fallback.
- Persist at emission: engine version/commit, selected zone ID, `ztype`, `is_buy_zone`, retest count, penetration, setup direction, and complete raw/formatted payload.
- Add the 20 post-stop OHLC bars to the normal evaluation output, including coverage status, rather than calculating only MFE/MAE up to exit.
- Keep re-fire/freshness work separate from this historical rerun correction. The former is a new orchestration control; the latter is a report data-integrity fix.

## Conclusion for management

The present report mixes retained historical orders with a newer engine and then incorrectly fills unmatched zone fields from unrelated current zones. That makes several zone labels and freshness values vague or wrong. The two apparent SZ-BUY orders are confirmed live BZ-BUY orders, so they are not evidence of a live direction bug. Post-stop analysis currently finds no target recovery among the 10 trades with full 20-bar coverage, while 7 trades remain inconclusive due to data cutoff. Finally, scans do not carry consumed-zone history into the engine; durable re-fire prevention must be implemented in the order-aware orchestration layer once its policy is agreed.
