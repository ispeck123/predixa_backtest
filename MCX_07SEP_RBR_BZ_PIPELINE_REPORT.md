# MCX CRUDEOILM 07-Sep RBR BZ Live-Path Trace

## Requested candidate

Base coordinates: **07-Sep-2026 13:00–15:00** (`3979..3981`); legout `3982` (16:00). The replay identifies candidates by these coordinates, not by the 07-Sep gap-open GDZ.

## Stage result

- Detector: **NOT EMITTED**
- Post-structure: **NOT REACHED**
- Final qualified list: **False**
- Active X setup list: **False**
- ZoneQualityScorer.score_zone: **NOT_REACHED**
- Block reason: **Detector emitted no BZ with base_start=3979 and base_end=3981.**

The live detector does not emit the requested three-bar `3979..3981` zone because 13:00 and 14:00 are non-basing. It **does**, however, emit the 15:00 candle as `TEST_X_BZ_3981`. The live failure is therefore in the later qualification pass, not in recognizing the 15:00 base candle.

## Exact rejection logic

`ZoneDetector._is_basing` calculates `body_pct` and immediately returns false when it exceeds `Config.basing_body_pct` (0.50): `scripts/trade_engine.py:421-425`. During detector flag construction, those results become the only eligible base flags: `scripts/trade_engine.py:1413-1416`. The detector then skips every false flag: `scripts/trade_engine.py:1422-1425`.

- Index 3979 / 13:00: body `88` ÷ range `100` = `0.880` > `0.50` → **not basing**.
- Index 3980 / 14:00: body `47` ÷ range `62` = `0.758` > `0.50` → **not basing**.
- Index 3981 / 15:00: body `3` ÷ range `34` = `0.088` ≤ `0.50` → basing.

Consequently, the engine cannot construct the requested wide three-bar zone (`prox≈8409/distal≈8344`). It constructs a narrower one-bar zone instead. `ZoneQualityScorer.score_zone` is only invoked after `process_qualified_zones_setup` returns zones, at `scripts/setup_engine_new.py:632-659`.

## Base and legout evidence

| Idx | Timestamp | O | H | L | C | Body% | Basing |
|---:|---|---:|---:|---:|---:|---:|---|
| 3979 | 2026-09-07 13:00:00 | 8437.0 | 8444.0 | 8344.0 | 8349.0 | 0.880 | False |
| 3980 | 2026-09-07 14:00:00 | 8353.0 | 8409.0 | 8347.0 | 8400.0 | 0.758 | False |
| 3981 | 2026-09-07 15:00:00 | 8400.0 | 8415.0 | 8381.0 | 8397.0 | 0.088 | True |
| 3982 | 2026-09-07 16:00:00 | 8397.0 | 8469.0 | 8387.0 | 8463.0 | 0.805 | False |

## Nearest actual emitted BZ

`TEST_X_BZ_3981` is the actual one-bar RBR emitted from the 15:00 base: proximal `8400.0`, distal `8381.0`, base `3981..3981`, departure `3982`. Its stage trace is:
- detector: **EMITTED**; structure removal: **True**; final qualified: **False**; active X setup: **False**; scorer: **NOT_REACHED (removed by qualification filter)**.
- block reason: **DEEP_PENETRATION (68% consumed)**.

### Actual invalidation cause

`ZoneQualifier.update_violation` starts scanning at `zone.created_idx + 1` (`scripts/trade_engine.py:2391`). For this zone, `created_idx=3981`, therefore it scans index `3982` — the zone's own 16:00 departure candle. For regular X-TF BZs, a low below proximal is converted to penetration (`scripts/trade_engine.py:2491-2497`) and penetration above 65% makes the zone RED/invalidated (`scripts/trade_engine.py:2510-2515`).
At index 3982 (2026-09-07 16:00:00), low `8387.0` produces depth `13.0` from proximal `8400.0` over zone height `19.0` = **68.42%**. It is the departure bar: **True**. This is the direct reason the emitted 15:00 BZ never becomes qualified.

### As-of evidence

The principal replay uses the requested scanner cutoff `2026-10-06 15:30:00`. The same zone is already RED at that cutoff because the consuming candle is dated 07-Sep, not a later 06-Oct candle.
| Replay cutoff | BZ_3981 emitted | State | Block reason | Qualified |
|---|---|---|---|---|
| 2026-09-07 15:30:00 | False | None | None | False |
| 2026-09-07 16:30:00 | False | None | None | False |
| 2026-09-07 17:30:00 | True | RED | DEEP_PENETRATION (68% consumed) | False |

The 15:30 and 16:30 formation-time replays do not yet emit the one-bar pattern. At 17:30—when the detector first emits it—the result is already RED from the 16:00 departure low. This rules out a later 06-Oct / 18:00 candle as the source of the recorded 68.42% penetration.

## Composite / GDZ check

**No — the requested BZ was not absorbed by `TEST_X_GDZ_3975`.** It was never emitted. The nearest actual `TEST_X_BZ_3981` has `replaced_by_composite=false`, no overlap partners, and no source-zone IDs. `TEST_X_GDZ_3975` is independently `RED` / `INVALIDATED_GAP_DEEP_WICK (>86%)` and is also absent from final qualified zones.

## Evidence locations

- Detector base/legout logic: `scripts/trade_engine.py:1360-1535`
- Qualification path: `scripts/trade_engine.py:4220-4249`
- Active setup path and scoring loop: `scripts/setup_engine_new.py:629-659`
- ZoneQualityScorer: `scripts/additional_engine_class.py:324-347`
