# MCX CRUDEOILM 07-Sep Zone Pipeline Replay

## Scope

Read-only replay. No scanner, database, OMS, broker, or production logic was changed.

## Requested post-pipeline row

| Field | Value |
|---|---:|
| zone_id | TEST_X_GDZ_3975 |
| zone_type | GDZ |
| timeframe | X |
| proximal | 8399.0 |
| distal | 8126.0 |
| base_start_idx | 3970 |
| legout_end_idx | 3975 |
| removes_structure | True |
| rr | None |
| retest_count | 4 |
| penetration_pct | 0.0 |
| score_total | None |
| state | RED |
| block_reason | INVALIDATED_GAP_DEEP_WICK (>86%) |

`legout_end_idx` is represented by the model field `departure_idx`. `score_total` and `rr` are not populated by the active MCX setup path; its active equivalents are `final_score=0` and `rr_ratio=None`.

## Pipeline result

The raw detector found the GDZ, with `removes_structure=True`. After violation/retest processing it is `RED` and is rejected with `INVALIDATED_GAP_DEEP_WICK (>86%)`. It is absent from both the final qualified list and active setup-engine X-zone list.

## Construction window

| Pos | Timestamp | Open | High | Low | Close |
|---:|---|---:|---:|---:|---:|
| 3970 | 2026-09-04 19:00:00 | 8246.0 | 8252.0 | 8126.0 | 8218.0 |
| 3971 | 2026-09-04 20:00:00 | 8218.0 | 8328.0 | 8218.0 | 8303.0 |
| 3972 | 2026-09-04 21:00:00 | 8305.0 | 8336.0 | 8287.0 | 8307.0 |
| 3973 | 2026-09-04 22:00:00 | 8307.0 | 8355.0 | 8299.0 | 8324.0 |
| 3974 | 2026-09-04 23:00:00 | 8328.0 | 8335.0 | 8314.0 | 8321.0 |
| 3975 | 2026-09-07 09:00:00 | 8399.0 | 8449.0 | 8399.0 | 8436.0 |

## ZoneRanker configuration check

The active MCX scanner does **not** invoke `ZoneRanker.score`; it uses `ZoneQualityScorer.score_zone`. The legacy `trade_engine.SDEnginePipeline` constructs ZoneRanker with `scripts.models.Config`. That object has `badzone_base_len=False` and `rr_min=False`; it instead exposes `max_base_len=4` and `min_rr=2.1`. For this already-invalidated zone, the first ZoneRanker hard gate returns `INVALIDATED_TRUE_BREAK` before reading configuration. A copy with only that preceding gate bypassed fails at the configuration access: `AttributeError: 'Config' object has no attribute 'badzone_base_len'`.

## Source evidence

- Detection: `scripts/trade_engine.py:4204-4209`
- Violation, retest and structure pass: `scripts/trade_engine.py:4220-4230`
- Final qualified-zone filter: `scripts/trade_engine.py:4241-4249`
- Active MCX setup scoring: `scripts/setup_engine_new.py:632-659`
- Legacy ZoneRanker gates: `scripts/trade_engine.py:3249-3287`
- Legacy ZoneRanker construction: `scripts/trade_engine.py:3492-3498`
- Base Config thresholds: `scripts/models.py:383-440`
