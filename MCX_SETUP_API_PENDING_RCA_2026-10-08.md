# MCX Setup API Pending / UI Freeze — Root-Cause Analysis

**Date:** 08 October 2026  
**Scope:** Diagnostic investigation only. No production logic, configuration, database rows, or migrations were changed.  
**Instrument reproduced:** `CRUDEOILM`  
**Contract / expiry:** `19-Oct-2026` (`19102026`)  
**Strategy stack:** TF4, `daily -> two_forty -> sixty`  
**Execution timeframe:** 60-minute (`X`)  
**Reproduction cutoff:** `2026-10-06 15:30:00`  
**Relevant chart CMP in the reproduction:** `8479.0`

---

## 1. Executive conclusion

The backend is not stuck in a non-terminating `while` loop. It is spending an extremely large amount of CPU time in a pathological zone-overlap / stop-reference calculation after the MCX E1 session changes leave far too many historical 60-minute zones eligible.

The causal chain is:

```text
MCX E1 session IDs attached
        |
        v
Cross-session violation and retest bars skipped
        |
        v
Old zones are not invalidated or marked retested after a later E1 session
        |
        v
372 X-TF zones remain qualified (instead of 37)
        |
        v
Huge overlap-partner groups are generated
        |
        v
RiskTargetCalculator repeatedly linearly searches zones by zone_id
        |
        v
~1.09 million list searches over a 512-zone list
        |
        v
~560 million zone-id comparisons / property constructions
        |
        v
/calculate_setup_commodities occupies the server event loop
        |
        v
UI API requests remain pending
```

The direct qualified-zones calculation is **not** the long-running call. On the same CRUDEOILM input, `process_qualified_zones(...)` completed in approximately **0.251 seconds**. The long-running work is in the setup pipeline invoked by `/calculate_setup_commodities`.

---

## 2. User-visible symptom explained

The frontend normally requests several commodity chart resources at once, including zones and setup calculation. The setup endpoint is declared `async`, but its core work is synchronous CPU work:

```python
# apps/backend/api/indian_commodity_futures.py
@router.get("/calculate_setup_commodities", tags=['Indian Commodity'])
async def calculate_setup_commodities(...):
    ...
    setup_data = process_setup_fc(st_sym, time_list, exp_num, last_d_time, False)
```

Source evidence: [`apps/backend/api/indian_commodity_futures.py:93-117`](../apps/backend/api/indian_commodity_futures.py).

`process_setup_fc(...)` has no `await`; it runs the entire `SDEnginePipeline` on the request-handling thread/event loop. Therefore, under a single-worker/single-event-loop backend, while this request is calculating, other requests assigned to that worker cannot run. They appear as **Pending** in the browser.

This is a starvation/blocking issue, not an API serialization issue and not evidence of database locking.

---

## 3. Exact reproduction and observed measurements

### 3.1 Dataset and invocation

The test used the existing live/historical commodity candle file:

```text
data/indian_commodity_data/latest_data_csv/CRUDEOILM_19102026_sixty.csv
```

The same time stack used by commodity TF4 was loaded from configuration:

```python
COMMODITY_TIME_FRAME_4 = ['daily', 'two_forty', 'sixty']
```

Source evidence: [`shared/config/settings.py:150`](../shared/config/settings.py).

The setup pipeline was invoked as the API does:

```python
process_setup_fc(
    'CRUDEOILM',
    ['daily', 'two_forty', 'sixty'],
    '19102026',
    datetime(2026, 10, 6, 15, 30),
    False,
)
```

### 3.2 Endpoint component timing

| Operation | Result |
|---|---:|
| `process_qualified_zones(...)` | completed in **0.251 seconds** |
| `process_setup_fc(...)`, normal MCX E1 path | did not complete within the bounded diagnostic window; stack sample at 10 seconds was inside `get_zone_by_id()` |
| `process_setup_fc(...)`, diagnostic-only E1-disabled control | completed in **0.652 seconds** |

The E1-disabled control was performed only by temporary in-memory monkey-patching inside a one-off diagnostic Python process. It did **not** alter a repository file or the running backend.

---

## 4. Quantitative E1 comparison

The same dataset and cutoff were evaluated by `process_qualified_zones_setup(...)` with E1 enforcement toggled in the diagnostic process.

| Metric | E1 disabled control | Current E1-enabled path |
|---|---:|---:|
| Raw X-timeframe zones | 585 | 512 |
| Qualified X-timeframe zones | 37 | 372 |
| Zones flagged as overlapping | 18 | 357 |
| Total overlap-partner references | 26 | 18,648 |
| Maximum partners held by one zone | 3 | 87 |

The raw number of detected zones does not grow under E1. The critical regression is the number of zones that **survive qualification**: 372 instead of 37, a little over ten times as many.

---

## 5. Technical root cause

### 5.1 E1 session filtering is applied to later lifecycle evaluation

`mcx_e1_session_ids(...)` assigns a different session ID when a prior bar is at or after 23:30 IST and the next bar is at or before 09:00 IST:

```python
seam = (
    previous.date() < now.date()
    and previous.time() >= time(23, 30)
    and now.time() <= time(9, 0)
)
if seam:
    current += 1
```

Source evidence: [`scripts/mcx_session.py:14-50`](../scripts/mcx_session.py).

`same_e1_session(...)` returns true only when both bar indices share that session ID:

```python
return not ids or ids[left] == ids[right]
```

Source evidence: [`scripts/mcx_session.py:53-55`](../scripts/mcx_session.py).

In the current lifecycle logic, every post-creation bar from a later session is ignored:

```python
for i in range(start, end):
    if not same_e1_session(cs, zone.created_idx, i):
        continue
```

Source evidence: [`scripts/trade_engine.py:2391-2401`](../scripts/trade_engine.py).

The same skip is repeated for retest counting:

```python
for i in range(start, cs.n):
    if not same_e1_session(cs, zone.created_idx, i):
        continue
```

Source evidence: [`scripts/trade_engine.py:2496-2514`](../scripts/trade_engine.py).

The implementation comment describes this intended behavior directly:

```text
a zone belongs to the session in which it was formed. A later MCX 09:00
gap-open cannot consume, invalidate, or retest it.
```

The practical effect on a multi-month candle history is that zones formed in earlier E1 sessions no longer receive their later-session invalidation/retest lifecycle updates. They remain available to the qualification result.

### 5.2 The setup pipeline explicitly enables this logic for MCX

The commodity path sets E1 on whenever `is_future is False`:

```python
_e1 = is_future is False
cs_E = self.get_candle_series_data(..., e1_session_enforced=_e1)
cs_A = self.get_candle_series_data(..., e1_session_enforced=_e1)
cs_X = self.get_candle_series_data(..., e1_session_enforced=_e1)
```

Source evidence: [`scripts/setup_engine_new.py:355-367`](../scripts/setup_engine_new.py).

Later, the execution timeframe runs:

```python
zones_X_processed, all_zone_X = process_qualified_zones_setup(
    self.csv_path_X, TF.X, self.last_d_time, e1_session_enforced=_e1)
```

Source evidence: [`scripts/setup_engine_new.py:628-634`](../scripts/setup_engine_new.py).

The setup qualification function attaches the MCX E1 IDs to both the detector and violation candle streams:

```python
session_id=mcx_e1_session_ids(df['unix_timestamp'].tolist()) if e1_session_enforced else None
...
session_id=mcx_e1_session_ids(violation_df['unix_timestamp'].tolist()) if e1_session_enforced else None
```

Source evidence: [`scripts/trade_engine.py:4128-4149`](../scripts/trade_engine.py).

### 5.3 Zone processing reaches a combinatorial overlap/stop calculation

The setup route processes every qualified X zone:

```python
for zone in zones_X_processed:
    ...
    risk_target = self.risk_calc.calculate(
        zone, current_cmp, all_zone_X, atr_X_val,
        partner_distal, zones_X_processed, htf_zones
    )
```

Source evidence: [`scripts/setup_engine_new.py:635-740`](../scripts/setup_engine_new.py).

For every overlapping zone, `_compute_entry(...)` resolves all partner IDs by repeatedly scanning the full active-zone list:

```python
for partner_id in zone.overlapping_partners_id:
    partner_zone = self.get_zone_by_id(all_zones, partner_id)
```

Source evidence: [`scripts/additional_engine_class.py:417-454`](../scripts/additional_engine_class.py).

Then `_get_sl_reference_levels(...)` repeats this for each selected stop-reference zone and each of its overlap partners:

```python
for zone in zones:
    ...
    for partner_id in zone.overlapping_partners_id:
        partner_zone = self.get_zone_by_id(all_zones, partner_id)
```

Source evidence: [`scripts/additional_engine_class.py:604-679`](../scripts/additional_engine_class.py).

Each `get_zone_by_id(...)` is a linear scan:

```python
for z in zones:
    if z.zone_id == zone_id:
        return z
```

Source evidence: [`scripts/additional_engine_class.py:364-369`](../scripts/additional_engine_class.py).

### 5.4 Measured computational load

Using the actual current E1 result (372 qualified zones, 512 all zones), the diagnostic calculation derived:

| Quantity | Measured value |
|---|---:|
| Entry-side linear ID lookups | 18,660 |
| Stop-reference linear ID lookups | 1,075,055 |
| Total linear `get_zone_by_id()` calls | 1,093,715 |
| Elements scanned per call | up to 512 |
| Estimated `zone_id` comparisons/property evaluations | **559,982,080** |

This is the dominant source of runtime. It is not an endless loop: every individual loop is finite. It is nevertheless large enough to keep a request worker busy for tens of seconds or longer.

---

## 6. Runtime stack evidence

The normal setup execution was bounded and a traceback was captured after ten seconds. It showed the active CPU stack as:

```text
scripts/models.py:332              Zone.zone_id
scripts/additional_engine_class.py:367  RiskTargetCalculator.get_zone_by_id
scripts/additional_engine_class.py:662  _get_sl_reference_levels
scripts/additional_engine_class.py:708  RiskTargetCalculator.calculate
scripts/setup_engine_new.py:740         SDEnginePipeline.run
scripts/setup_engine_new.py:1220        process_setup_fc
```

Relevant code locations:

- [`scripts/models.py:330-333`](../scripts/models.py) — computed `zone_id` property.
- [`scripts/additional_engine_class.py:364-369`](../scripts/additional_engine_class.py) — linear lookup.
- [`scripts/additional_engine_class.py:660-665`](../scripts/additional_engine_class.py) — partner lookup in stop reference construction.
- [`scripts/additional_engine_class.py:692-740`](../scripts/additional_engine_class.py) — risk calculation invokes the stop reference construction.

The stack was sampled while the first risk calculation was traversing overlap-derived stop levels. A focused, diagnostic-only interception immediately before normal risk calculation reported:

```json
{
  "first_risk_zone_id": "TEST_X_BZ_28",
  "all_zone_X": 512,
  "active_zones": 372,
  "sl_zones": 68,
  "sl_zone_overlap_partners_first_10": [68,68,68,68,68,68,68,68,68,68],
  "all_overlap_max": 87,
  "all_overlap_total": 18648
}
```

This confirms the scale is present before the request enters the expensive risk loop; it is not caused by frontend retries.

---

## 7. What is and is not affected

### Confirmed affected

- MCX setup calculation through `/calculate_setup_commodities`.
- The CRUDEOILM TF4 path at the tested cutoff.
- Other requests sharing a single event-loop worker while this computation runs.
- Any MCX symbol/timeframe with a similarly large E1-qualified historical zone population and overlap graph.

### Explicitly not identified as the direct blocker in this reproduction

- The `/get_qualified_zones_commodity` calculation itself. Its underlying function completed in approximately 0.251 seconds on the same data/cutoff.
- A database lock, migration, or write contention. No database mutation was performed during the investigation.
- An unbounded `while` condition. The observed delay is finite combinatorial work.

### Note on synchronous logging

`process_qualified_zones(...)` also prints every zone during its qualification loop:

```python
print(zone.ztype, zone.proximal, zone.distal, zone.state, zone.violation, zone.block_reason)
```

Source evidence: [`scripts/trade_engine.py:4003-4013`](../scripts/trade_engine.py).

This is undesirable in production and can increase request latency or log volume. It is not, however, the primary freeze mechanism demonstrated above: the qualified-zones calculation still completed in about 0.251 seconds. The setup route’s overlap/risk traversal is the primary bottleneck.

---

## 8. Why this began after the E1-related work

The relevant behavioral change is not simply detection segmentation. It is the combined effect of:

1. attaching `session_id` on the MCX setup data path;
2. using E1 session identity when evaluating violations; and
3. using it again while counting retests.

The detector also segments MCX detection by session:

```python
if not session_ids or len(session_ids) != cs.n or len(set(session_ids)) == 1:
    return self._detect_unbounded(symbol, tf, cs)
...
for end in range(1, cs.n + 1):
    ...
    local = CandleSeries(...)
    for zone in self._detect_unbounded(symbol, tf, local):
        ...
```

Source evidence: [`scripts/trade_engine.py:1360-1394`](../scripts/trade_engine.py).

This detector loop is finite and, in fact, raw zone count fell from 585 to 512 in the control comparison. The post-detection lifecycle behavior is what causes the qualified population to grow from 37 to 372.

---

## 9. Reproduction commands used

These commands were read-only diagnostics. The control invocation altered only symbols in that temporary Python process; it did not write source files or mutate the backend.

### 9.1 Confirm normal qualified-zones duration

```bash
PYTHONPATH=. python - <<'PY'
from datetime import datetime
from scripts.trade_engine import process_qualified_zones
from scripts.models import TF

process_qualified_zones(
    'data/indian_commodity_data/latest_data_csv/CRUDEOILM_19102026_sixty.csv',
    TF.X,
    datetime(2026, 10, 6, 15, 30),
    ['daily', 'two_forty', 'sixty'],
    'CRUDEOILM',
)
PY
```

### 9.2 Compare E1 enabled and disabled qualification populations

```python
qualified, all_zones = process_qualified_zones_setup(
    csv_path,
    TF.X,
    datetime(2026, 10, 6, 15, 30),
    e1_session_enforced=True,  # then False for control
)
```

### 9.3 Bounded stack capture

```python
faulthandler.dump_traceback_later(10, repeat=False)
process_setup_fc(
    'CRUDEOILM',
    ['daily', 'two_forty', 'sixty'],
    '19102026',
    datetime(2026, 10, 6, 15, 30),
    False,
)
```

---

## 10. Recommended remediation direction (not implemented)

No fix was made as part of this investigation. The evidence supports treating this as two separate correctness/performance decisions:

1. **Clarify lifecycle semantics across an E1 seam.**  
   The E1 requirement must prevent a zone’s base/legout formation from spanning the overnight seam. It must be explicitly decided whether a zone formed in a prior session is still allowed to be invalidated/retested by ordinary later-session price action. The present code makes every later session invisible for those lifecycle operations, which creates the 372-zone accumulation.

2. **Make overlap partner lookup bounded/linear-time.**  
   The risk path should not repeatedly search a list by computed `zone_id` for each overlap reference. A per-call ID map or a topology representation built once would remove the observed ~560 million comparison pattern. This is a performance hardening measure; it should be implemented separately from the semantic E1 decision and verified against expected stop-reference behavior.

3. **Do not run long synchronous CPU work directly in the async request event loop.**  
   Even after correctness and complexity fixes, CPU-heavy setup generation should not prevent unrelated chart APIs from responding. This is an operational resilience concern, not a substitute for fixing the explosion above.

4. **Replace production `print()` statements with bounded debug logging.**  
   In particular, avoid printing every zone in a live chart endpoint.

---

## 11. Verification criteria for a future fix

A future corrective change should not be considered complete until it proves all of the following on this exact CRUDEOILM TF4 input:

| Check | Required result |
|---|---|
| Setup endpoint completes | bounded, operational response time; no pending UI cascade |
| X-zone population | no historical cross-session accumulation caused solely by E1 lifecycle skipping |
| E1 formation rule | base/legout construction cannot span the 23:30 -> 09:00 seam |
| Lifecycle rule | documented and tested behavior for later-session invalidation/retest |
| Overlap performance | no repeated full-list lookup per overlap partner/reference |
| Qualified-zones endpoint | retains its response contract and remains responsive |
| Setup output | entry/SL/target behavior verified independently from this performance fix |

---

## 12. Final finding

**Primary root cause:** the current E1 lifecycle skip causes a large historical MCX zone population to remain qualified; the existing overlap-based risk/stop implementation then performs approximately 560 million zone-ID comparisons for one CRUDEOILM setup request.

**Why APIs remain pending:** `/calculate_setup_commodities` executes this CPU-bound setup pipeline synchronously inside an `async` route. In a single worker/event loop, it prevents other UI API calls from being serviced until it returns.

**Is this an infinite loop?** No. It is a finite but pathological combinatorial workload.

**Were production files/data changed for this report?** No.
