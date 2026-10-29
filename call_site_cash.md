`prod` HEAD (`5dba823`, matching `origin/prod`) uses two conflicting sizing sources.

### Call site

scripts/cash\_wetrun\_scanner.py:430 calls `gate()`; `:457` (scripts/cash\_wetrun\_scanner.py:457) passes its multiplier into `size_for_order()`:

```
tier, size_mult, gate_reason = gate(
    "NSE_CASH", job.time_frame,
    formatted.get("e_regime"), formatted.get("a_regime"),
)
...
qty = size_for_order(
    formatted, t_data, str(job.time_frame), "NSE_CASH",
    size_multiplier=size_mult,
)
```

`gate()` returns the multiplier. `size_for_order()` returns an order quantity (or `None`), not a multiplier.

### Actual source used

- Applied multiplier: `scripts.tf_regime_gate_by_exchange.gate()`
- Wrapper: `scripts.sizing_bridge.size_for_order()`
- Conflicting preliminary eligibility lookup: `scripts.position_sizing.REGIME_TF_SIZE_WEIGHT`

scripts/sizing\_bridge.py:91:

```
e, a, tf = _regime_key(formatted, time_frame)

# Fail-safe: if regime context is missing, the weight lookup will miss -> None (do not place blind).
weight = REGIME_TF_SIZE_WEIGHT.get((e, a, tf), 0.0)
if weight <= 0.0:
    logger.error("return None because weight <= 0.0")
```

Then, with `WETRUN_FIXED_QTY = True`, it uses one share and applies the gate multiplier:

```
if WETRUN_FIXED_QTY:
    quantity = lot_size if lot_size > 1 else 1

quantity = int(quantity * float(size_multiplier))
return quantity if quantity > 0 else None
```

So this is not a single source of truth. The gate determines the passed multiplier, but the legacy table can still veto it.

### Production grep hits

```
scripts/cash_wetrun_scanner.py:22:from scripts.sizing_bridge import size_for_order
scripts/cash_wetrun_scanner.py:23:from scripts.tf_regime_gate_by_exchange import gate
scripts/cash_wetrun_scanner.py:438:formatted["gate_size_multiplier"] = size_mult
scripts/cash_wetrun_scanner.py:457:qty = size_for_order(
scripts/position_sizing.py:34:REGIME_TF_SIZE_WEIGHT = {
scripts/position_sizing.py:104:w = REGIME_TF_SIZE_WEIGHT.get((s.e_regime, s.a_regime, s.time_frame), 0.0)
scripts/setup_engine_new.py:17:from scripts.tf_regime_gate_by_exchange import gate
scripts/setup_engine_new.py:1208:out.update(gate_tier=tier, gate_size_multiplier=size_mult, gate_reason=gate_reason)
scripts/sizing_bridge.py:23:PositionSizer, Setup, PortfolioPolicy, SegmentPolicy, REGIME_TF_SIZE_WEIGHT,
scripts/sizing_bridge.py:79:def size_for_order(
scripts/sizing_bridge.py:94:weight = REGIME_TF_SIZE_WEIGHT.get((e, a, tf), 0.0)
```

### Cell-by-cell: deployed live path

| Cell | `gate()` multiplier | Legacy sizing eligibility | Actual wet-run outcome |
|---|---:|---:|---|
| SW/SW/2 | 1\.0 | 1\.0 | emits qty 1 |
| SW/SW/6 | 1\.0 | 1\.0 | emits qty 1 |
| SW/SW/25 | 1\.0 | 1\.0 | emits qty 1 |
| SW/UP/2 | 1\.0 | 0\.35 | emits qty 1 |
| SW/UP/6 | 1\.0 | 1\.0 | emits qty 1 |
| UP/UP/2 | 1\.0 | 0\.35 | emits qty 1 |
| UP/UP/5 | 1\.0 | 0\.0 / absent | blocked |
| UP/UP/25 | 0\.5 | 0\.0 / absent | blocked |
| UP/SW/2 | 0\.5 | 0\.35 | blocked: `int(1 × .5) == 0` |
| UP/SW/5 | 0\.5 | 0\.0 / absent | blocked |

Therefore:

- `SW/UP/2` is live as **1\.0**, not 0.35.
- `UP/UP/5` is gate-permitted at **1\.0** but is still blocked by the old table.
- Marginal cells return **0\.5** from the live gate, not 0.35.
- In the present one-share wet-run, a `0.5` multiplier cannot emit an order because integer conversion makes it zero.

### Last \~two weeks: persisted cash wet-run evidence

The database does not persist a dedicated `size_mult_applied` column. It does persist `order_pushed` / `regime_tf_excluded` inside `trade_signals.trade_data`, and actual orders in `order_master`. This is the reconciled CSV:

```
symbol,e_regime,a_regime,time_frame,size_mult_applied,emitted
hindalco_industries,UP,UP,2,1.0,Y
reliance,UP,SW,2,0.5,N
shree_cement,UP,SW,2,0.5,N
hero_motocorp,UP,SW,5,0.5,N
divis_laboratories,UP,UP,25,0.5,N
britannia_industries,UP,UP,25,0.5,N
```

Direct DB evidence:

- `hindalco_industries / UP-UP / TF 2`: `order_pushed=yes`, order `164514`, quantity `1`.
- All other rows: `regime_tf_excluded=yes`, no `order_master` row.

Conclusion: the active gate table is the source of the multiplier passed to the order bridge, but the legacy `REGIME_TF_SIZE_WEIGHT` is still a second veto source. The current live behavior is internally inconsistent and explains the disagreement.
