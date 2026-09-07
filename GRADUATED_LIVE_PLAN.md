# PREDIXA — Graduated-Live Validation Plan (cash + capped futures)
### Rev 1 · real capital · hand-hold spec — nothing here is optional or discretionary

This moves from wet-run (fidelity proven) to graduated-live, where validation continues
IN production at size proportional to confidence. Go-live is NOT gated on edge-proof (months);
it is gated on TRACKING-ERROR (live matches its own backtest expectation), which needs ~40
trades, not hundreds. Futures is a MECHANISM spot-check (not a volume lever — Rs50k can't fund
volume), throttled dynamically so wins keep it open and losses close the tap.

---
## 0. HARD NUMBERS (do not change without my sign-off)
- Total drawdown ceiling (cash+futures REALIZED loss): **Rs 50,000 → HALT ALL.** Unconditional.
- Futures realized-drawdown bucket: **Rs 35,000.**
- Cash: 1 share/trade (informational bucket ~Rs15k; 1-share risk ~Rs2-175/trade, never binds).
- Concurrent OPEN futures risk cap: **Rs 18,000.**

---
## 1. TWO TRACKS, TWO INDEPENDENT GATES (never pool them)
### Track A — CASH (continuous)
- 52-symbol allowlist (frozen), TFs {1,2,5,6,25}, LONG only, **1 share/trade.**
- Runs continuously. Time is the volume lever.
### Track B — FUTURES-BUY (dynamic-throttled)
- LONG only (futures BUY = +0.685R validated), **1 lot/trade.**
- Symbols: mechanically selected (see §4). NO human judgement.
- Governed by the dynamic throttle (§3).

**Cash and futures feed SEPARATE tracking-error gates. Do NOT combine — different edge
distributions (cash +0.384R, futures +0.685R) and different execution characteristics.**

---
## 2. TRACKING-ERROR GATE (computed per track, by us; BE supplies the data)
Per completed trade:
    live_R = win:  +|real_target - real_entry| / |real_entry - real_stop|
             loss: -|real_entry - real_stop_actual| / |real_entry - real_stop|   (~ -1.0)
    bt_R   = win:  +signalled RR ;  loss: -1.0
    track_err = live_R - bt_R
GATE (per track): PASS if 95% CI of mean(track_err) CONTAINS 0 AND mean(track_err) > -0.15R.
  n>=20 = gross-divergence check ; n>=40 = full gate.
BE provides per completed trade: symbol, TF, side, zone_type, signalled entry/stop/target,
real filled entry, real exit price, exit leg, win/loss. (We compute the gate.)

---
## 3. FUTURES DYNAMIC THROTTLE (state machine — evaluate BEFORE every futures fire)
Maintain two live state values:
  OPEN_FUT_RISK   = sum over all currently-open futures positions of |entry - stop| * lot_size
  REALIZED_FUT_DD = cumulative realized futures LOSS so far (rupees, losses positive)

Before firing a new futures entry, ALL must hold or SUPPRESS the futures signal:
  (a) OPEN_FUT_RISK + this_trade_risk <= Rs 18,000            [concurrent in-flight guard]
  (b) tap state by REALIZED_FUT_DD:
        REALIZED_FUT_DD <  Rs 15,000  -> allow up to 2 concurrent open lots
        Rs15k <= DD    <  Rs 25,000  -> allow 1 concurrent open lot
        REALIZED_FUT_DD >= Rs 30,000  -> TAP CLOSED: fire NO futures.
             re-open only when wins recover REALIZED_FUT_DD back below Rs 22,000.
  (c) this_trade_risk itself must be <= Rs 14,000 (reject any lot exceeding it)

WINS keep the tap open (they reduce REALIZED_FUT_DD). LOSSES tighten/close it. This makes the
number of futures trades OUTCOME-DETERMINED, not pre-set. Cash is UNAFFECTED by the futures tap.

HARD CEILING (independent, dumb, unconditional): if (cash realized loss + futures realized loss)
>= Rs 50,000 -> HALT ALL TRADING (both tracks). Overrides everything. This is a redundant
backstop — §3(a)+(b) already keep the smart layer under ceiling (30k tap-close + 18k open = 48k).

---
## 4. FUTURES SYMBOL SELECTION (100% mechanical — no judgement, no B/C scrips)
From OUR VALIDATED futures universe ONLY (the liquid large-caps the +0.685R edge was proven on —
NOT the full F&O list, NOT penny/distressed names like IDEA/YESBANK).
  per_lot_risk = current_futures_price * lot_size * median_stop_pct/100
  - median_stop_pct: WE supply per symbol (from the futures backtest). Provided separately.
  - current price + lot_size: BE supplies live from Fyers/NSE master.
Rank ascending by per_lot_risk; take the LOWEST 6-8 that also pass a liquidity floor
(min futures OI / avg daily volume — BE's standard liquidity check).
Illustrative low-risk validated candidates (confirm live): NTPC, ASHOKLEY, BANKINDIA, MOTHERSON,
IOC, TATASTEEL, ONGC, BEL, WIPRO, BANKBARODA. FINAL list = lowest measured per_lot_risk.
Reject any name not in the validated universe regardless of how cheap its lot is.

---
## 5. SIZE SCHEDULE (per track, independent; advance only when THAT track's gate passes)
  Tier 0 (now): cash 1 share ; futures 1 lot (throttled). Go live, accrue data.
  Tier 1: after that track's 20 completions with NO gross divergence (mean track_err > -0.15R).
  Tier 2: after that track's 40 completions with FULL gate pass (CI contains 0, mean > -0.15R).
  Each size increase REQUIRES the gate still passing at the larger sample; divergence -> freeze/step down.

---
## 6. FUTURES POST-SEP BRANCH (no cliff)
At 30 Sep: review futures track_err.
  - passing + tap not closed -> continue, consider lifting toward Tier 2.
  - failing OR tap closed -> HALT futures, continue cash. Investigate before any resumption.

---
## 7. WHAT BE BUILDS
  1. Two-track runner: cash (52 sym, existing) + futures-BUY (selected sym, 1 lot).
  2. Futures selection query (§4): per_lot_risk rank within validated universe + liquidity floor.
  3. Dynamic throttle state machine (§3): OPEN_FUT_RISK + REALIZED_FUT_DD, evaluated before each
     futures fire; suppress + log reason when a rule blocks.
  4. Hard ceiling monitor (§3): halt-all at Rs50k combined realized loss.
  5. Per-completed-trade export (§2 fields) for both tracks -> we compute the gates.
  All existing controls remain: consumed-zone dedup, freshness gate, circuit-limit gate, OCO logging.

## 8. GO / NO-GO before starting
  - Rs50k hard ceiling monitor live and tested (simulate a breach -> confirm halt).
  - Futures throttle state machine tested (simulate DD tiers -> confirm tap tightens/closes).
  - Futures symbols selected mechanically + liquidity-checked + resolve in Fyers master.
  - Per-trade export emitting all §2 fields for both tracks.
  - If ANY red -> do not start futures; cash-only is fine to start alone.
