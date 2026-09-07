#!/usr/bin/env python3
"""
PREDIXA tracking-error gate — computes, PER TRACK (cash / futures separately), whether live
execution matches its backtest expectation. This is the go/no-go for each size tier.

track_err = live_R - bt_R   per completed trade
GATE: PASS if 95% CI of mean(track_err) contains 0 AND mean(track_err) > -0.15R
      n>=20 gross-divergence check ; n>=40 full gate.

NEVER pool cash and futures — run this once per track. Input: completed-trade CSV with columns:
  segment(cash/futures), symbol, tf, side, zone_type,
  entry_price, stoploss_price, target_price,          # signalled -> bt_R
  real_entry_price, real_exit_price, exit_leg, status # realized  -> live_R
"""
import csv, sys, math

TOLERANCE = -0.15   # mean track_err must exceed this (structural-divergence floor)
N_GROSS   = 20      # gross-divergence checkpoint
N_FULL    = 40      # full statistical gate

def f(x):
    try: return float(x)
    except: return None

def live_R(r):
    e=f(r.get('real_entry_price')) or f(r.get('entry_price'))
    s=f(r.get('stoploss_price')); 
    if e is None or s is None or abs(e-s)<1e-9: return None
    risk=abs(e-s)
    st=str(r.get('status','')).lower()
    if 'success' in st or r.get('exit_leg','').upper()=='TARGET':
        xt=f(r.get('real_exit_price')) or f(r.get('target_price'))
        return abs(xt-e)/risk if xt is not None else None
    else:
        xt=f(r.get('real_exit_price'))
        return -abs(e-xt)/risk if xt is not None else -1.0

def bt_R(r):
    e=f(r.get('entry_price')); s=f(r.get('stoploss_price')); t=f(r.get('target_price'))
    if None in (e,s,t) or abs(e-s)<1e-9: return None
    st=str(r.get('status','')).lower()
    return abs(t-e)/abs(e-s) if ('success' in st or r.get('exit_leg','').upper()=='TARGET') else -1.0

def gate(rows, label):
    errs=[]
    for r in rows:
        lr=live_R(r); br=bt_R(r)
        if lr is None or br is None: continue
        errs.append(lr-br)
    n=len(errs)
    print(f"\n=== {label} — tracking-error gate ===")
    print(f"  completed (usable): {n}")
    if n==0: print("  no data yet."); return
    mean=sum(errs)/n
    if n>1:
        var=sum((x-mean)**2 for x in errs)/(n-1); se=math.sqrt(var/n); ci=1.96*se
    else:
        ci=float('inf')
    lo,hi=mean-ci,mean+ci
    print(f"  mean(track_err) = {mean:+.3f}R   95% CI [{lo:+.3f}, {hi:+.3f}]")
    print(f"  live vs backtest: {'live BELOW backtest' if mean<0 else 'live AT/ABOVE backtest'}")
    ci_contains_0 = (lo<=0<=hi)
    tol_ok = mean>TOLERANCE
    if n<N_GROSS:
        print(f"  STATUS: pre-checkpoint (n<{N_GROSS}). Accrue more before any size increase.")
    elif n<N_FULL:
        verdict='PASS (gross)' if (tol_ok and mean>TOLERANCE) else 'FAIL (gross divergence)'
        print(f"  GROSS-DIVERGENCE CHECK (n>={N_GROSS}): {verdict}")
        print(f"    -> {'Tier-1 size step permitted' if 'PASS' in verdict else 'FREEZE — investigate'}")
    else:
        verdict='PASS' if (ci_contains_0 and tol_ok) else 'FAIL'
        print(f"  FULL GATE (n>={N_FULL}): {verdict}  (CI contains 0: {ci_contains_0}; mean>{TOLERANCE}: {tol_ok})")
        print(f"    -> {'Tier-2 size step permitted' if verdict=='PASS' else 'FREEZE/STEP-DOWN — investigate divergence'}")

def main():
    if len(sys.argv)<2:
        print("usage: tracking_error_gate.py completed_trades.csv"); return
    rows=list(csv.DictReader(open(sys.argv[1])))
    cash=[r for r in rows if 'cash' in str(r.get('segment','')).lower()]
    fut =[r for r in rows if 'fut'  in str(r.get('segment','')).lower()]
    if not cash and not fut:  # no segment column -> treat all as one, warn
        print("WARN: no 'segment' column — running as single pool (should be split cash/futures!)")
        gate(rows,"ALL (unsplit — FIX: add segment column)")
    else:
        gate(cash,"CASH")
        gate(fut,"FUTURES")
    print("\nReminder: cash and futures gates are INDEPENDENT. Each earns its own size tier.")

if __name__=='__main__': main()
