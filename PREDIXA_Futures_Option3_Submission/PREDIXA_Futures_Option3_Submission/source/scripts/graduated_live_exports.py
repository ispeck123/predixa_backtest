"""Strict normalized completed-trade export; never infer broker fills from OHLC.
CI critical value is explicit because the specification does not prescribe a
sampling model. Passing a normal 1.96 or Student-t value is an owner decision.
"""
import csv
from statistics import mean, stdev
from math import sqrt, isfinite
from scripts.futures_risk_engine import number

REQUIRED = ('segment', 'symbol', 'tf', 'side', 'zone_type', 'entry_price',
            'stoploss_price', 'target_price', 'real_entry_price', 'real_exit_price',
            'exit_leg', 'status')

class DataQualityFail(ValueError):
    pass

def tracking_error(row):
    try:
        if any(row.get(k) in (None, '') for k in REQUIRED):
            raise ValueError('Missing required completed-trade field')
        if row['segment'] not in ('CASH', 'FUTURES') or row['side'] != 'BUY':
            raise ValueError('Invalid segment or side')
        if row['status'] != 'COMPLETED' or row['exit_leg'] not in ('TARGET', 'STOP'):
            raise ValueError('Unapproved outcome/status')
        e, s, t, re, rx = [number(row[k], True) for k in
                          ('entry_price', 'stoploss_price', 'target_price', 'real_entry_price', 'real_exit_price')]
        # Applicable stop must be supplied explicitly, including unchanged stops.
        active = number(row['applicable_stop'], True)
        if not s < e < t or re == active:
            raise ValueError('Invalid signal/risk denominator')
        winner = row['exit_leg'] == 'TARGET'
        if (winner and rx <= re) or (not winner and rx >= re):
            raise ValueError('Outcome inconsistent with actual LONG P&L')
        live = abs(rx - re) / abs(re - active) * (1 if winner else -1)
        bt = abs(t - e) / abs(e - s) if winner else -1
        return float(live - bt)
    except (ValueError, KeyError, TypeError) as exc:
        raise DataQualityFail(f'DATA_QUALITY_FAIL: {exc}') from exc

def evaluate_tracks(rows, *, ci_critical=None):
    samples = {'CASH': [], 'FUTURES': []}
    errors = []
    for i, row in enumerate(rows):
        if row.get('status') == 'OPEN':
            continue
        try:
            samples[row.get('segment')].append(tracking_error(row))
        except (DataQualityFail, KeyError) as exc:
            errors.append(f'row {i}: DATA_QUALITY_FAIL: {exc}')
    result = {'errors': errors}
    for segment, values in samples.items():
        n = len(values)
        avg = mean(values) if values else None
        ci = None
        if n >= 2 and ci_critical is not None:
            critical = float(ci_critical(n))
            if not isfinite(critical) or critical <= 0:
                raise ValueError('Invalid CI critical value')
            width = critical * stdev(values) / sqrt(n)
            ci = [avg - width, avg + width]
        result[segment] = dict(n=n, mean=avg, ci95=ci,
            gross_pass=not errors and n >= 20 and avg > -0.15,
            full_pass=not errors and n >= 40 and ci is not None and ci[0] <= 0 <= ci[1] and avg > -0.15)
    return result

def export_completed(rows, path):
    completed = [dict(r) for r in rows if r.get('status') != 'OPEN']
    for row in completed:
        tracking_error(row)  # validate all before creating any output
    fields = list(REQUIRED) + sorted(set().union(*(set(r) for r in completed)) - set(REQUIRED))
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(completed)
