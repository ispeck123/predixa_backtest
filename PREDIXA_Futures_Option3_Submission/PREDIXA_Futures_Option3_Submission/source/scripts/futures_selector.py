"""Mechanical, data-driven futures candidate selection.

The function accepts normalized snapshots so it can be tested without a broker
or a network call.  It never discovers symbols outside the supplied validated
universe and emits both ranking risk and broker one-lot margin metrics.
"""
from __future__ import annotations

import csv
import logging
from datetime import date

from scripts.futures_risk_engine import number, units

logger = logging.getLogger("stock_project_logger")


def _metric(value):
    return number(value) if value is not None else None


def select_futures(validated_universe, candidates, master, *, count=None,
                   liquidity_floor=None, as_of, master_as_of, market_as_of,
                   selection_mode=None, candidate_pool_size=None,
                   liquidity_metric=None, liquidity_lookback=None,
                   require_token=False):
    """Return full audit rows plus selected approved rows.

    ``count``/``liquidity_floor`` preserve the first-pass API.  In production,
    use the explicit Option-3 policy fields and inspect ``policy_ready`` before
    persisting ``approved``.
    """
    pool = candidate_pool_size if candidate_pool_size is not None else count
    if type(pool) is not int or pool not in (6, 8):
        raise ValueError("Configure deterministic candidate pool size 6 or 8")
    if as_of != master_as_of or as_of != market_as_of:
        raise ValueError("Current dated master and market snapshot required")
    date.fromisoformat(as_of)
    floor = _metric(liquidity_floor)
    policy_errors = []
    if selection_mode not in (None, "PER_LOT_RISK", "LOWEST_MARGIN"):
        policy_errors.append("OPTION3_SELECTION_MODE")
    if selection_mode is None:
        policy_errors.append("OPTION3_SELECTION_MODE")
    if floor is None:
        policy_errors.append("OPTION3_LIQUIDITY_THRESHOLD")
    elif floor < 0:
        raise ValueError("Invalid liquidity floor")
    if not liquidity_metric:
        policy_errors.append("OPTION3_LIQUIDITY_METRIC")
    if not liquidity_lookback:
        policy_errors.append("OPTION3_LIQUIDITY_LOOKBACK")

    universe = set(validated_universe)
    rows, seen = [], set()
    for candidate in candidates:
        symbol = candidate.get("symbol")
        if symbol in seen:
            raise ValueError("Ambiguous duplicate symbol/contract selection input")
        seen.add(symbol)
        row = dict(candidate, as_of=as_of, selected=False, selection_rank=None,
                   fyers_resolved=False, liquidity_pass=False,
                   ranking_per_lot_risk=None, current_one_lot_margin_required=None,
                   reason=None)
        try:
            if symbol not in universe:
                raise ValueError("SYMBOL_OUTSIDE_VALIDATED_UNIVERSE")
            contract = candidate.get("contract")
            master_row = master.get(contract)
            if not master_row or master_row.get("symbol") != symbol:
                raise ValueError("FUT_REJECT_SYMBOL_UNRESOLVED")
            if master_row.get("instrument_type") != "FUTURES" or master_row.get("segment") not in (None, "NSEFO", "NSE_FO"):
                raise ValueError("FUT_REJECT_SYMBOL_UNRESOLVED")
            if master_row.get("active") is False:
                raise ValueError("FUT_REJECT_SYMBOL_UNRESOLVED")
            if date.fromisoformat(master_row["expiry"]) < date.fromisoformat(as_of):
                raise ValueError("EXPIRED_CONTRACT")
            lot = int(units(master_row["lot_size"]))
            price = number(candidate["futures_price"], True)
            median = number(candidate["median_stop_pct"], True)
            liquidity = number(candidate["liquidity_metric"])
            margin = candidate.get("current_one_lot_margin_required", master_row.get("current_one_lot_margin_required"))
            margin_value = _metric(margin)
            row.update(
                underlying=master_row.get("underlying", symbol),
                expiry=master_row["expiry"], lot_size=lot,
                token=master_row.get("token"), fyers_symbol=master_row.get("fyers_symbol", contract),
                fyers_resolved=bool((master_row.get("token") or not require_token) and master_row.get("active", True)),
                liquidity_metric=liquidity, liquidity_pass=floor is not None and liquidity >= floor,
                current_one_lot_margin_required=str(margin_value) if margin_value is not None else None,
                ranking_per_lot_risk=str(price * lot * median / 100),
            )
            if not row["fyers_resolved"]:
                raise ValueError("FUT_REJECT_SYMBOL_UNRESOLVED")
            if not row["liquidity_pass"]:
                raise ValueError("LIQUIDITY_FAILURE")
            if selection_mode == "LOWEST_MARGIN" and margin_value is None:
                raise ValueError("MARGIN_REQUIRED_FOR_SELECTION")
        except (ValueError, KeyError, TypeError) as exc:
            row["reason"] = str(exc)
        rows.append(row)

    for missing in sorted(universe - seen):
        rows.append(dict(symbol=missing, selected=False, selection_rank=None,
                         reason="MISSING_SELECTION_INPUT", contract=None, expiry=None,
                         lot_size=None, futures_price=None, median_stop_pct=None,
                         ranking_per_lot_risk=None, current_one_lot_margin_required=None,
                         liquidity_metric=None, liquidity_pass=False, fyers_resolved=False,
                         as_of=as_of))

    eligible = [row for row in rows if row.get("reason") is None]
    if selection_mode == "LOWEST_MARGIN":
        eligible.sort(key=lambda row: (number(row["current_one_lot_margin_required"]), row["symbol"], row["contract"]))
    else:
        eligible.sort(key=lambda row: (number(row["ranking_per_lot_risk"]), row["symbol"], row["contract"]))
    for rank, row in enumerate(eligible, 1):
        row["selection_rank"] = rank
        row["selected"] = rank <= pool

    data_pass = len(eligible) >= pool
    policy_ready = data_pass and not policy_errors and selection_mode in ("PER_LOT_RISK", "LOWEST_MARGIN")
    if not data_pass:
        for row in rows:
            row["selected"] = False
    for row in rows:
        logger.info("graduated_live_selection %s", row)
    approved = {row["symbol"]: row for row in rows if row.get("selected")}
    return {
        "passed": data_pass, "policy_ready": policy_ready,
        "policy_errors": policy_errors, "selection_mode": selection_mode,
        "candidate_pool_size": pool, "as_of": as_of, "rows": rows,
        "approved": approved if policy_ready else approved,
    }


def write_selection_reports(result, selection_path, resolution_path):
    """Write deterministic analytical and master-resolution reports."""
    rows = result.get("rows", [])
    fields = [
        "symbol", "contract", "expiry", "lot_size", "token", "futures_symbol",
        "futures_price", "median_stop_pct", "ranking_per_lot_risk",
        "current_one_lot_margin_required", "liquidity_metric", "liquidity_pass",
        "selection_rank", "selected", "fyers_resolved", "reason", "as_of",
    ]
    for path in (selection_path, resolution_path):
        with open(path, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
