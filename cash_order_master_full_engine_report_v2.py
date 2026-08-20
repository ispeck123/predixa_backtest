#!/usr/bin/env python
"""
Cash order_master engine-rerun report exporter.

Purpose
-------
Reads rows from order_master, then reruns the CASH setup engine for each row using:
    stock_tick + time_frame + purchased_cmp_date/purchased_on

It exports:
1) completed_order_execution_log.csv
2) setup_to_fill_funnel.csv
3) engine_rerun_payloads.jsonl
4) all_orders_engine_enriched.csv
5) summary.json

This is for CASH/order_master. It does not require futures_selected_zone_fields.jsonl.

Run from project root:
    python backtest/export_cash_order_master_engine_rerun_report.py --progress

Typical wet-run date filter:
    python backtest/export_cash_order_master_engine_rerun_report.py \
      --purchased-from "2026-08-20 09:00:00" \
      --purchased-to "2026-08-20 15:30:00" \
      --progress
"""

# v2 single pipeline: engine rerun + zone extraction + report generation.
# MFE/MAE replay columns are populated during evaluation replay stage.

from __future__ import annotations

import argparse
import ast
import csv
import json
import math
import os
import re
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from sqlalchemy import text

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from shared.db.dbconn import DBConnection
from scripts.setup_engine_new import process_setup, format_calculate_setup_response


# ----------------------------------------------------------------------
# CASH timeframe config
# ----------------------------------------------------------------------
# The default report filters the user's wet-run cash TFs {1,2,5,6,25}.
# TF=3 is included in the map so the script works if you pass --timeframes 3.
# ----------------------------------------------------------------------

CASH_STACKS_BY_TF: Dict[int, Tuple[str, List[str]]] = {
    1: ("M-W-D", ["monthly", "weekly", "daily"]),
    2: ("W-D-60", ["weekly", "daily", "sixty"]),
    3: ("D-60-15", ["daily", "sixty", "fifteen"]),
    5: ("W-D-125", ["weekly", "daily", "one_twenty_five"]),
    6: ("D-125-25", ["daily", "one_twenty_five", "twenty_five"]),
    25: ("W-D-75", ["weekly", "daily", "seventy_five"]),
}

DEFAULT_TIMEFRAMES = [1, 2, 5, 6, 25]
DEFAULT_MIN_RR = 2.1


# ----------------------------------------------------------------------
# Dataclasses
# ----------------------------------------------------------------------

@dataclass(frozen=True)
class OrderRow:
    order_id: int
    trade_signal_id: Optional[int]
    stock_tick: str
    stock_id: Optional[int]
    country_id: Optional[int]
    time_frame: int
    order_type: str
    entry_price: Optional[float]
    stoploss_price: Optional[float]
    target_price: Optional[float]
    stock_quantity: Optional[float]
    purchased_cmp_date: Optional[str]
    purchased_on: Optional[Any]
    order_status: Optional[str]
    is_trade_started: Optional[bool]
    is_evaluated: Optional[bool]
    entry_timestamp: Optional[Any]
    completed_on: Optional[Any]
    raw: Dict[str, Any]


# ----------------------------------------------------------------------
# Generic helpers
# ----------------------------------------------------------------------

def now_label() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def safe_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except Exception:
        return None


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        number = float(value)
        if not math.isfinite(number):
            return None
        return number
    except Exception:
        return None


def parse_bool(value: Any) -> Optional[bool]:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text_value = str(value).strip().lower()
    if text_value in {"1", "true", "yes", "y"}:
        return True
    if text_value in {"0", "false", "no", "n"}:
        return False
    return None


def parse_status(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)

    text_value = str(value).strip()
    if not text_value:
        return {}

    if text_value.startswith("{") and text_value.endswith("}"):
        try:
            parsed = ast.literal_eval(text_value)
            if isinstance(parsed, Mapping):
                return dict(parsed)
        except Exception:
            pass
        try:
            parsed = json.loads(text_value)
            if isinstance(parsed, Mapping):
                return dict(parsed)
        except Exception:
            pass

    return {"raw_order_status": text_value}


def status_text(status: Mapping[str, Any]) -> str:
    return str(status.get("status") or status.get("raw_order_status") or "").lower()


def parse_datetime_value(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.replace(second=0, microsecond=0)

    text_value = str(value).strip()
    if not text_value:
        return None

    try:
        from dateutil import parser as dt_parser
        return dt_parser.parse(text_value).replace(second=0, microsecond=0)
    except Exception:
        pass

    for fmt in (
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
    ):
        try:
            return datetime.strptime(text_value[:19], fmt).replace(second=0, microsecond=0)
        except Exception:
            pass

    return None


def datetime_text(value: Any) -> Optional[str]:
    dt = parse_datetime_value(value)
    if dt is not None:
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def scan_timestamp_for_order(row: OrderRow) -> Optional[datetime]:
    # Prefer purchased_cmp_date because that is usually the exact candle cut-off used for emission.
    return parse_datetime_value(row.purchased_cmp_date) or parse_datetime_value(row.purchased_on)


def normalize_symbol(value: Any, lowercase: bool = True) -> str:
    text_value = str(value or "").strip()
    return text_value.lower() if lowercase else text_value


def normalize_trade_type(value: Any) -> str:
    text_value = str(value or "").strip().upper()
    if text_value in {"LONG", "BUY", "BZ", "GDZ"}:
        return "BUY"
    if text_value in {"SHORT", "SELL", "SZ", "GSZ"}:
        return "SELL"
    return text_value


def json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=json_default, separators=(",", ":"))


def to_builtin(value: Any, max_string: Optional[int] = None) -> Any:
    if isinstance(value, Mapping):
        return {str(k): to_builtin(v, max_string=max_string) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_builtin(v, max_string=max_string) for v in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, (str, int, float, bool)) or value is None:
        if isinstance(value, str) and max_string is not None and len(value) > max_string:
            return value[:max_string] + "...<truncated>"
        return value
    text_value = str(value)
    if max_string is not None and len(text_value) > max_string:
        return text_value[:max_string] + "...<truncated>"
    return text_value


def parse_csv_int_list(value: Optional[str], default: Optional[List[int]] = None) -> List[int]:
    if value is None or str(value).strip() == "":
        return list(default or [])
    return [int(item.strip()) for item in str(value).split(",") if item.strip()]


def parse_csv_str_list(value: Optional[str]) -> List[str]:
    if value is None or str(value).strip() == "":
        return []
    return [str(item).strip().lower() for item in str(value).split(",") if str(item).strip()]


# ----------------------------------------------------------------------
# Engine setup extraction
# ----------------------------------------------------------------------

def price_dict_for_side(formatted: Mapping[str, Any], side: str) -> Optional[Dict[str, float]]:
    payload = formatted.get(side)
    if not isinstance(payload, Mapping):
        return None

    entry = safe_float(payload.get("entry_price") or payload.get("entry"))
    stop = safe_float(payload.get("stop_loss") or payload.get("stoploss_price") or payload.get("stop"))
    target = safe_float(payload.get("target_price") or payload.get("target"))

    if entry is None or stop is None or target is None:
        return None

    return {
        "entry_price": entry,
        "stop_loss": stop,
        "target_price": target,
    }


def extract_valid_engine_setups(formatted: Mapping[str, Any], min_rr: float, include_sell: bool) -> List[Dict[str, Any]]:
    setups: List[Dict[str, Any]] = []

    buy_rrr = safe_float(formatted.get("BUY_RRR"))
    buy_prices = price_dict_for_side(formatted, "BUY")
    if buy_prices is not None and buy_rrr is not None and buy_rrr >= min_rr:
        setups.append({"trade_type": "BUY", "rrr": buy_rrr, "trade_dict": buy_prices})

    sell_rrr = safe_float(formatted.get("SELL_RRR"))
    sell_prices = price_dict_for_side(formatted, "SELL")
    if include_sell and sell_prices is not None and sell_rrr is not None and sell_rrr >= min_rr:
        setups.append({"trade_type": "SELL", "rrr": sell_rrr, "trade_dict": sell_prices})

    return setups


def price_match(actual: Optional[float], expected: Optional[float], abs_tol: float, rel_tol: float) -> bool:
    if actual is None or expected is None:
        return False
    return abs(actual - expected) <= max(abs_tol, abs(expected) * rel_tol)


def choose_order_matching_setup(
    setups: List[Dict[str, Any]],
    order: OrderRow,
    abs_tol: float,
    rel_tol: float,
) -> Tuple[Optional[Dict[str, Any]], str]:
    side = normalize_trade_type(order.order_type)
    same_side = [s for s in setups if normalize_trade_type(s.get("trade_type")) == side]

    if not same_side:
        return None, "NO_SAME_SIDE_SETUP"

    exact: List[Tuple[float, Dict[str, Any]]] = []
    for setup in same_side:
        td = setup.get("trade_dict") or {}
        entry = safe_float(td.get("entry_price"))
        stop = safe_float(td.get("stop_loss") or td.get("stoploss_price"))
        target = safe_float(td.get("target_price"))
        if (
            price_match(entry, order.entry_price, abs_tol, rel_tol)
            and price_match(stop, order.stoploss_price, abs_tol, rel_tol)
            and price_match(target, order.target_price, abs_tol, rel_tol)
        ):
            score = (
                abs((entry or 0) - (order.entry_price or 0))
                + abs((stop or 0) - (order.stoploss_price or 0))
                + abs((target or 0) - (order.target_price or 0))
            )
            exact.append((score, setup))

    if exact:
        exact.sort(key=lambda item: item[0])
        return exact[0][1], "PRICE_MATCH"

    # fallback: if prices changed because of rounding or engine adjustment, use same-side first setup
    return same_side[0], "SAME_SIDE_FALLBACK"


# ----------------------------------------------------------------------
# Selected zone extraction from raw payload
# ----------------------------------------------------------------------

def enum_or_text(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "value"):
        return value.value
    text_value = str(value)
    match = re.search(r": '([^']+)'", text_value)
    if match:
        return match.group(1)
    return text_value


def get_attr_or_key(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


def parse_zone_id(value: Any) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    if not value:
        return None, None, None
    text_value = str(value)
    # examples: TEST_X_GDZ_644, UNIONBANK_X_GDZ_644
    match = re.search(r"_([EAX])_(BZ|SZ|GDZ|GSZ)_(\d+)$", text_value)
    if not match:
        return None, None, None
    return match.group(1), match.group(2), int(match.group(3))


def selected_cascade_row(payload: Any, order_type: str) -> Dict[str, Any]:
    if not isinstance(payload, Mapping):
        return {}

    cascade = payload.get("setup_cascade_log") or []
    if not isinstance(cascade, list):
        return {}

    want_direction = "LONG" if normalize_trade_type(order_type) == "BUY" else "SHORT"
    selected_rows = []

    for item in cascade:
        if not isinstance(item, Mapping):
            continue
        direction = str(item.get("direction") or "").upper()
        if direction and direction != want_direction:
            continue
        if item.get("selected") is True:
            selected_rows.append(item)

    if selected_rows:
        selected_rows.sort(key=lambda x: safe_int(x.get("rank")) or 999999)
        return dict(selected_rows[0])

    # fallback: rank 1 of the matching direction
    same_direction = [
        dict(item) for item in cascade
        if isinstance(item, Mapping)
        and str(item.get("direction") or "").upper() in {want_direction, ""}
    ]
    if same_direction:
        same_direction.sort(key=lambda x: safe_int(x.get("rank")) or 999999)
        return same_direction[0]

    return {}


def best_setup_zone_id(payload: Any, order_type: str) -> Optional[str]:
    if not isinstance(payload, Mapping):
        return None

    key = "best_setup_long" if normalize_trade_type(order_type) == "BUY" else "best_setup_short"
    best = payload.get(key)
    if best is None:
        return None

    if isinstance(best, Mapping):
        return best.get("zone_id")

    if hasattr(best, "zone_id"):
        return getattr(best, "zone_id")

    text_value = str(best)
    match = re.search(r"zone_id=['\"]([^'\"]+)['\"]", text_value)
    if match:
        return match.group(1)

    return None


def extract_selected_zone_id(payload: Any, order_type: str) -> Tuple[Optional[str], Dict[str, Any], str]:
    row = selected_cascade_row(payload, order_type)
    if row.get("zone_id"):
        return str(row["zone_id"]), row, "setup_cascade_log"

    zone_id = best_setup_zone_id(payload, order_type)
    if zone_id:
        return str(zone_id), {}, "best_setup"

    return None, {}, "NOT_FOUND"


def parse_zone_string(text_value: str) -> Dict[str, Any]:
    """Parse required fields from Zone(...) string."""
    output: Dict[str, Any] = {"_raw_zone_string": text_value}

    # enum format: ztype=<ZoneType.GDZ: 'GDZ'>
    enum_patterns = {
        "tf": r"tf=<TF\.([A-Z]+): '([^']+)'>",
        "ztype": r"ztype=<ZoneType\.([A-Z]+): '([^']+)'>",
        "state": r"state=<ZoneState\.([A-Z]+): '([^']+)'>",
        "nesting_tier": r"nesting_tier=<ZoneNestingTier\.([A-Z0-9_]+): '([^']+)'>",
    }
    for field, pattern in enum_patterns.items():
        match = re.search(pattern, text_value)
        if match:
            output[field] = match.group(2)

    # plain string format fallback: ztype='GDZ'
    for field in ("tf", "ztype", "state", "quality_priority", "age_class", "nesting_tier"):
        if field not in output:
            match = re.search(rf"{field}=['\"]([^'\"]+)['\"]", text_value)
            if match:
                output[field] = match.group(1)

    numeric_fields = [
        "distal", "proximal", "created_idx", "base_start", "base_end",
        "penetration_pct", "retest_count", "gap_composite_score", "final_score",
        "entry_price", "target_price", "stop_price", "rr_ratio", "zone_v38_score",
        "overlap_ratio", "htf_target_price", "raw_score", "age_penalty",
        "approach_penalty", "penetration_penalty", "age_bars",
    ]

    for field in numeric_fields:
        # Stop at comma or close paren to avoid nested zone's values being read first.
        match = re.search(rf"{field}=([-+]?\d+(?:\.\d+)?|None)", text_value)
        if not match:
            continue
        raw = match.group(1)
        if raw == "None":
            output[field] = None
        elif field in {"created_idx", "base_start", "base_end", "retest_count", "age_bars"}:
            output[field] = int(float(raw))
        else:
            output[field] = float(raw)

    bool_fields = [
        "session_accepted", "gap_is_structural", "gap_is_mechanical", "pattern_validated",
        "entry_path_clear", "zone_in_zone", "invalidated", "violated_by_close",
    ]
    for field in bool_fields:
        match = re.search(rf"{field}=(True|False|None)", text_value)
        if match:
            output[field] = None if match.group(1) == "None" else match.group(1) == "True"

    return output


def zone_object_to_fields(zone: Any) -> Dict[str, Any]:
    if isinstance(zone, str):
        return parse_zone_string(zone)

    if isinstance(zone, Mapping) and "meta" in zone:
        meta = zone.get("meta") or {}
        if isinstance(meta, Mapping):
            return dict(meta)

    output: Dict[str, Any] = {}
    for field in (
        "symbol", "tf", "ztype", "distal", "proximal", "created_idx", "base_start", "base_end",
        "penetration_pct", "retest_count", "state", "gap_composite_score", "gap_is_structural",
        "gap_is_mechanical", "final_score", "quality_priority", "age_class", "entry_price",
        "target_price", "stop_price", "rr_ratio", "nesting_tier", "zone_v38_score",
        "overlap_ratio", "htf_target_price", "session_accepted", "pattern_validated",
        "entry_path_clear", "zone_in_zone", "age_bars",
    ):
        value = get_attr_or_key(zone, field, None)
        if value is not None:
            output[field] = enum_or_text(value)
    return output


def iter_payload_zones(payload: Any) -> Iterable[Tuple[str, Dict[str, Any]]]:
    if not isinstance(payload, Mapping):
        return

    for tf in ("X", "A", "E"):
        key = f"zones_{tf}"
        zones = payload.get(key)
        if isinstance(zones, list):
            for zone in zones:
                yield key, zone_object_to_fields(zone)

    # formatted shape: ZONES_X -> Buy/Sell -> [{meta: {...}}]
    for tf in ("X", "A", "E"):
        key = f"ZONES_{tf}"
        obj = payload.get(key)
        if isinstance(obj, Mapping):
            for side in ("Buy", "Sell"):
                zones = obj.get(side) or []
                if isinstance(zones, list):
                    for zone in zones:
                        yield key, zone_object_to_fields(zone)


def calc_entry_tertile(entry_price: Any, proximal: Any, distal: Any) -> Optional[int]:
    entry = safe_float(entry_price)
    prox = safe_float(proximal)
    dist = safe_float(distal)
    if entry is None or prox is None or dist is None:
        return None
    width = abs(prox - dist)
    if width == 0:
        return None

    # 1 = proximal, 2 = middle, 3 = distal
    depth_pct = abs(entry - prox) / width * 100.0
    depth_pct = max(0.0, min(100.0, depth_pct))
    if depth_pct <= 33.3333:
        return 1
    if depth_pct <= 66.6666:
        return 2
    return 3


def freshness_limit(zone_type: Any) -> Optional[int]:
    z = str(zone_type or "").upper()
    if z in {"BZ", "SZ", "GSZ"}:
        return 0
    if z == "GDZ":
        return 1
    return None


def freshness_status(zone_type: Any, retest_count: Any) -> str:
    limit = freshness_limit(zone_type)
    retest = safe_int(retest_count)
    if limit is None or retest is None:
        return "UNKNOWN"
    return "FRESH_PASS" if retest <= limit else "NON_FRESH_BLOCK_BY_G4"


def extract_selected_zone_fields(raw_payload: Any, formatted_payload: Any, order_type: str, entry_price: Any) -> Dict[str, Any]:
    zone_id, cascade_row, source = extract_selected_zone_id(raw_payload, order_type)
    if zone_id is None:
        # formatted payload normally does not carry setup_cascade_log, but try anyway
        zone_id, cascade_row, source = extract_selected_zone_id(formatted_payload, order_type)

    tf, ztype, created_idx = parse_zone_id(zone_id)
    selected_zone: Dict[str, Any] = {}
    selected_zone_source = None

    # Prefer raw payload because raw Zone(...) has retest_count and penetration_pct.
    for payload_name, payload in (("raw_setup_payload", raw_payload), ("formatted_setup_payload", formatted_payload)):
        for zone_key, zone_fields in iter_payload_zones(payload) or []:
            zone_created_idx = safe_int(zone_fields.get("created_idx"))
            zone_ztype = str(zone_fields.get("ztype") or "").upper()
            zone_tf = str(zone_fields.get("tf") or "").upper()

            if created_idx is not None and zone_created_idx != created_idx:
                continue
            if ztype and zone_ztype and zone_ztype != ztype:
                continue
            if tf and zone_tf and zone_tf != tf:
                continue

            selected_zone = zone_fields
            selected_zone_source = f"{payload_name}.{zone_key}"
            break
        if selected_zone:
            break

    zone_type = (
        selected_zone.get("ztype")
        or ztype
        or selected_zone.get("zone_type")
    )
    retest_count = selected_zone.get("retest_count")
    penetration_pct = selected_zone.get("penetration_pct")
    proximal = selected_zone.get("proximal")
    distal = selected_zone.get("distal")
    entry_tertile = calc_entry_tertile(entry_price, proximal, distal)
    fresh_status = freshness_status(zone_type, retest_count)

    return {
        "selected_zone_id": zone_id,
        "selected_zone_found": bool(selected_zone),
        "selected_zone_id_source": source,
        "selected_zone_source": selected_zone_source,
        "cascade_rank": cascade_row.get("rank") if isinstance(cascade_row, Mapping) else None,
        "cascade_weighted_score": cascade_row.get("weighted_score") if isinstance(cascade_row, Mapping) else None,
        "zone_type": zone_type,
        "retest_count": retest_count,
        "penetration_pct": penetration_pct,
        "entry_tertile": entry_tertile,
        "proximal": proximal,
        "distal": distal,
        "freshness_limit": freshness_limit(zone_type),
        "freshness_status_at_emission": fresh_status,
        "freshness_suppressed_by_g4": fresh_status == "NON_FRESH_BLOCK_BY_G4",
        "zone_state": selected_zone.get("state"),
        "quality_priority": selected_zone.get("quality_priority"),
        "age_class": selected_zone.get("age_class"),
        "zone_v38_score": selected_zone.get("zone_v38_score"),
        "gap_composite_score": selected_zone.get("gap_composite_score"),
        "overlap_ratio": selected_zone.get("overlap_ratio"),
        "htf_target_price": selected_zone.get("htf_target_price"),
        "selected_zone_raw": selected_zone,
    }


def contains_circuit_block(payloads: Iterable[Any]) -> Tuple[bool, str]:
    patterns = ("circuit", "lower_ckt", "upper_ckt", "lower circuit", "upper circuit")
    for payload in payloads:
        text_value = str(payload).lower()
        for p in patterns:
            if p in text_value:
                return True, p
    return False, ""


# ----------------------------------------------------------------------
# DB reader
# ----------------------------------------------------------------------

def build_where(args: argparse.Namespace) -> Tuple[str, Dict[str, Any]]:
    clauses: List[str] = []
    params: Dict[str, Any] = {}

    timeframes = parse_csv_int_list(args.timeframes, DEFAULT_TIMEFRAMES)
    if timeframes:
        placeholders = []
        for idx, tf in enumerate(timeframes):
            key = f"tf_{idx}"
            placeholders.append(f":{key}")
            params[key] = tf
        clauses.append(f"time_frame IN ({', '.join(placeholders)})")

    symbols = parse_csv_str_list(args.symbols)
    if symbols:
        placeholders = []
        for idx, sym in enumerate(symbols):
            key = f"sym_{idx}"
            placeholders.append(f":{key}")
            params[key] = sym
        clauses.append(f"LOWER(stock_tick) IN ({', '.join(placeholders)})")

    if args.order_id is not None:
        clauses.append("order_id = :order_id")
        params["order_id"] = int(args.order_id)

    if args.purchased_from:
        clauses.append("purchased_on >= :purchased_from")
        params["purchased_from"] = args.purchased_from

    if args.purchased_to:
        clauses.append("purchased_on <= :purchased_to")
        params["purchased_to"] = args.purchased_to

    if args.completed_from:
        clauses.append("completed_on >= :completed_from")
        params["completed_from"] = args.completed_from

    if args.completed_to:
        clauses.append("completed_on <= :completed_to")
        params["completed_to"] = args.completed_to

    where_sql = " WHERE " + " AND ".join(clauses) if clauses else ""
    return where_sql, params


def row_to_order(row: Mapping[str, Any]) -> OrderRow:
    return OrderRow(
        order_id=int(row["order_id"]),
        trade_signal_id=safe_int(row.get("trade_signal_id")),
        stock_tick=str(row.get("stock_tick") or ""),
        stock_id=safe_int(row.get("stock_id")),
        country_id=safe_int(row.get("country_id")),
        time_frame=int(row.get("time_frame")),
        order_type=normalize_trade_type(row.get("order_type")),
        entry_price=safe_float(row.get("entry_price")),
        stoploss_price=safe_float(row.get("stoploss_price")),
        target_price=safe_float(row.get("target_price")),
        stock_quantity=safe_float(row.get("stock_quantity")),
        purchased_cmp_date=datetime_text(row.get("purchased_cmp_date")),
        purchased_on=row.get("purchased_on"),
        order_status=str(row.get("order_status")) if row.get("order_status") is not None else None,
        is_trade_started=parse_bool(row.get("is_trade_started")),
        is_evaluated=parse_bool(row.get("is_evaluated")),
        entry_timestamp=row.get("entry_timestamp"),
        completed_on=row.get("completed_on"),
        raw=dict(row),
    )


def fetch_orders(args: argparse.Namespace) -> List[OrderRow]:
    where_sql, params = build_where(args)
    sql = f"""
        SELECT
            order_id,
            trade_signal_id,
            stock_tick,
            stock_id,
            country_id,
            time_frame,
            order_type,
            entry_price,
            stoploss_price,
            target_price,
            stock_quantity,
            purchased_cmp_date,
            purchased_on,
            order_status,
            is_trade_started,
            is_evaluated,
            entry_timestamp,
            completed_on,
            lstm_rf_model_prediction,
            lstm_rf_model_prob,
            arima_ab_model_prediction,
            arima_ab_model_prob
        FROM {args.table_name}
        {where_sql}
        ORDER BY order_id ASC
    """

    db = DBConnection()
    session = db.get_session()
    try:
        rows = session.execute(text(sql), params).mappings().all()
        return [row_to_order(dict(row)) for row in rows]
    finally:
        session.close()
        try:
            db.close_engine()
        except Exception:
            pass


# ----------------------------------------------------------------------
# Engine rerun worker
# ----------------------------------------------------------------------

def rerun_engine_for_order(order: OrderRow, args: argparse.Namespace) -> Dict[str, Any]:
    started = time.time()
    status = parse_status(order.order_status)

    scan_at = scan_timestamp_for_order(order)
    stack = CASH_STACKS_BY_TF.get(int(order.time_frame))

    record: Dict[str, Any] = {
        "order_id": order.order_id,
        "trade_signal_id": order.trade_signal_id,
        "symbol": order.stock_tick,
        "engine_symbol": normalize_symbol(order.stock_tick, lowercase=not args.keep_symbol_case),
        "time_frame": order.time_frame,
        "order_type": order.order_type,
        "scan_at": scan_at.isoformat() if scan_at else None,
        "purchased_cmp_date": order.purchased_cmp_date,
        "purchased_on": datetime_text(order.purchased_on),
        "entry_timestamp": datetime_text(order.entry_timestamp),
        "completed_on": datetime_text(order.completed_on),
        "source_order": {
            "entry_price": order.entry_price,
            "stoploss_price": order.stoploss_price,
            "target_price": order.target_price,
            "stock_quantity": order.stock_quantity,
            "order_status": status,
            "is_trade_started": order.is_trade_started,
            "is_evaluated": order.is_evaluated,
        },
        "engine_status": "NOT_RUN",
        "engine_error": None,
        "stack_code": stack[0] if stack else None,
        "time_list": stack[1] if stack else None,
        "execute_tf": stack[1][-1] if stack else None,
        "engine_generated_setups_count": 0,
        "engine_generated_setups": [],
        "matched_setup": None,
        "match_reason": None,
        "selected_zone_fields": {},
        "circuit_block_detected": False,
        "circuit_block_reason": "",
        "raw_setup_payload": None,
        "formatted_setup_payload": None,
        "worker_seconds": None,
    }

    try:
        if scan_at is None:
            raise RuntimeError("Cannot parse purchased_cmp_date or purchased_on as scan timestamp")
        if stack is None:
            raise RuntimeError(f"Unsupported cash time_frame={order.time_frame}")

        engine_symbol = record["engine_symbol"]
        time_list = list(stack[1])

        raw = process_setup(engine_symbol, time_list, scan_at)
        if isinstance(raw, str):
            raise RuntimeError(raw)

        formatted = format_calculate_setup_response(
            raw,
            stock_name=engine_symbol,
            time_fr=int(order.time_frame),
            last_d_time=scan_at,
            is_cash=True,
        )

        if not isinstance(formatted, Mapping):
            raise RuntimeError("format_calculate_setup_response did not return a mapping")

        setups = extract_valid_engine_setups(
            formatted=formatted,
            min_rr=args.min_rr,
            include_sell=args.include_sell,
        )
        matched_setup, match_reason = choose_order_matching_setup(
            setups=setups,
            order=order,
            abs_tol=args.price_tolerance,
            rel_tol=args.relative_price_tolerance,
        )

        selected_zone_fields = extract_selected_zone_fields(
            raw_payload=raw,
            formatted_payload=formatted,
            order_type=order.order_type,
            entry_price=order.entry_price,
        )

        circuit_block, circuit_reason = contains_circuit_block([raw, formatted])

        record.update({
            "engine_status": "OK",
            "engine_generated_setups_count": len(setups),
            "engine_generated_setups": to_builtin(setups, max_string=args.max_string_field),
            "matched_setup": to_builtin(matched_setup, max_string=args.max_string_field),
            "match_reason": match_reason,
            "selected_zone_fields": to_builtin(selected_zone_fields, max_string=args.max_string_field),
            "circuit_block_detected": circuit_block,
            "circuit_block_reason": circuit_reason,
            "raw_setup_payload": to_builtin(raw, max_string=args.max_string_field),
            "formatted_setup_payload": to_builtin(formatted, max_string=args.max_string_field),
        })

    except Exception as exc:
        record.update({
            "engine_status": "ERROR",
            "engine_error": str(exc),
            "traceback": traceback.format_exc(limit=8),
        })

    record["worker_seconds"] = round(time.time() - started, 4)
    return record


# ----------------------------------------------------------------------
# Execution report builders
# ----------------------------------------------------------------------

def is_entry_filled(order: OrderRow, status: Mapping[str, Any]) -> bool:
    if order.is_trade_started is True:
        return True
    if order.entry_timestamp not in (None, ""):
        return True
    if status.get("entry_hit") is True:
        return True
    return False


def is_completed(order: OrderRow, status: Mapping[str, Any]) -> bool:
    st = status_text(status)
    if order.completed_on not in (None, ""):
        return True
    if st in {"success", "failed"}:
        return True
    return False


def exit_leg(status: Mapping[str, Any]) -> str:
    st = status_text(status)
    reason = str(status.get("reason") or "").lower()
    if st == "success" or "target" in reason:
        return "TARGET"
    if st == "failed" or "stoploss" in reason or "stop loss" in reason:
        return "SL"
    return "UNKNOWN"


def actual_filled_entry(order: OrderRow, status: Mapping[str, Any]) -> Tuple[Optional[float], str]:
    for key in ("actual_filled_entry_price", "filled_entry_price", "entry_fill_price"):
        value = safe_float(status.get(key))
        if value is not None:
            return value, f"order_status.{key}"
    return order.entry_price, "assumed_signal_entry_price_from_order_master"


def actual_exit_price(order: OrderRow, status: Mapping[str, Any]) -> Tuple[Optional[float], str]:
    for key in ("actual_exit_price", "exit_price", "filled_exit_price"):
        value = safe_float(status.get(key))
        if value is not None:
            return value, f"order_status.{key}"

    leg = exit_leg(status)
    if leg == "TARGET":
        return order.target_price, "assumed_target_price_from_order_master"
    if leg == "SL":
        return order.stoploss_price, "assumed_stoploss_price_from_order_master"
    return None, "UNKNOWN"


def entry_slippage(signal_entry: Any, filled_entry: Any, order_type: Any) -> Optional[float]:
    signal = safe_float(signal_entry)
    filled = safe_float(filled_entry)
    if signal is None or filled is None:
        return None
    side = normalize_trade_type(order_type)
    if side == "BUY":
        return round(filled - signal, 6)
    if side == "SELL":
        return round(signal - filled, 6)
    return round(filled - signal, 6)


def oco_confirmed(status: Mapping[str, Any]) -> Tuple[str, str]:
    for key in (
        "oco_confirmed", "gtt_oco_confirmed", "oco_placed_and_confirmed",
        "oco_placed", "gtt_oco_placed", "bracket_confirmed",
    ):
        if key in status:
            value = parse_bool(status.get(key))
            if value is True:
                return "YES", f"order_status.{key}"
            if value is False:
                return "NO", f"order_status.{key}"
    return "UNKNOWN", "order_master_has_no_oco_confirmation_column"


def completed_execution_row(order: OrderRow, engine_record: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    status = parse_status(order.order_status)
    if not is_completed(order, status):
        return None

    zone = engine_record.get("selected_zone_fields") or {}
    filled_entry, filled_entry_source = actual_filled_entry(order, status)
    exit_price, exit_price_source = actual_exit_price(order, status)
    oco_flag, oco_source = oco_confirmed(status)

    return {
        "order_id": order.order_id,
        "trade_signal_id": order.trade_signal_id,
        "symbol": order.stock_tick,
        "TF": order.time_frame,
        "order_type": order.order_type,
        "zone_type": zone.get("zone_type"),
        "selected_zone_id": zone.get("selected_zone_id"),
        "signalled_entry": order.entry_price,
        "signalled_sl": order.stoploss_price,
        "signalled_target": order.target_price,
        "actual_filled_entry_price": filled_entry,
        "fill_timestamp": datetime_text(order.entry_timestamp),
        "entry_slippage": entry_slippage(order.entry_price, filled_entry, order.order_type),
        "OCO_confirmed_flag": oco_flag,
        "which_leg_exited": exit_leg(status),
        "actual_exit_price": exit_price,
        "exit_timestamp": datetime_text(order.completed_on),
        "retest_count": zone.get("retest_count"),
        "freshness_status_at_emission": zone.get("freshness_status_at_emission"),
        "penetration_pct": zone.get("penetration_pct"),
        "entry_tertile": zone.get("entry_tertile"),
        "engine_status": engine_record.get("engine_status"),
        "engine_generated_setups_count": engine_record.get("engine_generated_setups_count"),
        "match_reason": engine_record.get("match_reason"),
        "order_status_status": status.get("status"),
        "order_status_reason": status.get("reason"),
        "profit_amount": status.get("profit_amount"),
        "profit_pct": status.get("profit_pct"),
        "loss_amount": status.get("loss_amount"),
        "loss_pct": status.get("loss_pct"),
        "mfe_R": status.get("mfe_R"),
        "mae_R": status.get("mae_R"),
        "bars_after_fill": status.get("bars_after_fill"),
        "stock_quantity": order.stock_quantity,
        "purchased_cmp_date": order.purchased_cmp_date,
        "purchased_on": datetime_text(order.purchased_on),
        "is_trade_started": order.is_trade_started,
        "is_evaluated": order.is_evaluated,
        "filled_entry_price_source": filled_entry_source,
        "exit_price_source": exit_price_source,
        "oco_source": oco_source,
        "selected_zone_source": zone.get("selected_zone_source"),
    }


def enriched_order_row(order: OrderRow, engine_record: Mapping[str, Any]) -> Dict[str, Any]:
    status = parse_status(order.order_status)
    zone = engine_record.get("selected_zone_fields") or {}
    filled_entry, _ = actual_filled_entry(order, status)
    exit_price, _ = actual_exit_price(order, status)
    oco_flag, _ = oco_confirmed(status)

    return {
        "order_id": order.order_id,
        "symbol": order.stock_tick,
        "time_frame": order.time_frame,
        "order_type": order.order_type,
        "entry_price": order.entry_price,
        "stoploss_price": order.stoploss_price,
        "target_price": order.target_price,
        "stock_quantity": order.stock_quantity,
        "purchased_cmp_date": order.purchased_cmp_date,
        "purchased_on": datetime_text(order.purchased_on),
        "is_trade_started": order.is_trade_started,
        "is_evaluated": order.is_evaluated,
        "entry_timestamp": datetime_text(order.entry_timestamp),
        "completed_on": datetime_text(order.completed_on),
        "status": status.get("status"),
        "reason": status.get("reason"),
        "entry_hit": status.get("entry_hit"),
        "exit_leg": exit_leg(status),
        "actual_filled_entry_price": filled_entry,
        "actual_exit_price": exit_price,
        "OCO_confirmed_flag": oco_flag,
        "engine_status": engine_record.get("engine_status"),
        "engine_error": engine_record.get("engine_error"),
        "engine_generated_setups_count": engine_record.get("engine_generated_setups_count"),
        "match_reason": engine_record.get("match_reason"),
        "zone_type": zone.get("zone_type"),
        "selected_zone_id": zone.get("selected_zone_id"),
        "retest_count": zone.get("retest_count"),
        "penetration_pct": zone.get("penetration_pct"),
        "entry_tertile": zone.get("entry_tertile"),
        "freshness_status_at_emission": zone.get("freshness_status_at_emission"),
        "freshness_suppressed_by_g4": zone.get("freshness_suppressed_by_g4"),
        "zone_state": zone.get("zone_state"),
        "quality_priority": zone.get("quality_priority"),
        "age_class": zone.get("age_class"),
        "zone_v38_score": zone.get("zone_v38_score"),
        "gap_composite_score": zone.get("gap_composite_score"),
        "selected_zone_source": zone.get("selected_zone_source"),
    }


def build_funnel(orders: List[OrderRow], engine_records: Mapping[int, Mapping[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    total_rows = len(orders)
    setups_generated = 0
    freshness_suppressed = 0
    freshness_known = 0
    circuit_suppressed = 0
    engine_errors = 0
    no_generated_setup = 0
    price_mismatch = 0
    gtt_entries_placed = total_rows
    filled = 0
    completed = 0
    target_exit = 0
    sl_exit = 0
    pending = 0
    not_triggered = 0
    oco_yes = 0
    oco_no = 0
    oco_unknown = 0

    for order in orders:
        record = engine_records.get(order.order_id, {})
        status = parse_status(order.order_status)
        st = status_text(status)

        generated_count = safe_int(record.get("engine_generated_setups_count")) or 0
        setups_generated += generated_count

        if record.get("engine_status") == "ERROR":
            engine_errors += 1
        if generated_count == 0:
            no_generated_setup += 1
        if record.get("match_reason") in {"NO_SAME_SIDE_SETUP", "SAME_SIDE_FALLBACK"}:
            price_mismatch += 1
        if record.get("circuit_block_detected") is True:
            circuit_suppressed += 1

        zone = record.get("selected_zone_fields") or {}
        if zone.get("freshness_status_at_emission") != "UNKNOWN":
            freshness_known += 1
            if zone.get("freshness_suppressed_by_g4") is True:
                freshness_suppressed += 1

        if is_entry_filled(order, status):
            filled += 1
            oco_flag, _ = oco_confirmed(status)
            if oco_flag == "YES":
                oco_yes += 1
            elif oco_flag == "NO":
                oco_no += 1
            else:
                oco_unknown += 1

        if is_completed(order, status):
            completed += 1

        leg = exit_leg(status)
        if leg == "TARGET":
            target_exit += 1
        elif leg == "SL":
            sl_exit += 1

        if st == "pending":
            pending += 1
        elif st == "not_triggered":
            not_triggered += 1

    suppressed_other = engine_errors + price_mismatch

    funnel = [
        {
            "stage_order": 1,
            "stage": "Setups generated by cash engine rerun",
            "count": setups_generated,
            "available_from": "ENGINE_RERUN",
            "notes": "Sum of valid BUY/SELL setups emitted by process_setup + format_calculate_setup_response at each order timestamp, post RR filter.",
        },
        {
            "stage_order": 2,
            "stage": "Suppressed at Execute-TF freshness gate G4",
            "count": freshness_suppressed,
            "available_from": "ENGINE_RERUN_ZONE_PARSE" if freshness_known else "NOT_AVAILABLE",
            "notes": "Computed from selected zone retest_count rule: BZ/SZ/GSZ limit=0, GDZ limit=1. Count is based on rerun/persisted order timestamps.",
        },
        {
            "stage_order": 3,
            "stage": "Suppressed at circuit-limit gate",
            "count": circuit_suppressed,
            "available_from": "ENGINE_RERUN_TEXT_SCAN",
            "notes": "Counts rerun payloads containing circuit/lower_ckt/upper_ckt text. If your engine does not emit circuit gate decisions, this remains 0.",
        },
        {
            "stage_order": 4,
            "stage": "Suppressed — other",
            "count": suppressed_other,
            "available_from": "ENGINE_RERUN",
            "notes": f"engine_errors={engine_errors}, price_or_side_mismatch={price_mismatch}, no_generated_setup={no_generated_setup}.",
        },
        {
            "stage_order": 5,
            "stage": "GTT Single entries placed / persisted",
            "count": gtt_entries_placed,
            "available_from": "ORDER_MASTER",
            "notes": "order_master rows are treated as persisted/sent entry orders. Exact broker placement needs broker/GTT table columns.",
        },
        {
            "stage_order": 6,
            "stage": "Filled — entry confirmed",
            "count": filled,
            "available_from": "ORDER_MASTER",
            "notes": "is_trade_started=1 OR entry_timestamp exists OR order_status.entry_hit=True.",
        },
        {
            "stage_order": 7,
            "stage": "Filled with OCO confirmed",
            "count": oco_yes,
            "available_from": "ORDER_STATUS_IF_PRESENT",
            "notes": f"oco_yes={oco_yes}, oco_no={oco_no}, oco_unknown_for_filled={oco_unknown}. order_master has no native OCO column.",
        },
        {
            "stage_order": 8,
            "stage": "Completed",
            "count": completed,
            "available_from": "ORDER_MASTER",
            "notes": "completed_on exists OR order_status status is success/failed.",
        },
        {
            "stage_order": 9,
            "stage": "Exited by Target",
            "count": target_exit,
            "available_from": "ORDER_STATUS",
            "notes": "Derived from order_status.status/reason.",
        },
        {
            "stage_order": 10,
            "stage": "Exited by SL",
            "count": sl_exit,
            "available_from": "ORDER_STATUS",
            "notes": "Derived from order_status.status/reason.",
        },
        {
            "stage_order": 11,
            "stage": "Still pending",
            "count": pending,
            "available_from": "ORDER_STATUS",
            "notes": "Derived from order_status.status=pending.",
        },
        {
            "stage_order": 12,
            "stage": "Entry not triggered",
            "count": not_triggered,
            "available_from": "ORDER_STATUS",
            "notes": "Derived from order_status.status=not_triggered.",
        },
    ]

    summary = {
        "total_order_master_rows": total_rows,
        "engine_generated_setups": setups_generated,
        "freshness_known_rows": freshness_known,
        "freshness_suppressed_by_g4": freshness_suppressed,
        "circuit_suppressed_detected": circuit_suppressed,
        "suppressed_other": suppressed_other,
        "engine_errors": engine_errors,
        "no_generated_setup_rows": no_generated_setup,
        "price_or_side_mismatch_rows": price_mismatch,
        "gtt_single_entries_placed_or_persisted": gtt_entries_placed,
        "filled": filled,
        "completed": completed,
        "target_exit": target_exit,
        "sl_exit": sl_exit,
        "pending": pending,
        "not_triggered": not_triggered,
        "oco_yes": oco_yes,
        "oco_no": oco_no,
        "oco_unknown_for_filled": oco_unknown,
    }

    return funnel, summary


# ----------------------------------------------------------------------
# Writers
# ----------------------------------------------------------------------

def write_csv(path: str, rows: List[Mapping[str, Any]]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        output.write_text("", encoding="utf-8")
        return

    fieldnames = list(rows[0].keys())
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: str, payload: Mapping[str, Any]) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=json_default) + "\n", encoding="utf-8")


# ----------------------------------------------------------------------
# Runner / CLI
# ----------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Cash order_master report: rerun engine by order timestamp/symbol/TF and export populated execution/funnel reports."
    )

    parser.add_argument("--table-name", default="order_master")
    parser.add_argument("--timeframes", default="1,2,5,6,25")
    parser.add_argument("--symbols", default=None, help="Comma-separated cash stock_tick values")
    parser.add_argument("--order-id", type=int, default=None)
    parser.add_argument("--purchased-from", default=None)
    parser.add_argument("--purchased-to", default=None)
    parser.add_argument("--completed-from", default=None)
    parser.add_argument("--completed-to", default=None)
    parser.add_argument("--min-rr", type=float, default=DEFAULT_MIN_RR)
    parser.add_argument("--include-sell", action="store_true", help="Include SELL setups in generated setup count")
    parser.add_argument("--keep-symbol-case", action="store_true", help="Do not lowercase stock_tick before calling cash engine")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--price-tolerance", type=float, default=0.05)
    parser.add_argument("--relative-price-tolerance", type=float, default=0.00001)
    parser.add_argument("--max-string-field", type=int, default=None, help="Optionally truncate very large object strings in payload JSONL")
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--run-label", default=None)
    parser.add_argument("--progress", action="store_true")
    return parser


def main() -> None:
    args = build_parser().parse_args()

    if args.workers < 1:
        raise ValueError("--workers must be >= 1")

    label = args.run_label or now_label()
    output_dir = args.output_dir or f"outputs/cash_order_master_engine_rerun_{label}"
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    if args.progress:
        print("[1/5] Reading order_master rows...")

    orders = fetch_orders(args)

    if args.progress:
        print(f"Loaded {len(orders)} orders")
        print("[2/5] Rerunning cash engine per order...")

    engine_records: Dict[int, Dict[str, Any]] = {}
    payload_jsonl = os.path.join(output_dir, "engine_rerun_payloads.jsonl")

    started = time.time()
    completed_jobs = 0

    with open(payload_jsonl, "w", encoding="utf-8") as payload_handle:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_map = {executor.submit(rerun_engine_for_order, order, args): order for order in orders}
            for future in as_completed(future_map):
                order = future_map[future]
                try:
                    record = future.result()
                except Exception as exc:
                    record = {
                        "order_id": order.order_id,
                        "symbol": order.stock_tick,
                        "time_frame": order.time_frame,
                        "order_type": order.order_type,
                        "engine_status": "ERROR",
                        "engine_error": f"Worker crashed: {exc}",
                        "traceback": traceback.format_exc(limit=8),
                    }

                engine_records[order.order_id] = record
                payload_handle.write(json_dumps(record) + "\n")
                payload_handle.flush()
                completed_jobs += 1

                if args.progress and (completed_jobs == 1 or completed_jobs % 25 == 0 or completed_jobs == len(orders)):
                    elapsed = max(time.time() - started, 0.001)
                    print(
                        f"[progress] engine_rerun={completed_jobs}/{len(orders)} "
                        f"rate={completed_jobs / elapsed:.2f}/sec"
                    )

    if args.progress:
        print("[3/5] Building execution log and funnel...")

    completed_rows = []
    enriched_rows = []
    for order in orders:
        record = engine_records.get(order.order_id, {})
        completed_row = completed_execution_row(order, record)
        if completed_row is not None:
            completed_rows.append(completed_row)
        enriched_rows.append(enriched_order_row(order, record))

    funnel_rows, summary = build_funnel(orders, engine_records)

    if args.progress:
        print("[4/5] Writing CSV/JSON outputs...")

    completed_csv = os.path.join(output_dir, "completed_order_execution_log.csv")
    funnel_csv = os.path.join(output_dir, "setup_to_fill_funnel.csv")
    enriched_csv = os.path.join(output_dir, "all_orders_engine_enriched.csv")
    summary_json = os.path.join(output_dir, "summary.json")

    write_csv(completed_csv, completed_rows)
    write_csv(funnel_csv, funnel_rows)
    write_csv(enriched_csv, enriched_rows)

    summary.update({
        "table_name": args.table_name,
        "timeframes": parse_csv_int_list(args.timeframes, DEFAULT_TIMEFRAMES),
        "symbols": parse_csv_str_list(args.symbols),
        "order_id": args.order_id,
        "purchased_from": args.purchased_from,
        "purchased_to": args.purchased_to,
        "completed_from": args.completed_from,
        "completed_to": args.completed_to,
        "min_rr": args.min_rr,
        "include_sell": bool(args.include_sell),
        "workers": args.workers,
        "duration_seconds": round(time.time() - started, 2),
        "output_dir": output_dir,
        "completed_order_execution_log_csv": completed_csv,
        "setup_to_fill_funnel_csv": funnel_csv,
        "all_orders_engine_enriched_csv": enriched_csv,
        "engine_rerun_payloads_jsonl": payload_jsonl,
    })

    write_json(summary_json, summary)

    if args.progress:
        print("[5/5] Done")

    print("CASH ORDER MASTER ENGINE-RERUN EXPORT COMPLETE")
    print(f"completed_order_execution_log_csv: {completed_csv}")
    print(f"setup_to_fill_funnel_csv: {funnel_csv}")
    print(f"all_orders_engine_enriched_csv: {enriched_csv}")
    print(f"engine_rerun_payloads_jsonl: {payload_jsonl}")
    print(f"summary_json: {summary_json}")


if __name__ == "__main__":
    main()
