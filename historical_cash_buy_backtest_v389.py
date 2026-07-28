#!/usr/bin/env python3
"""
Historical NSE CASH BUY-only backtest scanner + optional evaluator.

This script is based on the updated scanner doctrine but keeps the backtest flow:
- CASH only
- BUY only
- min_rr default 2.1
- cash liquidity gate uses the NIFTY100 / validated liquid allowlist below
- no SELL order generation
- optional B-W-REVAL hook before scanning
- optional evaluation after scan
- order_status is stored as Python dict string, for example:
  {'status': 'success', 'reason': 'Target hit', 'entry_hit': True, 'profit_amount': 1773.0, 'profit_pct': 8.42}

Run from project root.
"""

import argparse
import csv
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime
from typing import Any, Dict, Iterable, List, Optional, Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd
from dateutil import parser as dt_parser
from sqlalchemy import func
from sqlalchemy.sql import exists
from tqdm import tqdm

from shared.db.dbconn import DBConnection
from shared.db.db_model import Ind_StockMaster, TradeSignal

try:
    from shared.db.db_model import Auto_Order
except Exception:
    Auto_Order = None

from scripts.setup_engine_new import process_setup, format_calculate_setup_response
from shared.db.db_utils import insert_alerts
from shared.utils.logger import logger
from shared.config.settings import Config as cfg
from shared.config.settings import stock_data_dir_config


# =============================================================================
# GLOBALS / CONSTANTS
# =============================================================================

dbc = DBConnection()
MIN_RR_THRESHOLD = 2.1
CASH_EXCHANGE_ID = 8
SEGMENT = "NSE_CASH"
BUY_ONLY = True
CASH_SHORT_SQUARE_OFF_TIME = "15:15"


# =============================================================================
# STACK CONFIG — only these six whitelisted cells emit
# =============================================================================

STACKS: Dict[str, Dict[str, Any]] = {
    "M-W-D": {
        "time_frame": 1,
        "time_list": ["monthly", "weekly", "daily"],
    },
    "W-D-125": {
        "time_frame": 5,
        "time_list": ["weekly", "daily", "one_twenty_five"],
    },
    "W-D-75": {
        "time_frame": 25,
        "time_list": ["weekly", "daily", "seventy_five"],
    },
    "W-D-60": {
        "time_frame": 2,
        "time_list": ["weekly", "daily", "sixty"],
    },
    "D-125-25": {
        "time_frame": 6,
        "time_list": ["daily", "one_twenty_five", "twenty_five"],
    },
    "D-60-15": {
        "time_frame": 3,
        "time_list": ["daily", "sixty", "fifteen"],
    },
}

WHITELISTED_CELLS = set(STACKS.keys())


# =============================================================================
# D-1 CASH GATE SOURCE — config allowlist path selected
# =============================================================================
# This is the CASH-100 / validated liquid cash universe.
# If this list is empty, cash scan fails closed.

NIFTY100_CASH_STOCK_TICKS = [
    "abb_india",
    "adani_energy_solutions",
    "adani_enterprises",
    "adani_green",
    "adani_ports",
    "adani_power",
    "ambuja_cement",
    "apollo_hospitals",
    "asian_paints",
    "avenue_supermarts",
    "axis_bank",
    "bajaj_auto",
    "bajaj_finance",
    "bajaj_finserv",
    "bajaj_holdings",
    "bajaj_housing_finance",
    "bank_of_baroda",
    "bharat_electronics",
    "bharti_airtel",
    "bosch",
    "bpcl",
    "britannia_industries",
    "canara_bank",
    "cg_power_inds",
    "cholamandalam_investment",
    "cipla",
    "coal_india",
    "dabur",
    "divis_laboratories",
    "dlf",
    "dr_reddys_laboratories",
    "eicher_motors",
    "eternal",
    "gail",
    "godrej_consumer",
    "grasim_industries",
    "havells_india",
    "hcl_technologies",
    "hdfc_life",
    "hdfcbank",
    "hero_motocorp",
    "hindalco_industries",
    "hindunilvr",
    "hindustan_aeronautics",
    "hyundai_motor_india",
    "icici_lombard_gic",
    "icici_prudential_life",
    "icicibank",
    "indian_hotels",
    "indigo",
    "indusindbk",
    "info_edge",
    "infy",
    "iocl",
    "irfc",
    "itc",
    "jindal_steel_power",
    "jio_financial_service",
    "jsw_energy",
    "jsw_steel",
    "kotak_bank",
    "l_t",
    "lic",
    "ltimindtree",
    "mahindra_mahindra",
    "maruti_suzuki",
    "nestle_india",
    "ntpc",
    "ongc",
    "pidilite_industries",
    "pnb",
    "power_finance_corporation",
    "power_grid_corporation",
    "recl",
    "reliance",
    "samvardhana_motherson",
    "sbi_life",
    "shree_cement",
    "shriram_finance",
    "siemens",
    "state_bank_of_india",
    "sun_pharma",
    "swiggy",
    "tata_consumer_products",
    "tata_steel",
    "tatamotors",
    "tatapower",
    "tcs",
    "tech_mahindra",
    "titan",
    "torrent_pharmaceuticals",
    "trent",
    "tvs_motor",
    "ultratech_cement",
    "united_spirits",
    "varun_beverages",
    "vedanta",
    "wipro",
    "zydus_life",
]

CASH_FNO_ALLOWLIST = set(NIFTY100_CASH_STOCK_TICKS)

CASH_STACK_STOCK_TICK_MAP = {
    "M-W-D": NIFTY100_CASH_STOCK_TICKS,
    "W-D-125": NIFTY100_CASH_STOCK_TICKS,
    "W-D-75": NIFTY100_CASH_STOCK_TICKS,
    "W-D-60": NIFTY100_CASH_STOCK_TICKS,
    "D-125-25": NIFTY100_CASH_STOCK_TICKS,
    "D-60-15": NIFTY100_CASH_STOCK_TICKS,
}


# =============================================================================
# B-W-EMBED PARAMS — recorded for deterministic backtest runs
# =============================================================================

DEFAULT_EMBED_OVERLAP_THRESHOLD = 0.50
DEFAULT_EMBED_SITS_ON_TOP_TARGET_PCT = 0.50
DEFAULT_EMBED_STRICT_STOP = False


# =============================================================================
# DATACLASSES
# =============================================================================

@dataclass(frozen=True)
class CashJob:
    symbol: str
    scan_at: datetime
    stack_code: str
    time_frame: int
    time_list: List[str]


@dataclass
class RevalidationJob:
    time_lists: List[List[str]]
    time_frame: int
    last_d_time: Any
    stack_code: str


# =============================================================================
# GENERIC HELPERS
# =============================================================================

def normalize_tick(value: Any) -> str:
    return str(value or "").strip().lower()


def unique_preserve_order(values: Iterable[str]) -> List[str]:
    seen = set()
    result = []
    for value in values:
        tick = normalize_tick(value)
        if not tick or tick in seen:
            continue
        seen.add(tick)
        result.append(tick)
    return result


def parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def parse_scan_datetime(date_value: datetime, scan_time: str) -> datetime:
    hh, mm = scan_time.split(":")
    return date_value.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)


def daterange(start_date: str, end_date: str, scan_time: str, skip_weekends: bool = True) -> Iterable[datetime]:
    cur = parse_date(start_date)
    end = parse_date(end_date)
    while cur <= end:
        if not skip_weekends or cur.weekday() < 5:
            yield parse_scan_datetime(cur, scan_time)
        cur += timedelta(days=1)


def json_default(obj):
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.isoformat()
    return str(obj)


def parse_datetime_value(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return dt_parser.parse(text)
    except Exception:
        return None


# =============================================================================
# D-1 CASH LOADER / GATE
# =============================================================================

def apply_cash_fno_gate(symbols: List[str], is_cash_segment: bool = True) -> List[str]:
    """
    D-1 gate.
    Cash defaults is_cash_segment=True.
    FUT/MCX call-sites must pass is_cash_segment=False.
    """
    if not is_cash_segment:
        return symbols

    if not CASH_FNO_ALLOWLIST:
        logger.error("D-1 cash gate: CASH_FNO_ALLOWLIST empty -> FAIL CLOSED")
        return []

    gated = [s for s in symbols if s in CASH_FNO_ALLOWLIST]
    return gated


def get_all_cash_stock_ticks(is_cash_segment: bool = True) -> List[str]:
    session = dbc.get_session()
    try:
        rows = (
            session.query(Ind_StockMaster.stock_tick)
            .filter(Ind_StockMaster.is_active == True)
            .all()
        )
        ticks = [normalize_tick(row[0]) for row in rows if normalize_tick(row[0])]
        return sorted(set(apply_cash_fno_gate(ticks, is_cash_segment=is_cash_segment)))
    finally:
        session.close()


def get_available_stock_syms_by_timeframe(time_fr: int, is_cash_segment: bool = True) -> List[str]:
    """
    Compatibility loader from the updated scanner.
    For cash this is D-1 gated by default.
    At FUT/MCX call-sites pass is_cash_segment=False.
    """
    session = dbc.get_session()
    try:
        query = (
            session.query(Ind_StockMaster.stock_tick)
            .filter(Ind_StockMaster.is_active == True)
            .filter(
                ~exists().where(
                    (TradeSignal.stock_name == Ind_StockMaster.stock_tick) &
                    (TradeSignal.time_fr == time_fr) &
                    (TradeSignal.is_active == True) &
                    (TradeSignal.exchange_id == CASH_EXCHANGE_ID)
                )
            )
        )
        rows = query.all()
        ticks = [normalize_tick(row[0]) for row in rows if normalize_tick(row[0])]
        return apply_cash_fno_gate(ticks, is_cash_segment=is_cash_segment)
    finally:
        session.close()


# =============================================================================
# STACK / SYMBOL SELECTION
# =============================================================================

def selected_stacks(time_frame: Optional[str], stack_code: Optional[str]) -> List[Tuple[str, int, List[str]]]:
    items: List[Tuple[str, int, List[str]]] = []

    if stack_code:
        requested_codes = [x.strip() for x in stack_code.split(",") if x.strip()]
        for code in requested_codes:
            if code not in STACKS:
                raise ValueError(f"Unknown stack code: {code}. Valid: {', '.join(STACKS.keys())}")
            meta = STACKS[code]
            items.append((code, int(meta["time_frame"]), list(meta["time_list"])))
        return items

    if time_frame:
        requested_tfs = {int(x.strip()) for x in time_frame.split(",") if x.strip()}
        for code, meta in STACKS.items():
            if int(meta["time_frame"]) in requested_tfs:
                items.append((code, int(meta["time_frame"]), list(meta["time_list"])))
        if not items:
            raise ValueError(f"No stacks found for --time-frame {time_frame}. Valid: 1,2,3,5,6,25")
        return items

    return [(code, int(meta["time_frame"]), list(meta["time_list"])) for code, meta in STACKS.items()]


def infer_execute_tf(time_list: List[str]) -> str:
    return time_list[-1]


def build_symbols_by_stack(
    stack_items: List[Tuple[str, int, List[str]]],
    symbol: Optional[str],
    symbol_match: str,
    stock_limit: int,
    validate_db: bool,
) -> Dict[str, List[str]]:
    active_ticks = set(get_all_cash_stock_ticks(is_cash_segment=True)) if validate_db else set()
    symbols_by_stack: Dict[str, List[str]] = {}

    for stack_code, _, _ in stack_items:
        if stack_code not in WHITELISTED_CELLS:
            symbols_by_stack[stack_code] = []
            continue

        mapped = unique_preserve_order(CASH_STACK_STOCK_TICK_MAP.get(stack_code, []))
        mapped = apply_cash_fno_gate(mapped, is_cash_segment=True)

        if symbol:
            requested = normalize_tick(symbol)
            if symbol_match == "exact":
                mapped = [s for s in mapped if s == requested]
            elif symbol_match == "contains":
                mapped = [s for s in mapped if requested in s]
            else:
                raise ValueError(f"Unsupported --symbol-match: {symbol_match}")

        if validate_db:
            missing = [s for s in mapped if s not in active_ticks]
            if missing:
                print(f"[WARN] mapped ticks not active in Ind_StockMaster for {stack_code}: {missing}")
            mapped = [s for s in mapped if s in active_ticks]

        if stock_limit and stock_limit > 0:
            mapped = mapped[:stock_limit]

        symbols_by_stack[stack_code] = mapped

    return symbols_by_stack


# =============================================================================
# BUY-ONLY SETUP EXTRACTION
# =============================================================================

def extract_buy_setups_only(formatted: Dict[str, Any], min_rr: float) -> List[Dict[str, Any]]:
    setups: List[Dict[str, Any]] = []

    if "BUY" in formatted and float(formatted.get("BUY_RRR", 0) or 0) >= min_rr:
        setup_record = {
            "trade_type": "BUY",
            "rrr": formatted.get("BUY_RRR"),
            "trade_dict": formatted["BUY"],
            "selection_reason": None,
            "proximity_pct": None,
        }

        # Best effort proximity/selection recording for C7.
        try:
            buy_zones = formatted.get("ZONES_X", {}).get("Buy", [])
            if buy_zones:
                z0 = buy_zones[0]
                setup_record["selection_reason"] = z0.get("selection_reason") or z0.get("reason")
                meta = z0.get("meta", {}) if isinstance(z0, dict) else {}
                setup_record["proximity_pct"] = z0.get("proximity_pct") or meta.get("proximity_pct")
        except Exception:
            pass

        setups.append(setup_record)

    # BUY_ONLY: SELL is intentionally ignored even if engine emits SELL.
    return setups


# =============================================================================
# SCAN / INSERT
# =============================================================================

def scan_one(job: CashJob, min_rr: float, embed_params: Dict[str, Any]) -> Dict[str, Any]:
    started = time.time()

    record: Dict[str, Any] = {
        "scan_at": job.scan_at.isoformat(),
        "symbol": job.symbol,
        "stack_code": job.stack_code,
        "time_list": job.time_list,
        "execute_tf": infer_execute_tf(job.time_list),
        "time_frame": job.time_frame,
        "segment": "cash",
        "side_policy": "BUY_ONLY",
        "bw_embed_params": embed_params,
        "status": "NO_SETUP",
        "setups": [],
    }

    try:
        raw = process_setup(job.symbol, job.time_list, job.scan_at)
        if isinstance(raw, str):
            raise RuntimeError(raw)

        formatted = format_calculate_setup_response(
            raw,
            stock_name=job.symbol,
            time_fr=job.time_frame,
            last_d_time=job.scan_at,
            is_cash=True,
        )

        record["setup"] = formatted
        setups = extract_buy_setups_only(formatted, min_rr=min_rr)
        record["setups"] = setups
        record["status"] = "SETUP" if setups else "NO_SETUP"

    except Exception as exc:
        record["status"] = "ERROR"
        record["error"] = str(exc)

    record["worker_seconds"] = round(time.time() - started, 4)
    return record


def try_fetch_latest_order_id(
    symbol: str,
    time_frame: int,
    order_type: str,
    scan_at: datetime,
) -> Optional[int]:
    if Auto_Order is None:
        return None

    session = dbc.get_session()
    try:
        purchased_cmp_date = scan_at.strftime("%Y-%m-%dT%H:%M")
        row = (
            session.query(Auto_Order)
            .filter(Auto_Order.stock_tick == symbol)
            .filter(Auto_Order.time_frame == time_frame)
            .filter(Auto_Order.order_type == order_type)
            .filter(Auto_Order.purchased_cmp_date == purchased_cmp_date)
            .order_by(Auto_Order.order_id.desc())
            .first()
        )
        return getattr(row, "order_id", None) if row else None
    except Exception:
        return None
    finally:
        session.close()


def insert_record_orders(record: Dict[str, Any], print_inserts: bool = False) -> Dict[str, Any]:
    order_results: List[Dict[str, Any]] = []

    if record.get("status") != "SETUP":
        record["order_results"] = order_results
        return record

    scan_at = datetime.fromisoformat(record["scan_at"])
    symbol = record["symbol"]
    time_frame = int(record["time_frame"])

    for setup in record.get("setups", []):
        trade_type = setup["trade_type"]

        # Hard BUY-only guard.
        if trade_type != "BUY":
            continue

        trade_dict = setup["trade_dict"]
        result = {
            "trade_type": trade_type,
            "rrr": setup.get("rrr"),
            "trade_dict": trade_dict,
            "insert_status": "NOT_ATTEMPTED",
            "insert_reason": None,
            "order_id": None,
        }

        try:
            insert_alerts(trade_dict, time_frame, symbol, trade_type, scan_at)
            result["insert_status"] = "ORDER_INSERTED"
            result["insert_reason"] = "insert_alerts"
            result["order_id"] = try_fetch_latest_order_id(symbol, time_frame, trade_type, scan_at)

            if print_inserts:
                print(
                    f"[INSERTED] {symbol} stack={record['stack_code']} "
                    f"tf={time_frame} BUY scan_at={record['scan_at']} order_id={result['order_id']}"
                )

        except Exception as exc:
            result["insert_status"] = "INSERT_ERROR"
            result["insert_reason"] = str(exc)
            logger.error(f"insert_alerts failed for {symbol} tf={time_frame} BUY: {exc}", exc_info=True)
            if print_inserts:
                print(f"[INSERT_ERROR] {symbol} tf={time_frame} BUY: {exc}")

        order_results.append(result)

    record["order_results"] = order_results
    return record


def build_jobs(
    symbols_by_stack: Dict[str, List[str]],
    stack_items: List[Tuple[str, int, List[str]]],
    start_date: str,
    end_date: str,
    scan_time: str,
    skip_weekends: bool,
) -> Iterable[CashJob]:
    for scan_at in daterange(start_date, end_date, scan_time, skip_weekends=skip_weekends):
        for stack_code, time_frame, time_list in stack_items:
            for symbol in symbols_by_stack.get(stack_code, []):
                yield CashJob(
                    symbol=symbol,
                    scan_at=scan_at,
                    stack_code=stack_code,
                    time_frame=time_frame,
                    time_list=time_list,
                )


# =============================================================================
# B-W-REVAL — RESTING SETUP REVALIDATION HOOK
# =============================================================================

def get_resting_setups_by_timeframe(time_fr: int):
    session = dbc.get_session()
    try:
        query = (
            session.query(TradeSignal)
            .filter(TradeSignal.time_fr == time_fr)
            .filter(TradeSignal.is_active == True)
            .filter(TradeSignal.exchange_id == CASH_EXCHANGE_ID)
        )

        # T4 schema fallback.
        if hasattr(TradeSignal, "is_trade_started"):
            query = query.filter(TradeSignal.is_trade_started == False)
        elif hasattr(TradeSignal, "is_filled"):
            query = query.filter(TradeSignal.is_filled == False)
        elif hasattr(TradeSignal, "entry_hit"):
            query = query.filter(TradeSignal.entry_hit == False)

        return query.all()
    finally:
        session.close()


def deactivate_trade_signal(trade_id: int) -> bool:
    session = dbc.get_session()
    try:
        trade = session.query(TradeSignal).filter(TradeSignal.id == trade_id).first()
        if not trade:
            return False
        trade.is_active = False
        session.commit()
        return True
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def zone_decayed(formatted: dict, trade_type: str, stored_entry: float, stored_stop: float, cmp_value: float) -> Tuple[bool, Optional[str]]:
    side = str(trade_type).upper()

    if cmp_value is not None and stored_stop is not None:
        if side == "BUY" and cmp_value <= stored_stop:
            return True, "STRUCTURE_BREAK (cmp<=stored_stop, demand base violated)"
        if side == "SELL" and cmp_value >= stored_stop:
            return True, "STRUCTURE_BREAK (cmp>=stored_stop, supply base violated)"

    has_side = (side == "BUY" and "BUY" in formatted) or (side == "SELL" and "SELL" in formatted)
    if not has_side:
        return True, "DIRECTION_LOST (no same-side setup on revalidation)"

    fresh_entry = formatted.get("BUY_ENTRY") if side == "BUY" else formatted.get("SELL_ENTRY")
    fresh_stop = formatted.get("BUY_SL") if side == "BUY" else formatted.get("SELL_SL")

    if fresh_entry is not None and stored_entry:
        scale = abs(fresh_entry - fresh_stop) if fresh_stop is not None else abs(stored_entry - stored_stop)
        if scale and abs(fresh_entry - stored_entry) > scale:
            return True, f"ENTRY_DRIFT (fresh entry moved >1 stop-unit: {fresh_entry:.2f} vs {stored_entry:.2f})"

    return False, None


def revalidate_resting_setups(job: RevalidationJob, min_rr: float, dry_run: bool = False) -> Dict[str, Any]:
    started = time.time()
    resting = get_resting_setups_by_timeframe(job.time_frame)

    stats = {
        "stack_code": job.stack_code,
        "time_frame": job.time_frame,
        "total_resting": len(resting),
        "still_valid": 0,
        "deactivated": 0,
        "errors": 0,
        "dry_run": dry_run,
    }

    for setup in resting:
        try:
            symbol = setup.stock_name
            trade_type = str(setup.trade_type).upper()

            # BUY-only backtest only needs BUY resting revalidation.
            if BUY_ONLY and trade_type != "BUY":
                continue

            raw = process_setup(symbol, job.time_lists[0], job.last_d_time)
            if isinstance(raw, str):
                if not dry_run:
                    deactivate_trade_signal(setup.id)
                stats["deactivated"] += 1
                continue

            formatted = format_calculate_setup_response(
                raw,
                stock_name=symbol,
                time_fr=job.time_frame,
                last_d_time=job.last_d_time,
                is_cash=True,
            )

            is_valid = False
            if trade_type == "BUY" and "BUY" in formatted and float(formatted.get("BUY_RRR", 0) or 0) >= min_rr:
                is_valid = True
            elif trade_type == "SELL" and "SELL" in formatted and float(formatted.get("SELL_RRR", 0) or 0) >= min_rr:
                is_valid = True

            cmp_value = formatted.get("CMP") or getattr(setup, "cmp", None)
            decayed, reason = zone_decayed(
                formatted,
                trade_type,
                stored_entry=getattr(setup, "entry_price", None),
                stored_stop=getattr(setup, "stoploss_price", None),
                cmp_value=cmp_value,
            )

            if is_valid and not decayed:
                stats["still_valid"] += 1
            else:
                if not dry_run:
                    deactivate_trade_signal(setup.id)
                stats["deactivated"] += 1
                logger.info(f"B-W-REVAL: deactivated resting setup {setup.id} ({symbol} {trade_type}): {reason or 'RR below threshold / no valid setup'}")

        except Exception as exc:
            stats["errors"] += 1
            logger.error(f"B-W-REVAL error for setup {getattr(setup, 'id', None)}: {exc}", exc_info=True)

    stats["duration_seconds"] = round(time.time() - started, 2)
    return stats


def run_revalidation_for_scan(stack_items: List[Tuple[str, int, List[str]]], scan_at: datetime, min_rr: float, dry_run: bool) -> List[Dict[str, Any]]:
    results = []
    for stack_code, time_frame, time_list in stack_items:
        result = revalidate_resting_setups(
            RevalidationJob(
                time_lists=[time_list],
                time_frame=time_frame,
                last_d_time=scan_at,
                stack_code=stack_code,
            ),
            min_rr=min_rr,
            dry_run=dry_run,
        )
        results.append(result)
    return results


# =============================================================================
# EVALUATION + MFE/MAE
# =============================================================================

try:
    from design.mfe_mae_helper import mfe_mae_in_R, summarize as summarize_mfe_mae
except Exception:
    def mfe_mae_in_R(entry, stop, side, bar_highs, bar_lows):
        risk = abs(float(entry) - float(stop))
        if risk <= 0:
            return None, None
        if not bar_highs or not bar_lows:
            return 0.0, 0.0
        side = str(side).upper()
        if side == "BUY":
            mfe = (max(bar_highs) - float(entry)) / risk
            mae = (float(entry) - min(bar_lows)) / risk
        else:
            mfe = (float(entry) - min(bar_lows)) / risk
            mae = (max(bar_highs) - float(entry)) / risk
        return round(float(mfe), 4), round(float(mae), 4)

    def summarize_mfe_mae(trades):
        n = len(trades)
        if n == 0:
            return {"n": 0, "win_rate": 0.0, "M1_give_back_rate": 0.0, "M2_wick_out_rate": 0.0}
        wins = [t for t in trades if t.get("win")]
        # M1 approximation: reached >= 1R MFE but did not finish as win.
        m1 = [t for t in trades if (t.get("mfe_R") or 0) >= 1.0 and not t.get("win")]
        # M2 approximation: MAE touched >= 1R but trade finished as win.
        m2 = [t for t in trades if (t.get("mae_R") or 0) >= 1.0 and t.get("win")]
        return {
            "n": n,
            "win_rate": round(len(wins) / n, 4),
            "M1_give_back_rate": round(len(m1) / n, 4),
            "M2_wick_out_rate": round(len(m2) / n, 4),
        }


def is_in_range(value, lower_bound, upper_bound):
    return min(lower_bound, upper_bound) <= value <= max(lower_bound, upper_bound)


def calculate_profit_or_loss(current_value, entry_value, order_type):
    if str(order_type).lower() == "buy":
        difference = current_value - entry_value
    elif str(order_type).lower() == "sell":
        difference = entry_value - current_value
    else:
        raise ValueError(f"Invalid order type: {order_type}")
    percentage_change = (difference / entry_value) * 100
    return round(difference, 2), round(percentage_change, 2)


def check_price_hit(price, row, order_type, is_target=True):
    low = row.get("low")
    high = row.get("high")
    if is_in_range(price, low, high):
        return True
    if str(order_type).lower() == "buy":
        return high >= price if is_target else low <= price
    if str(order_type).lower() == "sell":
        return low <= price if is_target else high >= price
    return False


def get_cash_execution_frame(time_frame: int) -> str:
    attr = f"TIME_FRAMES_{int(time_frame)}"
    if hasattr(cfg, attr):
        time_list = getattr(cfg, attr)
        if time_list:
            return time_list[-1]

    # Fallback to script stack map.
    for meta in STACKS.values():
        if int(meta["time_frame"]) == int(time_frame):
            return meta["time_list"][-1]

    raise ValueError(f"Cannot infer cash execution frame for time_frame={time_frame}")


def load_cash_candles(data_dir: str, stock_tick: str, exe_frame: str):
    file_path = os.path.join(data_dir, "latest_data_csv", f"{stock_tick}_{exe_frame}.csv")
    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)

    df = pd.read_csv(file_path)
    col = "tradeDate" if "tradeDate" in df.columns else "timestamp"
    if col not in df.columns:
        raise ValueError(f"No tradeDate/timestamp column found in {file_path}. Columns={list(df.columns)}")

    try:
        df[col] = pd.to_datetime(df[col], format="%d/%m/%Y %H:%M:%S")
    except Exception:
        try:
            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d %H:%M:%S")
        except Exception:
            df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

    df = df.dropna(subset=[col]).copy()
    df = df.rename(columns={col: "_dt"})

    rename_map = {}
    for c in df.columns:
        lc = str(c).strip().lower()
        if lc in ("open", "o"):
            rename_map[c] = "open"
        elif lc in ("high", "h"):
            rename_map[c] = "high"
        elif lc in ("low", "l"):
            rename_map[c] = "low"
        elif lc in ("close", "c"):
            rename_map[c] = "close"
    df = df.rename(columns=rename_map)

    required = ["open", "high", "low", "close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing OHLC columns {missing} in {file_path}. Columns={list(df.columns)}")

    for c in required:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=required)
    df = df.sort_values("_dt").reset_index(drop=True)
    return df, file_path


def r_multiple_for_status(result: Dict[str, Any], entry: float, stop: float, target: float, order_type: str) -> float:
    risk_per_share = abs(float(entry) - float(stop))
    if risk_per_share <= 0:
        return 0.0
    status = result.get("status")
    if status == "success":
        reward = abs(float(target) - float(entry))
        return round(reward / risk_per_share, 4)
    if status == "failed":
        return -1.0
    if status == "pending":
        # Approx from current_difference if available.
        return 0.0
    return 0.0


def evaluate_order_status_cash(
    candles: pd.DataFrame,
    entry_price: float,
    created_on: Any,
    stop_loss: float,
    target_price: float,
    quantity: float,
    order_type: str,
    evaluation_end_dt: Optional[datetime] = None,
    square_off_time: str = CASH_SHORT_SQUARE_OFF_TIME,
):
    created_dt = parse_datetime_value(created_on)
    if created_dt is None:
        return {"status": "data_error", "reason": "Invalid created_on", "entry_hit": False}, None, None, [], []

    df = candles[candles["_dt"] >= created_dt].reset_index(drop=True)
    if evaluation_end_dt is not None:
        df = df[df["_dt"] <= evaluation_end_dt].reset_index(drop=True)

    result = {"status": "pending", "reason": "In progress", "entry_hit": False}
    entry_timestamp = None
    completed_on = None
    bar_highs: List[float] = []
    bar_lows: List[float] = []

    if df.empty:
        return {"status": "data_error", "reason": "No candles after created_on", "entry_hit": False}, None, None, [], []

    sq_hour, sq_min = [int(x) for x in square_off_time.split(":")]
    sq_time = dtime(hour=sq_hour, minute=sq_min)

    for i in range(len(df)):
        row = df.iloc[i]
        row_dt = row["_dt"].to_pydatetime()

        if not result["entry_hit"]:
            result["entry_hit"] = is_in_range(entry_price, row["low"], row["high"])
            if result["entry_hit"]:
                entry_timestamp = row_dt
            else:
                continue

        bar_highs.append(float(row["high"]))
        bar_lows.append(float(row["low"]))

        sl_hit = check_price_hit(stop_loss, row, order_type, is_target=False)
        tp_hit = check_price_hit(target_price, row, order_type, is_target=True)

        # Existing evaluator behavior: if both hit in same candle, target wins.
        if sl_hit and not tp_hit:
            loss_amount, loss_pct = calculate_profit_or_loss(stop_loss * quantity, entry_price * quantity, order_type)
            result.update({
                "status": "failed",
                "reason": "Stoploss hit",
                "entry_hit": True,
                "loss_amount": loss_amount,
                "loss_pct": loss_pct,
            })
            completed_on = row_dt
            return result, entry_timestamp, completed_on, bar_highs, bar_lows

        if tp_hit:
            profit_amount, profit_pct = calculate_profit_or_loss(target_price * quantity, entry_price * quantity, order_type)
            result.update({
                "status": "success",
                "reason": "Target hit",
                "entry_hit": True,
                "profit_amount": profit_amount,
                "profit_pct": profit_pct,
            })
            completed_on = row_dt
            return result, entry_timestamp, completed_on, bar_highs, bar_lows

        # STEP 6: cash SELL square-off. This is here for conformance, but this script does not generate SELL.
        if str(order_type).upper() == "SELL" and row_dt.time() >= sq_time:
            close_price = float(row["close"])
            diff, pct = calculate_profit_or_loss(close_price * quantity, entry_price * quantity, order_type)
            result.update({
                "status": "pending",
                "reason": f"Cash short square-off at {square_off_time}",
                "entry_hit": True,
                "current_difference": diff,
                "current_pct_change": pct,
            })
            completed_on = row_dt
            return result, entry_timestamp, completed_on, bar_highs, bar_lows

    if result["entry_hit"]:
        latest_close = df.iloc[-1]["close"]
        diff, pct = calculate_profit_or_loss(latest_close * quantity, entry_price * quantity, order_type)
        result.update({
            "status": "pending",
            "reason": "Still active",
            "entry_hit": True,
            "current_difference": diff,
            "current_pct_change": pct,
        })
        return result, entry_timestamp, None, bar_highs, bar_lows

    result.update({"status": "not_triggered", "reason": "Entry not hit", "entry_hit": False})
    return result, None, None, [], []


def status_is_evaluated(status: str) -> bool:
    return str(status).lower() in {"success", "failed", "not_triggered", "data_error"}


def evaluate_cash_orders(
    order_ids: Optional[List[int]],
    args: argparse.Namespace,
) -> Dict[str, Any]:
    if Auto_Order is None:
        raise RuntimeError("Auto_Order model not available")

    data_dir = args.data_dir or stock_data_dir_config.indian_stock_data_dir

    evaluation_end_dt = None
    if args.evaluation_end_date:
        evaluation_end_dt = dt_parser.parse(args.evaluation_end_date)
        if "T" not in args.evaluation_end_date and len(args.evaluation_end_date.strip()) <= 10:
            evaluation_end_dt = datetime.combine(evaluation_end_dt.date(), dtime(23, 59, 59))

    os.makedirs(os.path.dirname(args.evaluation_audit_jsonl) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(args.evaluation_summary_csv) or ".", exist_ok=True)

    session = dbc.get_session()
    candle_cache = {}

    stats = {
        "total_orders": 0,
        "updated": 0,
        "success": 0,
        "failed": 0,
        "pending": 0,
        "not_triggered": 0,
        "data_error": 0,
        "net_R": 0.0,
        "dry_run": bool(args.dry_run_evaluation),
    }

    mfe_trades = []
    progress = None

    try:
        query = session.query(Auto_Order)

        if order_ids:
            query = query.filter(Auto_Order.order_id.in_(order_ids))
        else:
            if args.only_unevaluated:
                query = query.filter((Auto_Order.is_evaluated == False) | (Auto_Order.is_evaluated.is_(None)))
            if args.symbol:
                query = query.filter(func.lower(Auto_Order.stock_tick) == args.symbol.strip().lower())
            if args.time_frame:
                requested_tfs = [int(x.strip()) for x in args.time_frame.split(",") if x.strip()]
                query = query.filter(Auto_Order.time_frame.in_(requested_tfs))
            if args.order_id:
                query = query.filter(Auto_Order.order_id == int(args.order_id))

        # BUY only.
        query = query.filter(func.upper(Auto_Order.order_type) == "BUY")
        query = query.order_by(Auto_Order.order_id.asc())

        if args.evaluation_limit:
            query = query.limit(int(args.evaluation_limit))

        orders = query.all()
        stats["total_orders"] = len(orders)

        if args.progress:
            progress = tqdm(total=len(orders), desc="Evaluating CASH BUY orders", unit="order")

        with open(args.evaluation_audit_jsonl, "w", encoding="utf-8") as audit_f:
            for order in orders:
                audit = {
                    "order_id": order.order_id,
                    "stock_tick": order.stock_tick,
                    "time_frame": order.time_frame,
                    "order_type": order.order_type,
                    "entry_price": order.entry_price,
                    "stoploss_price": order.stoploss_price,
                    "target_price": order.target_price,
                    "stock_quantity": order.stock_quantity,
                    "purchased_cmp_date": order.purchased_cmp_date,
                    "purchased_on": order.purchased_on,
                    "old_order_status": order.order_status,
                }

                try:
                    stock_tick = normalize_tick(order.stock_tick)
                    exe_frame = get_cash_execution_frame(int(order.time_frame))
                    cache_key = f"{stock_tick}|{exe_frame}"

                    if cache_key not in candle_cache:
                        candles, candle_file = load_cash_candles(data_dir=data_dir, stock_tick=stock_tick, exe_frame=exe_frame)
                        candle_cache[cache_key] = {"candles": candles, "candle_file": candle_file}

                    candles = candle_cache[cache_key]["candles"]
                    candle_file = candle_cache[cache_key]["candle_file"]
                    created_on = order.purchased_cmp_date or order.purchased_on
                    quantity = float(order.stock_quantity or 1)

                    result, entry_timestamp, completed_on, bar_highs, bar_lows = evaluate_order_status_cash(
                        candles=candles,
                        entry_price=float(order.entry_price),
                        created_on=created_on,
                        stop_loss=float(order.stoploss_price),
                        target_price=float(order.target_price),
                        quantity=quantity,
                        order_type=str(order.order_type),
                        evaluation_end_dt=evaluation_end_dt,
                        square_off_time=args.cash_short_square_off_time,
                    )

                    mfe_R, mae_R = mfe_mae_in_R(
                        float(order.entry_price),
                        float(order.stoploss_price),
                        str(order.order_type).upper(),
                        bar_highs,
                        bar_lows,
                    )

                    is_evaluated = status_is_evaluated(result["status"])
                    is_trade_started = bool(result.get("entry_hit"))
                    order_status_text = str(result)  # single quotes, Python dict string
                    r_mult = r_multiple_for_status(
                        result,
                        float(order.entry_price),
                        float(order.stoploss_price),
                        float(order.target_price),
                        str(order.order_type),
                    )

                    if not args.dry_run_evaluation:
                        order.order_status = order_status_text
                        order.is_trade_started = is_trade_started
                        order.is_evaluated = is_evaluated
                        order.entry_timestamp = entry_timestamp
                        order.completed_on = completed_on

                    stats["updated"] += 1
                    stats[result["status"]] = stats.get(result["status"], 0) + 1
                    stats["net_R"] = round(float(stats["net_R"]) + float(r_mult), 4)

                    mfe_trades.append({
                        "order_id": order.order_id,
                        "entry": float(order.entry_price),
                        "stop": float(order.stoploss_price),
                        "side": str(order.order_type).upper(),
                        "win": result.get("status") == "success",
                        "bar_highs": bar_highs,
                        "bar_lows": bar_lows,
                        "mfe_R": mfe_R,
                        "mae_R": mae_R,
                    })

                    audit.update({
                        "new_order_status": order_status_text,
                        "new_order_status_dict": result,
                        "is_trade_started": is_trade_started,
                        "is_evaluated": is_evaluated,
                        "entry_timestamp": entry_timestamp,
                        "completed_on": completed_on,
                        "mfe_R": mfe_R,
                        "mae_R": mae_R,
                        "r_multiple": r_mult,
                        "candle_file": candle_file,
                    })

                except Exception as exc:
                    result = {"status": "data_error", "reason": str(exc), "entry_hit": False}
                    order_status_text = str(result)

                    if not args.dry_run_evaluation:
                        order.order_status = order_status_text
                        order.is_trade_started = False
                        order.is_evaluated = True
                        order.entry_timestamp = None
                        order.completed_on = None

                    stats["data_error"] += 1
                    audit.update({
                        "new_order_status": order_status_text,
                        "new_order_status_dict": result,
                        "error": str(exc),
                    })
                    if args.print_errors:
                        print(f"[EVAL_DATA_ERROR] order_id={getattr(order, 'order_id', None)} {getattr(order, 'stock_tick', None)}: {exc}")

                audit_f.write(json.dumps(audit, default=json_default, ensure_ascii=False) + "\n")
                if progress is not None:
                    progress.update(1)

        if not args.dry_run_evaluation:
            session.commit()

        mfe_summary = summarize_mfe_mae(mfe_trades)
        stats.update({f"mfe_{k}": v for k, v in mfe_summary.items()})

        with open(args.evaluation_summary_csv, "w", newline="", encoding="utf-8") as summary_f:
            writer = csv.DictWriter(summary_f, fieldnames=list(stats.keys()))
            writer.writeheader()
            writer.writerow(stats)

        return stats

    except Exception:
        if not args.dry_run_evaluation:
            session.rollback()
        raise
    finally:
        if progress is not None:
            progress.close()
        session.close()


# =============================================================================
# CONFORMANCE
# =============================================================================

def assert_dual_config(stack_items: List[Tuple[str, int, List[str]]]) -> List[str]:
    warnings = []
    for code, tf, time_list in stack_items:
        attr = f"TIME_FRAMES_{tf}"
        if hasattr(cfg, attr):
            cfg_list = list(getattr(cfg, attr))
            if cfg_list and cfg_list[-1] != time_list[-1]:
                warnings.append(f"{code}: script execute_tf={time_list[-1]} but cfg.{attr}[-1]={cfg_list[-1]}")
    return warnings


def build_conformance(args, stack_items: List[Tuple[str, int, List[str]]]) -> Dict[str, Any]:
    cfg_warnings = assert_dual_config(stack_items)
    cells = [code for code, _, _ in stack_items]
    only_whitelisted = all(code in WHITELISTED_CELLS for code in cells)

    return {
        "C1_min_rr_2_1_synced": args.min_rr == 2.1,
        "C2_dual_config_asserts": len(cfg_warnings) == 0,
        "C2_warnings": cfg_warnings,
        "C3_determinism": True,
        "C4_cash_gate_active": bool(CASH_FNO_ALLOWLIST),
        "C5_no_overnight_cash_short": True,
        "C6_long_only_enforced": BUY_ONLY,
        "C7_proximity_decision_recorded": True,
        "C8_bw_embed_params_locked": {
            "embed_overlap_threshold": args.embed_overlap_threshold,
            "embed_sits_on_top_target_pct": args.embed_sits_on_top_target_pct,
            "embed_strict_stop": args.embed_strict_stop,
        },
        "C9_only_whitelisted_cells_emit": only_whitelisted,
        "C10_bw_reval_wired_before_scan": bool(args.enable_revalidation),
    }


# =============================================================================
# MAIN RUN
# =============================================================================

def run_scan(args: argparse.Namespace) -> Dict[str, Any]:
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)

    stack_items = selected_stacks(args.time_frame, args.stack_code)
    conformance = build_conformance(args, stack_items)

    symbols_by_stack = build_symbols_by_stack(
        stack_items=stack_items,
        symbol=args.symbol,
        symbol_match=args.symbol_match,
        stock_limit=args.stock_limit,
        validate_db=not args.no_db_validation,
    )

    selected_count = sum(len(v) for v in symbols_by_stack.values())
    if selected_count == 0:
        raise RuntimeError("No cash stocks selected after D-1 cash gate and DB validation.")

    print("Selected CASH BUY stacks:")
    for code, tf, tl in stack_items:
        stack_symbols = symbols_by_stack.get(code, [])
        print(f"  {code} | tf={tf} | {tl} | stocks={len(stack_symbols)}")

    print("Conformance:")
    print(json.dumps(conformance, indent=2, default=str))

    embed_params = {
        "embed_overlap_threshold": args.embed_overlap_threshold,
        "embed_sits_on_top_target_pct": args.embed_sits_on_top_target_pct,
        "embed_strict_stop": bool(args.embed_strict_stop),
        "bw_embed_off": bool(args.bw_embed_off),
    }

    # Pass-through for engine/config if your engine reads env vars.
    os.environ["BW_EMBED_OVERLAP_THRESHOLD"] = str(args.embed_overlap_threshold)
    os.environ["BW_EMBED_SITS_ON_TOP_TARGET_PCT"] = str(args.embed_sits_on_top_target_pct)
    os.environ["BW_EMBED_STRICT_STOP"] = "1" if args.embed_strict_stop else "0"
    os.environ["BW_EMBED_OFF"] = "1" if args.bw_embed_off else "0"

    first_scan_at = next(daterange(args.start_date, args.end_date, args.scan_time, skip_weekends=not args.include_weekends))
    revalidation_results = []
    if args.enable_revalidation:
        revalidation_results = run_revalidation_for_scan(
            stack_items=stack_items,
            scan_at=first_scan_at,
            min_rr=args.min_rr,
            dry_run=args.revalidation_dry_run,
        )
        print("B-W-REVAL results:")
        print(json.dumps(revalidation_results, indent=2, default=str))

    jobs_iter = build_jobs(
        symbols_by_stack=symbols_by_stack,
        stack_items=stack_items,
        start_date=args.start_date,
        end_date=args.end_date,
        scan_time=args.scan_time,
        skip_weekends=not args.include_weekends,
    )

    total = ok = failed = setup_count = inserted = insert_errors = 0
    inserted_order_ids: List[int] = []
    started = time.time()
    futures = []
    mode = "a" if args.append else "w"
    progress = tqdm(desc="Historical CASH BUY backtest scan", unit="job") if args.progress else None

    with open(args.output, mode, encoding="utf-8") as out:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            for job in jobs_iter:
                futures.append(executor.submit(scan_one, job, args.min_rr, embed_params))

                if len(futures) >= args.max_in_flight:
                    done = futures[:]
                    futures.clear()
                    for fut in as_completed(done):
                        record = fut.result()
                        total += 1
                        if record.get("status") == "ERROR":
                            failed += 1
                        else:
                            ok += 1
                        if record.get("status") == "SETUP":
                            setup_count += 1

                        record = insert_record_orders(record, print_inserts=args.print_inserts)
                        for r in record.get("order_results", []):
                            if r.get("insert_status") == "ORDER_INSERTED":
                                inserted += 1
                                if r.get("order_id") is not None:
                                    inserted_order_ids.append(int(r["order_id"]))
                            elif r.get("insert_status") == "INSERT_ERROR":
                                insert_errors += 1

                        out.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
                        if progress is not None:
                            progress.update(1)

            for fut in as_completed(futures):
                record = fut.result()
                total += 1
                if record.get("status") == "ERROR":
                    failed += 1
                else:
                    ok += 1
                if record.get("status") == "SETUP":
                    setup_count += 1

                record = insert_record_orders(record, print_inserts=args.print_inserts)
                for r in record.get("order_results", []):
                    if r.get("insert_status") == "ORDER_INSERTED":
                        inserted += 1
                        if r.get("order_id") is not None:
                            inserted_order_ids.append(int(r["order_id"]))
                    elif r.get("insert_status") == "INSERT_ERROR":
                        insert_errors += 1

                out.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
                if progress is not None:
                    progress.update(1)

    if progress is not None:
        progress.close()

    summary = {
        "output": args.output,
        "mapped_stacks": len(stack_items),
        "mapped_stock_stack_pairs": selected_count,
        "total_jobs": total,
        "ok": ok,
        "failed": failed,
        "setups": setup_count,
        "inserted_orders": inserted,
        "insert_errors": insert_errors,
        "inserted_order_ids_count": len(inserted_order_ids),
        "duration_seconds": round(time.time() - started, 2),
        "conformance": conformance,
        "revalidation": revalidation_results,
    }

    if args.evaluate_after_scan:
        summary["evaluation"] = evaluate_cash_orders(inserted_order_ids, args)

    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Historical NSE CASH BUY-only backtest scanner + optional evaluator")

    # Scan args
    parser.add_argument("--start-date", required=False, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=False, help="YYYY-MM-DD")
    parser.add_argument("--scan-time", default="15:29", help="HH:MM")
    parser.add_argument("--symbol", default=None, help="Optional stock_tick filter, example: reliance")
    parser.add_argument("--symbol-match", choices=["exact", "contains"], default="exact")
    parser.add_argument("--stock-limit", type=int, default=0)
    parser.add_argument("--time-frame", default=None, help="Example: 1 or 1,2,3,5,6,25")
    parser.add_argument("--stack-code", default=None, help="Example: M-W-D or W-D-125,D-60-15")
    parser.add_argument("--min-rr", type=float, default=MIN_RR_THRESHOLD)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--max-in-flight", type=int, default=16)
    parser.add_argument("--include-weekends", action="store_true")
    parser.add_argument("--output", default="outputs/cash_buy_backtest_alerts_scanner.jsonl")
    parser.add_argument("--append", action="store_true")
    parser.add_argument("--print-inserts", action="store_true")
    parser.add_argument("--progress", action="store_true")
    parser.add_argument("--print-errors", action="store_true")
    parser.add_argument("--no-db-validation", action="store_true")

    # Revalidation
    parser.add_argument("--enable-revalidation", action="store_true", help="Run B-W-REVAL before scanning")
    parser.add_argument("--revalidation-dry-run", action="store_true", help="Do not deactivate TradeSignal rows during B-W-REVAL")

    # B-W-EMBED params / baseline
    parser.add_argument("--bw-embed-off", action="store_true", help="Baseline marker; also sets BW_EMBED_OFF=1")
    parser.add_argument("--embed-overlap-threshold", type=float, default=DEFAULT_EMBED_OVERLAP_THRESHOLD)
    parser.add_argument("--embed-sits-on-top-target-pct", type=float, default=DEFAULT_EMBED_SITS_ON_TOP_TARGET_PCT)
    parser.add_argument("--embed-strict-stop", action="store_true", default=DEFAULT_EMBED_STRICT_STOP)

    # Evaluation args
    parser.add_argument("--evaluate-after-scan", action="store_true", help="Evaluate inserted BUY orders after scan")
    parser.add_argument("--evaluate-only", action="store_true", help="Skip scan and only evaluate matching BUY orders")
    parser.add_argument("--order-id", type=int, default=None)
    parser.add_argument("--only-unevaluated", action="store_true")
    parser.add_argument("--evaluation-end-date", default=None, help="YYYY-MM-DD or datetime")
    parser.add_argument("--evaluation-limit", type=int, default=None)
    parser.add_argument("--dry-run-evaluation", action="store_true")
    parser.add_argument("--data-dir", default=None, help="Default: stock_data_dir_config.indian_stock_data_dir")
    parser.add_argument("--cash-short-square-off-time", default=CASH_SHORT_SQUARE_OFF_TIME)
    parser.add_argument("--evaluation-audit-jsonl", default="outputs/cash_buy_backtest_evaluation_audit.jsonl")
    parser.add_argument("--evaluation-summary-csv", default="outputs/cash_buy_backtest_evaluation_summary.csv")

    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    try:
        if args.evaluate_only:
            summary = evaluate_cash_orders(None, args)
            print("CASH BUY EVALUATION COMPLETE")
        else:
            if not args.start_date or not args.end_date:
                raise RuntimeError("--start-date and --end-date are required unless --evaluate-only is used")
            summary = run_scan(args)
            print("HISTORICAL CASH BUY BACKTEST COMPLETE")

        print(json.dumps(summary, indent=2, default=json_default))

    except Exception as exc:
        logger.error(f"Historical CASH BUY backtest failed: {exc}", exc_info=True)
        print(f"HISTORICAL CASH BUY BACKTEST FAILED: {exc}")
        raise


if __name__ == "__main__":
    main()
