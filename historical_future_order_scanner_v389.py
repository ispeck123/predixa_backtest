"""
Parallel historical Futures Contract scanner -> existing future_order_master.

Updated futures version:
- BUY and SELL are both enabled by default.
- Uses ProcessPoolExecutor workers for engine calculation only.
- Database writes use ONLY the parent process through
  insert_future_and_commodity_order(..., segment="future").
- Worker always calls process_setup_fc(..., is_future=True).
- No cash allowlist / D-1 cash gate is applied here.
- Keeps full setup audit JSONL for later quality-slim JSONL recovery.
- Supports side selection: BOTH / BUY / SELL.
- Supports single or multiple futures timeframes by --time-frame or --stack-code.

Run from the PROJECT ROOT on Ubuntu:
    python backtest/historical_future_order_scanner_v389.py --start-date 2026-06-01 --end-date 2026-07-06 --side BOTH
"""

from __future__ import annotations

import argparse
import copy
import json
import multiprocessing as mp
import os
import sys
import time
import traceback
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import date, datetime, time as clock_time, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Tuple

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.setup_engine_new import format_calculate_setup_response, process_setup_fc
from scripts.side_enablement_policy import SIDE_POLICY, Side, resolve_segment
from shared.utils.logger import logger


# ----------------------------------------------------------------------
# FUTURES UNIVERSE
# ----------------------------------------------------------------------

FIXED_FUTURES_EXPIRIES: Dict[str, List[str]] = {
    "BANKNIFTY": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "NIFTY": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "WIPRO": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "CANBK": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "PNB": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "HDFCBANK": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "ASHOKLEY": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "ETERNAL": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "MOTHERSON": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "TATASTEEL": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "GMRAIRPORT": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "SAIL": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "KOTAKBANK": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "BANKBARODA": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "UNIONBANK": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "JIOFIN": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "BANDHANBNK": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "INFY": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "NBCC": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "ITC": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "BHEL": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "IOC": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "BANKINDIA": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "RELIANCE": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "BEL": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "ICICIBANK": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "KALYANKJIL": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "ONGC": ["2026-07-28", "2026-08-25", "2026-09-29"],
    "NTPC": ["2026-07-28", "2026-08-25", "2026-09-29"],
}


# ----------------------------------------------------------------------
# FUTURES TIMEFRAME CELLS
# ----------------------------------------------------------------------

FUTURE_STACKS: Dict[str, Dict[str, Any]] = {
    "M-W-D": {
        "time_frame": 1,
        "time_list": ["monthly", "weekly", "daily"],
    },
    "W-D-125": {
        "time_frame": 5,
        "time_list": ["weekly", "daily", "one_twenty_five"],
    },
    "W-D-60": {
        "time_frame": 2,
        "time_list": ["weekly", "daily", "sixty"],
    },
    "W-D-75": {
        "time_frame": 25,
        "time_list": ["weekly", "daily", "seventy_five"],
    },
    "D-60-15": {
        "time_frame": 3,
        "time_list": ["daily", "sixty", "fifteen"],
    },
    "W-125-25": {
        "time_frame": 6,
        "time_list": ["weekly", "one_twenty_five", "twenty_five"],
    },
}

FUTURE_TIME_FRAME_IDS: Dict[str, int] = {
    "daily": 1,
    "sixty": 2,
    "fifteen": 3,
    "one_twenty_five": 5,
    "twenty_five": 6,
    "seventy_five": 25,
}

BACKTEST_STATUS_PENDING_ENTRY = "BACKTEST_PENDING_ENTRY"
MIN_RRR_DEFAULT = 2.1


@dataclass(frozen=True)
class ScanTask:
    scan_at: datetime
    symbol: str
    expiry_date: date
    exp_num: str
    stack_code: str
    time_list: Tuple[str, str, str]
    time_frame: int
    min_rrr: float
    side_mode: str
    allow_sell: bool
    stock_id: Optional[int]


@dataclass
class ParallelFutureScanJob:
    start_date: date
    end_date: date
    stack_items: List[Tuple[str, int, List[str]]]
    country_id: int = 1
    scan_time: clock_time = clock_time(15, 29)
    min_rrr: float = MIN_RRR_DEFAULT
    min_days_before_expiry: int = 5
    step_days: int = 1
    weekdays_only: bool = True
    side_mode: str = "BOTH"
    respect_sell_policy: bool = False
    symbol_filter: Optional[str] = None
    output_path: str = "outputs/future_backtest_order_scanner_v389.jsonl"
    workers: int = 4
    max_in_flight: int = 16
    progress_every: int = 25
    start_method: str = "spawn"
    fixed_expiry_map: Dict[str, List[str]] = field(default_factory=lambda: copy.deepcopy(FIXED_FUTURES_EXPIRIES))


def json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


def json_dumps(value: Any) -> str:
    return json.dumps(value, default=json_default, ensure_ascii=False)


def to_builtin(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): to_builtin(child) for key, child in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_builtin(child) for child in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def parse_iso_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError("date must be YYYY-MM-DD") from exc


def parse_clock_time(value: str) -> clock_time:
    try:
        return datetime.strptime(value, "%H:%M").time()
    except ValueError as exc:
        raise argparse.ArgumentTypeError("scan time must be HH:MM, for example 15:29") from exc


def normalise_tf_name(value: Any) -> str:
    return str(value).strip().lower()


def normalise_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def normalise_key(key: Any) -> str:
    return "".join(character for character in str(key).upper() if character.isalnum())


def safe_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def selected_stacks(time_frame: Optional[str], stack_code: Optional[str]) -> List[Tuple[str, int, List[str]]]:
    items: List[Tuple[str, int, List[str]]] = []

    if stack_code:
        requested_codes = [x.strip() for x in stack_code.split(",") if x.strip()]
        for code in requested_codes:
            if code not in FUTURE_STACKS:
                raise ValueError(f"Unknown stack code: {code}. Valid: {', '.join(FUTURE_STACKS.keys())}")
            meta = FUTURE_STACKS[code]
            items.append((code, int(meta["time_frame"]), list(meta["time_list"])))
        return items

    if time_frame:
        requested_tfs = {int(x.strip()) for x in time_frame.split(",") if x.strip()}
        for code, meta in FUTURE_STACKS.items():
            if int(meta["time_frame"]) in requested_tfs:
                items.append((code, int(meta["time_frame"]), list(meta["time_list"])))
        if not items:
            raise ValueError(f"No futures stacks found for --time-frame {time_frame}. Valid: 1,2,3,5,6,25")
        return items

    return [(code, int(meta["time_frame"]), list(meta["time_list"])) for code, meta in FUTURE_STACKS.items()]


def validate_stack_item(item: Tuple[str, int, List[str]]) -> Tuple[str, int, List[str]]:
    code, time_frame, time_list = item

    if not isinstance(time_list, list) or len(time_list) != 3:
        raise ValueError(f"{code} must contain exactly three timeframes")

    normalized = [normalise_tf_name(x) for x in time_list]
    execute_tf = normalized[-1]

    if execute_tf not in FUTURE_TIME_FRAME_IDS:
        raise ValueError(
            f"Unsupported futures execute timeframe {execute_tf!r}. "
            f"Allowed: {', '.join(FUTURE_TIME_FRAME_IDS)}"
        )

    expected_time_frame = FUTURE_TIME_FRAME_IDS[execute_tf]
    if int(time_frame) != int(expected_time_frame):
        raise ValueError(
            f"Dual-config assert failed for {code}: configured time_frame={time_frame}, "
            f"but execute_tf={execute_tf} maps to {expected_time_frame}"
        )

    return code, int(time_frame), normalized


def validate_stack_items(items: List[Tuple[str, int, List[str]]]) -> List[Tuple[str, int, List[str]]]:
    return [validate_stack_item(item) for item in items]


def flatten_mapping(value: Any, prefix: str = "") -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    if isinstance(value, Mapping):
        for key, child in value.items():
            full_key = f"{prefix}_{key}" if prefix else str(key)
            if isinstance(child, Mapping):
                result.update(flatten_mapping(child, full_key))
            else:
                result[normalise_key(full_key)] = child
                result.setdefault(normalise_key(key), child)
    return result


def price_from_setup(payload: Mapping[str, Any], side: str, field: str) -> Optional[float]:
    field_aliases = {
        "entry_price": ("ENTRYPRICE", "ENTRY", "ENTRYPOINT", "PRICE"),
        "stop_loss": ("STOPLOSS", "STOPLOSSPRICE", "SL", "SLPRICE", "STOP"),
        "target_price": ("TARGETPRICE", "TARGET", "TARGET1", "TP", "TAKEPROFIT"),
    }[field]
    flattened = flatten_mapping(payload)
    side_key = normalise_key(side)

    for alias in field_aliases:
        for candidate in (side_key + alias, alias):
            output = safe_float(flattened.get(candidate))
            if output is not None:
                return output
    return None


def get_trade_dict(formatted: Mapping[str, Any], side: str) -> Dict[str, float]:
    output = {
        "entry_price": price_from_setup(formatted, side, "entry_price"),
        "stop_loss": price_from_setup(formatted, side, "stop_loss"),
        "target_price": price_from_setup(formatted, side, "target_price"),
    }

    missing = [key for key, value in output.items() if value is None]
    if missing:
        raise ValueError(
            f"{side} setup is missing: {', '.join(missing)}. "
            f"Available keys: {sorted(flatten_mapping(formatted).keys())}"
        )

    return {key: float(value) for key, value in output.items()}


def side_allowed(side: str, side_mode: str, allow_sell: bool) -> bool:
    side = side.upper()
    side_mode = side_mode.upper()

    if side_mode == "BUY":
        return side == "BUY"
    if side_mode == "SELL":
        return side == "SELL" and allow_sell
    if side_mode == "BOTH":
        if side == "BUY":
            return True
        if side == "SELL":
            return allow_sell

    return False


def qualified_sides(formatted: Mapping[str, Any], min_rrr: float, side_mode: str, allow_sell: bool) -> List[Tuple[str, float]]:
    qualified: List[Tuple[str, float]] = []

    buy_rrr = safe_float(formatted.get("BUY_RRR"))
    if side_allowed("BUY", side_mode, allow_sell) and "BUY" in formatted and buy_rrr is not None and buy_rrr >= min_rrr:
        qualified.append(("BUY", buy_rrr))

    sell_rrr = safe_float(formatted.get("SELL_RRR"))
    if side_allowed("SELL", side_mode, allow_sell) and "SELL" in formatted and sell_rrr is not None and sell_rrr >= min_rrr:
        qualified.append(("SELL", sell_rrr))

    return qualified


def get_selected_zone_meta(formatted: Mapping[str, Any], side: str) -> Dict[str, Any]:
    zones_x = formatted.get("ZONES_X") if isinstance(formatted.get("ZONES_X"), Mapping) else {}
    side_key = "Buy" if side == "BUY" else "Sell"
    zones = zones_x.get(side_key) if isinstance(zones_x, Mapping) else None

    if not isinstance(zones, list) or not zones:
        return {}

    zone = zones[0]
    if not isinstance(zone, Mapping):
        return {}

    meta = zone.get("meta") if isinstance(zone.get("meta"), Mapping) else {}

    return {
        "final_score": meta.get("final_score", zone.get("final_score")),
        "zone_v38_score": meta.get("zone_v38_score", zone.get("zone_v38_score")),
        "gap_composite_score": meta.get("gap_composite_score", zone.get("gap_composite_score")),
        "quality_priority": meta.get("quality_priority", zone.get("quality_priority")),
        "age_class": meta.get("age_class", zone.get("age_class")),
        "pattern_validated": meta.get("pattern_validated", zone.get("pattern_validated")),
        "state": meta.get("state", zone.get("state")),
        "nesting_tier": meta.get("nesting_tier", zone.get("nesting_tier")),
        "ztype": meta.get("ztype", zone.get("ztype")),
        "final_weighted_score": meta.get("final_weighted_score", zone.get("final_weighted_score")),
    }


def process_scan_task(task: ScanTask) -> Dict[str, Any]:
    started = time.perf_counter()

    record: Dict[str, Any] = {
        "scan_at": task.scan_at,
        "symbol": task.symbol,
        "expiry_date": task.expiry_date,
        "exp_num": task.exp_num,
        "stack_code": task.stack_code,
        "time_list": list(task.time_list),
        "execute_tf": task.time_list[-1],
        "time_frame": task.time_frame,
        "stock_id": task.stock_id,
        "segment": "future",
        "side_mode": task.side_mode,
        "min_rrr": task.min_rrr,
        "cash_gate_applied": False,
        "data_source": "process_setup_fc(is_future=True): futures data branch",
    }

    try:
        raw = process_setup_fc(
            task.symbol,
            list(task.time_list),
            task.exp_num,
            task.scan_at,
            True,
        )

        if isinstance(raw, str):
            raise RuntimeError(raw)

        formatted = format_calculate_setup_response(
            raw,
            stock_name=task.symbol,
            time_fr=task.time_frame,
            exp_num=task.exp_num,
            last_d_time=task.scan_at,
            is_future=True,
        )

        if not isinstance(formatted, Mapping):
            raise RuntimeError("format_calculate_setup_response must return a dictionary")

        setups: List[Dict[str, Any]] = []

        for side, rrr in qualified_sides(
            formatted=formatted,
            min_rrr=task.min_rrr,
            side_mode=task.side_mode,
            allow_sell=task.allow_sell,
        ):
            trade_dict = get_trade_dict(formatted, side)
            setups.append({
                "trade_type": side,
                "rrr": rrr,
                "trade_dict": trade_dict,
                "selected_zone_meta": get_selected_zone_meta(formatted, side),
            })

        record.update({
            "status": "SETUP" if setups else "NO_SETUP",
            "setups": setups,
            "setup": to_builtin(formatted),
            "worker_seconds": round(time.perf_counter() - started, 4),
        })
        return record

    except Exception as exc:
        record.update({
            "status": "ERROR",
            "error": str(exc),
            "traceback": traceback.format_exc(limit=8),
            "worker_seconds": round(time.perf_counter() - started, 4),
        })
        return record


def resolve_future_stock_id(session: Any, symbol: str, expiry_date: date) -> Optional[int]:
    from shared.db.db_model import FuturesMaster

    rows = session.query(FuturesMaster).filter(FuturesMaster.symbol == symbol).all()
    if not rows:
        return None

    for row in rows:
        raw_expiry = getattr(row, "expiry_date", None)

        if isinstance(raw_expiry, datetime):
            raw_expiry = raw_expiry.date()

        if isinstance(raw_expiry, date) and raw_expiry == expiry_date:
            return row.id

        if raw_expiry is not None and str(raw_expiry)[:10] == expiry_date.isoformat():
            return row.id

    return rows[0].id


def preload_stock_ids(job: ParallelFutureScanJob) -> Dict[Tuple[str, date], Optional[int]]:
    output: Dict[Tuple[str, date], Optional[int]] = {}

    from shared.db.dbconn import DBConnection

    db = DBConnection()
    session = db.get_session()
    symbol_filter = normalise_symbol(job.symbol_filter) if job.symbol_filter else None

    try:
        for symbol, expiry_texts in job.fixed_expiry_map.items():
            symbol = normalise_symbol(symbol)

            if symbol_filter and symbol != symbol_filter:
                continue

            for expiry_text in expiry_texts:
                expiry = parse_iso_date(expiry_text)
                output[(symbol, expiry)] = resolve_future_stock_id(session, symbol, expiry)

        return output

    finally:
        session.close()
        try:
            db.close_engine()
        except Exception:
            pass


def iter_days(start_date: date, end_date: date, step_days: int, weekdays_only: bool) -> Iterable[date]:
    current = start_date
    while current <= end_date:
        if not weekdays_only or current.weekday() < 5:
            yield current
        current += timedelta(days=step_days)


def build_task_iterator(job: ParallelFutureScanJob, stock_ids: Mapping[Tuple[str, date], Optional[int]]) -> Iterator[ScanTask]:
    symbol_filter = normalise_symbol(job.symbol_filter) if job.symbol_filter else None
    segment = resolve_segment(is_future=True)

    if job.respect_sell_policy:
        allow_sell = SIDE_POLICY.is_enabled(segment, Side.SHORT)
    else:
        allow_sell = True

    for scan_day in iter_days(job.start_date, job.end_date, job.step_days, job.weekdays_only):
        scan_at = datetime.combine(scan_day, job.scan_time)

        for symbol, expiry_texts in job.fixed_expiry_map.items():
            symbol = normalise_symbol(symbol)

            if symbol_filter and symbol != symbol_filter:
                continue

            for expiry_text in expiry_texts:
                expiry = parse_iso_date(expiry_text)

                if (expiry - scan_day).days <= job.min_days_before_expiry:
                    continue

                exp_num = expiry.strftime("%d%m%Y")
                stock_id = stock_ids.get((symbol, expiry))

                for stack_code, time_frame, time_list in job.stack_items:
                    yield ScanTask(
                        scan_at=scan_at,
                        symbol=symbol,
                        expiry_date=expiry,
                        exp_num=exp_num,
                        stack_code=stack_code,
                        time_list=(time_list[0], time_list[1], time_list[2]),
                        time_frame=time_frame,
                        min_rrr=job.min_rrr,
                        side_mode=job.side_mode,
                        allow_sell=allow_sell,
                        stock_id=stock_id,
                    )


def count_skipped_near_expiry(job: ParallelFutureScanJob) -> int:
    symbol_filter = normalise_symbol(job.symbol_filter) if job.symbol_filter else None
    skipped = 0

    for scan_day in iter_days(job.start_date, job.end_date, job.step_days, job.weekdays_only):
        for symbol, expiry_texts in job.fixed_expiry_map.items():
            symbol = normalise_symbol(symbol)

            if symbol_filter and symbol != symbol_filter:
                continue

            for expiry_text in expiry_texts:
                expiry = parse_iso_date(expiry_text)
                if (expiry - scan_day).days <= job.min_days_before_expiry:
                    skipped += len(job.stack_items)

    return skipped


def handle_result(result: Dict[str, Any], job: ParallelFutureScanJob, audit: Any, stats: Dict[str, Any], dry_run: bool) -> None:
    status = result.get("status")

    if status == "ERROR":
        stats["errors"] += 1
        audit.write(json_dumps(result) + "\n")
        return

    if status == "NO_SETUP":
        stats["no_setup"] += 1
        audit.write(json_dumps(result) + "\n")
        return

    if status != "SETUP":
        stats["errors"] += 1
        result["status"] = "ERROR"
        result["error"] = f"Unexpected worker status: {status!r}"
        audit.write(json_dumps(result) + "\n")
        return

    setups = result.get("setups", [])
    stats["qualified_setups"] += len(setups)
    insert_records: List[Dict[str, Any]] = []

    from fc_backtest_order_helper import insert_future_and_commodity_order

    for setup in setups:
        insert_record: Dict[str, Any] = {
            "trade_type": setup["trade_type"],
            "rrr": setup["rrr"],
            "trade_dict": setup["trade_dict"],
            "selected_zone_meta": setup.get("selected_zone_meta", {}),
            "insert_status": "DRY_RUN" if dry_run else "NOT_ATTEMPTED",
            "order_id": None,
        }

        if dry_run:
            insert_records.append(insert_record)
            continue

        try:
            insert_result = insert_future_and_commodity_order(
                stock_id=result.get("stock_id"),
                c_id=job.country_id,
                trade_dict=setup["trade_dict"],
                time_fr=int(result["time_frame"]),
                stock_name=result["symbol"],
                dt=result["scan_at"],
                ord_type=setup["trade_type"],
                exp_dt=result["exp_num"],
                prediction="NA",
                probability=0.0,
                trade_id=None,
                segment="future",
                is_backtest=True,
                order_status=BACKTEST_STATUS_PENDING_ENTRY,
            )

            insert_record.update({
                "order_id": insert_result.get("order_id"),
                "insert_status": "ORDER_INSERTED" if insert_result.get("created") else "DUPLICATE_ORDER",
                "insert_reason": insert_result.get("reason"),
            })

            if insert_result.get("created"):
                stats["orders_inserted"] += 1
            else:
                stats["duplicate_orders_skipped"] += 1

        except Exception as exc:
            logger.error(
                "Future backtest order insert failed | %s | %s | %s | %s",
                result.get("symbol"),
                result.get("exp_num"),
                result.get("scan_at"),
                setup.get("trade_type"),
                exc_info=True,
            )
            insert_record.update({
                "insert_status": "INSERT_ERROR",
                "insert_error": str(exc),
            })
            stats["insert_errors"] += 1

        insert_records.append(insert_record)

    audit_record = dict(result)
    audit_record["order_results"] = insert_records
    audit.write(json_dumps(audit_record) + "\n")


def submit_until_full(executor: ProcessPoolExecutor, task_iterator: Iterator[ScanTask], pending: Dict[Future, ScanTask], max_in_flight: int) -> bool:
    exhausted = False

    while len(pending) < max_in_flight and not exhausted:
        try:
            task = next(task_iterator)
        except StopIteration:
            exhausted = True
            break

        future = executor.submit(process_scan_task, task)
        pending[future] = task

    return exhausted


def run(job: ParallelFutureScanJob, dry_run: bool = False) -> Dict[str, Any]:
    if job.start_date > job.end_date:
        raise ValueError("start_date cannot be after end_date")
    if job.step_days < 1:
        raise ValueError("step_days must be at least 1")
    if job.workers < 1:
        raise ValueError("workers must be at least 1")
    if job.max_in_flight < job.workers:
        raise ValueError("max_in_flight must be greater than or equal to workers")
    if job.progress_every < 1:
        raise ValueError("progress_every must be at least 1")

    job.stack_items = validate_stack_items(job.stack_items)

    started = time.time()
    output_path = Path(job.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    stats: Dict[str, Any] = {
        "target_table": "future_order_master",
        "segment": "future",
        "executor": "ProcessPoolExecutor",
        "workers": job.workers,
        "max_in_flight": job.max_in_flight,
        "side_mode": job.side_mode,
        "min_rrr": job.min_rrr,
        "cash_gate_applied": False,
        "time_lists": [
            {"stack_code": code, "time_frame": time_frame, "time_list": time_list}
            for code, time_frame, time_list in job.stack_items
        ],
        "time_frame_mapping": FUTURE_TIME_FRAME_IDS,
        "data_path_mode": "process_setup_fc(is_future=True): futures data branch",
        "engine_calls_submitted": 0,
        "engine_calls_completed": 0,
        "qualified_setups": 0,
        "orders_inserted": 0,
        "duplicate_orders_skipped": 0,
        "no_setup": 0,
        "errors": 0,
        "insert_errors": 0,
        "skipped_near_expiry": count_skipped_near_expiry(job),
        "dry_run": dry_run,
        "output_path": str(output_path),
    }

    stock_ids = preload_stock_ids(job)
    task_iterator = build_task_iterator(job, stock_ids)
    context = mp.get_context(job.start_method)
    pending: Dict[Future, ScanTask] = {}

    with output_path.open("a", encoding="utf-8") as audit:
        with ProcessPoolExecutor(max_workers=job.workers, mp_context=context) as executor:
            exhausted = submit_until_full(executor, task_iterator, pending, job.max_in_flight)
            stats["engine_calls_submitted"] = len(pending)

            while pending:
                done, _ = wait(pending, return_when=FIRST_COMPLETED)

                for future in done:
                    task = pending.pop(future)

                    try:
                        result = future.result()
                    except Exception as exc:
                        logger.error("Future worker crashed for %s: %s", task, exc, exc_info=True)
                        result = {
                            "status": "ERROR",
                            "scan_at": task.scan_at,
                            "symbol": task.symbol,
                            "expiry_date": task.expiry_date,
                            "exp_num": task.exp_num,
                            "stack_code": task.stack_code,
                            "time_list": list(task.time_list),
                            "time_frame": task.time_frame,
                            "stock_id": task.stock_id,
                            "segment": "future",
                            "error": f"Worker process exception: {exc}",
                        }

                    handle_result(result, job, audit, stats, dry_run=dry_run)
                    stats["engine_calls_completed"] += 1

                    if stats["engine_calls_completed"] % job.progress_every == 0:
                        elapsed = max(time.time() - started, 0.001)
                        rate = stats["engine_calls_completed"] / elapsed
                        print(
                            f"[progress] completed={stats['engine_calls_completed']} "
                            f"qualified={stats['qualified_setups']} "
                            f"inserted={stats['orders_inserted']} "
                            f"duplicates={stats['duplicate_orders_skipped']} "
                            f"errors={stats['errors'] + stats['insert_errors']} "
                            f"rate={rate:.2f} jobs/sec",
                            flush=True,
                        )

                previous_count = len(pending)
                exhausted = submit_until_full(executor, task_iterator, pending, job.max_in_flight) or exhausted
                stats["engine_calls_submitted"] += max(0, len(pending) - previous_count)

    stats["duration_seconds"] = round(time.time() - started, 2)
    if stats["duration_seconds"] > 0:
        stats["average_jobs_per_second"] = round(stats["engine_calls_completed"] / stats["duration_seconds"], 3)

    return stats


def default_workers() -> int:
    return max(1, min(8, (os.cpu_count() or 2) - 1))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Parallel historical futures scanner -> future_order_master. BUY and SELL both enabled by default."
    )

    parser.add_argument("--start-date", required=True, help="Historical start date, YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="Historical end date, YYYY-MM-DD")
    parser.add_argument("--country-id", type=int, default=1, help="country_id stored in future_order_master")
    parser.add_argument("--scan-time", type=parse_clock_time, default=clock_time(15, 29), help="Historical candle cut-off, HH:MM; default 15:29")

    parser.add_argument("--time-frame", default=None, help="Optional selected DB timeframes. Example: 1 or 1,2,3,5,6,25. Default all.")
    parser.add_argument("--stack-code", default=None, help="Optional selected stack codes. Example: M-W-D or W-D-125,W-D-60. Default all.")
    parser.add_argument("--side", choices=["BOTH", "BUY", "SELL"], default="BOTH", help="Trade direction to emit. Default BOTH.")

    parser.add_argument("--min-rrr", type=float, default=MIN_RRR_DEFAULT)
    parser.add_argument("--min-days-before-expiry", type=int, default=5)
    parser.add_argument("--step-days", type=int, default=1)
    parser.add_argument("--include-weekends", action="store_true", help="Normally Saturday/Sunday are skipped")
    parser.add_argument("--respect-sell-policy", action="store_true", help="If passed, SELL follows SideEnablementPolicy. Default false, so futures SELL is enabled.")
    parser.add_argument("--symbol", default=None, help="Optional test filter, example: NIFTY")
    parser.add_argument("--workers", type=int, default=default_workers(), help="Process count; default min(8, CPU-1)")
    parser.add_argument("--max-in-flight", type=int, default=None, help="Queued jobs limit; default workers x 4")
    parser.add_argument("--progress-every", type=int, default=25)
    parser.add_argument("--start-method", choices=("spawn", "forkserver", "fork"), default="spawn", help="spawn is safest with SQLAlchemy and DB connections")
    parser.add_argument("--output-path", default="outputs/future_backtest_order_scanner_v389.jsonl")
    parser.add_argument("--dry-run", action="store_true", help="Do not insert DB rows; write audit JSONL only")

    return parser


def main() -> None:
    args = build_parser().parse_args()

    max_in_flight = args.max_in_flight or (args.workers * 4)
    stack_items = selected_stacks(args.time_frame, args.stack_code)

    job = ParallelFutureScanJob(
        start_date=parse_iso_date(args.start_date),
        end_date=parse_iso_date(args.end_date),
        stack_items=stack_items,
        country_id=args.country_id,
        scan_time=args.scan_time,
        min_rrr=args.min_rrr,
        min_days_before_expiry=args.min_days_before_expiry,
        step_days=args.step_days,
        weekdays_only=not args.include_weekends,
        side_mode=args.side,
        respect_sell_policy=args.respect_sell_policy,
        symbol_filter=args.symbol,
        workers=args.workers,
        max_in_flight=max_in_flight,
        progress_every=args.progress_every,
        start_method=args.start_method,
        output_path=args.output_path,
    )

    try:
        result = run(job, dry_run=args.dry_run)
        print("\nPARALLEL HISTORICAL FUTURE ORDER SCAN COMPLETED")
        print(json.dumps(result, default=json_default, indent=2))
    except Exception as exc:
        logger.exception("Parallel historical future order scan stopped: %s", exc)
        raise SystemExit(f"PARALLEL HISTORICAL FUTURE ORDER SCAN FAILED: {exc}")


if __name__ == "__main__":
    main()
