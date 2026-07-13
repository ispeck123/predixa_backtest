#!/usr/bin/env python3
"""
Historical NSE Cash backtest scanner based on the original cash scanner flow.

Important:
- Uses process_setup() + format_calculate_setup_response()
- Does NOT use insert_trade_signals()
- Does NOT use check_and_insert_automated_alert()
- Inserts directly with shared.db.db_utils.insert_alerts()
- No expiry logic
- stock_tick is kept lowercase for cash CSV file names like: reliance_daily.csv

Run from project root:
  python backtest/historical_cash_alerts_backtest_scanner.py --start-date 2026-06-01 --end-date 2026-06-03 --symbol reliance --time-frame 1
"""

import argparse
import json
import sys, os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

# try:
from tqdm import tqdm
# except Exception:  # pragma: no cover
#     tqdm = None

from shared.db.dbconn import DBConnection
from shared.db.db_model import Ind_StockMaster
try:
    from shared.db.db_model import Auto_Order
except Exception:  # model name may differ in some deployments
    Auto_Order = None

from scripts.setup_engine_new import process_setup, format_calculate_setup_response
from scripts.side_enablement_policy import SIDE_POLICY, Side
from shared.db.db_utils import insert_alerts
from shared.utils.logger import logger


dbc = DBConnection()
MIN_RR_THRESHOLD = 2.1

# Cash timeframe stacks. These are intentionally lowercase because your data files are lowercase.
STACKS: Dict[str, Dict[str, Any]] = {
    "M-W-D": {
        "time_frame": 1,
        "time_list": ["monthly", "weekly", "daily"],
    },
    "W-D-60": {
        "time_frame": 2,
        "time_list": ["weekly", "daily", "sixty"],
    },
    "D-75-60-15": {
        "time_frame": 3,
        "time_list": ["daily", "seventy_five", "sixty", "fifteen"],
    },
    "M-W-125": {
        "time_frame": 5,
        "time_list": ["monthly", "weekly", "one_twenty_five"],
    },
    "W-D-125": {
        "time_frame": 5,
        "time_list": ["weekly", "daily", "one_twenty_five"],
    },
    "D-125-25": {
        "time_frame": 6,
        "time_list": ["daily", "one_twenty_five", "twenty_five"],
    },
}


@dataclass(frozen=True)
class CashJob:
    symbol: str
    scan_at: datetime
    stack_code: str
    time_frame: int
    time_list: List[str]


def normalize_tick(value: Any) -> str:
    return str(value or "").strip().lower()


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


def get_all_cash_stock_ticks() -> List[str]:
    """Original scanner style: active Ind_StockMaster rows only; no TradeSignal filtering."""
    session = dbc.get_session()
    try:
        rows = (
            session.query(Ind_StockMaster.stock_tick)
            .filter(Ind_StockMaster.is_active == True)
            .all()
        )
        ticks = [normalize_tick(row[0]) for row in rows if normalize_tick(row[0])]
        return sorted(set(ticks))
    finally:
        session.close()


def filter_symbols(all_symbols: List[str], symbol: Optional[str], symbol_match: str) -> List[str]:
    if not symbol:
        return all_symbols

    requested = normalize_tick(symbol)
    if symbol_match == "exact":
        matched = [s for s in all_symbols if s == requested]
    elif symbol_match == "contains":
        matched = [s for s in all_symbols if requested in s]
    else:
        raise ValueError(f"Unsupported --symbol-match: {symbol_match}")

    return matched


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
            raise ValueError(f"No stacks found for --time-frame {time_frame}")
        return items

    # default: all stacks
    return [(code, int(meta["time_frame"]), list(meta["time_list"])) for code, meta in STACKS.items()]


def infer_execute_tf(time_list: List[str]) -> str:
    return time_list[-1]


def extract_setups(formatted: Dict[str, Any], min_rr: float, ignore_sell_policy: bool) -> List[Dict[str, Any]]:
    setups: List[Dict[str, Any]] = []

    if "BUY" in formatted and float(formatted.get("BUY_RRR", 0) or 0) >= min_rr:
        setups.append({
            "trade_type": "BUY",
            "rrr": formatted.get("BUY_RRR"),
            "trade_dict": formatted["BUY"],
        })

    sell_enabled = ignore_sell_policy or SIDE_POLICY.is_enabled("NSE_CASH", Side.SHORT)
    if sell_enabled and "SELL" in formatted and float(formatted.get("SELL_RRR", 0) or 0) >= min_rr:
        setups.append({
            "trade_type": "SELL",
            "rrr": formatted.get("SELL_RRR"),
            "trade_dict": formatted["SELL"],
        })

    return setups


def scan_one(job: CashJob, min_rr: float, ignore_sell_policy: bool) -> Dict[str, Any]:
    started = time.time()
    record: Dict[str, Any] = {
        "scan_at": job.scan_at.isoformat(),
        "symbol": job.symbol,
        "stack_code": job.stack_code,
        "time_list": job.time_list,
        "execute_tf": infer_execute_tf(job.time_list),
        "time_frame": job.time_frame,
        "segment": "cash",
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
        setups = extract_setups(formatted, min_rr=min_rr, ignore_sell_policy=ignore_sell_policy)
        record["setups"] = setups
        record["status"] = "SETUP" if setups else "NO_SETUP"

    except Exception as exc:
        record["status"] = "ERROR"
        record["error"] = str(exc)

    record["worker_seconds"] = round(time.time() - started, 4)
    return record


def try_fetch_latest_order_id(symbol: str, time_frame: int, order_type: str, trade_dict: Dict[str, Any], scan_at: datetime) -> Optional[int]:
    """insert_alerts does not return order_id, so this is best-effort only."""
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
    """Parent-thread DB insert only. Uses your shared.db.db_utils.insert_alerts()."""
    order_results: List[Dict[str, Any]] = []

    if record.get("status") != "SETUP":
        record["order_results"] = order_results
        return record

    scan_at = datetime.fromisoformat(record["scan_at"])
    symbol = record["symbol"]
    time_frame = int(record["time_frame"])

    for setup in record.get("setups", []):
        trade_type = setup["trade_type"]
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
            result["order_id"] = try_fetch_latest_order_id(symbol, time_frame, trade_type, trade_dict, scan_at)
            if print_inserts:
                print(f"[INSERTED] {symbol} tf={time_frame} {trade_type} scan_at={record['scan_at']} order_id={result['order_id']}")
        except Exception as exc:
            result["insert_status"] = "INSERT_ERROR"
            result["insert_reason"] = str(exc)
            logger.error(f"insert_alerts failed for {symbol} tf={time_frame} {trade_type}: {exc}", exc_info=True)
            if print_inserts:
                print(f"[INSERT_ERROR] {symbol} tf={time_frame} {trade_type}: {exc}")

        order_results.append(result)

    record["order_results"] = order_results
    return record


def build_jobs(symbols: List[str], stack_items: List[Tuple[str, int, List[str]]], start_date: str, end_date: str, scan_time: str, skip_weekends: bool) -> Iterable[CashJob]:
    for scan_at in daterange(start_date, end_date, scan_time, skip_weekends=skip_weekends):
        for symbol in symbols:
            for stack_code, time_frame, time_list in stack_items:
                yield CashJob(
                    symbol=symbol,
                    scan_at=scan_at,
                    stack_code=stack_code,
                    time_frame=time_frame,
                    time_list=time_list,
                )


def run(args: argparse.Namespace) -> Dict[str, Any]:
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)

    all_symbols = get_all_cash_stock_ticks()
    symbols = filter_symbols(all_symbols, args.symbol, args.symbol_match)
    if args.stock_limit and args.stock_limit > 0:
        symbols = symbols[: args.stock_limit]

    if not symbols:
        raise RuntimeError(
            f"No NSE cash stocks selected. symbol={args.symbol!r}, match={args.symbol_match!r}. "
            f"DB active symbols count={len(all_symbols)}"
        )

    stack_items = selected_stacks(args.time_frame, args.stack_code)

    print("Selected symbols:", symbols[:20], "..." if len(symbols) > 20 else "")
    print("Selected stacks:")
    for code, tf, tl in stack_items:
        print(f"  {tf} -> {code} -> {tl}")

    total = 0
    ok = 0
    failed = 0
    setup_count = 0
    inserted = 0
    insert_errors = 0
    started = time.time()

    jobs_iter = build_jobs(
        symbols=symbols,
        stack_items=stack_items,
        start_date=args.start_date,
        end_date=args.end_date,
        scan_time=args.scan_time,
        skip_weekends=not args.include_weekends,
    )

    futures = []
    mode = "a" if args.append else "w"
    # progress = None
    # if tqdm is not None:
    progress = tqdm(desc="Historical cash backtest scan", unit="job")

    with open(args.output, mode, encoding="utf-8") as out:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            for job in jobs_iter:
                futures.append(executor.submit(scan_one, job, args.min_rr, args.ignore_sell_policy))

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
                    elif r.get("insert_status") == "INSERT_ERROR":
                        insert_errors += 1

                out.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
                if progress is not None:
                    progress.update(1)

    if progress is not None:
        progress.close()

    return {
        "output": args.output,
        "symbols": len(symbols),
        "stacks": len(stack_items),
        "total_jobs": total,
        "ok": ok,
        "failed": failed,
        "setups": setup_count,
        "inserted_orders": inserted,
        "insert_errors": insert_errors,
        "duration_seconds": round(time.time() - started, 2),
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Historical NSE cash backtest scanner using insert_alerts only")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--scan-time", default="15:29", help="HH:MM")
    parser.add_argument("--symbol", default=None, help="stock_tick, lowercase file key, e.g. reliance_industries_ltd")
    parser.add_argument("--symbol-match", choices=["exact", "contains"], default="exact")
    parser.add_argument("--stock-limit", type=int, default=0)
    parser.add_argument("--time-frame", default=None, help="Example: 1 or 1,2,3,5,6")
    parser.add_argument("--stack-code", default=None, help="Example: M-W-D or W-D-60,D-125-25")
    parser.add_argument("--min-rr", type=float, default=MIN_RR_THRESHOLD)
    parser.add_argument("--ignore-sell-policy", action="store_true")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--max-in-flight", type=int, default=16)
    parser.add_argument("--include-weekends", action="store_true")
    parser.add_argument("--output", default="outputs/cash_backtest_alerts_scanner.jsonl")
    parser.add_argument("--append", action="store_true")
    parser.add_argument("--print-inserts", action="store_true")
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    try:
        summary = run(args)
        print("HISTORICAL NSE CASH BACKTEST SCAN COMPLETE")
        print(json.dumps(summary, indent=2, default=str))
    except Exception as exc:
        logger.error(f"Historical NSE cash backtest scan failed: {exc}", exc_info=True)
        print(f"HISTORICAL NSE CASH BACKTEST SCAN FAILED: {exc}")
        raise


if __name__ == "__main__":
    main()
