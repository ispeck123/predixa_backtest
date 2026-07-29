#!/usr/bin/env python
"""
Regenerate scanner JSONL records for missing NSE cash BUY orders only.

This script uses the existing historical_cash_buy_backtest_v389.py scanner as
the single source of truth. For every row in the missing-orders CSV it:

1. Recreates the original CashJob from stock_tick, time_frame and
   purchased_cmp_date.
2. Calls the scanner's scan_one() function.
3. Matches the regenerated BUY setup to the CSV entry/stop/target prices.
4. Attaches the existing order_id without calling insert_alerts().
5. Writes only successfully recovered records to the output JSONL.
6. Writes rows that cannot be regenerated/matched to an unresolved CSV.

No database insert or update is performed by this recovery script.

The engine work runs in ProcessPoolExecutor workers. Every process loads the
scanner once in its initializer; only the parent process writes output files.

Run from the project root:

    python backtest/recover_cash_missing_jsonl.py \
      --scanner-script backtest/historical_cash_buy_backtest_v389.py \
      --missing-orders-csv cash_orders_missing_order_ids.csv \
      --output outputs/cash_missing_recovered.jsonl \
      --unresolved-csv outputs/cash_missing_unresolved.csv \
      --workers 8 \
      --start-method spawn \
      --chunksize 1 \
      --price-tolerance 0.05 \
      --progress
"""

from __future__ import annotations

import argparse
import copy
import csv
import importlib.util
import json
import logging
import math
import multiprocessing as mp
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from types import ModuleType
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from dateutil import parser as dt_parser


LOGGER = logging.getLogger("cash_missing_jsonl_recovery")

# Initialized separately inside every ProcessPool worker. Module objects and
# closures are intentionally not sent through multiprocessing queues.
_WORKER_SCANNER: Optional[ModuleType] = None
_WORKER_STACK_LOOKUP: Optional[Dict[int, Tuple[str, List[str]]]] = None
_WORKER_EMBED_PARAMS: Optional[Dict[str, Any]] = None
_WORKER_MIN_RR: float = 2.1
_WORKER_ABSOLUTE_TOLERANCE: float = 0.05
_WORKER_RELATIVE_TOLERANCE: float = 0.00001


@dataclass(frozen=True)
class MissingOrder:
    input_index: int
    order_id: int
    stock_tick: str
    stock_id: Optional[int]
    country_id: Optional[int]
    time_frame: int
    order_type: str
    entry_price: float
    stoploss_price: float
    target_price: float
    stock_quantity: float
    purchased_cmp_date: str
    purchased_on: str
    order_status: str
    is_trade_started: Optional[bool]
    is_evaluated: Optional[bool]
    entry_timestamp: str
    completed_on: str
    raw_row: Dict[str, str]


@dataclass
class RecoveryResult:
    input_index: int
    order_id: int
    recovered: bool
    record: Optional[Dict[str, Any]]
    unresolved: Optional[Dict[str, Any]]
    elapsed_seconds: float


def configure_logging(log_file: str, verbose: bool) -> None:
    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]

    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_path, encoding="utf-8"))

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=handlers,
        force=True,
    )


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
    return json.dumps(
        value,
        default=json_default,
        ensure_ascii=False,
        separators=(",", ":"),
    )


def normalize_null(value: Any) -> str:
    text = str(value or "").strip()
    if text.upper() in {"NULL", "NONE", "NAN", "NA"}:
        return ""
    return text


def parse_optional_int(value: Any) -> Optional[int]:
    text = normalize_null(value)
    if not text:
        return None
    return int(float(text))


def parse_float(value: Any, field_name: str, row_number: int) -> float:
    text = normalize_null(value)
    if not text:
        raise ValueError(
            f"Row {row_number}: missing required {field_name}"
        )

    parsed = float(text)
    if not math.isfinite(parsed):
        raise ValueError(
            f"Row {row_number}: non-finite {field_name}={value!r}"
        )
    return parsed


def parse_optional_bool(value: Any) -> Optional[bool]:
    text = normalize_null(value).lower()
    if not text:
        return None
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def parse_scan_at(value: Any, row_number: int) -> datetime:
    text = normalize_null(value)
    if not text:
        raise ValueError(
            f"Row {row_number}: purchased_cmp_date is empty"
        )

    try:
        parsed = dt_parser.parse(text)
    except Exception as exc:
        raise ValueError(
            f"Row {row_number}: invalid purchased_cmp_date={text!r}"
        ) from exc

    # The original cash scanner writes minute-level scan timestamps.
    return parsed.replace(second=0, microsecond=0)


def load_missing_orders(csv_path: str) -> List[MissingOrder]:
    required = {
        "order_id",
        "stock_tick",
        "time_frame",
        "order_type",
        "entry_price",
        "stoploss_price",
        "target_price",
        "purchased_cmp_date",
    }
    orders: List[MissingOrder] = []
    seen_order_ids = set()

    with open(csv_path, "r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        columns = set(reader.fieldnames or [])
        missing_columns = sorted(required - columns)

        if missing_columns:
            raise ValueError(
                "Missing required CSV columns: "
                + ", ".join(missing_columns)
            )

        for input_index, row in enumerate(reader):
            row_number = input_index + 2
            order_id = parse_optional_int(row.get("order_id"))

            if order_id is None:
                raise ValueError(
                    f"Row {row_number}: order_id is empty"
                )

            if order_id in seen_order_ids:
                LOGGER.warning(
                    "Skipping duplicate order_id=%s at CSV row %s",
                    order_id,
                    row_number,
                )
                continue

            seen_order_ids.add(order_id)

            stock_tick = normalize_null(row.get("stock_tick")).lower()
            if not stock_tick:
                raise ValueError(
                    f"Row {row_number}: stock_tick is empty"
                )

            order_type = normalize_null(
                row.get("order_type")
            ).upper()
            if order_type != "BUY":
                raise ValueError(
                    f"Row {row_number}: expected BUY, got {order_type!r}"
                )

            quantity_text = normalize_null(row.get("stock_quantity"))
            quantity = float(quantity_text) if quantity_text else 1.0

            orders.append(
                MissingOrder(
                    input_index=input_index,
                    order_id=order_id,
                    stock_tick=stock_tick,
                    stock_id=parse_optional_int(row.get("stock_id")),
                    country_id=parse_optional_int(row.get("country_id")),
                    time_frame=int(
                        parse_float(
                            row.get("time_frame"),
                            "time_frame",
                            row_number,
                        )
                    ),
                    order_type=order_type,
                    entry_price=parse_float(
                        row.get("entry_price"),
                        "entry_price",
                        row_number,
                    ),
                    stoploss_price=parse_float(
                        row.get("stoploss_price"),
                        "stoploss_price",
                        row_number,
                    ),
                    target_price=parse_float(
                        row.get("target_price"),
                        "target_price",
                        row_number,
                    ),
                    stock_quantity=quantity,
                    purchased_cmp_date=normalize_null(
                        row.get("purchased_cmp_date")
                    ),
                    purchased_on=normalize_null(
                        row.get("purchased_on")
                    ),
                    order_status=normalize_null(
                        row.get("order_status")
                    ),
                    is_trade_started=parse_optional_bool(
                        row.get("is_trade_started")
                    ),
                    is_evaluated=parse_optional_bool(
                        row.get("is_evaluated")
                    ),
                    entry_timestamp=normalize_null(
                        row.get("entry_timestamp")
                    ),
                    completed_on=normalize_null(
                        row.get("completed_on")
                    ),
                    raw_row=dict(row),
                )
            )

    return orders


def import_scanner(scanner_script: str) -> ModuleType:
    scanner_path = Path(scanner_script).resolve()
    if not scanner_path.is_file():
        raise FileNotFoundError(
            f"Scanner script not found: {scanner_path}"
        )

    scanner_dir = str(scanner_path.parent)
    project_root = str(scanner_path.parent.parent)

    for value in (project_root, scanner_dir):
        if value not in sys.path:
            sys.path.insert(0, value)

    module_name = "_cash_backtest_scanner_for_recovery"
    spec = importlib.util.spec_from_file_location(
        module_name,
        scanner_path,
    )
    if spec is None or spec.loader is None:
        raise ImportError(
            f"Unable to load scanner module from {scanner_path}"
        )

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    required = (
        "STACKS",
        "CashJob",
        "scan_one",
    )
    missing = [name for name in required if not hasattr(module, name)]
    if missing:
        raise AttributeError(
            "Scanner is missing required members: "
            + ", ".join(missing)
        )

    return module


def build_stack_lookup(
    scanner: ModuleType,
) -> Dict[int, Tuple[str, List[str]]]:
    lookup: Dict[int, Tuple[str, List[str]]] = {}

    for stack_code, metadata in scanner.STACKS.items():
        time_frame = int(metadata["time_frame"])
        time_list = list(metadata["time_list"])

        if time_frame in lookup:
            raise ValueError(
                f"Scanner has more than one stack for time_frame={time_frame}"
            )

        lookup[time_frame] = (str(stack_code), time_list)

    return lookup


def initialize_process_worker(
    scanner_script: str,
    embed_params: Dict[str, Any],
    min_rr: float,
    absolute_tolerance: float,
    relative_tolerance: float,
    blas_threads: int,
) -> None:
    """
    Load expensive project/scanner modules exactly once in each worker.

    BLAS thread limits are set before the scanner imports pandas/numpy. This
    prevents every process from creating another large internal thread pool.
    """
    global _WORKER_SCANNER
    global _WORKER_STACK_LOOKUP
    global _WORKER_EMBED_PARAMS
    global _WORKER_MIN_RR
    global _WORKER_ABSOLUTE_TOLERANCE
    global _WORKER_RELATIVE_TOLERANCE

    thread_count = str(max(1, int(blas_threads)))
    os.environ["OMP_NUM_THREADS"] = thread_count
    os.environ["MKL_NUM_THREADS"] = thread_count
    os.environ["OPENBLAS_NUM_THREADS"] = thread_count
    os.environ["NUMEXPR_NUM_THREADS"] = thread_count

    os.environ["BW_EMBED_OVERLAP_THRESHOLD"] = str(
        embed_params["embed_overlap_threshold"]
    )
    os.environ["BW_EMBED_SITS_ON_TOP_TARGET_PCT"] = str(
        embed_params["embed_sits_on_top_target_pct"]
    )
    os.environ["BW_EMBED_STRICT_STOP"] = (
        "1" if embed_params["embed_strict_stop"] else "0"
    )
    os.environ["BW_EMBED_OFF"] = (
        "1" if embed_params["bw_embed_off"] else "0"
    )

    scanner = import_scanner(scanner_script)
    _WORKER_SCANNER = scanner
    _WORKER_STACK_LOOKUP = build_stack_lookup(scanner)
    _WORKER_EMBED_PARAMS = dict(embed_params)
    _WORKER_MIN_RR = float(min_rr)
    _WORKER_ABSOLUTE_TOLERANCE = float(absolute_tolerance)
    _WORKER_RELATIVE_TOLERANCE = float(relative_tolerance)


def process_recovery_worker(order: MissingOrder) -> RecoveryResult:
    """Pickle-safe top-level ProcessPool worker entry point."""
    if (
        _WORKER_SCANNER is None
        or _WORKER_STACK_LOOKUP is None
        or _WORKER_EMBED_PARAMS is None
    ):
        raise RuntimeError(
            "Process worker was not initialized correctly"
        )

    return recover_one(
        order=order,
        scanner=_WORKER_SCANNER,
        stack_lookup=_WORKER_STACK_LOOKUP,
        embed_params=_WORKER_EMBED_PARAMS,
        min_rr=_WORKER_MIN_RR,
        absolute_tolerance=_WORKER_ABSOLUTE_TOLERANCE,
        relative_tolerance=_WORKER_RELATIVE_TOLERANCE,
    )


def prices_match(
    actual: float,
    expected: float,
    absolute_tolerance: float,
    relative_tolerance: float,
) -> bool:
    allowed = max(
        absolute_tolerance,
        abs(expected) * relative_tolerance,
    )
    return abs(actual - expected) <= allowed


def candidate_prices(setup: Mapping[str, Any]) -> Dict[str, float]:
    trade_dict = setup.get("trade_dict")
    if not isinstance(trade_dict, Mapping):
        raise ValueError("Setup trade_dict is missing")

    return {
        "entry_price": float(trade_dict["entry_price"]),
        "stoploss_price": float(
            trade_dict.get(
                "stop_loss",
                trade_dict.get("stoploss_price"),
            )
        ),
        "target_price": float(trade_dict["target_price"]),
    }


def setup_price_score(
    setup: Mapping[str, Any],
    order: MissingOrder,
) -> float:
    prices = candidate_prices(setup)
    return (
        abs(prices["entry_price"] - order.entry_price)
        + abs(prices["stoploss_price"] - order.stoploss_price)
        + abs(prices["target_price"] - order.target_price)
    )


def setup_matches_order(
    setup: Mapping[str, Any],
    order: MissingOrder,
    absolute_tolerance: float,
    relative_tolerance: float,
) -> bool:
    try:
        prices = candidate_prices(setup)
    except Exception:
        return False

    return all(
        (
            prices_match(
                prices["entry_price"],
                order.entry_price,
                absolute_tolerance,
                relative_tolerance,
            ),
            prices_match(
                prices["stoploss_price"],
                order.stoploss_price,
                absolute_tolerance,
                relative_tolerance,
            ),
            prices_match(
                prices["target_price"],
                order.target_price,
                absolute_tolerance,
                relative_tolerance,
            ),
        )
    )


def get_buy_candidates(
    scanner: ModuleType,
    record: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    fingerprints = set()

    def append_candidate(candidate: Mapping[str, Any]) -> None:
        if str(candidate.get("trade_type", "BUY")).upper() != "BUY":
            return

        try:
            prices = candidate_prices(candidate)
        except Exception:
            return

        fingerprint = (
            round(prices["entry_price"], 10),
            round(prices["stoploss_price"], 10),
            round(prices["target_price"], 10),
        )
        if fingerprint in fingerprints:
            return

        fingerprints.add(fingerprint)
        candidates.append(copy.deepcopy(dict(candidate)))

    for setup in record.get("setups", []) or []:
        if isinstance(setup, Mapping):
            append_candidate(setup)

    # If current tuning filters the setup by RR, the formatted BUY may still
    # carry the exact original prices. Re-extract with min_rr=0 as a recovery
    # fallback while preserving the scanner's setup-record structure.
    formatted = record.get("setup")
    if isinstance(formatted, Mapping):
        extractor = getattr(scanner, "extract_buy_setups_only", None)
        if callable(extractor):
            try:
                for setup in extractor(dict(formatted), min_rr=0.0):
                    append_candidate(setup)
            except Exception:
                pass

        buy = formatted.get("BUY")
        if isinstance(buy, Mapping):
            append_candidate(
                {
                    "trade_type": "BUY",
                    "rrr": formatted.get("BUY_RRR"),
                    "trade_dict": dict(buy),
                    "selection_reason": None,
                    "proximity_pct": None,
                }
            )

    return candidates


def stored_trade_dict(order: MissingOrder) -> Dict[str, float]:
    # Use the database values from the CSV so downstream order_id/price joins
    # match exactly.
    return {
        "entry_price": order.entry_price,
        "stop_loss": order.stoploss_price,
        "target_price": order.target_price,
    }


def unresolved_record(
    order: MissingOrder,
    reason: str,
    record: Optional[Mapping[str, Any]] = None,
    candidates: Optional[Sequence[Mapping[str, Any]]] = None,
    error: Optional[str] = None,
) -> Dict[str, Any]:
    result: Dict[str, Any] = dict(order.raw_row)
    result["recovery_reason"] = reason
    result["scanner_status"] = (
        record.get("status") if record is not None else ""
    )
    result["scanner_error"] = (
        record.get("error", "") if record is not None else ""
    )
    result["recovery_error"] = error or ""

    serialized_candidates = []
    for candidate in candidates or []:
        try:
            serialized_candidates.append(
                {
                    **candidate_prices(candidate),
                    "rrr": candidate.get("rrr"),
                }
            )
        except Exception:
            continue

    result["regenerated_candidates"] = json_dumps(
        serialized_candidates
    )
    return result


def recover_one(
    order: MissingOrder,
    scanner: ModuleType,
    stack_lookup: Mapping[int, Tuple[str, List[str]]],
    embed_params: Mapping[str, Any],
    min_rr: float,
    absolute_tolerance: float,
    relative_tolerance: float,
) -> RecoveryResult:
    started = time.time()

    try:
        stack = stack_lookup.get(order.time_frame)
        if stack is None:
            unresolved = unresolved_record(
                order,
                reason="UNSUPPORTED_TIME_FRAME",
            )
            return RecoveryResult(
                order.input_index,
                order.order_id,
                False,
                None,
                unresolved,
                time.time() - started,
            )

        stack_code, time_list = stack
        scan_at = parse_scan_at(
            order.purchased_cmp_date,
            order.input_index + 2,
        )
        job = scanner.CashJob(
            symbol=order.stock_tick,
            scan_at=scan_at,
            stack_code=stack_code,
            time_frame=order.time_frame,
            time_list=list(time_list),
        )

        record = scanner.scan_one(
            job,
            min_rr=min_rr,
            embed_params=dict(embed_params),
        )
        candidates = get_buy_candidates(scanner, record)
        matching = [
            candidate
            for candidate in candidates
            if setup_matches_order(
                candidate,
                order,
                absolute_tolerance,
                relative_tolerance,
            )
        ]

        if not matching:
            reason = (
                "SCANNER_ERROR"
                if record.get("status") == "ERROR"
                else (
                    "NO_REGENERATED_BUY_SETUP"
                    if not candidates
                    else "PRICE_MISMATCH"
                )
            )
            unresolved = unresolved_record(
                order,
                reason=reason,
                record=record,
                candidates=candidates,
            )
            return RecoveryResult(
                order.input_index,
                order.order_id,
                False,
                None,
                unresolved,
                time.time() - started,
            )

        matched = min(
            matching,
            key=lambda value: setup_price_score(value, order),
        )
        regenerated_prices = candidate_prices(matched)
        db_trade_dict = stored_trade_dict(order)

        matched_setup = copy.deepcopy(matched)
        matched_setup["trade_type"] = "BUY"
        matched_setup["trade_dict"] = db_trade_dict

        record["status"] = "SETUP"
        record["setups"] = [matched_setup]
        record["order_results"] = [
            {
                "trade_type": "BUY",
                "rrr": matched_setup.get("rrr"),
                "trade_dict": db_trade_dict,
                "insert_status": "ORDER_INSERTED",
                "insert_reason": (
                    "recovered_existing_order_from_missing_csv"
                ),
                "order_id": order.order_id,
            }
        ]
        record["recovery"] = {
            "source": "missing_orders_csv",
            "existing_order_id": order.order_id,
            "stock_id": order.stock_id,
            "country_id": order.country_id,
            "stock_quantity": order.stock_quantity,
            "order_status": order.order_status,
            "is_trade_started": order.is_trade_started,
            "is_evaluated": order.is_evaluated,
            "entry_timestamp": order.entry_timestamp,
            "completed_on": order.completed_on,
            "regenerated_prices": regenerated_prices,
            "stored_prices": {
                "entry_price": order.entry_price,
                "stoploss_price": order.stoploss_price,
                "target_price": order.target_price,
            },
            "price_match_score": round(
                setup_price_score(matched, order),
                10,
            ),
            "database_insert_performed": False,
        }

        return RecoveryResult(
            order.input_index,
            order.order_id,
            True,
            record,
            None,
            time.time() - started,
        )

    except Exception as exc:
        unresolved = unresolved_record(
            order,
            reason="RECOVERY_EXCEPTION",
            error=str(exc),
        )
        return RecoveryResult(
            order.input_index,
            order.order_id,
            False,
            None,
            unresolved,
            time.time() - started,
        )


def read_existing_order_ids(patterns: Sequence[str]) -> set[int]:
    import glob

    order_ids: set[int] = set()
    files: List[str] = []

    for pattern in patterns:
        files.extend(glob.glob(pattern, recursive=True))

    for file_path in sorted(set(files)):
        with open(file_path, "r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                text = line.strip()
                if not text:
                    continue

                try:
                    record = json.loads(text)
                except json.JSONDecodeError:
                    LOGGER.warning(
                        "Ignoring invalid JSON: %s line %s",
                        file_path,
                        line_number,
                    )
                    continue

                for item in record.get("order_results", []) or []:
                    value = item.get("order_id")
                    try:
                        if value is not None:
                            order_ids.add(int(value))
                    except (TypeError, ValueError):
                        continue

    return order_ids


def write_unresolved_csv(
    path: str,
    rows: Sequence[Mapping[str, Any]],
    original_columns: Sequence[str],
) -> None:
    extra_columns = [
        "recovery_reason",
        "scanner_status",
        "scanner_error",
        "recovery_error",
        "regenerated_candidates",
    ]
    fieldnames = list(original_columns)
    for name in extra_columns:
        if name not in fieldnames:
            fieldnames.append(name)

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(
        output_path,
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Regenerate scanner JSONL records only for cash BUY order_ids "
            "listed in a missing-orders CSV."
        )
    )
    parser.add_argument(
        "--scanner-script",
        default="backtest/historical_cash_buy_backtest_v389.py",
        help="Path to the original cash backtest scanner.",
    )
    parser.add_argument(
        "--missing-orders-csv",
        required=True,
        help="CSV containing the missing auto_order_master rows.",
    )
    parser.add_argument(
        "--output",
        default="outputs/cash_missing_recovered.jsonl",
        help="Recovered records JSONL.",
    )
    parser.add_argument(
        "--unresolved-csv",
        default="outputs/cash_missing_unresolved.csv",
    )
    parser.add_argument(
        "--summary-json",
        default="outputs/cash_missing_recovery_summary.json",
    )
    parser.add_argument(
        "--log-file",
        default="outputs/cash_missing_recovery.log",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=min(8, os.cpu_count() or 4),
        help=(
            "Process count. Start with 6-8; very high values can cause "
            "memory pressure and CSV disk contention."
        ),
    )
    parser.add_argument(
        "--start-method",
        choices=("spawn", "fork", "forkserver"),
        default="spawn",
        help=(
            "Multiprocessing start method. spawn is safest because the "
            "scanner creates project/database globals during import."
        ),
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=1,
        help=(
            "Orders assigned to a worker per dispatch batch. Keep 1 when "
            "individual setup calculations have uneven runtimes."
        ),
    )
    parser.add_argument(
        "--blas-threads",
        type=int,
        default=1,
        help="Internal NumPy/BLAS threads allowed inside each process.",
    )
    parser.add_argument("--min-rr", type=float, default=2.1)
    parser.add_argument(
        "--price-tolerance",
        type=float,
        default=0.05,
        help="Absolute tolerance for each entry/stop/target comparison.",
    )
    parser.add_argument(
        "--relative-price-tolerance",
        type=float,
        default=0.00001,
        help="Relative tolerance for each price comparison.",
    )
    parser.add_argument(
        "--embed-overlap-threshold",
        type=float,
        default=0.50,
    )
    parser.add_argument(
        "--embed-sits-on-top-target-pct",
        type=float,
        default=0.50,
    )
    parser.add_argument(
        "--embed-strict-stop",
        action="store_true",
    )
    parser.add_argument("--bw-embed-off", action="store_true")
    parser.add_argument(
        "--existing-jsonl",
        action="append",
        default=[],
        help=(
            "Optional existing JSONL glob. Any order_id already present is "
            "skipped. May be supplied multiple times."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional number of missing CSV rows to process for testing.",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append recovered records instead of replacing output.",
    )
    parser.add_argument("--progress", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    configure_logging(args.log_file, args.verbose)

    if args.workers < 1:
        raise ValueError("--workers must be at least 1")
    if args.chunksize < 1:
        raise ValueError("--chunksize must be at least 1")
    if args.blas_threads < 1:
        raise ValueError("--blas-threads must be at least 1")
    if args.price_tolerance < 0:
        raise ValueError("--price-tolerance cannot be negative")
    if args.relative_price_tolerance < 0:
        raise ValueError(
            "--relative-price-tolerance cannot be negative"
    )

    started = time.time()
    scanner_path = Path(args.scanner_script).resolve()
    if not scanner_path.is_file():
        raise FileNotFoundError(
            f"Scanner script not found: {scanner_path}"
        )

    orders = load_missing_orders(args.missing_orders_csv)
    if args.limit is not None:
        if args.limit < 1:
            raise ValueError("--limit must be at least 1")
        orders = orders[: args.limit]

    original_columns = list(
        orders[0].raw_row.keys()
    ) if orders else []

    already_present = read_existing_order_ids(args.existing_jsonl)
    pending_orders = [
        order
        for order in orders
        if order.order_id not in already_present
    ]
    skipped_existing = len(orders) - len(pending_orders)

    embed_params = {
        "embed_overlap_threshold": args.embed_overlap_threshold,
        "embed_sits_on_top_target_pct": (
            args.embed_sits_on_top_target_pct
        ),
        "embed_strict_stop": bool(args.embed_strict_stop),
        "bw_embed_off": bool(args.bw_embed_off),
    }

    # Match the environment configured by the original scanner.
    os.environ["BW_EMBED_OVERLAP_THRESHOLD"] = str(
        args.embed_overlap_threshold
    )
    os.environ["BW_EMBED_SITS_ON_TOP_TARGET_PCT"] = str(
        args.embed_sits_on_top_target_pct
    )
    os.environ["BW_EMBED_STRICT_STOP"] = (
        "1" if args.embed_strict_stop else "0"
    )
    os.environ["BW_EMBED_OFF"] = "1" if args.bw_embed_off else "0"

    # These must be present before spawned processes import NumPy/Pandas.
    thread_count = str(args.blas_threads)
    os.environ["OMP_NUM_THREADS"] = thread_count
    os.environ["MKL_NUM_THREADS"] = thread_count
    os.environ["OPENBLAS_NUM_THREADS"] = thread_count
    os.environ["NUMEXPR_NUM_THREADS"] = thread_count

    LOGGER.info(
        "Loaded %s missing orders; %s require recovery; %s already exist",
        len(orders),
        len(pending_orders),
        skipped_existing,
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_mode = "a" if args.append else "w"

    recovered_count = 0
    unresolved_rows: List[Dict[str, Any]] = []
    processed_count = 0

    progress = None
    if args.progress:
        try:
            from tqdm import tqdm

            progress = tqdm(
                total=len(pending_orders),
                desc="Recovering cash JSONL",
                unit="order",
            )
        except Exception:
            progress = None

    try:
        with open(
            output_path,
            output_mode,
            encoding="utf-8",
        ) as output_handle:
            process_context = mp.get_context(args.start_method)

            with ProcessPoolExecutor(
                max_workers=args.workers,
                mp_context=process_context,
                initializer=initialize_process_worker,
                initargs=(
                    str(scanner_path),
                    embed_params,
                    args.min_rr,
                    args.price_tolerance,
                    args.relative_price_tolerance,
                    args.blas_threads,
                ),
            ) as executor:
                # executor.map preserves CSV order while still executing the
                # engine calls concurrently across independent processes.
                for result in executor.map(
                    process_recovery_worker,
                    pending_orders,
                    chunksize=args.chunksize,
                ):
                    processed_count += 1

                    if result.recovered and result.record is not None:
                        output_handle.write(
                            json_dumps(result.record) + "\n"
                        )
                        output_handle.flush()
                        recovered_count += 1
                    elif result.unresolved is not None:
                        unresolved_rows.append(result.unresolved)

                    if progress is not None:
                        progress.update(1)

                    if (
                        processed_count % 100 == 0
                        or processed_count == len(pending_orders)
                    ):
                        LOGGER.info(
                            "Progress %s/%s | recovered=%s | unresolved=%s",
                            processed_count,
                            len(pending_orders),
                            recovered_count,
                            len(unresolved_rows),
                        )
    finally:
        if progress is not None:
            progress.close()

    write_unresolved_csv(
        args.unresolved_csv,
        unresolved_rows,
        original_columns,
    )

    summary = {
        "missing_orders_csv": str(
            Path(args.missing_orders_csv).resolve()
        ),
        "scanner_script": str(Path(args.scanner_script).resolve()),
        "output_jsonl": str(output_path.resolve()),
        "unresolved_csv": str(
            Path(args.unresolved_csv).resolve()
        ),
        "input_rows": len(orders),
        "unique_order_ids": len({o.order_id for o in orders}),
        "skipped_already_present": skipped_existing,
        "processed": processed_count,
        "recovered": recovered_count,
        "unresolved": len(unresolved_rows),
        "database_inserts": 0,
        "price_tolerance": args.price_tolerance,
        "relative_price_tolerance": (
            args.relative_price_tolerance
        ),
        "executor": "ProcessPoolExecutor",
        "workers": args.workers,
        "start_method": args.start_method,
        "chunksize": args.chunksize,
        "blas_threads_per_process": args.blas_threads,
        "duration_seconds": round(time.time() - started, 2),
    }

    summary_path = Path(args.summary_json)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    with open(summary_path, "w", encoding="utf-8") as handle:
        json.dump(
            summary,
            handle,
            indent=2,
            default=json_default,
            ensure_ascii=False,
        )
        handle.write("\n")

    LOGGER.info("Recovery complete")
    LOGGER.info(json.dumps(summary, indent=2))


if __name__ == "__main__":
    mp.freeze_support()
    main()