#!/usr/bin/env python3
"""
Merge evaluated futures orders from CSV with BE enrichment captured in the
scanner JSONL and export one JSON object per order.

Primary join key:
    order_id

Inputs:
    1. future_orders.csv
    2. scanner JSONL containing order_results and the configured BE fields
    3. optional OHLC directory, used only when bar_highs/bar_lows are absent
       from the scanner JSONL

The script preserves every CSV row. Different expiry dates are not deduplicated.

Example:
    python export_futures_be_jsonl_fixed.py \
        --orders future_orders.csv \
        --scanner-jsonl futures_scanner_output.jsonl \
        --ohlc-dir /home/ispeck/STOCK_PROJECT/STP_LATEST_V6_CUT/data/indian_stocks_future_data/latest_data_csv \
        --output future_orders_be_enriched.jsonl \
        --unresolved future_orders_be_unresolved.csv \
        --log-file future_orders_be_export.log
"""

from __future__ import annotations

import argparse
import ast
import json
import logging
import math
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MARKET_TIMEZONE = "Asia/Kolkata"

# Used only if execute_tf is not available in the scanner record.
# In the futures scanner, time_frame=5 is W-D-125, therefore its execution
# timeframe is one_twenty_five.
TIME_FRAME_TO_EXECUTE_TF: dict[int, str] = {
    1: "daily",
    2: "sixty",
    3: "fifteen",
    5: "one_twenty_five",
    25: "seventy_five",
}

TIMESTAMP_COLUMN_CANDIDATES: tuple[str, ...] = (
    "timestamp",
    "tradeDate",
    "trade_date",
    "datetime",
    "date_time",
    "date",
    "time",
)

HIGH_COLUMN_CANDIDATES: tuple[str, ...] = ("high", "High", "HIGH")
LOW_COLUMN_CANDIDATES: tuple[str, ...] = ("low", "Low", "LOW")

FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "overlap_ratio": (
        "overlap_ratio",
        "overlap",
        "x_overlap_ratio",
        "nesting_overlap_ratio",
        "zone_overlap_ratio",
    ),
    "far_htf_dist": (
        "far_htf_dist",
        "far_htf_distance",
        "far_htf_target_distance",
        "full_distance",
        "full_target_distance",
    ),
    "struct_stop_A": (
        "struct_stop_A",
        "struct_stop_a",
        "structural_stop_A",
        "structural_stop_a",
        "a_struct_stop",
        "a_level_stop",
        "a_level_distal",
        "struct_A",
    ),
    "struct_stop_E": (
        "struct_stop_E",
        "struct_stop_e",
        "structural_stop_E",
        "structural_stop_e",
        "e_struct_stop",
        "e_level_stop",
        "e_level_distal",
        "struct_E",
    ),
    "bar_highs": (
        "bar_highs",
        "highs_after_fill",
        "path_highs",
        "trade_bar_highs",
    ),
    "bar_lows": (
        "bar_lows",
        "lows_after_fill",
        "path_lows",
        "trade_bar_lows",
    ),
}

# Cache standardized OHLC frames. Many orders reuse the same file.
OHLC_CACHE: dict[Path, pd.DataFrame] = {}

LOGGER = logging.getLogger("futures_be_export")


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------

@dataclass
class ScannerOrderData:
    """Scanner information belonging to one inserted order."""

    order_id: int
    scanner_line: int
    symbol: str | None
    expiry_date: str | None
    time_frame: int | None
    execute_tf: str | None
    trade_type: str | None
    record: dict[str, Any]
    order_result: dict[str, Any]
    setup_item: dict[str, Any] | None


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def configure_logging(log_file: Path, verbose: bool = False) -> None:
    """Configure console and file logging."""

    level = logging.DEBUG if verbose else logging.INFO
    LOGGER.setLevel(level)
    LOGGER.handlers.clear()
    LOGGER.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    LOGGER.addHandler(console_handler)

    log_file.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)


# ---------------------------------------------------------------------------
# Generic conversion helpers
# ---------------------------------------------------------------------------

def is_missing(value: Any) -> bool:
    if value is None:
        return True

    if isinstance(value, str):
        return value.strip().lower() in {"", "nan", "none", "null", "nat"}

    try:
        result = pd.isna(value)
    except (TypeError, ValueError):
        return False

    if result is pd.NA:
        return True

    if isinstance(result, bool) or type(result).__name__ == "bool_":
        return bool(result)

    return False


def to_int(value: Any) -> int | None:
    if is_missing(value):
        return None

    try:
        return int(float(value))
    except (TypeError, ValueError, OverflowError):
        return None


def to_float(value: Any) -> float | None:
    if is_missing(value):
        return None

    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None

    if not math.isfinite(number):
        return None

    return number


def to_float_list(value: Any) -> list[float] | None:
    """Convert a JSON/Python list or a serialized list to list[float]."""

    if is_missing(value):
        return None

    parsed = value

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None

        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            try:
                parsed = ast.literal_eval(text)
            except (ValueError, SyntaxError):
                return None

    if not isinstance(parsed, (list, tuple)):
        return None

    result: list[float] = []
    for item in parsed:
        number = to_float(item)
        if number is None:
            return None
        result.append(round(number, 10))

    return result


def json_safe(value: Any) -> Any:
    """Convert pandas/numpy values into strict JSON-compatible values."""

    if value is None:
        return None

    if isinstance(value, Mapping):
        return {str(key): json_safe(item) for key, item in value.items()}

    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]

    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    if hasattr(value, "item"):
        try:
            return json_safe(value.item())
        except (TypeError, ValueError):
            pass

    if isinstance(value, float) and not math.isfinite(value):
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    return value


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


def normalize_symbol(value: Any) -> str:
    if is_missing(value):
        return ""

    symbol = str(value).strip().upper()

    if ":" in symbol:
        symbol = symbol.split(":")[-1]

    if symbol.endswith("-EQ"):
        symbol = symbol[:-3]

    return symbol


def normalize_trade_type(value: Any) -> str | None:
    if is_missing(value):
        return None

    text = str(value).strip().upper()

    if text in {"BUY", "LONG", "BZ", "GDZ"}:
        return "BUY"

    if text in {"SELL", "SHORT", "SZ", "GSZ"}:
        return "SELL"

    return text or None


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

def parse_datetime_value(value: Any) -> pd.Timestamp | None:
    """
    Parse a datetime without using pd.Timestamp.strptime(), which is not
    implemented in several pandas versions.
    """

    if is_missing(value):
        return None

    # Numeric epoch support.
    numeric_value = to_float(value)
    if numeric_value is not None and not isinstance(value, str):
        absolute = abs(numeric_value)
        if absolute >= 1e17:
            unit = "ns"
        elif absolute >= 1e14:
            unit = "us"
        elif absolute >= 1e11:
            unit = "ms"
        else:
            unit = "s"

        try:
            timestamp = pd.to_datetime(
                numeric_value,
                unit=unit,
                errors="raise",
                utc=True,
            )
            return pd.Timestamp(timestamp).tz_convert(MARKET_TIMEZONE).tz_localize(None)
        except (ValueError, TypeError, OverflowError):
            pass

    text = str(value).strip()

    known_formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%d-%m-%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
    )

    timestamp: pd.Timestamp | None = None

    for date_format in known_formats:
        try:
            parsed = pd.to_datetime(text, format=date_format, errors="raise")
            timestamp = pd.Timestamp(parsed)
            break
        except (ValueError, TypeError):
            continue

    if timestamp is None:
        try:
            timestamp = pd.Timestamp(pd.to_datetime(text, errors="raise"))
        except (ValueError, TypeError, OverflowError):
            return None

    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert(MARKET_TIMEZONE).tz_localize(None)

    return timestamp


def parse_expiry_date(value: Any) -> tuple[str | None, str | None]:
    """Return (DD-MM-YYYY, DDMMYYYY)."""

    if is_missing(value):
        return None, None

    text = str(value).strip()
    known_formats = (
        "%d-%m-%Y",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%d%m%Y",
        "%Y%m%d",
    )

    expiry: pd.Timestamp | None = None

    for date_format in known_formats:
        try:
            parsed = pd.to_datetime(text, format=date_format, errors="raise")
            expiry = pd.Timestamp(parsed)
            break
        except (ValueError, TypeError):
            continue

    if expiry is None:
        try:
            expiry = pd.Timestamp(
                pd.to_datetime(text, errors="raise", dayfirst=True)
            )
        except (ValueError, TypeError, OverflowError):
            return None, None

    return expiry.strftime("%d-%m-%Y"), expiry.strftime("%d%m%Y")


# ---------------------------------------------------------------------------
# order_status parsing
# ---------------------------------------------------------------------------

def parse_order_status(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return json_safe(value)

    if is_missing(value):
        return {}

    text = str(value).strip()

    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return json_safe(parsed)
    except json.JSONDecodeError:
        pass

    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, dict):
            return json_safe(parsed)
    except (ValueError, SyntaxError):
        pass

    return {
        "status": None,
        "reason": "Unable to parse order_status",
        "raw_value": text,
    }


def calculate_win(row: pd.Series, order_status: Mapping[str, Any]) -> int | None:
    existing = row.get("win")
    if not is_missing(existing):
        numeric = to_int(existing)
        if numeric is not None:
            return 1 if numeric == 1 else 0

    status = str(order_status.get("status", "")).strip().lower()
    reason = str(order_status.get("reason", "")).strip().lower()

    if status == "success" or "target hit" in reason:
        return 1

    if status in {"failed", "failure"} or any(
        phrase in reason
        for phrase in ("stoploss hit", "stop loss hit", "stop-loss hit")
    ):
        return 0

    return None


# ---------------------------------------------------------------------------
# Scanner JSONL indexing and extraction
# ---------------------------------------------------------------------------

def find_matching_setup_item(
    record: Mapping[str, Any],
    order_result: Mapping[str, Any],
) -> dict[str, Any] | None:
    setups = record.get("setups")
    if not isinstance(setups, list):
        return None

    wanted_type = normalize_trade_type(order_result.get("trade_type"))
    result_trade_dict = order_result.get("trade_dict")

    # First match by trade type and prices.
    for item in setups:
        if not isinstance(item, dict):
            continue

        if wanted_type and normalize_trade_type(item.get("trade_type")) != wanted_type:
            continue

        item_trade_dict = item.get("trade_dict")
        if isinstance(result_trade_dict, dict) and isinstance(item_trade_dict, dict):
            fields = ("entry_price", "stop_loss", "target_price")
            equal = True
            for field in fields:
                left = to_float(result_trade_dict.get(field))
                right = to_float(item_trade_dict.get(field))
                if left is None or right is None or abs(left - right) > 1e-8:
                    equal = False
                    break
            if equal:
                return item

    # Then match by trade type only.
    for item in setups:
        if isinstance(item, dict) and normalize_trade_type(item.get("trade_type")) == wanted_type:
            return item

    return None


def build_scanner_index(scanner_jsonl: Path) -> tuple[dict[int, ScannerOrderData], dict[str, int]]:
    """Build order_id -> scanner context index."""

    if not scanner_jsonl.exists():
        raise FileNotFoundError(f"Scanner JSONL not found: {scanner_jsonl}")

    index: dict[int, ScannerOrderData] = {}
    stats = {
        "lines": 0,
        "invalid_json": 0,
        "lines_with_orders": 0,
        "indexed_orders": 0,
        "duplicate_order_ids": 0,
        "results_without_order_id": 0,
    }

    LOGGER.info("Indexing scanner JSONL: %s", scanner_jsonl)

    with scanner_jsonl.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue

            stats["lines"] += 1

            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                stats["invalid_json"] += 1
                LOGGER.warning(
                    "Invalid JSON at scanner line %d: %s",
                    line_number,
                    error,
                )
                continue

            if not isinstance(record, dict):
                continue

            order_results = record.get("order_results")
            if not isinstance(order_results, list) or not order_results:
                continue

            stats["lines_with_orders"] += 1

            for result in order_results:
                if not isinstance(result, dict):
                    continue

                order_id = to_int(result.get("order_id"))
                if order_id is None:
                    stats["results_without_order_id"] += 1
                    continue

                setup_item = find_matching_setup_item(record, result)
                data = ScannerOrderData(
                    order_id=order_id,
                    scanner_line=line_number,
                    symbol=(
                        normalize_symbol(record.get("symbol"))
                        if not is_missing(record.get("symbol"))
                        else None
                    ),
                    expiry_date=(
                        str(record.get("expiry_date"))
                        if not is_missing(record.get("expiry_date"))
                        else None
                    ),
                    time_frame=to_int(record.get("time_frame")),
                    execute_tf=(
                        str(record.get("execute_tf")).strip().lower()
                        if not is_missing(record.get("execute_tf"))
                        else None
                    ),
                    trade_type=normalize_trade_type(result.get("trade_type")),
                    record=record,
                    order_result=result,
                    setup_item=setup_item,
                )

                if order_id in index:
                    stats["duplicate_order_ids"] += 1
                    LOGGER.warning(
                        "Duplicate order_id=%s in scanner JSONL; keeping latest line %d",
                        order_id,
                        line_number,
                    )

                index[order_id] = data
                stats["indexed_orders"] += 1

            if stats["lines"] % 5000 == 0:
                LOGGER.info(
                    "Scanner progress: %s lines read, %s unique order IDs indexed",
                    f"{stats['lines']:,}",
                    f"{len(index):,}",
                )

    LOGGER.info(
        "Scanner index ready: %s lines, %s unique order IDs, %s invalid lines",
        f"{stats['lines']:,}",
        f"{len(index):,}",
        f"{stats['invalid_json']:,}",
    )

    return index, stats


def iter_candidate_sources(data: ScannerOrderData) -> Iterable[Mapping[str, Any]]:
    """
    Yield focused containers in priority order. This avoids accidentally taking
    enrichment values from another zone in the full ZONES_X list.
    """

    result = data.order_result
    setup_item = data.setup_item
    record = data.record

    candidates: list[Any] = [
        result.get("be_enrichment"),
        result.get("enrichment"),
        result.get("selected_zone_meta", {}).get("be_enrichment")
        if isinstance(result.get("selected_zone_meta"), dict)
        else None,
        result.get("selected_zone_meta", {}).get("enrichment")
        if isinstance(result.get("selected_zone_meta"), dict)
        else None,
        result.get("selected_zone_meta"),
        result.get("trade_dict", {}).get("be_enrichment")
        if isinstance(result.get("trade_dict"), dict)
        else None,
        result.get("trade_dict", {}).get("enrichment")
        if isinstance(result.get("trade_dict"), dict)
        else None,
        result.get("trade_dict"),
        result,
    ]

    if isinstance(setup_item, dict):
        candidates.extend(
            [
                setup_item.get("be_enrichment"),
                setup_item.get("enrichment"),
                setup_item.get("selected_zone_meta", {}).get("be_enrichment")
                if isinstance(setup_item.get("selected_zone_meta"), dict)
                else None,
                setup_item.get("selected_zone_meta"),
                setup_item.get("trade_dict", {}).get("be_enrichment")
                if isinstance(setup_item.get("trade_dict"), dict)
                else None,
                setup_item.get("trade_dict"),
                setup_item,
            ]
        )

    side = data.trade_type
    setup = record.get("setup")
    if isinstance(setup, dict) and side:
        side_data = setup.get(side)
        if isinstance(side_data, dict):
            candidates.extend(
                [
                    side_data.get("be_enrichment"),
                    side_data.get("enrichment"),
                    side_data,
                ]
            )

    candidates.extend(
        [
            record.get("be_enrichment"),
            record.get("enrichment"),
        ]
    )

    seen: set[int] = set()
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            continue
        identity = id(candidate)
        if identity in seen:
            continue
        seen.add(identity)
        yield candidate


def find_alias_in_mapping(mapping: Mapping[str, Any], aliases: Sequence[str]) -> Any:
    """Find an alias at the current level, case/punctuation-insensitively."""

    normalized = {normalize_key(str(key)): value for key, value in mapping.items()}

    for alias in aliases:
        key = normalize_key(alias)
        if key in normalized and not is_missing(normalized[key]):
            return normalized[key]

    return None


def extract_scanner_field(data: ScannerOrderData, field_name: str) -> Any:
    aliases = FIELD_ALIASES[field_name]

    for source in iter_candidate_sources(data):
        value = find_alias_in_mapping(source, aliases)
        if value is not None:
            return value

        # Support a one-level nested path object, e.g. path.bar_highs.
        for nested_key in ("path", "trade_path", "bars", "be", "geometry"):
            nested = source.get(nested_key)
            if isinstance(nested, Mapping):
                value = find_alias_in_mapping(nested, aliases)
                if value is not None:
                    return value

    return None


# ---------------------------------------------------------------------------
# OHLC fallback
# ---------------------------------------------------------------------------

def find_column(dataframe: pd.DataFrame, candidates: Sequence[str]) -> str | None:
    normalized = {
        normalize_key(str(column)): str(column)
        for column in dataframe.columns
    }

    for candidate in candidates:
        matched = normalized.get(normalize_key(candidate))
        if matched is not None:
            return matched

    return None


def parse_ohlc_timestamps(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")

    if numeric.notna().mean() >= 0.90 and not numeric.dropna().empty:
        median_value = float(numeric.dropna().abs().median())

        if median_value >= 1e17:
            unit = "ns"
        elif median_value >= 1e14:
            unit = "us"
        elif median_value >= 1e11:
            unit = "ms"
        else:
            unit = "s"

        timestamps = pd.to_datetime(numeric, unit=unit, errors="coerce", utc=True)
        return timestamps.dt.tz_convert(MARKET_TIMEZONE).dt.tz_localize(None)

    try:
        timestamps = pd.to_datetime(series, errors="coerce", format="mixed")
    except (TypeError, ValueError):
        timestamps = pd.to_datetime(series, errors="coerce")

    try:
        if timestamps.dt.tz is not None:
            timestamps = timestamps.dt.tz_convert(MARKET_TIMEZONE).dt.tz_localize(None)
    except (AttributeError, TypeError):
        pass

    return timestamps


def load_ohlc_file(file_path: Path) -> pd.DataFrame:
    resolved = file_path.resolve()
    cached = OHLC_CACHE.get(resolved)
    if cached is not None:
        return cached

    dataframe = pd.read_csv(resolved, low_memory=False)

    timestamp_column = find_column(dataframe, TIMESTAMP_COLUMN_CANDIDATES)
    high_column = find_column(dataframe, HIGH_COLUMN_CANDIDATES)
    low_column = find_column(dataframe, LOW_COLUMN_CANDIDATES)

    missing: list[str] = []
    if timestamp_column is None:
        missing.append("timestamp/tradeDate")
    if high_column is None:
        missing.append("high")
    if low_column is None:
        missing.append("low")

    if missing:
        raise ValueError(
            f"{resolved.name} is missing {', '.join(missing)}; "
            f"columns={list(dataframe.columns)}"
        )

    standardized = pd.DataFrame(
        {
            "timestamp": parse_ohlc_timestamps(dataframe[timestamp_column]),
            "high": pd.to_numeric(dataframe[high_column], errors="coerce"),
            "low": pd.to_numeric(dataframe[low_column], errors="coerce"),
        }
    )

    standardized = (
        standardized.dropna(subset=["timestamp", "high", "low"])
        .sort_values("timestamp")
        .drop_duplicates(subset=["timestamp"], keep="last")
        .reset_index(drop=True)
    )

    OHLC_CACHE[resolved] = standardized
    return standardized


def resolve_execute_tf(
    scanner_data: ScannerOrderData | None,
    time_frame: int | None,
) -> str | None:
    if scanner_data and scanner_data.execute_tf:
        return scanner_data.execute_tf.strip().lower()

    if time_frame is None:
        return None

    return TIME_FRAME_TO_EXECUTE_TF.get(time_frame)


def build_ohlc_file_path(
    ohlc_directory: Path,
    stock_tick: str,
    expiry_token: str,
    execute_tf: str,
) -> Path:
    return ohlc_directory / f"{stock_tick}_{expiry_token}_{execute_tf}.csv"


def extract_bar_path_from_ohlc(
    ohlc_data: pd.DataFrame,
    entry_timestamp: pd.Timestamp,
    completed_on: pd.Timestamp,
    execute_tf: str,
) -> tuple[list[float], list[float]]:
    if completed_on < entry_timestamp:
        raise ValueError("completed_on is earlier than entry_timestamp")

    timestamps = ohlc_data["timestamp"]

    if execute_tf in {"daily", "weekly", "monthly"}:
        candle_times = timestamps.dt.normalize()
        start = entry_timestamp.normalize()
        end = completed_on.normalize()
        mask = candle_times.between(start, end, inclusive="both")
    else:
        mask = timestamps.between(entry_timestamp, completed_on, inclusive="both")

    path = ohlc_data.loc[mask, ["timestamp", "high", "low"]].sort_values("timestamp")

    if path.empty:
        raise ValueError(
            f"No candles between {entry_timestamp} and {completed_on}"
        )

    highs = [round(float(value), 10) for value in path["high"].tolist()]
    lows = [round(float(value), 10) for value in path["low"].tolist()]
    return highs, lows


# ---------------------------------------------------------------------------
# Record construction
# ---------------------------------------------------------------------------

def validate_scanner_identity(
    row: pd.Series,
    scanner_data: ScannerOrderData,
) -> list[str]:
    issues: list[str] = []

    csv_symbol = normalize_symbol(row.get("stock_tick"))
    if scanner_data.symbol and csv_symbol and scanner_data.symbol != csv_symbol:
        issues.append(
            f"symbol mismatch: CSV={csv_symbol}, scanner={scanner_data.symbol}"
        )

    csv_expiry, _ = parse_expiry_date(row.get("expiry_date"))
    scanner_expiry, _ = parse_expiry_date(scanner_data.expiry_date)
    if csv_expiry and scanner_expiry and csv_expiry != scanner_expiry:
        issues.append(
            f"expiry mismatch: CSV={csv_expiry}, scanner={scanner_expiry}"
        )

    csv_type = normalize_trade_type(row.get("order_type"))
    if scanner_data.trade_type and csv_type and scanner_data.trade_type != csv_type:
        issues.append(
            f"side mismatch: CSV={csv_type}, scanner={scanner_data.trade_type}"
        )

    return issues


def build_output_record(
    row: pd.Series,
    scanner_index: Mapping[int, ScannerOrderData],
    ohlc_directory: Path | None,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, int]]:
    order_id = to_int(row.get("order_id"))
    stock_tick = normalize_symbol(row.get("stock_tick"))
    time_frame = to_int(row.get("time_frame"))
    order_type = normalize_trade_type(row.get("order_type"))

    entry_price = to_float(row.get("entry_price"))
    stoploss_price = to_float(row.get("stoploss_price"))
    target_price = to_float(row.get("target_price"))

    order_status = parse_order_status(row.get("order_status"))
    entry_timestamp = parse_datetime_value(row.get("entry_timestamp"))
    completed_on = parse_datetime_value(row.get("completed_on"))
    expiry_display, expiry_token = parse_expiry_date(row.get("expiry_date"))
    win = calculate_win(row, order_status)

    unresolved: list[dict[str, Any]] = []
    counters = {
        "scanner_match": 0,
        "scanner_missing": 0,
        "path_from_scanner": 0,
        "path_from_ohlc": 0,
        "path_missing": 0,
    }

    scanner_data = scanner_index.get(order_id) if order_id is not None else None

    overlap_ratio: float | None = None
    far_htf_dist: float | None = None
    struct_stop_a: float | None = None
    struct_stop_e: float | None = None
    bar_highs: list[float] | None = None
    bar_lows: list[float] | None = None

    if scanner_data is None:
        counters["scanner_missing"] += 1
        unresolved.append(
            {
                "order_id": order_id,
                "field": "scanner_match",
                "reason": "order_id not found in scanner JSONL order_results",
            }
        )
    else:
        counters["scanner_match"] += 1

        identity_issues = validate_scanner_identity(row, scanner_data)
        for issue in identity_issues:
            unresolved.append(
                {
                    "order_id": order_id,
                    "field": "scanner_identity",
                    "reason": issue,
                    "scanner_line": scanner_data.scanner_line,
                }
            )

        overlap_ratio = to_float(
            extract_scanner_field(scanner_data, "overlap_ratio")
        )
        far_htf_dist = to_float(
            extract_scanner_field(scanner_data, "far_htf_dist")
        )
        struct_stop_a = to_float(
            extract_scanner_field(scanner_data, "struct_stop_A")
        )
        struct_stop_e = to_float(
            extract_scanner_field(scanner_data, "struct_stop_E")
        )
        bar_highs = to_float_list(
            extract_scanner_field(scanner_data, "bar_highs")
        )
        bar_lows = to_float_list(
            extract_scanner_field(scanner_data, "bar_lows")
        )

        if overlap_ratio is None:
            unresolved.append(
                {
                    "order_id": order_id,
                    "field": "overlap_ratio",
                    "reason": "not found in matched scanner order data",
                    "scanner_line": scanner_data.scanner_line,
                }
            )
        elif not 0.0 <= overlap_ratio <= 1.0:
            unresolved.append(
                {
                    "order_id": order_id,
                    "field": "overlap_ratio",
                    "reason": f"out of expected range 0..1: {overlap_ratio}",
                    "scanner_line": scanner_data.scanner_line,
                }
            )

        if far_htf_dist is None:
            unresolved.append(
                {
                    "order_id": order_id,
                    "field": "far_htf_dist",
                    "reason": "not found in matched scanner order data",
                    "scanner_line": scanner_data.scanner_line,
                }
            )

        # A/E may legitimately be null according to the enrichment spec. We do
        # not mark them unresolved merely because their value is null.

        if bar_highs is not None and bar_lows is not None:
            if len(bar_highs) == len(bar_lows):
                counters["path_from_scanner"] += 1
            else:
                unresolved.append(
                    {
                        "order_id": order_id,
                        "field": "bar_path",
                        "reason": (
                            "scanner bar_highs/bar_lows length mismatch: "
                            f"{len(bar_highs)} vs {len(bar_lows)}"
                        ),
                        "scanner_line": scanner_data.scanner_line,
                    }
                )
                bar_highs = None
                bar_lows = None

    # OHLC fallback only when the scanner did not provide a complete path.
    if bar_highs is None or bar_lows is None:
        if ohlc_directory is not None:
            try:
                if not stock_tick:
                    raise ValueError("stock_tick is missing")
                if expiry_token is None:
                    raise ValueError("expiry_date is invalid")
                if entry_timestamp is None:
                    raise ValueError("entry_timestamp is invalid")
                if completed_on is None:
                    raise ValueError("completed_on is invalid")

                execute_tf = resolve_execute_tf(scanner_data, time_frame)
                if not execute_tf:
                    raise ValueError(
                        f"cannot resolve execution timeframe for time_frame={time_frame}"
                    )

                ohlc_file = build_ohlc_file_path(
                    ohlc_directory,
                    stock_tick,
                    expiry_token,
                    execute_tf,
                )

                if not ohlc_file.exists():
                    raise FileNotFoundError(f"OHLC file not found: {ohlc_file}")

                ohlc_data = load_ohlc_file(ohlc_file)
                bar_highs, bar_lows = extract_bar_path_from_ohlc(
                    ohlc_data,
                    entry_timestamp,
                    completed_on,
                    execute_tf,
                )
                counters["path_from_ohlc"] += 1

            except (
                FileNotFoundError,
                ValueError,
                KeyError,
                pd.errors.ParserError,
                OSError,
            ) as error:
                counters["path_missing"] += 1
                unresolved.append(
                    {
                        "order_id": order_id,
                        "field": "bar_path",
                        "reason": str(error),
                    }
                )
                bar_highs = None
                bar_lows = None
        else:
            counters["path_missing"] += 1
            unresolved.append(
                {
                    "order_id": order_id,
                    "field": "bar_path",
                    "reason": (
                        "bar path missing in scanner JSONL and --ohlc-dir was not supplied"
                    ),
                }
            )

    if bar_highs is not None and is_missing(order_status.get("bars_after_fill")):
        order_status["bars_after_fill"] = len(bar_highs)

    expected_bars = to_int(order_status.get("bars_after_fill"))
    if bar_highs is not None and expected_bars is not None and expected_bars != len(bar_highs):
        unresolved.append(
            {
                "order_id": order_id,
                "field": "bars_after_fill",
                "reason": (
                    f"order_status has {expected_bars}, extracted path has {len(bar_highs)}"
                ),
            }
        )

    output_record = {
        "order_id": order_id,
        "stock_tick": stock_tick,
        "time_frame": time_frame,
        "order_type": order_type,
        "entry_price": entry_price,
        "stoploss_price": stoploss_price,
        "target_price": target_price,
        "order_status": json_safe(order_status),
        "entry_timestamp": (
            entry_timestamp.strftime("%Y-%m-%d %H:%M:%S")
            if entry_timestamp is not None
            else None
        ),
        "completed_on": (
            completed_on.strftime("%Y-%m-%d %H:%M:%S")
            if completed_on is not None
            else None
        ),
        "expiry_date": expiry_display,
        "win": win,
        "overlap_ratio": overlap_ratio,
        "far_htf_dist": far_htf_dist,
        "struct_stop_A": struct_stop_a,
        "struct_stop_E": struct_stop_e,
        "bar_highs": bar_highs,
        "bar_lows": bar_lows,
    }

    return json_safe(output_record), unresolved, counters


# ---------------------------------------------------------------------------
# Export driver
# ---------------------------------------------------------------------------

def export_orders(
    orders_csv: Path,
    scanner_jsonl: Path,
    output_jsonl: Path,
    unresolved_csv: Path,
    ohlc_directory: Path | None,
    progress_every: int,
) -> None:
    if not orders_csv.exists():
        raise FileNotFoundError(f"Orders CSV not found: {orders_csv}")

    if ohlc_directory is not None and not ohlc_directory.exists():
        raise FileNotFoundError(f"OHLC directory not found: {ohlc_directory}")

    scanner_index, scanner_stats = build_scanner_index(scanner_jsonl)

    LOGGER.info("Reading orders CSV: %s", orders_csv)
    orders = pd.read_csv(orders_csv, low_memory=False)

    required_columns = {
        "order_id",
        "stock_tick",
        "time_frame",
        "order_type",
        "entry_price",
        "stoploss_price",
        "target_price",
        "order_status",
        "entry_timestamp",
        "completed_on",
        "expiry_date",
    }

    missing_columns = required_columns - set(orders.columns)
    if missing_columns:
        raise ValueError(
            f"Orders CSV is missing required columns: {sorted(missing_columns)}"
        )

    output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    unresolved_csv.parent.mkdir(parents=True, exist_ok=True)

    totals = {
        "exported": 0,
        "scanner_match": 0,
        "scanner_missing": 0,
        "path_from_scanner": 0,
        "path_from_ohlc": 0,
        "path_missing": 0,
        "fully_enriched": 0,
    }
    unresolved_rows: list[dict[str, Any]] = []

    LOGGER.info("Exporting %s CSV orders", f"{len(orders):,}")
    LOGGER.info("Output JSONL: %s", output_jsonl)

    with output_jsonl.open("w", encoding="utf-8") as output_handle:
        for position, (_, row) in enumerate(orders.iterrows(), start=1):
            record, row_issues, counters = build_output_record(
                row=row,
                scanner_index=scanner_index,
                ohlc_directory=ohlc_directory,
            )

            output_handle.write(
                json.dumps(
                    record,
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                )
            )
            output_handle.write("\n")

            totals["exported"] += 1
            for key, value in counters.items():
                totals[key] += value

            required_enrichment_present = all(
                record.get(field) is not None
                for field in ("overlap_ratio", "far_htf_dist", "bar_highs", "bar_lows")
            )
            if required_enrichment_present:
                totals["fully_enriched"] += 1

            for issue in row_issues:
                issue.update(
                    {
                        "stock_tick": record.get("stock_tick"),
                        "time_frame": record.get("time_frame"),
                        "expiry_date": record.get("expiry_date"),
                    }
                )
                unresolved_rows.append(issue)

            if position == 1 or position % progress_every == 0 or position == len(orders):
                percent = (position / len(orders) * 100.0) if len(orders) else 100.0
                LOGGER.info(
                    "Progress: %s/%s (%.1f%%) | scanner=%s | paths scanner=%s OHLC=%s missing=%s | fully enriched=%s",
                    f"{position:,}",
                    f"{len(orders):,}",
                    percent,
                    f"{totals['scanner_match']:,}",
                    f"{totals['path_from_scanner']:,}",
                    f"{totals['path_from_ohlc']:,}",
                    f"{totals['path_missing']:,}",
                    f"{totals['fully_enriched']:,}",
                )

    if unresolved_rows:
        pd.DataFrame(unresolved_rows).to_csv(unresolved_csv, index=False)
    elif unresolved_csv.exists():
        unresolved_csv.unlink()

    LOGGER.info("=" * 80)
    LOGGER.info("FUTURES BE EXPORT COMPLETED")
    LOGGER.info("Input CSV rows            : %s", f"{len(orders):,}")
    LOGGER.info("Output JSONL rows         : %s", f"{totals['exported']:,}")
    LOGGER.info("Scanner order matches     : %s", f"{totals['scanner_match']:,}")
    LOGGER.info("Scanner order missing     : %s", f"{totals['scanner_missing']:,}")
    LOGGER.info("Paths from scanner JSONL  : %s", f"{totals['path_from_scanner']:,}")
    LOGGER.info("Paths from OHLC fallback  : %s", f"{totals['path_from_ohlc']:,}")
    LOGGER.info("Paths still missing       : %s", f"{totals['path_missing']:,}")
    LOGGER.info("Fully enriched rows       : %s", f"{totals['fully_enriched']:,}")
    LOGGER.info("Issue records             : %s", f"{len(unresolved_rows):,}")
    LOGGER.info("Output                    : %s", output_jsonl)
    if unresolved_rows:
        LOGGER.info("Unresolved report         : %s", unresolved_csv)
    LOGGER.info("Scanner lines read        : %s", f"{scanner_stats['lines']:,}")
    LOGGER.info("Unique scanner order IDs  : %s", f"{len(scanner_index):,}")
    LOGGER.info("=" * 80)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Merge future_orders.csv with scanner BE enrichment by order_id "
            "and export strict JSONL."
        )
    )

    parser.add_argument(
        "--orders",
        type=Path,
        default=Path("future_orders.csv"),
        help="Evaluated futures orders CSV",
    )
    parser.add_argument(
        "--scanner-jsonl",
        type=Path,
        required=True,
        help="Scanner JSONL containing order_results and BE enrichment fields",
    )
    parser.add_argument(
        "--ohlc-dir",
        type=Path,
        default=None,
        help=(
            "Optional OHLC directory. Used only when bar_highs/bar_lows are "
            "missing from scanner JSONL."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("future_orders_be_enriched.jsonl"),
        help="Output enriched JSONL",
    )
    parser.add_argument(
        "--unresolved",
        type=Path,
        default=Path("future_orders_be_unresolved.csv"),
        help="Output CSV containing missing/mismatched enrichment details",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=Path("future_orders_be_export.log"),
        help="Progress and diagnostics log",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="Log progress after this many orders",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug-level console logging",
    )

    arguments = parser.parse_args()

    if arguments.progress_every <= 0:
        parser.error("--progress-every must be greater than zero")

    return arguments


def main() -> None:
    arguments = parse_arguments()
    configure_logging(arguments.log_file, verbose=arguments.verbose)

    try:
        export_orders(
            orders_csv=arguments.orders,
            scanner_jsonl=arguments.scanner_jsonl,
            output_jsonl=arguments.output,
            unresolved_csv=arguments.unresolved,
            ohlc_directory=arguments.ohlc_dir,
            progress_every=arguments.progress_every,
        )
    except Exception:
        LOGGER.exception("Export failed")
        raise


if __name__ == "__main__":
    main()
