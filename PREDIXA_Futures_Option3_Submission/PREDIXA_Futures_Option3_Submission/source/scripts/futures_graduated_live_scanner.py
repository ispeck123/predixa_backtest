import argparse
import csv
import json
import time
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional
import sys, os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from shared.db import db_utils
from shared.db.db_model import Future_Order, FuturesMaster, OMSOrderBucketFuture, TradeSignal
from shared.config.settings import stock_logic_config
from scripts.graduated_live_config import FUTURES_LOT_SIZES, FUTURES_TIER0_LOTS
from scripts.setup_engine_new import format_calculate_setup_response, process_setup_fc


EXCHANGE_ID_NSEFO = 11
BUY_RRR_THRESHOLD = 2.1
DEFAULT_OUTPUT_DIR = Path("outputs/graduated_live_validation")
DEFAULT_TIMEFRAME_IDS = [1, 25, 2, 3, 4, 5]
DEFAULT_MIN_EXPIRY_DAYS = 5
ALLOWED_FUTURES_SYMBOLS = (
    "CANBK",
    "HDFCBANK",
    "PNB",
    "ASHOKLEY",
    "TATASTEEL",
    "ETERNAL",
    "JIOFIN",
    "MOTHERSON",
)


SAFETY_BANNER = """============================================================
PREDIXA FUTURES GRADUATED-LIVE SCANNER
MODE: DB CANDIDATE INSERT
SEGMENT: NSE FUTURES
SIDE: BUY ONLY
BROKER MUTATIONS: DISABLED
OMS FUTURES CANDIDATES: ENABLED
================================"""


@dataclass(frozen=True)
class FuturesScanJob:
    symbol: str
    expiry: Any
    exp_num: Any
    timeframe_id: int
    time_list: List[str]
    last_d_time: Any = None
    exchange_id: int = EXCHANGE_ID_NSEFO
    is_future: bool = True
    contract: Optional[str] = None
    lot_size: Optional[int] = None
    stock_id: Optional[int] = None


@dataclass
class ScanOutputs:
    run_id: str
    scan_timestamp: str
    candidates: List[Dict[str, Any]] = field(default_factory=list)
    diagnostics: List[Dict[str, Any]] = field(default_factory=list)
    total_jobs: int = 0
    processed: int = 0
    errors: int = 0
    csv_path: Optional[Path] = None
    jsonl_path: Optional[Path] = None


SetupRunner = Callable[[FuturesScanJob], Any]
Formatter = Callable[[Any, FuturesScanJob], Dict[str, Any]]
JobProvider = Callable[[List[int], Optional[int], Any, int], Iterable[FuturesScanJob]]
SignalInserter = Callable[[Dict[str, Any], int, int, int], Any]
CandidatePersister = Callable[[Dict[str, Any], FuturesScanJob, Any], Any]


def _one_lot(value: Any) -> int:
    if isinstance(value, bool) or value is None:
        raise ValueError("FUT_REJECT_LOT_SIZE_UNAVAILABLE")
    try:
        quantity = int(value)
    except (TypeError, ValueError):
        raise ValueError("FUT_REJECT_INVALID_LOT_SIZE") from None
    try:
        is_integral = quantity == value
    except Exception:
        is_integral = False
    if quantity <= 0 or not is_integral:
        raise ValueError("FUT_REJECT_INVALID_LOT_SIZE")
    return quantity


def persist_futures_candidate(candidate: Dict[str, Any], job: FuturesScanJob,
                              signal_result: Any) -> Dict[str, Any]:
    """Create/reuse the DB-side futures approval candidate only.

    ``signal_result`` is the return value of ``insert_trade_signals`` and may
    be ``(inserted, id)`` or a test-specific ID.  No broker or reservation
    code is reachable from this helper.
    """
    quantity = _one_lot(job.lot_size)
    trade_signal_id = signal_result[1] if isinstance(signal_result, tuple) and len(signal_result) > 1 else signal_result
    trade = candidate.get("setup_result") or {}
    buy = trade.get("BUY") if isinstance(trade, dict) else None
    if not isinstance(buy, dict):
        raise ValueError("FUT_REJECT_BUY_SETUP_MISSING")
    session = db_utils.dbc.get_session()
    try:
        if trade_signal_id:
            signal = session.query(TradeSignal).filter(TradeSignal.id == trade_signal_id).first()
        else:
            signal = session.query(TradeSignal).filter(
                TradeSignal.stock_name == job.symbol,
                TradeSignal.time_fr == job.timeframe_id,
                TradeSignal.trade_type == "BUY",
                TradeSignal.exp_date == str(job.exp_num),
                TradeSignal.price_cmp == candidate.get("PRICE_CMP"),
            ).order_by(TradeSignal.id.desc()).first()
        if signal is None:
            raise ValueError("FUT_REJECT_TRADE_SIGNAL_NOT_PERSISTED")
        trade_signal_id = signal.id

        stock_id = job.stock_id
        if stock_id is None:
            stock = session.query(FuturesMaster).filter(
                FuturesMaster.symbol == job.symbol,
            ).order_by(FuturesMaster.expiry_date).first()
            stock_id = stock.id if stock else None

        future_order = session.query(Future_Order).filter(
            Future_Order.trade_signal_id == trade_signal_id
        ).first()
        now = datetime.now()
        if future_order is None:
            future_order = Future_Order(
                stock_tick=job.symbol, stock_id=stock_id, country_id=1,
                time_frame=job.timeframe_id, order_type="BUY",
                entry_price=buy["entry_price"], stoploss_price=buy["stop_loss"],
                target_price=buy["target_price"], stock_quantity=quantity,
                purchased_cmp_date=now.strftime("%Y-%m-%dT%H:%M"),
                purchased_on=now, order_status=None, is_trade_started=False,
                is_evaluated=False, entry_timestamp=None, completed_on=None,
                arima_ab_model_prediction="NA", arima_ab_model_prob=0.0,
                expiry_date=str(job.expiry), trade_signal_id=trade_signal_id,
            )
            session.add(future_order)
            session.flush()

        oms = session.query(OMSOrderBucketFuture).filter(
            OMSOrderBucketFuture.trade_signal_id == trade_signal_id
        ).first()
        if oms is None:
            oms = OMSOrderBucketFuture(
                order_id=future_order.order_id, trade_signal_id=trade_signal_id,
                stock_tick=job.symbol, stock_id=stock_id, country_id=1,
                time_frame=job.timeframe_id, order_type="BUY",
                entry_price=buy["entry_price"], stoploss_price=buy["stop_loss"],
                target_price=buy["target_price"], stock_quantity=quantity,
                purchased_cmp_date=now.strftime("%Y-%m-%dT%H:%M"),
                purchased_on=now, order_status="pending", is_trade_started=0,
                is_evaluated=0, entry_timestamp=None, completed_on=None,
                expiry_date=str(job.expiry),
                approval_status="pending", approved_by=None, approved_at=None,
                id_fyers=None, gtt_id=None,
            )
            session.add(oms)
        session.commit()
        return {"success": True, "trade_signal_id": trade_signal_id,
                "future_order_id": future_order.order_id, "bucket_id": oms.bucket_id,
                "status": "CANDIDATE_PENDING"}
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def default_time_list(timeframe_id: int) -> List[str]:
    return list(getattr(stock_logic_config, f"FUTURE_TIME_FRAME_{timeframe_id}"))


def execute_timeframe(time_list: List[str]) -> Optional[str]:
    return time_list[-1] if time_list else None


def default_futures_jobs(
    timeframe_ids: List[int],
    max_symbols: Optional[int],
    last_d_time: Any,
    min_expiry_days: int = DEFAULT_MIN_EXPIRY_DAYS,
) -> Iterable[FuturesScanJob]:
    from scripts.scanner_fc import build_futures_expiry_map

    allowed_symbols = set(ALLOWED_FUTURES_SYMBOLS)
    emitted = set()
    emitted_symbols = set()
    max_reached = False

    for timeframe_id in timeframe_ids:
        futures_map = build_futures_expiry_map(
            min_days=min_expiry_days,
            time_fr=timeframe_id,
            allowed_symbols=ALLOWED_FUTURES_SYMBOLS,
        )
        for item in futures_map:
            symbol = str(item.get("symbol") or "").strip().upper()
            if symbol not in allowed_symbols:
                continue
            if symbol not in emitted_symbols:
                if max_symbols is not None and len(emitted_symbols) >= max_symbols:
                    max_reached = True
                    break
                emitted_symbols.add(symbol)

            for exp_num in item.get("expiry", []):
                key = (symbol, str(exp_num), timeframe_id)
                if key in emitted:
                    continue
                emitted.add(key)
                yield FuturesScanJob(
                    symbol=symbol,
                    expiry=exp_num,
                    exp_num=exp_num,
                    timeframe_id=timeframe_id,
                    time_list=default_time_list(timeframe_id),
                    last_d_time=last_d_time,
                    lot_size=(FUTURES_LOT_SIZES.get(symbol) * FUTURES_TIER0_LOTS
                              if symbol in FUTURES_LOT_SIZES else None),
                )
        if max_reached:
            break


def run_setup_engine(job: FuturesScanJob) -> Any:
    return process_setup_fc(job.symbol, job.time_list, job.exp_num, normalize_last_d_time(job.last_d_time), True)


def format_setup_result(raw: Any, job: FuturesScanJob) -> Dict[str, Any]:
    return format_calculate_setup_response(
        raw,
        stock_name=job.symbol,
        time_fr=job.timeframe_id,
        exp_num=job.exp_num,
        last_d_time=normalize_last_d_time(job.last_d_time),
        is_future=True,
        is_cash=False,
    )


def normalize_last_d_time(value: Any) -> Any:
    """Return the datetime shape expected by the pandas setup-data loader."""
    import pandas as pd

    if value is None or value == "":
        return pd.Timestamp.now().to_pydatetime()
    if isinstance(value, (datetime, pd.Timestamp)):
        return pd.Timestamp(value).to_pydatetime()
    return pd.to_datetime(value, dayfirst=True, errors="raise").to_pydatetime()


def _base_record(run_id: str, scan_timestamp: str, job: FuturesScanJob) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "scan_timestamp": scan_timestamp,
        "symbol": job.symbol,
        "expiry": job.expiry,
        "EXP_NUM": job.exp_num,
        "timeframe_id": job.timeframe_id,
        "time_list": job.time_list,
        "execute_timeframe": execute_timeframe(job.time_list),
        "exchange_id": job.exchange_id,
        "is_future": job.is_future,
        "contract": job.contract,
    }


def classify_formatted_result(
    formatted: Any,
    job: FuturesScanJob,
    *,
    run_id: str,
    scan_timestamp: str,
    buy_rrr_threshold: float = BUY_RRR_THRESHOLD,
) -> tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    record = _base_record(run_id, scan_timestamp, job)
    record["setup_result"] = formatted

    if job.exchange_id != EXCHANGE_ID_NSEFO or job.is_future is not True:
        record["scan_status"] = "INVALID_RESULT"
        record["rejection_reason"] = "NOT_NSE_FUTURES"
        return record, None

    if job.exp_num in (None, ""):
        record["scan_status"] = "INVALID_RESULT"
        record["rejection_reason"] = "INVALID_EXPIRY"
        return record, None

    if not isinstance(formatted, dict):
        record["scan_status"] = "INVALID_RESULT"
        record["rejection_reason"] = "FORMATTED_RESULT_NOT_DICT"
        return record, None

    buy = formatted.get("BUY")
    if not buy:
        record["scan_status"] = "NO_SETUP"
        record["rejection_reason"] = "NO_BUY_SETUP"
        return record, None

    try:
        buy_rrr = float(formatted.get("BUY_RRR", 0.0))
    except (TypeError, ValueError):
        record["scan_status"] = "INVALID_RESULT"
        record["rejection_reason"] = "INVALID_BUY_RRR"
        return record, None

    if buy_rrr < buy_rrr_threshold:
        record["scan_status"] = "BUY_RRR_TOO_LOW"
        record["rejection_reason"] = f"BUY_RRR_LT_{buy_rrr_threshold}"
        return record, None

    if not isinstance(buy, dict):
        record["scan_status"] = "INVALID_RESULT"
        record["rejection_reason"] = "BUY_NOT_DICT"
        return record, None

    required = ("entry_price", "stop_loss", "target_price")
    if any(buy.get(key) is None for key in required):
        record["scan_status"] = "INVALID_RESULT"
        record["rejection_reason"] = "BUY_PRICE_FIELD_MISSING"
        return record, None

    record["scan_status"] = "ACCEPTED"
    record["rejection_reason"] = None

    timestamps = formatted.get("BUY_TIMESTAMPS") if isinstance(formatted.get("BUY_TIMESTAMPS"), dict) else {}
    candidate = {
        **_base_record(run_id, scan_timestamp, job),
        "side": "BUY",
        "entry": buy.get("entry_price"),
        "stop_loss": buy.get("stop_loss"),
        "target": buy.get("target_price"),
        "BUY_RRR": buy_rrr,
        "zone_type": buy.get("zone_type"),
        "zone_signature": buy.get("zone_signature"),
        "zone_metadata": {
            key: buy.get(key)
            for key in (
                "is_execute_tf",
                "consumption_dead_after",
                "base_start_idx",
                "legout_end_idx",
                "retest_count",
                "overlap_ratio",
                "htf_target_price",
                "struct_stop_A",
                "struct_stop_E",
            )
            if key in buy
        },
        "signal_timestamp": timestamps.get("entry_price_timestamp"),
        "PRICE_CMP": formatted.get("PRICE_CMP"),
        "setup_result": formatted,
    }
    return record, candidate


class FuturesGraduatedLiveReadonlyScanner:
    def __init__(
        self,
        *,
        job_provider: JobProvider = default_futures_jobs,
        setup_runner: SetupRunner = run_setup_engine,
        formatter: Formatter = format_setup_result,
        signal_inserter: SignalInserter = db_utils.insert_trade_signals,
        candidate_persister: CandidatePersister = persist_futures_candidate,
        buy_rrr_threshold: float = BUY_RRR_THRESHOLD,
    ) -> None:
        self.job_provider = job_provider
        self.setup_runner = setup_runner
        self.formatter = formatter
        self.signal_inserter = signal_inserter
        self.candidate_persister = candidate_persister
        self.buy_rrr_threshold = buy_rrr_threshold

    def run(
        self,
        *,
        timeframe_ids: List[int],
        max_symbols: Optional[int] = None,
        last_d_time: Any = None,
        min_expiry_days: int = DEFAULT_MIN_EXPIRY_DAYS,
        output_dir: Path = DEFAULT_OUTPUT_DIR,
        write_files: bool = True,
    ) -> ScanOutputs:
        run_id = f"FGLS_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        scan_timestamp = datetime.now().isoformat()
        outputs = ScanOutputs(run_id=run_id, scan_timestamp=scan_timestamp)
        for job in self.job_provider(timeframe_ids, max_symbols, last_d_time, min_expiry_days):
            job = replace(job, last_d_time=normalize_last_d_time(job.last_d_time))
            outputs.total_jobs += 1
            record = _base_record(run_id, scan_timestamp, job)
            # print("########################################################################")
            try:
                print("*********************************************")
                print(f"Processing job: {job}")
                print("*********************************************")
                raw = self.setup_runner(job)
                if isinstance(raw, str):
                    raise RuntimeError(raw)
                formatted = self.formatter(raw, job)
                record, candidate = classify_formatted_result(
                    formatted,
                    job,
                    run_id=run_id,
                    scan_timestamp=scan_timestamp,
                    buy_rrr_threshold=self.buy_rrr_threshold,
                )
                outputs.processed += 1
                if candidate:
                    trade_signal = dict(formatted)
                    trade_signal["TRADE_TYPE"] = "BUY"
                    trade_signal["PREDICTION"] = "NA"
                    trade_signal["PROBABILITY"] = 0.0
                    trade_signal["EXP_NUM"] = job.exp_num
                    signal_result = self.signal_inserter(
                        trade_signal, 1, EXCHANGE_ID_NSEFO, job.timeframe_id
                    )
                    self.candidate_persister(candidate, job, signal_result)
                    outputs.candidates.append(candidate)
            except Exception as exc:
                outputs.errors += 1
                record["scan_status"] = "PROCESSING_ERROR"
                record["rejection_reason"] = str(exc)
                record["setup_result"] = None
            outputs.diagnostics.append(record)

        if write_files:
            write_outputs(outputs, output_dir)
        return outputs


def write_outputs(outputs: ScanOutputs, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    outputs.csv_path = output_dir / f"futures_readonly_candidates_{timestamp}.csv"
    outputs.jsonl_path = output_dir / f"futures_readonly_scan_{timestamp}.jsonl"

    candidate_fields = [
        "run_id",
        "scan_timestamp",
        "symbol",
        "expiry",
        "EXP_NUM",
        "timeframe_id",
        "time_list",
        "execute_timeframe",
        "exchange_id",
        "is_future",
        "contract",
        "side",
        "entry",
        "stop_loss",
        "target",
        "BUY_RRR",
        "zone_type",
        "zone_signature",
        "zone_metadata",
        "signal_timestamp",
        "PRICE_CMP",
    ]
    with outputs.csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=candidate_fields, extrasaction="ignore")
        writer.writeheader()
        for candidate in outputs.candidates:
            row = dict(candidate)
            row["time_list"] = json.dumps(row.get("time_list"), default=str)
            row["zone_metadata"] = json.dumps(row.get("zone_metadata"), default=str)
            writer.writerow(row)

    with outputs.jsonl_path.open("w", encoding="utf-8") as fh:
        for record in outputs.diagnostics:
            fh.write(json.dumps(record, default=str))
            fh.write("\n")


def print_candidate_table(outputs: ScanOutputs) -> None:
    print("\nPREDIXA FUTURES BUY CANDIDATES\n")
    if not outputs.candidates:
        print("NO QUALIFYING FUTURES BUY SETUPS FOUND")
    else:
        print(f"{'Symbol':<14} {'Expiry':<12} {'TF':<12} {'Entry':>10} {'Stop':>10} {'Target':>10} {'RRR':>7}")
        for candidate in outputs.candidates:
            print(
                f"{str(candidate.get('symbol', '')):<14} "
                f"{str(candidate.get('expiry', '')):<12} "
                f"{str(candidate.get('execute_timeframe', '')):<12} "
                f"{candidate.get('entry', ''):>10} "
                f"{candidate.get('stop_loss', ''):>10} "
                f"{candidate.get('target', ''):>10} "
                f"{candidate.get('BUY_RRR', ''):>7}"
            )

    print("")
    print(f"Total jobs: {outputs.total_jobs}")
    print(f"Processed: {outputs.processed}")
    print(f"Errors: {outputs.errors}")
    print(f"BUY candidates: {len(outputs.candidates)}")
    print(f"Output CSV: {outputs.csv_path}")
    print(f"Diagnostic JSONL: {outputs.jsonl_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only NSE futures graduated-live BUY scanner.")
    parser.add_argument("--timeframe", type=int, action="append", dest="timeframes", help="Futures timeframe id. Repeat to scan multiple ids.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--min-expiry-days", type=int, default=DEFAULT_MIN_EXPIRY_DAYS)
    parser.add_argument("--last-d-time", default=None)
    return parser.parse_args()


def main() -> int:
    print(SAFETY_BANNER)
    args = parse_args()
    scanner = FuturesGraduatedLiveReadonlyScanner()
    started = time.time()
    outputs = scanner.run(
        timeframe_ids=args.timeframes or DEFAULT_TIMEFRAME_IDS,
        max_symbols=args.max_symbols,
        last_d_time=args.last_d_time,
        min_expiry_days=args.min_expiry_days,
        output_dir=args.output_dir,
        write_files=True,
    )
    print_candidate_table(outputs)
    print(f"Elapsed seconds: {round(time.time() - started, 2)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
