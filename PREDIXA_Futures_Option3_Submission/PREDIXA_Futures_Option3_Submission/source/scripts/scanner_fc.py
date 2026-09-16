import json
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from shared.db.dbconn import DBConnection
from scripts.setup_engine_new import process_setup_fc, format_calculate_setup_response
from scripts.side_enablement_policy import SIDE_POLICY, Side, resolve_segment
from shared.db.db_model import FuturesMaster, TradeSignal
from concurrent.futures import ThreadPoolExecutor
from shared.db.db_utils import insert_trade_signals
# from nselib.derivatives import expiry_dates_future
from datetime import date
from sqlitedict import SqliteDict
from shared.utils.logger import logger
from sqlalchemy import distinct
from sqlalchemy import exists
import os
from shared.config.settings import stock_data_dir_config as sdc
import pandas as pd
from dateutil.relativedelta import relativedelta
import requests
from io import StringIO
import re

# Cut 1: FUT/MCX SELL path. SHORT gating is governed by SIDE_POLICY
# (side_enablement_policy.py); the legacy ENABLE_FC_SELL flag is retired.

dbc = DBConnection()
ldb = SqliteDict('finprod.sqlite', autocommit=False)


def get_latest_fyers_futures_symbol(product: str):
    try:
        url = "https://public.fyers.in/sym_details/NSE_FO.csv"
        response = requests.get(url, timeout=20)
        response.raise_for_status()

        df = pd.read_csv(StringIO(response.text), header=None)

        # symbol column
        df[9] = df[9].astype(str)
        df[1] = df[1].astype(str)

        # match only this product's futures
        # df = df[df[9].str.startswith(f"NSE:{product}", na=False)]
        # df = df[df[9].str.contains("FUT", na=False, regex=False)]
        df = df[df[9].str.match(rf"NSE:{product}\d{{2}}[A-Za-z]+FUT$", na=False)]

        if df.empty:
            print(f"No futures found for {product}")
            return []

        # parse expiry from description column
        df["parsed_date"] = df[1].apply(extract_date)
        df["parsed_date"] = pd.to_datetime(df["parsed_date"], errors="coerce")

        df = df.dropna(subset=["parsed_date"]).sort_values("parsed_date")

        if df.empty:
            print(f"No valid expiries found for {product}")
            return []

        # remove duplicate symbols if any
        df = df.drop_duplicates(subset=[9, "parsed_date"])

        result = df[[9, "parsed_date"]].head(3).copy()
        result.columns = ["symbol", "expiry"]
        result["expiry"] = result["expiry"].dt.strftime("%d-%m-%Y")

        return result.to_dict("records")
    except Exception as e:
        logger.exception(e)
        print(f"Error fetching latest symbol: {product} {e}")
        return []

def parse_expiry_date(expiry_str):
    """
    Supports:
    - '30-Mar-2026'
    - '30-03-2026'
    """
    for fmt in ("%d-%b-%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(expiry_str, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Unsupported expiry format: {expiry_str}")

def expiry_to_numeric(expiry_str):
    """
    '28-Apr-2026' -> 28042026
    '28-04-2026'  -> 28042026
    """
    return parse_expiry_date(expiry_str).strftime("%d%m%Y")

def extract_date(entry):
    try:
        if not entry or not isinstance(entry, str):
            return None

        match = re.search(r'\b(\d{1,2})\s([A-Za-z]{3})\s(\d{2})\b', entry.strip())
        if not match:
            return None

        day, month, year = match.groups()
        return datetime.strptime(f"{day} {month} 20{year}", "%d %b %Y")

    except Exception:
        return None

def is_expiry_valid(expiry_str, min_days=5, today=None):
    """
    Reject expiries with min_days or fewer days remaining.
    Keep only expiry if days_left > min_days.
    """
    if today is None:
        today = date.today()
    elif isinstance(today, str):
        today = datetime.strptime(today, "%d-%m-%Y").date()

    exp_date = parse_expiry_date(expiry_str)
    days_left = (exp_date - today).days
    return days_left > min_days

def get_all_future_stock_syms(time_fr: int = 1):
    db = dbc.get_session()
    res = db.query(FuturesMaster.symbol).distinct().filter(~exists().where(
                    (TradeSignal.stock_name == FuturesMaster.symbol) &
                    (TradeSignal.time_fr == time_fr) &
                    (TradeSignal.is_active == True) & 
                    (TradeSignal.exchange_id == 11))).all()
    return [row[0] for row in res]

def get_nse_stock_futures_expiries():
    """
    Example output:
    ['30-Mar-2026', '28-Apr-2026', '26-May-2026']
    """
    # expiries = expiry_dates_future()
    expiries = get_latest_fyers_futures_symbol("TCS")
    # print("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", expiries)
    expiries = [expiry['expiry'] for expiry in expiries]
    # print("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", expiries)

    return list(dict.fromkeys(expiries))

def build_futures_expiry_map(min_days=5, today=None, time_fr: int = 1, allowed_symbols=None):
    """
    Output:
    [
        {'symbol': 'RELIANCE', 'expiry': [28042026, 26052026]},
        {'symbol': 'SBIN', 'expiry': [28042026, 26052026]},
    ]
    """
    allowed = {str(symbol).strip().upper() for symbol in allowed_symbols} if allowed_symbols else None
    sorted_symbols_by_volume = future_liquidity_by_volume(time_fr, allowed_symbols=allowed)
    if len(sorted_symbols_by_volume) > 0:
        no_of_stocks = 50 if len(sorted_symbols_by_volume) > 50 else len(sorted_symbols_by_volume)
        sorted_symbols_by_volume = sorted_symbols_by_volume[0 : no_of_stocks]
        symbols = [item["symbol"] for item in sorted_symbols_by_volume]
    else:
        symbols = get_all_future_stock_syms(time_fr)
        if allowed is not None:
            symbols = [symbol for symbol in symbols if str(symbol).strip().upper() in allowed]
    raw_expiries = get_nse_stock_futures_expiries()
    print("raw_expiries", raw_expiries)
    valid_expiries = [
        expiry_to_numeric(exp)
        for exp in raw_expiries
        if is_expiry_valid(exp, min_days=min_days, today=today)
    ]
    valid_expiries = [valid_expiries[0]]
    # print(symbols)
    print("valid_expiries", valid_expiries)
    return [{"symbol": sym, "expiry": valid_expiries.copy()} for sym in symbols]



def get_nearest_expiry_date(dates):
    today = datetime.now().date()
    future_dates = [date for date in dates if (date-today) >= timedelta(days=5)]
    if future_dates:
        return future_dates[0]
    return None


def future_liquidity_by_volume(time_fr: int, allowed_symbols=None):
    try:
        session = dbc.get_session()
        # symbols = session.query(distinct(FuturesMaster.symbol)).all()
        res = session.query(FuturesMaster.symbol).distinct().filter(~exists().where(
                    (TradeSignal.stock_name == FuturesMaster.symbol) &
                    (TradeSignal.time_fr == time_fr) &
                    (TradeSignal.is_active == True) & 
                    (TradeSignal.exchange_id == 11))).all()
        allowed = {str(symbol).strip().upper() for symbol in allowed_symbols} if allowed_symbols else None
        sym_list = [s[0] for s in res]
        if allowed is not None:
            sym_list = [symbol for symbol in sym_list if str(symbol).strip().upper() in allowed]
        avg_traded_quantity = []
        for symbol in sym_list:
            results = session.query(FuturesMaster).filter(FuturesMaster.symbol == symbol.upper()).order_by(FuturesMaster.expiry_date).all()
            exp_dates = [r.expiry_date for r in results]
            nearest_exp = get_nearest_expiry_date(exp_dates)
            if nearest_exp:
                #print(f"Symbol: {symbol}, Nearest Expiry: {nearest_exp}")
                nearest_exp = nearest_exp.strftime('%d%m%Y')
                futute_data_dir = os.path.join(sdc.indian_stock_future_data_dir, 
                                               "latest_data_csv", 
                                               f"{symbol}_{nearest_exp}_daily.csv")
                if os.path.exists(futute_data_dir):
                    #print(f"Symbol: {symbol}, File Exists")
                    df = pd.read_csv(futute_data_dir)
                    #print(df)
                    df['tradeDate'] = pd.to_datetime(df['tradeDate'], format='%d/%m/%Y %H:%M:%S')
                    #print(df)

                    current_date = datetime.now()#.strptime('%d/%m/%Y %H:%M:%S')
                    one_month_ago = current_date - relativedelta(months=1)
                    df_last_month = df[df['tradeDate'] >= one_month_ago]
                    #print(df_last_month)
                    if not df_last_month.empty:
                        #print(df_last_month['qty'].mean())
                        avg_traded_quantity.append({"symbol": symbol, "avg_qty": round(df_last_month['qty'].mean(), 2), "expiry_dates": exp_dates})                    
                else:
                    print(f"Symbol: {symbol}, File Not Exists: {futute_data_dir}")
        # print(results[0].expiry_date)
        # print(type(results[0].expiry_date))
        if len(avg_traded_quantity) > 0:
            avg_traded_quantity = sorted(
                            avg_traded_quantity,
                            key=lambda qty: qty['avg_qty'],
                            reverse=True)
        session.close()
    except Exception as ex:
        session.rollback()
        logger.error(ex, stack_info=True)
    return avg_traded_quantity


def build_commodity_expiry_map(min_days=5, today=None, time_fr: int = 1):
    """
    commodity_expiry_map input example:
    [
        {'symbol': 'GOLDM', 'expiry': ['03-04-2026', '05-05-2026', '05-06-2026']},
        {'symbol': 'NATGASM', 'expiry': ['26-03-2026', '27-04-2026', '26-05-2026']},
    ]

    Output:
    [
        {'symbol': 'GOLDM', 'expiry': [3042026, 5052026, 5062026]},
        {'symbol': 'NATGASM', 'expiry': [27042026, 26052026]},
    ]
    """
    commodity_expiry_map = ldb['indian_commodity_expiries']
    result = []
    session = dbc.get_session()
    for item in commodity_expiry_map:
        symbol = item.get("symbol")
        expiries = item.get("expiry", [])
        valid_expiries = []
        for exp in expiries:
            exp_dt = expiry_to_numeric(exp)
            # BE-DUP-1 fix: de-dup against already-active COMMODITY signals.
            # Was exchange_id==8 (cash); commodity signals are stored at 10 (see ~L309),
            # so the check never matched -> every cycle re-emitted the still-pending setup
            # (duplicates accumulate, all fire at execution). Also restructured: the skip now
            # actually gates valid_expiries (previously a structural no-op).
            existing = session.query(TradeSignal).filter(
                TradeSignal.stock_name == symbol,
                TradeSignal.time_fr == time_fr,
                TradeSignal.is_active == True,
                TradeSignal.exp_date == exp_dt,
                TradeSignal.exchange_id == 10,   # COMMODITY (was 8 = cash)
            ).first()
            if existing:
                continue
            if is_near_expiry_valid(exp, max_days=max_days, today=today):
                valid_expiries.append(exp_dt)

        if valid_expiries:
            result.append({
                "symbol": symbol,
                "expiry": valid_expiries
            })
    session.close()
    return result



@dataclass
class ScanJob_FC:
    time_lists: List[List[str]]   # e.g. [["Monthly","Weekly","Daily"], ["Weekly","Daily","60m"], ...]
    time_frame: int 
    last_d_time: Any              # whatever your load_preprocess_data expects (datetime / int / etc.)
    output_path: str = "scan_results_fc.json"
    append: bool = True
    is_future: bool = True



class SetupScannerOrchestrator_FC:
    """
    Runs your SD Engine pipeline across symbols and timeframe triplets,
    and writes formatted results to JSONL.
    """

    def __init__(self):
        self.run_id_counter = 0

    def _new_run_id(self) -> str:
        self.run_id_counter += 1
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"RUN_{ts}_{self.run_id_counter}"

    @staticmethod
    def _infer_execute_tf(time_list: List[str]) -> str:
        # Your engine expects time_list = [E, A, X]
        # So execute timeframe is last item
        return time_list[-1]

    def run(self, job: ScanJob_FC) -> Dict[str, Any]:
        run_id = self._new_run_id()
        started = time.time()
        exchange_id = 11 if job.is_future else 10
        mode = "a" if job.append else "w"
        total = 0
        ok = 0
        failed = 0

        with open(job.output_path, mode, encoding="utf-8") as f:
            all_job_syms = build_futures_expiry_map() if job.is_future else build_commodity_expiry_map()
            for items in all_job_syms:
                symbol = items["symbol"]
                expiries = items["expiry"]
                for EXP_NUM in expiries:
                    for time_list in job.time_lists:
                        total += 1
                        execute_tf = self._infer_execute_tf(time_list)

                        record: Dict[str, Any] = {
                            "run_id": run_id,
                            "timestamp": datetime.now().isoformat(),
                            "symbol": symbol,
                            "time_list": time_list,
                            "execute_tf": execute_tf,
                            "exchange_id": exchange_id,
                            "is_future": job.is_future,
                        }

                        try:
                            raw = process_setup_fc(symbol, time_list, EXP_NUM, job.last_d_time, job.is_future)
                            # If process_setup returns str(error), treat as error
                            if isinstance(raw, str):
                                raise RuntimeError(raw)

                            formatted = format_calculate_setup_response(
                                raw,
                                stock_name=symbol,
                                time_fr=job.time_frame,
                                exp_num=EXP_NUM,
                                last_d_time=job.last_d_time,
                                is_future=job.is_future,
                                is_cash=False
                            )

                            record["status"] = "OK"
                            record["setup"] = formatted
                            ok += 1
                            if 'BUY' in formatted and formatted['BUY_RRR'] >= 2.1:
                                formatted['TRADE_TYPE'] = 'BUY'
                                formatted['PREDICTION'] = "NA"
                                formatted['PROBABILITY'] = 0.0
                                formatted['EXP_NUM'] = EXP_NUM
                                insert_trade_signals(formatted, 1, exchange_id, job.time_frame)
                                # return ("BUY", result['BUY'], stock, tday)
                            
                            if ('SELL' in formatted
                                    and formatted.get('SELL_RRR', 0) >= 2.1
                                    and SIDE_POLICY.is_enabled(resolve_segment(is_future=job.is_future), Side.SHORT)):
                                formatted['TRADE_TYPE'] = 'SELL'
                                formatted['PREDICTION'] = "NA"
                                formatted['PROBABILITY'] = 0.0
                                formatted['EXP_NUM'] = EXP_NUM
                                insert_trade_signals(formatted, 1, exchange_id, job.time_frame)


                        except Exception as e:
                            record["status"] = "ERROR"
                            record["error"] = str(e)
                            failed += 1
                            logger.error(f"Error processing {symbol} {time_list}: {e}", exc_info=True, stack_info=True)

                        # Write one JSON per line (JSONL)
                        f.write(json.dumps(record, default=str))
                        f.write("\n")

        return {
            "run_id": run_id,
            "output_path": job.output_path,
            "total_jobs": total,
            "ok": ok,
            "failed": failed,
            "duration_seconds": round(time.time() - started, 2),
        }
