from fastapi import FastAPI, Request, Response
from fastapi.responses import RedirectResponse, FileResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from shared.db.api_responce import APIResponse
from data_fetchers.nselib.futures_utility import Future_Data_Handler
from shared.db.db_model import FuturesMaster 
from apps.backend.services.update_futures_category import refresh_futures_master_and_update_categories
from nselib import derivatives
from sqlalchemy import distinct
from shared.db.dbconn import DBConnection
from shared.config.settings import DB_Config, stock_data_dir_config, stock_logic_config, validate_required_config
import uvicorn
import logging, os
import pandas as pd
import zipfile
import io
from datetime import datetime
from apps.backend.api.indian_stock_futures import router as future_stock_chart_router
from apps.backend.api.indian_commodity_futures import router as commodity_stock_chart_router
from apps.backend.api.alerts import router as alert_router
from apps.backend.api.user_device_token import router as udt_router
from apps.backend.api.available_trades import router as avl_trades_router
from apps.backend.api.commodity_orders import router as commodity_order_router
from apps.backend.api.future_orders import router as future_order_router
from apps.backend.api.stock_model import router as stock_model_router
from apps.backend.api.cash_orders import router as cash_order_router
from apps.backend.api.cash import router as cash_router
from data_fetchers.kiteconnect.kite_session import *
from apps.backend.api.fyers_order_management import router as fyers_order_router
from apps.backend.api.bucket_orders import router as bucket_order_router
from apps.backend.api.holdings import router as holdings_router
from apps.backend.api.bucket_orders_future import router as bucket_order_future_router
from apps.backend.api.bucket_orders_commodity import router as bucket_order_commodity_router
import subprocess
from typing import Optional, Any

logger = logging.getLogger()
dbc = DBConnection()
app = FastAPI()


def reconcile_futures_on_startup():
    """Run fail-closed futures recovery using broker reads only."""
    from scripts.futures_state_store import StateStore
    from scripts.futures_risk_engine import RiskEngine
    from scripts.futures_reconciliation import build_approved_contracts, reconcile_futures_startup
    from shared.db.fyers_req_model import get_fyers
    from shared.db.db_model import OMSOrderBucketFuture

    store = StateStore(dbc.engine)
    store.require_initialized()
    store.sync_owner_approved_gates()
    snapshot = store.snapshot()
    session = dbc.get_session()
    try:
        symbols = set((snapshot.get("approved") or {}).keys())
        for reservation in (snapshot.get("reservations") or {}).values():
            if reservation.get("symbol"):
                symbols.add(reservation["symbol"])
        rows = session.query(OMSOrderBucketFuture.stock_tick).filter(
            OMSOrderBucketFuture.approval_status.in_(("pending", "approved"))
        ).all()
        symbols.update(row[0] for row in rows if row[0])
        approved = build_approved_contracts(session, symbols)
    finally:
        session.close()

    risk = RiskEngine(store)
    broker = get_fyers()
    ready = reconcile_futures_startup(
        risk=risk, broker=broker, approved=approved,
        gates=snapshot.get("gates") or {}, source_verified=True,
    )
    if ready:
        logger.info("FUTURES_RECONCILIATION_READY")
    else:
        logger.error("FUT_REJECT_STATE_RECONCILIATION_FAILED")
    return ready


@app.on_event("startup")
def futures_startup_reconciliation():
    try:
        reconcile_futures_on_startup()
    except Exception:
        # Futures approval remains fail-closed; non-trading API routes can run.
        logger.exception("FUT_REJECT_STATE_RECONCILIATION_FAILED")

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

validate_required_config()

@app.get("/")
async def index():
    return RedirectResponse("/docs")

@app.get("/update_token")
async def update_kite_token(request: Request, request_token: str):
    apiresponce = APIResponse(request)
    try:
        logger.info(request_token)
        data = kite.generate_session(request_token, api_secret=kcfg.api_secret)
        save_access_token(data)
        apiresponce.setMsg("success")
        apiresponce.setResponse({'request_token' : request_token})
        subprocess.run([f"{stock_data_dir_config.project_root}/manage_dashboard.sh", "restart"])
    except Exception as ex:
        logger.error(ex, stack_info=True)
        apiresponce.setMsg("failed")
        apiresponce.setResponse(str(ex))
    return apiresponce


@app.get("/validate_token")
async def validate_token(request: Request):
    apiresponce = APIResponse(request)
    try:
        acc_t = load_access_token()['access_token']
        print(f"Access Token {acc_t}")
        logger.info(f"Access Token {acc_t}")
        res = is_token_valid(acc_t)
        apiresponce.setMsg("success")
        apiresponce.setResponse({'valid': res})
    except Exception as ex:
        logger.error(ex, stack_info=True)
        apiresponce.setMsg("failed")
        apiresponce.setResponse(str(ex))
    return apiresponce



@app.get('/db_update_futures_category')
async def db_update_futures_category(request: Request):
    apiresponce = APIResponse(request)
    try:
        map_dict = {}
        session = dbc.get_session()
        symbols = session.query(distinct(FuturesMaster.symbol)).all()
        sym_list = [s[0] for s in symbols]
        exp_list = derivatives.expiry_dates_future()
        converted_list = [
                            datetime.strptime(date_str, "%d-%b-%Y").strftime("%Y-%m-%d")
                            for date_str in exp_list
                            ]
        for items in sym_list:
            map_dict[items] = converted_list
        refresh_futures_master_and_update_categories(DB_Config.db_config, map_dict)
        apiresponce.setMsg("success")
        # apiresponce.setResponse(map_dict)
    except Exception as ex:
        logger.error(ex, stack_info=True)
        apiresponce.setMsg("failed")
        apiresponce.setResponse(str(ex))
    return apiresponce


@app.get("/get_ohlc_stock_data")
async def get_ohlc_stock_data(tick: str, category: str, start_date: str, end_date: str, request: Request, responce: Response, expiry_date: Optional[str] = None, time_frame: Optional[Any] = None, single_time_frame: Optional[Any] = None):
    apiresponce = APIResponse(request)
    try:
        if time_frame in [None, '', "null"]:
            time_frame = None
        if single_time_frame in [None, '', "null"]:
            single_time_frame = None

        if time_frame == None and single_time_frame == None:
            raise ValueError("Time Frame must be provided")
        if time_frame != None and single_time_frame != None:
            raise ValueError("Provide either Three Time Frames or Single Time Frame, not both")
        elif time_frame == None and single_time_frame != None:
            time_frame = single_time_frame
        
        category = category.upper().strip()
        allowed_categories = ["NSE", "NSEFO", "MCX"]
        if category not in allowed_categories:
            raise ValueError(f"Invalid category. Allowed categories are: {allowed_categories}")
        
        files_to_process = []
        if category == "NSE":
            csv_dir = stock_data_dir_config.indian_stock_data_dir
            # csv_dir = "/home/ispeck/STOCK_PROJECT/STP_LATEST_V6_CUT/data/indian_stock_data"
            time_list = getattr(stock_logic_config, f"TIME_FRAME_EXE_{time_frame}")
            if single_time_frame:
                time_list = [time_list[-1]]

            for frames in time_list:
                file_name = f"{tick}_{frames}.csv"
                file_path = os.path.join(csv_dir, "latest_data_csv", file_name)
                files_to_process.append((file_name, file_path))



        else:
            if not expiry_date:
                raise ValueError(f"expiry_date is required for category {category}")
            if category == "NSEFO":
                csv_dir = stock_data_dir_config.indian_stock_future_data_dir
                time_list = getattr(stock_logic_config, f"FUTURE_TIME_FRAME_{time_frame}")
            elif category == "MCX":
                csv_dir = stock_data_dir_config.indian_commodity_data
                time_list = getattr(stock_logic_config, f"COMMODITY_TIME_FRAME_{time_frame}")
            
            if single_time_frame:
                time_list = [time_list[-1]]

            expiry_date = pd.to_datetime(expiry_date).strftime("%d%m%Y")
            for frames in time_list:
                file_name = f"{tick}_{expiry_date}_{frames}.csv"
                file_path = os.path.join(csv_dir, "latest_data_csv", file_name)
                files_to_process.append((file_name, file_path))

            # for frames in time_list:
                # files_to_process.append(f"{tick}_{frames}.csv")
        
        # file_path = os.path.join(csv_dir, "latest_data_csv", file_name)
        # print(file_path)
        # 
        # file_path = f"{csv_dir}/latest_data_csv/{tick}_{time_frame}.csv"
        # if not os.path.exists(file_path):
        #     raise FileNotFoundError(f"CSV file not found for tick: {tick}")

        start_dt = pd.to_datetime(start_date).date()
        end_dt = pd.to_datetime(end_date).date()
        if start_dt > end_dt:
            raise ValueError("start_date cannot be greater than end_date")

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zip_file:
            for file_name, file_path in files_to_process:
                if not os.path.exists(file_path):
                    #print(file_path)
                    continue
                
                #print(file_path)
                df = pd.read_csv(file_path)
                df['tradeDateTemp'] = pd.to_datetime(df['tradeDate'], dayfirst=True).dt.date
                #df['tradeDate'] = pd.to_datetime(df['tradeDate'], dayfirst=True).dt
                date_col = 'tradeDateTemp'
                df[date_col] = pd.to_datetime(df[date_col], dayfirst=True).dt.date
                csv_output = io.StringIO()
                filtered_df = df[(df[date_col] >= start_dt) & (df[date_col] <= end_dt)]
                try:
                    filtered_df.drop(columns = ['tradeDateTemp'], inplace = True)
                except Exception as exx:
                    pass
                filtered_df.to_csv(csv_output, index=False)
                zip_file.writestr(file_name, csv_output.getvalue())

        # output = io.StringIO()
        # filtered_df.to_csv(output, index=False)
        zip_buffer.seek(0)
        return StreamingResponse(
            zip_buffer,
            media_type="application/zip",
            headers={
                "Content-Disposition": f"attachment; filename={category}_{tick}_{start_date}_{end_date}_{'-'.join(time_list)}.zip"
            }
        )
    except Exception as ex:
        logger.error(ex, stack_info=True)
        apiresponce.setMsg("failed")
        apiresponce.setResponse(str(ex))
        apiresponce.setHeaderStatus(responce, 400)
    return apiresponce



app.include_router(future_stock_chart_router)
app.include_router(alert_router)
app.include_router(udt_router)
app.include_router(commodity_stock_chart_router)
app.include_router(avl_trades_router)
app.include_router(commodity_order_router)
app.include_router(future_order_router)
app.include_router(stock_model_router)
app.include_router(cash_order_router)
app.include_router(cash_router)
app.include_router(fyers_order_router)
app.include_router(bucket_order_router)
app.include_router(holdings_router)
app.include_router(bucket_order_future_router)
app.include_router(bucket_order_commodity_router)


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=2570, reload=False)
