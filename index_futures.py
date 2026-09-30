from fastapi import APIRouter, Request, HTTPException, WebSocket, WebSocketDisconnect
from shared.db.api_responce import APIResponse
from shared.db.dbconn import DBConnection
from shared.db.db_model import FuturesMaster, Index
from datetime import datetime, timedelta
from sqlalchemy import distinct
import logging
from shared.config.settings import fyers_config_credentials as fcc
import pandas as pd
from data_fetchers.fyers.indian_futures_data_updater import Indian_future_data_handler
import re
from data_fetchers.fyers.fyers_session import fyers_access_token_handler
from fyers_apiv3 import fyersModel
from data_fetchers.fyers.option_chain import get_futures_option_data
import websockets
from fastapi.responses import JSONResponse, HTMLResponse
import asyncio
import json
from shared.config.settings import stock_data_dir_config as sdc
import os
from dateutil.relativedelta import relativedelta

logger = logging.getLogger()
db = DBConnection()
router = APIRouter()


def extract_date(entry):
    match = re.search(r'\b(\d{2})\s([A-Za-z]{3})\s(\d{2})\b', entry)
    if match:
        year, month, day = match.groups()
        full_date_str = f"20{year} {month} {day}"
        return datetime.strptime(full_date_str, "%Y %b %d")
    return None


@router.get('/get_all_future_symbols', tags=['Futures'])
async def get_all_future_symbols(request: Request):
    df = pd.read_csv(fcc.FUTURE_SYMBOL_CSV_URL, header=None)
    apiresponce = APIResponse(request)
    try:
        session = db.get_session()
        symbols = session.query(distinct(FuturesMaster.symbol)).all()
        apiresponce.setMsg("success")
        apiresponce.setResponse([s[0] for s in symbols])
        session.close()
    except Exception as ex:
        session.rollback()
        logger.error(ex, stack_info=True)
        apiresponce.setMsg("failed")
        apiresponce.setResponse(str(ex))
    return apiresponce

# @router.get('/get_future_id_by_expiry_and_symbols', tags=['Futures'])
# async def get_future_id_by_expiry_and_symbols(symbol: str, exp_dt: str, request: Request):
#     apiresponce = APIResponse(request)
#     try:
#         session = get_session()
#         exp_dtt = date
#         symbols = session.query(FuturesMaster).filter(FuturesMaster.symbol == symbol).filter(FuturesMaster.expiry_date == ).all()
#         apiresponce.setMsg("success")
#         apiresponce.setResponse([s[0] for s in symbols])
#     except Exception as ex:
#         session.rollback()
#         logger.error(ex, stack_info=True)
#         apiresponce.setMsg("failed")
#         apiresponce.setResponse(str(ex))
#     return apiresponce


@router.get('/get_future_options_by_symbols', tags=['Futures'])
async def get_future_options_by_symbols(symbol: str, request: Request):
    apiresponce = APIResponse(request)
    try:
        session = db.get_session()
        symbols = session.query(distinct(FuturesMaster.symbol)).all()
        sym_list = [s[0] for s in symbols]
        if symbol not in sym_list:
            return HTTPException(status_code=404, detail=f"No futures found for symbol '{symbol}'")
        results = session.query(FuturesMaster).filter(FuturesMaster.symbol == symbol.upper()).order_by(FuturesMaster.expiry_date).all()
        apiresponce.setMsg("success")
        apiresponce.setResponse(results)
        session.close()
    except Exception as ex:
        session.rollback()
        logger.error(ex, stack_info=True)
        apiresponce.setMsg("failed")
        apiresponce.setResponse(str(ex))
    return apiresponce


@router.get('/get_all_stock_indices', tags=['Futures'])
async def get_future_options_by_symbols(request: Request, country_id: int, index_id: int = None):
    apiresponce = APIResponse(request)
    try:
        session = db.get_session()
        if index_id == None:
            indexes = session.query(Index).filter(Index.country_id == country_id, Index.is_active == 1).all()
        else:
            indexes = session.query(Index).filter(Index.index_id == index_id, Index.country_id == country_id, Index.is_active == 1).all()
        if indexes and len(indexes) > 0:
            apiresponce.setMsg("success")
            apiresponce.setResponse(indexes)
        else:
            apiresponce.setMsg("failed")
            apiresponce.setResponse("Not found !")
        session.close()
    except Exception as ex:
        session.rollback()
        logger.error(ex, stack_info=True)
        apiresponce.setMsg("failed")
        apiresponce.setResponse(str(ex))
    return apiresponce

@router.get('/add_new_future', tags=['Futures'])
async def add_new_future(request: Request):
    apiresponce = APIResponse(request)
    try:
        df = pd.read_csv(fcc.FUTURE_SYMBOL_CSV_URL, header=None)
        df = df[df[9].astype(str).str.contains(f"NSE:ABB", na=False)]
        df = df[df[9].astype(str).str.contains("FUT", na=False)]
        df["parsed_date"] = df[1].apply(extract_date)
        df = df.dropna(subset=["parsed_date"]).sort_values(by="parsed_date")
        df['parsed_date'] = df['parsed_date'].dt.strftime('%d-%m-%Y')
        # print(df[1])
        # print(df[9])
        print(df.columns)
        for i in list(df.columns):
            print(df[i])
        symbol = df.iloc[0, 1]
        symbol = symbol.split(' ')[0]
        print(symbol)
        exp_date = df.iloc[0, 21]
        print(exp_date)
        # fut = Indian_future_data_handler().get_latest_fyers_futures_symbol("ABB")
        # return fut
        # print(model.stock_name)
        # if bool(re.search(r'[^a-zA-Z0-9]', model.stock_name)):
        #     apiresponce.setMsg("failed")
        #     apiresponce.setResponse("Special characters not supported in stock name !")
        #     return apiresponce
        
        # model.symbol = model.symbol.upper()
        # session = db.get_session()
        # if model.country.lower().__contains__('us'):
        #     existing = session.query(Us_StockMaster).filter(Us_StockMaster.y_finance == model.symbol, 
        #                                                     Us_StockMaster.is_active == 1).all()
        # else:
        #     existing = session.query(Ind_StockMaster).filter(or_(Ind_StockMaster.y_finance == model.symbol,
        #                                                          Ind_StockMaster.kite_symbol == model.symbol,
        #                                                          Ind_StockMaster.fyers_symbol == model.symbol),
        #                                                     Ind_StockMaster.is_active == 1).all()
        # if existing and len(existing) > 0:
        #     session.close()
        #     apiresponce.setMsg("failed")
        #     apiresponce.setResponse("Stock already exists !")
        #     return apiresponce
        
        # valid_index_ids = session.query(Index.index_id).filter(Index.index_id.in_(model.index_ids)).all()
        # valid_index_ids = {idx[0] for idx in valid_index_ids}  # Extract IDs from tuples

        # invalid_ids = set(model.index_ids) - valid_index_ids
        # if invalid_ids:
        #     apiresponce.setMsg("invalid index")
        #     apiresponce.setResponse({"invalid_ids": list(invalid_ids)})
        #     session.close()
        #     return apiresponce

        # added = False
        # if model.country.lower().__contains__('us'):
        #     db_order = Us_StockMaster(stock_tick=model.stock_name, y_finance=model.symbol)
        #     added = True
        # else:
        #     y_fin_symbol = model.symbol.split(':')[1]
        #     y_fin_symbol = y_fin_symbol.split('-')[0]
        #     instrument_token = get_instrument_token(y_fin_symbol)
        #     #if instrument_token:
        #     db_order = Ind_StockMaster(stock_tick = model.stock_name, 
        #                                 y_finance = f"{y_fin_symbol}.NS", 
        #                                 kite_symbol = y_fin_symbol,
        #                                 kite_token = instrument_token,
        #                                 fyers_symbol = model.symbol)
        #     added = True
        #     # else:
        #     #     session.close()
        #     #     db.close_engine()
        #     #     apiresponce.setMsg("failed")
        #     #     apiresponce.setResponse("Instrument token not found for this stock !")
        #     #     return apiresponce
        # if added:
        #     session.add(db_order)
        #     session.commit()
        #     session.refresh(db_order)

        #     index_ids = model.index_ids

        #     if model.country.lower().__contains__('us'):
        #         mapping_entries = [
        #                     Us_StockIndexMap(stock_id=db_order.id, index_id=idx_id)
        #                     for idx_id in index_ids]
        #     else:
        #         mapping_entries = [
        #                     Ind_StockIndexMap(stock_id=db_order.id, index_id=idx_id)
        #                     for idx_id in index_ids]
        #     session.add_all(mapping_entries)
        #     session.commit()
        #     apiresponce.setMsg("success")
        #     apiresponce.setResponse({"stock_id": db_order.id, "country" : model.country})
        #     session.close()
    except Exception as ex:
        logger.exception(ex)
        logger.error(ex)
        apiresponce.setMsg("failed")
        apiresponce.setResponse(str(ex))
    return apiresponce


@router.get('/get_future_oi', tags=['Futures'])
async def get_future_oi(request: Request, symbol: str, expiry_date: str):
    apiresponce = APIResponse(request)
    try:
        res = get_futures_option_data(symbol, expiry_date)
        #print(res)
        if len(res) > 0:# and res["callOi"] is not None and res["putOi"] is not None:
            apiresponce.setMsg("success")
            apiresponce.setResponse(res)
        else:
            apiresponce.setMsg("failed")
            apiresponce.setResponse("No data found!")
    except Exception as ex:
        logger.error(ex, stack_info=True)
        apiresponce.setMsg("failed")
        apiresponce.setResponse(str(ex))
    return apiresponce


# @router.websocket("/ws/get_future_oi")
# async def ws_get_future_oi(websocket: WebSocket, 
#                          symbol: str,
#                          expiry_date: str):
#     await websocket.accept()
#     try:
#         option_chain_data = {}
#         start_time = datetime.now().timestamp()
#         while True:
#             res = get_futures_option_data(symbol, expiry_date)            
#             if len(res) > 0:
#                 option_chain_data = res
#                 await websocket.send_json(res)
#             elif len(option_chain_data) > 0:
#                 await websocket.send_json(option_chain_data)
#             else:
#                 await websocket.send_text("No data found!")
#             now_time = datetime.now().timestamp()
#             if (now_time - start_time) > 60*5:  # 5 minutes
#                 option_chain_data = {}
#             await asyncio.sleep(60)
#     except WebSocketDisconnect:
#         print("WebSocket Error: Client disconnected from WebSocket")
#     except asyncio.CancelledError:
#         return
#     except Exception as e:
#         logger.exception(e, exc_info=True, stack_info=True)
#         print("WebSocket Error:", e)



# # @router.get("/get_future_oi_from_websocket")
# # async def get_future_oi_from_websocket(symbol: str,
# #                                         expiry_date: str):
# #     try:
# #         async with websockets.connect(f"ws://103.13.113.132:2560/ws/get_future_oi?symbol={symbol}&expiry_date={expiry_date}") as websocket:
# #             # You can send a request to fetch data
# #             await websocket.send("fetch-latest")

# #             # Wait for the response from /ws
# #             response = await websocket.recv()
# #             try:
# #                 parsed = json.loads(response)
# #                 return JSONResponse(content={"response": parsed})
# #             except:
# #                 return JSONResponse(content={"response": response})
# #     except Exception as e:
# #         return JSONResponse(content={"error": str(e)}, status_code=500)




# @router.get("/get_future_oi_from_websocket", tags=['Futures'])
# async def get_future_oi_from_websocket(symbol: str, expiry_date: str):
#     html = f"""
#     <html>
#     <head>
#         <title>WebSocket JSON Viewer</title>
#         <style>
#             body {{
#                 font-family: monospace;
#                 background: #f9f9f9;
#                 padding: 20px;
#             }}
#             pre {{
#                 background: #272822;
#                 color: #f8f8f2;
#                 padding: 15px;
#                 border-radius: 8px;
#                 overflow-x: auto;
#                 white-space: pre-wrap;
#                 word-wrap: break-word;
#             }}
#         </style>
#     </head>
#     <body>
#         <h2>WebSocket JSON Response</h2>
#         <pre id="json">Waiting for data...</pre>

#         <script>
#             const symbol = "{symbol}";
#             const expiry_date = "{expiry_date}";
#             const ws = new WebSocket(`ws://103.13.113.132:2560/ws/get_future_oi?symbol={symbol}&expiry_date={expiry_date}`);

#             ws.onmessage = function(event) {{
#                 try {{
#                     let response = JSON.parse(event.data);
#                     document.getElementById("json").textContent = 
#                         JSON.stringify(response, null, 4); // pretty-print
#                 }} catch (e) {{
#                     document.getElementById("json").textContent = event.data;
#                 }}
#             }};

#             ws.onerror = function(event) {{
#                 console.error("WebSocket error:", event);
#                 document.getElementById("json").textContent = "WebSocket error!";
#             }};

#             ws.onclose = function() {{
#                 console.log("WebSocket closed");
#             }};
#         </script>
#     </body>
#     </html>
#     """
#     return HTMLResponse(html)


def get_nearest_expiry_date(dates):
    today = datetime.now().date()
    future_dates = [date for date in dates if (date-today) >= timedelta(days=5)]
    if future_dates:
        return future_dates[0]
    return None

@router.get('/future_liquidity_by_volume', tags=['Futures'])
async def future_liquidity_by_volume(request: Request):
    apiresponce = APIResponse(request)
    try:
        session = db.get_session()
        symbols = session.query(distinct(FuturesMaster.symbol)).all()
        sym_list = [s[0] for s in symbols]
        avg_traded_quantity = []
        nifty_data = []
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
                    # print('close', df.iloc[-1]['close'])
                    if df.iloc[-1]['close'] >= 100:
                        #print(df)
                        df['tradeDate'] = pd.to_datetime(df['tradeDate'], format='%d/%m/%Y %H:%M:%S')
                        #print(df)

                        current_date = datetime.now()#.strptime('%d/%m/%Y %H:%M:%S')
                        one_month_ago = current_date - relativedelta(months=1)
                        df_last_month = df[df['tradeDate'] >= one_month_ago]
                        #print(df_last_month)
                        if not df_last_month.empty:
                            #print(df_last_month['qty'].mean())
                            if symbol == "NIFTY" or symbol == "BANKNIFTY" or symbol == "FINNIFTY":
                                nifty_data.append({"symbol": symbol, "avg_qty": round(df_last_month['qty'].mean(), 2), "expiry_dates": exp_dates})
                            else:
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
        avg_traded_quantity = nifty_data + avg_traded_quantity
        apiresponce.setMsg("success")
        apiresponce.setResponse(avg_traded_quantity)
        session.close()
    except Exception as ex:
        session.rollback()
        logger.error(ex, stack_info=True)
        apiresponce.setMsg("failed")
        apiresponce.setResponse(str(ex))
    return apiresponce