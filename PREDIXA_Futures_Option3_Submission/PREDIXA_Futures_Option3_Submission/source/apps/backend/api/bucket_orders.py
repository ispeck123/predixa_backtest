from fastapi import APIRouter, HTTPException, Request
from shared.db.db_model import OMSOrderBucket, Order, Ind_StockMaster
from shared.db.req_model import BucketOrderBase, Update_BucketOrderBase, GetOrder, ApproveReject_BucketOrderBase
from shared.db.fyers_req_model import GttLeg, GttOrderInfo, SingleGttOrderRequest, get_fyers
from shared.db.dbconn import DBConnection
from shared.db.api_responce import APIResponse
import logging
from datetime import datetime
from sqlalchemy import text, or_
import ast, re
from data_fetchers.fyers.get_stock_prices import StockPriceFetcher
from data_fetchers.fyers.fyers_utils import normalize_cash_oco_prices


logger = logging.getLogger("fyers_orders")
logging.basicConfig(level=logging.INFO)

db = DBConnection()
router = APIRouter()
spf = StockPriceFetcher()


def safe_parse_order_status(order_status):
    if not isinstance(order_status, str):
        return order_status  # Already a dict

    if order_status.strip().startswith("{"):
        try:
            # Clean Timestamp if present
            order_status = re.sub(r"Timestamp\('(.*?)'\)", r"'\1'", order_status)
            return ast.literal_eval(order_status)
        except Exception as e:
            logging.error(f"Failed to eval order_status: {order_status} — {e}")
            return {"status": "unknown", "error": str(e)}
    else:
        # It's a simple status string like 'PENDING'
        return {"status": order_status.strip().lower()}


@router.post("/get/bucket/order/count", tags=["bucket_orders"])
async def get_bucket_orders_count(req_model: GetOrder, request: Request):
    apiresponce = APIResponse(request)
    try:
        if req_model.time_frame == None or req_model.time_frame == 'null' or req_model.time_frame == '':
            req_model.time_frame = '%%'
        
        if req_model.order_type.lower() == "all":
            order_types = ("buy", "sell", "Buy", "Sell", "BUY", "SELL")
        else:
            order_types = (req_model.order_type.lower(), req_model.order_type.upper(), req_model.order_type.capitalize())

        if req_model.prediction_type == None or req_model.prediction_type == 'null' or req_model.prediction_type == '':
            prediction_type = " "
        else:
            prediction_type = f"and arima_ab_model_prediction like '%{req_model.prediction_type}%'"
        
        req_model.status = req_model.status.strip().lower()

        if req_model.status == 'all':
            print('1')
            total_count = db.raw_query(text(f"""select count(*) as count from oms_order_bucket 
                                where country_id = {req_model.country_id} 
                                and date(purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                and time_frame like '{req_model.time_frame}' 
                                and order_type in {order_types}
                                {prediction_type};"""))
        elif req_model.status != 'pending' and req_model.status != 'progress':
            print('2')
            total_count = db.raw_query(text(f"""select count(*) as count from oms_order_bucket 
                                where country_id = {req_model.country_id} 
                                and date(entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                and order_status like '%{req_model.status}%'
                                and time_frame like '{req_model.time_frame}' 
                                and order_type in {order_types}
                                {prediction_type};"""))
        elif req_model.status == 'pending':
            print('3')
            total_count = db.raw_query(text(f"""select count(*) as count from oms_order_bucket 
                                where country_id = {req_model.country_id} 
                                and date(purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                and order_status is null
                                and time_frame like '{req_model.time_frame}'
                                and order_type in {order_types}
                                {prediction_type};"""))
        elif req_model.status == 'progress':
            print('4')
            total_count = db.raw_query(text(f"""select count(*) as count from oms_order_bucket 
                                where country_id = {req_model.country_id} 
                                and date(entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                and order_status like '%pending%'
                                and time_frame like '{req_model.time_frame}'
                                and order_type in {order_types}
                                {prediction_type};"""))
        print("total_count:", total_count)
        if total_count and len(total_count) > 0:
            apiresponce.setMsg("success")
            apiresponce.setResponse(total_count)
        else:
            apiresponce.setMsg("success")
            apiresponce.setResponse([{"count": 0}])
    except Exception as ex:
        logger.exception(ex)
        logger.error(ex)
        apiresponce.setMsg("failed")
        apiresponce.setResponse(str(ex))
    return apiresponce


@router.post("/get/bucket/orders", tags=["bucket_orders"])
async def get_bucket_orders(req_model: GetOrder, request: Request):
    apiresponce = APIResponse(request)
    try:
        # if req_model.stock_tick == None or req_model.stock_tick == 'null' or req_model.stock_tick == '':
        #     req_model.stock_tick = ''
        # req_model.stock_tick = '%' + req_model.stock_tick + '%'
        try:
            req_model.page_no = int(req_model.page_no)
        except Exception as er:
            print("page_no:", req_model.page_no)
            req_model.page_no = None
        
        try:
            req_model.trade_id = int(req_model.trade_id)
        except Exception as er:
            req_model.trade_id = None

        if req_model.time_frame == None or req_model.time_frame == 'null' or req_model.time_frame == '':
            req_model.time_frame = '%%'
        
        if req_model.order_by_purchased_cmp_date:
            order_by = "oob.purchased_cmp_date desc"
        else:
            order_by = "oob.stock_tick asc"
        
        if req_model.order_type.lower() == "all":
            order_types = ("buy", "sell", "Buy", "Sell", "BUY", "SELL")
        else:
            order_types = (req_model.order_type.lower(), req_model.order_type.upper(), req_model.order_type.capitalize())

        if req_model.prediction_type == None or req_model.prediction_type == 'null' or req_model.prediction_type == '':
            prediction_type = " "
        else:
            prediction_type = f"and oob.arima_ab_model_prediction like '%{req_model.prediction_type}%'"
        
        req_model.status = req_model.status.strip().lower()

        res = []


        if (req_model.trade_id != None and req_model.trade_id != 'null' and req_model.trade_id != ''):
            if req_model.status == 'all':
                print('1')
                total_res = db.raw_query(text(f"""select oob.order_id, oob.trade_signal_id, oob.stock_tick, oob.stock_id, oob.country_id, 
                                                oob.time_frame, oob.order_type, oob.entry_price, oob.stoploss_price, oob.target_price, 
                                                oob.stock_quantity, oob.purchased_cmp_date, oob.purchased_on, oob.order_status, oob.is_trade_started, 
                                                oob.is_evaluated, oob.entry_timestamp, oob.completed_on, oob.lstm_rf_model_prediction, oob.lstm_rf_model_prob, 
                                                oob.arima_ab_model_prediction, oob.arima_ab_model_prob, oob.approval_status, oob.approved_by, oob.approved_at, 
                                                oob.bucket_id, ism.fyers_symbol, "NSE" as exchange 
                                                from oms_order_bucket as oob
                                                inner join ind_stock_master as ism on oob.stock_id = ism.id
                                                where oob.country_id = {req_model.country_id} 
                                                and date(oob.purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and oob.time_frame like '{req_model.time_frame}' 
                                                and oob.order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by};"""))
            elif req_model.status != 'pending' and req_model.status != 'progress':
                print('2')
                total_res = db.raw_query(text(f"""select oob.order_id, oob.trade_signal_id, oob.stock_tick, oob.stock_id, oob.country_id, 
                                                oob.time_frame, oob.order_type, oob.entry_price, oob.stoploss_price, oob.target_price, 
                                                oob.stock_quantity, oob.purchased_cmp_date, oob.purchased_on, oob.order_status, oob.is_trade_started, 
                                                oob.is_evaluated, oob.entry_timestamp, oob.completed_on, oob.lstm_rf_model_prediction, oob.lstm_rf_model_prob, 
                                                oob.arima_ab_model_prediction, oob.arima_ab_model_prob, oob.approval_status, oob.approved_by, oob.approved_at, 
                                                oob.bucket_id, ism.fyers_symbol, "NSE" as exchange 
                                                from oms_order_bucket as oob
                                                inner join ind_stock_master as ism on oob.stock_id = ism.id
                                                where oob.country_id = {req_model.country_id} 
                                                and date(oob.entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and oob.order_status like '%{req_model.status}%'
                                                and oob.time_frame like '{req_model.time_frame}' 
                                                and oob.order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by};"""))
            elif req_model.status == 'pending':
                print('3')
                total_res = db.raw_query(text(f"""select oob.order_id, oob.trade_signal_id, oob.stock_tick, oob.stock_id, oob.country_id, 
                                                oob.time_frame, oob.order_type, oob.entry_price, oob.stoploss_price, oob.target_price, 
                                                oob.stock_quantity, oob.purchased_cmp_date, oob.purchased_on, oob.order_status, oob.is_trade_started, 
                                                oob.is_evaluated, oob.entry_timestamp, oob.completed_on, oob.lstm_rf_model_prediction, oob.lstm_rf_model_prob, 
                                                oob.arima_ab_model_prediction, oob.arima_ab_model_prob, oob.approval_status, oob.approved_by, oob.approved_at, 
                                                oob.bucket_id, ism.fyers_symbol, "NSE" as exchange 
                                                from oms_order_bucket as oob
                                                inner join ind_stock_master as ism on oob.stock_id = ism.id
                                                where oob.country_id = {req_model.country_id} 
                                                and date(oob.purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and oob.order_status is null
                                                and oob.time_frame like '{req_model.time_frame}'
                                                and oob.order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by};"""))
            elif req_model.status == 'progress':
                print('4')
                total_res = db.raw_query(text(f"""select oob.order_id, oob.trade_signal_id, oob.stock_tick, oob.stock_id, oob.country_id, 
                                                oob.time_frame, oob.order_type, oob.entry_price, oob.stoploss_price, oob.target_price, 
                                                oob.stock_quantity, oob.purchased_cmp_date, oob.purchased_on, oob.order_status, oob.is_trade_started, 
                                                oob.is_evaluated, oob.entry_timestamp, oob.completed_on, oob.lstm_rf_model_prediction, oob.lstm_rf_model_prob, 
                                                oob.arima_ab_model_prediction, oob.arima_ab_model_prob, oob.approval_status, oob.approved_by, oob.approved_at, 
                                                oob.bucket_id, ism.fyers_symbol, "NSE" as exchange 
                                                from oms_order_bucket as oob
                                                inner join ind_stock_master as ism on oob.stock_id = ism.id
                                                where oob.country_id = {req_model.country_id} 
                                                and date(oob.entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and oob.order_status like '%pending%'
                                                and oob.time_frame like '{req_model.time_frame}'
                                                and oob.order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by};"""))
            #print("total_res:", total_res)
            found = False
            for i in range(len(total_res)):
                if req_model.trade_id == total_res[i]['trade_signal_id']:
                    found = i+1
                    break
            print("found:", found)
            if found:
                req_model.page_no = ((found - 1) // req_model.limit) + 1
                offset = (req_model.page_no - 1) * req_model.limit
                #print("page_no:", req_model.page_no)
                #print("offset:", offset)
                res = total_res[offset : offset + req_model.limit]
            else:
                pass

        elif (req_model.stock_tick == None or req_model.stock_tick == 'null' or req_model.stock_tick == '') and (req_model.page_no == None or req_model.page_no == 'null' or req_model.page_no == ''):
            req_model.page_no = 1
            offset = (req_model.page_no - 1) * req_model.limit
            if req_model.status == 'all':
                print('5')
                res = db.raw_query(text(f"""select oob.order_id, oob.trade_signal_id, oob.stock_tick, oob.stock_id, oob.country_id, 
                                                oob.time_frame, oob.order_type, oob.entry_price, oob.stoploss_price, oob.target_price, 
                                                oob.stock_quantity, oob.purchased_cmp_date, oob.purchased_on, oob.order_status, oob.is_trade_started, 
                                                oob.is_evaluated, oob.entry_timestamp, oob.completed_on, oob.lstm_rf_model_prediction, oob.lstm_rf_model_prob, 
                                                oob.arima_ab_model_prediction, oob.arima_ab_model_prob, oob.approval_status, oob.approved_by, oob.approved_at, 
                                                oob.bucket_id, ism.fyers_symbol, "NSE" as exchange 
                                                from oms_order_bucket as oob
                                                inner join ind_stock_master as ism on oob.stock_id = ism.id
                                                where oob.country_id = {req_model.country_id} 
                                                and date(oob.purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and oob.time_frame like '{req_model.time_frame}'
                                                and oob.order_type in {order_types} 
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
                
            elif req_model.status != 'pending' and req_model.status != 'progress':
                print('6')
                res = db.raw_query(text(f"""select oob.order_id, oob.trade_signal_id, oob.stock_tick, oob.stock_id, oob.country_id, 
                                                oob.time_frame, oob.order_type, oob.entry_price, oob.stoploss_price, oob.target_price, 
                                                oob.stock_quantity, oob.purchased_cmp_date, oob.purchased_on, oob.order_status, oob.is_trade_started, 
                                                oob.is_evaluated, oob.entry_timestamp, oob.completed_on, oob.lstm_rf_model_prediction, oob.lstm_rf_model_prob, 
                                                oob.arima_ab_model_prediction, oob.arima_ab_model_prob, oob.approval_status, oob.approved_by, oob.approved_at, 
                                                oob.bucket_id, ism.fyers_symbol, "NSE" as exchange 
                                                from oms_order_bucket as oob
                                                inner join ind_stock_master as ism on oob.stock_id = ism.id
                                                where oob.country_id = {req_model.country_id} 
                                                and date(oob.entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and oob.order_status like '%{req_model.status}%'
                                                and oob.time_frame like '{req_model.time_frame}'
                                                and oob.order_type in {order_types} 
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
            elif req_model.status == 'pending':
                print('7')
                res = db.raw_query(text(f"""select oob.order_id, oob.trade_signal_id, oob.stock_tick, oob.stock_id, oob.country_id, 
                                                oob.time_frame, oob.order_type, oob.entry_price, oob.stoploss_price, oob.target_price, 
                                                oob.stock_quantity, oob.purchased_cmp_date, oob.purchased_on, oob.order_status, oob.is_trade_started, 
                                                oob.is_evaluated, oob.entry_timestamp, oob.completed_on, oob.lstm_rf_model_prediction, oob.lstm_rf_model_prob, 
                                                oob.arima_ab_model_prediction, oob.arima_ab_model_prob, oob.approval_status, oob.approved_by, oob.approved_at, 
                                                oob.bucket_id, ism.fyers_symbol, "NSE" as exchange 
                                                from oms_order_bucket as oob
                                                inner join ind_stock_master as ism on oob.stock_id = ism.id
                                                where oob.country_id = {req_model.country_id} 
                                                and date(oob.purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and oob.order_status is null
                                                and oob.time_frame like '{req_model.time_frame}'
                                                and oob.order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
            elif req_model.status == 'progress':
                print('8')
                res = db.raw_query(text(f"""select oob.order_id, oob.trade_signal_id, oob.stock_tick, oob.stock_id, oob.country_id, 
                                                oob.time_frame, oob.order_type, oob.entry_price, oob.stoploss_price, oob.target_price, 
                                                oob.stock_quantity, oob.purchased_cmp_date, oob.purchased_on, oob.order_status, oob.is_trade_started, 
                                                oob.is_evaluated, oob.entry_timestamp, oob.completed_on, oob.lstm_rf_model_prediction, oob.lstm_rf_model_prob, 
                                                oob.arima_ab_model_prediction, oob.arima_ab_model_prob, oob.approval_status, oob.approved_by, oob.approved_at, 
                                                oob.bucket_id, ism.fyers_symbol, "NSE" as exchange 
                                                from oms_order_bucket as oob
                                                inner join ind_stock_master as ism on oob.stock_id = ism.id
                                                where oob.country_id = {req_model.country_id} 
                                                and date(oob.entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and oob.order_status like '%pending%'
                                                and oob.time_frame like '{req_model.time_frame}'
                                                and oob.order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
                   
        elif (req_model.stock_tick == None or req_model.stock_tick == 'null' or req_model.stock_tick == '') and (req_model.page_no != None and req_model.page_no != 'null' and req_model.page_no != ''):
            offset = (req_model.page_no - 1) * req_model.limit
            if req_model.status == 'all':
                print('9')
                res = db.raw_query(text(f"""select oob.order_id, oob.trade_signal_id, oob.stock_tick, oob.stock_id, oob.country_id, 
                                                oob.time_frame, oob.order_type, oob.entry_price, oob.stoploss_price, oob.target_price, 
                                                oob.stock_quantity, oob.purchased_cmp_date, oob.purchased_on, oob.order_status, oob.is_trade_started, 
                                                oob.is_evaluated, oob.entry_timestamp, oob.completed_on, oob.lstm_rf_model_prediction, oob.lstm_rf_model_prob, 
                                                oob.arima_ab_model_prediction, oob.arima_ab_model_prob, oob.approval_status, oob.approved_by, oob.approved_at, 
                                                oob.bucket_id, ism.fyers_symbol, "NSE" as exchange 
                                                from oms_order_bucket as oob
                                                inner join ind_stock_master as ism on oob.stock_id = ism.id
                                                where oob.country_id = {req_model.country_id} 
                                                and date(oob.purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and oob.time_frame like '{req_model.time_frame}'
                                                and oob.order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
                
            elif req_model.status != 'pending' and req_model.status != 'progress':
                print('10')
                res = db.raw_query(text(f"""select oob.order_id, oob.trade_signal_id, oob.stock_tick, oob.stock_id, oob.country_id, 
                                                oob.time_frame, oob.order_type, oob.entry_price, oob.stoploss_price, oob.target_price, 
                                                oob.stock_quantity, oob.purchased_cmp_date, oob.purchased_on, oob.order_status, oob.is_trade_started, 
                                                oob.is_evaluated, oob.entry_timestamp, oob.completed_on, oob.lstm_rf_model_prediction, oob.lstm_rf_model_prob, 
                                                oob.arima_ab_model_prediction, oob.arima_ab_model_prob, oob.approval_status, oob.approved_by, oob.approved_at, 
                                                oob.bucket_id, ism.fyers_symbol, "NSE" as exchange 
                                                from oms_order_bucket as oob
                                                inner join ind_stock_master as ism on oob.stock_id = ism.id
                                                where oob.country_id = {req_model.country_id} 
                                                and date(oob.entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and oob.order_status like '%{req_model.status}%' 
                                                and oob.time_frame like '{req_model.time_frame}'
                                                and oob.order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
            elif req_model.status == 'pending':
                print('11')
                res = db.raw_query(text(f"""select oob.order_id, oob.trade_signal_id, oob.stock_tick, oob.stock_id, oob.country_id, 
                                                oob.time_frame, oob.order_type, oob.entry_price, oob.stoploss_price, oob.target_price, 
                                                oob.stock_quantity, oob.purchased_cmp_date, oob.purchased_on, oob.order_status, oob.is_trade_started, 
                                                oob.is_evaluated, oob.entry_timestamp, oob.completed_on, oob.lstm_rf_model_prediction, oob.lstm_rf_model_prob, 
                                                oob.arima_ab_model_prediction, oob.arima_ab_model_prob, oob.approval_status, oob.approved_by, oob.approved_at, 
                                                oob.bucket_id, ism.fyers_symbol, "NSE" as exchange 
                                                from oms_order_bucket as oob
                                                inner join ind_stock_master as ism on oob.stock_id = ism.id
                                                where oob.country_id = {req_model.country_id} 
                                                and date(oob.purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and oob.order_status is null 
                                                and oob.time_frame like '{req_model.time_frame}'
                                                and oob.order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
            elif req_model.status == 'progress':
                print('12')
                res = db.raw_query(text(f"""select oob.order_id, oob.trade_signal_id, oob.stock_tick, oob.stock_id, oob.country_id, 
                                                oob.time_frame, oob.order_type, oob.entry_price, oob.stoploss_price, oob.target_price, 
                                                oob.stock_quantity, oob.purchased_cmp_date, oob.purchased_on, oob.order_status, oob.is_trade_started, 
                                                oob.is_evaluated, oob.entry_timestamp, oob.completed_on, oob.lstm_rf_model_prediction, oob.lstm_rf_model_prob, 
                                                oob.arima_ab_model_prediction, oob.arima_ab_model_prob, oob.approval_status, oob.approved_by, oob.approved_at, 
                                                oob.bucket_id, ism.fyers_symbol, "NSE" as exchange 
                                                from oms_order_bucket as oob
                                                inner join ind_stock_master as ism on oob.stock_id = ism.id
                                                where oob.country_id = {req_model.country_id} 
                                                and date(oob.entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and oob.order_status like '%pending%'
                                                and oob.time_frame like '{req_model.time_frame}' 
                                                and oob.order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
        
        elif (req_model.stock_tick != None and req_model.stock_tick != 'null' and req_model.stock_tick != '') and (req_model.page_no == None or req_model.page_no == 'null' or req_model.page_no == ''):            
            if req_model.status == 'all':
                print('13')
                total_res = db.raw_query(text(f"""select oob.order_id, oob.trade_signal_id, oob.stock_tick, oob.stock_id, oob.country_id, 
                                                oob.time_frame, oob.order_type, oob.entry_price, oob.stoploss_price, oob.target_price, 
                                                oob.stock_quantity, oob.purchased_cmp_date, oob.purchased_on, oob.order_status, oob.is_trade_started, 
                                                oob.is_evaluated, oob.entry_timestamp, oob.completed_on, oob.lstm_rf_model_prediction, oob.lstm_rf_model_prob, 
                                                oob.arima_ab_model_prediction, oob.arima_ab_model_prob, oob.approval_status, oob.approved_by, oob.approved_at, 
                                                oob.bucket_id, ism.fyers_symbol, "NSE" as exchange 
                                                from oms_order_bucket as oob
                                                inner join ind_stock_master as ism on oob.stock_id = ism.id
                                                where oob.country_id = {req_model.country_id} 
                                                and date(oob.purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and oob.time_frame like '{req_model.time_frame}' 
                                                and oob.order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by};"""))
            elif req_model.status != 'pending' and req_model.status != 'progress':
                print('14')
                total_res = db.raw_query(text(f"""select oob.order_id, oob.trade_signal_id, oob.stock_tick, oob.stock_id, oob.country_id, 
                                                oob.time_frame, oob.order_type, oob.entry_price, oob.stoploss_price, oob.target_price, 
                                                oob.stock_quantity, oob.purchased_cmp_date, oob.purchased_on, oob.order_status, oob.is_trade_started, 
                                                oob.is_evaluated, oob.entry_timestamp, oob.completed_on, oob.lstm_rf_model_prediction, oob.lstm_rf_model_prob, 
                                                oob.arima_ab_model_prediction, oob.arima_ab_model_prob, oob.approval_status, oob.approved_by, oob.approved_at, 
                                                oob.bucket_id, ism.fyers_symbol, "NSE" as exchange 
                                                from oms_order_bucket as oob
                                                inner join ind_stock_master as ism on oob.stock_id = ism.id
                                                where oob.country_id = {req_model.country_id} 
                                                and date(oob.entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and oob.order_status like '%{req_model.status}%'
                                                and oob.time_frame like '{req_model.time_frame}' 
                                                and oob.order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by};"""))
            elif req_model.status == 'pending':
                print('15')
                total_res = db.raw_query(text(f"""select oob.order_id, oob.trade_signal_id, oob.stock_tick, oob.stock_id, oob.country_id, 
                                                oob.time_frame, oob.order_type, oob.entry_price, oob.stoploss_price, oob.target_price, 
                                                oob.stock_quantity, oob.purchased_cmp_date, oob.purchased_on, oob.order_status, oob.is_trade_started, 
                                                oob.is_evaluated, oob.entry_timestamp, oob.completed_on, oob.lstm_rf_model_prediction, oob.lstm_rf_model_prob, 
                                                oob.arima_ab_model_prediction, oob.arima_ab_model_prob, oob.approval_status, oob.approved_by, oob.approved_at, 
                                                oob.bucket_id, ism.fyers_symbol, "NSE" as exchange 
                                                from oms_order_bucket as oob
                                                inner join ind_stock_master as ism on oob.stock_id = ism.id
                                                where oob.country_id = {req_model.country_id} 
                                                and date(oob.purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and oob.order_status is null
                                                and oob.time_frame like '{req_model.time_frame}'
                                                and oob.order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by};"""))
            elif req_model.status == 'progress':
                print('16')
                total_res = db.raw_query(text(f"""select oob.order_id, oob.trade_signal_id, oob.stock_tick, oob.stock_id, oob.country_id, 
                                                oob.time_frame, oob.order_type, oob.entry_price, oob.stoploss_price, oob.target_price, 
                                                oob.stock_quantity, oob.purchased_cmp_date, oob.purchased_on, oob.order_status, oob.is_trade_started, 
                                                oob.is_evaluated, oob.entry_timestamp, oob.completed_on, oob.lstm_rf_model_prediction, oob.lstm_rf_model_prob, 
                                                oob.arima_ab_model_prediction, oob.arima_ab_model_prob, oob.approval_status, oob.approved_by, oob.approved_at, 
                                                oob.bucket_id, ism.fyers_symbol, "NSE" as exchange 
                                                from oms_order_bucket as oob
                                                inner join ind_stock_master as ism on oob.stock_id = ism.id
                                                where oob.country_id = {req_model.country_id} 
                                                and date(oob.entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and oob.order_status like '%pending%'
                                                and oob.time_frame like '{req_model.time_frame}'
                                                and oob.order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by};"""))
            found = False
            for i in range(len(total_res)):
                if total_res[i]['stock_tick'].lower().startswith(req_model.stock_tick.lower()):
                    found = i+1
                    break
            if not found:
                for i in range(len(total_res)):
                    if req_model.stock_tick.lower() in total_res[i]['stock_tick'].lower():
                        found = i+1
                        break
            #print("found:", found)
            if found:
                req_model.page_no = ((found - 1) // req_model.limit) + 1
                offset = (req_model.page_no - 1) * req_model.limit
                #print("offset:", offset)
                res = total_res[offset : offset + req_model.limit]
            else:
                pass
        
        elif (req_model.stock_tick != None and req_model.stock_tick != 'null' and req_model.stock_tick != '') and (req_model.page_no != None and req_model.page_no != 'null' and req_model.page_no != ''):
            offset = (req_model.page_no - 1) * req_model.limit
            if req_model.status == 'all':
                print('17')
                res = db.raw_query(text(f"""select oob.order_id, oob.trade_signal_id, oob.stock_tick, oob.stock_id, oob.country_id, 
                                                oob.time_frame, oob.order_type, oob.entry_price, oob.stoploss_price, oob.target_price, 
                                                oob.stock_quantity, oob.purchased_cmp_date, oob.purchased_on, oob.order_status, oob.is_trade_started, 
                                                oob.is_evaluated, oob.entry_timestamp, oob.completed_on, oob.lstm_rf_model_prediction, oob.lstm_rf_model_prob, 
                                                oob.arima_ab_model_prediction, oob.arima_ab_model_prob, oob.approval_status, oob.approved_by, oob.approved_at, 
                                                oob.bucket_id, ism.fyers_symbol, "NSE" as exchange 
                                                from oms_order_bucket as oob
                                                inner join ind_stock_master as ism on oob.stock_id = ism.id
                                                where oob.country_id = {req_model.country_id} 
                                                and date(oob.purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and oob.time_frame like '{req_model.time_frame}'
                                                and oob.order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
            elif req_model.status != 'pending' and req_model.status != 'progress':
                print('18')
                res = db.raw_query(text(f"""select oob.order_id, oob.trade_signal_id, oob.stock_tick, oob.stock_id, oob.country_id, 
                                                oob.time_frame, oob.order_type, oob.entry_price, oob.stoploss_price, oob.target_price, 
                                                oob.stock_quantity, oob.purchased_cmp_date, oob.purchased_on, oob.order_status, oob.is_trade_started, 
                                                oob.is_evaluated, oob.entry_timestamp, oob.completed_on, oob.lstm_rf_model_prediction, oob.lstm_rf_model_prob, 
                                                oob.arima_ab_model_prediction, oob.arima_ab_model_prob, oob.approval_status, oob.approved_by, oob.approved_at, 
                                                oob.bucket_id, ism.fyers_symbol, "NSE" as exchange 
                                                from oms_order_bucket as oob
                                                inner join ind_stock_master as ism on oob.stock_id = ism.id
                                                where oob.country_id = {req_model.country_id} 
                                                and date(oob.entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and oob.order_status like '%{req_model.status}%' 
                                                and oob.time_frame like '{req_model.time_frame}'
                                                and oob.order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
            elif req_model.status == 'pending':
                print('19')
                res = db.raw_query(text(f"""select oob.order_id, oob.trade_signal_id, oob.stock_tick, oob.stock_id, oob.country_id, 
                                                oob.time_frame, oob.order_type, oob.entry_price, oob.stoploss_price, oob.target_price, 
                                                oob.stock_quantity, oob.purchased_cmp_date, oob.purchased_on, oob.order_status, oob.is_trade_started, 
                                                oob.is_evaluated, oob.entry_timestamp, oob.completed_on, oob.lstm_rf_model_prediction, oob.lstm_rf_model_prob, 
                                                oob.arima_ab_model_prediction, oob.arima_ab_model_prob, oob.approval_status, oob.approved_by, oob.approved_at, 
                                                oob.bucket_id, ism.fyers_symbol, "NSE" as exchange 
                                                from oms_order_bucket as oob
                                                inner join ind_stock_master as ism on oob.stock_id = ism.id
                                                where oob.country_id = {req_model.country_id} 
                                                and date(oob.purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and oob.order_status is null 
                                                and oob.time_frame like '{req_model.time_frame}'
                                                and oob.order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
            elif req_model.status == 'progress':
                print('20')
                res = db.raw_query(text(f"""select oob.order_id, oob.trade_signal_id, oob.stock_tick, oob.stock_id, oob.country_id, 
                                                oob.time_frame, oob.order_type, oob.entry_price, oob.stoploss_price, oob.target_price, 
                                                oob.stock_quantity, oob.purchased_cmp_date, oob.purchased_on, oob.order_status, oob.is_trade_started, 
                                                oob.is_evaluated, oob.entry_timestamp, oob.completed_on, oob.lstm_rf_model_prediction, oob.lstm_rf_model_prob, 
                                                oob.arima_ab_model_prediction, oob.arima_ab_model_prob, oob.approval_status, oob.approved_by, oob.approved_at, 
                                                oob.bucket_id, ism.fyers_symbol, "NSE" as exchange 
                                                from oms_order_bucket as oob
                                                inner join ind_stock_master as ism on oob.stock_id = ism.id
                                                where oob.country_id = {req_model.country_id} 
                                                and date(oob.entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and oob.order_status like '%pending%' 
                                                and oob.time_frame like '{req_model.time_frame}'
                                                and oob.order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
        # if res != None:
        #     for order in res:
        #         try:                
        #             if order['order_status'] == None:
        #                 order['order_status'] = 'pending'
        #             else:
        #                 # order['order_status'] = clean_order_status(order['order_status'])
        #                 # print(order['order_status'], type(order['order_status']))
        #                 # order['order_status'] = ast.literal_eval(order['order_status'].replace("'", '"')) #.
        #                 order['order_status'] = safe_parse_order_status(order['order_status'])
        #                 # order['order_status'] = json.loads(order['order_status'])
        #                 status = order['order_status']
                    
        #                 if order['order_status']['status'] == 'success':                                            
        #                     order['order_status'] = 'success'
        #                     order['reason'] = status.get('reason', None)
        #                     order['profit_amount'] = status.get('profit_amount', None)
                        
        #                 elif order['order_status']['status'] == 'failed':                    
        #                     order['order_status'] = 'failed'
        #                     order['reason'] = status.get('reason', None)
        #                     order['loss_amount'] = status.get('loss_amount', None)
                        
        #                 elif order['order_status']['status'] == 'pending':                    
        #                     order['order_status'] = 'executed'
        #                     order['difference'] = status.get('current_difference', None)
        #                     order['percentage_change'] = status.get('current_pct_change', None)

        #                     # purchases_on = datetime.strptime(order['purchased_on'], "%Y-%m-%dT%H:%M:%S.%f")
        #                     # order['evaluated_order_status'] = evaluate_order_status(
        #                     #     stock_name = order['stock_tick'],
        #                     #     entry_price = order['entry_price'],
        #                     #     created_on = datetime.strftime(purchases_on, "%Y-%m-%dT%H:%M"),
        #                     #     stop_loss = order['stoploss_price'],
        #                     #     target_price = order['target_price'],
        #                     #     quantity = order['stock_quantity'],
        #                     #     order_type = order['order_type'].capitalize(),
        #                     #     time_frame = order['time_frame']
        #                     # )
        #                     # order['evaluated_order_status']['entry_hit'] = str(order['evaluated_order_status']['entry_hit'])
        #                     # print(order['evaluated_order_status'])

        #                     if req_model.country_id == 1:
        #                         stock_row = db.raw_query(text(f"""select * from ind_stock_master where id = '{order["stock_id"]}' and is_active = 1 and fyers_symbol is not null;"""))
        #                         if stock_row and len(stock_row) > 0:
        #                             latest_price = spf.fyers_data.get_latest_price(stock_row[0]['fyers_symbol'])
        #                             if latest_price:
        #                                 profit_loss_amount = (latest_price['ltp'] - order['entry_price']) * order['stock_quantity']
        #                                 if (profit_loss_amount >= 0) and (order['order_type'] in ('buy', 'BUY', 'Buy')):
        #                                     order['current_status'] = {
        #                                         "latest_price": latest_price['ltp'],
        #                                         "status": "profit",
        #                                         "amount": profit_loss_amount
        #                                     }
        #                                 elif (profit_loss_amount < 0) and (order['order_type'] in ('buy', 'BUY', 'Buy')):
        #                                     order['current_status'] = {
        #                                         "latest_price": latest_price['ltp'],
        #                                         "status": "loss",
        #                                         "amount": profit_loss_amount
        #                                     }
        #                                 elif (profit_loss_amount < 0) and (order['order_type'] in ('sell', 'SELL', 'Sell')):
        #                                     order['current_status'] = {
        #                                         "latest_price": latest_price['ltp'],
        #                                         "status": "profit",
        #                                         "amount": abs(profit_loss_amount)
        #                                     }
        #                                 elif (profit_loss_amount >= 0) and (order['order_type'] in ('sell', 'SELL', 'Sell')):
        #                                     order['current_status'] = {
        #                                         "latest_price": latest_price['ltp'],
        #                                         "status": "loss",
        #                                         "amount": -profit_loss_amount
        #                                     }

        #         except Exception as err:
        #             logger.exception(err)
        #             print(order['order_status'])
        #             order['order_status'] = None 
        
        apiresponce.setMsg("success")
        apiresponce.setResponse({"orders": res, "page_no": req_model.page_no})
    except Exception as ex:
        logger.exception(ex)
        logger.error(ex)
        apiresponce.setMsg("failed")
        apiresponce.setResponse(str(ex))
    return apiresponce


@router.get("/bucket/orders/{order_id}", tags=["bucket_orders"])
async def get_bucket_order_by_id(order_id: int, request: Request):
    apiresponce = APIResponse(request)
    try:
        session = db.get_session()
        res = session.query(OMSOrderBucket).filter(OMSOrderBucket.order_id == order_id).first()
        if res is None:
            apiresponce.setMsg("failed")
            apiresponce.setResponse("Order Not Found")
            return apiresponce
        apiresponce.setMsg("success")
        apiresponce.setResponse(res)
        return apiresponce
    except Exception as e:
        logger.exception("Failed to fetch bucket order by id")
        raise HTTPException(status_code=502, detail=f"Error fetching bucket order by id: {e}")
    finally:
        session.close()


@router.post("/create/bucket/orders", tags=["bucket_orders"])
async def create_bucket_order(body: BucketOrderBase, request: Request):
    apiresponce = APIResponse(request)
    try:
        session = db.get_session()
        if body.trade_id != None:
            bucket_order = session.query(OMSOrderBucket).filter(OMSOrderBucket.trade_signal_id == body.trade_id).all()
            if bucket_order and len(bucket_order) > 0:
                apiresponce.setMsg("failed")
                apiresponce.setResponse("Bucket order already exists for the given trade")
                session.close()
                return apiresponce
        order_id = None
        if body.trade_id != None:
            order = session.query(Order).filter(Order.trade_signal_id == body.trade_id).all()
            if order and len(order) > 0 and order[0].order_id != None:
                order_id = order[0].order_id
            else:
                order_id = body.order_id
        oms_order_bucket = OMSOrderBucket(
            order_id = order_id,
            trade_signal_id = body.trade_id,
            country_id = body.country_id,
            stock_tick = body.stock_tick,
            stock_id = body.stock_id,
            time_frame = body.time_frame,
            order_type = body.order_type,
            entry_price = body.entry_price,
            stoploss_price = body.stoploss_price,
            target_price = body.target_price,
            stock_quantity = body.stock_quantity,
            purchased_cmp_date = body.purchased_cmp_date,
            purchased_on = datetime.now(),
            order_status = "pending")
        session.add(oms_order_bucket)
        session.commit()
        apiresponce.setMsg("success")
        apiresponce.setResponse(oms_order_bucket)
    except Exception as e:
        logger.exception("Failed to create bucket order")
        raise HTTPException(status_code=502, detail=f"Error creating bucket order: {e}")
    finally:
        session.close()
    return apiresponce

@router.delete("/bucket/orders/{order_id}", tags=["bucket_orders"])
async def delete_bucket_order(order_id: int, request: Request):
    apiresponce = APIResponse(request)
    try:
        session = db.get_session()
        session.query(OMSOrderBucket).filter(OMSOrderBucket.order_id == order_id).delete()
        session.commit()
        apiresponce.setMsg("success")
        apiresponce.setResponse("Order deleted successfully")
        #return apiresponce
    except Exception as e:
        logger.exception("Failed to delete bucket order")
        raise HTTPException(status_code=502, detail=f"Error deleting bucket order: {e}")
    finally:
        session.close()
    return apiresponce
            

@router.post("/bucket/orders/update", tags=["bucket_orders"])
async def update_bucket_order(body: Update_BucketOrderBase, request: Request):
    apiresponce = APIResponse(request)
    try:
        session = db.get_session()
        oms_order_bucket = OMSOrderBucket(
                    order_id = body.order_id,
                    trade_id = body.trade_id,
                    stock_tick = body.stock_tick,
                    stock_id = body.stock_id,
                    time_frame = body.time_frame,
                    order_type = body.order_type,
                    entry_price = body.entry_price,
                    stop_loss = body.stoploss_price,
                    target_price = body.target_price,
                    stock_quantity = body.stock_quantity,
                    purchased_cmp_date = body.purchased_cmp_date)
        
        session.query(OMSOrderBucket).filter(OMSOrderBucket.bucket_id == body.bucket_id).update(oms_order_bucket)
        session.commit()
        apiresponce.setMsg("success")
        apiresponce.setResponse(oms_order_bucket)
    except Exception as ex:
        logger.exception("Failed to update bucket order")
        raise HTTPException(status_code=502, detail=f"Error updating bucket order: {ex}")
    finally:
        session.close()
    return apiresponce


@router.post("/approve/reject/bucket/orders", tags=["bucket_orders"])
async def approve_reject_bucket_orders(body: ApproveReject_BucketOrderBase, request: Request):
    apiresponce = APIResponse(request)
    try:
        session = db.get_session()
        row = session.query(OMSOrderBucket).filter(OMSOrderBucket.bucket_id == body.bucket_id).first()
        if row is None:
            raise HTTPException(status_code=404, detail="Bucket order not found !")
        print(body.status)
        if body.status == "approved":
            # PRODUCTION GUARD: Check GLOBAL_HALT before any new cash entry
            try:
                from scripts.futures_state_store import StateStore
                from scripts.futures_risk_engine import RiskEngine
                from shared.db.dbconn import DBConnection
                store = StateStore(DBConnection().engine)
                risk = RiskEngine(store)
                if risk.cash_entry_allowed() == 'GLOBAL_REALIZED_LOSS_CEILING':
                    raise HTTPException(status_code=403, detail="GLOBAL_HALT ACTIVE: New cash entries blocked")
            except Exception as e:
                if isinstance(e, HTTPException): raise e
                raise HTTPException(status_code=500, detail=f"Global Halt check failed: {e}")

            f_sym = session.query(Ind_StockMaster).filter(Ind_StockMaster.id == row.stock_id).first()
            symbol = f_sym.fyers_symbol
            entry_price = round(row.entry_price, 1)
            # stop_loss = round(row.stoploss_price, 1)
            # target_price = round(row.target_price, 1)
            quantity = row.stock_quantity
            order_type = row.order_type
            side = 1 if order_type == 'BUY' else -1
            
            # Normalize prices for GTT order
            prices = normalize_cash_oco_prices(
                entry=entry_price,
                target=row.target_price,
                stoploss=row.stoploss_price,
                order_type=order_type
            )

            if side == 1:
                leg1 = GttLeg(
                    price=prices["entry"],
                    triggerPrice=prices["entry"],
                    qty=quantity
                )
                order_info = GttOrderInfo(
                    leg1=leg1
                )
                fyers = get_fyers()
                single_gtt_order_request = SingleGttOrderRequest(
                    side=side,
                    symbol=symbol,
                    productType="CNC",
                    orderInfo=order_info,
                    orderTag=str(row.bucket_id)
                )
                # response = fyers._client.place_gtt_order(data=single_gtt_order_request.model_dump())
                response = fyers.place_cash_gtt(single_gtt_order_request.model_dump())
                print(response)
                if response and response.get("s") == "ok":
                    row.gtt_id = response.get("id")
                    row.id_fyers = response.get("id_fyers")
                    row.approval_status = body.status
                    row.approved_by = body.approved_by
                    row.approved_at = datetime.now()
                    session.add(row)
                    session.commit()
                    print("GTB ID updated successfully")
                    apiresponce.setMsg("success")
                    apiresponce.setResponse("Bucket order updated successfully")
                else:
                    print("Failed to update GTB ID")
                    apiresponce.setMsg("error")
                    apiresponce.setResponse("Failed to update GTB ID")
            else:
                ####### To do for side = -1 (SELL)
                print("To do for side = -1 (SELL)")

        elif body.status == "rejected":
            fyers = get_fyers()

            gtt_order_resp = fyers._client.gtt_orderbook()
            gtt_orderbook = gtt_order_resp.get("orderBook", [])

            matching_order = next((order for order in gtt_orderbook if str(order.get("ordertag")).replace("1:GTT", "") == str(body.bucket_id)), None)
            if matching_order:
                print("Matching order found")
                # Delete the matching order
                delete_response = fyers._client.cancel_gtt_order(data={
                    "id": matching_order.get("id")
                })
                print("Delete response:", delete_response)
                row.approval_status = body.status
                row.approved_by = body.approved_by
                row.approved_at = datetime.now()
                session.add(row)
                session.commit()
                apiresponce.setMsg("success")
                apiresponce.setResponse("Bucket order and pending fyers GTT order rejected successfully")
            else:
                print("No matching order found")
                apiresponce.setMsg("failed")
                apiresponce.setResponse("Unable to find pending GTT order for this bucket")
            
            # print("Bucket order rejected successfully")
            
        #return apiresponce
        #return apiresponce
    except HTTPException:
        raise

    except Exception as ex:
        logger.exception("Failed to update bucket order")
        raise HTTPException(
            status_code=502,
            detail=f"Error updating bucket order: {ex}"
        )
    finally:
        session.close()
    return apiresponce


def reject_bucket_order(bucket_id):
    try:
        fyers = get_fyers()
        
        gtt_order_resp = fyers._client.gtt_orderbook()
        gtt_orderbook = gtt_order_resp.get("orderBook", [])

        matching_order = next((order for order in gtt_orderbook if str(order.get("ordertag")).replace("1:GTT", "") == str(bucket_id)), None)
        if matching_order:
            print("Matching order found")
            # Delete the matching order
            delete_response = fyers._client.cancel_gtt_order(data={
                "id": matching_order.get("id")
            })
            logger.info(f"Delete response: {delete_response}")
            if delete_response and delete_response.get("s") == "ok":
                logger.info(f"GTT order deleted successfully for bucket_id: {bucket_id}")
                return True
            else:
                logger.error(f"Failed to delete GTT order for bucket_id: {bucket_id}")
                return False
    except Exception as ex:
        logger.error(f"Failed to reject bucket order bucket_id: {bucket_id}")
        logger.exception(ex)
        return False
        
    