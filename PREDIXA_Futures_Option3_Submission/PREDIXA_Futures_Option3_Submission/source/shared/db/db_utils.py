from urllib3 import response
from shared.db.dbconn import DBConnection
from shared.db.db_model import Ind_StockMaster, Us_StockMaster, Order, CountryMaster, TradeSignal, Auto_Order, UserAlert, ExchangeMaster, CommoditiesMaster, FuturesMaster
from shared.db.db_model import Commodity_Order, Future_Order, Future_Auto_Order, OMSOrderBucket
from shared.db.req_model import BucketOrderBase
from shared.db.fyers_req_model import get_fyers, GttLeg, GttOrderInfo, SingleGttOrderRequest
from datetime import datetime, timedelta
from sqlalchemy import func, cast, DateTime
from shared.utils.logger import logger
import math
from apps.pipelines.market_conditions import check_upper_lower_circuit_range
from sqlalchemy import false
from typing import Dict, Any, Optional
from data_fetchers.fyers.fyers_utils import normalize_cash_oco_prices
import logging

logger = logging.getLogger(__name__)

dbc = DBConnection()

def insert_order_alerts(buy_dict, time_fr, stock_name, ord_type, dt):
    # if not(is_similar_order_exists_auto_order(dt, stock_name, time_fr, buy_dict['entry_price'], buy_dict['stop_loss'], buy_dict['target_price'])):
        # dbc = DBConnection()
    db_order = Order(
            stock_tick=stock_name,
            order_type=ord_type,
            time_frame=time_fr,
            entry_price=buy_dict['entry_price'],
            stoploss_price=buy_dict['stop_loss'],
            target_price=buy_dict['target_price'],
            stock_quantity=100,
            purchased_cmp_date=dt.strftime("%Y-%m-%dT%H:%M"),
            purchased_on=datetime.now())
    session = dbc.get_session()
    session.add(db_order)
    session.commit()
    session.refresh(db_order)
    print("order pushed successfully")
    session.close()
    # dbc.close_engine()
    print("Order Create Successfully...........................", stock_name)

def insert_all_alerts(buy_dict, ord_type, time_fr, stock_name, dt, country, country_id, prediction, probability, trade_id):
    dt = datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%f") if isinstance(dt, str) else dt
    # dbc = DBConnection()
    session = dbc.get_session()
    if country.lower() == "india":
        stk = session.query(Ind_StockMaster).filter(Ind_StockMaster.stock_tick == stock_name, Ind_StockMaster.is_active==1).all()
        stock_id = stk[0].id
    elif country.lower() == "us":
        stk = session.query(Us_StockMaster).filter(Us_StockMaster.stock_tick == stock_name, Us_StockMaster.is_active==1).all()
        stock_id = stk[0].id
    else:
        stock_id = None
    if stk and len(stk) == 0:
        stock_id = None

    if not(is_similar_order_exists_orm("order", stock_name, time_fr, buy_dict['entry_price'], buy_dict['stop_loss'], buy_dict['target_price'])):
        existing_order = session.query(Order).filter(Order.trade_signal_id == trade_id).all()
        if existing_order and len(existing_order) > 0:
            print("Order already exists")
            return {"order_id": existing_order[0].order_id}
        db_order = Order(
                stock_tick=stock_name,
                stock_id=stock_id,
                country_id=country_id,
                time_frame=time_fr,
                order_type=ord_type,
                entry_price=buy_dict['entry_price'],
                stoploss_price=buy_dict['stop_loss'],
                target_price=buy_dict['target_price'],
                stock_quantity=100,
                purchased_cmp_date=dt.strftime("%Y-%m-%dT%H:%M"),
                purchased_on=datetime.now(),
                arima_ab_model_prediction = prediction,
                arima_ab_model_prob = probability,
                trade_signal_id = trade_id)

        session.add(db_order)
        session.commit()
        session.refresh(db_order)
        session.close()
        # dbc.close_engine()
        return {"order_id": db_order.order_id}
    else:
        print("Order already exists")



def push_trades_to_order(time_fr: int, country: str, avl_trades: list):
    # dbc = DBConnection()
    session = dbc.get_session()
    try:
        cntry = session.query(CountryMaster).filter(
            func.lower(CountryMaster.country_name) == country.lower(),
            CountryMaster.status == 1
        ).all()
    finally:
        session.close()
        # dbc.close_engine()

    country_id = cntry[0].country_id if cntry else None

    for item in avl_trades:
        trade_type = item.get("TRADE_TYPE")
        # status = check_upper_lower_circuit_range(stock_name = item["STOCK_NAME"], 
        #                                          trade_data = item.get(trade_type))
        # if status != None and status == True:
        stock_name = item.get("STOCK_NAME")
        prediction = item.get("PREDICTION")
        probability = item.get("PROBABILITY")
        trade_data = item.get(trade_type)
        trade_id = item.get("trade_id")
        created_at = item.get("created_at", datetime.now())
        if not trade_data:
            logger.warning(f"[push_trades_to_order] Missing trade data for {stock_name} ({trade_type})")
            continue

        try:
            insert_all_alerts(
                trade_data,
                trade_type,
                time_fr,
                stock_name,
                created_at,
                country,
                country_id,
                prediction,
                probability,
                trade_id
            )
            logger.info(f"[push_trades_to_order] Pushed {trade_type} for {stock_name}")
        except Exception as ex:
            logger.error(f"[push_trades_to_order] Failed to push {trade_type} for {stock_name}: {ex}")
        # elif status != None and status == False:
        #     print(f"\nBeyond upper/lower circuit : {item}")
        # else:
        #     print(f"\nMarket depth not found : {item}")
        



def insert_trade_signals(trade_result: dict, country_id, exchange_id, time_frame):
    try:
        if time_frame == 1:
            status = True
        else:
            status = check_upper_lower_circuit_range(stock_name = trade_result["STOCK_NAME"], 
                                            trade_data = {
                                                            "entry_price": trade_result[trade_result["TRADE_TYPE"]]["entry_price"],
                                                            "target_price": trade_result[trade_result["TRADE_TYPE"]]["target_price"],
                                                            "stop_loss": trade_result[trade_result["TRADE_TYPE"]]["stop_loss"]
                                                        })
        st_sym, exchange_name = get_stock_symbol(trade_result["STOCK_NAME"], exchange_id)
        if (status != None and status == True) or exchange_name != 'NSE':    
            # dbc = DBConnection()
            session = dbc.get_session()
            #st_sym, exchange_name = get_stock_symbol(trade_result["STOCK_NAME"], exchange_id)
            trade_type = trade_result["TRADE_TYPE"]
            stock_name = trade_result["STOCK_NAME"]
            price_cmp = round(float(trade_result["PRICE_CMP"]), 2)
            exp_date = trade_result.get("EXP_NUM", "")
            created_cutoff = datetime.now() - timedelta(days=2)
            rrr = trade_result.get(f"{trade_type}_RRR", None)
            if rrr in [None, float("inf"), float("-inf")] or math.isinf(rrr) or math.isnan(rrr):
                rrr = None
            rrr = trade_result.get(f"{trade_type}_RRR", None)

            # existing = session.query(TradeSignal).filter(
            #     TradeSignal.stock_name == stock_name,
            #     TradeSignal.time_fr == time_frame,
            #     TradeSignal.trade_type == trade_type,
            #     TradeSignal.exp_date == exp_date,
            #     TradeSignal.price_cmp.between(price_cmp - 0.05, price_cmp + 0.05),
            #     TradeSignal.created_at >= created_cutoff
            # ).first()
            existing = find_similar_trade(
            stock_name=stock_name,
            time_frame=time_frame,
            trade_type=trade_type,
            exp_date=exp_date,  
            created_cutoff=created_cutoff,
            new_trade_data={
                "entry_price": trade_result[trade_result["TRADE_TYPE"]]["entry_price"],
                "target_price": trade_result[trade_result["TRADE_TYPE"]]["target_price"],
                "stop_loss": trade_result[trade_result["TRADE_TYPE"]]["stop_loss"]
                }
            )



            if existing:
                print(f"⚠️ Skipped duplicate trade for {stock_name} ({trade_type}) at price ~{price_cmp}")
                session.close()
                return False, None


            if 'EXP_NUM' in trade_result.keys():
                new_trade = TradeSignal(
                    exchange_id=exchange_id,
                    stock_name=stock_name,
                    time_fr=time_frame,
                    country_id=country_id,
                    price_cmp=trade_result["PRICE_CMP"],
                    trade_type=trade_result["TRADE_TYPE"],
                    rrr=trade_result[f"{trade_result['TRADE_TYPE']}_RRR"],
                    timestamps=trade_result[f"{trade_result['TRADE_TYPE']}_TIMESTAMPS"],
                    trade_data=trade_result[trade_result["TRADE_TYPE"]],
                    prediction=trade_result["PREDICTION"],
                    probability=trade_result["PROBABILITY"],
                    created_at=datetime.now(),
                    is_active=True,
                    is_send=0,
                    exp_date=trade_result['EXP_NUM']
                )
            else:
                new_trade = TradeSignal(
                    exchange_id=exchange_id,
                    stock_name=stock_name,
                    time_fr=time_frame,
                    country_id=country_id,
                    price_cmp=trade_result["PRICE_CMP"],
                    trade_type=trade_result["TRADE_TYPE"],
                    rrr=trade_result[f"{trade_result['TRADE_TYPE']}_RRR"],
                    timestamps=trade_result[f"{trade_result['TRADE_TYPE']}_TIMESTAMPS"],
                    trade_data=trade_result[trade_result["TRADE_TYPE"]],
                    prediction=trade_result["PREDICTION"],
                    probability=trade_result["PROBABILITY"],
                    created_at=datetime.now(),
                    is_active=True,
                    is_send=0
                )
            session.add(new_trade)
            session.commit()
            session.refresh(new_trade)
            session.close()
            # dbc.close_engine()
            print(f"✅ Trade inserted: {stock_name} [{trade_type}] @ {price_cmp}")
            return True, new_trade.id
        elif status != None and status == False:
            print(f"\nBeyond upper/lower circuit : {trade_result}")
        else:
            print(f"\nMarket depth not found : {trade_result}")
        return False, None
    except Exception as e:
        logger.exception(e)
        logger.error(f"Error inserting trade_signal {trade_result}: {e}")
        return False, None


def insert_alerts(buy_dict, time_fr, stock_name, ord_type, dt):
    # if not(is_similar_order_exists_auto_order(dt, stock_name, time_fr, buy_dict['entry_price'], buy_dict['stop_loss'], buy_dict['target_price'])):
        # dbc = DBConnection()
    db_order = Auto_Order(
            stock_tick=stock_name,
            order_type=ord_type,
            time_frame=time_fr,
            entry_price=buy_dict['entry_price'],
            stoploss_price=buy_dict['stop_loss'],
            target_price=buy_dict['target_price'],
            stock_quantity=100,
            purchased_cmp_date=dt.strftime("%Y-%m-%dT%H:%M"),
            purchased_on=datetime.now())
    session = dbc.get_session()
    session.add(db_order)
    session.commit()
    session.refresh(db_order)
    print("order pushed successfully")
    session.close()
    # dbc.close_engine()
    print("Order Create Successfully...........................", stock_name)

def future_insert_alerts(buy_dict, time_fr, stock_name, ord_type, dt, exp_num):
    # if not(is_similar_order_exists_auto_order(dt, stock_name, time_fr, buy_dict['entry_price'], buy_dict['stop_loss'], buy_dict['target_price'])):
        # dbc = DBConnection()
    db_order = Future_Auto_Order(
            stock_tick=stock_name,
            order_type=ord_type,
            time_frame=time_fr,
            entry_price=buy_dict['entry_price'],
            stoploss_price=buy_dict['stop_loss'],
            target_price=buy_dict['target_price'],
            stock_quantity=100,
            purchased_cmp_date=dt.strftime("%Y-%m-%dT%H:%M"),
            purchased_on=datetime.now(),
            expiry_date = exp_num)
    session = dbc.get_session()
    session.add(db_order)
    session.commit()
    session.refresh(db_order)
    print("order pushed successfully")
    session.close()
    # dbc.close_engine()
    print("Order Create Successfully...........................", stock_name)


def check_and_insert_automated_alert(trade_result: dict, exchange_id, c_id, timeframe):
    # status = check_upper_lower_circuit_range(stock_name = trade_result["STOCK_NAME"], 
    #                                 trade_data = {
    #                                                 "entry_price": trade_result[trade_result["TRADE_TYPE"]]["entry_price"],
    #                                                 "target_price": trade_result[trade_result["TRADE_TYPE"]]["target_price"],
    #                                                 "stop_loss": trade_result[trade_result["TRADE_TYPE"]]["stop_loss"]
    #                                              })
    # if status != None and status == True:
    # dbc = DBConnection()
    t_data = trade_result[trade_result["TRADE_TYPE"]]
    stock_tick_name = trade_result["STOCK_NAME"]
    st_sym, exchange_name = get_stock_symbol(stock_tick_name, exchange_id)
    #  {"stop_loss": 2190.7081213553292, "entry_price": 2251.2836989746093, "target_price": 2554.280901539082}
    if "EXP_NUM" in trade_result.keys():
        alert_ord = UserAlert(
                                user_id=1,
                                country_id=c_id,
                                stock_symbol=st_sym,
                                exchange=exchange_name,
                                alert_type=trade_result["TRADE_TYPE"],
                                trigger_price=t_data['entry_price'],
                                price_range_min=t_data['entry_price'] - t_data['entry_price'] * 0.005,
                                price_range_max=t_data['entry_price'] + t_data['entry_price'] * 0.005,
                                threshold=0.5,  # percent
                                is_active=True,
                                recurrent=False,
                                created_at=datetime.now(),
                                cooldown_minutes=2,
                                timeframe=str(timeframe),
                                note='Entry Near Setup',
                                expiry_date=trade_result["EXP_NUM"],
                                trade_signal_id=trade_result["trade_id"]
                            )
    else:
        alert_ord = UserAlert(
                                user_id=1,
                                country_id=c_id,
                                stock_symbol=st_sym,
                                exchange=exchange_name,
                                alert_type=trade_result["TRADE_TYPE"],
                                trigger_price=t_data['entry_price'],
                                price_range_min=t_data['entry_price'] - t_data['entry_price'] * 0.005,
                                price_range_max=t_data['entry_price'] + t_data['entry_price'] * 0.005,
                                threshold=0.5,  # percent
                                is_active=True,
                                recurrent=False,
                                created_at=datetime.now(),
                                cooldown_minutes=2,
                                timeframe=str(timeframe),
                                note='Entry Near Setup',
                                trade_signal_id=trade_result["trade_id"]
                            )
    session = dbc.get_session()
    session.add(alert_ord)
    session.commit()
    session.refresh(alert_ord)
    session.close()
    # dbc.close_engine()
    # elif status != None and status == False:
    #     print(f"\nBeyond upper/lower circuit : {trade_result}")
    # else:
    #     print(f"\nMarket depth not found : {trade_result}")


def get_stock_symbol(stock_name: str, exchange_id: int):
    # dbc = DBConnection()
    db = dbc.get_session()
    exchange = db.query(ExchangeMaster).filter(
        ExchangeMaster.exchange_id == exchange_id,
        ExchangeMaster.is_active == True
    ).first()
    
    exchange_name = exchange.exchange_name
    stock = None
    if exchange_name == 'NSE':  # NSE
        stock = db.query(Ind_StockMaster).filter(Ind_StockMaster.stock_tick == stock_name).first()
        return stock.kite_symbol, exchange_name
    elif exchange_name == 'MCX':  # MCX
        stock = db.query(CommoditiesMaster).filter(CommoditiesMaster.symbol == stock_name).first()
        return stock.symbol, exchange_name
    elif exchange_name == 'NSEFO':  # NSEFO
        stock = db.query(FuturesMaster).filter(FuturesMaster.symbol == stock_name).first()
        return stock.symbol, exchange_name
    else:
        return False, False
    




def insert_future_and_commodity_order(stock_id, c_id, trade_dict, time_fr, stock_name, dt, ord_type, exp_dt, prediction, probability, trade_id, segment):
    dt = datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S.%f") if isinstance(dt, str) else dt
    # dbc = DBConnection()
    formatted_date = datetime.strptime(exp_dt, "%d%m%Y").strftime("%d-%m-%Y")
    session = dbc.get_session()

    session = dbc.get_session()
    if segment == "commodity":
        stk = session.query(CommoditiesMaster).filter(CommoditiesMaster.symbol == stock_name, CommoditiesMaster.is_active==1).all()
        stock_id = stk[0].id    
        if stk and len(stk) == 0:
            stock_id = None
    
    if not(is_similar_order_exists_orm(segment, stock_name, time_fr, trade_dict['entry_price'], trade_dict['stop_loss'], trade_dict['target_price'])):
        if segment == 'commodity':
            existing_order = session.query(Commodity_Order).filter(Commodity_Order.trade_signal_id == trade_id).all()
            if existing_order and len(existing_order) > 0:
                print("Order already exists")
                return {"order_id": existing_order[0].order_id}
            db_order = Commodity_Order(
                stock_tick=stock_name,
                stock_id=stock_id,
                country_id=c_id,
                time_frame=time_fr,
                order_type=ord_type,
                entry_price=trade_dict['entry_price'],
                stoploss_price=trade_dict['stop_loss'],
                target_price=trade_dict['target_price'],
                stock_quantity=100,
                purchased_cmp_date=dt.strftime("%Y-%m-%dT%H:%M"),
                purchased_on=datetime.now(),
                arima_ab_model_prediction = prediction,
                arima_ab_model_prob = probability,
                expiry_date=formatted_date,
                trade_signal_id = trade_id)
        else:
            existing_order = session.query(Future_Order).filter(Future_Order.trade_signal_id == trade_id).all()
            if existing_order and len(existing_order) > 0:
                print("Order already exists")
                return {"order_id": existing_order[0].order_id}
            db_order = Future_Order(
                stock_tick=stock_name,
                stock_id=stock_id,
                country_id=c_id,
                time_frame=time_fr,
                order_type=ord_type,
                entry_price=trade_dict['entry_price'],
                stoploss_price=trade_dict['stop_loss'],
                target_price=trade_dict['target_price'],
                stock_quantity=100,
                purchased_cmp_date=dt.strftime("%Y-%m-%dT%H:%M"),
                purchased_on=datetime.now(),
                arima_ab_model_prediction = prediction,
                arima_ab_model_prob = probability,
                expiry_date=formatted_date,
                trade_signal_id = trade_id)
        session.add(db_order)
        session.commit()
        session.refresh(db_order)
        session.close()
        # dbc.close_engine()
        print("Order Inserted Successfully............................", stock_name)
    else:
        print("Similar order exists")




def push_future_commodity_order(time_fr: int, country: str, avl_trades: list, segment):
    # dbc = DBConnection()
    session = dbc.get_session()
    try:
        cntry = session.query(CountryMaster).filter(
            func.lower(CountryMaster.country_name) == country.lower(),
            CountryMaster.status == 1
        ).all()
    finally:
        session.close()
        # dbc.close_engine()

    country_id = cntry[0].country_id if cntry else None

    for item in avl_trades:
        trade_type = item.get("TRADE_TYPE")
        stock_name = item.get("STOCK_NAME")
        prediction = item.get("PREDICTION")
        probability = item.get("PROBABILITY")
        exp_date = item.get("EXP_NUM")
        trade_data = item.get(trade_type)
        trade_id = item.get("trade_id")
        created_at = item.get("created_at", datetime.now())
        if not trade_data:
            logger.warning(f"[push_future_commodity_order] Missing trade data for {stock_name} ({trade_type})")
            continue

        try:
            insert_future_and_commodity_order(
                1,
                country_id,
                trade_data,
                time_fr,
                stock_name,
                created_at,
                trade_type,
                exp_date,
                prediction,
                probability,
                trade_id,
                segment=segment
            )
            logger.info(f"[push_future_commodity_order] Pushed {trade_type} for {stock_name}")
        except Exception as ex:
            logger.error(f"[push_future_commodity_order] Failed to push {trade_type} for {stock_name}: {ex}")


def is_similar_order_exists_orm( table_name: str,
                                 stock_tick: str,
                                 time_frame: int,
                                 entry_price: float,
                                 stoploss_price: float,
                                 target_price: float,
                                 entry_threshold: float = 0.5,
                                 stoploss_threshold: float = 0.5,
                                 target_threshold: float = 0.5) -> bool:
    try:
    
        # Compute dynamic ranges
        entry_min = entry_price * (1 - entry_threshold / 100)
        entry_max = entry_price * (1 + entry_threshold / 100)
        stop_min = stoploss_price * (1 - stoploss_threshold / 100)
        stop_max = stoploss_price * (1 + stoploss_threshold / 100)
        target_min = target_price * (1 - target_threshold / 100)
        target_max = target_price * (1 + target_threshold / 100)
        if time_frame == 1:
            time_min = datetime.now() - timedelta(days = 7)
        elif time_frame in (2, 3, 25, 6):
            time_min = datetime.now() - timedelta(days = 2)
        elif time_frame == 5:
            time_min = datetime.now() - timedelta(days = 3)
        
        db = dbc.get_session()
        if table_name == "order":
            result = db.query(Order).filter(
                Order.stock_tick == stock_tick,
                Order.time_frame == time_frame,
                Order.entry_price.between(entry_min, entry_max),
                Order.stoploss_price.between(stop_min, stop_max),
                Order.target_price.between(target_min, target_max),
                Order.purchased_on >= time_min
                # Order.is_evaluated == false(),
                # Order.is_trade_started == false()
            ).scalar()
        
        elif table_name == "futures":
            result = db.query(Future_Order).filter(
                Future_Order.stock_tick == stock_tick,
                Future_Order.time_frame == time_frame,
                Future_Order.entry_price.between(entry_min, entry_max),
                Future_Order.stoploss_price.between(stop_min, stop_max),
                Future_Order.target_price.between(target_min, target_max),
                Future_Order.purchased_on >= time_min
                # Future_Order.is_evaluated == false(),
                # Future_Order.is_trade_started == false()
            ).scalar()

        elif table_name == "commodity":
            result = db.query(Commodity_Order).filter(
                Commodity_Order.stock_tick == stock_tick,
                Commodity_Order.time_frame == time_frame,
                Commodity_Order.entry_price.between(entry_min, entry_max),
                Commodity_Order.stoploss_price.between(stop_min, stop_max),
                Commodity_Order.target_price.between(target_min, target_max),
                Commodity_Order.purchased_on >= time_min
                # Commodity_Order.is_evaluated == false(),
                # Commodity_Order.is_trade_started == false()
            ).scalar()

        elif table_name == "oms_order_bucket":
                    result = db.query(OMSOrderBucket).filter(
                        OMSOrderBucket.stock_tick == stock_tick,
                        OMSOrderBucket.time_frame == time_frame,
                        OMSOrderBucket.entry_price.between(entry_min, entry_max),
                        OMSOrderBucket.stoploss_price.between(stop_min, stop_max),
                        OMSOrderBucket.target_price.between(target_min, target_max),
                        OMSOrderBucket.purchased_on >= time_min
                        # OMSOrderBucket.is_trade_started == false()
                    ).scalar()
        
        #result = db.query(ord.exists()).scalar()
        db.close()
        # dbc.close_engine()
        return result
    except:
        return None


def is_similar_order_exists_auto_order(
                                 timestamp: datetime,
                                 stock_tick: str,
                                 time_frame: int,
                                 entry_price: float,
                                 stoploss_price: float,
                                 target_price: float,
                                 entry_threshold: float = 0.5,
                                 stoploss_threshold: float = 0.5,
                                 target_threshold: float = 0.5) -> bool:
    # Compute dynamic ranges
    entry_min = entry_price * (1 - entry_threshold / 100)
    entry_max = entry_price * (1 + entry_threshold / 100)
    stop_min = stoploss_price * (1 - stoploss_threshold / 100)
    stop_max = stoploss_price * (1 + stoploss_threshold / 100)
    target_min = target_price * (1 - target_threshold / 100)
    target_max = target_price * (1 + target_threshold / 100)
    # dbc = DBConnection()
    # if time_frame == 2:
        # time_threshold_day = 3  # +/- 1 hour
    # elif time_frame == 1:
        # time_threshold_day = 14  # +/- 1 day
    # else:
        # time_threshold_day = 1  # default fallback

    # time_min = timestamp - timedelta(days=time_threshold_day)
    # time_max = timestamp + timedelta(days=time_threshold_day)

    db = dbc.get_session()
    query = db.query(Auto_Order).filter(
        Auto_Order.stock_tick == stock_tick,
        Auto_Order.time_frame == time_frame,
        Auto_Order.entry_price.between(entry_min, entry_max),
        Auto_Order.stoploss_price.between(stop_min, stop_max),
        Auto_Order.target_price.between(target_min, target_max)
        # cast(Auto_Order.purchased_cmp_date, DateTime).between(time_min, time_max)
    )
    
    result = db.query(query.exists()).scalar()
    db.close()
    # dbc.close_engine()
    return result


def find_similar_trade(stock_name, time_frame, trade_type, exp_date, created_cutoff, new_trade_data):

    if time_frame == 1:
        created_cutoff = datetime.now() - timedelta(days = 7)
    elif time_frame in (2, 3, 25, 6):
        created_cutoff = datetime.now() - timedelta(days = 2)
    elif time_frame == 5:
        created_cutoff = datetime.now() - timedelta(days = 3)

    db = dbc.get_session()
    candidates = db.query(TradeSignal).filter(
        TradeSignal.stock_name == stock_name,
        TradeSignal.time_fr == time_frame,
        TradeSignal.trade_type == trade_type,
        TradeSignal.exp_date == exp_date,
        TradeSignal.created_at >= created_cutoff
    ).all()
    db.close()
    for signal in candidates:
        existing_data = signal.trade_data
        if existing_data and is_similar_trade(new_trade_data, existing_data):
            return signal  # Found similar signal

    return None

def is_similar_trade(new_trade_data, existing_trade_data, epsilon=1):
    try:
        # return (
        #     abs(new_trade_data["entry_price"] - existing_trade_data["entry_price"]) <= epsilon and
        #     abs(new_trade_data["target_price"] - existing_trade_data["target_price"]) <= epsilon and
        #     abs(new_trade_data["stop_loss"] - existing_trade_data["stop_loss"]) <= epsilon
        # )
        return (
            (
                abs(new_trade_data["entry_price"] - existing_trade_data["entry_price"]) <= epsilon and
                abs(new_trade_data["target_price"] - existing_trade_data["target_price"]) <= epsilon
            ) or 
            (
                abs(new_trade_data["target_price"] - existing_trade_data["target_price"]) <= epsilon and
                abs(new_trade_data["stop_loss"] - existing_trade_data["stop_loss"]) <= epsilon
            ) or 
            (
                abs(new_trade_data["entry_price"] - existing_trade_data["entry_price"]) <= epsilon and
                abs(new_trade_data["stop_loss"] - existing_trade_data["stop_loss"]) <= epsilon
            )
        )
    except Exception as ex:
        print("Error in similarity check:", ex)
        return False
    

def insert_order_and_oms_bucket(
    trade_data: Dict[str, Any],
    order_type: str,
    time_fr: int,
    stock_name: str,
    created_at: Any,
    country: str,
    country_id: int,
    prediction: Optional[str],
    probability: Optional[float],
    trade_signal_id: int,
    stock_quantity: int = 1,
) -> Dict[str, Any]:
    """
    Insert a trade into both:

        1. orders
        2. oms_order_bucket

    Both records are inserted in one transaction.

    Behavior:
    - If the Order already exists and its OMS bucket also exists, return both IDs.
    - If the Order exists but the OMS bucket is missing, create only the bucket.
    - If neither exists, create Order first, then OMS bucket.
    - If either insert fails, roll back the complete transaction.

    Expected trade_data:
        {
            "entry_price": ...,
            "stop_loss": ...,
            "target_price": ...
        }
    """

    session = dbc.get_session()

    try:
        order_type = str(order_type or "").strip().upper()
        country = str(country or "").strip()

        if order_type not in {"BUY", "SELL"}:
            raise ValueError(
                f"Invalid order type: {order_type!r}"
            )

        if not trade_signal_id:
            raise ValueError(
                "trade_signal_id is required"
            )

        required_fields = (
            "entry_price",
            "stop_loss",
            "target_price",
        )

        missing_fields = [
            field
            for field in required_fields
            if trade_data.get(field) is None
        ]

        if missing_fields:
            raise ValueError(
                "Missing trade data fields: "
                + ", ".join(missing_fields)
            )

        if isinstance(created_at, str):
            try:
                created_at = datetime.strptime(
                    created_at,
                    "%Y-%m-%dT%H:%M:%S.%f",
                )
            except ValueError:
                created_at = datetime.fromisoformat(
                    created_at
                )

        if created_at is None:
            created_at = datetime.now()

        entry_price = float(
            trade_data["entry_price"]
        )
        stoploss_price = float(
            trade_data["stop_loss"]
        )
        target_price = float(
            trade_data["target_price"]
        )

        # ---------------------------------------------------------
        # Resolve stock ID
        # ---------------------------------------------------------
        stock_id = None

        if country.lower() == "india":
            stock = (
                session.query(Ind_StockMaster)
                .filter(
                    Ind_StockMaster.stock_tick == stock_name,
                    Ind_StockMaster.is_active == 1,
                )
                .first()
            )

            if stock:
                stock_id = stock.id

        elif country.lower() == "us":
            stock = (
                session.query(Us_StockMaster)
                .filter(
                    Us_StockMaster.stock_tick == stock_name,
                    Us_StockMaster.is_active == 1,
                )
                .first()
            )

            if stock:
                stock_id = stock.id

        if stock_id is None:
            raise ValueError(
                f"Active stock not found: "
                f"country={country}, stock={stock_name}"
            )

        # ---------------------------------------------------------
        # Check whether Order already exists
        # ---------------------------------------------------------
        existing_order = (
            session.query(Order)
            .filter(
                Order.trade_signal_id == trade_signal_id
            )
            .first()
        )

        order_created = False

        if existing_order:
            db_order = existing_order

            logger.info(
                "[insert_order_and_oms_bucket] "
                f"Order already exists: "
                f"order_id={db_order.order_id}, "
                f"trade_signal_id={trade_signal_id}"
            )

        else:
            # Your circuit-range validation can remain inside
            # the existing insert/validation flow before this point.

            similar_order_exists = (
                session.query(Order.order_id)
                .filter(
                    Order.stock_tick == stock_name,
                    Order.time_frame == time_fr,
                    Order.entry_price == entry_price,
                    Order.stoploss_price == stoploss_price,
                    Order.target_price == target_price,
                )
                .first()
            )

            if similar_order_exists:
                logger.warning(
                    "[insert_order_and_oms_bucket] "
                    f"Similar order already exists: "
                    f"stock={stock_name}, "
                    f"time_frame={time_fr}, "
                    f"entry={entry_price}"
                )

                return {
                    "success": False,
                    "status": "SIMILAR_ORDER_EXISTS",
                    "order_id": similar_order_exists[0],
                    "bucket_id": None,
                    "trade_signal_id": trade_signal_id,
                }

            if not(is_similar_order_exists_orm("order", stock_name, time_fr, entry_price,
                                               stoploss_price, target_price)):

                db_order = Order(
                    stock_tick=stock_name,
                    stock_id=stock_id,
                    country_id=country_id,
                    time_frame=time_fr,
                    order_type=order_type,

                    entry_price=entry_price,
                    stoploss_price=stoploss_price,
                    target_price=target_price,

                    stock_quantity=stock_quantity,

                    purchased_cmp_date=created_at.strftime(
                        "%Y-%m-%dT%H:%M"
                    ),
                    purchased_on=datetime.now(),

                    arima_ab_model_prediction=prediction,
                    arima_ab_model_prob=probability,

                    trade_signal_id=trade_signal_id,
                    zone_signature=trade_data.get("zone_signature"),
                    base_start_idx=trade_data.get("base_start_idx"),
                    legout_end_idx=trade_data.get("legout_end_idx"),
                )

                session.add(db_order)

                # Generate order_id without committing yet.
                session.flush()

                order_created = True

                logger.info(
                    "[insert_order_and_oms_bucket] "
                    f"Order prepared: "
                    f"order_id={db_order.order_id}, "
                    f"stock={stock_name}, "
                    f"type={order_type}"
                )

        # ---------------------------------------------------------
        # Check whether OMS bucket already exists
        # ---------------------------------------------------------
        existing_bucket = (
            session.query(OMSOrderBucket)
            .filter(
                OMSOrderBucket.trade_signal_id
                == trade_signal_id
            )
            .first()
        )

        bucket_created = False

        if existing_bucket:
            db_bucket = existing_bucket

            logger.info(
                "[insert_order_and_oms_bucket] "
                f"OMS bucket already exists: "
                f"bucket_id={db_bucket.bucket_id}, "
                f"trade_signal_id={trade_signal_id}"
            )

        else:

            if not(is_similar_order_exists_orm("oms_order_bucket", stock_name, time_fr, entry_price,
                                                           stoploss_price, target_price)):
                db_bucket = OMSOrderBucket(
                    order_id=db_order.order_id,
                    trade_signal_id=trade_signal_id,

                    stock_tick=stock_name,
                    stock_id=stock_id,
                    country_id=country_id,
                    time_frame=time_fr,
                    order_type=order_type,

                    entry_price=entry_price,
                    stoploss_price=stoploss_price,
                    target_price=target_price,

                    stock_quantity=stock_quantity,

                    purchased_cmp_date=created_at.strftime(
                        "%Y-%m-%dT%H:%M"
                    ),
                    purchased_on=datetime.now(),

                    order_status="pending",
                    is_trade_started=False,
                    is_evaluated=False,

                    entry_timestamp=None,
                    completed_on=None,

                    lstm_rf_model_prediction=None,
                    lstm_rf_model_prob=None,

                    # Preserve your existing prediction mapping.
                    arima_ab_model_prediction=prediction,
                    arima_ab_model_prob=probability,

                    approval_status="pending",
                    approved_by=None,
                    approved_at=None,

                    id_fyers=None,
                    gtt_id=None,
                )

                session.add(db_bucket)

                # Generate bucket_id before commit.
                session.flush()

                bucket_created = True

                logger.info(
                    "[insert_order_and_oms_bucket] "
                    f"OMS bucket prepared: "
                    f"bucket_id={db_bucket.bucket_id}, "
                    f"order_id={db_order.order_id}"
                )

        # Both records commit together.
        session.commit()
        
        if order_created and bucket_created:
            place_real_fyers_order(db_bucket.bucket_id)
            insert_user_alert(country_id, trade_signal_id, stock_name, order_type, entry_price, time_fr, trade_data)

            return {
                "success": True,
                "status": "INSERTED",
                "order_id": db_order.order_id,
                "bucket_id": db_bucket.bucket_id,
                "trade_signal_id": trade_signal_id,
                "order_created": order_created,
                "bucket_created": bucket_created,
            }

        else:
            return {
                "success": False,
                "status": "SIMILAR_ORDER/BUCKET_EXISTS",
                "order_id": db_order.order_id if db_order else None,
                "bucket_id": db_bucket.bucket_id if db_bucket else None,
                "trade_signal_id": trade_signal_id,
                "order_created": order_created,
                "bucket_created": bucket_created,
            }

    except Exception as ex:
        session.rollback()

        logger.error(
            "[insert_order_and_oms_bucket] "
            f"Failed for stock={stock_name}, "
            f"trade_signal_id={trade_signal_id}: {ex}",
            exc_info=True,
        )

        return {
            "success": False,
            "status": "ERROR",
            "order_id": None,
            "bucket_id": None,
            "trade_signal_id": trade_signal_id,
            "error": str(ex),
        }

    finally:
        session.close()





def place_real_fyers_order(bucket_id):
    session = dbc.get_session()
    row = session.query(OMSOrderBucket).filter(OMSOrderBucket.bucket_id == bucket_id).first()
    if row is None:
        return 0

    # PRODUCTION GUARD: Check GLOBAL_HALT before any new cash entry
    try:
        from scripts.futures_state_store import StateStore
        from scripts.futures_risk_engine import RiskEngine
        store = StateStore(dbc.engine)
        risk = RiskEngine(store)
        if risk.cash_entry_allowed() == 'GLOBAL_REALIZED_LOSS_CEILING':
            logger.warning(f"GLOBAL_HALT ACTIVE: Blocking new cash entry for bucket {bucket_id}")
            return 0
    except Exception as e:
        logger.error(f"Global Halt check failed: {e}. Failing closed (blocking order).")
        return 0

    print(bucket_id)
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
        # fyers.place_cash_gtt
        if response and response.get("s") == "ok":
            row.gtt_id = response.get("id")
            row.id_fyers = response.get("id_fyers")
            row.approval_status = "approved"
            row.approved_by = 1
            row.approved_at = datetime.now()
            session.add(row)
            session.commit()
            print("GTB ID updated successfully")
            return 1
        else:
            return 0


def insert_user_alert(country_id, trade_signal_id, stock_tick_name, order_type, entry_price,
                      time_frame, trade_data):
    try:
        session = dbc.get_session()
        trade = session.query(TradeSignal).filter(TradeSignal.id == trade_signal_id).first()
        exchange_id = None
        if trade:
            exchange_id = trade.exchange_id
        st_sym, exchange_name = get_stock_symbol(stock_tick_name, exchange_id)
        if "EXP_NUM" in trade_data.keys():
            alert_ord = UserAlert(
                                    user_id=1,
                                    country_id=country_id,
                                    stock_symbol=st_sym,
                                    exchange=exchange_name,
                                    alert_type=order_type,
                                    trigger_price=entry_price,
                                    price_range_min= entry_price - (entry_price * 0.005),
                                    price_range_max= entry_price + (entry_price * 0.005),
                                    threshold=0.5,  # percent
                                    is_active=True,
                                    recurrent=False,
                                    created_at=datetime.now(),
                                    cooldown_minutes=2,
                                    timeframe=str(time_frame),
                                    note='Entry Near Setup',
                                    expiry_date=trade_data["EXP_NUM"],
                                    trade_signal_id=trade_signal_id
                                )
        else:
            alert_ord = UserAlert(
                                    user_id=1,
                                    country_id=country_id,
                                    stock_symbol=st_sym,
                                    exchange=exchange_name,
                                    alert_type=order_type,
                                    trigger_price=entry_price,
                                    price_range_min= entry_price - (entry_price * 0.005),
                                    price_range_max= entry_price + (entry_price * 0.005),
                                    threshold=0.5,  # percent
                                    is_active=True,
                                    recurrent=False,
                                    created_at=datetime.now(),
                                    cooldown_minutes=2,
                                    timeframe=str(time_frame),
                                    note='Entry Near Setup',
                                    trade_signal_id=trade_signal_id
                                )
        
        session.add(alert_ord)
        session.commit()
        session.refresh(alert_ord)
        session.close()
        logger.info(f"User alert inserted for trade_signal_id : {trade_signal_id}, alert_id : {alert_ord.id}")
    except Exception as ex:
        logger.exception(ex)
        logger.error(f"Error inserting user alert for trade_signal_id {trade_signal_id}: {ex}")
