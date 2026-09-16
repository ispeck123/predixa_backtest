import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from fyers_apiv3.FyersWebsocket import order_ws
from data_fetchers.fyers.fyers_session import fyers_access_token_handler
from shared.db.db_model import OMSOrderBucket, OMSOrderBucketFuture, OMSOrderBucketCommodity, Order, Commodity_Order, Future_Order
from shared.db.fyers_req_model import CreateGTTOCORequest, build_gtt_oco_payload, get_fyers
from data_fetchers.fyers.fyers_utils import normalize_cash_oco_prices, bucket_format_futures_contract, clear_all_futures_orders
from shared.db.dbconn import DBConnection
from scripts.futures_state_store import StateStore
from scripts.futures_risk_engine import RiskEngine
from scripts.graduated_live_execution import GraduatedLiveExecution
import logging
from datetime import datetime
import time
from threading import Thread
from sqlalchemy import or_

access_token = fyers_access_token_handler().get_access_token()
logfile = "fyers_order_response.log"
db = DBConnection()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

_option3_execution = None


def _get_option3_execution():
    """Build the winner adapter lazily so import does not create broker state."""
    global _option3_execution
    if _option3_execution is None:
        store = StateStore(db.engine)
        risk = RiskEngine(store)
        _option3_execution = GraduatedLiveExecution(risk, get_fyers())
    return _option3_execution


def _option3_signal_id(order, snapshot):
    """Match only persisted broker IDs; symbol/price are never identifiers."""
    identifiers = {
        str(value) for value in (
            order.get("id"), order.get("id_fyers"), order.get("orderTag"),
            order.get("ordertag"),
        ) if value not in (None, "")
    }
    for signal_id, reservation in (snapshot.get("reservations") or {}).items():
        broker_order = (snapshot.get("broker_orders") or {}).get(signal_id) or {}
        persisted = {
            str(value) for value in (
                signal_id, reservation.get("gtt_id"), reservation.get("broker_order_id"),
                broker_order.get("entry_gtt_id"), broker_order.get("entry_order_id"),
            ) if value not in (None, "")
        }
        if identifiers & persisted:
            return signal_id
    return None


def _option3_exit_signal_id(order, snapshot):
    """Match a filled OCO leg against persisted winner protection IDs only."""
    identifiers = {
        str(value) for value in (order.get("id"), order.get("id_fyers"))
        if value not in (None, "")
    }
    winner = snapshot.get("active_winner") or snapshot.get("serial_reservation")
    if not winner:
        return None
    broker_order = (snapshot.get("broker_orders") or {}).get(winner) or {}
    protected = {
        str(value) for value in (
            broker_order.get("protective_gtt_id"), broker_order.get("protective_order_id"),
            broker_order.get("protective_id_fyers"),
        ) if value not in (None, "")
    }
    return winner if identifiers & protected else None


def handle_option3_order_event(message, *, execution=None):
    """Process a confirmed FYERS futures fill for our Option-3 pool only.

    The existing socket uses ``orders.status == 2`` for Traded/Filled.  Other
    messages are deliberately left to the legacy callback branches.
    """
    if not isinstance(message, dict) or message.get("s") != "ok":
        return {"matched": False, "reason": "INVALID_ORDER_EVENT"}
    order = message.get("orders")
    if not isinstance(order, dict) or order.get("status") != 2:
        return {"matched": False, "reason": "NOT_CONFIRMED_FILL"}
    symbol = str(order.get("symbol") or "")
    if not (order.get("exchange") == 10 and symbol.startswith("NSE:") and symbol.endswith("FUT")):
        return {"matched": False, "reason": "NOT_NSE_FUTURES"}
    execution = execution or _get_option3_execution()
    snapshot = execution.risk.store.snapshot()
    exit_signal_id = _option3_exit_signal_id(order, snapshot)
    if exit_signal_id is not None:
        result = execution.risk.mark_winner_exit_filled(
            exit_signal_id,
            exit_id=order.get("id") or order.get("id_fyers"),
            event_id="FYERS_EXIT:%s" % (order.get("id") or exit_signal_id),
        )
        return {"matched": True, "signal_id": exit_signal_id, "exit": True, "result": result,
                "execution": execution}
    signal_id = _option3_signal_id(order, snapshot)
    if signal_id is None:
        logger.info("Ignoring unmatched futures order event for Option-3: %s", order.get("id"))
        return {"matched": False, "reason": "UNMATCHED_OPTION3_ORDER"}
    result = execution.claim_futures_winner_and_cancel_losers(
        signal_id, event_id="FYERS_ORDER:%s" % (order.get("id") or signal_id)
    )
    return {"matched": True, "signal_id": signal_id, "result": result,
            "execution": execution}

if not os.path.isfile(logfile):
    with open(logfile, "a") as fp:
        fp.write("\n")


def place_order(fyers, gtt_payload, bucket_id, max_retries=10, sleep_time=10):
    try:
        with open(logfile, "a") as fp:
            fp.write("\n" + f"Rate limit exceeded. Retrying after {sleep_time} seconds...")
        order_placed = False        
        while max_retries > 0:
            logger.info("\nRate limit exceeded. Retrying after %s seconds...", sleep_time)
            time.sleep(sleep_time)
            response = fyers._client.place_gtt_order(data=gtt_payload)
            if response.get("s") != "ok" and abs(response.get("code")) == 429:
                max_retries -= 1
            else:
                logger.info("GTTOCO order placed (bucket_id: %s): %s", bucket_id, response)
                with open(logfile, "a") as fp:
                    fp.write("\n" + f"GTTOCO order placed (bucket_id: {bucket_id}): {response}")
                order_placed = True
                break
        if not order_placed: 
            logger.info("Failed to place GTTOCO order (bucket_id: %s), place the order manually ! %s", bucket_id, response)
            with open(logfile, "a") as fp:
                fp.write("\n" + f"Failed to place GTTOCO order (bucket_id: {bucket_id}), place the order manually ! {response}")
    except Exception as er:
        logger.error(er)


def onOrder(message):
    """
    Callback function to handle incoming messages from the FyersDataSocket WebSocket.

    Parameters:
        message (dict): The received message from the WebSocket.

    """
    logger.info(f"Order Response: {message}")
    with open(logfile, "a") as fp:
        fp.write("\n" + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + " --> " + str(message))
    session = db.get_session()
    try:
        option3_result = handle_option3_order_event(message)
        if option3_result.get("matched"):
            logger.info("Option-3 futures winner processing: %s", option3_result)
        if message.get("s") == "ok" and "orders" in message:
            resp = message["orders"]

            # Process the order data as needed
            # status -> 2 (Traded / Filled) 
            if  resp.get("status") == 2:
                try:
                    bucket_id = resp.get("orderTag").replace("1:GTT", "")
                    bucket_id = int(bucket_id)
                except Exception as e:
                    logger.error(e)
                    bucket_id = None
                
                symbol = resp.get("symbol")
                side = resp.get("side")  # side -> 1(Buy), -1(Sell)
                if side == 1:
                    order_type = "BUY"
                elif side == -1:
                    order_type = "SELL"   
                row = None

                #################### For NSE ##########################
                if resp.get("exchange") == 10 and symbol.startswith("NSE:") and symbol.endswith("-EQ"):


                    ################ Checking for Entry Hit ##################
                    if bucket_id is not None:
                        row = session.query(OMSOrderBucket).filter(OMSOrderBucket.bucket_id == bucket_id,
                                                                OMSOrderBucket.is_trade_started == 0,
                                                                OMSOrderBucket.order_type == order_type).first()
                        if row:
                            oco_side = 1 if row.order_type == "BUY" else -1
                            prices = normalize_cash_oco_prices(
                                entry=row.entry_price,
                                target=row.target_price,
                                stoploss=row.stoploss_price,
                                order_type=row.order_type,
                                # Best option:
                                # tick_size=row.tick_size
                                )

                            oco_body = CreateGTTOCORequest(
                                symbol=symbol,
                                side=oco_side,
                                entry=prices["entry"],
                                target=prices["target"],
                                stoploss=prices["stoploss"],
                                qty=row.stock_quantity,
                                productType="CNC",
                                orderTag=str(bucket_id)
                            )
                                # oco_body = CreateGTTOCORequest(
                                #     symbol=symbol,
                                #     side=oco_side,
                                #     entry=row.entry_price,
                                #     target=row.target_price,
                                #     stoploss=row.stoploss_price,
                                #     qty=row.stock_quantity,
                                #     productType="CNC"
                                # )
                            gtt_payload = build_gtt_oco_payload(oco_body, ltp=resp.get("limitPrice"))
                            fyers = get_fyers()

                            response = fyers._client.place_gtt_order(data=gtt_payload)
                            # logger.info("GTTOCO order placed (bucket_id: %s): %s", bucket_id, response)
                            # with open(logfile, "a") as fp:
                            #     fp.write("\n" + f"GTTOCO order placed (bucket_id: {bucket_id}): {response}")
                            if response.get("s") != "ok" and abs(response.get("code")) == 429:
                                Thread(target=place_order, args=(fyers, gtt_payload, bucket_id), daemon=True).start()
                            elif response.get("s") != "ok":
                                logger.info("Failed to place GTTOCO order (bucket_id: %s), place the order manually ! %s", bucket_id, response)
                                with open(logfile, "a") as fp:
                                    fp.write("\n" + f"Failed to place GTTOCO order (bucket_id: {bucket_id}), place the order manually ! {response}")
                            elif response.get("s") == "ok":
                                logger.info("GTTOCO order placed (bucket_id: %s): %s", bucket_id, response)
                                with open(logfile, "a") as fp:
                                    fp.write("\n" + f"GTTOCO order placed (bucket_id: {bucket_id}): {response}")
                                row.id_fyers_gtt_oco = response.get("id_fyers")
                                row.gtt_oco_id = response.get("id")
                            else:
                                logger.info("GTTOCO order response (bucket_id: %s): %s", bucket_id, response)
                                with open(logfile, "a") as fp:
                                    fp.write("\n" + f"GTTOCO order response (bucket_id: {bucket_id}): {response}")

                            row.order_status = "executed"
                            row.is_trade_started = 1
                            dt = datetime.strptime(resp.get("orderDateTime"), "%d-%b-%Y %H:%M:%S")
                            row.entry_timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")

                            order_row  = session.query(Order).filter(Order.trade_signal_id == row.trade_signal_id).first()
                            if order_row:
                                order_row.is_trade_started = 1
                                order_row.entry_timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
                                order_row.order_status = "{'status': 'pending', 'reason': 'Still active', 'entry_hit': True}"
                                session.add(order_row)

                    ################ Checking for Target/Stoploss Hit ##################
                    else:
                        stock = db.raw_query(f"""select * from ind_stock_master where fyers_symbol = '{symbol}' and is_active = 1""")
                        logger.info(f"Stock query result: {stock}")
                        if stock and len(stock) > 0:
                            stock_id = stock[0].get("id")
                            limit_price = resp.get("limitPrice")
                            row = session.query(OMSOrderBucket).filter(OMSOrderBucket.stock_id == stock_id,
                                                                or_(OMSOrderBucket.target_price.between(limit_price-1, limit_price+1),      
                                                                    OMSOrderBucket.stoploss_price.between(limit_price-1, limit_price+1)),
                                                                OMSOrderBucket.is_trade_started == 1,
                                                                OMSOrderBucket.is_evaluated == 0).first()
                            if row:
                                logger.info(row.__dict__)
                                row.order_status = "completed"
                                row.is_evaluated = 1
                                dt = datetime.strptime(resp.get("orderDateTime"), "%d-%b-%Y %H:%M:%S")
                                row.completed_on = dt.strftime("%Y-%m-%d %H:%M:%S")
                                bucket_id = row.bucket_id


                #################### For NSEFO #############################
                elif resp.get("exchange") == 10 and symbol.startswith("NSE:") and symbol.endswith("FUT"):
                    
                    ################ Checking for Entry Hit ##################
                    row = session.query(OMSOrderBucketFuture).filter(OMSOrderBucketFuture.bucket_id == bucket_id,
                                                                    OMSOrderBucketFuture.is_trade_started == 0,
                                                                    OMSOrderBucketFuture.order_type == order_type).first()
                    if row:
                        oco_side = 1 if row.order_type == "BUY" else -1
                        prices = normalize_cash_oco_prices(
                            entry=row.entry_price,
                            target=row.target_price,
                            stoploss=row.stoploss_price,
                            order_type=row.order_type,
                            # Best option:
                            # tick_size=row.tick_size
                            )
                        symbol = bucket_format_futures_contract(symbol, row.expiry_date)
                        oco_body = CreateGTTOCORequest(
                            symbol=symbol,
                            side=oco_side,
                            entry=prices["entry"],
                            target=prices["target"],
                            stoploss=prices["stoploss"],
                            qty=row.stock_quantity,
                            productType="CNC",
                            orderTag=str(bucket_id)
                        )
                        gtt_payload = build_gtt_oco_payload(oco_body, ltp=resp.get("limitPrice"))
                        fyers = get_fyers()
                        response = fyers._client.place_gtt_order(data=gtt_payload)
                        # logger.info("GTTOCO order placed (bucket_id: %s): %s", bucket_id, response)
                        # with open(logfile, "a") as fp:
                        #     fp.write("\n" + f"GTTOCO order placed (bucket_id: {bucket_id}): {response}")
                        if response.get("s") != "ok" and abs(response.get("code")) == 429:
                            Thread(target=place_order, args=(fyers, gtt_payload, bucket_id), daemon=True).start()
                        elif response.get("s") != "ok":
                            logger.info("Failed to place GTTOCO order (bucket_id: %s), place the order manually ! %s", bucket_id, response)
                            with open(logfile, "a") as fp:
                                fp.write("\n" + f"Failed to place GTTOCO order (bucket_id: {bucket_id}), place the order manually ! {response}")
                        elif response.get("s") == "ok":
                            logger.info("GTTOCO order placed (bucket_id: %s): %s", bucket_id, response)
                            with open(logfile, "a") as fp:
                                fp.write("\n" + f"GTTOCO order placed (bucket_id: {bucket_id}): {response}")
                            row.id_fyers_gtt_oco = response.get("id_fyers")
                            row.gtt_oco_id = response.get("id")
                            if (option3_result.get("matched")
                                    and not option3_result.get("exit")
                                    and option3_result.get("result", {}).get("status") == "WINNER"):
                                option3_result["execution"].risk.record_protective_order(
                                    option3_result["signal_id"],
                                    gtt_id=response.get("id"),
                                    broker_order_id=response.get("id_fyers"),
                                )
                        else:
                            logger.info("GTTOCO order response (bucket_id: %s): %s", bucket_id, response)
                            with open(logfile, "a") as fp:
                                fp.write("\n" + f"GTTOCO order response (bucket_id: {bucket_id}): {response}")

                        row.order_status = "executed"
                        row.is_trade_started = 1
                        dt = datetime.strptime(resp.get("orderDateTime"), "%d-%b-%Y %H:%M:%S")
                        row.entry_timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")

                        order_row  = session.query(Future_Order).filter(Future_Order.trade_signal_id == row.trade_signal_id).first()
                        if order_row:
                            order_row.is_trade_started = 1
                            order_row.entry_timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
                            order_row.order_status = "{'status': 'pending', 'reason': 'Still active', 'entry_hit': True}"
                            session.add(order_row)
                        
                        clear_all_futures_orders(fyers)

                    # ################ Checking for Target/Stoploss Hit ##################
                    # else:
                    #     limit_price = resp.get("limitPrice")
                    #     fut_symbol = symbol.replace("NSE:","")[:-3]
                    #     row = session.query(OMSOrderBucketFuture).filter(OMSOrderBucketFuture.stock_tick == fut_symbol,
                    #                                         or_(OMSOrderBucketFuture.target_price.between(limit_price-1, limit_price+1),      
                    #                                             OMSOrderBucketFuture.stoploss_price.between(limit_price-1, limit_price+1)),
                    #                                         OMSOrderBucketFuture.is_trade_started == 1,
                    #                                         OMSOrderBucketFuture.is_evaluated == 0).first()
                    #     if row:
                    #         logger.info(row.__dict__)
                    #         row.order_status = "completed"
                    #         row.is_evaluated = 1
                    #         dt = datetime.strptime(resp.get("orderDateTime"), "%d-%b-%Y %H:%M:%S")
                    #         row.completed_on = dt.strftime("%Y-%m-%d %H:%M:%S")
                    #         bucket_id = row.bucket_id

                    
                # ##################### For MCX ###############################
                # elif resp.get("exchange") == 11 and symbol.startswith("MCX:"):

                #     ################ Checking for Entry Hit ##################
                #     row = session.query(OMSOrderBucketCommodity).filter(OMSOrderBucketCommodity.bucket_id == bucket_id,
                #                                                         OMSOrderBucketCommodity.is_trade_started == 0,
                #                                                         OMSOrderBucketCommodity.order_type == order_type).first()
                #     if row:
                #         oco_side = 1 if row.order_type == "BUY" else -1
                #         oco_body = CreateGTTOCORequest(
                #             symbol=symbol,
                #             side=oco_side,
                #             entry=row.entry_price,
                #             target=row.target_price,
                #             stoploss=row.stoploss_price,
                #             qty=row.stock_quantity,
                #             productType="CNC",
                #             orderTag=str(bucket_id)
                #         )
                #         gtt_payload = build_gtt_oco_payload(oco_body, ltp=resp.get("limitPrice"))
                #         fyers = get_fyers()
                #         response = fyers._client.place_gtt_order(data=gtt_payload)
                #         # logger.info("GTTOCO order placed (bucket_id: %s): %s", bucket_id, response)
                #         # with open(logfile, "a") as fp:
                #         #     fp.write("\n" + f"GTTOCO order placed (bucket_id: {bucket_id}): {response}")
                #         if response.get("s") != "ok" and abs(response.get("code")) == 429:
                #             Thread(target=place_order, args=(fyers, gtt_payload, bucket_id), daemon=True).start()
                #         elif response.get("s") != "ok":
                #             logger.info("Failed to place GTTOCO order (bucket_id: %s), place the order manually ! %s", bucket_id, response)
                #             with open(logfile, "a") as fp:
                #                 fp.write("\n" + f"Failed to place GTTOCO order (bucket_id: {bucket_id}), place the order manually ! {response}")
                #         elif response.get("s") == "ok":
                #             logger.info("GTTOCO order placed (bucket_id: %s): %s", bucket_id, response)
                #             with open(logfile, "a") as fp:
                #                 fp.write("\n" + f"GTTOCO order placed (bucket_id: {bucket_id}): {response}")
                #             row.id_fyers_gtt_oco = response.get("id_fyers")
                #             row.gtt_oco_id = response.get("id")
                #         else:
                #             logger.info("GTTOCO order response (bucket_id: %s): %s", bucket_id, response)
                #             with open(logfile, "a") as fp:
                #                 fp.write("\n" + f"GTTOCO order response (bucket_id: {bucket_id}): {response}")

                #         row.order_status = "executed"
                #         row.is_trade_started = 1
                #         dt = datetime.strptime(resp.get("orderDateTime"), "%d-%b-%Y %H:%M:%S")
                #         row.entry_timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")

                #         order_row  = session.query(Commodity_Order).filter(Commodity_Order.trade_signal_id == row.trade_signal_id).first()
                #         if order_row:
                #             order_row.is_trade_started = 1
                #             order_row.entry_timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
                #             order_row.order_status = "{'status': 'pending', 'reason': 'Still active', 'entry_hit': True}"
                #             session.add(order_row)

                #     ################ Checking for Target/Stoploss Hit ##################
                #     else:
                #         row = session.query(OMSOrderBucketCommodity).filter(OMSOrderBucketCommodity.bucket_id == bucket_id,
                #                                                 OMSOrderBucketCommodity.is_trade_started == 1,
                #                                                 OMSOrderBucketCommodity.is_evaluated == 0).first()
                #         if row:
                #             row.order_status = "completed"
                #             row.is_evaluated = 1
                #             dt = datetime.strptime(resp.get("orderDateTime"), "%d-%b-%Y %H:%M:%S")
                #             row.completed_on = dt.strftime("%Y-%m-%d %H:%M:%S")


                if row:
                    session.add(row)
                    session.commit()
                    msg = f"Order status updated (bucket_id : {bucket_id}, symbol : {symbol}, stock_tick : {row.stock_tick})"
                    logger.info(msg)
                    with open(logfile, "a") as fp:
                        fp.write("\n" + msg)

    except Exception as ex:
        logger.error(ex)
    finally:
        session.close()


def onTrade(message):
    """
    Callback function to handle incoming messages from the FyersDataSocket WebSocket.

    Parameters:
        message (dict): The received message from the WebSocket.

    """
    logger.info(f"Trade Response: {message}")
    with open(logfile, "a") as fp:
        fp.write("\n" + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + " --> " + str(message))


def onPosition(message):
    """
    Callback function to handle incoming messages from the FyersDataSocket WebSocket.

    Parameters:
        message (dict): The received message from the WebSocket.

    """
    logger.info(f"Position Response: {message}")
    with open(logfile, "a") as fp:
        fp.write("\n" + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f") + " --> " + str(message))


def onerror(message):
    """
    Callback function to handle WebSocket errors.

    Parameters:
        message (dict): The error message received from the WebSocket.

    """
    logger.error(f"Error: {message}")


def onclose(message):
    """
    Callback function to handle WebSocket connection close events.
    """
    logger.info(f"Connection closed: {message}")


def onopen():
    """
    Callback function to subscribe to data type and symbols upon WebSocket connection.

    """
    # Specify the data type and symbols you want to subscribe to
    # data_type = "OnOrders"
    # data_type = "OnTrades"
    # data_type = "OnPositions"
    # data_type = "OnGeneral"
    data_type = "OnOrders,OnTrades,OnPositions,OnGeneral"

    fyers.subscribe(data_type=data_type)

    # Keep the socket running to receive real-time data
    fyers.keep_running()


# Replace the sample access token with your actual access token obtained from Fyers
#access_token = "AAAAAAAA-100:eyJ..."

# Create a FyersDataSocket instance with the provided parameters
fyers = order_ws.FyersOrderSocket(
    access_token=access_token,  # Your access token for authenticating with the Fyers API.
    write_to_file=True,        # A boolean flag indicating whether to write data to a log file or not.
    log_path=".",                # The path to the log file if write_to_file is set to True (empty string means current directory).
    on_connect=onopen,          # Callback function to be executed upon successful WebSocket connection.
    on_close=onclose,           # Callback function to be executed when the WebSocket connection is closed.
    on_error=onerror,           # Callback function to handle any WebSocket errors that may occur.
    on_orders=onOrder,          # Callback function to handle order-related events from the WebSocket.
    on_trades=onTrade,          # Callback function to handle trade-related events from the WebSocket.
    on_positions=onPosition,    # Callback function to handle position-related events from the Web
)





# Establish a connection only when this module is explicitly run.  Importing
# callbacks in tests or another service must never open a production socket.
if __name__ == "__main__":
    fyers.connect()


# ------------------------------------------------------------------------------------------------------------------------------------------
# Sample Success Response 
# ------------------------------------------------------------------------------------------------------------------------------------------
                      
# {
#   "s":"ok",
#   "orders":{
#       "clientId":"FY****",
#       "id":"23080400089344",
#       "exchOrdId":"1100000009596016",
#       "qty":1,
#       "filledQty":1,
#       "limitPrice":7.95,
#       "type":1,
#       "fyToken":"101000000014366",
#       "exchange":10,
#       "segment":10,
#       "symbol":"NSE:IDEA-EQ",
#       "instrument":0,
#       "offlineOrder":false,
#       "orderDateTime":"04-Aug-2023 10:12:58",
#       "orderValidity":"DAY",
#       "productType":"INTRADAY",
#       "side":-1,
#       "status":90,
#       "source":"W",
#       "ex_sym":"IDEA",
#       "description":"VODAFONE IDEA LIMITED",
#       "orderNumStatus":"23080400089344:2",
#       "id_fyers":"1b30241e-2819-4ec9-a3e4-69b6155cacab"
#   }
# }
