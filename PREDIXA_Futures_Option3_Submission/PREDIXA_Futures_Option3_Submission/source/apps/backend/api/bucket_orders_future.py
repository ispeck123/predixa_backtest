from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from shared.db.db_model import OMSOrderBucketFuture, Future_Order
from shared.db.req_model import (BucketOrderBase, Update_BucketOrderBase, GetOrder,
                                  ApproveReject_BucketOrderBase, EditFutureCandidateRequest)
from data_fetchers.fyers.fyers_utils import normalize_cash_oco_prices
from shared.db.dbconn import DBConnection
from shared.db.api_responce import APIResponse
import logging
from datetime import datetime
from sqlalchemy import text, or_
import ast, re
import json
from data_fetchers.fyers.get_stock_prices import StockPriceFetcher


logger = logging.getLogger("fyers_orders")
logging.basicConfig(level=logging.INFO)

db = DBConnection()
router = APIRouter()
#spf = StockPriceFetcher()


def _futures_response(**kwargs):
    from apps.backend.api.futures_approval_response import build_futures_response
    return build_futures_response(**kwargs)


def _retry_flags(reason):
    non_retryable = {
        "FUT_REJECT_ACTIVE_FUTURES_WINNER",
        "FUT_REJECT_STATE_RECONCILIATION_FAILED",
        "FYERS_GTT_STATUS_AMBIGUOUS",
        "FUT_REJECT_BROKER_LINKED_CANDIDATE",
    }
    return {"retryable": reason not in non_retryable,
            "editable": reason not in non_retryable}


def _risk_state_after(risk):
    if risk is None or not hasattr(risk, "store"):
        return None


def _broker_confirms_gtt_absent(value):
    """Return true only for an explicit broker 'already absent' result."""
    if value is None:
        return False
    if isinstance(value, BaseException):
        text = str(value).lower()
    else:
        try:
            text = json.dumps(value, default=str).lower()
        except (TypeError, ValueError):
            text = str(value).lower()
    return any(marker in text for marker in (
        "not found", "does not exist", "doesn't exist", "no such gtt",
        "already cancelled", "already canceled",
    ))


def _gtt_absent_from_orderbook(snapshot, gtt_id):
    """Verify absence only from a successful broker orderbook snapshot."""
    if not isinstance(snapshot, dict) or snapshot.get("s") != "ok":
        return False
    orders = snapshot.get("orderBook")
    if not isinstance(orders, list):
        return False
    return not any(str(item.get("id")) == str(gtt_id)
                   for item in orders if isinstance(item, dict))
    try:
        return risk.store.snapshot().get("state")
    except Exception:
        return None


def _future_risk():
    from scripts.futures_state_store import StateStore
    from scripts.futures_risk_engine import RiskEngine
    store = StateStore(DBConnection().engine)
    store.sync_owner_approved_gates()
    return RiskEngine(store)


def _future_fyers():
    from shared.db.fyers_req_model import get_fyers
    return get_fyers()


def _parse_expiry(value):
    if value is None:
        return None
    text_value = str(value).strip()
    for fmt in ("%d%m%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text_value, fmt).date()
        except ValueError:
            continue
    return None


def format_futures_contract(symbol, expiry):
    """Build the NSE stock-futures symbol for the persisted contract expiry."""
    if not symbol or expiry is None:
        return None
    return f"NSE:{str(symbol).strip().upper()}{expiry.strftime('%y%b').upper()}FUT"


def _approver_id(value):
    try:
        result = int(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="approved_by must be numeric") from None
    if result <= 0:
        raise HTTPException(status_code=422, detail="approved_by must be positive")
    return result


def _refresh_approved_contract(session, risk, symbol):
    """Resolve a scanner-created contract added after backend startup.

    Startup reconciliation may run before the daily scanner.  Refreshing only
    this symbol from FuturesMaster is read-only and does not mark reconciliation
    ready or bypass any risk gate.
    """
    from scripts.futures_reconciliation import build_approved_contracts

    contracts = build_approved_contracts(session, [symbol])
    contract = contracts.get(symbol)
    if contract is None:
        return None
    with risk.store.transaction() as (data, _):
        data.setdefault("approved", {})[symbol] = contract
    return contract


def approve_existing_futures_candidate(*, bucket_id, status, approved_by,
                                        session_factory=None, risk_factory=_future_risk,
                                        fyers_factory=_future_fyers, now_factory=datetime.now):
    """Approve one scanner-created futures row and submit its guarded entry GTT.

    This function deliberately does not create TradeSignal, Future_Order, or
    OMSOrderBucketFuture rows.  The scanner owns those records.
    """
    session = session_factory() if session_factory else db.get_session()
    risk = None
    row = None
    try:
        row = session.query(OMSOrderBucketFuture).filter(
            OMSOrderBucketFuture.bucket_id == bucket_id
        ).first()
        if row is None:
            raise HTTPException(status_code=404, detail="FUTURES_BUCKET_NOT_FOUND")

        normalized_status = str(status).strip().lower()
        if normalized_status == "pending":
            if row.approval_status != "rejected":
                raise HTTPException(status_code=409, detail="FUT_REJECT_NOT_REJECTED")
            if row.gtt_id or row.id_fyers or int(row.is_trade_started or 0) != 0:
                raise HTTPException(status_code=409, detail="FUT_REJECT_BROKER_LINKED_CANDIDATE")
            risk = risk_factory()
            before = risk.store.snapshot().get("state")
            release = getattr(risk, "release_unsubmitted_reservation", None)
            if release is not None:
                release(str(row.trade_signal_id), reason="CANDIDATE_REOPENED")
            row.approval_status = "pending"
            row.approved_by = None
            row.approved_at = None
            row.order_status = "pending"
            session.commit()
            after = risk.store.snapshot().get("state")
            return _futures_response(
                reason_code="FUTURES_CANDIDATE_REOPENED", http_status=200, row=row,
                risk=risk, success=True, decision="REOPENED",
                approval_status=row.approval_status,
                is_trade_started=row.is_trade_started,
                state={"before": before, "after": after},
                extra={"status": "pending", "retryable": True, "editable": True})
        if normalized_status == "rejected":
            if row.approval_status == "pending" and not row.gtt_id and not row.id_fyers:
                risk = risk_factory()
                before = risk.store.snapshot().get("state")
                release = getattr(risk, "release_unsubmitted_reservation", None)
                if release is not None:
                    release(str(row.trade_signal_id), reason="USER_REJECTED")
                row.approval_status = "rejected"
                row.approved_by = _approver_id(approved_by)
                row.approved_at = now_factory()
                session.commit()
                after = risk.store.snapshot().get("state")
                return _futures_response(
                    reason_code="USER_REJECTED", http_status=200, row=row,
                    success=True, decision="REJECTED_BY_USER",
                    approval_status=row.approval_status,
                    is_trade_started=row.is_trade_started,
                    risk=risk, state={"before": before, "after": after},
                    extra={"status": "rejected", "retryable": True, "editable": True},
                )
            risk = risk_factory()
            snapshot = risk.store.snapshot()
            signal_id = str(row.trade_signal_id) if row.trade_signal_id is not None else None
            if snapshot.get("active_winner") == signal_id or snapshot.get("serial_reservation") == signal_id:
                raise HTTPException(status_code=409, detail="FUT_REJECT_ACTIVE_FUTURES_WINNER")
            if int(row.is_trade_started or 0) != 0:
                raise HTTPException(status_code=409, detail="FUT_REJECT_FILLED_ORDER")

            # A manually deleted GTT can leave the OMS broker ID present while
            # the durable reservation has already been removed.  The exact
            # persisted ID is still checked at FYERS; absence is terminal.
            if not row.gtt_id:
                release = getattr(risk, "release_unsubmitted_reservation", None)
                if release is not None and signal_id in (snapshot.get("reservations") or {}):
                    release(signal_id, reason="USER_REJECTED")
                row.approval_status = "rejected"
                row.approved_by = _approver_id(approved_by)
                row.approved_at = now_factory()
                row.order_status = "cancelled"
                row.id_fyers = None
                session.commit()
                after = _risk_state_after(risk)
                return _futures_response(
                    reason_code="FUTURES_GTT_ALREADY_ABSENT", http_status=200, row=row,
                    risk=risk, success=True, decision="REJECTED_BY_USER",
                    approval_status=row.approval_status,
                    is_trade_started=row.is_trade_started,
                    state={"after": after},
                    extra={"status": "rejected", "cancellation": "already_absent",
                           "retryable": True, "editable": True})
            reservation = (snapshot.get("reservations") or {}).get(signal_id)

            broker = fyers_factory()
            try:
                response = broker.cancel_gtt_order({"id": str(row.gtt_id)})
            except Exception as exc:
                if not _broker_confirms_gtt_absent(exc):
                    try:
                        absent_snapshot = broker.read_gtt_orderbook()
                    except Exception:
                        absent_snapshot = None
                    if not _gtt_absent_from_orderbook(absent_snapshot, row.gtt_id):
                        raise HTTPException(status_code=502, detail="Futures GTT cancellation is unresolved") from exc
                response = None
                cancellation_result = "already_absent"
            else:
                if isinstance(response, dict) and response.get("s") == "ok":
                    cancellation_result = "confirmed"
                elif _broker_confirms_gtt_absent(response):
                    cancellation_result = "already_absent"
                else:
                    try:
                        absent_snapshot = broker.read_gtt_orderbook()
                    except Exception:
                        absent_snapshot = None
                    if not _gtt_absent_from_orderbook(absent_snapshot, row.gtt_id):
                        raise HTTPException(status_code=502, detail="Futures GTT cancellation is unresolved")
                    cancellation_result = "already_absent"

            cancelled_gtt_id = row.gtt_id
            cancelled_fyers_id = row.id_fyers
            if reservation is not None:
                risk.confirm_entry_cancel(signal_id, broker_verified=True, filled=False, open_position=False)
            elif hasattr(risk, "recompute_futures_state"):
                risk.recompute_futures_state()
            row.approval_status = "rejected"
            row.approved_by = _approver_id(approved_by)
            row.approved_at = now_factory()
            row.order_status = "cancelled"
            row.is_trade_started = 0
            # The old broker IDs remain in reservation_history/audit.  Clear
            # the live OMS link so a confirmed cancellation can be edited and
            # submitted as a new approval attempt with new broker IDs.
            row.gtt_id = None
            row.id_fyers = None
            session.commit()
            after = risk.store.snapshot().get("state")
            reason_code = ("FUTURES_GTT_ALREADY_ABSENT"
                           if cancellation_result == "already_absent" else "USER_REJECTED")
            return _futures_response(
                reason_code=reason_code, http_status=200, row=row,
                success=True, decision="REJECTED_BY_USER",
                broker={"gtt_id": cancelled_gtt_id, "id_fyers": cancelled_fyers_id},
                approval_status=row.approval_status,
                is_trade_started=row.is_trade_started,
                state={"before": "ENTRY_GTT_RESTING", "after": after},
                extra={"status": "rejected", "cancellation": cancellation_result,
                       "retryable": True, "editable": True})
        if normalized_status != "approved":
            raise HTTPException(status_code=422, detail="FUT_REJECT_INVALID_STATUS")

        if row.approval_status == "approved" or row.gtt_id or row.id_fyers:
            return _futures_response(
                reason_code="ALREADY_APPROVED", http_status=200, row=row,
                success=True, decision="ALREADY_SUBMITTED",
                broker={"gtt_id": row.gtt_id, "id_fyers": row.id_fyers},
                approval_status=row.approval_status,
                is_trade_started=row.is_trade_started,
                extra={"status": "ALREADY_APPROVED"})
            if row.approval_status == "rejected" and not row.gtt_id and not row.id_fyers \
                    and int(row.is_trade_started or 0) == 0:
                # A rejected candidate with no broker identity is safe to
                # retry directly.  Preserve the prior rejection in audit and
                # use the current OMS prices for this new approval attempt.
                row.approval_status = "pending"
                row.approved_by = None
                row.approved_at = None
                row.order_status = "pending"
                session.flush()
            if row.approval_status != "pending":
                raise HTTPException(status_code=409, detail="FUT_REJECT_NOT_PENDING")
        if str(row.order_type).upper() != "BUY":
            raise HTTPException(status_code=409, detail="FUT_REJECT_INVALID_LONG_SIGNAL")
        if int(row.is_trade_started or 0) != 0:
            raise HTTPException(status_code=409, detail="FUT_REJECT_ORDER_ALREADY_STARTED")
        if not row.trade_signal_id or not row.stock_tick or row.stock_quantity is None:
            raise HTTPException(status_code=422, detail="FUT_REJECT_INCOMPLETE_CANDIDATE")
        try:
            requested_quantity = int(row.stock_quantity)
        except (TypeError, ValueError):
            raise HTTPException(status_code=422, detail="FUT_REJECT_INVALID_LOT_SIZE") from None
        if requested_quantity <= 0 or requested_quantity != row.stock_quantity:
            raise HTTPException(status_code=422, detail="FUT_REJECT_INVALID_LOT_SIZE")
        expiry = _parse_expiry(row.expiry_date)
        if expiry is None or expiry < datetime.now().date():
            raise HTTPException(status_code=409, detail="FUT_REJECT_INVALID_EXPIRY")

        risk = risk_factory()
        snapshot = risk.store.snapshot()
        if snapshot.get("active_winner") or snapshot.get("serial_reservation"):
            raise HTTPException(status_code=409, detail="FUT_REJECT_ACTIVE_FUTURES_WINNER")
        approved_contract = (snapshot.get("approved") or {}).get(row.stock_tick)
        if not isinstance(approved_contract, dict):
            approved_contract = _refresh_approved_contract(session, risk, row.stock_tick)
            if approved_contract is None:
                raise HTTPException(status_code=409, detail="FUT_REJECT_SYMBOL_UNRESOLVED")
        derived_contract = format_futures_contract(row.stock_tick, expiry)
        contract = approved_contract.get("contract")
        if contract != derived_contract:
            raise HTTPException(status_code=409, detail="FUT_REJECT_SYMBOL_UNRESOLVED")
        lot_size = approved_contract.get("lot_size")
        try:
            resolved_lot_size = int(lot_size)
        except (TypeError, ValueError):
            raise HTTPException(status_code=409, detail="FUT_REJECT_INVALID_LOT_SIZE_OR_CONTRACT") from None
        if not contract or lot_size is None or resolved_lot_size != requested_quantity:
            raise HTTPException(status_code=409, detail="FUT_REJECT_INVALID_LOT_SIZE_OR_CONTRACT")
        approved_expiry = _parse_expiry(approved_contract.get("expiry"))
        if approved_expiry != expiry:
            raise HTTPException(status_code=409, detail="FUT_REJECT_EXPIRY_MISMATCH")

        from scripts.futures_risk_engine import Signal
        signal_id = str(row.trade_signal_id)
        # The broker requires a valid price tick.  Reserve the same normalized
        # entry that is sent in the GTT so the final boundary cannot see a
        # reservation/payload mismatch (for example 120.831 vs 120.83).
        prices = normalize_cash_oco_prices(
            entry=row.entry_price,
            target=row.target_price,
            stoploss=row.stoploss_price,
            order_type="BUY",
        )
        signal = Signal(
            signal_id=signal_id,
            symbol=row.stock_tick,
            timeframe=int(row.time_frame),
            entry=prices["entry"],
            stop=row.stoploss_price,
            target=row.target_price,
            contract=contract,
            lot_size=requested_quantity,
            side="BUY",
        )
        reason = risk.reserve(signal, production=True, margin_ok=True)
        if reason != "FUT_ALLOW":
            raise HTTPException(status_code=409, detail=reason)

        from shared.db.fyers_req_model import GttLeg, GttOrderInfo, SingleGttOrderRequest

        payload = SingleGttOrderRequest(
            side=1,
            symbol=contract,
            productType="MARGIN",
            orderInfo=GttOrderInfo(leg1=GttLeg(
                price=float(prices["entry"]),
                triggerPrice=float(prices["entry"]),
                qty=requested_quantity,
            )),
            orderTag=str(row.bucket_id),
        ).model_dump()
        print("Payload:", payload)
        try:
            response = fyers_factory().place_futures_entry_gtt(
                payload, risk=risk, signal_id=signal_id
            )
            print("Response:", response)
        except Exception as exc:
            print("Exception:", exc)
            if hasattr(risk, "set_error_locked"):
                risk.set_error_locked("FUTURES_GTT_SUBMISSION_UNKNOWN: %s" % exc)
            raise HTTPException(status_code=502, detail="FYERS_GTT_STATUS_AMBIGUOUS") from None

        if not isinstance(response, dict) or response.get("s") != "ok":
            if isinstance(response, dict) and response.get("broker_verified_no_order") is True:
                risk.mark_submission_rejected(signal_id, broker_verified_no_order=True)
                raise HTTPException(status_code=502, detail="FYERS_GTT_REJECTED")
            if hasattr(risk, "set_error_locked"):
                risk.set_error_locked("FUTURES_GTT_REJECTED_REQUIRES_RECONCILIATION")
            raise HTTPException(status_code=502, detail="FYERS_GTT_STATUS_AMBIGUOUS")

        gtt_id = response.get("id")
        if not gtt_id:
            if hasattr(risk, "set_error_locked"):
                risk.set_error_locked("FUTURES_GTT_RESPONSE_MISSING_ID")
            raise HTTPException(status_code=502, detail="FUT_REJECT_MISSING_GTT_ID")
        risk.mark_gtt_resting(signal_id, gtt_id=gtt_id, broker_order_id=response.get("id_fyers"))
        row.approval_status = "approved"
        row.approved_by = _approver_id(approved_by)
        row.approved_at = now_factory()
        row.gtt_id = gtt_id
        row.id_fyers = response.get("id_fyers")
        row.order_status = "pending"
        row.is_trade_started = 0
        session.commit()
        return _futures_response(
            reason_code="FUT_ALLOW", http_status=200, row=row,
            risk=risk, success=True, decision="SUBMITTED",
            contract=contract,
            broker={"gtt_id": row.gtt_id, "id_fyers": row.id_fyers},
            approval_status=row.approval_status,
            is_trade_started=row.is_trade_started,
            extra={"status": "approved"})
    except HTTPException as exc:
        session.rollback()
        detail = exc.detail if isinstance(exc.detail, dict) else None
        if detail is None:
            code = str(exc.detail)
            detail = _futures_response(
                reason_code=code, http_status=exc.status_code, row=row, risk=risk,
                state={"after": _risk_state_after(risk)} if risk is not None else None,
                extra={**_retry_flags(code), **({"bucket_id": bucket_id} if row is None else {})})
        raise HTTPException(status_code=exc.status_code, detail=detail) from None
    except Exception as exc:
        session.rollback()
        logger.exception("Failed to approve futures bucket order")
        detail = _futures_response(
            reason_code="FUTURES_APPROVAL_PROCESSING_ERROR", http_status=502,
            row=row, risk=risk)
        raise HTTPException(status_code=502, detail=detail) from None
    finally:
        session.close()


def edit_rejected_futures_candidate(*, bucket_id, entry_price, stoploss_price,
                                    target_price, session_factory=None,
                                    risk_factory=_future_risk):
    """Reopen and edit only a rejected candidate with no broker identity."""
    session = session_factory() if session_factory else db.get_session()
    row = None
    try:
        row = session.query(OMSOrderBucketFuture).filter(
            OMSOrderBucketFuture.bucket_id == bucket_id
        ).first()
        if row is None:
            raise HTTPException(status_code=404, detail="FUTURES_BUCKET_NOT_FOUND")
        if row.approval_status != "rejected":
            raise HTTPException(status_code=409, detail="FUT_REJECT_NOT_REJECTED")
        if row.gtt_id or row.id_fyers or int(row.is_trade_started or 0) != 0:
            raise HTTPException(status_code=409, detail="FUT_REJECT_BROKER_LINKED_CANDIDATE")
        from scripts.futures_risk_engine import number
        try:
            entry = number(entry_price, True)
            stop = number(stoploss_price, True)
            target = number(target_price, True)
        except ValueError:
            raise HTTPException(status_code=422, detail="FUT_REJECT_INVALID_LONG_SIGNAL") from None
        if not stop < entry < target:
            raise HTTPException(status_code=409, detail="FUT_REJECT_INVALID_LONG_SIGNAL")

        changes = {"entry_price": entry_price, "stoploss_price": stoploss_price,
                   "target_price": target_price}
        row.entry_price = float(entry)
        row.stoploss_price = float(stop)
        row.target_price = float(target)
        row.approval_status = "pending"
        row.approved_by = None
        row.approved_at = None
        row.order_status = "pending"
        session.commit()
        try:
            risk = risk_factory()
            if hasattr(risk, "record_candidate_edit"):
                risk.record_candidate_edit(row.trade_signal_id, changes)
        except Exception:
            logger.exception("Could not write futures candidate edit audit")
        return _futures_response(
            reason_code="FUTURES_CANDIDATE_REOPENED", http_status=200, row=row,
            risk=locals().get("risk"), success=True, decision="REOPENED",
            approval_status=row.approval_status,
            is_trade_started=row.is_trade_started,
            extra={"status": "pending", "retryable": True, "editable": True})
    except HTTPException as exc:
        session.rollback()
        detail = _futures_response(
            reason_code=str(exc.detail), http_status=exc.status_code, row=row,
            extra={"retryable": True, "editable": True})
        raise HTTPException(status_code=exc.status_code, detail=detail) from None
    finally:
        session.close()


@router.put("/edit/bucket/orders/future", tags=["bucket_orders"])
async def edit_bucket_order_future(body: EditFutureCandidateRequest, request: Request):
    try:
        result = edit_rejected_futures_candidate(
            bucket_id=body.bucket_id, entry_price=body.entry_price,
            stoploss_price=body.stoploss_price, target_price=body.target_price,
        )
        return result
    except HTTPException as exc:
        if isinstance(exc.detail, dict):
            return JSONResponse(status_code=exc.status_code, content=exc.detail)
        raise


@router.post("/futures/recovery/reconcile", tags=["bucket_orders"])
async def recover_futures_error_locked(request: Request):
    """Run explicit read-only futures recovery; no caller-supplied evidence."""
    from scripts.futures_state_store import StateStore
    from scripts.futures_risk_engine import RiskEngine
    from scripts.futures_reconciliation import recover_error_locked_state

    session = db.get_session()
    try:
        risk = RiskEngine(StateStore(DBConnection().engine))
        result = recover_error_locked_state(
            risk=risk, broker=_future_fyers(), session=session,
        )
        response = APIResponse(request)
        response.setMsg("success")
        response.setResponse(result)
        return response
    except Exception as exc:
        session.rollback()
        logger.exception("Failed to recover futures ERROR_LOCKED state")
        raise HTTPException(status_code=502, detail="Futures recovery could not be completed") from None
    finally:
        session.close()


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


@router.post("/get/bucket/order/count/future", tags=["bucket_orders"])
async def get_bucket_orders_count_future(req_model: GetOrder, request: Request):
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
            total_count = db.raw_query(text(f"""select count(*) as count from oms_order_bucket_future 
                                where country_id = {req_model.country_id} 
                                and date(purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                and time_frame like '{req_model.time_frame}' 
                                and order_type in {order_types}
                                {prediction_type};"""))
        elif req_model.status != 'pending' and req_model.status != 'progress':
            print('2')
            total_count = db.raw_query(text(f"""select count(*) as count from oms_order_bucket_future 
                                where country_id = {req_model.country_id} 
                                and date(entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                and order_status like '%{req_model.status}%'
                                and time_frame like '{req_model.time_frame}' 
                                and order_type in {order_types}
                                {prediction_type};"""))
        elif req_model.status == 'pending':
            print('3')
            total_count = db.raw_query(text(f"""select count(*) as count from oms_order_bucket_future 
                                where country_id = {req_model.country_id} 
                                and date(purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                and order_status is null
                                and time_frame like '{req_model.time_frame}'
                                and order_type in {order_types}
                                {prediction_type};"""))
        elif req_model.status == 'progress':
            print('4')
            total_count = db.raw_query(text(f"""select count(*) as count from oms_order_bucket_future 
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


@router.post("/get/bucket/orders/future", tags=["bucket_orders"])
async def get_bucket_orders_future(req_model: GetOrder, request: Request):
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
            order_by = "purchased_cmp_date desc"
        else:
            order_by = "stock_tick asc"
        
        if req_model.order_type.lower() == "all":
            order_types = ("buy", "sell", "Buy", "Sell", "BUY", "SELL")
        else:
            order_types = (req_model.order_type.lower(), req_model.order_type.upper(), req_model.order_type.capitalize())

        if req_model.prediction_type == None or req_model.prediction_type == 'null' or req_model.prediction_type == '':
            prediction_type = " "
        else:
            prediction_type = f"and arima_ab_model_prediction like '%{req_model.prediction_type}%'"
        
        req_model.status = req_model.status.strip().lower()

        res = []


        if (req_model.trade_id != None and req_model.trade_id != 'null' and req_model.trade_id != ''):
            if req_model.status == 'all':
                print('1')
                total_res = db.raw_query(text(f"""select * from oms_order_bucket_future 
                                    where country_id = {req_model.country_id} 
                                    and date(purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                    and time_frame like '{req_model.time_frame}' 
                                    and order_type in {order_types}
                                    {prediction_type}
                                    order by {order_by};"""))
            elif req_model.status != 'pending' and req_model.status != 'progress':
                print('2')
                total_res = db.raw_query(text(f"""select * from oms_order_bucket_future 
                                    where country_id = {req_model.country_id} 
                                    and date(entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                    and order_status like '%{req_model.status}%'
                                    and time_frame like '{req_model.time_frame}' 
                                    and order_type in {order_types}
                                    {prediction_type}
                                    order by {order_by};"""))
            elif req_model.status == 'pending':
                print('3')
                total_res = db.raw_query(text(f"""select * from oms_order_bucket_future 
                                    where country_id = {req_model.country_id} 
                                    and date(purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                    and order_status is null
                                    and time_frame like '{req_model.time_frame}'
                                    and order_type in {order_types}
                                    {prediction_type}
                                    order by {order_by};"""))
            elif req_model.status == 'progress':
                print('4')
                total_res = db.raw_query(text(f"""select * from oms_order_bucket_future 
                                    where country_id = {req_model.country_id} 
                                    and date(entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                    and order_status like '%pending%'
                                    and time_frame like '{req_model.time_frame}'
                                    and order_type in {order_types}
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
                res = db.raw_query(text(f"""select * from oms_order_bucket_future 
                                                where country_id = {req_model.country_id} 
                                                and date(purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and time_frame like '{req_model.time_frame}'
                                                and order_type in {order_types} 
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
                print(f"""select * from oms_order_bucket_future 
                                                where country_id = {req_model.country_id} 
                                                and date(purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and time_frame like '{req_model.time_frame}'
                                                and order_type in {order_types} 
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};""")
            elif req_model.status != 'pending' and req_model.status != 'progress':
                print('6')
                res = db.raw_query(text(f"""select * from oms_order_bucket_future 
                                                where country_id = {req_model.country_id} 
                                                and date(entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and order_status like '%{req_model.status}%'
                                                and time_frame like '{req_model.time_frame}'
                                                and order_type in {order_types} 
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
            elif req_model.status == 'pending':
                print('7')
                res = db.raw_query(text(f"""select * from oms_order_bucket_future 
                                                where country_id = {req_model.country_id} 
                                                and date(purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and order_status is null
                                                and time_frame like '{req_model.time_frame}'
                                                and order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
            elif req_model.status == 'progress':
                print('8')
                res = db.raw_query(text(f"""select * from oms_order_bucket_future 
                                                where country_id = {req_model.country_id} 
                                                and date(entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and order_status like '%pending%'
                                                and time_frame like '{req_model.time_frame}'
                                                and order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
                   
        elif (req_model.stock_tick == None or req_model.stock_tick == 'null' or req_model.stock_tick == '') and (req_model.page_no != None and req_model.page_no != 'null' and req_model.page_no != ''):
            offset = (req_model.page_no - 1) * req_model.limit
            if req_model.status == 'all':
                print('9')
                res = db.raw_query(text(f"""select * from oms_order_bucket_future 
                                                where country_id = {req_model.country_id} 
                                                and date(purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and time_frame like '{req_model.time_frame}'
                                                and order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
                print(text(f"""select * from oms_order_bucket_future 
                                                where country_id = {req_model.country_id} 
                                                and date(purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and time_frame like '{req_model.time_frame}'
                                                and order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
            elif req_model.status != 'pending' and req_model.status != 'progress':
                print('10')
                res = db.raw_query(text(f"""select * from oms_order_bucket_future 
                                                where country_id = {req_model.country_id} 
                                                and date(entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and order_status like '%{req_model.status}%' 
                                                and time_frame like '{req_model.time_frame}'
                                                and order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
            elif req_model.status == 'pending':
                print('11')
                res = db.raw_query(text(f"""select * from oms_order_bucket_future 
                                                where country_id = {req_model.country_id} 
                                                and date(purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and order_status is null 
                                                and time_frame like '{req_model.time_frame}'
                                                and order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
            elif req_model.status == 'progress':
                print('12')
                res = db.raw_query(text(f"""select * from oms_order_bucket_future 
                                                where country_id = {req_model.country_id} 
                                                and date(entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and order_status like '%pending%'
                                                and time_frame like '{req_model.time_frame}' 
                                                and order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
        
        elif (req_model.stock_tick != None and req_model.stock_tick != 'null' and req_model.stock_tick != '') and (req_model.page_no == None or req_model.page_no == 'null' or req_model.page_no == ''):            
            if req_model.status == 'all':
                print('13')
                total_res = db.raw_query(text(f"""select * from oms_order_bucket_future 
                                    where country_id = {req_model.country_id} 
                                    and date(purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                    and time_frame like '{req_model.time_frame}' 
                                    and order_type in {order_types}
                                    {prediction_type}
                                    order by {order_by};"""))
            elif req_model.status != 'pending' and req_model.status != 'progress':
                print('14')
                total_res = db.raw_query(text(f"""select * from oms_order_bucket_future 
                                    where country_id = {req_model.country_id} 
                                    and date(entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                    and order_status like '%{req_model.status}%'
                                    and time_frame like '{req_model.time_frame}' 
                                    and order_type in {order_types}
                                    {prediction_type}
                                    order by {order_by};"""))
            elif req_model.status == 'pending':
                print('15')
                total_res = db.raw_query(text(f"""select * from oms_order_bucket_future 
                                    where country_id = {req_model.country_id} 
                                    and date(purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                    and order_status is null
                                    and time_frame like '{req_model.time_frame}'
                                    and order_type in {order_types}
                                    {prediction_type}
                                    order by {order_by};"""))
            elif req_model.status == 'progress':
                print('16')
                total_res = db.raw_query(text(f"""select * from oms_order_bucket_future 
                                    where country_id = {req_model.country_id} 
                                    and date(entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                    and order_status like '%pending%'
                                    and time_frame like '{req_model.time_frame}'
                                    and order_type in {order_types}
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
                res = db.raw_query(text(f"""select * from oms_order_bucket_future 
                                                where country_id = {req_model.country_id} 
                                                and date(purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and time_frame like '{req_model.time_frame}'
                                                and order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
            elif req_model.status != 'pending' and req_model.status != 'progress':
                print('18')
                res = db.raw_query(text(f"""select * from oms_order_bucket_future 
                                                where country_id = {req_model.country_id} 
                                                and date(entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and order_status like '%{req_model.status}%' 
                                                and time_frame like '{req_model.time_frame}'
                                                and order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
            elif req_model.status == 'pending':
                print('19')
                res = db.raw_query(text(f"""select * from oms_order_bucket_future 
                                                where country_id = {req_model.country_id} 
                                                and date(purchased_cmp_date) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and order_status is null 
                                                and time_frame like '{req_model.time_frame}'
                                                and order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
            elif req_model.status == 'progress':
                print('20')
                res = db.raw_query(text(f"""select * from oms_order_bucket_future 
                                                where country_id = {req_model.country_id} 
                                                and date(entry_timestamp) between '{req_model.start_date}' and '{req_model.end_date}' 
                                                and order_status like '%pending%' 
                                                and time_frame like '{req_model.time_frame}'
                                                and order_type in {order_types}
                                                {prediction_type}
                                                order by {order_by}
                                                limit {req_model.limit} offset {offset};"""))
         
        
        apiresponce.setMsg("success")
        apiresponce.setResponse({"orders": res, "page_no": req_model.page_no})
    except Exception as ex:
        logger.exception(ex)
        logger.error(ex)
        apiresponce.setMsg("failed")
        apiresponce.setResponse(str(ex))
    return apiresponce


@router.get("/bucket/orders/future/{order_id}", tags=["bucket_orders"])
async def get_bucket_order_future_by_id(order_id: int, request: Request):
    apiresponce = APIResponse(request)
    try:
        session = db.get_session()
        res = session.query(OMSOrderBucketFuture).filter(OMSOrderBucketFuture.order_id == order_id).first()
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


@router.post("/create/bucket/orders/future", tags=["bucket_orders"])
async def create_bucket_order_future(body: BucketOrderBase, request: Request):
    apiresponce = APIResponse(request)
    try:
        session = db.get_session()
        if body.trade_id != None:
            bucket_order = session.query(OMSOrderBucketFuture).filter(OMSOrderBucketFuture.trade_signal_id == body.trade_id).all()
            if bucket_order and len(bucket_order) > 0:
                apiresponce.setMsg("failed")
                apiresponce.setResponse("Bucket order already exists for the given trade")
                session.close()
                return apiresponce
        order_id = None
        if body.trade_id != None:
            order = session.query(Future_Order).filter(Future_Order.trade_signal_id == body.trade_id).all()
            if order and len(order) > 0 and order[0].order_id != None:
                order_id = order[0].order_id
            else:
                order_id = body.order_id
        oms_order_bucket = OMSOrderBucketFuture(
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
            order_status = "pending",
            expiry_date = body.expiry_date)
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


@router.post("/approve/reject/bucket/orders/future", tags=["bucket_orders"])
async def approve_reject_bucket_orders_future(body: ApproveReject_BucketOrderBase, request: Request):
    try:
        logger.info("ApproveReject_BucketOrderBase: %s", body.model_dump())
        result = approve_existing_futures_candidate(
            bucket_id=body.bucket_id,
            status=body.status,
            approved_by=body.approved_by,
        )
        return result
    except HTTPException as ex:
        # Keep the structured contract at the top level for Angular/CMS,
        # rather than forcing clients to unpack FastAPI's ``detail`` field.
        if isinstance(ex.detail, dict):
            return JSONResponse(status_code=ex.status_code, content=ex.detail)
        raise
    except Exception as ex:
        logger.exception("Failed to update bucket order")
        raise HTTPException(status_code=502, detail=f"Error updating bucket order: {ex}")
    return apiresponce
