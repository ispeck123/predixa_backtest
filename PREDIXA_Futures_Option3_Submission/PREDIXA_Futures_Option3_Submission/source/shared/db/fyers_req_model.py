from enum import IntEnum, Enum
from typing import Optional, List, Any, Dict
from pydantic import BaseModel, Field, field_validator, model_validator, validator
from pydantic_settings import BaseSettings
from fastapi import Depends, HTTPException
from data_fetchers.fyers.fyers_session import fyers_access_token_handler
import re, logging
try:
    from fyers_apiv3 import fyersModel
except Exception as e:  # pragma: no cover
    fyersModel = None


logger = logging.getLogger("fyers_orders")
logging.basicConfig(level=logging.INFO)

# class Settings(BaseSettings):
#     fyers_client_id: str = Field(alias="FYERS_CLIENT_ID")
#     fyers_access_token: str = Field(alias="FYERS_ACCESS_TOKEN")
#     fyers_is_async: bool = Field(default=False, alias="FYERS_IS_ASYNC")
#     fyers_log_path: str = Field(default="", alias="FYERS_LOG_PATH")

#     class Config:
#         extra = "ignore"


# def get_settings() -> Settings:
    # return Settings()  # reads from env


class FyersClient:
    """Fyers wrapper with explicit new-entry/protective action separation.

    Legacy callers that invoke ``place_order`` are treated as new entries and
    must pass the persistent global ceiling.  Protective orders use the explicit
    protective methods below and remain available during a halt.
    """

    def __init__(self):
        if fyersModel is None:  # pragma: no cover
            raise RuntimeError("fyers_apiv3 not installed. pip install fyers-apiv3")
        full_access_token = fyers_access_token_handler().get_access_token()
        self._client = fyersModel.FyersModel(
            client_id=full_access_token.rsplit(":", 1)[0],
            token=full_access_token.rsplit(":", 1)[1],
            is_async=False,
            log_path="./logs",
        )

    @staticmethod
    def _is_futures_symbol(payload: dict) -> bool:
        symbol = str(payload.get("symbol", ""))
        return symbol.startswith(("NSE:", "NFO:")) and symbol.endswith("FUT")

    def _assert_cash_entry_allowed(self):
        from scripts.futures_state_store import StateStore
        from scripts.futures_risk_engine import RiskEngine
        from shared.db.dbconn import DBConnection

        store = StateStore(DBConnection().engine)
        risk = RiskEngine(store)
        if risk.cash_entry_allowed() != "CASH_ALLOW":
            raise RuntimeError("GLOBAL_REALIZED_LOSS_CEILING")

    def place_order(self, payload: dict) -> dict:
        """Submit a new cash order only after the durable global check.

        Futures orders must use ``place_futures_entry_gtt`` through the
        Option-3 adapter.  This prevents generic/manual API routes from creating
        a futures position outside the serial reservation.
        """
        if self._is_futures_symbol(payload):
            raise RuntimeError("FUTURES_ENTRY_REQUIRES_OPTION3_SERIAL_GATE")
        self._assert_cash_entry_allowed()
        return self._client.place_order(data=payload)

    def place_cash_order(self, payload: dict) -> dict:
        return self.place_order(payload)

    def place_protective_order(self, payload: dict) -> dict:
        return self._client.place_order(data=payload)

    def place_cash_gtt(self, payload: dict) -> dict:
        if self._is_futures_symbol(payload):
            raise RuntimeError("FUTURES_ENTRY_REQUIRES_OPTION3_SERIAL_GATE")
        self._assert_cash_entry_allowed()
        return self._client.place_gtt_order(data=payload)

    def place_protective_gtt(self, payload: dict) -> dict:
        return self._client.place_gtt_order(data=payload)

    def place_futures_entry_gtt(self, payload: dict, *, risk, signal_id=None) -> dict:
        """The only Fyers futures-entry primitive.

        The durable store must already hold a matching pending reservation.
        ``serial_reservation`` is the post-trigger winner and is deliberately
        not required before an entry is submitted.
        """
        if self._is_futures_symbol(payload) is False:
            raise ValueError("Futures GTT requires a futures symbol")
        snapshot = risk.store.snapshot()
        reservations = snapshot.get("reservations") or {}
        if snapshot.get("active_winner") or snapshot.get("serial_reservation"):
            raise RuntimeError("FUT_REJECT_ACTIVE_FUTURES_WINNER")
        order_info = payload.get("orderInfo")
        leg1 = order_info.get("leg1") if isinstance(order_info, dict) else None

        if snapshot.get("state") not in ("ENTRY_RESERVING", "ENTRY_GTT_RESTING"):
            raise RuntimeError("FUTURES_PENDING_RESERVATION_REQUIRED")
        if signal_id is not None:
            reservation = reservations.get(signal_id)
            reservation_id = signal_id
        else:
            matches = [
                (rid, item) for rid, item in reservations.items()
                if item.get("contract") == payload.get("symbol")
            ]
            reservation_id, reservation = matches[0] if len(matches) == 1 else (None, None)
        if not reservation_id or reservation is None or reservation.get("status", "PENDING") != "PENDING":
            raise RuntimeError("FUTURES_PENDING_RESERVATION_REQUIRED")
        if payload.get("symbol") != reservation.get("contract"):
            raise RuntimeError("FUTURES_SERIAL_RESERVATION_PAYLOAD_MISMATCH")
        if payload.get("side") != 1:
            raise RuntimeError("FUTURES_SERIAL_RESERVATION_PAYLOAD_MISMATCH")
        if not isinstance(leg1, dict):
            raise RuntimeError("FUTURES_SERIAL_RESERVATION_PAYLOAD_MISMATCH")

        reserved_qty = reservation.get("quantity", reservation.get("requested_quantity"))
        try:
            if int(leg1.get("qty")) != int(reserved_qty):
                raise RuntimeError("FUTURES_SERIAL_RESERVATION_PAYLOAD_MISMATCH")
        except (TypeError, ValueError):
            raise RuntimeError("FUTURES_SERIAL_RESERVATION_PAYLOAD_MISMATCH") from None
        return self._client.place_gtt_order(data=payload)

    def cancel_gtt_order(self, payload: dict) -> dict:
        return self._client.cancel_gtt_order(data=payload)

    def read_gtt_orderbook(self) -> dict:
        """Read-only GTT snapshot used by startup reconciliation."""
        return self._client.gtt_orderbook()

    def read_positions(self) -> dict:
        """Read-only positions snapshot used by startup reconciliation."""
        return self._client.positions()


def get_fyers() -> FyersClient:
    return FyersClient()



class OrderType(IntEnum):
    LIMIT = 1
    MARKET = 2
    STOP = 3  # SL-M
    STOPLIMIT = 4  # SL-L


class Side(IntEnum):
    BUY = 1
    SELL = -1


class ProductType(str, Enum):
    CNC = "CNC"  # equity only
    INTRADAY = "INTRADAY"
    MARGIN = "MARGIN"  # derivatives only
    CO = "CO"
    BO = "BO"
    MTF = "MTF"  # approved symbols only


class Validity(str, Enum):
    IOC = "IOC"
    DAY = "DAY"


SYMBOL_REGEX = re.compile(r"^(NSE|BSE|NFO|MCX|CDS):[A-Z0-9_-]+(?:-[A-Z]+)?$")




class PlaceOrderRequest(BaseModel):
    symbol: str = Field(..., description="Exchange:Symbol, e.g., NSE:SBIN-EQ")
    qty: int = Field(..., gt=0, description="Quantity. For derivatives, must be lot multiple (validated upstream).")
    type: OrderType = Field(..., description="1=Limit, 2=Market, 3=SL-M, 4=SL-L")
    side: Side = Field(..., description="1=Buy, -1=Sell")
    productType: ProductType

    limitPrice: float = Field(0, ge=0, description="Required for LIMIT/SL-L. Else keep 0.")
    stopPrice: float = Field(0, ge=0, description="Required for SL-M/SL-L. Else keep 0.")

    disclosedQty: int = Field(0, ge=0, description="Equity only. 0 for none.")
    validity: Validity = Field(Validity.DAY)
    offlineOrder: bool = Field(False, description="True for AMO")

    stopLoss: float = Field(0, ge=0, description="Required for CO/BO")
    takeProfit: float = Field(0, ge=0, description="Required for BO")

    orderTag: Optional[str] = Field(None, max_length=30)

    # ---------- Validators ----------
    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        if not SYMBOL_REGEX.match(v):
            raise ValueError(
                "Invalid symbol format. Expected <EXCHANGE>:<SYMBOL>, e.g., NSE:SBIN-EQ"
            )
        return v

    @model_validator(mode="after")
    def validate_conditional_fields(self):
        # Limit/StopLimit require a limit price
        if self.type in {OrderType.LIMIT, OrderType.STOPLIMIT} and self.limitPrice <= 0:
            raise ValueError("limitPrice must be > 0 for LIMIT and STOPLIMIT orders")

        # Stop/StopLimit require a stop price
        if self.type in {OrderType.STOP, OrderType.STOPLIMIT} and self.stopPrice <= 0:
            raise ValueError("stopPrice must be > 0 for STOP (SL-M) and STOPLIMIT (SL-L) orders")

        # CO/BO require stopLoss; BO requires takeProfit as well
        if self.productType in {ProductType.CO, ProductType.BO}:
            if self.stopLoss <= 0:
                raise ValueError("stopLoss must be > 0 for CO/BO orders")
            if self.productType is ProductType.BO and self.takeProfit <= 0:
                raise ValueError("takeProfit must be > 0 for BO orders")

        # disclosedQty allowed for equity only (symbol ends with -EQ in NSE/BSE)
        if self.disclosedQty:
            if not (self.symbol.startswith(("NSE:", "BSE:")) and self.symbol.endswith("-EQ")):
                raise ValueError("disclosedQty is allowed only for equity symbols (e.g., NSE:SBIN-EQ)")
            if self.disclosedQty >= self.qty:
                raise ValueError("disclosedQty must be less than qty")

        # CNC only for equity; MARGIN for derivatives only. We do a basic heuristic based on suffix.
        is_equity = self.symbol.endswith("-EQ") and self.symbol.startswith(("NSE:", "BSE:"))
        if self.productType is ProductType.CNC and not is_equity:
            raise ValueError("CNC is allowed only for equity symbols")
        if self.productType is ProductType.MARGIN and is_equity:
            raise ValueError("MARGIN is applicable only for derivatives")

        return self

    def to_fyers_payload(self) -> dict:
        # Map our model to FYERS payload naming exactly as per API
        payload = {
            "symbol": self.symbol,
            "qty": self.qty,
            "type": int(self.type),
            "side": int(self.side),
            "productType": self.productType.value,
            "limitPrice": float(self.limitPrice or 0),
            "stopPrice": float(self.stopPrice or 0),
            "validity": self.validity.value,
            "disclosedQty": int(self.disclosedQty or 0),
            "offlineOrder": bool(self.offlineOrder),
            "stopLoss": float(self.stopLoss or 0),
            "takeProfit": float(self.takeProfit or 0),
        }
        if self.orderTag is not None:
            payload["orderTag"] = self.orderTag
        return payload


class PlaceOrderResponse(BaseModel):
    s: str
    code: int
    message: str
    id: Optional[str] = None


class BracketOrderRequest(BaseModel):
    symbol: str
    qty: int
    entry_price: float
    stoploss_price: float
    target_price: float


class BracketOrderResponse(BaseModel):
    status: str
    entry_id: Optional[str]
    stoploss_id: Optional[str]
    target_id: Optional[str]
    message: str


class CombinedOrderResponse(BaseModel):
    entry: PlaceOrderResponse
    gtt: PlaceOrderResponse

class ExitPositionsRequest(BaseModel):
    id: List[str] = Field(
        ...,
        description='List of FYERS position ids, for example: ["NSE:SBIN-EQ-BO"]',
        min_length=1,
    )


class ExitPositionItemResponse(BaseModel):
    id: str
    success: bool
    response: Dict[str, Any]


class ExitPositionsResponse(BaseModel):
    success: bool
    results: List[ExitPositionItemResponse]


class ModifyGttRequest(BaseModel):
    id: str
    entry: Optional[float] = None
    sl: Optional[float] = None
    tp: Optional[float] = None
    qty: Optional[int] = None


class GttLeg(BaseModel):
    price: float
    triggerPrice: float
    qty: int


class GttOrderInfo(BaseModel):
    leg1: GttLeg


class SingleGttOrderRequest(BaseModel):
    side: int = Field(..., description="1 for BUY, -1 for SELL")
    symbol: str = Field(..., example="NSE:SBIN-EQ")
    productType: str = Field(..., example="CNC")
    orderInfo: GttOrderInfo
    orderTag: Optional[str] = None

class CreateGTTOCORequest(BaseModel):
    symbol: str
    side: int
    entry: float
    target: float
    stoploss: float
    qty: int = Field(1, gt=0, description="Quantity for both legs")
    productType: str = Field("CNC", description="CNC / MARGIN / MTF")
    orderTag: Optional[str] = None

    @validator("target", "stoploss", "entry", pre=True)
    def ensure_float(cls, v):
        return float(v)


class UpdateLiveOrderRequest(BaseModel):
    """
    Update a live order in the database.
    """
    orderId: str
    bucket_id: Optional[int] = None
    status: str
    marketSection: str
    originalEntry: float
    originalTarget: float
    originalStoploss: float
    entry: float
    target: float
    stoploss: float
    entryChanged: bool
    targetChanged: bool
    stoplossChanged: bool
    hasChanges: Optional[bool] = None


def build_gtt_oco_payload(req: CreateGTTOCORequest, ltp: float) -> dict:
    """
    Build FYERS GTT-OCO payload.

    - leg1: target (trigger > LTP)
    - leg2: stoploss (trigger < LTP)
    - Assumes we are exiting an existing position:
        * Long position => side = -1 (sell)
    """
    gtt_side = -req.side
    # Basic validation based on your description & Fyers constraints
    if not (req.stoploss < ltp < req.target):
        raise HTTPException(
            status_code=422,
            detail={
                "message": (
                    "Invalid GTT OCO configuration: expected stoploss < LTP < target "
                    f"(stoploss={req.stoploss}, ltp={ltp}, target={req.target})."
                )
            },
        )

    # You can extend this later for short positions if needed
    # exit long (sell)

    payload = {
        # id is omitted for new GTT creation (used only for modify)
        "symbol": req.symbol,
        "side": gtt_side,
        "productType": req.productType,  # CNC / MARGIN / MTF

        "orderInfo": {
            "leg1": {  # TARGET LEG
                "price": req.target,          # limit price when triggered
                "triggerPrice": req.target,   # trigger > LTP
                "qty": req.qty,
            },
            "leg2": {  # STOPLOSS LEG
                "price": req.stoploss,        # limit price when triggered
                "triggerPrice": req.stoploss, # trigger < LTP
                "qty": req.qty,
            },
        },
    }
    return payload


def get_ltp_for_symbol(fyers: FyersClient, symbol: str) -> float:
    """
    Fetch LTP for the given symbol using your FyersClient.

    Implement this based on how your FyersClient exposes quotes.
    """
    # Example shape – adapt to your actual client:
    quote_resp = fyers._client.quotes({"symbols": symbol})
    if not isinstance(quote_resp, dict):
        raise HTTPException(status_code=502, detail="Unexpected quote response from FYERS")

    try:
        # Adapt this according to the actual response structure
        print("#########################################")
        print(quote_resp)
        print("#########################################")
        ltp = quote_resp["d"][0]["v"]["lp"]
    except Exception as ex:
        logger.exception("Failed to fetch LTP for symbol %s", symbol, exc_info=True)
        raise HTTPException(status_code=502, detail="Could not extract LTP from FYERS quote response, Possible invalid symbol")

    return float(ltp)


def classify_gtt_orders(holdings_response: dict, gtt_response: dict):
    """
    Classify only true pending GTT orders into:
    - against_holding
    - fresh_entry
    - add_on_to_holding
    - naked_sell_or_unknown
    - unknown

    FYERS order status codes:
        1 = Cancelled
        2 = Traded / Filled
        3 = Future use
        4 = Transit
        5 = Rejected
        6 = Pending

    Only orders with ord_status == 6 are returned.

    Returns:
        tuple:
            classified_orders: list of pending classified GTT orders
            status_summary: count of orders grouped by status
    """

    holdings_list = holdings_response.get("holdings", [])
    gtt_list = gtt_response.get("orderBook", [])

    # FYERS order-status mapping
    order_status_map = {
        1: "cancelled",
        2: "traded_or_filled",
        3: "future_use",
        4: "transit",
        5: "rejected",
        6: "pending"
    }

    print("#####################################")
    print("Total GTT records received:", len(gtt_list))
    print(gtt_list)
    print("#####################################")

    # Build holdings lookup by symbol
    holdings_map = {}

    for holding_record in holdings_list:
        symbol = holding_record.get("symbol")

        if not symbol:
            continue

        holdings_map[symbol] = {
            "quantity": holding_record.get("quantity", 0),
            "remainingQuantity": holding_record.get("remainingQuantity", 0),
            "costPrice": holding_record.get("costPrice", 0),
            "ltp": holding_record.get("ltp", 0),
            "marketVal": holding_record.get("marketVal", 0),
            "pl": holding_record.get("pl", 0),
            "holdingType": holding_record.get("holdingType", "")
        }

    classified_orders = []

    status_summary = {
        "total_received": len(gtt_list),
        "pending": 0,
        "cancelled": 0,
        "traded_or_filled": 0,
        "future_use": 0,
        "transit": 0,
        "rejected": 0,
        "unknown": 0,
        "returned_pending_orders": 0
    }

    for order in gtt_list:
        raw_ord_status = order.get("ord_status")

        # Convert string status such as "6" to integer when possible
        try:
            ord_status = int(raw_ord_status)
        except (TypeError, ValueError):
            ord_status = None

        order_status = order_status_map.get(ord_status, "unknown")

        # Update status count
        if order_status in status_summary:
            status_summary[order_status] += 1
        else:
            status_summary["unknown"] += 1

        # Only true pending orders should be classified and returned
        if ord_status != 6:
            continue

        # Additional defensive check:
        # Skip records explicitly marked as cancelled, triggered, filled,
        # traded, or rejected even when ord_status is unexpectedly 6.
        report_type = str(order.get("report_type", "")).strip().upper()

        non_pending_report_types = {
            "CANCELLED",
            "TRIGGERED",
            "TRADED",
            "FILLED",
            "REJECTED"
        }

        if report_type in non_pending_report_types:
            print(
                f"Skipping inconsistent GTT record: "
                f"symbol={order.get('symbol')}, "
                f"ord_status={ord_status}, "
                f"report_type={report_type}"
            )
            continue

        symbol = order.get("symbol")
        tran_side = order.get("tran_side")  # 1 = buy, -1 = sell

        try:
            tran_side = int(tran_side)
        except (TypeError, ValueError):
            tran_side = None

        try:
            qty = float(order.get("qty", 0) or 0)
        except (TypeError, ValueError):
            qty = 0

        holding = holdings_map.get(symbol)

        try:
            remaining_quantity = float(
                holding.get("remainingQuantity", 0) or 0
            ) if holding else 0
        except (TypeError, ValueError):
            remaining_quantity = 0

        has_holding = holding is not None and remaining_quantity > 0

        classification = None
        reason = None

        if has_holding:
            held_qty = remaining_quantity

            if tran_side == -1:
                classification = "against_holding"

                if qty <= held_qty:
                    reason = (
                        f"Pending sell-side GTT for existing holding. "
                        f"Order qty {qty:g} <= held qty {held_qty:g}."
                    )
                else:
                    reason = (
                        f"Pending sell-side GTT for existing holding, "
                        f"but order qty {qty:g} > held qty {held_qty:g}."
                    )

            elif tran_side == 1:
                classification = "add_on_to_holding"
                reason = (
                    "Pending buy-side GTT on a symbol already held. "
                    "Likely an averaging or add-on position."
                )

            else:
                classification = "unknown"
                reason = (
                    "Holding exists, but the GTT transaction side "
                    "is not recognized."
                )

        else:
            if tran_side == 1:
                classification = "fresh_entry"
                reason = (
                    "Pending buy-side GTT on a symbol not present "
                    "in current holdings."
                )

            elif tran_side == -1:
                classification = "naked_sell_or_unknown"
                reason = (
                    "Pending sell-side GTT on a symbol not present "
                    "in current holdings."
                )

            else:
                classification = "unknown"
                reason = (
                    "No holding found and the GTT transaction side "
                    "is not recognized."
                )

        classified_orders.append({
            "symbol": symbol,
            "symbol_desc": order.get("symbol_desc"),
            "symbol_exch": order.get("symbol_exch"),

            "gtt_id": order.get("id"),
            "id_fyers": order.get("id_fyers"),

            "product_type": order.get("product_type"),
            "qty": order.get("qty", 0),
            "qty2": order.get("qty2", 0),
            "tran_side": tran_side,
            "gtt_oco_ind": order.get("gtt_oco_ind"),

            "price_trigger": order.get("price_trigger"),
            "price_limit": order.get("price_limit"),
            "price2_trigger": order.get("price2_trigger"),
            "price2_limit": order.get("price2_limit"),

            "ord_status": ord_status,
            "order_status": order_status,
            "is_pending": True,

            "report_type": order.get("report_type"),
            "oms_msg": order.get("oms_msg"),

            "create_time": order.get("create_time"),
            "create_time_epoch": order.get("create_time_epoch"),

            "classification": classification,
            "reason": reason,
            "holding_details": holding if holding else None
        })

    status_summary["returned_pending_orders"] = len(classified_orders)

    print("GTT status summary:", status_summary)
    print("True pending GTT orders:", len(classified_orders))

    return classified_orders


def build_modify_gtt_payload(order: dict, entry: float = None, sl: float = None, tp: float = None, qty: int = None):
    """
    Build payload for fyers.modify_gtt_order based on gtt type.

    gtt_oco_ind:
        1 = single
        2 = oco
    """

    gtt_type = order.get("gtt_oco_ind")
    if qty is None:
        qty = order.get("qty", 0)
    else:
        qty = int(qty)

    if gtt_type == 1:
        # Single GTT -> modify only entry
        if entry is None:
            raise ValueError("For single GTT, 'entry' is required.")

        return {
            "id": order["id"],
            "orderInfo": {
                "leg1": {
                    "price": entry,
                    "triggerPrice": entry,
                    "qty": qty
                }
            }
        }

    elif gtt_type == 2:
        # OCO GTT -> modify SL and TP only
        if sl is None or tp is None:
            raise ValueError("For OCO GTT, both 'sl' and 'tp' are required.")

        # Existing order book has:
        # price_limit / price_trigger     -> one leg
        # price2_limit / price2_trigger   -> second leg
        #
        # We must preserve which leg is upper and which is lower.
        existing_leg1 = float(order.get("price_trigger", 0))
        existing_leg2 = float(order.get("price2_trigger", 0))

        # Usually for sell OCO:
        # higher price = TP
        # lower price = SL
        #
        # We preserve same leg structure as existing order.
        if existing_leg1 >= existing_leg2:
            leg1_price = tp
            leg2_price = sl
        else:
            leg1_price = sl
            leg2_price = tp

        return {
            "id": order["id"],
            "orderInfo": {
                "leg1": {
                    "price": leg1_price,
                    "triggerPrice": leg1_price,
                    "qty": qty
                },
                "leg2": {
                    "price": leg2_price,
                    "triggerPrice": leg2_price,
                    "qty": qty
                }
            }
        }

    else:
        raise ValueError(f"Unsupported gtt_oco_ind: {gtt_type}")
