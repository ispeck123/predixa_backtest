"""Stable, presentation-only responses for futures approval decisions."""
from datetime import date, datetime
from decimal import Decimal
from typing import Any
import logging

from pydantic import BaseModel, ConfigDict

logger = logging.getLogger("fyers_orders")


class FuturesApprovalResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    success: bool
    decision: str
    reason_code: str
    message: str
    http_status: int
    bucket_id: int | None = None
    trade_signal_id: int | str | None = None
    symbol: str | None = None
    contract: str | None = None
    timeframe: int | None = None
    order: dict[str, Any] | None = None
    calculation: dict[str, Any] | None = None
    risk_state: dict[str, Any] | None = None
    state: dict[str, Any] | None = None
    broker: dict[str, Any] | None = None
    checks: dict[str, Any] | None = None
    approval_status: str | None = None
    is_trade_started: int | None = None
    display: dict[str, str] | None = None
    timestamp: str


_MESSAGES = {
    "FUT_ALLOW": "The futures entry passed the configured checks and was submitted successfully.",
    "ALREADY_APPROVED": "This futures setup was already approved and submitted. No duplicate order was created.",
    "USER_REJECTED": "The futures setup was rejected by the user and no broker order was submitted.",
    "FUTURES_GTT_ALREADY_ABSENT": "The futures setup was rejected and no related FYERS GTT was found.",
    "FUTURES_CANDIDATE_REOPENED": "The rejected futures setup was reopened for editing and approval.",
    "FUT_REJECT_TRADE_RISK_GT_CONFIGURED_CAP": "Trade rejected because calculated risk {risk} exceeds the configured limit {limit} by {excess}.",
    "GLOBAL_REALIZED_LOSS_CEILING": "New trading is blocked because combined realized loss {combined_loss} has reached the configured ceiling {ceiling}.",
    "FUT_REJECT_MARGIN_PREFLIGHT": "Trade rejected because sufficient broker margin could not be confirmed.",
    "FUT_REJECT_ACTIVE_FUTURES_WINNER": "A futures position is already active. New futures entries are blocked until it completes.",
    "FUT_REJECT_DUPLICATE_SIGNAL": "This futures signal has already been reserved or processed.",
    "FUT_REJECT_INVALID_LOT_SIZE": "Trade rejected because the futures lot size is missing or invalid for the resolved contract.",
    "FUT_REJECT_INVALID_LOT_SIZE_OR_CONTRACT": "Trade rejected because the futures lot size does not match the resolved contract.",
    "FUT_REJECT_SYMBOL_UNRESOLVED": "Trade rejected because the current FYERS futures contract could not be resolved safely.",
    "FUT_REJECT_INVALID_LONG_SIGNAL": "Trade rejected because the setup does not satisfy the required LONG entry, stop, and target relationship.",
    "FUT_REJECT_ENABLEMENT_GATES": "Futures trading is currently disabled by the configured trading controls.",
    "FUT_REJECT_STATE_RECONCILIATION_FAILED": "Futures trading is blocked because the current trading state could not be reconciled safely.",
    "FUT_REJECT_SERIAL_LOCK_ACTIVE": "A futures trade lifecycle is already active. A new futures entry cannot be submitted.",
    "FUT_REJECT_MISSING_ENTRY": "Trade rejected because the entry price is missing or invalid.",
    "FUT_REJECT_MISSING_STOP": "Trade rejected because the stop price is missing or invalid.",
    "FUT_REJECT_INVALID_TARGET": "Trade rejected because the target price is missing or invalid.",
    "FUTURES_BUCKET_NOT_FOUND": "The requested futures setup was not found.",
    "FYERS_GTT_REJECTED": "The trade passed PREDIXA checks, but FYERS rejected the GTT request.",
    "FYERS_GTT_STATUS_AMBIGUOUS": "The broker response is unresolved and could not be confirmed. The system will not retry automatically.",
    "FUT_REJECT_EXPIRY_MISMATCH": "Trade rejected because the stored futures expiry does not match the resolved contract.",
    "FUT_REJECT_FILLED_ORDER": "A futures position has already started, so this order cannot be rejected here.",
    "FUT_REJECT_RESERVATION_NOT_FOUND": "The durable futures reservation for this setup was not found.",
    "FUT_REJECT_INVALID_STATUS": "The request status must be approved or rejected.",
    "FUT_REJECT_NOT_PENDING": "This futures setup is no longer pending approval.",
    "FUT_REJECT_ORDER_ALREADY_STARTED": "This futures order has already started and cannot be approved again.",
    "FUT_REJECT_INVALID_EXPIRY": "Trade rejected because the futures candidate expiry is invalid or has passed.",
    "FUTURES_APPROVAL_PROCESSING_ERROR": "The futures approval request could not be completed safely.",
    "FUT_REJECT_MISSING_SIGNAL_ID": "Trade rejected because the futures signal identifier is missing.",
    "FUT_REJECT_FUTURES_POLICY_UNAPPROVED": "Futures approval is blocked because the configured futures policy is incomplete.",
    "FUT_REJECT_SYMBOL_NOT_APPROVED": "Trade rejected because this futures symbol is not approved for the current futures universe.",
    "FUT_REJECT_TAP_CLOSED": "Trade rejected because the configured futures risk throttle is closed.",
    "FUT_REJECT_DD_BAND_UNDEFINED": "Trade rejected because the configured risk band has no approved capacity.",
    "FUT_REJECT_CONCURRENCY_LIMIT": "Trade rejected because the configured futures concurrency limit has been reached.",
    "FUT_REJECT_OPEN_RISK_GT_18000": "Trade rejected because open futures risk would exceed the configured limit.",
    "FUT_REJECT_TRADE_RISK_GT_14000": "Trade rejected because calculated trade risk exceeds the configured limit.",
    "FUT_REJECT_OPTION3_MODE_REQUIRED": "Trade rejected because the configured futures execution mode is not Option-3.",
    "FUT_REJECT_MISSING_GTT_ID": "The futures broker identifier is missing, so this request cannot be processed safely.",
    "FUT_REJECT_INCOMPLETE_CANDIDATE": "The scanner-created futures candidate is missing required data.",
    "FUT_REJECT_NOT_REJECTED": "Only a rejected, never-submitted futures candidate can be reopened.",
    "FUT_REJECT_BROKER_LINKED_CANDIDATE": "This futures candidate has broker-linked or active execution state and cannot be reopened here.",
}


def _number(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _money(value):
    try:
        return f"₹{float(value):,.2f}"
    except (TypeError, ValueError):
        return "₹0.00"


def get_futures_reason_message(reason_code, context=None):
    context = context or {}
    template = _MESSAGES.get(reason_code, "Futures approval was rejected: {reason}.")
    values = {
        "reason": reason_code,
        "risk": _money(context.get("trade_risk")),
        "limit": _money(context.get("configured_trade_risk_cap")),
        "excess": _money(context.get("excess_risk")),
        "combined_loss": _money(context.get("combined_realized_loss")),
        "ceiling": _money(context.get("global_loss_ceiling")),
    }
    return template.format(**values)


def _risk_context(risk):
    decision = getattr(risk, "last_decision", None) or {}
    if not decision:
        return {}, {}
    calculation = {
        key: _number(decision.get(key))
        for key in ("risk_per_unit", "trade_risk", "configured_trade_risk_cap", "excess_risk")
        if decision.get(key) is not None
    }
    risk_state = {
        key: _number(decision.get(key))
        for key in ("current_open_futures_risk", "proposed_open_futures_risk",
                    "cash_realized_loss", "futures_realized_loss",
                    "combined_realized_loss", "global_loss_ceiling", "global_halt")
        if decision.get(key) is not None
    }
    return calculation, risk_state


def build_futures_response(*, reason_code, http_status, row=None, risk=None,
                           success=False, decision="REJECTED", broker=None,
                           approval_status=None, is_trade_started=None,
                           contract=None, state=None, extra=None):
    calculation, risk_state = _risk_context(risk)
    context = dict(calculation)
    if context.get("configured_trade_risk_cap") is not None and context.get("trade_risk") is not None:
        excess = max(
            Decimal("0"),
            Decimal(str(context["trade_risk"]))
            - Decimal(str(context["configured_trade_risk_cap"])),
        )
        context["excess_risk"] = float(excess)
        calculation["excess_risk"] = context["excess_risk"]
    order = None
    if row is not None:
        order = {
            "side": str(getattr(row, "order_type", "BUY")).upper(),
            "quantity": _number(getattr(row, "stock_quantity", None)),
            "lot_size": _number(getattr(row, "stock_quantity", None)),
            "entry_price": _number(getattr(row, "entry_price", None)),
            "stop_price": _number(getattr(row, "stoploss_price", None)),
            "target_price": _number(getattr(row, "target_price", None)),
            "expiry": _number(getattr(row, "expiry_date", None)),
        }
    checks = {}
    if row is not None:
        checks["approval_status"] = {
            "evaluated": True,
            "passed": getattr(row, "approval_status", None) == "pending"
                or approval_status in ("approved", "rejected"),
        }
        checks["side"] = {"evaluated": True,
                           "passed": str(getattr(row, "order_type", "")).upper() == "BUY"}
    if "global_halt" in risk_state:
        checks["global_halt"] = {
            "evaluated": True,
            "passed": reason_code != "GLOBAL_REALIZED_LOSS_CEILING"
                and risk_state["global_halt"] is False,
        }
    if calculation.get("configured_trade_risk_cap") is not None:
        checks["risk_cap"] = {
            "evaluated": True,
            "passed": reason_code != "FUT_REJECT_TRADE_RISK_GT_CONFIGURED_CAP",
            "actual": calculation.get("trade_risk"),
            "limit": calculation.get("configured_trade_risk_cap"),
        }
    payload = FuturesApprovalResponse(
        success=success, decision=decision, reason_code=reason_code,
        message=get_futures_reason_message(reason_code, context),
        http_status=http_status,
        bucket_id=getattr(row, "bucket_id", None) if row is not None else None,
        trade_signal_id=getattr(row, "trade_signal_id", None) if row is not None else None,
        symbol=getattr(row, "stock_tick", None) if row is not None else None,
        contract=contract,
        timeframe=getattr(row, "time_frame", None) if row is not None else None,
        order=order, calculation=calculation or None, risk_state=risk_state or None,
        state=state,
        broker=broker, approval_status=approval_status,
        is_trade_started=is_trade_started,
        checks=checks or None,
        timestamp=datetime.now().astimezone().isoformat(),
        display={"severity": "success" if success else "warning",
                 "title": "Futures approval"},
    )
    result = payload.model_dump(exclude_none=True)
    if extra:
        result.update(extra)
    logger.info(
        "futures approval decision bucket_id=%s signal_id=%s decision=%s reason_code=%s trade_risk=%s http_status=%s",
        result.get("bucket_id"), result.get("trade_signal_id"), decision,
        reason_code, calculation.get("trade_risk"), http_status,
    )
    return result
