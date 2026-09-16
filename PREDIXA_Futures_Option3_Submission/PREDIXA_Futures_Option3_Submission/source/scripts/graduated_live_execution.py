"""The only approved broker-entry adapter for graduated-live.

It separates new entries from protective actions.  A caller cannot treat a
network exception as a broker rejection: the serial reservation remains locked
until an authoritative broker reconciliation proves there is no exposure.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Protocol

from scripts.futures_risk_engine import RiskEngine, Signal, number


class Broker(Protocol):
    def place_order(self, payload: dict) -> dict: ...


@dataclass(frozen=True)
class MarginQuote:
    required_one_lot: object
    available_funds: object
    as_of: str


class MarginPreflight:
    """Pure margin/capital check.  It never substitutes notional for margin."""

    def __init__(self, *, buffer_inr=None, buffer_pct=None):
        if buffer_inr is None and buffer_pct is None:
            raise ValueError("An approved margin buffer policy is required")
        if buffer_inr is not None and number(buffer_inr) < 0:
            raise ValueError("Invalid margin buffer")
        if buffer_pct is not None and number(buffer_pct) < 0:
            raise ValueError("Invalid margin buffer")
        self.buffer_inr = Decimal(str(buffer_inr)) if buffer_inr is not None else None
        self.buffer_pct = Decimal(str(buffer_pct)) if buffer_pct is not None else None

    def required_with_buffer(self, required):
        value = number(required, True)
        if self.buffer_inr is not None:
            return value + self.buffer_inr
        return value * (Decimal(1) + self.buffer_pct / Decimal(100))

    def check(self, quote: MarginQuote) -> tuple[bool, str]:
        required = self.required_with_buffer(quote.required_one_lot)
        available = number(quote.available_funds)
        if available < required:
            return False, "FUT_REJECT_MARGIN_INSUFFICIENT"
        return True, "MARGIN_ALLOW"


class GraduatedLiveExecution:
    """Routes new risk through the global/serial guard.

    ``submit_futures_option3`` is the production method.  The legacy
    ``submit_futures`` method is retained as a compatibility shim for the
    first-pass offline tests and is never called by the production routes.
    """

    def __init__(self, risk: RiskEngine, broker: Any, *, margin_provider: Callable[[Signal], MarginQuote] | None = None,
                 margin_policy: MarginPreflight | None = None):
        self.risk = risk
        self.broker = broker
        self.margin_provider = margin_provider
        self.margin_policy = margin_policy

    @staticmethod
    def _is_futures_payload(signal: Signal, payload: dict) -> bool:
        return payload.get("symbol") == signal.contract or str(signal.contract).endswith("FUT")

    @staticmethod
    def _matches_single_futures(signal: Signal, payload: dict) -> bool:
        if payload.get("symbol") != signal.contract or payload.get("side") != 1:
            return False
        if payload.get("productType") not in ("MARGIN", "NRML"):
            return False
        info = payload.get("orderInfo")
        if not isinstance(info, dict):
            return False
        leg = info.get("leg1")
        if not isinstance(leg, dict):
            return False
        try:
            return (
                leg.get("qty") == signal.lot_size
                and number(leg.get("price")) == number(signal.entry)
                and number(leg.get("triggerPrice")) == number(signal.entry)
            )
        except ValueError:
            return False

    def _margin_ok(self, signal):
        if self.margin_provider is None or self.margin_policy is None:
            return False, "FUT_REJECT_MARGIN_POLICY_UNAPPROVED"
        try:
            quote = self.margin_provider(signal)
            return self.margin_policy.check(quote)
        except Exception as exc:
            self.risk.set_error_locked("MARGIN_PREFLIGHT_FAILED: %s" % exc)
            return False, "FUT_REJECT_MARGIN_UNAVAILABLE"

    def _place_futures_gtt(self, payload):
        if not hasattr(self.broker, "place_futures_entry_gtt"):
            raise RuntimeError(
                "Approved Option-3 futures broker gateway is unavailable"
            )

        return self.broker.place_futures_entry_gtt(dict(payload), risk=self.risk)

    def submit_futures_option3(self, signal: Signal, payload: dict):
        """Preflight, atomically reserve, then submit exactly one GTT Single."""
        if not self.risk.production_policy_ready:
            return {"submitted": False, "reason": "FUT_REJECT_FUTURES_POLICY_UNAPPROVED"}
        margin_ok, margin_reason = self._margin_ok(signal)
        if not margin_ok:
            return {"submitted": False, "reason": margin_reason}
        if not self._matches_single_futures(signal, payload):
            return {"submitted": False, "reason": "FUT_REJECT_PAYLOAD_MISMATCH"}
        reason = self.risk.reserve(signal, production=True, margin_ok=True)
        if reason != "FUT_ALLOW":
            return {"submitted": False, "reason": reason}
        # Re-read the live margin immediately after reservation.  A changed
        # quote suppresses the broker call and safely releases this reservation.
        margin_ok_after, margin_reason_after = self._margin_ok(signal)
        if not margin_ok_after:
            self.risk.cancel_unsubmitted(signal, margin_reason_after)
            return {"submitted": False, "reason": margin_reason_after}
        try:
            response = self.broker.place_futures_entry_gtt(
                dict(payload), risk=self.risk, signal_id=signal.signal_id
            )
        except Exception:
            # Unknown broker outcome: preserve ENTRY_RESERVING and lock locally.
            self.risk.ready = False
            self.risk.set_error_locked("FUTURES_GTT_SUBMISSION_UNKNOWN")
            raise
        if not isinstance(response, dict):
            self.risk.set_error_locked("FUTURES_GTT_UNEXPECTED_RESPONSE")
            return {"submitted": False, "reason": "FUT_BROKER_RESPONSE_UNKNOWN"}
        if response.get("s") != "ok":
            # A response is not proof that no order exists.  Keep the slot until
            # the caller runs reconcile_broker(..., broker_known=True).
            self.risk.set_error_locked("FUTURES_GTT_REJECTED_REQUIRES_RECONCILIATION")
            return {"submitted": False, "reason": "FUT_BROKER_REJECTED", "response": response}
        self.risk.mark_gtt_resting(signal.signal_id, gtt_id=response.get("id"),
                                   broker_order_id=response.get("id_fyers"))
        return {"submitted": True, "reason": "FUT_ALLOW", "response": response}

    def submit_futures(self, signal: Signal, payload: dict):
        """Compatibility shim; production must call ``submit_futures_option3``."""
        reason = self.risk.reserve(signal)
        if reason != "FUT_ALLOW":
            return {"submitted": False, "reason": reason}
        expected = {"symbol": signal.contract, "qty": signal.lot_size, "side": 1, "type": 1}
        try:
            matches = not any(payload.get(k) != v for k, v in expected.items()) and number(payload.get("limitPrice")) == number(signal.entry)
        except ValueError:
            matches = False
        if not matches:
            self.risk.cancel_unsubmitted(signal, "FUT_REJECT_PAYLOAD_MISMATCH")
            raise ValueError("Payload does not match signal risk basis")
        try:
            response = self.broker.place_order(dict(payload))
        except Exception:
            self.risk.ready = False
            raise
        return {"submitted": True, "reason": reason, "response": response}

    def submit_cash(self, payload: dict):
        """Global ceiling guard for a new cash entry."""
        reason = self.risk.cash_entry_allowed()
        if reason != "CASH_ALLOW":
            return {"submitted": False, "reason": reason}
        if hasattr(self.broker, "place_cash_order"):
            response = self.broker.place_cash_order(dict(payload))
        else:
            response = self.broker.place_order(dict(payload))
        return {"submitted": True, "response": response}

    def request_stale_entry_cancel(self, signal_id):
        """Request cancellation and remain locked until broker confirmation."""
        self.risk.mark_entry_cancel_pending(signal_id)
        data = self.risk.store.snapshot()
        order = data.get("broker_orders", {}).get(signal_id, {})
        gtt_id = order.get("entry_gtt_id")
        if not gtt_id:
            self.risk.set_error_locked("STALE_ENTRY_MISSING_GTT_ID")
            return {"requested": False, "reason": "FUT_REJECT_GTT_ID_MISSING"}
        try:
            if hasattr(self.broker, "cancel_gtt_order"):
                response = self.broker.cancel_gtt_order({"id": gtt_id})
            else:
                response = self.broker._client.cancel_gtt_order(data={"id": gtt_id})
        except Exception:
            self.risk.ready = False
            self.risk.set_error_locked("STALE_ENTRY_CANCEL_UNKNOWN")
            raise
        return {"requested": True, "response": response, "reason": "FUT_CANCEL_PENDING"}

    def claim_futures_winner_and_cancel_losers(self, signal_id, *, event_id=None):
        """Claim atomically, then cancel only persisted loser entry GTT IDs."""
        result = self.risk.claim_futures_winner(signal_id, event_id=event_id)
        if result.get("status") != "WINNER":
            return result
        responses = []
        for loser in result["losers"]:
            gtt_id = loser.get("gtt_id")
            if not gtt_id:
                self.risk.set_error_locked("FUTURES_LOSER_GTT_ID_MISSING:%s" % loser["signal_id"])
                continue
            try:
                if hasattr(self.broker, "cancel_gtt_order"):
                    response = self.broker.cancel_gtt_order({"id": gtt_id})
                else:
                    response = self.broker._client.cancel_gtt_order(data={"id": gtt_id})
            except Exception:
                self.risk.ready = False
                self.risk.set_error_locked("FUTURES_LOSER_CANCEL_UNKNOWN:%s" % loser["signal_id"])
                raise
            responses.append({"signal_id": loser["signal_id"], "gtt_id": gtt_id, "response": response})
        return {**result, "cancel_requests": responses}

    def reconcile_entry_cancel(self, signal_id, *, broker_known, cancelled, filled, open_position):
        if broker_known is not True:
            self.risk.set_error_locked("STALE_ENTRY_CANCEL_UNCONFIRMED")
            return False
        if filled or open_position:
            self.risk.confirm_entry_cancel(signal_id, broker_verified=True, filled=True, open_position=True)
            return False
        if cancelled:
            return self.risk.confirm_entry_cancel(signal_id, broker_verified=True)
        self.risk.set_error_locked("STALE_ENTRY_TERMINAL_STATE_UNKNOWN")
        return False

    def submit_protective_oco(self, signal_id, payload: dict):
        """Protective exits stay available during a global halt."""
        try:
            if hasattr(self.broker, "place_protective_gtt"):
                response = self.broker.place_protective_gtt(dict(payload))
            else:
                response = self.broker._client.place_gtt_order(data=dict(payload))
        except Exception:
            self.risk.mark_protective_result(signal_id, success=False)
            raise
        success = isinstance(response, dict) and response.get("s") == "ok"
        self.risk.mark_protective_result(signal_id, success=success, gtt_id=response.get("id") if isinstance(response, dict) else None)
        return response

    def reconcile_startup(self, **snapshot):
        return self.risk.reconcile_broker(**snapshot)

    def close_position(self, payload: dict):
        """Protective/manual close; never passed through a new-entry guard."""
        if hasattr(self.broker, "close_position"):
            return self.broker.close_position(dict(payload))
        return self.broker._client.exit_positions(data=dict(payload))
