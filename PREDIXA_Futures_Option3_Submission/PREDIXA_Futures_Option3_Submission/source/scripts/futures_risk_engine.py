"""Durable graduated-live risk and futures Option-3 serial state machine.

The engine has no broker dependency. It owns the transaction that reserves the
single futures slot; the execution adapter submits a broker order only after
that transaction commits. Option-2's old DD throttle is retained behind the
explicit ``OPTION2_DYNAMIC`` mode and is never consulted by Option-3.
"""
from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation

from scripts.graduated_live_config import (
    DD_ONE_LOT_MAX_EXCLUSIVE, DD_TAP_CLOSE, DD_TAP_REOPEN_BELOW,
    DD_TWO_LOT_MAX_EXCLUSIVE, FUTURES_MODE, FUTURES_TIER0_LOTS, GATES,
    MAX_OPEN_FUTURES_RISK, MAX_SINGLE_FUTURES_TRADE_RISK, OPTION2_MODE,
    Option3Policy, Policy, TOTAL_REALIZED_LOSS_CEILING,
)

logger = logging.getLogger("stock_project_logger")


def number(value, positive: bool = False) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ValueError("Missing/invalid numeric value")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("Invalid numeric value") from exc
    if not result.is_finite() or (positive and result <= 0):
        raise ValueError("Nonfinite/nonpositive numeric value")
    return result


def units(value) -> Decimal:
    result = number(value, True)
    if result != result.to_integral_value():
        raise ValueError("Quantity must be integral units")
    return result

def nonnegative_units(value) -> Decimal:
    result = number(value)

    if result < 0:
        raise ValueError("Quantity cannot be negative")

    if result != result.to_integral_value():
        raise ValueError("Quantity must be integral units")

    return result


@dataclass(frozen=True)
class Signal:
    signal_id: str
    symbol: str
    timeframe: int
    entry: object
    stop: object
    target: object
    contract: str
    lot_size: int
    side: str = "BUY"
    zone_id: str = ""


class RiskEngine:
    """Persistent risk decisions and lifecycle transitions.

    ``production=True`` is required by the production gateway. The default is
    retained only for first-pass offline fixtures; it never enables a broker.
    """

    def __init__(self, store, policy: Policy = Policy(), option3_policy: Option3Policy | None = None):
        self.store = store
        self.policy = policy
        self._option3_explicit = option3_policy is not None
        existing_mode = None
        try:
            existing_mode = store.snapshot().get("mode")
        except Exception:
            pass
        self._legacy_compat = (
            not self._option3_explicit
            and (policy.dd_update is not None or existing_mode == OPTION2_MODE)
        )
        self.option3_policy = option3_policy or Option3Policy()
        # Readiness is established by an explicit reconciliation call.  A new
        # engine must not infer it from construction; production checks use the
        # durable ``reconciled`` flag below.
        self.ready = False
        # Presentation-only context for the approval API.  Decisions and
        # formulas remain owned by this engine; callers still receive the
        # legacy reason string from reserve().
        self.last_decision = {}
        # if policy.dd_update is None:
        #     logger.error("FUT_REJECT_DD_POLICY_UNAPPROVED")
        valid, errors = self.option3_policy.validate()
        if not valid:
            logger.warning("FUTURES_POLICY_UNRESOLVED: %s", ",".join(errors))

    @staticmethod
    def _state_from_exposure(data):
        if data.get("positions"):
            return "POSITION_OPEN_LOCKED"
        if data.get("reservations"):
            return "ENTRY_GTT_RESTING"
        return "IDLE"

    @classmethod
    def _recompute_state(cls, data):
        """Derive lifecycle state from durable execution exposure.

        Candidate approval status is deliberately excluded. Exceptional
        states remain sticky until their explicit recovery path resolves them.
        """
        if data.get("state") == "ERROR_LOCKED":
            return data["state"]
        if data.get("positions"):
            if data.get("state") in ("EXIT_RECONCILING", "EXIT_OCO_ACTIVE"):
                return data["state"]
            return "POSITION_OPEN_LOCKED"
        if data.get("active_winner") or data.get("serial_reservation"):
            return "EXIT_RECONCILING" if data.get("winner_exit_confirmed") else "ENTRY_FILL_PENDING_RECONCILIATION"
        if data.get("reservations"):
            return "ENTRY_GTT_RESTING"
        return "IDLE"

    def recompute_futures_state(self):
        """Recompute normal lifecycle state without clearing safety locks."""
        with self.store.transaction() as (data, _):
            before = data.get("state")
            after = self._recompute_state(data)
            if before != "ERROR_LOCKED":
                data["state"] = after
            return {"before": before, "after": data.get("state")}

    def release_unsubmitted_reservation(self, signal_id, *, reason="CANDIDATE_TERMINAL"):
        """Release only a reservation with no broker order or open exposure."""
        with self.store.transaction() as (data, conn):
            reservation = data.get("reservations", {}).get(signal_id)
            if reservation is None:
                return {"released": False, "state": data.get("state")}
            if reservation.get("gtt_id") or reservation.get("broker_order_id"):
                raise ValueError("Broker-linked reservation requires broker confirmation")
            if data.get("active_winner") == signal_id or data.get("serial_reservation") == signal_id:
                raise ValueError("Active futures winner cannot be released")
            data["reservations"].pop(signal_id, None)
            data.setdefault("reservation_history", {})[signal_id] = {
                **reservation, "status": "TERMINAL_REJECTED", "terminal_reason": reason,
                "completed_at": datetime.now(timezone.utc).isoformat(),
            }
            data.setdefault("broker_orders", {}).pop(signal_id, None)
            before = data.get("state")
            if before != "ERROR_LOCKED":
                data["state"] = self._recompute_state(data)
            record = {
                "event": "UNSUBMITTED_RESERVATION_RELEASED", "signal_id": signal_id,
                "reason": reason, "state_before": before,
                "state_after": data.get("state"),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            self.store.log(conn, record)
            return {"released": True, "state": data.get("state"), "record": record}

    def record_candidate_edit(self, signal_id, changes):
        """Audit a database-only candidate edit; it creates no reservation."""
        with self.store.transaction() as (data, conn):
            record = {
                "event": "FUTURES_CANDIDATE_EDITED", "signal_id": str(signal_id),
                "changes": {str(key): str(value) for key, value in changes.items()},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            data.setdefault("events", []).append(record["event"] + ":" + str(signal_id))
            self.store.log(conn, record)
            return record

    def refresh(self, data):
        """Recompute hard-ceiling/latches; DD is Option-2-only."""
        data.setdefault("mode", FUTURES_MODE)
        data.setdefault("state", "IDLE")
        data.setdefault("positions", {})
        data.setdefault("reservations", {})
        data.setdefault("tap", False)
        data.setdefault("halt", False)
        if number(data.get("cash_loss", 0)) + number(data.get("futures_loss", 0)) >= TOTAL_REALIZED_LOSS_CEILING:
            data["halt"] = True
            data["global_halt_reason"] = "GLOBAL_REALIZED_LOSS_CEILING"
        if data.get("mode") == OPTION2_MODE:
            dd = number(data.get("dd", 0))
            if dd >= DD_TAP_CLOSE:
                data["tap"] = True
            elif data.get("tap") and dd < DD_TAP_REOPEN_BELOW:
                data["tap"] = False
        return self.dynamic_capacity(data) if data.get("mode") == OPTION2_MODE else None

    def dynamic_capacity(self, data):
        """Parked Option-2 policy. Never call for Option-3 decisions."""
        if data.get("mode") != OPTION2_MODE:
            return None
        dd = number(data.get("dd", 0))
        if dd < DD_TWO_LOT_MAX_EXCLUSIVE:
            return 2
        if dd < DD_ONE_LOT_MAX_EXCLUSIVE:
            return 1
        if dd < DD_TAP_CLOSE:
            return self.policy.undefined_band_max_lots
        return 0

    @staticmethod
    def exposure(data):
        risk = Decimal(0)
        lots = Decimal(0)
        for position in list(data.get("positions", {}).values()) + list(data.get("reservations", {}).values()):
            quantity = units(position["quantity"])
            risk += abs(number(position["entry"], True) - number(position["stop"], True)) * quantity
            lots += quantity / units(position["lot_size"])
        return risk, lots

    def _policy_ready(self):
        valid, _ = self.option3_policy.validate()
        return valid

    @property
    def production_policy_ready(self):
        return FUTURES_MODE == "OPTION3_SERIAL" and self._policy_ready()

    @property
    def futures_enabled(self):
        data = self.store.snapshot()
        self.refresh(data)
        if self._legacy_compat and data.get("mode") == OPTION2_MODE:
            return bool(
                self.ready and data.get("reconciled") and not data.get("halt")
                and all(data.get("gates", {}).get(g) is True for g in GATES)
            )
        return bool(
            data.get("reconciled") is True
            and data.get("state") != "ERROR_LOCKED"
            and FUTURES_MODE == "OPTION3_SERIAL" and self._policy_ready()
            and all(data.get("gates", {}).get(g) is True for g in GATES)
            and not data.get("halt")
        )

    @property
    def serial_state(self):
        return self.store.snapshot().get("state", "ERROR_LOCKED")

    def reconcile(self, positions, *, dd, cash_loss, futures_loss, approved, gates,
                  unresolved_orders, source_verified):
        """Apply an authoritative broker/account snapshot before new entries."""
        self.ready = False
        try:
            if source_verified is not True or unresolved_orders:
                raise ValueError("Unverified state or unresolved broker orders")
            for value in (dd, cash_loss, futures_loss):
                if number(value) < 0:
                    raise ValueError("Negative loss state")
            for contract in approved.values():
                date.fromisoformat(contract["expiry"])
                date.fromisoformat(contract["as_of"])
                units(contract["lot_size"])
            self.exposure({"positions": positions, "reservations": {}})
            with self.store.transaction() as (data, _):
                was_error_locked = data.get("state") == "ERROR_LOCKED"
                if data.get("reservations"):
                    raise ValueError("Resolve durable reservations individually before startup")
                data.update(positions=positions, dd=str(dd), cash_loss=str(cash_loss),
                            futures_loss=str(futures_loss), approved=approved, gates=gates,
                            reconciled=True, mode=FUTURES_MODE,
                            last_reconciliation=datetime.now(timezone.utc).isoformat())
                if self._legacy_compat:
                    # Compatibility only for first-pass Option-2 fixtures.
                    # Default/production instances remain OPTION3_SERIAL.
                    data["mode"] = OPTION2_MODE
                data["state"] = "ERROR_LOCKED" if was_error_locked else (
                    "POSITION_OPEN_LOCKED" if positions else "IDLE"
                )
                data["broker_state_verified"] = True
                data["recovery_possible"] = was_error_locked
                self.refresh(data)
            self.ready = True
            return True
        except Exception as exc:
            logger.exception("FUT_REJECT_STATE_RECONCILIATION_FAILED: %s", exc)
            try:
                self.store.mark_error(str(exc))
            except Exception:
                pass
            return False

    def reconcile_broker(self, *, broker_known, resting_entry, open_position,
                         protective_exit_active=False, positions=None):
        """Reconcile broker state; a timeout is never treated as cancellation."""
        self.ready = False
        try:
            if broker_known is not True:
                raise ValueError("Broker state ambiguous")
            if resting_entry and open_position:
                raise ValueError("Broker reports resting entry and open position")
            with self.store.transaction() as (data, _):
                if positions is not None:
                    data["positions"] = positions
                if resting_entry:
                    if not data.get("reservations"):
                        raise ValueError("Broker entry has no local serial reservation")
                    data["state"] = "ENTRY_GTT_RESTING"
                elif open_position:
                    data["state"] = "EXIT_OCO_ACTIVE" if protective_exit_active else "POSITION_OPEN_LOCKED"
                else:
                    if data.get("state") == "ERROR_LOCKED":
                        raise ValueError("Explicit ERROR_LOCKED requires operator reconciliation")
                    data["reservations"] = {}
                    data["serial_reservation"] = None
                    data["positions"] = {}
                    data["state"] = "IDLE"
                data["reconciled"] = True
                data["last_reconciliation"] = datetime.now(timezone.utc).isoformat()
                self.refresh(data)
            self.ready = True
            return True
        except Exception as exc:
            logger.exception("FUT_REJECT_STATE_RECONCILIATION_FAILED: %s", exc)
            try:
                self.store.mark_error(str(exc))
            except Exception:
                pass
            return False

    def reconcile_verified_state(self, *, positions, reservation_statuses,
                                 approved, gates, dd, cash_loss, futures_loss,
                                 source_verified):
        """Persist a verified broker snapshot without submitting or cancelling.

        ``reservation_statuses`` is keyed by the durable reservation ID and is
        produced by a read-only exact-GTT-ID lookup.  This method intentionally
        supports multiple pending entry reservations; the old one-reservation
        startup restriction is not valid for Option-3.
        """
        self.ready = False
        try:
            if source_verified is not True:
                raise ValueError("Broker/account snapshot is not verified")
            for value in (dd, cash_loss, futures_loss):
                if number(value) < 0:
                    raise ValueError("Negative loss state")
            for contract in (approved or {}).values():
                date.fromisoformat(contract["expiry"])
                date.fromisoformat(contract["as_of"])
                units(contract["lot_size"])
                if contract.get("fyers_resolved") is not True:
                    raise ValueError("Unresolved approved futures contract")
            missing_gates = [g for g in GATES if (gates or {}).get(g) is not True]
            if missing_gates:
                # Gate evidence controls entry readiness, not whether the
                # broker snapshot itself was successfully reconciled.
                logger.warning("GATE_EVIDENCE_NOT_IMPLEMENTED: %s", ",".join(missing_gates))

            with self.store.transaction() as (data, _):
                was_error_locked = data.get("state") == "ERROR_LOCKED"
                reservations = data.setdefault("reservations", {})
                if set(reservation_statuses or {}) != set(reservations):
                    raise ValueError("Durable reservations do not match broker snapshot")
                for signal_id, status in (reservation_statuses or {}).items():
                    if status not in {"PENDING", "CANCEL_REQUESTED", "CANCEL_CONFIRMED", "REJECTED", "EXPIRED", "TRIGGERED"}:
                        raise ValueError("Unknown broker reservation status")
                    if status == "TRIGGERED" and signal_id != (data.get("active_winner") or data.get("serial_reservation")):
                        raise ValueError("Broker reports a triggered non-winner entry")
                    reservations[signal_id]["status"] = status

                active = data.get("active_winner") or data.get("serial_reservation")
                if active and active not in reservations and active not in (data.get("positions") or {}):
                    raise ValueError("Durable winner is not represented")
                if active and active not in (positions or {}):
                    raise ValueError("Active winner has no broker position")
                if not active:
                    broker_position_ids = set(positions or {})
                    if broker_position_ids:
                        raise ValueError("Broker futures exposure has no durable winner")

                data.update(
                    positions=positions or {}, approved=approved or {}, gates=gates or {},
                    dd=str(dd), cash_loss=str(cash_loss), futures_loss=str(futures_loss),
                    reconciled=True, mode=FUTURES_MODE,
                    last_reconciliation=datetime.now(timezone.utc).isoformat(),
                    broker_state_verified=True,
                    recovery_possible=was_error_locked,
                )
                terminal = {"CANCEL_CONFIRMED", "REJECTED", "EXPIRED"}
                for signal_id, reservation in list(reservations.items()):
                    if signal_id != active and reservation.get("status") in terminal:
                        data.setdefault("reservation_history", {})[signal_id] = {
                            **reservation,
                            "completed_at": datetime.now(timezone.utc).isoformat(),
                        }
                        reservations.pop(signal_id, None)
                if was_error_locked:
                    # A verified snapshot is useful for operator recovery, but
                    # it is never permission to clear an exposure incident.
                    data["state"] = "ERROR_LOCKED"
                elif active:
                    data["state"] = "POSITION_OPEN_LOCKED"
                elif reservations:
                    data["state"] = "ENTRY_GTT_RESTING"
                else:
                    data["state"] = "IDLE"
                data["cancel_requested"] = any(
                    item.get("status") == "CANCEL_REQUESTED"
                    for item in reservations.values()
                )
                self.refresh(data)
            self.ready = True
            return True
        except Exception as exc:
            logger.exception("FUT_REJECT_STATE_RECONCILIATION_FAILED: %s", exc)
            try:
                self.store.mark_error(str(exc))
            except Exception:
                pass
            return False

    def _audit(self, conn, signal, *, risk, risk_before, lots, cap, decision, reason, data):
        record = {k: str(v) if k in ("entry", "stop", "target", "lot_size") else v
                  for k, v in asdict(signal).items()}
        proposed = risk_before + risk if risk is not None else None
        record.update(timestamp=datetime.now(timezone.utc).isoformat(),
                     this_trade_risk=str(risk) if risk is not None else None,
                     OPEN_FUT_RISK=str(risk_before),
                     proposed_OPEN_FUT_RISK=str(proposed) if proposed is not None else None,
                     REALIZED_FUT_DD=str(data.get("dd", "0")), tap=data.get("tap", False),
                     open_lots=str(lots), max_lots=cap, global_halt=data.get("halt", False),
                     decision=decision, reason=reason, mode=data.get("mode", FUTURES_MODE),
                     state=data.get("state", "IDLE"))
        self.store.log(conn, record)
        logger.info("graduated_live %s", record)

    def reserve(self, signal: Signal, *, production=False, margin_ok=True):
        """Atomically reserve one approved futures entry.

        Multiple reservations are intentional. ``serial_reservation`` is
        populated only after the first trigger is claimed.
        """
        with self.store.transaction() as (data, conn):
            self.refresh(data)
            risk_before, lots = self.exposure(data)
            cap = None
            risk = None
            reason = None
            state = data.get("state", "IDLE")
            serial_active = production or data.get("mode", FUTURES_MODE) == "OPTION3_SERIAL"
            if serial_active and (data.get("active_winner") or data.get("serial_reservation")
                                  or data.get("positions")):
                reason = "FUT_REJECT_ACTIVE_FUTURES_WINNER"
            elif data.get("state") == "ERROR_LOCKED":
                reason = "FUT_REJECT_STATE_RECONCILIATION_FAILED"
            elif data.get("halt"):
                reason = "GLOBAL_REALIZED_LOSS_CEILING"
            elif production and not self.production_policy_ready:
                reason = "FUT_REJECT_FUTURES_POLICY_UNAPPROVED"
            elif production and data.get("mode", FUTURES_MODE) != "OPTION3_SERIAL":
                reason = "FUT_REJECT_OPTION3_MODE_REQUIRED"
            elif not production and not self.ready and data.get("positions"):
                reason = "FUT_REJECT_STATE_RECONCILIATION_FAILED"
            elif not margin_ok:
                reason = "FUT_REJECT_MARGIN_PREFLIGHT"
            elif not production and self.policy.dd_update is None:
                reason = "FUT_REJECT_DD_POLICY_UNAPPROVED"
            elif signal.symbol not in data.get("approved", {}):
                reason = "FUT_REJECT_SYMBOL_NOT_APPROVED"
            else:
                contract = data["approved"][signal.symbol]
                if contract.get("contract") != signal.contract or contract.get("fyers_resolved") is not True:
                    reason = "FUT_REJECT_SYMBOL_UNRESOLVED"
                elif contract.get("lot_size") != signal.lot_size:
                    reason = "FUT_REJECT_INVALID_LOT_SIZE"
                elif contract.get("as_of") != date.today().isoformat():
                    reason = "FUT_REJECT_SYMBOL_UNRESOLVED"
                elif not contract.get("expiry") or contract["expiry"] < date.today().isoformat():
                    reason = "FUT_REJECT_SYMBOL_UNRESOLVED"

            if reason is None and data.get("mode") == OPTION2_MODE and data.get("tap"):
                reason = "FUT_REJECT_TAP_CLOSED"
            if reason is None and not all(data.get("gates", {}).get(g) is True for g in GATES):
                reason = "FUT_REJECT_ENABLEMENT_GATES"
            if reason is None and signal.signal_id in data.get("reservations", {}):
                reason = "FUT_REJECT_DUPLICATE_SIGNAL"

            for key, value, code in (("entry", signal.entry, "FUT_REJECT_MISSING_ENTRY"),
                                     ("stop", signal.stop, "FUT_REJECT_MISSING_STOP"),
                                     ("lot", signal.lot_size, "FUT_REJECT_INVALID_LOT_SIZE"),
                                     ("target", signal.target, "FUT_REJECT_INVALID_TARGET")):
                try:
                    units(value) if key == "lot" else number(value, True)
                except ValueError:
                    reason = reason or code

            if reason is None:
                risk = abs(number(signal.entry) - number(signal.stop)) * units(signal.lot_size)
                if signal.side != "BUY" or not number(signal.stop) < number(signal.entry) < number(signal.target):
                    reason = "FUT_REJECT_INVALID_LONG_SIGNAL"
                elif not signal.signal_id:
                    reason = "FUT_REJECT_MISSING_SIGNAL_ID"
                elif signal.signal_id in data.get("consumed", []):
                    reason = "FUT_REJECT_DUPLICATE_SIGNAL"
                elif ((production and not data.get("reconciled"))
                      or (not production and (not self.ready or not data.get("reconciled")))):
                    reason = "FUT_REJECT_STATE_RECONCILIATION_FAILED"
                elif production:
                    cap = Decimal(str(self.option3_policy.single_trade_risk_cap_inr))
                    if risk > cap:
                        reason = "FUT_REJECT_TRADE_RISK_GT_CONFIGURED_CAP"
                else:
                    # Compatibility path for first-pass offline fixtures only.
                    cap = Decimal(str(MAX_SINGLE_FUTURES_TRADE_RISK))
                    if data.get("mode") == OPTION2_MODE:
                        cap_slots = self.dynamic_capacity(data)
                        if cap_slots is None:
                            reason = "FUT_REJECT_DD_BAND_UNDEFINED"
                        elif lots + FUTURES_TIER0_LOTS > cap_slots:
                            reason = "FUT_REJECT_CONCURRENCY_LIMIT"
                        elif risk_before + risk > MAX_OPEN_FUTURES_RISK:
                            reason = "FUT_REJECT_OPEN_RISK_GT_18000"
                    if reason is None and risk > cap:
                        reason = "FUT_REJECT_TRADE_RISK_GT_14000"

            reason = reason or "FUT_ALLOW"
            if reason != "FUT_ALLOW" and data.get("state") != "ERROR_LOCKED":
                if not (data.get("positions") or data.get("reservations")
                        or data.get("active_winner") or data.get("serial_reservation")):
                    data["state"] = self._recompute_state(data)
            self.last_decision = {
                "risk_per_unit": (abs(number(signal.entry) - number(signal.stop))
                                   if risk is not None else None),
                "trade_risk": risk,
                "current_open_futures_risk": risk_before,
                "proposed_open_futures_risk": (risk_before + risk if risk is not None else None),
                "configured_trade_risk_cap": cap,
                "cash_realized_loss": data.get("cash_loss", 0),
                "futures_realized_loss": data.get("futures_loss", 0),
                "combined_realized_loss": (number(data.get("cash_loss", 0))
                                            + number(data.get("futures_loss", 0))),
                "global_loss_ceiling": TOTAL_REALIZED_LOSS_CEILING,
                "global_halt": bool(data.get("halt")),
                "reason_code": reason,
            }
            self._audit(conn, signal, risk=risk, risk_before=risk_before, lots=lots,
                        cap=cap, decision="ALLOW" if reason == "FUT_ALLOW" else "REJECT",
                        reason=reason, data=data)
            if reason == "FUT_ALLOW":
                reservation = dict(entry=str(signal.entry), stop=str(signal.stop), target=str(signal.target),
                                   quantity=int(units(signal.lot_size)), requested_quantity=int(units(signal.lot_size)),
                                   lot_size=int(units(signal.lot_size)), contract=signal.contract,
                                   signal_id=signal.signal_id, timeframe=signal.timeframe,
                                   status="PENDING", cancel_status=None,
                                   created_at=datetime.now(timezone.utc).isoformat())
                data["reservations"][signal.signal_id] = reservation
                data["state"] = "ENTRY_RESERVING" if production else "ENTRY_GTT_RESTING"
            return reason

    def mark_gtt_resting(self, signal_id, *, gtt_id=None, broker_order_id=None):
        with self.store.transaction() as (data, _):
            if signal_id not in data.get("reservations", {}):
                raise ValueError("Unknown futures reservation")
            if data.get("state") not in ("ENTRY_RESERVING", "ENTRY_GTT_RESTING"):
                raise ValueError("Invalid state for resting entry")
            data["reservations"][signal_id].update(gtt_id=gtt_id, broker_order_id=broker_order_id)
            data["reservations"][signal_id]["status"] = "PENDING"
            data.setdefault("broker_orders", {})[signal_id] = {
                "entry_gtt_id": gtt_id, "entry_order_id": broker_order_id,
            }
            data["state"] = "ENTRY_GTT_RESTING"
            data["reconciled"] = True

    def mark_submission_rejected(self, signal_id, *, broker_verified_no_order):
        with self.store.transaction() as (data, _):
            if signal_id not in data.get("reservations", {}):
                raise ValueError("Unknown serial reservation")
            if broker_verified_no_order is not True:
                data["state"] = "ERROR_LOCKED"
                data["reconciled"] = False
                return False
            reservation = data["reservations"].pop(signal_id)
            data.setdefault("reservation_history", {})[signal_id] = {
                **reservation, "status": "BROKER_REJECTED",
                "completed_at": datetime.now(timezone.utc).isoformat(),
            }
            data.setdefault("broker_orders", {}).pop(signal_id, None)
            if data.get("state") != "ERROR_LOCKED":
                data["state"] = self._recompute_state(data)
            return True

    def claim_futures_winner(self, signal_id, *, event_id=None):
        """Atomically claim the first reliable trigger/fill event."""
        with self.store.transaction() as (data, _):
            if event_id and event_id in data.setdefault("events", []):
                return {"status": "DUPLICATE", "winner": data.get("active_winner"), "losers": []}
            reservation = data.get("reservations", {}).get(signal_id)
            if reservation is None:
                raise ValueError("Unknown futures reservation")
            active = data.get("active_winner") or data.get("serial_reservation")
            if active and active != signal_id:
                data["state"] = "ERROR_LOCKED"
                data["reconciled"] = False
                data.setdefault("errors", []).append(
                    "FUTURES_EXPOSURE_BREACH_MULTIPLE_TRIGGERED:%s:%s" % (active, signal_id)
                )
                if event_id:
                    data["events"].append(event_id)
                return {"status": "EXPOSURE_BREACH", "winner": active, "losers": []}
            data["active_winner"] = signal_id
            data["serial_reservation"] = signal_id
            reservation["status"] = "TRIGGERED"
            losers = []
            for other_id, other in data.get("reservations", {}).items():
                if other_id != signal_id and other.get("status", "PENDING") in ("PENDING", "CANCEL_REQUESTED"):
                    other["status"] = "CANCEL_REQUESTED"
                    losers.append({"signal_id": other_id, "gtt_id": other.get("gtt_id")})
            data["state"] = "ENTRY_FILL_PENDING_RECONCILIATION"
            data["cancel_requested"] = bool(losers)
            if event_id:
                data["events"].append(event_id)
            return {"status": "WINNER", "winner": signal_id, "losers": losers}

    def confirm_loser_cancel(self, signal_id, *, broker_verified, cancelled,
                             filled=False, open_position=False):
        """Finalize a loser only after authoritative broker confirmation."""
        if broker_verified is not True:
            self.ready = False
            self.set_error_locked("FUTURES_ENTRY_CANCEL_UNCONFIRMED")
            return False
        with self.store.transaction() as (data, _):
            reservation = data.get("reservations", {}).get(signal_id)
            if reservation is None:
                return True
            if filled or open_position:
                reservation["status"] = "TRIGGERED"
                data["state"] = "ERROR_LOCKED"
                data["reconciled"] = False
                data.setdefault("errors", []).append("FUTURES_EXPOSURE_BREACH_CANCEL_RACE:%s" % signal_id)
                return False
            if cancelled is not True:
                data["state"] = "ERROR_LOCKED"
                data["reconciled"] = False
                return False
            reservation["status"] = "CANCEL_CONFIRMED"
            broker_order = data.setdefault("broker_orders", {}).pop(signal_id, None)
            data["reservations"].pop(signal_id, None)
            data.setdefault("reservation_history", {})[signal_id] = {
                **reservation, "cancel_confirmed_at": datetime.now(timezone.utc).isoformat(),
            }
            if broker_order is not None:
                data["reservation_history"][signal_id]["broker_order"] = broker_order
            data["cancel_requested"] = any(
                r.get("status") == "CANCEL_REQUESTED" for r in data.get("reservations", {}).values()
            )
            return True

    def record_protective_order(self, signal_id, *, gtt_id=None, broker_order_id=None):
        """Persist the existing OMS OCO identifiers for exact exit matching."""
        with self.store.transaction() as (data, _):
            winner = data.get("active_winner") or data.get("serial_reservation")
            if winner != signal_id:
                raise ValueError("Protective order is not for active futures winner")
            data.setdefault("broker_orders", {}).setdefault(signal_id, {}).update(
                protective_gtt_id=gtt_id, protective_order_id=broker_order_id
            )

    def mark_winner_exit_filled(self, signal_id, *, exit_id=None, event_id=None):
        """Record an authoritative winner OCO fill; closure is confirmed separately."""
        with self.store.transaction() as (data, _):
            if event_id and event_id in data.setdefault("events", []):
                return {"status": "DUPLICATE", "winner": signal_id}
            winner = data.get("active_winner") or data.get("serial_reservation")
            if winner != signal_id:
                raise ValueError("Exit does not belong to active futures winner")
            data["winner_exit_confirmed"] = True
            data["winner_exit_signal_id"] = signal_id
            data["winner_exit_event_id"] = event_id
            data["winner_exit_id"] = exit_id
            data["state"] = "EXIT_RECONCILING"
            if event_id:
                data["events"].append(event_id)
            return {"status": "EXIT_RECORDED", "winner": signal_id}

    def resolve_order(self, signal_id, *, filled_quantity, actual_entry, active_stop,
                      terminal, broker_verified, event_id=None):
        """Apply cumulative broker fill quantity idempotently."""
        if broker_verified is not True:
            self.ready = False
            return False
        with self.store.transaction() as (data, _):
            if event_id and event_id in data.setdefault("events", []):
                return True
            reservation = data.get("reservations", {}).get(signal_id)
            if reservation is None:
                raise ValueError("Unknown reservation")
            cumulative = number(filled_quantity)
            if cumulative < 0 or cumulative != cumulative.to_integral_value():
                raise ValueError("Invalid cumulative fill quantity")
            requested = units(reservation.get("requested_quantity", reservation["quantity"]))
            if cumulative > requested:
                data["state"] = "ERROR_LOCKED"
                data["reconciled"] = False
                raise ValueError("Filled quantity exceeds requested one lot")
            if cumulative:
                active = data.get("active_winner") or data.get("serial_reservation")
                if active is None:
                    data["active_winner"] = signal_id
                    data["serial_reservation"] = signal_id
                    reservation["status"] = "TRIGGERED"
                    for other_id, other in data.get("reservations", {}).items():
                        if other_id != signal_id and other.get("status", "PENDING") == "PENDING":
                            other["status"] = "CANCEL_REQUESTED"
                    data["cancel_requested"] = any(
                        other.get("status") == "CANCEL_REQUESTED"
                        for other in data.get("reservations", {}).values()
                    )
                number(actual_entry, True)
                number(active_stop, True)
                position = dict(reservation, entry=str(actual_entry), stop=str(active_stop), quantity=int(cumulative))
                data.setdefault("positions", {})[signal_id] = position
                if signal_id not in data.setdefault("consumed", []):
                    data["consumed"].append(signal_id)
                if active and active != signal_id:
                    data["state"] = "ERROR_LOCKED"
                    data["reconciled"] = False
                    data.setdefault("errors", []).append(
                        "FUTURES_EXPOSURE_BREACH_MULTIPLE_FILLED:%s:%s" % (active, signal_id)
                    )
                else:
                    data["active_winner"] = signal_id
                    data["serial_reservation"] = signal_id
                    data["state"] = "POSITION_OPEN_LOCKED"
            remaining = requested - cumulative
            if terminal or remaining == 0:
                data["reservations"].pop(signal_id, None)
            else:
                reservation["quantity"] = int(remaining)
            data["serial_reservation"] = data.get("active_winner")
            if event_id:
                data["events"].append(event_id)
            if not cumulative and terminal:
                data["state"] = "IDLE"

    def mark_protective_result(self, signal_id, *, success, gtt_id=None):
        with self.store.transaction() as (data, _):
            if signal_id not in data.get("positions", {}):
                raise ValueError("No open position")
            if success is True:
                data.setdefault("broker_orders", {}).setdefault(signal_id, {})["protective_gtt_id"] = gtt_id
                data["state"] = "EXIT_OCO_ACTIVE"
            else:
                data["state"] = "POSITION_OPEN_UNPROTECTED"
                data["reconciled"] = False
                data.setdefault("errors", []).append("PROTECTIVE_OCO_CREATION_FAILED")

    def apply_exit_fill(self, signal_id, *, remaining_quantity, broker_verified, event_id=None):
        if broker_verified is not True:
            self.ready = False
            return False
        with self.store.transaction() as (data, _):
            if event_id and event_id in data.setdefault("events", []):
                return True
            position = data.get("positions", {}).get(signal_id)
            if not position:
                raise ValueError("Unknown open position")
            if data.get("active_winner") != signal_id and data.get("serial_reservation") != signal_id:
                raise ValueError("Exit does not belong to active futures winner")
            # remaining = units(remaining_quantity)
            remaining = nonnegative_units(remaining_quantity)
            if remaining > units(position["quantity"]):
                raise ValueError("Exit quantity exceeds open quantity")
            if remaining:
                position["quantity"] = int(remaining)
            else:
                data["positions"].pop(signal_id)
            data["state"] = "EXIT_RECONCILING"
            if not remaining:
                data["winner_exit_confirmed"] = True
                data["winner_exit_signal_id"] = signal_id
                data["winner_exit_event_id"] = event_id
            if event_id:
                data["events"].append(event_id)

    def confirm_exit_complete(self, *, broker_verified, no_open_position,
                              winner_signal_id=None, exit_event_id=None):
        if broker_verified is not True or no_open_position is not True:
            self.ready = False
            return False
        with self.store.transaction() as (data, _):
            if data.get("state") == "ERROR_LOCKED":
                return False
            winner = data.get("active_winner") or data.get("serial_reservation")
            if winner is not None:
                if winner_signal_id is not None and winner_signal_id != winner:
                    return False
                if not data.get("winner_exit_confirmed"):
                    return False
                if exit_event_id is not None and data.get("winner_exit_event_id") not in (None, exit_event_id):
                    return False
            if data.get("positions"):
                return False
            if any("EXPOSURE_BREACH" in str(error) for error in data.get("errors", [])):
                return False

            terminal = {"CANCEL_CONFIRMED", "REJECTED", "EXPIRED", "CANCELLED"}
            for signal_id, reservation in list(data.get("reservations", {}).items()):
                if signal_id == winner:
                    continue
                if reservation.get("status") in terminal:
                    data.setdefault("reservation_history", {})[signal_id] = {
                        **reservation, "completed_at": datetime.now(timezone.utc).isoformat(),
                    }
                    data["reservations"].pop(signal_id, None)
            data["cancel_requested"] = any(
                item.get("status") in ("PENDING", "CANCEL_REQUESTED", "UNKNOWN")
                for item in data.get("reservations", {}).values()
            )
            if data.get("cancel_requested") or any(
                item.get("status") in ("PENDING", "CANCEL_REQUESTED", "UNKNOWN")
                for item in data.get("reservations", {}).values()
            ):
                data["state"] = "EXIT_RECONCILING"
                return False
            if winner is not None:
                winner_reservation = data.get("reservations", {}).pop(winner, None)
                data.setdefault("reservation_history", {})[winner] = {
                    **(winner_reservation or {}),
                    "status": "COMPLETED", "completed_at": datetime.now(timezone.utc).isoformat(),
                    "winner": True, "exit_event_id": data.get("winner_exit_event_id"),
                    "broker_order": (data.get("broker_orders") or {}).get(winner),
                }
            data["serial_reservation"] = None
            data["active_winner"] = None
            data["winner_exit_confirmed"] = False
            data["winner_exit_signal_id"] = None
            data["winner_exit_event_id"] = None
            data["state"] = "IDLE"
            data["reconciled"] = True
            return True

    def mark_entry_cancel_pending(self, signal_id):
        with self.store.transaction() as (data, _):
            if signal_id not in data.get("reservations", {}):
                raise ValueError("Entry is not resting")
            if signal_id == data.get("active_winner"):
                raise ValueError("Winner entry cannot be cancelled")
            data["reservations"][signal_id]["status"] = "CANCEL_REQUESTED"
            data["cancel_requested"] = True

    def confirm_entry_cancel(self, signal_id, *, broker_verified, filled=False, open_position=False):
        if broker_verified is not True:
            self.ready = False
            return False
        with self.store.transaction() as (data, _):
            if filled or open_position:
                data["state"] = "POSITION_OPEN_LOCKED"
                return False
            reservation = data.get("reservations", {}).pop(signal_id, None)
            if reservation is None:
                return False
            broker_order = data.setdefault("broker_orders", {}).pop(signal_id, None)
            data.setdefault("reservation_history", {})[signal_id] = {
                **reservation, "status": "CANCEL_CONFIRMED",
                "cancel_confirmed_at": datetime.now(timezone.utc).isoformat(),
            }
            if broker_order is not None:
                data["reservation_history"][signal_id]["broker_order"] = broker_order
            data["cancel_requested"] = any(
                r.get("status") == "CANCEL_REQUESTED" for r in data.get("reservations", {}).values()
            )
            if data.get("state") != "ERROR_LOCKED":
                data["state"] = self._recompute_state(data)
            data["reconciled"] = True
            return True

    def global_halt_requires_cancel(self):
        with self.store.transaction() as (data, _):
            self.refresh(data)
            return bool(data.get("halt") and data.get("state") in {
                "ENTRY_RESERVING", "ENTRY_GTT_RESTING", "ENTRY_CANCEL_PENDING"
            })

    def set_error_locked(self, reason=None):
        with self.store.transaction() as (data, _):
            data["state"] = "ERROR_LOCKED"
            data["reconciled"] = False
            if reason:
                data.setdefault("errors", []).append(str(reason))
                logger.error("FUT_ERROR_LOCKED: %s", reason)

    def mark_exit_reconciling(self):
        with self.store.transaction() as (data, _):
            data["state"] = "EXIT_RECONCILING"

    def update_stop(self, signal_id, active_stop):
        number(active_stop, True)
        with self.store.transaction() as (data, _):
            if signal_id not in data.get("positions", {}):
                raise ValueError("Unknown position")
            data["positions"][signal_id]["stop"] = str(active_stop)

    def cash_entry_allowed(self):
        with self.store.transaction() as (data, _):
            self.refresh(data)
            return "GLOBAL_REALIZED_LOSS_CEILING" if data.get("halt") else "CASH_ALLOW"

    def recover_matched_option3_position(self, *, signal_id, position,
                                         broker_identifiers, evidence):
        """Explicitly reconstruct a proven open Option-3 position.

        This is intentionally separate from startup reconciliation.  The
        caller must supply evidence produced by the read-only recovery service;
        this method never contacts or mutates the broker.
        """
        if not signal_id or not broker_identifiers or not evidence.get("exact_id_match"):
            raise ValueError("Exact Option-3 ownership evidence is required")
        with self.store.transaction() as (data, conn):
            existing = data.get("active_winner") or data.get("serial_reservation")
            if existing and existing != signal_id:
                data["state"] = "ERROR_LOCKED"
                data["reconciled"] = False
                raise ValueError("Conflicting active futures winner")
            data.setdefault("positions", {})[signal_id] = dict(position)
            data["active_winner"] = signal_id
            data["serial_reservation"] = signal_id
            data["state"] = "POSITION_OPEN_LOCKED"
            data["reconciled"] = True
            data["broker_state_verified"] = True
            data["recovery_possible"] = False
            data.setdefault("broker_orders", {}).setdefault(signal_id, {}).update(
                **broker_identifiers
            )
            record = {
                "event": "EXPLICIT_OPTION3_POSITION_RECOVERY",
                "signal_id": signal_id,
                "broker_identifiers": broker_identifiers,
                "evidence": evidence,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            data.setdefault("reconciliation_reports", []).append(record)
            self.store.log(conn, record)
        self.ready = True
        return True

    def clear_error_locked_after_recovery(self, *, evidence):
        """Clear ERROR_LOCKED only after backend-derived clean proof."""
        required = ("broker_authoritative", "no_open_positions",
                    "no_unresolved_entry_gtts", "no_unresolved_protective_orders",
                    "no_local_exposure", "no_exposure_breach")
        if any(evidence.get(key) is not True for key in required):
            raise ValueError("Clean recovery evidence is incomplete")
        with self.store.transaction() as (data, conn):
            if data.get("state") != "ERROR_LOCKED":
                return data.get("state") == "IDLE"
            if data.get("positions") or data.get("reservations") or data.get("active_winner") or data.get("serial_reservation"):
                raise ValueError("Durable futures exposure remains")
            if any("EXPOSURE_BREACH" in str(item) for item in data.get("errors", [])):
                raise ValueError("Exposure breach requires separate operator resolution")
            record = {
                "event": "EXPLICIT_ERROR_LOCKED_RECOVERY_TO_IDLE",
                "evidence": evidence,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            data["state"] = "IDLE"
            data["reconciled"] = True
            data["broker_state_verified"] = True
            data["recovery_possible"] = False
            data["last_reconciliation"] = record["timestamp"]
            data.setdefault("reconciliation_reports", []).append(record)
            self.store.log(conn, record)
        self.ready = True
        return True

    def record_realized(self, event_id, segment, pnl, *, cash_loss, futures_loss):
        """Consume owner-supplied authoritative loss totals idempotently."""
        pnl_value = number(pnl)
        if segment not in ("CASH", "FUTURES") or not event_id:
            raise ValueError("Invalid ledger event")
        if min(number(cash_loss), number(futures_loss)) < 0:
            raise ValueError("Negative loss total")
        with self.store.transaction() as (data, _):
            if event_id in data.setdefault("events", []):
                return
            data.update(cash_loss=str(cash_loss), futures_loss=str(futures_loss))
            data["events"].append(event_id)
            self.refresh(data)
            if data.get("mode") == OPTION2_MODE and self.policy.dd_update is not None:
                data["dd"] = str(max(Decimal(0), number(data.get("dd", 0)) - pnl_value))
                self.refresh(data)
            if self.policy.dd_update is None:
                self.ready = False

    def complete_trade(self, row, *, cash_loss, futures_loss, broker_verified):
        from scripts.graduated_live_exports import tracking_error

        tracking_error(row)
        if broker_verified is not True:
            raise ValueError("Broker verification required")
        trade_id = row.get("trade_id")
        signal_id = row.get("signal_id")
        if not trade_id or not signal_id:
            raise ValueError("Trade and signal IDs required")
        number(row.get("realized_pnl"))
        quantity = units(row.get("quantity"))
        if min(number(cash_loss), number(futures_loss)) < 0:
            raise ValueError("Negative loss total")
        key = row["segment"] + ":" + str(trade_id)
        with self.store.transaction() as (data, _):
            if key in data.setdefault("completed", {}):
                if data["completed"][key] != row:
                    raise ValueError("Conflicting completed event")
                return
            if row["segment"] == "FUTURES":
                if signal_id in data.get("reservations", {}):
                    raise ValueError("Pending entry remainder must be resolved first")
                position = data.get("positions", {}).get(signal_id)
                if not position or units(position["quantity"]) != quantity:
                    raise ValueError("Closing quantity does not match position")
                if number(position["entry"]) != number(row["real_entry_price"]):
                    raise ValueError("Actual entry does not match position")
                del data["positions"][signal_id]
            data["completed"][key] = dict(row)
            data.update(cash_loss=str(cash_loss), futures_loss=str(futures_loss))
            self.refresh(data)

    def cancel_unsubmitted(self, signal, reason):
        result = self.release_unsubmitted_reservation(signal.signal_id, reason=reason)
        with self.store.transaction() as (data, conn):
            risk_before, lots = self.exposure(data)
            risk = abs(number(signal.entry) - number(signal.stop)) * units(signal.lot_size)
            self._audit(conn, signal, risk=risk, risk_before=risk_before,
                        lots=lots, cap=None, decision="REJECT", reason=reason, data=data)
        return result
