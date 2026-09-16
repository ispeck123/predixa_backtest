"""Read-only startup/recovery reconciliation for graduated-live futures.

This module has no broker mutation calls.  A successful run is the only code
path that marks the durable futures state as reconciled and ready for approval.
"""
from __future__ import annotations

from datetime import date, datetime

from scripts.futures_risk_engine import RiskEngine
from scripts.graduated_live_config import FUTURES_LOT_SIZES


STATUS_MAP = {
    1: "CANCEL_CONFIRMED",
    2: "TRIGGERED",
    5: "REJECTED",
    6: "PENDING",
}


def _response_list(response, key):
    if not isinstance(response, dict) or response.get("s") not in (None, "ok"):
        raise ValueError("FYERS read response is not verified")
    values = response.get(key)
    if not isinstance(values, list):
        raise ValueError("FYERS read response is malformed")
    return values


def _read(broker, method, raw_method):
    reader = getattr(broker, method, None)
    if callable(reader):
        return reader()
    client = getattr(broker, "_client", None)
    raw_reader = getattr(client, raw_method, None)
    if not callable(raw_reader):
        raise ValueError("FYERS read operation is unavailable")
    return raw_reader()


def _broker_gtts(broker):
    return _response_list(_read(broker, "read_gtt_orderbook", "gtt_orderbook"), "orderBook")


def _broker_positions(broker):
    return _response_list(_read(broker, "read_positions", "positions"), "netPositions")


def _open_broker_positions(rows):
    result = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("Malformed broker position")
        symbol = str(row.get("symbol", "")).upper()
        product = str(row.get("productType", row.get("product_type", ""))).upper()
        # FYERS positions() also returns cash CNC positions.  They are not
        # futures exposure and must never affect Option-3 recovery.
        if not symbol.endswith("FUT") or product in {"CNC", "INTRADAY"}:
            continue
        raw_qty = row.get("netQty", row.get("qty", row.get("quantity", 0)))
        try:
            quantity = int(raw_qty or 0)
        except (TypeError, ValueError):
            raise ValueError("Malformed broker position quantity") from None
        if quantity:
            result.append((row, quantity))
    return result


def _gtt_id(reservation_id, reservation, broker_orders):
    return reservation.get("gtt_id") or (broker_orders.get(reservation_id) or {}).get("entry_gtt_id")


def _reservation_statuses(data, gtt_rows):
    by_id = {str(row.get("id")): row for row in gtt_rows if row.get("id") is not None}
    statuses = {}
    for reservation_id, reservation in (data.get("reservations") or {}).items():
        gtt_id = _gtt_id(reservation_id, reservation, data.get("broker_orders") or {})
        if not gtt_id or str(gtt_id) not in by_id:
            raise ValueError("Persisted futures GTT is missing from broker snapshot")
        row = by_id[str(gtt_id)]
        try:
            status = STATUS_MAP[int(row.get("ord_status"))]
        except (KeyError, TypeError, ValueError):
            raise ValueError("Persisted futures GTT has unknown broker status") from None
        statuses[reservation_id] = status
    return statuses


def build_approved_contracts(session, symbols, *, as_of=None):
    """Resolve contracts from current FuturesMaster and existing OMS rows.

    No expiry or quantity is invented: each entry must have a current
    FuturesMaster expiry and the configured one-lot quantity.
    """
    from shared.db.db_model import FuturesMaster, OMSOrderBucketFuture

    as_of = as_of or date.today()
    contracts = {}
    for symbol in sorted({str(value).strip().upper() for value in symbols if value}):
        rows = session.query(OMSOrderBucketFuture).filter(
            OMSOrderBucketFuture.stock_tick == symbol,
            OMSOrderBucketFuture.approval_status.in_(("pending", "approved")),
        ).all()
        if not rows:
            continue
        row = rows[-1]
        expiry = str(row.expiry_date or "")
        parsed = None
        for fmt in ("%d%m%Y", "%Y-%m-%d", "%d-%m-%Y"):
            try:
                parsed = datetime.strptime(expiry, fmt).date()
                break
            except ValueError:
                pass
        if parsed is None or parsed < as_of:
            continue
        master = session.query(FuturesMaster).filter(
            FuturesMaster.symbol == symbol,
            FuturesMaster.expiry_date == parsed,
            FuturesMaster.exchange == "NSEFO",
        ).first()
        lot_size = FUTURES_LOT_SIZES.get(symbol)
        if master is None or lot_size is None or int(row.stock_quantity or 0) != int(lot_size):
            continue
        contracts[symbol] = {
            "contract": f"NSE:{symbol}{parsed.strftime('%y%b').upper()}FUT",
            "lot_size": int(lot_size),
            "expiry": parsed.isoformat(),
            "fyers_resolved": True,
            "as_of": as_of.isoformat(),
        }
    return contracts


def reconcile_futures_startup(*, risk: RiskEngine, broker, approved, gates,
                              source_verified=True, as_of=None):
    """Read FYERS positions/GTTs and persist a verified P2-compatible state."""
    if source_verified is not True:
        return False
    try:
        gtt_rows = _broker_gtts(broker)
        position_rows = _broker_positions(broker)
        data = risk.store.snapshot()
        statuses = _reservation_statuses(data, gtt_rows)
        open_positions = _open_broker_positions(position_rows)
        active = data.get("active_winner") or data.get("serial_reservation")
        positions = data.get("positions") or {}
        if active:
            reservation = (data.get("reservations") or {}).get(active)
            expected_contract = (reservation or positions.get(active) or {}).get("contract")
            matches = [row for row, _ in open_positions if row.get("symbol") == expected_contract]
            if len(matches) != 1 or len(open_positions) != 1:
                raise ValueError("Active winner broker exposure is unresolved")
        elif open_positions:
            raise ValueError("Broker futures exposure has no durable Option-3 winner")

        return risk.reconcile_verified_state(
            positions=positions if active else {},
            reservation_statuses=statuses,
            approved=approved,
            gates=gates,
            dd=data.get("dd", "0"),
            cash_loss=data.get("cash_loss", "0"),
            futures_loss=data.get("futures_loss", "0"),
            source_verified=True,
        )
    except Exception as exc:
        try:
            risk.store.mark_error(str(exc))
        except Exception:
            pass
        return False


def _position_identifier_values(row):
    keys = ("id", "orderId", "order_id", "gtt_id", "gttId",
            "parentId", "parent_id", "id_fyers", "fyersOrderId")
    return {str(row[key]) for key in keys if row.get(key) not in (None, "")}


def _known_ownership(data, session=None):
    known = {}

    def add(identifier, owner):
        if identifier not in (None, ""):
            known[str(identifier)] = owner

    for signal_id, item in (data.get("broker_orders") or {}).items():
        for key in ("entry_gtt_id", "entry_order_id", "protective_gtt_id", "protective_order_id"):
            add(item.get(key), {"signal_id": str(signal_id), "source": "broker_orders"})
    for collection_name in ("reservations", "reservation_history"):
        for signal_id, item in (data.get(collection_name) or {}).items():
            for key in ("gtt_id", "broker_order_id", "protective_gtt_id", "protective_order_id"):
                add(item.get(key), {"signal_id": str(signal_id), "source": collection_name})

    if session is not None:
        from shared.db.db_model import OMSOrderBucketFuture
        for row in session.query(OMSOrderBucketFuture).all():
            owner = {
                "signal_id": str(row.trade_signal_id) if row.trade_signal_id is not None else None,
                "bucket_id": row.bucket_id,
                "symbol": row.stock_tick,
                "entry": row.entry_price,
                "stop": row.stoploss_price,
                "target": row.target_price,
                "quantity": row.stock_quantity,
                "expiry": row.expiry_date,
                "source": "OMSOrderBucketFuture",
            }
            add(row.gtt_id, owner)
            add(row.id_fyers, owner)
    return known


def _diagnostic_position(row, classification, owner=None):
    allowed = ("symbol", "positionId", "position_id", "id", "orderId", "order_id",
                "gttId", "gtt_id", "id_fyers", "fyersOrderId", "qty", "netQty",
                "quantity", "side", "buyQty", "sellQty", "productType", "product_type",
                "avgPrice", "averagePrice", "pl", "realized", "unrealized")
    diagnostic = {key: row.get(key) for key in allowed if key in row}
    diagnostic["classification"] = classification
    if owner:
        diagnostic["owner_signal_id"] = owner.get("signal_id")
        diagnostic["owner_bucket_id"] = owner.get("bucket_id")
        diagnostic["owner_source"] = owner.get("source")
    return diagnostic


def diagnose_futures_exposure(*, risk, broker, session=None):
    """Classify broker futures exposure using exact persisted identifiers first."""
    gtt_rows = _broker_gtts(broker)
    position_rows = _broker_positions(broker)
    data = risk.store.snapshot()
    known = _known_ownership(data, session)
    open_positions = _open_broker_positions(position_rows)
    diagnostics = []
    matched = []
    local_contracts = {
        item.get("contract") for collection in ("reservations", "positions", "reservation_history")
        for item in (data.get(collection) or {}).values() if item.get("contract")
    }
    for row, quantity in open_positions:
        identifiers = _position_identifier_values(row)
        owner = next((known[value] for value in identifiers if value in known), None)
        if owner:
            classification = "MATCHED_OPTION3_POSITION"
            matched.append((row, quantity, owner))
        elif identifiers:
            classification = "UNRELATED_OR_MANUAL_FUTURES_POSITION"
        elif row.get("symbol") in local_contracts:
            classification = "AMBIGUOUS_FUTURES_POSITION"
        else:
            classification = "UNRELATED_OR_MANUAL_FUTURES_POSITION"
        diagnostics.append(_diagnostic_position(row, classification, owner))

    unresolved_entries = []
    by_gtt_id = {str(row.get("id")): row for row in gtt_rows if row.get("id") is not None}
    for signal_id, reservation in (data.get("reservations") or {}).items():
        gtt_id = _gtt_id(signal_id, reservation, data.get("broker_orders") or {})
        broker_row = by_gtt_id.get(str(gtt_id)) if gtt_id else None
        status = None
        if broker_row is not None:
            try:
                status = STATUS_MAP.get(int(broker_row.get("ord_status")))
            except (TypeError, ValueError):
                status = None
        if not gtt_id or status not in {"CANCEL_CONFIRMED", "REJECTED"}:
            unresolved_entries.append({"signal_id": signal_id, "gtt_id": gtt_id, "status": status})

    unresolved_protective = []
    for signal_id, broker_order in (data.get("broker_orders") or {}).items():
        protective_id = broker_order.get("protective_gtt_id")
        if not protective_id:
            continue
        row = by_gtt_id.get(str(protective_id))
        if row is None:
            unresolved_protective.append({"signal_id": signal_id, "gtt_id": protective_id, "status": "MISSING"})
            continue
        try:
            status = STATUS_MAP.get(int(row.get("ord_status")))
        except (TypeError, ValueError):
            status = None
        if status not in {"CANCEL_CONFIRMED", "REJECTED"}:
            unresolved_protective.append({"signal_id": signal_id, "gtt_id": protective_id, "status": status})

    if not diagnostics:
        classification = "NO_BROKER_FUTURES_EXPOSURE"
    elif len(matched) == len(diagnostics) and len(matched) == 1:
        classification = "MATCHED_OPTION3_POSITION"
    elif any(item["classification"] == "AMBIGUOUS_FUTURES_POSITION" for item in diagnostics):
        classification = "AMBIGUOUS_FUTURES_POSITION"
    else:
        classification = "UNRELATED_OR_MANUAL_FUTURES_POSITION"
    return {
        "classification": classification,
        "positions": diagnostics,
        "matched": matched,
        "unresolved_entry_gtts": unresolved_entries,
        "unresolved_protective_orders": unresolved_protective,
        "broker_authoritative": True,
    }


def _record_recovery_report(risk, report):
    with risk.store.transaction() as (data, conn):
        data.setdefault("reconciliation_reports", []).append(report)
        data["broker_state_verified"] = True
        data["reconciled"] = False
        data["state"] = "ERROR_LOCKED"
        risk.store.log(conn, report)


def recover_error_locked_state(*, risk, broker, session=None):
    """Explicit operator recovery; broker state is always read internally."""
    report = diagnose_futures_exposure(risk=risk, broker=broker, session=session)
    report["timestamp"] = datetime.utcnow().isoformat() + "Z"
    report["recovery_operation"] = "EXPLICIT_ERROR_LOCKED_RECOVERY"

    if report["classification"] == "MATCHED_OPTION3_POSITION":
        if len(report["matched"]) != 1 or report["unresolved_entry_gtts"] or report["unresolved_protective_orders"]:
            _record_recovery_report(risk, report)
            return {**report, "result": "BLOCKED_AMBIGUOUS_EXPOSURE"}
        row, quantity, owner = report["matched"][0]
        if (quantity <= 0 or not owner.get("signal_id")
                or owner.get("stop") is None or owner.get("target") is None
                or owner.get("quantity") is None):
            _record_recovery_report(risk, report)
            return {**report, "result": "BLOCKED_AMBIGUOUS_EXPOSURE"}
        position = {
            "signal_id": owner["signal_id"],
            "contract": row.get("symbol"),
            "entry": str(row.get("averagePrice", row.get("avgPrice", owner.get("entry")))),
            "stop": str(owner.get("stop")),
            "target": str(owner.get("target")),
            "quantity": quantity,
            "lot_size": int(owner.get("quantity") or quantity),
        }
        identifiers = {"entry_order_id": row.get("orderId") or row.get("order_id"),
                       "entry_gtt_id": row.get("gttId") or row.get("gtt_id")}
        identifiers = {key: value for key, value in identifiers.items() if value not in (None, "")}
        risk.recover_matched_option3_position(
            signal_id=owner["signal_id"], position=position,
            broker_identifiers=identifiers,
            evidence={"exact_id_match": True, "diagnostic": report["positions"][0]},
        )
        return {**report, "result": "RECOVERED_ACTIVE_WINNER"}

    data = risk.store.snapshot()
    orphan_reservations = [
        item for item in report["unresolved_entry_gtts"]
        if not item.get("gtt_id")
        and item.get("signal_id") in (data.get("reservations") or {})
        and not (data.get("broker_orders") or {}).get(item.get("signal_id"))
    ]
    # A reservation created before submission may have no broker ID at all.
    # If the authoritative broker snapshot contains no position/GTT, archive
    # that local-only reservation instead of treating it as an open order.
    if (report["classification"] == "NO_BROKER_FUTURES_EXPOSURE"
            and len(orphan_reservations) == len(report["unresolved_entry_gtts"])
            and not report["unresolved_protective_orders"]):
        with risk.store.transaction() as (state, conn):
            for item in orphan_reservations:
                signal_id = item["signal_id"]
                reservation = state.get("reservations", {}).pop(signal_id, None)
                if reservation is not None:
                    state.setdefault("reservation_history", {})[signal_id] = {
                        **reservation,
                        "status": "BROKER_VERIFIED_NO_ORDER",
                        "completed_at": datetime.utcnow().isoformat() + "Z",
                    }
            state["cancel_requested"] = False
        report["orphan_reservations_archived"] = [item["signal_id"] for item in orphan_reservations]
        report["unresolved_entry_gtts"] = []
        data = risk.store.snapshot()
    clean = (
        report["classification"] == "NO_BROKER_FUTURES_EXPOSURE"
        and not report["unresolved_entry_gtts"]
        and not report["unresolved_protective_orders"]
        and not data.get("positions") and not data.get("reservations")
        and not data.get("active_winner") and not data.get("serial_reservation")
        and not any("EXPOSURE_BREACH" in str(item) for item in data.get("errors", []))
    )
    if clean:
        evidence = {
            "broker_authoritative": True, "no_open_positions": True,
            "no_unresolved_entry_gtts": True, "no_unresolved_protective_orders": True,
            "no_local_exposure": True, "no_exposure_breach": True,
        }
        risk.clear_error_locked_after_recovery(evidence=evidence)
        return {**report, "result": "RECOVERED_TO_IDLE"}

    result = "BLOCKED_AMBIGUOUS_EXPOSURE" if report["classification"] == "AMBIGUOUS_FUTURES_POSITION" else "BLOCKED_EXTERNAL_EXPOSURE"
    _record_recovery_report(risk, report)
    return {**report, "result": result}
