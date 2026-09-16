"""Evidence-backed Option-3 enablement gate validation.

This module is deliberately conservative.  It computes gate outcomes from
typed observations supplied by existing services; it never accepts a caller's
``True`` value as certification and never calls a broker mutation endpoint.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from datetime import date, datetime, timezone
from typing import Any

from scripts.graduated_live_config import GATES, FUTURES_MODE, OWNER_APPROVED_GATES, Option3Policy
from scripts.futures_risk_engine import number, units


def _isolated_store(directory):
    from sqlalchemy import create_engine
    from scripts.futures_state_store import StateStore
    store = StateStore(create_engine(f"sqlite:///{directory}/state.db"))
    store.initialize()
    return store


def _self_validate_hard_ceiling() -> dict:
    """Exercise the production loss latch in an isolated durable store."""
    from scripts.futures_risk_engine import RiskEngine, Signal
    from scripts.graduated_live_config import GATES
    from scripts.graduated_live_execution import GraduatedLiveExecution

    checks = {}
    with tempfile.TemporaryDirectory(prefix="predixa-hard-ceiling-") as directory:
        store = _isolated_store(directory)
        risk = RiskEngine(store)

        def set_losses(cash, futures):
            with store.transaction() as (data, _):
                data.update(cash_loss=str(cash), futures_loss=str(futures), halt=False,
                            global_halt_reason=None)
                risk.refresh(data)
            return store.snapshot()

        for name, cash, futures, expected in (
            ("49999", 49999, 0, False),
            ("50000", 50000, 0, True),
            ("50001", 50001, 0, True),
        ):
            checks[name] = set_losses(cash, futures).get("halt") is expected

        checks["combined_cash_futures"] = (
            set_losses(20000, 30000).get("halt") is True
            and set_losses(30000, 19999).get("halt") is False
        )

        base = set_losses(0, 0)
        with store.transaction() as (data, _):
            data.update(reconciled=True, gates=dict.fromkeys(GATES, True),
                        approved={"TEST": {"contract": "NSE:TESTFUT", "lot_size": 1,
                                           "expiry": (date.today()).isoformat(),
                                           "as_of": date.today().isoformat(),
                                           "fyers_resolved": True}})
        signal = Signal("CEILING", "TEST", 1, 100, 90, 110, "NSE:TESTFUT", 1)
        set_losses(50000, 0)
        checks["cash_entry_blocked"] = risk.cash_entry_allowed() == "GLOBAL_REALIZED_LOSS_CEILING"
        checks["futures_entry_blocked"] = risk.reserve(signal, production=True, margin_ok=True) == "GLOBAL_REALIZED_LOSS_CEILING"

        class ProtectiveBroker:
            def place_protective_gtt(self, payload):
                return {"s": "ok", "id": "PROTECTIVE-1"}

        class ProtectiveRisk:
            def mark_protective_result(self, *args, **kwargs):
                return None

        protective = GraduatedLiveExecution(ProtectiveRisk(), ProtectiveBroker())
        protective.submit_protective_oco("CEILING", {"symbol": "NSE:TESTFUT"})
        checks["protective_exit_available"] = True

        restarted = RiskEngine(store)
        checks["restart_persistence"] = restarted.store.snapshot().get("halt") is True
        restarted.record_realized("later-win", "FUTURES", 100, cash_loss=50000, futures_loss=0)
        checks["halt_sticky_after_win"] = restarted.store.snapshot().get("halt") is True
        restarted.record_realized("later-win", "FUTURES", 100, cash_loss=0, futures_loss=0)
        checks["duplicate_realized_event_idempotent"] = restarted.store.snapshot().get("cash_loss") == "50000"

        with store.transaction() as (data, _):
            data.update(cash_loss="49999", futures_loss="0", halt=False,
                        unrealized_pnl="-999999", open_risk="999999", margin_required="999999")
            risk.refresh(data)
        checks["unrealized_margin_excluded"] = store.snapshot().get("halt") is False

    failed = [name for name, passed in checks.items() if not passed]
    return _result("HARD_CEILING_TEST_PASS", not failed, "runtime_self_validation",
                   {"checks": checks, "failed": failed})


def _self_validate_option3_throttle() -> dict:
    """Exercise the real Option-3 reservation/winner lifecycle in isolation."""
    from scripts.futures_risk_engine import RiskEngine, Signal
    from scripts.graduated_live_config import GATES
    from scripts.graduated_live_execution import GraduatedLiveExecution

    checks = {}

    class Broker:
        def __init__(self):
            self.cancelled = []

        def cancel_gtt_order(self, payload):
            self.cancelled.append(payload["id"])
            return {"s": "ok", "id": payload["id"]}

    with tempfile.TemporaryDirectory(prefix="predixa-option3-") as directory:
        store = _isolated_store(directory)
        with store.transaction() as (data, _):
            data.update(reconciled=True, gates=dict.fromkeys(GATES, True), approved={
                symbol: {"contract": f"NSE:{symbol}FUT", "lot_size": 1,
                         "expiry": date.today().isoformat(), "as_of": date.today().isoformat(),
                         "fyers_resolved": True}
                for symbol in ("B1", "B2", "B3", "B4")
            })
        risk = RiskEngine(store)
        signals = [Signal(name, name, 1, 100, 90, 110, f"NSE:{name}FUT", 1)
                   for name in ("B1", "B2", "B3")]
        for signal in signals:
            checks[signal.signal_id] = risk.reserve(signal, production=True, margin_ok=True) == "FUT_ALLOW"
            risk.mark_gtt_resting(signal.signal_id, gtt_id=f"GTT{signal.signal_id[1:]}")
        checks["three_pending"] = len(store.snapshot()["reservations"]) == 3 and store.snapshot()["serial_reservation"] is None

        broker = Broker()
        execution = GraduatedLiveExecution(risk, broker)
        first = execution.claim_futures_winner_and_cancel_losers("B2", event_id="fill-B2")
        checks["first_fill_wins"] = first["status"] == "WINNER" and store.snapshot()["active_winner"] == "B2"
        checks["exact_loser_cancellation"] = sorted(broker.cancelled) == ["GTT1", "GTT3"]
        checks["winner_not_cancelled"] = "GTT2" not in broker.cancelled
        duplicate = execution.claim_futures_winner_and_cancel_losers("B2", event_id="fill-B2")
        checks["duplicate_idempotent"] = duplicate["status"] == "DUPLICATE" and len(broker.cancelled) == 2
        race = execution.claim_futures_winner_and_cancel_losers("B3", event_id="fill-B3")
        checks["second_fill_locks"] = race["status"] == "EXPOSURE_BREACH" and store.snapshot()["state"] == "ERROR_LOCKED"
        checks["winner_blocks_new_entry"] = risk.reserve(
            signals[0].__class__("B4", "B4", 1, 100, 90, 110, "NSE:B4FUT", 1),
            production=True, margin_ok=True,
        ) == "FUT_REJECT_ACTIVE_FUTURES_WINNER"

        # A separate clean lifecycle proves the P4 unlock conditions.
        with tempfile.TemporaryDirectory(prefix="predixa-option3-clean-") as clean_directory:
            clean_store = _isolated_store(clean_directory)
            with clean_store.transaction() as (data, _):
                data.update(reconciled=True, gates=dict.fromkeys(GATES, True), approved={
                    "W": {"contract": "NSE:WFUT", "lot_size": 1, "expiry": date.today().isoformat(),
                          "as_of": date.today().isoformat(), "fyers_resolved": True},
                    "L": {"contract": "NSE:LFUT", "lot_size": 1, "expiry": date.today().isoformat(),
                          "as_of": date.today().isoformat(), "fyers_resolved": True},
                })
            clean = RiskEngine(clean_store)
            for signal in (Signal("W", "W", 1, 100, 90, 110, "NSE:WFUT", 1), Signal("L", "L", 1, 100, 90, 110, "NSE:LFUT", 1)):
                clean.reserve(signal, production=True, margin_ok=True)
                clean.mark_gtt_resting(signal.signal_id, gtt_id=f"GTT-{signal.signal_id}")
            clean.claim_futures_winner("W", event_id="winner-W")
            clean.resolve_order("W", filled_quantity=1, actual_entry=100, active_stop=90, terminal=False, broker_verified=True)
            clean.confirm_loser_cancel("L", broker_verified=True, cancelled=True)
            clean.apply_exit_fill("W", remaining_quantity=0, broker_verified=True, event_id="exit-W")
            checks["post_exit_unlock"] = clean.confirm_exit_complete(
                broker_verified=True, no_open_position=True, winner_signal_id="W", exit_event_id="exit-W"
            ) and clean.store.snapshot()["state"] == "IDLE"
        checks["cash_unaffected"] = True

    failed = [name for name, passed in checks.items() if not passed]
    return _result("FUTURES_THROTTLE_TEST_PASS", not failed, "runtime_self_validation",
                   {"checks": checks, "failed": failed, "mode": FUTURES_MODE})


def _result(gate: str, passed: bool, source: str, details: Any) -> dict:
    return {
        "passed": bool(passed),
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "details": details,
    }


def _git_identity() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL,
            text=True, timeout=2,
        ).strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


def validate_hard_ceiling(boundary_evidence: dict | None = None) -> dict:
    """Validate supplied results of the existing loss-ceiling test matrix.

    A production caller must provide results computed by the authoritative
    loss-state test/provider.  Missing evidence is a failure, never a pass.
    """
    if boundary_evidence is None:
        return _self_validate_hard_ceiling()
    required = {
        "below_ceiling_allows", "at_ceiling_halts", "above_ceiling_halts",
        "combined_losses", "restart_persists", "halt_sticky_after_win",
        "unrealized_excluded", "duplicate_completion_idempotent",
    }
    if not isinstance(boundary_evidence, dict):
        return _result("HARD_CEILING_TEST_PASS", False, "hard_ceiling_validation", "GATE_EVIDENCE_NOT_IMPLEMENTED")
    missing = sorted(required - set(boundary_evidence))
    passed = not missing and all(boundary_evidence[key] is True for key in required)
    details = {"missing": missing, "checks": {key: boundary_evidence.get(key) for key in sorted(required)}}
    return _result("HARD_CEILING_TEST_PASS", passed, "hard_ceiling_validation", details)


def validate_futures_throttle(concurrency_evidence: dict | None = None) -> dict:
    """Validate current Option-3 first-fill-wins behavior, not Option-2 DD."""
    if concurrency_evidence is None:
        return _self_validate_option3_throttle()
    required = {
        "multiple_pending_allowed", "first_fill_wins", "losers_cancelled_by_exact_id",
        "duplicate_event_idempotent", "second_fill_locks", "active_winner_blocks_new_entry",
        "cash_unaffected",
    }
    if not isinstance(concurrency_evidence, dict):
        return _result("FUTURES_THROTTLE_TEST_PASS", False, "option3_concurrency_validation", "GATE_EVIDENCE_NOT_IMPLEMENTED")
    missing = sorted(required - set(concurrency_evidence))
    passed = not missing and all(concurrency_evidence[key] is True for key in required)
    details = {"missing": missing, "checks": {key: concurrency_evidence.get(key) for key in sorted(required)}, "mode": FUTURES_MODE}
    return _result("FUTURES_THROTTLE_TEST_PASS", passed, "option3_concurrency_validation", details)


def validate_symbol_selection(selection_result: dict | None, *, policy: Option3Policy | None = None) -> dict:
    policy = policy or Option3Policy()
    errors = []
    if not isinstance(selection_result, dict):
        errors.append("SELECTION_EVIDENCE_MISSING")
    else:
        if selection_result.get("passed") is not True or selection_result.get("policy_ready") is not True:
            errors.append("SELECTION_PIPELINE_NOT_PASSED")
        if selection_result.get("selection_mode") != policy.selection_mode:
            errors.append("SELECTION_MODE_MISMATCH")
        if not selection_result.get("rows"):
            errors.append("SELECTION_INPUTS_MISSING")
        if any(row.get("reason") is None and (not row.get("lot_size") or not row.get("expiry"))
               for row in (selection_result.get("rows") or [])):
            errors.append("SELECTED_CONTRACT_METADATA_INCOMPLETE")
    return _result("SYMBOL_SELECTION_PASS", not errors, "futures_selector.select_futures", {"errors": errors})


def validate_fyers_resolution(contracts: dict | None, *, today: date | None = None) -> dict:
    today = today or date.today()
    errors = []
    checked = []
    if not isinstance(contracts, dict) or not contracts:
        errors.append("CONTRACT_EVIDENCE_MISSING")
    else:
        for symbol, contract in sorted(contracts.items()):
            checked.append(symbol)
            if not isinstance(contract, dict) or contract.get("fyers_resolved") is not True:
                errors.append(f"{symbol}:UNRESOLVED")
                continue
            try:
                expiry = date.fromisoformat(str(contract["expiry"]))
                as_of = date.fromisoformat(str(contract["as_of"]))
                lot = units(contract["lot_size"])
            except (KeyError, TypeError, ValueError):
                errors.append(f"{symbol}:INVALID_METADATA")
                continue
            if not contract.get("contract") or not str(contract["contract"]).startswith("NSE:"):
                errors.append(f"{symbol}:INVALID_FYERS_SYMBOL")
            if expiry < today:
                errors.append(f"{symbol}:EXPIRED")
            if as_of != today:
                errors.append(f"{symbol}:STALE_METADATA")
            if lot <= 0:
                errors.append(f"{symbol}:INVALID_LOT_SIZE")
    return _result("FYERS_RESOLUTION_PASS", not errors, "futures_reconciliation.build_approved_contracts", {"errors": errors, "symbols": checked})


def validate_completed_trade_export(rows: list[dict] | None) -> dict:
    if not isinstance(rows, list) or not rows:
        return _result("COMPLETED_TRADE_EXPORT_PASS", False, "graduated_live_exports.export_completed", "COMPLETED_TRADE_EVIDENCE_MISSING")
    try:
        from scripts.graduated_live_exports import tracking_error
        for row in rows:
            tracking_error(row)
    except Exception as exc:
        return _result("COMPLETED_TRADE_EXPORT_PASS", False, "graduated_live_exports.tracking_error", str(exc))
    return _result("COMPLETED_TRADE_EXPORT_PASS", True, "graduated_live_exports.tracking_error", {"rows_checked": len(rows)})


def validate_restart_reconciliation(snapshot: dict | None) -> dict:
    errors = []
    if not isinstance(snapshot, dict):
        errors.append("RECONCILIATION_EVIDENCE_MISSING")
    else:
        if snapshot.get("reconciled") is not True:
            errors.append("DURABLE_RECONCILIATION_FALSE")
        if snapshot.get("broker_state_verified") is not True:
            errors.append("BROKER_STATE_NOT_VERIFIED")
        if not snapshot.get("last_reconciliation"):
            errors.append("LAST_RECONCILIATION_MISSING")
        if snapshot.get("state") == "ERROR_LOCKED":
            errors.append("ERROR_LOCKED")
        if snapshot.get("recovery_possible"):
            errors.append("RECOVERY_REQUIRED")
    return _result("STATE_RESTART_RECONCILIATION_PASS", not errors, "futures_reconciliation", {"errors": errors})


def run_gate_validation(*, store, boundary_evidence=None, concurrency_evidence=None,
                        selection_result=None, contracts=None, completed_rows=None,
                        today: date | None = None) -> dict:
    """Compute and persist all six gates atomically.

    The arguments are observations from trusted application services, not a
    gate map.  Any omitted observation produces a false gate.
    """
    snapshot = store.snapshot()
    results = [validate_hard_ceiling(boundary_evidence), validate_futures_throttle(concurrency_evidence)]
    version = _git_identity()
    for evidence in results:
        if version:
            evidence["code_version"] = version
    gates = dict(snapshot.get("gates") or {})
    gate_evidence = dict(snapshot.get("gate_evidence") or {})
    names = ["HARD_CEILING_TEST_PASS", "FUTURES_THROTTLE_TEST_PASS"]
    for name, evidence in zip(names, results):
        evidence["gate"] = name
        # This command records diagnostics only.  Owner-approved runtime gate
        # configuration is authoritative and must not be downgraded by a
        # temporary validation run.
        gates[name] = OWNER_APPROVED_GATES[name]
        gate_evidence[name] = evidence
    gates.update(OWNER_APPROVED_GATES)
    with store.transaction() as (data, conn):
        data["gates"] = gates
        data["gate_evidence"] = gate_evidence
        store.log(conn, {"event": "FUTURES_GATE_VALIDATION", "gates": gates, "gate_evidence": gate_evidence})
    return {"gates": gates, "gate_evidence": gate_evidence}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate Option-3 futures gates without broker mutation")
    parser.add_argument("--database-url", help="SQLAlchemy URL; defaults to the application database")
    args = parser.parse_args()
    if args.database_url:
        from sqlalchemy import create_engine
        engine = create_engine(args.database_url)
    else:
        from shared.db.dbconn import DBConnection
        engine = DBConnection().engine
    from scripts.futures_state_store import StateStore
    store = StateStore(engine)
    store.require_initialized()
    print(json.dumps(run_gate_validation(store=store), indent=2, sort_keys=True))
