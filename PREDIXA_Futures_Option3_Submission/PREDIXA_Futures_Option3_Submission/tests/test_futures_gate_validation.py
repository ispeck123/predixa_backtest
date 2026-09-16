from datetime import date, timedelta

from sqlalchemy import create_engine

from scripts.futures_gate_validation import (
    run_gate_validation,
    validate_completed_trade_export,
    validate_futures_throttle,
    validate_fyers_resolution,
    validate_hard_ceiling,
    validate_restart_reconciliation,
    validate_symbol_selection,
)
from scripts.futures_state_store import StateStore
from scripts.graduated_live_config import GATES


def store(tmp_path):
    result = StateStore(create_engine(f"sqlite:///{tmp_path / 'state.db'}"))
    result.initialize()
    return result


def test_hard_ceiling_requires_all_boundary_evidence():
    evidence = {
        "below_ceiling_allows": True, "at_ceiling_halts": True,
        "above_ceiling_halts": True, "combined_losses": True,
        "restart_persists": True, "halt_sticky_after_win": True,
        "unrealized_excluded": True, "duplicate_completion_idempotent": True,
    }
    assert validate_hard_ceiling(evidence)["passed"] is True
    evidence.pop("combined_losses")
    assert validate_hard_ceiling(evidence)["passed"] is False


def test_option3_throttle_is_not_option2_dd():
    evidence = {key: True for key in (
        "multiple_pending_allowed", "first_fill_wins", "losers_cancelled_by_exact_id",
        "duplicate_event_idempotent", "second_fill_locks", "active_winner_blocks_new_entry",
        "cash_unaffected",
    )}
    result = validate_futures_throttle(evidence)
    assert result["passed"] is True
    assert result["details"]["mode"] == "OPTION3_SERIAL"


def test_symbol_selection_and_resolution_require_real_metadata():
    selection = {
        "passed": True, "policy_ready": True, "selection_mode": "PER_LOT_RISK",
        "rows": [{"reason": None, "lot_size": 6750, "expiry": "2026-09-29"}],
    }
    assert validate_symbol_selection(selection)["passed"] is True
    contracts = {"CANBK": {
        "contract": "NSE:CANBK26SEPFUT", "lot_size": 6750,
        "expiry": (date.today() + timedelta(days=5)).isoformat(),
        "as_of": date.today().isoformat(), "fyers_resolved": True,
    }}
    assert validate_fyers_resolution(contracts)["passed"] is True
    contracts["CANBK"]["fyers_resolved"] = False
    assert validate_fyers_resolution(contracts)["passed"] is False


def test_completed_export_and_restart_are_fail_closed():
    row = {
        "segment": "FUTURES", "symbol": "CANBK", "tf": 1, "side": "BUY",
        "zone_type": "DEMAND", "entry_price": 100, "stoploss_price": 95,
        "target_price": 110, "real_entry_price": 100, "real_exit_price": 110,
        "applicable_stop": 95, "exit_leg": "TARGET", "status": "COMPLETED",
    }
    assert validate_completed_trade_export([row])["passed"] is True
    assert validate_completed_trade_export([dict(row, real_exit_price=None)])["passed"] is False
    assert validate_restart_reconciliation({"reconciled": True, "broker_state_verified": True,
                                            "last_reconciliation": "now", "state": "IDLE",
                                            "recovery_possible": False})["passed"] is True
    assert validate_restart_reconciliation({"state": "ERROR_LOCKED", "reconciled": True,
                                            "broker_state_verified": True, "last_reconciliation": "now"})["passed"] is False


def test_run_persists_every_gate_with_matching_evidence(tmp_path):
    target_store = store(tmp_path)
    with target_store.transaction() as (data, _):
        data["gates"] = {
            "SYMBOL_SELECTION_PASS": False,
            "FYERS_RESOLUTION_PASS": "unchanged",
            "COMPLETED_TRADE_EXPORT_PASS": False,
            "STATE_RESTART_RECONCILIATION_PASS": False,
        }
        data["gate_evidence"] = {"FYERS_RESOLUTION_PASS": {"source": "prior"}}
    result = run_gate_validation(store=target_store)
    snapshot = StateStore(create_engine(f"sqlite:///{tmp_path / 'state.db'}"))
    snapshot.require_initialized()
    data = snapshot.snapshot()
    assert result["gates"]["HARD_CEILING_TEST_PASS"] is True
    assert result["gates"]["FUTURES_THROTTLE_TEST_PASS"] is True
    assert data["gates"]["SYMBOL_SELECTION_PASS"] is True
    assert data["gates"]["FYERS_RESOLUTION_PASS"] is True
    assert data["gate_evidence"]["FYERS_RESOLUTION_PASS"] == {"source": "prior"}
    assert data["gates"]["COMPLETED_TRADE_EXPORT_PASS"] is True
    assert data["gates"]["STATE_RESTART_RECONCILIATION_PASS"] is True
    assert data["gate_evidence"]["HARD_CEILING_TEST_PASS"]["passed"] is True
    assert data["gate_evidence"]["FUTURES_THROTTLE_TEST_PASS"]["passed"] is True


def test_owner_approved_gate_bootstrap_syncs_all_gates_without_exposure_changes(tmp_path):
    target_store = store(tmp_path)
    with target_store.transaction() as (data, _):
        data.update(state="IDLE", reconciled=True, positions={}, reservations={},
                    active_winner=None, serial_reservation=None)
    target_store.sync_owner_approved_gates()
    data = target_store.snapshot()
    assert data["gates"] == {gate: True for gate in GATES}
    assert data["state"] == "IDLE"
    assert data["reconciled"] is True
    assert data["positions"] == {}
    assert data["reservations"] == {}
