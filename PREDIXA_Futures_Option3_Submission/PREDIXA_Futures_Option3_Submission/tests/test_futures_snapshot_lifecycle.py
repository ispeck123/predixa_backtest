from datetime import date, timedelta

from sqlalchemy import create_engine

from scripts.futures_risk_engine import RiskEngine, Signal
from scripts.futures_state_store import StateStore
from scripts.graduated_live_config import GATES


def _engine(tmp_path):
    store = StateStore(create_engine("sqlite:///" + str(tmp_path / "lifecycle.sqlite")))
    store.initialize()
    today = date.today()
    assert RiskEngine(store).reconcile(
        {}, dd=0, cash_loss=0, futures_loss=0,
        approved={"CANBK": {
            "contract": "NSE:CANBK26SEPFUT", "lot_size": 6750,
            "fyers_resolved": True, "as_of": today.isoformat(),
            "expiry": (today + timedelta(days=30)).isoformat(),
        }}, gates=dict.fromkeys(GATES, True), unresolved_orders=[],
        source_verified=True,
    )
    return RiskEngine(store), store


def _signal():
    return Signal("S1", "CANBK", 1, 100, 99, 110,
                   "NSE:CANBK26SEPFUT", 6750)


def test_definitive_rejection_releases_reservation_and_recomputes_state(tmp_path):
    engine, store = _engine(tmp_path)
    assert engine.reserve(_signal(), production=True) == "FUT_ALLOW"
    assert store.snapshot()["state"] == "ENTRY_RESERVING"

    assert engine.mark_submission_rejected("S1", broker_verified_no_order=True) is True
    snapshot = store.snapshot()
    assert snapshot["state"] == "IDLE"
    assert snapshot["reservations"] == {}
    assert snapshot["active_winner"] is None
    assert snapshot["serial_reservation"] is None
    assert "S1" not in snapshot["consumed"]
    assert snapshot["reservation_history"]["S1"]["status"] == "BROKER_REJECTED"


def test_unsubmitted_release_does_not_clear_error_locked(tmp_path):
    engine, store = _engine(tmp_path)
    with store.transaction() as (data, _):
        data["state"] = "ERROR_LOCKED"
        data["reservations"]["S1"] = {
            "signal_id": "S1", "quantity": 6750, "lot_size": 6750,
            "entry": "100", "stop": "99", "status": "PENDING",
        }
    result = engine.release_unsubmitted_reservation("S1", reason="USER_REJECTED")
    assert result["state"] == "ERROR_LOCKED"
    assert store.snapshot()["state"] == "ERROR_LOCKED"
