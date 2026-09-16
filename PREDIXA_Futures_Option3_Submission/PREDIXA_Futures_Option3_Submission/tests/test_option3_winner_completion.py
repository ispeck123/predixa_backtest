import pytest

from data_fetchers.fyers import fyers_general_socket as socket
from tests.test_futures_multi_pending import make_engine, signal


def setup_winner(tmp_path):
    engine = make_engine(tmp_path, ["A", "B", "C", "D", "E"])
    for symbol in ("A", "B", "C", "D", "E"):
        item = signal(symbol, f"S-{symbol}")
        assert engine.reserve(item) == "FUT_ALLOW"
        engine.mark_gtt_resting(item.signal_id, gtt_id=f"G-{symbol}")
    engine.claim_futures_winner("S-C", event_id="entry-C")
    with engine.store.transaction() as (state, _):
        state["positions"]["S-C"] = {
            "signal_id": "S-C", "contract": "NSE:CFUT", "entry": "1000",
            "stop": "990", "quantity": 100, "lot_size": 100,
        }
    return engine


@pytest.mark.parametrize("exit_id", ["target-C", "stop-C"])
def test_closed_winner_and_confirmed_losers_unlock(tmp_path, exit_id):
    engine = setup_winner(tmp_path)
    for signal_id in ("S-A", "S-B", "S-D", "S-E"):
        assert engine.confirm_loser_cancel(signal_id, broker_verified=True, cancelled=True)

    engine.apply_exit_fill("S-C", remaining_quantity=0, broker_verified=True, event_id=exit_id)
    assert engine.store.snapshot()["state"] == "EXIT_RECONCILING"
    assert engine.confirm_exit_complete(
        broker_verified=True, no_open_position=True,
        winner_signal_id="S-C", exit_event_id=exit_id,
    )
    state = engine.store.snapshot()
    assert state["state"] == "IDLE"
    assert state["serial_reservation"] is None
    assert state["active_winner"] is None
    assert state["reservation_history"]["S-C"]["status"] == "COMPLETED"


def test_unconfirmed_loser_keeps_winner_locked(tmp_path):
    engine = setup_winner(tmp_path)
    for signal_id in ("S-A", "S-B", "S-D"):
        engine.confirm_loser_cancel(signal_id, broker_verified=True, cancelled=True)
    engine.apply_exit_fill("S-C", remaining_quantity=0, broker_verified=True, event_id="exit-C")

    with engine.store.transaction() as (stored, _):
        stored["reservations"]["S-E"]["status"] = "CANCEL_REQUESTED"
    assert not engine.confirm_exit_complete(broker_verified=True, no_open_position=True,
                                             winner_signal_id="S-C", exit_event_id="exit-C")
    assert engine.store.snapshot()["state"] == "EXIT_RECONCILING"
    assert engine.store.snapshot()["serial_reservation"] == "S-C"

    assert engine.confirm_loser_cancel("S-E", broker_verified=True, cancelled=True)
    assert engine.confirm_exit_complete(broker_verified=True, no_open_position=True,
                                        winner_signal_id="S-C", exit_event_id="exit-C")


def test_unknown_loser_and_open_position_do_not_unlock(tmp_path):
    engine = setup_winner(tmp_path)
    with engine.store.transaction() as (state, _):
        state["reservations"]["S-A"]["status"] = "UNKNOWN"
    assert not engine.confirm_exit_complete(broker_verified=True, no_open_position=False,
                                             winner_signal_id="S-C")
    assert engine.store.snapshot()["state"] == "ENTRY_FILL_PENDING_RECONCILIATION"

    engine.mark_winner_exit_filled("S-C", exit_id="OCO-C", event_id="exit-C")
    assert not engine.confirm_exit_complete(broker_verified=True, no_open_position=False,
                                             winner_signal_id="S-C", exit_event_id="exit-C")
    assert engine.store.snapshot()["serial_reservation"] == "S-C"


def test_duplicate_exit_event_is_idempotent(tmp_path):
    engine = setup_winner(tmp_path)
    for signal_id in ("S-A", "S-B", "S-D", "S-E"):
        engine.confirm_loser_cancel(signal_id, broker_verified=True, cancelled=True)
    engine.apply_exit_fill("S-C", remaining_quantity=0, broker_verified=True, event_id="exit-C")
    assert engine.apply_exit_fill("S-C", remaining_quantity=0, broker_verified=True, event_id="exit-C")
    assert engine.store.snapshot()["state"] == "EXIT_RECONCILING"


def test_error_locked_and_second_exposure_never_auto_unlock(tmp_path):
    engine = setup_winner(tmp_path)
    with engine.store.transaction() as (state, _):
        state["errors"].append("FUTURES_EXPOSURE_BREACH_MULTIPLE_FILLED:S-C:S-B")
        state["state"] = "ERROR_LOCKED"
        state["positions"]["S-B"] = {"signal_id": "S-B", "quantity": 100}
    assert not engine.confirm_exit_complete(broker_verified=True, no_open_position=True,
                                             winner_signal_id="S-C")
    state = engine.store.snapshot()
    assert state["state"] == "ERROR_LOCKED"
    assert "S-B" in state["positions"]


def test_authoritative_protective_oco_fill_marks_exit_reconciling(tmp_path):
    engine = setup_winner(tmp_path)
    for signal_id in ("S-A", "S-B", "S-D", "S-E"):
        engine.confirm_loser_cancel(signal_id, broker_verified=True, cancelled=True)
    with engine.store.transaction() as (state, _):
        state["broker_orders"]["S-C"] = {
            "entry_gtt_id": "G-C", "protective_gtt_id": "OCO-C",
        }

    result = socket.handle_option3_order_event({"s": "ok", "orders": {
        "status": 2, "exchange": 10, "symbol": "NSE:CFUT",
        "side": 1, "id": "OCO-C",
    }}, execution=type("Execution", (), {"risk": engine})())

    assert result["exit"] is True
    assert result["result"]["status"] == "EXIT_RECORDED"
    assert engine.store.snapshot()["state"] == "EXIT_RECONCILING"
