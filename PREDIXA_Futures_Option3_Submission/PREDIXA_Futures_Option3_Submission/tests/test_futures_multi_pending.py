from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

from sqlalchemy import create_engine

from scripts.futures_risk_engine import RiskEngine, Signal
from scripts.futures_state_store import StateStore
from scripts.graduated_live_config import GATES, Policy
from scripts.graduated_live_execution import GraduatedLiveExecution
from shared.db.fyers_req_model import FyersClient


def make_engine(tmp_path, symbols):
    store = StateStore(create_engine("sqlite:///" + str(tmp_path / "multi.sqlite")))
    store.initialize()
    engine = RiskEngine(store, Policy(dd_update="LOSS_MINUS_WIN_FLOORED"))
    # Exercise the Option-3 concurrency model, not the legacy Option-2 fixture path.
    engine._legacy_compat = False
    today = date.today()
    approved = {
        symbol: {
            "contract": f"NSE:{symbol}FUT", "lot_size": 100,
            "fyers_resolved": True, "as_of": today.isoformat(),
            "expiry": (today + timedelta(days=30)).isoformat(),
        }
        for symbol in symbols
    }
    assert engine.reconcile({}, dd=0, cash_loss=0, futures_loss=0,
                            approved=approved, gates=dict.fromkeys(GATES, True),
                            unresolved_orders=[], source_verified=True)
    return engine


def signal(symbol, signal_id):
    return Signal(signal_id, symbol, 1, 1000, 990, 1200,
                   f"NSE:{symbol}FUT", 100)


def payload(s):
    return {"symbol": s.contract, "side": 1, "productType": "MARGIN",
            "orderInfo": {"leg1": {"qty": s.lot_size, "price": s.entry,
                                      "triggerPrice": s.entry}}}


def test_ten_generated_five_approved_first_wins_and_cancels_only_losers(tmp_path):
    symbols = [f"T{i}" for i in range(1, 11)]
    engine = make_engine(tmp_path, symbols)
    approved = [signal(symbols[i], f"S{i + 1}") for i in range(5)]

    for item in approved:
        assert engine.reserve(item, production=False) == "FUT_ALLOW"
        engine.mark_gtt_resting(item.signal_id, gtt_id=f"G-{item.signal_id}")

    state = engine.store.snapshot()
    assert len(state["reservations"]) == 5
    assert state["serial_reservation"] is None

    broker = SimpleNamespace(
        cancel_gtt_order=MagicMock(return_value={"s": "ok"}),
        place_order=MagicMock(),
    )
    execution = GraduatedLiveExecution(engine, broker)
    result = execution.claim_futures_winner_and_cancel_losers("S3", event_id="trigger-S3")

    assert result["winner"] == "S3"
    assert [call.kwargs["data"] if "data" in call.kwargs else call.args[0]
            for call in broker.cancel_gtt_order.call_args_list] == [
                {"id": "G-S1"}, {"id": "G-S2"}, {"id": "G-S4"}, {"id": "G-S5"}
            ]
    assert broker.place_order.call_count == 0
    assert engine.reserve(signal("T6", "S6")) == "FUT_REJECT_ACTIVE_FUTURES_WINNER"

    # Cash remains independent, while a new futures broker call is impossible.
    cash = execution.submit_cash({"symbol": "NSE:SBIN-EQ"})
    assert cash["submitted"] is True
    assert broker.place_order.call_count == 1
    assert broker.cancel_gtt_order.call_count == 4


def test_boundary_accepts_specific_pending_reservations_without_global_serial_id(tmp_path):
    engine = make_engine(tmp_path, ["A", "B"])
    a, b = signal("A", "A1"), signal("B", "B1")
    engine.reserve(a, production=False)
    engine.reserve(b, production=False)
    client = FyersClient.__new__(FyersClient)
    client._client = SimpleNamespace(place_gtt_order=MagicMock(return_value={"s": "ok"}))

    client.place_futures_entry_gtt(payload(a), risk=engine, signal_id=a.signal_id)
    client.place_futures_entry_gtt(payload(b), risk=engine, signal_id=b.signal_id)
    assert client._client.place_gtt_order.call_count == 2


def test_winner_claim_is_idempotent_and_competing_trigger_locks(tmp_path):
    engine = make_engine(tmp_path, ["A", "B"])
    a, b = signal("A", "A1"), signal("B", "B1")
    engine.reserve(a)
    engine.reserve(b)
    first = engine.claim_futures_winner(a.signal_id, event_id="e1")
    duplicate = engine.claim_futures_winner(a.signal_id, event_id="e1")
    race = engine.claim_futures_winner(b.signal_id, event_id="e2")

    assert first["status"] == "WINNER"
    assert duplicate["status"] == "DUPLICATE"
    assert race["status"] == "EXPOSURE_BREACH"
    assert engine.store.snapshot()["state"] == "ERROR_LOCKED"


def test_cancel_timeout_keeps_loser_and_locks(tmp_path):
    engine = make_engine(tmp_path, ["A", "B"])
    a, b = signal("A", "A1"), signal("B", "B1")
    engine.reserve(a)
    engine.reserve(b)
    engine.mark_gtt_resting(a.signal_id, gtt_id="G-A")
    engine.mark_gtt_resting(b.signal_id, gtt_id="G-B")
    engine.claim_futures_winner(a.signal_id)

    assert engine.confirm_loser_cancel("B1", broker_verified=False, cancelled=False) is False
    state = engine.store.snapshot()
    assert "B1" in state["reservations"]
    assert state["state"] == "ERROR_LOCKED"
