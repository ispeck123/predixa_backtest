from types import SimpleNamespace
from unittest.mock import MagicMock

from data_fetchers.fyers import fyers_general_socket as socket
from scripts.graduated_live_execution import GraduatedLiveExecution
from shared.db.db_model import Future_Order, OMSOrderBucketFuture
from tests.test_futures_multi_pending import make_engine, signal


def order(gtt_id, symbol="A"):
    return {"s": "ok", "orders": {
        "status": 2, "exchange": 10, "symbol": f"NSE:{symbol}FUT",
        "side": 1, "id": gtt_id,
    }}


def execution_with_reservations(tmp_path):
    engine = make_engine(tmp_path, ["A", "B", "C", "D", "E"])
    broker = SimpleNamespace(cancel_gtt_order=MagicMock(return_value={"s": "ok"}))
    execution = GraduatedLiveExecution(engine, broker)
    for symbol in ("A", "B", "C", "D", "E"):
        item = signal(symbol, f"S-{symbol}")
        assert engine.reserve(item) == "FUT_ALLOW"
        engine.mark_gtt_resting(item.signal_id, gtt_id=f"G-{symbol}")
    return execution, broker


def test_on_order_claims_t3_and_cancels_four_exact_ids(tmp_path):
    execution, broker = execution_with_reservations(tmp_path)
    result = socket.handle_option3_order_event(order("G-C", "C"), execution=execution)

    assert result["matched"] is True
    assert execution.risk.store.snapshot()["active_winner"] == "S-C"
    assert [call.args[0] for call in broker.cancel_gtt_order.call_args_list] == [
        {"id": "G-A"}, {"id": "G-B"}, {"id": "G-D"}, {"id": "G-E"}
    ]


def test_duplicate_order_event_is_idempotent(tmp_path):
    execution, broker = execution_with_reservations(tmp_path)
    first = socket.handle_option3_order_event(order("G-C", "C"), execution=execution)
    second = socket.handle_option3_order_event(order("G-C", "C"), execution=execution)

    assert first["result"]["status"] == "WINNER"
    assert second["result"]["status"] == "DUPLICATE"
    assert broker.cancel_gtt_order.call_count == 4


def test_unmatched_and_cash_events_do_not_claim_or_cancel(tmp_path):
    execution, broker = execution_with_reservations(tmp_path)
    unmatched = socket.handle_option3_order_event(order("MANUAL", "C"), execution=execution)
    cash = socket.handle_option3_order_event(order("G-C", "C").copy() | {
        "orders": order("G-C", "C")["orders"] | {"symbol": "NSE:SBIN-EQ"}
    }, execution=execution)

    assert unmatched["matched"] is False
    assert cash["matched"] is False
    assert execution.risk.store.snapshot()["active_winner"] is None
    assert broker.cancel_gtt_order.call_count == 0


def test_competing_filled_event_locks_durable_state(tmp_path):
    execution, broker = execution_with_reservations(tmp_path)
    socket.handle_option3_order_event(order("G-C", "C"), execution=execution)
    result = socket.handle_option3_order_event(order("G-B", "B"), execution=execution)

    assert result["result"]["status"] == "EXPOSURE_BREACH"
    assert execution.risk.store.snapshot()["state"] == "ERROR_LOCKED"
    assert broker.cancel_gtt_order.call_count == 4


def test_same_symbol_uses_exact_persisted_id(tmp_path):
    engine = make_engine(tmp_path, ["A"])
    broker = SimpleNamespace(cancel_gtt_order=MagicMock(return_value={"s": "ok"}))
    execution = GraduatedLiveExecution(engine, broker)
    first, second = signal("A", "S1"), signal("A", "S2")
    engine.reserve(first)
    engine.reserve(second)
    engine.mark_gtt_resting(first.signal_id, gtt_id="G-1")
    engine.mark_gtt_resting(second.signal_id, gtt_id="G-2")

    result = socket.handle_option3_order_event(order("G-2", "A"), execution=execution)

    assert result["signal_id"] == "S2"
    assert execution.risk.store.snapshot()["active_winner"] == "S2"
    assert broker.cancel_gtt_order.call_args_list[0].args[0] == {"id": "G-1"}


def test_matched_futures_fill_continues_existing_oms_oco_flow(monkeypatch):
    row = SimpleNamespace(
        bucket_id=123, order_type="BUY", entry_price=1000,
        target_price=1200, stoploss_price=990, stock_quantity=100,
        trade_signal_id=456, is_trade_started=0,
        id_fyers_gtt_oco=None, gtt_oco_id=None,
    )
    order_row = SimpleNamespace(is_trade_started=0, entry_timestamp=None,
                                order_status=None)

    class Query:
        def __init__(self):
            self.calls = 0

        def filter(self, *args, **kwargs):
            return self

        def first(self):
            self.calls += 1
            return row if self.calls == 1 else order_row

    session = SimpleNamespace(
        query=MagicMock(side_effect=lambda model: Query()),
        add=MagicMock(), commit=MagicMock(), close=MagicMock(),
    )
    monkeypatch.setattr(socket, "db", SimpleNamespace(get_session=lambda: session))
    monkeypatch.setattr(socket, "handle_option3_order_event",
                        MagicMock(return_value={"matched": True, "signal_id": "S-C"}))
    fyers = SimpleNamespace(_client=SimpleNamespace(
        place_gtt_order=MagicMock(return_value={"s": "ok", "id": "OCO-C", "id_fyers": "FOCO-C"})
    ))
    monkeypatch.setattr(socket, "get_fyers", lambda: fyers)

    socket.onOrder({"s": "ok", "orders": {
        "status": 2, "exchange": 10, "symbol": "NSE:TC FUT".replace(" ", ""),
        "side": 1, "orderTag": "1:GTT123", "limitPrice": 1000,
        "orderDateTime": "10-Sep-2026 14:01:27",
    }})

    assert fyers._client.place_gtt_order.call_count == 1
    assert row.is_trade_started == 1
    assert session.commit.call_count == 1
