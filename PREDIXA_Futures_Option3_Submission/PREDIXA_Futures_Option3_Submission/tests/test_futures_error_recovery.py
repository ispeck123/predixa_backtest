from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from scripts.futures_reconciliation import recover_error_locked_state
from scripts.futures_risk_engine import RiskEngine
from scripts.futures_state_store import StateStore
from shared.db.db_model import Base, OMSOrderBucketFuture


class ReadOnlyBroker:
    def __init__(self, positions=None, gtts=None):
        self.positions = positions or []
        self.gtts = gtts or []
        self.mutations = 0

    def read_positions(self):
        return {"s": "ok", "netPositions": self.positions}

    def read_gtt_orderbook(self):
        return {"s": "ok", "orderBook": self.gtts}

    def place_gtt_order(self, *args, **kwargs):
        self.mutations += 1
        raise AssertionError("recovery must not place orders")

    def cancel_gtt_order(self, *args, **kwargs):
        self.mutations += 1
        raise AssertionError("recovery must not cancel orders")


def _state(tmp_path):
    store = StateStore(create_engine("sqlite:///" + str(tmp_path / "recovery.sqlite")))
    store.initialize()
    with store.transaction() as (data, _):
        data["state"] = "ERROR_LOCKED"
        data["reconciled"] = False
        data["errors"] = ["Broker futures exposure has no durable Option-3 winner"]
    return store


def test_manual_exposure_remains_locked(tmp_path):
    store = _state(tmp_path)
    broker = ReadOnlyBroker(positions=[{
        "symbol": "NSE:MANUAL26SEPFUT", "netQty": 100,
        "side": 1, "productType": "MARGIN", "avgPrice": 100,
    }])

    result = recover_error_locked_state(risk=RiskEngine(store), broker=broker)

    assert result["result"] == "BLOCKED_EXTERNAL_EXPOSURE"
    assert result["classification"] == "UNRELATED_OR_MANUAL_FUTURES_POSITION"
    assert store.snapshot()["state"] == "ERROR_LOCKED"
    assert store.snapshot()["positions"] == {}
    assert broker.mutations == 0


def test_exact_historical_id_reconstructs_active_winner(tmp_path):
    store = _state(tmp_path)
    engine = create_engine("sqlite:///" + str(tmp_path / "oms.sqlite"))
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    session = factory()
    session.add(OMSOrderBucketFuture(
        trade_signal_id=3003, stock_tick="CANBK", country_id=1, time_frame=1,
        order_type="BUY", entry_price=100, stoploss_price=99, target_price=110,
        stock_quantity=6750, expiry_date="29092026", approval_status="approved",
        gtt_id="GTT-3", id_fyers="FY-3", is_trade_started=1,
    ))
    session.commit()
    broker = ReadOnlyBroker(positions=[{
        "symbol": "NSE:CANBK26SEPFUT", "netQty": 6750,
        "side": 1, "productType": "MARGIN", "avgPrice": 100,
        "orderId": "GTT-3",
    }])
    result = recover_error_locked_state(
        risk=RiskEngine(store), broker=broker, session=session,
    )

    assert result["result"] == "RECOVERED_ACTIVE_WINNER"
    snapshot = store.snapshot()
    assert snapshot["active_winner"] == "3003"
    assert snapshot["serial_reservation"] == "3003"
    assert snapshot["state"] == "POSITION_OPEN_LOCKED"
    assert snapshot["positions"]["3003"]["quantity"] == 6750
    assert broker.mutations == 0
    session.close()


def test_clean_recovery_clears_error_only_explicitly(tmp_path):
    store = _state(tmp_path)
    broker = ReadOnlyBroker()

    result = recover_error_locked_state(risk=RiskEngine(store), broker=broker)

    assert result["result"] == "RECOVERED_TO_IDLE"
    snapshot = store.snapshot()
    assert snapshot["state"] == "IDLE"
    assert snapshot["reconciled"] is True
    assert snapshot["last_reconciliation"]
    assert snapshot["errors"]
    assert any(item.get("event") == "EXPLICIT_ERROR_LOCKED_RECOVERY_TO_IDLE"
               for item in snapshot["reconciliation_reports"])
    assert broker.mutations == 0


def test_verified_missing_submission_is_archived_and_recovered(tmp_path):
    store = _state(tmp_path)
    with store.transaction() as (data, _):
        data["reservations"]["11933"] = {
            "signal_id": "11933", "contract": "NSE:CANBK26SEPFUT",
            "status": "PENDING", "quantity": 6750,
        }
    broker = ReadOnlyBroker()

    result = recover_error_locked_state(risk=RiskEngine(store), broker=broker)

    assert result["result"] == "RECOVERED_TO_IDLE"
    assert result["orphan_reservations_archived"] == ["11933"]
    snapshot = store.snapshot()
    assert snapshot["state"] == "IDLE"
    assert snapshot["reservations"] == {}
    assert snapshot["reservation_history"]["11933"]["status"] == "BROKER_VERIFIED_NO_ORDER"
    assert broker.mutations == 0


def test_duplicate_clean_recovery_is_idempotent(tmp_path):
    store = _state(tmp_path)
    broker = ReadOnlyBroker()
    risk = RiskEngine(store)

    assert recover_error_locked_state(risk=risk, broker=broker)["result"] == "RECOVERED_TO_IDLE"
    assert recover_error_locked_state(risk=RiskEngine(store), broker=broker)["result"] == "RECOVERED_TO_IDLE"
    assert store.snapshot()["state"] == "IDLE"
    assert broker.mutations == 0


def test_cash_positions_are_ignored_by_futures_recovery(tmp_path):
    store = _state(tmp_path)
    broker = ReadOnlyBroker(positions=[
        {"symbol": "NSE:ICICIBANK-EQ", "netQty": -1, "productType": "CNC"},
        {"symbol": "NSE:BEL-EQ", "netQty": 1, "productType": "CNC"},
    ])

    result = recover_error_locked_state(risk=RiskEngine(store), broker=broker)

    assert result["result"] == "RECOVERED_TO_IDLE"
    assert result["classification"] == "NO_BROKER_FUTURES_EXPOSURE"
    assert broker.mutations == 0
