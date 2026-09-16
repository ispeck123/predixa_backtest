from datetime import date, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine

from scripts.futures_reconciliation import reconcile_futures_startup
from scripts.futures_risk_engine import RiskEngine, Signal
from scripts.futures_state_store import StateStore
from scripts.graduated_live_config import GATES


class ReadOnlyBroker:
    def __init__(self, gtts=None, positions=None):
        self.gtts = gtts or []
        self.positions = positions or []
        self.mutations = 0

    def read_gtt_orderbook(self):
        return {"s": "ok", "orderBook": self.gtts}

    def read_positions(self):
        return {"s": "ok", "netPositions": self.positions}

    def place_gtt_order(self, *args, **kwargs):
        self.mutations += 1
        raise AssertionError("reconciliation must not place orders")

    def cancel_gtt_order(self, *args, **kwargs):
        self.mutations += 1
        raise AssertionError("reconciliation must not cancel orders")


def _engine(tmp_path):
    store = StateStore(create_engine("sqlite:///" + str(tmp_path / "state.sqlite")))
    store.initialize()
    return store, RiskEngine(store)


def _approved():
    return {"CANBK": {
        "contract": "NSE:CANBK26SEPFUT", "lot_size": 6750,
        "expiry": (date.today() + timedelta(days=20)).isoformat(),
        "fyers_resolved": True, "as_of": date.today().isoformat(),
    }}


def _gates():
    return dict.fromkeys(GATES, True)


def _reconcile(store, broker):
    return reconcile_futures_startup(
        risk=RiskEngine(store), broker=broker, approved=_approved(),
        gates=_gates(),
    )


def _signal(signal_id):
    return Signal(signal_id, "CANBK", 1, 100, 99, 110,
                  "NSE:CANBK26SEPFUT", 6750)


def test_verified_clean_startup_and_fresh_engine_readiness(tmp_path):
    store, risk = _engine(tmp_path)
    broker = ReadOnlyBroker()

    assert _reconcile(store, broker)
    assert store.snapshot()["reconciled"] is True
    assert RiskEngine(store).futures_enabled
    assert RiskEngine(store).reserve(_signal("B1"), production=True) == "FUT_ALLOW"
    assert broker.mutations == 0


def test_restart_preserves_three_pending_exact_gtts(tmp_path):
    store, risk = _engine(tmp_path)
    assert _reconcile(store, ReadOnlyBroker())
    for signal_id, gtt_id in (("B1", "1001"), ("B3", "1003"), ("B5", "1005")):
        assert risk.reserve(_signal(signal_id), production=True) == "FUT_ALLOW"
        risk.mark_gtt_resting(signal_id, gtt_id=gtt_id, broker_order_id="O" + signal_id)

    broker = ReadOnlyBroker(gtts=[
        {"id": "1001", "ord_status": 6},
        {"id": "1003", "ord_status": 6},
        {"id": "1005", "ord_status": 6},
    ])
    restarted = RiskEngine(store)
    assert reconcile_futures_startup(
        risk=restarted, broker=broker, approved=_approved(), gates=_gates()
    )
    snapshot = store.snapshot()
    assert set(snapshot["reservations"]) == {"B1", "B3", "B5"}
    assert snapshot["state"] == "ENTRY_GTT_RESTING"
    assert snapshot["serial_reservation"] is None
    assert broker.mutations == 0


def test_active_winner_restart_is_preserved(tmp_path):
    store, risk = _engine(tmp_path)
    assert _reconcile(store, ReadOnlyBroker())
    assert risk.reserve(_signal("B3"), production=True) == "FUT_ALLOW"
    risk.mark_gtt_resting("B3", gtt_id="1003", broker_order_id="OB3")
    risk.claim_futures_winner("B3", event_id="fill-1")
    with store.transaction() as (data, _):
        data["positions"]["B3"] = dict(data["reservations"]["B3"], quantity=6750)

    broker = ReadOnlyBroker(
        gtts=[{"id": "1003", "ord_status": 2}],
        positions=[{"symbol": "NSE:CANBK26SEPFUT", "netQty": 6750}],
    )
    assert reconcile_futures_startup(
        risk=RiskEngine(store), broker=broker, approved=_approved(), gates=_gates()
    )
    snapshot = store.snapshot()
    assert snapshot["active_winner"] == "B3"
    assert snapshot["serial_reservation"] == "B3"
    assert snapshot["state"] == "POSITION_OPEN_LOCKED"


def test_unknown_gtt_status_fails_closed(tmp_path):
    store, _ = _engine(tmp_path)
    risk = RiskEngine(store)
    assert _reconcile(store, ReadOnlyBroker())
    assert risk.reserve(_signal("B1"), production=True) == "FUT_ALLOW"
    risk.mark_gtt_resting("B1", gtt_id="1001")
    broker = ReadOnlyBroker(gtts=[{"id": "1001", "ord_status": 99}])
    assert not reconcile_futures_startup(
        risk=RiskEngine(store), broker=broker, approved=_approved(), gates=_gates()
    )
    assert store.snapshot()["reconciled"] is False
    assert store.snapshot()["state"] == "ERROR_LOCKED"
    assert broker.mutations == 0


def test_read_failure_does_not_mark_ready(tmp_path):
    store, _ = _engine(tmp_path)

    class BrokenBroker:
        def read_gtt_orderbook(self):
            raise TimeoutError("broker unavailable")

    assert not reconcile_futures_startup(
        risk=RiskEngine(store), broker=BrokenBroker(),
        approved=_approved(), gates=_gates(),
    )
    assert store.snapshot()["reconciled"] is False


def test_error_locked_stays_sticky_after_verified_clean_snapshot(tmp_path):
    store, _ = _engine(tmp_path)
    with store.transaction() as (data, _):
        data["state"] = "ERROR_LOCKED"
        data["reconciled"] = False
        data["errors"] = ["operator exposure review required"]

    assert _reconcile(store, ReadOnlyBroker())
    snapshot = store.snapshot()
    assert snapshot["state"] == "ERROR_LOCKED"
    assert snapshot["reconciled"] is True
    assert snapshot["broker_state_verified"] is True
    assert snapshot["recovery_possible"] is True
    assert not RiskEngine(store).futures_enabled
    assert RiskEngine(store).reserve(_signal("NEW"), production=True) == \
        "FUT_REJECT_STATE_RECONCILIATION_FAILED"


def test_clean_broker_reconciliation_does_not_fabricate_missing_gates(tmp_path):
    store, _ = _engine(tmp_path)
    broker = ReadOnlyBroker()

    assert reconcile_futures_startup(
        risk=RiskEngine(store), broker=broker, approved=_approved(), gates={}
    )
    snapshot = store.snapshot()
    assert snapshot["reconciled"] is True
    assert snapshot["state"] == "IDLE"
    assert not RiskEngine(store).futures_enabled
    assert snapshot["gates"] == {}
    assert broker.mutations == 0
