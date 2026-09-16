from types import SimpleNamespace
from datetime import date

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from apps.backend.api import bucket_orders_future as api
from shared.db.db_model import Base, OMSOrderBucketFuture
from shared.db.db_model import FuturesMaster
from scripts.futures_state_store import StateStore
from scripts.futures_risk_engine import RiskEngine
from scripts.graduated_live_config import GATES


class FakeRisk:
    def __init__(self, active=None, expiry="29092026", lot_size=75):
        self.reservations = {}
        self.active = active
        self.store = SimpleNamespace(snapshot=self.snapshot)
        self.expiry = expiry
        self.lot_size = lot_size

    def snapshot(self):
        return {
            "active_winner": self.active,
            "serial_reservation": None,
            "reservations": {key: {} for key in self.reservations},
            "approved": {
                "CANBK": {
                    "contract": "NSE:CANBK26SEPFUT",
                    "lot_size": self.lot_size,
                    "expiry": self.expiry,
                    "fyers_resolved": True,
                    "as_of": "2026-09-10",
                }
            },
        }

    def reserve(self, signal, production, margin_ok):
        if self.active:
            return "FUT_REJECT_ACTIVE_FUTURES_WINNER"
        if signal.signal_id in self.reservations:
            return "FUT_REJECT_DUPLICATE_SIGNAL"
        self.reservations[signal.signal_id] = signal
        return "FUT_ALLOW"

    def mark_gtt_resting(self, signal_id, *, gtt_id, broker_order_id):
        return None

    def mark_submission_rejected(self, signal_id, *, broker_verified_no_order):
        self.reservations.pop(signal_id, None)

    def set_error_locked(self, reason):
        self.error = reason

    def confirm_entry_cancel(self, signal_id, *, broker_verified, filled, open_position):
        self.reservations.pop(signal_id, None)
        return True


class FakeFyers:
    def __init__(self, response=None):
        self.calls = []
        self.response = response or {"s": "ok", "id": "GTT-1", "id_fyers": "FY-1"}

    def place_futures_entry_gtt(self, payload, *, risk, signal_id):
        self.calls.append((payload, risk, signal_id))
        return self.response

    def cancel_gtt_order(self, payload):
        self.cancel_calls = getattr(self, "cancel_calls", [])
        self.cancel_calls.append(payload)
        return self.cancel_response if hasattr(self, "cancel_response") else {
            "s": "ok", "id": payload["id"]}

    def read_gtt_orderbook(self):
        return {"s": "ok", "orderBook": []}


def _setup(monkeypatch, count=1):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    session = factory()
    for index in range(count):
        session.add(OMSOrderBucketFuture(
            trade_signal_id=100 + index,
            stock_tick="CANBK",
            country_id=1,
            time_frame=1,
            order_type="BUY",
            entry_price=1000 + index,
            stoploss_price=950,
            target_price=1100,
            stock_quantity=75,
            purchased_cmp_date="2026-09-10T10:00",
            order_status="pending",
            is_trade_started=0,
            is_evaluated=0,
            expiry_date="29092026",
            approval_status="pending",
        ))
    session.commit()
    ids = [row.bucket_id for row in session.query(OMSOrderBucketFuture).all()]
    session.close()
    return factory, ids


def test_pending_row_is_approved_and_submitted_once(monkeypatch):
    factory, ids = _setup(monkeypatch)
    risk = FakeRisk()
    fyers = FakeFyers()
    result = api.approve_existing_futures_candidate(
        bucket_id=ids[0], status="approved", approved_by="7",
        session_factory=factory, risk_factory=lambda: risk,
        fyers_factory=lambda: fyers,
    )

    assert result["status"] == "approved"
    assert len(fyers.calls) == 1
    session = factory()
    row = session.get(OMSOrderBucketFuture, ids[0])
    assert row.approval_status == "approved"
    assert row.approved_by == 7
    assert row.gtt_id == "GTT-1"
    assert row.id_fyers == "FY-1"
    assert row.is_trade_started == 0
    assert len(risk.reservations) == 1
    assert risk.store.snapshot()["serial_reservation"] is None
    assert result["success"] is True
    assert result["decision"] == "SUBMITTED"
    assert result["reason_code"] == "FUT_ALLOW"
    assert result["contract"] == "NSE:CANBK26SEPFUT"
    assert result.get("calculation") is None  # FakeRisk has no decision context.
    session.close()


def test_three_pending_rows_create_three_reservations_and_gtts(monkeypatch):
    factory, ids = _setup(monkeypatch, count=3)
    risk = FakeRisk()
    fyers = FakeFyers()
    for bucket_id in ids:
        api.approve_existing_futures_candidate(
            bucket_id=bucket_id, status="approved", approved_by="7",
            session_factory=factory, risk_factory=lambda: risk,
            fyers_factory=lambda: fyers,
        )

    assert len(risk.reservations) == 3
    assert len(fyers.calls) == 3
    assert risk.store.snapshot()["serial_reservation"] is None


def test_duplicate_approval_makes_no_broker_call(monkeypatch):
    factory, ids = _setup(monkeypatch)
    risk = FakeRisk()
    fyers = FakeFyers()
    api.approve_existing_futures_candidate(
        bucket_id=ids[0], status="approved", approved_by="7",
        session_factory=factory, risk_factory=lambda: risk, fyers_factory=lambda: fyers,
    )
    result = api.approve_existing_futures_candidate(
        bucket_id=ids[0], status="approved", approved_by="7",
        session_factory=factory, risk_factory=lambda: risk, fyers_factory=lambda: fyers,
    )
    assert result["status"] == "ALREADY_APPROVED"
    assert len(fyers.calls) == 1


def test_approved_futures_rejection_cancels_exact_persisted_gtt(monkeypatch):
    factory, ids = _setup(monkeypatch)
    risk = FakeRisk()
    fyers = FakeFyers()
    api.approve_existing_futures_candidate(
        bucket_id=ids[0], status="approved", approved_by="7",
        session_factory=factory, risk_factory=lambda: risk, fyers_factory=lambda: fyers,
    )
    result = api.approve_existing_futures_candidate(
        bucket_id=ids[0], status="rejected", approved_by="7",
        session_factory=factory, risk_factory=lambda: risk, fyers_factory=lambda: fyers,
    )
    assert result["cancellation"] == "confirmed"
    assert fyers.cancel_calls == [{"id": "GTT-1"}]
    row = factory().get(OMSOrderBucketFuture, ids[0])
    assert row.approval_status == "rejected"
    assert row.order_status == "cancelled"
    assert row.gtt_id is None
    assert row.id_fyers is None


def test_missing_fyers_gtt_is_treated_as_confirmed_absence(monkeypatch):
    factory, ids = _setup(monkeypatch)
    risk = FakeRisk()
    fyers = FakeFyers()
    api.approve_existing_futures_candidate(
        bucket_id=ids[0], status="approved", approved_by="7",
        session_factory=factory, risk_factory=lambda: risk,
        fyers_factory=lambda: fyers,
    )
    fyers.cancel_response = {"s": "error", "message": "GTT order not found"}
    result = api.approve_existing_futures_candidate(
        bucket_id=ids[0], status="rejected", approved_by="7",
        session_factory=factory, risk_factory=lambda: risk,
        fyers_factory=lambda: fyers,
    )
    assert result["status"] == "rejected"
    assert result["cancellation"] == "already_absent"
    assert fyers.cancel_calls == [{"id": "GTT-1"}]
    assert risk.reservations == {}
    row = factory().get(OMSOrderBucketFuture, ids[0])
    assert row.approval_status == "rejected"
    assert row.gtt_id is None
    assert row.id_fyers is None


def test_missing_reservation_with_missing_fyers_gtt_is_successful_rejection(monkeypatch):
    factory, ids = _setup(monkeypatch)
    risk = FakeRisk()
    fyers = FakeFyers()
    api.approve_existing_futures_candidate(
        bucket_id=ids[0], status="approved", approved_by="7",
        session_factory=factory, risk_factory=lambda: risk,
        fyers_factory=lambda: fyers,
    )
    risk.reservations.clear()
    fyers.cancel_response = {"s": "error", "message": "GTT order not found"}

    result = api.approve_existing_futures_candidate(
        bucket_id=ids[0], status="rejected", approved_by="7",
        session_factory=factory, risk_factory=lambda: risk,
        fyers_factory=lambda: fyers,
    )

    assert result["success"] is True
    assert result["reason_code"] == "FUTURES_GTT_ALREADY_ABSENT"
    assert result["cancellation"] == "already_absent"
    assert fyers.cancel_calls == [{"id": "GTT-1"}]
    row = factory().get(OMSOrderBucketFuture, ids[0])
    assert row.approval_status == "rejected"
    assert row.gtt_id is None
    assert row.id_fyers is None


def test_orderbook_absence_allows_rejection_when_cancel_error_has_no_text(monkeypatch):
    factory, ids = _setup(monkeypatch)
    risk = FakeRisk()
    fyers = FakeFyers()
    api.approve_existing_futures_candidate(
        bucket_id=ids[0], status="approved", approved_by="7",
        session_factory=factory, risk_factory=lambda: risk,
        fyers_factory=lambda: fyers,
    )
    risk.reservations.clear()
    fyers.cancel_response = {"s": "error", "code": -50}

    result = api.approve_existing_futures_candidate(
        bucket_id=ids[0], status="rejected", approved_by="7",
        session_factory=factory, risk_factory=lambda: risk,
        fyers_factory=lambda: fyers,
    )

    assert result["success"] is True
    assert result["reason_code"] == "FUTURES_GTT_ALREADY_ABSENT"
    assert result["cancellation"] == "already_absent"


def test_active_winner_blocks_new_approval(monkeypatch):
    factory, ids = _setup(monkeypatch)
    with pytest.raises(HTTPException, match="ACTIVE_FUTURES_WINNER"):
        api.approve_existing_futures_candidate(
            bucket_id=ids[0], status="approved", approved_by="7",
            session_factory=factory, risk_factory=lambda: FakeRisk(active="100"),
            fyers_factory=lambda: FakeFyers(),
        )


def test_reject_pending_row_does_not_call_broker(monkeypatch):
    factory, ids = _setup(monkeypatch)
    risk = FakeRisk()
    result = api.approve_existing_futures_candidate(
        bucket_id=ids[0], status="rejected", approved_by="7", session_factory=factory,
        risk_factory=lambda: risk,
        fyers_factory=lambda: pytest.fail("broker should not be used"),
    )
    assert result["status"] == "rejected"


def test_rejected_candidate_can_be_edited_reopened_and_approved(monkeypatch):
    factory, ids = _setup(monkeypatch)
    risk = FakeRisk()
    api.approve_existing_futures_candidate(
        bucket_id=ids[0], status="rejected", approved_by="7",
        session_factory=factory, risk_factory=lambda: risk,
        fyers_factory=lambda: pytest.fail("broker should not be used"),
    )
    edited = api.edit_rejected_futures_candidate(
        bucket_id=ids[0], entry_price=1001, stoploss_price=990,
        target_price=1150, session_factory=factory,
        risk_factory=lambda: risk,
    )
    assert edited["retryable"] is True
    assert edited["editable"] is True
    assert edited["status"] == "pending"

    fyers = FakeFyers()
    result = api.approve_existing_futures_candidate(
        bucket_id=ids[0], status="approved", approved_by="7",
        session_factory=factory, risk_factory=lambda: risk,
        fyers_factory=lambda: fyers,
    )
    assert result["status"] == "approved"
    assert fyers.calls[0][0]["orderInfo"]["leg1"]["price"] == 1001.0


def test_rejected_unlinked_candidate_can_be_approved_directly(monkeypatch):
    factory, ids = _setup(monkeypatch)
    risk = FakeRisk()
    fyers = FakeFyers()
    api.approve_existing_futures_candidate(
        bucket_id=ids[0], status="rejected", approved_by="7",
        session_factory=factory, risk_factory=lambda: risk,
        fyers_factory=lambda: pytest.fail("broker should not be used"),
    )

    result = api.approve_existing_futures_candidate(
        bucket_id=ids[0], status="approved", approved_by="1",
        session_factory=factory, risk_factory=lambda: risk,
        fyers_factory=lambda: fyers,
    )

    assert result["status"] == "approved"
    assert len(fyers.calls) == 1


def test_definite_rejection_cleans_reservation_but_ambiguous_keeps_it(monkeypatch):
    factory, ids = _setup(monkeypatch)
    risk = FakeRisk()
    fyers = FakeFyers({"s": "error", "broker_verified_no_order": True})
    with pytest.raises(HTTPException, match="rejected"):
        api.approve_existing_futures_candidate(
            bucket_id=ids[0], status="approved", approved_by="7",
            session_factory=factory, risk_factory=lambda: risk, fyers_factory=lambda: fyers,
        )
    assert risk.reservations == {}

    factory, ids = _setup(monkeypatch)
    risk = FakeRisk()
    fyers = FakeFyers({"s": "error"})
    with pytest.raises(HTTPException, match="unresolved"):
        api.approve_existing_futures_candidate(
            bucket_id=ids[0], status="approved", approved_by="7",
            session_factory=factory, risk_factory=lambda: risk, fyers_factory=lambda: fyers,
        )
    assert len(risk.reservations) == 1


def test_scanner_row_added_after_startup_refreshes_missing_contract_map():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    session = factory()
    row = OMSOrderBucketFuture(
        trade_signal_id=9001, stock_tick="CANBK", country_id=1, time_frame=1,
        order_type="BUY", entry_price=100, stoploss_price=99, target_price=110,
        stock_quantity=6750, purchased_cmp_date="2026-09-10T10:00",
        order_status="pending", is_trade_started=0, is_evaluated=0,
        expiry_date="29092026", approval_status="pending",
    )
    session.add(row)
    session.add(FuturesMaster(symbol="CANBK", exchange="NSEFO",
                              expiry_date=date(2026, 9, 29),
                              instrument="FUTSTK"))
    session.commit()
    bucket_id = row.bucket_id
    session.close()

    store = StateStore(engine)
    store.initialize()
    with store.transaction() as (data, _):
        data["reconciled"] = True
        data["gates"] = dict.fromkeys(GATES, True)
    risk = RiskEngine(store)
    fyers = FakeFyers()
    result = api.approve_existing_futures_candidate(
        bucket_id=bucket_id, status="approved", approved_by="1",
        session_factory=factory, risk_factory=lambda: risk,
        fyers_factory=lambda: fyers,
    )

    assert result["status"] == "approved"
    assert len(fyers.calls) == 1
    assert store.snapshot()["approved"]["CANBK"]["fyers_resolved"] is True
