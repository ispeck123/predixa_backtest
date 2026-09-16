import pytest
from decimal import Decimal
from unittest.mock import MagicMock
from types import SimpleNamespace
from datetime import date, timedelta
from scripts.futures_risk_engine import RiskEngine, Signal
from scripts.futures_state_store import StateStore
from scripts.graduated_live_config import Policy, TOTAL_REALIZED_LOSS_CEILING
from shared.db.fyers_req_model import FyersClient
from sqlalchemy import create_engine

# Mock Store for tests
class MockStore:
    def __init__(self):
        self.data = {
            'dd': '0', 'cash_loss': '0', 'futures_loss': '0', 'tap': False, 'halt': False,
            'reconciled': True, 'positions': {}, 'reservations': {}, 'consumed': [],
            'approved': {'SINE': {'contract': 'NSE:SINEFUT', 'fyers_resolved': True,
                           'as_of': date.today().isoformat(),
                           'expiry': (date.today() + timedelta(days=30)).isoformat(),
                           'lot_size': 1}},
            'gates': {'HARD_CEILING_TEST_PASS': True, 'FUTURES_THROTTLE_TEST_PASS': True,
                      'SYMBOL_SELECTION_PASS': True, 'FYERS_RESOLUTION_PASS': True,
                      'COMPLETED_TRADE_EXPORT_PASS': True, 'STATE_RESTART_RECONCILIATION_PASS': True},
            'events': [], 'completed': {}, 'state': 'IDLE'
        }

    def transaction(self):
        store_self = self
        class Transaction:
            def __enter__(self):
                return store_self.data, None
            def __exit__(self, exc_type, exc_val, exc_tb):
                pass
        return Transaction()

    def log(self, conn, record):
        pass

    def snapshot(self):
        return self.data

def test_global_halt_activation():
    store = MockStore()
    risk = RiskEngine(store, Policy(dd_update='LOSS_MINUS_WIN_FLOORED'))

    # ₹49,999 does not halt
    store.data['cash_loss'] = '49999'
    store.data['futures_loss'] = '0'
    risk.refresh(store.data)
    assert store.data['halt'] is False

    # ₹50,000 activates GLOBAL_HALT
    store.data['cash_loss'] = '50000'
    risk.refresh(store.data)
    assert store.data['halt'] is True
    assert risk.cash_entry_allowed() == 'GLOBAL_REALIZED_LOSS_CEILING'

def test_futures_serial_locking():
    store = MockStore()
    risk = RiskEngine(store, Policy(dd_update='LOSS_MINUS_WIN_FLOORED'))
    risk.ready = True

    sig = Signal(signal_id='S1', symbol='SINE', timeframe=1, entry=100, stop=90, target=110, contract='NSE:SINEFUT', lot_size=1)

    # First reservation succeeds
    res1 = risk.reserve(sig)
    assert res1 == 'FUT_ALLOW'
    assert store.data['state'] == 'ENTRY_GTT_RESTING'

    # Multiple pending reservations are allowed before a winner exists.
    sig2 = Signal(signal_id='S2', symbol='SINE', timeframe=1, entry=100, stop=90, target=110, contract='NSE:SINEFUT', lot_size=1)
    res2 = risk.reserve(sig2)
    assert res2 == 'FUT_ALLOW'
    assert store.data.get('serial_reservation') is None

def test_futures_position_lock():
    store = MockStore()
    risk = RiskEngine(store, Policy(dd_update='LOSS_MINUS_WIN_FLOORED'))
    risk.ready = True

    sig = Signal(signal_id='S1', symbol='SINE', timeframe=1, entry=100, stop=90, target=110, contract='NSE:SINEFUT', lot_size=1)
    risk.reserve(sig)

    # Fill the order -> move to POSITION_OPEN_LOCKED
    risk.resolve_order('S1', filled_quantity=1, actual_entry=100, active_stop=90, terminal=True, broker_verified=True)
    assert store.data['state'] == 'POSITION_OPEN_LOCKED'

    # Next entry blocked
    sig2 = Signal(signal_id='S2', symbol='SINE', timeframe=1, entry=100, stop=90, target=110, contract='NSE:SINEFUT', lot_size=1)
    assert risk.reserve(sig2) == 'FUT_REJECT_ACTIVE_FUTURES_WINNER'

def test_live_risk_formula():
    store = MockStore()
    risk = RiskEngine(store, Policy(dd_update='LOSS_MINUS_WIN_FLOORED'))
    risk.ready = True

    # Entry 100, Stop 90, Lot 1 -> Risk = 10 * 1 = 10
    sig = Signal(signal_id='S1', symbol='SINE', timeframe=1, entry=100, stop=90, target=110, contract='NSE:SINEFUT', lot_size=1)

    # We can't easily check the internal 'risk' variable, but we can check if it allows/rejects based on MAX_SINGLE_FUTURES_TRADE_RISK
    # Set a very low risk cap for the test
    import scripts.futures_risk_engine as risk_mod
    old_cap = risk_mod.MAX_SINGLE_FUTURES_TRADE_RISK
    risk_mod.MAX_SINGLE_FUTURES_TRADE_RISK = 5
    try:
        res = risk.reserve(sig)
        assert res == 'FUT_REJECT_TRADE_RISK_GT_14000' # The reason code is hardcoded in RiskEngine
    finally:
        risk_mod.MAX_SINGLE_FUTURES_TRADE_RISK = old_cap

def test_futures_enabled_fail_closed():
    store = MockStore()
    store.data['reconciled'] = False
    risk = RiskEngine(store, Policy(dd_update='LOSS_MINUS_WIN_FLOORED'))
    risk.ready = True

    # Policy configuration alone does not enable an unreconciled account.
    assert risk.futures_enabled is False


def fyers_client():
    client = FyersClient.__new__(FyersClient)
    client._client = SimpleNamespace(
        place_order=MagicMock(return_value={'s': 'ok'}),
        place_gtt_order=MagicMock(return_value={'s': 'ok'}),
    )
    return client


def test_global_halt_blocks_new_cash_entry(monkeypatch):
    client = fyers_client()
    monkeypatch.setattr(client, '_assert_cash_entry_allowed',
                        MagicMock(side_effect=RuntimeError('GLOBAL_REALIZED_LOSS_CEILING')))

    with pytest.raises(RuntimeError, match='GLOBAL_REALIZED_LOSS_CEILING'):
        client.place_cash_gtt({'symbol': 'NSE:SBIN-EQ'})

    client._client.place_gtt_order.assert_not_called()


def test_protective_broker_action_available_during_global_halt(monkeypatch):
    client = fyers_client()
    monkeypatch.setattr(client, '_assert_cash_entry_allowed',
                        MagicMock(side_effect=RuntimeError('GLOBAL_REALIZED_LOSS_CEILING')))

    assert client.place_protective_gtt({'symbol': 'NFO:SFUT'}) == {'s': 'ok'}

    client._client.place_gtt_order.assert_called_once_with(data={'symbol': 'NFO:SFUT'})
