"""Offline graduated-live acceptance simulations; no application/broker imports."""
import json
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import MagicMock
import pytest
from sqlalchemy import create_engine, select
from scripts.futures_state_store import StateStore, audit
from scripts.futures_risk_engine import RiskEngine, Signal, Policy
from scripts.graduated_live_config import GATES
from scripts.graduated_live_execution import GraduatedLiveExecution
from shared.db.fyers_req_model import FyersClient
from scripts.futures_selector import select_futures
from scripts.graduated_live_exports import evaluate_tracks, export_completed, DataQualityFail

@pytest.fixture
def engine(tmp_path):
    store = StateStore(create_engine('sqlite:///' + str(tmp_path / 'risk.sqlite'), connect_args={'timeout': 15}))
    store.initialize()
    r = RiskEngine(store, Policy(dd_update='LOSS_MINUS_WIN_FLOORED'))
    reconcile(r)
    return r

def reconcile(r, dd=0, positions=None, cash_loss=0, futures_loss=0):
    return r.reconcile(positions or {}, dd=dd, cash_loss=cash_loss, futures_loss=futures_loss,
        approved={'S': {'contract': 'NSE:SFUT', 'lot_size': 100, 'fyers_resolved': True,
            'as_of': datetime.now(timezone.utc).date().isoformat(),
            'expiry': (datetime.now(timezone.utc).date()+timedelta(days=30)).isoformat()}},
        gates=dict.fromkeys(GATES, True), unresolved_orders=[], source_verified=True)

def signal(risk=1000, signal_id='s'):
    return Signal(signal_id, 'S', 1, 1000, 1000-risk/100, 1200, 'NSE:SFUT', 100)

def position(risk, qty=100):
    return dict(entry='1000', stop=str(1000-risk/qty), quantity=qty, lot_size=100, contract='NSE:SFUT')

@pytest.mark.parametrize('risk,reason', [(13999,'FUT_ALLOW'), (14000,'FUT_ALLOW'), (14001,'FUT_REJECT_TRADE_RISK_GT_14000')])
def test_single_trade_limit(engine, risk, reason):
    assert engine.reserve(signal(risk)) == reason

@pytest.mark.parametrize('risk,reason', [(9500,'FUT_ALLOW'), (10000,'FUT_ALLOW'), (10001,'FUT_REJECT_OPEN_RISK_GT_18000')])
def test_open_risk(engine, risk, reason):
    reconcile(engine, positions={'old': position(8000)})
    assert engine.reserve(signal(risk)) == reason

@pytest.mark.parametrize('dd,cap,reason', [(0,2,'FUT_ALLOW'), (14999,2,'FUT_ALLOW'),
    (15000,1,'FUT_ALLOW'), (24999,1,'FUT_ALLOW'), (25000,None,'FUT_REJECT_DD_BAND_UNDEFINED'),
    (29999,None,'FUT_REJECT_DD_BAND_UNDEFINED'), (30000,0,'FUT_REJECT_TAP_CLOSED'),
    (30500,0,'FUT_REJECT_TAP_CLOSED')])
def test_dd_bands(engine, dd, cap, reason):
    reconcile(engine, dd)
    assert engine.refresh(engine.store.snapshot()) == cap
    assert engine.reserve(signal()) == reason

@pytest.mark.parametrize('dd,closed', [(25000,True), (22000,True), (21999,False)])
def test_tap_hysteresis(engine, dd, closed):
    reconcile(engine, 30500)
    restarted = RiskEngine(engine.store, engine.policy)
    reconcile(restarted, dd)
    assert restarted.store.snapshot()['tap'] is closed

@pytest.mark.parametrize('dd,n', [(0,2), (15000,1)])
def test_concurrency(engine, dd, n):
    reconcile(engine, dd, {str(i): position(1000) for i in range(n)})
    assert engine.reserve(signal()) == 'FUT_REJECT_CONCURRENCY_LIMIT'

def test_atomicity_across_connections(engine):
    reconcile(engine, positions={'old': position(8000)})
    other = RiskEngine(StateStore(engine.store.engine), engine.policy)
    reconcile(other, positions={'old': position(8000)})
    with ThreadPoolExecutor(2) as pool:
        a = pool.submit(engine.reserve, signal(6000, 'A'))
        b = pool.submit(other.reserve, signal(6000, 'B'))
        outcomes = [a.result(), b.result()]
    assert outcomes.count('FUT_ALLOW') == 1
    assert engine.exposure(engine.store.snapshot())[0] == 14000

@pytest.mark.parametrize('loss,halt', [(49999,False), (50000,True), (50001,True)])
def test_global_ceiling(engine, loss, halt):
    reconcile(engine, cash_loss=20000, futures_loss=loss-20000)
    assert engine.store.snapshot()['halt'] is halt
    assert (engine.cash_entry_allowed() == 'GLOBAL_REALIZED_LOSS_CEILING') is halt
    assert (engine.reserve(signal()) == 'GLOBAL_REALIZED_LOSS_CEILING') is halt
    if halt:
        restarted = RiskEngine(engine.store, engine.policy)
        reconcile(restarted)
        assert restarted.cash_entry_allowed() == 'GLOBAL_REALIZED_LOSS_CEILING'
    broker = SimpleNamespace(_client=SimpleNamespace(exit_positions=lambda data: 'closed'))
    assert GraduatedLiveExecution(engine, broker).close_position({'id': 'position'}) == 'closed'

def test_restart(engine):
    reconcile(engine, 18000, {'old': position(7000)})
    restarted = RiskEngine(engine.store, engine.policy)
    assert restarted.store.snapshot()['dd'] == '18000'
    assert restarted.exposure(restarted.store.snapshot())[0] == 7000
    assert restarted.reserve(signal()) == 'FUT_REJECT_STATE_RECONCILIATION_FAILED'
    assert reconcile(restarted, 18000, {'old': position(7000)})
    assert restarted.refresh(restarted.store.snapshot()) == 1

def payload(s):
    return dict(symbol=s.contract, qty=s.lot_size, side=1, type=1, limitPrice=s.entry)

def test_broker_definite_failure(engine):
    s = signal()
    broker = SimpleNamespace(place_order=lambda p: {'s': 'error'})
    GraduatedLiveExecution(engine, broker).submit_futures(s, payload(s))
    assert engine.exposure(engine.store.snapshot())[0] == 1000
    engine.resolve_order(s.signal_id, filled_quantity=0, actual_entry=None, active_stop=None,
                         terminal=True, broker_verified=True)
    assert engine.exposure(engine.store.snapshot())[0] == 0

def test_broker_ambiguous_failure(engine):
    def fail(p):
        raise TimeoutError('unknown acceptance')
    with pytest.raises(TimeoutError):
        GraduatedLiveExecution(engine, SimpleNamespace(place_order=fail)).submit_futures(signal(), payload(signal()))
    assert engine.exposure(engine.store.snapshot())[0] == 1000
    assert not engine.ready
    assert not reconcile(engine)

def test_partial_fill_and_active_stop(engine):
    engine.reserve(signal())
    engine.resolve_order('s', filled_quantity=40, actual_entry=1001, active_stop=990,
                         terminal=False, broker_verified=True)
    s = engine.store.snapshot()
    assert s['reservations']['s']['quantity'] == 60
    assert engine.exposure(s)[0] == 1040
    engine.resolve_order('s', filled_quantity=40, actual_entry=1001, active_stop=990,
                         terminal=True, broker_verified=True)
    assert engine.exposure(engine.store.snapshot())[0] == 440
    engine.update_stop('s', 995)
    assert engine.exposure(engine.store.snapshot())[0] == 240

@pytest.mark.parametrize('changes,reason', [({'entry':None}, 'FUT_REJECT_MISSING_ENTRY'),
    ({'stop':None}, 'FUT_REJECT_MISSING_STOP'), ({'lot_size':0}, 'FUT_REJECT_INVALID_LOT_SIZE'),
    ({'contract':'unknown'}, 'FUT_REJECT_SYMBOL_UNRESOLVED'),
    ({'entry':float('nan')}, 'FUT_REJECT_MISSING_ENTRY'),
    ({'stop':float('inf')}, 'FUT_REJECT_MISSING_STOP'),
    ({'side':'SELL'}, 'FUT_REJECT_INVALID_LONG_SIGNAL')])
def test_invalid_signal(engine, changes, reason):
    assert engine.reserve(replace(signal(), **changes)) == reason
    assert engine.store.snapshot()['reservations'] == {}
    with engine.store.engine.connect() as c:
        row = json.loads(c.execute(select(audit.c.body).order_by(audit.c.id.desc())).first()[0])
    assert row['reason'] == reason
    assert {'timestamp','signal_id','zone_id','OPEN_FUT_RISK','proposed_OPEN_FUT_RISK',
            'REALIZED_FUT_DD','tap','max_lots','global_halt','target'} <= row.keys()

def test_suppression_never_submits(engine):
    calls = []
    s = signal(14001)
    gateway = GraduatedLiveExecution(engine, SimpleNamespace(place_order=lambda p: calls.append(p)))
    assert not gateway.submit_futures(s, payload(s))['submitted']
    assert calls == []

def test_default_policy_and_cash_independence(engine):
    r = RiskEngine(engine.store)
    reconcile(r)
    assert r.reserve(signal()) == 'FUT_REJECT_DD_POLICY_UNAPPROVED'
    assert r.cash_entry_allowed() == 'CASH_ALLOW'

@pytest.mark.parametrize('gate', GATES)
def test_each_enablement_gate(engine, gate):
    with engine.store.transaction() as (s, _):
        s['gates'][gate] = False
    assert engine.reserve(signal()) == 'FUT_REJECT_ENABLEMENT_GATES'
    assert engine.cash_entry_allowed() == 'CASH_ALLOW'

def test_dd_updates_idempotent(engine):
    engine.record_realized('a', 'FUTURES', -30500, cash_loss=0, futures_loss=30500)
    engine.record_realized('a', 'FUTURES', -30500, cash_loss=0, futures_loss=30500)
    assert engine.store.snapshot()['dd'] == '30500'
    engine.record_realized('b', 'FUTURES', 8501, cash_loss=0, futures_loss=30500)
    assert engine.store.snapshot()['dd'] == '21999'
    assert not engine.store.snapshot()['tap']

def test_duplicate(engine):
    assert engine.reserve(signal()) == 'FUT_ALLOW'
    assert engine.reserve(signal()) == 'FUT_REJECT_DUPLICATE_SIGNAL'

def selection_inputs():
    universe = [f'S{i}' for i in range(8)]
    candidates = [dict(symbol=s, contract=s+'FUT', futures_price=100+i,
                       median_stop_pct=1, liquidity_metric=1000) for i,s in enumerate(universe)]
    master = {s+'FUT': dict(symbol=s, instrument_type='FUTURES', expiry='2026-09-30', lot_size=100) for s in universe}
    return universe, candidates, master

def selection(*args, **kw):
    return select_futures(*args, count=6, liquidity_floor=500, as_of='2026-09-07',
                          master_as_of='2026-09-07', market_as_of='2026-09-07', **kw)

def test_mechanical_selection():
    u,c,m = selection_inputs()
    result = selection(u,c,m)
    assert list(result['approved']) == u[:6]
    assert result['rows'][0]['ranking_per_lot_risk'] == '100'
    assert result['passed']

@pytest.mark.parametrize('bad', ['outside','missing_lot','median','liquidity','unresolved','expired'])
def test_selection_rejections(bad):
    u,c,m = selection_inputs()
    if bad == 'outside': u.remove('S0')
    if bad == 'missing_lot': m['S0FUT'].pop('lot_size')
    if bad == 'median': c[0].pop('median_stop_pct')
    if bad == 'liquidity': c[0]['liquidity_metric'] = 1
    if bad == 'unresolved': m.pop('S0FUT')
    if bad == 'expired': m['S0FUT']['expiry'] = '2026-01-01'
    assert 'S0' not in selection(u,c,m)['approved']

def completed(segment='CASH', **kw):
    return dict(segment=segment, symbol='S', tf=1, side='BUY', zone_type='DZ', entry_price=100,
                stoploss_price=90, target_price=120, real_entry_price=100, real_exit_price=120,
                applicable_stop=90, exit_leg='TARGET', status='COMPLETED', **kw)

def test_completed_exports_and_separate_gates(tmp_path):
    rows = [completed()] * 40 + [completed('FUTURES')] * 20
    report = evaluate_tracks(rows, ci_critical=lambda n: 1.96)
    assert report['CASH']['full_pass']
    assert report['FUTURES']['gross_pass'] and not report['FUTURES']['full_pass']
    export_completed(rows, tmp_path/'completed.csv')
    assert len((tmp_path/'completed.csv').read_text().splitlines()) == 61

@pytest.mark.parametrize('field', ['real_entry_price','real_exit_price','entry_price', 'stoploss_price',
                                  'target_price','segment','exit_leg','applicable_stop'])
def test_malformed_completed_blocks_gate(tmp_path, field):
    bad = completed(); bad.pop(field)
    rows = [completed()] * 40 + [bad]
    assert evaluate_tracks(rows)['errors']
    assert not evaluate_tracks(rows, ci_critical=lambda n: 1.96)['CASH']['full_pass']
    with pytest.raises(DataQualityFail): export_completed(rows, tmp_path/'bad.csv')
    assert not (tmp_path/'bad.csv').exists()

def test_open_not_completed():
    assert evaluate_tracks([{'status':'OPEN'}])['CASH']['n'] == 0

@pytest.mark.parametrize('changes', [{'entry':None}, {'stop':None}, {'lot_size':0}, {'entry':float('nan')}])
def test_invalid_gateway_never_submits(engine, changes):
    s = replace(signal(), **changes)
    calls = []
    result = GraduatedLiveExecution(engine, SimpleNamespace(place_order=lambda p: calls.append(p))).submit_futures(s, payload(s))
    assert not result['submitted'] and not calls

def test_payload_mismatch_releases_without_submission(engine):
    calls = []
    with pytest.raises(ValueError):
        GraduatedLiveExecution(engine, SimpleNamespace(place_order=lambda p: calls.append(p))).submit_futures(signal(), {})
    assert not calls and not engine.store.snapshot()['reservations']

def test_completed_lifecycle_persistence(engine):
    engine.reserve(signal())
    engine.resolve_order('s', filled_quantity=100, actual_entry=1000, active_stop=990,
                         terminal=True, broker_verified=True)
    row = completed('FUTURES', trade_id='T', signal_id='s', quantity=100, realized_pnl=20000)
    row.update(entry_price=1000, stoploss_price=990, target_price=1200,
               real_entry_price=1000, real_exit_price=1200, applicable_stop=990)
    engine.complete_trade(row, cash_loss=0, futures_loss=0, broker_verified=True)
    engine.complete_trade(row, cash_loss=0, futures_loss=0, broker_verified=True)
    state = engine.store.snapshot()
    assert not state['positions'] and len(state['completed']) == 1
    assert engine.reserve(signal()) == 'FUT_REJECT_DUPLICATE_SIGNAL'

def test_reconciliation_failure_disables(engine):
    assert not engine.reconcile({}, dd=0, cash_loss=0, futures_loss=0, approved={}, gates={},
                                unresolved_orders=['unknown'], source_verified=True)
    assert engine.reserve(signal()) == 'FUT_REJECT_STATE_RECONCILIATION_FAILED'
    assert engine.cash_entry_allowed() == 'CASH_ALLOW'

def test_unknown_dd_policy_records_ceiling(engine):
    r = RiskEngine(engine.store)
    reconcile(r)
    r.record_realized('unknown-policy', 'FUTURES', -50000, cash_loss=0, futures_loss=50000)
    assert r.cash_entry_allowed() == 'GLOBAL_REALIZED_LOSS_CEILING'
    assert not r.ready


def test_stale_master_rejected(engine):
    with engine.store.transaction() as (s, _):
        s['approved']['S']['as_of'] = '2000-01-01'
    assert engine.reserve(signal()) == 'FUT_REJECT_SYMBOL_UNRESOLVED'

def test_enablement_defaults(engine):
    assert engine.futures_enabled
    assert not RiskEngine(engine.store).futures_enabled

@pytest.mark.parametrize('n,gross,full', [(19,False,False),(20,True,False),(39,True,False),(40,True,True)])
def test_tracking_checkpoints(n, gross, full):
    report = evaluate_tracks([completed()]*n, ci_critical=lambda n: 1.96)['CASH']
    assert report['gross_pass'] is gross
    assert report['full_pass'] is full

def test_tracking_loser_and_missing_ci_approval():
    from scripts.graduated_live_exports import tracking_error
    row = completed()
    row.update(real_entry_price=101, real_exit_price=89, exit_leg='STOP')
    assert tracking_error(row) == pytest.approx(-12/11 + 1)
    assert not evaluate_tracks([completed()]*40)['CASH']['full_pass']

def test_insufficient_selection_fails_closed():
    u,c,m = selection_inputs()
    report = selection(u,c[:5],m)
    assert not report['passed'] and report['approved'] == {}

def test_explicit_band_policy(engine):
    r = RiskEngine(engine.store, Policy(dd_update='LOSS_MINUS_WIN_FLOORED', undefined_band_max_lots=1))
    reconcile(r, 26000)
    assert r.reserve(signal()) == 'FUT_ALLOW'

def test_missing_gate_defaults_disabled(engine):
    with engine.store.transaction() as (s, _):
        s['gates'] = {}
    assert not engine.futures_enabled


def nested_gtt(s=None, **overrides):
    s = s or signal()
    payload_data = dict(
        symbol=s.contract,
        side=1,
        productType='MARGIN',
        orderInfo={'leg1': {'qty': s.lot_size, 'price': s.entry, 'triggerPrice': s.entry}},
    )
    for key, value in overrides.items():
        if key == 'leg1':
            payload_data['orderInfo']['leg1'].update(value)
        else:
            payload_data[key] = value
    return payload_data


def test_nested_futures_gtt_validation_accepts_without_top_level_qty():
    p = nested_gtt()
    p.pop('qty', None)
    assert GraduatedLiveExecution._matches_single_futures(signal(), p)


@pytest.mark.parametrize('payload_change', [
    {'leg1': {'qty': 99}},
    {'symbol': 'NSE:OTHERFUT'},
    {'side': -1},
    {'leg1': {'price': 1001}},
    {'leg1': {'triggerPrice': 1001}},
])
def test_nested_futures_gtt_validation_rejects_mismatch(payload_change):
    assert not GraduatedLiveExecution._matches_single_futures(signal(), nested_gtt(**payload_change))


def fyers_client():
    client = FyersClient.__new__(FyersClient)
    client._client = SimpleNamespace(place_gtt_order=MagicMock(return_value={'s': 'ok', 'id': 'gtt'}))
    return client


def set_entry_reserving(engine, s=None):
    s = s or signal()
    with engine.store.transaction() as (state, _):
        state['mode'] = 'OPTION3_SERIAL'
        state['state'] = 'ENTRY_RESERVING'
        state['reservations'] = {
            s.signal_id: dict(
                entry=str(s.entry),
                stop=str(s.stop),
                target=str(s.target),
                quantity=s.lot_size,
                requested_quantity=s.lot_size,
                lot_size=s.lot_size,
                contract=s.contract,
                signal_id=s.signal_id,
                timeframe=s.timeframe,
            )
        }
        state['serial_reservation'] = None
        state['active_winner'] = None
    return s


def assert_boundary_rejects(engine, client, payload_data):
    with pytest.raises(RuntimeError):
        client.place_futures_entry_gtt(payload_data, risk=engine)
    client._client.place_gtt_order.assert_not_called()


def test_futures_boundary_accepts_without_serial_reservation(engine):
    s = set_entry_reserving(engine)
    with engine.store.transaction() as (state, _):
        state['serial_reservation'] = None
        state['active_winner'] = None
    client = fyers_client()
    assert client.place_futures_entry_gtt(nested_gtt(s), risk=engine) == {'s': 'ok', 'id': 'gtt'}
    client._client.place_gtt_order.assert_called_once()


def test_futures_boundary_rejects_non_reserving_state(engine):
    s = set_entry_reserving(engine)
    with engine.store.transaction() as (state, _):
        state['state'] = 'IDLE'
    assert_boundary_rejects(engine, fyers_client(), nested_gtt(s))


@pytest.mark.parametrize('payload_change', [
    {'symbol': 'NSE:OTHERFUT'},
    {'leg1': {'qty': 99}},
    {'leg1': {'price': 1001}},
])
def test_futures_boundary_rejects_reservation_payload_mismatch(engine, payload_change):
    s = set_entry_reserving(engine)
    assert_boundary_rejects(engine, fyers_client(), nested_gtt(s, **payload_change))


def test_futures_boundary_accepts_matching_reservation_payload(engine):
    s = set_entry_reserving(engine)
    client = fyers_client()
    assert client.place_futures_entry_gtt(nested_gtt(s), risk=engine) == {'s': 'ok', 'id': 'gtt'}
    client._client.place_gtt_order.assert_called_once_with(data=nested_gtt(s))


def test_remaining_quantity_zero_moves_to_exit_reconciling(engine):
    engine.reserve(signal())
    engine.resolve_order('s', filled_quantity=100, actual_entry=1000, active_stop=990,
                         terminal=True, broker_verified=True)
    engine.apply_exit_fill('s', remaining_quantity=0, broker_verified=True)
    state = engine.store.snapshot()
    assert state['positions'] == {}
    assert state['state'] == 'EXIT_RECONCILING'
