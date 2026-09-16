from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from scripts import futures_graduated_live_scanner as scanner
from shared.db.db_model import Base, Future_Order, OMSOrderBucketFuture, TradeSignal


def _formatted(symbol, entry=2500.0, stop=2450.0, target=2610.0, rrr=2.2):
    return {
        "STOCK_NAME": symbol,
        "PRICE_CMP": entry - 10,
        "BUY": {
            "entry_price": entry,
            "stop_loss": stop,
            "target_price": target,
            "zone_signature": f"{symbol}:zone",
        },
        "BUY_RRR": rrr,
        "BUY_TIMESTAMPS": {"entry_price_timestamp": 1810000000},
    }


def _job(symbol, expiry="29092026", lot_size=75):
    return scanner.FuturesScanJob(
        symbol=symbol,
        expiry=expiry,
        exp_num=expiry,
        timeframe_id=1,
        time_list=["monthly", "weekly", "daily"],
        contract=f"NSE:{symbol}26SEPFUT",
        lot_size=lot_size,
    )


def _database(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr(scanner.db_utils, "dbc", SimpleNamespace(get_session=factory))
    return factory


def _signal_inserter(factory):
    def insert(trade, country_id, exchange_id, timeframe_id):
        session = factory()
        try:
            existing = session.query(TradeSignal).filter(
                TradeSignal.stock_name == trade["STOCK_NAME"],
                TradeSignal.time_fr == timeframe_id,
                TradeSignal.trade_type == "BUY",
                TradeSignal.exp_date == str(trade["EXP_NUM"]),
            ).first()
            if existing:
                return False, None
            row = TradeSignal(
                exchange_id=exchange_id,
                stock_name=trade["STOCK_NAME"],
                time_fr=timeframe_id,
                country_id=country_id,
                price_cmp=trade["PRICE_CMP"],
                trade_type="BUY",
                rrr=trade["BUY_RRR"],
                timestamps=trade["BUY_TIMESTAMPS"],
                trade_data=trade["BUY"],
                prediction="NA",
                probability=0.0,
                created_at=scanner.datetime.now(),
                is_active=True,
                is_send=0,
                exp_date=str(trade["EXP_NUM"]),
            )
            session.add(row)
            session.commit()
            return True, row.id
        finally:
            session.close()
    return insert


def _run(jobs, formatted, factory):
    formatted_iter = iter(formatted)
    sut = scanner.FuturesGraduatedLiveReadonlyScanner(
        job_provider=lambda *_args: jobs,
        setup_runner=lambda job: {"symbol": job.symbol},
        formatter=lambda raw, job: next(formatted_iter),
        signal_inserter=_signal_inserter(factory),
    )
    return sut.run(timeframe_ids=[1], write_files=False)


def test_scanner_creates_pending_futures_candidate_and_is_idempotent(monkeypatch):
    factory = _database(monkeypatch)
    job = _job("CANBK", lot_size=75)

    first = _run([job], [_formatted("CANBK")], factory)
    second = _run([job], [_formatted("CANBK")], factory)

    session = factory()
    try:
        signals = session.query(TradeSignal).all()
        futures = session.query(Future_Order).all()
        oms_rows = session.query(OMSOrderBucketFuture).all()
        assert len(first.candidates) == 1
        assert second.errors == 0
        assert len(signals) == len(futures) == len(oms_rows) == 1
        signal, future, oms = signals[0], futures[0], oms_rows[0]
        assert future.trade_signal_id == signal.id
        assert oms.trade_signal_id == signal.id
        assert oms.order_id == future.order_id
        assert oms.approval_status == "pending"
        assert oms.approved_by is None
        assert oms.approved_at is None
        assert oms.gtt_id is None
        assert oms.id_fyers is None
        assert oms.is_trade_started == 0
        assert oms.entry_price == 2500.0
        assert oms.stoploss_price == 2450.0
        assert oms.target_price == 2610.0
        assert oms.expiry_date == "29092026"
        assert oms.stock_quantity == 75
    finally:
        session.close()


def test_five_accepted_setups_create_five_pending_candidates(monkeypatch):
    factory = _database(monkeypatch)
    symbols = ["CANBK", "HDFCBANK", "PNB", "ASHOKLEY", "TATASTEEL"]
    jobs = [_job(symbol, lot_size=25 + index) for index, symbol in enumerate(symbols)]
    results = [_formatted(job.symbol, entry=1000 + index * 10) for index, job in enumerate(jobs)]

    outputs = _run(jobs, results, factory)

    session = factory()
    try:
        assert len(outputs.candidates) == 5
        assert session.query(TradeSignal).count() == 5
        assert session.query(Future_Order).count() == 5
        assert session.query(OMSOrderBucketFuture).count() == 5
        assert session.query(OMSOrderBucketFuture).filter(
            OMSOrderBucketFuture.approval_status != "pending"
        ).count() == 0
    finally:
        session.close()


def test_missing_lot_size_fails_closed_without_creating_candidate(monkeypatch):
    factory = _database(monkeypatch)
    outputs = _run([_job("CANBK", lot_size=None)], [_formatted("CANBK")], factory)

    session = factory()
    try:
        assert outputs.candidates == []
        assert outputs.diagnostics[0]["scan_status"] == "PROCESSING_ERROR"
        assert "FUT_REJECT_LOT_SIZE_UNAVAILABLE" in outputs.diagnostics[0]["rejection_reason"]
        assert session.query(Future_Order).count() == 0
        assert session.query(OMSOrderBucketFuture).count() == 0
    finally:
        session.close()


def test_scanner_persists_without_any_broker_or_reservation_path(monkeypatch):
    factory = _database(monkeypatch)
    calls = []

    def forbidden(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("FYERS mutation called")

    from shared.db import fyers_req_model
    for name in (
        "place_order", "place_gtt_order", "place_futures_entry_gtt",
        "cancel_gtt_order", "exit_positions",
    ):
        monkeypatch.setattr(fyers_req_model.FyersClient, name, forbidden, raising=False)

    outputs = _run([_job("CANBK")], [_formatted("CANBK")], factory)

    assert len(outputs.candidates) == 1
    assert calls == []
