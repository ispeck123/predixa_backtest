from types import SimpleNamespace

import pytest

from scripts import futures_graduated_live_scanner as scanner


def job(symbol="RELIANCE", exp_num="26092026", timeframe_id=1, exchange_id=11, is_future=True):
    return scanner.FuturesScanJob(
        symbol=symbol,
        expiry=exp_num,
        exp_num=exp_num,
        timeframe_id=timeframe_id,
        time_list=["monthly", "weekly", "daily"],
        exchange_id=exchange_id,
        is_future=is_future,
        contract=f"NSE:{symbol}26SEPFUT",
        lot_size=100,
    )


def formatted_buy(rrr=2.2):
    return {
        "STOCK_NAME": "RELIANCE",
        "PRICE_CMP": 2490.0,
        "BUY": {
            "entry_price": 2500.0,
            "stop_loss": 2450.0,
            "target_price": 2610.0,
            "zone_signature": "RELIANCE:X:1:2",
            "is_execute_tf": True,
            "retest_count": 1,
        },
        "BUY_RRR": rrr,
        "BUY_TIMESTAMPS": {"entry_price_timestamp": 1810000000},
    }


def formatted_sell_only():
    return {
        "STOCK_NAME": "RELIANCE",
        "PRICE_CMP": 2490.0,
        "SELL": {
            "entry_price": 2480.0,
            "stop_loss": 2520.0,
            "target_price": 2390.0,
        },
        "SELL_RRR": 2.5,
    }


def run_with(formatted_results, jobs):
    formatted_iter = iter(formatted_results)
    inserted = []

    def provider(timeframe_ids, max_symbols, last_d_time, min_expiry_days):
        assert timeframe_ids == [1]
        assert max_symbols is None
        assert min_expiry_days == scanner.DEFAULT_MIN_EXPIRY_DAYS
        return jobs

    def setup_runner(scan_job):
        return {"raw": scan_job.symbol}

    def formatter(raw, scan_job):
        return next(formatted_iter)

    sut = scanner.FuturesGraduatedLiveReadonlyScanner(
        job_provider=provider,
        setup_runner=setup_runner,
        formatter=formatter,
        signal_inserter=lambda trade, country_id, exchange_id, timeframe_id: inserted.append(
            (trade, country_id, exchange_id, timeframe_id)
        ),
        candidate_persister=lambda candidate, scan_job, signal_result: None,
    )
    return sut.run(timeframe_ids=[1], write_files=False), inserted


def test_buy_candidate_is_accepted():
    outputs, inserted = run_with([formatted_buy(2.2)], [job()])

    assert len(outputs.candidates) == 1
    candidate = outputs.candidates[0]
    assert candidate["side"] == "BUY"
    assert candidate["exchange_id"] == 11
    assert candidate["is_future"] is True
    assert candidate["entry"] == 2500.0
    assert candidate["stop_loss"] == 2450.0
    assert candidate["target"] == 2610.0
    assert candidate["BUY_RRR"] == 2.2
    assert outputs.diagnostics[0]["scan_status"] == "ACCEPTED"
    assert len(inserted) == 1
    trade, country_id, exchange_id, timeframe_id = inserted[0]
    assert trade["TRADE_TYPE"] == "BUY"
    assert trade["PREDICTION"] == "NA"
    assert trade["PROBABILITY"] == 0.0
    assert trade["EXP_NUM"] == "26092026"
    assert country_id == 1
    assert exchange_id == 11
    assert timeframe_id == 1


def test_low_rr_is_rejected():
    outputs, inserted = run_with([formatted_buy(2.09)], [job()])

    assert outputs.candidates == []
    assert outputs.diagnostics[0]["scan_status"] == "BUY_RRR_TOO_LOW"
    assert inserted == []


def test_sell_only_never_becomes_candidate():
    outputs, inserted = run_with([formatted_sell_only()], [job()])

    assert outputs.candidates == []
    assert outputs.diagnostics[0]["scan_status"] == "NO_SETUP"
    assert outputs.diagnostics[0]["rejection_reason"] == "NO_BUY_SETUP"
    assert inserted == []


@pytest.mark.parametrize("formatted", [{}, {"BUY": None}, None])
def test_no_setup_produces_no_candidate(formatted):
    outputs, inserted = run_with([formatted], [job()])

    assert outputs.candidates == []
    assert outputs.diagnostics[0]["scan_status"] in {"NO_SETUP", "INVALID_RESULT"}
    assert inserted == []


def test_multiple_futures_scan_without_db_or_broker_mutation():
    jobs = [job("RELIANCE"), job("TCS")]
    outputs, inserted = run_with([formatted_buy(2.2), formatted_buy(2.3)], jobs)

    assert outputs.total_jobs == 2
    assert outputs.processed == 2
    assert outputs.errors == 0
    assert [candidate["symbol"] for candidate in outputs.candidates] == ["RELIANCE", "TCS"]
    assert len(inserted) == 2


def test_default_jobs_use_fixed_allowed_symbols_and_dynamic_expiries_only(monkeypatch):
    import scripts.scanner_fc as scanner_fc

    monkeypatch.setattr(
        scanner_fc,
        "build_futures_expiry_map",
        lambda min_days, time_fr, allowed_symbols: [
            {"symbol": "CANBK", "expiry": ["29092026", "27102026"]},
            {"symbol": "HDFCBANK", "expiry": ["29092026"]},
            {"symbol": "RELIANCE", "expiry": ["29092026"]},
        ],
    )

    jobs = list(scanner.default_futures_jobs([1], max_symbols=None, last_d_time=None))

    assert [item.symbol for item in jobs] == ["CANBK", "CANBK", "HDFCBANK"]
    assert [item.expiry for item in jobs] == ["29092026", "27102026", "29092026"]
    assert [item.exp_num for item in jobs] == ["29092026", "27102026", "29092026"]
    assert [item.contract for item in jobs] == [None, None, None]
    assert all(item.exchange_id == 11 for item in jobs)
    assert all(item.is_future is True for item in jobs)


def test_fixed_allowed_symbols_max_symbols_limits_symbols_not_expiries(monkeypatch):
    import scripts.scanner_fc as scanner_fc

    monkeypatch.setattr(
        scanner_fc,
        "build_futures_expiry_map",
        lambda min_days, time_fr, allowed_symbols: [
            {"symbol": "CANBK", "expiry": ["29092026", "27102026", "23112026"]},
            {"symbol": "HDFCBANK", "expiry": ["29092026", "27102026", "23112026"]},
        ],
    )

    jobs = list(scanner.default_futures_jobs([1], max_symbols=1, last_d_time=None))

    assert len(jobs) == 3
    assert {item.symbol for item in jobs} == {"CANBK"}


def test_default_jobs_passes_five_day_expiry_filter_to_scanner_fc(monkeypatch):
    calls = []

    import scripts.scanner_fc as scanner_fc

    def fake_build_futures_expiry_map(min_days, time_fr, allowed_symbols):
        calls.append((min_days, time_fr))
        return [{"symbol": "CANBK", "expiry": ["27102026"]}]

    monkeypatch.setattr(scanner_fc, "build_futures_expiry_map", fake_build_futures_expiry_map)

    jobs = list(scanner.default_futures_jobs([1], max_symbols=None, last_d_time=None, min_expiry_days=5))

    assert calls == [(5, 1)]
    assert [item.exp_num for item in jobs] == ["27102026"]


def test_broker_mutation_methods_are_not_called(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("broker mutation called")

    from shared.db import fyers_req_model

    for name in (
        "place_order",
        "place_cash_gtt",
        "place_futures_entry_gtt",
        "cancel_gtt_order",
    ):
        monkeypatch.setattr(fyers_req_model.FyersClient, name, forbidden, raising=False)

    fake_client = SimpleNamespace(
        place_gtt_order=forbidden,
        modify_gtt_order=forbidden,
        cancel_gtt_order=forbidden,
        exit_positions=forbidden,
    )
    monkeypatch.setattr(fyers_req_model.FyersClient, "_client", fake_client, raising=False)

    outputs, inserted = run_with([formatted_buy(2.2)], [job()])

    assert len(outputs.candidates) == 1
    assert len(inserted) == 1


def test_trade_insert_function_is_called_for_accepted_candidate():
    def forbidden(*args, **kwargs):
        raise AssertionError("unused")

    outputs, inserted = run_with([formatted_buy(2.2)], [job()])

    assert len(outputs.candidates) == 1
    assert len(inserted) == 1


def test_futures_only_rejects_non_nsefo_path():
    outputs, inserted = run_with([formatted_buy(2.2)], [job(exchange_id=10, is_future=False)])

    assert outputs.candidates == []
    assert outputs.diagnostics[0]["scan_status"] == "INVALID_RESULT"
    assert outputs.diagnostics[0]["rejection_reason"] == "NOT_NSE_FUTURES"
    assert inserted == []


def test_buy_only_rejects_sell_result_even_when_sell_rrr_qualifies():
    outputs, inserted = run_with([formatted_sell_only()], [job()])

    assert outputs.candidates == []
    assert all(candidate["side"] == "BUY" for candidate in outputs.candidates)
    assert inserted == []
