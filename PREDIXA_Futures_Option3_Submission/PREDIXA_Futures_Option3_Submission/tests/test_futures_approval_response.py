from types import SimpleNamespace

from apps.backend.api.futures_approval_response import build_futures_response


def test_response_exposes_authoritative_risk_context():
    row = SimpleNamespace(
        bucket_id=23, trade_signal_id=11929, stock_tick="MOTHERSON",
        time_frame=1, order_type="BUY", stock_quantity=6150,
        entry_price=147.351, stoploss_price=143.319,
        target_price=158.159, expiry_date="24092026",
        approval_status="approved", is_trade_started=0,
    )
    risk = SimpleNamespace(last_decision={
        "risk_per_unit": 4.032,
        "trade_risk": 24796.8,
        "current_open_futures_risk": 0,
        "proposed_open_futures_risk": 24796.8,
        "configured_trade_risk_cap": 18000,
        "cash_realized_loss": 0,
        "futures_realized_loss": 0,
        "combined_realized_loss": 0,
        "global_loss_ceiling": 50000,
        "global_halt": False,
    })

    result = build_futures_response(
        reason_code="FUT_REJECT_TRADE_RISK_GT_CONFIGURED_CAP",
        http_status=409, row=row, risk=risk,
    )

    assert result["reason_code"] == "FUT_REJECT_TRADE_RISK_GT_CONFIGURED_CAP"
    assert result["calculation"]["risk_per_unit"] == 4.032
    assert result["calculation"]["trade_risk"] == 24796.8
    assert result["calculation"]["configured_trade_risk_cap"] == 18000.0
    assert result["calculation"]["excess_risk"] == 6796.8
    assert result["risk_state"]["proposed_open_futures_risk"] == 24796.8
    assert "24,796.80" in result["message"]
    assert "18,000.00" in result["message"]
    assert "token" not in str(result).lower()
