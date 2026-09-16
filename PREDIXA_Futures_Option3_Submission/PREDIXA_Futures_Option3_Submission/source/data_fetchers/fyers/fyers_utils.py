from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN, ROUND_UP
from datetime import datetime


def get_nse_cash_tick_size(reference_price):
    """
    Fallback NSE cash tick-size calculation.

    Prefer the tick size from the FYERS Symbol Master whenever available,
    because NSE reviews tick-size assignments monthly.
    """
    price = Decimal(str(reference_price))

    if price < Decimal("250"):
        return Decimal("0.01")
    elif price <= Decimal("1000"):
        return Decimal("0.05")
    elif price <= Decimal("5000"):
        return Decimal("0.10")
    elif price <= Decimal("10000"):
        return Decimal("0.50")
    elif price <= Decimal("20000"):
        return Decimal("1.00")
    else:
        return Decimal("5.00")


def normalize_price_to_tick(price, tick_size, rounding="nearest"):
    """
    Convert a price into a valid multiple of the supplied tick size.

    rounding:
        nearest -> nearest valid tick
        down    -> lower valid tick
        up      -> higher valid tick
    """
    price = Decimal(str(price))
    tick = Decimal(str(tick_size))

    if price <= 0:
        raise ValueError("Price must be greater than zero")

    if tick <= 0:
        raise ValueError("Tick size must be greater than zero")

    rounding_modes = {
        "nearest": ROUND_HALF_UP,
        "down": ROUND_DOWN,
        "up": ROUND_UP,
    }

    if rounding not in rounding_modes:
        raise ValueError(
            "rounding must be 'nearest', 'down', or 'up'"
        )

    number_of_ticks = (price / tick).quantize(
        Decimal("1"),
        rounding=rounding_modes[rounding],
    )

    normalized_price = number_of_ticks * tick

    return float(normalized_price.quantize(tick))


def normalize_cash_oco_prices(
    entry,
    target,
    stoploss,
    order_type="BUY",
    tick_size=None,
):
    """
    Normalize all OCO prices for NSE cash orders.

    For a BUY entry:
        Target is rounded down, making the sell target slightly easier.
        Stop-loss is rounded up, avoiding additional downside risk.

    For a SELL entry:
        Target is rounded up.
        Stop-loss is rounded down.

    Pass the FYERS Symbol Master tick_size whenever available.
    Otherwise, the entry price is used to estimate it.
    """
    order_type = str(order_type).strip().upper()

    if order_type not in {"BUY", "SELL"}:
        raise ValueError("order_type must be BUY or SELL")

    tick = (
        Decimal(str(tick_size))
        if tick_size is not None
        else get_nse_cash_tick_size(entry)
    )

    normalized_entry = normalize_price_to_tick(
        entry,
        tick,
        rounding="nearest",
    )

    if order_type == "BUY":
        normalized_target = normalize_price_to_tick(
            target,
            tick,
            rounding="down",
        )
        normalized_stoploss = normalize_price_to_tick(
            stoploss,
            tick,
            rounding="up",
        )
    else:
        normalized_target = normalize_price_to_tick(
            target,
            tick,
            rounding="up",
        )
        normalized_stoploss = normalize_price_to_tick(
            stoploss,
            tick,
            rounding="down",
        )

    return {
        "entry": normalized_entry,
        "target": normalized_target,
        "stoploss": normalized_stoploss,
        "tick_size": float(tick),
    }


def get_pending_oco_by_id(fyers, gtt_id):
    pending_orders = fyers._client.gtt_orderbook().get('orderBook', [])

    for item in pending_orders:
        if (
            item.get('gtt_oco_ind') == 2
            and item.get('ord_status') == 6
            and str(item.get('id')) == str(gtt_id)
        ):
            return item

    return None

def get_pending_gtt_by_id(fyers, bucket_id):
    pending_orders = fyers._client.gtt_orderbook().get('orderBook', [])

    for item in pending_orders:
        if (
            item.get('gtt_oco_ind') == 1
            and item.get('ord_status') == 6
            and str(item.get('ordertag')).replace("1:GTT", "") == str(bucket_id)
        ):
            return item

    return None


def clear_all_futures_orders(fyers):
    try:
        gtt_order_resp = fyers._client.gtt_orderbook()
        gtt_orderbook = gtt_order_resp.get("orderBook", [])
        fut_pending_gtt_orderbook = []
        for items in gtt_orderbook:
            if items['symbol'].endswith('FUT') and items['ord_status'] == 6:
                fut_pending_gtt_orderbook.append(items)
            ###############

        print(fut_pending_gtt_orderbook)
        for items in fut_pending_gtt_orderbook:
            print(items['symbol'])
            fyers.cancel_gtt_order({"id": items['id']})
        return True
    except Exception as e:
        print(f"Error cancelling GTT order: {e}")
        return False

def bucket_format_futures_contract(symbol, expiry):
    """Build the NSE stock-futures symbol for the persisted contract expiry."""
    if isinstance(expiry, str):
        expiry = datetime.strptime(expiry, "%d%m%Y")
    if not symbol or expiry is None:
        return None
    return f"NSE:{str(symbol).strip().upper()}{expiry.strftime('%y%b').upper()}FUT"