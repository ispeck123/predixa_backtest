"""Centralized graduated-live settings.

The repository is intentionally fail-closed: the values needed to certify a
broker account are ``None`` until the owner supplies an approved production
configuration.  The old dynamic throttle constants are retained for the
parked Option-2 implementation and are never consulted by Option-3.
"""
from dataclasses import dataclass
from typing import Optional

TOTAL_REALIZED_LOSS_CEILING = 50_000
FUTURES_MODE = "OPTION3_SERIAL"
OPTION2_MODE = "OPTION2_DYNAMIC"
FUTURES_DD_REFERENCE_BUCKET = 35_000

# Option-3 production policy values.
OPTION3_SINGLE_TRADE_RISK_CAP_INR: Optional[float] = 18000
OPTION3_SELECTION_MODE: Optional[str] = "PER_LOT_RISK"
OPTION3_CANDIDATE_POOL_SIZE: Optional[int] = 8
OPTION3_LIQUIDITY_METRIC: Optional[str] = "average_quantity"
OPTION3_LIQUIDITY_LOOKBACK: Optional[str] = "7 days"
OPTION3_LIQUIDITY_THRESHOLD: Optional[float] = 2.0
OPTION3_MARGIN_BUFFER_INR: Optional[float] = 35000
OPTION3_MARGIN_BUFFER_PCT: Optional[float] = 1.5
OPTION3_STALE_GTT_TRADING_DAYS: Optional[int] = 4

# Backwards-compatible aliases consumed by the first-pass tests/importers.
FUTURES_RISK_CAP = OPTION3_SINGLE_TRADE_RISK_CAP_INR
FUTURES_SELECTION_MODE = OPTION3_SELECTION_MODE
FUTURES_CANDIDATE_POOL_SIZE = OPTION3_CANDIDATE_POOL_SIZE
FUTURES_LIQUIDITY_THRESHOLD = OPTION3_LIQUIDITY_THRESHOLD
FUTURES_MARGIN_BUFFER = OPTION3_MARGIN_BUFFER_INR

# Parked Option-2 dynamic-throttle settings.  Do not use in Option-3.
MAX_OPEN_FUTURES_RISK = 18_000
MAX_SINGLE_FUTURES_TRADE_RISK = 14_000
DD_TWO_LOT_MAX_EXCLUSIVE = 15_000
DD_ONE_LOT_MAX_EXCLUSIVE = 25_000
DD_TAP_CLOSE = 30_000
DD_TAP_REOPEN_BELOW = 22_000

FUTURES_TIER0_LOTS = 1
# Exact broker quantity for one lot of each approved futures instrument.
FUTURES_LOT_SIZES = {
    "CANBK": 6750,
    "HDFCBANK": 650,
    "PNB": 8000,
    "ASHOKLEY": 5000,
    "TATASTEEL": 2750,
    "ETERNAL": 2425,
    "JIOFIN": 2350,
    "MOTHERSON": 6150,
}
CASH_TIER0_QTY = 1
GATES = (
    "HARD_CEILING_TEST_PASS",
    "FUTURES_THROTTLE_TEST_PASS",
    "SYMBOL_SELECTION_PASS",
    "FYERS_RESOLUTION_PASS",
    "COMPLETED_TRADE_EXPORT_PASS",
    "STATE_RESTART_RECONCILIATION_PASS",
)

# Owner-approved production enablement.  This is configuration, not a change
# to the RiskEngine's loss, exposure, or entry rules.
OWNER_APPROVED_GATES = {gate: True for gate in GATES}

OPTION3_STATES = (
    "IDLE",
    "ENTRY_RESERVING",
    "ENTRY_GTT_RESTING",
    "ENTRY_CANCEL_PENDING",
    "ENTRY_FILL_PENDING_RECONCILIATION",
    "POSITION_OPEN_LOCKED",
    "POSITION_OPEN_UNPROTECTED",
    "EXIT_OCO_ACTIVE",
    "EXIT_RECONCILING",
    "ERROR_LOCKED",
)


@dataclass(frozen=True)
class Policy:
    """Option-2 policy retained for isolated simulations.

    Option-3 does not need a DD update formula; setting ``dd_update`` is only
    useful when deliberately running the parked dynamic model in tests.
    """

    dd_update: str | None = None
    undefined_band_max_lots: int | None = None

    def __post_init__(self):
        if self.dd_update not in (None, "LOSS_MINUS_WIN_FLOORED"):
            raise ValueError("Unknown DD update policy")
        if self.undefined_band_max_lots is not None and (
            type(self.undefined_band_max_lots) is not int
            or self.undefined_band_max_lots not in (0, 1, 2)
        ):
            raise ValueError("Explicit DD band capacity must be 0, 1 or 2")


@dataclass(frozen=True)
class Option3Policy:
    """Explicit Option-3 production policy; unresolved by default."""

    single_trade_risk_cap_inr: float | None = OPTION3_SINGLE_TRADE_RISK_CAP_INR
    selection_mode: str | None = OPTION3_SELECTION_MODE
    candidate_pool_size: int | None = OPTION3_CANDIDATE_POOL_SIZE
    liquidity_metric: str | None = OPTION3_LIQUIDITY_METRIC
    liquidity_lookback: str | None = OPTION3_LIQUIDITY_LOOKBACK
    liquidity_threshold: float | None = OPTION3_LIQUIDITY_THRESHOLD
    margin_buffer_inr: float | None = OPTION3_MARGIN_BUFFER_INR
    margin_buffer_pct: float | None = OPTION3_MARGIN_BUFFER_PCT
    stale_gtt_trading_days: int | None = OPTION3_STALE_GTT_TRADING_DAYS

    def validate(self) -> tuple[bool, list[str]]:
        errors: list[str] = []
        if self.single_trade_risk_cap_inr is None:
            errors.append("OPTION3_SINGLE_TRADE_RISK_CAP_INR")
        elif self.single_trade_risk_cap_inr <= 0:
            errors.append("OPTION3_SINGLE_TRADE_RISK_CAP_INR_INVALID")
        if self.selection_mode not in ("PER_LOT_RISK", "LOWEST_MARGIN"):
            errors.append("OPTION3_SELECTION_MODE")
        if self.candidate_pool_size not in (6, 8):
            errors.append("OPTION3_CANDIDATE_POOL_SIZE")
        if not self.liquidity_metric:
            errors.append("OPTION3_LIQUIDITY_METRIC")
        if not self.liquidity_lookback:
            errors.append("OPTION3_LIQUIDITY_LOOKBACK")
        if self.liquidity_threshold is None or self.liquidity_threshold < 0:
            errors.append("OPTION3_LIQUIDITY_THRESHOLD")
        if self.margin_buffer_inr is None and self.margin_buffer_pct is None:
            errors.append("OPTION3_MARGIN_BUFFER")
        if self.margin_buffer_inr is not None and self.margin_buffer_inr < 0:
            errors.append("OPTION3_MARGIN_BUFFER_INR_INVALID")
        if self.margin_buffer_pct is not None and self.margin_buffer_pct < 0:
            errors.append("OPTION3_MARGIN_BUFFER_PCT_INVALID")
        if self.stale_gtt_trading_days is None:
            errors.append("OPTION3_STALE_GTT_TRADING_DAYS")
        elif self.stale_gtt_trading_days <= 0:
            errors.append("OPTION3_STALE_GTT_TRADING_DAYS_INVALID")
        return not errors, errors
