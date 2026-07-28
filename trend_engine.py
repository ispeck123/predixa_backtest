from __future__ import annotations
from dataclasses import dataclass, field, is_dataclass, asdict
from enum import Enum
from typing import List, Optional, Tuple, Dict, Literal, Set
# from scripts.trade_engine import Config
import pandas as pd
from shared.config.settings import stock_data_dir_config
from scripts.models import load_preprocess_data
# from scripts.additional_engine_class import ZoneNestingTier
# from scripts.trend_engine import MultiTimeframeTrendCalculator
from scripts.trade_engine import process_trend_zones, process_qualified_zones
import os, json




# ==============================================================================
# ENUMERATIONS
# ==============================================================================
class ZoneNestingTier(Enum):
    """X Zone nesting tiers based on HTF alignment."""
    TIER_1 = "TIER_1"  # X nested in BOTH E and A (HIGHEST probability)
    TIER_2 = "TIER_2"  # X nested in E OR A (HIGH probability)
    TIER_3 = "TIER_3"  # X overlapping with E or A (MODERATE probability)
    TIER_4 = "TIER_4"  # X standalone (LOW probability - AVOID)


class TF(str, Enum):
    E = "E"  # Evaluate - Primary regime (overrides all)
    A = "A"  # Analyze - Confirms/conflicts
    X = "X"  # Execute - Entry timing only


class ZoneType(str, Enum):
    BZ = "BZ"
    SZ = "SZ"
    GDZ = "GDZ"
    GSZ = "GSZ"


class BoundaryMode(str, Enum):
    WICK_TO_WICK = "WICK_TO_WICK"
    BODY_TO_WICK = "BODY_TO_WICK"


class ViolationType(str, Enum):
    NONE = "NONE"
    WICK = "WICK"        # Secondary: early warning only
    CLOSE = "CLOSE"      # Primary: required for confirmation
    TRUE_BREAK = "TRUE_BREAK"  # Zone invalidated


class VolatilityRegime(str, Enum):
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"


class TrendRegime(str, Enum):
    UP = "UP"
    SW = "SW"
    DN = "DN"


class Quadrant(str, Enum):
    Q1 = "Q1"  # 0-33.3% Discount
    Q2 = "Q2"  # 33.3-66.6% Equilibrium
    Q3 = "Q3"  # 66.6-100% Premium
    NONE = "NONE"


class ZoneState(str, Enum):
    GREEN = "GREEN"
    AMBER = "AMBER"
    RED = "RED"


# NEW in v3.3: Trade Type Classification (v4.1 Section 9)
class TradeType(str, Enum):
    """
    v4.1 Section 9.1: Trade Type Definitions
    
    CONTINUATION: Trading in direction of both Eval and Analyze
    CONT_REDUCED: Direction aligned but reduced confidence
    REVERSAL_ONLY: Counter to Eval direction with strict gating (DBR/RBR REQUIRED)
    RANGE_EXTREME: Trading at range boundaries only
    NO_TRADE: No trades in this direction
    """
    CONTINUATION = "CONTINUATION"
    CONT_REDUCED = "CONT_REDUCED"
    REVERSAL_ONLY = "REVERSAL_ONLY"
    RANGE_EXTREME = "RANGE_EXTREME"
    NO_TRADE = "NO_TRADE"


# NEW in v3.3: Pattern Type for DBR/RBR
class PatternType(str, Enum):
    DBR = "DBR"  # Drop-Base-Rally (for longs in DN regime)
    RBR = "RBR"  # Rally-Base-Drop (for shorts in UP regime)
    NONE = "NONE"

class ZonePattern(str, Enum):
    RBR = "RBR"
    RBD = "RBD"
    DBR = "DBR"
    DBD = "DBD"


# NEW in v3.4: Zone Age Classification (v4.2 Section 13.2)
class ZoneAgeClass(str, Enum):
    """
    v4.2 Section 13.2: Zone Age Classification
    
    Zones decay in quality over time unless reactivated.
    """
    FRESH = "FRESH"          # < 50 bars since creation
    ACTIVE = "ACTIVE"        # 50-200 bars + CMP within 5 ATR
    STALE = "STALE"          # > 200 bars OR CMP > 10 ATR away
    REACTIVATED = "REACTIVATED"  # Was STALE but CMP returned to zone


# ==============================================================================
# CONFIGURATION
# ==============================================================================

@dataclass
class Config:
    """All thresholds in one place for ATR-normalized scoring."""
    
    # ATR
    atr_period: int = 14
    ema_period: int = 20
    
    # Volatility regime thresholds
    vol_low_thresh: float = 0.8
    vol_high_thresh: float = 1.8
    
    # Base classification
    basing_body_pct: float = 0.50
    max_base_len: int = 4
    
    # Departure thresholds (ATR-normalized)
    departure_atr_score1: float = 1.2
    departure_atr_score2: float = 1.5
    departure_body_score1: float = 0.60
    departure_body_score2: float = 0.70

    basing_max_range_atr: float = 2.5 
    max_structure_extension_atr: float = 5.0
    
    # Boundary modes per TF
    boundary_E: BoundaryMode = BoundaryMode.WICK_TO_WICK
    boundary_A: BoundaryMode = BoundaryMode.WICK_TO_WICK
    boundary_X: BoundaryMode = BoundaryMode.BODY_TO_WICK
    
    # Gap zones
    gap_min_atr_low: float = 0.5         # Low vol: tighter filter
    gap_min_atr_norm: float = 0.3        # Normal vol: v3.8.7 lowered from 0.5
    gap_min_atr_high: float = 0.3        # High vol: v3.8.7 lowered from 0.5
    gap_departure_range_atr: float = 1.2  # D2: v2.4 says 1.2x minimum
    gap_departure_body_pct: float = 0.60  # D3: v2.4 says 60%
    gap_session_followthrough_bars: int = 3
    
    # Structure removal
    major_pivot_lookback: int = 5
    
    # Risk
    default_rr: float = 2.1
    reject_obstructed_targets: bool = False   # config-guarded; default OFF. OOS-refuted: in-sample -0.16R did NOT replicate (OOS +0.39R, pooled +0.12R). Retained for future regime-aware use.
    min_rr: float = 2.1                  # BUG-03 FIX: was 1.5 → 2.1 (unified min RR, cost-adjusted)
    # Freshness
    max_retest_htf: int = 2
    max_retest_execute: int = 0
    
    # Age penalty
    age_bars_mild: int = 60
    age_bars_severe: int = 200
    
    # Approach penalty
    approach_speed_mild: float = 0.9  # ATR per bar
    approach_speed_severe: float = 1.2
    
    # Multi-zone
    consecutive_max_gap_atr: float = 2.0
    
    # Trend engine
    higher_high_candles: int = 5
    lower_high_candles: int = 5
    
    # NEW v3.3: DBR/RBR validation
    dbr_rbr_min_base_candles: int = 2
    dbr_rbr_max_base_candles: int = 5
    
    # NEW v3.3: Zone Quality Scoring thresholds
    zone_quality_high: int = 8
    zone_quality_medium: int = 5
    distance_buffer_atr_high: float = 2.0
    distance_buffer_atr_low: float = 1.0
    
    # NEW v3.4: Sliding Window Boundaries (v4.2 Section 13.1)
    sliding_window_max_bars: int = 200
    sliding_window_max_atr: float = 20.0
    
    # NEW v3.4: Zone Age Classification (v4.2 Section 13.2)
    zone_age_fresh_bars: int = 50
    zone_age_active_bars: int = 200
    zone_age_active_atr: float = 5.0
    zone_age_stale_atr: float = 10.0
    zone_age_fresh_penalty: int = 0
    zone_age_active_penalty: int = -1
    zone_age_stale_penalty: int = -2

    min_zone_v38_score: int = 3
    
    # NEW v3.4: Gap Zone Integration (v4.2 Section 14)
    gap_score_size_high_atr: float = 0.75   # D10: was 2.0
    gap_score_size_medium_atr: float = 0.5  # D10: was 1.0
    
    # NEW v3.4: Wick Violation Handling (v4.2 Section 15.2)
    wick_violation_reversal_probability_boost: float = 0.15  # 15%

    # Setup Extraction (v3.8.1: Step 19 — BUG-21 IMPLEMENTED)
    # REF: Methodology v3.8.1 Sec 9.3, Sec 13.1; Annexure v1.2 Sec 4
    setup_proximity_pct: float = 5.0     # Max % distance from CMP to zone proximal
    # PROX (CB): ATR-tier proximity. Default OFF reproduces legacy flat-5%. TUNE proximity_atr_mult
    # on full universe before enabling (same discipline as B-W-EMBED params).
    proximity_use_atr_tier: bool = False
    proximity_atr_mult: float = 1.5      # threshold = max(setup_proximity_pct, mult * ATR%) [TUNE]
    setup_top_n: int = 3                 # Methodology Sec 13.1: "top N" candidates per direction
    
    # ── Weighted Setup Scoring (configurable, sum must = 1.0) ──
    # Order of importance (institutional S/D doctrine):
    # 1. Trend direction     2. Base quality + structure removal
    # 3. Freshness           4. RR ratio
    # 5. Departure strength  6. HTF nesting/confluence
    # 7. EMA-20 confluence   8. Age penalty
    # 9. Proximity to CMP (hard filter does heavy lifting)
    ws_trend: float = 0.20              # #1 E/A regime supports trade direction
    ws_base_quality: float = 0.18       # #2 Zone formation + opposing structure violation
    ws_freshness: float = 0.16          # #3 Untested + penetration penalty (sub-component)
    ws_rr: float = 0.15                 # RR ratio (between freshness and departure)
    ws_departure: float = 0.12          # #5 Impulsive departure = institutional conviction
    ws_confluence: float = 0.10         # #6 Nesting tier + consecutive (HTF zone support)
    ws_ema_confluence: float = 0.04     # #7 EMA-20 (lagging indicator, minor)
    ws_age: float = 0.03               # #8 Zone staleness (FRESH/ACTIVE/STALE)
    ws_proximity: float = 0.02          # #9 Hard filter primary; scoring weight minimal

    entry_buffer_pct: float = 0.0015 
    sl_buffer_atr: float = 0.25
    min_risk_pct: float = 0.01
    embed_strict_stop: bool = False      # B-W-EMBED Edit 3: strict structural stop (measure before enabling)
    # B-W-EMBED tunable parameters — REQUIRE full-universe tuning before lock (see embed_tuning.md).
    # Defaults are provisional placeholders, NOT validated values.
    embed_sits_on_top_target_pct: float = 0.5   # TIER_3 far-HTF target discount [TUNE: sweep 0.3-0.7]
    embed_overlap_threshold: float = 0.5        # nesting vs TIER_3 boundary [TUNE: sweep 0.4-0.6]
    min_risk_atr: float = 0.5            # 0.5 × ATR absolute floor (ATR override)

    compound_stale_age_bars: int = 100
    compound_stale_distance_pct: float = 0.20

    swing_left = 3          # candles to the left for pivot check
    swing_right = 3         # candles to the right for pivot check
    sweep_lookback = 6      # how far before base to look for LL/HH wicks (leg-in / swing sweep)
    sweep_eps = 0.0         # optional filter (like 0.01% of price), can keep 0

    target_htf_pct: float = 0.75       # Distance multiplier for HTF opposing (E/A)
    target_xtf_pct: float = 0.90       # Distance multiplier for X-TF opposing
    default_target_atr: float = 3.0    # Fallback ATR multiple when no opposing found

    cleanness_band_1: float = 0.25    # depth ≤ 25% → cleanness 0.85
    cleanness_band_2: float = 0.50    # depth ≤ 50% → cleanness 0.60
    cleanness_band_3: float = 0.75    # depth ≤ 75% → cleanness 0.35
    cleanness_band_4: float = 1.00    # depth ≤ 100% → cleanness 0.15

    violation_hold_bars: int = 3





# ==============================================================================
# CANDLE SERIES
# ==============================================================================

@dataclass
class CandleSeries:
    o: List[float]
    h: List[float]
    l: List[float]
    c: List[float]
    v: Optional[List[float]] = None
    ts: Optional[List[int]] = None
    session_id: Optional[List[int]] = None
    is_incomplete: Optional[List[bool]] = None
    
    @property
    def n(self) -> int:
        return len(self.o)

    @property
    def cmp(self) -> float:
        return self.c[-1] if self.c else 0.0
# ==============================================================================
# ZONE DATA CLASS
# ==============================================================================

@dataclass
class Zone:
    """Complete zone representation with all qualification fields."""
    symbol: str
    tf: TF
    ztype: ZoneType
    distal: float
    proximal: float
    created_idx: int
    base_start: int
    base_end: int
    base_len: int
    departure_idx: Optional[int] = None
    departure_atr: float = 0.0
    body_pct: float = 0.0
    zone_pattern: Optional[ZonePattern] = None  # "RBR" | "DBR" | "RBD" | "DBD"
    gate_warnings: List[str] = field(default_factory=list)
    gap_wick_touch_count: int = 0
    gap_wick_max_depth_pct: float = 0.0 
    gap_void_height: float = 0.0  
    gap_is_breakaway: bool = False
    # State tracking
    state: ZoneState = ZoneState.AMBER
    block_reason: Optional[str] = None
    
    # Qualification flags
    invalidated: bool = False
    violation: ViolationType = ViolationType.NONE
    violated_by_close: bool = False
    penetration_pct: float = 0.0
    retest_count: int = 0
    removes_structure: bool = False
    removes_structure_type: Optional[str] = None
    
    # Gap-specific
    session_accepted: bool = False
    
    # Multi-zone
    is_composite: bool = False
    source_zone_ids: List[str] = field(default_factory=list)
    is_part_of_consecutive: bool = False
    is_part_of_overlapping: bool = False
    consecutive_partner_id: Optional[str] = None
    overlapping_partners_id: List[str] = field(default_factory=list)
    nesting_tier: Optional[ZoneNestingTier] = None
    
    # Scoring (v3.3 enhanced)
    raw_score: int = 0
    age_penalty: int = 0
    approach_penalty: int = 0
    penetration_penalty: int = 0
    final_score: int = 0
    quality_priority: str = "UNKNOWN"  # HIGH / MEDIUM / LOW / AVOID
    
    # Risk/Target
    entry: Optional[float] = None
    target_price: Optional[float] = None
    stop_price: Optional[float] = None
    rr_ratio: Optional[float] = None
    
    # NEW v3.3: DBR/RBR pattern association
    associated_pattern: Optional[PatternType] = None
    pattern_validated: bool = False
    gap_fill_pct: float = 0.0
    
    # NEW v3.4: Zone Age (v4.2 Section 13.2)
    age_class: str = "FRESH"  # FRESH / ACTIVE / STALE / REACTIVATED
    age_bars: int = 0
    reactivation_count: int = 0
    
    # NEW v3.4: Wick violation tracking (v4.2 Section 15.2)
    wick_violation_detected: bool = False
    reversal_probability_boost: float = 0.0
    
    # NEW v3.4: Legout validation (v3.4 Section 4.4)
    legout_count: int = 0  # Number of valid legout candles
    legout_range: float = 0.0  # v3.4 ENHANCED: Range of legout sequence (for P2 validation)

    enclosing_e_zone: Optional[Zone] = None  # HTF E-zone containing this zone
    enclosing_a_zone: Optional[Zone] = None  # HTF A-zone containing this zone
    ###################
    zone_in_zone: bool = False
    pattern_validated: bool = False
    nesting_debug: Optional[dict] = None
    final_weighted_score: float = 0.0
    sl_levels: List[float] = field(default_factory=list)

    distal_base: Optional[float] = None

    # BUG-33: Target source tracking (v3.8.4)
    target_source_zone_id: Optional[str] = None
    target_source_tf: Optional[str] = None
    target_multiplier: Optional[float] = None

    structure_low: Optional[float] = None       # Min LOW from swing high to base end (BZ)
    structure_high: Optional[float] = None      # Max HIGH from swing low to base end (SZ)
    structure_start_idx: Optional[int] = None   # Index of swing point boundary
    legin_start_idx: Optional[int] = None       # First non-basing candle before base

    legout_cleanness: float = 1.0               # 0.0 (worst) to 1.0 (pristine)
    legout_hard_discard: bool = False            # True if legout breached base floor

    has_internal_gap: bool = False
    internal_gap_levels: list = field(default_factory=list)  # [(lower, upper, dir), ...]

    entry_path_clear: Optional[bool] = None
    entry_blocking_zone_id: Optional[str] = None

    overlap_ratio: float = 0.0
    htf_target_price: Optional[float] = None
    
    @property
    def zone_id(self) -> str:
        return f"{self.symbol}_{self.tf.value}_{self.ztype.value}_{self.created_idx}"
    
    @property
    def low_edge(self) -> float:
        return min(self.proximal, self.distal)
    
    @property
    def high_edge(self) -> float:
        return max(self.proximal, self.distal)
    
    @property
    def zone_height(self) -> float:
        return abs(self.proximal - self.distal)
    
    @property
    def is_buy_zone(self) -> bool:
        return self.ztype in (ZoneType.BZ, ZoneType.GDZ)
    
    @property
    def is_sell_zone(self) -> bool:
        return self.ztype in (ZoneType.SZ, ZoneType.GSZ)

    @property
    def low_edge_base(self) -> float:
        """Order area lower boundary (for nesting, enclosure, overlap detection)."""
        d = self.distal_base if self.distal_base is not None else self.distal
        return min(self.proximal, d)
    
    @property
    def high_edge_base(self) -> float:
        """Order area upper boundary (for nesting, enclosure, overlap detection)."""
        d = self.distal_base if self.distal_base is not None else self.distal
        return max(self.proximal, d)
    
    @property
    def zone_height_base(self) -> float:
        """Order area height (for scoring denominators)."""
        d = self.distal_base if self.distal_base is not None else self.distal
        return abs(self.proximal - d)


# ==============================================================================
# NEW v3.3: DBR/RBR PATTERN DATA CLASS (v4.1 Section 10)
# ==============================================================================




# ==============================================================================
# TREND RESULT DATA CLASS
# ==============================================================================

@dataclass
class TrendResult:
    """Result of trend calculation for a single timeframe."""
    regime: TrendRegime
    quadrant: Optional[Quadrant]
    quadrant_price_pct: Optional[float]
    nearest_bz: Optional[Zone]
    nearest_sz: Optional[Zone]
    diagnosis: str
    rule_applied: str
    reinforcers: List[str] = field(default_factory=list)
    htf_veto_applied: bool = False
    htf_veto_reason: Optional[str] = None

# ==============================================================================
# NEW v3.3: ENHANCED TREND CONTEXT (v4.1 Sections 7-9)
# ==============================================================================

@dataclass
class TrendContext:
    """Complete multi-timeframe trend context with trade type classification."""
    regime_E: TrendRegime
    regime_A: TrendRegime
    regime_X: TrendRegime
    quadrant_E: Optional[Quadrant]
    quadrant_A: Optional[Quadrant]
    quadrant_X: Optional[Quadrant]
    
    # Execution permissions
    allow_long: bool = True
    allow_short: bool = True
    bias: str = "Unknown"
    permitted_setup: str = ""
    
    # NEW v3.3: Trade Type Classification (v4.1 Section 9)
    trade_type_long: TradeType = TradeType.NO_TRADE
    trade_type_short: TradeType = TradeType.NO_TRADE
    dbr_required: bool = False
    rbd_required: bool = False
    
    # HTF Veto status (v4.1 Section 6.2)
    htf_veto_longs: bool = False
    htf_veto_shorts: bool = False
    htf_veto_reason: Optional[str] = None
    
    # Detailed results
    result_E: Optional[TrendResult] = None
    result_A: Optional[TrendResult] = None
    result_X: Optional[TrendResult] = None


@dataclass
class RiskTarget:
    """Risk and target calculation result."""
    entry: float
    stop: float
    target: float
    rr: float
    valid: bool
    reason: Optional[str] = None
    target_mode: str = "UNKNOWN"  # STRUCTURAL_CONSERVATIVE | STRUCTURAL_AGGRESSIVE | MINIMUM_RR
    target_source_zone_id: Optional[str] = None
    target_source_tf: Optional[str] = None
    target_multiplier: Optional[float] = None



@dataclass(frozen=True)
class RuleSpec:
    allowed_quadrants: Optional[Set[Quadrant]] = None  # None => any quadrant
    allow_when_no_quadrant: bool = True
    requires_dbr: bool = False
    requires_rbr: bool = False
    # Optional: structured requirements you can check later
    requires_htf_demand: bool = False
    requires_htf_supply: bool = False
    requires_price_in_analyze_zone: bool = False
    # Keep your text too (so you don't lose readability)
    text: str = ""

@dataclass
class ExecutionContext:
    quadrant: Quadrant = Quadrant.NONE
    has_dbr: bool = False
    has_rbr: bool = False
    in_analyze_zone: bool = False
    htf_demand_present: bool = False
    htf_supply_present: bool = False


# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================

def atr(h: List[float], l: List[float], c: List[float], period: int) -> List[Optional[float]]:
    """Average True Range calculation with bootstrapping for early candles.
    
    ATR-GAP FIX (v3.8.6): When fewer than period+1 candles are available,
    the standard ATR returns None for all early indices. This blocks gap
    detection, zone scoring, and structure extension for instruments with
    short history (futures contracts, newly listed stocks).
    
    Fix: For indices 1 through period-1, compute a progressive ATR from
    available true ranges (simple mean of TR[1:i+1]). Index 0 uses its own
    range as fallback. Minimum 2 candles required for any ATR value.
    Standard EMA-smoothed ATR takes over from index period onward.
    """
    n = len(c)
    if n == 0:
        return []
    
    tr = [None] * n
    tr[0] = h[0] - l[0]
    for i in range(1, n):
        tr[i] = max(h[i] - l[i], abs(h[i] - c[i-1]), abs(l[i] - c[i-1]))
    
    result = [None] * n
    
    # ATR-GAP: Bootstrap for early candles (before standard ATR is available)
    # Index 0: no prior close, use candle range as fallback
    if n >= 1:
        result[0] = tr[0] if tr[0] and tr[0] > 0 else None
    # Indices 1 through min(period-1, n-1): progressive mean of available TRs
    for i in range(1, min(period, n)):
        valid_trs = [t for t in tr[1:i+1] if t is not None]
        if valid_trs:
            result[i] = sum(valid_trs) / len(valid_trs)
    
    # Standard ATR from index period onward (unchanged)
    if n >= period + 1:
        s = sum(tr[1:period+1])
        result[period] = s / period
        for i in range(period + 1, n):
            result[i] = (result[i-1] * (period - 1) + tr[i]) / period
    
    return result


def ema(values: List[float], period: int) -> List[Optional[float]]:
    """Exponential Moving Average."""
    n = len(values)
    if n < period:
        return [None] * n
    
    result = [None] * n
    result[period - 1] = sum(values[:period]) / period
    mult = 2 / (period + 1)
    for i in range(period, n):
        result[i] = values[i] * mult + result[i-1] * (1 - mult)
    return result


def body_pct(o: float, h: float, l: float, c: float) -> float:
    rng = h - l
    return abs(c - o) / rng if rng > 0 else 0


def volatility_regime(atr_now: float, atr_ema20: float, cfg: Config) -> VolatilityRegime:
    if atr_ema20 == 0:
        return VolatilityRegime.NORMAL
    ratio = atr_now / atr_ema20
    if ratio <= cfg.vol_low_thresh:
        return VolatilityRegime.LOW
    if ratio >= cfg.vol_high_thresh:
        return VolatilityRegime.HIGH
    return VolatilityRegime.NORMAL


def last_pivot_high_idx(h: List[float], end_idx: int, lookback: int) -> Optional[int]:
    best_idx = None
    best_val = float('-inf')
    start = max(0, end_idx - lookback * 3)
    for i in range(start, end_idx + 1):
        left = max(0, i - lookback)
        right = min(len(h) - 1, i + lookback)
        is_pivot = all(h[i] >= h[j] for j in range(left, right + 1) if j != i)
        if is_pivot and h[i] > best_val:
            best_val = h[i]
            best_idx = i
    return best_idx


def last_pivot_low_idx(l: List[float], end_idx: int, lookback: int) -> Optional[int]:
    best_idx = None
    best_val = float('inf')
    start = max(0, end_idx - lookback * 3)
    for i in range(start, end_idx + 1):
        left = max(0, i - lookback)
        right = min(len(l) - 1, i + lookback)
        is_pivot = all(l[i] <= l[j] for j in range(left, right + 1) if j != i)
        if is_pivot and l[i] < best_val:
            best_val = l[i]
            best_idx = i
    return best_idx

class ReinforcerDetector:
    """
    Reinforcers are SECONDARY - they cannot define trend alone.
    
    Rule D - Higher Highs: 4-5 candles with H1 < H2 < H3 < H4 < H5 = buyers stepping up
    Rule D - Lower Highs: 4-5 candles with H1 > H2 > H3 > H4 > H5 = sellers stepping down
    
    CRITICAL: Lower HIGHS - not lower lows - define institutional control.
    """
    
    def __init__(self, cfg: Config):
        self.cfg = cfg
    
    def detect_higher_highs(self, cs: CandleSeries, zone_idx: int) -> bool:
        """Rule D for UPTREND: Higher Highs preceding zone."""
        count = self.cfg.higher_high_candles
        start = zone_idx - count
        if start < 0:
            return False
        
        for i in range(start, zone_idx - 1):
            if cs.h[i] >= cs.h[i + 1]:
                return False
        return True
    
    def detect_lower_highs(self, cs: CandleSeries, zone_idx: int) -> bool:
        """
        Rule D for DOWNTREND: Lower Highs preceding zone.
        
        CRITICAL: Lower HIGHS (where sellers transact), NOT lower lows.
        Lower lows show distance, lower highs show control.
        """
        count = self.cfg.lower_high_candles
        start = zone_idx - count
        if start < 0:
            return False
        
        for i in range(start, zone_idx - 1):
            if cs.h[i] <= cs.h[i + 1]:
                return False
        return True
    
    def detect_swing_high_breach(self, cs: CandleSeries, zone: Zone) -> bool:
        """Rule E: BZ breaching Previous Swing High reinforces strength."""
        if not zone.is_buy_zone:
            return False
        pivot = last_pivot_high_idx(cs.h, zone.created_idx - 1, self.cfg.major_pivot_lookback)
        if pivot is None:
            return False
        for i in range(zone.created_idx, cs.n):
            if cs.h[i] > cs.h[pivot]:
                return True
        return False
    
    def detect_swing_low_breach(self, cs: CandleSeries, zone: Zone) -> bool:
        """Rule E: SZ breaching Previous Swing Low reinforces strength."""
        if not zone.is_sell_zone:
            return False
        pivot = last_pivot_low_idx(cs.l, zone.created_idx - 1, self.cfg.major_pivot_lookback)
        if pivot is None:
            return False
        for i in range(zone.created_idx, cs.n):
            if cs.l[i] < cs.l[pivot]:
                return True
        return False


# ==============================================================================
# QUADRANT CALCULATOR (v4.1 Section 4)
# ==============================================================================

# class QuadrantCalculator:
#     """
#     Section 4: Quadrant Analysis (Execution Tool Only)
    
#     Construction:
#     - BZ distal = 0% (bottom reference)
#     - SZ distal = 100% (top reference)
    
#     CARDINAL RULE: Quadrants determine WHERE to execute, never WHAT the trend is.
#     """
    
#     def calculate(self, cmp: float, bz: Optional[Zone], 
#                   sz: Optional[Zone]) -> Optional[Quadrant, float]:
#         if bz is None or sz is None:
#             return None, None
        
#         range_size = sz.distal - bz.distal
#         if range_size <= 0:
#             return None, None
        
#         position_pct = ((cmp - bz.distal) / range_size) * 100
        
#         if position_pct <= 33.3:
#             return Quadrant.Q1, position_pct
#         elif position_pct <= 66.6:
#             return Quadrant.Q2, position_pct
#         else:
#             return Quadrant.Q3, position_pct

class QuadrantCalculator:
    def calculate(
        self,
        cmp: float,
        bz_distal: Optional[float],
        sz_distal: Optional[float]
    ) -> Optional[Quadrant]:
        if bz_distal is None or sz_distal is None:
            return None

        range_size = sz_distal - bz_distal
        if range_size <= 0:
            return None

        position_pct = ((cmp - bz_distal) / range_size) * 100

        if position_pct <= 33.3:
            return Quadrant.Q1
        elif position_pct <= 66.6:
            return Quadrant.Q2
        else:
            return Quadrant.Q3


# ==============================================================================
# TREND VIOLATION CHECKER (Dual Context)
# ==============================================================================

class TrendViolationChecker:
    """
    CRITICAL DISTINCTION:
    
    Context 1 - Execute TF Zone Qualification: compute_structure_removal()
    Context 2 - Evaluate/Analyze Trend Calculation: has_bz_violated_sz() / has_sz_violated_bz()
    
    Violation Types (v4.4.1 Section 5):
    - Primary: CLOSE beyond distal (required for confirmation)
    - Secondary: Wick beyond distal (early warning only)
    """
    
    def has_bz_violated_sz(self, cs: CandleSeries, bz: Zone, sz: Zone) -> bool:
        """
        For TREND CALCULATION (Rule C):
        Has BZ side demonstrated dominance by closing above SZ distal?
        """
        for i in range(bz.created_idx, cs.n):
            if cs.c[i] > sz.distal:
                return True
        return False
    
    def has_sz_violated_bz(self, cs: CandleSeries, sz: Zone, bz: Zone) -> bool:
        """
        For TREND CALCULATION (Rule C):
        Has SZ side demonstrated dominance by closing below BZ distal?
        """
        for i in range(sz.created_idx, cs.n):
            if cs.c[i] < bz.distal:
                return True
        return False
    
    def wick_violation_bz_over_sz(self, cs: CandleSeries, bz: Zone, sz: Zone) -> bool:
        """Secondary: Wick above SZ distal (early warning)."""
        for i in range(bz.created_idx, cs.n):
            if cs.h[i] > sz.distal:
                return True
        return False
    
    def wick_violation_sz_under_bz(self, cs: CandleSeries, sz: Zone, bz: Zone) -> bool:
        """Secondary: Wick below BZ distal (early warning)."""
        for i in range(sz.created_idx, cs.n):
            if cs.l[i] < bz.distal:
                return True
        return False
    
    def is_violation_accepted_above(self, cs: CandleSeries, bz: Zone, sz: Zone,
                                     hold_bars: int = 3) -> bool:
        """
        v3.8.2: Violation Acceptance Test (UPTREND Scenario 2).
        
        After BZ side CLOSE-violates SZ distal, price must SUSTAIN at least
        `hold_bars` consecutive candles with CLOSE above SZ PROXIMAL.
        
        Proximal (not distal) is the threshold because:
        - Proximal is the weaker edge (closer to CMP)
        - If institutions can't even hold above proximal after violating distal,
          the violation was a liquidity grab, not a structural break
        - This is the MINIMUM institutional commitment test
        
        Returns True if acceptance confirmed, False otherwise.
        """
        violation_candle = None
        for i in range(bz.created_idx, cs.n):
            if cs.c[i] > sz.distal:
                violation_candle = i
                break
        
        if violation_candle is None:
            return False  # No violation occurred
        
        # Count consecutive candles holding CLOSE above SZ proximal
        consecutive = 0
        for j in range(violation_candle + 1, cs.n):
            if cs.c[j] > sz.proximal:
                consecutive += 1
                if consecutive >= hold_bars:
                    return True
            else:
                consecutive = 0  # Reset on failure
        
        return False
    
    def is_violation_accepted_below(self, cs: CandleSeries, sz: Zone, bz: Zone,
                                     hold_bars: int = 3) -> bool:
        """
        v3.8.2: Violation Acceptance Test (DOWNTREND Scenario 2).
        
        After SZ side CLOSE-violates BZ distal, price must SUSTAIN at least
        `hold_bars` consecutive candles with CLOSE below BZ PROXIMAL.
        
        Symmetric to is_violation_accepted_above.
        """
        violation_candle = None
        for i in range(sz.created_idx, cs.n):
            if cs.c[i] < bz.distal:
                violation_candle = i
                break
        
        if violation_candle is None:
            return False
        
        consecutive = 0
        for j in range(violation_candle + 1, cs.n):
            if cs.c[j] < bz.proximal:
                consecutive += 1
                if consecutive >= hold_bars:
                    return True
            else:
                consecutive = 0
        
        return False

#######################################################################################################

#######################################################################################################

class TrendEngine:
    """
    Section 5: Trend Identification Rules
    
    MASTER FORMULA: TREND = STRUCTURAL ZONE DOMINANCE + TIMEFRAME HIERARCHY
    
    Rules:
    - Rule A: Structural Primacy (quadrant excluded from trend)
    - Rule B: Only BZ / Only SZ (unchallenged accumulation/distribution)
    - Rule C: Sliding Window Dominance (3 scenarios each for UP/DN)
    - Rule D: Reinforcers (HH/LH) - cannot define alone
    - Rule E: Swing Breach - reinforcer only
    """
    
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.violation_checker = TrendViolationChecker()
        self.reinforcer_detector = ReinforcerDetector(cfg)
        self.quadrant_calc = QuadrantCalculator()

    def find_nearest_bz(self, zones: List[Zone], cmp: float) -> Optional[Zone]:
        """Find nearest valid BZ below CMP.
        
        v3.8.2: Gap zones (GDZ) require session_accepted=True to participate
        in trend calculation (Trend v4.4.1 Sec 14).
        """
        zones = zones['BUY']
        valid_bzs = [z for z in zones if z.is_buy_zone and not z.invalidated and z.distal < cmp
                     and not (z.ztype in (ZoneType.GDZ, ZoneType.GSZ) and not z.session_accepted)]
        if not valid_bzs:
            return None
        return max(valid_bzs, key=lambda z: z.proximal)

    def find_nearest_sz(self, zones: List[Zone], cmp: float) -> Optional[Zone]:
        """Find nearest valid SZ above CMP.
        
        v3.8.2: Gap zones (GSZ) require session_accepted=True to participate
        in trend calculation (Trend v4.4.1 Sec 14).
        """
        zones = zones['SELL']
        valid_szs = [z for z in zones if z.is_sell_zone and not z.invalidated and z.distal > cmp
                     and not (z.ztype in (ZoneType.GDZ, ZoneType.GSZ) and not z.session_accepted)]
        if not valid_szs:
            return None
        return min(valid_szs, key=lambda z: z.proximal)

    def find_next_sz_above(self, zones: List[Zone], reference_sz: Zone) -> Optional[Zone]:
        """Find next SZ above the reference SZ for Rule C Scenario 2/3.
        v3.8.2: Excludes unaccepted gap zones."""
        zones = zones['SELL']
        valid_szs = [z for z in zones if z.is_sell_zone and not z.invalidated 
                     and z.distal > reference_sz.distal and z.zone_id != reference_sz.zone_id
                     and not (z.ztype in (ZoneType.GDZ, ZoneType.GSZ) and not z.session_accepted)]
        if not valid_szs:
            return None
        return min(valid_szs, key=lambda z: z.distal)
    
    def find_next_bz_below(self, zones: List[Zone], reference_bz: Zone) -> Optional[Zone]:
        """Find next BZ below the reference BZ for Rule C Scenario 2/3.
        v3.8.2: Excludes unaccepted gap zones."""
        zones = zones['BUY']
        valid_bzs = [z for z in zones if z.is_buy_zone and not z.invalidated 
                     and z.distal < reference_bz.distal and z.zone_id != reference_bz.zone_id
                     and not (z.ztype in (ZoneType.GDZ, ZoneType.GSZ) and not z.session_accepted)]
        if not valid_bzs:
            return None
        return max(valid_bzs, key=lambda z: z.distal)

    

    def _preceding_zone(self,
                        zones: List[Zone],
                        *,
                        ztype: Literal["BZ", "SZ"],
                        distal: float,
                        proximal: float,
                        before_idx: int) -> Optional[Zone]:
        """
        Returns the most recent zone of `ztype` whose created_idx < before_idx.
        """
        cands = []
        for z in zones: 
            if ztype == 'SZ':
                zone_mc = z.proximal > proximal
            else:
                zone_mc = z.proximal < proximal
            if z.ztype == ztype and z.created_idx < before_idx and zone_mc:
                cands.append(z)
        if not cands:
            return None
        return max(cands, key=lambda z: z.created_idx)


    def _close_violates_zone(self, df: pd.DataFrame, start_idx: int, zone: Optional[Zone] = None) -> bool:
        """
        PRIMARY violation for trend regime: any CLOSE beyond distal.

        SZ violated if any close > SZ.distal
        BZ violated if any close < BZ.distal

        start_idx is IMPORTANT:
        - For this FIX, we evaluate whether CURRENT zone removed PRECEDING opposing structure,
        so start_idx must be CURRENT zone's created_idx (not the opposing zone's created_idx).
        """
        if not zone:
            return False

        if start_idx >= len(df):
            return False

        closes = df["close"].iloc[start_idx:]
        if zone.ztype == "SZ":
            return bool((closes > zone.distal).any())
        else:
            return bool((closes < zone.distal).any())

    def _preceding_two_zones(self,
                            zones: List[Zone],
                            *,
                            ztype: Literal["BZ", "SZ"],
                            before_idx: int,
                            distal: float,
                            proximal: float
                            ) -> Tuple[Optional[Zone], Optional[Zone]]:
        """
        Returns (z1, z2):
        z1 = most recent preceding zone
        z2 = preceding zone before z1
        """
        z1 = self._preceding_zone(zones, ztype=ztype, before_idx=before_idx, distal=distal, proximal=proximal)
        if not z1:
            return None, None
        z2 = self._preceding_zone(zones, ztype=ztype, before_idx=z1.created_idx, distal=distal, proximal=proximal)
        return z1, z2

    def _cmp(self, df: pd.DataFrame) -> float:
        return float(df["close"].iloc[-1])
    

    
    def calculate_trend(
        self,
        tf: TF,
        df,
        nearest_bz: Optional[Zone],
        nearest_sz: Optional[Zone],
        zones: List[Zone],
        htf_regime: Optional[TrendRegime] = None,
        htf_sz_overhead: bool = False,
        htf_bz_below: bool = False,
        all_zones_for_cascade: Optional[List[Zone]] = None
    ) -> TrendResult:
        """
        Calculate trend for a single timeframe.

        Updated to match SD-engine trend logic:
        - Rule D reinforcers are diagnostic only
        - Rule C Scenario 2 upgrade requires:
        1. Violation Acceptance
        2. Swing Breach
        """

        cmp = float(df["close"].iloc[-1])
        cs = CandleSeries(
            o=df['open'].tolist(),
            h=df['high'].tolist(),
            l=df['low'].tolist(),
            c=df['close'].tolist()
        )
        # Quadrant remains execution context only
        quadrant = None
        quadrant_position_pct = None
        if nearest_bz and nearest_sz:
            quadrant = self.quadrant_calc.calculate(cmp, nearest_bz.distal, nearest_sz.distal)
            if nearest_bz.distal is not None and nearest_sz.distal is not None:
                rng = nearest_sz.distal - nearest_bz.distal
                if rng > 0:
                    quadrant_position_pct = ((cmp - nearest_bz.distal) / rng) * 100.0

        reinforcers = []

        # Rule B: Only BZ (Unchallenged Accumulation)
        if nearest_bz and not nearest_sz:
            # HTF Safeguard (Rule F)
            if htf_regime == TrendRegime.DN:
                return TrendResult(
                    regime=TrendRegime.SW,
                    quadrant=quadrant,
                    quadrant_price_pct=quadrant_position_pct,
                    nearest_bz=nearest_bz,
                    nearest_sz=None,
                    diagnosis="Only BZ exists but HTF is DN or SZ overhead",
                    rule_applied="Rule B",
                    htf_veto_applied=True,
                    htf_veto_reason="HTF_DN or SZ_OVERHEAD"
                )
            return TrendResult(
                regime=TrendRegime.UP,
                quadrant=quadrant,
                quadrant_price_pct=quadrant_position_pct,
                nearest_bz=nearest_bz,
                nearest_sz=None,
                diagnosis="Only BZ exists, no opposing SZ above",
                rule_applied="Rule B (Only BZ)"
            )
        
        # Rule B: Only SZ (Unchallenged Distribution)
        if nearest_sz and not nearest_bz:
            # HTF Safeguard (Rule F)
            if htf_regime == TrendRegime.UP:
                return TrendResult(
                    regime=TrendRegime.SW,
                    quadrant=quadrant,
                    quadrant_price_pct=quadrant_position_pct,
                    nearest_bz=None,
                    nearest_sz=nearest_sz,
                    diagnosis="Only SZ exists but HTF is UP or BZ below",
                    rule_applied="Rule B",
                    htf_veto_applied=True,
                    htf_veto_reason="HTF_UP or BZ_BELOW"
                )
            return TrendResult(
                regime=TrendRegime.DN,
                quadrant=quadrant,
                quadrant_price_pct=quadrant_position_pct,
                nearest_bz=None,
                nearest_sz=nearest_sz,
                diagnosis="Only SZ exists, no opposing BZ below",
                rule_applied="Rule B (Only SZ)"
            )
        
        # Neither BZ nor SZ
        if not nearest_bz and not nearest_sz:
            return TrendResult(
                regime=TrendRegime.SW,
                quadrant=None,
                quadrant_price_pct=quadrant_position_pct,
                nearest_bz=None,
                nearest_sz=None,
                diagnosis="No valid zones found",
                rule_applied="No zones"
            )
        
        # ================================================================
        # CASCADING WINDOW (v3.8.8): Full Violation Chain Analysis
        # Before checking nearest BZ vs nearest SZ (Rule C), assess the
        # historical violation chain. If the BZ departure CLOSE-violated
        # multiple SZ distals and no BZ distal was CLOSE-breached on
        # correction (or vice versa), the cascade proves directional
        # dominance even when the current nearest pair hasn't violated
        # each other.
        # Uses ALL raw zones (pre-filter) to see the full history.
        # ================================================================
        if all_zones_for_cascade is not None and len(all_zones_for_cascade) > 0:
            _cascade_all_bz = [z for z in all_zones_for_cascade if z.is_buy_zone]
            _cascade_all_sz = [z for z in all_zones_for_cascade if z.is_sell_zone]
            
            # BZ violation reach: max CLOSE from nearest_bz formation to now
            # Scoped to current structural cycle, not all-time
            _bz_reach = max(cs.c[i] for i in range(nearest_bz.created_idx, cs.n))
            # Count SZ distals violated by BZ reach (CLOSE beyond distal)
            _sz_violated = [z for z in _cascade_all_sz if _bz_reach > z.distal]
            
            # SZ violation reach: min CLOSE from nearest_sz formation to now
            _sz_reach = min(cs.c[i] for i in range(nearest_sz.created_idx, cs.n))
            # Count BZ distals violated by SZ reach
            _bz_violated = [z for z in _cascade_all_bz if _sz_reach < z.distal]
            
            # Check if nearest BZ/SZ distals are intact (no CLOSE breach)
            _bz_intact = all(cs.c[i] >= nearest_bz.distal
                            for i in range(nearest_sz.created_idx, cs.n))
            _sz_intact = all(cs.c[i] <= nearest_sz.distal
                            for i in range(nearest_bz.created_idx, cs.n))
            
            _up_count = len(_sz_violated)
            _dn_count = len(_bz_violated)
            _min_adv = 2  # Minimum cascade advantage to avoid marginal calls
            
            # UP cascade: demand violated multiple supply levels, demand intact
            # HTF veto: SZ overhead blocks ONLY when htf_regime != UP
            # (Sec 6.2 BUG-VETO-SW: "SZ overhead in non-bullish regime (E≠UP)")
            _up_threshold = _min_adv if _sz_intact else 1
            # if (_up_count > 0 and _bz_intact
            #         and (_dn_count == 0 or _up_count >= _dn_count + _min_adv)):
            if (_up_count >= _up_threshold and _bz_intact
                    and (_dn_count == 0 or _up_count >= _dn_count + _min_adv)):
                if htf_regime == TrendRegime.DN:
                    return TrendResult(
                        regime=TrendRegime.SW,
                        quadrant=quadrant,
                        quadrant_price_pct=quadrant_position_pct,
                        nearest_bz=nearest_bz, nearest_sz=nearest_sz,
                        diagnosis=f"Cascade UP ({_up_count} SZ violated, BZ intact) but HTF veto",
                        rule_applied="Cascading Window",
                        htf_veto_applied=True, htf_veto_reason="HTF_DN or SZ_OVERHEAD"
                    )
                return TrendResult(
                    regime=TrendRegime.UP,
                    quadrant=quadrant,
                    quadrant_price_pct=quadrant_position_pct,
                    nearest_bz=nearest_bz, nearest_sz=nearest_sz,
                    diagnosis=(f"Cascade: BZ reach {_bz_reach:.0f} violated {_up_count} SZ distals, "
                               f"BZ distal {nearest_bz.distal:.0f} intact, {_dn_count} BZ violated"),
                    rule_applied="Cascading Window (Demand Dominance)"
                )
            
            # DN cascade: supply violated multiple demand levels, supply intact
            # HTF veto: BZ below blocks ONLY when htf_regime != DN
            _dn_threshold = _min_adv if _bz_intact else 1
            # if (_dn_count > 0 and _sz_intact
            #         and (_up_count == 0 or _dn_count >= _up_count + _min_adv)):
            if (_dn_count >= _dn_threshold and _sz_intact
                    and (_up_count == 0 or _dn_count >= _up_count + _min_adv)):
                if htf_regime == TrendRegime.UP:
                    return TrendResult(
                        regime=TrendRegime.SW,
                        quadrant=quadrant,
                        quadrant_price_pct=quadrant_position_pct,
                        nearest_bz=nearest_bz, nearest_sz=nearest_sz,
                        diagnosis=f"Cascade DN ({_dn_count} BZ violated, SZ intact) but HTF veto",
                        rule_applied="Cascading Window",
                        htf_veto_applied=True, htf_veto_reason="HTF_UP or BZ_BELOW"
                    )
                return TrendResult(
                    regime=TrendRegime.DN,
                    quadrant=quadrant,
                    quadrant_price_pct=quadrant_position_pct,
                    nearest_bz=nearest_bz, nearest_sz=nearest_sz,
                    diagnosis=(f"Cascade: SZ reach {_sz_reach:.0f} violated {_dn_count} BZ distals, "
                               f"SZ distal {nearest_sz.distal:.0f} intact, {_up_count} SZ violated"),
                    rule_applied="Cascading Window (Supply Dominance)"
                )
        
        # Rule C: Sliding Window Dominance (both BZ and SZ exist)
        bz_violated_sz = self.violation_checker.has_bz_violated_sz(cs, nearest_bz, nearest_sz)
        sz_violated_bz = self.violation_checker.has_sz_violated_bz(cs, nearest_sz, nearest_bz)
        
        # Check reinforcers (Rule D/E) — DIAGNOSTIC ONLY (v3.8.2)
        # HH/LH are reported in TrendResult but do NOT trigger Scenario 2 upgrades
        if self.reinforcer_detector.detect_higher_highs(cs, nearest_bz.created_idx):
            reinforcers.append("Higher Highs")
        if self.reinforcer_detector.detect_swing_high_breach(cs, nearest_bz):
            reinforcers.append("Swing High Breach")
        if self.reinforcer_detector.detect_lower_highs(cs, nearest_sz.created_idx):
            reinforcers.append("Lower Highs")
        if self.reinforcer_detector.detect_swing_low_breach(cs, nearest_sz):
            reinforcers.append("Swing Low Breach")
        
        # v3.8.2: Structural Scenario 2 upgrade criteria
        # Computed here, used only in Scenario 2 branches below
        hold_bars = self.cfg.violation_hold_bars
        
        # UPTREND upgrade: Violation Acceptance + Swing High Breach
        up_violation_accepted = self.violation_checker.is_violation_accepted_above(
            cs, nearest_bz, nearest_sz, hold_bars
        )
        up_swing_breach = self.reinforcer_detector.detect_swing_high_breach(cs, nearest_bz)
        
        # DOWNTREND upgrade: Violation Acceptance + Swing Low Breach
        dn_violation_accepted = self.violation_checker.is_violation_accepted_below(
            cs, nearest_sz, nearest_bz, hold_bars
        )
        dn_swing_breach = self.reinforcer_detector.detect_swing_low_breach(cs, nearest_sz)
        
        # UPTREND scenarios (v4.4.1 Section 5.1 Rule C)
        if bz_violated_sz and not sz_violated_bz:
            sz2 = self.find_next_sz_above(zones, nearest_sz)
            
            # Scenario 1: No SZ above -> UPTREND
            if sz2 is None:
                if htf_regime == TrendRegime.DN:
                    return TrendResult(
                        regime=TrendRegime.SW,
                        quadrant=quadrant,
                        quadrant_price_pct=quadrant_position_pct,
                        nearest_bz=nearest_bz,
                        nearest_sz=nearest_sz,
                        diagnosis="BZ violated SZ, no SZ above, but HTF veto",
                        rule_applied="Rule C Scenario 1",
                        reinforcers=reinforcers,
                        htf_veto_applied=True,
                        htf_veto_reason="HTF_DN or SZ_OVERHEAD"
                    )
                return TrendResult(
                    regime=TrendRegime.UP,
                    quadrant=quadrant,
                    quadrant_price_pct=quadrant_position_pct,
                    nearest_bz=nearest_bz,
                    nearest_sz=nearest_sz,
                    diagnosis="BZ CLOSE-violated SZ, no SZ above",
                    rule_applied="Rule C Scenario 1",
                    reinforcers=reinforcers
                )
            
            # Check if BZ also violated SZ2
            bz_violated_sz2 = self.violation_checker.has_bz_violated_sz(cs, nearest_bz, sz2)
            
            # Scenario 3: BZ violated both SZ1 and SZ2 -> UPTREND
            if bz_violated_sz2:
                if htf_regime == TrendRegime.DN:
                    return TrendResult(
                        regime=TrendRegime.SW,
                        quadrant=quadrant,
                        quadrant_price_pct=quadrant_position_pct,
                        nearest_bz=nearest_bz,
                        nearest_sz=nearest_sz,
                        diagnosis="BZ violated SZ1 and SZ2, but HTF veto",
                        rule_applied="Rule C Scenario 3",
                        reinforcers=reinforcers,
                        htf_veto_applied=True,
                        htf_veto_reason="HTF_DN or SZ_OVERHEAD"
                    )
                return TrendResult(
                    regime=TrendRegime.UP,
                    quadrant=quadrant,
                    quadrant_price_pct=quadrant_position_pct,
                    nearest_bz=nearest_bz,
                    nearest_sz=nearest_sz,
                    diagnosis="BZ CLOSE-violated both SZ1 and SZ2 (multi-zone dominance)",
                    rule_applied="Rule C Scenario 3",
                    reinforcers=reinforcers
                )
            
            # Scenario 2: SZ2 intact -> SIDEWAYS by default
            # v3.8.2: Upgrade to UPTREND ONLY if BOTH conditions met:
            #   1. Violation Acceptance: CLOSE sustains above SZ1 proximal for N bars
            #   2. Swing High Breach: BZ departure broke a major swing high
            # Plus: HTF must not veto
            if (up_violation_accepted and up_swing_breach 
                    and htf_regime != TrendRegime.DN):
                return TrendResult(
                    regime=TrendRegime.UP,
                    quadrant=quadrant,
                    quadrant_price_pct=quadrant_position_pct,
                    nearest_bz=nearest_bz,
                    nearest_sz=nearest_sz,
                    diagnosis=(
                        "BZ violated SZ1, SZ2 intact — upgraded via "
                        "Violation Acceptance + Swing High Breach (v3.8.2)"
                    ),
                    rule_applied="Rule C Scenario 2 + Structural Upgrade (v3.8.2)",
                    reinforcers=reinforcers
                )
            
            return TrendResult(
                regime=TrendRegime.SW,
                quadrant=quadrant,
                quadrant_price_pct=quadrant_position_pct,
                nearest_bz=nearest_bz,
                nearest_sz=nearest_sz,
                diagnosis="BZ violated SZ1, but SZ2 above remains intact",
                rule_applied="Rule C Scenario 2",
                reinforcers=reinforcers
            )
        
        # DOWNTREND scenarios (v4.4.1 Section 5.2 Rule C)
        if sz_violated_bz and not bz_violated_sz:
            bz2 = self.find_next_bz_below(zones, nearest_bz)
            
            # Scenario 1: No BZ below -> DOWNTREND
            if bz2 is None:
                if htf_regime == TrendRegime.UP:
                    return TrendResult(
                        regime=TrendRegime.SW,
                        quadrant=quadrant,
                        quadrant_price_pct=quadrant_position_pct,
                        nearest_bz=nearest_bz,
                        nearest_sz=nearest_sz,
                        diagnosis="SZ violated BZ, no BZ below, but HTF veto",
                        rule_applied="Rule C Scenario 1",
                        reinforcers=reinforcers,
                        htf_veto_applied=True,
                        htf_veto_reason="HTF_UP or BZ_BELOW"
                    )
                return TrendResult(
                    regime=TrendRegime.DN,
                    quadrant=quadrant,
                    quadrant_price_pct=quadrant_position_pct,
                    nearest_bz=nearest_bz,
                    nearest_sz=nearest_sz,
                    diagnosis="SZ CLOSE-violated BZ, no BZ below",
                    rule_applied="Rule C Scenario 1",
                    reinforcers=reinforcers
                )
            
            # Check if SZ also violated BZ2
            sz_violated_bz2 = self.violation_checker.has_sz_violated_bz(cs, nearest_sz, bz2)
            
            # Scenario 3: SZ violated both BZ1 and BZ2 -> DOWNTREND
            if sz_violated_bz2:
                if htf_regime == TrendRegime.UP:
                    return TrendResult(
                        regime=TrendRegime.SW,
                        quadrant=quadrant,
                        quadrant_price_pct=quadrant_position_pct,
                        nearest_bz=nearest_bz,
                        nearest_sz=nearest_sz,
                        diagnosis="SZ violated BZ1 and BZ2, but HTF veto",
                        rule_applied="Rule C Scenario 3",
                        reinforcers=reinforcers,
                        htf_veto_applied=True,
                        htf_veto_reason="HTF_UP or BZ_BELOW"
                    )
                return TrendResult(
                    regime=TrendRegime.DN,
                    quadrant=quadrant,
                    quadrant_price_pct=quadrant_position_pct,
                    nearest_bz=nearest_bz,
                    nearest_sz=nearest_sz,
                    diagnosis="SZ CLOSE-violated both BZ1 and BZ2 (multi-zone dominance)",
                    rule_applied="Rule C Scenario 3",
                    reinforcers=reinforcers
                )
            
            # Scenario 2: BZ2 intact -> SIDEWAYS by default
            # v3.8.2: Upgrade to DOWNTREND ONLY if BOTH conditions met:
            #   1. Violation Acceptance: CLOSE sustains below BZ1 proximal for N bars
            #   2. Swing Low Breach: SZ departure broke a major swing low
            # Plus: HTF must not veto
            if (dn_violation_accepted and dn_swing_breach
                    and htf_regime != TrendRegime.UP):
                return TrendResult(
                    regime=TrendRegime.DN,
                    quadrant=quadrant,
                    quadrant_price_pct=quadrant_position_pct,
                    nearest_bz=nearest_bz,
                    nearest_sz=nearest_sz,
                    diagnosis=(
                        "SZ violated BZ1, BZ2 intact — upgraded via "
                        "Violation Acceptance + Swing Low Breach (v3.8.2)"
                    ),
                    rule_applied="Rule C Scenario 2 + Structural Upgrade (v3.8.2)",
                    reinforcers=reinforcers
                )
            
            return TrendResult(
                regime=TrendRegime.SW,
                quadrant=quadrant,
                quadrant_price_pct=quadrant_position_pct,
                nearest_bz=nearest_bz,
                nearest_sz=nearest_sz,
                diagnosis="SZ violated BZ1, but BZ2 below remains intact",
                rule_applied="Rule C Scenario 2",
                reinforcers=reinforcers
            )
        
        # SIDEWAYS: Both sides violated or neither
        return TrendResult(
            regime=TrendRegime.SW,
            quadrant=quadrant,
            quadrant_price_pct=quadrant_position_pct,
            nearest_bz=nearest_bz,
            nearest_sz=nearest_sz,
            diagnosis="Both BZ and SZ exist, neither has multi-zone dominance",
            rule_applied="Section 5.3 (Sideways)",
            reinforcers=reinforcers
        )

##################################################################################################

##################################################################################################

class MultiTimeframeTrendCalculator:
    """
    Section 6: Multi-Timeframe Hierarchy
    
    Enhanced with:
    - Trade Type classification (v4.1 Section 9)
    - DBR/RBR requirements (v4.1 Section 10)
    - HTF Veto logic (v4.1 Section 6.2)
    """
    
    def __init__(self):
        self.cfg = Config()
        self.trend_engine = TrendEngine(self.cfg)
        self.execution_engine = ExecutionDecisionEngine()
        self.htf_veto_engine = HTFVetoEngine()
        self.trade_type_classifier = TradeTypeClassifier()

    def _set_symbol_and_timeframe(self, time_list, tick, last_d_time):
        self.csv_path_E = os.path.join(stock_data_dir_config.indian_stock_data_dir, 'latest_data_csv', f'{tick}_{time_list[0]}.csv')
        self.csv_path_A = os.path.join(stock_data_dir_config.indian_stock_data_dir, 'latest_data_csv', f'{tick}_{time_list[1]}.csv')
        self.csv_path_X = os.path.join(stock_data_dir_config.indian_stock_data_dir, 'latest_data_csv', f'{tick}_{time_list[-1]}.csv')    
        self.last_d_time = last_d_time

    def _set_symbol_and_timeframe_future_and_commodity(self, time_list, tick, last_d_time, exp_num, data_dir):
        self.csv_path_E = os.path.join(data_dir, 'latest_data_csv', f'{tick}_{exp_num}_{time_list[0]}.csv')
        self.csv_path_A = os.path.join(data_dir, 'latest_data_csv', f'{tick}_{exp_num}_{time_list[1]}.csv')
        self.csv_path_X = os.path.join(data_dir, 'latest_data_csv', f'{tick}_{exp_num}_{time_list[-1]}.csv')    
        self.last_d_time = last_d_time

    def to_api(self, obj):
        """Convert dataclasses/enums/nested structures into JSON-safe python types."""
        if obj is None:
            return None
        if isinstance(obj, Enum):
            return obj.value
        if is_dataclass(obj):
            # asdict() recursively converts nested dataclasses to dicts/lists
            return {k: self.to_api(v) for k, v in asdict(obj).items()}
        if isinstance(obj, dict):
            return {k: self.to_api(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [self.to_api(v) for v in obj]
        # numbers/strings/bools fall through
        return obj

    def is_valid_for_rule_c(self, zone: Zone) -> bool:
        """
        Check if gap zone is valid for Rule C trend calculation.
        
        Gap zones require session_accepted == TRUE before counting.
        """
        if zone.ztype not in (ZoneType.GDZ, ZoneType.GSZ):
            return True  # Regular zones always valid
        
        return zone.session_accepted

    def get_current_cmp(self, csv_path, last_d_time):
        df = pd.read_csv(csv_path)
        col_name = 'tradeDate' if 'tradeDate' in df.columns else 'timestamp'
        df[col_name] = pd.to_datetime(df[col_name], dayfirst = True)
        df = df[df[col_name] <= last_d_time]
        return df['close'].iloc[-1]
    
    def calculate_full_context(
        self,
        htf_sz_overhead: bool = False,
        htf_bz_below: bool = False,
        zones_E_cascade: Optional[List[Zone]] = None,
        zones_A_cascade: Optional[List[Zone]] = None

    ) -> TrendContext:
        """
        Calculate complete trend context across all timeframes.
        
        Hierarchy:
        1. E regime calculated first (no HTF reference)
        2. A regime calculated with E as HTF veto
        3. X regime calculated with A as HTF veto
        4. Trade Type classification
        5. HTF Veto check
        6. DBR/RBR requirements determination
        """
        res_zone_E, all_zone_E = process_trend_zones(self.csv_path_E, TF.E, self.last_d_time, for_frps=False)
        res_zone_A, all_zone_A = process_trend_zones(self.csv_path_A, TF.A, self.last_d_time, for_frps=False)
        res_zone_X, all_zone_X = process_trend_zones(self.csv_path_X, TF.X, self.last_d_time)
        # zones_A_rule_c = [z for z in res_zone_A if self.is_valid_for_rule_c(z)]

        filtered_zone_E = res_zone_E['BUY'] + res_zone_E['SELL']
        zones_E_rule_c = [z for z in filtered_zone_E if self.is_valid_for_rule_c(z)]
        htf_sz_overhead = any(z.is_sell_zone and z.distal > self.get_current_cmp(self.csv_path_X, self.last_d_time) for z in zones_E_rule_c if not z.invalidated)
        htf_bz_below = any(z.is_buy_zone and z.distal < self.get_current_cmp(self.csv_path_X, self.last_d_time) for z in zones_E_rule_c if not z.invalidated)

        df_E, _ = load_preprocess_data(self.csv_path_E, self.last_d_time)
        df_A, _ = load_preprocess_data(self.csv_path_A, self.last_d_time)
        df_X, _ = load_preprocess_data(self.csv_path_X, self.last_d_time)

        cmp_E = float(df_E['close'].iloc[-1])
        cmp_A = float(df_A['close'].iloc[-1])
        cmp_X = float(df_X['close'].iloc[-1])
        
        buy_zone_E = self.trend_engine.find_nearest_bz(res_zone_E, cmp_E)
        sell_zone_E = self.trend_engine.find_nearest_sz(res_zone_E, cmp_E)
        buy_zone_A = self.trend_engine.find_nearest_bz(res_zone_A, cmp_A)
        sell_zone_A = self.trend_engine.find_nearest_sz(res_zone_A, cmp_A)
        buy_zone_X = self.trend_engine.find_nearest_bz(res_zone_X, cmp_X)
        sell_zone_X = self.trend_engine.find_nearest_sz(res_zone_X, cmp_X)

        # buy_zone_E = res_zone_E['BUY'][0] if len(res_zone_E['BUY']) > 0 else None
        # sell_zone_E = res_zone_E['SELL'][0] if len(res_zone_E['SELL']) > 0 else None

        # buy_zone_A = res_zone_A['BUY'][0] if len(res_zone_A['BUY']) > 0 else None
        # sell_zone_A = res_zone_A['SELL'][0] if len(res_zone_A['SELL']) > 0 else None

        # buy_zone_X = res_zone_X['BUY'][0] if len(res_zone_X['BUY']) > 0 else None
        # sell_zone_X = res_zone_X['SELL'][0] if len(res_zone_X['SELL']) > 0 else None
        # Step 1: Calculate Evaluate TF (primary)


        result_E = self.trend_engine.calculate_trend(
            tf=TF.E,
            df=df_E,
            zones=res_zone_E,
            nearest_bz=buy_zone_E,
            nearest_sz=sell_zone_E,
            htf_regime=None,
            all_zones_for_cascade=zones_E_cascade
        )

        result_A = self.trend_engine.calculate_trend(
            tf=TF.A,
            df=df_A,
            zones=res_zone_A,
            nearest_bz=buy_zone_A,
            nearest_sz=sell_zone_A,
            htf_regime=result_E.regime,
            htf_sz_overhead=htf_sz_overhead,
            htf_bz_below=htf_bz_below,
            all_zones_for_cascade=zones_A_cascade
        )
        result_X = self.trend_engine.calculate_trend(
            tf=TF.X,
            df=df_X,
            zones=res_zone_X,
            nearest_bz=buy_zone_X,
            nearest_sz=sell_zone_X,
            htf_regime=result_A.regime,
            htf_sz_overhead=htf_sz_overhead,
            htf_bz_below=htf_bz_below
        )
        
        # Step 4: Get execution permissions with trade types
        permissions = self.execution_engine.get_permissions(result_E.regime, result_A.regime, result_X.quadrant)
        
        # Step 5: Check HTF veto
        veto_longs, veto_shorts, veto_reason = self.htf_veto_engine.check_veto(
            result_E.regime, result_A.regime, htf_sz_overhead, htf_bz_below
        )
        
        # Apply veto — DOCTRINE ALIGNMENT (v3.8.6 fix):
        # Sec 6.2 says SZ overhead/CONFLICT → "only DBR/RBD permitted", NOT
        # "block all longs/shorts". The veto converts from direction block to
        # PATTERN REQUIREMENT. Continuation patterns (RBR/DBD) are blocked;
        # reversal patterns (DBR/RBD) are permitted. Parameter gates (G4 RR,
        # G6b obstruction, G8 structure) handle whether the specific trade
        # is viable given the overhead/below supply/demand.
        _dbr_override = False
        _rbd_override = False
        if veto_longs:
            # Veto longs → require DBR instead of blanket block
            _dbr_override = True
            # allow_long stays True — G18 (DBR enforcement) will filter
        if veto_shorts:
            # Veto shorts → require RBD instead of blanket block
            _rbd_override = True
            # allow_short stays True — G18 (RBD enforcement) will filter
        
        allow_long = permissions['allow_long']    # NOT modified by veto
        allow_short = permissions['allow_short']  # NOT modified by veto
        _final_dbr = permissions['dbr_required'] or _dbr_override
        _final_rbd = permissions['rbd_required'] or _rbd_override
        
        # BUG-SNAPSHOT-RECONCILE FIX: Reconcile permitted_setup and trade_type
        # with FINAL allow_long/allow_short (after veto). Raw matrix strings
        # must NOT leak to TrendContext when the direction is blocked.
        _setup_long = permissions['long_setup'] if allow_long else "BLOCKED"
        _setup_short = permissions['short_setup'] if allow_short else "BLOCKED"
        _tt_long = permissions['trade_type_long'] if allow_long else TradeType.NO_TRADE
        _tt_short = permissions['trade_type_short'] if allow_short else TradeType.NO_TRADE
        # Override setup text when veto converted to pattern requirement
        if _dbr_override and allow_long:
            _setup_long = "DBR longs only (HTF SZ overhead)"
            if _tt_long not in (TradeType.REVERSAL_ONLY, TradeType.NO_TRADE):
                _tt_long = TradeType.REVERSAL_ONLY
        if _rbd_override and allow_short:
            _setup_short = "RBD shorts only (HTF conflict)"
            if _tt_short not in (TradeType.REVERSAL_ONLY, TradeType.NO_TRADE):
                _tt_short = TradeType.REVERSAL_ONLY
        
        return TrendContext(
            regime_E=result_E.regime,
            regime_A=result_A.regime,
            regime_X=result_X.regime,
            quadrant_E=result_E.quadrant,
            quadrant_A=result_A.quadrant,
            quadrant_X=result_X.quadrant,
            allow_long=allow_long,
            allow_short=allow_short,
            bias=permissions['bias'],
            permitted_setup=f"Long: {_setup_long} | Short: {_setup_short}",
            trade_type_long=_tt_long,
            trade_type_short=_tt_short,
            dbr_required=_final_dbr,
            rbd_required=_final_rbd,
            htf_veto_longs=veto_longs,
            htf_veto_shorts=veto_shorts,
            htf_veto_reason=veto_reason,
            result_E=result_E,
            result_A=result_A,
            result_X=result_X
        )


###############################################################################################################################

###############################################################################################################################

class TradeTypeClassifier:
    """
    v4.1 Section 9: Continuation vs Reversal vs No-Trade Classification
    
    Classification Logic Flow:
    Step 1: Is Eval aligned with trade direction?
    Step 2: Is Analyze aligned with trade direction?
    Step 3: If Eval = SW, determine based on Analyze
    """
    
    def classify_long(self, eval_regime: TrendRegime, analyze_regime: TrendRegime) -> TradeType:
        """Classify trade type for LONG positions."""
        
        # Step 1: Is Eval aligned with long direction (UP)?
        if eval_regime == TrendRegime.UP:
            # Step 2: Is Analyze aligned?
            if analyze_regime == TrendRegime.UP:
                return TradeType.CONTINUATION
            elif analyze_regime == TrendRegime.SW:
                return TradeType.CONT_REDUCED
            else:  # DN
                return TradeType.REVERSAL_ONLY  # DBR required
        
        # Step 3: Eval = SW
        elif eval_regime == TrendRegime.SW:
            if analyze_regime == TrendRegime.UP:
                return TradeType.CONT_REDUCED
            elif analyze_regime == TrendRegime.SW:
                return TradeType.RANGE_EXTREME
            else:  # DN
                return TradeType.NO_TRADE
        
        # Eval = DN (opposes long direction)
        else:  # DN
            if analyze_regime == TrendRegime.UP:
                return TradeType.REVERSAL_ONLY  # DBR required
            elif analyze_regime == TrendRegime.SW:
                return TradeType.NO_TRADE
            else:  # DN
                return TradeType.NO_TRADE
    
    def classify_short(self, eval_regime: TrendRegime, analyze_regime: TrendRegime) -> TradeType:
        """Classify trade type for SHORT positions."""
        
        # Step 1: Is Eval aligned with short direction (DN)?
        if eval_regime == TrendRegime.DN:
            if analyze_regime == TrendRegime.DN:
                return TradeType.CONTINUATION
            elif analyze_regime == TrendRegime.SW:
                return TradeType.CONT_REDUCED
            else:  # UP
                return TradeType.REVERSAL_ONLY  # RBR required
        
        # Step 3: Eval = SW
        elif eval_regime == TrendRegime.SW:
            if analyze_regime == TrendRegime.DN:
                return TradeType.CONT_REDUCED
            elif analyze_regime == TrendRegime.SW:
                return TradeType.RANGE_EXTREME
            else:  # UP
                return TradeType.NO_TRADE
        
        # Eval = UP (opposes short direction)
        else:  # UP
            if analyze_regime == TrendRegime.DN:
                return TradeType.REVERSAL_ONLY  # RBR required
            elif analyze_regime == TrendRegime.SW:
                return TradeType.NO_TRADE
            else:  # UP
                return TradeType.NO_TRADE
    
    def is_dbr_required(self, trade_type: TradeType) -> bool:
        """DBR is required when trade type is REVERSAL_ONLY for longs."""
        return trade_type == TradeType.REVERSAL_ONLY
    
    def is_rbd_required(self, trade_type: TradeType) -> bool:
        """RBR is required when trade type is REVERSAL_ONLY for shorts."""
        return trade_type == TradeType.REVERSAL_ONLY


#########################################################################################

#########################################################################################


class ExecutionDecisionEngine:
    """
    Sections 7 & 8: Execution Decision Trees (9 scenarios each)
    
    Enhanced with TradeType classification and DBR/RBR requirements.
    """
    
    # Section 7: BUY Decision Tree
    # (Eval, Analyze) -> (Bias, Permitted Setup, Allow Long, Trade Type, Hard Conditions)
    BUY_MATRIX = {
        (TrendRegime.UP, TrendRegime.UP): ("Strong Bullish", "Continuation longs", True, TradeType.CONTINUATION, RuleSpec(allowed_quadrants={Quadrant.Q1, Quadrant.Q2}, text="Q1/Q2; avoid HTF SZ overhead; full size")),
        (TrendRegime.UP, TrendRegime.SW): ("Cautious Bullish", "BZ longs only", True, TradeType.CONT_REDUCED, RuleSpec(allowed_quadrants={Quadrant.Q1}, text="Q1 only; zone-in-zone preferred; reduced size")),
        (TrendRegime.UP, TrendRegime.DN): ("Conflicted", "DBR longs only", True, TradeType.REVERSAL_ONLY, RuleSpec(allowed_quadrants={Quadrant.Q1}, text="DBR required; price must enter Analyze BZ; Q1; tight risk")),
        (TrendRegime.SW, TrendRegime.UP): ("Emerging Bullish", "BZ longs (reduced)", True, TradeType.CONT_REDUCED, RuleSpec(allowed_quadrants={Quadrant.Q1}, text="Q1; fresh BZ; confirmation required")),
        (TrendRegime.SW, TrendRegime.SW): ("Neutral", "Range-extremes", True, TradeType.RANGE_EXTREME, RuleSpec(allowed_quadrants={Quadrant.Q1}, text="Q1; clear rejection; no trend assumption")),
        (TrendRegime.SW, TrendRegime.DN): ("Weak Bearish", "Typically no longs", False, TradeType.NO_TRADE, RuleSpec(allowed_quadrants={Quadrant.Q1}, text="Only deepest discount BZ with HTF demand; otherwise avoid")),
        (TrendRegime.DN, TrendRegime.UP): ("Conflicted", "DBR longs only", True, TradeType.REVERSAL_ONLY, RuleSpec(allowed_quadrants={Quadrant.Q1}, text="DBR + HTF demand required; Q1 only; tight risk")),
        (TrendRegime.DN, TrendRegime.SW): ("Weak Bearish", "Deep discount BZ only", False, TradeType.NO_TRADE, RuleSpec(allowed_quadrants={Quadrant.Q1}, text="Only if entering HTF BZ; Q1; reduced size")),
        (TrendRegime.DN, TrendRegime.DN): ("Strong Bearish", "NO LONGS", False, TradeType.NO_TRADE, RuleSpec(allowed_quadrants={Quadrant.Q1}, text="Wait for regime change; reassess after confirmed DBR")),
    }
    
    # Section 8: SELL Decision Tree
    SELL_MATRIX = {
        (TrendRegime.DN, TrendRegime.DN): ("Strong Bearish", "Continuation shorts", True, TradeType.CONTINUATION, RuleSpec(allowed_quadrants={Quadrant.Q3, Quadrant.Q2}, text="Q3/Q2; avoid HTF BZ below; full size")),
        (TrendRegime.DN, TrendRegime.SW): ("Cautious Bearish", "SZ shorts only", True, TradeType.CONT_REDUCED, RuleSpec(allowed_quadrants={Quadrant.Q3}, text="Q3 only; zone-in-zone preferred; reduced size")),
        (TrendRegime.DN, TrendRegime.UP): ("Conflicted", "RBR shorts only", True, TradeType.REVERSAL_ONLY, RuleSpec(allowed_quadrants={Quadrant.Q3}, text="RBR required; price must enter Analyze SZ; Q3; tight risk")),
        (TrendRegime.SW, TrendRegime.DN): ("Emerging Bearish", "SZ shorts (reduced)", True, TradeType.CONT_REDUCED, RuleSpec(allowed_quadrants={Quadrant.Q3}, text="Q3; fresh SZ; confirmation required")),
        (TrendRegime.SW, TrendRegime.SW): ("Neutral", "Range-extremes", True, TradeType.RANGE_EXTREME, RuleSpec(allowed_quadrants={Quadrant.Q3}, text="Q3; clear rejection; no trend assumption")),
        (TrendRegime.SW, TrendRegime.UP): ("Weak Bullish", "Typically no shorts", False, TradeType.NO_TRADE, RuleSpec(allowed_quadrants={Quadrant.Q3}, text="Only deepest premium SZ with HTF supply; otherwise avoid")),
        (TrendRegime.UP, TrendRegime.DN): ("Conflicted", "RBR shorts only", True, TradeType.REVERSAL_ONLY, RuleSpec(allowed_quadrants={Quadrant.Q3}, text="RBR + HTF supply required; Q3 only; tight risk")),
        (TrendRegime.UP, TrendRegime.SW): ("Weak Bullish", "Deep premium SZ only", True, TradeType.CONT_REDUCED, RuleSpec(allowed_quadrants={Quadrant.Q3}, text="Only if entering HTF SZ; Q3; reduced size")),
        (TrendRegime.UP, TrendRegime.UP): ("Strong Bullish", "NO SHORTS", False, TradeType.NO_TRADE, RuleSpec(allowed_quadrants={Quadrant.Q3}, text="Wait for regime change; reassess after confirmed RBR")),
    }
    
    def __init__(self):
        self.trade_type_classifier = TradeTypeClassifier()

    def _passes_rules(self, allow: bool, rule: RuleSpec, ctx: ExecutionContext) -> bool:
        if not allow:
            return False

        # STRICT: matrix-only when no quadrant
        if ctx.quadrant == Quadrant.NONE:
            return True

        # Otherwise enforce everything
        if rule.allowed_quadrants and ctx.quadrant not in rule.allowed_quadrants:
            return False
        if rule.requires_dbr and not ctx.has_dbr:
            return False
        if rule.requires_rbr and not ctx.has_rbr:
            return False
        if rule.requires_price_in_analyze_zone and not ctx.in_analyze_zone:
            return False
        if rule.requires_htf_demand and not ctx.htf_demand_present:
            return False
        if rule.requires_htf_supply and not ctx.htf_supply_present:
            return False

        return True

    
    def get_permissions(self, eval_regime: TrendRegime, 
                        analyze_regime: TrendRegime, x_quadrant: Quadrant) -> Dict:
        """
        Get complete execution permissions with trade type classification.
        
        Returns dict with:
        - allow_long, allow_short
        - bias
        - trade_type_long, trade_type_short
        - dbr_required, rbd_required
        - hard_conditions_long, hard_conditions_short
        """
        key = (eval_regime, analyze_regime)
        
        buy_info = self.BUY_MATRIX.get(key)
        sell_info = self.SELL_MATRIX.get(key)
        
        if buy_info is None or sell_info is None:
            return {
                'allow_long': False,
                'allow_short': False,
                'bias': 'Unknown',
                'trade_type_long': TradeType.NO_TRADE,
                'trade_type_short': TradeType.NO_TRADE,
                'dbr_required': False,
                'rbd_required': False,
                'hard_conditions_long': '',
                'hard_conditions_short': ''
            }
        
        bias, long_setup, allow_long, trade_type_long, long_rule  = buy_info
        _, short_setup, allow_short, trade_type_short, short_rule  = sell_info

        # ctx.quadrant = x_quadrant
        # if x_quadrant is not None:
        #     # ctx.quadrant = x_quadrant
        #     if allow_long and long_rule.allowed_quadrants is not None:
        #         if x_quadrant not in long_rule.allowed_quadrants:
        #             allow_long = False

        #     if allow_short and short_rule.allowed_quadrants is not None:
        #         if x_quadrant not in short_rule.allowed_quadrants:
        #             allow_short = False

        # final_trade_type_long = trade_type_long if allow_long else TradeType.NO_TRADE
        # final_trade_type_short = trade_type_short if allow_short else TradeType.NO_TRADE

        return {
            'allow_long': allow_long,
            'allow_short': allow_short,
            'bias': bias,
            'trade_type_long': trade_type_long,
            'trade_type_short': trade_type_short,
            'dbr_required': trade_type_long == TradeType.REVERSAL_ONLY,
            'rbd_required': trade_type_short == TradeType.REVERSAL_ONLY,
            'long_setup': long_setup,
            'short_setup': short_setup,
            'hard_conditions_long': long_rule.text,
            'hard_conditions_short': short_rule.text
        }


# ==============================================================================
# NEW v3.3: HTF VETO ENGINE (v4.1 Section 6.2)
# ==============================================================================

class HTFVetoEngine:
    """
    v4.1 Section 6.2: HTF/LTF Veto Rules (Strict)
    
    ABSOLUTE RULE: Higher Timeframe Structure ALWAYS Dominates. No exceptions.
    
    Situation                          | Veto Rule
    -----------------------------------|------------------------------------------
    HTF SZ overhead + LTF uptrend      | Block continuation longs; only DBR permitted
    HTF BZ below + LTF downtrend       | Block continuation shorts; only RBR permitted
    HTF UP + Analyze DN                | CONFLICT: No continuation; DBR required
    HTF DN + Analyze UP                | CONFLICT: No continuation; RBR required
    LTF zone inside opposing HTF zone  | LTF zone is execution-only; does NOT redefine trend
    """
    
    def check_veto(self, eval_regime: TrendRegime, analyze_regime: TrendRegime,
                   htf_sz_overhead: bool, htf_bz_below: bool) -> Tuple[bool, bool, Optional[str]]:
        """
        Check HTF veto conditions.
        
        Returns: (veto_longs, veto_shorts, reason)
        """
        veto_longs = False
        veto_shorts = False
        reason = None
        
        # HTF SZ overhead + LTF sideways uptrend -> Block continuation longs
        # v3.8.8: When E=UP (cascade or Rule C proved demand dominance),
        # the SZ overhead is already factored into trend determination.
        # Veto only applies when E=SW (no directional conviction to push through).
        # if htf_sz_overhead and eval_regime == TrendRegime.SW:
            # if analyze_regime == TrendRegime.UP:
                # veto_longs = True
                # reason = "HTF SZ overhead blocks continuation longs; only DBR permitted"
        
        # BUG-VETO-SW FIX: HTF SZ overhead in non-bullish regime -> Block longs
        # When unviolated SZ zones exist above CMP and the E-TF regime is NOT UP
        # (i.e., no strong bullish conviction to push through supply), longs face
        # supply ceiling they are unlikely to break. Block regardless of A regime.
        # Self-audit: 2/18 configurations blocked (POWER_GRID×2). Surgical.
        # if htf_sz_overhead and not veto_longs:
            # if eval_regime != TrendRegime.UP:
                # veto_longs = True
                # reason = "HTF SZ overhead in non-bullish regime (E≠UP); longs blocked"
        
        # HTF BZ below + LTF downtrend -> Block continuation shorts
        # REMOVED: Original rule blocked shorts when ANY BZ exists below CMP,
        # regardless of distance. CRUDEOILM: BZ at 5000-7148 (19-30% below CMP
        # 8794) blocked shorts despite SZ at 8801 being ideal short entry.
        # The CONFLICT rules (below) handle E↔A directional mismatches.
        # BZ below is a TARGET for shorts, not a barrier.
        
        # Symmetric fix: ALSO REMOVED for same reason.
        
        # HTF UP + Analyze DN -> CONFLICT
        # if eval_regime == TrendRegime.UP and analyze_regime == TrendRegime.DN:
        #     veto_shorts = True  # No continuation shorts, only RBD
        #     reason = "CONFLICT: HTF UP + Analyze DN; RBD required for shorts"
        
        # HTF DN + Analyze UP -> CONFLICT
        # if eval_regime == TrendRegime.DN and analyze_regime == TrendRegime.UP:
            # veto_longs = True  # No continuation longs, only DBR
            # reason = "CONFLICT: HTF DN + Analyze UP; DBR required for longs"
        
        return veto_longs, veto_shorts, reason