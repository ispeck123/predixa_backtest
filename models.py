from __future__ import annotations
from typing import Optional, List
from enum import Enum
from dataclasses import dataclass, field
import pandas as pd
import numpy as np
from datetime import timedelta


# ==============================================================================
# ENUMERATIONS
# ==============================================================================

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
    SWEEP = "SWEEP"      # Zone invalidated


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


class ZoneState(str, Enum):
    GREEN = "GREEN"
    AMBER = "AMBER"
    RED = "RED"

class ZonePattern(str, Enum):
    RBR = "RBR"
    RBD = "RBD"
    DBR = "DBR"
    DBD = "DBD"
    NONE = "NONE"

# NEW in v3.3: Trade Type Classification (v4.4.1 Section 9)
class TradeType(str, Enum):
    """
    v4.4.1 Section 9.1: Trade Type Definitions
    
    CONTINUATION: Trading in direction of both Eval and Analyze
    CONT_REDUCED: Direction aligned but reduced confidence
    REVERSAL_ONLY: Counter to Eval direction with strict gating (DBR/RBD REQUIRED)
    RANGE_EXTREME: Trading at range boundaries only
    NO_TRADE: No trades in this direction
    """
    CONTINUATION = "CONTINUATION"
    CONT_REDUCED = "CONT_REDUCED"
    REVERSAL_ONLY = "REVERSAL_ONLY"
    RANGE_EXTREME = "RANGE_EXTREME"
    NO_TRADE = "NO_TRADE"


# NEW in v3.3: Pattern Type for DBR/RBD
class PatternType(str, Enum):
    RBR = "RBR"
    RBD = "RBD"
    DBR = "DBR"
    DBD = "DBD"
    NONE = "NONE"


# NEW in v3.4: Zone Age Classification (v4.4.1 Section 13.2)
class ZoneAgeClass(str, Enum):
    """
    v4.4.1 Section 13.2: Zone Age Classification
    
    Zones decay in quality over time unless reactivated.
    """
    FRESH = "FRESH"          # < 50 bars since creation
    ACTIVE = "ACTIVE"        # 50-200 bars + CMP within 5 ATR
    STALE = "STALE"          # > 200 bars OR CMP > 10 ATR away
    REACTIVATED = "REACTIVATED"  # Was STALE but CMP returned to zone


# ==============================================================================
# TREND RESULT DATA CLASS
# ==============================================================================

@dataclass
class TrendResult:
    """Result of trend calculation for a single timeframe."""
    regime: TrendRegime
    quadrant: Optional[Quadrant]
    nearest_bz: Optional[Zone]
    nearest_sz: Optional[Zone]
    diagnosis: str
    rule_applied: str
    reinforcers: List[str] = field(default_factory=list)
    htf_veto_applied: bool = False
    htf_veto_reason: Optional[str] = None



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
    is_live: bool = False
    is_incomplete: Optional[List[bool]] = None
    
    @property
    def n(self) -> int:
        return len(self.o)

    @property
    def n_completed(self) -> int:
        """Number of completed (closed) candles. Zone detection uses this."""
        return self.n - 1 if self.is_live else self.n

    @property
    def cmp(self) -> float:
        return self.c[-1] if self.c else 0.0


# ==============================================================================
# ENHANCED TREND CONTEXT (v4.4.1 Sections 7-9)
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
    
    # Trade Type Classification (v4.4.1 Section 9)
    trade_type_long: TradeType = TradeType.NO_TRADE
    trade_type_short: TradeType = TradeType.NO_TRADE
    dbr_required: bool = False
    rbd_required: bool = False
    
    # HTF Veto status (v4.4.1 Section 6.2)
    htf_veto_longs: bool = False
    htf_veto_shorts: bool = False
    htf_veto_reason: Optional[str] = None
    
    # Detailed results
    result_E: Optional[TrendResult] = None
    result_A: Optional[TrendResult] = None
    result_X: Optional[TrendResult] = None


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
    created_ts: Optional[int] = None
    departure_atr: float = 0.0
    body_pct: float = 0.0
    zone_pattern: Optional[ZonePattern] = None  # "RBR" | "DBR" | "RBD" | "DBD"
    sl_levels: List[float] = field(default_factory=list)
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
    # D5: Gap v2.4 Sec 4 composite scoring (6 dimensions, max 12)
    gap_composite_score: int = 0
    gap_is_structural: bool = False     # Score >= 7 AND S1 passed
    gap_is_mechanical: bool = False     # S1 or S2 failed
    # D8: Gap v2.4 Sec 13 fill tracking
    gap_fill_pct: float = 0.0          # 0.0 = unfilled, 1.0 = fully filled
    # D9: Gap v2.4 Sec 12 breakaway classification
    gap_is_breakaway: bool = False     # Meets criteria B1-B5
    
    # Multi-zone
    is_composite: bool = False
    source_zone_ids: List[str] = field(default_factory=list)
    is_part_of_consecutive: bool = False
    is_part_of_overlapping: bool = False
    consecutive_partner_id: Optional[str] = None
    overlapping_partners_id: List[str] = field(default_factory=list)
    
    # Scoring (v3.3 enhanced)
    raw_score: int = 0
    age_penalty: int = 0
    approach_penalty: int = 0
    penetration_penalty: int = 0
    final_score: int = 0
    quality_priority: str = "UNKNOWN"  # HIGH / MEDIUM / LOW / AVOID
    
    # Risk/Target
    entry_price: Optional[float] = None   # Proximal ± buffer (from RiskTargetCalculator)
    target_price: Optional[float] = None
    stop_price: Optional[float] = None
    rr_ratio: Optional[float] = None
    target_mode: str = "UNKNOWN"  # STRUCTURAL_CONSERVATIVE | STRUCTURAL_AGGRESSIVE | MINIMUM_RR
    
    # DBR/RBD pattern association
    associated_pattern: Optional[PatternType] = None
    pattern_validated: bool = False
    
    # Zone Age (v4.4.1 Section 13.2)
    age_class: str = "FRESH"  # FRESH / ACTIVE / STALE / REACTIVATED
    age_bars: int = 0
    reactivation_count: int = 0
    
    # Wick violation tracking (v4.4.1 Section 15.2)
    wick_violation_detected: bool = False
    reversal_probability_boost: float = 0.0
    
    # Legout validation (v3.4 Section 4.4)
    legout_count: int = 0  # Number of valid legout candles
    legout_range: float = 0.0  # v3.4 ENHANCED: Range of legout sequence (for P2 validation)
    
    # NEW v3.8.1: Topology fields (BUG-08, BUG-11)
    nesting_tier: Optional['ZoneNestingTier'] = None
    nesting_debug: Optional[dict] = None  # Diagnostic info from classify_with_debug
    zone_v38_score: Optional[int] = None
    obstruction_clear: bool = True
    blocking_zone: Optional['Zone'] = None
    enclosing_e_zone: Optional['Zone'] = None  # HTF E-zone containing this zone
    enclosing_a_zone: Optional['Zone'] = None  # HTF A-zone containing this zone
    
    # NEW v3.8.1: Ranking field (BUG-25)
    zone_in_zone: bool = False  # True if TIER_1 or TIER_2 (nested in HTF)

    distal_base: Optional[float] = None

    # BUG-33: Target source tracking (v3.8.4)
    target_source_zone_id: Optional[str] = None
    target_source_tf: Optional[str] = None
    target_multiplier: Optional[float] = None

    structure_low: Optional[float] = None       # Min LOW from swing high to base end (BZ)
    structure_high: Optional[float] = None      # Max HIGH from swing low to base end (SZ)
    structure_start_idx: Optional[int] = None 
    legin_start_idx: Optional[int] = None  

    legout_cleanness: float = 1.0               # 0.0 (worst) to 1.0 (pristine)
    legout_hard_discard: bool = False 

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

    gap_departure_body_abs_atr: float = 1.0   # BUG-38: min absolute body in ATR multiples
    gap_departure_body_min_pct: float = 0.40
    
    # Boundary modes per TF
    boundary_E: BoundaryMode = BoundaryMode.WICK_TO_WICK
    boundary_A: BoundaryMode = BoundaryMode.WICK_TO_WICK
    boundary_X: BoundaryMode = BoundaryMode.BODY_TO_WICK

    basing_max_range_atr: float = 2.5 
    max_structure_extension_atr: float = 5.0
    
    # Gap zones
    # Gap v2.4 Sec 3.1 S3: Min gap size (ATR multiples by volatility regime)
    # v2.4 says >= 0.5x ATR minimum. Low vol = stricter, High vol = relaxed.
    gap_min_atr_low: float = 0.75       # Low vol: tighter filter (D4: aligned to spec)
    gap_min_atr_norm: float = 0.5        # Normal vol: v2.4 Sec 3.1 minimum (D4: was 1.0)
    gap_min_atr_high: float = 0.5        # High vol: v2.4 Sec 3.1 minimum
    # Gap v2.4 Sec 3.1 S4: Departure candle >= 1.2x ATR (acceptable)
    gap_departure_range_atr: float = 1.2  # D2: was 1.0, v2.4 says 1.2x minimum
    # Gap v2.4 Sec 3.1 S5: Body >= 60% of total candle range
    gap_departure_body_pct: float = 0.60  # D3: was 0.50, v2.4 says 60%
    # Gap v2.4 Sec 4: Composite score minimum for structural classification
    gap_composite_score_min: int = 7      # D5: Score < 7 = low-grade, reject
    # Gap v2.4 Sec 12: Breakaway gap criteria
    gap_breakaway_departure_atr: float = 1.5   # D9: B2 criterion
    gap_breakaway_body_pct: float = 0.70       # D9: B2 criterion
    gap_session_followthrough_bars: int = 3
    # D1: Swing lookback for structure removal check
    swing_lookback: int = 20              # Bars to look back for swing high/low
    
    # Structure removal
    major_pivot_lookback: int = 5
    
    # Risk (v3.8.2: unified thresholds)
    default_rr: float = 2.1              # BUG-04 FIX: was 2.0 → 2.1 (no-opposing target multiplier)
    reject_obstructed_targets: bool = False   # config-guarded; default OFF. OOS-refuted: in-sample -0.16R did NOT replicate (OOS +0.39R, pooled +0.12R). Retained for future regime-aware use.
    min_rr: float = 2.1                  # BUG-03 FIX: was 1.5 → 2.1 (unified min RR, cost-adjusted)
    
    # Entry buffer (v3.8.1: BUG-05)
    entry_buffer_pct: float = 0.0015     # ±0.15% from proximal (BUY +, SELL -)
    
    # Target (v3.8.3: HTF-conditional target)
    target_conservative_pct: float = 0.75   # Multiplier when HTF opposing exists (conservative)
    target_buffer_pct: float = 0.003        # ±0.3% buffer from opposing proximal (no-HTF mode)
    
    # SL buffer (v3.8.1: BUG-06)
    sl_buffer_atr: float = 0.25          # 0.25 × ATR beyond reference distal
    
    # Minimum risk floor (v3.8.3_withfix_v1.1: degenerate-SL guard)
    # If risk (|entry - stop|) / entry < this fraction, skip that SL level in cascade.
    # Prevents sub-ATR stops that get triggered by normal intraday noise.
    # Example: BOSCH entry 29595, risk 257 = 0.87% < 1% → skip to wider SL.
    min_risk_pct: float = 0.01
    embed_strict_stop: bool = False      # B-W-EMBED Edit 3: strict structural stop (measure before enabling)
    # B-W-EMBED tunable parameters — REQUIRE full-universe tuning before lock (see embed_tuning.md).
    # Defaults are provisional placeholders, NOT validated values.
    embed_sits_on_top_target_pct: float = 0.5   # TIER_3 far-HTF target discount [TUNE: sweep 0.3-0.7]
    embed_overlap_threshold: float = 0.5        # nesting vs TIER_3 boundary [TUNE: sweep 0.4-0.6]           # 1% minimum risk as fraction of entry price

    single_base_nonbasing_needed: int = 2
    single_base_range_mult: float = 2.5

    
    # Compound staleness gate (v3.8.3_withfix_v1.2: ghost zone guard)
    # Zone must fail BOTH thresholds to be rejected (compound AND logic).
    # Prevents ghost zones from stale historical detections reaching output.
    # Example: AUBANK zone at 773 when CMP is 1030 (age=130, dist=33%) → REJECTED.
    # Counter-example: TRENT zone at 3729 when CMP is 4171 (age=20, dist=10.6%) → PASSES.
    compound_stale_age_bars: int = 100    # Zone age in bars (must exceed this AND...)
    compound_stale_distance_pct: float = 0.20  # ...distance from CMP as fraction (20%)
    
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
    higher_high_candles: int = 5       # Diagnostic only (v3.8.2: no longer triggers upgrade)
    lower_high_candles: int = 5        # Diagnostic only (v3.8.2: no longer triggers upgrade)
    violation_hold_bars: int = 3       # v3.8.2: Min bars CLOSE must sustain beyond violated zone proximal
    
    # DBR/RBD validation
    dbr_rbr_min_base_candles: int = 2
    dbr_rbr_max_base_candles: int = 5
    
    # Zone Quality Scoring thresholds
    zone_quality_high: int = 8
    zone_quality_medium: int = 5
    distance_buffer_atr_high: float = 2.0
    distance_buffer_atr_low: float = 1.0
    min_zone_v38_score: int = 3           # BUG-10 FIX: G10 threshold (was hardcoded)
    
    # Sliding Window Boundaries (v4.4.1 Section 13.1)
    sliding_window_max_bars: int = 200
    sliding_window_max_atr: float = 20.0
    
    # Zone Age Classification (v4.4.1 Section 13.2)
    zone_age_fresh_bars: int = 50
    zone_age_active_bars: int = 200
    zone_age_active_atr: float = 5.0
    zone_age_stale_atr: float = 10.0
    zone_age_fresh_penalty: int = 0
    zone_age_active_penalty: int = -1
    zone_age_stale_penalty: int = -2
    
    # Gap Zone Integration (v4.4.1 Section 14)
    # Gap v2.4 Sec 4: Gap Size scoring (dimension 2 of 6)
    # Score 2: > 0.75x ATR, Score 1: 0.5-0.75x ATR, Score 0: < 0.5x ATR
    gap_score_size_high_atr: float = 0.75   # D10: was 2.0
    gap_score_size_medium_atr: float = 0.5  # D10: was 1.0
    
    # Wick Violation Handling (v4.4.1 Section 15.2)
    wick_violation_reversal_probability_boost: float = 0.15  # 15%
    
    # Setup Extraction (v3.8.2: Step 19 — BUG-21 IMPLEMENTED)
    # REF: Methodology v3.8.2 Sec 9.3, Sec 13.1; Annexure v1.2 Sec 4
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
    ws_proximity: float = 0.02          # #9 Proximity scoring (informational, not a gate)

    target_htf_pct: float = 0.75       # Distance multiplier for HTF opposing (E/A)
    target_xtf_pct: float = 0.90       # Distance multiplier for X-TF opposing
    default_target_atr: float = 3.0    # Fallback ATR multiple when no opposing found

    cleanness_band_1: float = 0.25    # depth ≤ 25% → cleanness 0.85
    cleanness_band_2: float = 0.50    # depth ≤ 50% → cleanness 0.60
    cleanness_band_3: float = 0.75    # depth ≤ 75% → cleanness 0.35
    cleanness_band_4: float = 1.00    # depth ≤ 100% → cleanness 0.15

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



# ==============================================================================
# DBR/RBD PATTERN DATA CLASS (v4.4.1 Section 10)
# ==============================================================================

@dataclass
class ReversalPattern:
    """
    v4.4.1 Section 10: DBR/RBD Pattern
    
    DBR (Drop-Base-Rally): Demand reversal pattern for longs in DN regime
    RBD (Rally-Base-Drop): Supply reversal pattern for shorts in UP regime
    """
    pattern_type: PatternType
    initial_move_start_idx: int
    initial_move_end_idx: int
    base_start_idx: int
    base_end_idx: int
    departure_idx: int
    zone: Optional[Zone] = None
    
    # Validation flags (v4.4.1 Section 10.2)
    has_initial_move: bool = False
    has_base_formation: bool = False
    has_departure: bool = False
    has_structure_shift: bool = False
    has_htf_alignment: bool = False
    correct_quadrant: bool = False
    
    # Overall validation
    is_valid: bool = False
    validation_failures: List[str] = field(default_factory=list)



def _infer_freq_from_path(path: str) -> str:
    """
    Infer pandas frequency alias from filename/path.
    Adjust mappings as needed for your files.
    """
    p = str(path).lower()
    if "month" in p:         # monthly candles
        return "M"          # Month Start
    if "week" in p:          # weekly candles
        return "W"       # Week starts Monday (change to W-SUN if needed)
    if "daily" in p or "day" in p:
        return "D"           # calendar day
    if "two_forty" in p:
        return "4H"          # 4-hour
    if "one_twenty" in p:
        return "2H"          # 2-hour
    if "sixty" in p or "hour" in p:
        return "H"           # hourly
    # add more if you have: 15m, 5m, 1m…
    if "seventy_five" in p or "75" in p:
        return "75T"
    if "fifteen" in p or "15" in p: 
        return "15T"
    if "five" in p or "5" in p:
        return "5T"
    if "one" in p or "1m" in p:
        return "T"           # 1-minute
    # default: treat as daily if unknown
    return "D"

# def _start_of_current_period(ts: pd.Timestamp, tf: str, week_start: int = 0) -> pd.Timestamp:
#     """
#     Compute the start of the *current* period that contains ts.
#     We will keep data with timestamp < returned cutoff (i.e., only completed candles).

#     week_start: 0=Monday .. 6=Sunday
#     """
#     ts = pd.Timestamp(ts)

#     if tf == "M":
#         # Month start at 00:00
#         return ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0, nanosecond=0)

#     if tf == "W":
#         # Week start (default Monday). Compute Monday 00:00 of current week.
#         # If you need Sunday, set week_start=6.
#         weekday = ts.weekday()  # Monday=0 ... Sunday=6
#         market_close_time = ts.replace(hour=15, minute=30, second=0, microsecond=0, nanosecond=0)
#         monday_start = (ts - pd.Timedelta(days=weekday)).normalize()

#         if weekday < 4 or (weekday == 4 and ts < market_close_time):
#             prev_monday = monday_start - pd.Timedelta(days=7)
#             return prev_monday.replace(hour=17, minute=30, second=0, microsecond=0, nanosecond=0)
#         else:
#             return monday_start.replace(hour=17, minute=30, second=0, microsecond=0, nanosecond=0)
#         # delta_days = (ts.weekday() - week_start) % 7
#         # start = (ts - pd.Timedelta(days=delta_days)).normalize()
#         # return start

#     if tf == "D":
#         # Day start at 00:00
#         return ts.normalize()

#     if tf == "H":
#         # Hour start
#         return ts.replace(minute=0, second=0, microsecond=0, nanosecond=0)

#     if tf.endswith("T"):  # minute bars like '15T','5T','T'
#         minutes = 1 if tf == "T" else int(tf[:-1])
#         minute_bucket = (ts.minute // minutes) * minutes
#         return ts.replace(minute=minute_bucket, second=0, microsecond=0, nanosecond=0)

#     # Fallback: treat as daily
#     return ts.normalize()

def _replace_time(ts, hour=None, minute=0, second=0, microsecond=0, nanosecond=0):
    """
    Works with both pandas Timestamp and normal Python datetime.
    """
    kwargs = {
        "minute": minute,
        "second": second,
        "microsecond": microsecond,
    }

    if hour is not None:
        kwargs["hour"] = hour

    # pandas Timestamp supports nanosecond, normal datetime does not
    if hasattr(ts, "nanosecond"):
        kwargs["nanosecond"] = nanosecond

    return ts.replace(**kwargs)

def _floor_by_anchor_hour(ts, interval_hours, anchor_hour=9):
    """
    Example for 4H with anchor_hour=9:
    09:00 - 12:59 => 09:00
    13:00 - 16:59 => 13:00
    17:00 - 20:59 => 17:00
    """

    anchor = _replace_time(
        ts,
        hour=anchor_hour,
        minute=0,
        second=0,
        microsecond=0,
        nanosecond=0
    )

    # If time is before today's anchor, use previous day's anchor
    if ts < anchor:
        anchor = anchor - timedelta(days=1)

    diff_seconds = (ts - anchor).total_seconds()
    diff_hours = int(diff_seconds // 3600)

    bucket_start_hours = (diff_hours // interval_hours) * interval_hours

    return anchor + timedelta(hours=bucket_start_hours)


def _start_of_current_period(ts: pd.Timestamp, tf: str, week_start: int = 0) -> pd.Timestamp:
    """
    Cutoff helper for backtesting on NSE market.

    Caller keeps:
        df[df["timestamp"] < cutoff]

    This version assumes stored candle timestamps are:
    - Daily   -> date at 00:00 or session date marker
    - Weekly  -> Monday date marker
    - Monthly -> first date of month marker
    """
    ts = pd.Timestamp(ts)

    market_open = ts.replace(hour=9, minute=15, second=0, microsecond=0, nanosecond=0)
    market_close = ts.replace(hour=15, minute=30, second=0, microsecond=0, nanosecond=0)

    if tf == "M":
        return ts.replace(day=1, hour=0, minute=0, second=0, microsecond=0, nanosecond=0)

    if tf == "W":
        weekday = ts.weekday()  # Monday=0 ... Sunday=6
        delta_days = (weekday - week_start) % 7
        current_monday = (ts - pd.Timedelta(days=delta_days)).normalize()

        # Before Friday 15:30, this week is incomplete -> exclude it
        friday_close = (current_monday + pd.Timedelta(days=4)).replace(
            hour=15, minute=30, second=0, microsecond=0, nanosecond=0
        )

        return current_monday

    if tf == "D":
        # Before market open, current daily candle should not be treated as active yet
        if ts < market_open:
            return (ts.normalize() - pd.Timedelta(days=1))
        return ts.normalize()

    if tf == "4H":
        # return ts.replace(minute=0, second=0, microsecond=0, nanosecond=0)
        return _floor_by_anchor_hour(ts, interval_hours=4, anchor_hour=9)
    
    if tf == "2H":
        # return ts.replace(minute=0, second=0, microsecond=0, nanosecond=0)
        return _floor_by_anchor_hour(ts, interval_hours=2, anchor_hour=9)


    if tf == "H":
        return ts.replace(minute=0, second=0, microsecond=0, nanosecond=0)

    if tf.endswith("T"):
        minutes = 1 if tf == "T" else int(tf[:-1])
        minute_bucket = (ts.minute // minutes) * minutes
        return ts.replace(minute=minute_bucket, second=0, microsecond=0, nanosecond=0)

    return ts.normalize()

def load_preprocess_data(csv_path, last_d_time):
    """Load and preprocess data once during initialization"""
    df = pd.read_csv(csv_path)
    col = 'tradeDate' if 'tradeDate' in df.columns else 'timestamp'
    df[col] = pd.to_datetime(df[col], dayfirst=True)
    df = df[df[col] <= last_d_time]
    # df.sort_values(by='timestamp', ignore_index=True)
    # self.current_price = self.df['close'].iloc[-1]
    violation_df = df.copy()
    freq = _infer_freq_from_path(csv_path)
    cutoff = _start_of_current_period(last_d_time, freq)
    print(csv_path, freq, cutoff, "*****************************************************************")
    # if freq in ['W', 'M', 'D']:
    df = df[df[col] < cutoff]
    # else:
        # df = df[df["timestamp"] < cutoff]
    if not(str(csv_path).__contains__('monthly') or str(csv_path).__contains__('weekly') or str(csv_path).__contains__('daily')):
        one_year_ago = last_d_time - pd.DateOffset(years=2)
        df = df[df[col] >= one_year_ago]
        violation_df = violation_df[violation_df[col] >= one_year_ago]
        # df.reset_index(inplace=True)
    df['unix_timestamp'] = (df[col].astype(np.int64) // 10**9).astype(int)
    violation_df['unix_timestamp'] = (violation_df[col].astype(np.int64) // 10**9).astype(int)
    df.reset_index(inplace=True)
    violation_df.reset_index(inplace=True)
    # violation_df.reset_index(inplace=True)
    # df = self.calculate_base_candles(df)
    # self._precompute_columns()
    return df, violation_df


def load_preprocess_chart_data(csv_path, last_d_time):
    """Load and preprocess data once during initialization"""
    df = pd.read_csv(csv_path)
    col = 'tradeDate' if 'tradeDate' in df.columns else 'timestamp'
    df[col] = pd.to_datetime(df[col], dayfirst=True)
    df = df[df[col] <= last_d_time]
    # self.current_price = self.df['close'].iloc[-1]
    violation_df = df.copy()
    freq = _infer_freq_from_path(csv_path)
    cutoff = _start_of_current_period(last_d_time, freq)
    # print(freq, cutoff, "*****************************************************************")


    # if freq == 'W':
        # df = df[df[col] <= cutoff]
    # else:
    if freq in ['W', 'M']:
        df = df[df[col] < cutoff]

    if freq == 'W':
        daily_csv_path = csv_path.replace('weekly', 'daily')
        daily_csv_df = pd.read_csv(daily_csv_path)
        daily_csv_df[col] = pd.to_datetime(daily_csv_df[col], dayfirst=True)
        daily_csv_df = daily_csv_df[daily_csv_df[col] <= last_d_time]

        cutoff_candle = aggregate_from_daily(daily_csv_df, 'W', cutoff=cutoff)
        if not cutoff_candle.empty:
            df = pd.concat([df, cutoff_candle], ignore_index=True)
        # df = aggregate_from_daily(daily_csv_df, 'W')


    elif freq == 'M':
        daily_csv_path = csv_path.replace('monthly', 'daily')
        daily_csv_df = pd.read_csv(daily_csv_path)
        daily_csv_df[col] = pd.to_datetime(daily_csv_df[col], dayfirst=True)
        daily_csv_df = daily_csv_df[daily_csv_df[col] <= last_d_time]
        cutoff_candle = aggregate_from_daily(daily_csv_df, 'M', cutoff=cutoff)
        if not cutoff_candle.empty:
            df = pd.concat([df, cutoff_candle], ignore_index=True)
    elif freq == 'D':
        pass  # already daily

    # if not(str(csv_path).__contains__('monthly') or str(csv_path).__contains__('weekly') or str(csv_path).__contains__('daily')):
        # one_year_ago = last_d_time - pd.DateOffset(years=1)
        # df = df[df[col] >= one_year_ago]
        # df.reset_index(inplace=True)
    df.reset_index(inplace=True)
    # violation_df.reset_index(inplace=True)
    # df = self.calculate_base_candles(df)
    # self._precompute_columns()
    return df, violation_df


# def aggregate_from_daily(df, freq):
#     df = df.copy()
#     col = 'tradeDate' if 'tradeDate' in df.columns else 'timestamp'
#     df = df.sort_values(col)

#     if freq == 'W':
#         # week starting Monday
#         df['period'] = df[col].dt.to_period('W-SUN')
#         df['period_start'] = df['period'].apply(lambda p: p.start_time)
#     elif freq == 'M':
#         df['period'] = df[col].dt.to_period('M')
#         df['period_start'] = df['period'].apply(lambda p: p.start_time)
#     else:
#         return df

#     agg_map = {
#         'open': 'first',
#         'high': 'max',
#         'low': 'min',
#         'close': 'last'
#     }

#     if 'volume' in df.columns:
#         agg_map['volume'] = 'sum'

#     out = (
#         df.groupby('period_start', as_index=False)
#           .agg(agg_map)
#           .rename(columns={'period_start': col})
#     )

#     return out


def aggregate_from_daily(df, freq, cutoff):
    df = df.copy()
    col = 'tradeDate' if 'tradeDate' in df.columns else 'timestamp'

    df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).dt.normalize()
    cutoff = pd.to_datetime(cutoff, errors='coerce').normalize()

    if freq == 'W':
        period_start = cutoff
        period_end = cutoff + pd.Timedelta(days=6)
    elif freq == 'M':
        period_start = cutoff
        period_end = cutoff + pd.offsets.MonthEnd(0)
    else:
        return df

    # only rows belonging to the cutoff candle
    df = df[(df[col] >= period_start) & (df[col] <= period_end)].sort_values(col)

    if df.empty:
        return pd.DataFrame(columns=[col, 'open', 'high', 'low', 'close'] + (['volume'] if 'volume' in df.columns else []))

    out = {
        col: [period_start],   # timestamp = start of week/month at 00:00:00
        'open': [df['open'].iloc[0]],
        'high': [df['high'].max()],
        'low': [df['low'].min()],
        'close': [df['close'].iloc[-1]],
    }

    if 'volume' in df.columns:
        out['volume'] = [df['volume'].sum()]

    return pd.DataFrame(out)