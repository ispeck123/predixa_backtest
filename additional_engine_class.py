
from typing import Optional, Tuple, Dict, List
from dataclasses import dataclass, field
from enum import Enum

from scripts.models import Zone, TrendRegime, TF, Quadrant, TradeType, CandleSeries, ReversalPattern, PatternType, ZoneType, ZoneState
from scripts.trend_engine import Config



###############################################################################################

##############################################################

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

@dataclass
class RiskTarget:
    """Risk and target calculation result."""
    entry: float
    stop: float
    target: float
    rr: float
    valid: bool
    reason: Optional[str] = None
    target_mode: str = "UNKNOWN"  # STRUCTURAL | MINIMUM_RR
    target_source_zone_id: Optional[str] = None
    target_source_tf: Optional[str] = None
    target_multiplier: Optional[float] = None
    htf_target_price: Optional[float] = None



class ZoneNestingTier(Enum):
    """X Zone nesting tiers based on HTF alignment."""
    TIER_1 = "TIER_1"  # X nested in BOTH E and A (HIGHEST probability)
    TIER_2 = "TIER_2"  # X nested in E OR A (HIGH probability)
    TIER_3 = "TIER_3"  # X overlapping with E or A (MODERATE probability)
    TIER_4 = "TIER_4"  # X standalone (LOW probability - AVOID)




@dataclass
class SetupPayload:
    """
    v3.8.1 Step 19: Final actionable trade setup output.
    
    REF: Methodology v3.8.1 Sec 9.3 (Ranking Key), Sec 13.1 (Entry Checklist),
         Annexure v1.2 Sec 4 (Proximity Ranking), Sec 5 (Entry/SL/Target).
    BUG-21 IMPLEMENTED.
    """
    # Identity
    symbol: str
    zone_id: str
    zone_type: str              # BZ / SZ / GDZ / GSZ
    timeframe: str              # Execute TF
    
    # Trade
    side: str                   # LONG / SHORT
    entry_price: float          # Proximal ± 0.15% buffer (Annexure Sec 5)
    stop_price: float           # Distal ± 0.25 × ATR topology cascade (BUG-06)
    target_price: float         # 75% opposing proximal OR 2.1 × risk (BUG-07)
    rr_ratio: float
    target_mode: str            # STRUCTURAL / MINIMUM_RR (BUG-20)
    
    # Scoring & Selection
    rank_key: tuple             # Methodology Sec 9.3 tuple
    zone_score_legacy: int      # 0-13 scale (Methodology Sec 9)
    zone_score_v38: int         # Base-10 scale (Methodology Sec 5.1 G10)
    nesting_tier: str           # TIER_1 / TIER_2 / TIER_3 / TIER_4 / NONE
    gap_composite_score: Optional[int] = None  # GDZ/GSZ only (Gap v2.4 Sec 4)

    # B-W-EMBED export geometry
    overlap_ratio: float = 0.0
    htf_target_price: Optional[float] = None
    struct_stop_A: Optional[float] = None
    struct_stop_E: Optional[float] = None
    
    # Context
    trend_regime: str = ""      # UP / SW / DN (E-TF resolved)
    quadrant: str = ""          # Q1 / Q2 / Q3
    trade_type: str = ""        # CONTINUATION / CONT_REDUCED / RANGE_EXTREME / REVERSAL_ONLY
    bias: str = ""              # LONG / SHORT / NEUTRAL
    entry_mode: str = "AUTO"    # AUTO / CONFIRM_PENDING
    
    # Selection metadata
    selection_reason: str = ""          # Why this zone was chosen
    candidates_count: int = 0           # How many GREEN zones were eligible
    proximity_pct: float = 0.0          # Distance from CMP as % of CMP
    
    # Gating
    gates_passed: List[str] = field(default_factory=list)
    dbr_required: bool = False
    rbd_required: bool = False
    pattern_validated: bool = False



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

#####################################################################################




class ZoneQualityScorer:
    """
    v4.4.1 Section 11: Zone Quality Scoring
    
    Score each zone 0-2 on each dimension. Prioritize total score + HTF alignment.
    
    Dimension          | Score 0          | Score 1              | Score 2
    -------------------|------------------|----------------------|------------------
    Base Quality       | No clear base    | Acceptable           | Clean; orderly
    Departure Strength | Weak             | Moderate             | Impulsive; full
    Freshness          | 3+ retests       | 1 retest             | Untested (fresh)
    HTF Alignment      | Against regime   | Neutral/unclear      | Aligned with HTF
    Distance Buffer    | < 1 ATR          | 1-2 ATR              | > 2 ATR
    
    Priority: Score >= 8 = High | 5-7 = Medium | < 5 = Low / Avoid
    """
    
    def __init__(self):
        self.cfg = Config()
    
    def score_base_quality(self, zone: Zone) -> int:
        """Score 0-2 for base quality."""
        if zone.base_len == 0:
            return 0  # No clear base
        elif zone.base_len == 1:
            return 1  # Single candle (acceptable)
        else:
            # BUG-BASE-LEN-TF: TF-aware max_base_len for scoring
            _bl_adj_sc = {TF.E: 1.5, TF.A: 1.25, TF.X: 1.0}.get(zone.tf, 1.0)
            _eff_mbl_sc = int(self.cfg.max_base_len * _bl_adj_sc + 0.5)
            if zone.base_len <= _eff_mbl_sc:
                return 2  # Clean, orderly base
            else:
                return 0  # Too many candles (messy)
    
    def score_departure_strength(self, zone: Zone) -> int:
        """Score 0-2 for departure strength."""
        if zone.departure_atr >= self.cfg.departure_atr_score2 and zone.body_pct >= self.cfg.departure_body_score2:
            return 2  # Impulsive, full removal
        elif zone.departure_atr >= self.cfg.departure_atr_score1 and zone.body_pct >= self.cfg.departure_body_score1:
            return 1  # Moderate, partial removal
        else:
            return 0  # Weak, no structure removal
    
    def score_freshness(self, zone: Zone) -> int:
        """Score 0-2 for freshness."""
        if zone.retest_count == 0:
            return 2  # Untested (fresh)
        elif zone.retest_count == 1:
            return 1  # One retest
        else:
            return 0  # 2+ retests
    
    def score_htf_alignment(self, zone: Zone, htf_regime: Optional[TrendRegime]) -> int:
        """Score 0-2 for HTF alignment."""
        if htf_regime is None:
            return 1  # Neutral (no HTF data)
        
        if zone.is_buy_zone:
            if htf_regime == TrendRegime.UP:
                return 2  # Aligned
            elif htf_regime == TrendRegime.SW:
                return 1  # Neutral
            else:  # DN
                return 0  # Against
        else:  # Sell zone
            if htf_regime == TrendRegime.DN:
                return 2  # Aligned
            elif htf_regime == TrendRegime.SW:
                return 1  # Neutral
            else:  # UP
                return 0  # Against
    
    def score_distance_buffer(self, zone: Zone, opposing_zone: Optional[Zone], atr_value: float) -> int:
        """Score 0-2 for distance buffer to opposing zone."""
        if opposing_zone is None or atr_value <= 0:
            return 2  # No opposing zone = maximum buffer
        
        distance = abs(opposing_zone.distal - zone.distal)
        atr_distance = distance / atr_value
        
        if atr_distance > self.cfg.distance_buffer_atr_high:
            return 2  # > 2 ATR buffer
        elif atr_distance >= self.cfg.distance_buffer_atr_low:
            return 1  # 1-2 ATR buffer
        else:
            return 0  # < 1 ATR buffer
    
    def calculate_score(self, zone: Zone, htf_regime: Optional[TrendRegime],
                        opposing_zone: Optional[Zone], atr_value: float,
                        ema_20_value: Optional[float] = None) -> int:
        """Calculate total quality score (0-10, +1 for EMA-20 confluence = max 11)."""
        score = 0
        score += self.score_base_quality(zone)
        score += self.score_departure_strength(zone)
        score += self.score_freshness(zone)
        score += self.score_htf_alignment(zone, htf_regime)
        score += self.score_distance_buffer(zone, opposing_zone, atr_value)
        # BUG-12 FIX: EMA-20 confluence (Methodology Sec 8, +1 scoring only, never hard gate)
        score += self.score_ema_confluence(zone, ema_20_value)
        return score
    
    def score_ema_confluence(self, zone: Zone, ema_20: Optional[float]) -> int:
        """
        BUG-12: EMA-20 confluence scoring (Methodology Sec 8).
        
        +1 if EMA-20 is within or near zone boundary (supports trade direction).
        BZ: EMA at or below proximal (price approaching from above into demand)
        SZ: EMA at or above proximal (price approaching from below into supply)
        BUG-37: Uses zone_height_base and distal_base for order-area boundary.
        Buffer: 20% of zone_height_base from proximal.
        """
        z_height = zone.zone_height_base
        if ema_20 is None or z_height == 0:
            return 0
        
        buffer = 0.20 * z_height
        d = zone.distal_base if zone.distal_base is not None else zone.distal
        
        if zone.is_buy_zone:
            # BZ confluence: EMA within ORDER AREA or just above proximal
            if d <= ema_20 <= zone.proximal:
                return 1  # EMA inside order area
            if zone.proximal < ema_20 <= zone.proximal + buffer:
                return 1  # EMA just above proximal (near zone)
        else:
            # SZ confluence: EMA within ORDER AREA or just below proximal
            if zone.proximal <= ema_20 <= d:
                return 1  # EMA inside order area
            if zone.proximal - buffer <= ema_20 < zone.proximal:
                return 1  # EMA just below proximal (near zone)
        
        return 0
    
    def classify_priority(self, score: int) -> str:
        """Classify zone priority based on score."""
        if score >= self.cfg.zone_quality_high:
            return "HIGH"
        elif score >= self.cfg.zone_quality_medium:
            return "MEDIUM"
        else:
            return "AVOID"
    
    def score_zone(self, zone: Zone, htf_regime: Optional[TrendRegime],
                   opposing_zone: Optional[Zone], atr_value: float,
                   ema_20_value: Optional[float] = None) -> None:
        """Score zone and update its attributes."""
        zone.raw_score = self.calculate_score(zone, htf_regime, opposing_zone, atr_value, ema_20_value)
        
        # BUG-14 FIX: Compute penetration_penalty from zone.penetration_pct
        if zone.penetration_pct > 0:
            if zone.penetration_pct <= 20:
                zone.penetration_penalty = 0
            elif zone.penetration_pct <= 50:
                zone.penetration_penalty = 1
            elif zone.penetration_pct <= 75:
                zone.penetration_penalty = 2
            else:
                zone.penetration_penalty = 3
        
        # BUG-13 NOTE: approach_penalty requires bar-by-bar approach speed analysis.
        # Proper implementation needs last N candles measuring ATR-per-bar rate into zone.
        # Deferred to v3.9 — requires CandleSeries in scorer (currently not passed).
        # zone.approach_penalty remains 0 for now.
        
        zone.final_score = max(0, zone.raw_score - zone.age_penalty - zone.approach_penalty - zone.penetration_penalty)
        zone.quality_priority = self.classify_priority(zone.final_score)



class RiskTargetCalculator:
    """
    v3.8.1: Entry/SL/Target calculator with topology-aware cascading SL.
    
    BUG-05 FIX: Entry = proximal ± 0.15% buffer
    BUG-06 FIX: SL = reference_distal ± 0.25 × ATR (configurable)
    BUG-07 FIX: Target = 0.75 × opposing.proximal (conservative) | 2.1 × risk (no opposing)
    BUG-22 FIX: Topology-aware cascading SL (E-zone → A-zone → X-zone distal)
    """
    
    def __init__(self, cfg: Config):
        self.cfg = cfg
    
    def get_zone_by_id(self, zones: List[Zone], zone_id: str) -> Optional[Zone]:
        """Return the first Zone whose zone_id matches, else None."""
        for z in zones:
            if z.zone_id == zone_id:
                return z
        return None

    def get_min_max_overlapping(self, overlapping_zones: list[Zone]) -> Tuple[Zone, Zone]:
        min_zone = min(overlapping_zones, key=lambda z: z.distal)
        max_zone = max(overlapping_zones, key=lambda z: z.proximal)
        return min_zone, max_zone
        

    def get_overlapping_ranges(self, zones: List[Zone]) -> List[Tuple[float, float]]:
        """
        Returns overlap ranges (low, high) based on distal/proximal.
        Example:
        [100-150], [125-200], [180-250] -> [(125,150), (180,200)]
        """
        intervals: List[Tuple[float, float]] = []
        for z in zones:
            lo = min(z.distal, z.proximal)
            hi = max(z.distal, z.proximal)
            intervals.append((lo, hi))

        overlaps: List[Tuple[float, float]] = []

        # pairwise intersections
        n = len(intervals)
        for i in range(n):
            a_lo, a_hi = intervals[i]
            for j in range(i + 1, n):
                b_lo, b_hi = intervals[j]
                lo = max(a_lo, b_lo)
                hi = min(a_hi, b_hi)
                if lo < hi:  # strict overlap; use <= if you want touching edges counted
                    overlaps.append((lo, hi))

        if not overlaps:
            return []

        # sort and merge duplicates/adjacent overlaps (keeps output clean)
        overlaps.sort()
        merged: List[Tuple[float, float]] = [overlaps[0]]
        for lo, hi in overlaps[1:]:
            prev_lo, prev_hi = merged[-1]
            if lo <= prev_hi:  # merge if overlapping/touching
                merged[-1] = (prev_lo, max(prev_hi, hi))
            else:
                merged.append((lo, hi))

        return merged
    
    def _compute_entry(self, zone: Zone, all_zones: List[Zone] = None) -> float:
        """BUG-05: Entry with ±0.15% buffer from proximal."""
        #####################################################
        # print(zone.ztype, zone.is_part_of_consecutive, zone.is_part_of_overlapping, "........................pppppp")
        if zone.is_part_of_consecutive:
            # print("consecutive", zone.consecutive_partner_id, "***********************************************************")
            partner_zone = self.get_zone_by_id(all_zones, zone.consecutive_partner_id)
            min_zone, max_zone = self.get_min_max_overlapping([zone, partner_zone])
            partner_has_enclosing = bool(partner_zone and (partner_zone.enclosing_e_zone or partner_zone.enclosing_a_zone))
            if partner_zone and partner_has_enclosing:
                return min_zone.proximal * (1 + self.cfg.entry_buffer_pct), [min_zone]
            else:
                return zone.proximal * (1 + self.cfg.entry_buffer_pct), [zone]
        #####################################################
        if zone.is_part_of_overlapping:
            # print("overlapping", zone.overlapping_partners_id, "***********************************************************")
            overlapping_zones = []
            overlapping_zones.append(zone)
            for partner_id in zone.overlapping_partners_id:
                partner_zone = self.get_zone_by_id(all_zones, partner_id)
                if partner_zone:
                    overlapping_zones.append(partner_zone)

            overlapping_ranges = self.get_overlapping_ranges(overlapping_zones)
            # min_zone, max_zone = self.get_min_max_overlapping(overlapping_zones)
            other_zones = [z for z in overlapping_zones if z != zone]
            if overlapping_ranges:
                if zone.is_buy_zone:
                    return max(max(overlapping_ranges)), other_zones
                else:
                    return min(min(overlapping_ranges)), other_zones
            else:
                return zone.distal, [zone]
                
        if zone.is_buy_zone:
            return zone.proximal * (1 + self.cfg.entry_buffer_pct), [zone]
        else:
            return zone.proximal * (1 - self.cfg.entry_buffer_pct), [zone]
    
    def _compute_stop(self, sl_reference_distal: float, atr_val: float, is_buy: bool) -> float:
        """BUG-06: SL with configurable ATR buffer beyond reference distal."""
        buffer = self.cfg.sl_buffer_atr * atr_val
        if is_buy:
            return sl_reference_distal - buffer
        else:
            return sl_reference_distal + buffer
    
    def _compute_target(self, entry: float, risk: float, opposing: Optional[Zone], 
                    opposing_htf_zone: Optional[Zone], is_buy: bool,
                    atr_val: float, cmp: Optional[float] = None,
                    htf_nested: bool = False, sits_on_top: bool = False) -> Tuple[float, str, Optional[float]]:
        """
        BUG-07:
        Conservative/aggressive structural target selection with
        consumed-zone handling and ATR fallback.

        Returns:
            (target_price, target_mode)
        """

        candidates: list[tuple[float, str]] = []

        def _resolve_zone_price(zone: Zone) -> Optional[float]:
            """
            Returns:
                valid target price from zone
                OR None if zone fully consumed/dead
            """

            # Fresh/tested zone
            if cmp is None:
                return zone.proximal

            if is_buy:
                # CMP has crossed proximal
                if cmp > zone.proximal:

                    # Fully consumed
                    if cmp > zone.distal:
                        return None

                    # Proximal consumed, distal still valid
                    return zone.distal

            else:
                # SELL
                if cmp < zone.proximal:

                    # Fully consumed
                    if cmp < zone.distal:
                        return None

                    # Proximal consumed, distal still valid
                    return zone.distal

            return zone.proximal

        # X-TF opposing zone
        if opposing is not None:
            resolved_price = _resolve_zone_price(opposing)

            if resolved_price is not None:
                candidates.append((resolved_price, "OPPOSING"))

        # HTF / E-A TF opposing zone
        if opposing_htf_zone is not None:
            resolved_price = _resolve_zone_price(opposing_htf_zone)

            if resolved_price is not None:
                candidates.append((resolved_price, "HTF"))

        # If every zone is consumed/dead -> ATR fallback
        if not candidates:
            if is_buy:
                return (
                    entry + self.cfg.default_target_atr * atr_val,
                    "ATR_FALLBACK", None
                )
            else:
                return (
                    entry - self.cfg.default_target_atr * atr_val,
                    "ATR_FALLBACK", None
                )

        # Select nearest valid structural target
        if is_buy:
            eligible = [(p, src) for (p, src) in candidates if p > entry]
            pool = eligible if eligible else candidates
            nearest_price, source = min(
                pool,
                key=lambda x: abs(x[0] - entry)
            )

        else:
            eligible = [(p, src) for (p, src) in candidates if p < entry]
            pool = eligible if eligible else candidates
            nearest_price, source = min(
                pool,
                key=lambda x: abs(x[0] - entry)
            )

        # HTF targets are more conservative
        # B-W-EMBED: TIER_3 (sits-on-top / partial overlap) has NO structural mandate to reach
        # the far HTF opposing zone. Force a conservative reference so we do not set unreachable
        # targets that produce give-back losers (price runs partway, reverses, hits SL).
        if source == "HTF" and sits_on_top:
            pct = getattr(self.cfg, "embed_sits_on_top_target_pct", 0.5)   # B-W-EMBED: config-driven (TUNE on full universe; see embed_tuning.md)
        elif source == "HTF" and htf_nested:
            pct = 1.0   # Full institutional target — nesting confirms HTF thesis
        elif source == "HTF":
            pct = 0.85  # HTF but not nested — discount for intermediate obstacles
        else:
            pct = 0.95  # X-TF opposing — nearby target

        if is_buy:
            target = entry + pct * abs(nearest_price - entry)
        else:
            target = entry - pct * abs(nearest_price - entry)

        mode = (
            "STRUCTURAL_CONSERVATIVE"
            if source == "HTF"
            else "STRUCTURAL_AGGRESSIVE"
        )
        htf_target_price = nearest_price if source == "HTF" else None

        return target, mode, htf_target_price
        # if opposing is not None:
        #     # print("opposing", opposing.proximal, opposing.distal, opposing.penetration_pct)
        #     if opposing_htf_zones:
        #         # STRUCTURAL target: 75% of distance to opposing proximal
        #         if is_buy:
        #             structural_target = entry + 0.75 * (opposing.distal - entry)
        #         else:
        #             structural_target = entry - 0.75 * (entry - opposing.distal)
        #         return structural_target, "STRUCTURAL_CONSERVATIVE"
        #     else:
        #         # STRUCTURAL_AGGRESSIVE: Only X-TF opposing exists, no HTF overhead
        #         # Target near opposing proximal with small buffer
        #         if is_buy:
        #             structural_target = entry + 0.80 * abs(opposing.proximal - entry)
        #         else:
        #             structural_target = entry - 0.80 * abs(opposing.proximal - entry)
        #         return structural_target, "STRUCTURAL_AGGRESSIVE"
                
        
    
    def _get_sl_reference_levels(self, all_zones, zones: List[Zone]) -> List[float]:
        """
        BUG-22: Get cascading SL reference distals based on zone topology.
        
        Returns list from widest (most protective) to tightest (zone's own distal).
        Cascade: try widest first, fall back if RR < min_rr.
        """
        levels = []
        
        # TIER_1 (nested in both E and A): E-zone → A-zone → X-zone
        for zone in zones:
            # if zone.nesting_tier:
                # print(zone.ztype, zone.distal, zone.proximal, zone.nesting_tier, zone.nesting_tier.value , zone.enclosing_e_zone, zone.enclosing_a_zone, "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv")
                # print(type(zone.nesting_tier.value), "tttttttttttttttttttttttttttttttttttttttttttttttttttttttt")
            levels.append(zone.distal)
            if zone.nesting_tier and zone.nesting_tier.value == 'TIER_1':  # TIER_1
                if zone.enclosing_e_zone is not None:
                    levels.append(zone.enclosing_e_zone.distal)
                    if len(zone.enclosing_e_zone.sl_levels) > 0:
                        levels.extend(zone.enclosing_e_zone.sl_levels)
                if zone.enclosing_a_zone is not None:
                    levels.append(zone.enclosing_a_zone.distal)
                    if len(zone.enclosing_a_zone.sl_levels) > 0:
                        levels.extend(zone.enclosing_a_zone.sl_levels)
            
            # B-W-EMBED: TIER_3 (sits-on-top / overlapping) — stop belongs at the structural
            # level it sits on (the overlapping A/E distal), NOT the tight X-distal. This fixes
            # wick-out losses on zones that were structurally right. RR only filters (Edit 3).
            elif zone.nesting_tier and zone.nesting_tier.value == 'TIER_3':  # TIER_3 sits-on-top
                if zone.enclosing_a_zone is not None:
                    levels.append(zone.enclosing_a_zone.distal)
                    if len(zone.enclosing_a_zone.sl_levels) > 0:
                        levels.extend(zone.enclosing_a_zone.sl_levels)
                elif zone.enclosing_e_zone is not None:
                    levels.append(zone.enclosing_e_zone.distal)
                    if len(zone.enclosing_e_zone.sl_levels) > 0:
                        levels.extend(zone.enclosing_e_zone.sl_levels)

            # TIER_2 (nested in E or A): HTF zone → X-zone
            elif zone.nesting_tier and zone.nesting_tier.value == 'TIER_2':  # TIER_2
                if zone.enclosing_e_zone is not None:
                    levels.append(zone.enclosing_e_zone.distal)
                    if len(zone.enclosing_e_zone.sl_levels) > 0:
                        levels.extend(zone.enclosing_e_zone.sl_levels)
                elif zone.enclosing_a_zone is not None:
                    levels.append(zone.enclosing_a_zone.distal)
                    if len(zone.enclosing_a_zone.sl_levels) > 0:
                        levels.extend(zone.enclosing_a_zone.sl_levels)
                    # print("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            # CONSECUTIVE STACK: outermost partner → own
            if zone.is_part_of_consecutive and zone.consecutive_partner_id:
                partner_zone = self.get_zone_by_id(all_zones, zone.consecutive_partner_id)
                min_zone, max_zone = self.get_min_max_overlapping([zone, partner_zone])
                levels.append(min_zone.distal)
                # Partner distal handled by caller (needs zone lookup)
                # pass  # Partner distal prepended by caller if available
            elif zone.is_part_of_overlapping and zone.overlapping_partners_id: 
                for partner_id in zone.overlapping_partners_id:
                    partner_zone = self.get_zone_by_id(all_zones, partner_id)
                    if partner_zone != zone: 
                        # print(partner_zone.distal)
                        levels.append(partner_zone.distal) 
            else:
                # Always include own distal as final fallback
                # print("ooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo")
                levels.append(zone.distal)
                # pass
        # Deduplicate while preserving order
        seen = set()
        unique = []
        for lvl in levels:
            if lvl not in seen:
                seen.add(lvl)
                unique.append(lvl)
        # print(unique, "uuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu")
        return unique
    
    def get_active_opposing_zone(self, zone: Zone, all_zones: List[Zone], cmp: float):
        if zone.is_buy_zone:
            candidates = [z for z in all_zones if z.is_sell_zone and not z.invalidated and z.penetration_pct <= 99.99]
            active_opposing = min(candidates, key=lambda z: abs(z.proximal - cmp), default=None)

        else:
            candidates = [z for z in all_zones if z.is_buy_zone and not z.invalidated and z.penetration_pct <= 99.99]
            active_opposing = min(candidates, key=lambda z: abs(z.proximal - cmp), default=None)

        return active_opposing

    def calculate(self, zone: Zone, cmp: float, all_zone_X: List[Zone], atr_val: float,
                  partner_distal: Optional[float] = None, active_zones: List[Zone] = None, htf_zones: List[Zone] = None) -> RiskTarget:
        """
        Full risk/target calculation with topology-aware cascading SL.
        
        Args:
            zone: The execution-TF zone
            opposing: Full opposing zone object (for proximal-based target)
            atr_val: ATR value at execution TF
            partner_distal: Distal of consecutive stack partner (if applicable)
        """

        is_buy = zone.is_buy_zone
        entry, sl_zones = self._compute_entry(zone, active_zones)
        # print(sl_zones)
        # Build cascading SL reference levels
        sl_levels = self._get_sl_reference_levels(all_zone_X, sl_zones)

        # Prepend consecutive partner distal if available
        if partner_distal is not None:
            sl_levels.insert(0, partner_distal)

        # print(partner_distal, "pppppppppppppppppppppppppppppppppppppppppppppppppppppppppp")
        if is_buy:
            sl_levels.sort()  # Ascending: lowest (widest) first
        else:
            sl_levels.sort(reverse=True)  # Descending: highest (widest) first
        
        # sl_levels.sort(reverse=True)
        # print(sl_levels, "ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss")
        # Cascade: try widest SL first, fall back if RR < min_rr
        best_result = None
        skipped_degenerate = 0
        htf_target_price = None

        for sl_ref in sl_levels:
            stop = self._compute_stop(sl_ref, atr_val, is_buy)
            risk = abs(entry - stop)
            
            if risk <= 0:
                continue
            
            if risk / entry < self.cfg.min_risk_pct:
                skipped_degenerate += 1
                continue

            opposing_zones = self.get_active_opposing_zone(zone, all_zone_X, cmp)
            opposing_htf_zones = self.get_active_opposing_zone(zone, htf_zones, cmp)
            _htf_nested = (zone.nesting_tier is not None and 
                           zone.nesting_tier in (ZoneNestingTier.TIER_1, ZoneNestingTier.TIER_2))
            # B-W-EMBED: TIER_3 = overlapping/sits-on-top (partial overlap below nesting threshold)
            _sits_on_top = (zone.nesting_tier is not None and 
                            zone.nesting_tier == ZoneNestingTier.TIER_3)
            target, target_mode, htf_target_price = self._compute_target(entry, risk, opposing_zones, opposing_htf_zones, is_buy, atr_val, cmp, _htf_nested, _sits_on_top)
            reward = abs(target - entry)
            rr = reward / risk

            # B-W-EMBED Edit 3 (config-gated, default OFF): for structurally-nested zones
            # (TIER_1/2/3), do NOT accept the zone's own tight X-distal as an RR-passing stop.
            # Structure defines the stop; RR only filters fire/no-fire. When enabled, a nested
            # zone whose structural (A/E) distal fails min_rr will FAIL rather than fall through
            # to a structurally-wrong tight stop. Ships OFF so the trade-count vs win-rate
            # trade-off can be measured on a full backtest before committing (pre-registered).
            _embed_strict = getattr(self.cfg, "embed_strict_stop", False)
            _structural_tier = zone.nesting_tier in (ZoneNestingTier.TIER_1, ZoneNestingTier.TIER_2, ZoneNestingTier.TIER_3) if zone.nesting_tier else False
            if _embed_strict and _structural_tier and abs(sl_ref - zone.distal) < 1e-9:
                # this sl_ref is the tight own-distal; skip in strict mode for nested zones
                continue
            # print(zone.ztype, zone.proximal, zone.distal, "=====", rr, entry, target, stop, target_mode, "rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr")
            if rr >= self.cfg.min_rr:
                # print("returning here ...................................")
                return RiskTarget(
                    entry=entry, stop=stop, target=target,
                    rr=round(rr, 2), valid=True, reason=None,
                    target_mode=target_mode, 
                    htf_target_price=htf_target_price
                )
            else:
                if best_result is None or rr > best_result.rr:
                    best_result = RiskTarget(
                        entry=entry, stop=stop, target=target,
                        rr=round(rr, 2), valid=False,
                        reason=f"RR {rr:.2f} < {self.cfg.min_rr} (SL ref: {sl_ref:.2f})",
                        target_mode=target_mode, 
                        htf_target_price=htf_target_price
                    )
        
        # All levels failed — return best (tightest) for diagnostics
        if best_result is not None:
            return best_result

        if zone.ztype in (ZoneType.BZ, getattr(ZoneType, "GDZ", ZoneType.BZ)):
            stop = entry - (atr_val * 2)
        else:
            stop = entry + (atr_val * 2)

        if skipped_degenerate > 0:
            return RiskTarget(
                entry=entry, stop=stop,
                target=entry, rr=0.0, valid=False,
                reason=f"RISK_TOO_SMALL (all {skipped_degenerate} SL refs produce risk < {self.cfg.min_risk_pct:.1%} of entry)",
                target_mode="ATR FALLBACK", 
                htf_target_price=htf_target_price
            )

        
        
        # Degenerate case: no valid SL levels
        return RiskTarget(
            entry=entry, stop=stop, target=entry,
            rr=0.0, valid=False, reason="NO_VALID_SL_REFERENCE",
            target_mode="UNKNOWN", htf_target_price=htf_target_price
        )



# ==============================================================================
# HARD GATE CHECKER
# ==============================================================================

# class HardGateChecker:
#     """
#     Hard Gates v3.8.1 (Must ALL pass for GREEN):
    
#     ┌──────┬────────────────────────────────────────────────────────────┬────────────┐
#     │ Gate │ What It Checks                                            │ TF         │
#     ├──────┼────────────────────────────────────────────────────────────┼────────────┤
#     │ G1   │ E regime permits trade direction                          │ E          │
#     │ G2   │ Analyze confirms E trend / doesn't conflict with E        │ E + A      │
#     │ G3   │ Execution Zone not violated by WICK                       │ X (Execute)│
#     │ G4   │ Execution Zone is FRESH (0 retest)                        │ X (Execute)│
#     │ G5   │ X zone nesting tier 1,2,3 (NOT standalone T4)             │ E + A + X  │
#     │ G6   │ No unviolated opposing HTF zone obstructs path to target  │ E + A      │
#     │      │ (OR target adjusted to nearest obstruction with RR met)   │            │
#     │ G7   │ Quadrant qualification (CMP within E+A range)             │ E + A      │
#     │ G8   │ Structure removal confirmed (P3 Legout)                   │ X (Execute)│
#     │ G9   │ RR >= 2.1 (after SL and target adjustments)               │ X (Execute)│
#     │ G10  │ Zone quality score >= minimum threshold (Base 10 scoring   │ E + A + X  │
#     │      │ with retest + penetration penalties) + base_len check     │            │
#     └──────┴────────────────────────────────────────────────────────────┴────────────┘
    
#     PROCESSING ORDER: G1 → G2 → G5 → G6 → G7 → G3 → G4 → G8 → G9 → G10
    
#     HTF gates first (cheapest, filter most), then X-TF gates.
#     G9 (RR) runs at Step 19 after weighted scoring and selection.
    
#     PHASE 1 (check_gates_pre_rr): G1, G2, G5, G6, G7, G3, G4, G8, G10
#     PHASE 2 (check_g9_rr): G9 only — runs at Step 19 on selected candidate
#     """
    
#     def __init__(self, cfg: Config):
#         self.cfg = cfg
    
#     def check_gates_pre_rr(self, zone: Zone,
#                            trend_context: TrendContext,
#                            tf: TF,
#                            cmp: float,
#                            nesting_tier: Optional[ZoneNestingTier] = None,
#                            obstruction_clear: bool = True,
#                            obstruction_zone: Optional[Zone] = None,
#                            quadrant: Optional[Quadrant] = None,
#                            zone_v38_score: Optional[int] = None,
#                            cmp_inside_htf_sz: bool = False,
#                            cmp_inside_htf_bz: bool = False
#                            ) -> Tuple[bool, Optional[str]]:
#         """
#         Phase 1: Gates G1, G2, G5, G6, G7, G3, G4, G8, G10.
#         Processing order follows screenshot: G1→G2→G5→G6→G7→G3→G4→G8→G10.
#         G9 (RR) deferred to Phase 2 at Step 19.
#         """
#         regime_E = trend_context.regime_E

#         if cmp > 0:
#             distance_pct = abs(cmp - zone.proximal) / cmp
#             if (zone.age_bars > self.cfg.compound_stale_age_bars
#                     and distance_pct > self.cfg.compound_stale_distance_pct):
#                 return False, (
#                     f"G11_COMPOUND_STALE "
#                     f"(age={zone.age_bars} bars > {self.cfg.compound_stale_age_bars}, "
#                     f"dist={distance_pct:.1%} > {self.cfg.compound_stale_distance_pct:.0%})"
#                 )
        
#         # Resolve trade type for this zone's direction
#         if zone.is_buy_zone:
#             trade_type_dir = trend_context.trade_type_long
#         else:
#             trade_type_dir = trend_context.trade_type_short
        
#         # ── G1: E regime permits trade direction (TF: E) ─────────────
#         # E aligned or neutral → pass. E opposes → check G2 for reversal.
#         if zone.is_buy_zone:
#             e_supports = (regime_E in (TrendRegime.UP, TrendRegime.SW))
#         else:
#             e_supports = (regime_E in (TrendRegime.DN, TrendRegime.SW))
        
#         if not e_supports:
#             # E opposes this direction. G2 might save via reversal path.
#             if trade_type_dir == TradeType.NO_TRADE:
#                 return False, "G1_E_REGIME_OPPOSES"
#             # else: REVERSAL_ONLY — G1 passes via reversal path (A saves)
        
#         # ── G2: Analyze confirms E / doesn't conflict (TF: E+A) ──────
#         # If E supports, A must not push combination to NO_TRADE.
#         if e_supports and trade_type_dir == TradeType.NO_TRADE:
#             return False, "G2_A_CONFLICTS_WITH_E"
        
#         # ── G5: Nesting tier 1/2/3, NOT standalone T4 (TF: E+A+X) ───
#         if nesting_tier is not None:
#             if nesting_tier == ZoneNestingTier.TIER_4:
#                 return False, "G5_NESTING_TIER_4_STANDALONE"
        
#         # ── G6: HTF obstruction OR target adjusted with RR met (TF: E+A) ─
#         # Also: CMP inside opposing HTF zone = hard block
#         if zone.is_buy_zone and cmp_inside_htf_sz:
#             return False, "G6_CMP_INSIDE_HTF_SZ"
#         if zone.is_sell_zone and cmp_inside_htf_bz:
#             return False, "G6_CMP_INSIDE_HTF_BZ"
        
#         if not obstruction_clear and obstruction_zone is not None:
#             # Try adjusting target to blocking zone's proximal
#             adjusted_target = obstruction_zone.proximal
#             # entry_approx = zone.proximal  # Entry ≈ proximal (± 0.15% buffer)
#             entry_approx = zone.entry if zone.entry else zone.proximal
#             stop = zone.stop_price if zone.stop_price else zone.distal
            
#             if zone.is_buy_zone:
#                 risk = abs(entry_approx - stop)
#                 reward = adjusted_target - entry_approx
#             else:
#                 risk = abs(stop - entry_approx)
#                 reward = entry_approx - adjusted_target
            
#             if risk > 0 and reward > 0:
#                 adjusted_rr = reward / risk
#                 if adjusted_rr >= self.cfg.min_rr:
#                     # Obstruction absorbed — adjust target and RR on zone
#                     zone.target_price = adjusted_target
#                     zone.rr_ratio = round(adjusted_rr, 4)
#                     zone.target_mode = "OBSTRUCTION_ADJUSTED"
#                     # G6 PASSES with adjusted target
#                 else:
#                     return False, (
#                         f"G6_HTF_OBSTRUCTION_RR_INSUFFICIENT "
#                         f"(adj_RR={adjusted_rr:.2f} < {self.cfg.min_rr}, "
#                         f"blocker={obstruction_zone.zone_id})"
#                     )
#             else:
#                 return False, (
#                     f"G6_HTF_OBSTRUCTION "
#                     f"(blocker={obstruction_zone.zone_id}, no valid adjustment)"
#                 )
#         elif not obstruction_clear:
#             return False, "G6_HTF_OBSTRUCTION"
        
#         # ── G7: Quadrant qualification (CMP within E+A range) (TF: E+A) ─
#         if quadrant is not None:
#             if trade_type_dir == TradeType.RANGE_EXTREME:
#                 if zone.is_buy_zone and quadrant != Quadrant.Q1:
#                     return False, f"G7_QUADRANT_NOT_Q1_FOR_RANGE_EXTREME ({quadrant})"
#                 if zone.is_sell_zone and quadrant != Quadrant.Q3:
#                     return False, f"G7_QUADRANT_NOT_Q3_FOR_RANGE_EXTREME ({quadrant})"
#             elif trade_type_dir == TradeType.REVERSAL_ONLY:
#                 if zone.is_buy_zone and quadrant != Quadrant.Q1:
#                     return False, f"G7_QUADRANT_NOT_Q1_FOR_REVERSAL ({quadrant})"
#                 if zone.is_sell_zone and quadrant != Quadrant.Q3:
#                     return False, f"G7_QUADRANT_NOT_Q3_FOR_REVERSAL ({quadrant})"
#             elif trade_type_dir == TradeType.CONT_REDUCED:
#                 if zone.is_buy_zone and quadrant == Quadrant.Q3:
#                     return False, "G7_QUADRANT_Q3_BLOCKS_CONT_REDUCED_LONG"
#                 if zone.is_sell_zone and quadrant == Quadrant.Q1:
#                     return False, "G7_QUADRANT_Q1_BLOCKS_CONT_REDUCED_SHORT"
        
#         # ── G3: Execution Zone not violated by WICK (TF: X) ──────────
#         if zone.invalidated:
#             return False, "G3_ZONE_VIOLATED_BY_WICK"
        
#         # ── G4: Execution Zone is FRESH — 0 retest (TF: X) ──────────
#         if tf == TF.X and zone.retest_count > self.cfg.max_retest_execute:
#             return False, f"G4_NOT_FRESH (retests={zone.retest_count})"
        
#         # ── G8: Structure removal confirmed — P3 Legout (TF: X) ─────
#         # if not zone.removes_structure:
#         #     return False, "G8_STRUCTURE_NOT_REMOVED"
        
#         # ── G10: Zone quality score + base_len (TF: E+A+X) ──────────
#         # BADZONE check folded in (was engine's old G2)
#         if zone.base_len == 0 or zone.base_len > self.cfg.max_base_len:
#             return False, f"G10_BADZONE (base_len={zone.base_len})"
#         if zone_v38_score is not None:
#             if zone_v38_score < self.cfg.min_zone_v38_score:
#                 return False, f"G10_ZONE_SCORE_LOW ({zone_v38_score} < {self.cfg.min_zone_v38_score})"
        
#         return True, None
    
#     def check_g9_rr(self, zone: Zone) -> Tuple[bool, Optional[str]]:
#         """
#         Phase 2: G9 — RR >= 2.1 (after SL and target adjustments).
#         Runs ONLY on selected candidate at Step 19.
#         If fails, caller cascades to next-ranked zone.
        
#         Note: zone.rr_ratio may have been adjusted by G6 (obstruction
#         target adjustment). G9 checks the final RR.
#         """
#         rr = zone.rr_ratio if zone.rr_ratio else 0.0
#         if zone.is_buy_zone:
#             if rr <= 1:
#                 return False, f"G9_RR_LOW ({rr:.2f} <= {self.cfg.min_rr})"
#         elif zone.is_sell_zone:
#             if rr <= 1:
#                 return False, f"G9_RR_LOW ({rr:.2f} <= 1.6)"
#         return True, None

class HardGateChecker:
    """
    Hard Gates v3.8.3 (Must ALL pass for GREEN):
    
    ┌──────┬────────────────────────────────────────────────────────────┬────────────┐
    │ Gate │ What It Checks                                            │ TF         │
    ├──────┼────────────────────────────────────────────────────────────┼────────────┤
    │ G11  │ Compound staleness: NOT (age > N bars AND dist > M%)      │ X (Execute)│
    │ G1   │ E regime permits trade direction                          │ E          │
    │ G2   │ Analyze confirms E trend / doesn't conflict with E        │ E + A      │
    │ G3   │ Execution Zone not violated by WICK                       │ X (Execute)│
    │ G4   │ Execution Zone is FRESH (0 retest)                        │ X (Execute)│
    │ G5   │ X zone nesting tier 1,2,3 (NOT standalone T4)             │ E + A + X  │
    │ G6   │ No unviolated opposing HTF zone obstructs path to target  │ E + A      │
    │      │ (OR target adjusted to nearest obstruction with RR met)   │            │
    │ G7   │ Quadrant qualification (CMP within E+A range)             │ E + A      │
    │ G8   │ Structure removal confirmed (P3 Legout)                   │ X (Execute)│
    │ G9   │ RR >= 2.1 (after SL and target adjustments)               │ X (Execute)│
    │ G10  │ Zone quality score >= minimum threshold (Base 10 scoring   │ E + A + X  │
    │      │ with retest + penetration penalties) + base_len check     │            │
    └──────┴────────────────────────────────────────────────────────────┴────────────┘
    
    PROCESSING ORDER: G11 → G1 → G2 → G5 → G6 → G7 → G3 → G4 → G8 → G9 → G10
    
    G11 runs first (cheapest check, eliminates ghost zones before expensive gates).
    HTF gates next (cheapest remaining, filter most), then X-TF gates.
    G9 (RR) runs at Step 19 after weighted scoring and selection.
    
    PHASE 1 (check_gates_pre_rr): G11, G1, G2, G5, G6, G7, G3, G4, G8, G10
    PHASE 2 (check_g9_rr): G9 only — runs at Step 19 on selected candidate
    """
    
    def __init__(self, cfg: Config):
        self.cfg = cfg
    
    def check_gates_pre_rr(self, zone: Zone,
                           trend_context: 'TrendContext',
                           tf: TF,
                           cmp: float,
                           nesting_tier: Optional['ZoneNestingTier'] = None,
                           obstruction_clear: bool = True,
                           obstruction_zone: Optional[Zone] = None,
                           quadrant: Optional[Quadrant] = None,
                           zone_v38_score: Optional[int] = None,
                           cmp_inside_htf_sz: bool = False,
                           cmp_inside_htf_bz: bool = False,
                           entry_path_clear: bool = True,
                           entry_blocking_zone: Optional[Zone] = None
                           ) -> Tuple[bool, Optional[str]]:
        """
        Phase 1: Gates G11, G1, G2, G5, G6, G7, G3, G4, G8, G10.
        Processing order: G11→G1→G2→G5→G6→G7→G3→G4→G8→G10.
        G9 (RR) deferred to Phase 2 at Step 19.
        
        v3.8.7 ARCHITECTURE: Gates split into HARD and SOFT.
        HARD gates (G3, G10): Zone is invalid — blocked outright.
        SOFT gates (G11, G1, G2, G5, G6, G7, G4, G8): Zone has reduced
        probability — passes with warning. SetupExtractor uses warnings
        for scoring. This prevents cascading hard-block from killing
        ALL zones in bearish/sideways markets.
        
        Args:
            cmp: Current Market Price — required for G11 compound staleness check.
        """
        regime_E = trend_context.regime_E
        
        # Accumulate soft gate warnings
        _soft_warnings = []
        
        # ── G11: Compound Staleness (TF-aware) — v3.8.3_withfix_v1.2 + v3.8.6 ──
        # Reject ghost zones that are BOTH very old AND very far from CMP.
        _g11_age_thresholds = {TF.E: 12, TF.A: 52, TF.X: self.cfg.compound_stale_age_bars}
        g11_age_threshold = _g11_age_thresholds.get(zone.tf, self.cfg.compound_stale_age_bars)
        if cmp > 0:
            distance_pct = abs(cmp - zone.proximal) / cmp
            if (zone.age_bars > g11_age_threshold
                    and distance_pct > self.cfg.compound_stale_distance_pct):
                _soft_warnings.append("G11_COMPOUND_STALE")
        
        # Resolve trade type for this zone's direction
        if zone.is_buy_zone:
            trade_type_dir = trend_context.trade_type_long
        else:
            trade_type_dir = trend_context.trade_type_short
        
        # ── G1: E regime permits trade direction (TF: E) — HARD for NO_TRADE ─
        if zone.is_buy_zone:
            e_supports = (regime_E in (TrendRegime.UP, TrendRegime.SW))
        else:
            e_supports = (regime_E in (TrendRegime.DN, TrendRegime.SW))
        
        if not e_supports:
            if trade_type_dir == TradeType.NO_TRADE:
                return False, "G1_E_REGIME_OPPOSES"
            # REVERSAL_ONLY passes G1 via reversal path
        
        # ── G2: Analyze confirms E / doesn't conflict — HARD for NO_TRADE ─
        if e_supports and trade_type_dir == TradeType.NO_TRADE:
            return False, "G2_A_CONFLICTS_WITH_E"
        
        # ── G5: Nesting (SOFT) ─────────────────────────────────────────
        if nesting_tier is not None:
            if nesting_tier == ZoneNestingTier.TIER_4:
                _soft_warnings.append("G5_TIER_4")
        
        # ── G6a: CMP inside opposing HTF zone (SOFT) ──────────────────
        if zone.is_buy_zone and cmp_inside_htf_sz:
            _soft_warnings.append("G6a_CMP_INSIDE_HTF_SZ")
        if zone.is_sell_zone and cmp_inside_htf_bz:
            _soft_warnings.append("G6a_CMP_INSIDE_HTF_BZ")
        
        # ── G6c: Entry path (SOFT) ────────────────────────────────────
        if not entry_path_clear and entry_blocking_zone is not None:
            _soft_warnings.append("G6c_ENTRY_PATH_BLOCKED")
        
        # ── G6b: HTF obstruction — adjusts target, SOFT if RR fails ──
        if not obstruction_clear and obstruction_zone is not None:
            # TARGET-MODEL fix: a live opposing zone in the path makes the setup
            # structurally inferior. Evidence (net-of-cost walk): OBSTRUCTION_ADJUSTED
            # bleeds in both shipping segments — NSEFO -0.16R (n=12), NSE cash -1.00R (n=2),
            # pooled -0.28R (n=14); other modes positive. Hard-reject when enabled.
            # (MCX showed +0.14R n=6 but is gated off; revisit per-segment if re-enabled.)
            if getattr(self.cfg, "reject_obstructed_targets", True):
                return False, "G6b_OBSTRUCTION_REJECTED"
            adjusted_target = obstruction_zone.proximal
            entry_approx = zone.entry_price if zone.entry_price else zone.proximal
            stop = zone.stop_price if zone.stop_price else zone.distal
            if zone.is_buy_zone:
                risk = entry_approx - stop
                reward = adjusted_target - entry_approx
            else:
                risk = stop - entry_approx
                reward = entry_approx - adjusted_target
            if risk > 0 and reward > 0:
                adjusted_rr = reward / risk
                if adjusted_rr >= self.cfg.min_rr:
                    zone.target_price = adjusted_target
                    zone.rr_ratio = round(adjusted_rr, 4)
                    zone.target_mode = "OBSTRUCTION_ADJUSTED"
                else:
                    _soft_warnings.append("G6b_OBSTRUCTION_RR_LOW")
            else:
                _soft_warnings.append("G6b_OBSTRUCTION_NO_ADJUSTMENT")
        
        # ── G7: Quadrant qualification (SOFT) ─────────────────────────
        if quadrant is not None:
            if trade_type_dir == TradeType.RANGE_EXTREME:
                if zone.is_buy_zone and quadrant != Quadrant.Q1:
                    _soft_warnings.append(f"G7_NOT_Q1")
                if zone.is_sell_zone and quadrant != Quadrant.Q3:
                    _soft_warnings.append(f"G7_NOT_Q3")
            elif trade_type_dir == TradeType.REVERSAL_ONLY:
                if zone.is_buy_zone and quadrant != Quadrant.Q1:
                    _soft_warnings.append(f"G7_NOT_Q1")
                if zone.is_sell_zone and quadrant != Quadrant.Q3:
                    _soft_warnings.append(f"G7_NOT_Q3")
            elif trade_type_dir == TradeType.CONT_REDUCED:
                if zone.is_buy_zone and quadrant == Quadrant.Q3:
                    _soft_warnings.append("G7_Q3_BLOCKS_CONT")
                if zone.is_sell_zone and quadrant == Quadrant.Q1:
                    _soft_warnings.append("G7_Q1_BLOCKS_CONT")
        
        # ── G3: Zone not violated — HARD BLOCK ───────────────────────
        if zone.invalidated:
            return False, "G3_ZONE_VIOLATED_BY_WICK"
        
        # G3 soft: gap zones with 1 wick touch survived violation but
        # carry reduced probability. Add warning to push toward AMBER.
        if (zone.gap_wick_touch_count > 0
                and zone.ztype in (ZoneType.GDZ, ZoneType.GSZ)):
            _soft_warnings.append(
                f"G3_GAP_WICK_TOUCHED ({zone.gap_wick_touch_count}x, "
                f"{zone.gap_wick_max_depth_pct:.0f}% depth)"
            )
        
        # ── G4: Execution Zone fresh (SOFT) ───────────────────────────
        if tf == TF.X and zone.retest_count > self.cfg.max_retest_execute:
            _soft_warnings.append(f"G4_NOT_FRESH")
        
        # ── G8: Structure removal (SOFT) ──────────────────────────────
        if not zone.removes_structure:
            _soft_warnings.append("G8_STRUCTURE_NOT_REMOVED")
        
        # ── G10: Zone quality + base_len — HARD BLOCK for malformed ──
        # BADZONE check folded in (was engine's old G2)
        # BUG-BASE-LEN-TF: TF-aware max_base_len (same as detect())
        _bl_adj_g10 = {TF.E: 1.5, TF.A: 1.25, TF.X: 1.0}.get(zone.tf, 1.0)
        # v3.8.7: Gap zones (GDZ/GSZ) inherently have longer bases because
        # Fix B+C extends boundaries to include basing structure. Allow +2
        # for gap zones to accommodate pre-gap basing + gap pair.
        _is_gap_zone = zone.ztype in (ZoneType.GDZ, ZoneType.GSZ)
        _gap_adj = 2 if _is_gap_zone else 0
        _eff_mbl_g10 = int(self.cfg.max_base_len * _bl_adj_g10 + 0.5) + _gap_adj
        if zone.base_len == 0 or zone.base_len > _eff_mbl_g10:
            return False, f"G10_BADZONE (base_len={zone.base_len})"
        if zone_v38_score is not None:
            if zone_v38_score < self.cfg.min_zone_v38_score:
                _soft_warnings.append("G10_SCORE_LOW")
        
        # Store soft warnings on zone for SetupExtractor ranking
        zone.gate_warnings = _soft_warnings
        
        # ── COMBINED HARD BLOCK: No institutional proof ──────────────
        # A TIER_4 zone (no HTF nesting) WITH no structure removal (G8)
        # has ZERO institutional confirmation. The zone might be noise.
        # Either nesting OR structure removal must exist for the zone
        # to be considered institutional.
        _is_t4 = "G5_TIER_4" in _soft_warnings
        _no_struct = "G8_STRUCTURE_NOT_REMOVED" in _soft_warnings
        if _is_t4 and _no_struct:
            return False, "NO_INSTITUTIONAL_PROOF (TIER_4 + no structure removal)"
        
        return True, None
    
    def check_g9_rr(self, zone: Zone) -> Tuple[bool, Optional[str]]:
        """
        Phase 2: G9 — RR >= 2.1 (after SL and target adjustments).
        Runs ONLY on selected candidate at Step 19.
        If fails, caller cascades to next-ranked zone.
        
        Note: zone.rr_ratio may have been adjusted by G6 (obstruction
        target adjustment). G9 checks the final RR.
        """
        rr = zone.rr_ratio if zone.rr_ratio else 0.0
        if rr < self.cfg.min_rr:
            return False, f"G9_RR_LOW ({rr:.2f} < {self.cfg.min_rr})"
        return True, None



class DBRRBRValidator:
    """
    v4.4 Section 10: DBR/RBR Gating Requirements
    
    DBR (Drop-Base-Rally) for longs when trade opposes Eval regime
    RBR (Rally-Base-Drop) for shorts when trade opposes Eval regime
    
    STRICT RULE: Without confirmed DBR/RBR meeting ALL criteria,
    REVERSAL trades are NOT permitted.
    """
    
    def __init__(self, cfg: Config):
        self.cfg = cfg
    
    def validate_dbr(self, cs: CandleSeries, zone: Zone, 
                     htf_bz_present: bool, quadrant: Optional[Quadrant]) -> ReversalPattern:
        """
        v4.4 Section 10.2: DBR Validation Criteria
        
        Criterion          | Requirement
        -------------------|----------------------------
        Initial Move       | Clear drop into demand area
        Base Formation     | 2-3+ candles of consolidation/absorption
        Departure          | Impulsive rally away from base
        Structure Shift    | Must break a Lower High or swing point
        HTF Alignment      | HTF BZ must be present below/at pattern
        Quadrant           | Pattern must form in Q1 (discount)
        """
        pattern = ReversalPattern(
            pattern_type=PatternType.DBR,
            initial_move_start_idx=0,
            initial_move_end_idx=0,
            base_start_idx=zone.base_start,
            base_end_idx=zone.base_end,
            departure_idx=zone.departure_idx or zone.base_end + 1,
            zone=zone
        )
        
        failures = []
        
        # Criterion 1: Initial Move (clear drop into demand)
        if zone.base_start > 0:
            drop_range = cs.h[zone.base_start - 1] - cs.l[zone.base_start]
            if drop_range > 0:
                pattern.has_initial_move = True
            else:
                failures.append("No clear drop into demand area")
        else:
            failures.append("Cannot verify initial move (zone at start)")
        
        # Criterion 2: Base Formation (2-3+ candles)
        base_len = zone.base_end - zone.base_start + 1
        if self.cfg.dbr_rbr_min_base_candles <= base_len <= self.cfg.dbr_rbr_max_base_candles:
            pattern.has_base_formation = True
        else:
            failures.append(f"Base length {base_len} not in range [{self.cfg.dbr_rbr_min_base_candles}, {self.cfg.dbr_rbr_max_base_candles}]")
        
        # Criterion 3: Departure (impulsive rally)
        if zone.departure_idx and zone.departure_atr >= self.cfg.departure_atr_score1:
            pattern.has_departure = True
        else:
            failures.append("Departure not impulsive enough")
        
        # Criterion 4: Structure Shift (must break a Lower High)
        if zone.departure_idx and zone.departure_idx < cs.n:
            # Look for lower high before zone
            lh_idx = None
            for i in range(max(0, zone.base_start - 10), zone.base_start):
                if i > 0 and cs.h[i] < cs.h[i-1] and (i + 1 >= cs.n or cs.h[i] < cs.h[i+1]):
                    lh_idx = i
            
            # Check if departure broke the lower high
            if lh_idx and cs.h[zone.departure_idx] > cs.h[lh_idx]:
                pattern.has_structure_shift = True
            else:
                failures.append("No Lower High breach detected")
        else:
            failures.append("Cannot verify structure shift")
        
        # Criterion 5: HTF Alignment (HTF BZ must be present)
        if htf_bz_present:
            pattern.has_htf_alignment = True
        else:
            failures.append("HTF BZ not present")
        
        # Criterion 6: Quadrant (must be Q1)
        if quadrant == Quadrant.Q1:
            pattern.correct_quadrant = True
        else:
            failures.append(f"Quadrant is {quadrant}, must be Q1")
        
        # Overall validation
        pattern.validation_failures = failures
        pattern.is_valid = (
            pattern.has_initial_move and
            pattern.has_base_formation and
            pattern.has_departure and
            pattern.has_structure_shift and
            pattern.has_htf_alignment and
            pattern.correct_quadrant
        )
        
        return pattern
    
    def validate_rbr(self, cs: CandleSeries, zone: Zone,
                     htf_sz_present: bool, quadrant: Optional[Quadrant]) -> ReversalPattern:
        """
        v4.4 Section 10.2: RBR Validation Criteria
        
        Mirror of DBR for short positions.
        """
        pattern = ReversalPattern(
            pattern_type=PatternType.RBR,
            initial_move_start_idx=0,
            initial_move_end_idx=0,
            base_start_idx=zone.base_start,
            base_end_idx=zone.base_end,
            departure_idx=zone.departure_idx or zone.base_end + 1,
            zone=zone
        )
        
        failures = []
        
        # Criterion 1: Initial Move (clear rally into supply)
        if zone.base_start > 0:
            rally_range = cs.h[zone.base_start] - cs.l[zone.base_start - 1]
            if rally_range > 0:
                pattern.has_initial_move = True
            else:
                failures.append("No clear rally into supply area")
        else:
            failures.append("Cannot verify initial move (zone at start)")
        
        # Criterion 2: Base Formation
        base_len = zone.base_end - zone.base_start + 1
        if self.cfg.dbr_rbr_min_base_candles <= base_len <= self.cfg.dbr_rbr_max_base_candles:
            pattern.has_base_formation = True
        else:
            failures.append(f"Base length {base_len} not in range")
        
        # Criterion 3: Departure (impulsive drop)
        if zone.departure_idx and zone.departure_atr >= self.cfg.departure_atr_score1:
            pattern.has_departure = True
        else:
            failures.append("Departure not impulsive enough")
        
        # Criterion 4: Structure Shift (must break a Higher Low)
        if zone.departure_idx and zone.departure_idx < cs.n:
            hl_idx = None
            for i in range(max(0, zone.base_start - 10), zone.base_start):
                if i > 0 and cs.l[i] > cs.l[i-1] and (i + 1 >= cs.n or cs.l[i] > cs.l[i+1]):
                    hl_idx = i
            
            if hl_idx and cs.l[zone.departure_idx] < cs.l[hl_idx]:
                pattern.has_structure_shift = True
            else:
                failures.append("No Higher Low breach detected")
        else:
            failures.append("Cannot verify structure shift")
        
        # Criterion 5: HTF Alignment (HTF SZ must be present)
        if htf_sz_present:
            pattern.has_htf_alignment = True
        else:
            failures.append("HTF SZ not present")
        
        # Criterion 6: Quadrant (must be Q3)
        if quadrant == Quadrant.Q3:
            pattern.correct_quadrant = True
        else:
            failures.append(f"Quadrant is {quadrant}, must be Q3")
        
        pattern.validation_failures = failures
        pattern.is_valid = (
            pattern.has_initial_move and
            pattern.has_base_formation and
            pattern.has_departure and
            pattern.has_structure_shift and
            pattern.has_htf_alignment and
            pattern.correct_quadrant
        )
        
        return pattern


class ZoneAgeManager:
    """
    v4.4 Section 13.2: Zone Age Classification
    
    Age Classes:
    - FRESH: < 50 bars since creation, no penalty
    - ACTIVE: 50-200 bars + CMP within 5 ATR, -1 penalty
    - STALE: > 200 bars OR CMP > 10 ATR away, -2 penalty
    - REACTIVATED: Was STALE but CMP returned to zone, reset to -1
    """
    
    def __init__(self, cfg: Config):
        self.cfg = cfg
    
    def classify_age(self, zone: Zone, current_idx: int, cmp: float, atr_value: float) -> str:
        """Classify zone age and return age class."""
        bars_since_creation = current_idx - zone.created_idx
        zone.age_bars = bars_since_creation
        
        # Calculate ATR distance
        # atr_distance = abs(zone.distal - cmp) / atr_value if atr_value > 0 else 0
        d = zone.distal_base if zone.distal_base is not None else zone.distal
        atr_distance = abs(d - cmp) / atr_value if atr_value > 0 else 0
        # Check FRESH
        if bars_since_creation < self.cfg.zone_age_fresh_bars:
            return "FRESH"
        
        # Check STALE (by bars or ATR distance)
        if bars_since_creation > self.cfg.zone_age_active_bars:
            # Check for reactivation
            if self._is_cmp_in_zone(zone, cmp):
                return "REACTIVATED"
            return "STALE"
        
        if atr_distance > self.cfg.zone_age_stale_atr:
            # Check for reactivation
            if self._is_cmp_in_zone(zone, cmp):
                return "REACTIVATED"
            return "STALE"
        
        # Check ACTIVE (within 5 ATR)
        if atr_distance <= self.cfg.zone_age_active_atr:
            return "ACTIVE"
        
        return "STALE"
    
    def _is_cmp_in_zone(self, zone: Zone, cmp: float) -> bool:
        """Check if CMP is within zone boundaries (for reactivation)."""
        return zone.low_edge_base <= cmp <= zone.high_edge_base
    
    def get_age_penalty(self, age_class: str) -> int:
        """Get scoring penalty for age class."""
        penalties = {
            "FRESH": self.cfg.zone_age_fresh_penalty,
            "ACTIVE": self.cfg.zone_age_active_penalty,
            "STALE": self.cfg.zone_age_stale_penalty,
            "REACTIVATED": self.cfg.zone_age_active_penalty  # Reset to ACTIVE penalty
        }
        return penalties.get(age_class, 0)
    
    def update_zone_age(self, zone: Zone, current_idx: int, cmp: float, atr_value: float) -> None:
        """Update zone's age classification and penalty."""
        new_age_class = self.classify_age(zone, current_idx, cmp, atr_value)
        
        # Track reactivation
        if zone.age_class == "STALE" and new_age_class == "REACTIVATED":
            zone.reactivation_count += 1
        
        zone.age_class = new_age_class
        zone.age_penalty = abs(self.get_age_penalty(new_age_class))


class GapZoneIntegrator:
    """
    v4.4 Section 14: Gap Zone Integration
    
    - GDZ = BZ equivalent for Rule C
    - GSZ = SZ equivalent for Rule C
    - Session acceptance required before Rule C inclusion
    """
    
    def __init__(self, cfg: Config):
        self.cfg = cfg
    
    def is_valid_for_rule_c(self, zone: Zone) -> bool:
        """
        Check if gap zone is valid for Rule C trend calculation.
        
        Gap zones require session_accepted == TRUE before counting.
        """
        if zone.ztype not in (ZoneType.GDZ, ZoneType.GSZ):
            return True  # Regular zones always valid
        
        return zone.session_accepted
    
    def get_equivalent_type(self, zone: Zone) -> str:
        """Get equivalent zone type for Rule C."""
        if zone.ztype == ZoneType.GDZ:
            return "BZ"
        elif zone.ztype == ZoneType.GSZ:
            return "SZ"
        elif zone.ztype == ZoneType.BZ:
            return "BZ"
        else:
            return "SZ"
    
    def score_gap_size(self, gap_atr: float) -> int:
        """
        Score gap size (utility for external callers).
        Gap v2.3 Sec 4 — Dimension 2.
        
        NOTE: Internal pipeline uses GapModule._score_composite() which
        scores all 6 dimensions. This method is retained as a public utility
        for callers that need individual dimension scoring.
        
        Score 0: < 0.5x ATR
        Score 1: 0.5 - 0.75x ATR
        Score 2: > 0.75x ATR
        """
        if gap_atr > self.cfg.gap_score_size_high_atr:
            return 2
        elif gap_atr >= self.cfg.gap_score_size_medium_atr:
            return 1
        else:
            return 0


# ==============================================================================
# NEW v3.4: WICK VIOLATION HANDLER (v4.4 Section 15.2)
# ==============================================================================

class WickViolationHandler:
    """
    v4.4 Section 15.2: Wick Violation Handling
    
    Context-dependent treatment:
    - Trend Calculation: CLOSE required, wick = early warning only
    - Zone Status: Wick beyond distal = TESTED state, -1 penalty
    - HTF Veto: Wick = increased caution, NOT full veto
    - Failed Violation: Wick + rejection = +15% reversal probability
    """
    
    def __init__(self, cfg: Config):
        self.cfg = cfg
    
    def detect_wick_violation(self, cs: CandleSeries, zone: Zone) -> bool:
        """Detect if zone has wick violation (secondary violation)."""
        i = cs.n - 1
        if zone.is_buy_zone:
            # Wick below distal but close inside
            if cs.l[i] < zone.distal and cs.c[i] >= zone.distal:
                return True
        else:
            # Wick above distal but close inside
            if cs.h[i] > zone.distal and cs.c[i] <= zone.distal:
                return True
        return False
    
    def detect_failed_violation(self, cs: CandleSeries, zone: Zone) -> bool:
        """
        Detect failed violation (wick beyond distal + rejection back).
        
        Returns True if zone shows wick violation followed by rejection.
        """
        if cs.n < 2:
            return False
        
        i = cs.n - 1
        prev_i = cs.n - 2
        
        if zone.is_buy_zone:
            # Previous bar wicked below, current bar closed above proximal (rejection)
            if cs.l[prev_i] < zone.distal and cs.c[i] > zone.proximal:
                return True
        else:
            # Previous bar wicked above, current bar closed below proximal (rejection)
            if cs.h[prev_i] > zone.distal and cs.c[i] < zone.proximal:
                return True
        
        return False
    
    def update_wick_status(self, cs: CandleSeries, zone: Zone) -> None:
        """Update zone's wick violation status and reversal probability."""
        zone.wick_violation_detected = self.detect_wick_violation(cs, zone)
        
        if self.detect_failed_violation(cs, zone):
            zone.reversal_probability_boost = self.cfg.wick_violation_reversal_probability_boost
        else:
            zone.reversal_probability_boost = 0.0


# ==============================================================================
# NEW v3.4: THREE-WAY CONFLICT RESOLVER (v4.4 Section 15.1)
# ==============================================================================

class ThreeWayConflictResolver:
    """
    v4.4 Section 15.1: Three-Way Conflict Resolution
    
    When E, A, X all show different regimes:
    - E dominates when directional (UP or DN)
    - A becomes tiebreaker when E = SW
    - X NEVER overrides E or A for regime determination
    """
    
    def resolve(self, regime_E: TrendRegime, regime_A: TrendRegime, 
                regime_X: TrendRegime) -> Dict:
        """
        Resolve three-way conflict and return resolution.
        
        Returns dict with:
        - primary_regime: The dominant regime
        - trade_bias: Direction bias for trading
        - allow_long, allow_short: Permissions
        - resolution_reason: Explanation
        """
        # Check if all three differ
        regimes = {regime_E, regime_A, regime_X}
        all_different = len(regimes) == 3
        
        if not all_different:
            # Standard two-way resolution (delegate to existing logic)
            return {
                'resolved': False,
                'reason': "Not a three-way conflict"
            }
        
        # E dominates when directional
        if regime_E == TrendRegime.UP:
            return {
                'resolved': True,
                'primary_regime': TrendRegime.UP,
                'trade_bias': 'Bullish (E dominates)',
                'allow_long': True,
                'allow_short': False,
                'trade_type_long': TradeType.CONT_REDUCED,
                'trade_type_short': TradeType.NO_TRADE,
                'resolution_reason': f"E={regime_E} dominates; A={regime_A} reduces confidence; X={regime_X} ignored"
            }
        
        if regime_E == TrendRegime.DN:
            return {
                'resolved': True,
                'primary_regime': TrendRegime.DN,
                'trade_bias': 'Bearish (E dominates)',
                'allow_long': False,
                'allow_short': True,
                'trade_type_long': TradeType.NO_TRADE,
                'trade_type_short': TradeType.CONT_REDUCED,
                'resolution_reason': f"E={regime_E} dominates; A={regime_A} reduces confidence; X={regime_X} ignored"
            }
        
        # E = SW: A becomes tiebreaker
        if regime_E == TrendRegime.SW:
            if regime_A == TrendRegime.UP:
                return {
                    'resolved': True,
                    'primary_regime': TrendRegime.UP,
                    'trade_bias': 'Bullish (A tiebreaker)',
                    'allow_long': True,
                    'allow_short': False,
                    'trade_type_long': TradeType.CONT_REDUCED,
                    'trade_type_short': TradeType.NO_TRADE,
                    'resolution_reason': f"E={regime_E}; A={regime_A} becomes tiebreaker; X={regime_X} ignored"
                }
            else:  # regime_A == DN
                return {
                    'resolved': True,
                    'primary_regime': TrendRegime.DN,
                    'trade_bias': 'Bearish (A tiebreaker)',
                    'allow_long': False,
                    'allow_short': True,
                    'trade_type_long': TradeType.NO_TRADE,
                    'trade_type_short': TradeType.CONT_REDUCED,
                    'resolution_reason': f"E={regime_E}; A={regime_A} becomes tiebreaker; X={regime_X} ignored"
                }
        
        return {
            'resolved': False,
            'reason': "Unexpected regime combination"
        }


class ZoneNestingClassifier:
    """
    v3.8: Classify X zones based on nesting within HTF zones.
    
    Higher tier = Higher probability setup.
    """
    
    def __init__(self, overlap_threshold: float = 0.5):
        # B-W-EMBED: overlap_threshold is the TIER_2(nested) vs TIER_3(sits-on-top) boundary.
        # Caller should pass Config.embed_overlap_threshold. TUNE on full universe (see embed_tuning.md).
        self.overlap_threshold = overlap_threshold  # provisional 0.5 — NOT validated
    
    def _zones_overlap(self, z1: Zone, z2: Zone) -> bool:
        """Check if two zones overlap at all."""
        if z1.is_buy_zone != z2.is_buy_zone:
            return False  # Must be same direction
        
        # Get range of each zone
        z1_min = min(z1.proximal, z1.distal)
        z1_max = max(z1.proximal, z1.distal)
        z2_min = min(z2.proximal, z2.distal)
        z2_max = max(z2.proximal, z2.distal)
        
        # Check overlap
        return z1_min <= z2_max and z2_min <= z1_max
    
    def _calculate_overlap_pct(self, x_zone: Zone, htf_zone: Zone) -> float:
        """Calculate what percentage of X zone falls within HTF zone."""
        if x_zone.zone_height == 0:
            return 0.0
        
        x_min = min(x_zone.proximal, x_zone.distal)
        x_max = max(x_zone.proximal, x_zone.distal)
        htf_min = min(htf_zone.proximal, htf_zone.distal)
        htf_max = max(htf_zone.proximal, htf_zone.distal)
        
        # Calculate overlap range
        overlap_min = max(x_min, htf_min)
        overlap_max = min(x_max, htf_max)
        
        if overlap_max <= overlap_min:
            return 0.0
        
        overlap_size = overlap_max - overlap_min
        return overlap_size / x_zone.zone_height
    
    def _is_nested(self, x_zone: Zone, htf_zone: Zone) -> bool:
        """Check if X zone is nested within HTF zone (>threshold overlap)."""
        if x_zone.is_buy_zone != htf_zone.is_buy_zone:
            return False
        
        overlap_pct = self._calculate_overlap_pct(x_zone, htf_zone)
        return overlap_pct >= self.overlap_threshold

    
    def classify(self, x_zone: Zone, a_zones: List[Zone], e_zones: List[Zone]) -> ZoneNestingTier:
        """
        Classify X zone based on nesting within HTF zones.
        
        Args:
            x_zone: The Execute TF zone to classify
            a_zones: List of Analyze TF zones
            e_zones: List of Evaluate TF zones
        
        Returns:
            ZoneNestingTier indicating probability level
        """
        tier, _ = self.classify_with_debug(x_zone, a_zones, e_zones)
        return tier

    def classify_with_debug(self, x_zone: Zone, a_zones: List[Zone],
                            e_zones: List[Zone]) -> Tuple[ZoneNestingTier, dict]:
        """
        Classify X zone with detailed diagnostic output.
        
        Returns:
            (tier, debug_info) where debug_info contains:
            - e_zones_checked: count of non-invalidated E zones checked
            - a_zones_checked: count of non-invalidated A zones checked
            - e_same_dir: count of same-direction E zones
            - a_same_dir: count of same-direction A zones
            - best_e_overlap: highest overlap % with any E zone (same dir)
            - best_a_overlap: highest overlap % with any A zone (same dir)
            - best_e_overlap_any_dir: highest overlap % ignoring direction
            - best_a_overlap_any_dir: highest overlap % ignoring direction
            - reason: human-readable explanation of tier
        """
        debug = {
            'e_zones_total': len(e_zones),
            'a_zones_total': len(a_zones),
            'e_zones_checked': 0,
            'a_zones_checked': 0,
            'e_same_dir': 0,
            'a_same_dir': 0,
            'best_e_overlap': 0.0,
            'best_a_overlap': 0.0,
            'best_e_overlap_any_dir': 0.0,
            'best_a_overlap_any_dir': 0.0,
            'reason': '',
        }
        
        # Check E zones
        for ez in e_zones:
            if ez.invalidated:
                continue
            debug['e_zones_checked'] += 1
            
            # Overlap ignoring direction (diagnostic only)
            overlap_any = self._calculate_overlap_pct(x_zone, ez)
            debug['best_e_overlap_any_dir'] = max(
                debug['best_e_overlap_any_dir'], overlap_any
            )
            
            # Same-direction check
            if x_zone.is_buy_zone == ez.is_buy_zone:
                debug['e_same_dir'] += 1
                overlap = self._calculate_overlap_pct(x_zone, ez)
                debug['best_e_overlap'] = max(debug['best_e_overlap'], overlap)
        
        # Check A zones
        for az in a_zones:
            if az.invalidated:
                continue
            debug['a_zones_checked'] += 1
            
            # Overlap ignoring direction (diagnostic only)
            overlap_any = self._calculate_overlap_pct(x_zone, az)
            debug['best_a_overlap_any_dir'] = max(
                debug['best_a_overlap_any_dir'], overlap_any
            )
            
            # Same-direction check
            if x_zone.is_buy_zone == az.is_buy_zone:
                debug['a_same_dir'] += 1
                overlap = self._calculate_overlap_pct(x_zone, az)
                debug['best_a_overlap'] = max(debug['best_a_overlap'], overlap)
        
        # Determine nesting
        nested_in_e = debug['best_e_overlap'] >= self.overlap_threshold
        nested_in_a = debug['best_a_overlap'] >= self.overlap_threshold
        
        if nested_in_e and nested_in_a:
            debug['reason'] = (
                f"TIER_1: nested in E ({debug['best_e_overlap']:.0%}) "
                f"and A ({debug['best_a_overlap']:.0%})"
            )
            return ZoneNestingTier.TIER_1, debug
        elif nested_in_e or nested_in_a:
            which = "E" if nested_in_e else "A"
            pct = debug['best_e_overlap'] if nested_in_e else debug['best_a_overlap']
            debug['reason'] = f"TIER_2: nested in {which} ({pct:.0%})"
            return ZoneNestingTier.TIER_2, debug
        
        # Check overlap (not nested but touching) — same direction
        overlaps_e = any(
            self._zones_overlap(x_zone, ez) 
            for ez in e_zones 
            if not ez.invalidated
        )
        overlaps_a = any(
            self._zones_overlap(x_zone, az) 
            for az in a_zones 
            if not az.invalidated
        )
        
        if overlaps_e or overlaps_a:
            debug['reason'] = f"TIER_3: overlaps {'E' if overlaps_e else ''}{'A' if overlaps_a else ''}"
            return ZoneNestingTier.TIER_3, debug
        
        # Build diagnostic reason for TIER_4
        reasons = []
        if debug['e_same_dir'] == 0 and debug['a_same_dir'] == 0:
            reasons.append("no same-direction HTF zones")
        else:
            if debug['best_e_overlap'] > 0:
                reasons.append(f"best E overlap={debug['best_e_overlap']:.0%} (need ≥{self.overlap_threshold:.0%})")
            if debug['best_a_overlap'] > 0:
                reasons.append(f"best A overlap={debug['best_a_overlap']:.0%} (need ≥{self.overlap_threshold:.0%})")
            if debug['best_e_overlap'] == 0 and debug['best_a_overlap'] == 0:
                reasons.append("same-dir zones exist but zero price overlap")
        
        if debug['best_e_overlap_any_dir'] > 0 or debug['best_a_overlap_any_dir'] > 0:
            reasons.append(
                f"cross-dir overlap: E={debug['best_e_overlap_any_dir']:.0%}, "
                f"A={debug['best_a_overlap_any_dir']:.0%}"
            )
        
        debug['reason'] = f"TIER_4: {'; '.join(reasons)}"
        return ZoneNestingTier.TIER_4, debug


class ObstructionChecker:
    """
    v3.8: Check if HTF zones obstruct trade setup.
    
    Unviolated HTF zones act as obstruction:
    - LONG blocked by unviolated SZ above CMP
    - SHORT blocked by unviolated BZ below CMP
    
    Zone is obstruction until WICK violates distal.
    """
    
    def get_obstructions(self, direction: str, cmp: float, 
                         a_zones: List[Zone], e_zones: List[Zone]) -> List[Zone]:
        """
        Get list of obstructing zones for a given direction.
        
        Args:
            direction: "LONG" or "SHORT"
            cmp: Current market price
            a_zones: Analyze TF zones
            e_zones: Evaluate TF zones
        
        Returns:
            List of obstructing zones sorted by distance from CMP
        """
        all_htf_zones = a_zones + e_zones
        obstructions = []
        
        if direction == "LONG":
            # Find unviolated SZ above CMP
            for zone in all_htf_zones:
                if zone.is_sell_zone and not zone.invalidated:
                    if zone.proximal > cmp:  # Zone is above current price
                        obstructions.append(zone)
            # Sort by proximal (nearest first)
            obstructions.sort(key=lambda z: z.proximal)
        
        elif direction == "SHORT":
            # Find unviolated BZ below CMP
            for zone in all_htf_zones:
                if zone.is_buy_zone and not zone.invalidated:
                    if zone.proximal < cmp:  # Zone is below current price
                        obstructions.append(zone)
            # Sort by proximal (nearest first, descending)
            obstructions.sort(key=lambda z: z.proximal, reverse=True)
        
        return obstructions
    
    def get_nearest_obstruction(self, direction: str, cmp: float,
                                 a_zones: List[Zone], e_zones: List[Zone]) -> Optional[Zone]:
        """Get the nearest obstructing zone."""
        obstructions = self.get_obstructions(direction, cmp, a_zones, e_zones)
        return obstructions[0] if obstructions else None
    
    def is_path_clear(self, direction: str, cmp: float, target: float,
                      a_zones: List[Zone], e_zones: List[Zone]) -> Tuple[bool, Optional[Zone]]:
        """
        Check if path to target is clear of obstructions.
        
        Returns:
            (is_clear, blocking_zone) - True if clear, else False with the blocking zone
        """
        obstructions = self.get_obstructions(direction, cmp, a_zones, e_zones)
        
        for zone in obstructions:
            if direction == "LONG":
                # Check if obstruction is between CMP and target
                if cmp < zone.proximal < target:
                    return False, zone
            elif direction == "SHORT":
                # Check if obstruction is between CMP and target
                if target < zone.proximal < cmp:
                    return False, zone
        
        return True, None




class ZoneScorerV38:
    """
    v3.8: Zone scoring with retest and penetration penalties.
    
    BASE_SCORE = 10
    
    Retest penalty: -1 per retest
    Penetration penalty per retest:
      - 0-20%: -0
      - 21-50%: -1
      - 51-75%: -2
      - 76-99%: -3
    """
    
    BASE_SCORE = 10
    
    def __init__(self):
        self.penetration_penalties = [
            (20, 0),   # 0-20%: no additional penalty
            (50, 1),   # 21-50%: -1
            (75, 2),   # 51-75%: -2
            (100, 3),  # 76-99%: -3
        ]
    
    def _get_penetration_penalty(self, penetration_pct: float) -> int:
        """Get penalty based on penetration percentage."""
        for threshold, penalty in self.penetration_penalties:
            if penetration_pct <= threshold:
                return penalty
        return 3  # Max penalty
    
    def calculate_score(self, zone: Zone, retest_penetrations: List[float] = None) -> int:
        """
        Calculate zone score based on retests and penetrations.
        
        Args:
            zone: The zone to score
            retest_penetrations: List of max penetration % for each retest
                                 (if None, uses zone.retest_count with assumed 30% each)
        
        Returns:
            Zone score (can be negative for heavily tested zones)
        """
        score = self.BASE_SCORE
        
        # Retest penalty
        retest_count = zone.retest_count
        score -= retest_count  # -1 per retest
        
        # Penetration penalty
        if retest_penetrations:
            for pct in retest_penetrations:
                score -= self._get_penetration_penalty(pct)
        else:
            # Assume moderate penetration (30%) for each retest if not tracked
            for _ in range(retest_count):
                score -= self._get_penetration_penalty(30)

        score += zone.age_penalty
        
        return score
    
    def classify_score(self, score: int) -> str:
        """Classify score into probability category."""
        if score >= 8:
            return "EXCELLENT"
        elif score >= 6:
            return "GOOD"
        elif score >= 3:
            return "FAIR"
        elif score >= 1:
            return "WEAK"
        else:
            return "DEPLETED"



class SlidingWindowManager:
    """
    v4.4 Section 13.1: Sliding Window Boundaries
    
    When identifying 'nearest BZ' and 'nearest SZ' for Rule C:
    - Max Lookback Bars: 200 (configurable)
    - Max ATR Distance: 20 ATR (configurable)
    - Window Type: min(bars limit, ATR distance limit)
    """
    
    def __init__(self, cfg: Config):
        self.cfg = cfg
    
    def is_zone_in_window(self, zone: Zone, current_idx: int, cmp: float, atr_value: float) -> bool:
        """
        Check if zone is within sliding window for Rule C.
        
        Window = zones where:
          created_idx >= (current_idx - max_bars) AND
          abs(zone.distal - CMP) <= max_atr * ATR(14)
        """
        # Check bars limit
        _tf_bars = {TF.E: 200, TF.A: 500, TF.X: 800}
        max_bars = _tf_bars.get(zone.tf, self.cfg.sliding_window_max_bars)

        bars_since_creation = current_idx - zone.created_idx
        
        if bars_since_creation > max_bars:
            return False
        
        # Check ATR distance limit
        if atr_value > 0:
            atr_distance = abs(zone.distal - cmp) / atr_value
            if atr_distance > self.cfg.sliding_window_max_atr:
                return False
        
        return True
    
    def filter_zones_in_window(self, zones: List[Zone], current_idx: int, 
                               cmp: float, atr_value: float) -> List[Zone]:
        """Filter zones to only those within sliding window."""
        return [z for z in zones if self.is_zone_in_window(z, current_idx, cmp, atr_value)]



class SetupExtractor:
    """
    Pipeline Step 19: Select ONE highest-probability setup per direction.
    
    Architecture (v3.8.1 FINAL):
    1. Collect preliminary GREEN zones (passed G1-G8, G10 + DBR/RBD)
    2. Proximity hard filter (>5% from CMP = excluded — Annexure v1.2 Sec 4)
    3. Compute weighted score per zone (uses RR from E/S/T already in loop)
    4. Rank by weighted score descending
    5. For highest-scored zone: check G9 (RR >= 2.1)
       - Pass → this is THE setup → done
       - Fail → cascade to next-ranked → repeat
    6. Output: ONE SetupPayload per direction (LONG/SHORT), or None
    
    E/S/T is computed in the qualification loop (lightweight, for RR scoring).
    Only the SELECTED zone's E/S/T becomes the final output.
    G6 may have adjusted the target if obstruction was present.
    
    REF: Methodology v3.8.1 Sec 9.3 (weighted model replaces tuple key),
         Annexure v1.2 Sec 4-5
    """
    
    def __init__(self, cfg: Config, gate_checker: HardGateChecker):
        self.cfg = cfg
        self.gate_checker = gate_checker
        self.scorer = WeightedZoneScorer(cfg)

    def _validate_setup_sanity(self, zone: Zone) -> bool:
        """BUG-36: Last-line defense against phantom/impossible setups."""
        if zone.entry:
            zone.entry_price = zone.entry
        if zone.entry_price is None or zone.stop_price is None or zone.target_price is None:
            print("bs1.............................")
            return False
        if zone.is_buy_zone:
            if not (zone.stop_price < zone.entry_price < zone.target_price):
                print("bs2.............................")
                return False
        else:
            if not (zone.stop_price > zone.entry_price > zone.target_price):
                print("bs3.............................")
                return False
        if zone.proximal == zone.distal:
            print("bs4.............................")
            return False
        rr = zone.rr_ratio or 0.0
        if rr <= 0 or not (rr < float("inf")):
            print("bs5.............................")
            return False
        print("bs6.............................")
        return True
    
    def _build_payload(self, zone: Zone, weighted_score: float,
                       score_breakdown: dict, proximity_pct: float,
                       trend_context: TrendContext, candidates_count: int,
                       selection_reason: str) -> SetupPayload:
        """Package selected zone into SetupPayload."""
        side = "LONG" if zone.is_buy_zone else "SHORT"
        trade_type = (
            trend_context.trade_type_long if zone.is_buy_zone
            else trend_context.trade_type_short
        )
        
        return SetupPayload(
            symbol=zone.symbol,
            zone_id=zone.zone_id,
            zone_type=zone.ztype.value if hasattr(zone.ztype, 'value') else str(zone.ztype),
            timeframe=zone.tf.value if hasattr(zone.tf, 'value') else str(zone.tf),
            side=side,
            entry_price=zone.entry if zone.entry else zone.proximal,
            stop_price=zone.stop_price if zone.stop_price else zone.distal,
            target_price=zone.target_price if zone.target_price else 0.0,
            rr_ratio=zone.rr_ratio if zone.rr_ratio else 0.0,
            target_mode=zone.target_mode,
            rank_key=(weighted_score,),  # Single weighted score replaces tuple
            zone_score_legacy=zone.final_score if zone.final_score else 0,
            zone_score_v38=zone.zone_v38_score if zone.zone_v38_score else 0,
            nesting_tier=(zone.nesting_tier.name
                          if zone.nesting_tier and hasattr(zone.nesting_tier, 'name')
                          else "NONE"),
            gap_composite_score=(zone.gap_composite_score
                                 if zone.ztype in (ZoneType.GDZ, ZoneType.GSZ)
                                 else None),
            overlap_ratio=float(
                getattr(zone, "overlap_ratio", 0.0) or 0.0
            ),
            htf_target_price=(
                float(zone.htf_target_price)
                if getattr(zone, "htf_target_price", None) is not None
                else None
            ),

            struct_stop_A=(
                float(zone.enclosing_a_zone.distal)
                if getattr(zone, "enclosing_a_zone", None) is not None
                else None
            ),

            struct_stop_E=(
                float(zone.enclosing_e_zone.distal)
                if getattr(zone, "enclosing_e_zone", None) is not None
                else None
            ),
            trend_regime=(trend_context.regime_E.value
                          if hasattr(trend_context.regime_E, 'value')
                          else str(trend_context.regime_E)),
            quadrant=(trend_context.quadrant_E.value
                      if trend_context.quadrant_E and hasattr(trend_context.quadrant_E, 'value')
                      else "UNKNOWN"),
            trade_type=(trade_type.value
                        if hasattr(trade_type, 'value')
                        else str(trade_type)),
            bias=trend_context.bias,
            entry_mode="AUTO",
            selection_reason=selection_reason,
            candidates_count=candidates_count,
            proximity_pct=round(proximity_pct, 2),
            dbr_required=trend_context.dbr_required,
            rbd_required=trend_context.rbd_required,
            pattern_validated=zone.pattern_validated
        )
    
    def extract(self, zones_X: List[Zone], trend_context: TrendContext,
                cmp: float, atr_X: float,
                ema_20: Optional[float] = None,
                max_entry_distance_pct: Optional[float] = None,
                zones_A: Optional[List[Zone]] = None) -> Tuple[Optional[SetupPayload],
                                                          Optional[SetupPayload],
                                                          List[dict]]:
        """
        Step 19: Weighted score → rank → G9 cascade → ONE setup per direction.
        
        Returns: (best_long, best_short, cascade_log)
        """
        # 1. FILTER: Preliminary GREEN zones (passed G1-G3, G5-G10 + DBR/RBD)
        green_zones = [z for z in zones_X if z.state == ZoneState.GREEN]
        # print(green_zones, "ggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg")
        if max_entry_distance_pct is not None and max_entry_distance_pct > 0:
            before = len(green_zones)
            green_zones = [
                z for z in green_zones
                if z.entry_price is None
                or abs(z.entry_price - cmp) / cmp <= max_entry_distance_pct
            ]
            _filtered = before - len(green_zones)
            if _filtered > 0:
                pass  # Zones beyond entry distance threshold silently removed
        if not green_zones:
            return None, None, []
        
        # 2. PROXIMITY FILTER: exclude zones too far from CMP.
        # PROX (CB): the flat 5% filter is volatility-blind (5% is huge for a low-vol large-cap,
        # tight for a high-vol name). When proximity_use_atr_tier is enabled, the threshold is
        # ATR-scaled: max(setup_proximity_pct, proximity_atr_mult * ATR%). Defaults reproduce the
        # legacy flat-5% behaviour until the ATR multiple is tuned on the full universe (see notes).
        scored = []
        for z in green_zones:
            prox_pct = abs(cmp - z.proximal) / cmp * 100.0 if cmp > 0 else float('inf')
            _prox_threshold = self.cfg.setup_proximity_pct
            if getattr(self.cfg, 'proximity_use_atr_tier', False) and cmp > 0 and atr_X:
                # atr_X is this function's X-TF ATR param; express as % of CMP.
                _atr_pct = atr_X / cmp * 100.0
                _atr_thresh = getattr(self.cfg, 'proximity_atr_mult', 1.5) * _atr_pct
                _prox_threshold = max(self.cfg.setup_proximity_pct, _atr_thresh)
            if prox_pct <= _prox_threshold:
            # 3. WEIGHTED SCORE (uses RR from E/S/T already computed in loop)
                w_score = self.scorer.compute_weighted_score(z, cmp, trend_context, ema_20)
                breakdown = self.scorer.compute_score_breakdown(z, cmp, trend_context, ema_20)
                scored.append((w_score, z, prox_pct, breakdown))
        
        print(len(scored), "scored")
        if not scored:
            return None, None, []
        
        # 4. RANK: descending by weighted score (higher = better)
        scored.sort(key=lambda x: -x[0])
        
        # 5. SEPARATE by direction and CASCADE G4
        long_candidates = [(ws, z, pp, bd) for ws, z, pp, bd in scored if z.is_buy_zone]
        short_candidates = [(ws, z, pp, bd) for ws, z, pp, bd in scored if z.is_sell_zone]
        
        # v3.8.9 Fix #104: HTF boundary filter for SHORT.
        # X-TF SZ above A-TF SZ distal is unreachable: price must consume
        # all A-TF sell orders first, invalidating the short thesis.
        # NOT applied to LONG — deeper demand below A-TF BZ distal is
        # structurally valid (nested in E-TF).
        # Example: ADANI_ENT X-TF SZ prox=3085 > Weekly SZ dist=3070 → skip.
        if zones_A:
            _a_sz = [z for z in zones_A if z.is_sell_zone]
            if _a_sz:
                _a_sz_distal = max(z.distal for z in _a_sz)
                short_candidates = [
                    (ws, z, pp, bd) for ws, z, pp, bd in short_candidates
                    if z.proximal <= _a_sz_distal
                ]
        
        cascade_log = []
        
        def _select_best(candidates: list, direction: str) -> Optional[SetupPayload]:
            """Cascade: check G9 (RR) on highest-scored zone, fall to next if fail."""
            for rank_idx, (w_score, zone, prox_pct, breakdown) in enumerate(candidates):
                # G9: RR >= 2.1 (after SL and target adjustments including G6)
                g9_passed, g9_reason = self.gate_checker.check_g9_rr(zone)
                
                log_entry = {
                    'direction': direction,
                    'rank': rank_idx + 1,
                    'zone_id': zone.zone_id,
                    'weighted_score': w_score,
                    'breakdown': breakdown,
                    'rr': zone.rr_ratio if zone.rr_ratio else 0.0,
                    'g9_passed': g9_passed,
                    'g9_reason': g9_reason or "",
                    'selected': False
                }
                
                rr = zone.rr_ratio if zone.rr_ratio else 0.0
                
                if g9_passed:

                    if not self._validate_setup_sanity(zone):
                        zone.state = ZoneState.RED
                        zone.block_reason = "BUG36_SANITY_FAIL"
                        log_entry['g9_reason'] = 'BUG36_SANITY_FAIL'
                        cascade_log.append(log_entry)
                        continue
                    
                    reason = (
                        f"SELECTED {direction} — Rank #{rank_idx + 1} of "
                        f"{len(candidates)} (W={w_score:.1f}, "
                        f"RR={rr:.2f}, tier={zone.nesting_tier}, "
                        f"prox={prox_pct:.1f}%)"
                    )
                    if rank_idx > 0:
                        reason += f" [cascaded past {rank_idx} G9-failed zone(s)]"
                    
                    log_entry['selected'] = True
                    cascade_log.append(log_entry)
                    print(zone.entry, zone.target_price, zone.stop_price, "selected")
                    return self._build_payload(
                        zone, w_score, breakdown, prox_pct, trend_context,
                        candidates_count=len(candidates),
                        selection_reason=reason
                    )
                else:
                    zone.state = ZoneState.RED
                    zone.block_reason = f"G9_RR_LOW ({rr:.2f}) — cascaded"
                    cascade_log.append(log_entry)
            
            return None
        
        best_long = _select_best(long_candidates, "LONG")
        best_short = _select_best(short_candidates, "SHORT")
        print("best long", best_long)
        print("best short", best_short)
        print("cascade log", cascade_log)
        return best_long, best_short, cascade_log




# ==============================================================================
# WEIGHTED ZONE SCORER — SETUP SELECTION SCORING (v3.8.1 RESTRUCTURED)
# ==============================================================================

class WeightedZoneScorer:
    """
    Weighted scoring system for setup selection (0-100 scale).
    9 dimensions, configurable weights via Config.ws_* fields.
    
    Objectives (institutional S/D doctrine):
    (A) TRIGGER: Will price reach this zone and execute?
    (B) HOLD: Will the zone hold (not hit SL)?
    (C) REWARD: Is the payoff worth the risk?
    
    Priority order (from user's domain expertise):
    ┌────┬───────────────────────────────┬────────┬────────────────────────────┐
    │ #  │ Factor                        │ Weight │ What it determines         │
    ├────┼───────────────────────────────┼────────┼────────────────────────────┤
    │ 1  │ Trend Direction               │  20%   │ Against E/A trend = fail   │
    │ 2  │ Base Quality + Struct Removal │  18%   │ Zone formation integrity   │
    │ 3  │ Freshness + Penetration       │  16%   │ Unfilled orders remain     │
    │ 4  │ RR Ratio                      │  15%   │ Reward quality             │
    │ 5  │ Departure Strength            │  12%   │ Institutional conviction   │
    │ 6  │ HTF Nesting / Confluence      │  10%   │ Multi-TF order depth       │
    │ 7  │ EMA-20 Confluence             │   4%   │ Lagging indicator support  │
    │ 8  │ Age Penalty                   │   3%   │ Zone staleness             │
    │ 9  │ Proximity to CMP              │   2%   │ Hard filter primary        │
    └────┴───────────────────────────────┴────────┴────────────────────────────┘
    
    Key design principles:
    - Trend is #1: trading against E/A regime has structurally high failure
      rate. No amount of zone quality compensates for wrong direction.
    - Base quality includes structure removal: a BZ that hasn't violated the
      opposing SZ has no institutional confirmation. Critical for S/D.
    - Freshness > departure: untested zone with moderate departure beats
      retested zone with strong departure (orders already filled).
    - RR between freshness (#3) and departure (#5): zone quality determines
      win rate, RR determines payoff. Win rate first, payoff second.
    - Nesting at #6 (not #1): nested zone against trend still loses.
      Nesting adds confluence but regime direction dominates.
    - Proximity at #9 with 2% weight: E/A filtering already selected nearest
      institutional zones. In X, quality > proximity. Hard filter (>5%) does
      the heavy lifting.
    - EMA-20 is lagging, minor signal. Age is lowest — valid old zones still
      have institutional interest.
    
    All weights configurable via Config for backtesting optimization.
    Scoring bands are FIXED (not configurable) — they encode S/D doctrine.
    """
    
    def __init__(self, cfg: Config):
        self.cfg = cfg
    
    # ── Dimension 1: Trend Direction (0-100) ──────────────────────────
    
    def score_trend(self, zone: Zone,
                    trend_context: TrendContext) -> float:
        """
        #1 — Is the E+A regime combination supporting this trade?
        
        Uses TradeType from the 18-scenario classification (Trend v4.4.1
        Sec 7-9). TradeType already encodes the E+A combination:
        
        ┌─────────────────┬─────────────────────────────────┬───────┐
        │ TradeType        │ E+A combo (longs example)       │ Score │
        ├─────────────────┼─────────────────────────────────┼───────┤
        │ CONTINUATION     │ E=UP, A=UP                      │  100  │
        │ CONT_REDUCED     │ E=UP+A=SW, or E=SW+A=UP         │   70  │
        │ RANGE_EXTREME    │ E=SW, A=SW                      │   40  │
        │ REVERSAL_ONLY    │ E=UP+A=DN, or E=DN+A=UP (DBR)   │   20  │
        │ NO_TRADE         │ blocked (won't reach scorer)     │    0  │
        └─────────────────┴─────────────────────────────────┴───────┘
        
        CONTINUATION: Both E and A aligned → highest probability.
        CONT_REDUCED: One aligned, one neutral → good but reduced.
        RANGE_EXTREME: Both neutral → only at range boundaries.
        REVERSAL_ONLY: Counter-trend with DBR/RBD pattern required →
          structurally low probability, strict gating compensates.
        NO_TRADE: G5 blocks before scoring → score 0 if reached.
        """
        # Get the trade type for this zone's direction
        if zone.is_buy_zone:
            trade_type = trend_context.trade_type_long
        else:
            trade_type = trend_context.trade_type_short
        
        if trade_type == TradeType.CONTINUATION:
            return 100.0
        elif trade_type == TradeType.CONT_REDUCED:
            return 70.0
        elif trade_type == TradeType.RANGE_EXTREME:
            return 40.0
        elif trade_type == TradeType.REVERSAL_ONLY:
            return 20.0
        else:  # NO_TRADE — should not reach here (G5 blocks)
            return 0.0
    
    # ── Dimension 2: Base Quality + Structure Removal (0-100) ─────────
    
    def score_base_quality(self, zone: Zone) -> float:
        """
        #2 — Zone formation quality + opposing structure violation.
        
        Two sub-components (each 0-50, total 0-100):
        
        A) Base formation (0-50):
           No base (0 candles):     0
           Single candle:          25
           Clean 2-N candle base:  50
           Too many candles:        0  (messy, not institutional)
        
        B) Structure removal (0-50):
           Removed opposing: 50  (departure candle violated opposing zone)
           Not removed:       0  (no institutional confirmation)
        
        A BZ that hasn't violated the opposing SZ has no institutional
        confirmation that orders were placed there.
        """
        # A) Base formation
        if zone.base_len == 0:
            base_score = 0.0
        elif zone.base_len == 1:
            base_score = 25.0
        elif 2 <= zone.base_len <= self.cfg.max_base_len:
            base_score = 50.0
        else:
            base_score = 0.0
        
        # B) Structure removal
        struct_score = 50.0 if zone.removes_structure else 0.0
        
        return base_score + struct_score
    
    # ── Dimension 3: Freshness + Penetration (0-100) ──────────────────
    
    def score_freshness(self, zone: Zone) -> float:
        """
        #3 — Untested zone = unfilled orders. Penetration degrades this.
        
        Freshness base (0-80):
           0 retests (untested):  80  (all orders pending)
           1 retest:             40  (some orders filled)
           2+ retests:            0  (most orders filled)
        
        Penetration penalty (0 to -30):
           0% penetration:        0
           1-20% penetration:    -5   (minor wick, zone held)
           21-50% penetration:  -15   (significant, zone weakened)
           51-75% penetration:  -25   (deep, zone barely held)
           76%+ penetration:    -30   (zone nearly invalidated)
        
        Reactivation bonus:
           REACTIVATED zones:   +20   (survived retest = confirmed)
        
        Final: clamp to 0-100
        """
        # Freshness base
        if zone.retest_count == 0:
            base = 80.0
        elif zone.retest_count == 1:
            base = 40.0
        else:
            base = 0.0
        
        # Penetration penalty
        pen_pct = zone.penetration_pct if zone.penetration_pct else 0
        if pen_pct <= 0:
            pen_penalty = 0.0
        elif pen_pct <= 20:
            pen_penalty = -5.0
        elif pen_pct <= 50:
            pen_penalty = -15.0
        elif pen_pct <= 75:
            pen_penalty = -25.0
        else:
            pen_penalty = -30.0
        
        # Reactivation bonus
        reactivation = 20.0 if (hasattr(zone, 'age_class') and
                                str(zone.age_class) == 'REACTIVATED') else 0.0
        
        return max(0.0, min(100.0, base + pen_penalty + reactivation))
    
    # ── Dimension 4: RR Ratio (0-100) ─────────────────────────────────
    
    def score_rr(self, zone: Zone) -> float:
        # """
        # #4 — Reward-to-risk quality.
        
        # < 2.1:       0   (fails G4 anyway)
        # 2.1–2.5:  40-60  (acceptable)
        # 2.5–3.0:  60-80  (good)
        # 3.0–4.0:  80-100 (very good)
        # > 4.0:     100   (excellent)
        
        # Linear interpolation within bands.
        # """
        rr = zone.rr_ratio if zone.rr_ratio else 0.0
        
        if rr < self.cfg.default_rr:
            return 0.0
        elif rr <= 2.5:
            return 40.0 + (rr - 2.1) / 0.4 * 20.0
        elif rr <= 3.0:
            return 60.0 + (rr - 2.5) / 0.5 * 20.0
        elif rr <= 4.0:
            return 80.0 + (rr - 3.0) / 1.0 * 20.0
        else:
            return 100.0
    
    # ── Dimension 5: Departure Strength (0-100) ───────────────────────
    
    def score_departure(self, zone: Zone) -> float:
        """
        #5 — Impulsive departure = institutional conviction.
        
        Strong (ATR >= 1.5x, body >= 70%):  100
        Moderate (ATR >= 1.0x, body >= 50%):  50
        Weak:                                  0
        """
        if (zone.departure_atr >= self.cfg.departure_atr_score2 and
                zone.body_pct >= self.cfg.departure_body_score2):
            return 100.0
        elif (zone.departure_atr >= self.cfg.departure_atr_score1 and
                zone.body_pct >= self.cfg.departure_body_score1):
            return 50.0
        else:
            return 0.0
    
    # ── Dimension 6: HTF Nesting / Confluence (0-100) ─────────────────
    
    def score_confluence(self, zone: Zone) -> float:
        """
        #6 — Is this X zone nested inside E or A zones?
        
        Nesting = multi-TF institutional order depth at the same level.
        Important, but ranked below trend (#1) because a nested zone
        against the trend still loses.
        
        TIER_1 (nested in E + A):    100
        TIER_2 (nested in E or A):    75
        TIER_3 (standalone, valid):   40
        TIER_4 / None:                 0
        Consecutive bonus:           +15 (partner zone adds depth)
        """
        tier = zone.nesting_tier
        if tier is None:
            base = 40.0
        elif tier == ZoneNestingTier.TIER_1:
            base = 100.0
        elif tier == ZoneNestingTier.TIER_2:
            base = 75.0
        elif tier == ZoneNestingTier.TIER_3:
            base = 40.0
        else:
            base = 0.0
        
        if zone.is_part_of_consecutive:
            base = min(base + 15.0, 100.0)
        
        if zone.is_part_of_overlapping:
            base = min(base + 15.0, 100.0)

        return base
    
    # ── Dimension 7: EMA-20 Confluence (0-100) ────────────────────────
    
    def score_ema_confluence(self, zone: Zone,
                              ema_20: Optional[float]) -> float:
        """
        #7 — EMA-20 near zone supports trade. Lagging indicator, minor.
        
        EMA inside zone:      100
        EMA near zone (20%):   50
        EMA far from zone:      0
        """
        if ema_20 is None or zone.zone_height == 0:
            return 0.0
        
        buffer = 0.20 * zone.zone_height
        
        if zone.is_buy_zone:
            if zone.distal <= ema_20 <= zone.proximal:
                return 100.0
            if zone.proximal < ema_20 <= zone.proximal + buffer:
                return 50.0
        else:
            if zone.proximal <= ema_20 <= zone.distal:
                return 100.0
            if zone.proximal - buffer <= ema_20 < zone.proximal:
                return 50.0
        
        return 0.0
    
    # ── Dimension 8: Age Penalty (0-100) ──────────────────────────────
    
    def score_age(self, zone: Zone) -> float:
        """
        #8 — Zone staleness. Old but valid zones still have institutional
        interest, so this is the lowest-weighted quality factor.
        
        FRESH:         100
        ACTIVE:         70
        REACTIVATED:    50  (returned to relevance)
        STALE:          20  (old, lower priority)
        """
        age = str(zone.age_class) if hasattr(zone, 'age_class') else 'UNKNOWN'
        
        if 'FRESH' in age:
            return 100.0
        elif 'ACTIVE' in age:
            return 70.0
        elif 'REACTIVATED' in age:
            return 50.0
        elif 'STALE' in age:
            return 20.0
        else:
            return 70.0  # Default to ACTIVE equivalent
    
    # ── Dimension 9: Proximity to CMP (0-100) ─────────────────────────
    
    def score_proximity(self, zone: Zone, cmp: float) -> float:
        """
        #9 — Trigger probability. Lowest scoring weight (2%).
        
        E/A filtering already selects nearest institutional zones to CMP.
        In X-TF, zone quality matters more than proximity.
        Hard filter (>5% = excluded) does the heavy lifting.
        This scoring component only differentiates within the 0-5% band.
        
        0-1%: 100, 1-2%: 80, 2-3%: 60, 3-4%: 40, 4-5%: 20, >5%: 0
        """
        if cmp <= 0:
            return 0.0
        pct = abs(cmp - zone.proximal) / cmp * 100.0
        
        if pct <= 1.0:
            return 100.0
        elif pct <= 2.0:
            return 100.0 - (pct - 1.0) * 20.0
        elif pct <= 3.0:
            return 80.0 - (pct - 2.0) * 20.0
        elif pct <= 4.0:
            return 60.0 - (pct - 3.0) * 20.0
        elif pct <= 5.0:
            return 40.0 - (pct - 4.0) * 20.0
        else:
            return 0.0
    
    # ── Composite Score ───────────────────────────────────────────────
    
    def compute_weighted_score(self, zone: Zone, cmp: float,
                                trend_context: 'TrendContext',
                                ema_20: Optional[float] = None) -> float:
        """
        Compute final weighted score (0-100).
        
        Score = ws_trend × Trend + ws_base_quality × Base
              + ws_freshness × Freshness + ws_rr × RR
              + ws_departure × Departure + ws_confluence × Confluence
              + ws_ema_confluence × EMA + ws_age × Age
              + ws_proximity × Proximity
        
        All ws_* weights read from Config (configurable for backtesting).
        """
        s_trend = self.score_trend(zone, trend_context)
        s_base = self.score_base_quality(zone)
        s_fresh = self.score_freshness(zone)
        s_rr = self.score_rr(zone)
        s_depart = self.score_departure(zone)
        s_conf = self.score_confluence(zone)
        s_ema = self.score_ema_confluence(zone, ema_20)
        s_age = self.score_age(zone)
        s_prox = self.score_proximity(zone, cmp)
        
        weighted = (
            self.cfg.ws_trend * s_trend +
            self.cfg.ws_base_quality * s_base +
            self.cfg.ws_freshness * s_fresh +
            self.cfg.ws_rr * s_rr +
            self.cfg.ws_departure * s_depart +
            self.cfg.ws_confluence * s_conf +
            self.cfg.ws_ema_confluence * s_ema +
            self.cfg.ws_age * s_age +
            self.cfg.ws_proximity * s_prox
        )
        
        return round(weighted, 2)
    
    def compute_score_breakdown(self, zone: Zone, cmp: float,
                                 trend_context: 'TrendContext',
                                 ema_20: Optional[float] = None) -> dict:
        """Detailed score breakdown for diagnostics and transparency."""
        s_trend = self.score_trend(zone, trend_context)
        s_base = self.score_base_quality(zone)
        s_fresh = self.score_freshness(zone)
        s_rr = self.score_rr(zone)
        s_depart = self.score_departure(zone)
        s_conf = self.score_confluence(zone)
        s_ema = self.score_ema_confluence(zone, ema_20)
        s_age = self.score_age(zone)
        s_prox = self.score_proximity(zone, cmp)
        
        return {
            'trend': round(s_trend, 1),
            'base_quality': round(s_base, 1),
            'freshness': round(s_fresh, 1),
            'rr': round(s_rr, 1),
            'departure': round(s_depart, 1),
            'confluence': round(s_conf, 1),
            'ema_confluence': round(s_ema, 1),
            'age': round(s_age, 1),
            'proximity': round(s_prox, 1),
            'weighted_total': round(
                self.cfg.ws_trend * s_trend +
                self.cfg.ws_base_quality * s_base +
                self.cfg.ws_freshness * s_fresh +
                self.cfg.ws_rr * s_rr +
                self.cfg.ws_departure * s_depart +
                self.cfg.ws_confluence * s_conf +
                self.cfg.ws_ema_confluence * s_ema +
                self.cfg.ws_age * s_age +
                self.cfg.ws_proximity * s_prox, 2
            )
        }


def validate_setup_readiness(zone: Zone, cmp: float, execution_tf: TF,
                              atr_value: float, current_idx: int,
                              cfg: Config) -> Tuple[bool, Optional[str]]:
    """
    Scanner-callable re-validation utility. Call on EVERY scan cycle to detect
    and purge ghost/stale setups that should no longer be displayed.
    
    This is NOT a replacement for HardGateChecker — it is a lightweight
    pre-filter that catches obvious invalid states before expensive gate checks.
    
    Checks (in order):
      1. Zone-TF match: zone.tf must equal execution_tf
      2. Zone invalidation: zone must not be wick-violated
      3. Compound staleness: age + distance compound gate (same as G11)
      4. Age re-classification: updates zone.age_class with current CMP/ATR
    
    Returns:
        (True, None) if zone is ready for gate checking
        (False, reason) if zone should be purged from active setup list
    
    Usage by scanner:
        for zone in active_zones:
            ready, reason = validate_setup_readiness(
                zone, cmp, TF.X, atr_val, current_bar_idx, cfg
            )
            if not ready:
                purge(zone, reason)
    """
    # 1. Zone-TF match
    if zone.tf != execution_tf:
        return False, (
            f"TF_MISMATCH (zone.tf={zone.tf.value}, "
            f"execution_tf={execution_tf.value})"
        )
    
    # 2. Zone invalidation (wick violation)
    if zone.invalidated:
        return False, "ZONE_INVALIDATED (wick violation)"
    
    # 3. Compound staleness (mirrors G11)
    if cmp > 0:
        # Recompute age_bars relative to current bar
        age_bars = current_idx - zone.created_idx
        zone.age_bars = age_bars  # Update zone's age tracking
        
        distance_pct = abs(cmp - zone.proximal) / cmp
        if (age_bars > cfg.compound_stale_age_bars
                and distance_pct > cfg.compound_stale_distance_pct):
            return False, (
                f"COMPOUND_STALE "
                f"(age={age_bars} bars, dist={distance_pct:.1%})"
            )
    
    # 4. Age re-classification (side-effect: updates zone.age_class)
    if atr_value > 0:
        age_classifier = ZoneAgeManager(cfg)
        age_classifier.update_zone_age(zone, current_idx, cmp, atr_value)
    
    return True, None