import logging
from scripts.trade_engine import ZoneDetector, ZoneQualifier, ZoneRanker, GapModule, MultiZoneHandler, load_preprocess_data, process_trend_zones, process_qualified_zones_setup

from scripts.additional_engine_class import ZoneQualityScorer, RiskTargetCalculator, HardGateChecker, DBRRBRValidator, ZoneAgeManager
from scripts.additional_engine_class import GapZoneIntegrator, WickViolationHandler, ThreeWayConflictResolver, ZoneNestingClassifier, SlidingWindowManager
from scripts.additional_engine_class import ObstructionChecker, ZoneScorerV38, ZoneNestingTier, SetupExtractor, WeightedZoneScorer
from typing import Optional, List, Dict, Any, Tuple

from scripts.trade_engine import atr, volatility_regime, ema, format_zone_ranges_with_setup

from scripts.trend_engine import Config as trend_config

from scripts.models import Config as trade_config
from scripts.models import Zone, CandleSeries
from shared.config.settings import stock_data_dir_config, stock_logic_config
from scripts.side_enablement_policy import SIDE_POLICY, Side, resolve_segment
from dataclasses import asdict, is_dataclass
from datetime import timedelta
from scripts.trend_engine import TrendEngine, MultiTimeframeTrendCalculator

from scripts.models import TrendContext, Quadrant, ReversalPattern, PatternType, TF, ZoneType, ZoneState, TrendRegime, TradeType
import os

from shared.utils.logger import logger
# logger = Logger(__name__)


# process_qualified_zones()

class SDEnginePipeline:
    """
    Complete SD Engine Pipeline v3.8
    
    COMPLETE FEATURES (v3.3 + v4.4 + v3.8):
    1. Zone Detection (Regular + Gap)
    2. Multi-Zone Handling (Consecutive + Overlapping)
    3. Zone Qualification (Hard Gates + Structure Removal)
    4. Trend Calculation (Rules A-F with HTF Veto)
    5. Trade Type Classification (v4.4 Section 9)
    6. DBR/RBR Validation (v4.4 Section 10)
    7. Zone Quality Scoring (v4.4 Section 11)
    8. Execution Decision Trees (v4.4 Sections 7-8)
    
    v3.4 ADDITIONS (from v4.4):
    9. Sliding Window Boundaries (v4.4 Section 13.1)
    10. Zone Age Classification (v4.4 Section 13.2)
    11. Gap Zone Integration (v4.4 Section 14) — FULLY WIRED
    12. Three-Way Conflict Resolution (v4.4 Section 15.1)
    13. Wick Violation Handling (v4.4 Section 15.2)
    
    v3.8 ADDITIONS:
    14. Zone Nesting Tier Classification (G7)
    15. HTF Obstruction Checking (G8)
    16. CMP-inside-HTF-zone Veto (G8b)
    17. Quadrant-based Permission Enforcement (G9)
    18. Enhanced Zone Scoring with ZoneScorerV38 (G10)
    19. X-TF Regime Bias Modifier for SW x SW
    20. 10-Gate HardGateChecker (replaces 6-gate legacy)
    
    v3.8 (BUG-1 FROM v3.7 RESOLVED):
    21. GapModule.detect() wired into pipeline (GDZ/GSZ now detected)
    22. VolatilityRegime computed per TF via volatility_regime() + ema()
    23. GapModule.check_session_acceptance() enforced post-filter
    24. Non-accepted gap zones get AMBER state, blocked before hard gates
    
    v3.8 (GAP v2.3 CROSS-VERIFICATION FIXES — D1 through D10):
    D1.  Structure Removal (NON-NEGOTIABLE) now enforced in GapModule.detect()
         Mechanical gaps (no structure removed) are REJECTED at detection time.
         Uses opposing BZ/SZ zones + swing high/low pivots.
    D2.  gap_departure_range_atr: 1.0 → 1.2 (Gap v2.3 Sec 3.1 S4)
    D3.  gap_departure_body_pct: 0.50 → 0.60 (Gap v2.3 Sec 3.1 S5)
    D4.  gap_min_atr_norm: 1.0 → 0.5, gap_min_atr_low: 1.5 → 0.75 (aligned to spec)
    D5.  Full 6-dimension composite scoring (Gap v2.3 Sec 4) implemented.
         Dimensions: Structure Removal, Gap Size, Departure, Post-Gap Basing,
         HTF Alignment, Freshness. Score < 7 = blocked. Wired into pipeline.
    D6.  HTF gap hierarchy covered by existing G8 obstruction + G5 trend gate.
    D7.  GDZ/GSZ execution: LTF confirmation is execution-layer concern (Annexure).
    D8.  Gap fill tracking (Gap v2.3 Sec 13): compute_gap_fill() computes
         fill percentage. >50% fill = blocked. Wired into pipeline.
    D9.  Breakaway gap classifier (Gap v2.3 Sec 12): B1-B3 checked at detection,
         B4 (HTF) and B5 (retest) checked post-detection.
    D10. gap_score_size thresholds aligned: 2.0→0.75 (high), 1.0→0.5 (medium).
    """
    
    def __init__(self):
        self.trade_cfg = trade_config()
        self.trend_cfg = trend_config()
        
        # v3.8.9: Config sync validation — fail loud on critical field drift.
        # Two Config classes exist (models.py + trend_engine.py). Critical
        # overlapping fields must match. If they don't, the engine will
        # silently use wrong thresholds (e.g., min_rr=1 vs 2.1).
        _critical_fields = {
            'min_rr': (self.trade_cfg.min_rr, self.trend_cfg.min_rr),
            'atr_period': (self.trade_cfg.atr_period, self.trend_cfg.atr_period),
            'ema_period': (self.trade_cfg.ema_period, self.trend_cfg.ema_period),
            'basing_body_pct': (self.trade_cfg.basing_body_pct, self.trend_cfg.basing_body_pct),
            'max_base_len': (self.trade_cfg.max_base_len, self.trend_cfg.max_base_len),
            'default_target_atr': (self.trade_cfg.default_target_atr, self.trend_cfg.default_target_atr),
        }
        for field_name, (trade_val, trend_val) in _critical_fields.items():
            if trade_val != trend_val:
                raise ValueError(
                    f"CONFIG SYNC FAILURE: {field_name} differs between "
                    f"models.py ({trade_val}) and trend_engine.py ({trend_val}). "
                    f"Both must match. Fix in the source Config class."
                )
        self.zone_detector = ZoneDetector(self.trade_cfg)
        self.gap_module = GapModule()
        self.multi_zone = MultiZoneHandler(self.trade_cfg)
        self.qualifier = ZoneQualifier(self.trade_cfg)
        self.risk_calc = RiskTargetCalculator(self.trend_cfg)
        self.gate_checker = HardGateChecker(self.trend_cfg)
        self.trend_calculator = MultiTimeframeTrendCalculator()
        self.dbr_rbr_validator = DBRRBRValidator(self.trade_cfg)
        self.zone_scorer = ZoneQualityScorer()
        
        # NEW v3.4 components
        self.sliding_window = SlidingWindowManager(self.trade_cfg)
        self.zone_age_manager = ZoneAgeManager(self.trade_cfg)
        self.gap_integrator = GapZoneIntegrator(self.trade_cfg)
        self.wick_handler = WickViolationHandler(self.trade_cfg)
        self.conflict_resolver = ThreeWayConflictResolver()
        
        # NEW v3.8 components (WIRED INTO PIPELINE)
        self.nesting_classifier = ZoneNestingClassifier()
        self.obstruction_checker = ObstructionChecker()
        self.zone_scorer_v38 = ZoneScorerV38()
        self.setup_extractor = SetupExtractor(self.trend_cfg, self.gate_checker)
        self.weight_zone_score = WeightedZoneScorer(self.trend_cfg)
    
    def add_timestamp(self):
        return timedelta(hours=720).total_seconds()

    @staticmethod
    def _reduce_to_nearest(zones: List[Zone], cmp: float) -> List[Zone]:
        """
        Reduce HTF zone list to nearest BZ (below CMP) + nearest SZ (above CMP).
        
        Methodology: "In E and A, ONLY the BZ and SZ CLOSEST to CMP 
        need to be considered."
        
        v3.8.3 FIX (ISSUE-2): Uses z.distal for CMP comparison instead of
        z.proximal. This includes zones where CMP is inside the zone body
        (distal < CMP < proximal), consistent with TrendEngine.find_nearest_bz()
        and find_nearest_sz() which also use z.distal. Without this fix,
        CMP-inside zones were excluded from E/A reduction but still visible
        to the trend engine for Rule C, creating an inconsistency.
        
        Returns: list of max 2 zones (1 nearest BZ + 1 nearest SZ).
        """
        result = []
        
        # Nearest BZ: buy zone with distal below CMP (includes CMP-inside zones)
        valid_bz = [z for z in zones if z.is_buy_zone and not z.invalidated 
                     and z.distal < cmp]
        if valid_bz:
            nearest_bz = max(valid_bz, key=lambda z: z.proximal)
            result.append(nearest_bz)
        
        # Nearest SZ: sell zone with distal above CMP (includes CMP-inside zones)
        valid_sz = [z for z in zones if z.is_sell_zone and not z.invalidated 
                     and z.distal > cmp]
        if valid_sz:
            nearest_sz = min(valid_sz, key=lambda z: z.proximal)
            result.append(nearest_sz)
        
        return result

    def calculate_trend_context(
        self,
        symbol,
        last_d_time,
        time_list,
        htf_sz_overhead: bool = False,
        htf_bz_below: bool = False,
        zones_E_cascade: Optional[List[Zone]] = None,
        zones_A_cascade: Optional[List[Zone]] = None,
        exp_num = None,
        data_dir = None
    ) -> TrendContext:
        """Calculate complete trend context with trade type classification."""
        if exp_num and data_dir:
            self.trend_calculator._set_symbol_and_timeframe_future_and_commodity(time_list, symbol, last_d_time, exp_num, data_dir)
        else:
            self.trend_calculator._set_symbol_and_timeframe(time_list, symbol, last_d_time)
        return self.trend_calculator.calculate_full_context(
            htf_sz_overhead, htf_bz_below,
            zones_E_cascade, zones_A_cascade
        )

    def _find_enclosing_zone(self, x_zone: Zone, htf_zones: List[Zone]) -> Optional[Zone]:
        """
        BUG-08 FIX: Find the HTF zone that encloses this X-zone.
        
        An HTF zone encloses X-zone if X-zone's proximal-distal range
        falls within the HTF zone's proximal-distal range AND both are
        same direction (both buy or both sell).
        
        Returns the tightest enclosing HTF zone (smallest that still contains X).
        """
        enclosing = None
        for hz in htf_zones:
            if hz.invalidated:
                continue
            # Same direction check: BZ encloses BZ, SZ encloses SZ
            if hz.is_buy_zone != x_zone.is_buy_zone:
                continue
            # Check containment: X-zone range within HTF zone range
            # if hz.low_edge <= x_zone.low_edge and hz.high_edge >= x_zone.high_edge:
            #     if enclosing is None or hz.zone_height < enclosing.zone_height:
            #         enclosing = hz  # Pick tightest enclosure
            # Check containment: X-zone ORDER AREA within HTF zone range
            # BUG-37: Use base boundaries (pre-BUG-32) for containment.
            # The extended distal is for SL protection, not zone location.
            if hz.low_edge <= x_zone.low_edge_base and hz.high_edge >= x_zone.high_edge_base:
                if enclosing is None or hz.zone_height < enclosing.zone_height:
                    enclosing = hz  # Pick tightest enclosure

        return enclosing

    def _set_timeframe_candle_series(self, time_list, tick, last_d_time) -> CandleSeries:
        self.csv_path_E = os.path.join(stock_data_dir_config.indian_stock_data_dir, 'latest_data_csv', f'{tick}_{time_list[0]}.csv')
        self.csv_path_A = os.path.join(stock_data_dir_config.indian_stock_data_dir, 'latest_data_csv', f'{tick}_{time_list[1]}.csv')
        self.csv_path_X = os.path.join(stock_data_dir_config.indian_stock_data_dir, 'latest_data_csv', f'{tick}_{time_list[-1]}.csv')    
        self.last_d_time = last_d_time

    def _set_timeframe_candle_series_future_and_commodity(self, time_list, tick, last_d_time, exp_num, data_dir) -> CandleSeries:
        self.csv_path_E = os.path.join(data_dir, 'latest_data_csv', f'{tick}_{exp_num}_{time_list[0]}.csv')
        self.csv_path_A = os.path.join(data_dir, 'latest_data_csv', f'{tick}_{exp_num}_{time_list[1]}.csv')
        self.csv_path_X = os.path.join(data_dir, 'latest_data_csv', f'{tick}_{exp_num}_{time_list[-1]}.csv')    
        self.last_d_time = last_d_time

    def get_candle_series_data(self, csv_path, last_d_time):
        print(csv_path, "ffffffffffffffffffffffffffffffffffffff")
        df, violation_df = load_preprocess_data(csv_path, last_d_time)
        cs = CandleSeries(
            o=df['open'].tolist(),
            h=df['high'].tolist(),
            l=df['low'].tolist(),
            c=df['close'].tolist(),
            ts=df['unix_timestamp'].tolist()
        )
        print("Candle series data loaded for setup engine", df.iloc[-1]['close'], csv_path)
        return cs

    def get_cmp(self, csv_path, last_d_time):
        df, violation_df = load_preprocess_data(csv_path, last_d_time)
        print("Violation df for setup engine", violation_df.iloc[-1]['close'], csv_path)
        return violation_df.iloc[-1]['close']
    
    def validate_reversal_pattern(self, cs: CandleSeries, zone: Zone, 
                                  pattern_type: PatternType, 
                                  htf_zone_present: bool,
                                  quadrant: Optional[Quadrant]) -> ReversalPattern:
        """Validate DBR/RBR pattern for reversal trades."""
        if pattern_type == PatternType.DBR:
            return self.dbr_rbr_validator.validate_dbr(cs, zone, htf_zone_present, quadrant)
        elif pattern_type == PatternType.RBR:
            return self.dbr_rbr_validator.validate_rbr(cs, zone, htf_zone_present, quadrant)
        else:
            return ReversalPattern(
                pattern_type=PatternType.NONE,
                initial_move_start_idx=0,
                initial_move_end_idx=0,
                base_start_idx=0,
                base_end_idx=0,
                departure_idx=0,
                is_valid=False,
                validation_failures=["No pattern type specified"]
            )

    def _check_entry_reachability(self, zone: Zone, cmp: float,
                                   a_zones: List[Zone],
                                   e_zones: List[Zone]
                                   ) -> Tuple[bool, Optional[Zone]]:
        """
        BUG-34: Check if price can reach zone entry without passing through
        an unviolated same-direction HTF zone.
        AUD-05: Excludes unaccepted gap zones (Trend v4.4.2 Sec 14).
        v3.8.7: Excludes the zone's own nesting parent (enclosing E/A zone).
        The nesting parent is the REASON the zone has high probability —
        it is not an obstacle to entry.
        """
        htf_zones = list(a_zones) + list(e_zones)
        # Exclude unaccepted gap zones
        valid_htf = [z for z in htf_zones
                     if not (z.ztype in (ZoneType.GDZ, ZoneType.GSZ)
                             and not z.session_accepted)]
        # v3.8.7: Exclude nesting parents — these are enabling, not blocking
        _enc_ids = set()
        if zone.enclosing_e_zone:
            _enc_ids.add(zone.enclosing_e_zone.zone_id)
        if zone.enclosing_a_zone:
            _enc_ids.add(zone.enclosing_a_zone.zone_id)
        valid_htf = [z for z in valid_htf if z.zone_id not in _enc_ids]
        
        if zone.is_buy_zone and zone.proximal < cmp:
            blockers = [z for z in valid_htf
                        if z.is_buy_zone and not z.invalidated
                        and zone.proximal < z.proximal < cmp]
            if blockers:
                return False, min(blockers, key=lambda z: z.proximal)
        elif zone.is_sell_zone and zone.proximal > cmp:
            blockers = [z for z in valid_htf
                        if z.is_sell_zone and not z.invalidated
                        and cmp < z.proximal < zone.proximal]
            if blockers:
                return False, max(blockers, key=lambda z: z.proximal)
        return True, None
    
    def run(
        self,
        symbol: str, time_list, 
        last_d_time,
        segment: Optional[str] = None,
        days_to_expiry: Optional[int] = None,
        exp_num = None, is_future: bool = None, is_cash: bool = None
    ) -> Dict:
        """
        Run complete pipeline v3.8.
        
        Returns comprehensive result with:
        - Zones per timeframe (BZ/SZ + GDZ/GSZ)
        - Trend context with trade types
        - DBR/RBR validation results
        - Zone quality scores (v3.8 enhanced)
        - Execution recommendations
        - v3.4: Zone age classifications
        - v3.4: Wick violation status
        - v3.4: Three-way conflict resolution
        - v3.8: Nesting tiers per X zone
        - v3.8: Obstruction check results
        - v3.8: Quadrant-enforced permissions
        - v3.8: CMP-inside-HTF-zone flags
        - v3.8: X-TF regime bias modifier
        - v3.8 FIX: Gap zone detection (GDZ/GSZ) fully wired
        - v3.8 FIX: VolatilityRegime computed per TF for gap detection
        - v3.8 FIX: Session acceptance enforced for gap zones
        """
        # ── v3.8.7: Compute max entry distance from segment/expiry ────
        _max_entry_dist = None
        if segment and segment.upper() != "CASH":
            if days_to_expiry is not None and days_to_expiry <= 7:
                _max_entry_dist = 0.01 if segment.upper() in ("FUTURES", "NSE_FO") else 0.015
            elif days_to_expiry is not None and days_to_expiry <= 14:
                _max_entry_dist = 0.02 if segment.upper() in ("FUTURES", "NSE_FO") else 0.03
            elif segment.upper() in ("FUTURES", "NSE_FO"):
                _max_entry_dist = 0.05  # Far-dated futures
            # MCX >14 days or no expiry: no limit


        logger.info(f"Running SD Engine Pipeline for {symbol}")
        data_dir = stock_data_dir_config.indian_stock_future_data_dir if is_future == True else stock_data_dir_config.indian_commodity_data
        if exp_num and data_dir:
            self._set_timeframe_candle_series_future_and_commodity(time_list, symbol, last_d_time, exp_num, data_dir)
        else:
            self._set_timeframe_candle_series(time_list, symbol, last_d_time)
        cs_E = self.get_candle_series_data(self.csv_path_E, last_d_time)
        cs_A = self.get_candle_series_data(self.csv_path_A, last_d_time)
        cs_X = self.get_candle_series_data(self.csv_path_X, last_d_time)

        current_cmp = float(self.get_cmp(self.csv_path_X, last_d_time))
        logger.info(f"cmp aquired....................{current_cmp}")
        cmp_t_stamp = int(cs_X.ts[-1])

        _MIN_BARS_REJECT = {TF.E: 7, TF.A: 15, TF.X: 50}
        _MIN_BARS_WARN = {TF.E: 20, TF.A: 40, TF.X: 100}
        for _tf, _cs, _label in [(TF.E, cs_E, 'E'), (TF.A, cs_A, 'A'), (TF.X, cs_X, 'X')]:
            if _cs.n < _MIN_BARS_REJECT[_tf]:
                return {
                    'symbol': symbol, 'data_insufficient': True,
                    'data_insufficient_reason': f"{_label}-TF has {_cs.n} bars, minimum {_MIN_BARS_REJECT[_tf]} required",
                    'trend_context': None, 'zones_E': [], 'zones_A': [], 'zones_X': [],
                    'excluded_zone_ids': [], 'dbr_results': {}, 'rbd_results': {},
                    'atr_E': 0, 'atr_A': 0, 'atr_X': 0,
                    'htf_sz_overhead': False, 'htf_bz_below': False,
                    'three_way_resolution': None,
                    'zones_E_unfiltered_count': 0, 'zones_A_unfiltered_count': 0, 'zones_X_unfiltered_count': 0,
                    'cmp_inside_htf_sz': False, 'cmp_inside_htf_bz': False, 'adjusted_bias': None,
                    'gap_zones_E_count': 0, 'gap_zones_A_count': 0, 'gap_zones_X_count': 0,
                    'vol_regime_E': None, 'vol_regime_A': None, 'vol_regime_X': None,
                    'best_setup_long': None, 'best_setup_short': None, 'setup_cascade_log': [],
                }
        _data_warnings = []
        for _tf, _cs, _label in [(TF.E, cs_E, 'E'), (TF.A, cs_A, 'A'), (TF.X, cs_X, 'X')]:
            if _cs.n < _MIN_BARS_WARN[_tf]:
                _data_warnings.append(f"{_label}-TF has {_cs.n} bars (recommended {_MIN_BARS_WARN[_tf]}+)")



        # ============================================================
        # STEP 1: Detect regular zones (BZ/SZ)
        # ============================================================
        logger.info(f"Detecting regular zones for {symbol}")
        # zones_E = self.zone_detector.detect(symbol, TF.E, cs_E)
        # zones_A = self.zone_detector.detect(symbol, TF.A, cs_A)
        result_E, all_zones_E = process_trend_zones(self.csv_path_E, TF.E, self.last_d_time)
        result_A, all_zones_A = process_trend_zones(self.csv_path_A, TF.A, self.last_d_time)
        zones_E = result_E['BUY'] + result_E['SELL']
        zones_A = result_A['BUY'] + result_A['SELL']
        zones_X = self.zone_detector.detect(symbol, TF.X, cs_X)
        
        # ============================================================
        # STEP 2: Calculate ATR for each TF
        # ============================================================
        logger.info(f"Calculating ATR for {symbol}")
        atr_E = atr(cs_E.h, cs_E.l, cs_E.c, self.trade_cfg.atr_period)
        atr_A = atr(cs_A.h, cs_A.l, cs_A.c, self.trade_cfg.atr_period)
        atr_X = atr(cs_X.h, cs_X.l, cs_X.c, self.trade_cfg.atr_period)
        
        atr_E_val = atr_E[-1] if atr_E[-1] else 1.0
        atr_A_val = atr_A[-1] if atr_A[-1] else 1.0
        atr_X_val = atr_X[-1] if atr_X[-1] else 1.0
        
        # ============================================================
        # STEP 2b: Detect gap zones (GDZ/GSZ) — BUG-1 FIX
        # REF: Gap Zones v2.3 Sec 2.1 (OHLC definition),
        #      Methodology v3.8 Sec 11 (Gap Zones),
        #      Trend v4.4 Sec 14 (Gap Zone Integration)
        # ============================================================
        logger.info(f"Detecting gap zones for {symbol}")
        # Compute VolatilityRegime per TF (v3.1: EMA-20 based)
        # ATR output has None for warmup indices — extract non-None tail for EMA
        def _safe_atr_ema(atr_vals, ema_period):
            """Compute EMA over non-None ATR values, return last value."""
            valid = [v for v in atr_vals if v is not None]
            if len(valid) < ema_period:
                return valid[-1] if valid else 1.0
            ema_vals = ema(valid, ema_period)
            return ema_vals[-1] if ema_vals[-1] is not None else (valid[-1] if valid else 1.0)
        
        atr_E_ema_val = _safe_atr_ema(atr_E, self.trend_cfg.ema_period)
        atr_A_ema_val = _safe_atr_ema(atr_A, self.trend_cfg.ema_period)
        atr_X_ema_val = _safe_atr_ema(atr_X, self.trend_cfg.ema_period)
        
        vol_E = volatility_regime(atr_E_val, atr_E_ema_val, self.trade_cfg)
        vol_A = volatility_regime(atr_A_val, atr_A_ema_val, self.trade_cfg)
        vol_X = volatility_regime(atr_X_val, atr_X_ema_val, self.trade_cfg)


        ema_20_X_series = ema(cs_X.c, self.trend_cfg.ema_period)
        ema_20_X_val = ema_20_X_series[-1] if ema_20_X_series and ema_20_X_series[-1] is not None else None
        # Detect GDZ/GSZ per TF
        # D1 FIX: Pass opposing BZ/SZ zones for structure removal validation
        # Gap v2.3 Sec 3.1 S1: Structure removal is NON-NEGOTIABLE
        gap_zones_E = self.gap_module.detect(symbol, TF.E, cs_E, vol_E, opposing_zones=zones_E)
        gap_zones_A = self.gap_module.detect(symbol, TF.A, cs_A, vol_A, opposing_zones=zones_A)
        gap_zones_X = self.gap_module.detect(symbol, TF.X, cs_X, vol_X, opposing_zones=zones_X)

        
        # Merge gap zones into main zone lists (GDZ/GSZ participate
        # alongside BZ/SZ — they share the same qualification pipeline)
        # zones_E = zones_E + gap_zones_E
        # zones_A = zones_A + gap_zones_A
        zones_X = zones_X + gap_zones_X

        zones_E_raw_for_cascade = list(zones_E + gap_zones_E)
        zones_A_raw_for_cascade = list(zones_A + gap_zones_A)


        logger.info(f"Applying sliding window filter for {symbol}")
        # ============================================================
        # STEP 3: Apply sliding window filter to ALL zones (BZ/SZ/GDZ/GSZ)
        # REF: Trend v4.4 Sec 13.1 (Sliding Window Boundaries)
        # ============================================================
        zones_E_filtered = self.sliding_window.filter_zones_in_window(
            zones_E, cs_E.n - 1, cs_E.cmp, atr_E_val
        )
        zones_A_filtered = self.sliding_window.filter_zones_in_window(
            zones_A, cs_A.n - 1, cs_A.cmp, atr_A_val
        )
        zones_X_filtered = self.sliding_window.filter_zones_in_window(
            zones_X, cs_X.n - 1, cs_X.cmp, atr_X_val
        )
        # v3.8.8 Fix B: E/A Zone Violation Check
        for zone in zones_E_filtered:
            self.qualifier.update_violation(cs_E, zone)
        for zone in zones_A_filtered:
            self.qualifier.update_violation(cs_A, zone)
        zones_E_filtered = [z for z in zones_E_filtered if not z.invalidated]
        zones_A_filtered = [z for z in zones_A_filtered if not z.invalidated]


        zones_E_filtered = self._reduce_to_nearest(zones_E_filtered, cs_E.cmp)
        zones_A_filtered = self._reduce_to_nearest(zones_A_filtered, cs_A.cmp)

        logger.info(f"Applying sliding window filter for session acceptance for {symbol}")
        # ============================================================
        # STEP 3b: Gap zone session acceptance — BUG-1 FIX (continued)
        # REF: Gap Zones v2.3 Sec 3.1 (Session Acceptance),
        #      Methodology v3.8 Sec 11.2 (Session Acceptance)
        # Gap zones must pass session acceptance BEFORE participating
        # in any downstream logic. Non-accepted gaps get AMBER state.
        # ============================================================
        for zone in zones_E_filtered + zones_A_filtered + zones_X_filtered:
            if zone.ztype in (ZoneType.GDZ, ZoneType.GSZ):
                # break_level: proximal for same-session close check
                break_level = zone.proximal
                cs_for_tf = {TF.E: cs_E, TF.A: cs_A, TF.X: cs_X}[zone.tf]
                accepted = self.gap_module.check_session_acceptance(
                    cs_for_tf, zone, break_level
                )
                if not accepted:
                    # Gap zone remains AMBER — will be filtered from
                    # Rule C by gap_integrator.is_valid_for_rule_c()
                    # and blocked from hard gates by session_accepted check
                    zone.state = ZoneState.AMBER
                    zone.block_reason = "AWAITING_SESSION_ACCEPTANCE"
        
        # ============================================================
        # STEP 3c: Update zone ages (ALL zones including GDZ/GSZ)
        # REF: Trend v4.4 Sec 13.2 (Zone Age Classification)
        # ============================================================
        for zone in zones_E_filtered:
            self.zone_age_manager.update_zone_age(zone, cs_E.n - 1, cs_E.cmp, atr_E_val)
        for zone in zones_A_filtered:
            self.zone_age_manager.update_zone_age(zone, cs_A.n - 1, cs_A.cmp, atr_A_val)
        for zone in zones_X_filtered:
            self.zone_age_manager.update_zone_age(zone, cs_X.n - 1, cs_X.cmp, atr_X_val)
        
        # ============================================================
        # STEP 3d: Filter gap zones not valid for Rule C
        # REF: Trend v4.4 Sec 14 (GDZ/GSZ with session_accepted=False
        #      excluded from Rule C sliding window dominance)
        # ============================================================
        logger.info(f"Filtering gap zones not valid for Rule C for {symbol}")
        zones_E_rule_c = [z for z in zones_E_filtered if self.gap_integrator.is_valid_for_rule_c(z)]
        zones_A_rule_c = [z for z in zones_A_filtered if self.gap_integrator.is_valid_for_rule_c(z)]
        
        # Detect HTF zone positions for veto
        logger.info(f"Detecting HTF zone positions for veto for {symbol}")
        htf_sz_overhead = any(z.is_sell_zone and z.distal > current_cmp for z in zones_E_rule_c if not z.invalidated)
        htf_bz_below = any(z.is_buy_zone and z.distal < current_cmp for z in zones_E_rule_c if not z.invalidated)
        
        # v3.8: Detect CMP inside HTF zone conditions
        cmp_inside_htf_sz = any(
            z.is_sell_zone and not z.invalidated and
            min(z.proximal, z.distal) <= current_cmp <= max(z.proximal, z.distal)
            for z in (zones_E_filtered + zones_A_filtered)
        )
        cmp_inside_htf_bz = any(
            z.is_buy_zone and not z.invalidated and
            min(z.proximal, z.distal) <= current_cmp <= max(z.proximal, z.distal)
            for z in (zones_E_filtered + zones_A_filtered)
        )
        print("cmp_inside_htf_sz", cmp_inside_htf_sz, current_cmp)
        print("cmp_inside_htf_bz", cmp_inside_htf_bz, current_cmp)
        
        # Calculate trend context (using Rule C filtered zones)
        logger.info(f"Calculating trend context for {symbol}")
        if is_cash == True:
            trend_context = self.calculate_trend_context(symbol, last_d_time, time_list, htf_sz_overhead, htf_bz_below, zones_E_raw_for_cascade, zones_A_raw_for_cascade)
        else:
            trend_context = self.calculate_trend_context(symbol, last_d_time, time_list, htf_sz_overhead, htf_bz_below, zones_E_raw_for_cascade, zones_A_raw_for_cascade, exp_num, data_dir)

        # v3.4: Check for three-way conflict and resolve
        three_way_resolution = self.conflict_resolver.resolve(
            trend_context.regime_E, trend_context.regime_A, trend_context.regime_X
        )
        logger.info(f"Three-way resolution for {symbol}: {three_way_resolution}")
        # v3.8: Apply X-TF regime as bias modifier when E=SW, A=SW
        
        if three_way_resolution.get('resolved', False):
            if not three_way_resolution.get('allow_long', True):
                trend_context.trade_type_long = TradeType.NO_TRADE
            if not three_way_resolution.get('allow_short', True):
                trend_context.trade_type_short = TradeType.NO_TRADE
       
        adjusted_bias = trend_context.bias
        if trend_context.regime_E == TrendRegime.SW and trend_context.regime_A == TrendRegime.SW:
            if trend_context.regime_X == TrendRegime.DN:
                adjusted_bias = "Short-Leaning (X=DN reinforcement)"
            elif trend_context.regime_X == TrendRegime.UP:
                adjusted_bias = "Long-Leaning (X=UP reinforcement)"
                # else X=SW: remains Neutral
        
        trend_context.bias = adjusted_bias


        # BUG-TREND-DECLINE FIX: Momentum override for strong declines.
        # V1 (v3.8.6): SW→DN when below EMA + decline > 15%.
        # V2: Also override UP→DN when decline is severe (>15% + >5% below EMA).
        # CRUDEOILM: E=UP (Rule B, only BZ on Weekly, no SZ formed in V-top),
        # but CMP crashed 20% in one week. Without UP→DN override, shorts
        # are blocked by HTF veto ("E=UP + A=DN = CONFLICT").
        # _ema_chk = ema(cs_X.c, self.trade_cfg.ema_period)
        # _ema_last = _ema_chk[-1] if _ema_chk and _ema_chk[-1] else None
        # if _ema_last and _ema_last > 0:
        #     _below = (_ema_last - cs_X.cmp) / _ema_last
        #     _peak = max(cs_X.h[-min(100, len(cs_X.h)):])
        #     _decl = (_peak - cs_X.cmp) / _peak if _peak > 0 else 0
        #     if _below > 0.05 and _decl > 0.15:
        #         if trend_context.regime_E in (TrendRegime.SW, TrendRegime.UP):
        #             trend_context.regime_E = TrendRegime.DN
        #         if trend_context.regime_A in (TrendRegime.SW, TrendRegime.UP):
        #             trend_context.regime_A = TrendRegime.DN
        #         trend_context.bias = "Bearish (momentum decline override)"
        #         # Recompute trade types with overridden regimes
        #         _override_perms = self.trend_calculator.execution_engine.get_permissions(
        #             trend_context.regime_E, trend_context.regime_A, trend_context.quadrant_X)
        #         trend_context.trade_type_long = _override_perms['trade_type_long']
        #         trend_context.trade_type_short = _override_perms['trade_type_short']
        #         trend_context.dbr_required = _override_perms['dbr_required']
        #         trend_context.rbd_required = _override_perms['rbd_required']
        #         # Reconcile allow flags and permitted_setup with new permissions
        #         trend_context.allow_long = _override_perms['allow_long'] and not trend_context.htf_veto_longs
        #         trend_context.allow_short = _override_perms['allow_short'] and not trend_context.htf_veto_shorts
        #         _ol = _override_perms['long_setup'] if trend_context.allow_long else "BLOCKED"
        #         _os = _override_perms['short_setup'] if trend_context.allow_short else "BLOCKED"
        #         trend_context.permitted_setup = f"Long: {_ol} | Short: {_os}"
        # else:
            # adjusted_bias = trend_context.bias
        # Process multi-zone interactions
        zones_X_processed, excluded = self.multi_zone.process_overlaps(zones_X_filtered, atr_X_val)
        self.multi_zone.find_consecutive_stack(zones_X_processed, atr_X_val)

        zones_X_processed, all_zone_X = process_qualified_zones_setup(self.csv_path_X, TF.X, self.last_d_time)
        htf_zones = zones_E_filtered + zones_A_filtered
        # Qualify zones and calculate scores
        for zone in zones_X_processed:
            # self.qualifier.update_violation(cs_X, zone)
            # self.qualifier.update_retest(cs_X, zone)
            
            # v3.4: Update wick violation status
            self.wick_handler.update_wick_status(cs_X, zone)
            
            # D8: Gap fill tracking (Gap v2.3 Sec 13)
            # Fill degrades zone non-linearly. Must happen before scoring.
            if zone.ztype in (ZoneType.GDZ, ZoneType.GSZ):
                self.gap_module.compute_gap_fill(cs_X, zone)
            
            # Find opposing zone for structure removal and scoring
     

            opposing = self.qualifier.get_preceding_zones(zone, all_zone_X)
            opposing = opposing[0] if opposing else None
    
            
            opposing_distal = opposing.distal if opposing else None
            opposing_prox = opposing.proximal if opposing else None
            self.qualifier.compute_structure_removal(cs_X, zone, opposing_distal, opposing_prox)
            # Score zone (age penalty already set by zone_age_manager)
            self.zone_scorer.score_zone(zone, trend_context.regime_E, opposing, atr_X_val, ema_20_X_val)
            
            # D5: Update gap composite score dimensions 5+6 (Gap v2.3 Sec 4)
            # Requires trend context (dim 5: HTF alignment) and retest_count (dim 6: freshness)
            if zone.ztype in (ZoneType.GDZ, ZoneType.GSZ):
                htf_aligned = (
                    (zone.is_buy_zone and trend_context.regime_E == TrendRegime.UP) or
                    (zone.is_sell_zone and trend_context.regime_E == TrendRegime.DN)
                )
                htf_neutral = trend_context.regime_E == TrendRegime.SW
                self.gap_module.update_composite_score_htf_freshness(zone, htf_aligned, htf_neutral)
            
            # v3.8: Enhanced zone scoring
            zone_v38_score = self.zone_scorer_v38.calculate_score(zone)
            
            # v3.8: Nesting tier classification
            # nesting_tier = self.nesting_classifier.classify(
            #     zone, zones_A_filtered, zones_E_filtered
            # )
            tier, debug = self.nesting_classifier.classify_with_debug(zone, zones_A_filtered, zones_E_filtered)
            zone.nesting_tier = tier
            nesting_tier = tier
            zone.overlap_ratio = max(
                debug["best_a_overlap"],
                debug["best_e_overlap"],
            )
            zone.zone_v38_score = zone_v38_score
        
            
            zone.enclosing_e_zone = self._find_enclosing_zone(zone, zones_E_filtered)
            zone.enclosing_a_zone = self._find_enclosing_zone(zone, zones_A_filtered)

            zone.zone_in_zone = (
                nesting_tier is not None and 
                nesting_tier in (ZoneNestingTier.TIER_1, ZoneNestingTier.TIER_2)
            )

            direction = "LONG" if zone.is_buy_zone else "SHORT"
            target_price = opposing.proximal if opposing else (
                current_cmp * 1.05 if zone.is_buy_zone else current_cmp * 0.95
            )

            obstruction_clear, blocking_zone = self.obstruction_checker.is_path_clear(
                direction, current_cmp, target_price,
                zones_A_filtered, zones_E_filtered
            )
            zone.obstruction_clear = obstruction_clear
            zone.blocking_zone = blocking_zone
            

            # ── BUG-34: Entry path reachability (G6c) ───────────────
            entry_path_clear, entry_blocking_zone = self._check_entry_reachability(
                zone, cs_X.cmp, zones_A_filtered, zones_E_filtered
            )
            zone.entry_path_clear = entry_path_clear
            zone.entry_blocking_zone = entry_blocking_zone   # B-W4: retain object for G6c gate
            zone.entry_blocking_zone_id = (
                entry_blocking_zone.zone_id if entry_blocking_zone else None
            )
            # Calculate risk/target
            partner_distal = None
            # if zone.is_part_of_consecutive and zone.consecutive_partner_id:
            #     partner = next(
            #         (z for z in zones_X_processed if z.zone_id == zone.consecutive_partner_id),
            #         None
            #     )
            #     if partner:
            #         partner_distal = partner.distal

            
            risk_target = self.risk_calc.calculate(zone, current_cmp, all_zone_X, atr_X_val, partner_distal, zones_X_processed, htf_zones)
            zone.entry = risk_target.entry
            zone.target_price = risk_target.target
            zone.stop_price = risk_target.stop
            zone.rr_ratio = risk_target.rr
            zone.target_mode = risk_target.target_mode
            zone.htf_target_price = risk_target.htf_target_price
            
            # Determine trade type for quadrant enforcement
            # trade_type_for_zone = (
            #     trend_context.trade_type_long if zone.is_buy_zone 
            #     else trend_context.trade_type_short
            # )
            
            # Determine relevant quadrant (use E quadrant for HTF context)
            relevant_quadrant = trend_context.quadrant_X
            
            # Check hard gates (v3.8: all 10 gates)
            # BUG-1 FIX: Gap zones without session acceptance stay AMBER
            # REF: Methodology v3.8 Sec 11.2, Gap Zones v2.3 Sec 3.1
            if zone.ztype in (ZoneType.GDZ, ZoneType.GSZ) and not zone.session_accepted:
                zone.state = ZoneState.AMBER
                zone.block_reason = "AWAITING_SESSION_ACCEPTANCE"
                continue  # Skip hard gates — gap zone not yet accepted
            
            # D5: Gap composite score gate (Gap v2.3 Sec 4 / SC)
            # Score < 7 = low-grade structural gap, not tradeable
            # gap_is_mechanical = True if score dropped below threshold post-HTF update
            if zone.ztype in (ZoneType.GDZ, ZoneType.GSZ):
                if zone.gap_is_mechanical or not zone.gap_is_structural:
                    zone.state = ZoneState.RED
                    zone.block_reason = f"GAP_COMPOSITE_SCORE_LOW ({zone.gap_composite_score})"
                    continue  # Skip hard gates — gap zone not structural grade
                # D8: Block gap zones with >50% fill (Gap v2.3 Sec 13)
                if zone.gap_fill_pct > 0.50:
                    zone.state = ZoneState.RED
                    zone.block_reason = f"GAP_FILL_DEGRADED ({zone.gap_fill_pct:.0%})"
                    continue
            
            # trade_allowed = trend_context.allow_long if zone.is_buy_zone else trend_context.allow_short

            # passed, reason = self.gate_checker.check_all(
            #     zone, risk_target, trade_allowed, TF.X,
            #     nesting_tier=nesting_tier,
            #     obstruction_clear=obstruction_clear,
            #     obstruction_zone=blocking_zone,
            #     quadrant=relevant_quadrant,
            #     trade_type=trade_type_for_zone,
            #     zone_v38_score=zone_v38_score,
            #     cmp_inside_htf_sz=cmp_inside_htf_sz,
            #     cmp_inside_htf_bz=cmp_inside_htf_bz
            # )
            
            passed, reason = self.gate_checker.check_gates_pre_rr(
                zone, trend_context, TF.X, cmp=current_cmp,
                nesting_tier=nesting_tier,
                obstruction_clear=obstruction_clear,
                obstruction_zone=blocking_zone,
                quadrant=relevant_quadrant,
                zone_v38_score=zone_v38_score,
                cmp_inside_htf_sz=cmp_inside_htf_sz,
                cmp_inside_htf_bz=cmp_inside_htf_bz,
                entry_path_clear=entry_path_clear,
                entry_blocking_zone=entry_blocking_zone
            )

            if passed:
                _n_warn = len(zone.gate_warnings)
                if _n_warn <= 3:
                    zone.state = ZoneState.GREEN
                else:
                    zone.state = ZoneState.AMBER
                    zone.block_reason = f"AMBER ({_n_warn} warnings)"
            else:
                zone.state = ZoneState.RED
                zone.block_reason = reason
            print("reason", zone.ztype, zone.proximal, zone.distal, reason)
            zone.final_weighted_score = self.weight_zone_score.compute_weighted_score(zone, current_cmp, trend_context, ema_20=ema_20_X_val)
        # ============================================================
        # POST-LOOP: Consecutive structure removal propagation
        # REF: Consecutive zones share institutional intent. If partner
        #      zone's departure removed structure, this zone inherits it.
        #      Must run after main loop so ALL zones have structure
        #      removal computed before propagation.
        # ============================================================
        g8_blocked = [z for z in zones_X_processed if z.state == ZoneState.RED and z.block_reason == "G8_STRUCTURE_NOT_REMOVED" and (z.is_part_of_consecutive or z.is_part_of_overlapping)]
        
        for zone in g8_blocked:
            partner_ids = (
                ([zone.consecutive_partner_id] if zone.consecutive_partner_id is not None else []) +
                (zone.overlapping_partners_id or [])
            )
            partner = next(
                (z for z in zones_X_processed if z.zone_id in partner_ids),
                None
            )

            # partner = next(
            #     (z for z in zones_X_processed 
            #      if z.zone_id in {zone.consecutive_partner_id, all(zone.overlapping_partners_id)}),
            #     None
            # )
            if partner and partner.removes_structure:
                # Inherit structure removal from consecutive partner
                zone.removes_structure = True
                zone.removes_structure_type = (
                    f"CONSECUTIVE_INHERITED ({partner.zone_id}: "
                    f"{partner.removes_structure_type})"
                )
                
                # Re-run ALL gates for this zone (not just G8)
                nesting_tier = zone.nesting_tier
                zone_v38_score = zone.zone_v38_score
                obstruction_clear = zone.obstruction_clear
                blocking_zone = getattr(zone, 'blocking_zone', None)
                relevant_quadrant = trend_context.quadrant_X

                passed, reason = self.gate_checker.check_gates_pre_rr(
                    zone, trend_context, TF.X, cmp=current_cmp,
                    nesting_tier=nesting_tier,
                    obstruction_clear=obstruction_clear,
                    obstruction_zone=blocking_zone,
                    quadrant=relevant_quadrant,
                    zone_v38_score=zone_v38_score,
                    cmp_inside_htf_sz=cmp_inside_htf_sz,
                    cmp_inside_htf_bz=cmp_inside_htf_bz,
                    entry_path_clear=getattr(zone, 'entry_path_clear', True),
                    entry_blocking_zone=getattr(zone, 'entry_blocking_zone', None)
                )
                
                if passed:
                    _n_w = len(zone.gate_warnings)
                    if _n_w <= 3:
                        zone.state = ZoneState.GREEN
                    else:
                        zone.state = ZoneState.AMBER
                        zone.block_reason = f"AMBER ({_n_w} warnings)"
                    if zone.state == ZoneState.GREEN:
                        zone.block_reason = None
                else:
                    # Still blocked by a different gate
                    zone.block_reason = reason




        # Validate DBR/RBR if required
        dbr_results = []
        rbd_results = []
        
        if trend_context.dbr_required:
            for zone in zones_X_processed:
                if zone.is_buy_zone and zone.state == ZoneState.GREEN:

                    if zone.nesting_tier in (ZoneNestingTier.TIER_1, ZoneNestingTier.TIER_2):
                        zone.pattern_validated = True
                        zone.associated_pattern = zone.zone_pattern
                        continue
                    
                    result = self.validate_reversal_pattern(
                        cs_X, zone, PatternType.DBR, htf_bz_below, trend_context.quadrant_X
                    )
                    dbr_results.append(result)
                    zone.associated_pattern = PatternType.DBR
                    zone.pattern_validated = result.is_valid

                    if not result.is_valid:
                        zone.state = ZoneState.RED
                        zone.block_reason = "PATTERN_NOT_VALIDATED (DBR required but failed)"
        
        if trend_context.rbd_required:
            for zone in zones_X_processed:
                if zone.is_sell_zone and zone.state == ZoneState.GREEN:

                    if zone.nesting_tier in (ZoneNestingTier.TIER_1, ZoneNestingTier.TIER_2):
                        zone.pattern_validated = True
                        zone.associated_pattern = zone.zone_pattern
                        continue

                    result = self.validate_reversal_pattern(
                        cs_X, zone, PatternType.RBD, htf_sz_overhead, trend_context.quadrant_X
                    )
                    rbd_results.append(result)
                    zone.associated_pattern = PatternType.RBD
                    zone.pattern_validated = result.is_valid

                    if not result.is_valid:
                        zone.state = ZoneState.RED
                        zone.block_reason = "PATTERN_NOT_VALIDATED (RBD required but failed)"
        

        # =====================================================================
        # STEP 19: Setup Selection — 9-Dimension Weighted Score + G9 Cascade
        # E/S/T computed in loop (for RR scoring). Weights from Config.ws_*:
        #   20% trend + 18% base_quality + 16% freshness + 15% RR
        #   + 12% departure + 10% confluence + 4% EMA + 3% age + 2% proximity
        # Hard Gates: G1→G2→G5→G6→G7→G3→G4→G8→G10 (pre-RR, in loop)
        # G9 (RR >= 2.1) checked on top-ranked zone; cascades if fail.
        # G6 may have adjusted target to obstruction boundary.
        # Output: ONE SetupPayload per direction, or None.
        # REF: Methodology v3.8.1 Sec 9.3, Annexure v1.2 Sec 4-5
        # =====================================================================
        cmp = cs_X.c[-1]  # Current Market Price = last close on Execute TF

        if is_cash == True:
            best_long, best_short, cascade_log = self.setup_extractor.extract(
                zones_X=zones_X_processed,
                trend_context=trend_context,
                cmp=cmp,
                atr_X=atr_X_val,
                ema_20=ema_20_X_val,
                max_entry_distance_pct=_max_entry_dist,
                zones_A=zones_A_filtered
            )
        else:
            best_long, best_short, cascade_log = self.setup_extractor.extract(
                zones_X=zones_X_processed,
                trend_context=trend_context,
                cmp=current_cmp,
                atr_X=atr_X_val,
                ema_20=ema_20_X_val,
                max_entry_distance_pct=_max_entry_dist,
                zones_A=zones_A_filtered
            )

        # ===== Phase-0 SideEnablementPolicy gate (single chokepoint, fail-closed SELL) =====
        # Replaces ad-hoc ENABLE_FC_SELL / CASH_SHORT_TFS gating. A disabled side can never
        # reach order construction. Every drop is audited.
        _seg = resolve_segment(segment, is_cash, is_future)
        if best_short is not None and not SIDE_POLICY.is_enabled(_seg, Side.SHORT):
            logger.info(f"[SIDE_POLICY] SHORT dropped | segment={_seg} | "
                        f"zone={getattr(best_short, 'zone_id', None)} | reason=side_disabled")
            best_short = None
        if best_long is not None and not SIDE_POLICY.is_enabled(_seg, Side.LONG):
            logger.info(f"[SIDE_POLICY] LONG dropped | segment={_seg} | reason=side_disabled")
            best_long = None
        # ===================================================================================

        return {
            'symbol': symbol,
            'trend_context': trend_context,
            'zones_E': zones_E_filtered,
            'zones_A': zones_A_filtered,
            'zones_X': zones_X_processed,
            'excluded_zone_ids': excluded,
            'dbr_results': dbr_results,
            'rbd_results': rbd_results,
            'atr_E': atr_E_val,
            'atr_A': atr_A_val,
            'atr_X': atr_X_val,
            'htf_sz_overhead': htf_sz_overhead,
            'htf_bz_below': htf_bz_below,
            # v3.4 additions
            'three_way_resolution': three_way_resolution,
            'zones_E_unfiltered_count': len(zones_E),
            'zones_A_unfiltered_count': len(zones_A),
            'zones_X_unfiltered_count': len(zones_X),
            # v3.8 additions
            'cmp_inside_htf_sz': cmp_inside_htf_sz,
            'cmp_inside_htf_bz': cmp_inside_htf_bz,
            'adjusted_bias': adjusted_bias,
            # v3.8 FIX: Gap zone tracking (BUG-1 resolved)
            'gap_zones_E_count': len(gap_zones_E),
            'gap_zones_A_count': len(gap_zones_A),
            'gap_zones_X_count': len(gap_zones_X),
            'vol_regime_E': vol_E,
            'vol_regime_A': vol_A,
            'vol_regime_X': vol_X,
            # v3.8.1 Step 19: Setup Selection + E/S/T + G4 (RESTRUCTURED)
            'best_setup_long': best_long,       # SetupPayload or None
            'best_setup_short': best_short,      # SetupPayload or None
            'setup_cascade_log': cascade_log,    # List[dict] — cascade trace
            'data_warnings': _data_warnings,
            'entry_ts': cmp_t_stamp,
            'extend_ts': cmp_t_stamp + self.add_timestamp(),
            'price_cmp': cmp
        }




# def format_calculate_setup_response(
#     result: Dict[str, Any],
#     *,
#     stock_name: str,
#     time_fr: int
# ) -> Dict[str, Any]:
#     """
#     Convert your v3.8.x result dict (with best_setup_long/best_setup_short = SetupPayload)
#     into the required RESPONSE structure only.
#     """
#     # print(result, "#################################################################")
#     def _to_dict(sp: Any) -> Optional[Dict[str, Any]]:
#         if sp is None:
#             return None
#         if is_dataclass(sp):
#             return asdict(sp)
#         if isinstance(sp, dict):
#             return sp
#         # fallback: object with attrs
#         return sp.__dict__

#     out: Dict[str, Any] = {
#         "STOCK_NAME": stock_name,
#         "TIME_FR": time_fr,
#         "PRICE_CMP": float(result.get("price_cmp")),
#     }

#     best_long = _to_dict(result.get("best_setup_long"))
#     if best_long:
#         out["BUY"] = {
#             "entry_price": float(best_long["entry_price"]),
#             "stop_loss": float(best_long["stop_price"]),
#             "target_price": float(best_long["target_price"]),
#         }
#         out["BUY_RRR"] = float(best_long.get("rr_ratio", 0.0))
#         out["BUY_TIMESTAMPS"] = {
#             "entry_price_timestamp": int(result.get("entry_ts")),
#             "target_price_timestamp": int(result.get("entry_ts")),
#             "extend_timestamp": float(result.get("extend_ts")),
#         }

#     best_short = _to_dict(result.get("best_setup_short"))
#     if best_short:
#         out["SELL"] = {
#             "entry_price": float(best_short["entry_price"]),
#             "stop_loss": float(best_short["stop_price"]),
#             "target_price": float(best_short["target_price"]),
#         }
#         out["SELL_RRR"] = float(best_short.get("rr_ratio", 0.0))
#         out["SELL_TIMESTAMPS"] = {
#             "entry_price_timestamp": int(result.get("entry_ts")),
#             "target_price_timestamp": int(result.get("entry_ts")),
#             "extend_timestamp": float(result.get("extend_ts")),
#         }

#     return out


def format_calculate_setup_response(
    result: Dict[str, Any],
    *,
    stock_name: str,
    time_fr: int,
    last_d_time,
    exp_num: str = None,
    is_future: bool = None,
    is_cash: bool = None
) -> Dict[str, Any]:
    """
    Convert your v3.8.x result dict (with best_setup_long/best_setup_short = SetupPayload)
    into the required RESPONSE structure only.
    """
    def _to_dict(sp: Any) -> Optional[Dict[str, Any]]:
        if sp is None:
            return None
        if is_dataclass(sp):
            return asdict(sp)
        if isinstance(sp, dict):
            return sp
        # fallback: object with attrs
        return sp.__dict__

    out: Dict[str, Any] = {
        "STOCK_NAME": stock_name,
        "TIME_FR": time_fr,
        "PRICE_CMP": float(result.get("price_cmp")),
    }

    best_long = _to_dict(result.get("best_setup_long"))
    if best_long:
        out["BUY"] = {
            "entry_price": float(best_long["entry_price"]),
            "stop_loss": float(best_long["stop_price"]),
            "target_price": float(best_long["target_price"]),

            "overlap_ratio": float(best_long.get("overlap_ratio", 0.0)),
            "htf_target_price": best_long.get("htf_target_price"),
            "struct_stop_A": best_long.get("struct_stop_A"),
            "struct_stop_E": best_long.get("struct_stop_E")
        }
        out["BUY_RRR"] = float(best_long.get("rr_ratio", 0.0))
        out["BUY_TIMESTAMPS"] = {
            "entry_price_timestamp": int(result.get("entry_ts")),
            "target_price_timestamp": int(result.get("entry_ts")),
            "extend_timestamp": float(result.get("extend_ts")),
        }

    best_short = _to_dict(result.get("best_setup_short"))
    if best_short:
        out["SELL"] = {
            "entry_price": float(best_short["entry_price"]),
            "stop_loss": float(best_short["stop_price"]),
            "target_price": float(best_short["target_price"]),

            "overlap_ratio": float(best_short.get("overlap_ratio", 0.0)),
            "htf_target_price": best_short.get("htf_target_price"),
            "struct_stop_A": best_short.get("struct_stop_A"),
            "struct_stop_E": best_short.get("struct_stop_E"),
        }
        out["SELL_RRR"] = float(best_short.get("rr_ratio", 0.0))
        out["SELL_TIMESTAMPS"] = {
            "entry_price_timestamp": int(result.get("entry_ts")),
            "target_price_timestamp": int(result.get("entry_ts")),
            "extend_timestamp": float(result.get("extend_ts")),
        }

    zones_x = result.get("zones_X") or []
    if is_cash == True:
        time_list = getattr(stock_logic_config, f"TIME_FRAMES_{time_fr}")
        csv_path_x = os.path.join(stock_data_dir_config.indian_stock_data_dir, 'latest_data_csv', f'{stock_name}_{time_list[-1]}.csv')
    else:
        list_attribute = f"FUTURE_TIME_FRAME_{time_fr}" if is_future == True else f"COMMODITY_TIME_FRAME_{time_fr}"
        time_list = getattr(stock_logic_config, list_attribute)
        data_dir = stock_data_dir_config.indian_stock_future_data_dir if is_future == True else stock_data_dir_config.indian_commodity_data
        csv_path_x = os.path.join(data_dir, 'latest_data_csv', f'{stock_name}_{exp_num}_{time_list[-1]}.csv')
    df, violation_df = load_preprocess_data(csv_path_x, last_d_time)
    out["ZONES_X"] = format_zone_ranges_with_setup(zones_x, df, True)

    return out




def process_setup(tick: str, time_list, last_d_time):
    try:
        sdep = SDEnginePipeline()
        result = sdep.run(tick, time_list, last_d_time)
        return result
    except Exception as e:
        print(f"Error processing setup for {tick}: {e}")
        logger.error(f"Error processing setup for {tick}: {e}", exc_info=True, stack_info=True)
        return str(e)


def process_setup_fc(tick: str, time_list, exp_num, last_d_time, is_future):
    try:
        sdep = SDEnginePipeline()
        result = sdep.run(symbol=tick, time_list=time_list, last_d_time=last_d_time, exp_num=exp_num, is_future=is_future)
        # print(result, "kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk")
        return result
    except Exception as e:
        print(f"Error processing setup for {tick}: {e}")
        logger.error(f"Error processing setup for {tick}: {e}", exc_info=True, stack_info=True)
        return str(e)