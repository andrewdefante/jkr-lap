"""
Shared at-bat simulation engine for Joker Lap.

Pitch selection uses count-specific mix from mlb.pitcher_count_mix.
Fallback chain:
  1. Pitcher's actual count mix (pitcher_count_mix) — season 2026 then 2025
  2. League average count mix for that count
  3. Pitcher's overall platoon mix (pitcher_mix_profile)
"""
import random
from collections import defaultdict
from sqlalchemy.orm import Session
from sqlalchemy import text

# ── CONSTANTS ────────────────────────────────────────────────────────────────
LEAGUE_WHIFF = 0.267
LEAGUE_CHASE = 0.310
LEAGUE_HR_RATE = 0.034
LEAGUE_AVG_BATTER_WHIFF = 0.267

LG_K_PCT  = 22.0   # league strikeout rate (%)
LG_BB_PCT =  8.5   # league walk rate (%)
LG_HR_PCT =  3.5   # league HR rate (%)
TTO_REG   = 30     # PA regression constant for TTO splits

FASTBALL_TYPES = {'FF', 'SI', 'FC'}
OFFSPEED_TYPES = {'CH', 'FS'}
BREAKING_TYPES = {'SL', 'ST', 'CU', 'KC'}

MIN_COUNT_PITCHES = 15

# Total swing rates by count — calibrated from 2026 MLB pitch data.
# Decouples swing decision from the in_zone CSW proxy (which over-swings in
# early counts and under-swings in 2-strike counts, compressing K rates).
COUNT_SWING_RATE = {
    (0, 0): 0.316, (1, 0): 0.422, (2, 0): 0.398, (3, 0): 0.076,
    (0, 1): 0.497, (1, 1): 0.548, (2, 1): 0.583, (3, 1): 0.520,
    (0, 2): 0.534, (1, 2): 0.601, (2, 2): 0.651, (3, 2): 0.715,
}

# Module-level cache for league mix (computed once per process)
_league_mix_cache = None

# Per-BIP rates from 2026 MLB actuals (May 2026, n=15,259 BIP)
# BABIP=.280, SLG/BIP=0.499, HR/BIP=0.040
BASE_CONTACT = {
    'single':   0.199,  # 19.9% of BIP
    'double':   0.065,  # 6.5% of BIP
    'triple':   0.005,  # 0.5% of BIP
    'home_run': 0.040,  # 4.0% of BIP
    'field_out': 0.691, # 69.1% of BIP
}

RUN_VALUES = {
    'single': 0.47, 'double': 0.78, 'triple': 1.07,
    'home_run': 1.40, 'walk': 0.33,
    'strikeout': -0.08, 'field_out': -0.10
}


# ── DATA LOADERS ─────────────────────────────────────────────────────────────

def load_pitcher_count_mix(pitcher_id: int, bat_side: str, db: Session) -> dict:
    """
    Load pitcher's count-specific mix from pitcher_count_mix table.
    Returns dict keyed by (balls, strikes) → list of pitch dicts.
    Falls back to league average for counts with insufficient data.

    When no 2026 count-mix data exists (pitcher below build threshold), scales
    the 2025 whiff/CSW rates using 2026 vs 2025 pitcher_mix_profile ratios so
    current-season stuff changes are reflected.
    """
    sql = text("""
        SELECT balls, strikes, pitch_type_code,
               usage_pct, whiff_rate, csw_rate, avg_velo, pitches, season
        FROM mlb.pitcher_count_mix
        WHERE pitcher_id = :pid
        AND bat_side = :side
        AND season IN (2026, 2025)
        ORDER BY season DESC, balls, strikes, usage_pct DESC
    """)
    rows = db.execute(sql, {"pid": pitcher_id, "side": bat_side}).mappings().all()

    has_2026 = any(int(r['season']) == 2026 for r in rows)

    # Build per-pitch-type scaling factors when falling back to 2025 data
    whiff_scales: dict = {}
    csw_scales: dict = {}
    if not has_2026 and rows:
        profile_rows = db.execute(text("""
            SELECT pitch_type_code, whiff_rate, csw_rate, season
            FROM mlb.pitcher_mix_profile
            WHERE pitcher_id = :pid AND bat_side = :side
            AND season IN (2025, 2026)
        """), {"pid": pitcher_id, "side": bat_side}).mappings().all()

        by_season: dict = {2025: {}, 2026: {}}
        for pr in profile_rows:
            s = int(pr['season'])
            by_season[s][pr['pitch_type_code']] = pr

        for pt, p26 in by_season[2026].items():
            p25 = by_season[2025].get(pt)
            if p25:
                w25 = float(p25['whiff_rate'] or LEAGUE_WHIFF)
                w26 = float(p26['whiff_rate'] or LEAGUE_WHIFF)
                c25 = float(p25['csw_rate'] or 0.28)
                c26 = float(p26['csw_rate'] or 0.28)
                if w25 > 0:
                    whiff_scales[pt] = max(0.5, min(2.0, w26 / w25))
                if c25 > 0:
                    csw_scales[pt] = max(0.5, min(2.0, c26 / c25))

    count_mix = {}
    seen = set()
    for r in rows:
        key = (int(r['balls']), int(r['strikes']))
        pt = r['pitch_type_code']
        entry_key = (key, pt)
        if entry_key not in seen:
            seen.add(entry_key)
            if key not in count_mix:
                count_mix[key] = []

            base_whiff = float(r['whiff_rate'] or LEAGUE_WHIFF)
            base_csw = float(r['csw_rate'] or 0.28)
            if not has_2026 and whiff_scales:
                base_whiff = min(0.90, base_whiff * whiff_scales.get(pt, 1.0))
                base_csw = min(0.90, base_csw * csw_scales.get(pt, 1.0))

            count_mix[key].append({
                "pitch_type": pt,
                "prob": float(r['usage_pct']),
                "whiff_rate": base_whiff,
                "csw_rate": base_csw,
                "avg_velo": float(r['avg_velo'] or 90.0),
                "pitches": int(r['pitches']),
            })

    return count_mix


def load_league_count_mix(db: Session) -> dict:
    """
    Load league average pitch mix by count.
    Used as fallback when pitcher has insufficient count data.
    Cached for process lifetime — the 2025 aggregate never changes during a session.
    """
    global _league_mix_cache
    if _league_mix_cache is not None:
        return _league_mix_cache

    sql = text("""
        SELECT balls, strikes, pitch_type_code,
               SUM(pitches) as total_pitches,
               SUM(pitches * usage_pct) / NULLIF(SUM(pitches), 0) as usage_pct,
               SUM(pitches * COALESCE(whiff_rate, 0)) / NULLIF(SUM(pitches), 0) as whiff_rate,
               SUM(pitches * COALESCE(csw_rate, 0)) / NULLIF(SUM(pitches), 0) as csw_rate,
               SUM(pitches * COALESCE(avg_velo, 90)) / NULLIF(SUM(pitches), 0) as avg_velo
        FROM mlb.pitcher_count_mix
        WHERE season = 2025
        GROUP BY balls, strikes, pitch_type_code
        HAVING SUM(pitches) >= 100
        ORDER BY balls, strikes, usage_pct DESC
    """)
    rows = db.execute(sql).mappings().all()

    lg_mix = {}
    for r in rows:
        key = (int(r['balls']), int(r['strikes']))
        if key not in lg_mix:
            lg_mix[key] = []
        lg_mix[key].append({
            "pitch_type": r['pitch_type_code'],
            "prob": float(r['usage_pct']),
            "whiff_rate": float(r['whiff_rate'] or LEAGUE_WHIFF),
            "csw_rate": float(r['csw_rate'] or 0.28),
            "avg_velo": float(r['avg_velo'] or 90.0),
            "pitches": int(r['total_pitches']),
        })

    _league_mix_cache = lg_mix
    return lg_mix


def load_pitcher_overall_mix(pitcher_id: int, bat_side: str, db: Session) -> list:
    """Load pitcher's overall platoon mix as final fallback."""
    sql = text("""
        SELECT pitch_type_code, usage_pct, whiff_rate, csw_rate, avg_velo
        FROM mlb.pitcher_mix_profile
        WHERE pitcher_id = :pid AND bat_side = :side
        AND season IN (2026, 2025)
        ORDER BY season DESC, usage_pct DESC
    """)
    rows = db.execute(sql, {"pid": pitcher_id, "side": bat_side}).mappings().all()

    if not rows:
        return []

    total = sum(float(r['usage_pct']) for r in rows)
    return [{
        "pitch_type": r['pitch_type_code'],
        "prob": float(r['usage_pct']) / max(total, 1.0),
        "whiff_rate": float(r['whiff_rate'] or LEAGUE_WHIFF),
        "csw_rate": float(r['csw_rate'] or 0.28),
        "avg_velo": float(r['avg_velo'] or 90.0),
    } for r in rows]


def load_pitcher_zone_rate(pitcher_id: int, db: Session) -> float:
    """
    Pitcher's actual zone rate (fraction of pitches in strike zone).
    Returns float between 0.45 and 0.72. Defaults to MLB average 0.560 (shadow-zone-inclusive).
    """
    sql = text("""
        SELECT
            AVG(CASE 
                WHEN p.zone BETWEEN 1 AND 9 THEN 1.0
                WHEN p.zone BETWEEN 11 AND 14 THEN 0.12
                ELSE 0.0 END) as zone_rate,
            COUNT(*) as pitches
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE ab.pitcher_id = :pid
        AND g.season = 2026 AND g.game_type = 'R'
        AND p.zone IS NOT NULL
        AND p.call_code IN ('B','C','S','W','T','F','X')
        HAVING COUNT(*) >= 50
    """)
    row = db.execute(sql, {"pid": pitcher_id}).mappings().first()
    if row and row['zone_rate']:
        return round(max(0.40, min(0.62, float(row['zone_rate']))), 3)
    return 0.510  # MLB average including shadow zone weighting


def load_batter_tends(batter_id: int, db: Session) -> dict:
    """Load batter pitch type tendencies."""
    sql = text("""
        SELECT pitch_type_code, whiff_rate, chase_rate,
               hard_hit_rate, avg_exit_velo, avg_woba_on_contact
        FROM mlb.batter_pitch_type_tendencies
        WHERE batter_id = :bid AND season = 2025
    """)
    rows = db.execute(sql, {"bid": batter_id}).mappings().all()
    return {r['pitch_type_code']: dict(r) for r in rows}


def load_goose2_pitch_adjustments(pitcher_id: int, bat_side: str,
                                   db: Session) -> dict:
    """
    Load Goose+ 2 per-pitch adjustments for simulation.
    Returns dict of pitch_type → adjustment multipliers.
    Dampened 50%: G2+ 120 → 1.10x whiff/CSW boost, G2+ 80 → 0.90x reduction.
    """
    sql = text("""
        SELECT pitch_type_code, goose2_plus, whiff_idx, csw_idx,
               slg_idx, contact_idx, pitches
        FROM mlb.goose2_scores
        WHERE pitcher_id = :pid
        AND season = 2026
        AND bat_side = :side
        AND pitches >= 15
    """)
    rows = db.execute(sql, {"pid": pitcher_id, "side": bat_side}).mappings().all()

    adjustments = {}
    for r in rows:
        pt = r['pitch_type_code']
        g2 = float(r['goose2_plus'] or 100)
        adj = 1.0 + (g2 - 100) / 200
        adjustments[pt] = {
            'whiff_mult': round(adj, 3),
            'csw_mult': round(adj, 3),
            'contact_mult': round(2.0 - adj, 3),
            'goose2_plus': g2,
        }

    return adjustments


def load_juiced2_batter_adjustments(batter_id: int, db: Session) -> dict:
    """
    Load Juiced+ 2 adjustments for batter simulation.
    Returns multipliers for contact outcomes.
    """
    sql = text("""
        SELECT juiced2_plus, quality_adj_idx, ops_idx, slg_idx,
               hard_hit_idx, avg_goose2_faced
        FROM mlb.juiced2_scores
        WHERE batter_id = :bid AND season = 2026
    """)
    row = db.execute(sql, {"bid": batter_id}).mappings().first()
    if not row:
        return {}

    j2 = float(row['juiced2_plus'] or 100)
    adj = 1.0 + (j2 - 100) / 200

    return {
        'ops_mult': round(adj, 3),
        'contact_mult': round(adj, 3),
        'hr_mult': round(adj, 3),
        'juiced2_plus': j2,
        'quality_adj_idx': float(row['quality_adj_idx'] or 100),
    }


def load_stuff_score(pitcher_id: int, bat_side: str, db: Session) -> dict:
    """
    Load Stuff Score per pitch type for simulation adjustments.
    Returns dict of pitch_type → stuff adjustment multiplier.
    """
    sql = text("""
        SELECT pitch_type_code, stuff_score, physical_score,
               location_score, tunnel_score, pitches
        FROM mlb.stuff_scores
        WHERE pitcher_id = :pid AND season = 2026
        AND pitches >= 15
        ORDER BY pitches DESC
    """)
    rows = db.execute(sql, {"pid": pitcher_id}).mappings().all()

    if not rows:
        return {}

    adjustments = {}
    for r in rows:
        pt = r['pitch_type_code']
        stuff = float(r['stuff_score'] or 100)
        adj = max(0.50, min(2.00, 1.0 + (stuff - 100) / 60))
        adjustments[pt] = {
            'stuff_score': stuff,
            'whiff_mult': round(adj, 3),
            'csw_mult': round(adj, 3),
            'contact_mult': round(2.0 - adj, 3),
            'physical_score': float(r['physical_score'] or 100),
            'location_score': float(r['location_score'] or 100),
            'tunnel_score': float(r['tunnel_score'] or 100),
        }
    return adjustments


def load_combined_pitcher_adjustments(pitcher_id: int, bat_side: str,
                                       db: Session) -> dict:
    """
    Blend Goose+1, Goose+2, and Stuff Score into a single per-pitch adjustment dict.
    Weights: Stuff 35% / Goose+2 35% / Goose+1 30%.
    Falls back gracefully when any signal is missing.
    """
    goose2_adj = load_goose2_pitch_adjustments(pitcher_id, bat_side, db)
    stuff_adj = load_stuff_score(pitcher_id, bat_side, db)

    goose1_sql = text("""
        SELECT goose_plus, goose_plus_vs_lhh, goose_plus_vs_rhh
        FROM mlb.goose_overall
        WHERE pitcher_id = :pid AND season = 2026 AND game_pk IS NULL
    """)
    goose1 = db.execute(goose1_sql, {"pid": pitcher_id}).mappings().first()
    goose1_val = None
    if goose1:
        if bat_side == 'L' and goose1['goose_plus_vs_lhh']:
            goose1_val = float(goose1['goose_plus_vs_lhh'])
        elif bat_side == 'R' and goose1['goose_plus_vs_rhh']:
            goose1_val = float(goose1['goose_plus_vs_rhh'])
        else:
            goose1_val = float(goose1['goose_plus'] or 100)

    all_pitch_types = set(goose2_adj.keys()) | set(stuff_adj.keys())
    if not all_pitch_types:
        return {}

    combined = {}
    for pt in all_pitch_types:
        g2 = goose2_adj.get(pt, {})
        st = stuff_adj.get(pt, {})

        g1_adj = 1.0 + (goose1_val - 100) / 200 if goose1_val else 1.0

        g2_whiff = g2.get('whiff_mult', 1.0)
        st_whiff = st.get('whiff_mult', 1.0)
        g2_contact = g2.get('contact_mult', 1.0)
        st_contact = st.get('contact_mult', 1.0)

        w_g2 = 0.35 if g2 else 0.0
        w_st = 0.35 if st else 0.0
        w_g1 = 0.30 if goose1_val is not None else 0.0
        total_w = w_g2 + w_st + w_g1

        if total_w == 0:
            combined[pt] = {'whiff_mult': 1.0, 'contact_mult': 1.0}
            continue

        w_g2 /= total_w
        w_st /= total_w
        w_g1 /= total_w

        whiff_mult = g2_whiff * w_g2 + st_whiff * w_st + g1_adj * w_g1
        contact_mult = g2_contact * w_g2 + st_contact * w_st + (2.0 - g1_adj) * w_g1

        combined[pt] = {
            'whiff_mult': round(whiff_mult, 3),
            'csw_mult': round(whiff_mult, 3),
            'contact_mult': round(contact_mult, 3),
            'goose1': round(goose1_val, 1) if goose1_val else None,
            'goose2': round(g2.get('goose2_plus', 100), 1) if g2 else None,
            'stuff': round(st.get('stuff_score', 100), 1) if st else None,
        }

    return combined


def load_pk_phr_adjustments(pitcher_id: int, bat_side: str,
                             db: Session) -> dict:
    """
    Load pK+ (whiff signal) and pHR+ (contact signal) per pitch type.
    pK+  → whiff_mult/csw_mult  (best K rate predictor, r=0.473)
    pHR+ → contact_mult/hr_mult (best HR predictor, r=-0.311 SLG)
    """
    sql = text("""
        SELECT
            COALESCE(pk.pitch_type_code, phr.pitch_type_code) AS pitch_type_code,
            pk.pk_plus,
            pk.whiff_pct,
            phr.phr_plus,
            so.stuff_score,
            COALESCE(pk.pitches, phr.pitches) AS pitches
        FROM mlb.pk_scores pk
        FULL OUTER JOIN mlb.phr_scores phr
            ON  phr.pitcher_id     = pk.pitcher_id
            AND phr.pitch_type_code = pk.pitch_type_code
            AND phr.bat_side        = pk.bat_side
            AND phr.season          = pk.season
        LEFT JOIN mlb.stuff_scores so
            ON  so.pitcher_id      = COALESCE(pk.pitcher_id, phr.pitcher_id)
            AND so.pitch_type_code = COALESCE(pk.pitch_type_code, phr.pitch_type_code)
            AND so.season          = COALESCE(pk.season, phr.season)
        WHERE COALESCE(pk.pitcher_id, phr.pitcher_id) = :pid
        AND   COALESCE(pk.season, phr.season) = 2026
        AND   COALESCE(pk.bat_side, phr.bat_side) = :side
        AND   COALESCE(pk.pitches, phr.pitches) >= 10
    """)
    rows = db.execute(sql, {"pid": pitcher_id, "side": bat_side}).mappings().all()

    if not rows:
        sql2 = text("""
            SELECT pk.pitch_type_code,
                   AVG(pk.pk_plus)   AS pk_plus,
                   AVG(phr.phr_plus) AS phr_plus,
                   AVG(so.stuff_score) AS stuff_score,
                   SUM(pk.pitches)   AS pitches
            FROM mlb.pk_scores pk
            LEFT JOIN mlb.phr_scores phr
                ON  phr.pitcher_id     = pk.pitcher_id
                AND phr.pitch_type_code = pk.pitch_type_code
                AND phr.season          = pk.season
            LEFT JOIN mlb.stuff_scores so
                ON  so.pitcher_id      = pk.pitcher_id
                AND so.pitch_type_code = pk.pitch_type_code
                AND so.season          = pk.season
            WHERE pk.pitcher_id = :pid AND pk.season = 2026
            GROUP BY pk.pitch_type_code
            HAVING SUM(pk.pitches) >= 10
        """)
        rows = db.execute(sql2, {"pid": pitcher_id}).mappings().all()

    if not rows:
        return {}

    adjustments = {}
    for r in rows:
        pt    = r['pitch_type_code']
        pk    = float(r['pk_plus']    or 100)
        phr   = float(r['phr_plus']   or 100)
        stuff = float(r['stuff_score'] or 100)

        pk_adj  = max(0.50, min(2.00, 1.0 + (pk  - 100) / 60))
        phr_adj = max(0.50, min(2.00, 1.0 + (phr - 100) / 60))

        # Use season whiff rate from pk_scores as base (more stable than count-mix)
        season_whiff = float(r.get('whiff_pct') or 0)

        adjustments[pt] = {
            'whiff_mult':   round(pk_adj, 3),
            'csw_mult':     round(pk_adj, 3),
            'contact_mult': round(2.0 - phr_adj, 3),
            'hr_mult':      round(2.0 - phr_adj, 3),
            'pk_plus':      round(pk, 1),
            'phr_plus':     round(phr, 1),
            'stuff_score':  round(stuff, 1),
            'season_whiff': round(season_whiff, 3),
        }

    return adjustments


def load_goose3_adjustments(pitcher_id: int, bat_side: str,
                             db: Session) -> dict:
    """
    Load per-pitch adjustments using empirically validated model assignments:
    - Stuff Score → whiff_mult/csw_mult (best K rate predictor, r=0.358)
    - Goose+2     → contact_mult + hr_mult (best HR predictor, r=-0.298)
    """
    sql = text("""
        SELECT pitch_type_code, goose3_plus, stuff_score,
               goose2_score, stuff_outcomes_gap, pitches
        FROM mlb.goose3_scores
        WHERE pitcher_id = :pid
        AND season = 2026
        AND bat_side = :side
        AND pitches >= 10
    """)
    rows = db.execute(sql, {"pid": pitcher_id, "side": bat_side}).mappings().all()

    if not rows:
        sql2 = text("""
            SELECT pitch_type_code,
                   AVG(goose3_plus)       AS goose3_plus,
                   AVG(stuff_score)       AS stuff_score,
                   AVG(goose2_score)      AS goose2_score,
                   AVG(stuff_outcomes_gap) AS stuff_outcomes_gap,
                   SUM(pitches)           AS pitches
            FROM mlb.goose3_scores
            WHERE pitcher_id = :pid AND season = 2026
            GROUP BY pitch_type_code
            HAVING SUM(pitches) >= 10
        """)
        rows = db.execute(sql2, {"pid": pitcher_id}).mappings().all()

    if not rows:
        return {}

    adjustments = {}
    for r in rows:
        pt = r['pitch_type_code']
        stuff  = float(r['stuff_score']  or 100)
        goose2 = float(r['goose2_score'] or 100)
        goose3 = float(r['goose3_plus']  or 100)

        # Stuff Score drives whiff (best K predictor r=0.358)
        stuff_adj = max(0.50, min(2.00, 1.0 + (stuff - 100) / 60))
        # Goose+2 drives contact quality and HR (best HR predictor r=-0.298)
        g2_adj = max(0.50, min(2.00, 1.0 + (goose2 - 100) / 60))

        adjustments[pt] = {
            'whiff_mult':   round(stuff_adj, 3),
            'csw_mult':     round(stuff_adj, 3),
            'contact_mult': round(2.0 - g2_adj, 3),
            'hr_mult':      round(2.0 - g2_adj, 3),
            'goose3_plus':  round(goose3, 1),
            'stuff_score':  round(stuff, 1),
            'goose2_score': round(goose2, 1),
            'gap':          round(float(r['stuff_outcomes_gap'] or 0), 1),
        }

    return adjustments


# ── SPLITS HELPERS ────────────────────────────────────────────────────────────

def _combine_mults(*mults: dict, lo: float = 0.60, hi: float = 1.50) -> dict:
    """Multiply outcome multipliers from multiple adjustment dicts, clamp to [lo, hi]."""
    keys = ('k_mult', 'bb_mult', 'hr_mult', 'contact_mult')
    out = {k: 1.0 for k in keys}
    for m in mults:
        if not m:
            continue
        for k in keys:
            if k in m:
                out[k] *= m[k]
    return {k: max(lo, min(hi, round(v, 3))) for k, v in out.items()}


def load_tto_adjustment(pitcher_id: int, season: int, tto: int,
                         db: Session) -> dict:
    """
    Load time-through-order adjustment from mlb.pitcher_splits.
    Returns k/bb/hr multipliers as ratio of tto split vs pitcher's overall rate.
    Regression weight = min(0.80, tto_pa / (tto_pa + TTO_REG)).
    """
    tto_key = f'tto_{tto}' if tto <= 2 else 'tto_3plus'
    try:
        rows = db.execute(text("""
            SELECT split_type, pa, k_pct, bb_pct, hr_pct
            FROM mlb.pitcher_splits
            WHERE pitcher_id = :pid AND season = :season
            AND split_type IN (:tto_key, 'overall')
        """), {"pid": pitcher_id, "season": season, "tto_key": tto_key}).mappings().all()
    except Exception:
        return {}

    by_split = {r['split_type']: dict(r) for r in rows}
    tto_row    = by_split.get(tto_key)
    overall_row = by_split.get('overall')
    if not tto_row or not overall_row:
        return {}

    tto_pa = float(tto_row.get('pa') or 0)
    w = min(0.80, tto_pa / (tto_pa + TTO_REG))

    def _ratio(tto_val, overall_val):
        blended = tto_val * w + overall_val * (1 - w)
        return blended / max(overall_val, 0.01)

    return {
        'k_mult':  round(_ratio(float(tto_row.get('k_pct')  or LG_K_PCT),
                                float(overall_row.get('k_pct')  or LG_K_PCT)), 3),
        'bb_mult': round(_ratio(float(tto_row.get('bb_pct') or LG_BB_PCT),
                                float(overall_row.get('bb_pct') or LG_BB_PCT)), 3),
        'hr_mult': round(_ratio(float(tto_row.get('hr_pct') or LG_HR_PCT),
                                float(overall_row.get('hr_pct') or LG_HR_PCT)), 3),
        'tto':              tto,
        'tto_pa':           int(tto_pa),
        'regression_weight': round(w, 3),
    }


def load_h2h_adjustment(pitcher_id: int, batter_id: int, db: Session) -> dict:
    """
    Load head-to-head history adjustment from mlb.matchup_history.
    Aggregates all seasons for career H2H; re-applies regression at career PA level.
    Returns k_mult, hr_mult relative to league average.
    """
    try:
        row = db.execute(text("""
            SELECT
                SUM(pa)  AS pa,
                ROUND(SUM(k)::NUMERIC  / NULLIF(SUM(pa), 0) * 100, 2) AS k_pct_raw,
                ROUND(SUM(hr)::NUMERIC / NULLIF(SUM(pa), 0) * 100, 2) AS hr_pct_raw,
                ROUND(
                    (SUM(k)::NUMERIC / NULLIF(SUM(pa), 0) * 100)
                    * (SUM(pa)::NUMERIC / (SUM(pa) + 20))
                    + 22.0 * (20.0 / (SUM(pa) + 20)), 2) AS k_pct_reg,
                ROUND(
                    (SUM(hr)::NUMERIC / NULLIF(SUM(pa), 0) * 100)
                    * (SUM(pa)::NUMERIC / (SUM(pa) + 20))
                    + 3.5 * (20.0 / (SUM(pa) + 20)), 2) AS hr_pct_reg
            FROM mlb.matchup_history
            WHERE pitcher_id = :pid AND batter_id = :bid
        """), {"pid": pitcher_id, "bid": batter_id}).mappings().first()
    except Exception:
        return {}

    if not row or not row['pa']:
        return {}

    k_reg  = float(row['k_pct_reg']  or LG_K_PCT)
    hr_reg = float(row['hr_pct_reg'] or LG_HR_PCT)

    return {
        'k_mult':    round(k_reg  / LG_K_PCT,  3),
        'hr_mult':   round(hr_reg / LG_HR_PCT, 3),
        'pa':        int(row['pa']),
        'k_pct_raw':  float(row['k_pct_raw']  or 0),
        'hr_pct_raw': float(row['hr_pct_raw'] or 0),
        'k_pct_reg':  round(k_reg, 2),
        'hr_pct_reg': round(hr_reg, 2),
    }


# ── PITCH SELECTION ───────────────────────────────────────────────────────────

def normalize_mix(mix: list) -> list:
    """Normalize pitch probabilities to sum to 1.0."""
    total = sum(p['prob'] for p in mix)
    if total <= 0:
        return mix
    return [{**p, 'prob': p['prob'] / total} for p in mix]


def get_mix_for_count(balls: int, strikes: int,
                      count_mix: dict,
                      lg_mix: dict,
                      fallback_mix: list) -> list:
    """
    Get pitch mix for a specific count using fallback chain:
    1. Pitcher count-specific data (if >= MIN_COUNT_PITCHES)
    2. League average count mix
    3. Pitcher overall platoon mix
    """
    key = (balls, strikes)

    pitcher_count = count_mix.get(key, [])
    total_pitches = sum(p['pitches'] for p in pitcher_count)

    if pitcher_count and total_pitches >= MIN_COUNT_PITCHES:
        return normalize_mix(pitcher_count)

    lg_count = lg_mix.get(key, [])
    if lg_count:
        if fallback_mix:
            lg_norm = normalize_mix(lg_count)
            fb_norm = normalize_mix(fallback_mix)

            pitch_types = set(p['pitch_type'] for p in lg_norm) | \
                          set(p['pitch_type'] for p in fb_norm)
            lg_map = {p['pitch_type']: p for p in lg_norm}
            fb_map = {p['pitch_type']: p for p in fb_norm}

            blended = []
            for pt in pitch_types:
                lg_p = lg_map.get(pt, {})
                fb_p = fb_map.get(pt, {})
                prob = (lg_p.get('prob', 0) * 0.60 +
                        fb_p.get('prob', 0) * 0.40)
                if prob > 0:
                    blended.append({
                        "pitch_type": pt,
                        "prob": prob,
                        "whiff_rate": (fb_p.get('whiff_rate') or
                                       lg_p.get('whiff_rate') or LEAGUE_WHIFF),
                        "csw_rate": (fb_p.get('csw_rate') or
                                     lg_p.get('csw_rate') or 0.28),
                        "avg_velo": (fb_p.get('avg_velo') or
                                     lg_p.get('avg_velo') or 90.0),
                    })
            return normalize_mix(blended)
        return normalize_mix(lg_count)

    return normalize_mix(fallback_mix) if fallback_mix else []


def select_pitch(mix: list) -> dict:
    """Randomly select a pitch from the mix."""
    if not mix:
        return {"pitch_type": "FF", "prob": 1.0,
                "whiff_rate": LEAGUE_WHIFF, "csw_rate": 0.28, "avg_velo": 93.0}
    rand = random.random()
    cum = 0.0
    for p in mix:
        cum += p['prob']
        if rand <= cum:
            return p
    return mix[-1]


# ── BATTER OUTCOME ────────────────────────────────────────────────────────────

def get_contact_probs(pitch_type: str, batter_tends: dict,
                      defense: dict = None) -> dict:
    """Get batter-specific contact outcome probabilities.

    Calibrated to 2026 MLB actuals (May 2026, n=15,259 BIP):
    BABIP=.280, SLG/BIP=0.499, HR/BIP=0.040, HardHit%=39.5%

    HR driven primarily by hard_hit_rate (captures both EV and launch angle).
    EV used only as minor secondary signal (dampened 600x) since contact EV
    in GUMBO is small-sample and doesn't capture launch angle.

    defense: team_defense row for opposing team — adjusts BABIP and XBH rate.
    """
    tend = batter_tends.get(pitch_type, {})
    hh_rate = float(tend.get('hard_hit_rate') or 0.395)
    avg_ev  = float(tend.get('avg_exit_velo') or 88.0)

    # HR/BIP: hard hit rate is primary driver
    # League avg: hh_rate=39.5% → HR/BIP=4.0% (2026 actuals)
    # Linear scale: 0% hh → ~0% HR, 80% hh → ~8% HR
    hr_from_hh = max(0.001, min(0.10, 0.040 * (hh_rate / 0.395)))

    # EV adds minor signal (±1.5% max) — dampened to avoid small-sample inflation
    ev_boost = max(-0.015, min(0.015, (avg_ev - 88.0) / 600.0))
    hr_adj = max(0.001, min(0.12, hr_from_hh + ev_boost))

    # BABIP scales gently with EV (±8% per 10mph around 88)
    babip = max(0.15, min(0.40, 0.280 * (1.0 + (avg_ev - 88.0) / 125.0)))

    # Defense adjustment: scale BABIP and XBH rate by opposing team's defensive profile.
    # League avg BABIP allowed = .290. COL (.338) → +16%, LAD (.253) → -13%.
    if defense:
        def_babip = defense.get('babip_allowed', 0.290)
        defense_mult = max(0.80, min(1.25, def_babip / 0.290))
        babip = min(0.40, babip * defense_mult)

    non_hr_hit = babip * (1.0 - hr_adj)

    # Split non-HR hits: doubles scale with EV, singles absorb remainder
    double_frac = max(0.10, min(0.40, 0.24 * (1.0 + (avg_ev - 88.0) / 60.0)))
    triple_frac = 0.02

    # Defense XBH adjustment: poor defenses allow more gap hits
    if defense:
        def_xbh = defense.get('xbh_rate', 0.220)
        xbh_mult = max(0.75, min(1.30, def_xbh / 0.220))
        double_frac = max(0.10, min(0.40, double_frac * xbh_mult))

    single_frac = 1.0 - double_frac - triple_frac

    single_p = non_hr_hit * single_frac
    double_p = non_hr_hit * double_frac
    triple_p = non_hr_hit * triple_frac
    out_p    = max(0.30, 1.0 - single_p - double_p - triple_p - hr_adj)

    return {
        'home_run':  hr_adj,
        'single':    single_p,
        'double':    double_p,
        'triple':    triple_p,
        'field_out': out_p,
    }


def resolve_contact(pitch_type: str, batter_tends: dict) -> str:
    """Resolve a ball-in-play outcome."""
    probs = get_contact_probs(pitch_type, batter_tends)
    rand = random.random()
    cum = 0.0
    for outcome, prob in probs.items():
        cum += prob
        if rand <= cum:
            return outcome
    return 'field_out'


def load_team_defense(opp_team_id: int, db) -> dict:
    """Load defensive metrics for opposing team. Keyed by team_id to avoid abbrev mismatch."""
    if not opp_team_id:
        return {}
    row = db.execute(text("""
        SELECT babip_allowed, slg_allowed, xbh_rate,
               single_rate, double_rate, triple_rate
        FROM mlb.team_defense
        WHERE team_id = :tid AND season = 2026
    """), {"tid": opp_team_id}).mappings().first()
    if row:
        return dict(row)
    return {}


def load_weather_modifier(game_pk: int, db) -> float:
    """Load combined HR modifier from weather data. Default 1.0."""
    if not game_pk:
        return 1.0
    try:
        result = db.execute(text("""
            SELECT combined_hr_modifier
            FROM mlb.game_weather
            WHERE game_pk = :gp
        """), {"gp": game_pk}).scalar()
        return float(result) if result else 1.0
    except Exception:
        return 1.0


K_REG = 30  # regression constant for splits blending


def load_game_state_adjustments(pitcher_id: int, batter_id: int,
                                 on_first: bool, on_second: bool, on_third: bool,
                                 outs: int, db) -> dict:
    """
    Load splits-based adjustments for the current base-out state.
    Returns k_mult, bb_mult, hr_mult blended toward league average via regression.
    """
    try:
        season = db.execute(text("""
            SELECT MAX(g.season) FROM mlb.games g WHERE g.game_type = 'R'
        """)).scalar() or 2026

        # Determine base/out context key
        if on_second or on_third:
            base_ctx = 'risp'
        elif on_first or on_second or on_third:
            base_ctx = 'runners_on'
        else:
            base_ctx = 'bases_empty'

        if outs == 0:
            out_ctx = 'no_outs'
        elif outs == 1:
            out_ctx = 'one_out'
        else:
            out_ctx = 'two_outs'

        # League baseline k_pct and bb_pct for regression
        lg_row = db.execute(text("""
            SELECT
                ROUND(AVG(CASE WHEN event IN ('Strikeout','Strikeout - DP') THEN 1.0 ELSE 0.0 END) * 100, 1) as k_pct,
                ROUND(AVG(CASE WHEN event IN ('Walk','Intent Walk') THEN 1.0 ELSE 0.0 END) * 100, 1) as bb_pct,
                ROUND(AVG(CASE WHEN event = 'Home Run' THEN 1.0 ELSE 0.0 END) * 100, 1) as hr_pct
            FROM mlb.at_bats ab
            JOIN mlb.games g ON g.game_pk = ab.game_pk
            WHERE g.season = :season AND g.game_type = 'R'
            AND ab.runners_reconstructed = TRUE
            AND ab.event NOT IN ('Runner Out','Field Error','Caught Stealing 2B',
                                  'Caught Stealing 3B','Caught Stealing Home',
                                  'Pickoff','Wild Pitch','Passed Ball')
        """), {"season": season}).mappings().first()

        if not lg_row:
            return {}

        lg_k = float(lg_row['k_pct'] or 22.0)
        lg_bb = float(lg_row['bb_pct'] or 8.5)
        lg_hr = float(lg_row['hr_pct'] or 3.5)

        def get_split(player_id: int, is_pitcher: bool) -> dict:
            if is_pitcher:
                pa_col = "pitcher_id"
            else:
                pa_col = "batter_id"
            # Query split stats for this context
            col_filter = "pitcher_id" if is_pitcher else "batter_id"
            rows = db.execute(text(f"""
                SELECT
                    COUNT(*) as pa,
                    ROUND(AVG(CASE WHEN event IN ('Strikeout','Strikeout - DP') THEN 1.0 ELSE 0.0 END) * 100, 1) as k_pct,
                    ROUND(AVG(CASE WHEN event IN ('Walk','Intent Walk') THEN 1.0 ELSE 0.0 END) * 100, 1) as bb_pct,
                    ROUND(AVG(CASE WHEN event = 'Home Run' THEN 1.0 ELSE 0.0 END) * 100, 1) as hr_pct
                FROM mlb.at_bats ab
                JOIN mlb.games g ON g.game_pk = ab.game_pk
                WHERE ab.{col_filter} = :pid
                AND g.season = :season AND g.game_type = 'R'
                AND ab.runners_reconstructed = TRUE
                AND (
                    (:ctx = 'risp' AND (ab.on_second OR ab.on_third))
                    OR (:ctx = 'runners_on' AND (ab.on_first OR ab.on_second OR ab.on_third))
                    OR (:ctx = 'bases_empty' AND NOT ab.on_first AND NOT ab.on_second AND NOT ab.on_third)
                )
                AND (
                    (:out_ctx = 'no_outs' AND ab.outs_before = 0)
                    OR (:out_ctx = 'one_out' AND ab.outs_before = 1)
                    OR (:out_ctx = 'two_outs' AND ab.outs_before = 2)
                )
                AND ab.event NOT IN ('Runner Out','Field Error','Caught Stealing 2B',
                                      'Caught Stealing 3B','Caught Stealing Home',
                                      'Pickoff','Wild Pitch','Passed Ball')
            """), {"pid": player_id, "season": season, "ctx": base_ctx, "out_ctx": out_ctx}).mappings().first()
            return dict(rows) if rows else {}

        p_split = get_split(pitcher_id, is_pitcher=True)
        b_split = get_split(batter_id, is_pitcher=False)

        def blend(actual, pa, lg_val):
            if not actual or not pa:
                return lg_val
            w = float(pa) / (float(pa) + K_REG)
            return actual * w + lg_val * (1 - w)

        p_pa = float(p_split.get('pa') or 0)
        b_pa = float(b_split.get('pa') or 0)

        p_k = blend(float(p_split.get('k_pct') or lg_k), p_pa, lg_k)
        b_k = blend(float(b_split.get('k_pct') or lg_k), b_pa, lg_k)
        p_bb = blend(float(p_split.get('bb_pct') or lg_bb), p_pa, lg_bb)
        b_bb = blend(float(b_split.get('bb_pct') or lg_bb), b_pa, lg_bb)
        p_hr = blend(float(p_split.get('hr_pct') or lg_hr), p_pa, lg_hr)
        b_hr = blend(float(b_split.get('hr_pct') or lg_hr), b_pa, lg_hr)

        import math as _math
        k_ratio_p = p_k / max(lg_k, 0.01)
        k_ratio_b = b_k / max(lg_k, 0.01)
        k_mult = round(_math.sqrt(k_ratio_p * k_ratio_b), 3)
        k_mult = max(0.70, min(1.30, k_mult))

        bb_ratio_p = p_bb / max(lg_bb, 0.01)
        bb_ratio_b = b_bb / max(lg_bb, 0.01)
        bb_mult = round(_math.sqrt(bb_ratio_p * bb_ratio_b), 3)
        bb_mult = max(0.70, min(1.30, bb_mult))

        hr_ratio_p = p_hr / max(lg_hr, 0.01)
        hr_ratio_b = b_hr / max(lg_hr, 0.01)
        hr_mult = round(_math.sqrt(hr_ratio_p * hr_ratio_b), 3)
        hr_mult = max(0.70, min(1.30, hr_mult))

        runners = []
        if on_first: runners.append('1B')
        if on_second: runners.append('2B')
        if on_third: runners.append('3B')
        state_label = (', '.join(runners) if runners else 'Bases Empty') + f', {outs} out{"s" if outs != 1 else ""}'

        return {
            'k_mult': k_mult,
            'bb_mult': bb_mult,
            'hr_mult': hr_mult,
            'base_context': base_ctx,
            'out_context': out_ctx,
            'state_label': state_label,
            'pitcher_pa': int(p_pa),
            'batter_pa': int(b_pa),
        }
    except Exception as e:
        print(f"  Splits adjustment error: {e}")
        return {}


def resolve_contact_v2(pitch_type: str, batter_tends: dict,
                        contact_mult: float = 1.0,
                        hr_mult: float = 1.0,
                        weather_hr_mult: float = 1.0,
                        defense: dict = None) -> str:
    """
    Resolve ball-in-play with Juiced+ 2 batter adjustments and weather modifier.
    contact_mult > 1.0 = better hitter, more likely to get hits.
    hr_mult > 1.0 = better hitter, more likely to hit HR.
    weather_hr_mult: combined wind+temp HR modifier from game_weather table.
    defense: team_defense row — adjusts BABIP and XBH rate for opposing team.
    """
    probs = get_contact_probs(pitch_type, batter_tends, defense=defense)

    adjusted = {}
    for outcome, prob in probs.items():
        if outcome == 'home_run':
            adjusted[outcome] = prob * hr_mult * weather_hr_mult
        elif outcome in ('single', 'double', 'triple'):
            adjusted[outcome] = prob * contact_mult
        else:
            adjusted[outcome] = prob

    total = sum(adjusted.values())
    for k in adjusted:
        adjusted[k] /= max(total, 0.001)

    rand = random.random()
    cum = 0.0
    for outcome, prob in adjusted.items():
        cum += prob
        if rand <= cum:
            return outcome
    return 'field_out'


# ── MAIN SIMULATION ───────────────────────────────────────────────────────────

def simulate_pa(count_mix: dict, lg_mix: dict,
                fallback_mix: list, batter_tends: dict,
                record_pitches: bool = False,
                balls_start: int = 0,
                strikes_start: int = 0,
                goose2_adj: dict = None,
                juiced2_adj: dict = None,
                weather_hr_mult: float = 1.0,
                splits_adj: dict = None,
                zone_rate: float = 0.510,
                batter_overall: dict = None,
                defense: dict = None) -> dict:
    """
    Simulate a single plate appearance.
    balls_start/strikes_start: starting count (default 0-0)

    Returns dict with:
        result: str (outcome)
        pitches: list (if record_pitches=True)
        pitch_count: int
    """
    balls = balls_start
    strikes = strikes_start
    pitch_log = [] if record_pitches else None
    pitch_count = 0

    batter_contact_mult = juiced2_adj.get('contact_mult', 1.0) if juiced2_adj else 1.0
    batter_hr_mult = juiced2_adj.get('hr_mult', 1.0) if juiced2_adj else 1.0

    if splits_adj:
        k_mult = splits_adj.get('k_mult', 1.0)
        batter_hr_mult *= splits_adj.get('hr_mult', 1.0)
        bb_mult = splits_adj.get('bb_mult', 1.0)
    else:
        k_mult = 1.0
        bb_mult = 1.0

    for _ in range(20):
        pitch_count += 1
        mix = get_mix_for_count(balls, strikes, count_mix, lg_mix, fallback_mix)
        selected = select_pitch(mix)
        pt = selected['pitch_type']
        pitcher_whiff = selected['whiff_rate']
        csw = selected['csw_rate']

        if goose2_adj and pt in goose2_adj:
            adj = goose2_adj[pt]
            # Use season whiff rate as base if available — it already encodes pitcher
            # quality, so skip whiff_mult (which measures the same signal) to avoid
            # double-counting. Fall back to count-mix × whiff_mult when unavailable.
            season_whiff = adj.get('season_whiff', 0)
            if season_whiff > 0:
                pitcher_whiff = min(0.95, season_whiff)
            else:
                pitcher_whiff = min(0.95, pitcher_whiff * adj['whiff_mult'])
            csw = min(0.95, csw * adj['csw_mult'])
            pitcher_contact_mult = adj.get('contact_mult', 1.0)
            pitcher_hr_mult = adj.get('hr_mult', 1.0)
        else:
            pitcher_contact_mult = 1.0
            pitcher_hr_mult = 1.0

        in_zone = random.random() < zone_rate

        tend = batter_tends.get(pt, {})
        overall = batter_overall or {}
        batter_whiff = float(tend.get('whiff_rate') or overall.get('whiff_rate') or LEAGUE_WHIFF)
        batter_swing = float(tend.get('swing_rate') or overall.get('swing_rate') or 0.46)
        batter_contact = float(tend.get('contact_rate') or overall.get('contact_rate') or 0.78)
        batter_zone_swing = float(tend.get('zone_swing_rate') or overall.get('zone_swing_rate') or 0.670)
        batter_chase_direct = float(tend.get('chase_rate') or overall.get('chase_rate') or LEAGUE_CHASE)

        if in_zone:
            # Use batter's actual zone swing rate, adjusted by count
            count_zone_adj = {
                (0,0): 0.95, (0,1): 1.00, (0,2): 1.05,
                (1,0): 0.92, (1,1): 0.98, (1,2): 1.05,
                (2,0): 0.88, (2,1): 0.95, (2,2): 1.05,
                (3,0): 0.60, (3,1): 0.88, (3,2): 1.05,
            }.get((balls, strikes), 1.00)
            swing_prob = max(0.10, min(0.95, batter_zone_swing * count_zone_adj))
        else:
            # Use batter's actual chase rate, adjusted by count
            count_chase_adj = {
                (0,0): 0.97, (0,1): 1.09, (0,2): 1.26,
                (1,0): 0.92, (1,1): 1.05, (1,2): 1.25,
                (2,0): 0.81, (2,1): 1.00, (2,2): 1.21,
                (3,0): 0.46, (3,1): 0.90, (3,2): 1.17,
            }.get((balls, strikes), 1.00)
            swing_prob = max(0.02, min(0.70, batter_chase_direct * count_chase_adj))

        swings = random.random() < swing_prob

        if record_pitches:
            pitch_log.append({
                "pitch_type": pt,
                "count_before": f"{balls}-{strikes}",
                "in_zone": in_zone,
                "swings": swings,
                "velo": round(selected['avg_velo'], 1),
            })

        if swings:
            # Batter whiff modifier relative to league average
            # batter_whiff is per-pitch; LEAGUE_WHIFF=0.267 per-pitch
            # modifier < 1.0 = contact hitter, > 1.0 = free swinger
            batter_whiff_mod = batter_whiff / LEAGUE_WHIFF

            if in_zone:
                # Pitcher stuff (80%) + batter contact tendency (20%)
                batter_contact_mod = (1.0 - batter_contact) / (1.0 - 0.78)
                # Apply batter whiff modifier directly — contact hitters suppress K%
                whiff_prob = pitcher_whiff * (0.65 + 0.35 * batter_whiff_mod) * k_mult
            else:
                # Out of zone: pitcher (70%) + batter chase/whiff (30%)
                whiff_prob = pitcher_whiff * (0.70 + 0.30 * batter_whiff_mod) * k_mult
            whiff_prob = min(whiff_prob, 0.95)
            if random.random() < whiff_prob:
                strikes += 1
                if record_pitches:
                    pitch_log[-1]["outcome"] = "whiff"
                if strikes >= 3:
                    return {"result": "strikeout",
                            "pitches": pitch_log,
                            "pitch_count": pitch_count}
            else:
                # Foul probability by strike count: batters foul off ~70% of
                # early-count contact (extending the PA), dropping to 35% in
                # 2-strike counts (more desperate, more fair contact).
                # Calibrated so first-pitch BIP ≈ 7% (real MLB) vs 14% with
                # the old flat 0.40, which was also suppressing walk rates.
                # Foul rate scales with batter contact ability
                # TruMedia: high contact batters (Arraez 92%) foul more (43.7%)
                # Low contact batters (Schwarber 67%) foul less (38.9%)
                base_foul = {0: 0.48, 1: 0.46, 2: 0.40}.get(strikes, 0.44)
                contact_scale = max(0.85, min(1.15, batter_contact / 0.78))
                foul_prob = max(0.28, min(0.58, base_foul * contact_scale))
                if random.random() < foul_prob:
                    if strikes < 2:
                        strikes += 1
                    if record_pitches:
                        pitch_log[-1]["outcome"] = "foul"
                else:
                    result = resolve_contact_v2(pt, batter_tends,
                                                batter_contact_mult * pitcher_contact_mult,
                                                batter_hr_mult * pitcher_hr_mult,
                                                weather_hr_mult,
                                                defense=defense)
                    if record_pitches:
                        pitch_log[-1]["outcome"] = result
                    return {"result": result,
                            "pitches": pitch_log,
                            "pitch_count": pitch_count}
        else:
            if in_zone:
                strikes += 1
                if record_pitches:
                    pitch_log[-1]["outcome"] = "called_strike"
                if strikes >= 3:
                    return {"result": "strikeout",
                            "pitches": pitch_log,
                            "pitch_count": pitch_count}
            else:
                balls += 1
                if record_pitches:
                    pitch_log[-1]["outcome"] = "ball"
                walk_threshold = max(3, round(4 / bb_mult)) if bb_mult > 1.0 else 4
                if balls >= walk_threshold:
                    return {"result": "walk",
                            "pitches": pitch_log,
                            "pitch_count": pitch_count}

    return {"result": "field_out", "pitches": pitch_log, "pitch_count": pitch_count}


def aggregate_outcomes(results: list, n_sims: int) -> dict:
    """Aggregate simulation results into summary stats."""
    outcome_counts = defaultdict(int)
    pa_lengths = []
    for r in results:
        outcome_counts[r['result']] += 1
        pa_lengths.append(r.get('pitch_count', 0))

    hits = (outcome_counts['single'] + outcome_counts['double'] +
            outcome_counts['triple'] + outcome_counts['home_run'])
    total_bases = (outcome_counts['single'] +
                   outcome_counts['double'] * 2 +
                   outcome_counts['triple'] * 3 +
                   outcome_counts['home_run'] * 4)
    ab = n_sims - outcome_counts['walk']

    avg = hits / max(ab, 1)
    slg = total_bases / max(ab, 1)
    obp = (hits + outcome_counts['walk']) / max(n_sims, 1)

    expected_rv = sum(
        RUN_VALUES.get(outcome, 0) * cnt / n_sims
        for outcome, cnt in outcome_counts.items()
    )

    return {
        "avg": round(avg, 3),
        "obp": round(obp, 3),
        "slg": round(slg, 3),
        "ops": round(obp + slg, 3),
        "hit_pct": round(hits / n_sims * 100, 1),
        "hr_pct": round(outcome_counts['home_run'] / n_sims * 100, 1),
        "k_pct": round(outcome_counts['strikeout'] / n_sims * 100, 1),
        "bb_pct": round(outcome_counts['walk'] / n_sims * 100, 1),
        "avg_pitches_per_pa": round(sum(pa_lengths) / max(len(pa_lengths), 1), 1),
        "expected_run_value": round(expected_rv, 3),
        "outcome_counts": dict(outcome_counts),
        "n_simulations": n_sims,
    }


def run_simulation(pitcher_id: int, batter_id: int,
                   n_sims: int, db: Session,
                   record_sample: int = 10,
                   balls_start: int = 0,
                   strikes_start: int = 0,
                   game_pk: int = None,
                   on_first: bool = False,
                   on_second: bool = False,
                   on_third: bool = False,
                   outs: int = 0,
                   tto: int = 1,
                   season: int = 2026,
                   opp_team_id: int = None) -> dict:
    """
    Full simulation runner. Returns aggregated results + sample PAs.

    Pitcher adjustment priority: Goose+3 (Stuff→whiff, G2→contact/HR) → pK+/pHR+ fallback.
    Split layers combined multiplicatively: base-out state × TTO × head-to-head history.
    """
    bat_side = db.execute(text("""
        SELECT bat_side FROM mlb.at_bats
        WHERE batter_id = :bid AND bat_side IS NOT NULL LIMIT 1
    """), {"bid": batter_id}).scalar() or 'R'

    count_mix = load_pitcher_count_mix(pitcher_id, bat_side, db)
    lg_mix = load_league_count_mix(db)
    fallback_mix = load_pitcher_overall_mix(pitcher_id, bat_side, db)
    batter_tends = load_batter_tends(batter_id, db)

    if not fallback_mix and not count_mix:
        return {"error": "No pitcher mix data found"}

    # Primary: Goose+3 (Stuff Score → whiff, Goose+2 → contact/HR); fallback: pK+/pHR+
    pitcher_adj = load_goose3_adjustments(pitcher_id, bat_side, db)
    using_goose3 = bool(pitcher_adj)
    if not pitcher_adj:
        pitcher_adj = load_pk_phr_adjustments(pitcher_id, bat_side, db)

    juiced2_adj  = load_juiced2_batter_adjustments(batter_id, db)
    zone_rate    = load_pitcher_zone_rate(pitcher_id, db)
    weather_mult = load_weather_modifier(game_pk, db)
    defense      = load_team_defense(opp_team_id, db) if opp_team_id else {}

    # Three split layers; combine multiplicatively, clamp to [0.60, 1.50]
    base_splits = load_game_state_adjustments(
        pitcher_id, batter_id, on_first, on_second, on_third, outs, db
    )
    tto_adj = load_tto_adjustment(pitcher_id, season, tto, db)
    h2h_adj = load_h2h_adjustment(pitcher_id, batter_id, db)

    splits_adj = _combine_mults(base_splits, tto_adj, h2h_adj)
    if base_splits:
        splits_adj['state_label'] = base_splits.get('state_label', '')
        splits_adj['base_context'] = base_splits.get('base_context', '')

    if pitcher_adj:
        adj_label = 'Goose+3' if using_goose3 else 'pK+/pHR+'
        print(f"  Applying {adj_label} adjustments for {len(pitcher_adj)} pitch types")
    if juiced2_adj:
        print(f"  Applying Juiced+2 batter adjustment: "
              f"{juiced2_adj.get('juiced2_plus', 100):.1f}")
    if weather_mult != 1.0:
        print(f"  Weather HR modifier: {weather_mult:.3f}")
    if tto_adj:
        print(f"  TTO {tto} adjustment: k_mult={tto_adj.get('k_mult', 1.0):.3f} "
              f"(PA={tto_adj.get('tto_pa', 0)})")
    if h2h_adj:
        print(f"  H2H adjustment: k_mult={h2h_adj.get('k_mult', 1.0):.3f} "
              f"(PA={h2h_adj.get('pa', 0)})")
    if splits_adj:
        print(f"  Combined splits: {splits_adj.get('state_label', '')} "
              f"k={splits_adj.get('k_mult', 1.0):.3f} "
              f"bb={splits_adj.get('bb_mult', 1.0):.3f} "
              f"hr={splits_adj.get('hr_mult', 1.0):.3f}")

    results = []
    sample_pas = []

    for i in range(n_sims):
        record = i < record_sample
        pa = simulate_pa(count_mix, lg_mix, fallback_mix,
                         batter_tends, record_pitches=record,
                         balls_start=balls_start, strikes_start=strikes_start,
                         goose2_adj=pitcher_adj if pitcher_adj else None,
                         juiced2_adj=juiced2_adj if juiced2_adj else None,
                         weather_hr_mult=weather_mult,
                         splits_adj=splits_adj if splits_adj else None,
                         zone_rate=zone_rate,
                         defense=defense if defense else None)
        results.append(pa)
        if record:
            sample_pas.append(pa)

    summary = aggregate_outcomes(results, n_sims)

    pitch_counts = defaultdict(int)
    for r in results:
        if r.get('pitches'):
            for p in r['pitches']:
                pitch_counts[p['pitch_type']] += 1

    total_pitches = sum(pitch_counts.values())
    pitch_distribution = {
        pt: {
            "count": cnt,
            "pct": round(cnt / max(total_pitches, 1) * 100, 1)
        }
        for pt, cnt in sorted(pitch_counts.items(), key=lambda x: -x[1])
    }

    count_mix_used = {}
    for (b, s), mix in count_mix.items():
        total = sum(p['pitches'] for p in mix)
        if total >= MIN_COUNT_PITCHES:
            count_mix_used[f"{b}-{s}"] = "pitcher_data"
        else:
            count_mix_used[f"{b}-{s}"] = "lg_blend"

    model_context = {}
    for pt, adj in pitcher_adj.items():
        if using_goose3:
            model_context[pt] = {
                'goose3_plus':  adj.get('goose3_plus'),
                'stuff_score':  adj.get('stuff_score'),
                'goose2_score': adj.get('goose2_score'),
                'gap':          adj.get('gap'),
                'whiff_mult':   adj.get('whiff_mult'),
                'contact_mult': adj.get('contact_mult'),
                'hr_mult':      adj.get('hr_mult'),
            }
        else:
            model_context[pt] = {
                'pk_plus':      adj.get('pk_plus'),
                'phr_plus':     adj.get('phr_plus'),
                'stuff_score':  adj.get('stuff_score'),
                'whiff_mult':   adj.get('whiff_mult'),
                'contact_mult': adj.get('contact_mult'),
                'hr_mult':      adj.get('hr_mult'),
            }
    summary['model_context'] = model_context
    summary['juiced2_context'] = juiced2_adj or {}

    return {
        "splits_context": splits_adj or {},
        "tto_context": tto_adj or {},
        "h2h_context": h2h_adj or {},
        "bat_side": bat_side,
        "results": summary,
        "outcome_distribution": {
            k: {"count": v, "pct": round(v / n_sims * 100, 1)}
            for k, v in sorted(summary["outcome_counts"].items(),
                               key=lambda x: -x[1])
        },
        "pitch_distribution": pitch_distribution,
        "sample_pas": [
            {"result": p["result"],
             "pitch_count": p.get("pitch_count", 0),
             "pitches": p.get("pitches", [])}
            for p in sample_pas
        ],
        "count_mix_source": count_mix_used,
        "data_quality": {
            "has_count_mix": bool(count_mix),
            "count_mix_counts": len(count_mix),
            "has_batter_tends": bool(batter_tends),
            "fallback_pitches": len(fallback_mix),
            "pitcher_adj_model": 'goose3' if using_goose3 else 'pk_phr',
        }
    }


def load_batter_overall_rates(batter_id: int, db) -> dict:
    """
    Batter's overall plate discipline rates across all pitch types.
    Used as fallback when pitch-type specific tendency data is missing.
    """
    from sqlalchemy import text
    sql = text("""
        SELECT
            -- Overall swing rate (swings / total pitches)
            AVG(CASE WHEN p.call_code IN ('S','W','T','F','X','D','E')
                THEN 1.0 ELSE 0.0 END) as swing_rate,
            -- Whiff rate on swings (whiffs / swings)
            AVG(CASE WHEN p.call_code IN ('S','W','T','F','X','D','E')
                THEN CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END
                ELSE NULL END) as whiff_rate,
            -- Chase rate = out-of-zone swings / out-of-zone pitches
            AVG(CASE WHEN p.zone NOT BETWEEN 1 AND 9
                THEN CASE WHEN p.call_code IN ('S','W','T','F','X','D','E')
                    THEN 1.0 ELSE 0.0 END
                ELSE NULL END) as chase_rate,
            -- Zone swing rate = in-zone swings / in-zone pitches
            AVG(CASE WHEN p.zone BETWEEN 1 AND 9
                THEN CASE WHEN p.call_code IN ('S','W','T','F','X','D','E')
                    THEN 1.0 ELSE 0.0 END
                ELSE NULL END) as zone_swing_rate,
            -- Contact rate = non-whiff swings / swings
            AVG(CASE WHEN p.call_code IN ('S','W','T','F','X','D','E')
                THEN CASE WHEN p.call_code NOT IN ('S','W','T') THEN 1.0 ELSE 0.0 END
                ELSE NULL END) as contact_rate,
            COUNT(*) as pitches
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE ab.batter_id = :bid
        AND g.season = 2026 AND g.game_type = 'R'
        AND p.call_code IN ('B','C','S','W','T','F','X','D','E')
        HAVING COUNT(*) >= 30
    """)
    row = db.execute(sql, {"bid": batter_id}).mappings().first()
    if row and row['whiff_rate'] is not None:
        return {
            'swing_rate': float(row['swing_rate'] or 0.460),
            'whiff_rate': float(row['whiff_rate'] or 0.267),
            'chase_rate': float(row['chase_rate'] or 0.310),
            'zone_swing_rate': float(row['zone_swing_rate'] or 0.670),
            'contact_rate': float(row['contact_rate'] or 0.78),
        }
    return {}
