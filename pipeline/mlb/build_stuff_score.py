"""
Stuff Score — empirically weighted pitch quality model.

Three components:
1. Physical Score (40%): velo, IVB, HBreak, spin weighted by empirical
   correlations with whiff/CSW/SLG outcomes per pitch type
2. Location Score (35%): zone quality weighted by whiff/CSW/SLG outcomes
   per zone per pitch type, using batter-quality-adjusted whiff rates
3. Tunnel Score (25%): movement divergence vs pitcher's own fastball

All scores indexed to 100 = league average for that pitch type.
"""

import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import argparse
import numpy as np
import pandas as pd
from database import SessionLocal
from sqlalchemy import text

# ── CONSTANTS ─────────────────────────────────────────────────────────────────
LEAGUE_AVG_WHIFF = 0.267

# Empirical Pearson r between physical characteristics and outcomes per pitch type.
# For whiff/CSW: positive = higher value → better outcome for pitcher.
# For SLG: raw correlation coefficients (negative = higher char → lower SLG → better).
#   The formula negates these so that the sign convention is: positive contribution
#   means the pitcher is above average at suppressing SLG with that characteristic.
EMPIRICAL_WEIGHTS = {
    'FF': {
        'whiff': {'velo':  0.284, 'ivb': -0.013, 'hbreak':  0.074, 'spin':  0.236},
        'csw':   {'velo':  0.057, 'ivb':  0.186, 'hbreak': -0.040, 'spin':  0.139},
        'slg':   {'velo': -0.107, 'ivb': -0.105, 'hbreak':  0.131, 'spin': -0.025},
    },
    'SI': {
        'whiff': {'velo':  0.205, 'ivb':  0.182, 'hbreak': -0.288, 'spin':  0.110},
        'csw':   {'velo': -0.078, 'ivb': -0.177, 'hbreak':  0.216, 'spin': -0.042},
        'slg':   {'velo': -0.150, 'ivb': -0.048, 'hbreak':  0.048, 'spin': -0.058},
    },
    'SL': {
        'whiff': {'velo':  0.091, 'ivb': -0.103, 'hbreak': -0.069, 'spin':  0.064},
        'csw':   {'velo':  0.073, 'ivb': -0.100, 'hbreak':  0.066, 'spin':  0.151},
        'slg':   {'velo':  0.017, 'ivb':  0.002, 'hbreak': -0.091, 'spin': -0.154},
    },
    'CH': {
        'whiff': {'velo': -0.029, 'ivb':  0.125, 'hbreak': -0.108, 'spin':  0.005},
        'csw':   {'velo': -0.091, 'ivb':  0.233, 'hbreak': -0.005, 'spin':  0.103},
        'slg':   {'velo': -0.097, 'ivb': -0.015, 'hbreak':  0.085, 'spin':  0.158},
    },
    'CU': {
        'whiff': {'velo':  0.206, 'ivb': -0.084, 'hbreak':  0.074, 'spin':  0.123},
        'csw':   {'velo': -0.064, 'ivb':  0.116, 'hbreak':  0.087, 'spin':  0.058},
        'slg':   {'velo':  0.017, 'ivb': -0.081, 'hbreak': -0.069, 'spin':  0.014},
    },
    'FC': {
        'whiff': {'velo': -0.022, 'ivb': -0.185, 'hbreak':  0.063, 'spin':  0.101},
        'csw':   {'velo': -0.144, 'ivb': -0.162, 'hbreak':  0.079, 'spin':  0.183},
        'slg':   {'velo': -0.278, 'ivb':  0.008, 'hbreak': -0.126, 'spin': -0.255},
    },
    'ST': {
        'whiff': {'velo':  0.157, 'ivb': -0.089, 'hbreak': -0.074, 'spin':  0.023},
        'csw':   {'velo':  0.123, 'ivb':  0.013, 'hbreak': -0.018, 'spin':  0.087},
        'slg':   {'velo': -0.026, 'ivb': -0.017, 'hbreak':  0.029, 'spin':  0.148},
    },
}

OUTCOME_WEIGHTS    = {'whiff': 0.40, 'slg': 0.35, 'csw': 0.25}
COMPONENT_WEIGHTS  = {
    'weighted_whiff': 0.45,
    'physical':       0.35,
    'location':       0.15,
    'tunnel':         0.05,
}
ZONE_OUTCOME_WEIGHTS = {'whiff': 0.40, 'slg': 0.35, 'csw': 0.25}

REGRESSION_K = 200
PITCH_TYPES  = ('FF', 'SI', 'SL', 'CH', 'CU', 'FC', 'ST')
SCALE        = 18.0


# ── HELPERS ───────────────────────────────────────────────────────────────────

def normalize_weights(weights: dict) -> dict:
    """Normalize weights so absolute values sum to 1."""
    total = sum(abs(v) for v in weights.values())
    if total == 0:
        return {k: 0.0 for k in weights}
    return {k: v / total for k, v in weights.items()}


def regress_to_mean(score: float, n: int, k: int = REGRESSION_K) -> float:
    w = n / (n + k)
    return round(100 + w * (score - 100), 1)


# ── BATTER WHIFF WEIGHTS ──────────────────────────────────────────────────────

def compute_batter_whiff_weights(season: int, db) -> dict:
    """
    Compute each batter's quality adjustment weight.
    Weight = LEAGUE_AVG_WHIFF / batter_whiff_rate, capped at 5x.
    Arraez (~5% whiff) → 5.3x weight; average batter → 1.0x; free swinger → 0.67x.
    """
    sql = text("""
        SELECT ab.batter_id,
            SUM(CASE WHEN p.call_code IN ('S','W','T') THEN 1 ELSE 0 END)::float /
            NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','X','D','E')
                THEN 1 ELSE 0 END), 0) as whiff_rate,
            SUM(CASE WHEN p.call_code IN ('S','W','T','F','X','D','E')
                THEN 1 ELSE 0 END) as swings
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE g.season = :season AND g.game_type = 'R'
        GROUP BY ab.batter_id
        HAVING SUM(CASE WHEN p.call_code IN ('S','W','T','F','X','D','E')
            THEN 1 ELSE 0 END) >= 50
    """)
    rows = db.execute(sql, {"season": season}).mappings().all()

    weights = {}
    for r in rows:
        wr = float(r['whiff_rate'] or LEAGUE_AVG_WHIFF)
        wr = max(wr, 0.02)
        weights[int(r['batter_id'])] = min(LEAGUE_AVG_WHIFF / wr, 5.0)

    vals = list(weights.values())
    print(f"  Batter whiff weights: n={len(weights)} "
          f"min={min(vals):.2f} avg={sum(vals)/len(vals):.2f} max={max(vals):.2f}")
    return weights


# ── ZONE VALUES ───────────────────────────────────────────────────────────────

def compute_zone_values(baseline_season: int, db) -> pd.DataFrame:
    """
    Compute league average outcomes per pitch type × zone.
    Whiff rate is batter-quality-adjusted: whiffs against contact hitters
    count more than whiffs against free swingers.
    """
    print(f"\n  Computing zone values from {baseline_season}...")

    whiff_sql = text("""
        WITH batter_weights AS (
            SELECT ab2.batter_id,
                LEAST(
                    :lg_whiff / NULLIF(
                        SUM(CASE WHEN p2.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END) /
                        NULLIF(SUM(CASE WHEN p2.call_code IN ('S','W','T','F','X','D','E')
                            THEN 1.0 ELSE 0.0 END), 0)
                    , 0),
                    5.0
                ) AS weight
            FROM mlb.pitches p2
            JOIN mlb.at_bats ab2 ON ab2.game_pk = p2.game_pk
                AND ab2.at_bat_index = p2.at_bat_index
            JOIN mlb.games g2 ON g2.game_pk = p2.game_pk
            WHERE g2.season = :season AND g2.game_type = 'R'
            GROUP BY ab2.batter_id
            HAVING SUM(CASE WHEN p2.call_code IN ('S','W','T','F','X','D','E')
                THEN 1 ELSE 0 END) >= 50
        )
        SELECT p.pitch_type_code, p.zone,
            SUM(CASE WHEN p.call_code IN ('S','W','T')
                THEN COALESCE(bw.weight, 1.0) ELSE 0.0 END) /
            NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','X','D','E')
                THEN 1.0 ELSE 0.0 END), 0) AS whiff_pct,
            AVG(CASE WHEN p.call_code IN ('C','S','W','T')
                THEN 1.0 ELSE 0.0 END) AS csw_pct,
            COUNT(*) AS pitches
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        LEFT JOIN batter_weights bw ON bw.batter_id = ab.batter_id
        WHERE g.season = :season AND g.game_type = 'R'
        AND p.pitch_type_code IN :pitch_types
        AND p.zone BETWEEN 1 AND 14
        AND p.call_code IN ('S','W','T','F','X','D','E','C','B')
        GROUP BY p.pitch_type_code, p.zone
        HAVING COUNT(*) >= 100
    """)

    slg_sql = text("""
        SELECT p.pitch_type_code, p.zone,
            AVG(CASE ab.event_type
                WHEN 'single'   THEN 1.0
                WHEN 'double'   THEN 2.0
                WHEN 'triple'   THEN 3.0
                WHEN 'home_run' THEN 4.0
                ELSE 0.0
            END) AS slg_against,
            COUNT(DISTINCT ab.id) AS pa
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE g.season = :season AND g.game_type = 'R'
        AND p.pitch_type_code IN :pitch_types
        AND p.zone BETWEEN 1 AND 14
        AND ab.event_type IN (
            'single','double','triple','home_run',
            'field_out','force_out','grounded_into_double_play',
            'double_play','fielders_choice','fielders_choice_out',
            'sac_fly','sac_bunt','field_error','strikeout_double_play'
        )
        AND p.pitch_number = (
            SELECT MAX(p2.pitch_number) FROM mlb.pitches p2
            WHERE p2.game_pk = p.game_pk AND p2.at_bat_index = p.at_bat_index
        )
        GROUP BY p.pitch_type_code, p.zone
        HAVING COUNT(DISTINCT ab.id) >= 20
    """)

    params = {"season": baseline_season, "lg_whiff": LEAGUE_AVG_WHIFF,
              "pitch_types": PITCH_TYPES}
    whiff_rows = db.execute(whiff_sql, params).mappings().all()
    slg_rows = db.execute(slg_sql,
                          {"season": baseline_season, "pitch_types": PITCH_TYPES}
                          ).mappings().all()

    df_whiff = pd.DataFrame([dict(r) for r in whiff_rows])
    df_slg   = pd.DataFrame([dict(r) for r in slg_rows])

    if df_whiff.empty:
        print("  No zone data found")
        return pd.DataFrame()

    df = df_whiff.merge(
        df_slg[['pitch_type_code', 'zone', 'slg_against', 'pa']],
        on=['pitch_type_code', 'zone'], how='left'
    )

    slg_max = float(df['slg_against'].max() or 1.0)
    slg_min = float(df['slg_against'].min() or 0.0)

    def zone_value(row):
        whiff = float(row['whiff_pct'] or 0)
        csw   = float(row['csw_pct'] or 0)
        slg   = float(row['slg_against'] if pd.notna(row.get('slg_against')) else slg_max)
        slg_norm = 1.0 - (slg - slg_min) / max(slg_max - slg_min, 0.001)
        return (
            whiff * ZONE_OUTCOME_WEIGHTS['whiff'] +
            slg_norm * ZONE_OUTCOME_WEIGHTS['slg'] +
            csw * ZONE_OUTCOME_WEIGHTS['csw']
        )

    df['zone_value'] = df.apply(zone_value, axis=1)
    print(f"  Computed {len(df)} zone value rows")
    return df


def store_zone_values(df: pd.DataFrame, baseline_season: int, db):
    upsert = text("""
        INSERT INTO mlb.zone_values (
            baseline_season, pitch_type_code, zone,
            whiff_pct, csw_pct, slg_against, pitches, pa, zone_value
        ) VALUES (
            :baseline_season, :pitch_type_code, :zone,
            :whiff_pct, :csw_pct, :slg_against, :pitches, :pa, :zone_value
        )
        ON CONFLICT (baseline_season, pitch_type_code, zone) DO UPDATE SET
            whiff_pct   = EXCLUDED.whiff_pct,
            csw_pct     = EXCLUDED.csw_pct,
            slg_against = EXCLUDED.slg_against,
            zone_value  = EXCLUDED.zone_value,
            computed_at = NOW()
    """)
    stored = 0
    for _, r in df.iterrows():
        def fv(col):
            v = r.get(col)
            return float(v) if pd.notna(v) else None
        def fi(col):
            v = r.get(col)
            return int(v) if pd.notna(v) else None
        db.execute(upsert, {
            "baseline_season": baseline_season,
            "pitch_type_code": r['pitch_type_code'],
            "zone":            int(r['zone']),
            "whiff_pct":       fv('whiff_pct'),
            "csw_pct":         fv('csw_pct'),
            "slg_against":     fv('slg_against'),
            "pitches":         fi('pitches'),
            "pa":              fi('pa'),
            "zone_value":      fv('zone_value'),
        })
        stored += 1
    db.commit()
    print(f"  Stored {stored} zone value rows (season {baseline_season})")


# ── LEAGUE AVERAGES ───────────────────────────────────────────────────────────

def compute_league_physical_avgs(baseline_season: int, db) -> dict:
    """
    League average and std dev of physical characteristics per pitch type.
    Aggregates to pitcher level first so std dev reflects pitcher-to-pitcher
    variation, not pitch-to-pitch noise within a single pitcher's arsenal.
    Also computes avg/std of batter-quality-weighted whiff rate per pitch type.
    """
    sql = text("""
        SELECT pitch_type_code,
            AVG(avg_velo)    AS avg_velo,    STDDEV(avg_velo)    AS std_velo,
            AVG(avg_ivb)     AS avg_ivb,     STDDEV(avg_ivb)     AS std_ivb,
            AVG(avg_hbreak)  AS avg_hbreak,  STDDEV(avg_hbreak)  AS std_hbreak,
            AVG(avg_spin)    AS avg_spin,    STDDEV(avg_spin)    AS std_spin,
            AVG(weighted_whiff)  AS avg_weighted_whiff,
            STDDEV(weighted_whiff) AS std_weighted_whiff,
            COUNT(*) AS n_pitchers
        FROM (
            SELECT p.pitch_type_code, ab.pitcher_id,
                AVG(p.start_speed)  AS avg_velo,
                AVG(ABS(p.pfx_z))   AS avg_ivb,
                AVG(ABS(p.pfx_x))   AS avg_hbreak,
                AVG(p.spin_rate)    AS avg_spin,
                SUM(CASE WHEN p.call_code IN ('S','W','T')
                    THEN COALESCE(0.267 / NULLIF(bw.whiff_weight, 0), 1.0)
                    ELSE 0.0 END) /
                NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','X','D','E')
                    THEN 1.0 ELSE 0.0 END), 0) AS weighted_whiff,
                COUNT(*) AS n
            FROM mlb.pitches p
            JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
                AND ab.at_bat_index = p.at_bat_index
            JOIN mlb.games g ON g.game_pk = p.game_pk
            LEFT JOIN (
                SELECT ab2.batter_id,
                    LEAST(
                        0.267 / NULLIF(
                            AVG(CASE WHEN p2.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END) /
                            NULLIF(AVG(CASE WHEN p2.call_code IN ('S','W','T','F','X','D','E')
                                THEN 1.0 ELSE 0.0 END), 0)
                        , 0),
                        5.0
                    ) AS whiff_weight
                FROM mlb.pitches p2
                JOIN mlb.at_bats ab2 ON ab2.game_pk = p2.game_pk
                    AND ab2.at_bat_index = p2.at_bat_index
                JOIN mlb.games g2 ON g2.game_pk = p2.game_pk
                WHERE g2.season = :season AND g2.game_type = 'R'
                GROUP BY ab2.batter_id
                HAVING SUM(CASE WHEN p2.call_code IN ('S','W','T','F','X','D','E')
                    THEN 1 ELSE 0 END) >= 50
            ) bw ON bw.batter_id = ab.batter_id
            WHERE g.season = :season AND g.game_type = 'R'
            AND p.pitch_type_code IN :pitch_types
            AND p.start_speed IS NOT NULL
            AND p.pfx_z IS NOT NULL AND p.pfx_x IS NOT NULL
            AND p.spin_rate IS NOT NULL
            GROUP BY p.pitch_type_code, ab.pitcher_id
            HAVING COUNT(*) >= 20
        ) pitcher_avgs
        GROUP BY pitch_type_code
    """)
    rows = db.execute(sql, {"season": baseline_season,
                             "pitch_types": PITCH_TYPES}).mappings().all()
    avgs = {r['pitch_type_code']: dict(r) for r in rows}
    for pt, d in avgs.items():
        print(f"    {pt}: velo {float(d['avg_velo']):.1f}±{float(d['std_velo'] or 1.5):.1f} "
              f"ivb {float(d['avg_ivb']):.3f}±{float(d['std_ivb'] or 0.05):.3f} "
              f"wtd_whiff {float(d['avg_weighted_whiff'] or 0):.3f}±{float(d['std_weighted_whiff'] or 0.05):.3f}")
    return avgs


def compute_league_zone_avg(zone_df: pd.DataFrame) -> dict:
    """Usage-weighted mean zone value per pitch type."""
    avgs = {}
    for pt in zone_df['pitch_type_code'].unique():
        pt_df = zone_df[zone_df['pitch_type_code'] == pt]
        total = pt_df['pitches'].sum()
        if total > 0:
            avgs[pt] = float((pt_df['zone_value'] * pt_df['pitches']).sum() / total)
    return avgs


def compute_league_zone_std(db, season: int) -> dict:
    """
    Std dev of each pitcher's usage-weighted zone value per pitch type.
    Used to z-score normalize location scores.
    """
    sql = text("""
        WITH pitcher_zone_counts AS (
            SELECT ab.pitcher_id, p.pitch_type_code, p.zone, COUNT(*) AS pitches
            FROM mlb.pitches p
            JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
                AND ab.at_bat_index = p.at_bat_index
            JOIN mlb.games g ON g.game_pk = p.game_pk
            WHERE g.season = :season AND g.game_type = 'R'
            AND p.zone BETWEEN 1 AND 14
            AND p.pitch_type_code IN :pitch_types
            GROUP BY ab.pitcher_id, p.pitch_type_code, p.zone
        ),
        pitcher_weighted_zone AS (
            SELECT pzc.pitcher_id, pzc.pitch_type_code,
                SUM(zv.zone_value * pzc.pitches) /
                    NULLIF(SUM(pzc.pitches), 0) AS weighted_zone_val
            FROM pitcher_zone_counts pzc
            JOIN mlb.zone_values zv
                ON  zv.pitch_type_code = pzc.pitch_type_code
                AND zv.zone = pzc.zone
                AND zv.baseline_season = :season
            GROUP BY pzc.pitcher_id, pzc.pitch_type_code
            HAVING SUM(pzc.pitches) >= 20
        )
        SELECT pitch_type_code,
            STDDEV(weighted_zone_val) AS std_zone_val,
            COUNT(*) AS n_pitchers
        FROM pitcher_weighted_zone
        GROUP BY pitch_type_code
    """)
    rows = db.execute(sql, {"season": season, "pitch_types": PITCH_TYPES}).mappings().all()
    return {r['pitch_type_code']: float(r['std_zone_val'] or 0.02) for r in rows}


# ── PHYSICAL SCORE ────────────────────────────────────────────────────────────

# Scale factor: a pitcher with composite z-score of 1.0 → score of 112.
# Chosen empirically: with ~4 weighted characteristics, top pitchers accumulate
# z-scores of 1.5-2.5 across dimensions, mapping to ~118-130 range.
PHYSICAL_SCALE = 18.0


def compute_physical_score(pitcher_vals: dict, league_avgs: dict,
                            pitch_type: str) -> float:
    """
    Score pitcher's physical characteristics using z-score normalization.
    100 = league average; +12 per composite standard deviation above average.
    Capped [50, 160].
    """
    weights = EMPIRICAL_WEIGHTS.get(pitch_type)
    lg = league_avgs.get(pitch_type)
    if not weights or not lg:
        return 100.0

    p_velo   = float(pitcher_vals.get('avg_velo')   or 0)
    p_ivb    = float(pitcher_vals.get('avg_ivb')    or 0)
    p_hbreak = float(pitcher_vals.get('avg_hbreak') or 0)
    p_spin   = float(pitcher_vals.get('avg_spin')   or 0)

    def z(val, mean_key, std_key, std_default):
        mean = float(lg.get(mean_key) or 0)
        std  = float(lg.get(std_key)  or std_default)
        return (val - mean) / max(std, 0.001)

    z_velo   = z(p_velo,   'avg_velo',   'std_velo',   1.5)
    z_ivb    = z(p_ivb,    'avg_ivb',    'std_ivb',    0.05)
    z_hbreak = z(p_hbreak, 'avg_hbreak', 'std_hbreak', 0.05)
    z_spin   = z(p_spin,   'avg_spin',   'std_spin',   200.0)

    char_z = {'velo': z_velo, 'ivb': z_ivb, 'hbreak': z_hbreak, 'spin': z_spin}

    composite_z = 0.0
    for outcome, o_weight in OUTCOME_WEIGHTS.items():
        w = normalize_weights(weights[outcome])
        # For SLG: weights are raw Pearson r values where negative r means
        # higher characteristic → lower SLG → better for pitcher.
        # Negate so that positive contribution = better outcome for pitcher.
        sign = -1.0 if outcome == 'slg' else 1.0
        outcome_z = sign * sum(w[c] * char_z[c] for c in char_z)
        composite_z += outcome_z * o_weight

    score = 100.0 + composite_z * PHYSICAL_SCALE
    return round(max(50.0, min(160.0, score)), 1)


# ── LOCATION SCORE ────────────────────────────────────────────────────────────

LOCATION_SCALE = 18.0


def compute_location_score(pitcher_id: int, pitch_type: str,
                            zone_values: dict, league_zone_avgs: dict,
                            league_zone_stds: dict,
                            db, season: int) -> tuple:
    """
    Score pitcher's zone distribution using z-score normalization.
    100 = league average location quality; +12 per std above average.
    Returns (score, zone_distribution).
    """
    sql = text("""
        SELECT p.zone, COUNT(*) AS pitches
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE ab.pitcher_id = :pid
        AND p.pitch_type_code = :pt
        AND g.season = :season AND g.game_type = 'R'
        AND p.zone BETWEEN 1 AND 14
        GROUP BY p.zone
    """)
    rows = db.execute(sql, {"pid": pitcher_id, "pt": pitch_type,
                             "season": season}).mappings().all()
    if not rows:
        return 100.0, {}

    total = sum(r['pitches'] for r in rows)
    if total == 0:
        return 100.0, {}

    weighted_zv = 0.0
    zone_dist = {}
    for r in rows:
        zone = int(r['zone'])
        freq = r['pitches'] / total
        zv = zone_values.get((pitch_type, zone), {}).get('zone_value')
        if zv is not None:
            weighted_zv += freq * float(zv)
        zone_dist[zone] = int(r['pitches'])

    lg_avg = league_zone_avgs.get(pitch_type, 0.15)
    lg_std = league_zone_stds.get(pitch_type, 0.02)

    if lg_std == 0:
        return 100.0, zone_dist

    z_loc = (weighted_zv - lg_avg) / lg_std
    score = 100.0 + z_loc * LOCATION_SCALE
    return round(max(50.0, min(160.0, score)), 1), zone_dist


# ── TUNNEL SCORE ──────────────────────────────────────────────────────────────

def compute_tunnel_score(pitcher_id: int, pitch_type: str,
                          db, season: int) -> float:
    """
    Tunnel score vs pitcher's own fastball.
    Uses z-score normalization like physical score.
    """
    if pitch_type in ('FF', 'SI', 'FC'):
        sql = text("""
            SELECT STDDEV(p.x0) AS x0_std, STDDEV(p.z0) AS z0_std,
                   COUNT(*) AS pitches
            FROM mlb.pitches p
            JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
                AND ab.at_bat_index = p.at_bat_index
            JOIN mlb.games g ON g.game_pk = p.game_pk
            WHERE ab.pitcher_id = :pid
            AND p.pitch_type_code = :pt
            AND g.season = :season AND g.game_type = 'R'
            AND p.x0 IS NOT NULL AND p.z0 IS NOT NULL
        """)
        row = db.execute(sql, {"pid": pitcher_id, "pt": pitch_type,
                                "season": season}).mappings().first()
        if not row or not row['x0_std'] or int(row['pitches'] or 0) < 20:
            return 100.0

        x0_std = float(row['x0_std'])
        z0_std = float(row['z0_std'])
        total_std = x0_std + z0_std

        # League avg total std ≈ 0.25 feet, stddev ≈ 0.08
        # Inverted: lower release-point std = positive z = better
        lg_mean = 0.25
        lg_std = 0.08
        z = (lg_mean - total_std) / lg_std
        score = 100 + z * SCALE * 0.5  # dampen tunnel contribution
        return round(max(70.0, min(130.0, score)), 1)

    # For offspeed/breaking: movement divergence from fastball
    sql = text("""
        SELECT
            AVG(CASE WHEN p.pitch_type_code = :pt   THEN p.pfx_x END) AS pt_pfx_x,
            AVG(CASE WHEN p.pitch_type_code = :pt   THEN p.pfx_z END) AS pt_pfx_z,
            AVG(CASE WHEN p.pitch_type_code IN ('FF','SI') THEN p.pfx_x END) AS fb_pfx_x,
            AVG(CASE WHEN p.pitch_type_code IN ('FF','SI') THEN p.pfx_z END) AS fb_pfx_z,
            COUNT(CASE WHEN p.pitch_type_code = :pt   THEN 1 END) AS pt_pitches,
            COUNT(CASE WHEN p.pitch_type_code IN ('FF','SI') THEN 1 END) AS fb_pitches
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE ab.pitcher_id = :pid
        AND p.pitch_type_code IN (:pt, 'FF', 'SI')
        AND g.season = :season AND g.game_type = 'R'
        AND p.pfx_x IS NOT NULL AND p.pfx_z IS NOT NULL
    """)
    row = db.execute(sql, {"pid": pitcher_id, "pt": pitch_type,
                            "season": season}).mappings().first()

    if not row or not row['fb_pitches'] or int(row['fb_pitches']) < 20:
        return 100.0
    if any(row[k] is None for k in ['pt_pfx_x', 'pt_pfx_z', 'fb_pfx_x', 'fb_pfx_z']):
        return 100.0

    delta_x = abs(float(row['pt_pfx_x']) - float(row['fb_pfx_x']))
    delta_z = abs(float(row['pt_pfx_z']) - float(row['fb_pfx_z']))
    divergence = (delta_x ** 2 + delta_z ** 2) ** 0.5

    # Compute league average divergence per pitch type from DB
    lg_sql = text("""
        SELECT
            AVG(ABS(p.pfx_x - fb.avg_pfx_x) + ABS(p.pfx_z - fb.avg_pfx_z)) AS avg_div,
            STDDEV(ABS(p.pfx_x - fb.avg_pfx_x) + ABS(p.pfx_z - fb.avg_pfx_z)) AS std_div
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        JOIN (
            SELECT ab2.pitcher_id,
                   AVG(p2.pfx_x) AS avg_pfx_x,
                   AVG(p2.pfx_z) AS avg_pfx_z
            FROM mlb.pitches p2
            JOIN mlb.at_bats ab2 ON ab2.game_pk = p2.game_pk
                AND ab2.at_bat_index = p2.at_bat_index
            JOIN mlb.games g2 ON g2.game_pk = p2.game_pk
            WHERE p2.pitch_type_code IN ('FF','SI')
            AND g2.season = :season AND g2.game_type = 'R'
            GROUP BY ab2.pitcher_id
        ) fb ON fb.pitcher_id = ab.pitcher_id
        WHERE p.pitch_type_code = :pt
        AND g.season = :season AND g.game_type = 'R'
        AND p.pfx_x IS NOT NULL AND p.pfx_z IS NOT NULL
    """)
    lg = db.execute(lg_sql, {"pt": pitch_type, "season": season}).mappings().first()

    if not lg or not lg['avg_div'] or not lg['std_div']:
        return 100.0

    lg_mean = float(lg['avg_div'])
    lg_std_val = float(lg['std_div'])
    if lg_std_val == 0:
        return 100.0

    z = (divergence - lg_mean) / lg_std_val
    score = 100 + z * SCALE * 0.5  # dampen tunnel
    return round(max(70.0, min(130.0, score)), 1)


# ── BUILD STUFF SCORES ────────────────────────────────────────────────────────

def build_stuff_scores(score_season: int, baseline_season: int, db):
    """Build Stuff Scores for all qualified pitchers."""
    print(f"\n  Building Stuff Scores for {score_season} "
          f"(baseline: {baseline_season})...")

    # Load pre-computed zone values
    zone_sql = text("""
        SELECT pitch_type_code, zone, zone_value, pitches
        FROM mlb.zone_values WHERE baseline_season = :season
    """)
    zone_rows = db.execute(zone_sql, {"season": baseline_season}).mappings().all()
    zone_values = {(r['pitch_type_code'], int(r['zone'])): dict(r)
                   for r in zone_rows}

    zone_df = pd.DataFrame([dict(r) for r in zone_rows])
    league_zone_avgs = compute_league_zone_avg(zone_df) if not zone_df.empty else {}
    print(f"  Computing league zone stds...")
    league_zone_stds = compute_league_zone_std(db, baseline_season)
    print(f"  League physical avgs...")
    league_phys = compute_league_physical_avgs(baseline_season, db)

    pitcher_sql = text("""
        WITH batter_weights AS (
            SELECT ab2.batter_id,
                LEAST(
                    :lg_whiff / NULLIF(
                        SUM(CASE WHEN p2.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END) /
                        NULLIF(SUM(CASE WHEN p2.call_code IN ('S','W','T','F','X','D','E')
                            THEN 1.0 ELSE 0.0 END), 0)
                    , 0),
                    5.0
                ) AS weight
            FROM mlb.pitches p2
            JOIN mlb.at_bats ab2 ON ab2.game_pk = p2.game_pk
                AND ab2.at_bat_index = p2.at_bat_index
            JOIN mlb.games g2 ON g2.game_pk = p2.game_pk
            WHERE g2.season = :season AND g2.game_type = 'R'
            GROUP BY ab2.batter_id
            HAVING SUM(CASE WHEN p2.call_code IN ('S','W','T','F','X','D','E')
                THEN 1 ELSE 0 END) >= 50
        )
        SELECT
            ab.pitcher_id,
            MAX(ab.pitcher_name)   AS pitcher_name,
            MAX(ab.pitch_hand)     AS pitch_hand,
            p.pitch_type_code,
            AVG(p.start_speed)     AS avg_velo,
            AVG(ABS(p.pfx_z))      AS avg_ivb,
            AVG(ABS(p.pfx_x))      AS avg_hbreak,
            AVG(p.spin_rate)       AS avg_spin,
            SUM(CASE WHEN p.call_code IN ('S','W','T')
                THEN COALESCE(bw.weight, 1.0) ELSE 0.0 END) /
            NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','X','D','E')
                THEN 1.0 ELSE 0.0 END), 0) AS weighted_whiff_rate,
            AVG(CASE WHEN p.call_code IN ('S','W','T')
                THEN 1.0 ELSE 0.0 END)     AS raw_whiff_rate,
            COUNT(*)               AS pitches
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        LEFT JOIN batter_weights bw ON bw.batter_id = ab.batter_id
        WHERE g.season = :season AND g.game_type = 'R'
        AND p.pitch_type_code IN :pitch_types
        AND p.start_speed IS NOT NULL
        AND p.pfx_z IS NOT NULL AND p.pfx_x IS NOT NULL
        AND p.spin_rate IS NOT NULL
        GROUP BY ab.pitcher_id, p.pitch_type_code
        HAVING COUNT(*) >= 20
    """)

    pitcher_rows = db.execute(pitcher_sql, {
        "season": score_season,
        "pitch_types": PITCH_TYPES,
        "lg_whiff": LEAGUE_AVG_WHIFF,
    }).mappings().all()
    print(f"  Scoring {len(pitcher_rows)} pitcher × pitch type combinations...")

    upsert = text("""
        INSERT INTO mlb.stuff_scores (
            pitcher_id, pitcher_name, season, pitch_type_code, pitch_hand,
            avg_velo, avg_ivb, avg_hbreak, avg_spin,
            physical_score, location_score, tunnel_score, stuff_score,
            weighted_whiff_rate, raw_whiff_rate, whiff_score, pitches
        ) VALUES (
            :pitcher_id, :pitcher_name, :season, :pitch_type_code, :pitch_hand,
            :avg_velo, :avg_ivb, :avg_hbreak, :avg_spin,
            :physical_score, :location_score, :tunnel_score, :stuff_score,
            :weighted_whiff_rate, :raw_whiff_rate, :whiff_score, :pitches
        )
        ON CONFLICT (pitcher_id, season, pitch_type_code) DO UPDATE SET
            physical_score      = EXCLUDED.physical_score,
            location_score      = EXCLUDED.location_score,
            tunnel_score        = EXCLUDED.tunnel_score,
            stuff_score         = EXCLUDED.stuff_score,
            weighted_whiff_rate = EXCLUDED.weighted_whiff_rate,
            raw_whiff_rate      = EXCLUDED.raw_whiff_rate,
            whiff_score         = EXCLUDED.whiff_score,
            pitches             = EXCLUDED.pitches,
            computed_at         = NOW()
    """)

    pitcher_totals = {}
    stored = 0

    for r in pitcher_rows:
        pid = r['pitcher_id']
        pt  = r['pitch_type_code']
        ph  = r['pitch_hand']
        n   = int(r['pitches'])

        pitcher_vals = {
            'avg_velo':   r['avg_velo'],
            'avg_ivb':    r['avg_ivb'],
            'avg_hbreak': r['avg_hbreak'],
            'avg_spin':   r['avg_spin'],
        }

        physical  = compute_physical_score(pitcher_vals, league_phys, pt)
        location, _ = compute_location_score(
            pid, pt, zone_values, league_zone_avgs, league_zone_stds,
            db, score_season
        )
        tunnel = compute_tunnel_score(pid, pt, db, score_season)

        wtd_whiff = float(r['weighted_whiff_rate'] or 0)
        raw_whiff = float(r['raw_whiff_rate'] or 0)

        lg_wtd_whiff = float(league_phys.get(pt, {}).get('avg_weighted_whiff') or 0)
        lg_wtd_whiff_std = float(league_phys.get(pt, {}).get('std_weighted_whiff') or 0)
        if lg_wtd_whiff > 0 and lg_wtd_whiff_std > 0:
            z_whiff = (wtd_whiff - lg_wtd_whiff) / lg_wtd_whiff_std
            whiff_score = round(100.0 + z_whiff * SCALE, 1)
            whiff_score = max(50.0, min(160.0, whiff_score))
        else:
            whiff_score = 100.0

        stuff_raw = (
            whiff_score  * COMPONENT_WEIGHTS['weighted_whiff'] +
            physical     * COMPONENT_WEIGHTS['physical'] +
            location     * COMPONENT_WEIGHTS['location'] +
            tunnel       * COMPONENT_WEIGHTS['tunnel']
        )
        stuff = regress_to_mean(stuff_raw, n)

        def fv(v):
            return float(v) if v is not None else None

        db.execute(upsert, {
            "pitcher_id":         pid,
            "pitcher_name":       r['pitcher_name'],
            "season":             score_season,
            "pitch_type_code":    pt,
            "pitch_hand":         ph,
            "avg_velo":           round(fv(r['avg_velo']),   1) if fv(r['avg_velo'])   else None,
            "avg_ivb":            round(fv(r['avg_ivb'])   * 12, 2) if fv(r['avg_ivb'])   else None,
            "avg_hbreak":         round(fv(r['avg_hbreak']) * 12, 2) if fv(r['avg_hbreak']) else None,
            "avg_spin":           round(fv(r['avg_spin']),   0) if fv(r['avg_spin'])   else None,
            "physical_score":     physical,
            "location_score":     location,
            "tunnel_score":       tunnel,
            "stuff_score":        stuff,
            "weighted_whiff_rate": fv(r['weighted_whiff_rate']),
            "raw_whiff_rate":      fv(r['raw_whiff_rate']),
            "whiff_score":         whiff_score,
            "pitches":            n,
        })
        stored += 1

        pitcher_totals.setdefault(pid, {
            'name': r['pitcher_name'], 'pitch_hand': ph, 'scores': []
        })
        pitcher_totals[pid]['scores'].append({
            'stuff': stuff, 'stuff_raw': stuff_raw, 'pitches': n, 'pitch_type': pt,
        })

    db.commit()
    print(f"  Stored {stored} Stuff Score rows")

    overall_upsert = text("""
        INSERT INTO mlb.stuff_overall (
            pitcher_id, pitcher_name, season, pitch_hand,
            stuff_score, top_pitch_type, top_pitch_stuff, total_pitches
        ) VALUES (
            :pitcher_id, :pitcher_name, :season, :pitch_hand,
            :stuff_score, :top_pitch_type, :top_pitch_stuff, :total_pitches
        )
        ON CONFLICT (pitcher_id, season) DO UPDATE SET
            stuff_score     = EXCLUDED.stuff_score,
            top_pitch_type  = EXCLUDED.top_pitch_type,
            top_pitch_stuff = EXCLUDED.top_pitch_stuff,
            total_pitches   = EXCLUDED.total_pitches,
            computed_at     = NOW()
    """)

    for pid, data in pitcher_totals.items():
        scores = data['scores']
        total_p = sum(s['pitches'] for s in scores)
        if total_p == 0:
            continue
        overall = sum(s['stuff'] * s['pitches'] for s in scores) / total_p
        top = max(scores, key=lambda x: x['stuff'])

        db.execute(overall_upsert, {
            "pitcher_id":     int(pid),
            "pitcher_name":   data['name'],
            "season":         score_season,
            "pitch_hand":     data['pitch_hand'],
            "stuff_score":    round(overall, 1),
            "top_pitch_type": top['pitch_type'],
            "top_pitch_stuff": round(top['stuff'], 1),
            "total_pitches":  int(total_p),
        })

    db.commit()
    print(f"  Stored {len(pitcher_totals)} overall Stuff Scores")


# ── VALIDATION ────────────────────────────────────────────────────────────────

def validate_stuff_scores(season: int, db):
    print(f"\n  Validating Stuff Score ({season})...")

    sql = text("""
        SELECT
            so.pitcher_id,
            so.pitcher_name,
            so.stuff_score,
            so.top_pitch_type,
            so.top_pitch_stuff,
            so.total_pitches,
            ROUND(go2.goose2_plus::numeric, 1) AS goose2_plus,
            ROUND(go1.goose_plus::numeric, 1)  AS goose1_plus,
            ROUND(SUM(bp.strikeouts)::numeric /
                NULLIF(SUM(bp.batters_faced), 0) * 100, 2) AS k_rate,
            ROUND(SUM(bp.home_runs)::numeric /
                NULLIF(SUM(bp.batters_faced), 0) * 100, 2) AS hr_rate,
            br.era_plus
        FROM mlb.stuff_overall so
        LEFT JOIN mlb.goose2_overall go2 ON go2.pitcher_id = so.pitcher_id
            AND go2.season = so.season
        LEFT JOIN mlb.goose_overall go1 ON go1.pitcher_id = so.pitcher_id
            AND go1.season = so.season AND go1.game_pk IS NULL
        LEFT JOIN mlb.boxscore_pitching bp ON bp.player_id = so.pitcher_id
            AND bp.game_pk IN (
                SELECT game_pk FROM mlb.games
                WHERE season = :season AND game_type = 'R'
            )
        LEFT JOIN mlb.bbref_pitching br ON br.mlb_id = so.pitcher_id
            AND br.year_id = :season
        WHERE so.season = :season AND so.total_pitches >= 200
        GROUP BY so.pitcher_id, so.pitcher_name, so.stuff_score,
                 so.top_pitch_type, so.top_pitch_stuff, so.total_pitches,
                 go2.goose2_plus, go1.goose_plus, br.era_plus
        HAVING SUM(bp.batters_faced) >= 50
        ORDER BY so.stuff_score DESC
    """)
    rows = db.execute(sql, {"season": season}).mappings().all()
    df = pd.DataFrame([dict(r) for r in rows])

    if df.empty or len(df) < 10:
        print(f"  Insufficient data (n={len(df)})")
        return

    metrics = {
        'K Rate':   ('k_rate',    '+'),
        'HR Rate':  ('hr_rate',   '-'),
        'ERA+':     ('era_plus',  '+'),
        'Goose+ 2': ('goose2_plus', '+'),
        'Goose+ 1': ('goose1_plus', '+'),
    }

    print(f"\n  Stuff Score Correlations (n={len(df)}):")
    print(f"  {'Metric':<12} {'Pearson r':>10}  Expected")
    print(f"  {'-'*35}")
    for label, (col, expected) in metrics.items():
        valid = df[['stuff_score', col]].dropna()
        if len(valid) < 10:
            print(f"  {label:<12} {'n/a':>10}")
            continue
        corr = valid['stuff_score'].corr(valid[col])
        ok = '✅' if ('+' if corr > 0 else '-') == expected else '❌'
        print(f"  {label:<12} {corr:>+10.3f}  {ok} (expected {expected})")

    print(f"\n  Top 15 Stuff Scores ({season}):")
    print(f"  {'Pitcher':<26} {'SS':>6} {'Top Pitch':>9} {'SS':>6} "
          f"{'G2+':>6} {'K%':>6} {'ERA+':>6}")
    print(f"  {'-'*70}")
    for _, r in df.nlargest(15, 'stuff_score').iterrows():
        print(f"  {(r['pitcher_name'] or '?'):<26} "
              f"{r['stuff_score']:>6.1f} "
              f"{r['top_pitch_type']:>9} "
              f"{r['top_pitch_stuff']:>6.1f} "
              f"{str(r['goose2_plus'] or '—'):>6} "
              f"{str(r['k_rate'] or '—'):>6} "
              f"{str(r['era_plus'] or '—'):>6}")

    # Whiff weight impact
    weight_sql = text("""
        SELECT pitcher_name, pitch_type_code,
               ROUND(weighted_whiff_rate::numeric * 100, 1) AS weighted_whiff,
               ROUND(raw_whiff_rate::numeric * 100, 1)      AS raw_whiff,
               ROUND((weighted_whiff_rate - raw_whiff_rate)::numeric * 100, 1) AS delta,
               pitches
        FROM mlb.stuff_scores
        WHERE season = :season AND pitches >= 100
        AND weighted_whiff_rate IS NOT NULL
        ORDER BY weighted_whiff_rate DESC
        LIMIT 10
    """)
    wrows = db.execute(weight_sql, {"season": season}).mappings().all()
    if wrows:
        print(f"\n  Whiff weight impact — top 10 by quality-adjusted whiff:")
        print(f"  {'Pitcher':<26} {'PT':>4} {'Wtd%':>7} {'Raw%':>7} {'Delta':>7}")
        print(f"  {'-'*55}")
        for r in wrows:
            print(f"  {(r['pitcher_name'] or '?'):<26} {r['pitch_type_code']:>4} "
                  f"{r['weighted_whiff']:>6.1f}% "
                  f"{r['raw_whiff']:>6.1f}% "
                  f"{r['delta']:>+6.1f}%")


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-season", type=int, default=2025)
    parser.add_argument("--score-season",    type=int, default=2026)
    parser.add_argument("--skip-zones",      action="store_true")
    parser.add_argument("--validate-only",   action="store_true")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        if args.validate_only:
            validate_stuff_scores(args.score_season, db)
        else:
            print(f"=== Building Stuff Score ===")
            print(f"  Baseline: {args.baseline_season} → Score: {args.score_season}")

            if not args.skip_zones:
                print(f"\n  Step 1: Zone values ({args.baseline_season})...")
                zone_df = compute_zone_values(args.baseline_season, db)
                if not zone_df.empty:
                    store_zone_values(zone_df, args.baseline_season, db)

                # If score season differs, also compute zone values for it
                # (used as baseline when baseline_season == score_season)
                if args.score_season != args.baseline_season:
                    print(f"\n  Step 1b: Zone values ({args.score_season})...")
                    zone_df_curr = compute_zone_values(args.score_season, db)
                    if not zone_df_curr.empty:
                        store_zone_values(zone_df_curr, args.score_season, db)

            # Use score_season zone values if available, else fall back to baseline
            baseline_for_zones = args.score_season

            print(f"\n  Step 2: Stuff Scores (zones from {baseline_for_zones})...")
            build_stuff_scores(args.score_season, baseline_for_zones, db)

            print(f"\n  Step 3: Validation...")
            validate_stuff_scores(args.score_season, db)
    finally:
        db.close()
