"""
Goose+ 2 and Juiced+ 2 experimental models.

Goose+ 2 methodology:
- Compute league average outcomes per pitch type x bat_side x pitch_hand
- Score each pitcher's outcomes vs those league averages
- Weighted composite: Whiff% 30%, SLG 30%, CSW% 15%, Contact% 10%, Chase% 10%, HH% 5%
- 100 = league average, higher = better pitcher

Juiced+ 2 methodology:
- Score batters on: SLG 20%, OPS 30%, Quality-adj 30%, Hard Hit% 20%
- Quality adjustment: actual SLG / expected SLG given avg Goose+ 2 of pitchers faced
- Uses prior year baselines to score current season (breaks circularity)

Key schema notes:
- pitches.launch_speed = exit velocity (use >= 95 for hard hit)
- pitches.start_speed  = pitch velocity
- at_bats.event_type   = outcome (field_out, strikeout, single, etc.)
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import argparse
import pandas as pd
import numpy as np
from database import SessionLocal
from sqlalchemy import text

# ── WEIGHTS ──────────────────────────────────────────────────────────────────
GOOSE2_WEIGHTS = {
    'whiff':    0.30,
    'slg':      0.30,
    'csw':      0.15,
    'contact':  0.10,
    'chase':    0.10,
    'hard_hit': 0.05,
}

JUICED2_WEIGHTS = {
    'quality_adj': 0.40,  # r=0.480 OPS — dominant signal
    'slg':         0.28,  # r=0.327 SLG — strong
    'ops':         0.22,  # r=0.288 OPS — moderate
    'hard_hit':    0.10,  # r=0.161 HR  — weakest
}

MIN_PITCHES_BASELINE = 200
MIN_PITCHES_PITCHER  = 20
MIN_PA_BATTER        = 50

REGRESSION_K = 200  # at 200 pitches: 50% actual / 50% mean

def regress_to_mean(score, n_pitches, k=REGRESSION_K):
    weight = n_pitches / (n_pitches + k)
    return round(100 + weight * (score - 100), 1)


# ── ROLLING BASELINE WEIGHTS ──────────────────────────────────────────────────

def get_season_weights(db, season: int) -> tuple:
    """
    Compute how much to weight current season vs prior season baselines.
    Based on pitch volume in current season.

    < 50,000 pitches:  30% current / 70% prior
    50k-150k pitches:  linear blend
    > 150,000 pitches: 100% current / 0% prior
    """
    count_sql = text("""
        SELECT COUNT(*) as n FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE g.season = :season AND g.game_type = 'R'
        AND p.pitch_type_code IS NOT NULL
    """)
    n = db.execute(count_sql, {"season": season}).scalar() or 0

    if n < 50000:
        w_current = 0.30
    elif n >= 150000:
        w_current = 1.00
    else:
        w_current = 0.30 + 0.70 * ((n - 50000) / 100000)

    w_prior = 1.0 - w_current
    print(f"  Season {season}: {n:,} pitches → "
          f"{w_current:.0%} current / {w_prior:.0%} prior baseline weight")
    return round(w_current, 3), round(w_prior, 3)


def get_blended_baselines(score_season: int, db) -> dict:
    """
    Compute blended league baselines from current + prior season.
    Returns dict keyed by (pitch_type, bat_side, pitch_hand).
    """
    w_current, w_prior = get_season_weights(db, score_season)
    prior_season = score_season - 1

    print(f"  Computing {score_season} baselines ({w_current:.0%} weight)...")
    current_df = compute_league_baselines(score_season, db)
    current_slg = compute_slg_baseline(score_season, db)

    if not current_df.empty and not current_slg.empty:
        current_df = current_df.merge(
            current_slg[['pitch_type_code', 'bat_side', 'pitch_hand', 'slg_against']],
            on=['pitch_type_code', 'bat_side', 'pitch_hand'], how='left'
        )

    prior_sql = text("""
        SELECT pitch_type_code, bat_side, pitch_hand,
               contact_pct, whiff_pct, csw_pct, slg_against,
               hard_hit_pct, chase_pct
        FROM mlb.league_pitch_baselines
        WHERE baseline_season = :season
    """)
    prior_rows = db.execute(prior_sql, {"season": prior_season}).mappings().all()
    prior_df = pd.DataFrame([dict(r) for r in prior_rows])

    if prior_df.empty:
        print(f"  No prior season baselines found — using current only")
        w_current, w_prior = 1.0, 0.0

    metrics = ['contact_pct', 'whiff_pct', 'csw_pct',
               'slg_against', 'hard_hit_pct', 'chase_pct']

    blended = {}
    for _, row in current_df.iterrows():
        key = (row['pitch_type_code'], row['bat_side'], row['pitch_hand'])
        blended[key] = {}
        for m in metrics:
            val = row.get(m)
            blended[key][m] = float(val) if pd.notna(val) else None

    if w_prior > 0 and not prior_df.empty:
        for _, row in prior_df.iterrows():
            key = (row['pitch_type_code'], row['bat_side'], row['pitch_hand'])
            if key in blended:
                for m in metrics:
                    curr_val = blended[key].get(m)
                    prior_val = row.get(m)
                    if curr_val is not None and prior_val is not None:
                        blended[key][m] = (
                            float(curr_val) * w_current +
                            float(prior_val) * w_prior
                        )
                    elif prior_val is not None:
                        blended[key][m] = float(prior_val)
            else:
                blended[key] = {}
                for m in metrics:
                    v = row.get(m)
                    blended[key][m] = float(v) if pd.notna(v) else None

    print(f"  Blended baselines: {len(blended)} combinations")
    return blended


# ── PART A: LEAGUE BASELINES ──────────────────────────────────────────────────

def compute_league_baselines(baseline_season: int, db) -> pd.DataFrame:
    print(f"\n  Computing league baselines from {baseline_season} data...")
    sql = text("""
        SELECT
            p.pitch_type_code,
            ab.bat_side,
            ab.pitch_hand,
            ROUND(AVG(CASE
                WHEN p.call_code IN ('S','W','T','F','X','D','E')
                THEN CASE WHEN p.call_code IN ('F','X','D','E') THEN 1.0 ELSE 0.0 END
            END)::numeric, 4) AS contact_pct,
            ROUND(AVG(CASE
                WHEN p.call_code IN ('S','W','T','F','X','D','E')
                THEN CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END
            END)::numeric, 4) AS whiff_pct,
            ROUND(AVG(CASE WHEN p.call_code IN ('C','S','W','T')
                THEN 1.0 ELSE 0.0 END)::numeric, 4) AS csw_pct,
            ROUND(AVG(CASE
                WHEN p.zone NOT BETWEEN 1 AND 9
                THEN CASE WHEN p.call_code IN ('S','W','T','F','X','D','E')
                    THEN 1.0 ELSE 0.0 END
            END)::numeric, 4) AS chase_pct,
            ROUND(AVG(CASE
                WHEN p.launch_speed IS NOT NULL AND p.call_code = 'X'
                THEN CASE WHEN p.launch_speed >= 95 THEN 1.0 ELSE 0.0 END
            END)::numeric, 4) AS hard_hit_pct,
            COUNT(*) AS total_pitches,
            COUNT(DISTINCT ab.id) AS total_pa
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE g.season = :season
          AND g.game_type = 'R'
          AND p.pitch_type_code IS NOT NULL
          AND ab.bat_side IN ('L','R')
          AND ab.pitch_hand IN ('L','R')
        GROUP BY p.pitch_type_code, ab.bat_side, ab.pitch_hand
        HAVING COUNT(*) >= :min_pitches
    """)
    rows = db.execute(sql, {"season": baseline_season, "min_pitches": MIN_PITCHES_BASELINE}).mappings().all()
    df = pd.DataFrame([dict(r) for r in rows])
    print(f"  Found {len(df)} pitch_type x platoon combinations")
    return df


def compute_slg_baseline(baseline_season: int, db) -> pd.DataFrame:
    print(f"  Computing SLG baselines from {baseline_season}...")
    sql = text("""
        SELECT
            p.pitch_type_code,
            ab.bat_side,
            ab.pitch_hand,
            ROUND(AVG(CASE
                WHEN ab.event_type IN ('single','double','triple','home_run',
                    'field_out','force_out','grounded_into_double_play',
                    'double_play','fielders_choice','fielders_choice_out',
                    'sac_fly','sac_bunt','field_error','strikeout_double_play')
                THEN CASE ab.event_type
                    WHEN 'single'    THEN 1.0
                    WHEN 'double'    THEN 2.0
                    WHEN 'triple'    THEN 3.0
                    WHEN 'home_run'  THEN 4.0
                    ELSE 0.0
                END
            END)::numeric, 4) AS slg_against,
            COUNT(DISTINCT ab.id) AS pa_count
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE g.season = :season
          AND g.game_type = 'R'
          AND p.pitch_type_code IS NOT NULL
          AND ab.bat_side IN ('L','R')
          AND ab.pitch_hand IN ('L','R')
          AND p.pitch_number = (
              SELECT MAX(p2.pitch_number) FROM mlb.pitches p2
              WHERE p2.game_pk = p.game_pk
                AND p2.at_bat_index = p.at_bat_index
          )
        GROUP BY p.pitch_type_code, ab.bat_side, ab.pitch_hand
        HAVING COUNT(DISTINCT ab.id) >= 30
    """)
    rows = db.execute(sql, {"season": baseline_season}).mappings().all()
    df = pd.DataFrame([dict(r) for r in rows])
    print(f"  Found {len(df)} SLG baseline rows")
    return df


def store_baselines(df: pd.DataFrame, slg_df: pd.DataFrame, baseline_season: int, db):
    merged = df.merge(
        slg_df[['pitch_type_code', 'bat_side', 'pitch_hand', 'slg_against']],
        on=['pitch_type_code', 'bat_side', 'pitch_hand'],
        how='left'
    )
    upsert = text("""
        INSERT INTO mlb.league_pitch_baselines (
            baseline_season, pitch_type_code, bat_side, pitch_hand,
            contact_pct, whiff_pct, csw_pct, slg_against,
            hard_hit_pct, chase_pct, total_pitches, total_pa
        ) VALUES (
            :baseline_season, :pitch_type_code, :bat_side, :pitch_hand,
            :contact_pct, :whiff_pct, :csw_pct, :slg_against,
            :hard_hit_pct, :chase_pct, :total_pitches, :total_pa
        )
        ON CONFLICT (baseline_season, pitch_type_code, bat_side, pitch_hand) DO UPDATE SET
            contact_pct   = EXCLUDED.contact_pct,
            whiff_pct     = EXCLUDED.whiff_pct,
            csw_pct       = EXCLUDED.csw_pct,
            slg_against   = EXCLUDED.slg_against,
            hard_hit_pct  = EXCLUDED.hard_hit_pct,
            chase_pct     = EXCLUDED.chase_pct,
            total_pitches = EXCLUDED.total_pitches,
            computed_at   = NOW()
    """)
    stored = 0
    for _, r in merged.iterrows():
        def fv(col):
            v = r.get(col)
            return float(v) if pd.notna(v) else None
        db.execute(upsert, {
            "baseline_season": int(baseline_season),
            "pitch_type_code": r['pitch_type_code'],
            "bat_side":        r['bat_side'],
            "pitch_hand":      r['pitch_hand'],
            "contact_pct":     fv('contact_pct'),
            "whiff_pct":       fv('whiff_pct'),
            "csw_pct":         fv('csw_pct'),
            "slg_against":     fv('slg_against'),
            "hard_hit_pct":    fv('hard_hit_pct'),
            "chase_pct":       fv('chase_pct'),
            "total_pitches":   int(r['total_pitches']) if pd.notna(r.get('total_pitches')) else None,
            "total_pa":        int(r['total_pa']) if pd.notna(r.get('total_pa')) else None,
        })
        stored += 1
    db.commit()
    print(f"  Stored {stored} baseline rows")


# ── PART B: GOOSE+ 2 ─────────────────────────────────────────────────────────

def build_goose2(score_season: int, baseline_season: int, db):
    print(f"\n  Building Goose+ 2 for {score_season}...")
    baselines = get_blended_baselines(score_season, db)
    print(f"  Using blended {score_season}/{baseline_season} baselines: {len(baselines)} combinations")

    pitcher_sql = text("""
        SELECT
            ab.pitcher_id,
            MAX(ab.pitcher_name) AS pitcher_name,
            MAX(ab.pitch_hand)   AS pitch_hand,
            p.pitch_type_code,
            ab.bat_side,
            AVG(CASE
                WHEN p.call_code IN ('S','W','T','F','X','D','E')
                THEN CASE WHEN p.call_code IN ('F','X','D','E') THEN 1.0 ELSE 0.0 END
            END) AS contact_pct,
            AVG(CASE
                WHEN p.call_code IN ('S','W','T','F','X','D','E')
                THEN CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END
            END) AS whiff_pct,
            AVG(CASE WHEN p.call_code IN ('C','S','W','T')
                THEN 1.0 ELSE 0.0 END) AS csw_pct,
            AVG(CASE
                WHEN p.zone NOT BETWEEN 1 AND 9
                THEN CASE WHEN p.call_code IN ('S','W','T','F','X','D','E')
                    THEN 1.0 ELSE 0.0 END
            END) AS chase_pct,
            AVG(CASE
                WHEN p.launch_speed IS NOT NULL AND p.call_code = 'X'
                THEN CASE WHEN p.launch_speed >= 95 THEN 1.0 ELSE 0.0 END
            END) AS hard_hit_pct,
            COUNT(*) AS pitches
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE g.season = :season
          AND g.game_type = 'R'
          AND p.pitch_type_code IS NOT NULL
          AND ab.bat_side IN ('L','R')
          AND ab.pitch_hand IN ('L','R')
        GROUP BY ab.pitcher_id, p.pitch_type_code, ab.bat_side
        HAVING COUNT(*) >= :min_pitches
    """)

    slg_sql = text("""
        SELECT
            ab.pitcher_id,
            p.pitch_type_code,
            ab.bat_side,
            AVG(CASE
                WHEN ab.event_type IN ('single','double','triple','home_run',
                    'field_out','force_out','grounded_into_double_play',
                    'double_play','fielders_choice','fielders_choice_out',
                    'sac_fly','sac_bunt','field_error','strikeout_double_play')
                THEN CASE ab.event_type
                    WHEN 'single'   THEN 1.0
                    WHEN 'double'   THEN 2.0
                    WHEN 'triple'   THEN 3.0
                    WHEN 'home_run' THEN 4.0
                    ELSE 0.0
                END
            END) AS slg_against
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE g.season = :season
          AND g.game_type = 'R'
          AND p.pitch_type_code IS NOT NULL
          AND ab.bat_side IN ('L','R')
          AND p.pitch_number = (
              SELECT MAX(p2.pitch_number) FROM mlb.pitches p2
              WHERE p2.game_pk = p.game_pk
                AND p2.at_bat_index = p.at_bat_index
          )
        GROUP BY ab.pitcher_id, p.pitch_type_code, ab.bat_side
        HAVING COUNT(DISTINCT ab.id) >= 10
    """)

    pitcher_rows = db.execute(pitcher_sql, {
        "season": score_season, "min_pitches": MIN_PITCHES_PITCHER
    }).mappings().all()

    slg_rows = db.execute(slg_sql, {"season": score_season}).mappings().all()
    slg_map = {(r['pitcher_id'], r['pitch_type_code'], r['bat_side']): r['slg_against'] for r in slg_rows}

    print(f"  Scoring {len(pitcher_rows)} pitcher x pitch x side combinations...")

    def safe_idx(pitcher_val, league_val, invert=False):
        if pitcher_val is None or league_val is None or float(league_val) == 0:
            return 100.0
        ratio = float(pitcher_val) / float(league_val)
        if invert:
            return 100.0 if ratio == 0 else round(100 / ratio, 1)
        return round(ratio * 100, 1)

    upsert = text("""
        INSERT INTO mlb.goose2_scores (
            pitcher_id, pitcher_name, season, pitch_type_code, bat_side, pitch_hand,
            contact_pct, whiff_pct, csw_pct, slg_against, hard_hit_pct, chase_pct,
            contact_idx, whiff_idx, csw_idx, slg_idx, hard_hit_idx, chase_idx,
            goose2_plus, goose2_raw, pitches
        ) VALUES (
            :pitcher_id, :pitcher_name, :season, :pitch_type_code, :bat_side, :pitch_hand,
            :contact_pct, :whiff_pct, :csw_pct, :slg_against, :hard_hit_pct, :chase_pct,
            :contact_idx, :whiff_idx, :csw_idx, :slg_idx, :hard_hit_idx, :chase_idx,
            :goose2_plus, :goose2_raw, :pitches
        )
        ON CONFLICT (pitcher_id, season, pitch_type_code, bat_side) DO UPDATE SET
            contact_idx = EXCLUDED.contact_idx,
            whiff_idx   = EXCLUDED.whiff_idx,
            csw_idx     = EXCLUDED.csw_idx,
            slg_idx     = EXCLUDED.slg_idx,
            hard_hit_idx = EXCLUDED.hard_hit_idx,
            chase_idx   = EXCLUDED.chase_idx,
            goose2_plus = EXCLUDED.goose2_plus,
            goose2_raw  = EXCLUDED.goose2_raw,
            pitches     = EXCLUDED.pitches,
            computed_at = NOW()
    """)

    stored_scores = 0
    pitcher_totals = {}

    for r in pitcher_rows:
        pid = r['pitcher_id']
        pt  = r['pitch_type_code']
        bs  = r['bat_side']
        ph  = r['pitch_hand']

        bl = baselines.get((pt, bs, ph))
        if not bl:
            bl = baselines.get((pt, bs, 'R')) or baselines.get((pt, bs, 'L'))
        if not bl:
            continue

        slg = slg_map.get((pid, pt, bs))

        contact_idx  = safe_idx(r['contact_pct'],  bl['contact_pct'],  invert=True)
        whiff_idx    = safe_idx(r['whiff_pct'],    bl['whiff_pct'],    invert=False)
        csw_idx      = safe_idx(r['csw_pct'],      bl['csw_pct'],      invert=False)
        slg_idx      = safe_idx(slg,               bl['slg_against'],  invert=True)
        hard_hit_idx = safe_idx(r['hard_hit_pct'], bl['hard_hit_pct'], invert=True)
        chase_idx    = safe_idx(r['chase_pct'],    bl['chase_pct'],    invert=False)

        goose2_raw = (
            whiff_idx    * GOOSE2_WEIGHTS['whiff']    +
            slg_idx      * GOOSE2_WEIGHTS['slg']      +
            csw_idx      * GOOSE2_WEIGHTS['csw']      +
            contact_idx  * GOOSE2_WEIGHTS['contact']  +
            chase_idx    * GOOSE2_WEIGHTS['chase']    +
            hard_hit_idx * GOOSE2_WEIGHTS['hard_hit']
        )
        goose2 = regress_to_mean(goose2_raw, int(r['pitches']))

        def fv(v):
            return float(v) if v is not None else None

        db.execute(upsert, {
            "pitcher_id":    pid,
            "pitcher_name":  r['pitcher_name'],
            "season":        score_season,
            "pitch_type_code": pt,
            "bat_side":      bs,
            "pitch_hand":    ph,
            "contact_pct":   fv(r['contact_pct']),
            "whiff_pct":     fv(r['whiff_pct']),
            "csw_pct":       fv(r['csw_pct']),
            "slg_against":   fv(slg),
            "hard_hit_pct":  fv(r['hard_hit_pct']),
            "chase_pct":     fv(r['chase_pct']),
            "contact_idx":   round(contact_idx, 1),
            "whiff_idx":     round(whiff_idx, 1),
            "csw_idx":       round(csw_idx, 1),
            "slg_idx":       round(slg_idx, 1),
            "hard_hit_idx":  round(hard_hit_idx, 1),
            "chase_idx":     round(chase_idx, 1),
            "goose2_plus":   round(goose2, 1),
            "goose2_raw":    round(goose2_raw, 1),
            "pitches":       int(r['pitches']),
        })
        stored_scores += 1

        pitcher_totals.setdefault(pid, {'name': r['pitcher_name'], 'pitch_hand': ph, 'scores': []})
        pitcher_totals[pid]['scores'].append({
            'goose2_raw': goose2_raw, 'goose2': goose2,
            'pitches': int(r['pitches']), 'bat_side': bs
        })

    db.commit()
    print(f"  Stored {stored_scores} Goose+ 2 pitch scores")

    # Overall (usage-weighted)
    overall_upsert = text("""
        INSERT INTO mlb.goose2_overall (
            pitcher_id, pitcher_name, season, pitch_hand,
            goose2_plus, goose2_raw, goose2_vs_lhh, goose2_vs_rhh, total_pitches
        ) VALUES (
            :pitcher_id, :pitcher_name, :season, :pitch_hand,
            :goose2_plus, :goose2_raw, :goose2_vs_lhh, :goose2_vs_rhh, :total_pitches
        )
        ON CONFLICT (pitcher_id, season) DO UPDATE SET
            goose2_plus    = EXCLUDED.goose2_plus,
            goose2_raw     = EXCLUDED.goose2_raw,
            goose2_vs_lhh  = EXCLUDED.goose2_vs_lhh,
            goose2_vs_rhh  = EXCLUDED.goose2_vs_rhh,
            total_pitches  = EXCLUDED.total_pitches,
            computed_at    = NOW()
    """)

    for pid, data in pitcher_totals.items():
        scores = data['scores']
        total_p = sum(s['pitches'] for s in scores)
        if total_p == 0:
            continue
        # Raw overall (usage-weighted, unregressed)
        overall_raw = sum(s['goose2_raw'] * s['pitches'] for s in scores) / total_p
        # Regress overall by total pitch count
        overall = regress_to_mean(overall_raw, total_p)

        lhh = [s for s in scores if s['bat_side'] == 'L']
        rhh = [s for s in scores if s['bat_side'] == 'R']
        lhh_p = sum(s['pitches'] for s in lhh)
        rhh_p = sum(s['pitches'] for s in rhh)
        vs_lhh_raw = sum(s['goose2_raw'] * s['pitches'] for s in lhh) / lhh_p if lhh_p else None
        vs_rhh_raw = sum(s['goose2_raw'] * s['pitches'] for s in rhh) / rhh_p if rhh_p else None
        vs_lhh = regress_to_mean(vs_lhh_raw, lhh_p) if vs_lhh_raw is not None else None
        vs_rhh = regress_to_mean(vs_rhh_raw, rhh_p) if vs_rhh_raw is not None else None

        db.execute(overall_upsert, {
            "pitcher_id":   int(pid),
            "pitcher_name": data['name'],
            "season":       score_season,
            "pitch_hand":   data['pitch_hand'],
            "goose2_plus":  round(overall, 1),
            "goose2_raw":   round(overall_raw, 1),
            "goose2_vs_lhh": round(vs_lhh, 1) if vs_lhh else None,
            "goose2_vs_rhh": round(vs_rhh, 1) if vs_rhh else None,
            "total_pitches": int(total_p),
        })

    db.commit()
    print(f"  Stored {len(pitcher_totals)} Goose+ 2 overall scores")


# ── PART C: JUICED+ 2 ────────────────────────────────────────────────────────

def build_juiced2(score_season: int, db):
    print(f"\n  Building Juiced+ 2 for {score_season}...")

    lg_sql = text("""
        SELECT
            AVG(CASE ab.event_type
                WHEN 'single'   THEN 1.0
                WHEN 'double'   THEN 2.0
                WHEN 'triple'   THEN 3.0
                WHEN 'home_run' THEN 4.0
                ELSE 0.0
            END) AS lg_slg,
            AVG(CASE WHEN ab.event_type IN ('single','double','triple','home_run','walk',
                    'intent_walk','hit_by_pitch')
                THEN 1.0 ELSE 0.0 END) AS lg_obp,
            COUNT(*) AS total_pa
        FROM mlb.at_bats ab
        JOIN mlb.games g ON g.game_pk = ab.game_pk
        WHERE g.season = :season AND g.game_type = 'R'
          AND ab.event_type NOT IN ('game_advisory','runner_double_play','balk',
                                    'wild_pitch','passed_ball','stolen_base_2b',
                                    'stolen_base_3b','stolen_base_home',
                                    'caught_stealing_2b','caught_stealing_3b',
                                    'caught_stealing_home','pickoff_1b','pickoff_2b',
                                    'pickoff_3b')
    """)
    lg = db.execute(lg_sql, {"season": score_season}).mappings().first()
    lg_slg = float(lg['lg_slg'] or 0.40)
    lg_obp = float(lg['lg_obp'] or 0.32)
    lg_ops = lg_slg + lg_obp
    print(f"  League avg SLG: {lg_slg:.3f}  OBP: {lg_obp:.3f}  OPS: {lg_ops:.3f}")

    batter_sql = text("""
        WITH batter_pa AS (
            SELECT
                ab.batter_id,
                MAX(ab.batter_name) AS batter_name,
                MAX(ab.bat_side)    AS bat_side,
                AVG(CASE ab.event_type
                    WHEN 'single'   THEN 1.0
                    WHEN 'double'   THEN 2.0
                    WHEN 'triple'   THEN 3.0
                    WHEN 'home_run' THEN 4.0
                    ELSE 0.0
                END) AS slg,
                AVG(CASE WHEN ab.event_type IN ('single','double','triple','home_run',
                        'walk','intent_walk','hit_by_pitch')
                    THEN 1.0 ELSE 0.0 END) AS obp,
                AVG(CASE WHEN ab.event_type = 'home_run' THEN 1.0 ELSE 0.0 END) AS hr_rate,
                COUNT(*) AS total_pa
            FROM mlb.at_bats ab
            JOIN mlb.games g ON g.game_pk = ab.game_pk
            WHERE g.season = :season AND g.game_type = 'R'
              AND ab.event_type NOT IN ('game_advisory','runner_double_play','balk',
                  'wild_pitch','passed_ball','stolen_base_2b','stolen_base_3b',
                  'stolen_base_home','caught_stealing_2b','caught_stealing_3b',
                  'caught_stealing_home','pickoff_1b','pickoff_2b','pickoff_3b')
            GROUP BY ab.batter_id
            HAVING COUNT(*) >= :min_pa
        ),
        goose2_faced AS (
            SELECT
                ab.batter_id,
                AVG(go.goose2_plus) AS avg_goose2_faced
            FROM mlb.at_bats ab
            JOIN mlb.games g ON g.game_pk = ab.game_pk
            JOIN mlb.goose2_overall go ON go.pitcher_id = ab.pitcher_id
                AND go.season = :season
            WHERE g.season = :season AND g.game_type = 'R'
            GROUP BY ab.batter_id
        )
        SELECT
            bp.*,
            COALESCE(gf.avg_goose2_faced, 100.0) AS avg_goose2_faced
        FROM batter_pa bp
        LEFT JOIN goose2_faced gf ON gf.batter_id = bp.batter_id
    """)

    rows = db.execute(batter_sql, {
        "season": score_season, "min_pa": MIN_PA_BATTER
    }).mappings().all()
    print(f"  Scoring {len(rows)} batters...")

    # Load bat tracking for hard hit metric
    bt_sql = text("""
        SELECT mlbam_id, bat_speed, squared_up_pct, blast_rate
        FROM mlb.bat_tracking
        WHERE season = :season AND player_type = 'batter' AND bat_speed IS NOT NULL
    """)
    bt_rows = db.execute(bt_sql, {"season": score_season}).mappings().all()
    bat_tracking = {int(r['mlbam_id']): dict(r) for r in bt_rows}
    if not bat_tracking:
        bt_rows = db.execute(bt_sql, {"season": score_season - 1}).mappings().all()
        bat_tracking = {int(r['mlbam_id']): dict(r) for r in bt_rows}
    print(f"  Loaded bat tracking for {len(bat_tracking)} batters")

    bt_vals = list(bat_tracking.values())
    lg_bat_speed  = float(pd.Series([b['bat_speed']    for b in bt_vals if b['bat_speed']]).mean())    if bt_vals else 72.0
    lg_squared_up = float(pd.Series([b['squared_up_pct'] for b in bt_vals if b['squared_up_pct']]).mean()) if bt_vals else 0.45
    lg_blast_rate = float(pd.Series([b['blast_rate']   for b in bt_vals if b['blast_rate']]).mean())   if bt_vals else 0.25

    slg_vals = [float(r['slg']) for r in rows if r['slg'] is not None]
    ops_vals = [float(r['obp'] or 0) + float(r['slg'] or 0) for r in rows]
    hh_vals  = [float(r['hr_rate']) for r in rows if r['hr_rate'] is not None]

    lg_avg_slg = np.mean(slg_vals) if slg_vals else lg_slg
    lg_avg_ops = np.mean(ops_vals) if ops_vals else lg_ops
    lg_avg_hh  = np.mean(hh_vals)  if hh_vals  else 0.035

    upsert = text("""
        INSERT INTO mlb.juiced2_scores (
            batter_id, batter_name, season, bat_side,
            slg, ops, hard_hit_pct, avg_goose2_faced,
            slg_vs_expected, ops_vs_expected,
            slg_idx, ops_idx, quality_adj_idx, hard_hit_idx,
            juiced2_plus, total_pa,
            bat_speed, squared_up_pct, blast_rate, hard_hit_source
        ) VALUES (
            :batter_id, :batter_name, :season, :bat_side,
            :slg, :ops, :hard_hit_pct, :avg_goose2_faced,
            :slg_vs_expected, :ops_vs_expected,
            :slg_idx, :ops_idx, :quality_adj_idx, :hard_hit_idx,
            :juiced2_plus, :total_pa,
            :bat_speed, :squared_up_pct, :blast_rate, :hard_hit_source
        )
        ON CONFLICT (batter_id, season) DO UPDATE SET
            slg              = EXCLUDED.slg,
            ops              = EXCLUDED.ops,
            avg_goose2_faced = EXCLUDED.avg_goose2_faced,
            slg_vs_expected  = EXCLUDED.slg_vs_expected,
            ops_vs_expected  = EXCLUDED.ops_vs_expected,
            quality_adj_idx  = EXCLUDED.quality_adj_idx,
            hard_hit_idx     = EXCLUDED.hard_hit_idx,
            juiced2_plus     = EXCLUDED.juiced2_plus,
            bat_speed        = EXCLUDED.bat_speed,
            squared_up_pct   = EXCLUDED.squared_up_pct,
            blast_rate       = EXCLUDED.blast_rate,
            hard_hit_source  = EXCLUDED.hard_hit_source,
            computed_at      = NOW()
    """)

    for r in rows:
        slg = float(r['slg'] or 0)
        obp = float(r['obp'] or 0)
        ops = obp + slg
        hh  = float(r['hr_rate'] or 0)
        avg_goose2 = float(r['avg_goose2_faced'] or 100)

        expected_slg = lg_avg_slg * (100 / max(avg_goose2, 70))
        expected_ops = lg_avg_ops * (100 / max(avg_goose2, 70))

        slg_vs_exp = slg / max(expected_slg, 0.01)
        ops_vs_exp = ops / max(expected_ops, 0.01)

        slg_idx     = round((slg / max(lg_avg_slg, 0.01)) * 100, 1)
        ops_idx     = round((ops / max(lg_avg_ops, 0.01)) * 100, 1)
        quality_idx = round(slg_vs_exp * 100, 1)

        # Hard hit: bat tracking composite (50% bat speed, 30% squared up, 20% blast)
        bt = bat_tracking.get(int(r['batter_id']), {})
        bat_speed_val  = float(bt.get('bat_speed') or 0)
        squared_up_val = float(bt.get('squared_up_pct') or 0)
        blast_val      = float(bt.get('blast_rate') or 0)

        if bat_speed_val > 0:
            hh_idx = round(
                (bat_speed_val / max(lg_bat_speed, 1)) * 100  * 0.50 +
                (squared_up_val / max(lg_squared_up, 0.01)) * 100 * 0.30 +
                (blast_val / max(lg_blast_rate, 0.01)) * 100  * 0.20,
                1
            )
            hh_source = "bat_tracking"
        else:
            hh_idx = round((hh / max(lg_avg_hh, 0.001)) * 100, 1)
            hh_source = "hr_proxy"

        juiced2 = (
            ops_idx     * JUICED2_WEIGHTS['ops']         +
            quality_idx * JUICED2_WEIGHTS['quality_adj'] +
            slg_idx     * JUICED2_WEIGHTS['slg']         +
            hh_idx      * JUICED2_WEIGHTS['hard_hit']
        )

        db.execute(upsert, {
            "batter_id":       int(r['batter_id']),
            "batter_name":     r['batter_name'],
            "season":          score_season,
            "bat_side":        r['bat_side'],
            "slg":             round(slg, 3),
            "ops":             round(ops, 3),
            "hard_hit_pct":    round(hh, 3),
            "avg_goose2_faced": round(avg_goose2, 1),
            "slg_vs_expected": round(slg_vs_exp, 3),
            "ops_vs_expected": round(ops_vs_exp, 3),
            "slg_idx":         slg_idx,
            "ops_idx":         ops_idx,
            "quality_adj_idx": quality_idx,
            "hard_hit_idx":    hh_idx,
            "juiced2_plus":    round(juiced2, 1),
            "total_pa":        int(r['total_pa']),
            "bat_speed":       float(bt['bat_speed']) if bt.get('bat_speed') else None,
            "squared_up_pct":  float(bt['squared_up_pct']) if bt.get('squared_up_pct') else None,
            "blast_rate":      float(bt['blast_rate']) if bt.get('blast_rate') else None,
            "hard_hit_source": hh_source,
        })

    db.commit()
    print(f"  Stored {len(rows)} Juiced+ 2 scores")


# ── PART D: VALIDATION ────────────────────────────────────────────────────────

def validate_goose2(season: int, db):
    print(f"\n  Validating Goose+ 2 ({season})...")
    sql = text("""
        SELECT
            go.pitcher_id,
            go.pitcher_name,
            go.goose2_plus,
            go.total_pitches,
            ROUND(SUM(bp.strikeouts)::numeric /
                NULLIF(SUM(bp.batters_faced), 0) * 100, 2) AS k_rate,
            ROUND(SUM(bp.home_runs)::numeric /
                NULLIF(SUM(bp.batters_faced), 0) * 100, 2) AS hr_rate,
            MAX(br.era_plus) AS era_plus
        FROM mlb.goose2_overall go
        JOIN mlb.games g ON g.season = :season AND g.game_type = 'R'
        LEFT JOIN mlb.boxscore_pitching bp ON bp.player_id = go.pitcher_id
            AND bp.game_pk = g.game_pk
        LEFT JOIN mlb.bbref_pitching br ON br.mlb_id = go.pitcher_id
            AND br.year_id = :season
        WHERE go.season = :season
          AND go.total_pitches >= 200
        GROUP BY go.pitcher_id, go.pitcher_name, go.goose2_plus, go.total_pitches
        HAVING SUM(bp.batters_faced) >= 50
        ORDER BY go.goose2_plus DESC
    """)
    rows = db.execute(sql, {"season": season}).mappings().all()
    df = pd.DataFrame([dict(r) for r in rows])

    if df.empty or len(df) < 10:
        print(f"  Insufficient data for validation (n={len(df)})")
        # Still show top scorers without correlation
        top_sql = text("""
            SELECT pitcher_name, goose2_plus, goose2_vs_lhh, goose2_vs_rhh, total_pitches
            FROM mlb.goose2_overall WHERE season = :season
            ORDER BY goose2_plus DESC LIMIT 15
        """)
        top = db.execute(top_sql, {"season": season}).mappings().all()
        print(f"\n  Top 15 Goose+ 2 ({season}):")
        for r in top:
            print(f"    {(r['pitcher_name'] or '?'):<28} | G2+: {r['goose2_plus']:>6.1f} | "
                  f"vs LHH: {r['goose2_vs_lhh'] or '—':>6} | vs RHH: {r['goose2_vs_rhh'] or '—':>6} | "
                  f"P: {r['total_pitches']}")
        return

    for label, col, expected_pos in [('K Rate', 'k_rate', True), ('HR Rate', 'hr_rate', False), ('ERA+', 'era_plus', True)]:
        valid = df[['goose2_plus', col]].dropna()
        if len(valid) < 10:
            print(f"  {label:<15} n/a (n={len(valid)})")
            continue
        corr = valid['goose2_plus'].corr(valid[col])
        ok = '✅' if (corr > 0) == expected_pos else '❌'
        exp = '+' if expected_pos else '-'
        print(f"  {label:<15} r={corr:>+.3f}  {ok} (expected {exp})")

    print(f"\n  Top 15 Goose+ 2 ({season}):")
    for _, r in df.nlargest(15, 'goose2_plus').iterrows():
        print(f"    {(r['pitcher_name'] or '?'):<28} | G2+: {r['goose2_plus']:>6.1f} | "
              f"K%: {r['k_rate'] or '—':>5} | HR%: {r['hr_rate'] or '—':>5} | ERA+: {r['era_plus'] or '—'}")


def validate_juiced2(season: int, db):
    print(f"\n  Validating Juiced+ 2 ({season})...")
    sql = text("""
        SELECT
            j.batter_id,
            j.batter_name,
            j.juiced2_plus,
            j.quality_adj_idx,
            j.avg_goose2_faced,
            j.total_pa,
            SUM(bb.home_runs) AS hr,
            SUM(bb.hits)      AS hits,
            ROUND(SUM(bb.hits)::numeric / NULLIF(SUM(bb.at_bats), 0), 3) AS avg,
            MAX(br.ops_plus) AS ops_plus
        FROM mlb.juiced2_scores j
        JOIN mlb.games g ON g.season = :season AND g.game_type = 'R'
        LEFT JOIN mlb.boxscore_batting bb ON bb.player_id = j.batter_id
            AND bb.game_pk = g.game_pk
        LEFT JOIN mlb.bbref_batting br ON br.mlb_id = j.batter_id
            AND br.year_id = :season
        WHERE j.season = :season AND j.total_pa >= 100
        GROUP BY j.batter_id, j.batter_name, j.juiced2_plus,
                 j.quality_adj_idx, j.avg_goose2_faced, j.total_pa
        ORDER BY j.juiced2_plus DESC
    """)
    rows = db.execute(sql, {"season": season}).mappings().all()
    df = pd.DataFrame([dict(r) for r in rows])

    if df.empty or len(df) < 10:
        print(f"  Insufficient data (n={len(df)})")
        top_sql = text("""
            SELECT batter_name, juiced2_plus, quality_adj_idx, avg_goose2_faced,
                   slg, ops, total_pa
            FROM mlb.juiced2_scores WHERE season = :season
            ORDER BY juiced2_plus DESC LIMIT 15
        """)
        top = db.execute(top_sql, {"season": season}).mappings().all()
        print(f"\n  Top 15 Juiced+ 2 ({season}):")
        for r in top:
            print(f"    {(r['batter_name'] or '?'):<28} | J2+: {r['juiced2_plus']:>6.1f} | "
                  f"QAdj: {r['quality_adj_idx']:>6.1f} | vs G2: {r['avg_goose2_faced']:>5.1f} | "
                  f"SLG: {r['slg']:.3f} | PA: {r['total_pa']}")
        return

    for label, col in [('HR', 'hr'), ('Hits', 'hits'), ('AVG', 'avg'), ('OPS+', 'ops_plus')]:
        valid = df[['juiced2_plus', col]].dropna()
        if len(valid) < 10:
            continue
        corr = valid['juiced2_plus'].corr(valid[col])
        ok = '✅' if corr > 0 else '❌'
        print(f"  {label:<15} r={corr:>+.3f}  {ok} (expected +)")

    top20 = df.nlargest(20, 'juiced2_plus')
    print(f"\n  Quality check — avg Goose+ 2 faced by top 20 Juiced+ 2 batters: "
          f"{top20['avg_goose2_faced'].mean():.1f} (should be >= 100)")

    print(f"\n  Top 15 Juiced+ 2 ({season}):")
    for _, r in df.nlargest(15, 'juiced2_plus').iterrows():
        print(f"    {(r['batter_name'] or '?'):<28} | J2+: {r['juiced2_plus']:>6.1f} | "
              f"QAdj: {r['quality_adj_idx']:>6.1f} | vs G2: {r['avg_goose2_faced']:>5.1f} | PA: {r['total_pa']}")


def store_2026_baselines_snapshot(score_season: int, db):
    """Store current season baselines to league_pitch_baselines table."""
    df = compute_league_baselines(score_season, db)
    slg_df = compute_slg_baseline(score_season, db)
    if not df.empty:
        store_baselines(df, slg_df, score_season, db)
        print(f"  Stored {score_season} baseline snapshot")


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-season", type=int, default=2025)
    parser.add_argument("--score-season",    type=int, default=2026)
    parser.add_argument("--skip-baselines",  action="store_true")
    parser.add_argument("--skip-goose2",     action="store_true")
    parser.add_argument("--skip-juiced2",    action="store_true")
    parser.add_argument("--validate-only",   action="store_true")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        if args.validate_only:
            validate_goose2(args.score_season, db)
            validate_juiced2(args.score_season, db)
        else:
            print(f"=== Building Goose+ 2 / Juiced+ 2 ===")
            print(f"  Score season:    {args.score_season}")

            if not args.skip_baselines:
                print(f"\n  Snapshotting {args.score_season} baselines...")
                store_2026_baselines_snapshot(args.score_season, db)

            if not args.skip_goose2:
                build_goose2(args.score_season, args.baseline_season, db)

            if not args.skip_juiced2:
                build_juiced2(args.score_season, db)

            print(f"\n=== Validation ===")
            validate_goose2(args.score_season, db)
            validate_juiced2(args.score_season, db)
    finally:
        db.close()
