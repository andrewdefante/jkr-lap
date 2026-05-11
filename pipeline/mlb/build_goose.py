"""
Build Goose+ pitch quality scores and Juiced+ batter scores.

Goose+ methodology:
1. Compute percentile quintile boundaries per pitch type from 2024+2025 data
2. Compute outcome stats per bucket (strike%, whiff%, pitch-level OPS/SLG)
3. Score each pitcher's pitches relative to league distribution
4. Weight by usage to produce overall Goose+ per season

Juiced+ methodology:
1. Find PA against pitchers whose primary pitch falls in each Goose+ bucket
2. Compute batter OPS in those PA vs league average in same bucket
3. Scale to 100 = league average
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import argparse
import numpy as np
import pandas as pd
from database import SessionLocal
from sqlalchemy import text

PITCH_TYPES = ['FF', 'SI', 'SL', 'CH', 'CU', 'ST', 'FC', 'FS']
LEAGUE_AVG_BATTER_WHIFF = 0.267  # matches LEAGUE_WHIFF in matchup engine
HISTORICAL_SEASONS = [2024, 2025]
CHARACTERISTICS = ['start_speed', 'spin_rate', 'pfx_x', 'pfx_z']
CHAR_NAMES = {
    'start_speed': 'velo',
    'spin_rate': 'spin',
    'pfx_x': 'hbreak',
    'pfx_z': 'vbreak',
}


def compute_boundaries(db):
    """Step 1: Compute quintile boundaries per pitch type from historical data."""
    print("\n=== Step 1: Computing Quintile Boundaries ===")
    seasons_str = ','.join(str(s) for s in HISTORICAL_SEASONS)

    for pt in PITCH_TYPES:
        sql = text(f"""
            SELECT p.start_speed, p.spin_rate, p.pfx_x, p.pfx_z
            FROM mlb.pitches p
            JOIN mlb.games g ON g.game_pk = p.game_pk
            WHERE p.pitch_type_code = :pt
            AND g.season IN ({seasons_str})
            AND g.game_type = 'R'
            AND p.start_speed IS NOT NULL
            AND p.spin_rate IS NOT NULL
            AND p.pfx_x IS NOT NULL
            AND p.pfx_z IS NOT NULL
        """)
        rows = db.execute(sql, {"pt": pt}).fetchall()
        if len(rows) < 100:
            print(f"  {pt}: insufficient data ({len(rows)} pitches), skipping")
            continue

        df = pd.DataFrame(rows, columns=['start_speed', 'spin_rate', 'pfx_x', 'pfx_z'])
        df = df.astype(float)
        print(f"  {pt}: {len(df):,} pitches")

        for col in CHARACTERISTICS:
            vals = df[col].dropna()
            if len(vals) < 100:
                continue

            upsert = text("""
                INSERT INTO mlb.goose_pitch_boundaries
                    (pitch_type_code, characteristic, q20, q40, q60, q80,
                     mean, std_dev, sample_size)
                VALUES (:pt, :char, :q20, :q40, :q60, :q80, :mean, :std, :n)
                ON CONFLICT (pitch_type_code, characteristic) DO UPDATE SET
                    q20 = EXCLUDED.q20, q40 = EXCLUDED.q40,
                    q60 = EXCLUDED.q60, q80 = EXCLUDED.q80,
                    mean = EXCLUDED.mean, std_dev = EXCLUDED.std_dev,
                    sample_size = EXCLUDED.sample_size,
                    computed_at = NOW()
            """)
            db.execute(upsert, {
                "pt": pt,
                "char": CHAR_NAMES[col],
                "q20": float(np.percentile(vals, 20)),
                "q40": float(np.percentile(vals, 40)),
                "q60": float(np.percentile(vals, 60)),
                "q80": float(np.percentile(vals, 80)),
                "mean": float(vals.mean()),
                "std": float(vals.std()),
                "n": len(vals),
            })

    db.commit()
    print("  Boundaries stored.")


def load_boundaries(db):
    """Load boundaries into dict for fast lookup."""
    rows = db.execute(text("""
        SELECT pitch_type_code, characteristic, q20, q40, q60, q80, mean, std_dev
        FROM mlb.goose_pitch_boundaries
    """)).mappings().all()

    bounds = {}
    for r in rows:
        pt = r['pitch_type_code']
        char = r['characteristic']
        if pt not in bounds:
            bounds[pt] = {}
        bounds[pt][char] = dict(r)
    return bounds


def get_quintile(value, boundaries):
    if value is None or not boundaries:
        return 3
    if value <= boundaries['q20']:
        return 1
    elif value <= boundaries['q40']:
        return 2
    elif value <= boundaries['q60']:
        return 3
    elif value <= boundaries['q80']:
        return 4
    return 5


def get_percentile_score(value, boundaries):
    if value is None or not boundaries:
        return 50.0

    breakpoints = [
        (boundaries['q20'], 20),
        (boundaries['q40'], 40),
        (boundaries['q60'], 60),
        (boundaries['q80'], 80),
    ]

    if value <= breakpoints[0][0]:
        denom = breakpoints[0][0] if breakpoints[0][0] != 0 else 1
        return max(0.0, 20.0 * value / denom)
    if value >= breakpoints[-1][0]:
        return min(100.0, 80.0 + 20.0 * (value - breakpoints[-1][0]) /
                   max(1.0, boundaries.get('std_dev', 1.0)))

    for i in range(len(breakpoints) - 1):
        lo_val, lo_pct = breakpoints[i]
        hi_val, hi_pct = breakpoints[i + 1]
        if lo_val <= value <= hi_val:
            t = (value - lo_val) / max(0.001, hi_val - lo_val)
            return lo_pct + t * (hi_pct - lo_pct)

    return 50.0


def compute_bucket_outcomes(db, bounds):
    """Step 2: Compute outcome stats per pitch type + velocity quintile."""
    print("\n=== Step 2: Computing Bucket Outcomes ===")
    seasons_str = ','.join(str(s) for s in HISTORICAL_SEASONS)

    for pt in PITCH_TYPES:
        if pt not in bounds or 'velo' not in bounds[pt]:
            continue

        vb = bounds[pt]['velo']
        sql = text(f"""
            SELECT
                p.start_speed, p.spin_rate, p.pfx_x, p.pfx_z,
                p.call_code, ab.event_type
            FROM mlb.pitches p
            JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
                AND ab.at_bat_index = p.at_bat_index
            JOIN mlb.games g ON g.game_pk = p.game_pk
            WHERE p.pitch_type_code = :pt
            AND g.season IN ({seasons_str})
            AND g.game_type = 'R'
            AND p.start_speed IS NOT NULL
        """)
        rows = db.execute(sql, {"pt": pt}).fetchall()
        if not rows:
            continue

        df = pd.DataFrame(rows, columns=[
            'start_speed', 'spin_rate', 'pfx_x', 'pfx_z', 'call_code', 'event_type'
        ])
        df['start_speed'] = pd.to_numeric(df['start_speed'], errors='coerce')
        df['spin_rate'] = pd.to_numeric(df['spin_rate'], errors='coerce')

        df['velo_q'] = df['start_speed'].apply(lambda v: get_quintile(v, vb))

        for q in range(1, 6):
            qdf = df[df['velo_q'] == q]
            if len(qdf) < 50:
                continue

            total = len(qdf)
            strikes = qdf['call_code'].isin(['S','W','T','C','F','D','E','L','X']).sum()
            swings = qdf['call_code'].isin(['S','W','T','F','D','E','X']).sum()
            whiffs = qdf['call_code'].isin(['S','W','T']).sum()
            bip_mask = qdf['call_code'] == 'X'
            bip_df = qdf[bip_mask]

            hits = bip_df['event_type'].isin(['single','double','triple','home_run']).sum()
            singles = (bip_df['event_type'] == 'single').sum()
            doubles = (bip_df['event_type'] == 'double').sum()
            triples = (bip_df['event_type'] == 'triple').sum()
            hrs = (bip_df['event_type'] == 'home_run').sum()
            total_bases = singles + 2*doubles + 3*triples + 4*hrs

            sw = max(int(swings), 1)
            pitch_obp = float(hits) / sw
            pitch_slg = float(total_bases) / sw
            pitch_ops = pitch_obp + pitch_slg

            upsert = text("""
                INSERT INTO mlb.goose_bucket_outcomes
                    (pitch_type_code, velo_quintile, pitches,
                     strike_pct, whiff_pct, pitch_ops, pitch_slg, pitch_obp,
                     avg_spin, avg_hbreak, avg_vbreak, avg_velo)
                VALUES (:pt, :q, :pitches, :strike_pct, :whiff_pct,
                        :pitch_ops, :pitch_slg, :pitch_obp,
                        :avg_spin, :avg_hbreak, :avg_vbreak, :avg_velo)
                ON CONFLICT (pitch_type_code, velo_quintile) DO UPDATE SET
                    pitches = EXCLUDED.pitches,
                    strike_pct = EXCLUDED.strike_pct,
                    whiff_pct = EXCLUDED.whiff_pct,
                    pitch_ops = EXCLUDED.pitch_ops,
                    pitch_slg = EXCLUDED.pitch_slg,
                    pitch_obp = EXCLUDED.pitch_obp,
                    avg_spin = EXCLUDED.avg_spin,
                    avg_hbreak = EXCLUDED.avg_hbreak,
                    avg_vbreak = EXCLUDED.avg_vbreak,
                    avg_velo = EXCLUDED.avg_velo,
                    computed_at = NOW()
            """)
            spin_col = qdf['spin_rate'].dropna()
            pfx_x_col = qdf['pfx_x'].dropna()
            pfx_z_col = qdf['pfx_z'].dropna()
            db.execute(upsert, {
                "pt": pt, "q": q,
                "pitches": int(total),
                "strike_pct": float(strikes) / total,
                "whiff_pct": float(whiffs) / sw,
                "pitch_ops": pitch_ops,
                "pitch_slg": pitch_slg,
                "pitch_obp": pitch_obp,
                "avg_spin": float(spin_col.mean()) if len(spin_col) > 0 else None,
                "avg_hbreak": float(pfx_x_col.mean()) if len(pfx_x_col) > 0 else None,
                "avg_vbreak": float(pfx_z_col.mean()) if len(pfx_z_col) > 0 else None,
                "avg_velo": float(qdf['start_speed'].mean()),
            })

    db.commit()
    print("  Bucket outcomes stored.")


def compute_goose_scores(db, bounds, season):
    """Step 3: Score each pitcher's pitches for a given season."""
    print(f"\n=== Step 3: Computing Goose+ Scores — {season} ===")

    outcomes = {}
    for r in db.execute(text("""
        SELECT pitch_type_code, velo_quintile, strike_pct, whiff_pct,
               pitch_ops, pitches
        FROM mlb.goose_bucket_outcomes
    """)).mappings().all():
        pt = r['pitch_type_code']
        q = r['velo_quintile']
        if pt not in outcomes:
            outcomes[pt] = {}
        outcomes[pt][q] = dict(r)

    # League averages per pitch type (pitch-count weighted across quintiles)
    lg_avg = {}
    for pt in PITCH_TYPES:
        if pt not in outcomes:
            continue
        total_p = sum(outcomes[pt][q]['pitches'] for q in outcomes[pt])
        if total_p == 0:
            continue
        lg_avg[pt] = {
            'strike_pct': sum(outcomes[pt][q]['strike_pct'] * outcomes[pt][q]['pitches']
                              for q in outcomes[pt]) / total_p,
            'whiff_pct': sum(outcomes[pt][q]['whiff_pct'] * outcomes[pt][q]['pitches']
                             for q in outcomes[pt]) / total_p,
            'pitch_ops': sum(outcomes[pt][q]['pitch_ops'] * outcomes[pt][q]['pitches']
                             for q in outcomes[pt]) / total_p,
        }

    sql = text("""
        SELECT
            ab.pitcher_id,
            MAX(ab.pitcher_name) as pitcher_name,
            p.pitch_type_code,
            ab.bat_side,
            COUNT(*) as pitches,
            AVG(p.start_speed) as avg_velo,
            AVG(p.spin_rate::float) as avg_spin,
            AVG(p.pfx_x) as avg_hbreak,
            AVG(p.pfx_z) as avg_vbreak,
            AVG(CASE WHEN p.call_code IN ('S','W','T','C','F','D','E','L','X')
                THEN 1.0 ELSE 0.0 END) as strike_pct,
            SUM(CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END) /
                NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X')
                    THEN 1.0 ELSE 0.0 END), 0) as whiff_pct,
            SUM(CASE WHEN p.call_code = 'X' AND ab.event_type IN
                ('single','double','triple','home_run') THEN 1.0 ELSE 0.0 END) /
                NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X')
                    THEN 1.0 ELSE 0.0 END), 0) as pitch_obp,
            SUM(CASE
                WHEN p.call_code = 'X' AND ab.event_type = 'single' THEN 1.0
                WHEN p.call_code = 'X' AND ab.event_type = 'double' THEN 2.0
                WHEN p.call_code = 'X' AND ab.event_type = 'triple' THEN 3.0
                WHEN p.call_code = 'X' AND ab.event_type = 'home_run' THEN 4.0
                ELSE 0.0 END) /
                NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X')
                    THEN 1.0 ELSE 0.0 END), 0) as pitch_slg
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE g.season = :season
        AND g.game_type = 'R'
        AND p.pitch_type_code IN ('FF','SI','SL','CH','CU','ST','FC','FS')
        AND p.start_speed IS NOT NULL
        GROUP BY ab.pitcher_id, p.pitch_type_code, ab.bat_side
        HAVING COUNT(*) >= 10
    """)

    rows = db.execute(sql, {"season": season}).mappings().all()
    df = pd.DataFrame([dict(r) for r in rows])

    if df.empty:
        print(f"  No data for {season}")
        return

    # Convert all numeric columns (psycopg2 returns Decimal for computed ratios)
    for col in ['avg_velo','avg_spin','avg_hbreak','avg_vbreak',
                'strike_pct','whiff_pct','pitch_obp','pitch_slg']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    print(f"  Loaded {len(df)} pitcher-pitch-type-side rows")

    # Aggregate over bat_side to get combined per-pitcher-pitch_type stats
    agg_cols = {
        'pitches': 'sum',
        'pitches_vs_lhh': 'first',   # filled below
        'pitches_vs_rhh': 'first',
        'avg_velo': 'mean',
        'avg_spin': 'mean',
        'avg_hbreak': 'mean',
        'avg_vbreak': 'mean',
        'strike_pct': 'mean',
        'whiff_pct': 'mean',
        'pitch_obp': 'mean',
        'pitch_slg': 'mean',
    }

    def agg_group(g):
        total = g['pitches'].sum()
        lhh = g.loc[g['bat_side'] == 'L', 'pitches'].sum()
        rhh = g.loc[g['bat_side'] == 'R', 'pitches'].sum()
        wa = lambda col: (g[col].fillna(0) * g['pitches']).sum() / max(total, 1)
        return pd.Series({
            'pitcher_name': g['pitcher_name'].iloc[0],
            'pitches': int(total),
            'pitches_vs_lhh': int(lhh),
            'pitches_vs_rhh': int(rhh),
            'avg_velo': wa('avg_velo'),
            'avg_spin': wa('avg_spin'),
            'avg_hbreak': wa('avg_hbreak'),
            'avg_vbreak': wa('avg_vbreak'),
            'strike_pct': wa('strike_pct'),
            'whiff_pct': wa('whiff_pct'),
            'pitch_obp': wa('pitch_obp'),
            'pitch_slg': wa('pitch_slg'),
        })

    combined = (df.groupby(['pitcher_id', 'pitch_type_code'], group_keys=True)
                  .apply(agg_group, include_groups=False)
                  .reset_index())

    stored = 0
    for _, r in combined.iterrows():
        pt = r['pitch_type_code']
        if pt not in bounds or 'velo' not in bounds[pt]:
            continue

        vb = bounds[pt]['velo']
        sb = bounds[pt].get('spin')
        hb = bounds[pt].get('hbreak')
        vvb = bounds[pt].get('vbreak')

        velo_q = get_quintile(r['avg_velo'], vb)
        velo_pct = get_percentile_score(r['avg_velo'], vb)
        spin_pct = get_percentile_score(r['avg_spin'], sb) if sb else 50.0
        hbreak_abs = abs(r['avg_hbreak']) if pd.notna(r['avg_hbreak']) else 0.0
        vbreak_abs = abs(r['avg_vbreak']) if pd.notna(r['avg_vbreak']) else 0.0
        hbreak_pct = get_percentile_score(hbreak_abs, hb) if hb else 50.0
        vbreak_pct = get_percentile_score(vbreak_abs, vvb) if vvb else 50.0

        bkt = outcomes.get(pt, {}).get(velo_q, {})
        lg = lg_avg.get(pt, {})
        pitch_ops = (r['pitch_obp'] or 0) + (r['pitch_slg'] or 0)

        if bkt and lg:
            lg_strike = lg.get('strike_pct', 0.60)
            lg_whiff = lg.get('whiff_pct', 0.27)
            lg_ops = lg.get('pitch_ops', 0.80)
            strike_rel = (r['strike_pct'] or 0) / max(lg_strike, 0.001)
            whiff_rel = (r['whiff_pct'] or 0) / LEAGUE_AVG_BATTER_WHIFF
            ops_rel = max(0.5, 2.0 - (pitch_ops / max(lg_ops, 0.001)))
            outcome_score = (strike_rel * 0.30 + whiff_rel * 0.40 + ops_rel * 0.30) * 50
        else:
            outcome_score = 50.0

        physical_score = (velo_pct*0.35 + spin_pct*0.20 + hbreak_pct*0.15 + vbreak_pct*0.15)
        raw_score = physical_score * 0.85 + outcome_score * 0.15
        goose_plus = raw_score * 2.0

        # Platoon splits: reuse physical score, recalculate outcome from side-specific rows
        def side_goose(bat_side):
            side = df[(df['pitcher_id'] == r['pitcher_id']) &
                      (df['pitch_type_code'] == pt) & (df['bat_side'] == bat_side)]
            if side.empty:
                return None
            sr = side.iloc[0]
            so = (sr['pitch_obp'] or 0) + (sr['pitch_slg'] or 0)
            if lg:
                s_strike = (sr['strike_pct'] or 0) / max(lg.get('strike_pct', 0.60), 0.001)
                s_whiff = (sr['whiff_pct'] or 0) / LEAGUE_AVG_BATTER_WHIFF
                s_ops = max(0.5, 2.0 - (so / max(lg.get('pitch_ops', 0.80), 0.001)))
                s_outcome = (s_strike*0.30 + s_whiff*0.40 + s_ops*0.30) * 50
            else:
                s_outcome = 50.0
            return (physical_score * 0.85 + s_outcome * 0.15) * 2.0

        goose_lhh = side_goose('L')
        goose_rhh = side_goose('R')

        upsert = text("""
            INSERT INTO mlb.goose_scores (
                pitcher_id, pitcher_name, season, pitch_type_code,
                avg_velo, avg_spin, avg_hbreak, avg_vbreak, velo_quintile,
                strike_pct, whiff_pct, pitch_ops, pitch_slg,
                velo_score, spin_score, hbreak_score, vbreak_score,
                outcome_score, goose_plus, pitches_thrown,
                pitches_vs_lhh, pitches_vs_rhh,
                goose_plus_vs_lhh, goose_plus_vs_rhh
            ) VALUES (
                :pitcher_id, :pitcher_name, :season, :pt,
                :avg_velo, :avg_spin, :avg_hbreak, :avg_vbreak, :velo_q,
                :strike_pct, :whiff_pct, :pitch_ops, :pitch_slg,
                :velo_pct, :spin_pct, :hbreak_pct, :vbreak_pct,
                :outcome_score, :goose_plus, :pitches,
                :pitches_vs_lhh, :pitches_vs_rhh, :goose_lhh, :goose_rhh
            )
            ON CONFLICT (pitcher_id, season, pitch_type_code) DO UPDATE SET
                avg_velo = EXCLUDED.avg_velo, avg_spin = EXCLUDED.avg_spin,
                avg_hbreak = EXCLUDED.avg_hbreak, avg_vbreak = EXCLUDED.avg_vbreak,
                velo_quintile = EXCLUDED.velo_quintile,
                strike_pct = EXCLUDED.strike_pct, whiff_pct = EXCLUDED.whiff_pct,
                pitch_ops = EXCLUDED.pitch_ops, pitch_slg = EXCLUDED.pitch_slg,
                velo_score = EXCLUDED.velo_score, spin_score = EXCLUDED.spin_score,
                hbreak_score = EXCLUDED.hbreak_score, vbreak_score = EXCLUDED.vbreak_score,
                outcome_score = EXCLUDED.outcome_score, goose_plus = EXCLUDED.goose_plus,
                pitches_thrown = EXCLUDED.pitches_thrown,
                pitches_vs_lhh = EXCLUDED.pitches_vs_lhh,
                pitches_vs_rhh = EXCLUDED.pitches_vs_rhh,
                goose_plus_vs_lhh = EXCLUDED.goose_plus_vs_lhh,
                goose_plus_vs_rhh = EXCLUDED.goose_plus_vs_rhh,
                computed_at = NOW()
        """)
        db.execute(upsert, {
            "pitcher_id": int(r['pitcher_id']),
            "pitcher_name": str(r['pitcher_name']),
            "season": season, "pt": pt,
            "avg_velo": float(r['avg_velo']) if pd.notna(r['avg_velo']) else None,
            "avg_spin": float(r['avg_spin']) if pd.notna(r['avg_spin']) else None,
            "avg_hbreak": float(r['avg_hbreak']) if pd.notna(r['avg_hbreak']) else None,
            "avg_vbreak": float(r['avg_vbreak']) if pd.notna(r['avg_vbreak']) else None,
            "velo_q": velo_q,
            "strike_pct": float(r['strike_pct']) if pd.notna(r['strike_pct']) else None,
            "whiff_pct": float(r['whiff_pct']) if pd.notna(r['whiff_pct']) else None,
            "pitch_ops": float(pitch_ops),
            "pitch_slg": float(r['pitch_slg']) if pd.notna(r['pitch_slg']) else None,
            "velo_pct": float(velo_pct), "spin_pct": float(spin_pct),
            "hbreak_pct": float(hbreak_pct), "vbreak_pct": float(vbreak_pct),
            "outcome_score": float(outcome_score), "goose_plus": float(goose_plus),
            "pitches": int(r['pitches']),
            "pitches_vs_lhh": int(r['pitches_vs_lhh']),
            "pitches_vs_rhh": int(r['pitches_vs_rhh']),
            "goose_lhh": float(goose_lhh) if goose_lhh is not None else None,
            "goose_rhh": float(goose_rhh) if goose_rhh is not None else None,
        })
        stored += 1

    db.commit()
    print(f"  Stored {stored} Goose+ pitch scores for {season}")


def compute_overall_goose(db, season):
    """Step 4: Compute usage-weighted overall Goose+ per pitcher."""
    print(f"\n=== Step 4: Computing Overall Goose+ — {season} ===")

    sql = text("""
        SELECT
            pitcher_id,
            MAX(pitcher_name) as pitcher_name,
            SUM(goose_plus * pitches_thrown) / NULLIF(SUM(pitches_thrown), 0) as goose_plus,
            SUM(COALESCE(goose_plus_vs_lhh, goose_plus) * pitches_vs_lhh) /
                NULLIF(SUM(pitches_vs_lhh), 0) as goose_plus_vs_lhh,
            SUM(COALESCE(goose_plus_vs_rhh, goose_plus) * pitches_vs_rhh) /
                NULLIF(SUM(pitches_vs_rhh), 0) as goose_plus_vs_rhh,
            SUM(pitches_thrown) as total_pitches
        FROM mlb.goose_scores
        WHERE season = :season
        GROUP BY pitcher_id
        HAVING SUM(pitches_thrown) >= 50
    """)
    rows = db.execute(sql, {"season": season}).mappings().all()

    # Delete existing season aggregates (NULL game_pk) then re-insert
    # (UNIQUE index uses COALESCE(game_pk, -1) so NULLs collide safely on delete)
    db.execute(text("""
        DELETE FROM mlb.goose_overall
        WHERE season = :season AND game_pk IS NULL
    """), {"season": season})

    insert = text("""
        INSERT INTO mlb.goose_overall
            (pitcher_id, pitcher_name, season, game_pk,
             goose_plus, goose_plus_vs_lhh, goose_plus_vs_rhh, total_pitches)
        VALUES
            (:pitcher_id, :pitcher_name, :season, NULL,
             :goose_plus, :goose_lhh, :goose_rhh, :total_pitches)
    """)

    stored = 0
    for r in rows:
        db.execute(insert, {
            "pitcher_id": int(r['pitcher_id']),
            "pitcher_name": str(r['pitcher_name']),
            "season": season,
            "goose_plus": float(r['goose_plus']) if r['goose_plus'] else None,
            "goose_lhh": float(r['goose_plus_vs_lhh']) if r['goose_plus_vs_lhh'] else None,
            "goose_rhh": float(r['goose_plus_vs_rhh']) if r['goose_plus_vs_rhh'] else None,
            "total_pitches": int(r['total_pitches']),
        })
        stored += 1

    db.commit()
    print(f"  Stored {stored} overall Goose+ scores for {season}")


def compute_juiced_scores(db, season):
    """Step 5: Compute Juiced+ batter scores."""
    print(f"\n=== Step 5: Computing Juiced+ Scores — {season} ===")

    sql = text("""
        WITH pitcher_primary AS (
            SELECT DISTINCT ON (pitcher_id, season)
                pitcher_id, season, pitch_type_code, velo_quintile
            FROM mlb.goose_scores
            WHERE season = :season
            ORDER BY pitcher_id, season, pitches_thrown DESC
        ),
        pa_data AS (
            SELECT
                ab.batter_id,
                MAX(ab.batter_name) as batter_name,
                pp.pitch_type_code,
                pp.velo_quintile,
                COUNT(*) as pa,
                SUM(CASE WHEN ab.event_type IN ('single','double','triple',
                    'home_run','field_out','force_out','grounded_into_double_play',
                    'strikeout','fielders_choice') THEN 1 ELSE 0 END) as real_ab,
                SUM(CASE WHEN ab.event_type IN ('single','double','triple','home_run')
                    THEN 1 ELSE 0 END) as hits,
                SUM(CASE
                    WHEN ab.event_type = 'single' THEN 1
                    WHEN ab.event_type = 'double' THEN 2
                    WHEN ab.event_type = 'triple' THEN 3
                    WHEN ab.event_type = 'home_run' THEN 4
                    ELSE 0 END) as total_bases
            FROM mlb.at_bats ab
            JOIN mlb.games g ON g.game_pk = ab.game_pk
            JOIN pitcher_primary pp ON pp.pitcher_id = ab.pitcher_id
            WHERE g.season = :season AND g.game_type = 'R'
            GROUP BY ab.batter_id, pp.pitch_type_code, pp.velo_quintile
            HAVING COUNT(*) >= 5
        )
        SELECT * FROM pa_data
    """)
    rows = db.execute(sql, {"season": season}).mappings().all()
    df = pd.DataFrame([dict(r) for r in rows])

    if df.empty:
        print(f"  No Juiced+ data for {season}")
        return

    # League average OPS per bucket
    lg_sql = text("""
        WITH pitcher_primary AS (
            SELECT DISTINCT ON (pitcher_id, season)
                pitcher_id, season, pitch_type_code, velo_quintile
            FROM mlb.goose_scores
            WHERE season = :season
            ORDER BY pitcher_id, season, pitches_thrown DESC
        )
        SELECT
            pp.pitch_type_code,
            pp.velo_quintile,
            SUM(CASE WHEN ab.event_type IN ('single','double','triple','home_run')
                THEN 1 ELSE 0 END)::float /
                NULLIF(SUM(CASE WHEN ab.event_type IN ('single','double','triple',
                    'home_run','field_out','force_out',
                    'grounded_into_double_play','strikeout','fielders_choice')
                    THEN 1 ELSE 0 END), 0) as lg_avg,
            SUM(CASE
                WHEN ab.event_type = 'single' THEN 1
                WHEN ab.event_type = 'double' THEN 2
                WHEN ab.event_type = 'triple' THEN 3
                WHEN ab.event_type = 'home_run' THEN 4
                ELSE 0 END)::float /
                NULLIF(SUM(CASE WHEN ab.event_type IN ('single','double','triple',
                    'home_run','field_out','force_out',
                    'grounded_into_double_play','strikeout','fielders_choice')
                    THEN 1 ELSE 0 END), 0) as lg_slg,
            SUM(CASE WHEN ab.event_type IN ('single','double','triple',
                'home_run','walk','hit_by_pitch') THEN 1 ELSE 0 END)::float /
                NULLIF(COUNT(*), 0) as lg_obp
        FROM mlb.at_bats ab
        JOIN mlb.games g ON g.game_pk = ab.game_pk
        JOIN pitcher_primary pp ON pp.pitcher_id = ab.pitcher_id
        WHERE g.season = :season AND g.game_type = 'R'
        GROUP BY pp.pitch_type_code, pp.velo_quintile
    """)
    lg_rows = db.execute(lg_sql, {"season": season}).mappings().all()
    lg_map = {(r['pitch_type_code'], r['velo_quintile']): dict(r) for r in lg_rows}

    upsert = text("""
        INSERT INTO mlb.juiced_scores (
            batter_id, batter_name, season, pitch_type_code, velo_quintile,
            pa, ab, hits, total_bases, avg, obp, slg, ops, lg_ops, juiced_plus
        ) VALUES (
            :batter_id, :batter_name, :season, :pt, :vq,
            :pa, :ab, :hits, :tb, :avg, :obp, :slg, :ops, :lg_ops, :juiced_plus
        )
        ON CONFLICT (batter_id, season, pitch_type_code, velo_quintile) DO UPDATE SET
            pa = EXCLUDED.pa, ab = EXCLUDED.ab, hits = EXCLUDED.hits,
            total_bases = EXCLUDED.total_bases, avg = EXCLUDED.avg,
            obp = EXCLUDED.obp, slg = EXCLUDED.slg, ops = EXCLUDED.ops,
            lg_ops = EXCLUDED.lg_ops, juiced_plus = EXCLUDED.juiced_plus,
            computed_at = NOW()
    """)

    stored = 0
    for _, r in df.iterrows():
        real_ab = max(int(r['real_ab']), 1)
        pa = max(int(r['pa']), 1)
        hits = int(r['hits'])
        tb = int(r['total_bases'])
        avg = hits / real_ab
        slg = tb / real_ab
        obp = hits / pa
        ops = obp + slg

        lg = lg_map.get((r['pitch_type_code'], int(r['velo_quintile'])), {})
        lg_obp = lg.get('lg_obp') or 0.32
        lg_slg = lg.get('lg_slg') or 0.40
        lg_ops = (lg_obp + lg_slg) if lg else 0.72
        juiced_plus = 100 * (ops / max(lg_ops, 0.001))

        db.execute(upsert, {
            "batter_id": int(r['batter_id']),
            "batter_name": str(r['batter_name']),
            "season": season, "pt": r['pitch_type_code'],
            "vq": int(r['velo_quintile']),
            "pa": pa, "ab": real_ab, "hits": hits, "tb": tb,
            "avg": round(avg, 3), "obp": round(obp, 3),
            "slg": round(slg, 3), "ops": round(ops, 3),
            "lg_ops": round(lg_ops, 3),
            "juiced_plus": round(juiced_plus, 1),
        })
        stored += 1

    db.commit()
    print(f"  Stored {stored} Juiced+ scores for {season}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build Goose+ and Juiced+ scores")
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--skip-boundaries", action="store_true")
    parser.add_argument("--skip-buckets", action="store_true")
    parser.add_argument("--juiced-only", action="store_true")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        if not args.juiced_only:
            if not args.skip_boundaries:
                compute_boundaries(db)
            bounds = load_boundaries(db)
            if not args.skip_buckets:
                compute_bucket_outcomes(db, bounds)
            compute_goose_scores(db, bounds, args.season)
            compute_overall_goose(db, args.season)
        compute_juiced_scores(db, args.season)
    finally:
        db.close()
