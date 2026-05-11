"""
Goose+ 3 — Unified pitcher quality model.

Architecture:
    Stuff Score (physics + location + tunnel) = Bayesian prior
    Goose+ 2 (observed outcomes vs league baselines) = likelihood

    Goose+ 3 = stuff_weight * Stuff + outcomes_weight * Goose2

Blending weights by sample size:
    outcomes_weight = min(1.0, pitches / 300) * 0.65
    stuff_weight = 1.0 - outcomes_weight

At 0 pitches:     100% Stuff Score (pure physics)
At 150 pitches:   67% Stuff / 33% Goose+2
At 300 pitches:   54% Stuff / 46% Goose+2
At 600 pitches:   43% Stuff / 57% Goose+2
At 1200+ pitches: 35% Stuff / 65% Goose+2 (outcomes eventually dominate)

All scores indexed to 100 = league average.
Regression to mean applied at final composite level.
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import argparse
import pandas as pd
import numpy as np
from database import SessionLocal
from sqlalchemy import text

REGRESSION_K = 200
MAX_OUTCOMES_WEIGHT = 0.65


def get_outcomes_weight(pitches: int) -> float:
    raw = min(1.0, pitches / 300)
    return round(raw * MAX_OUTCOMES_WEIGHT, 3)


def regress_to_mean(score: float, n: int, k: int = REGRESSION_K) -> float:
    weight = n / (n + k)
    return round(100 + weight * (score - 100), 1)


def build_goose3(score_season: int, db):
    print(f"\n=== Building Goose+ 3 — {score_season} ===")

    stuff_sql = text("""
        SELECT pitcher_id, pitch_type_code, stuff_score, pitches
        FROM mlb.stuff_scores
        WHERE season = :season AND pitches >= 15
    """)
    stuff_rows = db.execute(stuff_sql, {"season": score_season}).mappings().all()
    stuff_map = {}
    for r in stuff_rows:
        key = (int(r['pitcher_id']), r['pitch_type_code'])
        stuff_map[key] = {
            'stuff_score': float(r['stuff_score'] or 100),
            'pitches': int(r['pitches']),
        }
    print(f"  Loaded {len(stuff_map)} Stuff Score entries")

    g2_sql = text("""
        SELECT pitcher_id, pitcher_name, pitch_type_code, bat_side,
               pitch_hand, goose2_plus, pitches
        FROM mlb.goose2_scores
        WHERE season = :season AND pitches >= 10
    """)
    g2_rows = db.execute(g2_sql, {"season": score_season}).mappings().all()
    g2_map = {}
    for r in g2_rows:
        key = (int(r['pitcher_id']), r['pitch_type_code'], r['bat_side'])
        g2_map[key] = {
            'goose2_plus': float(r['goose2_plus'] or 100),
            'pitcher_name': r['pitcher_name'],
            'pitch_hand': r['pitch_hand'],
            'pitches': int(r['pitches']),
        }
    print(f"  Loaded {len(g2_map)} Goose+2 entries")

    all_keys = set(g2_map.keys())

    upsert_sql = text("""
        INSERT INTO mlb.goose3_scores (
            pitcher_id, pitcher_name, season, pitch_type_code,
            pitch_hand, bat_side,
            stuff_score, goose2_score,
            stuff_weight, outcomes_weight,
            goose3_plus, stuff_outcomes_gap, pitches
        ) VALUES (
            :pitcher_id, :pitcher_name, :season, :pitch_type_code,
            :pitch_hand, :bat_side,
            :stuff_score, :goose2_score,
            :stuff_weight, :outcomes_weight,
            :goose3_plus, :stuff_outcomes_gap, :pitches
        )
        ON CONFLICT (pitcher_id, season, pitch_type_code, bat_side)
        DO UPDATE SET
            stuff_score = EXCLUDED.stuff_score,
            goose2_score = EXCLUDED.goose2_score,
            stuff_weight = EXCLUDED.stuff_weight,
            outcomes_weight = EXCLUDED.outcomes_weight,
            goose3_plus = EXCLUDED.goose3_plus,
            stuff_outcomes_gap = EXCLUDED.stuff_outcomes_gap,
            pitches = EXCLUDED.pitches,
            computed_at = NOW()
    """)

    pitcher_totals = {}
    stored = 0

    for (pid, pt, bat_side) in all_keys:
        g2 = g2_map[(pid, pt, bat_side)]
        stuff = stuff_map.get((pid, pt))

        pitches = g2['pitches']
        goose2_score = g2['goose2_plus']
        stuff_score = stuff['stuff_score'] if stuff else 100.0

        outcomes_w = get_outcomes_weight(pitches)
        stuff_w = 1.0 - outcomes_w

        goose3_raw = (stuff_score * stuff_w) + (goose2_score * outcomes_w)
        goose3 = regress_to_mean(goose3_raw, pitches)
        gap = round(goose2_score - stuff_score, 1)

        db.execute(upsert_sql, {
            "pitcher_id": pid,
            "pitcher_name": g2['pitcher_name'],
            "season": score_season,
            "pitch_type_code": pt,
            "pitch_hand": g2['pitch_hand'],
            "bat_side": bat_side,
            "stuff_score": round(stuff_score, 1),
            "goose2_score": round(goose2_score, 1),
            "stuff_weight": stuff_w,
            "outcomes_weight": outcomes_w,
            "goose3_plus": goose3,
            "stuff_outcomes_gap": gap,
            "pitches": pitches,
        })
        stored += 1

        key = pid
        if key not in pitcher_totals:
            pitcher_totals[key] = {
                'name': g2['pitcher_name'],
                'pitch_hand': g2['pitch_hand'],
                'scores': []
            }
        pitcher_totals[key]['scores'].append({
            'goose3': goose3,
            'stuff': stuff_score,
            'goose2': goose2_score,
            'pitches': pitches,
            'bat_side': bat_side,
        })

    db.commit()
    print(f"  Stored {stored} Goose+3 pitch scores")

    overall_upsert = text("""
        INSERT INTO mlb.goose3_overall (
            pitcher_id, pitcher_name, season, pitch_hand,
            goose3_plus, goose3_vs_lhh, goose3_vs_rhh,
            avg_stuff_score, avg_goose2_score,
            stuff_outcomes_gap, total_pitches
        ) VALUES (
            :pitcher_id, :pitcher_name, :season, :pitch_hand,
            :goose3_plus, :goose3_vs_lhh, :goose3_vs_rhh,
            :avg_stuff, :avg_goose2,
            :gap, :total_pitches
        )
        ON CONFLICT (pitcher_id, season) DO UPDATE SET
            goose3_plus = EXCLUDED.goose3_plus,
            goose3_vs_lhh = EXCLUDED.goose3_vs_lhh,
            goose3_vs_rhh = EXCLUDED.goose3_vs_rhh,
            avg_stuff_score = EXCLUDED.avg_stuff_score,
            avg_goose2_score = EXCLUDED.avg_goose2_score,
            stuff_outcomes_gap = EXCLUDED.stuff_outcomes_gap,
            total_pitches = EXCLUDED.total_pitches,
            computed_at = NOW()
    """)

    for pid, data in pitcher_totals.items():
        scores = data['scores']
        total_p = sum(s['pitches'] for s in scores)
        if total_p == 0:
            continue

        def weighted_avg(key):
            return sum(s[key] * s['pitches'] for s in scores) / total_p

        overall = weighted_avg('goose3')
        avg_stuff = weighted_avg('stuff')
        avg_goose2 = weighted_avg('goose2')
        gap = round(avg_goose2 - avg_stuff, 1)

        lhh = [s for s in scores if s['bat_side'] == 'L']
        rhh = [s for s in scores if s['bat_side'] == 'R']
        lhh_p = sum(s['pitches'] for s in lhh)
        rhh_p = sum(s['pitches'] for s in rhh)

        vs_lhh = sum(s['goose3'] * s['pitches'] for s in lhh) / max(lhh_p, 1) if lhh else None
        vs_rhh = sum(s['goose3'] * s['pitches'] for s in rhh) / max(rhh_p, 1) if rhh else None

        db.execute(overall_upsert, {
            "pitcher_id": int(pid),
            "pitcher_name": data['name'],
            "season": score_season,
            "pitch_hand": data['pitch_hand'],
            "goose3_plus": round(overall, 1),
            "goose3_vs_lhh": round(vs_lhh, 1) if vs_lhh else None,
            "goose3_vs_rhh": round(vs_rhh, 1) if vs_rhh else None,
            "avg_stuff": round(avg_stuff, 1),
            "avg_goose2": round(avg_goose2, 1),
            "gap": gap,
            "total_pitches": int(total_p),
        })

    db.commit()
    print(f"  Stored {len(pitcher_totals)} Goose+3 overall scores")


def validate_goose3(season: int, db):
    print(f"\n=== Validating Goose+ 3 ({season}) ===")

    sql = text("""
        SELECT
            g3.pitcher_id,
            g3.pitcher_name,
            g3.goose3_plus,
            g3.avg_stuff_score,
            g3.avg_goose2_score,
            g3.stuff_outcomes_gap,
            g3.total_pitches,
            ROUND(go1.goose_plus::numeric,1) as goose1_plus,
            ROUND(go2.goose2_plus::numeric,1) as goose2_plus,
            ROUND(so.stuff_score::numeric,1) as stuff_score,
            ROUND(SUM(bp.strikeouts)::numeric /
                NULLIF(SUM(bp.batters_faced),0)*100,2) as k_rate,
            ROUND(SUM(bp.home_runs)::numeric /
                NULLIF(SUM(bp.batters_faced),0)*100,2) as hr_rate,
            br.era_plus
        FROM mlb.goose3_overall g3
        LEFT JOIN mlb.goose_overall go1 ON go1.pitcher_id = g3.pitcher_id
            AND go1.season = g3.season AND go1.game_pk IS NULL
        LEFT JOIN mlb.goose2_overall go2 ON go2.pitcher_id = g3.pitcher_id
            AND go2.season = g3.season
        LEFT JOIN mlb.stuff_overall so ON so.pitcher_id = g3.pitcher_id
            AND so.season = g3.season
        LEFT JOIN mlb.boxscore_pitching bp ON bp.player_id = g3.pitcher_id
            AND bp.game_pk IN (
                SELECT game_pk FROM mlb.games
                WHERE season = :season AND game_type = 'R'
            )
        LEFT JOIN mlb.bbref_pitching br ON br.mlb_id = g3.pitcher_id
            AND br.year_id = :season
        WHERE g3.season = :season
        AND g3.total_pitches >= 200
        GROUP BY g3.pitcher_id, g3.pitcher_name, g3.goose3_plus,
                 g3.avg_stuff_score, g3.avg_goose2_score,
                 g3.stuff_outcomes_gap, g3.total_pitches,
                 go1.goose_plus, go2.goose2_plus, so.stuff_score, br.era_plus
        HAVING SUM(bp.batters_faced) >= 50
        ORDER BY g3.goose3_plus DESC
    """)

    rows = db.execute(sql, {"season": season}).mappings().all()
    df = pd.DataFrame([dict(r) for r in rows])

    if df.empty or len(df) < 10:
        print("  Insufficient data for validation")
        return

    models = {
        'Goose+ 3':    'goose3_plus',
        'Goose+ 1':    'goose1_plus',
        'Goose+ 2':    'goose2_plus',
        'Stuff Score': 'stuff_score',
    }
    outcomes = {
        'K Rate':  ('k_rate', '+'),
        'HR Rate': ('hr_rate', '-'),
        'ERA+':    ('era_plus', '+'),
    }

    print(f"\n  Correlation comparison (n={len(df)}):")
    header = f"  {'Outcome':<12}" + "".join(f"  {m:>12}" for m in models)
    print(header)
    print("  " + "-" * (12 + 14 * len(models)))

    for outcome_label, (outcome_col, expected) in outcomes.items():
        row_str = f"  {outcome_label:<12}"
        for model_label, model_col in models.items():
            valid = df[[model_col, outcome_col]].dropna()
            if len(valid) < 10:
                row_str += f"  {'n/a':>12}"
                continue
            corr = valid[model_col].corr(valid[outcome_col])
            actual = '+' if corr > 0 else '-'
            match = '✅' if actual == expected else '❌'
            row_str += f"  {corr:>+10.3f}{match}"
        print(row_str)

    print(f"\n  Biggest over-performers (Goose2 >> Stuff — likely regression):")
    over = df.nlargest(5, 'stuff_outcomes_gap')
    for _, r in over.iterrows():
        print(f"    {r['pitcher_name']:<25} "
              f"G3+:{r['goose3_plus']:>6.1f} "
              f"Stuff:{r['avg_stuff_score']:>6.1f} "
              f"G2:{r['avg_goose2_score']:>6.1f} "
              f"Gap:{r['stuff_outcomes_gap']:>+6.1f}")

    print(f"\n  Biggest under-performers (Stuff >> Goose2 — likely to improve):")
    under = df.nsmallest(5, 'stuff_outcomes_gap')
    for _, r in under.iterrows():
        print(f"    {r['pitcher_name']:<25} "
              f"G3+:{r['goose3_plus']:>6.1f} "
              f"Stuff:{r['avg_stuff_score']:>6.1f} "
              f"G2:{r['avg_goose2_score']:>6.1f} "
              f"Gap:{r['stuff_outcomes_gap']:>+6.1f}")

    print(f"\n  Top 15 Goose+ 3:")
    print(f"  {'Pitcher':<25} {'G3+':>6} {'Stuff':>6} "
          f"{'G2+':>6} {'G1+':>6} {'Gap':>6} {'K%':>6}")
    print(f"  {'-'*65}")
    for _, r in df.nlargest(15, 'goose3_plus').iterrows():
        g1 = f"{r['goose1_plus']:.1f}" if pd.notna(r.get('goose1_plus')) else '—'
        k = f"{r['k_rate']:.1f}" if pd.notna(r.get('k_rate')) else '—'
        print(f"  {r['pitcher_name']:<25} "
              f"{r['goose3_plus']:>6.1f} "
              f"{r['avg_stuff_score']:>6.1f} "
              f"{r['avg_goose2_score']:>6.1f} "
              f"{g1:>6} "
              f"{r['stuff_outcomes_gap']:>+6.1f} "
              f"{k:>6}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--score-season", type=int, default=2026)
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        if args.validate_only:
            validate_goose3(args.score_season, db)
        else:
            build_goose3(args.score_season, db)
            validate_goose3(args.score_season, db)
    finally:
        db.close()
