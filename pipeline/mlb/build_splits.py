"""
Contextual splits library.

Computes player performance across situational contexts,
compared to league average in the same context.

Splits computed:
1. Runners on base (empty/on_first/scoring_position/loaded)
2. Runners in scoring position (2B or 3B)
3. Time through order (1st/2nd/3rd+ time facing pitcher)
4. First pitch swinging (0-0 count swing rate + outcomes)
5. Pitcher first pitch strike %
6. By inning group (early 1-3 / middle 4-6 / late 7+)
7. Close game (within 2 runs)
8. Outs context (0/1/2 outs)
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import argparse
import pandas as pd
from database import SessionLocal
from sqlalchemy import text


def compute_batter_splits(batter_id: int, season: int, db) -> dict:
    """Compute all contextual splits for a batter."""

    base_sql = text("""
        SELECT
            ab.at_bat_index,
            ab.event,
            ab.on_first, ab.on_second, ab.on_third,
            ab.outs_before,
            ab.inning,
            ab.half_inning,
            ab.home_score,
            ab.away_score,
            ab.pitch_hand,
            CASE WHEN ab.event IN ('Single','Double','Triple','Home Run')
                THEN 1 ELSE 0 END as is_hit,
            CASE WHEN ab.event = 'Home Run' THEN 1 ELSE 0 END as is_hr,
            CASE WHEN ab.event IN ('Strikeout','Strikeout - DP')
                THEN 1 ELSE 0 END as is_k,
            CASE WHEN ab.event IN ('Walk','Intent Walk')
                THEN 1 ELSE 0 END as is_bb,
            CASE ab.event
                WHEN 'Single' THEN 1
                WHEN 'Double' THEN 2
                WHEN 'Triple' THEN 3
                WHEN 'Home Run' THEN 4
                ELSE 0 END as total_bases,
            ROW_NUMBER() OVER (
                PARTITION BY ab.game_pk, ab.pitcher_id, ab.batter_id
                ORDER BY ab.at_bat_index
            ) as times_faced
        FROM mlb.at_bats ab
        JOIN mlb.games g ON g.game_pk = ab.game_pk
        WHERE ab.batter_id = :bid
        AND g.season = :season
        AND g.game_type = 'R'
        AND ab.runners_reconstructed = TRUE
        AND ab.event NOT IN ('Runner Out','Field Error','Caught Stealing 2B',
                              'Caught Stealing 3B','Caught Stealing Home',
                              'Pickoff','Wild Pitch','Passed Ball')
    """)

    rows = db.execute(base_sql, {"bid": batter_id, "season": season}).mappings().all()
    if not rows:
        return {}

    df = pd.DataFrame([dict(r) for r in rows])

    def split_stats(subset):
        n = len(subset)
        if n == 0:
            return None
        ab = n - int(subset['is_bb'].sum())
        hits = int(subset['is_hit'].sum())
        tb = int(subset['total_bases'].sum())
        hr = int(subset['is_hr'].sum())
        k = int(subset['is_k'].sum())
        bb = int(subset['is_bb'].sum())
        avg = round(hits / max(ab, 1), 3)
        slg = round(tb / max(ab, 1), 3)
        obp = round((hits + bb) / max(n, 1), 3)
        return {
            'pa': n,
            'avg': float(avg),
            'obp': float(obp),
            'slg': float(slg),
            'ops': round(obp + slg, 3),
            'hr_pct': round(hr / max(n, 1) * 100, 1),
            'k_pct': round(k / max(n, 1) * 100, 1),
            'bb_pct': round(bb / max(n, 1) * 100, 1),
        }

    splits = {}

    splits['bases_empty'] = split_stats(
        df[~df['on_first'] & ~df['on_second'] & ~df['on_third']])
    splits['runners_on'] = split_stats(
        df[df['on_first'] | df['on_second'] | df['on_third']])
    splits['risp'] = split_stats(df[df['on_second'] | df['on_third']])
    splits['bases_loaded'] = split_stats(
        df[df['on_first'] & df['on_second'] & df['on_third']])

    splits['first_time'] = split_stats(df[df['times_faced'] == 1])
    splits['second_time'] = split_stats(df[df['times_faced'] == 2])
    splits['third_time_plus'] = split_stats(df[df['times_faced'] >= 3])

    splits['early_innings'] = split_stats(df[df['inning'] <= 3])
    splits['middle_innings'] = split_stats(df[df['inning'].between(4, 6)])
    splits['late_innings'] = split_stats(df[df['inning'] >= 7])

    splits['close_game'] = split_stats(
        df[df.apply(lambda r: abs(
            (r['home_score'] if r['half_inning'] == 'bottom'
             else r['away_score']) -
            (r['away_score'] if r['half_inning'] == 'bottom'
             else r['home_score'])
        ) <= 2, axis=1)])

    splits['no_outs'] = split_stats(df[df['outs_before'] == 0])
    splits['one_out'] = split_stats(df[df['outs_before'] == 1])
    splits['two_outs'] = split_stats(df[df['outs_before'] == 2])

    return splits


def compute_pitcher_splits(pitcher_id: int, season: int, db) -> dict:
    """Compute all contextual splits for a pitcher."""

    base_sql = text("""
        SELECT
            ab.at_bat_index,
            ab.event,
            ab.on_first, ab.on_second, ab.on_third,
            ab.outs_before,
            ab.inning,
            ab.bat_side,
            CASE WHEN ab.event IN ('Single','Double','Triple','Home Run')
                THEN 1 ELSE 0 END as is_hit,
            CASE WHEN ab.event = 'Home Run' THEN 1 ELSE 0 END as is_hr,
            CASE WHEN ab.event IN ('Strikeout','Strikeout - DP')
                THEN 1 ELSE 0 END as is_k,
            CASE WHEN ab.event IN ('Walk','Intent Walk')
                THEN 1 ELSE 0 END as is_bb,
            CASE ab.event
                WHEN 'Single' THEN 1 WHEN 'Double' THEN 2
                WHEN 'Triple' THEN 3 WHEN 'Home Run' THEN 4
                ELSE 0 END as total_bases,
            ROW_NUMBER() OVER (
                PARTITION BY ab.game_pk, ab.pitcher_id, ab.batter_id
                ORDER BY ab.at_bat_index
            ) as times_faced,
            EXISTS (
                SELECT 1 FROM mlb.pitches p
                WHERE p.game_pk = ab.game_pk
                AND p.at_bat_index = ab.at_bat_index
                AND p.pitch_number = 1
                AND p.call_code IN ('C','S','W','T','F','X')
            ) as first_pitch_strike
        FROM mlb.at_bats ab
        JOIN mlb.games g ON g.game_pk = ab.game_pk
        WHERE ab.pitcher_id = :pid
        AND g.season = :season
        AND g.game_type = 'R'
        AND ab.runners_reconstructed = TRUE
        AND ab.event NOT IN ('Runner Out','Field Error','Caught Stealing 2B',
                              'Caught Stealing 3B','Caught Stealing Home',
                              'Pickoff','Wild Pitch','Passed Ball')
    """)

    rows = db.execute(base_sql, {"pid": pitcher_id, "season": season}).mappings().all()
    if not rows:
        return {}

    df = pd.DataFrame([dict(r) for r in rows])

    def split_stats(subset):
        n = len(subset)
        if n < 5:
            return None
        ab = n - int(subset['is_bb'].sum())
        hits = int(subset['is_hit'].sum())
        tb = int(subset['total_bases'].sum())
        hr = int(subset['is_hr'].sum())
        k = int(subset['is_k'].sum())
        bb = int(subset['is_bb'].sum())
        avg_against = round(hits / max(ab, 1), 3)
        slg_against = round(tb / max(ab, 1), 3)
        obp_against = round((hits + bb) / max(n, 1), 3)
        return {
            'pa': n,
            'avg_against': float(avg_against),
            'slg_against': float(slg_against),
            'ops_against': round(obp_against + slg_against, 3),
            'k_pct': round(k / max(n, 1) * 100, 1),
            'bb_pct': round(bb / max(n, 1) * 100, 1),
            'hr_pct': round(hr / max(n, 1) * 100, 1),
        }

    splits = {}

    splits['bases_empty'] = split_stats(
        df[~df['on_first'] & ~df['on_second'] & ~df['on_third']])
    splits['runners_on'] = split_stats(
        df[df['on_first'] | df['on_second'] | df['on_third']])
    splits['risp'] = split_stats(df[df['on_second'] | df['on_third']])
    splits['first_time'] = split_stats(df[df['times_faced'] == 1])
    splits['second_time'] = split_stats(df[df['times_faced'] == 2])
    splits['third_time_plus'] = split_stats(df[df['times_faced'] >= 3])
    splits['early_innings'] = split_stats(df[df['inning'] <= 3])
    splits['middle_innings'] = split_stats(df[df['inning'].between(4, 6)])
    splits['late_innings'] = split_stats(df[df['inning'] >= 7])
    splits['no_outs'] = split_stats(df[df['outs_before'] == 0])
    splits['two_outs'] = split_stats(df[df['outs_before'] == 2])

    fp_total = len(df)
    fp_strikes = int(df['first_pitch_strike'].sum()) if 'first_pitch_strike' in df else 0
    splits['first_pitch_strike_pct'] = round(fp_strikes / max(fp_total, 1) * 100, 1)

    return splits


def compute_league_splits(season: int, db) -> dict:
    """Compute league average splits for comparison."""
    print(f"  Computing league splits for {season}...")

    sql = text("""
        SELECT
            ROUND(AVG(CASE WHEN NOT on_first AND NOT on_second AND NOT on_third
                AND event IN ('Single','Double','Triple','Home Run')
                THEN 1.0 ELSE 0.0 END) /
                NULLIF(AVG(CASE WHEN NOT on_first AND NOT on_second AND NOT on_third
                THEN 1.0 ELSE 0.0 END), 0), 3) as avg_bases_empty,
            ROUND(AVG(CASE WHEN (on_second OR on_third)
                AND event IN ('Single','Double','Triple','Home Run')
                THEN 1.0 ELSE 0.0 END) /
                NULLIF(AVG(CASE WHEN on_second OR on_third
                THEN 1.0 ELSE 0.0 END), 0), 3) as avg_risp,
            ROUND(AVG(CASE WHEN event IN ('Strikeout','Strikeout - DP')
                THEN 1.0 ELSE 0.0 END) * 100, 1) as overall_k_pct,
            ROUND(AVG(CASE WHEN (on_second OR on_third)
                AND event IN ('Strikeout','Strikeout - DP')
                THEN 1.0 ELSE 0.0 END) /
                NULLIF(AVG(CASE WHEN on_second OR on_third
                THEN 1.0 ELSE 0.0 END), 0) * 100, 1) as k_pct_risp,
            COUNT(*) as total_pa
        FROM mlb.at_bats ab
        JOIN mlb.games g ON g.game_pk = ab.game_pk
        WHERE g.season = :season AND g.game_type = 'R'
        AND ab.runners_reconstructed = TRUE
        AND ab.event NOT IN ('Runner Out','Field Error','Caught Stealing 2B',
                              'Caught Stealing 3B','Caught Stealing Home')
    """)

    row = db.execute(sql, {"season": season}).mappings().first()
    return dict(row) if row else {}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--pitcher-id", type=int)
    parser.add_argument("--batter-id", type=int)
    args = parser.parse_args()

    db = SessionLocal()
    try:
        lg = compute_league_splits(args.season, db)
        print(f"League splits: {lg}")

        if args.pitcher_id:
            splits = compute_pitcher_splits(args.pitcher_id, args.season, db)
            import json
            print(f"\nPitcher {args.pitcher_id} splits:")
            print(json.dumps(splits, indent=2))

        if args.batter_id:
            splits = compute_batter_splits(args.batter_id, args.season, db)
            import json
            print(f"\nBatter {args.batter_id} splits:")
            print(json.dumps(splits, indent=2))
    finally:
        db.close()
