"""
Build pitcher count mix profiles.
For each pitcher, compute pitch usage % by count (balls-strikes) and bat side.
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import argparse
import pandas as pd
from database import SessionLocal
from sqlalchemy import text


def build_count_mix(season: int, db):
    print(f"\n=== Building Pitcher Count Mix — {season} ===")

    sql = text("""
        SELECT
            ab.pitcher_id,
            MAX(ab.pitcher_name) as pitcher_name,
            ab.bat_side,
            p.balls_before as balls,
            p.strikes_before as strikes,
            p.pitch_type_code,
            COUNT(*) as pitches,
            ROUND(AVG(CASE WHEN p.call_code IN ('S','W','T')
                THEN 1.0 ELSE 0.0 END)::numeric, 4) as whiff_rate,
            ROUND(AVG(CASE WHEN p.call_code IN ('S','W','T','C')
                THEN 1.0 ELSE 0.0 END)::numeric, 4) as csw_rate,
            ROUND(AVG(CASE WHEN p.call_code = 'C'
                THEN 1.0 ELSE 0.0 END)::numeric, 4) as called_strike_rate,
            ROUND(AVG(CASE WHEN p.call_code IN ('B','*B')
                THEN 1.0 ELSE 0.0 END)::numeric, 4) as ball_rate,
            ROUND(AVG(p.start_speed)::numeric, 2) as avg_velo
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE g.season = :season
        AND g.game_type = 'R'
        AND p.balls_before IS NOT NULL
        AND p.strikes_before IS NOT NULL
        AND p.pitch_type_code IS NOT NULL
        AND ab.bat_side IN ('L', 'R')
        GROUP BY ab.pitcher_id, ab.bat_side,
                 p.balls_before, p.strikes_before, p.pitch_type_code
        HAVING COUNT(*) >= 3
    """)

    rows = db.execute(sql, {"season": season}).mappings().all()
    df = pd.DataFrame([dict(r) for r in rows])
    print(f"  Loaded {len(df):,} pitcher-count-pitch rows")

    if df.empty:
        print("  No data found")
        return

    df['total_in_count'] = df.groupby(
        ['pitcher_id', 'bat_side', 'balls', 'strikes']
    )['pitches'].transform('sum')
    df['usage_pct'] = (df['pitches'] / df['total_in_count']).round(4)

    upsert_sql = text("""
        INSERT INTO mlb.pitcher_count_mix (
            pitcher_id, pitcher_name, season, bat_side,
            balls, strikes, pitch_type_code,
            pitches, usage_pct, whiff_rate, csw_rate,
            called_strike_rate, ball_rate, avg_velo
        ) VALUES (
            :pitcher_id, :pitcher_name, :season, :bat_side,
            :balls, :strikes, :pitch_type_code,
            :pitches, :usage_pct, :whiff_rate, :csw_rate,
            :called_strike_rate, :ball_rate, :avg_velo
        )
        ON CONFLICT (pitcher_id, season, bat_side, balls, strikes, pitch_type_code)
        DO UPDATE SET
            pitches = EXCLUDED.pitches,
            usage_pct = EXCLUDED.usage_pct,
            whiff_rate = EXCLUDED.whiff_rate,
            csw_rate = EXCLUDED.csw_rate,
            called_strike_rate = EXCLUDED.called_strike_rate,
            ball_rate = EXCLUDED.ball_rate,
            avg_velo = EXCLUDED.avg_velo,
            computed_at = NOW()
    """)

    stored = 0
    for _, r in df.iterrows():
        db.execute(upsert_sql, {
            "pitcher_id": int(r['pitcher_id']),
            "pitcher_name": str(r['pitcher_name']),
            "season": season,
            "bat_side": str(r['bat_side']),
            "balls": int(r['balls']),
            "strikes": int(r['strikes']),
            "pitch_type_code": str(r['pitch_type_code']),
            "pitches": int(r['pitches']),
            "usage_pct": float(r['usage_pct']),
            "whiff_rate": float(r['whiff_rate']) if pd.notna(r['whiff_rate']) else None,
            "csw_rate": float(r['csw_rate']) if pd.notna(r['csw_rate']) else None,
            "called_strike_rate": float(r['called_strike_rate']) if pd.notna(r['called_strike_rate']) else None,
            "ball_rate": float(r['ball_rate']) if pd.notna(r['ball_rate']) else None,
            "avg_velo": float(r['avg_velo']) if pd.notna(r['avg_velo']) else None,
        })
        stored += 1

    db.commit()
    print(f"  Stored {stored:,} count mix rows for {season}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, nargs='+', default=[2025, 2026])
    args = parser.parse_args()
    db = SessionLocal()
    try:
        for season in args.season:
            build_count_mix(season, db)
    finally:
        db.close()
