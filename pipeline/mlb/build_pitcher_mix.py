"""
Build pitcher platoon mix profiles.
Computes pitch usage % and outcomes split by batter handedness (L/R).
Run after compute_bapv.py for any season.
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import argparse
import pandas as pd
from database import SessionLocal
from sqlalchemy import text


def build_pitcher_mix(season: int, db):
    print(f"\n=== Building Pitcher Mix Profiles — {season} ===")

    sql = text("""
        SELECT
            ab.pitcher_id,
            MAX(ab.pitcher_name) as pitcher_name,
            p.pitch_type_code,
            ab.bat_side,
            COUNT(*) as pitches,
            ROUND(AVG(CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END)::numeric, 4) as whiff_rate,
            ROUND(AVG(CASE WHEN p.call_code IN ('S','W','T','C') THEN 1.0 ELSE 0.0 END)::numeric, 4) as csw_rate,
            ROUND(AVG(p.start_speed)::numeric, 2) as avg_velo,
            ROUND(AVG(CASE WHEN p.launch_speed >= 95 AND p.call_code = 'X'
                THEN 1.0 ELSE 0.0 END)::numeric, 4) as hard_hit_rate
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE g.season = :season
        AND g.game_type = 'R'
        AND p.pitch_type_code IS NOT NULL
        AND ab.bat_side IN ('L', 'R')
        AND ab.pitcher_id IS NOT NULL
        GROUP BY ab.pitcher_id, p.pitch_type_code, ab.bat_side
        HAVING COUNT(*) >= 5
    """)

    df = pd.read_sql(sql, db.bind, params={"season": season})
    print(f"  Loaded {len(df)} pitcher-pitch-side rows")

    # Compute usage % within each pitcher-bat_side group
    df['total_pitches'] = df.groupby(['pitcher_id', 'bat_side'])['pitches'].transform('sum')
    df['usage_pct'] = (df['pitches'] / df['total_pitches']).round(4)

    # Upsert into pitcher_mix_profile
    upsert_sql = text("""
        INSERT INTO mlb.pitcher_mix_profile (
            pitcher_id, pitcher_name, season, pitch_type_code, bat_side,
            pitches, usage_pct, whiff_rate, csw_rate, avg_velo, hard_hit_rate
        ) VALUES (
            :pitcher_id, :pitcher_name, :season, :pitch_type_code, :bat_side,
            :pitches, :usage_pct, :whiff_rate, :csw_rate, :avg_velo, :hard_hit_rate
        )
        ON CONFLICT (pitcher_id, season, pitch_type_code, bat_side)
        DO UPDATE SET
            pitches = EXCLUDED.pitches,
            usage_pct = EXCLUDED.usage_pct,
            whiff_rate = EXCLUDED.whiff_rate,
            csw_rate = EXCLUDED.csw_rate,
            avg_velo = EXCLUDED.avg_velo,
            hard_hit_rate = EXCLUDED.hard_hit_rate
    """)

    stored = 0
    for _, r in df.iterrows():
        db.execute(upsert_sql, {
            "pitcher_id": int(r['pitcher_id']),
            "pitcher_name": r['pitcher_name'],
            "season": season,
            "pitch_type_code": r['pitch_type_code'],
            "bat_side": r['bat_side'],
            "pitches": int(r['pitches']),
            "usage_pct": float(r['usage_pct']),
            "whiff_rate": float(r['whiff_rate']) if pd.notna(r['whiff_rate']) else None,
            "csw_rate": float(r['csw_rate']) if pd.notna(r['csw_rate']) else None,
            "avg_velo": float(r['avg_velo']) if pd.notna(r['avg_velo']) else None,
            "hard_hit_rate": float(r['hard_hit_rate']) if pd.notna(r['hard_hit_rate']) else None,
        })
        stored += 1

    db.commit()
    print(f"  Stored {stored} mix profile rows")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, default=2025)
    args = parser.parse_args()
    db = SessionLocal()
    try:
        build_pitcher_mix(args.season, db)
    finally:
        db.close()
