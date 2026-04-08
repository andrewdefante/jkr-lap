"""
Build batter tendency tables for BAPV pitch quality model.

Computes per-batter tendencies from pitch data:
  mlb.batter_pitch_type_tendencies  -- whiff/contact/hard hit by pitch type
  mlb.batter_zone_tendencies        -- swing/take/whiff by zone location
  mlb.linear_weights                -- wOBA linear weights by season

Usage:
    PYTHONPATH=/app python3 /pipeline/mlb/build_batter_tendencies.py --season 2025
    PYTHONPATH=/app python3 /pipeline/mlb/build_batter_tendencies.py --season 2023 2024 2025
"""

import sys
import os
import argparse
import pandas as pd
from sqlalchemy import text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'api'))
from database import SessionLocal
from models.mlb import (
    MLBBatterPitchTypeTendencies,
    MLBBatterZoneTendencies,
    MLBLinearWeights
)

MIN_PITCHES_TYPE = 50   # minimum pitches faced per pitch type
MIN_PITCHES_ZONE = 20   # minimum pitches in zone


def build_pitch_type_tendencies(season: int, db) -> int:
    """
    Compute per-batter per-pitch-type tendencies.
    Call codes:
      Swing: S, W, T, F, D, E, X
      Whiff: S, W, T
      Ball:  B, *B, H
      Called strike: C
      In play: X
    """
    print(f"  Building pitch type tendencies for {season}...")

    sql = text("""
        SELECT
            ab.batter_id,
            p.pitch_type_code,
            COUNT(*) as pitches_faced,

            -- Swing rate
            ROUND(AVG(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X')
                THEN 1.0 ELSE 0.0 END)::numeric, 4) as swing_rate,

            -- Whiff rate (whiffs / swings)
            ROUND(
                SUM(CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END) /
                NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X')
                    THEN 1.0 ELSE 0.0 END), 0)
            ::numeric, 4) as whiff_rate,

            -- Contact rate (contact / swings)
            ROUND(
                SUM(CASE WHEN p.call_code IN ('F','D','E','X') THEN 1.0 ELSE 0.0 END) /
                NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X')
                    THEN 1.0 ELSE 0.0 END), 0)
            ::numeric, 4) as contact_rate,

            -- Chase rate (swings on balls / total balls)
            ROUND(
                SUM(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X')
                    AND p.zone NOT BETWEEN 1 AND 9 THEN 1.0 ELSE 0.0 END) /
                NULLIF(SUM(CASE WHEN p.zone NOT BETWEEN 1 AND 9
                    THEN 1.0 ELSE 0.0 END), 0)
            ::numeric, 4) as chase_rate,

            -- CSW rate
            ROUND(AVG(CASE WHEN p.call_code IN ('S','W','T','C')
                THEN 1.0 ELSE 0.0 END)::numeric, 4) as csw_rate,

            -- In play rate
            ROUND(AVG(CASE WHEN p.call_code = 'X'
                THEN 1.0 ELSE 0.0 END)::numeric, 4) as in_play_rate,

            -- Hard hit rate (exit velo >= 95 mph)
            ROUND(
                SUM(CASE WHEN p.launch_speed >= 95 THEN 1.0 ELSE 0.0 END) /
                NULLIF(SUM(CASE WHEN p.is_in_play = true THEN 1.0 ELSE 0.0 END), 0)
            ::numeric, 4) as hard_hit_rate,

            -- Avg exit velo on contact
            ROUND(AVG(CASE WHEN p.is_in_play = true
                THEN p.launch_speed ELSE NULL END)::numeric, 2) as avg_exit_velo,

            -- Avg launch angle on contact
            ROUND(AVG(CASE WHEN p.is_in_play = true
                THEN p.launch_angle ELSE NULL END)::numeric, 2) as avg_launch_angle,

            -- Barrel rate (launch_speed >= 98 AND angle 26-30)
            ROUND(
                SUM(CASE WHEN p.launch_speed >= 98
                    AND p.launch_angle BETWEEN 26 AND 30
                    THEN 1.0 ELSE 0.0 END) /
                NULLIF(SUM(CASE WHEN p.is_in_play = true THEN 1.0 ELSE 0.0 END), 0)
            ::numeric, 4) as barrel_rate

        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE g.season = :season
        AND g.game_type = 'R'
        AND p.pitch_type_code IN ('FF','SI','SL','CH','FC','ST','CU','FS','KC')
        AND p.call_code IS NOT NULL
        AND ab.batter_id IS NOT NULL
        GROUP BY ab.batter_id, p.pitch_type_code
        HAVING COUNT(*) >= :min_pitches
    """)

    df = pd.read_sql(sql, db.bind, params={
        "season": season,
        "min_pitches": MIN_PITCHES_TYPE
    })
    print(f"    {len(df):,} batter-pitch type combinations")

    # Delete existing
    db.execute(text(
        "DELETE FROM mlb.batter_pitch_type_tendencies WHERE season = :s"
    ), {"s": season})
    db.commit()

    rows = []
    for _, r in df.iterrows():
        rows.append(MLBBatterPitchTypeTendencies(
            batter_id=int(r["batter_id"]),
            pitch_type_code=r["pitch_type_code"],
            season=season,
            pitches_faced=int(r["pitches_faced"]),
            swing_rate=float(r["swing_rate"]) if r["swing_rate"] else None,
            whiff_rate=float(r["whiff_rate"]) if r["whiff_rate"] else None,
            contact_rate=float(r["contact_rate"]) if r["contact_rate"] else None,
            chase_rate=float(r["chase_rate"]) if r["chase_rate"] else None,
            csw_rate=float(r["csw_rate"]) if r["csw_rate"] else None,
            in_play_rate=float(r["in_play_rate"]) if r["in_play_rate"] else None,
            hard_hit_rate=float(r["hard_hit_rate"]) if r["hard_hit_rate"] else None,
            avg_exit_velo=float(r["avg_exit_velo"]) if r["avg_exit_velo"] else None,
            avg_launch_angle=float(r["avg_launch_angle"]) if r["avg_launch_angle"] else None,
            barrel_rate=float(r["barrel_rate"]) if r["barrel_rate"] else None,
        ))

    db.bulk_save_objects(rows)
    db.commit()
    return len(rows)


def build_zone_tendencies(season: int, db) -> int:
    """Compute per-batter per-zone tendencies."""
    print(f"  Building zone tendencies for {season}...")

    sql = text("""
        SELECT
            ab.batter_id,
            p.zone,
            COUNT(*) as pitches_faced,

            ROUND(AVG(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X')
                THEN 1.0 ELSE 0.0 END)::numeric, 4) as swing_rate,

            ROUND(AVG(CASE WHEN p.call_code NOT IN ('S','W','T','F','D','E','X')
                THEN 1.0 ELSE 0.0 END)::numeric, 4) as take_rate,

            ROUND(
                SUM(CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END) /
                NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X')
                    THEN 1.0 ELSE 0.0 END), 0)
            ::numeric, 4) as whiff_rate,

            ROUND(
                SUM(CASE WHEN p.call_code IN ('F','D','E','X') THEN 1.0 ELSE 0.0 END) /
                NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X')
                    THEN 1.0 ELSE 0.0 END), 0)
            ::numeric, 4) as contact_rate,

            ROUND(AVG(CASE WHEN p.call_code = 'X'
                THEN 1.0 ELSE 0.0 END)::numeric, 4) as in_play_rate,

            ROUND(
                SUM(CASE WHEN p.launch_speed >= 95 THEN 1.0 ELSE 0.0 END) /
                NULLIF(SUM(CASE WHEN p.is_in_play = true THEN 1.0 ELSE 0.0 END), 0)
            ::numeric, 4) as hard_hit_rate,

            ROUND(AVG(CASE WHEN p.is_in_play = true
                THEN p.launch_speed ELSE NULL END)::numeric, 2) as avg_exit_velo

        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE g.season = :season
        AND g.game_type = 'R'
        AND p.zone BETWEEN 1 AND 14
        AND p.call_code IS NOT NULL
        AND ab.batter_id IS NOT NULL
        GROUP BY ab.batter_id, p.zone
        HAVING COUNT(*) >= :min_pitches
    """)

    df = pd.read_sql(sql, db.bind, params={
        "season": season,
        "min_pitches": MIN_PITCHES_ZONE
    })
    print(f"    {len(df):,} batter-zone combinations")

    db.execute(text(
        "DELETE FROM mlb.batter_zone_tendencies WHERE season = :s"
    ), {"s": season})
    db.commit()

    rows = []
    for _, r in df.iterrows():
        rows.append(MLBBatterZoneTendencies(
            batter_id=int(r["batter_id"]),
            zone=int(r["zone"]),
            season=season,
            pitches_faced=int(r["pitches_faced"]),
            swing_rate=float(r["swing_rate"]) if r["swing_rate"] else None,
            take_rate=float(r["take_rate"]) if r["take_rate"] else None,
            whiff_rate=float(r["whiff_rate"]) if r["whiff_rate"] else None,
            contact_rate=float(r["contact_rate"]) if r["contact_rate"] else None,
            in_play_rate=float(r["in_play_rate"]) if r["in_play_rate"] else None,
            hard_hit_rate=float(r["hard_hit_rate"]) if r["hard_hit_rate"] else None,
            avg_exit_velo=float(r["avg_exit_velo"]) if r["avg_exit_velo"] else None,
        ))

    db.bulk_save_objects(rows)
    db.commit()
    return len(rows)


def build_linear_weights(season: int, db) -> None:
    """
    Store standard wOBA linear weights.
    Using 2023-2025 era values from FanGraphs guts page.
    These are stable enough to use across seasons.
    """
    print(f"  Building linear weights for {season}...")

    # Standard wOBA weights (FanGraphs 2024 era)
    # These are runs above average per event
    weights = {
        2023: dict(weight_out=-0.098, weight_bb=0.690, weight_hbp=0.720,
                   weight_single=0.888, weight_double=1.271,
                   weight_triple=1.616, weight_hr=2.101,
                   woba_scale=1.157, league_woba=0.317),
        2024: dict(weight_out=-0.097, weight_bb=0.688, weight_hbp=0.718,
                   weight_single=0.885, weight_double=1.268,
                   weight_triple=1.612, weight_hr=2.098,
                   woba_scale=1.152, league_woba=0.315),
        2025: dict(weight_out=-0.097, weight_bb=0.688, weight_hbp=0.718,
                   weight_single=0.885, weight_double=1.268,
                   weight_triple=1.612, weight_hr=2.098,
                   woba_scale=1.152, league_woba=0.315),
    }

    w = weights.get(season, weights[2024])

    existing = db.query(MLBLinearWeights).filter(
        MLBLinearWeights.season == season
    ).first()

    if existing:
        for k, v in w.items():
            setattr(existing, k, v)
    else:
        db.add(MLBLinearWeights(season=season, **w))

    db.commit()
    print(f"    Linear weights stored for {season}")


def main():
    parser = argparse.ArgumentParser(description="Build batter tendency tables")
    parser.add_argument("--season", type=int, nargs="+", required=True)
    args = parser.parse_args()

    db = SessionLocal()
    try:
        for season in args.season:
            print(f"\n=== {season} Batter Tendencies ===")
            n_type = build_pitch_type_tendencies(season, db)
            print(f"  ✓ pitch type tendencies: {n_type:,} rows")
            n_zone = build_zone_tendencies(season, db)
            print(f"  ✓ zone tendencies: {n_zone:,} rows")
            build_linear_weights(season, db)
            print(f"  ✓ linear weights stored")
        print("\nDone.")
    finally:
        db.close()


if __name__ == "__main__":
    main()