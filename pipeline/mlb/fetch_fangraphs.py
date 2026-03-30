"""
Fangraphs + Baseball Reference fetch via pybaseball.

Pulls season-level batting and pitching stats and stores in:
  mlb.fangraphs_batting
  mlb.fangraphs_pitching

Usage:
    PYTHONPATH=/app python3 /pipeline/mlb/fetch_fangraphs.py --season 2026
    PYTHONPATH=/app python3 /pipeline/mlb/fetch_fangraphs.py --season 2023 2024 2025 2026
"""

import sys
import os
import argparse
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'api'))

from database import SessionLocal
from models.mlb import MLBFangraphsBatting, MLBFangraphsPitching

import pybaseball
pybaseball.cache.enable()


def safe_float(val):
    try:
        return float(val) if val is not None and str(val) not in ('nan', 'NaN', '') else None
    except (ValueError, TypeError):
        return None


def safe_int(val):
    try:
        return int(val) if val is not None and str(val) not in ('nan', 'NaN', '') else None
    except (ValueError, TypeError):
        return None


def fetch_batting(season: int, db) -> int:
    """Fetch Fangraphs batting stats for a season."""
    print(f"  Fetching Fangraphs batting stats for {season}...")
    try:
        df = pybaseball.batting_stats(season, qual=10)
    except Exception as e:
        print(f"  Error fetching batting stats: {e}")
        return 0

    print(f"  Got {len(df)} batters. Columns: {list(df.columns[:20])}")

    # Delete existing rows for this season
    db.query(MLBFangraphsBatting).filter(
        MLBFangraphsBatting.season == season
    ).delete()
    db.commit()

    rows = []
    for _, r in df.iterrows():
        rows.append(MLBFangraphsBatting(
            season=season,
            player_name=r.get("Name"),
            team=r.get("Team"),
            mlbam_id=safe_int(r.get("IDfg") or r.get("mlbamID")),
            fg_id=str(r.get("IDfg", "")) if r.get("IDfg") else None,

            games=safe_int(r.get("G")),
            pa=safe_int(r.get("PA")),
            ab=safe_int(r.get("AB")),

            avg=safe_float(r.get("AVG")),
            obp=safe_float(r.get("OBP")),
            slg=safe_float(r.get("SLG")),
            ops=safe_float(r.get("OPS")),

            woba=safe_float(r.get("wOBA")),
            wrc_plus=safe_float(r.get("wRC+")),
            ops_plus=safe_float(r.get("OPS+")),
            off=safe_float(r.get("Off")),
            war=safe_float(r.get("WAR")),

            iso=safe_float(r.get("ISO")),
            hr=safe_int(r.get("HR")),
            barrel_pct=safe_float(r.get("Barrel%") or r.get("Barrels%")),
            hard_hit_pct=safe_float(r.get("HardHit%")),
            avg_exit_velo=safe_float(r.get("EV")),

            xba=safe_float(r.get("xBA")),
            xslg=safe_float(r.get("xSLG")),
            xwobacon=safe_float(r.get("xwOBA")),

            whiff_pct=safe_float(r.get("SwStr%")),

            o_swing_pct=safe_float(r.get("O-Swing%")),
            z_swing_pct=safe_float(r.get("Z-Swing%")),
            swing_pct=safe_float(r.get("Swing%")),
            o_contact_pct=safe_float(r.get("O-Contact%")),
            z_contact_pct=safe_float(r.get("Z-Contact%")),
            contact_pct=safe_float(r.get("Contact%")),
            zone_pct=safe_float(r.get("Zone%")),
            swstr_pct=safe_float(r.get("SwStr%")),

            sprint_speed=safe_float(r.get("Sprint Speed") or r.get("Spd")),
        ))

    db.bulk_save_objects(rows)
    db.commit()
    return len(rows)


def fetch_pitching(season: int, db) -> int:
    """Fetch Fangraphs pitching stats for a season."""
    print(f"  Fetching Fangraphs pitching stats for {season}...")
    try:
        df = pybaseball.pitching_stats(season, qual=5)
    except Exception as e:
        print(f"  Error fetching pitching stats: {e}")
        return 0

    print(f"  Got {len(df)} pitchers.")

    db.query(MLBFangraphsPitching).filter(
        MLBFangraphsPitching.season == season
    ).delete()
    db.commit()

    rows = []
    for _, r in df.iterrows():
        rows.append(MLBFangraphsPitching(
            season=season,
            player_name=r.get("Name"),
            team=r.get("Team"),
            mlbam_id=safe_int(r.get("IDfg") or r.get("mlbamID")),
            fg_id=str(r.get("IDfg", "")) if r.get("IDfg") else None,

            games=safe_int(r.get("G")),
            games_started=safe_int(r.get("GS")),
            ip=safe_float(r.get("IP")),

            era=safe_float(r.get("ERA")),
            whip=safe_float(r.get("WHIP")),
            wins=safe_int(r.get("W")),
            losses=safe_int(r.get("L")),
            saves=safe_int(r.get("SV")),

            era_plus=safe_float(r.get("ERA+")),
            era_minus=safe_float(r.get("ERA-")),
            fip=safe_float(r.get("FIP")),
            fip_minus=safe_float(r.get("FIP-")),
            xfip=safe_float(r.get("xFIP")),
            xfip_minus=safe_float(r.get("xFIP-")),
            siera=safe_float(r.get("SIERA")),
            war=safe_float(r.get("WAR")),

            k_pct=safe_float(r.get("K%")),
            bb_pct=safe_float(r.get("BB%")),
            k_minus_bb=safe_float(r.get("K-BB%")),
            hr_per_9=safe_float(r.get("HR/9") or r.get("HR9")),
            avg_fastball_velo=safe_float(r.get("FBv") or r.get("vFB")),

            o_swing_pct=safe_float(r.get("O-Swing%")),
            z_swing_pct=safe_float(r.get("Z-Swing%")),
            swing_pct=safe_float(r.get("Swing%")),
            o_contact_pct=safe_float(r.get("O-Contact%")),
            z_contact_pct=safe_float(r.get("Z-Contact%")),
            contact_pct=safe_float(r.get("Contact%")),
            zone_pct=safe_float(r.get("Zone%")),
            whiff_pct=safe_float(r.get("Whiff%")),
            swstr_pct=safe_float(r.get("SwStr%")),

            barrel_pct=safe_float(r.get("Barrel%") or r.get("Barrels%")),
            hard_hit_pct=safe_float(r.get("HardHit%")),
            avg_exit_velo=safe_float(r.get("AvgEV") or r.get("EV")),
            stuff_plus=safe_float(r.get("Stuff+")),
        ))

    db.bulk_save_objects(rows)
    db.commit()
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description="Fetch Fangraphs stats via pybaseball")
    parser.add_argument("--season", type=int, nargs="+", required=True)
    args = parser.parse_args()

    db = SessionLocal()
    try:
        for season in args.season:
            print(f"\n=== {season} Fangraphs Stats ===")
            n_bat = fetch_batting(season, db)
            print(f"  ✓ batting: {n_bat} rows")
            n_pitch = fetch_pitching(season, db)
            print(f"  ✓ pitching: {n_pitch} rows")
        print("\nDone.")
    finally:
        db.close()


if __name__ == "__main__":
    main()