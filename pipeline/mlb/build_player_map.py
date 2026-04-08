"""
Build MLB player ID crosswalk table.

Pulls player ID mappings from pybaseball and stores in mlb.player_id_map.
Maps MLBAM IDs (used by GUMBO) to Fangraphs IDs and BBRef IDs.

Run this once before the season, then as needed when new players appear.

Usage:
    PYTHONPATH=/app python3 /pipeline/mlb/build_player_map.py
    PYTHONPATH=/app python3 /pipeline/mlb/build_player_map.py --update-missing
"""

import sys
import os
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'api'))

from database import SessionLocal
from models.mlb import MLBPlayerIDMap, MLBAtBat
import pybaseball
from sqlalchemy import text


def build_full_map(db) -> int:
    """Pull the complete pybaseball player ID table and store it."""
    print("Fetching complete player ID table from pybaseball...")
    print("This may take 30-60 seconds on first run...")

    try:
        df = pybaseball.chadwick_register()
    except Exception as e:
        print(f"Error fetching player map: {e}")
        return 0

    print(f"Got {len(df)} players. Building map...")

    # Filter to players with valid MLBAM or Fangraphs ID
    df = df[
        (df["key_mlbam"].notna() & (df["key_mlbam"] > 0)) |
        (df["key_fangraphs"].notna() & (df["key_fangraphs"] > 0))
    ].copy()

    # Deduplicate on MLBAM ID — keep first occurrence
    df = df[df["key_mlbam"] > 0].drop_duplicates(subset=["key_mlbam"])

    print(f"  {len(df)} players with valid IDs")

    # Clear existing map
    db.query(MLBPlayerIDMap).delete()
    db.commit()

    rows = []
    for _, r in df.iterrows():
        mlbam = int(r["key_mlbam"]) if r.get("key_mlbam") and str(r["key_mlbam"]) != 'nan' else None
        fg = int(r["key_fangraphs"]) if r.get("key_fangraphs") and str(r["key_fangraphs"]) != 'nan' else None
        bbref = str(r["key_bbref"]) if r.get("key_bbref") and str(r["key_bbref"]) != 'nan' else None
        birth_year = int(r["birth_year"]) if r.get("birth_year") and str(r["birth_year"]) != 'nan' else None

        rows.append(MLBPlayerIDMap(
            mlbam_id=mlbam,
            fangraphs_id=fg,
            bbref_id=bbref,
            first_name=r.get("name_first"),
            last_name=r.get("name_last"),
            birth_year=birth_year,
        ))

        if len(rows) % 5000 == 0:
            db.bulk_save_objects(rows)
            db.commit()
            print(f"  Saved {len(rows)} rows...")
            rows = []

    if rows:
        db.bulk_save_objects(rows)
        db.commit()

    total = db.query(MLBPlayerIDMap).count()
    print(f"  Done — {total} players in map")
    return total


def update_missing(db) -> int:
    """
    Find GUMBO batter_ids not in the map and look them up individually.
    Useful for new players mid-season.
    """
    print("Finding GUMBO players missing from ID map...")

    missing = db.execute(text("""
        SELECT DISTINCT ab.batter_id, ab.batter_name
        FROM mlb.at_bats ab
        LEFT JOIN mlb.player_id_map m ON m.mlbam_id = ab.batter_id
        WHERE m.mlbam_id IS NULL
        AND ab.batter_id IS NOT NULL
        ORDER BY ab.batter_name
    """)).mappings().all()

    print(f"Found {len(missing)} players missing from map")

    added = 0
    for player in missing:
        name = player["batter_name"]
        # Parse name — GUMBO format is "First Last"
        parts = name.strip().split(" ", 1)
        if len(parts) < 2:
            continue
        first, last = parts[0], parts[1]

        try:
            results = pybaseball.playerid_lookup(last, first)
            if len(results) == 0:
                print(f"  Not found: {name}")
                continue

            # Take first result if multiple
            r = results.iloc[0]
            mlbam = int(r["key_mlbam"]) if str(r.get("key_mlbam", "nan")) != "nan" else None
            fg = int(r["key_fangraphs"]) if str(r.get("key_fangraphs", "nan")) != "nan" else None
            bbref = str(r["key_bbref"]) if str(r.get("key_bbref", "nan")) != "nan" else None

            # Check if MLBAM ID already exists (different name spelling)
            if mlbam:
                existing = db.query(MLBPlayerIDMap).filter(
                    MLBPlayerIDMap.mlbam_id == mlbam
                ).first()
                if existing:
                    continue

            mapping = MLBPlayerIDMap(
                mlbam_id=mlbam or player["batter_id"],
                fangraphs_id=fg,
                bbref_id=bbref,
                first_name=first,
                last_name=last,
            )
            db.add(mapping)
            db.commit()
            added += 1
            print(f"  Added: {name} (mlbam={mlbam}, fg={fg})")

        except Exception as e:
            print(f"  Error looking up {name}: {e}")
            continue

    print(f"Added {added} missing players")
    return added


def main():
    parser = argparse.ArgumentParser(description="Build MLB player ID crosswalk")
    parser.add_argument("--update-missing", action="store_true",
                        help="Only look up players missing from existing map")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        if args.update_missing:
            update_missing(db)
        else:
            build_full_map(db)
            print("\nRunning missing player check...")
            update_missing(db)
    finally:
        db.close()


if __name__ == "__main__":
    main()