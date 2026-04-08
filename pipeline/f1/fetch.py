"""
F1 pipeline fetch script.

Fetches race data from Jolpica F1 API and stores in f1.raw_events.

Usage:
    # Fetch specific round
    PYTHONPATH=/app python3 /pipeline/f1/fetch.py --season 2026 --round 1

    # Fetch all rounds for a season
    PYTHONPATH=/app python3 /pipeline/f1/fetch.py --season 2026

    # Fetch multiple seasons
    PYTHONPATH=/app python3 /pipeline/f1/fetch.py --season 2023 2024 2025 2026

    # Dry run
    PYTHONPATH=/app python3 /pipeline/f1/fetch.py --season 2026 --dry-run
"""

import sys
import os
import argparse
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'api'))

from database import SessionLocal
from models.f1 import F1RawEvent
from client import F1Client

ENDPOINTS = ["results", "qualifying", "lap_times", "pit_stops"]


def upsert_raw(db, season: int, round: int, endpoint_type: str,
               data: any, circuit_id: str = None) -> str:
    existing = db.query(F1RawEvent).filter(
        F1RawEvent.season == season,
        F1RawEvent.round == round,
        F1RawEvent.source == "jolpica",
        F1RawEvent.event_type == endpoint_type,
    ).first()

    if existing:
        existing.data = data
        existing.updated_at = datetime.utcnow()
        db.commit()
        return "updated"
    else:
        raw = F1RawEvent(
            source="jolpica",
            event_type=endpoint_type,
            season=season,
            round=round,
            circuit_id=circuit_id,
            data=data if isinstance(data, dict) else {"records": data},
        )
        db.add(raw)
        db.commit()
        return "inserted"


def fetch_round(client: F1Client, db, season: int, round: int,
                circuit_id: str = None, race_name: str = None):
    """Fetch all endpoints for a single race round."""
    label = race_name or f"Round {round}"
    print(f"\n  [{season} R{round}] {label}")

    # Results
    results = client.get_results(season, round)
    if results:
        action = upsert_raw(db, season, round, "results",
                            {"results": results}, circuit_id)
        print(f"    ✓ results: {len(results)} drivers ({action})")
    else:
        print(f"    – results: not available")

    # Qualifying
    quali = client.get_qualifying(season, round)
    if quali:
        action = upsert_raw(db, season, round, "qualifying",
                            {"qualifying": quali}, circuit_id)
        print(f"    ✓ qualifying: {len(quali)} drivers ({action})")
    else:
        print(f"    – qualifying: not available")

    # Lap times
    laps = client.get_lap_times(season, round)
    if laps:
        action = upsert_raw(db, season, round, "lap_times",
                            {"laps": laps}, circuit_id)
        print(f"    ✓ lap_times: {len(laps)} records ({action})")
    else:
        print(f"    – lap_times: not available")

    # Pit stops
    stops = client.get_pit_stops(season, round)
    if stops:
        action = upsert_raw(db, season, round, "pit_stops",
                            {"pit_stops": stops}, circuit_id)
        print(f"    ✓ pit_stops: {len(stops)} records ({action})")
    else:
        print(f"    – pit_stops: not available")


def fetch_season(client: F1Client, db, season: int,
                 round_filter: int = None, dry_run: bool = False):
    """Fetch all rounds for a season."""
    print(f"\n=== {season} F1 Season ===")
    schedule = client.get_schedule(season)

    if not schedule:
        print(f"  No schedule found for {season}")
        return

    print(f"  Found {len(schedule)} rounds")

    if dry_run:
        for race in schedule:
            print(f"    R{race['round']}: {race['raceName']} ({race['date']})")
        return

    for race in schedule:
        round_num = int(race["round"])
        if round_filter and round_num != round_filter:
            continue

        circuit = race.get("Circuit", {})
        fetch_round(
            client=client,
            db=db,
            season=season,
            round=round_num,
            circuit_id=circuit.get("circuitId"),
            race_name=race.get("raceName"),
        )


def main():
    parser = argparse.ArgumentParser(description="Fetch F1 race data")
    parser.add_argument("--season", type=int, nargs="+", required=True)
    parser.add_argument("--round", type=int, default=None,
                        help="Specific round number. Omit for all rounds.")
    parser.add_argument("--dry-run", action="store_true",
                        help="List rounds without fetching")
    args = parser.parse_args()

    client = F1Client()
    db = SessionLocal()

    try:
        for season in args.season:
            fetch_season(
                client=client,
                db=db,
                season=season,
                round_filter=args.round,
                dry_run=args.dry_run,
            )
        print("\n=== Done ===")
    finally:
        db.close()


if __name__ == "__main__":
    main()