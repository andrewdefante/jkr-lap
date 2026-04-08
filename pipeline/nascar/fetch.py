"""
NASCAR pipeline fetch script.

Usage:
    # Fetch a specific race
    python pipeline/nascar/fetch.py --year 2026 --series 1 --race 5596

    # Fetch all Cup Series races for a season
    python pipeline/nascar/fetch.py --year 2026 --series 1

    # Fetch all series for a season
    python pipeline/nascar/fetch.py --year 2026
"""

import sys
import os
import argparse
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'api'))

from database import SessionLocal
from models.nascar import NASCARRawEvent
from client import NASCARClient

SERIES_NAMES = {1: "Cup", 2: "Xfinity", 3: "Truck"}
ALL_SERIES = [1, 2, 3]
ALL_ENDPOINTS = [
    "weekend_feed", "lap_times", "pit_stops",
    "lap_notes", "driver_stats", "advanced_stats"
]


def get_race_ids_for_season(client: NASCARClient, year: int, series_id: int) -> list:
    """Pull schedule and return list of race dicts with race_id and race_name."""
    schedule = client.get_schedule(year)
    if not schedule:
        print(f"  Could not fetch schedule for {year}")
        return []

    races = []
    # Schedule is keyed by series e.g. "series_1", "series_2", "series_3"
    series_key = f"series_{series_id}"
    series_races = schedule.get(series_key, [])

    for race in series_races:
        race_id = race.get("race_id")
        race_name = race.get("race_name", f"Race {race_id}")
        if race_id:
            races.append({"race_id": int(race_id), "race_name": race_name})

    print(f"  Found {len(races)} races for {year} Series {series_id}")
    return races
    

def store_endpoint(
    db, client: NASCARClient,
    year: int, series_id: int, race_id: int, endpoint_type: str
) -> str:
    """Fetch one endpoint and upsert into nascar.raw_events. Returns action taken."""
    if endpoint_type == "weekend_feed":
        data = client.get_weekend_feed(year, series_id, race_id)
    elif endpoint_type == "lap_times":
        data = client.get_lap_times(year, series_id, race_id)
    elif endpoint_type == "pit_stops":
        data = client.get_pit_stops(year, series_id, race_id)
    elif endpoint_type == "lap_notes":
        data = client.get_lap_notes(year, series_id, race_id)
    elif endpoint_type == "driver_stats":
        data = client.get_driver_stats(year, series_id, race_id)
    elif endpoint_type == "advanced_stats":
        data = client.get_advanced_stats(series_id, race_id)
    else:
        return "unknown_endpoint"

    if data is None:
        return "unavailable"

    existing = db.query(NASCARRawEvent).filter(
        NASCARRawEvent.season == year,
        NASCARRawEvent.series_id == series_id,
        NASCARRawEvent.race_id == race_id,
        NASCARRawEvent.endpoint_type == endpoint_type,
    ).first()

    if existing:
        existing.data = data
        existing.updated_at = datetime.utcnow()
        db.commit()
        return "updated"
    else:
        raw = NASCARRawEvent(
            season=year, series_id=series_id, race_id=race_id,
            endpoint_type=endpoint_type, data=data,
        )
        db.add(raw)
        db.commit()
        return "inserted"


def fetch_race(db, client: NASCARClient, year: int, series_id: int, race: dict):
    """Fetch all endpoints for a single race."""
    race_id = race["race_id"]
    race_name = race["race_name"]
    print(f"\n  [{SERIES_NAMES[series_id]}] {race_name} (race_id={race_id})")

    for endpoint_type in ALL_ENDPOINTS:
        action = store_endpoint(db, client, year, series_id, race_id, endpoint_type)
        symbol = "✓" if action in ("inserted", "updated") else "–"
        print(f"    {symbol} {endpoint_type}: {action}")


def main():
    parser = argparse.ArgumentParser(description="Fetch NASCAR race data")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--series", type=int, choices=[1, 2, 3], default=None,
                        help="1=Cup, 2=Xfinity, 3=Truck. Omit for all series.")
    parser.add_argument("--race", type=int, default=None,
                        help="Specific race_id. Omit to fetch entire season.")
    args = parser.parse_args()

    series_list = [args.series] if args.series else ALL_SERIES
    client = NASCARClient()
    db = SessionLocal()

    try:
        for series_id in series_list:
            print(f"\n=== {args.year} NASCAR {SERIES_NAMES[series_id]} Series ===")

            if args.race:
                races = [{"race_id": args.race, "race_name": f"Race {args.race}"}]
            else:
                races = get_race_ids_for_season(client, args.year, series_id)

            for race in races:
                fetch_race(db, client, args.year, series_id, race)

        print("\n=== Done ===")

    finally:
        db.close()


if __name__ == "__main__":
    main()