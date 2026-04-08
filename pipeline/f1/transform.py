"""
F1 Transform

Reads raw JSON from f1.raw_events and populates:
  f1.races
  f1.results
  f1.qualifying
  f1.lap_times
  f1.pit_stops
  f1.drivers
  f1.constructors

Usage:
    PYTHONPATH=/app python3 /pipeline/f1/transform.py --season 2026
    PYTHONPATH=/app python3 /pipeline/f1/transform.py --season 2026 --round 1
    PYTHONPATH=/app python3 /pipeline/f1/transform.py --all
"""

import sys
import os
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'api'))

from database import SessionLocal
from models.f1 import (
    F1RawEvent, F1Race, F1Result, F1LapTime,
    F1PitStop, F1Qualifying, F1Driver, F1Constructor
)


def time_to_millis(time_str: str) -> int:
    """Convert '1:23:06.801' or '1:22.670' to milliseconds."""
    if not time_str:
        return None
    try:
        parts = time_str.split(":")
        if len(parts) == 3:
            h, m, s = parts
            return int((int(h) * 3600 + int(m) * 60 + float(s)) * 1000)
        elif len(parts) == 2:
            m, s = parts
            return int((int(m) * 60 + float(s)) * 1000)
        else:
            return int(float(parts[0]) * 1000)
    except Exception:
        return None


def upsert_driver(driver_data: dict, db, seen: set) -> None:
    """Insert or update a driver in f1.drivers."""
    driver_id = driver_data.get("driverId")
    if not driver_id or driver_id in seen:
        return
    seen.add(driver_id)
    existing = db.query(F1Driver).filter(F1Driver.driver_id == driver_id).first()
    if existing:
        return
    db.add(F1Driver(
        driver_id=driver_id,
        permanent_number=int(driver_data["permanentNumber"]) if driver_data.get("permanentNumber") else None,
        code=driver_data.get("code"),
        given_name=driver_data.get("givenName"),
        family_name=driver_data.get("familyName"),
        date_of_birth=driver_data.get("dateOfBirth"),
        nationality=driver_data.get("nationality"),
        url=driver_data.get("url"),
    ))


def upsert_constructor(constructor_data: dict, db, seen: set) -> None:
    """Insert or update a constructor in f1.constructors."""
    constructor_id = constructor_data.get("constructorId")
    if not constructor_id or constructor_id in seen:
        return
    seen.add(constructor_id)
    existing = db.query(F1Constructor).filter(
        F1Constructor.constructor_id == constructor_id
    ).first()
    if existing:
        return
    db.add(F1Constructor(
        constructor_id=constructor_id,
        name=constructor_data.get("name"),
        nationality=constructor_data.get("nationality"),
        url=constructor_data.get("url"),
    ))

def transform_results(raw: F1RawEvent, db) -> int:
    """Transform results raw event into f1.races, f1.results, f1.drivers, f1.constructors."""
    results = raw.data.get("results", [])
    if not results:
        return 0

    # Upsert race metadata from first result
    existing_race = db.query(F1Race).filter(
        F1Race.season == raw.season,
        F1Race.round == raw.round,
    ).first()

    if not existing_race:
        # Get race info from raw event
        race = F1Race(
            season=raw.season,
            round=raw.round,
            circuit_id=raw.circuit_id,
        )
        db.add(race)
        db.flush()

    # Delete existing results for this race
    db.query(F1Result).filter(
        F1Result.season == raw.season,
        F1Result.round == raw.round,
    ).delete()
    db.commit()

    rows = []
    seen_drivers, seen_constructors = set(), set()
    for r in results:
        driver = r.get("Driver", {})
        constructor = r.get("Constructor", {})
        time_data = r.get("Time", {})
        fastest = r.get("FastestLap", {})

        upsert_driver(driver, db, seen_drivers)
        upsert_constructor(constructor, db, seen_constructors)

        rows.append(F1Result(
            season=raw.season,
            round=raw.round,
            driver_id=driver.get("driverId"),
            constructor_id=constructor.get("constructorId"),
            grid=int(r["grid"]) if r.get("grid") else None,
            position=int(r["position"]) if r.get("position") else None,
            position_text=r.get("positionText"),
            position_order=int(r["positionOrder"]) if r.get("positionOrder") else None,
            points=float(r["points"]) if r.get("points") else None,
            laps=int(r["laps"]) if r.get("laps") else None,
            status=r.get("status"),
            time_millis=int(time_data["millis"]) if time_data.get("millis") else None,
            time_display=time_data.get("time"),
            fastest_lap_rank=int(fastest["rank"]) if fastest.get("rank") else None,
            fastest_lap_time=fastest.get("Time", {}).get("time"),
            fastest_lap_speed=float(fastest["AverageSpeed"]["speed"])
                if fastest.get("AverageSpeed") else None,
        ))

    db.bulk_save_objects(rows)
    db.commit()
    return len(rows)


def transform_qualifying(raw: F1RawEvent, db) -> int:
    """Transform qualifying raw event into f1.qualifying."""
    quali = raw.data.get("qualifying", [])
    if not quali:
        return 0

    db.query(F1Qualifying).filter(
        F1Qualifying.season == raw.season,
        F1Qualifying.round == raw.round,
    ).delete()
    db.commit()

    rows = []
    seen_drivers, seen_constructors = set(), set()
    for q in quali:
        driver = q.get("Driver", {})
        constructor = q.get("Constructor", {})
        upsert_driver(driver, db, seen_drivers)
        upsert_constructor(constructor, db, seen_constructors)

        rows.append(F1Qualifying(
            season=raw.season,
            round=raw.round,
            driver_id=driver.get("driverId"),
            constructor_id=constructor.get("constructorId"),
            position=int(q["position"]) if q.get("position") else None,
            q1_time=q.get("Q1"),
            q2_time=q.get("Q2"),
            q3_time=q.get("Q3"),
        ))

    db.bulk_save_objects(rows)
    db.commit()
    return len(rows)


def transform_lap_times(raw: F1RawEvent, db) -> int:
    """Transform lap times raw event into f1.lap_times."""
    laps = raw.data.get("laps", [])
    if not laps:
        return 0

    db.query(F1LapTime).filter(
        F1LapTime.season == raw.season,
        F1LapTime.round == raw.round,
    ).delete()
    db.commit()

    rows = []
    for lap in laps:
        rows.append(F1LapTime(
            season=raw.season,
            round=raw.round,
            driver_id=lap.get("driverId"),
            lap=lap.get("lap"),
            position=lap.get("position"),
            time_display=lap.get("time"),
            time_millis=time_to_millis(lap.get("time")),
        ))

    db.bulk_save_objects(rows)
    db.commit()
    return len(rows)


def transform_pit_stops(raw: F1RawEvent, db) -> int:
    """Transform pit stops raw event into f1.pit_stops."""
    stops = raw.data.get("pit_stops", [])
    if not stops:
        return 0

    db.query(F1PitStop).filter(
        F1PitStop.season == raw.season,
        F1PitStop.round == raw.round,
    ).delete()
    db.commit()

    rows = []
    for s in stops:
        rows.append(F1PitStop(
            season=raw.season,
            round=raw.round,
            driver_id=s.get("driverId"),
            stop=int(s["stop"]) if s.get("stop") else None,
            lap=int(s["lap"]) if s.get("lap") else None,
            time_of_day=s.get("time"),
            duration_display=s.get("duration"),
            duration_millis=time_to_millis(s.get("duration")),
        ))

    db.bulk_save_objects(rows)
    db.commit()
    return len(rows)


def transform_round(season: int, round: int, db):
    """Transform all endpoints for a single race round."""
    raws = db.query(F1RawEvent).filter(
        F1RawEvent.season == season,
        F1RawEvent.round == round,
        F1RawEvent.source == "jolpica",
    ).all()

    if not raws:
        print(f"  No raw data for {season} R{round}. Fetch it first.")
        return

    stored = {r.event_type: r for r in raws}
    print(f"  Transforming {season} R{round}...")

    if "results" in stored:
        n = transform_results(stored["results"], db)
        print(f"    ✓ results: {n} rows")

    if "qualifying" in stored:
        n = transform_qualifying(stored["qualifying"], db)
        print(f"    ✓ qualifying: {n} rows")

    if "lap_times" in stored:
        n = transform_lap_times(stored["lap_times"], db)
        print(f"    ✓ lap_times: {n} rows")

    if "pit_stops" in stored:
        n = transform_pit_stops(stored["pit_stops"], db)
        print(f"    ✓ pit_stops: {n} rows")


def main():
    parser = argparse.ArgumentParser(description="Transform F1 raw data")
    parser.add_argument("--season", type=int, nargs="+", default=None)
    parser.add_argument("--round", type=int, default=None)
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        if args.all:
            raws = db.query(F1RawEvent).filter(
                F1RawEvent.source == "jolpica",
                F1RawEvent.event_type == "results",
            ).order_by(F1RawEvent.season, F1RawEvent.round).all()
            print(f"Transforming {len(raws)} rounds...")
            for raw in raws:
                transform_round(raw.season, raw.round, db)
        elif args.season:
            for season in args.season:
                rounds = db.query(F1RawEvent).filter(
                    F1RawEvent.season == season,
                    F1RawEvent.source == "jolpica",
                    F1RawEvent.event_type == "results",
                ).order_by(F1RawEvent.round).all()
                print(f"\n=== {season} F1 Season ({len(rounds)} rounds) ===")
                for raw in rounds:
                    if args.round and raw.round != args.round:
                        continue
                    transform_round(season, raw.round, db)
        print("\nDone.")
    finally:
        db.close()


if __name__ == "__main__":
    main()