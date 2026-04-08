"""
NASCAR Transform

Reads raw JSON from nascar.raw_events and populates:
  nascar.races
  nascar.results
  nascar.laps
  nascar.pit_stops
  nascar.cautions
  nascar.lead_changes
  nascar.stage_results

Usage:
    PYTHONPATH=/app python3 /pipeline/nascar/transform.py --year 2026 --series 1
    PYTHONPATH=/app python3 /pipeline/nascar/transform.py --year 2024 2025 2026 --series 1
    PYTHONPATH=/app python3 /pipeline/nascar/transform.py --all
"""

import sys
import os
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'api'))

from database import SessionLocal
from models.nascar import (
    NASCARRawEvent, NASCARRace, NASCARResult, NASCARLap,
    NASCARPitStop, NASCARCaution, NASCARLeadChange, 
    NASCARStageResult, NASCARDriverStat
)

def safe_float(val):
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def safe_int(val):
    try:
        return int(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def transform_race(raw: NASCARRawEvent, db) -> NASCARRace:
    """Transform weekend_feed race metadata into nascar.races."""
    weekend_races = raw.data.get("weekend_race", [])
    if not weekend_races:
        return None

    wr = weekend_races[0]

    existing = db.query(NASCARRace).filter(
        NASCARRace.race_id == raw.race_id
    ).first()
    race = existing or NASCARRace()

    race.race_id = raw.race_id
    race.series_id = raw.series_id
    race.season = raw.season
    race.race_name = wr.get("race_name")
    race.track_name = wr.get("track_name")
    race.track_id = safe_int(wr.get("track_id"))
    race.race_date = wr.get("race_date", "")[:10] if wr.get("race_date") else None
    race.scheduled_laps = safe_int(wr.get("scheduled_laps"))
    race.actual_laps = safe_int(wr.get("actual_laps"))
    race.caution_count = safe_int(wr.get("number_of_cautions"))
    race.caution_laps = safe_int(wr.get("number_of_caution_laps"))
    race.lead_changes = safe_int(wr.get("number_of_lead_changes"))
    race.leaders_count = safe_int(wr.get("number_of_leaders"))
    race.total_miles = safe_float(wr.get("actual_distance"))

    if not existing:
        db.add(race)
    db.commit()
    db.refresh(race)
    return race


def transform_results(raw: NASCARRawEvent, db) -> int:
    """Transform weekend_feed results into nascar.results."""
    weekend_races = raw.data.get("weekend_race", [])
    if not weekend_races:
        return 0

    results = weekend_races[0].get("results", [])
    if not results:
        return 0

    db.query(NASCARResult).filter(NASCARResult.race_id == raw.race_id).delete()
    db.commit()

    rows = []
    for r in results:
        rows.append(NASCARResult(
            race_id=raw.race_id,
            season=raw.season,
            series_id=raw.series_id,
            driver_id=safe_int(r.get("driver_id")),
            driver_name=r.get("driver_fullname"),
            car_number=str(r.get("official_car_number", "")).strip(),
            team_name=r.get("team_name"),
            manufacturer=r.get("car_make"),
            start_position=safe_int(r.get("starting_position")),
            finish_position=safe_int(r.get("finishing_position")),
            laps_completed=safe_int(r.get("laps_completed")),
            laps_led=safe_int(r.get("laps_led")),
            finishing_status=r.get("finishing_status"),
            dnf=r.get("finishing_status", "").lower() not in ("running", ""),
            points=safe_int(r.get("points_earned")),
            playoff_points=safe_int(r.get("playoff_points_earned")),
            avg_position=safe_float(r.get("average_running_position")),
            avg_speed=safe_float(r.get("average_speed")),
            pit_stop_count=safe_int(r.get("num_pit_stops")),
        ))

    db.bulk_save_objects(rows)
    db.commit()
    return len(rows)


def transform_laps(raw: NASCARRawEvent, lap_raw: NASCARRawEvent, db) -> int:
    """Transform lap_times raw event into nascar.laps."""
    if not lap_raw:
        return 0

    data = lap_raw.data
    drivers = data.get("laps", [])
    if not drivers:
        return 0

    db.query(NASCARLap).filter(NASCARLap.race_id == raw.race_id).delete()
    db.commit()

    rows = []
    for driver in drivers:
        driver_id = safe_int(driver.get("NASCARDriverID"))
        driver_name = driver.get("FullName")
        car_number = str(driver.get("Number", "")).strip()
        manufacturer = driver.get("Manufacturer")

        for lap in driver.get("Laps", []):
            lap_num = safe_int(lap.get("Lap"))
            if lap_num is None:
                continue
            rows.append(NASCARLap(
                race_id=raw.race_id,
                season=raw.season,
                series_id=raw.series_id,
                driver_id=driver_id,
                driver_name=driver_name,
                car_number=car_number,
                lap_number=lap_num,
                position=safe_int(lap.get("RunningPos")),
                lap_time=safe_float(lap.get("LapTime")),
                lap_speed=safe_float(lap.get("LapSpeed")),
            ))

    db.bulk_save_objects(rows)
    db.commit()
    return len(rows)


def transform_pit_stops(raw: NASCARRawEvent, pit_raw: NASCARRawEvent, db) -> int:
    """Transform pit_stops raw event into nascar.pit_stops."""
    if not pit_raw:
        return 0

    data = pit_raw.data
    stops = data if isinstance(data, list) else data.get("pit_stops", [])
    if not stops:
        return 0

    db.query(NASCARPitStop).filter(NASCARPitStop.race_id == raw.race_id).delete()
    db.flush()

    rows = []
    for s in stops:
        rows.append(NASCARPitStop(
            race_id=raw.race_id,
            season=raw.season,
            series_id=raw.series_id,
            driver_id=None,
            driver_name=s.get("driver_name"),
            car_number=str(s.get("vehicle_number", "")).strip(),
            stop_number=safe_int(s.get("pit_in_rank")),
            lap=safe_int(s.get("lap_count")),
            duration_seconds=safe_float(s.get("total_duration")),
        ))

    # Insert in batches to avoid constraint issues
    for i in range(0, len(rows), 50):
        batch = rows[i:i+50]
        for row in batch:
            db.add(row)
        db.commit()

    return len(rows)


def transform_lead_changes(raw: NASCARRawEvent, db) -> int:
    """Transform race_leaders from weekend_feed into nascar.lead_changes."""
    weekend_races = raw.data.get("weekend_race", [])
    if not weekend_races:
        return 0

    leaders = weekend_races[0].get("race_leaders", [])
    if not leaders:
        return 0

    db.query(NASCARLeadChange).filter(
        NASCARLeadChange.race_id == raw.race_id
    ).delete()
    db.commit()

    rows = []
    for leader in leaders:
        rows.append(NASCARLeadChange(
            race_id=raw.race_id,
            season=raw.season,
            series_id=raw.series_id,
            lap=safe_int(leader.get("start_lap")),
            car_number=str(leader.get("car_number", "")).strip(),
        ))

    db.bulk_save_objects(rows)
    db.commit()
    return len(rows)

def transform_driver_stats(raw: NASCARRawEvent, stats_raw: NASCARRawEvent, db) -> int:
    """Transform driver_stats raw event into nascar.driver_stats."""
    if not stats_raw:
        return 0

    data = stats_raw.data
    # Structure: array with one element containing a 'drivers' key
    if isinstance(data, list) and len(data) > 0:
        drivers = data[0].get("drivers", [])
    elif isinstance(data, dict):
        drivers = data.get("drivers", [])
    else:
        return 0

    if not drivers:
        return 0

    db.query(NASCARDriverStat).filter(
        NASCARDriverStat.race_id == raw.race_id
    ).delete()
    db.commit()

    rows = []
    for d in drivers:
        rows.append(NASCARDriverStat(
            race_id=raw.race_id,
            season=raw.season,
            series_id=raw.series_id,
            driver_id=safe_int(d.get("driver_id")),
            avg_running_position=safe_float(d.get("avg_ps")),
            fastest_lap_number=safe_int(d.get("fast_laps")),
            quality_passes=safe_int(d.get("quality_passes")),
            green_flag_passes=safe_int(d.get("passes_gf")),
            green_flag_passed=safe_int(d.get("passed_gf")),
            quality_pass_differential=safe_int(d.get("passing_diff")),
            driver_rating=safe_float(d.get("rating")),
        ))

    db.bulk_save_objects(rows)
    db.commit()
    return len(rows)

def transform_race_id(year: int, series_id: int, race_id: int, db):
    """Transform all endpoints for a single race."""
    raws = db.query(NASCARRawEvent).filter(
        NASCARRawEvent.season == year,
        NASCARRawEvent.series_id == series_id,
        NASCARRawEvent.race_id == race_id,
    ).all()

    if not raws:
        print(f"  No raw data for {year} series={series_id} race={race_id}")
        return

    stored = {r.endpoint_type: r for r in raws}
    weekend_raw = stored.get("weekend_feed")

    if not weekend_raw:
        print(f"  No weekend_feed for race {race_id}, skipping")
        return

    race = transform_race(weekend_raw, db)
    race_name = race.race_name if race else f"race {race_id}"
    print(f"  [{year}] {race_name} (race_id={race_id})")

    n = transform_results(weekend_raw, db)
    print(f"    ✓ results: {n} rows")

    n = transform_laps(weekend_raw, stored.get("lap_times"), db)
    print(f"    ✓ laps: {n} rows")

    n = transform_pit_stops(weekend_raw, stored.get("pit_stops"), db)
    print(f"    ✓ pit_stops: {n} rows")

    n = transform_lead_changes(weekend_raw, db)
    print(f"    ✓ lead_changes: {n} rows")

    n = transform_driver_stats(weekend_raw, stored.get("driver_stats"), db)
    print(f"    ✓ driver_stats: {n} rows")


def main():
    parser = argparse.ArgumentParser(description="Transform NASCAR raw data")
    parser.add_argument("--year", type=int, nargs="+", default=None)
    parser.add_argument("--series", type=int, default=1)
    parser.add_argument("--race", type=int, default=None)
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        if args.all or not args.year:
            # Get all unique race keys from weekend_feed rows
            raws = db.query(NASCARRawEvent).filter(
                NASCARRawEvent.endpoint_type == "weekend_feed"
            ).order_by(NASCARRawEvent.season, NASCARRawEvent.race_id).all()
            print(f"Transforming {len(raws)} NASCAR races...")
            for raw in raws:
                transform_race_id(raw.season, raw.series_id, raw.race_id, db)
        else:
            for year in args.year:
                raws = db.query(NASCARRawEvent).filter(
                    NASCARRawEvent.season == year,
                    NASCARRawEvent.series_id == args.series,
                    NASCARRawEvent.endpoint_type == "weekend_feed",
                ).order_by(NASCARRawEvent.race_id).all()

                print(f"\n=== {year} NASCAR Cup Series ({len(raws)} races) ===")
                for raw in raws:
                    if args.race and raw.race_id != args.race:
                        continue
                    transform_race_id(year, args.series, raw.race_id, db)

        print("\nDone.")
    finally:
        db.close()


if __name__ == "__main__":
    main()