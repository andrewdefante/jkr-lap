"""
NASCAR Live Race Poller

Polls the NASCAR live feed every 30 seconds during a race,
stores snapshots in nascar.live_snapshots, and computes
projected finishes based on current performance + historical data.

Usage:
    PYTHONPATH=/app python3 /pipeline/nascar/live_poller.py --race-id 5603 --season 2026
    
    # Custom poll interval
    PYTHONPATH=/app python3 /pipeline/nascar/live_poller.py --race-id 5603 --season 2026 --interval 30
"""

import sys
import os
import argparse
import time
import requests
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'api'))

from database import SessionLocal
from models.nascar import NASCARLiveSnapshot

LIVE_BASE = "https://cf.nascar.com/cacher/live"


def fetch_live_feed(series_id: int, race_id: int) -> dict:
    url = f"{LIVE_BASE}/series_{series_id}/{race_id}/live-feed.json"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"  [poller] fetch error: {e}")
        return None


def parse_snapshot(data: dict, race_id: int, season: int, series_id: int) -> list:
    """Parse live feed JSON into a list of NASCARLiveSnapshot rows."""
    if not data:
        return []

    vehicles = data.get("vehicles", [])
    current_lap = data.get("lap_number", 0)
    total_laps = data.get("number_of_race_laps", 0)
    now = datetime.now(timezone.utc)

    rows = []
    for v in vehicles:
        driver = v.get("driver", {})

        # Calculate laps since last pit stop
        pit_stop_list = v.get("pit_stops", [])
        last_pit_lap = None
        if isinstance(pit_stop_list, list) and pit_stop_list:
            completed = [p for p in pit_stop_list
                         if p.get("pit_in_lap_count", 0) > 0]
            if completed:
                last_pit_lap = max(p["pit_in_lap_count"] for p in completed)

        rows.append(NASCARLiveSnapshot(
            race_id=race_id,
            season=season,
            series_id=series_id,
            snapshot_at=now,
            lap=current_lap,
            total_laps=total_laps,
            driver_id=driver.get("driver_id"),
            driver_name=driver.get("full_name"),
            car_number=str(v.get("vehicle_number", "")).strip(),
            manufacturer=v.get("vehicle_manufacturer"),
            position=v.get("running_position"),
            laps_completed=v.get("laps_completed"),
            pit_stops=len(pit_stop_list),
            laps_led=len(v.get("laps_led", [])),
            last_lap_time=v.get("last_lap_time"),
            last_lap_speed=v.get("last_lap_speed"),
            best_lap_time=v.get("best_lap_time"),
            best_lap_speed=v.get("best_lap_speed"),
            status=str(v.get("status", "")),
            delta_leader=v.get("delta"),
            last_pit_lap=last_pit_lap,
            tire_age=(current_lap - last_pit_lap) if last_pit_lap and current_lap else None,
        ))

    return rows

def compute_projections(snapshots: list, track_name: str, race_name_filter: str, db) -> list:
    """
    Compute projected finish for each driver based on:
    - Current running position (weighted by race progress)
    - Historical avg finish at this track
    - Historical avg running position at this track
    - Current speed vs historical speed
    """
    from sqlalchemy import text

    if not snapshots:
        return []

    race_pct = 0
    if snapshots[0].total_laps and snapshots[0].total_laps > 0:
        race_pct = (snapshots[0].lap or 0) / snapshots[0].total_laps

    # Pull historical baselines for all drivers
    hist_sql = text("""
        SELECT
            res.driver_name,
            res.driver_id,
            ROUND(AVG(res.finish_position)::numeric, 2) as hist_avg_finish,
            ROUND(AVG(res.avg_position)::numeric, 2) as hist_avg_running_pos,
            ROUND(AVG(ds.driver_rating)::numeric, 2) as hist_rating
        FROM nascar.results res
        JOIN nascar.races r ON r.race_id = res.race_id
        LEFT JOIN nascar.driver_stats ds ON ds.race_id = res.race_id
            AND ds.driver_id = res.driver_id
        WHERE r.track_name ILIKE :track
        AND (:race_name IS NULL OR r.race_name ILIKE :race_name)
        AND res.finish_position > 0
        GROUP BY res.driver_name, res.driver_id
    """)

    hist_params = {
        "track": f"%{track_name}%",
        "race_name": f"%{race_name_filter}%" if race_name_filter else None,
    }

    hist_rows = db.execute(hist_sql, hist_params).mappings().all()
    hist_map = {r["driver_id"]: dict(r) for r in hist_rows}
    hist_name_map = {r["driver_name"]: dict(r) for r in hist_rows}

    field_size = len(snapshots)
    projections = []

    for s in snapshots:
        hist = hist_map.get(s.driver_id) or hist_name_map.get(s.driver_name, {})

        hist_finish = hist.get("hist_avg_finish") or field_size / 2
        hist_running = hist.get("hist_avg_running_pos") or hist_finish
        current_pos = s.position or field_size

        # Weight shifts from historical → current as race progresses
        current_weight = 0.3 + (0.55 * race_pct)
        hist_finish_weight = 0.4 * (1 - race_pct)
        hist_running_weight = 0.3 * (1 - race_pct)

        projected = (
            current_pos * current_weight +
            float(hist_finish) * hist_finish_weight +
            float(hist_running) * hist_running_weight
        )

        # Speed adjustment — if driver has no historical data use neutral
        speed_adj = 0
        if s.best_lap_speed and s.best_lap_speed > 0:
            # Compare to field best so far in this snapshot
            field_best = max(
                (snap.best_lap_speed for snap in snapshots if snap.best_lap_speed),
                default=s.best_lap_speed
            )
            speed_delta = (s.best_lap_speed - field_best) / field_best
            speed_adj = speed_delta * 5 * (1 - race_pct)

        projected = max(1, min(field_size, projected + speed_adj))

        projections.append({
            "driver_name": s.driver_name,
            "car_number": s.car_number,
            "manufacturer": s.manufacturer,
            "current_position": current_pos,
            "laps_completed": s.laps_completed,
            "laps_led": s.laps_led,
            "best_lap_speed": s.best_lap_speed,
            "last_lap_speed": s.last_lap_speed,
            "pit_stops": s.pit_stops,
            "projected_finish": round(projected, 1),
            "hist_avg_finish": float(hist_finish) if hist.get("hist_avg_finish") else None,
            "hist_rating": float(hist["hist_rating"]) if hist.get("hist_rating") else None,
            "race_pct": round(race_pct * 100, 1),
            "lap": s.lap,
            "total_laps": s.total_laps,
        })

    # Sort by projected finish
    projections.sort(key=lambda x: x["projected_finish"])
    return projections


def poll_once(race_id: int, season: int, series_id: int,
              track_name: str, race_name_filter: str, db) -> dict:
    """Single poll cycle — fetch, store, project."""
    data = fetch_live_feed(series_id, race_id)
    if not data:
        return {"error": "no data"}

    lap = data.get("lap_number", 0)
    total = data.get("number_of_race_laps", 0)
    flag = data.get("flag_state", 0)

    flag_names = {0: "none", 1: "green", 2: "yellow", 3: "red",
                  4: "checkered", 8: "warm up", 9: "pre-race"}
    flag_str = flag_names.get(flag, str(flag))

    print(f"  [{datetime.now().strftime('%H:%M:%S')}] "
          f"Lap {lap}/{total} · flag: {flag_str}")

    snapshots = parse_snapshot(data, race_id, season, series_id)
    if snapshots:
        for s in snapshots:
            db.add(s)
        db.commit()

    projections = compute_projections(snapshots, track_name, race_name_filter, db)

    # Print top 10 projected
    print(f"  Projected top 10:")
    for p in projections[:10]:
        hist_str = f"(hist: {p['hist_avg_finish']:.1f})" if p['hist_avg_finish'] else "(no history)"
        print(f"    {int(p['projected_finish']):2d}. #{p['car_number']} "
              f"{p['driver_name']} — P{p['current_position']} now {hist_str}")

    return {
        "lap": lap,
        "total_laps": total,
        "flag": flag_str,
        "projections": projections,
    }


def main():
    parser = argparse.ArgumentParser(description="NASCAR live race poller")
    parser.add_argument("--race-id", type=int, required=True)
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--series", type=int, default=1)
    parser.add_argument("--track", type=str, default="Darlington")
    parser.add_argument("--race-name", type=str, default="Goodyear",
                        help="Race name filter for historical data")
    parser.add_argument("--interval", type=int, default=30,
                        help="Poll interval in seconds (default: 30)")
    parser.add_argument("--once", action="store_true",
                        help="Poll once and exit (for testing)")
    args = parser.parse_args()

    print(f"NASCAR Live Poller")
    print(f"  Race ID: {args.race_id} · Season: {args.season} · Series: {args.series}")
    print(f"  Track: {args.track} · Poll interval: {args.interval}s")
    print(f"  Press Ctrl+C to stop\n")

    db = SessionLocal()
    try:
        if args.once:
            poll_once(args.race_id, args.season, args.series,
                      args.track, args.race_name, db)
        else:
            while True:
                try:
                    result = poll_once(
                        args.race_id, args.season, args.series,
                        args.track, args.race_name, db
                    )
                    # Stop if checkered flag
                    if result.get("flag") == "checkered":
                        print("\n  Checkered flag — race complete. Stopping poller.")
                        break
                except Exception as e:
                    print(f"  [poller] error: {e}")

                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n  Stopped by user.")
    finally:
        db.close()


if __name__ == "__main__":
    main()