from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from database import get_db
from models.nascar import NASCARRawEvent
import httpx
from datetime import datetime

router = APIRouter()

NASCAR_BASE = "https://cf.nascar.com/cacher"
NASCAR_LIVE = "https://cf.nascar.com/cacher/live"
NASCAR_LOOP = "https://cf.nascar.com/loopstats/prod"

SERIES_NAMES = {1: "Cup Series", 2: "Xfinity Series", 3: "Truck Series"}

ENDPOINTS = {
    "weekend_feed":   "{base}/{year}/{series}/{race}/weekend-feed.json",
    "lap_times":      "{base}/{year}/{series}/{race}/lap-times.json",
    "pit_stops":      "{base}/{year}/{series}/{race}/live-pit-data.json",
    "lap_notes":      "{base}/{year}/{series}/{race}/lap-notes.json",
    "driver_stats":   "{loop}/{year}/{series}/{race}.json",
    "advanced_stats": "{live}/series_{series}/{race}/live-feed.json",
}


def build_url(endpoint_type: str, year: int, series_id: int, race_id: int) -> str:
    template = ENDPOINTS[endpoint_type]
    return template.format(
        base=NASCAR_BASE, live=NASCAR_LIVE, loop=NASCAR_LOOP,
        year=year, series=series_id, race=race_id
    )


async def fetch_and_store(
    endpoint_type: str, year: int, series_id: int, race_id: int, db: Session
) -> dict:
    url = build_url(endpoint_type, year, series_id, race_id)
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPStatusError as e:
            return {"endpoint_type": endpoint_type, "status": "not_available",
                    "detail": str(e)}
        except httpx.HTTPError as e:
            return {"endpoint_type": endpoint_type, "status": "error", "detail": str(e)}

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
        return {"endpoint_type": endpoint_type, "status": "updated", "url": url}
    else:
        raw = NASCARRawEvent(
            season=year, series_id=series_id, race_id=race_id,
            endpoint_type=endpoint_type, endpoint=url, data=data,
        )
        db.add(raw)
        db.commit()
        return {"endpoint_type": endpoint_type, "status": "inserted", "url": url}


@router.get("/schedule/{year}")
async def fetch_schedule(year: int):
    """Fetch the NASCAR race schedule for a given year."""
    url = f"{NASCAR_BASE}/{year}/race_list_basic.json"
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
        except httpx.HTTPError as e:
            raise HTTPException(status_code=502, detail=f"NASCAR API error: {str(e)}")
    return response.json()


@router.get("/fetch-race/{year}/{series_id}/{race_id}")
async def fetch_race(
    year: int, series_id: int, race_id: int,
    db: Session = Depends(get_db)
):
    """
    Fetch all available data for a single NASCAR race.
    Hits all 6 endpoints and stores each as a separate row in nascar.raw_events.

    Series IDs: 1=Cup, 2=Xfinity, 3=Truck
    Example: GET /nascar/fetch-race/2026/1/5596
    """
    results = []
    for endpoint_type in ENDPOINTS:
        result = await fetch_and_store(endpoint_type, year, series_id, race_id, db)
        results.append(result)

    successful = [r for r in results if r["status"] in ("inserted", "updated")]
    unavailable = [r for r in results if r["status"] == "not_available"]

    return {
        "race_id": race_id,
        "series": SERIES_NAMES.get(series_id, f"Series {series_id}"),
        "year": year,
        "endpoints_fetched": len(successful),
        "endpoints_unavailable": len(unavailable),
        "results": results,
    }


@router.get("/explore/{year}/{series_id}/{race_id}")
def explore_race(
    year: int, series_id: int, race_id: int,
    db: Session = Depends(get_db)
):
    """Summarize what's stored for a race across all endpoints."""
    rows = db.query(NASCARRawEvent).filter(
        NASCARRawEvent.season == year,
        NASCARRawEvent.series_id == series_id,
        NASCARRawEvent.race_id == race_id,
    ).all()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No data found. Call /nascar/fetch-race/{year}/{series_id}/{race_id} first."
        )

    stored = {r.endpoint_type: r for r in rows}
    summary = {
        "race_id": race_id,
        "series": SERIES_NAMES.get(series_id),
        "year": year,
        "stored_endpoints": list(stored.keys()),
        "missing_endpoints": [e for e in ENDPOINTS if e not in stored],
    }

    if "weekend_feed" in stored:
        wf = stored["weekend_feed"].data
        summary["weekend_feed_keys"] = list(wf.keys()) if isinstance(wf, dict) else "not a dict"

    if "lap_times" in stored:
        lt = stored["lap_times"].data
        summary["total_lap_records"] = len(lt) if isinstance(lt, list) else "see lap_times_keys"

    if "pit_stops" in stored:
        ps = stored["pit_stops"].data
        summary["total_pit_records"] = len(ps) if isinstance(ps, list) else "see pit_stops_keys"

    return summary


@router.get("/races")
def list_races(
    year: int = Query(default=None),
    series_id: int = Query(default=None),
    db: Session = Depends(get_db)
):
    """List all stored NASCAR races."""
    q = db.query(NASCARRawEvent).filter(NASCARRawEvent.endpoint_type == "weekend_feed")
    if year:
        q = q.filter(NASCARRawEvent.season == year)
    if series_id:
        q = q.filter(NASCARRawEvent.series_id == series_id)

    rows = q.order_by(NASCARRawEvent.season.desc(), NASCARRawEvent.race_id).all()
    return [
        {
            "race_id": r.race_id,
            "series": SERIES_NAMES.get(r.series_id),
            "season": r.season,
            "fetched_at": r.fetched_at,
        }
        for r in rows
    ]

@router.get("/track-profile/{track_name}")
def track_profile(
    track_name: str,
    race_name: str = Query(default=None),
    series_id: int = Query(default=1),
    db: Session = Depends(get_db)
):
    """
    Returns driver performance profile for a specific track.
    Powers the Darlington-style pre-race dashboard.

    Example:
      GET /nascar/track-profile/Darlington?race_name=Goodyear
    """
    from sqlalchemy import text

    passing_sql = text("""
        SELECT
            res.driver_name,
            res.manufacturer,
            ROUND(AVG(res.finish_position)::numeric, 1) as avg_finish,
            ROUND(AVG(res.start_position)::numeric, 1) as avg_start,
            ROUND(AVG(res.finish_position - res.start_position)::numeric, 1) as avg_pos_gained,
            ROUND(AVG(ds.quality_passes)::numeric, 1) as avg_quality_passes,
            ROUND(AVG(ds.green_flag_passes)::numeric, 1) as avg_gf_passes,
            ROUND(AVG(ds.green_flag_passed)::numeric, 1) as avg_gf_passed,
            ROUND(AVG(ds.quality_pass_differential)::numeric, 1) as avg_pass_diff,
            ROUND(AVG(ds.driver_rating)::numeric, 1) as avg_rating,
            COUNT(*) as races
        FROM nascar.results res
        JOIN nascar.races r ON r.race_id = res.race_id
        JOIN nascar.driver_stats ds ON ds.race_id = res.race_id
            AND ds.driver_id = res.driver_id
        WHERE r.track_name ILIKE :track
        AND (:race_name IS NULL OR r.race_name ILIKE :race_name)
        AND res.series_id = :series_id
        AND res.finish_position > 0
        GROUP BY res.driver_name, res.manufacturer
        HAVING COUNT(*) >= 1
        ORDER BY avg_rating DESC
    """)

    mfr_sql = text("""
        WITH field_avg AS (
            SELECT
                AVG(ds.driver_rating) as field_rating,
                AVG(res.finish_position) as field_finish,
                AVG(ds.quality_passes) as field_qp,
                AVG(ds.green_flag_passes::float / NULLIF(ds.green_flag_passed, 0)) as field_pass_ratio
            FROM nascar.results res
            JOIN nascar.races r ON r.race_id = res.race_id
            JOIN nascar.driver_stats ds ON ds.race_id = res.race_id
                AND ds.driver_id = res.driver_id
            WHERE r.track_name ILIKE :track
            AND (:race_name IS NULL OR r.race_name ILIKE :race_name)
            AND res.series_id = :series_id
            AND res.finish_position > 0
        ),
        mfr_stats AS (
            SELECT
                res.manufacturer,
                COUNT(DISTINCT res.driver_name) as drivers,
                COUNT(*) as entries,
                ROUND(AVG(ds.driver_rating)::numeric, 2) as avg_rating,
                ROUND(AVG(res.finish_position)::numeric, 1) as avg_finish,
                ROUND(AVG(ds.quality_passes)::numeric, 1) as avg_qp,
                ROUND(AVG(ds.green_flag_passes::float / 
                    NULLIF(ds.green_flag_passed, 0))::numeric, 3) as pass_ratio
            FROM nascar.results res
            JOIN nascar.races r ON r.race_id = res.race_id
            JOIN nascar.driver_stats ds ON ds.race_id = res.race_id
                AND ds.driver_id = res.driver_id
            WHERE r.track_name ILIKE :track
            AND (:race_name IS NULL OR r.race_name ILIKE :race_name)
            AND res.series_id = :series_id
            AND res.finish_position > 0
            AND res.manufacturer IS NOT NULL
            GROUP BY res.manufacturer
        )
        SELECT
            m.manufacturer,
            m.drivers,
            m.entries,
            m.avg_rating,
            ROUND((m.avg_rating / f.field_rating * 100)::numeric, 1) as rating_plus,
            m.avg_finish,
            ROUND((f.field_finish / NULLIF(m.avg_finish, 0) * 100)::numeric, 1) as finish_plus,
            m.avg_qp,
            ROUND((m.avg_qp / f.field_qp * 100)::numeric, 1) as qp_plus,
            m.pass_ratio,
            ROUND((m.pass_ratio / NULLIF(f.field_pass_ratio, 0) * 100)::numeric, 1) as pass_ratio_plus
        FROM mfr_stats m
        CROSS JOIN field_avg f
        ORDER BY rating_plus DESC
    """)

    lap_sql = text("""
        SELECT
            l.driver_name,
            CASE
                WHEN l.lap_number <= 30 THEN 'early'
                WHEN l.lap_number <= 100 THEN 'mid'
                ELSE 'late'
            END as segment,
            ROUND(AVG(l.lap_speed)::numeric, 3) as avg_speed
        FROM nascar.laps l
        JOIN nascar.races r ON r.race_id = l.race_id
        WHERE r.track_name ILIKE :track
        AND (:race_name IS NULL OR r.race_name ILIKE :race_name)
        AND l.series_id = :series_id
        AND l.lap_speed > 100
        AND l.lap_number > 0
        GROUP BY l.driver_name, segment
        HAVING COUNT(*) > 50
        ORDER BY l.driver_name, segment
    """)

    params = {
        "track": f"%{track_name}%",
        "race_name": f"%{race_name}%" if race_name else None,
        "series_id": series_id,
    }

    drivers_raw = db.execute(passing_sql, params).mappings().all()
    mfr_raw = db.execute(mfr_sql, params).mappings().all()
    laps_raw = db.execute(lap_sql, params).mappings().all()

    # Pivot lap segments per driver
    lap_map = {}
    for row in laps_raw:
        name = row["driver_name"]
        if name not in lap_map:
            lap_map[name] = {}
        lap_map[name][row["segment"]] = float(row["avg_speed"])

    drivers_out = []
    for d in drivers_raw:
        seg = lap_map.get(d["driver_name"], {})
        drivers_out.append({
            "driver_name": d["driver_name"],
            "manufacturer": d["manufacturer"],
            "avg_finish": float(d["avg_finish"]),
            "avg_start": float(d["avg_start"]),
            "avg_pos_gained": float(d["avg_pos_gained"]),
            "avg_quality_passes": float(d["avg_quality_passes"]) if d["avg_quality_passes"] else None,
            "avg_gf_passes": float(d["avg_gf_passes"]) if d["avg_gf_passes"] else None,
            "avg_gf_passed": float(d["avg_gf_passed"]) if d["avg_gf_passed"] else None,
            "avg_pass_diff": float(d["avg_pass_diff"]) if d["avg_pass_diff"] else None,
            "avg_rating": float(d["avg_rating"]) if d["avg_rating"] else None,
            "races": int(d["races"]),
            "lap_early": seg.get("early"),
            "lap_mid": seg.get("mid"),
            "lap_late": seg.get("late"),
        })

    return {
        "track": track_name,
        "race_name_filter": race_name,
        "drivers": drivers_out,
        "manufacturers": [dict(m) for m in mfr_raw],
    }


@router.get("/live/{race_id}/projections")
def live_projections(race_id: int, db: Session = Depends(get_db)):
    try:
        from sqlalchemy import text

        latest_sql = text("""
            SELECT MAX(snapshot_at) as latest
            FROM nascar.live_snapshots
            WHERE race_id = :race_id
        """)
        latest = db.execute(latest_sql, {"race_id": race_id}).scalar()
        if not latest:
            raise HTTPException(status_code=404, detail="No live data for this race yet.")

        snap_sql = text("""
            SELECT 
                driver_name, car_number, manufacturer,
                position, laps_completed, laps_led,
                last_lap_speed, best_lap_speed,
                pit_stops, status, delta_leader,
                lap, total_laps, snapshot_at,
                last_pit_lap, tire_age
            FROM nascar.live_snapshots
            WHERE race_id = :race_id
            AND snapshot_at = :latest
            ORDER BY position
        """)
        snaps = db.execute(snap_sql, {"race_id": race_id, "latest": latest}).mappings().all()

        # Look up race metadata so we can use the correct total laps and
        # pull historically comparable races at the same track / race name.
        race_meta_sql = text("""
            SELECT track_name, race_name, scheduled_laps, actual_laps, track_type
            FROM nascar.races
            WHERE race_id = :race_id
        """)
        race_meta = db.execute(race_meta_sql, {"race_id": race_id}).mappings().first()
        track_name  = race_meta["track_name"]  if race_meta else None
        race_name   = race_meta["race_name"]   if race_meta else None
        track_type  = race_meta["track_type"]  if race_meta else "intermediate"

        hist_sql = text("""
            WITH race_meta AS (
                SELECT track_name, track_type, scheduled_laps
                FROM nascar.races
                WHERE race_id = :race_id
            ),
            same_track AS (
                SELECT
                    res.driver_name,
                    AVG(res.finish_position) as avg_finish,
                    AVG(res.average_running_position) as avg_running,
                    COUNT(*) as races
                FROM nascar.results res
                JOIN nascar.races r ON r.race_id = res.race_id
                CROSS JOIN race_meta rm
                WHERE r.track_name = rm.track_name
                AND res.race_id != :race_id
                AND res.finish_position > 0
                AND r.season >= 2023
                GROUP BY res.driver_name
            ),
            same_type AS (
                SELECT
                    res.driver_name,
                    AVG(res.finish_position) as avg_finish,
                    AVG(res.average_running_position) as avg_running,
                    COUNT(*) as races
                FROM nascar.results res
                JOIN nascar.races r ON r.race_id = res.race_id
                CROSS JOIN race_meta rm
                WHERE r.track_type = rm.track_type
                AND res.race_id != :race_id
                AND res.finish_position > 0
                AND r.season >= 2024
                GROUP BY res.driver_name
            ),
            all_track AS (
                SELECT
                    res.driver_name,
                    AVG(res.finish_position) as avg_finish,
                    AVG(res.average_running_position) as avg_running,
                    COUNT(*) as races
                FROM nascar.results res
                JOIN nascar.races r ON r.race_id = res.race_id
                WHERE res.race_id != :race_id
                AND res.finish_position > 0
                AND r.season >= 2024
                GROUP BY res.driver_name
            )
            SELECT
                COALESCE(st.driver_name, stype.driver_name, at.driver_name) as driver_name,
                CASE
                    WHEN st.races >= 2 THEN
                        (st.avg_finish * 0.50) +
                        (COALESCE(stype.avg_finish, at.avg_finish, 20) * 0.30) +
                        (COALESCE(at.avg_finish, 20) * 0.20)
                    WHEN stype.races >= 3 THEN
                        (stype.avg_finish * 0.65) +
                        (COALESCE(at.avg_finish, 20) * 0.35)
                    ELSE
                        COALESCE(at.avg_finish, 20)
                END as hist_avg_finish,
                CASE
                    WHEN st.races >= 2 THEN
                        (COALESCE(st.avg_running, st.avg_finish) * 0.50) +
                        (COALESCE(stype.avg_running, stype.avg_finish, at.avg_finish, 20) * 0.30) +
                        (COALESCE(at.avg_running, at.avg_finish, 20) * 0.20)
                    WHEN stype.races >= 3 THEN
                        (COALESCE(stype.avg_running, stype.avg_finish) * 0.65) +
                        (COALESCE(at.avg_running, at.avg_finish, 20) * 0.35)
                    ELSE
                        COALESCE(at.avg_running, at.avg_finish, 20)
                END as hist_avg_running,
                COALESCE(st.races, 0) as same_track_races,
                COALESCE(stype.races, 0) as same_type_races,
                COALESCE(at.races, 0) as all_track_races
            FROM all_track at
            FULL OUTER JOIN same_type stype ON stype.driver_name = at.driver_name
            FULL OUTER JOIN same_track st ON st.driver_name = COALESCE(stype.driver_name, at.driver_name)
        """)
        hist_rows = db.execute(hist_sql, {"race_id": race_id}).mappings().all()
        hist_map = {
            r["driver_name"]: {
                "hist_avg_finish":  float(r["hist_avg_finish"]  or 20),
                "hist_avg_running": float(r["hist_avg_running"] or 20),
                "same_track_races": int(r["same_track_races"]   or 0),
                "same_type_races":  int(r["same_type_races"]    or 0),
            }
            for r in hist_rows
        }

        variance_sql = text("""
            SELECT
                driver_name,
                ROUND(STDDEV(last_lap_speed)::numeric, 3) as speed_stddev,
                ROUND(AVG(last_lap_speed)::numeric, 3) as speed_avg,
                COUNT(*) as lap_samples
            FROM nascar.live_snapshots
            WHERE race_id = :race_id
            AND last_lap_speed > 0
            AND lap > 5
            GROUP BY driver_name
        """)
        variance_rows = db.execute(variance_sql, {"race_id": race_id}).mappings().all()
        variance_map = {r["driver_name"]: dict(r) for r in variance_rows}

        # Get total laps with multiple fallbacks
        snap_total = snaps[0]["total_laps"] if snaps and snaps[0]["total_laps"] else 0
        meta_total = (race_meta["actual_laps"] or race_meta["scheduled_laps"]) if race_meta else 0
        total_laps = meta_total or snap_total or 400  # 400 as last resort default

        lap = snaps[0]["lap"] if snaps else 0
        race_pct = min(1.0, lap / total_laps) if total_laps > 0 else 0
        # Before lap 5 treat as pre-race
        if lap < 5:
            race_pct = 0
        field_size = len(snaps)

        projections = []
        for s in snaps:
            hist = hist_map.get(s["driver_name"], {})
            variance = variance_map.get(s["driver_name"], {})
            speed_stddev = float(variance["speed_stddev"]) if variance.get("speed_stddev") else None
            lap_samples = int(variance["lap_samples"]) if variance.get("lap_samples") else 0
            current_pos = s["position"] or field_size

            if hist.get("hist_avg_finish"):
                hist_finish = float(hist["hist_avg_finish"])
                hist_running = float(hist.get("hist_avg_running") or hist_finish)
            else:
                hist_finish = float(current_pos)
                hist_running = float(current_pos)

            # Accelerate current position weight in final 20% of race
            # so that by lap 400, current position is ~98% of the projection
            if race_pct >= 0.8:
                # Scale from 0.85 at 80% to 0.98 at 100%
                late_pct = (race_pct - 0.8) / 0.2
                current_weight = 0.85 + (0.13 * late_pct)
            else:
                current_weight = 0.3 + (0.55 * race_pct)
            hist_finish_weight = 0.4 * (1 - race_pct)
            hist_running_weight = 0.3 * (1 - race_pct)

            projected = (
                current_pos * current_weight +
                hist_finish * hist_finish_weight +
                hist_running * hist_running_weight
            )

            field_best = max((s2["best_lap_speed"] for s2 in snaps if s2["best_lap_speed"]), default=1)
            if s["best_lap_speed"] and field_best:
                speed_delta = (s["best_lap_speed"] - field_best) / field_best
                projected += speed_delta * 5 * (1 - race_pct)

            tire_age = s["tire_age"]
            if tire_age is not None:
                if tire_age <= 20:
                    tire_adj = -1.5 * (1 - tire_age / 20) * (1 - race_pct)
                elif tire_age >= 50:
                    tire_adj = 2.0 * min(1.0, (tire_age - 50) / 40) * (1 - race_pct)
                else:
                    tire_adj = 0
                projected += tire_adj

            projected = max(1, min(field_size, projected))

            projections.append({
                "driver_name": s["driver_name"],
                "car_number": s["car_number"],
                "manufacturer": s["manufacturer"],
                "current_position": current_pos,
                "projected_finish": round(projected, 1),
                "laps_completed": s["laps_completed"],
                "laps_led": s["laps_led"],
                "last_lap_speed": float(s["last_lap_speed"]) if s["last_lap_speed"] else None,
                "best_lap_speed": float(s["best_lap_speed"]) if s["best_lap_speed"] else None,
                "pit_stops": s["pit_stops"],
                "delta_leader": float(s["delta_leader"]) if s["delta_leader"] else None,
                "hist_avg_finish": hist.get("hist_avg_finish"),
                "has_history": bool(hist),
                "same_track_races": hist.get("same_track_races", 0),
                "same_type_races": hist.get("same_type_races", 0),
                "tire_age": tire_age,
                "last_pit_lap": s["last_pit_lap"],
                "speed_stddev": speed_stddev,
                "lap_samples": lap_samples,
            })

        projections.sort(key=lambda x: (x["projected_finish"], x["current_position"]))

        for i, p in enumerate(projections, 1):
            p["projected_finish"] = i

        return {
            "race_id": race_id,
            "track_name": track_name,
            "track_type": track_type,
            "lap": lap,
            "total_laps": total_laps,
            "race_pct": round(race_pct * 100, 1),
            "snapshot_at": latest.isoformat(),
            "projections": projections,
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"ERROR in live_projections: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))