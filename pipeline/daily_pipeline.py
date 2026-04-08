"""
Daily Sports Data Pipeline

Runs on a schedule to keep all sports data current.

Schedule:
  6:00 AM ET daily     — MLB fetch + transform + health check
  6:30 AM ET daily     — Fangraphs stats update
  11:00 PM ET Sundays  — NASCAR fetch + transform
  11:00 PM ET Sundays  — F1 fetch + transform (race weekends)

Usage:
    PYTHONPATH=/app python3 /pipeline/daily_pipeline.py           # run scheduler
    PYTHONPATH=/app python3 /pipeline/daily_pipeline.py --now mlb # run MLB pipeline now
    PYTHONPATH=/app python3 /pipeline/daily_pipeline.py --now all # run everything now
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'api'))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


def run_mlb_daily():
    """Fetch and transform all Final MLB games from yesterday."""
    from database import SessionLocal
    from mlb.fetch import get_schedule, fetch_game
    from mlb.transform import transform_game_pk
    from mlb.health_check import run_health_checks
    from models.mlb import MLBRawEvent

    yesterday = (datetime.now(ET) - timedelta(days=1)).strftime("%Y-%m-%d")
    log.info(f"MLB daily pipeline starting for {yesterday}")

    db = SessionLocal()
    try:
        # Fetch
        games = get_schedule_for_date(yesterday)
        log.info(f"Found {len(games)} Final games for {yesterday}")

        inserted, skipped, errors = 0, 0, 0
        for game in games:
            action = fetch_game(game["game_pk"], db)
            if action == "inserted":
                inserted += 1
            elif action == "skipped":
                skipped += 1
            else:
                errors += 1

        log.info(f"Fetch complete: {inserted} inserted, {skipped} skipped, {errors} errors")

        # Transform new games only
        if inserted > 0:
            new_pks = db.execute(
                __import__('sqlalchemy').text(
                    "SELECT game_pk FROM mlb.raw_events WHERE game_date = :d"
                ),
                {"d": yesterday}
            ).scalars().all()

            log.info(f"Transforming {len(new_pks)} games...")
            for pk in new_pks:
                try:
                    transform_game_pk(pk, db)
                except Exception as e:
                    log.error(f"Transform error for {pk}: {e}")

        # Health check
        log.info("Running health checks...")
        passed = run_health_checks(yesterday, db)
        if passed:
            log.info("Health checks passed")
        else:
            log.error("Health checks FAILED — review pipeline output")

    except Exception as e:
        log.error(f"MLB daily pipeline error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


def get_schedule_for_date(date_str: str) -> list:
    """Fetch Final games for a specific date from MLB API."""
    import httpx
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&gameType=R,S&date={date_str}"
    try:
        res = httpx.get(url, timeout=15)
        data = res.json()
        games = []
        for d in data.get("dates", []):
            for g in d.get("games", []):
                if g.get("status", {}).get("detailedState") == "Final":
                    games.append({
                        "game_pk": g["gamePk"],
                        "game_date": date_str,
                        "status": "Final",
                        "away": g.get("teams", {}).get("away", {}).get("team", {}).get("abbreviation"),
                        "home": g.get("teams", {}).get("home", {}).get("team", {}).get("abbreviation"),
                    })
        return games
    except Exception as e:
        log.error(f"Schedule fetch error: {e}")
        return []


def run_fangraphs_daily():
    """Update Fangraphs season stats for current year."""
    from database import SessionLocal
    from mlb.fetch_fangraphs import fetch_batting, fetch_pitching

    season = datetime.now().year
    log.info(f"Fangraphs update starting for {season}")

    db = SessionLocal()
    try:
        n_bat = fetch_batting(season, db)
        log.info(f"Fangraphs batting: {n_bat} rows updated")
        n_pitch = fetch_pitching(season, db)
        log.info(f"Fangraphs pitching: {n_pitch} rows updated")
    except Exception as e:
        log.error(f"Fangraphs pipeline error: {e}")
    finally:
        db.close()


def run_nascar_sunday():
    """Fetch and transform all completed NASCAR races from the current weekend."""
    from database import SessionLocal
    from nascar.client import NASCARClient
    from nascar.fetch import get_race_ids_for_season, fetch_race
    from nascar.transform import transform_race_id

    season = datetime.now().year
    log.info(f"NASCAR Sunday pipeline starting for {season}")

    client = NASCARClient()
    db = SessionLocal()
    try:
        from models.nascar import NASCARRawEvent
        races = get_race_ids_for_season(client, season, series_id=1)
        log.info(f"Found {len(races)} Cup races in schedule")

        for race in races:
            race_id = race["race_id"]
            existing = db.query(NASCARRawEvent).filter(
                NASCARRawEvent.season == season,
                NASCARRawEvent.series_id == 1,
                NASCARRawEvent.race_id == race_id,
                NASCARRawEvent.endpoint_type == "weekend_feed",
            ).first()

            if not existing:
                log.info(f"Fetching new race: {race['race_name']} ({race_id})")
                fetch_race(db, client, season, 1, race)
                transform_race_id(season, 1, race_id, db)
            else:
                log.debug(f"Already stored: {race['race_name']}")

    except Exception as e:
        log.error(f"NASCAR Sunday pipeline error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


def run_f1_sunday():
    """Fetch and transform any new F1 race results."""
    from database import SessionLocal
    from f1.client import F1Client
    from f1.fetch import fetch_season
    from f1.transform import transform_round
    from models.f1 import F1RawEvent

    season = datetime.now().year
    log.info(f"F1 Sunday pipeline starting for {season}")

    client = F1Client()
    db = SessionLocal()
    try:
        schedule = client.get_schedule(season)
        if not schedule:
            log.info("No F1 schedule found")
            return

        today = datetime.now(ET).date()
        for race in schedule:
            race_date = race.get("date", "")
            if not race_date:
                continue

            rd = datetime.strptime(race_date, "%Y-%m-%d").date()
            # Only fetch races that completed in the last 7 days
            if 0 <= (today - rd).days <= 7:
                round_num = int(race["round"])
                existing = db.query(F1RawEvent).filter(
                    F1RawEvent.season == season,
                    F1RawEvent.round == round_num,
                    F1RawEvent.event_type == "results",
                ).first()

                if not existing:
                    log.info(f"Fetching F1 R{round_num}: {race['raceName']}")
                    fetch_season(client, db, season, round_filter=round_num)
                    transform_round(season, round_num, db)
                else:
                    log.info(f"F1 R{round_num} already stored")

    except Exception as e:
        log.error(f"F1 Sunday pipeline error: {e}")
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description="Daily sports data pipeline scheduler")
    parser.add_argument("--now", type=str, default=None,
                        choices=["mlb", "fangraphs", "nascar", "f1", "all"],
                        help="Run a specific pipeline immediately instead of scheduling")
    args = parser.parse_args()

    if args.now:
        log.info(f"Running {args.now} pipeline now...")
        if args.now in ("mlb", "all"):
            run_mlb_daily()
        if args.now in ("fangraphs", "all"):
            run_fangraphs_daily()
        if args.now in ("nascar", "all"):
            run_nascar_sunday()
        if args.now in ("f1", "all"):
            run_f1_sunday()
        return

    # Scheduled mode
    scheduler = BlockingScheduler(timezone=ET)

    # MLB: 6am ET daily
    scheduler.add_job(
        run_mlb_daily,
        CronTrigger(hour=6, minute=0, timezone=ET),
        id="mlb_daily",
        name="MLB daily fetch + transform + health check",
        misfire_grace_time=3600,
    )

    # Fangraphs: 6:30am ET daily
    scheduler.add_job(
        run_fangraphs_daily,
        CronTrigger(hour=6, minute=30, timezone=ET),
        id="fangraphs_daily",
        name="Fangraphs season stats update",
        misfire_grace_time=3600,
    )

    # NASCAR: 11pm ET Sundays
    scheduler.add_job(
        run_nascar_sunday,
        CronTrigger(day_of_week="sun", hour=23, minute=0, timezone=ET),
        id="nascar_sunday",
        name="NASCAR Sunday fetch + transform",
        misfire_grace_time=7200,
    )

    # F1: 11pm ET Sundays
    scheduler.add_job(
        run_f1_sunday,
        CronTrigger(day_of_week="sun", hour=23, minute=15, timezone=ET),
        id="f1_sunday",
        name="F1 Sunday fetch + transform",
        misfire_grace_time=7200,
    )

    log.info("Scheduler starting...")
    log.info("  MLB daily:      6:00 AM ET")
    log.info("  Fangraphs:      6:30 AM ET")
    log.info("  NASCAR Sunday: 11:00 PM ET")
    log.info("  F1 Sunday:     11:15 PM ET")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        log.info("Scheduler stopped.")


if __name__ == "__main__":
    main()