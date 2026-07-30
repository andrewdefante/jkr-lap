"""
Daily Sports Data Pipeline

Runs on a schedule to keep all sports data current.

Schedule:
  6:00 AM ET daily     — MLB morning pipeline (fetch → BAPV+ → Goose+ → runners →
                         Stuff Score → pK+/pHR+ → Juiced+2 → Goose+3 → weather → projections)
  10:00 AM ET daily    — Weather fetch
  10:30 AM ET daily    — Daily projections (lineups posted)
  11:00 AM ET daily    — pK+ / pHR+ rebuild
  11:15 AM ET daily    — Juiced+ 2 rebuild
  11:30 AM ET daily    — Goose+ 3 rebuild
  7:00 AM ET Mondays   — Batter tendencies (weekly)
  11:00 PM ET Sundays  — NASCAR fetch + transform
  11:15 PM ET Sundays  — F1 fetch + transform (race weekends)

Usage:
    PYTHONPATH=/app python3 /pipeline/daily_pipeline.py                      # run scheduler
    PYTHONPATH=/app python3 /pipeline/daily_pipeline.py --now mlb            # run MLB fetch now
    PYTHONPATH=/app python3 /pipeline/daily_pipeline.py --now pk-phr         # run pK+/pHR+ now
    PYTHONPATH=/app python3 /pipeline/daily_pipeline.py --now goose3         # run Goose+3 now
    PYTHONPATH=/app python3 /pipeline/daily_pipeline.py --now weather        # run weather now
    PYTHONPATH=/app python3 /pipeline/daily_pipeline.py --now all            # run everything now
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
ET = ZoneInfo("America/New_York")   # keep for overnight date math (actuals fire at 9pm PT = midnight ET)
PT = ZoneInfo("America/Los_Angeles")


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


def run_fetch_bbref():
    _run_script("BBref WAR/OPS+/PA fetch", "/pipeline/mlb/fetch_bbref.py",
                ["--season", "2026"])

def run_build_player_map():
    _run_script("Player ID Map Update", "/pipeline/mlb/build_player_map.py",
                ["--update-missing"])


def run_send_integrity_email(pipeline_log: list = None, pa_results: dict = None):
    """Send data integrity email with pipeline run log and PA validation results."""
    try:
        from send_email import send_integrity_email
        send_integrity_email(pipeline_log or [], pa_results or {})
    except Exception as e:
        log.error(f"Integrity email failed: {e}")


def run_send_daily_brief():
    """Send daily morning brief as email."""
    try:
        from send_email import send_daily_brief_email
        brief_path = '/app/static/homepage.html'
        if os.path.exists(brief_path):
            with open(brief_path, 'r') as f:
                html = f.read()
            send_daily_brief_email(html)
        else:
            log.warning("Homepage not found for email brief")
    except Exception as e:
        log.error(f"Daily brief email failed: {e}")


def run_pa_validation():
    """Compare BBref season PA totals to our at_bats table. Log mismatches."""
    from database import SessionLocal
    from sqlalchemy import text
    db = SessionLocal()
    try:
        result = db.execute(text("""
            WITH bbref_pa AS (
                SELECT bb.mlb_id::text as player_id,
                       bb.pa as bbref_pa, bb.name
                FROM mlb.bbref_batting bb
                WHERE bb.year_id = 2026
                AND bb.pa > 0
            ),
            our_pa AS (
                SELECT ab.batter_id::text as player_id, COUNT(*) as our_pa
                FROM mlb.at_bats ab
                JOIN mlb.games g ON g.game_pk = ab.game_pk
                WHERE g.season = 2026 AND g.game_type = 'R'
                AND ab.event NOT IN ('Runner Out','Caught Stealing 2B',
                    'Caught Stealing 3B','Wild Pitch','Passed Ball')
                GROUP BY ab.batter_id
            )
            SELECT COUNT(*) as total_bbref,
                   SUM(bbref_pa) as sum_bbref_pa,
                   SUM(our_pa) as sum_our_pa,
                   SUM(CASE WHEN ABS(COALESCE(our_pa,0) - bbref_pa) > 5
                       THEN 1 ELSE 0 END) as mismatch_count
            FROM bbref_pa
            LEFT JOIN our_pa USING (player_id)
            WHERE bbref_pa >= 10
        """)).mappings().first()

        sum_bbref = int(result['sum_bbref_pa'] or 0)
        sum_ours = int(result['sum_our_pa'] or 0)
        mismatches = int(result['mismatch_count'] or 0)
        diff_pct = abs(sum_bbref - sum_ours) / max(sum_bbref, 1) * 100

        log.info(f"PA Validation: BBref={sum_bbref:,} Ours={sum_ours:,} "
                 f"Diff={diff_pct:.1f}% Mismatches={mismatches}")

        if diff_pct > 2.0:
            log.warning(f"PA MISMATCH DETECTED — {diff_pct:.1f}% gap. "
                        f"BBref data may be stale — run fetch_bbref.py to update.")
            rows = db.execute(text("""
                WITH bbref_pa AS (
                    SELECT bb.mlb_id::text as player_id,
                           bb.pa as bbref_pa, bb.name
                    FROM mlb.bbref_batting bb
                    WHERE bb.year_id = 2026
                    AND bb.pa > 0
                ),
                our_pa AS (
                    SELECT ab.batter_id::text as player_id, COUNT(*) as our_pa
                    FROM mlb.at_bats ab
                    JOIN mlb.games g ON g.game_pk = ab.game_pk
                    WHERE g.season = 2026 AND g.game_type = 'R'
                    AND ab.event NOT IN ('Runner Out','Caught Stealing 2B',
                        'Caught Stealing 3B','Wild Pitch','Passed Ball')
                    GROUP BY ab.batter_id
                )
                SELECT b.name, b.player_id, b.bbref_pa,
                       COALESCE(o.our_pa, 0) as our_pa,
                       b.bbref_pa - COALESCE(o.our_pa, 0) as gap
                FROM bbref_pa b
                LEFT JOIN our_pa o USING (player_id)
                WHERE ABS(COALESCE(o.our_pa, 0) - b.bbref_pa) > 10
                AND b.bbref_pa >= 20
                ORDER BY ABS(COALESCE(o.our_pa, 0) - b.bbref_pa) DESC
                LIMIT 20
            """)).mappings().all()
            for r in rows:
                log.warning(f"  {r['name']}: BBref={r['bbref_pa']} "
                            f"Ours={r['our_pa']} Gap={r['gap']}")
        else:
            log.info("PA validation OK — within 2% tolerance")

        return {
            'sum_bbref_pa': sum_bbref,
            'sum_our_pa': sum_ours,
            'diff_pct': round(diff_pct, 2),
            'mismatch_count': mismatches,
        }
    finally:
        db.close()


def _run_script(name: str, script: str, args: list = None):
    """Run a pipeline script via subprocess and log tail output."""
    import subprocess
    cmd = ['python3', script] + (args or [])
    env = {**os.environ, 'PYTHONPATH': '/app:/pipeline'}
    start = datetime.now()
    log.info(f"Starting: {name}")
    try:
        result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=1800)
        elapsed = (datetime.now() - start).seconds
        if result.returncode == 0:
            last = [l for l in result.stdout.strip().split('\n') if l][-3:]
            log.info(f"Completed: {name} ({elapsed}s) — {' | '.join(last)}")
        else:
            log.error(f"Failed: {name} ({elapsed}s)\n{result.stderr[-600:]}")
    except subprocess.TimeoutExpired:
        log.error(f"Timeout: {name} (>30 min)")
    except Exception as e:
        log.error(f"Error running {name}: {e}")


def run_compute_bapv():
    _run_script("Compute BAPV+", "/pipeline/mlb/compute_bapv.py", ["--season", "2026"])


def run_build_pitcher_mix():
    _run_script("Pitcher Mix", "/pipeline/mlb/build_pitcher_mix.py", ["--season", "2026"])


def run_build_goose():
    _run_script("Goose+/Juiced+", "/pipeline/mlb/build_goose.py",
                ["--season", "2026", "--skip-boundaries", "--skip-buckets"])


def run_build_goose2():
    _run_script("Goose+ 2 / Juiced+ 2", "/pipeline/mlb/build_goose2.py",
                ["--score-season", "2026", "--baseline-season", "2025"])


def run_build_juiced2():
    _run_script("Juiced+ 2", "/pipeline/mlb/build_goose2.py",
                ["--score-season", "2026", "--baseline-season", "2025"])


def run_build_team_defense():
    _run_script("Team Defense", "/pipeline/mlb/build_team_defense.py",
                ["--season", "2026"])


def run_build_stuff_score():
    _run_script("Stuff Score", "/pipeline/mlb/build_stuff_score.py",
                ["--score-season", "2026"])


def run_build_pk_phr():
    _run_script("pK+ / pHR+", "/pipeline/mlb/build_pk_phr.py",
                ["--score-season", "2026", "--baseline-season", "2025"])


def run_build_goose3():
    _run_script("Goose+ 3", "/pipeline/mlb/build_goose3.py",
                ["--score-season", "2026"])


def run_build_context_splits():
    _run_script("Context Splits", "/pipeline/mlb/build_context_splits.py",
                ["--seasons", "2024", "2025", "2026"])


def run_build_workload():
    _run_script("Player Workload", "/pipeline/mlb/build_context_splits.py",
                ["--workload-only"])


def run_fetch_weather():
    today = datetime.now(PT).strftime("%Y-%m-%d")
    _run_script("Weather Fetch", "/pipeline/mlb/fetch_weather.py",
                ["--date", today])


def run_reconstruct_runners():
    _run_script("Reconstruct Runners", "/pipeline/mlb/reconstruct_runners.py",
                ["--season", "2026"])


def run_build_batter_tendencies():
    _run_script("Batter Tendencies", "/pipeline/mlb/build_batter_tendencies.py",
                ["--season", "2026"])


def run_fetch_kalshi():
    _run_script("Kalshi Markets", "/pipeline/mlb/fetch_kalshi.py")


def run_update_statuses():
    _run_script("Status Update", "/pipeline/mlb/fetch.py",
                ["--season", "2026", "--game-type", "R", "--update-status"])


def run_build_actuals():
    yesterday = (datetime.now(ET) - timedelta(days=1)).strftime("%Y-%m-%d")
    _run_script("Daily Actuals", "/pipeline/mlb/build_actuals.py",
                ["--date", yesterday])


def run_build_model_scores():
    yesterday = (datetime.now(ET) - timedelta(days=1)).strftime("%Y-%m-%d")
    _run_script("Model Scores", "/pipeline/mlb/build_model_scores.py",
                ["--date", yesterday])


def run_build_calibration():
    _run_script("Projection Calibration", "/pipeline/mlb/build_calibration.py",
                ["--lookback", "14"])


def run_build_game_projections():
    """Score yesterday's completed games (nightly)."""
    yesterday = (datetime.now(ET) - timedelta(days=1)).strftime("%Y-%m-%d")
    _run_script("Game Projections", "/pipeline/mlb/build_game_projections.py",
                ["--date", yesterday])


def run_build_today_game_projections():
    """Build game projections for today's slate (daytime hourly)."""
    today = datetime.now(PT).strftime("%Y-%m-%d")
    _run_script("Today Game Projections", "/pipeline/mlb/build_game_projections.py",
                ["--date", today])


def run_integrity_check():
    _run_script("Integrity Check", "/pipeline/mlb/build_integrity_check.py")


def run_generate_homepage():
    _run_script("Morning Brief", "/pipeline/mlb/generate_homepage.py")


def run_daily_projections():
    today = datetime.now(PT).strftime("%Y-%m-%d")
    _run_script("Daily Projections", "/pipeline/mlb/build_daily_projections.py",
                ["--date", today])


def run_fetch_lineups():
    today = datetime.now(PT).strftime("%Y-%m-%d")
    _run_script("Lineup Fetch", "/pipeline/mlb/fetch_lineups.py",
                ["--date", today])


def run_mlb_morning():
    """
    Full MLB morning pipeline — runs sequentially after overnight games are final.
    Fetch → transform → BAPV+ → pitcher mix → Goose+ → runners → Stuff Score →
    pK+/pHR+ → Juiced+2 → Goose+3 → weather → daily projections.
    Sends data integrity email on completion.
    """
    import time
    pipeline_log = []

    def tracked(name, fn):
        start = time.time()
        try:
            result = fn()
            duration = time.time() - start
            pipeline_log.append({
                'name': name, 'success': True,
                'duration': duration, 'detail': str(result or '')[:80]
            })
            return result
        except Exception as e:
            duration = time.time() - start
            pipeline_log.append({
                'name': name, 'success': False,
                'duration': duration, 'detail': str(e)[:80]
            })
            log.error(f"{name} failed: {e}")
            return None

    log.info("=" * 60)
    log.info("MLB MORNING PIPELINE START")
    log.info("=" * 60)

    tracked("Status Update", run_update_statuses)
    tracked("MLB Fetch", run_mlb_daily)
    tracked("BBref Fetch", run_fetch_bbref)
    tracked("Player ID Map", run_build_player_map)
    pa_results = tracked("PA Validation", run_pa_validation)
    tracked("Fangraphs", run_fangraphs_daily)
    tracked("BAPV+", run_compute_bapv)
    tracked("Pitcher Mix", run_build_pitcher_mix)
    tracked("Goose+", run_build_goose)
    tracked("Runners", run_reconstruct_runners)
    tracked("Stuff Score", run_build_stuff_score)
    tracked("pK+/pHR+", run_build_pk_phr)
    tracked("Juiced+2", run_build_juiced2)
    tracked("Team Defense", run_build_team_defense)
    tracked("Goose+3", run_build_goose3)
    tracked("Context Splits", run_build_context_splits)
    tracked("Weather", run_fetch_weather)
    tracked("Model Scores", run_build_model_scores)
    tracked("Calibration", run_build_calibration)
    tracked("Daily Projections", run_daily_projections)

    run_send_integrity_email(pipeline_log, pa_results)

    log.info("=" * 60)
    log.info("MLB MORNING PIPELINE COMPLETE")
    log.info("=" * 60)


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
    parser.add_argument(
        "--now", type=str, default=None,
        choices=["mlb", "mlb-morning", "fangraphs", "bbref", "pa-validation",
                 "bapv", "pitcher-mix",
                 "goose", "stuff-score", "pk-phr", "juiced2", "goose3",
                 "splits", "workload", "scores", "game-scores", "calibration",
                 "weather", "runners", "kalshi", "actuals", "homepage",
                 "batter-tendencies", "daily-projections", "lineups",
                 "update-status", "team-defense", "nascar", "f1", "integrity", "player-map", "all"],
        help="Run a specific pipeline immediately instead of scheduling",
    )
    args = parser.parse_args()

    if args.now:
        log.info(f"Running '{args.now}' now...")
        if args.now in ("update-status", "all"):
            run_update_statuses()
        if args.now in ("mlb", "all"):
            run_mlb_daily()
        if args.now in ("mlb-morning", "all"):
            run_mlb_morning()
        if args.now in ("fangraphs", "all"):
            run_fangraphs_daily()
        if args.now in ("bbref", "all"):
            run_fetch_bbref()
        if args.now in ("pa-validation", "all"):
            run_pa_validation()
        if args.now in ("bapv", "all"):
            run_compute_bapv()
        if args.now in ("pitcher-mix", "all"):
            run_build_pitcher_mix()
        if args.now in ("goose", "all"):
            run_build_goose()
        if args.now in ("stuff-score", "all"):
            run_build_stuff_score()
        if args.now in ("pk-phr", "all"):
            run_build_pk_phr()
        if args.now in ("juiced2", "all"):
            run_build_juiced2()
        if args.now in ("team-defense", "all"):
            run_build_team_defense()
        if args.now in ("goose3", "all"):
            run_build_goose3()
        if args.now in ("splits", "all"):
            run_build_context_splits()
        if args.now in ("workload",):
            run_build_workload()
        if args.now in ("weather", "all"):
            run_fetch_weather()
        if args.now in ("runners", "all"):
            run_reconstruct_runners()
        if args.now in ("kalshi", "all"):
            run_fetch_kalshi()
        if args.now in ("actuals", "all"):
            run_build_actuals()
        if args.now in ("scores", "all"):
            run_build_model_scores()
        if args.now in ("calibration", "all"):
            run_build_calibration()
        if args.now in ("game-scores", "all"):
            run_build_game_projections()
        if args.now in ("player-map", "all"):
            run_build_player_map()
        if args.now in ("integrity", "all"):
            run_integrity_check()
        if args.now in ("homepage", "all"):
            run_generate_homepage()
        if args.now in ("batter-tendencies", "all"):
            run_build_batter_tendencies()
        if args.now in ("daily-projections", "all"):
            run_daily_projections()
        if args.now in ("lineups", "all"):
            run_fetch_lineups()
        if args.now in ("nascar", "all"):
            run_nascar_sunday()
        if args.now in ("f1", "all"):
            run_f1_sunday()
        return

    # Scheduled mode (all times PT = America/Los_Angeles)
    scheduler = BlockingScheduler(timezone=PT)

    # ── OVERNIGHT ────────────────────────────────────────────────────────────
    # 11:15pm PT — build actuals
    scheduler.add_job(run_build_actuals, CronTrigger(hour=23, minute=15, timezone=PT),
        id="actuals_nightly", name="Build daily actuals (11:15pm PT)", misfire_grace_time=1800)

    # 11:20pm PT — model scores (after actuals)
    scheduler.add_job(run_build_model_scores, CronTrigger(hour=23, minute=20, timezone=PT),
        id="model_scores_nightly", name="Score projections vs actuals (11:20pm PT)",
        misfire_grace_time=1800)

    # 11:22pm PT — calibration (after model scores)
    scheduler.add_job(run_build_calibration, CronTrigger(hour=23, minute=22, timezone=PT),
        id="calibration_nightly", name="Update K/HR calibration factors (11:22pm PT)",
        misfire_grace_time=1800)

    # 11:25pm PT — game projections scoring (after actuals)
    scheduler.add_job(run_build_game_projections, CronTrigger(hour=23, minute=25, timezone=PT),
        id="game_projections_nightly", name="Score game run and W/L projections (11:25pm PT)",
        misfire_grace_time=1800)

    # 11:30pm PT — final status update
    scheduler.add_job(run_update_statuses, CronTrigger(hour=23, minute=30, timezone=PT),
        id="status_final", name="Final status update (11:30pm PT)", misfire_grace_time=1800)

    # ── EARLY MORNING ────────────────────────────────────────────────────────
    # 6:00am PT — MLB morning pipeline + integrity email
    scheduler.add_job(run_mlb_morning, CronTrigger(hour=6, minute=0, timezone=PT),
        id="mlb_morning", name="MLB morning pipeline (6:00am PT)", misfire_grace_time=1800)

    # 6:05am PT — team defense rebuild (after overnight game data is loaded)
    scheduler.add_job(run_build_team_defense, CronTrigger(hour=6, minute=5, timezone=PT),
        id="team_defense_daily", name="Team defense metrics rebuild (6:05am PT)",
        misfire_grace_time=600)

    # 6:10am PT — data integrity check (after fetch, before projections)
    scheduler.add_job(run_integrity_check, CronTrigger(hour=6, minute=10, timezone=PT),
        id="integrity_check_daily",
        name="Data integrity check (6:10am PT)",
        misfire_grace_time=600)

    # 6:30am PT — morning homepage + email
    scheduler.add_job(run_generate_homepage, CronTrigger(hour=6, minute=30, timezone=PT),
        id="homepage_morning", name="Generate morning brief (6:30am PT)", misfire_grace_time=600)
    scheduler.add_job(run_send_daily_brief, CronTrigger(hour=6, minute=35, timezone=PT),
        id="daily_brief_email", name="Send daily brief email (6:35am PT)", misfire_grace_time=600)

    # ── MORNING REFRESH ──────────────────────────────────────────────────────
    # 7:00am PT — weather + workload
    scheduler.add_job(run_fetch_weather, CronTrigger(hour=7, minute=0, timezone=PT),
        id="weather_morning", name="Weather fetch (7:00am PT)", misfire_grace_time=600)
    scheduler.add_job(run_build_workload, CronTrigger(hour=7, minute=0, timezone=PT),
        id="workload_morning", name="Workload/ACWR rebuild (7:00am PT)", misfire_grace_time=600)

    # 7:30am PT — first daily projections + Kalshi refresh
    scheduler.add_job(run_daily_projections, CronTrigger(hour=7, minute=30, timezone=PT),
        id="projections_730", name="Daily projections 7:30am PT", misfire_grace_time=600)
    scheduler.add_job(run_fetch_kalshi, CronTrigger(hour=7, minute=30, timezone=PT),
        id="kalshi_730", name="Kalshi fetch 7:30am PT", misfire_grace_time=600)

    # 8:00am–11:00pm PT — projections + Kalshi every 30 min
    for h, m, suffix in [
        (8, 0, "800"), (8, 30, "830"), (9, 0, "900"),
        (9, 30, "930"), (10, 0, "1000"), (10, 30, "1030"),
        (11, 0, "1100"), (11, 30, "1130"),
        (12, 0, "1200"), (12, 30, "1230"),
        (13, 0, "1300"), (13, 30, "1330"),
        (14, 0, "1400"), (14, 30, "1430"),
        (15, 0, "1500"), (15, 30, "1530"),
        (16, 0, "1600"), (16, 30, "1630"),
        (17, 0, "1700"), (17, 30, "1730"),
        (18, 0, "1800"), (18, 30, "1830"),
        (19, 0, "1900"), (19, 30, "1930"),
        (20, 0, "2000"), (20, 30, "2030"),
        (21, 0, "2100"), (21, 30, "2130"),
        (22, 0, "2200"), (22, 30, "2230"),
        (23, 0, "2300"),
    ]:
        scheduler.add_job(run_daily_projections,
            CronTrigger(hour=h, minute=m, timezone=PT),
            id=f"projections_{suffix}", name=f"Daily projections {suffix} PT",
            misfire_grace_time=600)
        scheduler.add_job(run_fetch_kalshi,
            CronTrigger(hour=h, minute=m, timezone=PT),
            id=f"kalshi_{suffix}", name=f"Kalshi fetch {suffix} PT",
            misfire_grace_time=600)

    # ── GAME PROJECTIONS (hourly 9am–9pm PT) ────────────────────────────────
    for h in range(9, 22):
        scheduler.add_job(
            run_build_today_game_projections,
            CronTrigger(hour=h, minute=0, timezone=PT),
            id=f"game_proj_{h:02d}00",
            name=f"Game projections {h:02d}:00 PT",
            misfire_grace_time=600,
        )
        scheduler.add_job(
            run_generate_homepage,
            CronTrigger(hour=h, minute=2, timezone=PT),
            id=f"homepage_{h:02d}02",
            name=f"Homepage refresh {h:02d}:02 PT",
            misfire_grace_time=600,
        )

    # ── HOMEPAGE REFRESHES + EMAILS ──────────────────────────────────────────
    # 9:00am PT — mid-morning homepage + email
    scheduler.add_job(run_generate_homepage, CronTrigger(hour=9, minute=0, timezone=PT),
        id="homepage_midmorning", name="Homepage mid-morning (9:00am PT)", misfire_grace_time=600)
    scheduler.add_job(run_send_daily_brief, CronTrigger(hour=9, minute=5, timezone=PT),
        id="brief_email_midmorning", name="Brief email mid-morning (9:05am PT)", misfire_grace_time=600)

    # 12:00pm PT — noon homepage + email
    scheduler.add_job(run_generate_homepage, CronTrigger(hour=12, minute=0, timezone=PT),
        id="homepage_noon", name="Homepage noon (12:00pm PT)", misfire_grace_time=600)
    scheduler.add_job(run_send_daily_brief, CronTrigger(hour=12, minute=5, timezone=PT),
        id="brief_email_noon", name="Brief email noon (12:05pm PT)", misfire_grace_time=600)

    # 4:00pm PT — evening homepage + email
    scheduler.add_job(run_generate_homepage, CronTrigger(hour=16, minute=0, timezone=PT),
        id="homepage_evening", name="Homepage evening (4:00pm PT)", misfire_grace_time=600)
    scheduler.add_job(run_send_daily_brief, CronTrigger(hour=16, minute=5, timezone=PT),
        id="brief_email_evening", name="Brief email evening (4:05pm PT)", misfire_grace_time=600)

    # ── STATUS UPDATES ───────────────────────────────────────────────────────
    # 7am–11:30pm PT — lineup poll every 30 min
    scheduler.add_job(run_fetch_lineups,
        CronTrigger(hour="7-23", minute="0,30", timezone=PT),
        id="lineup_poll", name="Poll MLB lineups every 30 min",
        misfire_grace_time=600)

    # Every 30 min 7am–11:30pm PT
    scheduler.add_job(run_update_statuses,
        CronTrigger(hour="7-23", minute="15,45", timezone=PT),
        id="status_updates", name="Status updates every 30 min", misfire_grace_time=600)

    # ── WEEKLY ───────────────────────────────────────────────────────────────
    # Monday 4:00am PT — batter tendencies rebuild
    scheduler.add_job(run_build_batter_tendencies,
        CronTrigger(day_of_week="mon", hour=4, minute=0, timezone=PT),
        id="batter_tendencies", name="Batter tendencies weekly rebuild (Mon 4am PT)",
        misfire_grace_time=3600)

    log.info("Joker Lap Scheduler starting... (all times PT = America/Los_Angeles)")
    log.info("  Actuals:            11:15pm PT nightly")
    log.info("  Final status:       11:30pm PT nightly")
    log.info("  MLB morning:         6:00am PT — fetch→BBref→models→projections + integrity email")
    log.info("  Integrity check:     6:10am PT")
    log.info("  Morning brief:       6:30am PT / email 6:35am PT")
    log.info("  Weather + workload:  7:00am PT")
    log.info("  Projections+Kalshi:  7:30am–11pm PT every 30 min (32 runs/day)")
    log.info("  Lineup poll:         7am–11:30pm PT every 30 min")
    log.info("  Homepage + email:    9am / 12pm / 4pm PT")
    log.info("  Status updates:      7am–11:30pm PT every 30 min (offset :15/:45)")
    log.info("  Batter tendencies:   Mon 4:00am PT (weekly)")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        log.info("Scheduler stopped.")


if __name__ == "__main__":
    main()