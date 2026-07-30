"""
MLB Pipeline Integrity Check

Runs after the daily fetch to validate data completeness:
  1. Game completeness — compares mlb.games against the MLB Stats API schedule
     for the last N days, and auto-fetches/transforms anything missing or
     incomplete.
  2. PA validation — compares mlb.boxscore_batting against the MLB API
     boxscore endpoint for recent games.
  3. Writes a summary row to mlb.pipeline_integrity.
  4. Emails an alert if anything was missing or mismatched.

Usage:
    PYTHONPATH=/app:/pipeline python3 /pipeline/mlb/build_integrity_check.py --lookback 30
"""

import sys
import os
import json
import argparse
import httpx
from datetime import date, timedelta
from sqlalchemy import text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'api'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from database import SessionLocal

MLB_BASE = "https://statsapi.mlb.com/api/v1"
MIN_RAW_EVENTS = 10


def _force_fetch_and_transform(game_pk: int, db) -> bool:
    """Delete any stale/incomplete raw row so fetch.fetch_game's 7-day skip
    logic can't short-circuit the re-fetch, then fetch + transform fresh."""
    from mlb.fetch import fetch_game
    from mlb.transform import transform_game_pk
    from models.mlb import MLBRawEvent

    db.query(MLBRawEvent).filter(MLBRawEvent.game_pk == game_pk).delete()
    db.commit()

    action = fetch_game(game_pk, db)
    if action == "error":
        return False

    transform_game_pk(game_pk, db)
    return True


def find_and_fix_missing_games(db, lookback_days: int = 30) -> dict:
    """
    Fetch MLB API schedule for last lookback_days days, compare to our DB.
    Auto-fetch and transform any missing or incomplete games.
    Returns summary dict with counts.
    """
    today = date.today()
    start = today - timedelta(days=lookback_days)

    games_checked = 0
    to_fix = []

    d = start
    while d <= today:
        date_str = d.strftime("%Y-%m-%d")
        try:
            res = httpx.get(
                f"{MLB_BASE}/schedule",
                params={"sportId": 1, "gameType": "R,S,P", "date": date_str},
                timeout=15,
            )
            res.raise_for_status()
            data = res.json()
        except Exception as e:
            print(f"    ✗ Schedule fetch failed for {date_str}: {e}")
            d += timedelta(days=1)
            continue

        for dd in data.get("dates", []):
            for g in dd.get("games", []):
                if g.get("status", {}).get("detailedState") != "Final":
                    continue
                games_checked += 1
                game_pk = g["gamePk"]
                away = g.get("teams", {}).get("away", {}).get("team", {}).get("abbreviation", "?")
                home = g.get("teams", {}).get("home", {}).get("team", {}).get("abbreviation", "?")
                matchup = f"{away} @ {home}"

                game_row = db.execute(text(
                    "SELECT 1 FROM mlb.games WHERE game_pk = :pk"
                ), {"pk": game_pk}).first()

                raw_row = db.execute(text(
                    "SELECT data FROM mlb.raw_events WHERE game_pk = :pk"
                ), {"pk": game_pk}).first()

                if not game_row and not raw_row:
                    to_fix.append((game_pk, date_str, matchup, "missing_from_db"))
                elif not raw_row:
                    to_fix.append((game_pk, date_str, matchup, "missing_raw_data"))
                else:
                    plays = (raw_row[0] or {}).get("liveData", {}).get("plays", {}).get("allPlays", [])
                    if len(plays) < MIN_RAW_EVENTS:
                        to_fix.append((game_pk, date_str, matchup, "incomplete_raw_events"))
                    elif not game_row:
                        to_fix.append((game_pk, date_str, matchup, "missing_from_db"))

        d += timedelta(days=1)

    print(f"    Checked {games_checked} Final games over last {lookback_days} days")
    print(f"    {len(to_fix)} games need fetch/transform")

    fixed, errors = [], []
    for game_pk, date_str, matchup, reason in to_fix:
        print(f"    Fixing {game_pk} ({matchup}, {date_str}) — {reason}")
        try:
            ok = _force_fetch_and_transform(game_pk, db)
        except Exception as e:
            ok = False
            print(f"      ✗ error: {e}")

        entry = {"game_pk": game_pk, "game_date": date_str, "matchup": matchup, "reason": reason}
        if ok:
            fixed.append(entry)
        else:
            errors.append(entry)

    return {
        "games_checked": games_checked,
        "games_missing": len(to_fix),
        "games_fixed": len(fixed),
        "games_errored": len(errors),
        "fixed_details": fixed,
        "error_details": errors,
    }


def validate_pa_vs_api(db, lookback_days: int = 7) -> list:
    """
    For each Final game in the last lookback_days, fetch the MLB API boxscore
    and compare total plateAppearances to our mlb.boxscore_batting.

    We compare against the live API, not our own mlb.raw_events — if a game
    was fetched mid-game, raw_events would be just as incomplete as
    boxscore_batting, and comparing the two would false-pass. The live API
    boxscore endpoint is lightweight (summary stats only, not full
    play-by-play) and always reflects the final, complete state for a
    Final game.

    Any mismatch is fixed immediately: delete raw_events, re-fetch, re-transform.
    Returns list of discrepancies: {game_pk, game_date, our_pa, api_pa, diff, fixed}
    """
    today = date.today()
    start = today - timedelta(days=lookback_days)

    games = db.execute(text("""
        SELECT game_pk, game_date
        FROM mlb.games
        WHERE game_date >= :start AND game_date <= :today
          AND game_type = 'R'
          AND status = 'Final'
        ORDER BY game_date
    """), {"start": start.strftime("%Y-%m-%d"), "today": today.strftime("%Y-%m-%d")}).mappings().all()

    discrepancies = []
    for g in games:
        game_pk = g["game_pk"]

        our_pa = db.execute(text("""
            SELECT COALESCE(SUM(plate_appearances), 0)
            FROM mlb.boxscore_batting
            WHERE game_pk = :pk
        """), {"pk": game_pk}).scalar() or 0

        try:
            res = httpx.get(f"{MLB_BASE}/game/{game_pk}/boxscore", timeout=15)
            res.raise_for_status()
            box = res.json()
        except Exception as e:
            print(f"    ✗ Boxscore fetch failed for {game_pk}: {e}")
            continue

        api_pa = 0
        for side in ("away", "home"):
            players = box.get("teams", {}).get(side, {}).get("players", {})
            for player in players.values():
                api_pa += player.get("stats", {}).get("batting", {}).get("plateAppearances") or 0

        diff = int(our_pa) - api_pa
        if diff == 0:
            continue

        print(f"    ⚠️  game_pk={game_pk} {g['game_date']}: "
              f"api={api_pa} ours={our_pa} diff={diff} — re-fetching...")
        try:
            fixed = _force_fetch_and_transform(game_pk, db)
        except Exception as e:
            fixed = False
            print(f"      ✗ re-fetch error: {e}")

        if fixed:
            print(f"    ✅ Fixed game_pk={game_pk}")

        discrepancies.append({
            "game_pk": game_pk,
            "game_date": g["game_date"],
            "our_pa": int(our_pa),
            "api_pa": api_pa,
            "diff": diff,
            "fixed": fixed,
        })

    return discrepancies


def write_integrity_results(db, check_date: str, missing_summary: dict, pa_discrepancies: list):
    """Create mlb.pipeline_integrity if needed and upsert today's results."""
    games_checked = missing_summary["games_checked"]
    games_missing = missing_summary["games_missing"]
    games_fixed = missing_summary["games_fixed"]
    games_errored = missing_summary["games_errored"]
    pa_mismatches = len(pa_discrepancies)

    status = "ok"
    notes = []
    if games_errored:
        status = "error"
        notes.append(f"{games_errored} games failed to fetch/transform")
    if games_missing:
        if status == "ok":
            status = "warning"
        notes.append(f"{games_missing} games missing/incomplete, {games_fixed} fixed")
    if pa_mismatches:
        if status == "ok":
            status = "warning"
        notes.append(f"{pa_mismatches} PA count mismatches")
    if not notes:
        notes.append("All checks passed")

    db.execute(text("""
        CREATE TABLE IF NOT EXISTS mlb.pipeline_integrity (
            check_date DATE NOT NULL,
            check_type VARCHAR(50) NOT NULL,
            games_checked INTEGER,
            games_missing INTEGER,
            games_fixed INTEGER,
            pa_mismatches INTEGER,
            pa_mismatch_details JSONB,
            status VARCHAR(20),
            notes TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (check_date, check_type)
        )
    """))

    db.execute(text("""
        INSERT INTO mlb.pipeline_integrity
            (check_date, check_type, games_checked, games_missing, games_fixed,
             pa_mismatches, pa_mismatch_details, status, notes, created_at)
        VALUES
            (:check_date, 'nightly', :games_checked, :games_missing, :games_fixed,
             :pa_mismatches, CAST(:pa_details AS JSONB), :status, :notes, NOW())
        ON CONFLICT (check_date, check_type) DO UPDATE SET
            games_checked = EXCLUDED.games_checked,
            games_missing = EXCLUDED.games_missing,
            games_fixed = EXCLUDED.games_fixed,
            pa_mismatches = EXCLUDED.pa_mismatches,
            pa_mismatch_details = EXCLUDED.pa_mismatch_details,
            status = EXCLUDED.status,
            notes = EXCLUDED.notes,
            created_at = NOW()
    """), {
        "check_date": check_date,
        "games_checked": games_checked,
        "games_missing": games_missing,
        "games_fixed": games_fixed,
        "pa_mismatches": pa_mismatches,
        "pa_details": json.dumps(pa_discrepancies, default=str),
        "status": status,
        "notes": " · ".join(notes),
    })
    db.commit()
    return status, notes


def main():
    parser = argparse.ArgumentParser(description="MLB pipeline integrity check")
    parser.add_argument("--lookback", type=int, default=None,
                         help="Days to look back for missing/incomplete games (default 30). "
                              "Also drives the PA validation lookback unless --pa-lookback is set — "
                              "pass this alone (e.g. --lookback 150) for a full backfill pass.")
    parser.add_argument("--pa-lookback", type=int, default=None,
                         help="Days to look back for PA validation (default: same as --lookback "
                              "if given, otherwise 7)")
    args = parser.parse_args()

    missing_lookback = args.lookback if args.lookback is not None else 30
    pa_lookback = args.pa_lookback if args.pa_lookback is not None else (
        args.lookback if args.lookback is not None else 7)

    check_date = date.today().strftime("%Y-%m-%d")
    db = SessionLocal()
    try:
        print(f"\n{'='*55}")
        print(f"  MLB Pipeline Integrity Check — {check_date}")
        print(f"{'='*55}")

        print(f"\n  Step 1: Game Completeness (last {missing_lookback} days)")
        missing_summary = find_and_fix_missing_games(db, missing_lookback)
        print(f"    Missing/incomplete: {missing_summary['games_missing']}")
        print(f"    Fixed:              {missing_summary['games_fixed']}")
        if missing_summary["games_errored"]:
            print(f"    Errors:             {missing_summary['games_errored']}")

        print(f"\n  Step 2: PA Validation vs live API (last {pa_lookback} days)")
        pa_discrepancies = validate_pa_vs_api(db, pa_lookback)
        print(f"    Mismatches found: {len(pa_discrepancies)}")
        for disc in pa_discrepancies:
            print(f"      {disc['game_date']} game {disc['game_pk']}: "
                  f"ours={disc['our_pa']} api={disc['api_pa']} diff={disc['diff']} "
                  f"({'fixed' if disc['fixed'] else 'FIX FAILED'})")

        print(f"\n  Step 3: Writing results to mlb.pipeline_integrity")
        status, notes = write_integrity_results(db, check_date, missing_summary, pa_discrepancies)
        print(f"    Status: {status}")
        for n in notes:
            print(f"      · {n}")

        if missing_summary["games_missing"] > 0 or pa_discrepancies:
            print(f"\n  Step 4: Sending alert email")
            from send_email import send_pipeline_integrity_alert
            send_pipeline_integrity_alert(
                fixed_games=missing_summary.get("fixed_details", []),
                error_games=missing_summary.get("error_details", []),
                pa_discrepancies=pa_discrepancies,
            )
        else:
            print(f"\n  No issues found — skipping alert email")

        print(f"\n{'='*55}\n")
    finally:
        db.close()


if __name__ == "__main__":
    main()
