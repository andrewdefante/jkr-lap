"""
MLB bulk fetch script.

Pulls game schedules from the MLB Stats API and stores raw GUMBO
data in mlb.raw_events for all Final games.

Usage:
    # Fetch a single season
    PYTHONPATH=/app python3 /pipeline/mlb/fetch.py --season 2024

    # Fetch multiple seasons
    PYTHONPATH=/app python3 /pipeline/mlb/fetch.py --season 2022 2023 2024

    # Fetch and immediately transform
    PYTHONPATH=/app python3 /pipeline/mlb/fetch.py --season 2024 --transform

    # Spring training
    PYTHONPATH=/app python3 /pipeline/mlb/fetch.py --season 2026 --game-type S

    # Dry run - just count games, don't fetch
    PYTHONPATH=/app python3 /pipeline/mlb/fetch.py --season 2024 --dry-run
"""

import sys
import os
import argparse
import time
import httpx

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'api'))

from database import SessionLocal
from models.mlb import MLBRawEvent
from datetime import datetime

MLB_BASE = "https://statsapi.mlb.com/api/v1"
GUMBO_BASE = "https://statsapi.mlb.com/api/v1.1"

# Game types:
# R = Regular season
# S = Spring training
# P = Playoffs
# W = World Series
GAME_TYPES = {"R": "Regular Season", "S": "Spring Training", "P": "Playoffs"}


def get_schedule(season: int, game_type: str = "R") -> list[dict]:
    """Fetch full schedule for a season and game type. Returns list of game dicts."""
    url = f"{MLB_BASE}/schedule"
    params = {
        "sportId": 1,
        "season": season,
        "gameType": game_type,
        "fields": "dates,date,games,gamePk,gameType,status,detailedState,teams,away,home,team,abbreviation,name"
    }

    print(f"  Fetching {GAME_TYPES.get(game_type, game_type)} schedule for {season}...")
    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, params=params)
        response.raise_for_status()

    data = response.json()
    games = []
    for date in data.get("dates", []):
        for game in date.get("games", []):
            games.append({
                "game_pk": game["gamePk"],
                "game_date": date["date"],
                "status": game.get("status", {}).get("detailedState"),
                "away": game.get("teams", {}).get("away", {}).get("team", {}).get("abbreviation"),
                "home": game.get("teams", {}).get("home", {}).get("team", {}).get("abbreviation"),
            })

    final_games = [g for g in games if g["status"] == "Final"]
    print(f"  Found {len(games)} total games, {len(final_games)} Final")
    return final_games


def get_full_schedule(season: int, game_type: str = "R") -> list[dict]:
    """Fetch full schedule with all statuses and current scores (one API call)."""
    url = f"{MLB_BASE}/schedule"
    params = {
        "sportId": 1,
        "season": season,
        "gameType": game_type,
        "fields": "dates,date,games,gamePk,gameType,status,detailedState,teams,away,home,team,abbreviation,name,score"
    }

    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, params=params)
        response.raise_for_status()

    data = response.json()
    games = []
    for date in data.get("dates", []):
        for game in date.get("games", []):
            teams = game.get("teams", {})
            games.append({
                "game_pk": game["gamePk"],
                "game_date": date["date"],
                "status": game.get("status", {}).get("detailedState"),
                "away": teams.get("away", {}).get("team", {}).get("abbreviation"),
                "home": teams.get("home", {}).get("team", {}).get("abbreviation"),
                "away_score": teams.get("away", {}).get("score"),
                "home_score": teams.get("home", {}).get("score"),
            })
    return games


def update_statuses(season: int, game_type: str = "R", db=None) -> dict:
    """
    Update status and scores in mlb.games from the schedule API without re-fetching GUMBO.
    Fast: one API call covers all games for the season.
    """
    from sqlalchemy import text

    print(f"  Fetching {GAME_TYPES.get(game_type, game_type)} schedule for {season}...")
    games = get_full_schedule(season, game_type)
    print(f"  Found {len(games)} games — checking for status changes...")

    update_sql = text("""
        UPDATE mlb.games
        SET status     = :status,
            home_score = :home_score,
            away_score = :away_score
        WHERE game_pk = :game_pk
          AND (status IS DISTINCT FROM :status
               OR home_score IS DISTINCT FROM :home_score
               OR away_score IS DISTINCT FROM :away_score)
    """)

    updated = 0
    for game in games:
        result = db.execute(update_sql, {
            "game_pk":    game["game_pk"],
            "status":     game["status"],
            "home_score": game["home_score"],
            "away_score": game["away_score"],
        })
        updated += result.rowcount

    db.commit()
    print(f"  Updated {updated} games (status/score changed)")
    return {"total": len(games), "updated": updated}


def fetch_game(game_pk: int, db) -> str:
    """
    Fetch a single game from GUMBO and upsert into mlb.raw_events.
    Returns action: 'inserted', 'updated', 'skipped', or 'error'
    """
    # Skip if already stored
    existing = db.query(MLBRawEvent).filter(MLBRawEvent.game_pk == game_pk).first()
    if existing and existing.status == "Final":
        from datetime import datetime, timedelta, timezone
        if existing.updated_at:
            from datetime import datetime as _dt, timedelta, timezone
            age = _dt.now(timezone.utc) - existing.updated_at.replace(tzinfo=timezone.utc)
            if age.days >= 7:
                return "skipped"

    url = f"{GUMBO_BASE}/game/{game_pk}/feed/live"
    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.get(url)
            response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"    ✗ {game_pk}: {e}")
        return "error"

    gd = data.get("gameData", {})
    game_date = gd.get("datetime", {}).get("originalDate")
    status = gd.get("status", {}).get("detailedState")
    away = gd.get("teams", {}).get("away", {}).get("abbreviation")
    home = gd.get("teams", {}).get("home", {}).get("abbreviation")

    if existing:
        existing.data = data
        existing.status = status
        existing.updated_at = datetime.utcnow()
        db.commit()
        return "updated"
    else:
        raw = MLBRawEvent(
            game_pk=game_pk,
            game_date=game_date,
            status=status,
            away_team=away,
            home_team=home,
            endpoint=url,
            data=data,
        )
        db.add(raw)
        db.commit()
        return "inserted"


def fetch_season(
    season: int,
    game_type: str = "R",
    transform: bool = False,
    delay: float = 0.5,
    db=None
):
    """Fetch all Final games for a season."""
    games = get_schedule(season, game_type)
    if not games:
        print("  No games found.")
        return

    counts = {"inserted": 0, "updated": 0, "skipped": 0, "error": 0}

    for i, game in enumerate(games, 1):
        game_pk = game["game_pk"]
        action = fetch_game(game_pk, db)
        counts[action] += 1

        if action in ("inserted", "updated"):
            print(f"  [{i}/{len(games)}] {action}: {game_pk} "
                  f"({game['away']} @ {game['home']}) {game['game_date']}")
            time.sleep(delay)  # be polite to the MLB API
        elif action == "error":
            print(f"  [{i}/{len(games)}] ERROR: {game_pk}")
        else:
            # Print progress every 100 skipped games
            if i % 100 == 0:
                print(f"  [{i}/{len(games)}] {counts['skipped']} skipped (already stored)...")

    print(f"\n  Season {season} complete:")
    print(f"    inserted: {counts['inserted']}")
    print(f"    updated:  {counts['updated']}")
    print(f"    skipped:  {counts['skipped']}")
    print(f"    errors:   {counts['error']}")

    if transform:
        print(f"\n  Running transform for new/updated games...")
        from mlb.transform import transform_game_pk
        raw_games = db.query(MLBRawEvent).filter(
            MLBRawEvent.status == "Final"
        ).all()
        for raw in raw_games:
            transform_game_pk(raw.game_pk, db)


def main():
    parser = argparse.ArgumentParser(description="Fetch MLB game data from GUMBO")
    parser.add_argument("--season", type=int, nargs="+", required=True,
                        help="Season year(s) e.g. --season 2022 2023 2024")
    parser.add_argument("--game-type", default="R", choices=["R", "S", "P"],
                        help="R=Regular, S=Spring Training, P=Playoffs (default: R)")
    parser.add_argument("--transform", action="store_true",
                        help="Run transform after fetching")
    parser.add_argument("--delay", type=float, default=0.5,
                        help="Seconds between API calls (default: 0.5)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Count games without fetching")
    parser.add_argument("--update-status", action="store_true",
                        help="Update status/scores for existing games without re-fetching GUMBO")
    args = parser.parse_args()

    if args.update_status:
        db = SessionLocal()
        try:
            for season in args.season:
                print(f"\n=== {season} MLB {GAME_TYPES.get(args.game_type)} — Status Update ===")
                update_statuses(season, args.game_type, db)
            print("\n=== Status update complete ===")
        finally:
            db.close()
        return

    if args.dry_run:
        for season in args.season:
            games = get_schedule(season, args.game_type)
            print(f"  {season}: {len(games)} Final games to fetch")
        return

    db = SessionLocal()
    try:
        for season in args.season:
            print(f"\n=== {season} MLB {GAME_TYPES.get(args.game_type)} ===")
            fetch_season(
                season=season,
                game_type=args.game_type,
                transform=args.transform,
                delay=args.delay,
                db=db,
            )
        print("\n=== All done ===")
    finally:
        db.close()


if __name__ == "__main__":
    main()