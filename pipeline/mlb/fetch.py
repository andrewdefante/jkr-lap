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

# team_id -> abbreviation, used only to populate home/away_team_abbrev when
# update_statuses() creates a new mlb.games row from schedule data (which has
# no abbreviation field). transform_game() overwrites these with the real
# GUMBO-sourced abbreviation once the game is Final and fully fetched.
TEAM_ID_TO_ABBREV = {
    108: 'LAA', 109: 'AZ',  110: 'BAL', 111: 'BOS', 112: 'CHC',
    113: 'CIN', 114: 'CLE', 115: 'COL', 116: 'DET', 117: 'HOU',
    118: 'KC',  119: 'LAD', 120: 'WSH', 121: 'NYM', 133: 'ATH',
    134: 'PIT', 135: 'SD',  136: 'SEA', 137: 'SF',  138: 'STL',
    139: 'TB',  140: 'TEX', 141: 'TOR', 142: 'MIN', 143: 'PHI',
    144: 'ATL', 145: 'CWS', 146: 'MIA', 147: 'NYY', 158: 'MIL',
}


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
    """Fetch full schedule with all statuses, current scores, and probable
    pitchers (one API call). hydrate=probablePitcher is required for MLB's
    schedule endpoint to include teams.{home,away}.probablePitcher — without
    it, those keys are simply absent from the response."""
    url = f"{MLB_BASE}/schedule"
    params = {
        "sportId": 1,
        "season": season,
        "gameType": game_type,
        "hydrate": "probablePitcher",
        "fields": "dates,date,games,gamePk,gameType,gameDate,status,detailedState,teams,"
                  "away,home,team,id,name,score,probablePitcher,fullName"
    }

    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, params=params)
        response.raise_for_status()

    data = response.json()
    games = []
    for date in data.get("dates", []):
        for game in date.get("games", []):
            teams = game.get("teams", {})
            away_team = teams.get("away", {}).get("team", {})
            home_team = teams.get("home", {}).get("team", {})
            away_probable = teams.get("away", {}).get("probablePitcher") or {}
            home_probable = teams.get("home", {}).get("probablePitcher") or {}
            games.append({
                "game_pk": game["gamePk"],
                "game_date": date["date"],
                "game_time_utc": game.get("gameDate"),
                "status": game.get("status", {}).get("detailedState"),
                "away_team_id": away_team.get("id"),
                "away_team_name": away_team.get("name"),
                "home_team_id": home_team.get("id"),
                "home_team_name": home_team.get("name"),
                "away_score": teams.get("away", {}).get("score"),
                "home_score": teams.get("home", {}).get("score"),
                "away_probable_pitcher_id": away_probable.get("id"),
                "away_probable_pitcher_name": away_probable.get("fullName"),
                "home_probable_pitcher_id": home_probable.get("id"),
                "home_probable_pitcher_name": home_probable.get("fullName"),
            })
    return games


def update_statuses(season: int, game_type: str = "R", db=None) -> dict:
    """
    Upsert status, scores, and probable pitchers into mlb.games from the
    schedule API without re-fetching GUMBO. Fast: one API call covers all
    games for the season.

    This is an upsert (not a plain UPDATE) because mlb.games only gains a
    row via GUMBO fetch+transform, which only happens for Final games —
    scheduled/in-progress games otherwise have no row at all. Probable
    pitchers are only useful pre-game, so this must be able to create the
    row. On conflict, only status/scores/probable-pitcher fields are
    touched — venue, weather, decisions, etc. are GUMBO-only and get set
    later by transform_game() once the game is Final and fully fetched.
    """
    from sqlalchemy import text

    print(f"  Fetching {GAME_TYPES.get(game_type, game_type)} schedule for {season}...")
    games = get_full_schedule(season, game_type)
    print(f"  Found {len(games)} games — upserting status/scores/probable pitchers...")

    upsert_sql = text("""
        INSERT INTO mlb.games (
            game_pk, game_date, game_type, season, status,
            home_team_id, home_team_name, home_team_abbrev,
            away_team_id, away_team_name, away_team_abbrev,
            home_score, away_score,
            home_probable_pitcher_id, home_probable_pitcher_name,
            away_probable_pitcher_id, away_probable_pitcher_name,
            game_time_utc
        ) VALUES (
            :game_pk, :game_date, :game_type, :season, :status,
            :home_team_id, :home_team_name, :home_team_abbrev,
            :away_team_id, :away_team_name, :away_team_abbrev,
            :home_score, :away_score,
            :home_probable_pitcher_id, :home_probable_pitcher_name,
            :away_probable_pitcher_id, :away_probable_pitcher_name,
            :game_time_utc
        )
        ON CONFLICT (game_pk) DO UPDATE SET
            status = EXCLUDED.status,
            home_score = EXCLUDED.home_score,
            away_score = EXCLUDED.away_score,
            home_probable_pitcher_id = EXCLUDED.home_probable_pitcher_id,
            home_probable_pitcher_name = EXCLUDED.home_probable_pitcher_name,
            away_probable_pitcher_id = EXCLUDED.away_probable_pitcher_id,
            away_probable_pitcher_name = EXCLUDED.away_probable_pitcher_name,
            game_time_utc = EXCLUDED.game_time_utc,
            updated_at = NOW()
        WHERE mlb.games.status IS DISTINCT FROM EXCLUDED.status
           OR mlb.games.home_score IS DISTINCT FROM EXCLUDED.home_score
           OR mlb.games.away_score IS DISTINCT FROM EXCLUDED.away_score
           OR mlb.games.home_probable_pitcher_id IS DISTINCT FROM EXCLUDED.home_probable_pitcher_id
           OR mlb.games.away_probable_pitcher_id IS DISTINCT FROM EXCLUDED.away_probable_pitcher_id
           OR mlb.games.game_time_utc IS DISTINCT FROM EXCLUDED.game_time_utc
    """)

    updated = 0
    for game in games:
        result = db.execute(upsert_sql, {
            "game_pk":    game["game_pk"],
            "game_date":  game["game_date"],
            "game_type":  game_type,
            "season":     season,
            "status":     game["status"],
            "home_team_id":   game["home_team_id"],
            "home_team_name": game["home_team_name"],
            "home_team_abbrev": TEAM_ID_TO_ABBREV.get(game["home_team_id"]),
            "away_team_id":   game["away_team_id"],
            "away_team_name": game["away_team_name"],
            "away_team_abbrev": TEAM_ID_TO_ABBREV.get(game["away_team_id"]),
            "home_score": game["home_score"],
            "away_score": game["away_score"],
            "home_probable_pitcher_id":   game["home_probable_pitcher_id"],
            "home_probable_pitcher_name": game["home_probable_pitcher_name"],
            "away_probable_pitcher_id":   game["away_probable_pitcher_id"],
            "away_probable_pitcher_name": game["away_probable_pitcher_name"],
            "game_time_utc": game["game_time_utc"],
        })
        updated += result.rowcount

    db.commit()
    print(f"  Upserted {updated} games (status/score/probable-pitcher changed or new)")
    return {"total": len(games), "updated": updated}


def fetch_game(game_pk: int, db) -> str:
    """
    Fetch a single game from GUMBO and upsert into mlb.raw_events.
    Returns action: 'inserted', 'updated', 'skipped', or 'error'
    """
    # Skip if already stored
    existing = db.query(MLBRawEvent).filter(MLBRawEvent.game_pk == game_pk).first()
    if existing and existing.status == "Final":
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