"""
Fetch today's posted lineups from MLB Stats API.
Polls every 30 minutes during day, stores to mlb.game_lineups.
Only stores lineups once they're posted (homePlayers/awayPlayers present).
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import httpx
import json
from datetime import date
from database import SessionLocal
from sqlalchemy import text

MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"


def fetch_lineups_for_date(game_date: str, db) -> dict:
    """
    Fetch all lineups for a given date.
    Returns dict of game_pk → {home: [...], away: [...], home_pitcher, away_pitcher}
    """
    r = httpx.get(MLB_SCHEDULE_URL, params={
        "sportId": 1,
        "date": game_date,
        "hydrate": "lineups,probablePitcher",
    }, timeout=15)
    r.raise_for_status()
    data = r.json()

    results = {}
    for date_entry in data.get("dates", []):
        for game in date_entry.get("games", []):
            game_pk = game["gamePk"]
            lineups = game.get("lineups", {})
            home_players = lineups.get("homePlayers", [])
            away_players = lineups.get("awayPlayers", [])

            if not home_players and not away_players:
                continue  # lineup not posted yet

            home_pitcher = game["teams"]["home"].get("probablePitcher", {})
            away_pitcher = game["teams"]["away"].get("probablePitcher", {})

            results[game_pk] = {
                "game_pk": game_pk,
                "game_date": game_date,
                "home_team_id": game["teams"]["home"]["team"]["id"],
                "away_team_id": game["teams"]["away"]["team"]["id"],
                "home_lineup": [
                    {
                        "mlbam_id": p["id"],
                        "name": p["fullName"],
                        "position": p.get("primaryPosition", {}).get("abbreviation"),
                    }
                    for p in home_players
                ],
                "away_lineup": [
                    {
                        "mlbam_id": p["id"],
                        "name": p["fullName"],
                        "position": p.get("primaryPosition", {}).get("abbreviation"),
                    }
                    for p in away_players
                ],
                "home_pitcher_id": home_pitcher.get("id"),
                "home_pitcher_name": home_pitcher.get("fullName"),
                "away_pitcher_id": away_pitcher.get("id"),
                "away_pitcher_name": away_pitcher.get("fullName"),
                "status": game["status"]["detailedState"],
            }

    return results


def store_lineups(lineups: dict, db) -> int:
    """Create table if needed, then upsert all lineups."""
    db.execute(text("""
        CREATE TABLE IF NOT EXISTS mlb.game_lineups (
            id BIGSERIAL PRIMARY KEY,
            game_pk INTEGER NOT NULL,
            game_date DATE NOT NULL,
            home_team_id INTEGER,
            away_team_id INTEGER,
            home_lineup JSONB,
            away_lineup JSONB,
            home_pitcher_id INTEGER,
            home_pitcher_name VARCHAR(100),
            away_pitcher_id INTEGER,
            away_pitcher_name VARCHAR(100),
            status VARCHAR(50),
            lineup_posted BOOLEAN DEFAULT FALSE,
            fetched_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(game_pk)
        )
    """))

    upsert = text("""
        INSERT INTO mlb.game_lineups (
            game_pk, game_date, home_team_id, away_team_id,
            home_lineup, away_lineup,
            home_pitcher_id, home_pitcher_name,
            away_pitcher_id, away_pitcher_name,
            status, lineup_posted, fetched_at
        ) VALUES (
            :game_pk, :game_date, :home_team_id, :away_team_id,
            :home_lineup, :away_lineup,
            :home_pitcher_id, :home_pitcher_name,
            :away_pitcher_id, :away_pitcher_name,
            :status, TRUE, NOW()
        )
        ON CONFLICT (game_pk) DO UPDATE SET
            home_lineup       = EXCLUDED.home_lineup,
            away_lineup       = EXCLUDED.away_lineup,
            home_pitcher_id   = EXCLUDED.home_pitcher_id,
            away_pitcher_id   = EXCLUDED.away_pitcher_id,
            status            = EXCLUDED.status,
            lineup_posted     = TRUE,
            fetched_at        = NOW()
    """)

    stored = 0
    for game_pk, d in lineups.items():
        db.execute(upsert, {
            "game_pk": game_pk,
            "game_date": d["game_date"],
            "home_team_id": d["home_team_id"],
            "away_team_id": d["away_team_id"],
            "home_lineup": json.dumps(d["home_lineup"]),
            "away_lineup": json.dumps(d["away_lineup"]),
            "home_pitcher_id": d["home_pitcher_id"],
            "home_pitcher_name": d["home_pitcher_name"],
            "away_pitcher_id": d["away_pitcher_id"],
            "away_pitcher_name": d["away_pitcher_name"],
            "status": d["status"],
        })
        stored += 1
    db.commit()
    return stored


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=str(date.today()))
    args = parser.parse_args()

    db = SessionLocal()
    try:
        print(f"Fetching lineups for {args.date}...")
        lineups = fetch_lineups_for_date(args.date, db)
        n = store_lineups(lineups, db)
        print(f"  Stored {n} lineups")
        for gp, d in lineups.items():
            print(f"  Game {gp}: {d['away_pitcher_name']} vs {d['home_pitcher_name']} "
                  f"| Lineups: {len(d['away_lineup'])}/{len(d['home_lineup'])}")
    finally:
        db.close()
