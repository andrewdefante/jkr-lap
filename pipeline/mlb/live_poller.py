"""
MLB Live Game Poller

Polls active games every N minutes and stores pitch-by-pitch data.
Automatically detects live games — only runs when games are in progress.
Computes rolling BAPV+ per pitcher updated after each half-inning.

Usage:
    # Poll all live games today
    PYTHONPATH=/app:/pipeline python3 /pipeline/mlb/live_poller.py

    # Poll a specific game
    PYTHONPATH=/app:/pipeline python3 /pipeline/mlb/live_poller.py --game-pk 823244

    # Custom interval (seconds)
    PYTHONPATH=/app:/pipeline python3 /pipeline/mlb/live_poller.py --interval 120
"""

import sys
import os
import time
import argparse
import httpx
from datetime import datetime, timezone, date, timedelta
from sqlalchemy import text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'api'))
from database import SessionLocal

POLL_INTERVAL = 120  # seconds
MLB_SCHEDULE  = "https://statsapi.mlb.com/api/v1/schedule"
MLB_LIVE_FEED = "https://statsapi.mlb.com/api/v1.1/game/{game_pk}/feed/live"


def get_live_games(target_date: str = None) -> list:
    """Fetch all in-progress games for today and yesterday (UTC-safe)."""
    dates_to_check = (
        [target_date] if target_date
        else [date.today().isoformat(), (date.today() - timedelta(days=1)).isoformat()]
    )
    seen = set()
    games = []
    for d in dates_to_check:
        try:
            res = httpx.get(
                MLB_SCHEDULE,
                params={"sportId": 1, "date": d, "gameType": "R,S,E"},
                timeout=15
            )
            data = res.json()
            for day in data.get("dates", []):
                for g in day.get("games", []):
                    state = g["status"]["detailedState"]
                    pk = g["gamePk"]
                    if pk in seen:
                        continue
                    if state in ("In Progress", "Manager challenge", "Delayed"):
                        seen.add(pk)
                        games.append({
                            "game_pk":      pk,
                            "away":         g["teams"]["away"]["team"]["name"],
                            "home":         g["teams"]["home"]["team"]["name"],
                            "state":        state,
                            "inning":       g.get("linescore", {}).get("currentInning"),
                            "inning_state": g.get("linescore", {}).get("inningState"),
                        })
        except Exception as e:
            print(f"  Schedule fetch error for {d}: {e}")
    return games


def get_completed_games(target_date: str = None) -> list:
    """Fetch all Final games for today and yesterday — for post-game processing."""
    dates_to_check = (
        [target_date] if target_date
        else [date.today().isoformat(), (date.today() - timedelta(days=1)).isoformat()]
    )
    seen = set()
    games = []
    for d in dates_to_check:
        try:
            res = httpx.get(
                MLB_SCHEDULE,
                params={"sportId": 1, "date": d, "gameType": "R,S,E"},
                timeout=15
            )
            data = res.json()
            for day in data.get("dates", []):
                for g in day.get("games", []):
                    pk = g["gamePk"]
                    if pk not in seen and g["status"]["detailedState"] == "Final":
                        seen.add(pk)
                        games.append(pk)
        except Exception as e:
            print(f"  Schedule fetch error for {d}: {e}")
    return games


def fetch_live_feed(game_pk: int) -> dict:
    """Fetch current GUMBO state for a game."""
    try:
        res = httpx.get(
            MLB_LIVE_FEED.format(game_pk=game_pk),
            timeout=20
        )
        return res.json()
    except Exception as e:
        print(f"  Feed fetch error for {game_pk}: {e}")
        return {}


def store_raw_event(game_pk: int, data: dict, db) -> bool:
    """Store/update raw GUMBO event. Returns True if game state changed."""
    from models.mlb import MLBRawEvent

    status = data.get("gameData", {}).get("status", {}).get("detailedState", "")
    game_date = data.get("gameData", {}).get("datetime", {}).get("originalDate", "")
    teams = data.get("gameData", {}).get("teams", {})
    away = teams.get("away", {}).get("abbreviation", "")
    home = teams.get("home", {}).get("abbreviation", "")

    existing = db.query(MLBRawEvent).filter(
        MLBRawEvent.game_pk == game_pk
    ).first()

    import json
    if existing:
        prev_status = existing.status
        existing.data = data
        existing.status = status
        existing.updated_at = datetime.now(timezone.utc)
        db.commit()
        return prev_status != status
    else:
        db.add(MLBRawEvent(
            game_pk=game_pk,
            game_date=game_date,
            status=status,
            away_team=away,
            home_team=home,
            data=data,
        ))
        db.commit()
        return True

def get_current_pitcher_stats(game_pk: int, db) -> list:
    """
    Get rolling BAPV+ for pitchers in this game so far today.
    Used for live display.
    """
    sql = text("""
        SELECT
            p.pitcher_id,
            ab.pitcher_name,
            p.pitch_type_code,
            COUNT(*) as pitches,
            ROUND(AVG(CASE WHEN p.call_code IN ('S','W','T','C')
                THEN 1.0 ELSE 0.0 END)::numeric, 3) as csw_rate,
            ROUND(AVG(CASE WHEN p.call_code IN ('S','W','T')
                THEN 1.0 ELSE 0.0 END)::numeric, 3) as whiff_rate,
            ROUND(AVG(p.start_speed)::numeric, 1) as avg_velo,
            ROUND(AVG(p.spin_rate)::numeric, 0) as avg_spin
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        WHERE p.game_pk = :game_pk
        AND p.pitch_type_code IS NOT NULL
        GROUP BY p.pitcher_id, ab.pitcher_name, p.pitch_type_code
        HAVING COUNT(*) >= 5
        ORDER BY pitches DESC
    """)
    rows = db.execute(sql, {"game_pk": game_pk}).mappings().all()
    return [dict(r) for r in rows]


def transform_and_score(game_pk: int, db) -> int:
    """Transform raw GUMBO and compute BAPV for completed game."""
    try:
        sys.path.insert(0, '/pipeline')
        from mlb.transform import transform_game_pk
        transform_game_pk(game_pk, db)

        # Compute BAPV for this game
        from mlb.compute_bapv import (
            load_tendencies, load_pitches,
            compute_bapv_vectorized, aggregate_per_game,
            normalize_bapv_plus, store_scores
        )

        season = datetime.now().year
        type_lookup, zone_lookup, weights, league_avg = load_tendencies(
            season - 1, db  # use prior season tendencies
        )
        pitches = load_pitches(season, db, game_pk=game_pk)
        if len(pitches) == 0:
            return 0

        pitches = compute_bapv_vectorized(
            pitches, type_lookup, zone_lookup, weights, league_avg
        )

        # Use season league avg for normalization
        season_avg_sql = text("""
            SELECT AVG(avg_bapv) FROM mlb.pitch_quality_scores
            WHERE season = :s AND game_type = 'R'
        """)
        league_avg_bapv = db.execute(
            season_avg_sql, {"s": season - 1}
        ).scalar() or 0.0358

        agg = aggregate_per_game(pitches)
        agg = normalize_bapv_plus(agg, float(league_avg_bapv))
        n = store_scores(agg, db, game_pk=game_pk)
        return n

    except Exception as e:
        import traceback
        print(f"  Transform/score error for {game_pk}: {e}")
        traceback.print_exc()
        return 0


def poll_game(game_pk: int, db) -> dict:
    """Poll a single game and return current state."""
    data = fetch_live_feed(game_pk)
    if not data:
        return {}

    game_data = data.get("gameData", {})
    live_data = data.get("liveData", {})
    status = game_data.get("status", {}).get("detailedState", "")
    linescore = live_data.get("linescore", {})

    # Store raw event
    store_raw_event(game_pk, data, db)

    # Transform into structured tables every poll
    try:
        sys.path.insert(0, '/pipeline')
        from mlb.transform import transform_game_pk
        transform_game_pk(game_pk, db)
        print(f"  Transformed game {game_pk}")
    except Exception as e:
        print(f"  Transform error: {e}")

    # Get current pitcher stats from DB
    pitcher_stats = get_current_pitcher_stats(game_pk, db)

    return {
        "game_pk":     game_pk,
        "status":      status,
        "inning":      linescore.get("currentInning"),
        "inning_state": linescore.get("inningState"),
        "away":        game_data.get("teams", {}).get("away", {}).get("name", ""),
        "home":        game_data.get("teams", {}).get("home", {}).get("name", ""),
        "away_score":  linescore.get("teams", {}).get("away", {}).get("runs", 0),
        "home_score":  linescore.get("teams", {}).get("home", {}).get("runs", 0),
        "pitcher_stats": pitcher_stats,
        "is_final":    status == "Final",
    }


def print_game_state(state: dict):
    """Print current game state to console."""
    if not state:
        return
    inning_str = f"{state.get('inning_state','')} {state.get('inning','')}"
    print(f"  {state['away']} {state['away_score']} @ "
          f"{state['home']} {state['home_score']} "
          f"· {inning_str} · {state['status']}")

    for p in state.get('pitcher_stats', [])[:6]:
        print(f"    {p['pitcher_name']:<25} "
              f"{p['pitch_type_code']:>3} "
              f"{p['pitches']:>3}p "
              f"velo {p['avg_velo']:>5.1f} "
              f"CSW {p['csw_rate']:.3f} "
              f"whiff {p['whiff_rate']:.3f}")


def main():
    parser = argparse.ArgumentParser(description="MLB live game poller")
    parser.add_argument("--game-pk", type=int, default=None,
                        help="Poll a specific game PK")
    parser.add_argument("--interval", type=int, default=POLL_INTERVAL,
                        help="Poll interval in seconds (default 120)")
    parser.add_argument("--date", type=str, default=None,
                        help="Date to poll (YYYY-MM-DD, default today)")
    args = parser.parse_args()

    print(f"\n{'='*55}")
    print(f"  MLB Live Poller — interval {args.interval}s")
    print(f"{'='*55}\n")

    db = SessionLocal()
    completed_games = set()
    snapshotted_games = set()

    try:
        while True:
            now = datetime.now()
            print(f"\n[{now.strftime('%H:%M:%S')}] Polling...")

            # Get games to poll
            if args.game_pk:
                game_pks = [args.game_pk]
            else:
                live = get_live_games(args.date)
                game_pks = [g["game_pk"] for g in live]
                if live:
                    print(f"  {len(live)} live game(s):")
                    for g in live:
                        print(f"    {g['game_pk']} — {g['away']} @ {g['home']} "
                              f"({g.get('inning_state','')} {g.get('inning','')})")
                else:
                    print("  No live games found")

            if not game_pks:
                # Check if there are any games today at all
                completed = get_completed_games(args.date)
                new_completed = [pk for pk in completed if pk not in completed_games]
                if new_completed:
                    print(f"  {len(new_completed)} newly completed game(s) — transforming...")
                    for pk in new_completed:
                        n = transform_and_score(pk, db)
                        print(f"    {pk}: {n} rows scored")
                        completed_games.add(pk)

                if not args.game_pk:
                    print(f"  Waiting {args.interval}s...")
                    time.sleep(args.interval)
                    continue

            # Poll each live game
            for game_pk in game_pks:
                print(f"\n  Game {game_pk}:")
                state = poll_game(game_pk, db)
                print_game_state(state)

                # Snapshot projections the first time we see a game in progress
                if game_pk not in snapshotted_games:
                    try:
                        snap_res = httpx.post(
                            f"http://localhost:8000/mlb/matchup/snapshot/{game_pk}",
                            timeout=30
                        )
                        if snap_res.status_code == 200:
                            snap_data = snap_res.json()
                            print(f"  Snapshot stored: {snap_data['snapshots_stored']} batter projections")
                        else:
                            print(f"  Snapshot skipped (HTTP {snap_res.status_code})")
                    except Exception as e:
                        print(f"  Snapshot error: {e}")
                    snapshotted_games.add(game_pk)

                # If game just finished, transform and score
                if state.get("is_final") and game_pk not in completed_games:
                    print(f"\n  ✓ Game {game_pk} Final — transforming and scoring...")
                    n = transform_and_score(game_pk, db)
                    print(f"  Stored {n} BAPV rows")
                    completed_games.add(game_pk)

                    if args.game_pk:
                        print("\n  Single game complete. Exiting.")
                        return

            print(f"\n  Next poll in {args.interval}s... (Ctrl+C to stop)")
            time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\n\n  Poller stopped.")
    finally:
        db.close()


if __name__ == "__main__":
    main()