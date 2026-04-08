from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from database import get_db
from models.mlb import MLBRawEvent
import httpx
from datetime import datetime
import time

_leaderboard_cache = {}
CACHE_TTL = 300  # 5 minutes
router = APIRouter()
GUMBO_BASE = "https://statsapi.mlb.com/api/v1.1"


@router.get("/fetch-game/{game_pk}")
async def fetch_game(game_pk: int, db: Session = Depends(get_db)):
    """
    Fetch a single game from GUMBO and store raw JSON in mlb.raw_events.
    Example game_pks (2023 World Series): 716463, 716464, 716465, 716466, 716467
    """
    url = f"{GUMBO_BASE}/game/{game_pk}/feed/live"
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
        except httpx.HTTPError as e:
            raise HTTPException(status_code=502, detail=f"MLB API error: {str(e)}")

    data = response.json()
    game_date = data.get("gameData", {}).get("datetime", {}).get("originalDate")
    status = data.get("gameData", {}).get("status", {}).get("detailedState")
    away = data.get("gameData", {}).get("teams", {}).get("away", {}).get("abbreviation")
    home = data.get("gameData", {}).get("teams", {}).get("home", {}).get("abbreviation")

    existing = db.query(MLBRawEvent).filter(MLBRawEvent.game_pk == game_pk).first()
    if existing:
        existing.data = data
        existing.status = status
        existing.fetched_at = datetime.utcnow()
        db.commit()
        action, raw_id = "updated", existing.id
    else:
        raw = MLBRawEvent(
            game_pk=game_pk, game_date=game_date, status=status,
            away_team=away, home_team=home, endpoint=url, data=data,
        )
        db.add(raw)
        db.commit()
        db.refresh(raw)
        action, raw_id = "inserted", raw.id

    return {
        "action": action, "raw_event_id": raw_id, "game_pk": game_pk,
        "matchup": f"{away} @ {home}", "game_date": game_date, "status": status,
    }


@router.get("/explore/{game_pk}")
def explore_game(game_pk: int, db: Session = Depends(get_db)):
    """Summarize the structure of a stored GUMBO response."""
    raw = db.query(MLBRawEvent).filter(MLBRawEvent.game_pk == game_pk).first()
    if not raw:
        raise HTTPException(status_code=404, detail=f"Fetch game {game_pk} first.")

    data = raw.data
    live = data.get("liveData", {})
    all_plays = live.get("plays", {}).get("allPlays", [])

    total_pitches, pitch_types = 0, set()
    for play in all_plays:
        for event in play.get("playEvents", []):
            if event.get("isPitch"):
                total_pitches += 1
                pt = event.get("details", {}).get("type", {}).get("code")
                if pt:
                    pitch_types.add(pt)

    return {
        "game_pk": game_pk,
        "game_date": raw.game_date,
        "status": raw.status,
        "matchup": f"{raw.away_team} @ {raw.home_team}",
        "venue": data.get("gameData", {}).get("venue", {}).get("name"),
        "weather": data.get("gameData", {}).get("weather", {}),
        "total_at_bats": len(all_plays),
        "total_pitches": total_pitches,
        "pitch_types_seen": sorted(list(pitch_types)),
        "current_inning": live.get("linescore", {}).get("currentInning"),
        "decisions": live.get("decisions", {}),
    }


@router.get("/live-games")
def live_games(db: Session = Depends(get_db)):
    from sqlalchemy import text
    import httpx
    from datetime import date, timedelta
    
    # Check today in ET (container runs UTC, games are ET)
    # Simple fix: check both today and yesterday UTC
    dates_to_check = [
        date.today().isoformat(),
        (date.today() - timedelta(days=1)).isoformat()
    ]
    
    games = []
    try:
        for check_date in dates_to_check:
            res = httpx.get(
                "https://statsapi.mlb.com/api/v1/schedule",
                params={"sportId": 1, "date": check_date, "gameType": "R,S,E"},
                timeout=10
            )
            data = res.json()
            for day in data.get("dates", []):
                for g in day.get("games", []):
                    state = g["status"]["detailedState"]
                    if state in ("In Progress", "Manager challenge", "Delayed", "Final", "Game Over"):
                        # avoid duplicates
                        if any(x["game_pk"] == g["gamePk"] for x in games):
                            continue
                        ls = g.get("linescore", {})
                        game_pk = g["gamePk"]
                        pitcher_stats = db.execute(text("""
                            SELECT ab.pitcher_name, p.pitch_type_code,
                                   COUNT(*) as pitches,
                                   ROUND(AVG(p.start_speed)::numeric,1) as avg_velo,
                                   ROUND(AVG(CASE WHEN p.call_code IN ('S','W','T','C')
                                       THEN 1.0 ELSE 0.0 END)::numeric,3) as csw_rate
                            FROM mlb.pitches p
                            JOIN mlb.at_bats ab ON ab.game_pk=p.game_pk
                                AND ab.at_bat_index=p.at_bat_index
                            WHERE p.game_pk=:gp AND p.pitch_type_code IS NOT NULL
                            GROUP BY ab.pitcher_name, p.pitch_type_code
                            HAVING COUNT(*)>=5 ORDER BY pitches DESC LIMIT 10
                        """), {"gp": game_pk}).mappings().all()
                        score_sql = text("""
                            SELECT
                                SUM(CASE WHEN half_inning = 'top' THEN runs ELSE 0 END) as away_runs,
                                SUM(CASE WHEN half_inning = 'bottom' THEN runs ELSE 0 END) as home_runs,
                                MAX(inning) as current_inning
                            FROM mlb.linescore
                            WHERE game_pk = :gp
                        """)
                        score = db.execute(score_sql, {"gp": game_pk}).mappings().first()
                        games.append({
                            "game_pk": game_pk,
                            "away": g["teams"]["away"]["team"]["name"],
                            "home": g["teams"]["home"]["team"]["name"],
                            "away_score": int(score["away_runs"] or 0) if score else 0,
                            "home_score": int(score["home_runs"] or 0) if score else 0,
                            "inning": int(score["current_inning"]) if score and score["current_inning"] else None,
                            "inning_state": g.get("linescore", {}).get("inningState"),
                            "status": state,
                            "is_live": state in ("In Progress", "Manager challenge", "Delayed"),
                            "is_final": state in ("Final", "Game Over"),
                            "pitcher_stats": [dict(r) for r in pitcher_stats],
                        })
        return {"games": games}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"games": [], "error": str(e)}


@router.get("/live-pitcher/{pitcher_id}")
def live_pitcher(pitcher_id: int, db: Session = Depends(get_db)):
    from sqlalchemy import text
    from datetime import date
    sql = text("""
        SELECT p.pitch_type_code, COUNT(*) as pitches,
               ROUND(AVG(p.start_speed)::numeric,1) as avg_velo,
               ROUND(AVG(CASE WHEN p.call_code IN ('S','W','T','C')
                   THEN 1.0 ELSE 0.0 END)::numeric,3) as csw_rate,
               ROUND(
                    SUM(CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END) /
                    NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X') 
                        THEN 1.0 ELSE 0.0 END), 0)
                ::numeric, 3) as whiff_rate
        FROM mlb.pitches p
        JOIN mlb.games g ON g.game_pk=p.game_pk
        WHERE p.pitcher_id=:pid
        AND g.game_date=:today
        AND p.pitch_type_code IS NOT NULL
        GROUP BY p.pitch_type_code
        HAVING COUNT(*)>=5
        ORDER BY pitches DESC
    """)
    rows = db.execute(sql, {"pid": pitcher_id, "today": date.today().isoformat()}).mappings().all()
    return {"pitches": [dict(r) for r in rows], "game_info": date.today().isoformat()}


@router.get("/pitcher/{pitcher_id}/season-comparison")
def pitcher_season_comparison(
    pitcher_id: int,
    db: Session = Depends(get_db)
):
    """
    Compare pitcher's 2025 season BAPV+ vs 2026 regular season.
    Returns per-pitch-type breakdown with velocity and movement trends.
    Used for season-over-season comparison and early season blending.
    """
    try:
        from sqlalchemy import text

        sql = text("""
            WITH s2025 AS (
                SELECT
                    pitch_type_code,
                    SUM(pitches_thrown) as pitches,
                    ROUND(AVG(bapv_plus)::numeric, 1) as bapv_plus,
                    ROUND(AVG(avg_velo)::numeric, 1) as avg_velo,
                    ROUND(AVG(avg_spin)::numeric, 0) as avg_spin,
                    ROUND(AVG(avg_hmov)::numeric, 2) as avg_hmov,
                    ROUND(AVG(avg_ivb)::numeric, 2) as avg_ivb,
                    ROUND(AVG(whiff_rate)::numeric, 3) as whiff_rate,
                    ROUND(AVG(csw_rate)::numeric, 3) as csw_rate,
                    ROUND(AVG(hard_hit_rate)::numeric, 3) as hard_hit_rate,
                    COUNT(DISTINCT game_pk) as games
                FROM mlb.pitch_quality_scores
                WHERE pitcher_id = :pid
                AND season = 2025
                AND game_type = 'R'
                GROUP BY pitch_type_code
            ),
            s2026 AS (
                SELECT
                    pitch_type_code,
                    SUM(pitches_thrown) as pitches,
                    ROUND(AVG(bapv_plus)::numeric, 1) as bapv_plus,
                    ROUND(AVG(avg_velo)::numeric, 1) as avg_velo,
                    ROUND(AVG(avg_spin)::numeric, 0) as avg_spin,
                    ROUND(AVG(avg_hmov)::numeric, 2) as avg_hmov,
                    ROUND(AVG(avg_ivb)::numeric, 2) as avg_ivb,
                    ROUND(AVG(whiff_rate)::numeric, 3) as whiff_rate,
                    ROUND(AVG(csw_rate)::numeric, 3) as csw_rate,
                    ROUND(AVG(hard_hit_rate)::numeric, 3) as hard_hit_rate,
                    COUNT(DISTINCT game_pk) as games
                FROM mlb.pitch_quality_scores
                WHERE pitcher_id = :pid
                AND season = 2026
                AND game_type = 'R'
                GROUP BY pitch_type_code
            ),
            s2026r AS (
                SELECT
                    pitch_type_code,
                    SUM(pitches_thrown) as pitches,
                    ROUND(AVG(bapv_plus)::numeric, 1) as bapv_plus,
                    ROUND(AVG(avg_velo)::numeric, 1) as avg_velo,
                    ROUND(AVG(whiff_rate)::numeric, 3) as whiff_rate,
                    ROUND(AVG(csw_rate)::numeric, 3) as csw_rate
                FROM mlb.pitch_quality_scores
                WHERE pitcher_id = :pid
                AND season = 2026
                AND game_type = 'R'
                GROUP BY pitch_type_code
            )
            SELECT
                COALESCE(s25.pitch_type_code, s26.pitch_type_code, s26r.pitch_type_code) as pitch_type,
                -- 2025 season
                s25.pitches as pitches_2025,
                s25.games as games_2025,
                s25.bapv_plus as bapv_plus_2025,
                s25.avg_velo as velo_2025,
                s25.avg_spin as spin_2025,
                s25.avg_hmov as hmov_2025,
                s25.avg_ivb as ivb_2025,
                s25.whiff_rate as whiff_2025,
                s25.csw_rate as csw_2025,
                s25.hard_hit_rate as hard_hit_2025,
                -- 2026 season (with >=10 pitch threshold)
                s26.pitches as pitches_2026,
                s26.games as games_2026,
                s26.bapv_plus as bapv_plus_2026,
                s26.avg_velo as velo_2026,
                s26.avg_spin as spin_2026,
                s26.avg_hmov as hmov_2026,
                s26.avg_ivb as ivb_2026,
                s26.whiff_rate as whiff_2026,
                s26.csw_rate as csw_2026,
                s26.hard_hit_rate as hard_hit_2026,
                -- 2026 regular season (no threshold — full sample)
                s26r.pitches as pitches_2026r,
                s26r.bapv_plus as bapv_plus_2026r,
                s26r.avg_velo as velo_2026r,
                s26r.whiff_rate as whiff_2026r,
                s26r.csw_rate as csw_2026r,
                -- Changes
                ROUND((s26.bapv_plus - s25.bapv_plus)::numeric, 1) as bapv_change,
                ROUND((s26.avg_velo - s25.avg_velo)::numeric, 1) as velo_change,
                ROUND((s26.avg_hmov - s25.avg_hmov)::numeric, 2) as hmov_change,
                ROUND((s26.avg_ivb - s25.avg_ivb)::numeric, 2) as ivb_change
            FROM s2025 s25
            FULL OUTER JOIN s2026 s26
                ON s26.pitch_type_code = s25.pitch_type_code
            FULL OUTER JOIN s2026r s26r
                ON s26r.pitch_type_code = COALESCE(s25.pitch_type_code, s26.pitch_type_code)
            ORDER BY COALESCE(s25.pitches, 0) DESC
        """)

        rows = db.execute(sql, {"pid": pitcher_id}).mappings().all()

        # Get pitcher name
        name_sql = text("""
            SELECT DISTINCT pitcher_name FROM mlb.at_bats
            WHERE pitcher_id = :pid LIMIT 1
        """)
        name = db.execute(name_sql, {"pid": pitcher_id}).scalar()

        return {
            "pitcher_id": pitcher_id,
            "pitcher_name": name,
            "pitch_arsenal": [dict(r) for r in rows]
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/pitcher-search")
def pitcher_search(q: str, db: Session = Depends(get_db)):
    from sqlalchemy import text
    sql = text("""
        SELECT DISTINCT pitcher_id, pitcher_name
        FROM mlb.at_bats
        WHERE unaccent(LOWER(pitcher_name)) LIKE unaccent(LOWER(:q))
        AND pitcher_id IS NOT NULL
        ORDER BY pitcher_name
        LIMIT 10
    """)
    rows = db.execute(sql, {"q": f"%{q}%"}).mappings().all()
    return [dict(r) for r in rows]

@router.get("/pitcher/{pitcher_id}/game-log")
def pitcher_game_log(
    pitcher_id: int,
    season: int = 2026,
    db: Session = Depends(get_db)
):
    """
    Per-game BAPV+ log for a pitcher.
    Returns one row per game with aggregate stats across all pitch types.
    Also returns per-pitch-type breakdown per game for expandable detail.
    """
    from sqlalchemy import text

    sql = text("""
        WITH game_totals AS (
            SELECT
                game_pk,
                MAX(game_date) as game_date,
                MAX(game_type) as game_type,
                SUM(pitches_thrown) as total_pitches,
                ROUND(AVG(bapv_plus)::numeric, 1) as avg_bapv_plus,
                ROUND(AVG(avg_velo)::numeric, 1) as avg_velo,
                ROUND(AVG(whiff_rate)::numeric, 3) as whiff_rate,
                ROUND(AVG(csw_rate)::numeric, 3) as csw_rate,
                ROUND(AVG(hard_hit_rate)::numeric, 3) as hard_hit_rate
            FROM mlb.pitch_quality_scores
            WHERE pitcher_id = :pid
            AND season = :season
            AND game_type = 'R'
            GROUP BY game_pk
        ),
        game_opponent AS (
            SELECT DISTINCT
                g.game_pk,
                CASE
                    WHEN g.home_team_abbrev = (
                        SELECT MAX(home_team_abbrev) FROM mlb.games
                        WHERE game_pk IN (
                            SELECT DISTINCT game_pk FROM mlb.at_bats
                            WHERE pitcher_id = :pid
                        ) LIMIT 1
                    ) THEN g.away_team_abbrev
                    ELSE g.home_team_abbrev
                END as opponent,
                CASE
                    WHEN g.home_team_abbrev = (
                        SELECT MAX(home_team_abbrev) FROM mlb.games
                        WHERE game_pk IN (
                            SELECT DISTINCT game_pk FROM mlb.at_bats
                            WHERE pitcher_id = :pid
                        ) LIMIT 1
                    ) THEN false
                    ELSE true
                END as is_home
            FROM mlb.games g
            WHERE g.game_pk IN (SELECT game_pk FROM game_totals)
        ),
        pitch_breakdown AS (
            SELECT
                game_pk,
                pitch_type_code,
                pitches_thrown,
                ROUND(bapv_plus::numeric, 1) as bapv_plus,
                ROUND(avg_velo::numeric, 1) as avg_velo,
                ROUND(whiff_rate::numeric, 3) as whiff_rate,
                ROUND(csw_rate::numeric, 3) as csw_rate,
                ROUND(hard_hit_rate::numeric, 3) as hard_hit_rate,
                ROUND(avg_spin::numeric, 0) as avg_spin,
                ROUND(avg_hmov::numeric, 2) as avg_hmov,
                ROUND(avg_ivb::numeric, 2) as avg_ivb
            FROM mlb.pitch_quality_scores
            WHERE pitcher_id = :pid
            AND season = :season
            AND game_type = 'R'
        )
        SELECT
            gt.game_pk,
            gt.game_date,
            gt.total_pitches,
            gt.avg_bapv_plus,
            gt.avg_velo,
            gt.whiff_rate,
            gt.csw_rate,
            gt.hard_hit_rate,
            go.opponent,
            go.is_home
        FROM game_totals gt
        LEFT JOIN game_opponent go ON go.game_pk = gt.game_pk
        ORDER BY gt.game_date DESC
    """)

    games = db.execute(sql, {"pid": pitcher_id, "season": season}).mappings().all()

    # Get pitch breakdown per game
    breakdown_sql = text("""
        SELECT game_pk, pitch_type_code, pitches_thrown,
               ROUND(bapv_plus::numeric, 1) as bapv_plus,
               ROUND(avg_velo::numeric, 1) as avg_velo,
               ROUND(whiff_rate::numeric, 3) as whiff_rate,
               ROUND(csw_rate::numeric, 3) as csw_rate,
               ROUND(avg_spin::numeric, 0) as avg_spin,
               ROUND(avg_hmov::numeric, 2) as avg_hmov,
               ROUND(avg_ivb::numeric, 2) as avg_ivb
        FROM mlb.pitch_quality_scores
        WHERE pitcher_id = :pid
        AND season = :season
        AND game_type = 'R'
        ORDER BY game_date DESC, pitches_thrown DESC
    """)
    breakdown = db.execute(breakdown_sql, {"pid": pitcher_id, "season": season}).mappings().all()

    # Group breakdown by game_pk
    from collections import defaultdict
    breakdown_by_game = defaultdict(list)
    for row in breakdown:
        breakdown_by_game[row["game_pk"]].append(dict(row))

    result = []
    for g in games:
        game_dict = dict(g)
        game_dict["pitches"] = breakdown_by_game.get(g["game_pk"], [])
        result.append(game_dict)

    return {"pitcher_id": pitcher_id, "season": season, "games": result}


@router.get("/leaderboard")
def mlb_leaderboard(
    season: int = 2025,
    min_pitches: int = 200,
    pitch_type: str = None,
    db: Session = Depends(get_db)
):
    from sqlalchemy import text
    sql = text("""
        SELECT
            pitcher_id,
            MAX(pitcher_name) as pitcher_name,
            pitch_type_code as pitch_type,
            SUM(pitches_thrown) as total_pitches,
            ROUND(AVG(bapv_plus)::numeric, 1) as season_bapv_plus,
            ROUND(AVG(avg_velo)::numeric, 1) as avg_velo,
            ROUND(AVG(whiff_rate)::numeric, 3) as whiff_rate,
            ROUND(AVG(csw_rate)::numeric, 3) as csw_rate,
            ROUND(AVG(hard_hit_rate)::numeric, 3) as hard_hit_rate,
            COUNT(DISTINCT game_pk) as games
        FROM mlb.pitch_quality_scores
        WHERE season = :season
        AND game_type = 'R'
        GROUP BY pitcher_id, pitch_type_code
        HAVING SUM(pitches_thrown) >= :min_pitches
        ORDER BY season_bapv_plus DESC
        LIMIT 200
    """)
    rows = db.execute(sql, {"season": season, "min_pitches": min_pitches}).mappings().all()
    return {"pitchers": [dict(r) for r in rows]}


@router.get("/batter-tendencies")
def batter_tendencies(
    name: str,
    season: int = 2025,
    db: Session = Depends(get_db)
):
    from sqlalchemy import text
    sql = text("""
        SELECT
            ab.batter_id,
            ab.batter_name,
            t.pitch_type_code,
            t.whiff_rate,
            t.chase_rate,
            t.hard_hit_rate,
            t.avg_exit_velo,
            t.csw_rate
        FROM mlb.batter_pitch_type_tendencies t
        JOIN (
            SELECT DISTINCT batter_id, batter_name
            FROM mlb.at_bats
            WHERE LOWER(batter_name) LIKE LOWER(:name)
            LIMIT 1
        ) ab ON ab.batter_id = t.batter_id
        WHERE t.season = :season
    """)
    rows = db.execute(sql, {
        "name": f"%{name}%",
        "season": season
    }).mappings().all()
    if not rows:
        raise HTTPException(status_code=404, detail="Batter not found")
    batter_name = rows[0]["batter_name"]
    tendencies = {r["pitch_type_code"]: dict(r) for r in rows}
    return {"batter_name": batter_name, "tendencies": tendencies}


@router.get("/leaderboard")
def mlb_leaderboard(
    season: int = 2025,
    min_pitches: int = 200,
    pitch_type: str = None,
    db: Session = Depends(get_db)
):
    cache_key = f"{season}_{min_pitches}_{pitch_type}"
    now = time.time()
    
    if cache_key in _leaderboard_cache:
        cached_at, data = _leaderboard_cache[cache_key]
        if now - cached_at < CACHE_TTL:
            return data

    from sqlalchemy import text
    sql = text("""
        SELECT
            pitcher_id,
            MAX(pitcher_name) as pitcher_name,
            pitch_type_code as pitch_type,
            SUM(pitches_thrown) as total_pitches,
            ROUND(AVG(bapv_plus)::numeric, 1) as season_bapv_plus,
            ROUND(AVG(avg_velo)::numeric, 1) as avg_velo,
            ROUND(AVG(whiff_rate)::numeric, 3) as whiff_rate,
            ROUND(AVG(csw_rate)::numeric, 3) as csw_rate,
            ROUND(AVG(hard_hit_rate)::numeric, 3) as hard_hit_rate,
            COUNT(DISTINCT game_pk) as games
        FROM mlb.pitch_quality_scores
        WHERE season = :season
        AND game_type = 'R'
        GROUP BY pitcher_id, pitch_type_code
        HAVING SUM(pitches_thrown) >= :min_pitches
        ORDER BY season_bapv_plus DESC
        LIMIT 200
    """)
    rows = db.execute(sql, {"season": season, "min_pitches": min_pitches}).mappings().all()
    result = {"pitchers": [dict(r) for r in rows]}
    _leaderboard_cache[cache_key] = (now, result)
    return result

@router.get("/live-game/{game_pk}/pitcher-scores")
def live_pitcher_scores(game_pk: int, db: Session = Depends(get_db)):
    from sqlalchemy import text
    from datetime import datetime
    import pandas as pd
    import numpy as np

    season = datetime.now().year
    tend_season = season - 1

    # Pull pitches with batter tendencies joined
    pitch_sql = text("""
        SELECT
            p.pitcher_id,
            ab.pitcher_name,
            p.pitch_type_code,
            p.call_code,
            p.start_speed,
            p.spin_rate,
            p.pfx_x,
            p.pfx_z,
            p.zone,
            p.launch_speed,
            ab.batter_id,
            ab.event_type,
            COALESCE(bt.whiff_rate, 0.267) as batter_whiff,
            COALESCE(bt.chase_rate, 0.310) as batter_chase,
            COALESCE(bt.hard_hit_rate, 0.395) as batter_hard_hit
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        LEFT JOIN mlb.batter_pitch_type_tendencies bt
            ON bt.batter_id = ab.batter_id
            AND bt.pitch_type_code = p.pitch_type_code
            AND bt.season = :tend_season
        WHERE p.game_pk = :game_pk
        AND p.pitch_type_code IN ('FF','SI','SL','CH','FC','ST','CU','FS','KC')
        AND p.call_code IS NOT NULL
        AND ab.batter_id IS NOT NULL
    """)

    pitches = pd.read_sql(pitch_sql, db.bind, params={
        "game_pk": game_pk,
        "tend_season": tend_season
    })

    if pitches.empty:
        raise HTTPException(status_code=404, detail="No pitch data for this game")

    # Compute BAPV per pitch
    LEAGUE_WHIFF  = 0.267
    LEAGUE_CHASE  = 0.310
    LEAGUE_HH     = 0.395

    event_weights = {
        'single': 0.888, 'double': 1.271, 'triple': 1.616,
        'home_run': 2.101, 'walk': 0.690, 'hit_by_pitch': 0.720,
        'field_out': -0.098, 'strikeout': -0.098, 'force_out': -0.098,
        'grounded_into_double_play': -0.196, 'sac_fly': -0.049,
    }

    def compute_bapv(row):
        cc = row['call_code']
        bw = row['batter_whiff']
        bc = row['batter_chase']
        bh = row['batter_hard_hit']
        ls = row['launch_speed']
        ev = str(row['event_type']).lower() if row['event_type'] else ''

        if cc in ('S', 'W', 'T'):
            adj = 1.0 + max(0, LEAGUE_WHIFF - bw)
            return 0.170 * adj
        elif cc == 'C':
            adj = 1.0 + max(0, LEAGUE_CHASE - bc)
            return 0.025 * adj
        elif cc in ('F', 'D', 'E', 'L'):
            return 0.020
        elif cc in ('B', '*B', 'H'):
            return -0.040
        elif cc == 'M':
            return -0.150
        elif cc == 'X':
            base = event_weights.get(ev, -0.098)
            if ls and not pd.isna(ls):
                if ls >= 95:
                    # Hard hit — penalize regardless of outcome
                    # High exit velo = bad pitch even if it resulted in an out
                    hard_hit_surprise = 1.0 + max(0, LEAGUE_HH - bh)
                    # If it's a hit, penalize more; if it's an out, only partial credit
                    if ev in ('single', 'double', 'triple', 'home_run'):
                        base *= hard_hit_surprise
                    else:
                        # Hard hit out — still a bad pitch, reduce the out credit
                        base = base * 0.5  # only half credit for hard hit outs
                elif ls < 85:
                    base *= 0.85
            return -base
        return 0.0

    pitches['bapv'] = pitches.apply(compute_bapv, axis=1)

    # Compute league avg bapv for this game (normalize within game)
    league_avg_bapv = float(pitches['bapv'].mean())
    if abs(league_avg_bapv) < 0.001:
        league_avg_bapv = 0.0358

    # Aggregate by pitcher + pitch type
    agg = pitches.groupby(['pitcher_id', 'pitcher_name', 'pitch_type_code']).agg(
        pitches_today=('bapv', 'count'),
        avg_bapv_live=('bapv', 'mean'),
        avg_velo_today=('start_speed', 'mean'),
        avg_spin_today=('spin_rate', 'mean'),
        avg_hmov_today=('pfx_x', 'mean'),
        avg_ivb_today=('pfx_z', 'mean'),
        zone_pct_today=('zone', lambda x: (x.between(1, 9)).mean()),
        csw_today=('call_code', lambda x: x.isin({'S','W','T','C'}).mean()),
        whiff_today=('call_code', lambda x: (
            x.isin({'S','W','T'}).sum() /
            max(x.isin({'S','W','T','F','D','E','X'}).sum(), 1)
        )),
        chase_today=('call_code', lambda x: x.isin({'S','W','T','F','D','E','X'}).mean()),
    ).reset_index()

    agg = agg[agg['pitches_today'] >= 1]

    # Normalize live BAPV+ to 100 scale using season league avg
    season_avg_sql = text("""
        SELECT AVG(avg_bapv) as avg_bapv
        FROM mlb.pitch_quality_scores
        WHERE season = :s AND game_type = 'R'
        AND pitch_type_code = 'FF'
    """)

    # Use pitch-type-specific normalization from stored scores
    type_avgs_sql = text("""
        SELECT pitch_type_code, AVG(avg_bapv) as type_avg_bapv
        FROM mlb.pitch_quality_scores
        WHERE season = :s AND game_type = 'R'
        GROUP BY pitch_type_code
    """)
    type_avgs = dict(db.execute(type_avgs_sql, {"s": tend_season}).fetchall())

    agg['bapv_plus_live'] = agg.apply(
        lambda r: round(r['avg_bapv_live'] /
            abs(type_avgs.get(r['pitch_type_code'], league_avg_bapv) or league_avg_bapv) * 100, 1)
        if type_avgs.get(r['pitch_type_code']) else None,
        axis=1
    )

    # Round numeric columns
    for col in ['avg_velo_today', 'avg_spin_today', 'csw_today', 'whiff_today', 'chase_today',
                'avg_hmov_today', 'avg_ivb_today', 'zone_pct_today']:
        agg[col] = agg[col].round(3)
    agg['avg_velo_today'] = agg['avg_velo_today'].round(1)
    agg['avg_spin_today'] = agg['avg_spin_today'].round(0)
    agg['avg_hmov_today'] = agg['avg_hmov_today'].round(2)
    agg['avg_ivb_today'] = agg['avg_ivb_today'].round(2)

    # Pull season scores for comparison
    season_sql = text("""
        SELECT pitcher_id, pitch_type_code,
               ROUND(AVG(bapv_plus)::numeric, 1) as bapv_season,
               ROUND(AVG(avg_velo)::numeric, 1) as avg_velo_season,
               ROUND(AVG(whiff_rate)::numeric, 3) as whiff_season,
               ROUND(AVG(csw_rate)::numeric, 3) as csw_season,
               ROUND(AVG(avg_hmov)::numeric, 2) as avg_hmov_season,
               ROUND(AVG(avg_ivb)::numeric, 2) as avg_ivb_season,
               ROUND(AVG(avg_spin)::numeric, 0) as avg_spin_season,
               SUM(pitches_thrown) as season_pitches
        FROM mlb.pitch_quality_scores
        WHERE season = :s AND game_type = 'R'
        GROUP BY pitcher_id, pitch_type_code
    """)
    season_rows = pd.read_sql(season_sql, db.bind, params={"s": tend_season})
    season_map = {(r.pitcher_id, r.pitch_type_code): r
                  for r in season_rows.itertuples()}

    # Merge season data
    result_pitchers = {}
    for _, r in agg.iterrows():
        pid = int(r['pitcher_id'])
        pt = r['pitch_type_code']
        s = season_map.get((pid, pt))

        pitch_data = {
            "pitch_type_code": pt,
            "pitches_today": int(r['pitches_today']),
            "avg_velo_today": float(r['avg_velo_today']) if pd.notna(r['avg_velo_today']) else None,
            "avg_spin_today": float(r['avg_spin_today']) if pd.notna(r['avg_spin_today']) else None,
            "csw_today": float(r['csw_today']),
            "whiff_today": float(r['whiff_today']),
            "chase_today": float(r['chase_today']),
            "bapv_plus_live": float(r['bapv_plus_live']) if pd.notna(r['bapv_plus_live']) else None,
            "bapv_season": float(s.bapv_season) if s and pd.notna(s.bapv_season) else None,
            "avg_velo_season": float(s.avg_velo_season) if s and pd.notna(s.avg_velo_season) else None,
            "whiff_season": float(s.whiff_season) if s and pd.notna(s.whiff_season) else None,
            "csw_season": float(s.csw_season) if s and pd.notna(s.csw_season) else None,
            "season_pitches": int(s.season_pitches) if s and pd.notna(s.season_pitches) else None,
            "velo_delta": round(float(r['avg_velo_today']) - float(s.avg_velo_season), 1)
                          if s and pd.notna(r['avg_velo_today']) and pd.notna(s.avg_velo_season) else None,
            "whiff_delta": round(float(r['whiff_today']) - float(s.whiff_season), 3)
                           if s and pd.notna(s.whiff_season) else None,
            "hmov_delta": round(float(r['avg_hmov_today']) - float(s.avg_hmov_season), 2)
                          if s and pd.notna(r['avg_hmov_today']) and pd.notna(s.avg_hmov_season) else None,
            "ivb_delta": round(float(r['avg_ivb_today']) - float(s.avg_ivb_season), 2)
                          if s and pd.notna(r['avg_ivb_today']) and pd.notna(s.avg_ivb_season) else None,
            "spin_delta": round(float(r['avg_spin_today']) - float(s.avg_spin_season), 0)
                          if s and pd.notna(r['avg_spin_today']) and pd.notna(s.avg_spin_season) else None,
            "zone_pct_today": float(r['zone_pct_today']) if pd.notna(r['zone_pct_today']) else None,
            "avg_hmov_today": float(r['avg_hmov_today']) if pd.notna(r['avg_hmov_today']) else None,
            "avg_ivb_today": float(r['avg_ivb_today']) if pd.notna(r['avg_ivb_today']) else None,
            "avg_hmov_season": float(s.avg_hmov_season) if s and pd.notna(s.avg_hmov_season) else None,
            "avg_ivb_season": float(s.avg_ivb_season) if s and pd.notna(s.avg_ivb_season) else None,
            "avg_spin_season": float(s.avg_spin_season) if s and pd.notna(s.avg_spin_season) else None,
        }

        if pid not in result_pitchers:
            result_pitchers[pid] = {
                "pitcher_id": pid,
                "pitcher_name": r['pitcher_name'],
                "pitches": []
            }
        result_pitchers[pid]["pitches"].append(pitch_data)

    return {"game_pk": game_pk, "pitchers": list(result_pitchers.values())}


@router.post("/transform/{game_pk}")
def transform_game(game_pk: int, db: Session = Depends(get_db)):
    """
    Transform raw GUMBO data for a game into structured mlb.games,
    mlb.at_bats, and mlb.pitches tables.
    Run /mlb/fetch-game/{game_pk} first if you haven't already.
    """
    import sys
    sys.path.insert(0, "/pipeline")

    from mlb.transform import transform_game_pk
    try:
        transform_game_pk(game_pk, db)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    from models.mlb import MLBGame, MLBAtBat, MLBPitch
    game = db.query(MLBGame).filter(MLBGame.game_pk == game_pk).first()
    n_ab = db.query(MLBAtBat).filter(MLBAtBat.game_pk == game_pk).count()
    n_pitches = db.query(MLBPitch).filter(MLBPitch.game_pk == game_pk).count()

    return {
        "game_pk": game_pk,
        "status": game.status if game else None,
        "matchup": f"{game.away_team_abbrev} @ {game.home_team_abbrev}" if game else None,
        "at_bats": n_ab,
        "pitches": n_pitches,
    }


@router.get("/schedule/today")
def today_schedule(db: Session = Depends(get_db)):
    """Returns today's full MLB schedule with game status and probable starters."""
    import httpx
    from datetime import date, timedelta

    games = []
    for check_date in [date.today().isoformat(), (date.today() - timedelta(days=1)).isoformat()]:
        try:
            res = httpx.get(
                "https://statsapi.mlb.com/api/v1/schedule",
                params={
                    "sportId": 1,
                    "date": check_date,
                    "gameType": "R,S,E",
                    "hydrate": "probablePitcher,team,linescore"
                },
                timeout=10
            )
            data = res.json()
            for day in data.get("dates", []):
                for g in day.get("games", []):
                    state = g["status"]["detailedState"]
                    away = g["teams"]["away"]
                    home = g["teams"]["home"]
                    ls = g.get("linescore", {})

                    away_prob = away.get("probablePitcher", {})
                    home_prob = home.get("probablePitcher", {})

                    games.append({
                        "game_pk": g["gamePk"],
                        "game_date": day["date"],
                        "status": state,
                        "is_live": state in ("In Progress", "Manager challenge", "Delayed"),
                        "is_final": state in ("Final", "Game Over"),
                        "is_preview": state in ("Preview", "Warmup", "Pre-Game", "Scheduled"),
                        "away_team": away["team"]["name"],
                        "away_abbrev": away["team"].get("abbreviation", ""),
                        "home_team": home["team"]["name"],
                        "home_abbrev": home["team"].get("abbreviation", ""),
                        "away_score": ls.get("teams", {}).get("away", {}).get("runs", 0),
                        "home_score": ls.get("teams", {}).get("home", {}).get("runs", 0),
                        "inning": ls.get("currentInning"),
                        "inning_state": ls.get("inningState"),
                        "away_probable": {
                            "id": away_prob.get("id"),
                            "name": away_prob.get("fullName"),
                        } if away_prob else None,
                        "home_probable": {
                            "id": home_prob.get("id"),
                            "name": home_prob.get("fullName"),
                        } if home_prob else None,
                    })
        except Exception as e:
            print(f"Schedule fetch error: {e}")

    def sort_key(g):
        if g["is_live"]: return 0
        if g["is_preview"]: return 1
        return 2
    games.sort(key=sort_key)

    return {"games": games, "date": date.today().isoformat()}


@router.get("/matchup/pitcher/{pitcher_id}/vs-lineup/{game_pk}")
def pitcher_vs_lineup(pitcher_id: int, game_pk: int, db: Session = Depends(get_db)):
    """
    Projects strikeout probability and K count for a pitcher
    against today's specific lineup, weighted by his pitch mix and each
    batter's historical tendencies vs that pitch type.
    """
    import httpx
    from sqlalchemy import text

    tend_season = 2025

    # Get pitcher name early so it's available for all error/early responses
    pitcher_name_sql = text("SELECT MAX(pitcher_name) FROM mlb.at_bats WHERE pitcher_id = :pid")
    pitcher_name = db.execute(pitcher_name_sql, {"pid": pitcher_id}).scalar() or "Unknown"

    # 1. Fetch today's lineup from MLB API
    try:
        res = httpx.get(
            f"https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore",
            timeout=10
        )
        boxscore = res.json()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"MLB API error: {e}")

    teams = boxscore.get("teams", {})
    away_players = teams.get("away", {}).get("players", {})
    home_players = teams.get("home", {}).get("players", {})

    # Find pitcher's team by searching boxscore players dict (works once game is active)
    pitcher_team = None
    for pid, pdata in {**away_players, **home_players}.items():
        if pdata.get("person", {}).get("id") == pitcher_id:
            pitcher_team = "away" if pid in away_players else "home"
            break

    # Pre-game fallback: boxscore players dict may also be empty before roster is populated
    if not pitcher_team:
        # Try live feed first (works for in-progress games)
        try:
            live_res = httpx.get(
                f"https://statsapi.mlb.com/api/v1/game/{game_pk}/feed/live",
                params={"fields": "gameData,probablePitchers"},
                timeout=10
            )
            if live_res.status_code == 200:
                probable = live_res.json().get("gameData", {}).get("probablePitchers", {})
                if probable.get("away", {}).get("id") == pitcher_id:
                    pitcher_team = "away"
                elif probable.get("home", {}).get("id") == pitcher_id:
                    pitcher_team = "home"
        except Exception:
            pass

    if not pitcher_team:
        # Fall back to schedule API (reliable for pre-game / warmup states)
        try:
            sched_res = httpx.get(
                "https://statsapi.mlb.com/api/v1/schedule",
                params={"sportId": 1, "gamePk": game_pk, "hydrate": "probablePitcher,team"},
                timeout=10
            )
            for day in sched_res.json().get("dates", []):
                for g in day.get("games", []):
                    if g["gamePk"] == game_pk:
                        away_prob = g["teams"]["away"].get("probablePitcher", {})
                        home_prob = g["teams"]["home"].get("probablePitcher", {})
                        if away_prob.get("id") == pitcher_id:
                            pitcher_team = "away"
                        elif home_prob.get("id") == pitcher_id:
                            pitcher_team = "home"
        except Exception:
            pass

    if not pitcher_team:
        raise HTTPException(status_code=404, detail="Pitcher not found in this game")

    # Get opposing batting order
    opp_team = "home" if pitcher_team == "away" else "away"
    opp_data = teams.get(opp_team, {})
    batting_order = opp_data.get("battingOrder", [])
    opp_players = opp_data.get("players", {})

    # Hard stop — don't fabricate a lineup from roster
    if not batting_order:
        return {
            "game_pk": game_pk,
            "pitcher_id": pitcher_id,
            "pitcher_name": pitcher_name,
            "status": "lineup_not_posted",
            "message": "Lineup has not been posted yet. Check back closer to game time.",
            "pitch_mix": {},
            "lineup": [],
            "projections": None,
        }

    lineup = []
    for player_id in batting_order:
        key = f"ID{player_id}"
        if key in opp_players:
            p = opp_players[key]
            lineup.append({
                "player_id": player_id,
                "name": p.get("person", {}).get("fullName", "Unknown"),
                "bat_side": p.get("batSide", {}).get("code", "R"),
                "batting_order": batting_order.index(player_id) + 1
            })

    # Filter out pitchers in case any snuck into the batting order
    away_pitchers = set(boxscore["teams"].get("away", {}).get("pitchers", []))
    home_pitchers = set(boxscore["teams"].get("home", {}).get("pitchers", []))
    all_pitchers = away_pitchers | home_pitchers
    lineup = [b for b in lineup if b["player_id"] not in all_pitchers]

    if not lineup:
        raise HTTPException(status_code=404, detail="Could not find opposing lineup — game may not have a posted lineup yet")

    # 2. Get pitcher's pitch mix and BAPV+ from 2025 season
    pitch_mix_sql = text("""
        SELECT pitch_type_code,
               ROUND(AVG(bapv_plus)::numeric, 1) as bapv_plus,
               ROUND(AVG(whiff_rate)::numeric, 3) as whiff_rate,
               ROUND(AVG(csw_rate)::numeric, 3) as csw_rate,
               ROUND(AVG(avg_velo)::numeric, 1) as avg_velo,
               SUM(pitches_thrown) as total_pitches
        FROM mlb.pitch_quality_scores
        WHERE pitcher_id = :pid
        AND season = :season
        AND game_type = 'R'
        GROUP BY pitch_type_code
        HAVING SUM(pitches_thrown) >= 50
    """)
    pitch_mix = db.execute(pitch_mix_sql, {
        "pid": pitcher_id, "season": tend_season
    }).mappings().all()

    if not pitch_mix:
        raise HTTPException(status_code=404, detail="No 2025 pitch data for this pitcher")

    total_pitches = sum(p["total_pitches"] for p in pitch_mix)
    pitch_usage = {
        p["pitch_type_code"]: {
            "usage": p["total_pitches"] / total_pitches,
            "bapv_plus": float(p["bapv_plus"]),
            "whiff_rate": float(p["whiff_rate"]),
            "csw_rate": float(p["csw_rate"]),
            "avg_velo": float(p["avg_velo"]),
        }
        for p in pitch_mix
    }

    # 3. Get pitcher's actual 2025 K rate as anchor (join via name — player_id_map incomplete)
    pitcher_k_sql = text("""
        SELECT f.k_pct, f.swstr_pct, f.ip
        FROM mlb.fangraphs_pitching f
        WHERE LOWER(f.player_name) = (
            SELECT LOWER(MAX(pitcher_name)) FROM mlb.at_bats WHERE pitcher_id = :pid
        )
        AND f.season = 2025
        AND f.ip >= 30
        LIMIT 1
    """)
    pitcher_fg = db.execute(pitcher_k_sql, {"pid": pitcher_id}).mappings().first()
    pitcher_actual_k = float(pitcher_fg["k_pct"]) if pitcher_fg and pitcher_fg["k_pct"] else 0.23
    pitcher_ip = float(pitcher_fg["ip"]) if pitcher_fg else 0
    ip_weight = min(0.40, pitcher_ip / 200)

    LEAGUE_WHIFF = 0.267
    LEAGUE_K = 0.227

    # 4. For each batter in lineup, compute projected K probability
    batter_ids = [b["player_id"] for b in lineup]

    # Fetch batter K rates from boxscore history (fangraphs_batting lacks k_pct)
    batter_k_sql = text("""
        SELECT
            bb.player_id as batter_id,
            SUM(bb.strikeouts)::float / NULLIF(SUM(bb.at_bats), 0) as batter_k_pct
        FROM mlb.boxscore_batting bb
        JOIN mlb.games g ON g.game_pk = bb.game_pk
        WHERE bb.player_id = ANY(:bids)
        AND g.season = :season
        AND g.game_type = 'R'
        GROUP BY bb.player_id
        HAVING SUM(bb.at_bats) >= 50
    """)
    batter_k_rows = db.execute(batter_k_sql, {
        "bids": batter_ids, "season": tend_season
    }).mappings().all()
    batter_k_map = {r["batter_id"]: float(r["batter_k_pct"]) for r in batter_k_rows}

    tend_sql = text("""
        SELECT batter_id, pitch_type_code,
               whiff_rate, chase_rate, hard_hit_rate, csw_rate
        FROM mlb.batter_pitch_type_tendencies
        WHERE batter_id = ANY(:bids)
        AND season = :season
        AND pitches_faced >= 10
    """)
    tend_rows = db.execute(tend_sql, {
        "bids": batter_ids, "season": tend_season
    }).mappings().all()

    # Build lookup: batter_id -> pitch_type -> stats
    batter_tend = {}
    for r in tend_rows:
        bid = r["batter_id"]
        pt = r["pitch_type_code"]
        if bid not in batter_tend:
            batter_tend[bid] = {}
        batter_tend[bid][pt] = dict(r)

    # 5. Compute per-batter projected K probability
    results = []
    for batter in lineup:
        bid = batter["player_id"]
        btend = batter_tend.get(bid, {})

        # Weighted whiff rate across pitcher's arsenal
        weighted_whiff = 0.0
        weighted_csw = 0.0
        total_weight = 0.0

        for pt, pitch_stats in pitch_usage.items():
            usage = pitch_stats["usage"]
            pitcher_whiff = pitch_stats["whiff_rate"]
            pitcher_csw = pitch_stats["csw_rate"]
            batter_whiff = btend.get(pt, {}).get("whiff_rate", LEAGUE_WHIFF) or LEAGUE_WHIFF

            # Adjust pitcher whiff by how this batter compares to league avg
            batter_adj = batter_whiff / LEAGUE_WHIFF  # ratio: <1 = contact hitter
            adj_whiff = pitcher_whiff * batter_adj
            adj_csw = pitcher_csw * batter_adj

            weighted_whiff += adj_whiff * usage
            weighted_csw += adj_csw * usage
            total_weight += usage

        if total_weight > 0:
            weighted_whiff /= total_weight
            weighted_csw /= total_weight

        # Three-way blend: pitcher anchor + batter anchor + CSW
        batter_overall_whiff = sum(
            (btend.get(pt, {}).get("whiff_rate", LEAGUE_WHIFF) or LEAGUE_WHIFF) * pitch_usage[pt]["usage"]
            for pt in pitch_usage
        )
        csw_proj_k = min(0.60, weighted_csw * 0.75)
        pitcher_k_adj = pitcher_actual_k * (batter_overall_whiff / LEAGUE_WHIFF)
        batter_actual_k = batter_k_map.get(bid, LEAGUE_K) or LEAGUE_K
        batter_k_adj = batter_actual_k * (pitcher_actual_k / LEAGUE_K)
        csw_fill = 1 - ip_weight - 0.35
        projected_k_pct = min(0.65, max(0.05,
            pitcher_k_adj * ip_weight +
            batter_k_adj * 0.35 +
            csw_proj_k * csw_fill
        ))

        # Weighted BAPV+ for this batter vs this pitcher
        weighted_bapv = sum(
            pitch_usage[pt]["bapv_plus"] * pitch_usage[pt]["usage"]
            for pt in pitch_usage
        )

        results.append({
            "player_id": bid,
            "name": batter["name"],
            "batting_order": batter["batting_order"],
            "bat_side": batter["bat_side"],
            "projected_k_pct": round(projected_k_pct, 3),
            "projected_whiff_rate": round(weighted_whiff, 3),
            "projected_csw": round(weighted_csw, 3),
            "weighted_bapv_vs_batter": round(weighted_bapv, 1),
            "has_tendency_data": bid in batter_tend,
        })

    # Sort by projected K probability descending
    results.sort(key=lambda x: x["projected_k_pct"], reverse=True)

    # Aggregate: projected Ks over 6 innings (18 batters faced ~= 3 times through order)
    avg_k_pct = sum(r["projected_k_pct"] for r in results) / len(results)
    projected_ks_6inn = round(avg_k_pct * 18, 1)
    projected_ks_9inn = round(avg_k_pct * 27, 1)

    return {
        "game_pk": game_pk,
        "pitcher_id": pitcher_id,
        "pitcher_name": pitcher_name,
        "pitch_mix": pitch_usage,
        "lineup": results,
        "projections": {
            "avg_k_pct": round(avg_k_pct, 3),
            "projected_ks_6inn": projected_ks_6inn,
            "projected_ks_9inn": projected_ks_9inn,
            "lineup_size": len(results),
            "pitcher_actual_k_pct": round(pitcher_actual_k * 100, 1),
            "ip_anchor_weight": round(ip_weight, 2),
        }
    }


@router.get("/matchup/batter/{batter_id}/vs-pitcher/{pitcher_id}")
def batter_vs_pitcher(batter_id: int, pitcher_id: int, db: Session = Depends(get_db)):
    """
    Projects total bases, wOBA, and K probability for a specific
    batter vs a specific pitcher based on pitch mix and batter tendencies.
    """
    from sqlalchemy import text

    tend_season = 2025

    # Get pitcher's pitch mix
    pitch_mix_sql = text("""
        SELECT pitch_type_code,
               ROUND(AVG(bapv_plus)::numeric, 1) as bapv_plus,
               ROUND(AVG(whiff_rate)::numeric, 3) as pitcher_whiff,
               ROUND(AVG(csw_rate)::numeric, 3) as pitcher_csw,
               ROUND(AVG(hard_hit_rate)::numeric, 3) as pitcher_hard_hit_allowed,
               ROUND(AVG(avg_velo)::numeric, 1) as avg_velo,
               SUM(pitches_thrown) as total_pitches
        FROM mlb.pitch_quality_scores
        WHERE pitcher_id = :pid
        AND season = :season
        AND game_type = 'R'
        GROUP BY pitch_type_code
        HAVING SUM(pitches_thrown) >= 50
    """)
    pitch_mix = db.execute(pitch_mix_sql, {
        "pid": pitcher_id, "season": tend_season
    }).mappings().all()

    if not pitch_mix:
        raise HTTPException(status_code=404, detail="No pitch data for pitcher")

    total_pitches = sum(p["total_pitches"] for p in pitch_mix)

    # Get batter tendencies — use actual available columns
    tend_sql = text("""
        SELECT pitch_type_code,
               whiff_rate, chase_rate, hard_hit_rate, barrel_rate,
               avg_exit_velo, avg_launch_angle, avg_woba_on_contact,
               contact_rate, in_play_rate, csw_rate, swing_rate,
               pitches_faced
        FROM mlb.batter_pitch_type_tendencies
        WHERE batter_id = :bid
        AND season = :season
        AND pitches_faced >= 10
    """)
    tend_rows = db.execute(tend_sql, {
        "bid": batter_id, "season": tend_season
    }).mappings().all()

    batter_tend = {r["pitch_type_code"]: dict(r) for r in tend_rows}

    # Get batter name
    batter_name_sql = text("""
        SELECT MAX(batter_name) FROM mlb.at_bats WHERE batter_id = :bid
    """)
    batter_name = db.execute(batter_name_sql, {"bid": batter_id}).scalar() or "Unknown"

    # Get pitcher name
    pitcher_name_sql = text("""
        SELECT MAX(pitcher_name) FROM mlb.at_bats WHERE pitcher_id = :pid
    """)
    pitcher_name = db.execute(pitcher_name_sql, {"pid": pitcher_id}).scalar() or "Unknown"

    # Get pitcher's actual 2025 K rate as anchor (join via name — player_id_map incomplete)
    pitcher_k_sql = text("""
        SELECT f.k_pct, f.ip
        FROM mlb.fangraphs_pitching f
        WHERE LOWER(f.player_name) = (
            SELECT LOWER(MAX(pitcher_name)) FROM mlb.at_bats WHERE pitcher_id = :pid
        )
        AND f.season = 2025 AND f.ip >= 30
        LIMIT 1
    """)
    pitcher_fg = db.execute(pitcher_k_sql, {"pid": pitcher_id}).mappings().first()
    pitcher_actual_k = float(pitcher_fg["k_pct"]) if pitcher_fg and pitcher_fg["k_pct"] else 0.23
    pitcher_ip = float(pitcher_fg["ip"]) if pitcher_fg else 0
    ip_weight = min(0.40, pitcher_ip / 200)
    LEAGUE_K = 0.227

    # Batter's own K rate from boxscore history
    batter_k_hist_sql = text("""
        SELECT SUM(bb.strikeouts)::float / NULLIF(SUM(bb.at_bats), 0) as batter_k_pct
        FROM mlb.boxscore_batting bb
        JOIN mlb.games g ON g.game_pk = bb.game_pk
        WHERE bb.player_id = :bid
        AND g.season = :season AND g.game_type = 'R'
        HAVING SUM(bb.at_bats) >= 50
    """)
    batter_k_row = db.execute(batter_k_hist_sql, {
        "bid": batter_id, "season": tend_season
    }).scalar()
    batter_actual_k = float(batter_k_row) if batter_k_row else LEAGUE_K

    LEAGUE_WHIFF = 0.267
    LEAGUE_HH = 0.395

    pitch_results = []
    weighted_k_pct = 0.0
    weighted_woba = 0.0
    weighted_hard_hit = 0.0
    total_weight = 0.0

    for p in pitch_mix:
        pt = p["pitch_type_code"]
        usage = p["total_pitches"] / total_pitches
        bt = batter_tend.get(pt, {})

        pitcher_whiff = float(p["pitcher_whiff"] or LEAGUE_WHIFF)
        pitcher_csw = float(p["pitcher_csw"] or 0.30)
        pitcher_hh_allowed = float(p["pitcher_hard_hit_allowed"] or LEAGUE_HH)
        pitcher_bapv = float(p["bapv_plus"] or 100)

        # Batter tendencies vs this pitch type (fall back to league avg)
        batter_whiff = float(bt.get("whiff_rate") or LEAGUE_WHIFF)
        batter_hh = float(bt.get("hard_hit_rate") or LEAGUE_HH)
        batter_barrel = float(bt.get("barrel_rate") or 0.08)
        batter_woba_contact = float(bt.get("avg_woba_on_contact") or 0.380)
        batter_contact = float(bt.get("contact_rate") or 0.78)
        batter_ev = float(bt.get("avg_exit_velo") or 88.0)
        batter_la = float(bt.get("avg_launch_angle") or 12.0)

        # Adjusted whiff: pitcher's whiff rate scaled by how this batter
        # whiffs vs this pitch type relative to league average
        batter_whiff_adj = batter_whiff / LEAGUE_WHIFF
        adj_whiff = min(0.80, pitcher_whiff * batter_whiff_adj)

        # Adjusted CSW
        adj_csw = min(0.65, pitcher_csw * batter_whiff_adj)

        # Three-way blend: pitcher anchor + batter anchor + CSW
        csw_proj_k = min(0.60, adj_csw * 0.75)
        pitcher_k_adj = pitcher_actual_k * batter_whiff_adj
        batter_k_adj = batter_actual_k * (pitcher_actual_k / LEAGUE_K)
        csw_fill = 1 - ip_weight - 0.35
        k_pct = min(0.65, max(0.05,
            pitcher_k_adj * ip_weight +
            batter_k_adj * 0.35 +
            csw_proj_k * csw_fill
        ))

        # Hard hit rate: blend pitcher's allowed rate and batter's tendency
        adj_hard_hit = (batter_hh * 0.6 + pitcher_hh_allowed * 0.4)

        # wOBA on contact: use batter's actual wOBA on contact vs this pitch type
        # suppressed by pitcher's BAPV+ (better pitcher = lower wOBA allowed)
        bapv_suppression = 100.0 / max(pitcher_bapv, 50)
        adj_woba_contact = batter_woba_contact * bapv_suppression

        # In-play rate: (1 - k_pct) * contact_rate approximation
        in_play_rate = (1 - k_pct) * batter_contact
        exp_woba = in_play_rate * adj_woba_contact

        # TB/PA ≈ wOBA * 1.3 (rough linear approximation)
        exp_tb_per_pa = exp_woba * 1.3

        # Barrel rate adjusted
        adj_barrel = batter_barrel * bapv_suppression

        pitch_results.append({
            "pitch_type_code": pt,
            "usage_pct": round(usage * 100, 1),
            "avg_velo": float(p["avg_velo"] or 0),
            "bapv_plus": float(pitcher_bapv),
            "adj_whiff_pct": round(adj_whiff * 100, 1),
            "adj_k_pct": round(k_pct * 100, 1),
            "adj_hard_hit_pct": round(adj_hard_hit * 100, 1),
            "adj_barrel_pct": round(adj_barrel * 100, 1),
            "batter_avg_ev": round(batter_ev, 1),
            "exp_woba": round(exp_woba, 3),
            "exp_tb_per_pa": round(exp_tb_per_pa, 3),
            "woba_on_contact": round(adj_woba_contact, 3),
            "has_tendency_data": pt in batter_tend,
            "pitches_faced_sample": int(bt.get("pitches_faced") or 0),
        })

        weighted_k_pct += k_pct * usage
        weighted_woba += exp_woba * usage
        weighted_hard_hit += adj_hard_hit * usage
        total_weight += usage

    if total_weight > 0:
        weighted_k_pct /= total_weight
        weighted_woba /= total_weight
        weighted_hard_hit /= total_weight

    avg_tb_per_pa = sum(
        p["exp_tb_per_pa"] * (p["usage_pct"] / 100)
        for p in pitch_results
    )
    proj_tb_game = round(avg_tb_per_pa * 3.5, 2)

    pitch_results.sort(key=lambda x: x["usage_pct"], reverse=True)

    return {
        "batter_id": batter_id,
        "batter_name": batter_name,
        "pitcher_id": pitcher_id,
        "pitcher_name": pitcher_name,
        "pitch_breakdown": pitch_results,
        "projections": {
            "projected_k_pct": round(weighted_k_pct * 100, 1),
            "projected_woba": round(weighted_woba, 3),
            "projected_hard_hit_pct": round(weighted_hard_hit * 100, 1),
            "projected_tb_per_pa": round(avg_tb_per_pa, 3),
            "projected_tb_game": proj_tb_game,
            "pa_assumption": 3.5,
            "pitcher_actual_k_pct": round(pitcher_actual_k * 100, 1),
            "ip_anchor_weight": round(ip_weight, 2),
        }
    }


@router.post("/matchup/snapshot/{game_pk}")
def snapshot_matchup_projections(game_pk: int, db: Session = Depends(get_db)):
    """
    Store pre-game matchup projections for both pitchers vs opposing lineups.
    Call this at or before first pitch for each game.
    Idempotent — safe to call multiple times, uses INSERT ... ON CONFLICT DO NOTHING.
    """
    import httpx
    from sqlalchemy import text
    from datetime import date

    tend_season = 2025
    LEAGUE_WHIFF = 0.267
    LEAGUE_HH = 0.395

    # Fetch boxscore for lineups and probable pitchers
    try:
        res = httpx.get(
            f"https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore",
            timeout=10
        )
        boxscore = res.json()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"MLB API error: {e}")

    teams = boxscore.get("teams", {})
    snapshots_stored = 0

    for pitcher_side, batter_side in [("away", "home"), ("home", "away")]:
        pitcher_team = teams.get(pitcher_side, {})
        batter_team = teams.get(batter_side, {})

        # Get starting pitcher (first in pitchers list)
        pitchers = pitcher_team.get("pitchers", [])
        if not pitchers:
            continue
        pitcher_id = pitchers[0]

        # Get pitcher's pitch mix
        pitch_mix_sql = text("""
            SELECT pitch_type_code,
                   ROUND(AVG(bapv_plus)::numeric, 1) as bapv_plus,
                   ROUND(AVG(whiff_rate)::numeric, 3) as pitcher_whiff,
                   ROUND(AVG(csw_rate)::numeric, 3) as pitcher_csw,
                   ROUND(AVG(hard_hit_rate)::numeric, 3) as pitcher_hh,
                   SUM(pitches_thrown) as total_pitches
            FROM mlb.pitch_quality_scores
            WHERE pitcher_id = :pid
            AND season = :season AND game_type = 'R'
            GROUP BY pitch_type_code
            HAVING SUM(pitches_thrown) >= 50
        """)
        pitch_mix = db.execute(pitch_mix_sql, {
            "pid": pitcher_id, "season": tend_season
        }).mappings().all()

        if not pitch_mix:
            continue

        total_pitches = sum(p["total_pitches"] for p in pitch_mix)

        pitcher_name_sql = text("""
            SELECT MAX(pitcher_name) FROM mlb.at_bats WHERE pitcher_id = :pid
        """)
        pitcher_name = db.execute(pitcher_name_sql, {"pid": pitcher_id}).scalar() or "Unknown"

        weighted_bapv = sum(
            float(p["bapv_plus"] or 100) * p["total_pitches"] / total_pitches
            for p in pitch_mix
        )

        # Get opposing batting order
        batting_order = batter_team.get("battingOrder", [])
        batter_players = batter_team.get("players", {})

        if not batting_order:
            continue

        # Get batter tendencies in bulk
        batter_ids = list(batting_order)
        tend_sql = text("""
            SELECT batter_id, pitch_type_code,
                   whiff_rate, hard_hit_rate, barrel_rate,
                   avg_woba_on_contact, contact_rate, pitches_faced
            FROM mlb.batter_pitch_type_tendencies
            WHERE batter_id = ANY(:bids)
            AND season = :season
            AND pitches_faced >= 10
        """)
        tend_rows = db.execute(tend_sql, {
            "bids": batter_ids, "season": tend_season
        }).mappings().all()

        batter_tend = {}
        for r in tend_rows:
            bid = r["batter_id"]
            pt = r["pitch_type_code"]
            if bid not in batter_tend:
                batter_tend[bid] = {}
            batter_tend[bid][pt] = dict(r)

        # Compute and store projection for each batter
        for order_pos, batter_id in enumerate(batting_order, 1):
            key = f"ID{batter_id}"
            if key not in batter_players:
                continue

            batter_info = batter_players[key]
            batter_name = batter_info.get("person", {}).get("fullName", "Unknown")
            btend = batter_tend.get(batter_id, {})

            weighted_k = 0.0
            weighted_woba = 0.0
            weighted_hh = 0.0
            weighted_woba_contact = 0.0
            total_weight = 0.0

            for p in pitch_mix:
                pt = p["pitch_type_code"]
                usage = p["total_pitches"] / total_pitches
                bt = btend.get(pt, {})

                pitcher_whiff = float(p["pitcher_whiff"] or LEAGUE_WHIFF)
                pitcher_csw = float(p["pitcher_csw"] or 0.30)
                pitcher_hh = float(p["pitcher_hh"] or LEAGUE_HH)
                pitcher_bapv = float(p["bapv_plus"] or 100)

                batter_whiff = float(bt.get("whiff_rate") or LEAGUE_WHIFF)
                batter_hh = float(bt.get("hard_hit_rate") or LEAGUE_HH)
                batter_woba_contact = float(bt.get("avg_woba_on_contact") or 0.380)
                batter_contact = float(bt.get("contact_rate") or 0.78)

                batter_whiff_adj = batter_whiff / LEAGUE_WHIFF
                adj_csw = min(0.65, pitcher_csw * batter_whiff_adj)
                k_pct = min(0.60, adj_csw * 0.75)

                bapv_suppression = 100.0 / max(pitcher_bapv, 50)
                adj_woba_contact = batter_woba_contact * bapv_suppression
                in_play_rate = (1 - k_pct) * batter_contact
                exp_woba = in_play_rate * adj_woba_contact
                adj_hh = (batter_hh * 0.6 + pitcher_hh * 0.4)

                weighted_k += k_pct * usage
                weighted_woba += exp_woba * usage
                weighted_hh += adj_hh * usage
                weighted_woba_contact += batter_woba_contact * usage
                total_weight += usage

            if total_weight > 0:
                weighted_k /= total_weight
                weighted_woba /= total_weight
                weighted_hh /= total_weight
                weighted_woba_contact /= total_weight

            avg_tb_per_pa = weighted_woba * 1.3
            proj_tb_game = round(avg_tb_per_pa * 3.5, 2)

            insert_sql = text("""
                INSERT INTO mlb.matchup_projections (
                    game_pk, game_date, pitcher_id, pitcher_name,
                    batter_id, batter_name, batting_order,
                    pitcher_bapv_plus, batter_whiff_rate, batter_hard_hit_rate,
                    batter_woba_on_contact, proj_k_pct, proj_woba,
                    proj_tb_per_pa, proj_tb_game, proj_hard_hit_pct, season
                ) VALUES (
                    :game_pk, :game_date, :pitcher_id, :pitcher_name,
                    :batter_id, :batter_name, :batting_order,
                    :pitcher_bapv_plus, :batter_whiff_rate, :batter_hard_hit_rate,
                    :batter_woba_on_contact, :proj_k_pct, :proj_woba,
                    :proj_tb_per_pa, :proj_tb_game, :proj_hard_hit_pct, :season
                )
                ON CONFLICT (game_pk, pitcher_id, batter_id) DO NOTHING
            """)
            db.execute(insert_sql, {
                "game_pk": game_pk,
                "game_date": date.today().isoformat(),
                "pitcher_id": pitcher_id,
                "pitcher_name": pitcher_name,
                "batter_id": batter_id,
                "batter_name": batter_name,
                "batting_order": order_pos,
                "pitcher_bapv_plus": round(weighted_bapv, 1),
                "batter_whiff_rate": round(weighted_k / 0.75 if weighted_k else LEAGUE_WHIFF, 3),
                "batter_hard_hit_rate": round(weighted_hh, 3),
                "batter_woba_on_contact": round(weighted_woba_contact, 3),
                "proj_k_pct": round(weighted_k, 3),
                "proj_woba": round(weighted_woba, 3),
                "proj_tb_per_pa": round(avg_tb_per_pa, 3),
                "proj_tb_game": proj_tb_game,
                "proj_hard_hit_pct": round(weighted_hh, 3),
                "season": 2026,
            })
            snapshots_stored += 1

        db.commit()

    return {
        "game_pk": game_pk,
        "snapshots_stored": snapshots_stored,
        "message": f"Stored {snapshots_stored} batter projections"
    }


@router.post("/matchup/grade/{game_pk}")
def grade_matchup_projections(game_pk: int, db: Session = Depends(get_db)):
    """
    After a game finishes, populate actual outcomes in matchup_projections
    by joining against boxscore_batting. Computes actual K%, TB/PA, wOBA.
    """
    from sqlalchemy import text
    from datetime import datetime, timezone

    actuals_sql = text("""
        SELECT
            bb.player_id as batter_id,
            bb.at_bats,
            bb.hits,
            bb.home_runs,
            bb.doubles,
            COALESCE(bb.triples, 0) as triples,
            COALESCE(bb.strikeouts, 0) as strikeouts,
            COALESCE(bb.total_bases, 0) as total_bases,
            bb.hit_by_pitch,
            bb.sac_flies,
            (bb.at_bats + COALESCE(bb.hit_by_pitch,0) + COALESCE(bb.sac_flies,0)) as pa,
            CASE WHEN bb.at_bats > 0
                THEN COALESCE(bb.strikeouts, 0)::float / bb.at_bats
                ELSE NULL END as actual_k_pct,
            CASE WHEN (bb.at_bats + COALESCE(bb.hit_by_pitch,0) + COALESCE(bb.sac_flies,0)) > 0
                THEN COALESCE(bb.total_bases, 0)::float /
                     (bb.at_bats + COALESCE(bb.hit_by_pitch,0) + COALESCE(bb.sac_flies,0))
                ELSE NULL END as actual_tb_per_pa
        FROM mlb.boxscore_batting bb
        WHERE bb.game_pk = :game_pk
        AND bb.at_bats > 0
    """)
    actuals = db.execute(actuals_sql, {"game_pk": game_pk}).mappings().all()

    if not actuals:
        raise HTTPException(status_code=404, detail="No batting data found for this game")

    graded = 0
    for row in actuals:
        update_sql = text("""
            UPDATE mlb.matchup_projections
            SET
                actual_pa = :pa,
                actual_strikeouts = :strikeouts,
                actual_hits = :hits,
                actual_total_bases = :total_bases,
                actual_home_runs = :home_runs,
                actual_k_pct = :actual_k_pct,
                actual_tb_per_pa = :actual_tb_per_pa,
                graded_at = :graded_at
            WHERE game_pk = :game_pk
            AND batter_id = :batter_id
        """)
        result = db.execute(update_sql, {
            "game_pk": game_pk,
            "batter_id": row["batter_id"],
            "pa": row["pa"],
            "strikeouts": row["strikeouts"],
            "hits": row["hits"],
            "total_bases": row["total_bases"],
            "home_runs": row["home_runs"],
            "actual_k_pct": row["actual_k_pct"],
            "actual_tb_per_pa": row["actual_tb_per_pa"],
            "graded_at": datetime.now(timezone.utc),
        })
        if result.rowcount > 0:
            graded += 1

    db.commit()

    summary_sql = text("""
        WITH graded AS (
            SELECT
                proj_k_pct, proj_tb_per_pa, proj_tb_game, proj_woba,
                actual_k_pct, actual_tb_per_pa, actual_total_bases,
                actual_pa, batter_name, pitcher_name
            FROM mlb.matchup_projections
            WHERE game_pk = :game_pk
            AND graded_at IS NOT NULL
            AND actual_k_pct IS NOT NULL
            AND actual_pa >= 2
        )
        SELECT
            COUNT(*) as n,
            ROUND(AVG(ABS(proj_k_pct - actual_k_pct))::numeric, 3) as mae_k,
            ROUND(SQRT(AVG(POWER(proj_k_pct - actual_k_pct, 2)))::numeric, 3) as rmse_k,
            ROUND(CORR(proj_k_pct, actual_k_pct)::numeric, 3) as pearson_k,
            ROUND(AVG(ABS(proj_tb_per_pa - actual_tb_per_pa))::numeric, 3) as mae_tb,
            ROUND(SQRT(AVG(POWER(proj_tb_per_pa - actual_tb_per_pa, 2)))::numeric, 3) as rmse_tb,
            ROUND(CORR(proj_tb_per_pa, actual_tb_per_pa)::numeric, 3) as pearson_tb,
            ROUND(AVG(POWER(
                CASE WHEN actual_k_pct > 0 THEN 1.0 ELSE 0.0 END -
                LEAST(proj_k_pct * 2.5, 0.99)
            , 2))::numeric, 3) as brier_k,
            ROUND(AVG(POWER(
                CASE WHEN actual_total_bases > 0 THEN 1.0 ELSE 0.0 END -
                LEAST(proj_tb_game / 3.0, 0.99)
            , 2))::numeric, 3) as brier_tb_any,
            ROUND(AVG(CASE WHEN proj_k_pct >= 0.28 THEN actual_k_pct END)::numeric, 3) as actual_k_high_proj,
            ROUND(AVG(CASE WHEN proj_k_pct < 0.20 THEN actual_k_pct END)::numeric, 3) as actual_k_low_proj,
            COUNT(CASE WHEN proj_k_pct >= 0.28 THEN 1 END) as n_high_k_proj,
            COUNT(CASE WHEN proj_k_pct < 0.20 THEN 1 END) as n_low_k_proj,
            ROUND(AVG(ABS(0.227 - actual_k_pct))::numeric, 3) as mae_k_naive,
            ROUND(AVG(ABS(0.260 - actual_tb_per_pa))::numeric, 3) as mae_tb_naive,
            ROUND((1 - (AVG(POWER(proj_k_pct - actual_k_pct, 2)) /
                   NULLIF(AVG(POWER(0.227 - actual_k_pct, 2)), 0)))::numeric, 3) as skill_score_k,
            ROUND((1 - (AVG(POWER(proj_tb_per_pa - actual_tb_per_pa, 2)) /
                   NULLIF(AVG(POWER(0.260 - actual_tb_per_pa, 2)), 0)))::numeric, 3) as skill_score_tb
        FROM graded
    """)
    summary = db.execute(summary_sql, {"game_pk": game_pk}).mappings().first()

    return {
        "game_pk": game_pk,
        "graded": graded,
        "accuracy": {
            "n": summary["n"],
            "mae_k": summary["mae_k"],
            "rmse_k": summary["rmse_k"],
            "pearson_k": summary["pearson_k"],
            "mae_k_naive": summary["mae_k_naive"],
            "skill_score_k": summary["skill_score_k"],
            "mae_tb": summary["mae_tb"],
            "rmse_tb": summary["rmse_tb"],
            "pearson_tb": summary["pearson_tb"],
            "mae_tb_naive": summary["mae_tb_naive"],
            "skill_score_tb": summary["skill_score_tb"],
            "brier_k": summary["brier_k"],
            "brier_tb_any": summary["brier_tb_any"],
            "actual_k_high_proj": summary["actual_k_high_proj"],
            "actual_k_low_proj": summary["actual_k_low_proj"],
            "n_high_k_proj": summary["n_high_k_proj"],
            "n_low_k_proj": summary["n_low_k_proj"],
        } if summary else {}
    }


@router.get("/matchup/grade/summary")
def grade_summary(season: int = 2026, db: Session = Depends(get_db)):
    """
    Aggregate accuracy metrics across all graded matchup projections.
    Use this to track model performance over the season.
    """
    from sqlalchemy import text

    sql = text("""
        WITH graded AS (
            SELECT *
            FROM mlb.matchup_projections
            WHERE graded_at IS NOT NULL
            AND actual_k_pct IS NOT NULL
            AND actual_pa >= 2
            AND season = :season
        )
        SELECT
            COUNT(*) as n,
            COUNT(DISTINCT game_pk) as games,
            COUNT(DISTINCT pitcher_id) as pitchers,
            ROUND(AVG(ABS(proj_k_pct - actual_k_pct))::numeric, 3) as mae_k,
            ROUND(SQRT(AVG(POWER(proj_k_pct - actual_k_pct, 2)))::numeric, 3) as rmse_k,
            ROUND(CORR(proj_k_pct, actual_k_pct)::numeric, 3) as pearson_k,
            ROUND(AVG(ABS(0.227 - actual_k_pct))::numeric, 3) as mae_k_naive,
            ROUND((1 - (AVG(POWER(proj_k_pct - actual_k_pct, 2)) /
                   NULLIF(AVG(POWER(0.227 - actual_k_pct, 2)), 0)))::numeric, 3) as skill_score_k,
            ROUND(AVG(ABS(proj_tb_per_pa - actual_tb_per_pa))::numeric, 3) as mae_tb,
            ROUND(SQRT(AVG(POWER(proj_tb_per_pa - actual_tb_per_pa, 2)))::numeric, 3) as rmse_tb,
            ROUND(CORR(proj_tb_per_pa, actual_tb_per_pa)::numeric, 3) as pearson_tb,
            ROUND(AVG(ABS(0.260 - actual_tb_per_pa))::numeric, 3) as mae_tb_naive,
            ROUND((1 - (AVG(POWER(proj_tb_per_pa - actual_tb_per_pa, 2)) /
                   NULLIF(AVG(POWER(0.260 - actual_tb_per_pa, 2)), 0)))::numeric, 3) as skill_score_tb,
            ROUND(AVG(POWER(
                CASE WHEN actual_k_pct > 0 THEN 1.0 ELSE 0.0 END -
                LEAST(proj_k_pct * 2.5, 0.99)
            , 2))::numeric, 3) as brier_k,
            ROUND(AVG(POWER(
                CASE WHEN actual_total_bases > 0 THEN 1.0 ELSE 0.0 END -
                LEAST(proj_tb_game / 3.0, 0.99)
            , 2))::numeric, 3) as brier_tb_any,
            ROUND(AVG(CASE WHEN proj_k_pct >= 0.28 THEN actual_k_pct END)::numeric, 3) as actual_k_high_proj,
            ROUND(AVG(CASE WHEN proj_k_pct BETWEEN 0.20 AND 0.27 THEN actual_k_pct END)::numeric, 3) as actual_k_mid_proj,
            ROUND(AVG(CASE WHEN proj_k_pct < 0.20 THEN actual_k_pct END)::numeric, 3) as actual_k_low_proj,
            COUNT(CASE WHEN proj_k_pct >= 0.28 THEN 1 END) as n_high_k,
            COUNT(CASE WHEN proj_k_pct BETWEEN 0.20 AND 0.27 THEN 1 END) as n_mid_k,
            COUNT(CASE WHEN proj_k_pct < 0.20 THEN 1 END) as n_low_k
        FROM graded
    """)

    result = db.execute(sql, {"season": season}).mappings().first()

    return {
        "season": season,
        "sample": {
            "n": result["n"],
            "games": result["games"],
            "pitchers": result["pitchers"],
        },
        "k_pct": {
            "mae": result["mae_k"],
            "rmse": result["rmse_k"],
            "pearson": result["pearson_k"],
            "mae_naive_baseline": result["mae_k_naive"],
            "skill_score": result["skill_score_k"],
            "interpretation": "skill_score > 0 means beats league-avg naive baseline",
        },
        "tb_per_pa": {
            "mae": result["mae_tb"],
            "rmse": result["rmse_tb"],
            "pearson": result["pearson_tb"],
            "mae_naive_baseline": result["mae_tb_naive"],
            "skill_score": result["skill_score_tb"],
        },
        "brier": {
            "k_any": result["brier_k"],
            "tb_any": result["brier_tb_any"],
            "interpretation": "Lower is better. Coin flip = 0.25, good model ~0.20",
        },
        "calibration": {
            "high_proj_k_actual": result["actual_k_high_proj"],
            "mid_proj_k_actual": result["actual_k_mid_proj"],
            "low_proj_k_actual": result["actual_k_low_proj"],
            "n_high": result["n_high_k"],
            "n_mid": result["n_mid_k"],
            "n_low": result["n_low_k"],
            "interpretation": "High proj should produce higher actual K% than low proj",
        },
    }


@router.get("/marcel/{pitcher_id}")
def marcel_projection(pitcher_id: int, season: int = 2026, db: Session = Depends(get_db)):
    """
    Marcel-style projection for a pitcher.
    Marcel uses 3 years of data weighted 5/4/3 with regression to mean.
    Fangraphs lookup uses name join since player_id_map is incomplete.
    Reference: Tom Tango's Marcel projection system.
    """
    from sqlalchemy import text

    sql = text("""
        WITH fg_data AS (
            SELECT
                f.season, f.era, f.fip, f.xfip, f.siera,
                f.k_pct, f.bb_pct, f.ip, f.war, f.swstr_pct,
                CASE
                    WHEN f.season = :prev1 THEN 5
                    WHEN f.season = :prev2 THEN 4
                    WHEN f.season = :prev3 THEN 3
                    ELSE 0
                END as marcel_weight
            FROM mlb.fangraphs_pitching f
            WHERE LOWER(f.player_name) = (
                SELECT LOWER(MAX(pitcher_name)) FROM mlb.at_bats WHERE pitcher_id = :pid
            )
            AND f.season BETWEEN :prev3 AND :prev1
            AND f.ip >= 10
        ),
        league_avgs AS (
            SELECT
                AVG(k_pct) as lg_k, AVG(bb_pct) as lg_bb,
                AVG(fip) as lg_fip, AVG(era) as lg_era, AVG(siera) as lg_siera
            FROM mlb.fangraphs_pitching
            WHERE season = :prev1 AND ip >= 50
        ),
        weighted AS (
            SELECT
                SUM(k_pct * marcel_weight * ip) / NULLIF(SUM(marcel_weight * ip), 0) as w_k_pct,
                SUM(bb_pct * marcel_weight * ip) / NULLIF(SUM(marcel_weight * ip), 0) as w_bb_pct,
                SUM(fip * marcel_weight * ip) / NULLIF(SUM(marcel_weight * ip), 0) as w_fip,
                SUM(era * marcel_weight * ip) / NULLIF(SUM(marcel_weight * ip), 0) as w_era,
                SUM(siera * marcel_weight * ip) / NULLIF(SUM(marcel_weight * ip), 0) as w_siera,
                SUM(ip * marcel_weight) / NULLIF(SUM(marcel_weight), 0) as w_ip,
                SUM(marcel_weight * ip) as total_weight
            FROM fg_data
        )
        SELECT
            w.*,
            la.lg_k, la.lg_bb, la.lg_fip, la.lg_era, la.lg_siera,
            CASE WHEN w.total_weight > 0
                THEN 1.0 - (200.0 / (w.w_ip + 200.0))
                ELSE 0 END as reliability
        FROM weighted w
        CROSS JOIN league_avgs la
    """)

    result = db.execute(sql, {
        "pid": pitcher_id,
        "season": season,
        "prev1": season - 1,
        "prev2": season - 2,
        "prev3": season - 3,
    }).mappings().first()

    if not result or not result["w_k_pct"]:
        raise HTTPException(status_code=404, detail="Insufficient data for Marcel projection")

    r = float(result["reliability"])
    proj_k     = (r * float(result["w_k_pct"]))  + ((1-r) * float(result["lg_k"]))
    proj_bb    = (r * float(result["w_bb_pct"])) + ((1-r) * float(result["lg_bb"]))
    proj_fip   = (r * float(result["w_fip"]))    + ((1-r) * float(result["lg_fip"]))
    proj_era   = (r * float(result["w_era"]))    + ((1-r) * float(result["lg_era"]))
    proj_siera = (r * float(result["w_siera"]))  + ((1-r) * float(result["lg_siera"]))

    name_sql = text("SELECT MAX(pitcher_name) FROM mlb.at_bats WHERE pitcher_id = :pid")
    pitcher_name = db.execute(name_sql, {"pid": pitcher_id}).scalar() or "Unknown"

    bapv_sql = text("""
        SELECT ROUND(AVG(bapv_plus)::numeric, 1) as bapv_plus,
               SUM(pitches_thrown) as pitches
        FROM mlb.pitch_quality_scores
        WHERE pitcher_id = :pid AND season = :prev1 AND game_type = 'R'
    """)
    bapv = db.execute(bapv_sql, {"pid": pitcher_id, "prev1": season - 1}).mappings().first()

    return {
        "pitcher_id": pitcher_id,
        "pitcher_name": pitcher_name,
        "projection_season": season,
        "marcel": {
            "proj_k_pct": round(proj_k * 100, 1),
            "proj_bb_pct": round(proj_bb * 100, 1),
            "proj_fip": round(proj_fip, 2),
            "proj_era": round(proj_era, 2),
            "proj_siera": round(proj_siera, 2),
            "reliability": round(r, 3),
            "note": "Marcel weights: 5×(n-1) + 4×(n-2) + 3×(n-3), regressed to league mean",
        },
        "bapv_context": {
            "bapv_plus_2025": float(bapv["bapv_plus"]) if bapv and bapv["bapv_plus"] else None,
            "pitches_2025": int(bapv["pitches"]) if bapv and bapv["pitches"] else None,
        },
        "methodology": {
            "marcel_reference": "Tom Tango, 2004 — weighted 3-year average regressed to league mean",
            "reliability_formula": "1 - (200 / (weighted_IP + 200))",
            "use_as_baseline": "Compare BAPV+ predictions against Marcel to measure added value",
        },
    }
