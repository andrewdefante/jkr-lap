from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from database import get_db
from models.mlb import MLBRawEvent
import httpx
from datetime import datetime
import time
import asyncio
import math

_cache: dict = {}
router = APIRouter()


def cache_get(key: str, ttl_seconds: int):
    entry = _cache.get(key)
    if entry and (time.time() - entry[0]) < ttl_seconds:
        return entry[1]
    return None


def cache_set(key: str, val):
    _cache[key] = (time.time(), val)


def cache_bust(key: str):
    _cache.pop(key, None)
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

    cached = cache_get('live_games', 60)
    if cached:
        return cached

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
                        current_ab = db.execute(text("""
                            SELECT ab.pitcher_name, ab.batter_name,
                                   ab.pitcher_id, ab.batter_id
                            FROM mlb.at_bats ab
                            WHERE ab.game_pk = :gp
                            ORDER BY ab.id DESC
                            LIMIT 1
                        """), {"gp": game_pk}).mappings().first()
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
                            "current_pitcher": current_ab["pitcher_name"] if current_ab else None,
                            "current_batter": current_ab["batter_name"] if current_ab else None,
                            "current_pitcher_id": current_ab["pitcher_id"] if current_ab else None,
                            "current_batter_id": current_ab["batter_id"] if current_ab else None,
                        })
        result = {"games": games}
        cache_set('live_games', result)
        return result
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
    import decimal

    def safe_row(row) -> dict:
        out = {}
        for k, v in dict(row).items():
            if isinstance(v, decimal.Decimal):
                out[k] = None if v.is_nan() else float(v)
            else:
                out[k] = v
        return out

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

        goose_sql = text("""
            SELECT
                gs.pitch_type_code,
                ROUND(gs.goose_plus::numeric, 1) as goose_plus,
                ROUND(gs.goose_plus_vs_lhh::numeric, 1) as goose_plus_vs_lhh,
                ROUND(gs.goose_plus_vs_rhh::numeric, 1) as goose_plus_vs_rhh,
                ROUND(gs.avg_velo::numeric, 1) as avg_velo,
                ROUND(gs.avg_spin::numeric, 0) as avg_spin,
                ROUND(gs.avg_hbreak::numeric, 2) as avg_hbreak,
                ROUND(gs.avg_vbreak::numeric, 2) as avg_vbreak,
                gs.velo_quintile,
                gs.velo_score,
                gs.spin_score,
                gs.outcome_score,
                gs.pitches_thrown,
                gb.avg_velo as bucket_avg_velo,
                ROUND(gb.whiff_pct::numeric * 100, 1) as bucket_whiff_pct,
                ROUND(gb.strike_pct::numeric * 100, 1) as bucket_strike_pct
            FROM mlb.goose_scores gs
            LEFT JOIN mlb.goose_bucket_outcomes gb
                ON gb.pitch_type_code = gs.pitch_type_code
                AND gb.velo_quintile = gs.velo_quintile
            WHERE gs.pitcher_id = :pid AND gs.season = 2026
            ORDER BY gs.pitches_thrown DESC
        """)
        goose_rows = db.execute(goose_sql, {"pid": pitcher_id}).mappings().all()

        overall_goose_sql = text("""
            SELECT
                ROUND(goose_plus::numeric, 1) as goose_plus,
                ROUND(goose_plus_vs_lhh::numeric, 1) as goose_plus_vs_lhh,
                ROUND(goose_plus_vs_rhh::numeric, 1) as goose_plus_vs_rhh,
                total_pitches
            FROM mlb.goose_overall
            WHERE pitcher_id = :pid AND season = 2026 AND game_pk IS NULL
        """)
        overall_goose = db.execute(overall_goose_sql, {"pid": pitcher_id}).mappings().first()

        # FG stats + K/BB from boxscore
        fg_sql = text("""
            SELECT f.era, f.era_plus, f.fip, f.whip, f.k_pct, f.bb_pct,
                   f.swstr_pct, f.war, f.xera, f.siera
            FROM mlb.fangraphs_pitching f
            JOIN mlb.player_id_map m ON m.fangraphs_id::text = f.mlbam_id::text
            WHERE m.mlbam_id = :pid AND f.season = :season
            LIMIT 1
        """)
        fg_row = db.execute(fg_sql, {"pid": pitcher_id, "season": 2026}).mappings().first()
        if not fg_row:
            fg_row = db.execute(fg_sql, {"pid": pitcher_id, "season": 2025}).mappings().first()

        box_sql = text("""
            SELECT SUM(bp.strikeouts) as season_k,
                   SUM(bp.walks) as season_bb,
                   SUM(bp.innings_pitched) as ip,
                   SUM(bp.games_started) as gs,
                   COUNT(*) as g
            FROM mlb.boxscore_pitching bp
            JOIN mlb.games g ON g.game_pk = bp.game_pk
            WHERE bp.player_id = :pid AND g.season = 2026 AND g.game_type = 'R'
        """)
        box_row = db.execute(box_sql, {"pid": pitcher_id}).mappings().first()

        bbref_sql = text("""
            SELECT era_plus, war as bbref_war,
                   ROUND(ip_outs::numeric / 3, 1) as ip
            FROM mlb.bbref_pitching
            WHERE mlb_id = :pid AND year_id = :season
            LIMIT 1
        """)
        bbref = db.execute(bbref_sql, {"pid": pitcher_id, "season": 2026}).mappings().first()

        headshot_url = f"https://img.mlbstatic.com/mlb-photos/image/upload/d_people:generic:headshot:67:current.png/w_213,q_auto:best/v1/people/{pitcher_id}/headshot/67/current"

        # Merge fg_stats with bbref era_plus (bbref is source of truth for 2026)
        fg_merged = safe_row(fg_row) if fg_row else {}
        if bbref and bbref["era_plus"] is not None:
            fg_merged["era_plus"] = float(bbref["era_plus"])
        if bbref and bbref["bbref_war"] is not None:
            fg_merged["war"] = float(bbref["bbref_war"])

        pk_sql = text("""
            SELECT ROUND(pk_plus::numeric,1) as pk_plus,
                   ROUND(pk_vs_lhh::numeric,1) as pk_vs_lhh,
                   ROUND(pk_vs_rhh::numeric,1) as pk_vs_rhh
            FROM mlb.pk_overall
            WHERE pitcher_id = :pid AND season = :season
        """)
        phr_sql = text("""
            SELECT ROUND(phr_plus::numeric,1) as phr_plus,
                   ROUND(phr_vs_lhh::numeric,1) as phr_vs_lhh,
                   ROUND(phr_vs_rhh::numeric,1) as phr_vs_rhh
            FROM mlb.phr_overall
            WHERE pitcher_id = :pid AND season = :season
        """)
        pk  = db.execute(pk_sql,  {"pid": pitcher_id, "season": 2026}).mappings().first()
        phr = db.execute(phr_sql, {"pid": pitcher_id, "season": 2026}).mappings().first()

        return {
            "pitcher_id": pitcher_id,
            "pitcher_name": name,
            "pitch_arsenal": [safe_row(r) for r in rows],
            "goose_plus": {
                "overall": safe_row(overall_goose) if overall_goose else None,
                "by_pitch": [safe_row(r) for r in goose_rows],
            },
            "pk":  safe_row(pk)  if pk  else None,
            "phr": safe_row(phr) if phr else None,
            "fg_stats": fg_merged,
            "season_k": int(box_row["season_k"]) if box_row and box_row["season_k"] else None,
            "season_bb": int(box_row["season_bb"]) if box_row and box_row["season_bb"] else None,
            "season_ip": round(float(box_row["ip"]), 1) if box_row and box_row["ip"] else None,
            "season_gs": int(box_row["gs"]) if box_row and box_row["gs"] else None,
            "season_g": int(box_row["g"]) if box_row and box_row["g"] else None,
            "headshot_url": headshot_url,
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
    cache_key = f"leaderboard_{season}_{min_pitches}_{pitch_type}"
    cached = cache_get(cache_key, 300)
    if cached:
        return cached

    from sqlalchemy import text
    sql = text("""
        SELECT
            pq.pitcher_id,
            MAX(pq.pitcher_name) as pitcher_name,
            pq.pitch_type_code as pitch_type,
            SUM(pq.pitches_thrown) as total_pitches,
            ROUND(AVG(pq.bapv_plus)::numeric, 1) as season_bapv_plus,
            ROUND(AVG(pq.avg_velo)::numeric, 1) as avg_velo,
            ROUND(AVG(pq.whiff_rate)::numeric, 3) as whiff_rate,
            ROUND(AVG(pq.csw_rate)::numeric, 3) as csw_rate,
            ROUND(AVG(pq.hard_hit_rate)::numeric, 3) as hard_hit_rate,
            COUNT(DISTINCT pq.game_pk) as games,
            MAX(pk.pk_plus) as pk_plus,
            MAX(phr.phr_plus) as phr_plus,
            MAX(g3.goose3_plus) as goose3_plus,
            MAX(so.stuff_score) as stuff_score
        FROM mlb.pitch_quality_scores pq
        LEFT JOIN mlb.pk_overall pk
            ON pk.pitcher_id = pq.pitcher_id AND pk.season = pq.season
        LEFT JOIN mlb.phr_overall phr
            ON phr.pitcher_id = pq.pitcher_id AND phr.season = pq.season
        LEFT JOIN mlb.goose3_overall g3
            ON g3.pitcher_id = pq.pitcher_id AND g3.season = pq.season
        LEFT JOIN mlb.stuff_overall so
            ON so.pitcher_id = pq.pitcher_id AND so.season = pq.season
        WHERE pq.season = :season
        AND pq.game_type = 'R'
        GROUP BY pq.pitcher_id, pq.pitch_type_code
        HAVING SUM(pq.pitches_thrown) >= :min_pitches
        ORDER BY MAX(g3.goose3_plus) DESC NULLS LAST
        LIMIT 200
    """)
    rows = db.execute(sql, {"season": season, "min_pitches": min_pitches}).mappings().all()
    result = {"pitchers": [dict(r) for r in rows]}
    cache_set(cache_key, result)
    return result


@router.get("/pitcher/pk-phr/leaderboard")
def pk_phr_leaderboard(season: int = 2026, min_pitches: int = 100,
                       db: Session = Depends(get_db)):
    from sqlalchemy import text
    sql = text("""
        SELECT
            pk.pitcher_id, pk.pitcher_name, pk.pitch_hand,
            ROUND(pk.pk_plus::numeric, 1)     as pk_plus,
            ROUND(pk.pk_vs_lhh::numeric, 1)   as pk_vs_lhh,
            ROUND(pk.pk_vs_rhh::numeric, 1)   as pk_vs_rhh,
            ROUND(phr.phr_plus::numeric, 1)   as phr_plus,
            ROUND(phr.phr_vs_lhh::numeric, 1) as phr_vs_lhh,
            ROUND(phr.phr_vs_rhh::numeric, 1) as phr_vs_rhh,
            ROUND(so.stuff_score::numeric, 1)  as stuff_score,
            ROUND(g3.goose3_plus::numeric, 1)  as goose3_plus,
            pk.total_pitches,
            br.era_plus
        FROM mlb.pk_overall pk
        LEFT JOIN mlb.phr_overall phr ON phr.pitcher_id = pk.pitcher_id AND phr.season = pk.season
        LEFT JOIN mlb.stuff_overall so  ON so.pitcher_id  = pk.pitcher_id AND so.season  = pk.season
        LEFT JOIN mlb.goose3_overall g3 ON g3.pitcher_id  = pk.pitcher_id AND g3.season  = pk.season
        LEFT JOIN mlb.bbref_pitching br ON br.mlb_id = pk.pitcher_id AND br.year_id = pk.season
        WHERE pk.season = :season AND pk.total_pitches >= :min_pitches
        ORDER BY pk.pk_plus DESC
        LIMIT 100
    """)
    rows = db.execute(sql, {"season": season, "min_pitches": min_pitches}).mappings().all()
    return {"season": season, "pitchers": [dict(r) for r in rows]}


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

    cached = cache_get('schedule_today', 300)
    if cached:
        return cached

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

    result = {"games": games, "date": date.today().isoformat()}
    cache_set('schedule_today', result)
    return result


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
    _bsc_key = f"boxscore_{game_pk}"
    boxscore = cache_get(_bsc_key, 120)
    if boxscore is None:
        try:
            res = httpx.get(
                f"https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore",
                timeout=10
            )
            boxscore = res.json()
            cache_set(_bsc_key, boxscore)
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

    # 2b. Get platoon mix — pitch usage/outcomes split by batter handedness
    platoon_sql = text("""
        SELECT pitch_type_code, bat_side, usage_pct, whiff_rate, csw_rate, pitches
        FROM mlb.pitcher_mix_profile
        WHERE pitcher_id = :pid
        AND season = :season
        ORDER BY bat_side, usage_pct DESC
    """)
    platoon_rows = db.execute(platoon_sql, {
        "pid": pitcher_id, "season": tend_season
    }).mappings().all()

    # Build platoon lookup: {bat_side: {pitch_type: {usage, whiff, csw, pitches}}}
    platoon_mix = {}
    for r in platoon_rows:
        side = r["bat_side"]
        if side not in platoon_mix:
            platoon_mix[side] = {}
        platoon_mix[side][r["pitch_type_code"]] = {
            "usage": float(r["usage_pct"]),
            "whiff_rate": float(r["whiff_rate"]) if r["whiff_rate"] else None,
            "csw_rate": float(r["csw_rate"]) if r["csw_rate"] else None,
            "pitches": int(r["pitches"]),
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
        batter_side = batter.get("bat_side", "R")

        # Build effective mix: platoon if available (min 20 pitches), else season average
        effective_mix = {}
        for pt, season_stats in pitch_usage.items():
            platoon_stats = platoon_mix.get(batter_side, {}).get(pt)
            if platoon_stats and platoon_stats["pitches"] >= 20:
                effective_mix[pt] = {
                    "usage": platoon_stats["usage"],
                    "bapv_plus": season_stats["bapv_plus"],
                    "whiff_rate": platoon_stats["whiff_rate"] or season_stats["whiff_rate"],
                    "csw_rate": platoon_stats["csw_rate"] or season_stats["csw_rate"],
                    "avg_velo": season_stats["avg_velo"],
                    "mix_source": "platoon",
                }
            else:
                effective_mix[pt] = {**season_stats, "mix_source": "season_avg"}

        # Weighted whiff rate across pitcher's arsenal using effective mix
        weighted_whiff = 0.0
        weighted_csw = 0.0
        total_weight = 0.0

        for pt, pitch_stats in effective_mix.items():
            usage = pitch_stats["usage"]
            pitcher_whiff = pitch_stats["whiff_rate"]
            pitcher_csw = pitch_stats["csw_rate"]
            batter_whiff = btend.get(pt, {}).get("whiff_rate", LEAGUE_WHIFF) or LEAGUE_WHIFF

            batter_adj = batter_whiff / LEAGUE_WHIFF
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
            (btend.get(pt, {}).get("whiff_rate", LEAGUE_WHIFF) or LEAGUE_WHIFF) * effective_mix[pt]["usage"]
            for pt in effective_mix
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
            effective_mix[pt]["bapv_plus"] * effective_mix[pt]["usage"]
            for pt in effective_mix
        )

        results.append({
            "player_id": bid,
            "name": batter["name"],
            "batting_order": batter["batting_order"],
            "bat_side": batter_side,
            "mix_source": "platoon" if batter_side in platoon_mix else "season_avg",
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

    # Get batter's bat side from at_bats history
    bat_side_sql = text("""
        SELECT bat_side FROM mlb.at_bats
        WHERE batter_id = :bid AND bat_side IS NOT NULL
        LIMIT 1
    """)
    bat_side_result = db.execute(bat_side_sql, {"bid": batter_id}).scalar()
    batter_bat_side = bat_side_result or 'R'

    # Get platoon mix — pitch usage/outcomes split by batter handedness
    platoon_sql = text("""
        SELECT pitch_type_code, bat_side, usage_pct, whiff_rate, csw_rate, pitches
        FROM mlb.pitcher_mix_profile
        WHERE pitcher_id = :pid
        AND season = :season
        ORDER BY bat_side, usage_pct DESC
    """)
    platoon_rows = db.execute(platoon_sql, {
        "pid": pitcher_id, "season": tend_season
    }).mappings().all()

    platoon_mix = {}
    for r in platoon_rows:
        side = r["bat_side"]
        if side not in platoon_mix:
            platoon_mix[side] = {}
        platoon_mix[side][r["pitch_type_code"]] = {
            "usage": float(r["usage_pct"]),
            "whiff_rate": float(r["whiff_rate"]) if r["whiff_rate"] else None,
            "csw_rate": float(r["csw_rate"]) if r["csw_rate"] else None,
            "pitches": int(r["pitches"]),
        }

    LEAGUE_WHIFF = 0.267
    LEAGUE_HH = 0.395

    pitch_results = []
    weighted_k_pct = 0.0
    weighted_woba = 0.0
    weighted_hard_hit = 0.0
    total_weight = 0.0

    for p in pitch_mix:
        pt = p["pitch_type_code"]
        season_usage = p["total_pitches"] / total_pitches

        # Use platoon mix usage/whiff/csw if available (min 20 pitches), else season avg
        platoon_stats = platoon_mix.get(batter_bat_side, {}).get(pt)
        if platoon_stats and platoon_stats["pitches"] >= 20:
            usage = platoon_stats["usage"]
            mix_source = "platoon"
            pitcher_whiff = float(platoon_stats["whiff_rate"] or p["pitcher_whiff"] or LEAGUE_WHIFF)
            pitcher_csw = float(platoon_stats["csw_rate"] or p["pitcher_csw"] or 0.30)
        else:
            usage = season_usage
            mix_source = "season_avg"
            pitcher_whiff = float(p["pitcher_whiff"] or LEAGUE_WHIFF)
            pitcher_csw = float(p["pitcher_csw"] or 0.30)
        pitcher_hh_allowed = float(p["pitcher_hard_hit_allowed"] or LEAGUE_HH)
        pitcher_bapv = float(p["bapv_plus"] or 100)
        bt = batter_tend.get(pt, {})

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
            "mix_source": mix_source,
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
        "bat_side": batter_bat_side,
        "mix_source": "platoon" if batter_bat_side in platoon_mix else "season_avg",
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
    _bsc_key = f"boxscore_{game_pk}"
    boxscore = cache_get(_bsc_key, 120)
    if boxscore is None:
        try:
            res = httpx.get(
                f"https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore",
                timeout=10
            )
            boxscore = res.json()
            cache_set(_bsc_key, boxscore)
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


@router.get("/leaderboard/expanded")
def expanded_leaderboard(
    season: int = 2025,
    min_pitches: int = 200,
    db: Session = Depends(get_db)
):
    """
    Expanded leaderboard with rate stats and pitch group mix percentages.
    Joins pitch_quality_scores with fangraphs_pitching and pitcher_mix_profile.
    """
    from sqlalchemy import text
    sql = text("""
        WITH bapv AS (
            SELECT
                pitcher_id,
                MAX(pitcher_name) as pitcher_name,
                ROUND(AVG(bapv_plus)::numeric, 1) as bapv_plus,
                SUM(pitches_thrown) as total_pitches,
                COUNT(DISTINCT game_pk) as games,
                ROUND(AVG(whiff_rate)::numeric, 3) as whiff_rate,
                ROUND(AVG(csw_rate)::numeric, 3) as csw_rate,
                ROUND(AVG(hard_hit_rate)::numeric, 3) as hard_hit_rate
            FROM mlb.pitch_quality_scores
            WHERE season = :season AND game_type = 'R'
            GROUP BY pitcher_id
            HAVING SUM(pitches_thrown) >= :min_pitches
        ),
        fg AS (
            SELECT f.mlbam_id, f.k_pct, f.bb_pct, f.ip,
                   f.zone_pct, f.o_swing_pct, f.swstr_pct, f.whiff_pct
            FROM mlb.fangraphs_pitching f
            WHERE f.season = :season AND f.ip >= 20
        ),
        mix AS (
            SELECT pitcher_id,
                ROUND(SUM(CASE WHEN pitch_type_code IN ('FF','SI','FC')
                    THEN pitches ELSE 0 END) * 100.0 / NULLIF(SUM(pitches),0)::numeric, 1) as hard_pct,
                ROUND(SUM(CASE WHEN pitch_type_code IN ('CU','SL','ST','KC')
                    THEN pitches ELSE 0 END) * 100.0 / NULLIF(SUM(pitches),0)::numeric, 1) as break_pct,
                ROUND(SUM(CASE WHEN pitch_type_code IN ('CH','FS')
                    THEN pitches ELSE 0 END) * 100.0 / NULLIF(SUM(pitches),0)::numeric, 1) as soft_pct
            FROM mlb.pitcher_mix_profile
            WHERE season = :season
            GROUP BY pitcher_id
        )
        SELECT
            b.pitcher_id,
            b.pitcher_name,
            b.games as gp,
            ROUND(fg.ip::numeric, 1) as ip,
            b.total_pitches,
            b.bapv_plus,
            ROUND(g3.goose3_plus::numeric, 1) as goose3_plus,
            ROUND(pk.pk_plus::numeric, 1) as pk_plus,
            ROUND(phr.phr_plus::numeric, 1) as phr_plus,
            ROUND(so.stuff_score::numeric, 1) as stuff_score,
            ROUND((fg.k_pct * 100)::numeric, 1) as k_pct,
            ROUND((fg.bb_pct * 100)::numeric, 1) as bb_pct,
            ROUND((fg.zone_pct * 100)::numeric, 1) as strike_pct,
            ROUND((fg.o_swing_pct * 100)::numeric, 1) as chase_pct,
            ROUND((fg.swstr_pct * 100)::numeric, 1) as swstr_pct,
            ROUND((fg.whiff_pct * 100)::numeric, 1) as whiff_pct,
            b.whiff_rate,
            b.csw_rate,
            m.hard_pct,
            m.break_pct,
            m.soft_pct
        FROM bapv b
        LEFT JOIN fg ON fg.mlbam_id = b.pitcher_id
        LEFT JOIN mix m ON m.pitcher_id = b.pitcher_id
        LEFT JOIN mlb.goose3_overall g3 ON g3.pitcher_id = b.pitcher_id AND g3.season = :season
        LEFT JOIN mlb.pk_overall pk ON pk.pitcher_id = b.pitcher_id AND pk.season = :season
        LEFT JOIN mlb.phr_overall phr ON phr.pitcher_id = b.pitcher_id AND phr.season = :season
        LEFT JOIN mlb.stuff_overall so ON so.pitcher_id = b.pitcher_id AND so.season = :season
        ORDER BY g3.goose3_plus DESC NULLS LAST
    """)
    rows = db.execute(sql, {"season": season, "min_pitches": min_pitches}).mappings().all()
    return {"pitchers": [dict(r) for r in rows], "season": season, "n": len(rows)}


@router.get("/batter/{batter_id}/recent")
def batter_recent(batter_id: int, days: int = 7, pitcher_hand: str = 'R', db: Session = Depends(get_db)):
    """
    Batter performance over last N days and 2026 season, filtered to same-handed pitchers.
    Used for matchup engine batter expansion panel.
    """
    from sqlalchemy import text
    from datetime import date, timedelta

    cutoff = (date.today() - timedelta(days=days)).isoformat()
    season_start = '2026-03-26'

    def get_stats(date_filter):
        sql = text("""
            SELECT
                COUNT(DISTINCT bb.game_pk) as games,
                SUM(bb.at_bats) as ab,
                SUM(bb.hits) as h,
                SUM(bb.doubles) as doubles,
                SUM(bb.triples) as triples,
                SUM(bb.home_runs) as hr,
                SUM(bb.strikeouts) as k,
                SUM(bb.total_bases) as tb,
                SUM(bb.hit_by_pitch) as hbp,
                SUM(bb.sac_flies) as sf,
                ROUND(SUM(bb.hits)::numeric / NULLIF(SUM(bb.at_bats),0), 3) as avg,
                ROUND(SUM(bb.total_bases)::numeric / NULLIF(SUM(bb.at_bats),0), 3) as slg,
                ROUND((SUM(bb.hits) + COALESCE(SUM(bb.hit_by_pitch),0))::numeric /
                    NULLIF(SUM(bb.at_bats) + COALESCE(SUM(bb.hit_by_pitch),0) + COALESCE(SUM(bb.sac_flies),0),0), 3) as obp
            FROM mlb.boxscore_batting bb
            JOIN mlb.games g ON g.game_pk = bb.game_pk
            WHERE bb.player_id = :bid
            AND g.game_date >= :cutoff
            AND g.game_type = 'R'
            AND bb.at_bats > 0
            AND bb.game_pk IN (
                SELECT DISTINCT ab2.game_pk FROM mlb.at_bats ab2
                WHERE ab2.batter_id = :bid AND ab2.pitch_hand = :phand
            )
        """)
        result = db.execute(sql, {"bid": batter_id, "cutoff": date_filter, "phand": pitcher_hand}).mappings().first()
        return dict(result) if result else {}

    def get_groups(date_filter):
        sql = text("""
            SELECT
                CASE
                    WHEN p.pitch_type_code IN ('FF','SI','FC') THEN 'Hard'
                    WHEN p.pitch_type_code IN ('CU','SL','ST','KC') THEN 'Breaking'
                    WHEN p.pitch_type_code IN ('CH','FS') THEN 'Soft'
                END as pitch_group,
                COUNT(*) as pitches,
                ROUND(AVG(CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END)::numeric*100,1) as whiff_pct,
                ROUND(AVG(CASE WHEN p.launch_speed >= 95 AND p.call_code='X' THEN 1.0 ELSE 0.0 END)::numeric*100,1) as hard_hit_pct,
                ROUND(AVG(CASE WHEN p.call_code='X' THEN p.launch_speed END)::numeric,1) as avg_ev,
                COUNT(CASE WHEN p.call_code='X' THEN 1 END) as bip
            FROM mlb.pitches p
            JOIN mlb.at_bats ab ON ab.game_pk=p.game_pk AND ab.at_bat_index=p.at_bat_index
            JOIN mlb.games g ON g.game_pk=p.game_pk
            WHERE ab.batter_id=:bid
            AND g.game_date >= :cutoff
            AND g.game_type='R'
            AND p.pitch_type_code IN ('FF','SI','FC','CU','SL','ST','KC','CH','FS')
            AND ab.pitch_hand = :phand
            GROUP BY pitch_group
            HAVING COUNT(*) >= 3
            ORDER BY pitches DESC
        """)
        return db.execute(sql, {"bid": batter_id, "cutoff": date_filter, "phand": pitcher_hand}).mappings().all()

    zone_sql = text("""
        SELECT p.zone,
            COUNT(*) as pitches,
            ROUND(AVG(CASE WHEN p.launch_speed >= 95 AND p.call_code='X' THEN 1.0 ELSE 0.0 END)::numeric*100,1) as hard_hit_pct,
            ROUND(AVG(CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END)::numeric*100,1) as whiff_pct,
            COUNT(CASE WHEN p.call_code='X' THEN 1 END) as bip
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk=p.game_pk AND ab.at_bat_index=p.at_bat_index
        JOIN mlb.games g ON g.game_pk=p.game_pk
        WHERE ab.batter_id=:bid
        AND g.game_date >= :cutoff
        AND g.game_type='R'
        AND p.zone BETWEEN 1 AND 9
        AND ab.pitch_hand = :phand
        GROUP BY p.zone
        ORDER BY p.zone
    """)
    zones_7 = db.execute(zone_sql, {"bid": batter_id, "cutoff": cutoff, "phand": pitcher_hand}).mappings().all()

    pitch_type_sql = text("""
        SELECT
            p.pitch_type_code,
            COUNT(*) as pitches,
            SUM(CASE WHEN p.call_code IN ('B','*B','H') THEN 1 ELSE 0 END) as balls,
            SUM(CASE WHEN p.call_code IN ('S','W','T','C','F','D','E','L','X') THEN 1 ELSE 0 END) as strikes,
            ROUND(AVG(CASE WHEN p.call_code = 'C' THEN 1.0 ELSE 0.0 END)::numeric*100,1) as cs_pct,
            ROUND(
                SUM(CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END) /
                NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X') THEN 1.0 ELSE 0.0 END),0)
            ::numeric*100,1) as whiff_pct,
            ROUND(AVG(CASE WHEN p.call_code IN ('S','W','T','C','F','D','E','L','X')
                THEN 1.0 ELSE 0.0 END)::numeric*100,1) as strike_pct,
            COUNT(CASE WHEN p.call_code='X' THEN 1 END) as bip,
            ROUND(AVG(CASE WHEN p.call_code='X' THEN p.launch_speed END)::numeric,1) as avg_ev
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk=p.game_pk AND ab.at_bat_index=p.at_bat_index
        JOIN mlb.games g ON g.game_pk=p.game_pk
        WHERE ab.batter_id=:bid
        AND g.game_date >= :season_start
        AND g.game_type='R'
        AND p.pitch_type_code IN ('FF','SI','FC','CU','SL','ST','KC','CH','FS')
        AND ab.pitch_hand = :phand
        GROUP BY p.pitch_type_code
        HAVING COUNT(*) >= 3
        ORDER BY pitches DESC
    """)
    pitch_types = db.execute(pitch_type_sql, {
        "bid": batter_id, "season_start": season_start, "phand": pitcher_hand
    }).mappings().all()

    return {
        "batter_id": batter_id,
        "pitcher_hand": pitcher_hand,
        "days": days,
        "last_7": {
            "stats": get_stats(cutoff),
            "by_group": [dict(r) for r in get_groups(cutoff)],
            "by_zone": {r["zone"]: dict(r) for r in zones_7 if r["zone"]},
        },
        "season_2026": {
            "stats": get_stats(season_start),
            "by_group": [dict(r) for r in get_groups(season_start)],
        },
        "pitch_type_splits": [dict(r) for r in pitch_types],
    }


@router.get("/matchup/attack-plan/{pitcher_id}/{batter_id}")
def attack_plan(pitcher_id: int, batter_id: int, db: Session = Depends(get_db)):
    """
    Synthesizes pitcher mix profile vs batter tendencies to produce
    an attack plan: which pitches to use, which to avoid, and why.
    """
    from sqlalchemy import text

    tend_season = 2025
    LEAGUE_WHIFF = 0.267

    bat_side_sql = text("SELECT bat_side FROM mlb.at_bats WHERE batter_id=:bid AND bat_side IS NOT NULL LIMIT 1")
    bat_side = db.execute(bat_side_sql, {"bid": batter_id}).scalar() or 'R'

    mix_sql = text("""
        SELECT pitch_type_code, usage_pct, whiff_rate, csw_rate, avg_velo, pitches
        FROM mlb.pitcher_mix_profile
        WHERE pitcher_id=:pid AND season=:season AND bat_side=:side
        ORDER BY usage_pct DESC
    """)
    mix = db.execute(mix_sql, {"pid": pitcher_id, "season": tend_season, "side": bat_side}).mappings().all()

    tend_sql = text("""
        SELECT pitch_type_code, whiff_rate, chase_rate, hard_hit_rate,
               avg_exit_velo, avg_woba_on_contact, pitches_faced
        FROM mlb.batter_pitch_type_tendencies
        WHERE batter_id=:bid AND season=:season
    """)
    tends = {r["pitch_type_code"]: dict(r) for r in db.execute(tend_sql, {"bid": batter_id, "season": tend_season}).mappings().all()}

    batter_name = db.execute(text("SELECT MAX(batter_name) FROM mlb.at_bats WHERE batter_id=:bid"), {"bid": batter_id}).scalar()
    pitcher_name = db.execute(text("SELECT MAX(pitcher_name) FROM mlb.at_bats WHERE pitcher_id=:pid"), {"pid": pitcher_id}).scalar()
    pitcher_hand = db.execute(text("""
        SELECT pitch_hand FROM mlb.at_bats WHERE pitcher_id=:pid AND pitch_hand IS NOT NULL
        GROUP BY pitch_hand ORDER BY COUNT(*) DESC LIMIT 1
    """), {"pid": pitcher_id}).scalar() or 'R'

    PITCH_GROUPS = {
        'FF': 'Hard', 'SI': 'Hard', 'FC': 'Hard',
        'CU': 'Breaking', 'SL': 'Breaking', 'ST': 'Breaking', 'KC': 'Breaking',
        'CH': 'Soft', 'FS': 'Soft'
    }

    pitch_analysis = []
    for p in mix:
        pt = p["pitch_type_code"]
        bt = tends.get(pt, {})

        pitcher_whiff = float(p["whiff_rate"] or LEAGUE_WHIFF)
        batter_whiff = float(bt.get("whiff_rate") or LEAGUE_WHIFF)
        batter_hh = float(bt.get("hard_hit_rate") or 0.395)
        batter_chase = float(bt.get("chase_rate") or 0.310)
        batter_ev = float(bt.get("avg_exit_velo") or 88.0)
        batter_woba = float(bt.get("avg_woba_on_contact") or 0.380)

        contact_danger = batter_hh - 0.395
        whiff_score = pitcher_whiff - (batter_whiff * 0.5)
        chase_score = batter_chase - 0.310
        contact_penalty = max(0, contact_danger - 0.10)

        advantage_score = (whiff_score * 3.0) + (chase_score * 1.5) - (contact_penalty * 1.0)

        if advantage_score > 0.02:
            recommendation = "USE"
            if pitcher_whiff > 0.25:
                reason = f"High whiff ({pitcher_whiff*100:.0f}%), batter misses {batter_whiff*100:.0f}%"
            elif batter_chase > 0.35:
                reason = f"Batter chases ({batter_chase*100:.0f}%), generates weak contact"
            else:
                reason = f"Pitcher advantage on this pitch type"
        elif advantage_score < -0.03:
            recommendation = "AVOID"
            if batter_hh > 0.50:
                reason = f"Batter hard contact ({batter_hh*100:.0f}% HH, {batter_ev:.0f} mph avg EV)"
            elif batter_whiff < 0.15:
                reason = f"Batter makes consistent contact ({batter_whiff*100:.0f}% whiff)"
            else:
                reason = f"Batter has advantage on this pitch type"
        else:
            recommendation = "NEUTRAL"
            reason = f"Even matchup — {pitcher_whiff*100:.0f}% whiff, {batter_hh*100:.0f}% HH"

        pitch_analysis.append({
            "pitch_type_code": pt,
            "pitch_group": PITCH_GROUPS.get(pt, 'Other'),
            "usage_pct": float(p["usage_pct"]),
            "avg_velo": float(p["avg_velo"] or 0),
            "pitcher_whiff_pct": round(pitcher_whiff * 100, 1),
            "batter_whiff_pct": round(batter_whiff * 100, 1),
            "batter_hard_hit_pct": round(batter_hh * 100, 1),
            "batter_chase_pct": round(batter_chase * 100, 1),
            "batter_avg_ev": batter_ev,
            "batter_woba_contact": batter_woba,
            "advantage_score": round(advantage_score, 3),
            "recommendation": recommendation,
            "reason": reason,
            "has_batter_data": pt in tends,
        })

    order = {"USE": 0, "NEUTRAL": 1, "AVOID": 2}
    pitch_analysis.sort(key=lambda x: (order[x["recommendation"]], -x["usage_pct"]))

    best_pitch = next((p for p in pitch_analysis if p["recommendation"] == "USE"), None)
    avoid_pitch = next((p for p in pitch_analysis if p["recommendation"] == "AVOID"), None)

    return {
        "pitcher_id": pitcher_id,
        "pitcher_name": pitcher_name,
        "pitcher_hand": pitcher_hand,
        "batter_id": batter_id,
        "batter_name": batter_name,
        "bat_side": bat_side,
        "pitches": pitch_analysis,
        "summary": {
            "best_pitch": best_pitch["pitch_type_code"] if best_pitch else None,
            "best_pitch_reason": best_pitch["reason"] if best_pitch else None,
            "avoid_pitch": avoid_pitch["pitch_type_code"] if avoid_pitch else None,
            "avoid_pitch_reason": avoid_pitch["reason"] if avoid_pitch else None,
        }
    }


@router.get("/pitcher/{pitcher_id}/pitch-counts")
def pitcher_pitch_counts(pitcher_id: int, season: int = 2026, bat_side: str = None, db: Session = Depends(get_db)):
    """Ball and strike counts per pitch type for a pitcher. bat_side=L/R to filter by batter handedness."""
    from sqlalchemy import text
    bat_filter = "AND ab.bat_side = :bat_side" if bat_side else ""
    params = {"pid": pitcher_id, "season": season}
    if bat_side:
        params["bat_side"] = bat_side
    sql = text(f"""
        SELECT
            p.pitch_type_code,
            COUNT(*) as total_pitches,
            SUM(CASE WHEN p.call_code IN ('B','*B','H') THEN 1 ELSE 0 END) as balls,
            SUM(CASE WHEN p.call_code IN ('S','W','T','C','F','D','E','L','X') THEN 1 ELSE 0 END) as strikes,
            ROUND(
                SUM(CASE WHEN p.call_code IN ('S','W','T','C','F','D','E','L','X') THEN 1.0 ELSE 0.0 END) /
                NULLIF(COUNT(*), 0) * 100
            ::numeric, 1) as strike_pct
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk=p.game_pk AND ab.at_bat_index=p.at_bat_index
        JOIN mlb.games g ON g.game_pk=p.game_pk
        WHERE ab.pitcher_id=:pid
        AND g.season=:season
        AND g.game_type='R'
        AND p.pitch_type_code IS NOT NULL
        {bat_filter}
        GROUP BY p.pitch_type_code
        HAVING COUNT(*) >= 5
        ORDER BY total_pitches DESC
    """)
    rows = db.execute(sql, params).mappings().all()
    return {"pitcher_id": pitcher_id, "season": season, "bat_side": bat_side, "counts": [dict(r) for r in rows]}


@router.get("/pitcher/{pitcher_id}/pitch-counts-full")
def pitcher_pitch_counts_full(pitcher_id: int, season: int = 2026, db: Session = Depends(get_db)):
    """Season + last-14-day pitch counts with velo/whiff/csw/usage, plus league averages."""
    from sqlalchemy import text
    from datetime import date, timedelta

    cutoff_14 = (date.today() - timedelta(days=14)).isoformat()

    _SELECT = """
                p.pitch_type_code,
                COUNT(*) as total_pitches,
                SUM(CASE WHEN p.call_code IN ('B','*B','H') THEN 1 ELSE 0 END) as balls,
                SUM(CASE WHEN p.call_code IN ('S','W','T','C','F','D','E','L','X') THEN 1 ELSE 0 END) as strikes,
                ROUND(SUM(CASE WHEN p.call_code IN ('S','W','T','C','F','D','E','L','X') THEN 1.0 ELSE 0.0 END) /
                    NULLIF(COUNT(*), 0) * 100 ::numeric, 1) as strike_pct,
                ROUND(AVG(p.start_speed)::numeric, 1) as avg_velo,
                ROUND(SUM(CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END) /
                    NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X') THEN 1.0 ELSE 0.0 END), 0) * 100
                ::numeric, 1) as whiff_pct,
                ROUND(SUM(CASE WHEN p.call_code IN ('S','W','T','C') THEN 1.0 ELSE 0.0 END) /
                    NULLIF(COUNT(*), 0) * 100 ::numeric, 1) as csw_pct"""

    def get_counts(bat_side=None, cutoff=None):
        bat_filter = "AND ab.bat_side = :bat_side" if bat_side else ""
        date_filter = "AND g.game_date >= :cutoff" if cutoff else "AND g.season = :season"
        params = {"pid": pitcher_id, "season": season}
        if bat_side:
            params["bat_side"] = bat_side
        if cutoff:
            params["cutoff"] = cutoff
        sql = text(f"""
            WITH raw AS (
                SELECT {_SELECT}
                FROM mlb.pitches p
                JOIN mlb.at_bats ab ON ab.game_pk=p.game_pk AND ab.at_bat_index=p.at_bat_index
                JOIN mlb.games g ON g.game_pk=p.game_pk
                WHERE ab.pitcher_id=:pid AND g.game_type='R'
                AND p.pitch_type_code IS NOT NULL
                {date_filter}
                {bat_filter}
                GROUP BY p.pitch_type_code
                HAVING COUNT(*) >= 5
            )
            SELECT *,
                ROUND(total_pitches * 100.0 / NULLIF(SUM(total_pitches) OVER(), 0) ::numeric, 1) as usage_pct
            FROM raw ORDER BY total_pitches DESC
        """)
        return [dict(r) for r in db.execute(sql, params).mappings().all()]

    league_sql = text(f"""
        SELECT {_SELECT},
            ROUND(AVG(p.start_speed)::numeric, 1) as league_avg_velo
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk=p.game_pk AND ab.at_bat_index=p.at_bat_index
        JOIN mlb.games g ON g.game_pk=p.game_pk
        WHERE g.season=:season AND g.game_type='R'
        AND p.pitch_type_code IS NOT NULL
        GROUP BY p.pitch_type_code
        HAVING COUNT(*) >= 500
    """)
    league_avg = {r["pitch_type_code"]: dict(r) for r in
                  db.execute(league_sql, {"season": season}).mappings().all()}

    return {
        "pitcher_id": pitcher_id,
        "season": season,
        "overall": {"season": get_counts(), "last_14": get_counts(cutoff=cutoff_14)},
        "lhh":     {"season": get_counts("L"), "last_14": get_counts("L", cutoff_14)},
        "rhh":     {"season": get_counts("R"), "last_14": get_counts("R", cutoff_14)},
        "league_avg": league_avg,
    }


@router.get("/pitcher/{pitcher_id}/pitch-group-splits")
def pitcher_pitch_group_splits(pitcher_id: int, season: int = 2026, db: Session = Depends(get_db)):
    """Pitch group (Hard/Breaking/Soft) splits for a pitcher vs batters, with season/14-day windows and league averages."""
    from sqlalchemy import text
    from datetime import date, timedelta

    season_start = f"{season}-03-01"
    cutoff_14 = (date.today() - timedelta(days=14)).isoformat()

    def get_splits(cutoff, bat_side=None):
        side_filter = "AND ab.bat_side = :bat_side" if bat_side else ""
        params = {"pid": pitcher_id, "cutoff": cutoff}
        if bat_side:
            params["bat_side"] = bat_side
        sql = text(f"""
            SELECT
                CASE
                    WHEN p.pitch_type_code IN ('FF','SI','FC') THEN 'Hard'
                    WHEN p.pitch_type_code IN ('CU','SL','ST','KC') THEN 'Breaking'
                    WHEN p.pitch_type_code IN ('CH','FS') THEN 'Soft'
                END as pitch_group,
                COUNT(*) as pitches,
                ROUND(
                    SUM(CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END) /
                    NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X')
                        THEN 1.0 ELSE 0.0 END), 0) * 100
                ::numeric, 1) as whiff_pct,
                ROUND(AVG(CASE WHEN p.call_code='X' AND p.launch_speed >= 95
                    THEN 1.0 ELSE 0.0 END)::numeric * 100, 1) as hard_hit_pct,
                ROUND(AVG(CASE WHEN p.call_code='X' THEN p.launch_speed END)::numeric, 1) as avg_ev,
                ROUND(AVG(CASE WHEN p.call_code='X' THEN p.launch_angle END)::numeric, 1) as avg_la,
                COUNT(CASE WHEN p.call_code='X' THEN 1 END) as bip,
                ROUND(AVG(CASE WHEN p.call_code IN ('S','W','T','C','F','D','E','L','X')
                    THEN 1.0 ELSE 0.0 END)::numeric * 100, 1) as strike_pct
            FROM mlb.pitches p
            JOIN mlb.at_bats ab ON ab.game_pk=p.game_pk AND ab.at_bat_index=p.at_bat_index
            JOIN mlb.games g ON g.game_pk=p.game_pk
            WHERE ab.pitcher_id = :pid
            AND g.game_date >= :cutoff
            AND g.game_type = 'R'
            AND p.pitch_type_code IN ('FF','SI','FC','CU','SL','ST','KC','CH','FS')
            {side_filter}
            GROUP BY pitch_group
            HAVING COUNT(*) >= 5
            ORDER BY pitches DESC
        """)
        return [dict(r) for r in db.execute(sql, params).mappings().all()]

    league_sql = text("""
        SELECT
            CASE
                WHEN p.pitch_type_code IN ('FF','SI','FC') THEN 'Hard'
                WHEN p.pitch_type_code IN ('CU','SL','ST','KC') THEN 'Breaking'
                WHEN p.pitch_type_code IN ('CH','FS') THEN 'Soft'
            END as pitch_group,
            COUNT(*) as pitches,
            ROUND(
                SUM(CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END) /
                NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X')
                    THEN 1.0 ELSE 0.0 END), 0) * 100
            ::numeric, 1) as whiff_pct,
            ROUND(AVG(CASE WHEN p.call_code='X' AND p.launch_speed >= 95
                THEN 1.0 ELSE 0.0 END)::numeric * 100, 1) as hard_hit_pct,
            ROUND(AVG(CASE WHEN p.call_code='X' THEN p.launch_speed END)::numeric, 1) as avg_ev,
            ROUND(AVG(CASE WHEN p.call_code='X' THEN p.launch_angle END)::numeric, 1) as avg_la,
            COUNT(CASE WHEN p.call_code='X' THEN 1 END) as bip,
            ROUND(AVG(CASE WHEN p.call_code IN ('S','W','T','C','F','D','E','L','X')
                THEN 1.0 ELSE 0.0 END)::numeric * 100, 1) as strike_pct
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk=p.game_pk AND ab.at_bat_index=p.at_bat_index
        JOIN mlb.games g ON g.game_pk=p.game_pk
        WHERE g.game_date >= :cutoff
        AND g.game_type = 'R'
        AND p.pitch_type_code IN ('FF','SI','FC','CU','SL','ST','KC','CH','FS')
        GROUP BY pitch_group
        ORDER BY pitches DESC
    """)
    league_avg = {r["pitch_group"]: dict(r) for r in
                  db.execute(league_sql, {"cutoff": season_start}).mappings().all()}

    return {
        "pitcher_id": pitcher_id,
        "season": season,
        "overall": {
            "season": get_splits(season_start),
            "last_14": get_splits(cutoff_14),
        },
        "vs_lhh": {
            "season": get_splits(season_start, 'L'),
            "last_14": get_splits(cutoff_14, 'L'),
        },
        "vs_rhh": {
            "season": get_splits(season_start, 'R'),
            "last_14": get_splits(cutoff_14, 'R'),
        },
        "league_avg": league_avg,
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


@router.get("/batter/{batter_id}/season-stats")
def batter_season_stats(batter_id: int, season: int = 2026, db: Session = Depends(get_db)):
    from sqlalchemy import text

    sql = text("""
        SELECT
            COUNT(DISTINCT bb.game_pk) as games,
            SUM(bb.at_bats) as ab,
            SUM(bb.hits) as h,
            SUM(bb.doubles) as doubles,
            SUM(bb.triples) as triples,
            SUM(bb.home_runs) as hr,
            SUM(bb.strikeouts) as k,
            SUM(bb.total_bases) as tb,
            SUM(bb.runs) as r,
            SUM(bb.rbi) as rbi,
            SUM(bb.hit_by_pitch) as hbp,
            SUM(bb.sac_flies) as sf,
            ROUND(SUM(bb.hits)::numeric / NULLIF(SUM(bb.at_bats), 0), 3) as avg,
            ROUND((SUM(bb.hits) + COALESCE(SUM(bb.hit_by_pitch),0))::numeric /
                NULLIF(SUM(bb.at_bats) + COALESCE(SUM(bb.hit_by_pitch),0) +
                       COALESCE(SUM(bb.sac_flies),0), 0), 3) as obp,
            ROUND(SUM(bb.total_bases)::numeric / NULLIF(SUM(bb.at_bats), 0), 3) as slg,
            ROUND(
                (SUM(bb.hits) + COALESCE(SUM(bb.hit_by_pitch),0))::numeric /
                NULLIF(SUM(bb.at_bats) + COALESCE(SUM(bb.hit_by_pitch),0) +
                       COALESCE(SUM(bb.sac_flies),0), 0) +
                SUM(bb.total_bases)::numeric / NULLIF(SUM(bb.at_bats), 0)
            , 3) as ops,
            ROUND(SUM(bb.strikeouts)::numeric / NULLIF(SUM(bb.at_bats), 0) * 100, 1) as k_pct
        FROM mlb.boxscore_batting bb
        JOIN mlb.games g ON g.game_pk = bb.game_pk
        WHERE bb.player_id = :bid
        AND g.season = :season
        AND g.game_type = 'R'
        AND bb.at_bats > 0
    """)

    season_stats = db.execute(sql, {"bid": batter_id, "season": season}).mappings().first()

    name_sql = text("SELECT MAX(batter_name) FROM mlb.at_bats WHERE batter_id = :bid")
    batter_name = db.execute(name_sql, {"bid": batter_id}).scalar()

    return {
        "batter_id": batter_id,
        "batter_name": batter_name,
        "season": season,
        "stats": dict(season_stats) if season_stats and season_stats["ab"] else {}
    }


@router.get("/lineup-stats/{game_pk}")
def lineup_stats(game_pk: int, season: int = 2026, db: Session = Depends(get_db)):
    from sqlalchemy import text

    _bsc_key = f"boxscore_{game_pk}"
    boxscore = cache_get(_bsc_key, 120)
    if boxscore is None:
        try:
            res = httpx.get(
                f"https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore",
                timeout=10
            )
            boxscore = res.json()
            cache_set(_bsc_key, boxscore)
        except Exception as e:
            raise HTTPException(status_code=503, detail=str(e))

    all_batter_ids = []
    for side in ['away', 'home']:
        order = boxscore.get('teams', {}).get(side, {}).get('battingOrder', [])
        all_batter_ids.extend(order)

    if not all_batter_ids:
        return {"game_pk": game_pk, "stats": {}}

    sql = text("""
        SELECT
            bb.player_id as batter_id,
            MAX(ab.batter_name) as batter_name,
            SUM(bb.at_bats) as ab,
            SUM(bb.hits) as h,
            SUM(bb.home_runs) as hr,
            SUM(bb.strikeouts) as k,
            SUM(bb.total_bases) as tb,
            SUM(bb.hit_by_pitch) as hbp,
            SUM(bb.sac_flies) as sf,
            ROUND(SUM(bb.hits)::numeric / NULLIF(SUM(bb.at_bats), 0), 3) as avg,
            ROUND((SUM(bb.hits) + COALESCE(SUM(bb.hit_by_pitch),0))::numeric /
                NULLIF(SUM(bb.at_bats) + COALESCE(SUM(bb.hit_by_pitch),0) +
                       COALESCE(SUM(bb.sac_flies),0), 0), 3) as obp,
            ROUND(SUM(bb.total_bases)::numeric / NULLIF(SUM(bb.at_bats), 0), 3) as slg,
            ROUND(
                (SUM(bb.hits) + COALESCE(SUM(bb.hit_by_pitch),0))::numeric /
                NULLIF(SUM(bb.at_bats) + COALESCE(SUM(bb.hit_by_pitch),0) +
                       COALESCE(SUM(bb.sac_flies),0), 0) +
                SUM(bb.total_bases)::numeric / NULLIF(SUM(bb.at_bats), 0)
            , 3) as ops,
            ROUND(SUM(bb.strikeouts)::numeric / NULLIF(SUM(bb.at_bats), 0) * 100, 1) as k_pct,
            ROUND(MAX(j2.juiced2_plus)::numeric, 1) as juiced2_plus
        FROM mlb.boxscore_batting bb
        JOIN mlb.games g ON g.game_pk = bb.game_pk
        LEFT JOIN mlb.at_bats ab ON ab.game_pk = bb.game_pk AND ab.batter_id = bb.player_id
        LEFT JOIN mlb.juiced2_scores j2
            ON j2.batter_id = bb.player_id AND j2.season = :season
        WHERE bb.player_id = ANY(:bids)
        AND g.season = :season
        AND g.game_type = 'R'
        AND bb.at_bats > 0
        GROUP BY bb.player_id
    """)

    rows = db.execute(sql, {"bids": all_batter_ids, "season": season}).mappings().all()
    stats_map = {r["batter_id"]: dict(r) for r in rows}

    # Enrich with BBref OPS+ and Fangraphs wRC+/WAR
    # fangraphs_batting.mlbam_id stores Fangraphs player ID — join through player_id_map
    bbref_sql = text("""
        SELECT
            b.mlb_id as batter_id,
            b.pa as bbref_pa,
            b.ops_plus,
            f.wrc_plus,
            f.war
        FROM mlb.bbref_batting b
        LEFT JOIN mlb.player_id_map m ON m.mlbam_id = b.mlb_id
        LEFT JOIN mlb.fangraphs_batting f
            ON f.mlbam_id = m.fangraphs_id AND f.season = :season
        WHERE b.mlb_id = ANY(:bids)
        AND b.year_id = :season
    """)
    bbref_rows = db.execute(bbref_sql, {"bids": all_batter_ids, "season": season}).mappings().all()
    for r in bbref_rows:
        bid = r["batter_id"]
        if bid in stats_map:
            stats_map[bid]["bbref_pa"] = r["bbref_pa"]
            stats_map[bid]["ops_plus"] = r["ops_plus"]
            stats_map[bid]["wrc_plus"] = r["wrc_plus"]
            stats_map[bid]["war"] = r["war"]
        else:
            stats_map[bid] = dict(r)

    return {"game_pk": game_pk, "season": season, "stats": stats_map}


# ── HITTER PROFILE ─────────────────────────────────────────────────────────────

@router.get("/hitter-search")
def hitter_search(q: str, db: Session = Depends(get_db)):
    from sqlalchemy import text
    sql = text("""
        SELECT DISTINCT ab.batter_id as player_id, ab.batter_name as player_name,
               COUNT(DISTINCT ab.game_pk) as games
        FROM mlb.at_bats ab
        JOIN mlb.games g ON g.game_pk = ab.game_pk
        WHERE LOWER(ab.batter_name) LIKE LOWER(:q)
        AND g.season >= 2025
        AND g.game_type = 'R'
        GROUP BY ab.batter_id, ab.batter_name
        HAVING COUNT(DISTINCT ab.game_pk) >= 5
        ORDER BY games DESC
        LIMIT 10
    """)
    rows = db.execute(sql, {"q": f"%{q}%"}).mappings().all()
    return [dict(r) for r in rows]


@router.get("/hitter/{batter_id}/profile")
def hitter_profile(batter_id: int, season: int = 2026, db: Session = Depends(get_db)):
    from sqlalchemy import text
    from datetime import date, timedelta
    from collections import defaultdict

    season_start = f"{season}-03-01"
    cutoff_14 = (date.today() - timedelta(days=14)).isoformat()
    cutoff_7 = (date.today() - timedelta(days=7)).isoformat()

    # Batter name and bat side
    info_sql = text("""
        SELECT MAX(batter_name) as name, MAX(bat_side) as bat_side
        FROM mlb.at_bats WHERE batter_id = :bid
    """)
    info = db.execute(info_sql, {"bid": batter_id}).mappings().first()

    # Season counting + rate stats from boxscore_batting
    season_sql = text("""
        SELECT
            COUNT(DISTINCT bb.game_pk) as g,
            SUM(bb.at_bats) as ab,
            SUM(bb.at_bats) + COALESCE(SUM(bb.walks),0) + COALESCE(SUM(bb.hit_by_pitch),0) +
                COALESCE(SUM(bb.sac_flies),0) + COALESCE(SUM(bb.sac_bunts),0) as pa,
            SUM(bb.hits) as h,
            SUM(bb.doubles) as doubles,
            SUM(bb.triples) as triples,
            SUM(bb.home_runs) as hr,
            SUM(bb.rbi) as rbi,
            SUM(bb.runs) as r,
            SUM(bb.strikeouts) as k,
            SUM(bb.walks) as bb,
            SUM(bb.total_bases) as tb,
            ROUND(SUM(bb.hits)::numeric / NULLIF(SUM(bb.at_bats), 0), 3) as avg,
            ROUND(
                (SUM(bb.hits) + COALESCE(SUM(bb.walks),0) + COALESCE(SUM(bb.hit_by_pitch),0))::numeric /
                NULLIF(SUM(bb.at_bats) + COALESCE(SUM(bb.walks),0) +
                    COALESCE(SUM(bb.hit_by_pitch),0) + COALESCE(SUM(bb.sac_flies),0), 0)
            , 3) as obp,
            ROUND(SUM(bb.total_bases)::numeric / NULLIF(SUM(bb.at_bats), 0), 3) as slg,
            ROUND(
                (SUM(bb.hits) + COALESCE(SUM(bb.walks),0) + COALESCE(SUM(bb.hit_by_pitch),0))::numeric /
                NULLIF(SUM(bb.at_bats) + COALESCE(SUM(bb.walks),0) +
                    COALESCE(SUM(bb.hit_by_pitch),0) + COALESCE(SUM(bb.sac_flies),0), 0) +
                SUM(bb.total_bases)::numeric / NULLIF(SUM(bb.at_bats), 0)
            , 3) as ops,
            ROUND(
                SUM(bb.strikeouts)::numeric /
                NULLIF(SUM(bb.at_bats) + COALESCE(SUM(bb.walks),0) +
                    COALESCE(SUM(bb.hit_by_pitch),0) + COALESCE(SUM(bb.sac_flies),0) +
                    COALESCE(SUM(bb.sac_bunts),0), 0) * 100
            , 1) as k_pct
        FROM mlb.boxscore_batting bb
        JOIN mlb.games g ON g.game_pk = bb.game_pk
        WHERE bb.player_id = :bid
        AND g.season = :season
        AND g.game_type = 'R'
    """)
    season_stats = db.execute(season_sql, {"bid": batter_id, "season": season}).mappings().first()

    # OPS+ from bbref
    bbref_sql = text("""
        SELECT ops_plus FROM mlb.bbref_batting WHERE mlb_id = :bid AND year_id = :season
    """)
    bbref_row = db.execute(bbref_sql, {"bid": batter_id, "season": season}).mappings().first()

    # Fangraphs stats via player_id_map
    fg_sql = text("""
        SELECT f.wrc_plus, f.war, f.hard_hit_pct, f.barrel_pct, f.avg_exit_velo
        FROM mlb.player_id_map m
        JOIN mlb.fangraphs_batting f ON f.mlbam_id = m.fangraphs_id AND f.season = :season
        WHERE m.mlbam_id = :bid
    """)
    fg_row = db.execute(fg_sql, {"bid": batter_id, "season": season}).mappings().first()

    plus_stats = {}
    if bbref_row:
        plus_stats["ops_plus"] = bbref_row["ops_plus"]
    if fg_row:
        plus_stats.update({k: fg_row[k] for k in ("wrc_plus", "war", "hard_hit_pct", "barrel_pct", "avg_exit_velo")})

    # Pitch group splits
    def get_group_splits(cutoff, pitcher_hand=None):
        hand_filter = "AND ab.pitch_hand = :phand" if pitcher_hand else ""
        params = {"bid": batter_id, "cutoff": cutoff}
        if pitcher_hand:
            params["phand"] = pitcher_hand
        sql = text(f"""
            SELECT
                CASE
                    WHEN p.pitch_type_code IN ('FF','SI','FC') THEN 'Hard'
                    WHEN p.pitch_type_code IN ('CU','SL','ST','KC') THEN 'Breaking'
                    WHEN p.pitch_type_code IN ('CH','FS') THEN 'Soft'
                END as pitch_group,
                COUNT(*) as pitches,
                SUM(CASE WHEN p.call_code IN ('S','W','T') THEN 1 ELSE 0 END) as whiffs,
                SUM(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X') THEN 1 ELSE 0 END) as swings,
                ROUND(
                    SUM(CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END) /
                    NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X')
                        THEN 1.0 ELSE 0.0 END), 0) * 100
                ::numeric, 1) as whiff_pct,
                ROUND(AVG(CASE WHEN p.call_code='X' AND p.launch_speed >= 95
                    THEN 1.0 ELSE 0.0 END)::numeric * 100, 1) as hard_hit_pct,
                ROUND(AVG(CASE WHEN p.call_code='X' THEN p.launch_speed END)::numeric, 1) as avg_ev,
                ROUND(AVG(CASE WHEN p.call_code='X' THEN p.launch_angle END)::numeric, 1) as avg_la,
                COUNT(CASE WHEN p.call_code='X' THEN 1 END) as bip,
                ROUND(AVG(CASE WHEN p.call_code IN ('S','W','T','C','F','D','E','L','X')
                    THEN 1.0 ELSE 0.0 END)::numeric * 100, 1) as strike_pct
            FROM mlb.pitches p
            JOIN mlb.at_bats ab ON ab.game_pk=p.game_pk AND ab.at_bat_index=p.at_bat_index
            JOIN mlb.games g ON g.game_pk=p.game_pk
            WHERE ab.batter_id = :bid
            AND g.game_date >= :cutoff
            AND g.game_type = 'R'
            AND p.pitch_type_code IN ('FF','SI','FC','CU','SL','ST','KC','CH','FS')
            {hand_filter}
            GROUP BY pitch_group
            HAVING COUNT(*) >= 5
            ORDER BY pitches DESC
        """)
        return [dict(r) for r in db.execute(sql, params).mappings().all()]

    def get_league_avg_groups():
        sql = text("""
            SELECT
                CASE
                    WHEN p.pitch_type_code IN ('FF','SI','FC') THEN 'Hard'
                    WHEN p.pitch_type_code IN ('CU','SL','ST','KC') THEN 'Breaking'
                    WHEN p.pitch_type_code IN ('CH','FS') THEN 'Soft'
                END as pitch_group,
                COUNT(*) as pitches,
                ROUND(
                    SUM(CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END) /
                    NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X')
                        THEN 1.0 ELSE 0.0 END), 0) * 100
                ::numeric, 1) as whiff_pct,
                ROUND(AVG(CASE WHEN p.call_code='X' AND p.launch_speed >= 95
                    THEN 1.0 ELSE 0.0 END)::numeric * 100, 1) as hard_hit_pct,
                ROUND(AVG(CASE WHEN p.call_code='X' THEN p.launch_speed END)::numeric, 1) as avg_ev,
                ROUND(AVG(CASE WHEN p.call_code='X' THEN p.launch_angle END)::numeric, 1) as avg_la,
                COUNT(CASE WHEN p.call_code='X' THEN 1 END) as bip,
                ROUND(AVG(CASE WHEN p.call_code IN ('S','W','T','C','F','D','E','L','X')
                    THEN 1.0 ELSE 0.0 END)::numeric * 100, 1) as strike_pct
            FROM mlb.pitches p
            JOIN mlb.at_bats ab ON ab.game_pk=p.game_pk AND ab.at_bat_index=p.at_bat_index
            JOIN mlb.games g ON g.game_pk=p.game_pk
            WHERE g.game_date >= :cutoff
            AND g.game_type = 'R'
            AND p.pitch_type_code IN ('FF','SI','FC','CU','SL','ST','KC','CH','FS')
            GROUP BY pitch_group
            ORDER BY pitches DESC
        """)
        return {r["pitch_group"]: dict(r) for r in
                db.execute(sql, {"cutoff": season_start}).mappings().all()}

    groups_season = get_group_splits(season_start)
    groups_14 = get_group_splits(cutoff_14)
    groups_7 = get_group_splits(cutoff_7)
    groups_season_lhp = get_group_splits(season_start, 'L')
    groups_14_lhp = get_group_splits(cutoff_14, 'L')
    groups_season_rhp = get_group_splits(season_start, 'R')
    groups_14_rhp = get_group_splits(cutoff_14, 'R')
    league_avg_groups = get_league_avg_groups()

    # Zone data
    zone_sql = text("""
        SELECT
            p.zone,
            COUNT(*) as pitches,
            COUNT(CASE WHEN p.call_code='X' THEN 1 END) as bip,
            ROUND(AVG(CASE WHEN p.call_code='X' AND p.launch_speed >= 95
                THEN 1.0 ELSE 0.0 END)::numeric * 100, 1) as hard_hit_pct,
            ROUND(AVG(CASE WHEN p.call_code='X' THEN p.launch_speed END)::numeric, 1) as avg_ev,
            ROUND(
                SUM(CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END) /
                NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X')
                    THEN 1.0 ELSE 0.0 END), 0) * 100
            ::numeric, 1) as whiff_pct,
            ROUND(COUNT(CASE WHEN p.call_code = 'C' THEN 1 END)::numeric /
                NULLIF(COUNT(*), 0) * 100, 1) as cs_pct
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk=p.game_pk AND ab.at_bat_index=p.at_bat_index
        JOIN mlb.games g ON g.game_pk=p.game_pk
        WHERE ab.batter_id = :bid
        AND g.game_date >= :cutoff
        AND g.game_type = 'R'
        AND p.zone BETWEEN 1 AND 9
        GROUP BY p.zone
        ORDER BY p.zone
    """)
    zones_season = {r["zone"]: dict(r) for r in
        db.execute(zone_sql, {"bid": batter_id, "cutoff": season_start}).mappings().all()}
    zones_14 = {r["zone"]: dict(r) for r in
        db.execute(zone_sql, {"bid": batter_id, "cutoff": cutoff_14}).mappings().all()}

    # Game log
    gamelog_sql = text("""
        SELECT
            g.game_pk,
            g.game_date,
            CASE WHEN g.home_team_id = bb.team_id
                THEN g.away_team_abbrev ELSE g.home_team_abbrev
            END as opponent,
            bb.at_bats as ab,
            bb.hits as h,
            bb.doubles,
            bb.triples,
            bb.home_runs as hr,
            bb.rbi,
            bb.runs as r,
            bb.strikeouts as k,
            bb.total_bases as tb,
            COALESCE(bb.walks, 0) as bb,
            COALESCE(bb.hit_by_pitch, 0) as hbp,
            COALESCE(bb.sac_flies, 0) as sf
        FROM mlb.boxscore_batting bb
        JOIN mlb.games g ON g.game_pk = bb.game_pk
        WHERE bb.player_id = :bid
        AND g.season = :season
        AND g.game_type = 'R'
        ORDER BY g.game_date DESC
    """)
    gamelog = db.execute(gamelog_sql, {"bid": batter_id, "season": season}).mappings().all()

    # Per-game PA detail
    pa_sql = text("""
        SELECT
            ab.game_pk,
            ab.at_bat_index,
            ab.inning,
            ab.half_inning,
            ab.outs,
            ab.event_type,
            ab.description,
            ab.pitcher_name,
            COUNT(p.id) as pitches,
            STRING_AGG(p.pitch_type_code, ',' ORDER BY p.id) as pitch_sequence,
            MAX(CASE WHEN p.call_code = 'X' THEN p.play_id END) as bip_play_id,
            (SELECT p2.play_id FROM mlb.pitches p2
             WHERE p2.game_pk = ab.game_pk
             AND p2.at_bat_index = ab.at_bat_index
             ORDER BY p2.id DESC LIMIT 1) as final_play_id,
            MAX(CASE WHEN p.call_code = 'X' THEN p.launch_speed END) as exit_velo,
            MAX(CASE WHEN p.call_code = 'X' THEN p.launch_angle END) as launch_angle
        FROM mlb.at_bats ab
        LEFT JOIN mlb.pitches p ON p.game_pk = ab.game_pk
            AND p.at_bat_index = ab.at_bat_index
        WHERE ab.batter_id = :bid
        AND ab.game_pk IN (
            SELECT DISTINCT game_pk FROM mlb.games
            WHERE season = :season AND game_type = 'R'
        )
        GROUP BY ab.game_pk, ab.inning, ab.half_inning, ab.outs,
                 ab.event_type, ab.description, ab.pitcher_name, ab.at_bat_index
        ORDER BY ab.game_pk DESC, ab.at_bat_index
    """)
    pa_rows = db.execute(pa_sql, {"bid": batter_id, "season": season}).mappings().all()

    pa_by_game = defaultdict(list)
    for row in pa_rows:
        pa_by_game[row["game_pk"]].append(dict(row))

    # Recent form summary by pitch group (last 14 days)
    hitting_well = []
    for g in groups_14:
        if g["bip"] and g["bip"] >= 3:
            hitting_well.append({
                "group": g["pitch_group"],
                "hard_hit_pct": g["hard_hit_pct"],
                "avg_ev": g["avg_ev"],
                "whiff_pct": g["whiff_pct"],
                "bip": g["bip"],
                "pitches": g["pitches"],
                "signal": "hot" if (g["hard_hit_pct"] or 0) >= 40 else
                          "cold" if (g["hard_hit_pct"] or 0) <= 20 else "neutral",
            })
    hitting_well.sort(key=lambda x: x["hard_hit_pct"] or 0, reverse=True)

    juiced_sql = text("""
        SELECT
            js.pitch_type_code,
            js.velo_quintile,
            js.pa,
            js.ab,
            js.hits,
            ROUND(js.avg::numeric, 3) as avg,
            ROUND(js.obp::numeric, 3) as obp,
            ROUND(js.slg::numeric, 3) as slg,
            ROUND(js.ops::numeric, 3) as ops,
            ROUND(js.lg_ops::numeric, 3) as lg_ops,
            ROUND(js.juiced_plus::numeric, 1) as juiced_plus
        FROM mlb.juiced_scores js
        WHERE js.batter_id = :bid
        AND js.season = :season
        AND js.pa >= 5
        ORDER BY js.pitch_type_code, js.velo_quintile
    """)
    juiced_rows = db.execute(juiced_sql, {"bid": batter_id, "season": season}).mappings().all()

    bounds_sql = text("""
        SELECT pitch_type_code, q20, q40, q60, q80
        FROM mlb.goose_pitch_boundaries
        WHERE characteristic = 'velo'
    """)
    velo_boundaries = {r['pitch_type_code']: dict(r)
                       for r in db.execute(bounds_sql).mappings().all()}

    juiced_list = [dict(r) for r in juiced_rows]
    total_juiced_pa = sum(r['pa'] for r in juiced_list)
    overall_juiced = (
        sum(r['juiced_plus'] * r['pa'] for r in juiced_list) / total_juiced_pa
        if total_juiced_pa > 0 else None
    )

    juiced2_sql = text("""
        SELECT ROUND(juiced2_plus::numeric,1) as juiced2_plus,
               ROUND(quality_adj_idx::numeric,1) as quality_adj_idx,
               ROUND(avg_goose2_faced::numeric,1) as avg_goose2_faced,
               ROUND(ops_idx::numeric,1) as ops_idx,
               ROUND(slg_vs_expected::numeric,3) as slg_vs_expected,
               hard_hit_source,
               ROUND(bat_speed::numeric,1) as bat_speed
        FROM mlb.juiced2_scores
        WHERE batter_id = :bid AND season = :season
    """)
    juiced2 = db.execute(juiced2_sql, {"bid": batter_id, "season": season}).mappings().first()

    return {
        "batter_id": batter_id,
        "batter_name": info["name"] if info else "Unknown",
        "bat_side": info["bat_side"] if info else None,
        "season": season,
        "season_stats": dict(season_stats) if season_stats else {},
        "plus_stats": plus_stats,
        "pitch_groups": {
            "overall": {"season": groups_season, "last_14": groups_14},
            "vs_lhp": {"season": groups_season_lhp, "last_14": groups_14_lhp},
            "vs_rhp": {"season": groups_season_rhp, "last_14": groups_14_rhp},
            "league_avg": league_avg_groups,
            "last_7": groups_7,
        },
        "zones": {
            "season": zones_season,
            "last_14": zones_14,
        },
        "hitting_well": hitting_well,
        "juiced_plus": {
            "overall": round(overall_juiced, 1) if overall_juiced is not None else None,
            "total_pa": total_juiced_pa,
            "splits": juiced_list,
            "velo_boundaries": velo_boundaries,
        },
        "juiced2": dict(juiced2) if juiced2 else None,
        "game_log": [
            {**dict(g), "plate_appearances": pa_by_game.get(g["game_pk"], [])}
            for g in gamelog
        ],
    }


# ── GOOSE+ / JUICED+ ──────────────────────────────────────────────────────────

@router.get("/goose/leaderboard")
def goose_leaderboard(season: int = 2026, min_pitches: int = 100, db: Session = Depends(get_db)):
    from sqlalchemy import text
    sql = text("""
        SELECT
            go.pitcher_id, go.pitcher_name,
            ROUND(go.goose_plus::numeric, 1) as goose_plus,
            ROUND(go.goose_plus_vs_lhh::numeric, 1) as goose_plus_vs_lhh,
            ROUND(go.goose_plus_vs_rhh::numeric, 1) as goose_plus_vs_rhh,
            go.total_pitches,
            json_agg(json_build_object(
                'pitch_type', gs.pitch_type_code,
                'goose_plus', ROUND(gs.goose_plus::numeric, 1),
                'avg_velo', ROUND(gs.avg_velo::numeric, 1),
                'velo_quintile', gs.velo_quintile,
                'whiff_pct', ROUND(gs.whiff_pct::numeric * 100, 1),
                'strike_pct', ROUND(gs.strike_pct::numeric * 100, 1),
                'pitches', gs.pitches_thrown
            ) ORDER BY gs.pitches_thrown DESC) as pitch_breakdown
        FROM mlb.goose_overall go
        JOIN mlb.goose_scores gs ON gs.pitcher_id = go.pitcher_id
            AND gs.season = go.season
        WHERE go.season = :season
        AND go.game_pk IS NULL
        AND go.total_pitches >= :min_pitches
        GROUP BY go.pitcher_id, go.pitcher_name, go.goose_plus,
                 go.goose_plus_vs_lhh, go.goose_plus_vs_rhh, go.total_pitches
        ORDER BY go.goose_plus DESC NULLS LAST
        LIMIT 100
    """)
    rows = db.execute(sql, {"season": season, "min_pitches": min_pitches}).mappings().all()
    return {"pitchers": [dict(r) for r in rows], "season": season}


@router.get("/goose/pitcher/{pitcher_id}")
def goose_pitcher(pitcher_id: int, season: int = 2026, db: Session = Depends(get_db)):
    from sqlalchemy import text
    sql = text("""
        SELECT gs.*,
               go.goose_plus as overall_goose_plus,
               go.goose_plus_vs_lhh as overall_lhh,
               go.goose_plus_vs_rhh as overall_rhh,
               go.total_pitches,
               gb.strike_pct as bucket_strike_pct,
               gb.whiff_pct as bucket_whiff_pct,
               gb.avg_velo as bucket_avg_velo,
               gb.pitches as bucket_pitches
        FROM mlb.goose_scores gs
        JOIN mlb.goose_overall go ON go.pitcher_id = gs.pitcher_id
            AND go.season = gs.season AND go.game_pk IS NULL
        LEFT JOIN mlb.goose_bucket_outcomes gb ON gb.pitch_type_code = gs.pitch_type_code
            AND gb.velo_quintile = gs.velo_quintile
        WHERE gs.pitcher_id = :pid AND gs.season = :season
        ORDER BY gs.pitches_thrown DESC
    """)
    rows = db.execute(sql, {"pid": pitcher_id, "season": season}).mappings().all()
    if not rows:
        raise HTTPException(status_code=404, detail="No Goose+ data found")
    return {
        "pitcher_id": pitcher_id,
        "pitcher_name": rows[0]["pitcher_name"],
        "season": season,
        "overall_goose_plus": rows[0]["overall_goose_plus"],
        "overall_lhh": rows[0]["overall_lhh"],
        "overall_rhh": rows[0]["overall_rhh"],
        "pitches": [dict(r) for r in rows],
    }


@router.get("/juiced/batter/{batter_id}")
def juiced_batter(batter_id: int, season: int = 2026, db: Session = Depends(get_db)):
    from sqlalchemy import text
    sql = text("""
        SELECT js.*,
               gpb.avg_velo as bucket_avg_velo,
               gpb.strike_pct as bucket_strike_pct,
               gpb.whiff_pct as bucket_whiff_pct
        FROM mlb.juiced_scores js
        LEFT JOIN mlb.goose_bucket_outcomes gpb
            ON gpb.pitch_type_code = js.pitch_type_code
            AND gpb.velo_quintile = js.velo_quintile
        WHERE js.batter_id = :bid AND js.season = :season
        AND js.pa >= 5
        ORDER BY js.pa DESC
    """)
    rows = db.execute(sql, {"bid": batter_id, "season": season}).mappings().all()
    batter_name = rows[0]["batter_name"] if rows else "Unknown"
    total_pa = sum(r["pa"] for r in rows)
    overall = sum(r["juiced_plus"] * r["pa"] for r in rows) / max(total_pa, 1) if rows else None
    return {
        "batter_id": batter_id,
        "batter_name": batter_name,
        "season": season,
        "overall_juiced_plus": round(overall, 1) if overall is not None else None,
        "total_pa": total_pa,
        "splits": [dict(r) for r in rows],
    }


@router.get("/goose/buckets")
def goose_buckets(db: Session = Depends(get_db)):
    from sqlalchemy import text
    bounds = db.execute(text("""
        SELECT pitch_type_code, characteristic, q20, q40, q60, q80, mean
        FROM mlb.goose_pitch_boundaries ORDER BY pitch_type_code, characteristic
    """)).mappings().all()
    outcomes = db.execute(text("""
        SELECT * FROM mlb.goose_bucket_outcomes ORDER BY pitch_type_code, velo_quintile
    """)).mappings().all()
    return {
        "boundaries": [dict(r) for r in bounds],
        "outcomes": [dict(r) for r in outcomes],
    }


@router.get("/goose/bucket-labels")
def goose_bucket_labels(db: Session = Depends(get_db)):
    """Returns velo range labels per pitch type per quintile for display in Juiced+ tier tables."""
    from sqlalchemy import text
    rows = db.execute(text("""
        SELECT pitch_type_code, q20, q40, q60, q80
        FROM mlb.goose_pitch_boundaries
        WHERE characteristic = 'velo'
        ORDER BY pitch_type_code
    """)).mappings().all()

    TIER_LABELS = ['Soft', 'Below Avg', 'Average', 'Above Avg', 'Elite']

    result = {}
    for r in rows:
        pt = r['pitch_type_code']
        bounds = [float(r['q20']), float(r['q40']), float(r['q60']), float(r['q80'])]
        result[pt] = {}
        for q in range(1, 6):
            if q == 1:
                velo_range = f"< {bounds[0]:.1f}"
            elif q == 5:
                velo_range = f"{bounds[3]:.1f}+"
            else:
                velo_range = f"{bounds[q-2]:.1f} – {bounds[q-1]:.1f}"
            result[pt][q] = {
                "label": TIER_LABELS[q - 1],
                "velo_range": velo_range,
                "display": f"{TIER_LABELS[q - 1]} ({velo_range} mph)",
            }
    return result


# ── AT-BAT SIMULATOR ──────────────────────────────────────────────────────────

@router.post("/simulate/at-bat")
def simulate_at_bat(payload: dict, db: Session = Depends(get_db)):
    """Monte Carlo at-bat simulation using count-specific pitch mix."""
    import sys
    sys.path.insert(0, '/pipeline')
    from mlb.simulate import run_simulation
    from sqlalchemy import text

    pitcher_id = int(payload.get("pitcher_id", 0))
    batter_id = int(payload.get("batter_id", 0))
    n_sims = min(int(payload.get("n_simulations", 1000)), 5000)
    outs = int(payload.get("outs", 0))
    runners = payload.get("runners", {"first": False, "second": False, "third": False})
    season = int(payload.get("season", 2026))
    balls_start = max(0, min(3, int(payload.get("balls", 0))))
    strikes_start = max(0, min(2, int(payload.get("strikes", 0))))
    game_pk = payload.get("game_pk")
    game_pk = int(game_pk) if game_pk else None

    if not pitcher_id or not batter_id:
        raise HTTPException(status_code=400, detail="pitcher_id and batter_id required")

    on_first = bool(runners.get("first", False))
    on_second = bool(runners.get("second", False))
    on_third = bool(runners.get("third", False))

    sim = run_simulation(pitcher_id, batter_id, n_sims, db,
                         balls_start=balls_start, strikes_start=strikes_start,
                         game_pk=game_pk,
                         on_first=on_first, on_second=on_second,
                         on_third=on_third, outs=outs)

    if "error" in sim:
        raise HTTPException(status_code=404, detail=sim["error"])

    pitcher_name = db.execute(text(
        "SELECT MAX(pitcher_name) FROM mlb.at_bats WHERE pitcher_id = :pid"
    ), {"pid": pitcher_id}).scalar() or "Unknown"
    batter_name = db.execute(text(
        "SELECT MAX(batter_name) FROM mlb.at_bats WHERE batter_id = :bid"
    ), {"bid": batter_id}).scalar() or "Unknown"

    goose = db.execute(text("""
        SELECT ROUND(goose_plus::numeric, 1) as goose_plus
        FROM mlb.goose_overall
        WHERE pitcher_id = :pid AND season = :season AND game_pk IS NULL
    """), {"pid": pitcher_id, "season": season}).scalar()

    juiced = db.execute(text("""
        SELECT ROUND((SUM(juiced_plus * pa) / NULLIF(SUM(pa), 0))::numeric, 1)
        FROM mlb.juiced_scores
        WHERE batter_id = :bid AND season = :season AND pa >= 3
    """), {"bid": batter_id, "season": season}).scalar()

    goose3 = db.execute(text("""
        SELECT ROUND(goose3_plus::numeric, 1) as goose3_plus,
               ROUND(avg_stuff_score::numeric, 1) as avg_stuff,
               ROUND(avg_goose2_score::numeric, 1) as avg_goose2,
               stuff_outcomes_gap
        FROM mlb.goose3_overall
        WHERE pitcher_id = :pid AND season = :season
    """), {"pid": pitcher_id, "season": season}).mappings().first()

    return {
        "pitcher_id": pitcher_id,
        "pitcher_name": pitcher_name,
        "batter_id": batter_id,
        "batter_name": batter_name,
        "bat_side": sim["bat_side"],
        "n_simulations": n_sims,
        "situation": {
            "outs": outs,
            "runners": runners,
            "count": f"{balls_start}-{strikes_start}",
            "state_label": sim.get("splits_context", {}).get("state_label", ""),
        },
        "context": {
            "goose_plus": float(goose) if goose else None,
            "juiced_plus": float(juiced) if juiced else None,
            "goose3_plus": float(goose3["goose3_plus"]) if goose3 and goose3["goose3_plus"] else None,
            "stuff_score": float(goose3["avg_stuff"]) if goose3 and goose3["avg_stuff"] else None,
            "stuff_outcomes_gap": float(goose3["stuff_outcomes_gap"]) if goose3 and goose3["stuff_outcomes_gap"] else None,
            "weather": (lambda r: dict(r) if r else None)(
                db.execute(text("""
                    SELECT temperature_f, wind_speed_mph, wind_direction_deg,
                           weather_condition, wind_hr_modifier, temp_hr_modifier,
                           combined_hr_modifier, venue_name, is_indoor
                    FROM mlb.game_weather WHERE game_pk = :gp
                """), {"gp": game_pk}).mappings().first()
            ) if game_pk else None,
        },
        "results": sim["results"],
        "outcome_distribution": sim["outcome_distribution"],
        "pitch_distribution": sim["pitch_distribution"],
        "sample_pas": sim["sample_pas"],
        "data_quality": sim["data_quality"],
        "count_mix_source": sim["count_mix_source"],
        "splits_context": sim.get("splits_context", {}),
    }


@router.get("/weather/today")
def weather_today(db: Session = Depends(get_db)):
    """Get weather conditions for today's games."""
    from sqlalchemy import text
    sql = text("""
        SELECT gw.game_pk, gw.team_abbrev, gw.venue_name, gw.is_indoor,
               gw.temperature_f, gw.humidity_pct, gw.wind_speed_mph,
               gw.wind_direction_deg, gw.wind_gust_mph, gw.precipitation_in,
               gw.weather_condition, gw.wind_hr_modifier, gw.temp_hr_modifier,
               gw.combined_hr_modifier, gw.fetched_at
        FROM mlb.game_weather gw
        JOIN mlb.games g ON g.game_pk = gw.game_pk
        WHERE g.game_date = CURRENT_DATE::varchar
        ORDER BY gw.team_abbrev
    """)
    rows = db.execute(sql).mappings().all()
    return {"date": str(__import__('datetime').date.today()),
            "games": [dict(r) for r in rows]}


@router.get("/leaderboard/daily-projections")
def daily_projections(
    date: str = None,
    db: Session = Depends(get_db),
):
    from sqlalchemy import text
    if date:
        target = date
    else:
        from zoneinfo import ZoneInfo
        target = datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

    rows = db.execute(text("""
        SELECT dp.pitcher_id, dp.pitcher_name, dp.pitcher_hand, dp.team_abbrev, dp.opp_abbrev,
               dp.goose_plus, dp.bapv_plus, dp.proj_k_pct, dp.proj_hr_pct, dp.proj_hit_pct,
               dp.proj_ops, dp.proj_ks_6inn, dp.proj_bb_pct, dp.batters_simulated,
               dp.proj_ks_f5, dp.proj_er_f5, dp.proj_ip_f5,
               dp.simulations_per_batter, dp.snapshot_date, dp.game_pk,
               ROUND(pk.pk_plus::numeric, 1) as pk_plus,
               ROUND(g3.goose3_plus::numeric, 1) as goose3_plus
        FROM mlb.daily_projections dp
        LEFT JOIN mlb.pk_overall pk ON pk.pitcher_id = dp.pitcher_id AND pk.season = 2026
        LEFT JOIN mlb.goose3_overall g3 ON g3.pitcher_id = dp.pitcher_id AND g3.season = 2026
        WHERE dp.snapshot_date = :d
        ORDER BY dp.proj_k_pct DESC NULLS LAST
    """), {"d": target}).fetchall()

    available = db.execute(text("""
        SELECT DISTINCT snapshot_date::text
        FROM mlb.daily_projections
        ORDER BY snapshot_date DESC
        LIMIT 30
    """)).fetchall()

    return {
        "date": target,
        "projections": [dict(r._mapping) for r in rows],
        "available_dates": [r[0] for r in available],
    }


@router.post("/leaderboard/run-daily-projections")
async def run_daily_projections(date: str = None):
    import os
    from zoneinfo import ZoneInfo
    target = date or datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    proc = await asyncio.create_subprocess_exec(
        "python3", "/pipeline/mlb/build_daily_projections.py", "--date", target,
        env={**os.environ, "PYTHONPATH": "/app:/pipeline"},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=600)
    if proc.returncode != 0:
        raise HTTPException(status_code=500, detail=stderr.decode()[-2000:])
    return {"status": "ok", "date": target, "output": stdout.decode()[-3000:]}


@router.get("/leaderboard/daily-batter-projections")
def daily_batter_projections(
    snapshot_date: str = None,
    db: Session = Depends(get_db),
):
    from sqlalchemy import text
    from zoneinfo import ZoneInfo
    target = snapshot_date or datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")

    rows = db.execute(text("""
        SELECT
            batter_id, batter_name, bat_side,
            team_abbrev, opp_pitcher_id, opp_pitcher_name, opp_pitcher_hand,
            ROUND(opp_goose_plus::numeric, 1) as opp_goose_plus,
            primary_pitch_type, primary_pitch_velo_quintile,
            ROUND(primary_pitch_goose_plus::numeric, 1) as primary_pitch_goose_plus,
            ROUND(juiced_plus_vs_bucket::numeric, 1) as juiced_plus_vs_bucket,
            ROUND(juiced_plus_overall::numeric, 1) as juiced_plus_overall,
            ROUND(proj_avg::numeric, 3) as proj_avg,
            ROUND(proj_obp::numeric, 3) as proj_obp,
            ROUND(proj_slg::numeric, 3) as proj_slg,
            ROUND(proj_ops::numeric, 3) as proj_ops,
            ROUND(proj_tb::numeric, 2) as proj_tb,
            proj_hr_pct, proj_k_pct, proj_bb_pct, proj_hit_pct,
            simulations, game_pk, snapshot_date::text as snapshot_date
        FROM mlb.daily_batter_projections
        WHERE snapshot_date = :snap_date
        ORDER BY proj_ops DESC NULLS LAST
    """), {"snap_date": target}).mappings().all()

    dates = db.execute(text("""
        SELECT DISTINCT snapshot_date::text
        FROM mlb.daily_batter_projections
        ORDER BY snapshot_date DESC LIMIT 30
    """)).fetchall()

    return {
        "snapshot_date": target,
        "available_dates": [r[0] for r in dates],
        "projections": [dict(r) for r in rows],
        "n": len(rows),
    }


@router.get("/game/{game_pk}/score-projection")
def game_score_projection(
    game_pk: int,
    n_sims: int = 500,
    db: Session = Depends(get_db),
):
    """
    Project final score for a game using inning simulator.
    Requires lineup to be posted in mlb.game_lineups.
    Cached 15 minutes — simulation is expensive.
    """
    import sys
    sys.path.insert(0, '/pipeline')

    cache_key = f"score_proj_{game_pk}_{n_sims}"
    cached = cache_get(cache_key, 900)
    if cached:
        return cached

    from mlb.simulate_innings import run_game_simulation
    result = run_game_simulation(game_pk, db, n_sims)
    if 'error' not in result:
        cache_set(cache_key, result)
    return result


@router.get("/schedule/today-with-lineups")
def schedule_with_lineups(db: Session = Depends(get_db)):
    """
    Today's schedule with lineup status and model scores for starting pitchers.
    Only returns games that have lineups posted in mlb.game_lineups.
    """
    from sqlalchemy import text

    cached = cache_get('today_with_lineups', 300)
    if cached:
        return cached

    try:
        rows = db.execute(text("""
            SELECT
                gl.game_pk,
                gl.home_team_id,
                gl.away_team_id,
                gl.home_pitcher_id,
                gl.home_pitcher_name,
                gl.away_pitcher_id,
                gl.away_pitcher_name,
                gl.status,
                gl.lineup_posted,
                jsonb_array_length(gl.home_lineup) as home_lineup_count,
                jsonb_array_length(gl.away_lineup) as away_lineup_count,
                ROUND(pk_h.pk_plus::numeric, 1)    as home_pk_plus,
                ROUND(pk_a.pk_plus::numeric, 1)    as away_pk_plus,
                ROUND(g3_h.goose3_plus::numeric, 1) as home_goose3,
                ROUND(g3_a.goose3_plus::numeric, 1) as away_goose3
            FROM mlb.game_lineups gl
            LEFT JOIN mlb.pk_overall pk_h
                ON pk_h.pitcher_id = gl.home_pitcher_id AND pk_h.season = 2026
            LEFT JOIN mlb.pk_overall pk_a
                ON pk_a.pitcher_id = gl.away_pitcher_id AND pk_a.season = 2026
            LEFT JOIN mlb.goose3_overall g3_h
                ON g3_h.pitcher_id = gl.home_pitcher_id AND g3_h.season = 2026
            LEFT JOIN mlb.goose3_overall g3_a
                ON g3_a.pitcher_id = gl.away_pitcher_id AND g3_a.season = 2026
            WHERE gl.game_date = CURRENT_DATE
            ORDER BY gl.game_pk
        """)).mappings().all()
        result = {"games": [dict(r) for r in rows]}
        cache_set('today_with_lineups', result)
        return result
    except Exception:
        return {"games": [], "error": "game_lineups table not yet created — run fetch_lineups.py first"}


@router.get("/pitcher/{pitcher_id}/pitch-physics")
def pitcher_pitch_physics(pitcher_id: int, season: int = 2026, db: Session = Depends(get_db)):
    """Average pitch physics parameters per pitch type for 3D trajectory animation."""
    from sqlalchemy import text

    sql = text("""
        SELECT
            p.pitch_type_code,
            COUNT(*) as pitches,
            ROUND(AVG(p.x0)::numeric, 4) as x0,
            ROUND(AVG(p.y0)::numeric, 4) as y0,
            ROUND(AVG(p.z0)::numeric, 4) as z0,
            ROUND(AVG(p.vx0)::numeric, 4) as vx0,
            ROUND(AVG(p.vy0)::numeric, 4) as vy0,
            ROUND(AVG(p.vz0)::numeric, 4) as vz0,
            ROUND(AVG(p.ax)::numeric, 4) as ax,
            ROUND(AVG(p.ay)::numeric, 4) as ay,
            ROUND(AVG(p.az)::numeric, 4) as az,
            ROUND(AVG(p.pfx_x)::numeric, 3) as pfx_x,
            ROUND(AVG(p.pfx_z)::numeric, 3) as pfx_z,
            ROUND(AVG(p.start_speed)::numeric, 1) as avg_velo,
            ROUND(AVG(p.spin_rate)::numeric, 0) as avg_spin,
            ROUND(AVG(p.px)::numeric, 3) as avg_px,
            ROUND(AVG(p.pz)::numeric, 3) as avg_pz
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE ab.pitcher_id = :pid
        AND g.season = :season
        AND g.game_type = 'R'
        AND p.pitch_type_code IS NOT NULL
        AND p.x0 IS NOT NULL
        AND p.vx0 IS NOT NULL
        AND p.ax IS NOT NULL
        GROUP BY p.pitch_type_code
        HAVING COUNT(*) >= 10
        ORDER BY COUNT(*) DESC
    """)

    rows = db.execute(sql, {"pid": pitcher_id, "season": season}).mappings().all()

    if not rows:
        rows = db.execute(sql, {"pid": pitcher_id, "season": 2025}).mappings().all()

    pitcher_name = db.execute(text(
        "SELECT MAX(pitcher_name) FROM mlb.at_bats WHERE pitcher_id = :pid"
    ), {"pid": pitcher_id}).scalar()

    return {
        "pitcher_id": pitcher_id,
        "pitcher_name": pitcher_name,
        "season": season,
        "pitch_types": [dict(r) for r in rows],
    }


@router.get("/pitcher/{pitcher_id}/percentiles")
def pitcher_percentiles(pitcher_id: int, season: int = 2026, db: Session = Depends(get_db)):
    """Percentile rankings for pitcher metrics vs all qualified pitchers this season."""
    from sqlalchemy import text

    league_sql = text("""
        WITH pitcher_agg AS (
            SELECT
                pqs.pitcher_id,
                AVG(pqs.whiff_rate) as whiff_rate,
                AVG(pqs.csw_rate) as csw_rate,
                AVG(pqs.hard_hit_rate) as hard_hit_rate,
                SUM(pqs.pitches_thrown) as total_pitches
            FROM mlb.pitch_quality_scores pqs
            WHERE pqs.season = :season AND pqs.game_type = 'R'
            GROUP BY pqs.pitcher_id
            HAVING SUM(pqs.pitches_thrown) >= 50
        ),
        with_goose AS (
            SELECT
                pa.*,
                go.goose_plus,
                go.goose_plus_vs_lhh,
                go.goose_plus_vs_rhh
            FROM pitcher_agg pa
            LEFT JOIN mlb.goose_overall go
                ON go.pitcher_id = pa.pitcher_id
                AND go.season = :season
                AND go.game_pk IS NULL
        ),
        with_bbref AS (
            SELECT
                wg.*,
                bp.era_plus,
                fg.k_pct, fg.bb_pct, fg.swstr_pct
            FROM with_goose wg
            LEFT JOIN mlb.bbref_pitching bp
                ON bp.mlb_id = wg.pitcher_id
                AND bp.year_id = :season
            LEFT JOIN mlb.player_id_map m ON m.mlbam_id = wg.pitcher_id
            LEFT JOIN mlb.fangraphs_pitching fg
                ON fg.mlbam_id::text = m.fangraphs_id::text
                AND fg.season = :season
        ),
        with_models AS (
            SELECT
                wb.*,
                g3.goose3_plus,
                pk.pk_plus,
                phr.phr_plus
            FROM with_bbref wb
            LEFT JOIN mlb.goose3_overall g3
                ON g3.pitcher_id = wb.pitcher_id AND g3.season = :season
            LEFT JOIN mlb.pk_overall pk
                ON pk.pitcher_id = wb.pitcher_id AND pk.season = :season
            LEFT JOIN mlb.phr_overall phr
                ON phr.pitcher_id = wb.pitcher_id AND phr.season = :season
        )
        SELECT
            MAX(CASE WHEN pitcher_id = :pid THEN goose_plus END) as my_goose,
            MAX(CASE WHEN pitcher_id = :pid THEN goose3_plus END) as my_goose3,
            MAX(CASE WHEN pitcher_id = :pid THEN pk_plus END) as my_pk,
            MAX(CASE WHEN pitcher_id = :pid THEN phr_plus END) as my_phr,
            MAX(CASE WHEN pitcher_id = :pid THEN whiff_rate END) as my_whiff,
            MAX(CASE WHEN pitcher_id = :pid THEN csw_rate END) as my_csw,
            MAX(CASE WHEN pitcher_id = :pid THEN hard_hit_rate END) as my_hh,
            MAX(CASE WHEN pitcher_id = :pid THEN era_plus END) as my_era_plus,
            MAX(CASE WHEN pitcher_id = :pid THEN k_pct END) as my_k_pct,
            MAX(CASE WHEN pitcher_id = :pid THEN bb_pct END) as my_bb_pct,
            MIN(goose_plus) as min_goose, MAX(goose_plus) as max_goose,
            MIN(goose3_plus) as min_goose3, MAX(goose3_plus) as max_goose3,
            MIN(pk_plus) as min_pk, MAX(pk_plus) as max_pk,
            MIN(phr_plus) as min_phr, MAX(phr_plus) as max_phr,
            MIN(whiff_rate) as min_whiff, MAX(whiff_rate) as max_whiff,
            MIN(csw_rate) as min_csw, MAX(csw_rate) as max_csw,
            MIN(hard_hit_rate) as min_hh, MAX(hard_hit_rate) as max_hh,
            MIN(era_plus) as min_era_plus, MAX(era_plus) as max_era_plus,
            MIN(k_pct) as min_k_pct, MAX(k_pct) as max_k_pct,
            MIN(bb_pct) as min_bb_pct, MAX(bb_pct) as max_bb_pct,
            COUNT(*) as n_pitchers
        FROM with_models
    """)

    row = db.execute(league_sql, {"pid": pitcher_id, "season": season}).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Insufficient data")

    def pct_score(val, min_val, max_val, invert=False):
        if any(v is None for v in [val, min_val, max_val]):
            return None
        mn, mx = float(min_val), float(max_val)
        if mx == mn:
            return 50.0
        s = (float(val) - mn) / (mx - mn) * 100
        return round(100 - s if invert else s, 1)

    def fmt_pct(v):
        return round(float(v) * 100, 1) if v is not None else None

    return {
        "pitcher_id": pitcher_id,
        "season": season,
        "n_pitchers": row["n_pitchers"],
        "metrics": {
            "goose_plus": {
                "value": round(float(row["my_goose"]), 1) if row["my_goose"] else None,
                "min": round(float(row["min_goose"]), 1) if row["min_goose"] else None,
                "max": round(float(row["max_goose"]), 1) if row["max_goose"] else None,
                "percentile": pct_score(row["my_goose"], row["min_goose"], row["max_goose"]),
                "label": "Goose+", "higher_better": True,
            },
            "whiff_rate": {
                "value": fmt_pct(row["my_whiff"]),
                "min": fmt_pct(row["min_whiff"]),
                "max": fmt_pct(row["max_whiff"]),
                "percentile": pct_score(row["my_whiff"], row["min_whiff"], row["max_whiff"]),
                "label": "Whiff%", "higher_better": True,
            },
            "csw_rate": {
                "value": fmt_pct(row["my_csw"]),
                "min": fmt_pct(row["min_csw"]),
                "max": fmt_pct(row["max_csw"]),
                "percentile": pct_score(row["my_csw"], row["min_csw"], row["max_csw"]),
                "label": "CSW%", "higher_better": True,
            },
            "hard_hit_rate": {
                "value": fmt_pct(row["my_hh"]),
                "min": fmt_pct(row["min_hh"]),
                "max": fmt_pct(row["max_hh"]),
                "percentile": pct_score(row["my_hh"], row["min_hh"], row["max_hh"], invert=True),
                "label": "Hard Hit%", "higher_better": False,
            },
            "era_plus": {
                "value": round(float(row["my_era_plus"]), 1) if row["my_era_plus"] else None,
                "min": round(float(row["min_era_plus"]), 1) if row["min_era_plus"] else None,
                "max": round(float(row["max_era_plus"]), 1) if row["max_era_plus"] else None,
                "percentile": pct_score(row["my_era_plus"], row["min_era_plus"], row["max_era_plus"]),
                "label": "ERA+", "higher_better": True,
            },
            "k_pct": {
                "value": fmt_pct(row["my_k_pct"]),
                "min": fmt_pct(row["min_k_pct"]),
                "max": fmt_pct(row["max_k_pct"]),
                "percentile": pct_score(row["my_k_pct"], row["min_k_pct"], row["max_k_pct"]),
                "label": "K%", "higher_better": True,
            },
            "bb_pct": {
                "value": fmt_pct(row["my_bb_pct"]),
                "min": fmt_pct(row["min_bb_pct"]),
                "max": fmt_pct(row["max_bb_pct"]),
                "percentile": pct_score(row["my_bb_pct"], row["min_bb_pct"], row["max_bb_pct"], invert=True),
                "label": "BB%", "higher_better": False,
            },
        },
    }


@router.get("/pitcher/{pitcher_id}/pitch-percentiles")
def pitch_percentiles(pitcher_id: int, season: int = 2026, bat_side: str = None, db: Session = Depends(get_db)):
    """Per-pitch-type percentile rankings vs all pitchers throwing that pitch type. bat_side=L/R to filter by batter handedness."""
    from sqlalchemy import text

    bat_filter = "AND ab.bat_side = :bat_side" if bat_side else ""
    base_params = {"pid": pitcher_id, "season": season}
    if bat_side:
        base_params["bat_side"] = bat_side

    # Get target pitcher's per-pitch metrics
    my_sql = text(f"""
        SELECT
            p.pitch_type_code,
            AVG(p.start_speed) as avg_velo,
            AVG(p.spin_rate) as avg_spin,
            AVG(ABS(p.pfx_z)) as avg_ivb,
            AVG(p.pfx_x) as avg_hbreak,
            AVG(CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END) as whiff_rate,
            COUNT(*) as pitches
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE ab.pitcher_id = :pid
        AND g.season = :season AND g.game_type = 'R'
        AND p.pitch_type_code IS NOT NULL
        AND p.start_speed IS NOT NULL
        {bat_filter}
        GROUP BY p.pitch_type_code
        HAVING COUNT(*) >= 10
        ORDER BY COUNT(*) DESC
    """)
    my_rows = db.execute(my_sql, base_params).mappings().all()

    if not my_rows:
        fallback_params = {"pid": pitcher_id, "season": 2025}
        if bat_side:
            fallback_params["bat_side"] = bat_side
        my_rows = db.execute(my_sql, fallback_params).mappings().all()

    if not my_rows:
        return {"pitcher_id": pitcher_id, "season": season, "bat_side": bat_side, "pitches": []}

    pitch_types = [r["pitch_type_code"] for r in my_rows]

    # Get league ranges for each pitch type (separate query per type to avoid array binding issues)
    league_map = {}
    for pt in pitch_types:
        lg_sql = text("""
            SELECT
                MIN(s.avg_velo) as min_velo, MAX(s.avg_velo) as max_velo,
                MIN(s.avg_spin) as min_spin, MAX(s.avg_spin) as max_spin,
                MIN(s.avg_ivb) as min_ivb, MAX(s.avg_ivb) as max_ivb,
                MIN(s.avg_hbreak) as min_hbreak, MAX(s.avg_hbreak) as max_hbreak,
                MIN(s.whiff_rate) as min_whiff, MAX(s.whiff_rate) as max_whiff
            FROM (
                SELECT ab.pitcher_id,
                    AVG(p.start_speed) as avg_velo,
                    AVG(p.spin_rate) as avg_spin,
                    AVG(ABS(p.pfx_z)) as avg_ivb,
                    AVG(ABS(p.pfx_x)) as avg_hbreak,
                    AVG(CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END) as whiff_rate
                FROM mlb.pitches p
                JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
                    AND ab.at_bat_index = p.at_bat_index
                JOIN mlb.games g ON g.game_pk = p.game_pk
                WHERE g.season = :season AND g.game_type = 'R'
                AND p.pitch_type_code = :pt
                AND p.start_speed IS NOT NULL
                GROUP BY ab.pitcher_id
                HAVING COUNT(*) >= 20
            ) s
        """)
        lg_row = db.execute(lg_sql, {"season": season, "pt": pt}).mappings().first()
        if lg_row:
            league_map[pt] = lg_row
    # league_map already built above per pitch type

    def pct(val, mn, mx, invert=False):
        if any(v is None for v in [val, mn, mx]):
            return 50.0
        mn_f, mx_f = float(mn), float(mx)
        if mx_f == mn_f:
            return 50.0
        s = (float(val) - mn_f) / (mx_f - mn_f) * 100
        return round(100 - s if invert else s, 1)

    result = []
    for r in my_rows:
        pt = r["pitch_type_code"]
        lg = league_map.get(pt, {})
        result.append({
            "pitch_type": pt,
            "pitches": r["pitches"],
            "metrics": {
                "avg_velo": {
                    "value": round(float(r["avg_velo"]), 1),
                    "min": round(float(lg["min_velo"]), 1) if lg.get("min_velo") else None,
                    "max": round(float(lg["max_velo"]), 1) if lg.get("max_velo") else None,
                    "percentile": pct(r["avg_velo"], lg.get("min_velo"), lg.get("max_velo")),
                    "label": "Avg Velocity",
                },
                "whiff_pct": {
                    "value": round(float(r["whiff_rate"]) * 100, 1),
                    "min": round(float(lg["min_whiff"]) * 100, 1) if lg.get("min_whiff") else None,
                    "max": round(float(lg["max_whiff"]) * 100, 1) if lg.get("max_whiff") else None,
                    "percentile": pct(r["whiff_rate"], lg.get("min_whiff"), lg.get("max_whiff")),
                    "label": "Whiff%",
                },
                "spin": {
                    "value": round(float(r["avg_spin"]), 0) if r["avg_spin"] else None,
                    "min": round(float(lg["min_spin"]), 0) if lg.get("min_spin") else None,
                    "max": round(float(lg["max_spin"]), 0) if lg.get("max_spin") else None,
                    "percentile": pct(r["avg_spin"], lg.get("min_spin"), lg.get("max_spin")),
                    "label": "Spin Rate",
                },
                "ivb": {
                    "value": round(float(r["avg_ivb"]), 1) if r["avg_ivb"] else None,
                    "min": round(float(lg["min_ivb"]), 1) if lg.get("min_ivb") else None,
                    "max": round(float(lg["max_ivb"]), 1) if lg.get("max_ivb") else None,
                    "percentile": pct(r["avg_ivb"], lg.get("min_ivb"), lg.get("max_ivb")),
                    "label": "IVB (in)",
                },
                "hbreak": {
                    "value": round(abs(float(r["avg_hbreak"])), 1) if r["avg_hbreak"] else None,
                    "min": round(float(lg["min_hbreak"]), 1) if lg.get("min_hbreak") else None,
                    "max": round(float(lg["max_hbreak"]), 1) if lg.get("max_hbreak") else None,
                    "percentile": pct(abs(float(r["avg_hbreak"] or 0)), lg.get("min_hbreak"), lg.get("max_hbreak")),
                    "label": "H-Break (in)",
                },
            },
        })
    return {"pitcher_id": pitcher_id, "season": season, "bat_side": bat_side, "pitches": result}


@router.get("/hitter/{batter_id}/percentiles")
def hitter_percentiles(batter_id: int, season: int = 2026, db: Session = Depends(get_db)):
    """Hitter percentile rankings + Backwards Helmet % and LHP/RHP swing rate splits."""
    from sqlalchemy import text

    from datetime import date as _date, timedelta as _timedelta
    cutoff_14 = (_date.today() - _timedelta(days=14)).isoformat()

    _SWING_SELECT = """
                ROUND(AVG(CASE
                    WHEN p.balls_before IN (2,3) AND p.strikes_before = 0 AND p.balls_before = 2
                        THEN CASE WHEN p.call_code IN ('S','W','T','F','D','E','X') THEN 1.0 ELSE 0.0 END
                    WHEN p.balls_before = 3 AND p.strikes_before = 0
                        THEN CASE WHEN p.call_code IN ('S','W','T','F','D','E','X') THEN 1.0 ELSE 0.0 END
                    WHEN p.balls_before = 3 AND p.strikes_before = 1
                        THEN CASE WHEN p.call_code IN ('S','W','T','F','D','E','X') THEN 1.0 ELSE 0.0 END
                    END) * 100, 1) as hitters_count_swing,
                ROUND(AVG(CASE
                    WHEN p.strikes_before = 2
                        THEN CASE WHEN p.call_code IN ('S','W','T','F','D','E','X') THEN 1.0 ELSE 0.0 END
                    END) * 100, 1) as pitchers_count_swing,
                ROUND(AVG(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X')
                    THEN 1.0 ELSE 0.0 END) * 100, 1) as overall_swing,
                ROUND(
                    AVG(CASE WHEN p.call_code IN ('F','D','E','X') THEN 1.0 ELSE 0.0 END) /
                    NULLIF(AVG(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X') THEN 1.0 ELSE 0.0 END), 0)
                * 100, 1) as contact_pct,
                ROUND(
                    AVG(CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END) /
                    NULLIF(AVG(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X') THEN 1.0 ELSE 0.0 END), 0)
                * 100, 1) as whiff_pct,
                ROUND(
                    AVG(CASE WHEN p.zone BETWEEN 1 AND 9 AND p.call_code IN ('F','D','E','X') THEN 1.0 ELSE 0.0 END) /
                    NULLIF(AVG(CASE WHEN p.zone BETWEEN 1 AND 9 AND p.call_code IN ('S','W','T','F','D','E','X') THEN 1.0 ELSE 0.0 END), 0)
                * 100, 1) as zone_contact_pct,
                ROUND(
                    AVG(CASE WHEN (p.zone IS NULL OR p.zone NOT BETWEEN 1 AND 9) AND p.call_code IN ('S','W','T','F','D','E','X') THEN 1.0 ELSE 0.0 END) /
                    NULLIF(AVG(CASE WHEN (p.zone IS NULL OR p.zone NOT BETWEEN 1 AND 9) THEN 1.0 ELSE 0.0 END), 0)
                * 100, 1) as chase_pct,
                COUNT(*) as total_pitches"""

    def run_swing_stats(pitch_hand=None, cutoff=None):
        hand_filter = "AND ab.pitch_hand = :phand" if pitch_hand else ""
        date_filter = "AND g.game_date >= :cutoff" if cutoff else "AND g.season = :season"
        params = {"bid": batter_id, "season": season}
        if pitch_hand:
            params["phand"] = pitch_hand
        if cutoff:
            params["cutoff"] = cutoff
        sql = text(f"""
            SELECT {_SWING_SELECT}
            FROM mlb.pitches p
            JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
                AND ab.at_bat_index = p.at_bat_index
            JOIN mlb.games g ON g.game_pk = p.game_pk
            WHERE ab.batter_id = :bid
            AND g.game_type = 'R'
            AND p.balls_before IS NOT NULL
            {date_filter}
            {hand_filter}
        """)
        return db.execute(sql, params).mappings().first()

    def run_league_avg_swing_stats(pitch_hand=None):
        hand_filter = "AND ab.pitch_hand = :phand" if pitch_hand else ""
        params = {"season": season}
        if pitch_hand:
            params["phand"] = pitch_hand
        sql = text(f"""
            SELECT {_SWING_SELECT}
            FROM mlb.pitches p
            JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
                AND ab.at_bat_index = p.at_bat_index
            JOIN mlb.games g ON g.game_pk = p.game_pk
            WHERE g.season = :season AND g.game_type = 'R'
            AND p.balls_before IS NOT NULL
            {hand_filter}
        """)
        return db.execute(sql, params).mappings().first()

    def build_swing_rates(s):
        if not s:
            return {}
        bh = None
        if s["hitters_count_swing"] is not None and s["pitchers_count_swing"] is not None:
            bh = round(float(s["hitters_count_swing"]) - float(s["pitchers_count_swing"]), 1)
        return {
            "total_pitches": int(s["total_pitches"]) if s["total_pitches"] else 0,
            "swing_pct": float(s["overall_swing"]) if s["overall_swing"] else None,
            "contact_pct": float(s["contact_pct"]) if s["contact_pct"] else None,
            "whiff_pct": float(s["whiff_pct"]) if s["whiff_pct"] else None,
            "zone_contact_pct": float(s["zone_contact_pct"]) if s["zone_contact_pct"] else None,
            "chase_pct": float(s["chase_pct"]) if s["chase_pct"] else None,
            "backwards_helmet_pct": bh,
            "hitters_count_swing": float(s["hitters_count_swing"]) if s["hitters_count_swing"] else None,
            "pitchers_count_swing": float(s["pitchers_count_swing"]) if s["pitchers_count_swing"] else None,
        }

    stats = run_swing_stats()
    stats_lhp = run_swing_stats('L')
    stats_rhp = run_swing_stats('R')
    stats_14 = run_swing_stats(cutoff=cutoff_14)
    stats_lhp_14 = run_swing_stats('L', cutoff=cutoff_14)
    stats_rhp_14 = run_swing_stats('R', cutoff=cutoff_14)
    stats_lg = run_league_avg_swing_stats()
    stats_lg_lhp = run_league_avg_swing_stats('L')
    stats_lg_rhp = run_league_avg_swing_stats('R')

    backwards_helmet = None
    if stats and stats["hitters_count_swing"] is not None and stats["pitchers_count_swing"] is not None:
        backwards_helmet = round(
            float(stats["hitters_count_swing"]) - float(stats["pitchers_count_swing"]), 1
        )

    league_sql = text("""
        WITH batter_stats AS (
            SELECT ab.batter_id,
                AVG(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X') THEN 1.0 ELSE 0.0 END) as swing_pct,
                AVG(CASE WHEN p.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END) /
                    NULLIF(AVG(CASE WHEN p.call_code IN ('S','W','T','F','D','E','X') THEN 1.0 ELSE 0.0 END), 0) as whiff_pct,
                AVG(CASE WHEN (p.zone IS NULL OR p.zone NOT BETWEEN 1 AND 9) AND p.call_code IN ('S','W','T','F','D','E','X') THEN 1.0 ELSE 0.0 END) /
                    NULLIF(AVG(CASE WHEN (p.zone IS NULL OR p.zone NOT BETWEEN 1 AND 9) THEN 1.0 ELSE 0.0 END), 0) as chase_pct
            FROM mlb.pitches p
            JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk AND ab.at_bat_index = p.at_bat_index
            JOIN mlb.games g ON g.game_pk = p.game_pk
            WHERE g.season = :season AND g.game_type = 'R' AND p.balls_before IS NOT NULL
            GROUP BY ab.batter_id
            HAVING COUNT(*) >= 100
        )
        SELECT
            MIN(swing_pct) as min_swing, MAX(swing_pct) as max_swing,
            MIN(whiff_pct) as min_whiff, MAX(whiff_pct) as max_whiff,
            MIN(chase_pct) as min_chase, MAX(chase_pct) as max_chase
        FROM batter_stats
    """)
    lg = db.execute(league_sql, {"season": season}).mappings().first()

    juiced_sql = text("""
        SELECT
            ROUND(SUM(juiced_plus * pa)::numeric / NULLIF(SUM(pa), 0), 1) as juiced_plus,
            ROUND(SUM(juiced_plus * pa) FILTER (WHERE pitch_type_code IN ('FF','SI','FC'))::numeric
                / NULLIF(SUM(pa) FILTER (WHERE pitch_type_code IN ('FF','SI','FC')), 0), 1) as juiced_hard,
            ROUND(SUM(juiced_plus * pa) FILTER (WHERE pitch_type_code IN ('SL','ST','CU','KC'))::numeric
                / NULLIF(SUM(pa) FILTER (WHERE pitch_type_code IN ('SL','ST','CU','KC')), 0), 1) as juiced_breaking,
            ROUND(SUM(juiced_plus * pa) FILTER (WHERE pitch_type_code IN ('CH','FS'))::numeric
                / NULLIF(SUM(pa) FILTER (WHERE pitch_type_code IN ('CH','FS')), 0), 1) as juiced_soft
        FROM mlb.juiced_scores
        WHERE batter_id = :bid AND season = :season AND pa >= 3
    """)
    juiced = db.execute(juiced_sql, {"bid": batter_id, "season": season}).mappings().first()

    def pct(val, mn, mx, invert=False):
        if any(v is None for v in [val, mn, mx]):
            return 50.0
        mn_f, mx_f = float(mn), float(mx)
        if mx_f == mn_f:
            return 50.0
        s = (float(val) - mn_f) / (mx_f - mn_f) * 100
        return round(100 - s if invert else s, 1)

    headshot_url = f"https://img.mlbstatic.com/mlb-photos/image/upload/d_people:generic:headshot:67:current.png/w_213,q_auto:best/v1/people/{batter_id}/headshot/67/current"

    juiced_val = float(juiced["juiced_plus"]) if juiced and juiced["juiced_plus"] else None
    swing_raw = float(stats["overall_swing"]) / 100 if stats and stats["overall_swing"] else None
    whiff_raw = float(stats["whiff_pct"]) / 100 if stats and stats["whiff_pct"] else None
    chase_raw = float(stats["chase_pct"]) / 100 if stats and stats["chase_pct"] else None

    return {
        "batter_id": batter_id,
        "season": season,
        "headshot_url": headshot_url,
        "swing_rates": build_swing_rates(stats),
        "swing_rates_lhp": build_swing_rates(stats_lhp),
        "swing_rates_rhp": build_swing_rates(stats_rhp),
        "swing_rates_14": build_swing_rates(stats_14),
        "swing_rates_lhp_14": build_swing_rates(stats_lhp_14),
        "swing_rates_rhp_14": build_swing_rates(stats_rhp_14),
        "swing_rates_lg": build_swing_rates(stats_lg),
        "swing_rates_lg_lhp": build_swing_rates(stats_lg_lhp),
        "swing_rates_lg_rhp": build_swing_rates(stats_lg_rhp),
        "juiced": dict(juiced) if juiced else {},
        "percentiles": {
            "juiced_plus": {
                "value": juiced_val,
                "min": 50.0, "max": 180.0,
                "percentile": pct(juiced_val, 50.0, 180.0) if juiced_val else 50.0,
                "label": "Juiced+",
            },
            "swing_pct": {
                "value": float(stats["overall_swing"]) if stats and stats["overall_swing"] else None,
                "min": round(float(lg["min_swing"]) * 100, 1) if lg and lg["min_swing"] else None,
                "max": round(float(lg["max_swing"]) * 100, 1) if lg and lg["max_swing"] else None,
                "percentile": pct(swing_raw, lg["min_swing"], lg["max_swing"]) if lg else 50.0,
                "label": "Swing%",
            },
            "whiff_pct": {
                "value": float(stats["whiff_pct"]) if stats and stats["whiff_pct"] else None,
                "min": round(float(lg["min_whiff"]) * 100, 1) if lg and lg["min_whiff"] else None,
                "max": round(float(lg["max_whiff"]) * 100, 1) if lg and lg["max_whiff"] else None,
                "percentile": pct(whiff_raw, lg["min_whiff"], lg["max_whiff"], invert=True) if lg else 50.0,
                "label": "Whiff%", "higher_better": False,
            },
            "chase_pct": {
                "value": float(stats["chase_pct"]) if stats and stats["chase_pct"] else None,
                "min": round(float(lg["min_chase"]) * 100, 1) if lg and lg["min_chase"] else None,
                "max": round(float(lg["max_chase"]) * 100, 1) if lg and lg["max_chase"] else None,
                "percentile": pct(chase_raw, lg["min_chase"], lg["max_chase"], invert=True) if lg else 50.0,
                "label": "Chase%", "higher_better": False,
            },
            "backwards_helmet": {
                "value": backwards_helmet,
                "min": -40.0, "max": 40.0,
                "percentile": pct(backwards_helmet, -40.0, 40.0) if backwards_helmet is not None else 50.0,
                "label": "Backwards Helmet%",
            },
            "contact_pct": {
                "value": float(stats["contact_pct"]) if stats and stats["contact_pct"] else None,
                "min": 50.0, "max": 100.0,
                "percentile": pct(float(stats["contact_pct"] or 75) / 100, 0.50, 1.0) if stats else 50.0,
                "label": "Contact%",
            },
        },
    }


@router.get("/hitter/{batter_id}/juiced-splits")
def hitter_juiced_splits(batter_id: int, season: int = 2026,
                          db: Session = Depends(get_db)):
    """Juiced+ splits by pitcher hand (L/R). League average = 100."""
    from sqlalchemy import text

    bat_side = db.execute(text("""
        SELECT bat_side FROM mlb.at_bats
        WHERE batter_id = :bid AND bat_side IS NOT NULL LIMIT 1
    """), {"bid": batter_id}).scalar() or 'R'

    overall_row = db.execute(text("""
        SELECT ROUND(juiced2_plus::numeric,1) as juiced2_plus,
               ROUND(quality_adj_idx::numeric,1) as quality_adj_idx,
               ROUND(avg_goose2_faced::numeric,1) as avg_goose2_faced,
               ROUND(ops::numeric,3) as ops,
               total_pa
        FROM mlb.juiced2_scores
        WHERE batter_id = :bid AND season = :season
    """), {"bid": batter_id, "season": season}).mappings().first()

    splits_sql = text("""
        WITH batter_stats AS (
            SELECT
                ab.pitch_hand,
                COUNT(*) as pa,
                SUM(CASE WHEN ab.event IN ('Single','Double','Triple','Home Run')
                    THEN 1 ELSE 0 END) as hits,
                SUM(CASE WHEN ab.event IN ('Walk','Intent Walk','Hit By Pitch')
                    THEN 1 ELSE 0 END) as bb,
                SUM(CASE WHEN ab.event IN ('Strikeout','Strikeout - DP')
                    THEN 1 ELSE 0 END) as k,
                SUM(CASE WHEN ab.event = 'Home Run' THEN 1 ELSE 0 END) as hr,
                AVG(CASE ab.event
                    WHEN 'Single' THEN 1.0 WHEN 'Double' THEN 2.0
                    WHEN 'Triple' THEN 3.0 WHEN 'Home Run' THEN 4.0
                    ELSE 0.0 END) as slg,
                AVG(CASE WHEN ab.event IN ('Single','Double','Triple','Home Run',
                    'Walk','Intent Walk','Hit By Pitch')
                    THEN 1.0 ELSE 0.0 END) as obp,
                AVG(g2.goose2_plus) as avg_goose2_faced
            FROM mlb.at_bats ab
            JOIN mlb.games g ON g.game_pk = ab.game_pk
            LEFT JOIN mlb.goose2_overall g2 ON g2.pitcher_id = ab.pitcher_id
                AND g2.season = :season
            WHERE ab.batter_id = :bid
            AND g.season = :season AND g.game_type = 'R'
            AND ab.pitch_hand IN ('L','R')
            AND ab.event NOT IN ('Runner Out','Caught Stealing 2B',
                'Caught Stealing 3B','Wild Pitch','Passed Ball')
            GROUP BY ab.pitch_hand
        ),
        league_stats AS (
            SELECT
                ab.bat_side,
                ab.pitch_hand,
                AVG(CASE ab.event
                    WHEN 'Single' THEN 1.0 WHEN 'Double' THEN 2.0
                    WHEN 'Triple' THEN 3.0 WHEN 'Home Run' THEN 4.0
                    ELSE 0.0 END) as lg_slg,
                AVG(CASE WHEN ab.event IN ('Single','Double','Triple','Home Run',
                    'Walk','Intent Walk','Hit By Pitch')
                    THEN 1.0 ELSE 0.0 END) as lg_obp
            FROM mlb.at_bats ab
            JOIN mlb.games g ON g.game_pk = ab.game_pk
            WHERE g.season = :season AND g.game_type = 'R'
            AND ab.bat_side = :bat_side
            AND ab.pitch_hand IN ('L','R')
            AND ab.event NOT IN ('Runner Out','Caught Stealing 2B',
                'Caught Stealing 3B','Wild Pitch','Passed Ball')
            GROUP BY ab.bat_side, ab.pitch_hand
        )
        SELECT
            bs.pitch_hand,
            bs.pa,
            bs.hits, bs.bb, bs.k, bs.hr,
            ROUND(CAST(bs.slg AS numeric), 3) as slg,
            ROUND(CAST(bs.obp AS numeric), 3) as obp,
            ROUND(CAST(bs.slg + bs.obp AS numeric), 3) as ops,
            ROUND(CAST(bs.avg_goose2_faced AS numeric), 1) as avg_goose2_faced,
            ROUND(CAST(
                ((bs.obp / NULLIF(ls.lg_obp, 0)) +
                 (bs.slg / NULLIF(ls.lg_slg, 0)) - 1) * 100
            AS numeric), 0) as ops_plus,
            ROUND(CAST(
                (bs.slg + bs.obp) /
                NULLIF(
                    (ls.lg_slg + ls.lg_obp) *
                    (100.0 / NULLIF(bs.avg_goose2_faced, 100)),
                    0
                ) * 100
            AS numeric), 1) as juiced_plus
        FROM batter_stats bs
        LEFT JOIN league_stats ls ON ls.pitch_hand = bs.pitch_hand
        ORDER BY bs.pitch_hand
    """)

    splits = db.execute(splits_sql, {
        "bid": batter_id, "season": season, "bat_side": bat_side
    }).mappings().all()

    result = {
        "batter_id": batter_id,
        "bat_side": bat_side,
        "season": season,
        "overall": dict(overall_row) if overall_row else None,
        "vs_lhp": None,
        "vs_rhp": None,
    }
    for r in splits:
        hand_key = "vs_lhp" if r['pitch_hand'] == 'L' else "vs_rhp"
        result[hand_key] = dict(r)

    return result


@router.get("/goose2/leaderboard")
def goose2_leaderboard(
    season: int = 2026,
    min_pitches: int = 100,
    db: Session = Depends(get_db)
):
    """Goose+ 2 leaderboard — experimental model with regression to mean."""
    from sqlalchemy import text
    sql = text("""
        SELECT
            go.pitcher_id,
            go.pitcher_name,
            go.pitch_hand,
            ROUND(go.goose2_plus::numeric, 1) as goose2_plus,
            ROUND(go.goose2_raw::numeric, 1) as goose2_raw,
            ROUND(go.goose2_vs_lhh::numeric, 1) as goose2_vs_lhh,
            ROUND(go.goose2_vs_rhh::numeric, 1) as goose2_vs_rhh,
            go.total_pitches,
            br.era_plus,
            ROUND(br.war::numeric, 2) as war
        FROM mlb.goose2_overall go
        LEFT JOIN mlb.bbref_pitching br ON br.mlb_id = go.pitcher_id
            AND br.year_id = go.season
        WHERE go.season = :season
          AND go.total_pitches >= :min_pitches
        ORDER BY go.goose2_plus DESC
        LIMIT 100
    """)
    rows = db.execute(sql, {"season": season, "min_pitches": min_pitches}).mappings().all()
    return {"season": season, "model": "goose2", "pitchers": [dict(r) for r in rows]}


@router.get("/goose2/pitcher/{pitcher_id}")
def goose2_pitcher(pitcher_id: int, season: int = 2026, db: Session = Depends(get_db)):
    """Goose+ 2 breakdown by pitch type for a pitcher."""
    from sqlalchemy import text
    sql = text("""
        SELECT
            pitch_type_code,
            bat_side,
            ROUND(goose2_plus::numeric, 1) as goose2_plus,
            ROUND(goose2_raw::numeric, 1) as goose2_raw,
            ROUND(contact_pct::numeric * 100, 1) as contact_pct,
            ROUND(whiff_pct::numeric * 100, 1) as whiff_pct,
            ROUND(csw_pct::numeric * 100, 1) as csw_pct,
            ROUND(slg_against::numeric, 3) as slg_against,
            ROUND(chase_pct::numeric * 100, 1) as chase_pct,
            contact_idx, whiff_idx, csw_idx,
            slg_idx, hard_hit_idx, chase_idx,
            pitches
        FROM mlb.goose2_scores
        WHERE pitcher_id = :pid AND season = :season
        ORDER BY pitches DESC
    """)
    rows = db.execute(sql, {"pid": pitcher_id, "season": season}).mappings().all()

    overall_sql = text("""
        SELECT
            ROUND(goose2_plus::numeric, 1) as goose2_plus,
            ROUND(goose2_raw::numeric, 1) as goose2_raw,
            ROUND(goose2_vs_lhh::numeric, 1) as goose2_vs_lhh,
            ROUND(goose2_vs_rhh::numeric, 1) as goose2_vs_rhh,
            total_pitches
        FROM mlb.goose2_overall
        WHERE pitcher_id = :pid AND season = :season
    """)
    overall = db.execute(overall_sql, {"pid": pitcher_id, "season": season}).mappings().first()

    return {
        "pitcher_id": pitcher_id,
        "season": season,
        "overall": dict(overall) if overall else None,
        "by_pitch": [dict(r) for r in rows],
    }


@router.get("/juiced2/leaderboard")
def juiced2_leaderboard(season: int = 2026, min_pa: int = 100, db: Session = Depends(get_db)):
    """Juiced+ 2 leaderboard — quality-adjusted hitter metric."""
    from sqlalchemy import text
    sql = text("""
        SELECT
            j.batter_id,
            j.batter_name,
            j.bat_side,
            ROUND(j.juiced2_plus::numeric, 1) as juiced2_plus,
            ROUND(j.slg::numeric, 3) as slg,
            ROUND(j.ops::numeric, 3) as ops,
            ROUND(j.avg_goose2_faced::numeric, 1) as avg_goose2_faced,
            ROUND(j.slg_vs_expected::numeric, 3) as slg_vs_expected,
            ROUND(j.quality_adj_idx::numeric, 1) as quality_adj_idx,
            ROUND(j.slg_idx::numeric, 1) as slg_idx,
            ROUND(j.ops_idx::numeric, 1) as ops_idx,
            j.total_pa,
            br.ops_plus
        FROM mlb.juiced2_scores j
        LEFT JOIN mlb.bbref_batting br ON br.mlb_id = j.batter_id
            AND br.year_id = j.season
        WHERE j.season = :season AND j.total_pa >= :min_pa
        ORDER BY j.juiced2_plus DESC
        LIMIT 100
    """)
    rows = db.execute(sql, {"season": season, "min_pa": min_pa}).mappings().all()
    return {"season": season, "model": "juiced2", "batters": [dict(r) for r in rows]}


@router.get("/juiced2/batter/{batter_id}")
def juiced2_batter(batter_id: int, season: int = 2026, db: Session = Depends(get_db)):
    """Juiced+ 2 detail for a batter."""
    from sqlalchemy import text
    sql = text("""
        SELECT j.*, br.ops_plus
        FROM mlb.juiced2_scores j
        LEFT JOIN mlb.bbref_batting br ON br.mlb_id = j.batter_id
            AND br.year_id = j.season
        WHERE j.batter_id = :bid AND j.season = :season
    """)
    row = db.execute(sql, {"bid": batter_id, "season": season}).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="No Juiced+ 2 data found")

    components = {
        "ops":         {"value": float(row["ops_idx"]),         "weight": 0.30, "contribution": round(float(row["ops_idx"]) * 0.30, 1),         "label": "OPS Index"},
        "quality_adj": {"value": float(row["quality_adj_idx"]), "weight": 0.30, "contribution": round(float(row["quality_adj_idx"]) * 0.30, 1), "label": "Quality Adjustment"},
        "slg":         {"value": float(row["slg_idx"]),         "weight": 0.20, "contribution": round(float(row["slg_idx"]) * 0.20, 1),         "label": "SLG Index"},
        "hard_hit":    {
            "value": float(row["hard_hit_idx"]), "weight": 0.20,
            "contribution": round(float(row["hard_hit_idx"]) * 0.20, 1),
            "label": "Hard Hit Index",
            "source": row.get("hard_hit_source", "unknown"),
            "bat_speed": float(row["bat_speed"]) if row.get("bat_speed") else None,
            "squared_up_pct": float(row["squared_up_pct"]) if row.get("squared_up_pct") else None,
            "blast_rate": float(row["blast_rate"]) if row.get("blast_rate") else None,
        },
    }

    return {
        "batter_id":       batter_id,
        "season":          season,
        "juiced2_plus":    row["juiced2_plus"],
        "avg_goose2_faced": row["avg_goose2_faced"],
        "slg_vs_expected": row["slg_vs_expected"],
        "ops_plus":        row["ops_plus"],
        "components":      components,
        **{k: row[k] for k in ["slg","ops","hard_hit_pct","total_pa",
                                "slg_idx","ops_idx","quality_adj_idx","hard_hit_idx"]},
    }


@router.get("/stuff/pitcher/{pitcher_id}")
def stuff_pitcher(pitcher_id: int, season: int = 2026, db: Session = Depends(get_db)):
    """Stuff Score summary for a pitcher."""
    from sqlalchemy import text
    sql = text("""
        SELECT ROUND(so.stuff_score::numeric, 1) as stuff_score,
               so.top_pitch_type,
               ROUND(so.top_pitch_stuff::numeric, 1) as top_pitch_stuff,
               so.total_pitches,
               ARRAY_AGG(
                   JSON_BUILD_OBJECT(
                       'pitch_type', ss.pitch_type_code,
                       'stuff_score', ROUND(ss.stuff_score::numeric, 1),
                       'physical', ROUND(ss.physical_score::numeric, 1),
                       'location', ROUND(ss.location_score::numeric, 1),
                       'tunnel', ROUND(ss.tunnel_score::numeric, 1),
                       'pitches', ss.pitches
                   ) ORDER BY ss.pitches DESC
               ) as by_pitch
        FROM mlb.stuff_overall so
        JOIN mlb.stuff_scores ss ON ss.pitcher_id = so.pitcher_id
            AND ss.season = so.season
        WHERE so.pitcher_id = :pid AND so.season = :season
        GROUP BY so.stuff_score, so.top_pitch_type, so.top_pitch_stuff,
                 so.total_pitches
    """)
    row = db.execute(sql, {"pid": pitcher_id, "season": season}).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="No Stuff Score data")
    return dict(row)


@router.get("/goose3/leaderboard")
def goose3_leaderboard(season: int = 2026, min_pitches: int = 100,
                        db: Session = Depends(get_db)):
    """Goose+ 3 leaderboard — unified physics + outcomes model."""
    from sqlalchemy import text
    sql = text("""
        SELECT g3.pitcher_id, g3.pitcher_name, g3.pitch_hand,
               ROUND(g3.goose3_plus::numeric, 1) as goose3_plus,
               ROUND(g3.goose3_vs_lhh::numeric, 1) as goose3_vs_lhh,
               ROUND(g3.goose3_vs_rhh::numeric, 1) as goose3_vs_rhh,
               ROUND(g3.avg_stuff_score::numeric, 1) as avg_stuff,
               ROUND(g3.avg_goose2_score::numeric, 1) as avg_goose2,
               g3.stuff_outcomes_gap,
               g3.total_pitches,
               ROUND(go1.goose_plus::numeric, 1) as goose1_plus,
               br.era_plus
        FROM mlb.goose3_overall g3
        LEFT JOIN mlb.goose_overall go1 ON go1.pitcher_id = g3.pitcher_id
            AND go1.season = g3.season AND go1.game_pk IS NULL
        LEFT JOIN mlb.bbref_pitching br ON br.mlb_id = g3.pitcher_id
            AND br.year_id = g3.season
        WHERE g3.season = :season AND g3.total_pitches >= :min_pitches
        ORDER BY g3.goose3_plus DESC
        LIMIT 100
    """)
    rows = db.execute(sql, {
        "season": season, "min_pitches": min_pitches
    }).mappings().all()
    return {"season": season, "model": "goose3", "pitchers": [dict(r) for r in rows]}


@router.get("/goose3/pitcher/{pitcher_id}")
def goose3_pitcher(pitcher_id: int, season: int = 2026,
                    db: Session = Depends(get_db)):
    """Goose+ 3 detail for a pitcher."""
    from sqlalchemy import text
    overall = db.execute(text("""
        SELECT ROUND(goose3_plus::numeric, 1) as goose3_plus,
               ROUND(goose3_vs_lhh::numeric, 1) as goose3_vs_lhh,
               ROUND(goose3_vs_rhh::numeric, 1) as goose3_vs_rhh,
               ROUND(avg_stuff_score::numeric, 1) as avg_stuff,
               ROUND(avg_goose2_score::numeric, 1) as avg_goose2,
               stuff_outcomes_gap, total_pitches
        FROM mlb.goose3_overall
        WHERE pitcher_id = :pid AND season = :season
    """), {"pid": pitcher_id, "season": season}).mappings().first()

    by_pitch = db.execute(text("""
        SELECT pitch_type_code, bat_side,
               ROUND(goose3_plus::numeric, 1) as goose3_plus,
               ROUND(stuff_score::numeric, 1) as stuff_score,
               ROUND(goose2_score::numeric, 1) as goose2_score,
               stuff_outcomes_gap,
               ROUND(stuff_weight::numeric, 2) as stuff_weight,
               ROUND(outcomes_weight::numeric, 2) as outcomes_weight,
               pitches
        FROM mlb.goose3_scores
        WHERE pitcher_id = :pid AND season = :season
        ORDER BY pitches DESC
    """), {"pid": pitcher_id, "season": season}).mappings().all()

    if not overall:
        raise HTTPException(status_code=404, detail="No Goose+ 3 data")

    return {
        "pitcher_id": pitcher_id,
        "season": season,
        "overall": dict(overall),
        "by_pitch": [dict(r) for r in by_pitch],
    }


# ── GAME COMPANION ENDPOINTS ──────────────────────────────────────────────────



_TEAM_ID_TO_ABBREV = {
    109:'ARI',144:'ATL',110:'BAL',111:'BOS',112:'CHC',145:'CWS',
    113:'CIN',114:'CLE',115:'COL',116:'DET',117:'HOU',118:'KC',
    108:'LAA',119:'LAD',146:'MIA',158:'MIL',142:'MIN',121:'NYM',
    147:'NYY',133:'OAK',143:'PHI',134:'PIT',135:'SD', 137:'SF',
    136:'SEA',138:'STL',139:'TB', 140:'TEX',141:'TOR',120:'WSH',
}


@router.get("/game/{game_pk}/summary")
def game_summary(game_pk: int, db: Session = Depends(get_db)):
    """Full game state for Game Companion."""
    from sqlalchemy import text

    game_sql = text("""
        SELECT game_pk, game_date, game_type, season, status,
               home_team_abbrev, away_team_abbrev,
               home_team_id, away_team_id
        FROM mlb.games
        WHERE game_pk = :gp
    """)
    game_row = db.execute(game_sql, {"gp": game_pk}).mappings().first()
    if not game_row:
        raise HTTPException(status_code=404, detail="Game not found")
    game = dict(game_row)
    game['home_abbrev'] = game.get('home_team_abbrev') or _TEAM_ID_TO_ABBREV.get(game.get('home_team_id'), '???')
    game['away_abbrev'] = game.get('away_team_abbrev') or _TEAM_ID_TO_ABBREV.get(game.get('away_team_id'), '???')

    # Derive score and inning from at_bats
    score_row = db.execute(text("""
        SELECT home_score, away_score, inning, half_inning as inning_state
        FROM mlb.at_bats WHERE game_pk = :gp
        ORDER BY at_bat_index DESC LIMIT 1
    """), {"gp": game_pk}).mappings().first()
    if score_row:
        game.update({
            'home_score': score_row['home_score'],
            'away_score': score_row['away_score'],
            'inning': score_row['inning'],
            'inning_state': score_row['inning_state'],
        })
    else:
        game.update({'home_score': 0, 'away_score': 0, 'inning': None, 'inning_state': None})

    abs_sql = text("""
        SELECT
            ab.at_bat_index,
            ab.pitcher_id, ab.pitcher_name, ab.pitch_hand,
            ab.batter_id, ab.batter_name, ab.bat_side,
            ab.inning, ab.half_inning,
            ab.outs, ab.event, ab.description,
            ab.home_score, ab.away_score,
            ab.balls, ab.strikes,
            ab.on_first, ab.on_second, ab.on_third,
            JSON_AGG(
                JSON_BUILD_OBJECT(
                    'pitch_number', p.pitch_number,
                    'pitch_type', p.pitch_type_code,
                    'velo', ROUND(p.start_speed::numeric, 1),
                    'call_code', p.call_code,
                    'px', ROUND(p.px::numeric, 3),
                    'pz', ROUND(p.pz::numeric, 3),
                    'zone', p.zone,
                    'x0', p.x0, 'y0', p.y0, 'z0', p.z0,
                    'vx0', p.vx0, 'vy0', p.vy0, 'vz0', p.vz0,
                    'ax', p.ax, 'ay', p.ay, 'az', p.az,
                    'spin_rate', p.spin_rate,
                    'pfx_x', p.pfx_x, 'pfx_z', p.pfx_z,
                    'balls_before', p.balls_before,
                    'strikes_before', p.strikes_before
                ) ORDER BY p.pitch_number
            ) FILTER (WHERE p.pitch_number IS NOT NULL) as pitches
        FROM mlb.at_bats ab
        LEFT JOIN mlb.pitches p ON p.game_pk = ab.game_pk
            AND p.at_bat_index = ab.at_bat_index
        WHERE ab.game_pk = :gp
        GROUP BY ab.at_bat_index, ab.pitcher_id, ab.pitcher_name,
                 ab.pitch_hand, ab.batter_id, ab.batter_name,
                 ab.bat_side, ab.inning, ab.half_inning,
                 ab.outs, ab.event, ab.description,
                 ab.home_score, ab.away_score, ab.balls, ab.strikes,
                 ab.on_first, ab.on_second, ab.on_third
        ORDER BY ab.at_bat_index
    """)
    at_bats_rows = db.execute(abs_sql, {"gp": game_pk}).mappings().all()
    at_bats = [dict(ab) for ab in at_bats_rows]

    current_ab = at_bats[-1] if at_bats else None
    current_pitcher_id = current_ab['pitcher_id'] if current_ab else None
    current_batter_id = current_ab['batter_id'] if current_ab else None

    # Overlay live runner/count state from GUMBO (most recent play)
    if current_ab:
        try:
            gumbo_sql = text("""
                SELECT
                    (play -> 'matchup' -> 'postOnFirst') IS NOT NULL
                        AND (play -> 'matchup' -> 'postOnFirst') != 'null'::jsonb as on_first,
                    (play -> 'matchup' -> 'postOnSecond') IS NOT NULL
                        AND (play -> 'matchup' -> 'postOnSecond') != 'null'::jsonb as on_second,
                    (play -> 'matchup' -> 'postOnThird') IS NOT NULL
                        AND (play -> 'matchup' -> 'postOnThird') != 'null'::jsonb as on_third,
                    CAST(play -> 'count' ->> 'outs' AS int) as outs,
                    CAST(play -> 'count' ->> 'balls' AS int) as balls,
                    CAST(play -> 'count' ->> 'strikes' AS int) as strikes
                FROM mlb.raw_events r,
                jsonb_array_elements(r.data -> 'liveData' -> 'plays' -> 'allPlays') as play
                WHERE r.game_pk = :gp
                ORDER BY CAST(play -> 'about' ->> 'atBatIndex' AS int) DESC
                LIMIT 1
            """)
            live = db.execute(gumbo_sql, {"gp": game_pk}).mappings().first()
            if live:
                current_ab['on_first'] = bool(live['on_first'])
                current_ab['on_second'] = bool(live['on_second'])
                current_ab['on_third'] = bool(live['on_third'])
                current_ab['outs'] = int(live['outs'] or 0)
                current_ab['balls'] = int(live['balls'] or 0)
                current_ab['strikes'] = int(live['strikes'] or 0)
                # Also update the last at_bat entry in the list
                if at_bats:
                    at_bats[-1].update(current_ab)
        except Exception as _e:
            pass  # Non-fatal; fall back to at_bats data

    pitcher_today = None
    if current_pitcher_id:
        pt_sql = text("""
            SELECT
                COUNT(DISTINCT ab.at_bat_index) as batters_faced,
                SUM(CASE WHEN p.call_code IS NOT NULL THEN 1 ELSE 0 END) as total_pitches,
                SUM(CASE WHEN p.call_code IN ('C','S','W','T','F','X') THEN 1 ELSE 0 END) as strikes,
                SUM(CASE WHEN p.call_code = 'B' THEN 1 ELSE 0 END) as balls,
                SUM(CASE WHEN ab.event IN ('Strikeout','Strikeout - DP') THEN 1 ELSE 0 END) as k,
                SUM(CASE WHEN ab.event IN ('Walk','Intent Walk') THEN 1 ELSE 0 END) as bb,
                SUM(CASE WHEN ab.event = 'Home Run' THEN 1 ELSE 0 END) as hr,
                MAX(ab.inning) as last_inning,
                MAX(ab.inning) * 3 as outs_recorded
            FROM mlb.at_bats ab
            LEFT JOIN mlb.pitches p ON p.game_pk = ab.game_pk
                AND p.at_bat_index = ab.at_bat_index
            WHERE ab.game_pk = :gp AND ab.pitcher_id = :pid
        """)
        pitcher_today = dict(db.execute(pt_sql, {"gp": game_pk, "pid": current_pitcher_id}).mappings().first() or {})

    batter_today = None
    if current_batter_id:
        bt_sql = text("""
            SELECT
                COUNT(*) as pa,
                SUM(CASE WHEN event IN ('Single','Double','Triple','Home Run') THEN 1 ELSE 0 END) as hits,
                SUM(CASE WHEN event = 'Double' THEN 1 ELSE 0 END) as doubles,
                SUM(CASE WHEN event = 'Triple' THEN 1 ELSE 0 END) as triples,
                SUM(CASE WHEN event = 'Home Run' THEN 1 ELSE 0 END) as hr,
                SUM(CASE WHEN event IN ('Walk','Intent Walk') THEN 1 ELSE 0 END) as bb,
                SUM(CASE WHEN event IN ('Strikeout','Strikeout - DP') THEN 1 ELSE 0 END) as k,
                SUM(CASE WHEN event IN ('Single','Double','Triple','Home Run',
                    'Groundout','Flyout','Lineout','Pop Out','Forceout',
                    'Grounded Into DP','Double Play','Fielders Choice')
                    THEN 1 ELSE 0 END) as ab
            FROM mlb.at_bats
            WHERE game_pk = :gp AND batter_id = :bid
        """)
        batter_today = dict(db.execute(bt_sql, {"gp": game_pk, "bid": current_batter_id}).mappings().first() or {})

    pitch_mix = []
    if current_pitcher_id:
        mix_sql = text("""
            WITH today AS (
                SELECT p.pitch_type_code,
                    COUNT(*) as total,
                    ROUND(CAST(AVG(p.start_speed) AS numeric),1) as velo,
                    ROUND(CAST(AVG(p.pfx_z) * 12 AS numeric),1) as ivb,
                    ROUND(CAST(AVG(p.pfx_x) * 12 AS numeric),1) as hbreak,
                    ROUND(CAST(AVG(CASE WHEN p.call_code IN ('S','W','T')
                        THEN 1.0 ELSE 0.0 END)*100 AS numeric),1) as whiff_pct,
                    SUM(CASE WHEN p.call_code IN ('C','S','W','T','F','X')
                        THEN 1 ELSE 0 END) as strikes,
                    SUM(CASE WHEN p.call_code = 'B' THEN 1 ELSE 0 END) as balls
                FROM mlb.pitches p
                JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
                    AND ab.at_bat_index = p.at_bat_index
                WHERE p.game_pk = :gp AND ab.pitcher_id = :pid
                AND p.pitch_type_code IS NOT NULL
                AND p.pfx_z IS NOT NULL AND p.pfx_x IS NOT NULL
                GROUP BY p.pitch_type_code
            ),
            season AS (
                SELECT pqs.pitch_type_code,
                    SUM(pqs.pitches_thrown) as total,
                    ROUND(CAST(AVG(pqs.avg_velo) AS numeric),1) as velo,
                    ROUND(CAST(AVG(pqs.avg_ivb) AS numeric),1) as ivb,
                    ROUND(CAST(AVG(pqs.avg_hmov) AS numeric),1) as hbreak,
                    ROUND(CAST(AVG(pqs.whiff_rate)*100 AS numeric),1) as whiff_pct,
                    MAX(gs.goose_plus) as goose_plus
                FROM mlb.pitch_quality_scores pqs
                LEFT JOIN mlb.goose_scores gs ON gs.pitcher_id = pqs.pitcher_id
                    AND gs.pitch_type_code = pqs.pitch_type_code
                    AND gs.season = 2026
                WHERE pqs.pitcher_id = :pid
                AND pqs.season = 2026 AND pqs.game_type = 'R'
                GROUP BY pqs.pitch_type_code
            )
            SELECT
                t.pitch_type_code,
                t.total as today_total,
                ROUND(CAST(t.total AS numeric) /
                    NULLIF(SUM(t.total) OVER(), 0) * 100, 0) as today_usage_pct,
                t.velo as today_velo,
                t.ivb as today_ivb,
                t.hbreak as today_hbreak,
                t.whiff_pct as today_whiff,
                t.strikes as today_strikes,
                t.balls as today_balls,
                s.total as season_total,
                ROUND(CAST(s.total AS numeric) /
                    NULLIF(SUM(s.total) OVER(), 0) * 100, 0) as season_usage_pct,
                s.velo as season_velo,
                s.ivb as season_ivb,
                s.hbreak as season_hbreak,
                s.whiff_pct as season_whiff,
                s.goose_plus
            FROM today t
            LEFT JOIN season s ON s.pitch_type_code = t.pitch_type_code
            ORDER BY t.total DESC
        """)
        pitch_mix = [dict(r) for r in db.execute(mix_sql, {"gp": game_pk, "pid": current_pitcher_id}).mappings().all()]

    current_half = current_ab['half_inning'] if current_ab else 'top'
    recent_batters = []
    if current_ab:
        rb_sql = text("""
            SELECT DISTINCT ON (batter_id) batter_id, batter_name, bat_side, at_bat_index
            FROM mlb.at_bats
            WHERE game_pk = :gp AND half_inning = :half
            ORDER BY batter_id, at_bat_index DESC
        """)
        recent_batters = [dict(r) for r in db.execute(rb_sql, {"gp": game_pk, "half": current_half}).mappings().all()]
        recent_batters.sort(key=lambda r: r['at_bat_index'], reverse=True)
        recent_batters = recent_batters[:5]

    goose3 = None
    if current_pitcher_id:
        g3_sql = text("""
            SELECT ROUND(goose3_plus::numeric,1) as goose3_plus,
                   ROUND(avg_stuff_score::numeric,1) as stuff_score
            FROM mlb.goose3_overall WHERE pitcher_id = :pid AND season = 2026
        """)
        row = db.execute(g3_sql, {"pid": current_pitcher_id}).mappings().first()
        goose3 = dict(row) if row else None

    juiced = None
    if current_batter_id:
        j_sql = text("""
            SELECT ROUND((SUM(juiced_plus * pa) / NULLIF(SUM(pa), 0))::numeric, 1) as juiced_plus
            FROM mlb.juiced_scores WHERE batter_id = :bid AND season = 2026 AND pa >= 3
        """)
        row = db.execute(j_sql, {"bid": current_batter_id}).mappings().first()
        juiced = dict(row) if row else None

    weather = None
    w_row = db.execute(text("""
        SELECT temperature_f, wind_speed_mph, wind_direction_deg,
               weather_condition, combined_hr_modifier, venue_name
        FROM mlb.game_weather WHERE game_pk = :gp
    """), {"gp": game_pk}).mappings().first()
    if w_row:
        weather = dict(w_row)

    return {
        "game_pk": game_pk,
        "game": game,
        "at_bats": at_bats,
        "current_ab": current_ab,
        "pitcher_today": pitcher_today,
        "batter_today": batter_today,
        "pitch_mix_today": pitch_mix,
        "goose3": goose3,
        "juiced": juiced,
        "weather": weather,
        "recent_batters": recent_batters,
    }


@router.get("/game/{game_pk}/batter-splits/{batter_id}")
def game_batter_splits(game_pk: int, batter_id: int,
                        pitcher_id: int = None,
                        outs: int = None,
                        on_first: bool = False,
                        on_second: bool = False,
                        on_third: bool = False,
                        db: Session = Depends(get_db)):
    """Contextual splits for batter in current game state."""
    from sqlalchemy import text

    pitch_hand = 'R'
    if pitcher_id:
        row = db.execute(text("""
            SELECT pitch_hand FROM mlb.at_bats
            WHERE pitcher_id = :pid AND pitch_hand IS NOT NULL LIMIT 1
        """), {"pid": pitcher_id}).scalar()
        if row:
            pitch_hand = row

    mix = []
    if pitcher_id:
        mix_rows = db.execute(text("""
            SELECT pitch_type_code,
                   ROUND(AVG(start_speed)::numeric,1) as avg_velo,
                   COUNT(*) as pitches
            FROM mlb.pitches p
            JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk AND ab.at_bat_index = p.at_bat_index
            JOIN mlb.games g ON g.game_pk = p.game_pk
            WHERE ab.pitcher_id = :pid AND g.season = 2026 AND g.game_type = 'R'
            AND p.pitch_type_code IS NOT NULL
            GROUP BY p.pitch_type_code
            HAVING COUNT(*) >= 20
            ORDER BY pitches DESC LIMIT 5
        """), {"pid": pitcher_id}).mappings().all()
        mix = list(mix_rows)

    ops_vs_hand = db.execute(text("""
        SELECT
            ROUND(CAST(((AVG(CASE WHEN ab.event IN ('Single','Double','Triple','Home Run',
                    'Walk','Intent Walk','Hit By Pitch')
                    THEN 1.0 ELSE 0.0 END) / 0.320) +
                   (AVG(CASE ab.event WHEN 'Single' THEN 1.0 WHEN 'Double' THEN 2.0
                    WHEN 'Triple' THEN 3.0 WHEN 'Home Run' THEN 4.0
                    ELSE 0.0 END) / 0.415) - 1) * 100 AS numeric), 0) as avg_index,
            ROUND(CAST(AVG(CASE WHEN ab.event IN ('Single','Double','Triple','Home Run',
                'Walk','Intent Walk','Hit By Pitch')
                THEN 1.0 ELSE 0.0 END) AS numeric), 3) as obp,
            ROUND(CAST(AVG(CASE ab.event WHEN 'Single' THEN 1.0 WHEN 'Double' THEN 2.0
                WHEN 'Triple' THEN 3.0 WHEN 'Home Run' THEN 4.0
                ELSE 0.0 END) AS numeric), 3) as slg,
            COUNT(*) as pa
        FROM mlb.at_bats ab
        JOIN mlb.games g ON g.game_pk = ab.game_pk
        WHERE ab.batter_id = :bid AND ab.pitch_hand = :hand
        AND g.season = 2026 AND g.game_type = 'R'
        AND ab.event NOT IN ('Runner Out','Caught Stealing 2B',
            'Caught Stealing 3B','Wild Pitch','Passed Ball')
    """), {"bid": batter_id, "hand": pitch_hand}).mappings().first()

    ops_by_pitch = {}
    for pt_row in mix:
        pt = pt_row['pitch_type_code']
        result = db.execute(text("""
            SELECT
                ROUND(AVG(CASE WHEN ab.event = 'Home Run' THEN 4.0
                    WHEN ab.event = 'Triple' THEN 3.0
                    WHEN ab.event = 'Double' THEN 2.0
                    WHEN ab.event = 'Single' THEN 1.0
                    ELSE 0.0 END) * 100, 0) as slg_idx,
                COUNT(DISTINCT ab.at_bat_index) as pa
            FROM mlb.pitches p
            JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk AND ab.at_bat_index = p.at_bat_index
            JOIN mlb.games g ON g.game_pk = p.game_pk
            WHERE ab.batter_id = :bid AND p.pitch_type_code = :pt
            AND g.season = 2026 AND g.game_type = 'R'
            AND p.pitch_number = (
                SELECT MAX(p2.pitch_number) FROM mlb.pitches p2
                WHERE p2.game_pk = p.game_pk AND p2.at_bat_index = p.at_bat_index
            )
        """), {"bid": batter_id, "pt": pt}).mappings().first()
        if result and (result['pa'] or 0) >= 5:
            ops_by_pitch[pt] = {"slg_idx": int(result['slg_idx'] or 100), "pa": int(result['pa'])}

    juiced_by_pitch_rows = db.execute(text("""
        SELECT pitch_type_code,
               ROUND(AVG(juiced_plus)::numeric,1) as juiced_plus,
               SUM(pa) as total_pa
        FROM mlb.juiced_scores
        WHERE batter_id = :bid AND season = 2026 AND pa >= 3
        GROUP BY pitch_type_code
    """), {"bid": batter_id}).mappings().all()
    juiced_by_pitch = {r['pitch_type_code']: {
        "juiced_plus": float(r['juiced_plus'] or 100),
        "pa": int(r['total_pa'])
    } for r in juiced_by_pitch_rows}

    risp = db.execute(text("""
        SELECT
            ROUND(AVG(CASE WHEN event IN ('Single','Double','Triple','Home Run')
                THEN 1.0 ELSE 0.0 END) * 100, 1) as hit_rate,
            ROUND(AVG(CASE WHEN event IN ('Strikeout','Strikeout - DP')
                THEN 1.0 ELSE 0.0 END) * 100, 1) as k_rate,
            COUNT(*) as pa
        FROM mlb.at_bats ab
        JOIN mlb.games g ON g.game_pk = ab.game_pk
        WHERE ab.batter_id = :bid AND g.season = 2026 AND g.game_type = 'R'
        AND ab.runners_reconstructed = TRUE AND (ab.on_second OR ab.on_third)
        AND ab.event NOT IN ('Runner Out','Caught Stealing 2B','Wild Pitch')
    """), {"bid": batter_id}).mappings().first()

    PNAMES = {
        'FF':'Four-Seam FB','SI':'Sinker','SL':'Slider','CH':'Changeup',
        'CU':'Curveball','FC':'Cutter','ST':'Sweeper','FS':'Splitter','KC':'Knuckle-Curve'
    }
    mix_codes = {r['pitch_type_code'] for r in mix}

    ops_splits = []
    if ops_vs_hand and ops_vs_hand['pa']:
        ops_splits.append({
            "label": f"Vs {'LHP' if pitch_hand == 'L' else 'RHP'}",
            "value": int(ops_vs_hand['avg_index'] or 100),
            "pa": int(ops_vs_hand['pa']),
            "obp": float(ops_vs_hand.get('obp') or 0),
            "slg": float(ops_vs_hand.get('slg') or 0),
            "ops": float((ops_vs_hand.get('obp') or 0) +
                         (ops_vs_hand.get('slg') or 0)),
        })
    for pt, data in ops_by_pitch.items():
        ops_splits.append({
            "label": f"Vs {PNAMES.get(pt, pt)}",
            "pitch_type": pt,
            "value": data['slg_idx'],
            "pa": data['pa']
        })
    if risp and (risp['pa'] or 0) >= 10:
        ops_splits.append({
            "label": "RISP",
            "value": int(float(risp["hit_rate"] or 0) / 0.26),
            "pa": int(risp['pa'])
        })

    juiced_splits = []
    for pt, data in juiced_by_pitch.items():
        if pt in mix_codes:
            juiced_splits.append({
                "label": f"Vs {PNAMES.get(pt, pt)}",
                "pitch_type": pt,
                "value": data['juiced_plus'],
                "pa": data['pa']
            })

    game_state_split = None
    if outs is not None:
        is_risp = on_second or on_third
        has_runners = on_first or on_second or on_third
        state_label = "RISP" if is_risp else ("Runners On" if has_runners else "Bases Empty")
        if is_risp:
            state_filter = "AND (ab.on_second OR ab.on_third)"
        elif has_runners:
            state_filter = "AND (ab.on_first OR ab.on_second OR ab.on_third)"
        else:
            state_filter = "AND NOT ab.on_first AND NOT ab.on_second AND NOT ab.on_third"

        gs_sql = text(f"""
            SELECT
                COUNT(*) as pa,
                ROUND(AVG(CASE WHEN event IN ('Single','Double','Triple','Home Run')
                    THEN 1.0 ELSE 0.0 END)::numeric, 3) as avg,
                ROUND(AVG(CASE WHEN event IN ('Single','Double','Triple','Home Run',
                    'Walk','Intent Walk') THEN 1.0 ELSE 0.0 END)::numeric, 3) as obp,
                ROUND(AVG(CASE ab.event WHEN 'Single' THEN 1.0 WHEN 'Double' THEN 2.0
                    WHEN 'Triple' THEN 3.0 WHEN 'Home Run' THEN 4.0
                    ELSE 0.0 END)::numeric, 3) as slg,
                ROUND(AVG(CASE WHEN event IN ('Strikeout','Strikeout - DP')
                    THEN 1.0 ELSE 0.0 END)::numeric, 3) as k_rate,
                ROUND(AVG(CASE WHEN event = 'Home Run'
                    THEN 1.0 ELSE 0.0 END)::numeric, 3) as hr_rate
            FROM mlb.at_bats ab
            JOIN mlb.games g ON g.game_pk = ab.game_pk
            WHERE ab.batter_id = :bid
            AND g.season = 2026 AND g.game_type = 'R'
            AND ab.runners_reconstructed = TRUE
            AND ab.outs_before = :outs
            {state_filter}
            AND ab.event NOT IN ('Runner Out','Caught Stealing 2B',
                'Caught Stealing 3B','Wild Pitch','Passed Ball')
        """)
        gs_row = db.execute(gs_sql, {"bid": batter_id, "outs": outs}).mappings().first()
        if gs_row and int(gs_row['pa'] or 0) >= 5:
            game_state_split = {
                "label": f"{state_label}, {outs} out{'s' if outs != 1 else ''}",
                "pa": int(gs_row['pa']),
                "avg": float(gs_row['avg'] or 0),
                "obp": float(gs_row['obp'] or 0),
                "slg": float(gs_row['slg'] or 0),
                "ops": float((gs_row['obp'] or 0) + (gs_row['slg'] or 0)),
                "k_rate": float(gs_row['k_rate'] or 0),
                "hr_rate": float(gs_row['hr_rate'] or 0),
            }

    return {
        "batter_id": batter_id,
        "pitch_hand": pitch_hand,
        "ops_splits": ops_splits[:6],
        "juiced_splits": juiced_splits[:6],
        "risp_pa": int(risp['pa']) if risp else 0,
        "game_state_split": game_state_split,
    }


@router.get("/lineup-batter-stats")
def lineup_batter_stats(ids: str, season: int = 2026, db: Session = Depends(get_db)):
    from sqlalchemy import text
    batter_ids = [int(i) for i in ids.split(',') if i.strip().isdigit()]
    if not batter_ids:
        return {}
    rows = db.execute(text("""
        SELECT
            bb.player_id,
            ROUND(SUM(bb.hits)::numeric / NULLIF(SUM(bb.at_bats), 0), 3) as avg,
            ROUND(
                (SUM(bb.hits) + SUM(bb.walks))::numeric /
                NULLIF(SUM(bb.at_bats) + SUM(bb.walks), 0), 3) as obp,
            ROUND(SUM(bb.total_bases)::numeric / NULLIF(SUM(bb.at_bats), 0), 3) as slg,
            ROUND(
                (SUM(bb.hits) + SUM(bb.walks))::numeric /
                NULLIF(SUM(bb.at_bats) + SUM(bb.walks), 0) +
                SUM(bb.total_bases)::numeric / NULLIF(SUM(bb.at_bats), 0), 3) as ops,
            SUM(bb.home_runs) as hr,
            SUM(bb.strikeouts) as k,
            SUM(bb.walks) as bb,
            ROUND(j2.juiced2_plus::numeric, 1) as juiced2_plus
        FROM mlb.boxscore_batting bb
        JOIN mlb.games g ON g.game_pk = bb.game_pk
        LEFT JOIN mlb.juiced2_scores j2
            ON j2.batter_id = bb.player_id AND j2.season = :season
        WHERE bb.player_id = ANY(:ids)
          AND g.season = :season AND g.game_type = 'R'
        GROUP BY bb.player_id, j2.juiced2_plus
    """), {"ids": batter_ids, "season": season}).mappings().all()
    return {r['player_id']: dict(r) for r in rows}


@router.get("/lineup-pitcher-stats")
def lineup_pitcher_stats(ids: str, season: int = 2026, db: Session = Depends(get_db)):
    from sqlalchemy import text
    pitcher_ids = [int(i) for i in ids.split(',') if i.strip().isdigit()]
    if not pitcher_ids:
        return {}
    rows = db.execute(text("""
        SELECT
            bp.player_id,
            ROUND(SUM(bp.innings_pitched)::numeric, 1) as ip,
            SUM(bp.strikeouts) as k,
            SUM(bp.walks) as bb,
            SUM(bp.hits) as hits_allowed,
            SUM(bp.earned_runs) as er,
            ROUND(
                SUM(bp.earned_runs)::numeric * 9 /
                NULLIF(SUM(bp.innings_pitched)::numeric, 0), 2) as era,
            ROUND(g3.goose3_plus::numeric, 1) as goose3_plus
        FROM mlb.boxscore_pitching bp
        JOIN mlb.games g ON g.game_pk = bp.game_pk
        LEFT JOIN mlb.goose3_overall g3
            ON g3.pitcher_id = bp.player_id AND g3.season = :season
        WHERE bp.player_id = ANY(:ids)
          AND g.season = :season AND g.game_type = 'R'
        GROUP BY bp.player_id, g3.goose3_plus
    """), {"ids": pitcher_ids, "season": season}).mappings().all()
    return {r['player_id']: dict(r) for r in rows}


# ── CONTEXT SPLITS & WORKLOAD ────────────────────────────────────────────────

@router.get("/pitcher/{pitcher_id}/context-splits")
def pitcher_context_splits(pitcher_id: int, season: int = 2026,
                            db: Session = Depends(get_db)):
    from sqlalchemy import text
    rows = db.execute(text("""
        SELECT split_type, pa, k_pct, bb_pct, hr_pct,
               avg_against, obp_against, slg_against, ops_against
        FROM mlb.pitcher_splits
        WHERE pitcher_id = :pid AND season = :season
        ORDER BY split_type
    """), {"pid": pitcher_id, "season": season}).mappings().all()
    if not rows:
        raise HTTPException(status_code=404, detail="No splits found")
    return {r['split_type']: {k: (float(v) if v is not None else None)
                               for k, v in r.items() if k != 'split_type'}
            for r in rows}


@router.get("/batter/{batter_id}/context-splits")
def batter_context_splits(batter_id: int, season: int = 2026,
                           db: Session = Depends(get_db)):
    from sqlalchemy import text
    rows = db.execute(text("""
        SELECT split_type, pa, k_pct, bb_pct, hr_pct,
               avg_for, obp_for, slg_for, ops_for
        FROM mlb.batter_splits
        WHERE batter_id = :bid AND season = :season
        ORDER BY split_type
    """), {"bid": batter_id, "season": season}).mappings().all()
    if not rows:
        raise HTTPException(status_code=404, detail="No splits found")
    return {r['split_type']: {k: (float(v) if v is not None else None)
                               for k, v in r.items() if k != 'split_type'}
            for r in rows}


@router.get("/player/{player_id}/workload")
def player_workload(player_id: int, player_type: str = "pitcher",
                    db: Session = Depends(get_db)):
    from sqlalchemy import text
    rows = db.execute(text("""
        SELECT calc_date, player_type, load_7d, load_28d, acwr,
               games_7d, games_28d, total_load_7d
        FROM mlb.player_workload
        WHERE player_id = :pid AND player_type = :ptype
        ORDER BY calc_date DESC
        LIMIT 28
    """), {"pid": player_id, "ptype": player_type}).mappings().all()
    if not rows:
        raise HTTPException(status_code=404, detail="No workload data found")

    latest = dict(rows[0])
    acwr = float(latest.get('acwr') or 0)
    acwr_flag = 'red' if acwr > 1.5 else ('yellow' if acwr > 1.2 else 'green')

    return {
        'player_id': player_id,
        'player_type': player_type,
        'acwr_flag': acwr_flag,
        'latest': {k: (float(v) if hasattr(v, '__float__') else str(v) if v is not None else None)
                   for k, v in latest.items()},
        'history': [{k: (float(v) if hasattr(v, '__float__') else str(v) if v is not None else None)
                     for k, v in dict(r).items()} for r in rows],
    }


@router.get("/matchup/{pitcher_id}/vs/{batter_id}")
def matchup_h2h(pitcher_id: int, batter_id: int, db: Session = Depends(get_db)):
    from sqlalchemy import text
    # Career aggregate across all stored seasons
    agg = db.execute(text("""
        SELECT
            SUM(pa) AS pa, SUM(k) AS k, SUM(bb) AS bb,
            SUM(hr) AS hr, SUM(hits) AS hits,
            SUM(total_bases) AS total_bases,
            ROUND(SUM(k)::NUMERIC  / NULLIF(SUM(pa), 0) * 100, 1) AS k_pct,
            ROUND(SUM(hr)::NUMERIC / NULLIF(SUM(pa), 0) * 100, 1) AS hr_pct,
            ROUND(SUM(hits)::NUMERIC / NULLIF(SUM(pa) - SUM(bb), 0), 3) AS avg,
            ROUND(SUM(total_bases)::NUMERIC / NULLIF(SUM(pa) - SUM(bb), 0), 3) AS slg,
            ROUND(
                (SUM(k)::NUMERIC / NULLIF(SUM(pa), 0) * 100)
                * (SUM(pa)::NUMERIC / (SUM(pa) + 20))
                + 22.0 * (20.0 / (SUM(pa) + 20)), 2) AS k_pct_reg,
            ROUND(
                (SUM(hr)::NUMERIC / NULLIF(SUM(pa), 0) * 100)
                * (SUM(pa)::NUMERIC / (SUM(pa) + 20))
                + 3.5 * (20.0 / (SUM(pa) + 20)), 2) AS hr_pct_reg
        FROM mlb.matchup_history
        WHERE pitcher_id = :pid AND batter_id = :bid
    """), {"pid": pitcher_id, "bid": batter_id}).mappings().first()

    # Per-season breakdown
    seasons = db.execute(text("""
        SELECT season, pa, k, bb, hr, hits, total_bases,
               k_pct_raw, hr_pct_raw, k_pct_reg, hr_pct_reg
        FROM mlb.matchup_history
        WHERE pitcher_id = :pid AND batter_id = :bid
        ORDER BY season DESC
    """), {"pid": pitcher_id, "bid": batter_id}).mappings().all()

    def _fmt(row):
        return {k: (float(v) if v is not None else None) for k, v in dict(row).items()}

    return {
        'pitcher_id': pitcher_id,
        'batter_id': batter_id,
        'career': _fmt(agg) if agg and agg['pa'] else None,
        'by_season': [_fmt(r) for r in seasons],
    }


# ── MODEL SCORES ──────────────────────────────────────────────────────────────

@router.get("/model-scores")
def get_model_scores(
    days: int = Query(30, ge=1, le=365),
    db: Session = Depends(get_db),
):
    """
    Returns daily model accuracy scores for the last N days plus rolling
    7-day and season-to-date averages for each metric.
    """
    from sqlalchemy import text
    from datetime import date, timedelta

    today = date.today()
    start = today - timedelta(days=days)
    season_start = date(today.year, 3, 1)

    rows = db.execute(text("""
        SELECT
            ms.score_date,
            ms.player_type,
            ms.metric,
            ms.n_players,
            ms.mae,
            ms.rmse,
            ms.mean_proj,
            ms.mean_actual,
            ms.bias,
            ms.within_1,
            ms.within_2,
            -- 7-day rolling avg for this metric
            ROUND(AVG(ms.mae) OVER (
                PARTITION BY ms.player_type, ms.metric
                ORDER BY ms.score_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            )::numeric, 4) AS mae_7d,
            ROUND(AVG(ms.bias) OVER (
                PARTITION BY ms.player_type, ms.metric
                ORDER BY ms.score_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            )::numeric, 4) AS bias_7d
        FROM mlb.model_scores ms
        WHERE ms.score_date >= :start
        ORDER BY ms.score_date DESC, ms.player_type, ms.metric
    """), {"start": start}).mappings().all()

    # Season averages (separate query)
    season_avgs = db.execute(text("""
        SELECT
            player_type,
            metric,
            COUNT(*) AS n_days,
            ROUND(AVG(mae)::numeric, 4)  AS mae_season,
            ROUND(AVG(bias)::numeric, 4) AS bias_season
        FROM mlb.model_scores
        WHERE score_date >= :s AND score_date <= :e
        GROUP BY player_type, metric
    """), {"s": season_start, "e": today}).mappings().all()

    season_map = {(r['player_type'], r['metric']): dict(r) for r in season_avgs}

    def _row(r):
        d = {k: (float(v) if v is not None else None) for k, v in dict(r).items()
             if k not in ('score_date', 'player_type', 'metric')}
        d['score_date'] = str(r['score_date'])
        d['player_type'] = r['player_type']
        d['metric'] = r['metric']
        sk = (r['player_type'], r['metric'])
        if sk in season_map:
            d['mae_season'] = season_map[sk]['mae_season']
            d['bias_season'] = season_map[sk]['bias_season']
            d['n_days_season'] = season_map[sk]['n_days']
        return d

    return {
        'days': days,
        'start_date': str(start),
        'rows': [_row(r) for r in rows],
        'season_averages': [
            {**{k: (float(v) if v is not None else None)
                for k, v in s.items() if k not in ('player_type', 'metric')},
             'player_type': s['player_type'],
             'metric': s['metric']}
            for s in season_avgs
        ],
    }


# ── PROP TRACKER ──────────────────────────────────────────────────────────────

def _poisson_over(lam: float, line: float) -> float:
    """P(X >= ceil(line)) — probability over prop hits."""
    if lam <= 0:
        return 0.03
    k = int(math.ceil(line))
    cumulative = 0.0
    log_lam = math.log(lam) if lam > 0 else 0
    log_term = -lam
    for i in range(k):
        cumulative += math.exp(log_term)
        if i < k - 1:
            log_term += log_lam - math.log(i + 1)
    return round(max(0.03, min(0.97, 1.0 - cumulative)), 3)


def _poisson_under(lam: float, line: float) -> float:
    """P(X <= floor(line)) — probability under prop hits."""
    if lam <= 0:
        return 0.97
    k = int(math.floor(line))
    cumulative = 0.0
    log_lam = math.log(lam) if lam > 0 else 0
    log_term = -lam
    for i in range(k + 1):
        cumulative += math.exp(log_term)
        if i < k:
            log_term += log_lam - math.log(i + 1)
    return round(max(0.03, min(0.97, cumulative)), 3)


@router.get("/props/players/search")
def props_player_search(q: str = "", db: Session = Depends(get_db)):
    """Search today's projected pitchers and batters for prop tracker."""
    from sqlalchemy import text
    from datetime import date
    today = str(date.today())
    pat = f"%{q.lower().strip()}%"

    # Fall back to most recent snapshot if today has no data yet
    snap = db.execute(text("""
        SELECT MAX(snapshot_date) FROM mlb.daily_projections
        WHERE snapshot_date <= :d
    """), {"d": today}).scalar()
    if not snap:
        return {"pitchers": [], "batters": []}
    snap = str(snap)

    pitchers = db.execute(text("""
        SELECT dp.pitcher_id    AS player_id,
               dp.pitcher_name  AS name,
               dp.game_pk,
               dp.proj_ks_6inn,
               dp.proj_ks_f5,
               dp.proj_er_f5,
               dp.proj_k_pct,
               'pitcher'        AS player_type
        FROM mlb.daily_projections dp
        WHERE dp.snapshot_date = :d
          AND LOWER(dp.pitcher_name) LIKE :q
        ORDER BY dp.pitcher_name
        LIMIT 15
    """), {"d": snap, "q": pat}).mappings().all()

    batters = db.execute(text("""
        SELECT dbp.batter_id       AS player_id,
               dbp.batter_name     AS name,
               dbp.game_pk,
               dbp.proj_tb,
               dbp.proj_hr_pct,
               dbp.proj_k_pct,
               dbp.opp_pitcher_name,
               'batter'            AS player_type
        FROM mlb.daily_batter_projections dbp
        WHERE dbp.snapshot_date = :d
          AND LOWER(dbp.batter_name) LIKE :q
        ORDER BY dbp.batter_name
        LIMIT 15
    """), {"d": snap, "q": pat}).mappings().all()

    return {
        "pitchers": [dict(p) for p in pitchers],
        "batters":  [dict(b) for b in batters],
    }


@router.get("/props/confidence")
def props_confidence(
    player_id: int,
    player_type: str,
    stat: str,
    line: float,
    direction: str,
    db: Session = Depends(get_db),
):
    """
    Return pre-game probability and projection for a prop.
    stat: ks | f5_ks | er | hr | tb | hits
    direction: over | under
    """
    from sqlalchemy import text
    from datetime import date
    today = str(date.today())

    snap = db.execute(text("""
        SELECT MAX(snapshot_date) FROM mlb.daily_projections WHERE snapshot_date <= :d
    """), {"d": today}).scalar()
    snap = str(snap) if snap else today

    prob = None
    proj_value = None
    label = ""

    if player_type == "pitcher":
        row = db.execute(text("""
            SELECT proj_ks_6inn, proj_ks_f5, proj_er_f5, proj_k_pct, proj_ops
            FROM mlb.daily_projections
            WHERE pitcher_id = :pid AND snapshot_date = :d
            LIMIT 1
        """), {"pid": player_id, "d": snap}).mappings().first()

        if not row:
            raise HTTPException(404, "No projection found for pitcher today")

        if stat == "ks":
            lam = float(row["proj_ks_6inn"] or 0)
            proj_value = lam
            label = f"Proj {lam:.1f} Ks"
        elif stat == "f5_ks":
            lam = float(row["proj_ks_f5"] or 0)
            proj_value = lam
            label = f"Proj {lam:.1f} F5 Ks"
        elif stat == "er":
            lam = float(row["proj_er_f5"] or 0)
            proj_value = lam
            label = f"Proj {lam:.1f} ER (F5)"
        else:
            raise HTTPException(400, f"Unknown pitcher stat: {stat}")

        prob = _poisson_over(lam, line) if direction == "over" else _poisson_under(lam, line)

    elif player_type == "batter":
        row = db.execute(text("""
            SELECT proj_tb, proj_hr_pct, proj_k_pct, proj_hit_pct
            FROM mlb.daily_batter_projections
            WHERE batter_id = :bid AND snapshot_date = :d
            ORDER BY simulations DESC NULLS LAST
            LIMIT 1
        """), {"bid": player_id, "d": snap}).mappings().first()

        if not row:
            raise HTTPException(404, "No projection found for batter today")

        if stat == "hr":
            p = float(row["proj_hr_pct"] or 0) / 100.0
            proj_value = round(p * 100, 1)
            label = f"Proj {proj_value:.1f}% HR chance"
            # Bernoulli: P(at least 1 HR) = p
            prob = round(max(0.03, min(0.97, p)), 3) if direction == "over" else round(max(0.03, min(0.97, 1 - p)), 3)
        elif stat == "tb":
            lam = float(row["proj_tb"] or 0)
            proj_value = lam
            label = f"Proj {lam:.2f} TB"
            prob = _poisson_over(lam, line) if direction == "over" else _poisson_under(lam, line)
        elif stat == "hits":
            hit_pct = float(row["proj_hit_pct"] or 0) / 100.0
            lam = hit_pct * 3.8  # ~3.8 AB per game
            proj_value = round(lam, 2)
            label = f"Proj {lam:.2f} hits"
            prob = _poisson_over(lam, line) if direction == "over" else _poisson_under(lam, line)
        else:
            raise HTTPException(400, f"Unknown batter stat: {stat}")
    else:
        raise HTTPException(400, f"Unknown player_type: {player_type}")

    return {
        "probability": prob,
        "proj_value": proj_value,
        "label": label,
        "stat": stat,
        "line": line,
        "direction": direction,
    }


@router.get("/props/live/{game_pk}")
async def props_live(game_pk: int, player_id: int, player_type: str, stat: str):
    """
    Fetch current stat value from the MLB live boxscore API.
    Returns current_value, game_state, inning, and score.
    """
    async with httpx.AsyncClient(timeout=10.0) as client:
        bs_resp, ls_resp = await asyncio.gather(
            client.get(f"https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore"),
            client.get(f"https://statsapi.mlb.com/api/v1/game/{game_pk}/linescore"),
        )

    boxscore  = bs_resp.json()  if bs_resp.status_code == 200 else {}
    linescore = ls_resp.json()  if ls_resp.status_code == 200 else {}

    # Game state from linescore
    inning       = linescore.get("currentInning")
    inning_half  = linescore.get("inningHalf", "")
    home_runs    = linescore.get("teams", {}).get("home", {}).get("runs")
    away_runs    = linescore.get("teams", {}).get("away", {}).get("runs")

    # Determine game state
    innings_data = linescore.get("innings", [])
    is_final = (
        inning is not None
        and inning >= 9
        and inning_half == "Bottom"
        and len(innings_data) >= 9
        and linescore.get("isTopInning") is False
    )
    # Better: check via boxscore info field
    bs_info = boxscore.get("info", [])
    for entry in bs_info:
        if entry.get("label") == "Status":
            if "Final" in (entry.get("value") or ""):
                is_final = True

    if inning is None:
        game_state = "Preview"
    elif is_final:
        game_state = "Final"
    else:
        game_state = "Live"

    # Find player stats in boxscore
    current_value = None
    ip_pitched = None
    player_key = f"ID{player_id}"

    for side in ("home", "away"):
        players = boxscore.get("teams", {}).get(side, {}).get("players", {})
        if player_key not in players:
            continue
        pdata = players[player_key]

        if player_type == "pitcher":
            pit = pdata.get("stats", {}).get("pitching", {})
            ip_str = pit.get("inningsPitched", "0")
            try:
                ip_pitched = float(ip_str)
            except (ValueError, TypeError):
                ip_pitched = 0.0
            if stat in ("ks", "f5_ks"):
                current_value = pit.get("strikeOuts", 0)
            elif stat == "er":
                current_value = pit.get("earnedRuns", 0)

        elif player_type == "batter":
            bat = pdata.get("stats", {}).get("batting", {})
            if stat == "hr":
                current_value = bat.get("homeRuns", 0)
            elif stat == "tb":
                current_value = bat.get("totalBases", 0)
            elif stat == "hits":
                current_value = bat.get("hits", 0)
        break

    return {
        "current_value": current_value,
        "game_state": game_state,
        "inning": inning,
        "inning_half": inning_half,
        "ip_pitched": ip_pitched,
        "home_runs": home_runs,
        "away_runs": away_runs,
    }
