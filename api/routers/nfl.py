from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from database import get_db
from datetime import date as dt
from zoneinfo import ZoneInfo

router = APIRouter(prefix="/nfl", tags=["NFL"])

PT = ZoneInfo("America/Los_Angeles")


@router.get("/matchups")
def nfl_matchups(date: str = None, db: Session = Depends(get_db)):
    """
    Returns games for a given date with team matchup stats.
    Rolling stats (EPA, run/pass splits, blitz proxy) use each team's last 3 games
    played strictly before the target date.
    """
    target = date or dt.today().isoformat()

    games = db.execute(text("""
        SELECT game_id, season, week, game_date, game_datetime,
               home_team, away_team, home_score, away_score,
               roof, surface, temp, wind, stadium, location,
               spread_line, total_line, home_win
        FROM nfl.games
        WHERE game_date = :d
          AND game_type IN ('REG', 'POST', 'WC', 'DIV', 'CON', 'SB')
        ORDER BY game_datetime ASC NULLS LAST
    """), {"d": target}).mappings().all()

    if not games:
        return []

    # Rolling last-3-games EPA/success-rate metrics per team (from nfl.team_metrics_weekly)
    team_stats = db.execute(text("""
        WITH ranked AS (
            SELECT tm.*,
                ROW_NUMBER() OVER (
                    PARTITION BY tm.team ORDER BY g.game_date DESC
                ) AS rn
            FROM nfl.team_metrics_weekly tm
            JOIN nfl.games g ON g.game_id = tm.game_id
            WHERE tm.season IN (2025, 2026)
              AND g.game_date < :d
        )
        SELECT
            team,
            ROUND(AVG(off_epa_per_play)::numeric, 3) AS off_epa,
            ROUND(AVG(off_epa_pass)::numeric, 3) AS off_epa_pass,
            ROUND(AVG(off_epa_rush)::numeric, 3) AS off_epa_rush,
            ROUND(AVG(off_success_rate)::numeric, 3) AS off_success_rate,
            ROUND(AVG(def_epa_per_play)::numeric, 3) AS def_epa,
            ROUND(AVG(def_epa_pass)::numeric, 3) AS def_epa_pass,
            ROUND(AVG(def_epa_rush)::numeric, 3) AS def_epa_rush,
            ROUND(AVG(def_success_rate)::numeric, 3) AS def_success_rate,
            ROUND(AVG(points_scored)::numeric, 1) AS avg_pts_scored,
            ROUND(AVG(points_allowed)::numeric, 1) AS avg_pts_allowed,
            COUNT(*) AS games_sample
        FROM ranked
        WHERE rn <= 3
        GROUP BY team
    """), {"d": target}).mappings().all()

    # Rolling last-3-games run/pass split + blitz proxy per team, straight from nfl.plays.
    # Ranked per-team-per-game (not a flat row LIMIT) so each team's own last 3 games
    # are used regardless of how many games other teams have played.
    play_splits = db.execute(text("""
        WITH game_off AS (
            SELECT p.posteam AS team, p.game_id, g.game_date,
                COUNT(*) AS total_plays,
                SUM(CASE WHEN p.play_type = 'run' THEN 1 ELSE 0 END) AS run_plays,
                SUM(CASE WHEN p.play_type = 'pass' THEN 1 ELSE 0 END) AS pass_plays,
                AVG(CASE WHEN p.play_type = 'run' THEN p.yards_gained END) AS rush_yds,
                AVG(CASE WHEN p.play_type = 'pass' AND p.complete_pass THEN p.yards_gained END) AS pass_yds,
                AVG(CASE WHEN p.play_type = 'pass' THEN p.air_yards END) AS air_yards
            FROM nfl.plays p
            JOIN nfl.games g ON g.game_id = p.game_id
            WHERE p.play_type IN ('run', 'pass')
              AND p.penalty IS NOT TRUE
              AND p.season IN (2025, 2026)
              AND g.game_date < :d
              AND p.posteam IS NOT NULL
            GROUP BY p.posteam, p.game_id, g.game_date
        ),
        game_def AS (
            SELECT p.defteam AS team, p.game_id, g.game_date,
                SUM(CASE WHEN p.play_type = 'pass' THEN 1 ELSE 0 END) AS def_pass_plays,
                AVG(CASE WHEN p.play_type = 'run' THEN p.yards_gained END) AS rush_yds_allowed,
                AVG(CASE WHEN p.play_type = 'pass' AND p.complete_pass THEN p.yards_gained END) AS pass_yds_allowed,
                -- Blitz proxy: share of pass plays with heavily negative EPA (pressure indicator) —
                -- no PFF/snap-count pressure data available, so this is an approximation.
                SUM(CASE WHEN p.play_type = 'pass' AND p.epa < -0.5 THEN 1 ELSE 0 END) AS blitz_plays
            FROM nfl.plays p
            JOIN nfl.games g ON g.game_id = p.game_id
            WHERE p.play_type IN ('run', 'pass')
              AND p.penalty IS NOT TRUE
              AND p.season IN (2025, 2026)
              AND g.game_date < :d
              AND p.defteam IS NOT NULL
            GROUP BY p.defteam, p.game_id, g.game_date
        ),
        off_ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY team ORDER BY game_date DESC) AS rn
            FROM game_off
        ),
        def_ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY team ORDER BY game_date DESC) AS rn
            FROM game_def
        ),
        off_agg AS (
            SELECT team,
                SUM(total_plays) AS total_plays,
                SUM(run_plays) AS run_plays,
                SUM(pass_plays) AS pass_plays,
                ROUND(AVG(rush_yds)::numeric, 1) AS avg_rush_yds,
                ROUND(AVG(pass_yds)::numeric, 1) AS avg_pass_yds,
                ROUND(AVG(air_yards)::numeric, 1) AS avg_air_yards
            FROM off_ranked WHERE rn <= 3
            GROUP BY team
        ),
        def_agg AS (
            SELECT team,
                SUM(def_pass_plays) AS def_pass_plays,
                SUM(blitz_plays) AS blitz_plays,
                ROUND(AVG(rush_yds_allowed)::numeric, 1) AS avg_rush_yds_allowed,
                ROUND(AVG(pass_yds_allowed)::numeric, 1) AS avg_pass_yds_allowed
            FROM def_ranked WHERE rn <= 3
            GROUP BY team
        )
        SELECT
            o.team,
            o.total_plays, o.run_plays, o.pass_plays,
            ROUND(o.run_plays::numeric / NULLIF(o.total_plays, 0) * 100, 1) AS run_pct,
            ROUND(o.pass_plays::numeric / NULLIF(o.total_plays, 0) * 100, 1) AS pass_pct,
            o.avg_rush_yds, o.avg_pass_yds, o.avg_air_yards,
            d.avg_rush_yds_allowed, d.avg_pass_yds_allowed,
            ROUND(d.blitz_plays::numeric / NULLIF(d.def_pass_plays, 0) * 100, 1) AS blitz_proxy_pct
        FROM off_agg o
        JOIN def_agg d ON d.team = o.team
    """), {"d": target}).mappings().all()

    stats = {r['team']: dict(r) for r in team_stats}
    splits = {r['team']: dict(r) for r in play_splits}

    result = []
    for g in games:
        g = dict(g)
        home, away = g['home_team'], g['away_team']

        game_time_pt = None
        if g.get('game_datetime'):
            game_time_pt = g['game_datetime'].astimezone(PT).strftime('%-I:%M %p PT')

        result.append({
            'game_id': g['game_id'],
            'game_date': str(g['game_date']),
            'game_time_pt': game_time_pt,
            'week': g['week'],
            'home_team': home,
            'away_team': away,
            'home_score': g['home_score'],
            'away_score': g['away_score'],
            'home_win': g['home_win'],
            'stadium': g['stadium'],
            'location': g['location'],
            'roof': g['roof'],
            'surface': g['surface'],
            'temp': g['temp'],
            'wind': g['wind'],
            'spread_line': g['spread_line'],
            'total_line': g['total_line'],
            'home_stats': {**stats.get(home, {}), **splits.get(home, {})},
            'away_stats': {**stats.get(away, {}), **splits.get(away, {})},
        })

    return result
