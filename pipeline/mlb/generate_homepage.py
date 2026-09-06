"""
Generate daily MiaCorp sports briefing as HTML.
Runs each morning after projections are ready.
Outputs to /app/static/homepage.html (served by FastAPI).

Sections:
1. Yesterday's Scores + Projection Results
2. Focus Team Box Scores (SD, SEA, CIN)
3. Pitcher Strikeout Projections
4. Today's Outlook
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import os
from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo
from database import SessionLocal
from sqlalchemy import text


FOCUS_TEAMS = {
    'SD':  {'name': 'San Diego Padres',   'team_id': 135},
    'SEA': {'name': 'Seattle Mariners',   'team_id': 136},
    'CIN': {'name': 'Cincinnati Reds',    'team_id': 113},
}

TEAM_ID_TO_ABBREV = {
    109:'ARI',144:'ATL',110:'BAL',111:'BOS',112:'CHC',
    145:'CWS',113:'CIN',114:'CLE',115:'COL',116:'DET',
    117:'HOU',118:'KC',108:'LAA',119:'LAD',146:'MIA',
    158:'MIL',142:'MIN',121:'NYM',147:'NYY',133:'OAK',
    143:'PHI',134:'PIT',135:'SD',137:'SF',136:'SEA',
    138:'STL',139:'TB',140:'TEX',141:'TOR',120:'WSH',
}


def get_yesterday_scores(yesterday: date, db) -> list:
    rows = db.execute(text("""
        SELECT game_pk, game_date, home_team_id, away_team_id,
               home_team_abbrev, away_team_abbrev,
               home_score, away_score, status
        FROM mlb.games
        WHERE game_date = :d AND game_type = 'R'
          AND home_score IS NOT NULL AND away_score IS NOT NULL
          AND home_score IS DISTINCT FROM away_score
        ORDER BY game_pk
    """), {"d": str(yesterday)}).mappings().all()
    return list(rows)


def get_focus_team_boxscore(game_pk: int, team_id: int, db) -> dict:
    game = db.execute(text("""
        SELECT home_team_id, away_team_id,
               home_team_abbrev, away_team_abbrev,
               home_score, away_score
        FROM mlb.games WHERE game_pk = :gp
    """), {"gp": game_pk}).mappings().first()
    if not game:
        return {}

    is_home = game['home_team_id'] == team_id
    team_abbrev = game['home_team_abbrev'] if is_home else game['away_team_abbrev']
    opp_abbrev  = game['away_team_abbrev'] if is_home else game['home_team_abbrev']
    team_score  = game['home_score'] if is_home else game['away_score']
    opp_score   = game['away_score'] if is_home else game['home_score']
    score = f"{team_abbrev} {team_score} - {opp_abbrev} {opp_score}"

    # Pitchers for this team
    pitchers = db.execute(text("""
        SELECT bp.player_id,
               MAX(names.pitcher_name) as name,
               bp.innings_pitched as ip,
               bp.hits, bp.earned_runs, bp.walks,
               bp.strikeouts as k,
               bp.pitches_thrown as pitches,
               bp.games_started as gs,
               bp.wins as win, bp.losses as loss
        FROM mlb.boxscore_pitching bp
        LEFT JOIN LATERAL (
            SELECT DISTINCT pitcher_name
            FROM mlb.at_bats
            WHERE game_pk = bp.game_pk AND pitcher_id = bp.player_id
            LIMIT 1
        ) names ON true
        WHERE bp.game_pk = :gp AND bp.team_id = :tid
        GROUP BY bp.player_id, bp.innings_pitched, bp.hits, bp.earned_runs,
                 bp.walks, bp.strikeouts, bp.pitches_thrown, bp.games_started,
                 bp.wins, bp.losses
        ORDER BY bp.games_started DESC, bp.innings_pitched DESC
    """), {"gp": game_pk, "tid": team_id}).mappings().all()

    starter_id = next((p['player_id'] for p in pitchers if p['gs']), None)

    # Pitch mix for starter — today vs season vs league
    pitch_mix = []
    pitch_hand = 'R'
    if starter_id:
        hand_row = db.execute(text("""
            SELECT MAX(pitch_hand) as pitch_hand FROM mlb.at_bats
            WHERE pitcher_id = :pid AND pitch_hand IS NOT NULL LIMIT 1
        """), {"pid": starter_id}).mappings().first()
        if hand_row and hand_row['pitch_hand']:
            pitch_hand = hand_row['pitch_hand']

        pitch_mix = db.execute(text("""
            WITH today AS (
                SELECT p.pitch_type_code,
                    COUNT(*) as total,
                    ROUND(AVG(p.start_speed)::numeric,1) as velo,
                    ROUND(AVG(p.pfx_z)::numeric,1) as ivb,
                    ROUND(AVG(p.pfx_x)::numeric,1) as hbreak,
                    ROUND((AVG(CASE WHEN p.call_code IN ('S','W','T')
                        THEN 1.0 ELSE 0.0 END)*100)::numeric,1) as whiff_pct,
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
                    ROUND(AVG(pqs.avg_velo)::numeric,1) as velo,
                    ROUND(AVG(pqs.avg_ivb)::numeric,1) as ivb,
                    ROUND(AVG(pqs.avg_hmov)::numeric,1) as hbreak,
                    ROUND((AVG(pqs.whiff_rate)*100)::numeric,1) as whiff_pct
                FROM mlb.pitch_quality_scores pqs
                WHERE pqs.pitcher_id = :pid AND pqs.season = 2026
                  AND pqs.game_type = 'R'
                GROUP BY pqs.pitch_type_code
            ),
            league_outcomes AS (
                SELECT pitch_type_code,
                    ROUND((AVG(whiff_pct) * 100)::numeric,1) as lg_whiff,
                    ROUND((AVG(csw_pct) * 100)::numeric,1) as lg_csw
                FROM mlb.league_pitch_baselines
                WHERE baseline_season = 2025 AND pitch_hand = :pitch_hand
                GROUP BY pitch_type_code
            ),
            league_physical AS (
                SELECT pitch_type_code,
                    ROUND(AVG(avg_velo)::numeric,1) as lg_velo,
                    ROUND(AVG(avg_ivb)::numeric,1) as lg_ivb,
                    ROUND(AVG(avg_hmov)::numeric,1) as lg_hmov
                FROM mlb.pitch_quality_scores
                WHERE season = 2026 AND game_type = 'R'
                GROUP BY pitch_type_code
            )
            SELECT t.pitch_type_code,
                t.total as today_n,
                ROUND(t.total::numeric/NULLIF(SUM(t.total) OVER(),0)*100,0) as today_pct,
                t.velo, t.ivb, t.hbreak, t.whiff_pct,
                t.strikes, t.balls,
                ROUND(s.total::numeric/NULLIF(SUM(s.total) OVER(),0)*100,0) as s_pct,
                s.velo as s_velo, s.ivb as s_ivb,
                s.hbreak as s_hbreak, s.whiff_pct as s_whiff,
                lo.lg_whiff, lo.lg_csw,
                lp.lg_velo, lp.lg_ivb, lp.lg_hmov
            FROM today t
            LEFT JOIN season s ON s.pitch_type_code = t.pitch_type_code
            LEFT JOIN league_outcomes lo ON lo.pitch_type_code = t.pitch_type_code
            LEFT JOIN league_physical lp ON lp.pitch_type_code = t.pitch_type_code
            ORDER BY t.total DESC
        """), {"gp": game_pk, "pid": starter_id, "pitch_hand": pitch_hand}).mappings().all()

    # Batters for focus team
    batters = db.execute(text("""
        SELECT bb.player_id,
               MAX(names.batter_name) as name,
               bb.at_bats as ab,
               bb.hits as h,
               bb.doubles as d,
               bb.triples as t,
               bb.home_runs as hr,
               bb.total_bases as tb,
               bb.walks as bb_count,
               bb.strikeouts as k,
               bb.rbi,
               bb.slg
        FROM mlb.boxscore_batting bb
        LEFT JOIN LATERAL (
            SELECT DISTINCT batter_name
            FROM mlb.at_bats
            WHERE game_pk = bb.game_pk AND batter_id = bb.player_id
            LIMIT 1
        ) names ON true
        WHERE bb.game_pk = :gp AND bb.team_id = :tid
        GROUP BY bb.player_id, bb.at_bats, bb.hits, bb.doubles, bb.triples,
                 bb.home_runs, bb.total_bases, bb.walks, bb.strikeouts,
                 bb.rbi, bb.slg
        ORDER BY bb.total_bases DESC, bb.hits DESC
    """), {"gp": game_pk, "tid": team_id}).mappings().all()

    return {
        "score":      score,
        "team_abbrev": team_abbrev,
        "opp_abbrev":  opp_abbrev,
        "is_home":     is_home,
        "pitchers":    [dict(p) for p in pitchers],
        "pitch_mix":   [dict(p) for p in pitch_mix],
        "batters":     [dict(b) for b in batters],
        "starter_id":  starter_id,
        "pitch_hand":  pitch_hand,
    }


def get_model_scores(yesterday: date, db) -> dict:
    """Return yesterday's scores + 7-day and season rolling averages."""
    daily = db.execute(text("""
        SELECT player_type, metric, n_players, mae, rmse,
               mean_proj, mean_actual, bias, within_1, within_2
        FROM mlb.model_scores
        WHERE score_date = :d
        ORDER BY player_type, metric
    """), {"d": yesterday}).mappings().all()

    rolling = db.execute(text("""
        SELECT player_type, metric,
               ROUND(AVG(mae)::numeric, 3)   AS mae_7d,
               ROUND(AVG(bias)::numeric, 3)  AS bias_7d,
               COUNT(*)                      AS days_7d
        FROM mlb.model_scores
        WHERE score_date >= :d7 AND score_date <= :d
          AND mae IS NOT NULL
        GROUP BY player_type, metric
    """), {"d": yesterday, "d7": yesterday - timedelta(days=6)}).mappings().all()

    season = db.execute(text("""
        SELECT player_type, metric,
               ROUND(AVG(mae)::numeric, 3)   AS mae_season,
               ROUND(AVG(bias)::numeric, 3)  AS bias_season,
               COUNT(*)                      AS days_season
        FROM mlb.model_scores
        WHERE score_date >= '2026-03-01' AND score_date <= :d
          AND mae IS NOT NULL
        GROUP BY player_type, metric
    """), {"d": yesterday}).mappings().all()

    roll_map   = {(r['player_type'], r['metric']): dict(r) for r in rolling}
    season_map = {(r['player_type'], r['metric']): dict(r) for r in season}

    rows = []
    for r in daily:
        key = (r['player_type'], r['metric'])
        rows.append({**dict(r),
                     **(roll_map.get(key, {})),
                     **(season_map.get(key, {}))})
    return {'rows': rows, 'has_data': bool(rows)}


def get_projection_results(yesterday: date, db) -> list:
    rows = db.execute(text("""
        SELECT
            da.player_name,
            da.player_type,
            da.actual_k,
            da.proj_k_pct,
            da.proj_ks_locked,
            da.opponent_abbrev,
            CASE WHEN g.home_team_abbrev = da.team_abbrev THEN 'vs' ELSE '@' END as venue_prefix
        FROM mlb.daily_actuals da
        LEFT JOIN mlb.games g ON g.game_pk = da.game_pk
        WHERE da.game_date = :d AND da.proj_k_pct IS NOT NULL
        ORDER BY da.player_name
    """), {"d": yesterday}).mappings().all()
    return [dict(r) for r in rows]


def get_batter_call_results(yesterday: date, db) -> dict:
    """Yesterday's batter TB and HR projections vs actuals."""
    tb_rows = db.execute(text("""
        SELECT
            da.player_name,
            ROUND((dbp.proj_slg * 4)::numeric, 2)                  AS proj_tb,
            da.actual_tb,
            da.actual_tb - ROUND((dbp.proj_slg * 4)::numeric, 2)   AS diff,
            ROUND(j2.juiced2_plus::numeric, 1)                     AS juiced2_plus
        FROM mlb.daily_actuals da
        JOIN LATERAL (
            SELECT proj_slg, bat_side
            FROM mlb.daily_batter_projections
            WHERE batter_id = da.player_id AND snapshot_date = da.game_date
            ORDER BY simulations DESC NULLS LAST
            LIMIT 1
        ) dbp ON true
        LEFT JOIN LATERAL (
            SELECT juiced2_plus
            FROM mlb.juiced2_scores
            WHERE batter_id = da.player_id AND season = 2026
              AND bat_side = dbp.bat_side
            LIMIT 1
        ) j2 ON true
        WHERE da.game_date  = :d
          AND da.player_type = 'batter'
          AND da.actual_tb  IS NOT NULL
          AND da.actual_ab  >= 2
          AND dbp.proj_slg  IS NOT NULL
        ORDER BY dbp.proj_slg * 4 DESC
        LIMIT 15
    """), {"d": yesterday}).mappings().all()

    hr_rows = db.execute(text("""
        SELECT
            da.player_name,
            ROUND(dbp.proj_hr_pct::numeric, 1)  AS proj_hr_pct,
            COALESCE(da.actual_hr, 0)            AS actual_hr,
            ROUND(j2.juiced2_plus::numeric, 1)   AS juiced2_plus
        FROM mlb.daily_actuals da
        JOIN LATERAL (
            SELECT proj_hr_pct, bat_side
            FROM mlb.daily_batter_projections
            WHERE batter_id = da.player_id AND snapshot_date = da.game_date
            ORDER BY simulations DESC NULLS LAST
            LIMIT 1
        ) dbp ON true
        LEFT JOIN LATERAL (
            SELECT juiced2_plus
            FROM mlb.juiced2_scores
            WHERE batter_id = da.player_id AND season = 2026
              AND bat_side = dbp.bat_side
            LIMIT 1
        ) j2 ON true
        WHERE da.game_date    = :d
          AND da.player_type  = 'batter'
          AND da.actual_ab    >= 2
          AND dbp.proj_hr_pct > 5.0
        ORDER BY dbp.proj_hr_pct DESC
    """), {"d": yesterday}).mappings().all()

    return {
        "tb_rows": [dict(r) for r in tb_rows],
        "hr_rows": [dict(r) for r in hr_rows],
    }


def get_pitcher_boxwhisker(db, today) -> list:
    """
    Box/whisker K projection data for today's starters.
    Same query as the /mlb/pitcher-boxwhisker endpoint (api/routers/mlb.py).
    """
    rows = db.execute(text("""
        WITH pitcher_hand AS (
            SELECT pitcher_id, MAX(pitch_hand) AS pitch_hand
            FROM mlb.at_bats WHERE pitch_hand IS NOT NULL
            GROUP BY pitcher_id
        ),
        today_starters AS (
            SELECT
                g.game_pk, g.game_date, g.game_time_utc,
                g.home_probable_pitcher_id AS home_pid,
                g.home_probable_pitcher_name AS home_name,
                g.home_team_id, g.home_team_abbrev,
                g.away_team_id, g.away_team_abbrev,
                g.away_probable_pitcher_id AS away_pid,
                g.away_probable_pitcher_name AS away_name
            FROM mlb.games g
            WHERE g.game_date = :today
              AND g.game_type = 'R'
              AND (g.home_probable_pitcher_id IS NOT NULL
                   OR g.away_probable_pitcher_id IS NOT NULL)
        ),
        starters AS (
            SELECT game_pk, game_time_utc,
                   home_pid AS pitcher_id, home_name AS pitcher_name,
                   home_team_id AS team_id, home_team_abbrev AS team_abbrev,
                   away_team_id AS opp_team_id, away_team_abbrev AS opp_abbrev,
                   'home' AS pitcher_side, 'away' AS opp_side
            FROM today_starters WHERE home_pid IS NOT NULL
            UNION ALL
            SELECT game_pk, game_time_utc,
                   away_pid, away_name,
                   away_team_id, away_team_abbrev,
                   home_team_id, home_team_abbrev,
                   'away', 'home'
            FROM today_starters WHERE away_pid IS NOT NULL
        ),
        actuals AS (
            SELECT bp.game_pk, bp.player_id AS pitcher_id, bp.strikeouts AS actual_k
            FROM mlb.boxscore_pitching bp
            WHERE bp.games_started = 1
        ),
        pitcher_game_stats AS (
            SELECT bp.player_id AS pitcher_id, bp.game_pk, g.game_date,
                bp.strikeouts AS pitcher_ks, bp.batters_faced AS pitcher_bf,
                bp.strikeouts::numeric / NULLIF(bp.batters_faced, 0) AS pitcher_k_pct,
                CASE WHEN g.home_team_id = bp.team_id THEN 'home' ELSE 'away' END AS side
            FROM mlb.boxscore_pitching bp
            JOIN mlb.games g ON g.game_pk = bp.game_pk
            WHERE g.game_date::date BETWEEN (:today)::date - INTERVAL '60 days'
                                        AND (:today)::date - INTERVAL '1 day'
              AND g.season = 2026 AND g.game_type = 'R'
              AND bp.games_started = 1 AND bp.batters_faced > 0
        ),
        pitcher_ranked AS (
            SELECT pgs.*,
                ROW_NUMBER() OVER (
                    PARTITION BY pitcher_id, side ORDER BY game_date DESC, game_pk DESC
                ) AS rn
            FROM pitcher_game_stats pgs
        ),
        pitcher_last3 AS (
            SELECT pitcher_id, side,
                SUM(pitcher_ks)::numeric / NULLIF(SUM(pitcher_bf), 0) AS pitcher_k_pct_3w,
                ROUND(AVG(pitcher_bf), 2) AS pitcher_avg_bf_3w,
                COUNT(*) AS pitcher_starts_3w,
                ROUND(STDDEV_SAMP(pitcher_k_pct), 4) AS pitcher_k_pct_sd_3starts,
                ROUND(STDDEV_SAMP(pitcher_bf), 2) AS pitcher_bf_sd_3starts,
                STRING_AGG(pitcher_ks::text, ', ' ORDER BY game_date DESC, game_pk DESC) AS pitcher_last3_k,
                STRING_AGG(pitcher_bf::text, ', ' ORDER BY game_date DESC, game_pk DESC) AS pitcher_last3_bf,
                STRING_AGG(game_date::text, ', ' ORDER BY game_date DESC, game_pk DESC) AS pitcher_last3_dates
            FROM pitcher_ranked WHERE rn <= 3
            GROUP BY pitcher_id, side
        ),
        historical_starters AS (
            SELECT DISTINCT bp.game_pk, g.game_date, bp.player_id AS pitcher_id,
                bp.team_id AS pitcher_team_id,
                CASE WHEN g.home_team_id = bp.team_id THEN g.away_team_id
                     ELSE g.home_team_id END AS opp_team_id,
                CASE WHEN g.home_team_id = bp.team_id THEN 'away' ELSE 'home' END AS opp_side,
                ph.pitch_hand
            FROM mlb.boxscore_pitching bp
            JOIN mlb.games g ON g.game_pk = bp.game_pk
            JOIN pitcher_hand ph ON ph.pitcher_id = bp.player_id
            WHERE g.game_date::date BETWEEN (:today)::date - INTERVAL '60 days'
                                        AND (:today)::date - INTERVAL '1 day'
              AND g.season = 2026 AND g.game_type = 'R'
              AND bp.games_started = 1 AND bp.batters_faced > 0
        ),
        opp_game_vs_sp_raw AS (
            SELECT hs.opp_team_id, hs.opp_side, hs.game_pk, hs.game_date,
                hs.pitcher_id, hs.pitch_hand,
                COUNT(*) AS pa,
                SUM(CASE WHEN ab.event IN ('Strikeout','Strikeout - DP') THEN 1 ELSE 0 END) AS k
            FROM historical_starters hs
            JOIN mlb.at_bats ab ON ab.game_pk = hs.game_pk AND ab.pitcher_id = hs.pitcher_id
            WHERE ab.event NOT IN (
                'Runner Out','Caught Stealing 2B','Caught Stealing 3B',
                'Wild Pitch','Passed Ball','Pickoff','Caught Stealing Home',
                'Pickoff Caught Stealing 2B','Pickoff Caught Stealing 3B',
                'Pickoff Caught Stealing Home'
            )
            GROUP BY hs.opp_team_id, hs.opp_side, hs.game_pk, hs.game_date,
                     hs.pitcher_id, hs.pitch_hand
        ),
        opp_game_vs_sp AS (
            SELECT opp_team_id, opp_side, game_pk, game_date, pitch_hand,
                   SUM(pa) AS pa, SUM(k) AS k
            FROM opp_game_vs_sp_raw
            GROUP BY opp_team_id, opp_side, game_pk, game_date, pitch_hand
        ),
        opp_ranked AS (
            SELECT og.*,
                ROW_NUMBER() OVER (
                    PARTITION BY opp_team_id, opp_side, pitch_hand
                    ORDER BY game_date DESC, game_pk DESC
                ) AS rn
            FROM opp_game_vs_sp og
        ),
        opp_last3 AS (
            SELECT opp_team_id, opp_side, pitch_hand,
                SUM(k)::numeric / NULLIF(SUM(pa), 0) AS opp_k_pct,
                ROUND(AVG(pa), 2) AS opp_avg_pa,
                COUNT(*) AS opp_games,
                ROUND(STDDEV_SAMP(k::numeric / NULLIF(pa, 0)), 4) AS opp_k_pct_sd_3games,
                ROUND(STDDEV_SAMP(pa), 2) AS opp_pa_sd_3games,
                STRING_AGG(pa::text, ', ' ORDER BY game_date DESC, game_pk DESC) AS last3_pa,
                STRING_AGG(k::text, ', ' ORDER BY game_date DESC, game_pk DESC) AS last3_k,
                STRING_AGG(game_date::text, ', ' ORDER BY game_date DESC, game_pk DESC) AS last3_dates
            FROM opp_ranked WHERE rn <= 3
            GROUP BY opp_team_id, opp_side, pitch_hand
        ),
        projections AS (
            SELECT
                s.pitcher_name, s.team_abbrev, ph.pitch_hand, s.opp_abbrev,
                s.pitcher_id, s.game_pk, s.game_time_utc,
                s.pitcher_side, s.opp_side, act.actual_k,
                p3w.pitcher_k_pct_3w, p3w.pitcher_avg_bf_3w, p3w.pitcher_starts_3w,
                p3w.pitcher_k_pct_sd_3starts, p3w.pitcher_bf_sd_3starts,
                p3w.pitcher_last3_k, p3w.pitcher_last3_bf, p3w.pitcher_last3_dates,
                opp.opp_k_pct, opp.opp_avg_pa,
                opp.opp_k_pct_sd_3games, opp.opp_pa_sd_3games,
                opp.last3_pa, opp.last3_k, opp.last3_dates,
                p3w.pitcher_k_pct_3w * p3w.pitcher_avg_bf_3w AS pit,
                opp.opp_k_pct * opp.opp_avg_pa AS opp_proj,
                (
                    p3w.pitcher_k_pct_3w * p3w.pitcher_avg_bf_3w
                    + COALESCE(opp.opp_k_pct * opp.opp_avg_pa,
                               p3w.pitcher_k_pct_3w * p3w.pitcher_avg_bf_3w)
                ) / 2.0 AS proj,
                SQRT(
                    POWER(COALESCE(p3w.pitcher_avg_bf_3w, 21.0), 2)
                        * POWER(COALESCE(p3w.pitcher_k_pct_sd_3starts, 0.05), 2)
                    + POWER(COALESCE(p3w.pitcher_k_pct_3w, 0.22), 2)
                        * POWER(COALESCE(p3w.pitcher_bf_sd_3starts, 4.5), 2)
                ) AS sd_pit,
                SQRT(
                    POWER(COALESCE(opp.opp_avg_pa, 21.0), 2)
                        * POWER(COALESCE(opp.opp_k_pct_sd_3games, 0.05), 2)
                    + POWER(COALESCE(opp.opp_k_pct, 0.22), 2)
                        * POWER(COALESCE(opp.opp_pa_sd_3games, 4.5), 2)
                ) AS sd_opp
            FROM starters s
            LEFT JOIN pitcher_hand ph ON ph.pitcher_id = s.pitcher_id
            LEFT JOIN pitcher_last3 p3w
                ON p3w.pitcher_id = s.pitcher_id AND p3w.side = s.pitcher_side
            LEFT JOIN opp_last3 opp
                ON opp.opp_team_id = s.opp_team_id
                AND opp.opp_side = s.opp_side
                AND opp.pitch_hand = ph.pitch_hand
            LEFT JOIN actuals act ON act.game_pk = s.game_pk AND act.pitcher_id = s.pitcher_id
        )
        SELECT
            pitcher_name, team_abbrev, pitch_hand, opp_abbrev,
            pitcher_id, pitcher_side, opp_side, game_time_utc, actual_k,
            ROUND(pit::numeric, 2) AS pit,
            ROUND(opp_proj::numeric, 2) AS opp,
            ROUND(proj::numeric, 2) AS proj,
            ROUND((0.5 * SQRT(POWER(sd_pit,2) + POWER(sd_opp,2)))::numeric, 2) AS proj_sd,
            ROUND(GREATEST(0, proj - 2.576*0.5*SQRT(POWER(sd_pit,2)+POWER(sd_opp,2)))::numeric, 2) AS whisker_low,
            ROUND(GREATEST(0, proj - 0.674*0.5*SQRT(POWER(sd_pit,2)+POWER(sd_opp,2)))::numeric, 2) AS q1,
            ROUND(proj::numeric, 2) AS median,
            ROUND((proj + 0.674*0.5*SQRT(POWER(sd_pit,2)+POWER(sd_opp,2)))::numeric, 2) AS q3,
            ROUND((proj + 2.576*0.5*SQRT(POWER(sd_pit,2)+POWER(sd_opp,2)))::numeric, 2) AS whisker_high,
            pitcher_k_pct_3w, pitcher_avg_bf_3w, pitcher_starts_3w,
            pitcher_last3_k, pitcher_last3_bf, pitcher_last3_dates,
            opp_k_pct, opp_avg_pa, opp_k_pct_sd_3games, opp_pa_sd_3games,
            last3_pa, last3_k, last3_dates
        FROM projections
        ORDER BY game_time_utc ASC NULLS LAST, proj DESC NULLS LAST
    """), {"today": str(today)}).mappings().all()

    pt = ZoneInfo("America/Los_Angeles")
    result = []
    for row in rows:
        r = dict(row)
        game_time_utc = r.pop("game_time_utc", None)
        if game_time_utc is not None:
            r["game_time_pt"] = game_time_utc.astimezone(pt).strftime("%-I:%M %p")
        else:
            r["game_time_pt"] = None
        for k, v in r.items():
            if hasattr(v, '__float__'):
                r[k] = float(v)
        result.append(r)
    return result


def draw_box_whisker_svg(p: dict, width: int = 380, actual_k=None) -> str:
    """
    Inline SVG box/whisker plot for a pitcher's K projection.
    Black/white color scheme with a red line for actual observed Ks.
    """
    H = 52
    PAD = 20
    MAX_K = 15

    def scale(v):
        v = min(float(v or 0), MAX_K)
        return PAD + (v / MAX_K) * (width - PAD * 2)

    wLow  = scale(p.get('whisker_low', 0))
    q1x   = scale(p.get('q1', 0))
    medx  = scale(p.get('median', 0))
    q3x   = scale(p.get('q3', 0))
    wHigh = scale(p.get('whisker_high', MAX_K))

    svg = [f'<svg width="{width}" height="{H}" viewBox="0 0 {width} {H}" '
           f'xmlns="http://www.w3.org/2000/svg" style="display:block">']

    # Whisker line
    svg.append(f'<line x1="{wLow:.1f}" x2="{wHigh:.1f}" y1="36" y2="36" '
               f'stroke="#1a1a1a" stroke-width="1.5"/>')

    # End caps
    for x in [wLow, wHigh]:
        svg.append(f'<line x1="{x:.1f}" x2="{x:.1f}" y1="30" y2="42" '
                   f'stroke="#1a1a1a" stroke-width="1.5"/>')

    # IQR box
    box_w = max(q3x - q1x, 2)
    svg.append(f'<rect x="{q1x:.1f}" y="28" width="{box_w:.1f}" height="16" '
               f'fill="white" stroke="#1a1a1a" stroke-width="1.5" rx="2"/>')

    # Median line
    svg.append(f'<line x1="{medx:.1f}" x2="{medx:.1f}" y1="28" y2="44" '
               f'stroke="#1a1a1a" stroke-width="2"/>')

    # Labels with collision detection
    labels = [
        {'x': wLow,  'v': p.get('whisker_low', 0),  'bold': False},
        {'x': q1x,   'v': p.get('q1', 0),            'bold': False},
        {'x': medx,  'v': p.get('median', 0),         'bold': True},
        {'x': q3x,   'v': p.get('q3', 0),            'bold': False},
        {'x': wHigh, 'v': p.get('whisker_high', 0),  'bold': False},
    ]
    true_x = [lb['x'] for lb in labels]

    MIN_GAP = 24
    for _ in range(20):
        moved = False
        for i in range(len(labels) - 1):
            gap = labels[i+1]['x'] - labels[i]['x']
            if gap < MIN_GAP:
                nudge = (MIN_GAP - gap) / 2
                labels[i]['x'] -= nudge
                labels[i+1]['x'] += nudge
                moved = True
        for lb in labels:
            lb['x'] = max(PAD, min(width - PAD, lb['x']))
        if not moved:
            break

    # Connector ticks
    for i, lb in enumerate(labels):
        tx = true_x[i]
        if abs(lb['x'] - tx) > 2:
            svg.append(f'<line x1="{lb["x"]:.1f}" x2="{tx:.1f}" y1="20" y2="26" '
                       f'stroke="#aaa" stroke-width="0.8"/>')

    # Label text
    for lb in labels:
        fw = '600' if lb['bold'] else '400'
        fill = '#1a1a1a' if lb['bold'] else '#555'
        fs = '11' if lb['bold'] else '10'
        val = f"{float(lb['v']):.1f}" if lb['v'] is not None else '—'
        svg.append(f'<text x="{lb["x"]:.1f}" y="16" text-anchor="middle" '
                   f'font-size="{fs}" font-weight="{fw}" fill="{fill}" '
                   f'font-family="system-ui,sans-serif">{val}</text>')

    # Actual K red vertical line
    if actual_k is not None:
        ax = scale(actual_k)
        svg.append(f'<line x1="{ax:.1f}" x2="{ax:.1f}" y1="24" y2="46" '
                   f'stroke="#cc2200" stroke-width="2.5"/>')
        svg.append(f'<text x="{ax:.1f}" y="{H-1}" text-anchor="middle" '
                   f'font-size="9" font-weight="600" fill="#cc2200" '
                   f'font-family="system-ui,sans-serif">{actual_k:.0f}K</text>')

    svg.append('</svg>')
    return ''.join(svg)


def get_today_outlook(today: date, db) -> dict:
    batters = db.execute(text("""
        SELECT
            dbp.batter_id,
            dbp.batter_name,
            dbp.opp_pitcher_name,
            ROUND(dbp.proj_slg::numeric,3)    as proj_slg,
            ROUND(dbp.proj_hr_pct::numeric,3) as proj_hr_pct,
            ROUND(dbp.proj_ops::numeric,3)    as proj_ops,
            ROUND((dbp.proj_slg * 4)::numeric,2) as proj_tb,
            ROUND(j2.juiced2_plus::numeric,1) as juiced2_plus,
            ROUND(pk.pk_plus::numeric,1)      as opp_pk_plus,
            ROUND(phr.phr_plus::numeric,1)    as opp_phr_plus
        FROM mlb.daily_batter_projections dbp
        LEFT JOIN mlb.juiced2_scores j2
            ON j2.batter_id = dbp.batter_id
            AND j2.season = 2026
            AND j2.bat_side = dbp.bat_side
        LEFT JOIN mlb.pk_overall pk
            ON pk.pitcher_id = dbp.opp_pitcher_id AND pk.season = 2026
        LEFT JOIN mlb.phr_overall phr
            ON phr.pitcher_id = dbp.opp_pitcher_id AND phr.season = 2026
        WHERE dbp.snapshot_date = (SELECT MAX(snapshot_date) FROM mlb.daily_batter_projections)
          AND dbp.proj_slg * 4 >= 1.2
        ORDER BY dbp.proj_slg DESC
        LIMIT 20
    """), {"today": today}).mappings().all()

    mismatches = db.execute(text("""
        SELECT
            dbp.batter_name,
            dbp.opp_pitcher_name,
            ROUND(j2.juiced2_plus::numeric, 1)  AS juiced2_plus,
            ROUND(g3.goose3_plus::numeric, 1)   AS goose3_plus,
            ROUND(dbp.proj_ops::numeric, 3)     AS proj_ops,
            ROUND(
                (COALESCE(j2.juiced2_plus, 100) - COALESCE(g3.goose3_plus, 100))::numeric, 1
            )                                   AS mismatch_score
        FROM mlb.daily_batter_projections dbp
        LEFT JOIN mlb.juiced2_scores j2
            ON j2.batter_id = dbp.batter_id AND j2.season = 2026
            AND j2.bat_side = dbp.bat_side
        LEFT JOIN mlb.goose3_overall g3
            ON g3.pitcher_id = dbp.opp_pitcher_id AND g3.season = 2026
        WHERE dbp.snapshot_date = (SELECT MAX(snapshot_date) FROM mlb.daily_batter_projections)
          AND COALESCE(j2.juiced2_plus, 100) > COALESCE(g3.goose3_plus, 100)
        ORDER BY (COALESCE(j2.juiced2_plus, 100) - COALESCE(g3.goose3_plus, 100)) DESC
        LIMIT 10
    """), {"today": today}).mappings().all()

    return {
        "batters":    [dict(b) for b in batters],
        "mismatches": [dict(m) for m in mismatches],
    }


def get_today_game_projections(today: date, db) -> list:
    rows = db.execute(text("""
        SELECT away_team_abbrev, home_team_abbrev,
               away_pitcher_name, home_pitcher_name,
               proj_away_runs, proj_home_runs, proj_total, proj_winner
        FROM mlb.game_projections
        WHERE game_date = :d
        ORDER BY proj_total DESC NULLS LAST
    """), {"d": today}).mappings().all()
    return [dict(r) for r in rows]


def get_latest_integrity_check(db) -> dict | None:
    """Latest mlb.pipeline_integrity row, if the table/data exists yet."""
    try:
        row = db.execute(text("""
            SELECT check_date, games_checked, games_missing, games_fixed,
                   pa_mismatches, status, notes
            FROM mlb.pipeline_integrity
            ORDER BY check_date DESC, created_at DESC
            LIMIT 1
        """)).mappings().first()
        return dict(row) if row else None
    except Exception:
        db.rollback()
        return None


def get_report_mode() -> str:
    """Returns 'morning' before 9am PT, 'daytime' from 9am onward."""
    now_pt = datetime.now(ZoneInfo("America/Los_Angeles"))
    return 'morning' if now_pt.hour < 9 else 'daytime'


def render_homepage(yesterday: date, today: date, db) -> str:
    scores       = get_yesterday_scores(yesterday, db)
    proj_results = get_projection_results(yesterday, db)
    batter_calls = get_batter_call_results(yesterday, db)
    outlook      = get_today_outlook(today, db)
    model_score_data = get_model_scores(yesterday, db)
    today_games  = get_today_game_projections(today, db)
    integrity_check = get_latest_integrity_check(db)

    focus_boxes = {}
    for abbrev, info in FOCUS_TEAMS.items():
        for game in scores:
            if game['home_team_id'] == info['team_id'] or \
               game['away_team_id'] == info['team_id']:
                focus_boxes[abbrev] = get_focus_team_boxscore(
                    game['game_pk'], info['team_id'], db
                )
                break

    accuracy_rows = [r for r in proj_results
                     if r.get('proj_k_pct') is not None
                     and r.get('actual_k') is not None]

    def accuracy_color(proj_k, actual_k):
        diff = abs(float(proj_k) - float(actual_k))
        if diff <= 1: return '#1b5e20'
        if diff <= 2: return '#e65100'
        return '#b71c1c'

    day_str  = today.strftime("%A, %B %d, %Y").upper()
    yest_str = yesterday.strftime("%B %d, %Y")

    def fmt(v, dec=1):
        return f"{v:.{dec}f}" if v is not None else "—"

    def pct_fmt(v):
        return f"{v*100:.1f}%" if v is not None else "—"

    def score_color(val):
        if val is None: return '#888'
        if val >= 115: return '#1a3a5c'
        if val >= 102: return '#2e7d32'
        if val >= 95:  return '#e65100'
        return '#b71c1c'

    def pct_diff_color(today_val, league_val):
        try:
            today = float(today_val)
            league = float(league_val)
            if league == 0:
                return '#1a1a1a'
            diff_pct = (today - league) / abs(league)
            if diff_pct >= 0.10:
                return '#1b5e20'
            elif diff_pct <= -0.10:
                return '#b71c1c'
            else:
                return '#1a1a1a'
        except (TypeError, ValueError):
            return '#1a1a1a'

    def pct_diff_color_invert(today_val, league_val):
        try:
            today = float(today_val)
            league = float(league_val)
            if league == 0:
                return '#1a1a1a'
            diff_pct = (today - league) / abs(league)
            if diff_pct <= -0.10:
                return '#1b5e20'
            elif diff_pct >= 0.10:
                return '#b71c1c'
            else:
                return '#1a1a1a'
        except (TypeError, ValueError):
            return '#1a1a1a'

    def cell(today, season, league, suffix='', invert=False):
        if today is None:
            return '—'
        color_fn = pct_diff_color_invert if invert else pct_diff_color
        color = color_fn(today, league) if league is not None else '#1a1a1a'
        bold = 'font-weight:700' if color != '#1a1a1a' else 'font-weight:400'
        s_str = (f'<span style="color:#888;font-size:10px">({season}{suffix})</span>'
                 if season is not None else '')
        return (f'<span style="color:{color};{bold}">'
                f'{today}{suffix}</span> {s_str}')

    # ── Score table ───────────────────────────────────────────────────────────
    score_rows = ""
    for g in scores:
        home, away = g['home_team_abbrev'], g['away_team_abbrev']
        hs, as_ = g['home_score'] or 0, g['away_score'] or 0
        winner = home if hs > as_ else away
        score_rows += f"""
        <tr>
            <td style="padding:3px 12px;font-weight:{'700' if away==winner else '400'}">{away}</td>
            <td style="padding:3px 12px;text-align:center;font-weight:700">{as_}</td>
            <td style="padding:3px 12px;text-align:center;color:#888">@</td>
            <td style="padding:3px 12px;font-weight:{'700' if home==winner else '400'}">{home}</td>
            <td style="padding:3px 12px;text-align:center;font-weight:700">{hs}</td>
            <td style="padding:3px 12px;text-align:center;color:#888">F</td>
        </tr>"""

    # ── Batter TB table ───────────────────────────────────────────────────────
    batter_tb_rows_html = ""
    for r in batter_calls.get('tb_rows', []):
        proj = float(r.get('proj_tb') or 0)
        act  = int(r.get('actual_tb') or 0)
        diff = act - proj
        diff_str  = f"+{diff:.1f}" if diff > 0 else f"{diff:.1f}"
        diff_color = "#1b5e20" if diff > 0 else ("#b71c1c" if diff < 0 else "#888")
        j2 = r.get('juiced2_plus')
        batter_tb_rows_html += f"""
        <tr style="border-bottom:1px solid #eee">
            <td style="padding:4px 8px">{r.get('player_name','—')}</td>
            <td style="padding:4px 8px;text-align:right">{fmt(proj,2)}</td>
            <td style="padding:4px 8px;text-align:right;font-weight:700">{act}</td>
            <td style="padding:4px 8px;text-align:right;color:{diff_color}">{diff_str}</td>
            <td style="padding:4px 8px;text-align:right;color:{score_color(j2)}">{fmt(j2,1) if j2 is not None else '—'}</td>
        </tr>"""

    # ── Batter HR table ───────────────────────────────────────────────────────
    batter_hr_rows_html = ""
    for r in batter_calls.get('hr_rows', []):
        actual_hr = int(r.get('actual_hr') or 0)
        hit_one   = actual_hr > 0
        hr_color  = "#1b5e20" if hit_one else "#555"
        hr_label  = f"✓ {actual_hr} HR" if hit_one else "—"
        j2 = r.get('juiced2_plus')
        batter_hr_rows_html += f"""
        <tr style="border-bottom:1px solid #eee">
            <td style="padding:4px 8px">{r.get('player_name','—')}</td>
            <td style="padding:4px 8px;text-align:right">{fmt(r.get('proj_hr_pct'),1)}%</td>
            <td style="padding:4px 8px;text-align:right;font-weight:700;color:{hr_color}">{hr_label}</td>
            <td style="padding:4px 8px;text-align:right;color:{score_color(j2)}">{fmt(j2,1) if j2 is not None else '—'}</td>
        </tr>"""

    # ── Pitcher K accuracy table ──────────────────────────────────────────────
    accuracy_table_rows = ""
    for r in sorted(accuracy_rows, key=lambda x: abs(
            (float(x.get('proj_ks_locked') or 0) or (x.get('proj_k_pct') or 0)/100*27)
            - float(x.get('actual_k') or 0)))[:12]:
        proj_k  = float(r.get('proj_ks_locked') or 0) or (r.get('proj_k_pct') or 0) / 100 * 27
        actual_k = r.get('actual_k') or 0
        diff    = float(actual_k) - proj_k
        color   = accuracy_color(proj_k, actual_k)
        diff_str = f"+{diff:.1f}" if diff > 0 else f"{diff:.1f}"
        opp = r.get('opponent_abbrev', '')
        venue = r.get('venue_prefix', 'vs')
        opp_str = (f" <span style='color:#888;font-size:10px'>{venue} {opp}</span>"
                   if opp else "")
        accuracy_table_rows += f"""
        <tr style="border-bottom:1px solid #eee">
            <td style="padding:4px 8px">{r.get('player_name','—')}{opp_str}</td>
            <td style="padding:4px 8px;text-align:right">{fmt(proj_k,1)}</td>
            <td style="padding:4px 8px;text-align:right;font-weight:700;color:{color}">{actual_k}</td>
            <td style="padding:4px 8px;text-align:right;color:{color}">{diff_str}</td>
        </tr>"""

    # ── Focus team boxes ──────────────────────────────────────────────────────
    focus_html = ""
    for abbrev, box in focus_boxes.items():
        if not box:
            continue
        team_info = FOCUS_TEAMS[abbrev]

        pit_rows = ""
        for p in box['pitchers']:
            dec = "W" if p.get('win') else ("L" if p.get('loss') else "")
            pit_rows += f"""
            <tr style="border-bottom:1px solid #ddd">
                <td style="padding:4px 8px;font-weight:{'700' if p.get('gs') else '400'}">
                    {p.get('name','—')}{' ('+dec+')' if dec else ''}</td>
                <td style="padding:4px 8px;text-align:right">{p.get('ip','—')}</td>
                <td style="padding:4px 8px;text-align:right">{p.get('hits','—')}</td>
                <td style="padding:4px 8px;text-align:right">{p.get('earned_runs','—')}</td>
                <td style="padding:4px 8px;text-align:right">{p.get('walks','—')}</td>
                <td style="padding:4px 8px;text-align:right;font-weight:700">{p.get('k','—')}</td>
                <td style="padding:4px 8px;text-align:right;color:#888">{p.get('pitches','—')}</td>
            </tr>"""

        mix_rows = ""
        for m in box.get('pitch_mix', []):
            mix_rows += f"""
            <tr style="border-bottom:1px solid #eee">
                <td style="padding:3px 8px;font-weight:700">{m['pitch_type_code']}</td>
                <td style="padding:3px 8px;text-align:right">{m.get('today_n','—')}</td>
                <td style="padding:3px 8px;text-align:right">
                    {cell(m.get('today_pct'), m.get('s_pct'), None, '%')}</td>
                <td style="padding:3px 8px;text-align:right">
                    {cell(m.get('velo'), m.get('s_velo'), m.get('lg_velo'))}</td>
                <td style="padding:3px 8px;text-align:right">
                    {cell(m.get('ivb'), m.get('s_ivb'), m.get('lg_ivb'))}</td>
                <td style="padding:3px 8px;text-align:right">
                    {cell(m.get('hbreak'), m.get('s_hbreak'), m.get('lg_hmov'))}</td>
                <td style="padding:3px 8px;text-align:right">
                    {cell(m.get('whiff_pct'), m.get('s_whiff'), m.get('lg_whiff'), '%')}</td>
                <td style="padding:3px 8px;text-align:right">{m.get('strikes','—')}</td>
                <td style="padding:3px 8px;text-align:right">{m.get('balls','—')}</td>
            </tr>"""

        bat_rows = ""
        for b in box['batters']:
            extra = []
            if b.get('d'):        extra.append(f"{b['d']}2B")
            if b.get('t'):        extra.append(f"{b['t']}3B")
            if b.get('hr'):       extra.append(f"{b['hr']}HR")
            if b.get('bb_count'): extra.append(f"{b['bb_count']}BB")
            if b.get('k'):        extra.append(f"{b['k']}K")
            notes = ", ".join(extra)
            bat_rows += f"""
            <tr style="border-bottom:1px solid #eee">
                <td style="padding:4px 8px">{b.get('name','—')}</td>
                <td style="padding:4px 8px;text-align:right">{b.get('ab','—')}</td>
                <td style="padding:4px 8px;text-align:right;font-weight:700">{b.get('h','—')}</td>
                <td style="padding:4px 8px;text-align:right;font-weight:700">{b.get('tb','—')}</td>
                <td style="padding:4px 8px;text-align:right">{b.get('rbi','—')}</td>
                <td style="padding:4px 8px;color:#888;font-size:11px">{notes}</td>
            </tr>"""

        starter_name = next(
            (p['name'] for p in box['pitchers'] if p.get('gs')), 'Starter'
        )

        mix_section = ""
        if mix_rows:
            profile_link = (
                f'<a href="/mlb-dashboard#pitcher/{box["starter_id"]}" target="_blank" '
                f'style="font-size:11px;font-weight:400;color:#1a3a5c;text-decoration:none;'
                f'letter-spacing:0.05em">VIEW PROFILE →</a>'
                if box.get('starter_id') else ''
            )
            mix_section = f"""
            <div style="font-size:13px;font-weight:700;margin-bottom:2px;letter-spacing:0.08em;
                        display:flex;justify-content:space-between;align-items:baseline">
                <span>PITCH MIX — {(starter_name or 'STARTER').upper()}</span>
                {profile_link}
            </div>
            <div style="font-size:10px;color:#888;margin-bottom:6px;letter-spacing:0.06em">
                TODAY vs (SEASON AVG)</div>
            <table style="width:100%;border-collapse:collapse;font-size:11px;margin-bottom:4px">
                <thead>
                    <tr style="border-bottom:2px solid #000;font-size:10px;letter-spacing:0.06em;background:#f5f5f5">
                        <th style="text-align:left;padding:3px 8px">TYPE</th>
                        <th style="text-align:right;padding:3px 8px">N</th>
                        <th style="text-align:right;padding:3px 8px">USE%</th>
                        <th style="text-align:right;padding:3px 8px">VELO</th>
                        <th style="text-align:right;padding:3px 8px">IVB</th>
                        <th style="text-align:right;padding:3px 8px">HBRK</th>
                        <th style="text-align:right;padding:3px 8px">WHIFF%</th>
                        <th style="text-align:right;padding:3px 8px">STR</th>
                        <th style="text-align:right;padding:3px 8px">BALL</th>
                    </tr>
                </thead>
                <tbody>{mix_rows}</tbody>
            </table>
            <div style="font-size:10px;color:#888;margin-bottom:16px;letter-spacing:0.04em">
                Color vs league avg ({box.get('pitch_hand','R')}HP) —
                <span style="color:#1b5e20;font-weight:700">green</span> ≥10% above ·
                <span style="color:#b71c1c;font-weight:700">red</span> ≥10% below ·
                season avg shown in (parens)
            </div>"""

        focus_html += f"""
        <div style="margin-bottom:32px">
            <div style="font-size:14px;font-weight:700;border-bottom:2px solid #000;
                        padding-bottom:4px;margin-bottom:12px;letter-spacing:0.05em">
                {team_info['name'].upper()} — {box['score']}
            </div>
            <div style="font-size:13px;font-weight:700;margin-bottom:4px;letter-spacing:0.08em">PITCHING</div>
            <table style="width:100%;border-collapse:collapse;font-size:12px;margin-bottom:16px">
                <thead>
                    <tr style="border-bottom:2px solid #000;font-size:10px;letter-spacing:0.08em">
                        <th style="text-align:left;padding:3px 8px">PITCHER</th>
                        <th style="text-align:right;padding:3px 8px">IP</th>
                        <th style="text-align:right;padding:3px 8px">H</th>
                        <th style="text-align:right;padding:3px 8px">ER</th>
                        <th style="text-align:right;padding:3px 8px">BB</th>
                        <th style="text-align:right;padding:3px 8px">K</th>
                        <th style="text-align:right;padding:3px 8px">P</th>
                    </tr>
                </thead>
                <tbody>{pit_rows}</tbody>
            </table>
            {mix_section}
            <div style="font-size:13px;font-weight:700;margin-bottom:4px;letter-spacing:0.08em">BATTING</div>
            <table style="width:100%;border-collapse:collapse;font-size:12px;margin-bottom:8px">
                <thead>
                    <tr style="border-bottom:2px solid #000;font-size:10px;letter-spacing:0.08em">
                        <th style="text-align:left;padding:3px 8px">BATTER</th>
                        <th style="text-align:right;padding:3px 8px">AB</th>
                        <th style="text-align:right;padding:3px 8px">H</th>
                        <th style="text-align:right;padding:3px 8px">TB</th>
                        <th style="text-align:right;padding:3px 8px">RBI</th>
                        <th style="text-align:left;padding:3px 8px">NOTES</th>
                    </tr>
                </thead>
                <tbody>{bat_rows}</tbody>
            </table>
        </div>"""

    # ── Today's outlook — pitcher box/whisker projections ────────────────────
    bw_pitchers = get_pitcher_boxwhisker(db, today)

    bw_rows = []
    for p in bw_pitchers:
        matchup = f"{p['team_abbrev']} vs {p['opp_abbrev']}" if p['pitcher_side'] == 'home' \
                  else f"{p['team_abbrev']} @ {p['opp_abbrev']}"
        hand = p.get('pitch_hand') or '?'
        time_str = p.get('game_time_pt') or ''
        proj_val = f"{float(p['proj']):.1f}" if p.get('proj') is not None else '—'
        sd_val = f"±{float(p['proj_sd']):.1f}" if p.get('proj_sd') is not None else ''

        svg_str = draw_box_whisker_svg(p, width=380, actual_k=p.get('actual_k'))

        bw_rows.append(f"""
        <tr>
          <td style="padding:6px 12px 6px 0; white-space:nowrap; vertical-align:middle;">
            <div style="font-weight:500; font-size:13px;">{p['pitcher_name']}</div>
            <div style="font-size:11px; color:#555;">{matchup} · {hand}HP
              {f'· {time_str} PT' if time_str else ''}</div>
          </td>
          <td style="padding:6px 0; vertical-align:middle;">
            {svg_str}
          </td>
          <td style="padding:6px 0 6px 16px; text-align:right; vertical-align:middle; white-space:nowrap;">
            <div style="font-size:14px; font-weight:600; color:#1a1a1a;">{proj_val} Ks</div>
            <div style="font-size:11px; color:#888;">{sd_val}</div>
          </td>
        </tr>
        """)

    pitcher_bw_html = f"""
<table style="width:100%; border-collapse:collapse;">
  <colgroup>
    <col style="width:180px">
    <col>
    <col style="width:80px">
  </colgroup>
  {''.join(bw_rows) if bw_rows else '<tr><td colspan="3" style="color:#888; font-size:13px; padding:8px 0;">No starters found for today.</td></tr>'}
</table>
<div class="footnote" style="margin-top:6px;margin-bottom:20px">
    Box = IQR (25th–75th percentile) · Whiskers = 1st–99th percentile ·
    Center line = projected median · <span style="color:#cc2200;">Red line = actual observed Ks</span> ·
    Projection blends pitcher's last 3 starts (same home/away side) with opponent's last 3 games
    vs same-handedness starters · 60-day lookback
</div>"""

    batter_rows = ""
    for b in outlook['batters'][:12]:
        phr = b.get('opp_phr_plus') or 100
        j2  = b.get('juiced2_plus') or 100
        gap = j2 - phr
        if abs(gap) >= 8:
            mismatch = (f"<span style='color:#1b5e20'>BAT+{gap:.0f}</span>"
                        if gap > 0 else
                        f"<span style='color:#b71c1c'>PIT{gap:.0f}</span>")
        else:
            mismatch = ""
        batter_rows += f"""
        <tr style="border-bottom:1px solid #eee">
            <td style="padding:4px 8px;font-weight:700">{b.get('batter_name','—')}</td>
            <td style="padding:4px 8px;color:#888">vs {b.get('opp_pitcher_name','—')}</td>
            <td style="padding:4px 8px;text-align:right;font-weight:700">{fmt(b.get('proj_tb'),2)}</td>
            <td style="padding:4px 8px;text-align:right">{fmt(b.get('proj_hr_pct'), 1)}%</td>
            <td style="padding:4px 8px;text-align:right;color:{score_color(b.get('juiced2_plus'))}">{b.get('juiced2_plus','—')}</td>
            <td style="padding:4px 8px;text-align:right;color:{score_color(b.get('opp_phr_plus'))}">{b.get('opp_phr_plus','—')}</td>
            <td style="padding:4px 8px">{mismatch}</td>
        </tr>"""

    # ── Model score section ───────────────────────────────────────────────────
    def _fmt_mae(v, unit=''):
        if v is None: return '—'
        return f"{float(v):.2f}{unit}"

    def _bias_label(v):
        if v is None: return '—'
        v = float(v)
        if abs(v) < 0.05: return '<span style="color:#888">≈ calibrated</span>'
        direction = 'over' if v > 0 else 'under'
        color = '#e65100'
        return f'<span style="color:{color}">{direction} {abs(v):.2f}</span>'

    ms_rows = {(r['player_type'], r['metric']): r for r in model_score_data.get('rows', [])}

    def ms(ptype, metric):
        return ms_rows.get((ptype, metric), {})

    model_html = ""
    if model_score_data['has_data']:
        pkc = ms('pitcher', 'k_count')
        pkr = ms('pitcher', 'k_rate')
        per = ms('pitcher', 'er_proxy')
        pip = ms('pitcher', 'ip_dist')
        btb = ms('batter',  'tb')
        bkr = ms('batter',  'k_rate')
        bhr = ms('batter',  'hr_brier')
        grt = ms('game',    'run_total')
        gtr = ms('game',    'team_runs')
        gwp = ms('game',    'win_pick')

        model_html = f"""
<div class="section-header">Model Accuracy — {yest_str}</div>

<div style="font-size:12px;font-weight:700;letter-spacing:0.1em;margin-bottom:6px">
    PITCHERS
    <span style="font-weight:400;color:#888;font-size:11px">
        ({pkc.get('n_players','?')} starters with projections)
    </span>
</div>
<table style="width:100%;font-size:12px;margin-bottom:12px">
    <thead><tr>
        <th style="text-align:left">METRIC</th>
        <th>MAE</th><th>BIAS</th><th>WITHIN 1</th><th>WITHIN 2</th>
        <th>7-DAY MAE</th><th>SEASON MAE</th>
    </tr></thead>
    <tbody>
    <tr style="border-bottom:1px solid #eee">
        <td style="padding:3px 8px;font-weight:700">K Count
            <span style="font-weight:400;color:#888;font-size:10px">(proj @21 BF)</span></td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(pkc.get('mae'),' K')}</td>
        <td style="padding:3px 8px;text-align:right">{_bias_label(pkc.get('bias'))}</td>
        <td style="padding:3px 8px;text-align:right">{pkc.get('within_1','—')}%</td>
        <td style="padding:3px 8px;text-align:right">{pkc.get('within_2','—')}%</td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(pkc.get('mae_7d'),' K')}</td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(pkc.get('mae_season'),' K')}</td>
    </tr>
    <tr style="border-bottom:1px solid #eee">
        <td style="padding:3px 8px;font-weight:700">K Rate</td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(pkr.get('mae'),'%')}</td>
        <td style="padding:3px 8px;text-align:right">{_bias_label(pkr.get('bias'))}</td>
        <td style="padding:3px 8px;text-align:right">—</td>
        <td style="padding:3px 8px;text-align:right">—</td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(pkr.get('mae_7d'),'%')}</td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(pkr.get('mae_season'),'%')}</td>
    </tr>
    <tr style="border-bottom:1px solid #eee">
        <td style="padding:3px 8px;font-weight:700">ER
            <span style="font-weight:400;color:#888;font-size:10px">(OPS proxy)</span></td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(per.get('mae'),' ER')}</td>
        <td style="padding:3px 8px;text-align:right">{_bias_label(per.get('bias'))}</td>
        <td style="padding:3px 8px;text-align:right">—</td>
        <td style="padding:3px 8px;text-align:right">—</td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(per.get('mae_7d'),' ER')}</td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(per.get('mae_season'),' ER')}</td>
    </tr>
    <tr>
        <td style="padding:3px 8px;font-weight:700">IP
            <span style="font-weight:400;color:#888;font-size:10px">(actuals only)</span></td>
        <td style="padding:3px 8px;text-align:right">—</td>
        <td style="padding:3px 8px;text-align:right">—</td>
        <td style="padding:3px 8px;text-align:right">—</td>
        <td style="padding:3px 8px;text-align:right">—</td>
        <td colspan="2" style="padding:3px 8px;text-align:right;color:#555">
            avg {_fmt_mae(pip.get('mean_actual'))} inn · σ {_fmt_mae(pip.get('rmse'))}
        </td>
    </tr>
    </tbody>
</table>

<div style="font-size:12px;font-weight:700;letter-spacing:0.1em;margin-bottom:6px">
    BATTERS
    <span style="font-weight:400;color:#888;font-size:11px">
        ({btb.get('n_players','?')} qualified)
    </span>
</div>
<table style="width:100%;font-size:12px;margin-bottom:4px">
    <thead><tr>
        <th style="text-align:left">METRIC</th>
        <th>MAE</th><th>BIAS</th><th>WITHIN 1</th>
        <th>7-DAY MAE</th><th>SEASON MAE</th>
    </tr></thead>
    <tbody>
    <tr style="border-bottom:1px solid #eee">
        <td style="padding:3px 8px;font-weight:700">Total Bases</td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(btb.get('mae'),' TB')}</td>
        <td style="padding:3px 8px;text-align:right">{_bias_label(btb.get('bias'))}</td>
        <td style="padding:3px 8px;text-align:right">{btb.get('within_1','—')}%</td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(btb.get('mae_7d'),' TB')}</td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(btb.get('mae_season'),' TB')}</td>
    </tr>
    <tr style="border-bottom:1px solid #eee">
        <td style="padding:3px 8px;font-weight:700">K Rate</td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(bkr.get('mae'),'%')}</td>
        <td style="padding:3px 8px;text-align:right">{_bias_label(bkr.get('bias'))}</td>
        <td style="padding:3px 8px;text-align:right">—</td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(bkr.get('mae_7d'),'%')}</td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(bkr.get('mae_season'),'%')}</td>
    </tr>
    <tr>
        <td style="padding:3px 8px;font-weight:700">HR Rate
            <span style="font-weight:400;color:#888;font-size:10px">(Brier)</span></td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(bhr.get('mae'))}</td>
        <td style="padding:3px 8px;text-align:right">
            proj {_fmt_mae(bhr.get('mean_proj'),'%')} · act {_fmt_mae(bhr.get('mean_actual'),'%')}
        </td>
        <td style="padding:3px 8px;text-align:right">—</td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(bhr.get('mae_7d'))}</td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(bhr.get('mae_season'))}</td>
    </tr>
    </tbody>
</table>
<div style="font-size:12px;font-weight:700;letter-spacing:0.1em;margin:12px 0 6px">
    GAMES
    <span style="font-weight:400;color:#888;font-size:11px">
        ({grt.get('n_players','?')} completed games)
    </span>
</div>
<table style="width:100%;font-size:12px;margin-bottom:4px">
    <thead><tr>
        <th style="text-align:left">METRIC</th>
        <th>MAE / ACC</th><th>BIAS</th>
        <th>7-DAY</th><th>SEASON</th>
    </tr></thead>
    <tbody>
    <tr style="border-bottom:1px solid #eee">
        <td style="padding:3px 8px;font-weight:700">Total Runs
            <span style="font-weight:400;color:#888;font-size:10px">(both teams)</span></td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(grt.get('mae'),' R')}</td>
        <td style="padding:3px 8px;text-align:right">{_bias_label(grt.get('bias'))}</td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(grt.get('mae_7d'),' R')}</td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(grt.get('mae_season'),' R')}</td>
    </tr>
    <tr style="border-bottom:1px solid #eee">
        <td style="padding:3px 8px;font-weight:700">Per-Team Runs</td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(gtr.get('mae'),' R')}</td>
        <td style="padding:3px 8px;text-align:right">{_bias_label(gtr.get('bias'))}</td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(gtr.get('mae_7d'),' R')}</td>
        <td style="padding:3px 8px;text-align:right">{_fmt_mae(gtr.get('mae_season'),' R')}</td>
    </tr>
    <tr>
        <td style="padding:3px 8px;font-weight:700">Win Prediction</td>
        <td style="padding:3px 8px;text-align:right;font-weight:{'700' if gwp.get('within_1') else '400'}">
            {f"{gwp['within_1']}%" if gwp.get('within_1') else '—'}
        </td>
        <td style="padding:3px 8px;text-align:right;color:#888;font-size:10px">correct picks</td>
        <td style="padding:3px 8px;text-align:right">
            {f"{gwp['mae_7d'] and round((1 - float(gwp['mae_7d'])) * 100, 1)}%" if gwp.get('mae_7d') else '—'}
        </td>
        <td style="padding:3px 8px;text-align:right">
            {f"{gwp['mae_season'] and round((1 - float(gwp['mae_season'])) * 100, 1)}%" if gwp.get('mae_season') else '—'}
        </td>
    </tr>
    </tbody>
</table>
<div class="footnote">
    K Count: proj = K% × 21 PA (6-inn baseline) · ER: OPS-against × 4.5 × IP/9 proxy ·
    Game runs: proj_ops × 9.0 − 0.88 (starter OPS → runs, bullpen not modeled) ·
    Bias +/− = over/under projection · Brier score: lower = better HR probability calibration
</div>
<hr>
"""

    mismatch_html = ""
    for m in outlook['mismatches'][:8]:
        mismatch_html += f"""
        <div style="padding:6px 0;border-bottom:1px solid #eee;display:flex;justify-content:space-between">
            <div>
                <strong>{m.get('batter_name','—')}</strong>
                <span style="color:#888"> vs </span>
                <strong>{m.get('opp_pitcher_name','—')}</strong>
            </div>
            <div style="text-align:right">
                <span style="color:#1b5e20;font-weight:700">BATTER EDGE</span>
                <span style="color:#888;margin-left:8px">
                    J2+:{m.get('juiced2_plus','—')} / G3+:{m.get('goose3_plus','—')}
                    (gap: {m.get('mismatch_score','—')})
                </span>
            </div>
        </div>"""

    # ── Report mode and section ordering ─────────────────────────────────────
    mode = get_report_mode()
    _now_pt = datetime.now(ZoneInfo("America/Los_Angeles"))
    if mode == 'morning':
        report_label = "MORNING BRIEF"
    elif _now_pt.hour < 12:
        report_label = "MID-MORNING REPORT"
    elif _now_pt.hour < 17:
        report_label = "AFTERNOON REPORT"
    else:
        report_label = "EVENING REPORT"

    recap_block = f"""<div class="section-header">Yesterday's Final Scores</div>
<table style="width:100%;max-width:480px"><tbody>{score_rows}</tbody></table>

{f'''<div style="font-size:13px;font-weight:700;letter-spacing:0.08em;margin:16px 0 8px">
    PROJECTION ACCURACY — {yest_str.upper()}</div>
<table style="width:100%;font-size:12px;margin-bottom:4px">
    <thead><tr>
        <th style="text-align:left">PITCHER</th>
        <th>PROJ Ks</th><th>ACTUAL</th><th>DIFF</th>
    </tr></thead>
    <tbody>{accuracy_table_rows}</tbody>
</table>
<div class="footnote" style="margin-top:4px;margin-bottom:8px">
    Accuracy: <span style="color:#1b5e20">green</span> = within 1K ·
    <span style="color:#e65100">orange</span> = off by 1–2K ·
    <span style="color:#b71c1c">red</span> = off by 3+K
</div>''' if accuracy_rows else ""}

{f'''<div style="font-size:13px;font-weight:700;letter-spacing:0.08em;margin:20px 0 8px">
    BATTER TB PROJECTIONS vs ACTUALS — {yest_str.upper()}</div>
<table style="width:100%;font-size:12px;margin-bottom:4px">
    <thead><tr>
        <th style="text-align:left">BATTER</th>
        <th>PROJ TB</th><th>ACT TB</th><th>DIFF</th><th>J2+</th>
    </tr></thead>
    <tbody>{batter_tb_rows_html}</tbody>
</table>''' if batter_tb_rows_html else ""}

{f'''<div style="font-size:13px;font-weight:700;letter-spacing:0.08em;margin:20px 0 8px">
    HR PROJECTIONS (&gt;5%) vs ACTUALS — {yest_str.upper()}</div>
<table style="width:100%;font-size:12px;margin-bottom:4px">
    <thead><tr>
        <th style="text-align:left">BATTER</th>
        <th>PROJ HR%</th><th>ACT HR</th><th>J2+</th>
    </tr></thead>
    <tbody>{batter_hr_rows_html}</tbody>
</table>''' if batter_hr_rows_html else ""}

<hr>

{model_html}

<div class="section-header">Focus Team Box Scores — {yest_str}</div>
{focus_html if focus_html else
 '<div style="color:#888;font-size:12px;padding:12px 0">No focus team games yesterday.</div>'}

<hr>"""

    outlook_block = f"""<div class="section-header">Today's Outlook — {day_str}</div>

<div style="font-size:13px;font-weight:700;letter-spacing:0.08em;margin-bottom:8px">
    PITCHER STRIKEOUT PROJECTIONS</div>
{pitcher_bw_html}

<div style="font-size:13px;font-weight:700;letter-spacing:0.08em;margin-bottom:8px">
    HIGH TOTAL BASE PROJECTIONS</div>
<table style="width:100%;font-size:12px;margin-bottom:20px">
    <thead><tr>
        <th style="text-align:left">BATTER</th>
        <th style="text-align:left">OPPONENT</th>
        <th>PROJ TB</th><th>HR%</th><th>J2+</th><th>OPP pHR+</th><th>EDGE</th>
    </tr></thead>
    <tbody>{batter_rows if batter_rows else
        '<tr><td colspan="7" style="padding:8px;color:#888">No batter projections for today yet.</td></tr>'
    }</tbody>
</table>

<div style="font-size:13px;font-weight:700;letter-spacing:0.08em;margin-bottom:8px">
    GAME WIN PICKS</div>
<table style="width:100%;font-size:12px;margin-bottom:20px">
    <thead><tr>
        <th style="text-align:left">MATCHUP</th>
        <th style="text-align:left">AWAY STARTER</th>
        <th style="text-align:left">HOME STARTER</th>
        <th>PROJ</th>
        <th>TOTAL</th>
        <th>PICK</th>
    </tr></thead>
    <tbody>{
        "".join(
            f'''<tr style="border-bottom:1px solid #eee">
                <td style="padding:4px 8px;font-weight:700">{g["away_team_abbrev"]} @ {g["home_team_abbrev"]}</td>
                <td style="padding:4px 8px;color:#555">{g["away_pitcher_name"] or "—"}</td>
                <td style="padding:4px 8px;color:#555">{g["home_pitcher_name"] or "—"}</td>
                <td style="padding:4px 8px;text-align:center">{
                    f"{float(g['proj_away_runs']):.1f}−{float(g['proj_home_runs']):.1f}"
                    if g["proj_away_runs"] and g["proj_home_runs"] else "—"
                }</td>
                <td style="padding:4px 8px;text-align:center;color:#888">{
                    f"{float(g['proj_total']):.1f}" if g["proj_total"] else "—"
                }</td>
                <td style="padding:4px 8px;text-align:center;font-weight:700;color:{
                    "#1b5e20" if g["proj_winner"] not in (None,"push") else "#888"
                }">{
                    (g["away_team_abbrev"] if g["proj_winner"]=="away"
                     else g["home_team_abbrev"] if g["proj_winner"]=="home"
                     else "PUSH")
                    if g["proj_winner"] else "—"
                }</td>
            </tr>'''
            for g in today_games
        ) if today_games else
        '<tr><td colspan="6" style="padding:8px;color:#888">No game projections for today yet.</td></tr>'
    }</tbody>
</table>
<div class="footnote" style="margin-bottom:20px">
    Game runs: starter proj_ops × 9.0 − 0.88 · Bullpen not modeled · Push = &lt;0.1 run diff
</div>

<div style="font-size:13px;font-weight:700;letter-spacing:0.08em;margin-bottom:8px">
    KEY MATCHUP MISMATCHES</div>
<div style="font-size:12px;margin-bottom:20px">
    {mismatch_html if mismatch_html else
     '<div style="color:#888;padding:8px 0">No significant mismatches identified.</div>'}
</div>"""

    body_blocks = (recap_block + '\n\n' + outlook_block if mode == 'morning'
                   else outlook_block + '\n\n' + recap_block)

    # ── Data integrity status ────────────────────────────────────────────────
    if integrity_check:
        ic_ok = integrity_check['status'] == 'ok'
        ic_color = '#1b5e20' if ic_ok else '#c0392b'
        ic_bg = '#eaf4ea' if ic_ok else '#fdecea'
        ic_label = 'CLEAN' if ic_ok else integrity_check['status'].upper()
        integrity_block = f"""
<div style="margin-top:24px;padding:10px 14px;background:{ic_bg};
            border-left:3px solid {ic_color};font-size:11px">
    <span style="font-weight:700;letter-spacing:0.08em;color:{ic_color}">
        ● DATA INTEGRITY: {ic_label}
    </span>
    &nbsp;&nbsp;·&nbsp;&nbsp; {integrity_check['check_date']}
    &nbsp;&nbsp;·&nbsp;&nbsp; {integrity_check['games_checked'] or 0} games checked,
    {integrity_check['games_missing'] or 0} missing/incomplete
    ({integrity_check['games_fixed'] or 0} fixed),
    {integrity_check['pa_mismatches'] or 0} PA mismatches
    <div style="color:#555;margin-top:2px">{integrity_check['notes'] or ''}</div>
</div>"""
    else:
        integrity_block = """
<div style="margin-top:24px;padding:10px 14px;background:#f5f5f5;
            border-left:3px solid #999;font-size:11px;color:#888">
    ● DATA INTEGRITY: no check has run yet
</div>"""

    body_blocks = body_blocks + '\n\n' + integrity_block

    # ── Full page ─────────────────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<!-- Google tag (gtag.js) -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-KGQB8SMVY9"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){{dataLayer.push(arguments);}}
  gtag('js', new Date());
  gtag('config', 'G-KGQB8SMVY9');
</script>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MiaCorp Sports Brief — {day_str}</title>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{
    font-family: 'Courier New', Courier, monospace;
    background: #fffef9;
    color: #1a1a1a;
    max-width: 960px;
    margin: 0 auto;
    padding: 24px 20px;
    line-height: 1.5;
}}
.masthead {{
    text-align: center;
    border-top: 4px solid #000;
    border-bottom: 4px solid #000;
    padding: 12px 0;
    margin-bottom: 4px;
}}
.masthead-title {{
    font-size: 42px;
    font-weight: 700;
    letter-spacing: 0.15em;
    text-transform: uppercase;
}}
.masthead-sub {{
    font-size: 12px;
    letter-spacing: 0.2em;
    color: #555;
    margin-top: 4px;
}}
.dateline {{
    text-align: center;
    font-size: 11px;
    letter-spacing: 0.15em;
    color: #555;
    border-bottom: 1px solid #000;
    padding: 4px 0 8px;
    margin-bottom: 20px;
}}
.section-header {{
    font-size: 16px;
    font-weight: 700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    border-bottom: 3px double #000;
    padding-bottom: 4px;
    margin: 28px 0 14px;
}}
table {{ border-collapse: collapse; font-family: 'Courier New', monospace; }}
th {{
    text-align: right;
    font-size: 10px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    padding: 3px 8px;
    border-bottom: 2px solid #000;
}}
th:first-child {{ text-align: left; }}
.footnote {{
    font-size: 10px;
    color: #888;
    margin-top: 20px;
    letter-spacing: 0.05em;
    border-top: 1px solid #ccc;
    padding-top: 8px;
}}
hr {{ border: none; border-top: 1px solid #ccc; margin: 20px 0; }}
</style>
</head>
<body>

<div class="masthead">
    <div class="masthead-title">MIACORP SPORTS BRIEF</div>
    <div class="masthead-sub">ANALYTICS · PROJECTIONS · MARKET INTELLIGENCE</div>
    <div style="margin-top:8px">
        <a href="/mlb-dashboard"
           style="font-family:'Courier New',monospace;font-size:11px;
                  color:#1a3a5c;text-decoration:none;letter-spacing:0.1em;
                  border:1px solid #1a3a5c;padding:4px 12px;border-radius:3px">
            → MLB DASHBOARD
        </a>
    </div>
</div>
<div class="dateline">
    {day_str} &nbsp;·&nbsp; {report_label} &nbsp;·&nbsp; YESTERDAY: {yest_str}
</div>

{body_blocks}

<div class="footnote">
    Generated {datetime.now().strftime('%Y-%m-%d %H:%M ET')} · MiaCorp Analytics ·
    Models: pK+ (K rate r=+0.560), pHR+ (HR rate r=−0.357), Juiced+2 (SLG r=+0.558)
</div>

</body>
</html>"""
    return html


def main():
    today     = date.today()
    yesterday = today - timedelta(days=1)

    db = SessionLocal()
    try:
        print(f"Generating homepage for {today} (yesterday: {yesterday})...")
        html = render_homepage(yesterday, today, db)

        output_path = "/app/static/homepage.html"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w") as f:
            f.write(html)

        print(f"  Written to {output_path} ({len(html):,} chars)")
    finally:
        db.close()


if __name__ == "__main__":
    main()
