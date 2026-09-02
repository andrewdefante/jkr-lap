WITH pitcher_hand AS (
    SELECT pitcher_id, MAX(pitch_hand) AS pitch_hand
    FROM mlb.at_bats WHERE pitch_hand IS NOT NULL
    GROUP BY pitcher_id
),
today_starters AS (
    SELECT
        g.game_pk, g.game_date,
        g.home_probable_pitcher_id AS home_pid,
        g.home_probable_pitcher_name AS home_name,
        g.home_team_id, g.home_team_abbrev,
        g.away_team_id, g.away_team_abbrev,
        g.away_probable_pitcher_id AS away_pid,
        g.away_probable_pitcher_name AS away_name
    FROM mlb.games g
    WHERE g.game_date = '2026-08-27'
      AND g.game_type = 'R'
      AND (g.home_probable_pitcher_id IS NOT NULL
           OR g.away_probable_pitcher_id IS NOT NULL)
),
starters AS (
    SELECT game_pk, home_pid AS pitcher_id, home_name AS pitcher_name,
           home_team_id AS team_id, home_team_abbrev AS team_abbrev,
           away_team_id AS opp_team_id, away_team_abbrev AS opp_abbrev, 'home' AS side
    FROM today_starters WHERE home_pid IS NOT NULL
    UNION ALL
    SELECT game_pk, away_pid AS pitcher_id, away_name AS pitcher_name,
           away_team_id AS team_id, away_team_abbrev AS team_abbrev,
           home_team_id AS opp_team_id, home_team_abbrev AS opp_abbrev, 'away' AS side
    FROM today_starters WHERE away_pid IS NOT NULL
),
pitcher_game_stats AS (
    SELECT
        bp.player_id AS pitcher_id, bp.game_pk, g.game_date,
        bp.strikeouts AS pitcher_ks, bp.batters_faced AS pitcher_bf,
        bp.strikeouts::numeric / NULLIF(bp.batters_faced, 0) AS pitcher_k_pct
    FROM mlb.boxscore_pitching bp
    JOIN mlb.games g ON g.game_pk = bp.game_pk
    WHERE g.game_date::date BETWEEN (CURRENT_DATE-2) - INTERVAL '21 days'
                                AND (CURRENT_DATE-2) - INTERVAL '1 day'
      AND g.season = 2026 AND g.game_type = 'R'
      AND bp.games_started = 1 AND bp.batters_faced > 0
),
pitcher_last3 AS (
    SELECT
        pitcher_id,
        SUM(pitcher_ks)::numeric / NULLIF(SUM(pitcher_bf), 0) AS pitcher_k_pct_3w,
        ROUND(AVG(pitcher_bf), 2) AS pitcher_avg_bf_3w,
        COUNT(*) AS pitcher_starts_3w,
        ROUND(STDDEV_SAMP(pitcher_k_pct), 4) AS pitcher_k_pct_sd_3starts,
        ROUND(STDDEV_SAMP(pitcher_bf), 2) AS pitcher_bf_sd_3starts
    FROM (
        SELECT pgs.*,
            ROW_NUMBER() OVER (
                PARTITION BY pitcher_id ORDER BY game_date DESC, game_pk DESC
            ) AS rn
        FROM pitcher_game_stats pgs
    ) x
    WHERE rn <= 3
    GROUP BY pitcher_id
),
historical_starters AS (
    SELECT DISTINCT
        bp.game_pk, g.game_date, bp.player_id AS pitcher_id,
        bp.team_id AS pitcher_team_id,
        CASE WHEN g.home_team_id = bp.team_id THEN g.away_team_id
             ELSE g.home_team_id END AS opp_team_id,
        ph.pitch_hand
    FROM mlb.boxscore_pitching bp
    JOIN mlb.games g ON g.game_pk = bp.game_pk
    JOIN pitcher_hand ph ON ph.pitcher_id = bp.player_id
    WHERE g.game_date::date BETWEEN (CURRENT_DATE-2) - INTERVAL '21 days'
                                AND (CURRENT_DATE-2) - INTERVAL '1 day'
      AND g.season = 2026 AND g.game_type = 'R'
      AND bp.games_started = 1 AND bp.batters_faced > 0
),
opp_game_vs_sp_raw AS (
    SELECT
        hs.opp_team_id, hs.game_pk, hs.game_date, hs.pitcher_id, hs.pitch_hand,
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
    GROUP BY hs.opp_team_id, hs.game_pk, hs.game_date, hs.pitcher_id, hs.pitch_hand
),
opp_game_vs_sp AS (
    SELECT opp_team_id, game_pk, game_date, pitch_hand,
           SUM(pa) AS pa, SUM(k) AS k
    FROM opp_game_vs_sp_raw
    GROUP BY opp_team_id, game_pk, game_date, pitch_hand
),
opp_ranked AS (
    SELECT og.*,
        ROW_NUMBER() OVER (
            PARTITION BY opp_team_id, pitch_hand
            ORDER BY game_date DESC, game_pk DESC
        ) AS rn
    FROM opp_game_vs_sp og
),
opp_last3 AS (
    SELECT
        opp_team_id, pitch_hand,
        SUM(k)::numeric / NULLIF(SUM(pa), 0) AS opp_k_pct,
        ROUND(AVG(pa), 2) AS opp_avg_pa,
        COUNT(*) AS opp_games,
        ROUND(STDDEV_SAMP(k::numeric / NULLIF(pa, 0)), 4) AS opp_k_pct_sd_3games,
        ROUND(STDDEV_SAMP(pa), 2) AS opp_pa_sd_3games,
        STRING_AGG(pa::text, ', ' ORDER BY game_date DESC, game_pk DESC) AS last3_pa,
        STRING_AGG(k::text, ', ' ORDER BY game_date DESC, game_pk DESC) AS last3_k,
        STRING_AGG(game_date::text, ', ' ORDER BY game_date DESC, game_pk DESC) AS last3_dates
    FROM opp_ranked
    WHERE rn <= 3
    GROUP BY opp_team_id, pitch_hand
),
projections AS (
    SELECT
        s.pitcher_name, s.team_abbrev, ph.pitch_hand, s.opp_abbrev,
        p3w.pitcher_k_pct_3w, p3w.pitcher_avg_bf_3w, p3w.pitcher_starts_3w,
        p3w.pitcher_k_pct_sd_3starts, p3w.pitcher_bf_sd_3starts,
        opp.opp_k_pct AS opp_k_pct_vs_this_hand,
        opp.opp_avg_pa AS opp_avg_pa_vs_this_hand,
        opp.opp_k_pct_sd_3games, opp.opp_pa_sd_3games,
        opp.last3_pa, opp.last3_k, opp.last3_dates,

        -- Pitcher projected K count
        p3w.pitcher_k_pct_3w * p3w.pitcher_avg_bf_3w AS pit,

        -- Opponent projected K count
        opp.opp_k_pct * opp.opp_avg_pa AS opp_proj,

        -- Blended projection
        (
            p3w.pitcher_k_pct_3w * p3w.pitcher_avg_bf_3w
            + COALESCE(opp.opp_k_pct * opp.opp_avg_pa,
                       p3w.pitcher_k_pct_3w * p3w.pitcher_avg_bf_3w)
        ) / 2.0 AS proj,

        -- SD of pitcher K count: sqrt(bf² × k_pct_sd² + k_pct² × bf_sd²)
        SQRT(
            POWER(COALESCE(p3w.pitcher_avg_bf_3w, 21.0), 2)
                * POWER(COALESCE(p3w.pitcher_k_pct_sd_3starts, 0.05), 2)
            + POWER(COALESCE(p3w.pitcher_k_pct_3w, 0.22), 2)
                * POWER(COALESCE(p3w.pitcher_bf_sd_3starts, 4.5), 2)
        ) AS sd_pit,

        -- SD of opponent K count: sqrt(pa² × k_pct_sd² + k_pct² × pa_sd²)
        SQRT(
            POWER(COALESCE(opp.opp_avg_pa, 21.0), 2)
                * POWER(COALESCE(opp.opp_k_pct_sd_3games, 0.05), 2)
            + POWER(COALESCE(opp.opp_k_pct, 0.22), 2)
                * POWER(COALESCE(opp.opp_pa_sd_3games, 4.5), 2)
        ) AS sd_opp

    FROM starters s
    LEFT JOIN pitcher_hand ph ON ph.pitcher_id = s.pitcher_id
    LEFT JOIN pitcher_last3 p3w ON p3w.pitcher_id = s.pitcher_id
    LEFT JOIN opp_last3 opp
        ON opp.opp_team_id = s.opp_team_id
        AND opp.pitch_hand = ph.pitch_hand
),
final AS (
    SELECT
        pitcher_name, team_abbrev, pitch_hand, opp_abbrev,
        ROUND(pit::numeric, 2) AS pit,
        ROUND(opp_proj::numeric, 2) AS opp,
        ROUND(proj::numeric, 2) AS proj,

        -- Combined SD: (1/2) × sqrt(sd_pit² + sd_opp²)
        ROUND(
            (0.5 * SQRT(POWER(sd_pit, 2) + POWER(sd_opp, 2)))::numeric
        , 2) AS proj_sd,

        -- Box plot values using combined SD
        ROUND(GREATEST(0, proj - 2.576 * 0.5 * SQRT(POWER(sd_pit, 2) + POWER(sd_opp, 2)))::numeric, 2) AS whisker_low,
        ROUND(GREATEST(0, proj - 0.674 * 0.5 * SQRT(POWER(sd_pit, 2) + POWER(sd_opp, 2)))::numeric, 2) AS q1,
        ROUND(proj::numeric, 2) AS median,
        ROUND((proj + 0.674 * 0.5 * SQRT(POWER(sd_pit, 2) + POWER(sd_opp, 2)))::numeric, 2) AS q3,
        ROUND((proj + 2.576 * 0.5 * SQRT(POWER(sd_pit, 2) + POWER(sd_opp, 2)))::numeric, 2) AS whisker_high,

        pitcher_k_pct_sd_3starts, pitcher_bf_sd_3starts,
        opp_k_pct_sd_3games, opp_pa_sd_3games,
        last3_pa, last3_k, last3_dates
    FROM projections
)
SELECT
    pitcher_name, team_abbrev, opp_abbrev,
    pit, opp, proj, proj_sd,
    whisker_low, q1, median, q3, whisker_high,
    pitcher_k_pct_sd_3starts, pitcher_bf_sd_3starts,
    opp_k_pct_sd_3games, opp_pa_sd_3games,
    last3_pa, last3_k, last3_dates
FROM final
ORDER BY proj DESC NULLS LAST;