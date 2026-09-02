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
    WHERE g.game_date = '2026-09-02'
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
    WHERE g.game_date::date BETWEEN CURRENT_DATE - INTERVAL '22 days'
                                AND CURRENT_DATE - INTERVAL '1 day'
      AND g.season = 2026 AND g.game_type = 'R'
      AND bp.games_started = 1 AND bp.batters_faced > 0
),
pitcher_ranked AS (
    SELECT pgs.*,
        ROW_NUMBER() OVER (
            PARTITION BY pitcher_id ORDER BY game_date DESC, game_pk DESC
        ) AS rn
    FROM pitcher_game_stats pgs
),
pitcher_last3 AS (
    SELECT
        pitcher_id,
        SUM(pitcher_ks)::numeric / NULLIF(SUM(pitcher_bf), 0) AS pitcher_k_pct_3w,
        ROUND(AVG(pitcher_bf), 2) AS pitcher_avg_bf_3w,
        COUNT(*) AS pitcher_starts_3w,
        ROUND(STDDEV_SAMP(pitcher_k_pct), 4) AS pitcher_k_pct_sd_3starts,
        ROUND(STDDEV_SAMP(pitcher_bf), 2) AS pitcher_bf_sd_3starts,
        -- Last 3 game K totals, BF, dates for the pitcher
        STRING_AGG(pitcher_ks::text, ', ' ORDER BY game_date DESC, game_pk DESC) AS pitcher_last3_k,
        STRING_AGG(pitcher_bf::text, ', ' ORDER BY game_date DESC, game_pk DESC) AS pitcher_last3_bf,
        STRING_AGG(game_date::text, ', ' ORDER BY game_date DESC, game_pk DESC) AS pitcher_last3_dates
    FROM pitcher_ranked
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
    WHERE g.game_date::date BETWEEN CURRENT_DATE - INTERVAL '22 days'
                                AND CURRENT_DATE - INTERVAL '1 day'
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
        s.pitcher_id, s.game_pk,
        p3w.pitcher_k_pct_3w, p3w.pitcher_avg_bf_3w, p3w.pitcher_starts_3w,
        p3w.pitcher_k_pct_sd_3starts, p3w.pitcher_bf_sd_3starts,
        p3w.pitcher_last3_k, p3w.pitcher_last3_bf, p3w.pitcher_last3_dates,
        opp.opp_k_pct AS opp_k_pct_vs_this_hand,
        opp.opp_avg_pa AS opp_avg_pa_vs_this_hand,
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
    LEFT JOIN pitcher_last3 p3w ON p3w.pitcher_id = s.pitcher_id
    LEFT JOIN opp_last3 opp
        ON opp.opp_team_id = s.opp_team_id
        AND opp.pitch_hand = ph.pitch_hand
),
actuals AS (
    SELECT
        bp.player_id AS pitcher_id,
        bp.game_pk,
        bp.strikeouts AS actual_k,
        bp.batters_faced AS actual_bf,
        ROUND(bp.strikeouts::numeric / NULLIF(bp.batters_faced, 0), 4) AS actual_k_pct,
        ROUND(bp.outs::numeric / 3, 2) AS actual_ip
    FROM mlb.boxscore_pitching bp
    JOIN mlb.games g ON g.game_pk = bp.game_pk
    WHERE g.game_date = '2026-09-02'
      AND g.game_type = 'R'
      AND bp.games_started = 1
      AND bp.batters_faced > 0
),
final AS (
    SELECT
        p.pitcher_name, p.team_abbrev, p.pitch_hand, p.opp_abbrev,
        ROUND(p.pit::numeric, 2) AS pit,
        ROUND(p.opp_proj::numeric, 2) AS opp,
        ROUND(p.proj::numeric, 2) AS proj,
        ROUND((0.5 * SQRT(POWER(p.sd_pit, 2) + POWER(p.sd_opp, 2)))::numeric, 2) AS proj_sd,
        ROUND(GREATEST(0, p.proj - 2.576 * 0.5 * SQRT(POWER(p.sd_pit, 2) + POWER(p.sd_opp, 2)))::numeric, 2) AS whisker_low,
        ROUND(GREATEST(0, p.proj - 0.674 * 0.5 * SQRT(POWER(p.sd_pit, 2) + POWER(p.sd_opp, 2)))::numeric, 2) AS q1,
        ROUND(p.proj::numeric, 2) AS median,
        ROUND((p.proj + 0.674 * 0.5 * SQRT(POWER(p.sd_pit, 2) + POWER(p.sd_opp, 2)))::numeric, 2) AS q3,
        ROUND((p.proj + 2.576 * 0.5 * SQRT(POWER(p.sd_pit, 2) + POWER(p.sd_opp, 2)))::numeric, 2) AS whisker_high,
        a.actual_k,
        a.actual_bf,
        a.actual_k_pct,
        a.actual_ip,
        ROUND((a.actual_k - p.proj)::numeric, 2) AS residual,
        ROUND(ABS(a.actual_k - p.proj)::numeric, 2) AS abs_residual,
        CASE WHEN a.actual_k BETWEEN
            GREATEST(0, p.proj - 0.674 * 0.5 * SQRT(POWER(p.sd_pit, 2) + POWER(p.sd_opp, 2)))
            AND (p.proj + 0.674 * 0.5 * SQRT(POWER(p.sd_pit, 2) + POWER(p.sd_opp, 2)))
            THEN 'YES' ELSE 'NO' END AS in_iqr,
        CASE WHEN a.actual_k BETWEEN
            GREATEST(0, p.proj - 2.576 * 0.5 * SQRT(POWER(p.sd_pit, 2) + POWER(p.sd_opp, 2)))
            AND (p.proj + 2.576 * 0.5 * SQRT(POWER(p.sd_pit, 2) + POWER(p.sd_opp, 2)))
            THEN 'YES' ELSE 'NO' END AS in_99pct,
        p.pitcher_k_pct_sd_3starts, p.pitcher_bf_sd_3starts,
        p.pitcher_last3_k, p.pitcher_last3_bf, p.pitcher_last3_dates,
        p.opp_k_pct_sd_3games, p.opp_pa_sd_3games,
        p.last3_pa, p.last3_k, p.last3_dates
    FROM projections p
    LEFT JOIN actuals a
        ON a.pitcher_id = p.pitcher_id
        AND a.game_pk = p.game_pk
)
SELECT
    pitcher_name
    , team_abbrev
    , opp_abbrev
    ,pit
    ,opp
    , proj
    , proj_sd
    ,whisker_low
    , q1
    , median
    , q3
    , whisker_high,
    actual_k, 
    actual_bf
    , actual_ip
    , actual_k_pct
    --residual
    --, abs_residual
    --in_iqr
    --, in_99pct
    --pitcher_k_pct_sd_3starts
    --, pitcher_bf_sd_3starts
    ,pitcher_last3_k
    --, pitcher_last3_bf
    , pitcher_last3_dates
    --opp_k_pct_sd_3games
    --, opp_pa_sd_3games
    --,last3_pa
    , last3_k
    , last3_dates
FROM final
ORDER BY proj DESC NULLS LAST;