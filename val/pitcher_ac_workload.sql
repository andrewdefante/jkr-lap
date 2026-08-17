---- Pitcher Acute-Chronic Workload ----
-- AC = acute_load (last 7 days before game) / chronic_load (28-day avg before game)
-- Calculated per pitcher per game using only prior game data (no lookahead)
-- Three AC variants: BF-based, Pitches-based, Outs-based

WITH pitcher_games AS (
    -- All starter appearances with workload metrics
    SELECT
        bp.player_id,
        bp.game_pk,
        g.game_date::date as game_date,
        bp.batters_faced,
        bp.pitches_thrown as pitches,
        bp.outs,
        bp.innings_pitched
    FROM mlb.boxscore_pitching bp
    JOIN mlb.games g ON g.game_pk = bp.game_pk
    WHERE g.season = 2026
    AND g.game_type = 'R'
    AND bp.games_started = 1
    AND bp.batters_faced > 0
)
, ac_components AS (
    SELECT
        curr.player_id,
        curr.game_pk,
        curr.game_date,
        curr.batters_faced as game_bf,
        curr.pitches as game_pitches,
        curr.outs as game_outs,

        -- ACUTE: sum of workload in 7 days BEFORE this game (not including this game)
        SUM(CASE WHEN prior.game_date >= curr.game_date - INTERVAL '7 days'
                  AND prior.game_date < curr.game_date
                 THEN prior.batters_faced ELSE 0 END) as acute_bf,

        SUM(CASE WHEN prior.game_date >= curr.game_date - INTERVAL '7 days'
                  AND prior.game_date < curr.game_date
                 THEN prior.pitches ELSE 0 END) as acute_pitches,

        SUM(CASE WHEN prior.game_date >= curr.game_date - INTERVAL '7 days'
                  AND prior.game_date < curr.game_date
                 THEN prior.outs ELSE 0 END) as acute_outs,

        -- CHRONIC: sum of workload in 28 days BEFORE this game / 4 (weekly avg)
        SUM(CASE WHEN prior.game_date >= curr.game_date - INTERVAL '28 days'
                  AND prior.game_date < curr.game_date
                 THEN prior.batters_faced ELSE 0 END) / 4.0 as chronic_bf,

        SUM(CASE WHEN prior.game_date >= curr.game_date - INTERVAL '28 days'
                  AND prior.game_date < curr.game_date
                 THEN prior.pitches ELSE 0 END) / 4.0 as chronic_pitches,

        SUM(CASE WHEN prior.game_date >= curr.game_date - INTERVAL '28 days'
                  AND prior.game_date < curr.game_date
                 THEN prior.outs ELSE 0 END) / 4.0 as chronic_outs,

        -- Count of prior starts in window
        COUNT(CASE WHEN prior.game_date >= curr.game_date - INTERVAL '28 days'
                    AND prior.game_date < curr.game_date
                   THEN 1 END) as prior_starts_28d,

        COUNT(CASE WHEN prior.game_date >= curr.game_date - INTERVAL '7 days'
                    AND prior.game_date < curr.game_date
                   THEN 1 END) as prior_starts_7d

    FROM pitcher_games curr
    LEFT JOIN pitcher_games prior
        ON curr.player_id = prior.player_id
        AND prior.game_date < curr.game_date
    GROUP BY
        curr.player_id,
        curr.game_pk,
        curr.game_date,
        curr.batters_faced,
        curr.pitches,
        curr.outs
)
SELECT
    player_id,
    game_pk,
    game_date,
    game_bf,
    game_pitches,
    game_outs,
    acute_bf,
    acute_pitches,
    acute_outs,
    chronic_bf,
    chronic_pitches,
    chronic_outs,
    prior_starts_7d,
    prior_starts_28d,

    -- AC RATIOS (null-safe — avoid division by zero on early season starts)
    ROUND(acute_bf / NULLIF(chronic_bf, 0), 4) as ac_bf,
    ROUND(acute_pitches / NULLIF(chronic_pitches, 0), 4) as ac_pitches,
    ROUND(acute_outs / NULLIF(chronic_outs, 0), 4) as ac_outs,

    -- Days rest since last start
    game_date - LAG(game_date) OVER (PARTITION BY player_id ORDER BY game_date) as days_rest

FROM ac_components
ORDER BY player_id, game_date;
