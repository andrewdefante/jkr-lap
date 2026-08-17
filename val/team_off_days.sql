-- Days since last game for the OPPOSING batting team before each game
-- Proxy for rest/fatigue effect on K rate
WITH team_game_dates AS (
    SELECT
        CASE WHEN g.home_team_id = bb.team_id THEN g.home_team_id
             ELSE g.away_team_id END as team_id,
        g.game_pk,
        g.game_date,
        LAG(g.game_date) OVER (
            PARTITION BY CASE WHEN g.home_team_id = bb.team_id 
                              THEN g.home_team_id ELSE g.away_team_id END
            ORDER BY g.game_date
        ) as prev_game_date
    FROM mlb.boxscore_batting bb
    JOIN mlb.games g ON g.game_pk = bb.game_pk
    WHERE g.season = 2026 AND g.game_type = 'R'
    AND bb.plate_appearances > 0
    GROUP BY 1, g.game_pk, g.game_date
)
SELECT
    team_id,
    game_pk,
    game_date,
    prev_game_date,
    (game_date - prev_game_date) as days_since_last_game,
    CASE WHEN (game_date - prev_game_date) > 1 THEN 1 ELSE 0 END as had_off_day
FROM team_game_dates
ORDER BY team_id, game_date;
