---- QUERY 1: Pitcher season K rate ----
SELECT
    bp.player_id,
    SUM(bp.strikeouts) as k,
    SUM(bp.batters_faced) as bf,
    ROUND(SUM(bp.strikeouts)::numeric / NULLIF(SUM(bp.batters_faced), 0), 4) as season_k_rate
FROM mlb.boxscore_pitching bp
JOIN mlb.games g ON g.game_pk = bp.game_pk
WHERE g.season = 2026
AND g.game_type = 'R'
AND bp.games_started = 1
GROUP BY bp.player_id
ORDER BY season_k_rate DESC;


---- QUERY 2: Opposing team season K rate ----
SELECT
    CASE WHEN g.home_team_id = bb.team_id THEN g.home_team_id ELSE g.away_team_id END as team_id,
    CASE WHEN g.home_team_id = bb.team_id THEN g.home_team_abbrev ELSE g.away_team_abbrev END as team_abbrev,
    SUM(bb.plate_appearances) as pa,
    SUM(bb.strikeouts) as k,
    ROUND(SUM(bb.strikeouts)::numeric / NULLIF(SUM(bb.plate_appearances), 0), 4) as season_k_rate
FROM mlb.boxscore_batting bb
JOIN mlb.games g ON g.game_pk = bb.game_pk
WHERE g.season = 2026
AND g.game_type = 'R'
AND bb.plate_appearances > 0
GROUP BY 1, 2
ORDER BY season_k_rate DESC;


---- QUERY 3: Pitcher rolling 3-game K rate ----
WITH p_game_number AS (
    SELECT
        bp.player_id,
        bp.game_pk,
        ROW_NUMBER() OVER (PARTITION BY bp.player_id ORDER BY g.game_date ASC, bp.game_pk ASC) as game_number
    FROM mlb.boxscore_pitching bp
    JOIN mlb.games g ON g.game_pk = bp.game_pk
    WHERE g.season = 2026
    AND g.game_type = 'R'
    AND bp.games_started = 1
    AND bp.batters_faced > 0
)
, rolling_last_3 AS (
    SELECT
        player_id,
        game_pk,
        game_number,
        GREATEST(0, game_number - 3) as game_lookback_floor
    FROM p_game_number
)
, pitcher_k_lookback AS (
    SELECT DISTINCT
        pgn.player_id,
        rl3.game_pk,
        rl3.game_number,
        rl3.game_lookback_floor,
        pgn.game_number as lookback_game_numbers,
        pgn.game_pk as lookback_game_pk
    FROM p_game_number pgn
    INNER JOIN rolling_last_3 rl3
        ON pgn.player_id = rl3.player_id
        AND pgn.game_number >= rl3.game_lookback_floor
        AND pgn.game_number < rl3.game_number
)
, pitcher_k_game_level AS (
    SELECT
        pkl.player_id,
        pkl.game_pk,
        pkl.game_number,
        bp.batters_faced,
        bp.strikeouts
    FROM pitcher_k_lookback pkl
    INNER JOIN mlb.boxscore_pitching bp
        ON pkl.player_id = bp.player_id
        AND pkl.lookback_game_pk = bp.game_pk
)
, roll_last3_k AS (
    SELECT
        player_id,
        game_pk,
        SUM(batters_faced) as roll_bf,
        SUM(strikeouts) as roll_k,
        ROUND(SUM(strikeouts)::numeric / NULLIF(SUM(batters_faced), 0), 4) as roll_3g_k_rate
    FROM pitcher_k_game_level
    GROUP BY player_id, game_pk
)
SELECT
    bp.player_id,
    bp.game_pk,
    g.game_date,
    bp.strikeouts as actual_k,
    bp.batters_faced as actual_bf,
    ROUND(bp.strikeouts::numeric / NULLIF(bp.batters_faced, 0), 4) as actual_k_rate,
    rl3.roll_bf,
    rl3.roll_k,
    rl3.roll_3g_k_rate
FROM mlb.boxscore_pitching bp
JOIN mlb.games g ON g.game_pk = bp.game_pk
INNER JOIN roll_last3_k rl3
    ON bp.player_id = rl3.player_id
    AND bp.game_pk = rl3.game_pk
WHERE g.season = 2026
AND g.game_type = 'R'
AND bp.games_started = 1
ORDER BY bp.game_pk DESC;


---- QUERY 4: Opposing team rolling 10-game K rate ----
WITH team_game_number AS (
    SELECT
        CASE WHEN g.home_team_id = bb.team_id THEN g.home_team_id ELSE g.away_team_id END as team_id,
        bb.game_pk,
        ROW_NUMBER() OVER (
            PARTITION BY CASE WHEN g.home_team_id = bb.team_id THEN g.home_team_id ELSE g.away_team_id END
            ORDER BY g.game_date ASC, bb.game_pk ASC
        ) as game_number
    FROM mlb.boxscore_batting bb
    JOIN mlb.games g ON g.game_pk = bb.game_pk
    WHERE g.season = 2026
    AND g.game_type = 'R'
    AND bb.plate_appearances > 0
    GROUP BY 1, 2, g.game_date
)
, rolling_last_10 AS (
    SELECT
        team_id,
        game_pk,
        game_number,
        GREATEST(0, game_number - 10) as game_lookback_floor
    FROM team_game_number
)
, team_k_lookback AS (
    SELECT DISTINCT
        tgn.team_id,
        rl10.game_pk,
        rl10.game_number,
        rl10.game_lookback_floor,
        tgn.game_number as lookback_game_numbers,
        tgn.game_pk as lookback_game_pk
    FROM team_game_number tgn
    INNER JOIN rolling_last_10 rl10
        ON tgn.team_id = rl10.team_id
        AND tgn.game_number >= rl10.game_lookback_floor
        AND tgn.game_number < rl10.game_number
)
, team_k_game_level AS (
    SELECT
        tkl.team_id,
        tkl.game_pk,
        tkl.game_number,
        tkl.lookback_game_pk,
        SUM(bb.plate_appearances) as pa,
        SUM(bb.strikeouts) as strikeouts
    FROM team_k_lookback tkl
    INNER JOIN mlb.boxscore_batting bb
        ON tkl.team_id = bb.team_id
        AND tkl.lookback_game_pk = bb.game_pk
    GROUP BY 1, 2, 3, 4
)
, roll_last10_k AS (
    SELECT
        team_id,
        game_pk,
        SUM(pa) as roll_pa,
        SUM(strikeouts) as roll_k,
        ROUND(SUM(strikeouts)::numeric / NULLIF(SUM(pa), 0), 4) as roll_10g_k_rate
    FROM team_k_game_level
    GROUP BY team_id, game_pk
)
SELECT
    bb.team_id,
    CASE WHEN g.home_team_id = bb.team_id THEN g.home_team_abbrev ELSE g.away_team_abbrev END as team_abbrev,
    bb.game_pk,
    g.game_date,
    SUM(bb.strikeouts) as actual_k,
    SUM(bb.plate_appearances) as actual_pa,
    ROUND(SUM(bb.strikeouts)::numeric / NULLIF(SUM(bb.plate_appearances), 0), 4) as actual_k_rate,
    rl10.roll_pa,
    rl10.roll_k,
    rl10.roll_10g_k_rate
FROM mlb.boxscore_batting bb
JOIN mlb.games g ON g.game_pk = bb.game_pk
INNER JOIN roll_last10_k rl10
    ON bb.team_id = rl10.team_id
    AND bb.game_pk = rl10.game_pk
WHERE g.season = 2026
AND g.game_type = 'R'
AND bb.plate_appearances > 0
GROUP BY bb.team_id, 2, bb.game_pk, g.game_date, rl10.roll_pa, rl10.roll_k, rl10.roll_10g_k_rate
ORDER BY bb.game_pk DESC;
