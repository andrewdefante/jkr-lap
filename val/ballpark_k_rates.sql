---- CUT 1A: Team Pitching Home/Road Splits ----
SELECT
    bp.team_id,
    CASE WHEN g.home_team_id = bp.team_id THEN g.home_team_abbrev
         ELSE g.away_team_abbrev END as team_abbrev,
    CASE WHEN g.home_team_id = bp.team_id THEN 'home' ELSE 'away' END as split,
    COUNT(DISTINCT bp.game_pk) as games,
    SUM(bp.strikeouts) as k,
    SUM(bp.batters_faced) as bf,
    ROUND(SUM(bp.strikeouts)::numeric / NULLIF(SUM(bp.batters_faced), 0), 4) as k_rate
FROM mlb.game_lineups gl
JOIN mlb.games g ON g.game_pk = gl.game_pk
JOIN mlb.boxscore_pitching bp ON bp.game_pk = gl.game_pk
WHERE g.season = 2026 AND g.game_type = 'R'
AND bp.batters_faced > 0
GROUP BY bp.team_id, team_abbrev, split
ORDER BY team_abbrev, split;


---- CUT 1B: Team Batting Home/Road Splits ----
SELECT
    bb.team_id,
    CASE WHEN g.home_team_id = bb.team_id THEN g.home_team_abbrev
         ELSE g.away_team_abbrev END as team_abbrev,
    CASE WHEN g.home_team_id = bb.team_id THEN 'home' ELSE 'away' END as split,
    COUNT(DISTINCT bb.game_pk) as games,
    SUM(bb.strikeouts) as k,
    SUM(bb.plate_appearances) as pa,
    ROUND(SUM(bb.strikeouts)::numeric / NULLIF(SUM(bb.plate_appearances), 0), 4) as k_rate
FROM mlb.game_lineups gl
JOIN mlb.games g ON g.game_pk = gl.game_pk
JOIN mlb.boxscore_batting bb ON bb.game_pk = gl.game_pk
WHERE g.season = 2026 AND g.game_type = 'R'
AND bb.plate_appearances > 0
GROUP BY bb.team_id, team_abbrev, split
ORDER BY team_abbrev, split;


---- CUT 2A: Team Pitching at Each Specific Ballpark ----
SELECT
    bp.team_id,
    CASE WHEN g.home_team_id = bp.team_id THEN g.home_team_abbrev
         ELSE g.away_team_abbrev END as team_abbrev,
    g.home_team_id as site_id,
    g.home_team_abbrev as ballpark,
    CASE WHEN g.home_team_id = bp.team_id THEN 'home' ELSE 'away' END as split,
    COUNT(DISTINCT bp.game_pk) as games,
    SUM(bp.strikeouts) as k,
    SUM(bp.batters_faced) as bf,
    ROUND(SUM(bp.strikeouts)::numeric / NULLIF(SUM(bp.batters_faced), 0), 4) as k_rate
FROM mlb.game_lineups gl
JOIN mlb.games g ON g.game_pk = gl.game_pk
JOIN mlb.boxscore_pitching bp ON bp.game_pk = gl.game_pk
WHERE g.season = 2026 AND g.game_type = 'R'
AND bp.batters_faced > 0
GROUP BY bp.team_id, team_abbrev, site_id, ballpark, split
ORDER BY team_abbrev, ballpark;


---- CUT 2B: Team Batting at Each Specific Ballpark ----
SELECT
    bb.team_id,
    CASE WHEN g.home_team_id = bb.team_id THEN g.home_team_abbrev
         ELSE g.away_team_abbrev END as team_abbrev,
    g.home_team_id as site_id,
    g.home_team_abbrev as ballpark,
    CASE WHEN g.home_team_id = bb.team_id THEN 'home' ELSE 'away' END as split,
    COUNT(DISTINCT bb.game_pk) as games,
    SUM(bb.strikeouts) as k,
    SUM(bb.plate_appearances) as pa,
    ROUND(SUM(bb.strikeouts)::numeric / NULLIF(SUM(bb.plate_appearances), 0), 4) as k_rate
FROM mlb.game_lineups gl
JOIN mlb.games g ON g.game_pk = gl.game_pk
JOIN mlb.boxscore_batting bb ON bb.game_pk = gl.game_pk
WHERE g.season = 2026 AND g.game_type = 'R'
AND bb.plate_appearances > 0
GROUP BY bb.team_id, team_abbrev, site_id, ballpark, split
ORDER BY team_abbrev, ballpark;
