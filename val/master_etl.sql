-- calculate daily player performance - tie player id to game id to determine game counts across the current season. this should be used to count # of games per season 
with bat_game as 
(
	select distinct 
		game_pk
		,player_id
		,team_id
	from sample_boxscore_batting 
) 
, pit_game as 
( 
	select distinct 
		game_pk
		,player_id
		,team_id
	from sample_boxscore_pitching
)
, fld_game as 
(
	select distinct 
		game_pk
		,player_id
		,team_id
	from sample_boxscore_fielding
)
, rn_game as 
(
	select distinct 
		game_pk
		,runner_id as player_id
		-- ,team_id
	from sample_runners sr 
)
, player_game_season_union as 
(
	select distinct 
		game_pk
		,player_id
		,'1' as player_g
	from bat_game
	union 
	select distinct 
		game_pk
		,player_id
		,'1' as player_g
	from pit_game
	union
	select distinct 
		game_pk
		,player_id
		,'1' as player_g
	from fld_game
	union 
	select distinct 
		game_pk
		,player_id
		,'1' as player_g
	from rn_game
)
--- individual hitter performance per game
, dailybat as 
(
select distinct 
	pgg.game_pk 
	,pgg.player_id
	,pgg.player_g 
	,sgl.player_team_id
	,sgl.opp_team_id
	,sgl.game_date
	,sgl.side 
	,sg.venue_id as site_id
	,sg.venue_name as site_name
	,sg.day_night 
	,sbb.batting_order 
	,sbb.at_bats
	-- need to find out how to derive PAs - reached on error should be included in the endpoint output
	,(sbb.at_bats + sbb.walks + sbb.intentional_walks + sbb.hit_by_pitch + sbb.sac_bunts + sbb.sac_flies + sbb.reached_on_error) as plate_appearances 
	,sbb.runs
	,sbb.hits
	,sbb.doubles
	,sbb.triples
	,sbb.home_runs
	,sbb.rbi
	,sbb.walks
	,sbb.intentional_walks
	,sbb.strikeouts
	,sbb.hit_by_pitch
	,sbb.stolen_bases
	,sbb.caught_stealing
	,sbb.left_on_base
	,sbb.total_bases
	,sbb.ground_into_double_play
	,sbb.sac_bunts
	,sbb.sac_flies
	,sbf.assists
	,sbf.put_outs
	,sbf.errors
	,sbf.chances
	,sbf.caught_stealing
	,sbf.passed_balls
	,sbf.pickoffs
	,sbf.stolen_bases
from player_game_season_union pgg
	inner join sample_boxscore_batting sbb
		on pgg.game_pk = sbb.game_pk 
		and pgg.player_id = sbb.player_id
		and ppg.team_id = sbb.team_id
	inner join sample_games sg
	 	on pgg.game_pk = sg.game_pk
	-- in case hitter is not in starting lineup using left join 
	left join sample_game_lineups sgl
		on pgg.game_pk = sgl.game_pk
		and pgg.player_id = sgl.player_id
	-- in case hitter does not play defense - will need to reconcile defense only players when doing game counts 
	left join sample_boxscore_fielding sbf
		on ppg.game_pk = sbf.game_pk 
		and ppg.player_id = sbf.player_id 
)
, dailypit as 
(
select distinct 
	pgg.game_pk 
	,pgg.player_id
	,pgg.player_g 
	,sgl.player_team_id
	,sgl.opp_team_id
	,sgl.game_date
	,sgl.side 
	,sg.venue_id as site_id
	,sg.venue_name as site_name
	,sg.day_night 
	,spp.batters_faced as total_batters_faced
	,spp.outs as total_outs
	,round(spp.outs / 3,2) as innings_pitched
	,spp.hits
	,spp.runs
	,spp.earned_runs
	,spp.walks
	,spp.intentional_walks
	,spp.strikeouts
	,spp.home_runs
	,spp.hit_batsmen
	,spp.wild_pitches
	,spp.wild_pitches
	,spp.pitches_thrown
	,spp.strikes
	,spp.balls
	,spp.inherited_runners
	,spp.inherited_runners_scored
	,spp.wins
	,spp.losses
	,spp.saves
	,spp.holds
	,spp.blown_saves
	,spp.games_started
	,spp.complete_games
	,spp.shutouts
	,sbf.assists
	,sbf.put_outs
	,sbf.errors
	,sbf.chances
	,sbf.pickoffs
	,sbf.stolen_bases
	-- need to find out how to derive total batters faced, outs recorded -> use that to calculate innings pitched -> each out recorded = .333 IP
from player_game_season_union pgg
	inner join sample_boxscore_pitching spp
		on pgg.game_pk = spp.game_pk 
		and pgg.player_id = spp.player_id
		and ppg.team_id = spp.team_id
	inner join sample_games sg
	 	on pgg.game_pk = sg.game_pk
	inner join sample_box_score_fielding sbf
		on pgg.game_pk = sbf.game_pk
		and ppg.player_id = sbf.player_id

) 
, dailydef as 
(
	select distinct 
	pgg.game_pk 
	,pgg.player_id
	,pgg.player_g 
	,sgl.player_team_id
	,sgl.opp_team_id
	,sgl.game_date
	,sgl.side 
	,sg.venue_id as site_id
	,sg.venue_name as site_name
	,sg.day_night 
	-- does sample_boxscore_fielding not have position???
	,sbf.assists
	,sbf.put_outs
	,sbf.errors
	,sbf.chances
	,sbf.caught_stealing
	,sbf.passed_balls
	,sbf.pickoffs
	,sbf.stolen_bases
	-- need to find out how to derive total batters faced, outs recorded -> use that to calculate innings pitched -> each out recorded = .333 IP
from player_game_season_union pgg
	inner join sample_box_score_fielding sbf
		on pgg.game_pk = sbf.game_pk
		and ppg.player_id = sbf.player_id
	inner join sample_games sg
	 	on pgg.game_pk = sg.game_pk
) 
-- calculate season level batting performance -- aggregate counting stats and use stats calculator to derive AVG, OPS, SLG, etc. 
, daily_bat_ytd as
(
--- write all columns above + quality stats here (AVG, OPS, SLG) 
-- new column set type as 'hitter' 
)
-- calculate season level pitching performance -- aggregate counting stats and use stats calculator to derive ERA, K%, etc. 
, daily_pit_ytd as 
(
--- write all columns above + quality stats here (ERA, K%, BB%) 
-- new column set type as 'pitcher' 
) 
-- calculate season level defense performance -- aggregate counting stats and use stats calculator to derive fielding_pct 
, daily_def_ytd as 
(
--- write all columns above + quality stats here (FLD %, Error%) 
-- new column set type as 'defense' 
) 
-- roll all ytd tables into master table called players_stats
-- granularity - one row, per season, per type (batter, pitcher, defense) 
-- many rows will be null (i.e batter will have null values for pitcher columns, but ok)
