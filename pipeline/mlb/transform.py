"""
MLB Transform

Reads raw GUMBO JSON from mlb.raw_events and populates:
  mlb.games
  mlb.at_bats
  mlb.pitches
  mlb.runners
  mlb.linescore
  mlb.boxscore_batting
  mlb.boxscore_pitching
  mlb.boxscore_fielding
  mlb.fielding_credits

Usage:
    PYTHONPATH=/app python3 /pipeline/mlb/transform.py --game-pk 832034
    PYTHONPATH=/app python3 /pipeline/mlb/transform.py --all
"""

import sys
import os
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'api'))

from database import SessionLocal
from models.mlb import (
    MLBRawEvent, MLBGame, MLBAtBat, MLBPitch, MLBRunner,
    MLBLinescore, MLBBoxscoreBatting, MLBBoxscorePitching,
    MLBBoxscoreFielding, MLBFieldingCredit
)


def transform_game(raw, db):
    gd = raw.data.get("gameData", {})
    ld = raw.data.get("liveData", {})
    teams = gd.get("teams", {})
    away = teams.get("away", {})
    home = teams.get("home", {})
    linescore = ld.get("linescore", {})
    ls_teams = linescore.get("teams", {})
    decisions = ld.get("decisions", {})
    flags = gd.get("flags", {})
    weather = gd.get("weather", {})
    dt = gd.get("datetime", {})
    status = gd.get("status", {})
    venue = gd.get("venue", {})
    game_info = gd.get("game", {})

    existing = db.query(MLBGame).filter(MLBGame.game_pk == raw.game_pk).first()
    game = existing or MLBGame()

    game.game_pk = raw.game_pk
    game.game_date = raw.game_date
    game.game_type = game_info.get("type")
    game.season = int(game_info.get("season", 0)) if game_info.get("season") else None
    game.status = status.get("detailedState")
    game.double_header = game_info.get("doubleHeader")
    game.game_number = game_info.get("gameNumber")
    game.day_night = dt.get("dayNight")
    game.scheduled_innings = linescore.get("scheduledInnings")

    game.away_team_id = away.get("id")
    game.away_team_name = away.get("name")
    game.away_team_abbrev = away.get("abbreviation")
    game.home_team_id = home.get("id")
    game.home_team_name = home.get("name")
    game.home_team_abbrev = home.get("abbreviation")

    game.away_score = ls_teams.get("away", {}).get("runs")
    game.home_score = ls_teams.get("home", {}).get("runs")
    game.away_hits = ls_teams.get("away", {}).get("hits")
    game.home_hits = ls_teams.get("home", {}).get("hits")
    game.away_errors = ls_teams.get("away", {}).get("errors")
    game.home_errors = ls_teams.get("home", {}).get("errors")

    game.venue_id = venue.get("id")
    game.venue_name = venue.get("name")
    game.weather_temp = int(weather["temp"]) if weather.get("temp") else None
    game.weather_condition = weather.get("condition")
    game.weather_wind = weather.get("wind")

    game.winning_pitcher_id = decisions.get("winner", {}).get("id")
    game.losing_pitcher_id = decisions.get("loser", {}).get("id")
    game.save_pitcher_id = decisions.get("save", {}).get("id")
    game.no_hitter = flags.get("noHitter")
    game.perfect_game = flags.get("perfectGame")
    game.raw_event_id = raw.id

    if not existing:
        db.add(game)
    db.commit()
    db.refresh(game)
    return game


def transform_at_bats(raw, db):
    all_plays = raw.data.get("liveData", {}).get("plays", {}).get("allPlays", [])
    db.query(MLBAtBat).filter(MLBAtBat.game_pk == raw.game_pk).delete()
    db.commit()

    at_bats = []
    for play in all_plays:
        result = play.get("result", {})
        about = play.get("about", {})
        matchup = play.get("matchup", {})
        count = play.get("count", {})

        at_bats.append(MLBAtBat(
            game_pk=raw.game_pk,
            at_bat_index=about.get("atBatIndex"),
            inning=about.get("inning"),
            half_inning=about.get("halfInning"),
            batter_id=matchup.get("batter", {}).get("id"),
            batter_name=matchup.get("batter", {}).get("fullName"),
            pitcher_id=matchup.get("pitcher", {}).get("id"),
            pitcher_name=matchup.get("pitcher", {}).get("fullName"),
            bat_side=matchup.get("batSide", {}).get("code"),
            pitch_hand=matchup.get("pitchHand", {}).get("code"),
            event=result.get("event"),
            event_type=result.get("eventType"),
            description=result.get("description"),
            rbi=result.get("rbi"),
            is_scoring_play=about.get("isScoringPlay"),
            has_out=about.get("hasOut"),
            captivating_index=about.get("captivatingIndex"),
            balls=count.get("balls"),
            strikes=count.get("strikes"),
            outs=count.get("outs"),
            away_score=result.get("awayScore"),
            home_score=result.get("homeScore"),
        ))

    db.bulk_save_objects(at_bats)
    db.commit()
    return len(at_bats)


def transform_pitches(raw, db):
    all_plays = raw.data.get("liveData", {}).get("plays", {}).get("allPlays", [])
    db.query(MLBPitch).filter(MLBPitch.game_pk == raw.game_pk).delete()
    db.commit()

    pitches = []
    for play in all_plays:
        at_bat_index = play.get("about", {}).get("atBatIndex")
        matchup = play.get("matchup", {})
        pitcher_id = matchup.get("pitcher", {}).get("id")
        batter_id = matchup.get("batter", {}).get("id")

        for idx, event in enumerate(play.get("playEvents", [])):
            if not event.get("isPitch"):
                continue

            details = event.get("details", {})
            pd = event.get("pitchData", {})
            coords = pd.get("coordinates", {})
            breaks = pd.get("breaks", {})
            hd = event.get("hitData", {})
            count = event.get("count", {})

            pitches.append(MLBPitch(
                game_pk=raw.game_pk,
                at_bat_index=at_bat_index,
                pitch_index=idx,
                pitch_number=event.get("pitchNumber"),
                pitcher_id=pitcher_id,
                batter_id=batter_id,
                pitch_type_code=details.get("type", {}).get("code"),
                pitch_type_desc=details.get("type", {}).get("description"),
                call_code=details.get("code"),
                call_description=details.get("description"),
                is_strike=details.get("isStrike"),
                is_ball=details.get("isBall"),
                is_in_play=details.get("isInPlay"),
                start_speed=pd.get("startSpeed"),
                end_speed=pd.get("endSpeed"),
                zone=pd.get("zone"),
                strike_zone_top=pd.get("strikeZoneTop"),
                strike_zone_bottom=pd.get("strikeZoneBottom"),
                px=coords.get("pX"),
                pz=coords.get("pZ"),
                pfx_x=coords.get("pfxX"),
                pfx_z=coords.get("pfxZ"),
                x0=coords.get("x0"),
                y0=coords.get("y0"),
                z0=coords.get("z0"),
                vx0=coords.get("vX0"),
                vy0=coords.get("vY0"),
                vz0=coords.get("vZ0"),
                ax=coords.get("aX"),
                ay=coords.get("aY"),
                az=coords.get("aZ"),
                break_angle=breaks.get("breakAngle"),
                break_length=breaks.get("breakLength"),
                break_y=breaks.get("breakY"),
                spin_rate=breaks.get("spinRate"),
                spin_direction=breaks.get("spinDirection"),
                launch_speed=hd.get("launchSpeed"),
                launch_angle=hd.get("launchAngle"),
                hit_distance=hd.get("totalDistance"),
                trajectory=hd.get("trajectory"),
                hit_hardness=hd.get("hardness"),
                hit_coord_x=hd.get("coordinates", {}).get("coordX"),
                hit_coord_y=hd.get("coordinates", {}).get("coordY"),
                balls_after=count.get("balls"),
                strikes_after=count.get("strikes"),
                play_id=event.get("playId"),
                pfx_id=event.get("pfxId"),
            ))

    db.bulk_save_objects(pitches)
    db.commit()
    return len(pitches)


def transform_runners(raw, db):
    all_plays = raw.data.get("liveData", {}).get("plays", {}).get("allPlays", [])
    db.query(MLBRunner).filter(MLBRunner.game_pk == raw.game_pk).delete()
    db.commit()

    runners = []
    for play in all_plays:
        at_bat_index = play.get("about", {}).get("atBatIndex")
        for runner_event in play.get("runners", []):
            movement = runner_event.get("movement", {})
            details = runner_event.get("details", {})
            runner = details.get("runner", {})
            resp_pitcher = details.get("responsiblePitcher") or {}

            runners.append(MLBRunner(
                game_pk=raw.game_pk,
                at_bat_index=at_bat_index,
                play_index=runner_event.get("details", {}).get("playIndex"),
                runner_id=runner.get("id"),
                runner_name=runner.get("fullName"),
                start_base=movement.get("start"),
                end_base=movement.get("end"),
                out_base=movement.get("outBase"),
                is_out=movement.get("isOut"),
                out_number=movement.get("outNumber"),
                event=details.get("event"),
                event_type=details.get("eventType"),
                is_scoring_event=details.get("isScoringEvent"),
                rbi=details.get("rbi"),
                earned=details.get("earned"),
                team_unearned=details.get("teamUnearned"),
                responsible_pitcher_id=resp_pitcher.get("id"),
            ))

    db.bulk_save_objects(runners)
    db.commit()
    return len(runners)


def transform_linescore(raw, db):
    ld = raw.data.get("liveData", {})
    innings = ld.get("linescore", {}).get("innings", [])
    db.query(MLBLinescore).filter(MLBLinescore.game_pk == raw.game_pk).delete()
    db.commit()

    rows = []
    for inning in innings:
        inning_num = inning.get("num")
        for half, side in [("top", "away"), ("bottom", "home")]:
            half_data = inning.get(side, {})
            if not half_data:
                continue
            rows.append(MLBLinescore(
                game_pk=raw.game_pk,
                inning=inning_num,
                half_inning=half,
                runs=half_data.get("runs"),
                hits=half_data.get("hits"),
                errors=half_data.get("errors"),
                left_on_base=half_data.get("leftOnBase"),
            ))

    db.bulk_save_objects(rows)
    db.commit()
    return len(rows)


def transform_boxscore(raw, db):
    boxscore = raw.data.get("liveData", {}).get("boxscore", {})
    teams = boxscore.get("teams", {})

    # Clear existing boxscore data for this game
    db.query(MLBBoxscoreBatting).filter(MLBBoxscoreBatting.game_pk == raw.game_pk).delete()
    db.query(MLBBoxscorePitching).filter(MLBBoxscorePitching.game_pk == raw.game_pk).delete()
    db.query(MLBBoxscoreFielding).filter(MLBBoxscoreFielding.game_pk == raw.game_pk).delete()
    db.query(MLBFieldingCredit).filter(MLBFieldingCredit.game_pk == raw.game_pk).delete()
    db.commit()

    batting_rows, pitching_rows, fielding_rows = [], [], []

    for side in ["away", "home"]:
        team_data = teams.get(side, {})
        team_id = team_data.get("team", {}).get("id")
        players = team_data.get("players", {})

        for player_key, player_data in players.items():
            player_id = player_data.get("person", {}).get("id")
            if not player_id:
                continue

            stats = player_data.get("stats", {})
            batting_order = player_data.get("battingOrder")

            # Batting
            b = stats.get("batting", {})
            if b:
                batting_rows.append(MLBBoxscoreBatting(
                    game_pk=raw.game_pk,
                    player_id=player_id,
                    team_id=team_id,
                    batting_order=int(str(batting_order)[:1]) * 100 if batting_order else None,
                    at_bats=b.get("atBats"),
                    runs=b.get("runs"),
                    hits=b.get("hits"),
                    doubles=b.get("doubles"),
                    triples=b.get("triples"),
                    home_runs=b.get("homeRuns"),
                    rbi=b.get("rbi"),
                    walks=b.get("baseOnBalls"),
                    intentional_walks=b.get("intentionalWalks"),
                    strikeouts=b.get("strikeOuts"),
                    hit_by_pitch=b.get("hitByPitch"),
                    stolen_bases=b.get("stolenBases"),
                    caught_stealing=b.get("caughtStealing"),
                    left_on_base=b.get("leftOnBase"),
                    avg=float(b["avg"]) if b.get("avg") else None,
                    obp=float(b["obp"]) if b.get("obp") else None,
                    slg=float(b["slg"]) if b.get("slg") else None,
                    ops=float(b["ops"]) if b.get("ops") else None,
                    total_bases=b.get("totalBases"),
                    ground_into_double_play=b.get("groundIntoDoublePlay"),
                    sac_bunts=b.get("sacBunts"),
                    sac_flies=b.get("sacFlies"),
                ))

            # Pitching
            p = stats.get("pitching", {})
            if p:
                pitching_rows.append(MLBBoxscorePitching(
                    game_pk=raw.game_pk,
                    player_id=player_id,
                    team_id=team_id,
                    innings_pitched=float(p["inningsPitched"]) if p.get("inningsPitched") else None,
                    hits=p.get("hits"),
                    runs=p.get("runs"),
                    earned_runs=p.get("earnedRuns"),
                    walks=p.get("baseOnBalls"),
                    intentional_walks=p.get("intentionalWalks"),
                    strikeouts=p.get("strikeOuts"),
                    home_runs=p.get("homeRuns"),
                    hit_batsmen=p.get("hitBatsmen"),
                    wild_pitches=p.get("wildPitches"),
                    pitches_thrown=p.get("pitchesThrown"),
                    strikes=p.get("strikes"),
                    balls=p.get("balls"),
                    era=float(p["era"]) if p.get("era") else None,
                    whip=float(p["whip"]) if p.get("whip") else None,
                    batters_faced=p.get("battersFaced"),
                    outs=p.get("outs"),
                    inherited_runners=p.get("inheritedRunners"),
                    inherited_runners_scored=p.get("inheritedRunnersScored"),
                    wins=p.get("wins"),
                    losses=p.get("losses"),
                    saves=p.get("saves"),
                    holds=p.get("holds"),
                    blown_saves=p.get("blownSaves"),
                    games_started=p.get("gamesStarted"),
                    complete_games=p.get("completeGames"),
                    shutouts=p.get("shutouts"),
                ))

            # Fielding
            f = stats.get("fielding", {})
            if f:
                fielding_rows.append(MLBBoxscoreFielding(
                    game_pk=raw.game_pk,
                    player_id=player_id,
                    team_id=team_id,
                    assists=f.get("assists"),
                    put_outs=f.get("putOuts"),
                    errors=f.get("errors"),
                    chances=f.get("chances"),
                    fielding_pct=float(f["fielding"]) if f.get("fielding") else None,
                    caught_stealing=f.get("caughtStealing"),
                    passed_balls=f.get("passedBall"),
                    stolen_bases=f.get("stolenBases"),
                    pickoffs=f.get("pickoffs"),
                ))

    db.bulk_save_objects(batting_rows)
    db.bulk_save_objects(pitching_rows)
    db.bulk_save_objects(fielding_rows)
    db.commit()
    return len(batting_rows), len(pitching_rows), len(fielding_rows)


def transform_fielding_credits(raw, db):
    all_plays = raw.data.get("liveData", {}).get("plays", {}).get("allPlays", [])
    db.query(MLBFieldingCredit).filter(MLBFieldingCredit.game_pk == raw.game_pk).delete()
    db.commit()

    credits = []
    for play in all_plays:
        at_bat_index = play.get("about", {}).get("atBatIndex")
        for runner_event in play.get("runners", []):
            for credit in runner_event.get("credits", []):
                player = credit.get("player", {})
                position = credit.get("position", {})
                credits.append(MLBFieldingCredit(
                    game_pk=raw.game_pk,
                    at_bat_index=at_bat_index,
                    player_id=player.get("id"),
                    position_code=position.get("code"),
                    position_name=position.get("name"),
                    credit=credit.get("credit"),
                ))

    db.bulk_save_objects(credits)
    db.commit()
    return len(credits)


def transform_game_pk(game_pk: int, db):
    raw = db.query(MLBRawEvent).filter(MLBRawEvent.game_pk == game_pk).first()
    if not raw:
        print(f"  No raw data for game_pk {game_pk}. Fetch it first.")
        return

    print(f"  Transforming {game_pk} ({raw.away_team} @ {raw.home_team})...")

    game = transform_game(raw, db)
    print(f"    ✓ game: {game.status}")

    n_ab = transform_at_bats(raw, db)
    print(f"    ✓ at_bats: {n_ab} rows")

    n_pitches = transform_pitches(raw, db)
    print(f"    ✓ pitches: {n_pitches} rows")

    n_runners = transform_runners(raw, db)
    print(f"    ✓ runners: {n_runners} rows")

    n_linescore = transform_linescore(raw, db)
    print(f"    ✓ linescore: {n_linescore} rows")

    n_bat, n_pitch, n_field = transform_boxscore(raw, db)
    print(f"    ✓ boxscore_batting: {n_bat} rows")
    print(f"    ✓ boxscore_pitching: {n_pitch} rows")
    print(f"    ✓ boxscore_fielding: {n_field} rows")

    n_credits = transform_fielding_credits(raw, db)
    print(f"    ✓ fielding_credits: {n_credits} rows")


def main():
    parser = argparse.ArgumentParser(description="Transform MLB GUMBO raw data")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--game-pk", type=int)
    group.add_argument("--all", action="store_true")
    args = parser.parse_args()

    if args.game_pk:
        db = SessionLocal()
        try:
            transform_game_pk(args.game_pk, db)
        finally:
            db.close()
    else:
        # Get all game_pks first, then process each with a fresh session
        db = SessionLocal()
        game_pks = [r.game_pk for r in db.query(MLBRawEvent.game_pk).all()]
        db.close()

        total = len(game_pks)
        print(f"Transforming {total} games...")

        for i, game_pk in enumerate(game_pks, 1):
            if i % 100 == 0:
                print(f"  Progress: {i}/{total}")
            db = SessionLocal()
            try:
                transform_game_pk(game_pk, db)
            except Exception as e:
                print(f"  ERROR on {game_pk}: {e}")
            finally:
                db.close()

    print("\nDone.")


if __name__ == "__main__":
    main()