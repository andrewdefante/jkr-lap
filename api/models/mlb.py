"""
MLB schema - fully self-contained.

Tables:
  mlb.raw_events     Raw GUMBO JSON blobs. Landing zone before transform.
  mlb.games          One row per game.
  mlb.at_bats        One row per at-bat.
  mlb.pitches        One row per pitch (most granular, Statcast data lives here).

All tables live in the 'mlb' Postgres schema. No cross-sport dependencies.
"""

from sqlalchemy import (
    Column, BigInteger, Integer, String, Float, Boolean,
    DateTime, Text, Index, UniqueConstraint
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from models.base import Base


class MLBRawEvent(Base):
    """
    Raw landing zone for GUMBO API responses.
    Every fetch writes here first. Transform reads from here.
    If transform logic changes, re-run transform against existing rows
    without re-hitting the API.
    """
    __tablename__ = "raw_events"
    __table_args__ = (
        Index("ix_mlb_raw_events_game_pk", "game_pk"),
        Index("ix_mlb_raw_events_game_date", "game_date"),
        Index("ix_mlb_raw_events_status", "status"),
        {"schema": "mlb"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    game_pk = Column(Integer, nullable=False, unique=True)
    game_date = Column(String(10), nullable=True)           # YYYY-MM-DD
    status = Column(String(50), nullable=True)              # Final, In Progress, etc.
    away_team = Column(String(10), nullable=True)           # abbreviation e.g. TEX
    home_team = Column(String(10), nullable=True)           # abbreviation e.g. ARI
    endpoint = Column(Text, nullable=True)                  # full URL called
    data = Column(JSONB, nullable=False)                    # complete GUMBO response
    fetched_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class MLBGame(Base):
    """
    One row per game. Transformed from mlb.raw_events.
    Game-level facts: teams, score, venue, weather, decisions.
    """
    __tablename__ = "games"
    __table_args__ = (
        Index("ix_mlb_games_game_date", "game_date"),
        Index("ix_mlb_games_season", "season"),
        Index("ix_mlb_games_away_team_id", "away_team_id"),
        Index("ix_mlb_games_home_team_id", "home_team_id"),
        {"schema": "mlb"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    game_pk = Column(Integer, unique=True, nullable=False)
    game_date = Column(String(10), nullable=False)
    game_type = Column(String(5), nullable=True)            # R=Regular, P=Playoff, W=WS
    season = Column(Integer, nullable=True)
    status = Column(String(50), nullable=True)
    double_header = Column(String(1), nullable=True)        # Y, N, S
    game_number = Column(Integer, nullable=True)            # 1 or 2 for doubleheaders

    # Teams
    away_team_id = Column(Integer, nullable=True)
    away_team_name = Column(String(100), nullable=True)
    away_team_abbrev = Column(String(10), nullable=True)
    home_team_id = Column(Integer, nullable=True)
    home_team_name = Column(String(100), nullable=True)
    home_team_abbrev = Column(String(10), nullable=True)

    # Final score
    away_score = Column(Integer, nullable=True)
    home_score = Column(Integer, nullable=True)

    # Linescore totals
    away_hits = Column(Integer, nullable=True)
    home_hits = Column(Integer, nullable=True)
    away_errors = Column(Integer, nullable=True)
    home_errors = Column(Integer, nullable=True)

    # Venue
    venue_id = Column(Integer, nullable=True)
    venue_name = Column(String(100), nullable=True)

    # Weather at first pitch
    weather_temp = Column(Integer, nullable=True)           # Fahrenheit
    weather_condition = Column(String(50), nullable=True)
    weather_wind = Column(String(100), nullable=True)

    # Decisions
    winning_pitcher_id = Column(Integer, nullable=True)
    losing_pitcher_id = Column(Integer, nullable=True)
    save_pitcher_id = Column(Integer, nullable=True)

    # No-hitter / perfect game flags (after 5 innings per GUMBO docs)
    no_hitter = Column(Boolean, nullable=True)
    perfect_game = Column(Boolean, nullable=True)

    # Scheduling
    day_night = Column(String(5), nullable=True)            # day, night
    scheduled_innings = Column(Integer, nullable=True)

    raw_event_id = Column(BigInteger, nullable=True)        # ref back to mlb.raw_events
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class MLBAtBat(Base):
    """
    One row per at-bat. Core unit of baseball analysis.
    Transformed from the allPlays array in mlb.raw_events.
    """
    __tablename__ = "at_bats"
    __table_args__ = (
        Index("ix_mlb_at_bats_game_pk", "game_pk"),
        Index("ix_mlb_at_bats_batter_id", "batter_id"),
        Index("ix_mlb_at_bats_pitcher_id", "pitcher_id"),
        Index("ix_mlb_at_bats_event_type", "event_type"),
        {"schema": "mlb"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    game_pk = Column(Integer, nullable=False)
    at_bat_index = Column(Integer, nullable=False)          # 0-based index within game
    inning = Column(Integer, nullable=False)
    half_inning = Column(String(10), nullable=False)        # 'top' or 'bottom'

    # Matchup
    batter_id = Column(Integer, nullable=False)
    batter_name = Column(String(100), nullable=True)
    pitcher_id = Column(Integer, nullable=False)
    pitcher_name = Column(String(100), nullable=True)
    bat_side = Column(String(1), nullable=True)             # L, R, S
    pitch_hand = Column(String(1), nullable=True)           # L, R

    # Result
    event = Column(String(100), nullable=True)              # 'Single', 'Home Run', etc.
    event_type = Column(String(100), nullable=True)         # snake_case version
    description = Column(Text, nullable=True)               # play-by-play text
    rbi = Column(Integer, nullable=True)
    is_scoring_play = Column(Boolean, nullable=True)
    has_out = Column(Boolean, nullable=True)
    captivating_index = Column(Integer, nullable=True)      # 0-100 MLB highlight score

    # Count at end of AB
    balls = Column(Integer, nullable=True)
    strikes = Column(Integer, nullable=True)
    outs = Column(Integer, nullable=True)                   # outs at END of AB

    # Score at end of AB
    away_score = Column(Integer, nullable=True)
    home_score = Column(Integer, nullable=True)

    # Timing
    start_time = Column(DateTime(timezone=True), nullable=True)
    end_time = Column(DateTime(timezone=True), nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())


class MLBPitch(Base):
    """
    One row per pitch. Most granular table — full Statcast data lives here.
    Transformed from playEvents arrays within each at-bat in mlb.raw_events.
    """
    __tablename__ = "pitches"
    __table_args__ = (
        Index("ix_mlb_pitches_game_pk", "game_pk"),
        Index("ix_mlb_pitches_pitcher_id", "pitcher_id"),
        Index("ix_mlb_pitches_batter_id", "batter_id"),
        Index("ix_mlb_pitches_pitch_type_code", "pitch_type_code"),
        {"schema": "mlb"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    game_pk = Column(Integer, nullable=False)
    at_bat_index = Column(Integer, nullable=False)
    pitch_index = Column(Integer, nullable=False)           # index within at-bat
    pitch_number = Column(Integer, nullable=True)           # sequence number within AB

    pitcher_id = Column(Integer, nullable=False)
    batter_id = Column(Integer, nullable=False)

    # Classification
    pitch_type_code = Column(String(5), nullable=True)      # FF, SL, CH, CU, SI, etc.
    pitch_type_desc = Column(String(50), nullable=True)     # Four-Seam Fastball, etc.

    # Pitch result
    call_code = Column(String(5), nullable=True)
    call_description = Column(String(100), nullable=True)
    is_strike = Column(Boolean, nullable=True)
    is_ball = Column(Boolean, nullable=True)
    is_in_play = Column(Boolean, nullable=True)

    # Velocity
    start_speed = Column(Float, nullable=True)              # mph at 50ft from plate
    end_speed = Column(Float, nullable=True)                # mph at front of plate

    # Location at plate
    px = Column(Float, nullable=True)                       # horizontal (feet)
    pz = Column(Float, nullable=True)                       # vertical (feet)
    zone = Column(Integer, nullable=True)                   # 1-14, see GUMBO PlateZones

    # Strike zone for this batter
    strike_zone_top = Column(Float, nullable=True)
    strike_zone_bottom = Column(Float, nullable=True)

    # Movement
    pfx_x = Column(Float, nullable=True)                    # horizontal movement (inches)
    pfx_z = Column(Float, nullable=True)                    # vertical movement (inches)
    break_angle = Column(Float, nullable=True)              # degrees clockwise
    break_length = Column(Float, nullable=True)             # inches
    break_y = Column(Float, nullable=True)                  # feet from plate
    spin_rate = Column(Integer, nullable=True)              # rpm
    spin_direction = Column(Integer, nullable=True)

    # Release point
    x0 = Column(Float, nullable=True)
    y0 = Column(Float, nullable=True)
    z0 = Column(Float, nullable=True)

    # Velocity components at release
    vx0 = Column(Float, nullable=True)
    vy0 = Column(Float, nullable=True)
    vz0 = Column(Float, nullable=True)

    # Acceleration
    ax = Column(Float, nullable=True)
    ay = Column(Float, nullable=True)
    az = Column(Float, nullable=True)

    # Hit data - only populated when is_in_play = True
    launch_speed = Column(Float, nullable=True)             # exit velocity mph
    launch_angle = Column(Float, nullable=True)             # degrees
    hit_distance = Column(Float, nullable=True)             # feet
    trajectory = Column(String(50), nullable=True)          # fly_ball, line_drive, etc.
    hit_hardness = Column(String(20), nullable=True)        # soft, medium, hard
    hit_coord_x = Column(Float, nullable=True)              # spray chart x
    hit_coord_y = Column(Float, nullable=True)              # spray chart y

    # Count AFTER this pitch
    balls_after = Column(Integer, nullable=True)
    strikes_after = Column(Integer, nullable=True)

    # Identifiers
    pfx_id = Column(String(50), nullable=True)              # Pitch f/x identifier
    play_id = Column(String(100), nullable=True)            # Statcast GUID

    start_time = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class MLBPlayer(Base):
    """
    Player reference table. Populated from the players object in GUMBO.
    Updated whenever new player data is seen during a fetch.
    """
    __tablename__ = "players"
    __table_args__ = {"schema": "mlb"}

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    player_id = Column(Integer, unique=True, nullable=False)    # MLBAM player_id
    full_name = Column(String(100), nullable=True)
    first_name = Column(String(50), nullable=True)
    last_name = Column(String(50), nullable=True)
    primary_number = Column(Integer, nullable=True)             # uniform number
    birth_date = Column(String(10), nullable=True)              # YYYY-MM-DD
    birth_city = Column(String(100), nullable=True)
    birth_country = Column(String(100), nullable=True)
    height = Column(String(10), nullable=True)                  # e.g. "6' 2\""
    weight = Column(Integer, nullable=True)
    bat_side = Column(String(1), nullable=True)                 # L, R, S
    pitch_hand = Column(String(1), nullable=True)               # L, R
    primary_position = Column(String(5), nullable=True)         # position code
    mlb_debut_date = Column(String(10), nullable=True)
    active = Column(Boolean, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

class MLBRunner(Base):
    """
    Baserunner movement on every play.
    One row per runner per play event.
    Essential for RE24, WPA, and situational analysis.
    """
    __tablename__ = "runners"
    __table_args__ = (
        Index("ix_mlb_runners_game_pk", "game_pk"),
        Index("ix_mlb_runners_runner_id", "runner_id"),
        {"schema": "mlb"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    game_pk = Column(Integer, nullable=False)
    at_bat_index = Column(Integer, nullable=False)
    play_index = Column(Integer, nullable=True)      # index within playEvents

    runner_id = Column(Integer, nullable=False)
    runner_name = Column(String(100), nullable=True)

    start_base = Column(String(5), nullable=True)    # 1B, 2B, 3B, null for batter
    end_base = Column(String(5), nullable=True)      # 1B, 2B, 3B, score, null if out
    out_base = Column(String(5), nullable=True)      # base where out occurred
    is_out = Column(Boolean, nullable=True)
    out_number = Column(Integer, nullable=True)

    event = Column(String(100), nullable=True)       # reason for movement
    event_type = Column(String(100), nullable=True)
    is_scoring_event = Column(Boolean, nullable=True)
    rbi = Column(Boolean, nullable=True)
    earned = Column(Boolean, nullable=True)
    team_unearned = Column(Boolean, nullable=True)

    responsible_pitcher_id = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class MLBLinescore(Base):
    """
    Runs/hits/errors per half inning per game.
    Good for game flow analysis and inning-by-inning breakdowns.
    """
    __tablename__ = "linescore"
    __table_args__ = (
        Index("ix_mlb_linescore_game_pk", "game_pk"),
        {"schema": "mlb"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    game_pk = Column(Integer, nullable=False)
    inning = Column(Integer, nullable=False)
    half_inning = Column(String(10), nullable=False)    # top, bottom
    runs = Column(Integer, nullable=True)
    hits = Column(Integer, nullable=True)
    errors = Column(Integer, nullable=True)
    left_on_base = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class MLBBoxscoreBatting(Base):
    """
    Cumulative batting stats per player per game from GUMBO boxscore.
    These are computed by MLB — saves us from aggregating pitch-by-pitch.
    """
    __tablename__ = "boxscore_batting"
    __table_args__ = (
        Index("ix_mlb_bs_batting_game_pk", "game_pk"),
        Index("ix_mlb_bs_batting_player_id", "player_id"),
        {"schema": "mlb"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    game_pk = Column(Integer, nullable=False)
    player_id = Column(Integer, nullable=False)
    team_id = Column(Integer, nullable=True)
    batting_order = Column(Integer, nullable=True)      # 100=1st, 200=2nd, etc.

    at_bats = Column(Integer, nullable=True)
    runs = Column(Integer, nullable=True)
    hits = Column(Integer, nullable=True)
    doubles = Column(Integer, nullable=True)
    triples = Column(Integer, nullable=True)
    home_runs = Column(Integer, nullable=True)
    rbi = Column(Integer, nullable=True)
    walks = Column(Integer, nullable=True)
    intentional_walks = Column(Integer, nullable=True)
    strikeouts = Column(Integer, nullable=True)
    hit_by_pitch = Column(Integer, nullable=True)
    stolen_bases = Column(Integer, nullable=True)
    caught_stealing = Column(Integer, nullable=True)
    left_on_base = Column(Integer, nullable=True)
    avg = Column(Float, nullable=True)
    obp = Column(Float, nullable=True)
    slg = Column(Float, nullable=True)
    ops = Column(Float, nullable=True)
    total_bases = Column(Integer, nullable=True)
    ground_into_double_play = Column(Integer, nullable=True)
    sac_bunts = Column(Integer, nullable=True)
    sac_flies = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class MLBBoxscorePitching(Base):
    """
    Cumulative pitching stats per player per game from GUMBO boxscore.
    """
    __tablename__ = "boxscore_pitching"
    __table_args__ = (
        Index("ix_mlb_bs_pitching_game_pk", "game_pk"),
        Index("ix_mlb_bs_pitching_player_id", "player_id"),
        {"schema": "mlb"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    game_pk = Column(Integer, nullable=False)
    player_id = Column(Integer, nullable=False)
    team_id = Column(Integer, nullable=True)

    innings_pitched = Column(Float, nullable=True)
    hits = Column(Integer, nullable=True)
    runs = Column(Integer, nullable=True)
    earned_runs = Column(Integer, nullable=True)
    walks = Column(Integer, nullable=True)
    intentional_walks = Column(Integer, nullable=True)
    strikeouts = Column(Integer, nullable=True)
    home_runs = Column(Integer, nullable=True)
    hit_batsmen = Column(Integer, nullable=True)
    wild_pitches = Column(Integer, nullable=True)
    pitches_thrown = Column(Integer, nullable=True)
    strikes = Column(Integer, nullable=True)
    balls = Column(Integer, nullable=True)
    era = Column(Float, nullable=True)
    whip = Column(Float, nullable=True)
    batters_faced = Column(Integer, nullable=True)
    outs = Column(Integer, nullable=True)
    inherited_runners = Column(Integer, nullable=True)
    inherited_runners_scored = Column(Integer, nullable=True)
    wins = Column(Integer, nullable=True)
    losses = Column(Integer, nullable=True)
    saves = Column(Integer, nullable=True)
    holds = Column(Integer, nullable=True)
    blown_saves = Column(Integer, nullable=True)
    games_started = Column(Integer, nullable=True)
    complete_games = Column(Integer, nullable=True)
    shutouts = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class MLBBoxscoreFielding(Base):
    """
    Cumulative fielding stats per player per game from GUMBO boxscore.
    """
    __tablename__ = "boxscore_fielding"
    __table_args__ = (
        Index("ix_mlb_bs_fielding_game_pk", "game_pk"),
        Index("ix_mlb_bs_fielding_player_id", "player_id"),
        {"schema": "mlb"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    game_pk = Column(Integer, nullable=False)
    player_id = Column(Integer, nullable=False)
    team_id = Column(Integer, nullable=True)

    assists = Column(Integer, nullable=True)
    put_outs = Column(Integer, nullable=True)
    errors = Column(Integer, nullable=True)
    chances = Column(Integer, nullable=True)
    fielding_pct = Column(Float, nullable=True)
    caught_stealing = Column(Integer, nullable=True)
    passed_balls = Column(Integer, nullable=True)
    stolen_bases = Column(Integer, nullable=True)
    pickoffs = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class MLBFieldingCredit(Base):
    """
    Fielding credits per ball in play.
    Who fielded it, what position, putout vs assist vs error.
    """
    __tablename__ = "fielding_credits"
    __table_args__ = (
        Index("ix_mlb_fielding_game_pk", "game_pk"),
        Index("ix_mlb_fielding_player_id", "player_id"),
        {"schema": "mlb"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    game_pk = Column(Integer, nullable=False)
    at_bat_index = Column(Integer, nullable=False)
    player_id = Column(Integer, nullable=False)
    position_code = Column(String(5), nullable=True)    # 1-9
    position_name = Column(String(20), nullable=True)
    credit = Column(String(50), nullable=True)          # f_putout, f_assist, f_error
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class MLBFangraphsBatting(Base):
    """
    Season-level batting stats from Fangraphs via pybaseball.
    One row per player per season. Updated daily during season.
    Covers production metrics (wRC+, wOBA) and process metrics
    (swing%, chase%, zone contact%, whiff%).
    """
    __tablename__ = "fangraphs_batting"
    __table_args__ = (
        Index("ix_mlb_fg_batting_season", "season"),
        Index("ix_mlb_fg_batting_player_id", "mlbam_id"),
        {"schema": "mlb"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    season = Column(Integer, nullable=False)
    player_name = Column(String(100), nullable=True)
    team = Column(String(10), nullable=True)
    mlbam_id = Column(Integer, nullable=True)        # links to mlb.players
    fg_id = Column(String(20), nullable=True)        # Fangraphs player ID

    # Playing time
    games = Column(Integer, nullable=True)
    pa = Column(Integer, nullable=True)
    ab = Column(Integer, nullable=True)

    # Standard slash line
    avg = Column(Float, nullable=True)
    obp = Column(Float, nullable=True)
    slg = Column(Float, nullable=True)
    ops = Column(Float, nullable=True)

    # Production metrics
    woba = Column(Float, nullable=True)
    wrc_plus = Column(Float, nullable=True)          # wRC+ (100 = avg)
    ops_plus = Column(Float, nullable=True)          # OPS+ (100 = avg)
    off = Column(Float, nullable=True)               # Offensive runs above avg
    war = Column(Float, nullable=True)               # fWAR

    # Power
    iso = Column(Float, nullable=True)               # Isolated power
    hr = Column(Integer, nullable=True)
    barrel_pct = Column(Float, nullable=True)        # Barrel%
    hard_hit_pct = Column(Float, nullable=True)      # Hard Hit%
    avg_exit_velo = Column(Float, nullable=True)

    # Statcast expected
    xba = Column(Float, nullable=True)
    xslg = Column(Float, nullable=True)
    xwoba = Column(Float, nullable=True)
    xwobacon = Column(Float, nullable=True)          # xwOBA on contact

    # Plate discipline — swing decisions
    o_swing_pct = Column(Float, nullable=True)       # Chase% (swing at balls)
    z_swing_pct = Column(Float, nullable=True)       # Zone swing%
    swing_pct = Column(Float, nullable=True)         # Overall swing%
    o_contact_pct = Column(Float, nullable=True)     # Contact% on balls
    z_contact_pct = Column(Float, nullable=True)     # Zone contact%
    contact_pct = Column(Float, nullable=True)       # Overall contact%
    zone_pct = Column(Float, nullable=True)          # Zone% (pitches in zone)
    whiff_pct = Column(Float, nullable=True)         # Whiff% (swings and misses)
    swstr_pct = Column(Float, nullable=True)         # SwStr% (of all pitches)

    # Speed
    sprint_speed = Column(Float, nullable=True)

    fetched_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

class MLBPlayerIDMap(Base):
    """
    Player ID crosswalk between MLBAM, Fangraphs, and Baseball Reference.
    Built from pybaseball.playerid_lookup().
    Used to join Fangraphs stats to GUMBO data via MLBAM ID.
    """
    __tablename__ = "player_id_map"
    __table_args__ = (
        Index("ix_mlb_id_map_mlbam", "mlbam_id"),
        Index("ix_mlb_id_map_fangraphs", "fangraphs_id"),
        Index("ix_mlb_id_map_bbref", "bbref_id"),
        {"schema": "mlb"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    mlbam_id = Column(Integer, unique=True, nullable=True)
    fangraphs_id = Column(Integer, nullable=True)
    bbref_id = Column(String(20), nullable=True)
    first_name = Column(String(50), nullable=True)
    last_name = Column(String(50), nullable=True)
    birth_year = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

class MLBFangraphsPitching(Base):
    """
    Season-level pitching stats from Fangraphs via pybaseball.
    One row per pitcher per season. Updated daily during season.
    """
    __tablename__ = "fangraphs_pitching"
    __table_args__ = (
        Index("ix_mlb_fg_pitching_season", "season"),
        Index("ix_mlb_fg_pitching_player_id", "mlbam_id"),
        {"schema": "mlb"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    season = Column(Integer, nullable=False)
    player_name = Column(String(100), nullable=True)
    team = Column(String(10), nullable=True)
    mlbam_id = Column(Integer, nullable=True)
    fg_id = Column(String(20), nullable=True)

    # Playing time
    games = Column(Integer, nullable=True)
    games_started = Column(Integer, nullable=True)
    ip = Column(Float, nullable=True)

    # Standard
    era = Column(Float, nullable=True)
    whip = Column(Float, nullable=True)
    wins = Column(Integer, nullable=True)
    losses = Column(Integer, nullable=True)
    saves = Column(Integer, nullable=True)

    # ERA estimators — the plus/minus metrics
    era_plus = Column(Float, nullable=True)          # ERA+ (100 = avg, higher better)
    era_minus = Column(Float, nullable=True)         # ERA- (100 = avg, lower better)
    fip = Column(Float, nullable=True)
    fip_minus = Column(Float, nullable=True)         # FIP- (100 = avg, lower better)
    xfip = Column(Float, nullable=True)
    xfip_minus = Column(Float, nullable=True)
    siera = Column(Float, nullable=True)
    xera = Column(Float, nullable=True)
    war = Column(Float, nullable=True)               # fWAR

    # Stuff
    k_pct = Column(Float, nullable=True)             # K%
    bb_pct = Column(Float, nullable=True)            # BB%
    k_minus_bb = Column(Float, nullable=True)        # K-BB%
    hr_per_9 = Column(Float, nullable=True)
    avg_fastball_velo = Column(Float, nullable=True)
    stuff_plus = Column(Float, nullable=True)          # Fangraphs Stuff+

    # Plate discipline (from pitcher's perspective)
    o_swing_pct = Column(Float, nullable=True)       # Chase% induced
    z_swing_pct = Column(Float, nullable=True)       # Zone swing% induced
    swing_pct = Column(Float, nullable=True)
    o_contact_pct = Column(Float, nullable=True)
    z_contact_pct = Column(Float, nullable=True)
    contact_pct = Column(Float, nullable=True)
    zone_pct = Column(Float, nullable=True)
    whiff_pct = Column(Float, nullable=True)
    swstr_pct = Column(Float, nullable=True)         # SwStr%

    # Statcast
    barrel_pct = Column(Float, nullable=True)        # Barrel% allowed
    hard_hit_pct = Column(Float, nullable=True)      # Hard Hit% allowed
    avg_exit_velo = Column(Float, nullable=True)
    xera = Column(Float, nullable=True)

    fetched_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class MLBBatterPitchTypeTendencies(Base):
    """
    Per batter per pitch type tendencies computed from pitch data.
    Used to adjust pitch quality scores for batter context.
    Min 50 pitches faced to qualify.
    """
    __tablename__ = "batter_pitch_type_tendencies"
    __table_args__ = (
        Index("ix_mlb_bpt_batter_id", "batter_id"),
        Index("ix_mlb_bpt_pitch_type", "pitch_type_code"),
        {"schema": "mlb"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    batter_id = Column(Integer, nullable=False)
    pitch_type_code = Column(String(5), nullable=False)
    season = Column(Integer, nullable=False)
    pitches_faced = Column(Integer, nullable=True)

    # Swing decisions
    swing_rate = Column(Float, nullable=True)
    whiff_rate = Column(Float, nullable=True)      # whiffs / swings
    contact_rate = Column(Float, nullable=True)    # contact / swings
    chase_rate = Column(Float, nullable=True)      # swings on balls / balls

    # Contact quality
    hard_hit_rate = Column(Float, nullable=True)   # launch_speed >= 95
    avg_exit_velo = Column(Float, nullable=True)
    avg_launch_angle = Column(Float, nullable=True)
    barrel_rate = Column(Float, nullable=True)     # launch_speed >= 98 + angle

    # Outcomes
    csw_rate = Column(Float, nullable=True)        # called strike + whiff rate
    in_play_rate = Column(Float, nullable=True)
    avg_woba_on_contact = Column(Float, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class MLBBatterZoneTendencies(Base):
    """
    Per batter per zone (1-14) tendencies.
    Used to adjust called strike values by location.
    Min 20 pitches in zone to qualify.
    """
    __tablename__ = "batter_zone_tendencies"
    __table_args__ = (
        Index("ix_mlb_bzt_batter_id", "batter_id"),
        Index("ix_mlb_bzt_zone", "zone"),
        {"schema": "mlb"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    batter_id = Column(Integer, nullable=False)
    zone = Column(Integer, nullable=False)         # 1-14
    season = Column(Integer, nullable=False)
    pitches_faced = Column(Integer, nullable=True)

    # Zone discipline
    swing_rate = Column(Float, nullable=True)
    take_rate = Column(Float, nullable=True)       # 1 - swing_rate
    whiff_rate = Column(Float, nullable=True)
    contact_rate = Column(Float, nullable=True)

    # Contact quality in zone
    hard_hit_rate = Column(Float, nullable=True)
    avg_exit_velo = Column(Float, nullable=True)
    in_play_rate = Column(Float, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class MLBLinearWeights(Base):
    """
    wOBA linear weights by season computed from RE24.
    Used to value contact outcomes in BAPV.
    """
    __tablename__ = "linear_weights"
    __table_args__ = {"schema": "mlb"}

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    season = Column(Integer, unique=True, nullable=False)

    # Event weights (runs above average)
    weight_out = Column(Float, nullable=True)
    weight_bb = Column(Float, nullable=True)
    weight_hbp = Column(Float, nullable=True)
    weight_single = Column(Float, nullable=True)
    weight_double = Column(Float, nullable=True)
    weight_triple = Column(Float, nullable=True)
    weight_hr = Column(Float, nullable=True)

    # wOBA scale factor
    woba_scale = Column(Float, nullable=True)
    league_woba = Column(Float, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

class MLBPitchQualityScore(Base):
    """
    Per-game BAPV+ pitch quality scores.
    One row per pitcher per pitch type per game.
    Aggregates to any window: season, rolling, career.
    """
    __tablename__ = "pitch_quality_scores"
    __table_args__ = (
        Index("ix_mlb_pqs_pitcher_id", "pitcher_id"),
        Index("ix_mlb_pqs_game_pk", "game_pk"),
        Index("ix_mlb_pqs_game_date", "game_date"),
        Index("ix_mlb_pqs_season", "season"),
        UniqueConstraint("pitcher_id", "pitch_type_code", "game_pk",
                        name="uq_pqs_pitcher_type_game"),
        {"schema": "mlb"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    pitcher_id = Column(Integer, nullable=False)
    pitcher_name = Column(String(100), nullable=True)
    pitch_type_code = Column(String(5), nullable=False)
    game_pk = Column(BigInteger, nullable=False)
    game_date = Column(String(10), nullable=True)   # YYYY-MM-DD
    season = Column(Integer, nullable=False)
    game_type = Column(String(5), nullable=True)    # R, S, F etc

    # Volume
    pitches_thrown = Column(Integer, nullable=True)

    # BAPV scores
    avg_bapv = Column(Float, nullable=True)
    bapv_plus = Column(Float, nullable=True)        # normalized 100 = avg

    # Physical characteristics this game
    avg_velo = Column(Float, nullable=True)
    avg_spin = Column(Float, nullable=True)
    avg_hmov = Column(Float, nullable=True)
    avg_vmov = Column(Float, nullable=True)

    # Outcome rates this game
    whiff_rate = Column(Float, nullable=True)
    cs_rate = Column(Float, nullable=True)
    csw_rate = Column(Float, nullable=True)
    in_play_rate = Column(Float, nullable=True)
    hard_hit_rate = Column(Float, nullable=True)

    # Season rolling context (populated at query time, not stored)
    # season_bapv_plus, last_30_bapv_plus computed from aggregating rows

    created_at = Column(DateTime(timezone=True), server_default=func.now())