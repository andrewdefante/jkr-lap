"""
NFL schema - fully self-contained.

Tables:
  nfl.games                 One row per game.
  nfl.plays                 One row per play (nflfastR play-by-play).
  nfl.player_stats_weekly   One row per player per week.
  nfl.snap_counts           One row per player per game (offense/defense/ST snap %).
  nfl.players               Player reference table (gsis_id keyed).
  nfl.team_metrics_weekly   One row per team per game — EPA/success rate rollups.

All tables live in the 'nfl' Postgres schema. No cross-sport dependencies.
Source: nflverse-data public releases (https://github.com/nflverse/nflverse-data).
"""

from sqlalchemy import (
    Column, BigInteger, Integer, String, Float, Boolean,
    Date, DateTime, ForeignKey, Index, UniqueConstraint
)
from sqlalchemy.sql import func
from models.base import Base


class NFLGame(Base):
    """One row per game. Sourced from the nflverse 'schedules' release."""
    __tablename__ = "games"
    __table_args__ = (
        Index("ix_nfl_games_season", "season"),
        Index("ix_nfl_games_season_week", "season", "week"),
        {"schema": "nfl"},
    )

    game_id = Column(String(20), primary_key=True)
    season = Column(Integer, nullable=False)
    game_type = Column(String(10), nullable=True)            # REG, POST, PRE
    week = Column(Integer, nullable=True)
    game_date = Column(Date, nullable=True)
    game_datetime = Column(DateTime(timezone=True), nullable=True)
    home_team = Column(String(5), nullable=True)
    away_team = Column(String(5), nullable=True)
    home_score = Column(Integer, nullable=True)
    away_score = Column(Integer, nullable=True)
    home_win = Column(Boolean, nullable=True)
    overtime = Column(Boolean, nullable=True)
    roof = Column(String(20), nullable=True)                 # outdoors, dome, retractable
    surface = Column(String(20), nullable=True)
    temp = Column(Integer, nullable=True)
    wind = Column(Integer, nullable=True)
    stadium = Column(String(100), nullable=True)
    location = Column(String(100), nullable=True)
    spread_line = Column(Float, nullable=True)               # Vegas spread (negative = home favored)
    total_line = Column(Float, nullable=True)                # Vegas over/under
    div_game = Column(Boolean, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class NFLPlay(Base):
    """
    One row per play. Transformed from nflverse play-by-play (nflfastR).
    Includes EPA/WP metrics and passer/rusher/receiver attribution.
    """
    __tablename__ = "plays"
    __table_args__ = (
        Index("ix_nfl_plays_game", "game_id"),
        Index("ix_nfl_plays_season_week", "season", "week"),
        Index("ix_nfl_plays_passer", "passer_id"),
        Index("ix_nfl_plays_rusher", "rusher_id"),
        Index("ix_nfl_plays_receiver", "receiver_id"),
        {"schema": "nfl"},
    )

    play_id = Column(BigInteger, primary_key=True, autoincrement=True)
    game_id = Column(String(20), ForeignKey("nfl.games.game_id"), nullable=True)
    season = Column(Integer, nullable=True)
    week = Column(Integer, nullable=True)
    play_index = Column(Integer, nullable=True)               # nflverse play_id — order within game
    game_seconds_remaining = Column(Integer, nullable=True)
    quarter_seconds_remaining = Column(Integer, nullable=True)
    quarter = Column(Integer, nullable=True)
    down = Column(Integer, nullable=True)
    ydstogo = Column(Integer, nullable=True)
    yardline_100 = Column(Integer, nullable=True)             # yards from own end zone
    play_type = Column(String(20), nullable=True)             # pass, run, punt, kickoff, etc.
    yards_gained = Column(Integer, nullable=True)
    touchdown = Column(Boolean, nullable=True)
    first_down = Column(Boolean, nullable=True)

    # EPA metrics
    epa = Column(Float, nullable=True)
    wp = Column(Float, nullable=True)                         # win probability before play
    wpa = Column(Float, nullable=True)                        # win probability added

    # Passer
    passer_id = Column(String(20), nullable=True)
    passer_name = Column(String(100), nullable=True)

    # Rusher
    rusher_id = Column(String(20), nullable=True)
    rusher_name = Column(String(100), nullable=True)

    # Receiver
    receiver_id = Column(String(20), nullable=True)
    receiver_name = Column(String(100), nullable=True)

    # Pass details
    pass_length = Column(String(10), nullable=True)           # short, deep
    pass_location = Column(String(10), nullable=True)         # left, middle, right
    air_yards = Column(Integer, nullable=True)
    yards_after_catch = Column(Integer, nullable=True)
    complete_pass = Column(Boolean, nullable=True)
    interception = Column(Boolean, nullable=True)

    # Rush details
    run_location = Column(String(10), nullable=True)
    run_gap = Column(String(10), nullable=True)

    # Situation
    posteam = Column(String(5), nullable=True)                # possession team
    defteam = Column(String(5), nullable=True)
    posteam_score = Column(Integer, nullable=True)
    defteam_score = Column(Integer, nullable=True)
    score_differential = Column(Integer, nullable=True)

    # Penalty
    penalty = Column(Boolean, nullable=True)
    penalty_team = Column(String(5), nullable=True)
    penalty_yards = Column(Integer, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())


class NFLPlayerStatsWeekly(Base):
    """One row per player per week. Sourced from nflverse 'stats_player' weekly release."""
    __tablename__ = "player_stats_weekly"
    __table_args__ = (
        UniqueConstraint("player_id", "season", "week", name="uq_nfl_player_stats_weekly"),
        Index("ix_nfl_stats_player", "player_id"),
        Index("ix_nfl_stats_season_week", "season", "week"),
        Index("ix_nfl_stats_team", "team"),
        {"schema": "nfl"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    player_id = Column(String(20), nullable=False)
    player_name = Column(String(100), nullable=True)
    player_display_name = Column(String(100), nullable=True)
    position = Column(String(5), nullable=True)
    position_group = Column(String(10), nullable=True)
    team = Column(String(5), nullable=True)
    season = Column(Integer, nullable=False)
    week = Column(Integer, nullable=False)
    game_id = Column(String(20), nullable=True)

    # Passing
    completions = Column(Integer, default=0)
    attempts = Column(Integer, default=0)
    passing_yards = Column(Integer, default=0)
    passing_tds = Column(Integer, default=0)
    interceptions = Column(Integer, default=0)
    sacks = Column(Integer, default=0)
    sack_yards = Column(Integer, default=0)

    # Rushing
    carries = Column(Integer, default=0)
    rushing_yards = Column(Integer, default=0)
    rushing_tds = Column(Integer, default=0)

    # Receiving
    targets = Column(Integer, default=0)
    receptions = Column(Integer, default=0)
    receiving_yards = Column(Integer, default=0)
    receiving_tds = Column(Integer, default=0)

    # Fantasy
    fantasy_points = Column(Float, default=0)
    fantasy_points_ppr = Column(Float, default=0)

    # Advanced
    air_yards_share = Column(Float, nullable=True)
    target_share = Column(Float, nullable=True)
    wopr = Column(Float, nullable=True)                       # weighted opportunity rating
    racr = Column(Float, nullable=True)                       # receiver air conversion ratio

    created_at = Column(DateTime(timezone=True), server_default=func.now())


class NFLSnapCount(Base):
    """One row per player per game. Sourced from nflverse 'snap_counts' release (PFR ids)."""
    __tablename__ = "snap_counts"
    __table_args__ = (
        UniqueConstraint("player_id", "game_id", name="uq_nfl_snap_counts"),
        {"schema": "nfl"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    game_id = Column(String(20), nullable=True)
    season = Column(Integer, nullable=True)
    week = Column(Integer, nullable=True)
    player_id = Column(String(20), nullable=True)             # PFR player id
    player_name = Column(String(100), nullable=True)
    team = Column(String(5), nullable=True)
    position = Column(String(5), nullable=True)
    offense_snaps = Column(Integer, default=0)
    offense_pct = Column(Float, nullable=True)
    defense_snaps = Column(Integer, default=0)
    defense_pct = Column(Float, nullable=True)
    st_snaps = Column(Integer, default=0)
    st_pct = Column(Float, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class NFLPlayer(Base):
    """
    Player reference table. Sourced from the nflverse 'players' release.
    Not season-scoped — one row per player across their whole career.
    """
    __tablename__ = "players"
    __table_args__ = {"schema": "nfl"}

    gsis_id = Column(String(20), primary_key=True)
    player_name = Column(String(100), nullable=True)
    first_name = Column(String(50), nullable=True)
    last_name = Column(String(50), nullable=True)
    position = Column(String(5), nullable=True)
    position_group = Column(String(10), nullable=True)
    team = Column(String(5), nullable=True)
    jersey_number = Column(Integer, nullable=True)
    height = Column(String(10), nullable=True)                # inches
    weight = Column(Integer, nullable=True)
    birth_date = Column(Date, nullable=True)
    college = Column(String(255), nullable=True)
    draft_year = Column(Integer, nullable=True)
    draft_round = Column(Integer, nullable=True)
    draft_pick = Column(Integer, nullable=True)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())


class NFLTeamMetricsWeekly(Base):
    """
    One row per team per game — EPA/success rate rollups built from nfl.plays.
    Populated by fetch_nfl.py's build_team_metrics() after plays are loaded.
    """
    __tablename__ = "team_metrics_weekly"
    __table_args__ = (
        UniqueConstraint("team", "game_id", name="uq_nfl_team_metrics_weekly"),
        {"schema": "nfl"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    team = Column(String(5), nullable=True)
    season = Column(Integer, nullable=True)
    week = Column(Integer, nullable=True)
    game_id = Column(String(20), nullable=True)
    home_away = Column(String(5), nullable=True)

    # Offense
    off_epa_per_play = Column(Float, nullable=True)
    off_epa_pass = Column(Float, nullable=True)
    off_epa_rush = Column(Float, nullable=True)
    off_success_rate = Column(Float, nullable=True)
    off_plays = Column(Integer, nullable=True)
    off_yards = Column(Integer, nullable=True)
    off_tds = Column(Integer, nullable=True)
    off_third_down_pct = Column(Float, nullable=True)
    off_red_zone_pct = Column(Float, nullable=True)

    # Defense
    def_epa_per_play = Column(Float, nullable=True)
    def_epa_pass = Column(Float, nullable=True)
    def_epa_rush = Column(Float, nullable=True)
    def_success_rate = Column(Float, nullable=True)
    def_plays = Column(Integer, nullable=True)
    def_yards_allowed = Column(Integer, nullable=True)
    def_tds_allowed = Column(Integer, nullable=True)
    def_third_down_pct = Column(Float, nullable=True)
    def_red_zone_pct = Column(Float, nullable=True)

    # Result
    points_scored = Column(Integer, nullable=True)
    points_allowed = Column(Integer, nullable=True)
    won = Column(Boolean, nullable=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
