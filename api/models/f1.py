"""
F1 schema - fully self-contained.

Primary data sources:
  - Ergast API (ergast.com/api/f1) — historical results, lap times, standings (free)
  - OpenF1 API (api.openf1.org)   — real-time telemetry during race weekends (free)

Tables:
  f1.raw_events       Raw API blobs. One row per API response.
  f1.races            One row per race (Grand Prix).
  f1.results          One row per driver per race (finishing position, points, etc.)
  f1.lap_times        One row per lap per driver.
  f1.pit_stops        One row per pit stop.
  f1.drivers          Driver reference table.
  f1.constructors     Constructor (team) reference table.
"""

from sqlalchemy import (
    Column, Integer, String, DateTime, Text,
    Float, Boolean, BigInteger, Index
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from models.base import Base


class F1RawEvent(Base):
    """
    Raw landing zone for F1 API responses (Ergast + OpenF1).
    source column distinguishes which API the blob came from.
    """
    __tablename__ = "raw_events"
    __table_args__ = (
        Index("ix_f1_raw_events_season_round", "season", "round"),
        Index("ix_f1_raw_events_source", "source"),
        Index("ix_f1_raw_events_event_type", "event_type"),
        {"schema": "f1"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    source = Column(String(20), nullable=False)             # 'ergast' or 'openf1'
    event_type = Column(String(50), nullable=False)         # 'race_results', 'lap_times', 'telemetry', etc.
    season = Column(Integer, nullable=True)
    round = Column(Integer, nullable=True)                  # round number within season
    circuit_id = Column(String(50), nullable=True)          # e.g. 'monza', 'spa'
    endpoint = Column(Text, nullable=True)
    data = Column(JSONB, nullable=False)
    fetched_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class F1Race(Base):
    """
    One row per race (Grand Prix event).
    """
    __tablename__ = "races"
    __table_args__ = (
        Index("ix_f1_races_season", "season"),
        {"schema": "f1"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    season = Column(Integer, nullable=False)
    round = Column(Integer, nullable=False)
    race_name = Column(String(100), nullable=True)          # 'Italian Grand Prix'
    circuit_id = Column(String(50), nullable=True)
    circuit_name = Column(String(100), nullable=True)
    country = Column(String(100), nullable=True)
    locality = Column(String(100), nullable=True)
    race_date = Column(String(10), nullable=True)           # YYYY-MM-DD
    race_time = Column(String(10), nullable=True)           # HH:MM:SSZ
    url = Column(Text, nullable=True)                       # Wikipedia link from Ergast
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class F1Result(Base):
    """
    One row per driver per race. Finishing positions, points, status.
    """
    __tablename__ = "results"
    __table_args__ = (
        Index("ix_f1_results_season_round", "season", "round"),
        Index("ix_f1_results_driver_id", "driver_id"),
        Index("ix_f1_results_constructor_id", "constructor_id"),
        {"schema": "f1"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    season = Column(Integer, nullable=False)
    round = Column(Integer, nullable=False)
    driver_id = Column(String(50), nullable=False)          # Ergast driver id e.g. 'hamilton'
    constructor_id = Column(String(50), nullable=True)      # e.g. 'mercedes'
    grid = Column(Integer, nullable=True)                   # starting grid position
    position = Column(Integer, nullable=True)               # finishing position (null if DNF)
    position_text = Column(String(5), nullable=True)        # 'R' for retired, 'D' for DSQ
    position_order = Column(Integer, nullable=True)         # always numeric for sorting
    points = Column(Float, nullable=True)
    laps = Column(Integer, nullable=True)
    status = Column(String(100), nullable=True)             # 'Finished', '+1 Lap', 'Engine', etc.
    time_millis = Column(BigInteger, nullable=True)         # race time in ms
    time_display = Column(String(20), nullable=True)        # '1:32:04.512'
    fastest_lap_rank = Column(Integer, nullable=True)
    fastest_lap_time = Column(String(20), nullable=True)
    fastest_lap_speed = Column(Float, nullable=True)        # kph
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class F1LapTime(Base):
    """
    One row per lap per driver. Most granular table for race analysis.
    """
    __tablename__ = "lap_times"
    __table_args__ = (
        Index("ix_f1_lap_times_season_round", "season", "round"),
        Index("ix_f1_lap_times_driver_id", "driver_id"),
        {"schema": "f1"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    season = Column(Integer, nullable=False)
    round = Column(Integer, nullable=False)
    driver_id = Column(String(50), nullable=False)
    lap = Column(Integer, nullable=False)
    position = Column(Integer, nullable=True)               # track position on this lap
    time_display = Column(String(20), nullable=True)        # '1:23.456'
    time_millis = Column(BigInteger, nullable=True)         # lap time in ms
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class F1PitStop(Base):
    """
    One row per pit stop.
    """
    __tablename__ = "pit_stops"
    __table_args__ = (
        Index("ix_f1_pit_stops_season_round", "season", "round"),
        Index("ix_f1_pit_stops_driver_id", "driver_id"),
        {"schema": "f1"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    season = Column(Integer, nullable=False)
    round = Column(Integer, nullable=False)
    driver_id = Column(String(50), nullable=False)
    stop = Column(Integer, nullable=True)                   # stop number (1st, 2nd, etc.)
    lap = Column(Integer, nullable=True)                    # lap on which stop occurred
    time_of_day = Column(String(10), nullable=True)         # time of day HH:MM:SS
    duration_display = Column(String(10), nullable=True)    # '23.456'
    duration_millis = Column(BigInteger, nullable=True)     # stop duration in ms
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class F1Qualifying(Base):
    """One row per driver per qualifying session."""
    __tablename__ = "qualifying"
    __table_args__ = (
        Index("ix_f1_qualifying_season_round", "season", "round"),
        Index("ix_f1_qualifying_driver_id", "driver_id"),
        {"schema": "f1"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    season = Column(Integer, nullable=False)
    round = Column(Integer, nullable=False)
    driver_id = Column(String(50), nullable=False)
    constructor_id = Column(String(50), nullable=True)
    position = Column(Integer, nullable=True)
    q1_time = Column(String(20), nullable=True)
    q2_time = Column(String(20), nullable=True)
    q3_time = Column(String(20), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class F1Driver(Base):
    """Driver reference table."""
    __tablename__ = "drivers"
    __table_args__ = {"schema": "f1"}

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    driver_id = Column(String(50), unique=True, nullable=False)  # Ergast id e.g. 'verstappen'
    permanent_number = Column(Integer, nullable=True)
    code = Column(String(5), nullable=True)                 # 'VER', 'HAM', etc.
    given_name = Column(String(50), nullable=True)
    family_name = Column(String(50), nullable=True)
    date_of_birth = Column(String(10), nullable=True)
    nationality = Column(String(50), nullable=True)
    url = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class F1Constructor(Base):
    """Constructor (team) reference table."""
    __tablename__ = "constructors"
    __table_args__ = {"schema": "f1"}

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    constructor_id = Column(String(50), unique=True, nullable=False)  # e.g. 'red_bull'
    name = Column(String(100), nullable=True)
    nationality = Column(String(50), nullable=True)
    url = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
