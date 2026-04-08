from sqlalchemy import (
    Column, Integer, String, DateTime, Text,
    Float, Boolean, BigInteger, Index
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from models.base import Base


class NASCARRawEvent(Base):
    __tablename__ = "raw_events"
    __table_args__ = (
        Index("ix_nascar_raw_season_series_race", "season", "series_id", "race_id"),
        Index("ix_nascar_raw_endpoint_type", "endpoint_type"),
        {"schema": "nascar"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    season = Column(Integer, nullable=False)
    series_id = Column(Integer, nullable=False)
    race_id = Column(Integer, nullable=False)
    endpoint_type = Column(String(50), nullable=False)
    endpoint = Column(Text, nullable=True)
    data = Column(JSONB, nullable=False)
    fetched_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class NASCARRace(Base):
    __tablename__ = "races"
    __table_args__ = (
        Index("ix_nascar_races_season_series", "season", "series_id"),
        Index("ix_nascar_races_race_date", "race_date"),
        {"schema": "nascar"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    race_id = Column(Integer, unique=True, nullable=False)
    series_id = Column(Integer, nullable=False)
    season = Column(Integer, nullable=False)
    race_name = Column(String(200), nullable=True)
    track_name = Column(String(100), nullable=True)
    track_id = Column(Integer, nullable=True)
    track_length = Column(Float, nullable=True)
    track_type = Column(String(50), nullable=True)
    race_date = Column(String(10), nullable=True)
    scheduled_laps = Column(Integer, nullable=True)
    actual_laps = Column(Integer, nullable=True)
    total_miles = Column(Float, nullable=True)
    caution_laps = Column(Integer, nullable=True)
    caution_count = Column(Integer, nullable=True)
    lead_changes = Column(Integer, nullable=True)
    leaders_count = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class NASCARResult(Base):
    __tablename__ = "results"
    __table_args__ = (
        Index("ix_nascar_results_race_id", "race_id"),
        Index("ix_nascar_results_driver_id", "driver_id"),
        Index("ix_nascar_results_season_series", "season", "series_id"),
        {"schema": "nascar"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    race_id = Column(Integer, nullable=False)
    season = Column(Integer, nullable=False)
    series_id = Column(Integer, nullable=False)
    driver_id = Column(Integer, nullable=False)
    driver_name = Column(String(100), nullable=True)
    car_number = Column(String(5), nullable=True)
    team_name = Column(String(100), nullable=True)
    manufacturer = Column(String(50), nullable=True)
    start_position = Column(Integer, nullable=True)
    finish_position = Column(Integer, nullable=True)
    laps_completed = Column(Integer, nullable=True)
    laps_led = Column(Integer, nullable=True)
    finishing_status = Column(String(100), nullable=True)
    dnf = Column(Boolean, nullable=True)
    points = Column(Integer, nullable=True)
    playoff_points = Column(Integer, nullable=True)
    stage_1_points = Column(Integer, nullable=True)
    stage_2_points = Column(Integer, nullable=True)
    stage_3_points = Column(Integer, nullable=True)
    avg_position = Column(Float, nullable=True)
    avg_speed = Column(Float, nullable=True)
    fastest_lap_speed = Column(Float, nullable=True)
    pit_stop_count = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class NASCARLap(Base):
    __tablename__ = "laps"
    __table_args__ = (
        Index("ix_nascar_laps_race_driver", "race_id", "driver_id"),
        Index("ix_nascar_laps_race_lap", "race_id", "lap_number"),
        {"schema": "nascar"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    race_id = Column(Integer, nullable=False)
    season = Column(Integer, nullable=False)
    series_id = Column(Integer, nullable=False)
    driver_id = Column(Integer, nullable=False)
    driver_name = Column(String(100), nullable=True)
    car_number = Column(String(5), nullable=True)
    lap_number = Column(Integer, nullable=False)
    position = Column(Integer, nullable=True)
    lap_time = Column(Float, nullable=True)
    lap_speed = Column(Float, nullable=True)
    is_caution = Column(Boolean, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class NASCARPitStop(Base):
    __tablename__ = "pit_stops"
    __table_args__ = (
        Index("ix_nascar_pit_race_driver", "race_id", "driver_id"),
        {"schema": "nascar"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    race_id = Column(Integer, nullable=False)
    season = Column(Integer, nullable=False)
    series_id = Column(Integer, nullable=False)
    driver_id = Column(Integer, nullable=False)
    driver_name = Column(String(100), nullable=True)
    car_number = Column(String(5), nullable=True)
    stop_number = Column(Integer, nullable=True)
    lap = Column(Integer, nullable=True)
    pit_in_lap = Column(Integer, nullable=True)
    pit_out_lap = Column(Integer, nullable=True)
    duration_seconds = Column(Float, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class NASCARCaution(Base):
    __tablename__ = "cautions"
    __table_args__ = (
        Index("ix_nascar_cautions_race_id", "race_id"),
        {"schema": "nascar"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    race_id = Column(Integer, nullable=False)
    season = Column(Integer, nullable=False)
    series_id = Column(Integer, nullable=False)
    caution_number = Column(Integer, nullable=True)
    start_lap = Column(Integer, nullable=True)
    end_lap = Column(Integer, nullable=True)
    laps_under_caution = Column(Integer, nullable=True)
    reason = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class NASCARLeadChange(Base):
    __tablename__ = "lead_changes"
    __table_args__ = (
        Index("ix_nascar_lead_changes_race_id", "race_id"),
        {"schema": "nascar"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    race_id = Column(Integer, nullable=False)
    season = Column(Integer, nullable=False)
    series_id = Column(Integer, nullable=False)
    lap = Column(Integer, nullable=True)
    driver_id = Column(Integer, nullable=True)
    driver_name = Column(String(100), nullable=True)
    car_number = Column(String(5), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class NASCARStageResult(Base):
    __tablename__ = "stage_results"
    __table_args__ = (
        Index("ix_nascar_stage_results_race_id", "race_id"),
        {"schema": "nascar"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    race_id = Column(Integer, nullable=False)
    season = Column(Integer, nullable=False)
    series_id = Column(Integer, nullable=False)
    stage_number = Column(Integer, nullable=False)
    driver_id = Column(Integer, nullable=True)
    driver_name = Column(String(100), nullable=True)
    car_number = Column(String(5), nullable=True)
    finish_position = Column(Integer, nullable=True)
    laps_led = Column(Integer, nullable=True)
    points = Column(Integer, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class NASCARDriverStat(Base):
    __tablename__ = "driver_stats"
    __table_args__ = (
        Index("ix_nascar_driver_stats_race_driver", "race_id", "driver_id"),
        {"schema": "nascar"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    race_id = Column(Integer, nullable=False)
    season = Column(Integer, nullable=False)
    series_id = Column(Integer, nullable=False)
    driver_id = Column(Integer, nullable=False)
    driver_name = Column(String(100), nullable=True)
    car_number = Column(String(5), nullable=True)
    avg_running_position = Column(Float, nullable=True)
    fastest_lap_time = Column(Float, nullable=True)
    fastest_lap_speed = Column(Float, nullable=True)
    fastest_lap_number = Column(Integer, nullable=True)
    quality_passes = Column(Integer, nullable=True)
    percent_quality_passes = Column(Float, nullable=True)
    green_flag_passes = Column(Integer, nullable=True)
    green_flag_passed = Column(Integer, nullable=True)
    quality_pass_differential = Column(Integer, nullable=True)
    driver_rating = Column(Float, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class NASCARDriver(Base):
    __tablename__ = "drivers"
    __table_args__ = {"schema": "nascar"}

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    driver_id = Column(Integer, unique=True, nullable=False)
    full_name = Column(String(100), nullable=True)
    first_name = Column(String(50), nullable=True)
    last_name = Column(String(50), nullable=True)
    active = Column(Boolean, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class NASCARLiveSnapshot(Base):
    """
    Point-in-time snapshot of live race data.
    One row per driver per snapshot. Polled every 30 seconds during race.
    Used to build projected finish model.
    """
    __tablename__ = "live_snapshots"
    __table_args__ = (
        Index("ix_nascar_live_race_id", "race_id"),
        Index("ix_nascar_live_snapshot_time", "snapshot_at"),
        {"schema": "nascar"},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    race_id = Column(Integer, nullable=False)
    season = Column(Integer, nullable=False)
    series_id = Column(Integer, nullable=False)
    snapshot_at = Column(DateTime(timezone=True), server_default=func.now())
    lap = Column(Integer, nullable=True)
    total_laps = Column(Integer, nullable=True)

    driver_id = Column(Integer, nullable=True)
    driver_name = Column(String(100), nullable=True)
    car_number = Column(String(5), nullable=True)
    manufacturer = Column(String(50), nullable=True)
    position = Column(Integer, nullable=True)
    laps_completed = Column(Integer, nullable=True)
    laps_led = Column(Integer, nullable=True)
    last_lap_time = Column(Float, nullable=True)
    last_lap_speed = Column(Float, nullable=True)
    best_lap_time = Column(Float, nullable=True)
    best_lap_speed = Column(Float, nullable=True)
    pit_stops = Column(Integer, nullable=True)
    status = Column(String(50), nullable=True)
    delta_leader = Column(Float, nullable=True)
    last_pit_lap = Column(Integer, nullable=True)
    tire_age = Column(Integer, nullable=True)   # laps on current set