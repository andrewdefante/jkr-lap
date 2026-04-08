"""
Models package. Import Base and all models here so:
  1. Alembic can discover all tables via Base.metadata
  2. main.py can create schemas + tables on startup with a single import
"""

from models.base import Base
from models.mlb import (
    MLBRawEvent, MLBGame, MLBAtBat, MLBPitch, MLBPlayer,
    MLBRunner, MLBLinescore, MLBBoxscoreBatting,
    MLBBoxscorePitching, MLBBoxscoreFielding, MLBFieldingCredit,
    MLBFangraphsBatting, MLBFangraphsPitching, MLBPlayerIDMap,
    MLBBatterPitchTypeTendencies, MLBBatterZoneTendencies,
    MLBLinearWeights, MLBPitchQualityScore
)
from models.f1 import (
    F1RawEvent, F1Race, F1Result, F1LapTime,
    F1PitStop, F1Qualifying, F1Driver, F1Constructor
)
from models.nascar import (
    NASCARRawEvent, NASCARRace, NASCARResult, NASCARLap,
    NASCARPitStop, NASCARCaution, NASCARLeadChange,
    NASCARStageResult, NASCARDriverStat, NASCARDriver,
    NASCARLiveSnapshot
)
