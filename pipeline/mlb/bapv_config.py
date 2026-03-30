"""
BAPV+ Model Configuration — v2.0
Batter-Adjusted Pitch Value

Version history:
  v1.0 — Initial implementation, equal CS/whiff weighting
  v2.0 — Reduced CS weight (0.080→0.025), chase rate adjustment,
          hard-hit out penalty (0.5x), whiff denominator fixed to swings

Validation (2024→2025, n=170):
  BAPV+ vs SIERA next year: -0.560
  Stuff+  vs SIERA next year: -0.508
  BAPV+ beats Stuff+ on all cross-season metrics
"""

BAPV_VERSION = "2.0"

# Base values before batter adjustment
BASE_WHIFF_VALUE  =  0.170   # swinging strike — scaled by batter whiff rate vs pitch type
BASE_CS_VALUE     =  0.025   # called strike — scaled by batter chase rate
BASE_FOUL_VALUE   =  0.020   # foul ball — flat, no batter adjustment
BASE_BALL_VALUE   = -0.040   # ball — flat negative
BASE_HBP_VALUE    = -0.150   # hit by pitch

# Contact adjustments
HARD_HIT_THRESHOLD   = 95    # mph — above this = hard contact
SOFT_HIT_THRESHOLD   = 85    # mph — below this = soft contact
HARD_HIT_OUT_MULT    = 0.50  # hard hit out gets half credit (still an out, but bad pitch)
SOFT_CONTACT_MULT    = 0.85  # soft contact out gets slight bonus

# League average fallbacks (2025)
LEAGUE_WHIFF_RATE    = 0.267
LEAGUE_CHASE_RATE    = 0.310
LEAGUE_HARD_HIT_RATE = 0.395
LEAGUE_TAKE_RATE     = 0.465

# Minimum pitches to display score
MIN_PITCHES_SEASON   = 200   # for leaderboard
MIN_PITCHES_GAME     = 1     # for live display (show all, flag small samples)
SMALL_SAMPLE_WARNING = 100   # flag in UI if below this

# wOBA linear weights (2024-2025 era)
WOBA_WEIGHTS = {
    'single':    0.888,
    'double':    1.271,
    'triple':    1.616,
    'home_run':  2.101,
    'walk':      0.690,
    'hit_by_pitch': 0.720,
    'out':      -0.098,
}

# Call code mappings
WHIFF_CODES   = {'S', 'W', 'T'}        # swinging strikes
CS_CODES      = {'C'}                   # called strikes
FOUL_CODES    = {'F', 'D', 'E', 'L'}   # foul balls
BALL_CODES    = {'B', '*B', 'H'}        # balls
IN_PLAY_CODE  = 'X'                     # in play
HBP_CODE      = 'M'                     # hit by pitch

# Whiff rate denominator (swings only, not total pitches — matches Baseball Savant)
SWING_CODES   = {'S', 'W', 'T', 'F', 'D', 'E', 'X'}
