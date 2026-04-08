"""
Batter-Adjusted Pitch Value (BAPV) — Per-Game Computation

Computes BAPV+ for each pitcher per pitch type per game.
Stores results in mlb.pitch_quality_scores.

Three temporal views supported:
  Season:   aggregate all game rows for the season
  Rolling:  filter by date range (last 30 days, last 5 starts)
  Live:     filter by today's game_pk

Usage:
    # Compute and store scores for a full season
    PYTHONPATH=/app python3 /pipeline/mlb/compute_bapv.py --season 2025

    # Compute for a specific date range
    PYTHONPATH=/app python3 /pipeline/mlb/compute_bapv.py --season 2025 --start 2025-04-01 --end 2025-04-30

    # Compute for a single game
    PYTHONPATH=/app python3 /pipeline/mlb/compute_bapv.py --game-pk 745528

    # Show leaderboard
    PYTHONPATH=/app python3 /pipeline/mlb/compute_bapv.py --season 2025 --leaderboard

    # Show pitcher profile
    PYTHONPATH=/app python3 /pipeline/mlb/compute_bapv.py --season 2025 --pitcher 694973
"""

import sys
import os
import argparse
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import text
from sqlalchemy import text as sqlt

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'api'))
from database import SessionLocal
from models.mlb import MLBPitchQualityScore

# Base values before batter adjustment
BASE_WHIFF_VALUE  =  0.170
BASE_CS_VALUE     =  0.025
BASE_FOUL_VALUE   =  0.020
BASE_BALL_VALUE   = -0.040
BASE_HBP_VALUE    = -0.150

# Minimum pitches per game to store a score
MIN_PITCHES_GAME  = 1

# Call code sets
WHIFF_CODES  = {'S', 'W', 'T'}
CS_CODES     = {'C'}
FOUL_CODES   = {'F', 'D', 'E', 'L'}
BALL_CODES   = {'B', '*B', 'H'}
IN_PLAY_CODE = 'X'
HBP_CODE     = 'M'


# ── Data Loading ──────────────────────────────────────────────────────────────

def load_tendencies(season: int, db) -> tuple:
    """Load batter tendencies and linear weights for a season."""

    type_sql = text("""
        SELECT batter_id, pitch_type_code,
               whiff_rate, chase_rate, hard_hit_rate,
               avg_exit_velo, csw_rate, contact_rate
        FROM mlb.batter_pitch_type_tendencies
        WHERE season = :season
    """)
    type_tend = pd.read_sql(type_sql, db.bind, params={"season": season})
    type_lookup = type_tend.set_index(
        ['batter_id', 'pitch_type_code']
    ).to_dict('index')

    zone_sql = text("""
        SELECT batter_id, zone, take_rate, whiff_rate, hard_hit_rate
        FROM mlb.batter_zone_tendencies
        WHERE season = :season
    """)
    zone_tend = pd.read_sql(zone_sql, db.bind, params={"season": season})
    zone_lookup = zone_tend.set_index(
        ['batter_id', 'zone']
    ).to_dict('index')

    lw_sql = text("""
        SELECT * FROM mlb.linear_weights
        WHERE season = :season
    """)
    lw = pd.read_sql(lw_sql, db.bind, params={"season": season})
    if lw.empty:
        lw = pd.read_sql(lw_sql, db.bind, params={"season": 2024})
    weights = lw.iloc[0].to_dict()

    league_avg = {
        'whiff_rate':   float(type_tend['whiff_rate'].mean()),
        'take_rate':    float(zone_tend['take_rate'].mean()),
        'chase_rate':   float(type_tend['chase_rate'].mean()),
        'hard_hit_rate': float(type_tend['hard_hit_rate'].mean()),
    }

    return type_lookup, zone_lookup, weights, league_avg


def load_pitches(season: int, db,
                 start_date: str = None,
                 end_date: str = None,
                 game_pk: int = None) -> pd.DataFrame:
    """Load pitches with game and at-bat context."""

    filters = ["g.season = :season",
               "g.game_type IN ('R', 'S')",
               "p.pitch_type_code IN ('FF','SI','SL','CH','FC','ST','CU','FS','KC')",
               "p.call_code IS NOT NULL",
               "ab.batter_id IS NOT NULL"]
    params = {"season": season}

    if start_date:
        filters.append("g.game_date >= :start_date")
        params["start_date"] = start_date
    if end_date:
        filters.append("g.game_date <= :end_date")
        params["end_date"] = end_date
    if game_pk:
        filters.append("p.game_pk = :game_pk")
        params["game_pk"] = game_pk

    where = " AND ".join(filters)

    sql = text(f"""
        SELECT
            p.game_pk,
            g.game_date,
            g.game_type,
            g.season,
            p.pitcher_id,
            ab.pitcher_name,
            p.pitch_type_code,
            p.call_code,
            p.zone,
            p.is_in_play,
            CASE WHEN p.call_code = 'X' THEN p.launch_speed ELSE NULL END as launch_speed,
            CASE WHEN p.call_code = 'X' THEN p.launch_angle ELSE NULL END as launch_angle,
            p.start_speed,
            p.spin_rate,
            p.pfx_x,
            p.pfx_z,
            ab.batter_id,
            CASE WHEN p.call_code = 'X' THEN ab.event_type ELSE NULL END as event_type
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE {where}
    """)

    df = pd.read_sql(sql, db.bind, params=params)
    return df


# ── BAPV Computation (vectorized) ─────────────────────────────────────────────

def compute_bapv_vectorized(pitches: pd.DataFrame,
                             type_lookup: dict,
                             zone_lookup: dict,
                             weights: dict,
                             league_avg: dict) -> pd.DataFrame:
    """
    Compute BAPV for each pitch using vectorized operations.
    Much faster than row-by-row iteration.
    """
    df = pitches.copy()

    # ── Look up batter tendencies ──────────────────────────────────────────
    # Map (batter_id, pitch_type) → whiff_rate
# ── Look up batter tendencies ──────────────────────────────────────────
    df['batter_whiff_rate'] = df.apply(
        lambda r: (type_lookup.get(
            (r['batter_id'], r['pitch_type_code']), {}
        ).get('whiff_rate') or league_avg['whiff_rate']),
        axis=1
    )

    df['batter_chase_rate'] = df.apply(
        lambda r: (type_lookup.get(
            (r['batter_id'], r['pitch_type_code']), {}
        ).get('chase_rate') or league_avg['chase_rate']),
        axis=1
    )

    df['batter_take_rate'] = df.apply(
        lambda r: (zone_lookup.get(
            (r['batter_id'], int(r['zone']) if pd.notna(r['zone']) else 0), {}
        ).get('take_rate') or league_avg['take_rate']),
        axis=1
    )

    df['batter_hard_hit'] = df.apply(
        lambda r: (type_lookup.get(
            (r['batter_id'], r['pitch_type_code']), {}
        ).get('hard_hit_rate') or league_avg['hard_hit_rate']),
        axis=1
    )

    # ── Outcome flags ──────────────────────────────────────────────────────
    df['is_whiff']    = df['call_code'].isin(WHIFF_CODES)
    df['is_cs']       = df['call_code'].isin(CS_CODES)
    df['is_foul']     = df['call_code'].isin(FOUL_CODES)
    df['is_ball']     = df['call_code'].isin(BALL_CODES)
    df['is_in_play']  = df['call_code'] == IN_PLAY_CODE
    df['is_hbp']      = df['call_code'] == HBP_CODE

    # ── Batter adjustments ────────────────────────────────────────────────
    # Whiff adj: harder to whiff contact hitters = more valuable
    df['whiff_adj'] = 1.0 + (
        league_avg['whiff_rate'] - df['batter_whiff_rate']
    ).clip(lower=0)

    # CS adj: harder to get called strikes vs patient hitters = more valuable
    # Disciplined batters (low chase) taking a strike = more impressive
    df['cs_adj'] = 1.0 + (
        league_avg['chase_rate'] - df['batter_chase_rate']
    ).clip(lower=0)

    # ── Linear weights for in-play events ─────────────────────────────────
    event_weights = {
        'single':                       weights.get('weight_single', 0.888),
        'double':                       weights.get('weight_double', 1.271),
        'triple':                       weights.get('weight_triple', 1.616),
        'home_run':                     weights.get('weight_hr', 2.101),
        'walk':                         weights.get('weight_bb', 0.690),
        'hit_by_pitch':                 weights.get('weight_hbp', 0.720),
        'field_out':                    weights.get('weight_out', -0.098),
        'grounded_into_double_play':    weights.get('weight_out', -0.098) * 2,
        'strikeout':                    weights.get('weight_out', -0.098),
        'force_out':                    weights.get('weight_out', -0.098),
        'sac_fly':                      weights.get('weight_out', -0.098) * 0.5,
        'sac_bunt':                     weights.get('weight_out', -0.098) * 0.5,
    }
    default_out = weights.get('weight_out', -0.098)

    df['event_weight'] = df['event_type'].str.lower().map(event_weights).fillna(default_out)

    # Hard hit adjustment on in-play events
    HIT_EVENTS = {'single', 'double', 'triple', 'home_run'}
    df['is_hit'] = df['event_type'].str.lower().isin(HIT_EVENTS)

    hard_hit = df['is_in_play'] & (df['launch_speed'] >= 95)
    hard_hit_surprise = 1.0 + (league_avg['hard_hit_rate'] - df['batter_hard_hit']).clip(lower=0)

    df['hard_hit_surprise'] = np.where(
        hard_hit & df['is_hit'],
        hard_hit_surprise,                          # full penalty for hard-hit balls in play
        np.where(
            hard_hit & ~df['is_hit'],
            0.5,                                    # half credit for hard-hit outs
            np.where(
                df['is_in_play'] & (df['launch_speed'] < 85),
                0.85,
                1.0
            )
        )
    )
    df['adj_event_weight'] = df['event_weight'] * df['hard_hit_surprise']

    # ── Compute BAPV per pitch ─────────────────────────────────────────────
    df['bapv'] = np.select(
        [
            df['is_whiff'],
            df['is_cs'],
            df['is_foul'],
            df['is_ball'],
            df['is_in_play'],
            df['is_hbp'],
        ],
        [
            BASE_WHIFF_VALUE * df['whiff_adj'],
            BASE_CS_VALUE * df['cs_adj'],
            BASE_FOUL_VALUE,
            BASE_BALL_VALUE,
            -df['adj_event_weight'],
            BASE_HBP_VALUE,
        ],
        default=0.0
    )

    return df


# ── Aggregation ───────────────────────────────────────────────────────────────

def aggregate_per_game(pitches: pd.DataFrame) -> pd.DataFrame:
    """Aggregate BAPV to pitcher-pitch type-game level."""

    agg = pitches.groupby([
        'game_pk', 'game_date', 'game_type', 'season',
        'pitcher_id', 'pitcher_name', 'pitch_type_code'
    ]).agg(
        pitches_thrown  = ('bapv', 'count'),
        avg_bapv        = ('bapv', 'mean'),
        avg_velo        = ('start_speed', 'mean'),
        avg_spin        = ('spin_rate', 'mean'),
        avg_hmov        = ('pfx_x', 'mean'),
        avg_ivb         = ('pfx_z', 'mean'),
        whiff_rate=('call_code', lambda x: (
            x.isin({'S','W','T'}).sum() /
            max(x.isin({'S','W','T','F','D','E','X'}).sum(), 1)
        )),
        cs_rate         = ('is_cs', 'mean'),
        csw_rate        = ('call_code', lambda x: x.isin(
                            WHIFF_CODES | CS_CODES).mean()),
        in_play_rate    = ('is_in_play', 'mean'),
        hard_hit_rate   = ('launch_speed', lambda x: (x >= 95).mean()),
    ).reset_index()

    # Filter minimum pitches
    agg = agg[agg['pitches_thrown'] >= MIN_PITCHES_GAME]

    return agg


def normalize_bapv_plus(agg: pd.DataFrame, league_avg_bapv: float) -> pd.DataFrame:
    agg = agg.copy()
    type_avgs = agg.groupby('pitch_type_code')['avg_bapv'].agg(['mean','count'])
    
    def normalize_row(r):
        stats = type_avgs.loc[r['pitch_type_code']]
        # Need at least 3 rows to normalize within game, else use league avg
        if stats['count'] >= 3:
            type_avg = stats['mean']
        else:
            type_avg = league_avg_bapv
        if abs(type_avg) < 0.0001:
            return 100.0
        return round(r['avg_bapv'] / abs(type_avg) * 100, 1)
    
    agg['bapv_plus'] = agg.apply(normalize_row, axis=1)
    return agg

# ── Storage ───────────────────────────────────────────────────────────────────

def store_scores(agg: pd.DataFrame, db,
                 game_pk: int = None,
                 start_date: str = None,
                 end_date: str = None) -> int:
    """Upsert per-game scores into pitch_quality_scores."""

    # Delete existing rows for this scope
    if game_pk:
        db.execute(text(
            "DELETE FROM mlb.pitch_quality_scores WHERE game_pk = :gp"
        ), {"gp": game_pk})
    elif start_date and end_date:
        db.execute(text("""
            DELETE FROM mlb.pitch_quality_scores
            WHERE game_date BETWEEN :s AND :e
        """), {"s": start_date, "e": end_date})
    else:
        season = int(agg['season'].iloc[0]) if len(agg) > 0 else None
        if season:
            db.execute(text(
                "DELETE FROM mlb.pitch_quality_scores WHERE season = :s"
            ), {"s": season})
    db.commit()

    rows = []
    for _, r in agg.iterrows():
        rows.append(MLBPitchQualityScore(
            pitcher_id      = int(r['pitcher_id']),
            pitcher_name    = r.get('pitcher_name'),
            pitch_type_code = r['pitch_type_code'],
            game_pk         = int(r['game_pk']),
            game_date       = str(r['game_date'])[:10],
            season          = int(r['season']),
            game_type       = r.get('game_type'),
            pitches_thrown  = int(r['pitches_thrown']),
            avg_bapv        = float(r['avg_bapv']),
            bapv_plus       = float(r['bapv_plus']),
            avg_velo        = float(r['avg_velo']) if pd.notna(r['avg_velo']) else None,
            avg_spin        = float(r['avg_spin']) if pd.notna(r['avg_spin']) else None,
            avg_hmov        = float(r['avg_hmov']) if pd.notna(r['avg_hmov']) else None,
            avg_ivb         = float(r['avg_ivb']) if pd.notna(r['avg_ivb']) else None,
            whiff_rate      = float(r['whiff_rate']),
            cs_rate         = float(r['cs_rate']),
            csw_rate        = float(r['csw_rate']),
            in_play_rate    = float(r['in_play_rate']),
            hard_hit_rate   = float(r['hard_hit_rate']),
        ))

    db.bulk_save_objects(rows)
    db.commit()
    return len(rows)


# ── Display ───────────────────────────────────────────────────────────────────

def show_leaderboard(season: int, db, min_pitches: int = 1,
                     game_type: str = 'R'):
    """Print season leaderboard from stored scores."""
    sql = text("""
        SELECT
            pitcher_id,
            MAX(pitcher_name) as pitcher_name,
            pitch_type_code,
            SUM(pitches_thrown) as total_pitches,
            ROUND(AVG(bapv_plus)::numeric, 1) as season_bapv_plus,
            ROUND(AVG(avg_velo)::numeric, 1) as avg_velo,
            ROUND(AVG(whiff_rate)::numeric, 3) as whiff_rate,
            ROUND(AVG(csw_rate)::numeric, 3) as csw_rate,
            COUNT(DISTINCT game_pk) as games
        FROM mlb.pitch_quality_scores
        WHERE season = :season
        AND game_type = :game_type
        GROUP BY pitcher_id, pitch_type_code
        HAVING SUM(pitches_thrown) >= :min_pitches
        ORDER BY season_bapv_plus DESC
        LIMIT 30
    """)

    df = pd.read_sql(sql, db.bind, params={
        "season": season,
        "game_type": game_type,
        "min_pitches": min_pitches
    })

    print(f"\n  {'Pitcher':<25} {'Type':>4} {'BAPV+':>7} "
          f"{'Pitches':>8} {'Games':>6} {'Velo':>6} "
          f"{'Whiff%':>7} {'CSW%':>6}")
    print(f"  {'-'*25} {'-'*4} {'-'*7} {'-'*8} "
          f"{'-'*6} {'-'*6} {'-'*7} {'-'*6}")

    for _, r in df.iterrows():
        print(f"  {str(r['pitcher_name']):<25} "
              f"{r['pitch_type_code']:>4} "
              f"{r['season_bapv_plus']:>7.1f} "
              f"{int(r['total_pitches']):>8,} "
              f"{int(r['games']):>6} "
              f"{r['avg_velo']:>6.1f} "
              f"{r['whiff_rate']:>7.3f} "
              f"{r['csw_rate']:>6.3f}")


def show_pitcher_profile(pitcher_id: int, season: int, db):
    """Show a pitcher's per-pitch-type scores for the season."""
    sql = text("""
        SELECT
            pitch_type_code,
            SUM(pitches_thrown) as total_pitches,
            ROUND(AVG(bapv_plus)::numeric, 1) as season_bapv_plus,
            ROUND(AVG(avg_velo)::numeric, 1) as avg_velo,
            ROUND(AVG(avg_spin)::numeric, 0) as avg_spin,
            ROUND(AVG(whiff_rate)::numeric, 3) as whiff_rate,
            ROUND(AVG(csw_rate)::numeric, 3) as csw_rate,
            ROUND(AVG(hard_hit_rate)::numeric, 3) as hard_hit_rate,
            COUNT(DISTINCT game_pk) as games
        FROM mlb.pitch_quality_scores
        WHERE pitcher_id = :pid
        AND season = :season
        GROUP BY pitch_type_code
        ORDER BY total_pitches DESC
    """)

    df = pd.read_sql(sql, db.bind, params={
        "pid": pitcher_id,
        "season": season
    })

    name_sql = text("""
        SELECT DISTINCT pitcher_name FROM mlb.at_bats
        WHERE pitcher_id = :pid LIMIT 1
    """)
    name = db.execute(name_sql, {"pid": pitcher_id}).scalar()

    print(f"\n  {name or pitcher_id} — {season} Season")
    print(f"\n  {'Type':>4} {'BAPV+':>7} {'Pitches':>8} "
          f"{'Games':>6} {'Velo':>6} {'Spin':>6} "
          f"{'Whiff%':>7} {'CSW%':>6} {'HardHit%':>9}")
    print(f"  {'-'*4} {'-'*7} {'-'*8} {'-'*6} "
          f"{'-'*6} {'-'*6} {'-'*7} {'-'*6} {'-'*9}")

    for _, r in df.iterrows():
        print(f"  {r['pitch_type_code']:>4} "
              f"{r['season_bapv_plus']:>7.1f} "
              f"{int(r['total_pitches']):>8,} "
              f"{int(r['games']):>6} "
              f"{r['avg_velo']:>6.1f} "
              f"{r['avg_spin']:>6.0f} "
              f"{r['whiff_rate']:>7.3f} "
              f"{r['csw_rate']:>6.3f} "
              f"{r['hard_hit_rate']:>9.3f}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Compute and store BAPV scores")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--game-pk", type=int, default=None)
    parser.add_argument("--leaderboard", action="store_true")
    parser.add_argument("--pitcher", type=int, default=None)
    parser.add_argument("--min-pitches", type=int, default=200)
    parser.add_argument("--no-store", action="store_true",
                        help="Compute but don't store results")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        # Display mode — read from stored scores
        if args.leaderboard:
            print(f"\n=== {args.season} BAPV+ Leaderboard ===")
            show_leaderboard(args.season, db, args.min_pitches)
            return

        if args.pitcher and not args.game_pk:
            show_pitcher_profile(args.pitcher, args.season, db)
            return

        # Compute mode
        print(f"\n=== Computing BAPV — {args.season} ===")

        # Use prior season tendencies for early current season
        #tendency_season = args.season
        # For current season before tendencies are built, use prior season
        has_tendencies = db.execute(sqlt(
            "SELECT COUNT(*) FROM mlb.batter_pitch_type_tendencies WHERE season = :s"
        ), {"s": args.season}).scalar()
        tendency_season = args.season if has_tendencies > 100 else args.season - 1
        print(f"  Using {tendency_season} batter tendencies")
        print(f"  Loading tendencies for {tendency_season}...")
        type_lookup, zone_lookup, weights, league_avg = load_tendencies(
            tendency_season, db
        )
        print(f"  League avg whiff rate: {league_avg['whiff_rate']:.3f}")
        print(f"  League avg take rate:  {league_avg['take_rate']:.3f}")

        print(f"  Loading pitches...")
        pitches = load_pitches(
            args.season, db,
            start_date=args.start,
            end_date=args.end,
            game_pk=args.game_pk
        )
        print(f"  {len(pitches):,} pitches loaded")

        if len(pitches) == 0:
            print("  No pitches found for this scope")
            return

        print(f"  Computing BAPV (vectorized)...")
        pitches = compute_bapv_vectorized(
            pitches, type_lookup, zone_lookup, weights, league_avg
        )

        league_avg_bapv = float(pitches['bapv'].mean())
        print(f"  League avg BAPV/pitch: {league_avg_bapv:.4f}")

        print(f"  Aggregating per game...")
        agg = aggregate_per_game(pitches)
        agg = normalize_bapv_plus(agg, league_avg_bapv)

        print(f"  {len(agg):,} pitcher-game-pitch type rows")

        if not args.no_store:
            n = store_scores(agg, db,
                            game_pk=args.game_pk,
                            start_date=args.start,
                            end_date=args.end)
            print(f"  Stored {n:,} rows in pitch_quality_scores")

        # Show summary
        season_summary = agg.groupby('pitch_type_code').agg(
            rows=('avg_bapv', 'count'),
            avg_bapv_plus=('bapv_plus', 'mean'),
        ).round(1)
        print(f"\n  Summary by pitch type:")
        print(season_summary.to_string())

    finally:
        db.close()


if __name__ == "__main__":
    main()