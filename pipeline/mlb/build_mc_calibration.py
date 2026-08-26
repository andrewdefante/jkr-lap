"""
Nightly MC K projection calibration.

Runs alongside build_calibration.py (which still tunes K_PCT_CALIBRATION and
hr_rate_multiplier for the simulation engine — those outputs, proj_k_pct /
proj_hr_pct / proj_ks_6inn, are still used elsewhere: model_scores' k_rate,
hr_brier, er_proxy, tb metrics, and the batter side of the pipeline).

This script separately calibrates the MC regression model's K floor:

1. Computes rolling residuals from the last LOOKBACK_DAYS of MC projections
   vs actuals (mlb.daily_actuals.k_avg_mc / k_floor_mc vs actual_k).
2. Tracks floor hit rate (actual_k >= k_floor_mc) — target 90%.
3. Adjusts MC_FLOOR_ADJUSTMENT if floor hit rate drifts from target.
4. Appends a new row to mlb.projection_calibration (same append-only table
   build_calibration.py writes to — PK is computed_at, not a per-day key).
   k_pct_calibration and hr_rate_multiplier are carried forward unchanged
   from the latest existing row so this script never clobbers the
   simulation engine's calibration values.

Run nightly after build_calibration.py (~11:23pm PT), so it can carry
forward that run's freshly-computed hr_rate_multiplier/k_pct_calibration.
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import argparse
from datetime import date, timedelta
from database import SessionLocal
from sqlalchemy import text

LOOKBACK_DAYS = 14
TARGET_FLOOR_HIT_RATE = 0.90
LEARNING_RATE = 0.20
MIN_DAYS_DATA = 5
DEFAULT_FLOOR_ADJ = 0.95
MIN_FLOOR_ADJ = 0.80
MAX_FLOOR_ADJ = 1.00


def run_mc_calibration(lookback_days: int = LOOKBACK_DAYS, dry_run: bool = False):
    db = SessionLocal()
    try:
        cutoff = date.today() - timedelta(days=lookback_days)

        rows = db.execute(text("""
            SELECT
                da.game_date,
                da.player_id,
                da.actual_k,
                da.k_avg_mc,
                da.k_floor_mc,
                da.proj_ks_locked
            FROM mlb.daily_actuals da
            WHERE da.player_type = 'pitcher'
            AND da.game_date >= :cutoff
            AND da.actual_k IS NOT NULL
            AND da.k_avg_mc IS NOT NULL
            AND da.k_floor_mc IS NOT NULL
        """), {"cutoff": cutoff}).mappings().all()

        if len(rows) < MIN_DAYS_DATA:
            print(f"MC calibration: insufficient data ({len(rows)} rows, need {MIN_DAYS_DATA})")
            return

        import numpy as np
        actuals = np.array([r['actual_k'] for r in rows])
        mc_avg = np.array([r['k_avg_mc'] for r in rows])
        mc_floor = np.array([r['k_floor_mc'] for r in rows])

        mae = float(np.mean(np.abs(mc_avg - actuals)))
        bias = float(np.mean(mc_avg - actuals))  # positive = over-projecting
        floor_hit_rate = float(np.mean(actuals >= mc_floor))
        resid_std = float(np.std(actuals - mc_avg))

        print(f"MC Calibration ({len(rows)} outings, last {lookback_days} days):")
        print(f"  MAE:            {mae:.3f} Ks")
        print(f"  Bias:           {bias:+.3f} Ks ({'over' if bias > 0 else 'under'}-projecting)")
        print(f"  Floor hit rate: {floor_hit_rate:.1%} (target {TARGET_FLOOR_HIT_RATE:.0%})")
        print(f"  Resid std:      {resid_std:.4f}")

        # Latest row — used both to read the current floor adjustment and to
        # carry forward the simulation engine's calibration values unchanged.
        current = db.execute(text("""
            SELECT k_pct_calibration, hr_rate_multiplier, mc_floor_adjustment
            FROM mlb.projection_calibration
            ORDER BY computed_at DESC LIMIT 1
        """)).mappings().first()

        current_floor_adj = (
            float(current['mc_floor_adjustment'])
            if current and current['mc_floor_adjustment'] is not None
            else DEFAULT_FLOOR_ADJ
        )
        carry_k_pct_calibration = float(current['k_pct_calibration']) if current and current['k_pct_calibration'] is not None else None
        carry_hr_rate_multiplier = float(current['hr_rate_multiplier']) if current and current['hr_rate_multiplier'] is not None else None

        # Adjust floor if hit rate drifts from target.
        # If hitting too rarely (< target) -> lower floor adjustment (more conservative)
        # If hitting too often (> target) -> raise floor adjustment (more aggressive)
        floor_error = floor_hit_rate - TARGET_FLOOR_HIT_RATE
        new_floor_adj = current_floor_adj - (LEARNING_RATE * floor_error)
        new_floor_adj = max(MIN_FLOOR_ADJ, min(MAX_FLOOR_ADJ, new_floor_adj))

        print(f"  Floor adj:      {current_floor_adj:.3f} → {new_floor_adj:.3f}")

        if dry_run:
            print("  [DRY RUN — no changes written]")
            return

        # lookback_days is NOT NULL on this table with no default; carry a
        # value forward through this row too so the append-only insert works
        # regardless of whether build_calibration.py has run yet today.
        db.execute(text("""
            INSERT INTO mlb.projection_calibration (
                computed_at,
                lookback_days,
                k_pct_calibration,
                hr_rate_multiplier,
                mc_floor_adjustment,
                mc_resid_std,
                mc_mae,
                mc_bias,
                mc_floor_hit_rate,
                mc_n_outings
            ) VALUES (
                NOW(),
                :lookback_days,
                :k_pct,
                :hr_mult,
                :floor_adj,
                :resid_std,
                :mae,
                :bias,
                :floor_hit_rate,
                :n_outings
            )
        """), {
            "lookback_days": lookback_days,
            "k_pct": carry_k_pct_calibration,
            "hr_mult": carry_hr_rate_multiplier,
            "floor_adj": round(new_floor_adj, 4),
            "resid_std": round(resid_std, 4),
            "mae": round(mae, 4),
            "bias": round(bias, 4),
            "floor_hit_rate": round(floor_hit_rate, 4),
            "n_outings": len(rows),
        })
        db.commit()
        print(f"  Written to projection_calibration for {date.today()}")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lookback", type=int, default=LOOKBACK_DAYS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run_mc_calibration(args.lookback, args.dry_run)
