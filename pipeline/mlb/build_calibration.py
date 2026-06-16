"""
Derive dynamic K and HR calibration factors from rolling projection error.

Reads last LOOKBACK_DAYS of mlb.model_scores plus per-outing actuals,
diagnoses root causes of misses, and writes corrected multipliers to
mlb.projection_calibration for use by build_daily_projections.py.

Run nightly after build_model_scores.py (~11:22pm PT).

K calibration:
  The simulator produces raw K% ~27%; K_PCT_CALIBRATION (currently 0.830)
  scales it to match the league ~22.4%.  If the rolling 14-day bias shows
  we are still over/under projecting, this script nudges the calibration
  factor toward the observed ratio (30% learning rate, clamped to [0.60, 1.00]).

HR calibration:
  hr_rate_multiplier adjusts proj_hr_pct after simulation.  It starts at 1.0
  and drifts toward observed actual HR% / projected HR% (same learning rate,
  clamped to [0.70, 1.40]).

K miss diagnosis:
  Total K miss is decomposed into:
    • IP contribution  — pitchers going fewer/more innings than the 5.0 baseline
    • K rate contribution — actual K/BF differing from proj_k_pct even at actual IP

Usage:
    python build_calibration.py [--lookback N] [--dry-run]
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import argparse
from datetime import date, timedelta
from database import SessionLocal
from sqlalchemy import text

LOOKBACK_DAYS     = 14
LEARNING_RATE     = 0.30   # fraction of observed correction absorbed per run
MIN_DAYS_DATA     = 3      # require at least this many scored days before adjusting
BIAS_SKIP_K       = 0.10   # skip K adjustment when |bias_pct| < this (noise guard)
BIAS_SKIP_HR      = 0.03   # skip HR adjustment when |bias_ratio - 1| < this

K_CALIB_BASE      = 0.830
K_CALIB_MIN       = 0.60
K_CALIB_MAX       = 1.00
HR_MULT_BASE      = 1.00
HR_MULT_MIN       = 0.70
HR_MULT_MAX       = 1.40

BF_PER_IP         = 3.3    # MLB average batters faced per inning
IP_BASELINE       = 5.0    # baseline projected IP used when pitcher history is thin

DDL = """
CREATE TABLE IF NOT EXISTS mlb.projection_calibration (
    computed_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    lookback_days       INTEGER      NOT NULL,
    n_k_days            INTEGER,
    k_mean_proj         NUMERIC(8,3),
    k_mean_actual       NUMERIC(8,3),
    k_bias_pct          NUMERIC(8,3),
    k_pct_calibration   NUMERIC(8,4),
    n_hr_days           INTEGER,
    hr_mean_proj        NUMERIC(8,4),
    hr_mean_actual      NUMERIC(8,4),
    hr_rate_multiplier  NUMERIC(8,4),
    n_ip_days           INTEGER,
    ip_mean_actual      NUMERIC(8,3),
    ip_bias             NUMERIC(8,3),
    notes               TEXT,
    PRIMARY KEY (computed_at)
);
CREATE INDEX IF NOT EXISTS ix_proj_calibration_time
    ON mlb.projection_calibration (computed_at DESC);
"""


# ── CURRENT FACTOR READERS ────────────────────────────────────────────────────

def get_current_k_calibration(db) -> float:
    val = db.execute(text("""
        SELECT k_pct_calibration FROM mlb.projection_calibration
        ORDER BY computed_at DESC LIMIT 1
    """)).scalar()
    return float(val) if val else K_CALIB_BASE


def get_current_hr_multiplier(db) -> float:
    val = db.execute(text("""
        SELECT hr_rate_multiplier FROM mlb.projection_calibration
        ORDER BY computed_at DESC LIMIT 1
    """)).scalar()
    return float(val) if val else HR_MULT_BASE


# ── CALIBRATION COMPUTATION ───────────────────────────────────────────────────

def compute_k_calibration(lookback: int, db) -> dict:
    """
    Adjust K_PCT_CALIBRATION based on rolling k_count model error.
    Uses mean_proj / mean_actual from model_scores (k_count metric stores both).
    """
    row = db.execute(text("""
        SELECT
            COUNT(*)                                     AS n_days,
            ROUND(AVG(mean_proj)::numeric, 3)            AS avg_proj,
            ROUND(AVG(mean_actual)::numeric, 3)          AS avg_actual,
            ROUND(AVG(bias)::numeric, 3)                 AS avg_bias
        FROM mlb.model_scores
        WHERE metric      = 'k_count'
          AND player_type = 'pitcher'
          AND mean_proj   IS NOT NULL
          AND mean_actual IS NOT NULL
          AND n_players   >= 5
          AND score_date  >= CURRENT_DATE - :days * INTERVAL '1 day'
    """), {"days": lookback}).mappings().first()

    current = get_current_k_calibration(db)

    n_days = int(row['n_days']) if row and row['n_days'] else 0
    if n_days < MIN_DAYS_DATA:
        print(f"  K calibration: only {n_days} scored days (need {MIN_DAYS_DATA}), "
              f"keeping {current:.4f}")
        return {
            "n_days": n_days, "k_mean_proj": None, "k_mean_actual": None,
            "k_bias_pct": None, "k_pct_calibration": current,
        }

    avg_proj   = float(row['avg_proj'])
    avg_actual = float(row['avg_actual'])

    if avg_proj <= 0:
        return {"n_days": n_days, "k_mean_proj": None, "k_mean_actual": None,
                "k_bias_pct": None, "k_pct_calibration": current}

    bias_pct = (avg_proj - avg_actual) / avg_proj * 100  # positive = over-projecting

    if abs(bias_pct) < BIAS_SKIP_K * 100:
        print(f"  K calibration [{n_days}d]: bias {bias_pct:+.2f}% within noise threshold, "
              f"keeping {current:.4f}")
        return {
            "n_days": n_days, "k_mean_proj": round(avg_proj, 3),
            "k_mean_actual": round(avg_actual, 3),
            "k_bias_pct": round(bias_pct, 2), "k_pct_calibration": current,
        }

    observed_ratio = avg_actual / avg_proj
    new_calib = current * (1.0 + LEARNING_RATE * (observed_ratio - 1.0))
    new_calib = max(K_CALIB_MIN, min(K_CALIB_MAX, new_calib))

    arrow = "↓" if bias_pct > 0 else "↑"
    verdict = "over-projecting" if bias_pct > 0 else "under-projecting"
    print(f"  K calibration [{n_days}d avg]: proj={avg_proj:.2f}K actual={avg_actual:.2f}K "
          f"bias={bias_pct:+.1f}% ({verdict})")
    print(f"    {arrow} Adjusting: {current:.4f} → {new_calib:.4f} "
          f"(ratio={observed_ratio:.3f}, lr={LEARNING_RATE})")

    return {
        "n_days": n_days,
        "k_mean_proj": round(avg_proj, 3),
        "k_mean_actual": round(avg_actual, 3),
        "k_bias_pct": round(bias_pct, 2),
        "k_pct_calibration": round(new_calib, 4),
    }


def compute_hr_multiplier(lookback: int, db) -> dict:
    """
    Adjust HR rate multiplier from rolling hr_brier model error.
    hr_brier metric stores mean_proj (proj HR%) and mean_actual (actual HR rate).
    """
    row = db.execute(text("""
        SELECT
            COUNT(*)                                     AS n_days,
            ROUND(AVG(mean_proj)::numeric, 4)            AS avg_proj,
            ROUND(AVG(mean_actual)::numeric, 4)          AS avg_actual
        FROM mlb.model_scores
        WHERE metric      = 'hr_brier'
          AND player_type = 'batter'
          AND mean_proj   IS NOT NULL
          AND mean_actual IS NOT NULL
          AND n_players   >= 10
          AND score_date  >= CURRENT_DATE - :days * INTERVAL '1 day'
    """), {"days": lookback}).mappings().first()

    current = get_current_hr_multiplier(db)

    n_days = int(row['n_days']) if row and row['n_days'] else 0
    if n_days < MIN_DAYS_DATA:
        print(f"  HR multiplier: only {n_days} scored days, keeping {current:.4f}")
        return {"n_days": n_days, "hr_mean_proj": None, "hr_mean_actual": None,
                "hr_rate_multiplier": current}

    avg_proj   = float(row['avg_proj'])
    avg_actual = float(row['avg_actual'])

    if avg_proj <= 0:
        return {"n_days": n_days, "hr_mean_proj": None, "hr_mean_actual": None,
                "hr_rate_multiplier": current}

    observed_ratio = avg_actual / avg_proj

    if abs(observed_ratio - 1.0) < BIAS_SKIP_HR:
        print(f"  HR multiplier [{n_days}d]: ratio {observed_ratio:.3f} within noise threshold, "
              f"keeping {current:.4f}")
        return {
            "n_days": n_days, "hr_mean_proj": round(avg_proj, 4),
            "hr_mean_actual": round(avg_actual, 4), "hr_rate_multiplier": current,
        }

    new_mult = current * (1.0 + LEARNING_RATE * (observed_ratio - 1.0))
    new_mult = max(HR_MULT_MIN, min(HR_MULT_MAX, new_mult))

    bias_pct = (avg_proj - avg_actual) / avg_proj * 100
    arrow = "↓" if bias_pct > 0 else "↑"
    verdict = "over-projecting" if bias_pct > 0 else "under-projecting"
    print(f"  HR multiplier [{n_days}d avg]: proj={avg_proj:.4f} actual={avg_actual:.4f} "
          f"bias={bias_pct:+.1f}% ({verdict})")
    print(f"    {arrow} Adjusting: {current:.4f} → {new_mult:.4f}")

    return {
        "n_days": n_days,
        "hr_mean_proj": round(avg_proj, 4),
        "hr_mean_actual": round(avg_actual, 4),
        "hr_rate_multiplier": round(new_mult, 4),
    }


def get_ip_context(lookback: int, db) -> dict:
    """Avg actual IP from ip_dist model scores for context."""
    row = db.execute(text("""
        SELECT
            COUNT(*)                                     AS n_days,
            ROUND(AVG(mean_actual)::numeric, 2)          AS avg_ip
        FROM mlb.model_scores
        WHERE metric      = 'ip_dist'
          AND player_type = 'pitcher'
          AND mean_actual IS NOT NULL
          AND n_players   >= 5
          AND score_date  >= CURRENT_DATE - :days * INTERVAL '1 day'
    """), {"days": lookback}).mappings().first()

    if not row or not row['n_days']:
        return {"n_ip_days": 0, "ip_mean_actual": None, "ip_bias": None}

    avg_ip   = float(row['avg_ip'])
    ip_bias  = avg_ip - IP_BASELINE

    print(f"  IP context [{int(row['n_days'])}d avg]: {avg_ip:.2f} inn "
          f"({'above' if ip_bias > 0 else 'below'} {IP_BASELINE:.1f} baseline: {ip_bias:+.2f})")

    return {
        "n_ip_days": int(row['n_days']),
        "ip_mean_actual": round(avg_ip, 3),
        "ip_bias": round(ip_bias, 3),
    }


# ── DIAGNOSIS ─────────────────────────────────────────────────────────────────

def diagnose_k_miss(lookback: int, db):
    """
    Decompose K miss into IP contribution and K rate contribution.

    Total miss = actual_k - proj_k_at_baseline_ip
    IP contribution  = (actual_ip - baseline_ip) * proj_k_pct/100 * BF_PER_IP
    Rate contribution = actual_ip * BF_PER_IP * (actual_k_rate - proj_k_pct/100)
    """
    rows = db.execute(text("""
        SELECT
            COUNT(*)                                                            AS n,
            ROUND(AVG(da.actual_ip)::numeric, 2)                               AS avg_ip,
            ROUND(AVG(da.proj_k_pct)::numeric, 2)                              AS avg_proj_k_pct,
            ROUND(AVG(
                da.actual_k::float / NULLIF(da.actual_ip * :bf_per_ip, 0) * 100
            )::numeric, 2)                                                     AS avg_actual_k_rate,
            -- IP contribution: extra/fewer Ks just from going longer/shorter
            ROUND(AVG(
                (da.actual_ip - :ip_base) * da.proj_k_pct / 100.0 * :bf_per_ip
            )::numeric, 3)                                                     AS avg_ip_contrib,
            -- Rate contribution: K rate diff at actual IP
            ROUND(AVG(
                da.actual_ip * :bf_per_ip *
                (da.actual_k::float / NULLIF(da.actual_ip * :bf_per_ip, 0)
                 - da.proj_k_pct / 100.0)
            )::numeric, 3)                                                     AS avg_rate_contrib,
            -- Total miss vs proj at baseline IP
            ROUND(AVG(
                da.actual_k - da.proj_k_pct / 100.0 * :ip_base * :bf_per_ip
            )::numeric, 3)                                                     AS avg_total_miss
        FROM mlb.daily_actuals da
        WHERE da.game_date  >= CURRENT_DATE - :days * INTERVAL '1 day'
          AND da.player_type = 'pitcher'
          AND da.actual_ip  >= 2.0
          AND da.proj_k_pct IS NOT NULL
    """), {"days": lookback, "bf_per_ip": BF_PER_IP, "ip_base": IP_BASELINE}).mappings().first()

    if not rows or not rows['n']:
        return

    n             = int(rows['n'])
    avg_ip        = float(rows['avg_ip'] or 0)
    proj_k_pct    = float(rows['avg_proj_k_pct'] or 0)
    actual_k_rate = float(rows['avg_actual_k_rate'] or 0)
    ip_contrib    = float(rows['avg_ip_contrib'] or 0)
    rate_contrib  = float(rows['avg_rate_contrib'] or 0)
    total_miss    = float(rows['avg_total_miss'] or 0)

    print(f"\n  K Miss Diagnosis [{n} pitcher-games over {lookback}d]:")
    print(f"    Proj K%:              {proj_k_pct:.1f}%")
    print(f"    Actual K rate/BF:     {actual_k_rate:.1f}%  "
          f"(rate gap: {actual_k_rate - proj_k_pct:+.1f} pct pts)")
    print(f"    Avg actual IP:        {avg_ip:.2f} inn  "
          f"({avg_ip - IP_BASELINE:+.2f} vs {IP_BASELINE:.1f} baseline)")
    print(f"    Miss breakdown (Ks per start):")
    print(f"      IP contribution:    {ip_contrib:+.2f} Ks  "
          f"({'pitchers went longer' if ip_contrib > 0 else 'pitchers went shorter'})")
    print(f"      Rate contribution:  {rate_contrib:+.2f} Ks  "
          f"({'K rate above proj' if rate_contrib > 0 else 'K rate below proj'})")
    print(f"      Total miss:         {total_miss:+.2f} Ks vs {IP_BASELINE:.0f}-inn baseline")

    if total_miss == 0:
        driver = "calibrated"
    elif abs(ip_contrib) > abs(rate_contrib) and abs(ip_contrib) > 0.2:
        driver = "IP length (pitchers go shorter/longer than projected)"
    elif abs(rate_contrib) > abs(ip_contrib) and abs(rate_contrib) > 0.2:
        driver = "K rate model error (swing/whiff calibration)"
    else:
        driver = "mixed — both IP and K rate contribute roughly equally"

    print(f"    Primary driver:       {driver}")


def diagnose_top_k_misses(lookback: int, db, top_n: int = 5):
    """Print the pitchers with the largest K projection errors recently."""
    rows = db.execute(text("""
        SELECT
            da.player_name,
            da.game_date,
            da.actual_k,
            ROUND((da.proj_k_pct / 100.0 * :ip_base * :bf_per_ip)::numeric, 1) AS proj_k,
            da.actual_ip,
            ROUND((da.actual_k - da.proj_k_pct / 100.0 * :ip_base * :bf_per_ip)::numeric, 1) AS miss
        FROM mlb.daily_actuals da
        WHERE da.game_date  >= CURRENT_DATE - :days * INTERVAL '1 day'
          AND da.player_type = 'pitcher'
          AND da.actual_ip  >= 2.0
          AND da.proj_k_pct IS NOT NULL
        ORDER BY ABS(da.actual_k - da.proj_k_pct / 100.0 * :ip_base * :bf_per_ip) DESC
        LIMIT :top_n
    """), {"days": lookback, "ip_base": IP_BASELINE, "bf_per_ip": BF_PER_IP,
           "top_n": top_n}).mappings().all()

    if not rows:
        return

    print(f"\n  Top {top_n} K misses (last {lookback}d):")
    print(f"    {'Pitcher':<22} {'Date':<12} {'Proj':>5} {'Actual':>7} {'IP':>5} {'Miss':>6}")
    print(f"    {'-'*22} {'-'*12} {'-'*5} {'-'*7} {'-'*5} {'-'*6}")
    for r in rows:
        print(f"    {str(r['player_name']):<22} {str(r['game_date']):<12} "
              f"{float(r['proj_k']):>5.1f} {int(r['actual_k']):>7} "
              f"{float(r['actual_ip']):>5.1f} {float(r['miss']):>+6.1f}")


def diagnose_top_hr_misses(lookback: int, db, top_n: int = 5):
    """Print games with the most HR vs projection to identify structural HR bias."""
    rows = db.execute(text("""
        SELECT
            da.player_name,
            da.game_date,
            da.actual_hr,
            da.actual_ab,
            ROUND(da.proj_hr_pct::numeric, 1) AS proj_hr_pct,
            ROUND((da.actual_hr::float / NULLIF(da.actual_ab, 0) * 100)::numeric, 1)
                AS actual_hr_pct
        FROM mlb.daily_actuals da
        WHERE da.game_date  >= CURRENT_DATE - :days * INTERVAL '1 day'
          AND da.player_type = 'batter'
          AND da.actual_ab   >= 2
          AND da.proj_hr_pct IS NOT NULL
          AND da.actual_hr   >= 1
        ORDER BY da.actual_hr DESC
        LIMIT :top_n
    """), {"days": lookback, "top_n": top_n}).mappings().all()

    if not rows:
        return

    print(f"\n  Top {top_n} HR events (last {lookback}d):")
    print(f"    {'Batter':<22} {'Date':<12} {'ProjHR%':>8} {'ActHR%':>7} {'HR':>4}")
    print(f"    {'-'*22} {'-'*12} {'-'*8} {'-'*7} {'-'*4}")
    for r in rows:
        proj = float(r['proj_hr_pct']) if r['proj_hr_pct'] else 0
        actual = float(r['actual_hr_pct']) if r['actual_hr_pct'] else 0
        print(f"    {str(r['player_name']):<22} {str(r['game_date']):<12} "
              f"{proj:>8.1f} {actual:>7.1f} {int(r['actual_hr']):>4}")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def build_calibration(lookback: int, dry_run: bool, db):
    print(f"\n=== Projection Calibration — {date.today()} (lookback={lookback}d) ===")

    db.execute(text(DDL))
    db.commit()

    print("\n[K Calibration]")
    k_result = compute_k_calibration(lookback, db)

    print("\n[HR Calibration]")
    hr_result = compute_hr_multiplier(lookback, db)

    print("\n[IP Context]")
    ip_result = get_ip_context(lookback, db)

    print("\n[Diagnosis]")
    diagnose_k_miss(lookback, db)
    diagnose_top_k_misses(lookback, db)
    diagnose_top_hr_misses(lookback, db)

    notes_parts = []
    if k_result.get('k_bias_pct') is not None:
        notes_parts.append(f"K bias={k_result['k_bias_pct']:+.1f}%")
    if hr_result.get('hr_mean_proj') and hr_result.get('hr_mean_actual'):
        hr_bias = (hr_result['hr_mean_proj'] - hr_result['hr_mean_actual']) / hr_result['hr_mean_proj'] * 100
        notes_parts.append(f"HR bias={hr_bias:+.1f}%")
    if ip_result.get('ip_bias') is not None:
        notes_parts.append(f"IP bias={ip_result['ip_bias']:+.2f}inn")
    notes = "; ".join(notes_parts) if notes_parts else "no bias data"

    if dry_run:
        print(f"\n  [DRY RUN] Would write: k_calib={k_result['k_pct_calibration']:.4f} "
              f"hr_mult={hr_result['hr_rate_multiplier']:.4f}")
        print(f"  Notes: {notes}")
        return

    db.execute(text("""
        INSERT INTO mlb.projection_calibration (
            computed_at, lookback_days,
            n_k_days, k_mean_proj, k_mean_actual, k_bias_pct, k_pct_calibration,
            n_hr_days, hr_mean_proj, hr_mean_actual, hr_rate_multiplier,
            n_ip_days, ip_mean_actual, ip_bias,
            notes
        ) VALUES (
            NOW(), :lookback,
            :n_k, :k_proj, :k_actual, :k_bias, :k_calib,
            :n_hr, :hr_proj, :hr_actual, :hr_mult,
            :n_ip, :ip_actual, :ip_bias,
            :notes
        )
    """), {
        "lookback":  lookback,
        "n_k":       k_result.get("n_days"),
        "k_proj":    k_result.get("k_mean_proj"),
        "k_actual":  k_result.get("k_mean_actual"),
        "k_bias":    k_result.get("k_bias_pct"),
        "k_calib":   k_result["k_pct_calibration"],
        "n_hr":      hr_result.get("n_days"),
        "hr_proj":   hr_result.get("hr_mean_proj"),
        "hr_actual": hr_result.get("hr_mean_actual"),
        "hr_mult":   hr_result["hr_rate_multiplier"],
        "n_ip":      ip_result.get("n_ip_days"),
        "ip_actual": ip_result.get("ip_mean_actual"),
        "ip_bias":   ip_result.get("ip_bias"),
        "notes":     notes,
    })
    db.commit()

    print(f"\n=== Stored: k_calib={k_result['k_pct_calibration']:.4f}  "
          f"hr_mult={hr_result['hr_rate_multiplier']:.4f} ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lookback", type=int, default=LOOKBACK_DAYS,
                        help=f"Days of model_scores to average (default {LOOKBACK_DAYS})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print diagnosis only, do not write to DB")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        build_calibration(args.lookback, args.dry_run, db)
    finally:
        db.close()
