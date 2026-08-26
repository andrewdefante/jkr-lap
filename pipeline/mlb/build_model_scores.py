"""
Compute model accuracy scores — projections vs actuals.

Metrics stored per date:
  pitcher / k_count  — actual_k vs proj_ks_locked (pre-game K projection; falls back to proj_ks_6inn, then proj_k_pct/100*21)
  pitcher / k_rate   — actual K% vs proj_k_pct (rate calibration)
  pitcher / er_proxy — actual_er vs ops-derived ER estimate
  pitcher / ip_dist  — actual IP distribution (no projection, baseline tracking)
  batter  / tb       — actual_tb vs proj_tb (proj_slg*4)
  batter  / k_rate   — actual K/AB vs proj_k_pct (rate calibration)
  batter  / hr_brier — HR Brier score (binary HR vs proj_hr_pct probability)

Usage:
    python build_model_scores.py [--date 2025-05-20] [--days N] [--backfill]
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import argparse
from datetime import date, timedelta
from database import SessionLocal
from sqlalchemy import text


DDL = """
CREATE TABLE IF NOT EXISTS mlb.model_scores (
    score_date   DATE         NOT NULL,
    player_type  VARCHAR(10)  NOT NULL,
    metric       VARCHAR(30)  NOT NULL,
    n_players    INTEGER,
    mae          NUMERIC(8,4),
    rmse         NUMERIC(8,4),
    mean_proj    NUMERIC(8,4),
    mean_actual  NUMERIC(8,4),
    bias         NUMERIC(8,4),
    within_1     NUMERIC(5,1),
    within_2     NUMERIC(5,1),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (score_date, player_type, metric)
);
CREATE INDEX IF NOT EXISTS ix_model_scores_date
    ON mlb.model_scores (score_date DESC);
"""


def build_pitcher_scores(score_date: date, db) -> list:
    """Score pitcher K count, K rate, ER proxy, and IP tracking for one date."""
    row = db.execute(text("""
        WITH pd AS (
            SELECT
                actual_k,
                actual_ip,
                actual_er,
                proj_k_pct,
                k_avg_mc,
                -- Use locked pre-game K projection when available, else stored 6inn value,
                -- else fall back to deriving from K% (legacy rows before 2026-06-02)
                ROUND(COALESCE(
                    proj_ks_locked,
                    proj_ks_6inn,
                    proj_k_pct / 100.0 * 21
                )::numeric, 2)                                             AS proj_k_count,
                -- per-outing ER proxy via OPS → ERA conversion (ERA ≈ OPS * 4.5)
                ROUND((proj_ops * 4.5 * actual_ip / 9.0)::numeric, 2)     AS proj_er,
                -- actual K rate: K per estimated BF (3.3 BF/IP)
                ROUND((actual_k / NULLIF(actual_ip * 3.3, 0) * 100)::numeric, 2) AS actual_k_rate
            FROM mlb.daily_actuals
            WHERE game_date    = :d
              AND player_type  = 'pitcher'
              AND actual_k     IS NOT NULL
              AND proj_k_pct   IS NOT NULL
              AND actual_ip    >= 2.0
        )
        SELECT
            COUNT(*)                                                           AS n,
            -- K COUNT
            ROUND(AVG(ABS(actual_k - proj_k_count))::numeric, 3)             AS k_count_mae,
            ROUND(SQRT(AVG(POWER(actual_k - proj_k_count, 2)))::numeric, 3)  AS k_count_rmse,
            ROUND(AVG(proj_k_count)::numeric, 2)                             AS k_count_proj,
            ROUND(AVG(actual_k)::numeric, 2)                                 AS k_count_actual,
            ROUND(AVG(proj_k_count - actual_k)::numeric, 3)                  AS k_count_bias,
            ROUND(SUM(CASE WHEN ABS(actual_k - proj_k_count) <= 1
                           THEN 1 ELSE 0 END)::numeric
                  / NULLIF(COUNT(*),0) * 100, 1)                             AS k_count_w1,
            ROUND(SUM(CASE WHEN ABS(actual_k - proj_k_count) <= 2
                           THEN 1 ELSE 0 END)::numeric
                  / NULLIF(COUNT(*),0) * 100, 1)                             AS k_count_w2,
            -- K RATE
            ROUND(AVG(ABS(actual_k_rate - proj_k_pct))::numeric, 3)         AS k_rate_mae,
            ROUND(AVG(proj_k_pct - actual_k_rate)::numeric, 3)              AS k_rate_bias,
            -- MC K COUNT (actual_k vs the MC regression model's k_avg_mc)
            COUNT(CASE WHEN k_avg_mc IS NOT NULL THEN 1 END)                 AS n_mc,
            ROUND(AVG(ABS(actual_k - k_avg_mc))
                  FILTER (WHERE k_avg_mc IS NOT NULL)::numeric, 3)           AS mc_k_count_mae,
            ROUND(SQRT(AVG(POWER(actual_k - k_avg_mc, 2))
                  FILTER (WHERE k_avg_mc IS NOT NULL))::numeric, 3)          AS mc_k_count_rmse,
            ROUND(AVG(k_avg_mc)
                  FILTER (WHERE k_avg_mc IS NOT NULL)::numeric, 2)           AS mc_k_count_proj,
            ROUND(AVG(actual_k)
                  FILTER (WHERE k_avg_mc IS NOT NULL)::numeric, 2)           AS mc_k_count_actual,
            ROUND(AVG(k_avg_mc - actual_k)
                  FILTER (WHERE k_avg_mc IS NOT NULL)::numeric, 3)           AS mc_k_count_bias,
            ROUND(SUM(CASE WHEN k_avg_mc IS NOT NULL
                           AND ABS(actual_k - k_avg_mc) <= 1
                           THEN 1 ELSE 0 END)::numeric
                  / NULLIF(COUNT(CASE WHEN k_avg_mc IS NOT NULL THEN 1 END),0) * 100, 1)
                                                                              AS mc_k_count_w1,
            ROUND(SUM(CASE WHEN k_avg_mc IS NOT NULL
                           AND ABS(actual_k - k_avg_mc) <= 2
                           THEN 1 ELSE 0 END)::numeric
                  / NULLIF(COUNT(CASE WHEN k_avg_mc IS NOT NULL THEN 1 END),0) * 100, 1)
                                                                              AS mc_k_count_w2,
            -- ER PROXY
            COUNT(CASE WHEN actual_er IS NOT NULL AND proj_er IS NOT NULL
                       THEN 1 END)                                           AS n_er,
            ROUND(AVG(ABS(actual_er - proj_er))
                  FILTER (WHERE actual_er IS NOT NULL
                             AND proj_er  IS NOT NULL)::numeric, 3)          AS er_mae,
            ROUND(SQRT(AVG(POWER(actual_er - proj_er, 2))
                  FILTER (WHERE actual_er IS NOT NULL
                             AND proj_er  IS NOT NULL))::numeric, 3)         AS er_rmse,
            ROUND(AVG(proj_er)
                  FILTER (WHERE actual_er IS NOT NULL
                             AND proj_er  IS NOT NULL)::numeric, 2)          AS er_proj,
            ROUND(AVG(actual_er)
                  FILTER (WHERE actual_er IS NOT NULL
                             AND proj_er  IS NOT NULL)::numeric, 2)          AS er_actual,
            ROUND(AVG(proj_er - actual_er)
                  FILTER (WHERE actual_er IS NOT NULL
                             AND proj_er  IS NOT NULL)::numeric, 3)          AS er_bias,
            -- IP TRACKING (no projection — just distributions)
            ROUND(AVG(actual_ip)::numeric, 2)                                AS ip_mean,
            ROUND(STDDEV(actual_ip)::numeric, 2)                             AS ip_std
        FROM pd
    """), {"d": score_date}).mappings().first()

    if not row or not row['n']:
        return []

    r = dict(row)
    rows = [
        {
            'score_date': score_date, 'player_type': 'pitcher', 'metric': 'k_count',
            'n_players': r['n'],
            'mae': r['k_count_mae'],  'rmse': r['k_count_rmse'],
            'mean_proj': r['k_count_proj'], 'mean_actual': r['k_count_actual'],
            'bias': r['k_count_bias'],
            'within_1': r['k_count_w1'], 'within_2': r['k_count_w2'],
        },
        {
            'score_date': score_date, 'player_type': 'pitcher', 'metric': 'k_rate',
            'n_players': r['n'],
            'mae': r['k_rate_mae'], 'rmse': None,
            'mean_proj': None, 'mean_actual': None,
            'bias': r['k_rate_bias'],
            'within_1': None, 'within_2': None,
        },
        {
            'score_date': score_date, 'player_type': 'pitcher', 'metric': 'ip_dist',
            'n_players': r['n'],
            'mae': None, 'rmse': r['ip_std'],
            'mean_proj': None, 'mean_actual': r['ip_mean'],
            'bias': None, 'within_1': None, 'within_2': None,
        },
    ]
    if r.get('n_er'):
        rows.append({
            'score_date': score_date, 'player_type': 'pitcher', 'metric': 'er_proxy',
            'n_players': r['n_er'],
            'mae': r['er_mae'], 'rmse': r['er_rmse'],
            'mean_proj': r['er_proj'], 'mean_actual': r['er_actual'],
            'bias': r['er_bias'],
            'within_1': None, 'within_2': None,
        })
    if r.get('n_mc'):
        rows.append({
            'score_date': score_date, 'player_type': 'pitcher', 'metric': 'mc_k_count',
            'n_players': r['n_mc'],
            'mae': r['mc_k_count_mae'], 'rmse': r['mc_k_count_rmse'],
            'mean_proj': r['mc_k_count_proj'], 'mean_actual': r['mc_k_count_actual'],
            'bias': r['mc_k_count_bias'],
            'within_1': r['mc_k_count_w1'], 'within_2': r['mc_k_count_w2'],
        })
    return rows


def build_batter_scores(score_date: date, db) -> list:
    """Score batter TB, K rate, and HR Brier for one date."""
    row = db.execute(text("""
        WITH bd AS (
            SELECT
                da.actual_tb,
                da.actual_hr,
                da.actual_ab,
                da.actual_k_batter,
                da.proj_tb,
                da.proj_hr_pct,
                dbp.proj_k_pct                                          AS proj_k_rate,
                ROUND(da.actual_k_batter::numeric
                      / NULLIF(da.actual_ab, 0) * 100, 2)              AS actual_k_rate,
                (da.actual_hr > 0)::int                                 AS hr_binary
            FROM mlb.daily_actuals da
            LEFT JOIN LATERAL (
                SELECT proj_k_pct
                FROM mlb.daily_batter_projections
                WHERE batter_id    = da.player_id
                  AND snapshot_date = da.game_date
                ORDER BY simulations DESC NULLS LAST
                LIMIT 1
            ) dbp ON true
            WHERE da.game_date   = :d
              AND da.player_type = 'batter'
              AND da.actual_tb   IS NOT NULL
              AND da.actual_ab   >= 2
        )
        SELECT
            -- TB
            COUNT(CASE WHEN proj_tb IS NOT NULL THEN 1 END)            AS n_tb,
            ROUND(AVG(ABS(actual_tb - proj_tb))
                  FILTER (WHERE proj_tb IS NOT NULL)::numeric, 3)      AS tb_mae,
            ROUND(SQRT(AVG(POWER(actual_tb - proj_tb, 2))
                  FILTER (WHERE proj_tb IS NOT NULL))::numeric, 3)     AS tb_rmse,
            ROUND(AVG(proj_tb)
                  FILTER (WHERE proj_tb IS NOT NULL)::numeric, 2)      AS tb_proj,
            ROUND(AVG(actual_tb)
                  FILTER (WHERE proj_tb IS NOT NULL)::numeric, 2)      AS tb_actual,
            ROUND(AVG(proj_tb - actual_tb)
                  FILTER (WHERE proj_tb IS NOT NULL)::numeric, 3)      AS tb_bias,
            ROUND(SUM(CASE WHEN proj_tb IS NOT NULL
                           AND ABS(actual_tb - proj_tb) <= 1
                           THEN 1 ELSE 0 END)::numeric
                  / NULLIF(COUNT(CASE WHEN proj_tb IS NOT NULL
                                      THEN 1 END), 0) * 100, 1)       AS tb_w1,
            -- K RATE
            COUNT(CASE WHEN proj_k_rate IS NOT NULL
                            AND actual_k_rate IS NOT NULL THEN 1 END)  AS n_k,
            ROUND(AVG(ABS(actual_k_rate - proj_k_rate))
                  FILTER (WHERE proj_k_rate IS NOT NULL
                             AND actual_k_rate IS NOT NULL)::numeric, 3) AS k_mae,
            ROUND(AVG(proj_k_rate - actual_k_rate)
                  FILTER (WHERE proj_k_rate IS NOT NULL
                             AND actual_k_rate IS NOT NULL)::numeric, 3) AS k_bias,
            -- HR BRIER (probability score for binary event)
            COUNT(CASE WHEN proj_hr_pct IS NOT NULL THEN 1 END)        AS n_hr,
            -- Convert per-PA HR rate to per-game probability (avg 3.8 PA/game)
            -- P(HR in game) = 1 - (1 - hr_per_pa)^3.8
            ROUND(AVG(POWER(
                    hr_binary::numeric - (1.0 - POWER((1.0 - proj_hr_pct/100.0), 3.8)), 2.0))
                  FILTER (WHERE proj_hr_pct IS NOT NULL)::numeric, 4)  AS hr_brier,
            ROUND(AVG((1.0 - POWER((1.0 - proj_hr_pct/100.0), 3.8)) * 100.0)
                  FILTER (WHERE proj_hr_pct IS NOT NULL)::numeric, 2)  AS hr_proj_rate,
            ROUND(AVG(hr_binary)
                  FILTER (WHERE proj_hr_pct IS NOT NULL)::numeric, 4)  AS hr_actual_rate
        FROM bd
    """), {"d": score_date}).mappings().first()

    if not row:
        return []

    r = dict(row)
    rows = []
    if r.get('n_tb'):
        rows.append({
            'score_date': score_date, 'player_type': 'batter', 'metric': 'tb',
            'n_players': r['n_tb'],
            'mae': r['tb_mae'], 'rmse': r['tb_rmse'],
            'mean_proj': r['tb_proj'], 'mean_actual': r['tb_actual'],
            'bias': r['tb_bias'],
            'within_1': r['tb_w1'], 'within_2': None,
        })
    if r.get('n_k'):
        rows.append({
            'score_date': score_date, 'player_type': 'batter', 'metric': 'k_rate',
            'n_players': r['n_k'],
            'mae': r['k_mae'], 'rmse': None,
            'mean_proj': None, 'mean_actual': None,
            'bias': r['k_bias'],
            'within_1': None, 'within_2': None,
        })
    if r.get('n_hr'):
        rows.append({
            'score_date': score_date, 'player_type': 'batter', 'metric': 'hr_brier',
            'n_players': r['n_hr'],
            'mae': r['hr_brier'], 'rmse': None,
            'mean_proj': r['hr_proj_rate'], 'mean_actual': r['hr_actual_rate'],
            'bias': None, 'within_1': None, 'within_2': None,
        })
    return rows


def upsert_scores(rows: list, db):
    if not rows:
        return
    sql = text("""
        INSERT INTO mlb.model_scores
            (score_date, player_type, metric, n_players, mae, rmse,
             mean_proj, mean_actual, bias, within_1, within_2)
        VALUES
            (:score_date, :player_type, :metric, :n_players, :mae, :rmse,
             :mean_proj, :mean_actual, :bias, :within_1, :within_2)
        ON CONFLICT (score_date, player_type, metric) DO UPDATE SET
            n_players   = EXCLUDED.n_players,
            mae         = EXCLUDED.mae,
            rmse        = EXCLUDED.rmse,
            mean_proj   = EXCLUDED.mean_proj,
            mean_actual = EXCLUDED.mean_actual,
            bias        = EXCLUDED.bias,
            within_1    = EXCLUDED.within_1,
            within_2    = EXCLUDED.within_2,
            updated_at  = NOW()
    """)
    for row in rows:
        db.execute(sql, row)
    db.commit()


def build_scores_for_date(score_date: date, db) -> int:
    rows = build_pitcher_scores(score_date, db) + build_batter_scores(score_date, db)
    if rows:
        upsert_scores(rows, db)
        print(f"  {score_date}: {len(rows)} metric rows stored")
    else:
        print(f"  {score_date}: no data")
    return len(rows)


def create_tables(db):
    db.execute(text(DDL))
    db.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=str(date.today() - timedelta(days=1)))
    parser.add_argument("--days", type=int, default=1,
                        help="Score last N days ending at --date")
    parser.add_argument("--backfill", action="store_true",
                        help="Score all dates with actuals not yet scored")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        create_tables(db)

        if args.backfill:
            dates = db.execute(text("""
                SELECT DISTINCT da.game_date
                FROM mlb.daily_actuals da
                WHERE da.player_type = 'pitcher'
                  AND da.proj_k_pct IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM mlb.model_scores ms
                      WHERE ms.score_date = da.game_date
                        AND ms.metric = 'k_count'
                  )
                ORDER BY da.game_date
            """)).scalars().all()
            total = 0
            for d in dates:
                total += build_scores_for_date(d, db)
            print(f"Backfill complete: {len(dates)} dates, {total} rows")
        else:
            end = date.fromisoformat(args.date)
            for i in range(args.days):
                build_scores_for_date(end - timedelta(days=i), db)
    finally:
        db.close()
