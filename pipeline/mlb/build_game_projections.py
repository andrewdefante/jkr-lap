"""
Game-level run and W/L projection scoring.

For each game where we have both starting pitcher projections, compute:
  proj_away_runs = home_pitcher proj_ops × 9.0 - 0.876  (opp-lineup projection)
  proj_home_runs = away_pitcher proj_ops × 9.0 - 0.876
  proj_winner    = team with higher projected runs

Writes per-game rows to mlb.game_projections, then aggregates daily accuracy
into mlb.model_scores under player_type='game' with metrics:
  run_total  — MAE/RMSE/bias on projected vs actual total game runs
  team_runs  — same, per team (2× sample size)
  win_pick   — win prediction accuracy (mae=error rate, within_1=accuracy%)

Usage:
    python build_game_projections.py [--date 2025-05-20] [--days N] [--backfill]
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import argparse
from datetime import date, timedelta
from database import SessionLocal
from sqlalchemy import text

# Calibrated from 2026 season sample: avg proj_ops=0.595, avg actual runs=4.3/team
# proj_runs = proj_ops * SLOPE + INTERCEPT
OPS_SLOPE     = 9.0
OPS_INTERCEPT = -0.876


DDL = """
CREATE TABLE IF NOT EXISTS mlb.game_projections (
    game_pk           INTEGER      NOT NULL,
    game_date         DATE         NOT NULL,
    season            INTEGER,
    away_team_abbrev  VARCHAR(10),
    home_team_abbrev  VARCHAR(10),
    away_pitcher_id   INTEGER,
    home_pitcher_id   INTEGER,
    away_pitcher_name VARCHAR(100),
    home_pitcher_name VARCHAR(100),
    away_proj_ops     NUMERIC(6,3),
    home_proj_ops     NUMERIC(6,3),
    proj_away_runs    NUMERIC(5,2),
    proj_home_runs    NUMERIC(5,2),
    proj_total        NUMERIC(5,2),
    proj_run_diff     NUMERIC(5,2),
    proj_winner       VARCHAR(5),
    actual_away_runs  INTEGER,
    actual_home_runs  INTEGER,
    actual_total      INTEGER,
    actual_run_diff   INTEGER,
    actual_winner     VARCHAR(5),
    win_correct       BOOLEAN,
    snapshot_date     DATE,
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (game_pk)
);
CREATE INDEX IF NOT EXISTS ix_game_projections_date
    ON mlb.game_projections (game_date DESC);
"""


def upsert_game_projections(game_date: date, db) -> int:
    """Compute and store game projections for all games on game_date."""
    rows = db.execute(text("""
        SELECT
            g.game_pk,
            g.game_date::date,
            g.season,
            g.away_team_abbrev,
            g.home_team_abbrev,
            g.away_score,
            g.home_score,
            dp_away.pitcher_id   AS away_pitcher_id,
            dp_away.pitcher_name AS away_pitcher_name,
            dp_away.proj_ops     AS away_proj_ops,
            dp_home.pitcher_id   AS home_pitcher_id,
            dp_home.pitcher_name AS home_pitcher_name,
            dp_home.proj_ops     AS home_proj_ops,
            dp_away.snapshot_date
        FROM mlb.games g
        JOIN mlb.daily_projections dp_away
            ON dp_away.game_pk    = g.game_pk
           AND dp_away.team_abbrev = g.away_team_abbrev
        JOIN mlb.daily_projections dp_home
            ON dp_home.game_pk    = g.game_pk
           AND dp_home.team_abbrev = g.home_team_abbrev
        WHERE g.game_date::date = :d
          AND g.game_type = 'R'
    """), {"d": game_date}).mappings().all()

    if not rows:
        return 0

    sql = text("""
        INSERT INTO mlb.game_projections (
            game_pk, game_date, season,
            away_team_abbrev, home_team_abbrev,
            away_pitcher_id, home_pitcher_id,
            away_pitcher_name, home_pitcher_name,
            away_proj_ops, home_proj_ops,
            proj_away_runs, proj_home_runs, proj_total, proj_run_diff, proj_winner,
            actual_away_runs, actual_home_runs, actual_total, actual_run_diff,
            actual_winner, win_correct, snapshot_date
        ) VALUES (
            :game_pk, :game_date, :season,
            :away_abbrev, :home_abbrev,
            :away_pid, :home_pid,
            :away_pname, :home_pname,
            :away_ops, :home_ops,
            :proj_away, :proj_home, :proj_total, :proj_diff, :proj_winner,
            :actual_away, :actual_home, :actual_total, :actual_diff,
            :actual_winner, :win_correct, :snapshot_date
        )
        ON CONFLICT (game_pk) DO UPDATE SET
            away_proj_ops    = EXCLUDED.away_proj_ops,
            home_proj_ops    = EXCLUDED.home_proj_ops,
            proj_away_runs   = EXCLUDED.proj_away_runs,
            proj_home_runs   = EXCLUDED.proj_home_runs,
            proj_total       = EXCLUDED.proj_total,
            proj_run_diff    = EXCLUDED.proj_run_diff,
            proj_winner      = EXCLUDED.proj_winner,
            actual_away_runs = EXCLUDED.actual_away_runs,
            actual_home_runs = EXCLUDED.actual_home_runs,
            actual_total     = EXCLUDED.actual_total,
            actual_run_diff  = EXCLUDED.actual_run_diff,
            actual_winner    = EXCLUDED.actual_winner,
            win_correct      = EXCLUDED.win_correct,
            updated_at       = NOW()
    """)

    count = 0
    for r in rows:
        away_ops  = float(r['away_proj_ops']) if r['away_proj_ops'] else None
        home_ops  = float(r['home_proj_ops']) if r['home_proj_ops'] else None
        proj_away = round(away_ops * OPS_SLOPE + OPS_INTERCEPT, 2) if away_ops else None
        proj_home = round(home_ops * OPS_SLOPE + OPS_INTERCEPT, 2) if home_ops else None

        # away team runs = runs scored against the home pitcher (home_ops)
        # home team runs = runs scored against the away pitcher (away_ops)
        proj_away_runs = round(home_ops * OPS_SLOPE + OPS_INTERCEPT, 2) if home_ops else None
        proj_home_runs = round(away_ops * OPS_SLOPE + OPS_INTERCEPT, 2) if away_ops else None

        proj_total = round(proj_away_runs + proj_home_runs, 2) if (proj_away_runs and proj_home_runs) else None
        proj_diff  = round(proj_home_runs - proj_away_runs, 2) if (proj_away_runs and proj_home_runs) else None

        if proj_away_runs and proj_home_runs:
            if proj_away_runs > proj_home_runs + 0.1:
                proj_winner = 'away'
            elif proj_home_runs > proj_away_runs + 0.1:
                proj_winner = 'home'
            else:
                proj_winner = 'push'
        else:
            proj_winner = None

        actual_away = r['away_score']
        actual_home = r['home_score']
        actual_total = (actual_away + actual_home) if (actual_away is not None and actual_home is not None) else None
        actual_diff  = (actual_home - actual_away) if (actual_away is not None and actual_home is not None) else None

        if actual_away is not None and actual_home is not None:
            actual_winner = 'away' if actual_away > actual_home else ('home' if actual_home > actual_away else 'tie')
        else:
            actual_winner = None

        win_correct = None
        if proj_winner and actual_winner and proj_winner != 'push' and actual_winner != 'tie':
            win_correct = (proj_winner == actual_winner)

        db.execute(sql, {
            'game_pk':    r['game_pk'],
            'game_date':  game_date,
            'season':     r['season'],
            'away_abbrev': r['away_team_abbrev'],
            'home_abbrev': r['home_team_abbrev'],
            'away_pid':   r['away_pitcher_id'],
            'home_pid':   r['home_pitcher_id'],
            'away_pname': r['away_pitcher_name'],
            'home_pname': r['home_pitcher_name'],
            'away_ops':   away_ops,
            'home_ops':   home_ops,
            'proj_away':  proj_away_runs,
            'proj_home':  proj_home_runs,
            'proj_total': proj_total,
            'proj_diff':  proj_diff,
            'proj_winner': proj_winner,
            'actual_away': actual_away,
            'actual_home': actual_home,
            'actual_total': actual_total,
            'actual_diff':  actual_diff,
            'actual_winner': actual_winner,
            'win_correct':   win_correct,
            'snapshot_date': r['snapshot_date'],
        })
        count += 1

    db.commit()
    return count


def build_game_model_scores(score_date: date, db):
    """Aggregate game_projections for score_date into mlb.model_scores."""
    row = db.execute(text("""
        SELECT
            COUNT(*) FILTER (WHERE actual_total IS NOT NULL
                               AND proj_total  IS NOT NULL) AS n_total,
            ROUND(AVG(ABS(actual_total - proj_total))
                  FILTER (WHERE actual_total IS NOT NULL
                             AND proj_total  IS NOT NULL)::numeric, 3) AS total_mae,
            ROUND(SQRT(AVG(POWER(actual_total - proj_total, 2))
                  FILTER (WHERE actual_total IS NOT NULL
                             AND proj_total  IS NOT NULL))::numeric, 3) AS total_rmse,
            ROUND(AVG(proj_total)
                  FILTER (WHERE actual_total IS NOT NULL
                             AND proj_total  IS NOT NULL)::numeric, 2)  AS mean_proj_total,
            ROUND(AVG(actual_total)
                  FILTER (WHERE actual_total IS NOT NULL
                             AND proj_total  IS NOT NULL)::numeric, 2)  AS mean_actual_total,
            ROUND(AVG(proj_total - actual_total)
                  FILTER (WHERE actual_total IS NOT NULL
                             AND proj_total  IS NOT NULL)::numeric, 3)  AS total_bias,
            -- per-team runs (2× sample: treat away and home as separate obs)
            COUNT(*) FILTER (WHERE actual_total IS NOT NULL
                               AND proj_total   IS NOT NULL) * 2  AS n_team,
            ROUND(((
                AVG(ABS(actual_away_runs - proj_away_runs))
                FILTER (WHERE actual_away_runs IS NOT NULL AND proj_away_runs IS NOT NULL)
                +
                AVG(ABS(actual_home_runs - proj_home_runs))
                FILTER (WHERE actual_home_runs IS NOT NULL AND proj_home_runs IS NOT NULL)
            ) / 2.0)::numeric, 3) AS team_mae,
            ROUND(((
                AVG(proj_away_runs - actual_away_runs)
                FILTER (WHERE actual_away_runs IS NOT NULL AND proj_away_runs IS NOT NULL)
                +
                AVG(proj_home_runs - actual_home_runs)
                FILTER (WHERE actual_home_runs IS NOT NULL AND proj_home_runs IS NOT NULL)
            ) / 2.0)::numeric, 3) AS team_bias,
            -- win pick
            COUNT(*) FILTER (WHERE win_correct IS NOT NULL) AS n_win,
            ROUND(AVG(CASE WHEN win_correct THEN 1.0 ELSE 0.0 END)
                  FILTER (WHERE win_correct IS NOT NULL)::numeric * 100, 1) AS win_acc,
            ROUND(AVG(CASE WHEN win_correct THEN 0.0 ELSE 1.0 END)
                  FILTER (WHERE win_correct IS NOT NULL)::numeric, 4)       AS win_err_rate
        FROM mlb.game_projections
        WHERE game_date = :d
    """), {"d": score_date}).mappings().first()

    if not row or not row['n_total']:
        return []

    r = dict(row)
    score_rows = [
        {
            'score_date': score_date, 'player_type': 'game', 'metric': 'run_total',
            'n_players': r['n_total'],
            'mae': r['total_mae'], 'rmse': r['total_rmse'],
            'mean_proj': r['mean_proj_total'], 'mean_actual': r['mean_actual_total'],
            'bias': r['total_bias'],
            'within_1': None, 'within_2': None,
        },
        {
            'score_date': score_date, 'player_type': 'game', 'metric': 'team_runs',
            'n_players': r['n_team'],
            'mae': r['team_mae'], 'rmse': None,
            'mean_proj': None, 'mean_actual': None,
            'bias': r['team_bias'],
            'within_1': None, 'within_2': None,
        },
    ]
    if r.get('n_win'):
        score_rows.append({
            'score_date': score_date, 'player_type': 'game', 'metric': 'win_pick',
            'n_players': r['n_win'],
            'mae': r['win_err_rate'], 'rmse': None,
            'mean_proj': None, 'mean_actual': None,
            'bias': None,
            'within_1': r['win_acc'], 'within_2': None,
        })
    return score_rows


def upsert_model_scores(rows: list, db):
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


def build_for_date(score_date: date, db) -> int:
    n_games = upsert_game_projections(score_date, db)
    score_rows = build_game_model_scores(score_date, db)
    if score_rows:
        upsert_model_scores(score_rows, db)
        print(f"  {score_date}: {n_games} games projected, {len(score_rows)} score metrics stored")
    else:
        print(f"  {score_date}: {n_games} games projected, no completed games to score yet")
    return n_games


def create_tables(db):
    db.execute(text(DDL))
    db.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=str(date.today() - timedelta(days=1)))
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--backfill", action="store_true",
                        help="Project all games with daily_projections data")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        create_tables(db)

        if args.backfill:
            dates = db.execute(text("""
                SELECT DISTINCT g.game_date::date AS d
                FROM mlb.games g
                JOIN mlb.daily_projections dp_away
                    ON dp_away.game_pk = g.game_pk AND dp_away.team_abbrev = g.away_team_abbrev
                JOIN mlb.daily_projections dp_home
                    ON dp_home.game_pk = g.game_pk AND dp_home.team_abbrev = g.home_team_abbrev
                WHERE g.game_type = 'R'
                ORDER BY d
            """)).scalars().all()
            total = 0
            for d in dates:
                total += build_for_date(d, db)
            print(f"Backfill complete: {len(dates)} dates, {total} games")
        else:
            end = date.fromisoformat(args.date)
            for i in range(args.days):
                build_for_date(end - timedelta(days=i), db)
    finally:
        db.close()
