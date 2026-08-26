"""
Monte Carlo K count projection.

Uses 6-feature Ridge regression model retrained daily on point-in-time
season data (every feature for a historical row is computed using only
games strictly before that row's own game_date — no lookahead):
  - pitcher_season_k_rate
  - pitcher_roll_3g_k_rate
  - opp_season_k_rate
  - opp_roll_10g_k_rate (season rate used as proxy — see note below)
  - pit_park_k_rate
  - opp_bat_park_k_rate

Outputs:
  - k_avg_mc: mean simulated K count
  - k_floor_mc: 10th percentile x 0.95 (empirically hits ~90% in walk-forward backtest)
  - k_resid_std: model residual std used in simulation

Usage:
    from mlb.build_mc_projection import build_mc_projections
    build_mc_projections(target_date, db)
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import numpy as np
import pandas as pd
from datetime import date
from scipy.stats import truncnorm
from sqlalchemy import text
from sklearn.linear_model import RidgeCV
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

FEATURES = [
    'pitcher_season_k_rate',
    'pitcher_roll_3g_k_rate',
    'opp_season_k_rate',
    'opp_roll_10g_k_rate',
    'pit_park_k_rate',
    'opp_bat_park_k_rate',
]

LEAGUE_AVG_BF = 22.0
LEAGUE_STD_BF = 4.5
LEAGUE_MIN_BF = 15.0
LEAGUE_MAX_BF = 33.0
N_SIMS = 5000
MIN_TRAIN_ROWS = 100
MC_FLOOR_ADJUSTMENT = 0.95  # empirically validated at ~90% walk-forward hit rate

# Point-in-time training query.
#
# `pitcher_starts` numbers each pitcher's starts in date order. Every feature
# below is computed for a given start using only OTHER rows that are earlier
# in that same ordering (or, for opponent/park features, games strictly
# before that start's own game_date) — never using data from after the row's
# own game. This avoids baking a pitcher's full-season-to-date-of-training
# stats into their early-season rows (which would leak future performance
# into the fit and understate residual variance used by the simulation).
TRAINING_SQL = """
    WITH pitcher_starts AS (
        SELECT
            bp.player_id, bp.game_pk, g.game_date::date as game_date, bp.team_id,
            bp.strikeouts, bp.batters_faced,
            g.home_team_id,
            CASE WHEN g.home_team_id = bp.team_id THEN g.away_team_id
                 ELSE g.home_team_id END as opp_team_id,
            ROW_NUMBER() OVER (
                PARTITION BY bp.player_id ORDER BY g.game_date::date ASC, bp.game_pk ASC
            ) as game_number
        FROM mlb.boxscore_pitching bp
        JOIN mlb.games g ON g.game_pk = bp.game_pk
        WHERE g.season = 2026 AND g.game_type = 'R'
        AND g.game_date::date < :target_date
        AND bp.games_started = 1 AND bp.batters_faced > 0
    ),
    pitcher_season AS (
        SELECT cur.game_pk,
            SUM(prior.strikeouts)::numeric / NULLIF(SUM(prior.batters_faced), 0) as pitcher_season_k_rate
        FROM pitcher_starts cur
        JOIN pitcher_starts prior
            ON prior.player_id = cur.player_id
            AND prior.game_number < cur.game_number
        GROUP BY cur.game_pk
    ),
    pitcher_roll3 AS (
        SELECT cur.game_pk,
            SUM(prior.strikeouts)::numeric / NULLIF(SUM(prior.batters_faced), 0) as pitcher_roll_3g_k_rate
        FROM pitcher_starts cur
        JOIN pitcher_starts prior
            ON prior.player_id = cur.player_id
            AND prior.game_number >= cur.game_number - 3
            AND prior.game_number < cur.game_number
        GROUP BY cur.game_pk
    ),
    opp_season AS (
        SELECT cur.game_pk,
            SUM(bb.strikeouts)::numeric / NULLIF(SUM(bb.plate_appearances), 0) as opp_season_k_rate
        FROM pitcher_starts cur
        JOIN mlb.boxscore_batting bb ON bb.team_id = cur.opp_team_id
        JOIN mlb.games g2 ON g2.game_pk = bb.game_pk
        WHERE g2.season = 2026 AND g2.game_type = 'R'
        AND g2.game_date::date < cur.game_date
        AND bb.plate_appearances > 0
        GROUP BY cur.game_pk
    ),
    pit_park AS (
        SELECT cur.game_pk,
            SUM(bp2.strikeouts)::numeric / NULLIF(SUM(bp2.batters_faced), 0) as pit_park_k_rate
        FROM pitcher_starts cur
        JOIN mlb.boxscore_pitching bp2 ON bp2.team_id = cur.team_id
        JOIN mlb.games g2 ON g2.game_pk = bp2.game_pk
        WHERE g2.season = 2026 AND g2.game_type = 'R'
        AND g2.game_date::date < cur.game_date
        AND g2.home_team_id = cur.home_team_id
        AND bp2.batters_faced > 0
        GROUP BY cur.game_pk
    ),
    opp_bat_park AS (
        SELECT cur.game_pk,
            SUM(bb2.strikeouts)::numeric / NULLIF(SUM(bb2.plate_appearances), 0) as opp_bat_park_k_rate
        FROM pitcher_starts cur
        JOIN mlb.boxscore_batting bb2 ON bb2.team_id = cur.opp_team_id
        JOIN mlb.games g2 ON g2.game_pk = bb2.game_pk
        WHERE g2.season = 2026 AND g2.game_type = 'R'
        AND g2.game_date::date < cur.game_date
        AND g2.home_team_id = cur.home_team_id
        AND bb2.plate_appearances > 0
        GROUP BY cur.game_pk
    )
    SELECT
        ps.player_id, ps.game_pk, ps.game_date,
        ps.strikeouts as actual_k,
        ps.strikeouts::numeric / NULLIF(ps.batters_faced, 0) as actual_k_rate,
        pse.pitcher_season_k_rate,
        pr3.pitcher_roll_3g_k_rate,
        os.opp_season_k_rate,
        os.opp_season_k_rate as opp_roll_10g_k_rate,
        pp.pit_park_k_rate,
        obp.opp_bat_park_k_rate
    FROM pitcher_starts ps
    LEFT JOIN pitcher_season pse ON pse.game_pk = ps.game_pk
    LEFT JOIN pitcher_roll3 pr3 ON pr3.game_pk = ps.game_pk
    LEFT JOIN opp_season os ON os.game_pk = ps.game_pk
    LEFT JOIN pit_park pp ON pp.game_pk = ps.game_pk
    LEFT JOIN opp_bat_park obp ON obp.game_pk = ps.game_pk
    WHERE pse.pitcher_season_k_rate IS NOT NULL
"""

# Today's prediction features. Unlike the training query, there is only one
# "row" per pitcher to predict, and today's game hasn't happened yet, so
# aggregating everything strictly before target_date is correct here (not a
# lookahead — it's exactly what would be knowable this morning).
TODAY_SQL = """
    WITH pitcher_season AS (
        SELECT bp.player_id,
            SUM(bp.strikeouts)::numeric / NULLIF(SUM(bp.batters_faced), 0) as pitcher_season_k_rate
        FROM mlb.boxscore_pitching bp
        JOIN mlb.games g ON g.game_pk = bp.game_pk
        WHERE g.season = 2026 AND g.game_type = 'R'
        AND g.game_date::date < :target_date
        AND bp.games_started = 1 AND bp.batters_faced > 0
        GROUP BY bp.player_id
    ),
    pitcher_roll3 AS (
        SELECT player_id,
            SUM(strikeouts)::numeric / NULLIF(SUM(batters_faced), 0) as pitcher_roll_3g_k_rate
        FROM (
            SELECT bp.player_id, bp.strikeouts, bp.batters_faced,
                ROW_NUMBER() OVER (PARTITION BY bp.player_id ORDER BY g.game_date DESC) as rn
            FROM mlb.boxscore_pitching bp
            JOIN mlb.games g ON g.game_pk = bp.game_pk
            WHERE g.season = 2026 AND g.game_type = 'R'
            AND g.game_date::date < :target_date
            AND bp.games_started = 1 AND bp.batters_faced > 0
        ) ranked
        WHERE rn <= 3
        GROUP BY player_id
    ),
    opp_season AS (
        SELECT
            CASE WHEN g.home_team_id = bb.team_id THEN g.away_team_id
                 ELSE g.home_team_id END as team_id,
            SUM(bb.strikeouts)::numeric / NULLIF(SUM(bb.plate_appearances), 0) as opp_season_k_rate
        FROM mlb.boxscore_batting bb
        JOIN mlb.games g ON g.game_pk = bb.game_pk
        WHERE g.season = 2026 AND g.game_type = 'R'
        AND g.game_date::date < :target_date
        AND bb.plate_appearances > 0
        GROUP BY 1
    ),
    pit_park AS (
        SELECT bp.team_id, g.home_team_id as site_id,
            SUM(bp.strikeouts)::numeric / NULLIF(SUM(bp.batters_faced), 0) as pit_park_k_rate
        FROM mlb.boxscore_pitching bp
        JOIN mlb.games g ON g.game_pk = bp.game_pk
        WHERE g.season = 2026 AND g.game_type = 'R'
        AND g.game_date::date < :target_date AND bp.batters_faced > 0
        GROUP BY bp.team_id, g.home_team_id
    ),
    opp_bat_park AS (
        SELECT bb.team_id as opp_team_id, g.home_team_id as site_id,
            SUM(bb.strikeouts)::numeric / NULLIF(SUM(bb.plate_appearances), 0) as opp_bat_park_k_rate
        FROM mlb.boxscore_batting bb
        JOIN mlb.games g ON g.game_pk = bb.game_pk
        WHERE g.season = 2026 AND g.game_type = 'R'
        AND g.game_date::date < :target_date AND bb.plate_appearances > 0
        GROUP BY bb.team_id, g.home_team_id
    ),
    pitcher_bf AS (
        SELECT player_id,
            AVG(batters_faced) as avg_bf,
            STDDEV(batters_faced) as std_bf,
            MIN(batters_faced) as min_bf,
            MAX(batters_faced) as max_bf
        FROM mlb.boxscore_pitching bp
        JOIN mlb.games g ON g.game_pk = bp.game_pk
        WHERE g.season = 2026 AND g.game_type = 'R'
        AND g.game_date::date < :target_date
        AND bp.games_started = 1 AND bp.batters_faced > 0
        GROUP BY player_id
    ),
    -- Team abbrev -> team_id lookup built from this season's history, so it
    -- never depends on today's specific game_pk being present in mlb.games
    -- (daily_projections is populated straight from the live MLB schedule
    -- API and can be ahead of mlb.games, which lags until the fetch job
    -- picks a game up).
    team_abbrev_map AS (
        SELECT DISTINCT home_team_abbrev as abbrev, home_team_id as team_id
        FROM mlb.games WHERE season = 2026
        UNION
        SELECT DISTINCT away_team_abbrev, away_team_id
        FROM mlb.games WHERE season = 2026
    )
    SELECT
        dp.pitcher_id as player_id,
        dp.game_pk,
        dp.pitcher_name,
        dp.team_abbrev,
        dp.opp_abbrev,
        dp.opp_team_id,
        site_map.team_id as site_id,
        pitcher_map.team_id as pitcher_team_id,
        ps.pitcher_season_k_rate,
        pr3.pitcher_roll_3g_k_rate,
        os.opp_season_k_rate,
        os.opp_season_k_rate as opp_roll_10g_k_rate,
        COALESCE(pp.pit_park_k_rate,
            (SELECT SUM(bp2.strikeouts)::numeric / NULLIF(SUM(bp2.batters_faced), 0)
             FROM mlb.boxscore_pitching bp2
             JOIN mlb.games g2 ON g2.game_pk = bp2.game_pk
             WHERE g2.season = 2026 AND g2.game_type = 'R'
             AND g2.game_date::date < :target_date
             AND g2.home_team_id = site_map.team_id
             AND bp2.batters_faced > 0)
        ) as pit_park_k_rate,
        COALESCE(obp.opp_bat_park_k_rate,
            (SELECT SUM(bb2.strikeouts)::numeric / NULLIF(SUM(bb2.plate_appearances), 0)
             FROM mlb.boxscore_batting bb2
             JOIN mlb.games g2 ON g2.game_pk = bb2.game_pk
             WHERE g2.season = 2026 AND g2.game_type = 'R'
             AND g2.game_date::date < :target_date
             AND g2.home_team_id = site_map.team_id
             AND bb2.plate_appearances > 0)
        ) as opp_bat_park_k_rate,
        COALESCE(pbf.avg_bf, :league_avg_bf) as avg_bf,
        COALESCE(pbf.std_bf, :league_std_bf) as std_bf,
        COALESCE(pbf.min_bf, :league_min_bf) as min_bf,
        COALESCE(pbf.max_bf, :league_max_bf) as max_bf
    FROM mlb.daily_projections dp
    LEFT JOIN team_abbrev_map site_map ON site_map.abbrev = dp.home_team_abbrev
    LEFT JOIN team_abbrev_map pitcher_map ON pitcher_map.abbrev = dp.team_abbrev
    LEFT JOIN pitcher_season ps ON ps.player_id = dp.pitcher_id
    LEFT JOIN pitcher_roll3 pr3 ON pr3.player_id = dp.pitcher_id
    LEFT JOIN opp_season os ON os.team_id = dp.opp_team_id
    LEFT JOIN pitcher_bf pbf ON pbf.player_id = dp.pitcher_id
    LEFT JOIN pit_park pp
        ON pp.team_id = pitcher_map.team_id
        AND pp.site_id = site_map.team_id
    LEFT JOIN opp_bat_park obp ON obp.opp_team_id = dp.opp_team_id AND obp.site_id = site_map.team_id
    WHERE dp.snapshot_date = :target_date
"""


def get_training_data(target_date: str, db) -> list:
    rows = db.execute(text(TRAINING_SQL), {"target_date": target_date}).mappings().all()
    return [dict(r) for r in rows]


def get_floor_adjustment(db) -> float:
    """Load the most recent MC floor adjustment from projection_calibration."""
    try:
        row = db.execute(text("""
            SELECT mc_floor_adjustment
            FROM mlb.projection_calibration
            WHERE mc_floor_adjustment IS NOT NULL
            ORDER BY computed_at DESC LIMIT 1
        """)).scalar()
        if row is not None:
            return float(row)
    except Exception:
        pass
    return MC_FLOOR_ADJUSTMENT  # fallback to default 0.95


def get_today_features(target_date: str, db) -> list:
    rows = db.execute(text(TODAY_SQL), {
        "target_date": target_date,
        "league_avg_bf": LEAGUE_AVG_BF,
        "league_std_bf": LEAGUE_STD_BF,
        "league_min_bf": LEAGUE_MIN_BF,
        "league_max_bf": LEAGUE_MAX_BF,
    }).mappings().all()
    return [dict(r) for r in rows]


def simulate_k(pred_k_rate, avg_bf, std_bf, min_bf, max_bf, resid_std,
                floor_adjustment=MC_FLOOR_ADJUSTMENT, n_sims=N_SIMS):
    """
    Monte Carlo K count simulation using:
    - Truncated normal for BF (properly bounded at pitcher's actual min/max)
    - Normal for K rate residuals (clipped at physiological extremes)
    """
    np.random.seed(42)

    std_bf = max(std_bf, 1.0)
    if max_bf - min_bf < 1:
        # Guard against degenerate bounds (e.g. a pitcher with a single
        # historical start, where min_bf == max_bf) — truncnorm requires a < b.
        min_bf, max_bf = min_bf - 2, max_bf + 2
    a = (min_bf - avg_bf) / std_bf
    b = (max_bf - avg_bf) / std_bf
    sim_bf = truncnorm.rvs(a, b, loc=avg_bf, scale=std_bf, size=n_sims, random_state=42)

    sim_rates = pred_k_rate + np.random.normal(0, resid_std, n_sims)
    sim_rates = np.clip(sim_rates, 0.05, 0.60)
    sim_k = sim_rates * sim_bf
    floor = np.percentile(sim_k, 10) * floor_adjustment
    avg = np.mean(sim_k)
    return round(float(floor), 2), round(float(avg), 2)


def build_mc_projections(target_date: str, db) -> int:
    """
    Train Ridge model on point-in-time data before target_date,
    predict K floor and avg for today's starters,
    upsert into daily_projections.
    Returns number of pitchers updated.
    """
    floor_adjustment = get_floor_adjustment(db)

    train_rows = get_training_data(target_date, db)
    if len(train_rows) < MIN_TRAIN_ROWS:
        print(f"  MC projection: insufficient training data ({len(train_rows)} rows)")
        return 0

    train_df = pd.DataFrame(train_rows).dropna(subset=FEATURES + ['actual_k_rate'])
    if len(train_df) < MIN_TRAIN_ROWS:
        print(f"  MC projection: insufficient complete-feature rows ({len(train_df)})")
        return 0
    for feat in FEATURES + ['actual_k_rate']:
        train_df[feat] = train_df[feat].astype(float)

    pipe = Pipeline([
        ('scaler', StandardScaler()),
        ('ridge', RidgeCV(alphas=[0.1, 1, 5, 10, 50, 100], cv=5,
                           scoring='neg_mean_absolute_error'))
    ])
    pipe.fit(train_df[FEATURES], train_df['actual_k_rate'])

    train_pred = pipe.predict(train_df[FEATURES])
    resid_std = float((train_df['actual_k_rate'].values - train_pred).std())

    today_rows = get_today_features(target_date, db)
    if not today_rows:
        print(f"  MC projection: no pitchers found for {target_date}")
        return 0

    today_df = pd.DataFrame(today_rows)
    for feat in FEATURES:
        today_df[feat] = today_df[feat].astype(float)

    league_avgs = train_df[FEATURES].mean()
    for feat in FEATURES:
        today_df[feat] = today_df[feat].fillna(league_avgs[feat])

    today_df['pred_k_rate'] = pipe.predict(today_df[FEATURES])

    updated = 0
    for _, row in today_df.iterrows():
        floor_mc, avg_mc = simulate_k(
            row['pred_k_rate'],
            float(row['avg_bf']),
            float(row['std_bf']),
            float(row['min_bf']),
            float(row['max_bf']),
            resid_std,
            floor_adjustment=floor_adjustment,
        )

        db.execute(text("""
            UPDATE mlb.daily_projections
            SET k_floor_mc = :floor_mc,
                k_avg_mc = :avg_mc,
                k_resid_std = :resid_std
            WHERE snapshot_date = :snap_date
            AND pitcher_id = :pitcher_id
        """), {
            "floor_mc": floor_mc,
            "avg_mc": avg_mc,
            "resid_std": round(resid_std, 4),
            "snap_date": target_date,
            "pitcher_id": int(row['player_id']),
        })
        updated += 1

    db.commit()
    print(f"  MC projection: {updated} pitchers updated "
          f"(train_rows={len(train_df)}, resid_std={resid_std:.4f}, "
          f"floor_adj={floor_adjustment:.3f}, "
          f"alpha={pipe.named_steps['ridge'].alpha_})")
    return updated


if __name__ == "__main__":
    import argparse
    from datetime import date as dt
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=str(dt.today()))
    args = parser.parse_args()
    from database import SessionLocal
    db = SessionLocal()
    try:
        build_mc_projections(args.date, db)
    finally:
        db.close()
