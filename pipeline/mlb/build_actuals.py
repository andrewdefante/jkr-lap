"""
Build daily_actuals table joining yesterday's projections to box scores.
Run each evening after games finish (~11pm ET).
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import argparse
from datetime import date, timedelta
from database import SessionLocal
from sqlalchemy import text


def build_pitcher_actuals(game_date: date, db):
    sql = text("""
        INSERT INTO mlb.daily_actuals (
            game_date, game_pk, player_id, player_name,
            player_type, team_abbrev, opponent_abbrev,
            actual_k, actual_ip, actual_er, actual_bb,
            actual_hits_allowed, actual_pitches,
            proj_k_pct, proj_hr_pct, proj_ops,
            proj_ks_6inn, proj_ks_locked,
            kalshi_k_line, kalshi_k_yes_price
        )
        SELECT
            :game_date,
            bp.game_pk,
            bp.player_id,
            names.pitcher_name,
            'pitcher',
            CASE WHEN g.home_team_id = bp.team_id
                 THEN g.home_team_abbrev
                 ELSE g.away_team_abbrev END as team_abbrev,
            CASE WHEN g.home_team_id = bp.team_id
                 THEN g.away_team_abbrev
                 ELSE g.home_team_abbrev END as opponent_abbrev,
            bp.strikeouts,
            bp.innings_pitched,
            bp.earned_runs,
            bp.walks,
            bp.hits,
            bp.pitches_thrown,
            dp.proj_k_pct,
            dp.proj_hr_pct,
            dp.proj_ops,
            dp.proj_ks_6inn,
            COALESCE(dp.proj_ks_locked, dp.proj_ks_6inn) as proj_ks_locked,
            km.floor_strike,
            km.yes_ask
        FROM mlb.boxscore_pitching bp
        JOIN mlb.games g ON g.game_pk = bp.game_pk
            AND g.game_date = :game_date_str
            AND g.game_type = 'R'
            AND g.status IN ('Final','Game Over')
        -- Name lookup: one row per pitcher per game
        LEFT JOIN LATERAL (
            SELECT DISTINCT pitcher_name
            FROM mlb.at_bats
            WHERE game_pk = bp.game_pk AND pitcher_id = bp.player_id
            LIMIT 1
        ) names ON true
        LEFT JOIN mlb.daily_projections dp
            ON dp.pitcher_id = bp.player_id
            AND dp.snapshot_date = :game_date
        LEFT JOIN LATERAL (
            SELECT floor_strike, yes_ask
            FROM mlb.kalshi_markets
            WHERE mlbam_id = bp.player_id
              AND game_date = :game_date
              AND fetch_date = (
                  SELECT MAX(fetch_date) FROM mlb.kalshi_markets
                  WHERE game_date = :game_date
              )
            ORDER BY ABS(implied_prob - 0.5)
            LIMIT 1
        ) km ON true
        WHERE bp.games_started = 1
        ON CONFLICT (game_date, game_pk, player_id, player_type) DO UPDATE SET
            actual_k = EXCLUDED.actual_k,
            actual_ip = EXCLUDED.actual_ip,
            actual_er = EXCLUDED.actual_er,
            proj_ks_6inn = EXCLUDED.proj_ks_6inn,
            proj_ks_locked = EXCLUDED.proj_ks_locked,
            kalshi_k_line = EXCLUDED.kalshi_k_line,
            kalshi_k_yes_price = EXCLUDED.kalshi_k_yes_price,
            computed_at = NOW()
    """)
    result = db.execute(sql, {"game_date": game_date, "game_date_str": str(game_date)})
    print(f"  Pitcher actuals: {result.rowcount} rows upserted")


def build_batter_actuals(game_date: date, db):
    sql = text("""
        INSERT INTO mlb.daily_actuals (
            game_date, game_pk, player_id, player_name,
            player_type, team_abbrev,
            actual_tb, actual_hr, actual_hits,
            actual_ab, actual_bb_batter, actual_k_batter,
            proj_tb, proj_hr_pct, proj_ops
        )
        SELECT
            :game_date,
            bb.game_pk,
            bb.player_id,
            names.batter_name,
            'batter',
            CASE WHEN g.home_team_id = bb.team_id
                 THEN g.home_team_abbrev
                 ELSE g.away_team_abbrev END,
            bb.total_bases,
            bb.home_runs,
            bb.hits,
            bb.at_bats,
            bb.walks,
            bb.strikeouts,
            dbp.proj_slg * 4,   -- rough TB proxy
            dbp.proj_hr_pct,
            dbp.proj_ops
        FROM mlb.boxscore_batting bb
        JOIN mlb.games g ON g.game_pk = bb.game_pk
            AND g.game_date = :game_date_str
            AND g.game_type = 'R'
            AND g.status IN ('Final','Game Over')
        LEFT JOIN LATERAL (
            SELECT DISTINCT batter_name
            FROM mlb.at_bats
            WHERE game_pk = bb.game_pk AND batter_id = bb.player_id
            LIMIT 1
        ) names ON true
        -- Pick one projection row per batter (could be multiple bat_sides)
        LEFT JOIN LATERAL (
            SELECT proj_slg, proj_hr_pct, proj_ops
            FROM mlb.daily_batter_projections
            WHERE batter_id = bb.player_id AND snapshot_date = :game_date
            ORDER BY simulations DESC NULLS LAST
            LIMIT 1
        ) dbp ON true
        WHERE bb.at_bats >= 2
        ON CONFLICT (game_date, game_pk, player_id, player_type) DO UPDATE SET
            actual_tb = EXCLUDED.actual_tb,
            actual_hr = EXCLUDED.actual_hr,
            actual_hits = EXCLUDED.actual_hits,
            computed_at = NOW()
    """)
    result = db.execute(sql, {"game_date": game_date, "game_date_str": str(game_date)})
    print(f"  Batter actuals: {result.rowcount} rows upserted")


def build_actuals(game_date: date, db):
    print(f"Building actuals for {game_date}...")
    build_pitcher_actuals(game_date, db)
    build_batter_actuals(game_date, db)
    db.commit()
    print(f"  Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str,
                        default=str(date.today() - timedelta(days=1)))
    args = parser.parse_args()
    game_date = date.fromisoformat(args.date)

    db = SessionLocal()
    try:
        build_actuals(game_date, db)
    finally:
        db.close()
