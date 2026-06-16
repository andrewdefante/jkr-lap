"""
Build team defensive metrics from 2026 at_bats data.
Computes BABIP allowed, SLG allowed on BIP, XBH rate, error rate.
Stored in mlb.team_defense — used by batter projections and inning simulator.

Usage:
    PYTHONPATH=/app:/pipeline python3 /pipeline/mlb/build_team_defense.py --season 2026
"""
import sys
import os
import argparse
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'api'))
from database import SessionLocal
from sqlalchemy import text

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS mlb.team_defense (
    team_abbrev     VARCHAR(10) NOT NULL,
    team_id         INTEGER,
    season          INTEGER NOT NULL,
    babip_allowed   FLOAT,
    slg_allowed     FLOAT,
    xbh_rate        FLOAT,
    single_rate     FLOAT,
    double_rate     FLOAT,
    triple_rate     FLOAT,
    bip_count       INTEGER,
    pa_count        INTEGER,
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (team_abbrev, season)
);
CREATE INDEX IF NOT EXISTS ix_team_defense_team_id
    ON mlb.team_defense (team_id, season);
"""


def build_team_defense(season: int, db) -> int:
    db.execute(text(CREATE_TABLE))
    db.commit()

    rows = db.execute(text("""
        SELECT
            g.home_team_id   as team_id,
            g.home_team_abbrev as team_abbrev,
            COUNT(*) as pa_count,
            SUM(CASE WHEN ab.event NOT IN ('Home Run','Strikeout','Strikeout - DP',
                'Walk','Intent Walk','Hit By Pitch') THEN 1 ELSE 0 END) as bip_count,
            SUM(CASE WHEN ab.event = 'Single'   THEN 1 ELSE 0 END) as singles,
            SUM(CASE WHEN ab.event = 'Double'   THEN 1 ELSE 0 END) as doubles,
            SUM(CASE WHEN ab.event = 'Triple'   THEN 1 ELSE 0 END) as triples,
            SUM(CASE WHEN ab.event = 'Home Run' THEN 1 ELSE 0 END) as hrs
        FROM mlb.at_bats ab
        JOIN mlb.games g ON g.game_pk = ab.game_pk
        WHERE g.season = :season AND g.game_type = 'R'
        AND ab.half_inning = 'top'
        AND ab.event NOT IN ('Runner Out','Caught Stealing 2B','Caught Stealing 3B',
            'Wild Pitch','Passed Ball')
        GROUP BY g.home_team_id, g.home_team_abbrev
        HAVING COUNT(*) >= 100
    """), {"season": season}).mappings().all()

    upserted = 0
    for r in rows:
        bip  = r['bip_count'] or 1
        hits = (r['singles'] or 0) + (r['doubles'] or 0) + (r['triples'] or 0)
        total_bases = ((r['singles'] or 0) + (r['doubles'] or 0) * 2 +
                       (r['triples'] or 0) * 3 + (r['hrs'] or 0) * 4)

        babip   = hits / max(1, bip - (r['hrs'] or 0))
        slg     = total_bases / max(1, r['pa_count'])
        xbh_rate = ((r['doubles'] or 0) + (r['triples'] or 0)) / max(1, hits)

        db.execute(text("""
            INSERT INTO mlb.team_defense
                (team_abbrev, team_id, season, babip_allowed, slg_allowed, xbh_rate,
                 single_rate, double_rate, triple_rate, bip_count, pa_count, updated_at)
            VALUES
                (:team, :team_id, :season, :babip, :slg, :xbh,
                 :single_r, :double_r, :triple_r, :bip, :pa, NOW())
            ON CONFLICT (team_abbrev, season) DO UPDATE SET
                team_id       = EXCLUDED.team_id,
                babip_allowed = EXCLUDED.babip_allowed,
                slg_allowed   = EXCLUDED.slg_allowed,
                xbh_rate      = EXCLUDED.xbh_rate,
                single_rate   = EXCLUDED.single_rate,
                double_rate   = EXCLUDED.double_rate,
                triple_rate   = EXCLUDED.triple_rate,
                bip_count     = EXCLUDED.bip_count,
                pa_count      = EXCLUDED.pa_count,
                updated_at    = NOW()
        """), {
            "team":     r['team_abbrev'],
            "team_id":  int(r['team_id']),
            "season":   season,
            "babip":    round(babip, 4),
            "slg":      round(slg, 4),
            "xbh":      round(xbh_rate, 4),
            "single_r": round((r['singles'] or 0) / max(1, hits), 4),
            "double_r": round((r['doubles'] or 0) / max(1, hits), 4),
            "triple_r": round((r['triples'] or 0) / max(1, hits), 4),
            "bip":      int(bip),
            "pa":       int(r['pa_count']),
        })
        upserted += 1

    db.commit()
    print(f"  team_defense: {upserted} teams upserted for {season}")
    return upserted


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, default=2026)
    args = parser.parse_args()
    db = SessionLocal()
    try:
        build_team_defense(args.season, db)
    finally:
        db.close()
