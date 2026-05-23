"""
Contextual splits pipeline.

Builds and persists:
  1. mlb.pitcher_splits  — TTO, platoon, situation splits for all pitchers
  2. mlb.batter_splits   — same for batters
  3. mlb.player_workload — acute:chronic workload ratio (ACWR) per player
  4. mlb.matchup_history — head-to-head history with pre-computed regression

Usage:
    PYTHONPATH=/app:/pipeline python3 /pipeline/mlb/build_context_splits.py --seasons 2024 2025 2026
    PYTHONPATH=/app:/pipeline python3 /pipeline/mlb/build_context_splits.py --workload-only
    PYTHONPATH=/app:/pipeline python3 /pipeline/mlb/build_context_splits.py --matchup-only --seasons 2024 2025 2026
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import argparse
from datetime import date, timedelta
from database import SessionLocal
from sqlalchemy import text


# ── DDL ──────────────────────────────────────────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS mlb.pitcher_splits (
    pitcher_id   INTEGER      NOT NULL,
    season       SMALLINT     NOT NULL,
    split_type   VARCHAR(30)  NOT NULL,
    pa           INTEGER      NOT NULL DEFAULT 0,
    k_pct        NUMERIC(6,2),
    bb_pct       NUMERIC(6,2),
    hr_pct       NUMERIC(6,2),
    avg_against  NUMERIC(6,3),
    obp_against  NUMERIC(6,3),
    slg_against  NUMERIC(6,3),
    ops_against  NUMERIC(6,3),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pitcher_id, season, split_type)
);
CREATE INDEX IF NOT EXISTS ix_pitcher_splits_season
    ON mlb.pitcher_splits (season, pitcher_id);

CREATE TABLE IF NOT EXISTS mlb.batter_splits (
    batter_id    INTEGER      NOT NULL,
    season       SMALLINT     NOT NULL,
    split_type   VARCHAR(30)  NOT NULL,
    pa           INTEGER      NOT NULL DEFAULT 0,
    k_pct        NUMERIC(6,2),
    bb_pct       NUMERIC(6,2),
    hr_pct       NUMERIC(6,2),
    avg          NUMERIC(6,3),
    obp          NUMERIC(6,3),
    slg          NUMERIC(6,3),
    ops          NUMERIC(6,3),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batter_id, season, split_type)
);
CREATE INDEX IF NOT EXISTS ix_batter_splits_season
    ON mlb.batter_splits (season, batter_id);

CREATE TABLE IF NOT EXISTS mlb.player_workload (
    player_id    INTEGER      NOT NULL,
    player_type  VARCHAR(10)  NOT NULL,
    calc_date    DATE         NOT NULL,
    load_7d      NUMERIC(8,2),
    load_28d     NUMERIC(8,2),
    acwr         NUMERIC(6,3),
    games_7d     SMALLINT,
    games_28d    SMALLINT,
    total_load_7d INTEGER,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (player_id, player_type, calc_date)
);
CREATE INDEX IF NOT EXISTS ix_player_workload_date
    ON mlb.player_workload (calc_date DESC, player_id);

CREATE TABLE IF NOT EXISTS mlb.matchup_history (
    pitcher_id   INTEGER      NOT NULL,
    batter_id    INTEGER      NOT NULL,
    season       SMALLINT     NOT NULL,
    pa           INTEGER      NOT NULL DEFAULT 0,
    k            INTEGER      NOT NULL DEFAULT 0,
    bb           INTEGER      NOT NULL DEFAULT 0,
    hr           INTEGER      NOT NULL DEFAULT 0,
    hits         INTEGER      NOT NULL DEFAULT 0,
    total_bases  INTEGER      NOT NULL DEFAULT 0,
    k_pct_raw    NUMERIC(6,2),
    hr_pct_raw   NUMERIC(6,2),
    k_pct_reg    NUMERIC(6,2),
    hr_pct_reg   NUMERIC(6,2),
    sample_weight NUMERIC(5,3),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pitcher_id, batter_id, season)
);
CREATE INDEX IF NOT EXISTS ix_matchup_history_pitcher
    ON mlb.matchup_history (pitcher_id, season);
CREATE INDEX IF NOT EXISTS ix_matchup_history_batter
    ON mlb.matchup_history (batter_id, season);
"""


def create_tables(db):
    for stmt in DDL.strip().split(';'):
        stmt = stmt.strip()
        if stmt:
            db.execute(text(stmt))
    db.commit()
    print("  Tables created/verified.")


# ── PITCHER SPLITS ────────────────────────────────────────────────────────────

PITCHER_SPLITS_SQL = """
WITH valid_abs AS (
    SELECT
        ab.pitcher_id,
        ab.batter_id,
        ab.game_pk,
        ab.inning,
        ab.bat_side,
        ab.on_first,
        ab.on_second,
        ab.on_third,
        ab.outs_before,
        g.season,
        ROW_NUMBER() OVER (
            PARTITION BY ab.game_pk, ab.pitcher_id, ab.batter_id
            ORDER BY ab.at_bat_index
        ) AS times_faced,
        CASE WHEN ab.event IN ('Single','Double','Triple','Home Run') THEN 1 ELSE 0 END AS is_hit,
        CASE WHEN ab.event = 'Home Run'                              THEN 1 ELSE 0 END AS is_hr,
        CASE WHEN ab.event IN ('Strikeout','Strikeout - DP')         THEN 1 ELSE 0 END AS is_k,
        CASE WHEN ab.event IN ('Walk','Intent Walk')                  THEN 1 ELSE 0 END AS is_bb,
        CASE ab.event
            WHEN 'Single'   THEN 1 WHEN 'Double' THEN 2
            WHEN 'Triple'   THEN 3 WHEN 'Home Run' THEN 4
            ELSE 0 END AS total_bases
    FROM mlb.at_bats ab
    JOIN mlb.games g ON g.game_pk = ab.game_pk
    WHERE g.season IN ({seasons_clause})
    AND g.game_type = 'R'
    AND ab.runners_reconstructed = TRUE
    AND ab.event NOT IN (
        'Runner Out','Field Error','Caught Stealing 2B','Caught Stealing 3B',
        'Caught Stealing Home','Pickoff','Wild Pitch','Passed Ball'
    )
),
tagged AS (
    SELECT
        v.pitcher_id, v.season,
        s.split_type,
        v.is_hit, v.is_hr, v.is_k, v.is_bb, v.total_bases
    FROM valid_abs v
    CROSS JOIN LATERAL (VALUES
        ('tto_1',         v.times_faced = 1),
        ('tto_2',         v.times_faced = 2),
        ('tto_3plus',     v.times_faced >= 3),
        ('vs_lhh',        v.bat_side = 'L'),
        ('vs_rhh',        v.bat_side = 'R'),
        ('bases_empty',   NOT v.on_first AND NOT v.on_second AND NOT v.on_third),
        ('risp',          v.on_second OR v.on_third),
        ('early_innings', v.inning <= 3),
        ('mid_innings',   v.inning BETWEEN 4 AND 6),
        ('late_innings',  v.inning >= 7),
        ('overall',       TRUE)
    ) AS s(split_type, in_split)
    WHERE s.in_split
)
INSERT INTO mlb.pitcher_splits
    (pitcher_id, season, split_type, pa, k_pct, bb_pct, hr_pct,
     avg_against, obp_against, slg_against, ops_against, updated_at)
SELECT
    pitcher_id,
    season,
    split_type,
    COUNT(*)                                                                       AS pa,
    ROUND(SUM(is_k)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2)                      AS k_pct,
    ROUND(SUM(is_bb)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2)                     AS bb_pct,
    ROUND(SUM(is_hr)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2)                     AS hr_pct,
    ROUND(SUM(is_hit)::NUMERIC / NULLIF(COUNT(*) - SUM(is_bb), 0), 3)             AS avg_against,
    ROUND((SUM(is_hit) + SUM(is_bb))::NUMERIC / NULLIF(COUNT(*), 0), 3)           AS obp_against,
    ROUND(SUM(total_bases)::NUMERIC / NULLIF(COUNT(*) - SUM(is_bb), 0), 3)        AS slg_against,
    ROUND(
        (SUM(is_hit) + SUM(is_bb))::NUMERIC / NULLIF(COUNT(*), 0) +
        SUM(total_bases)::NUMERIC / NULLIF(COUNT(*) - SUM(is_bb), 0), 3
    )                                                                              AS ops_against,
    NOW()
FROM tagged
GROUP BY pitcher_id, season, split_type
HAVING COUNT(*) >= 5
ON CONFLICT (pitcher_id, season, split_type) DO UPDATE SET
    pa          = EXCLUDED.pa,
    k_pct       = EXCLUDED.k_pct,
    bb_pct      = EXCLUDED.bb_pct,
    hr_pct      = EXCLUDED.hr_pct,
    avg_against = EXCLUDED.avg_against,
    obp_against = EXCLUDED.obp_against,
    slg_against = EXCLUDED.slg_against,
    ops_against = EXCLUDED.ops_against,
    updated_at  = EXCLUDED.updated_at
"""


def build_pitcher_splits(seasons: list, db):
    print(f"\n  Building pitcher splits for seasons: {seasons}")
    seasons_clause = ','.join(str(s) for s in seasons)
    sql = PITCHER_SPLITS_SQL.format(seasons_clause=seasons_clause)
    result = db.execute(text(sql))
    db.commit()
    print(f"  Pitcher splits: {result.rowcount} rows upserted.")


# ── BATTER SPLITS ─────────────────────────────────────────────────────────────

BATTER_SPLITS_SQL = """
WITH valid_abs AS (
    SELECT
        ab.batter_id,
        ab.pitcher_id,
        ab.game_pk,
        ab.inning,
        ab.pitch_hand,
        ab.on_first,
        ab.on_second,
        ab.on_third,
        ab.outs_before,
        g.season,
        ROW_NUMBER() OVER (
            PARTITION BY ab.game_pk, ab.pitcher_id, ab.batter_id
            ORDER BY ab.at_bat_index
        ) AS times_faced,
        CASE WHEN ab.event IN ('Single','Double','Triple','Home Run') THEN 1 ELSE 0 END AS is_hit,
        CASE WHEN ab.event = 'Home Run'                              THEN 1 ELSE 0 END AS is_hr,
        CASE WHEN ab.event IN ('Strikeout','Strikeout - DP')         THEN 1 ELSE 0 END AS is_k,
        CASE WHEN ab.event IN ('Walk','Intent Walk')                  THEN 1 ELSE 0 END AS is_bb,
        CASE ab.event
            WHEN 'Single'   THEN 1 WHEN 'Double' THEN 2
            WHEN 'Triple'   THEN 3 WHEN 'Home Run' THEN 4
            ELSE 0 END AS total_bases
    FROM mlb.at_bats ab
    JOIN mlb.games g ON g.game_pk = ab.game_pk
    WHERE g.season IN ({seasons_clause})
    AND g.game_type = 'R'
    AND ab.runners_reconstructed = TRUE
    AND ab.event NOT IN (
        'Runner Out','Field Error','Caught Stealing 2B','Caught Stealing 3B',
        'Caught Stealing Home','Pickoff','Wild Pitch','Passed Ball'
    )
),
tagged AS (
    SELECT
        v.batter_id, v.season,
        s.split_type,
        v.is_hit, v.is_hr, v.is_k, v.is_bb, v.total_bases
    FROM valid_abs v
    CROSS JOIN LATERAL (VALUES
        ('tto_1',         v.times_faced = 1),
        ('tto_2',         v.times_faced = 2),
        ('tto_3plus',     v.times_faced >= 3),
        ('vs_lhp',        v.pitch_hand = 'L'),
        ('vs_rhp',        v.pitch_hand = 'R'),
        ('bases_empty',   NOT v.on_first AND NOT v.on_second AND NOT v.on_third),
        ('risp',          v.on_second OR v.on_third),
        ('early_innings', v.inning <= 3),
        ('mid_innings',   v.inning BETWEEN 4 AND 6),
        ('late_innings',  v.inning >= 7),
        ('overall',       TRUE)
    ) AS s(split_type, in_split)
    WHERE s.in_split
)
INSERT INTO mlb.batter_splits
    (batter_id, season, split_type, pa, k_pct, bb_pct, hr_pct,
     avg, obp, slg, ops, updated_at)
SELECT
    batter_id,
    season,
    split_type,
    COUNT(*)                                                                       AS pa,
    ROUND(SUM(is_k)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2)                      AS k_pct,
    ROUND(SUM(is_bb)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2)                     AS bb_pct,
    ROUND(SUM(is_hr)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2)                     AS hr_pct,
    ROUND(SUM(is_hit)::NUMERIC / NULLIF(COUNT(*) - SUM(is_bb), 0), 3)             AS avg,
    ROUND((SUM(is_hit) + SUM(is_bb))::NUMERIC / NULLIF(COUNT(*), 0), 3)           AS obp,
    ROUND(SUM(total_bases)::NUMERIC / NULLIF(COUNT(*) - SUM(is_bb), 0), 3)        AS slg,
    ROUND(
        (SUM(is_hit) + SUM(is_bb))::NUMERIC / NULLIF(COUNT(*), 0) +
        SUM(total_bases)::NUMERIC / NULLIF(COUNT(*) - SUM(is_bb), 0), 3
    )                                                                              AS ops,
    NOW()
FROM tagged
GROUP BY batter_id, season, split_type
HAVING COUNT(*) >= 5
ON CONFLICT (batter_id, season, split_type) DO UPDATE SET
    pa         = EXCLUDED.pa,
    k_pct      = EXCLUDED.k_pct,
    bb_pct     = EXCLUDED.bb_pct,
    hr_pct     = EXCLUDED.hr_pct,
    avg        = EXCLUDED.avg,
    obp        = EXCLUDED.obp,
    slg        = EXCLUDED.slg,
    ops        = EXCLUDED.ops,
    updated_at = EXCLUDED.updated_at
"""


def build_batter_splits(seasons: list, db):
    print(f"\n  Building batter splits for seasons: {seasons}")
    seasons_clause = ','.join(str(s) for s in seasons)
    sql = BATTER_SPLITS_SQL.format(seasons_clause=seasons_clause)
    result = db.execute(text(sql))
    db.commit()
    print(f"  Batter splits: {result.rowcount} rows upserted.")


# ── PLAYER WORKLOAD (ACWR) ────────────────────────────────────────────────────

def build_player_workload(db, calc_date: date = None):
    """
    Build ACWR (acute:chronic workload ratio) for all pitchers and batters.
    Pitcher load = pitches thrown. Batter load = plate appearances.
    ACWR = 7-day avg load / 28-day avg load per game.
    """
    if calc_date is None:
        calc_date = date.today()

    date_7d  = calc_date - timedelta(days=7)
    date_28d = calc_date - timedelta(days=28)

    print(f"\n  Building workload as of {calc_date} (7d: {date_7d}, 28d: {date_28d})")

    # Pitchers: load = pitches thrown per game appearance
    pitcher_sql = text("""
        INSERT INTO mlb.player_workload
            (player_id, player_type, calc_date, load_7d, load_28d, acwr,
             games_7d, games_28d, total_load_7d, updated_at)
        SELECT
            p.pitcher_id                          AS player_id,
            'pitcher'                             AS player_type,
            :calc_date,
            ROUND(
                SUM(CASE WHEN g.game_date >= :date_7d THEN p.game_pitches ELSE 0 END)::NUMERIC
                / NULLIF(SUM(CASE WHEN g.game_date >= :date_7d THEN 1 ELSE 0 END), 0)
            , 2)                                  AS load_7d,
            ROUND(
                SUM(p.game_pitches)::NUMERIC
                / NULLIF(COUNT(DISTINCT p.game_pk), 0)
            , 2)                                  AS load_28d,
            ROUND(
                SUM(CASE WHEN g.game_date >= :date_7d THEN p.game_pitches ELSE 0 END)::NUMERIC
                / NULLIF(SUM(CASE WHEN g.game_date >= :date_7d THEN 1 ELSE 0 END), 0)
                /
                NULLIF(SUM(p.game_pitches)::NUMERIC
                / NULLIF(COUNT(DISTINCT p.game_pk), 0), 0)
            , 3)                                  AS acwr,
            SUM(CASE WHEN g.game_date >= :date_7d THEN 1 ELSE 0 END) AS games_7d,
            COUNT(DISTINCT p.game_pk)             AS games_28d,
            SUM(CASE WHEN g.game_date >= :date_7d THEN p.game_pitches ELSE 0 END) AS total_load_7d,
            NOW()
        FROM (
            SELECT pitcher_id, game_pk, COUNT(*) AS game_pitches
            FROM mlb.pitches
            GROUP BY pitcher_id, game_pk
        ) p
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE g.game_type = 'R'
        AND g.game_date >= :date_28d
        AND g.game_date < :calc_date
        GROUP BY p.pitcher_id
        HAVING COUNT(DISTINCT p.game_pk) >= 1
        ON CONFLICT (player_id, player_type, calc_date) DO UPDATE SET
            load_7d       = EXCLUDED.load_7d,
            load_28d      = EXCLUDED.load_28d,
            acwr          = EXCLUDED.acwr,
            games_7d      = EXCLUDED.games_7d,
            games_28d     = EXCLUDED.games_28d,
            total_load_7d = EXCLUDED.total_load_7d,
            updated_at    = EXCLUDED.updated_at
    """)

    r = db.execute(pitcher_sql, {
        "calc_date": str(calc_date),
        "date_7d":   str(date_7d),
        "date_28d":  str(date_28d),
    })
    print(f"  Pitcher workload: {r.rowcount} rows upserted.")

    # Batters: load = plate appearances per game
    batter_sql = text("""
        INSERT INTO mlb.player_workload
            (player_id, player_type, calc_date, load_7d, load_28d, acwr,
             games_7d, games_28d, total_load_7d, updated_at)
        SELECT
            b.batter_id                           AS player_id,
            'batter'                              AS player_type,
            :calc_date,
            ROUND(
                SUM(CASE WHEN g.game_date >= :date_7d THEN b.game_pa ELSE 0 END)::NUMERIC
                / NULLIF(SUM(CASE WHEN g.game_date >= :date_7d THEN 1 ELSE 0 END), 0)
            , 2)                                  AS load_7d,
            ROUND(
                SUM(b.game_pa)::NUMERIC
                / NULLIF(COUNT(DISTINCT b.game_pk), 0)
            , 2)                                  AS load_28d,
            ROUND(
                SUM(CASE WHEN g.game_date >= :date_7d THEN b.game_pa ELSE 0 END)::NUMERIC
                / NULLIF(SUM(CASE WHEN g.game_date >= :date_7d THEN 1 ELSE 0 END), 0)
                /
                NULLIF(SUM(b.game_pa)::NUMERIC
                / NULLIF(COUNT(DISTINCT b.game_pk), 0), 0)
            , 3)                                  AS acwr,
            SUM(CASE WHEN g.game_date >= :date_7d THEN 1 ELSE 0 END) AS games_7d,
            COUNT(DISTINCT b.game_pk)             AS games_28d,
            SUM(CASE WHEN g.game_date >= :date_7d THEN b.game_pa ELSE 0 END) AS total_load_7d,
            NOW()
        FROM (
            SELECT ab.batter_id, ab.game_pk,
                   COUNT(*) AS game_pa
            FROM mlb.at_bats ab
            JOIN mlb.games g2 ON g2.game_pk = ab.game_pk
            WHERE g2.game_type = 'R'
            AND ab.event NOT IN (
                'Runner Out','Caught Stealing 2B','Caught Stealing 3B',
                'Caught Stealing Home','Pickoff','Wild Pitch','Passed Ball'
            )
            GROUP BY ab.batter_id, ab.game_pk
        ) b
        JOIN mlb.games g ON g.game_pk = b.game_pk
        WHERE g.game_type = 'R'
        AND g.game_date >= :date_28d
        AND g.game_date < :calc_date
        GROUP BY b.batter_id
        HAVING COUNT(DISTINCT b.game_pk) >= 1
        ON CONFLICT (player_id, player_type, calc_date) DO UPDATE SET
            load_7d       = EXCLUDED.load_7d,
            load_28d      = EXCLUDED.load_28d,
            acwr          = EXCLUDED.acwr,
            games_7d      = EXCLUDED.games_7d,
            games_28d     = EXCLUDED.games_28d,
            total_load_7d = EXCLUDED.total_load_7d,
            updated_at    = EXCLUDED.updated_at
    """)

    r = db.execute(batter_sql, {
        "calc_date": str(calc_date),
        "date_7d":   str(date_7d),
        "date_28d":  str(date_28d),
    })
    db.commit()
    print(f"  Batter workload: {r.rowcount} rows upserted.")


# ── MATCHUP HISTORY ───────────────────────────────────────────────────────────
# Regression constants: pitcher-favored — small samples regress K% up, HR% down
LG_K_PCT  = 22.0   # league average K% (percentage scale)
LG_HR_PCT = 3.5    # league average HR%
H2H_K_REG = 20     # PA regression constant

MATCHUP_HISTORY_SQL = """
WITH valid_abs AS (
    SELECT
        ab.pitcher_id,
        ab.batter_id,
        g.season,
        CASE WHEN ab.event IN ('Single','Double','Triple','Home Run') THEN 1 ELSE 0 END AS is_hit,
        CASE WHEN ab.event = 'Home Run'                              THEN 1 ELSE 0 END AS is_hr,
        CASE WHEN ab.event IN ('Strikeout','Strikeout - DP')         THEN 1 ELSE 0 END AS is_k,
        CASE WHEN ab.event IN ('Walk','Intent Walk')                  THEN 1 ELSE 0 END AS is_bb,
        CASE ab.event
            WHEN 'Single'   THEN 1 WHEN 'Double' THEN 2
            WHEN 'Triple'   THEN 3 WHEN 'Home Run' THEN 4
            ELSE 0 END AS total_bases
    FROM mlb.at_bats ab
    JOIN mlb.games g ON g.game_pk = ab.game_pk
    WHERE g.season IN ({seasons_clause})
    AND g.game_type = 'R'
    AND ab.event NOT IN (
        'Runner Out','Field Error','Caught Stealing 2B','Caught Stealing 3B',
        'Caught Stealing Home','Pickoff','Wild Pitch','Passed Ball'
    )
)
INSERT INTO mlb.matchup_history
    (pitcher_id, batter_id, season, pa, k, bb, hr, hits, total_bases,
     k_pct_raw, hr_pct_raw, k_pct_reg, hr_pct_reg, sample_weight, updated_at)
SELECT
    pitcher_id,
    batter_id,
    season,
    COUNT(*)    AS pa,
    SUM(is_k)   AS k,
    SUM(is_bb)  AS bb,
    SUM(is_hr)  AS hr,
    SUM(is_hit) AS hits,
    SUM(total_bases) AS total_bases,
    ROUND(SUM(is_k)::NUMERIC  / NULLIF(COUNT(*), 0) * 100, 2) AS k_pct_raw,
    ROUND(SUM(is_hr)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS hr_pct_raw,
    -- Regress toward league average (pitcher-favored: K% up if below LG, HR% down if above LG)
    ROUND(
        SUM(is_k)::NUMERIC / NULLIF(COUNT(*), 0) * 100
            * (COUNT(*)::NUMERIC / (COUNT(*) + {k_reg}))
        + {lg_k} * ({k_reg}::NUMERIC / (COUNT(*) + {k_reg}))
    , 2) AS k_pct_reg,
    ROUND(
        SUM(is_hr)::NUMERIC / NULLIF(COUNT(*), 0) * 100
            * (COUNT(*)::NUMERIC / (COUNT(*) + {k_reg}))
        + {lg_hr} * ({k_reg}::NUMERIC / (COUNT(*) + {k_reg}))
    , 2) AS hr_pct_reg,
    ROUND(COUNT(*)::NUMERIC / (COUNT(*) + {k_reg}), 3) AS sample_weight,
    NOW()
FROM valid_abs
GROUP BY pitcher_id, batter_id, season
HAVING COUNT(*) >= 2
ON CONFLICT (pitcher_id, batter_id, season) DO UPDATE SET
    pa            = EXCLUDED.pa,
    k             = EXCLUDED.k,
    bb            = EXCLUDED.bb,
    hr            = EXCLUDED.hr,
    hits          = EXCLUDED.hits,
    total_bases   = EXCLUDED.total_bases,
    k_pct_raw     = EXCLUDED.k_pct_raw,
    hr_pct_raw    = EXCLUDED.hr_pct_raw,
    k_pct_reg     = EXCLUDED.k_pct_reg,
    hr_pct_reg    = EXCLUDED.hr_pct_reg,
    sample_weight = EXCLUDED.sample_weight,
    updated_at    = EXCLUDED.updated_at
"""


def build_matchup_history(seasons: list, db):
    print(f"\n  Building matchup history for seasons: {seasons}")
    seasons_clause = ','.join(str(s) for s in seasons)
    sql = MATCHUP_HISTORY_SQL.format(
        seasons_clause=seasons_clause,
        k_reg=H2H_K_REG,
        lg_k=LG_K_PCT,
        lg_hr=LG_HR_PCT,
    )
    result = db.execute(text(sql))
    db.commit()
    print(f"  Matchup history: {result.rowcount} rows upserted.")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Build contextual splits tables")
    parser.add_argument("--seasons", nargs="+", type=int, default=[2024, 2025, 2026])
    parser.add_argument("--workload-only", action="store_true",
                        help="Only rebuild player workload (ACWR)")
    parser.add_argument("--matchup-only", action="store_true",
                        help="Only rebuild matchup history")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        print(f"\n=== Context Splits Build ===")
        create_tables(db)

        if args.workload_only:
            build_player_workload(db)
            print("\n=== Workload build complete ===")
            return

        if args.matchup_only:
            build_matchup_history(args.seasons, db)
            print("\n=== Matchup history build complete ===")
            return

        build_pitcher_splits(args.seasons, db)
        build_batter_splits(args.seasons, db)
        build_player_workload(db)
        build_matchup_history(args.seasons, db)
        print("\n=== Context Splits build complete ===")
    finally:
        db.close()


if __name__ == "__main__":
    main()
