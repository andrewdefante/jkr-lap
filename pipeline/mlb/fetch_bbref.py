"""
Fetch Baseball Reference WAR data directly from BBref's public CSV files.

Stores OPS+ and PA in mlb.bbref_batting (one row per player per season).
Multi-team players are aggregated with PA-weighted OPS+.
Also updates player_id_map.bbref_id from the Chadwick register CSV.

Usage:
    PYTHONPATH=/app:/pipeline python3 /pipeline/mlb/fetch_bbref.py --season 2026
    PYTHONPATH=/app:/pipeline python3 /pipeline/mlb/fetch_bbref.py --season 2025
"""

import sys
import os
import argparse
import io
import httpx
import pandas as pd
from sqlalchemy import text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'api'))
from database import SessionLocal


BBREF_BAT_URL = "https://www.baseball-reference.com/data/war_daily_bat.txt"
BBREF_PITCH_URL = "https://www.baseball-reference.com/data/war_daily_pitch.txt"
CHADWICK_BASE = "https://raw.githubusercontent.com/chadwickbureau/register/master/data"
CHADWICK_SHARDS = [f"people-{c}.csv" for c in "0123456789abcdef"]


def _safe_int(val):
    try:
        return int(val) if pd.notna(val) else None
    except (ValueError, TypeError):
        return None


def _safe_float(val):
    try:
        return float(val) if pd.notna(val) else None
    except (ValueError, TypeError):
        return None


def fetch_bbref_batting(season: int, db) -> int:
    print(f"\n=== Fetching BBref Batting (direct) — {season} ===")

    try:
        r = httpx.get(BBREF_BAT_URL, timeout=60,
                      headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        print(f"  Downloaded {len(r.content):,} bytes")
    except Exception as e:
        print(f"  ERROR: {e}")
        return 0

    df = pd.read_csv(io.StringIO(r.text), low_memory=False)
    print(f"  Loaded {len(df):,} total rows, columns: {list(df.columns[:8])}")

    df = df[df['year_ID'] == season].copy()
    print(f"  Filtered to {len(df):,} rows for {season}")
    if df.empty:
        return 0

    df['mlb_ID'] = pd.to_numeric(df['mlb_ID'], errors='coerce')
    df = df[df['mlb_ID'].notna() & (df['mlb_ID'] > 0)].copy()
    df['PA'] = pd.to_numeric(df['PA'], errors='coerce').fillna(0)

    # Aggregate by player+year — handles multi-team stints
    agg_rows = []
    for (mlb_id, year_id, player_id), group in df.groupby(['mlb_ID', 'year_ID', 'player_ID']):
        total_pa = group['PA'].sum()

        ops_plus = None
        if total_pa > 0 and 'OPS_plus' in group.columns:
            valid = group[group['OPS_plus'].notna() & (group['OPS_plus'] != 'NULL')].copy()
            valid['OPS_plus'] = pd.to_numeric(valid['OPS_plus'], errors='coerce')
            valid = valid[valid['OPS_plus'].notna()]
            valid_pa = valid['PA'].sum()
            if valid_pa > 0:
                ops_plus = (valid['OPS_plus'] * valid['PA']).sum() / valid_pa

        n_teams = group['team_ID'].nunique()
        team = f"{n_teams}TM" if n_teams > 1 else str(group['team_ID'].iloc[-1])

        agg_rows.append({
            'mlb_id': int(mlb_id),
            'player_id': str(player_id),
            'name': group['name_common'].iloc[-1],
            'year_id': int(year_id),
            'team': team,
            'lg_id': group['lg_ID'].iloc[-1],
            'age': _safe_int(group['age'].iloc[0]),
            'pa': int(total_pa),
            'g': int(pd.to_numeric(group['G'], errors='coerce').fillna(0).sum()),
            'war': _safe_float(pd.to_numeric(group['WAR'], errors='coerce').sum())
                   if 'WAR' in group else None,
            'ops_plus': round(ops_plus, 1) if ops_plus is not None else None,
        })

    agg = pd.DataFrame(agg_rows)
    print(f"  Aggregated to {len(agg)} unique players")

    upsert_sql = text("""
        INSERT INTO mlb.bbref_batting (
            mlb_id, name, year_id, team, age, pa, g, ops_plus, updated_at
        ) VALUES (
            :mlb_id, :name, :year_id, :team, :age, :pa, :g, :ops_plus, NOW()
        )
        ON CONFLICT (mlb_id, year_id) DO UPDATE SET
            name       = EXCLUDED.name,
            team       = EXCLUDED.team,
            age        = EXCLUDED.age,
            pa         = EXCLUDED.pa,
            g          = EXCLUDED.g,
            ops_plus   = EXCLUDED.ops_plus,
            updated_at = NOW()
    """)

    stored = 0
    for _, row in agg.iterrows():
        ops_val = row['ops_plus']
        if ops_val is not None:
            try:
                ops_val = float(ops_val)
                if ops_val != ops_val:  # NaN check
                    ops_val = None
            except (TypeError, ValueError):
                ops_val = None
        try:
            db.execute(upsert_sql, {
                "mlb_id":   int(row['mlb_id']),
                "name":     str(row['name']),
                "year_id":  int(row['year_id']),
                "team":     str(row['team']),
                "age":      row['age'],
                "pa":       int(row['pa']),
                "g":        int(row['g']),
                "ops_plus": ops_val,
            })
            stored += 1
        except Exception as e:
            db.rollback()
            print(f"  Error {row.get('name')}: {e}")

    db.commit()
    print(f"  Stored/updated {stored} rows")
    return stored


def fetch_bbref_pitching(season: int, db) -> int:
    print(f"\n=== Fetching BBref Pitching — {season} ===")

    try:
        r = httpx.get(BBREF_PITCH_URL, timeout=60,
                      headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        print(f"  Downloaded {len(r.content):,} bytes")
    except Exception as e:
        print(f"  ERROR: {e}")
        return 0

    df = pd.read_csv(io.StringIO(r.text), low_memory=False)
    print(f"  Loaded {len(df):,} total rows, columns: {list(df.columns[:10])}")

    df = df[df['year_ID'] == season].copy()
    print(f"  Filtered to {len(df):,} rows for {season}")
    if df.empty:
        return 0

    df['mlb_ID'] = pd.to_numeric(df['mlb_ID'], errors='coerce')
    df = df[df['mlb_ID'].notna() & (df['mlb_ID'] > 0)].copy()

    agg_rows = []
    for (mlb_id, year_id, player_id), group in df.groupby(['mlb_ID', 'year_ID', 'player_ID']):
        total_ip_outs = pd.to_numeric(group['IPouts'], errors='coerce').fillna(0).sum()

        era_plus = None
        if total_ip_outs > 0 and 'ERA_plus' in group.columns:
            valid = group.copy()
            valid['ERA_plus'] = pd.to_numeric(valid['ERA_plus'], errors='coerce')
            valid['IPouts_n'] = pd.to_numeric(valid['IPouts'], errors='coerce').fillna(0)
            valid = valid[valid['ERA_plus'].notna() & (valid['IPouts_n'] > 0)]
            if not valid.empty:
                era_plus = (valid['ERA_plus'] * valid['IPouts_n']).sum() / valid['IPouts_n'].sum()

        n_teams = group['team_ID'].nunique()
        team = f"{n_teams}TM" if n_teams > 1 else str(group['team_ID'].iloc[-1])

        agg_rows.append({
            'mlb_id':        int(mlb_id),
            'player_id':     str(player_id),
            'name':          group['name_common'].iloc[-1],
            'year_id':       int(year_id),
            'team_id':       team,
            'lg_id':         str(group['lg_ID'].iloc[-1]),
            'age':           _safe_int(group['age'].iloc[0]),
            'g':             _safe_int(pd.to_numeric(group['G'], errors='coerce').fillna(0).sum()),
            'gs':            _safe_int(pd.to_numeric(group['GS'], errors='coerce').fillna(0).sum()),
            'ip_outs':       int(total_ip_outs),
            'war':           _safe_float(pd.to_numeric(group['WAR'], errors='coerce').sum()) if 'WAR' in group.columns else None,
            'era_plus':      round(float(era_plus), 1) if era_plus is not None and era_plus == era_plus else None,
            'runs_above_avg': _safe_float(pd.to_numeric(group['runs_above_avg'], errors='coerce').sum()) if 'runs_above_avg' in group.columns else None,
        })

    agg = pd.DataFrame(agg_rows)
    print(f"  Aggregated to {len(agg)} unique pitchers")

    upsert_sql = text("""
        INSERT INTO mlb.bbref_pitching (
            mlb_id, player_id, name, year_id, team_id, lg_id, age,
            g, gs, ip_outs, war, era_plus, runs_above_avg, updated_at
        ) VALUES (
            :mlb_id, :player_id, :name, :year_id, :team_id, :lg_id, :age,
            :g, :gs, :ip_outs, :war, :era_plus, :runs_above_avg, NOW()
        )
        ON CONFLICT (mlb_id, year_id, team_id) DO UPDATE SET
            name            = EXCLUDED.name,
            g               = EXCLUDED.g,
            gs              = EXCLUDED.gs,
            ip_outs         = EXCLUDED.ip_outs,
            war             = EXCLUDED.war,
            era_plus        = EXCLUDED.era_plus,
            runs_above_avg  = EXCLUDED.runs_above_avg,
            updated_at      = NOW()
    """)

    stored = 0
    for _, row in agg.iterrows():
        try:
            db.execute(upsert_sql, {
                "mlb_id":         int(row['mlb_id']),
                "player_id":      str(row['player_id']),
                "name":           str(row['name']),
                "year_id":        int(row['year_id']),
                "team_id":        str(row['team_id']),
                "lg_id":          str(row['lg_id']),
                "age":            row['age'],
                "g":              row['g'],
                "gs":             row['gs'],
                "ip_outs":        row['ip_outs'],
                "war":            row['war'],
                "era_plus":       row['era_plus'],
                "runs_above_avg": row['runs_above_avg'],
            })
            stored += 1
        except Exception as e:
            db.rollback()
            print(f"  Error {row.get('name')}: {e}")

    db.commit()
    print(f"  Stored/updated {stored} pitching rows for {season}")
    return stored


def update_bbref_ids(db):
    """Update player_id_map.bbref_id from the Chadwick register."""
    missing_count_sql = text("""
        SELECT COUNT(*) FROM mlb.player_id_map
        WHERE mlbam_id IS NOT NULL AND (bbref_id IS NULL OR bbref_id = '')
    """)
    missing = db.execute(missing_count_sql).scalar()
    if missing == 0:
        print("  player_id_map.bbref_id already fully populated")
        return

    print(f"  {missing} rows missing bbref_id — fetching Chadwick register ({len(CHADWICK_SHARDS)} shards)...")
    try:
        frames = []
        for shard in CHADWICK_SHARDS:
            r = httpx.get(f"{CHADWICK_BASE}/{shard}", timeout=60)
            r.raise_for_status()
            frames.append(pd.read_csv(io.StringIO(r.text), low_memory=False))
        chad = pd.concat(frames, ignore_index=True)
        print(f"  Loaded {len(chad):,} total Chadwick rows")
        chad = chad[chad['key_mlbam'].notna() & chad['key_bbref'].notna()].copy()
        chad['key_mlbam'] = pd.to_numeric(chad['key_mlbam'], errors='coerce')
        chad = chad[chad['key_mlbam'] > 0]

        update_sql = text("""
            UPDATE mlb.player_id_map SET bbref_id = :bbref_id, updated_at = NOW()
            WHERE mlbam_id = :mlbam_id
            AND (bbref_id IS NULL OR bbref_id = '')
        """)

        updated = 0
        for _, row in chad.iterrows():
            result = db.execute(update_sql, {
                "mlbam_id": int(row['key_mlbam']),
                "bbref_id": str(row['key_bbref']),
            })
            if result.rowcount > 0:
                updated += 1

        db.commit()
        print(f"  Updated {updated} bbref_ids in player_id_map")
    except Exception as e:
        print(f"  ERROR: {e}")


def main():
    parser = argparse.ArgumentParser(description="Fetch BBref WAR data (batting + pitching)")
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--batting-only", action="store_true")
    parser.add_argument("--pitching-only", action="store_true")
    parser.add_argument("--skip-bbref-ids", action="store_true",
                        help="Skip updating player_id_map.bbref_id")
    args = parser.parse_args()

    print(f"\n{'='*50}")
    print(f"  BBref Fetch — {args.season}")
    print(f"{'='*50}\n")

    db = SessionLocal()
    try:
        if not args.pitching_only:
            n_bat = fetch_bbref_batting(args.season, db)
            if not args.skip_bbref_ids:
                update_bbref_ids(db)
            print(f"\nBatting: {n_bat} rows stored for {args.season}")
        if not args.batting_only:
            n_pitch = fetch_bbref_pitching(args.season, db)
            print(f"Pitching: {n_pitch} rows stored for {args.season}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
