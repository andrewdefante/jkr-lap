"""
Fetch Statcast bat tracking data from Baseball Savant.
Populates mlb.bat_tracking with bat_speed, swing_length, squared_up_pct, etc.
Available from 2023 second half onward.
"""
import sys
sys.path.insert(0, '/app')

import httpx
import pandas as pd
import io
import argparse
from database import SessionLocal
from sqlalchemy import text

SAVANT_BAT_TRACKING_URL = "https://baseballsavant.mlb.com/leaderboard/bat-tracking"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}


def fetch_bat_tracking_leaderboard(season: int, player_type: str = "batter") -> pd.DataFrame:
    params = {
        "gameType": "Regular",
        "year": season,
        "type": player_type,
        "csv": "true",
    }
    print(f"  Fetching bat tracking [{player_type}] for {season}...")
    try:
        r = httpx.get(SAVANT_BAT_TRACKING_URL, params=params,
                      headers=HEADERS, timeout=30)
        r.raise_for_status()
        if len(r.text) < 100:
            print(f"  Empty response")
            return pd.DataFrame()
        df = pd.read_csv(io.StringIO(r.text))
        print(f"  Got {len(df)} rows | columns: {list(df.columns)}")
        return df
    except Exception as e:
        print(f"  ERROR: {e}")
        return pd.DataFrame()


def create_bat_tracking_table(db):
    db.execute(text("""
        CREATE TABLE IF NOT EXISTS mlb.bat_tracking (
            id BIGSERIAL PRIMARY KEY,
            mlbam_id INTEGER NOT NULL,
            player_name VARCHAR(100),
            season INTEGER NOT NULL,
            player_type VARCHAR(10) NOT NULL,
            pa INTEGER,
            bat_speed FLOAT,
            swing_length FLOAT,
            squared_up_pct FLOAT,
            blast_rate FLOAT,
            attack_angle FLOAT,
            swing_path_tilt FLOAT,
            attack_direction FLOAT,
            intercept_point_contact FLOAT,
            intercept_point_percent FLOAT,
            swords_pct FLOAT,
            fast_swing_rate FLOAT,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(mlbam_id, season, player_type)
        );
        CREATE INDEX IF NOT EXISTS ix_bat_tracking_mlbam
            ON mlb.bat_tracking(mlbam_id, season);
    """))
    db.commit()
    print("  Table ready")


def store_bat_tracking(df: pd.DataFrame, season: int, player_type: str, db) -> int:
    if df.empty:
        return 0

    upsert = text("""
        INSERT INTO mlb.bat_tracking (
            mlbam_id, player_name, season, player_type,
            pa, bat_speed, swing_length, squared_up_pct, blast_rate,
            attack_angle, swing_path_tilt, attack_direction,
            intercept_point_contact, intercept_point_percent,
            swords_pct, fast_swing_rate, updated_at
        ) VALUES (
            :mlbam_id, :player_name, :season, :player_type,
            :pa, :bat_speed, :swing_length, :squared_up_pct, :blast_rate,
            :attack_angle, :swing_path_tilt, :attack_direction,
            :intercept_point_contact, :intercept_point_percent,
            :swords_pct, :fast_swing_rate, NOW()
        )
        ON CONFLICT (mlbam_id, season, player_type) DO UPDATE SET
            pa = EXCLUDED.pa,
            bat_speed = EXCLUDED.bat_speed,
            swing_length = EXCLUDED.swing_length,
            squared_up_pct = EXCLUDED.squared_up_pct,
            blast_rate = EXCLUDED.blast_rate,
            attack_angle = EXCLUDED.attack_angle,
            intercept_point_contact = EXCLUDED.intercept_point_contact,
            swords_pct = EXCLUDED.swords_pct,
            fast_swing_rate = EXCLUDED.fast_swing_rate,
            updated_at = NOW()
    """)

    def sf(val):
        try:
            f = float(val)
            return f if pd.notna(f) else None
        except Exception:
            return None

    def si(val):
        try:
            f = float(val)
            return int(f) if pd.notna(f) else None
        except Exception:
            return None

    cols = list(df.columns)
    stored = 0
    for _, row in df.iterrows():
        # Resolve MLBAM id — Savant uses various column names
        mlbam_id = None
        for c in ['player_id', 'mlbam_id', 'batter', 'pitcher', 'id']:
            if c in cols:
                try:
                    v = int(float(row[c]))
                    mlbam_id = v
                    break
                except Exception:
                    continue
        if not mlbam_id:
            continue

        name = str(row.get('player_name', row.get('name', row.get('last_name, first_name', ''))))[:100]

        # Savant actual column names (as of 2025-2026):
        # avg_bat_speed, swing_length, squared_up_per_bat_contact,
        # blast_per_bat_contact, hard_swing_rate, swords (count),
        # swings_competitive (total swings)
        pa_val = si(row.get('swings_competitive', row.get('pa', row.get('attempts'))))
        swords_count = sf(row.get('swords'))
        swords_pct = None
        if swords_count is not None and pa_val and pa_val > 0:
            swords_pct = round(swords_count / pa_val * 100, 2)

        db.execute(upsert, {
            "mlbam_id":    mlbam_id,
            "player_name": name,
            "season":      season,
            "player_type": player_type,
            "pa":                       pa_val,
            "bat_speed":                sf(row.get('avg_bat_speed', row.get('bat_speed'))),
            "swing_length":             sf(row.get('swing_length')),
            "squared_up_pct":           sf(row.get('squared_up_per_bat_contact', row.get('squared_up_percent', row.get('squared_up_pct')))),
            "blast_rate":               sf(row.get('blast_per_bat_contact', row.get('blasts_percent', row.get('blast_rate')))),
            "attack_angle":             sf(row.get('attack_angle')),
            "swing_path_tilt":          sf(row.get('swing_path_tilt')),
            "attack_direction":         sf(row.get('attack_direction')),
            "intercept_point_contact":  sf(row.get('intercept_point_contact')),
            "intercept_point_percent":  sf(row.get('intercept_point_percent')),
            "swords_pct":               swords_pct,
            "fast_swing_rate":          sf(row.get('hard_swing_rate', row.get('fast_swing_rate', row.get('fast_swing_percent')))),
        })
        stored += 1

    db.commit()
    return stored


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, nargs='+', default=[2025, 2026])
    args = parser.parse_args()

    db = SessionLocal()
    try:
        create_bat_tracking_table(db)

        for season in args.season:
            print(f"\n=== Bat Tracking — {season} ===")
            for ptype in ['batter', 'pitcher']:
                df = fetch_bat_tracking_leaderboard(season, ptype)
                if not df.empty:
                    n = store_bat_tracking(df, season, ptype, db)
                    print(f"  Stored {n} {ptype} rows")

        result = db.execute(text("""
            SELECT season, player_type, COUNT(*) as n,
                   ROUND(AVG(bat_speed)::numeric, 1) as avg_bat_speed,
                   ROUND(AVG(swing_length)::numeric, 2) as avg_swing_length
            FROM mlb.bat_tracking
            GROUP BY season, player_type
            ORDER BY season, player_type
        """)).mappings().all()

        print("\n=== Bat Tracking Summary ===")
        for r in result:
            print(f"  {r['season']} {r['player_type']}: "
                  f"{r['n']} players | "
                  f"avg bat speed: {r['avg_bat_speed']} mph | "
                  f"avg swing length: {r['avg_swing_length']} ft")
    finally:
        db.close()
