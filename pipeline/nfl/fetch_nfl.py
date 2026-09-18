"""
Fetch NFL data from nflverse-data public GitHub releases.
Populates nfl.games, nfl.plays, nfl.player_stats_weekly, nfl.snap_counts,
nfl.players, and nfl.team_metrics_weekly (derived from nfl.plays).

Data source: https://github.com/nflverse/nflverse-data/releases
No API key required — public release assets (parquet).

Usage:
    PYTHONPATH=/app:/pipeline python3 /pipeline/nfl/fetch_nfl.py --season 2025
    PYTHONPATH=/app:/pipeline python3 /pipeline/nfl/fetch_nfl.py --season 2026
    PYTHONPATH=/app:/pipeline python3 /pipeline/nfl/fetch_nfl.py --season 2026 --week 3
"""

import sys
import os
import io
import argparse
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'api'))

import httpx
import pandas as pd
from database import SessionLocal
from sqlalchemy import text

BASE_URL = "https://github.com/nflverse/nflverse-data/releases/download"

URLS = {
    'pbp': f"{BASE_URL}/pbp/play_by_play_{{season}}.parquet",
    'stats_player_week': f"{BASE_URL}/stats_player/stats_player_week_{{season}}.parquet",
    'snap_counts': f"{BASE_URL}/snap_counts/snap_counts_{{season}}.parquet",
    'games': f"{BASE_URL}/schedules/games.parquet",   # all seasons, filter locally
    'players': f"{BASE_URL}/players/players.parquet",  # all-time bios, filter not needed
}

ET = ZoneInfo("America/New_York")


def _download_parquet(url: str) -> pd.DataFrame:
    r = httpx.get(url, timeout=120, headers={"User-Agent": "Mozilla/5.0"},
                  follow_redirects=True)
    r.raise_for_status()
    return pd.read_parquet(io.BytesIO(r.content))


def _safe_int(v):
    try:
        return int(v) if pd.notna(v) else None
    except (ValueError, TypeError):
        return None


def _safe_float(v):
    try:
        return float(v) if pd.notna(v) else None
    except (ValueError, TypeError):
        return None


def _safe_bool(v):
    if pd.isna(v):
        return None
    try:
        return bool(v)
    except (ValueError, TypeError):
        return None


def _safe_str(v):
    return str(v) if pd.notna(v) else None


def fetch_games(season: int, db) -> int:
    """Fetch schedule/game data for a season from the combined 'games' release."""
    print(f"  Fetching {season} schedule...")
    df = _download_parquet(URLS['games'])
    df = df[df['season'] == season].copy()

    inserted = 0
    for _, row in df.iterrows():
        game_datetime = None
        gameday, gametime = row.get('gameday'), row.get('gametime')
        if pd.notna(gameday) and pd.notna(gametime):
            try:
                naive = datetime.strptime(f"{gameday} {gametime}", "%Y-%m-%d %H:%M")
                game_datetime = naive.replace(tzinfo=ET)
            except ValueError:
                game_datetime = None

        home_score = _safe_int(row.get('home_score'))
        away_score = _safe_int(row.get('away_score'))
        home_win = (home_score > away_score) if (home_score is not None and away_score is not None) else None

        try:
            db.execute(text("""
                INSERT INTO nfl.games (
                    game_id, season, game_type, week, game_date, game_datetime,
                    home_team, away_team, home_score, away_score, home_win,
                    overtime, roof, surface, temp, wind, stadium, location,
                    spread_line, total_line, div_game, updated_at
                ) VALUES (
                    :game_id, :season, :game_type, :week, :game_date, :game_datetime,
                    :home_team, :away_team, :home_score, :away_score, :home_win,
                    :overtime, :roof, :surface, :temp, :wind, :stadium, :location,
                    :spread_line, :total_line, :div_game, NOW()
                )
                ON CONFLICT (game_id) DO UPDATE SET
                    home_score = EXCLUDED.home_score,
                    away_score = EXCLUDED.away_score,
                    home_win = EXCLUDED.home_win,
                    updated_at = NOW()
            """), {
                'game_id': _safe_str(row.get('game_id')),
                'season': int(season),
                'game_type': _safe_str(row.get('game_type')),
                'week': _safe_int(row.get('week')),
                'game_date': _safe_str(gameday),
                'game_datetime': game_datetime,
                'home_team': _safe_str(row.get('home_team')),
                'away_team': _safe_str(row.get('away_team')),
                'home_score': home_score,
                'away_score': away_score,
                'home_win': home_win,
                'overtime': _safe_bool(row.get('overtime')),
                'roof': _safe_str(row.get('roof')),
                'surface': _safe_str(row.get('surface')),
                'temp': _safe_int(row.get('temp')),
                'wind': _safe_int(row.get('wind')),
                'stadium': _safe_str(row.get('stadium')),
                'location': _safe_str(row.get('location')),
                'spread_line': _safe_float(row.get('spread_line')),
                'total_line': _safe_float(row.get('total_line')),
                'div_game': _safe_bool(row.get('div_game')),
            })
            inserted += 1
        except Exception as e:
            print(f"    Game error {row.get('game_id')}: {e}")

    db.commit()
    print(f"  Games: {inserted} upserted")
    return inserted


PLAY_TYPES = ('pass', 'run', 'punt', 'kickoff', 'field_goal',
              'extra_point', 'no_play', 'qb_kneel', 'qb_spike')

PLAY_INSERT_SQL = text("""
    INSERT INTO nfl.plays (
        game_id, season, week, play_index, game_seconds_remaining,
        quarter_seconds_remaining, quarter, down, ydstogo, yardline_100,
        play_type, yards_gained, touchdown, first_down, epa, wp, wpa,
        passer_id, passer_name, rusher_id, rusher_name,
        receiver_id, receiver_name, pass_length, pass_location,
        air_yards, yards_after_catch, complete_pass, interception,
        run_location, run_gap, posteam, defteam, posteam_score,
        defteam_score, score_differential, penalty, penalty_team, penalty_yards
    ) VALUES (
        :game_id, :season, :week, :play_index, :game_seconds_remaining,
        :quarter_seconds_remaining, :quarter, :down, :ydstogo, :yardline_100,
        :play_type, :yards_gained, :touchdown, :first_down, :epa, :wp, :wpa,
        :passer_id, :passer_name, :rusher_id, :rusher_name,
        :receiver_id, :receiver_name, :pass_length, :pass_location,
        :air_yards, :yards_after_catch, :complete_pass, :interception,
        :run_location, :run_gap, :posteam, :defteam, :posteam_score,
        :defteam_score, :score_differential, :penalty, :penalty_team, :penalty_yards
    )
""")


def fetch_plays(season: int, week: int = None, db=None) -> int:
    """Fetch play-by-play data for a season (nflfastR)."""
    print(f"  Fetching {season} play-by-play...")
    url = URLS['pbp'].format(season=season)
    df = _download_parquet(url)

    if week is not None:
        df = df[df['week'] == week]
    df = df[df['play_type'].isin(PLAY_TYPES)]
    print(f"  {len(df):,} plays to insert...")

    if week:
        db.execute(text("DELETE FROM nfl.plays WHERE season = :s AND week = :w"),
                   {"s": season, "w": week})
    else:
        db.execute(text("DELETE FROM nfl.plays WHERE season = :s"), {"s": season})
    db.commit()

    BATCH = 1000
    inserted = 0
    for i in range(0, len(df), BATCH):
        batch = df.iloc[i:i + BATCH]
        rows = []
        for _, r in batch.iterrows():
            rows.append({
                'game_id': _safe_str(r.get('game_id')),
                'season': _safe_int(r.get('season')),
                'week': _safe_int(r.get('week')),
                'play_index': _safe_int(r.get('play_id')),
                'game_seconds_remaining': _safe_int(r.get('game_seconds_remaining')),
                'quarter_seconds_remaining': _safe_int(r.get('quarter_seconds_remaining')),
                'quarter': _safe_int(r.get('qtr')),
                'down': _safe_int(r.get('down')),
                'ydstogo': _safe_int(r.get('ydstogo')),
                'yardline_100': _safe_int(r.get('yardline_100')),
                'play_type': _safe_str(r.get('play_type')),
                'yards_gained': _safe_int(r.get('yards_gained')),
                'touchdown': _safe_bool(r.get('touchdown')),
                'first_down': _safe_bool(r.get('first_down')),
                'epa': _safe_float(r.get('epa')),
                'wp': _safe_float(r.get('wp')),
                'wpa': _safe_float(r.get('wpa')),
                'passer_id': _safe_str(r.get('passer_id')),
                'passer_name': _safe_str(r.get('passer_player_name')),
                'rusher_id': _safe_str(r.get('rusher_id')),
                'rusher_name': _safe_str(r.get('rusher_player_name')),
                'receiver_id': _safe_str(r.get('receiver_id')),
                'receiver_name': _safe_str(r.get('receiver_player_name')),
                'pass_length': _safe_str(r.get('pass_length')),
                'pass_location': _safe_str(r.get('pass_location')),
                'air_yards': _safe_int(r.get('air_yards')),
                'yards_after_catch': _safe_int(r.get('yards_after_catch')),
                'complete_pass': _safe_bool(r.get('complete_pass')),
                'interception': _safe_bool(r.get('interception')),
                'run_location': _safe_str(r.get('run_location')),
                'run_gap': _safe_str(r.get('run_gap')),
                'posteam': _safe_str(r.get('posteam')),
                'defteam': _safe_str(r.get('defteam')),
                'posteam_score': _safe_int(r.get('posteam_score')),
                'defteam_score': _safe_int(r.get('defteam_score')),
                'score_differential': _safe_int(r.get('score_differential')),
                'penalty': _safe_bool(r.get('penalty')),
                'penalty_team': _safe_str(r.get('penalty_team')),
                'penalty_yards': _safe_int(r.get('penalty_yards')),
            })

        db.execute(PLAY_INSERT_SQL, rows)
        db.commit()
        inserted += len(rows)
        if (i // BATCH + 1) % 10 == 0:
            print(f"    [{inserted:,}/{len(df):,}] plays inserted...")

    print(f"  Plays: {inserted:,} inserted")
    return inserted


def fetch_player_stats(season: int, week: int = None, db=None) -> int:
    """Fetch weekly player stats (nflverse 'stats_player' release)."""
    print(f"  Fetching {season} player stats...")
    url = URLS['stats_player_week'].format(season=season)
    df = _download_parquet(url)
    df = df[df['player_id'].notna()]  # drop team-total placeholder rows

    if week is not None:
        df = df[df['week'] == week]

    if week:
        db.execute(text("DELETE FROM nfl.player_stats_weekly WHERE season = :s AND week = :w"),
                   {"s": season, "w": week})
    else:
        db.execute(text("DELETE FROM nfl.player_stats_weekly WHERE season = :s"), {"s": season})
    db.commit()

    upsert_sql = text("""
        INSERT INTO nfl.player_stats_weekly (
            player_id, player_name, player_display_name, position, position_group,
            team, season, week, game_id, completions, attempts, passing_yards,
            passing_tds, interceptions, sacks, sack_yards, carries, rushing_yards,
            rushing_tds, targets, receptions, receiving_yards, receiving_tds,
            fantasy_points, fantasy_points_ppr, air_yards_share, target_share,
            wopr, racr
        ) VALUES (
            :player_id, :player_name, :player_display_name, :position, :position_group,
            :team, :season, :week, :game_id, :completions, :attempts, :passing_yards,
            :passing_tds, :interceptions, :sacks, :sack_yards, :carries, :rushing_yards,
            :rushing_tds, :targets, :receptions, :receiving_yards, :receiving_tds,
            :fantasy_points, :fantasy_points_ppr, :air_yards_share, :target_share,
            :wopr, :racr
        )
        ON CONFLICT (player_id, season, week) DO UPDATE SET
            passing_yards = EXCLUDED.passing_yards,
            rushing_yards = EXCLUDED.rushing_yards,
            receiving_yards = EXCLUDED.receiving_yards,
            fantasy_points = EXCLUDED.fantasy_points,
            fantasy_points_ppr = EXCLUDED.fantasy_points_ppr,
            target_share = EXCLUDED.target_share
    """)

    BATCH = 2000
    stored = 0
    for i in range(0, len(df), BATCH):
        batch = df.iloc[i:i + BATCH]
        rows = []
        for _, r in batch.iterrows():
            rows.append({
                'player_id': _safe_str(r.get('player_id')),
                'player_name': _safe_str(r.get('player_name')),
                'player_display_name': _safe_str(r.get('player_display_name')),
                'position': _safe_str(r.get('position')),
                'position_group': _safe_str(r.get('position_group')),
                'team': _safe_str(r.get('team')),
                'season': int(season),
                'week': _safe_int(r.get('week')),
                'game_id': _safe_str(r.get('game_id')),
                'completions': _safe_int(r.get('completions')) or 0,
                'attempts': _safe_int(r.get('attempts')) or 0,
                'passing_yards': _safe_int(r.get('passing_yards')) or 0,
                'passing_tds': _safe_int(r.get('passing_tds')) or 0,
                'interceptions': _safe_int(r.get('passing_interceptions')) or 0,
                'sacks': _safe_int(r.get('sacks_suffered')) or 0,
                'sack_yards': _safe_int(r.get('sack_yards_lost')) or 0,
                'carries': _safe_int(r.get('carries')) or 0,
                'rushing_yards': _safe_int(r.get('rushing_yards')) or 0,
                'rushing_tds': _safe_int(r.get('rushing_tds')) or 0,
                'targets': _safe_int(r.get('targets')) or 0,
                'receptions': _safe_int(r.get('receptions')) or 0,
                'receiving_yards': _safe_int(r.get('receiving_yards')) or 0,
                'receiving_tds': _safe_int(r.get('receiving_tds')) or 0,
                'fantasy_points': _safe_float(r.get('fantasy_points')) or 0,
                'fantasy_points_ppr': _safe_float(r.get('fantasy_points_ppr')) or 0,
                'air_yards_share': _safe_float(r.get('air_yards_share')),
                'target_share': _safe_float(r.get('target_share')),
                'wopr': _safe_float(r.get('wopr')),
                'racr': _safe_float(r.get('racr')),
            })
        if rows:
            db.execute(upsert_sql, rows)
            db.commit()
            stored += len(rows)

    print(f"  Player stats: {stored} upserted")
    return stored


def fetch_snap_counts(season: int, week: int = None, db=None) -> int:
    """Fetch snap count data (nflverse 'snap_counts' release, PFR ids)."""
    print(f"  Fetching {season} snap counts...")
    try:
        url = URLS['snap_counts'].format(season=season)
        df = _download_parquet(url)
    except Exception as e:
        print(f"  Snap counts not available for {season}: {e}")
        return 0

    if week is not None:
        df = df[df['week'] == week]

    upsert_sql = text("""
        INSERT INTO nfl.snap_counts (
            game_id, season, week, player_id, player_name, team, position,
            offense_snaps, offense_pct, defense_snaps, defense_pct, st_snaps, st_pct
        ) VALUES (
            :game_id, :season, :week, :player_id, :player_name, :team, :position,
            :offense_snaps, :offense_pct, :defense_snaps, :defense_pct, :st_snaps, :st_pct
        )
        ON CONFLICT (player_id, game_id) DO UPDATE SET
            offense_snaps = EXCLUDED.offense_snaps,
            offense_pct = EXCLUDED.offense_pct,
            defense_snaps = EXCLUDED.defense_snaps,
            defense_pct = EXCLUDED.defense_pct
    """)

    rows = []
    for _, r in df.iterrows():
        rows.append({
            'game_id': _safe_str(r.get('game_id')),
            'season': int(season),
            'week': _safe_int(r.get('week')),
            'player_id': _safe_str(r.get('pfr_player_id')),
            'player_name': _safe_str(r.get('player')),
            'team': _safe_str(r.get('team')),
            'position': _safe_str(r.get('position')),
            'offense_snaps': _safe_int(r.get('offense_snaps')) or 0,
            'offense_pct': _safe_float(r.get('offense_pct')),
            'defense_snaps': _safe_int(r.get('defense_snaps')) or 0,
            'defense_pct': _safe_float(r.get('defense_pct')),
            'st_snaps': _safe_int(r.get('st_snaps')) or 0,
            'st_pct': _safe_float(r.get('st_pct')),
        })

    if rows:
        db.execute(upsert_sql, rows)
        db.commit()

    print(f"  Snap counts: {len(rows)} upserted")
    return len(rows)


def fetch_players(db) -> int:
    """Fetch player bios (nflverse 'players' release — all-time, not season-scoped)."""
    print("  Fetching player bios...")
    df = _download_parquet(URLS['players'])

    upsert_sql = text("""
        INSERT INTO nfl.players (
            gsis_id, player_name, first_name, last_name, position, position_group,
            team, jersey_number, height, weight, birth_date, college,
            draft_year, draft_round, draft_pick, active, updated_at
        ) VALUES (
            :gsis_id, :player_name, :first_name, :last_name, :position, :position_group,
            :team, :jersey_number, :height, :weight, :birth_date, :college,
            :draft_year, :draft_round, :draft_pick, :active, NOW()
        )
        ON CONFLICT (gsis_id) DO UPDATE SET
            player_name = EXCLUDED.player_name,
            team = EXCLUDED.team,
            jersey_number = EXCLUDED.jersey_number,
            active = EXCLUDED.active,
            updated_at = NOW()
    """)

    BATCH = 2000
    stored = 0
    for i in range(0, len(df), BATCH):
        batch = df.iloc[i:i + BATCH]
        rows = []
        for _, r in batch.iterrows():
            gsis_id = _safe_str(r.get('gsis_id'))
            if not gsis_id:
                continue
            height = _safe_int(r.get('height'))
            rows.append({
                'gsis_id': gsis_id,
                'player_name': _safe_str(r.get('display_name')),
                'first_name': _safe_str(r.get('first_name')),
                'last_name': _safe_str(r.get('last_name')),
                'position': _safe_str(r.get('position')),
                'position_group': _safe_str(r.get('position_group')),
                'team': _safe_str(r.get('latest_team')),
                'jersey_number': _safe_int(r.get('jersey_number')),
                'height': str(height) if height is not None else None,
                'weight': _safe_int(r.get('weight')),
                'birth_date': _safe_str(r.get('birth_date')),
                'college': _safe_str(r.get('college_name')),
                'draft_year': _safe_int(r.get('draft_year')),
                'draft_round': _safe_int(r.get('draft_round')),
                'draft_pick': _safe_int(r.get('draft_pick')),
                'active': r.get('status') == 'ACT',
            })
        if rows:
            db.execute(upsert_sql, rows)
            db.commit()
            stored += len(rows)

    print(f"  Players: {stored} upserted")
    return stored


def build_team_metrics(season: int, week: int = None, db=None) -> int:
    """Calculate team EPA metrics from play-by-play and store in team_metrics_weekly."""
    print(f"  Building team metrics for {season}...")

    week_filter = "AND p.week = :w" if week else ""
    params = {"s": season}
    if week:
        params["w"] = week

    if week:
        db.execute(text("DELETE FROM nfl.team_metrics_weekly WHERE season = :s AND week = :w"), params)
    else:
        db.execute(text("DELETE FROM nfl.team_metrics_weekly WHERE season = :s"), params)

    db.execute(text(f"""
        WITH off AS (
            SELECT
                p.posteam AS team, p.season, p.week, p.game_id,
                ROUND(AVG(p.epa)::numeric, 4) AS off_epa_per_play,
                ROUND(AVG(CASE WHEN p.play_type = 'pass' THEN p.epa END)::numeric, 4) AS off_epa_pass,
                ROUND(AVG(CASE WHEN p.play_type = 'run' THEN p.epa END)::numeric, 4) AS off_epa_rush,
                ROUND(AVG(CASE WHEN p.epa > 0 THEN 1.0 ELSE 0.0 END)::numeric, 4) AS off_success_rate,
                COUNT(*) AS off_plays,
                SUM(COALESCE(p.yards_gained, 0)) AS off_yards,
                SUM(COALESCE(p.touchdown::int, 0)) AS off_tds,
                ROUND(AVG(CASE WHEN p.down = 3 AND p.first_down THEN 1.0
                               WHEN p.down = 3 THEN 0.0 END)::numeric, 4) AS off_third_down_pct,
                ROUND(AVG(CASE WHEN p.yardline_100 <= 20 AND p.touchdown THEN 1.0
                               WHEN p.yardline_100 <= 20 THEN 0.0 END)::numeric, 4) AS off_red_zone_pct
            FROM nfl.plays p
            WHERE p.season = :s {week_filter}
              AND p.play_type IN ('pass', 'run')
              AND p.epa IS NOT NULL
            GROUP BY p.posteam, p.season, p.week, p.game_id
        ),
        def AS (
            SELECT
                p.defteam AS team, p.season, p.week, p.game_id,
                ROUND(AVG(p.epa)::numeric, 4) AS def_epa_per_play,
                ROUND(AVG(CASE WHEN p.play_type = 'pass' THEN p.epa END)::numeric, 4) AS def_epa_pass,
                ROUND(AVG(CASE WHEN p.play_type = 'run' THEN p.epa END)::numeric, 4) AS def_epa_rush,
                ROUND(AVG(CASE WHEN p.epa > 0 THEN 1.0 ELSE 0.0 END)::numeric, 4) AS def_success_rate,
                COUNT(*) AS def_plays,
                SUM(COALESCE(p.yards_gained, 0)) AS def_yards_allowed,
                SUM(COALESCE(p.touchdown::int, 0)) AS def_tds_allowed,
                ROUND(AVG(CASE WHEN p.down = 3 AND p.first_down THEN 1.0
                               WHEN p.down = 3 THEN 0.0 END)::numeric, 4) AS def_third_down_pct,
                ROUND(AVG(CASE WHEN p.yardline_100 <= 20 AND p.touchdown THEN 1.0
                               WHEN p.yardline_100 <= 20 THEN 0.0 END)::numeric, 4) AS def_red_zone_pct
            FROM nfl.plays p
            WHERE p.season = :s {week_filter}
              AND p.play_type IN ('pass', 'run')
              AND p.epa IS NOT NULL
            GROUP BY p.defteam, p.season, p.week, p.game_id
        )
        INSERT INTO nfl.team_metrics_weekly (
            team, season, week, game_id, home_away,
            off_epa_per_play, off_epa_pass, off_epa_rush, off_success_rate,
            off_plays, off_yards, off_tds, off_third_down_pct, off_red_zone_pct,
            def_epa_per_play, def_epa_pass, def_epa_rush, def_success_rate,
            def_plays, def_yards_allowed, def_tds_allowed,
            def_third_down_pct, def_red_zone_pct,
            points_scored, points_allowed, won
        )
        SELECT
            o.team, o.season, o.week, o.game_id,
            CASE WHEN g.home_team = o.team THEN 'home' ELSE 'away' END AS home_away,
            o.off_epa_per_play, o.off_epa_pass, o.off_epa_rush, o.off_success_rate,
            o.off_plays, o.off_yards, o.off_tds, o.off_third_down_pct, o.off_red_zone_pct,
            d.def_epa_per_play, d.def_epa_pass, d.def_epa_rush, d.def_success_rate,
            d.def_plays, d.def_yards_allowed, d.def_tds_allowed,
            d.def_third_down_pct, d.def_red_zone_pct,
            CASE WHEN g.home_team = o.team THEN g.home_score ELSE g.away_score END AS points_scored,
            CASE WHEN g.home_team = o.team THEN g.away_score ELSE g.home_score END AS points_allowed,
            CASE WHEN g.home_team = o.team THEN g.home_win ELSE NOT g.home_win END AS won
        FROM off o
        JOIN def d ON d.team = o.team AND d.game_id = o.game_id
        JOIN nfl.games g ON g.game_id = o.game_id
        ON CONFLICT (team, game_id) DO UPDATE SET
            off_epa_per_play = EXCLUDED.off_epa_per_play,
            def_epa_per_play = EXCLUDED.def_epa_per_play,
            points_scored = EXCLUDED.points_scored,
            points_allowed = EXCLUDED.points_allowed,
            won = EXCLUDED.won
    """), params)

    db.commit()
    count = db.execute(text(
        "SELECT COUNT(*) FROM nfl.team_metrics_weekly WHERE season = :s"), {"s": season}
    ).scalar()
    print(f"  Team metrics: {count} rows")
    return count


def run_season(season: int, week: int = None):
    db = SessionLocal()
    try:
        print(f"\n=== NFL Fetch: Season {season}{f' Week {week}' if week else ''} ===")
        fetch_games(season, db)
        fetch_plays(season, week, db)
        fetch_player_stats(season, week, db)
        fetch_snap_counts(season, week, db)
        fetch_players(db)
        build_team_metrics(season, week, db)
        print(f"\nSeason {season} complete")
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--week", type=int, default=None)
    args = parser.parse_args()
    run_season(args.season, args.week)
