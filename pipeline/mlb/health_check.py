"""
MLB Daily Pipeline Health Check Suite

Runs after fetch + transform to validate data quality.
Prints a summary report and returns True if all checks pass.

Usage:
    PYTHONPATH=/app python3 /pipeline/mlb/health_check.py --date 2026-04-01
    PYTHONPATH=/app python3 /pipeline/mlb/health_check.py --yesterday
"""

import sys
import os
import argparse
from datetime import datetime, timedelta, timezone
from sqlalchemy import text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'api'))
from database import SessionLocal

WARN = "⚠️ "
OK   = "✓  "
FAIL = "✗  "
INFO = "   "


def get_schedule_count(date_str: str, db) -> int:
    """How many Final games does the MLB API say there were on this date."""
    import httpx
    try:
        url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&gameType=R,S&date={date_str}"
        res = httpx.get(url, timeout=15)
        data = res.json()
        total = 0
        for d in data.get("dates", []):
            for g in d.get("games", []):
                if g.get("status", {}).get("detailedState") == "Final":
                    total += 1
        return total
    except Exception as e:
        print(f"{WARN}Could not fetch schedule: {e}")
        return -1


def run_health_checks(date_str: str, db) -> bool:
    print(f"\n{'='*55}")
    print(f"  MLB Pipeline Health Check — {date_str}")
    print(f"{'='*55}")

    all_passed = True
    warnings = []

    # ── 1. Game count vs schedule ──────────────────────────
    print(f"\n  Game Coverage")
    scheduled = get_schedule_count(date_str, db)
    stored = db.execute(text("""
        SELECT COUNT(*) FROM mlb.games WHERE game_date = :d
    """), {"d": date_str}).scalar()

    raw_stored = db.execute(text("""
        SELECT COUNT(*) FROM mlb.raw_events WHERE game_date = :d
    """), {"d": date_str}).scalar()

    if scheduled == -1:
        print(f"{WARN}Could not verify schedule — {stored} games in DB")
        warnings.append("Schedule API unavailable")
    elif stored == scheduled:
        print(f"{OK}{stored}/{scheduled} games transformed")
    elif stored >= scheduled * 0.95:
        print(f"{WARN}{stored}/{scheduled} games transformed (>{scheduled - stored} missing)")
        warnings.append(f"{scheduled - stored} games missing from transform")
    else:
        print(f"{FAIL}{stored}/{scheduled} games transformed — significant gap")
        all_passed = False

    print(f"{INFO}{raw_stored} raw events stored")

    # ── 1b. Fangraphs season total cross-reference ────────
    print(f"\n  Fangraphs Cross-Reference")
    #season = datetime.now().year
    season = int(date_str[:4])

    fg_totals = db.execute(text("""
        SELECT 
            SUM(pa) as total_pa,
            COUNT(*) as players
        FROM mlb.fangraphs_batting
        WHERE season = :season
    """), {"season": season}).mappings().first()

    gumbo_totals = db.execute(text("""
        SELECT 
            COUNT(*) as total_pa,
            COUNT(DISTINCT g.game_pk) as total_games
        FROM mlb.at_bats ab
        JOIN mlb.games g ON g.game_pk = ab.game_pk
        WHERE g.season = :season
        AND g.game_type = 'R'
    """), {"season": season}).mappings().first()

    if fg_totals and fg_totals["total_pa"] and gumbo_totals["total_pa"]:
        fg_pa = int(fg_totals["total_pa"])
        gumbo_pa = int(gumbo_totals["total_pa"])
        diff = fg_pa - gumbo_pa
        diff_pct = abs(diff) / max(gumbo_pa, 1) * 100

        if diff == 0:
            print(f"{OK}Fangraphs PA ({fg_pa:,}) exactly matches GUMBO ({gumbo_pa:,})")
        elif diff > 0:
            print(f"{FAIL}Fangraphs ahead by {diff:,} PAs — missing games in GUMBO")
            warnings.append(f"Missing data: Fangraphs has {diff:,} more PAs than GUMBO")
            all_passed = False
        elif abs(diff) <= 50:
            print(f"{WARN}GUMBO ahead by {abs(diff):,} PAs — Fangraphs may not have updated yet")
            warnings.append(f"Fangraphs lag: GUMBO has {abs(diff):,} more PAs")
        else:
            print(f"{FAIL}GUMBO ahead by {abs(diff):,} PAs — significant gap, investigate")
            warnings.append(f"Large PA gap: GUMBO has {abs(diff):,} more PAs than Fangraphs")
            all_passed = False
    else:
        print(f"{WARN}No Fangraphs data for {season} — run fetch_fangraphs.py first")


    # Diagnostic — only runs when there's a mismatch
    if diff != 0 and abs(diff) > 50:
        print(f"\n  PA Mismatch Diagnostic")
        diagnostic = db.execute(text("""
            WITH gumbo_pa AS (
                SELECT 
                    ab.batter_id as player_id,
                    ab.batter_name as player_name,
                    COUNT(*) as gumbo_pa_count
                FROM mlb.at_bats ab
                JOIN mlb.games g ON g.game_pk = ab.game_pk
                WHERE g.season = :season
                AND g.game_type = 'R'
                GROUP BY ab.batter_id, ab.batter_name
            ),
            fg_pa AS (
                SELECT
                    mlbam_id as player_id,
                    player_name,
                    pa as fg_pa_count
                FROM mlb.fangraphs_batting
                WHERE season = :season
            )
            SELECT
                COALESCE(g.player_name, f.player_name) as player_name,
                COALESCE(g.player_id, f.player_id) as player_id,
                COALESCE(g.gumbo_pa_count, 0) as gumbo_pa,
                COALESCE(f.fg_pa_count, 0) as fg_pa,
                COALESCE(g.gumbo_pa_count, 0) - COALESCE(f.fg_pa_count, 0) as diff
            FROM gumbo_pa g
            FULL OUTER JOIN fg_pa f ON f.player_id = g.player_id
            WHERE ABS(COALESCE(g.gumbo_pa_count, 0) - COALESCE(f.fg_pa_count, 0)) > 5
            ORDER BY ABS(COALESCE(g.gumbo_pa_count, 0) - COALESCE(f.fg_pa_count, 0)) DESC
            LIMIT 20
        """), {"season": season}).mappings().all()

        if diagnostic:
            print(f"{INFO}Top PA discrepancies (player level):")
            print(f"{INFO}  {'Player':<25} {'GUMBO PA':>8} {'FG PA':>8} {'Diff':>8}")
            print(f"{INFO}  {'-'*25} {'-'*8} {'-'*8} {'-'*8}")
            for row in diagnostic:
                diff_str = f"+{row['diff']}" if row['diff'] > 0 else str(row['diff'])
                print(f"{INFO}  {str(row['player_name']):<25} {row['gumbo_pa']:>8,} {row['fg_pa']:>8,} {diff_str:>8}")
        else:
            print(f"{INFO}No individual player discrepancies over 5 PAs found")

    # ── 2. Plate appearance sanity ─────────────────────────
    print(f"\n  Plate Appearances")
    pa_stats = db.execute(text("""
        SELECT 
            COUNT(*) as total_abs,
            COUNT(DISTINCT ab.game_pk) as games,
            ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT ab.game_pk), 0), 1) as pa_per_game
        FROM mlb.at_bats ab
        JOIN mlb.games g ON g.game_pk = ab.game_pk
        WHERE g.game_date = :d
    """), {"d": date_str}).mappings().first()

    if pa_stats and pa_stats["games"] > 0:
        pa_per = pa_stats["pa_per_game"]
        total = pa_stats["total_abs"]
        if 25 <= pa_per <= 45:
            print(f"{OK}{total} total PAs · {pa_per} per game")
        elif pa_per < 25:
            print(f"{FAIL}{pa_per} PAs/game — suspiciously low")
            all_passed = False
        else:
            print(f"{WARN}{pa_per} PAs/game — higher than expected")
            warnings.append(f"High PA count: {pa_per}/game")
    else:
        print(f"{WARN}No at-bat data found for {date_str}")

    # ── 3. Pitch count per game ────────────────────────────
    print(f"\n  Pitch Counts")
    pitch_stats = db.execute(text("""
        SELECT 
            g.game_pk,
            g.away_team_abbrev || ' @ ' || g.home_team_abbrev as matchup,
            COUNT(p.id) as pitch_count
        FROM mlb.games g
        LEFT JOIN mlb.pitches p ON p.game_pk = g.game_pk
        WHERE g.game_date = :d
        GROUP BY g.game_pk, matchup
        ORDER BY pitch_count
    """), {"d": date_str}).mappings().all()

    low_pitch_games = [r for r in pitch_stats if r["pitch_count"] < 200]
    if not pitch_stats:
        print(f"{WARN}No pitch data found")
    elif low_pitch_games:
        print(f"{FAIL}{len(low_pitch_games)} games with <200 pitches:")
        for g in low_pitch_games:
            print(f"{INFO}  {g['matchup']}: {g['pitch_count']} pitches")
        all_passed = False
    else:
        avg_pitches = sum(r["pitch_count"] for r in pitch_stats) / len(pitch_stats)
        print(f"{OK}All {len(pitch_stats)} games have 200+ pitches · avg {avg_pitches:.0f}")

    # ── 4. Statcast coverage ───────────────────────────────
    print(f"\n  Statcast Coverage")
    statcast = db.execute(text("""
        SELECT
            COUNT(*) as total_pitches,
            COUNT(spin_rate) as has_spin_rate,
            COUNT(launch_speed) as has_launch_speed,
            COUNT(px) as has_location,
            ROUND(COUNT(spin_rate)::numeric / NULLIF(COUNT(*), 0) * 100, 1) as spin_pct,
            ROUND(COUNT(launch_speed)::numeric / NULLIF(COUNT(*), 0) * 100, 1) as launch_pct,
            ROUND(COUNT(px)::numeric / NULLIF(COUNT(*), 0) * 100, 1) as location_pct
        FROM mlb.pitches p
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE g.game_date = :d
    """), {"d": date_str}).mappings().first()

    if statcast and statcast["total_pitches"] > 0:
        print(f"{OK}{statcast['total_pitches']} pitches")
        spin_ok = statcast["spin_pct"] > 80
        launch_ok = statcast["launch_pct"] > 5
        loc_ok = statcast["location_pct"] > 90

        print(f"{'OK' if spin_ok else WARN}  Spin rate: {statcast['spin_pct']}% coverage")
        print(f"{'OK' if launch_ok else WARN}  Launch speed: {statcast['launch_pct']}% coverage (in-play only)")
        print(f"{'OK' if loc_ok else WARN}  Location (px/pz): {statcast['location_pct']}% coverage")

        if statcast["spin_pct"] < 50:
            warnings.append(f"Low spin rate coverage: {statcast['spin_pct']}%")
        if statcast["location_pct"] < 80:
            all_passed = False
    else:
        print(f"{WARN}No pitch data to check")

    # ── 5. Duplicate game_pk check ─────────────────────────
    print(f"\n  Duplicate Check")
    dupes = db.execute(text("""
        SELECT game_pk, COUNT(*) as cnt
        FROM mlb.games
        WHERE game_date = :d
        GROUP BY game_pk
        HAVING COUNT(*) > 1
    """), {"d": date_str}).mappings().all()

    if dupes:
        print(f"{FAIL}{len(dupes)} duplicate game_pks found")
        all_passed = False
    else:
        print(f"{OK}No duplicate game_pks")

    # ── 6. Games stuck in non-Final status ─────────────────
    print(f"\n  Game Status")
    non_final = db.execute(text("""
        SELECT game_pk, away_team_abbrev, home_team_abbrev, status
        FROM mlb.games
        WHERE game_date = :d
        AND status != 'Final'
    """), {"d": date_str}).mappings().all()

    if non_final:
        print(f"{WARN}{len(non_final)} games not in Final status:")
        for g in non_final:
            print(f"{INFO}  {g['away_team_abbrev']} @ {g['home_team_abbrev']}: {g['status']}")
        warnings.append(f"{len(non_final)} non-Final games")
    else:
        print(f"{OK}All games in Final status")

    # ── 7. Runner data completeness ────────────────────────
    print(f"\n  Runner Data")
    runner_check = db.execute(text("""
        SELECT
            COUNT(DISTINCT g.game_pk) as games_with_runners,
            COUNT(DISTINCT ab.game_pk) as games_with_atbats
        FROM mlb.games g
        LEFT JOIN mlb.at_bats ab ON ab.game_pk = g.game_pk
        LEFT JOIN mlb.runners r ON r.game_pk = g.game_pk
        WHERE g.game_date = :d
    """), {"d": date_str}).mappings().first()

    runner_games = db.execute(text("""
        SELECT COUNT(DISTINCT r.game_pk) as cnt
        FROM mlb.runners r
        JOIN mlb.games g ON g.game_pk = r.game_pk
        WHERE g.game_date = :d
    """), {"d": date_str}).scalar()

    if stored and stored > 0:
        if runner_games == stored:
            print(f"{OK}Runner data present for all {stored} games")
        else:
            print(f"{WARN}Runner data for {runner_games}/{stored} games")
            warnings.append("Missing runner data")
    else:
        print(f"{INFO}No games to check")

    # ── Summary ────────────────────────────────────────────
    print(f"\n{'='*55}")
    if all_passed and not warnings:
        print(f"  ✅ All checks passed")
    elif all_passed:
        print(f"  ⚠️  Passed with {len(warnings)} warning(s):")
        for w in warnings:
            print(f"     · {w}")
    else:
        print(f"  ❌ Health check FAILED")
        if warnings:
            print(f"  Warnings:")
            for w in warnings:
                print(f"     · {w}")
    print(f"{'='*55}\n")

    return all_passed


def main():
    parser = argparse.ArgumentParser(description="MLB pipeline health checks")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", type=str, help="Date to check (YYYY-MM-DD)")
    group.add_argument("--yesterday", action="store_true")
    group.add_argument("--today", action="store_true")
    args = parser.parse_args()

    if args.yesterday:
        date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    elif args.today:
        date_str = datetime.now().strftime("%Y-%m-%d")
    else:
        date_str = args.date

    db = SessionLocal()
    try:
        run_health_checks(date_str, db)
    finally:
        db.close()


if __name__ == "__main__":
    main()