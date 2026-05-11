"""
Daily projection pipeline.

For each probable starter today:
1. Get opposing lineup (posted) or roster (fallback)
2. Filter to top 8 batters by PA vs pitcher handedness
3. Run 500 simulations per batter using simulate_at_bat logic
4. Aggregate projected K%, HR%, OPS, etc.
5. Store in mlb.daily_projections

Run daily before first pitch (~10am ET).
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import argparse
import json as json_lib
import httpx
from datetime import date
from database import SessionLocal
from sqlalchemy import text
from mlb.simulate import (
    load_pitcher_count_mix,
    load_league_count_mix,
    load_pitcher_overall_mix,
    load_batter_tends,
    load_goose3_adjustments,
    load_juiced2_batter_adjustments,
    simulate_pa,
    aggregate_outcomes,
    LEAGUE_WHIFF,
    LEAGUE_CHASE,
    LEAGUE_HR_RATE,
)

N_SIMS_PER_BATTER = 500
TOP_N_BATTERS = 8
MIN_PA = 20

TEAM_ABBREVS = {
    108: 'LAA', 109: 'AZ', 110: 'BAL', 111: 'BOS', 112: 'CHC',
    113: 'CIN', 114: 'CLE', 115: 'COL', 116: 'DET', 117: 'HOU',
    118: 'KC',  119: 'LAD', 120: 'WSH', 121: 'NYM', 133: 'ATH',
    134: 'PIT', 135: 'SD',  136: 'SEA', 137: 'SF',  138: 'STL',
    139: 'TB',  140: 'TEX', 141: 'TOR', 142: 'MIN', 143: 'PHI',
    144: 'ATL', 145: 'CWS', 146: 'MIA', 147: 'NYY', 158: 'MIL',
}

def get_schedule(target_date: str) -> list:
    """Fetch today's schedule with probable pitchers and lineups."""
    r = httpx.get('https://statsapi.mlb.com/api/v1/schedule', params={
        'sportId': 1,
        'date': target_date,
        'gameType': 'R',
        'hydrate': 'probablePitcher,lineups,teams'
    }, timeout=20)
    r.raise_for_status()
    d = r.json()

    games = []
    for date_entry in d.get('dates', []):
        for game in date_entry.get('games', []):
            status = game.get('status', {}).get('detailedState', '')
            if status in ('Final', 'Game Over'):
                continue

            away = game.get('teams', {}).get('away', {})
            home = game.get('teams', {}).get('home', {})
            lineups = game.get('lineups', {})

            away_tid = away.get('team', {}).get('id')
            home_tid = home.get('team', {}).get('id')
            games.append({
                'game_pk': game['gamePk'],
                'away_team_id': away_tid,
                'away_abbrev': TEAM_ABBREVS.get(away_tid, str(away_tid)),
                'home_team_id': home_tid,
                'home_abbrev': TEAM_ABBREVS.get(home_tid, str(home_tid)),
                'away_probable': away.get('probablePitcher', {}),
                'home_probable': home.get('probablePitcher', {}),
                'away_lineup': [p['id'] for p in lineups.get('awayPlayers', [])],
                'home_lineup': [p['id'] for p in lineups.get('homePlayers', [])],
            })
    return games


def get_team_roster(team_id: int) -> list:
    """Get active roster player IDs (non-pitchers)."""
    r = httpx.get(f'https://statsapi.mlb.com/api/v1/teams/{team_id}/roster', params={
        'rosterType': 'active', 'season': 2026
    }, timeout=15)
    r.raise_for_status()
    roster = r.json().get('roster', [])
    return [
        p['person']['id'] for p in roster
        if p.get('position', {}).get('abbreviation') != 'P'
    ]


def get_pitcher_hand(pitcher_id: int, db) -> str:
    result = db.execute(text("""
        SELECT pitch_hand FROM mlb.at_bats
        WHERE pitcher_id = :pid AND pitch_hand IS NOT NULL LIMIT 1
    """), {"pid": pitcher_id}).scalar()
    return result or 'R'


def get_top_batters(batter_ids: list, pitcher_hand: str, db) -> list:
    if not batter_ids:
        return []

    sql = text("""
        SELECT
            bb.player_id as batter_id,
            MAX(ab.batter_name) as batter_name,
            SUM(bb.at_bats) as ab,
            SUM(bb.at_bats) + COALESCE(SUM(bb.hit_by_pitch),0) as pa
        FROM mlb.boxscore_batting bb
        JOIN mlb.games g ON g.game_pk = bb.game_pk
        JOIN mlb.at_bats ab ON ab.game_pk = bb.game_pk
            AND ab.batter_id = bb.player_id
        WHERE bb.player_id = ANY(:bids)
        AND g.season = 2026
        AND g.game_type = 'R'
        AND ab.pitch_hand = :phand
        GROUP BY bb.player_id
        HAVING SUM(bb.at_bats) >= :min_pa
        ORDER BY pa DESC
        LIMIT :top_n
    """)
    rows = db.execute(sql, {
        "bids": batter_ids, "phand": pitcher_hand,
        "min_pa": MIN_PA, "top_n": TOP_N_BATTERS
    }).mappings().all()

    if len(rows) < 4:
        sql2 = text("""
            SELECT
                bb.player_id as batter_id,
                MAX(ab.batter_name) as batter_name,
                SUM(bb.at_bats) as ab,
                SUM(bb.at_bats) + COALESCE(SUM(bb.hit_by_pitch),0) as pa
            FROM mlb.boxscore_batting bb
            JOIN mlb.games g ON g.game_pk = bb.game_pk
            JOIN mlb.at_bats ab ON ab.game_pk = bb.game_pk
                AND ab.batter_id = bb.player_id
            WHERE bb.player_id = ANY(:bids)
            AND g.season = 2026
            AND g.game_type = 'R'
            GROUP BY bb.player_id
            HAVING SUM(bb.at_bats) >= 10
            ORDER BY pa DESC
            LIMIT :top_n
        """)
        rows = db.execute(sql2, {"bids": batter_ids, "top_n": TOP_N_BATTERS}).mappings().all()

    return [dict(r) for r in rows]


def simulate_pitcher_vs_batters(pitcher_id: int, batters: list, db) -> dict:
    """Run N_SIMS_PER_BATTER simulations for each batter using combined Goose+1/2 + Stuff Score."""
    lg_mix = load_league_count_mix(db)
    all_results = []

    for batter in batters:
        batter_id = int(batter['batter_id'])

        bat_side = db.execute(text("""
            SELECT bat_side FROM mlb.at_bats
            WHERE batter_id = :bid AND bat_side IS NOT NULL LIMIT 1
        """), {"bid": batter_id}).scalar() or 'R'

        count_mix = load_pitcher_count_mix(pitcher_id, bat_side, db)
        fallback_mix = load_pitcher_overall_mix(pitcher_id, bat_side, db)
        batter_tends = load_batter_tends(batter_id, db)
        pitcher_adj = load_goose3_adjustments(pitcher_id, bat_side, db)
        juiced2_adj = load_juiced2_batter_adjustments(batter_id, db)

        if not fallback_mix and not count_mix:
            continue

        for _ in range(N_SIMS_PER_BATTER):
            pa = simulate_pa(count_mix, lg_mix, fallback_mix, batter_tends,
                             record_pitches=False,
                             goose2_adj=pitcher_adj if pitcher_adj else None,
                             juiced2_adj=juiced2_adj if juiced2_adj else None)
            all_results.append(pa)

    if not all_results:
        return {}

    summary = aggregate_outcomes(all_results, len(all_results))

    return {
        "proj_k_pct": summary["k_pct"],
        "proj_hr_pct": summary["hr_pct"],
        "proj_hit_pct": summary["hit_pct"],
        "proj_ops": summary["ops"],
        "proj_bb_pct": summary["bb_pct"],
        "proj_ks_6inn": round(summary["k_pct"] / 100 * 21, 1),
        "total_sims": len(all_results),
    }


def build_daily_projections(target_date: str, db):
    print(f"\n=== Building Daily Projections — {target_date} ===")

    games = get_schedule(target_date)
    print(f"  Found {len(games)} games")

    stored = 0
    for game in games:
        for side in ['away', 'home']:
            probable = game[f'{side}_probable']
            if not probable or not probable.get('id'):
                print(f"  No probable pitcher for {side} in game {game['game_pk']}")
                continue

            pitcher_id = int(probable['id'])
            pitcher_name = probable.get('fullName', 'Unknown')
            opp_side = 'home' if side == 'away' else 'away'
            opp_team_id = game[f'{opp_side}_team_id']
            opp_abbrev = game[f'{opp_side}_abbrev']
            team_abbrev = game[f'{side}_abbrev']

            print(f"  Processing {pitcher_name} ({team_abbrev} vs {opp_abbrev})...")

            pitcher_hand = get_pitcher_hand(pitcher_id, db)

            posted_lineup = game[f'{opp_side}_lineup']
            if posted_lineup and len(posted_lineup) >= 7:
                batter_ids = posted_lineup
                print(f"    Using posted lineup ({len(batter_ids)} players)")
            else:
                try:
                    batter_ids = get_team_roster(opp_team_id)
                    print(f"    Using roster ({len(batter_ids)} players)")
                except Exception as e:
                    print(f"    Roster fetch failed: {e}")
                    continue

            batters = get_top_batters(batter_ids, pitcher_hand, db)
            if not batters:
                print(f"    No batter data found, skipping")
                continue

            print(f"    Simulating vs {len(batters)} batters...")

            proj = simulate_pitcher_vs_batters(pitcher_id, batters, db)
            if not proj:
                continue

            goose = db.execute(text("""
                SELECT goose_plus FROM mlb.goose_overall
                WHERE pitcher_id = :pid AND season = 2026 AND game_pk IS NULL
            """), {"pid": pitcher_id}).scalar()

            bapv = db.execute(text("""
                SELECT ROUND(AVG(bapv_plus)::numeric, 1)
                FROM mlb.pitch_quality_scores
                WHERE pitcher_id = :pid AND season = 2026 AND game_type = 'R'
            """), {"pid": pitcher_id}).scalar()

            db.execute(text("""
                INSERT INTO mlb.daily_projections (
                    snapshot_date, game_pk, pitcher_id, pitcher_name,
                    pitcher_hand, team_abbrev, opp_abbrev, opp_team_id,
                    goose_plus, bapv_plus,
                    proj_k_pct, proj_hr_pct, proj_hit_pct, proj_ops,
                    proj_ks_6inn, proj_bb_pct,
                    batters_simulated, simulations_per_batter, batter_ids
                ) VALUES (
                    :snap_date, :game_pk, :pitcher_id, :pitcher_name,
                    :pitcher_hand, :team_abbrev, :opp_abbrev, :opp_team_id,
                    :goose_plus, :bapv_plus,
                    :proj_k_pct, :proj_hr_pct, :proj_hit_pct, :proj_ops,
                    :proj_ks_6inn, :proj_bb_pct,
                    :batters_sim, :sims_per, :batter_ids
                )
                ON CONFLICT (snapshot_date, pitcher_id) DO UPDATE SET
                    team_abbrev = EXCLUDED.team_abbrev,
                    opp_abbrev = EXCLUDED.opp_abbrev,
                    opp_team_id = EXCLUDED.opp_team_id,
                    proj_k_pct = EXCLUDED.proj_k_pct,
                    proj_hr_pct = EXCLUDED.proj_hr_pct,
                    proj_hit_pct = EXCLUDED.proj_hit_pct,
                    proj_ops = EXCLUDED.proj_ops,
                    proj_ks_6inn = EXCLUDED.proj_ks_6inn,
                    proj_bb_pct = EXCLUDED.proj_bb_pct,
                    batters_simulated = EXCLUDED.batters_simulated,
                    goose_plus = EXCLUDED.goose_plus,
                    bapv_plus = EXCLUDED.bapv_plus,
                    created_at = NOW()
            """), {
                "snap_date": target_date,
                "game_pk": game['game_pk'],
                "pitcher_id": pitcher_id,
                "pitcher_name": pitcher_name,
                "pitcher_hand": pitcher_hand,
                "team_abbrev": team_abbrev,
                "opp_abbrev": opp_abbrev,
                "opp_team_id": opp_team_id,
                "goose_plus": float(goose) if goose else None,
                "bapv_plus": float(bapv) if bapv else None,
                "proj_k_pct": proj["proj_k_pct"],
                "proj_hr_pct": proj["proj_hr_pct"],
                "proj_hit_pct": proj["proj_hit_pct"],
                "proj_ops": proj["proj_ops"],
                "proj_ks_6inn": proj["proj_ks_6inn"],
                "proj_bb_pct": proj["proj_bb_pct"],
                "batters_sim": len(batters),
                "sims_per": N_SIMS_PER_BATTER,
                "batter_ids": json_lib.dumps([b['batter_id'] for b in batters]),
            })
            db.commit()
            stored += 1
            print(f"    Stored: K%={proj['proj_k_pct']}% HR%={proj['proj_hr_pct']}% OPS={proj['proj_ops']}")

    print(f"\n=== Done. Stored {stored} projections ===")


def get_pitcher_primary_pitch(pitcher_id: int, db):
    """Get pitcher's most-used pitch type and its Goose+ bucket."""
    for season in [2026, 2025]:
        row = db.execute(text("""
            SELECT pitch_type_code, velo_quintile, goose_plus, pitches_thrown
            FROM mlb.goose_scores
            WHERE pitcher_id = :pid AND season = :s
            ORDER BY pitches_thrown DESC
            LIMIT 1
        """), {"pid": pitcher_id, "s": season}).mappings().first()
        if row:
            return dict(row)
    return None


def get_batter_juiced_vs_bucket(batter_id: int, pitch_type: str,
                                 velo_quintile: int, db) -> float:
    result = db.execute(text("""
        SELECT juiced_plus FROM mlb.juiced_scores
        WHERE batter_id = :bid AND pitch_type_code = :pt
        AND velo_quintile = :vq AND season = 2026
    """), {"bid": batter_id, "pt": pitch_type, "vq": velo_quintile}).scalar()
    if result is None:
        result = db.execute(text("""
            SELECT ROUND(AVG(juiced_plus)::numeric, 1)
            FROM mlb.juiced_scores
            WHERE batter_id = :bid AND pitch_type_code = :pt
            AND season = 2026 AND pa >= 3
        """), {"bid": batter_id, "pt": pitch_type}).scalar()
    return float(result) if result is not None else None


def simulate_batter_vs_pitcher(batter_id: int, pitcher_id: int,
                                n_sims: int, db) -> dict:
    """Run n_sims PA simulations for one batter vs one pitcher."""
    bat_side = db.execute(text("""
        SELECT bat_side FROM mlb.at_bats
        WHERE batter_id = :bid AND bat_side IS NOT NULL LIMIT 1
    """), {"bid": batter_id}).scalar() or 'R'

    count_mix = load_pitcher_count_mix(pitcher_id, bat_side, db)
    lg_mix = load_league_count_mix(db)
    fallback_mix = load_pitcher_overall_mix(pitcher_id, bat_side, db)
    batter_tends = load_batter_tends(batter_id, db)

    if not fallback_mix and not count_mix:
        return {}

    results = []
    for _ in range(n_sims):
        pa = simulate_pa(count_mix, lg_mix, fallback_mix,
                         batter_tends, record_pitches=False)
        results.append(pa)

    summary = aggregate_outcomes(results, n_sims)
    return {"bat_side": bat_side, **summary}


def build_daily_batter_projections(target_date: str, db):
    print(f"\n=== Building Daily Batter Projections — {target_date} ===")

    games = get_schedule(target_date)
    print(f"  Found {len(games)} games")

    stored = 0
    for game in games:
        for side in ['away', 'home']:
            opp_side = 'home' if side == 'away' else 'away'
            probable = game[f'{opp_side}_probable']

            if not probable or not probable.get('id'):
                continue

            pitcher_id = int(probable['id'])
            pitcher_name = probable.get('fullName', 'Unknown')
            team_id = game[f'{side}_team_id']
            team_abbrev = game[f'{side}_abbrev']
            opp_abbrev = game[f'{opp_side}_abbrev']

            print(f"  {team_abbrev} batters vs {pitcher_name} ({opp_abbrev})...")

            pitcher_hand = get_pitcher_hand(pitcher_id, db)
            primary_pitch = get_pitcher_primary_pitch(pitcher_id, db)

            opp_goose = db.execute(text("""
                SELECT goose_plus FROM mlb.goose_overall
                WHERE pitcher_id = :pid AND season = 2026 AND game_pk IS NULL
            """), {"pid": pitcher_id}).scalar()

            try:
                batter_ids = get_team_roster(team_id)
            except Exception as e:
                print(f"    Roster fetch failed: {e}")
                continue

            if not batter_ids:
                continue

            batter_rows = db.execute(text("""
                SELECT DISTINCT bb.player_id as batter_id,
                       MAX(ab.batter_name) as batter_name,
                       SUM(bb.at_bats) as ab
                FROM mlb.boxscore_batting bb
                JOIN mlb.games g ON g.game_pk = bb.game_pk
                JOIN mlb.at_bats ab ON ab.game_pk = bb.game_pk
                    AND ab.batter_id = bb.player_id
                WHERE bb.player_id = ANY(:bids)
                AND g.season = 2026 AND g.game_type = 'R'
                AND bb.at_bats > 0
                GROUP BY bb.player_id
                HAVING SUM(bb.at_bats) >= 10
                ORDER BY ab DESC
            """), {"bids": batter_ids}).mappings().all()

            print(f"    {len(batter_rows)} batters with PA data")

            for batter in batter_rows:
                batter_id = int(batter['batter_id'])
                batter_name = batter['batter_name']

                sim = simulate_batter_vs_pitcher(batter_id, pitcher_id, N_SIMS_PER_BATTER, db)
                if not sim:
                    continue

                juiced_bucket = None
                if primary_pitch:
                    juiced_bucket = get_batter_juiced_vs_bucket(
                        batter_id,
                        primary_pitch['pitch_type_code'],
                        primary_pitch['velo_quintile'],
                        db
                    )

                juiced_overall = db.execute(text("""
                    SELECT ROUND(
                        (SUM(juiced_plus * pa) / NULLIF(SUM(pa), 0))::numeric, 1
                    )
                    FROM mlb.juiced_scores
                    WHERE batter_id = :bid AND season = 2026 AND pa >= 3
                """), {"bid": batter_id}).scalar()

                db.execute(text("""
                    INSERT INTO mlb.daily_batter_projections (
                        snapshot_date, game_pk, batter_id, batter_name,
                        bat_side, team_abbrev, opp_pitcher_id, opp_pitcher_name,
                        opp_pitcher_hand, opp_goose_plus,
                        primary_pitch_type, primary_pitch_velo_quintile,
                        primary_pitch_goose_plus,
                        juiced_plus_vs_bucket, juiced_plus_overall,
                        proj_avg, proj_obp, proj_slg, proj_ops,
                        proj_hr_pct, proj_k_pct, proj_bb_pct, proj_hit_pct,
                        simulations
                    ) VALUES (
                        :snap_date, :game_pk, :batter_id, :batter_name,
                        :bat_side, :team_abbrev, :pitcher_id, :pitcher_name,
                        :pitcher_hand, :opp_goose,
                        :primary_pt, :primary_vq, :primary_gp,
                        :juiced_bucket, :juiced_overall,
                        :proj_avg, :proj_obp, :proj_slg, :proj_ops,
                        :proj_hr_pct, :proj_k_pct, :proj_bb_pct, :proj_hit_pct,
                        :sims
                    )
                    ON CONFLICT (snapshot_date, batter_id, opp_pitcher_id)
                    DO UPDATE SET
                        proj_avg = EXCLUDED.proj_avg,
                        proj_obp = EXCLUDED.proj_obp,
                        proj_slg = EXCLUDED.proj_slg,
                        proj_ops = EXCLUDED.proj_ops,
                        proj_hr_pct = EXCLUDED.proj_hr_pct,
                        proj_k_pct = EXCLUDED.proj_k_pct,
                        proj_bb_pct = EXCLUDED.proj_bb_pct,
                        proj_hit_pct = EXCLUDED.proj_hit_pct,
                        juiced_plus_vs_bucket = EXCLUDED.juiced_plus_vs_bucket,
                        juiced_plus_overall = EXCLUDED.juiced_plus_overall,
                        team_abbrev = EXCLUDED.team_abbrev,
                        opp_goose_plus = EXCLUDED.opp_goose_plus,
                        created_at = NOW()
                """), {
                    "snap_date": target_date,
                    "game_pk": game['game_pk'],
                    "batter_id": batter_id,
                    "batter_name": batter_name,
                    "bat_side": sim.get('bat_side', 'R'),
                    "team_abbrev": team_abbrev,
                    "pitcher_id": pitcher_id,
                    "pitcher_name": pitcher_name,
                    "pitcher_hand": pitcher_hand,
                    "opp_goose": float(opp_goose) if opp_goose else None,
                    "primary_pt": primary_pitch['pitch_type_code'] if primary_pitch else None,
                    "primary_vq": primary_pitch['velo_quintile'] if primary_pitch else None,
                    "primary_gp": float(primary_pitch['goose_plus']) if primary_pitch and primary_pitch.get('goose_plus') else None,
                    "juiced_bucket": float(juiced_bucket) if juiced_bucket is not None else None,
                    "juiced_overall": float(juiced_overall) if juiced_overall is not None else None,
                    "proj_avg": sim['avg'],
                    "proj_obp": sim['obp'],
                    "proj_slg": sim['slg'],
                    "proj_ops": sim['ops'],
                    "proj_hr_pct": sim['hr_pct'],
                    "proj_k_pct": sim['k_pct'],
                    "proj_bb_pct": sim['bb_pct'],
                    "proj_hit_pct": sim['hit_pct'],
                    "sims": N_SIMS_PER_BATTER,
                })
                stored += 1

            db.commit()
            print(f"    Committed. Running total: {stored}")

    print(f"\n=== Done. Total stored: {stored} batter projections ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=str(date.today()))
    parser.add_argument("--pitchers-only", action="store_true")
    parser.add_argument("--batters-only", action="store_true")
    args = parser.parse_args()
    db = SessionLocal()
    try:
        if not args.batters_only:
            build_daily_projections(args.date, db)
        if not args.pitchers_only:
            build_daily_batter_projections(args.date, db)
    finally:
        db.close()
