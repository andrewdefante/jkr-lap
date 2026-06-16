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
    load_pk_phr_adjustments,
    load_pitcher_zone_rate,
    load_juiced2_batter_adjustments,
    load_team_defense,
    simulate_pa,
    aggregate_outcomes,
    LEAGUE_WHIFF,
    LEAGUE_CHASE,
    LEAGUE_HR_RATE,
)

N_SIMS_PER_BATTER = 500
K_PCT_CALIBRATION = 0.830  # fallback when no calibration row exists in DB
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

def get_schedule(target_date: str, include_final: bool = False) -> list:
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
            if not include_final and status in ('Final', 'Game Over'):
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


def get_pitcher_avg_ip(pitcher_id: int, db) -> float:
    """Pitcher's avg IP per start in 2026. Defaults to 5.0 if insufficient data."""
    result = db.execute(text("""
        SELECT AVG(bp.innings_pitched)
        FROM mlb.boxscore_pitching bp
        JOIN mlb.games g ON g.game_pk = bp.game_pk
        WHERE bp.player_id = :pid
        AND g.season = 2026 AND g.game_type = 'R'
        AND bp.games_started = 1
    """), {"pid": pitcher_id}).scalar()
    if result and float(result) >= 1.0:
        return min(float(result), 9.0)
    return 5.0  # league avg starter ~5.1 inn


def get_pitcher_season_k_pct(pitcher_id: int, db) -> tuple:
    """
    Returns (season_k_pct, batters_faced) for blending with sim K%.
    Uses 2026 at_bats data for the most accurate current-season rate.
    Returns (None, 0) if fewer than 30 BF.
    """
    result = db.execute(text("""
        SELECT
            AVG(CASE WHEN ab.event IN ('Strikeout', 'Strikeout - DP')
                THEN 1.0 ELSE 0.0 END) as k_pct,
            COUNT(*) as bf
        FROM mlb.at_bats ab
        JOIN mlb.games g ON g.game_pk = ab.game_pk
        WHERE ab.pitcher_id = :pid
        AND g.season = 2026 AND g.game_type = 'R'
        AND ab.event NOT IN ('Runner Out','Caught Stealing 2B',
            'Caught Stealing 3B','Wild Pitch','Passed Ball')
        HAVING COUNT(*) >= 30
    """), {"pid": pitcher_id}).mappings().first()
    if result:
        return float(result['k_pct']), int(result['bf'])
    return None, 0


def get_pitcher_pa_per_inn(pitcher_id: int, db) -> float:
    """
    Pitcher's actual PA faced per inning in 2026.
    Reflects walk/hit tendency — pitchers who allow more baserunners
    face more batters per inning.
    Defaults to 4.1 (MLB average) if insufficient data.
    """
    result = db.execute(text("""
        SELECT
            COUNT(ab.at_bat_index)::float /
            NULLIF(SUM(bp.innings_pitched::float), 0)
        FROM mlb.boxscore_pitching bp
        JOIN mlb.games g ON g.game_pk = bp.game_pk
        JOIN mlb.at_bats ab ON ab.game_pk = bp.game_pk
            AND ab.pitcher_id = bp.player_id
        WHERE bp.player_id = :pid
        AND g.season = 2026 AND g.game_type = 'R'
        AND bp.games_started = 1
        AND ab.event NOT IN ('Runner Out','Caught Stealing 2B',
            'Caught Stealing 3B','Wild Pitch','Passed Ball')
    """), {"pid": pitcher_id}).scalar()
    if result and 2.5 <= float(result) <= 6.0:
        return round(float(result), 2)
    return 4.1  # MLB average PA per inning


def get_lineup_k_factor(batter_ids: list, db) -> tuple:
    """Lineup K% vs league avg (25.1%), dampened 50%, clamped [0.80, 1.25].
    Returns (k_factor, raw_lineup_k_rate) tuple.
    """
    if not batter_ids:
        return 1.0, 0.224
    result = db.execute(text("""
        SELECT SUM(bb.strikeouts)::float / NULLIF(SUM(bb.at_bats), 0)
        FROM mlb.boxscore_batting bb
        JOIN mlb.games g ON g.game_pk = bb.game_pk
        WHERE bb.player_id = ANY(:bids)
        AND g.season = 2026 AND g.game_type = 'R'
        HAVING SUM(bb.at_bats) >= 50
    """), {"bids": batter_ids}).scalar()
    if not result:
        return 1.0, 0.224
    LEAGUE_K_PCT = 0.251
    raw_k = float(result)
    raw_ratio = raw_k / LEAGUE_K_PCT
    clamped = max(0.80, min(1.25, raw_ratio))
    dampened = 1.0 + (clamped - 1.0) * 0.5
    return round(dampened, 3), round(raw_k, 3)


def load_calibration_factors(db) -> dict:
    """
    Load the most recent calibration factors from mlb.projection_calibration.
    Falls back to hardcoded defaults if the table is empty or missing.
    """
    try:
        row = db.execute(text("""
            SELECT k_pct_calibration, hr_rate_multiplier
            FROM mlb.projection_calibration
            ORDER BY computed_at DESC
            LIMIT 1
        """)).mappings().first()
        if row:
            return {
                "k_pct_calibration": float(row["k_pct_calibration"]),
                "hr_rate_multiplier": float(row["hr_rate_multiplier"]),
            }
    except Exception:
        pass
    return {"k_pct_calibration": K_PCT_CALIBRATION, "hr_rate_multiplier": 1.0}


def get_acwr_ip_factor(pitcher_id: int, db) -> float:
    """IP multiplier from ACWR fatigue signal. Returns 1.0 gracefully when table is empty."""
    try:
        acwr = db.execute(text("""
            SELECT acwr FROM mlb.player_workload
            WHERE player_id = :pid AND player_type = 'pitcher'
            ORDER BY calc_date DESC LIMIT 1
        """), {"pid": pitcher_id}).scalar()
        if acwr is None:
            return 1.0
        acwr = float(acwr)
        if acwr > 1.5:
            return 0.70
        elif acwr > 1.3:
            return 0.85
        elif acwr < 0.8:
            return 1.05
        return 1.0
    except Exception:
        return 1.0


def simulate_pitcher_vs_batters(pitcher_id: int, batters: list, db,
                                k_calibration: float = K_PCT_CALIBRATION,
                                hr_multiplier: float = 1.0,
                                opp_team_id: int = None) -> dict:
    """Run N_SIMS_PER_BATTER simulations for each batter using combined Goose+1/2 + Stuff Score."""
    lg_mix = load_league_count_mix(db)
    zone_rate = load_pitcher_zone_rate(pitcher_id, db)
    defense = load_team_defense(opp_team_id, db) if opp_team_id else {}
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
        pitcher_adj = load_pk_phr_adjustments(pitcher_id, bat_side, db)
        juiced2_adj = load_juiced2_batter_adjustments(batter_id, db)

        if not fallback_mix and not count_mix:
            continue

        for _ in range(N_SIMS_PER_BATTER):
            pa = simulate_pa(count_mix, lg_mix, fallback_mix, batter_tends,
                             record_pitches=False,
                             goose2_adj=pitcher_adj if pitcher_adj else None,
                             juiced2_adj=juiced2_adj if juiced2_adj else None,
                             zone_rate=zone_rate,
                             defense=defense if defense else None)
            all_results.append(pa)

    if not all_results:
        return {}

    summary = aggregate_outcomes(all_results, len(all_results))

    return {
        "proj_k_pct": round(summary["k_pct"] * k_calibration, 2),
        "proj_hr_pct": round(summary["hr_pct"] * hr_multiplier, 2),
        "proj_hit_pct": summary["hit_pct"],
        "proj_ops": summary["ops"],
        "proj_bb_pct": summary["bb_pct"],
        "total_sims": len(all_results),
    }


def build_daily_projections(target_date: str, db, include_final: bool = False):
    print(f"\n=== Building Daily Projections — {target_date} ===")

    calib = load_calibration_factors(db)
    k_calib  = calib["k_pct_calibration"]
    hr_mult  = calib["hr_rate_multiplier"]
    print(f"  Calibration: k_pct={k_calib:.4f} hr_mult={hr_mult:.4f}")

    games = get_schedule(target_date, include_final=include_final)
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
            used_posted_lineup = bool(posted_lineup and len(posted_lineup) >= 7)
            if used_posted_lineup:
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

            proj = simulate_pitcher_vs_batters(pitcher_id, batters, db,
                                               k_calibration=k_calib,
                                               hr_multiplier=hr_mult,
                                               opp_team_id=opp_team_id)
            if not proj:
                continue

            avg_ip = get_pitcher_avg_ip(pitcher_id, db)
            lineup_k, lineup_raw_k = get_lineup_k_factor(batter_ids, db)
            acwr_factor = get_acwr_ip_factor(pitcher_id, db)
            proj_ip = round(avg_ip * acwr_factor, 2)
            pa_per_inn = get_pitcher_pa_per_inn(pitcher_id, db)

            # Per-lineup K% calibration: scale by how much this lineup Ks
            # relative to league avg (0.224), clamped to avoid extremes
            lineup_k_calib = k_calib * max(0.60, min(1.30, lineup_raw_k / 0.224))
            proj["proj_k_pct"] = round(proj["proj_k_pct"] / k_calib * lineup_k_calib, 2)

            # Blend simulated K% with actual 2026 season K%.
            # Sim converges toward mean and under-differentiates elite K pitchers.
            # Blend weight grows with sample size, caps at 0.60 at 300+ BF.
            season_k_pct, bf = get_pitcher_season_k_pct(pitcher_id, db)
            if season_k_pct is not None:
                blend_weight = min(0.60, bf / 300)
                sim_k_pct = proj["proj_k_pct"]
                blended_k = (1 - blend_weight) * (sim_k_pct / 100) + blend_weight * season_k_pct
                proj["proj_k_pct"] = round(blended_k * 100, 2)
                print(f"    K% blend: sim={sim_k_pct:.1f}% actual={season_k_pct*100:.1f}% "
                      f"bf={bf} w={blend_weight:.2f} → {proj['proj_k_pct']:.1f}%")

            proj["proj_ks_6inn"] = round(proj["proj_k_pct"] / 100 * proj_ip * pa_per_inn * lineup_k, 1)

            # F5 projections — cap at 5 innings regardless of pitcher avg IP
            f5_ip = min(proj_ip, 5.0)
            proj["proj_ip_f5"] = round(f5_ip, 1)
            proj["proj_ks_f5"] = round(proj["proj_k_pct"] / 100 * f5_ip * pa_per_inn * lineup_k, 1)
            # ER F5: use proj_ops as proxy for run scoring (higher OPS allowed = more ER)
            # League avg ER/9 ~4.5, scale by pitcher quality
            lg_er_per_inn = 0.50  # ~4.5 ER/9
            ops_factor = (proj.get("proj_ops", 0.700) / 0.700)  # scale vs league avg OPS
            proj["proj_er_f5"] = round(lg_er_per_inn * f5_ip * ops_factor, 1)
            print(f"    IP={proj_ip} pa/inn={pa_per_inn:.2f} lineup_k={lineup_k:.3f} raw_k={lineup_raw_k:.3f} calib={lineup_k_calib:.3f} → proj_ks={proj['proj_ks_6inn']}")

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
                    home_team_abbrev,
                    goose_plus, bapv_plus,
                    proj_k_pct, proj_hr_pct, proj_hit_pct, proj_ops,
                    proj_ks_6inn, proj_bb_pct, proj_ks_f5, proj_er_f5, proj_ip_f5,
                    batters_simulated, simulations_per_batter, batter_ids,
                    proj_ks_locked, proj_ks_f5_locked, proj_k_pct_locked, locked_at
                ) VALUES (
                    :snap_date, :game_pk, :pitcher_id, :pitcher_name,
                    :pitcher_hand, :team_abbrev, :opp_abbrev, :opp_team_id,
                    :home_team_abbrev,
                    :goose_plus, :bapv_plus,
                    :proj_k_pct, :proj_hr_pct, :proj_hit_pct, :proj_ops,
                    :proj_ks_6inn, :proj_bb_pct, :proj_ks_f5, :proj_er_f5, :proj_ip_f5,
                    :batters_sim, :sims_per, :batter_ids,
                    :proj_ks_locked, :proj_ks_f5_locked, :proj_k_pct_locked, NOW()
                )
                ON CONFLICT (snapshot_date, pitcher_id) DO UPDATE SET
                    team_abbrev = EXCLUDED.team_abbrev,
                    opp_abbrev = EXCLUDED.opp_abbrev,
                    opp_team_id = EXCLUDED.opp_team_id,
                    home_team_abbrev = EXCLUDED.home_team_abbrev,
                    proj_k_pct = EXCLUDED.proj_k_pct,
                    proj_hr_pct = EXCLUDED.proj_hr_pct,
                    proj_hit_pct = EXCLUDED.proj_hit_pct,
                    proj_ops = EXCLUDED.proj_ops,
                    proj_ks_6inn = EXCLUDED.proj_ks_6inn,
                    proj_bb_pct = EXCLUDED.proj_bb_pct,
                    proj_ks_f5 = EXCLUDED.proj_ks_f5,
                    proj_er_f5 = EXCLUDED.proj_er_f5,
                    proj_ip_f5 = EXCLUDED.proj_ip_f5,
                    batters_simulated = EXCLUDED.batters_simulated,
                    goose_plus = EXCLUDED.goose_plus,
                    bapv_plus = EXCLUDED.bapv_plus,
                    proj_ks_locked = COALESCE(mlb.daily_projections.proj_ks_locked, EXCLUDED.proj_ks_locked),
                    proj_ks_f5_locked = COALESCE(mlb.daily_projections.proj_ks_f5_locked, EXCLUDED.proj_ks_f5_locked),
                    proj_k_pct_locked = COALESCE(mlb.daily_projections.proj_k_pct_locked, EXCLUDED.proj_k_pct_locked),
                    locked_at = COALESCE(mlb.daily_projections.locked_at, EXCLUDED.locked_at),
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
                "home_team_abbrev": game['home_abbrev'],
                "goose_plus": float(goose) if goose else None,
                "bapv_plus": float(bapv) if bapv else None,
                "proj_k_pct": proj["proj_k_pct"],
                "proj_hr_pct": proj["proj_hr_pct"],
                "proj_hit_pct": proj["proj_hit_pct"],
                "proj_ops": proj["proj_ops"],
                "proj_ks_6inn": proj["proj_ks_6inn"],
                "proj_ks_locked": proj.get("proj_ks_6inn") if used_posted_lineup else None,
                "proj_ks_f5_locked": proj.get("proj_ks_f5") if used_posted_lineup else None,
                "proj_k_pct_locked": proj.get("proj_k_pct") if used_posted_lineup else None,
                "proj_bb_pct": proj["proj_bb_pct"],
                "proj_ks_f5": proj.get("proj_ks_f5", proj["proj_ks_6inn"]),
                "proj_er_f5": proj.get("proj_er_f5", 2.0),
                "proj_ip_f5": proj.get("proj_ip_f5", 5.0),
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
                        proj_tb, simulations
                    ) VALUES (
                        :snap_date, :game_pk, :batter_id, :batter_name,
                        :bat_side, :team_abbrev, :pitcher_id, :pitcher_name,
                        :pitcher_hand, :opp_goose,
                        :primary_pt, :primary_vq, :primary_gp,
                        :juiced_bucket, :juiced_overall,
                        :proj_avg, :proj_obp, :proj_slg, :proj_ops,
                        :proj_hr_pct, :proj_k_pct, :proj_bb_pct, :proj_hit_pct,
                        :proj_tb, :sims
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
                        proj_tb = EXCLUDED.proj_tb,
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
                    "proj_tb": round(float(sim['slg']) * 3.5, 2) if sim.get('slg') else None,
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
    parser.add_argument("--include-final", action="store_true",
                        help="Include already-final games (for retroactive projections)")
    args = parser.parse_args()
    db = SessionLocal()
    try:
        if not args.batters_only:
            build_daily_projections(args.date, db, include_final=args.include_final)
        if not args.pitchers_only:
            build_daily_batter_projections(args.date, db)
    finally:
        db.close()
