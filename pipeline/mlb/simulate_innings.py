"""
Inning simulator — projects final scores using actual lineups.

Method:
1. Pre-load all pitcher/batter data once before simulations start
2. For each game simulation, run 9 innings with base-state transitions
3. Track runs scored per inning across N game simulations
4. Return projected score, win probabilities, and over/under distribution

Performance note: all DB lookups are done once before the simulation loop.
Per-PA game state adjustments are intentionally skipped here — they require
per-PA DB queries and would make 500-game sims prohibitively slow (~72k queries).
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import json
import random
from collections import Counter
from database import SessionLocal
from sqlalchemy import text
from mlb.simulate import (
    load_pitcher_count_mix,
    load_league_count_mix,
    load_pitcher_overall_mix,
    load_batter_tends,
    load_goose3_adjustments,
    load_pk_phr_adjustments,
    load_juiced2_batter_adjustments,
    load_weather_modifier,
    simulate_pa,
)

N_GAME_SIMS = 500
N_INNINGS = 9


# ── DATA PRE-LOADING ─────────────────────────────────────────────────────────

def _preload_pitcher(pitcher_id: int, db) -> dict:
    """Load all pitch data for one pitcher keyed by bat_side."""
    data = {}
    for side in ('R', 'L'):
        adj = load_goose3_adjustments(pitcher_id, side, db)
        if not adj:
            adj = load_pk_phr_adjustments(pitcher_id, side, db)
        data[side] = {
            'count_mix': load_pitcher_count_mix(pitcher_id, side, db),
            'fallback_mix': load_pitcher_overall_mix(pitcher_id, side, db),
            'adj': adj,
        }
    return data


def _preload_batter(batter_id: int, db) -> dict:
    bat_side = db.execute(text("""
        SELECT bat_side FROM mlb.at_bats
        WHERE batter_id = :bid AND bat_side IS NOT NULL LIMIT 1
    """), {"bid": batter_id}).scalar() or 'R'
    return {
        'bat_side': bat_side,
        'tends': load_batter_tends(batter_id, db),
        'juiced2_adj': load_juiced2_batter_adjustments(batter_id, db),
    }


def load_lineup(game_pk: int, side: str, db) -> tuple:
    """
    Return (list_of_mlbam_ids, opposing_pitcher_id) for home or away.
    """
    row = db.execute(text("""
        SELECT home_lineup, away_lineup, home_pitcher_id, away_pitcher_id
        FROM mlb.game_lineups WHERE game_pk = :gp
    """), {"gp": game_pk}).mappings().first()
    if not row:
        return [], None

    lineup_data = row[f'{side}_lineup']
    if isinstance(lineup_data, str):
        lineup_data = json.loads(lineup_data)

    opp_side = 'away' if side == 'home' else 'home'
    pitcher_id = row[f'{opp_side}_pitcher_id']
    batter_ids = [p['mlbam_id'] for p in (lineup_data or [])]
    return batter_ids, pitcher_id


# ── BASE STATE TRANSITIONS ───────────────────────────────────────────────────

def resolve_base_state(outcome: str, on_first: bool, on_second: bool,
                        on_third: bool, outs: int) -> tuple:
    """
    Apply outcome to current base state. Returns ((f,s,t,outs), runs_scored).
    """
    runs = 0

    if outcome in ('strikeout', 'field_out'):
        return (on_first, on_second, on_third, outs + 1), 0

    if outcome == 'walk':
        if on_first and on_second and on_third:
            runs = 1
            return (True, True, True, outs), runs
        if on_first and on_second:
            return (True, True, True, outs), 0
        if on_first:
            return (True, True, on_third, outs), 0
        return (True, on_second, on_third, outs), 0

    if outcome == 'single':
        if on_third:
            runs += 1
        new_third = False
        if on_second:
            if random.random() < 0.5:
                runs += 1
            else:
                new_third = True
        new_second = False
        if on_first:
            if random.random() < 0.3:
                new_third = True
            else:
                new_second = True
        return (True, new_second, new_third, outs), runs

    if outcome == 'double':
        if on_third:
            runs += 1
        if on_second:
            runs += 1
        new_third = False
        if on_first:
            if random.random() < 0.5:
                runs += 1
            else:
                new_third = True
        return (False, True, new_third, outs), runs

    if outcome == 'triple':
        runs = int(on_first) + int(on_second) + int(on_third)
        return (False, False, True, outs), runs

    if outcome == 'home_run':
        runs = 1 + int(on_first) + int(on_second) + int(on_third)
        return (False, False, False, outs), runs

    return (on_first, on_second, on_third, outs + 1), 0


# ── HALF-INNING SIMULATOR ────────────────────────────────────────────────────

def simulate_half_inning(pitcher_data: dict, batter_cache: dict,
                          lineup: list, lineup_pos: int,
                          lg_mix: dict, weather_mult: float = 1.0) -> tuple:
    """
    Simulate one half-inning using pre-loaded data.
    Returns (runs_scored, new_lineup_pos).
    """
    outs = runs = 0
    on_first = on_second = on_third = False
    pos = lineup_pos

    while outs < 3:
        batter_id = lineup[pos % len(lineup)]
        pos += 1

        batter = batter_cache.get(batter_id, {})
        bat_side = batter.get('bat_side', 'R')
        side_data = pitcher_data.get(bat_side, pitcher_data.get('R', {}))

        pa = simulate_pa(
            side_data.get('count_mix', {}),
            lg_mix,
            side_data.get('fallback_mix', []),
            batter.get('tends', {}),
            record_pitches=False,
            goose2_adj=side_data.get('adj') or None,
            juiced2_adj=batter.get('juiced2_adj') or None,
            weather_hr_mult=weather_mult,
        )
        outcome = pa.get('result', 'field_out')

        (on_first, on_second, on_third, outs), pa_runs = resolve_base_state(
            outcome, on_first, on_second, on_third, outs
        )
        runs += pa_runs

    return runs, pos % len(lineup)


# ── GAME SIMULATOR ───────────────────────────────────────────────────────────

def simulate_game(home_pitcher_data: dict, away_pitcher_data: dict,
                   home_lineup: list, away_lineup: list,
                   batter_cache: dict, lg_mix: dict,
                   weather_mult: float = 1.0) -> dict:
    """Simulate a full 9-inning game. Returns inning-by-inning runs."""
    home_runs_by_inning = []
    away_runs_by_inning = []
    home_pos = away_pos = 0

    for _ in range(N_INNINGS):
        # Away bats top (vs home pitcher)
        away_runs, away_pos = simulate_half_inning(
            home_pitcher_data, batter_cache, away_lineup, away_pos, lg_mix, weather_mult
        )
        away_runs_by_inning.append(away_runs)

        # Home bats bottom (vs away pitcher)
        home_runs, home_pos = simulate_half_inning(
            away_pitcher_data, batter_cache, home_lineup, home_pos, lg_mix, weather_mult
        )
        home_runs_by_inning.append(home_runs)

    total_home = sum(home_runs_by_inning)
    total_away = sum(away_runs_by_inning)
    return {
        'home_runs': total_home,
        'away_runs': total_away,
        'home_by_inning': home_runs_by_inning,
        'away_by_inning': away_runs_by_inning,
        'home_win': total_home > total_away,
    }


# ── MAIN ENTRY POINT ─────────────────────────────────────────────────────────

def run_game_simulation(game_pk: int, db, n_sims: int = N_GAME_SIMS) -> dict:
    """
    Run N simulations for a game, return aggregated results.
    All DB lookups are done once before the simulation loop.
    """
    home_lineup, away_pitcher_id = load_lineup(game_pk, 'home', db)
    away_lineup, home_pitcher_id = load_lineup(game_pk, 'away', db)

    if not home_lineup or not away_lineup:
        return {'error': 'Lineup not yet posted'}
    if not home_pitcher_id or not away_pitcher_id:
        return {'error': 'Probable pitcher not set'}

    print(f"  Pre-loading data for {len(set(home_lineup + away_lineup))} batters "
          f"and 2 pitchers...")

    lg_mix = load_league_count_mix(db)
    weather_mult = load_weather_modifier(game_pk, db)

    home_pitcher_data = _preload_pitcher(home_pitcher_id, db)
    away_pitcher_data = _preload_pitcher(away_pitcher_id, db)

    all_batter_ids = list(set(home_lineup + away_lineup))
    batter_cache = {bid: _preload_batter(bid, db) for bid in all_batter_ids}

    print(f"  Running {n_sims} game simulations...")

    home_wins = 0
    total_home_runs = 0
    total_away_runs = 0
    home_inning_totals = [0] * N_INNINGS
    away_inning_totals = [0] * N_INNINGS
    run_totals = []

    for _ in range(n_sims):
        result = simulate_game(
            home_pitcher_data, away_pitcher_data,
            home_lineup, away_lineup,
            batter_cache, lg_mix, weather_mult,
        )
        home_wins += int(result['home_win'])
        total_home_runs += result['home_runs']
        total_away_runs += result['away_runs']
        run_totals.append(result['home_runs'] + result['away_runs'])
        for i in range(N_INNINGS):
            home_inning_totals[i] += result['home_by_inning'][i]
            away_inning_totals[i] += result['away_by_inning'][i]

    avg_home = round(total_home_runs / n_sims, 2)
    avg_away = round(total_away_runs / n_sims, 2)
    avg_total = round(sum(run_totals) / n_sims, 2)
    home_win_pct = round(home_wins / n_sims * 100, 1)

    run_dist = Counter(run_totals)
    over_under = {}
    for line in (6.5, 7.5, 8.5, 9.5, 10.5):
        over_pct = sum(v for k, v in run_dist.items() if k > line) / n_sims * 100
        over_under[str(line)] = round(over_pct, 1)

    return {
        'game_pk': game_pk,
        'n_sims': n_sims,
        'home_pitcher_id': home_pitcher_id,
        'away_pitcher_id': away_pitcher_id,
        'proj_home_runs': avg_home,
        'proj_away_runs': avg_away,
        'proj_total_runs': avg_total,
        'home_win_pct': home_win_pct,
        'away_win_pct': round(100 - home_win_pct, 1),
        'over_under': over_under,
        'home_by_inning': [round(t / n_sims, 2) for t in home_inning_totals],
        'away_by_inning': [round(t / n_sims, 2) for t in away_inning_totals],
    }


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--game-pk', type=int, required=True)
    parser.add_argument('--n-sims', type=int, default=500)
    args = parser.parse_args()

    db = SessionLocal()
    try:
        print(f'Simulating game {args.game_pk}...')
        result = run_game_simulation(args.game_pk, db, args.n_sims)
        import json as _json
        print(_json.dumps(result, indent=2))
    finally:
        db.close()
