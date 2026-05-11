"""
Shared at-bat simulation engine for Joker Lap.

Pitch selection uses count-specific mix from mlb.pitcher_count_mix.
Fallback chain:
  1. Pitcher's actual count mix (pitcher_count_mix) — season 2026 then 2025
  2. League average count mix for that count
  3. Pitcher's overall platoon mix (pitcher_mix_profile)
"""
import random
from collections import defaultdict
from sqlalchemy.orm import Session
from sqlalchemy import text

# ── CONSTANTS ────────────────────────────────────────────────────────────────
LEAGUE_WHIFF = 0.267
LEAGUE_CHASE = 0.310
LEAGUE_HR_RATE = 0.034
LEAGUE_AVG_BATTER_WHIFF = 0.267

FASTBALL_TYPES = {'FF', 'SI', 'FC'}
OFFSPEED_TYPES = {'CH', 'FS'}
BREAKING_TYPES = {'SL', 'ST', 'CU', 'KC'}

MIN_COUNT_PITCHES = 15

# Module-level cache for league mix (computed once per process)
_league_mix_cache = None

BASE_CONTACT = {
    'single': 0.155,
    'double': 0.050,
    'triple': 0.005,
    'home_run': LEAGUE_HR_RATE,
    'field_out': 0.756,
}

RUN_VALUES = {
    'single': 0.47, 'double': 0.78, 'triple': 1.07,
    'home_run': 1.40, 'walk': 0.33,
    'strikeout': -0.08, 'field_out': -0.10
}


# ── DATA LOADERS ─────────────────────────────────────────────────────────────

def load_pitcher_count_mix(pitcher_id: int, bat_side: str, db: Session) -> dict:
    """
    Load pitcher's count-specific mix from pitcher_count_mix table.
    Returns dict keyed by (balls, strikes) → list of pitch dicts.
    Falls back to league average for counts with insufficient data.
    """
    sql = text("""
        SELECT balls, strikes, pitch_type_code,
               usage_pct, whiff_rate, csw_rate, avg_velo, pitches
        FROM mlb.pitcher_count_mix
        WHERE pitcher_id = :pid
        AND bat_side = :side
        AND season IN (2026, 2025)
        ORDER BY season DESC, balls, strikes, usage_pct DESC
    """)
    rows = db.execute(sql, {"pid": pitcher_id, "side": bat_side}).mappings().all()

    count_mix = {}
    seen = set()
    for r in rows:
        key = (int(r['balls']), int(r['strikes']))
        pt = r['pitch_type_code']
        entry_key = (key, pt)
        if entry_key not in seen:
            seen.add(entry_key)
            if key not in count_mix:
                count_mix[key] = []
            count_mix[key].append({
                "pitch_type": pt,
                "prob": float(r['usage_pct']),
                "whiff_rate": float(r['whiff_rate'] or LEAGUE_WHIFF),
                "csw_rate": float(r['csw_rate'] or 0.28),
                "avg_velo": float(r['avg_velo'] or 90.0),
                "pitches": int(r['pitches']),
            })

    return count_mix


def load_league_count_mix(db: Session) -> dict:
    """
    Load league average pitch mix by count.
    Used as fallback when pitcher has insufficient count data.
    Cached for process lifetime — the 2025 aggregate never changes during a session.
    """
    global _league_mix_cache
    if _league_mix_cache is not None:
        return _league_mix_cache

    sql = text("""
        SELECT balls, strikes, pitch_type_code,
               SUM(pitches) as total_pitches,
               SUM(pitches * usage_pct) / NULLIF(SUM(pitches), 0) as usage_pct,
               SUM(pitches * COALESCE(whiff_rate, 0)) / NULLIF(SUM(pitches), 0) as whiff_rate,
               SUM(pitches * COALESCE(csw_rate, 0)) / NULLIF(SUM(pitches), 0) as csw_rate,
               SUM(pitches * COALESCE(avg_velo, 90)) / NULLIF(SUM(pitches), 0) as avg_velo
        FROM mlb.pitcher_count_mix
        WHERE season = 2025
        GROUP BY balls, strikes, pitch_type_code
        HAVING SUM(pitches) >= 100
        ORDER BY balls, strikes, usage_pct DESC
    """)
    rows = db.execute(sql).mappings().all()

    lg_mix = {}
    for r in rows:
        key = (int(r['balls']), int(r['strikes']))
        if key not in lg_mix:
            lg_mix[key] = []
        lg_mix[key].append({
            "pitch_type": r['pitch_type_code'],
            "prob": float(r['usage_pct']),
            "whiff_rate": float(r['whiff_rate'] or LEAGUE_WHIFF),
            "csw_rate": float(r['csw_rate'] or 0.28),
            "avg_velo": float(r['avg_velo'] or 90.0),
            "pitches": int(r['total_pitches']),
        })

    _league_mix_cache = lg_mix
    return lg_mix


def load_pitcher_overall_mix(pitcher_id: int, bat_side: str, db: Session) -> list:
    """Load pitcher's overall platoon mix as final fallback."""
    sql = text("""
        SELECT pitch_type_code, usage_pct, whiff_rate, csw_rate, avg_velo
        FROM mlb.pitcher_mix_profile
        WHERE pitcher_id = :pid AND bat_side = :side
        AND season IN (2026, 2025)
        ORDER BY season DESC, usage_pct DESC
    """)
    rows = db.execute(sql, {"pid": pitcher_id, "side": bat_side}).mappings().all()

    if not rows:
        return []

    total = sum(float(r['usage_pct']) for r in rows)
    return [{
        "pitch_type": r['pitch_type_code'],
        "prob": float(r['usage_pct']) / max(total, 1.0),
        "whiff_rate": float(r['whiff_rate'] or LEAGUE_WHIFF),
        "csw_rate": float(r['csw_rate'] or 0.28),
        "avg_velo": float(r['avg_velo'] or 90.0),
    } for r in rows]


def load_batter_tends(batter_id: int, db: Session) -> dict:
    """Load batter pitch type tendencies."""
    sql = text("""
        SELECT pitch_type_code, whiff_rate, chase_rate,
               hard_hit_rate, avg_exit_velo, avg_woba_on_contact
        FROM mlb.batter_pitch_type_tendencies
        WHERE batter_id = :bid AND season = 2025
    """)
    rows = db.execute(sql, {"bid": batter_id}).mappings().all()
    return {r['pitch_type_code']: dict(r) for r in rows}


def load_goose2_pitch_adjustments(pitcher_id: int, bat_side: str,
                                   db: Session) -> dict:
    """
    Load Goose+ 2 per-pitch adjustments for simulation.
    Returns dict of pitch_type → adjustment multipliers.
    Dampened 50%: G2+ 120 → 1.10x whiff/CSW boost, G2+ 80 → 0.90x reduction.
    """
    sql = text("""
        SELECT pitch_type_code, goose2_plus, whiff_idx, csw_idx,
               slg_idx, contact_idx, pitches
        FROM mlb.goose2_scores
        WHERE pitcher_id = :pid
        AND season = 2026
        AND bat_side = :side
        AND pitches >= 15
    """)
    rows = db.execute(sql, {"pid": pitcher_id, "side": bat_side}).mappings().all()

    adjustments = {}
    for r in rows:
        pt = r['pitch_type_code']
        g2 = float(r['goose2_plus'] or 100)
        adj = 1.0 + (g2 - 100) / 200
        adjustments[pt] = {
            'whiff_mult': round(adj, 3),
            'csw_mult': round(adj, 3),
            'contact_mult': round(2.0 - adj, 3),
            'goose2_plus': g2,
        }

    return adjustments


def load_juiced2_batter_adjustments(batter_id: int, db: Session) -> dict:
    """
    Load Juiced+ 2 adjustments for batter simulation.
    Returns multipliers for contact outcomes.
    """
    sql = text("""
        SELECT juiced2_plus, quality_adj_idx, ops_idx, slg_idx,
               hard_hit_idx, avg_goose2_faced
        FROM mlb.juiced2_scores
        WHERE batter_id = :bid AND season = 2026
    """)
    row = db.execute(sql, {"bid": batter_id}).mappings().first()
    if not row:
        return {}

    j2 = float(row['juiced2_plus'] or 100)
    adj = 1.0 + (j2 - 100) / 200

    return {
        'ops_mult': round(adj, 3),
        'contact_mult': round(adj, 3),
        'hr_mult': round(adj, 3),
        'juiced2_plus': j2,
        'quality_adj_idx': float(row['quality_adj_idx'] or 100),
    }


def load_stuff_score(pitcher_id: int, bat_side: str, db: Session) -> dict:
    """
    Load Stuff Score per pitch type for simulation adjustments.
    Returns dict of pitch_type → stuff adjustment multiplier.
    """
    sql = text("""
        SELECT pitch_type_code, stuff_score, physical_score,
               location_score, tunnel_score, pitches
        FROM mlb.stuff_scores
        WHERE pitcher_id = :pid AND season = 2026
        AND pitches >= 15
        ORDER BY pitches DESC
    """)
    rows = db.execute(sql, {"pid": pitcher_id}).mappings().all()

    if not rows:
        return {}

    adjustments = {}
    for r in rows:
        pt = r['pitch_type_code']
        stuff = float(r['stuff_score'] or 100)
        adj = 1.0 + (stuff - 100) / 180
        adjustments[pt] = {
            'stuff_score': stuff,
            'whiff_mult': round(adj, 3),
            'csw_mult': round(adj, 3),
            'contact_mult': round(2.0 - adj, 3),
            'physical_score': float(r['physical_score'] or 100),
            'location_score': float(r['location_score'] or 100),
            'tunnel_score': float(r['tunnel_score'] or 100),
        }
    return adjustments


def load_combined_pitcher_adjustments(pitcher_id: int, bat_side: str,
                                       db: Session) -> dict:
    """
    Blend Goose+1, Goose+2, and Stuff Score into a single per-pitch adjustment dict.
    Weights: Stuff 35% / Goose+2 35% / Goose+1 30%.
    Falls back gracefully when any signal is missing.
    """
    goose2_adj = load_goose2_pitch_adjustments(pitcher_id, bat_side, db)
    stuff_adj = load_stuff_score(pitcher_id, bat_side, db)

    goose1_sql = text("""
        SELECT goose_plus, goose_plus_vs_lhh, goose_plus_vs_rhh
        FROM mlb.goose_overall
        WHERE pitcher_id = :pid AND season = 2026 AND game_pk IS NULL
    """)
    goose1 = db.execute(goose1_sql, {"pid": pitcher_id}).mappings().first()
    goose1_val = None
    if goose1:
        if bat_side == 'L' and goose1['goose_plus_vs_lhh']:
            goose1_val = float(goose1['goose_plus_vs_lhh'])
        elif bat_side == 'R' and goose1['goose_plus_vs_rhh']:
            goose1_val = float(goose1['goose_plus_vs_rhh'])
        else:
            goose1_val = float(goose1['goose_plus'] or 100)

    all_pitch_types = set(goose2_adj.keys()) | set(stuff_adj.keys())
    if not all_pitch_types:
        return {}

    combined = {}
    for pt in all_pitch_types:
        g2 = goose2_adj.get(pt, {})
        st = stuff_adj.get(pt, {})

        g1_adj = 1.0 + (goose1_val - 100) / 200 if goose1_val else 1.0

        g2_whiff = g2.get('whiff_mult', 1.0)
        st_whiff = st.get('whiff_mult', 1.0)
        g2_contact = g2.get('contact_mult', 1.0)
        st_contact = st.get('contact_mult', 1.0)

        w_g2 = 0.35 if g2 else 0.0
        w_st = 0.35 if st else 0.0
        w_g1 = 0.30 if goose1_val is not None else 0.0
        total_w = w_g2 + w_st + w_g1

        if total_w == 0:
            combined[pt] = {'whiff_mult': 1.0, 'contact_mult': 1.0}
            continue

        w_g2 /= total_w
        w_st /= total_w
        w_g1 /= total_w

        whiff_mult = g2_whiff * w_g2 + st_whiff * w_st + g1_adj * w_g1
        contact_mult = g2_contact * w_g2 + st_contact * w_st + (2.0 - g1_adj) * w_g1

        combined[pt] = {
            'whiff_mult': round(whiff_mult, 3),
            'csw_mult': round(whiff_mult, 3),
            'contact_mult': round(contact_mult, 3),
            'goose1': round(goose1_val, 1) if goose1_val else None,
            'goose2': round(g2.get('goose2_plus', 100), 1) if g2 else None,
            'stuff': round(st.get('stuff_score', 100), 1) if st else None,
        }

    return combined


def load_goose3_adjustments(pitcher_id: int, bat_side: str,
                             db: Session) -> dict:
    """
    Load Goose+3 per-pitch adjustments for simulation.
    Single unified signal — replaces separate G1/G2/Stuff blending.
    """
    sql = text("""
        SELECT pitch_type_code, goose3_plus, stuff_score,
               goose2_score, stuff_outcomes_gap, pitches
        FROM mlb.goose3_scores
        WHERE pitcher_id = :pid
        AND season = 2026
        AND bat_side = :side
        AND pitches >= 10
    """)
    rows = db.execute(sql, {"pid": pitcher_id, "side": bat_side}).mappings().all()

    if not rows:
        sql2 = text("""
            SELECT DISTINCT ON (pitch_type_code)
                   pitch_type_code, goose3_plus, stuff_score,
                   goose2_score, stuff_outcomes_gap, pitches / 2 as pitches
            FROM mlb.goose3_scores
            WHERE pitcher_id = :pid AND season = 2026
            AND pitches >= 10
            ORDER BY pitch_type_code, pitches DESC
        """)
        rows = db.execute(sql2, {"pid": pitcher_id}).mappings().all()

    if not rows:
        return {}

    adjustments = {}
    for r in rows:
        pt = r['pitch_type_code']
        g3 = float(r['goose3_plus'] or 100)
        adj = 1.0 + (g3 - 100) / 180
        adjustments[pt] = {
            'whiff_mult': round(adj, 3),
            'csw_mult': round(adj, 3),
            'contact_mult': round(2.0 - adj, 3),
            'goose3_plus': g3,
            'stuff_score': float(r['stuff_score'] or 100),
            'goose2_score': float(r['goose2_score'] or 100),
            'gap': float(r['stuff_outcomes_gap'] or 0),
        }

    return adjustments


# ── PITCH SELECTION ───────────────────────────────────────────────────────────

def normalize_mix(mix: list) -> list:
    """Normalize pitch probabilities to sum to 1.0."""
    total = sum(p['prob'] for p in mix)
    if total <= 0:
        return mix
    return [{**p, 'prob': p['prob'] / total} for p in mix]


def get_mix_for_count(balls: int, strikes: int,
                      count_mix: dict,
                      lg_mix: dict,
                      fallback_mix: list) -> list:
    """
    Get pitch mix for a specific count using fallback chain:
    1. Pitcher count-specific data (if >= MIN_COUNT_PITCHES)
    2. League average count mix
    3. Pitcher overall platoon mix
    """
    key = (balls, strikes)

    pitcher_count = count_mix.get(key, [])
    total_pitches = sum(p['pitches'] for p in pitcher_count)

    if pitcher_count and total_pitches >= MIN_COUNT_PITCHES:
        return normalize_mix(pitcher_count)

    lg_count = lg_mix.get(key, [])
    if lg_count:
        if fallback_mix:
            lg_norm = normalize_mix(lg_count)
            fb_norm = normalize_mix(fallback_mix)

            pitch_types = set(p['pitch_type'] for p in lg_norm) | \
                          set(p['pitch_type'] for p in fb_norm)
            lg_map = {p['pitch_type']: p for p in lg_norm}
            fb_map = {p['pitch_type']: p for p in fb_norm}

            blended = []
            for pt in pitch_types:
                lg_p = lg_map.get(pt, {})
                fb_p = fb_map.get(pt, {})
                prob = (lg_p.get('prob', 0) * 0.60 +
                        fb_p.get('prob', 0) * 0.40)
                if prob > 0:
                    blended.append({
                        "pitch_type": pt,
                        "prob": prob,
                        "whiff_rate": (fb_p.get('whiff_rate') or
                                       lg_p.get('whiff_rate') or LEAGUE_WHIFF),
                        "csw_rate": (fb_p.get('csw_rate') or
                                     lg_p.get('csw_rate') or 0.28),
                        "avg_velo": (fb_p.get('avg_velo') or
                                     lg_p.get('avg_velo') or 90.0),
                    })
            return normalize_mix(blended)
        return normalize_mix(lg_count)

    return normalize_mix(fallback_mix) if fallback_mix else []


def select_pitch(mix: list) -> dict:
    """Randomly select a pitch from the mix."""
    if not mix:
        return {"pitch_type": "FF", "prob": 1.0,
                "whiff_rate": LEAGUE_WHIFF, "csw_rate": 0.28, "avg_velo": 93.0}
    rand = random.random()
    cum = 0.0
    for p in mix:
        cum += p['prob']
        if rand <= cum:
            return p
    return mix[-1]


# ── BATTER OUTCOME ────────────────────────────────────────────────────────────

def get_contact_probs(pitch_type: str, batter_tends: dict) -> dict:
    """Get batter-specific contact outcome probabilities."""
    tend = batter_tends.get(pitch_type, {})
    hh_rate = float(tend.get('hard_hit_rate') or 0.395)
    avg_ev = float(tend.get('avg_exit_velo') or 88.0)
    woba = float(tend.get('avg_woba_on_contact') or 0.380)

    hr_adj = LEAGUE_HR_RATE * (hh_rate / 0.395) * max(0.5, (avg_ev - 85) / 10)
    hr_adj = max(0.01, min(0.15, hr_adj))
    hit_adj = woba / 0.380

    single_p = BASE_CONTACT['single'] * hit_adj
    double_p = BASE_CONTACT['double'] * hit_adj
    triple_p = BASE_CONTACT['triple'] * hit_adj
    out_p = max(0.4, 1.0 - single_p - double_p - triple_p - hr_adj)

    return {
        'home_run': hr_adj,
        'single': single_p,
        'double': double_p,
        'triple': triple_p,
        'field_out': out_p,
    }


def resolve_contact(pitch_type: str, batter_tends: dict) -> str:
    """Resolve a ball-in-play outcome."""
    probs = get_contact_probs(pitch_type, batter_tends)
    rand = random.random()
    cum = 0.0
    for outcome, prob in probs.items():
        cum += prob
        if rand <= cum:
            return outcome
    return 'field_out'


def resolve_contact_v2(pitch_type: str, batter_tends: dict,
                        contact_mult: float = 1.0,
                        hr_mult: float = 1.0) -> str:
    """
    Resolve ball-in-play with Juiced+ 2 batter adjustments.
    contact_mult > 1.0 = better hitter, more likely to get hits.
    hr_mult > 1.0 = better hitter, more likely to hit HR.
    """
    probs = get_contact_probs(pitch_type, batter_tends)

    adjusted = {}
    for outcome, prob in probs.items():
        if outcome == 'home_run':
            adjusted[outcome] = prob * hr_mult
        elif outcome in ('single', 'double', 'triple'):
            adjusted[outcome] = prob * contact_mult
        else:
            adjusted[outcome] = prob

    total = sum(adjusted.values())
    for k in adjusted:
        adjusted[k] /= max(total, 0.001)

    rand = random.random()
    cum = 0.0
    for outcome, prob in adjusted.items():
        cum += prob
        if rand <= cum:
            return outcome
    return 'field_out'


# ── MAIN SIMULATION ───────────────────────────────────────────────────────────

def simulate_pa(count_mix: dict, lg_mix: dict,
                fallback_mix: list, batter_tends: dict,
                record_pitches: bool = False,
                balls_start: int = 0,
                strikes_start: int = 0,
                goose2_adj: dict = None,
                juiced2_adj: dict = None) -> dict:
    """
    Simulate a single plate appearance.
    balls_start/strikes_start: starting count (default 0-0)

    Returns dict with:
        result: str (outcome)
        pitches: list (if record_pitches=True)
        pitch_count: int
    """
    balls = balls_start
    strikes = strikes_start
    pitch_log = [] if record_pitches else None
    pitch_count = 0

    batter_contact_mult = juiced2_adj.get('contact_mult', 1.0) if juiced2_adj else 1.0
    batter_hr_mult = juiced2_adj.get('hr_mult', 1.0) if juiced2_adj else 1.0

    for _ in range(20):
        pitch_count += 1
        mix = get_mix_for_count(balls, strikes, count_mix, lg_mix, fallback_mix)
        selected = select_pitch(mix)
        pt = selected['pitch_type']
        pitcher_whiff = selected['whiff_rate']
        csw = selected['csw_rate']

        if goose2_adj and pt in goose2_adj:
            adj = goose2_adj[pt]
            pitcher_whiff = min(0.95, pitcher_whiff * adj['whiff_mult'])
            csw = min(0.95, csw * adj['csw_mult'])

        in_zone = random.random() < min(csw * 1.5, 0.75)

        tend = batter_tends.get(pt, {})
        batter_whiff = float(tend.get('whiff_rate') or LEAGUE_WHIFF)
        chase = float(tend.get('chase_rate') or LEAGUE_CHASE)
        swing_prob = 0.70 if in_zone else chase

        swings = random.random() < swing_prob

        if record_pitches:
            pitch_log.append({
                "pitch_type": pt,
                "count_before": f"{balls}-{strikes}",
                "in_zone": in_zone,
                "swings": swings,
                "velo": round(selected['avg_velo'], 1),
            })

        if swings:
            whiff_prob = pitcher_whiff * 0.60 + batter_whiff * 0.40
            if random.random() < whiff_prob:
                strikes += 1
                if record_pitches:
                    pitch_log[-1]["outcome"] = "whiff"
                if strikes >= 3:
                    return {"result": "strikeout",
                            "pitches": pitch_log,
                            "pitch_count": pitch_count}
            else:
                foul_prob = 0.40 if strikes < 2 else 0.35
                if random.random() < foul_prob:
                    if strikes < 2:
                        strikes += 1
                    if record_pitches:
                        pitch_log[-1]["outcome"] = "foul"
                else:
                    result = resolve_contact_v2(pt, batter_tends,
                                                batter_contact_mult,
                                                batter_hr_mult)
                    if record_pitches:
                        pitch_log[-1]["outcome"] = result
                    return {"result": result,
                            "pitches": pitch_log,
                            "pitch_count": pitch_count}
        else:
            if in_zone:
                strikes += 1
                if record_pitches:
                    pitch_log[-1]["outcome"] = "called_strike"
                if strikes >= 3:
                    return {"result": "strikeout",
                            "pitches": pitch_log,
                            "pitch_count": pitch_count}
            else:
                balls += 1
                if record_pitches:
                    pitch_log[-1]["outcome"] = "ball"
                if balls >= 4:
                    return {"result": "walk",
                            "pitches": pitch_log,
                            "pitch_count": pitch_count}

    return {"result": "field_out", "pitches": pitch_log, "pitch_count": pitch_count}


def aggregate_outcomes(results: list, n_sims: int) -> dict:
    """Aggregate simulation results into summary stats."""
    outcome_counts = defaultdict(int)
    pa_lengths = []
    for r in results:
        outcome_counts[r['result']] += 1
        pa_lengths.append(r.get('pitch_count', 0))

    hits = (outcome_counts['single'] + outcome_counts['double'] +
            outcome_counts['triple'] + outcome_counts['home_run'])
    total_bases = (outcome_counts['single'] +
                   outcome_counts['double'] * 2 +
                   outcome_counts['triple'] * 3 +
                   outcome_counts['home_run'] * 4)
    ab = n_sims - outcome_counts['walk']

    avg = hits / max(ab, 1)
    slg = total_bases / max(ab, 1)
    obp = (hits + outcome_counts['walk']) / max(n_sims, 1)

    expected_rv = sum(
        RUN_VALUES.get(outcome, 0) * cnt / n_sims
        for outcome, cnt in outcome_counts.items()
    )

    return {
        "avg": round(avg, 3),
        "obp": round(obp, 3),
        "slg": round(slg, 3),
        "ops": round(obp + slg, 3),
        "hit_pct": round(hits / n_sims * 100, 1),
        "hr_pct": round(outcome_counts['home_run'] / n_sims * 100, 1),
        "k_pct": round(outcome_counts['strikeout'] / n_sims * 100, 1),
        "bb_pct": round(outcome_counts['walk'] / n_sims * 100, 1),
        "avg_pitches_per_pa": round(sum(pa_lengths) / max(len(pa_lengths), 1), 1),
        "expected_run_value": round(expected_rv, 3),
        "outcome_counts": dict(outcome_counts),
        "n_simulations": n_sims,
    }


def run_simulation(pitcher_id: int, batter_id: int,
                   n_sims: int, db: Session,
                   record_sample: int = 10,
                   balls_start: int = 0,
                   strikes_start: int = 0) -> dict:
    """
    Full simulation runner. Returns aggregated results + sample PAs.

    Args:
        pitcher_id: MLBAM pitcher ID
        batter_id: MLBAM batter ID
        n_sims: number of simulations
        db: database session
        record_sample: number of sample PAs to record in detail
        balls_start/strikes_start: starting count (default 0-0)
    """
    bat_side = db.execute(text("""
        SELECT bat_side FROM mlb.at_bats
        WHERE batter_id = :bid AND bat_side IS NOT NULL LIMIT 1
    """), {"bid": batter_id}).scalar() or 'R'

    count_mix = load_pitcher_count_mix(pitcher_id, bat_side, db)
    lg_mix = load_league_count_mix(db)
    fallback_mix = load_pitcher_overall_mix(pitcher_id, bat_side, db)
    batter_tends = load_batter_tends(batter_id, db)

    if not fallback_mix and not count_mix:
        return {"error": "No pitcher mix data found"}

    pitcher_adj = load_goose3_adjustments(pitcher_id, bat_side, db)
    juiced2_adj = load_juiced2_batter_adjustments(batter_id, db)

    if pitcher_adj:
        print(f"  Applying Goose+3 adjustments for {len(pitcher_adj)} pitch types")
    if juiced2_adj:
        print(f"  Applying Juiced+ 2 batter adjustment: "
              f"{juiced2_adj.get('juiced2_plus', 100):.1f}")

    results = []
    sample_pas = []

    for i in range(n_sims):
        record = i < record_sample
        pa = simulate_pa(count_mix, lg_mix, fallback_mix,
                         batter_tends, record_pitches=record,
                         balls_start=balls_start, strikes_start=strikes_start,
                         goose2_adj=pitcher_adj if pitcher_adj else None,
                         juiced2_adj=juiced2_adj if juiced2_adj else None)
        results.append(pa)
        if record:
            sample_pas.append(pa)

    summary = aggregate_outcomes(results, n_sims)

    pitch_counts = defaultdict(int)
    for r in results:
        if r.get('pitches'):
            for p in r['pitches']:
                pitch_counts[p['pitch_type']] += 1

    total_pitches = sum(pitch_counts.values())
    pitch_distribution = {
        pt: {
            "count": cnt,
            "pct": round(cnt / max(total_pitches, 1) * 100, 1)
        }
        for pt, cnt in sorted(pitch_counts.items(), key=lambda x: -x[1])
    }

    count_mix_used = {}
    for (b, s), mix in count_mix.items():
        total = sum(p['pitches'] for p in mix)
        if total >= MIN_COUNT_PITCHES:
            count_mix_used[f"{b}-{s}"] = "pitcher_data"
        else:
            count_mix_used[f"{b}-{s}"] = "lg_blend"

    model_context = {}
    for pt, adj in pitcher_adj.items():
        model_context[pt] = {
            'goose3_plus': adj.get('goose3_plus'),
            'stuff_score': adj.get('stuff_score'),
            'goose2_score': adj.get('goose2_score'),
            'gap': adj.get('gap'),
            'whiff_mult': adj.get('whiff_mult'),
        }
    summary['model_context'] = model_context
    summary['juiced2_context'] = juiced2_adj or {}

    return {
        "bat_side": bat_side,
        "results": summary,
        "outcome_distribution": {
            k: {"count": v, "pct": round(v / n_sims * 100, 1)}
            for k, v in sorted(summary["outcome_counts"].items(),
                               key=lambda x: -x[1])
        },
        "pitch_distribution": pitch_distribution,
        "sample_pas": [
            {"result": p["result"],
             "pitch_count": p.get("pitch_count", 0),
             "pitches": p.get("pitches", [])}
            for p in sample_pas
        ],
        "count_mix_source": count_mix_used,
        "data_quality": {
            "has_count_mix": bool(count_mix),
            "count_mix_counts": len(count_mix),
            "has_batter_tends": bool(batter_tends),
            "fallback_pitches": len(fallback_mix),
        }
    }
