"""
pK+ and pHR+ — empirically validated pitcher quality models.

pK+  = whiff_idx(29%) + csw_idx(27%) + contact_idx(27%) + chase_idx(17%)
       Component correlations: whiff 0.473, csw 0.439, contact 0.437, chase 0.281

pHR+ = slg_idx(87%) + hard_hit_idx(13%)
       Predicts HR rate out-of-sample r=-0.215, SLG against r=-0.311

Both use:
- 2026 rolling baselines (same as Goose+2)
- Regression to mean (k=200 pitches)
- Batter-quality-weighted whiff in pK+
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import argparse
import pandas as pd
import numpy as np
from database import SessionLocal
from sqlalchemy import text

pK_WEIGHTS = {
    'whiff':   0.290,  # r=0.473 K rate
    'csw':     0.269,  # r=0.439
    'contact': 0.268,  # r=0.437
    'chase':   0.173,  # r=0.281
}

pHR_WEIGHTS = {
    'slg':      0.868,  # r=-0.311 SLG against
    'hard_hit': 0.132,  # r=-0.047
}

REGRESSION_K = 200
LEAGUE_AVG_WHIFF = 0.267


def regress_to_mean(score: float, n: int, k: int = REGRESSION_K) -> float:
    weight = n / (n + k)
    return round(100 + weight * (score - 100), 1)


def safe_idx(pitcher_val, league_val, invert=False):
    """Index vs league avg — 100 = avg, higher = better for pitcher."""
    if pitcher_val is None or league_val is None or league_val == 0:
        return 100.0
    ratio = float(pitcher_val) / float(league_val)
    if invert:
        return round(100 / ratio, 1) if ratio != 0 else 100.0
    return round(ratio * 100, 1)


def build_pk_phr(score_season: int, baseline_season: int, db):
    print(f"\n=== Building pK+ and pHR+ — {score_season} ===")

    # ── Load baselines ────────────────────────────────────────────────────────
    bl_sql = text("""
        SELECT pitch_type_code, bat_side, pitch_hand,
               contact_pct, whiff_pct, csw_pct,
               slg_against, hard_hit_pct, chase_pct
        FROM mlb.league_pitch_baselines
        WHERE baseline_season = :season
    """)
    bl_rows = db.execute(bl_sql, {"season": baseline_season}).mappings().all()
    baselines = {}
    for r in bl_rows:
        key = (r['pitch_type_code'], r['bat_side'], r['pitch_hand'])
        baselines[key] = dict(r)
    print(f"  Loaded {len(baselines)} baselines")

    # ── Load pitcher stats ────────────────────────────────────────────────────
    pitcher_sql = text("""
        SELECT
            ab.pitcher_id,
            MAX(ab.pitcher_name) as pitcher_name,
            MAX(ab.pitch_hand) as pitch_hand,
            p.pitch_type_code,
            ab.bat_side,
            AVG(CASE WHEN p.call_code IN ('S','W','T','F','X','D','E')
                THEN CASE WHEN p.call_code IN ('F','X','D','E')
                    THEN 1.0 ELSE 0.0 END
            END) as contact_pct,
            AVG(CASE WHEN p.call_code IN ('S','W','T','F','X','D','E')
                THEN CASE WHEN p.call_code IN ('S','W','T')
                    THEN 1.0 ELSE 0.0 END
            END) as whiff_pct,
            AVG(CASE WHEN p.call_code IN ('C','S','W','T')
                THEN 1.0 ELSE 0.0 END) as csw_pct,
            AVG(CASE WHEN p.zone NOT BETWEEN 1 AND 9
                THEN CASE WHEN p.call_code IN ('S','W','T','F','X','D','E')
                    THEN 1.0 ELSE 0.0 END
            END) as chase_pct,
            AVG(CASE WHEN p.call_code = 'X' AND p.start_speed >= 92
                THEN 1.0 WHEN p.call_code = 'X' THEN 0.0 END) as hard_hit_pct,
            SUM(CASE WHEN p.call_code IN ('S','W','T')
                THEN COALESCE(bw.weight, 1.0) ELSE 0.0 END) /
            NULLIF(SUM(CASE WHEN p.call_code IN ('S','W','T','F','X','D','E')
                THEN 1.0 ELSE 0.0 END), 0) as weighted_whiff_rate,
            COUNT(*) as pitches
        FROM mlb.pitches p
        JOIN mlb.at_bats ab ON ab.game_pk = p.game_pk
            AND ab.at_bat_index = p.at_bat_index
        JOIN mlb.games g ON g.game_pk = p.game_pk
        LEFT JOIN (
            SELECT ab2.batter_id,
                LEAST(:lg_whiff / NULLIF(
                    AVG(CASE WHEN p2.call_code IN ('S','W','T') THEN 1.0 ELSE 0.0 END) /
                    NULLIF(AVG(CASE WHEN p2.call_code IN ('S','W','T','F','X','D','E')
                        THEN 1.0 ELSE 0.0 END), 0)
                , 0), 5.0) as weight
            FROM mlb.pitches p2
            JOIN mlb.at_bats ab2 ON ab2.game_pk = p2.game_pk
                AND ab2.at_bat_index = p2.at_bat_index
            JOIN mlb.games g2 ON g2.game_pk = p2.game_pk
            WHERE g2.season = :season AND g2.game_type = 'R'
            GROUP BY ab2.batter_id
            HAVING SUM(CASE WHEN p2.call_code IN ('S','W','T','F','X','D','E')
                THEN 1 ELSE 0 END) >= 50
        ) bw ON bw.batter_id = ab.batter_id
        WHERE g.season = :season AND g.game_type = 'R'
        AND p.pitch_type_code IS NOT NULL
        AND p.call_code IN ('S','W','T','F','X','D','E','C','B')
        GROUP BY ab.pitcher_id, p.pitch_type_code, ab.bat_side
        HAVING COUNT(*) >= 10
    """)
    pitcher_rows = db.execute(pitcher_sql, {
        "season": score_season,
        "lg_whiff": LEAGUE_AVG_WHIFF,
    }).mappings().all()
    print(f"  Loaded {len(pitcher_rows)} pitcher×pitch×side combinations")

    # ── Load SLG against ──────────────────────────────────────────────────────
    slg_sql = text("""
        SELECT ab.pitcher_id, last_pitch.pitch_type_code, ab.bat_side,
            AVG(CASE ab.event
                WHEN 'Single' THEN 1.0 WHEN 'Double' THEN 2.0
                WHEN 'Triple' THEN 3.0 WHEN 'Home Run' THEN 4.0
                ELSE 0.0 END) as slg_against
        FROM mlb.at_bats ab
        JOIN mlb.games g ON g.game_pk = ab.game_pk
        JOIN LATERAL (
            SELECT p.pitch_type_code
            FROM mlb.pitches p
            WHERE p.game_pk = ab.game_pk
            AND p.at_bat_index = ab.at_bat_index
            ORDER BY p.pitch_number DESC LIMIT 1
        ) last_pitch ON true
        WHERE g.season = :season AND g.game_type = 'R'
        AND ab.event IN ('Single','Double','Triple','Home Run',
            'Groundout','Flyout','Lineout','Pop Out','Forceout',
            'Grounded Into DP','Sac Fly','Field Error',
            'Fielders Choice','Double Play')
        AND last_pitch.pitch_type_code IS NOT NULL
        GROUP BY ab.pitcher_id, last_pitch.pitch_type_code, ab.bat_side
        HAVING COUNT(*) >= 5
    """)
    slg_rows = db.execute(slg_sql, {"season": score_season}).mappings().all()
    slg_map = {
        (int(r['pitcher_id']), r['pitch_type_code'], r['bat_side']):
            float(r['slg_against'])
        for r in slg_rows
    }
    print(f"  Loaded SLG against for {len(slg_map)} pitcher×pitch×side combinations")

    # ── League averages for fallback indexing ─────────────────────────────────
    lg_rates = {}
    for key, bl in baselines.items():
        pt = key[0]
        if pt not in lg_rates:
            lg_rates[pt] = {
                'whiff': [], 'csw': [], 'contact': [],
                'chase': [], 'slg': [], 'hard_hit': []
            }
        for metric, short in [
            ('whiff_pct', 'whiff'), ('csw_pct', 'csw'),
            ('contact_pct', 'contact'), ('chase_pct', 'chase'),
            ('slg_against', 'slg'), ('hard_hit_pct', 'hard_hit'),
        ]:
            if bl.get(metric):
                lg_rates[pt][short].append(float(bl[metric]))

    lg_avg = {}
    for pt, metrics in lg_rates.items():
        lg_avg[pt] = {k: np.mean(v) if v else None for k, v in metrics.items()}

    # ── Upserts ───────────────────────────────────────────────────────────────
    pk_upsert = text("""
        INSERT INTO mlb.pk_scores (
            pitcher_id, pitcher_name, season, pitch_type_code, pitch_hand, bat_side,
            whiff_idx, csw_idx, contact_idx, chase_idx,
            whiff_pct, csw_pct, contact_pct, chase_pct,
            pk_plus, pitches
        ) VALUES (
            :pitcher_id, :pitcher_name, :season, :pitch_type_code, :pitch_hand, :bat_side,
            :whiff_idx, :csw_idx, :contact_idx, :chase_idx,
            :whiff_pct, :csw_pct, :contact_pct, :chase_pct,
            :pk_plus, :pitches
        )
        ON CONFLICT (pitcher_id, season, pitch_type_code, bat_side) DO UPDATE SET
            whiff_idx = EXCLUDED.whiff_idx,
            csw_idx = EXCLUDED.csw_idx,
            contact_idx = EXCLUDED.contact_idx,
            chase_idx = EXCLUDED.chase_idx,
            pk_plus = EXCLUDED.pk_plus,
            pitches = EXCLUDED.pitches,
            computed_at = NOW()
    """)

    phr_upsert = text("""
        INSERT INTO mlb.phr_scores (
            pitcher_id, pitcher_name, season, pitch_type_code, pitch_hand, bat_side,
            slg_idx, hard_hit_idx, slg_against, hard_hit_pct,
            phr_plus, pitches
        ) VALUES (
            :pitcher_id, :pitcher_name, :season, :pitch_type_code, :pitch_hand, :bat_side,
            :slg_idx, :hard_hit_idx, :slg_against, :hard_hit_pct,
            :phr_plus, :pitches
        )
        ON CONFLICT (pitcher_id, season, pitch_type_code, bat_side) DO UPDATE SET
            slg_idx = EXCLUDED.slg_idx,
            hard_hit_idx = EXCLUDED.hard_hit_idx,
            phr_plus = EXCLUDED.phr_plus,
            pitches = EXCLUDED.pitches,
            computed_at = NOW()
    """)

    pk_totals = {}
    phr_totals = {}
    stored_pk = stored_phr = 0

    for r in pitcher_rows:
        pid = r['pitcher_id']
        pt  = r['pitch_type_code']
        bs  = r['bat_side']
        ph  = r['pitch_hand']
        n   = int(r['pitches'])

        bl = (baselines.get((pt, bs, ph)) or
              baselines.get((pt, bs, 'R')) or
              baselines.get((pt, bs, 'L')))
        lg = lg_avg.get(pt, {})

        # ── pK+ ──────────────────────────────────────────────────────────────
        wtd_whiff = float(r['weighted_whiff_rate'] or r['whiff_pct'] or 0)
        lg_whiff   = float(bl['whiff_pct'])   if bl and bl.get('whiff_pct')   else float(lg.get('whiff')   or 0.25)
        lg_csw     = float(bl['csw_pct'])     if bl and bl.get('csw_pct')     else float(lg.get('csw')     or 0.30)
        lg_contact = float(bl['contact_pct']) if bl and bl.get('contact_pct') else float(lg.get('contact') or 0.75)
        lg_chase   = float(bl['chase_pct'])   if bl and bl.get('chase_pct')   else float(lg.get('chase')   or 0.28)

        whiff_idx   = safe_idx(wtd_whiff,         lg_whiff,   invert=False)
        csw_idx     = safe_idx(r['csw_pct'],       lg_csw,     invert=False)
        contact_idx = safe_idx(r['contact_pct'],   lg_contact, invert=True)
        chase_idx   = safe_idx(r['chase_pct'],     lg_chase,   invert=False)

        pk_raw = (
            whiff_idx   * pK_WEIGHTS['whiff'] +
            csw_idx     * pK_WEIGHTS['csw'] +
            contact_idx * pK_WEIGHTS['contact'] +
            chase_idx   * pK_WEIGHTS['chase']
        )
        pk_plus = regress_to_mean(pk_raw, n)

        db.execute(pk_upsert, {
            "pitcher_id": pid, "pitcher_name": r['pitcher_name'],
            "season": score_season, "pitch_type_code": pt,
            "pitch_hand": ph, "bat_side": bs,
            "whiff_idx":   round(whiff_idx, 1),
            "csw_idx":     round(csw_idx, 1),
            "contact_idx": round(contact_idx, 1),
            "chase_idx":   round(chase_idx, 1),
            "whiff_pct":   round(float(r['whiff_pct'] or 0), 3),
            "csw_pct":     round(float(r['csw_pct'] or 0), 3),
            "contact_pct": round(float(r['contact_pct'] or 0), 3),
            "chase_pct":   round(float(r['chase_pct'] or 0), 3),
            "pk_plus":     pk_plus,
            "pitches":     n,
        })
        stored_pk += 1

        pk_totals.setdefault(pid, {'name': r['pitcher_name'], 'hand': ph, 'scores': []})
        pk_totals[pid]['scores'].append({'pk': pk_plus, 'pitches': n, 'side': bs})

        # ── pHR+ ─────────────────────────────────────────────────────────────
        slg    = slg_map.get((pid, pt, bs))
        hh     = float(r['hard_hit_pct'] or 0)
        lg_slg = float(bl['slg_against'])  if bl and bl.get('slg_against')  else float(lg.get('slg')      or 0.40)
        lg_hh  = float(bl['hard_hit_pct']) if bl and bl.get('hard_hit_pct') else float(lg.get('hard_hit') or 0.035)

        slg_idx      = safe_idx(slg, lg_slg, invert=True)
        hard_hit_idx = safe_idx(hh,  lg_hh,  invert=True)

        phr_raw = (
            slg_idx      * pHR_WEIGHTS['slg'] +
            hard_hit_idx * pHR_WEIGHTS['hard_hit']
        )
        phr_plus = regress_to_mean(phr_raw, n)

        db.execute(phr_upsert, {
            "pitcher_id": pid, "pitcher_name": r['pitcher_name'],
            "season": score_season, "pitch_type_code": pt,
            "pitch_hand": ph, "bat_side": bs,
            "slg_idx":      round(slg_idx, 1),
            "hard_hit_idx": round(hard_hit_idx, 1),
            "slg_against":  round(float(slg), 3) if slg is not None else None,
            "hard_hit_pct": round(hh, 3),
            "phr_plus":     phr_plus,
            "pitches":      n,
        })
        stored_phr += 1

        phr_totals.setdefault(pid, {'name': r['pitcher_name'], 'hand': ph, 'scores': []})
        phr_totals[pid]['scores'].append({'phr': phr_plus, 'pitches': n, 'side': bs})

    db.commit()
    print(f"  Stored {stored_pk} pK+ / {stored_phr} pHR+ pitch scores")

    # ── Overall composites ────────────────────────────────────────────────────
    pk_overall_upsert = text("""
        INSERT INTO mlb.pk_overall (
            pitcher_id, pitcher_name, season, pitch_hand,
            pk_plus, pk_vs_lhh, pk_vs_rhh, total_pitches
        ) VALUES (
            :pitcher_id, :pitcher_name, :season, :pitch_hand,
            :pk_plus, :pk_vs_lhh, :pk_vs_rhh, :total_pitches
        )
        ON CONFLICT (pitcher_id, season) DO UPDATE SET
            pk_plus = EXCLUDED.pk_plus,
            pk_vs_lhh = EXCLUDED.pk_vs_lhh,
            pk_vs_rhh = EXCLUDED.pk_vs_rhh,
            total_pitches = EXCLUDED.total_pitches,
            computed_at = NOW()
    """)

    phr_overall_upsert = text("""
        INSERT INTO mlb.phr_overall (
            pitcher_id, pitcher_name, season, pitch_hand,
            phr_plus, phr_vs_lhh, phr_vs_rhh, total_pitches
        ) VALUES (
            :pitcher_id, :pitcher_name, :season, :pitch_hand,
            :phr_plus, :phr_vs_lhh, :phr_vs_rhh, :total_pitches
        )
        ON CONFLICT (pitcher_id, season) DO UPDATE SET
            phr_plus = EXCLUDED.phr_plus,
            phr_vs_lhh = EXCLUDED.phr_vs_lhh,
            phr_vs_rhh = EXCLUDED.phr_vs_rhh,
            total_pitches = EXCLUDED.total_pitches,
            computed_at = NOW()
    """)

    def build_overall(totals, score_key, upsert_sql, label):
        stored = 0
        for pid, data in totals.items():
            scores = data['scores']
            total_p = sum(s['pitches'] for s in scores)
            if total_p == 0:
                continue
            overall = sum(s[score_key] * s['pitches'] for s in scores) / total_p
            lhh = [s for s in scores if s['side'] == 'L']
            rhh = [s for s in scores if s['side'] == 'R']
            lhh_p = sum(s['pitches'] for s in lhh)
            rhh_p = sum(s['pitches'] for s in rhh)
            vs_lhh = sum(s[score_key] * s['pitches'] for s in lhh) / max(lhh_p, 1) if lhh else None
            vs_rhh = sum(s[score_key] * s['pitches'] for s in rhh) / max(rhh_p, 1) if rhh else None
            db.execute(upsert_sql, {
                "pitcher_id":   int(pid),
                "pitcher_name": data['name'],
                "season":       score_season,
                "pitch_hand":   data['hand'],
                f"{label}_plus":    round(overall, 1),
                f"{label}_vs_lhh":  round(vs_lhh, 1) if vs_lhh is not None else None,
                f"{label}_vs_rhh":  round(vs_rhh, 1) if vs_rhh is not None else None,
                "total_pitches": int(total_p),
            })
            stored += 1
        db.commit()
        print(f"  Stored {stored} {label} overall scores")

    build_overall(pk_totals,  'pk',  pk_overall_upsert,  'pk')
    build_overall(phr_totals, 'phr', phr_overall_upsert, 'phr')


def validate(season: int, db):
    print(f"\n=== Validating pK+ / pHR+ ({season}) ===")

    sql = text("""
        WITH may AS (
            SELECT ab.pitcher_id,
                AVG(CASE WHEN ab.event IN ('Strikeout','Strikeout - DP')
                    THEN 1.0 ELSE 0.0 END) as k_rate,
                AVG(CASE WHEN ab.event = 'Home Run'
                    THEN 1.0 ELSE 0.0 END) as hr_rate,
                COUNT(*) as pa
            FROM mlb.at_bats ab
            JOIN mlb.games g ON g.game_pk = ab.game_pk
            WHERE g.season = :season AND g.game_type = 'R'
            AND g.game_date >= '2026-05-01'
            AND ab.event NOT IN ('Runner Out','Caught Stealing 2B',
                'Caught Stealing 3B','Wild Pitch','Passed Ball')
            GROUP BY ab.pitcher_id HAVING COUNT(*) >= 20
        )
        SELECT
            COUNT(*) as n,
            ROUND(CORR(pk.pk_plus,   m.k_rate)::numeric, 3) as pk_k,
            ROUND(CORR(pk.pk_plus,   m.hr_rate)::numeric, 3) as pk_hr,
            ROUND(CORR(phr.phr_plus, m.k_rate)::numeric, 3) as phr_k,
            ROUND(CORR(phr.phr_plus, m.hr_rate)::numeric, 3) as phr_hr,
            ROUND(CORR(g2.goose2_plus, m.k_rate)::numeric, 3) as g2_k,
            ROUND(CORR(g2.goose2_plus, m.hr_rate)::numeric, 3) as g2_hr,
            ROUND(CORR(so.stuff_score, m.k_rate)::numeric, 3) as stuff_k
        FROM may m
        LEFT JOIN mlb.pk_overall pk   ON pk.pitcher_id  = m.pitcher_id AND pk.season  = :season
        LEFT JOIN mlb.phr_overall phr ON phr.pitcher_id = m.pitcher_id AND phr.season = :season
        LEFT JOIN mlb.goose2_overall g2 ON g2.pitcher_id = m.pitcher_id AND g2.season = :season
        LEFT JOIN mlb.stuff_overall so  ON so.pitcher_id  = m.pitcher_id AND so.season  = :season
    """)
    row = db.execute(sql, {"season": season}).mappings().first()

    print(f"\n  {'Model':<12} {'K Rate':>10} {'HR Rate':>10}")
    print(f"  {'-'*34}")
    print(f"  {'pK+':<12} {row['pk_k']:>+10.3f} {row['pk_hr']:>+10.3f}")
    print(f"  {'pHR+':<12} {row['phr_k']:>+10.3f} {row['phr_hr']:>+10.3f}")
    print(f"  {'Goose+2':<12} {row['g2_k']:>+10.3f} {row['g2_hr']:>+10.3f}")
    print(f"  {'Stuff':<12} {row['stuff_k']:>+10.3f} {'—':>10}")
    print(f"  n={row['n']}")

    top_sql = text("""
        SELECT pk.pitcher_name, pk.pk_plus, phr.phr_plus,
               so.stuff_score, g3.goose3_plus, pk.total_pitches
        FROM mlb.pk_overall pk
        LEFT JOIN mlb.phr_overall phr ON phr.pitcher_id = pk.pitcher_id AND phr.season = pk.season
        LEFT JOIN mlb.stuff_overall so  ON so.pitcher_id  = pk.pitcher_id AND so.season  = pk.season
        LEFT JOIN mlb.goose3_overall g3 ON g3.pitcher_id  = pk.pitcher_id AND g3.season  = pk.season
        WHERE pk.season = :season AND pk.total_pitches >= 200
        ORDER BY pk.pk_plus DESC LIMIT 15
    """)
    rows = db.execute(top_sql, {"season": season}).mappings().all()
    print(f"\n  Top 15 pK+ ({season}):")
    print(f"  {'Pitcher':<25} {'pK+':>6} {'pHR+':>6} {'Stuff':>6} {'G3+':>6} {'Pitches':>8}")
    print(f"  {'-'*60}")
    for r in rows:
        print(f"  {r['pitcher_name']:<25} "
              f"{r['pk_plus']:>6.1f} "
              f"{(r['phr_plus'] or 0):>6.1f} "
              f"{(r['stuff_score'] or 0):>6.1f} "
              f"{(r['goose3_plus'] or 0):>6.1f} "
              f"{r['total_pitches']:>8}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-season", type=int, default=2025)
    parser.add_argument("--score-season",    type=int, default=2026)
    parser.add_argument("--validate-only",   action="store_true")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        if args.validate_only:
            validate(args.score_season, db)
        else:
            build_pk_phr(args.score_season, args.baseline_season, db)
            validate(args.score_season, db)
    finally:
        db.close()
