"""
Backfill balls_before and strikes_before for all pitches.

Reconstruction logic using call_code:
- B, *B, H (HBP) → adds ball after
- C (called strike), S (swinging strike), T (foul tip) → adds strike after
- F (foul), L (foul bunt), O (bunt attempt) → adds strike if <2, else stays
- X (in play) → at-bat ends, no count change
- W (intentional ball) → adds ball

We use strikes_after which is already stored to validate our reconstruction.
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import argparse
from database import SessionLocal
from sqlalchemy import text

BALL_CODES = {'B', '*B', 'H', 'W', 'I', 'P'}
STRIKE_CODES = {'C', 'S', 'T', 'K', 'Q', 'R'}
FOUL_CODES = {'F', 'L', 'O', 'M', 'N', 'E', 'D'}
END_CODES = {'X'}


def backfill_game(game_pk: int, db) -> int:
    """Reconstruct and store balls_before/strikes_before for one game."""
    sql = text("""
        SELECT p.id, p.pitch_number, p.call_code,
               p.at_bat_index, p.game_pk
        FROM mlb.pitches p
        WHERE p.game_pk = :gp
        AND p.balls_before IS NULL
        ORDER BY p.at_bat_index, p.pitch_number
    """)
    rows = db.execute(sql, {"gp": game_pk}).mappings().all()
    if not rows:
        return 0

    at_bats = {}
    for r in rows:
        idx = r['at_bat_index']
        if idx not in at_bats:
            at_bats[idx] = []
        at_bats[idx].append(dict(r))

    updates = []
    for at_bat_idx, pitches in at_bats.items():
        balls = 0
        strikes = 0

        for pitch in sorted(pitches, key=lambda x: x['pitch_number']):
            call = (pitch['call_code'] or '').strip()

            updates.append({
                "id": pitch['id'],
                "balls_before": balls,
                "strikes_before": strikes
            })

            if call in BALL_CODES:
                balls = min(balls + 1, 4)
            elif call in STRIKE_CODES:
                strikes = min(strikes + 1, 3)
            elif call in FOUL_CODES:
                if strikes < 2:
                    strikes += 1
            elif call in END_CODES:
                break

            if balls >= 4 or strikes >= 3:
                break

    if not updates:
        return 0

    update_sql = text("""
        UPDATE mlb.pitches SET
            balls_before = :balls_before,
            strikes_before = :strikes_before
        WHERE id = :id
    """)
    for u in updates:
        db.execute(update_sql, u)

    db.commit()
    return len(updates)


def backfill_season(season: int, db):
    print(f"\n=== Backfilling Count Data — {season} ===")

    game_sql = text("""
        SELECT DISTINCT g.game_pk
        FROM mlb.games g
        JOIN mlb.pitches p ON p.game_pk = g.game_pk
        WHERE g.season = :season
        AND g.game_type = 'R'
        AND p.balls_before IS NULL
        ORDER BY g.game_pk
    """)
    game_pks = db.execute(game_sql, {"season": season}).scalars().all()
    print(f"  Games to backfill: {len(game_pks)}")

    total = 0
    for i, gp in enumerate(game_pks):
        n = backfill_game(gp, db)
        total += n
        if i % 50 == 0:
            print(f"  [{i}/{len(game_pks)}] {total:,} pitches updated...")

    print(f"  Done. {total:,} pitches backfilled for {season}")


def validate_sample(db):
    print("\n=== Validation Sample ===")
    sql = text("""
        SELECT p.balls_before, p.strikes_before, p.call_code,
               p.strikes_after, p.pitch_number,
               p.at_bat_index, p.game_pk
        FROM mlb.pitches p
        JOIN mlb.games g ON g.game_pk = p.game_pk
        WHERE g.season = 2026
        AND p.balls_before IS NOT NULL
        AND p.strikes_after IS NOT NULL
        AND p.call_code IN ('C','S','T')
        AND p.strikes_before + 1 != p.strikes_after
        AND p.strikes_before < 2
        LIMIT 10
    """)
    mismatches = db.execute(sql).mappings().all()
    if mismatches:
        print(f"  WARNING: {len(mismatches)} mismatches found")
        for m in mismatches:
            print(f"    {m}")
    else:
        print("  OK No mismatches found in sample")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, nargs='+', default=[2024, 2025, 2026])
    parser.add_argument("--validate", action="store_true")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        for season in args.season:
            backfill_season(season, db)
        if args.validate:
            validate_sample(db)
    finally:
        db.close()
