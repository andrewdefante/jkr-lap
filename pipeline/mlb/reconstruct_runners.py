"""
Reconstruct runner state for each at-bat from GUMBO raw_events data.

Method:
- For each game, iterate through allPlays in order by atBatIndex
- pre-play runner state = postOn* from previous play
- outs_before = outs count at start of PA
- Store on_first, on_second, on_third, outs_before in mlb.at_bats

GUMBO fields used:
- play['matchup']['postOnFirst/Second/Third'] — runner after play
- play['count']['outs'] — outs after play
- play['about']['atBatIndex'] — PA index within game
- play['about']['halfInning'] — top/bottom
- play['about']['inning'] — inning number
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import argparse
from database import SessionLocal
from sqlalchemy import text


def reconstruct_game(game_pk: int, db) -> int:
    """Reconstruct runner state for all at-bats in a game."""

    sql = text("""
        SELECT data FROM mlb.raw_events
        WHERE game_pk = :gp
        LIMIT 1
    """)
    row = db.execute(sql, {"gp": game_pk}).mappings().first()
    if not row:
        return 0

    data = row['data']
    all_plays = data.get('liveData', {}).get('plays', {}).get('allPlays', [])
    if not all_plays:
        return 0

    runner_states = {}

    prev_on_first = False
    prev_on_second = False
    prev_on_third = False
    prev_outs = 0
    prev_inning = None
    prev_half = None

    for play in sorted(all_plays, key=lambda x: x.get('about', {}).get('atBatIndex', 0)):
        about = play.get('about', {})
        matchup = play.get('matchup', {})
        count = play.get('count', {})

        ab_index = about.get('atBatIndex')
        inning = about.get('inning')
        half_inning = about.get('halfInning')

        if ab_index is None:
            continue

        # Reset runners at start of each half-inning
        if inning != prev_inning or half_inning != prev_half:
            prev_on_first = False
            prev_on_second = False
            prev_on_third = False
            prev_outs = 0

        runner_states[ab_index] = {
            'on_first': prev_on_first,
            'on_second': prev_on_second,
            'on_third': prev_on_third,
            'outs_before': prev_outs,
        }

        post_first  = matchup.get('postOnFirst')
        post_second = matchup.get('postOnSecond')
        post_third  = matchup.get('postOnThird')
        outs_after  = count.get('outs', 0)

        prev_on_first  = post_first is not None and bool(post_first)
        prev_on_second = post_second is not None and bool(post_second)
        prev_on_third  = post_third is not None and bool(post_third)
        prev_outs      = int(outs_after) if outs_after is not None else 0

        if prev_outs >= 3:
            prev_on_first = False
            prev_on_second = False
            prev_on_third = False
            prev_outs = 0

        prev_inning = inning
        prev_half = half_inning

    if not runner_states:
        return 0

    update_sql = text("""
        UPDATE mlb.at_bats SET
            on_first = :on_first,
            on_second = :on_second,
            on_third = :on_third,
            outs_before = :outs_before,
            runners_reconstructed = TRUE
        WHERE game_pk = :game_pk
        AND at_bat_index = :ab_index
        AND runners_reconstructed = FALSE
    """)

    updated = 0
    for ab_index, state in runner_states.items():
        result = db.execute(update_sql, {
            "game_pk": game_pk,
            "ab_index": int(ab_index),
            "on_first": state['on_first'],
            "on_second": state['on_second'],
            "on_third": state['on_third'],
            "outs_before": state['outs_before'],
        })
        updated += result.rowcount

    db.commit()
    return updated


def reconstruct_season(season: int, db):
    """Reconstruct runner states for all games in a season."""
    print(f"\n=== Reconstructing Runner States — {season} ===")

    games_sql = text("""
        SELECT DISTINCT g.game_pk
        FROM mlb.games g
        JOIN mlb.at_bats ab ON ab.game_pk = g.game_pk
        JOIN mlb.raw_events re ON re.game_pk = g.game_pk
        WHERE g.season = :season
        AND g.game_type = 'R'
        AND ab.runners_reconstructed = FALSE
        ORDER BY g.game_pk
    """)
    game_pks = db.execute(games_sql, {"season": season}).scalars().all()
    print(f"  Games to process: {len(game_pks)}")

    total = 0
    errors = 0
    for i, gp in enumerate(game_pks):
        try:
            n = reconstruct_game(gp, db)
            total += n
            if i % 50 == 0:
                print(f"  [{i}/{len(game_pks)}] {total:,} at-bats updated...")
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  Error game {gp}: {e}")

    print(f"  Done. {total:,} at-bats reconstructed, {errors} errors")


def validate(db):
    """Spot-check reconstruction."""
    print("\n=== Validation ===")

    sql = text("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN runners_reconstructed THEN 1 ELSE 0 END) as reconstructed,
            SUM(CASE WHEN on_first THEN 1 ELSE 0 END) as with_runner_1st,
            SUM(CASE WHEN on_second THEN 1 ELSE 0 END) as with_runner_2nd,
            SUM(CASE WHEN on_third THEN 1 ELSE 0 END) as with_runner_3rd,
            ROUND(AVG(CASE WHEN on_first OR on_second OR on_third
                THEN 1.0 ELSE 0.0 END) * 100, 1) as pct_with_runners,
            ROUND(AVG(outs_before::numeric), 2) as avg_outs_before
        FROM mlb.at_bats ab
        JOIN mlb.games g ON g.game_pk = ab.game_pk
        WHERE g.season = 2026 AND g.game_type = 'R'
    """)
    row = db.execute(sql).mappings().first()
    print(f"  Total at-bats:     {row['total']:,}")
    print(f"  Reconstructed:     {row['reconstructed']:,}")
    print(f"  Runner on 1st:     {row['with_runner_1st']:,}")
    print(f"  Runner on 2nd:     {row['with_runner_2nd']:,}")
    print(f"  Runner on 3rd:     {row['with_runner_3rd']:,}")
    print(f"  % with runners:    {row['pct_with_runners']}%  (expect 33-38%)")
    print(f"  Avg outs before:   {row['avg_outs_before']}  (expect ~1.0)")

    sample_sql = text("""
        SELECT event, on_first, on_second, on_third, outs_before,
               COUNT(*) as n
        FROM mlb.at_bats ab
        JOIN mlb.games g ON g.game_pk = ab.game_pk
        WHERE g.season = 2026 AND event = 'Home Run'
        AND runners_reconstructed = TRUE
        GROUP BY event, on_first, on_second, on_third, outs_before
        ORDER BY n DESC
        LIMIT 8
    """)
    print("\n  Home Run by base-out state:")
    rows = db.execute(sample_sql).mappings().all()
    for r in rows:
        state = (f"{'1B' if r['on_first'] else '--'} "
                 f"{'2B' if r['on_second'] else '--'} "
                 f"{'3B' if r['on_third'] else '--'} "
                 f"{r['outs_before']}out")
        print(f"    {state}: {r['n']} HRs")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, nargs='+', default=[2024, 2025, 2026])
    parser.add_argument("--validate", action="store_true")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        if args.validate:
            validate(db)
        else:
            for season in args.season:
                reconstruct_season(season, db)
            validate(db)
    finally:
        db.close()
