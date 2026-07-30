"""
Backfill new boxscore columns for a season by re-running transform_boxscore
against existing mlb.raw_events (no re-fetching from the API).

Usage:
    PYTHONPATH=/app:/pipeline python3 /pipeline/mlb/backfill_boxscore.py --season 2026
    PYTHONPATH=/app:/pipeline python3 /pipeline/mlb/backfill_boxscore.py --season 2026 --batch-size 100
"""
import argparse

from database import SessionLocal
from sqlalchemy import text
from models.mlb import MLBRawEvent
from mlb.transform import transform_boxscore


def backfill(season: int, batch_size: int = 50):
    db = SessionLocal()
    try:
        game_pks = db.execute(text("""
            SELECT re.game_pk
            FROM mlb.raw_events re
            JOIN mlb.games g ON g.game_pk = re.game_pk
            WHERE g.season = :season
              AND g.game_type = 'R'
              AND g.status = 'Final'
            ORDER BY g.game_date
        """), {"season": season}).scalars().all()

        total = len(game_pks)
        print(f"Backfilling {total} games for {season}...")

        errors = []
        for i, game_pk in enumerate(game_pks):
            try:
                raw_obj = db.query(MLBRawEvent).filter_by(game_pk=game_pk).first()
                if not raw_obj:
                    continue
                # transform_boxscore commits its own delete+insert per game
                transform_boxscore(raw_obj, db)
            except Exception as e:
                db.rollback()
                errors.append((game_pk, str(e)))
                print(f"  ERROR game_pk={game_pk}: {e}")
                continue

            if (i + 1) % batch_size == 0:
                print(f"  [{i + 1}/{total}] processed")

        print(f"\nBackfill complete. Errors: {len(errors)}")
        for pk, err in errors[:10]:
            print(f"  {pk}: {err}")

    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--batch-size", type=int, default=50)
    args = parser.parse_args()
    backfill(args.season, args.batch_size)
