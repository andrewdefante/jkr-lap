"""
Fetch Kalshi MLB strikeout markets and cache to DB.
Maps Kalshi player names to MLBAM IDs via at_bats lookup.
"""
import sys
sys.path.insert(0, '/app')
sys.path.insert(0, '/pipeline')

import httpx
import time
from datetime import date, datetime
from database import SessionLocal
from sqlalchemy import text

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
SERIES = "KXMLBKS"

MONTHS = {'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,
          'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}


def fetch_kalshi_markets() -> list:
    all_markets = []
    cursor = None
    while True:
        params = {"limit": 100, "series_ticker": SERIES}
        if cursor:
            params["cursor"] = cursor
        for attempt in range(3):
            r = httpx.get(f"{KALSHI_BASE}/markets", params=params,
                          headers={"Accept": "application/json"}, timeout=15)
            if r.status_code == 429:
                time.sleep(2 + attempt * 2)
                continue
            r.raise_for_status()
            break
        data = r.json()
        markets = data.get("markets", [])
        all_markets.extend(markets)
        cursor = data.get("cursor")
        if not cursor or not markets:
            break
        time.sleep(0.5)
    return all_markets


def extract_player_name(market: dict) -> str:
    """Extract full player name from Kalshi market. title = 'Cade Cavalli: 9+ strikeouts?'"""
    title = market.get("title", "")
    if ":" in title:
        return title.split(":")[0].strip()
    return ""


def match_player_to_mlbam(player_name: str, db) -> tuple:
    """Return (mlbam_id, canonical_name) or (None, player_name)."""
    if not player_name:
        return None, player_name

    # Exact full name match
    row = db.execute(text("""
        SELECT DISTINCT pitcher_id as player_id, pitcher_name
        FROM mlb.at_bats
        WHERE LOWER(TRIM(pitcher_name)) = LOWER(TRIM(:name))
        AND pitcher_id IS NOT NULL
        LIMIT 1
    """), {"name": player_name}).mappings().first()
    if row:
        return int(row['player_id']), row['pitcher_name']

    # Last name + first initial fallback
    parts = player_name.strip().split()
    if len(parts) >= 2:
        last = parts[-1]
        first_initial = parts[0][0].upper()
        row2 = db.execute(text("""
            SELECT DISTINCT pitcher_id as player_id, pitcher_name
            FROM mlb.at_bats
            WHERE UPPER(pitcher_name) LIKE :last
            AND UPPER(pitcher_name) LIKE :first
            AND pitcher_id IS NOT NULL
            LIMIT 1
        """), {"last": f"%{last.upper()}%",
               "first": f"{first_initial}%"}).mappings().first()
        if row2:
            return int(row2['player_id']), row2['pitcher_name']

    return None, player_name


def parse_game_date(ticker: str) -> date:
    """KXMLBKS-26MAY201305CINPHI → 2026-05-20  (format: YYMMMDD + time + teams)"""
    try:
        date_part = ticker.split("-")[1][:7]   # "26MAY20"
        year  = 2000 + int(date_part[:2])       # "26" → 2026
        month = MONTHS[date_part[2:5]]          # "MAY" → 5
        day   = int(date_part[5:7])             # "20" → 20
        return date(year, month, day)
    except Exception:
        return date.today()


def store_markets(markets: list, db) -> tuple:
    today = db.execute(text("SELECT CURRENT_DATE as d")).mappings().first()['d']
    stored = matched = 0
    uuid_cache = {}

    upsert = text("""
        INSERT INTO mlb.kalshi_markets (
            fetch_date, ticker, player_uuid, player_name, mlbam_id,
            game_date, floor_strike, yes_ask, no_ask, yes_bid, no_bid,
            implied_prob, status, close_time
        ) VALUES (
            :fetch_date, :ticker, :player_uuid, :player_name, :mlbam_id,
            :game_date, :floor_strike, :yes_ask, :no_ask, :yes_bid, :no_bid,
            :implied_prob, :status, :close_time
        )
        ON CONFLICT (fetch_date, ticker) DO UPDATE SET
            player_name  = COALESCE(EXCLUDED.player_name, kalshi_markets.player_name),
            mlbam_id     = COALESCE(EXCLUDED.mlbam_id,    kalshi_markets.mlbam_id),
            game_date    = EXCLUDED.game_date,
            yes_ask      = EXCLUDED.yes_ask,
            no_ask       = EXCLUDED.no_ask,
            yes_bid      = EXCLUDED.yes_bid,
            no_bid       = EXCLUDED.no_bid,
            implied_prob = EXCLUDED.implied_prob,
            status       = EXCLUDED.status,
            fetched_at   = NOW()
    """)

    for m in markets:
        ticker      = m.get("ticker", "")
        player_uuid = m.get("custom_strike", {}).get("baseball_player", "")
        kalshi_name = extract_player_name(m)

        yes_ask = float(m.get("yes_ask_dollars") or 0)
        no_ask  = float(m.get("no_ask_dollars")  or 0)
        yes_bid = float(m.get("yes_bid_dollars") or 0)
        no_bid  = float(m.get("no_bid_dollars")  or 0)

        if yes_bid + no_bid > 0:
            implied_prob = yes_bid / (yes_bid + no_bid)
        elif yes_ask > 0:
            implied_prob = yes_ask
        else:
            implied_prob = None

        # Skip lines with no valid market pricing
        if not implied_prob or implied_prob <= 0:
            continue

        if player_uuid not in uuid_cache:
            mlbam_id, player_name = match_player_to_mlbam(kalshi_name, db)
            uuid_cache[player_uuid] = (mlbam_id, player_name or kalshi_name)
            if mlbam_id:
                matched += 1
        else:
            mlbam_id, player_name = uuid_cache[player_uuid]

        game_date  = parse_game_date(ticker)
        close_time = m.get("close_time")
        if close_time:
            try:
                close_time = datetime.fromisoformat(close_time.replace('Z', '+00:00'))
            except Exception:
                close_time = None

        db.execute(upsert, {
            "fetch_date":   today,
            "ticker":       ticker,
            "player_uuid":  player_uuid,
            "player_name":  player_name,
            "mlbam_id":     mlbam_id,
            "game_date":    game_date,
            "floor_strike": float(m.get("floor_strike") or 0),
            "yes_ask":      yes_ask,
            "no_ask":       no_ask,
            "yes_bid":      yes_bid,
            "no_bid":       no_bid,
            "implied_prob": round(implied_prob, 3) if implied_prob is not None else None,
            "status":       m.get("status"),
            "close_time":   close_time,
        })
        stored += 1

    db.commit()
    print(f"  Stored {stored} markets, matched {matched} players to MLBAM IDs")
    return stored, matched


def backfill_missing_mlbam(db, fetch_date=None):
    """Patch rows where mlbam_id is NULL but player_name is a full name."""
    if fetch_date is None:
        row = db.execute(text(
            "SELECT MAX(fetch_date) as d FROM mlb.kalshi_markets"
        )).mappings().first()
        fetch_date = row['d']
    rows = db.execute(text("""
        SELECT DISTINCT player_uuid, player_name
        FROM mlb.kalshi_markets
        WHERE fetch_date = :fd AND mlbam_id IS NULL AND player_name IS NOT NULL
    """), {"fd": fetch_date}).mappings().all()

    fixed = 0
    for row in rows:
        mlbam_id, matched_name = match_player_to_mlbam(row['player_name'], db)
        if mlbam_id:
            db.execute(text("""
                UPDATE mlb.kalshi_markets
                SET mlbam_id = :mid, player_name = :pname
                WHERE fetch_date = :fd AND player_uuid = :uuid AND mlbam_id IS NULL
            """), {"mid": mlbam_id, "pname": matched_name,
                   "fd": fetch_date, "uuid": row['player_uuid']})
            fixed += 1
    db.commit()
    print(f"  Backfilled {fixed} previously unmatched players")


if __name__ == "__main__":
    db = SessionLocal()
    try:
        print("Fetching Kalshi markets...")
        markets = fetch_kalshi_markets()
        print(f"  Found {len(markets)} markets")
        store_markets(markets, db)

        print("\nBackfilling unmatched players...")
        backfill_missing_mlbam(db)

        summary = db.execute(text("""
            SELECT player_name, mlbam_id,
                   COUNT(*) as n_lines,
                   MIN(floor_strike) as min_k,
                   MAX(floor_strike) as max_k,
                   ROUND(AVG(implied_prob)::numeric, 3) as avg_prob
            FROM mlb.kalshi_markets
            WHERE fetch_date = (SELECT MAX(fetch_date) FROM mlb.kalshi_markets)
            AND mlbam_id IS NOT NULL
            GROUP BY player_name, mlbam_id
            ORDER BY player_name
        """)).mappings().all()

        print(f"\n  Matched players ({len(summary)}):")
        for r in summary:
            print(f"    {r['player_name']:<25} "
                  f"MLBAM: {r['mlbam_id']} | "
                  f"Lines: {r['n_lines']} ({r['min_k']}-{r['max_k']} Ks) | "
                  f"avg prob: {r['avg_prob']}")
    finally:
        db.close()
