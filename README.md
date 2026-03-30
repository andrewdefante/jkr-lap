# Sports Analytics Platform

MLB, NASCAR, and F1 data pipeline + analytics API.

## Stack
- **FastAPI** — Python web framework / API layer
- **PostgreSQL 16** — Primary database (three separate schemas: mlb, f1, nascar)
- **SQLAlchemy** — ORM
- **Alembic** — Database migrations
- **Docker Compose** — Local development environment
- **pgAdmin** — Visual database browser

---

## Database Architecture

Each sport lives in its own fully isolated Postgres schema.
No cross-sport dependencies. Issues in one schema can't affect the others.

```
sportsplatform (database)
├── mlb schema
│   ├── raw_events       ← Raw GUMBO JSON blobs
│   ├── games
│   ├── at_bats
│   ├── pitches
│   └── players
│
├── f1 schema
│   ├── raw_events       ← Raw Ergast / OpenF1 blobs
│   ├── races
│   ├── results
│   ├── lap_times
│   ├── pit_stops
│   ├── drivers
│   └── constructors
│
└── nascar schema
    ├── raw_events       ← Raw NASCAR data blobs
    ├── races
    ├── results
    ├── laps
    ├── pit_stops
    └── drivers
```

Each sport follows the same ETL pattern:
  fetch.py  →  raw_events (JSONB)  →  transform.py  →  structured tables

---

## First-Time Setup

### 1. Prerequisites
- Docker Desktop installed and running (whale icon in menu bar)

### 2. Start everything
```bash
docker compose up --build
```

First run ~2 minutes. Subsequent runs are instant.
All three Postgres schemas and their tables are created automatically on startup.

### 3. Verify

| URL | What it is |
|---|---|
| http://localhost:8000/health | Should return `{"api":"ok","database":"connected"}` |
| http://localhost:8000/docs | Interactive API docs (Swagger UI) |
| http://localhost:5050 | pgAdmin visual database browser |

### 4. Connect pgAdmin to your database

1. Go to http://localhost:5050
2. Login: `admin@sports.local` / `admin`
3. Right-click "Servers" → Register → Server
4. **General tab:** Name = `Sports Local`
5. **Connection tab:**
   - Host: `db`
   - Port: `5432`
   - Database: `sportsplatform`
   - Username: `sportsuser`
   - Password: `sportspass`

---

## Pulling Your First MLB Data

```bash
# Fetch Game 5 of the 2023 World Series (Rangers vs Diamondbacks)
curl -X GET http://localhost:8000/mlb/fetch-game/716463

# Explore what came back
curl http://localhost:8000/mlb/explore/716463

# List all stored games
curl http://localhost:8000/mlb/games
```

Or use the Swagger UI at http://localhost:8000/docs for a click-through experience.

---

## Project Structure

```
joker-lap/
├── docker-compose.yml
├── api/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py                  # App entry point, schema creation on startup
│   ├── database.py              # DB engine + session
│   ├── models/
│   │   ├── __init__.py          # Exports all models
│   │   ├── base.py              # Shared SQLAlchemy Base
│   │   ├── mlb.py               # All MLB tables (mlb schema)
│   │   ├── f1.py                # All F1 tables (f1 schema)
│   │   └── nascar.py            # All NASCAR tables (nascar schema)
│   └── routers/
│       ├── health.py
│       └── mlb.py               # MLB API endpoints
├── pipeline/
│   ├── mlb/
│   │   ├── fetch.py             # (next step) Hits GUMBO, writes to mlb.raw_events
│   │   └── transform.py         # (next step) Reads raw, writes structured tables
│   ├── f1/
│   │   ├── fetch.py
│   │   └── transform.py
│   └── nascar/
│       ├── fetch.py
│       └── transform.py
└── migrations/
    └── env.py                   # Alembic config (schema-aware)
```

---

## Day-to-Day Commands

```bash
docker compose up               # start everything
docker compose up -d            # start in background
docker compose down             # stop everything
docker compose logs -f api      # watch FastAPI logs
docker compose logs -f db       # watch Postgres logs
docker compose down -v          # stop AND wipe all data (nuclear option)
```

---

## Data Sources

| Sport | Source | Auth | Notes |
|---|---|---|---|
| MLB | GUMBO API (statsapi.mlb.com) | None | Live + historical |
| F1 | Ergast API (ergast.com/api/f1) | None | Historical back to 1950 |
| F1 | OpenF1 (api.openf1.org) | None | Real-time telemetry |
| NASCAR | Racing Reference | None (scrape) | Historical |
| NASCAR | NASCAR Stats API | Partnership required | Official, best quality |

---

## What's Next

1. Get Docker running, pull a game via `/mlb/fetch-game/{game_pk}`
2. Explore the raw data with `/mlb/explore/{game_pk}` and pgAdmin
3. Build `pipeline/mlb/transform.py` to populate structured tables
4. Repeat the fetch → explore → transform pattern for F1, then NASCAR
