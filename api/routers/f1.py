from fastapi import APIRouter, HTTPException
import httpx
import datetime
import os
import json
import subprocess

router = APIRouter(prefix="/f1", tags=["f1"])
OPENF1_BASE = "https://api.openf1.org/v1"


@router.get("/session/latest")
async def latest_session():
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(f"{OPENF1_BASE}/sessions?session_key=latest")
        return r.json()


@router.get("/drivers/latest")
async def drivers_latest():
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(f"{OPENF1_BASE}/drivers?session_key=latest")
        return r.json()


@router.get("/positions/latest")
async def positions_latest():
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(f"{OPENF1_BASE}/position?session_key=latest")
        return r.json()


@router.get("/laps/latest")
async def laps_latest():
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(f"{OPENF1_BASE}/laps?session_key=latest")
        return r.json()


@router.get("/stints/latest")
async def stints_latest():
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(f"{OPENF1_BASE}/stints?session_key=latest")
        return r.json()


@router.get("/intervals/latest")
async def intervals_latest():
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(f"{OPENF1_BASE}/intervals?session_key=latest")
        return r.json()


@router.get("/race_control/latest")
async def race_control_latest():
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(f"{OPENF1_BASE}/race_control?session_key=latest")
        return r.json()


@router.get("/track-map/{year}/{race}")
async def track_map(year: int, race: str):
    """Return SVG track map data for a circuit, generating it on first request."""
    out_path = f"/tmp/track_{race}_{year}.json"
    if not os.path.exists(out_path):
        result = subprocess.run(
            ['python3', '/pipeline/f1/generate_track_map.py',
             '--year', str(year), '--race', race, '--output', out_path],
            capture_output=True, text=True,
            env={**os.environ, 'PYTHONPATH': '/app:/pipeline'},
        )
        if not os.path.exists(out_path):
            raise HTTPException(status_code=500, detail=result.stderr[-500:])
    with open(out_path) as f:
        return json.load(f)
