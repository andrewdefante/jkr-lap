from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from database import engine
from models.base import Base
import models
from routers import health, mlb, nascar
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os

app = FastAPI()

from fastapi.staticfiles import StaticFiles
app.mount("/frontend", StaticFiles(directory="/frontend"), name="frontend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def on_startup():
    with engine.connect() as conn:
        for schema in ["mlb", "f1", "nascar"]:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        conn.commit()
    Base.metadata.create_all(bind=engine)

app.include_router(health.router)
app.include_router(mlb.router, prefix="/mlb", tags=["MLB"])
app.include_router(nascar.router, prefix="/nascar", tags=["NASCAR"])

@app.get("/dashboard", include_in_schema=False)
def dashboard():
    return FileResponse("/frontend/darlington.html")

@app.get("/mlb-dashboard")
async def mlb_dashboard():
    from fastapi.responses import FileResponse
    return FileResponse("/frontend/mlb.html")